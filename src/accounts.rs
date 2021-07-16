use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use actix::io::SinkWrite;
use actix::prelude::AsyncContext;
use actix::prelude::{Actor, Addr, Context, Handler, Message, Running, StreamHandler};
use actix::SpawnHandle;
use actix_http::ws;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{DelayQueue, Instant};
use tracing::{error, info, warn};

use crate::metrics::pubsub_metrics as metrics;
use crate::types::{
    AccountContext, AccountInfo, AccountsDb, Commitment, Encoding, Filter, ProgramAccountsDb,
    Pubkey, SolanaContext,
};

const MAILBOX_CAPACITY: usize = 512;
const PURGE_TIMEOUT: Duration = Duration::from_secs(600);
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(30);
const WEBSOCKET_PING_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum InflightRequest {
    Sub(Subscription, Commitment),
    Unsub(Subscription, Commitment),
    SlotSub(u64),
}

type WsSink = SinkWrite<
    awc::ws::Message,
    futures_util::stream::SplitSink<
        actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
        awc::ws::Message,
    >,
>;

#[derive(Clone)]
pub(crate) struct PubSubManager(Vec<(actix::Addr<AccountUpdateManager>, Arc<AtomicBool>)>);

impl PubSubManager {
    pub(crate) fn init(
        connections: u32,
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        websocket_url: &str,
    ) -> Self {
        let mut addrs = Vec::new();
        for id in 0..connections {
            let connected = Arc::new(AtomicBool::new(false));
            let addr = AccountUpdateManager::init(
                id,
                accounts.clone(),
                program_accounts.clone(),
                Arc::clone(&connected),
                &websocket_url,
            );
            addrs.push((addr, connected))
        }
        PubSubManager(addrs)
    }

    fn get_idx_by_key(&self, key: Pubkey) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % self.0.len() as u64) as usize
    }

    fn get_addr_by_key(&self, key: Pubkey) -> actix::Addr<AccountUpdateManager> {
        let idx = self.get_idx_by_key(key);
        self.0[idx].0.clone()
    }

    pub fn is_connected(&self, key: Pubkey) -> bool {
        let idx = self.get_idx_by_key(key);
        self.0[idx].1.load(Ordering::Relaxed)
    }

    pub fn reset(&self, sub: Subscription, commitment: Commitment) {
        let addr = self.get_addr_by_key(sub.key());
        addr.do_send(AccountCommand::Reset(sub, commitment))
    }

    pub fn subscribe(
        &self,
        sub: Subscription,
        commitment: Commitment,
        filters: Option<smallvec::SmallVec<[Filter; 2]>>,
    ) {
        let addr = self.get_addr_by_key(sub.key());
        addr.do_send(AccountCommand::Subscribe(sub, commitment, filters))
    }
}

pub(crate) struct AccountUpdateManager {
    websocket_url: String,
    actor_id: u32,
    request_id: u64,
    inflight: HashMap<u64, (InflightRequest, Instant)>,
    subs: HashSet<(Subscription, Commitment)>,
    id_to_sub: HashMap<u64, (Subscription, Commitment)>,
    sub_to_id: HashMap<(Subscription, Commitment), u64>,
    connection: Option<(WsSink, SpawnHandle)>,
    accounts: AccountsDb,
    program_accounts: ProgramAccountsDb,
    purge_queue: DelayQueueHandle<(Subscription, Commitment)>,
    additional_keys: HashMap<Pubkey, HashSet<SmallVec<[Filter; 2]>>>,
    last_received_at: Instant,
    connected: Arc<AtomicBool>,
    buffer: BytesMut,
}

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl AccountUpdateManager {
    pub fn init(
        actor_id: u32,
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        connected: Arc<AtomicBool>,
        websocket_url: &str,
    ) -> Addr<Self> {
        AccountUpdateManager::create(|ctx| {
            let (handle, stream) = delay_queue();
            let purge_stream = stream.map(|(sub, com)| AccountCommand::Purge(sub, com));

            ctx.set_mailbox_capacity(MAILBOX_CAPACITY);

            ctx.run_interval(Duration::from_secs(5), move |actor, ctx| {
                if actor.connected.load(Ordering::Relaxed) {
                    if let Some((sink, _)) = &mut actor.connection {
                        if sink
                            .write(awc::ws::Message::Ping(b"hello?".as_ref().into()))
                            .is_some()
                        {
                            warn!("failed to write ping");
                        }

                        let elapsed = actor.last_received_at.elapsed();
                        if elapsed > WEBSOCKET_PING_TIMEOUT {
                            warn!(
                                "no messages received in {:?}, assume connection lost ({:?})",
                                elapsed, actor.last_received_at
                            );
                            actor.reconnect(ctx);
                        }
                    }
                }
            });

            ctx.run_interval(Duration::from_secs(5), move |actor, _ctx| {
                if actor.connected.load(Ordering::Relaxed) {
                    actor.inflight.retain(|request_id, (req, time)| {
                        let elapsed = time.elapsed();
                        let too_long = elapsed > IN_FLIGHT_TIMEOUT;
                        if too_long {
                            warn!(request_id, request = ?req, timeout = ?IN_FLIGHT_TIMEOUT,
                                elapsed = ?elapsed, "request in flight too long, assume dead");
                        }
                        !too_long
                    })
                }
            });

            AccountUpdateManager::add_stream(purge_stream, ctx);
            AccountUpdateManager {
                actor_id,
                websocket_url: websocket_url.to_owned(),
                connection: None,
                id_to_sub: HashMap::default(),
                sub_to_id: HashMap::default(),
                inflight: HashMap::default(),
                subs: HashSet::default(),
                request_id: 1,
                accounts: accounts.clone(),
                program_accounts: program_accounts.clone(),
                purge_queue: handle,
                additional_keys: HashMap::default(),
                connected,
                last_received_at: Instant::now(),
                buffer: BytesMut::new(),
            }
        })
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.request_id;
        self.request_id += 1;
        request_id
    }

    fn send<T: Serialize>(&mut self, request: &T) -> Result<(), serde_json::Error> {
        if let Some((sink, _)) = &mut self.connection {
            sink.write(awc::ws::Message::Text(serde_json::to_string(request)?));
        } else {
            warn!("no sink");
        }
        Ok(())
    }

    fn connect(&self, ctx: &mut Context<Self>) {
        use actix::fut::{ActorFuture, WrapFuture};
        use backoff::backoff::Backoff;

        let websocket_url = self.websocket_url.clone();
        let actor_id = self.actor_id;
        let fut = async move {
            loop {
                info!(message = "connecting to websocket", url = %websocket_url, actor_id = %actor_id);
                let mut backoff = backoff::ExponentialBackoff {
                    initial_interval: Duration::from_millis(100),
                    ..Default::default()
                };
                let res = awc::Client::builder()
                    .max_http_version(awc::http::Version::HTTP_11)
                    .finish()
                    .ws(&websocket_url)
                    .connect()
                    .await;
                match res {
                    Ok((_, conn)) => break conn,
                    Err(err) => {
                        error!(message = "failed to connect", url = %websocket_url, error = ?err, actor_id = %actor_id);
                        tokio::time::delay_for(
                            backoff
                                .next_backoff()
                                .unwrap_or_else(|| Duration::from_secs(1)),
                        )
                        .await;
                    }
                }
            }
        };
        let fut = fut.into_actor(self).map(|conn, actor, ctx| {
            let (sink, stream) = futures_util::stream::StreamExt::split(conn);
            let actor_id = actor.actor_id;
            let sink = SinkWrite::new(sink, ctx);
            let stream_handle = AccountUpdateManager::add_stream(stream, ctx);

            let old = std::mem::replace(&mut actor.connection, Some((sink, stream_handle)));
            if let Some((mut sink, stream)) = old {
                warn!(message = "old connection not canceled properly", actor_id = %actor_id);
                sink.close();
                ctx.cancel_future(stream);
            }
            metrics().websocket_connected.inc();
            actor.last_received_at = Instant::now();
        });
        ctx.wait(fut);
    }

    fn subscribe(
        &mut self,
        sub: Subscription,
        commitment: Commitment,
    ) -> Result<(), serde_json::Error> {
        #[derive(Serialize)]
        struct Request<'a> {
            jsonrpc: &'a str,
            id: u64,
            method: &'a str,
            params: SubscribeParams, // TODO: commitment and other params
        }

        #[derive(Serialize)]
        struct Config {
            commitment: Commitment,
            encoding: Encoding,
        }

        struct SubscribeParams {
            key: Pubkey,
            config: Config,
        }

        impl Serialize for SubscribeParams {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&self.key)?;
                seq.serialize_element(&self.config)?;
                seq.end()
            }
        }

        if self.subs.contains(&(sub, commitment)) {
            info!(message = "already trying to subscribe", pubkey = %sub.key());
            return Ok(());
        }

        let request_id = self.next_request_id();

        let (key, method) = match sub {
            Subscription::Account(key) => (key, "accountSubscribe"),
            Subscription::Program(key) => (key, "programSubscribe"),
        };
        info!(
            message = "subscribe to",
            pubkey = %key,
            method = method,
            commitment = ?commitment,
            request_id = request_id,
        );

        let request = Request {
            jsonrpc: "2.0",
            id: request_id,
            method,
            params: SubscribeParams {
                key,
                config: Config {
                    commitment,
                    encoding: Encoding::Base64Zstd,
                },
            },
        };

        self.inflight.insert(
            request_id,
            (InflightRequest::Sub(sub, commitment), Instant::now()),
        );
        self.subs.insert((sub, commitment));
        self.send(&request)?;
        self.purge_queue.insert((sub, commitment), PURGE_TIMEOUT);
        metrics().subscribe_requests.inc();

        Ok(())
    }

    fn unsubscribe(
        &mut self,
        sub: Subscription,
        commitment: Commitment,
    ) -> Result<(), serde_json::Error> {
        let request_id = self.next_request_id();

        #[derive(Serialize)]
        struct Request<'a> {
            jsonrpc: &'a str,
            id: u64,
            method: &'a str,
            params: [u64; 1],
        }

        if let Some(sub_id) = self.sub_to_id.get(&(sub, commitment)) {
            info!(message = "unsubscribe", key = %sub.key(), commitment = ?commitment, request_id = request_id);

            let method = match sub {
                Subscription::Program(_) => "programUnsubscribe",
                Subscription::Account(_) => "accountUnsubscribe",
            };
            let request = Request {
                jsonrpc: "2.0",
                id: request_id,
                method,
                params: [*sub_id],
            };
            self.inflight.insert(
                request_id,
                (InflightRequest::Unsub(sub, commitment), Instant::now()),
            );
            self.send(&request)?;
        }
        self.purge_key(&sub, commitment);

        Ok(())
    }

    fn purge_key(&mut self, sub: &Subscription, commitment: Commitment) {
        info!(self.actor_id, message = "purge", key = %sub.key(), commitment = ?commitment);
        match sub {
            Subscription::Program(key) => {
                let keys = self
                    .additional_keys
                    .remove(&key)
                    .into_iter()
                    .flatten()
                    .map(|filter| (key, Some(filter)))
                    .chain(Some((key, None)));
                for (key, filter) in keys {
                    if let Some(program_accounts) = self.program_accounts.remove_all(&key, filter) {
                        for key in program_accounts.into_accounts() {
                            self.accounts.remove(&key, commitment)
                        }
                    }
                }
            }
            Subscription::Account(key) => {
                self.accounts.remove(&key, commitment);
            }
        }
    }

    fn process_ws_message(&mut self, text: &[u8]) -> Result<(), serde_json::Error> {
        #[derive(Deserialize, Debug)]
        struct AnyMessage<'a> {
            #[serde(borrow)]
            result: Option<&'a RawValue>,
            #[serde(borrow)]
            method: Option<&'a str>,
            id: Option<u64>,
            #[serde(borrow)]
            params: Option<&'a RawValue>,
            #[serde(borrow)]
            error: Option<&'a RawValue>,
        }
        let value: AnyMessage<'_> = serde_json::from_slice(&text)?;
        match value {
            // subscription error
            AnyMessage {
                error: Some(error),
                id: Some(id),
                ..
            } => {
                if let Some((req, _)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(sub, commitment) => {
                            warn!(self.actor_id, request_id = id, error = ?error, key = %sub.key(), commitment = ?commitment, "subscribe failed");
                            metrics().subscribe_errors.inc();
                            self.subs.remove(&(sub, commitment));
                        }
                        InflightRequest::Unsub(sub, commitment) => {
                            warn!(self.actor_id, request_id = id, error = ?error, key = %sub.key(), commitment = ?commitment, "unsubscribe failed");
                        }
                        InflightRequest::SlotSub(_) => {
                            warn!(self.actor_id, request_id = id, error = ?error, "slot subscribe failed");
                        }
                    }
                }
            }
            // subscription response
            AnyMessage {
                result: Some(result),
                id: Some(id),
                ..
            } => {
                if let Some((req, _)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(sub, commitment) => {
                            let sub_id: u64 = serde_json::from_str(result.get())?;
                            self.id_to_sub.insert(sub_id, (sub, commitment));
                            self.sub_to_id.insert((sub, commitment), sub_id);
                            info!(self.actor_id, message = "subscribed to stream", sub_id = sub_id, sub = %sub, commitment = ?commitment);
                            metrics().subscriptions_active.inc();
                        }
                        InflightRequest::Unsub(sub, commitment) => {
                            let is_ok: bool = serde_json::from_str(result.get())?;
                            if is_ok {
                                if let Some(sub_id) = self.sub_to_id.remove(&(sub, commitment)) {
                                    self.id_to_sub.remove(&sub_id);
                                    self.subs.remove(&(sub, commitment));
                                    info!(
                                        message = "unsubscribed from stream",
                                        sub_id = sub_id,
                                        key = %sub.key(),
                                        sub = %sub,
                                    );
                                } else {
                                }
                                metrics().subscriptions_active.dec();
                            } else {
                                warn!(self.actor_id, message = "unsubscribe failed", key = %sub.key());
                            }
                        }
                        InflightRequest::SlotSub(_) => {
                            info!(self.actor_id, message = "subscribed to root");
                        }
                    }
                }
            }
            // notification
            AnyMessage {
                method: Some(method),
                params: Some(params),
                ..
            } => {
                match method {
                    "accountNotification" => {
                        #[derive(Deserialize, Debug)]
                        struct Params {
                            result: AccountContext,
                            subscription: u64,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        if let Some((sub, commitment)) = self.id_to_sub.get(&params.subscription) {
                            //info!(key = %sub.key(), "received account notification");
                            self.accounts.insert(sub.key(), params.result, *commitment);
                        } else {
                            warn!(message = "unknown subscription", sub = params.subscription);
                        }
                        metrics()
                            .notifications_received
                            .with_label_values(&["accountNotification"])
                            .inc();
                    }
                    "programNotification" => {
                        #[derive(Deserialize, Debug)]
                        struct Value {
                            account: AccountInfo,
                            pubkey: Pubkey,
                        }
                        #[derive(Deserialize, Debug)]
                        struct Result {
                            context: SolanaContext,
                            value: Value,
                        }
                        #[derive(Deserialize, Debug)]
                        struct Params {
                            result: Result,
                            subscription: u64,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        if let Some((program_sub, commitment)) =
                            self.id_to_sub.get(&params.subscription)
                        {
                            let program_key = program_sub.key();
                            let key = params.result.value.pubkey;
                            let account_info = &params.result.value.account;
                            let data = &account_info.data;
                            if let Some(filters) = self.additional_keys.get(&program_key) {
                                for filter_group in filters {
                                    if filter_group.iter().all(|f| f.matches(data)) {
                                        self.program_accounts.add(
                                            &program_key,
                                            params.result.value.pubkey,
                                            Some(filter_group.clone()),
                                            *commitment,
                                        );
                                    } else {
                                        self.program_accounts.remove(
                                            &program_key,
                                            &params.result.value.pubkey,
                                            filter_group.clone(),
                                            *commitment,
                                        );
                                    }
                                }
                            }
                            self.accounts.insert(
                                key,
                                AccountContext {
                                    value: Some(params.result.value.account),
                                    context: params.result.context,
                                },
                                *commitment,
                            );
                            self.program_accounts.add(
                                &program_key,
                                params.result.value.pubkey,
                                None,
                                *commitment,
                            );
                        } else {
                            warn!(message = "unknown subscription", sub = params.subscription);
                        }
                        metrics()
                            .notifications_received
                            .with_label_values(&["programNotification"])
                            .inc();
                    }
                    "rootNotification" => {
                        #[derive(Deserialize)]
                        struct Params {
                            result: u64, //SlotInfo,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        //info!("slot {} root {} parent {}", params.result.slot, params.result.root, params.result.parent);
                        let _slot = params.result; // TODO: figure out which slot validator *actually* reports
                    }
                    _ => {
                        warn!(message = "unknown notification", method = method);
                    }
                }
            }
            any => {
                warn!(msg = ?any, text = ?text, "unidentified websocket message");
            }
        }

        Ok(())
    }

    fn reconnect(&mut self, ctx: &mut Context<Self>) {
        let actor_id = self.actor_id;
        info!(actor_id, "websocket disconnected");
        metrics().websocket_connected.dec();
        metrics()
            .subscriptions_active
            .sub(self.id_to_sub.len() as i64);
        self.connected.store(false, Ordering::Relaxed);

        if let Some((mut sink, stream)) = self.connection.take() {
            sink.close();
            ctx.cancel_future(stream);
        }

        self.buffer.clear();
        self.inflight.clear();
        self.id_to_sub.clear();
        self.sub_to_id.clear();

        info!(actor_id, "purging related caches");
        let to_purge: Vec<_> = self.subs.iter().cloned().collect();
        for (sub, commitment) in to_purge {
            self.purge_key(&sub, commitment);
        }

        self.connect(ctx);
    }
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        let _ = <Self as Handler<AccountCommand>>::handle(self, item, ctx);
    }

    fn finished(&mut self, _ctx: &mut Context<Self>) {
        info!("purge stream finished");
    }
}

impl Handler<AccountCommand> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: AccountCommand, _ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            match item {
                AccountCommand::Subscribe(sub, commitment, filters) => {
                    let key = sub.key();
                    metrics().commands.with_label_values(&["subscribe"]).inc();
                    self.subscribe(sub, commitment)?;
                    if let Some(filters) = filters {
                        self.additional_keys.entry(key).or_default().insert(filters);
                    }
                }
                AccountCommand::Purge(sub, commitment) => {
                    metrics().commands.with_label_values(&["purge"]).inc();
                    self.unsubscribe(sub, commitment)?;
                }
                AccountCommand::Reset(key, commitment) => {
                    metrics().commands.with_label_values(&["reset"]).inc();
                    self.purge_queue.reset((key, commitment), PURGE_TIMEOUT);
                }
            }
            Ok(())
        })()
        .map_err(|err| {
            error!(error = %err, "error handling AccountCommand");
        });
    }
}

impl StreamHandler<Result<awc::ws::Frame, awc::error::WsProtocolError>> for AccountUpdateManager {
    fn handle(
        &mut self,
        item: Result<awc::ws::Frame, awc::error::WsProtocolError>,
        ctx: &mut Context<Self>,
    ) {
        self.last_received_at = Instant::now();

        let item = match item {
            Ok(item) => item,
            Err(err) => {
                error!(error = %err, "websocket protocol error");
                self.reconnect(ctx);
                return;
            }
        };

        let _ = (|| -> Result<(), serde_json::Error> {
            use awc::ws::Frame;

            match item {
                Frame::Ping(data) => {
                    metrics().bytes_received.inc_by(data.len() as u64);
                    if let Some((sink, _)) = &mut self.connection {
                        sink.write(awc::ws::Message::Pong(data));
                    }
                }
                Frame::Pong(_) => {
                    // do nothing
                }
                Frame::Text(text) => {
                    metrics().bytes_received.inc_by(text.len() as u64);
                    self.process_ws_message(&text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing message");
                            err
                        })?
                }
                Frame::Close(reason) => {
                    warn!(reason = ?reason, "websocket closing");
                    self.reconnect(ctx);
                }
                Frame::Binary(msg) => {
                    warn!(msg = ?msg, "unexpected binary message");
                }
                Frame::Continuation(msg) => match msg {
                    ws::Item::FirstText(bytes) => {
                        metrics().bytes_received.inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Continue(bytes) => {
                        metrics().bytes_received.inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Last(bytes) => {
                        metrics().bytes_received.inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                        let text = std::mem::replace(&mut self.buffer, BytesMut::new());
                        self.process_ws_message(&text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing fragmented message");
                            err
                        })?;
                    }
                    ws::Item::FirstBinary(_) => {
                        warn!(msg = ?msg, "unexpected continuation message");
                    }
                },
            }
            Ok(())
        })()
        .map_err(|err| {
            error!(message = "error handling Frame", error = ?err);
        });
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(self.actor_id, "websocket connected");
        // subscribe to slots
        let request_id = self.next_request_id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "rootSubscribe",
        });
        self.inflight.insert(
            request_id,
            (InflightRequest::SlotSub(request_id), Instant::now()),
        );
        let _ = self.send(&request);

        // restore subscriptions
        info!(self.actor_id, "adding subscriptions");
        let subs_len = self.subs.len();
        let subs = std::mem::replace(&mut self.subs, HashSet::with_capacity(subs_len));

        for (sub, commitment) in subs {
            self.subscribe(sub, commitment).unwrap()
            // TODO: it would be nice to retrieve current state for
            // everything we had before
        }
        self.connected.store(true, Ordering::Relaxed);
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        self.reconnect(ctx);
    }
}

impl Actor for AccountUpdateManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.connect(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        info!("pubsub actor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("pubsub actor stopped");
    }
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {
    fn error(&mut self, err: awc::error::WsProtocolError, ctx: &mut Self::Context) -> Running {
        error!(message = "websocket write error", error = ?err);
        self.reconnect(ctx);
        Running::Continue
    }

    fn finished(&mut self, _ctx: &mut Self::Context) {
        info!("writer finished");
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash, Copy)]
pub(crate) enum Subscription {
    Account(Pubkey),
    Program(Pubkey),
}

impl Subscription {
    pub fn key(&self) -> Pubkey {
        match *self {
            Subscription::Account(key) => key,
            Subscription::Program(key) => key,
        }
    }
}

impl std::fmt::Display for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, key) = match self {
            Subscription::Account(key) => ("Account", key),
            Subscription::Program(key) => ("Program", key),
        };
        write!(f, "{}({})", prefix, key)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) enum AccountCommand {
    Subscribe(Subscription, Commitment, Option<SmallVec<[Filter; 2]>>),
    Reset(Subscription, Commitment),
    Purge(Subscription, Commitment),
}

enum DelayQueueCommand<T> {
    Insert(T, Instant),
    Reset(T, Instant),
}

struct DelayQueueHandle<T>(mpsc::UnboundedSender<DelayQueueCommand<T>>);

impl<T> DelayQueueHandle<T> {
    fn insert_at(&self, item: T, time: Instant) {
        let _ = self.0.send(DelayQueueCommand::Insert(item, time));
    }

    fn insert(&self, item: T, dur: Duration) {
        self.insert_at(item, Instant::now() + dur)
    }

    fn reset(&self, item: T, dur: Duration) {
        let _ = self
            .0
            .send(DelayQueueCommand::Reset(item, Instant::now() + dur));
    }
}

fn delay_queue<T: Clone + std::hash::Hash + Eq>() -> (DelayQueueHandle<T>, impl Stream<Item = T>) {
    let (sender, incoming) = mpsc::unbounded_channel::<DelayQueueCommand<T>>();
    let mut map: HashMap<T, _> = HashMap::default();
    let stream = stream_generator::generate_stream(|mut stream| async move {
        let mut delay_queue = DelayQueue::new();
        tokio::pin!(incoming);

        loop {
            tokio::select! {
                item = incoming.next() => {
                    if let Some(item) = item {
                        match item {
                            DelayQueueCommand::Insert(item, time) => {
                                map.insert(item.clone(), delay_queue.insert_at(item, time));
                            },
                            DelayQueueCommand::Reset(item, time) => {
                                if let Some(key) = map.get(&item) {
                                    delay_queue.reset_at(&key, time);
                                } else {
                                    map.insert(item.clone(), delay_queue.insert_at(item, time));
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                out = delay_queue.next(), if !delay_queue.is_empty() => {
                    if let Some(Ok(out)) = out {
                        let item = out.into_inner();
                        map.remove(&item);
                        stream.send(item).await;
                    }
                }
            }
        }
    });
    (DelayQueueHandle(sender), stream)
}
