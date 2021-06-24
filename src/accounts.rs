use std::collections::{HashMap, HashSet};
use std::time::Duration;

use actix::io::SinkWrite;
use actix::prelude::{Actor, Addr, Context, Handler, Message, Running, StreamHandler};
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

enum InflightRequest {
    Sub(Subscription, Commitment),
    Unsub(Subscription),
    SlotSub(u64),
}

type WsSink = SinkWrite<
    awc::ws::Message,
    futures_util::stream::SplitSink<
        actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
        awc::ws::Message,
    >,
>;

pub(crate) struct AccountUpdateManager {
    websocket_url: String,
    request_id: u64,
    inflight: HashMap<u64, InflightRequest>,
    subs: HashSet<(Subscription, Commitment)>,
    id_to_sub: HashMap<u64, (Subscription, Commitment)>,
    sub_to_id: HashMap<Subscription, u64>,
    sink: Option<WsSink>,
    accounts: AccountsDb,
    program_accounts: ProgramAccountsDb,
    purge_queue: DelayQueueHandle<Subscription>,
    additional_keys: HashMap<Pubkey, HashSet<SmallVec<[Filter; 2]>>>,
}

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl AccountUpdateManager {
    pub fn init(
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        websocket_url: &str,
    ) -> Addr<Self> {
        AccountUpdateManager::create(|ctx| {
            let (handle, stream) = delay_queue();
            let purge_stream = stream.map(AccountCommand::Purge);

            ctx.set_mailbox_capacity(MAILBOX_CAPACITY);

            AccountUpdateManager::add_stream(purge_stream, ctx);
            AccountUpdateManager {
                websocket_url: websocket_url.to_owned(),
                sink: None,
                id_to_sub: HashMap::default(),
                sub_to_id: HashMap::default(),
                inflight: HashMap::default(),
                subs: HashSet::default(),
                request_id: 1,
                accounts: accounts.clone(),
                program_accounts: program_accounts.clone(),
                purge_queue: handle,
                additional_keys: HashMap::default(),
            }
        })
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.request_id;
        self.request_id += 1;
        request_id
    }

    fn send<T: Serialize>(&mut self, request: &T) -> Result<(), serde_json::Error> {
        if let Some(sink) = &mut self.sink {
            sink.write(awc::ws::Message::Text(serde_json::to_string(request)?));
        } else {
            warn!("no sink");
        }
        Ok(())
    }

    fn connect(&self, ctx: &mut Context<Self>) {
        use actix::fut::{ActorFuture, WrapFuture};
        use actix::prelude::AsyncContext;
        use backoff::backoff::Backoff;

        let websocket_url = self.websocket_url.clone();
        let fut = async move {
            loop {
                info!(message = "connecting to websocket", url = %websocket_url);
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
                        error!(message = "failed to connect", url = %websocket_url, error = ?err);
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
            let (sink, stream) = (
                sink,
                stream
                    .take_while(|item| {
                        if let Err(err) = &item {
                            warn!(message = "websocket error", error = %err);
                        }
                        item.is_ok()
                    })
                    .filter_map(Result::ok),
            );
            let sink = SinkWrite::new(sink, ctx);
            AccountUpdateManager::add_stream(stream, ctx);
            actor.sink = Some(sink);
            metrics().websocket_connected.set(1);
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

        let request_id = self.next_request_id();

        let (key, method) = match sub {
            Subscription::Account(key) => (key, "accountSubscribe"),
            Subscription::Program(key) => (key, "programSubscribe"),
        };
        if self.subs.contains(&(sub, commitment)) {
            info!(message = "already trying to subscribe", pubkey = %key);
            return Ok(());
        }

        info!(
            message = "subscribe to",
            pubkey = %key,
            method = method,
            commitment = ?commitment
        );

        let request = Request {
            jsonrpc: "2.0",
            id: request_id,
            method,
            params: SubscribeParams {
                key,
                config: Config {
                    commitment,
                    encoding: Encoding::Base64,
                },
            },
        };

        self.inflight
            .insert(request_id, InflightRequest::Sub(sub, commitment));
        self.subs.insert((sub, commitment));
        self.send(&request)?;
        self.purge_queue.insert(sub, PURGE_TIMEOUT);

        Ok(())
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
            let request_id = self.next_request_id();
            match item {
                AccountCommand::Subscribe(sub, commitment, filters) => {
                    let key = sub.key();
                    metrics().commands.with_label_values(&["subscribe"]).inc();
                    self.subscribe(sub, commitment)?;
                    if let Some(filters) = filters {
                        self.additional_keys.entry(key).or_default().insert(filters);
                    }
                }
                AccountCommand::Purge(sub) => {
                    metrics().commands.with_label_values(&["purge"]).inc();
                    info!(message = "purging", sub = ?sub);

                    #[derive(Serialize)]
                    struct Request<'a> {
                        jsonrpc: &'a str,
                        id: u64,
                        method: &'a str,
                        params: [u64; 1],
                    }

                    if let Some(sub_id) = self.sub_to_id.get(&sub) {
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
                        self.inflight
                            .insert(request_id, InflightRequest::Unsub(sub));
                        self.send(&request)?;
                    }
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
                                if let Some(program_accounts) =
                                    self.program_accounts.remove_all(&key, filter)
                                {
                                    for key in program_accounts.into_accounts() {
                                        self.accounts.remove(&key)
                                    }
                                }
                            }
                        }
                        Subscription::Account(key) => {
                            self.accounts.remove(&key);
                        }
                    }
                }
                AccountCommand::Reset(key) => {
                    metrics().commands.with_label_values(&["reset"]).inc();
                    self.purge_queue.reset(key, PURGE_TIMEOUT);
                }
            }
            Ok(())
        })()
        .map_err(|err| {
            error!(error = %err, "error handling AccountCommand");
        });
    }
}

impl StreamHandler<awc::ws::Frame> for AccountUpdateManager {
    fn handle(&mut self, item: awc::ws::Frame, _ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            use awc::ws::Frame;
            match item {
                Frame::Ping(data) => {
                    if let Some(sink) = &mut self.sink {
                        sink.write(awc::ws::Message::Pong(data));
                    }
                }
                Frame::Text(text) => {
                    #[derive(Deserialize)]
                    struct AnyMessage<'a> {
                        #[serde(borrow)]
                        result: Option<&'a RawValue>,
                        #[serde(borrow)]
                        method: Option<&'a str>,
                        id: Option<u64>,
                        #[serde(borrow)]
                        params: Option<&'a RawValue>,
                    }
                    let value: AnyMessage<'_> = serde_json::from_slice(&text)?;
                    // subscription response
                    if let (Some(result), Some(id)) = (value.result, value.id) {
                        if let Some(req) = self.inflight.remove(&id) {
                            match req {
                                InflightRequest::Sub(sub, commitment) => {
                                    let sub_id: u64 = serde_json::from_str(result.get())?;
                                    self.id_to_sub.insert(sub_id, (sub, commitment));
                                    self.sub_to_id.insert(sub, sub_id);
                                    info!(message = "subscribed to stream", sub_id = sub_id, sub = %sub, commitment = ?commitment);
                                    metrics().subscriptions_active.inc();
                                }
                                InflightRequest::Unsub(sub) => {
                                    //let _is_ok: bool = serde_json::from_str(result.get()).unwrap();
                                    if let Some(sub_id) = self.sub_to_id.remove(&sub) {
                                        if let Some(val) = self.id_to_sub.remove(&sub_id) {
                                            self.subs.remove(&val);
                                        }
                                        info!(
                                            message = "unsubscribed from stream",
                                            sub_id = sub_id,
                                            sub= %sub,
                                        );
                                    }
                                    metrics().subscriptions_active.dec();
                                }
                                InflightRequest::SlotSub(_) => {
                                    info!(message = "subscribed to root");
                                }
                            }
                        }

                        // TODO: method response
                        return Ok(());
                    };
                    // notification
                    if let (Some(method), Some(params)) = (value.method, value.params) {
                        match method {
                            "accountNotification" => {
                                #[derive(Deserialize, Debug)]
                                struct Params {
                                    result: AccountContext,
                                    subscription: u64,
                                }
                                let params: Params = serde_json::from_str(params.get())?;
                                if let Some((sub, commitment)) = self.id_to_sub.get(&params.subscription) {
                                    self.accounts.insert(sub.key(), params.result, *commitment);
                                } else {
                                    warn!(message = "unknown subscription", sub = params.subscription);
                                }
                                metrics().notifications_received.with_label_values(&["accountNotification"]).inc();
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
                                if let Some((program_sub, commitment)) = self.id_to_sub.get(&params.subscription) {
                                    let program_key = program_sub.key();
                                    let key = params.result.value.pubkey;
                                    let account_info = &params.result.value.account;
                                    let data = &account_info.data;
                                    if let Some(filters) = self.additional_keys.get(&program_key) {
                                        for filter_group in filters {
                                            if filter_group.iter().all(|f| f.matches(data)) {
                                                self.program_accounts.add(&program_key, params.result.value.pubkey, Some(filter_group.clone()), *commitment);
                                            } else {
                                                self.program_accounts.remove(&program_key, &params.result.value.pubkey, filter_group.clone(), *commitment);
                                            }
                                        }
                                    }
                                    self.accounts.insert(key, AccountContext {
                                        value: Some(params.result.value.account), context: params.result.context }, *commitment);
                                    self.program_accounts.add(&program_key, params.result.value.pubkey, None, *commitment);
                                } else {
                                    warn!(message = "unknown subscription", sub = params.subscription);
                                }
                                metrics().notifications_received.with_label_values(&["programNotification"]).inc();
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
                }
                _ => return Ok(()),
            }
            Ok(())
        })().map_err(|err| {
            error!(message = "error handling Frame", error = ?err);
        });
    }

    fn started(&mut self, _: &mut Context<Self>) {
        info!("websocket connected");
        // subscribe to slots
        let request_id = self.next_request_id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "rootSubscribe",
        });
        self.inflight
            .insert(request_id, InflightRequest::SlotSub(request_id));
        let _ = self.send(&request);

        // restore subscriptions
        info!("adding subscriptions");
        let subs_len = self.subs.len();
        let subs = std::mem::replace(&mut self.subs, HashSet::with_capacity(subs_len));
        for (sub, commitment) in subs {
            self.subscribe(sub, commitment).unwrap()
            // TODO: it would be nice to retrieve current state for
            // everything we had before
        }
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        info!("websocket disconnected");
        metrics().websocket_connected.set(0);
        self.inflight.clear();
        self.id_to_sub.clear();
        self.sub_to_id.clear();

        info!("clearing db");
        self.accounts.clear();

        self.connect(ctx);
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
        error!(message = "websocket error", error = ?err);
        self.connect(ctx);
        Running::Continue
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash, Copy)]
pub(crate) enum Subscription {
    Account(Pubkey),
    Program(Pubkey),
}

impl Subscription {
    fn key(&self) -> Pubkey {
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
    Reset(Subscription),
    Purge(Subscription),
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
                                if let Some(key) = map.remove(&item) {
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
