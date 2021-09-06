use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use actix::io::SinkWrite;
use actix::prelude::AsyncContext;
use actix::prelude::{
    Actor, ActorContext, Addr, Context, Handler, Message, Running, StreamHandler, Supervised,
    Supervisor,
};
use actix::Arbiter;
use actix_http::ws;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use slab::Slab;
use smallvec::SmallVec;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{DelayQueue, Instant};
use tracing::{error, info, warn};

use crate::metrics::pubsub_metrics as metrics;
use crate::types::{
    AccountContext, AccountInfo, AccountsDb, Commitment, Encoding, Filter, ProgramAccountsDb,
    Pubkey, Slot, SolanaContext,
};

const MAILBOX_CAPACITY: usize = 512;
const DEAD_REQUEST_LIMIT: usize = 0;
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(60);
const WEBSOCKET_PING_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
enum InflightRequest {
    Sub(SlabId),
    Unsub(SlabId),
    SlotSub(u64),
}

type SlabId = usize;

struct Meta {
    created_at: Instant,
    updated_at: Instant,
    first_slot: Option<Slot>,
    slab_id: SlabId,
}

impl Meta {
    fn new(slab_id: SlabId) -> Self {
        let now = Instant::now();
        Meta {
            created_at: now,
            updated_at: now,
            first_slot: None,
            slab_id,
        }
    }

    fn update(&mut self) -> Duration {
        let ret = self.updated_at.elapsed();
        self.updated_at = Instant::now();
        ret
    }

    fn since_creation(&self) -> Duration {
        self.created_at.elapsed()
    }
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
        time_to_live: Duration,
    ) -> Self {
        let mut addrs = Vec::new();
        for id in 0..connections {
            let active = Arc::new(AtomicBool::new(false));
            let addr = AccountUpdateManager::init(
                id,
                accounts.clone(),
                program_accounts.clone(),
                Arc::clone(&active),
                websocket_url,
                time_to_live,
            );
            addrs.push((addr, active))
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

    pub fn subscription_active(&self, key: Pubkey) -> bool {
        let idx = self.get_idx_by_key(key);
        self.0[idx].1.load(Ordering::Relaxed)
    }

    pub fn reset(&self, sub: Subscription, commitment: Commitment) {
        let addr = self.get_addr_by_key(sub.key());
        addr.do_send(AccountCommand::Reset(sub, commitment))
    }

    pub fn subscribe_account(&self, key: Pubkey, commitment: Commitment) {
        let addr = self.get_addr_by_key(key);
        let sub = Subscription::Account(key);
        addr.do_send(AccountCommand::Subscribe(sub, commitment))
    }

    pub fn subscribe_program(
        &self,
        key: Pubkey,
        commitment: Commitment,
        filters: Option<smallvec::SmallVec<[Filter; 2]>>,
    ) {
        let sub = Subscription::Program(key, filters);
        let addr = self.get_addr_by_key(sub.key());
        addr.do_send(AccountCommand::Subscribe(sub, commitment))
    }

    // pub fn subscribe(
    //     &self,
    //     sub: Subscription,
    //     commitment: Commitment,
    //     filters: Option<smallvec::SmallVec<[Filter; 2]>>,
    // ) {
    //     let addr = self.get_addr_by_key(sub.key());
    //     addr.do_send(AccountCommand::Subscribe(sub, commitment, filters))
    // }
}

enum Connection {
    Disconnected,
    Connecting,
    Connected { sink: WsSink },
}

impl Connection {
    fn is_connected(&self) -> bool {
        matches!(self, Connection::Connected { .. })
    }

    fn send(&mut self, msg: ws::Message) -> Result<(), ws::Message> {
        if let Connection::Connected { sink, .. } = self {
            sink.write(msg).map_or(Ok(()), Err)?;
        }
        Ok(())
    }
}

pub(crate) struct AccountUpdateManager {
    websocket_url: String,
    time_to_live: Duration,
    actor_id: u32,
    actor_name: String,
    request_id: u64,
    inflight: HashMap<u64, (InflightRequest, Instant)>,
    subs: HashMap<(Subscription, Commitment), Meta>,
    sub_storage: Slab<(Subscription, Commitment)>,
    active_accounts: HashMap<(Pubkey, Commitment), Arc<Pubkey>>,
    id_to_sub: HashMap<u64, SlabId>,
    sub_to_id: HashMap<SlabId, u64>,
    connection: Connection,
    accounts: AccountsDb,
    program_accounts: ProgramAccountsDb,
    purge_queue: Option<DelayQueueHandle<SlabId>>,
    last_received_at: Instant,
    active: Arc<AtomicBool>,
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
        active: Arc<AtomicBool>,
        websocket_url: &str,
        time_to_live: Duration,
    ) -> Addr<Self> {
        let arbiter = Arbiter::new();
        let websocket_url = websocket_url.to_owned();
        let actor_name = format!("pubsub-{}", actor_id);
        Supervisor::start_in_arbiter(&arbiter, move |_ctx| AccountUpdateManager {
            actor_id,
            time_to_live,
            actor_name,
            websocket_url,
            connection: Connection::Disconnected,
            id_to_sub: HashMap::default(),
            sub_to_id: HashMap::default(),
            inflight: HashMap::default(),
            sub_storage: Slab::new(),
            subs: HashMap::default(),
            active_accounts: HashMap::default(),
            request_id: 1,
            accounts: accounts.clone(),
            program_accounts: program_accounts.clone(),
            purge_queue: None,
            active,
            last_received_at: Instant::now(),
            buffer: BytesMut::new(),
        })
    }

    fn start_periodic(ctx: &mut Context<Self>) {
        ctx.run_interval(Duration::from_secs(5), move |actor, ctx| {
            if actor.connection.is_connected() {
                if actor
                    .connection
                    .send(awc::ws::Message::Ping(b"hello?".as_ref().into()))
                    .is_err()
                {
                    warn!(actor_id = actor.actor_id, "failed to send ping");
                    ctx.stop();
                }

                let elapsed = actor.last_received_at.elapsed();
                if elapsed > WEBSOCKET_PING_TIMEOUT {
                    warn!(
                        actor_id = actor.actor_id,
                        "no messages received in {:?}, assume connection lost ({:?})",
                        elapsed,
                        actor.last_received_at
                    );
                    ctx.stop();
                }
            }
        });

        ctx.run_interval(Duration::from_secs(5), move |actor, ctx| {
            let actor_id = actor.actor_id;

            if actor.connection.is_connected() {
                let mut dead_requests = 0;
                actor.inflight.retain(|request_id, (req, time)| {
                    let elapsed = time.elapsed();
                    let too_long = elapsed > IN_FLIGHT_TIMEOUT;
                    if too_long {
                        warn!(actor_id,
                                request_id, request = ?req, timeout = ?IN_FLIGHT_TIMEOUT,
                                elapsed = ?elapsed, "request in flight too long, assume dead");
                        dead_requests += 1;
                    }
                    !too_long
                });
                metrics()
                    .inflight_entries
                    .with_label_values(&[&actor.actor_name])
                    .set(actor.inflight.len() as i64);
                if dead_requests > DEAD_REQUEST_LIMIT {
                    warn!(
                        message = "too many dead requests, disconnecting",
                        count = dead_requests
                    );
                    ctx.stop();
                }
            }
        });
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.request_id;
        self.request_id += 1;
        request_id
    }

    fn send<T: Serialize>(&mut self, request: &T) -> Result<(), serde_json::Error> {
        if self.connection.is_connected() {
            let _ = self
                .connection
                .send(awc::ws::Message::Text(serde_json::to_string(request)?));
        } else {
            warn!(self.actor_id, "not connected");
        }
        Ok(())
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        use actix::fut::{ActorFuture, WrapFuture};
        use backoff::backoff::Backoff;

        let websocket_url = self.websocket_url.clone();
        let actor_id = self.actor_id;

        if self.connection.is_connected() {
            warn!(message = "old connection not canceled properly", actor_id = %actor_id);
            return;
        }
        self.connection = Connection::Connecting;

        self.update_status();

        let fut = async move {
            let mut backoff = backoff::ExponentialBackoff {
                current_interval: Duration::from_millis(300),
                initial_interval: Duration::from_millis(300),
                ..Default::default()
            };

            loop {
                info!(message = "connecting to websocket", url = %websocket_url, actor_id = %actor_id);
                let res = awc::Client::builder()
                    .max_http_version(awc::http::Version::HTTP_11)
                    .timeout(Duration::from_secs(5))
                    .finish()
                    .ws(&websocket_url)
                    .max_frame_size(32 * 1024 * 1024)
                    .connect()
                    .await;
                match res {
                    Ok((resp, conn)) => {
                        info!(actor_id, message = "connection established", resp = ?resp);
                        break conn;
                    }
                    Err(err) => {
                        let delay = backoff
                            .next_backoff()
                            .unwrap_or_else(|| Duration::from_secs(1));
                        error!(message = "failed to connect, waiting", url = %websocket_url,
                                error = ?err, actor_id = %actor_id, delay = ?delay);
                        tokio::time::delay_for(delay).await;
                    }
                }
            }
        };
        let fut = fut.into_actor(self).map(|conn, actor, ctx| {
            let (sink, stream) = futures_util::stream::StreamExt::split(conn);
            let actor_id = actor.actor_id;
            let mut sink = SinkWrite::new(sink, ctx);

            info!(actor_id, message = "websocket sending ping");
            if sink
                .write(ws::Message::Ping(b"check connection".as_ref().into()))
                .is_some()
            {
                error!(actor_id, "failed to send check msg");
                ctx.stop();
                return;
            };
            info!(actor_id, message = "websocket ping sent");

            let old = std::mem::replace(&mut actor.connection, Connection::Connected { sink });
            if old.is_connected() {
                warn!(actor_id, "was connected, should not have happened");
            }

            AccountUpdateManager::add_stream(stream, ctx);
            info!(actor_id, message = "websocket stream added");
            metrics()
                .websocket_connected
                .with_label_values(&[&actor.actor_name])
                .set(1);
            actor.last_received_at = Instant::now();
        });
        ctx.wait(fut);
        info!(self.actor_id, message = "connection future complete");
        self.update_status();
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
            params: SubscribeParams<'a>,
        }

        #[derive(Serialize)]
        struct Config<'a> {
            commitment: Commitment,
            encoding: Encoding,
            #[serde(skip_serializing_if = "Option::is_none")]
            filters: Option<&'a SmallVec<[Filter; 2]>>,
        }

        struct SubscribeParams<'a> {
            key: Pubkey,
            config: Config<'a>,
        }

        impl Serialize for SubscribeParams<'_> {
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

        let key = (sub, commitment);
        if self.subs.contains_key(&key) {
            info!(self.actor_id, message = "already trying to subscribe", pubkey = %key.0.key());
            return Ok(());
        }
        let (sub, commitment) = key;

        let request_id = self.next_request_id();

        let (key, method, filters) = match &sub {
            Subscription::Account(key) => (key, "accountSubscribe", None),
            Subscription::Program(key, ref filters) => (key, "programSubscribe", filters.as_ref()),
        };
        info!(
            self.actor_id,
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
                key: *key,
                config: Config {
                    commitment,
                    encoding: Encoding::Base64Zstd,
                    filters,
                },
            },
        };

        let slab_id = self.sub_storage.insert((sub.clone(), commitment));
        self.inflight
            .insert(request_id, (InflightRequest::Sub(slab_id), Instant::now()));
        self.subs
            .insert((sub.clone(), commitment), Meta::new(slab_id));
        self.send(&request)?;
        self.purge_queue
            .as_ref()
            .unwrap()
            .insert(slab_id, self.time_to_live);
        metrics()
            .subscribe_requests
            .with_label_values(&[&self.actor_name])
            .inc();

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

        let subscription = (sub, commitment);
        let meta = self.subs.get(&subscription);
        let sub_id = meta
            .as_ref()
            .map(|meta| self.sub_to_id.get(&meta.slab_id))
            .flatten();

        if let Some((meta, sub_id)) = meta.zip(sub_id) {
            let (sub, commitment) = subscription;
            info!(
                self.actor_id,
                message = "unsubscribe",
                key = %sub.key(),
                commitment = ?commitment,
                request_id = request_id
            );

            let method = match sub {
                Subscription::Program(..) => "programUnsubscribe",
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
                (InflightRequest::Unsub(meta.slab_id), Instant::now()),
            );
            self.send(&request)?;
        }
        Ok(())
    }

    fn purge_key(&mut self, sub: &Subscription, commitment: Commitment) {
        info!(self.actor_id, message = "purge", key = %sub.key(), commitment = ?commitment);
        match sub {
            Subscription::Program(program_key, filter) => {
                for key in self
                    .program_accounts
                    .remove_all(program_key, commitment, filter.clone())
                {
                    self.accounts.remove(&key, commitment)
                }
            }
            Subscription::Account(key) => {
                self.accounts.remove(key, commitment);
            }
        }
    }

    fn update_status(&self) {
        let is_active = self.id_to_sub.len() == self.subs.len() && self.connection.is_connected();
        self.active.store(is_active, Ordering::Relaxed);

        metrics()
            .websocket_connected
            .with_label_values(&[&self.actor_name])
            .set(if self.connection.is_connected() { 1 } else { 0 });

        metrics()
            .websocket_active
            .with_label_values(&[&self.actor_name])
            .set(if is_active { 1 } else { 0 });
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
        let value: AnyMessage<'_> = serde_json::from_slice(text)?;
        match value {
            // subscription error
            AnyMessage {
                error: Some(error),
                id: Some(id),
                ..
            } => {
                if let Some((req, _time)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(slab_id) => {
                            if let Some(key) = self.sub_storage.try_remove(slab_id) {
                                self.subs.remove(&key);
                                let (sub, commitment) = key;

                                warn!(
                                    self.actor_id,
                                    request_id = id,
                                    error = ?error,
                                    key = %sub.key(),
                                    commitment = ?commitment,
                                    "subscribe failed"
                                );
                                metrics()
                                    .subscribe_errors
                                    .with_label_values(&[&self.actor_name])
                                    .inc();

                                if sub.is_account() {
                                    self.active_accounts.remove(&(sub.key(), commitment));
                                }
                                self.purge_key(&sub, commitment);
                            } else {
                                error!(slab_id, "subscribe error to unknown slab_id");
                            }
                        }
                        InflightRequest::Unsub(slab_id) => {
                            if let Some(key) = self.sub_storage.try_remove(slab_id) {
                                // it's unclear if we're subscribed now or not, so remove subscription
                                // *and* key to resubscribe later
                                if let Some(id) = self.sub_to_id.remove(&slab_id) {
                                    self.id_to_sub.remove(&id);
                                }
                                self.subs.remove(&key);
                                let (sub, commitment) = key;
                                warn!(
                                    self.actor_id,
                                    request_id = id,
                                    error = ?error,
                                    key = %sub.key(),
                                    commitment = ?commitment,
                                    "unsubscribe failed"
                                );
                                metrics()
                                    .unsubscribe_errors
                                    .with_label_values(&[&self.actor_name])
                                    .inc();

                                if sub.is_account() {
                                    self.active_accounts.remove(&(sub.key(), commitment));
                                }
                                self.purge_key(&sub, commitment);
                            } else {
                                error!(slab_id, "unsubscribe error to unknown slab_id");
                            }
                        }
                        InflightRequest::SlotSub(_) => {
                            warn!(self.actor_id, request_id = id, error = ?error, "slot subscribe failed");
                        }
                    }
                }
                metrics()
                    .inflight_entries
                    .with_label_values(&[&self.actor_name])
                    .set(self.inflight.len() as i64);
                self.update_status();
            }
            // subscription response
            AnyMessage {
                result: Some(result),
                id: Some(id),
                ..
            } => {
                if let Some((req, sent_at)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(slab_id) => {
                            if let Some((sub, commitment)) = self.sub_storage.get(slab_id) {
                                let sub_id: u64 = serde_json::from_str(result.get())?;
                                self.id_to_sub.insert(sub_id, slab_id);
                                self.sub_to_id.insert(slab_id, sub_id);

                                if let Subscription::Account(key) = sub {
                                    if let Some(key_ref) = self
                                        .accounts
                                        .get(key)
                                        .and_then(|data| data.get_ref(*commitment))
                                    {
                                        self.active_accounts.insert((*key, *commitment), key_ref);
                                    }
                                }

                                metrics()
                                    .id_sub_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.id_to_sub.len() as i64);

                                metrics()
                                    .sub_id_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.sub_to_id.len() as i64);

                                info!(self.actor_id, message = "subscribed to stream",
                                    sub_id = sub_id, sub = %sub, commitment = ?commitment, time = ?sent_at.elapsed());
                                metrics()
                                    .time_to_subscribe
                                    .with_label_values(&[&self.actor_name])
                                    .observe(sent_at.elapsed().as_secs_f64());
                                metrics()
                                    .subscriptions_active
                                    .with_label_values(&[&self.actor_name])
                                    .inc();
                            } else {
                                error!(slab_id, "subcribe result to unknown slab_id");
                            }
                        }
                        InflightRequest::Unsub(slab_id) => {
                            let is_ok: bool = serde_json::from_str(result.get())?;
                            if is_ok {
                                let key = self.sub_storage.try_remove(slab_id);
                                let sub_id = self.sub_to_id.remove(&slab_id);

                                match (key, sub_id) {
                                    (Some(key), Some(sub_id)) => {
                                        self.id_to_sub.remove(&sub_id);
                                        let created_at = self.subs.remove(&key);
                                        let (sub, commitment) = key;

                                        if sub.is_account() {
                                            self.active_accounts.remove(&(sub.key(), commitment));
                                        }
                                        info!(
                                            self.actor_id,
                                            message = "unsubscribed from stream",
                                            sub_id = sub_id,
                                            key = %sub.key(),
                                            sub = %sub,
                                            time = ?sent_at.elapsed(),
                                        );
                                        metrics()
                                            .subscriptions_active
                                            .with_label_values(&[&self.actor_name])
                                            .dec();
                                        if let Some(times) = created_at {
                                            metrics()
                                                .subscription_lifetime
                                                .observe(times.since_creation().as_secs_f64());
                                        }
                                        self.purge_key(&sub, commitment);
                                    }
                                    (Some((sub, commitment)), None) => {
                                        warn!(self.actor_id, sub = %sub, commitment = ?commitment, "unsubscribe for unknown subscription");
                                    }
                                    _ => {
                                        warn!(slab_id, "unsubscribe for unknown subscription");
                                    }
                                }

                                metrics()
                                    .id_sub_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.id_to_sub.len() as i64);

                                metrics()
                                    .sub_id_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.sub_to_id.len() as i64);
                            } else {
                                warn!(self.actor_id, message = "unsubscribe failed", slab_id = %slab_id);
                            }
                        }
                        InflightRequest::SlotSub(_) => {
                            info!(self.actor_id, message = "subscribed to root");
                        }
                    }
                }
                self.update_status();
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
                        let subscription = self
                            .id_to_sub
                            .get(&params.subscription)
                            .map(|id| self.sub_storage.get(*id))
                            .flatten();
                        if let Some((sub, commitment)) = subscription {
                            //info!(key = %sub.key(), "received account notification");
                            self.accounts.insert(sub.key(), params.result, *commitment);
                        } else {
                            warn!(
                                self.actor_id,
                                message = "unknown subscription",
                                sub = params.subscription
                            );
                        }
                        metrics()
                            .notifications_received
                            .with_label_values(&[&self.actor_name, "accountNotification"])
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
                        let sub_storage = &self.sub_storage; // So we can borrow mutably self.subs later
                        let subscription = self
                            .id_to_sub
                            .get(&params.subscription)
                            .map(|id| sub_storage.get(*id))
                            .flatten();

                        if let Some(key) = subscription {
                            if let Some(meta) = self.subs.get_mut(key) {
                                if meta.first_slot.is_none() {
                                    let (key, slot) = (key.0.key(), params.result.context.slot);
                                    info!(program = %key, slot, "first update for program");
                                    meta.first_slot.replace(slot);
                                }
                            }

                            let (program_sub, commitment) = key;
                            let program_key = program_sub.key();
                            let filters = program_sub.filters();
                            let key = params.result.value.pubkey;

                            let key_ref = self.accounts.insert(
                                key,
                                AccountContext {
                                    value: Some(params.result.value.account),
                                    context: params.result.context,
                                },
                                *commitment,
                            );

                            // add() returns false if we don't have an entry
                            // which means we haven't received complete result
                            // from validator by rpc
                            self.program_accounts.add(
                                &program_key,
                                key_ref.clone(),
                                filters.cloned(),
                                *commitment,
                            );
                            // important for proper removal
                            drop(key_ref);
                            self.accounts.remove(&key, *commitment);
                        } else {
                            warn!(
                                self.actor_id,
                                message = "unknown subscription",
                                sub = params.subscription
                            );
                        }
                        metrics()
                            .notifications_received
                            .with_label_values(&[&self.actor_name, "programNotification"])
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
                        warn!(
                            self.actor_id,
                            message = "unknown notification",
                            method = method
                        );
                    }
                }
            }
            any => {
                warn!(self.actor_id, msg = ?any, text = ?text, "unidentified websocket message");
            }
        }

        Ok(())
    }

    fn on_disconnect(&mut self) {
        info!(self.actor_id, "websocket disconnected");

        self.connection = Connection::Disconnected;

        metrics()
            .websocket_disconnects
            .with_label_values(&[&self.actor_name])
            .inc();
        metrics()
            .websocket_connected
            .with_label_values(&[&self.actor_name])
            .set(0);
        metrics()
            .subscriptions_active
            .with_label_values(&[&self.actor_name])
            .set(0);
        self.update_status();

        self.buffer.clear();
        self.inflight.clear();
        self.id_to_sub.clear();
        self.sub_to_id.clear();

        let active_accounts = self.active_accounts.drain().count();
        info!(
            self.actor_id,
            accounts = active_accounts,
            "purging related caches",
        );
        let to_purge: Vec<_> = self.subs.keys().cloned().collect();
        for (sub, commitment) in to_purge {
            self.purge_key(&sub, commitment);
        }
        self.update_status();
    }
}

impl Supervised for AccountUpdateManager {
    fn restarting(&mut self, _ctx: &mut Context<Self>) {
        self.on_disconnect();
        info!(self.actor_id, "restarting actor");
    }
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        let _ = <Self as Handler<AccountCommand>>::handle(self, item, ctx);
    }

    fn finished(&mut self, _ctx: &mut Context<Self>) {
        info!(self.actor_id, "purge stream finished");
    }
}

impl Handler<AccountCommand> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: AccountCommand, _ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            match item {
                AccountCommand::Subscribe(sub, commitment) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "subscribe"])
                        .inc();
                    self.subscribe(sub, commitment)?;
                }
                AccountCommand::Purge(slab_id) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "purge"])
                        .inc();

                    if let Some(subscription) = self.sub_storage.get(slab_id).cloned() {
                        if self.connection.is_connected() {
                            self.unsubscribe(subscription.0.clone(), subscription.1)?;
                        } else {
                            self.subs.remove(&subscription);
                            self.sub_storage.try_remove(slab_id);
                        }

                        let (sub, commitment) = subscription;
                        if sub.is_account() {
                            self.active_accounts.remove(&(sub.key(), commitment));
                        }
                        self.purge_key(&sub, commitment);
                    }
                }
                AccountCommand::Reset(sub, commitment) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "reset"])
                        .inc();
                    let key = (sub, commitment);
                    if let Some(meta) = self.subs.get_mut(&key) {
                        metrics()
                            .time_until_reset
                            .observe(meta.update().as_secs_f64());
                        self.purge_queue
                            .as_ref()
                            .unwrap()
                            .reset(meta.slab_id, self.time_to_live);
                    }
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
                error!(self.actor_id, error = %err, "websocket read error");
                metrics()
                    .websocket_errors
                    .with_label_values(&[&self.actor_name, "read"])
                    .inc();
                ctx.stop();
                return;
            }
        };

        let _ = (|| -> Result<(), serde_json::Error> {
            use awc::ws::Frame;

            match item {
                Frame::Ping(data) => {
                    metrics().bytes_received.with_label_values(&[&self.actor_name]).inc_by(data.len() as u64);
                    if let Connection::Connected { sink, .. } = &mut self.connection {
                        sink.write(awc::ws::Message::Pong(data));
                    }
                }
                Frame::Pong(_) => {
                    // do nothing
                }
                Frame::Text(text) => {
                    metrics().bytes_received.with_label_values(&[&self.actor_name]).inc_by(text.len() as u64);
                    self.process_ws_message(&text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing message");
                            err
                        })?
                }
                Frame::Close(reason) => {
                    warn!(self.actor_id, reason = ?reason, "websocket closing");
                    ctx.stop();
                }
                Frame::Binary(msg) => {
                    warn!(self.actor_id, msg = ?msg, "unexpected binary message");
                }
                Frame::Continuation(msg) => match msg {
                    ws::Item::FirstText(bytes) => {
                        metrics().bytes_received.with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Continue(bytes) => {
                        metrics().bytes_received
                            .with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Last(bytes) => {
                        metrics().bytes_received
                            .with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                        let text = std::mem::replace(&mut self.buffer, BytesMut::new());
                        self.process_ws_message(&text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing fragmented message");
                            err
                        })?;
                    }
                    ws::Item::FirstBinary(_) => {
                        warn!(self.actor_id, msg = ?msg, "unexpected continuation message");
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
        metrics()
            .inflight_entries
            .with_label_values(&[&self.actor_name])
            .set(self.inflight.len() as i64);

        let _ = self.send(&request);

        // restore subscriptions
        info!(self.actor_id, "adding subscriptions");
        let subs_len = self.subs.len();
        let subs = std::mem::replace(&mut self.subs, HashMap::with_capacity(subs_len));

        for ((sub, commitment), _) in subs {
            self.subscribe(sub, commitment).unwrap()
            // TODO: it would be nice to retrieve current state for
            // everything we had before
        }
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        info!(self.actor_id, "websocket stream finished");
        ctx.stop();
    }
}

impl Actor for AccountUpdateManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let (handle, stream) = delay_queue(self.actor_name.clone());
        let purge_stream = stream.map(AccountCommand::Purge);

        ctx.set_mailbox_capacity(MAILBOX_CAPACITY);

        Self::start_periodic(ctx);
        self.purge_queue = Some(handle);

        AccountUpdateManager::add_stream(purge_stream, ctx);

        self.connect(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        info!(self.actor_id, "pubsub actor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!(self.actor_id, "pubsub actor stopped");
    }
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {
    fn error(&mut self, err: awc::error::WsProtocolError, _ctx: &mut Self::Context) -> Running {
        error!(self.actor_id, message = "websocket write error", error = ?err);
        metrics()
            .websocket_errors
            .with_label_values(&[&self.actor_name, "write"])
            .inc();
        Running::Stop
    }

    fn finished(&mut self, _ctx: &mut Self::Context) {
        info!(self.actor_id, "writer closed");
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
#[allow(clippy::large_enum_variant)] // FIXME!!!
pub(crate) enum Subscription {
    Account(Pubkey),
    Program(Pubkey, Option<SmallVec<[Filter; 2]>>),
}

impl Subscription {
    pub fn key(&self) -> Pubkey {
        match *self {
            Subscription::Account(key) => key,
            Subscription::Program(key, ..) => key,
        }
    }

    pub fn is_account(&self) -> bool {
        matches!(self, Subscription::Account(_))
    }

    pub fn filters(&self) -> Option<&SmallVec<[Filter; 2]>> {
        match self {
            Subscription::Program(_, filters) => filters.as_ref(),
            _ => None,
        }
    }
}

impl std::fmt::Display for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, key) = match self {
            Subscription::Account(key) => ("Account", key),
            Subscription::Program(key, ..) => ("Program", key),
        };
        // TODO: add filters
        write!(f, "{}({})", prefix, key)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) enum AccountCommand {
    Subscribe(Subscription, Commitment),
    Reset(Subscription, Commitment),
    Purge(SlabId),
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

fn delay_queue<T: Clone + std::hash::Hash + Eq>(
    id: String,
) -> (DelayQueueHandle<T>, impl Stream<Item = T>) {
    let (sender, incoming) = mpsc::unbounded_channel::<DelayQueueCommand<T>>();
    let mut map: HashMap<T, _> = HashMap::default();
    let stream = stream_generator::generate_stream(|mut stream| async move {
        let mut delay_queue = DelayQueue::new();
        tokio::pin!(incoming);

        loop {
            metrics()
                .purge_queue_length
                .with_label_values(&[&id])
                .set(delay_queue.len() as i64);
            metrics()
                .purge_queue_entries
                .with_label_values(&[&id])
                .set(map.len() as i64);
            tokio::select! {
                item = incoming.next() => {
                    if let Some(item) = item {
                        match item {
                            DelayQueueCommand::Insert(item, time) | DelayQueueCommand::Reset(item, time) => {
                                if let Some(key) = map.get(&item) {
                                    delay_queue.reset_at(key, time);
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
