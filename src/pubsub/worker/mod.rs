use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, sync::atomic::AtomicBool};

use actix::io::SinkWrite;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, Arbiter, AsyncContext, Context, Handler, Running,
    SpawnHandle, Supervised, Supervisor,
};
use actix_http::ws as actixws;
use bytes::BytesMut;
use futures_util::StreamExt;
use serde::Serialize;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::filter::Filters;
use crate::metrics::pubsub_metrics as metrics;
use crate::types::Encoding;
use crate::{
    filter::FilterTree,
    types::{AccountsDb, Commitment, ProgramAccountsDb, Pubkey, Slot},
};

use self::autocmd::AccountCommand;
use self::ws::{Connection, InflightRequest};

use super::delay::delay_queue;
use super::manager::{InitManager, WorkerConfig};
use super::subscription::Subscription;
use super::{delay::DelayQueueHandle, manager::PubSubManager};

const MAILBOX_CAPACITY: usize = 512;
const DEAD_REQUEST_LIMIT: usize = 0;
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(60);
const WEBSOCKET_PING_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct ProgramFilters {
    filtered: HashMap<(Pubkey, Commitment), FilterTree<SpawnHandle>>,
    nofilters: HashMap<(Pubkey, Commitment), SpawnHandle>,
}

struct Meta {
    created_at: Instant,
    updated_at: Instant,
    first_slot: Option<Slot>,
}

pub struct AccountUpdateManager {
    actor_id: u32,
    actor_name: String,
    request_id: u64,
    inflight: HashMap<u64, (InflightRequest, Instant)>,
    subs: HashMap<(Subscription, Commitment), Meta>,
    active_accounts: HashMap<(Pubkey, Commitment), Arc<Pubkey>>,
    pub(super) id_to_sub: HashMap<u64, (Subscription, Commitment)>,
    pub(super) sub_to_id: HashMap<(Subscription, Commitment), u64>,
    connection: Connection,
    accounts: AccountsDb,
    program_accounts: ProgramAccountsDb,
    purge_queue: Option<DelayQueueHandle<(Subscription, Commitment)>>,
    additional_keys: ProgramFilters,
    last_received_at: Instant,
    connected: Arc<AtomicBool>,
    rpc_slot: Arc<AtomicU64>,
    buffer: BytesMut,
    config: Arc<WorkerConfig>,
    manager: Option<Addr<PubSubManager>>,
}

impl Meta {
    fn new() -> Self {
        let now = Instant::now();
        Meta {
            created_at: now,
            updated_at: now,
            first_slot: None,
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

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl ProgramFilters {
    fn len(&self) -> usize {
        self.filtered.len() + self.nofilters.len()
    }

    fn get(&self, key: &(Pubkey, Commitment)) -> Option<&FilterTree<SpawnHandle>> {
        self.filtered.get(key)
    }

    fn insert(
        &mut self,
        key: (Pubkey, Commitment),
        filters: Option<Filters>,
        handle: SpawnHandle,
    ) -> Option<SpawnHandle> {
        match filters {
            Some(filters) => self
                .filtered
                .entry(key)
                .or_insert_with(FilterTree::new)
                .insert(filters, handle),
            None => self.nofilters.insert(key, handle),
        }
    }

    fn remove(
        &mut self,
        key: &(Pubkey, Commitment),
        filters: &Option<Filters>,
    ) -> Option<SpawnHandle> {
        if let Some(filters) = filters {
            self.filtered
                .get_mut(key)
                .map(|entry| entry.remove(filters))
                .flatten()
        } else {
            self.nofilters.remove(key)
        }
    }
    fn remove_all(&mut self, key: &(Pubkey, Commitment)) -> impl Iterator<Item = SpawnHandle> {
        self.filtered
            .remove(key)
            .into_iter()
            .flatten()
            .map(|(_, h)| h)
            .chain(self.nofilters.remove(key).into_iter())
    }
}

impl AccountUpdateManager {
    pub fn init(
        actor_id: u32,
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        connected: Arc<AtomicBool>,
        rpc_slot: Arc<AtomicU64>,
        config: Arc<WorkerConfig>,
    ) -> Addr<Self> {
        let arbiter = Arbiter::new();
        let actor_name = format!("pubsub-{}", actor_id);
        Supervisor::start_in_arbiter(&arbiter.handle(), move |_ctx| AccountUpdateManager {
            actor_id,
            actor_name,
            connection: Connection::Disconnected,
            id_to_sub: HashMap::default(),
            sub_to_id: HashMap::default(),
            inflight: HashMap::default(),
            subs: HashMap::default(),
            active_accounts: HashMap::default(),
            request_id: 1,
            accounts: accounts.clone(),
            program_accounts,
            purge_queue: None,
            additional_keys: ProgramFilters::default(),
            connected,
            rpc_slot,
            last_received_at: Instant::now(),
            buffer: BytesMut::new(),
            config,
            manager: None,
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

    fn check_slot(&self, slot: u64, ctx: &mut Context<Self>) {
        let rpc_slot = self.rpc_slot.load(Ordering::Relaxed);
        let behind = rpc_slot
            .checked_sub(slot)
            .map(|diff| diff > self.config.slot_distance as u64)
            .unwrap_or(false);
        if behind {
            error!("websocket slot behind rpc, stopping");
            ctx.stop()
        }
    }

    fn send<T: Serialize>(&mut self, request: &T) -> Result<(), serde_json::Error> {
        if self.connection.is_connected() {
            let _ = self.connection.send(awc::ws::Message::Text(
                serde_json::to_string(request)?.into(),
            ));
        } else {
            warn!(self.actor_id, "not connected");
        }
        Ok(())
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        use actix::fut::WrapFuture;
        use backoff::backoff::Backoff;

        let websocket_url = self.config.websocket_url.clone();
        let actor_id = self.actor_id;

        if self.connection.is_connected() {
            warn!(message = "old connection not canceled properly", actor_id = %actor_id);
            return;
        }
        self.connection = Connection::Connecting;

        self.update_status();

        let actor_name = self.actor_name.clone();

        let fut = async move {
            let mut backoff = backoff::ExponentialBackoff {
                current_interval: Duration::from_millis(300),
                initial_interval: Duration::from_millis(300),
                max_interval: Duration::from_secs(10),
                max_elapsed_time: None, // will never terminate
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
                        metrics()
                            .websocket_reconnects
                            .with_label_values(&[&actor_name])
                            .inc();
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        };
        let fut = fut.into_actor(self).map(|conn, actor, ctx| {
            let (sink, stream) = StreamExt::split(conn);
            let actor_id = actor.actor_id;
            let mut sink = SinkWrite::new(sink, ctx);

            info!(actor_id, message = "websocket sending ping");
            if sink
                .write(actixws::Message::Ping(b"check connection".as_ref().into()))
                .is_err()
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

            ctx.add_stream(stream);
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
            params: SubscribeParams,
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

        if self.subs.get(&(sub, commitment)).is_some() {
            info!(self.actor_id, message = "already trying to subscribe", pubkey = %sub.key());
            return Ok(());
        }

        let request_id = self.next_request_id();

        let (key, method) = match sub {
            Subscription::Account(key) => (key, "accountSubscribe"),
            Subscription::Program(key) => (key, "programSubscribe"),
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
        self.subs.insert((sub, commitment), Meta::new());
        self.send(&request)?;

        self.purge_queue
            .as_ref()
            .unwrap()
            .insert((sub, commitment), self.config.ttl);
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

        if let Some(sub_id) = self.sub_to_id.get(&(sub, commitment)) {
            info!(self.actor_id, message = "unsubscribe", key = %sub.key(), commitment = ?commitment, request_id = request_id);

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
        Ok(())
    }

    fn reset_filter(
        &mut self,
        ctx: &mut Context<Self>,
        sub: Subscription,
        commitment: Commitment,
        filters: Option<Filters>,
    ) {
        let key = sub.key();
        let to_purge = filters.clone();
        let spawn_handle = ctx.run_later(self.config.ttl, move |actor, ctx| {
            actor.purge_filter(ctx, sub, commitment, to_purge);
        });

        let handle = self
            .additional_keys
            .insert((key, commitment), filters, spawn_handle);
        metrics()
            .additional_keys_entries
            .with_label_values(&[&self.actor_name])
            .set(self.additional_keys.len() as i64);
        // reset ttl for main key
        self.purge_queue
            .as_ref()
            .unwrap()
            .reset((sub, commitment), self.config.ttl);
        match handle {
            Some(handle) => {
                ctx.cancel_future(handle);
            }
            None => {
                debug!(key = %sub.key(), "filter added");
                metrics()
                    .filters
                    .with_label_values(&[&self.actor_name])
                    .inc();
            }
        }
    }

    fn purge_filter(
        &mut self,
        ctx: &mut Context<Self>,
        sub: Subscription,
        commitment: Commitment,
        filters: Option<Filters>,
    ) {
        let key = sub.key();
        let handle = self.additional_keys.remove(&(key, commitment), &filters);

        if let Some(handle) = handle {
            info!(key = %sub.key(), commitment = ?commitment, filter = ?filters, "purging filters");
            metrics()
                .filters
                .with_label_values(&[&self.actor_name])
                .dec();

            ctx.cancel_future(handle);
        }
        let accounts_for_filter = self
            .program_accounts
            .remove_keys_for_filter(&(key, commitment), filters);
        for arc in accounts_for_filter {
            let key = *arc;
            drop(arc);
            self.accounts.remove(&key, commitment)
        }
    }

    fn purge_key(&mut self, ctx: &mut Context<Self>, sub: &Subscription, commitment: Commitment) {
        info!(self.actor_id, message = "purge", key = %sub.key(), commitment = ?commitment);
        match sub {
            Subscription::Program(program_key) => {
                let filters_count = self
                    .additional_keys
                    .remove_all(&(*program_key, commitment))
                    .map(|handle| {
                        ctx.cancel_future(handle);
                    })
                    .count();
                metrics()
                    .filters
                    .with_label_values(&[&self.actor_name])
                    .sub(filters_count as i64);

                let program_key = &(*program_key, commitment);
                let accounts_to_remove = self.program_accounts.remove_all(program_key);
                // cleanup all child accounts, that were kept in AccountsDb,
                // while program subscription was active
                for arc in accounts_to_remove {
                    let acc = *arc;
                    // now ref in AccountsDb should be the only one left
                    drop(arc);
                    self.accounts.remove(&acc, commitment);
                }
                metrics()
                    .additional_keys_entries
                    .with_label_values(&[&self.actor_name])
                    .set(self.additional_keys.len() as i64);
            }
            Subscription::Account(key) => {
                let data = self
                    .accounts
                    .get(key)
                    .and_then(|state| state.get_ref(commitment))
                    .map(|acc_ref| (acc_ref, self.accounts.get_owner(key, commitment)));
                match data {
                    Some((acc_ref, Some(pubkey))) => {
                        let program_key = (pubkey, commitment);
                        self.program_accounts
                            .untrack_account_key(&program_key, acc_ref);
                    }
                    Some((acc_ref, None)) => {
                        warn!("empty account, reference exists without actual data");
                        drop(acc_ref);
                    }
                    None => (),
                }
                self.accounts.remove(key, commitment);
            }
        }
    }

    fn update_status(&self) {
        let connected = self.connection.is_connected();
        let active = self.id_to_sub.len() == self.subs.len() && connected;

        self.connected.store(connected, Ordering::Relaxed);

        metrics()
            .websocket_connected
            .with_label_values(&[&self.actor_name])
            .set(if connected { 1 } else { 0 });

        metrics()
            .websocket_active
            .with_label_values(&[&self.actor_name])
            .set(if active { 1 } else { 0 });
    }

    fn on_disconnect(&mut self, ctx: &mut Context<Self>) {
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
            self.purge_key(ctx, &sub, commitment);
        }
        self.update_status();
    }
}

impl Supervised for AccountUpdateManager {
    fn restarting(&mut self, ctx: &mut Context<Self>) {
        self.on_disconnect(ctx);
        info!(self.actor_id, "restarting actor");
    }
}

impl Handler<InitManager> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: InitManager, _: &mut Context<Self>) {
        self.manager.replace(item.0);
    }
}

impl Actor for AccountUpdateManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let (handle, stream) = delay_queue(self.actor_name.clone());
        let purge_stream = stream.map(|(sub, com)| AccountCommand::Purge(sub, com));

        ctx.set_mailbox_capacity(MAILBOX_CAPACITY);

        Self::start_periodic(ctx);
        self.purge_queue = Some(handle);

        ctx.add_stream(purge_stream);

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

pub(crate) mod autocmd;
pub(super) mod ws;
