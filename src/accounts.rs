use std::collections::{HashMap, HashSet};
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use actix::io::SinkWrite;
use actix::prelude::{Actor, Addr, Context, Handler, Message, StreamHandler};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{DelayQueue, Instant};
use tracing::{error, info};

use crate::types::{AccountContext, AccountInfo, Pubkey};

const PURGE_TIMEOUT: Duration = Duration::from_secs(600);

enum InflightRequest {
    Sub(Pubkey),
    Unsub(Pubkey),
    SlotSub(u64),
}

pub(crate) struct AccountUpdateManager {
    request_id: u64,
    inflight: HashMap<u64, InflightRequest>,
    subs: HashSet<Pubkey>,
    sub_to_key: HashMap<u64, Pubkey>,
    key_to_sub: HashMap<Pubkey, u64>,
    sink: SinkWrite<
        awc::ws::Message,
        futures_util::stream::SplitSink<
            actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
            awc::ws::Message,
        >,
    >,
    accounts_map: Arc<DashMap<Pubkey, Option<AccountInfo>>>,
    purge_queue: DelayQueueHandle<Pubkey>,
    slot: Arc<AtomicU64>,
}

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl AccountUpdateManager {
    pub fn init(
        current_slot: Arc<AtomicU64>,
        accounts_map: Arc<DashMap<Pubkey, Option<AccountInfo>>>,
        conn: actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
    ) -> Addr<Self> {
        AccountUpdateManager::create(|ctx| {
            let (handle, stream) = delay_queue();
            let purge_stream = stream.map(|item| AccountCommand::Purge(item));

            let (sink, stream) = futures_util::stream::StreamExt::split(conn);
            let (sink, stream) = (sink, stream.filter_map(Result::ok));
            let sink = SinkWrite::new(sink, ctx);
            AccountUpdateManager::add_stream(stream, ctx);
            AccountUpdateManager::add_stream(purge_stream, ctx);
            AccountUpdateManager {
                sink,
                sub_to_key: HashMap::default(),
                key_to_sub: HashMap::default(),
                inflight: HashMap::default(),
                subs: HashSet::default(),
                request_id: 1,
                accounts_map: accounts_map.clone(),
                purge_queue: handle,
                slot: current_slot.clone(),
            }
        })
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.request_id;
        self.request_id += 1;
        request_id
    }
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        let _ = <Self as Handler<AccountCommand>>::handle(self, item, ctx);
    }
}

impl Handler<AccountCommand> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: AccountCommand, _ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            let request_id = self.next_request_id();
            match item {
                AccountCommand::Subscribe(key) => {
                    if self.subs.contains(&key) {
                        info!("already trying to subscribe to {}", key);
                        return Ok(());
                    }

                    info!("subscribe to {}", key);

                    #[derive(Serialize)]
                    struct Request<'a> {
                        jsonrpc: &'a str,
                        id: u64,
                        method: &'a str,
                        params: [Pubkey; 1],
                    }
                    let request = Request {
                        jsonrpc: "2.0",
                        id: request_id,
                        method: "accountSubscribe",
                        params: [key],
                    };

                    self.inflight.insert(request_id, InflightRequest::Sub(key));
                    self.subs.insert(key);
                    self.sink
                        .write(awc::ws::Message::Text(serde_json::to_string(&request)?));
                    self.purge_queue.insert(key, PURGE_TIMEOUT);
                }
                AccountCommand::Purge(key) => {
                    info!("purging {}", key);

                    #[derive(Serialize)]
                    struct Request<'a> {
                        jsonrpc: &'a str,
                        id: u64,
                        method: &'a str,
                        params: [u64; 1],
                    }

                    if let Some(sub_id) = self.key_to_sub.get(&key) {
                        let request = Request {
                            jsonrpc: "2.0",
                            id: request_id,
                            method: "accountUnsubscribe",
                            params: [*sub_id],
                        };
                        self.inflight
                            .insert(request_id, InflightRequest::Unsub(key));
                        self.sink
                            .write(awc::ws::Message::Text(serde_json::to_string(&request)?));
                    }
                    self.accounts_map.remove(&key);
                }
                AccountCommand::Reset(key) => {
                    self.purge_queue.reset(key, PURGE_TIMEOUT);
                }
            }
            Ok(())
        })()
        .map_err(|err| {
            error!("error handling AccountCommand: {}", err);
        });
    }
}

impl StreamHandler<awc::ws::Frame> for AccountUpdateManager {
    fn handle(&mut self, item: awc::ws::Frame, _ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            use awc::ws::Frame;
            match item {
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
                    let value: AnyMessage = serde_json::from_slice(&text)?;
                    // subscription response
                    if let (Some(result), Some(id)) = (value.result, value.id) {
                        if let Some(req) = self.inflight.remove(&id) {
                            match req {
                                InflightRequest::Sub(key) => {
                                    let sub_id: u64 = serde_json::from_str(result.get())?;
                                    self.sub_to_key.insert(sub_id, key);
                                    self.key_to_sub.insert(key, sub_id);
                                    info!(message = "subscribed to stream", sub = sub_id, key = %key);
                                }
                                InflightRequest::Unsub(key) => {
                                    //let _is_ok: bool = serde_json::from_str(result.get()).unwrap();
                                    if let Some(sub) = self.key_to_sub.remove(&key) {
                                        self.subs.remove(&key);
                                        self.sub_to_key.remove(&sub);
                                        info!(
                                            message = "unsubscribed from stream",
                                            sub = sub,
                                            key = %key,
                                        );
                                    }
                                }
                                InflightRequest::SlotSub(_) => {
                                    info!(message = "subscribed to slot");
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
                                #[derive(Deserialize)]
                                struct Params {
                                    result: AccountContext,
                                    subscription: u64,
                                }
                                let params: Params = serde_json::from_str(params.get())?;
                                if let Some(key) = self.sub_to_key.get(&params.subscription) {
                                    self.accounts_map.insert(*key, params.result.value);
                                }
                            }
                            "slotNotification" => {
                                #[derive(Deserialize)]
                                struct SlotInfo {
                                    //slot: u64,
                                    root: u64,
                                    //parent: u64,
                                }
                                #[derive(Deserialize)]
                                struct Params {
                                    result: SlotInfo,
                                }
                                let params: Params = serde_json::from_str(params.get())?;
                                //info!("slot {} root {} parent {}", params.result.slot, params.result.root, params.result.parent);
                                let slot = params.result.root - 1; // TODO: figure out which slot validator *actually* reports
                                self.slot.store(slot, atomic::Ordering::SeqCst);
                            }
                            _ => {}
                        }
                    }
                }
                _ => return Ok(()),
            }
            Ok(())
        })().map_err(|err| {
            error!("error handling Frame: {}", err);
        });
    }
}

impl Actor for AccountUpdateManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // subscribe to slots
        let request_id = self.next_request_id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "slotSubscribe",
        });
        self.inflight
            .insert(request_id, InflightRequest::SlotSub(request_id));
        self.sink.write(awc::ws::Message::Text(
            serde_json::to_string(&request).unwrap(),
        ));
    }
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) enum AccountCommand {
    Subscribe(Pubkey),
    Reset(Pubkey),
    Purge(Pubkey),
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
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                out = delay_queue.next(), if !delay_queue.is_empty() => {
                    if let Some(Ok(out)) = out {
                        stream.send(out.into_inner()).await;
                    }
                }
            }
        }
    });
    (DelayQueueHandle(sender), stream)
}
