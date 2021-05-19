use std::collections::{HashMap, HashSet};
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

use crate::types::{
    AccountContext, AccountInfo, AccountsDb, Commitment, Encoding, Pubkey, SolanaContext,
};

const PURGE_TIMEOUT: Duration = Duration::from_secs(600);

enum InflightRequest {
    Sub(Subscription, Commitment),
    Unsub(Subscription),
    SlotSub(u64),
}

pub(crate) struct AccountUpdateManager {
    request_id: u64,
    inflight: HashMap<u64, InflightRequest>,
    subs: HashSet<(Subscription, Commitment)>,
    id_to_sub: HashMap<u64, (Subscription, Commitment)>,
    sub_to_id: HashMap<Subscription, u64>,
    sink: SinkWrite<
        awc::ws::Message,
        futures_util::stream::SplitSink<
            actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
            awc::ws::Message,
        >,
    >,
    accounts: AccountsDb,
    program_accounts: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
    purge_queue: DelayQueueHandle<Subscription>,
}

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl AccountUpdateManager {
    pub fn init(
        accounts: AccountsDb,
        program_accounts: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
        conn: actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
    ) -> Addr<Self> {
        AccountUpdateManager::create(|ctx| {
            let (handle, stream) = delay_queue();
            let purge_stream = stream.map(AccountCommand::Purge);

            let (sink, stream) = futures_util::stream::StreamExt::split(conn);
            let (sink, stream) = (sink, stream.filter_map(Result::ok));
            let sink = SinkWrite::new(sink, ctx);
            AccountUpdateManager::add_stream(stream, ctx);
            AccountUpdateManager::add_stream(purge_stream, ctx);
            AccountUpdateManager {
                sink,
                id_to_sub: HashMap::default(),
                sub_to_id: HashMap::default(),
                inflight: HashMap::default(),
                subs: HashSet::default(),
                request_id: 1,
                accounts: accounts.clone(),
                program_accounts: program_accounts.clone(),
                purge_queue: handle,
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
                AccountCommand::Subscribe(sub, commitment) => {
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

                    let (key, method) = match sub {
                        Subscription::Account(key) => (key, "accountSubscribe"),
                        Subscription::Program(key) => (key, "programSubscribe"),
                    };
                    if self.subs.contains(&(sub, commitment)) {
                        info!("already trying to subscribe to {}", key);
                        return Ok(());
                    }

                    info!("subscribe to {} ({}/{:?})", key, method, commitment);

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
                    self.sink
                        .write(awc::ws::Message::Text(serde_json::to_string(&request)?));
                    self.purge_queue.insert(sub, PURGE_TIMEOUT);
                }
                AccountCommand::Purge(sub) => {
                    info!("purging {}", sub);

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
                        self.sink
                            .write(awc::ws::Message::Text(serde_json::to_string(&request)?));
                    }
                    match sub {
                        Subscription::Program(key) => {
                            self.program_accounts.remove(&key);
                        }
                        Subscription::Account(key) => {
                            self.accounts.remove(&key);
                        }
                    }
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
                    let value: AnyMessage<'_> = serde_json::from_slice(&text)?;
                    // subscription response
                    if let (Some(result), Some(id)) = (value.result, value.id) {
                        if let Some(req) = self.inflight.remove(&id) {
                            match req {
                                InflightRequest::Sub(sub, commitment) => {
                                    let sub_id: u64 = serde_json::from_str(result.get())?;
                                    self.id_to_sub.insert(sub_id, (sub, commitment));
                                    self.sub_to_id.insert(sub, sub_id);
                                    info!(message = "subscribed to stream", sub_id = sub_id, sub = %sub);
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
                                }
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
                                    let key = params.result.value.pubkey;
                                    self.accounts.insert(key, AccountContext {
                                        value: Some(params.result.value.account), context: params.result.context }, *commitment);
                                    if let Some(mut keys) = self.program_accounts.get_mut(&program_sub.key()) {
                                        keys.insert(params.result.value.pubkey);
                                    }
                                }
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
            "method": "rootSubscribe",
        });
        self.inflight
            .insert(request_id, InflightRequest::SlotSub(request_id));
        self.sink.write(awc::ws::Message::Text(
            serde_json::to_string(&request).unwrap(),
        ));
    }
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {}

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
    Subscribe(Subscription, Commitment),
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
