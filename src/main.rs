use std::collections::HashMap;
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use actix::io::SinkWrite;
use actix::prelude::{Actor, Addr, Context, Handler, Message, StreamHandler};
use actix_web::{web, App, Error, HttpResponse, HttpServer};

use awc::Client;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{DelayQueue, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};

use tracing::{error, info};
use tracing_subscriber;

use structopt::StructOpt;

const PURGE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
struct Pubkey([u8; 32]);

impl std::fmt::Display for Pubkey {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str(&bs58::encode(&self.0).into_string())
    }
}

impl Serialize for Pubkey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let mut buf = [0; 64];
        let len = bs58::encode(self.0)
            .into(&mut buf[..])
            .map_err(|_| Error::custom("can't b58encode"))?;
        serializer.serialize_str(unsafe { std::str::from_utf8_unchecked(&buf[..len]) })
    }
}

impl<'de> Deserialize<'de> for Pubkey {
    fn deserialize<D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PubkeyVisitor;
        impl<'de> serde::de::Visitor<'de> for PubkeyVisitor {
            type Value = Pubkey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use serde::de::Error;
                let mut buf = [0; 32];
                bs58::decode(v)
                    .into(&mut buf)
                    .map_err(|_| Error::custom("can't b58decode"))?;
                Ok(Pubkey(buf))
            }
        }

        deserializer.deserialize_str(PubkeyVisitor)
    }
}

#[derive(Debug)]
struct AccountData {
    data: Bytes,
}

impl<'de> Deserialize<'de> for AccountData {
    fn deserialize<D>(deserializer: D) -> Result<AccountData, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AccountDataVisitor;

        impl<'de> serde::de::Visitor<'de> for AccountDataVisitor {
            type Value = AccountData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("[]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let err = || serde::de::Error::custom("can't decode");
                let dat: &str = seq.next_element()?.ok_or_else(|| todo!())?;
                let encoding: &str = seq.next_element()?.ok_or_else(|| todo!())?;
                let data = match encoding {
                    "base58" => bs58::decode(&dat).into_vec().map_err(|_| err())?,
                    "base64" => base64::decode(&dat).map_err(|_| err())?,
                    "base64+zstd" => {
                        let vec = base64::decode(&dat).map_err(|_| err())?;
                        zstd::decode_all(std::io::Cursor::new(vec)).map_err(|_| err())?
                    }
                    _ => return Err(serde::de::Error::custom("unsupported encoding")),
                };
                Ok(AccountData {
                    data: Bytes::from(data),
                })
            }
        }
        deserializer.deserialize_seq(AccountDataVisitor)
    }
}

impl Serialize for AccountData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&bs58::encode(&self.data).into_string())?;
        seq.serialize_element("base58")?;
        seq.end()
    }
}

#[test]
fn data() {
    let dat = r#"["2UzHM","base58"]"#;
    let data: AccountData = serde_json::from_str(&dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn kek() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let data: AccountInfo = serde_json::from_str(&dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn pooq() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let val: serde_json::Value = serde_json::from_str(&dat).unwrap();
    let data: AccountInfo = serde_json::from_value(val).unwrap();
    println!("{:?}", data);
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone)]
enum Encoding {
    #[serde(rename = "base58")]
    Base58,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

impl Encoding {
    fn with_account_data(self, data: &'_ AccountData) -> EncodedAccountData<'_> {
        EncodedAccountData {
            encoding: self,
            data,
            slice: None,
        }
    }
}

#[derive(Debug)]
struct EncodedAccountData<'a> {
    encoding: Encoding,
    data: &'a AccountData,
    slice: Option<Slice>,
}

impl EncodedAccountData<'_> {
    fn slice(self, slice: Option<Slice>) -> Self {
        EncodedAccountData {
            encoding: self.encoding,
            data: self.data,
            slice,
        }
    }
}

impl<'a> Serialize for EncodedAccountData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};
        let mut seq = serializer.serialize_seq(Some(2))?;
        let data = if let Some(slice) = &self.slice {
            self.data
                .data
                .get(slice.offset..slice.offset + slice.length)
                .ok_or(Error::custom("bad slice"))?
        } else {
            &self.data.data[..]
        };
        match self.encoding {
            Encoding::Base58 => {
                seq.serialize_element(&bs58::encode(&data).into_string())?;
            }
            Encoding::Base64 => {
                seq.serialize_element(&base64::encode(&data))?;
            }
            Encoding::Base64Zstd => {
                seq.serialize_element(&base64::encode(
                    zstd::encode_all(std::io::Cursor::new(&data), 0)
                        .map_err(|_| Error::custom("can't compress"))?,
                ))?;
            }
        }
        seq.serialize_element(&self.encoding)?;
        seq.end()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    lamports: u64,
    data: AccountData,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct EncodedAccountInfo<'a> {
    lamports: u64,
    data: EncodedAccountData<'a>,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
}

impl<'a> EncodedAccountInfo<'a> {
    fn with_context(self, ctx: &'a SolanaContext) -> EncodedAccountContext<'a> {
        EncodedAccountContext {
            value: self,
            context: ctx,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Slice {
    offset: usize,
    length: usize,
}

impl AccountInfo {
    fn encode(&self, encoding: Encoding, slice: Option<Slice>) -> EncodedAccountInfo {
        EncodedAccountInfo {
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data: encoding.with_account_data(&self.data).slice(slice),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SolanaContext {
    slot: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct AccountContext {
    context: SolanaContext,
    value: AccountInfo,
}

#[derive(Serialize, Debug)]
struct EncodedAccountContext<'a> {
    context: &'a SolanaContext,
    value: EncodedAccountInfo<'a>,
}

#[derive(Clone)]
struct State {
    map: Arc<DashMap<Pubkey, AccountContext>>,
    client: Client,
    tx: Addr<AccountUpdateManager>,
    rpc_url: String,
    slot: Arc<AtomicU64>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("State{}")
    }
}

impl State {
    fn get(&self, key: &Pubkey) -> Option<dashmap::mapref::one::Ref<'_, Pubkey, AccountContext>> {
        let tx = &self.tx;
        self.map.get(key).map(|v| {
            tx.do_send(AccountCommand::Reset(*key));
            v
        })
    }
}

async fn handler(body: Bytes, app_state: web::Data<State>) -> Result<HttpResponse, Error> {
    #[derive(Deserialize, Serialize, Debug)]
    struct Request<'a> {
        jsonrpc: &'a str,
        id: u64,
        method: &'a str,
        #[serde(borrow)]
        params: [&'a RawValue; 2],
    }
    let req: Request = serde_json::from_slice(&body)?;

    let mut cacheable_for_key = None;

    match req.method.as_ref() {
        "getAccountInfo" => {
            #[derive(Deserialize, Debug)]
            struct Config<'a> {
                encoding: Encoding,
                commitment: Option<&'a str>,
                #[serde(rename = "dataSlice")]
                data_slice: Option<Slice>,
            }
            let pubkey: Pubkey = serde_json::from_str(req.params[0].get())?;
            let config: Config = serde_json::from_str(req.params[1].get())?;

            match app_state.get(&pubkey) {
                Some(data) => {
                    let data = data.value();
                    #[derive(Serialize)]
                    struct Resp<'a> {
                        jsonrpc: &'a str,
                        result: EncodedAccountContext<'a>, //AccountContext,
                        id: u64,
                    }
                    let resp = Resp {
                        jsonrpc: "2.0",
                        result: data
                            .value
                            .encode(config.encoding, config.data_slice)
                            .with_context(&data.context),
                        id: req.id,
                    };

                    info!("cache hit for {}", pubkey);
                    return Ok(HttpResponse::Ok()
                        .content_type("application/json")
                        .json(&resp));
                }
                None => {
                    if config.data_slice.is_none() {
                        cacheable_for_key = Some(pubkey);
                    }
                    app_state
                        .tx
                        .send(AccountCommand::Subscribe(pubkey))
                        .await
                        .unwrap();
                }
            }
        }
        _ => {}
    }

    let client = &app_state.client;
    let mut resp = client
        .post(&app_state.rpc_url)
        .send_json(&req)
        .await
        .unwrap();
    let resp = resp.body().await?;

    if let Some(pubkey) = cacheable_for_key {
        #[derive(Deserialize)]
        struct Resp {
            result: AccountContext,
        }
        let info: Resp = serde_json::from_slice(&resp)?;
        app_state.map.insert(pubkey, info.result);
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

enum InflightRequest {
    Sub(Pubkey),
    Unsub(Pubkey),
    SlotSub(u64),
}

struct AccountUpdateManager {
    request_id: u64,
    inflight: HashMap<u64, InflightRequest>,
    sub_to_key: HashMap<u64, Pubkey>,
    key_to_sub: HashMap<Pubkey, u64>,
    sink: SinkWrite<
        awc::ws::Message,
        futures_util::stream::SplitSink<
            actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
            awc::ws::Message,
        >,
    >,
    map: Arc<DashMap<Pubkey, AccountContext>>,
    purge_queue: DelayQueueHandle<Pubkey>,
    slot: Arc<AtomicU64>,
}

impl std::fmt::Debug for AccountUpdateManager {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str("AccountUpdateManager{}")
    }
}

impl AccountUpdateManager {
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
                    self.map.remove(&key);
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
                                    self.map.insert(*key, params.result);
                                }
                            }
                            "slotNotification" => {
                                #[derive(Deserialize)]
                                struct SlotInfo {
                                    slot: u64,
                                }
                                #[derive(Deserialize)]
                                struct Params {
                                    result: SlotInfo,
                                }
                                let params: Params = serde_json::from_str(params.get())?;
                                let slot = params.result.slot;
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
enum AccountCommand {
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

#[derive(Debug, structopt::StructOpt)]
struct Options {
    #[structopt(
        short = "w",
        long = "websocket-url",
        default_value = "wss://solana-api.projectserum.com"
    )]
    ws_url: String,
    #[structopt(
        short = "r",
        long = "rpc-api-url",
        default_value = "https://solana-api.projectserum.com"
    )]
    rpc_url: String,
    #[structopt(short = "l", long = "listen", default_value = "127.0.0.1:8080")]
    addr: String,
}

#[actix_web::main]
async fn main() {
    let options = Options::from_args();

    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("options: {:?}", options);

    run(options).await;
}

async fn run(options: Options) {
    let map = Arc::new(DashMap::new());
    let current_slot = Arc::new(AtomicU64::new(0));

    let (_, conn) = Client::builder()
        .max_http_version(awc::http::Version::HTTP_11)
        .finish()
        .ws(&options.ws_url)
        .connect()
        .await
        .unwrap();

    info!("connected to websocket rpc @ {}", options.ws_url);

    let (handle, stream) = delay_queue();
    let purge_stream = stream.map(|item| AccountCommand::Purge(item));

    let addr = AccountUpdateManager::create(|ctx| {
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
            request_id: 1,
            map: map.clone(),
            purge_queue: handle,
            slot: current_slot.clone(),
        }
    });

    let rpc_url = options.rpc_url;
    HttpServer::new(move || {
        let state = State {
            map: map.clone(),
            client: Client::default(),
            tx: addr.clone(),
            rpc_url: rpc_url.clone(),
            slot: current_slot.clone(),
        };
        App::new()
            .data(state)
            .service(web::resource("/").route(web::post().to(handler)))
    })
    .bind(options.addr)
    .unwrap()
    .run()
    .await
    .unwrap();
}
