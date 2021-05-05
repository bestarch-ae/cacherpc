use std::borrow::Cow;
use std::collections::HashMap;
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
use serde_json::Value;

use tracing::info;
use tracing_subscriber;

use structopt::StructOpt;

const PURGE_TIMEOUT: Duration = Duration::from_secs(10);

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
        let mut buf = [0; 64];
        let len = bs58::encode(self.0).into(&mut buf[..]).unwrap();
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
                let mut buf = [0; 32];
                bs58::decode(v).into(&mut buf).unwrap();
                Ok(Pubkey(buf))
            }
        }

        deserializer.deserialize_str(PubkeyVisitor)
    }
}

#[derive(Default, Debug)]
struct EmptyMarker;

impl Serialize for EmptyMarker {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element("")?;
        seq.serialize_element("base58")?;
        seq.end()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    lamports: u64,
    #[serde(skip_deserializing)]
    data: EmptyMarker,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
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

#[derive(Clone)]
struct State {
    map: Arc<DashMap<Pubkey, AccountContext>>,
    client: Client,
    tx: Addr<AccountUpdateManager>,
    rpc_url: String,
}

async fn handler(body: Bytes, app_state: web::Data<State>) -> Result<HttpResponse, Error> {
    #[derive(Deserialize, Serialize, Debug)]
    struct Request<'a> {
        jsonrpc: Cow<'a, str>,
        id: u64,
        method: Cow<'a, str>,
        params: [Value; 2],
    }
    let req: Request = serde_json::from_slice(&body).unwrap();

    let mut cacheable_for_key = None;

    match req.method.as_ref() {
        "getAccountInfo" => {
            #[derive(Deserialize, Serialize, Debug)]
            struct Params<'a> {
                encoding: Cow<'a, str>,
                commitment: Option<Cow<'a, str>>,
            }
            let pubkey = match &req.params[0] {
                Value::String(pubkey) => {
                    let mut buf = [0; 32];
                    bs58::decode(&pubkey).into(&mut buf).unwrap();
                    Pubkey(buf)
                }
                _ => panic!(),
            };

            match app_state.map.get(&pubkey) {
                Some(data) => {
                    let data = data.value();
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "result": data,
                        "id": req.id,
                    });

                    info!("cache hit for {}", pubkey);
                    return Ok(HttpResponse::Ok()
                        .content_type("application/json")
                        .json(&resp));
                }
                None => {
                    cacheable_for_key = Some(pubkey);
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
    let resp: Value = resp.json().await.unwrap();

    if let Some(pubkey) = cacheable_for_key {
        #[derive(Deserialize, Serialize, Debug)]
        struct Resp {
            result: AccountContext,
        }
        let info: Resp = serde_json::from_value(resp.clone()).unwrap();
        app_state.map.insert(pubkey, info.result);
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(&resp))
}

enum InflightRequest {
    Sub(Pubkey),
    Unsub(Pubkey),
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
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        <Self as Handler<AccountCommand>>::handle(self, item, ctx)
    }
}

impl Handler<AccountCommand> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: AccountCommand, _ctx: &mut Context<Self>) {
        let request_id = self.request_id;
        self.request_id += 1;
        match item {
            AccountCommand::Subscribe(key) => {
                info!("subscribe to {}", key);
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "accountSubscribe",
                    "params": [ key ],
                });
                self.inflight.insert(request_id, InflightRequest::Sub(key));
                self.sink.write(awc::ws::Message::Text(
                    serde_json::to_string(&request).unwrap(),
                ));
                self.purge_queue.insert(key, PURGE_TIMEOUT);
            }
            AccountCommand::Purge(key) => {
                info!("purging {}", key);
                if let Some(sub_id) = self.key_to_sub.get(&key) {
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "accountUnsubscribe",
                        "params": [ sub_id ],
                    });
                    self.inflight
                        .insert(request_id, InflightRequest::Unsub(key));
                    self.sink.write(awc::ws::Message::Text(
                        serde_json::to_string(&request).unwrap(),
                    ));
                }
                self.map.remove(&key);
            }
        }
    }
}

impl StreamHandler<awc::ws::Frame> for AccountUpdateManager {
    fn handle(&mut self, item: awc::ws::Frame, _ctx: &mut Context<Self>) {
        use awc::ws::Frame;
        match item {
            Frame::Text(text) => {
                let value: serde_json::Value = serde_json::from_slice(&text).unwrap();
                match &value {
                    serde_json::Value::Object(obj) => {
                        if obj.get("result").is_some() {
                            #[derive(Deserialize)]
                            struct Res {
                                result: serde_json::Value,
                                id: u64,
                            }
                            let res: Res = serde_json::from_value(value).unwrap();
                            if let Some(req) = self.inflight.remove(&res.id) {
                                match req {
                                    InflightRequest::Sub(key) => {
                                        let sub_id: u64 =
                                            serde_json::from_value(res.result).unwrap();
                                        self.sub_to_key.insert(sub_id, key);
                                        self.key_to_sub.insert(key, sub_id);
                                        info!(message = "subscribed to stream", sub = sub_id, key = %key);
                                    }
                                    InflightRequest::Unsub(key) => {
                                        let _is_ok: bool =
                                            serde_json::from_value(res.result).unwrap();
                                        if let Some(sub) = self.key_to_sub.remove(&key) {
                                            self.sub_to_key.remove(&sub);
                                            info!(
                                                message = "unsubscribed from stream",
                                                sub = sub,
                                                key = %key,
                                            );
                                        }
                                    }
                                }
                            }

                            // TODO: method response
                            return;
                        };
                        if obj.get("method").is_some() {
                            #[derive(Deserialize)]
                            struct Params {
                                result: AccountContext,
                                subscription: u64,
                            }
                            #[derive(Deserialize)]
                            struct Notification {
                                params: Params,
                            }
                            let resp: Notification = serde_json::from_value(value).unwrap();
                            if let Some(key) = self.sub_to_key.get(&resp.params.subscription) {
                                self.map.insert(*key, resp.params.result);
                            }
                        }
                    }
                    _ => return,
                }
            }
            _ => return,
        }
    }
}

impl Actor for AccountUpdateManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
enum AccountCommand {
    Subscribe(Pubkey),
    Purge(Pubkey),
}

fn make_client() -> Client {
    Client::builder()
        .max_http_version(awc::http::Version::HTTP_11)
        .finish()
}

struct DelayQueueHandle<T>(mpsc::UnboundedSender<(T, Instant)>);

impl<T> DelayQueueHandle<T> {
    fn insert_at(&self, item: T, time: Instant) {
        let _ = self.0.send((item, time));
    }

    fn insert(&self, item: T, dur: Duration) {
        self.insert_at(item, Instant::now() + dur)
    }
}

fn delay_queue<T>() -> (DelayQueueHandle<T>, impl Stream<Item = T>) {
    let (sender, incoming) = mpsc::unbounded_channel();
    let stream = stream_generator::generate_stream(|mut stream| async move {
        let mut delay_queue = DelayQueue::new();
        tokio::pin!(incoming);

        loop {
            tokio::select! {
                item = incoming.next() => {
                    if let Some((item, time)) = item {
                        delay_queue.insert_at(item, time);
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

    //let (tx, rx) = mpsc::unbounded_channel();

    let (_, conn) = make_client().ws(&options.ws_url).connect().await.unwrap();

    info!("connected to websocket rpc @ {}", options.ws_url);

    let (handle, stream) = delay_queue();
    let purge_stream = stream.map(|item| AccountCommand::Purge(item));

    let addr = AccountUpdateManager::create(|ctx| {
        let (sink, stream) = futures_util::stream::StreamExt::split(conn);
        let (sink, stream) = (sink, stream.filter_map(Result::ok));
        let sink = SinkWrite::new(sink, ctx);
        AccountUpdateManager::add_stream(stream, ctx);
        AccountUpdateManager::add_stream(purge_stream, ctx);
        //AccountUpdateManager::add_stream(rx, ctx);
        AccountUpdateManager {
            sink,
            sub_to_key: HashMap::default(),
            key_to_sub: HashMap::default(),
            inflight: HashMap::default(),
            request_id: 1,
            map: map.clone(),
            purge_queue: handle,
        }
    });

    let rpc_url = options.rpc_url;
    HttpServer::new(move || {
        let state = State {
            map: map.clone(),
            client: Client::default(),
            tx: addr.clone(),
            rpc_url: rpc_url.clone(),
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
