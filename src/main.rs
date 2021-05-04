use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use actix::io::SinkWrite;
use actix::prelude::{Actor, Context, Message, StreamHandler};
use actix_web::{web, App, Error, HttpResponse, HttpServer};
use awc::Client;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    tx: mpsc::UnboundedSender<AccountCommand>,
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

    let mut save_for_pubkey = None;

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
                        "id": req.id,
                        "jsonrpc": "2.0",
                        "result": data,
                    });

                    println!("cache hit");
                    return Ok(HttpResponse::Ok()
                        .content_type("application/json")
                        .body(serde_json::to_string(&resp).unwrap()));
                }
                None => {
                    save_for_pubkey = Some(pubkey);
                    app_state
                        .tx
                        .send(AccountCommand::Subscribe(pubkey))
                        .unwrap();
                }
            }
        }
        _ => {}
    }

    let client = &app_state.client;
    let mut resp = client
        .post("https://solana-api.projectserum.com")
        .send_json(&req)
        .await
        .unwrap();
    let resp: Value = resp.json().await.unwrap();

    if let Some(pubkey) = save_for_pubkey {
        #[derive(Deserialize, Serialize, Debug)]
        struct Resp {
            result: AccountContext,
        }
        let info: Resp = serde_json::from_value(resp.clone()).unwrap();
        app_state.map.insert(pubkey, info.result);
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&resp).unwrap()))
}

struct AccountUpdateManager {
    request_id: u64,
    inflight: HashMap<u64, Pubkey>,
    subscriptions: HashMap<u64, Pubkey>,
    sink: SinkWrite<
        awc::ws::Message,
        futures_util::stream::SplitSink<
            actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
            awc::ws::Message,
        >,
    >,
    map: Arc<DashMap<Pubkey, AccountContext>>,
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, _ctx: &mut Context<Self>) {
        println!("command: {:?}", item);
        let request_id = self.request_id;
        self.request_id += 1;
        match item {
            AccountCommand::Subscribe(key) => {
                let param = bs58::encode(key.0).into_string();
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "accountSubscribe",
                    "params": [ param ],
                });
                self.inflight.insert(request_id, key);
                self.sink.write(awc::ws::Message::Text(
                    serde_json::to_string(&request).unwrap(),
                ));
            }
        }
    }
}

impl StreamHandler<awc::ws::Frame> for AccountUpdateManager {
    fn handle(&mut self, item: awc::ws::Frame, _ctx: &mut Context<Self>) {
        println!("frame: {:?}", item);
        use awc::ws::Frame;
        match item {
            Frame::Text(text) => {
                let value: serde_json::Value = serde_json::from_slice(&text).unwrap();
                match &value {
                    serde_json::Value::Object(obj) => {
                        if obj.get("result").is_some() {
                            #[derive(Deserialize)]
                            struct Res {
                                result: u64,
                                id: u64,
                            }
                            let res: Res = serde_json::from_value(value).unwrap();
                            if let Some(key) = self.inflight.remove(&res.id) {
                                self.subscriptions.insert(res.result, key);
                                println!("subscribed to stream #{} for {}", res.result, key);
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
                            if let Some(key) = self.subscriptions.get(&resp.params.subscription) {
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
}

fn make_client() -> Client {
    Client::builder()
        .max_http_version(awc::http::Version::HTTP_11)
        .finish()
}

#[actix_web::main]
async fn main() {
    use tokio::stream::StreamExt;
    let map = Arc::new(DashMap::new());

    let (tx, rx) = mpsc::unbounded_channel();

    let (_, conn) = make_client()
        .ws("wss://solana-api.projectserum.com")
        .connect()
        .await
        .unwrap();

    let _addr = AccountUpdateManager::create(|ctx| {
        AccountUpdateManager::add_stream(rx, ctx);
        let (sink, stream) = futures_util::stream::StreamExt::split(conn);
        let (sink, stream) = (sink, stream.filter_map(Result::ok));
        let sink = SinkWrite::new(sink, ctx);
        AccountUpdateManager::add_stream(stream, ctx);
        AccountUpdateManager {
            sink,
            subscriptions: HashMap::default(),
            inflight: HashMap::default(),
            request_id: 1,
            map: map.clone(),
        }
    });

    HttpServer::new(move || {
        let state = State {
            map: map.clone(),
            client: Client::default(),
            tx: tx.clone(),
        };
        App::new()
            .data(state)
            .service(web::resource("/").route(web::post().to(handler)))
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .run()
    .await
    .unwrap();
}
