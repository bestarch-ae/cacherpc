use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder};
use awc::Client;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Hash, Eq, PartialEq)]
struct Pubkey([u8; 32]);

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    lamports: u64,
    owner: String,
    executable: bool,
    rent_epoch: u64,
}

#[derive(Clone)]
struct State {
    map: Arc<DashMap<Pubkey, AccountInfo>>,
    client: Client,
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
                        "result": {
                            "value": data,
                        }
                    });

                    println!("cache hit");
                    return Ok(HttpResponse::Ok()
                        .content_type("application/json")
                        .body(serde_json::to_string(&resp).unwrap()));
                }
                None => {
                    save_for_pubkey = Some(pubkey);
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
        struct Res<T> {
            value: T,
        }
        #[derive(Deserialize, Serialize, Debug)]
        struct Resp<T> {
            result: Res<T>,
        }
        let info: Resp<AccountInfo> = serde_json::from_value(resp.clone()).unwrap();
        app_state.map.insert(pubkey, info.result.value);
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&resp).unwrap()))
}

#[actix_web::main]
async fn main() {
    let map = Arc::new(DashMap::new());

    HttpServer::new(move || {
        let state = State {
            map: map.clone(),
            client: Client::default(),
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
