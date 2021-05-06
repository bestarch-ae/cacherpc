use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use actix_web::{web, App, HttpServer};

use awc::Client;
use dashmap::DashMap;
use tokio::sync::{Notify, Semaphore};

use tracing::info;
use tracing_subscriber;

use structopt::StructOpt;

mod rpc;
mod types;
mod updater;

use updater::AccountUpdateManager;

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
    #[structopt(short = "n", long = "request-limit", default_value = "10")]
    request_limit: usize,
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

    let addr = AccountUpdateManager::init(current_slot.clone(), map.clone(), conn);

    let rpc_url = options.rpc_url;
    let notify = Arc::new(Notify::new());
    let semaphore = Arc::new(Semaphore::new(options.request_limit));
    HttpServer::new(move || {
        let state = rpc::State {
            map: map.clone(),
            client: Client::default(),
            tx: addr.clone(),
            rpc_url: rpc_url.clone(),
            slot: current_slot.clone(),
            map_updated: notify.clone(),
            request_limit: semaphore.clone(),
        };
        App::new()
            .data(state)
            .service(web::resource("/").route(web::post().to(rpc::rpc_handler)))
    })
    .bind(options.addr)
    .unwrap()
    .run()
    .await
    .unwrap();
}
