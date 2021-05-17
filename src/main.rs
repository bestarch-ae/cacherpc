use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use awc::Client;
use dashmap::DashMap;
use structopt::StructOpt;
use tokio::sync::{Notify, Semaphore};
use tracing::info;

mod accounts;
mod rpc;
mod types;

use accounts::AccountUpdateManager;
use types::AccountsDb;

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
    #[structopt(short = "p", long = "program-request-limit", default_value = "5")]
    program_accounts_request_limit: usize,
    #[structopt(short = "a", long = "account-request-limit", default_value = "100")]
    account_info_request_limit: usize,
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
    let accounts = AccountsDb::new();
    let program_accounts = Arc::new(DashMap::new());

    let (_, conn) = Client::builder()
        .max_http_version(awc::http::Version::HTTP_11)
        .finish()
        .ws(&options.ws_url)
        .connect()
        .await
        .unwrap();

    info!("connected to websocket rpc @ {}", options.ws_url);

    let addr = AccountUpdateManager::init(accounts.clone(), program_accounts.clone(), conn);

    let rpc_url = options.rpc_url;
    let notify = Arc::new(Notify::new());
    let connection_limit =
        options.account_info_request_limit + options.program_accounts_request_limit;
    let account_info_request_limit = Arc::new(Semaphore::new(options.account_info_request_limit));
    let program_accounts_request_limit =
        Arc::new(Semaphore::new(options.program_accounts_request_limit));

    HttpServer::new(move || {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .connector(
                awc::Connector::new()
                    .max_http_version(awc::http::Version::HTTP_11)
                    .limit(connection_limit)
                    //.conn_keep_alive(Duration::from_secs(0))
                    //.conn_lifetime(Duration::from_secs(0))
                    .finish(),
            )
            .finish();
        let state = rpc::State {
            accounts: accounts.clone(),
            program_accounts: program_accounts.clone(),
            client,
            tx: addr.clone(),
            rpc_url: rpc_url.clone(),
            map_updated: notify.clone(),
            account_info_request_limit: account_info_request_limit.clone(),
            program_accounts_request_limit: program_accounts_request_limit.clone(),
        };
        App::new()
            .data(state)
            .service(web::resource("/").route(web::post().to(rpc::rpc_handler)))
            .service(web::resource("/metrics").route(web::get().to(rpc::metrics_handler)))
    })
    .bind(options.addr)
    .unwrap()
    .run()
    .await
    .unwrap();
}
