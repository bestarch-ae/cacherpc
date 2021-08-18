use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer};
use anyhow::{Context, Result};
use awc::Client;
use either::Either;
use lru::LruCache;
use once_cell::sync::Lazy;
use structopt::StructOpt;
use tokio::sync::{Notify, Semaphore};
use tracing::info;
use tracing_subscriber::fmt;

mod accounts;
mod metrics;
mod rpc;
mod types;

use accounts::PubSubManager;
use types::{AccountsDb, ProgramAccountsDb};

pub fn version() -> &'static str {
    static VERSION: Lazy<String> = Lazy::new(|| {
        let pkg_version = std::env!("CARGO_PKG_VERSION");
        option_env!("CI_COMMIT_TAG")
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                format!(
                    "{}{}",
                    pkg_version,
                    option_env!("CI_COMMIT_SHA")
                        .map(|sha| format!("-{}", sha))
                        .unwrap_or_else(String::new)
                )
            })
    });
    &VERSION
}

#[derive(Debug, structopt::StructOpt)]
#[structopt(about = "Solana RPC cache server")]
struct Options {
    #[structopt(
        short = "w",
        long = "websocket-url",
        default_value = "wss://solana-api.projectserum.com",
        help = "validator or cluster PubSub endpoint"
    )]
    ws_url: String,
    #[structopt(
        short = "r",
        long = "rpc-api-url",
        default_value = "https://solana-api.projectserum.com",
        help = "validator or cluster JSON-RPC endpoint"
    )]
    rpc_url: String,
    #[structopt(
        short = "l",
        long = "listen",
        default_value = "127.0.0.1:8080",
        help = "cache server bind address"
    )]
    addr: String,
    #[structopt(
        short = "p",
        long = "program-request-limit",
        default_value = "5",
        help = "maximum number of concurrent getProgramAccounts cache-to-validator requests"
    )]
    program_accounts_request_limit: usize,
    #[structopt(
        short = "a",
        long = "account-request-limit",
        default_value = "100",
        help = "maximum number of concurrent getAccountInfo cache-to-validator requests"
    )]
    account_info_request_limit: usize,
    #[structopt(
        short = "b",
        long = "body-cache-size",
        default_value = "100",
        help = "maximum amount of entries in the response cache"
    )]
    body_cache_size: usize,
    #[structopt(
        long = "log-format",
        default_value = "plain",
        help = "one of: 'plain', 'json'"
    )]
    log_format: LogFormat,
    #[structopt(long = "log-file", help = "file path")]
    log_file: Option<std::path::PathBuf>,
    #[structopt(
        short = "c",
        long = "websocket-connections",
        help = "number of WebSocket connections to validator",
        default_value = "1"
    )]
    websocket_connections: u32,
    #[structopt(
        short = "t",
        long = "time-to-live",
        help = "time to live for cached values",
        default_value = "10m",
        parse(try_from_str = humantime::parse_duration)
    )]
    time_to_live: Duration,
}

#[derive(Debug)]
enum LogFormat {
    Plain,
    Json,
}

#[derive(thiserror::Error, Debug)]
#[error("must be one of: \"plain\", \"json\"")]
struct LogFormatParseError;

impl std::str::FromStr for LogFormat {
    type Err = LogFormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "plain" => Ok(LogFormat::Plain),
            "json" => Ok(LogFormat::Json),
            _ => Err(LogFormatParseError),
        }
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let options = Options::from_args();

    let writer = match &options.log_file {
        Some(path) => Either::Left(
            file_reopen::File::open(path)
                .with_context(|| format!("Can't open log file `{}`", path.display()))?,
        ),
        None => Either::Right(std::io::stdout()),
    };
    let (writer, _guard) = tracing_appender::non_blocking(writer);

    match options.log_format {
        LogFormat::Json => {
            let subscriber = fmt::Subscriber::builder()
                .with_thread_names(true)
                .with_writer(writer)
                .with_timer(fmt::time::ChronoLocal::rfc3339())
                .json()
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
        LogFormat::Plain => {
            let subscriber = fmt::Subscriber::builder().with_writer(writer).finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
    };

    let span = tracing::span!(tracing::Level::INFO, "global", version = %version());
    let _enter = span.enter();

    info!(?options, "configuration options");

    run(options).await?;
    Ok(())
}

async fn run(options: Options) -> Result<()> {
    let accounts = AccountsDb::new();
    let program_accounts = ProgramAccountsDb::new();

    let pubsub = PubSubManager::init(
        options.websocket_connections,
        accounts.clone(),
        program_accounts.clone(),
        &options.ws_url,
        options.time_to_live,
    );

    let rpc_url = options.rpc_url;
    let notify = Arc::new(Notify::new());
    let account_info_request_limit = Arc::new(Semaphore::new(options.account_info_request_limit));
    let program_accounts_request_limit =
        Arc::new(Semaphore::new(options.program_accounts_request_limit));
    let total_connection_limit =
        2 * (options.account_info_request_limit + options.program_accounts_request_limit);
    let body_cache_size = options.body_cache_size;
    let worker_id_counter = Arc::new(AtomicUsize::new(0));
    let bind_addr = &options.addr;

    HttpServer::new(move || {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .no_default_headers()
            .connector(
                awc::Connector::new()
                    .conn_keep_alive(Duration::from_secs(60))
                    .conn_lifetime(Duration::from_secs(600))
                    .limit(total_connection_limit)
                    .finish(),
            )
            .finish();
        let state = rpc::State {
            accounts: accounts.clone(),
            program_accounts: program_accounts.clone(),
            client,
            pubsub: pubsub.clone(),
            rpc_url: rpc_url.clone(),
            map_updated: notify.clone(),
            account_info_request_limit: account_info_request_limit.clone(),
            program_accounts_request_limit: program_accounts_request_limit.clone(),
            lru: RefCell::new(LruCache::new(body_cache_size)),
            worker_id: {
                let id = worker_id_counter.fetch_add(1, Ordering::SeqCst);
                format!("rpc-{}", id)
            },
        };
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["POST"])
            .allowed_header(actix_web::http::header::CONTENT_TYPE);
        App::new()
            .data(state)
            .wrap(cors)
            .service(web::resource("/").route(web::post().to(rpc::rpc_handler)))
            .service(web::resource("/metrics").route(web::get().to(rpc::metrics_handler)))
    })
    .bind(bind_addr)
    .with_context(|| format!("failed to bind to {}", bind_addr))?
    .run()
    .await
    .with_context(|| "failed to start http server")
}
