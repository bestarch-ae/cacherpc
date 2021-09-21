use std::cell::RefCell;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix_cors::Cors;
use actix_web::{guard, web, App, HttpServer};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use awc::Client;
use either::Either;
use lru::LruCache;
use structopt::StructOpt;
use tokio::sync::{watch, Notify, Semaphore};
use tracing::info;
use tracing_subscriber::fmt;

pub use cache_rpc::{accounts, metrics, rpc, types};

use accounts::PubSubManager;
use types::{AccountsDb, ProgramAccountsDb};

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
    #[structopt(long = "config", help = "config path")]
    config: Option<PathBuf>,
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

    let span = tracing::span!(tracing::Level::INFO, "global", version = %metrics::version());
    let _enter = span.enter();

    info!(?options, "configuration options");

    run(options).await?;
    Ok(())
}

#[derive(serde::Deserialize, Debug, Clone)]
struct Config {
    rpc: rpc::Config,
}

impl Config {
    fn from_file(f: File) -> Result<Config> {
        use std::io::{BufReader, Read};
        let mut reader = BufReader::new(f);
        let mut buf = Vec::new();

        reader.read_to_end(&mut buf)?;
        Ok(toml::from_slice(&buf)?)
    }

    fn from_options(options: &Options) -> Config {
        Config {
            rpc: rpc::Config {
                request_limits: rpc::RequestLimits {
                    account_info: options.account_info_request_limit,
                    program_accounts: options.program_accounts_request_limit,
                },
            },
        }
    }
}

async fn config_read_loop(path: PathBuf, rpc: watch::Sender<rpc::Config>) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut stream = signal(SignalKind::hangup()).expect("failed to register signal handler");

    while stream.recv().await.is_some() {
        match File::open(&path) {
            Ok(file) => match Config::from_file(file) {
                Ok(config) => {
                    let _ = rpc.broadcast(config.rpc);
                    info!("config reloaded");
                }
                Err(err) => tracing::error!(error = %err, path = ?path, "error parsing config"),
            },
            Err(err) => {
                tracing::error!(path = ?path, error = %err, "failed to open config");
            }
        }
    }
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

    let _rpc_monitor =
        cache_rpc::rpc_monitor::RpcMonitor::init(&options.rpc_url, Client::default());

    let config_file = options
        .config
        .as_ref()
        .map(std::fs::File::open)
        .transpose()?;
    let config = config_file
        .map(Config::from_file)
        .transpose()?
        .unwrap_or_else(|| Config::from_options(&options));

    let rpc_url = options.rpc_url;
    let notify = Arc::new(Notify::new());
    let account_info_request_limit =
        Arc::new(Semaphore::new(config.rpc.request_limits.account_info));
    let program_accounts_request_limit =
        Arc::new(Semaphore::new(config.rpc.request_limits.program_accounts));
    let total_connection_limit =
        2 * (config.rpc.request_limits.account_info + config.rpc.request_limits.program_accounts);
    let body_cache_size = options.body_cache_size;
    let worker_id_counter = Arc::new(AtomicUsize::new(0));
    let bind_addr = &options.addr;

    info!(?config, "config");
    let (sender, receiver) = watch::channel(config.rpc.clone());

    if let Some(path) = options.config {
        actix::spawn(config_read_loop(path, sender));
    }

    let rpc_config = Arc::new(ArcSwap::from(Arc::new(config.rpc)));

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
            config_watch: RefCell::new(receiver.clone()),
            config: rpc_config.clone(),
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

        let content_type_guard = guard::fn_guard(move |req| {
            req.headers()
                .get("content-type")
                .map(move |header| header.as_bytes().starts_with(b"application/json"))
                .unwrap_or(false)
        });

        App::new()
            .data(state)
            .wrap(cors)
            .service(
                web::resource("/")
                    .route(web::post().guard(content_type_guard).to(rpc::rpc_handler))
                    .route(web::post().to(rpc::bad_content_type_handler)),
            )
            .service(web::resource("/metrics").route(web::get().to(rpc::metrics_handler)))
    })
    .bind(bind_addr)
    .with_context(|| format!("failed to bind to {}", bind_addr))?
    .run()
    .await
    .with_context(|| "failed to start http server")
}
