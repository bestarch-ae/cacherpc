use actix_web::http::header;
use cache_rpc::control::{handle_command, run_control_interface, ControlState, RpcConfigSender};
use either::Either;
use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tracing_subscriber::fmt;

use actix_cors::Cors;
use actix_web::{guard, web, App, HttpServer};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use awc::Client;
use cache_rpc::types::SemaphoreQueue;
use lru::LruCache;
use tokio::sync::{watch, Notify};
use tracing::info;

pub use cache_rpc::{cli, metrics, pubsub, rpc, types};

use pubsub::manager::PubSubManager;
use types::{AccountsDb, ProgramAccountsDb};

#[actix_web::main]
async fn main() -> Result<()> {
    let options = cli::Options::from_args();
    // If command was passed during application startup, then it becomes a client, and tries to
    // execute the command againts other, already running application instance
    if let Some(ref cmd) = options.command {
        let mut exit_code = 0;
        if let Err(e) = handle_command(cmd, &options.control_socket_path).await {
            eprintln!("Command processing error: {}", e);
            eprintln!(
                "Make sure control interface is running and has access to UDS at {:?}",
                options.control_socket_path
            );
            exit_code = 1;
        };
        std::process::exit(exit_code);
    }
    let writer = match options.log_file {
        Some(ref path) => Either::Left(
            file_reopen::File::open(path)
                .with_context(|| format!("Can't open log file `{}`", path.display()))?,
        ),
        None => Either::Right(std::io::stdout()),
    };
    let (writer, _guard) = tracing_appender::non_blocking(writer);

    match options.log_format {
        cli::LogFormat::Json => {
            let subscriber = fmt::Subscriber::builder()
                .with_thread_names(true)
                .with_writer(writer)
                .with_timer(fmt::time::ChronoLocal::rfc3339())
                .json()
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
        cli::LogFormat::Plain => {
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

async fn config_read_loop(path: PathBuf, rpc: Arc<watch::Sender<rpc::Config>>) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut stream = signal(SignalKind::hangup()).expect("failed to register signal handler");

    while stream.recv().await.is_some() {
        match std::fs::File::open(&path) {
            Ok(file) => match cli::Config::from_file(file) {
                Ok(config) => {
                    info!(?config, "configuration reloaded");
                    let _ = rpc.send(config.rpc);
                }
                Err(err) => tracing::error!(error = %err, path = ?path, "error parsing config"),
            },
            Err(err) => {
                tracing::error!(path = ?path, error = %err, "failed to open config");
            }
        }
    }
}

async fn run(options: cli::Options) -> Result<()> {
    let accounts = AccountsDb::new();
    let program_accounts = ProgramAccountsDb::default();

    let rpc_slot = Arc::new(AtomicU64::new(0));
    let _rpc_monitor = cache_rpc::rpc::monitor::RpcMonitor::init(
        &options.rpc_url,
        Client::default(),
        rpc_slot.clone(),
    );

    let config_file = options
        .config
        .as_ref()
        .map(std::fs::File::open)
        .transpose()?;
    let config = config_file
        .map(cli::Config::from_file)
        .transpose()?
        .unwrap_or_else(|| cli::Config::from_options(&options));

    info!(?config, "config");

    let subscriptions_allowed = Arc::new(AtomicBool::new(true));
    let (pubsub, pubsub_actor) = PubSubManager::init(
        options.websocket_connections,
        accounts.clone(),
        program_accounts.clone(),
        rpc_slot.clone(),
        pubsub::manager::WorkerConfig {
            websocket_url: options.ws_url.to_owned(),
            ttl: options.time_to_live,
            slot_distance: options.slot_distance,
        },
        Arc::clone(&subscriptions_allowed),
    );

    let rules_path = options.rules.clone();

    let rpc_url = options.rpc_url;
    let notify = Arc::new(Notify::new());
    let account_info_request_limit = Arc::new(SemaphoreQueue::new(
        config.rpc.request_queue_size.account_info,
        config.rpc.request_limits.account_info,
    ));
    let program_accounts_request_limit = Arc::new(SemaphoreQueue::new(
        config.rpc.request_queue_size.program_accounts,
        config.rpc.request_limits.program_accounts,
    ));
    let total_connection_limit =
        2 * (config.rpc.request_limits.account_info + config.rpc.request_limits.program_accounts);
    let body_cache_size = options.body_cache_size;
    let worker_id_counter = Arc::new(AtomicUsize::new(0));
    let bind_addr = &options.addr;

    let (rpc_tx, rpc_rx) = watch::channel(config.rpc.clone());

    let mut rpc_config_sender = None;

    if let Some(path) = options.config {
        let rpc_tx = Arc::new(rpc_tx);
        actix::spawn(config_read_loop(path.clone(), Arc::clone(&rpc_tx)));
        rpc_config_sender = Some(RpcConfigSender::new(rpc_tx, path));
    }

    let (waf_tx, waf_rx) = watch::channel(());

    let fetch_wide_filters = Arc::default(); // by default will be set to false

    let control_state = ControlState::new(
        subscriptions_allowed,
        rpc_config_sender,
        options.control_socket_path,
        waf_tx,
        pubsub_actor,
        Arc::clone(&fetch_wide_filters),
    );

    actix::spawn(run_control_interface(control_state));

    let rpc_config = Arc::new(ArcSwap::from(Arc::new(config.rpc)));
    let timeout = options.request_timeout;

    let identity = options.identity;
    HttpServer::new(move || {
        let waf = rules_path
            .as_ref()
            .map(rpc::state::Waf::new)
            .transpose()
            .map_err(|err| {
                tracing::error!(error = %err, "failed to load waf rules");
            })
            .ok()
            .flatten();

        let client = Client::builder()
            .timeout(timeout)
            .no_default_headers()
            .connector(
                awc::Connector::new()
                    .conn_keep_alive(Duration::from_secs(60))
                    .conn_lifetime(Duration::from_secs(600))
                    .limit(total_connection_limit),
            )
            .finish();
        let state = rpc::state::State {
            accounts: accounts.clone(),
            program_accounts: program_accounts.clone(),
            client,
            pubsub: pubsub.clone(),
            rpc_url: rpc_url.clone(),
            map_updated: notify.clone(),
            account_info_request_limit: account_info_request_limit.clone(),
            program_accounts_request_limit: program_accounts_request_limit.clone(),
            config_watch: RefCell::new(rpc_rx.clone()),
            config: rpc_config.clone(),
            lru: RefCell::new(LruCache::new(body_cache_size)),
            worker_id: {
                let id = worker_id_counter.fetch_add(1, Ordering::SeqCst);
                format!("rpc-{}", id)
            },
            waf,
            waf_watch: RefCell::new(waf_rx.clone()),
            identity: identity.clone(),
            fetch_wide_filters: Arc::clone(&fetch_wide_filters),
        };
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["POST"])
            .allowed_header(actix_web::http::header::CONTENT_TYPE);

        let content_type_guard = guard::fn_guard(move |req| {
            req.header::<header::ContentType>()
                .map(move |header| {
                    header.type_() == mime::APPLICATION && header.subtype() == mime::JSON
                })
                .unwrap_or(false)
        });

        App::new()
            .app_data(web::Data::new(state))
            .wrap(cors)
            .service(
                web::resource("/")
                    .route(
                        web::post()
                            .guard(content_type_guard)
                            .to(rpc::handler::rpc_handler),
                    )
                    .route(web::post().to(rpc::handler::bad_content_type_handler)),
            )
            .service(web::resource("/metrics").route(web::get().to(rpc::metrics_handler)))
    })
    .bind(bind_addr)
    .with_context(|| format!("failed to bind to {}", bind_addr))?
    .run()
    .await
    .with_context(|| "failed to start http server")
}
