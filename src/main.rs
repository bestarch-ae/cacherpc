use cache_rpc::control_interface::{
    handle_command, run_control_interface, ControlState, RpcConfigSender,
};
use either::Either;
use std::cell::RefCell;
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
use lru::LruCache;
use tokio::sync::{watch, Notify, Semaphore};
use tracing::info;

use mlua::{Lua, LuaOptions, StdLib};

pub use cache_rpc::{cli_options, metrics, pubsub, rpc, types};

use pubsub::PubSubManager;
use types::{AccountsDb, ProgramAccountsDb};

const LUA_JSON: &str = include_str!("json.lua");

#[actix_web::main]
async fn main() -> Result<()> {
    let options = cli_options::Options::from_args();
    // If command was passed during application startup, then it becomes a client, and tries to
    // execute the command againts other, already running application instance
    if let Some(ref cmd) = options.command {
        let mut exit_code = 0;
        if let Err(e) = handle_command(cmd).await {
            eprintln!("Command processing error: {}", e);
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
        cli_options::LogFormat::Json => {
            let subscriber = fmt::Subscriber::builder()
                .with_thread_names(true)
                .with_writer(writer)
                .with_timer(fmt::time::ChronoLocal::rfc3339())
                .json()
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
        cli_options::LogFormat::Plain => {
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

fn lua(rules: &str) -> Result<Lua, mlua::Error> {
    // if any of the lua preparation steps contain errors, then WAF will not be used
    let lua = Lua::new_with(
        StdLib::MATH | StdLib::STRING | StdLib::PACKAGE,
        LuaOptions::default(),
    )?;

    let func = lua.load(LUA_JSON).into_function()?;

    let _: mlua::Value<'_> = lua.load_from_function("json", func)?;

    let rules = lua.load(&rules).into_function()?;

    let _: mlua::Value<'_> = lua.load_from_function("waf", rules)?;

    info!("loaded WAF rules");
    Ok(lua)
}

async fn run(options: cli_options::Options) -> Result<()> {
    let accounts = AccountsDb::new();
    let program_accounts = ProgramAccountsDb::new();

    let rpc_slot = Arc::new(AtomicU64::new(0));
    let _rpc_monitor = cache_rpc::rpc_monitor::RpcMonitor::init(
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
        .map(cli_options::Config::from_file)
        .transpose()?
        .unwrap_or_else(|| cli_options::Config::from_options(&options));

    info!(?config, "config");

    let subscriptions_allowed = Arc::new(AtomicBool::new(true));
    let pubsub = PubSubManager::init(
        options.websocket_connections,
        accounts.clone(),
        program_accounts.clone(),
        rpc_slot.clone(),
        pubsub::WorkerConfig {
            websocket_url: options.ws_url.to_owned(),
            ttl: options.time_to_live,
            slot_distance: options.slot_distance,
        },
        Arc::clone(&subscriptions_allowed),
    );

    let rules = options
        .rules
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?;

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

    let (rpc_tx, rpc_rx) = watch::channel(config.rpc.clone());

    let rpc_config_sender = options
        .config
        .map(|path| RpcConfigSender::new(rpc_tx, path));

    let control_state = ControlState::new(subscriptions_allowed, rpc_config_sender);

    actix::spawn(run_control_interface(control_state));

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
            config_watch: RefCell::new(rpc_rx.clone()),
            config: rpc_config.clone(),
            lru: RefCell::new(LruCache::new(body_cache_size)),
            worker_id: {
                let id = worker_id_counter.fetch_add(1, Ordering::SeqCst);
                format!("rpc-{}", id)
            },
            lua: rules
                .as_ref()
                .map(|rules| match lua(rules) {
                    Ok(lua) => Some(lua),
                    Err(e) => {
                        eprintln!(
                            "WAF rules were provided, but program was unable to parse them:\n{}\nFix the rules and try again.",
                            e
                        );
                        std::process::exit(1);
                    }
                })
                .flatten(),
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
            .app_data(web::Data::new(state))
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
