use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

use actix::Addr;
use actix_http::{body::BoxBody, StatusCode};
use actix_web::{post, web, web::Data, App, HttpResponse, HttpServer, ResponseError};
use tokio::sync::watch::Sender;
use tracing::{info, warn};

use crate::{
    cli::{Command, Config},
    pubsub::manager::PubSubManager,
};
use crate::{
    cli::{ForceReconnectSubCmd, StateSubCmd},
    pubsub::manager::WsReconnectInstruction,
    rpc,
};

pub const CACHER_SOCKET_DEFAULT: &str = "/run/cacherpc.sock";

#[derive(Clone)]
pub struct ControlState {
    rpc_config_sender: Option<RpcConfigSender>,
    subscriptions_allowed: Arc<AtomicBool>,
    socket_path: PathBuf,
    waf_tx: Arc<Sender<()>>,
    pubsub: Addr<PubSubManager>,
    wide_filters: Arc<AtomicBool>,
}

impl ControlState {
    pub fn new(
        subscriptions_allowed: Arc<AtomicBool>,
        rpc_config_sender: Option<RpcConfigSender>,
        socket_path: PathBuf,
        waf_tx: Sender<()>,
        pubsub: Addr<PubSubManager>,
        wide_filters: Arc<AtomicBool>,
    ) -> Self {
        if rpc_config_sender.is_none() {
            warn!("No configuration file was set up");
        }
        let waf_tx = Arc::new(waf_tx);

        ControlState {
            rpc_config_sender,
            subscriptions_allowed,
            socket_path,
            waf_tx,
            pubsub,
            wide_filters,
        }
    }
}

#[derive(Clone)]
pub struct RpcConfigSender {
    tx: Arc<Sender<rpc::Config>>,
    path: PathBuf,
}

impl RpcConfigSender {
    pub fn new(tx: Arc<Sender<rpc::Config>>, path: PathBuf) -> Self {
        RpcConfigSender { tx, path }
    }
}

pub async fn run_control_interface(state: ControlState) {
    info!("Starting control interface");
    let state_clone = state.clone();
    let uds = HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state_clone.clone()))
            .service(subscriptions_allowed_handler)
            .service(wide_filters_handler)
            .service(config_reloader)
            .service(force_ws_reconnect)
            .service(waf_reloader)
    })
    .workers(1)
    .bind_uds(&state.socket_path);
    match uds {
        Ok(server) => {
            if let Err(error) = server.run().await {
                warn!(
                    %error,
                    path=?state.socket_path,
                    "Failed to run control interface server"
                );
            }
        }
        Err(error) => {
            warn!(
                %error,
                path=?state.socket_path,
                "Failed to bind control interface to UDS at given path"
            );
        }
    }
}

#[post("/subscriptions/{action}")]
async fn subscriptions_allowed_handler(
    path: web::Path<StateSubCmd>,
    state: Data<ControlState>,
) -> Result<HttpResponse, ControlError> {
    let val = match path.into_inner() {
        StateSubCmd::On => true,
        StateSubCmd::Off => false,
        StateSubCmd::Status => state
            .subscriptions_allowed
            .load(std::sync::atomic::Ordering::Relaxed),
    };
    state
        .subscriptions_allowed
        .store(val, std::sync::atomic::Ordering::Relaxed);
    info!("Subscriptions allowed: {}", val);

    let response = format!("Subsriptions: {}", if val { "on" } else { "off" });
    Ok(HttpResponse::Ok().body(response.into_bytes()))
}

#[post("/wide/filters/{action}")]
async fn wide_filters_handler(
    path: web::Path<StateSubCmd>,
    state: Data<ControlState>,
) -> Result<HttpResponse, ControlError> {
    let val = match path.into_inner() {
        StateSubCmd::On => true,
        StateSubCmd::Off => false,
        StateSubCmd::Status => state
            .wide_filters
            .load(std::sync::atomic::Ordering::Relaxed),
    };
    state
        .wide_filters
        .store(val, std::sync::atomic::Ordering::Relaxed);
    info!("Wide filters fetching enabled: {}", val);

    let response = format!(
        "Wide filters has been turned: {}",
        if val { "on" } else { "off" }
    );
    Ok(HttpResponse::Ok().body(response.into_bytes()))
}

#[post("/config/reload")]
async fn config_reloader(state: Data<ControlState>) -> Result<HttpResponse, ControlError> {
    if let Some(ref sender) = state.rpc_config_sender {
        let config = std::fs::File::open(&sender.path)
            .map(Config::from_file)
            .map_err(|_| ControlError::BadConfigFile)?
            .map_err(|_| ControlError::BadConfigFile)?;

        sender
            .tx
            .send(config.rpc)
            .map_err(|_| ControlError::NoConfigFile)?;
        Ok(HttpResponse::Ok().body(b"OK".as_ref()))
    } else {
        Err(ControlError::NoConfigFile)
    }
}

#[post("/waf/reload")]
async fn waf_reloader(state: Data<ControlState>) -> Result<HttpResponse, ControlError> {
    state.waf_tx.send(()).map_err(|_| ControlError::NoWAFFile)?;
    Ok(HttpResponse::Ok().body(b"OK".as_ref()))
}

#[post("/force/reconnect")]
async fn force_ws_reconnect(
    body: web::Json<WsReconnectInstruction>,
    state: Data<ControlState>,
) -> Result<HttpResponse, ControlError> {
    let instruction = body.into_inner();
    match state.pubsub.send(instruction).await {
        Ok(res) => Ok(HttpResponse::Ok().body(res.into_bytes())),
        Err(_) => Err(ControlError::PubSubNotRunning),
    }
}

#[derive(Debug, thiserror::Error)]
enum ControlError {
    #[allow(unused)]
    #[error("Bad request")]
    BadRequest,
    #[error("No configuration file is set up")]
    NoConfigFile,
    #[error("No WAF file is set up")]
    NoWAFFile,
    #[error("Configuration file cannot be read")]
    BadConfigFile,
    #[error("PubSubManager is not running")]
    PubSubNotRunning,
}

impl ResponseError for ControlError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ControlError::BadRequest => HttpResponse::new(StatusCode::BAD_REQUEST)
                .set_body(BoxBody::new(b"Request not supported".as_ref())),
            ControlError::NoConfigFile => HttpResponse::new(StatusCode::NOT_FOUND)
                .set_body(BoxBody::new(b"Configuration file is not set up".as_ref())),
            ControlError::NoWAFFile => HttpResponse::new(StatusCode::NOT_FOUND)
                .set_body(BoxBody::new(b"WAF file is not set up".as_ref())),
            ControlError::BadConfigFile => HttpResponse::new(StatusCode::CONFLICT).set_body(
                BoxBody::new(b"Configuration file couldn't be read".as_ref()),
            ),
            ControlError::PubSubNotRunning => HttpResponse::new(StatusCode::NOT_FOUND).set_body(
                BoxBody::new(b"Websocket connections manager is not running".as_ref()),
            ),
        }
    }
}

pub async fn handle_command(
    cmd: &Command,
    socket_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use awc::{Client, Connector};
    use awc_uds::UdsConnector;
    use std::io::Write;
    let client = Client::builder()
        .connector(Connector::new().connector(UdsConnector::new(socket_path)))
        .finish();

    let mut response = match cmd {
        Command::ForceReconnect {
            subcmd,
            delay,
            interval,
        } => {
            let body = match subcmd {
                ForceReconnectSubCmd::Init => WsReconnectInstruction::Init {
                    delay: delay.expect("control: no delay was provided"),
                    interval: interval.expect("control: no interval was provided"),
                },
                ForceReconnectSubCmd::Status => WsReconnectInstruction::Status,
                ForceReconnectSubCmd::Abort => WsReconnectInstruction::Abort,
            };
            let body = serde_json::to_vec(&body)?;
            client
                .post(format!("http://localhost/{}", cmd.to_url_path()))
                .content_type("application/json")
                .send_body(body)
                .await?
        }
        _ => {
            client
                .post(format!("http://localhost/{}", cmd.to_url_path()))
                .send()
                .await?
        }
    };
    std::io::stdout()
        .write_all(&response.body().await?)
        .unwrap();
    Ok(())
}
