use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use actix_http::{body::AnyBody, StatusCode};
use actix_web::{post, web, web::Data, App, HttpResponse, HttpServer, ResponseError};
use serde::Deserialize;
use tokio::sync::watch::Sender;
use tracing::{info, warn};

use crate::{
    cli::{Command, Config},
    rpc,
};

pub const CACHER_SOCKET_DEFAULT: &str = "/run/cacherpc.sock";

#[derive(Clone)]
pub struct ControlState {
    rpc_config_sender: Option<RpcConfigSender>,
    subscriptions_allowed: Arc<AtomicBool>,
    socket_path: PathBuf,
}

impl ControlState {
    pub fn new(
        subscriptions_allowed: Arc<AtomicBool>,
        rpc_config_sender: Option<RpcConfigSender>,
        socket_path: PathBuf,
    ) -> Self {
        if rpc_config_sender.is_none() {
            warn!("No configuration file was set up");
        }
        ControlState {
            rpc_config_sender,
            subscriptions_allowed,
            socket_path,
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
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(state_clone.clone()))
            .service(subscriptions_allowed_handler)
            .service(config_reloader)
    })
    .workers(1)
    .bind_uds(state.socket_path)
    .unwrap()
    .run()
    .await
    .unwrap();
}

#[derive(Deserialize, Debug)]
struct SubscribeAction {
    action: String,
}

#[post("/subscriptions/{action}")]
async fn subscriptions_allowed_handler(
    path: web::Path<SubscribeAction>,
    state: Data<ControlState>,
) -> Result<HttpResponse, ControlError> {
    let val = match path.action.as_str() {
        "on" => true,
        "off" => false,
        "status" => state
            .subscriptions_allowed
            .load(std::sync::atomic::Ordering::Relaxed),
        other => {
            warn!("Invalid action for subscriptions_allowed: {}", other);
            return Err(ControlError::BadRequest);
        }
    };
    state
        .subscriptions_allowed
        .store(val, std::sync::atomic::Ordering::Relaxed);
    info!("Subscriptions allowed: {}", val);

    let response = format!("Subsriptions: {}", if val { "on" } else { "off" });
    Ok(HttpResponse::Ok().body(AnyBody::from_slice(response.as_bytes())))
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
        Ok(HttpResponse::Ok().body(AnyBody::from_slice(b"OK")))
    } else {
        Err(ControlError::NoConfigFile)
    }
}

#[derive(Debug, thiserror::Error)]
enum ControlError {
    #[error("Bad request")]
    BadRequest,
    #[error("No configuration file is set up")]
    NoConfigFile,
    #[error("Configuration file cannot be read")]
    BadConfigFile,
}

impl ResponseError for ControlError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ControlError::BadRequest => HttpResponse::new(StatusCode::BAD_REQUEST)
                .set_body(AnyBody::from_slice(b"Request not supported")),
            ControlError::NoConfigFile => HttpResponse::new(StatusCode::NOT_FOUND)
                .set_body(AnyBody::from_slice(b"Configuration file is not set up")),
            ControlError::BadConfigFile => HttpResponse::new(StatusCode::CONFLICT)
                .set_body(AnyBody::from_slice(b"Configuration file couldn't be read")),
        }
    }
}

pub async fn handle_command(
    cmd: &Command,
    socket_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use awc::{Client, Connector};
    use awc_uds::UdsConnector;
    use std::io::Write;
    let client = Client::builder()
        .connector(Connector::new().connector(UdsConnector::new(socket_path)))
        .finish();

    let mut response = client
        .post(format!("http://localhost/{}", cmd.to_url_path()))
        .send()
        .await?;
    std::io::stdout()
        .write_all(&response.body().await?)
        .unwrap();
    Ok(())
}
