use anyhow::Result;
use humantime;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

use crate::{control, rpc::config};

#[derive(Debug, StructOpt)]
#[structopt(about = "Solana RPC cache server")]
pub struct Options {
    #[structopt(subcommand)]
    pub command: Option<Command>,
    #[structopt(
        short = "w",
        long = "websocket-url",
        default_value = "wss://solana-api.projectserum.com",
        help = "validator or cluster PubSub endpoint"
    )]
    pub ws_url: String,
    #[structopt(
        short = "r",
        long = "rpc-api-url",
        default_value = "https://solana-api.projectserum.com",
        help = "validator or cluster JSON-RPC endpoint"
    )]
    pub rpc_url: String,
    #[structopt(
        short = "l",
        long = "listen",
        default_value = "127.0.0.1:8080",
        help = "cache server bind address"
    )]
    pub addr: String,
    #[structopt(
        short = "p",
        long = "program-request-limit",
        default_value = "5",
        help = "maximum number of concurrent getProgramAccounts cache-to-validator requests"
    )]
    pub program_accounts_request_limit: usize,
    #[structopt(
        short = "a",
        long = "account-request-limit",
        default_value = "100",
        help = "maximum number of concurrent getAccountInfo cache-to-validator requests"
    )]
    pub account_info_request_limit: usize,
    #[structopt(
        short = "A",
        long = "account-request-queue-size",
        help = "maximum number of getAccountInfo requests, that may be queued, if concurrent limit is reached"
    )]
    account_info_request_queue_size: Option<usize>,
    #[structopt(
        short = "P",
        long = "program-request-queue-size",
        help = "maximum number of getProgramAccounts requests, that may be queued, if concurrent limit is reached"
    )]
    program_accounts_request_queue_size: Option<usize>,
    #[structopt(
        short = "b",
        long = "body-cache-size",
        default_value = "100",
        help = "maximum amount of entries in the response cache"
    )]
    pub body_cache_size: usize,
    #[structopt(
        long = "log-format",
        default_value = "plain",
        help = "one of: 'plain', 'json'"
    )]
    pub log_format: LogFormat,
    #[structopt(long = "log-file", help = "file path")]
    pub log_file: Option<std::path::PathBuf>,
    #[structopt(
        short = "c",
        long = "websocket-connections",
        help = "number of WebSocket connections to validator",
        default_value = "1"
    )]
    pub websocket_connections: u32,
    #[structopt(
        short = "t",
        long = "time-to-live",
        help = "time to live for cached values",
        default_value = "10m",
        parse(try_from_str = humantime::parse_duration)
    )]
    pub time_to_live: Duration,

    #[structopt(
        long = "ignore-base58-limit",
        help = "ignore base58 overflowing size limit"
    )]
    pub ignore_base58: bool,
    #[structopt(long = "config", help = "config path")]
    pub config: Option<PathBuf>,
    #[structopt(
        long = "slot-distance",
        short = "d",
        help = "Health check slot distance",
        default_value = "150"
    )]
    pub slot_distance: u32,
    #[structopt(
        long = "rules",
        help = "web application firewall rules, to filter out specific requests"
    )]
    pub rules: Option<PathBuf>,
    #[structopt(
        long = "control-socket-path",
        help = "path to socket file, e.g. /run/cacherpc.sock",
        default_value = control::CACHER_SOCKET_DEFAULT
    )]
    pub control_socket_path: PathBuf,
    #[structopt(
        long = "request-timeout",
        help = "time duration, after which request to validator will be aborted, if no response arrives",
        default_value = "60s",
        parse(try_from_str = humantime::parse_duration)
    )]
    pub request_timeout: Duration,
    #[structopt(
        long = "identity",
        help = "public key of cacherpc, that should be sent to getIdentity requests",
        parse(try_from_str = parse_identity)
    )]
    pub identity: Option<String>,
}

#[derive(Debug)]
pub enum LogFormat {
    Plain,
    Json,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    Subscriptions { state: SubscriptionsState },
    ConfigReload,
    WafReload,
}

impl Command {
    pub const fn to_url_path(&self) -> &'static str {
        match self {
            Self::ConfigReload => "config/reload",
            Self::WafReload => "waf/reload",
            Self::Subscriptions { state } => match state {
                SubscriptionsState::On => "subscriptions/on",
                SubscriptionsState::Off => "subscriptions/off",
                SubscriptionsState::Status => "subscriptions/status",
            },
        }
    }
}

#[derive(Debug)]
pub enum SubscriptionsState {
    On,
    Off,
    Status,
}

#[derive(thiserror::Error, Debug)]
#[error("must be one of: \"plain\", \"json\"")]
pub struct LogFormatParseError;

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

#[derive(thiserror::Error, Debug)]
#[error("must be one of: \"on\", \"off\", \"status\"")]
pub struct SubscriptionsStateError;

impl std::str::FromStr for SubscriptionsState {
    type Err = SubscriptionsStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "on" => Ok(SubscriptionsState::On),
            "off" => Ok(SubscriptionsState::Off),
            "status" => Ok(SubscriptionsState::Status),
            _ => Err(SubscriptionsStateError),
        }
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct Config {
    pub rpc: config::Config,
}

impl Config {
    pub fn from_file(f: std::fs::File) -> Result<Self> {
        use std::io::{BufReader, Read};
        let mut reader = BufReader::new(f);
        let mut buf = Vec::new();

        reader.read_to_end(&mut buf)?;
        Ok(toml::from_slice(&buf)?)
    }

    pub fn from_options(options: &Options) -> Config {
        let account_info_request_queue_size = options
            .account_info_request_queue_size
            .unwrap_or(crate::DEFAULT_GAI_QUEUE_SIZE);
        let program_accounts_request_queue_size = options
            .program_accounts_request_queue_size
            .unwrap_or(crate::DEFAULT_GPA_QUEUE_SIZE);
        Config {
            rpc: config::Config {
                request_limits: config::RequestLimits {
                    account_info: options.account_info_request_limit,
                    program_accounts: options.program_accounts_request_limit,
                },
                request_queue_size: config::RequestQueueSize {
                    account_info: account_info_request_queue_size,
                    program_accounts: program_accounts_request_queue_size,
                },
                timeouts: config::Timeouts::default(),
                ignore_base58_limit: options.ignore_base58,
            },
        }
    }
}

fn parse_identity(value: &str) -> Result<String, &'static str> {
    match bs58::decode(value).into_vec() {
        Ok(vec) if vec.len() == 32 => Ok(value.into()),
        _ => Err("invalid identity key was provided"),
    }
}
