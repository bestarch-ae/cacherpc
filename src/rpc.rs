use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::Addr;
use actix_web::{web, HttpResponse, ResponseError};

use awc::Client;
use backoff::backoff::Backoff;
use bytes::Bytes;
use dashmap::mapref::one::Ref;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json::value::{to_raw_value, RawValue};
use smallvec::SmallVec;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};
use tracing::{error, info, warn};

use crate::accounts::{AccountCommand, AccountUpdateManager, Subscription};
use crate::metrics::rpc_metrics as metrics;
use crate::types::{
    AccountContext, AccountData, AccountInfo, AccountState, AccountsDb, Commitment, Encoding,
    Filter, ProgramAccountsDb, Pubkey, SolanaContext,
};

const BODY_LIMIT: usize = 1024 * 1024 * 100;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Error, Debug)]
#[error("can't encode in base58")]
struct Base58Error;

impl AccountInfo {
    fn encode(
        &self,
        encoding: Encoding,
        slice: Option<Slice>,
    ) -> Result<EncodedAccountInfo<'_>, Base58Error> {
        // Encoded binary (base 58) data should be less than 128 bytes
        if self.data.len() > 128 && encoding == Encoding::Base58 {
            return Err(Base58Error);
        }
        Ok(EncodedAccountInfo {
            account_info: &self,
            slice,
            encoding,
        })
    }
}

#[derive(Debug, Deserialize, Copy, Clone)]
struct Slice {
    offset: usize,
    length: usize,
}

#[derive(Debug)]
struct EncodedAccountInfo<'a> {
    encoding: Encoding,
    slice: Option<Slice>,
    account_info: &'a AccountInfo,
}

impl<'a> EncodedAccountInfo<'a> {
    fn with_context(self, ctx: &'a SolanaContext) -> EncodedAccountContext<'a> {
        EncodedAccountContext {
            value: Some(self),
            context: ctx,
        }
    }
}

impl<'a> Serialize for EncodedAccountInfo<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut account_info = serializer.serialize_struct("AccountInfo", 5)?;
        let encoded_data = EncodedAccountData {
            encoding: self.encoding,
            data: &self.account_info.data,
            slice: self.slice,
        };
        account_info.serialize_field("data", &encoded_data)?;
        account_info.serialize_field("lamports", &self.account_info.lamports)?;
        account_info.serialize_field("owner", &self.account_info.owner)?;
        account_info.serialize_field("executable", &self.account_info.executable)?;
        account_info.serialize_field("rentEpoch", &self.account_info.rent_epoch)?;
        account_info.end()
    }
}

#[derive(Serialize, Debug)]
struct EncodedAccountContext<'a> {
    context: &'a SolanaContext,
    value: Option<EncodedAccountInfo<'a>>,
}

impl<'a> EncodedAccountContext<'a> {
    fn empty(ctx: &'a SolanaContext) -> EncodedAccountContext<'_> {
        EncodedAccountContext {
            context: ctx,
            value: None,
        }
    }
}

struct EncodedAccountData<'a> {
    encoding: Encoding,
    data: &'a AccountData,
    slice: Option<Slice>,
}

impl<'a> Serialize for EncodedAccountData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};

        let data = if let Some(slice) = &self.slice {
            self.data
                .data
                .get(slice.offset..slice.offset + slice.length)
                .ok_or_else(|| Error::custom("bad slice"))?
        } else {
            &self.data.data[..]
        };

        if let Encoding::Default = self.encoding {
            return serializer.serialize_str(&bs58::encode(&data).into_string());
        }

        let mut seq = serializer.serialize_seq(Some(2))?;
        match self.encoding {
            Encoding::Base58 => {
                seq.serialize_element(&bs58::encode(&data).into_string())?;
            }
            Encoding::Base64 => {
                seq.serialize_element(&base64::encode(&data))?;
            }
            Encoding::Base64Zstd => {
                seq.serialize_element(&base64::encode(
                    zstd::encode_all(std::io::Cursor::new(&data), 0)
                        .map_err(|_| Error::custom("can't compress"))?,
                ))?;
            }
            Encoding::JsonParsed => {
                return Err(Error::custom("jsonParsed is not supported yet")); // TODO
            }
            _ => panic!("must not happen, handled above"),
        }
        seq.serialize_element(&self.encoding)?;
        seq.end()
    }
}

pub(crate) struct State {
    pub accounts: AccountsDb,
    pub program_accounts: ProgramAccountsDb,
    pub client: Client,
    pub actor: Addr<AccountUpdateManager>,
    pub rpc_url: String,
    pub map_updated: Arc<Notify>,
    pub account_info_request_limit: Arc<Semaphore>,
    pub program_accounts_request_limit: Arc<Semaphore>,
    pub lru: RefCell<LruCache<u64, Box<RawValue>>>,
    pub worker_id: String,
}

impl State {
    fn get_account(&self, key: &Pubkey) -> Option<Ref<'_, Pubkey, AccountState>> {
        let actor = &self.actor;
        self.accounts.get(key).map(|v| {
            actor.do_send(AccountCommand::Reset(Subscription::Account(*key)));
            v
        })
    }

    fn insert(&self, key: Pubkey, data: AccountContext, commitment: Commitment) {
        self.accounts.insert(key, data, commitment)
    }

    async fn subscribe(
        &self,
        subscription: Subscription,
        commitment: Commitment,
        filters: Option<SmallVec<[Filter; 2]>>,
    ) {
        let res = self
            .actor
            .send(AccountCommand::Subscribe(subscription, commitment, filters))
            .await;
        if let Err(err) = res {
            error!(error = %err, message = "error sending command to actor");
        }
    }

    async fn request(
        &self,
        req: &'_ Request<'_>,
        limit: &Semaphore,
    ) -> Result<Bytes, awc::error::SendRequestError> {
        let client = &self.client;
        let mut backoff = backoff_settings();
        loop {
            let _permit = limit.acquire().await;
            let timer = metrics()
                .backend_response_time
                .with_label_values(&[req.method])
                .start_timer();
            let mut resp = client
                .post(&self.rpc_url)
                .timeout(REQUEST_TIMEOUT)
                .send_json(&req)
                .await?;
            let body = resp.body().limit(BODY_LIMIT).await;
            timer.observe_duration();
            match body {
                Ok(body) => {
                    break Ok(body);
                }
                Err(err) => match backoff.next_backoff() {
                    Some(duration) => tokio::time::delay_for(duration).await,
                    None => {
                        warn!("request: {:?} error: {:?}", req, err);
                        break Err(awc::error::SendRequestError::Timeout);
                    }
                },
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum Id<'a> {
    Null,
    Num(u64),
    Str(&'a str),
}

#[derive(Deserialize, Serialize, Debug)]
struct Request<'a> {
    jsonrpc: &'a str,
    id: Id<'a>,
    method: &'a str,
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

#[derive(Deserialize, Serialize, Debug)]
struct RpcError<'a> {
    code: i64,
    #[serde(borrow)]
    message: Cow<'a, str>,
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Cow<'a, RawValue>>,
}

#[derive(Deserialize, Serialize, Debug)]
struct ErrorResponse<'a> {
    id: Option<Id<'a>>,
    jsonrpc: &'a str,
    error: RpcError<'a>,
}

impl<'a> ErrorResponse<'a> {
    fn not_enough_arguments(id: Id<'a>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id: Some(id),
            error: RpcError {
                code: -32602,
                message: "`params` should have at least 1 argument(s)".into(),
                data: None,
            },
        }
    }

    fn invalid_param(
        id: Id<'a>,
        msg: Cow<'a, str>,
        data: Option<Cow<'a, str>>,
    ) -> ErrorResponse<'a> {
        let data = data
            .and_then(|data| to_raw_value(&data).ok())
            .map(Cow::Owned);
        ErrorResponse {
            jsonrpc: "2.0",
            id: Some(id),
            error: RpcError {
                code: -32602,
                message: msg,
                data,
            },
        }
    }

    fn invalid_request(id: Option<Id<'a>>, msg: Option<&'a str>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32600,
                message: Cow::from(msg.unwrap_or("Invalid request")),
                data: None,
            },
        }
    }

    fn parse_error(id: Option<Id<'a>>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32700,
                message: "Parse error".into(),
                data: None,
            },
        }
    }

    fn gateway_timeout(id: Option<Id<'a>>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32000,
                message: "Gateway timeout".into(),
                data: None,
            },
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error<'a> {
    #[error("invalid request")]
    InvalidRequest(Option<Id<'a>>, Option<&'a str>),
    #[error("invalid param")]
    InvalidParam {
        req_id: Id<'a>,
        message: Cow<'a, str>,
        data: Option<Cow<'a, str>>,
    },
    #[error("parsing request")]
    Parsing(Option<Id<'a>>),
    #[error("not enough arguments")]
    NotEnoughArguments(Id<'a>),
    #[error("backend timeout")]
    Timeout(Id<'a>),
    #[error("forward error")]
    Forward(#[from] awc::error::SendRequestError),
}

impl From<serde_json::Error> for Error<'_> {
    fn from(_err: serde_json::Error) -> Self {
        Error::Parsing(None)
    }
}

impl ResponseError for Error<'_> {
    fn error_response(&self) -> HttpResponse {
        match self {
            Error::InvalidRequest(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::invalid_request(req_id.clone(), *msg)),
            Error::InvalidParam {
                req_id,
                message,
                data,
            } => HttpResponse::Ok().content_type("application/json").json(
                &ErrorResponse::invalid_param(req_id.clone(), message.clone(), data.clone()),
            ),
            Error::Parsing(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::parse_error(req_id.clone())),
            Error::NotEnoughArguments(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::not_enough_arguments(req_id.clone())),
            Error::Timeout(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(Some(req_id.clone()))),
            Error::Forward(_) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(None)),
        }
    }
}

#[derive(Deserialize, Debug)]
struct AccountInfoConfig {
    encoding: Encoding,
    commitment: Option<Commitment>,
    #[serde(rename = "dataSlice")]
    data_slice: Option<Slice>,
}

impl Default for AccountInfoConfig {
    fn default() -> Self {
        AccountInfoConfig {
            encoding: Encoding::Base58,
            commitment: None,
            data_slice: None,
        }
    }
}

fn hash<T: std::hash::Hash>(params: T) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    params.hash(&mut hasher);
    hasher.finish()
}

fn account_response<'a, 'b>(
    req_id: Id<'a>,
    request_hash: u64,
    acc: (Option<&'b AccountInfo>, u64),
    app_state: &web::Data<State>,
    config: AccountInfoConfig,
) -> Result<HttpResponse, Error<'a>> {
    #[derive(Serialize)]
    struct Resp<'a> {
        jsonrpc: &'a str,
        result: &'a RawValue,
        id: Id<'a>,
    }
    let request_and_slot_hash = hash((request_hash, acc.1));
    if let Some(result) = app_state.lru.borrow_mut().get(&request_and_slot_hash) {
        metrics().lru_cache_hits.inc();
        let resp = Resp {
            jsonrpc: "2.0",
            result: &result,
            id: req_id,
        };

        let body = serde_json::to_vec(&resp)?;

        metrics()
            .response_size_bytes
            .with_label_values(&["getAccountInfo"])
            .observe(body.len() as f64);

        return Ok(HttpResponse::Ok()
            .header("x-cache-status", "hit")
            .header("x-cache-type", "lru")
            .content_type("application/json")
            .body(body));
    }

    let slot = app_state
        .accounts
        .get_slot(config.commitment.unwrap_or_default());
    let ctx = SolanaContext { slot };
    let result = acc
            .0
            .as_ref()
            .map(|acc| {
                Ok::<_, Base58Error>(
                    acc.encode(config.encoding, config.data_slice)?
                        .with_context(&ctx),
                )
            })
            .transpose()
            .map_err(|_| Error::InvalidRequest(Some(req_id.clone()),
                    Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding.")))?
            .unwrap_or_else(|| EncodedAccountContext::empty(&ctx));
    let result = serde_json::value::to_raw_value(&result)?;
    let resp = Resp {
        jsonrpc: "2.0",
        result: &result,
        id: req_id,
    };
    let body = serde_json::to_vec(&resp)?;
    app_state
        .lru
        .borrow_mut()
        .put(request_and_slot_hash, result);

    metrics()
        .lru_cache_filled
        .with_label_values(&[&app_state.worker_id])
        .set(app_state.lru.borrow().len() as i64);

    metrics()
        .response_size_bytes
        .with_label_values(&["getAccountInfo"])
        .observe(body.len() as f64);

    Ok(HttpResponse::Ok()
        .header("x-cache-status", "hit")
        .header("x-cache-type", "data")
        .content_type("application/json")
        .body(body))
}

async fn get_account_info(
    req: Request<'_>,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'_>> {
    let (params, request_hash): (SmallVec<[&RawValue; 2]>, _) = match req.params {
        Some(params) => (serde_json::from_str(params.get())?, hash(params.get())),
        None => return Err(Error::NotEnoughArguments(req.id)),
    };

    if params.len() > 2 {
        return Err(Error::InvalidParam {
            req_id: req.id.clone(),
            message: "Expected from 1 to 2 parameters".into(),
            data: Some(format!("Got {}", params.len()).into()),
        });
    }

    let pubkey: Pubkey = match serde_json::from_str(params[0].get()) {
        Err(_) => {
            return Err(Error::InvalidParam {
                req_id: req.id,
                message: "Invalid param: WrongSize".into(),
                data: None,
            })
        }
        Ok(pubkey) => pubkey,
    };
    let config: AccountInfoConfig = {
        if let Some(param) = params.get(1) {
            serde_json::from_str(param.get()).map_err(|err| Error::InvalidParam {
                req_id: req.id.clone(),
                message: format!("Invalid params: {}", err).into(),
                data: None,
            })?
        } else {
            AccountInfoConfig::default()
        }
    };

    metrics()
        .request_encodings
        .with_label_values(&["getAccountInfo", config.encoding.as_str()])
        .inc();

    let mut cacheable_for_key = Some(pubkey);

    // pass through for JsonParsed as we don't support it yet
    if config.encoding != Encoding::JsonParsed {
        match app_state.get_account(&pubkey) {
            Some(data) => {
                let data = data.value();
                let commitment = config.commitment.unwrap_or_default();
                let account = data.get(commitment);
                if let Some(account) = account {
                    metrics().account_cache_hits.inc();
                    return account_response(req.id, request_hash, account, &app_state, config);
                }
            }
            None => {
                if config.data_slice.is_some() {
                    cacheable_for_key = None;
                    metrics()
                        .response_uncacheable
                        .with_label_values(&["getAccountInfo", "data_slice"])
                        .inc();
                }
                app_state
                    .subscribe(
                        Subscription::Account(pubkey),
                        config.commitment.unwrap_or_default(),
                        None,
                    )
                    .await;
            }
        }
    } else {
        cacheable_for_key = None;
        metrics()
            .response_uncacheable
            .with_label_values(&["getAccountInfo", "encoding"])
            .inc();
    }

    let limit = &app_state.account_info_request_limit;
    let wait_for_response = app_state.request(&req, limit);

    tokio::pin!(wait_for_response);

    let resp = loop {
        let notified = app_state.map_updated.notified();
        tokio::select! {
            body = &mut wait_for_response => {
                if let Ok(body) = body {
                    break body;
                } else {
                    info!(%pubkey, ?req.id, "reporting gateway timeout"); // TODO: return proper error
                    return Err(Error::Timeout(req.id.clone()));
                }
            }
            _ = notified => {
                if let Some(pubkey) = cacheable_for_key {
                    if let Some(data) = app_state.get_account(&pubkey) {
                        let data = data.value();
                        let commitment = config.commitment.unwrap_or_default();
                        if let Some(account) = data.get(commitment) {
                            metrics().account_cache_hits.inc();
                            metrics().account_cache_filled.inc();
                            return account_response(
                                    req.id.clone(),
                                    request_hash,
                                    account,
                                    &app_state,
                                    config,
                            );
                        }
                    }
                }
                continue;
            }
        }
    };

    if let Some(pubkey) = cacheable_for_key {
        #[derive(Deserialize, Debug)]
        struct Wrap<'a> {
            #[serde(flatten, borrow)]
            inner: Response<'a>,
        }
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "lowercase")]
        enum Response<'a> {
            Result(AccountContext),
            #[serde(borrow)]
            Error(RpcError<'a>),
        }
        let resp: Wrap<'_> = serde_json::from_slice(&resp)?;
        match resp.inner {
            Response::Result(info) => {
                info!(%pubkey, "cached for key");
                app_state.insert(pubkey, info, config.commitment.unwrap_or_default());
                app_state.map_updated.notify();
            }
            Response::Error(error) => {
                metrics()
                    .backend_errors
                    .with_label_values(&["getAccountInfo"])
                    .inc();
                info!(%pubkey, ?error, "can't cache for key");
                // check cache one more time, maybe another thread was more lucky
                if let Some(data) = app_state.get_account(&pubkey) {
                    let data = data.value();
                    let commitment = config.commitment.unwrap_or_default();
                    if let Some(account) = data.get(commitment) {
                        metrics().account_cache_hits.inc();
                        metrics().account_cache_filled.inc();
                        return account_response(
                            req.id.clone(),
                            request_hash,
                            account,
                            &app_state,
                            config,
                        );
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok()
        .header("x-cache-status", "miss")
        .content_type("application/json")
        .body(resp))
}

#[derive(Error, Debug)]
enum ProgramAccountsResponseError {
    #[error("serialization failed")]
    Serialize(#[from] serde_json::Error),
    #[error("data inconsistency")]
    Inconsistency,
}

#[derive(Deserialize, Debug)]
struct ProgramAccountsConfig {
    #[serde(default = "Encoding::default")]
    encoding: Encoding,
    commitment: Option<Commitment>,
    #[serde(rename = "dataSlice")]
    data_slice: Option<Slice>,
    filters: Option<SmallVec<[Filter; 2]>>,
}

impl Default for ProgramAccountsConfig {
    fn default() -> Self {
        ProgramAccountsConfig {
            encoding: Encoding::Default,
            commitment: None,
            data_slice: None,
            filters: None,
        }
    }
}

fn program_accounts_response<'a>(
    req_id: Id<'a>,
    accounts: &HashSet<Pubkey>,
    config: &'_ ProgramAccountsConfig,
    app_state: &web::Data<State>,
) -> Result<HttpResponse, ProgramAccountsResponseError> {
    struct Encode<'a, K> {
        inner: Ref<'a, K, AccountState>,
        encoding: Encoding,
        slice: Option<Slice>,
        commitment: Commitment,
    }

    impl<'a, K> Serialize for Encode<'a, K>
    where
        K: Eq + std::hash::Hash,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if let Some((Some(value), _)) = self.inner.value().get(self.commitment) {
                let encoded = value
                    .encode(self.encoding, self.slice)
                    .map_err(|_| serde::ser::Error::custom("fuck"))?;
                encoded.serialize(serializer)
            } else {
                // shouldn't happen
                serializer.serialize_none()
            }
        }
    }

    #[derive(Serialize)]
    struct AccountAndPubkey<'a> {
        account: Encode<'a, Pubkey>,
        pubkey: Pubkey,
    }

    let commitment = config.commitment.unwrap_or_default();

    let filters = &config.filters;

    let mut encoded_accounts = Vec::with_capacity(accounts.len());

    for key in accounts {
        if let Some(data) = app_state.get_account(&key) {
            if data.value().get(commitment).is_none() {
                warn!("data for key {}/{:?} not found", key, commitment);
                return Err(ProgramAccountsResponseError::Inconsistency);
            }

            if let Some(filters) = &filters {
                let matches = filters.iter().all(|f| {
                    let value = data.value().get(commitment).unwrap(); // checked above
                    value
                        .0
                        .as_ref()
                        .map(|val| f.matches(&val.data))
                        .unwrap_or(false)
                });
                if !matches {
                    info!(pubkey = ?data.key(), "skipped because of filter");
                    continue;
                }
            }

            encoded_accounts.push(AccountAndPubkey {
                account: Encode {
                    inner: data,
                    encoding: config.encoding,
                    slice: config.data_slice,
                    commitment,
                },
                pubkey: *key,
            })
        }
    }
    #[derive(Serialize)]
    struct Resp<'a> {
        jsonrpc: &'a str,
        result: Vec<AccountAndPubkey<'a>>,
        id: Id<'a>,
    }
    let resp = Resp {
        jsonrpc: "2.0",
        result: encoded_accounts,
        id: req_id,
    };

    let body = serde_json::to_vec(&resp)?;
    metrics()
        .response_size_bytes
        .with_label_values(&["getProgramAccounts"])
        .observe(body.len() as f64);
    Ok(HttpResponse::Ok()
        .header("x-cache-status", "hit")
        .header("x-cache-type", "data")
        .content_type("application/json")
        .body(body))
}

async fn get_program_accounts(
    req: Request<'_>,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'_>> {
    let params: SmallVec<[&RawValue; 2]> = match req.params {
        Some(params) => serde_json::from_str(params.get())?,
        None => SmallVec::new(),
    };
    if params.is_empty() {
        return Err(Error::NotEnoughArguments(req.id));
    }
    if params.len() > 2 {
        return Err(Error::InvalidParam {
            req_id: req.id.clone(),
            message: "Expected from 1 to 2 parameters".into(),
            data: Some(format!("Got {}", params.len()).into()),
        });
    }
    let pubkey: Pubkey = match serde_json::from_str(params[0].get()) {
        Ok(pubkey) => pubkey,
        Err(_) => {
            return Err(Error::InvalidParam {
                req_id: req.id,
                message: "Invalid param: WrongSize".into(),
                data: None,
            })
        }
    };
    let config: ProgramAccountsConfig = {
        if let Some(param) = params.get(1) {
            serde_json::from_str(param.get()).map_err(|err| Error::InvalidParam {
                req_id: req.id.clone(),
                message: format!("Invalid params: {}", err).into(),
                data: None,
            })?
        } else {
            ProgramAccountsConfig::default()
        }
    };

    metrics()
        .request_encodings
        .with_label_values(&["getProgramAccounts", config.encoding.as_str()])
        .inc();

    let mut cacheable_for_key = Some(pubkey);

    let filters: Option<SmallVec<[Filter; 2]>> = if let Some(filters) = &config.filters {
        let mut filters = filters.clone();
        filters.sort_unstable();
        Some(filters)
    } else {
        None
    };
    let uncacheable_filters = false; // TODO

    if config.encoding != Encoding::JsonParsed {
        match app_state.program_accounts.get(&pubkey, filters.clone()) {
            Some(data) => {
                let accounts = data.value();
                if let Some(accounts) = accounts.get(config.commitment.unwrap_or_default()) {
                    metrics().program_accounts_cache_hits.inc();
                    if let Ok(resp) =
                        program_accounts_response(req.id.clone(), accounts, &config, &app_state)
                    {
                        return Ok(resp);
                    }
                }
            }
            None => {
                let reason = match (config.data_slice.is_some(), uncacheable_filters) {
                    (true, true) => Some("data_slice_and_filters"),
                    (true, false) => Some("data_slice"),
                    (false, true) => Some("filters"),
                    (false, false) => None,
                };
                if let Some(reason) = reason {
                    metrics()
                        .response_uncacheable
                        .with_label_values(&["getProgramAccounts", reason])
                        .inc();
                    cacheable_for_key = None;
                }
                app_state
                    .subscribe(
                        Subscription::Program(pubkey),
                        config.commitment.unwrap_or_default(),
                        filters.clone(),
                    )
                    .await;
            }
        }
    } else {
        cacheable_for_key = None;
        metrics()
            .response_uncacheable
            .with_label_values(&["getProgramAccounts", "encoding"])
            .inc();
    }

    let limit = &app_state.program_accounts_request_limit;
    let wait_for_response = app_state.request(&req, limit);

    tokio::pin!(wait_for_response);

    let resp = loop {
        let notified = app_state.map_updated.notified();
        tokio::select! {
            body = &mut wait_for_response => {
                if let Ok(body) = body {
                    break body;
                } else {
                    info!(?req.id, "reporting gateway timeout"); // TODO: return proper error
                    return Err(Error::Timeout(req.id.clone()));
                }
            }
            _ = notified => {
                if let Some(program_pubkey) = cacheable_for_key {
                    if let Some(data) = app_state.program_accounts.get(&program_pubkey, filters.clone()) {
                        let data = data.value();
                        metrics().program_accounts_cache_filled.inc();
                        if let Some(accounts) = data.get(config.commitment.unwrap_or_default()) {
                            if let Ok(resp) = program_accounts_response(
                                req.id.clone(),
                                accounts,
                                &config,
                                &app_state,
                            ) {
                                return Ok(resp);
                            }
                        }
                    }
                }
                continue;
            }
        }
    };

    if let Some(program_pubkey) = cacheable_for_key {
        #[derive(Deserialize, Debug)]
        struct AccountAndPubkey {
            account: AccountInfo,
            pubkey: Pubkey,
        }
        #[derive(Deserialize, Debug)]
        struct Wrap<'a> {
            #[serde(flatten, borrow)]
            inner: Response<'a>,
        }
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "lowercase")]
        enum Response<'a> {
            Result(Vec<AccountAndPubkey>),
            #[serde(borrow)]
            Error(RpcError<'a>),
        }
        let resp: Wrap<'_> = serde_json::from_slice(&resp)?;
        match resp.inner {
            Response::Result(result) => {
                let mut keys = HashSet::with_capacity(result.len());
                for acc in result {
                    let AccountAndPubkey { account, pubkey } = acc;
                    app_state.insert(
                        pubkey,
                        AccountContext {
                            value: Some(account),
                            context: SolanaContext { slot: 0 },
                        },
                        config.commitment.unwrap_or_default(),
                    );
                    keys.insert(pubkey);
                }
                app_state.program_accounts.insert(
                    program_pubkey,
                    keys,
                    config.commitment.unwrap_or_default(),
                    filters,
                );
                app_state.map_updated.notify();
            }
            Response::Error(error) => {
                metrics()
                    .backend_errors
                    .with_label_values(&["getProgramAccounts"])
                    .inc();
                info!(%pubkey, ?error, "can't cache for key");
            }
        }
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .header("x-cache-status", "miss")
        .body(resp))
}

pub(crate) async fn rpc_handler(
    body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    let req: Request<'_> = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(err) => {
            return Ok(Error::from(err).error_response());
        }
    };

    if req.jsonrpc != "2.0" {
        return Ok(Error::InvalidRequest(Some(req.id), None).error_response());
    }

    match req.method {
        "getAccountInfo" => {
            metrics().request_types("getAccountInfo").inc();
            let timer = metrics()
                .handler_time
                .with_label_values(&["getAccountInfo"])
                .start_timer();
            let resp = get_account_info(req, app_state).await;
            timer.observe_duration();
            return Ok(resp.unwrap_or_else(|err| err.error_response()));
        }
        "getProgramAccounts" => {
            metrics().request_types("getProgramAccounts").inc();
            let timer = metrics()
                .handler_time
                .with_label_values(&["getProgramAccounts"])
                .start_timer();
            let resp = get_program_accounts(req, app_state).await;
            timer.observe_duration();
            return Ok(resp.unwrap_or_else(|err| err.error_response()));
        }
        method => {
            metrics().request_types(method).inc();
        }
    }

    let client = app_state.client.clone();
    let url = app_state.rpc_url.clone();

    let stream = stream_generator::generate_stream(move |mut stream| async move {
        use tokio::stream::StreamExt;
        let mut backoff = backoff_settings();
        loop {
            let resp = client
                .post(&url)
                .content_type("application/json")
                .send_body(body.clone())
                .await;
            match resp {
                Ok(mut resp) => {
                    while let Some(chunk) = resp.next().await {
                        stream.send(chunk).await;
                    }
                    return;
                }
                Err(err) => match backoff.next_backoff() {
                    Some(duration) => tokio::time::delay_for(duration).await,
                    None => {
                        warn!("request error: {:?}", err);
                        // TODO: return error
                        return;
                    }
                },
            }
        }
    });

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .streaming(Box::pin(stream)))
}

fn backoff_settings() -> backoff::ExponentialBackoff {
    backoff::ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(5),
        max_elapsed_time: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

pub(crate) async fn metrics_handler(
    _body: Bytes,
    _app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    use prometheus::{Encoder, TextEncoder};
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let families = prometheus::gather();
    let _ = encoder.encode(&families, &mut buffer);
    Ok(HttpResponse::Ok().content_type("text/plain").body(buffer))
}
