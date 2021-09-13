#![allow(explicit_outlives_requirements)]

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, HttpResponse, ResponseError};

use awc::Client;
use backoff::backoff::Backoff;
use bytes::Bytes;
use dashmap::mapref::one::Ref;
use futures_util::stream::Stream;
use lru::LruCache;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::{to_raw_value, RawValue};
use smallvec::SmallVec;
use thiserror::Error;
use tokio::stream::StreamExt;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, error, info, warn};

use crate::accounts::Subscription;
use crate::filter::{Filters, NormalizeError};
use crate::metrics::rpc_metrics as metrics;
use crate::types::{
    AccountContext, AccountData, AccountInfo, AccountState, AccountsDb, BytesChain, Commitment,
    Encoding, ProgramAccountsDb, Pubkey, Slot, SolanaContext,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize)]
struct JsonRpcResponse<'a, T> {
    jsonrpc: &'a str,
    result: T,
    id: Id<'a>,
}

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
        if self.data.len() > 128 && encoding.is_base58() {
            return Err(Base58Error);
        }
        Ok(EncodedAccountInfo {
            account_info: self,
            slice,
            encoding,
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
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
            let end = slice
                .offset
                .saturating_add(slice.length)
                .min(self.data.data.len());
            self.data.data.get(slice.offset..end).unwrap_or(&[])
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

pub struct LruEntry(Box<RawValue>);

impl AsRef<RawValue> for LruEntry {
    fn as_ref(&self) -> &RawValue {
        self.0.as_ref()
    }
}

impl From<Box<RawValue>> for LruEntry {
    fn from(inner: Box<RawValue>) -> Self {
        metrics().lru_cache_bytes.add(inner.get().len() as i64);
        LruEntry(inner)
    }
}

impl Drop for LruEntry {
    fn drop(&mut self) {
        metrics().lru_cache_bytes.sub(self.0.get().len() as i64);
    }
}

pub struct State {
    pub accounts: AccountsDb,
    pub program_accounts: ProgramAccountsDb,
    pub client: Client,
    pub pubsub: crate::accounts::PubSubManager,
    pub rpc_url: String,
    pub map_updated: Arc<Notify>,
    pub account_info_request_limit: Arc<Semaphore>,
    pub program_accounts_request_limit: Arc<Semaphore>,
    pub lru: RefCell<LruCache<u64, LruEntry>>,
    pub worker_id: String,
}

impl State {
    fn reset(&self, sub: Subscription, commitment: Commitment, filters: Option<Filters>) {
        self.pubsub.reset(sub, commitment, filters);
    }

    fn insert(&self, key: Pubkey, data: AccountContext, commitment: Commitment) -> Arc<Pubkey> {
        self.accounts.insert(key, data, commitment)
    }

    fn subscription_active(&self, key: Pubkey) -> bool {
        self.pubsub.subscription_active(key)
    }

    fn subscribe(
        &self,
        subscription: Subscription,
        commitment: Commitment,
        filters: Option<Filters>,
    ) {
        self.pubsub.subscribe(subscription, commitment, filters);
        /*
        let res = self
            .actor
            .get()
            .send(AccountCommand::Subscribe(subscription, commitment, filters))
            .await;
        if let Err(err) = res {
            error!(error = %err, message = "error sending command to actor");
        }
        */
    }

    async fn request<T>(
        &self,
        req: &'_ Request<'_, T>,
        limit: &Semaphore,
    ) -> Result<
        impl Stream<Item = Result<Bytes, awc::error::PayloadError>>,
        awc::error::SendRequestError,
    >
    where
        T: Serialize + Debug + ?Sized,
    {
        let client = &self.client;
        let mut backoff = backoff_settings();
        loop {
            let wait_time = metrics()
                .wait_time
                .with_label_values(&[req.method])
                .start_timer();
            let _permit = limit.acquire().await;
            metrics()
                .available_permits
                .with_label_values(&[req.method])
                .observe(limit.available_permits() as f64);
            wait_time.observe_duration();
            let body = client
                .post(&self.rpc_url)
                .timeout(REQUEST_TIMEOUT)
                .send_json(&req)
                .await;
            match body {
                Ok(resp) => {
                    break Ok(resp);
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
#[serde(bound(deserialize = "&'a T: Deserialize<'de>"))]
struct Request<'a, T>
where
    T: ?Sized,
{
    jsonrpc: &'a str,
    id: Id<'a>,
    method: &'a str,
    #[serde(borrow)]
    params: Option<&'a T>,
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
struct RpcErrorOwned {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Box<RawValue>>,
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
pub enum Error<'a> {
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
    #[error("streaming error")]
    Streaming(awc::error::PayloadError),
}

impl From<serde_json::Error> for Error<'_> {
    fn from(_err: serde_json::Error) -> Self {
        Error::Parsing(None)
    }
}

impl From<awc::error::PayloadError> for Error<'_> {
    fn from(err: awc::error::PayloadError) -> Self {
        Error::Streaming(err)
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
            Error::Streaming(_) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(None)),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
struct CommitmentConfig {
    commitment: Commitment,
}

impl Default for CommitmentConfig {
    fn default() -> Self {
        CommitmentConfig {
            commitment: Commitment::Finalized,
        }
    }
}

#[derive(Deserialize, Debug)]
struct AccountInfoConfig {
    encoding: Encoding,
    #[serde(flatten)]
    commitment: Option<CommitmentConfig>,
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

fn parse_params<'a, T: Default + Deserialize<'a>>(
    req: &Request<'a, RawValue>,
) -> Result<(Pubkey, T, u64), Error<'a>> {
    let (params, request_hash): (SmallVec<[&RawValue; 2]>, u64) = match req.params {
        Some(params) => (serde_json::from_str(params.get())?, hash(params.get())),
        None => return Err(Error::NotEnoughArguments(req.id.clone())),
    };

    if params.is_empty() {
        return Err(Error::NotEnoughArguments(req.id.clone()));
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
                req_id: req.id.clone(),
                message: "Invalid param: WrongSize".into(),
                data: None,
            })
        }
    };

    let config: T = {
        if let Some(param) = params.get(1) {
            serde_json::from_str(param.get()).map_err(|err| Error::InvalidParam {
                req_id: req.id.clone(),
                message: format!("Invalid params: {}", err).into(),
                data: None,
            })?
        } else {
            T::default()
        }
    };

    Ok((pubkey, config, request_hash))
}

fn account_response<'a, 'b>(
    req_id: Id<'a>,
    request_hash: u64,
    acc: (Option<&'b AccountInfo>, u64),
    app_state: &web::Data<State>,
    config: AccountInfoConfig,
) -> Result<HttpResponse, Error<'a>> {
    let request_and_slot_hash = hash((request_hash, acc.1));
    if let Some(result) = app_state.lru.borrow_mut().get(&request_and_slot_hash) {
        metrics().lru_cache_hits.inc();
        let resp = JsonRpcResponse {
            jsonrpc: "2.0",
            result: result.as_ref(),
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

    let slot = acc.1;
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
    let resp = JsonRpcResponse {
        jsonrpc: "2.0",
        result: &result,
        id: req_id,
    };
    let body = serde_json::to_vec(&resp)?;
    app_state
        .lru
        .borrow_mut()
        .put(request_and_slot_hash, LruEntry::from(result));

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
    req: Request<'_, RawValue>,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'_>> {
    let (pubkey, config, request_hash) = parse_params::<AccountInfoConfig>(&req)?;

    let commitment = config.commitment.unwrap_or_default().commitment;

    metrics()
        .request_encodings
        .with_label_values(&["getAccountInfo", config.encoding.as_str()])
        .inc();

    metrics()
        .request_commitments
        .with_label_values(&["getAccountInfo", commitment.as_str()])
        .inc();

    let mut cacheable_for_key = Some(pubkey);

    // pass through for JsonParsed as we don't support it yet
    if config.encoding != Encoding::JsonParsed && app_state.subscription_active(pubkey) {
        match app_state.accounts.get(&pubkey) {
            Some(data) => {
                let data = data.value();
                let account = data.get(commitment);
                match account {
                    Some((account_info, slot)) if slot != 0 => {
                        metrics().account_cache_hits.inc();
                        app_state.reset(Subscription::Account(pubkey), commitment, None);
                        return account_response(
                            req.id,
                            request_hash,
                            (account_info, slot),
                            &app_state,
                            config,
                        );
                    }
                    _ => {}
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
                    if let Some(data) = app_state.accounts.get(&pubkey) {
                        let data = data.value();
                        if let Some(account) = data.get(commitment) {
                            app_state.reset(Subscription::Account(pubkey), commitment, None);
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
        struct Wrap {
            #[serde(flatten)]
            inner: Response,
        }
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "lowercase")]
        enum Response {
            Result(AccountContext),
            Error(RpcErrorOwned),
        }
        let app_state = app_state.clone();
        let stream = stream_generator::generate_try_stream(move |mut stream| async move {
            let mut bytes_chain = BytesChain::new();
            {
                let incoming = collect_bytes("getAccountInfo", resp, &mut bytes_chain);

                tokio::pin!(incoming);

                while let Some(bytes) = incoming.next().await {
                    let bytes = bytes.map_err(Error::Streaming)?;
                    stream.send(Ok::<Bytes, Error<'_>>(bytes)).await;
                }
            }

            let res = (|| -> Result<_, Error<'_>> {
                let resp: Wrap = serde_json::from_reader(bytes_chain)?;
                match resp.inner {
                    Response::Result(info) => {
                        debug!(%pubkey, slot = %info.context.slot, ?commitment, "cached for key");
                        app_state.insert(pubkey, info, commitment);
                        app_state.map_updated.notify();
                        app_state.subscribe(Subscription::Account(pubkey), commitment, None);
                    }
                    Response::Error(error) => {
                        metrics()
                            .backend_errors
                            .with_label_values(&["getAccountInfo"])
                            .inc();
                        info!(%pubkey, ?error, "can't cache for key");
                    }
                }
                Ok(())
            })();
            if let Err(res) = res {
                tracing::error!(error = %res, "failed to parse response");
            }

            Ok(())
        });
        return Ok(HttpResponse::Ok()
            .header("x-cache-status", "miss")
            .content_type("application/json")
            .streaming(Box::pin(stream)));
    }
    Ok(HttpResponse::Ok()
        .header("x-cache-status", "miss")
        .content_type("application/json")
        .streaming(resp))
}

#[derive(Error, Debug)]
enum ProgramAccountsResponseError {
    #[error("serialization failed")]
    Serialize(#[from] serde_json::Error),
    #[error("data inconsistency")]
    Inconsistency,
    #[error("base58")]
    Base58,
}

#[derive(Deserialize, Serialize, Debug)]
struct ProgramAccountsConfig<'a> {
    #[serde(default = "Encoding::default")]
    encoding: Encoding,
    #[serde(flatten)]
    commitment: Option<CommitmentConfig>,
    #[serde(rename = "dataSlice")]
    data_slice: Option<Slice>,
    #[serde(borrow)]
    filters: Option<&'a RawValue>,
    #[serde(rename = "withContext")]
    with_context: Option<bool>,
}

impl Default for ProgramAccountsConfig<'_> {
    fn default() -> Self {
        ProgramAccountsConfig {
            encoding: Encoding::Default,
            commitment: None,
            data_slice: None,
            filters: None,
            with_context: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum MaybeContext<T> {
    With { context: SolanaContext, value: T },
    Without(T),
}

impl<T> MaybeContext<T> {
    fn into_slot_and_value(self) -> (Option<Slot>, T) {
        match self {
            Self::With { context, value } => (Some(context.slot), value),
            Self::Without(value) => (None, value),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct AccountAndPubkey {
    pub account: AccountInfo,
    pub pubkey: Pubkey,
}

fn program_accounts_response<'a>(
    req_id: Id<'a>,
    accounts: &HashSet<Arc<Pubkey>>,
    config: &'_ ProgramAccountsConfig<'_>,
    filters: Option<&'a Filters>,
    app_state: &web::Data<State>,
    with_context: bool,
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
                    .map_err(serde::ser::Error::custom)?;
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

    let commitment = config.commitment.unwrap_or_default().commitment;

    let mut encoded_accounts = Vec::with_capacity(accounts.len());
    let mut slot = 0;

    for key in accounts {
        if let Some(data) = app_state.accounts.get(key) {
            let (account_info, current_slot) = match data.value().get(commitment) {
                Some(data) => data,
                None => {
                    warn!("data for key {}/{:?} not found", key, commitment);
                    return Err(ProgramAccountsResponseError::Inconsistency);
                }
            };
            slot = current_slot.max(slot); // TODO: find a better way (store last slot with account set)

            let account_len = account_info
                .as_ref()
                .map(|info| info.data.len())
                .unwrap_or(0);

            // TODO: kinda hacky, find a better way
            if account_len > 128 && config.encoding.is_base58() {
                return Err(ProgramAccountsResponseError::Base58);
            }

            if let Some(filters) = &filters {
                let matches = account_info.map_or(false, |acc| filters.matches(&acc.data));

                if !matches {
                    debug!(pubkey = ?data.key(), "skipped because of filter");
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
                pubkey: **key,
            })
        } else {
            warn!(key = %key, request_id = ?req_id, "data for key not found");
            return Err(ProgramAccountsResponseError::Inconsistency);
        }
    }

    let value = match with_context {
        true => MaybeContext::With {
            context: SolanaContext { slot },
            value: encoded_accounts,
        },
        false => MaybeContext::Without(encoded_accounts),
    };

    let resp = JsonRpcResponse {
        jsonrpc: "2.0",
        result: value,
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
    req: Request<'_, RawValue>,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'_>> {
    let (pubkey, config, _hash) = parse_params::<ProgramAccountsConfig<'_>>(&req)?;
    let return_context = config.with_context;

    let commitment = config.commitment.unwrap_or_default().commitment;

    metrics()
        .request_encodings
        .with_label_values(&["getProgramAccounts", config.encoding.as_str()])
        .inc();

    metrics()
        .request_commitments
        .with_label_values(&["getProgramAccounts", commitment.as_str()])
        .inc();

    let mut cacheable_for_key = Some(pubkey);
    let filters = config
        .filters
        .map(RawValue::get)
        .map(serde_json::from_str::<SmallVec<[_; 3]>>)
        .transpose()?
        .map(Filters::new_normalized);

    let (filters, bad_filters) = match filters {
        Some(Ok(filters)) => (Some(filters), false),
        Some(Err(NormalizeError::Empty)) | None => (None, false), // Empty is ok
        Some(Err(err)) => {
            warn!(%err, "received bad filters. empty response");
            // TODO: consider just opting-out from caching this request
            (None, true)
        }
    };

    let is_json_parsed = config.encoding == Encoding::JsonParsed;
    let has_active_sub = app_state.subscription_active(pubkey);

    if !is_json_parsed && has_active_sub && !bad_filters {
        match app_state.program_accounts.get(&pubkey, filters.clone()) {
            Some(data) => {
                let accounts = data.value();
                if let Some(accounts) = accounts.get(commitment) {
                    app_state.reset(Subscription::Program(pubkey), commitment, filters.clone());
                    metrics().program_accounts_cache_hits.inc();
                    match program_accounts_response(
                        req.id.clone(),
                        accounts,
                        &config,
                        filters.as_ref(),
                        &app_state,
                        return_context.unwrap_or(false),
                    ) {
                        Ok(resp) => return Ok(resp),
                        Err(ProgramAccountsResponseError::Base58) => {
                            return Err(Error::InvalidRequest(Some(req.id.clone()),
                                Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding.")));
                        }
                        Err(_) => {}
                    }
                }
            }
            None => {
                let reason = if config.data_slice.is_some() {
                    Some("data_slice")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    metrics()
                        .response_uncacheable
                        .with_label_values(&["getProgramAccounts", reason])
                        .inc();
                    cacheable_for_key = None;
                }
            }
        }
    } else {
        cacheable_for_key = None;
        let reason = match (is_json_parsed, !has_active_sub, bad_filters) {
            (true, _, _) => "encoding",         // encoding has the highest priority
            (false, true, _) => "inactive sub", // broken sub is second
            (false, false, true) => "bad filters",
            (false, false, false) => "", // this is unreachable
        };

        metrics()
            .response_uncacheable
            .with_label_values(&["getProgramAccounts", reason])
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
                        if let Some(accounts) = data.get(commitment) {
                            app_state.reset(Subscription::Program(pubkey), commitment, filters.clone());
                            if let Ok(resp) = program_accounts_response(
                                req.id.clone(),
                                accounts,
                                &config,
                                filters.as_ref(),
                                &app_state,
                                return_context.unwrap_or(false),
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

    if cacheable_for_key.is_some() {
        let app_state = app_state.clone();
        let stream = program_response_stream::<Vec<AccountAndPubkey>, _, _>(
            app_state,
            (pubkey, commitment),
            filters,
            resp,
            true,
            |err| {
                tracing::error!(error = %err, "failed to parse response");
            },
        );

        return Ok(HttpResponse::Ok()
            .content_type("application/json")
            .header("x-cache-status", "miss")
            .streaming(Box::pin(stream)));
    } else if bad_filters {
        let app_state = app_state.clone();
        let stream = program_response_stream::<[AccountAndPubkey; 0], _, _>(
            app_state,
            (pubkey, commitment),
            filters,
            resp,
            false,
            move |err| {
                error!(pubkey = ?pubkey, error = ?err, "non-empty bad filter response");
            },
        );

        return Ok(HttpResponse::Ok()
            .content_type("application/json")
            .header("x-cache-status", "miss")
            .streaming(Box::pin(stream)));
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .header("x-cache-status", "miss")
        .streaming(Box::pin(resp)))
}

fn program_response_stream<'a, T, S, F>(
    app_state: web::Data<State>,
    (pubkey, commitment): (Pubkey, Commitment),
    filters: Option<Filters>,
    response_stream: S,
    do_cache: bool,
    on_error: F,
) -> impl Stream<Item = Result<Bytes, Error<'a>>> + 'a
where
    T: AsRef<[AccountAndPubkey]> + IntoIterator<Item = AccountAndPubkey> + DeserializeOwned,
    S: Stream<Item = Result<Bytes, awc::error::PayloadError>> + Unpin + 'a,
    F: FnOnce(Error<'a>) + 'a,
{
    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "lowercase")]
    struct Flatten<T> {
        #[serde(flatten)]
        inner: T,
    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "lowercase")]
    enum Response<T> {
        Result(T),
        Error(RpcErrorOwned),
    }

    stream_generator::generate_try_stream(move |mut stream| async move {
        let mut bytes_chain = BytesChain::new();
        {
            let incoming = collect_bytes("getProgramAccounts", response_stream, &mut bytes_chain);

            tokio::pin!(incoming);

            while let Some(bytes) = incoming.next().await {
                let bytes = bytes.map_err(Error::Streaming)?;
                stream.send(Ok::<Bytes, Error<'_>>(bytes)).await;
            }
        }

        let res = (|| -> Result<_, Error<'_>> {
            let (slot, accounts) = {
                let resp: Flatten<Response<MaybeContext<T>>> =
                    serde_json::from_reader(bytes_chain)?;
                match resp.inner {
                    Response::Result(maybe_ctx) => maybe_ctx.into_slot_and_value(),
                    Response::Error(error) => {
                        metrics()
                            .backend_errors
                            .with_label_values(&["getProgramAccounts"])
                            .inc();
                        info!(%pubkey, ?error, "can't cache for key");
                        return Ok(());
                    }
                }
            };
            if do_cache {
                app_state.subscribe(Subscription::Program(pubkey), commitment, filters.clone());

                let mut keys = HashSet::with_capacity(accounts.as_ref().len());
                for acc in accounts {
                    let AccountAndPubkey { account, pubkey } = acc;
                    let key_ref = app_state.insert(
                        pubkey,
                        AccountContext {
                            value: Some(account),
                            context: SolanaContext {
                                slot: slot.unwrap_or(0),
                            },
                        },
                        commitment,
                    );
                    keys.insert(key_ref);
                }
                app_state
                    .program_accounts
                    .insert(pubkey, keys, commitment, filters);
                app_state.map_updated.notify();
            }

            Ok(())
        })();

        if let Err(res) = res {
            on_error(res);
        }

        Ok(())
    })
}

pub async fn rpc_handler(
    body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    let req: Request<'_, _> = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(err) => {
            return Ok(Error::from(err).error_response());
        }
    };

    if req.jsonrpc != "2.0" {
        return Ok(Error::InvalidRequest(Some(req.id), None).error_response());
    }

    macro_rules! observe {
        ($method:expr, $fut:expr) => {{
            metrics().request_types($method).inc();
            let timer = metrics()
                .handler_time
                .with_label_values(&[$method])
                .start_timer();
            let resp = $fut.await;
            timer.observe_duration();
            Ok(resp.unwrap_or_else(|err| err.error_response()))
        }};
    }

    match req.method {
        "getAccountInfo" => {
            return observe!(req.method, get_account_info(req, app_state));
        }
        "getProgramAccounts" => {
            return observe!(req.method, get_program_accounts(req, app_state));
        }
        method => {
            metrics().request_types(method).inc();
        }
    }

    let client = app_state.client.clone();
    let url = app_state.rpc_url.clone();

    let stream = stream_generator::generate_stream(move |mut stream| async move {
        let mut backoff = backoff_settings();
        let total = metrics().passthrough_total_time.start_timer();
        loop {
            let request_time = metrics().passthrough_request_time.start_timer();
            let resp = client
                .post(&url)
                .content_type("application/json")
                .send_body(body.clone())
                .await;
            request_time.observe_duration();
            match resp {
                Ok(mut resp) => {
                    let forward_response_time =
                        metrics().passthrough_forward_response_time.start_timer();
                    while let Some(chunk) = resp.next().await {
                        stream.send(chunk).await;
                    }
                    forward_response_time.observe_duration();
                    break;
                }
                Err(err) => {
                    metrics().passthrough_errors.inc();
                    match backoff.next_backoff() {
                        Some(duration) => tokio::time::delay_for(duration).await,
                        None => {
                            warn!("request error: {:?}", err);
                            // TODO: return error
                            break;
                        }
                    }
                }
            }
        }
        total.observe_duration();
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

fn collect_bytes<'a, E: 'a>(
    method: &'a str,
    incoming: impl Stream<Item = Result<Bytes, E>> + Unpin + 'a,
    bytes_chain: &'a mut BytesChain,
) -> impl Stream<Item = Result<Bytes, E>> + 'a {
    stream_generator::generate_try_stream(move |mut stream| async move {
        let mut resp = incoming;

        let timer = metrics()
            .backend_response_time
            .with_label_values(&[method])
            .start_timer();

        while let Some(bytes) = resp.next().await {
            let bytes = bytes?;
            bytes_chain.push(bytes.clone());
            stream.send(Ok::<Bytes, E>(bytes)).await;
        }
        timer.observe_duration();
        Ok(())
    })
}

pub fn bad_content_type_handler() -> HttpResponse {
    HttpResponse::UnsupportedMediaType()
        .body("Supplied content type is not allowed. Content-Type: application/json is required")
}

pub async fn metrics_handler(
    _body: Bytes,
    _app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    use prometheus::{Encoder, TextEncoder};

    metrics().app_version.set(0);
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let families = prometheus::gather();
    let _ = encoder.encode(&families, &mut buffer);
    Ok(HttpResponse::Ok().content_type("text/plain").body(buffer))
}
