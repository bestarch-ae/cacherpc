#![allow(explicit_outlives_requirements)]

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use actix_http::error::PayloadError;
use actix_web::{web, HttpResponse, ResponseError};
use arc_swap::ArcSwap;
use awc::Client;
use backoff::backoff::Backoff;
use bytes::Bytes;
use dashmap::mapref::one::Ref;
use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use lru::LruCache;
use mlua::{Lua, LuaOptions, StdLib};
use prometheus::IntCounter;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::{to_raw_value, RawValue};
use smallvec::SmallVec;
use thiserror::Error;
use tokio::sync::{watch, Notify, Semaphore};
use tracing::{debug, error, info, warn};

use crate::filter::{Filter, Filters};
use crate::metrics::rpc_metrics as metrics;
use crate::pubsub::{PubSubManager, Subscription, SubscriptionActive};
use crate::types::{
    AccountContext, AccountData, AccountInfo, AccountState, AccountsDb, BytesChain, Commitment,
    Encoding, ProgramAccountsDb, Pubkey, SemaphoreQueue, Slot, SolanaContext,
};

#[cfg(feature = "jsonparsed")]
use solana_sdk::pubkey::Pubkey as SolanaPubkey;

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
        enforce_base58_limit: bool,
        pubkey: Pubkey,
    ) -> Result<EncodedAccountInfo<'_>, Base58Error> {
        // Encoded binary (base 58) data should be less than 128 bytes
        if enforce_base58_limit && self.data.len() > 128 && encoding.is_base58() {
            return Err(Base58Error);
        }
        Ok(EncodedAccountInfo {
            account_info: self,
            slice,
            encoding,
            pubkey,
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
    pubkey: Pubkey,
}

#[derive(Debug)]
struct EncodedAccountInfoOwned {
    encoding: Encoding,
    slice: Option<Slice>,
    account_info: AccountInfo,
    pubkey: Pubkey,
}

impl<'a> From<EncodedAccountInfo<'a>> for EncodedAccountInfoOwned {
    fn from(info: EncodedAccountInfo<'a>) -> Self {
        EncodedAccountInfoOwned {
            encoding: info.encoding,
            slice: info.slice,
            account_info: info.account_info.to_owned(),
            pubkey: info.pubkey,
        }
    }
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
            #[cfg(feature = "jsonparsed")]
            owner: self.account_info.owner,
            #[cfg(feature = "jsonparsed")]
            pubkey: self.pubkey,
        };
        account_info.serialize_field("data", &encoded_data)?;
        account_info.serialize_field("lamports", &self.account_info.lamports)?;
        account_info.serialize_field("owner", &self.account_info.owner)?;
        account_info.serialize_field("executable", &self.account_info.executable)?;
        account_info.serialize_field("rentEpoch", &self.account_info.rent_epoch)?;
        account_info.end()
    }
}

impl Serialize for EncodedAccountInfoOwned {
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
            #[cfg(feature = "jsonparsed")]
            owner: self.account_info.owner,
            #[cfg(feature = "jsonparsed")]
            pubkey: self.pubkey,
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

#[derive(Serialize, Debug)]
struct EncodedAccountContextOwned {
    context: SolanaContext,
    value: Option<EncodedAccountInfoOwned>,
}

impl<'a> From<EncodedAccountContext<'a>> for EncodedAccountContextOwned {
    fn from(reference: EncodedAccountContext<'a>) -> Self {
        Self {
            context: reference.context.clone(),
            value: reference.value.map(|value| value.into()),
        }
    }
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
    #[cfg(feature = "jsonparsed")]
    pubkey: Pubkey,
    #[cfg(feature = "jsonparsed")]
    owner: Pubkey,
}

impl<'a> Serialize for EncodedAccountData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};

        let encoding = self.encoding;

        #[cfg(feature = "jsonparsed")]
        if let Encoding::JsonParsed = self.encoding {
            use solana_account_decoder::{
                parse_account_data::{parse_account_data, AccountAdditionalData},
                parse_token::get_token_account_mint,
            };

            let pubkey = SolanaPubkey::new_from_array(self.pubkey.into());
            let program_id = SolanaPubkey::new_from_array(self.owner.into());

            let additional_data = get_token_account_mint(&self.data.data)
                .map(|key| get_mint_decimals(&key).ok())
                .map(|decimals| AccountAdditionalData {
                    spl_token_decimals: decimals,
                });

            match parse_account_data(&pubkey, &program_id, &self.data.data, additional_data) {
                Ok(parsed_acc) => {
                    return parsed_acc.serialize(serializer);
                }
                Err(_err) => {
                    // if we failed to parse the data, try to pass the request on to validator
                    return Err(Error::custom("Couldn't parse cached data"));
                }
            }
        }

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

        match encoding {
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
                #[cfg_attr(feature = "jsonparsed", allow(unused))]
                return Err(Error::custom("jsonParsed encoding is not supported"));
            }
            Encoding::Default => {
                panic!("default encoding should've been handled before");
            }
        }
        seq.serialize_element(&encoding)?;
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

struct SubDescriptor {
    kind: Subscription,
    commitment: Commitment,
    filters: Option<Filters>,
}

pub struct State {
    pub accounts: AccountsDb,
    pub program_accounts: ProgramAccountsDb,
    pub client: Client,
    pub pubsub: PubSubManager,
    pub rpc_url: String,
    pub map_updated: Arc<Notify>,
    pub account_info_request_limit: Arc<SemaphoreQueue>,
    pub program_accounts_request_limit: Arc<SemaphoreQueue>,
    pub config: Arc<ArcSwap<Config>>,
    pub config_watch: RefCell<watch::Receiver<Config>>,
    pub waf_watch: RefCell<watch::Receiver<()>>,
    pub lru: RefCell<LruCache<u64, LruEntry>>,
    pub worker_id: String,
    pub waf: Option<Waf>,
    pub serialize_threads_limit: Arc<Semaphore>,
}

pub struct Waf {
    lua: Lua,
    path: PathBuf,
}

use anyhow::Context;
impl Waf {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, anyhow::Error> {
        const LUA_JSON: &str = include_str!("json.lua");
        let lua = Lua::new_with(
            StdLib::MATH | StdLib::STRING | StdLib::PACKAGE,
            LuaOptions::default(),
        )?;

        let func = lua
            .load(LUA_JSON)
            .into_function()
            .with_context(|| "Error parsing lua file")?;

        let _: mlua::Value<'_> = lua
            .load_from_function("json", func)
            .with_context(|| "Error loading WAF function")?;

        let waf = Waf {
            lua,
            path: path.as_ref().to_path_buf(),
        };

        waf.reload()?;

        Ok(waf)
    }

    pub fn reload(&self) -> Result<(), anyhow::Error> {
        let rules =
            std::fs::read_to_string(&self.path).with_context(|| "Error reading from lua file")?;

        let rules = self
            .lua
            .load(&rules)
            .into_function()
            .with_context(|| "Error parsing lua file")?;

        self.lua
            .unload("waf")
            .with_context(|| "Error unloading old WAF function")?;
        let _: mlua::Value<'_> = self
            .lua
            .load_from_function("waf", rules)
            .with_context(|| "Error loading WAF function")?;

        info!("loaded WAF rules");
        Ok(())
    }
}

impl State {
    fn reset(&self, sub: SubDescriptor, owner: Option<Pubkey>) {
        self.pubsub
            .reset(sub.kind, sub.commitment, sub.filters, owner);
    }

    fn insert(&self, key: Pubkey, data: AccountContext, commitment: Commitment) -> Arc<Pubkey> {
        self.accounts.insert(key, data, commitment)
    }

    fn websocket_connected(&self, key: (Pubkey, Commitment)) -> bool {
        self.pubsub.websocket_connected(key)
    }

    // owner is the subscription for program, if given account belongs to one
    fn subscription_active(
        &self,
        sub: Subscription,
        commitment: Commitment,
        owner: Option<Pubkey>,
    ) -> SubscriptionActive {
        self.pubsub.subscription_active(sub, commitment, owner)
    }

    fn is_caching_allowed(&self) -> bool {
        self.pubsub.can_subscribe()
    }

    fn subscribe(&self, sub: SubDescriptor, owner: Option<Pubkey>) {
        self.pubsub
            .subscribe(sub.kind, sub.commitment, sub.filters, owner);
    }

    // used to unsubscribe from accounts of program, after owner
    // is inserted into cache
    fn unsubscribe(&self, key: Pubkey, commitment: Commitment) {
        self.pubsub.unsubscribe(key, commitment);
    }

    // clippy incorrectly assumes that lifetimes can be elided
    #[allow(clippy::needless_lifetimes)]
    async fn request<'a, T>(
        &self,
        req: &Request<'a, T>,
        limit: &SemaphoreQueue,
    ) -> Result<impl Stream<Item = Result<Bytes, awc::error::PayloadError>>, Error<'a>>
    where
        T: Serialize + Debug + ?Sized,
    {
        let client = &self.client;
        let mut backoff = backoff_settings(60);
        loop {
            let wait_time = metrics()
                .wait_time
                .with_label_values(&[req.method])
                .start_timer();

            let _permit = match limit.acquire().await {
                Some(permit) => permit, // on success will move out of wait queue
                None => {
                    warn!(?req, "wait queue on request type has been filled up");
                    return Err(Error::Internal(
                        Some(req.id.clone()),
                        Cow::from("Wait limit for RPC requests has been exhausted"),
                    ));
                }
            };

            metrics()
                .available_permits
                .with_label_values(&[req.method])
                .observe(limit.available_permits() as f64);
            wait_time.observe_duration();
            let body = client
                .post(&self.rpc_url)
                .append_header(("X-Cache-Request-Method", req.method))
                .timeout(REQUEST_TIMEOUT)
                .send_json(&req)
                .await;
            metrics()
                .backend_requests_count
                .with_label_values(&[req.method])
                .inc(); // count request attempts, even failed ones
            match body {
                Ok(resp) => {
                    break Ok(resp);
                }
                Err(err) => match backoff.next_backoff() {
                    Some(duration) => {
                        metrics().request_retries.inc();
                        tokio::time::sleep(duration).await;
                    }
                    None => {
                        warn!(?req, error=%err, "reporting gateway timeout");
                        break Err(Error::Timeout(req.id.clone()));
                    }
                },
            }
        }
    }

    async fn process_request<T: Cacheable + fmt::Display>(
        self: Arc<Self>,
        raw_request: Request<'_, RawValue>,
    ) -> CacheResult<'_> {
        let request = T::parse(&raw_request)?;
        let (mut is_cacheable, mut can_use_cache) = (true, true);
        let mut uncachable_reason = "";
        if let Err(reason) = request.is_cacheable(&self) {
            is_cacheable = reason.can_use_cache();
            can_use_cache = reason.can_use_cache();
            uncachable_reason = reason.as_str();
        }

        if is_cacheable {
            if let Some(data) = get_from_cache(&request, &raw_request.id, &self).await {
                T::cache_hit_counter().inc();
                let owner = data.owner();
                self.reset(request.sub_descriptor(), owner);
                if request.has_active_subscription(&self, owner).await {
                    return data.map(|data| data.response);
                }
            };
        } else {
            metrics()
                .response_uncacheable
                .with_label_values(&[T::REQUEST_TYPE, uncachable_reason])
                .inc();
        }

        let wait_for_response = self.request(&raw_request, T::get_limit(&self));
        tokio::pin!(wait_for_response);

        let resp = loop {
            let notified = self.map_updated.notified();
            tokio::select! {
                body = &mut wait_for_response => {
                    break body?;
                }
                _ = notified, if can_use_cache => {
                    if let Some(data) = get_from_cache(&request, &raw_request.id, &self).await {
                        T::cache_hit_counter().inc();
                        T::cache_filled_counter().inc();
                        self.reset(request.sub_descriptor(), data.owner());
                        return data.map(|data| data.response);
                    }
                    continue;
                }
            }
        };

        let mut response = HttpResponse::Ok();
        response
            .append_header(("x-cache-status", "miss"))
            .content_type("application/json");

        let resp = resp.map_err(|err| {
            error!(error = %err, "error while streaming response");
            metrics().streaming_errors.inc();
            err
        });

        if is_cacheable {
            let this = Arc::clone(&self);
            let stream = stream_generator::generate_try_stream(move |mut stream| async move {
                let mut bytes_chain = BytesChain::new();
                {
                    let incoming = collect_bytes(T::REQUEST_TYPE, resp, &mut bytes_chain);
                    tokio::pin!(incoming);

                    while let Some(bytes) = incoming.next().await {
                        let bytes = bytes.map_err(Error::Streaming)?;
                        stream.send(Ok::<Bytes, Error<'_>>(bytes)).await;
                    }
                }

                let resp = serde_json::from_reader(bytes_chain)
                    .map(|wrap: Flatten<Response<T::ResponseData>>| wrap.inner);

                match resp {
                    Ok(Response::Result(data)) => {
                        let owner = data.owner();
                        if this.is_caching_allowed() && request.put_into_cache(&this, data) {
                            debug!(%request, "cached for key");
                            this.map_updated.notify_waiters();
                            this.subscribe(request.sub_descriptor(), owner);
                        }
                    }
                    Ok(Response::Error(error)) => {
                        metrics()
                            .backend_errors
                            .with_label_values(&[T::REQUEST_TYPE])
                            .inc();
                        info!(%request, ?error, "can't cache for key");
                    }
                    Err(err) => request.handle_parse_error(err.into()),
                }

                Ok(())
            });
            Ok(response.streaming(Box::pin(stream)))
        } else {
            Ok(response.streaming(resp))
        }
    }
}

struct CachedResponse {
    owner: Option<Pubkey>,
    response: HttpResponse,
}

type CacheResult<'a> = Result<HttpResponse, Error<'a>>;

trait HasOwner {
    fn owner(&self) -> Option<Pubkey> {
        None
    }
}

impl<'a> HasOwner for Result<CachedResponse, Error<'a>> {
    fn owner(&self) -> Option<Pubkey> {
        self.as_ref().ok().map(|data| data.owner).flatten()
    }
}

impl HasOwner for AccountContext {
    fn owner(&self) -> Option<Pubkey> {
        self.value.as_ref().map(|value| value.owner)
    }
}

impl HasOwner for MaybeContext<Vec<AccountAndPubkey>> {}

enum CacheableType<'a> {
    GetAccountInfo(&'a GetAccountInfo),
    GetProgramAccounts(&'a GetProgramAccounts),
}

trait Cacheable: Sized + 'static {
    const REQUEST_TYPE: &'static str;
    type ResponseData: DeserializeOwned + HasOwner;

    fn parse<'a>(request: &Request<'a, RawValue>) -> Result<Self, Error<'a>>;
    fn get_limit(state: &State) -> &SemaphoreQueue;

    fn is_cacheable(&self, state: &State) -> Result<(), UncacheableReason>;
    // method to check whether cached entry has corresponding websocket subscription
    fn has_active_subscription(&self, state: &State, owner: Option<Pubkey>) -> SubscriptionActive;

    fn get_type(&self) -> CacheableType<'_>;

    fn put_into_cache(&self, state: &State, data: Self::ResponseData) -> bool;

    fn sub_descriptor(&self) -> SubDescriptor;

    fn handle_parse_error(&self, err: Error<'_>) {
        tracing::error!(error = %err, "failed to parse response");
    }

    // Metrics
    fn cache_hit_counter<'a>() -> &'a IntCounter;
    fn cache_filled_counter<'a>() -> &'a IntCounter;
}

macro_rules! emit_request_metrics {
    ($req:expr) => {
        metrics()
            .request_encodings
            .with_label_values(&[Self::REQUEST_TYPE, $req.encoding.as_str()])
            .inc();
        metrics()
            .request_commitments
            .with_label_values(&[
                Self::REQUEST_TYPE,
                $req.commitment
                    .map_or_else(Commitment::default, |c| c.commitment)
                    .as_str(),
            ])
            .inc();
    };
}

struct GetAccountInfo {
    pubkey: Pubkey,
    config: AccountInfoConfig,
    config_hash: u64,
}

impl GetAccountInfo {
    fn commitment(&self) -> Commitment {
        self.config
            .commitment
            .map_or_else(Commitment::default, |commitment| commitment.commitment)
    }

    async fn get_from_cache<'a, 'b>(
        &'a self,
        id: &Id<'b>,
        state: &State,
    ) -> Option<Result<CachedResponse, Error<'b>>> {
        let data = state.accounts.get(&self.pubkey)?;
        let mut account = data.value().get(self.commitment());
        let owner = account.and_then(|(info, _)| info).map(|info| info.owner);

        account = match account {
            Some((Some(info), slot)) if slot == 0 => state
                .program_accounts
                .get_slot(&(info.owner, self.commitment()))
                .map(|slot| (Some(info), slot)),
            acc => acc,
        };

        match account.filter(|(_, slot)| *slot != 0) {
            Some(data) => {
                let resp = account_response(
                    id.clone(),
                    self.config_hash,
                    data,
                    state,
                    &self.config,
                    self.pubkey,
                )
                .await;
                match resp {
                    Ok(res) => Some(Ok(CachedResponse {
                        response: res,
                        owner,
                    })),
                    Err(Error::Parsing(_)) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            _ => None,
        }
    }
}

impl Cacheable for GetAccountInfo {
    const REQUEST_TYPE: &'static str = "getAccountInfo";
    type ResponseData = AccountContext;

    fn parse<'a>(request: &Request<'a, RawValue>) -> Result<Self, Error<'a>> {
        let this = parse_params(request).map(|(pubkey, config, config_hash)| Self {
            pubkey,
            config,
            config_hash,
        })?;
        emit_request_metrics!(this.config);
        Ok(this)
    }

    fn get_limit(state: &State) -> &SemaphoreQueue {
        state.account_info_request_limit.as_ref()
    }

    fn get_type(&self) -> CacheableType<'_> {
        CacheableType::GetAccountInfo(self)
    }

    // for getAccountInfo requests, we don't need to subscribe in case if the owner program exists,
    // and there's already an active subscription present for it
    fn has_active_subscription(&self, state: &State, owner: Option<Pubkey>) -> SubscriptionActive {
        state.subscription_active(Subscription::Account(self.pubkey), self.commitment(), owner)
    }

    fn is_cacheable(&self, state: &State) -> Result<(), UncacheableReason> {
        if self.config.encoding == Encoding::JsonParsed {
            Err(UncacheableReason::Encoding)
        } else if self.config.data_slice.is_some() {
            Err(UncacheableReason::DataSlice)
        } else if !state.websocket_connected((self.pubkey, self.commitment())) {
            Err(UncacheableReason::Disconnected)
        } else {
            Ok(())
        }
    }

    fn put_into_cache(&self, state: &State, data: Self::ResponseData) -> bool {
        state.insert(self.pubkey, data, self.commitment());
        true
    }

    fn sub_descriptor(&self) -> SubDescriptor {
        SubDescriptor {
            kind: Subscription::Account(self.pubkey),
            commitment: self.commitment(),
            filters: None,
        }
    }

    fn cache_hit_counter<'a>() -> &'a IntCounter {
        &metrics().account_cache_hits
    }
    fn cache_filled_counter<'a>() -> &'a IntCounter {
        &metrics().account_cache_filled
    }
}

impl fmt::Display for GetAccountInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "getAccountInfo {{ pubkey: {}, commitment: {:?} }}",
            self.pubkey,
            self.commitment()
        )
    }
}

struct GetProgramAccounts {
    pubkey: Pubkey,
    config: ProgramAccountsConfig,
    filters: Option<Filters>,
    valid_filters: bool,
}

impl GetProgramAccounts {
    fn commitment(&self) -> Commitment {
        self.config
            .commitment
            .map_or_else(Commitment::default, |commitment| commitment.commitment)
    }
    async fn get_from_cache<'a, 'b>(
        &'a self,
        id: &Id<'b>,
        state: &State,
    ) -> Option<Result<CachedResponse, Error<'b>>> {
        let with_context = self.config.with_context.unwrap_or(false);
        let commitment = self.commitment();
        let filters = self.filters.as_ref();
        let config = &self.config;

        let entry = state
            .program_accounts
            .get_state((self.pubkey, commitment))?;
        let accounts = entry.value().get_account_keys(&self.filters)?;
        if accounts.is_empty() {
            return None;
        }
        let res =
            program_accounts_response(id.clone(), accounts, config, filters, state, with_context)
                .await;
        match res {
            Ok(res) => Some(Ok(CachedResponse {
                owner: None,
                response: res,
            })),
            Err(ProgramAccountsResponseError::Base58) => Some(Err(base58_error(id.clone()))),
            Err(_) => None,
        }
    }
}

// hacky generic to concrete type cast
async fn get_from_cache<'a, 'b, T: Cacheable>(
    req: &'a T,
    id: &Id<'b>,
    state: &State,
) -> Option<Result<CachedResponse, Error<'b>>> {
    match req.get_type() {
        CacheableType::GetProgramAccounts(req) => req.get_from_cache(id, state).await,
        CacheableType::GetAccountInfo(req) => req.get_from_cache(id, state).await,
    }
}

impl Cacheable for GetProgramAccounts {
    const REQUEST_TYPE: &'static str = "getProgramAccounts";
    type ResponseData = MaybeContext<Vec<AccountAndPubkey>>;

    fn parse<'a>(request: &Request<'a, RawValue>) -> Result<Self, Error<'a>> {
        let (pubkey, config, _) = parse_params::<ProgramAccountsConfig>(request)?;

        let (filters, valid_filters) = match config.filters.as_ref() {
            Some(MaybeFilters::Valid(filters)) => (Some(filters.clone()), true),
            Some(MaybeFilters::Invalid(vec)) if vec.is_empty() => (None, true), // Empty is ok
            Some(MaybeFilters::Invalid(_vec)) => (None, false),
            None => (None, true), // Empty is ok
        };

        emit_request_metrics!(config);
        Ok(Self {
            pubkey,
            config,
            filters,
            valid_filters,
        })
    }

    fn get_limit(state: &State) -> &SemaphoreQueue {
        state.program_accounts_request_limit.as_ref()
    }

    fn get_type(&self) -> CacheableType<'_> {
        CacheableType::GetProgramAccounts(self)
    }

    fn has_active_subscription(&self, state: &State, _owner: Option<Pubkey>) -> SubscriptionActive {
        state.subscription_active(Subscription::Program(self.pubkey), self.commitment(), None)
    }

    fn is_cacheable(&self, state: &State) -> Result<(), UncacheableReason> {
        if self.config.encoding == Encoding::JsonParsed {
            Err(UncacheableReason::Encoding)
        } else if self.config.data_slice.is_some() {
            Err(UncacheableReason::DataSlice)
        } else if !self.valid_filters {
            Err(UncacheableReason::Filters)
        } else if !state.websocket_connected((self.pubkey, self.commitment())) {
            Err(UncacheableReason::Disconnected)
        } else {
            Ok(())
        }
    }

    fn put_into_cache(&self, state: &State, data: Self::ResponseData) -> bool {
        if !self.valid_filters {
            return false;
        }

        let commitment = self.commitment();
        let (slot, accounts) = data.into_slot_and_value();
        let mut account_key_refs = HashSet::with_capacity(accounts.len());
        for acc in accounts {
            let AccountAndPubkey { account, pubkey } = acc;
            let key_ref = state.insert(
                pubkey,
                AccountContext {
                    value: Some(account),
                    context: SolanaContext {
                        slot: slot.unwrap_or(0),
                    },
                },
                commitment,
            );
            account_key_refs.insert(Arc::clone(&key_ref));
        }

        let program_key = (self.pubkey, commitment);
        let should_unsubscribe = !state.program_accounts.has_active_entry(&program_key);

        let program_state =
            state
                .program_accounts
                .insert(program_key, account_key_refs, self.filters.clone());

        // if the cache insertion for the given program key
        // happened for the first time, then we have to unsubscribe from all accounts, which are
        // owned by given program, otherwise, we have already unsubscribed from them, on previous
        // insert calls
        if should_unsubscribe {
            for key in program_state.tracked_keys() {
                state.unsubscribe(**key, commitment);
            }
        }

        true
    }

    fn sub_descriptor(&self) -> SubDescriptor {
        SubDescriptor {
            kind: Subscription::Program(self.pubkey),
            commitment: self.commitment(),
            filters: self.filters.clone(),
        }
    }

    fn cache_hit_counter<'a>() -> &'a IntCounter {
        &metrics().program_accounts_cache_hits
    }
    fn cache_filled_counter<'a>() -> &'a IntCounter {
        &metrics().program_accounts_cache_filled
    }
}

impl fmt::Display for GetProgramAccounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: print filters as well
        write!(
            f,
            "getProgramAccounts {{ pubkey: {}, commitment: {:?} }}",
            self.pubkey,
            self.commitment()
        )
    }
}

enum UncacheableReason {
    Encoding,
    DataSlice,
    Filters,
    Disconnected,
}

impl UncacheableReason {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::DataSlice => "data_slice",
            Self::Filters => "bad_filters",
            Self::Disconnected => "websocket_disconnected",
        }
    }

    /// Returns true if the request can still be fetched from cache
    fn can_use_cache(&self) -> bool {
        match self {
            Self::Encoding | Self::DataSlice => true,
            Self::Filters | Self::Disconnected => false,
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
pub struct Request<'a, T>
where
    T: ?Sized,
{
    pub jsonrpc: &'a str,
    pub id: Id<'a>,
    pub method: &'a str,
    #[serde(borrow)]
    pub params: Option<&'a T>,
}

impl<'a, 'b> mlua::UserData for &'b Request<'a, RawValue> {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("jsonrpc", |_, this| Ok(this.jsonrpc));
        fields.add_field_method_get("method", |_, this| Ok(this.method));
        fields.add_field_method_get("params", |_, this| Ok(this.params.map(|v| v.get())));
    }
}

#[derive(Deserialize, Debug)]
pub struct Flatten<T> {
    #[serde(flatten)]
    pub inner: T,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Response<T> {
    Result(T),
    Error(RpcErrorOwned),
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
pub struct RpcErrorOwned {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
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

    fn internal(id: Option<Id<'a>>, msg: Cow<'a, str>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32603,
                message: msg,
                data: None,
            },
        }
    }

    fn waf_rejection(id: Option<Id<'a>>, msg: &'a str) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -33000,
                message: Cow::from(msg),
                data: None,
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
    #[error("waf rejection error")]
    WAFRejection(Option<Id<'a>>, String),
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
    #[error("internal error")]
    Internal(Option<Id<'a>>, Cow<'a, str>),
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
            Error::WAFRejection(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::waf_rejection(req_id.clone(), msg)),
            Error::InvalidParam {
                req_id,
                message,
                data,
            } => HttpResponse::Ok().content_type("application/json").json(
                &ErrorResponse::invalid_param(req_id.clone(), message.clone(), data.clone()),
            ),
            Error::Internal(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::internal(req_id.clone(), msg.clone())),
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
    #[serde(default = "Encoding::default")]
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
            message: "Invalid parameters: Expected from 1 to 2 parameters".into(),
            data: Some(format!("\"Got {}\"", params.len()).into()),
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

async fn account_response<'a, 'b>(
    req_id: Id<'a>,
    request_hash: u64,
    acc: (Option<&'b AccountInfo>, u64),
    app_state: &State,
    config: &AccountInfoConfig,
    pubkey: Pubkey,
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
            .append_header(("x-cache-status", "hit"))
            .append_header(("x-cache-type", "lru"))
            .content_type("application/json")
            .body(body));
    }

    let slot = acc.1;
    let ctx = SolanaContext { slot };
    let mut should_spawn_thread = false;
    let result = acc
            .0
            .as_ref()
            .map(|acc| {
                should_spawn_thread = acc.data.len() > 1000;
                Ok::<_, Base58Error>(
                    acc.encode(config.encoding, config.data_slice, true, pubkey)?
                        .with_context(&ctx),
                )
            })
            .transpose()
            .map_err(|_| Error::InvalidRequest(Some(req_id.clone()),
                    Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding.")))?
            .unwrap_or_else(|| EncodedAccountContext::empty(&ctx));

    let permit = app_state.serialize_threads_limit.try_acquire();
    let result = if should_spawn_thread && permit.is_ok() {
        let acc: EncodedAccountContextOwned = result.into();
        let res = web::block(move || serde_json::value::to_raw_value(&acc))
            .await
            .expect("couldn't block on account serialization");
        drop(permit);
        res?
    } else {
        drop(permit);
        serde_json::value::to_raw_value(&result)?
    };
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
        .append_header(("x-cache-status", "hit"))
        .append_header(("x-cache-type", "data"))
        .content_type("application/json")
        .body(body))
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

#[derive(Debug, Deserialize)]
#[serde(from = "SmallVec<[Filter; 3]>")]
enum MaybeFilters {
    Valid(Filters),
    Invalid(SmallVec<[Filter; 3]>),
}

impl From<SmallVec<[Filter; 3]>> for MaybeFilters {
    fn from(value: SmallVec<[Filter; 3]>) -> MaybeFilters {
        Filters::new_normalized(value.clone()).map_or(Self::Invalid(value), Self::Valid)
    }
}

#[derive(Deserialize, Debug)]
struct ProgramAccountsConfig {
    #[serde(default = "Encoding::default")]
    encoding: Encoding,
    #[serde(flatten)]
    commitment: Option<CommitmentConfig>,
    #[serde(rename = "dataSlice")]
    data_slice: Option<Slice>,
    filters: Option<MaybeFilters>,
    #[serde(rename = "withContext")]
    with_context: Option<bool>,
}

impl Default for ProgramAccountsConfig {
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

async fn program_accounts_response<'a>(
    req_id: Id<'a>,
    accounts: &HashSet<Arc<Pubkey>>,
    config: &'_ ProgramAccountsConfig,
    filters: Option<&'a Filters>,
    app_state: &State,
    with_context: bool,
) -> Result<HttpResponse, ProgramAccountsResponseError> {
    let commitment = config.commitment.unwrap_or_default().commitment;

    let options = EncodeOptions {
        encoding: config.encoding,
        slice: config.data_slice,
        enforce_base58_limit: !app_state.config.load().ignore_base58_limit,
        with_context,
    };
    let accounts = accounts.iter().map(|key| **key).collect();
    let accounts_db = app_state.accounts.clone();
    let filters = filters.cloned();
    let permit = app_state.serialize_threads_limit.acquire().await;
    let value = web::block(move || {
        serialize_program_accounts(accounts, accounts_db, commitment, filters, options)
    })
    .await
    .expect("couldn't block on program account serialization")
    .map(|res| {
        drop(permit);
        res
    })?;

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
        .append_header(("x-cache-status", "hit"))
        .append_header(("x-cache-type", "data"))
        .content_type("application/json")
        .body(body))
}

struct EncodeOptions {
    encoding: Encoding,
    slice: Option<Slice>,
    enforce_base58_limit: bool,
    with_context: bool,
}

fn serialize_program_accounts(
    accounts: HashSet<Pubkey>,
    accounts_db: AccountsDb,
    commitment: Commitment,
    filters: Option<Filters>,
    options: EncodeOptions,
) -> Result<Box<RawValue>, ProgramAccountsResponseError> {
    let mut slot = 0;
    let mut encoded_accounts = Vec::new();
    struct Encode<'a, K> {
        inner: Ref<'a, K, AccountState>,
        encoding: Encoding,
        slice: Option<Slice>,
        commitment: Commitment,
        enforce_base58_limit: bool,
        pubkey: Pubkey,
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
                    .encode(
                        self.encoding,
                        self.slice,
                        self.enforce_base58_limit,
                        self.pubkey,
                    )
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
    let check_base58 = options.enforce_base58_limit && options.encoding.is_base58();

    for key in accounts {
        if let Some(data) = accounts_db.get(&key) {
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
            if check_base58 && account_len > 128 {
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
                    encoding: options.encoding,
                    slice: options.slice,
                    commitment,
                    enforce_base58_limit: options.enforce_base58_limit,
                    pubkey: key,
                },
                pubkey: key,
            });
        } else {
            warn!(key = %key, "data for key not found");
            return Err(ProgramAccountsResponseError::Inconsistency);
        }
    }

    let value = match options.with_context {
        true => MaybeContext::With {
            context: SolanaContext { slot },
            value: encoded_accounts,
        },
        false => MaybeContext::Without(encoded_accounts),
    };

    serde_json::value::to_raw_value(&value).map_err(ProgramAccountsResponseError::Serialize)
}

fn base58_error(id: Id<'_>) -> Error<'_> {
    Error::InvalidRequest(
        Some(id),
        Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding."),
    )
}

enum OneOrMany<'a> {
    One(Request<'a, RawValue>),
    Many(Vec<Request<'a, RawValue>>),
}

impl<'a> OneOrMany<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &Request<'a, RawValue>> {
        use either::Either;
        match self {
            OneOrMany::One(req) => Either::Left(std::iter::once(req)),
            OneOrMany::Many(reqs) => Either::Right(reqs.iter()),
        }
    }
}

impl<'de> Deserialize<'de> for OneOrMany<'de> {
    fn deserialize<D>(deserializer: D) -> Result<OneOrMany<'de>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OneOrMany<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("[] or {}")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let des = serde::de::value::SeqAccessDeserializer::new(seq);
                Ok(OneOrMany::Many(serde::Deserialize::deserialize(des)?))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let des = serde::de::value::MapAccessDeserializer::new(map);
                Ok(OneOrMany::One(serde::Deserialize::deserialize(des)?))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

async fn check_config_change(state: &web::Data<State>) {
    use futures_util::FutureExt;
    // apply new config (if any)
    {
        let mut rx = state.config_watch.borrow_mut();
        if rx.changed().now_or_never().is_some() {
            apply_config(state, rx.borrow().clone()).await;
        }
    }
    // apply new lua rules (if any)
    {
        let mut rx = state.waf_watch.borrow_mut();
        if rx.changed().now_or_never().is_some() {
            if let Some(ref waf) = state.waf {
                if let Err(err) = waf.reload() {
                    warn!(error = %err, "coudn't read waf rules from file");
                    return;
                }
                info!("Updated waf rules");
            }
        }
    }
}

pub async fn rpc_handler(
    body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    let req: OneOrMany<'_> = match serde_json::from_slice(&body) {
        Ok(val) => val,
        Err(_) => return Ok(Error::InvalidRequest(None, Some("Invalid request")).error_response()),
    };

    check_config_change(&app_state).await;

    // run WAF checks on all subquiries in request
    if let Some(waf) = &app_state.waf {
        let lua = &waf.lua;
        for r in req.iter() {
            let res = lua.scope(|scope| {
                lua.globals()
                    .set("request", scope.create_nonstatic_userdata(r)?)?;
                lua.load("require 'waf'.request(request)")
                    .eval::<(bool, String)>()
            });

            let (ok, err) = match res {
                Ok(tuple) => tuple,
                Err(e) => {
                    tracing::error!(%e, "Error occured during WAF rules evaluation");
                    return Ok(Error::Internal(
                        Some(r.id.clone()),
                        Cow::from("WAF internal error"),
                    )
                    .error_response());
                }
            };
            if !ok {
                info!(%err, "Request was rejected due to WAF rule violation");
                metrics().waf_rejections.inc();
                return Ok(Error::WAFRejection(Some(r.id.clone()), err).error_response());
            }
        }
    }

    let mut id = Id::Null;

    // if request contains only one query, try to serve it from cache
    if let OneOrMany::One(req) = req {
        id = req.id.clone();

        if req.jsonrpc != "2.0" {
            return Ok(Error::InvalidRequest(Some(id), None).error_response());
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

        let arc_state = app_state.clone().into_inner();
        match req.method {
            "getAccountInfo" => {
                return observe!(req.method, arc_state.process_request::<GetAccountInfo>(req));
            }
            "getProgramAccounts" => {
                return observe!(
                    req.method,
                    arc_state.process_request::<GetProgramAccounts>(req)
                );
            }
            method => {
                metrics().request_types(method).inc();
            }
        }
    } else {
        metrics().batch_requests.inc();
    }

    let client = app_state.client.clone();
    let url = app_state.rpc_url.clone();
    let error = Error::Timeout(id).error_response();

    let stream = stream_generator::generate_stream(move |mut stream| async move {
        let mut backoff = backoff_settings(30);
        let total = metrics().passthrough_total_time.start_timer();
        loop {
            let request_time = metrics().passthrough_request_time.start_timer();
            let resp = client
                .post(&url)
                .content_type("application/json")
                .send_body(body.clone())
                .await
                .map_err(|err| {
                    error!(error = %err, "error while streaming response");
                    metrics().streaming_errors.inc();
                    err
                });
            metrics()
                .backend_requests_count
                .with_label_values(&["passthrough"])
                .inc(); // count request attempts, even failed ones

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
                        Some(duration) => {
                            metrics().request_retries.inc();
                            tokio::time::sleep(duration).await;
                        }
                        None => {
                            let mut error_stream = error.into_body();
                            use actix_web::body::MessageBody;
                            warn!("request error: {:?}", err);
                            while let Some(chunk) = futures_util::future::poll_fn(|cx| {
                                std::pin::Pin::new(&mut error_stream).poll_next(cx)
                            })
                            .await
                            {
                                stream
                                    .send(chunk.map_err(|_| PayloadError::Incomplete(None))) // should never error
                                    .await;
                            }
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

fn backoff_settings(max: u64) -> backoff::ExponentialBackoff {
    backoff::ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(5),
        max_elapsed_time: Some(Duration::from_secs(max)),
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

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub struct RequestLimits {
    pub account_info: usize,
    pub program_accounts: usize,
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub struct RequestQueueSize {
    pub account_info: usize,
    pub program_accounts: usize,
}

// default values effectively disable any restrictions
impl Default for RequestQueueSize {
    fn default() -> Self {
        Self {
            account_info: crate::DEFAULT_GAI_QUEUE_SIZE,
            program_accounts: crate::DEFAULT_GPA_QUEUE_SIZE,
        }
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub request_limits: RequestLimits,
    #[serde(default)]
    pub request_queue_size: RequestQueueSize,
    #[serde(default)]
    pub ignore_base58_limit: bool,
}

pub async fn apply_config(app_state: &web::Data<State>, new_config: Config) {
    let current_config = app_state.config.load();

    if **current_config == new_config {
        return;
    }

    let current_limits = current_config.request_limits;
    let current_queue_size = current_config.request_queue_size;

    let new_limits = new_config.request_limits;
    let new_queue_size = new_config.request_queue_size;

    app_state.config.store(Arc::new(new_config.clone()));

    app_state
        .account_info_request_limit
        .apply_limit(current_limits.account_info, new_limits.account_info)
        .await;

    app_state
        .program_accounts_request_limit
        .apply_limit(current_limits.program_accounts, new_limits.program_accounts)
        .await;

    app_state
        .account_info_request_limit
        .apply_queue_size(current_queue_size.account_info, new_queue_size.account_info)
        .await;

    app_state
        .program_accounts_request_limit
        .apply_queue_size(
            current_queue_size.program_accounts,
            new_queue_size.program_accounts,
        )
        .await;

    let available_accounts = &app_state.account_info_request_limit.available_permits();
    let available_programs = &app_state.program_accounts_request_limit.available_permits();

    info!(
        old_config = ?current_config,
        new_config = ?new_config,
        %available_accounts,
        %available_programs,
        "new configuration applied"
    );
}

pub async fn metrics_handler(
    _body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    use prometheus::{Encoder, TextEncoder};

    let current_limits = app_state.config.load().request_limits;
    metrics()
        .max_permits
        .with_label_values(&["getAccountInfo"])
        .set(current_limits.account_info as i64);
    metrics()
        .max_permits
        .with_label_values(&["getProgramAccounts"])
        .set(current_limits.program_accounts as i64);

    metrics().app_version.set(0);
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let families = prometheus::gather();
    let _ = encoder.encode(&families, &mut buffer);
    Ok(HttpResponse::Ok().content_type("text/plain").body(buffer))
}

#[cfg(feature = "jsonparsed")]
pub fn get_mint_decimals(mint: &SolanaPubkey) -> Result<u8, &'static str> {
    use solana_account_decoder::parse_token::spl_token_v2_0_native_mint;

    if mint == &spl_token_v2_0_native_mint() {
        Ok(spl_token_v2_0::native_mint::DECIMALS)
    } else {
        Err("Invalid param: mint is not native")
    }
}
