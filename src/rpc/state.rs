use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use actix_web::HttpResponse;
use anyhow::Context;
use arc_swap::ArcSwap;
use awc::Client;
use backoff::backoff::Backoff;
use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use lru::LruCache;
use mlua::{Lua, LuaOptions, StdLib};
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, warn};

use crate::metrics::rpc_metrics as metrics;
use crate::pubsub::{PubSubManager, Subscription, SubscriptionActive};
use crate::rpc::request::Flatten;
use crate::types::{
    AccountContext, AccountsDb, BytesChain, Commitment, ProgramAccountsDb, Pubkey, SemaphoreQueue,
};

use super::cacheable::Cacheable;
use super::request::Request;
use super::response::{Error, Response};
use super::{backoff_settings, Config, HasOwner, LruEntry, SubDescriptor};

type CacheResult<'a> = Result<HttpResponse, Error<'a>>;

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
    pub identity: Option<String>,
}

pub struct Waf {
    pub(super) lua: Lua,
    pub(super) path: PathBuf,
}

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

    pub(super) fn reload(&self) -> Result<(), anyhow::Error> {
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

    pub fn insert(
        &self,
        key: Pubkey,
        data: AccountContext,
        commitment: Commitment,
        overwrite: bool,
    ) -> Arc<Pubkey> {
        self.accounts.insert(key, data, commitment, overwrite)
    }

    pub fn websocket_connected(&self, key: (Pubkey, Commitment)) -> bool {
        self.pubsub.websocket_connected(key)
    }

    // owner is the subscription for program, if given account belongs to one
    pub fn subscription_active(
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
    pub fn unsubscribe(&self, key: Pubkey, commitment: Commitment) {
        self.pubsub.unsubscribe(key, commitment);
    }

    // clippy incorrectly assumes that lifetimes can be elided
    #[allow(clippy::needless_lifetimes)]
    async fn request<'a, T>(
        &self,
        req: &Request<'a, T>,
        limit: &SemaphoreQueue,
        timeout: Duration,
        backoff: u64, // total number of seconds for retries
    ) -> Result<impl Stream<Item = Result<Bytes, awc::error::PayloadError>>, Error<'a>>
    where
        T: Serialize + Debug + ?Sized,
    {
        let client = &self.client;
        let mut backoff = backoff_settings(backoff);
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
                .timeout(timeout)
                .append_header(("X-Cache-Request-Method", req.method))
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

    pub(super) async fn process_request<T: Cacheable + fmt::Display>(
        self: Arc<Self>,
        raw_request: Request<'_, RawValue>,
    ) -> CacheResult<'_> {
        let request = T::parse(&raw_request)?;
        let (is_cacheable, can_use_cache) = match request
            .is_cacheable(&self)
            .map(|_| request.get_from_cache(&raw_request.id, &self))
        {
            Ok(Some(data)) => {
                let owner = data.owner();
                self.reset(request.sub_descriptor(), owner);
                if request.has_active_subscription(&self, owner).await {
                    T::cache_hit_counter().inc();
                    return data.map(|data| data.response);
                } else {
                    (true, false)
                }
            }
            Ok(None) => (true, true),
            Err(reason) => {
                let data = reason
                    .can_use_cache()
                    .then(|| request.get_from_cache(&raw_request.id, &self))
                    .flatten();

                if let Some(data) = data {
                    T::cache_hit_counter().inc();
                    self.reset(request.sub_descriptor(), data.owner());
                    return data.map(|data| data.response);
                }

                metrics()
                    .response_uncacheable
                    .with_label_values(&[T::REQUEST_TYPE, reason.as_str()])
                    .inc();
                (false, reason.can_use_cache())
            }
        };

        let wait_for_response = self.request(
            &raw_request,
            T::get_limit(&self),
            T::get_timeout(&self),
            T::get_backoff(&self),
        );
        tokio::pin!(wait_for_response);

        let resp = loop {
            let notified = self.map_updated.notified();
            tokio::select! {
                body = &mut wait_for_response => {
                    break body?;
                }
                _ = notified, if can_use_cache => {
                    if let Some(data) = request.get_from_cache(&raw_request.id, &self) {
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
            // If gPA already has active subscription, then we shouldn't overwrite existing account
            // entries. For gAI, if we had active subscription, then we wouldn't even make it here
            let overwrite = !request.has_active_subscription(&self, None).await;
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
                        if this.is_caching_allowed()
                            && request.put_into_cache(&this, data, overwrite)
                        {
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
