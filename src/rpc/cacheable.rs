use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use prometheus::IntCounter;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use crate::metrics::rpc_metrics as metrics;
use crate::pubsub::subscription::{Subscription, SubscriptionActive};
use crate::rpc::request::{parse_params, MaybeFilters, ProgramAccountsConfig};
use crate::types::SolanaContext;
use crate::types::{AccountContext, Commitment, Encoding, Pubkey, SemaphoreQueue};

use super::request::{AccountAndPubkey, MaybeContext};
use super::response::ProgramAccountsResponseError;
use super::{
    request::{GetAccountInfo, GetProgramAccounts, Id, Request},
    response::{account_response, program_accounts_response, CachedResponse, Error},
    state::State,
    HasOwner, SubDescriptor,
};

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

pub(super) trait Cacheable: Sized + 'static {
    const REQUEST_TYPE: &'static str;
    type ResponseData: DeserializeOwned + HasOwner;

    fn parse<'a>(request: &Request<'a, RawValue>) -> Result<Self, Error<'a>>;
    fn get_limit(state: &State) -> &SemaphoreQueue;
    fn get_timeout(state: &State) -> Duration;
    fn get_backoff(state: &State) -> u64;

    fn is_cacheable(&self, state: &State) -> Result<(), UncacheableReason>;
    // method to check whether cached entry has corresponding websocket subscription
    fn has_active_subscription(&self, state: &State, owner: Option<Pubkey>) -> SubscriptionActive;

    fn get_from_cache<'a>(
        &mut self, // gPA may modify internal state of the request object
        id: &Id<'a>,
        state: Arc<State>, // gPA may spawn a future to fetch extra accounts using State
    ) -> Option<Result<CachedResponse, Error<'a>>>;

    fn put_into_cache(
        &self,
        state: &State,
        data: Self::ResponseData,
        overwrite: bool, // indicates whether the cache entry should be overwritten if it already exists
    ) -> bool;

    fn sub_descriptor(&self) -> SubDescriptor;

    fn handle_parse_error(&self, err: Error<'_>) {
        tracing::error!(error = %err, "failed to parse response");
    }

    // Metrics
    fn cache_hit_counter<'a>() -> &'a IntCounter;
    fn cache_filled_counter<'a>() -> &'a IntCounter;
}

pub(super) enum UncacheableReason {
    Encoding,
    DataSlice,
    Filters,
    Disconnected,
}

impl UncacheableReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::DataSlice => "data_slice",
            Self::Filters => "bad_filters",
            Self::Disconnected => "websocket_disconnected",
        }
    }

    /// Returns true if the request can still be fetched from cache
    pub fn can_use_cache(&self) -> bool {
        match self {
            Self::Encoding | Self::DataSlice => true,
            Self::Filters | Self::Disconnected => false,
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

    fn get_timeout(state: &State) -> Duration {
        Duration::from_secs(state.config.load().timeouts.account_info_request)
    }

    fn get_backoff(state: &State) -> u64 {
        state.config.load().timeouts.account_info_backoff
    }

    fn get_limit(state: &State) -> &SemaphoreQueue {
        state.account_info_request_limit.as_ref()
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

    fn get_from_cache<'a>(
        &mut self,
        id: &Id<'a>,
        state: Arc<State>,
    ) -> Option<Result<CachedResponse, Error<'a>>> {
        let mut slot_update = None;
        let result = state.accounts.get(&self.pubkey).and_then(|data| {
            let mut account = data.value().get(self.commitment());
            let owner = account.and_then(|(info, _)| info).map(|info| info.owner);

            account = match account {
                Some((Some(info), slot)) if slot == 0 => state
                    .program_accounts
                    .get_slot(&(info.owner, self.commitment()))
                    .map(|slot| {
                        if slot > 0 {
                            slot_update = Some(slot);
                        }
                        (Some(info), slot)
                    }),

                acc => acc,
            };

            match account.filter(|(_, slot)| *slot != 0) {
                Some(data) => {
                    let resp = account_response(
                        id.clone(),
                        self.config_hash,
                        data,
                        &state,
                        &self.config,
                        self.pubkey,
                    );
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
        });
        // we have to update accounts map outside of closure, because
        // its underlying Dashmap will deadlock otherwise
        if let Some(slot) = slot_update {
            state
                .accounts
                .update_account_slot(&self.pubkey, self.commitment(), slot);
        }
        result
    }

    fn put_into_cache(&self, state: &State, data: Self::ResponseData, _: bool) -> bool {
        state.insert(self.pubkey, data, self.commitment(), true);
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

    fn get_timeout(state: &State) -> Duration {
        Duration::from_secs(state.config.load().timeouts.program_accounts_request)
    }

    fn get_backoff(state: &State) -> u64 {
        state.config.load().timeouts.program_accounts_backoff
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

    fn get_from_cache<'a>(
        &mut self,
        id: &Id<'a>,
        state: Arc<State>,
    ) -> Option<Result<CachedResponse, Error<'a>>> {
        let program_state = state
            .program_accounts
            .get_state((self.pubkey, self.commitment()))?;

        let slot = program_state.value().slot;
        let context = self.config.with_context.unwrap_or_default().then(|| slot);

        // do not serve data from cache if the cached data doesn't have slot info
        if context.is_some() && slot == 0 {
            return None;
        }
        let filters = self.filters.as_ref();
        let config = &self.config;
        let wide_filters = state
            .fetch_wide_filters
            .load(std::sync::atomic::Ordering::Relaxed);
        match program_state.value().get_account_keys(&self.filters) {
            Ok(cached) => {
                let res = program_accounts_response(
                    id.clone(),
                    cached.accounts,
                    config,
                    filters,
                    &state,
                    context,
                );
                match res {
                    Ok(res) => {
                        // if found data is not for the given filter, but rather
                        // for superset of it, then we should update request's
                        // filters, so that proper filter will be reset for pubsub
                        if cached.should_overwrite {
                            self.filters = cached.filter_overwrite;
                        }
                        Some(Ok(CachedResponse {
                            owner: None,
                            response: res,
                        }))
                    }
                    Err(ProgramAccountsResponseError::Base58) => {
                        Some(Err(base58_error(id.clone())))
                    }
                    Err(_) => None,
                }
            }
            Err(Some(filters)) if wide_filters => {
                // superset filter was found, fetch accounts for it,
                // so that future cache misses will be prevented
                drop(program_state);
                let mut cloned_gpa = self.clone();
                cloned_gpa.filters = Some(filters);
                // fetch account asynchronously, so that we don't block client request
                actix::spawn(state.fetch_program_accounts(cloned_gpa));
                None
            }
            _ => None,
        }
    }

    fn put_into_cache(&self, state: &State, data: Self::ResponseData, overwrite: bool) -> bool {
        if !self.valid_filters {
            return false;
        }

        let commitment = self.commitment();
        let (slot, accounts) = data.into_slot_and_value();
        let slot = slot.unwrap_or(0);
        let mut account_key_refs = HashSet::with_capacity(accounts.len());
        for acc in accounts {
            let AccountAndPubkey { account, pubkey } = acc;
            let key_ref = state.insert(
                pubkey,
                AccountContext {
                    value: Some(account),
                    context: SolanaContext { slot },
                },
                commitment,
                overwrite,
            );
            account_key_refs.insert(Arc::clone(&key_ref));
        }

        let program_key = (self.pubkey, commitment);
        let should_unsubscribe = !state.program_accounts.has_active_entry(&program_key);

        let program_state = state.program_accounts.insert(
            program_key,
            account_key_refs,
            self.filters.clone(),
            slot,
        );

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

fn base58_error(id: Id<'_>) -> Error<'_> {
    Error::InvalidRequest(
        Some(id),
        Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding."),
    )
}
