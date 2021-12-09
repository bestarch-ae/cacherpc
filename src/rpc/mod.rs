#![allow(explicit_outlives_requirements)]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::filter::Filters;
use crate::metrics::rpc_metrics as metrics;
use crate::pubsub::Subscription;
use crate::types::{AccountContext, Commitment, Pubkey};

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
struct Slice {
    offset: usize,
    length: usize,
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

pub(super) trait HasOwner {
    fn owner(&self) -> Option<Pubkey> {
        None
    }
}

impl HasOwner for AccountContext {
    fn owner(&self) -> Option<Pubkey> {
        self.value.as_ref().map(|value| value.owner)
    }
}

fn backoff_settings(max: u64) -> backoff::ExponentialBackoff {
    backoff::ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(5),
        max_elapsed_time: Some(Duration::from_secs(max)),
        ..Default::default()
    }
}

fn hash<T: std::hash::Hash>(params: T) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    params.hash(&mut hasher);
    hasher.finish()
}

mod cacheable;
pub mod config;
pub mod handler;
pub mod monitor;
mod request;
mod response;
pub mod state;
pub use config::Config;
pub use handler::{metrics_handler, rpc_handler};
