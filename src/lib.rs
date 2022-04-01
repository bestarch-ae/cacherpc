pub mod cli;
pub mod control;
pub mod filter;
pub mod metrics;
pub mod pubsub;
pub mod rpc;
pub mod types;

const DEFAULT_GAI_QUEUE_SIZE: usize = 2 << 19;
const DEFAULT_GPA_QUEUE_SIZE: usize = 2 << 18;
const DEFAULT_GAI_TIMEOUT: u64 = 30;
const DEFAULT_GPA_TIMEOUT: u64 = 60;
const DEFAULT_GAI_BACKOFF: u64 = 30;
const DEFAULT_GPA_BACKOFF: u64 = 60;
const PASSTHROUGH_BACKOFF: u64 = 30;

pub const CACHER_LOG_LEVEL: &str = "CACHER_LOG_LEVEL";
