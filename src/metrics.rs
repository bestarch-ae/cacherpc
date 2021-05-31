use once_cell::sync::Lazy;
use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    HistogramVec, IntCounter, IntCounterVec, IntGauge,
};

pub struct PubSubMetrics {
    pub subscriptions_active: IntGauge,
    pub websocket_connected: IntGauge,
    pub notifications_received: IntCounterVec,
    pub commands: IntCounterVec,
}

pub fn pubsub_metrics() -> &'static PubSubMetrics {
    static METRICS: Lazy<PubSubMetrics> = Lazy::new(|| PubSubMetrics {
        subscriptions_active: register_int_gauge!(
            "subscriptions_active",
            "number of active subcriptions"
        )
        .unwrap(),
        websocket_connected: register_int_gauge!(
            "websocket_connected",
            "websocket connection status"
        )
        .unwrap(),
        notifications_received: register_int_counter_vec!(
            "notifications_received",
            "number of notifications received",
            &["type"]
        )
        .unwrap(),
        commands: register_int_counter_vec!("commands", "number of commands received", &["type"])
            .unwrap(),
    });
    &METRICS
}

pub struct RpcMetrics {
    pub request_types: IntCounterVec,
    pub request_encodings: IntCounterVec,
    pub account_cache_hits: IntCounter,
    pub account_cache_filled: IntCounter,
    pub program_accounts_cache_hits: IntCounter,
    pub program_accounts_cache_filled: IntCounter,
    pub response_uncacheable: IntCounter,
    pub backend_response_time: HistogramVec,
    pub backend_errors: IntCounterVec,
    pub handler_time: HistogramVec,
    pub response_size_bytes: HistogramVec,
    pub lru_cache_hits: IntCounter,
}

pub fn rpc_metrics() -> &'static RpcMetrics {
    static METRICS: Lazy<RpcMetrics> = Lazy::new(|| RpcMetrics {
        request_types: register_int_counter_vec!(
            "request_types",
            "Request counts by type",
            &["type"]
        )
        .unwrap(),
        request_encodings: register_int_counter_vec!(
            "request_encodings",
            "Request encoding counts by type",
            &["type", "encoding"]
        )
        .unwrap(),
        account_cache_hits: register_int_counter!("account_cache_hits", "Accounts cache hit")
            .unwrap(),
        lru_cache_hits: register_int_counter!("lru_cache_hits", "LRU cache hit").unwrap(),
        account_cache_filled: register_int_counter!(
            "account_cache_filled",
            "Accounts cache filled while waiting for response"
        )
        .unwrap(),
        program_accounts_cache_hits: register_int_counter!(
            "program_accounts_cache_hits",
            "Program accounts cache hit"
        )
        .unwrap(),
        program_accounts_cache_filled: register_int_counter!(
            "program_accounts_cache_filled",
            "Program accounts cache filled while waiting for response"
        )
        .unwrap(),
        response_uncacheable: register_int_counter!(
            "response_uncacheable",
            "Could not cache response"
        )
        .unwrap(),
        backend_response_time: register_histogram_vec!(
            "backend_response_time",
            "Backend response time by type",
            &["type"]
        )
        .unwrap(),
        backend_errors: register_int_counter_vec!(
            "backend_errors",
            "Error responses by request type",
            &["type"]
        )
        .unwrap(),
        handler_time: register_histogram_vec!(
            "handler_time",
            "Handler processing time by type",
            &["type"]
        )
        .unwrap(),
        response_size_bytes: register_histogram_vec!(
            "response_size_bytes",
            "Response size by type",
            &["type"],
            vec![
                0.0, 1024.0, 4096.0, 16384.0, 65536.0, 524288.0, 1048576.0, 4194304.0, 10485760.0,
                20971520.0
            ]
        )
        .unwrap(),
    });

    &METRICS
}
