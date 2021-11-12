use once_cell::sync::Lazy;
use prometheus::core::Opts;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec,
};

pub fn version() -> &'static str {
    static VERSION: Lazy<String> = Lazy::new(|| {
        let pkg_version = std::env!("CARGO_PKG_VERSION");
        option_env!("CI_COMMIT_TAG")
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                format!(
                    "{}{}",
                    pkg_version,
                    option_env!("CI_COMMIT_SHA")
                        .map(|sha| format!("-{}", sha))
                        .unwrap_or_else(String::new)
                )
            })
    });
    &VERSION
}

pub struct DbMetrics {
    pub account_entries: IntGauge,
    pub program_account_entries: IntGauge,
    pub account_bytes: IntGauge,
}

pub fn db_metrics() -> &'static DbMetrics {
    static METRICS: Lazy<DbMetrics> = Lazy::new(|| DbMetrics {
        account_entries: register_int_gauge!(
            "account_entries",
            "number of entries in accounts cache"
        )
        .unwrap(),
        program_account_entries: register_int_gauge!(
            "program_account_entries",
            "number of entries in program accounts cache"
        )
        .unwrap(),
        account_bytes: register_int_gauge!("account_bytes", "number of entries in accounts cache")
            .unwrap(),
    });
    &METRICS
}

pub struct PubSubMetrics {
    pub subscriptions_active: IntGaugeVec,
    pub subscribe_requests: IntCounterVec,
    pub subscribe_errors: IntCounterVec,
    pub unsubscribe_errors: IntCounterVec,
    pub websocket_connected: IntGaugeVec,
    pub websocket_active: IntGaugeVec,
    pub websocket_errors: IntCounterVec,
    pub websocket_disconnects: IntCounterVec,
    pub notifications_received: IntCounterVec,
    pub commands: IntCounterVec,
    pub bytes_received: IntCounterVec,
    pub purge_queue_length: IntGaugeVec,
    pub purge_queue_entries: IntGaugeVec,
    pub additional_keys_entries: IntGaugeVec,
    pub filters: IntGaugeVec,
    pub sub_id_entries: IntGaugeVec,
    pub id_sub_entries: IntGaugeVec,
    pub inflight_entries: IntGaugeVec,
    pub subs_entries: IntGaugeVec,
    pub subscription_lifetime: Histogram,
    pub time_until_reset: Histogram,
    pub time_to_subscribe: HistogramVec,
    pub accounts_filtered_out: IntCounterVec,
    pub filtration_time: HistogramVec,
    pub pubsub_slot: IntGaugeVec,
    pub pubsub_program_slot: IntGaugeVec,
    pub pubsub_account_slot: IntGaugeVec,
    pub websocket_reconnects: IntCounterVec,
}

pub fn pubsub_metrics() -> &'static PubSubMetrics {
    static METRICS: Lazy<PubSubMetrics> = Lazy::new(|| PubSubMetrics {
        subscriptions_active: register_int_gauge_vec!(
            "subscriptions_active",
            "number of active subcriptions",
            &["connection_id"]
        )
        .unwrap(),
        subscribe_requests: register_int_counter_vec!(
            "subscribe_requests",
            "number of subcribe requests sent",
            &["connection_id"]
        )
        .unwrap(),
        bytes_received: register_int_counter_vec!(
            "bytes_received",
            "number of bytes received in websocket frames",
            &["connection_id"]
        )
        .unwrap(),
        purge_queue_length: register_int_gauge_vec!(
            "purge_queue_length",
            "queue len",
            &["connection_id"]
        )
        .unwrap(),
        purge_queue_entries: register_int_gauge_vec!(
            "purge_queue_entries",
            "queue entries",
            &["connection_id"]
        )
        .unwrap(),
        additional_keys_entries: register_int_gauge_vec!(
            "additiona_keys_entries",
            "filter keys",
            &["connection_id"]
        )
        .unwrap(),
        sub_id_entries: register_int_gauge_vec!("sub_id_entries", "sub to id", &["connection_id"])
            .unwrap(),
        id_sub_entries: register_int_gauge_vec!("id_sub_entries", "id to sub", &["connection_id"])
            .unwrap(),
        inflight_entries: register_int_gauge_vec!(
            "inflight_entries",
            "inflight",
            &["connection_id"]
        )
        .unwrap(),
        subs_entries: register_int_gauge_vec!("subs_entries", "subs", &["connection_id"]).unwrap(),
        subscribe_errors: register_int_counter_vec!(
            "subscribe_errors",
            "number of subscribe errors",
            &["connection_id"]
        )
        .unwrap(),
        unsubscribe_errors: register_int_counter_vec!(
            "unsubscribe_errors",
            "number of unsubscribe errors",
            &["connection_id"]
        )
        .unwrap(),
        websocket_connected: register_int_gauge_vec!(
            "websocket_connected",
            "websocket connection status",
            &["connection_id"]
        )
        .unwrap(),
        websocket_active: register_int_gauge_vec!(
            "websocket_active",
            "websocket active status",
            &["connection_id"]
        )
        .unwrap(),
        websocket_errors: register_int_counter_vec!(
            "websocket_errors",
            "number of websocket errors",
            &["connection_id", "type"]
        )
        .unwrap(),
        notifications_received: register_int_counter_vec!(
            "notifications_received",
            "number of notifications received",
            &["connection_id", "type"]
        )
        .unwrap(),
        commands: register_int_counter_vec!(
            "commands",
            "number of commands received",
            &["connection_id", "type"]
        )
        .unwrap(),
        subscription_lifetime: register_histogram!(
            "subscription_lifetime",
            "time before subscription expires",
            vec![30.0, 120.0, 300.0, 600.0, 1200.0, 3600.0, 21600.0]
        )
        .unwrap(),
        time_until_reset: register_histogram!(
            "time_until_reset",
            "time before subscription was extended",
            vec![30.0, 120.0, 300.0, 600.0, 1200.0, 3600.0, 21600.0]
        )
        .unwrap(),
        time_to_subscribe: register_histogram_vec!(
            "time_to_subscribe",
            "time before subscription was extended",
            &["connection_id"],
            vec![0.0, 0.1, 0.5, 1.0, 5.0, 10.0, 15.0, 30.0, 120.0]
        )
        .unwrap(),
        accounts_filtered_out: register_int_counter_vec!(
            "accounts_filtered_out",
            "accounts received by ws not matched by filters",
            &["connection_id"]
        )
        .unwrap(),
        filters: register_int_gauge_vec!("filters", "number of filters", &["connection_id"])
            .unwrap(),
        websocket_disconnects: register_int_counter_vec!(
            "websocket_disconnects",
            "number of websocket disconnects",
            &["connection_id"]
        )
        .unwrap(),
        filtration_time: register_histogram_vec!(
            "filtration_time",
            "time to process filters for one update (micros)",
            &["connection_id"],
            vec![
                0.0,
                10.0,
                50.0,
                100.0,
                500.0,
                1000.0,
                10_000.0,
                100_000.0,
                1_000_000.0,
                10_000_000.0,
                100_000_000.0
            ]
        )
        .unwrap(),
        pubsub_slot: register_int_gauge_vec!(
            "pubsub_subscription_slot",
            "latest slot reported by slotSubscribe",
            &["connection_id"]
        )
        .unwrap(),
        pubsub_program_slot: register_int_gauge_vec!(
            "pubsub_program_slot",
            "latest slot reported by programNotification",
            &["connection_id"]
        )
        .unwrap(),
        pubsub_account_slot: register_int_gauge_vec!(
            "pubsub_account_slot",
            "latest slot reported by accountNotification",
            &["connection_id"]
        )
        .unwrap(),
        websocket_reconnects: register_int_counter_vec!(
            "websocket_reconnects",
            "attempts to reconnect to websocket",
            &["connection_id"]
        )
        .unwrap(),
    });
    &METRICS
}

pub struct RpcMetrics {
    pub app_version: IntGauge,
    request_types: IntCounterVec,
    pub request_encodings: IntCounterVec,
    pub request_commitments: IntCounterVec,
    pub account_cache_hits: IntCounter,
    pub account_cache_filled: IntCounter,
    pub program_accounts_cache_hits: IntCounter,
    pub program_accounts_cache_filled: IntCounter,
    pub response_uncacheable: IntCounterVec,
    pub backend_response_time: HistogramVec,
    pub backend_errors: IntCounterVec,
    pub handler_time: HistogramVec,
    pub wait_time: HistogramVec,
    pub available_permits: HistogramVec,
    pub max_permits: IntGaugeVec,
    pub response_size_bytes: HistogramVec,
    pub lru_cache_hits: IntCounter,
    pub lru_cache_filled: IntGaugeVec,
    pub lru_cache_bytes: IntGauge,
    pub passthrough_total_time: Histogram,
    pub passthrough_request_time: Histogram,
    pub passthrough_forward_response_time: Histogram,
    pub passthrough_errors: IntCounter,
    pub rpc_slot: IntGauge,
    pub request_retries: IntCounter,
    pub batch_requests: IntCounter,
    pub waf_rejections: IntCounter,
    pub streaming_errors: IntCounter,
}

impl RpcMetrics {
    // We have to limit number of label values to avoid slowing
    // down metrics storage.
    pub fn request_types(&self, method: &str) -> IntCounter {
        const KNOWN_METHODS: [&str; 51] = [
            "getAccountInfo",
            "getBalance",
            "getBlock",
            "getBlockCommitment",
            "getBlockHeight",
            "getBlockProduction",
            "getBlockTime",
            "getBlocks",
            "getBlocksWithLimit",
            "getClusterNodes",
            "getEpochInfo",
            "getEpochSchedule",
            "getFeeCalculatorForBlockhash",
            "getFeeRateGovernor",
            "getFees",
            "getFirstAvailableBlock",
            "getGenesisHash",
            "getHealth",
            "getIdentity",
            "getInflationGovernor",
            "getInflationRate",
            "getInflationReward",
            "getLargestAccounts",
            "getLeaderSchedule",
            "getMaxRetransmitSlot",
            "getMaxShredInsertSlot",
            "getMinimumBalanceForRentExemption",
            "getMultipleAccounts",
            "getProgramAccounts",
            "getRecentBlockhash",
            "getRecentPerformanceSamples",
            "getSignatureStatuses",
            "getSignaturesForAddress",
            "getSlot",
            "getSlotLeader",
            "getSlotLeaders",
            "getStakeActivation",
            "getSupply",
            "getTokenAccountBalance",
            "getTokenAccountsByDelegate",
            "getTokenAccountsByOwner",
            "getTokenLargestAccounts",
            "getTokenSupply",
            "getTransaction",
            "getTransactionCount",
            "getVersion",
            "getVoteAccounts",
            "minimumLedgerSlot",
            "requestAirdrop",
            "sendTransaction",
            "simulateTransaction",
        ];

        if method == "getAccountInfo"
            || method == "getProgramAccounts" // fast path
            || KNOWN_METHODS.binary_search(&method).is_ok()
        {
            self.request_types.with_label_values(&[method])
        } else {
            self.request_types.with_label_values(&["other"])
        }
    }
}

pub fn rpc_metrics() -> &'static RpcMetrics {
    static METRICS: Lazy<RpcMetrics> = Lazy::new(|| RpcMetrics {
        app_version: register_int_gauge!(
            Opts::new("app_version", "Dumb metric, see label").const_label("version", version())
        )
        .unwrap(),
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
        request_commitments: register_int_counter_vec!(
            "request_commitments",
            "Request commitment counts by type",
            &["type", "commitment"]
        )
        .unwrap(),
        account_cache_hits: register_int_counter!("account_cache_hits", "Accounts cache hit")
            .unwrap(),
        lru_cache_hits: register_int_counter!("lru_cache_hits", "LRU cache hit").unwrap(),
        lru_cache_filled: register_int_gauge_vec!(
            "lru_cache_filled",
            "LRU cache size (in entries)",
            &["worker"]
        )
        .unwrap(),
        lru_cache_bytes: register_int_gauge!("lru_cache_bytes", "LRU cache size (in bytes)")
            .unwrap(),
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
        response_uncacheable: register_int_counter_vec!(
            "response_uncacheable",
            "Could not cache response",
            &["type", "reason"]
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
        wait_time: register_histogram_vec!(
            "wait_time",
            "Time spent waiting for request limit by type",
            &["type"]
        )
        .unwrap(),
        available_permits: register_histogram_vec!(
            "available_permits",
            "Permits available to make backend requests",
            &["type"],
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 20.0, 50.0, 100.0, 200.0,
                500.0
            ]
        )
        .unwrap(),
        max_permits: register_int_gauge_vec!(
            "max_permits",
            "Permits available to make backend requests",
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
        passthrough_errors: register_int_counter!(
            "passthrough_errors",
            "Errors while processing passthrough requests"
        )
        .unwrap(),
        passthrough_forward_response_time: register_histogram!(
            "passthrough_forward_response_time",
            "Time to forward response"
        )
        .unwrap(),
        passthrough_total_time: register_histogram!(
            "passthrough_total_time",
            "Total time to process passthrough request"
        )
        .unwrap(),
        passthrough_request_time: register_histogram!(
            "passthrough_request_time",
            "Time to send passthrough request"
        )
        .unwrap(),
        rpc_slot: register_int_gauge!("rpc_slot", "slot received by polling rpc").unwrap(),
        request_retries: register_int_counter!(
            "request_retries",
            "Number of attempts to retry request after failure"
        )
        .unwrap(),
        batch_requests: register_int_counter!(
            "batch_requests",
            "Number of request which contained multiple subqueries"
        )
        .unwrap(),
        waf_rejections: register_int_counter!(
            "waf_rejection",
            "Number of requests which were rejected due to WAF rules"
        )
        .unwrap(),
        streaming_errors: register_int_counter!(
            "streaming_errors",
            "Number of errors while streaming response"
        )
        .unwrap(),
    });

    &METRICS
}
