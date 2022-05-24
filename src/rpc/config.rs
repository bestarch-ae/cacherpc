use std::sync::Arc;

use super::state::State;
use actix_web::web;
use serde::Deserialize;

use tracing::{info, warn};

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub request_limits: RequestLimits,
    #[serde(default)]
    pub request_queue_size: RequestQueueSize,
    #[serde(default)]
    pub timeouts: Timeouts,
    #[serde(default)]
    pub ignore_base58_limit: bool,
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub struct RequestLimits {
    pub account_info: usize,
    pub program_accounts: usize,
}

/// Request and retry timouts in seconds for each type of request
#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub struct Timeouts {
    pub account_info_request: u64,
    pub program_accounts_request: u64,
    pub account_info_backoff: u64,
    pub program_accounts_backoff: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            account_info_request: crate::DEFAULT_GAI_TIMEOUT,
            program_accounts_request: crate::DEFAULT_GPA_TIMEOUT,
            account_info_backoff: crate::DEFAULT_GAI_BACKOFF,
            program_accounts_backoff: crate::DEFAULT_GPA_BACKOFF,
        }
    }
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

async fn apply_config(app_state: &web::Data<State>, new_config: Config) {
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

pub(super) async fn check_config_change(state: &web::Data<State>) {
    use futures_util::FutureExt;
    // apply new config (if any)
    {
        let config = {
            let mut rx = state.config_watch.borrow_mut();
            if rx.changed().now_or_never().is_some() {
                Some(rx.borrow().clone())
            } else {
                None
            }
        };
        if let Some(config) = config {
            apply_config(state, config).await;
        }
    }
    // apply new lua rules (if any)
    {
        let changed = state
            .waf_watch
            .borrow_mut()
            .changed()
            .now_or_never()
            .is_some();
        if changed {
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
