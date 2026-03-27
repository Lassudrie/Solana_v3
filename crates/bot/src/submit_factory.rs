use std::sync::Arc;

use submit::{
    JitoConfig, JitoSubmitter, RoutedSubmitter, RpcConfig, RpcSubmitter,
    SingleTransactionRoutingPolicy, SubmitMode, Submitter,
};

use crate::config::{BotConfig, SingleTransactionSubmitPolicyConfig, SubmitModeConfig};

const API_V1_PATH: &str = "/api/v1";
const TRANSACTION_PATH: &str = "/api/v1/transactions";
const BUNDLE_PATH: &str = "/api/v1/bundles";

pub(crate) fn submit_mode_from_config(config: &BotConfig) -> SubmitMode {
    match config.submit.mode {
        SubmitModeConfig::SingleTransaction => SubmitMode::SingleTransaction,
        SubmitModeConfig::Bundle => SubmitMode::Bundle,
    }
}

pub(crate) fn build_submitter(config: &BotConfig) -> Arc<dyn Submitter> {
    let jito: Arc<dyn Submitter> = Arc::new(JitoSubmitter::new(JitoConfig {
        endpoint: config.jito.endpoint.clone(),
        auth_token: config.jito.auth_token.clone(),
        bundle_enabled: config.jito.bundle_enabled,
        connect_timeout_ms: config.jito.connect_timeout_ms,
        request_timeout_ms: config.jito.request_timeout_ms,
        retry_attempts: config.jito.retry_attempts,
        retry_backoff_ms: config.jito.retry_backoff_ms,
        idempotency_cache_size: config.jito.idempotency_cache_size,
    }));
    let rpc = config.rpc_submit.enabled.then(|| {
        Arc::new(RpcSubmitter::new(RpcConfig {
            endpoint: rpc_submit_endpoint(config),
            connect_timeout_ms: config.rpc_submit.connect_timeout_ms,
            request_timeout_ms: config.rpc_submit.request_timeout_ms,
            skip_preflight: config.rpc_submit.skip_preflight,
            max_retries: config.rpc_submit.max_retries,
            idempotency_cache_size: config.rpc_submit.idempotency_cache_size,
        })) as Arc<dyn Submitter>
    });
    Arc::new(RoutedSubmitter::new(
        Some(jito),
        rpc,
        single_transaction_policy(config),
    ))
}

pub(crate) fn submit_endpoint_label(config: &BotConfig, mode: SubmitMode) -> String {
    match mode {
        SubmitMode::Bundle => request_url(&config.jito.endpoint, SubmitMode::Bundle),
        SubmitMode::SingleTransaction => {
            let rpc_enabled = config.rpc_submit.enabled;
            match config.submit.single_transaction_policy {
                SingleTransactionSubmitPolicyConfig::JitoOnly => {
                    request_url(&config.jito.endpoint, SubmitMode::SingleTransaction)
                }
                SingleTransactionSubmitPolicyConfig::RpcOnly if rpc_enabled => {
                    rpc_submit_endpoint(config)
                }
                SingleTransactionSubmitPolicyConfig::Fanout if rpc_enabled => format!(
                    "fanout://{}|{}",
                    rpc_submit_endpoint(config),
                    request_url(&config.jito.endpoint, SubmitMode::SingleTransaction)
                ),
                SingleTransactionSubmitPolicyConfig::LeaderAware if rpc_enabled => format!(
                    "leader-aware://{}|{}",
                    rpc_submit_endpoint(config),
                    request_url(&config.jito.endpoint, SubmitMode::SingleTransaction)
                ),
                _ => request_url(&config.jito.endpoint, SubmitMode::SingleTransaction),
            }
        }
    }
}

fn single_transaction_policy(config: &BotConfig) -> SingleTransactionRoutingPolicy {
    match config.submit.single_transaction_policy {
        SingleTransactionSubmitPolicyConfig::JitoOnly => SingleTransactionRoutingPolicy::JitoOnly,
        SingleTransactionSubmitPolicyConfig::RpcOnly => SingleTransactionRoutingPolicy::RpcOnly,
        SingleTransactionSubmitPolicyConfig::Fanout => SingleTransactionRoutingPolicy::Fanout,
        SingleTransactionSubmitPolicyConfig::LeaderAware => {
            SingleTransactionRoutingPolicy::LeaderAware
        }
    }
}

fn rpc_submit_endpoint(config: &BotConfig) -> String {
    if config.rpc_submit.endpoint.is_empty() {
        config.reconciliation.rpc_http_endpoint.clone()
    } else {
        config.rpc_submit.endpoint.clone()
    }
}

fn request_url(endpoint: &str, mode: SubmitMode) -> String {
    match mode {
        SubmitMode::SingleTransaction => {
            if endpoint.ends_with(API_V1_PATH) {
                format!("{endpoint}/transactions")
            } else if endpoint.ends_with("/transactions") {
                endpoint.to_owned()
            } else {
                format!("{endpoint}{TRANSACTION_PATH}")
            }
        }
        SubmitMode::Bundle => {
            if endpoint.ends_with(API_V1_PATH) {
                format!("{endpoint}/bundles")
            } else if endpoint.ends_with("/bundles") {
                endpoint.to_owned()
            } else {
                format!("{endpoint}{BUNDLE_PATH}")
            }
        }
    }
}
