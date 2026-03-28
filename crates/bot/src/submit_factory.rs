use std::sync::Arc;

use submit::{JitoConfig, JitoSubmitter, SubmitMode, Submitter};

use crate::config::{BotConfig, SubmitModeConfig};

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
    Arc::new(JitoSubmitter::new(JitoConfig {
        endpoint: config.jito.endpoint.clone(),
        auth_token: config.jito.auth_token.clone(),
        bundle_enabled: config.jito.bundle_enabled,
        connect_timeout_ms: config.jito.connect_timeout_ms,
        request_timeout_ms: config.jito.request_timeout_ms,
        retry_attempts: config.jito.retry_attempts,
        retry_backoff_ms: config.jito.retry_backoff_ms,
        idempotency_cache_size: config.jito.idempotency_cache_size,
    }))
}

pub(crate) fn submit_endpoint_label(config: &BotConfig, mode: SubmitMode) -> String {
    match mode {
        SubmitMode::Bundle => request_url(&config.jito.endpoint, SubmitMode::Bundle),
        SubmitMode::SingleTransaction => {
            request_url(&config.jito.endpoint, SubmitMode::SingleTransaction)
        }
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

#[cfg(test)]
mod tests {
    use super::submit_endpoint_label;
    use crate::config::BotConfig;
    use submit::SubmitMode;

    #[test]
    fn single_transaction_endpoint_is_always_jito() {
        let mut config = BotConfig::default();
        config.jito.endpoint = "https://mainnet.block-engine.jito.wtf".into();
        config.rpc_submit.enabled = true;

        assert_eq!(
            submit_endpoint_label(&config, SubmitMode::SingleTransaction),
            "https://mainnet.block-engine.jito.wtf/api/v1/transactions"
        );
    }

    #[test]
    fn bundle_endpoint_is_always_jito() {
        let mut config = BotConfig::default();
        config.jito.endpoint = "https://mainnet.block-engine.jito.wtf".into();

        assert_eq!(
            submit_endpoint_label(&config, SubmitMode::Bundle),
            "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
        );
    }
}
