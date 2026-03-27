use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use thiserror::Error;

const JSON_RPC_VERSION: &str = "2.0";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RpcError {
    #[error("{method} request failed: {detail}")]
    RequestFailed { method: String, detail: String },
    #[error("{method} rate limited: {detail}")]
    RateLimited { method: String, detail: String },
    #[error("{method} rpc error {code}: {message}")]
    JsonRpc {
        method: String,
        code: i64,
        message: String,
    },
    #[error("{method} response decode failed: {detail}")]
    DecodeFailed { method: String, detail: String },
    #[error("{method} http {status}: {detail}")]
    HttpStatus {
        method: String,
        status: u16,
        detail: String,
    },
}

impl RpcError {
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited { .. })
    }

    pub fn is_min_context_slot_not_reached(&self) -> bool {
        matches!(
            self,
            Self::JsonRpc {
                code: -32603 | -32016,
                message,
                ..
            } if message.contains("Min context slot not reached")
                || message.contains("Minimum context slot has not been reached")
        )
    }

    pub fn is_transient(&self) -> bool {
        self.is_rate_limited()
            || self.is_min_context_slot_not_reached()
            || matches!(
                self,
                Self::RequestFailed { .. }
                    | Self::HttpStatus {
                        status: 500..=599,
                        ..
                    }
            )
    }
}

pub fn rpc_call<T: DeserializeOwned>(
    http: &Client,
    endpoint: &str,
    method: &str,
    params: Value,
) -> Result<T, RpcError> {
    let response = http
        .post(endpoint)
        .json(&json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": 1,
            "method": method,
            "params": params,
        }))
        .send()
        .map_err(|error| RpcError::RequestFailed {
            method: method.into(),
            detail: error.to_string(),
        })?;

    let status = response.status();
    let body = response.bytes().map_err(|error| RpcError::DecodeFailed {
        method: method.into(),
        detail: error.to_string(),
    })?;
    if status.as_u16() == 429 {
        return Err(RpcError::RateLimited {
            method: method.into(),
            detail: String::from_utf8_lossy(&body).trim().to_string(),
        });
    }
    if !status.is_success() {
        return Err(RpcError::HttpStatus {
            method: method.into(),
            status: status.as_u16(),
            detail: String::from_utf8_lossy(&body).trim().to_string(),
        });
    }

    let envelope = serde_json::from_slice::<JsonRpcEnvelope<T>>(&body).map_err(|error| {
        RpcError::DecodeFailed {
            method: method.into(),
            detail: error.to_string(),
        }
    })?;
    if let Some(error) = envelope.error {
        if error.code == -32429 {
            return Err(RpcError::RateLimited {
                method: method.into(),
                detail: error.message,
            });
        }
        return Err(RpcError::JsonRpc {
            method: method.into(),
            code: error.code,
            message: error.message,
        });
    }
    envelope.result.ok_or_else(|| RpcError::DecodeFailed {
        method: method.into(),
        detail: "missing result field".into(),
    })
}

#[derive(Debug, Default)]
pub struct RpcRateLimitBackoff {
    consecutive_rate_limits: u32,
}

impl RpcRateLimitBackoff {
    pub fn on_success(&mut self) {
        self.consecutive_rate_limits = 0;
    }

    pub fn on_rate_limited(&mut self) -> Duration {
        self.consecutive_rate_limits = self.consecutive_rate_limits.saturating_add(1);
        jittered_delay(base_rate_limit_delay(self.consecutive_rate_limits))
    }

    pub fn on_error(&mut self, error: &RpcError) -> Duration {
        if !error.is_rate_limited() {
            return Duration::ZERO;
        }
        self.on_rate_limited()
    }
}

fn base_rate_limit_delay(attempt: u32) -> Duration {
    match attempt {
        0 | 1 => Duration::from_secs(1),
        2 => Duration::from_secs(2),
        3 => Duration::from_secs(5),
        4 => Duration::from_secs(10),
        5 => Duration::from_secs(20),
        _ => Duration::from_secs(30),
    }
}

fn jittered_delay(base: Duration) -> Duration {
    if base.is_zero() {
        return base;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .subsec_nanos();
    let jitter_window = (base.as_millis() as u64).max(1) / 5;
    if jitter_window == 0 {
        return base;
    }
    let offset = (nanos as u64) % (jitter_window.saturating_mul(2).saturating_add(1));
    let signed_offset = offset as i64 - jitter_window as i64;
    let millis = (base.as_millis() as i64)
        .saturating_add(signed_offset)
        .max(1) as u64;
    Duration::from_millis(millis)
}

#[derive(Debug, Deserialize)]
struct JsonRpcEnvelope<T> {
    result: Option<T>,
    error: Option<JsonRpcErrorEnvelope>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorEnvelope {
    code: i64,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::{RpcError, RpcRateLimitBackoff};

    #[test]
    fn rate_limit_backoff_resets_after_success() {
        let mut backoff = RpcRateLimitBackoff::default();
        let first = backoff.on_error(&RpcError::RateLimited {
            method: "getSlot".into(),
            detail: "rate limited".into(),
        });
        let second = backoff.on_error(&RpcError::RateLimited {
            method: "getSlot".into(),
            detail: "rate limited".into(),
        });
        assert!(second >= first);

        backoff.on_success();
        let reset = backoff.on_error(&RpcError::RateLimited {
            method: "getSlot".into(),
            detail: "rate limited".into(),
        });
        assert!(reset <= second);
    }

    #[test]
    fn json_rpc_error_body_deserializes() {
        let parsed = serde_json::from_str::<super::JsonRpcEnvelope<serde_json::Value>>(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32429,"message":"rate limited"}}"#,
        )
        .expect("json-rpc error envelope");
        assert!(parsed.result.is_none());
        assert_eq!(parsed.error.expect("error").code, -32429);
    }

    #[test]
    fn minimum_context_slot_errors_are_transient() {
        let legacy = RpcError::JsonRpc {
            method: "getMultipleAccounts".into(),
            code: -32603,
            message: "Min context slot not reached".into(),
        };
        assert!(legacy.is_min_context_slot_not_reached());
        assert!(legacy.is_transient());

        let modern = RpcError::JsonRpc {
            method: "getMultipleAccounts".into(),
            code: -32016,
            message: "Minimum context slot has not been reached".into(),
        };
        assert!(modern.is_min_context_slot_not_reached());
        assert!(modern.is_transient());
    }
}
