use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{
    StatusCode,
    blocking::Client,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    SubmissionId, SubmitAttemptOutcome, SubmitError, SubmitMode, SubmitRejectionReason,
    SubmitRequest, SubmitResult, SubmitStatus, Submitter,
};

const JSON_RPC_VERSION: &str = "2.0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcConfig {
    pub endpoint: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub skip_preflight: bool,
    pub max_retries: usize,
    pub idempotency_cache_size: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://api.mainnet-beta.solana.com".into(),
            connect_timeout_ms: 200,
            request_timeout_ms: 250,
            skip_preflight: true,
            max_retries: 0,
            idempotency_cache_size: 1_024,
        }
    }
}

#[derive(Debug)]
pub struct RpcSubmitter {
    config: RpcConfig,
    http: RpcHttpClient,
    idempotency: Mutex<IdempotencyCache>,
}

impl RpcSubmitter {
    pub fn new(config: RpcConfig) -> Self {
        Self {
            http: RpcHttpClient::new(&config),
            idempotency: Mutex::new(IdempotencyCache::default()),
            config,
        }
    }
}

impl Submitter for RpcSubmitter {
    fn submit_attempt(&self, request: SubmitRequest) -> Result<SubmitAttemptOutcome, SubmitError> {
        if request.mode != SubmitMode::SingleTransaction {
            return Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Rejected,
                submission_id: SubmissionId(format!(
                    "rpc-bundle-disabled-{}",
                    signature_component(&request.envelope.signature)
                )),
                endpoint: self.config.endpoint.clone(),
                rejection: Some(SubmitRejectionReason::BundleDisabled),
            }));
        }

        let signature = request.envelope.signature.clone();
        let key = IdempotencyKey {
            signature: signature.clone(),
            mode: request.mode,
        };
        if let Some(cached) = self
            .idempotency
            .lock()
            .expect("rpc idempotency lock")
            .get(&key)
        {
            return Ok(SubmitAttemptOutcome::Terminal(cached));
        }

        if self.config.endpoint.starts_with("mock://") {
            let result = SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId(format!(
                    "rpc-single-{}",
                    signature_component(&request.envelope.signature)
                )),
                endpoint: self.config.endpoint.clone(),
                rejection: None,
            };
            self.idempotency
                .lock()
                .expect("rpc idempotency lock")
                .insert(key, result.clone(), self.config.idempotency_cache_size);
            return Ok(SubmitAttemptOutcome::Terminal(result));
        }

        let encoded_message = STANDARD.encode(&request.envelope.signed_message);
        let response = self.http.send(
            &encoded_message,
            self.config.skip_preflight,
            self.config.max_retries,
        )?;
        if let Some(result) = response
            .parsed
            .as_ref()
            .and_then(|envelope| envelope.result.as_ref())
            .and_then(Value::as_str)
        {
            let result = SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId(format!("rpc-single-{result}")),
                endpoint: response.request_url,
                rejection: None,
            };
            self.idempotency
                .lock()
                .expect("rpc idempotency lock")
                .insert(key, result.clone(), self.config.idempotency_cache_size);
            return Ok(SubmitAttemptOutcome::Terminal(result));
        }

        let rejection = classify_rpc_rejection(
            response.status,
            response
                .parsed
                .as_ref()
                .and_then(|envelope| envelope.error.as_ref()),
            &response.body,
            response.retry_after,
        );
        let result = SubmitResult {
            status: SubmitStatus::Rejected,
            submission_id: SubmissionId(format!(
                "rpc-{}-{}",
                rejection_code(&rejection.reason),
                signature_component(&request.envelope.signature)
            )),
            endpoint: response.request_url,
            rejection: Some(rejection.reason.clone()),
        };
        Ok(attempt_outcome(result, &rejection))
    }
}

#[derive(Debug)]
struct RpcHttpClient {
    endpoint: String,
    client: Client,
}

impl RpcHttpClient {
    fn new(config: &RpcConfig) -> Self {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .default_headers(default_headers)
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(2)
            .build()
            .expect("rpc HTTP client should build with valid static defaults");

        Self {
            endpoint: config.endpoint.clone(),
            client,
        }
    }

    fn send(
        &self,
        encoded_message: &str,
        skip_preflight: bool,
        max_retries: usize,
    ) -> Result<HttpAttemptResponse, SubmitError> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({
                "id": 1,
                "jsonrpc": JSON_RPC_VERSION,
                "method": "sendTransaction",
                "params": [
                    encoded_message,
                    {
                        "encoding": "base64",
                        "skipPreflight": skip_preflight,
                        "maxRetries": max_retries,
                    }
                ],
            }))
            .send()
            .map_err(map_transport_error)?;
        let status = response.status();
        let retry_after = retry_after_from_headers(response.headers());
        let body_bytes = response.bytes().map_err(map_transport_error)?;
        let body = String::from_utf8_lossy(&body_bytes).into_owned();
        let parsed = serde_json::from_slice::<JsonRpcEnvelope>(body_bytes.as_ref()).ok();

        Ok(HttpAttemptResponse {
            status,
            request_url: self.endpoint.clone(),
            retry_after,
            body,
            parsed,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IdempotencyKey {
    signature: String,
    mode: SubmitMode,
}

#[derive(Debug, Default)]
struct IdempotencyCache {
    entries: HashMap<IdempotencyKey, SubmitResult>,
    order: VecDeque<IdempotencyKey>,
}

impl IdempotencyCache {
    fn get(&self, key: &IdempotencyKey) -> Option<SubmitResult> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: IdempotencyKey, result: SubmitResult, capacity: usize) {
        if capacity == 0 {
            return;
        }
        if !self.entries.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.entries.insert(key, result);
        while self.entries.len() > capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

#[derive(Debug)]
struct HttpAttemptResponse {
    status: StatusCode,
    request_url: String,
    retry_after: Option<Duration>,
    body: String,
    parsed: Option<JsonRpcEnvelope>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcEnvelope {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Debug)]
struct ClassifiedRejection {
    reason: SubmitRejectionReason,
    retryable: bool,
    retry_after: Option<Duration>,
}

fn classify_rpc_rejection(
    status: StatusCode,
    error: Option<&JsonRpcError>,
    body: &str,
    retry_after: Option<Duration>,
) -> ClassifiedRejection {
    let code = error.map(|error| error.code);
    let text = error_text(error, body).to_ascii_lowercase();
    let reason = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        || text.contains("unauthorized")
    {
        SubmitRejectionReason::Unauthorized
    } else if status == StatusCode::TOO_MANY_REQUESTS
        || text.contains("rate limit")
        || text.contains("too many requests")
    {
        SubmitRejectionReason::RateLimited
    } else if text.contains("duplicate")
        || text.contains("already processed")
        || text.contains("already received")
    {
        SubmitRejectionReason::DuplicateSubmission
    } else if matches!(code, Some(-32700 | -32600 | -32601 | -32602))
        || text.contains("invalid request")
        || text.contains("invalid params")
    {
        SubmitRejectionReason::InvalidRequest
    } else if text.contains("signature verification")
        || text.contains("failed to deserialize")
        || text.contains("invalid transaction")
        || text.contains("malformed transaction")
        || text.contains("base64")
        || text.contains("base58")
        || (text.contains("decode") && text.contains("transaction"))
    {
        SubmitRejectionReason::InvalidEnvelope
    } else if matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    ) || text.contains("unavailable")
        || text.contains("node is unhealthy")
        || text.contains("temporarily unavailable")
        || text.contains("leader unavailable")
    {
        SubmitRejectionReason::ChannelUnavailable
    } else {
        SubmitRejectionReason::RemoteRejected
    };

    let retryable = matches!(
        reason,
        SubmitRejectionReason::RateLimited | SubmitRejectionReason::ChannelUnavailable
    );
    ClassifiedRejection {
        reason,
        retryable,
        retry_after: retryable.then_some(retry_after).flatten(),
    }
}

fn attempt_outcome(result: SubmitResult, rejection: &ClassifiedRejection) -> SubmitAttemptOutcome {
    if rejection.retryable {
        SubmitAttemptOutcome::Retry {
            terminal_result: result,
            retry_after: rejection.retry_after,
        }
    } else {
        SubmitAttemptOutcome::Terminal(result)
    }
}

fn error_text(error: Option<&JsonRpcError>, body: &str) -> String {
    if let Some(error) = error {
        match &error.data {
            Some(Value::String(extra)) => format!("{} {extra}", error.message),
            Some(extra) => format!("{} {extra}", error.message),
            None => error.message.clone(),
        }
    } else {
        body.to_owned()
    }
}

fn rejection_code(reason: &SubmitRejectionReason) -> &'static str {
    match reason {
        SubmitRejectionReason::InvalidEnvelope => "invalid-envelope",
        SubmitRejectionReason::InvalidRequest => "invalid-request",
        SubmitRejectionReason::BundleDisabled => "bundle-disabled",
        SubmitRejectionReason::Unauthorized => "unauthorized",
        SubmitRejectionReason::RateLimited => "rate-limited",
        SubmitRejectionReason::DuplicateSubmission => "duplicate",
        SubmitRejectionReason::TipTooLow => "tip-too-low",
        SubmitRejectionReason::PathCongested => "path-congested",
        SubmitRejectionReason::ChannelUnavailable => "channel-unavailable",
        SubmitRejectionReason::RemoteRejected => "remote-rejected",
    }
}

fn retry_after_from_headers(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn map_transport_error(error: reqwest::Error) -> SubmitError {
    if error.is_decode() {
        SubmitError::UpstreamProtocol
    } else {
        SubmitError::TransportUnavailable
    }
}

fn signature_component(signature: &str) -> &str {
    if signature.is_empty() {
        "unknown"
    } else {
        signature
    }
}
