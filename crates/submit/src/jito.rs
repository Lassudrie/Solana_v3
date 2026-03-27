use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    thread,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{
    StatusCode,
    blocking::Client,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tungstenite::{client::IntoClientRequest, connect};

use crate::{
    submitter::{SubmitError, Submitter},
    types::{
        SubmissionId, SubmitMode, SubmitRejectionReason, SubmitRequest, SubmitResult, SubmitStatus,
    },
};

const JSON_RPC_VERSION: &str = "2.0";
const JITO_AUTH_HEADER: &str = "x-jito-auth";
const JITO_BUNDLE_ID_HEADER: &str = "x-bundle-id";
const JITO_WAIT_TO_RETRY_HEADER: &str = "x-wait-to-retry-ms";
const TRANSACTION_PATH: &str = "/api/v1/transactions";
const BUNDLE_PATH: &str = "/api/v1/bundles";
const API_V1_PATH: &str = "/api/v1";
const MOCK_SCHEME: &str = "mock://";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JitoConfig {
    pub endpoint: String,
    pub ws_endpoint: String,
    pub auth_token: Option<String>,
    pub bundle_enabled: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub retry_attempts: usize,
    pub retry_backoff_ms: u64,
    pub idempotency_cache_size: usize,
}

impl Default for JitoConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.block-engine.jito.wtf".into(),
            ws_endpoint: "wss://bundles.jito.wtf/api/v1/bundles/tip_stream".into(),
            auth_token: None,
            bundle_enabled: true,
            connect_timeout_ms: 300,
            request_timeout_ms: 1_000,
            retry_attempts: 3,
            retry_backoff_ms: 50,
            idempotency_cache_size: 1_024,
        }
    }
}

#[derive(Debug)]
pub struct JitoSubmitter {
    config: JitoConfig,
    http: JitoHttpClient,
    ws: JitoWsClient,
    idempotency: Mutex<IdempotencyCache>,
}

impl JitoSubmitter {
    pub fn new(config: JitoConfig) -> Self {
        Self {
            http: JitoHttpClient::new(&config),
            ws: JitoWsClient::new(&config),
            config,
            idempotency: Mutex::new(IdempotencyCache::default()),
        }
    }

    fn submit_live(
        &self,
        request: &SubmitRequest,
        cache_key: IdempotencyKey,
        encoded_message: &str,
    ) -> Result<SubmitResult, SubmitError> {
        let attempts = self.config.retry_attempts.max(1);
        let mut saw_transport_failure = false;

        for attempt in 0..attempts {
            match self.send_once(request, encoded_message)? {
                AttemptOutcome::Accepted(result) => {
                    self.remember(cache_key.clone(), result.clone());
                    return Ok(result);
                }
                AttemptOutcome::Rejected(result) => return Ok(result),
                AttemptOutcome::RetryableRejection {
                    result,
                    retry_after,
                } => {
                    if attempt + 1 == attempts {
                        return Ok(result);
                    }
                    thread::sleep(retry_after.unwrap_or_else(|| self.retry_delay(attempt)));
                }
                AttemptOutcome::RetryableTransport => {
                    saw_transport_failure = true;
                    if attempt + 1 == attempts {
                        break;
                    }
                    thread::sleep(self.retry_delay(attempt));
                }
            }
        }

        if saw_transport_failure && self.ws.probe() {
            return Ok(self.rejected_result(
                request,
                SubmitRejectionReason::ChannelUnavailable,
                self.http.request_url(request.mode),
            ));
        }

        Err(SubmitError::TransportUnavailable)
    }

    fn send_once(
        &self,
        request: &SubmitRequest,
        encoded_message: &str,
    ) -> Result<AttemptOutcome, SubmitError> {
        let response = match self.http.send(request.mode, encoded_message) {
            Ok(response) => response,
            Err(SubmitError::TransportUnavailable) => {
                return Ok(AttemptOutcome::RetryableTransport);
            }
            Err(error) => return Err(error),
        };
        if let Some(error) = response
            .parsed
            .as_ref()
            .and_then(|body| body.error.as_ref())
        {
            let rejection = classify_rejection(
                response.status,
                Some(error),
                &response.body,
                response.retry_after,
            );
            let result =
                self.rejected_result(request, rejection.reason, response.request_url.clone());
            return Ok(if rejection.retryable {
                AttemptOutcome::RetryableRejection {
                    result,
                    retry_after: rejection.retry_after,
                }
            } else {
                AttemptOutcome::Rejected(result)
            });
        }

        if !response.status.is_success() {
            let rejection =
                classify_rejection(response.status, None, &response.body, response.retry_after);
            let result =
                self.rejected_result(request, rejection.reason, response.request_url.clone());
            return Ok(if rejection.retryable {
                AttemptOutcome::RetryableRejection {
                    result,
                    retry_after: rejection.retry_after,
                }
            } else {
                AttemptOutcome::Rejected(result)
            });
        }

        let result = response
            .parsed
            .as_ref()
            .and_then(|body| body.result.as_ref())
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(SubmitError::UpstreamProtocol)?;
        let submission_id = match request.mode {
            SubmitMode::SingleTransaction => response.bundle_id.unwrap_or(result),
            SubmitMode::Bundle => result,
        };

        Ok(AttemptOutcome::Accepted(SubmitResult {
            status: SubmitStatus::Accepted,
            submission_id: SubmissionId(submission_id),
            endpoint: response.request_url,
            rejection: None,
        }))
    }

    fn rejected_result(
        &self,
        request: &SubmitRequest,
        reason: SubmitRejectionReason,
        endpoint: String,
    ) -> SubmitResult {
        SubmitResult {
            status: SubmitStatus::Rejected,
            submission_id: SubmissionId(format!(
                "jito-{}-{}",
                rejection_code(&reason),
                signature_component(&request.envelope.signature),
            )),
            endpoint,
            rejection: Some(reason),
        }
    }

    fn remember(&self, key: IdempotencyKey, result: SubmitResult) {
        if let Ok(mut cache) = self.idempotency.lock() {
            cache.insert(key, result, self.config.idempotency_cache_size);
        }
    }

    fn cached(&self, key: &IdempotencyKey) -> Option<SubmitResult> {
        self.idempotency
            .lock()
            .ok()
            .and_then(|cache| cache.get(key))
    }

    fn retry_delay(&self, attempt: usize) -> Duration {
        let multiplier = 1_u64 << attempt.min(8);
        Duration::from_millis(self.config.retry_backoff_ms.saturating_mul(multiplier))
    }

    fn mock_submit(&self, request: &SubmitRequest) -> SubmitResult {
        let mode_suffix = match request.mode {
            SubmitMode::SingleTransaction => "single",
            SubmitMode::Bundle => "bundle",
        };

        SubmitResult {
            status: SubmitStatus::Accepted,
            submission_id: SubmissionId(format!(
                "jito-{mode_suffix}-{}",
                signature_component(&request.envelope.signature),
            )),
            endpoint: self.http.request_url(request.mode),
            rejection: None,
        }
    }
}

impl Submitter for JitoSubmitter {
    fn submit(&self, request: SubmitRequest) -> Result<SubmitResult, SubmitError> {
        if request.envelope.signed_message.is_empty() {
            return Ok(self.rejected_result(
                &request,
                SubmitRejectionReason::InvalidEnvelope,
                self.http.request_url(request.mode),
            ));
        }

        if matches!(request.mode, SubmitMode::Bundle) && !self.config.bundle_enabled {
            return Ok(self.rejected_result(
                &request,
                SubmitRejectionReason::BundleDisabled,
                self.http.request_url(request.mode),
            ));
        }

        let cache_key = IdempotencyKey {
            signature: request.envelope.signature.clone(),
            mode: request.mode,
        };
        if let Some(result) = self.cached(&cache_key) {
            return Ok(result);
        }

        if self.http.is_mock() {
            let result = self.mock_submit(&request);
            self.remember(cache_key, result.clone());
            return Ok(result);
        }

        let encoded_message = BASE64_STANDARD.encode(&request.envelope.signed_message);
        self.submit_live(&request, cache_key, &encoded_message)
    }
}

#[derive(Debug)]
struct JitoHttpClient {
    endpoint: String,
    auth_token: Option<String>,
    client: Client,
}

impl JitoHttpClient {
    fn new(config: &JitoConfig) -> Self {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .default_headers(default_headers)
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(config.retry_attempts.max(2))
            .build()
            .expect("jito HTTP client should build with valid static defaults");

        Self {
            endpoint: config.endpoint.clone(),
            auth_token: config.auth_token.clone(),
            client,
        }
    }

    fn is_mock(&self) -> bool {
        self.endpoint.starts_with(MOCK_SCHEME)
    }

    fn request_url(&self, mode: SubmitMode) -> String {
        let endpoint = self.endpoint.trim_end_matches('/');
        match mode {
            SubmitMode::SingleTransaction => {
                if endpoint.ends_with(TRANSACTION_PATH) {
                    endpoint.to_owned()
                } else if endpoint.ends_with(API_V1_PATH) {
                    format!("{endpoint}/transactions")
                } else {
                    format!("{endpoint}{TRANSACTION_PATH}")
                }
            }
            SubmitMode::Bundle => {
                if endpoint.ends_with(BUNDLE_PATH) {
                    endpoint.to_owned()
                } else if endpoint.ends_with(API_V1_PATH) {
                    format!("{endpoint}/bundles")
                } else {
                    format!("{endpoint}{BUNDLE_PATH}")
                }
            }
        }
    }

    fn send(
        &self,
        mode: SubmitMode,
        encoded_message: &str,
    ) -> Result<HttpAttemptResponse, SubmitError> {
        let request_url = self.request_url(mode);
        let payload = match mode {
            SubmitMode::SingleTransaction => json!({
                "id": 1,
                "jsonrpc": JSON_RPC_VERSION,
                "method": "sendTransaction",
                "params": [encoded_message, { "encoding": "base64" }],
            }),
            SubmitMode::Bundle => json!({
                "id": 1,
                "jsonrpc": JSON_RPC_VERSION,
                "method": "sendBundle",
                "params": [[encoded_message], { "encoding": "base64" }],
            }),
        };

        let mut request = self.client.post(&request_url);
        if let Some(token) = self.auth_token.as_deref() {
            request = request.header(auth_header_name(), token);
        }

        let response = request.json(&payload).send().map_err(map_transport_error)?;
        let status = response.status();
        let retry_after = retry_after_from_headers(response.headers());
        let bundle_id = response
            .headers()
            .get(JITO_BUNDLE_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body_bytes = response.bytes().map_err(map_transport_error)?;
        let body = String::from_utf8_lossy(&body_bytes).into_owned();
        let parsed = serde_json::from_slice::<JsonRpcEnvelope>(body_bytes.as_ref()).ok();

        Ok(HttpAttemptResponse {
            status,
            request_url,
            bundle_id,
            retry_after,
            body,
            parsed,
        })
    }
}

#[derive(Debug, Clone)]
struct JitoWsClient {
    endpoint: String,
    auth_token: Option<String>,
}

impl JitoWsClient {
    fn new(config: &JitoConfig) -> Self {
        Self {
            endpoint: config.ws_endpoint.clone(),
            auth_token: config.auth_token.clone(),
        }
    }

    fn probe(&self) -> bool {
        if self.endpoint.is_empty() || self.endpoint.starts_with(MOCK_SCHEME) {
            return true;
        }

        let mut request = match self.endpoint.as_str().into_client_request() {
            Ok(request) => request,
            Err(_) => return false,
        };
        if let Some(token) = self.auth_token.as_deref() {
            let Ok(value) = HeaderValue::from_str(token) else {
                return false;
            };
            request.headers_mut().insert(auth_header_name(), value);
        }

        match connect(request) {
            Ok((mut socket, _)) => socket.close(None).is_ok(),
            Err(_) => false,
        }
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
    bundle_id: Option<String>,
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
enum AttemptOutcome {
    Accepted(SubmitResult),
    Rejected(SubmitResult),
    RetryableRejection {
        result: SubmitResult,
        retry_after: Option<Duration>,
    },
    #[allow(dead_code)]
    RetryableTransport,
}

#[derive(Debug)]
struct ClassifiedRejection {
    reason: SubmitRejectionReason,
    retryable: bool,
    retry_after: Option<Duration>,
}

fn auth_header_name() -> HeaderName {
    HeaderName::from_static(JITO_AUTH_HEADER)
}

fn map_transport_error(error: reqwest::Error) -> SubmitError {
    if error.is_decode() {
        SubmitError::UpstreamProtocol
    } else {
        SubmitError::TransportUnavailable
    }
}

fn retry_after_from_headers(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(JITO_WAIT_TO_RETRY_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .or_else(|| {
            headers
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
        })
}

fn classify_rejection(
    status: StatusCode,
    error: Option<&JsonRpcError>,
    body: &str,
    retry_after: Option<Duration>,
) -> ClassifiedRejection {
    let code = error.map(|error| error.code);
    let text = error_text(error, body).to_ascii_lowercase();
    let reason = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        || text.contains("unauthorized")
        || text.contains("not authorized")
        || text.contains("x-jito-auth")
    {
        SubmitRejectionReason::Unauthorized
    } else if status == StatusCode::TOO_MANY_REQUESTS
        || text.contains("rate limit")
        || text.contains("too many requests")
        || text.contains("limit:")
    {
        SubmitRejectionReason::RateLimited
    } else if text.contains("duplicate")
        || text.contains("already processed")
        || text.contains("already received")
    {
        SubmitRejectionReason::DuplicateSubmission
    } else if text.contains("minimum tip")
        || text.contains("tip too low")
        || (text.contains("tip") && text.contains("insufficient"))
    {
        SubmitRejectionReason::TipTooLow
    } else if text.contains("bundle") && text.contains("disabled") {
        SubmitRejectionReason::BundleDisabled
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
        || text.contains("temporarily unavailable")
        || text.contains("leader unavailable")
    {
        SubmitRejectionReason::ChannelUnavailable
    } else if status == StatusCode::BAD_REQUEST {
        SubmitRejectionReason::InvalidRequest
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

fn signature_component(signature: &str) -> &str {
    if signature.is_empty() {
        "unknown"
    } else {
        signature
    }
}
