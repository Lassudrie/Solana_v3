use std::{
    collections::{HashMap, HashSet},
    net::TcpStream,
    time::Duration,
};

use reqwest::{Url, blocking::Client};
use serde::Deserialize;
use serde_json::{Value, json};
use submit::{SubmissionId, SubmitMode};
use tungstenite::{
    Message, WebSocket, client::IntoClientRequest, client_tls_with_config, error::Error as WsError,
    stream::MaybeTlsStream,
};

use crate::{
    classifier::FailureClass,
    tracker::{ExecutionRecord, ExecutionTracker, ExecutionTransition, InclusionStatus},
};

const MOCK_SCHEME: &str = "mock://";
const JSON_RPC_VERSION: &str = "2.0";
const SIGNATURE_STATUS_BATCH_LIMIT: usize = 256;
const BUNDLE_STATUS_BATCH_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationConfig {
    pub enabled: bool,
    pub rpc_http_endpoint: String,
    pub rpc_ws_endpoint: String,
    pub websocket_enabled: bool,
    pub websocket_timeout_ms: u64,
    pub search_transaction_history: bool,
    pub max_pending_slots: u64,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rpc_http_endpoint: "https://api.mainnet-beta.solana.com".into(),
            rpc_ws_endpoint: "wss://api.mainnet-beta.solana.com".into(),
            websocket_enabled: true,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            max_pending_slots: 150,
        }
    }
}

#[derive(Debug)]
pub struct OnChainReconciler {
    config: ReconciliationConfig,
    rpc: RpcStatusClient,
    ws: SignatureWsClient,
}

impl Default for OnChainReconciler {
    fn default() -> Self {
        Self::disabled()
    }
}

impl OnChainReconciler {
    pub fn new(config: ReconciliationConfig) -> Self {
        Self {
            rpc: RpcStatusClient::new(&config.rpc_http_endpoint),
            ws: SignatureWsClient::new(
                &config.rpc_ws_endpoint,
                Duration::from_millis(config.websocket_timeout_ms),
            ),
            config,
        }
    }

    pub fn disabled() -> Self {
        Self::new(ReconciliationConfig {
            enabled: false,
            ..ReconciliationConfig::default()
        })
    }

    pub fn tick(
        &mut self,
        tracker: &mut ExecutionTracker,
        observed_slot: u64,
    ) -> Vec<ExecutionTransition> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut transitions = self.expire_stale_submissions(tracker, observed_slot);
        let mut pending = tracker.pending_records();
        if pending.is_empty() {
            return transitions;
        }

        if self.config.websocket_enabled {
            self.ws.ensure_subscriptions(&pending);
            transitions.extend(self.ws.drain_notifications(tracker));
        }

        pending = tracker.pending_records();
        if pending.is_empty() {
            return transitions;
        }

        transitions.extend(self.poll_transaction_statuses(tracker, &pending));
        transitions.extend(self.poll_bundle_statuses(tracker, &pending));
        transitions
    }

    fn expire_stale_submissions(
        &self,
        tracker: &mut ExecutionTracker,
        observed_slot: u64,
    ) -> Vec<ExecutionTransition> {
        let mut transitions = Vec::new();
        for record in tracker.pending_records() {
            if observed_slot.saturating_sub(record.build_slot) > self.config.max_pending_slots {
                if let Some(transition) = tracker.transition(
                    &record.submission_id,
                    InclusionStatus::Expired { observed_slot },
                ) {
                    transitions.push(transition);
                }
            }
        }
        transitions
    }

    fn poll_transaction_statuses(
        &mut self,
        tracker: &mut ExecutionTracker,
        pending: &[ExecutionRecord],
    ) -> Vec<ExecutionTransition> {
        let mut transitions = Vec::new();
        for batch in pending.chunks(SIGNATURE_STATUS_BATCH_LIMIT) {
            let signatures = batch
                .iter()
                .map(|record| record.chain_signature.clone())
                .collect::<Vec<_>>();
            let Some(statuses) = self
                .rpc
                .get_signature_statuses(&signatures, self.config.search_transaction_history)
            else {
                continue;
            };
            for (record, status) in batch.iter().zip(statuses.into_iter()) {
                match status {
                    Some(status) if status.err.is_some() => {
                        if let Some(transition) = tracker.transition(
                            &record.submission_id,
                            InclusionStatus::Failed(FailureClass::ChainExecutionFailed),
                        ) {
                            transitions.push(transition);
                        }
                    }
                    Some(status) => {
                        if let Some(transition) = tracker.transition(
                            &record.submission_id,
                            InclusionStatus::Landed { slot: status.slot },
                        ) {
                            transitions.push(transition);
                        }
                    }
                    None => {}
                }
            }
        }
        transitions
    }

    fn poll_bundle_statuses(
        &self,
        tracker: &mut ExecutionTracker,
        pending: &[ExecutionRecord],
    ) -> Vec<ExecutionTransition> {
        let mut transitions = Vec::new();
        let mut by_endpoint = HashMap::<String, Vec<ExecutionRecord>>::new();
        for record in pending
            .iter()
            .filter(|record| record.submit_mode == SubmitMode::Bundle)
            .cloned()
        {
            by_endpoint
                .entry(bundle_status_url(&record.submit_endpoint))
                .or_default()
                .push(record);
        }

        for (endpoint, records) in by_endpoint {
            for batch in records.chunks(BUNDLE_STATUS_BATCH_LIMIT) {
                let bundle_ids = batch
                    .iter()
                    .map(|record| record.submission_id.0.clone())
                    .collect::<Vec<_>>();
                let Some(statuses) = self
                    .rpc
                    .get_inflight_bundle_statuses(&endpoint, &bundle_ids)
                else {
                    continue;
                };
                let status_map = statuses
                    .into_iter()
                    .map(|status| (status.bundle_id.clone(), status))
                    .collect::<HashMap<_, _>>();
                for record in batch {
                    let Some(status) = status_map.get(&record.submission_id.0) else {
                        continue;
                    };
                    match status.status.as_str() {
                        "Pending" => {}
                        "Landed" => {}
                        "Failed" | "Invalid" => {
                            if let Some(transition) = tracker.transition(
                                &record.submission_id,
                                InclusionStatus::Failed(FailureClass::TransportFailed),
                            ) {
                                transitions.push(transition);
                            }
                        }
                        _ => {
                            if let Some(transition) = tracker.transition(
                                &record.submission_id,
                                InclusionStatus::Failed(FailureClass::Unknown),
                            ) {
                                transitions.push(transition);
                            }
                        }
                    }
                }
            }
        }
        transitions
    }
}

#[derive(Debug)]
struct RpcStatusClient {
    endpoint: String,
    http: Client,
}

impl RpcStatusClient {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            http: Client::builder()
                .connect_timeout(Duration::from_millis(300))
                .timeout(Duration::from_millis(1_000))
                .build()
                .expect("reconciliation HTTP client should build"),
        }
    }

    fn is_mock(&self, endpoint: &str) -> bool {
        endpoint.starts_with(MOCK_SCHEME)
    }

    fn get_signature_statuses(
        &self,
        signatures: &[String],
        search_transaction_history: bool,
    ) -> Option<Vec<Option<RpcSignatureStatus>>> {
        if signatures.is_empty() || self.is_mock(&self.endpoint) {
            return Some(
                std::iter::repeat_with(|| None)
                    .take(signatures.len())
                    .collect(),
            );
        }

        let response = self
            .http
            .post(&self.endpoint)
            .json(&json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [signatures, { "searchTransactionHistory": search_transaction_history }],
            }))
            .send()
            .ok()?;
        if !response.status().is_success() {
            return None;
        }

        let body = response.json::<SignatureStatusesEnvelope>().ok()?;
        Some(body.result.value)
    }

    fn get_inflight_bundle_statuses(
        &self,
        endpoint: &str,
        bundle_ids: &[String],
    ) -> Option<Vec<JitoInflightBundleStatus>> {
        if bundle_ids.is_empty() || self.is_mock(endpoint) {
            return Some(Vec::new());
        }

        let response = self
            .http
            .post(endpoint)
            .json(&json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": 1,
                "method": "getInflightBundleStatuses",
                "params": [bundle_ids],
            }))
            .send()
            .ok()?;
        if !response.status().is_success() {
            return None;
        }

        let body = response.json::<JitoInflightBundleEnvelope>().ok()?;
        Some(body.result.map(|result| result.value).unwrap_or_default())
    }
}

#[derive(Debug)]
struct SignatureWsClient {
    endpoint: String,
    timeout: Duration,
    socket: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    next_request_id: u64,
    pending_requests: HashMap<u64, SubmissionId>,
    subscriptions: HashMap<u64, SubmissionId>,
    tracked_submissions: HashSet<SubmissionId>,
}

impl SignatureWsClient {
    fn new(endpoint: &str, timeout: Duration) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout,
            socket: None,
            next_request_id: 1,
            pending_requests: HashMap::new(),
            subscriptions: HashMap::new(),
            tracked_submissions: HashSet::new(),
        }
    }

    fn ensure_subscriptions(&mut self, records: &[ExecutionRecord]) {
        if self.endpoint.starts_with(MOCK_SCHEME) || records.is_empty() {
            return;
        }
        if self.ensure_connected().is_none() {
            return;
        }

        for record in records {
            if self.tracked_submissions.contains(&record.submission_id) {
                continue;
            }

            let request_id = self.next_request_id;
            self.next_request_id += 1;
            let payload = json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": request_id,
                "method": "signatureSubscribe",
                "params": [
                    record.chain_signature.clone(),
                    {
                        "commitment": "confirmed",
                        "enableReceivedNotification": true
                    }
                ]
            })
            .to_string();

            let Some(socket) = self.socket.as_mut() else {
                return;
            };
            if socket.send(Message::Text(payload.into())).is_ok() {
                self.pending_requests
                    .insert(request_id, record.submission_id.clone());
                self.tracked_submissions
                    .insert(record.submission_id.clone());
            } else {
                self.reset_connection();
                return;
            }
        }
    }

    fn drain_notifications(&mut self, tracker: &mut ExecutionTracker) -> Vec<ExecutionTransition> {
        if self.endpoint.starts_with(MOCK_SCHEME) || self.socket.is_none() {
            return Vec::new();
        }

        let mut transitions = Vec::new();
        for _ in 0..64 {
            let read_result = {
                let Some(socket) = self.socket.as_mut() else {
                    return transitions;
                };
                socket.read()
            };

            match read_result {
                Ok(message) => {
                    if let Some(transition) = self.handle_message(message, tracker) {
                        transitions.push(transition);
                    }
                }
                Err(WsError::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break;
                }
                Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => {
                    self.reset_connection();
                    break;
                }
                Err(_) => {
                    self.reset_connection();
                    break;
                }
            }
        }
        transitions
    }

    fn ensure_connected(&mut self) -> Option<()> {
        if self.socket.is_some() {
            return Some(());
        }

        let url = Url::parse(&self.endpoint).ok()?;
        let host = url.host_str()?.to_owned();
        let port = url.port_or_known_default()?;
        let stream = TcpStream::connect((host.as_str(), port)).ok()?;
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(self.timeout));
        let _ = stream.set_write_timeout(Some(self.timeout));
        let request = self.endpoint.as_str().into_client_request().ok()?;
        let (socket, _) = client_tls_with_config(request, stream, None, None).ok()?;
        self.socket = Some(socket);
        Some(())
    }

    fn handle_message(
        &mut self,
        message: Message,
        tracker: &mut ExecutionTracker,
    ) -> Option<ExecutionTransition> {
        let Message::Text(text) = message else {
            return None;
        };
        let Ok(payload) = serde_json::from_str::<WsEnvelope>(text.as_ref()) else {
            return None;
        };

        if let (Some(id), Some(subscription_id)) = (payload.id, payload.result.as_u64()) {
            if let Some(submission_id) = self.pending_requests.remove(&id) {
                self.subscriptions.insert(subscription_id, submission_id);
            }
            return None;
        }

        if payload.method.as_deref() != Some("signatureNotification") {
            return None;
        }

        let Some(params) = payload.params else {
            return None;
        };
        let Some(submission_id) = self.subscriptions.get(&params.subscription).cloned() else {
            return None;
        };

        match params.result.value {
            WsSignatureValue::ReceivedSignature(value) if value == "receivedSignature" => {
                tracker.transition(&submission_id, InclusionStatus::Pending)
            }
            WsSignatureValue::Processed { err: Some(_) } => {
                self.subscriptions.remove(&params.subscription);
                self.tracked_submissions.remove(&submission_id);
                tracker.transition(
                    &submission_id,
                    InclusionStatus::Failed(FailureClass::ChainExecutionFailed),
                )
            }
            WsSignatureValue::Processed { err: None } => {
                self.subscriptions.remove(&params.subscription);
                self.tracked_submissions.remove(&submission_id);
                tracker.transition(
                    &submission_id,
                    InclusionStatus::Landed {
                        slot: params.result.context.slot,
                    },
                )
            }
            WsSignatureValue::ReceivedSignature(_) => None,
        }
    }

    fn reset_connection(&mut self) {
        self.socket = None;
        self.pending_requests.clear();
        self.subscriptions.clear();
        self.tracked_submissions.clear();
    }
}

#[derive(Debug, Deserialize)]
struct SignatureStatusesEnvelope {
    result: SignatureStatusesResult,
}

#[derive(Debug, Deserialize)]
struct SignatureStatusesResult {
    value: Vec<Option<RpcSignatureStatus>>,
}

#[derive(Debug, Deserialize)]
struct RpcSignatureStatus {
    slot: u64,
    #[serde(default)]
    err: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct JitoInflightBundleEnvelope {
    result: Option<JitoInflightBundleResult>,
}

#[derive(Debug, Deserialize)]
struct JitoInflightBundleResult {
    value: Vec<JitoInflightBundleStatus>,
}

#[derive(Debug, Deserialize)]
struct JitoInflightBundleStatus {
    bundle_id: String,
    status: String,
    #[serde(rename = "landed_slot")]
    _landed_slot: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WsEnvelope {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<WsNotificationParams>,
}

#[derive(Debug, Deserialize)]
struct WsNotificationParams {
    result: WsNotificationResult,
    subscription: u64,
}

#[derive(Debug, Deserialize)]
struct WsNotificationResult {
    context: WsNotificationContext,
    value: WsSignatureValue,
}

#[derive(Debug, Deserialize)]
struct WsNotificationContext {
    slot: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WsSignatureValue {
    ReceivedSignature(String),
    Processed {
        #[serde(default)]
        err: Option<Value>,
    },
}

fn bundle_status_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if let Some(base) = trimmed.strip_suffix("/transactions") {
        base.to_owned()
    } else if let Some(base) = trimmed.strip_suffix("/bundles") {
        base.to_owned()
    } else if trimmed.ends_with("/api/v1") {
        trimmed.to_owned()
    } else if trimmed.starts_with(MOCK_SCHEME) {
        format!("{trimmed}/api/v1")
    } else if let Ok(url) = Url::parse(trimmed) {
        let mut normalized = url;
        normalized.set_path("/api/v1");
        normalized.set_query(None);
        normalized.to_string().trim_end_matches('/').to_owned()
    } else {
        format!("{trimmed}/api/v1")
    }
}
