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
    tracker::{
        ExecutionFailureDetail, ExecutionOutcome, ExecutionRecord, ExecutionTracker,
        ExecutionTransition, InclusionStatus,
    },
};

const MOCK_SCHEME: &str = "mock://";
const JSON_RPC_VERSION: &str = "2.0";
const SIGNATURE_STATUS_BATCH_LIMIT: usize = 256;
const BUNDLE_STATUS_BATCH_LIMIT: usize = 5;
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TransactionObservation {
    failure_detail: Option<ExecutionFailureDetail>,
    realized_output_delta_quote_atoms: Option<i64>,
    realized_pnl_quote_atoms: Option<i64>,
}

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
            let ws_transitions = self.ws.drain_notifications(tracker);
            self.hydrate_terminal_transitions(tracker, &ws_transitions);
            transitions.extend(ws_transitions);
        }

        pending = tracker.pending_records();
        if pending.is_empty() {
            return transitions;
        }

        let status_transitions = self.poll_transaction_statuses(tracker, &pending);
        self.hydrate_terminal_transitions(tracker, &status_transitions);
        transitions.extend(status_transitions);
        let bundle_transitions = self.poll_bundle_statuses(tracker, &pending);
        self.hydrate_terminal_transitions(tracker, &bundle_transitions);
        transitions.extend(bundle_transitions);
        transitions
    }

    fn expire_stale_submissions(
        &self,
        tracker: &mut ExecutionTracker,
        observed_slot: u64,
    ) -> Vec<ExecutionTransition> {
        let mut transitions = Vec::new();
        for record in tracker.pending_records() {
            let Some(reference_slot) = record.expiry_reference_slot() else {
                continue;
            };
            if observed_slot.saturating_sub(reference_slot) > self.config.max_pending_slots {
                if let Some(transition) = tracker.transition(
                    &record.submission_id,
                    InclusionStatus::Expired { observed_slot },
                ) {
                    let _ = tracker.set_terminal_slot(&record.submission_id, observed_slot);
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
                        let (failure_class, failure_detail) =
                            self.classify_chain_failure(&record.chain_signature);
                        if let Some(detail) = failure_detail {
                            tracker.set_failure_detail(&record.submission_id, detail);
                        }
                        if let Some(transition) = tracker.transition(
                            &record.submission_id,
                            InclusionStatus::Failed(failure_class),
                        ) {
                            let _ = tracker.set_terminal_slot(&record.submission_id, status.slot);
                            transitions.push(transition);
                        }
                    }
                    Some(status) => {
                        if let Some(transition) = tracker.transition(
                            &record.submission_id,
                            InclusionStatus::Landed { slot: status.slot },
                        ) {
                            let _ = tracker.set_terminal_slot(&record.submission_id, status.slot);
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
                        "Landed" => {
                            let landed_slot = status.landed_slot.unwrap_or(record.slot_fallback());
                            if let Some(transition) = tracker.transition(
                                &record.submission_id,
                                InclusionStatus::Landed { slot: landed_slot },
                            ) {
                                let _ =
                                    tracker.set_terminal_slot(&record.submission_id, landed_slot);
                                transitions.push(transition);
                            }
                        }
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

    fn classify_chain_failure(
        &self,
        chain_signature: &str,
    ) -> (FailureClass, Option<ExecutionFailureDetail>) {
        let detail = self.rpc.get_transaction_failure_detail(chain_signature);
        let failure_class = detail
            .as_ref()
            .map_or(FailureClass::ChainExecutionFailed, |detail| {
                failure_class_for_error(detail.error_name.as_deref(), detail.custom_code)
            });
        (failure_class, detail)
    }

    fn hydrate_terminal_transitions(
        &self,
        tracker: &mut ExecutionTracker,
        transitions: &[ExecutionTransition],
    ) {
        for transition in transitions {
            let Some(record) = tracker.get(&transition.submission_id).cloned() else {
                continue;
            };
            self.hydrate_terminal_record(tracker, &record);
        }
    }

    fn hydrate_terminal_record(&self, tracker: &mut ExecutionTracker, record: &ExecutionRecord) {
        if record.realized_pnl_quote_atoms.is_some() {
            return;
        }

        match &record.outcome {
            ExecutionOutcome::Pending => {}
            ExecutionOutcome::Included { .. } => {
                let observation = self.rpc.get_transaction_observation(
                    &record.chain_signature,
                    record.profit_mint.as_deref(),
                    record.wallet_owner_pubkey.as_deref(),
                );
                if let Some(detail) = observation.failure_detail.clone()
                    && record.failure_detail.is_none()
                {
                    let _ = tracker.set_failure_detail(&record.submission_id, detail);
                }
                if let Some(realized_pnl) = observation.realized_pnl_quote_atoms {
                    let _ = tracker.set_realized_pnl(
                        &record.submission_id,
                        observation.realized_output_delta_quote_atoms,
                        Some(realized_pnl),
                    );
                } else if let Some(delta) = observation.realized_output_delta_quote_atoms {
                    let realized_pnl = subtract_execution_cost(
                        delta,
                        record
                            .estimated_execution_cost_quote_atoms
                            .unwrap_or_default(),
                    );
                    let _ = tracker.set_realized_pnl(
                        &record.submission_id,
                        Some(delta),
                        Some(realized_pnl),
                    );
                }
            }
            ExecutionOutcome::Failed(class) => match class {
                FailureClass::ChainExecutionFailed
                | FailureClass::ChainExecutionTooLittleOutput
                | FailureClass::ChainExecutionAmountInAboveMaximum
                | FailureClass::Unknown => {
                    let observation = self.rpc.get_transaction_observation(
                        &record.chain_signature,
                        record.profit_mint.as_deref(),
                        record.wallet_owner_pubkey.as_deref(),
                    );
                    if let Some(detail) = observation.failure_detail.clone()
                        && record.failure_detail.is_none()
                    {
                        let _ = tracker.set_failure_detail(&record.submission_id, detail);
                    }
                    if let Some(realized_pnl) = observation.realized_pnl_quote_atoms {
                        let _ = tracker.set_realized_pnl(
                            &record.submission_id,
                            observation.realized_output_delta_quote_atoms,
                            Some(realized_pnl),
                        );
                    } else {
                        let delta = observation.realized_output_delta_quote_atoms.or(Some(0));
                        let realized_pnl = delta.map(|delta| {
                            subtract_execution_cost(
                                delta,
                                record
                                    .estimated_execution_cost_quote_atoms
                                    .unwrap_or_default(),
                            )
                        });
                        let _ =
                            tracker.set_realized_pnl(&record.submission_id, delta, realized_pnl);
                    }
                }
                FailureClass::SubmitRejected
                | FailureClass::TransportFailed
                | FailureClass::ChainDropped
                | FailureClass::Expired => {
                    let _ = tracker.set_realized_pnl(&record.submission_id, Some(0), Some(0));
                }
            },
        }
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

    fn get_transaction_failure_detail(&self, signature: &str) -> Option<ExecutionFailureDetail> {
        self.get_transaction_observation(signature, None, None)
            .failure_detail
    }

    fn get_transaction_observation(
        &self,
        signature: &str,
        profit_mint: Option<&str>,
        wallet_owner_pubkey: Option<&str>,
    ) -> TransactionObservation {
        if signature.is_empty() || self.is_mock(&self.endpoint) {
            return TransactionObservation::default();
        }

        let response = self
            .http
            .post(&self.endpoint)
            .json(&json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                        "commitment": "confirmed"
                    }
                ],
            }))
            .send()
            .ok();
        let Some(response) = response else {
            return TransactionObservation::default();
        };
        if !response.status().is_success() {
            return TransactionObservation::default();
        }

        let Ok(body) = response.json::<Value>() else {
            return TransactionObservation::default();
        };
        let realized_quote = profit_mint
            .zip(wallet_owner_pubkey)
            .map(|(profit_mint, wallet_owner_pubkey)| {
                parse_realized_quote_observation(&body, profit_mint, wallet_owner_pubkey)
            })
            .unwrap_or_default();
        TransactionObservation {
            failure_detail: parse_transaction_failure_detail(&body),
            realized_output_delta_quote_atoms: realized_quote.realized_output_delta_quote_atoms,
            realized_pnl_quote_atoms: realized_quote.realized_pnl_quote_atoms,
        }
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
            WsSignatureValue::Processed { err: Some(err) } => {
                self.subscriptions.remove(&params.subscription);
                self.tracked_submissions.remove(&submission_id);
                let (failure_class, failure_detail) = classify_ws_failure(&err);
                if let Some(detail) = failure_detail {
                    tracker.set_failure_detail(&submission_id, detail);
                }
                let transition =
                    tracker.transition(&submission_id, InclusionStatus::Failed(failure_class));
                let _ = tracker.set_terminal_slot(&submission_id, params.result.context.slot);
                transition
            }
            WsSignatureValue::Processed { err: None } => {
                self.subscriptions.remove(&params.subscription);
                self.tracked_submissions.remove(&submission_id);
                let transition = tracker.transition(
                    &submission_id,
                    InclusionStatus::Landed {
                        slot: params.result.context.slot,
                    },
                );
                let _ = tracker.set_terminal_slot(&submission_id, params.result.context.slot);
                transition
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
    landed_slot: Option<u64>,
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

fn classify_ws_failure(err: &Value) -> (FailureClass, Option<ExecutionFailureDetail>) {
    let custom_code = instruction_error_custom_code(err);
    let error_name = error_name_for_custom_code(custom_code);
    let failure_class = failure_class_for_error(error_name.as_deref(), custom_code);
    let detail = ExecutionFailureDetail {
        instruction_index: instruction_error_index(err),
        program_id: None,
        custom_code,
        error_name,
    };
    let has_detail = detail.instruction_index.is_some()
        || detail.custom_code.is_some()
        || detail.error_name.is_some();
    (failure_class, has_detail.then_some(detail))
}

fn parse_transaction_failure_detail(body: &Value) -> Option<ExecutionFailureDetail> {
    let result = body.get("result")?;
    let meta = result.get("meta")?;
    let err = meta.get("err")?;
    let instruction_index = instruction_error_index(err);
    let mut custom_code = instruction_error_custom_code(err);
    let log_messages = meta
        .get("logMessages")
        .and_then(Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut error_name = parse_error_name(&log_messages);
    if custom_code.is_none() {
        custom_code = parse_custom_code_from_logs(&log_messages);
    }
    if error_name.is_none() {
        error_name = error_name_for_custom_code(custom_code);
    }
    let detail = ExecutionFailureDetail {
        instruction_index,
        program_id: instruction_index.and_then(|index| instruction_program_id(result, index)),
        custom_code,
        error_name,
    };

    (detail.instruction_index.is_some()
        || detail.program_id.is_some()
        || detail.custom_code.is_some()
        || detail.error_name.is_some())
    .then_some(detail)
}

fn parse_realized_output_delta_quote_atoms(
    body: &Value,
    profit_mint: &str,
    wallet_owner_pubkey: &str,
) -> Option<i64> {
    if profit_mint.is_empty() || wallet_owner_pubkey.is_empty() {
        return None;
    }

    let meta = body.get("result")?.get("meta")?;
    let (pre_total, pre_found) = sum_owner_token_balance(
        meta.get("preTokenBalances"),
        profit_mint,
        wallet_owner_pubkey,
    );
    let (post_total, post_found) = sum_owner_token_balance(
        meta.get("postTokenBalances"),
        profit_mint,
        wallet_owner_pubkey,
    );
    if !pre_found && !post_found {
        return None;
    }

    clamp_i128_to_i64(i128::from(post_total) - i128::from(pre_total))
}

fn parse_realized_quote_observation(
    body: &Value,
    profit_mint: &str,
    wallet_owner_pubkey: &str,
) -> TransactionObservation {
    let realized_output_delta_quote_atoms =
        parse_realized_output_delta_quote_atoms(body, profit_mint, wallet_owner_pubkey);
    let realized_pnl_quote_atoms =
        if realized_output_delta_quote_atoms.is_none() && profit_mint == SOL_MINT {
            parse_owner_lamport_delta(body, wallet_owner_pubkey)
        } else {
            None
        };

    TransactionObservation {
        realized_output_delta_quote_atoms,
        realized_pnl_quote_atoms,
        ..TransactionObservation::default()
    }
}

fn sum_owner_token_balance(
    balances: Option<&Value>,
    profit_mint: &str,
    wallet_owner_pubkey: &str,
) -> (u64, bool) {
    let mut total = 0u128;
    let mut found = false;

    for entry in balances
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|entry| entry.get("mint").and_then(Value::as_str) == Some(profit_mint))
        .filter(|entry| entry.get("owner").and_then(Value::as_str) == Some(wallet_owner_pubkey))
    {
        let Some(amount) = entry
            .get("uiTokenAmount")
            .and_then(|amount| amount.get("amount"))
            .and_then(Value::as_str)
            .and_then(|amount| amount.parse::<u128>().ok())
        else {
            continue;
        };
        found = true;
        total = total.saturating_add(amount);
    }

    (
        total
            .min(u128::from(u64::MAX))
            .try_into()
            .unwrap_or(u64::MAX),
        found,
    )
}

fn parse_owner_lamport_delta(body: &Value, wallet_owner_pubkey: &str) -> Option<i64> {
    let account_keys = body
        .get("result")?
        .get("transaction")?
        .get("message")?
        .get("accountKeys")?
        .as_array()?;
    let owner_index = account_keys.iter().position(|entry| {
        account_key_pubkey(entry)
            .map(|pubkey| pubkey == wallet_owner_pubkey)
            .unwrap_or(false)
    })?;
    let meta = body.get("result")?.get("meta")?;
    let pre_balance = meta
        .get("preBalances")?
        .as_array()?
        .get(owner_index)?
        .as_u64()?;
    let post_balance = meta
        .get("postBalances")?
        .as_array()?
        .get(owner_index)?
        .as_u64()?;
    clamp_i128_to_i64(i128::from(post_balance) - i128::from(pre_balance))
}

fn account_key_pubkey(entry: &Value) -> Option<&str> {
    entry
        .as_str()
        .or_else(|| entry.get("pubkey").and_then(Value::as_str))
}

fn subtract_execution_cost(
    output_delta_quote_atoms: i64,
    estimated_execution_cost_quote_atoms: u64,
) -> i64 {
    clamp_i128_to_i64(
        i128::from(output_delta_quote_atoms)
            .saturating_sub(i128::from(estimated_execution_cost_quote_atoms)),
    )
    .unwrap_or(if output_delta_quote_atoms.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn clamp_i128_to_i64(value: i128) -> Option<i64> {
    i64::try_from(value)
        .ok()
        .or_else(|| Some(value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64))
}

fn instruction_error_index(err: &Value) -> Option<u8> {
    err.get("InstructionError")?
        .as_array()?
        .first()?
        .as_u64()
        .and_then(|index| u8::try_from(index).ok())
}

fn instruction_error_custom_code(err: &Value) -> Option<u32> {
    err.get("InstructionError")?
        .as_array()?
        .get(1)?
        .get("Custom")?
        .as_u64()
        .and_then(|code| u32::try_from(code).ok())
}

fn error_name_for_custom_code(custom_code: Option<u32>) -> Option<String> {
    match custom_code {
        Some(6_022) => Some("TooLittleOutputReceived".to_string()),
        Some(6_036) => Some("AmountOutBelowMinimum".to_string()),
        Some(6_037) => Some("AmountInAboveMaximum".to_string()),
        _ => None,
    }
}

fn failure_class_for_error(error_name: Option<&str>, custom_code: Option<u32>) -> FailureClass {
    if matches!(
        error_name,
        Some("TooLittleOutputReceived" | "AmountOutBelowMinimum")
    ) || matches!(custom_code, Some(6_022 | 6_036))
    {
        FailureClass::ChainExecutionTooLittleOutput
    } else if error_name == Some("AmountInAboveMaximum") || custom_code == Some(6_037) {
        FailureClass::ChainExecutionAmountInAboveMaximum
    } else {
        FailureClass::ChainExecutionFailed
    }
}

fn parse_error_name(log_messages: &[String]) -> Option<String> {
    for message in log_messages {
        let lower_message = message.to_ascii_lowercase();
        if message.contains("TooLittleOutputReceived")
            || lower_message.contains("too little output received")
        {
            return Some("TooLittleOutputReceived".into());
        }
        if message.contains("AmountOutBelowMinimum")
            || lower_message.contains("amount out below minimum")
        {
            return Some("AmountOutBelowMinimum".into());
        }
        if message.contains("AmountInAboveMaximum")
            || lower_message.contains("amount in above maximum")
        {
            return Some("AmountInAboveMaximum".into());
        }
    }
    None
}

fn parse_custom_code_from_logs(log_messages: &[String]) -> Option<u32> {
    for message in log_messages {
        if let Some(number) = message.split("Error Number: ").nth(1) {
            let digits = number
                .chars()
                .take_while(|character| character.is_ascii_digit())
                .collect::<String>();
            if let Ok(code) = digits.parse::<u32>() {
                return Some(code);
            }
        }
        if let Some(hex_value) = message.split("custom program error: 0x").nth(1) {
            let hex = hex_value
                .chars()
                .take_while(|character| character.is_ascii_hexdigit())
                .collect::<String>();
            if let Ok(code) = u32::from_str_radix(&hex, 16) {
                return Some(code);
            }
        }
    }
    None
}

fn instruction_program_id(result: &Value, instruction_index: u8) -> Option<String> {
    let message = result.get("transaction")?.get("message")?;
    let instruction = message
        .get("instructions")?
        .as_array()?
        .get(instruction_index as usize)?;
    if let Some(program_id) = instruction.get("programId").and_then(Value::as_str) {
        return Some(program_id.into());
    }

    let program_index = instruction.get("programIdIndex")?.as_u64()? as usize;
    let account_key = message.get("accountKeys")?.as_array()?.get(program_index)?;
    account_key
        .get("pubkey")
        .and_then(Value::as_str)
        .or_else(|| account_key.as_str())
        .map(str::to_string)
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

#[cfg(test)]
mod tests {
    use super::SignatureWsClient;
    use crate::{
        classifier::FailureClass,
        tracker::{ExecutionTracker, InclusionStatus},
    };
    use domain::RouteId;
    use serde_json::json;
    use std::time::{Duration, UNIX_EPOCH};
    use submit::{SubmissionId, SubmitMode, SubmitResult, SubmitStatus};
    use tungstenite::Message;

    #[test]
    fn websocket_processed_error_is_terminal_and_captures_failure_detail() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            Some(11),
            Some(12),
            UNIX_EPOCH,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "mock://jito".into(),
                rejection: None,
            },
        );
        let mut ws = SignatureWsClient::new("mock://solana-ws", Duration::from_millis(5));
        ws.subscriptions.insert(7, record.submission_id.clone());
        ws.tracked_submissions.insert(record.submission_id.clone());

        let transition = ws
            .handle_message(
                Message::Text(
                    json!({
                        "method": "signatureNotification",
                        "params": {
                            "subscription": 7,
                            "result": {
                                "context": { "slot": 88 },
                                "value": {
                                    "err": { "InstructionError": [2, { "Custom": 6022 }] }
                                }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ),
                &mut tracker,
            )
            .expect("processed error should transition immediately");

        assert_eq!(
            transition.current_inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionTooLittleOutput)
        );
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionTooLittleOutput)
        );
        let detail = updated.failure_detail.as_ref().expect("failure detail");
        assert_eq!(detail.instruction_index, Some(2));
        assert_eq!(detail.custom_code, Some(6_022));
        assert_eq!(
            detail.error_name.as_deref(),
            Some("TooLittleOutputReceived")
        );
    }

    #[test]
    fn websocket_processed_amount_out_below_minimum_maps_to_too_little_output() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            Some(11),
            Some(12),
            UNIX_EPOCH,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "mock://jito".into(),
                rejection: None,
            },
        );
        let mut ws = SignatureWsClient::new("mock://solana-ws", Duration::from_millis(5));
        ws.subscriptions.insert(7, record.submission_id.clone());
        ws.tracked_submissions.insert(record.submission_id.clone());

        let transition = ws
            .handle_message(
                Message::Text(
                    json!({
                        "method": "signatureNotification",
                        "params": {
                            "subscription": 7,
                            "result": {
                                "context": { "slot": 88 },
                                "value": {
                                    "err": { "InstructionError": [5, { "Custom": 6036 }] }
                                }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ),
                &mut tracker,
            )
            .expect("processed error should transition immediately");

        assert_eq!(
            transition.current_inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionTooLittleOutput)
        );
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionTooLittleOutput)
        );
        let detail = updated.failure_detail.as_ref().expect("failure detail");
        assert_eq!(detail.instruction_index, Some(5));
        assert_eq!(detail.custom_code, Some(6_036));
        assert_eq!(detail.error_name.as_deref(), Some("AmountOutBelowMinimum"));
    }

    #[test]
    fn websocket_processed_amount_in_above_maximum_is_terminal_and_captures_failure_detail() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            Some(11),
            Some(12),
            UNIX_EPOCH,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "mock://jito".into(),
                rejection: None,
            },
        );
        let mut ws = SignatureWsClient::new("mock://solana-ws", Duration::from_millis(5));
        ws.subscriptions.insert(7, record.submission_id.clone());
        ws.tracked_submissions.insert(record.submission_id.clone());

        let transition = ws
            .handle_message(
                Message::Text(
                    json!({
                        "method": "signatureNotification",
                        "params": {
                            "subscription": 7,
                            "result": {
                                "context": { "slot": 88 },
                                "value": {
                                    "err": { "InstructionError": [4, { "Custom": 6037 }] }
                                }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ),
                &mut tracker,
            )
            .expect("processed error should transition immediately");

        assert_eq!(
            transition.current_inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionAmountInAboveMaximum)
        );
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Failed(FailureClass::ChainExecutionAmountInAboveMaximum)
        );
        let detail = updated.failure_detail.as_ref().expect("failure detail");
        assert_eq!(detail.instruction_index, Some(4));
        assert_eq!(detail.custom_code, Some(6_037));
        assert_eq!(detail.error_name.as_deref(), Some("AmountInAboveMaximum"));
    }
}
