pub mod classifier;
pub mod history;
pub mod onchain;
pub mod tracker;

pub use classifier::{FailureClass, OutcomeClassifier};
pub use history::ExecutionHistory;
pub use onchain::{OnChainReconciler, ReconciliationConfig as OnChainReconciliationConfig};
pub use tracker::{
    ExecutionOutcome, ExecutionRecord, ExecutionTracker, ExecutionTransition, InclusionStatus,
};

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::Arc,
        thread,
    };

    use crate::tracker::InclusionStatus;
    use serde_json::{Value, json};
    use state::types::RouteId;
    use submit::{SubmissionId, SubmitMode, SubmitRejectionReason, SubmitResult, SubmitStatus};

    use super::{
        ExecutionOutcome, ExecutionTracker, OnChainReconciler, OnChainReconciliationConfig,
    };

    #[test]
    fn tracker_records_state_transitions() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "sig-1".into(),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "jito".into(),
                rejection: None,
            },
        );

        let transition = tracker
            .transition(&record.submission_id, InclusionStatus::Landed { slot: 55 })
            .expect("transitioned");
        assert_eq!(
            transition.current_inclusion_status,
            InclusionStatus::Landed { slot: 55 }
        );
        assert_eq!(
            transition.current_outcome,
            ExecutionOutcome::Included { slot: 55 }
        );
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Landed { slot: 55 }
        );
        assert_eq!(updated.outcome, ExecutionOutcome::Included { slot: 55 });
    }

    #[test]
    fn onchain_reconciler_expires_old_pending_records() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "sig-1".into(),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "mock://solana-rpc".into(),
                rejection: None,
            },
        );
        let mut reconciler = OnChainReconciler::new(OnChainReconciliationConfig {
            enabled: true,
            rpc_http_endpoint: "mock://solana-rpc".into(),
            rpc_ws_endpoint: "mock://solana-ws".into(),
            websocket_enabled: true,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            max_pending_slots: 5,
        });

        reconciler.tick(&mut tracker, 20);

        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Expired { observed_slot: 20 }
        );
        assert_eq!(
            updated.outcome,
            ExecutionOutcome::Failed(super::FailureClass::Expired)
        );
    }

    #[test]
    fn tracker_marks_submit_rejections_as_terminal() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "sig-1".into(),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Rejected,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "jito".into(),
                rejection: Some(SubmitRejectionReason::RemoteRejected),
            },
        );

        assert_eq!(
            record.inclusion_status,
            InclusionStatus::Failed(super::FailureClass::SubmitRejected)
        );
        assert_eq!(
            record.outcome,
            ExecutionOutcome::Failed(super::FailureClass::SubmitRejected)
        );
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn tracker_ignores_duplicate_and_regressive_transitions() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "sig-1".into(),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "jito".into(),
                rejection: None,
            },
        );

        let pending = tracker
            .transition(&record.submission_id, InclusionStatus::Pending)
            .expect("submitted should advance to pending");
        assert_eq!(pending.current_inclusion_status, InclusionStatus::Pending);
        assert!(
            tracker
                .transition(&record.submission_id, InclusionStatus::Pending)
                .is_none()
        );

        let landed = tracker
            .transition(&record.submission_id, InclusionStatus::Landed { slot: 55 })
            .expect("pending should land");
        assert_eq!(
            landed.current_outcome,
            ExecutionOutcome::Included { slot: 55 }
        );
        assert!(
            tracker
                .transition(&record.submission_id, InclusionStatus::Pending)
                .is_none()
        );
        assert!(
            tracker
                .transition(
                    &record.submission_id,
                    InclusionStatus::Expired { observed_slot: 99 }
                )
                .is_none()
        );
    }

    #[test]
    fn onchain_reconciler_uses_chain_signature_for_status_queries() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => {
                    let signatures = payload["params"][0].as_array().expect("signature batch");
                    let uses_chain_signature = signatures
                        .iter()
                        .any(|entry| entry.as_str() == Some("chain-sig"));
                    let leaks_transport_id = signatures
                        .iter()
                        .any(|entry| entry.as_str() == Some("transport-id"));
                    if uses_chain_signature && !leaks_transport_id {
                        json!({ "result": { "value": [{ "slot": 88, "err": null }] } }).to_string()
                    } else {
                        json!({ "result": { "value": [null] } }).to_string()
                    }
                }
                _ => json!({ "result": { "value": [] } }).to_string(),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("transport-id".into()),
                endpoint: rpc_endpoint.clone(),
                rejection: None,
            },
        );
        let mut reconciler = OnChainReconciler::new(OnChainReconciliationConfig {
            enabled: true,
            rpc_http_endpoint: rpc_endpoint,
            rpc_ws_endpoint: "mock://solana-ws".into(),
            websocket_enabled: false,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            max_pending_slots: 5,
        });

        let transitions = reconciler.tick(&mut tracker, 10);

        assert_eq!(transitions.len(), 1);
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Landed { slot: 88 }
        );
    }

    #[test]
    fn onchain_reconciler_keeps_bundle_pending_when_only_bundle_lands() {
        let base_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({ "result": { "value": [null] } }).to_string(),
                "getInflightBundleStatuses" => json!({
                    "result": {
                        "value": [{
                            "bundle_id": "bundle-1",
                            "status": "Landed",
                            "landed_slot": 44
                        }]
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            SubmitMode::Bundle,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("bundle-1".into()),
                endpoint: format!("{base_endpoint}/api/v1/bundles"),
                rejection: None,
            },
        );
        let mut reconciler = OnChainReconciler::new(OnChainReconciliationConfig {
            enabled: true,
            rpc_http_endpoint: base_endpoint,
            rpc_ws_endpoint: "mock://solana-ws".into(),
            websocket_enabled: false,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            max_pending_slots: 5,
        });

        let transitions = reconciler.tick(&mut tracker, 10);

        assert!(transitions.is_empty());
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(updated.inclusion_status, InclusionStatus::Submitted);
        assert_eq!(updated.outcome, ExecutionOutcome::Pending);
    }

    #[test]
    fn onchain_reconciler_marks_bundle_transport_failures() {
        let base_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({ "result": { "value": [null] } }).to_string(),
                "getInflightBundleStatuses" => json!({
                    "result": {
                        "value": [{
                            "bundle_id": "bundle-1",
                            "status": "Failed",
                            "landed_slot": null
                        }]
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            "chain-sig".into(),
            10,
            SubmitMode::Bundle,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("bundle-1".into()),
                endpoint: format!("{base_endpoint}/api/v1/bundles"),
                rejection: None,
            },
        );
        let mut reconciler = OnChainReconciler::new(OnChainReconciliationConfig {
            enabled: true,
            rpc_http_endpoint: base_endpoint,
            rpc_ws_endpoint: "mock://solana-ws".into(),
            websocket_enabled: false,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            max_pending_slots: 5,
        });

        let transitions = reconciler.tick(&mut tracker, 10);

        assert_eq!(transitions.len(), 1);
        let updated = tracker.get(&record.submission_id).expect("record kept");
        assert_eq!(
            updated.inclusion_status,
            InclusionStatus::Failed(super::FailureClass::TransportFailed)
        );
        assert_eq!(
            updated.outcome,
            ExecutionOutcome::Failed(super::FailureClass::TransportFailed)
        );
    }

    fn spawn_mock_rpc_server<F>(handler: F) -> String
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock rpc server");
        let address = listener.local_addr().expect("mock rpc address");
        let handler = Arc::new(handler);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    continue;
                };
                let body = read_http_body(&mut stream);
                let response_body = (handler.as_ref())(&body);
                write_http_response(&mut stream, &response_body);
            }
        });

        format!("http://{address}")
    }

    fn read_http_body(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        let mut header_end = None;
        let mut content_length = 0usize;

        loop {
            let bytes_read = stream.read(&mut buffer).expect("read mock request");
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);

            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':').and_then(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                        })
                        .unwrap_or(0);
                }
            }

            if let Some(end) = header_end {
                if request.len() >= end + content_length {
                    let body = &request[end..end + content_length];
                    return String::from_utf8_lossy(body).into_owned();
                }
            }
        }

        String::new()
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
    }
}
