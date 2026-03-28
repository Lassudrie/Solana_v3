pub mod classifier;
pub mod history;
pub mod onchain;
pub mod tracker;

pub use classifier::{FailureClass, OutcomeClassifier};
pub use history::ExecutionHistory;
pub use onchain::{OnChainReconciler, ReconciliationConfig as OnChainReconciliationConfig};
pub use tracker::{
    ExecutionFailureDetail, ExecutionOutcome, ExecutionRecord, ExecutionTracker,
    ExecutionTransition, InclusionStatus,
};

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::Arc,
        thread,
        time::UNIX_EPOCH,
    };

    use crate::tracker::InclusionStatus;
    use domain::RouteId;
    use serde_json::{Value, json};
    use submit::{SubmissionId, SubmitMode, SubmitRejectionReason, SubmitResult, SubmitStatus};

    use super::{
        ExecutionOutcome, ExecutionTracker, OnChainReconciler, OnChainReconciliationConfig,
    };

    fn register_submission(
        tracker: &mut ExecutionTracker,
        route_id: &str,
        chain_signature: &str,
        quoted_slot: u64,
        submit_mode: SubmitMode,
        result: SubmitResult,
    ) -> super::ExecutionRecord {
        tracker.register_submission(
            RouteId(route_id.into()),
            chain_signature.into(),
            quoted_slot,
            Some(quoted_slot.saturating_add(1)),
            Some(quoted_slot.saturating_add(2)),
            UNIX_EPOCH,
            submit_mode,
            result,
        )
    }

    #[test]
    fn tracker_records_state_transitions() {
        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-a",
            "sig-1",
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "sig-1",
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "sig-1",
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "sig-1",
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
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
    fn onchain_reconciler_marks_bundle_landed_when_bundle_status_lands() {
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
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
            InclusionStatus::Landed { slot: 44 }
        );
        assert_eq!(updated.outcome, ExecutionOutcome::Included { slot: 44 });
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
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
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

    #[test]
    fn onchain_reconciler_classifies_too_little_output_with_failure_detail() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({
                    "result": {
                        "value": [{
                            "slot": 88,
                            "err": { "InstructionError": [2, { "Custom": 6022 }] }
                        }]
                    }
                })
                .to_string(),
                "getTransaction" => json!({
                    "result": {
                        "meta": {
                            "err": { "InstructionError": [2, { "Custom": 6022 }] },
                            "logMessages": [
                                "Program log: AnchorError thrown in programs/amm/src/instructions/swap_v2.rs:362. Error Code: TooLittleOutputReceived. Error Number: 6022. Error Message: Too little output received."
                            ]
                        },
                        "transaction": {
                            "message": {
                                "instructions": [
                                    { "programId": "ComputeBudget111111111111111111111111111111" },
                                    { "programId": "ComputeBudget111111111111111111111111111111" },
                                    { "programId": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK" }
                                ]
                            }
                        }
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
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
            InclusionStatus::Failed(super::FailureClass::ChainExecutionTooLittleOutput)
        );
        assert_eq!(
            updated.outcome,
            ExecutionOutcome::Failed(super::FailureClass::ChainExecutionTooLittleOutput)
        );
        let detail = updated.failure_detail.as_ref().expect("failure detail");
        assert_eq!(detail.instruction_index, Some(2));
        assert_eq!(
            detail.program_id.as_deref(),
            Some("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK")
        );
        assert_eq!(detail.custom_code, Some(6_022));
        assert_eq!(
            detail.error_name.as_deref(),
            Some("TooLittleOutputReceived")
        );
    }

    #[test]
    fn onchain_reconciler_classifies_amount_out_below_minimum_as_too_little_output() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({
                    "result": {
                        "value": [{
                            "slot": 88,
                            "err": { "InstructionError": [5, { "Custom": 6036 }] }
                        }]
                    }
                })
                .to_string(),
                "getTransaction" => json!({
                    "result": {
                        "meta": {
                            "err": { "InstructionError": [5, { "Custom": 6036 }] },
                            "logMessages": [
                                "Program log: AnchorError occurred. Error Code: AmountOutBelowMinimum. Error Number: 6036. Error Message: Amount out below minimum threshold."
                            ]
                        },
                        "transaction": {
                            "message": {
                                "instructions": [
                                    { "programId": "ComputeBudget111111111111111111111111111111" },
                                    { "programId": "ComputeBudget111111111111111111111111111111" },
                                    { "programId": "ATokenGPvbdGVxr1sWUhQjphn7x7ZsZfXg2f7Qp7i7m" },
                                    { "programId": "ATokenGPvbdGVxr1sWUhQjphn7x7ZsZfXg2f7Qp7i7m" },
                                    { "programId": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK" },
                                    { "programId": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" }
                                ]
                            }
                        }
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
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
            InclusionStatus::Failed(super::FailureClass::ChainExecutionTooLittleOutput)
        );
        assert_eq!(
            updated.outcome,
            ExecutionOutcome::Failed(super::FailureClass::ChainExecutionTooLittleOutput)
        );
        let detail = updated.failure_detail.as_ref().expect("failure detail");
        assert_eq!(detail.instruction_index, Some(5));
        assert_eq!(
            detail.program_id.as_deref(),
            Some("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc")
        );
        assert_eq!(detail.custom_code, Some(6_036));
        assert_eq!(detail.error_name.as_deref(), Some("AmountOutBelowMinimum"));
    }

    #[test]
    fn onchain_reconciler_computes_realized_pnl_from_token_balance_delta() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({
                    "result": {
                        "value": [{
                            "slot": 88,
                            "err": null
                        }]
                    }
                })
                .to_string(),
                "getTransaction" => json!({
                    "result": {
                        "meta": {
                            "err": null,
                            "preTokenBalances": [{
                                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                "owner": "wallet-owner",
                                "uiTokenAmount": { "amount": "1000" }
                            }],
                            "postTokenBalances": [{
                                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                "owner": "wallet-owner",
                                "uiTokenAmount": { "amount": "1125" }
                            }]
                        }
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-a",
            "chain-sig",
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: rpc_endpoint.clone(),
                rejection: None,
            },
        );
        assert!(tracker.set_profit_context(
            &record.submission_id,
            Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
            Some("wallet-owner".into()),
            Some(125),
            Some(25),
        ));
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
        assert_eq!(updated.outcome, ExecutionOutcome::Included { slot: 88 });
        assert_eq!(updated.realized_output_delta_quote_atoms, Some(125));
        assert_eq!(updated.realized_pnl_quote_atoms, Some(100));
    }

    #[test]
    fn onchain_reconciler_computes_realized_pnl_from_wsol_balance_delta() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({
                    "result": {
                        "value": [{
                            "slot": 91,
                            "err": null
                        }]
                    }
                })
                .to_string(),
                "getTransaction" => json!({
                    "result": {
                        "meta": {
                            "err": null,
                            "preTokenBalances": [{
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "wallet-owner",
                                "uiTokenAmount": { "amount": "1000000" }
                            }],
                            "postTokenBalances": [{
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "wallet-owner",
                                "uiTokenAmount": { "amount": "1000125" }
                            }]
                        }
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-sol",
            "chain-sig-sol",
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-sol".into()),
                endpoint: rpc_endpoint.clone(),
                rejection: None,
            },
        );
        assert!(tracker.set_profit_context(
            &record.submission_id,
            Some("So11111111111111111111111111111111111111112".into()),
            Some("wallet-owner".into()),
            Some(125),
            Some(25),
        ));
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
        assert_eq!(updated.outcome, ExecutionOutcome::Included { slot: 91 });
        assert_eq!(updated.realized_output_delta_quote_atoms, Some(125));
        assert_eq!(updated.realized_pnl_quote_atoms, Some(100));
    }

    #[test]
    fn onchain_reconciler_computes_realized_pnl_from_native_sol_lamport_delta() {
        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => json!({
                    "result": {
                        "value": [{
                            "slot": 92,
                            "err": null
                        }]
                    }
                })
                .to_string(),
                "getTransaction" => json!({
                    "result": {
                        "meta": {
                            "err": null,
                            "preTokenBalances": [],
                            "postTokenBalances": [],
                            "preBalances": [1000000, 1],
                            "postBalances": [1015000, 1]
                        },
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {
                                        "pubkey": "wallet-owner",
                                        "signer": true,
                                        "writable": true
                                    },
                                    {
                                        "pubkey": "other-account",
                                        "signer": false,
                                        "writable": false
                                    }
                                ]
                            }
                        }
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let mut tracker = ExecutionTracker::default();
        let record = register_submission(
            &mut tracker,
            "route-sol-native",
            "chain-sig-sol-native",
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-sol-native".into()),
                endpoint: rpc_endpoint.clone(),
                rejection: None,
            },
        );
        assert!(tracker.set_profit_context(
            &record.submission_id,
            Some("So11111111111111111111111111111111111111112".into()),
            Some("wallet-owner".into()),
            Some(125),
            Some(25),
        ));
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
        assert_eq!(updated.outcome, ExecutionOutcome::Included { slot: 92 });
        assert_eq!(updated.realized_output_delta_quote_atoms, None);
        assert_eq!(updated.realized_pnl_quote_atoms, Some(15_000));
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
