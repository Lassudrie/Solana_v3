pub mod classifier;
pub mod history;
pub mod onchain;
pub mod tracker;

pub use classifier::{FailureClass, OutcomeClassifier};
pub use history::ExecutionHistory;
pub use onchain::{OnChainReconciler, ReconciliationConfig as OnChainReconciliationConfig};
pub use tracker::{ExecutionOutcome, ExecutionRecord, ExecutionTracker, InclusionStatus};

#[cfg(test)]
mod tests {
    use crate::tracker::InclusionStatus;
    use state::types::RouteId;
    use submit::{SubmissionId, SubmitMode, SubmitResult, SubmitStatus};

    use super::{
        ExecutionOutcome, ExecutionTracker, OnChainReconciler, OnChainReconciliationConfig,
    };

    #[test]
    fn tracker_records_state_transitions() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
            10,
            SubmitMode::SingleTransaction,
            SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("submission-1".into()),
                endpoint: "jito".into(),
                rejection: None,
            },
        );

        let updated = tracker
            .transition(&record.submission_id, InclusionStatus::Landed { slot: 55 })
            .expect("transitioned");
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
}
