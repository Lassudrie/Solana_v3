pub mod classifier;
pub mod history;
pub mod tracker;

pub use classifier::{FailureClass, OutcomeClassifier};
pub use history::ExecutionHistory;
pub use tracker::{ExecutionOutcome, ExecutionRecord, ExecutionTracker, InclusionStatus};

#[cfg(test)]
mod tests {
    use crate::tracker::InclusionStatus;
    use state::types::RouteId;
    use submit::{SubmissionId, SubmitResult, SubmitStatus};

    use super::{ExecutionOutcome, ExecutionTracker};

    #[test]
    fn tracker_records_state_transitions() {
        let mut tracker = ExecutionTracker::default();
        let record = tracker.register_submission(
            RouteId("route-a".into()),
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
}
