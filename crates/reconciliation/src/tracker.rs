use std::time::SystemTime;

use state::types::RouteId;
use submit::{SubmissionId, SubmitResult, SubmitStatus};

use crate::{classifier::FailureClass, history::ExecutionHistory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InclusionStatus {
    Pending,
    Submitted,
    Landed { slot: u64 },
    Dropped,
    Failed(FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Pending,
    Included { slot: u64 },
    Failed(FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRecord {
    pub route_id: RouteId,
    pub submission_id: SubmissionId,
    pub submit_status: SubmitStatus,
    pub inclusion_status: InclusionStatus,
    pub outcome: ExecutionOutcome,
    pub created_at: SystemTime,
    pub last_updated_at: SystemTime,
}

#[derive(Debug, Default)]
pub struct ExecutionTracker {
    history: ExecutionHistory,
}

impl ExecutionTracker {
    pub fn register_submission(
        &mut self,
        route_id: RouteId,
        result: SubmitResult,
    ) -> ExecutionRecord {
        let now = SystemTime::now();
        let record = ExecutionRecord {
            route_id,
            submission_id: result.submission_id.clone(),
            submit_status: result.status,
            inclusion_status: InclusionStatus::Submitted,
            outcome: ExecutionOutcome::Pending,
            created_at: now,
            last_updated_at: now,
        };
        self.history.insert(record.clone());
        record
    }

    pub fn transition(
        &mut self,
        submission_id: &SubmissionId,
        status: InclusionStatus,
    ) -> Option<&ExecutionRecord> {
        let record = self.history.get_mut(submission_id)?;
        record.last_updated_at = SystemTime::now();
        record.inclusion_status = status.clone();
        record.outcome = match status {
            InclusionStatus::Landed { slot } => ExecutionOutcome::Included { slot },
            InclusionStatus::Dropped => ExecutionOutcome::Failed(FailureClass::ChainDropped),
            InclusionStatus::Failed(class) => ExecutionOutcome::Failed(class),
            InclusionStatus::Pending | InclusionStatus::Submitted => ExecutionOutcome::Pending,
        };
        self.history.get(submission_id)
    }

    pub fn get(&self, submission_id: &SubmissionId) -> Option<&ExecutionRecord> {
        self.history.get(submission_id)
    }

    pub fn pending_count(&self) -> usize {
        self.history
            .values()
            .filter(|record| record.outcome == ExecutionOutcome::Pending)
            .count()
    }
}
