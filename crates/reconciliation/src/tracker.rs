use std::time::SystemTime;

use domain::RouteId;
use submit::{SubmissionId, SubmitMode, SubmitResult, SubmitStatus};

use crate::{classifier::FailureClass, history::ExecutionHistory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InclusionStatus {
    Pending,
    Submitted,
    Landed { slot: u64 },
    Dropped,
    Expired { observed_slot: u64 },
    Failed(FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Pending,
    Included { slot: u64 },
    Failed(FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFailureDetail {
    pub instruction_index: Option<u8>,
    pub program_id: Option<String>,
    pub custom_code: Option<u32>,
    pub error_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTransition {
    pub submission_id: SubmissionId,
    pub previous_inclusion_status: InclusionStatus,
    pub current_inclusion_status: InclusionStatus,
    pub previous_outcome: ExecutionOutcome,
    pub current_outcome: ExecutionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRecord {
    pub route_id: RouteId,
    pub submission_id: SubmissionId,
    pub chain_signature: String,
    pub submit_mode: SubmitMode,
    pub submit_endpoint: String,
    pub submit_status: SubmitStatus,
    pub quoted_slot: u64,
    pub blockhash_slot: Option<u64>,
    pub submitted_slot: Option<u64>,
    pub inclusion_status: InclusionStatus,
    pub outcome: ExecutionOutcome,
    pub profit_mint: Option<String>,
    pub wallet_owner_pubkey: Option<String>,
    pub expected_pnl_quote_atoms: Option<i64>,
    pub estimated_execution_cost_quote_atoms: Option<u64>,
    pub realized_output_delta_quote_atoms: Option<i64>,
    pub realized_pnl_quote_atoms: Option<i64>,
    pub failure_detail: Option<ExecutionFailureDetail>,
    pub submitted_at: SystemTime,
    pub last_updated_at: SystemTime,
}

impl ExecutionRecord {
    pub fn expiry_reference_slot(&self) -> Option<u64> {
        self.submitted_slot.or(self.blockhash_slot)
    }

    pub fn slot_fallback(&self) -> u64 {
        self.submitted_slot
            .or(self.blockhash_slot)
            .unwrap_or(self.quoted_slot)
    }
}

#[derive(Debug, Default)]
pub struct ExecutionTracker {
    history: ExecutionHistory,
}

impl ExecutionTracker {
    pub fn upsert_record(&mut self, record: ExecutionRecord) {
        self.history.insert(record);
    }

    pub fn register_submission(
        &mut self,
        route_id: RouteId,
        chain_signature: String,
        quoted_slot: u64,
        blockhash_slot: Option<u64>,
        submitted_slot: Option<u64>,
        submitted_at: SystemTime,
        submit_mode: SubmitMode,
        result: SubmitResult,
    ) -> ExecutionRecord {
        let (inclusion_status, outcome) = match result.status {
            SubmitStatus::Accepted => (InclusionStatus::Submitted, ExecutionOutcome::Pending),
            SubmitStatus::Rejected => (
                InclusionStatus::Failed(FailureClass::SubmitRejected),
                ExecutionOutcome::Failed(FailureClass::SubmitRejected),
            ),
        };
        let record = ExecutionRecord {
            route_id,
            submission_id: result.submission_id.clone(),
            chain_signature,
            submit_mode,
            submit_endpoint: result.endpoint.clone(),
            submit_status: result.status,
            quoted_slot,
            blockhash_slot,
            submitted_slot,
            inclusion_status,
            outcome,
            profit_mint: None,
            wallet_owner_pubkey: None,
            expected_pnl_quote_atoms: None,
            estimated_execution_cost_quote_atoms: None,
            realized_output_delta_quote_atoms: None,
            realized_pnl_quote_atoms: None,
            failure_detail: None,
            submitted_at,
            last_updated_at: submitted_at,
        };
        self.history.insert(record.clone());
        record
    }

    pub fn transition(
        &mut self,
        submission_id: &SubmissionId,
        status: InclusionStatus,
    ) -> Option<ExecutionTransition> {
        let record = self.history.get_mut(submission_id)?;
        if !can_transition(&record.inclusion_status, &status) {
            return None;
        }

        let previous_inclusion_status = record.inclusion_status.clone();
        let previous_outcome = record.outcome.clone();
        let current_outcome = outcome_for_status(&status);
        record.last_updated_at = SystemTime::now();
        record.inclusion_status = status.clone();
        record.outcome = current_outcome.clone();

        Some(ExecutionTransition {
            submission_id: submission_id.clone(),
            previous_inclusion_status,
            current_inclusion_status: status,
            previous_outcome,
            current_outcome,
        })
    }

    pub fn get(&self, submission_id: &SubmissionId) -> Option<&ExecutionRecord> {
        self.history.get(submission_id)
    }

    pub fn set_failure_detail(
        &mut self,
        submission_id: &SubmissionId,
        detail: ExecutionFailureDetail,
    ) -> bool {
        let Some(record) = self.history.get_mut(submission_id) else {
            return false;
        };
        record.failure_detail = Some(detail);
        record.last_updated_at = SystemTime::now();
        true
    }

    pub fn set_profit_context(
        &mut self,
        submission_id: &SubmissionId,
        profit_mint: Option<String>,
        wallet_owner_pubkey: Option<String>,
        expected_pnl_quote_atoms: Option<i64>,
        estimated_execution_cost_quote_atoms: Option<u64>,
    ) -> bool {
        let Some(record) = self.history.get_mut(submission_id) else {
            return false;
        };
        record.profit_mint = profit_mint;
        record.wallet_owner_pubkey = wallet_owner_pubkey;
        record.expected_pnl_quote_atoms = expected_pnl_quote_atoms;
        record.estimated_execution_cost_quote_atoms = estimated_execution_cost_quote_atoms;
        record.last_updated_at = SystemTime::now();
        true
    }

    pub fn set_realized_pnl(
        &mut self,
        submission_id: &SubmissionId,
        realized_output_delta_quote_atoms: Option<i64>,
        realized_pnl_quote_atoms: Option<i64>,
    ) -> bool {
        let Some(record) = self.history.get_mut(submission_id) else {
            return false;
        };
        record.realized_output_delta_quote_atoms = realized_output_delta_quote_atoms;
        record.realized_pnl_quote_atoms = realized_pnl_quote_atoms;
        record.last_updated_at = SystemTime::now();
        true
    }

    pub fn pending_count(&self) -> usize {
        self.history
            .values()
            .filter(|record| record.outcome == ExecutionOutcome::Pending)
            .count()
    }

    pub fn pending_records(&self) -> Vec<ExecutionRecord> {
        self.history
            .values()
            .filter(|record| record.outcome == ExecutionOutcome::Pending)
            .cloned()
            .collect()
    }
}

fn outcome_for_status(status: &InclusionStatus) -> ExecutionOutcome {
    match status {
        InclusionStatus::Landed { slot } => ExecutionOutcome::Included { slot: *slot },
        InclusionStatus::Dropped => ExecutionOutcome::Failed(FailureClass::ChainDropped),
        InclusionStatus::Expired { .. } => ExecutionOutcome::Failed(FailureClass::Expired),
        InclusionStatus::Failed(class) => ExecutionOutcome::Failed(class.clone()),
        InclusionStatus::Pending | InclusionStatus::Submitted => ExecutionOutcome::Pending,
    }
}

fn can_transition(current: &InclusionStatus, next: &InclusionStatus) -> bool {
    if current == next {
        return false;
    }

    match current {
        InclusionStatus::Submitted => matches!(
            next,
            InclusionStatus::Pending
                | InclusionStatus::Landed { .. }
                | InclusionStatus::Dropped
                | InclusionStatus::Expired { .. }
                | InclusionStatus::Failed(_)
        ),
        InclusionStatus::Pending => matches!(
            next,
            InclusionStatus::Landed { .. }
                | InclusionStatus::Dropped
                | InclusionStatus::Expired { .. }
                | InclusionStatus::Failed(_)
        ),
        InclusionStatus::Landed { .. }
        | InclusionStatus::Dropped
        | InclusionStatus::Expired { .. }
        | InclusionStatus::Failed(_) => false,
    }
}
