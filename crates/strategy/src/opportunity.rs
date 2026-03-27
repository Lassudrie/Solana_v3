use crate::quote::LegQuote;
use crate::reasons::RejectionReason;
use domain::RouteId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpportunityCandidate {
    pub route_id: RouteId,
    pub quoted_slot: u64,
    pub leg_snapshot_slots: [u64; 2],
    pub trade_size: u64,
    pub active_execution_buffer_bps: Option<u16>,
    pub expected_net_output: u64,
    pub minimum_acceptable_output: u64,
    pub expected_gross_profit_quote_atoms: i64,
    pub estimated_execution_cost_lamports: u64,
    pub estimated_execution_cost_quote_atoms: u64,
    pub expected_net_profit_quote_atoms: i64,
    pub leg_quotes: [LegQuote; 2],
}

impl OpportunityCandidate {
    pub fn oldest_leg_snapshot_slot(&self) -> u64 {
        self.leg_snapshot_slots[0].min(self.leg_snapshot_slots[1])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpportunityDecision {
    Accepted(OpportunityCandidate),
    Rejected {
        route_id: RouteId,
        reason: RejectionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionOutcome {
    pub decisions: Vec<OpportunityDecision>,
    pub best_candidate: Option<OpportunityCandidate>,
}
