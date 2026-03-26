use crate::quote::LegQuote;
use crate::reasons::RejectionReason;
use state::types::RouteId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpportunityCandidate {
    pub route_id: RouteId,
    pub quoted_slot: u64,
    pub trade_size: u64,
    pub expected_net_output: u64,
    pub expected_gross_profit: i64,
    pub estimated_execution_cost_lamports: u64,
    pub expected_net_profit: i64,
    pub leg_quotes: [LegQuote; 2],
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
