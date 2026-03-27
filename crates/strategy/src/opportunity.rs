use crate::quote::LegQuote;
use crate::reasons::RejectionReason;
use state::types::RouteId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpportunityCandidate {
    pub route_id: RouteId,
    pub quoted_slot: u64,
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
