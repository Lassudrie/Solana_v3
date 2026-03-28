use crate::quote::LegQuote;
use crate::reasons::RejectionReason;
use crate::route_registry::{RouteKind, RouteLegSequence};
use domain::RouteId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateSelectionSource {
    Legacy,
    Ev,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpportunityCandidate {
    pub route_id: RouteId,
    pub route_kind: RouteKind,
    pub quoted_slot: u64,
    pub leg_snapshot_slots: RouteLegSequence<u64>,
    pub sol_quote_conversion_snapshot_slot: Option<u64>,
    pub trade_size: u64,
    pub selected_by: CandidateSelectionSource,
    pub ranking_score_quote_atoms: i64,
    pub expected_value_quote_atoms: i64,
    pub p_land_bps: u16,
    pub expected_shortfall_quote_atoms: u64,
    pub active_execution_buffer_bps: Option<u16>,
    pub source_input_balance: Option<u64>,
    pub expected_net_output: u64,
    pub minimum_acceptable_output: u64,
    pub expected_gross_profit_quote_atoms: i64,
    pub estimated_network_fee_lamports: u64,
    pub estimated_network_fee_quote_atoms: u64,
    pub jito_tip_lamports: u64,
    pub jito_tip_quote_atoms: u64,
    pub estimated_execution_cost_lamports: u64,
    pub estimated_execution_cost_quote_atoms: u64,
    pub expected_net_profit_quote_atoms: i64,
    pub intermediate_output_amounts: Vec<u64>,
    pub leg_quotes: RouteLegSequence<LegQuote>,
}

impl OpportunityCandidate {
    pub fn leg_count(&self) -> usize {
        self.leg_quotes.len()
    }

    pub fn oldest_leg_snapshot_slot(&self) -> u64 {
        self.leg_snapshot_slots
            .iter()
            .copied()
            .min()
            .unwrap_or_default()
    }

    pub fn oldest_relevant_snapshot_slot(&self) -> u64 {
        self.sol_quote_conversion_snapshot_slot
            .map(|slot| self.oldest_leg_snapshot_slot().min(slot))
            .unwrap_or_else(|| self.oldest_leg_snapshot_slot())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpportunityDecision {
    Accepted(OpportunityCandidate),
    Rejected {
        route_id: RouteId,
        route_kind: Option<RouteKind>,
        leg_count: usize,
        reason: RejectionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionOutcome {
    pub decisions: Vec<OpportunityDecision>,
    pub best_candidate: Option<OpportunityCandidate>,
    pub shadow_candidate: Option<OpportunityCandidate>,
}
