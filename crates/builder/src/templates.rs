use strategy::opportunity::OpportunityCandidate;

use crate::types::{AtomicLegPlan, InstructionTemplate};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicTwoLegTemplate {
    pub template_name: &'static str,
}

impl Default for AtomicTwoLegTemplate {
    fn default() -> Self {
        Self {
            template_name: "atomic-two-leg-arb-v1",
        }
    }
}

impl AtomicTwoLegTemplate {
    pub fn materialize_leg_plans(&self, candidate: &OpportunityCandidate) -> [AtomicLegPlan; 2] {
        let first = &candidate.leg_quotes[0];
        let second = &candidate.leg_quotes[1];
        [
            AtomicLegPlan {
                venue: first.venue.clone(),
                pool_id: first.pool_id.clone(),
                input_amount: first.input_amount,
                min_output_amount: first.output_amount,
            },
            AtomicLegPlan {
                venue: second.venue.clone(),
                pool_id: second.pool_id.clone(),
                input_amount: second.input_amount,
                min_output_amount: second.output_amount,
            },
        ]
    }

    pub fn materialize_instructions(
        &self,
        route_id: &state::types::RouteId,
        leg_plans: &[AtomicLegPlan; 2],
        tip_lamports: u64,
    ) -> Vec<InstructionTemplate> {
        vec![
            InstructionTemplate {
                label: "compute-budget".into(),
                program: "solana.compute_budget".into(),
                accounts: Vec::new(),
                data: b"set_budget".to_vec(),
            },
            InstructionTemplate {
                label: format!("{}-leg-1", route_id.0),
                program: leg_plans[0].venue.clone(),
                accounts: vec![leg_plans[0].pool_id.0.clone()],
                data: leg_plans[0].min_output_amount.to_le_bytes().to_vec(),
            },
            InstructionTemplate {
                label: format!("{}-leg-2", route_id.0),
                program: leg_plans[1].venue.clone(),
                accounts: vec![leg_plans[1].pool_id.0.clone()],
                data: leg_plans[1].min_output_amount.to_le_bytes().to_vec(),
            },
            InstructionTemplate {
                label: "jito-tip".into(),
                program: "jito.tip".into(),
                accounts: vec!["tip".into()],
                data: tip_lamports.to_le_bytes().to_vec(),
            },
        ]
    }
}
