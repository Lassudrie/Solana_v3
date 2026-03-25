use strategy::opportunity::OpportunityCandidate;

use crate::types::AtomicLegPlan;

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

    pub fn materialize_leg_memos(
        &self,
        route_id: &state::types::RouteId,
        quoted_slot: u64,
        expected_net_profit: i64,
        leg_plans: &[AtomicLegPlan; 2],
    ) -> [Vec<u8>; 2] {
        [
            format!(
                "template={};route={};leg=0;slot={quoted_slot};profit={expected_net_profit};venue={};pool={};in={};min_out={}",
                self.template_name,
                route_id.0,
                leg_plans[0].venue,
                leg_plans[0].pool_id.0,
                leg_plans[0].input_amount,
                leg_plans[0].min_output_amount,
            )
            .into_bytes(),
            format!(
                "template={};route={};leg=1;slot={quoted_slot};profit={expected_net_profit};venue={};pool={};in={};min_out={}",
                self.template_name,
                route_id.0,
                leg_plans[1].venue,
                leg_plans[1].pool_id.0,
                leg_plans[1].input_amount,
                leg_plans[1].min_output_amount,
            )
            .into_bytes(),
        ]
    }
}
