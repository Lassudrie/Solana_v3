use domain::RouteId;
use strategy::opportunity::OpportunityCandidate;
use strategy::route_registry::RouteLegSequence;

use crate::{
    execution::RouteExecutionConfig,
    types::{AtomicLegPlan, SwapAmountMode},
};

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
    pub fn materialize_leg_plans(
        &self,
        candidate: &OpportunityCandidate,
        route_execution: &RouteExecutionConfig,
    ) -> RouteLegSequence<AtomicLegPlan> {
        let quote_legs = candidate.leg_quotes.as_slice();
        let execution_legs = route_execution.legs.as_slice();
        let route_input_buffer_allowance = quote_legs
            .first()
            .zip(execution_legs.first())
            .filter(|(quote_leg, execution_leg)| execution_leg.supports_exact_out(quote_leg.side))
            .map(|(quote_leg, _)| {
                apply_input_buffer(
                    quote_leg.input_amount,
                    candidate.active_execution_buffer_bps.unwrap_or(0),
                )
                .saturating_sub(quote_leg.input_amount)
            })
            .unwrap_or(0);
        let mut plans = Vec::with_capacity(quote_legs.len());
        for index in 0..quote_legs.len() {
            let quote_leg = &quote_legs[index];
            let execution_leg = &execution_legs[index];
            let (amount_mode, specified_amount, other_amount_threshold) =
                if index + 1 == quote_legs.len() {
                    (
                        SwapAmountMode::ExactIn,
                        quote_leg.input_amount,
                        route_minimum_acceptable_output(candidate)
                            .saturating_add(route_input_buffer_allowance)
                            .min(quote_leg.output_amount),
                    )
                } else if execution_leg.supports_exact_out(quote_leg.side) {
                    (
                        SwapAmountMode::ExactOut,
                        quote_legs[index + 1].input_amount,
                        apply_input_buffer(
                            quote_leg.input_amount,
                            candidate.active_execution_buffer_bps.unwrap_or(0),
                        ),
                    )
                } else {
                    (
                        SwapAmountMode::ExactIn,
                        quote_leg.input_amount,
                        quote_legs[index + 1].input_amount,
                    )
                };
            plans.push(AtomicLegPlan {
                venue: quote_leg.venue.clone(),
                pool_id: quote_leg.pool_id.clone(),
                side: quote_leg.side,
                amount_mode,
                specified_amount,
                other_amount_threshold,
                current_tick_index: quote_leg.current_tick_index,
            });
        }

        RouteLegSequence::from_vec(plans).expect("candidate legs must be 2 or 3")
    }

    pub fn materialize_leg_memos(
        &self,
        route_id: &RouteId,
        quoted_slot: u64,
        expected_net_profit: i64,
        leg_plans: &RouteLegSequence<AtomicLegPlan>,
    ) -> Vec<Vec<u8>> {
        leg_plans
            .iter()
            .enumerate()
            .map(|(index, leg_plan)| {
                format!(
                    "template={};route={};leg={index};slot={quoted_slot};profit={expected_net_profit};venue={};pool={};mode={};amount={};threshold={}",
                    self.template_name,
                    route_id.0,
                    leg_plan.venue,
                    leg_plan.pool_id.0,
                    leg_mode_label(&leg_plan.amount_mode),
                    leg_plan.specified_amount,
                    leg_plan.other_amount_threshold,
                )
                .into_bytes()
            })
            .collect()
    }
}

fn leg_mode_label(mode: &SwapAmountMode) -> &'static str {
    match mode {
        SwapAmountMode::ExactIn => "exact_in",
        SwapAmountMode::ExactOut => "exact_out",
    }
}

fn route_minimum_acceptable_output(candidate: &OpportunityCandidate) -> u64 {
    candidate.minimum_acceptable_output
}

fn apply_input_buffer(amount: u64, buffer_bps: u16) -> u64 {
    if buffer_bps == 0 {
        return amount;
    }

    let numerator = u128::from(amount).saturating_mul(10_000u128 + u128::from(buffer_bps));
    numerator
        .saturating_add(9_999)
        .checked_div(10_000)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use domain::{PoolId, RouteId};
    use strategy::{
        opportunity::{CandidateSelectionSource, OpportunityCandidate},
        quote::LegQuote,
        route_registry::{RouteKind, SwapSide},
    };

    use crate::{
        execution::{
            MessageMode, OrcaSimplePoolConfig, OrcaWhirlpoolConfig, RaydiumClmmConfig,
            RouteExecutionConfig, VenueExecutionConfig,
        },
        types::SwapAmountMode,
    };

    use super::AtomicTwoLegTemplate;

    fn candidate() -> OpportunityCandidate {
        OpportunityCandidate {
            route_id: RouteId("route-a".into()),
            route_kind: RouteKind::TwoLeg,
            quoted_slot: 42,
            leg_snapshot_slots: [42, 42].into(),
            sol_quote_conversion_snapshot_slot: None,
            trade_size: 10_000,
            selected_by: CandidateSelectionSource::Legacy,
            ranking_score_quote_atoms: 250,
            expected_value_quote_atoms: 250,
            p_land_bps: 10_000,
            expected_shortfall_quote_atoms: 0,
            active_execution_buffer_bps: None,
            expected_net_output: 10_250,
            minimum_acceptable_output: 10_175,
            expected_gross_profit_quote_atoms: 350,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 100,
            expected_net_profit_quote_atoms: 250,
            intermediate_output_amounts: vec![10_120],
            leg_quotes: [
                LegQuote {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 10_000,
                    output_amount: 10_120,
                    fee_paid: 10,
                    current_tick_index: None,
                },
                LegQuote {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 10_120,
                    output_amount: 10_250,
                    fee_paid: 10,
                    current_tick_index: None,
                },
            ]
            .into(),
        }
    }

    fn route_execution(exact_out_supported: bool) -> RouteExecutionConfig {
        RouteExecutionConfig {
            route_id: RouteId("route-a".into()),
            kind: RouteKind::TwoLeg,
            message_mode: MessageMode::V0Required,
            lookup_tables: Vec::new(),
            default_compute_unit_limit: 300_000,
            minimum_compute_unit_limit: RouteKind::TwoLeg.minimum_compute_unit_limit(),
            default_compute_unit_price_micro_lamports: 25_000,
            default_jito_tip_lamports: 5_000,
            max_quote_slot_lag: 4,
            max_alt_slot_lag: 4,
            legs: [
                if exact_out_supported {
                    VenueExecutionConfig::OrcaWhirlpool(OrcaWhirlpoolConfig {
                        program_id: "program-a".into(),
                        token_program_id: "token-program".into(),
                        whirlpool: "pool-a".into(),
                        token_mint_a: "mint-a".into(),
                        token_vault_a: "vault-a".into(),
                        token_mint_b: "mint-b".into(),
                        token_vault_b: "vault-b".into(),
                        tick_spacing: 64,
                        a_to_b: true,
                    })
                } else {
                    VenueExecutionConfig::OrcaSimplePool(OrcaSimplePoolConfig {
                        program_id: "program-a".into(),
                        token_program_id: "token-program".into(),
                        swap_account: "pool-a".into(),
                        authority: "authority".into(),
                        pool_source_token_account: "vault-a".into(),
                        pool_destination_token_account: "vault-b".into(),
                        pool_mint: "pool-mint".into(),
                        fee_account: "fee-account".into(),
                        user_source_token_account: "user-source".into(),
                        user_destination_token_account: "user-destination".into(),
                        host_fee_account: None,
                    })
                },
                VenueExecutionConfig::RaydiumClmm(RaydiumClmmConfig {
                    program_id: "program-b".into(),
                    token_program_id: "token-program".into(),
                    token_program_2022_id: "token-program-2022".into(),
                    memo_program_id: "memo".into(),
                    pool_state: "pool-b".into(),
                    amm_config: "amm-config".into(),
                    observation_state: "observation".into(),
                    ex_bitmap_account: None,
                    token_mint_0: "mint-0".into(),
                    token_vault_0: "vault-0".into(),
                    token_mint_1: "mint-1".into(),
                    token_vault_1: "vault-1".into(),
                    tick_spacing: 60,
                    zero_for_one: true,
                }),
            ]
            .into(),
        }
    }

    #[test]
    fn materialized_leg_plans_emit_exact_out_for_supported_leg1() {
        let template = AtomicTwoLegTemplate::default();
        let plans = template.materialize_leg_plans(&candidate(), &route_execution(true));
        let first = &plans[0];
        let second = &plans[1];

        assert_eq!(first.amount_mode, SwapAmountMode::ExactOut);
        assert_eq!(first.specified_amount, 10_120);
        assert_eq!(first.other_amount_threshold, 10_000);
        assert_eq!(second.amount_mode, SwapAmountMode::ExactIn);
        assert_eq!(second.specified_amount, 10_120);
        assert_eq!(second.other_amount_threshold, 10_175);
    }

    #[test]
    fn materialized_leg_plans_fallback_to_exact_in_when_leg1_does_not_support_exact_out() {
        let template = AtomicTwoLegTemplate::default();
        let plans = template.materialize_leg_plans(&candidate(), &route_execution(false));
        let first = &plans[0];
        let second = &plans[1];

        assert_eq!(first.amount_mode, SwapAmountMode::ExactIn);
        assert_eq!(first.specified_amount, 10_000);
        assert_eq!(first.other_amount_threshold, 10_120);
        assert_eq!(second.amount_mode, SwapAmountMode::ExactIn);
    }

    #[test]
    fn materialized_leg_plans_never_exceed_quoted_outputs() {
        let mut candidate = candidate();
        candidate.minimum_acceptable_output = 20_000;

        let template = AtomicTwoLegTemplate::default();
        let plans = template.materialize_leg_plans(&candidate, &route_execution(true));
        let first = &plans[0];
        let second = &plans[1];

        assert_eq!(
            second.other_amount_threshold,
            candidate.leg_quotes[1].output_amount
        );
        assert_eq!(first.specified_amount, candidate.leg_quotes[1].input_amount);
        assert_eq!(
            first.other_amount_threshold,
            candidate.leg_quotes[0].input_amount
        );
    }

    #[test]
    fn exact_out_first_leg_applies_active_execution_buffer_to_max_input() {
        let mut candidate = candidate();
        candidate.active_execution_buffer_bps = Some(25);

        let template = AtomicTwoLegTemplate::default();
        let plans = template.materialize_leg_plans(&candidate, &route_execution(true));
        let first = &plans[0];

        assert_eq!(first.amount_mode, SwapAmountMode::ExactOut);
        assert_eq!(first.specified_amount, candidate.leg_quotes[1].input_amount);
        assert_eq!(first.other_amount_threshold, 10_025);
    }

    #[test]
    fn exact_out_first_leg_raises_final_min_output_by_buffer_allowance() {
        let mut candidate = candidate();
        candidate.active_execution_buffer_bps = Some(25);

        let template = AtomicTwoLegTemplate::default();
        let plans = template.materialize_leg_plans(&candidate, &route_execution(true));
        let second = &plans[1];

        assert_eq!(second.amount_mode, SwapAmountMode::ExactIn);
        assert_eq!(
            second.specified_amount,
            candidate.leg_quotes[1].input_amount
        );
        assert_eq!(second.other_amount_threshold, 10_200);
    }
}
