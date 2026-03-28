use domain::RouteId;
use strategy::opportunity::OpportunityCandidate;

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
    ) -> [AtomicLegPlan; 2] {
        let first = &candidate.leg_quotes[0];
        let second = &candidate.leg_quotes[1];
        let second_leg_min_output =
            route_minimum_acceptable_output(candidate).min(second.output_amount);
        let first_leg_amount_mode = if route_execution.legs[0].supports_exact_out_leg1(first.side) {
            SwapAmountMode::ExactOut
        } else {
            SwapAmountMode::ExactIn
        };
        [
            AtomicLegPlan {
                venue: first.venue.clone(),
                pool_id: first.pool_id.clone(),
                side: first.side,
                amount_mode: first_leg_amount_mode,
                specified_amount: match first_leg_amount_mode {
                    SwapAmountMode::ExactIn => first.input_amount,
                    SwapAmountMode::ExactOut => second.input_amount,
                },
                other_amount_threshold: match first_leg_amount_mode {
                    // The first leg fallback stays exact-in when the venue does
                    // not support exact-out for the bridge direction.
                    SwapAmountMode::ExactIn => second.input_amount,
                    SwapAmountMode::ExactOut => apply_input_buffer(
                        first.input_amount,
                        candidate.active_execution_buffer_bps.unwrap_or(0),
                    ),
                },
                current_tick_index: first.current_tick_index,
            },
            AtomicLegPlan {
                venue: second.venue.clone(),
                pool_id: second.pool_id.clone(),
                side: second.side,
                amount_mode: SwapAmountMode::ExactIn,
                specified_amount: second.input_amount,
                other_amount_threshold: second_leg_min_output,
                current_tick_index: second.current_tick_index,
            },
        ]
    }

    pub fn materialize_leg_memos(
        &self,
        route_id: &RouteId,
        quoted_slot: u64,
        expected_net_profit: i64,
        leg_plans: &[AtomicLegPlan; 2],
    ) -> [Vec<u8>; 2] {
        [
            format!(
                "template={};route={};leg=0;slot={quoted_slot};profit={expected_net_profit};venue={};pool={};mode={};amount={};threshold={}",
                self.template_name,
                route_id.0,
                leg_plans[0].venue,
                leg_plans[0].pool_id.0,
                leg_mode_label(&leg_plans[0].amount_mode),
                leg_plans[0].specified_amount,
                leg_plans[0].other_amount_threshold,
            )
            .into_bytes(),
            format!(
                "template={};route={};leg=1;slot={quoted_slot};profit={expected_net_profit};venue={};pool={};mode={};amount={};threshold={}",
                self.template_name,
                route_id.0,
                leg_plans[1].venue,
                leg_plans[1].pool_id.0,
                leg_mode_label(&leg_plans[1].amount_mode),
                leg_plans[1].specified_amount,
                leg_plans[1].other_amount_threshold,
            )
            .into_bytes(),
        ]
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
        route_registry::SwapSide,
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
            quoted_slot: 42,
            leg_snapshot_slots: [42, 42],
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
            ],
        }
    }

    fn route_execution(exact_out_supported: bool) -> RouteExecutionConfig {
        RouteExecutionConfig {
            route_id: RouteId("route-a".into()),
            message_mode: MessageMode::V0Required,
            lookup_tables: Vec::new(),
            default_compute_unit_limit: 300_000,
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
            ],
        }
    }

    #[test]
    fn materialized_leg_plans_emit_exact_out_for_supported_leg1() {
        let template = AtomicTwoLegTemplate::default();
        let [first, second] = template.materialize_leg_plans(&candidate(), &route_execution(true));

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
        let [first, second] = template.materialize_leg_plans(&candidate(), &route_execution(false));

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
        let [first, second] = template.materialize_leg_plans(&candidate, &route_execution(true));

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
        let [first, _] = template.materialize_leg_plans(&candidate, &route_execution(true));

        assert_eq!(first.amount_mode, SwapAmountMode::ExactOut);
        assert_eq!(first.specified_amount, candidate.leg_quotes[1].input_amount);
        assert_eq!(first.other_amount_threshold, 10_025);
    }
}
