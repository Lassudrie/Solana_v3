use crate::{
    guards::GuardrailSet,
    opportunity::{OpportunityCandidate, OpportunityDecision, SelectionOutcome},
    quote::{LocalTwoLegQuoteEngine, PoolPricingView, QuoteEngine, QuoteExecutionAdjustments},
    reasons::RejectionReason,
    route_registry::{RouteDefinition, RouteRegistry, SwapSide},
};
use state::{StatePlane, types::RouteId};
use std::collections::{BTreeSet, HashMap};

const DEFAULT_SIZE_LADDER: [u64; 6] =
    [250_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000];
const MAX_RESERVE_USAGE_BPS: u64 = 2_000;

#[derive(Debug)]
pub struct OpportunitySelector<E = LocalTwoLegQuoteEngine> {
    quote_engine: E,
    guards: GuardrailSet,
}

impl<E> OpportunitySelector<E>
where
    E: QuoteEngine,
{
    pub fn new(quote_engine: E, guards: GuardrailSet) -> Self {
        Self {
            quote_engine,
            guards,
        }
    }

    pub fn evaluate(
        &self,
        registry: &RouteRegistry,
        state: &StatePlane,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
        route_execution_buffers: &HashMap<RouteId, u16>,
    ) -> SelectionOutcome {
        if impacted_routes.is_empty() {
            return SelectionOutcome {
                decisions: vec![OpportunityDecision::Rejected {
                    route_id: RouteId("none".into()),
                    reason: RejectionReason::NoImpactedRoutes,
                }],
                best_candidate: None,
            };
        }

        let execution_state = state.execution_state();
        let mut decisions = Vec::with_capacity(impacted_routes.len());
        let mut accepted = Vec::new();

        for route_id in impacted_routes {
            let Some(route) = registry.get(route_id) else {
                decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    reason: RejectionReason::RouteNotRegistered,
                });
                continue;
            };

            match self.evaluate_route(
                route,
                state,
                &execution_state,
                inflight_submissions,
                route_execution_buffers.get(route_id).copied(),
            ) {
                Ok(candidate) => {
                    accepted.push(candidate.clone());
                    decisions.push(OpportunityDecision::Accepted(candidate));
                }
                Err(reason) => decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    reason,
                }),
            }
        }

        let best_candidate = accepted
            .into_iter()
            .max_by_key(|candidate| candidate.expected_net_profit_quote_atoms);

        SelectionOutcome {
            decisions,
            best_candidate,
        }
    }

    fn evaluate_route(
        &self,
        route: &RouteDefinition,
        state: &StatePlane,
        execution_state: &state::types::ExecutionStateSnapshot,
        inflight_submissions: usize,
        active_execution_buffer_bps: Option<u16>,
    ) -> Result<OpportunityCandidate, RejectionReason> {
        self.guards
            .evaluate_route_readiness(&route.route_id, state)?;

        let first = state.pool_snapshot(&route.legs[0].pool_id).ok_or_else(|| {
            RejectionReason::MissingSnapshot {
                pool_id: route.legs[0].pool_id.clone(),
            }
        })?;
        let second = state.pool_snapshot(&route.legs[1].pool_id).ok_or_else(|| {
            RejectionReason::MissingSnapshot {
                pool_id: route.legs[1].pool_id.clone(),
            }
        })?;
        let sol_quote_conversion_snapshot = route
            .sol_quote_conversion_pool_id
            .as_ref()
            .map(|pool_id| {
                state
                    .pool_snapshot(pool_id)
                    .ok_or_else(|| RejectionReason::MissingSnapshot {
                        pool_id: pool_id.clone(),
                    })
            })
            .transpose()?;

        let mut snapshots = vec![first, second];
        if let Some(snapshot) = sol_quote_conversion_snapshot {
            snapshots.push(snapshot);
        }
        self.guards.evaluate_snapshots(state, &snapshots)?;

        let mut best_candidate: Option<OpportunityCandidate> = None;
        let mut last_rejection = None;
        let adjustments = route_quote_adjustments(route, active_execution_buffer_bps);
        for trade_size in trade_sizes(route) {
            let quote = self
                .quote_engine
                .quote(
                    route,
                    [
                        PoolPricingView {
                            snapshot: first,
                            concentrated: state.concentrated_quote_model(&route.legs[0].pool_id),
                        },
                        PoolPricingView {
                            snapshot: second,
                            concentrated: state.concentrated_quote_model(&route.legs[1].pool_id),
                        },
                    ],
                    state.latest_slot(),
                    trade_size,
                    &adjustments,
                )
                .map_err(|error| RejectionReason::QuoteFailed {
                    detail: error.to_string(),
                })?;
            let quote = quote
                .with_estimated_execution_cost(
                    route,
                    sol_quote_conversion_snapshot,
                    route.estimated_execution_cost_lamports,
                )
                .map_err(|error| RejectionReason::ExecutionCostNotConvertible {
                    detail: error.to_string(),
                })?;

            if let Some(reason) = reserve_usage_rejection(route, [first, second], &quote) {
                last_rejection = Some(reason);
                continue;
            }

            match self
                .guards
                .evaluate_quote(route, &quote, execution_state, inflight_submissions)
            {
                Ok(()) => {
                    let candidate = OpportunityCandidate {
                        route_id: route.route_id.clone(),
                        quoted_slot: quote.quoted_slot,
                        leg_snapshot_slots: [first.last_update_slot, second.last_update_slot],
                        trade_size: quote.input_amount,
                        active_execution_buffer_bps: route
                            .execution_protection
                            .as_ref()
                            .map(|_| active_execution_buffer_bps.unwrap_or(0)),
                        expected_net_output: quote.net_output_amount,
                        minimum_acceptable_output: self.guards.minimum_acceptable_output(
                            quote.input_amount,
                            quote.estimated_execution_cost_quote_atoms,
                        ),
                        expected_gross_profit_quote_atoms: quote.expected_gross_profit_quote_atoms,
                        estimated_execution_cost_lamports: quote.estimated_execution_cost_lamports,
                        estimated_execution_cost_quote_atoms: quote
                            .estimated_execution_cost_quote_atoms,
                        expected_net_profit_quote_atoms: quote.expected_net_profit_quote_atoms,
                        leg_quotes: quote.leg_quotes,
                    };
                    match &best_candidate {
                        Some(best)
                            if best.expected_net_profit_quote_atoms
                                > candidate.expected_net_profit_quote_atoms => {}
                        Some(best)
                            if best.expected_net_profit_quote_atoms
                                == candidate.expected_net_profit_quote_atoms
                                && best.trade_size <= candidate.trade_size => {}
                        _ => best_candidate = Some(candidate),
                    }
                }
                Err(reason) => last_rejection = Some(reason),
            }
        }

        best_candidate.ok_or_else(|| {
            last_rejection.unwrap_or(RejectionReason::RouteFilteredOut {
                route_id: route.route_id.clone(),
            })
        })
    }
}

fn route_quote_adjustments(
    route: &RouteDefinition,
    active_execution_buffer_bps: Option<u16>,
) -> QuoteExecutionAdjustments {
    let mut adjustments = QuoteExecutionAdjustments::default();
    let Some(_) = route.execution_protection.as_ref() else {
        return adjustments;
    };
    let buffer_bps = active_execution_buffer_bps.unwrap_or(0);
    for (index, leg) in route.legs.iter().enumerate() {
        if leg.side == SwapSide::BuyBase {
            adjustments.extra_leg_slippage_bps[index] = buffer_bps;
        }
    }
    adjustments
}

fn trade_sizes(route: &RouteDefinition) -> Vec<u64> {
    let min_trade_size = effective_min_trade_size(route);
    let max_trade_size = route.max_trade_size.max(min_trade_size);
    let default_trade_size = route
        .default_trade_size
        .clamp(min_trade_size, max_trade_size);

    let mut candidates = BTreeSet::from([min_trade_size, default_trade_size, max_trade_size]);
    if !route.size_ladder.is_empty() {
        candidates.extend(
            route
                .size_ladder
                .iter()
                .copied()
                .filter(|size| *size >= min_trade_size && *size <= max_trade_size),
        );
    } else {
        candidates.extend(
            DEFAULT_SIZE_LADDER
                .iter()
                .copied()
                .filter(|size| *size >= min_trade_size && *size <= max_trade_size),
        );
    }

    prioritize_trade_sizes(candidates.into_iter().collect(), default_trade_size)
}

fn effective_min_trade_size(route: &RouteDefinition) -> u64 {
    route
        .min_trade_size
        .max(1)
        .min(route.default_trade_size.max(1))
        .min(route.max_trade_size.max(1))
}

fn prioritize_trade_sizes(mut sizes: Vec<u64>, default_trade_size: u64) -> Vec<u64> {
    sizes.sort_unstable();
    sizes.dedup();

    let mut ordered = Vec::with_capacity(sizes.len());
    if sizes.contains(&default_trade_size) {
        ordered.push(default_trade_size);
    }
    for size in sizes
        .iter()
        .copied()
        .filter(|size| *size < default_trade_size)
        .rev()
    {
        ordered.push(size);
    }
    for size in sizes
        .iter()
        .copied()
        .filter(|size| *size > default_trade_size)
    {
        ordered.push(size);
    }
    ordered
}

fn reserve_usage_rejection(
    route: &RouteDefinition,
    snapshots: [&state::types::PoolSnapshot; 2],
    quote: &crate::quote::RouteQuote,
) -> Option<RejectionReason> {
    let base_mint = route.base_mint.as_deref()?;
    let quote_mint = route.quote_mint.as_deref()?;

    for (index, (leg, snapshot)) in route.legs.iter().zip(snapshots.iter()).enumerate() {
        let (input_mint, output_mint) = match leg.side {
            SwapSide::BuyBase => (quote_mint, base_mint),
            SwapSide::SellBase => (base_mint, quote_mint),
        };
        let (reserve_in, _) = snapshot.constant_product_reserves_for(input_mint, output_mint)?;
        let input_amount = quote.leg_quotes[index].input_amount;
        let usage_bps = ((u128::from(input_amount) * 10_000u128)
            .saturating_add(u128::from(reserve_in).saturating_sub(1))
            / u128::from(reserve_in)) as u64;
        if usage_bps > MAX_RESERVE_USAGE_BPS {
            return Some(RejectionReason::ReserveUsageTooHigh {
                pool_id: snapshot.pool_id.clone(),
                usage_bps,
                maximum_bps: MAX_RESERVE_USAGE_BPS,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{OpportunitySelector, trade_sizes};
    use crate::{
        guards::{GuardrailConfig, GuardrailSet},
        quote::{
            LegQuote, PoolPricingView, QuoteEngine, QuoteError, QuoteExecutionAdjustments,
            RouteQuote,
        },
        route_registry::{ExecutionProtectionPolicy, RouteDefinition, RouteLeg, SwapSide},
    };
    use detection::events::SnapshotConfidence;
    use detection::{EventSourceKind, NormalizedEvent, PoolSnapshotUpdate};
    use state::{
        StatePlane,
        types::{ExecutionStateSnapshot, PoolId, RouteId},
    };

    #[derive(Debug, Default)]
    struct MockQuoteEngine;

    impl QuoteEngine for MockQuoteEngine {
        fn quote(
            &self,
            route: &RouteDefinition,
            _snapshots: [PoolPricingView<'_>; 2],
            quoted_slot: u64,
            input_amount: u64,
            adjustments: &QuoteExecutionAdjustments,
        ) -> Result<RouteQuote, QuoteError> {
            let adjustment_penalty = i64::from(adjustments.extra_leg_slippage_bps[0])
                * i64::try_from(input_amount / 250_000).unwrap_or(0)
                / 5;
            let expected_net_profit_quote_atoms = if route.execution_protection.is_some() {
                match input_amount {
                    1_000_000 => 60 - adjustment_penalty,
                    500_000 => 55 - adjustment_penalty,
                    250_000 => 40 - adjustment_penalty,
                    _ => -100,
                }
            } else {
                match input_amount {
                    1_000_000 => -50,
                    500_000 => 25,
                    250_000 => 25,
                    _ => -100,
                }
            };
            let net_output_amount = if expected_net_profit_quote_atoms >= 0 {
                input_amount.saturating_add(expected_net_profit_quote_atoms as u64)
            } else {
                input_amount.saturating_sub((-expected_net_profit_quote_atoms) as u64)
            };
            Ok(RouteQuote {
                quoted_slot,
                input_amount,
                gross_output_amount: net_output_amount,
                net_output_amount,
                expected_gross_profit_quote_atoms: expected_net_profit_quote_atoms,
                estimated_execution_cost_lamports: 0,
                estimated_execution_cost_quote_atoms: 0,
                expected_net_profit_quote_atoms,
                leg_quotes: [
                    LegQuote {
                        venue: route.legs[0].venue.clone(),
                        pool_id: route.legs[0].pool_id.clone(),
                        side: route.legs[0].side,
                        input_amount,
                        output_amount: input_amount,
                        fee_paid: 0,
                        current_tick_index: None,
                    },
                    LegQuote {
                        venue: route.legs[1].venue.clone(),
                        pool_id: route.legs[1].pool_id.clone(),
                        side: route.legs[1].side,
                        input_amount,
                        output_amount: net_output_amount,
                        fee_paid: 0,
                        current_tick_index: None,
                    },
                ],
            })
        }
    }

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            sol_quote_conversion_pool_id: Some(PoolId("pool-a".into())),
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    fee_bps: None,
                },
            ],
            min_trade_size: 250_000,
            default_trade_size: 1_000_000,
            max_trade_size: 5_000_000,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
            execution_protection: None,
        }
    }

    #[test]
    fn default_ladder_evaluates_default_then_smaller_then_larger_sizes() {
        let sizes = trade_sizes(&route_definition());
        assert_eq!(
            sizes,
            vec![1_000_000, 500_000, 250_000, 2_000_000, 3_000_000, 5_000_000]
        );
    }

    #[test]
    fn explicit_ladder_respects_min_trade_size_and_centered_order() {
        let mut route = route_definition();
        route.size_ladder = vec![100_000, 250_000, 500_000, 2_000_000, 3_000_000];

        let sizes = trade_sizes(&route);
        assert_eq!(
            sizes,
            vec![1_000_000, 500_000, 250_000, 2_000_000, 3_000_000, 5_000_000]
        );
    }

    #[test]
    fn selector_picks_smaller_trade_size_when_default_is_unprofitable() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(10);
        state.execution_state_mut().set_blockhash("blockhash-1", 10);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, None)
            .expect("smaller profitable trade size should be selected");

        assert_eq!(candidate.trade_size, 250_000);
        assert_eq!(candidate.expected_net_profit_quote_atoms, 25);
        assert_eq!(candidate.leg_snapshot_slots, [10, 10]);
    }

    #[test]
    fn protected_route_buffer_can_push_selection_to_smaller_size() {
        let mut route = route_definition();
        route.execution_protection = Some(ExecutionProtectionPolicy {
            tight_max_quote_slot_lag: 4,
            base_extra_buy_leg_slippage_bps: 50,
            failure_step_bps: 25,
            max_extra_buy_leg_slippage_bps: 150,
            recovery_success_count: 3,
        });
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(10);
        state.execution_state_mut().set_blockhash("blockhash-1", 10);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, Some(50))
            .expect("protected route should remain tradable");

        assert_eq!(candidate.trade_size, 500_000);
        assert_eq!(candidate.active_execution_buffer_bps, Some(50));
    }

    #[test]
    fn selector_carries_leg_snapshot_slots_even_when_head_slot_is_newer() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 12,
            rpc_slot: Some(12),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(12),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(4);
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(12);
        state.execution_state_mut().set_blockhash("blockhash-1", 12);

        for (pool_id, slot) in [("pool-a", 8), ("pool-b", 11)] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(12);

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, None)
            .expect("candidate should be selected");

        assert_eq!(candidate.quoted_slot, 12);
        assert_eq!(candidate.leg_snapshot_slots, [8, 11]);
        assert_eq!(candidate.oldest_leg_snapshot_slot(), 8);
    }
}
