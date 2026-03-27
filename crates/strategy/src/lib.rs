pub mod guards;
pub mod opportunity;
pub mod quote;
pub mod reasons;
pub mod route_registry;
pub mod selector;

use std::collections::HashMap;

use domain::{ExecutionSnapshot, RouteId};
use guards::{GuardrailConfig, GuardrailSet};
use opportunity::SelectionOutcome;
use quote::LocalTwoLegQuoteEngine;
use route_registry::{RouteDefinition, RouteRegistry};
use selector::OpportunitySelector;
use state::StatePlane;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RouteExecutionProtectionState {
    current_extra_buy_leg_slippage_bps: u16,
    consecutive_successes: u16,
}

#[derive(Debug)]
pub struct StrategyPlane {
    registry: RouteRegistry,
    selector: OpportunitySelector,
    execution_protection: HashMap<RouteId, RouteExecutionProtectionState>,
}

impl StrategyPlane {
    pub fn new(guardrails: GuardrailConfig) -> Self {
        Self {
            registry: RouteRegistry::default(),
            selector: OpportunitySelector::new(
                LocalTwoLegQuoteEngine,
                GuardrailSet::new(guardrails),
            ),
            execution_protection: HashMap::new(),
        }
    }

    pub fn register_route(&mut self, route: RouteDefinition) {
        if let Some(policy) = &route.execution_protection {
            self.execution_protection.insert(
                route.route_id.clone(),
                RouteExecutionProtectionState {
                    current_extra_buy_leg_slippage_bps: policy.base_extra_buy_leg_slippage_bps,
                    consecutive_successes: 0,
                },
            );
        }
        self.registry.register(route);
    }

    pub fn evaluate(
        &mut self,
        state: &StatePlane,
        execution: &ExecutionSnapshot,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
    ) -> SelectionOutcome {
        let route_execution_buffers = self
            .execution_protection
            .iter()
            .map(|(route_id, state)| (route_id.clone(), state.current_extra_buy_leg_slippage_bps))
            .collect::<HashMap<_, _>>();
        self.selector.evaluate(
            &self.registry,
            state,
            execution,
            impacted_routes,
            inflight_submissions,
            &route_execution_buffers,
        )
    }

    pub fn record_execution_too_little_output(&mut self, route_id: &RouteId) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        let Some(policy) = &route.execution_protection else {
            return;
        };
        let Some(state) = self.execution_protection.get_mut(route_id) else {
            return;
        };
        state.current_extra_buy_leg_slippage_bps = state
            .current_extra_buy_leg_slippage_bps
            .saturating_add(policy.failure_step_bps)
            .min(policy.max_extra_buy_leg_slippage_bps);
        state.consecutive_successes = 0;
    }

    pub fn record_execution_success(&mut self, route_id: &RouteId) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        let Some(policy) = &route.execution_protection else {
            return;
        };
        let Some(state) = self.execution_protection.get_mut(route_id) else {
            return;
        };
        state.consecutive_successes = state.consecutive_successes.saturating_add(1);
        if state.consecutive_successes < policy.recovery_success_count.max(1) {
            return;
        }

        state.current_extra_buy_leg_slippage_bps = state
            .current_extra_buy_leg_slippage_bps
            .saturating_sub(policy.failure_step_bps)
            .max(policy.base_extra_buy_leg_slippage_bps);
        state.consecutive_successes = 0;
    }

    pub fn active_execution_buffer_bps(&self, route_id: &RouteId) -> Option<u16> {
        self.execution_protection
            .get(route_id)
            .map(|state| state.current_extra_buy_leg_slippage_bps)
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        EventSourceKind, ExecutionSnapshot, NormalizedEvent, PoolId, PoolSnapshotUpdate, PoolVenue,
        RouteId, SnapshotConfidence,
    };

    use super::{
        StrategyPlane,
        guards::GuardrailConfig,
        opportunity::OpportunityDecision,
        reasons::RejectionReason,
        route_registry::{ExecutionProtectionPolicy, RouteDefinition, RouteLeg, SwapSide},
    };
    use state::StatePlane;

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: Some(SOL_MINT.into()),
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
            min_trade_size: 10_000,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
            execution_protection: None,
        }
    }

    fn protected_route_definition() -> RouteDefinition {
        let mut route = route_definition();
        route.execution_protection = Some(ExecutionProtectionPolicy {
            tight_max_quote_slot_lag: 4,
            base_extra_buy_leg_slippage_bps: 50,
            failure_step_bps: 25,
            max_extra_buy_leg_slippage_bps: 150,
            recovery_success_count: 3,
        });
        route
    }

    fn ready_execution_snapshot(head_slot: u64) -> ExecutionSnapshot {
        ExecutionSnapshot {
            head_slot,
            rpc_slot: Some(head_slot),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(head_slot),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        }
    }

    fn apply_executable_snapshot(
        state: &mut StatePlane,
        sequence: u64,
        pool_id: &str,
        slot: u64,
        reserve_a: u64,
        reserve_b: u64,
    ) {
        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::Synthetic,
                sequence,
                slot,
                PoolSnapshotUpdate {
                    pool_id: pool_id.into(),
                    price_bps: ((u128::from(reserve_b) * 10_000u128) / u128::from(reserve_a))
                        as u64,
                    fee_bps: 4,
                    reserve_depth: reserve_a.min(reserve_b),
                    reserve_a: Some(reserve_a),
                    reserve_b: Some(reserve_b),
                    active_liquidity: Some(reserve_a.min(reserve_b)),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
                    confidence: SnapshotConfidence::Executable,
                    repair_pending: Some(false),
                    token_mint_a: SOL_MINT.into(),
                    token_mint_b: "USDC".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot,
                    write_version: 1,
                },
            ))
            .unwrap();
    }

    #[test]
    fn blocks_routes_that_are_not_warm() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());
        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::RouteNotWarm { .. },
                ..
            })
        ));
    }

    #[test]
    fn produces_structured_opportunity_candidate() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig {
            min_profit_quote_atoms: 10,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        apply_executable_snapshot(&mut state, 1, "pool-a", 10, 1_000_000, 700_000);
        apply_executable_snapshot(&mut state, 2, "pool-b", 11, 1_000_000, 1_300_000);

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        let candidate = outcome.best_candidate.expect("candidate");
        assert_eq!(candidate.route_id, route.route_id);
        assert!(candidate.expected_net_profit_quote_atoms > 0);
        assert_eq!(candidate.leg_quotes.len(), 2);
    }

    #[test]
    fn selects_larger_size_when_it_improves_net_profit_after_cost() {
        let mut route = route_definition();
        route.estimated_execution_cost_lamports = 5_000;
        let mut strategy = StrategyPlane::new(GuardrailConfig {
            min_profit_quote_atoms: 10,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        apply_executable_snapshot(&mut state, 1, "pool-a", 10, 1_000_000, 700_000);
        apply_executable_snapshot(&mut state, 2, "pool-b", 11, 1_000_000, 1_300_000);

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        let candidate = outcome.best_candidate.expect("candidate");
        assert_eq!(candidate.trade_size, route.max_trade_size);
        assert!(
            candidate.expected_gross_profit_quote_atoms
                > candidate.estimated_execution_cost_quote_atoms as i64
        );
        assert!(candidate.expected_net_profit_quote_atoms > 0);
    }

    #[test]
    fn rejects_route_when_execution_cost_exceeds_edge() {
        let mut route = route_definition();
        route.estimated_execution_cost_lamports = 40_000;
        let mut strategy = StrategyPlane::new(GuardrailConfig {
            min_profit_quote_atoms: 10,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        apply_executable_snapshot(&mut state, 1, "pool-a", 10, 1_000_000, 700_000);
        apply_executable_snapshot(&mut state, 2, "pool-b", 11, 1_000_000, 1_300_000);

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::ProfitBelowThreshold { .. },
                ..
            })
        ));
        assert!(outcome.best_candidate.is_none());
    }

    #[test]
    fn rejects_route_when_snapshot_is_not_executable() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-a".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
                    confidence: SnapshotConfidence::Verified,
                    repair_pending: Some(true),
                    token_mint_a: "SOL".into(),
                    token_mint_b: "USDC".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                2,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-b".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
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
            .unwrap();

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::PoolRepairPending { .. },
                ..
            })
        ));
        assert!(outcome.best_candidate.is_none());
    }

    #[test]
    fn rejects_route_when_snapshot_is_verified_without_active_repair() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-a".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
                    confidence: SnapshotConfidence::Verified,
                    repair_pending: Some(false),
                    token_mint_a: "SOL".into(),
                    token_mint_b: "USDC".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                2,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-b".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
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
            .unwrap();

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::PoolStateNotExecutable { .. },
                ..
            })
        ));
        assert!(outcome.best_candidate.is_none());
    }

    #[test]
    fn rejects_route_when_clmm_quote_model_is_not_executable() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-a".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: None,
                    reserve_b: None,
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: Some(1u128 << 64),
                    venue: PoolVenue::OrcaWhirlpool,
                    confidence: SnapshotConfidence::Executable,
                    repair_pending: Some(false),
                    token_mint_a: SOL_MINT.into(),
                    token_mint_b: "USDC".into(),
                    tick_spacing: 4,
                    current_tick_index: Some(12),
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                2,
                10,
                PoolSnapshotUpdate {
                    pool_id: "pool-b".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    venue: PoolVenue::OrcaSimplePool,
                    confidence: SnapshotConfidence::Executable,
                    repair_pending: Some(false),
                    token_mint_a: SOL_MINT.into(),
                    token_mint_b: "USDC".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();

        let execution = ready_execution_snapshot(state.latest_slot());
        let outcome =
            strategy.evaluate(&state, &execution, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::PoolQuoteModelNotExecutable { .. },
                ..
            })
        ));
        assert!(outcome.best_candidate.is_none());
    }

    #[test]
    fn execution_protection_state_steps_up_and_recovers() {
        let route = protected_route_definition();
        let route_id = route.route_id.clone();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route);

        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(50));

        strategy.record_execution_too_little_output(&route_id);
        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(75));

        strategy.record_execution_too_little_output(&route_id);
        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(100));

        strategy.record_execution_success(&route_id);
        strategy.record_execution_success(&route_id);
        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(100));

        strategy.record_execution_success(&route_id);
        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(75));
    }
}
