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
use route_registry::{JitoTipPolicy, RouteDefinition, RouteKind, RouteRegistry, StrategySizingConfig};
use selector::OpportunitySelector;
use state::StatePlane;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RouteExecutionProtectionState {
    current_extra_buy_leg_slippage_bps: u16,
    consecutive_successes: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RouteExecutionSizingState {
    pub landing_rate_bps: u16,
    pub expected_shortfall_bps: u16,
    pub max_trade_size: u64,
}

#[derive(Debug)]
pub struct StrategyPlane {
    registry: RouteRegistry,
    selector: OpportunitySelector,
    sizing: StrategySizingConfig,
    route_eval_worker_count: usize,
    execution_protection: HashMap<RouteId, RouteExecutionProtectionState>,
    execution_sizing: HashMap<RouteId, RouteExecutionSizingState>,
}

impl StrategyPlane {
    pub fn new(
        guardrails: GuardrailConfig,
        sizing: StrategySizingConfig,
        jito_tip_policy: JitoTipPolicy,
    ) -> Self {
        Self::with_parallelism(guardrails, sizing, jito_tip_policy, 1)
    }

    pub fn with_parallelism(
        guardrails: GuardrailConfig,
        sizing: StrategySizingConfig,
        jito_tip_policy: JitoTipPolicy,
        route_eval_worker_count: usize,
    ) -> Self {
        Self {
            registry: RouteRegistry::default(),
            selector: OpportunitySelector::with_jito_tip_policy(
                LocalTwoLegQuoteEngine,
                GuardrailSet::new(guardrails),
                jito_tip_policy,
            ),
            sizing,
            route_eval_worker_count: route_eval_worker_count.max(1),
            execution_protection: HashMap::new(),
            execution_sizing: HashMap::new(),
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
        self.execution_sizing.insert(
            route.route_id.clone(),
            RouteExecutionSizingState {
                landing_rate_bps: route.sizing.base_landing_rate_bps,
                expected_shortfall_bps: route.sizing.base_expected_shortfall_bps,
                max_trade_size: route.max_trade_size.max(1),
            },
        );
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
            &self.sizing,
            &route_execution_buffers,
            &self.execution_sizing,
            self.route_eval_worker_count,
        )
    }

    pub fn has_impacted_triangular_route(&self, impacted_routes: &[RouteId]) -> bool {
        impacted_routes.iter().any(|route_id| {
            self.registry
                .get(route_id)
                .is_some_and(|route| route.kind == RouteKind::Triangular)
        })
    }

    pub fn route_metadata(&self, route_id: &RouteId) -> Option<(RouteKind, usize, u64)> {
        self.registry
            .get(route_id)
            .map(|route| (route.kind, route.leg_count(), route.max_quote_slot_lag))
    }

    pub fn record_execution_too_little_output(&mut self, route_id: &RouteId) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        if let (Some(policy), Some(state)) = (
            route.execution_protection.as_ref(),
            self.execution_protection.get_mut(route_id),
        ) {
            state.current_extra_buy_leg_slippage_bps = state
                .current_extra_buy_leg_slippage_bps
                .saturating_add(policy.failure_step_bps)
                .min(policy.max_extra_buy_leg_slippage_bps);
            state.consecutive_successes = 0;
        }

        if let Some(state) = self.execution_sizing.get_mut(route_id) {
            state.landing_rate_bps =
                ewma_bps(state.landing_rate_bps, 0, self.sizing.ewma_alpha_bps);
            let target = state
                .expected_shortfall_bps
                .saturating_add(self.sizing.too_little_output_shortfall_step_bps)
                .min(route.sizing.max_expected_shortfall_bps);
            state.expected_shortfall_bps = ewma_bps(
                state.expected_shortfall_bps,
                target,
                self.sizing.ewma_alpha_bps,
            );
        }
    }

    pub fn record_execution_success(&mut self, route_id: &RouteId) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        if let (Some(policy), Some(state)) = (
            route.execution_protection.as_ref(),
            self.execution_protection.get_mut(route_id),
        ) {
            state.consecutive_successes = state.consecutive_successes.saturating_add(1);
            if state.consecutive_successes >= policy.recovery_success_count.max(1) {
                state.current_extra_buy_leg_slippage_bps = state
                    .current_extra_buy_leg_slippage_bps
                    .saturating_sub(policy.failure_step_bps)
                    .max(policy.base_extra_buy_leg_slippage_bps);
                state.consecutive_successes = 0;
            }
        }

        if let Some(state) = self.execution_sizing.get_mut(route_id) {
            state.landing_rate_bps =
                ewma_bps(state.landing_rate_bps, 10_000, self.sizing.ewma_alpha_bps);
            state.max_trade_size = route.max_trade_size.max(1);
            state.expected_shortfall_bps = ewma_bps(
                state.expected_shortfall_bps,
                route.sizing.base_expected_shortfall_bps,
                self.sizing.ewma_alpha_bps,
            );
        }
    }

    pub fn record_execution_amount_in_above_maximum(&mut self, route_id: &RouteId) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        if let (Some(policy), Some(state)) = (
            route.execution_protection.as_ref(),
            self.execution_protection.get_mut(route_id),
        ) {
            state.current_extra_buy_leg_slippage_bps = state
                .current_extra_buy_leg_slippage_bps
                .saturating_add(policy.failure_step_bps)
                .min(policy.max_extra_buy_leg_slippage_bps);
            state.consecutive_successes = 0;
        }
        let Some(state) = self.execution_sizing.get_mut(route_id) else {
            return;
        };
        state.landing_rate_bps = ewma_bps(state.landing_rate_bps, 0, self.sizing.ewma_alpha_bps);
        let minimum_trade_size = route.min_trade_size.max(1);
        let route_max_trade_size = route.max_trade_size.max(1);
        let capped_max_trade_size = (state.max_trade_size / 2).max(minimum_trade_size);
        state.max_trade_size = state
            .max_trade_size
            .min(route_max_trade_size)
            .min(capped_max_trade_size);
    }

    pub fn record_submit_rejected(&mut self, route_id: &RouteId) {
        self.record_execution_failure(route_id);
    }

    pub fn record_execution_failure(&mut self, route_id: &RouteId) {
        let Some(state) = self.execution_sizing.get_mut(route_id) else {
            return;
        };
        state.landing_rate_bps = ewma_bps(state.landing_rate_bps, 0, self.sizing.ewma_alpha_bps);
    }

    pub fn record_realized_execution(
        &mut self,
        route_id: &RouteId,
        expected_pnl_quote_atoms: Option<i64>,
        realized_pnl_quote_atoms: Option<i64>,
    ) {
        let Some(route) = self.registry.get(route_id) else {
            return;
        };
        let Some(state) = self.execution_sizing.get_mut(route_id) else {
            return;
        };
        let (Some(expected), Some(realized)) = (expected_pnl_quote_atoms, realized_pnl_quote_atoms)
        else {
            return;
        };
        if expected <= 0 {
            return;
        }

        let expected = i128::from(expected);
        let realized = i128::from(realized);
        let shortfall_quote_atoms = (expected - realized).max(0);
        let shortfall_bps = shortfall_quote_atoms
            .saturating_mul(10_000)
            .checked_div(expected)
            .unwrap_or_default()
            .clamp(
                i128::from(route.sizing.base_expected_shortfall_bps),
                i128::from(route.sizing.max_expected_shortfall_bps),
            ) as u16;
        state.expected_shortfall_bps = ewma_bps(
            state.expected_shortfall_bps,
            shortfall_bps,
            self.sizing.ewma_alpha_bps,
        );

        if realized <= 0 {
            let minimum_trade_size = route.min_trade_size.max(1);
            let route_max_trade_size = route.max_trade_size.max(1);
            let capped_max_trade_size = (state.max_trade_size / 2).max(minimum_trade_size);
            state.max_trade_size = state
                .max_trade_size
                .min(route_max_trade_size)
                .min(capped_max_trade_size);
        }
    }

    pub fn active_execution_buffer_bps(&self, route_id: &RouteId) -> Option<u16> {
        self.execution_protection
            .get(route_id)
            .map(|state| state.current_extra_buy_leg_slippage_bps)
    }

    pub fn route_profit_mint(&self, route_id: &RouteId) -> Option<String> {
        self.registry
            .get(route_id)
            .map(|route| route.output_mint.clone())
    }
}

fn ewma_bps(current: u16, target: u16, alpha_bps: u16) -> u16 {
    let alpha = u32::from(alpha_bps.min(10_000));
    let keep = 10_000u32.saturating_sub(alpha);
    let next = u32::from(current)
        .saturating_mul(keep)
        .saturating_add(u32::from(target).saturating_mul(alpha))
        .saturating_add(9_999)
        / 10_000;
    next.min(u32::from(u16::MAX)) as u16
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
        route_registry::{
            ExecutionProtectionPolicy, JitoTipMode, JitoTipPolicy, RouteDefinition, RouteKind,
            RouteLeg, RouteSizingPolicy, SizingMode, StrategySizingConfig, SwapSide,
        },
    };
    use state::StatePlane;

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    fn sizing_config() -> StrategySizingConfig {
        StrategySizingConfig {
            mode: SizingMode::Legacy,
            fixed_trade_size: false,
            min_trade_floor_sol_lamports: 0,
            base_landing_rate_bps: 8_500,
            ewma_alpha_bps: 2_000,
            base_expected_shortfall_bps: 75,
            max_expected_shortfall_bps: 500,
            too_little_output_shortfall_step_bps: 75,
            inflight_penalty_bps_per_submission: 25,
            max_inflight_penalty_bps: 1_500,
            blockhash_penalty_bps_per_slot: 10,
            max_blockhash_penalty_bps: 1_000,
            quote_age_penalty_bps_per_slot: 15,
            max_quote_age_penalty_bps: 750,
            tick_cross_penalty_bps_per_tick: 20,
            max_tick_cross_penalty_bps: 1_000,
            max_reserve_usage_penalty_bps: 1_250,
        }
    }

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            kind: RouteKind::TwoLeg,
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            input_source_account: None,
            base_mint: Some(SOL_MINT.into()),
            quote_mint: Some("USDC".into()),
            sol_quote_conversion_pool_id: Some(PoolId("pool-a".into())),
            min_profit_quote_atoms: None,
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_mint: "USDC".into(),
                    output_mint: SOL_MINT.into(),
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_mint: SOL_MINT.into(),
                    output_mint: "USDC".into(),
                    fee_bps: None,
                },
            ]
            .into(),
            max_quote_slot_lag: 32,
            min_trade_size: 10_000,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            default_jito_tip_lamports: 0,
            estimated_execution_cost_lamports: 0,
            sizing: RouteSizingPolicy {
                mode: SizingMode::Legacy,
                min_trade_floor_sol_lamports: 0,
                base_landing_rate_bps: 8_500,
                base_expected_shortfall_bps: 75,
                max_expected_shortfall_bps: 500,
            },
            execution_protection: None,
        }
    }

    fn test_jito_tip_policy() -> JitoTipPolicy {
        JitoTipPolicy {
            mode: JitoTipMode::Fixed,
            share_bps_of_expected_net_profit: 0,
            min_lamports: 0,
            max_lamports: 0,
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
            source_token_balances: std::collections::HashMap::new(),
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig {
                min_profit_quote_atoms: 10,
                min_profit_bps: 0,
                require_route_warm: true,
                max_inflight_submissions: 64,
                min_wallet_balance_lamports: 1,
                max_blockhash_slot_lag: 8,
            },
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig {
                min_profit_quote_atoms: 10,
                min_profit_bps: 0,
                require_route_warm: true,
                max_inflight_submissions: 64,
                min_wallet_balance_lamports: 1,
                max_blockhash_slot_lag: 8,
            },
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig {
                min_profit_quote_atoms: 10,
                min_profit_bps: 0,
                require_route_warm: true,
                max_inflight_submissions: 64,
                min_wallet_balance_lamports: 1,
                max_blockhash_slot_lag: 8,
            },
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
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
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
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

    #[test]
    fn amount_in_above_maximum_steps_up_buffer_and_caps_trade_size() {
        let route = protected_route_definition();
        let route_id = route.route_id.clone();
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
        strategy.register_route(route.clone());

        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(50));

        strategy.record_execution_amount_in_above_maximum(&route_id);

        assert_eq!(strategy.active_execution_buffer_bps(&route_id), Some(75));

        let sizing = strategy
            .execution_sizing
            .get(&route_id)
            .copied()
            .expect("route sizing state");
        assert_eq!(sizing.max_trade_size, route.max_trade_size / 2);
        assert!(sizing.landing_rate_bps < route.sizing.base_landing_rate_bps);
    }

    #[test]
    fn realized_underperformance_increases_expected_shortfall() {
        let route = route_definition();
        let route_id = route.route_id.clone();
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
        strategy.register_route(route.clone());

        strategy.record_execution_success(&route_id);
        strategy.record_realized_execution(&route_id, Some(100), Some(50));

        let sizing = strategy
            .execution_sizing
            .get(&route_id)
            .copied()
            .expect("route sizing state");
        assert_eq!(sizing.max_trade_size, route.max_trade_size);
        assert!(sizing.expected_shortfall_bps > route.sizing.base_expected_shortfall_bps);
    }

    #[test]
    fn realized_loss_reduces_route_max_trade_size() {
        let route = route_definition();
        let route_id = route.route_id.clone();
        let mut strategy = StrategyPlane::new(
            GuardrailConfig::default(),
            sizing_config(),
            test_jito_tip_policy(),
        );
        strategy.register_route(route.clone());

        strategy.record_execution_success(&route_id);
        strategy.record_realized_execution(&route_id, Some(100), Some(-10));

        let sizing = strategy
            .execution_sizing
            .get(&route_id)
            .copied()
            .expect("route sizing state");
        assert_eq!(sizing.max_trade_size, route.max_trade_size / 2);
        assert!(sizing.expected_shortfall_bps > route.sizing.base_expected_shortfall_bps);
    }
}
