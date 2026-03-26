pub mod guards;
pub mod opportunity;
pub mod quote;
pub mod reasons;
pub mod route_registry;
pub mod selector;

use guards::{GuardrailConfig, GuardrailSet};
use opportunity::SelectionOutcome;
use quote::LocalTwoLegQuoteEngine;
use route_registry::{RouteDefinition, RouteRegistry};
use selector::OpportunitySelector;
use state::{StatePlane, types::RouteId};

#[derive(Debug)]
pub struct StrategyPlane {
    registry: RouteRegistry,
    selector: OpportunitySelector,
}

impl StrategyPlane {
    pub fn new(guardrails: GuardrailConfig) -> Self {
        Self {
            registry: RouteRegistry::default(),
            selector: OpportunitySelector::new(
                LocalTwoLegQuoteEngine,
                GuardrailSet::new(guardrails),
            ),
        }
    }

    pub fn register_route(&mut self, route: RouteDefinition) {
        self.registry.register(route);
    }

    pub fn evaluate(
        &self,
        state: &StatePlane,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
    ) -> SelectionOutcome {
        self.selector
            .evaluate(&self.registry, state, impacted_routes, inflight_submissions)
    }
}

#[cfg(test)]
mod tests {
    use detection::{AccountUpdate, EventSourceKind, NormalizedEvent};

    use super::{
        StrategyPlane,
        guards::GuardrailConfig,
        opportunity::OpportunityDecision,
        reasons::RejectionReason,
        route_registry::{RouteDefinition, RouteLeg, SwapSide},
    };
    use state::{
        StatePlane,
        decoder::PoolPriceAccountDecoder,
        types::{AccountKey, PoolId, RouteId},
    };

    fn encode_pool(price_bps: u64, fee_bps: u16, reserve_depth: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&price_bps.to_le_bytes());
        data.extend_from_slice(&fee_bps.to_le_bytes());
        data.extend_from_slice(&reserve_depth.to_le_bytes());
        data
    }

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
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
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
        }
    }

    #[test]
    fn blocks_routes_that_are_not_warm() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());
        let mut state = StatePlane::new(2);
        state
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );

        let outcome = strategy.evaluate(&state, std::slice::from_ref(&route.route_id), 0);
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
            min_profit_lamports: 10,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        state.register_account_dependency(
            AccountKey("acct-a".into()),
            PoolId("pool-a".into()),
            "pool-price-v1",
        );
        state.register_account_dependency(
            AccountKey("acct-b".into()),
            PoolId("pool-b".into()),
            "pool-price-v1",
        );
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(12);
        state.execution_state_mut().set_blockhash("blockhash-1", 12);

        let first = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            1,
            10,
            AccountUpdate {
                pubkey: "acct-a".into(),
                owner: "owner".into(),
                lamports: 0,
                data: encode_pool(10_150, 4, 100_000),
                slot: 10,
                write_version: 1,
            },
        );
        let second = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            2,
            11,
            AccountUpdate {
                pubkey: "acct-b".into(),
                owner: "owner".into(),
                lamports: 0,
                data: encode_pool(10_080, 4, 100_000),
                slot: 11,
                write_version: 1,
            },
        );
        state.apply_event(&first).unwrap();
        state.apply_event(&second).unwrap();

        let outcome = strategy.evaluate(&state, std::slice::from_ref(&route.route_id), 0);
        let candidate = outcome.best_candidate.expect("candidate");
        assert_eq!(candidate.route_id, route.route_id);
        assert!(candidate.expected_net_profit > 0);
        assert_eq!(candidate.leg_quotes.len(), 2);
    }

    #[test]
    fn selects_larger_size_when_it_improves_net_profit_after_cost() {
        let mut route = route_definition();
        route.estimated_execution_cost_lamports = 5_000;
        let mut strategy = StrategyPlane::new(GuardrailConfig {
            min_profit_lamports: 10,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        state.register_account_dependency(
            AccountKey("acct-a".into()),
            PoolId("pool-a".into()),
            "pool-price-v1",
        );
        state.register_account_dependency(
            AccountKey("acct-b".into()),
            PoolId("pool-b".into()),
            "pool-price-v1",
        );
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(12);
        state.execution_state_mut().set_blockhash("blockhash-1", 12);

        state
            .apply_event(&NormalizedEvent::account_update(
                EventSourceKind::Synthetic,
                1,
                10,
                AccountUpdate {
                    pubkey: "acct-a".into(),
                    owner: "owner".into(),
                    lamports: 0,
                    data: encode_pool(12_000, 4, 100_000),
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        state
            .apply_event(&NormalizedEvent::account_update(
                EventSourceKind::Synthetic,
                2,
                11,
                AccountUpdate {
                    pubkey: "acct-b".into(),
                    owner: "owner".into(),
                    lamports: 0,
                    data: encode_pool(12_000, 4, 100_000),
                    slot: 11,
                    write_version: 1,
                },
            ))
            .unwrap();

        let outcome = strategy.evaluate(&state, std::slice::from_ref(&route.route_id), 0);
        let candidate = outcome.best_candidate.expect("candidate");
        assert_eq!(candidate.trade_size, route.max_trade_size);
        assert!(
            candidate.expected_gross_profit > candidate.estimated_execution_cost_lamports as i64
        );
        assert!(candidate.expected_net_profit > 0);
    }

    #[test]
    fn rejects_route_when_execution_cost_exceeds_edge() {
        let mut route = route_definition();
        route.estimated_execution_cost_lamports = 10_000;
        let mut strategy = StrategyPlane::new(GuardrailConfig {
            min_profit_lamports: 10,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 8,
        });
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        state.register_account_dependency(
            AccountKey("acct-a".into()),
            PoolId("pool-a".into()),
            "pool-price-v1",
        );
        state.register_account_dependency(
            AccountKey("acct-b".into()),
            PoolId("pool-b".into()),
            "pool-price-v1",
        );
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(12);
        state.execution_state_mut().set_blockhash("blockhash-1", 12);

        state
            .apply_event(&NormalizedEvent::account_update(
                EventSourceKind::Synthetic,
                1,
                10,
                AccountUpdate {
                    pubkey: "acct-a".into(),
                    owner: "owner".into(),
                    lamports: 0,
                    data: encode_pool(10_150, 4, 100_000),
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        state
            .apply_event(&NormalizedEvent::account_update(
                EventSourceKind::Synthetic,
                2,
                11,
                AccountUpdate {
                    pubkey: "acct-b".into(),
                    owner: "owner".into(),
                    lamports: 0,
                    data: encode_pool(10_080, 4, 100_000),
                    slot: 11,
                    write_version: 1,
                },
            ))
            .unwrap();

        let outcome = strategy.evaluate(&state, std::slice::from_ref(&route.route_id), 0);
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
    fn rejects_route_when_snapshot_is_not_exact() {
        let route = route_definition();
        let mut strategy = StrategyPlane::new(GuardrailConfig::default());
        strategy.register_route(route.clone());

        let mut state = StatePlane::new(2);
        state.register_route(
            route.route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        state
            .execution_state_mut()
            .set_wallet_state(1_000_000, true);
        state.execution_state_mut().set_rpc_slot(12);
        state.execution_state_mut().set_blockhash("blockhash-1", 12);

        state
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                10,
                detection::PoolSnapshotUpdate {
                    pool_id: "pool-a".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    exact: Some(false),
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
                detection::PoolSnapshotUpdate {
                    pool_id: "pool-b".into(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 100_000,
                    reserve_a: Some(100_000),
                    reserve_b: Some(100_000),
                    active_liquidity: Some(100_000),
                    sqrt_price_x64: None,
                    exact: Some(true),
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

        let outcome = strategy.evaluate(&state, std::slice::from_ref(&route.route_id), 0);
        assert!(matches!(
            outcome.decisions.first(),
            Some(OpportunityDecision::Rejected {
                reason: RejectionReason::PoolStateNotExact { .. },
                ..
            })
        ));
        assert!(outcome.best_candidate.is_none());
    }
}
