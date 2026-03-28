use std::collections::HashMap;

use domain::{PoolId, RouteId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SizingMode {
    #[default]
    Legacy,
    EvShadow,
    EvLive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategySizingConfig {
    pub mode: SizingMode,
    pub min_trade_floor_sol_lamports: u64,
    pub base_landing_rate_bps: u16,
    pub ewma_alpha_bps: u16,
    pub base_expected_shortfall_bps: u16,
    pub max_expected_shortfall_bps: u16,
    pub too_little_output_shortfall_step_bps: u16,
    pub inflight_penalty_bps_per_submission: u16,
    pub max_inflight_penalty_bps: u16,
    pub blockhash_penalty_bps_per_slot: u16,
    pub max_blockhash_penalty_bps: u16,
    pub max_reserve_usage_penalty_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSizingPolicy {
    pub mode: SizingMode,
    pub min_trade_floor_sol_lamports: u64,
    pub base_landing_rate_bps: u16,
    pub base_expected_shortfall_bps: u16,
    pub max_expected_shortfall_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapSide {
    BuyBase,
    SellBase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteLeg {
    pub venue: String,
    pub pool_id: PoolId,
    pub side: SwapSide,
    pub fee_bps: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProtectionPolicy {
    pub tight_max_quote_slot_lag: u64,
    pub base_extra_buy_leg_slippage_bps: u16,
    pub failure_step_bps: u16,
    pub max_extra_buy_leg_slippage_bps: u16,
    pub recovery_success_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDefinition {
    pub route_id: RouteId,
    pub input_mint: String,
    pub output_mint: String,
    pub base_mint: Option<String>,
    pub quote_mint: Option<String>,
    pub sol_quote_conversion_pool_id: Option<PoolId>,
    pub legs: [RouteLeg; 2],
    pub max_quote_slot_lag: u64,
    pub min_trade_size: u64,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    pub size_ladder: Vec<u64>,
    pub estimated_execution_cost_lamports: u64,
    pub sizing: RouteSizingPolicy,
    pub execution_protection: Option<ExecutionProtectionPolicy>,
}

#[derive(Debug, Default)]
pub struct RouteRegistry {
    routes: HashMap<RouteId, RouteDefinition>,
}

impl RouteRegistry {
    pub fn register(&mut self, route: RouteDefinition) {
        self.routes.insert(route.route_id.clone(), route);
    }

    pub fn get(&self, route_id: &RouteId) -> Option<&RouteDefinition> {
        self.routes.get(route_id)
    }
}
