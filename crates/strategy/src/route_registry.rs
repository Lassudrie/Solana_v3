use std::collections::HashMap;

use state::types::{PoolId, RouteId};

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
    pub min_trade_size: u64,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    pub size_ladder: Vec<u64>,
    pub estimated_execution_cost_lamports: u64,
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
