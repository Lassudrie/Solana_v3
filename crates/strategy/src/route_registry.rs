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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDefinition {
    pub route_id: RouteId,
    pub input_mint: String,
    pub output_mint: String,
    pub legs: [RouteLeg; 2],
    pub default_trade_size: u64,
    pub max_trade_size: u64,
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
