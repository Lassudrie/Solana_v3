use std::collections::{HashMap, HashSet};

use crate::{
    pool_snapshots::PoolSnapshotStore,
    types::{PoolId, RouteId, RouteState, WarmupStatus},
};

#[derive(Debug, Default)]
pub struct WarmupManager {
    routes: HashMap<RouteId, RouteWarmupState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteWarmupState {
    required_pools: Vec<PoolId>,
    last_touched_slot: Option<u64>,
    status: WarmupStatus,
}

impl WarmupManager {
    pub fn register_route(&mut self, route_id: RouteId, required_pools: Vec<PoolId>) {
        self.routes.insert(
            route_id,
            RouteWarmupState {
                required_pools,
                last_touched_slot: None,
                status: WarmupStatus::Cold,
            },
        );
    }

    pub fn status(&self, route_id: &RouteId) -> WarmupStatus {
        self.routes
            .get(route_id)
            .map(|state| state.status)
            .unwrap_or(WarmupStatus::Cold)
    }

    pub fn route_state(&self, route_id: &RouteId) -> RouteState {
        let state = self.routes.get(route_id);
        RouteState {
            route_id: route_id.clone(),
            warmup_status: state
                .map(|entry| entry.status)
                .unwrap_or(WarmupStatus::Cold),
            last_touched_slot: state.and_then(|entry| entry.last_touched_slot),
        }
    }

    pub fn refresh_route(
        &mut self,
        route_id: &RouteId,
        snapshot_store: &PoolSnapshotStore,
        latest_slot: u64,
    ) {
        if let Some(route) = self.routes.get_mut(route_id) {
            let ready_pools = route
                .required_pools
                .iter()
                .filter(|pool_id| snapshot_store.get(pool_id).is_some())
                .count();
            route.last_touched_slot = Some(latest_slot);
            route.status = if ready_pools == 0 {
                WarmupStatus::Cold
            } else if ready_pools == route.required_pools.len() {
                WarmupStatus::Ready
            } else {
                WarmupStatus::Warming
            };
        }
    }

    pub fn register_routes_from_graph(&mut self, routes: Vec<(RouteId, Vec<PoolId>)>) {
        let mut seen = HashSet::new();
        for (route_id, pools) in routes {
            if seen.insert(route_id.clone()) {
                self.register_route(route_id, pools);
            }
        }
    }

    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub fn ready_route_count(&self) -> usize {
        self.routes
            .values()
            .filter(|route| route.status == WarmupStatus::Ready)
            .count()
    }
}
