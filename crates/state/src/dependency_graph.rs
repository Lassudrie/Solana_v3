use std::collections::{HashMap, HashSet};

use crate::types::{AccountKey, PoolId, RouteId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDependency {
    pub pool_id: PoolId,
    pub decoder_key: String,
}

#[derive(Debug, Default)]
pub struct DependencyGraph {
    account_to_pools: HashMap<AccountKey, Vec<AccountDependency>>,
    pool_to_routes: HashMap<PoolId, Vec<RouteId>>,
    route_to_pools: HashMap<RouteId, Vec<PoolId>>,
}

impl DependencyGraph {
    pub fn register_account_pool(
        &mut self,
        account: AccountKey,
        pool_id: PoolId,
        decoder_key: impl Into<String>,
    ) {
        self.account_to_pools
            .entry(account)
            .or_default()
            .push(AccountDependency {
                pool_id,
                decoder_key: decoder_key.into(),
            });
    }

    pub fn register_pool_route(&mut self, pool_id: PoolId, route_id: RouteId) {
        self.pool_to_routes
            .entry(pool_id.clone())
            .or_default()
            .push(route_id.clone());
        self.route_to_pools
            .entry(route_id)
            .or_default()
            .push(pool_id);
    }

    pub fn account_dependencies(&self, account: &AccountKey) -> &[AccountDependency] {
        self.account_to_pools
            .get(account)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn impacted_routes_for_pools(&self, pools: &[PoolId]) -> Vec<RouteId> {
        let mut deduped = HashSet::new();
        let mut routes = Vec::new();
        for pool_id in pools {
            if let Some(route_ids) = self.pool_to_routes.get(pool_id) {
                for route_id in route_ids {
                    if deduped.insert(route_id.clone()) {
                        routes.push(route_id.clone());
                    }
                }
            }
        }
        routes
    }

    pub fn route_pools(&self, route_id: &RouteId) -> &[PoolId] {
        self.route_to_pools
            .get(route_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::DependencyGraph;
    use crate::types::{AccountKey, PoolId, RouteId};

    #[test]
    fn propagates_account_to_pool_to_route() {
        let account = AccountKey("acct-a".into());
        let pool = PoolId("pool-a".into());
        let route = RouteId("route-a".into());
        let mut graph = DependencyGraph::default();

        graph.register_account_pool(account.clone(), pool.clone(), "pool-price-v1");
        graph.register_pool_route(pool.clone(), route.clone());

        let deps = graph.account_dependencies(&account);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].pool_id, pool);
        assert_eq!(
            graph.impacted_routes_for_pools(&[PoolId("pool-a".into())]),
            vec![route]
        );
    }
}
