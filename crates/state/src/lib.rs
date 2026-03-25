pub mod account_store;
pub mod decoder;
pub mod dependency_graph;
pub mod execution_state;
pub mod pool_snapshots;
pub mod types;
pub mod warmup;

use std::time::SystemTime;

use detection::{AccountUpdate, MarketEvent, NormalizedEvent};
use thiserror::Error;

use crate::{
    account_store::AccountStore,
    decoder::DecoderRegistry,
    dependency_graph::DependencyGraph,
    execution_state::ExecutionState,
    pool_snapshots::PoolSnapshotStore,
    types::{
        AccountKey, AccountRecord, AccountUpdateStatus, ExecutionStateSnapshot, PoolId,
        StateApplyOutcome, WarmupStatus,
    },
    warmup::WarmupManager,
};

#[derive(Debug, Error)]
pub enum StateError {
    #[error("decoder {decoder_key} not registered")]
    UnknownDecoder { decoder_key: String },
}

#[derive(Debug)]
pub struct StatePlane {
    account_store: AccountStore,
    decoder_registry: DecoderRegistry,
    dependency_graph: DependencyGraph,
    pool_snapshots: PoolSnapshotStore,
    execution_state: ExecutionState,
    warmup: WarmupManager,
    latest_slot: u64,
    max_slot_lag: u64,
}

impl StatePlane {
    pub fn new(max_slot_lag: u64) -> Self {
        Self {
            account_store: AccountStore::default(),
            decoder_registry: DecoderRegistry::default(),
            dependency_graph: DependencyGraph::default(),
            pool_snapshots: PoolSnapshotStore::default(),
            execution_state: ExecutionState::default(),
            warmup: WarmupManager::default(),
            latest_slot: 0,
            max_slot_lag,
        }
    }

    pub fn decoder_registry_mut(&mut self) -> &mut DecoderRegistry {
        &mut self.decoder_registry
    }

    pub fn register_route(&mut self, route_id: crate::types::RouteId, required_pools: Vec<PoolId>) {
        for pool_id in &required_pools {
            self.dependency_graph
                .register_pool_route(pool_id.clone(), route_id.clone());
        }
        self.warmup.register_route(route_id, required_pools);
    }

    pub fn register_account_dependency(
        &mut self,
        account: AccountKey,
        pool_id: PoolId,
        decoder_key: impl Into<String>,
    ) {
        self.dependency_graph
            .register_account_pool(account, pool_id, decoder_key);
    }

    pub fn execution_state(&self) -> ExecutionStateSnapshot {
        self.execution_state.snapshot(self.latest_slot)
    }

    pub fn execution_state_mut(&mut self) -> &mut ExecutionState {
        &mut self.execution_state
    }

    pub fn latest_slot(&self) -> u64 {
        self.latest_slot
    }

    pub fn set_latest_slot(&mut self, slot: u64) {
        self.latest_slot = self.latest_slot.max(slot);
    }

    pub fn route_warmup_status(&self, route_id: &crate::types::RouteId) -> WarmupStatus {
        self.warmup.status(route_id)
    }

    pub fn route_count(&self) -> usize {
        self.warmup.route_count()
    }

    pub fn ready_route_count(&self) -> usize {
        self.warmup.ready_route_count()
    }

    pub fn route_pool_ids(&self, route_id: &crate::types::RouteId) -> &[PoolId] {
        self.dependency_graph.route_pools(route_id)
    }

    pub fn pool_snapshot(&self, pool_id: &PoolId) -> Option<&crate::types::PoolSnapshot> {
        self.pool_snapshots.get(pool_id)
    }

    pub fn route_state(&self, route_id: &crate::types::RouteId) -> crate::types::RouteState {
        self.warmup.route_state(route_id)
    }

    pub fn apply_event(
        &mut self,
        event: &NormalizedEvent,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        match &event.payload {
            MarketEvent::AccountUpdate(update) => self.apply_account_update(update),
            MarketEvent::SlotBoundary(slot_boundary) => {
                self.latest_slot = self.latest_slot.max(slot_boundary.slot);
                self.pool_snapshots
                    .refresh_freshness(self.latest_slot, self.max_slot_lag);
                Ok(Some(StateApplyOutcome {
                    update_status: AccountUpdateStatus::Applied,
                    impacted_pools: Vec::new(),
                    impacted_routes: Vec::new(),
                    refreshed_snapshots: 0,
                    latest_slot: self.latest_slot,
                }))
            }
            MarketEvent::Heartbeat(heartbeat) => {
                self.latest_slot = self.latest_slot.max(heartbeat.slot);
                Ok(None)
            }
        }
    }

    fn apply_account_update(
        &mut self,
        update: &AccountUpdate,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        self.latest_slot = self.latest_slot.max(update.slot);
        let record = AccountRecord {
            key: AccountKey(update.pubkey.clone()),
            owner: update.owner.clone(),
            lamports: update.lamports,
            data: update.data.clone(),
            slot: update.slot,
            write_version: update.write_version,
            observed_at: SystemTime::now(),
        };
        let update_status = self.account_store.upsert(record.clone());
        if update_status == AccountUpdateStatus::StaleRejected {
            return Ok(Some(StateApplyOutcome {
                update_status,
                impacted_pools: Vec::new(),
                impacted_routes: Vec::new(),
                refreshed_snapshots: 0,
                latest_slot: self.latest_slot,
            }));
        }

        let dependencies = self
            .dependency_graph
            .account_dependencies(&record.key)
            .to_vec();
        let mut impacted_pools = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            let decoder = self
                .decoder_registry
                .get(&dependency.decoder_key)
                .ok_or_else(|| StateError::UnknownDecoder {
                    decoder_key: dependency.decoder_key.clone(),
                })?;
            if let crate::types::DecodedAccount::PoolState(mut snapshot) =
                decoder.decode(&dependency.pool_id, &record, self.latest_slot)
            {
                snapshot.freshness = crate::types::FreshnessState::at(
                    self.latest_slot,
                    snapshot.last_update_slot,
                    self.max_slot_lag,
                );
                self.pool_snapshots.upsert(snapshot);
                impacted_pools.push(dependency.pool_id);
            }
        }

        let impacted_routes = self
            .dependency_graph
            .impacted_routes_for_pools(&impacted_pools);
        for route_id in &impacted_routes {
            self.warmup
                .refresh_route(route_id, &self.pool_snapshots, self.latest_slot);
        }

        Ok(Some(StateApplyOutcome {
            update_status,
            refreshed_snapshots: impacted_pools.len(),
            impacted_pools,
            impacted_routes,
            latest_slot: self.latest_slot,
        }))
    }
}

#[cfg(test)]
mod tests {
    use detection::{AccountUpdate, EventSourceKind, NormalizedEvent};

    use super::StatePlane;
    use crate::{
        decoder::PoolPriceAccountDecoder,
        types::{AccountKey, PoolId, RouteId, WarmupStatus},
    };

    fn encode_pool(price_bps: u64, fee_bps: u16, reserve_depth: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&price_bps.to_le_bytes());
        data.extend_from_slice(&fee_bps.to_le_bytes());
        data.extend_from_slice(&reserve_depth.to_le_bytes());
        data
    }

    #[test]
    fn route_becomes_ready_after_all_required_pools_arrive() {
        let route_id = RouteId("route-a".into());
        let mut plane = StatePlane::new(2);
        plane
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        plane.register_route(
            route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        plane.register_account_dependency(
            AccountKey("acct-a".into()),
            PoolId("pool-a".into()),
            "pool-price-v1",
        );
        plane.register_account_dependency(
            AccountKey("acct-b".into()),
            PoolId("pool-b".into()),
            "pool-price-v1",
        );

        let first = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            1,
            10,
            AccountUpdate {
                pubkey: "acct-a".into(),
                owner: "pool".into(),
                lamports: 0,
                data: encode_pool(10_100, 4, 100_000),
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
                owner: "pool".into(),
                lamports: 0,
                data: encode_pool(10_050, 4, 100_000),
                slot: 11,
                write_version: 1,
            },
        );

        plane.apply_event(&first).unwrap();
        assert_eq!(plane.route_warmup_status(&route_id), WarmupStatus::Warming);
        let outcome = plane.apply_event(&second).unwrap().unwrap();
        assert_eq!(plane.route_warmup_status(&route_id), WarmupStatus::Ready);
        assert_eq!(outcome.impacted_routes, vec![route_id]);
    }
}
