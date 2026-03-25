use std::collections::HashMap;

use crate::types::{FreshnessState, PoolId, PoolSnapshot};

#[derive(Debug, Default)]
pub struct PoolSnapshotStore {
    snapshots: HashMap<PoolId, PoolSnapshot>,
}

impl PoolSnapshotStore {
    pub fn upsert(&mut self, snapshot: PoolSnapshot) {
        self.snapshots.insert(snapshot.pool_id.clone(), snapshot);
    }

    pub fn get(&self, pool_id: &PoolId) -> Option<&PoolSnapshot> {
        self.snapshots.get(pool_id)
    }

    pub fn refresh_freshness(&mut self, head_slot: u64, max_slot_lag: u64) {
        for snapshot in self.snapshots.values_mut() {
            snapshot.freshness =
                FreshnessState::at(head_slot, snapshot.last_update_slot, max_slot_lag);
        }
    }
}
