use std::collections::HashMap;

use crate::types::{FreshnessState, PoolConfidence, PoolId, PoolSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SnapshotVersion {
    slot: u64,
    write_version: u64,
}

#[derive(Debug, Default)]
pub struct PoolSnapshotStore {
    snapshots: HashMap<PoolId, PoolSnapshot>,
    versions: HashMap<PoolId, SnapshotVersion>,
}

impl PoolSnapshotStore {
    pub fn upsert(&mut self, snapshot: PoolSnapshot) {
        self.versions.insert(
            snapshot.pool_id.clone(),
            SnapshotVersion {
                slot: snapshot.last_update_slot,
                write_version: 0,
            },
        );
        self.snapshots.insert(snapshot.pool_id.clone(), snapshot);
    }

    pub fn upsert_with_version(&mut self, snapshot: PoolSnapshot, write_version: u64) -> bool {
        let next_version = SnapshotVersion {
            slot: snapshot.last_update_slot,
            write_version,
        };
        if let Some(existing) = self.versions.get(&snapshot.pool_id) {
            if existing.slot > next_version.slot
                || (existing.slot == next_version.slot
                    && existing.write_version >= next_version.write_version)
            {
                return false;
            }
        }
        self.versions.insert(snapshot.pool_id.clone(), next_version);
        self.snapshots.insert(snapshot.pool_id.clone(), snapshot);
        true
    }

    pub fn get(&self, pool_id: &PoolId) -> Option<&PoolSnapshot> {
        self.snapshots.get(pool_id)
    }

    pub fn version(&self, pool_id: &PoolId) -> Option<(u64, u64)> {
        self.versions
            .get(pool_id)
            .map(|version| (version.slot, version.write_version))
    }

    pub fn invalidate(&mut self, pool_id: &PoolId, head_slot: u64, max_slot_lag: u64) -> bool {
        let Some(snapshot) = self.snapshots.get_mut(pool_id) else {
            return false;
        };
        let forced_slot_lag = max_slot_lag.saturating_add(1);
        snapshot.freshness = FreshnessState {
            head_slot,
            slot_lag: forced_slot_lag,
            is_stale: true,
        };
        snapshot.confidence = PoolConfidence::Invalid;
        snapshot.repair_pending = true;
        true
    }

    pub fn refresh_freshness(&mut self, head_slot: u64, max_slot_lag: u64) {
        for snapshot in self.snapshots.values_mut() {
            snapshot.freshness =
                FreshnessState::at(head_slot, snapshot.last_update_slot, max_slot_lag);
        }
    }
}
