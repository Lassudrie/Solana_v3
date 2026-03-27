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

    pub fn invalidate(
        &mut self,
        pool_id: &PoolId,
        slot: u64,
        write_version: u64,
        head_slot: u64,
        max_slot_lag: u64,
    ) -> bool {
        let next_version = SnapshotVersion {
            slot,
            write_version,
        };
        if let Some(existing) = self.versions.get(pool_id) {
            if existing.slot > next_version.slot
                || (existing.slot == next_version.slot
                    && existing.write_version >= next_version.write_version)
            {
                return false;
            }
        }
        self.versions.insert(pool_id.clone(), next_version);

        let Some(snapshot) = self.snapshots.get_mut(pool_id) else {
            return true;
        };
        let forced_slot_lag = max_slot_lag.saturating_add(1);
        snapshot.freshness = FreshnessState {
            head_slot: head_slot.max(slot),
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::types::{LiquidityModel, PoolId, PoolSnapshot};

    use super::PoolSnapshotStore;

    fn snapshot(slot: u64) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 1_000,
            reserve_a: Some(1_000),
            reserve_b: Some(1_000),
            active_liquidity: 1_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: crate::types::PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: LiquidityModel::ConstantProduct,
            slippage_factor_bps: 100,
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: slot,
            derived_at: SystemTime::now(),
            freshness: crate::types::FreshnessState::at(slot, slot, 4),
        }
    }

    #[test]
    fn invalidate_rejects_stale_version_and_records_newer_one() {
        let pool_id = PoolId("pool-a".into());
        let mut store = PoolSnapshotStore::default();
        assert!(store.upsert_with_version(snapshot(100), 5));

        assert!(!store.invalidate(&pool_id, 99, 6, 100, 4));
        assert!(store.invalidate(&pool_id, 100, 6, 100, 4));
        assert_eq!(store.version(&pool_id), Some((100, 6)));
        assert_eq!(
            store.get(&pool_id).expect("snapshot").confidence,
            crate::types::PoolConfidence::Invalid
        );
    }

    #[test]
    fn invalidate_without_snapshot_still_records_version_barrier() {
        let pool_id = PoolId("pool-a".into());
        let mut store = PoolSnapshotStore::default();

        assert!(store.invalidate(&pool_id, 100, 5, 100, 4));
        assert_eq!(store.version(&pool_id), Some((100, 5)));
        assert!(store.get(&pool_id).is_none());
        assert!(!store.upsert_with_version(snapshot(99), 4));
        assert!(store.upsert_with_version(snapshot(100), 6));
    }
}
