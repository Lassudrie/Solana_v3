use std::collections::HashMap;

use crate::types::{ExecutablePoolState, PoolConfidence, PoolId};

#[derive(Debug, Default)]
pub struct ExecutablePoolStateStore {
    states: HashMap<PoolId, ExecutablePoolState>,
}

impl ExecutablePoolStateStore {
    pub fn get(&self, pool_id: &PoolId) -> Option<&ExecutablePoolState> {
        self.states.get(pool_id)
    }

    pub fn get_mut(&mut self, pool_id: &PoolId) -> Option<&mut ExecutablePoolState> {
        self.states.get_mut(pool_id)
    }

    pub fn upsert(&mut self, state: ExecutablePoolState) -> bool {
        let Some(existing) = self.states.get(state.pool_id()) else {
            self.states.insert(state.pool_id().clone(), state);
            return true;
        };

        if existing.last_update_slot() > state.last_update_slot() {
            return false;
        }
        if existing.last_update_slot() == state.last_update_slot()
            && existing.write_version() >= state.write_version()
        {
            return false;
        }

        self.states.insert(state.pool_id().clone(), state);
        true
    }

    pub fn invalidate(&mut self, pool_id: &PoolId, slot: u64, write_version: u64) -> bool {
        let Some(state) = self.states.get_mut(pool_id) else {
            return false;
        };
        if state.last_update_slot() > slot {
            return false;
        }
        if state.last_update_slot() == slot && state.write_version() >= write_version {
            return false;
        }
        state.set_confidence(PoolConfidence::Invalid, slot, write_version, None);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutablePoolStateStore;
    use crate::types::{
        ConstantProductPoolState, ExecutablePoolState, PoolConfidence, PoolId, PoolVenue,
    };

    fn exact_state(slot: u64, write_version: u64) -> ExecutablePoolState {
        ExecutablePoolState::OrcaSimplePool(ConstantProductPoolState {
            pool_id: PoolId("pool-a".into()),
            venue: PoolVenue::OrcaSimplePool,
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 10,
            reserve_b: 20,
            fee_bps: 30,
            last_update_slot: slot,
            write_version,
            last_verified_slot: slot,
            confidence: PoolConfidence::Exact,
            repair_pending: false,
        })
    }

    #[test]
    fn rejects_stale_updates() {
        let mut store = ExecutablePoolStateStore::default();
        assert!(store.upsert(exact_state(10, 2)));
        assert!(!store.upsert(exact_state(9, 3)));
        assert!(!store.upsert(exact_state(10, 2)));
    }

    #[test]
    fn invalidate_marks_state_invalid() {
        let mut store = ExecutablePoolStateStore::default();
        let pool_id = PoolId("pool-a".into());
        assert!(store.upsert(exact_state(10, 1)));
        assert!(store.invalidate(&pool_id, 11, 2));
        assert_eq!(
            store.get(&pool_id).unwrap().confidence(),
            PoolConfidence::Invalid
        );
    }
}
