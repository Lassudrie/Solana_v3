use std::collections::HashMap;

pub use domain::quote_models::{
    ConcentratedQuoteModel, DirectionalConcentratedQuoteModel, InitializedTick,
    ORCA_WHIRLPOOL_TICK_ARRAY_SIZE, RAYDIUM_CLMM_TICK_ARRAY_SIZE, TickArrayWindow,
    derive_orca_tick_arrays, derive_raydium_tick_arrays, tick_array_end_index,
    tick_array_start_index,
};

use crate::types::PoolId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct QuoteModelVersion {
    slot: u64,
    write_version: u64,
}

#[derive(Debug, Default)]
pub struct ConcentratedQuoteModelStore {
    models: HashMap<PoolId, ConcentratedQuoteModel>,
    versions: HashMap<PoolId, QuoteModelVersion>,
}

impl ConcentratedQuoteModelStore {
    pub fn get(&self, pool_id: &PoolId) -> Option<&ConcentratedQuoteModel> {
        self.models.get(pool_id)
    }

    pub fn upsert(&mut self, model: ConcentratedQuoteModel) -> bool {
        let next_version = QuoteModelVersion {
            slot: model.last_update_slot,
            write_version: model.write_version,
        };
        if let Some(existing) = self.versions.get(&model.pool_id) {
            if existing.slot > next_version.slot
                || (existing.slot == next_version.slot
                    && existing.write_version >= next_version.write_version)
            {
                return false;
            }
        }
        self.versions.insert(model.pool_id.clone(), next_version);
        self.models.insert(model.pool_id.clone(), model);
        true
    }

    pub fn remove(&mut self, pool_id: &PoolId) {
        self.versions.remove(pool_id);
        self.models.remove(pool_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ORCA_WHIRLPOOL_TICK_ARRAY_SIZE, RAYDIUM_CLMM_TICK_ARRAY_SIZE, tick_array_end_index,
        tick_array_start_index,
    };

    #[test]
    fn start_index_uses_floor_division_for_negative_ticks() {
        assert_eq!(
            tick_array_start_index(-1, 64, ORCA_WHIRLPOOL_TICK_ARRAY_SIZE, 0),
            -5632
        );
        assert_eq!(
            tick_array_start_index(-1, 1, RAYDIUM_CLMM_TICK_ARRAY_SIZE, -1),
            -120
        );
    }

    #[test]
    fn end_index_matches_array_span() {
        assert_eq!(
            tick_array_end_index(0, 64, ORCA_WHIRLPOOL_TICK_ARRAY_SIZE),
            5568
        );
        assert_eq!(
            tick_array_end_index(-120, 1, RAYDIUM_CLMM_TICK_ARRAY_SIZE),
            -61
        );
    }
}
