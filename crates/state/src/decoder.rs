use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::types::{AccountRecord, DecodedAccount, FreshnessState, PoolId, PoolSnapshot};

pub trait AccountDecoder: Send + Sync {
    fn decoder_key(&self) -> &'static str;
    fn decode(&self, pool_id: &PoolId, record: &AccountRecord, head_slot: u64) -> DecodedAccount;
}

#[derive(Default)]
pub struct DecoderRegistry {
    decoders: HashMap<String, Arc<dyn AccountDecoder>>,
}

impl std::fmt::Debug for DecoderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecoderRegistry")
            .field("decoder_keys", &self.decoders.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DecoderRegistry {
    pub fn register<D>(&mut self, decoder: D)
    where
        D: AccountDecoder + 'static,
    {
        self.decoders
            .insert(decoder.decoder_key().to_string(), Arc::new(decoder));
    }

    pub fn get(&self, decoder_key: &str) -> Option<&Arc<dyn AccountDecoder>> {
        self.decoders.get(decoder_key)
    }
}

#[derive(Debug, Default)]
pub struct PoolPriceAccountDecoder;

impl AccountDecoder for PoolPriceAccountDecoder {
    fn decoder_key(&self) -> &'static str {
        "pool-price-v1"
    }

    fn decode(&self, pool_id: &PoolId, record: &AccountRecord, head_slot: u64) -> DecodedAccount {
        let price_bps = read_u64(&record.data, 0).unwrap_or(10_000);
        let fee_bps = read_u16(&record.data, 8).unwrap_or(0);
        let reserve_depth = read_u64(&record.data, 10).unwrap_or(0);
        DecodedAccount::PoolState(PoolSnapshot {
            pool_id: pool_id.clone(),
            price_bps,
            fee_bps,
            reserve_depth,
            last_update_slot: record.slot,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(head_slot, record.slot, 0),
        })
    }
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    let bytes = data.get(offset..offset + 8)?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    Some(u64::from_le_bytes(buf))
}

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    let bytes = data.get(offset..offset + 2)?;
    let mut buf = [0u8; 2];
    buf.copy_from_slice(bytes);
    Some(u16::from_le_bytes(buf))
}
