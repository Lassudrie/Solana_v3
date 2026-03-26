use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::types::{
    AccountRecord, DecodedAccount, FreshnessState, LiquidityModel, PoolId, PoolSnapshot,
};

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
            reserve_a: None,
            reserve_b: None,
            active_liquidity: reserve_depth,
            sqrt_price_x64: None,
            venue: None,
            confidence: crate::types::PoolConfidence::Exact,
            repair_pending: false,
            liquidity_model: LiquidityModel::ConstantProduct,
            slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                LiquidityModel::ConstantProduct,
                0,
            ),
            token_mint_a: String::new(),
            token_mint_b: String::new(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: record.slot,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(head_slot, record.slot, 0),
        })
    }
}

#[derive(Debug, Default)]
pub struct OrcaWhirlpoolAccountDecoder;

impl AccountDecoder for OrcaWhirlpoolAccountDecoder {
    fn decoder_key(&self) -> &'static str {
        "orca-whirlpool-v1"
    }

    fn decode(&self, pool_id: &PoolId, record: &AccountRecord, head_slot: u64) -> DecodedAccount {
        if record.data.len() < 245 {
            return DecodedAccount::Ignored;
        }
        let tick_spacing = read_u16(&record.data, 41).unwrap_or_default();
        let fee_bps = read_u16(&record.data, 45)
            .map(|value| value / 100)
            .unwrap_or_default();
        let liquidity = read_u128(&record.data, 49).unwrap_or_default();
        let sqrt_price = read_u128(&record.data, 65).unwrap_or_default();
        let current_tick_index = read_i32(&record.data, 81);
        let token_mint_a = read_pubkey(&record.data, 101).unwrap_or_default();
        let token_mint_b = read_pubkey(&record.data, 181).unwrap_or_default();
        let active_liquidity = liquidity.min(u64::MAX as u128) as u64;
        let liquidity_model = LiquidityModel::ConcentratedLiquidity;

        DecodedAccount::PoolState(PoolSnapshot {
            pool_id: pool_id.clone(),
            price_bps: sqrt_price_x64_to_price_bps(sqrt_price),
            fee_bps,
            reserve_depth: active_liquidity,
            reserve_a: None,
            reserve_b: None,
            active_liquidity,
            sqrt_price_x64: Some(sqrt_price),
            venue: Some(crate::types::PoolVenue::OrcaWhirlpool),
            confidence: crate::types::PoolConfidence::Exact,
            repair_pending: false,
            liquidity_model,
            slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                liquidity_model,
                tick_spacing,
            ),
            token_mint_a,
            token_mint_b,
            tick_spacing,
            current_tick_index,
            last_update_slot: record.slot,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(head_slot, record.slot, 0),
        })
    }
}

#[derive(Debug, Default)]
pub struct RaydiumClmmPoolDecoder;

impl AccountDecoder for RaydiumClmmPoolDecoder {
    fn decoder_key(&self) -> &'static str {
        "raydium-clmm-v1"
    }

    fn decode(&self, pool_id: &PoolId, record: &AccountRecord, head_slot: u64) -> DecodedAccount {
        if record.data.len() < 273 {
            return DecodedAccount::Ignored;
        }
        let tick_spacing = read_u16(&record.data, 235).unwrap_or_default();
        let liquidity = read_u128(&record.data, 237).unwrap_or_default();
        let sqrt_price = read_u128(&record.data, 253).unwrap_or_default();
        let current_tick_index = read_i32(&record.data, 269);
        let token_mint_a = read_pubkey(&record.data, 73).unwrap_or_default();
        let token_mint_b = read_pubkey(&record.data, 105).unwrap_or_default();
        let active_liquidity = liquidity.min(u64::MAX as u128) as u64;
        let liquidity_model = LiquidityModel::ConcentratedLiquidity;

        DecodedAccount::PoolState(PoolSnapshot {
            pool_id: pool_id.clone(),
            price_bps: sqrt_price_x64_to_price_bps(sqrt_price),
            fee_bps: 0,
            reserve_depth: active_liquidity,
            reserve_a: None,
            reserve_b: None,
            active_liquidity,
            sqrt_price_x64: Some(sqrt_price),
            venue: Some(crate::types::PoolVenue::RaydiumClmm),
            confidence: crate::types::PoolConfidence::Exact,
            repair_pending: false,
            liquidity_model,
            slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                liquidity_model,
                tick_spacing,
            ),
            token_mint_a,
            token_mint_b,
            tick_spacing,
            current_tick_index,
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

fn read_u128(data: &[u8], offset: usize) -> Option<u128> {
    let bytes = data.get(offset..offset + 16)?;
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Some(u128::from_le_bytes(buf))
}

fn read_i32(data: &[u8], offset: usize) -> Option<i32> {
    let bytes = data.get(offset..offset + 4)?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    Some(i32::from_le_bytes(buf))
}

fn read_pubkey(data: &[u8], offset: usize) -> Option<String> {
    let bytes = data.get(offset..offset + 32)?;
    Some(bs58::encode(bytes).into_string())
}

fn sqrt_price_x64_to_price_bps(sqrt_price_x64: u128) -> u64 {
    let sqrt_ratio = (sqrt_price_x64 as f64) / 18_446_744_073_709_551_616.0;
    let price = sqrt_ratio * sqrt_ratio;
    let scaled = (price * 10_000.0).round();
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }
    scaled.min(u64::MAX as f64) as u64
}

#[cfg(test)]
mod tests {
    use super::{AccountDecoder, OrcaWhirlpoolAccountDecoder, RaydiumClmmPoolDecoder};
    use crate::types::{AccountKey, AccountRecord, DecodedAccount, LiquidityModel, PoolId};
    use std::time::SystemTime;

    fn account_record(data: Vec<u8>) -> AccountRecord {
        AccountRecord {
            key: AccountKey("acct".into()),
            owner: "owner".into(),
            lamports: 0,
            data,
            slot: 42,
            write_version: 1,
            observed_at: SystemTime::now(),
        }
    }

    fn write_u16(data: &mut [u8], offset: usize, value: u16) {
        data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u128(data: &mut [u8], offset: usize, value: u128) {
        data[offset..offset + 16].copy_from_slice(&value.to_le_bytes());
    }

    fn write_i32(data: &mut [u8], offset: usize, value: i32) {
        data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    #[test]
    fn orca_whirlpool_decoder_populates_liquidity_hints() {
        let mut data = vec![0u8; 245];
        write_u16(&mut data, 41, 4);
        write_u16(&mut data, 45, 400);
        write_u128(&mut data, 49, 500_000);
        write_u128(&mut data, 65, 1u128 << 64);
        write_i32(&mut data, 81, 12);
        data[101..133].fill(1);
        data[181..213].fill(2);

        let decoder = OrcaWhirlpoolAccountDecoder;
        let decoded = decoder.decode(&PoolId("pool-a".into()), &account_record(data), 42);
        let DecodedAccount::PoolState(snapshot) = decoded else {
            panic!("expected pool snapshot");
        };

        assert_eq!(snapshot.active_liquidity, 500_000);
        assert_eq!(snapshot.sqrt_price_x64, Some(1u128 << 64));
        assert_eq!(
            snapshot.liquidity_model,
            LiquidityModel::ConcentratedLiquidity
        );
        assert!(snapshot.slippage_factor_bps > 10_000);
    }

    #[test]
    fn raydium_clmm_decoder_populates_liquidity_hints() {
        let mut data = vec![0u8; 273];
        write_u16(&mut data, 235, 1);
        write_u128(&mut data, 237, 750_000);
        write_u128(&mut data, 253, 1u128 << 64);
        write_i32(&mut data, 269, -8);
        data[73..105].fill(3);
        data[105..137].fill(4);

        let decoder = RaydiumClmmPoolDecoder;
        let decoded = decoder.decode(&PoolId("pool-b".into()), &account_record(data), 42);
        let DecodedAccount::PoolState(snapshot) = decoded else {
            panic!("expected pool snapshot");
        };

        assert_eq!(snapshot.active_liquidity, 750_000);
        assert_eq!(snapshot.sqrt_price_x64, Some(1u128 << 64));
        assert_eq!(
            snapshot.liquidity_model,
            LiquidityModel::ConcentratedLiquidity
        );
        assert!(snapshot.slippage_factor_bps > 10_000);
    }
}
