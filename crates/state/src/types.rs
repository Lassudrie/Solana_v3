pub use domain::types::{
    AccountKey, AccountUpdateStatus, ConcentratedLiquidityPoolState, ConstantProductPoolState,
    ExecutablePoolState, ExecutionSnapshot, ExecutionStateSnapshot, FreshnessState, LiquidityModel,
    LookupTableSnapshot, PoolConfidence, PoolId, PoolSnapshot, PoolVenue, RouteId, RouteState,
    StateApplyOutcome, WarmupStatus, sqrt_price_x64_to_price_bps,
};

use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRecord {
    pub key: AccountKey,
    pub owner: String,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub slot: u64,
    pub write_version: u64,
    pub observed_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedAccount {
    PoolState(PoolSnapshot),
    Ignored,
}

#[cfg(test)]
mod tests {
    use super::{
        ConstantProductPoolState, ExecutablePoolState, FreshnessState, LiquidityModel,
        PoolConfidence, PoolId, PoolSnapshot, PoolVenue, sqrt_price_x64_to_price_bps,
    };
    use std::time::SystemTime;

    fn snapshot(liquidity_model: LiquidityModel, tick_spacing: u16) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: Some(1u128 << 64),
            venue: Some(PoolVenue::OrcaSimplePool),
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model,
            slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                liquidity_model,
                tick_spacing,
            ),
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            tick_spacing,
            current_tick_index: Some(10),
            last_update_slot: 1,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(1, 1, 2),
        }
    }

    #[test]
    fn concentrated_liquidity_penalizes_size_more_than_constant_product() {
        let constant_product = snapshot(LiquidityModel::ConstantProduct, 0);
        let concentrated = snapshot(LiquidityModel::ConcentratedLiquidity, 64);

        assert!(
            concentrated.estimated_slippage_bps(20_000)
                > constant_product.estimated_slippage_bps(20_000)
        );
    }

    #[test]
    fn executable_constant_product_state_derives_snapshot() {
        let state = ExecutablePoolState::OrcaSimplePool(ConstantProductPoolState {
            pool_id: PoolId("pool-a".into()),
            venue: PoolVenue::OrcaSimplePool,
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 2_000_000,
            reserve_b: 1_000_000,
            fee_bps: 30,
            last_update_slot: 9,
            write_version: 3,
            last_verified_slot: 9,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
        });

        let snapshot = state.to_pool_snapshot(10, 2);
        assert_eq!(snapshot.pool_id, PoolId("pool-a".into()));
        assert_eq!(snapshot.price_bps, 5_000);
        assert_eq!(snapshot.reserve_depth, 1_000_000);
    }

    #[test]
    fn sqrt_price_conversion_uses_fixed_point_arithmetic() {
        assert_eq!(sqrt_price_x64_to_price_bps(0), 0);
        assert_eq!(sqrt_price_x64_to_price_bps(1u128 << 63), 2_500);
        assert_eq!(sqrt_price_x64_to_price_bps(1u128 << 64), 10_000);
        assert_eq!(sqrt_price_x64_to_price_bps((3u128 << 63) / 1), 22_500);
    }
}
