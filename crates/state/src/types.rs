use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct AccountKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct PoolId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RouteId(pub String);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountUpdateStatus {
    Applied,
    StaleRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessState {
    pub head_slot: u64,
    pub slot_lag: u64,
    pub is_stale: bool,
}

impl FreshnessState {
    pub fn at(head_slot: u64, last_update_slot: u64, max_slot_lag: u64) -> Self {
        let slot_lag = head_slot.saturating_sub(last_update_slot);
        Self {
            head_slot,
            slot_lag,
            is_stale: slot_lag > max_slot_lag,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidityModel {
    Unknown,
    ConstantProduct,
    ConcentratedLiquidity,
}

impl LiquidityModel {
    pub fn from_market_hints(
        tick_spacing: u16,
        current_tick_index: Option<i32>,
        sqrt_price_x64: Option<u128>,
    ) -> Self {
        if tick_spacing > 0 || current_tick_index.is_some() || sqrt_price_x64.is_some() {
            Self::ConcentratedLiquidity
        } else {
            Self::ConstantProduct
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolVenue {
    OrcaSimplePool,
    RaydiumSimplePool,
    OrcaWhirlpool,
    RaydiumClmm,
}

impl PoolVenue {
    pub fn liquidity_model(self) -> LiquidityModel {
        match self {
            Self::OrcaSimplePool | Self::RaydiumSimplePool => LiquidityModel::ConstantProduct,
            Self::OrcaWhirlpool | Self::RaydiumClmm => LiquidityModel::ConcentratedLiquidity,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolConfidence {
    Decoded,
    Verified,
    Executable,
    Invalid,
}

impl PoolConfidence {
    pub fn is_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Executable)
    }

    pub fn is_executable(self) -> bool {
        self == Self::Executable
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstantProductPoolState {
    pub pool_id: PoolId,
    pub venue: PoolVenue,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_bps: u16,
    pub last_update_slot: u64,
    pub write_version: u64,
    pub last_verified_slot: u64,
    pub confidence: PoolConfidence,
    pub repair_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcentratedLiquidityPoolState {
    pub pool_id: PoolId,
    pub venue: PoolVenue,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub fee_bps: u16,
    pub active_liquidity: u64,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub current_tick_index: i32,
    pub tick_spacing: u16,
    pub loaded_tick_arrays: usize,
    pub expected_tick_arrays: usize,
    pub last_update_slot: u64,
    pub write_version: u64,
    pub last_verified_slot: u64,
    pub confidence: PoolConfidence,
    pub repair_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutablePoolState {
    OrcaSimplePool(ConstantProductPoolState),
    RaydiumSimplePool(ConstantProductPoolState),
    OrcaWhirlpool(ConcentratedLiquidityPoolState),
    RaydiumClmm(ConcentratedLiquidityPoolState),
}

impl ExecutablePoolState {
    pub fn pool_id(&self) -> &PoolId {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => &state.pool_id,
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => &state.pool_id,
        }
    }

    pub fn venue(&self) -> PoolVenue {
        match self {
            Self::OrcaSimplePool(_) => PoolVenue::OrcaSimplePool,
            Self::RaydiumSimplePool(_) => PoolVenue::RaydiumSimplePool,
            Self::OrcaWhirlpool(_) => PoolVenue::OrcaWhirlpool,
            Self::RaydiumClmm(_) => PoolVenue::RaydiumClmm,
        }
    }

    pub fn last_update_slot(&self) -> u64 {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => state.last_update_slot,
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => state.last_update_slot,
        }
    }

    pub fn write_version(&self) -> u64 {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => state.write_version,
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => state.write_version,
        }
    }

    pub fn confidence(&self) -> PoolConfidence {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => state.confidence,
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => state.confidence,
        }
    }

    pub fn set_confidence(
        &mut self,
        confidence: PoolConfidence,
        last_update_slot: u64,
        write_version: u64,
        last_verified_slot: Option<u64>,
    ) {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => {
                state.confidence = confidence;
                state.last_update_slot = last_update_slot;
                state.write_version = write_version;
                state.repair_pending = !confidence.is_executable();
                if let Some(slot) = last_verified_slot {
                    state.last_verified_slot = slot;
                }
            }
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => {
                state.confidence = confidence;
                state.last_update_slot = last_update_slot;
                state.write_version = write_version;
                state.repair_pending = !confidence.is_executable();
                if let Some(slot) = last_verified_slot {
                    state.last_verified_slot = slot;
                }
            }
        }
    }

    pub fn to_pool_snapshot(&self, head_slot: u64, max_slot_lag: u64) -> PoolSnapshot {
        match self {
            Self::OrcaSimplePool(state) | Self::RaydiumSimplePool(state) => {
                let reserve_depth = state.reserve_a.min(state.reserve_b);
                let price_bps = if state.reserve_a == 0 {
                    0
                } else {
                    ((u128::from(state.reserve_b) * 10_000u128) / u128::from(state.reserve_a))
                        .min(u128::from(u64::MAX)) as u64
                };
                PoolSnapshot {
                    pool_id: state.pool_id.clone(),
                    price_bps,
                    fee_bps: state.fee_bps,
                    reserve_depth,
                    reserve_a: Some(state.reserve_a),
                    reserve_b: Some(state.reserve_b),
                    active_liquidity: reserve_depth,
                    sqrt_price_x64: None,
                    venue: Some(state.venue),
                    confidence: state.confidence,
                    repair_pending: state.repair_pending,
                    liquidity_model: state.venue.liquidity_model(),
                    slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                        state.venue.liquidity_model(),
                        0,
                    ),
                    token_mint_a: state.token_mint_a.clone(),
                    token_mint_b: state.token_mint_b.clone(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    last_update_slot: state.last_update_slot,
                    derived_at: SystemTime::now(),
                    freshness: FreshnessState::at(head_slot, state.last_update_slot, max_slot_lag),
                }
            }
            Self::OrcaWhirlpool(state) | Self::RaydiumClmm(state) => {
                let liquidity_model = state.venue.liquidity_model();
                PoolSnapshot {
                    pool_id: state.pool_id.clone(),
                    price_bps: sqrt_price_x64_to_price_bps(state.sqrt_price_x64),
                    fee_bps: state.fee_bps,
                    reserve_depth: state.active_liquidity,
                    reserve_a: None,
                    reserve_b: None,
                    active_liquidity: state.active_liquidity,
                    sqrt_price_x64: Some(state.sqrt_price_x64),
                    venue: Some(state.venue),
                    confidence: state.confidence,
                    repair_pending: state.repair_pending,
                    liquidity_model,
                    slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(
                        liquidity_model,
                        state.tick_spacing,
                    ),
                    token_mint_a: state.token_mint_a.clone(),
                    token_mint_b: state.token_mint_b.clone(),
                    tick_spacing: state.tick_spacing,
                    current_tick_index: Some(state.current_tick_index),
                    last_update_slot: state.last_update_slot,
                    derived_at: SystemTime::now(),
                    freshness: FreshnessState::at(head_slot, state.last_update_slot, max_slot_lag),
                }
            }
        }
    }
}

pub fn sqrt_price_x64_to_price_bps(sqrt_price_x64: u128) -> u64 {
    let square = mul_u128_to_u256(sqrt_price_x64, sqrt_price_x64);
    let (mut scaled, overflow_mul) = mul_u256_by_u64(square, 10_000);
    let overflow_round = add_u64_to_u256(&mut scaled, 1, 1u64 << 63);
    if overflow_mul || overflow_round || scaled[3] != 0 {
        return u64::MAX;
    }
    scaled[2]
}

fn mul_u128_to_u256(left: u128, right: u128) -> [u64; 4] {
    let left_lo = left as u64;
    let left_hi = (left >> 64) as u64;
    let right_lo = right as u64;
    let right_hi = (right >> 64) as u64;
    let mut product = [0u64; 4];

    add_u128_to_u256(&mut product, 0, u128::from(left_lo) * u128::from(right_lo));
    add_u128_to_u256(&mut product, 1, u128::from(left_lo) * u128::from(right_hi));
    add_u128_to_u256(&mut product, 1, u128::from(left_hi) * u128::from(right_lo));
    add_u128_to_u256(&mut product, 2, u128::from(left_hi) * u128::from(right_hi));

    product
}

fn mul_u256_by_u64(value: [u64; 4], multiplier: u64) -> ([u64; 4], bool) {
    let mut out = [0u64; 4];
    let mut carry = 0u128;

    for (index, limb) in value.into_iter().enumerate() {
        let product = u128::from(limb) * u128::from(multiplier) + carry;
        out[index] = product as u64;
        carry = product >> 64;
    }

    (out, carry != 0)
}

fn add_u128_to_u256(target: &mut [u64; 4], offset: usize, value: u128) {
    add_u64_to_u256(target, offset, value as u64);
    let _ = add_u64_to_u256(target, offset + 1, (value >> 64) as u64);
}

fn add_u64_to_u256(target: &mut [u64; 4], offset: usize, value: u64) -> bool {
    if value == 0 {
        return false;
    }

    let mut index = offset;
    let mut carry = value;
    while index < target.len() {
        let (sum, overflow) = target[index].overflowing_add(carry);
        target[index] = sum;
        if !overflow {
            return false;
        }
        carry = 1;
        index += 1;
    }

    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSnapshot {
    pub pool_id: PoolId,
    pub price_bps: u64,
    pub fee_bps: u16,
    pub reserve_depth: u64,
    pub reserve_a: Option<u64>,
    pub reserve_b: Option<u64>,
    pub active_liquidity: u64,
    pub sqrt_price_x64: Option<u128>,
    pub venue: Option<PoolVenue>,
    pub confidence: PoolConfidence,
    pub repair_pending: bool,
    pub liquidity_model: LiquidityModel,
    pub slippage_factor_bps: u16,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub tick_spacing: u16,
    pub current_tick_index: Option<i32>,
    pub last_update_slot: u64,
    pub derived_at: SystemTime,
    pub freshness: FreshnessState,
}

impl PoolSnapshot {
    pub fn effective_liquidity(&self) -> u64 {
        self.active_liquidity.max(self.reserve_depth).max(1)
    }

    pub fn is_verified(&self) -> bool {
        self.confidence.is_verified()
    }

    pub fn is_executable(&self) -> bool {
        self.confidence.is_executable() && !self.repair_pending
    }

    pub fn has_executable_quote_model(&self) -> bool {
        self.liquidity_model == LiquidityModel::ConstantProduct
    }

    pub fn constant_product_reserves_for(
        &self,
        input_mint: &str,
        output_mint: &str,
    ) -> Option<(u64, u64)> {
        let reserve_a = self.reserve_a?;
        let reserve_b = self.reserve_b?;
        if input_mint == self.token_mint_a && output_mint == self.token_mint_b {
            Some((reserve_a, reserve_b))
        } else if input_mint == self.token_mint_b && output_mint == self.token_mint_a {
            Some((reserve_b, reserve_a))
        } else {
            None
        }
    }

    pub fn estimated_slippage_bps(&self, input_amount: u64) -> u16 {
        if input_amount == 0 {
            return 0;
        }

        let depth = self.effective_liquidity() as u128;
        let usage_bps =
            ((input_amount as u128) * 10_000u128).saturating_add(depth.saturating_sub(1)) / depth;
        let quadratic_impact_bps =
            usage_bps.saturating_mul(usage_bps).saturating_add(9_999) / 10_000;
        let factor_bps = u128::from(self.slippage_factor_bps.max(1));
        let adjusted_impact_bps = quadratic_impact_bps
            .saturating_mul(factor_bps)
            .saturating_add(9_999)
            / 10_000;

        adjusted_impact_bps.min(9_500) as u16
    }

    pub fn default_slippage_factor_bps(liquidity_model: LiquidityModel, tick_spacing: u16) -> u16 {
        match liquidity_model {
            LiquidityModel::Unknown | LiquidityModel::ConstantProduct => 10_000,
            LiquidityModel::ConcentratedLiquidity => {
                12_000u16.saturating_add(tick_spacing.min(100).saturating_mul(25))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteState {
    pub route_id: RouteId,
    pub warmup_status: WarmupStatus,
    pub last_touched_slot: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupStatus {
    Cold,
    Warming,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedAccount {
    PoolState(PoolSnapshot),
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionStateSnapshot {
    pub head_slot: u64,
    pub rpc_slot: Option<u64>,
    pub latest_blockhash: Option<String>,
    pub blockhash_slot: Option<u64>,
    pub alt_revision: u64,
    pub lookup_tables: Vec<LookupTableSnapshot>,
    pub wallet_balance_lamports: u64,
    pub wallet_ready: bool,
    pub kill_switch_enabled: bool,
}

impl ExecutionStateSnapshot {
    pub fn blockhash_slot_lag(&self) -> Option<u64> {
        Some(self.rpc_slot?.saturating_sub(self.blockhash_slot?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupTableSnapshot {
    pub account_key: String,
    pub addresses: Vec<String>,
    pub last_extended_slot: u64,
    pub fetched_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateApplyOutcome {
    pub update_status: AccountUpdateStatus,
    pub impacted_pools: Vec<PoolId>,
    pub impacted_routes: Vec<RouteId>,
    pub refreshed_snapshots: usize,
    pub latest_slot: u64,
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
