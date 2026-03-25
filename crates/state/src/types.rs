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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSnapshot {
    pub pool_id: PoolId,
    pub price_bps: u64,
    pub fee_bps: u16,
    pub reserve_depth: u64,
    pub last_update_slot: u64,
    pub derived_at: SystemTime,
    pub freshness: FreshnessState,
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
    pub latest_blockhash: Option<String>,
    pub blockhash_slot: Option<u64>,
    pub alt_revision: u64,
    pub wallet_balance_lamports: u64,
    pub wallet_ready: bool,
    pub kill_switch_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateApplyOutcome {
    pub update_status: AccountUpdateStatus,
    pub impacted_pools: Vec<PoolId>,
    pub impacted_routes: Vec<RouteId>,
    pub refreshed_snapshots: usize,
    pub latest_slot: u64,
}
