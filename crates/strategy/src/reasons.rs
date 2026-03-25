use state::types::{PoolId, RouteId, WarmupStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionReason {
    RouteNotRegistered,
    RouteNotWarm { status: WarmupStatus },
    MissingSnapshot { pool_id: PoolId },
    SnapshotStale { pool_id: PoolId, slot_lag: u64 },
    BlockhashUnavailable,
    BlockhashTooStale { slot_lag: u64, maximum: u64 },
    ProfitBelowThreshold { expected: i64, minimum: i64 },
    TradeSizeTooLarge { requested: u64, maximum: u64 },
    WalletNotReady,
    WalletBalanceTooLow { current: u64, minimum: u64 },
    TooManyInflight { current: usize, maximum: usize },
    KillSwitchActive,
    QuoteFailed { detail: String },
    NoImpactedRoutes,
    RouteFilteredOut { route_id: RouteId },
}
