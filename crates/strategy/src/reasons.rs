use domain::{PoolId, RouteId, WarmupStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionReason {
    RouteNotRegistered,
    RouteNotWarm {
        status: WarmupStatus,
    },
    MissingSnapshot {
        pool_id: PoolId,
    },
    SnapshotStale {
        pool_id: PoolId,
        slot_lag: u64,
    },
    QuoteStaleForExecution {
        pool_id: PoolId,
        slot_lag: u64,
        maximum: u64,
    },
    PoolRepairPending {
        pool_id: PoolId,
    },
    PoolStateNotExecutable {
        pool_id: PoolId,
    },
    PoolQuoteModelNotExecutable {
        pool_id: PoolId,
    },
    ReserveUsageTooHigh {
        pool_id: PoolId,
        usage_bps: u64,
        maximum_bps: u64,
    },
    BlockhashUnavailable,
    BlockhashTooStale {
        slot_lag: u64,
        maximum: u64,
    },
    ProfitBelowThreshold {
        expected: i64,
        minimum: i64,
    },
    TradeSizeTooLarge {
        requested: u64,
        maximum: u64,
    },
    WalletNotReady,
    WalletBalanceTooLow {
        current: u64,
        minimum: u64,
    },
    TooManyInflight {
        current: usize,
        maximum: usize,
    },
    KillSwitchActive,
    QuoteFailed {
        detail: String,
    },
    ExecutionCostNotConvertible {
        detail: String,
    },
    NoImpactedRoutes,
    RouteFilteredOut {
        route_id: RouteId,
    },
}
