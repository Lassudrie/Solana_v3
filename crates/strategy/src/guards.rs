use crate::{quote::RouteQuote, reasons::RejectionReason, route_registry::RouteDefinition};
use state::{
    StatePlane,
    types::{ExecutionStateSnapshot, PoolSnapshot, RouteId, WarmupStatus},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardrailConfig {
    pub min_profit_lamports: i64,
    pub max_snapshot_slot_lag: u64,
    pub require_route_warm: bool,
    pub max_inflight_submissions: usize,
    pub min_wallet_balance_lamports: u64,
    pub max_blockhash_slot_lag: u64,
}

impl Default for GuardrailConfig {
    fn default() -> Self {
        Self {
            min_profit_lamports: 1,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuardrailSet {
    config: GuardrailConfig,
}

impl GuardrailSet {
    pub fn new(config: GuardrailConfig) -> Self {
        Self { config }
    }

    pub fn evaluate_route_readiness(
        &self,
        route_id: &RouteId,
        state: &StatePlane,
    ) -> Result<(), RejectionReason> {
        let status = state.route_warmup_status(route_id);
        if self.config.require_route_warm && status != WarmupStatus::Ready {
            return Err(RejectionReason::RouteNotWarm { status });
        }
        Ok(())
    }

    pub fn evaluate_snapshots(&self, snapshots: [&PoolSnapshot; 2]) -> Result<(), RejectionReason> {
        for snapshot in snapshots {
            if snapshot.freshness.slot_lag > self.config.max_snapshot_slot_lag
                || snapshot.freshness.is_stale
            {
                return Err(RejectionReason::SnapshotStale {
                    pool_id: snapshot.pool_id.clone(),
                    slot_lag: snapshot.freshness.slot_lag,
                });
            }
        }
        Ok(())
    }

    pub fn evaluate_quote(
        &self,
        route: &RouteDefinition,
        quote: &RouteQuote,
        execution_state: &ExecutionStateSnapshot,
        inflight_submissions: usize,
    ) -> Result<(), RejectionReason> {
        if execution_state.kill_switch_enabled {
            return Err(RejectionReason::KillSwitchActive);
        }
        if !execution_state.wallet_ready {
            return Err(RejectionReason::WalletNotReady);
        }
        if execution_state.wallet_balance_lamports < self.config.min_wallet_balance_lamports {
            return Err(RejectionReason::WalletBalanceTooLow {
                current: execution_state.wallet_balance_lamports,
                minimum: self.config.min_wallet_balance_lamports,
            });
        }
        let Some(blockhash_slot) = execution_state.blockhash_slot else {
            return Err(RejectionReason::BlockhashUnavailable);
        };
        let blockhash_slot_lag = execution_state.head_slot.saturating_sub(blockhash_slot);
        if blockhash_slot_lag > self.config.max_blockhash_slot_lag {
            return Err(RejectionReason::BlockhashTooStale {
                slot_lag: blockhash_slot_lag,
                maximum: self.config.max_blockhash_slot_lag,
            });
        }
        if inflight_submissions >= self.config.max_inflight_submissions {
            return Err(RejectionReason::TooManyInflight {
                current: inflight_submissions,
                maximum: self.config.max_inflight_submissions,
            });
        }
        if route.default_trade_size > route.max_trade_size {
            return Err(RejectionReason::TradeSizeTooLarge {
                requested: route.default_trade_size,
                maximum: route.max_trade_size,
            });
        }
        if quote.expected_net_profit < self.config.min_profit_lamports {
            return Err(RejectionReason::ProfitBelowThreshold {
                expected: quote.expected_net_profit,
                minimum: self.config.min_profit_lamports,
            });
        }
        Ok(())
    }
}
