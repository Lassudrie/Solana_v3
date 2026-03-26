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
            if !snapshot.is_exact() {
                return Err(RejectionReason::PoolStateNotExact {
                    pool_id: snapshot.pool_id.clone(),
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
        let Some(blockhash_slot_lag) = execution_state.blockhash_slot_lag() else {
            return Err(RejectionReason::BlockhashUnavailable);
        };
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
        if quote.input_amount > route.max_trade_size {
            return Err(RejectionReason::TradeSizeTooLarge {
                requested: quote.input_amount,
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

#[cfg(test)]
mod tests {
    use state::types::{ExecutionStateSnapshot, PoolId, RouteId};

    use crate::{
        guards::{GuardrailConfig, GuardrailSet},
        quote::{LegQuote, RouteQuote},
        reasons::RejectionReason,
        route_registry::{RouteDefinition, RouteLeg, SwapSide},
    };

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    fee_bps: None,
                },
            ],
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
        }
    }

    fn profitable_quote() -> RouteQuote {
        RouteQuote {
            quoted_slot: 100,
            input_amount: 10_000,
            gross_output_amount: 10_200,
            net_output_amount: 10_150,
            expected_gross_profit: 150,
            estimated_execution_cost_lamports: 0,
            expected_net_profit: 150,
            leg_quotes: [
                LegQuote {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 10_000,
                    output_amount: 10_100,
                    fee_paid: 0,
                    current_tick_index: None,
                },
                LegQuote {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 10_100,
                    output_amount: 10_150,
                    fee_paid: 0,
                    current_tick_index: None,
                },
            ],
        }
    }

    #[test]
    fn blockhash_guard_uses_rpc_slot_instead_of_head_slot() {
        let guardrails = GuardrailSet::new(GuardrailConfig {
            max_blockhash_slot_lag: 8,
            ..GuardrailConfig::default()
        });
        let execution_state = ExecutionStateSnapshot {
            head_slot: 1_000,
            rpc_slot: Some(105),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(100),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        };

        let result = guardrails.evaluate_quote(
            &route_definition(),
            &profitable_quote(),
            &execution_state,
            0,
        );

        assert!(
            result.is_ok(),
            "head_slot should not drive blockhash freshness"
        );
    }

    #[test]
    fn blockhash_guard_rejects_when_rpc_slot_is_unavailable() {
        let guardrails = GuardrailSet::new(GuardrailConfig::default());
        let execution_state = ExecutionStateSnapshot {
            head_slot: 1_000,
            rpc_slot: None,
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(100),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            wallet_ready: true,
            kill_switch_enabled: false,
        };

        let result = guardrails.evaluate_quote(
            &route_definition(),
            &profitable_quote(),
            &execution_state,
            0,
        );

        assert_eq!(result, Err(RejectionReason::BlockhashUnavailable));
    }
}
