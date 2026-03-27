use crate::{quote::RouteQuote, reasons::RejectionReason, route_registry::RouteDefinition};
use domain::{ExecutionSnapshot, PoolSnapshot, RouteId, WarmupStatus};
use state::StatePlane;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardrailConfig {
    pub min_profit_quote_atoms: i64,
    pub require_route_warm: bool,
    pub max_inflight_submissions: usize,
    pub min_wallet_balance_lamports: u64,
    pub max_blockhash_slot_lag: u64,
}

impl Default for GuardrailConfig {
    fn default() -> Self {
        Self {
            min_profit_quote_atoms: 1,
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

    pub fn minimum_acceptable_output(
        &self,
        trade_size: u64,
        estimated_execution_cost_quote_atoms: u64,
    ) -> u64 {
        trade_size
            .saturating_add(estimated_execution_cost_quote_atoms)
            .saturating_add(self.config.min_profit_quote_atoms.max(0) as u64)
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

    pub fn evaluate_snapshots(
        &self,
        state: &StatePlane,
        snapshots: &[&PoolSnapshot],
    ) -> Result<(), RejectionReason> {
        for snapshot in snapshots {
            if snapshot.freshness.is_stale {
                if snapshot.repair_pending {
                    return Err(RejectionReason::PoolRepairPending {
                        pool_id: snapshot.pool_id.clone(),
                    });
                }
                return Err(RejectionReason::SnapshotStale {
                    pool_id: snapshot.pool_id.clone(),
                    slot_lag: snapshot.freshness.slot_lag,
                });
            }
            if !snapshot.is_executable() {
                if snapshot.repair_pending {
                    return Err(RejectionReason::PoolRepairPending {
                        pool_id: snapshot.pool_id.clone(),
                    });
                }
                return Err(RejectionReason::PoolStateNotExecutable {
                    pool_id: snapshot.pool_id.clone(),
                });
            }
            if !state.pool_has_executable_quote_model(&snapshot.pool_id) {
                return Err(RejectionReason::PoolQuoteModelNotExecutable {
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
        execution_state: &ExecutionSnapshot,
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
        if quote.expected_net_profit_quote_atoms < self.config.min_profit_quote_atoms {
            return Err(RejectionReason::ProfitBelowThreshold {
                expected: quote.expected_net_profit_quote_atoms,
                minimum: self.config.min_profit_quote_atoms,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        EventSourceKind, ExecutionStateSnapshot, FreshnessState, LiquidityModel, NormalizedEvent,
        PoolConfidence, PoolId, PoolSnapshot, PoolSnapshotUpdate, PoolVenue, RouteId,
        SnapshotConfidence,
    };
    use state::StatePlane;

    use crate::{
        guards::{GuardrailConfig, GuardrailSet},
        quote::{LegQuote, RouteQuote},
        reasons::RejectionReason,
        route_registry::{RouteDefinition, RouteLeg, SwapSide},
    };

    fn executable_snapshot(pool_id: &str, slot_lag: u64) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: PoolId(pool_id.into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: LiquidityModel::ConstantProduct,
            slippage_factor_bps: 100,
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 10u64.saturating_sub(slot_lag),
            derived_at: std::time::SystemTime::UNIX_EPOCH,
            freshness: FreshnessState {
                head_slot: 10,
                slot_lag,
                is_stale: slot_lag > 2,
            },
        }
    }

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            sol_quote_conversion_pool_id: Some(PoolId("pool-a".into())),
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
            min_trade_size: 10_000,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
            execution_protection: None,
        }
    }

    fn state_with_snapshots(snapshots: &[PoolSnapshot]) -> StatePlane {
        let mut state = StatePlane::new(2);
        for (index, snapshot) in snapshots.iter().enumerate() {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    index as u64 + 1,
                    snapshot.last_update_slot,
                    PoolSnapshotUpdate {
                        pool_id: snapshot.pool_id.0.clone(),
                        price_bps: snapshot.price_bps,
                        fee_bps: snapshot.fee_bps,
                        reserve_depth: snapshot.reserve_depth,
                        reserve_a: snapshot.reserve_a,
                        reserve_b: snapshot.reserve_b,
                        active_liquidity: Some(snapshot.active_liquidity),
                        sqrt_price_x64: snapshot.sqrt_price_x64,
                        venue: snapshot.venue.unwrap_or(match snapshot.liquidity_model {
                            LiquidityModel::Unknown | LiquidityModel::ConstantProduct => {
                                PoolVenue::OrcaSimplePool
                            }
                            LiquidityModel::ConcentratedLiquidity => PoolVenue::OrcaWhirlpool,
                        }),
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(snapshot.repair_pending),
                        token_mint_a: snapshot.token_mint_a.clone(),
                        token_mint_b: snapshot.token_mint_b.clone(),
                        tick_spacing: snapshot.tick_spacing,
                        current_tick_index: snapshot.current_tick_index,
                        slot: snapshot.last_update_slot,
                        write_version: index as u64 + 1,
                    },
                ))
                .expect("snapshot event should apply");
        }
        state
    }

    #[test]
    fn rejects_exact_but_stale_snapshot() {
        let guards = GuardrailSet::new(GuardrailConfig::default());
        let first = executable_snapshot("pool-a", 3);
        let second = executable_snapshot("pool-b", 0);
        let state = state_with_snapshots(&[first.clone(), second.clone()]);

        let rejection = guards
            .evaluate_snapshots(&state, &[&first, &second])
            .expect_err("stale snapshots should be rejected");

        assert_eq!(
            rejection,
            RejectionReason::SnapshotStale {
                pool_id: PoolId("pool-a".into()),
                slot_lag: 3,
            }
        );
    }

    #[test]
    fn rejects_concentrated_liquidity_without_executable_quote_model() {
        let guards = GuardrailSet::new(GuardrailConfig::default());
        let mut first = executable_snapshot("pool-a", 0);
        first.liquidity_model = LiquidityModel::ConcentratedLiquidity;
        first.sqrt_price_x64 = Some(1u128 << 64);
        first.tick_spacing = 4;
        first.current_tick_index = Some(12);
        first.reserve_a = None;
        first.reserve_b = None;
        let second = executable_snapshot("pool-b", 0);
        let state = state_with_snapshots(&[first.clone(), second.clone()]);

        let rejection = guards
            .evaluate_snapshots(&state, [&first, &second].as_slice())
            .expect_err("clmm quotes should be blocked until tick-aware modeling exists");

        assert_eq!(
            rejection,
            RejectionReason::PoolQuoteModelNotExecutable {
                pool_id: PoolId("pool-a".into()),
            }
        );
    }

    fn profitable_quote() -> RouteQuote {
        RouteQuote {
            quoted_slot: 100,
            input_amount: 10_000,
            gross_output_amount: 10_200,
            net_output_amount: 10_150,
            expected_gross_profit_quote_atoms: 150,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: 150,
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

    #[test]
    fn minimum_acceptable_output_includes_positive_profit_floor() {
        let guardrails = GuardrailSet::new(GuardrailConfig {
            min_profit_quote_atoms: 25,
            ..GuardrailConfig::default()
        });

        assert_eq!(guardrails.minimum_acceptable_output(1_000, 15), 1_040);
    }
}
