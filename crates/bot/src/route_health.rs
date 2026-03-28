use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};

use reconciliation::FailureClass;
use serde::{Deserialize, Serialize};
use state::types::{PoolId, PoolSnapshot, RouteId};

use crate::config::LiveSetHealthConfig;

pub type SharedRouteHealth = Arc<Mutex<RouteHealthRegistry>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolHealthState {
    Healthy,
    ExecutableRefreshPending,
    RepairPending,
    Degraded,
    Quarantined,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteHealthState {
    Eligible,
    BlockedPoolStale,
    BlockedPoolNotExecutable,
    BlockedPoolQuoteModelNotExecutable,
    BlockedPoolRepair,
    BlockedPoolQuarantined,
    BlockedPoolDisabled,
    ShadowOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteHealthSummary {
    pub eligible_live_routes: usize,
    pub shadow_routes: usize,
    pub quarantined_pools: usize,
    pub disabled_pools: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolHealthView {
    pub health_state: PoolHealthState,
    pub consecutive_refresh_failures: u32,
    pub consecutive_repair_failures: u32,
    pub quarantined_until_slot: Option<u64>,
    pub disable_reason: Option<String>,
    pub last_executable_slot: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteMonitorView {
    pub route_id: String,
    pub health_state: RouteHealthState,
    pub eligible_live: bool,
    pub pool_ids: Vec<String>,
    pub blocking_pool_id: Option<String>,
    pub blocking_reason: Option<String>,
    pub recent_chain_failure_count: usize,
    pub last_success_slot: Option<u64>,
    pub shadow_until_slot: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolHealthTransition {
    RefreshScheduled,
    RefreshStarted,
    RefreshFailed,
    RefreshSucceeded,
    RefreshCleared,
    RepairQueued,
    RepairStarted,
    RepairFailed,
    RepairSucceeded,
}

#[derive(Debug, Clone, Default)]
struct PoolHealthRecord {
    last_snapshot_slot: Option<u64>,
    last_executable_slot: Option<u64>,
    last_live_mutation_slot: Option<u64>,
    last_successful_refresh_slot: Option<u64>,
    last_successful_repair_slot: Option<u64>,
    refresh_pending: bool,
    refresh_in_flight: bool,
    repair_pending: bool,
    repair_in_flight: bool,
    snapshot_executable: bool,
    snapshot_quote_model_executable: bool,
    snapshot_stale: bool,
    consecutive_refresh_failures: u32,
    consecutive_repair_failures: u32,
    quarantine_entered_slots: VecDeque<u64>,
    quarantined_until_slot: Option<u64>,
    disable_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RouteHealthRecord {
    recent_failure_slots: VecDeque<u64>,
    last_success_slot: Option<u64>,
    shadow_until_slot: Option<u64>,
}

#[derive(Debug, Default)]
pub struct RouteHealthRegistry {
    config: LiveSetHealthConfig,
    default_max_snapshot_slot_lag: u64,
    pools: BTreeMap<String, PoolHealthRecord>,
    routes: BTreeMap<String, RouteHealthRecord>,
    route_pools: BTreeMap<String, Vec<String>>,
    route_execution_max_slot_lag: BTreeMap<String, u64>,
    pool_routes: BTreeMap<String, BTreeSet<String>>,
}

impl RouteHealthRegistry {
    pub fn new(config: LiveSetHealthConfig, default_max_snapshot_slot_lag: u64) -> Self {
        Self {
            config,
            default_max_snapshot_slot_lag,
            pools: BTreeMap::new(),
            routes: BTreeMap::new(),
            route_pools: BTreeMap::new(),
            route_execution_max_slot_lag: BTreeMap::new(),
            pool_routes: BTreeMap::new(),
        }
    }

    pub fn register_route(
        &mut self,
        route_id: RouteId,
        pool_ids: &[PoolId],
        max_quote_slot_lag: u64,
    ) {
        let route_key = route_id.0;
        let pool_ids = pool_ids
            .iter()
            .map(|pool_id| pool_id.0.clone())
            .collect::<Vec<_>>();
        self.routes.entry(route_key.clone()).or_default();
        self.route_pools.insert(route_key.clone(), pool_ids.clone());
        self.route_execution_max_slot_lag
            .insert(route_key.clone(), max_quote_slot_lag);
        for pool_id in pool_ids {
            self.pools.entry(pool_id.clone()).or_default();
            self.pool_routes
                .entry(pool_id)
                .or_default()
                .insert(route_key.clone());
        }
    }

    pub fn on_pool_snapshot(
        &mut self,
        snapshot: &PoolSnapshot,
        quote_model_executable: bool,
        observed_slot: u64,
    ) {
        let record = self.pools.entry(snapshot.pool_id.0.clone()).or_default();
        record.last_snapshot_slot = Some(snapshot.last_update_slot);
        record.last_live_mutation_slot = Some(observed_slot);
        record.snapshot_executable = snapshot.is_executable();
        record.snapshot_quote_model_executable = quote_model_executable;
        record.snapshot_stale = snapshot.freshness.is_stale;
        record.repair_pending = snapshot.repair_pending || record.repair_pending;
        if snapshot.is_executable() {
            record.last_executable_slot = Some(snapshot.last_update_slot);
            record.repair_pending = false;
            record.repair_in_flight = false;
        }
        if snapshot.is_executable() && !snapshot.freshness.is_stale {
            record.reset_on_executable_recovery(snapshot.last_update_slot);
        }
    }

    pub fn on_repair_transition(
        &mut self,
        pool_id: &str,
        transition: PoolHealthTransition,
        observed_slot: u64,
    ) {
        let record = self.pools.entry(pool_id.to_string()).or_default();
        record.last_live_mutation_slot = Some(observed_slot);
        match transition {
            PoolHealthTransition::RefreshScheduled => {
                record.refresh_pending = true;
            }
            PoolHealthTransition::RefreshStarted => {
                record.refresh_pending = true;
                record.refresh_in_flight = true;
            }
            PoolHealthTransition::RefreshFailed => {
                record.refresh_pending = true;
                record.refresh_in_flight = false;
                record.consecutive_refresh_failures =
                    record.consecutive_refresh_failures.saturating_add(1);
                if self.config.pool_quarantine_after_refresh_failures > 0
                    && record.consecutive_refresh_failures
                        >= self.config.pool_quarantine_after_refresh_failures
                {
                    record.enter_quarantine(
                        observed_slot,
                        self.config.pool_quarantine_slots,
                        self.config.pool_disable_after_quarantine_count,
                        self.config.pool_disable_window_slots,
                    );
                }
            }
            PoolHealthTransition::RefreshSucceeded => {
                record.reset_on_executable_recovery(observed_slot);
                record.last_successful_refresh_slot = Some(observed_slot);
            }
            PoolHealthTransition::RefreshCleared => {
                record.refresh_pending = false;
                record.refresh_in_flight = false;
            }
            PoolHealthTransition::RepairQueued => {
                record.repair_pending = true;
            }
            PoolHealthTransition::RepairStarted => {
                record.repair_pending = true;
                record.repair_in_flight = true;
            }
            PoolHealthTransition::RepairFailed => {
                record.repair_pending = true;
                record.repair_in_flight = false;
                record.consecutive_repair_failures =
                    record.consecutive_repair_failures.saturating_add(1);
                if record.consecutive_repair_failures
                    >= self.config.pool_quarantine_after_repair_failures
                {
                    record.enter_quarantine(
                        observed_slot,
                        self.config.pool_quarantine_slots,
                        self.config.pool_disable_after_quarantine_count,
                        self.config.pool_disable_window_slots,
                    );
                }
            }
            PoolHealthTransition::RepairSucceeded => {
                record.reset_on_executable_recovery(observed_slot);
                record.last_successful_repair_slot = Some(observed_slot);
            }
        }
    }

    pub fn on_execution_success(&mut self, route_id: &RouteId, observed_slot: u64) {
        let record = self.routes.entry(route_id.0.clone()).or_default();
        record.last_success_slot = Some(observed_slot);
        record.shadow_until_slot = None;
        record.recent_failure_slots.clear();
    }

    pub fn on_execution_failure(
        &mut self,
        route_id: &RouteId,
        class: &FailureClass,
        observed_slot: u64,
    ) {
        let record = self.routes.entry(route_id.0.clone()).or_default();
        record.recent_failure_slots.push_back(observed_slot);
        prune_old_slots(
            &mut record.recent_failure_slots,
            observed_slot,
            self.config.route_failure_window_slots,
        );
        if matches!(
            class,
            FailureClass::ChainExecutionTooLittleOutput
                | FailureClass::ChainExecutionAmountInAboveMaximum
        ) && self.config.route_targeted_failure_cooldown_slots > 0
        {
            let targeted_shadow_until =
                observed_slot.saturating_add(self.config.route_targeted_failure_cooldown_slots);
            record.shadow_until_slot = Some(
                record
                    .shadow_until_slot
                    .map(|slot| slot.max(targeted_shadow_until))
                    .unwrap_or(targeted_shadow_until),
            );
        }
        if record.recent_failure_slots.len() as u32 >= self.config.route_shadow_after_chain_failures
        {
            let budget_shadow_until =
                observed_slot.saturating_add(self.config.route_reentry_cooldown_slots);
            record.shadow_until_slot = Some(
                record
                    .shadow_until_slot
                    .map(|slot| slot.max(budget_shadow_until))
                    .unwrap_or(budget_shadow_until),
            );
        }
    }

    pub fn eligible_impacted_routes(
        &self,
        impacted_routes: &[RouteId],
        observed_slot: u64,
    ) -> Vec<RouteId> {
        if !self.config.enabled {
            return impacted_routes.to_vec();
        }

        impacted_routes
            .iter()
            .filter(|route_id| {
                self.route_state(route_id.0.as_str(), observed_slot)
                    .map(|state| state.health_state == RouteHealthState::Eligible)
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    pub fn summary(&self, observed_slot: u64) -> RouteHealthSummary {
        let eligible_live_routes = self
            .route_pools
            .keys()
            .filter(|route_id| {
                self.route_state(route_id, observed_slot)
                    .map(|state| state.health_state == RouteHealthState::Eligible)
                    .unwrap_or(false)
            })
            .count();
        let shadow_routes = self
            .route_pools
            .keys()
            .filter(|route_id| {
                self.route_state(route_id, observed_slot)
                    .map(|state| state.health_state == RouteHealthState::ShadowOnly)
                    .unwrap_or(false)
            })
            .count();
        let quarantined_pools = self
            .pools
            .keys()
            .filter(|pool_id| {
                matches!(
                    self.pool_health_state(pool_id, observed_slot),
                    PoolHealthState::Quarantined
                )
            })
            .count();
        let disabled_pools = self
            .pools
            .keys()
            .filter(|pool_id| {
                matches!(
                    self.pool_health_state(pool_id, observed_slot),
                    PoolHealthState::Disabled
                )
            })
            .count();

        RouteHealthSummary {
            eligible_live_routes,
            shadow_routes,
            quarantined_pools,
            disabled_pools,
        }
    }

    pub fn pool_view(&self, pool_id: &str, observed_slot: u64) -> Option<PoolHealthView> {
        let record = self.pools.get(pool_id)?;
        Some(PoolHealthView {
            health_state: self.pool_health_state(pool_id, observed_slot),
            consecutive_refresh_failures: record.consecutive_refresh_failures,
            consecutive_repair_failures: record.consecutive_repair_failures,
            quarantined_until_slot: record.active_quarantine_until(observed_slot),
            disable_reason: record.disable_reason.clone(),
            last_executable_slot: record.last_executable_slot,
        })
    }

    pub fn route_views(&self, observed_slot: u64) -> Vec<RouteMonitorView> {
        let mut items = self
            .route_pools
            .keys()
            .filter_map(|route_id| self.route_state(route_id, observed_slot))
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.route_id.cmp(&right.route_id));
        items
    }

    pub fn blocked_route_view(
        &self,
        route_id: &RouteId,
        observed_slot: u64,
    ) -> Option<RouteMonitorView> {
        if !self.config.enabled {
            return None;
        }
        let view = self.route_state(route_id.0.as_str(), observed_slot)?;
        (!view.eligible_live).then_some(view)
    }

    pub fn pool_is_blocked_from_repair(&self, pool_id: &str, observed_slot: u64) -> bool {
        matches!(
            self.pool_health_state(pool_id, observed_slot),
            PoolHealthState::Quarantined | PoolHealthState::Disabled
        )
    }

    fn route_state(&self, route_id: &str, observed_slot: u64) -> Option<RouteMonitorView> {
        let pool_ids = self.route_pools.get(route_id)?.clone();
        let route_record = self.routes.get(route_id).cloned().unwrap_or_default();
        let max_quote_slot_lag = self
            .route_execution_max_slot_lag
            .get(route_id)
            .copied()
            .unwrap_or(self.default_max_snapshot_slot_lag);
        for pool_id in &pool_ids {
            let route_pool_ids = pool_ids.clone();
            match self.pool_block(pool_id, observed_slot, max_quote_slot_lag) {
                PoolBlock::None => {}
                PoolBlock::Disabled => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolDisabled,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: self
                            .pools
                            .get(pool_id)
                            .and_then(|record| record.disable_reason.clone())
                            .or_else(|| Some("pool_disabled".into())),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
                PoolBlock::Quarantined => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolQuarantined,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: Some("pool_quarantined".into()),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
                PoolBlock::Repair => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolRepair,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: Some("pool_repair_pending".into()),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
                PoolBlock::Stale => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolStale,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: Some("snapshot_stale".into()),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
                PoolBlock::NotExecutable => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolNotExecutable,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: Some("pool_state_not_executable".into()),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
                PoolBlock::QuoteModelNotExecutable => {
                    return Some(RouteMonitorView {
                        route_id: route_id.to_string(),
                        health_state: RouteHealthState::BlockedPoolQuoteModelNotExecutable,
                        eligible_live: false,
                        pool_ids: route_pool_ids,
                        blocking_pool_id: Some(pool_id.clone()),
                        blocking_reason: Some("pool_quote_model_not_executable".into()),
                        recent_chain_failure_count: recent_failures(
                            &route_record.recent_failure_slots,
                            observed_slot,
                            self.config.route_failure_window_slots,
                        ),
                        last_success_slot: route_record.last_success_slot,
                        shadow_until_slot: active_shadow_until(
                            route_record.shadow_until_slot,
                            observed_slot,
                        ),
                    });
                }
            }
        }

        if active_shadow_until(route_record.shadow_until_slot, observed_slot).is_some() {
            return Some(RouteMonitorView {
                route_id: route_id.to_string(),
                health_state: RouteHealthState::ShadowOnly,
                eligible_live: false,
                pool_ids,
                blocking_pool_id: None,
                blocking_reason: Some("execution_failure_budget".into()),
                recent_chain_failure_count: recent_failures(
                    &route_record.recent_failure_slots,
                    observed_slot,
                    self.config.route_failure_window_slots,
                ),
                last_success_slot: route_record.last_success_slot,
                shadow_until_slot: active_shadow_until(
                    route_record.shadow_until_slot,
                    observed_slot,
                ),
            });
        }

        Some(RouteMonitorView {
            route_id: route_id.to_string(),
            health_state: RouteHealthState::Eligible,
            eligible_live: true,
            pool_ids,
            blocking_pool_id: None,
            blocking_reason: None,
            recent_chain_failure_count: recent_failures(
                &route_record.recent_failure_slots,
                observed_slot,
                self.config.route_failure_window_slots,
            ),
            last_success_slot: route_record.last_success_slot,
            shadow_until_slot: None,
        })
    }

    fn pool_block(&self, pool_id: &str, observed_slot: u64, max_quote_slot_lag: u64) -> PoolBlock {
        let Some(record) = self.pools.get(pool_id) else {
            return PoolBlock::NotExecutable;
        };

        if record.disable_reason.is_some() {
            return PoolBlock::Disabled;
        }
        if record.active_quarantine_until(observed_slot).is_some() {
            return PoolBlock::Quarantined;
        }
        if record.repair_pending
            || record.repair_in_flight
            || record.refresh_pending
            || record.refresh_in_flight
        {
            return PoolBlock::Repair;
        }
        if snapshot_stale(record, observed_slot, max_quote_slot_lag) {
            return PoolBlock::Stale;
        }
        if !record.snapshot_executable {
            return PoolBlock::NotExecutable;
        }
        if !record.snapshot_quote_model_executable {
            return PoolBlock::QuoteModelNotExecutable;
        }
        PoolBlock::None
    }

    fn pool_health_state(&self, pool_id: &str, observed_slot: u64) -> PoolHealthState {
        match self.pool_block(pool_id, observed_slot, self.default_max_snapshot_slot_lag) {
            PoolBlock::None => PoolHealthState::Healthy,
            PoolBlock::Disabled => PoolHealthState::Disabled,
            PoolBlock::Quarantined => PoolHealthState::Quarantined,
            PoolBlock::Repair => {
                let Some(record) = self.pools.get(pool_id) else {
                    return PoolHealthState::RepairPending;
                };
                if record.refresh_pending || record.refresh_in_flight {
                    PoolHealthState::ExecutableRefreshPending
                } else {
                    PoolHealthState::RepairPending
                }
            }
            PoolBlock::Stale | PoolBlock::NotExecutable | PoolBlock::QuoteModelNotExecutable => {
                PoolHealthState::Degraded
            }
        }
    }
}

impl PoolHealthRecord {
    fn reset_on_executable_recovery(&mut self, executable_slot: u64) {
        self.last_executable_slot = Some(executable_slot);
        self.refresh_pending = false;
        self.refresh_in_flight = false;
        self.repair_pending = false;
        self.repair_in_flight = false;
        self.snapshot_executable = true;
        self.snapshot_stale = false;
        self.consecutive_refresh_failures = 0;
        self.consecutive_repair_failures = 0;
        self.quarantined_until_slot = None;
        self.disable_reason = None;
    }

    fn active_quarantine_until(&self, observed_slot: u64) -> Option<u64> {
        self.quarantined_until_slot
            .filter(|until| *until > observed_slot)
    }

    fn enter_quarantine(
        &mut self,
        observed_slot: u64,
        quarantine_slots: u64,
        disable_after_quarantine_count: u32,
        disable_window_slots: u64,
    ) {
        self.refresh_pending = false;
        self.refresh_in_flight = false;
        self.repair_pending = false;
        self.repair_in_flight = false;
        self.quarantined_until_slot = Some(observed_slot.saturating_add(quarantine_slots));
        self.quarantine_entered_slots.push_back(observed_slot);
        prune_old_slots(
            &mut self.quarantine_entered_slots,
            observed_slot,
            disable_window_slots,
        );
        if self.quarantine_entered_slots.len() as u32 >= disable_after_quarantine_count {
            self.disable_reason = Some("quarantine_limit_exceeded".into());
            self.quarantined_until_slot = None;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolBlock {
    None,
    Disabled,
    Quarantined,
    Repair,
    Stale,
    NotExecutable,
    QuoteModelNotExecutable,
}

fn active_shadow_until(shadow_until_slot: Option<u64>, observed_slot: u64) -> Option<u64> {
    shadow_until_slot.filter(|until| *until > observed_slot)
}

fn prune_old_slots(slots: &mut VecDeque<u64>, observed_slot: u64, window_slots: u64) {
    while slots
        .front()
        .copied()
        .map(|slot| observed_slot.saturating_sub(slot) > window_slots)
        .unwrap_or(false)
    {
        slots.pop_front();
    }
}

fn recent_failures(slots: &VecDeque<u64>, observed_slot: u64, window_slots: u64) -> usize {
    slots
        .iter()
        .filter(|slot| observed_slot.saturating_sub(**slot) <= window_slots)
        .count()
}

fn snapshot_stale(record: &PoolHealthRecord, observed_slot: u64, max_quote_slot_lag: u64) -> bool {
    record.snapshot_stale
        || record
            .last_snapshot_slot
            .map(|slot| observed_slot.saturating_sub(slot) > max_quote_slot_lag)
            .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use reconciliation::FailureClass;
    use state::types::{
        FreshnessState, LiquidityModel, PoolConfidence, PoolSnapshot, PoolVenue, RouteId,
    };
    use std::time::SystemTime;

    use super::{PoolHealthState, PoolHealthTransition, RouteHealthRegistry, RouteHealthState};
    use crate::config::LiveSetHealthConfig;

    fn snapshot(
        pool_id: &str,
        slot: u64,
        head_slot: u64,
        confidence: PoolConfidence,
        repair_pending: bool,
    ) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: state::types::PoolId(pool_id.into()),
            price_bps: 10_000,
            fee_bps: 30,
            reserve_depth: 1_000_000,
            reserve_a: Some(1_000_000),
            reserve_b: Some(1_000_000),
            active_liquidity: 1_000_000,
            sqrt_price_x64: None,
            venue: Some(PoolVenue::OrcaSimplePool),
            confidence,
            repair_pending,
            liquidity_model: LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: "a".into(),
            token_mint_b: "b".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: slot,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(head_slot, slot, 2),
        }
    }

    #[test]
    fn route_is_eligible_only_when_both_pools_are_healthy() {
        let mut registry = RouteHealthRegistry::new(LiveSetHealthConfig::default(), 2);
        registry.register_route(
            RouteId("route-a".into()),
            &[
                state::types::PoolId("pool-a".into()),
                state::types::PoolId("pool-b".into()),
            ],
            32,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-a", 10, 10, PoolConfidence::Executable, false),
            true,
            10,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-b", 10, 10, PoolConfidence::Executable, false),
            true,
            10,
        );

        let route = registry.route_views(10).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::Eligible);

        registry.on_repair_transition("pool-b", PoolHealthTransition::RepairQueued, 11);
        let route = registry.route_views(11).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::BlockedPoolRepair);
    }

    #[test]
    fn repair_failures_quarantine_and_then_disable_pool() {
        let mut config = LiveSetHealthConfig::default();
        config.pool_quarantine_after_repair_failures = 2;
        config.pool_disable_after_quarantine_count = 2;
        config.pool_disable_window_slots = 20;
        let mut registry = RouteHealthRegistry::new(config, 2);
        registry.register_route(
            RouteId("route-a".into()),
            &[
                state::types::PoolId("pool-a".into()),
                state::types::PoolId("pool-b".into()),
            ],
            32,
        );
        registry.on_repair_transition("pool-a", PoolHealthTransition::RepairFailed, 10);
        registry.on_repair_transition("pool-a", PoolHealthTransition::RepairFailed, 11);
        assert_eq!(
            registry.pool_view("pool-a", 11).unwrap().health_state,
            PoolHealthState::Quarantined
        );

        registry.on_pool_snapshot(
            &snapshot("pool-a", 12, 12, PoolConfidence::Executable, false),
            true,
            12,
        );
        registry.on_repair_transition("pool-a", PoolHealthTransition::RepairFailed, 20);
        registry.on_repair_transition("pool-a", PoolHealthTransition::RepairFailed, 21);
        assert_eq!(
            registry.pool_view("pool-a", 21).unwrap().health_state,
            PoolHealthState::Disabled
        );
    }

    #[test]
    fn refresh_failures_can_quarantine_pool_when_enabled() {
        let mut config = LiveSetHealthConfig::default();
        config.pool_quarantine_after_refresh_failures = 2;
        let mut registry = RouteHealthRegistry::new(config, 2);
        registry.register_route(
            RouteId("route-a".into()),
            &[state::types::PoolId("pool-a".into())],
            2,
        );

        registry.on_repair_transition("pool-a", PoolHealthTransition::RefreshFailed, 10);
        assert_eq!(
            registry.pool_view("pool-a", 10).unwrap().health_state,
            PoolHealthState::ExecutableRefreshPending
        );

        registry.on_repair_transition("pool-a", PoolHealthTransition::RefreshFailed, 11);
        let view = registry.pool_view("pool-a", 11).unwrap();
        assert_eq!(view.health_state, PoolHealthState::Quarantined);
        assert!(view.quarantined_until_slot.is_some());
    }

    #[test]
    fn route_moves_to_shadow_only_after_failure_budget() {
        let mut config = LiveSetHealthConfig::default();
        config.route_targeted_failure_cooldown_slots = 0;
        config.route_shadow_after_chain_failures = 2;
        config.route_reentry_cooldown_slots = 8;
        let mut registry = RouteHealthRegistry::new(config, 2);
        registry.register_route(
            RouteId("route-a".into()),
            &[
                state::types::PoolId("pool-a".into()),
                state::types::PoolId("pool-b".into()),
            ],
            32,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-a", 10, 10, PoolConfidence::Executable, false),
            true,
            10,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-b", 10, 10, PoolConfidence::Executable, false),
            true,
            10,
        );
        registry.on_execution_failure(
            &RouteId("route-a".into()),
            &FailureClass::ChainExecutionFailed,
            11,
        );
        registry.on_execution_failure(
            &RouteId("route-a".into()),
            &FailureClass::ChainExecutionFailed,
            12,
        );

        let route = registry.route_views(12).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::ShadowOnly);

        let route = registry.route_views(21).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::Eligible);
    }

    #[test]
    fn targeted_chain_failure_immediately_shadows_route() {
        let mut config = LiveSetHealthConfig::default();
        config.route_targeted_failure_cooldown_slots = 8;
        config.route_shadow_after_chain_failures = 2;
        let mut registry = RouteHealthRegistry::new(config, 2);
        registry.register_route(
            RouteId("route-a".into()),
            &[state::types::PoolId("pool-a".into())],
            32,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-a", 10, 10, PoolConfidence::Executable, false),
            true,
            10,
        );

        registry.on_execution_failure(
            &RouteId("route-a".into()),
            &FailureClass::ChainExecutionTooLittleOutput,
            11,
        );

        let route = registry.route_views(11).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::ShadowOnly);
        assert_eq!(route.shadow_until_slot, Some(19));

        let route = registry.route_views(20).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::Eligible);
    }

    #[test]
    fn route_becomes_stale_when_observed_slot_exceeds_route_execution_lag() {
        let mut registry = RouteHealthRegistry::new(LiveSetHealthConfig::default(), 256);
        registry.register_route(
            RouteId("route-a".into()),
            &[
                state::types::PoolId("pool-a".into()),
                state::types::PoolId("pool-b".into()),
            ],
            16,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-a", 100, 100, PoolConfidence::Executable, false),
            true,
            100,
        );
        registry.on_pool_snapshot(
            &snapshot("pool-b", 100, 100, PoolConfidence::Executable, false),
            true,
            100,
        );

        let route = registry.route_views(116).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::Eligible);

        let route = registry.route_views(117).pop().unwrap();
        assert_eq!(route.health_state, RouteHealthState::BlockedPoolStale);
        assert_eq!(route.blocking_reason.as_deref(), Some("snapshot_stale"));
    }

    #[test]
    fn route_specific_execution_lag_is_respected() {
        let mut registry = RouteHealthRegistry::new(LiveSetHealthConfig::default(), 256);
        let shared_pool = state::types::PoolId("pool-a".into());
        registry.register_route(
            RouteId("fast".into()),
            std::slice::from_ref(&shared_pool),
            16,
        );
        registry.register_route(RouteId("slow".into()), &[shared_pool], 32);
        registry.on_pool_snapshot(
            &snapshot("pool-a", 100, 100, PoolConfidence::Executable, false),
            true,
            100,
        );

        let views = registry
            .route_views(125)
            .into_iter()
            .map(|view| (view.route_id, view.health_state))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(views.get("fast"), Some(&RouteHealthState::BlockedPoolStale));
        assert_eq!(views.get("slow"), Some(&RouteHealthState::Eligible));
    }
}
