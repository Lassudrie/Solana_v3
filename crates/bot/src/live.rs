use std::{collections::BTreeSet, sync::Arc, time::Duration};

use detection::live::{
    OrcaSimpleTrackedConfig, OrcaWhirlpoolTrackedConfig, RaydiumClmmTrackedConfig,
    RaydiumSimpleTrackedConfig,
};
use detection::{
    EventSourceKind, GetMultipleAccountsBatcher, GrpcEntriesConfig,
    GrpcEntriesEventSource as DetectionGrpcEntriesEventSource, IngestError, LiveHooks,
    LiveRepairEvent, LiveRepairEventKind, LiveRepairTransition, LookupTableCacheHandle,
    MarketEventSource, NormalizedEvent, ReducerRolloutMode, TrackedPool, TrackedPoolKind,
};

use crate::{
    config::{
        BotConfig, EventSourceMode, RouteClassConfig, RouteLegExecutionConfig, RuntimeProfileConfig,
    },
    observer::{ObserverHandle, RepairEvent, RepairEventKind},
    route_health::{PoolHealthTransition, SharedRouteHealth},
};

#[derive(Clone)]
struct BotLiveHooks {
    observer: ObserverHandle,
    route_health: SharedRouteHealth,
}

impl LiveHooks for BotLiveHooks {
    fn pool_is_blocked_from_repair(&self, pool_id: &str, observed_slot: u64) -> bool {
        self.route_health
            .lock()
            .ok()
            .map(|health| health.pool_is_blocked_from_repair(pool_id, observed_slot))
            .unwrap_or(false)
    }

    fn publish_repair(&self, event: LiveRepairEvent) {
        self.observer.publish_repair(RepairEvent {
            pool_id: event.pool_id,
            kind: map_repair_event_kind(event.kind),
            occurred_at: event.occurred_at,
        });
    }

    fn on_repair_transition(
        &self,
        pool_id: &str,
        transition: LiveRepairTransition,
        observed_slot: u64,
    ) {
        if let Ok(mut health) = self.route_health.lock() {
            health.on_repair_transition(pool_id, map_repair_transition(transition), observed_slot);
        }
    }
}

fn map_repair_event_kind(kind: LiveRepairEventKind) -> RepairEventKind {
    match kind {
        LiveRepairEventKind::RefreshScheduled { deadline_slot } => {
            RepairEventKind::RefreshScheduled { deadline_slot }
        }
        LiveRepairEventKind::RefreshAttemptStarted => RepairEventKind::RefreshAttemptStarted,
        LiveRepairEventKind::RefreshAttemptFailed => RepairEventKind::RefreshAttemptFailed,
        LiveRepairEventKind::RefreshAttemptSucceeded { latency_ms } => {
            RepairEventKind::RefreshAttemptSucceeded { latency_ms }
        }
        LiveRepairEventKind::RefreshCleared => RepairEventKind::RefreshCleared,
        LiveRepairEventKind::RepairAttemptStarted => RepairEventKind::RepairAttemptStarted,
        LiveRepairEventKind::RepairAttemptFailed => RepairEventKind::RepairAttemptFailed,
        LiveRepairEventKind::RepairAttemptSucceeded { latency_ms } => {
            RepairEventKind::RepairAttemptSucceeded { latency_ms }
        }
    }
}

fn map_repair_transition(transition: LiveRepairTransition) -> PoolHealthTransition {
    match transition {
        LiveRepairTransition::RefreshScheduled => PoolHealthTransition::RefreshScheduled,
        LiveRepairTransition::RefreshStarted => PoolHealthTransition::RefreshStarted,
        LiveRepairTransition::RefreshFailed => PoolHealthTransition::RefreshFailed,
        LiveRepairTransition::RefreshSucceeded => PoolHealthTransition::RefreshSucceeded,
        LiveRepairTransition::RefreshCleared => PoolHealthTransition::RefreshCleared,
        LiveRepairTransition::RepairQueued => PoolHealthTransition::RepairQueued,
        LiveRepairTransition::RepairStarted => PoolHealthTransition::RepairStarted,
        LiveRepairTransition::RepairFailed => PoolHealthTransition::RepairFailed,
        LiveRepairTransition::RepairSucceeded => PoolHealthTransition::RepairSucceeded,
    }
}

fn reducer_mode_for_config(
    config: &BotConfig,
    leg_execution: &RouteLegExecutionConfig,
) -> ReducerRolloutMode {
    match leg_execution {
        RouteLegExecutionConfig::OrcaSimplePool(_) => {
            map_reducer_mode(config.shredstream.reducers.orca_simple_pool)
        }
        RouteLegExecutionConfig::RaydiumSimplePool(_) => {
            map_reducer_mode(config.shredstream.reducers.raydium_simple_pool)
        }
        RouteLegExecutionConfig::OrcaWhirlpool(_) => {
            map_reducer_mode(config.shredstream.reducers.orca_whirlpool)
        }
        RouteLegExecutionConfig::RaydiumClmm(_) => {
            map_reducer_mode(config.shredstream.reducers.raydium_clmm)
        }
    }
}

fn map_reducer_mode(mode: crate::config::ReducerRolloutMode) -> ReducerRolloutMode {
    match mode {
        crate::config::ReducerRolloutMode::Disabled => ReducerRolloutMode::Disabled,
        crate::config::ReducerRolloutMode::Shadow => ReducerRolloutMode::Shadow,
        crate::config::ReducerRolloutMode::Active => ReducerRolloutMode::Active,
    }
}

fn merge_reducer_mode(
    current: ReducerRolloutMode,
    incoming: ReducerRolloutMode,
) -> ReducerRolloutMode {
    match (current, incoming) {
        (ReducerRolloutMode::Active, _) | (_, ReducerRolloutMode::Active) => {
            ReducerRolloutMode::Active
        }
        (ReducerRolloutMode::Shadow, _) | (_, ReducerRolloutMode::Shadow) => {
            ReducerRolloutMode::Shadow
        }
        _ => ReducerRolloutMode::Disabled,
    }
}

fn merge_watch_accounts(current: &mut Vec<String>, incoming: Vec<String>) {
    for account in incoming {
        if !current.contains(&account) {
            current.push(account);
        }
    }
}

fn merge_tracked_pool(current: &mut TrackedPool, incoming: TrackedPool) {
    current.reducer_mode = merge_reducer_mode(current.reducer_mode, incoming.reducer_mode);
    merge_watch_accounts(&mut current.watch_accounts, incoming.watch_accounts);

    match (&mut current.kind, incoming.kind) {
        (TrackedPoolKind::OrcaSimple(_), TrackedPoolKind::OrcaSimple(_))
        | (TrackedPoolKind::RaydiumSimple(_), TrackedPoolKind::RaydiumSimple(_)) => {}
        (
            TrackedPoolKind::OrcaWhirlpool(current_config),
            TrackedPoolKind::OrcaWhirlpool(incoming_config),
        ) => {
            current_config.require_a_to_b |= incoming_config.require_a_to_b;
            current_config.require_b_to_a |= incoming_config.require_b_to_a;
        }
        (
            TrackedPoolKind::RaydiumClmm(current_config),
            TrackedPoolKind::RaydiumClmm(incoming_config),
        ) => {
            current_config.require_zero_for_one |= incoming_config.require_zero_for_one;
            current_config.require_one_for_zero |= incoming_config.require_one_for_zero;
        }
        _ => {}
    }
}

fn push_or_merge_tracked_pool(tracked_pools: &mut Vec<TrackedPool>, tracked: TrackedPool) {
    if let Some(existing) = tracked_pools
        .iter_mut()
        .find(|pool| pool.pool_id == tracked.pool_id)
    {
        merge_tracked_pool(existing, tracked);
    } else {
        tracked_pools.push(tracked);
    }
}

fn max_repair_workers(
    ultra_fast: bool,
    shredstream_live: bool,
    tracked_pools: &[TrackedPool],
) -> usize {
    if !ultra_fast {
        return 2;
    }

    let active_hybrid_pools = tracked_pools
        .iter()
        .filter(|pool| {
            pool.reducer_mode == ReducerRolloutMode::Active
                && matches!(
                    pool.kind,
                    TrackedPoolKind::OrcaWhirlpool(_) | TrackedPoolKind::RaydiumClmm(_)
                )
        })
        .map(|pool| pool.pool_id.as_str())
        .collect::<BTreeSet<_>>()
        .len();

    if active_hybrid_pools == 0 {
        1
    } else if shredstream_live {
        1
    } else {
        active_hybrid_pools.clamp(2, 8)
    }
}

fn configured_repair_workers(
    configured: usize,
    ultra_fast: bool,
    shredstream_live: bool,
    tracked_pools: &[TrackedPool],
) -> usize {
    if configured > 0 {
        return configured;
    }
    max_repair_workers(ultra_fast, shredstream_live, tracked_pools)
}

fn build_grpc_entries_config(config: &BotConfig) -> GrpcEntriesConfig {
    let ultra_fast = config.runtime.profile == RuntimeProfileConfig::UltraFast;
    let shredstream_live = config.runtime.event_source.mode == EventSourceMode::Shredstream;
    let mut tracked_pools = Vec::new();
    let mut lookup_table_keys = BTreeSet::new();

    for route in &config.routes.definitions {
        if !route.enabled || (ultra_fast && route.route_class != RouteClassConfig::AmmFastPath) {
            continue;
        }

        for table in &route.execution.lookup_tables {
            lookup_table_keys.insert(table.account_key.clone());
        }

        let token_mint_a = route
            .base_mint
            .clone()
            .unwrap_or_else(|| route.input_mint.clone());
        let token_mint_b = route
            .quote_mint
            .clone()
            .unwrap_or_else(|| route.output_mint.clone());

        for leg in &route.legs {
            let reducer_mode = reducer_mode_for_config(config, &leg.execution);
            let tracked = match &leg.execution {
                RouteLegExecutionConfig::OrcaSimplePool(exec) => TrackedPool {
                    pool_id: leg.pool_id.clone(),
                    reducer_mode,
                    watch_accounts: vec![
                        exec.swap_account.clone(),
                        exec.pool_source_token_account.clone(),
                        exec.pool_destination_token_account.clone(),
                    ],
                    kind: TrackedPoolKind::OrcaSimple(OrcaSimpleTrackedConfig {
                        program_id: exec.program_id.clone(),
                        swap_account: exec.swap_account.clone(),
                        token_vault_a: exec.pool_source_token_account.clone(),
                        token_vault_b: exec.pool_destination_token_account.clone(),
                        token_mint_a: token_mint_a.clone(),
                        token_mint_b: token_mint_b.clone(),
                        fee_bps: leg.fee_bps.unwrap_or_default(),
                    }),
                },
                RouteLegExecutionConfig::RaydiumSimplePool(exec) => TrackedPool {
                    pool_id: leg.pool_id.clone(),
                    reducer_mode,
                    watch_accounts: vec![
                        exec.amm_pool.clone(),
                        exec.amm_open_orders.clone(),
                        exec.amm_coin_vault.clone(),
                        exec.amm_pc_vault.clone(),
                        exec.market.clone(),
                        exec.market_bids.clone(),
                        exec.market_asks.clone(),
                        exec.market_event_queue.clone(),
                        exec.market_coin_vault.clone(),
                        exec.market_pc_vault.clone(),
                    ],
                    kind: TrackedPoolKind::RaydiumSimple(RaydiumSimpleTrackedConfig {
                        program_id: exec.program_id.clone(),
                        token_program_id: exec.token_program_id.clone(),
                        amm_pool: exec.amm_pool.clone(),
                        token_vault_a: exec.amm_coin_vault.clone(),
                        token_vault_b: exec.amm_pc_vault.clone(),
                        token_mint_a: token_mint_a.clone(),
                        token_mint_b: token_mint_b.clone(),
                        fee_bps: leg.fee_bps.unwrap_or_default(),
                    }),
                },
                RouteLegExecutionConfig::OrcaWhirlpool(exec) => TrackedPool {
                    pool_id: leg.pool_id.clone(),
                    reducer_mode,
                    watch_accounts: vec![
                        exec.whirlpool.clone(),
                        exec.token_vault_a.clone(),
                        exec.token_vault_b.clone(),
                    ],
                    kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                        program_id: exec.program_id.clone(),
                        whirlpool: exec.whirlpool.clone(),
                        token_mint_a: exec.token_mint_a.clone(),
                        token_mint_b: exec.token_mint_b.clone(),
                        token_vault_a: exec.token_vault_a.clone(),
                        token_vault_b: exec.token_vault_b.clone(),
                        tick_spacing: exec.tick_spacing,
                        fee_bps: leg.fee_bps.unwrap_or_default(),
                        require_a_to_b: exec.a_to_b,
                        require_b_to_a: !exec.a_to_b,
                    }),
                },
                RouteLegExecutionConfig::RaydiumClmm(exec) => TrackedPool {
                    pool_id: leg.pool_id.clone(),
                    reducer_mode,
                    watch_accounts: vec![
                        exec.pool_state.clone(),
                        exec.token_vault_0.clone(),
                        exec.token_vault_1.clone(),
                        exec.observation_state.clone(),
                    ],
                    kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                        program_id: exec.program_id.clone(),
                        pool_state: exec.pool_state.clone(),
                        token_mint_a: exec.token_mint_0.clone(),
                        token_mint_b: exec.token_mint_1.clone(),
                        token_vault_a: exec.token_vault_0.clone(),
                        token_vault_b: exec.token_vault_1.clone(),
                        tick_spacing: exec.tick_spacing,
                        fee_bps: leg.fee_bps.unwrap_or_default(),
                        require_zero_for_one: exec.zero_for_one,
                        require_one_for_zero: !exec.zero_for_one,
                    }),
                },
            };
            push_or_merge_tracked_pool(&mut tracked_pools, tracked);
        }
    }

    GrpcEntriesConfig {
        grpc_endpoint: config.shredstream.grpc_endpoint.clone(),
        buffer_capacity: config.shredstream.buffer_capacity,
        grpc_connect_timeout_ms: config.shredstream.grpc_connect_timeout_ms,
        reconnect_backoff_millis: config.shredstream.reconnect_backoff_millis,
        max_reconnect_backoff_millis: config.shredstream.max_reconnect_backoff_millis,
        idle_refresh_slot_lag: config.shredstream.idle_refresh_slot_lag,
        max_repair_in_flight: configured_repair_workers(
            config.shredstream.max_repair_in_flight,
            ultra_fast,
            shredstream_live,
            &tracked_pools,
        ),
        tracked_pools,
        lookup_table_keys: lookup_table_keys.into_iter().collect(),
    }
}

#[derive(Debug)]
pub struct GrpcEntriesEventSource {
    inner: DetectionGrpcEntriesEventSource,
}

impl GrpcEntriesEventSource {
    pub fn spawn(
        config: &BotConfig,
        observer: ObserverHandle,
        route_health: SharedRouteHealth,
        account_batcher: GetMultipleAccountsBatcher,
        lookup_table_cache: LookupTableCacheHandle,
    ) -> Result<Self, String> {
        let hooks: Arc<dyn LiveHooks> = Arc::new(BotLiveHooks {
            observer,
            route_health,
        });
        let inner = DetectionGrpcEntriesEventSource::spawn(
            build_grpc_entries_config(config),
            hooks,
            account_batcher,
            lookup_table_cache,
        )?;
        Ok(Self { inner })
    }
}

impl MarketEventSource for GrpcEntriesEventSource {
    fn source_kind(&self) -> EventSourceKind {
        self.inner.source_kind()
    }

    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError> {
        self.inner.poll_next()
    }

    fn wait_next(&mut self, timeout: Duration) -> Result<Option<NormalizedEvent>, IngestError> {
        self.inner.wait_next(timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::{BotConfig, EventSourceMode, RuntimeProfileConfig};

    use super::{
        TrackedPoolKind, build_grpc_entries_config, configured_repair_workers, max_repair_workers,
    };

    fn repo_root_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(file)
    }

    #[test]
    fn standard_profile_keeps_two_repair_workers() {
        let config = BotConfig::default();
        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.max_repair_in_flight, 2);
    }

    #[test]
    fn ultra_fast_uses_single_repair_worker_without_hybrid_pools() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.max_repair_in_flight, 1);
        assert_eq!(max_repair_workers(true, false, &grpc.tracked_pools), 1);
    }

    #[test]
    fn ultra_fast_amm_fast_manifest_scales_repair_workers_for_hybrid_pools() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let grpc = build_grpc_entries_config(&config);

        assert!(grpc.max_repair_in_flight > 1);
    }

    #[test]
    fn ultra_fast_all_clmm_manifest_caps_repair_workers_at_eight() {
        let config = BotConfig::from_path(repo_root_path("amm_12_pairs_fast.toml")).unwrap();
        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.max_repair_in_flight, 4);
    }

    #[test]
    fn ultra_fast_shredstream_live_manifest_can_override_repair_workers() {
        let mut config =
            BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        config.runtime.event_source.mode = EventSourceMode::Shredstream;

        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.max_repair_in_flight, 4);
        assert_eq!(max_repair_workers(true, true, &grpc.tracked_pools), 1);
    }

    #[test]
    fn explicit_repair_worker_override_wins_over_auto_scaling() {
        let mut config =
            BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        config.runtime.event_source.mode = EventSourceMode::Shredstream;
        config.shredstream.max_repair_in_flight = 4;

        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.max_repair_in_flight, 4);
        assert_eq!(
            configured_repair_workers(4, true, true, &grpc.tracked_pools),
            4
        );
    }

    #[test]
    fn shared_clmm_pool_merges_required_directions_across_routes() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let grpc = build_grpc_entries_config(&config);

        assert_eq!(grpc.tracked_pools.len(), 6);

        let whirlpool = grpc
            .tracked_pools
            .iter()
            .find(|pool| pool.pool_id == "2UhFnySoJi6c89aydGAGS7ZRemo2dbkFRhvSJqDX4gHJ")
            .expect("tracked JTO whirlpool");
        let TrackedPoolKind::OrcaWhirlpool(config) = &whirlpool.kind else {
            panic!("expected JTO pool to be tracked as an Orca whirlpool");
        };
        assert!(config.require_a_to_b);
        assert!(config.require_b_to_a);
    }
}
