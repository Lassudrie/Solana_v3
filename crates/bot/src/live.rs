use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use detection::events::SnapshotConfidence;
use detection::{
    EventSourceKind, IngestError, MarketEventSource, NormalizedEvent, PoolInvalidation,
    PoolSnapshotUpdate,
};
use prost_types::Timestamp as ProstTimestamp;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use solana_entry_legacy::entry::Entry as LegacyEntry;
use solana_message_legacy::{
    AccountKeys, VersionedMessage as LegacyVersionedMessage,
    compiled_instruction::CompiledInstruction, v0,
};
use solana_pubkey_legacy::Pubkey as LegacyPubkey;
use solana_sdk::hash::hashv;
use solana_transaction_legacy::versioned::VersionedTransaction as LegacyVersionedTransaction;
use state::{
    executable_pool_state::ExecutablePoolStateStore,
    types::{
        ConcentratedLiquidityPoolState, ConstantProductPoolState, ExecutablePoolState,
        LookupTableSnapshot, PoolConfidence, PoolId, PoolVenue,
    },
};
use tonic::transport::Endpoint;

use crate::config::{
    BotConfig, ReducerRolloutMode, RouteClassConfig, RouteLegExecutionConfig, RuntimeProfileConfig,
};
use crate::observer::{ObserverHandle, RepairEvent, RepairEventKind};
use crate::route_health::{PoolHealthTransition, SharedRouteHealth};

#[allow(dead_code)]
pub mod shared {
    tonic::include_proto!("shared");
}

#[allow(dead_code)]
pub mod proto {
    tonic::include_proto!("shredstream");
}

const JSON_RPC_VERSION: &str = "2.0";
const MOCK_SCHEME: &str = "mock://";
const ORCA_SWAP_INSTRUCTION_TAG: u8 = 1;
const RAYDIUM_SWAP_BASE_IN_TAG: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT_TAG: u8 = 11;
const ORCA_SWAP_DISCRIMINATOR_NAME: &str = "swap";
const ORCA_SWAP_V2_DISCRIMINATOR_NAME: &str = "swap_v2";
const ORCA_TWO_HOP_SWAP_DISCRIMINATOR_NAME: &str = "two_hop_swap";
const ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME: &str = "two_hop_swap_v2";
const RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME: &str = "swap_v2";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const CLMM_REFRESH_GRACE_SLOTS: u64 = 8;
const ORCA_WHIRLPOOL_ACCOUNT_NAME: &str = "Whirlpool";
const RAYDIUM_CLMM_ACCOUNT_NAME: &str = "PoolState";
const ORCA_WHIRLPOOL_ACCOUNT_MIN_LEN: usize = 245;
const RAYDIUM_CLMM_ACCOUNT_MIN_LEN: usize = 273;

#[derive(Debug)]
pub struct GrpcEntriesEventSource {
    receiver: Receiver<NormalizedEvent>,
}

impl GrpcEntriesEventSource {
    pub fn spawn(
        config: &BotConfig,
        observer: ObserverHandle,
        route_health: SharedRouteHealth,
    ) -> Result<Self, String> {
        Endpoint::from_shared(config.shredstream.grpc_endpoint.clone())
            .map_err(|error| error.to_string())?;

        let registry = Arc::new(TrackedPoolRegistry::from_config(config));
        let executable_states = Arc::new(RwLock::new(ExecutablePoolStateStore::default()));
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (event_tx, event_rx) = mpsc::sync_channel(config.shredstream.buffer_capacity.max(64));
        let sequence = Arc::new(AtomicU64::new(1));

        let lookup_cache = Arc::new(RwLock::new(HashMap::<String, LookupTableSnapshot>::new()));
        spawn_lookup_table_refresher(
            config,
            Arc::clone(&lookup_cache),
            registry.lookup_table_keys(),
        );

        let repair_tx = spawn_repair_worker(
            config,
            Arc::clone(&registry),
            Arc::clone(&executable_states),
            Arc::clone(&sync_tracker),
            event_tx.clone(),
            Arc::clone(&sequence),
            observer.clone(),
            route_health.clone(),
        );

        for pool_id in registry.pool_ids() {
            let _ = repair_tx.send(RepairRequest {
                pool_id,
                mode: SyncRequestMode::RepairAfterInvalidation,
                priority: RepairPriority::Immediate,
                observed_slot: 0,
            });
        }

        spawn_stream_worker(
            config,
            registry,
            executable_states,
            sync_tracker,
            lookup_cache,
            event_tx,
            repair_tx,
            sequence,
            observer,
            route_health,
        );

        Ok(Self { receiver: event_rx })
    }
}

impl MarketEventSource for GrpcEntriesEventSource {
    fn source_kind(&self) -> EventSourceKind {
        EventSourceKind::ShredStream
    }

    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: "live shredstream worker disconnected".into(),
            }),
        }
    }

    fn wait_next(&mut self, timeout: Duration) -> Result<Option<NormalizedEvent>, IngestError> {
        if timeout.is_zero() {
            return self.poll_next();
        }

        match self.receiver.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: "live shredstream worker disconnected".into(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrcaSimpleTrackedConfig {
    program_id: String,
    swap_account: String,
    token_vault_a: String,
    token_vault_b: String,
    token_mint_a: String,
    token_mint_b: String,
    fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RaydiumSimpleTrackedConfig {
    program_id: String,
    token_program_id: String,
    amm_pool: String,
    token_vault_a: String,
    token_vault_b: String,
    token_mint_a: String,
    token_mint_b: String,
    fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrcaWhirlpoolTrackedConfig {
    program_id: String,
    whirlpool: String,
    token_mint_a: String,
    token_mint_b: String,
    token_vault_a: String,
    token_vault_b: String,
    tick_spacing: u16,
    fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RaydiumClmmTrackedConfig {
    program_id: String,
    pool_state: String,
    token_mint_a: String,
    token_mint_b: String,
    token_vault_a: String,
    token_vault_b: String,
    tick_spacing: u16,
    fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrackedPoolKind {
    OrcaSimple(OrcaSimpleTrackedConfig),
    RaydiumSimple(RaydiumSimpleTrackedConfig),
    OrcaWhirlpool(OrcaWhirlpoolTrackedConfig),
    RaydiumClmm(RaydiumClmmTrackedConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrackedPool {
    pool_id: String,
    reducer_mode: ReducerRolloutMode,
    kind: TrackedPoolKind,
    watch_accounts: Vec<String>,
}

impl TrackedPool {
    fn repair_account_keys(&self) -> Vec<String> {
        match &self.kind {
            TrackedPoolKind::OrcaSimple(config) => vec![
                config.swap_account.clone(),
                config.token_vault_a.clone(),
                config.token_vault_b.clone(),
            ],
            TrackedPoolKind::RaydiumSimple(config) => vec![
                config.amm_pool.clone(),
                config.token_vault_a.clone(),
                config.token_vault_b.clone(),
            ],
            TrackedPoolKind::OrcaWhirlpool(config) => vec![config.whirlpool.clone()],
            TrackedPoolKind::RaydiumClmm(config) => vec![config.pool_state.clone()],
        }
    }

    fn uses_hybrid_refresh(&self) -> bool {
        self.reducer_mode == ReducerRolloutMode::Active
            && matches!(
                self.kind,
                TrackedPoolKind::OrcaWhirlpool(_) | TrackedPoolKind::RaydiumClmm(_)
            )
    }
}

#[derive(Debug, Default)]
struct TrackedPoolRegistry {
    pools_by_id: HashMap<String, TrackedPool>,
    pools_by_watch_account: HashMap<String, Vec<String>>,
    orca_simple_by_account: HashMap<String, String>,
    raydium_simple_by_account: HashMap<String, String>,
    orca_whirlpool_by_account: HashMap<String, String>,
    raydium_clmm_by_account: HashMap<String, String>,
    lookup_table_keys: Vec<String>,
}

impl TrackedPoolRegistry {
    fn from_config(config: &BotConfig) -> Self {
        let mut registry = Self::default();
        let mut lookup_table_keys = BTreeSet::new();

        for route in &config.routes.definitions {
            if !route.enabled
                || (config.runtime.profile == RuntimeProfileConfig::UltraFast
                    && route.route_class != RouteClassConfig::AmmFastPath)
            {
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
                if registry.pools_by_id.contains_key(&leg.pool_id) {
                    continue;
                }

                let tracked = match &leg.execution {
                    RouteLegExecutionConfig::OrcaSimplePool(exec) => TrackedPool {
                        pool_id: leg.pool_id.clone(),
                        reducer_mode: config.shredstream.reducers.orca_simple_pool,
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
                        reducer_mode: config.shredstream.reducers.raydium_simple_pool,
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
                        reducer_mode: config.shredstream.reducers.orca_whirlpool,
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
                        }),
                    },
                    RouteLegExecutionConfig::RaydiumClmm(exec) => TrackedPool {
                        pool_id: leg.pool_id.clone(),
                        reducer_mode: config.shredstream.reducers.raydium_clmm,
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
                        }),
                    },
                };

                registry.register_pool(tracked);
            }
        }

        registry.lookup_table_keys = lookup_table_keys.into_iter().collect();
        registry
    }

    fn register_pool(&mut self, tracked: TrackedPool) {
        match &tracked.kind {
            TrackedPoolKind::OrcaSimple(config) => {
                self.orca_simple_by_account
                    .insert(config.swap_account.clone(), tracked.pool_id.clone());
            }
            TrackedPoolKind::RaydiumSimple(config) => {
                self.raydium_simple_by_account
                    .insert(config.amm_pool.clone(), tracked.pool_id.clone());
            }
            TrackedPoolKind::OrcaWhirlpool(config) => {
                self.orca_whirlpool_by_account
                    .insert(config.whirlpool.clone(), tracked.pool_id.clone());
            }
            TrackedPoolKind::RaydiumClmm(config) => {
                self.raydium_clmm_by_account
                    .insert(config.pool_state.clone(), tracked.pool_id.clone());
            }
        }

        for account in &tracked.watch_accounts {
            self.pools_by_watch_account
                .entry(account.clone())
                .or_default()
                .push(tracked.pool_id.clone());
        }
        self.pools_by_id.insert(tracked.pool_id.clone(), tracked);
    }

    fn lookup_table_keys(&self) -> Vec<String> {
        self.lookup_table_keys.clone()
    }

    fn pool_ids(&self) -> Vec<String> {
        self.pools_by_id.keys().cloned().collect()
    }

    fn tracked_pool(&self, pool_id: &str) -> Option<TrackedPool> {
        self.pools_by_id.get(pool_id).cloned()
    }

    fn touched_pools(&self, account_keys: &[String]) -> Vec<TrackedPool> {
        let mut touched = Vec::new();
        let mut seen = HashSet::new();
        for account_key in account_keys {
            let Some(pool_ids) = self.pools_by_watch_account.get(account_key) else {
                continue;
            };
            for pool_id in pool_ids {
                if !seen.insert(pool_id.clone()) {
                    continue;
                }
                if let Some(pool) = self.tracked_pool(pool_id) {
                    touched.push(pool);
                }
            }
        }
        touched
    }

    fn decode_instruction(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        self.decode_orca_simple(instruction)
            .or_else(|| self.decode_orca_whirlpool(instruction))
            .or_else(|| self.decode_raydium_clmm(instruction))
            .or_else(|| self.decode_raydium_simple(instruction))
    }

    fn decode_orca_simple(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        if instruction.data.first().copied()? != ORCA_SWAP_INSTRUCTION_TAG {
            return None;
        }
        let pool_id = self
            .orca_simple_by_account
            .get(instruction.accounts.first()?)?
            .clone();
        let tracked = self.tracked_pool(&pool_id)?;
        let TrackedPoolKind::OrcaSimple(config) = &tracked.kind else {
            return None;
        };
        if instruction.program_id != config.program_id || instruction.accounts.len() < 6 {
            return None;
        }
        Some(PoolMutation::OrcaSimpleSwap {
            pool_id,
            source_vault: instruction.accounts.get(4)?.clone(),
            destination_vault: instruction.accounts.get(5)?.clone(),
            amount_in: read_u64(&instruction.data, 1)?,
            min_output_amount: read_u64(&instruction.data, 9)?,
        })
    }

    fn decode_raydium_simple(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        let tag = instruction.data.first().copied()?;
        if !matches!(tag, RAYDIUM_SWAP_BASE_IN_TAG | RAYDIUM_SWAP_BASE_OUT_TAG) {
            return None;
        }
        let pool_id = self
            .raydium_simple_by_account
            .get(instruction.accounts.get(1)?)?
            .clone();
        let tracked = self.tracked_pool(&pool_id)?;
        let TrackedPoolKind::RaydiumSimple(config) = &tracked.kind else {
            return None;
        };
        if instruction.program_id != config.program_id || instruction.accounts.len() < 17 {
            return None;
        }
        Some(PoolMutation::RaydiumSimpleSwap {
            pool_id,
            exact_out: tag == RAYDIUM_SWAP_BASE_OUT_TAG,
            user_owner: instruction.accounts.get(16)?.clone(),
            user_source_token_account: instruction.accounts.get(14)?.clone(),
            user_destination_token_account: instruction.accounts.get(15)?.clone(),
            amount_specified: read_u64(&instruction.data, 1)?,
            min_output_amount: read_u64(&instruction.data, 9)?,
        })
    }

    fn decode_orca_whirlpool(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        if instruction_has_anchor_discriminator(instruction, ORCA_SWAP_DISCRIMINATOR_NAME) {
            return self.decode_orca_whirlpool_variant(instruction, &[(2, 40, 41)]);
        }
        if instruction_has_anchor_discriminator(instruction, ORCA_SWAP_V2_DISCRIMINATOR_NAME) {
            return self.decode_orca_whirlpool_variant(instruction, &[(4, 40, 41)]);
        }
        if instruction_has_anchor_discriminator(instruction, ORCA_TWO_HOP_SWAP_DISCRIMINATOR_NAME) {
            return self.decode_orca_whirlpool_variant(instruction, &[(2, 24, 25), (3, 24, 26)]);
        }
        if instruction_has_anchor_discriminator(
            instruction,
            ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME,
        ) {
            return self.decode_orca_whirlpool_variant(instruction, &[(0, 24, 25), (1, 24, 26)]);
        }
        None
    }

    fn decode_orca_whirlpool_variant(
        &self,
        instruction: &ResolvedInstruction,
        candidates: &[(usize, usize, usize)],
    ) -> Option<PoolMutation> {
        for (pool_account_index, amount_specified_is_input_offset, a_to_b_offset) in candidates {
            let Some(pool_account) = instruction.accounts.get(*pool_account_index) else {
                continue;
            };
            let Some(pool_id) = self.orca_whirlpool_by_account.get(pool_account).cloned() else {
                continue;
            };
            let tracked = self.tracked_pool(&pool_id)?;
            let TrackedPoolKind::OrcaWhirlpool(config) = &tracked.kind else {
                return None;
            };
            if instruction.program_id != config.program_id {
                return None;
            }
            return Some(PoolMutation::OrcaWhirlpoolSwap {
                pool_id,
                amount: read_u64(&instruction.data, 8)?,
                other_amount_threshold: read_u64(&instruction.data, 16)?,
                exact_out: instruction
                    .data
                    .get(*amount_specified_is_input_offset)
                    .copied()?
                    == 0,
                a_to_b: instruction.data.get(*a_to_b_offset).copied()? == 1,
            });
        }

        None
    }

    fn decode_raydium_clmm(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        if !instruction_has_anchor_discriminator(instruction, RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME)
        {
            return None;
        }
        let pool_id = self
            .raydium_clmm_by_account
            .get(instruction.accounts.get(2)?)?
            .clone();
        let tracked = self.tracked_pool(&pool_id)?;
        let TrackedPoolKind::RaydiumClmm(config) = &tracked.kind else {
            return None;
        };
        if instruction.program_id != config.program_id || instruction.accounts.len() < 7 {
            return None;
        }
        let zero_for_one = instruction.accounts.get(5)? == &config.token_vault_a
            && instruction.accounts.get(6)? == &config.token_vault_b;
        Some(PoolMutation::RaydiumClmmSwap {
            pool_id,
            amount: read_u64(&instruction.data, 8)?,
            other_amount_threshold: read_u64(&instruction.data, 16)?,
            exact_out: instruction.data.get(40).copied()? == 0,
            zero_for_one,
        })
    }
}

#[derive(Debug, Clone)]
struct RepairRequest {
    pool_id: String,
    mode: SyncRequestMode,
    priority: RepairPriority,
    observed_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairPriority {
    Immediate,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SyncRequestMode {
    RefreshExact,
    RepairAfterInvalidation,
}

#[derive(Debug, Clone)]
struct RepairJob {
    pool_id: String,
    attempt: u32,
    mode: SyncRequestMode,
    observed_slot: u64,
}

#[derive(Debug)]
enum RepairJobResult {
    Success {
        pool_id: String,
        mode: SyncRequestMode,
        slot: u64,
        next_state: ExecutablePoolState,
        attempt: u32,
        latency: Duration,
    },
    Retry {
        pool_id: String,
        mode: SyncRequestMode,
        attempt: u32,
        observed_slot: u64,
    },
    Dropped {
        pool_id: String,
        mode: SyncRequestMode,
        observed_slot: u64,
    },
}

#[derive(Debug, Clone, Default)]
struct PoolSyncState {
    refresh_pending: bool,
    refresh_in_flight: bool,
    refresh_deadline_slot: Option<u64>,
    repair_pending: bool,
    repair_in_flight: bool,
}

#[derive(Debug, Default)]
struct PoolSyncTracker {
    pools: HashMap<String, PoolSyncState>,
}

impl PoolSyncTracker {
    fn schedule_refresh(&mut self, pool_id: &str, observed_slot: u64) -> (u64, bool) {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        let deadline_slot = observed_slot.saturating_add(CLMM_REFRESH_GRACE_SLOTS);
        let enqueue = !entry.refresh_pending;
        entry.refresh_pending = true;
        entry.refresh_deadline_slot = Some(
            entry
                .refresh_deadline_slot
                .map(|current| current.max(deadline_slot))
                .unwrap_or(deadline_slot),
        );
        (
            entry.refresh_deadline_slot.unwrap_or(deadline_slot),
            enqueue,
        )
    }

    fn mark_refresh_started(&mut self, pool_id: &str) {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        entry.refresh_pending = true;
        entry.refresh_in_flight = true;
    }

    fn mark_refresh_failed(&mut self, pool_id: &str) {
        if let Some(entry) = self.pools.get_mut(pool_id) {
            entry.refresh_in_flight = false;
        }
    }

    fn clear_refresh(&mut self, pool_id: &str) -> bool {
        let Some(entry) = self.pools.get_mut(pool_id) else {
            return false;
        };
        let was_pending = entry.refresh_pending || entry.refresh_deadline_slot.is_some();
        entry.refresh_pending = false;
        entry.refresh_in_flight = false;
        entry.refresh_deadline_slot = None;
        let remove = !entry.refresh_pending && !entry.refresh_in_flight;
        let _ = entry;
        if remove {
            self.pools.remove(pool_id);
        }
        was_pending
    }

    fn refresh_required(&self, pool_id: &str) -> bool {
        self.pools
            .get(pool_id)
            .map(|entry| entry.refresh_pending)
            .unwrap_or(false)
    }

    fn repair_requested(&mut self, pool_id: &str) -> bool {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        if entry.repair_pending || entry.repair_in_flight {
            return false;
        }
        entry.repair_pending = true;
        true
    }

    fn mark_repair_started(&mut self, pool_id: &str) {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        entry.repair_pending = true;
        entry.repair_in_flight = true;
    }

    fn mark_repair_failed(&mut self, pool_id: &str) {
        if let Some(entry) = self.pools.get_mut(pool_id) {
            entry.repair_pending = true;
            entry.repair_in_flight = false;
        }
    }

    fn clear_repair(&mut self, pool_id: &str) -> bool {
        let Some(entry) = self.pools.get_mut(pool_id) else {
            return false;
        };
        let was_pending = entry.repair_pending || entry.repair_in_flight;
        entry.repair_pending = false;
        entry.repair_in_flight = false;
        let remove = !entry.refresh_pending
            && !entry.refresh_in_flight
            && !entry.repair_pending
            && !entry.repair_in_flight;
        let _ = entry;
        if remove {
            self.pools.remove(pool_id);
        }
        was_pending
    }

    fn repair_required(&self, pool_id: &str) -> bool {
        self.pools
            .get(pool_id)
            .map(|entry| entry.repair_pending || entry.repair_in_flight)
            .unwrap_or(false)
    }

    fn take_expired_refreshes(&mut self, slot: u64) -> Vec<String> {
        let expired = self
            .pools
            .iter()
            .filter_map(|(pool_id, entry)| {
                (entry.refresh_pending
                    && !entry.refresh_in_flight
                    && entry
                        .refresh_deadline_slot
                        .map(|deadline| deadline < slot)
                        .unwrap_or(false))
                .then_some(pool_id.clone())
            })
            .collect::<Vec<_>>();
        for pool_id in &expired {
            self.clear_refresh(pool_id);
        }
        expired
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedInstruction {
    program_id: String,
    accounts: Vec<String>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedTransaction {
    account_keys: Vec<String>,
    writable_account_keys: Vec<String>,
    instructions: Vec<ResolvedInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PoolMutation {
    OrcaSimpleSwap {
        pool_id: String,
        source_vault: String,
        destination_vault: String,
        amount_in: u64,
        min_output_amount: u64,
    },
    RaydiumSimpleSwap {
        pool_id: String,
        exact_out: bool,
        user_owner: String,
        user_source_token_account: String,
        user_destination_token_account: String,
        amount_specified: u64,
        min_output_amount: u64,
    },
    OrcaWhirlpoolSwap {
        pool_id: String,
        amount: u64,
        other_amount_threshold: u64,
        exact_out: bool,
        a_to_b: bool,
    },
    RaydiumClmmSwap {
        pool_id: String,
        amount: u64,
        other_amount_threshold: u64,
        exact_out: bool,
        zero_for_one: bool,
    },
}

impl PoolMutation {
    fn pool_id(&self) -> &str {
        match self {
            Self::OrcaSimpleSwap { pool_id, .. }
            | Self::RaydiumSimpleSwap { pool_id, .. }
            | Self::OrcaWhirlpoolSwap { pool_id, .. }
            | Self::RaydiumClmmSwap { pool_id, .. } => pool_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReductionStatus {
    Probable,
    Invalid,
}

fn spawn_stream_worker(
    config: &BotConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    lookup_cache: Arc<RwLock<HashMap<String, LookupTableSnapshot>>>,
    event_tx: SyncSender<NormalizedEvent>,
    repair_tx: mpsc::Sender<RepairRequest>,
    sequence: Arc<AtomicU64>,
    observer: ObserverHandle,
    route_health: SharedRouteHealth,
) {
    let grpc_endpoint = config.shredstream.grpc_endpoint.clone();
    let connect_timeout = Duration::from_millis(config.shredstream.grpc_connect_timeout_ms.max(1));
    let base_backoff = Duration::from_millis(config.shredstream.reconnect_backoff_millis.max(1));
    let max_backoff = Duration::from_millis(config.shredstream.max_reconnect_backoff_millis.max(1));

    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime for shredstream");

        runtime.block_on(async move {
            let mut backoff = base_backoff;
            loop {
                let endpoint = match Endpoint::from_shared(grpc_endpoint.clone()) {
                    Ok(endpoint) => endpoint.connect_timeout(connect_timeout),
                    Err(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                        continue;
                    }
                };

                let connect_result =
                    proto::shredstream_proxy_client::ShredstreamProxyClient::connect(endpoint)
                        .await;
                let Ok(mut client) = connect_result else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                };

                let subscribe_result = client
                    .subscribe_entries(proto::SubscribeEntriesRequest {})
                    .await;
                let Ok(response) = subscribe_result else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                };

                backoff = base_backoff;
                let mut stream = response.into_inner();

                while let Ok(Some(entry)) = stream.message().await {
                    let source_received_at = SystemTime::now();
                    let source_latency = entry
                        .created_at
                        .as_ref()
                        .and_then(timestamp_to_system_time)
                        .and_then(|created_at| source_received_at.duration_since(created_at).ok());
                    publish_slot_boundary(
                        &event_tx,
                        &sequence,
                        entry.slot,
                        source_received_at,
                        source_latency,
                    );
                    handle_entry_batch(
                        entry.slot,
                        &entry.entries,
                        source_received_at,
                        source_latency,
                        &registry,
                        &executable_states,
                        &sync_tracker,
                        &lookup_cache,
                        &event_tx,
                        &repair_tx,
                        &sequence,
                        &observer,
                        &route_health,
                    );
                }

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        });
    });
}

fn handle_entry_batch(
    slot: u64,
    payload: &[u8],
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    registry: &TrackedPoolRegistry,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    lookup_cache: &Arc<RwLock<HashMap<String, LookupTableSnapshot>>>,
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
) {
    let Ok(entries) = bincode::deserialize::<Vec<LegacyEntry>>(payload) else {
        return;
    };

    let lookup_tables = lookup_cache
        .read()
        .ok()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    let mut invalidated_pools = HashSet::new();
    expire_exact_refreshes(
        slot,
        registry,
        executable_states,
        sync_tracker,
        event_tx,
        repair_tx,
        sequence,
        observer,
        route_health,
        source_received_at,
        source_latency,
        &mut invalidated_pools,
    );

    for entry in entries {
        for transaction in entry.transactions {
            let Some(resolved) = resolve_transaction(&transaction, &lookup_tables) else {
                continue;
            };
            let mut handled_pools = HashSet::new();

            for instruction in &resolved.instructions {
                let Some(mutation) = registry.decode_instruction(instruction) else {
                    continue;
                };
                let pool_id = mutation.pool_id().to_string();
                handled_pools.insert(pool_id.clone());
                let Some(tracked_pool) = registry.tracked_pool(&pool_id) else {
                    continue;
                };
                match tracked_pool.reducer_mode {
                    ReducerRolloutMode::Active => match apply_mutation_exact(
                        slot,
                        &tracked_pool,
                        &mutation,
                        executable_states,
                    ) {
                        Some(next_state) => {
                            publish_pool_snapshot_update(
                                event_tx,
                                sequence,
                                slot,
                                source_received_at,
                                source_latency,
                                &next_state,
                            );
                        }
                        None if tracked_pool.uses_hybrid_refresh() => schedule_exact_refresh(
                            &tracked_pool,
                            slot,
                            sync_tracker,
                            repair_tx,
                            observer,
                            route_health,
                        ),
                        None => invalidate_and_repair(
                            &tracked_pool,
                            slot,
                            source_received_at,
                            source_latency,
                            executable_states,
                            sync_tracker,
                            event_tx,
                            repair_tx,
                            sequence,
                            observer,
                            route_health,
                            &mut invalidated_pools,
                        ),
                    },
                    ReducerRolloutMode::Shadow => {
                        apply_mutation_shadow(slot, &tracked_pool, &mutation, executable_states);
                        invalidate_and_repair(
                            &tracked_pool,
                            slot,
                            source_received_at,
                            source_latency,
                            executable_states,
                            sync_tracker,
                            event_tx,
                            repair_tx,
                            sequence,
                            observer,
                            route_health,
                            &mut invalidated_pools,
                        );
                    }
                    ReducerRolloutMode::Disabled => invalidate_and_repair(
                        &tracked_pool,
                        slot,
                        source_received_at,
                        source_latency,
                        executable_states,
                        sync_tracker,
                        event_tx,
                        repair_tx,
                        sequence,
                        observer,
                        route_health,
                        &mut invalidated_pools,
                    ),
                }
            }

            for tracked_pool in registry.touched_pools(&resolved.writable_account_keys) {
                if handled_pools.contains(&tracked_pool.pool_id) {
                    continue;
                }
                if tracked_pool.uses_hybrid_refresh() {
                    schedule_exact_refresh(
                        &tracked_pool,
                        slot,
                        sync_tracker,
                        repair_tx,
                        observer,
                        route_health,
                    );
                    continue;
                }
                invalidate_and_repair(
                    &tracked_pool,
                    slot,
                    source_received_at,
                    source_latency,
                    executable_states,
                    sync_tracker,
                    event_tx,
                    repair_tx,
                    sequence,
                    observer,
                    route_health,
                    &mut invalidated_pools,
                );
            }
        }
    }
}

fn resolve_transaction(
    transaction: &LegacyVersionedTransaction,
    lookup_tables: &HashMap<String, LookupTableSnapshot>,
) -> Option<ResolvedTransaction> {
    let loaded_addresses = match &transaction.message {
        LegacyVersionedMessage::Legacy(message) => {
            let account_keys = AccountKeys::new(&message.account_keys, None);
            let mut resolved_account_keys = Vec::with_capacity(account_keys.len());
            let mut writable_account_keys = Vec::new();
            for (index, account_key) in account_keys.iter().enumerate() {
                let account_key = account_key.to_string();
                if transaction.message.is_maybe_writable(index, None) {
                    writable_account_keys.push(account_key.clone());
                }
                resolved_account_keys.push(account_key);
            }
            return Some(ResolvedTransaction {
                account_keys: resolved_account_keys,
                writable_account_keys,
                instructions: transaction
                    .message
                    .instructions()
                    .iter()
                    .map(|instruction| resolve_instruction(instruction, &account_keys))
                    .collect::<Option<Vec<_>>>()?,
            });
        }
        LegacyVersionedMessage::V0(message) => {
            resolve_loaded_addresses(&message.address_table_lookups, lookup_tables)?
        }
    };

    let LegacyVersionedMessage::V0(message) = &transaction.message else {
        return None;
    };
    let account_keys = AccountKeys::new(&message.account_keys, Some(&loaded_addresses));
    let mut resolved_account_keys = Vec::with_capacity(account_keys.len());
    let mut writable_account_keys = Vec::new();
    for (index, account_key) in account_keys.iter().enumerate() {
        let account_key = account_key.to_string();
        if transaction.message.is_maybe_writable(index, None) {
            writable_account_keys.push(account_key.clone());
        }
        resolved_account_keys.push(account_key);
    }
    Some(ResolvedTransaction {
        account_keys: resolved_account_keys,
        writable_account_keys,
        instructions: transaction
            .message
            .instructions()
            .iter()
            .map(|instruction| resolve_instruction(instruction, &account_keys))
            .collect::<Option<Vec<_>>>()?,
    })
}

fn resolve_instruction(
    instruction: &CompiledInstruction,
    account_keys: &AccountKeys<'_>,
) -> Option<ResolvedInstruction> {
    Some(ResolvedInstruction {
        program_id: account_keys
            .get(instruction.program_id_index as usize)?
            .to_string(),
        accounts: instruction
            .accounts
            .iter()
            .map(|index| account_keys.get(*index as usize).map(ToString::to_string))
            .collect::<Option<Vec<_>>>()?,
        data: instruction.data.clone(),
    })
}

fn apply_mutation_exact(
    slot: u64,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> Option<ExecutablePoolState> {
    let mut store = executable_states.write().ok()?;
    let pool_id = PoolId(tracked_pool.pool_id.clone());
    let next_state = reduce_mutation_exact(store.get(&pool_id)?, tracked_pool, mutation, slot)?;
    if !store.upsert(next_state.clone()) {
        return None;
    }
    Some(next_state)
}

fn apply_mutation_shadow(
    slot: u64,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> ReductionStatus {
    let Ok(mut store) = executable_states.write() else {
        return ReductionStatus::Invalid;
    };
    let pool_id = PoolId(tracked_pool.pool_id.clone());
    let Some(current_state) = store.get(&pool_id).cloned() else {
        return ReductionStatus::Invalid;
    };

    match reduce_mutation_shadow(&current_state, tracked_pool, mutation, slot) {
        ReductionStatus::Probable => {
            if let Some(state) = store.get_mut(&pool_id) {
                let next_write_version = state.write_version().saturating_add(1);
                state.set_confidence(PoolConfidence::Verified, slot, next_write_version, None);
            }
            ReductionStatus::Probable
        }
        other => other,
    }
}

fn reduce_mutation_exact(
    current_state: &ExecutablePoolState,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    slot: u64,
) -> Option<ExecutablePoolState> {
    match (&tracked_pool.kind, mutation, current_state) {
        (
            TrackedPoolKind::OrcaSimple(_),
            PoolMutation::OrcaSimpleSwap {
                source_vault,
                destination_vault,
                amount_in,
                ..
            },
            ExecutablePoolState::OrcaSimplePool(state),
        ) => reduce_orca_simple_swap(state, source_vault, destination_vault, *amount_in, slot),
        (
            TrackedPoolKind::RaydiumSimple(config),
            PoolMutation::RaydiumSimpleSwap {
                exact_out,
                user_owner,
                user_source_token_account,
                user_destination_token_account,
                amount_specified,
                ..
            },
            ExecutablePoolState::RaydiumSimplePool(state),
        ) => reduce_raydium_simple_swap(
            state,
            config,
            *exact_out,
            user_owner,
            user_source_token_account,
            user_destination_token_account,
            *amount_specified,
            slot,
        ),
        _ => None,
    }
}

fn reduce_mutation_shadow(
    current_state: &ExecutablePoolState,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    _slot: u64,
) -> ReductionStatus {
    match (&tracked_pool.kind, mutation, current_state) {
        (
            TrackedPoolKind::OrcaWhirlpool(_),
            PoolMutation::OrcaWhirlpoolSwap { .. },
            ExecutablePoolState::OrcaWhirlpool(state),
        ) if state.loaded_tick_arrays < state.expected_tick_arrays => ReductionStatus::Probable,
        (
            TrackedPoolKind::RaydiumClmm(_),
            PoolMutation::RaydiumClmmSwap { .. },
            ExecutablePoolState::RaydiumClmm(state),
        ) if state.loaded_tick_arrays < state.expected_tick_arrays => ReductionStatus::Probable,
        (TrackedPoolKind::RaydiumSimple(_), PoolMutation::RaydiumSimpleSwap { .. }, _) => {
            ReductionStatus::Probable
        }
        _ => ReductionStatus::Invalid,
    }
}

fn reduce_orca_simple_swap(
    state: &ConstantProductPoolState,
    source_vault: &str,
    destination_vault: &str,
    amount_in: u64,
    slot: u64,
) -> Option<ExecutablePoolState> {
    reduce_constant_product_swap(
        state,
        source_vault,
        destination_vault,
        amount_in,
        slot,
        PoolVenue::OrcaSimplePool,
    )
}

fn reduce_raydium_simple_swap(
    state: &ConstantProductPoolState,
    config: &RaydiumSimpleTrackedConfig,
    exact_out: bool,
    user_owner: &str,
    user_source_token_account: &str,
    user_destination_token_account: &str,
    amount_specified: u64,
    slot: u64,
) -> Option<ExecutablePoolState> {
    let token_program = parse_legacy_pubkey(&config.token_program_id)?;
    let owner = parse_legacy_pubkey(user_owner)?;
    let mint_a = parse_legacy_pubkey(&state.token_mint_a)?;
    let mint_b = parse_legacy_pubkey(&state.token_mint_b)?;
    let ata_a = associated_token_address(owner, mint_a, token_program).to_string();
    let ata_b = associated_token_address(owner, mint_b, token_program).to_string();

    let a_to_b = (user_source_token_account == ata_a || user_destination_token_account == ata_b)
        && user_source_token_account != ata_b
        && user_destination_token_account != ata_a;
    let b_to_a = (user_source_token_account == ata_b || user_destination_token_account == ata_a)
        && user_source_token_account != ata_a
        && user_destination_token_account != ata_b;

    let (source_vault, destination_vault) = match (a_to_b, b_to_a) {
        (true, false) => (&state.token_vault_a, &state.token_vault_b),
        (false, true) => (&state.token_vault_b, &state.token_vault_a),
        _ => return None,
    };

    if exact_out {
        reduce_constant_product_swap_exact_out(
            state,
            source_vault,
            destination_vault,
            amount_specified,
            slot,
            PoolVenue::RaydiumSimplePool,
        )
    } else {
        reduce_constant_product_swap(
            state,
            source_vault,
            destination_vault,
            amount_specified,
            slot,
            PoolVenue::RaydiumSimplePool,
        )
    }
}

fn reduce_constant_product_swap(
    state: &ConstantProductPoolState,
    source_vault: &str,
    destination_vault: &str,
    amount_in: u64,
    slot: u64,
    venue: PoolVenue,
) -> Option<ExecutablePoolState> {
    let (reserve_in, reserve_out, source_is_a) =
        if source_vault == state.token_vault_a && destination_vault == state.token_vault_b {
            (state.reserve_a, state.reserve_b, true)
        } else if source_vault == state.token_vault_b && destination_vault == state.token_vault_a {
            (state.reserve_b, state.reserve_a, false)
        } else {
            return None;
        };

    let amount_out =
        constant_product_amount_out(reserve_in, reserve_out, amount_in, state.fee_bps)?;
    if amount_out == 0 || amount_out >= reserve_out {
        return None;
    }

    let mut next = state.clone();
    if source_is_a {
        next.reserve_a = next.reserve_a.saturating_add(amount_in);
        next.reserve_b = next.reserve_b.saturating_sub(amount_out);
    } else {
        next.reserve_b = next.reserve_b.saturating_add(amount_in);
        next.reserve_a = next.reserve_a.saturating_sub(amount_out);
    }
    next.last_update_slot = slot;
    next.write_version = next.write_version.saturating_add(1);
    next.confidence = PoolConfidence::Executable;
    next.repair_pending = false;

    Some(match venue {
        PoolVenue::OrcaSimplePool => ExecutablePoolState::OrcaSimplePool(next),
        PoolVenue::RaydiumSimplePool => ExecutablePoolState::RaydiumSimplePool(next),
        PoolVenue::OrcaWhirlpool | PoolVenue::RaydiumClmm => return None,
    })
}

fn reduce_constant_product_swap_exact_out(
    state: &ConstantProductPoolState,
    source_vault: &str,
    destination_vault: &str,
    amount_out: u64,
    slot: u64,
    venue: PoolVenue,
) -> Option<ExecutablePoolState> {
    let (reserve_in, reserve_out, source_is_a) =
        if source_vault == state.token_vault_a && destination_vault == state.token_vault_b {
            (state.reserve_a, state.reserve_b, true)
        } else if source_vault == state.token_vault_b && destination_vault == state.token_vault_a {
            (state.reserve_b, state.reserve_a, false)
        } else {
            return None;
        };

    let amount_in =
        constant_product_amount_in_for_output(reserve_in, reserve_out, amount_out, state.fee_bps)?;

    let mut next = state.clone();
    if source_is_a {
        next.reserve_a = next.reserve_a.saturating_add(amount_in);
        next.reserve_b = next.reserve_b.saturating_sub(amount_out);
    } else {
        next.reserve_b = next.reserve_b.saturating_add(amount_in);
        next.reserve_a = next.reserve_a.saturating_sub(amount_out);
    }
    next.last_update_slot = slot;
    next.write_version = next.write_version.saturating_add(1);
    next.confidence = PoolConfidence::Executable;
    next.repair_pending = false;

    Some(match venue {
        PoolVenue::OrcaSimplePool => ExecutablePoolState::OrcaSimplePool(next),
        PoolVenue::RaydiumSimplePool => ExecutablePoolState::RaydiumSimplePool(next),
        PoolVenue::OrcaWhirlpool | PoolVenue::RaydiumClmm => return None,
    })
}

fn constant_product_amount_out(
    reserve_in: u64,
    reserve_out: u64,
    amount_in: u64,
    fee_bps: u16,
) -> Option<u64> {
    if reserve_in == 0 || reserve_out == 0 || amount_in == 0 {
        return None;
    }

    let fee_denominator = 10_000u128;
    let fee_multiplier = fee_denominator.saturating_sub(u128::from(fee_bps));
    let effective_in = u128::from(amount_in).saturating_mul(fee_multiplier) / fee_denominator;
    if effective_in == 0 {
        return None;
    }

    let numerator = effective_in.saturating_mul(u128::from(reserve_out));
    let denominator = u128::from(reserve_in).saturating_add(effective_in);
    let output = numerator / denominator;
    Some(output.min(u128::from(u64::MAX)) as u64)
}

fn constant_product_amount_in_for_output(
    reserve_in: u64,
    reserve_out: u64,
    amount_out: u64,
    fee_bps: u16,
) -> Option<u64> {
    if reserve_in == 0 || reserve_out == 0 || amount_out == 0 || amount_out >= reserve_out {
        return None;
    }

    let fee_denominator = 10_000u128;
    let fee_multiplier = fee_denominator.saturating_sub(u128::from(fee_bps));
    if fee_multiplier == 0 {
        return None;
    }

    let required_effective_in = div_ceil_u128(
        u128::from(amount_out).saturating_mul(u128::from(reserve_in)),
        u128::from(reserve_out.saturating_sub(amount_out)),
    )?;
    let amount_in = div_ceil_u128(
        required_effective_in.saturating_mul(fee_denominator),
        fee_multiplier,
    )?;
    let amount_in = u64::try_from(amount_in).ok()?;

    (constant_product_amount_out(reserve_in, reserve_out, amount_in, fee_bps)? >= amount_out)
        .then_some(amount_in)
}

fn div_ceil_u128(numerator: u128, denominator: u128) -> Option<u128> {
    if denominator == 0 {
        return None;
    }
    Some(numerator.saturating_add(denominator.saturating_sub(1)) / denominator)
}

fn parse_legacy_pubkey(value: &str) -> Option<LegacyPubkey> {
    LegacyPubkey::from_str(value).ok()
}

fn associated_token_address(
    owner: LegacyPubkey,
    mint: LegacyPubkey,
    token_program: LegacyPubkey,
) -> LegacyPubkey {
    LegacyPubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &parse_legacy_pubkey(ASSOCIATED_TOKEN_PROGRAM_ID)
            .expect("associated token program id should be valid"),
    )
    .0
}

fn invalidate_and_repair(
    tracked_pool: &TrackedPool,
    slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
    invalidated_pools: &mut HashSet<String>,
) {
    if !invalidated_pools.insert(tracked_pool.pool_id.clone()) {
        return;
    }
    if route_health
        .lock()
        .ok()
        .map(|health| health.pool_is_blocked_from_repair(&tracked_pool.pool_id, slot))
        .unwrap_or(false)
    {
        return;
    }
    let repair_requested = sync_tracker
        .lock()
        .ok()
        .map(|mut tracker| tracker.repair_requested(&tracked_pool.pool_id))
        .unwrap_or(false);
    if !repair_requested {
        return;
    }

    if clear_exact_refresh(sync_tracker, &tracked_pool.pool_id) {
        observer.publish_repair(RepairEvent {
            pool_id: tracked_pool.pool_id.clone(),
            kind: RepairEventKind::RefreshCleared,
            occurred_at: source_received_at,
        });
        if let Ok(mut health) = route_health.lock() {
            health.on_repair_transition(
                &tracked_pool.pool_id,
                PoolHealthTransition::RefreshCleared,
                slot,
            );
        }
    }

    if let Ok(mut store) = executable_states.write() {
        let pool_id = PoolId(tracked_pool.pool_id.clone());
        let next_write_version = store
            .get(&pool_id)
            .map(|state| state.write_version().saturating_add(1))
            .unwrap_or(1);
        let _ = store.invalidate(&pool_id, slot, next_write_version);
    }

    let event = event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        detection::MarketEvent::PoolInvalidation(PoolInvalidation {
            pool_id: tracked_pool.pool_id.clone(),
        }),
    );
    let _ = try_send_event(event_tx, event);
    let _ = repair_tx.send(RepairRequest {
        pool_id: tracked_pool.pool_id.clone(),
        mode: SyncRequestMode::RepairAfterInvalidation,
        priority: RepairPriority::Immediate,
        observed_slot: slot,
    });
    if let Ok(mut health) = route_health.lock() {
        health.on_repair_transition(
            &tracked_pool.pool_id,
            PoolHealthTransition::RepairQueued,
            slot,
        );
    }
}

fn schedule_exact_refresh(
    tracked_pool: &TrackedPool,
    slot: u64,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
) {
    let (deadline_slot, should_enqueue) = match sync_tracker.lock() {
        Ok(mut tracker) => tracker.schedule_refresh(&tracked_pool.pool_id, slot),
        Err(_) => return,
    };
    observer.publish_repair(RepairEvent {
        pool_id: tracked_pool.pool_id.clone(),
        kind: RepairEventKind::RefreshScheduled { deadline_slot },
        occurred_at: SystemTime::now(),
    });
    if let Ok(mut health) = route_health.lock() {
        health.on_repair_transition(
            &tracked_pool.pool_id,
            PoolHealthTransition::RefreshScheduled,
            slot,
        );
    }
    if should_enqueue {
        let _ = repair_tx.send(RepairRequest {
            pool_id: tracked_pool.pool_id.clone(),
            mode: SyncRequestMode::RefreshExact,
            priority: RepairPriority::Immediate,
            observed_slot: slot,
        });
    }
}

fn expire_exact_refreshes(
    slot: u64,
    registry: &TrackedPoolRegistry,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    invalidated_pools: &mut HashSet<String>,
) {
    let expired_pool_ids = match sync_tracker.lock() {
        Ok(mut tracker) => tracker.take_expired_refreshes(slot),
        Err(_) => return,
    };

    for pool_id in expired_pool_ids {
        observer.publish_repair(RepairEvent {
            pool_id: pool_id.clone(),
            kind: RepairEventKind::RefreshCleared,
            occurred_at: source_received_at,
        });
        if let Ok(mut health) = route_health.lock() {
            health.on_repair_transition(&pool_id, PoolHealthTransition::RefreshCleared, slot);
        }
        let Some(tracked_pool) = registry.tracked_pool(&pool_id) else {
            continue;
        };
        invalidate_and_repair(
            &tracked_pool,
            slot,
            source_received_at,
            source_latency,
            executable_states,
            sync_tracker,
            event_tx,
            repair_tx,
            sequence,
            observer,
            route_health,
            invalidated_pools,
        );
    }
}

fn clear_exact_refresh(sync_tracker: &Arc<Mutex<PoolSyncTracker>>, pool_id: &str) -> bool {
    sync_tracker
        .lock()
        .ok()
        .map(|mut tracker| tracker.clear_refresh(pool_id))
        .unwrap_or(false)
}

fn publish_slot_boundary(
    event_tx: &SyncSender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
    slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
) {
    let event = event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        detection::MarketEvent::SlotBoundary(detection::SlotBoundary { slot, leader: None }),
    );
    let _ = try_send_event(event_tx, event);
}

fn publish_pool_snapshot_update(
    event_tx: &SyncSender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
    slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    state: &ExecutablePoolState,
) {
    let snapshot = state.to_pool_snapshot(slot, 0);
    let event = event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        detection::MarketEvent::PoolSnapshotUpdate(PoolSnapshotUpdate {
            pool_id: state.pool_id().0.clone(),
            price_bps: snapshot.price_bps,
            fee_bps: snapshot.fee_bps,
            reserve_depth: snapshot.reserve_depth,
            reserve_a: snapshot.reserve_a,
            reserve_b: snapshot.reserve_b,
            active_liquidity: Some(snapshot.active_liquidity),
            sqrt_price_x64: snapshot.sqrt_price_x64,
            confidence: snapshot_confidence(snapshot.confidence),
            repair_pending: Some(snapshot.repair_pending),
            token_mint_a: snapshot.token_mint_a,
            token_mint_b: snapshot.token_mint_b,
            tick_spacing: snapshot.tick_spacing,
            current_tick_index: snapshot.current_tick_index,
            slot,
            write_version: state.write_version(),
        }),
    );
    let _ = try_send_event(event_tx, event);
}

fn resolve_loaded_addresses(
    lookups: &[v0::MessageAddressTableLookup],
    lookup_tables: &HashMap<String, LookupTableSnapshot>,
) -> Option<v0::LoadedAddresses> {
    if lookups.is_empty() {
        return Some(v0::LoadedAddresses::default());
    }

    let mut writable = Vec::new();
    let mut readonly = Vec::new();
    for lookup in lookups {
        let snapshot = lookup_tables.get(&lookup.account_key.to_string())?;
        for index in &lookup.writable_indexes {
            let address = snapshot.addresses.get(*index as usize)?;
            writable.push(LegacyPubkey::from_str(address).ok()?);
        }
        for index in &lookup.readonly_indexes {
            let address = snapshot.addresses.get(*index as usize)?;
            readonly.push(LegacyPubkey::from_str(address).ok()?);
        }
    }

    Some(v0::LoadedAddresses { writable, readonly })
}

fn spawn_lookup_table_refresher(
    config: &BotConfig,
    lookup_cache: Arc<RwLock<HashMap<String, LookupTableSnapshot>>>,
    lookup_table_keys: Vec<String>,
) {
    if !config.runtime.refresh.enabled
        || config.runtime.refresh.alt_refresh_millis == 0
        || lookup_table_keys.is_empty()
        || is_mock_endpoint(&config.reconciliation.rpc_http_endpoint)
    {
        return;
    }

    let endpoint = config.reconciliation.rpc_http_endpoint.clone();
    let interval = Duration::from_millis(config.runtime.refresh.alt_refresh_millis);
    thread::spawn(move || {
        loop {
            if let Some(tables) = fetch_lookup_tables(&endpoint, &lookup_table_keys) {
                let next = tables
                    .into_iter()
                    .map(|table| (table.account_key.clone(), table))
                    .collect::<HashMap<_, _>>();
                if let Ok(mut guard) = lookup_cache.write() {
                    *guard = next;
                }
            }
            thread::sleep(interval);
        }
    });
}

fn spawn_repair_worker(
    config: &BotConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    event_tx: SyncSender<NormalizedEvent>,
    sequence: Arc<AtomicU64>,
    observer: ObserverHandle,
    route_health: SharedRouteHealth,
) -> mpsc::Sender<RepairRequest> {
    let (repair_tx, repair_rx) = mpsc::channel::<RepairRequest>();
    let rpc_endpoint = config.reconciliation.rpc_http_endpoint.clone();
    let retry_tx = repair_tx.clone();
    let (job_tx, job_rx) = mpsc::channel::<RepairJob>();
    let (result_tx, result_rx) = mpsc::channel::<RepairJobResult>();
    let job_rx = Arc::new(Mutex::new(job_rx));

    for _ in 0..2 {
        let registry = Arc::clone(&registry);
        let executable_states = Arc::clone(&executable_states);
        let rpc_endpoint = rpc_endpoint.clone();
        let job_rx = Arc::clone(&job_rx);
        let result_tx = result_tx.clone();
        thread::spawn(move || {
            let http = Client::builder()
                .connect_timeout(Duration::from_millis(300))
                .timeout(Duration::from_millis(1_000))
                .build()
                .expect("build live repair HTTP client");

            loop {
                let job = {
                    let receiver = job_rx.lock().expect("repair job receiver");
                    receiver.recv()
                };
                let Ok(job) = job else {
                    break;
                };

                let Some(tracked_pool) = registry.tracked_pool(&job.pool_id) else {
                    let _ = result_tx.send(RepairJobResult::Dropped {
                        pool_id: job.pool_id,
                        mode: job.mode,
                        observed_slot: job.observed_slot,
                    });
                    continue;
                };

                let started_at = Instant::now();
                let account_keys = tracked_pool.repair_account_keys();
                let Some((slot, accounts)) =
                    fetch_accounts_by_key(&http, &rpc_endpoint, &account_keys)
                else {
                    let _ = result_tx.send(RepairJobResult::Retry {
                        pool_id: job.pool_id,
                        mode: job.mode,
                        attempt: job.attempt,
                        observed_slot: job.observed_slot,
                    });
                    continue;
                };

                let Some(next_state) = build_executable_state_from_accounts(
                    &tracked_pool,
                    &accounts,
                    slot,
                    &executable_states,
                ) else {
                    let _ = result_tx.send(RepairJobResult::Retry {
                        pool_id: job.pool_id,
                        mode: job.mode,
                        attempt: job.attempt,
                        observed_slot: job.observed_slot,
                    });
                    continue;
                };

                let _ = result_tx.send(RepairJobResult::Success {
                    pool_id: job.pool_id,
                    mode: job.mode,
                    slot,
                    next_state,
                    attempt: job.attempt,
                    latency: started_at.elapsed(),
                });
            }
        });
    }
    drop(result_tx);

    thread::spawn(move || {
        let mut pending = VecDeque::new();
        let mut pending_modes = HashMap::<String, RepairRequest>::new();
        let mut in_flight = HashMap::<String, SyncRequestMode>::new();
        let mut attempts = HashMap::<(String, SyncRequestMode), u32>::new();

        loop {
            drain_repair_results(
                &result_rx,
                &executable_states,
                &sync_tracker,
                &event_tx,
                &sequence,
                &observer,
                &route_health,
                &retry_tx,
                &mut in_flight,
                &mut attempts,
            );
            dispatch_repair_jobs(
                &job_tx,
                &executable_states,
                &sync_tracker,
                &observer,
                &route_health,
                &mut pending,
                &mut pending_modes,
                &mut in_flight,
                &mut attempts,
            );

            match repair_rx.recv_timeout(Duration::from_millis(25)) {
                Ok(request) => {
                    enqueue_repair_request(
                        request,
                        &executable_states,
                        &sync_tracker,
                        &route_health,
                        &mut pending,
                        &mut pending_modes,
                        &in_flight,
                    );
                    while let Ok(next) = repair_rx.try_recv() {
                        enqueue_repair_request(
                            next,
                            &executable_states,
                            &sync_tracker,
                            &route_health,
                            &mut pending,
                            &mut pending_modes,
                            &in_flight,
                        );
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    drain_repair_results(
                        &result_rx,
                        &executable_states,
                        &sync_tracker,
                        &event_tx,
                        &sequence,
                        &observer,
                        &route_health,
                        &retry_tx,
                        &mut in_flight,
                        &mut attempts,
                    );
                    if pending.is_empty() && in_flight.is_empty() {
                        break;
                    }
                }
            }
        }
    });

    repair_tx
}

fn enqueue_repair_request(
    request: RepairRequest,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    route_health: &SharedRouteHealth,
    pending: &mut VecDeque<String>,
    pending_modes: &mut HashMap<String, RepairRequest>,
    in_flight: &HashMap<String, SyncRequestMode>,
) {
    if !request_still_needed(&request, executable_states, sync_tracker, route_health) {
        if request.mode == SyncRequestMode::RepairAfterInvalidation {
            if let Ok(mut tracker) = sync_tracker.lock() {
                tracker.clear_repair(&request.pool_id);
            }
        }
        return;
    }
    if in_flight.contains_key(&request.pool_id) {
        return;
    }

    if let Some(existing) = pending_modes.get_mut(&request.pool_id) {
        if existing.mode == SyncRequestMode::RefreshExact
            && request.mode == SyncRequestMode::RepairAfterInvalidation
        {
            existing.mode = SyncRequestMode::RepairAfterInvalidation;
        }
        existing.observed_slot = existing.observed_slot.max(request.observed_slot);
        if request.priority == RepairPriority::Immediate {
            promote_pending_repair(&request.pool_id, pending);
        }
        return;
    }

    match request.priority {
        RepairPriority::Immediate => pending.push_front(request.pool_id.clone()),
        RepairPriority::Retry => pending.push_back(request.pool_id.clone()),
    }
    pending_modes.insert(request.pool_id.clone(), request);
}

fn promote_pending_repair(pool_id: &str, pending: &mut VecDeque<String>) {
    if let Some(index) = pending.iter().position(|entry| entry == pool_id) {
        if let Some(existing) = pending.remove(index) {
            pending.push_front(existing);
        }
    }
}

fn dispatch_repair_jobs(
    job_tx: &mpsc::Sender<RepairJob>,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
    pending: &mut VecDeque<String>,
    pending_modes: &mut HashMap<String, RepairRequest>,
    in_flight: &mut HashMap<String, SyncRequestMode>,
    attempts: &mut HashMap<(String, SyncRequestMode), u32>,
) {
    while in_flight.len() < 2 {
        let Some(pool_id) = pending.pop_front() else {
            break;
        };
        let Some(request) = pending_modes.remove(&pool_id) else {
            continue;
        };
        if !request_still_needed(&request, executable_states, sync_tracker, route_health) {
            if request.mode == SyncRequestMode::RepairAfterInvalidation {
                if let Ok(mut tracker) = sync_tracker.lock() {
                    tracker.clear_repair(&pool_id);
                }
            }
            attempts.remove(&(pool_id.clone(), request.mode));
            continue;
        }

        let attempt = attempts
            .get(&(pool_id.clone(), request.mode))
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        attempts.insert((pool_id.clone(), request.mode), attempt);
        in_flight.insert(pool_id.clone(), request.mode);
        observer.publish_repair(RepairEvent {
            pool_id: pool_id.clone(),
            kind: match request.mode {
                SyncRequestMode::RefreshExact => RepairEventKind::RefreshAttemptStarted,
                SyncRequestMode::RepairAfterInvalidation => RepairEventKind::RepairAttemptStarted,
            },
            occurred_at: SystemTime::now(),
        });
        if let Ok(mut health) = route_health.lock() {
            health.on_repair_transition(
                &pool_id,
                match request.mode {
                    SyncRequestMode::RefreshExact => PoolHealthTransition::RefreshStarted,
                    SyncRequestMode::RepairAfterInvalidation => PoolHealthTransition::RepairStarted,
                },
                request.observed_slot,
            );
        }
        if request.mode == SyncRequestMode::RefreshExact {
            if let Ok(mut tracker) = sync_tracker.lock() {
                tracker.mark_refresh_started(&pool_id);
            }
        } else if let Ok(mut tracker) = sync_tracker.lock() {
            tracker.mark_repair_started(&pool_id);
        }

        if job_tx
            .send(RepairJob {
                pool_id,
                attempt,
                mode: request.mode,
                observed_slot: request.observed_slot,
            })
            .is_err()
        {
            break;
        }
    }
}

fn drain_repair_results(
    result_rx: &mpsc::Receiver<RepairJobResult>,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &SyncSender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
    observer: &ObserverHandle,
    route_health: &SharedRouteHealth,
    retry_tx: &mpsc::Sender<RepairRequest>,
    in_flight: &mut HashMap<String, SyncRequestMode>,
    attempts: &mut HashMap<(String, SyncRequestMode), u32>,
) {
    while let Ok(result) = result_rx.try_recv() {
        match result {
            RepairJobResult::Success {
                pool_id,
                mode,
                slot,
                next_state,
                attempt: _attempt,
                latency,
            } => {
                in_flight.remove(&pool_id);
                let mut published = false;
                if let Ok(mut store) = executable_states.write() {
                    published = store.upsert(next_state.clone());
                }
                if published {
                    publish_pool_snapshot_update(
                        event_tx,
                        sequence,
                        slot,
                        SystemTime::now(),
                        None,
                        &next_state,
                    );
                }
                if mode == SyncRequestMode::RefreshExact {
                    clear_exact_refresh(sync_tracker, &pool_id);
                } else if let Ok(mut tracker) = sync_tracker.lock() {
                    tracker.clear_repair(&pool_id);
                }
                attempts.remove(&(pool_id.clone(), mode));
                observer.publish_repair(RepairEvent {
                    pool_id,
                    kind: match mode {
                        SyncRequestMode::RefreshExact => RepairEventKind::RefreshAttemptSucceeded {
                            latency_ms: latency.as_millis().min(u128::from(u64::MAX)) as u64,
                        },
                        SyncRequestMode::RepairAfterInvalidation => {
                            RepairEventKind::RepairAttemptSucceeded {
                                latency_ms: latency.as_millis().min(u128::from(u64::MAX)) as u64,
                            }
                        }
                    },
                    occurred_at: SystemTime::now(),
                });
                if let Ok(mut health) = route_health.lock() {
                    health.on_repair_transition(
                        next_state.pool_id().0.as_str(),
                        match mode {
                            SyncRequestMode::RefreshExact => PoolHealthTransition::RefreshSucceeded,
                            SyncRequestMode::RepairAfterInvalidation => {
                                PoolHealthTransition::RepairSucceeded
                            }
                        },
                        slot,
                    );
                }
            }
            RepairJobResult::Retry {
                pool_id,
                mode,
                attempt,
                observed_slot,
            } => {
                in_flight.remove(&pool_id);
                if mode == SyncRequestMode::RefreshExact {
                    if let Ok(mut tracker) = sync_tracker.lock() {
                        tracker.mark_refresh_failed(&pool_id);
                    }
                } else if let Ok(mut tracker) = sync_tracker.lock() {
                    tracker.mark_repair_failed(&pool_id);
                }
                observer.publish_repair(RepairEvent {
                    pool_id: pool_id.clone(),
                    kind: match mode {
                        SyncRequestMode::RefreshExact => RepairEventKind::RefreshAttemptFailed,
                        SyncRequestMode::RepairAfterInvalidation => {
                            RepairEventKind::RepairAttemptFailed
                        }
                    },
                    occurred_at: SystemTime::now(),
                });
                if let Ok(mut health) = route_health.lock() {
                    health.on_repair_transition(
                        &pool_id,
                        match mode {
                            SyncRequestMode::RefreshExact => PoolHealthTransition::RefreshFailed,
                            SyncRequestMode::RepairAfterInvalidation => {
                                PoolHealthTransition::RepairFailed
                            }
                        },
                        observed_slot,
                    );
                }
                let retry_tx = retry_tx.clone();
                thread::spawn(move || {
                    thread::sleep(repair_backoff(attempt));
                    let _ = retry_tx.send(RepairRequest {
                        pool_id,
                        mode,
                        priority: RepairPriority::Retry,
                        observed_slot,
                    });
                });
            }
            RepairJobResult::Dropped {
                pool_id,
                mode,
                observed_slot,
            } => {
                in_flight.remove(&pool_id);
                if mode == SyncRequestMode::RefreshExact {
                    clear_exact_refresh(sync_tracker, &pool_id);
                } else if let Ok(mut tracker) = sync_tracker.lock() {
                    tracker.clear_repair(&pool_id);
                }
                attempts.remove(&(pool_id.clone(), mode));
                if let Ok(mut health) = route_health.lock() {
                    health.on_repair_transition(
                        &pool_id,
                        match mode {
                            SyncRequestMode::RefreshExact => PoolHealthTransition::RefreshCleared,
                            SyncRequestMode::RepairAfterInvalidation => {
                                PoolHealthTransition::RepairFailed
                            }
                        },
                        observed_slot,
                    );
                }
            }
        }
    }
}

fn request_still_needed(
    request: &RepairRequest,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    route_health: &SharedRouteHealth,
) -> bool {
    if route_health
        .lock()
        .ok()
        .map(|health| health.pool_is_blocked_from_repair(&request.pool_id, request.observed_slot))
        .unwrap_or(false)
    {
        return false;
    }
    match request.mode {
        SyncRequestMode::RefreshExact => sync_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.refresh_required(&request.pool_id))
            .unwrap_or(false),
        SyncRequestMode::RepairAfterInvalidation => {
            sync_tracker
                .lock()
                .ok()
                .map(|tracker| tracker.repair_required(&request.pool_id))
                .unwrap_or(false)
                && pool_requires_repair(&request.pool_id, executable_states)
        }
    }
}

fn repair_backoff(attempt: u32) -> Duration {
    match attempt {
        0 | 1 => Duration::from_millis(100),
        2 => Duration::from_millis(250),
        3 => Duration::from_millis(500),
        4 => Duration::from_millis(1_000),
        _ => Duration::from_millis(2_000),
    }
}

fn pool_requires_repair(
    pool_id: &str,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> bool {
    let Ok(store) = executable_states.read() else {
        return true;
    };
    let Some(state) = store.get(&PoolId(pool_id.into())) else {
        return true;
    };

    match state {
        ExecutablePoolState::OrcaSimplePool(state)
        | ExecutablePoolState::RaydiumSimplePool(state) => {
            !state.confidence.is_executable() || state.repair_pending
        }
        ExecutablePoolState::OrcaWhirlpool(state) | ExecutablePoolState::RaydiumClmm(state) => {
            !state.confidence.is_executable() || state.repair_pending
        }
    }
}

fn build_executable_state_from_accounts(
    tracked_pool: &TrackedPool,
    accounts: &HashMap<String, RpcAccountValue>,
    slot: u64,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> Option<ExecutablePoolState> {
    let next_write_version = executable_states
        .read()
        .ok()
        .and_then(|store| {
            store
                .get(&PoolId(tracked_pool.pool_id.clone()))
                .map(|state| state.write_version().saturating_add(1))
        })
        .unwrap_or(1);

    match &tracked_pool.kind {
        TrackedPoolKind::OrcaSimple(config) => {
            let reserve_a = token_account_amount(accounts.get(&config.token_vault_a)?)?;
            let reserve_b = token_account_amount(accounts.get(&config.token_vault_b)?)?;
            Some(ExecutablePoolState::OrcaSimplePool(
                ConstantProductPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::OrcaSimplePool,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    reserve_a,
                    reserve_b,
                    fee_bps: config.fee_bps,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: PoolConfidence::Executable,
                    repair_pending: false,
                },
            ))
        }
        TrackedPoolKind::RaydiumSimple(config) => {
            let reserve_a = token_account_amount(accounts.get(&config.token_vault_a)?)?;
            let reserve_b = token_account_amount(accounts.get(&config.token_vault_b)?)?;
            Some(ExecutablePoolState::RaydiumSimplePool(
                ConstantProductPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::RaydiumSimplePool,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    reserve_a,
                    reserve_b,
                    fee_bps: config.fee_bps,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: PoolConfidence::Executable,
                    repair_pending: false,
                },
            ))
        }
        TrackedPoolKind::OrcaWhirlpool(config) => {
            let account = accounts.get(&config.whirlpool)?;
            let raw = decode_base64_account(account)?;
            if !is_anchor_account_value(
                account,
                &raw,
                &config.program_id,
                ORCA_WHIRLPOOL_ACCOUNT_NAME,
                ORCA_WHIRLPOOL_ACCOUNT_MIN_LEN,
            ) {
                return None;
            }
            let liquidity = read_u128(&raw, 49)?;
            let sqrt_price_x64 = read_u128(&raw, 65)?;
            let current_tick_index = read_i32(&raw, 81)?;
            let fee_bps = read_u16(&raw, 45)
                .map(|value| value / 100)
                .unwrap_or(config.fee_bps);
            Some(ExecutablePoolState::OrcaWhirlpool(
                ConcentratedLiquidityPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::OrcaWhirlpool,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    fee_bps,
                    active_liquidity: liquidity.min(u128::from(u64::MAX)) as u64,
                    liquidity,
                    sqrt_price_x64,
                    current_tick_index,
                    tick_spacing: config.tick_spacing,
                    loaded_tick_arrays: 0,
                    expected_tick_arrays: 3,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: PoolConfidence::Verified,
                    repair_pending: false,
                },
            ))
        }
        TrackedPoolKind::RaydiumClmm(config) => {
            let account = accounts.get(&config.pool_state)?;
            let raw = decode_base64_account(account)?;
            if !is_anchor_account_value(
                account,
                &raw,
                &config.program_id,
                RAYDIUM_CLMM_ACCOUNT_NAME,
                RAYDIUM_CLMM_ACCOUNT_MIN_LEN,
            ) {
                return None;
            }
            let liquidity = read_u128(&raw, 237)?;
            let sqrt_price_x64 = read_u128(&raw, 253)?;
            let current_tick_index = read_i32(&raw, 269)?;
            Some(ExecutablePoolState::RaydiumClmm(
                ConcentratedLiquidityPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::RaydiumClmm,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    fee_bps: config.fee_bps,
                    active_liquidity: liquidity.min(u128::from(u64::MAX)) as u64,
                    liquidity,
                    sqrt_price_x64,
                    current_tick_index,
                    tick_spacing: config.tick_spacing,
                    loaded_tick_arrays: 0,
                    expected_tick_arrays: 3,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: PoolConfidence::Verified,
                    repair_pending: false,
                },
            ))
        }
    }
}

fn fetch_lookup_tables(
    endpoint: &str,
    lookup_table_keys: &[String],
) -> Option<Vec<LookupTableSnapshot>> {
    let http = Client::builder()
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_millis(1_000))
        .build()
        .ok()?;

    let response = http
        .post(endpoint)
        .json(&json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [
                lookup_table_keys,
                {
                    "commitment": "processed",
                    "encoding": "base64"
                }
            ]
        }))
        .send()
        .ok()?
        .json::<MultipleAccountsEnvelope>()
        .ok()?;

    let slot = response.result.context.slot;
    let tables = lookup_table_keys
        .iter()
        .zip(response.result.value)
        .filter_map(|(account_key, account)| decode_lookup_table(account_key, account, slot))
        .collect::<Vec<_>>();
    Some(tables)
}

fn fetch_accounts_by_key(
    http: &Client,
    endpoint: &str,
    account_keys: &[String],
) -> Option<(u64, HashMap<String, RpcAccountValue>)> {
    if is_mock_endpoint(endpoint) || account_keys.is_empty() {
        return None;
    }

    let response = http
        .post(endpoint)
        .json(&json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [
                account_keys,
                {
                    "commitment": "processed",
                    "encoding": "base64"
                }
            ]
        }))
        .send()
        .ok()?
        .json::<MultipleAccountsEnvelope>()
        .ok()?;

    let slot = response.result.context.slot;
    let accounts = account_keys
        .iter()
        .cloned()
        .zip(response.result.value)
        .filter_map(|(key, account)| account.map(|account| (key, account)))
        .collect::<HashMap<_, _>>();
    Some((slot, accounts))
}

fn decode_lookup_table(
    account_key: &str,
    account: Option<RpcAccountValue>,
    fetched_slot: u64,
) -> Option<LookupTableSnapshot> {
    let account = account?;
    if account.data.1 != "base64" {
        return None;
    }

    let raw = BASE64_STANDARD.decode(account.data.0.as_bytes()).ok()?;
    let table =
        solana_address_lookup_table_interface::state::AddressLookupTable::deserialize(&raw).ok()?;
    Some(LookupTableSnapshot {
        account_key: account_key.to_string(),
        addresses: table.addresses.iter().map(ToString::to_string).collect(),
        last_extended_slot: table.meta.last_extended_slot,
        fetched_slot,
    })
}

fn decode_base64_account(account: &RpcAccountValue) -> Option<Vec<u8>> {
    if account.data.1 != "base64" {
        return None;
    }
    BASE64_STANDARD.decode(account.data.0.as_bytes()).ok()
}

fn is_anchor_account_value(
    account: &RpcAccountValue,
    raw: &[u8],
    expected_owner: &str,
    account_name: &str,
    min_len: usize,
) -> bool {
    account.owner == expected_owner
        && raw.len() >= min_len
        && raw.starts_with(&anchor_account_discriminator(account_name))
}

fn token_account_amount(account: &RpcAccountValue) -> Option<u64> {
    let raw = decode_base64_account(account)?;
    read_u64(&raw, 64)
}

fn event_with_timing(
    sequence: u64,
    observed_slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    payload: detection::MarketEvent,
) -> NormalizedEvent {
    NormalizedEvent {
        payload,
        source: detection::SourceMetadata {
            source: EventSourceKind::ShredStream,
            sequence,
            observed_slot,
        },
        latency: detection::LatencyMetadata {
            source_received_at,
            normalized_at: SystemTime::now(),
            source_latency,
        },
    }
}

fn timestamp_to_system_time(timestamp: &ProstTimestamp) -> Option<SystemTime> {
    let seconds = u64::try_from(timestamp.seconds).ok()?;
    let nanos = u32::try_from(timestamp.nanos).ok()?;
    if nanos >= 1_000_000_000 {
        return None;
    }

    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))?
        .checked_add(Duration::from_nanos(u64::from(nanos)))
}

fn instruction_has_anchor_discriminator(instruction: &ResolvedInstruction, name: &str) -> bool {
    instruction
        .data
        .starts_with(&anchor_instruction_discriminator(name))
}

fn snapshot_confidence(confidence: PoolConfidence) -> SnapshotConfidence {
    match confidence {
        PoolConfidence::Decoded => SnapshotConfidence::Decoded,
        PoolConfidence::Verified => SnapshotConfidence::Verified,
        PoolConfidence::Executable => SnapshotConfidence::Executable,
        PoolConfidence::Invalid => SnapshotConfidence::Decoded,
    }
}

fn anchor_instruction_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("global:{name}");
    let hash = hashv(&[preimage.as_bytes()]);
    hash.to_bytes()[..8]
        .try_into()
        .expect("anchor discriminator should be 8 bytes")
}

fn anchor_account_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("account:{name}");
    let hash = hashv(&[preimage.as_bytes()]);
    hash.to_bytes()[..8]
        .try_into()
        .expect("anchor discriminator should be 8 bytes")
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    let bytes = data.get(offset..offset + 8)?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    Some(u64::from_le_bytes(buf))
}

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    let bytes = data.get(offset..offset + 2)?;
    let mut buf = [0u8; 2];
    buf.copy_from_slice(bytes);
    Some(u16::from_le_bytes(buf))
}

fn read_u128(data: &[u8], offset: usize) -> Option<u128> {
    let bytes = data.get(offset..offset + 16)?;
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Some(u128::from_le_bytes(buf))
}

fn read_i32(data: &[u8], offset: usize) -> Option<i32> {
    let bytes = data.get(offset..offset + 4)?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    Some(i32::from_le_bytes(buf))
}

fn try_send_event(
    event_tx: &SyncSender<NormalizedEvent>,
    event: NormalizedEvent,
) -> Result<(), TrySendError<NormalizedEvent>> {
    event_tx.try_send(event)
}

fn next_sequence(sequence: &AtomicU64) -> u64 {
    sequence.fetch_add(1, Ordering::Relaxed)
}

fn is_mock_endpoint(endpoint: &str) -> bool {
    endpoint.is_empty() || endpoint.starts_with(MOCK_SCHEME)
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsEnvelope {
    result: MultipleAccountsResult,
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsResult {
    context: RpcContext,
    value: Vec<Option<RpcAccountValue>>,
}

#[derive(Debug, Deserialize)]
struct RpcContext {
    slot: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct RpcAccountValue {
    data: (String, String),
    owner: String,
    lamports: u64,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        path::PathBuf,
        sync::{Arc, RwLock},
        time::{Duration, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use prost_types::Timestamp;
    use state::{
        executable_pool_state::ExecutablePoolStateStore,
        types::{ConstantProductPoolState, PoolConfidence, PoolId, PoolVenue},
    };

    use super::{
        CompiledInstruction, LegacyVersionedMessage, LegacyVersionedTransaction,
        ORCA_SWAP_INSTRUCTION_TAG, ORCA_SWAP_V2_DISCRIMINATOR_NAME,
        ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME, ORCA_WHIRLPOOL_ACCOUNT_NAME,
        OrcaSimpleTrackedConfig, OrcaWhirlpoolTrackedConfig, PoolMutation, PoolSyncTracker,
        RAYDIUM_CLMM_ACCOUNT_NAME, RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME, RAYDIUM_SWAP_BASE_OUT_TAG,
        RaydiumClmmTrackedConfig, RaydiumSimpleTrackedConfig, ResolvedInstruction, RpcAccountValue,
        TrackedPool, TrackedPoolKind, TrackedPoolRegistry, anchor_account_discriminator,
        anchor_instruction_discriminator, associated_token_address,
        build_executable_state_from_accounts, constant_product_amount_in_for_output,
        constant_product_amount_out, parse_legacy_pubkey, reduce_orca_simple_swap,
        reduce_raydium_simple_swap, resolve_transaction, timestamp_to_system_time,
    };
    use crate::config::{BotConfig, ReducerRolloutMode, RouteLegExecutionConfig};

    fn write_u16(data: &mut [u8], offset: usize, value: u16) {
        data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u64(data: &mut [u8], offset: usize, value: u64) {
        data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u128(data: &mut [u8], offset: usize, value: u128) {
        data[offset..offset + 16].copy_from_slice(&value.to_le_bytes());
    }

    fn write_i32(data: &mut [u8], offset: usize, value: i32) {
        data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn rpc_account_with_owner(raw: Vec<u8>, owner: &str) -> RpcAccountValue {
        RpcAccountValue {
            data: (BASE64_STANDARD.encode(raw), "base64".into()),
            owner: owner.into(),
            lamports: 0,
        }
    }

    fn repo_root_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(file)
    }

    #[test]
    fn registry_tracks_unique_pools_from_routes() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes.toml")).unwrap();
        let registry = TrackedPoolRegistry::from_config(&config);
        assert!(registry.pool_ids().len() >= 2);
    }

    #[test]
    fn amm_fast_manifest_tracks_curated_amm_routes() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let enabled_routes = config
            .routes
            .definitions
            .iter()
            .filter(|route| route.enabled)
            .collect::<Vec<_>>();
        let route_ids = enabled_routes
            .iter()
            .map(|route| route.route_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            route_ids,
            vec![
                "sol-usdc-ray-61ac-ray-5oav",
                "sol-usdc-ray-5oav-ray-61ac",
                "jto-sol-ray-jvop-orca-2uhf",
                "jto-sol-orca-2uhf-ray-jvop",
                "trump-sol-orca-ckp1-ray-gqsp",
                "trump-sol-ray-gqsp-orca-ckp1",
                "trump-usdc-orca-6nd6-ray-7xzv",
                "trump-usdc-ray-7xzv-orca-6nd6",
            ]
        );

        let groups = enabled_routes
            .iter()
            .map(|route| {
                (
                    route.base_mint.as_deref().unwrap_or(""),
                    route.quote_mint.as_deref().unwrap_or(""),
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            groups,
            BTreeSet::from([
                (
                    "So11111111111111111111111111111111111111112",
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                ),
                (
                    "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
                    "So11111111111111111111111111111111111111112",
                ),
                (
                    "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
                    "So11111111111111111111111111111111111111112",
                ),
                (
                    "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                ),
            ])
        );

        assert!(enabled_routes.iter().all(|route| {
            route.base_mint.as_deref() != Some("FYa25XnBsXQXAdTnsyKBKd5gZ1VZhChBRF57CqfRxJZX")
                && route.input_mint != "FYa25XnBsXQXAdTnsyKBKd5gZ1VZhChBRF57CqfRxJZX"
                && route.output_mint != "FYa25XnBsXQXAdTnsyKBKd5gZ1VZhChBRF57CqfRxJZX"
        }));

        let execution_kinds = enabled_routes
            .iter()
            .flat_map(|route| route.legs.iter())
            .map(|leg| match &leg.execution {
                RouteLegExecutionConfig::RaydiumSimplePool(_) => "raydium_simple_pool",
                RouteLegExecutionConfig::RaydiumClmm(_) => "raydium_clmm",
                RouteLegExecutionConfig::OrcaWhirlpool(_) => "orca_whirlpool",
                RouteLegExecutionConfig::OrcaSimplePool(_) => "orca_simple_pool",
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            execution_kinds,
            BTreeSet::from(["orca_whirlpool", "raydium_clmm", "raydium_simple_pool"])
        );

        let registry = TrackedPoolRegistry::from_config(&config);
        let pool_ids = registry.pool_ids().into_iter().collect::<BTreeSet<_>>();
        assert_eq!(
            pool_ids,
            BTreeSet::from([
                "2UhFnySoJi6c89aydGAGS7ZRemo2dbkFRhvSJqDX4gHJ".to_string(),
                "5oAvct85WyF7Sj73VYHbyFJkdRJ28D8m4z4Sxjvzuc6n".to_string(),
                "6nD6d8gG17wakW6Wu5URktBZQp3uxp5orgPa576QXigJ".to_string(),
                "61acRgpURKTU8LKPJKs6WQa18KzD9ogavXzjxfD84KLu".to_string(),
                "7XzVsjqTebULfkUofTDH5gDdZDmxacPmPuTfHa1n9kuh".to_string(),
                "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".to_string(),
                "GQsPr4RJk9AZkkfWHud7v4MtotcxhaYzZHdsPCg9vNvW".to_string(),
                "JVoPtWWDsRcLvQosu5fWc2CaNF6jEtJzbxdPtcEuvZo".to_string(),
            ])
        );
        assert!(
            registry
                .pools_by_id
                .values()
                .all(|tracked| tracked.reducer_mode == ReducerRolloutMode::Active)
        );
        assert_eq!(
            registry.lookup_table_keys,
            vec![
                "2abC3nxk29LkoeyEwxziaKoMyHFxdzQCycUSXTB2xMCu".to_string(),
                "8rG5WFRriQYz3SiYSjB2V7TVCqJiTfur23foeRkWLD67".to_string(),
                "BxGZYXxJYQ2uWegbHmuQWXvW5PCgmoU2X9AeF4yfLqrk".to_string(),
                "CaAB3ULQ5iYw6MQZqrzca6oY4J8UNznod2EkS8s1bfuX".to_string(),
                "G66u95YwxUKLibnT4ZenvPhMXWVqupLX6SNLBDdzH7o8".to_string(),
            ]
        );
    }

    #[test]
    fn orca_simple_reducer_updates_reserves_exactly() {
        let state = ConstantProductPoolState {
            pool_id: PoolId("pool-a".into()),
            venue: PoolVenue::OrcaSimplePool,
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 1_000_000,
            reserve_b: 2_000_000,
            fee_bps: 30,
            last_update_slot: 10,
            write_version: 2,
            last_verified_slot: 10,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
        };

        let next = reduce_orca_simple_swap(&state, "vault-a", "vault-b", 100_000, 11).unwrap();
        let state::types::ExecutablePoolState::OrcaSimplePool(next) = next else {
            panic!("expected orca simple state");
        };
        assert_eq!(next.reserve_a, 1_100_000);
        assert!(next.reserve_b < 2_000_000);
        assert_eq!(next.write_version, 3);
    }

    #[test]
    fn raydium_simple_reducer_updates_reserves_when_direction_is_inferred() {
        let state = ConstantProductPoolState {
            pool_id: PoolId("pool-ray".into()),
            venue: PoolVenue::RaydiumSimplePool,
            token_mint_a: "So11111111111111111111111111111111111111112".into(),
            token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 1_000_000_000,
            reserve_b: 100_000_000,
            fee_bps: 25,
            last_update_slot: 10,
            write_version: 2,
            last_verified_slot: 10,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
        };
        let owner = parse_legacy_pubkey("11111111111111111111111111111112").unwrap();
        let token_program =
            parse_legacy_pubkey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let usdc_mint = parse_legacy_pubkey(&state.token_mint_b).unwrap();
        let usdc_ata = associated_token_address(owner, usdc_mint, token_program).to_string();

        let next = reduce_raydium_simple_swap(
            &state,
            &RaydiumSimpleTrackedConfig {
                program_id: "raydium-program".into(),
                token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                amm_pool: "amm".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                token_mint_a: state.token_mint_a.clone(),
                token_mint_b: state.token_mint_b.clone(),
                fee_bps: 25,
            },
            false,
            &owner.to_string(),
            "temporary-wsol-account",
            &usdc_ata,
            100_000_000,
            11,
        )
        .unwrap();

        let state::types::ExecutablePoolState::RaydiumSimplePool(next) = next else {
            panic!("expected raydium simple state");
        };
        assert_eq!(next.reserve_a, 1_100_000_000);
        assert!(next.reserve_b < 100_000_000);
        assert_eq!(next.write_version, 3);
    }

    #[test]
    fn raydium_simple_reducer_supports_exact_out_swaps() {
        let state = ConstantProductPoolState {
            pool_id: PoolId("pool-ray".into()),
            venue: PoolVenue::RaydiumSimplePool,
            token_mint_a: "So11111111111111111111111111111111111111112".into(),
            token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 1_000_000_000,
            reserve_b: 100_000_000,
            fee_bps: 25,
            last_update_slot: 10,
            write_version: 2,
            last_verified_slot: 10,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
        };
        let owner = parse_legacy_pubkey("11111111111111111111111111111112").unwrap();
        let token_program =
            parse_legacy_pubkey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let usdc_mint = parse_legacy_pubkey(&state.token_mint_b).unwrap();
        let usdc_ata = associated_token_address(owner, usdc_mint, token_program).to_string();

        let next = reduce_raydium_simple_swap(
            &state,
            &RaydiumSimpleTrackedConfig {
                program_id: "raydium-program".into(),
                token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                amm_pool: "amm".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                token_mint_a: state.token_mint_a.clone(),
                token_mint_b: state.token_mint_b.clone(),
                fee_bps: 25,
            },
            true,
            &owner.to_string(),
            "temporary-wsol-account",
            &usdc_ata,
            9_000_000,
            11,
        )
        .unwrap();

        let state::types::ExecutablePoolState::RaydiumSimplePool(next) = next else {
            panic!("expected raydium simple state");
        };
        assert_eq!(next.reserve_b, 91_000_000);
        assert!(next.reserve_a > 1_000_000_000);
        assert_eq!(next.write_version, 3);
    }

    #[test]
    fn cpmm_output_accounts_for_fee() {
        let output = constant_product_amount_out(1_000_000, 2_000_000, 100_000, 30).unwrap();
        assert!(output > 0);
        assert!(output < 200_000);
    }

    #[test]
    fn cpmm_input_inverse_matches_requested_output() {
        let amount_in =
            constant_product_amount_in_for_output(1_000_000, 2_000_000, 150_000, 25).unwrap();
        let output = constant_product_amount_out(1_000_000, 2_000_000, amount_in, 25).unwrap();
        let previous =
            constant_product_amount_out(1_000_000, 2_000_000, amount_in - 1, 25).unwrap();

        assert!(output >= 150_000);
        assert!(previous < 150_000);
    }

    #[test]
    fn decodes_raydium_simple_swap_base_out_instruction() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec!["amm".into(), "vault-a".into(), "vault-b".into()],
            kind: TrackedPoolKind::RaydiumSimple(RaydiumSimpleTrackedConfig {
                program_id: "raydium-program".into(),
                token_program_id: "token-program".into(),
                amm_pool: "amm".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                fee_bps: 25,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_pool(tracked_pool);

        let mut data = Vec::new();
        data.push(RAYDIUM_SWAP_BASE_OUT_TAG);
        data.extend_from_slice(&123u64.to_le_bytes());
        data.extend_from_slice(&45u64.to_le_bytes());

        let decoded = registry.decode_instruction(&ResolvedInstruction {
            program_id: "raydium-program".into(),
            accounts: vec![
                "token-program".into(),
                "amm".into(),
                "authority".into(),
                "open-orders".into(),
                "vault-a".into(),
                "vault-b".into(),
                "market-program".into(),
                "market".into(),
                "bids".into(),
                "asks".into(),
                "queue".into(),
                "market-coin".into(),
                "market-pc".into(),
                "signer".into(),
                "user-source".into(),
                "user-destination".into(),
                "owner".into(),
            ],
            data,
        });

        assert_eq!(
            decoded,
            Some(PoolMutation::RaydiumSimpleSwap {
                pool_id: "pool-ray".into(),
                exact_out: true,
                user_owner: "owner".into(),
                user_source_token_account: "user-source".into(),
                user_destination_token_account: "user-destination".into(),
                amount_specified: 123,
                min_output_amount: 45,
            })
        );
    }

    #[test]
    fn decodes_orca_simple_swap_instruction() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-a".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec!["swap".into(), "vault-a".into(), "vault-b".into()],
            kind: TrackedPoolKind::OrcaSimple(OrcaSimpleTrackedConfig {
                program_id: "orca-program".into(),
                swap_account: "swap".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                fee_bps: 30,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_pool(tracked_pool);

        let mut data = Vec::new();
        data.push(ORCA_SWAP_INSTRUCTION_TAG);
        data.extend_from_slice(&100u64.to_le_bytes());
        data.extend_from_slice(&80u64.to_le_bytes());
        let decoded = registry.decode_instruction(&ResolvedInstruction {
            program_id: "orca-program".into(),
            accounts: vec![
                "swap".into(),
                "authority".into(),
                "payer".into(),
                "user-in".into(),
                "vault-a".into(),
                "vault-b".into(),
            ],
            data,
        });

        assert_eq!(
            decoded,
            Some(PoolMutation::OrcaSimpleSwap {
                pool_id: "pool-a".into(),
                source_vault: "vault-a".into(),
                destination_vault: "vault-b".into(),
                amount_in: 100,
                min_output_amount: 80,
            })
        );
    }

    #[test]
    fn anchor_discriminator_matches_expected_length() {
        assert_eq!(anchor_instruction_discriminator("swap").len(), 8);
    }

    #[test]
    fn decodes_orca_whirlpool_swap_v2_instruction() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec!["whirlpool".into(), "vault-a".into(), "vault-b".into()],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "orca-program".into(),
                whirlpool: "whirlpool".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 64,
                fee_bps: 30,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_pool(tracked_pool);

        let mut data = vec![0u8; 43];
        data[..8].copy_from_slice(&anchor_instruction_discriminator(
            ORCA_SWAP_V2_DISCRIMINATOR_NAME,
        ));
        write_u64(&mut data, 8, 123);
        write_u64(&mut data, 16, 45);
        data[40] = 1;
        data[41] = 1;
        data[42] = 0;

        let decoded = registry.decode_instruction(&ResolvedInstruction {
            program_id: "orca-program".into(),
            accounts: vec![
                "token-program-a".into(),
                "token-program-b".into(),
                "memo".into(),
                "authority".into(),
                "whirlpool".into(),
                "mint-a".into(),
                "mint-b".into(),
                "user-a".into(),
                "vault-a".into(),
                "user-b".into(),
                "vault-b".into(),
                "tick-0".into(),
                "tick-1".into(),
                "tick-2".into(),
                "oracle".into(),
            ],
            data,
        });

        assert_eq!(
            decoded,
            Some(PoolMutation::OrcaWhirlpoolSwap {
                pool_id: "pool-orca".into(),
                amount: 123,
                other_amount_threshold: 45,
                exact_out: false,
                a_to_b: true,
            })
        );
    }

    #[test]
    fn decodes_orca_whirlpool_two_hop_swap_v2_for_trump_sol_pool() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let registry = TrackedPoolRegistry::from_config(&config);

        let mut data = vec![0u8; 60];
        data[..8].copy_from_slice(&anchor_instruction_discriminator(
            ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME,
        ));
        write_u64(&mut data, 8, 1_000_000);
        write_u64(&mut data, 16, 900_000);
        data[24] = 1;
        data[25] = 0;
        data[26] = 1;
        data[59] = 0;

        let decoded = registry.decode_instruction(&ResolvedInstruction {
            program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
            accounts: vec![
                "other-whirlpool".into(),
                "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
                "mint-in".into(),
                "mint-mid".into(),
                "mint-out".into(),
                "token-program-in".into(),
                "token-program-mid".into(),
                "token-program-out".into(),
                "user-in".into(),
                "vault-one-in".into(),
                "vault-one-mid".into(),
                "vault-two-mid".into(),
                "vault-two-out".into(),
                "user-out".into(),
                "authority".into(),
                "tick-one-0".into(),
                "tick-one-1".into(),
                "tick-one-2".into(),
                "tick-two-0".into(),
                "tick-two-1".into(),
                "tick-two-2".into(),
                "oracle-one".into(),
                "oracle-two".into(),
                "memo".into(),
            ],
            data,
        });

        assert_eq!(
            decoded,
            Some(PoolMutation::OrcaWhirlpoolSwap {
                pool_id: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
                amount: 1_000_000,
                other_amount_threshold: 900_000,
                exact_out: false,
                a_to_b: true,
            })
        );
    }

    #[test]
    fn decodes_raydium_clmm_swap_v2_exact_out_instruction() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-clmm".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec!["pool-state".into(), "vault-a".into(), "vault-b".into()],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "raydium-clmm-program".into(),
                pool_state: "pool-state".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 60,
                fee_bps: 25,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_pool(tracked_pool);

        let mut data = vec![0u8; 41];
        data[..8].copy_from_slice(&anchor_instruction_discriminator(
            RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME,
        ));
        write_u64(&mut data, 8, 123);
        write_u64(&mut data, 16, 45);
        data[40] = 0;

        let decoded = registry.decode_instruction(&ResolvedInstruction {
            program_id: "raydium-clmm-program".into(),
            accounts: vec![
                "payer".into(),
                "amm-config".into(),
                "pool-state".into(),
                "user-in".into(),
                "user-out".into(),
                "vault-a".into(),
                "vault-b".into(),
                "observation".into(),
            ],
            data,
        });

        assert_eq!(
            decoded,
            Some(PoolMutation::RaydiumClmmSwap {
                pool_id: "pool-clmm".into(),
                amount: 123,
                other_amount_threshold: 45,
                exact_out: true,
                zero_for_one: true,
            })
        );
    }

    #[test]
    fn resolve_transaction_only_marks_writable_accounts_as_touched() {
        let transaction = LegacyVersionedTransaction {
            signatures: Vec::new(),
            message: LegacyVersionedMessage::Legacy(solana_message_legacy::legacy::Message {
                header: solana_message_legacy::MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: vec![
                    parse_legacy_pubkey("11111111111111111111111111111112").unwrap(),
                    parse_legacy_pubkey("11111111111111111111111111111113").unwrap(),
                    parse_legacy_pubkey("11111111111111111111111111111111").unwrap(),
                ],
                recent_blockhash: Default::default(),
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![1],
                    data: vec![],
                }],
            }),
        };

        let resolved = resolve_transaction(&transaction, &HashMap::new())
            .expect("legacy transaction should resolve");

        assert_eq!(
            resolved.account_keys,
            vec![
                "11111111111111111111111111111112".to_string(),
                "11111111111111111111111111111113".to_string(),
                "11111111111111111111111111111111".to_string(),
            ]
        );
        assert_eq!(
            resolved.writable_account_keys,
            vec!["11111111111111111111111111111112".to_string()]
        );
    }

    #[test]
    fn converts_proto_timestamp_to_system_time() {
        let converted = timestamp_to_system_time(&Timestamp {
            seconds: 12,
            nanos: 34,
        })
        .unwrap();

        assert_eq!(
            converted,
            UNIX_EPOCH
                .checked_add(Duration::from_secs(12))
                .unwrap()
                .checked_add(Duration::from_nanos(34))
                .unwrap()
        );
    }

    #[test]
    fn clmm_repairs_only_fetch_primary_pool_accounts() {
        let whirlpool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "orca-program".into(),
                whirlpool: "whirlpool".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 4,
                fee_bps: 4,
            }),
        };
        let raydium = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "raydium-program".into(),
                pool_state: "pool-state".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 1,
                fee_bps: 4,
            }),
        };

        assert_eq!(
            whirlpool.repair_account_keys(),
            vec!["whirlpool".to_string()]
        );
        assert_eq!(
            raydium.repair_account_keys(),
            vec!["pool-state".to_string()]
        );
    }

    #[test]
    fn repair_builds_orca_whirlpool_as_verified_state() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "orca-program".into(),
                whirlpool: "whirlpool".into(),
                token_mint_a: "So11111111111111111111111111111111111111112".into(),
                token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 4,
                fee_bps: 4,
            }),
        };
        let mut raw = vec![0u8; 245];
        raw[..8].copy_from_slice(&anchor_account_discriminator(ORCA_WHIRLPOOL_ACCOUNT_NAME));
        write_u16(&mut raw, 45, 400);
        write_u128(&mut raw, 49, 500_000);
        write_u128(&mut raw, 65, 1u128 << 64);
        write_i32(&mut raw, 81, 12);
        let accounts = HashMap::from([(
            "whirlpool".into(),
            rpc_account_with_owner(raw, "orca-program"),
        )]);
        let states = Arc::new(RwLock::new(ExecutablePoolStateStore::default()));

        let next = build_executable_state_from_accounts(&tracked_pool, &accounts, 77, &states)
            .expect("repair should rebuild the pool state");

        let state::types::ExecutablePoolState::OrcaWhirlpool(state) = next else {
            panic!("expected whirlpool state");
        };
        assert_eq!(state.confidence, PoolConfidence::Verified);
        assert!(!state.repair_pending);
        assert_eq!(state.last_verified_slot, 77);
    }

    #[test]
    fn repair_builds_raydium_clmm_as_verified_state() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "raydium-program".into(),
                pool_state: "pool-state".into(),
                token_mint_a: "So11111111111111111111111111111111111111112".into(),
                token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 1,
                fee_bps: 4,
            }),
        };
        let mut raw = vec![0u8; 273];
        raw[..8].copy_from_slice(&anchor_account_discriminator(RAYDIUM_CLMM_ACCOUNT_NAME));
        write_u128(&mut raw, 237, 750_000);
        write_u128(&mut raw, 253, 1u128 << 64);
        write_i32(&mut raw, 269, -8);
        let accounts = HashMap::from([(
            "pool-state".into(),
            rpc_account_with_owner(raw, "raydium-program"),
        )]);
        let states = Arc::new(RwLock::new(ExecutablePoolStateStore::default()));

        let next = build_executable_state_from_accounts(&tracked_pool, &accounts, 88, &states)
            .expect("repair should rebuild the pool state");

        let state::types::ExecutablePoolState::RaydiumClmm(state) = next else {
            panic!("expected raydium clmm state");
        };
        assert_eq!(state.confidence, PoolConfidence::Verified);
        assert!(!state.repair_pending);
        assert_eq!(state.last_verified_slot, 88);
    }

    #[test]
    fn pool_sync_tracker_extends_refresh_deadline_and_expires_after_failure() {
        let mut tracker = PoolSyncTracker::default();

        let (first_deadline, first_enqueue) = tracker.schedule_refresh("pool-a", 100);
        let (second_deadline, second_enqueue) = tracker.schedule_refresh("pool-a", 103);

        assert_eq!(first_deadline, 108);
        assert!(first_enqueue);
        assert_eq!(second_deadline, 111);
        assert!(!second_enqueue);

        tracker.mark_refresh_started("pool-a");
        assert!(tracker.take_expired_refreshes(120).is_empty());

        tracker.mark_refresh_failed("pool-a");
        assert_eq!(
            tracker.take_expired_refreshes(112),
            vec!["pool-a".to_string()]
        );
        assert!(!tracker.refresh_required("pool-a"));
    }

    #[test]
    fn hybrid_refresh_is_only_enabled_for_active_clmm_and_whirlpool() {
        let whirlpool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "orca-program".into(),
                whirlpool: "whirlpool".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 4,
                fee_bps: 4,
            }),
        };
        let shadow_clmm = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Shadow,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "ray-program".into(),
                pool_state: "pool-state".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                tick_spacing: 1,
                fee_bps: 1,
            }),
        };

        assert!(whirlpool.uses_hybrid_refresh());
        assert!(!shadow_clmm.uses_hybrid_refresh());
    }
}
