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

use crate::{
    DirectionalPoolQuoteModelUpdate, EventSourceKind, IngestError, InitializedTickUpdate,
    MarketEventSource, NormalizedEvent, PoolInvalidation, PoolQuoteModelUpdate, PoolSnapshotUpdate,
    TickArrayWindowUpdate,
    account_batcher::{GetMultipleAccountsBatcher, LookupTableCacheHandle, RpcAccountValue},
    events::SnapshotConfidence,
    rpc::RpcError,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use domain::{
    ConcentratedLiquidityPoolState, ConstantProductPoolState, ExecutablePoolState,
    ExecutablePoolStateStore, LookupTableSnapshot, ORCA_WHIRLPOOL_TICK_ARRAY_SIZE, PoolConfidence,
    PoolId, PoolVenue, RAYDIUM_CLMM_TICK_ARRAY_SIZE, derive_orca_tick_arrays,
    derive_raydium_tick_arrays, tick_array_end_index,
};
use prost_types::Timestamp as ProstTimestamp;
use solana_entry_legacy::entry::Entry as LegacyEntry;
use solana_message_legacy::{
    AccountKeys, VersionedMessage as LegacyVersionedMessage,
    compiled_instruction::CompiledInstruction, v0,
};
use solana_pubkey_legacy::Pubkey as LegacyPubkey;
use solana_sdk::{hash::hashv, pubkey::Pubkey};
use solana_transaction_legacy::versioned::VersionedTransaction as LegacyVersionedTransaction;
use tonic::transport::Endpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReducerRolloutMode {
    Disabled,
    Shadow,
    #[default]
    Active,
}

#[derive(Debug, Clone)]
pub struct LiveRepairEvent {
    pub pool_id: String,
    pub kind: LiveRepairEventKind,
    pub occurred_at: SystemTime,
}

#[derive(Debug, Clone)]
pub enum LiveRepairEventKind {
    RefreshScheduled { deadline_slot: u64 },
    RefreshAttemptStarted,
    RefreshAttemptFailed,
    RefreshAttemptSucceeded { latency_ms: u64 },
    RefreshCleared,
    RepairAttemptStarted,
    RepairAttemptFailed,
    RepairAttemptSucceeded { latency_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRepairTransition {
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

pub trait LiveHooks: Send + Sync {
    fn pool_is_blocked_from_repair(&self, _pool_id: &str, _observed_slot: u64) -> bool {
        false
    }

    fn publish_repair(&self, _event: LiveRepairEvent) {}

    fn on_repair_transition(
        &self,
        _pool_id: &str,
        _transition: LiveRepairTransition,
        _observed_slot: u64,
    ) {
    }
}

#[derive(Debug, Default)]
pub struct NoopLiveHooks;

impl LiveHooks for NoopLiveHooks {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcEntriesConfig {
    pub grpc_endpoint: String,
    pub buffer_capacity: usize,
    pub grpc_connect_timeout_ms: u64,
    pub reconnect_backoff_millis: u64,
    pub max_reconnect_backoff_millis: u64,
    pub max_repair_in_flight: usize,
    pub tracked_pools: Vec<TrackedPool>,
    pub lookup_table_keys: Vec<String>,
}

#[allow(dead_code)]
pub mod shared {
    tonic::include_proto!("shared");
}

#[allow(dead_code)]
pub mod proto {
    tonic::include_proto!("shredstream");
}

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
const ORCA_TICK_ARRAY_ACCOUNT_NAME: &str = "TickArray";
const RAYDIUM_TICK_ARRAY_ACCOUNT_NAME: &str = "TickArrayState";
const ORCA_WHIRLPOOL_ACCOUNT_MIN_LEN: usize = 245;
const RAYDIUM_CLMM_ACCOUNT_MIN_LEN: usize = 273;
const ORCA_TICK_ARRAY_ACCOUNT_MIN_LEN: usize = 8 + 4 + 113 * 88 + 32;
const RAYDIUM_TICK_ARRAY_ACCOUNT_MIN_LEN: usize = 8 + 32 + 4 + 168 * 60 + 1 + 8 + 107;

#[derive(Debug)]
pub struct GrpcEntriesEventSource {
    receiver: Receiver<NormalizedEvent>,
}

impl GrpcEntriesEventSource {
    pub fn spawn(
        config: GrpcEntriesConfig,
        hooks: Arc<dyn LiveHooks>,
        account_batcher: GetMultipleAccountsBatcher,
        lookup_table_cache: LookupTableCacheHandle,
    ) -> Result<Self, String> {
        Endpoint::from_shared(config.grpc_endpoint.clone()).map_err(|error| error.to_string())?;

        let registry = Arc::new(TrackedPoolRegistry::from_grpc_config(&config));
        let executable_states = Arc::new(RwLock::new(ExecutablePoolStateStore::default()));
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (event_tx, event_rx) = mpsc::sync_channel(config.buffer_capacity.max(64));
        let sequence = Arc::new(AtomicU64::new(1));

        let repair_tx = spawn_repair_worker(
            &config,
            Arc::clone(&registry),
            Arc::clone(&executable_states),
            Arc::clone(&sync_tracker),
            event_tx.clone(),
            Arc::clone(&sequence),
            Arc::clone(&hooks),
            account_batcher.clone(),
        );

        seed_initial_repairs(registry.pool_ids(), &sync_tracker, &repair_tx);

        spawn_stream_worker(
            &config,
            registry,
            executable_states,
            sync_tracker,
            lookup_table_cache,
            event_tx,
            repair_tx,
            sequence,
            hooks,
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
pub struct OrcaSimpleTrackedConfig {
    pub program_id: String,
    pub swap_account: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumSimpleTrackedConfig {
    pub program_id: String,
    pub token_program_id: String,
    pub amm_pool: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub fee_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaWhirlpoolTrackedConfig {
    pub program_id: String,
    pub whirlpool: String,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub tick_spacing: u16,
    pub fee_bps: u16,
    pub require_a_to_b: bool,
    pub require_b_to_a: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumClmmTrackedConfig {
    pub program_id: String,
    pub pool_state: String,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub token_vault_a: String,
    pub token_vault_b: String,
    pub tick_spacing: u16,
    pub fee_bps: u16,
    pub require_zero_for_one: bool,
    pub require_one_for_zero: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackedPoolKind {
    OrcaSimple(OrcaSimpleTrackedConfig),
    RaydiumSimple(RaydiumSimpleTrackedConfig),
    OrcaWhirlpool(OrcaWhirlpoolTrackedConfig),
    RaydiumClmm(RaydiumClmmTrackedConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedPool {
    pub pool_id: String,
    pub reducer_mode: ReducerRolloutMode,
    pub kind: TrackedPoolKind,
    pub watch_accounts: Vec<String>,
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
pub struct TrackedPoolRegistry {
    pools_by_id: HashMap<String, TrackedPool>,
    pools_by_watch_account: HashMap<String, Vec<String>>,
    orca_simple_by_account: HashMap<String, String>,
    raydium_simple_by_account: HashMap<String, String>,
    orca_whirlpool_by_account: HashMap<String, String>,
    raydium_clmm_by_account: HashMap<String, String>,
    lookup_table_keys: Vec<String>,
}

impl TrackedPoolRegistry {
    pub fn from_grpc_config(config: &GrpcEntriesConfig) -> Self {
        let mut registry = Self::default();
        let lookup_table_keys = config
            .lookup_table_keys
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        for tracked in config.tracked_pools.iter().cloned() {
            registry.register_or_merge_pool(tracked);
        }

        registry.lookup_table_keys = lookup_table_keys.into_iter().collect();
        registry
    }

    fn register_or_merge_pool(&mut self, tracked: TrackedPool) {
        if let Some(existing) = self.pools_by_id.get_mut(&tracked.pool_id) {
            match (&mut existing.kind, &tracked.kind) {
                (
                    TrackedPoolKind::OrcaWhirlpool(existing_config),
                    TrackedPoolKind::OrcaWhirlpool(new_config),
                ) => {
                    existing_config.require_a_to_b |= new_config.require_a_to_b;
                    existing_config.require_b_to_a |= new_config.require_b_to_a;
                }
                (
                    TrackedPoolKind::RaydiumClmm(existing_config),
                    TrackedPoolKind::RaydiumClmm(new_config),
                ) => {
                    existing_config.require_zero_for_one |= new_config.require_zero_for_one;
                    existing_config.require_one_for_zero |= new_config.require_one_for_zero;
                }
                _ => {}
            }
            return;
        }
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
        quote_model_update: Option<PoolQuoteModelUpdate>,
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

#[derive(Debug)]
struct RepairBuildResult {
    next_state: ExecutablePoolState,
    quote_model_update: Option<PoolQuoteModelUpdate>,
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

fn seed_initial_repairs(
    pool_ids: Vec<String>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    repair_tx: &mpsc::Sender<RepairRequest>,
) {
    for pool_id in pool_ids {
        let queued = sync_tracker
            .lock()
            .ok()
            .map(|mut tracker| tracker.repair_requested(&pool_id))
            .unwrap_or(false);
        if !queued {
            continue;
        }
        let _ = repair_tx.send(RepairRequest {
            pool_id,
            mode: SyncRequestMode::RepairAfterInvalidation,
            priority: RepairPriority::Immediate,
            observed_slot: 0,
        });
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactMutationOutcome {
    Applied(ExecutablePoolState),
    RefreshRequired,
    Invalid,
}

fn spawn_stream_worker(
    config: &GrpcEntriesConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    lookup_table_cache: LookupTableCacheHandle,
    event_tx: SyncSender<NormalizedEvent>,
    repair_tx: mpsc::Sender<RepairRequest>,
    sequence: Arc<AtomicU64>,
    hooks: Arc<dyn LiveHooks>,
) {
    let grpc_endpoint = config.grpc_endpoint.clone();
    let connect_timeout = Duration::from_millis(config.grpc_connect_timeout_ms.max(1));
    let base_backoff = Duration::from_millis(config.reconnect_backoff_millis.max(1));
    let max_backoff = Duration::from_millis(config.max_reconnect_backoff_millis.max(1));

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
                        &lookup_table_cache,
                        &event_tx,
                        &repair_tx,
                        &sequence,
                        hooks.as_ref(),
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
    lookup_table_cache: &LookupTableCacheHandle,
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    hooks: &dyn LiveHooks,
) {
    let Ok(entries) = bincode::deserialize::<Vec<LegacyEntry>>(payload) else {
        return;
    };

    let lookup_tables = lookup_table_cache.snapshot();
    let mut invalidated_pools = HashSet::new();
    expire_exact_refreshes(
        slot,
        registry,
        executable_states,
        sync_tracker,
        event_tx,
        repair_tx,
        sequence,
        hooks,
        source_received_at,
        source_latency,
        &mut invalidated_pools,
    );

    for entry in entries {
        for transaction in entry.transactions {
            let Some(resolved) = resolve_transaction(&transaction, &lookup_tables.tables_by_key)
            else {
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
                        ExactMutationOutcome::Applied(next_state) => {
                            publish_pool_snapshot_update(
                                event_tx,
                                sequence,
                                slot,
                                source_received_at,
                                source_latency,
                                &next_state,
                            );
                        }
                        ExactMutationOutcome::RefreshRequired => schedule_exact_refresh(
                            &tracked_pool,
                            slot,
                            sync_tracker,
                            repair_tx,
                            hooks,
                        ),
                        ExactMutationOutcome::Invalid if tracked_pool.uses_hybrid_refresh() => {
                            schedule_exact_refresh(
                                &tracked_pool,
                                slot,
                                sync_tracker,
                                repair_tx,
                                hooks,
                            )
                        }
                        ExactMutationOutcome::Invalid => invalidate_and_repair(
                            &tracked_pool,
                            slot,
                            source_received_at,
                            source_latency,
                            executable_states,
                            sync_tracker,
                            event_tx,
                            repair_tx,
                            sequence,
                            hooks,
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
                            hooks,
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
                        hooks,
                        &mut invalidated_pools,
                    ),
                }
            }

            for tracked_pool in registry.touched_pools(&resolved.writable_account_keys) {
                if handled_pools.contains(&tracked_pool.pool_id) {
                    continue;
                }
                if tracked_pool.uses_hybrid_refresh() {
                    schedule_exact_refresh(&tracked_pool, slot, sync_tracker, repair_tx, hooks);
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
                    hooks,
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
) -> ExactMutationOutcome {
    let Ok(mut store) = executable_states.write() else {
        return ExactMutationOutcome::Invalid;
    };
    let pool_id = PoolId(tracked_pool.pool_id.clone());
    let Some(current_state) = store.get(&pool_id).cloned() else {
        return ExactMutationOutcome::Invalid;
    };

    match reduce_mutation_exact(&current_state, tracked_pool, mutation, slot) {
        ExactMutationOutcome::Applied(next_state) => {
            if !store.upsert(next_state.clone()) {
                return ExactMutationOutcome::Invalid;
            }
            ExactMutationOutcome::Applied(next_state)
        }
        other => other,
    }
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
) -> ExactMutationOutcome {
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
        ) => reduce_orca_simple_swap(state, source_vault, destination_vault, *amount_in, slot)
            .map(ExactMutationOutcome::Applied)
            .unwrap_or(ExactMutationOutcome::Invalid),
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
        _ => ExactMutationOutcome::Invalid,
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
) -> ExactMutationOutcome {
    let Some(token_program) = parse_legacy_pubkey(&config.token_program_id) else {
        return ExactMutationOutcome::Invalid;
    };
    let Some(owner) = parse_legacy_pubkey(user_owner) else {
        return ExactMutationOutcome::Invalid;
    };
    let Some(mint_a) = parse_legacy_pubkey(&state.token_mint_a) else {
        return ExactMutationOutcome::Invalid;
    };
    let Some(mint_b) = parse_legacy_pubkey(&state.token_mint_b) else {
        return ExactMutationOutcome::Invalid;
    };
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
        // Raydium simple swaps can legitimately use non-ATA SPL accounts.
        // When the instruction accounts do not reveal a unique direction, refresh
        // the pool exactly instead of invalidating it on the hot path.
        _ => return ExactMutationOutcome::RefreshRequired,
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
        .map(ExactMutationOutcome::Applied)
        .unwrap_or(ExactMutationOutcome::Invalid)
    } else {
        reduce_constant_product_swap(
            state,
            source_vault,
            destination_vault,
            amount_specified,
            slot,
            PoolVenue::RaydiumSimplePool,
        )
        .map(ExactMutationOutcome::Applied)
        .unwrap_or(ExactMutationOutcome::Invalid)
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
    hooks: &dyn LiveHooks,
    invalidated_pools: &mut HashSet<String>,
) {
    if !invalidated_pools.insert(tracked_pool.pool_id.clone()) {
        return;
    }
    if hooks.pool_is_blocked_from_repair(&tracked_pool.pool_id, slot) {
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
        hooks.publish_repair(LiveRepairEvent {
            pool_id: tracked_pool.pool_id.clone(),
            kind: LiveRepairEventKind::RefreshCleared,
            occurred_at: source_received_at,
        });
        hooks.on_repair_transition(
            &tracked_pool.pool_id,
            LiveRepairTransition::RefreshCleared,
            slot,
        );
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
        crate::MarketEvent::PoolInvalidation(PoolInvalidation {
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
    hooks.on_repair_transition(
        &tracked_pool.pool_id,
        LiveRepairTransition::RepairQueued,
        slot,
    );
}

fn schedule_exact_refresh(
    tracked_pool: &TrackedPool,
    slot: u64,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    hooks: &dyn LiveHooks,
) {
    let (deadline_slot, should_enqueue) = match sync_tracker.lock() {
        Ok(mut tracker) => tracker.schedule_refresh(&tracked_pool.pool_id, slot),
        Err(_) => return,
    };
    hooks.publish_repair(LiveRepairEvent {
        pool_id: tracked_pool.pool_id.clone(),
        kind: LiveRepairEventKind::RefreshScheduled { deadline_slot },
        occurred_at: SystemTime::now(),
    });
    hooks.on_repair_transition(
        &tracked_pool.pool_id,
        LiveRepairTransition::RefreshScheduled,
        slot,
    );
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
    hooks: &dyn LiveHooks,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    invalidated_pools: &mut HashSet<String>,
) {
    let expired_pool_ids = match sync_tracker.lock() {
        Ok(mut tracker) => tracker.take_expired_refreshes(slot),
        Err(_) => return,
    };

    for pool_id in expired_pool_ids {
        hooks.publish_repair(LiveRepairEvent {
            pool_id: pool_id.clone(),
            kind: LiveRepairEventKind::RefreshCleared,
            occurred_at: source_received_at,
        });
        hooks.on_repair_transition(&pool_id, LiveRepairTransition::RefreshCleared, slot);
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
            hooks,
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
        crate::MarketEvent::SlotBoundary(crate::SlotBoundary { slot, leader: None }),
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
        crate::MarketEvent::PoolSnapshotUpdate(PoolSnapshotUpdate {
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

fn publish_pool_quote_model_update(
    event_tx: &SyncSender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
    slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    update: PoolQuoteModelUpdate,
) {
    let event = event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        crate::MarketEvent::PoolQuoteModelUpdate(update),
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

fn spawn_repair_worker(
    config: &GrpcEntriesConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    event_tx: SyncSender<NormalizedEvent>,
    sequence: Arc<AtomicU64>,
    hooks: Arc<dyn LiveHooks>,
    account_batcher: GetMultipleAccountsBatcher,
) -> mpsc::Sender<RepairRequest> {
    let (repair_tx, repair_rx) = mpsc::channel::<RepairRequest>();
    let retry_tx = repair_tx.clone();
    let (job_tx, job_rx) = mpsc::channel::<RepairJob>();
    let (result_tx, result_rx) = mpsc::channel::<RepairJobResult>();
    let job_rx = Arc::new(Mutex::new(job_rx));
    let max_in_flight = config.max_repair_in_flight.max(1);

    for _ in 0..max_in_flight {
        let registry = Arc::clone(&registry);
        let executable_states = Arc::clone(&executable_states);
        let account_batcher = account_batcher.clone();
        let job_rx = Arc::clone(&job_rx);
        let result_tx = result_tx.clone();
        thread::spawn(move || {
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
                let Some(build_result) =
                    build_repair_result(&account_batcher, &tracked_pool, &executable_states)
                else {
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
                    slot: build_result.next_state.last_update_slot(),
                    next_state: build_result.next_state,
                    quote_model_update: build_result.quote_model_update,
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
                hooks.as_ref(),
                &retry_tx,
                &mut in_flight,
                &mut attempts,
            );
            dispatch_repair_jobs(
                &job_tx,
                &executable_states,
                &sync_tracker,
                hooks.as_ref(),
                &mut pending,
                &mut pending_modes,
                &mut in_flight,
                &mut attempts,
                max_in_flight,
            );

            match repair_rx.recv_timeout(Duration::from_millis(25)) {
                Ok(request) => {
                    enqueue_repair_request(
                        request,
                        &executable_states,
                        &sync_tracker,
                        hooks.as_ref(),
                        &mut pending,
                        &mut pending_modes,
                        &in_flight,
                    );
                    while let Ok(next) = repair_rx.try_recv() {
                        enqueue_repair_request(
                            next,
                            &executable_states,
                            &sync_tracker,
                            hooks.as_ref(),
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
                        hooks.as_ref(),
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
    hooks: &dyn LiveHooks,
    pending: &mut VecDeque<String>,
    pending_modes: &mut HashMap<String, RepairRequest>,
    in_flight: &HashMap<String, SyncRequestMode>,
) {
    if !request_still_needed(&request, executable_states, sync_tracker, hooks) {
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
    hooks: &dyn LiveHooks,
    pending: &mut VecDeque<String>,
    pending_modes: &mut HashMap<String, RepairRequest>,
    in_flight: &mut HashMap<String, SyncRequestMode>,
    attempts: &mut HashMap<(String, SyncRequestMode), u32>,
    max_in_flight: usize,
) {
    while in_flight.len() < max_in_flight {
        let Some(pool_id) = pending.pop_front() else {
            break;
        };
        let Some(request) = pending_modes.remove(&pool_id) else {
            continue;
        };
        if !request_still_needed(&request, executable_states, sync_tracker, hooks) {
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
        hooks.publish_repair(LiveRepairEvent {
            pool_id: pool_id.clone(),
            kind: match request.mode {
                SyncRequestMode::RefreshExact => LiveRepairEventKind::RefreshAttemptStarted,
                SyncRequestMode::RepairAfterInvalidation => {
                    LiveRepairEventKind::RepairAttemptStarted
                }
            },
            occurred_at: SystemTime::now(),
        });
        hooks.on_repair_transition(
            &pool_id,
            match request.mode {
                SyncRequestMode::RefreshExact => LiveRepairTransition::RefreshStarted,
                SyncRequestMode::RepairAfterInvalidation => LiveRepairTransition::RepairStarted,
            },
            request.observed_slot,
        );
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
    hooks: &dyn LiveHooks,
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
                quote_model_update,
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
                    if let Some(quote_model_update) = quote_model_update {
                        publish_pool_quote_model_update(
                            event_tx,
                            sequence,
                            slot,
                            SystemTime::now(),
                            None,
                            quote_model_update,
                        );
                    }
                }
                if mode == SyncRequestMode::RefreshExact {
                    clear_exact_refresh(sync_tracker, &pool_id);
                } else if let Ok(mut tracker) = sync_tracker.lock() {
                    tracker.clear_repair(&pool_id);
                }
                attempts.remove(&(pool_id.clone(), mode));
                hooks.publish_repair(LiveRepairEvent {
                    pool_id,
                    kind: match mode {
                        SyncRequestMode::RefreshExact => {
                            LiveRepairEventKind::RefreshAttemptSucceeded {
                                latency_ms: latency.as_millis().min(u128::from(u64::MAX)) as u64,
                            }
                        }
                        SyncRequestMode::RepairAfterInvalidation => {
                            LiveRepairEventKind::RepairAttemptSucceeded {
                                latency_ms: latency.as_millis().min(u128::from(u64::MAX)) as u64,
                            }
                        }
                    },
                    occurred_at: SystemTime::now(),
                });
                hooks.on_repair_transition(
                    next_state.pool_id().0.as_str(),
                    match mode {
                        SyncRequestMode::RefreshExact => LiveRepairTransition::RefreshSucceeded,
                        SyncRequestMode::RepairAfterInvalidation => {
                            LiveRepairTransition::RepairSucceeded
                        }
                    },
                    slot,
                );
                if !next_state.confidence().is_executable() {
                    let _ = retry_tx.send(RepairRequest {
                        pool_id: next_state.pool_id().0.clone(),
                        mode: SyncRequestMode::RefreshExact,
                        priority: RepairPriority::Retry,
                        observed_slot: slot,
                    });
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
                hooks.publish_repair(LiveRepairEvent {
                    pool_id: pool_id.clone(),
                    kind: match mode {
                        SyncRequestMode::RefreshExact => LiveRepairEventKind::RefreshAttemptFailed,
                        SyncRequestMode::RepairAfterInvalidation => {
                            LiveRepairEventKind::RepairAttemptFailed
                        }
                    },
                    occurred_at: SystemTime::now(),
                });
                hooks.on_repair_transition(
                    &pool_id,
                    match mode {
                        SyncRequestMode::RefreshExact => LiveRepairTransition::RefreshFailed,
                        SyncRequestMode::RepairAfterInvalidation => {
                            LiveRepairTransition::RepairFailed
                        }
                    },
                    observed_slot,
                );
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
                hooks.on_repair_transition(
                    &pool_id,
                    match mode {
                        SyncRequestMode::RefreshExact => LiveRepairTransition::RefreshCleared,
                        SyncRequestMode::RepairAfterInvalidation => {
                            LiveRepairTransition::RepairFailed
                        }
                    },
                    observed_slot,
                );
            }
        }
    }
}

fn request_still_needed(
    request: &RepairRequest,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    hooks: &dyn LiveHooks,
) -> bool {
    if hooks.pool_is_blocked_from_repair(&request.pool_id, request.observed_slot) {
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
        0 | 1 => Duration::from_millis(250),
        2 => Duration::from_millis(500),
        3 => Duration::from_secs(1),
        4 => Duration::from_secs(2),
        _ => Duration::from_secs(5),
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

fn build_repair_result(
    account_batcher: &GetMultipleAccountsBatcher,
    tracked_pool: &TrackedPool,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> Option<RepairBuildResult> {
    let (slot, mut accounts) =
        match fetch_accounts_by_key(account_batcher, &tracked_pool.repair_account_keys()) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("repair fetch {} failed: {error}", tracked_pool.pool_id);
                return None;
            }
        };
    if extend_with_required_quote_model_accounts(account_batcher, tracked_pool, &mut accounts)
        .is_err()
    {
        return None;
    }
    build_executable_state_from_accounts(tracked_pool, &accounts, slot, executable_states)
}

fn extend_with_required_quote_model_accounts(
    account_batcher: &GetMultipleAccountsBatcher,
    tracked_pool: &TrackedPool,
    accounts: &mut HashMap<String, RpcAccountValue>,
) -> Result<(), RpcError> {
    let extra_keys = match &tracked_pool.kind {
        TrackedPoolKind::OrcaWhirlpool(config) => {
            let account =
                accounts
                    .get(&config.whirlpool)
                    .ok_or_else(|| RpcError::DecodeFailed {
                        method: "getMultipleAccounts".into(),
                        detail: "missing whirlpool account".into(),
                    })?;
            let raw = decode_base64_account(account).ok_or_else(|| RpcError::DecodeFailed {
                method: "getMultipleAccounts".into(),
                detail: "invalid whirlpool account encoding".into(),
            })?;
            let current_tick_index = read_i32(&raw, 81).ok_or_else(|| RpcError::DecodeFailed {
                method: "getMultipleAccounts".into(),
                detail: "missing whirlpool current_tick_index".into(),
            })?;
            required_orca_tick_array_keys(config, current_tick_index)?
        }
        TrackedPoolKind::RaydiumClmm(config) => {
            let account =
                accounts
                    .get(&config.pool_state)
                    .ok_or_else(|| RpcError::DecodeFailed {
                        method: "getMultipleAccounts".into(),
                        detail: "missing clmm pool state".into(),
                    })?;
            let raw = decode_base64_account(account).ok_or_else(|| RpcError::DecodeFailed {
                method: "getMultipleAccounts".into(),
                detail: "invalid clmm pool encoding".into(),
            })?;
            let current_tick_index = read_i32(&raw, 269).ok_or_else(|| RpcError::DecodeFailed {
                method: "getMultipleAccounts".into(),
                detail: "missing clmm current_tick_index".into(),
            })?;
            required_raydium_tick_array_keys(config, current_tick_index)?
        }
        _ => Vec::new(),
    };
    if extra_keys.is_empty() {
        return Ok(());
    }

    let missing = extra_keys
        .into_iter()
        .filter(|key| !accounts.contains_key(key))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let (_, extra_accounts) = fetch_accounts_by_key(account_batcher, &missing)?;
    accounts.extend(extra_accounts);
    Ok(())
}

fn required_orca_tick_array_keys(
    config: &OrcaWhirlpoolTrackedConfig,
    current_tick_index: i32,
) -> Result<Vec<String>, RpcError> {
    let program_id =
        Pubkey::from_str(&config.program_id).map_err(|error| RpcError::DecodeFailed {
            method: "getMultipleAccounts".into(),
            detail: error.to_string(),
        })?;
    let whirlpool =
        Pubkey::from_str(&config.whirlpool).map_err(|error| RpcError::DecodeFailed {
            method: "getMultipleAccounts".into(),
            detail: error.to_string(),
        })?;
    let mut keys = BTreeSet::new();
    if config.require_a_to_b {
        for key in derive_orca_tick_arrays(
            program_id,
            whirlpool,
            config.tick_spacing,
            current_tick_index,
            true,
        ) {
            keys.insert(key.to_string());
        }
    }
    if config.require_b_to_a {
        for key in derive_orca_tick_arrays(
            program_id,
            whirlpool,
            config.tick_spacing,
            current_tick_index,
            false,
        ) {
            keys.insert(key.to_string());
        }
    }
    Ok(keys.into_iter().collect())
}

fn required_raydium_tick_array_keys(
    config: &RaydiumClmmTrackedConfig,
    current_tick_index: i32,
) -> Result<Vec<String>, RpcError> {
    let program_id =
        Pubkey::from_str(&config.program_id).map_err(|error| RpcError::DecodeFailed {
            method: "getMultipleAccounts".into(),
            detail: error.to_string(),
        })?;
    let pool_state =
        Pubkey::from_str(&config.pool_state).map_err(|error| RpcError::DecodeFailed {
            method: "getMultipleAccounts".into(),
            detail: error.to_string(),
        })?;
    let mut keys = BTreeSet::new();
    if config.require_zero_for_one {
        for key in derive_raydium_tick_arrays(
            program_id,
            pool_state,
            config.tick_spacing,
            current_tick_index,
            true,
        ) {
            keys.insert(key.to_string());
        }
    }
    if config.require_one_for_zero {
        for key in derive_raydium_tick_arrays(
            program_id,
            pool_state,
            config.tick_spacing,
            current_tick_index,
            false,
        ) {
            keys.insert(key.to_string());
        }
    }
    Ok(keys.into_iter().collect())
}

fn decode_orca_tick_array(
    config: &OrcaWhirlpoolTrackedConfig,
    account_key: &str,
    account: &RpcAccountValue,
) -> Option<(TickArrayWindowUpdate, Vec<InitializedTickUpdate>)> {
    let raw = decode_base64_account(account)?;
    if !is_anchor_account_value(
        account,
        &raw,
        &config.program_id,
        ORCA_TICK_ARRAY_ACCOUNT_NAME,
        ORCA_TICK_ARRAY_ACCOUNT_MIN_LEN,
    ) {
        return None;
    }
    let start_tick_index = read_i32(&raw, 8)?;
    let mut initialized_ticks = Vec::new();
    let mut cursor = 12usize;
    for index in 0..ORCA_WHIRLPOOL_TICK_ARRAY_SIZE as usize {
        let initialized = raw.get(cursor).copied().unwrap_or_default() != 0;
        let liquidity_net = read_i128(&raw, cursor + 1)?;
        let liquidity_gross = read_u128(&raw, cursor + 17)?;
        if initialized && liquidity_gross > 0 {
            initialized_ticks.push(InitializedTickUpdate {
                tick_index: start_tick_index
                    .saturating_add((index as i32).saturating_mul(i32::from(config.tick_spacing))),
                liquidity_net,
                liquidity_gross,
            });
        }
        cursor = cursor.saturating_add(113);
    }
    let _ = account_key;
    Some((
        TickArrayWindowUpdate {
            start_tick_index,
            end_tick_index: tick_array_end_index(
                start_tick_index,
                config.tick_spacing,
                ORCA_WHIRLPOOL_TICK_ARRAY_SIZE,
            ),
            initialized_tick_count: initialized_ticks.len(),
        },
        initialized_ticks,
    ))
}

fn decode_raydium_tick_array(
    config: &RaydiumClmmTrackedConfig,
    account_key: &str,
    account: &RpcAccountValue,
) -> Option<(TickArrayWindowUpdate, Vec<InitializedTickUpdate>)> {
    let raw = decode_base64_account(account)?;
    if !is_anchor_account_value(
        account,
        &raw,
        &config.program_id,
        RAYDIUM_TICK_ARRAY_ACCOUNT_NAME,
        RAYDIUM_TICK_ARRAY_ACCOUNT_MIN_LEN,
    ) {
        return None;
    }
    let start_tick_index = read_i32(&raw, 40)?;
    let mut initialized_ticks = Vec::new();
    let mut cursor = 44usize;
    for _ in 0..RAYDIUM_CLMM_TICK_ARRAY_SIZE as usize {
        let tick_index = read_i32(&raw, cursor)?;
        let liquidity_net = read_i128(&raw, cursor + 4)?;
        let liquidity_gross = read_u128(&raw, cursor + 20)?;
        if liquidity_gross > 0 {
            initialized_ticks.push(InitializedTickUpdate {
                tick_index,
                liquidity_net,
                liquidity_gross,
            });
        }
        cursor = cursor.saturating_add(168);
    }
    let _ = account_key;
    Some((
        TickArrayWindowUpdate {
            start_tick_index,
            end_tick_index: tick_array_end_index(
                start_tick_index,
                config.tick_spacing,
                RAYDIUM_CLMM_TICK_ARRAY_SIZE,
            ),
            initialized_tick_count: initialized_ticks.len(),
        },
        initialized_ticks,
    ))
}

fn build_orca_directional_quote_model(
    config: &OrcaWhirlpoolTrackedConfig,
    accounts: &HashMap<String, RpcAccountValue>,
    current_tick_index: i32,
    a_to_b: bool,
) -> Option<DirectionalPoolQuoteModelUpdate> {
    let program_id = Pubkey::from_str(&config.program_id).ok()?;
    let whirlpool = Pubkey::from_str(&config.whirlpool).ok()?;
    let keys = derive_orca_tick_arrays(
        program_id,
        whirlpool,
        config.tick_spacing,
        current_tick_index,
        a_to_b,
    );
    let mut windows = Vec::new();
    let mut initialized_ticks = Vec::new();
    for key in keys {
        let key = key.to_string();
        let Some(account) = accounts.get(&key) else {
            continue;
        };
        let Some((window, mut ticks)) = decode_orca_tick_array(config, &key, account) else {
            continue;
        };
        windows.push(window);
        initialized_ticks.append(&mut ticks);
    }
    windows.sort_by_key(|window| window.start_tick_index);
    initialized_ticks.sort_by_key(|tick| tick.tick_index);
    Some(DirectionalPoolQuoteModelUpdate {
        loaded_tick_arrays: windows.len(),
        expected_tick_arrays: 3,
        complete: windows.len() == 3,
        windows,
        initialized_ticks,
    })
}

fn build_raydium_directional_quote_model(
    config: &RaydiumClmmTrackedConfig,
    accounts: &HashMap<String, RpcAccountValue>,
    current_tick_index: i32,
    zero_for_one: bool,
) -> Option<DirectionalPoolQuoteModelUpdate> {
    let program_id = Pubkey::from_str(&config.program_id).ok()?;
    let pool_state = Pubkey::from_str(&config.pool_state).ok()?;
    let keys = derive_raydium_tick_arrays(
        program_id,
        pool_state,
        config.tick_spacing,
        current_tick_index,
        zero_for_one,
    );
    let mut windows = Vec::new();
    let mut initialized_ticks = Vec::new();
    for key in keys {
        let key = key.to_string();
        let Some(account) = accounts.get(&key) else {
            continue;
        };
        let Some((window, mut ticks)) = decode_raydium_tick_array(config, &key, account) else {
            continue;
        };
        windows.push(window);
        initialized_ticks.append(&mut ticks);
    }
    windows.sort_by_key(|window| window.start_tick_index);
    initialized_ticks.sort_by_key(|tick| tick.tick_index);
    Some(DirectionalPoolQuoteModelUpdate {
        loaded_tick_arrays: windows.len(),
        expected_tick_arrays: 3,
        complete: windows.len() == 3,
        windows,
        initialized_ticks,
    })
}

fn quote_model_union(
    direction_a: Option<&DirectionalPoolQuoteModelUpdate>,
    direction_b: Option<&DirectionalPoolQuoteModelUpdate>,
) -> (usize, usize) {
    let mut windows = BTreeSet::new();
    let mut expected = 0usize;
    for direction in [direction_a, direction_b].into_iter().flatten() {
        expected = expected.max(direction.expected_tick_arrays);
        for window in &direction.windows {
            windows.insert(window.start_tick_index);
        }
    }
    let required_directions =
        usize::from(direction_a.is_some()) + usize::from(direction_b.is_some());
    (
        windows.len(),
        expected.saturating_mul(required_directions.max(1)),
    )
}

fn build_executable_state_from_accounts(
    tracked_pool: &TrackedPool,
    accounts: &HashMap<String, RpcAccountValue>,
    slot: u64,
    executable_states: &Arc<RwLock<ExecutablePoolStateStore>>,
) -> Option<RepairBuildResult> {
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
            Some(RepairBuildResult {
                next_state: ExecutablePoolState::OrcaSimplePool(ConstantProductPoolState {
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
                }),
                quote_model_update: None,
            })
        }
        TrackedPoolKind::RaydiumSimple(config) => {
            let reserve_a = token_account_amount(accounts.get(&config.token_vault_a)?)?;
            let reserve_b = token_account_amount(accounts.get(&config.token_vault_b)?)?;
            Some(RepairBuildResult {
                next_state: ExecutablePoolState::RaydiumSimplePool(ConstantProductPoolState {
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
                }),
                quote_model_update: None,
            })
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
            let a_to_b = config
                .require_a_to_b
                .then(|| {
                    build_orca_directional_quote_model(config, accounts, current_tick_index, true)
                })
                .flatten();
            let b_to_a = config
                .require_b_to_a
                .then(|| {
                    build_orca_directional_quote_model(config, accounts, current_tick_index, false)
                })
                .flatten();
            let quote_model_update = PoolQuoteModelUpdate {
                pool_id: tracked_pool.pool_id.clone(),
                liquidity,
                sqrt_price_x64,
                current_tick_index,
                tick_spacing: config.tick_spacing,
                required_a_to_b: config.require_a_to_b,
                required_b_to_a: config.require_b_to_a,
                a_to_b: a_to_b.clone(),
                b_to_a: b_to_a.clone(),
                slot,
                write_version: next_write_version,
            };
            let executable = (!config.require_a_to_b
                || a_to_b
                    .as_ref()
                    .map(|direction| direction.complete)
                    .unwrap_or(false))
                && (!config.require_b_to_a
                    || b_to_a
                        .as_ref()
                        .map(|direction| direction.complete)
                        .unwrap_or(false));
            let (loaded_tick_arrays, expected_tick_arrays) =
                quote_model_union(a_to_b.as_ref(), b_to_a.as_ref());
            Some(RepairBuildResult {
                next_state: ExecutablePoolState::OrcaWhirlpool(ConcentratedLiquidityPoolState {
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
                    loaded_tick_arrays,
                    expected_tick_arrays,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: if executable {
                        PoolConfidence::Executable
                    } else {
                        PoolConfidence::Verified
                    },
                    repair_pending: false,
                }),
                quote_model_update: Some(quote_model_update),
            })
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
            let zero_for_one = config
                .require_zero_for_one
                .then(|| {
                    build_raydium_directional_quote_model(
                        config,
                        accounts,
                        current_tick_index,
                        true,
                    )
                })
                .flatten();
            let one_for_zero = config
                .require_one_for_zero
                .then(|| {
                    build_raydium_directional_quote_model(
                        config,
                        accounts,
                        current_tick_index,
                        false,
                    )
                })
                .flatten();
            let quote_model_update = PoolQuoteModelUpdate {
                pool_id: tracked_pool.pool_id.clone(),
                liquidity,
                sqrt_price_x64,
                current_tick_index,
                tick_spacing: config.tick_spacing,
                required_a_to_b: config.require_zero_for_one,
                required_b_to_a: config.require_one_for_zero,
                a_to_b: zero_for_one.clone(),
                b_to_a: one_for_zero.clone(),
                slot,
                write_version: next_write_version,
            };
            let executable = (!config.require_zero_for_one
                || zero_for_one
                    .as_ref()
                    .map(|direction| direction.complete)
                    .unwrap_or(false))
                && (!config.require_one_for_zero
                    || one_for_zero
                        .as_ref()
                        .map(|direction| direction.complete)
                        .unwrap_or(false));
            let (loaded_tick_arrays, expected_tick_arrays) =
                quote_model_union(zero_for_one.as_ref(), one_for_zero.as_ref());
            Some(RepairBuildResult {
                next_state: ExecutablePoolState::RaydiumClmm(ConcentratedLiquidityPoolState {
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
                    loaded_tick_arrays,
                    expected_tick_arrays,
                    last_update_slot: slot,
                    write_version: next_write_version,
                    last_verified_slot: slot,
                    confidence: if executable {
                        PoolConfidence::Executable
                    } else {
                        PoolConfidence::Verified
                    },
                    repair_pending: false,
                }),
                quote_model_update: Some(quote_model_update),
            })
        }
    }
}

fn fetch_accounts_by_key(
    account_batcher: &GetMultipleAccountsBatcher,
    account_keys: &[String],
) -> Result<(u64, HashMap<String, RpcAccountValue>), RpcError> {
    let fetched = account_batcher.fetch(account_keys)?;
    Ok((fetched.slot, fetched.present_accounts()))
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
    payload: crate::MarketEvent,
) -> NormalizedEvent {
    NormalizedEvent {
        payload,
        source: crate::SourceMetadata {
            source: EventSourceKind::ShredStream,
            sequence,
            observed_slot,
        },
        latency: crate::LatencyMetadata {
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

fn read_i128(data: &[u8], offset: usize) -> Option<i128> {
    let bytes = data.get(offset..offset + 16)?;
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Some(i128::from_le_bytes(buf))
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        sync::{Arc, Mutex, RwLock, mpsc},
        time::{Duration, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use domain::{
        ConstantProductPoolState, ExecutablePoolState, ExecutablePoolStateStore, PoolConfidence,
        PoolId, PoolVenue,
    };
    use prost_types::Timestamp;

    use super::{
        CompiledInstruction, ExactMutationOutcome, GrpcEntriesConfig, LegacyVersionedMessage,
        LegacyVersionedTransaction, ORCA_SWAP_INSTRUCTION_TAG, ORCA_SWAP_V2_DISCRIMINATOR_NAME,
        ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME, ORCA_WHIRLPOOL_ACCOUNT_NAME,
        OrcaSimpleTrackedConfig, OrcaWhirlpoolTrackedConfig, PoolMutation, PoolSyncTracker,
        RAYDIUM_CLMM_ACCOUNT_NAME, RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME, RAYDIUM_SWAP_BASE_OUT_TAG,
        RaydiumClmmTrackedConfig, RaydiumSimpleTrackedConfig, ReducerRolloutMode,
        ResolvedInstruction, RpcAccountValue, TrackedPool, TrackedPoolKind, TrackedPoolRegistry,
        anchor_account_discriminator, anchor_instruction_discriminator, associated_token_address,
        build_executable_state_from_accounts, constant_product_amount_in_for_output,
        constant_product_amount_out, parse_legacy_pubkey, reduce_orca_simple_swap,
        reduce_raydium_simple_swap, resolve_transaction, seed_initial_repairs,
        timestamp_to_system_time,
    };

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

    fn sample_registry() -> TrackedPoolRegistry {
        TrackedPoolRegistry::from_grpc_config(&GrpcEntriesConfig {
            grpc_endpoint: "http://localhost".into(),
            buffer_capacity: 64,
            grpc_connect_timeout_ms: 100,
            reconnect_backoff_millis: 100,
            max_reconnect_backoff_millis: 1_000,
            max_repair_in_flight: 2,
            lookup_table_keys: vec!["table-a".into(), "table-b".into()],
            tracked_pools: vec![
                TrackedPool {
                    pool_id: "pool-orca-simple".into(),
                    reducer_mode: ReducerRolloutMode::Active,
                    watch_accounts: vec!["orca-swap".into(), "vault-a".into(), "vault-b".into()],
                    kind: TrackedPoolKind::OrcaSimple(OrcaSimpleTrackedConfig {
                        program_id: "orca-program".into(),
                        swap_account: "orca-swap".into(),
                        token_vault_a: "vault-a".into(),
                        token_vault_b: "vault-b".into(),
                        token_mint_a: "mint-a".into(),
                        token_mint_b: "mint-b".into(),
                        fee_bps: 30,
                    }),
                },
                TrackedPool {
                    pool_id: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
                    reducer_mode: ReducerRolloutMode::Active,
                    watch_accounts: vec![
                        "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
                        "whirlpool-vault-a".into(),
                        "whirlpool-vault-b".into(),
                    ],
                    kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                        program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                        whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
                        token_mint_a: "mint-a".into(),
                        token_mint_b: "mint-b".into(),
                        token_vault_a: "whirlpool-vault-a".into(),
                        token_vault_b: "whirlpool-vault-b".into(),
                        tick_spacing: 64,
                        fee_bps: 30,
                        require_a_to_b: true,
                        require_b_to_a: true,
                    }),
                },
            ],
        })
    }

    #[test]
    fn registry_tracks_unique_pools_from_specs() {
        let registry = sample_registry();
        assert!(registry.pool_ids().len() >= 2);
        assert_eq!(
            registry.lookup_table_keys,
            vec!["table-a".to_string(), "table-b".to_string()]
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
        let ExecutablePoolState::OrcaSimplePool(next) = next else {
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
        );

        let ExactMutationOutcome::Applied(next) = next else {
            panic!("expected applied raydium simple state");
        };
        let ExecutablePoolState::RaydiumSimplePool(next) = next else {
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
        );

        let ExactMutationOutcome::Applied(next) = next else {
            panic!("expected applied raydium simple state");
        };
        let ExecutablePoolState::RaydiumSimplePool(next) = next else {
            panic!("expected raydium simple state");
        };
        assert_eq!(next.reserve_b, 91_000_000);
        assert!(next.reserve_a > 1_000_000_000);
        assert_eq!(next.write_version, 3);
    }

    #[test]
    fn raydium_simple_reducer_requests_refresh_for_non_ata_accounts() {
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

        let outcome = reduce_raydium_simple_swap(
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
            "non-ata-source",
            "non-ata-destination",
            100_000_000,
            11,
        );

        assert_eq!(outcome, ExactMutationOutcome::RefreshRequired);
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
        registry.register_or_merge_pool(tracked_pool);

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
        registry.register_or_merge_pool(tracked_pool);

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
                require_a_to_b: true,
                require_b_to_a: false,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_or_merge_pool(tracked_pool);

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
        let registry = sample_registry();

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
                require_zero_for_one: true,
                require_one_for_zero: false,
            }),
        };
        let mut registry = TrackedPoolRegistry::default();
        registry.register_or_merge_pool(tracked_pool);

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
                require_a_to_b: true,
                require_b_to_a: false,
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
                require_zero_for_one: true,
                require_one_for_zero: false,
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
                require_a_to_b: true,
                require_b_to_a: false,
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

        let ExecutablePoolState::OrcaWhirlpool(state) = next.next_state else {
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
                require_zero_for_one: true,
                require_one_for_zero: false,
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

        let ExecutablePoolState::RaydiumClmm(state) = next.next_state else {
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
    fn seed_initial_repairs_marks_tracker_and_enqueues_once_per_pool() {
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (repair_tx, repair_rx) = mpsc::channel();

        seed_initial_repairs(
            vec!["pool-a".into(), "pool-b".into(), "pool-a".into()],
            &sync_tracker,
            &repair_tx,
        );

        let first = repair_rx.recv().expect("first repair request");
        let second = repair_rx.recv().expect("second repair request");
        assert!(repair_rx.try_recv().is_err());

        let received = BTreeSet::from([first.pool_id, second.pool_id]);
        assert_eq!(received, BTreeSet::from(["pool-a".into(), "pool-b".into()]));
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .repair_required("pool-a")
        );
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .repair_required("pool-b")
        );
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
                require_a_to_b: true,
                require_b_to_a: false,
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
                require_zero_for_one: true,
                require_one_for_zero: false,
            }),
        };

        assert!(whirlpool.uses_hybrid_refresh());
        assert!(!shadow_clmm.uses_hybrid_refresh());
    }
}
