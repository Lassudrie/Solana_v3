use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    DirectionalPoolQuoteModelUpdate, EventSourceKind, IngestError, InitializedTickUpdate,
    MarketEventSource, NormalizedEvent, PoolInvalidation, PoolQuoteModelUpdate, PoolSnapshotUpdate,
    TickArrayWindowUpdate,
    account_batcher::{
        GetMultipleAccountsBatcher, LookupTableCacheHandle, RpcAccountValue, decode_lookup_table,
    },
    events::SnapshotConfidence,
    proto::shredstream,
    rpc::{RpcError, RpcRateLimitBackoff},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use domain::{
    ConcentratedLiquidityPoolState, ConcentratedQuoteModel, ConstantProductPoolState,
    DestabilizingTransaction, DirectionalConcentratedQuoteModel, ExecutablePoolState,
    ExecutablePoolStateStore, LookupTableSnapshot, ORCA_WHIRLPOOL_TICK_ARRAY_SIZE, PoolConfidence,
    PoolId, PoolVenue, RAYDIUM_CLMM_TICK_ARRAY_SIZE, derive_orca_tick_arrays,
    derive_raydium_tick_arrays, tick_array_end_index, tick_array_start_index,
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
use strategy::quote::{
    ConcentratedSwapState, QuoteError, apply_concentrated_exact_input_update,
    apply_concentrated_exact_output_update,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveDestabilizationKind {
    Unclassified,
    Candidate,
    Triggered,
}

#[derive(Debug, Clone)]
pub struct LiveDestabilizationEvent {
    pub pool_id: String,
    pub observed_slot: u64,
    pub kind: LiveDestabilizationKind,
    pub input_amount: Option<u64>,
    pub estimated_price_impact_bps: Option<u16>,
    pub estimated_price_dislocation_bps: Option<u16>,
    pub estimated_net_edge_bps: Option<u16>,
    pub impact_floor_bps: u16,
    pub dislocation_trigger_bps: u16,
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

    fn publish_destabilization(&self, _event: LiveDestabilizationEvent) {}
}

#[derive(Debug, Default)]
pub struct NoopLiveHooks;

impl LiveHooks for NoopLiveHooks {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct LiveQuoteModelVersion {
    slot: u64,
    write_version: u64,
}

#[derive(Debug, Default)]
struct LiveConcentratedQuoteModelStore {
    models: HashMap<PoolId, PoolQuoteModelUpdate>,
    versions: HashMap<PoolId, LiveQuoteModelVersion>,
}

impl LiveConcentratedQuoteModelStore {
    fn get(&self, pool_id: &PoolId) -> Option<&PoolQuoteModelUpdate> {
        self.models.get(pool_id)
    }

    fn accepts(&self, pool_id: &PoolId, slot: u64, write_version: u64) -> bool {
        self.versions.get(pool_id).is_none_or(|existing| {
            existing.slot < slot
                || (existing.slot == slot && existing.write_version < write_version)
        })
    }

    fn upsert(&mut self, update: PoolQuoteModelUpdate) -> bool {
        let pool_id = PoolId(update.pool_id.clone());
        if !self.accepts(&pool_id, update.slot, update.write_version) {
            return false;
        }
        self.versions.insert(
            pool_id.clone(),
            LiveQuoteModelVersion {
                slot: update.slot,
                write_version: update.write_version,
            },
        );
        self.models.insert(pool_id, update);
        true
    }

    fn invalidate(&mut self, pool_id: &PoolId, slot: u64, write_version: u64) -> bool {
        if !self.accepts(pool_id, slot, write_version) {
            return false;
        }
        self.versions.insert(
            pool_id.clone(),
            LiveQuoteModelVersion {
                slot,
                write_version,
            },
        );
        self.models.remove(pool_id);
        true
    }
}

#[derive(Debug, Default)]
struct LiveExecutableStateStore {
    states: ExecutablePoolStateStore,
    concentrated_quote_models: LiveConcentratedQuoteModelStore,
}

impl LiveExecutableStateStore {
    fn get(&self, pool_id: &PoolId) -> Option<&ExecutablePoolState> {
        self.states.get(pool_id)
    }

    fn get_mut(&mut self, pool_id: &PoolId) -> Option<&mut ExecutablePoolState> {
        self.states.get_mut(pool_id)
    }

    fn concentrated_quote_model(&self, pool_id: &PoolId) -> Option<&PoolQuoteModelUpdate> {
        self.concentrated_quote_models.get(pool_id)
    }

    fn next_write_version(&self, pool_id: &PoolId) -> u64 {
        self.get(pool_id)
            .map(|state| state.write_version().saturating_add(1))
            .unwrap_or(1)
    }

    #[cfg(test)]
    fn upsert(&mut self, state: ExecutablePoolState) -> bool {
        self.states.upsert(state)
    }

    fn upsert_exact_reduction(&mut self, reduction: &ExactReduction) -> bool {
        let pool_id = reduction.next_state.pool_id().clone();
        let slot = reduction.next_state.last_update_slot();
        let write_version = reduction.next_state.write_version();
        if self.get(&pool_id).is_some_and(|existing| {
            existing.last_update_slot() > slot
                || (existing.last_update_slot() == slot
                    && existing.write_version() >= write_version)
        }) {
            return false;
        }
        if let Some(update) = &reduction.quote_model_update {
            if !self
                .concentrated_quote_models
                .accepts(&pool_id, update.slot, update.write_version)
            {
                return false;
            }
        }

        let state_applied = self.states.upsert(reduction.next_state.clone());
        if !state_applied {
            return false;
        }
        if let Some(update) = &reduction.quote_model_update {
            let _ = self.concentrated_quote_models.upsert(update.clone());
        }
        true
    }

    fn invalidate(&mut self, pool_id: &PoolId, slot: u64, write_version: u64) -> bool {
        let state_invalidated = self.states.invalidate(pool_id, slot, write_version);
        let quote_invalidated =
            self.concentrated_quote_models
                .invalidate(pool_id, slot, write_version);
        state_invalidated || quote_invalidated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcEntriesConfig {
    pub grpc_endpoint: String,
    pub buffer_capacity: usize,
    pub grpc_connect_timeout_ms: u64,
    pub reconnect_backoff_millis: u64,
    pub max_reconnect_backoff_millis: u64,
    pub idle_refresh_slot_lag: u64,
    pub price_impact_trigger_bps: u16,
    pub price_dislocation_trigger_bps: u16,
    pub max_repair_in_flight: usize,
    pub tracked_pools: Vec<TrackedPool>,
    pub lookup_table_keys: Vec<String>,
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
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (event_tx, event_rx) = mpsc::channel();
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
            account_batcher,
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
    fn token_pair(&self) -> (&str, &str) {
        match &self.kind {
            TrackedPoolKind::OrcaSimple(config) => (&config.token_mint_a, &config.token_mint_b),
            TrackedPoolKind::RaydiumSimple(config) => (&config.token_mint_a, &config.token_mint_b),
            TrackedPoolKind::OrcaWhirlpool(config) => (&config.token_mint_a, &config.token_mint_b),
            TrackedPoolKind::RaydiumClmm(config) => (&config.token_mint_a, &config.token_mint_b),
        }
    }

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
        observed_slot: u64,
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
        retry_kind: RepairRetryKind,
    },
    Dropped {
        pool_id: String,
        mode: SyncRequestMode,
        observed_slot: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairRetryKind {
    Failed,
    Transient,
    RateLimited,
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
    refresh_requested_observed_slot: Option<u64>,
    repair_pending: bool,
    repair_in_flight: bool,
    repair_requested_observed_slot: Option<u64>,
}

impl PoolSyncState {
    fn is_empty(&self) -> bool {
        !self.refresh_pending
            && !self.refresh_in_flight
            && self.refresh_deadline_slot.is_none()
            && self.refresh_requested_observed_slot.is_none()
            && !self.repair_pending
            && !self.repair_in_flight
            && self.repair_requested_observed_slot.is_none()
    }

    fn requested_observed_slot(&self, mode: SyncRequestMode) -> Option<u64> {
        match mode {
            SyncRequestMode::RefreshExact => self.refresh_requested_observed_slot,
            SyncRequestMode::RepairAfterInvalidation => self.repair_requested_observed_slot,
        }
    }
}

#[derive(Debug, Default)]
struct PoolSyncTracker {
    pools: HashMap<String, PoolSyncState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCompletionDisposition {
    Satisfied { observed_slot: u64 },
    Advanced { observed_slot: u64 },
    Unsatisfied { observed_slot: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncDropDisposition {
    Cleared { observed_slot: u64 },
    Requeue { observed_slot: u64 },
}

impl PoolSyncTracker {
    fn schedule_refresh(&mut self, pool_id: &str, observed_slot: u64) -> (u64, bool) {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        let deadline_slot = observed_slot.saturating_add(CLMM_REFRESH_GRACE_SLOTS);
        let enqueue = !entry.refresh_pending;
        entry.refresh_pending = true;
        entry.refresh_requested_observed_slot = Some(
            entry
                .refresh_requested_observed_slot
                .map(|current| current.max(observed_slot))
                .unwrap_or(observed_slot),
        );
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

    fn schedule_background_refresh(
        &mut self,
        pool_id: &str,
        requested_observed_slot: u64,
        deadline_reference_slot: u64,
    ) -> Option<u64> {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        if entry.refresh_pending
            || entry.refresh_in_flight
            || entry.repair_pending
            || entry.repair_in_flight
        {
            return None;
        }

        let deadline_slot = deadline_reference_slot.saturating_add(CLMM_REFRESH_GRACE_SLOTS);
        entry.refresh_pending = true;
        entry.refresh_in_flight = false;
        entry.refresh_requested_observed_slot = Some(requested_observed_slot);
        entry.refresh_deadline_slot = None;
        Some(deadline_slot)
    }

    fn mark_refresh_started(&mut self, pool_id: &str) {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        entry.refresh_pending = true;
        entry.refresh_in_flight = true;
    }

    fn mark_refresh_pending(&mut self, pool_id: &str) {
        if let Some(entry) = self.pools.get_mut(pool_id) {
            entry.refresh_pending = true;
            entry.refresh_in_flight = false;
        }
    }

    fn mark_refresh_failed(&mut self, pool_id: &str) {
        self.mark_refresh_pending(pool_id);
    }

    fn clear_refresh(&mut self, pool_id: &str) -> bool {
        let Some(entry) = self.pools.get_mut(pool_id) else {
            return false;
        };
        let was_pending = entry.refresh_pending || entry.refresh_deadline_slot.is_some();
        entry.refresh_pending = false;
        entry.refresh_in_flight = false;
        entry.refresh_deadline_slot = None;
        entry.refresh_requested_observed_slot = None;
        let remove = entry.is_empty();
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

    fn repair_requested(&mut self, pool_id: &str, observed_slot: u64) -> bool {
        let entry = self.pools.entry(pool_id.to_string()).or_default();
        entry.repair_requested_observed_slot = Some(
            entry
                .repair_requested_observed_slot
                .map(|current| current.max(observed_slot))
                .unwrap_or(observed_slot),
        );
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

    fn mark_repair_pending(&mut self, pool_id: &str) {
        if let Some(entry) = self.pools.get_mut(pool_id) {
            entry.repair_pending = true;
            entry.repair_in_flight = false;
        }
    }

    fn mark_repair_failed(&mut self, pool_id: &str) {
        self.mark_repair_pending(pool_id);
    }

    fn clear_repair(&mut self, pool_id: &str) -> bool {
        let Some(entry) = self.pools.get_mut(pool_id) else {
            return false;
        };
        let was_pending = entry.repair_pending || entry.repair_in_flight;
        entry.repair_pending = false;
        entry.repair_in_flight = false;
        entry.repair_requested_observed_slot = None;
        let remove = entry.is_empty();
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

    fn requested_observed_slot(&self, pool_id: &str, mode: SyncRequestMode) -> Option<u64> {
        self.pools
            .get(pool_id)
            .and_then(|entry| entry.requested_observed_slot(mode))
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
            .map(|mut tracker| tracker.repair_requested(&pool_id, 0))
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
    signature: Option<String>,
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

    fn reference_input_amount(&self) -> Option<u64> {
        let amount = match self {
            Self::OrcaSimpleSwap { amount_in, .. } => *amount_in,
            Self::RaydiumSimpleSwap {
                exact_out,
                amount_specified,
                min_output_amount,
                ..
            } => {
                if *exact_out {
                    (*amount_specified).max(*min_output_amount)
                } else {
                    *amount_specified
                }
            }
            Self::OrcaWhirlpoolSwap {
                amount,
                other_amount_threshold,
                exact_out,
                ..
            }
            | Self::RaydiumClmmSwap {
                amount,
                other_amount_threshold,
                exact_out,
                ..
            } => {
                if *exact_out {
                    (*amount).max(*other_amount_threshold)
                } else {
                    *amount
                }
            }
        };
        (amount > 0).then_some(amount)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReductionStatus {
    Probable,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactMutationOutcome {
    Applied(ExactReduction),
    RefreshRequired,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactReduction {
    next_state: ExecutablePoolState,
    quote_model_update: Option<PoolQuoteModelUpdate>,
    refresh_required: bool,
}

#[derive(Debug, Clone)]
struct PendingDestabilization {
    pool_id: String,
    venue: domain::PoolVenue,
    signature: Option<String>,
    baseline_snapshot: domain::PoolSnapshot,
    input_amount: u64,
    estimated_price_impact_bps: u16,
    estimated_price_dislocation_bps: u16,
    estimated_net_edge_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CrossPoolEdgeEstimate {
    dislocation_bps: u16,
    net_edge_bps: u16,
}

fn spawn_stream_worker(
    config: &GrpcEntriesConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    account_batcher: GetMultipleAccountsBatcher,
    lookup_table_cache: LookupTableCacheHandle,
    event_tx: Sender<NormalizedEvent>,
    repair_tx: mpsc::Sender<RepairRequest>,
    sequence: Arc<AtomicU64>,
    hooks: Arc<dyn LiveHooks>,
) {
    let grpc_endpoint = config.grpc_endpoint.clone();
    let connect_timeout = Duration::from_millis(config.grpc_connect_timeout_ms.max(1));
    let base_backoff = Duration::from_millis(config.reconnect_backoff_millis.max(1));
    let max_backoff = Duration::from_millis(config.max_reconnect_backoff_millis.max(1));
    let idle_refresh_slot_lag = config.idle_refresh_slot_lag;
    let price_impact_trigger_bps = config.price_impact_trigger_bps;
    let price_dislocation_trigger_bps = config.price_dislocation_trigger_bps;

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
                    shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(
                        endpoint,
                    )
                    .await;
                let Ok(mut client) = connect_result else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                };

                let subscribe_result = client
                    .subscribe_entries(shredstream::SubscribeEntriesRequest {})
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
                        &account_batcher,
                        &lookup_table_cache,
                        &event_tx,
                        &repair_tx,
                        &sequence,
                        idle_refresh_slot_lag,
                        price_impact_trigger_bps,
                        price_dislocation_trigger_bps,
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
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    account_batcher: &GetMultipleAccountsBatcher,
    lookup_table_cache: &LookupTableCacheHandle,
    event_tx: &Sender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    idle_refresh_slot_lag: u64,
    price_impact_trigger_bps: u16,
    price_dislocation_trigger_bps: u16,
    hooks: &dyn LiveHooks,
) {
    let Ok(entries) = bincode::deserialize::<Vec<LegacyEntry>>(payload) else {
        return;
    };

    let mut invalidated_pools = HashSet::new();
    let mut fetched_lookup_table_keys = HashSet::new();
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
            let Some(resolved) = resolve_transaction_with_dynamic_lookup_tables(
                &transaction,
                account_batcher,
                lookup_table_cache,
                &mut fetched_lookup_table_keys,
            ) else {
                continue;
            };
            let mut handled_pools = HashSet::new();
            let mut pending_destabilizations = HashMap::new();
            let mut observed_destabilization_pools = HashSet::new();

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
                    ReducerRolloutMode::Active => {
                        observed_destabilization_pools.insert(pool_id.clone());
                        accumulate_destabilization_candidate(
                            slot,
                            &resolved,
                            &tracked_pool,
                            &mutation,
                            executable_states,
                            &mut pending_destabilizations,
                        );
                        match apply_mutation_exact(
                            slot,
                            &tracked_pool,
                            &mutation,
                            executable_states,
                        ) {
                            ExactMutationOutcome::Applied(reduction) => {
                                publish_pool_snapshot_update(
                                    event_tx,
                                    sequence,
                                    slot,
                                    source_received_at,
                                    source_latency,
                                    &reduction.next_state,
                                );
                                if let Some(quote_model_update) = reduction.quote_model_update {
                                    publish_pool_quote_model_update(
                                        event_tx,
                                        sequence,
                                        slot,
                                        source_received_at,
                                        source_latency,
                                        quote_model_update,
                                    );
                                }
                                if reduction.refresh_required {
                                    schedule_background_refresh(
                                        &tracked_pool,
                                        slot,
                                        slot,
                                        sync_tracker,
                                        repair_tx,
                                        hooks,
                                        source_received_at,
                                    );
                                }
                            }
                            ExactMutationOutcome::RefreshRequired => schedule_exact_refresh(
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
                            ),
                            ExactMutationOutcome::Invalid if tracked_pool.uses_hybrid_refresh() => {
                                schedule_exact_refresh(
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
                        }
                    }
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

            for pool_id in observed_destabilization_pools {
                let Some(mut destabilization) = pending_destabilizations.remove(&pool_id) else {
                    hooks.publish_destabilization(LiveDestabilizationEvent {
                        pool_id,
                        observed_slot: slot,
                        kind: LiveDestabilizationKind::Unclassified,
                        input_amount: None,
                        estimated_price_impact_bps: None,
                        estimated_price_dislocation_bps: None,
                        estimated_net_edge_bps: None,
                        impact_floor_bps: price_impact_trigger_bps,
                        dislocation_trigger_bps: price_dislocation_trigger_bps,
                    });
                    continue;
                };
                if let Some(edge_estimate) = estimate_cross_pool_edge_bps(
                    registry,
                    executable_states,
                    slot,
                    &destabilization.pool_id,
                    destabilization.input_amount,
                ) {
                    destabilization.estimated_price_dislocation_bps = edge_estimate.dislocation_bps;
                    destabilization.estimated_net_edge_bps = edge_estimate.net_edge_bps;
                }
                hooks.publish_destabilization(LiveDestabilizationEvent {
                    pool_id: destabilization.pool_id.clone(),
                    observed_slot: slot,
                    kind: LiveDestabilizationKind::Candidate,
                    input_amount: Some(destabilization.input_amount),
                    estimated_price_impact_bps: Some(destabilization.estimated_price_impact_bps),
                    estimated_price_dislocation_bps: Some(
                        destabilization.estimated_price_dislocation_bps,
                    ),
                    estimated_net_edge_bps: Some(destabilization.estimated_net_edge_bps),
                    impact_floor_bps: price_impact_trigger_bps,
                    dislocation_trigger_bps: price_dislocation_trigger_bps,
                });
                if destabilization.estimated_price_impact_bps < price_impact_trigger_bps
                    || destabilization.estimated_net_edge_bps < price_dislocation_trigger_bps
                {
                    continue;
                }
                hooks.publish_destabilization(LiveDestabilizationEvent {
                    pool_id: destabilization.pool_id.clone(),
                    observed_slot: slot,
                    kind: LiveDestabilizationKind::Triggered,
                    input_amount: Some(destabilization.input_amount),
                    estimated_price_impact_bps: Some(destabilization.estimated_price_impact_bps),
                    estimated_price_dislocation_bps: Some(
                        destabilization.estimated_price_dislocation_bps,
                    ),
                    estimated_net_edge_bps: Some(destabilization.estimated_net_edge_bps),
                    impact_floor_bps: price_impact_trigger_bps,
                    dislocation_trigger_bps: price_dislocation_trigger_bps,
                });
                publish_destabilizing_transaction(
                    event_tx,
                    sequence,
                    slot,
                    source_received_at,
                    source_latency,
                    price_dislocation_trigger_bps,
                    destabilization,
                );
            }

            for tracked_pool in registry.touched_pools(&resolved.writable_account_keys) {
                if handled_pools.contains(&tracked_pool.pool_id) {
                    continue;
                }
                if tracked_pool.uses_hybrid_refresh() {
                    schedule_exact_refresh(
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
                    hooks,
                    &mut invalidated_pools,
                );
            }
        }
    }

    refresh_idle_tracked_pools(
        slot,
        registry,
        executable_states,
        sync_tracker,
        repair_tx,
        idle_refresh_slot_lag,
        hooks,
        source_received_at,
    );
}

fn resolve_transaction_with_dynamic_lookup_tables(
    transaction: &LegacyVersionedTransaction,
    account_batcher: &GetMultipleAccountsBatcher,
    lookup_table_cache: &LookupTableCacheHandle,
    fetched_lookup_table_keys: &mut HashSet<String>,
) -> Option<ResolvedTransaction> {
    let snapshot = lookup_table_cache.snapshot();
    if let Some(resolved) = resolve_transaction(transaction, &snapshot.tables_by_key) {
        return Some(resolved);
    }

    let missing_lookup_table_keys =
        unresolved_lookup_table_keys(transaction, &snapshot.tables_by_key);
    if missing_lookup_table_keys.is_empty() {
        return None;
    }

    let fetch_keys = missing_lookup_table_keys
        .into_iter()
        .filter(|account_key| fetched_lookup_table_keys.insert(account_key.clone()))
        .collect::<Vec<_>>();
    if fetch_keys.is_empty() {
        return None;
    }

    let (slot, fetched_tables) = fetch_lookup_table_snapshots(account_batcher, &fetch_keys).ok()?;
    if fetched_tables.is_empty() {
        return None;
    }
    lookup_table_cache.merge(slot, fetched_tables);
    let refreshed = lookup_table_cache.snapshot();
    resolve_transaction(transaction, &refreshed.tables_by_key)
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
                signature: transaction
                    .signatures
                    .first()
                    .map(|signature| signature.to_string()),
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
        signature: transaction
            .signatures
            .first()
            .map(|signature| signature.to_string()),
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

fn unresolved_lookup_table_keys(
    transaction: &LegacyVersionedTransaction,
    lookup_tables: &HashMap<String, LookupTableSnapshot>,
) -> Vec<String> {
    let LegacyVersionedMessage::V0(message) = &transaction.message else {
        return Vec::new();
    };
    message
        .address_table_lookups
        .iter()
        .map(|lookup| lookup.account_key.to_string())
        .filter(|account_key| !lookup_tables.contains_key(account_key))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
) -> ExactMutationOutcome {
    let Ok(mut store) = executable_states.write() else {
        return ExactMutationOutcome::Invalid;
    };
    let pool_id = PoolId(tracked_pool.pool_id.clone());
    let Some(current_state) = store.get(&pool_id).cloned() else {
        return ExactMutationOutcome::Invalid;
    };
    let current_quote_model = store.concentrated_quote_model(&pool_id).cloned();

    match reduce_mutation_exact(
        &current_state,
        current_quote_model.as_ref(),
        tracked_pool,
        mutation,
        slot,
    ) {
        ExactMutationOutcome::Applied(reduction) => {
            if !store.upsert_exact_reduction(&reduction) {
                return ExactMutationOutcome::Invalid;
            }
            ExactMutationOutcome::Applied(reduction)
        }
        other => other,
    }
}

fn apply_mutation_shadow(
    slot: u64,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
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
    current_quote_model: Option<&PoolQuoteModelUpdate>,
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
            .map(wrap_exact_reduction)
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
        (
            TrackedPoolKind::OrcaWhirlpool(config),
            PoolMutation::OrcaWhirlpoolSwap {
                amount,
                exact_out,
                a_to_b,
                ..
            },
            ExecutablePoolState::OrcaWhirlpool(state),
        ) => reduce_orca_whirlpool_swap(
            state,
            current_quote_model,
            config,
            *amount,
            *exact_out,
            *a_to_b,
            slot,
        ),
        (
            TrackedPoolKind::RaydiumClmm(config),
            PoolMutation::RaydiumClmmSwap {
                amount,
                exact_out,
                zero_for_one,
                ..
            },
            ExecutablePoolState::RaydiumClmm(state),
        ) => reduce_raydium_clmm_swap(
            state,
            current_quote_model,
            config,
            *amount,
            *exact_out,
            *zero_for_one,
            slot,
        ),
        _ => ExactMutationOutcome::Invalid,
    }
}

fn wrap_exact_reduction(next_state: ExecutablePoolState) -> ExactReduction {
    ExactReduction {
        next_state,
        quote_model_update: None,
        refresh_required: false,
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
        .map(wrap_exact_reduction)
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
        .map(wrap_exact_reduction)
        .map(ExactMutationOutcome::Applied)
        .unwrap_or(ExactMutationOutcome::Invalid)
    }
}

fn reduce_orca_whirlpool_swap(
    state: &ConcentratedLiquidityPoolState,
    current_quote_model: Option<&PoolQuoteModelUpdate>,
    config: &OrcaWhirlpoolTrackedConfig,
    amount: u64,
    exact_out: bool,
    a_to_b: bool,
    slot: u64,
) -> ExactMutationOutcome {
    reduce_concentrated_swap(
        state,
        current_quote_model,
        PoolVenue::OrcaWhirlpool,
        config.require_a_to_b,
        config.require_b_to_a,
        amount,
        exact_out,
        a_to_b,
        slot,
        |current_tick_index, direction_a_to_b, direction| {
            recenter_directional_quote_model(
                direction,
                current_tick_index,
                config.tick_spacing,
                ORCA_WHIRLPOOL_TICK_ARRAY_SIZE,
                direction_a_to_b,
            )
        },
    )
}

fn reduce_raydium_clmm_swap(
    state: &ConcentratedLiquidityPoolState,
    current_quote_model: Option<&PoolQuoteModelUpdate>,
    config: &RaydiumClmmTrackedConfig,
    amount: u64,
    exact_out: bool,
    zero_for_one: bool,
    slot: u64,
) -> ExactMutationOutcome {
    reduce_concentrated_swap(
        state,
        current_quote_model,
        PoolVenue::RaydiumClmm,
        config.require_zero_for_one,
        config.require_one_for_zero,
        amount,
        exact_out,
        zero_for_one,
        slot,
        |current_tick_index, direction_a_to_b, direction| {
            recenter_directional_quote_model(
                direction,
                current_tick_index,
                config.tick_spacing,
                RAYDIUM_CLMM_TICK_ARRAY_SIZE,
                direction_a_to_b,
            )
        },
    )
}

fn reduce_concentrated_swap<F>(
    state: &ConcentratedLiquidityPoolState,
    current_quote_model: Option<&PoolQuoteModelUpdate>,
    venue: PoolVenue,
    required_a_to_b: bool,
    required_b_to_a: bool,
    amount: u64,
    exact_out: bool,
    a_to_b: bool,
    slot: u64,
    recenter_direction: F,
) -> ExactMutationOutcome
where
    F: Fn(i32, bool, &DirectionalPoolQuoteModelUpdate) -> DirectionalPoolQuoteModelUpdate,
{
    let Some(current_quote_model) = current_quote_model else {
        return ExactMutationOutcome::RefreshRequired;
    };
    let domain_quote_model = to_domain_concentrated_quote_model(current_quote_model);
    let direction = if a_to_b {
        current_quote_model.a_to_b.as_ref()
    } else {
        current_quote_model.b_to_a.as_ref()
    };
    let Some(direction) = direction else {
        return ExactMutationOutcome::RefreshRequired;
    };
    if !direction_is_executable(Some(direction), current_quote_model.current_tick_index) {
        return ExactMutationOutcome::RefreshRequired;
    }
    let domain_direction = to_domain_directional_quote_model(direction);
    let swap_result = if exact_out {
        apply_concentrated_exact_output_update(
            venue,
            &domain_quote_model,
            &domain_direction,
            a_to_b,
            amount,
            state.fee_bps,
        )
    } else {
        apply_concentrated_exact_input_update(
            venue,
            &domain_quote_model,
            &domain_direction,
            a_to_b,
            amount,
            state.fee_bps,
        )
    };
    let swap_result = match swap_result {
        Ok(result) => result,
        Err(QuoteError::ConcentratedWindowExceeded | QuoteError::QuoteModelNotExecutable) => {
            return ExactMutationOutcome::RefreshRequired;
        }
        Err(
            QuoteError::ArithmeticOverflow
            | QuoteError::ArithmeticOverflowAt(_)
            | QuoteError::InvalidSnapshotLiquidity
            | QuoteError::InvalidSnapshotPrice
            | QuoteError::ExecutionCostNotConvertible
            | QuoteError::InvalidRouteShape
            | QuoteError::ZeroOutput { .. },
        ) => return ExactMutationOutcome::Invalid,
    };

    let next_a_to_b = required_a_to_b.then(|| {
        current_quote_model
            .a_to_b
            .as_ref()
            .map(|direction| recenter_direction(swap_result.current_tick_index, true, direction))
    });
    let next_b_to_a = required_b_to_a.then(|| {
        current_quote_model
            .b_to_a
            .as_ref()
            .map(|direction| recenter_direction(swap_result.current_tick_index, false, direction))
    });
    let next_a_to_b = next_a_to_b.flatten();
    let next_b_to_a = next_b_to_a.flatten();
    let executable = (!required_a_to_b
        || direction_is_executable(next_a_to_b.as_ref(), swap_result.current_tick_index))
        && (!required_b_to_a
            || direction_is_executable(next_b_to_a.as_ref(), swap_result.current_tick_index));
    let (loaded_tick_arrays, expected_tick_arrays) =
        quote_model_union(next_a_to_b.as_ref(), next_b_to_a.as_ref());
    let next_write_version = state.write_version.saturating_add(1);
    let next_state = concentrated_state_with_update(
        state,
        venue,
        swap_result,
        slot,
        next_write_version,
        executable,
        loaded_tick_arrays,
        expected_tick_arrays,
    );
    ExactMutationOutcome::Applied(ExactReduction {
        next_state,
        quote_model_update: Some(PoolQuoteModelUpdate {
            pool_id: state.pool_id.0.clone(),
            liquidity: swap_result.liquidity,
            sqrt_price_x64: swap_result.sqrt_price_x64,
            current_tick_index: swap_result.current_tick_index,
            tick_spacing: state.tick_spacing,
            required_a_to_b,
            required_b_to_a,
            a_to_b: next_a_to_b,
            b_to_a: next_b_to_a,
            slot,
            write_version: next_write_version,
        }),
        refresh_required: !executable,
    })
}

fn concentrated_state_with_update(
    state: &ConcentratedLiquidityPoolState,
    venue: PoolVenue,
    swap_result: ConcentratedSwapState,
    slot: u64,
    write_version: u64,
    executable: bool,
    loaded_tick_arrays: usize,
    expected_tick_arrays: usize,
) -> ExecutablePoolState {
    let next = ConcentratedLiquidityPoolState {
        pool_id: state.pool_id.clone(),
        venue,
        token_mint_a: state.token_mint_a.clone(),
        token_mint_b: state.token_mint_b.clone(),
        token_vault_a: state.token_vault_a.clone(),
        token_vault_b: state.token_vault_b.clone(),
        fee_bps: state.fee_bps,
        active_liquidity: swap_result.liquidity.min(u128::from(u64::MAX)) as u64,
        liquidity: swap_result.liquidity,
        sqrt_price_x64: swap_result.sqrt_price_x64,
        current_tick_index: swap_result.current_tick_index,
        tick_spacing: state.tick_spacing,
        loaded_tick_arrays,
        expected_tick_arrays,
        last_update_slot: slot,
        write_version,
        last_verified_slot: if executable {
            state.last_verified_slot.max(slot)
        } else {
            state.last_verified_slot
        },
        confidence: if executable {
            PoolConfidence::Executable
        } else {
            PoolConfidence::Verified
        },
        repair_pending: false,
    };
    match venue {
        PoolVenue::OrcaWhirlpool => ExecutablePoolState::OrcaWhirlpool(next),
        PoolVenue::RaydiumClmm => ExecutablePoolState::RaydiumClmm(next),
        _ => unreachable!("concentrated reducer only supports concentrated venues"),
    }
}

fn recenter_directional_quote_model(
    direction: &DirectionalPoolQuoteModelUpdate,
    current_tick_index: i32,
    tick_spacing: u16,
    tick_array_size: i32,
    a_to_b: bool,
) -> DirectionalPoolQuoteModelUpdate {
    let required_start_indexes = directional_required_start_indexes(
        current_tick_index,
        tick_spacing,
        tick_array_size,
        a_to_b,
    );
    let windows = direction
        .windows
        .iter()
        .filter(|window| required_start_indexes.contains(&window.start_tick_index))
        .cloned()
        .collect::<Vec<_>>();
    let initialized_ticks = direction
        .initialized_ticks
        .iter()
        .filter(|tick| {
            windows.iter().any(|window| {
                tick.tick_index >= window.start_tick_index
                    && tick.tick_index <= window.end_tick_index
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    DirectionalPoolQuoteModelUpdate {
        loaded_tick_arrays: windows.len(),
        expected_tick_arrays: 3,
        complete: windows.len() == 3,
        windows,
        initialized_ticks,
    }
}

fn directional_required_start_indexes(
    current_tick_index: i32,
    tick_spacing: u16,
    tick_array_size: i32,
    a_to_b: bool,
) -> BTreeSet<i32> {
    let offsets = if a_to_b { [0, -1, -2] } else { [0, 1, 2] };
    offsets
        .into_iter()
        .map(|offset| {
            tick_array_start_index(current_tick_index, tick_spacing, tick_array_size, offset)
        })
        .collect()
}

fn to_domain_concentrated_quote_model(update: &PoolQuoteModelUpdate) -> ConcentratedQuoteModel {
    ConcentratedQuoteModel {
        pool_id: PoolId(update.pool_id.clone()),
        liquidity: update.liquidity,
        sqrt_price_x64: update.sqrt_price_x64,
        current_tick_index: update.current_tick_index,
        tick_spacing: update.tick_spacing,
        required_a_to_b: update.required_a_to_b,
        required_b_to_a: update.required_b_to_a,
        a_to_b: update
            .a_to_b
            .as_ref()
            .map(to_domain_directional_quote_model),
        b_to_a: update
            .b_to_a
            .as_ref()
            .map(to_domain_directional_quote_model),
        last_update_slot: update.slot,
        write_version: update.write_version,
    }
}

fn to_domain_directional_quote_model(
    direction: &DirectionalPoolQuoteModelUpdate,
) -> DirectionalConcentratedQuoteModel {
    DirectionalConcentratedQuoteModel {
        loaded_tick_arrays: direction.loaded_tick_arrays,
        expected_tick_arrays: direction.expected_tick_arrays,
        complete: direction.complete,
        windows: direction
            .windows
            .iter()
            .map(|window| domain::TickArrayWindow {
                start_tick_index: window.start_tick_index,
                end_tick_index: window.end_tick_index,
                initialized_tick_count: window.initialized_tick_count,
            })
            .collect(),
        initialized_ticks: direction
            .initialized_ticks
            .iter()
            .map(|tick| domain::InitializedTick {
                tick_index: tick.tick_index,
                liquidity_net: tick.liquidity_net,
                liquidity_gross: tick.liquidity_gross,
            })
            .collect(),
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
    next.last_verified_slot = next.last_verified_slot.max(slot);
    next.confidence = constant_product_confidence_for_venue(venue);
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
    next.last_verified_slot = next.last_verified_slot.max(slot);
    next.confidence = constant_product_confidence_for_venue(venue);
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

fn constant_product_confidence_for_venue(venue: PoolVenue) -> PoolConfidence {
    match venue {
        PoolVenue::OrcaSimplePool => PoolConfidence::Executable,
        // Raydium "simple" pools are hybrid AMM/OpenBook markets. We currently
        // hydrate them from the two AMM vault balances only, so the state is
        // structurally verified but not safe to treat as executable for quotes.
        PoolVenue::RaydiumSimplePool => PoolConfidence::Verified,
        PoolVenue::OrcaWhirlpool | PoolVenue::RaydiumClmm => {
            unreachable!("constant product reducer only supports constant product venues")
        }
    }
}

fn constant_product_requires_repair(state: &ConstantProductPoolState) -> bool {
    match state.venue {
        PoolVenue::OrcaSimplePool => !state.confidence.is_executable() || state.repair_pending,
        PoolVenue::RaydiumSimplePool => !state.confidence.is_verified() || state.repair_pending,
        PoolVenue::OrcaWhirlpool | PoolVenue::RaydiumClmm => {
            unreachable!("constant product reducer only supports constant product venues")
        }
    }
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
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &Sender<NormalizedEvent>,
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
        .map(|mut tracker| tracker.repair_requested(&tracked_pool.pool_id, slot))
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

    let invalidation_event = if let Ok(mut store) = executable_states.write() {
        invalidate_live_snapshot(
            tracked_pool,
            slot,
            &mut store,
            source_received_at,
            source_latency,
            sequence,
        )
    } else {
        None
    };
    if let Some(event) = invalidation_event {
        send_lossless_event(event_tx, event);
    }
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
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &Sender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    hooks: &dyn LiveHooks,
) {
    let (deadline_slot, should_enqueue) = match sync_tracker.lock() {
        Ok(mut tracker) => tracker.schedule_refresh(&tracked_pool.pool_id, slot),
        Err(_) => return,
    };
    if should_enqueue {
        let invalidation_event = if let Ok(mut store) = executable_states.write() {
            invalidate_live_snapshot(
                tracked_pool,
                slot,
                &mut store,
                source_received_at,
                source_latency,
                sequence,
            )
        } else {
            None
        };
        if let Some(event) = invalidation_event {
            send_lossless_event(event_tx, event);
        }
    }
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

fn schedule_background_refresh(
    tracked_pool: &TrackedPool,
    slot: u64,
    refresh_observed_slot: u64,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    hooks: &dyn LiveHooks,
    occurred_at: SystemTime,
) {
    if hooks.pool_is_blocked_from_repair(&tracked_pool.pool_id, slot) {
        return;
    }

    let Some(deadline_slot) = sync_tracker.lock().ok().and_then(|mut tracker| {
        tracker.schedule_background_refresh(&tracked_pool.pool_id, refresh_observed_slot, slot)
    }) else {
        return;
    };

    hooks.publish_repair(LiveRepairEvent {
        pool_id: tracked_pool.pool_id.clone(),
        kind: LiveRepairEventKind::RefreshScheduled { deadline_slot },
        occurred_at,
    });
    hooks.on_repair_transition(
        &tracked_pool.pool_id,
        LiveRepairTransition::RefreshScheduled,
        slot,
    );
    let _ = repair_tx.send(RepairRequest {
        pool_id: tracked_pool.pool_id.clone(),
        mode: SyncRequestMode::RefreshExact,
        priority: RepairPriority::Retry,
        observed_slot: refresh_observed_slot,
    });
}

fn invalidate_live_snapshot_by_pool_id(
    pool_id: &str,
    slot: u64,
    store: &mut LiveExecutableStateStore,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    sequence: &Arc<AtomicU64>,
) -> Option<NormalizedEvent> {
    let pool_id = PoolId(pool_id.to_string());
    let next_write_version = store
        .get(&pool_id)
        .map(|state| state.write_version().saturating_add(1))
        .unwrap_or(1);
    if !store.invalidate(&pool_id, slot, next_write_version) {
        return None;
    }

    Some(event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        crate::MarketEvent::PoolInvalidation(PoolInvalidation {
            pool_id: pool_id.0.clone(),
            slot,
            write_version: next_write_version,
        }),
    ))
}

fn invalidate_live_snapshot_after_advanced_request(
    pool_id: &str,
    observed_slot: u64,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    event_tx: &Sender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
) {
    let invalidation_event = if let Ok(mut store) = executable_states.write() {
        invalidate_live_snapshot_by_pool_id(
            pool_id,
            observed_slot,
            &mut store,
            SystemTime::now(),
            None,
            sequence,
        )
    } else {
        None
    };
    if let Some(event) = invalidation_event {
        send_lossless_event(event_tx, event);
    }
}

fn invalidate_live_snapshot(
    tracked_pool: &TrackedPool,
    slot: u64,
    store: &mut LiveExecutableStateStore,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    sequence: &Arc<AtomicU64>,
) -> Option<NormalizedEvent> {
    invalidate_live_snapshot_by_pool_id(
        &tracked_pool.pool_id,
        slot,
        store,
        source_received_at,
        source_latency,
        sequence,
    )
}

fn expire_exact_refreshes(
    slot: u64,
    registry: &TrackedPoolRegistry,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &Sender<NormalizedEvent>,
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

fn refresh_idle_tracked_pools(
    slot: u64,
    registry: &TrackedPoolRegistry,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    idle_refresh_slot_lag: u64,
    hooks: &dyn LiveHooks,
    source_received_at: SystemTime,
) {
    let stale_pools = match executable_states.read() {
        Ok(store) => registry
            .pool_ids()
            .into_iter()
            .filter(|pool_id| {
                store.get(&PoolId(pool_id.clone())).is_some_and(|state| {
                    slot.saturating_sub(state.last_update_slot()) >= idle_refresh_slot_lag
                })
            })
            .filter_map(|pool_id| {
                store
                    .get(&PoolId(pool_id.clone()))
                    .map(|state| (pool_id, state.last_update_slot().saturating_add(1)))
            })
            .collect::<Vec<_>>(),
        Err(_) => return,
    };

    for (pool_id, refresh_observed_slot) in stale_pools {
        let Some(tracked_pool) = registry.tracked_pool(&pool_id) else {
            continue;
        };
        schedule_background_refresh(
            &tracked_pool,
            slot,
            refresh_observed_slot,
            sync_tracker,
            repair_tx,
            hooks,
            source_received_at,
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

fn clear_sync_request(tracker: &mut PoolSyncTracker, pool_id: &str, mode: SyncRequestMode) -> bool {
    match mode {
        SyncRequestMode::RefreshExact => tracker.clear_refresh(pool_id),
        SyncRequestMode::RepairAfterInvalidation => tracker.clear_repair(pool_id),
    }
}

fn mark_sync_request_pending(tracker: &mut PoolSyncTracker, pool_id: &str, mode: SyncRequestMode) {
    match mode {
        SyncRequestMode::RefreshExact => tracker.mark_refresh_pending(pool_id),
        SyncRequestMode::RepairAfterInvalidation => tracker.mark_repair_pending(pool_id),
    }
}

fn mark_sync_request_failed(tracker: &mut PoolSyncTracker, pool_id: &str, mode: SyncRequestMode) {
    match mode {
        SyncRequestMode::RefreshExact => tracker.mark_refresh_failed(pool_id),
        SyncRequestMode::RepairAfterInvalidation => tracker.mark_repair_failed(pool_id),
    }
}

fn finalize_completed_sync_request(
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    pool_id: &str,
    mode: SyncRequestMode,
    fallback_observed_slot: u64,
) -> SyncCompletionDisposition {
    let Ok(mut tracker) = sync_tracker.lock() else {
        return SyncCompletionDisposition::Unsatisfied {
            observed_slot: fallback_observed_slot,
        };
    };
    let current_observed_slot = tracker
        .requested_observed_slot(pool_id, mode)
        .map(|requested| requested.max(fallback_observed_slot))
        .unwrap_or(fallback_observed_slot);
    let satisfied = executable_states
        .read()
        .ok()
        .and_then(|store| {
            store
                .get(&PoolId(pool_id.to_string()))
                .map(|state| state_satisfies_request(state, current_observed_slot))
        })
        .unwrap_or(false);
    if satisfied {
        clear_sync_request(&mut tracker, pool_id, mode);
        SyncCompletionDisposition::Satisfied {
            observed_slot: current_observed_slot,
        }
    } else if current_observed_slot > fallback_observed_slot {
        mark_sync_request_pending(&mut tracker, pool_id, mode);
        SyncCompletionDisposition::Advanced {
            observed_slot: current_observed_slot,
        }
    } else {
        SyncCompletionDisposition::Unsatisfied {
            observed_slot: current_observed_slot,
        }
    }
}

fn finalize_dropped_sync_request(
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    pool_id: &str,
    mode: SyncRequestMode,
    fallback_observed_slot: u64,
) -> SyncDropDisposition {
    let Ok(mut tracker) = sync_tracker.lock() else {
        return SyncDropDisposition::Cleared {
            observed_slot: fallback_observed_slot,
        };
    };
    let current_observed_slot = tracker
        .requested_observed_slot(pool_id, mode)
        .map(|requested| requested.max(fallback_observed_slot))
        .unwrap_or(fallback_observed_slot);
    if current_observed_slot > fallback_observed_slot {
        mark_sync_request_pending(&mut tracker, pool_id, mode);
        SyncDropDisposition::Requeue {
            observed_slot: current_observed_slot,
        }
    } else {
        clear_sync_request(&mut tracker, pool_id, mode);
        SyncDropDisposition::Cleared {
            observed_slot: current_observed_slot,
        }
    }
}

fn requested_observed_slot(
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    pool_id: &str,
    mode: SyncRequestMode,
    fallback_observed_slot: u64,
) -> u64 {
    sync_tracker
        .lock()
        .ok()
        .and_then(|tracker| tracker.requested_observed_slot(pool_id, mode))
        .map(|requested| requested.max(fallback_observed_slot))
        .unwrap_or(fallback_observed_slot)
}

fn publish_slot_boundary(
    event_tx: &Sender<NormalizedEvent>,
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
    send_lossless_event(event_tx, event);
}

fn publish_pool_snapshot_update(
    event_tx: &Sender<NormalizedEvent>,
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
            venue: state.venue(),
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
    send_lossless_event(event_tx, event);
}

fn publish_pool_quote_model_update(
    event_tx: &Sender<NormalizedEvent>,
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
    send_lossless_event(event_tx, event);
}

fn current_snapshot_for_destabilization(
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    pool_id: &str,
    slot: u64,
) -> Option<domain::PoolSnapshot> {
    let pool_id = PoolId(pool_id.to_string());
    let store = executable_states.read().ok()?;
    let state = store.get(&pool_id)?;
    Some(state.to_pool_snapshot(slot, 0))
}

fn accumulate_destabilization_candidate(
    slot: u64,
    resolved: &ResolvedTransaction,
    tracked_pool: &TrackedPool,
    mutation: &PoolMutation,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    pending: &mut HashMap<String, PendingDestabilization>,
) {
    let Some(reference_input_amount) = mutation.reference_input_amount() else {
        return;
    };
    let pool_id = tracked_pool.pool_id.clone();
    if !pending.contains_key(&pool_id) {
        let Some(snapshot) =
            current_snapshot_for_destabilization(executable_states, &pool_id, slot)
        else {
            return;
        };
        pending.insert(
            pool_id.clone(),
            PendingDestabilization {
                pool_id: pool_id.clone(),
                venue: snapshot.venue.unwrap_or(domain::PoolVenue::OrcaSimplePool),
                signature: resolved.signature.clone(),
                baseline_snapshot: snapshot,
                input_amount: 0,
                estimated_price_impact_bps: 0,
                estimated_price_dislocation_bps: 0,
                estimated_net_edge_bps: 0,
            },
        );
    }
    let Some(candidate) = pending.get_mut(&pool_id) else {
        return;
    };
    candidate.input_amount = candidate
        .input_amount
        .saturating_add(reference_input_amount);
    candidate.estimated_price_impact_bps = candidate
        .baseline_snapshot
        .estimated_slippage_bps(candidate.input_amount);
}

fn estimate_cross_pool_edge_bps(
    registry: &TrackedPoolRegistry,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    slot: u64,
    pool_id: &str,
    input_amount: u64,
) -> Option<CrossPoolEdgeEstimate> {
    let tracked_pool = registry.tracked_pool(pool_id)?;
    let (token_mint_a, token_mint_b) = tracked_pool.token_pair();
    let target_snapshot = current_snapshot_for_destabilization(executable_states, pool_id, slot)?;
    if !target_snapshot.is_executable() {
        return None;
    }
    let target_price_bps =
        normalize_price_bps(&target_snapshot, token_mint_a, token_mint_b)?.max(1);
    let target_slippage_bps = target_snapshot.estimated_slippage_bps(input_amount);
    let mut best_estimate: Option<CrossPoolEdgeEstimate> = None;
    for peer in registry.pools_by_id.values() {
        if peer.pool_id == pool_id {
            continue;
        }
        let (peer_mint_a, peer_mint_b) = peer.token_pair();
        let same_pair = (peer_mint_a == token_mint_a && peer_mint_b == token_mint_b)
            || (peer_mint_a == token_mint_b && peer_mint_b == token_mint_a);
        if !same_pair {
            continue;
        }
        let Some(peer_snapshot) =
            current_snapshot_for_destabilization(executable_states, &peer.pool_id, slot)
        else {
            continue;
        };
        if !peer_snapshot.is_executable() {
            continue;
        }
        let Some(peer_price_bps) = normalize_price_bps(&peer_snapshot, token_mint_a, token_mint_b)
        else {
            continue;
        };
        let dislocation_bps = relative_price_dislocation_bps(target_price_bps, peer_price_bps);
        let peer_slippage_bps = peer_snapshot.estimated_slippage_bps(input_amount);
        let total_cost_bps = u32::from(target_snapshot.fee_bps)
            .saturating_add(u32::from(peer_snapshot.fee_bps))
            .saturating_add(u32::from(target_slippage_bps))
            .saturating_add(u32::from(peer_slippage_bps));
        let net_edge_bps =
            dislocation_bps.saturating_sub(total_cost_bps.min(u32::from(u16::MAX)) as u16);
        let candidate = CrossPoolEdgeEstimate {
            dislocation_bps,
            net_edge_bps,
        };
        best_estimate = Some(match best_estimate {
            Some(current)
                if current.net_edge_bps > candidate.net_edge_bps
                    || (current.net_edge_bps == candidate.net_edge_bps
                        && current.dislocation_bps >= candidate.dislocation_bps) =>
            {
                current
            }
            _ => candidate,
        });
    }
    best_estimate
}

fn normalize_price_bps(
    snapshot: &domain::PoolSnapshot,
    token_mint_a: &str,
    token_mint_b: &str,
) -> Option<u64> {
    let price_bps = snapshot.price_bps.max(1);
    if snapshot.token_mint_a == token_mint_a && snapshot.token_mint_b == token_mint_b {
        Some(price_bps)
    } else if snapshot.token_mint_a == token_mint_b && snapshot.token_mint_b == token_mint_a {
        let numerator = 10_000u128.saturating_mul(10_000u128);
        Some(
            numerator
                .saturating_add(u128::from(price_bps).saturating_sub(1))
                .saturating_div(u128::from(price_bps))
                .min(u128::from(u64::MAX)) as u64,
        )
    } else {
        None
    }
}

fn relative_price_dislocation_bps(left_price_bps: u64, right_price_bps: u64) -> u16 {
    let low = left_price_bps.min(right_price_bps).max(1);
    let high = left_price_bps.max(right_price_bps);
    let spread = u128::from(high.saturating_sub(low))
        .saturating_mul(10_000)
        .saturating_div(u128::from(low));
    spread.min(u128::from(u16::MAX)) as u16
}

fn publish_destabilizing_transaction(
    event_tx: &Sender<NormalizedEvent>,
    sequence: &Arc<AtomicU64>,
    slot: u64,
    source_received_at: SystemTime,
    source_latency: Option<Duration>,
    trigger_threshold_bps: u16,
    destabilization: PendingDestabilization,
) {
    let event = event_with_timing(
        next_sequence(sequence),
        slot,
        source_received_at,
        source_latency,
        crate::MarketEvent::DestabilizingTransaction(DestabilizingTransaction {
            pool_id: destabilization.pool_id,
            venue: destabilization.venue,
            slot,
            signature: destabilization.signature,
            input_amount: destabilization.input_amount,
            estimated_price_impact_bps: destabilization.estimated_price_impact_bps,
            trigger_threshold_bps,
        }),
    );
    send_lossless_event(event_tx, event);
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

fn fetch_lookup_table_snapshots(
    account_batcher: &GetMultipleAccountsBatcher,
    account_keys: &[String],
) -> Result<(u64, Vec<LookupTableSnapshot>), RpcError> {
    let fetched = account_batcher.fetch(account_keys)?;
    let slot = fetched.slot;
    let tables = account_keys
        .iter()
        .zip(fetched.ordered_values(account_keys))
        .filter_map(|(account_key, account)| decode_lookup_table(account_key, account, slot))
        .collect::<Vec<_>>();
    Ok((slot, tables))
}

fn spawn_repair_worker(
    config: &GrpcEntriesConfig,
    registry: Arc<TrackedPoolRegistry>,
    executable_states: Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: Arc<Mutex<PoolSyncTracker>>,
    event_tx: Sender<NormalizedEvent>,
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
            let mut rpc_rate_limit_backoff = RpcRateLimitBackoff::default();
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
                let build_result = build_repair_result(
                    &account_batcher,
                    &tracked_pool,
                    &executable_states,
                    job.observed_slot,
                );
                let build_result = match build_result {
                    Ok(build_result) => build_result,
                    Err(retry_kind) => {
                        if retry_kind == RepairRetryKind::RateLimited {
                            let delay = rpc_rate_limit_backoff.on_rate_limited();
                            if !delay.is_zero() {
                                thread::sleep(delay);
                            }
                        }
                        let _ = result_tx.send(RepairJobResult::Retry {
                            pool_id: job.pool_id,
                            mode: job.mode,
                            attempt: job.attempt,
                            observed_slot: job.observed_slot,
                            retry_kind,
                        });
                        continue;
                    }
                };

                rpc_rate_limit_backoff.on_success();
                let _ = result_tx.send(RepairJobResult::Success {
                    pool_id: job.pool_id,
                    mode: job.mode,
                    slot: build_result.next_state.last_update_slot(),
                    observed_slot: job.observed_slot,
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
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    hooks: &dyn LiveHooks,
    pending: &mut VecDeque<String>,
    pending_modes: &mut HashMap<String, RepairRequest>,
    _in_flight: &HashMap<String, SyncRequestMode>,
) {
    if !request_still_needed(&request, executable_states, sync_tracker, hooks) {
        if request.mode == SyncRequestMode::RepairAfterInvalidation {
            if let Ok(mut tracker) = sync_tracker.lock() {
                tracker.clear_repair(&request.pool_id);
            }
        }
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
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
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
        let observed_slot =
            requested_observed_slot(sync_tracker, &pool_id, request.mode, request.observed_slot);
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
            observed_slot,
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
                observed_slot,
            })
            .is_err()
        {
            break;
        }
    }
}

fn drain_repair_results(
    result_rx: &mpsc::Receiver<RepairJobResult>,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    sync_tracker: &Arc<Mutex<PoolSyncTracker>>,
    event_tx: &Sender<NormalizedEvent>,
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
                observed_slot,
                next_state,
                quote_model_update,
                attempt,
                latency,
            } => {
                in_flight.remove(&pool_id);
                let latest_observed_slot =
                    requested_observed_slot(sync_tracker, &pool_id, mode, observed_slot);
                let result_satisfies_job = state_satisfies_request(&next_state, observed_slot);
                let result_satisfies_latest =
                    state_satisfies_request(&next_state, latest_observed_slot);
                let mut published = false;
                if result_satisfies_latest {
                    if let Ok(mut store) = executable_states.write() {
                        published = store.upsert_exact_reduction(&ExactReduction {
                            next_state: next_state.clone(),
                            quote_model_update: quote_model_update.clone(),
                            refresh_required: false,
                        });
                    }
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
                match finalize_completed_sync_request(
                    executable_states,
                    sync_tracker,
                    &pool_id,
                    mode,
                    observed_slot,
                ) {
                    SyncCompletionDisposition::Satisfied { observed_slot } => {
                        attempts.remove(&(pool_id.clone(), mode));
                        hooks.publish_repair(LiveRepairEvent {
                            pool_id: pool_id.clone(),
                            kind: match mode {
                                SyncRequestMode::RefreshExact => {
                                    LiveRepairEventKind::RefreshAttemptSucceeded {
                                        latency_ms: latency.as_millis().min(u128::from(u64::MAX))
                                            as u64,
                                    }
                                }
                                SyncRequestMode::RepairAfterInvalidation => {
                                    LiveRepairEventKind::RepairAttemptSucceeded {
                                        latency_ms: latency.as_millis().min(u128::from(u64::MAX))
                                            as u64,
                                    }
                                }
                            },
                            occurred_at: SystemTime::now(),
                        });
                        hooks.on_repair_transition(
                            &pool_id,
                            match mode {
                                SyncRequestMode::RefreshExact => {
                                    LiveRepairTransition::RefreshSucceeded
                                }
                                SyncRequestMode::RepairAfterInvalidation => {
                                    LiveRepairTransition::RepairSucceeded
                                }
                            },
                            observed_slot.max(slot),
                        );
                    }
                    SyncCompletionDisposition::Advanced { observed_slot } => {
                        if result_satisfies_job {
                            invalidate_live_snapshot_after_advanced_request(
                                &pool_id,
                                observed_slot,
                                executable_states,
                                event_tx,
                                sequence,
                            );
                            let _ = retry_tx.send(RepairRequest {
                                pool_id,
                                mode,
                                priority: RepairPriority::Immediate,
                                observed_slot,
                            });
                        } else {
                            if let Ok(mut tracker) = sync_tracker.lock() {
                                mark_sync_request_failed(&mut tracker, &pool_id, mode);
                            }
                            hooks.publish_repair(LiveRepairEvent {
                                pool_id: pool_id.clone(),
                                kind: match mode {
                                    SyncRequestMode::RefreshExact => {
                                        LiveRepairEventKind::RefreshAttemptFailed
                                    }
                                    SyncRequestMode::RepairAfterInvalidation => {
                                        LiveRepairEventKind::RepairAttemptFailed
                                    }
                                },
                                occurred_at: SystemTime::now(),
                            });
                            hooks.on_repair_transition(
                                &pool_id,
                                match mode {
                                    SyncRequestMode::RefreshExact => {
                                        LiveRepairTransition::RefreshFailed
                                    }
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
                    }
                    SyncCompletionDisposition::Unsatisfied { observed_slot } => {
                        if let Ok(mut tracker) = sync_tracker.lock() {
                            mark_sync_request_failed(&mut tracker, &pool_id, mode);
                        }
                        hooks.publish_repair(LiveRepairEvent {
                            pool_id: pool_id.clone(),
                            kind: match mode {
                                SyncRequestMode::RefreshExact => {
                                    LiveRepairEventKind::RefreshAttemptFailed
                                }
                                SyncRequestMode::RepairAfterInvalidation => {
                                    LiveRepairEventKind::RepairAttemptFailed
                                }
                            },
                            occurred_at: SystemTime::now(),
                        });
                        hooks.on_repair_transition(
                            &pool_id,
                            match mode {
                                SyncRequestMode::RefreshExact => {
                                    LiveRepairTransition::RefreshFailed
                                }
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
                }
            }
            RepairJobResult::Retry {
                pool_id,
                mode,
                attempt,
                observed_slot,
                retry_kind,
            } => {
                in_flight.remove(&pool_id);
                let latest_observed_slot =
                    requested_observed_slot(sync_tracker, &pool_id, mode, observed_slot);
                if matches!(
                    retry_kind,
                    RepairRetryKind::Transient | RepairRetryKind::RateLimited
                ) {
                    if mode == SyncRequestMode::RefreshExact {
                        if let Ok(mut tracker) = sync_tracker.lock() {
                            tracker.mark_refresh_pending(&pool_id);
                        }
                    } else if let Ok(mut tracker) = sync_tracker.lock() {
                        tracker.mark_repair_pending(&pool_id);
                    }
                } else {
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
                            SyncRequestMode::RefreshExact => {
                                LiveRepairEventKind::RefreshAttemptFailed
                            }
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
                }
                let retry_tx = retry_tx.clone();
                let retry_delay = match retry_kind {
                    RepairRetryKind::Failed => repair_backoff(attempt),
                    RepairRetryKind::Transient | RepairRetryKind::RateLimited => {
                        transient_repair_backoff(attempt)
                    }
                };
                thread::spawn(move || {
                    thread::sleep(retry_delay);
                    let _ = retry_tx.send(RepairRequest {
                        pool_id,
                        mode,
                        priority: RepairPriority::Retry,
                        observed_slot: latest_observed_slot,
                    });
                });
            }
            RepairJobResult::Dropped {
                pool_id,
                mode,
                observed_slot,
            } => {
                in_flight.remove(&pool_id);
                match finalize_dropped_sync_request(sync_tracker, &pool_id, mode, observed_slot) {
                    SyncDropDisposition::Cleared { observed_slot } => {
                        attempts.remove(&(pool_id.clone(), mode));
                        hooks.on_repair_transition(
                            &pool_id,
                            match mode {
                                SyncRequestMode::RefreshExact => {
                                    LiveRepairTransition::RefreshCleared
                                }
                                SyncRequestMode::RepairAfterInvalidation => {
                                    LiveRepairTransition::RepairFailed
                                }
                            },
                            observed_slot,
                        );
                    }
                    SyncDropDisposition::Requeue { observed_slot } => {
                        invalidate_live_snapshot_after_advanced_request(
                            &pool_id,
                            observed_slot,
                            executable_states,
                            event_tx,
                            sequence,
                        );
                        let _ = retry_tx.send(RepairRequest {
                            pool_id,
                            mode,
                            priority: RepairPriority::Immediate,
                            observed_slot,
                        });
                    }
                }
            }
        }
    }
}

fn request_still_needed(
    request: &RepairRequest,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
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

fn transient_repair_backoff(attempt: u32) -> Duration {
    match attempt {
        0 | 1 => Duration::from_secs(1),
        2 => Duration::from_secs(2),
        3 => Duration::from_secs(5),
        4 => Duration::from_secs(10),
        _ => Duration::from_secs(15),
    }
}

fn pool_requires_repair(
    pool_id: &str,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
) -> bool {
    let Ok(store) = executable_states.read() else {
        return true;
    };
    let Some(state) = store.get(&PoolId(pool_id.into())) else {
        return true;
    };

    match state {
        ExecutablePoolState::OrcaSimplePool(state)
        | ExecutablePoolState::RaydiumSimplePool(state) => constant_product_requires_repair(state),
        ExecutablePoolState::OrcaWhirlpool(state) | ExecutablePoolState::RaydiumClmm(state) => {
            !state.confidence.is_executable() || state.repair_pending
        }
    }
}

fn build_repair_result(
    account_batcher: &GetMultipleAccountsBatcher,
    tracked_pool: &TrackedPool,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
    observed_slot: u64,
) -> Result<RepairBuildResult, RepairRetryKind> {
    let (slot, accounts) = match fetch_repair_accounts(account_batcher, tracked_pool, observed_slot)
    {
        Ok(result) => result,
        Err(error) => {
            eprintln!("repair fetch {} failed: {error}", tracked_pool.pool_id);
            return Err(if error.is_rate_limited() {
                RepairRetryKind::RateLimited
            } else if error.is_transient() {
                RepairRetryKind::Transient
            } else {
                RepairRetryKind::Failed
            });
        }
    };
    build_executable_state_from_accounts(tracked_pool, &accounts, slot, executable_states)
        .ok_or_else(|| {
            eprintln!(
                "repair build {} produced no state from fetched accounts",
                tracked_pool.pool_id
            );
            RepairRetryKind::Failed
        })
}

fn fetch_repair_accounts(
    account_batcher: &GetMultipleAccountsBatcher,
    tracked_pool: &TrackedPool,
    observed_slot: u64,
) -> Result<(u64, HashMap<String, RpcAccountValue>), RpcError> {
    let repair_keys = tracked_pool.repair_account_keys();
    let (slot, accounts) = fetch_accounts_by_key(account_batcher, &repair_keys, observed_slot)?;

    let concentrated_keys = required_quote_model_keys(tracked_pool, &accounts)?;
    if concentrated_keys.is_empty() {
        return Ok((slot, accounts));
    }

    let unified_keys = repair_keys
        .into_iter()
        .chain(concentrated_keys)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    // The second fetch only needs to stay at or above the event watermark.
    // Requiring the exact slot from the probe fetch can trigger transient
    // "minimum context slot" failures on public RPCs even when a coherent
    // snapshot is otherwise available.
    let (refetch_slot, refetched_accounts) =
        fetch_accounts_by_key(account_batcher, &unified_keys, observed_slot)?;
    ensure_quote_model_keys_covered(tracked_pool, &refetched_accounts, &unified_keys)?;
    Ok((refetch_slot, refetched_accounts))
}

fn required_quote_model_keys(
    tracked_pool: &TrackedPool,
    accounts: &HashMap<String, RpcAccountValue>,
) -> Result<Vec<String>, RpcError> {
    match &tracked_pool.kind {
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
            required_orca_tick_array_keys(config, current_tick_index)
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
            required_raydium_tick_array_keys(config, current_tick_index)
        }
        _ => Ok(Vec::new()),
    }
}

fn ensure_quote_model_keys_covered(
    tracked_pool: &TrackedPool,
    accounts: &HashMap<String, RpcAccountValue>,
    fetched_keys: &[String],
) -> Result<(), RpcError> {
    let fetched_keys = fetched_keys.iter().cloned().collect::<BTreeSet<_>>();
    let missing = required_quote_model_keys(tracked_pool, accounts)?
        .into_iter()
        .filter(|key| !fetched_keys.contains(key))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(RpcError::DecodeFailed {
        method: "getMultipleAccounts".into(),
        detail: format!(
            "concentrated repair context moved beyond fetched tick arrays: {}",
            missing.join(",")
        ),
    })
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

fn direction_covers_tick(
    direction: &DirectionalPoolQuoteModelUpdate,
    current_tick_index: i32,
) -> bool {
    direction.windows.iter().any(|window| {
        current_tick_index >= window.start_tick_index && current_tick_index <= window.end_tick_index
    })
}

fn direction_is_executable(
    direction: Option<&DirectionalPoolQuoteModelUpdate>,
    current_tick_index: i32,
) -> bool {
    direction
        .map(|direction| {
            direction.loaded_tick_arrays > 0
                && !direction.windows.is_empty()
                && direction_covers_tick(direction, current_tick_index)
        })
        .unwrap_or(false)
}

fn build_executable_state_from_accounts(
    tracked_pool: &TrackedPool,
    accounts: &HashMap<String, RpcAccountValue>,
    slot: u64,
    executable_states: &Arc<RwLock<LiveExecutableStateStore>>,
) -> Option<RepairBuildResult> {
    let next_write_version = executable_states
        .read()
        .ok()
        .map(|store| store.next_write_version(&PoolId(tracked_pool.pool_id.clone())))
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
                    confidence: constant_product_confidence_for_venue(PoolVenue::RaydiumSimplePool),
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
                || direction_is_executable(a_to_b.as_ref(), current_tick_index))
                && (!config.require_b_to_a
                    || direction_is_executable(b_to_a.as_ref(), current_tick_index));
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
                || direction_is_executable(zero_for_one.as_ref(), current_tick_index))
                && (!config.require_one_for_zero
                    || direction_is_executable(one_for_zero.as_ref(), current_tick_index));
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
    min_context_slot: u64,
) -> Result<(u64, HashMap<String, RpcAccountValue>), RpcError> {
    let fetched =
        match account_batcher.fetch_with_min_context_slot(account_keys, Some(min_context_slot)) {
            Ok(fetched) => fetched,
            Err(error) if error.is_min_context_slot_not_reached() => {
                account_batcher.fetch(account_keys)?
            }
            Err(error) => return Err(error),
        };
    Ok((fetched.slot, fetched.present_accounts()))
}

fn state_satisfies_request(state: &ExecutablePoolState, observed_slot: u64) -> bool {
    let requires_repair = match state {
        ExecutablePoolState::OrcaSimplePool(state)
        | ExecutablePoolState::RaydiumSimplePool(state) => constant_product_requires_repair(state),
        ExecutablePoolState::OrcaWhirlpool(state) | ExecutablePoolState::RaydiumClmm(state) => {
            !state.confidence.is_executable() || state.repair_pending
        }
    };
    state.last_update_slot() >= observed_slot && !requires_repair
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
    let lane = crate::events::lane_for_payload(&payload);
    let normalized_at = SystemTime::now();
    NormalizedEvent {
        payload,
        source: crate::SourceMetadata {
            source: EventSourceKind::ShredStream,
            sequence,
            observed_slot,
        },
        latency: crate::LatencyMetadata {
            source_received_at,
            normalized_at,
            source_latency,
            router_received_at: normalized_at,
            state_published_at: None,
            trigger_blocked_at: None,
            lane,
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

fn send_lossless_event(event_tx: &Sender<NormalizedEvent>, event: NormalizedEvent) {
    let _ = event_tx.send(event);
}

fn next_sequence(sequence: &AtomicU64) -> u64 {
    sequence.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap, HashSet},
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex, RwLock,
            atomic::AtomicU64,
            mpsc::{self, TryRecvError},
        },
        time::{Duration, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use domain::{
        ConcentratedLiquidityPoolState, ConstantProductPoolState, DirectionalPoolQuoteModelUpdate,
        ExecutablePoolState, InitializedTickUpdate, PoolConfidence, PoolId, PoolQuoteModelUpdate,
        PoolVenue, TickArrayWindowUpdate,
    };
    use prost_types::Timestamp;
    use serde_json::{Value, json};
    use solana_address_lookup_table_interface::{
        program as alt_program,
        state::{LOOKUP_TABLE_META_SIZE, LookupTableMeta, ProgramState},
    };
    use solana_sdk::pubkey::Pubkey;

    use super::{
        CompiledInstruction, ExactMutationOutcome, ExactReduction, GrpcEntriesConfig,
        LegacyVersionedMessage, LegacyVersionedTransaction, LiveExecutableStateStore,
        NoopLiveHooks, ORCA_SWAP_INSTRUCTION_TAG, ORCA_SWAP_V2_DISCRIMINATOR_NAME,
        ORCA_TICK_ARRAY_ACCOUNT_MIN_LEN, ORCA_TICK_ARRAY_ACCOUNT_NAME,
        ORCA_TWO_HOP_SWAP_V2_DISCRIMINATOR_NAME, ORCA_WHIRLPOOL_ACCOUNT_NAME,
        OrcaSimpleTrackedConfig, OrcaWhirlpoolTrackedConfig, PoolMutation, PoolSyncTracker,
        RAYDIUM_CLMM_ACCOUNT_NAME, RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME, RAYDIUM_SWAP_BASE_OUT_TAG,
        RAYDIUM_TICK_ARRAY_ACCOUNT_MIN_LEN, RAYDIUM_TICK_ARRAY_ACCOUNT_NAME,
        RaydiumClmmTrackedConfig, RaydiumSimpleTrackedConfig, ReducerRolloutMode, RepairJobResult,
        RepairPriority, RepairRetryKind, ResolvedInstruction, RpcAccountValue,
        SyncCompletionDisposition, SyncDropDisposition, SyncRequestMode, TrackedPool,
        TrackedPoolKind, TrackedPoolRegistry, anchor_account_discriminator,
        anchor_instruction_discriminator, apply_mutation_exact, associated_token_address,
        build_executable_state_from_accounts, build_repair_result,
        constant_product_amount_in_for_output, constant_product_amount_out, drain_repair_results,
        finalize_completed_sync_request, finalize_dropped_sync_request,
        invalidate_live_snapshot_after_advanced_request, parse_legacy_pubkey,
        reduce_orca_simple_swap, reduce_raydium_simple_swap, refresh_idle_tracked_pools,
        required_orca_tick_array_keys, required_raydium_tick_array_keys, resolve_transaction,
        resolve_transaction_with_dynamic_lookup_tables, schedule_exact_refresh,
        seed_initial_repairs, state_satisfies_request, timestamp_to_system_time,
    };
    use crate::account_batcher::{GetMultipleAccountsBatcher, LookupTableCacheHandle};

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

    fn orca_whirlpool_account(
        program_id: &str,
        fee_rate: u16,
        liquidity: u128,
        sqrt_price_x64: u128,
        current_tick_index: i32,
    ) -> RpcAccountValue {
        let mut raw = vec![0u8; 245];
        raw[..8].copy_from_slice(&anchor_account_discriminator(ORCA_WHIRLPOOL_ACCOUNT_NAME));
        write_u16(&mut raw, 45, fee_rate);
        write_u128(&mut raw, 49, liquidity);
        write_u128(&mut raw, 65, sqrt_price_x64);
        write_i32(&mut raw, 81, current_tick_index);
        rpc_account_with_owner(raw, program_id)
    }

    fn orca_tick_array_account(program_id: &str, start_tick_index: i32) -> RpcAccountValue {
        let mut raw = vec![0u8; ORCA_TICK_ARRAY_ACCOUNT_MIN_LEN];
        raw[..8].copy_from_slice(&anchor_account_discriminator(ORCA_TICK_ARRAY_ACCOUNT_NAME));
        write_i32(&mut raw, 8, start_tick_index);
        rpc_account_with_owner(raw, program_id)
    }

    fn raydium_tick_array_account(program_id: &str, start_tick_index: i32) -> RpcAccountValue {
        let mut raw = vec![0u8; RAYDIUM_TICK_ARRAY_ACCOUNT_MIN_LEN];
        raw[..8].copy_from_slice(&anchor_account_discriminator(
            RAYDIUM_TICK_ARRAY_ACCOUNT_NAME,
        ));
        write_i32(&mut raw, 40, start_tick_index);
        rpc_account_with_owner(raw, program_id)
    }

    fn lookup_table_account(addresses: Vec<Pubkey>, last_extended_slot: u64) -> RpcAccountValue {
        let mut raw = vec![0u8; LOOKUP_TABLE_META_SIZE];
        bincode::serialize_into(
            &mut raw[..],
            &ProgramState::LookupTable(LookupTableMeta {
                last_extended_slot,
                ..LookupTableMeta::default()
            }),
        )
        .expect("serialize lookup table metadata");
        for address in addresses {
            raw.extend_from_slice(address.as_ref());
        }
        rpc_account_with_owner(raw, &alt_program::id().to_string())
    }

    fn multiple_accounts_response(
        slot: u64,
        keys: &[String],
        accounts: &HashMap<String, RpcAccountValue>,
    ) -> String {
        let values = keys
            .iter()
            .map(|key| {
                accounts.get(key).cloned().map(|account| {
                    json!({
                        "lamports": account.lamports,
                        "owner": account.owner,
                        "data": [account.data.0, account.data.1],
                    })
                })
            })
            .collect::<Vec<_>>();
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "context": { "slot": slot },
                "value": values
            }
        })
        .to_string()
    }

    fn spawn_mock_rpc_server<F>(handler: F) -> String
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock rpc server");
        let address = listener.local_addr().expect("mock rpc address");
        let handler = Arc::new(handler);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    continue;
                };
                let body = read_http_body(&mut stream);
                let response_body = (handler.as_ref())(&body);
                write_http_response(&mut stream, &response_body);
            }
        });

        format!("http://{address}")
    }

    fn read_http_body(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        let mut header_end = None;
        let mut content_length = 0usize;

        loop {
            let bytes_read = stream.read(&mut buffer).expect("read mock request");
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);

            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':').and_then(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                        })
                        .unwrap_or(0);
                }
            }

            if let Some(end) = header_end {
                if request.len() >= end + content_length {
                    let body = &request[end..end + content_length];
                    return String::from_utf8_lossy(body).into_owned();
                }
            }
        }

        String::new()
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
    }

    fn constant_product_state(
        pool_id: &str,
        slot: u64,
        write_version: u64,
        confidence: PoolConfidence,
        repair_pending: bool,
    ) -> ExecutablePoolState {
        ExecutablePoolState::OrcaSimplePool(ConstantProductPoolState {
            pool_id: PoolId(pool_id.into()),
            venue: PoolVenue::OrcaSimplePool,
            token_mint_a: "mint-a".into(),
            token_mint_b: "mint-b".into(),
            token_vault_a: "vault-a".into(),
            token_vault_b: "vault-b".into(),
            reserve_a: 1_000_000,
            reserve_b: 1_000_000,
            fee_bps: 30,
            last_update_slot: slot,
            write_version,
            last_verified_slot: slot,
            confidence,
            repair_pending,
        })
    }

    fn sample_registry() -> TrackedPoolRegistry {
        TrackedPoolRegistry::from_grpc_config(&GrpcEntriesConfig {
            grpc_endpoint: "http://localhost".into(),
            buffer_capacity: 64,
            grpc_connect_timeout_ms: 100,
            reconnect_backoff_millis: 100,
            max_reconnect_backoff_millis: 1_000,
            idle_refresh_slot_lag: 32,
            price_impact_trigger_bps: 5,
            price_dislocation_trigger_bps: 5,
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
        let ExecutablePoolState::RaydiumSimplePool(next) = next.next_state else {
            panic!("expected raydium simple state");
        };
        assert_eq!(next.reserve_a, 1_100_000_000);
        assert!(next.reserve_b < 100_000_000);
        assert_eq!(next.write_version, 3);
        assert_eq!(next.confidence, PoolConfidence::Verified);
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
        let ExecutablePoolState::RaydiumSimplePool(next) = next.next_state else {
            panic!("expected raydium simple state");
        };
        assert_eq!(next.reserve_b, 91_000_000);
        assert!(next.reserve_a > 1_000_000_000);
        assert_eq!(next.write_version, 3);
        assert_eq!(next.confidence, PoolConfidence::Verified);
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
    fn repair_builds_raydium_simple_as_verified_state() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumSimple(RaydiumSimpleTrackedConfig {
                program_id: "raydium-program".into(),
                token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                amm_pool: "amm".into(),
                token_vault_a: "vault-a".into(),
                token_vault_b: "vault-b".into(),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                fee_bps: 25,
            }),
        };
        let accounts = HashMap::from([
            (
                "vault-a".into(),
                rpc_account_with_owner(
                    {
                        let mut raw = vec![0u8; 72];
                        write_u64(&mut raw, 64, 1_000_000);
                        raw
                    },
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                ),
            ),
            (
                "vault-b".into(),
                rpc_account_with_owner(
                    {
                        let mut raw = vec![0u8; 72];
                        write_u64(&mut raw, 64, 2_000_000);
                        raw
                    },
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                ),
            ),
        ]);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

        let next = build_executable_state_from_accounts(&tracked_pool, &accounts, 88, &states)
            .expect("repair should rebuild the pool state");

        let ExecutablePoolState::RaydiumSimplePool(state) = next.next_state else {
            panic!("expected raydium simple state");
        };
        assert_eq!(state.confidence, PoolConfidence::Verified);
        assert!(!state.repair_pending);
        assert_eq!(state.last_verified_slot, 88);
        assert!(state_satisfies_request(
            &ExecutablePoolState::RaydiumSimplePool(state),
            88,
        ));
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
    fn resolve_transaction_fetches_missing_lookup_table_from_rpc() {
        let table_account = Pubkey::new_from_array([9u8; 32]).to_string();
        let loaded_address = Pubkey::new_from_array([7u8; 32]);
        let requested = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let endpoint = spawn_mock_rpc_server({
            let requested = Arc::clone(&requested);
            let table_account = table_account.clone();
            move |body| {
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                requested.lock().expect("requested keys").push(keys.clone());

                let accounts = HashMap::from([(
                    table_account.clone(),
                    lookup_table_account(vec![loaded_address], 54),
                )]);
                multiple_accounts_response(55, &keys, &accounts)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let cache = LookupTableCacheHandle::default();
        let mut fetched_lookup_table_keys = HashSet::new();
        let transaction = LegacyVersionedTransaction {
            signatures: Vec::new(),
            message: LegacyVersionedMessage::V0(solana_message_legacy::v0::Message {
                header: solana_message_legacy::MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![
                    parse_legacy_pubkey("11111111111111111111111111111112").unwrap(),
                    parse_legacy_pubkey("11111111111111111111111111111111").unwrap(),
                ],
                recent_blockhash: Default::default(),
                instructions: vec![CompiledInstruction {
                    program_id_index: 1,
                    accounts: vec![2],
                    data: vec![],
                }],
                address_table_lookups: vec![solana_message_legacy::v0::MessageAddressTableLookup {
                    account_key: parse_legacy_pubkey(&table_account).unwrap(),
                    writable_indexes: vec![],
                    readonly_indexes: vec![0],
                }],
            }),
        };

        assert!(resolve_transaction(&transaction, &HashMap::new()).is_none());

        let resolved = resolve_transaction_with_dynamic_lookup_tables(
            &transaction,
            &batcher,
            &cache,
            &mut fetched_lookup_table_keys,
        )
        .expect("v0 transaction should resolve after ALT hydration");

        assert_eq!(
            requested.lock().expect("requested keys").as_slice(),
            &[vec![table_account]]
        );
        assert_eq!(
            cache
                .snapshot()
                .tables_by_key
                .get(&Pubkey::new_from_array([9u8; 32]).to_string())
                .expect("lookup table snapshot")
                .addresses,
            vec![loaded_address.to_string()]
        );
        assert_eq!(resolved.account_keys.len(), 3);
        assert_eq!(resolved.account_keys[2], loaded_address.to_string());
        assert_eq!(
            resolved.instructions[0].accounts,
            vec![loaded_address.to_string()]
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
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
            "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
            rpc_account_with_owner(raw, "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),
        )]);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

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
    fn repair_builds_partial_orca_whirlpool_as_executable_state() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked_pool.kind else {
            panic!("expected whirlpool config");
        };
        let start_tick_index = domain::tick_array_start_index(
            12,
            config.tick_spacing,
            domain::ORCA_WHIRLPOOL_TICK_ARRAY_SIZE,
            0,
        );
        let tick_array_key = required_orca_tick_array_keys(config, 12)
            .expect("required whirlpool tick arrays")
            .into_iter()
            .next()
            .expect("first whirlpool tick array");
        let accounts = HashMap::from([
            (
                config.whirlpool.clone(),
                orca_whirlpool_account(&config.program_id, 400, 500_000, 1u128 << 64, 12),
            ),
            (
                tick_array_key,
                orca_tick_array_account(&config.program_id, start_tick_index),
            ),
        ]);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

        let next = build_executable_state_from_accounts(&tracked_pool, &accounts, 77, &states)
            .expect("repair should rebuild the pool state");

        let ExecutablePoolState::OrcaWhirlpool(state) = next.next_state else {
            panic!("expected whirlpool state");
        };
        let quote_model = next.quote_model_update.expect("quote model update");
        let a_to_b = quote_model.a_to_b.expect("a_to_b quote model");
        assert_eq!(state.confidence, PoolConfidence::Executable);
        assert_eq!(state.loaded_tick_arrays, 1);
        assert_eq!(state.expected_tick_arrays, 3);
        assert_eq!(a_to_b.loaded_tick_arrays, 1);
        assert!(!a_to_b.complete);
        assert_eq!(a_to_b.windows.len(), 1);
    }

    #[test]
    fn concentrated_repair_rebuilds_from_unified_second_fetch_slot() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked_pool.kind else {
            panic!("expected whirlpool config");
        };
        let required_keys =
            required_orca_tick_array_keys(config, 12).expect("required whirlpool tick arrays");
        let requested = Arc::new(Mutex::new(Vec::<(Vec<String>, Option<u64>)>::new()));
        let endpoint = spawn_mock_rpc_server({
            let requested = Arc::clone(&requested);
            let whirlpool = config.whirlpool.clone();
            let program_id = config.program_id.clone();
            let required_keys = required_keys.clone();
            move |body| {
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                let min_context_slot = payload["params"][1]["minContextSlot"].as_u64();
                requested
                    .lock()
                    .expect("requested calls")
                    .push((keys.clone(), min_context_slot));

                let (slot, whirlpool_account) = if keys.len() == 1 {
                    (
                        100,
                        orca_whirlpool_account(&program_id, 400, 500_000, 1u128 << 64, 12),
                    )
                } else {
                    (
                        101,
                        orca_whirlpool_account(&program_id, 400, 700_000, 1u128 << 65, 12),
                    )
                };

                let mut accounts = HashMap::from([(whirlpool.clone(), whirlpool_account)]);
                for (index, key) in required_keys.iter().enumerate() {
                    accounts.insert(
                        key.clone(),
                        orca_tick_array_account(&program_id, index as i32),
                    );
                }

                multiple_accounts_response(slot, &keys, &accounts)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

        let next = build_repair_result(&batcher, &tracked_pool, &states, 90)
            .expect("repair should rebuild from unified second fetch");

        let ExecutablePoolState::OrcaWhirlpool(state) = next.next_state else {
            panic!("expected whirlpool state");
        };
        let quote_model = next.quote_model_update.expect("quote model update");
        assert_eq!(state.last_update_slot, 101);
        assert_eq!(state.last_verified_slot, 101);
        assert_eq!(state.liquidity, 700_000);
        assert_eq!(state.sqrt_price_x64, 1u128 << 65);
        assert_eq!(quote_model.slot, 101);
        assert_eq!(quote_model.liquidity, 700_000);
        assert_eq!(quote_model.sqrt_price_x64, 1u128 << 65);

        let requested = requested.lock().expect("requested calls");
        assert_eq!(requested.len(), 2);
        assert_eq!(requested[0].1, Some(90));
        assert_eq!(requested[1].1, Some(90));
        assert_eq!(
            requested[1].0.iter().cloned().collect::<BTreeSet<_>>(),
            tracked_pool
                .repair_account_keys()
                .into_iter()
                .chain(required_keys)
                .collect()
        );
    }

    #[test]
    fn concentrated_repair_retries_when_second_fetch_moves_beyond_initial_tick_plan() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked_pool.kind else {
            panic!("expected whirlpool config");
        };
        let initial_required_keys =
            required_orca_tick_array_keys(config, 12).expect("initial tick arrays");
        let moved_required_keys =
            required_orca_tick_array_keys(config, 100_000).expect("moved tick arrays");
        assert!(
            moved_required_keys
                .iter()
                .any(|key| !initial_required_keys.contains(key))
        );

        let requested = Arc::new(Mutex::new(Vec::<(Vec<String>, Option<u64>)>::new()));
        let endpoint = spawn_mock_rpc_server({
            let requested = Arc::clone(&requested);
            let whirlpool = config.whirlpool.clone();
            let program_id = config.program_id.clone();
            let initial_required_keys = initial_required_keys.clone();
            move |body| {
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                let min_context_slot = payload["params"][1]["minContextSlot"].as_u64();
                requested
                    .lock()
                    .expect("requested calls")
                    .push((keys.clone(), min_context_slot));

                let (slot, current_tick_index) = if keys.len() == 1 {
                    (100, 12)
                } else {
                    (101, 100_000)
                };
                let mut accounts = HashMap::from([(
                    whirlpool.clone(),
                    orca_whirlpool_account(
                        &program_id,
                        400,
                        700_000,
                        1u128 << 65,
                        current_tick_index,
                    ),
                )]);
                for (index, key) in initial_required_keys.iter().enumerate() {
                    accounts.insert(
                        key.clone(),
                        orca_tick_array_account(&program_id, index as i32),
                    );
                }

                multiple_accounts_response(slot, &keys, &accounts)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

        assert!(matches!(
            build_repair_result(&batcher, &tracked_pool, &states, 90),
            Err(RepairRetryKind::Failed)
        ));

        let requested = requested.lock().expect("requested calls");
        assert_eq!(requested.len(), 2);
        assert_eq!(requested[0].1, Some(90));
        assert_eq!(requested[1].1, Some(90));
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
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

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
    fn repair_builds_partial_raydium_clmm_as_executable_state() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "CAMMCzo5YL8w4VFF8KVHrK22GGUQprz5nBQqkN7rKCh5".into(),
                pool_state: "So11111111111111111111111111111111111111112".into(),
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
        let TrackedPoolKind::RaydiumClmm(config) = &tracked_pool.kind else {
            panic!("expected raydium clmm config");
        };
        let start_tick_index = domain::tick_array_start_index(
            -8,
            config.tick_spacing,
            domain::RAYDIUM_CLMM_TICK_ARRAY_SIZE,
            0,
        );
        let tick_array_key = required_raydium_tick_array_keys(config, -8)
            .expect("required raydium tick arrays")
            .into_iter()
            .next()
            .expect("first raydium tick array");
        let accounts = HashMap::from([
            (
                config.pool_state.clone(),
                rpc_account_with_owner(
                    {
                        let mut raw = vec![0u8; 273];
                        raw[..8].copy_from_slice(&anchor_account_discriminator(
                            RAYDIUM_CLMM_ACCOUNT_NAME,
                        ));
                        write_u128(&mut raw, 237, 750_000);
                        write_u128(&mut raw, 253, 1u128 << 64);
                        write_i32(&mut raw, 269, -8);
                        raw
                    },
                    &config.program_id,
                ),
            ),
            (
                tick_array_key,
                raydium_tick_array_account(&config.program_id, start_tick_index),
            ),
        ]);
        let states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));

        let next = build_executable_state_from_accounts(&tracked_pool, &accounts, 88, &states)
            .expect("repair should rebuild the pool state");

        let ExecutablePoolState::RaydiumClmm(state) = next.next_state else {
            panic!("expected raydium clmm state");
        };
        let quote_model = next.quote_model_update.expect("quote model update");
        let zero_for_one = quote_model.a_to_b.expect("zero_for_one quote model");
        assert_eq!(state.confidence, PoolConfidence::Executable);
        assert_eq!(state.loaded_tick_arrays, 1);
        assert_eq!(state.expected_tick_arrays, 3);
        assert_eq!(zero_for_one.loaded_tick_arrays, 1);
        assert!(!zero_for_one.complete);
        assert_eq!(zero_for_one.windows.len(), 1);
    }

    #[test]
    fn orca_whirlpool_active_reducer_applies_locally_before_background_refresh() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked_pool.kind else {
            panic!("expected whirlpool config");
        };
        let live_store = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        let initial_sqrt_price = 1u128 << 64;
        live_store
            .write()
            .expect("live store")
            .upsert_exact_reduction(&ExactReduction {
                next_state: ExecutablePoolState::OrcaWhirlpool(ConcentratedLiquidityPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::OrcaWhirlpool,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    fee_bps: config.fee_bps,
                    active_liquidity: 1_000_000,
                    liquidity: 1_000_000,
                    sqrt_price_x64: initial_sqrt_price,
                    current_tick_index: 0,
                    tick_spacing: config.tick_spacing,
                    loaded_tick_arrays: 1,
                    expected_tick_arrays: 3,
                    last_update_slot: 77,
                    write_version: 1,
                    last_verified_slot: 77,
                    confidence: PoolConfidence::Executable,
                    repair_pending: false,
                }),
                quote_model_update: Some(PoolQuoteModelUpdate {
                    pool_id: tracked_pool.pool_id.clone(),
                    liquidity: 1_000_000,
                    sqrt_price_x64: initial_sqrt_price,
                    current_tick_index: 0,
                    tick_spacing: config.tick_spacing,
                    required_a_to_b: true,
                    required_b_to_a: false,
                    a_to_b: Some(DirectionalPoolQuoteModelUpdate {
                        loaded_tick_arrays: 1,
                        expected_tick_arrays: 3,
                        complete: false,
                        windows: vec![TickArrayWindowUpdate {
                            start_tick_index: -5632,
                            end_tick_index: 0,
                            initialized_tick_count: 1,
                        }],
                        initialized_ticks: vec![InitializedTickUpdate {
                            tick_index: 0,
                            liquidity_net: 100_000,
                            liquidity_gross: 100_000,
                        }],
                    }),
                    b_to_a: None,
                    slot: 77,
                    write_version: 1,
                }),
                refresh_required: false,
            });

        let outcome = apply_mutation_exact(
            88,
            &tracked_pool,
            &PoolMutation::OrcaWhirlpoolSwap {
                pool_id: tracked_pool.pool_id.clone(),
                amount: 1_000,
                other_amount_threshold: 1,
                exact_out: false,
                a_to_b: true,
            },
            &live_store,
        );

        let ExactMutationOutcome::Applied(reduction) = outcome else {
            panic!("expected local whirlpool reduction, got {outcome:?}");
        };
        assert!(reduction.refresh_required);
        let Some(quote_model_update) = reduction.quote_model_update.as_ref() else {
            panic!("expected local quote model update");
        };
        assert_ne!(quote_model_update.sqrt_price_x64, initial_sqrt_price);
        let ExecutablePoolState::OrcaWhirlpool(next_state) = &reduction.next_state else {
            panic!("expected whirlpool state");
        };
        assert_eq!(next_state.last_update_slot, 88);
        assert_eq!(next_state.confidence, PoolConfidence::Verified);

        let store = live_store.read().expect("live store");
        let stored_state = store
            .get(&PoolId(tracked_pool.pool_id.clone()))
            .expect("stored whirlpool state");
        assert_eq!(stored_state.last_update_slot(), 88);
        let stored_quote_model = store
            .concentrated_quote_model(&PoolId(tracked_pool.pool_id.clone()))
            .expect("stored whirlpool quote model");
        assert_eq!(stored_quote_model.slot, 88);
        assert_eq!(
            stored_quote_model.sqrt_price_x64,
            quote_model_update.sqrt_price_x64
        );
    }

    #[test]
    fn orca_whirlpool_exact_reducer_refreshes_when_current_tick_leaves_loaded_windows() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::OrcaWhirlpool(OrcaWhirlpoolTrackedConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".into(),
                whirlpool: "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2".into(),
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
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked_pool.kind else {
            panic!("expected whirlpool config");
        };
        let live_store = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        live_store
            .write()
            .expect("live store")
            .upsert_exact_reduction(&ExactReduction {
                next_state: ExecutablePoolState::OrcaWhirlpool(ConcentratedLiquidityPoolState {
                    pool_id: PoolId(tracked_pool.pool_id.clone()),
                    venue: PoolVenue::OrcaWhirlpool,
                    token_mint_a: config.token_mint_a.clone(),
                    token_mint_b: config.token_mint_b.clone(),
                    token_vault_a: config.token_vault_a.clone(),
                    token_vault_b: config.token_vault_b.clone(),
                    fee_bps: config.fee_bps,
                    active_liquidity: 1_000_000,
                    liquidity: 1_000_000,
                    sqrt_price_x64: 1u128 << 64,
                    current_tick_index: 512,
                    tick_spacing: config.tick_spacing,
                    loaded_tick_arrays: 1,
                    expected_tick_arrays: 3,
                    last_update_slot: 77,
                    write_version: 1,
                    last_verified_slot: 77,
                    confidence: PoolConfidence::Executable,
                    repair_pending: false,
                }),
                quote_model_update: Some(PoolQuoteModelUpdate {
                    pool_id: tracked_pool.pool_id.clone(),
                    liquidity: 1_000_000,
                    sqrt_price_x64: 1u128 << 64,
                    current_tick_index: 512,
                    tick_spacing: config.tick_spacing,
                    required_a_to_b: true,
                    required_b_to_a: false,
                    a_to_b: Some(DirectionalPoolQuoteModelUpdate {
                        loaded_tick_arrays: 1,
                        expected_tick_arrays: 3,
                        complete: false,
                        windows: vec![TickArrayWindowUpdate {
                            start_tick_index: -352,
                            end_tick_index: 0,
                            initialized_tick_count: 0,
                        }],
                        initialized_ticks: Vec::new(),
                    }),
                    b_to_a: None,
                    slot: 77,
                    write_version: 1,
                }),
                refresh_required: false,
            });

        let outcome = apply_mutation_exact(
            88,
            &tracked_pool,
            &PoolMutation::OrcaWhirlpoolSwap {
                pool_id: tracked_pool.pool_id.clone(),
                amount: 1_000,
                other_amount_threshold: 1,
                exact_out: false,
                a_to_b: true,
            },
            &live_store,
        );

        assert!(matches!(outcome, ExactMutationOutcome::RefreshRequired));
    }

    #[test]
    fn raydium_clmm_active_reducer_supports_local_exact_out() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-ray".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
            kind: TrackedPoolKind::RaydiumClmm(RaydiumClmmTrackedConfig {
                program_id: "CAMMCzo5YL8w4VFF8KVHrK22GGUQprz5nBQqkN7rKCh5".into(),
                pool_state: "So11111111111111111111111111111111111111112".into(),
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
        let TrackedPoolKind::RaydiumClmm(config) = &tracked_pool.kind else {
            panic!("expected raydium clmm config");
        };
        let start_tick_index = domain::tick_array_start_index(
            -8,
            config.tick_spacing,
            domain::RAYDIUM_CLMM_TICK_ARRAY_SIZE,
            0,
        );
        let tick_array_key = required_raydium_tick_array_keys(config, -8)
            .expect("required raydium tick arrays")
            .into_iter()
            .next()
            .expect("first raydium tick array");
        let accounts = HashMap::from([
            (
                config.pool_state.clone(),
                rpc_account_with_owner(
                    {
                        let mut raw = vec![0u8; 273];
                        raw[..8].copy_from_slice(&anchor_account_discriminator(
                            RAYDIUM_CLMM_ACCOUNT_NAME,
                        ));
                        write_u128(&mut raw, 237, 750_000);
                        write_u128(&mut raw, 253, 1u128 << 64);
                        write_i32(&mut raw, 269, -8);
                        raw
                    },
                    &config.program_id,
                ),
            ),
            (
                tick_array_key,
                raydium_tick_array_account(&config.program_id, start_tick_index),
            ),
        ]);
        let live_store = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        let initial =
            build_executable_state_from_accounts(&tracked_pool, &accounts, 88, &live_store)
                .expect("repair should seed the clmm state");
        let initial_sqrt_price = initial
            .quote_model_update
            .as_ref()
            .expect("quote model update")
            .sqrt_price_x64;
        live_store
            .write()
            .expect("live store")
            .upsert_exact_reduction(&ExactReduction {
                next_state: initial.next_state,
                quote_model_update: initial.quote_model_update,
                refresh_required: false,
            });

        let outcome = apply_mutation_exact(
            89,
            &tracked_pool,
            &PoolMutation::RaydiumClmmSwap {
                pool_id: tracked_pool.pool_id.clone(),
                amount: 1_000,
                other_amount_threshold: 10_000,
                exact_out: true,
                zero_for_one: true,
            },
            &live_store,
        );

        let ExactMutationOutcome::Applied(reduction) = outcome else {
            panic!("expected local clmm reduction");
        };
        assert!(!reduction.refresh_required);
        let Some(quote_model_update) = reduction.quote_model_update.as_ref() else {
            panic!("expected local quote model update");
        };
        assert_ne!(quote_model_update.sqrt_price_x64, initial_sqrt_price);
        let ExecutablePoolState::RaydiumClmm(next_state) = &reduction.next_state else {
            panic!("expected raydium clmm state");
        };
        assert_eq!(next_state.last_update_slot, 89);
        assert_eq!(next_state.confidence, PoolConfidence::Executable);

        let store = live_store.read().expect("live store");
        let stored_state = store
            .get(&PoolId(tracked_pool.pool_id.clone()))
            .expect("stored clmm state");
        assert_eq!(stored_state.last_update_slot(), 89);
        let stored_quote_model = store
            .concentrated_quote_model(&PoolId(tracked_pool.pool_id.clone()))
            .expect("stored clmm quote model");
        assert_eq!(stored_quote_model.slot, 89);
        assert_eq!(
            stored_quote_model.sqrt_price_x64,
            quote_model_update.sqrt_price_x64
        );
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
    fn stale_repair_success_keeps_refresh_pending_and_retries() {
        let pool_id = "pool-a".to_string();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                12,
                4,
                PoolConfidence::Invalid,
                true,
            ));

        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            tracker.schedule_refresh(&pool_id, 12);
            tracker.mark_refresh_started(&pool_id);
        }

        let (result_tx, result_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        let hooks = NoopLiveHooks;
        let mut in_flight = HashMap::from([(pool_id.clone(), SyncRequestMode::RefreshExact)]);
        let mut attempts = HashMap::from([((pool_id.clone(), SyncRequestMode::RefreshExact), 1)]);

        result_tx
            .send(RepairJobResult::Success {
                pool_id: pool_id.clone(),
                mode: SyncRequestMode::RefreshExact,
                slot: 10,
                observed_slot: 12,
                next_state: constant_product_state(
                    &pool_id,
                    10,
                    3,
                    PoolConfidence::Executable,
                    false,
                ),
                quote_model_update: None,
                attempt: 1,
                latency: Duration::from_millis(1),
            })
            .expect("send repair result");
        drop(result_tx);

        drain_repair_results(
            &result_rx,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &sequence,
            &hooks,
            &retry_tx,
            &mut in_flight,
            &mut attempts,
        );

        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .refresh_required(&pool_id)
        );
        assert_eq!(
            executable_states
                .read()
                .expect("store lock")
                .get(&PoolId(pool_id.clone()))
                .expect("state")
                .last_update_slot(),
            12
        );

        let retry = retry_rx
            .recv_timeout(Duration::from_secs(8))
            .expect("retry request");
        assert_eq!(retry.pool_id, pool_id);
        assert_eq!(retry.mode, SyncRequestMode::RefreshExact);
        assert_eq!(retry.observed_slot, 12);
    }

    #[test]
    fn transient_refresh_retry_keeps_refresh_pending_and_requeues_without_events() {
        let pool_id = "pool-a".to_string();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                12,
                4,
                PoolConfidence::Invalid,
                true,
            ));

        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            tracker.schedule_refresh(&pool_id, 12);
            tracker.mark_refresh_started(&pool_id);
        }

        let (result_tx, result_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        let hooks = NoopLiveHooks;
        let mut in_flight = HashMap::from([(pool_id.clone(), SyncRequestMode::RefreshExact)]);
        let mut attempts = HashMap::from([((pool_id.clone(), SyncRequestMode::RefreshExact), 1)]);

        result_tx
            .send(RepairJobResult::Retry {
                pool_id: pool_id.clone(),
                mode: SyncRequestMode::RefreshExact,
                attempt: 1,
                observed_slot: 12,
                retry_kind: RepairRetryKind::Transient,
            })
            .expect("send retry result");
        drop(result_tx);

        drain_repair_results(
            &result_rx,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &sequence,
            &hooks,
            &retry_tx,
            &mut in_flight,
            &mut attempts,
        );

        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(in_flight.is_empty());
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .refresh_required(&pool_id)
        );

        let retry = retry_rx
            .recv_timeout(Duration::from_secs(8))
            .expect("retry request");
        assert_eq!(retry.pool_id, pool_id);
        assert_eq!(retry.mode, SyncRequestMode::RefreshExact);
        assert_eq!(retry.priority, RepairPriority::Retry);
        assert_eq!(retry.observed_slot, 12);
    }

    #[test]
    fn refresh_result_older_than_latest_watermark_stays_invalid_and_requeues_immediately() {
        let pool_id = "pool-a".to_string();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                100,
                4,
                PoolConfidence::Invalid,
                false,
            ));

        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            tracker.schedule_refresh(&pool_id, 100);
            tracker.mark_refresh_started(&pool_id);
            tracker.schedule_refresh(&pool_id, 105);
        }

        let (result_tx, result_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        let hooks = NoopLiveHooks;
        let mut in_flight = HashMap::from([(pool_id.clone(), SyncRequestMode::RefreshExact)]);
        let mut attempts = HashMap::from([((pool_id.clone(), SyncRequestMode::RefreshExact), 1)]);

        result_tx
            .send(RepairJobResult::Success {
                pool_id: pool_id.clone(),
                mode: SyncRequestMode::RefreshExact,
                slot: 102,
                observed_slot: 100,
                next_state: constant_product_state(
                    &pool_id,
                    102,
                    5,
                    PoolConfidence::Executable,
                    false,
                ),
                quote_model_update: None,
                attempt: 1,
                latency: Duration::from_millis(1),
            })
            .expect("send repair result");
        drop(result_tx);

        drain_repair_results(
            &result_rx,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &sequence,
            &hooks,
            &retry_tx,
            &mut in_flight,
            &mut attempts,
        );

        let event = event_rx.recv().expect("invalidation event");
        match event.payload {
            crate::MarketEvent::PoolInvalidation(invalidation) => {
                assert_eq!(invalidation.pool_id, pool_id);
            }
            other => panic!("expected pool invalidation, got {other:?}"),
        }
        let tracker = sync_tracker.lock().expect("tracker lock");
        assert!(tracker.refresh_required(&pool_id));
        assert_eq!(
            tracker.requested_observed_slot(&pool_id, SyncRequestMode::RefreshExact),
            Some(105)
        );
        drop(tracker);

        let state = executable_states
            .read()
            .expect("store lock")
            .get(&PoolId(pool_id.clone()))
            .expect("state")
            .clone();
        assert_eq!(state.last_update_slot(), 105);
        assert_eq!(state.confidence(), PoolConfidence::Invalid);

        let retry = retry_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("immediate retry request");
        assert_eq!(retry.pool_id, pool_id);
        assert_eq!(retry.mode, SyncRequestMode::RefreshExact);
        assert_eq!(retry.priority, RepairPriority::Immediate);
        assert_eq!(retry.observed_slot, 105);
    }

    #[test]
    fn refresh_success_with_late_watermark_reinvalidates_and_requeues() {
        let pool_id = "pool-a".to_string();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                100,
                4,
                PoolConfidence::Invalid,
                false,
            ));

        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            tracker.schedule_refresh(&pool_id, 100);
            tracker.mark_refresh_started(&pool_id);
        }

        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                102,
                5,
                PoolConfidence::Executable,
                false,
            ));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            let (_, should_enqueue) = tracker.schedule_refresh(&pool_id, 105);
            assert!(!should_enqueue);
        }

        let disposition = finalize_completed_sync_request(
            &executable_states,
            &sync_tracker,
            &pool_id,
            SyncRequestMode::RefreshExact,
            100,
        );
        assert_eq!(
            disposition,
            SyncCompletionDisposition::Advanced { observed_slot: 105 }
        );

        let (event_tx, event_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        invalidate_live_snapshot_after_advanced_request(
            &pool_id,
            105,
            &executable_states,
            &event_tx,
            &sequence,
        );

        let event = event_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("invalidation event");
        match event.payload {
            crate::MarketEvent::PoolInvalidation(invalidation) => {
                assert_eq!(invalidation.pool_id, pool_id);
            }
            other => panic!("expected pool invalidation, got {other:?}"),
        }

        let tracker = sync_tracker.lock().expect("tracker lock");
        assert!(tracker.refresh_required(&pool_id));
        assert_eq!(
            tracker.requested_observed_slot(&pool_id, SyncRequestMode::RefreshExact),
            Some(105)
        );
        drop(tracker);

        let state = executable_states
            .read()
            .expect("store lock")
            .get(&PoolId(pool_id.clone()))
            .expect("state")
            .clone();
        assert_eq!(state.last_update_slot(), 105);
        assert_eq!(state.confidence(), PoolConfidence::Invalid);
    }

    #[test]
    fn repair_result_older_than_latest_watermark_stays_invalid_and_requeues_immediately() {
        let pool_id = "pool-a".to_string();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                &pool_id,
                100,
                4,
                PoolConfidence::Invalid,
                true,
            ));

        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            assert!(tracker.repair_requested(&pool_id, 100));
            tracker.mark_repair_started(&pool_id);
            assert!(!tracker.repair_requested(&pool_id, 105));
        }

        let (result_tx, result_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        let hooks = NoopLiveHooks;
        let mut in_flight =
            HashMap::from([(pool_id.clone(), SyncRequestMode::RepairAfterInvalidation)]);
        let mut attempts = HashMap::from([(
            (pool_id.clone(), SyncRequestMode::RepairAfterInvalidation),
            1,
        )]);

        result_tx
            .send(RepairJobResult::Success {
                pool_id: pool_id.clone(),
                mode: SyncRequestMode::RepairAfterInvalidation,
                slot: 102,
                observed_slot: 100,
                next_state: constant_product_state(
                    &pool_id,
                    102,
                    5,
                    PoolConfidence::Executable,
                    false,
                ),
                quote_model_update: None,
                attempt: 1,
                latency: Duration::from_millis(1),
            })
            .expect("send repair result");
        drop(result_tx);

        drain_repair_results(
            &result_rx,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &sequence,
            &hooks,
            &retry_tx,
            &mut in_flight,
            &mut attempts,
        );

        let event = event_rx.recv().expect("invalidation event");
        match event.payload {
            crate::MarketEvent::PoolInvalidation(invalidation) => {
                assert_eq!(invalidation.pool_id, pool_id);
            }
            other => panic!("expected pool invalidation, got {other:?}"),
        }
        let tracker = sync_tracker.lock().expect("tracker lock");
        assert!(tracker.repair_required(&pool_id));
        assert_eq!(
            tracker.requested_observed_slot(&pool_id, SyncRequestMode::RepairAfterInvalidation),
            Some(105)
        );
        drop(tracker);

        let state = executable_states
            .read()
            .expect("store lock")
            .get(&PoolId(pool_id.clone()))
            .expect("state")
            .clone();
        assert_eq!(state.last_update_slot(), 105);
        assert_eq!(state.confidence(), PoolConfidence::Invalid);

        let retry = retry_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("immediate retry request");
        assert_eq!(retry.pool_id, pool_id);
        assert_eq!(retry.mode, SyncRequestMode::RepairAfterInvalidation);
        assert_eq!(retry.priority, RepairPriority::Immediate);
        assert_eq!(retry.observed_slot, 105);
    }

    #[test]
    fn dropped_refresh_with_late_watermark_requeues_instead_of_clearing() {
        let pool_id = "pool-a".to_string();
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        {
            let mut tracker = sync_tracker.lock().expect("tracker lock");
            tracker.schedule_refresh(&pool_id, 100);
            tracker.mark_refresh_started(&pool_id);
            tracker.schedule_refresh(&pool_id, 105);
        }

        let disposition = finalize_dropped_sync_request(
            &sync_tracker,
            &pool_id,
            SyncRequestMode::RefreshExact,
            100,
        );
        assert_eq!(
            disposition,
            SyncDropDisposition::Requeue { observed_slot: 105 }
        );

        let tracker = sync_tracker.lock().expect("tracker lock");
        assert!(tracker.refresh_required(&pool_id));
        assert_eq!(
            tracker.requested_observed_slot(&pool_id, SyncRequestMode::RefreshExact),
            Some(105)
        );
    }

    #[test]
    fn clearing_refresh_keeps_pending_repair_state() {
        let mut tracker = PoolSyncTracker::default();

        tracker.schedule_refresh("pool-a", 100);
        assert!(tracker.repair_requested("pool-a", 100));
        assert!(tracker.clear_refresh("pool-a"));

        assert!(tracker.repair_required("pool-a"));
        assert_eq!(
            tracker.requested_observed_slot("pool-a", SyncRequestMode::RepairAfterInvalidation),
            Some(100)
        );
    }

    #[test]
    fn schedule_exact_refresh_invalidates_hybrid_pool_immediately() {
        let tracked_pool = TrackedPool {
            pool_id: "pool-orca-clmm".into(),
            reducer_mode: ReducerRolloutMode::Active,
            watch_accounts: vec![],
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
                require_b_to_a: true,
            }),
        };
        let pool_id = PoolId(tracked_pool.pool_id.clone());
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(ExecutablePoolState::OrcaWhirlpool(
                ConcentratedLiquidityPoolState {
                    pool_id: pool_id.clone(),
                    venue: PoolVenue::OrcaWhirlpool,
                    token_mint_a: "mint-a".into(),
                    token_mint_b: "mint-b".into(),
                    token_vault_a: "vault-a".into(),
                    token_vault_b: "vault-b".into(),
                    fee_bps: 30,
                    active_liquidity: 1_000_000,
                    liquidity: 1_000_000,
                    sqrt_price_x64: 1u128 << 64,
                    current_tick_index: 0,
                    tick_spacing: 64,
                    loaded_tick_arrays: 3,
                    expected_tick_arrays: 3,
                    last_update_slot: 99,
                    write_version: 7,
                    last_verified_slot: 99,
                    confidence: PoolConfidence::Executable,
                    repair_pending: false,
                },
            ));
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (event_tx, event_rx) = mpsc::channel();
        let (repair_tx, repair_rx) = mpsc::channel();
        let sequence = Arc::new(AtomicU64::new(1));
        let hooks = NoopLiveHooks;

        schedule_exact_refresh(
            &tracked_pool,
            100,
            UNIX_EPOCH,
            None,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &repair_tx,
            &sequence,
            &hooks,
        );

        let event = event_rx.recv().expect("invalidation event");
        match event.payload {
            crate::MarketEvent::PoolInvalidation(invalidation) => {
                assert_eq!(invalidation.pool_id, tracked_pool.pool_id);
            }
            other => panic!("expected invalidation event, got {other:?}"),
        }

        let request = repair_rx.recv().expect("refresh request");
        assert_eq!(request.pool_id, tracked_pool.pool_id);
        assert_eq!(request.mode, SyncRequestMode::RefreshExact);
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .refresh_required(&tracked_pool.pool_id)
        );
        let confidence = executable_states
            .read()
            .expect("store lock")
            .get(&pool_id)
            .expect("pool state")
            .confidence();
        assert_eq!(confidence, PoolConfidence::Invalid);

        schedule_exact_refresh(
            &tracked_pool,
            101,
            UNIX_EPOCH,
            None,
            &executable_states,
            &sync_tracker,
            &event_tx,
            &repair_tx,
            &sequence,
            &hooks,
        );

        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(repair_rx.try_recv(), Err(TryRecvError::Empty)));
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
    fn idle_refresh_enqueues_background_refresh_without_invalidation() {
        let registry = sample_registry();
        let executable_states = Arc::new(RwLock::new(LiveExecutableStateStore::default()));
        executable_states
            .write()
            .expect("store lock")
            .upsert(constant_product_state(
                "pool-orca-simple",
                60,
                4,
                PoolConfidence::Executable,
                false,
            ));
        let sync_tracker = Arc::new(Mutex::new(PoolSyncTracker::default()));
        let (repair_tx, repair_rx) = mpsc::channel();
        let hooks = NoopLiveHooks;

        refresh_idle_tracked_pools(
            100,
            &registry,
            &executable_states,
            &sync_tracker,
            &repair_tx,
            32,
            &hooks,
            UNIX_EPOCH,
        );

        let request = repair_rx.recv().expect("background refresh request");
        assert_eq!(request.pool_id, "pool-orca-simple");
        assert_eq!(request.mode, SyncRequestMode::RefreshExact);
        assert_eq!(request.priority, RepairPriority::Retry);
        assert_eq!(request.observed_slot, 61);
        assert!(
            sync_tracker
                .lock()
                .expect("tracker lock")
                .refresh_required("pool-orca-simple")
        );
        assert_eq!(
            executable_states
                .read()
                .expect("store lock")
                .get(&PoolId("pool-orca-simple".into()))
                .expect("pool state")
                .confidence(),
            PoolConfidence::Executable
        );

        refresh_idle_tracked_pools(
            101,
            &registry,
            &executable_states,
            &sync_tracker,
            &repair_tx,
            32,
            &hooks,
            UNIX_EPOCH,
        );

        assert!(matches!(repair_rx.try_recv(), Err(TryRecvError::Empty)));
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
