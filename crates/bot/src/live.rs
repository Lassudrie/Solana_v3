use std::{
    collections::{BTreeSet, HashMap, HashSet},
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
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
    BotConfig, ReducerRolloutMode, RouteClassConfig, RouteLegExecutionConfig,
    RuntimeProfileConfig,
};

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
const ORCA_SWAP_DISCRIMINATOR_NAME: &str = "swap";
const RAYDIUM_CLMM_SWAP_DISCRIMINATOR_NAME: &str = "swap_v2";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

#[derive(Debug)]
pub struct GrpcEntriesEventSource {
    receiver: Receiver<NormalizedEvent>,
}

impl GrpcEntriesEventSource {
    pub fn spawn(config: &BotConfig) -> Result<Self, String> {
        Endpoint::from_shared(config.shredstream.grpc_endpoint.clone())
            .map_err(|error| error.to_string())?;

        let registry = Arc::new(TrackedPoolRegistry::from_config(config));
        let executable_states = Arc::new(RwLock::new(ExecutablePoolStateStore::default()));
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
            event_tx.clone(),
            Arc::clone(&sequence),
        );

        for pool_id in registry.pool_ids() {
            let _ = repair_tx.send(RepairRequest { pool_id });
        }

        spawn_stream_worker(
            config,
            registry,
            executable_states,
            lookup_cache,
            event_tx,
            repair_tx,
            sequence,
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
            TrackedPoolKind::OrcaWhirlpool(config) => vec![
                config.whirlpool.clone(),
                config.token_vault_a.clone(),
                config.token_vault_b.clone(),
            ],
            TrackedPoolKind::RaydiumClmm(config) => vec![
                config.pool_state.clone(),
                config.token_vault_a.clone(),
                config.token_vault_b.clone(),
            ],
        }
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
                            exec.amm_coin_vault.clone(),
                            exec.amm_pc_vault.clone(),
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
        if instruction.data.first().copied()? != RAYDIUM_SWAP_BASE_IN_TAG {
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
            user_owner: instruction.accounts.get(16)?.clone(),
            user_source_token_account: instruction.accounts.get(14)?.clone(),
            user_destination_token_account: instruction.accounts.get(15)?.clone(),
            amount_in: read_u64(&instruction.data, 1)?,
            min_output_amount: read_u64(&instruction.data, 9)?,
        })
    }

    fn decode_orca_whirlpool(&self, instruction: &ResolvedInstruction) -> Option<PoolMutation> {
        if !instruction_has_anchor_discriminator(instruction, ORCA_SWAP_DISCRIMINATOR_NAME) {
            return None;
        }
        let pool_id = self
            .orca_whirlpool_by_account
            .get(instruction.accounts.get(2)?)?
            .clone();
        let tracked = self.tracked_pool(&pool_id)?;
        let TrackedPoolKind::OrcaWhirlpool(config) = &tracked.kind else {
            return None;
        };
        if instruction.program_id != config.program_id || instruction.accounts.len() < 11 {
            return None;
        }
        Some(PoolMutation::OrcaWhirlpoolSwap {
            pool_id,
            amount: read_u64(&instruction.data, 8)?,
            other_amount_threshold: read_u64(&instruction.data, 16)?,
            a_to_b: instruction.data.last().copied()? == 1,
        })
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
            zero_for_one,
        })
    }
}

#[derive(Debug, Clone)]
struct RepairRequest {
    pool_id: String,
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
        user_owner: String,
        user_source_token_account: String,
        user_destination_token_account: String,
        amount_in: u64,
        min_output_amount: u64,
    },
    OrcaWhirlpoolSwap {
        pool_id: String,
        amount: u64,
        other_amount_threshold: u64,
        a_to_b: bool,
    },
    RaydiumClmmSwap {
        pool_id: String,
        amount: u64,
        other_amount_threshold: u64,
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
    lookup_cache: Arc<RwLock<HashMap<String, LookupTableSnapshot>>>,
    event_tx: SyncSender<NormalizedEvent>,
    repair_tx: mpsc::Sender<RepairRequest>,
    sequence: Arc<AtomicU64>,
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
                        &lookup_cache,
                        &event_tx,
                        &repair_tx,
                        &sequence,
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
    lookup_cache: &Arc<RwLock<HashMap<String, LookupTableSnapshot>>>,
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
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
                        None => invalidate_and_repair(
                            &tracked_pool,
                            slot,
                            source_received_at,
                            source_latency,
                            executable_states,
                            event_tx,
                            repair_tx,
                            sequence,
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
                            event_tx,
                            repair_tx,
                            sequence,
                            &mut invalidated_pools,
                        );
                    }
                    ReducerRolloutMode::Disabled => invalidate_and_repair(
                        &tracked_pool,
                        slot,
                        source_received_at,
                        source_latency,
                        executable_states,
                        event_tx,
                        repair_tx,
                        sequence,
                        &mut invalidated_pools,
                    ),
                }
            }

            for tracked_pool in registry.touched_pools(&resolved.account_keys) {
                if handled_pools.contains(&tracked_pool.pool_id) {
                    continue;
                }
                invalidate_and_repair(
                    &tracked_pool,
                    slot,
                    source_received_at,
                    source_latency,
                    executable_states,
                    event_tx,
                    repair_tx,
                    sequence,
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
            return Some(ResolvedTransaction {
                account_keys: account_keys.iter().map(ToString::to_string).collect(),
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
    Some(ResolvedTransaction {
        account_keys: account_keys.iter().map(ToString::to_string).collect(),
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
                state.set_confidence(PoolConfidence::Probable, slot, next_write_version, None);
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
                user_owner,
                user_source_token_account,
                user_destination_token_account,
                amount_in,
                ..
            },
            ExecutablePoolState::RaydiumSimplePool(state),
        ) => reduce_raydium_simple_swap(
            state,
            config,
            user_owner,
            user_source_token_account,
            user_destination_token_account,
            *amount_in,
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
    user_owner: &str,
    user_source_token_account: &str,
    user_destination_token_account: &str,
    amount_in: u64,
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

    reduce_constant_product_swap(
        state,
        source_vault,
        destination_vault,
        amount_in,
        slot,
        PoolVenue::RaydiumSimplePool,
    )
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
    next.confidence = PoolConfidence::Exact;
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

fn parse_legacy_pubkey(value: &str) -> Option<LegacyPubkey> {
    LegacyPubkey::from_str(value).ok()
}

fn associated_token_address(
    owner: LegacyPubkey,
    mint: LegacyPubkey,
    token_program: LegacyPubkey,
) -> LegacyPubkey {
    LegacyPubkey::find_program_address(
        &[
            owner.as_ref(),
            token_program.as_ref(),
            mint.as_ref(),
        ],
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
    event_tx: &SyncSender<NormalizedEvent>,
    repair_tx: &mpsc::Sender<RepairRequest>,
    sequence: &Arc<AtomicU64>,
    invalidated_pools: &mut HashSet<String>,
) {
    if !invalidated_pools.insert(tracked_pool.pool_id.clone()) {
        return;
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
    });
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
            exact: Some(snapshot.is_exact()),
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
    event_tx: SyncSender<NormalizedEvent>,
    sequence: Arc<AtomicU64>,
) -> mpsc::Sender<RepairRequest> {
    let (repair_tx, repair_rx) = mpsc::channel::<RepairRequest>();
    let rpc_endpoint = config.reconciliation.rpc_http_endpoint.clone();

    thread::spawn(move || {
        let http = Client::builder()
            .connect_timeout(Duration::from_millis(300))
            .timeout(Duration::from_millis(1_000))
            .build()
            .expect("build live repair HTTP client");

        while let Ok(first) = repair_rx.recv() {
            let mut requested = BTreeSet::from([first.pool_id]);
            while let Ok(next) = repair_rx.try_recv() {
                requested.insert(next.pool_id);
                if requested.len() >= 32 {
                    break;
                }
            }
            let tracked = requested
                .iter()
                .filter_map(|pool_id| registry.tracked_pool(pool_id))
                .collect::<Vec<_>>();
            if tracked.is_empty() {
                continue;
            }

            let account_keys = tracked
                .iter()
                .flat_map(TrackedPool::repair_account_keys)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let Some((slot, accounts)) = fetch_accounts_by_key(&http, &rpc_endpoint, &account_keys)
            else {
                continue;
            };

            for tracked_pool in tracked {
                let Some(next_state) = build_executable_state_from_accounts(
                    &tracked_pool,
                    &accounts,
                    slot,
                    &executable_states,
                ) else {
                    continue;
                };
                if let Ok(mut store) = executable_states.write() {
                    let _ = store.upsert(next_state.clone());
                }
                publish_pool_snapshot_update(
                    &event_tx,
                    &sequence,
                    slot,
                    SystemTime::now(),
                    None,
                    &next_state,
                );
            }
        }
    });

    repair_tx
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
                    confidence: PoolConfidence::Exact,
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
                    confidence: PoolConfidence::Exact,
                    repair_pending: false,
                },
            ))
        }
        TrackedPoolKind::OrcaWhirlpool(config) => {
            let raw = decode_base64_account(accounts.get(&config.whirlpool)?)?;
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
                    confidence: PoolConfidence::Probable,
                    repair_pending: true,
                },
            ))
        }
        TrackedPoolKind::RaydiumClmm(config) => {
            let raw = decode_base64_account(accounts.get(&config.pool_state)?)?;
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
                    confidence: PoolConfidence::Probable,
                    repair_pending: true,
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
            source_latency: source_latency.unwrap_or(Duration::ZERO),
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

fn anchor_instruction_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("global:{name}");
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
        fs,
        time::{Duration, UNIX_EPOCH},
    };

    use prost_types::Timestamp;
    use serde::Deserialize;
    use state::types::{ConstantProductPoolState, PoolConfidence, PoolId, PoolVenue};

    use super::{
        ORCA_SWAP_INSTRUCTION_TAG, OrcaSimpleTrackedConfig, PoolMutation,
        RaydiumSimpleTrackedConfig, ResolvedInstruction, TrackedPool, TrackedPoolKind,
        TrackedPoolRegistry, anchor_instruction_discriminator, associated_token_address,
        constant_product_amount_out, parse_legacy_pubkey, reduce_orca_simple_swap,
        reduce_raydium_simple_swap, timestamp_to_system_time,
    };
    use crate::config::{BotConfig, ReducerRolloutMode, RoutesConfig};

    #[derive(Debug, Deserialize)]
    struct RouteFile {
        routes: RoutesConfig,
    }

    #[test]
    fn registry_tracks_unique_pools_from_routes() {
        let text = fs::read_to_string("/root/bot/Solana_v3/sol_usdc_routes.toml").unwrap();
        let parsed: RouteFile = toml::from_str(&text).unwrap();
        let mut config = BotConfig::default();
        config.routes = parsed.routes;
        let registry = TrackedPoolRegistry::from_config(&config);
        assert!(registry.pool_ids().len() >= 2);
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
            confidence: PoolConfidence::Exact,
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
            confidence: PoolConfidence::Exact,
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
    fn cpmm_output_accounts_for_fee() {
        let output = constant_product_amount_out(1_000_000, 2_000_000, 100_000, 30).unwrap();
        assert!(output > 0);
        assert!(output < 200_000);
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
}
