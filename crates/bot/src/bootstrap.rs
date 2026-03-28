use base64::Engine;
use builder::{
    AtomicArbTransactionBuilder, ExecutionRegistry, LookupTableUsageConfig, MessageMode,
    OrcaSimplePoolConfig, OrcaWhirlpoolConfig, RaydiumClmmConfig, RaydiumSimplePoolConfig,
    RouteExecutionConfig as BuilderRouteExecutionConfig, VenueExecutionConfig,
};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use signing::{Signer, SigningError};
use solana_sdk::pubkey::Pubkey;
use state::{
    StatePlane,
    decoder::{OrcaWhirlpoolAccountDecoder, PoolPriceAccountDecoder, RaydiumClmmPoolDecoder},
    types::{AccountKey, PoolId, RouteId},
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strategy::{
    StrategyPlane,
    guards::GuardrailConfig,
    route_registry::{
        ExecutionProtectionPolicy, RouteDefinition, RouteKind, RouteLeg, RouteLegSequence,
        RouteSizingPolicy, RouteValidationError, SizingMode, StrategySizingConfig, SwapSide,
    },
};
use thiserror::Error;

use crate::{
    account_batcher::{GetMultipleAccountsBatcher, RpcContext, decode_lookup_table},
    config::{
        BotConfig, BuilderConfig, MessageModeConfig, RouteClassConfig, RouteKindConfig,
        RouteLegExecutionConfig, RuntimeProfileConfig, SigningConfig, SigningProviderKind,
        SwapSideConfig,
    },
    route_health::SharedRouteHealth,
    rpc::rpc_call,
    runtime::{
        AltCacheService, BlockhashService, BotRuntime, ColdPathServices, HotPathPipeline,
        WalletRefreshService, WarmupCoordinator,
    },
    submit_factory::{build_submitter, submit_mode_from_config},
};

const BASE_FEE_LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const MICRO_LAMPORTS_PER_LAMPORT: u64 = 1_000_000;
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const MOCK_SCHEME: &str = "mock://";
const SPL_TOKEN_ACCOUNT_LEN: usize = 165;
const SPL_TOKEN_ACCOUNT_STATE_OFFSET: usize = 108;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Signing(#[from] SigningError),
    #[error("invalid runtime config: {detail}")]
    InvalidConfig { detail: String },
    #[error("invalid route config for {route_id}: {detail}")]
    InvalidRouteConfig { route_id: String, detail: String },
    #[error("execution environment is not ready: {detail}")]
    ExecutionEnvironment { detail: String },
}

struct ResolvedSigner {
    owner_pubkey: String,
    signer_available: bool,
    signer: Arc<dyn Signer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequiredTokenAccount {
    address: String,
    token_program_id: String,
    wallet_owner: String,
    expected_mint: Option<String>,
    sources: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
struct RpcMultipleAccountsResponse {
    value: Vec<Option<RpcAccountInfo>>,
}

#[derive(Debug, Deserialize)]
struct RpcLatestBlockhashResponse {
    context: RpcContext,
    value: RpcLatestBlockhashValue,
}

#[derive(Debug, Deserialize)]
struct RpcLatestBlockhashValue {
    blockhash: String,
}

#[derive(Debug, Deserialize)]
struct RpcBalanceResponse {
    #[serde(rename = "context")]
    _context: RpcContext,
    value: u64,
}

#[derive(Debug, Deserialize)]
struct RpcAccountInfo {
    owner: String,
    data: (String, String),
}

#[derive(Debug)]
struct ParsedTokenAccount {
    mint: String,
    owner: String,
}

pub fn bootstrap(config: BotConfig) -> Result<BotRuntime, BootstrapError> {
    let route_health = std::sync::Arc::new(std::sync::Mutex::new(
        crate::route_health::RouteHealthRegistry::new(
            config.runtime.live_set_health.clone(),
            config.state.max_snapshot_slot_lag,
        ),
    ));
    bootstrap_with_health(config, route_health)
}

pub fn bootstrap_with_health(
    config: BotConfig,
    route_health: SharedRouteHealth,
) -> Result<BotRuntime, BootstrapError> {
    if config.strategy.max_snapshot_slot_lag != config.state.max_snapshot_slot_lag {
        return Err(BootstrapError::InvalidConfig {
            detail: format!(
                "state.max_snapshot_slot_lag ({}) must match strategy.max_snapshot_slot_lag ({})",
                config.state.max_snapshot_slot_lag, config.strategy.max_snapshot_slot_lag
            ),
        });
    }

    let mut state = StatePlane::new(config.state.max_snapshot_slot_lag);
    state
        .decoder_registry_mut()
        .register(PoolPriceAccountDecoder);
    state
        .decoder_registry_mut()
        .register(OrcaWhirlpoolAccountDecoder);
    state
        .decoder_registry_mut()
        .register(RaydiumClmmPoolDecoder);

    let mut strategy = StrategyPlane::new(
        GuardrailConfig {
            min_profit_quote_atoms: config.strategy.min_profit_quote_atoms,
            min_profit_bps: config.strategy.min_profit_bps,
            require_route_warm: config.strategy.require_route_warm,
            max_inflight_submissions: config.strategy.max_inflight_submissions,
            min_wallet_balance_lamports: config.strategy.min_wallet_balance_lamports,
            max_blockhash_slot_lag: config.strategy.max_blockhash_slot_lag,
        },
        StrategySizingConfig {
            mode: map_sizing_mode(config.strategy.sizing.mode),
            fixed_trade_size: config.strategy.sizing.fixed_trade_size,
            min_trade_floor_sol_lamports: config.strategy.sizing.min_trade_floor_sol_lamports,
            base_landing_rate_bps: config.strategy.sizing.base_landing_rate_bps,
            ewma_alpha_bps: config.strategy.sizing.ewma_alpha_bps,
            base_expected_shortfall_bps: config.strategy.sizing.base_expected_shortfall_bps,
            max_expected_shortfall_bps: config.strategy.sizing.max_expected_shortfall_bps,
            too_little_output_shortfall_step_bps: config
                .strategy
                .sizing
                .too_little_output_shortfall_step_bps,
            inflight_penalty_bps_per_submission: config
                .strategy
                .sizing
                .inflight_penalty_bps_per_submission,
            max_inflight_penalty_bps: config.strategy.sizing.max_inflight_penalty_bps,
            blockhash_penalty_bps_per_slot: config.strategy.sizing.blockhash_penalty_bps_per_slot,
            max_blockhash_penalty_bps: config.strategy.sizing.max_blockhash_penalty_bps,
            max_reserve_usage_penalty_bps: config.strategy.sizing.max_reserve_usage_penalty_bps,
        },
    );

    let active_routes = config
        .routes
        .definitions
        .iter()
        .filter(|route| route_enabled_in_profile(route, config.runtime.profile))
        .collect::<Vec<_>>();
    let lookup_table_keys = collect_lookup_table_keys(&active_routes);
    for route in &active_routes {
        validate_runtime_route_config(route)?;
    }
    let resolved_signer = resolve_signer(&config.signing)?;
    validate_execution_environment(&config, &active_routes, &resolved_signer.owner_pubkey)?;
    let tracked_pool_pairs = build_tracked_pool_pairs(&active_routes);

    let mut execution_registry = ExecutionRegistry::default();
    for route in active_routes {
        let sol_quote_conversion_pool_id =
            resolve_sol_quote_conversion_pool_id(route, &tracked_pool_pairs)?;
        let route_kind = map_route_kind(route.kind);
        let route_definition = resolve_route_definition(
            route,
            route_kind,
            &config.strategy.sizing,
            &config.builder,
            sol_quote_conversion_pool_id.clone(),
        )?;
        let route_execution = resolve_builder_route_execution(route, route_kind)?;
        let route_id = route_definition.route_id.clone();
        let mut pool_ids = route_definition
            .legs
            .iter()
            .map(|leg| leg.pool_id.clone())
            .collect::<Vec<_>>();
        if let Some(pool_id) = &sol_quote_conversion_pool_id {
            pool_ids.push(pool_id.clone());
        }
        let mut seen_pool_ids = HashSet::with_capacity(pool_ids.len());
        pool_ids.retain(|pool_id| seen_pool_ids.insert(pool_id.clone()));
        let max_quote_slot_lag = effective_max_quote_slot_lag(route);
        if let Ok(mut health) = route_health.lock() {
            health.register_route(route_id.clone(), &pool_ids, max_quote_slot_lag);
        }
        state.register_route_with_execution_lag(route_id.clone(), pool_ids, max_quote_slot_lag);
        for dependency in &route.account_dependencies {
            state.register_account_dependency(
                AccountKey(dependency.account_key.clone()),
                PoolId(dependency.pool_id.clone()),
                dependency.decoder_key.clone(),
            );
        }
        strategy.register_route(route_definition);
        execution_registry.register(route_execution);
    }

    let submit_mode = submit_mode_from_config(&config);
    let cold_path = build_cold_path_services(
        &config,
        &resolved_signer.owner_pubkey,
        resolved_signer.signer_available,
        &lookup_table_keys,
    );
    let wallet_status = if !config.signing.wallet_ready {
        signing::WalletStatus::Refreshing
    } else if resolved_signer.signer_available {
        signing::WalletStatus::Ready
    } else {
        signing::WalletStatus::MissingSigner
    };
    let mut runtime = BotRuntime::new(
        HotPathPipeline::new(
            state,
            strategy,
            AtomicArbTransactionBuilder::new(execution_registry),
            signing::HotWallet {
                wallet_id: config.signing.wallet_id.clone(),
                owner_pubkey: resolved_signer.owner_pubkey,
                balance_lamports: config.signing.bootstrap_balance_lamports,
                status: wallet_status,
            },
            resolved_signer.signer,
            build_submitter(&config),
        ),
        cold_path,
        submit_mode,
        config.builder.compute_unit_limit,
        config.builder.compute_unit_price_micro_lamports,
        config.builder.jito_tip_lamports,
        route_health,
    );
    runtime.apply_cold_path_seed();
    Ok(runtime)
}

fn build_cold_path_services(
    config: &BotConfig,
    wallet_pubkey: &str,
    signer_available: bool,
    lookup_table_keys: &[String],
) -> ColdPathServices {
    let mut blockhash = BlockhashService {
        current_blockhash: config.state.bootstrap_blockhash.clone(),
        slot: config.state.bootstrap_blockhash_slot,
    };
    let mut alt_cache = AltCacheService {
        revision: config.state.bootstrap_alt_revision,
        lookup_tables: Vec::new(),
    };
    let mut wallet_refresh = WalletRefreshService {
        balance_lamports: config.signing.bootstrap_balance_lamports,
        ready: config.signing.wallet_ready,
        signer_available,
    };

    let endpoint = config.reconciliation.rpc_http_endpoint.trim();
    if !endpoint.is_empty() && !endpoint.starts_with(MOCK_SCHEME) {
        match bootstrap_rpc_http_client() {
            Ok(http) => {
                match fetch_live_blockhash_seed(&http, endpoint) {
                    Ok(live_blockhash) => blockhash = live_blockhash,
                    Err(error) => eprintln!(
                        "bootstrap: failed to seed live blockhash from {endpoint}: {error}"
                    ),
                }
                match fetch_live_wallet_seed(&http, endpoint, wallet_pubkey) {
                    Ok(live_wallet) => {
                        wallet_refresh.balance_lamports = live_wallet.value;
                        wallet_refresh.ready = true;
                    }
                    Err(error) => eprintln!(
                        "bootstrap: failed to seed wallet balance from {endpoint}: {error}"
                    ),
                }
                if !lookup_table_keys.is_empty() {
                    match fetch_live_lookup_table_seed(endpoint, lookup_table_keys) {
                        Ok(live_alt_cache) => alt_cache = live_alt_cache,
                        Err(error) => eprintln!(
                            "bootstrap: failed to seed lookup tables from {endpoint}: {error}"
                        ),
                    }
                }
            }
            Err(error) => {
                eprintln!("bootstrap: failed to build RPC client for {endpoint}: {error}")
            }
        }
    }

    ColdPathServices {
        warmup: WarmupCoordinator::default(),
        blockhash,
        alt_cache,
        wallet_refresh,
    }
}

fn collect_lookup_table_keys(routes: &[&crate::config::RouteConfig]) -> Vec<String> {
    let mut unique = BTreeSet::new();
    for route in routes {
        for table in &route.execution.lookup_tables {
            unique.insert(table.account_key.clone());
        }
    }
    unique.into_iter().collect()
}

fn bootstrap_rpc_http_client() -> Result<Client, String> {
    Client::builder()
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_millis(1_000))
        .build()
        .map_err(|error| error.to_string())
}

fn fetch_live_blockhash_seed(http: &Client, endpoint: &str) -> Result<BlockhashService, String> {
    let response = rpc_call::<RpcLatestBlockhashResponse>(
        http,
        endpoint,
        "getLatestBlockhash",
        json!([{ "commitment": "processed" }]),
    )
    .map_err(|error| error.to_string())?;
    Ok(BlockhashService {
        current_blockhash: response.value.blockhash,
        slot: response.context.slot,
    })
}

fn fetch_live_wallet_seed(
    http: &Client,
    endpoint: &str,
    wallet_pubkey: &str,
) -> Result<RpcBalanceResponse, String> {
    rpc_call::<RpcBalanceResponse>(
        http,
        endpoint,
        "getBalance",
        json!([
            wallet_pubkey,
            { "commitment": "processed" }
        ]),
    )
    .map_err(|error| error.to_string())
}

fn fetch_live_lookup_table_seed(
    endpoint: &str,
    lookup_table_keys: &[String],
) -> Result<AltCacheService, String> {
    let account_batcher =
        GetMultipleAccountsBatcher::new_with_window(endpoint, Duration::from_millis(1));
    let fetched = account_batcher
        .fetch(lookup_table_keys)
        .map_err(|error| error.to_string())?;
    let lookup_tables = lookup_table_keys
        .iter()
        .zip(fetched.ordered_values(lookup_table_keys))
        .filter_map(|(account_key, account)| {
            decode_lookup_table(account_key, account, fetched.slot)
        })
        .collect::<Vec<_>>();
    Ok(AltCacheService {
        revision: fetched.slot,
        lookup_tables,
    })
}

fn route_enabled_in_profile(
    route: &crate::config::RouteConfig,
    profile: RuntimeProfileConfig,
) -> bool {
    route.enabled
        && match profile {
            RuntimeProfileConfig::Default => true,
            RuntimeProfileConfig::UltraFast => route.route_class == RouteClassConfig::AmmFastPath,
        }
}

fn map_swap_side(side: &SwapSideConfig) -> SwapSide {
    match side {
        SwapSideConfig::BuyBase => SwapSide::BuyBase,
        SwapSideConfig::SellBase => SwapSide::SellBase,
    }
}

fn map_sizing_mode(mode: crate::config::SizingModeConfig) -> SizingMode {
    match mode {
        crate::config::SizingModeConfig::Legacy => SizingMode::Legacy,
        crate::config::SizingModeConfig::EvShadow => SizingMode::EvShadow,
        crate::config::SizingModeConfig::EvLive => SizingMode::EvLive,
    }
}

fn map_route_kind(kind: RouteKindConfig) -> RouteKind {
    match kind {
        RouteKindConfig::TwoLeg => RouteKind::TwoLeg,
        RouteKindConfig::Triangular => RouteKind::Triangular,
    }
}

fn resolve_route_sizing(
    global: &crate::config::StrategySizingConfig,
    route: &crate::config::RouteSizingConfig,
) -> RouteSizingPolicy {
    RouteSizingPolicy {
        mode: route
            .mode
            .map(map_sizing_mode)
            .unwrap_or(map_sizing_mode(global.mode)),
        min_trade_floor_sol_lamports: route
            .min_trade_floor_sol_lamports
            .unwrap_or(global.min_trade_floor_sol_lamports),
        base_landing_rate_bps: route
            .base_landing_rate_bps
            .unwrap_or(global.base_landing_rate_bps),
        base_expected_shortfall_bps: route
            .base_expected_shortfall_bps
            .unwrap_or(global.base_expected_shortfall_bps),
        max_expected_shortfall_bps: route
            .max_expected_shortfall_bps
            .unwrap_or(global.max_expected_shortfall_bps),
    }
}

fn resolve_route_definition(
    route: &crate::config::RouteConfig,
    route_kind: RouteKind,
    strategy_sizing: &crate::config::StrategySizingConfig,
    builder: &BuilderConfig,
    sol_quote_conversion_pool_id: Option<PoolId>,
) -> Result<RouteDefinition, BootstrapError> {
    let min_trade_size = resolve_min_trade_size(route)?;
    let legs = route
        .legs
        .iter()
        .enumerate()
        .map(|(index, leg)| {
            let (input_mint, output_mint) = resolve_route_leg_mints(route, route_kind, leg, index)?;
            validate_leg_execution_mints(route, index, &leg.execution, &input_mint, &output_mint)?;
            Ok(RouteLeg {
                venue: leg.venue.clone(),
                pool_id: PoolId(leg.pool_id.clone()),
                side: map_swap_side(&leg.side),
                input_mint,
                output_mint,
                fee_bps: leg.fee_bps,
            })
        })
        .collect::<Result<Vec<_>, BootstrapError>>()?;

    let definition = RouteDefinition {
        route_id: RouteId(route.route_id.clone()),
        kind: route_kind,
        input_mint: route.input_mint.clone(),
        output_mint: route.output_mint.clone(),
        base_mint: route.base_mint.clone(),
        quote_mint: route.quote_mint.clone(),
        sol_quote_conversion_pool_id,
        min_profit_quote_atoms: route.min_profit_quote_atoms,
        legs: RouteLegSequence::from_vec(legs)
            .map_err(|error| route_validation_error(route, error))?,
        max_quote_slot_lag: effective_max_quote_slot_lag(route),
        min_trade_size,
        default_trade_size: route.default_trade_size,
        max_trade_size: route.max_trade_size,
        size_ladder: route.size_ladder.clone(),
        estimated_execution_cost_lamports: estimate_execution_cost_lamports(
            route,
            builder,
            route.execution.default_compute_unit_limit,
            route.execution.default_compute_unit_price_micro_lamports,
            route.execution.default_jito_tip_lamports,
        ),
        sizing: resolve_route_sizing(strategy_sizing, &route.sizing),
        execution_protection: map_execution_protection(&route.execution_protection),
    };

    definition
        .validate()
        .map_err(|error| route_validation_error(route, error))?;
    Ok(definition)
}

fn resolve_builder_route_execution(
    route: &crate::config::RouteConfig,
    route_kind: RouteKind,
) -> Result<BuilderRouteExecutionConfig, BootstrapError> {
    let minimum_compute_unit_limit = route
        .execution
        .minimum_compute_unit_limit
        .max(route_kind.minimum_compute_unit_limit());
    if route.execution.default_compute_unit_limit < minimum_compute_unit_limit {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "execution.default_compute_unit_limit ({}) must be >= {} for {:?} routes",
                route.execution.default_compute_unit_limit, minimum_compute_unit_limit, route_kind
            ),
        });
    }
    let execution_legs = route
        .legs
        .iter()
        .map(|leg| map_leg_execution(&leg.execution))
        .collect::<Vec<_>>();

    Ok(BuilderRouteExecutionConfig {
        route_id: RouteId(route.route_id.clone()),
        kind: route_kind,
        message_mode: map_message_mode(&route.execution.message_mode),
        lookup_tables: route
            .execution
            .lookup_tables
            .iter()
            .map(|table| LookupTableUsageConfig {
                account_key: table.account_key.clone(),
            })
            .collect(),
        default_compute_unit_limit: route.execution.default_compute_unit_limit,
        minimum_compute_unit_limit,
        default_compute_unit_price_micro_lamports: route
            .execution
            .default_compute_unit_price_micro_lamports,
        default_jito_tip_lamports: route.execution.default_jito_tip_lamports,
        max_quote_slot_lag: effective_max_quote_slot_lag(route),
        max_alt_slot_lag: route.execution.max_alt_slot_lag,
        legs: RouteLegSequence::from_vec(execution_legs)
            .map_err(|error| route_validation_error(route, error))?,
    })
}

fn resolve_route_leg_mints(
    route: &crate::config::RouteConfig,
    route_kind: RouteKind,
    leg: &crate::config::RouteLegConfig,
    leg_index: usize,
) -> Result<(String, String), BootstrapError> {
    match (&leg.input_mint, &leg.output_mint) {
        (Some(input_mint), Some(output_mint)) => Ok((input_mint.clone(), output_mint.clone())),
        (None, None) if route_kind == RouteKind::TwoLeg => {
            let base_mint = route.base_mint.clone().ok_or_else(|| BootstrapError::InvalidRouteConfig {
                route_id: route.route_id.clone(),
                detail: format!(
                    "legs[{leg_index}] must define input_mint/output_mint when base_mint is absent"
                ),
            })?;
            let quote_mint = route.quote_mint.clone().ok_or_else(|| BootstrapError::InvalidRouteConfig {
                route_id: route.route_id.clone(),
                detail: format!(
                    "legs[{leg_index}] must define input_mint/output_mint when quote_mint is absent"
                ),
            })?;
            Ok(match leg.side {
                SwapSideConfig::BuyBase => (quote_mint, base_mint),
                SwapSideConfig::SellBase => (base_mint, quote_mint),
            })
        }
        (None, None) => Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "triangular routes require explicit legs[{leg_index}].input_mint and legs[{leg_index}].output_mint"
            ),
        }),
        _ => Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "legs[{leg_index}] must define both input_mint and output_mint together"
            ),
        }),
    }
}

fn validate_leg_execution_mints(
    route: &crate::config::RouteConfig,
    leg_index: usize,
    execution: &RouteLegExecutionConfig,
    input_mint: &str,
    output_mint: &str,
) -> Result<(), BootstrapError> {
    let invalid = |detail: String| BootstrapError::InvalidRouteConfig {
        route_id: route.route_id.clone(),
        detail,
    };

    match execution {
        RouteLegExecutionConfig::OrcaWhirlpool(config) => {
            let expected_a_to_b =
                if input_mint == config.token_mint_a && output_mint == config.token_mint_b {
                    Some(true)
                } else if input_mint == config.token_mint_b && output_mint == config.token_mint_a {
                    Some(false)
                } else {
                    None
                };
            let Some(expected_a_to_b) = expected_a_to_b else {
                return Err(invalid(format!(
                    "legs[{leg_index}] mints {input_mint}->{output_mint} do not match whirlpool pair {}<->{}",
                    config.token_mint_a, config.token_mint_b
                )));
            };
            if config.a_to_b != expected_a_to_b {
                return Err(invalid(format!(
                    "legs[{leg_index}] direction {input_mint}->{output_mint} conflicts with orca_whirlpool.a_to_b={}",
                    config.a_to_b
                )));
            }
        }
        RouteLegExecutionConfig::RaydiumClmm(config) => {
            let expected_zero_for_one =
                if input_mint == config.token_mint_0 && output_mint == config.token_mint_1 {
                    Some(true)
                } else if input_mint == config.token_mint_1 && output_mint == config.token_mint_0 {
                    Some(false)
                } else {
                    None
                };
            let Some(expected_zero_for_one) = expected_zero_for_one else {
                return Err(invalid(format!(
                    "legs[{leg_index}] mints {input_mint}->{output_mint} do not match raydium_clmm pair {}<->{}",
                    config.token_mint_0, config.token_mint_1
                )));
            };
            if config.zero_for_one != expected_zero_for_one {
                return Err(invalid(format!(
                    "legs[{leg_index}] direction {input_mint}->{output_mint} conflicts with raydium_clmm.zero_for_one={}",
                    config.zero_for_one
                )));
            }
        }
        RouteLegExecutionConfig::OrcaSimplePool(_)
        | RouteLegExecutionConfig::RaydiumSimplePool(_) => {}
    }

    Ok(())
}

fn route_validation_error(
    route: &crate::config::RouteConfig,
    error: RouteValidationError,
) -> BootstrapError {
    BootstrapError::InvalidRouteConfig {
        route_id: route.route_id.clone(),
        detail: error.to_string(),
    }
}

fn map_message_mode(mode: &MessageModeConfig) -> MessageMode {
    match mode {
        MessageModeConfig::V0Required => MessageMode::V0Required,
        MessageModeConfig::V0OrLegacy => MessageMode::V0OrLegacy,
    }
}

fn validate_runtime_route_config(route: &crate::config::RouteConfig) -> Result<(), BootstrapError> {
    let value =
        serde_json::to_value(route).map_err(|error| BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!("failed to serialize route config for validation: {error}"),
        })?;

    let mut invalid_paths = Vec::new();
    collect_invalid_route_string_paths("", &value, &mut invalid_paths);
    if !invalid_paths.is_empty() {
        invalid_paths.sort();
        invalid_paths.dedup();

        let total = invalid_paths.len();
        let preview = invalid_paths
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if total > 4 {
            format!(" (+{} more)", total - 4)
        } else {
            String::new()
        };

        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "enabled route contains placeholder or empty string fields: {preview}{suffix}"
            ),
        });
    }

    validate_route_leg_requirements(route)
}

fn validate_route_leg_requirements(
    route: &crate::config::RouteConfig,
) -> Result<(), BootstrapError> {
    let expected_legs = map_route_kind(route.kind).expected_leg_count();
    if route.legs.len() != expected_legs {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "route kind {:?} requires {expected_legs} legs, got {}",
                route.kind,
                route.legs.len()
            ),
        });
    }
    for (index, leg) in route.legs.iter().enumerate() {
        if matches!(&leg.execution, RouteLegExecutionConfig::RaydiumClmm(_))
            && leg.fee_bps.is_none()
        {
            return Err(BootstrapError::InvalidRouteConfig {
                route_id: route.route_id.clone(),
                detail: format!(
                    "legs[{index}].fee_bps is required for raydium_clmm legs because PoolState does not expose the trade fee and the decoder otherwise falls back to zero"
                ),
            });
        }
        if leg.input_mint.is_some() ^ leg.output_mint.is_some() {
            return Err(BootstrapError::InvalidRouteConfig {
                route_id: route.route_id.clone(),
                detail: format!(
                    "legs[{index}] must define both input_mint and output_mint together"
                ),
            });
        }
    }

    Ok(())
}

fn collect_invalid_route_string_paths(
    path: &str,
    value: &serde_json::Value,
    invalid_paths: &mut Vec<String>,
) {
    match value {
        serde_json::Value::String(value) => {
            if route_string_is_invalid(value) {
                invalid_paths.push(path.to_string());
            }
        }
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                let next_path = if path.is_empty() {
                    format!("[{index}]")
                } else {
                    format!("{path}[{index}]")
                };
                collect_invalid_route_string_paths(&next_path, value, invalid_paths);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                let next_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_invalid_route_string_paths(&next_path, value, invalid_paths);
            }
        }
        _ => {}
    }
}

fn route_string_is_invalid(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty() || trimmed.to_ascii_uppercase().contains("REPLACE_")
}

fn route_requires_explicit_sol_quote_conversion(route: &crate::config::RouteConfig) -> bool {
    route.input_mint == route.output_mint && route.input_mint != SOL_MINT
}

fn validate_execution_environment(
    config: &BotConfig,
    routes: &[&crate::config::RouteConfig],
    wallet_owner: &str,
) -> Result<(), BootstrapError> {
    if !config.signing.validate_execution_accounts || routes.is_empty() {
        return Ok(());
    }

    let endpoint = config.reconciliation.rpc_http_endpoint.trim();
    if endpoint.is_empty() {
        return Err(BootstrapError::InvalidConfig {
            detail: "signing.validate_execution_accounts requires reconciliation.rpc_http_endpoint"
                .into(),
        });
    }
    if endpoint.starts_with(MOCK_SCHEME) {
        return Ok(());
    }

    let requirements = collect_required_token_accounts(routes, wallet_owner)?;
    if requirements.is_empty() {
        return Ok(());
    }

    let account_keys = requirements
        .iter()
        .map(|requirement| requirement.address.clone())
        .collect::<Vec<_>>();
    let accounts = fetch_accounts(endpoint, &account_keys).map_err(|detail| {
        BootstrapError::ExecutionEnvironment {
            detail: format!("failed to fetch execution accounts: {detail}"),
        }
    })?;

    let mut failures = Vec::new();
    for requirement in requirements {
        match accounts.get(&requirement.address) {
            None => failures.push(format!(
                "{} missing for {}",
                requirement.address,
                requirement
                    .sources
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            Some(None) => failures.push(format!(
                "{} missing for {}",
                requirement.address,
                requirement
                    .sources
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            Some(Some(account)) => {
                validate_required_token_account(&requirement, account, &mut failures);
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::ExecutionEnvironment {
            detail: failures.join(" | "),
        })
    }
}

fn collect_required_token_accounts(
    routes: &[&crate::config::RouteConfig],
    wallet_owner: &str,
) -> Result<Vec<RequiredTokenAccount>, BootstrapError> {
    let mut requirements = HashMap::<String, RequiredTokenAccount>::new();

    for route in routes {
        let route_token_program = route_primary_token_program(route)?;
        for mint in route_runtime_mints(route) {
            let address = associated_token_address(wallet_owner, &mint, &route_token_program)?;
            insert_required_token_account(
                &mut requirements,
                route,
                RequiredTokenAccount {
                    address,
                    token_program_id: route_token_program.clone(),
                    wallet_owner: wallet_owner.into(),
                    expected_mint: Some(mint),
                    sources: BTreeSet::new(),
                },
                "route_mint_ata",
            )?;
        }

        for leg in &route.legs {
            match &leg.execution {
                RouteLegExecutionConfig::OrcaSimplePool(config) => {
                    insert_required_token_account(
                        &mut requirements,
                        route,
                        RequiredTokenAccount {
                            address: config.user_source_token_account.clone(),
                            token_program_id: config.token_program_id.clone(),
                            wallet_owner: wallet_owner.into(),
                            expected_mint: None,
                            sources: BTreeSet::new(),
                        },
                        "configured_user_source_account",
                    )?;
                    insert_required_token_account(
                        &mut requirements,
                        route,
                        RequiredTokenAccount {
                            address: config.user_destination_token_account.clone(),
                            token_program_id: config.token_program_id.clone(),
                            wallet_owner: wallet_owner.into(),
                            expected_mint: None,
                            sources: BTreeSet::new(),
                        },
                        "configured_user_destination_account",
                    )?;
                }
                RouteLegExecutionConfig::RaydiumSimplePool(config) => {
                    let user_source_address = if let Some(mint) = &config.user_source_mint {
                        associated_token_address(wallet_owner, mint, &config.token_program_id)?
                    } else {
                        config.user_source_token_account.clone().ok_or_else(|| {
                            BootstrapError::InvalidRouteConfig {
                                route_id: route.route_id.clone(),
                                detail: "raydium_simple_pool leg requires user_source_token_account or user_source_mint".into(),
                            }
                        })?
                    };
                    let user_destination_address = if let Some(mint) = &config.user_destination_mint
                    {
                        associated_token_address(wallet_owner, mint, &config.token_program_id)?
                    } else {
                        config.user_destination_token_account.clone().ok_or_else(|| {
                                BootstrapError::InvalidRouteConfig {
                                    route_id: route.route_id.clone(),
                                    detail: "raydium_simple_pool leg requires user_destination_token_account or user_destination_mint".into(),
                                }
                            })?
                    };
                    insert_required_token_account(
                        &mut requirements,
                        route,
                        RequiredTokenAccount {
                            address: user_source_address,
                            token_program_id: config.token_program_id.clone(),
                            wallet_owner: wallet_owner.into(),
                            expected_mint: config.user_source_mint.clone(),
                            sources: BTreeSet::new(),
                        },
                        "configured_user_source_account",
                    )?;
                    insert_required_token_account(
                        &mut requirements,
                        route,
                        RequiredTokenAccount {
                            address: user_destination_address,
                            token_program_id: config.token_program_id.clone(),
                            wallet_owner: wallet_owner.into(),
                            expected_mint: config.user_destination_mint.clone(),
                            sources: BTreeSet::new(),
                        },
                        "configured_user_destination_account",
                    )?;
                }
                RouteLegExecutionConfig::OrcaWhirlpool(config) => {
                    for mint in [&config.token_mint_a, &config.token_mint_b] {
                        let address =
                            associated_token_address(wallet_owner, mint, &config.token_program_id)?;
                        insert_required_token_account(
                            &mut requirements,
                            route,
                            RequiredTokenAccount {
                                address,
                                token_program_id: config.token_program_id.clone(),
                                wallet_owner: wallet_owner.into(),
                                expected_mint: Some(mint.clone()),
                                sources: BTreeSet::new(),
                            },
                            "derived_whirlpool_ata",
                        )?;
                    }
                }
                RouteLegExecutionConfig::RaydiumClmm(config) => {
                    for mint in [&config.token_mint_0, &config.token_mint_1] {
                        let address =
                            associated_token_address(wallet_owner, mint, &config.token_program_id)?;
                        insert_required_token_account(
                            &mut requirements,
                            route,
                            RequiredTokenAccount {
                                address,
                                token_program_id: config.token_program_id.clone(),
                                wallet_owner: wallet_owner.into(),
                                expected_mint: Some(mint.clone()),
                                sources: BTreeSet::new(),
                            },
                            "derived_clmm_ata",
                        )?;
                    }
                }
            }
        }
    }

    Ok(requirements.into_values().collect())
}

fn insert_required_token_account(
    requirements: &mut HashMap<String, RequiredTokenAccount>,
    route: &crate::config::RouteConfig,
    mut requirement: RequiredTokenAccount,
    source: &str,
) -> Result<(), BootstrapError> {
    requirement
        .sources
        .insert(format!("{source}:{}", route.route_id));
    match requirements.get_mut(&requirement.address) {
        Some(existing) => {
            if existing.token_program_id != requirement.token_program_id {
                return Err(BootstrapError::InvalidConfig {
                    detail: format!(
                        "execution account {} has conflicting token programs: {} vs {}",
                        requirement.address,
                        existing.token_program_id,
                        requirement.token_program_id
                    ),
                });
            }
            if existing.wallet_owner != requirement.wallet_owner {
                return Err(BootstrapError::InvalidConfig {
                    detail: format!(
                        "execution account {} has conflicting wallet owners: {} vs {}",
                        requirement.address, existing.wallet_owner, requirement.wallet_owner
                    ),
                });
            }
            match (&existing.expected_mint, &requirement.expected_mint) {
                (Some(left), Some(right)) if left != right => {
                    return Err(BootstrapError::InvalidConfig {
                        detail: format!(
                            "execution account {} has conflicting expected mints: {} vs {}",
                            requirement.address, left, right
                        ),
                    });
                }
                (None, Some(mint)) => existing.expected_mint = Some(mint.clone()),
                _ => {}
            }
            existing.sources.extend(requirement.sources);
        }
        None => {
            requirements.insert(requirement.address.clone(), requirement);
        }
    }
    Ok(())
}

fn route_runtime_mints(route: &crate::config::RouteConfig) -> Vec<String> {
    let mut mints = BTreeSet::new();
    mints.insert(route.input_mint.clone());
    mints.insert(route.output_mint.clone());
    if let Some(mint) = &route.base_mint {
        mints.insert(mint.clone());
    }
    if let Some(mint) = &route.quote_mint {
        mints.insert(mint.clone());
    }
    for leg in &route.legs {
        if let Some(mint) = &leg.input_mint {
            mints.insert(mint.clone());
        }
        if let Some(mint) = &leg.output_mint {
            mints.insert(mint.clone());
        }
    }
    mints
        .into_iter()
        .filter(|mint| parse_pubkey(mint).is_ok())
        .collect()
}

fn route_primary_token_program(
    route: &crate::config::RouteConfig,
) -> Result<String, BootstrapError> {
    let token_programs = route
        .legs
        .iter()
        .map(|leg| match &leg.execution {
            RouteLegExecutionConfig::OrcaSimplePool(config) => config.token_program_id.clone(),
            RouteLegExecutionConfig::OrcaWhirlpool(config) => config.token_program_id.clone(),
            RouteLegExecutionConfig::RaydiumSimplePool(config) => config.token_program_id.clone(),
            RouteLegExecutionConfig::RaydiumClmm(config) => config.token_program_id.clone(),
        })
        .collect::<BTreeSet<_>>();

    if token_programs.len() != 1 {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "route uses conflicting token_program_id values across legs; wallet ATA validation is ambiguous".into(),
        });
    }

    token_programs
        .into_iter()
        .next()
        .ok_or_else(|| BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "route does not expose a token_program_id".into(),
        })
}

fn associated_token_address(
    owner: &str,
    mint: &str,
    token_program: &str,
) -> Result<String, BootstrapError> {
    let owner = parse_pubkey(owner).map_err(|detail| BootstrapError::InvalidConfig {
        detail: format!("invalid wallet owner pubkey {owner}: {detail}"),
    })?;
    let mint = parse_pubkey(mint).map_err(|detail| BootstrapError::InvalidConfig {
        detail: format!("invalid mint pubkey {mint}: {detail}"),
    })?;
    let token_program =
        parse_pubkey(token_program).map_err(|detail| BootstrapError::InvalidConfig {
            detail: format!("invalid token program pubkey {token_program}: {detail}"),
        })?;
    Ok(Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).expect("associated token program id"),
    )
    .0
    .to_string())
}

fn parse_pubkey(value: &str) -> Result<Pubkey, String> {
    Pubkey::from_str(value).map_err(|error| error.to_string())
}

fn fetch_accounts(
    endpoint: &str,
    account_keys: &[String],
) -> Result<HashMap<String, Option<RpcAccountInfo>>, String> {
    let http = Client::builder()
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_millis(1_000))
        .build()
        .map_err(|error| error.to_string())?;
    let mut accounts = HashMap::with_capacity(account_keys.len());
    for chunk in account_keys.chunks(100) {
        let response = rpc_call::<RpcMultipleAccountsResponse>(
            &http,
            endpoint,
            "getMultipleAccounts",
            json!([
                chunk,
                { "encoding": "base64", "commitment": "processed" }
            ]),
        )
        .map_err(|error| error.to_string())?;
        for (account_key, account) in chunk.iter().zip(response.value.into_iter()) {
            accounts.insert(account_key.clone(), account);
        }
    }
    Ok(accounts)
}

fn validate_required_token_account(
    requirement: &RequiredTokenAccount,
    account: &RpcAccountInfo,
    failures: &mut Vec<String>,
) {
    if account.owner != requirement.token_program_id {
        failures.push(format!(
            "{} owner program mismatch: expected {}, got {}",
            requirement.address, requirement.token_program_id, account.owner
        ));
        return;
    }

    match parse_token_account(account) {
        Ok(parsed) => {
            if parsed.owner != requirement.wallet_owner {
                failures.push(format!(
                    "{} token owner mismatch: expected {}, got {}",
                    requirement.address, requirement.wallet_owner, parsed.owner
                ));
            }
            if let Some(expected_mint) = &requirement.expected_mint
                && &parsed.mint != expected_mint
            {
                failures.push(format!(
                    "{} mint mismatch: expected {}, got {}",
                    requirement.address, expected_mint, parsed.mint
                ));
            }
        }
        Err(detail) => failures.push(format!(
            "{} invalid token account: {detail}",
            requirement.address
        )),
    }
}

fn parse_token_account(account: &RpcAccountInfo) -> Result<ParsedTokenAccount, String> {
    if account.data.1 != "base64" {
        return Err(format!("unsupported account encoding {}", account.data.1));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(account.data.0.as_bytes())
        .map_err(|error| error.to_string())?;
    if bytes.len() < SPL_TOKEN_ACCOUNT_LEN {
        return Err(format!(
            "account data too short: expected at least {SPL_TOKEN_ACCOUNT_LEN} bytes, got {}",
            bytes.len()
        ));
    }
    if bytes[SPL_TOKEN_ACCOUNT_STATE_OFFSET] != 1 {
        return Err(format!(
            "token account is not initialized (state={})",
            bytes[SPL_TOKEN_ACCOUNT_STATE_OFFSET]
        ));
    }

    let mint = Pubkey::try_from(&bytes[0..32])
        .map_err(|error| error.to_string())?
        .to_string();
    let owner = Pubkey::try_from(&bytes[32..64])
        .map_err(|error| error.to_string())?
        .to_string();
    Ok(ParsedTokenAccount { mint, owner })
}

fn build_tracked_pool_pairs(
    routes: &[&crate::config::RouteConfig],
) -> HashMap<String, (String, String)> {
    let mut pairs = HashMap::new();
    for route in routes {
        for leg in &route.legs {
            pairs
                .entry(leg.pool_id.clone())
                .or_insert_with(|| tracked_pool_pair(route, leg));
        }
    }
    pairs
}

fn tracked_pool_pair(
    route: &crate::config::RouteConfig,
    leg: &crate::config::RouteLegConfig,
) -> (String, String) {
    match &leg.execution {
        RouteLegExecutionConfig::OrcaSimplePool(_)
        | RouteLegExecutionConfig::RaydiumSimplePool(_) => {
            let (input_mint, output_mint) =
                resolve_route_leg_mints(route, map_route_kind(route.kind), leg, 0)
                    .unwrap_or_else(|_| (route.input_mint.clone(), route.output_mint.clone()));
            if matches!(leg.side, SwapSideConfig::BuyBase) {
                (output_mint, input_mint)
            } else {
                (input_mint, output_mint)
            }
        }
        RouteLegExecutionConfig::OrcaWhirlpool(config) => {
            (config.token_mint_a.clone(), config.token_mint_b.clone())
        }
        RouteLegExecutionConfig::RaydiumClmm(config) => {
            (config.token_mint_0.clone(), config.token_mint_1.clone())
        }
    }
}

fn resolve_sol_quote_conversion_pool_id(
    route: &crate::config::RouteConfig,
    tracked_pool_pairs: &HashMap<String, (String, String)>,
) -> Result<Option<PoolId>, BootstrapError> {
    if !route_requires_explicit_sol_quote_conversion(route) {
        return Ok(None);
    }

    let Some(quote_mint) = route.quote_mint.as_deref() else {
        return Ok(None);
    };
    let Some(pool_id) = route.sol_quote_conversion_pool_id.as_deref() else {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "sol_quote_conversion_pool_id is required when quote_mint != SOL".into(),
        });
    };
    let Some((token_mint_a, token_mint_b)) = tracked_pool_pairs.get(pool_id) else {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "sol_quote_conversion_pool_id must reference a pool_id from an enabled route leg: {pool_id}"
            ),
        });
    };
    let pair_matches = (token_mint_a == SOL_MINT && token_mint_b == quote_mint)
        || (token_mint_a == quote_mint && token_mint_b == SOL_MINT);
    if !pair_matches {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: format!(
                "sol_quote_conversion_pool_id must reference a tracked SOL/{quote_mint} pool: {pool_id}"
            ),
        });
    }

    Ok(Some(PoolId(pool_id.into())))
}

fn resolve_min_trade_size(route: &crate::config::RouteConfig) -> Result<u64, BootstrapError> {
    if route.default_trade_size == 0 {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "default_trade_size must be greater than zero".into(),
        });
    }
    if route.default_trade_size > route.max_trade_size {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "default_trade_size must be less than or equal to max_trade_size".into(),
        });
    }
    let min_trade_size = route.min_trade_size.unwrap_or(route.default_trade_size);
    if min_trade_size == 0 {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "min_trade_size must be greater than zero when provided".into(),
        });
    }
    if min_trade_size > route.default_trade_size {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "min_trade_size must be less than or equal to default_trade_size".into(),
        });
    }
    if min_trade_size > route.max_trade_size {
        return Err(BootstrapError::InvalidRouteConfig {
            route_id: route.route_id.clone(),
            detail: "min_trade_size must be less than or equal to max_trade_size".into(),
        });
    }
    Ok(min_trade_size)
}

fn estimate_execution_cost_lamports(
    route: &crate::config::RouteConfig,
    builder: &BuilderConfig,
    route_compute_unit_limit: u32,
    route_compute_unit_price_micro_lamports: u64,
    route_jito_tip_lamports: u64,
) -> u64 {
    if route.input_mint != route.output_mint {
        return 0;
    }

    let compute_unit_limit = effective_u32(builder.compute_unit_limit, route_compute_unit_limit);
    let compute_unit_price_micro_lamports = effective_u64(
        builder.compute_unit_price_micro_lamports,
        route_compute_unit_price_micro_lamports,
    );
    let jito_tip_lamports = effective_u64(builder.jito_tip_lamports, route_jito_tip_lamports);
    let priority_fee_lamports = (compute_unit_limit as u64)
        .saturating_mul(compute_unit_price_micro_lamports)
        .saturating_add(MICRO_LAMPORTS_PER_LAMPORT - 1)
        / MICRO_LAMPORTS_PER_LAMPORT;
    BASE_FEE_LAMPORTS_PER_SIGNATURE
        .saturating_add(priority_fee_lamports)
        .saturating_add(jito_tip_lamports)
}

fn effective_u32(value: u32, default_value: u32) -> u32 {
    if value == 0 { default_value } else { value }
}

fn effective_u64(value: u64, default_value: u64) -> u64 {
    if value == 0 { default_value } else { value }
}

const MIN_EXECUTION_PROTECTION_QUOTE_SLOT_LAG: u64 = 24;

fn map_execution_protection(
    config: &crate::config::RouteExecutionProtectionConfig,
) -> Option<ExecutionProtectionPolicy> {
    config.enabled.then_some(ExecutionProtectionPolicy {
        tight_max_quote_slot_lag: config.tight_max_quote_slot_lag,
        base_extra_buy_leg_slippage_bps: config.base_extra_buy_leg_slippage_bps,
        failure_step_bps: config.failure_step_bps,
        max_extra_buy_leg_slippage_bps: config.max_extra_buy_leg_slippage_bps,
        recovery_success_count: config.recovery_success_count,
    })
}

fn effective_max_quote_slot_lag(route: &crate::config::RouteConfig) -> u64 {
    let configured = route.execution.max_quote_slot_lag;
    let protection = &route.execution_protection;
    if !protection.enabled || protection.tight_max_quote_slot_lag == 0 {
        configured
    } else {
        configured.min(
            protection
                .tight_max_quote_slot_lag
                .max(MIN_EXECUTION_PROTECTION_QUOTE_SLOT_LAG),
        )
    }
}

fn map_leg_execution(config: &RouteLegExecutionConfig) -> VenueExecutionConfig {
    match config {
        RouteLegExecutionConfig::OrcaSimplePool(config) => {
            VenueExecutionConfig::OrcaSimplePool(OrcaSimplePoolConfig {
                program_id: config.program_id.clone(),
                token_program_id: config.token_program_id.clone(),
                swap_account: config.swap_account.clone(),
                authority: config.authority.clone(),
                pool_source_token_account: config.pool_source_token_account.clone(),
                pool_destination_token_account: config.pool_destination_token_account.clone(),
                pool_mint: config.pool_mint.clone(),
                fee_account: config.fee_account.clone(),
                user_source_token_account: config.user_source_token_account.clone(),
                user_destination_token_account: config.user_destination_token_account.clone(),
                host_fee_account: config.host_fee_account.clone(),
            })
        }
        RouteLegExecutionConfig::OrcaWhirlpool(config) => {
            VenueExecutionConfig::OrcaWhirlpool(OrcaWhirlpoolConfig {
                program_id: config.program_id.clone(),
                token_program_id: config.token_program_id.clone(),
                whirlpool: config.whirlpool.clone(),
                token_mint_a: config.token_mint_a.clone(),
                token_vault_a: config.token_vault_a.clone(),
                token_mint_b: config.token_mint_b.clone(),
                token_vault_b: config.token_vault_b.clone(),
                tick_spacing: config.tick_spacing,
                a_to_b: config.a_to_b,
            })
        }
        RouteLegExecutionConfig::RaydiumSimplePool(config) => {
            VenueExecutionConfig::RaydiumSimplePool(RaydiumSimplePoolConfig {
                program_id: config.program_id.clone(),
                token_program_id: config.token_program_id.clone(),
                amm_pool: config.amm_pool.clone(),
                amm_authority: config.amm_authority.clone(),
                amm_open_orders: config.amm_open_orders.clone(),
                amm_coin_vault: config.amm_coin_vault.clone(),
                amm_pc_vault: config.amm_pc_vault.clone(),
                market_program: config.market_program.clone(),
                market: config.market.clone(),
                market_bids: config.market_bids.clone(),
                market_asks: config.market_asks.clone(),
                market_event_queue: config.market_event_queue.clone(),
                market_coin_vault: config.market_coin_vault.clone(),
                market_pc_vault: config.market_pc_vault.clone(),
                market_vault_signer: config.market_vault_signer.clone(),
                user_source_token_account: config.user_source_token_account.clone(),
                user_destination_token_account: config.user_destination_token_account.clone(),
                user_source_mint: config.user_source_mint.clone(),
                user_destination_mint: config.user_destination_mint.clone(),
            })
        }
        RouteLegExecutionConfig::RaydiumClmm(config) => {
            VenueExecutionConfig::RaydiumClmm(RaydiumClmmConfig {
                program_id: config.program_id.clone(),
                token_program_id: config.token_program_id.clone(),
                token_program_2022_id: config.token_program_2022_id.clone(),
                memo_program_id: config.memo_program_id.clone(),
                pool_state: config.pool_state.clone(),
                amm_config: config.amm_config.clone(),
                observation_state: config.observation_state.clone(),
                ex_bitmap_account: config.ex_bitmap_account.clone(),
                token_mint_0: config.token_mint_0.clone(),
                token_vault_0: config.token_vault_0.clone(),
                token_mint_1: config.token_mint_1.clone(),
                token_vault_1: config.token_vault_1.clone(),
                tick_spacing: config.tick_spacing,
                zero_for_one: config.zero_for_one,
            })
        }
    }
}

fn resolve_signer(config: &SigningConfig) -> Result<ResolvedSigner, SigningError> {
    match config.provider {
        SigningProviderKind::Local => {
            let signer = signing::LocalWalletSigner::new(
                config.wallet_id.clone(),
                config.keypair_path.clone(),
                config.keypair_base58.clone(),
            );
            let owner_pubkey = if config.owner_pubkey.is_empty() {
                signer.pubkey_string().unwrap_or_default()
            } else {
                config.owner_pubkey.clone()
            };
            let signer_available = signer.is_available();
            Ok(ResolvedSigner {
                owner_pubkey,
                signer_available,
                signer: Arc::new(signer),
            })
        }
        SigningProviderKind::SecureUnix => {
            if config.keypair_path.is_some() || config.keypair_base58.is_some() {
                return Err(SigningError::InvalidSignerConfig(
                    "secure_unix provider does not accept signing.keypair_path or signing.keypair_base58"
                        .into(),
                ));
            }
            let socket_path = config.socket_path.clone().ok_or_else(|| {
                SigningError::InvalidSignerConfig(
                    "signing.socket_path is required for secure_unix provider".into(),
                )
            })?;
            let expected_pubkey = if config.owner_pubkey.is_empty() {
                None
            } else {
                Some(config.owner_pubkey.clone())
            };
            let signer = signing::SecureUnixWalletSigner::new(
                config.wallet_id.clone(),
                socket_path,
                expected_pubkey,
                config.connect_timeout_ms,
                config.read_timeout_ms,
            )?;
            let owner_pubkey = if config.owner_pubkey.is_empty() {
                signer.pubkey_string()?
            } else {
                config.owner_pubkey.clone()
            };
            Ok(ResolvedSigner {
                owner_pubkey,
                signer_available: signer.is_available(),
                signer: Arc::new(signer),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        str::FromStr,
        sync::Arc,
        thread,
    };

    use base64::Engine;
    use builder::{
        AtomicArbTransactionBuilder, BuildRequest, BuildStatus, DynamicBuildParameters,
        ExecutionRegistry, LookupTableUsageConfig, MessageFormat,
        RouteExecutionConfig as BuilderRouteExecutionConfig, TransactionBuilder,
    };
    use serde_json::{Value, json};
    use solana_address_lookup_table_interface::{
        program as alt_program,
        state::{LOOKUP_TABLE_META_SIZE, LookupTableMeta, ProgramState},
    };
    use solana_sdk::{
        hash::hashv,
        pubkey::Pubkey,
        signer::{Signer as SolanaCurveSigner, keypair::Keypair},
    };
    use state::types::{LookupTableSnapshot, PoolId, RouteId};
    use strategy::{
        opportunity::{CandidateSelectionSource, OpportunityCandidate},
        quote::LegQuote,
        route_registry::RouteLegSequence,
    };

    use super::{
        BootstrapError, bootstrap, effective_max_quote_slot_lag, map_leg_execution,
        map_message_mode, map_route_kind, map_swap_side, route_enabled_in_profile,
    };
    use crate::config::{
        BotConfig, LookupTableConfig, MessageModeConfig, OrcaSimplePoolLegExecutionConfig,
        OrcaWhirlpoolLegExecutionConfig, RaydiumClmmLegExecutionConfig,
        RaydiumSimplePoolLegExecutionConfig, RouteClassConfig, RouteConfig, RouteExecutionConfig,
        RouteKindConfig, RouteLegConfig, RouteLegExecutionConfig, RoutesConfig, SwapSideConfig,
    };

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
    }

    fn signing_material() -> (String, String) {
        let keypair = Keypair::new_from_array([7; 32]);
        (keypair.pubkey().to_string(), keypair.to_base58_string())
    }

    fn route_execution() -> RouteExecutionConfig {
        RouteExecutionConfig {
            message_mode: MessageModeConfig::V0Required,
            lookup_tables: Vec::new(),
            default_compute_unit_limit: 300_000,
            minimum_compute_unit_limit: 0,
            default_compute_unit_price_micro_lamports: 25_000,
            default_jito_tip_lamports: 5_000,
            max_quote_slot_lag: 4,
            max_alt_slot_lag: 4,
        }
    }

    fn valid_route_config() -> RouteConfig {
        RouteConfig {
            enabled: true,
            route_class: RouteClassConfig::AmmFastPath,
            kind: RouteKindConfig::TwoLeg,
            route_id: "route-a".into(),
            input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            base_mint: Some("So11111111111111111111111111111111111111112".into()),
            quote_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
            sol_quote_conversion_pool_id: Some("pool-a".into()),
            min_profit_quote_atoms: None,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            min_trade_size: None,
            size_ladder: Vec::new(),
            sizing: Default::default(),
            execution_protection: Default::default(),
            legs: vec![
                RouteLegConfig {
                    venue: "orca".into(),
                    pool_id: "pool-a".into(),
                    side: SwapSideConfig::BuyBase,
                    input_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                    output_mint: Some("So11111111111111111111111111111111111111112".into()),
                    fee_bps: None,
                    execution: RouteLegExecutionConfig::OrcaSimplePool(
                        OrcaSimplePoolLegExecutionConfig {
                            program_id: test_pubkey("orca-program"),
                            token_program_id: test_pubkey("spl-token-program"),
                            swap_account: test_pubkey("orca-swap"),
                            authority: test_pubkey("orca-authority"),
                            pool_source_token_account: test_pubkey("orca-pool-source"),
                            pool_destination_token_account: test_pubkey("orca-pool-destination"),
                            pool_mint: test_pubkey("orca-pool-mint"),
                            fee_account: test_pubkey("orca-fee-account"),
                            user_source_token_account: test_pubkey("route-input-ata"),
                            user_destination_token_account: test_pubkey("route-mid-ata"),
                            host_fee_account: None,
                        },
                    ),
                },
                RouteLegConfig {
                    venue: "raydium".into(),
                    pool_id: "pool-b".into(),
                    side: SwapSideConfig::SellBase,
                    input_mint: Some("So11111111111111111111111111111111111111112".into()),
                    output_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                    fee_bps: None,
                    execution: RouteLegExecutionConfig::RaydiumSimplePool(
                        RaydiumSimplePoolLegExecutionConfig {
                            program_id: test_pubkey("raydium-program"),
                            token_program_id: test_pubkey("spl-token-program"),
                            amm_pool: test_pubkey("raydium-amm-pool"),
                            amm_authority: test_pubkey("raydium-amm-authority"),
                            amm_open_orders: test_pubkey("raydium-open-orders"),
                            amm_coin_vault: test_pubkey("raydium-coin-vault"),
                            amm_pc_vault: test_pubkey("raydium-pc-vault"),
                            market_program: test_pubkey("serum-program"),
                            market: test_pubkey("serum-market"),
                            market_bids: test_pubkey("serum-bids"),
                            market_asks: test_pubkey("serum-asks"),
                            market_event_queue: test_pubkey("serum-event-queue"),
                            market_coin_vault: test_pubkey("serum-coin-vault"),
                            market_pc_vault: test_pubkey("serum-pc-vault"),
                            market_vault_signer: test_pubkey("serum-vault-signer"),
                            user_source_token_account: Some(test_pubkey("route-mid-ata")),
                            user_destination_token_account: Some(test_pubkey("route-output-ata")),
                            user_source_mint: None,
                            user_destination_mint: None,
                        },
                    ),
                },
            ],
            account_dependencies: Vec::new(),
            execution: route_execution(),
        }
    }

    fn triangular_route_config() -> RouteConfig {
        let mut route = valid_route_config();
        route.kind = RouteKindConfig::Triangular;
        route.route_id = "route-tri".into();
        route.execution.default_compute_unit_limit = 450_000;
        route.execution.minimum_compute_unit_limit = 420_000;

        route.legs[1].pool_id = "pool-b-tri".into();
        route.legs[1].input_mint = Some("So11111111111111111111111111111111111111112".into());
        route.legs[1].output_mint = Some("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".into());
        route.legs[1].execution =
            RouteLegExecutionConfig::RaydiumSimplePool(RaydiumSimplePoolLegExecutionConfig {
                program_id: test_pubkey("tri-raydium-program"),
                token_program_id: test_pubkey("spl-token-program"),
                amm_pool: test_pubkey("tri-raydium-amm-pool"),
                amm_authority: test_pubkey("tri-raydium-amm-authority"),
                amm_open_orders: test_pubkey("tri-raydium-open-orders"),
                amm_coin_vault: test_pubkey("tri-raydium-coin-vault"),
                amm_pc_vault: test_pubkey("tri-raydium-pc-vault"),
                market_program: test_pubkey("tri-serum-program"),
                market: test_pubkey("tri-serum-market"),
                market_bids: test_pubkey("tri-serum-bids"),
                market_asks: test_pubkey("tri-serum-asks"),
                market_event_queue: test_pubkey("tri-serum-event-queue"),
                market_coin_vault: test_pubkey("tri-serum-coin-vault"),
                market_pc_vault: test_pubkey("tri-serum-pc-vault"),
                market_vault_signer: test_pubkey("tri-serum-vault-signer"),
                user_source_token_account: Some(test_pubkey("tri-route-mid-a-ata")),
                user_destination_token_account: Some(test_pubkey("tri-route-mid-b-ata")),
                user_source_mint: None,
                user_destination_mint: None,
            });
        route.legs.push(RouteLegConfig {
            venue: "orca".into(),
            pool_id: "pool-c-tri".into(),
            side: SwapSideConfig::SellBase,
            input_mint: Some("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".into()),
            output_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
            fee_bps: None,
            execution: RouteLegExecutionConfig::OrcaSimplePool(OrcaSimplePoolLegExecutionConfig {
                program_id: test_pubkey("tri-orca-program-c"),
                token_program_id: test_pubkey("spl-token-program"),
                swap_account: test_pubkey("tri-orca-swap-c"),
                authority: test_pubkey("tri-orca-authority-c"),
                pool_source_token_account: test_pubkey("tri-orca-pool-source-c"),
                pool_destination_token_account: test_pubkey("tri-orca-pool-destination-c"),
                pool_mint: test_pubkey("tri-orca-pool-mint-c"),
                fee_account: test_pubkey("tri-orca-fee-account-c"),
                user_source_token_account: test_pubkey("tri-route-mid-b-ata"),
                user_destination_token_account: test_pubkey("tri-route-output-ata"),
                host_fee_account: None,
            }),
        });
        route
            .account_dependencies
            .push(crate::config::AccountDependencyConfig {
                account_key: "acct-c".into(),
                pool_id: "pool-c-tri".into(),
                decoder_key: "pool-price-v1".into(),
            });
        route
    }

    fn test_config(route: RouteConfig) -> BotConfig {
        let mut config = BotConfig::default();
        let (owner_pubkey, keypair_base58) = signing_material();
        config.signing.owner_pubkey = owner_pubkey;
        config.signing.keypair_base58 = Some(keypair_base58);
        config.runtime.refresh.enabled = false;
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        config.jito.endpoint = "mock://jito".into();
        config.routes = RoutesConfig {
            definitions: vec![route],
        };
        config
    }

    fn concentrated_route_config() -> RouteConfig {
        let base_mint = test_pubkey("mint-base");
        let quote_mint = "So11111111111111111111111111111111111111112".to_string();
        RouteConfig {
            enabled: true,
            route_class: RouteClassConfig::AmmFastPath,
            kind: RouteKindConfig::TwoLeg,
            route_id: "route-clmm".into(),
            input_mint: quote_mint.clone(),
            output_mint: quote_mint.clone(),
            base_mint: Some(base_mint.clone()),
            quote_mint: Some(quote_mint.clone()),
            sol_quote_conversion_pool_id: None,
            min_profit_quote_atoms: None,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            min_trade_size: None,
            size_ladder: Vec::new(),
            sizing: Default::default(),
            execution_protection: Default::default(),
            legs: vec![
                RouteLegConfig {
                    venue: "orca_whirlpool".into(),
                    pool_id: "pool-whirlpool".into(),
                    side: SwapSideConfig::BuyBase,
                    input_mint: Some(quote_mint.clone()),
                    output_mint: Some(base_mint.clone()),
                    fee_bps: Some(30),
                    execution: RouteLegExecutionConfig::OrcaWhirlpool(
                        OrcaWhirlpoolLegExecutionConfig {
                            program_id: test_pubkey("orca-whirlpool-program"),
                            token_program_id: test_pubkey("spl-token-program"),
                            whirlpool: test_pubkey("whirlpool"),
                            token_mint_a: base_mint.clone(),
                            token_vault_a: test_pubkey("vault-a"),
                            token_mint_b: quote_mint.clone(),
                            token_vault_b: test_pubkey("vault-b"),
                            tick_spacing: 64,
                            a_to_b: false,
                        },
                    ),
                },
                RouteLegConfig {
                    venue: "raydium_clmm".into(),
                    pool_id: "pool-clmm".into(),
                    side: SwapSideConfig::SellBase,
                    input_mint: Some(base_mint.clone()),
                    output_mint: Some(quote_mint.clone()),
                    fee_bps: Some(30),
                    execution: RouteLegExecutionConfig::RaydiumClmm(
                        RaydiumClmmLegExecutionConfig {
                            program_id: test_pubkey("raydium-clmm-program"),
                            token_program_id: test_pubkey("spl-token-program"),
                            token_program_2022_id: test_pubkey("token-2022-program"),
                            memo_program_id: test_pubkey("memo-program"),
                            pool_state: test_pubkey("pool-state"),
                            amm_config: test_pubkey("amm-config"),
                            observation_state: test_pubkey("observation-state"),
                            ex_bitmap_account: Some(test_pubkey("ex-bitmap")),
                            token_mint_0: base_mint.clone(),
                            token_vault_0: test_pubkey("vault-0"),
                            token_mint_1: quote_mint.clone(),
                            token_vault_1: test_pubkey("vault-1"),
                            tick_spacing: 64,
                            zero_for_one: true,
                        },
                    ),
                },
            ],
            account_dependencies: Vec::new(),
            execution: route_execution(),
        }
    }

    fn associated_token_address_for_test(owner: &str, mint: &str, token_program: &str) -> String {
        super::associated_token_address(owner, mint, token_program)
            .expect("associated token address")
    }

    fn token_account_data(mint: &str, owner: &str) -> String {
        let mut bytes = vec![0u8; super::SPL_TOKEN_ACCOUNT_LEN];
        bytes[..32].copy_from_slice(&Pubkey::from_str(mint).expect("mint").to_bytes());
        bytes[32..64].copy_from_slice(&Pubkey::from_str(owner).expect("owner").to_bytes());
        bytes[super::SPL_TOKEN_ACCOUNT_STATE_OFFSET] = 1;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn rpc_token_account(owner_program: &str, mint: &str, owner: &str) -> Value {
        json!({
            "owner": owner_program,
            "data": [token_account_data(mint, owner), "base64"]
        })
    }

    fn rpc_lookup_table_account(addresses: &[Pubkey], last_extended_slot: u64) -> Value {
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
        json!({
            "owner": alt_program::id().to_string(),
            "lamports": 0,
            "data": [base64::engine::general_purpose::STANDARD.encode(raw), "base64"]
        })
    }

    fn mock_latest_blockhash_response(slot: u64) -> Value {
        json!({
            "result": {
                "context": { "slot": slot },
                "value": {
                    "blockhash": recent_blockhash(),
                    "lastValidBlockHeight": 1
                }
            }
        })
    }

    fn mock_balance_response(slot: u64, lamports: u64) -> Value {
        json!({
            "result": {
                "context": { "slot": slot },
                "value": lamports
            }
        })
    }

    fn spawn_mock_rpc_server<F>(handler: F) -> String
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock rpc server");
        let address = listener.local_addr().expect("mock rpc address");
        let handler = Arc::new(handler);

        thread::spawn(move || {
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

            if let Some(end) = header_end
                && request.len() >= end + content_length
            {
                let body = &request[end..end + content_length];
                return String::from_utf8_lossy(body).into_owned();
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

    fn repo_root_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(file)
    }

    fn fee_payer_pubkey() -> String {
        Keypair::new_from_array([9; 32]).pubkey().to_string()
    }

    fn recent_blockhash() -> String {
        hashv(&[b"bootstrap-builder-test-blockhash"]).to_string()
    }

    fn execution_registry_from_config(config: &BotConfig) -> ExecutionRegistry {
        let mut registry = ExecutionRegistry::default();
        for route in &config.routes.definitions {
            if !route_enabled_in_profile(route, config.runtime.profile) {
                continue;
            }

            registry.register(BuilderRouteExecutionConfig {
                route_id: RouteId(route.route_id.clone()),
                kind: map_route_kind(route.kind),
                message_mode: map_message_mode(&route.execution.message_mode),
                lookup_tables: route
                    .execution
                    .lookup_tables
                    .iter()
                    .map(|table| LookupTableUsageConfig {
                        account_key: table.account_key.clone(),
                    })
                    .collect(),
                default_compute_unit_limit: route.execution.default_compute_unit_limit,
                minimum_compute_unit_limit: route
                    .execution
                    .minimum_compute_unit_limit
                    .max(map_route_kind(route.kind).minimum_compute_unit_limit()),
                default_compute_unit_price_micro_lamports: route
                    .execution
                    .default_compute_unit_price_micro_lamports,
                default_jito_tip_lamports: route.execution.default_jito_tip_lamports,
                max_quote_slot_lag: effective_max_quote_slot_lag(route),
                max_alt_slot_lag: route.execution.max_alt_slot_lag,
                legs: RouteLegSequence::from_vec(
                    route
                        .legs
                        .iter()
                        .map(|leg| map_leg_execution(&leg.execution))
                        .collect(),
                )
                .expect("test route should have valid leg count"),
            });
        }
        registry
    }

    fn lookup_table_snapshot(account_key: &str) -> LookupTableSnapshot {
        let addresses = match account_key {
            "G66u95YwxUKLibnT4ZenvPhMXWVqupLX6SNLBDdzH7o8" => vec![
                "JVoPtWWDsRcLvQosu5fWc2CaNF6jEtJzbxdPtcEuvZo",
                "J56JoCEHyCG3P5fCKdB9S78NLBXY6T4UfxiCwRNh8gGP",
                "GQYCUrerE5tH6Wx1QACTmsf8Jp9grViidEgx7m3r1Jw7",
                "So11111111111111111111111111111111111111112",
                "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
                "5cerU1uk6iPzndEK8AZguamz52e75am4NRXWrL9C19Bw",
                "GtDzTPATXEuGHrAb4seoW7HR9PkJMDxkPXwLWoqEkGNf",
            ],
            "CaAB3ULQ5iYw6MQZqrzca6oY4J8UNznod2EkS8s1bfuX" => vec![
                "GQsPr4RJk9AZkkfWHud7v4MtotcxhaYzZHdsPCg9vNvW",
                "B39UjYJDygUfufZfwGa2BVbSnLLw9qredg3wDWarMAUx",
                "HcyQbUni89pmghZMG29SNLA8wFq3waamCXQHNVATVUer",
                "So11111111111111111111111111111111111111112",
                "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
                "7bDUUvfSXFQAomGc4W9pUQt1UVrF485TVUP9iSM4d4u1",
                "3DZg4BbkBj8GKEXXXRtu6wUunpCAth6n8gZur9Dronvt",
            ],
            "2abC3nxk29LkoeyEwxziaKoMyHFxdzQCycUSXTB2xMCu" => vec![
                "7XzVsjqTebULfkUofTDH5gDdZDmxacPmPuTfHa1n9kuh",
                "AziwKtNZrKGLW4qBHtTUgRTUx1toW635UKAS74BN6VX7",
                "9VcbEtYfsnwcdtECY4jpNZAcAsioofYmK8BPiVpybnsQ",
                "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "HRN5wT735QT8Lf5symMkR22mUfRPJFseVUfRytfmmj6q",
                "GMeHiG3h9udQMwcHtE8GVNKi71nTEGL9m32HtRZn8JLR",
            ],
            other => panic!("unsupported lookup table snapshot for {other}"),
        };

        LookupTableSnapshot {
            account_key: account_key.into(),
            addresses: addresses.into_iter().map(str::to_string).collect(),
            last_extended_slot: 42,
            fetched_slot: 43,
        }
    }

    fn dynamic_params_for_route(route: &RouteConfig) -> DynamicBuildParameters {
        DynamicBuildParameters {
            recent_blockhash: recent_blockhash(),
            recent_blockhash_slot: Some(43),
            head_slot: 43,
            fee_payer_pubkey: fee_payer_pubkey(),
            compute_unit_limit: 300_000,
            compute_unit_price_micro_lamports: 25_000,
            jito_tip_lamports: 5_000,
            resolved_lookup_tables: route
                .execution
                .lookup_tables
                .iter()
                .map(|table| lookup_table_snapshot(&table.account_key))
                .collect(),
        }
    }

    fn candidate_for_route(route: &RouteConfig) -> OpportunityCandidate {
        let mut input_amount = route.default_trade_size;
        let mut leg_quotes = Vec::with_capacity(route.legs.len());
        let mut intermediate_output_amounts =
            Vec::with_capacity(route.legs.len().saturating_sub(1));

        for (index, leg) in route.legs.iter().enumerate() {
            let output_amount = input_amount.saturating_add(
                route.default_trade_size / 20 + u64::try_from(index).unwrap_or(0) + 1,
            );
            leg_quotes.push(LegQuote {
                venue: leg.venue.clone(),
                pool_id: PoolId(leg.pool_id.clone()),
                side: map_swap_side(&leg.side),
                input_amount,
                output_amount,
                fee_paid: 0,
                current_tick_index: current_tick_index_hint(&leg.execution),
            });
            input_amount = output_amount;
            if index + 1 < route.legs.len() {
                intermediate_output_amounts.push(output_amount);
            }
        }

        OpportunityCandidate {
            route_id: RouteId(route.route_id.clone()),
            route_kind: map_route_kind(route.kind),
            quoted_slot: 42,
            leg_snapshot_slots: RouteLegSequence::from_vec(vec![42; route.legs.len()])
                .expect("test route should have valid leg count"),
            sol_quote_conversion_snapshot_slot: None,
            trade_size: route.default_trade_size,
            active_execution_buffer_bps: None,
            expected_net_output: input_amount,
            minimum_acceptable_output: route.default_trade_size.saturating_add(1),
            expected_gross_profit_quote_atoms: 1,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: 1,
            selected_by: CandidateSelectionSource::Legacy,
            ranking_score_quote_atoms: 1,
            expected_value_quote_atoms: 1,
            p_land_bps: 10_000,
            expected_shortfall_quote_atoms: 0,
            intermediate_output_amounts,
            leg_quotes: RouteLegSequence::from_vec(leg_quotes)
                .expect("test route should have valid leg count"),
        }
    }

    fn current_tick_index_hint(execution: &RouteLegExecutionConfig) -> Option<i32> {
        match execution {
            RouteLegExecutionConfig::OrcaWhirlpool(_) | RouteLegExecutionConfig::RaydiumClmm(_) => {
                Some(0)
            }
            RouteLegExecutionConfig::OrcaSimplePool(_)
            | RouteLegExecutionConfig::RaydiumSimplePool(_) => None,
        }
    }

    #[test]
    fn bootstrap_rejects_enabled_route_with_placeholder_execution_strings() {
        let mut route = valid_route_config();
        route.legs[0].pool_id = "REPLACE_WITH_ORCA_SIMPLE_POOL".into();
        let error = match bootstrap(test_config(route)) {
            Ok(_) => panic!("placeholder route should fail"),
            Err(error) => error,
        };

        let BootstrapError::InvalidRouteConfig { route_id, detail } = error else {
            panic!("expected invalid route config error");
        };
        assert_eq!(route_id, "route-a");
        assert!(detail.contains("legs[0].pool_id"));
    }

    #[test]
    fn bootstrap_skips_disabled_placeholder_routes() {
        let mut route = valid_route_config();
        route.enabled = false;
        route.legs[0].pool_id = "REPLACE_WITH_ORCA_SIMPLE_POOL".into();

        bootstrap(test_config(route)).expect("disabled placeholder route should be skipped");
    }

    #[test]
    fn bootstrap_rejects_mismatched_snapshot_slot_lag_config() {
        let mut config = test_config(valid_route_config());
        config.state.max_snapshot_slot_lag = 2;
        config.strategy.max_snapshot_slot_lag = 3;

        assert!(matches!(
            bootstrap(config),
            Err(BootstrapError::InvalidConfig { detail })
                if detail.contains("state.max_snapshot_slot_lag")
        ));
    }

    #[test]
    fn bootstrap_rejects_route_missing_sol_quote_conversion_pool() {
        let mut route = valid_route_config();
        route.sol_quote_conversion_pool_id = None;

        let error = match bootstrap(test_config(route)) {
            Ok(_) => panic!("route should require SOL/quote pool"),
            Err(error) => error,
        };
        let BootstrapError::InvalidRouteConfig { route_id, detail } = error else {
            panic!("expected invalid route config error");
        };
        assert_eq!(route_id, "route-a");
        assert!(detail.contains("sol_quote_conversion_pool_id is required"));
    }

    #[test]
    fn bootstrap_rejects_raydium_clmm_leg_without_explicit_fee_bps() {
        let mut route = valid_route_config();
        route.legs[1].execution =
            RouteLegExecutionConfig::RaydiumClmm(RaydiumClmmLegExecutionConfig {
                program_id: test_pubkey("raydium-clmm-program"),
                token_program_id: test_pubkey("spl-token-program"),
                token_program_2022_id: test_pubkey("spl-token-2022-program"),
                memo_program_id: test_pubkey("memo-program"),
                pool_state: test_pubkey("raydium-clmm-pool-state"),
                amm_config: test_pubkey("raydium-clmm-amm-config"),
                observation_state: test_pubkey("raydium-clmm-observation-state"),
                ex_bitmap_account: Some(test_pubkey("raydium-clmm-ex-bitmap")),
                token_mint_0: test_pubkey("raydium-clmm-token-mint-0"),
                token_vault_0: test_pubkey("raydium-clmm-token-vault-0"),
                token_mint_1: test_pubkey("raydium-clmm-token-mint-1"),
                token_vault_1: test_pubkey("raydium-clmm-token-vault-1"),
                tick_spacing: 8,
                zero_for_one: true,
            });
        route.legs[1].fee_bps = None;

        let error = match bootstrap(test_config(route)) {
            Ok(_) => panic!("raydium clmm leg without fee should fail"),
            Err(error) => error,
        };
        let BootstrapError::InvalidRouteConfig { route_id, detail } = error else {
            panic!("expected invalid route config error");
        };
        assert_eq!(route_id, "route-a");
        assert!(detail.contains("legs[1].fee_bps"));
        assert!(detail.contains("raydium_clmm"));
    }

    #[test]
    fn bootstrap_rejects_route_with_non_sol_quote_conversion_pool() {
        let mut config = test_config(valid_route_config());
        config.routes.definitions[0].sol_quote_conversion_pool_id = Some("pool-whirlpool".into());
        config.routes.definitions.push(concentrated_route_config());

        let error = match bootstrap(config) {
            Ok(_) => panic!("non SOL/quote pool should be rejected"),
            Err(error) => error,
        };
        let BootstrapError::InvalidRouteConfig { route_id, detail } = error else {
            panic!("expected invalid route config error");
        };
        assert_eq!(route_id, "route-a");
        assert!(detail.contains("tracked SOL/"));
    }

    #[test]
    fn bootstrap_accepts_valid_triangular_route_config() {
        bootstrap(test_config(triangular_route_config()))
            .expect("valid triangular route should bootstrap cleanly");
    }

    #[test]
    fn bootstrap_rejects_triangular_route_with_broken_leg_chain() {
        let mut route = triangular_route_config();
        route.legs[1].output_mint = Some("DezXAZ8z7PnrnRJjz3Wm8PwVbQrc6f1an5d1Wb7B1pPB".into());

        let error = match bootstrap(test_config(route)) {
            Ok(_) => panic!("broken triangular chain should fail bootstrap"),
            Err(error) => error,
        };
        let BootstrapError::InvalidRouteConfig { route_id, detail } = error else {
            panic!("expected invalid route config error");
        };
        assert_eq!(route_id, "route-tri");
        assert!(detail.contains("legs do not chain"));
    }

    #[test]
    fn bootstrap_validates_wallet_execution_accounts_before_start() {
        let route = valid_route_config();
        let mut config = test_config(route.clone());
        let owner = config.signing.owner_pubkey.clone();
        let token_program = match &route.legs[0].execution {
            RouteLegExecutionConfig::OrcaSimplePool(config) => config.token_program_id.clone(),
            _ => panic!("expected orca simple pool leg"),
        };
        let mut accounts = HashMap::new();
        accounts.insert(
            associated_token_address_for_test(&owner, &route.input_mint, &token_program),
            rpc_token_account(&token_program, &route.input_mint, &owner),
        );
        accounts.insert(
            associated_token_address_for_test(
                &owner,
                route.base_mint.as_deref().expect("base mint"),
                &token_program,
            ),
            rpc_token_account(
                &token_program,
                route.base_mint.as_deref().expect("base mint"),
                &owner,
            ),
        );
        accounts.insert(
            test_pubkey("route-input-ata"),
            rpc_token_account(&token_program, &route.input_mint, &owner),
        );
        accounts.insert(
            test_pubkey("route-mid-ata"),
            rpc_token_account(
                &token_program,
                route.base_mint.as_deref().expect("base mint"),
                &owner,
            ),
        );
        accounts.insert(
            test_pubkey("route-output-ata"),
            rpc_token_account(&token_program, &route.output_mint, &owner),
        );

        config.reconciliation.rpc_http_endpoint = spawn_mock_rpc_server(move |body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getLatestBlockhash" => mock_latest_blockhash_response(42).to_string(),
                "getBalance" => mock_balance_response(42, 1).to_string(),
                "getMultipleAccounts" => {
                    let keys = payload["params"][0].as_array().expect("account keys");
                    let values = keys
                        .iter()
                        .map(|key| {
                            accounts
                                .get(key.as_str().expect("account key"))
                                .cloned()
                                .unwrap_or(Value::Null)
                        })
                        .collect::<Vec<_>>();
                    json!({ "result": { "value": values } }).to_string()
                }
                other => panic!("unexpected method {other}"),
            }
        });
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();

        bootstrap(config).expect("bootstrap should accept a prepared execution environment");
    }

    #[test]
    fn bootstrap_seeds_live_cold_path_from_rpc_when_refresh_is_disabled() {
        let mut route = valid_route_config();
        let lookup_table_key = test_pubkey("bootstrap-alt");
        let seeded_slot = 77u64;
        let wallet_balance = 424_242u64;
        let alt_addresses = vec![
            Pubkey::new_from_array([1; 32]),
            Pubkey::new_from_array([2; 32]),
            Pubkey::new_from_array([3; 32]),
        ];
        let expected_alt_address_count = alt_addresses.len();
        route.execution.lookup_tables = vec![LookupTableConfig {
            account_key: lookup_table_key.clone(),
        }];

        let mut config = test_config(route);
        config.signing.validate_execution_accounts = false;
        config.state.bootstrap_blockhash_slot = 1;
        config.state.bootstrap_alt_revision = 1;
        config.signing.bootstrap_balance_lamports = 0;
        config.signing.wallet_ready = false;
        let seeded_alt_addresses = alt_addresses.clone();
        config.reconciliation.rpc_http_endpoint = spawn_mock_rpc_server(move |body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getLatestBlockhash" => mock_latest_blockhash_response(seeded_slot).to_string(),
                "getBalance" => mock_balance_response(seeded_slot, wallet_balance).to_string(),
                "getMultipleAccounts" => json!({
                    "result": {
                        "context": { "slot": seeded_slot },
                        "value": [rpc_lookup_table_account(&seeded_alt_addresses, seeded_slot - 1)]
                    }
                })
                .to_string(),
                other => panic!("unexpected method {other}"),
            }
        });

        let runtime = bootstrap(config).expect("bootstrap should seed live cold-path services");
        let execution = runtime.execution_state();
        let expected_blockhash = recent_blockhash();

        assert_eq!(execution.rpc_slot, Some(seeded_slot));
        assert_eq!(execution.blockhash_slot, Some(seeded_slot));
        assert_eq!(
            execution.latest_blockhash.as_deref(),
            Some(expected_blockhash.as_str())
        );
        assert_eq!(execution.alt_revision, seeded_slot);
        assert_eq!(execution.wallet_balance_lamports, wallet_balance);
        assert!(execution.wallet_ready);
        assert_eq!(execution.lookup_tables.len(), 1);
        assert_eq!(execution.lookup_tables[0].account_key, lookup_table_key);
        assert_eq!(
            execution.lookup_tables[0].addresses.len(),
            expected_alt_address_count
        );
        assert_eq!(
            execution.lookup_tables[0].last_extended_slot,
            seeded_slot - 1
        );
        assert_eq!(execution.lookup_tables[0].fetched_slot, seeded_slot);
    }

    #[test]
    fn bootstrap_rejects_missing_derived_ata_for_concentrated_routes() {
        let route = concentrated_route_config();
        let mut config = test_config(route.clone());
        let owner = config.signing.owner_pubkey.clone();
        let token_program = match &route.legs[0].execution {
            RouteLegExecutionConfig::OrcaWhirlpool(config) => config.token_program_id.clone(),
            _ => panic!("expected whirlpool leg"),
        };
        let quote_mint = route.quote_mint.as_deref().expect("quote mint").to_string();
        let base_mint = route.base_mint.as_deref().expect("base mint").to_string();
        let mut accounts = HashMap::new();
        accounts.insert(
            associated_token_address_for_test(&owner, &quote_mint, &token_program),
            rpc_token_account(&token_program, &quote_mint, &owner),
        );
        // Intentionally omit the base ATA required by the concentrated legs.
        let missing_base_ata =
            associated_token_address_for_test(&owner, &base_mint, &token_program);

        config.reconciliation.rpc_http_endpoint = spawn_mock_rpc_server(move |body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getLatestBlockhash" => mock_latest_blockhash_response(42).to_string(),
                "getBalance" => mock_balance_response(42, 1).to_string(),
                "getMultipleAccounts" => {
                    let keys = payload["params"][0].as_array().expect("account keys");
                    let values = keys
                        .iter()
                        .map(|key| {
                            accounts
                                .get(key.as_str().expect("account key"))
                                .cloned()
                                .unwrap_or(Value::Null)
                        })
                        .collect::<Vec<_>>();
                    json!({ "result": { "value": values } }).to_string()
                }
                other => panic!("unexpected method {other}"),
            }
        });
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();

        let error = match bootstrap(config) {
            Ok(_) => panic!("missing base ATA should fail bootstrap"),
            Err(error) => error,
        };
        let BootstrapError::ExecutionEnvironment { detail } = error else {
            panic!("expected execution environment error");
        };
        assert!(detail.contains(&missing_base_ata));
        assert!(
            detail.contains("route_mint_ata:route-clmm")
                || detail.contains("derived_whirlpool_ata:route-clmm")
        );
    }

    #[test]
    fn execution_protection_tightens_quote_slot_lag() {
        let mut route = valid_route_config();
        route.execution.max_quote_slot_lag = 32;
        route.execution_protection.enabled = true;
        route.execution_protection.tight_max_quote_slot_lag = 4;
        let route_id = RouteId(route.route_id.clone());
        let registry = execution_registry_from_config(&test_config(route));
        let registered = registry.get(&route_id).expect("route should be registered");

        assert_eq!(registered.max_quote_slot_lag, 24);
    }

    #[test]
    fn amm_fast_new_routes_compile_v0_with_lookup_tables() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let builder = AtomicArbTransactionBuilder::new(execution_registry_from_config(&config));
        let route_ids = [
            "jto-sol-ray-jvop-orca-2uhf",
            "jto-sol-orca-2uhf-ray-jvop",
            "trump-usdc-orca-6nd6-ray-7xzv",
            "trump-usdc-ray-7xzv-orca-6nd6",
        ];

        for route_id in route_ids {
            let route = config
                .routes
                .definitions
                .iter()
                .find(|route| route.route_id == route_id)
                .unwrap_or_else(|| panic!("missing route {route_id}"));

            let result = builder.build(BuildRequest {
                candidate: candidate_for_route(route),
                dynamic: dynamic_params_for_route(route),
            });

            assert_eq!(result.status, BuildStatus::Built, "{route_id}");
            let envelope = result
                .envelope
                .unwrap_or_else(|| panic!("missing envelope for {route_id}"));
            assert_eq!(envelope.message_format, MessageFormat::V0, "{route_id}");
            assert_eq!(envelope.resolved_lookup_tables.len(), 1, "{route_id}");
        }
    }

    #[test]
    fn amm_12_pairs_manifest_loads_with_expected_route_set() {
        use std::collections::BTreeSet;

        let config = BotConfig::from_path(repo_root_path("amm_12_pairs_fast.toml")).unwrap();
        let enabled = config
            .routes
            .definitions
            .iter()
            .filter(|route| route.enabled)
            .collect::<Vec<_>>();

        assert_eq!(enabled.len(), 20);
        assert!(
            enabled
                .iter()
                .all(|route| route.input_mint == route.output_mint)
        );
        assert!(enabled.iter().all(|route| {
            route.input_mint == "So11111111111111111111111111111111111111112"
                || route.input_mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        }));
        assert!(enabled.iter().all(|route| {
            matches!(
                route.legs[0].execution,
                RouteLegExecutionConfig::RaydiumClmm(_)
            ) || matches!(
                route.legs[0].execution,
                RouteLegExecutionConfig::OrcaWhirlpool(_)
            )
        }));
        assert!(enabled.iter().all(|route| {
            matches!(
                route.legs[1].execution,
                RouteLegExecutionConfig::RaydiumClmm(_)
            ) || matches!(
                route.legs[1].execution,
                RouteLegExecutionConfig::OrcaWhirlpool(_)
            )
        }));
        assert!(
            enabled
                .iter()
                .all(|route| route.execution.lookup_tables.len() == 1)
        );

        let pairs = enabled
            .iter()
            .map(|route| {
                (
                    route.base_mint.clone().unwrap_or_default(),
                    route.quote_mint.clone().unwrap_or_default(),
                )
            })
            .collect::<BTreeSet<_>>();
        let expected = [
            (
                "So11111111111111111111111111111111111111112",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            ),
            (
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "So11111111111111111111111111111111111111112",
            ),
            (
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            ),
            (
                "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
                "So11111111111111111111111111111111111111112",
            ),
            (
                "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            ),
            (
                "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
                "So11111111111111111111111111111111111111112",
            ),
            (
                "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            ),
            (
                "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
                "So11111111111111111111111111111111111111112",
            ),
            (
                "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            ),
            (
                "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
                "So11111111111111111111111111111111111111112",
            ),
        ]
        .into_iter()
        .map(|(base, quote)| (base.to_string(), quote.to_string()))
        .collect::<BTreeSet<_>>();

        assert_eq!(pairs, expected);
    }
}
