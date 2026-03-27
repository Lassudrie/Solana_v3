use builder::{
    AtomicArbTransactionBuilder, ExecutionRegistry, LookupTableUsageConfig, MessageMode,
    OrcaSimplePoolConfig, OrcaWhirlpoolConfig, RaydiumClmmConfig, RaydiumSimplePoolConfig,
    RouteExecutionConfig as BuilderRouteExecutionConfig, VenueExecutionConfig,
};
use reconciliation::{OnChainReconciler, OnChainReconciliationConfig};
use signing::{Signer, SigningError};
use state::{
    StatePlane,
    decoder::{OrcaWhirlpoolAccountDecoder, PoolPriceAccountDecoder, RaydiumClmmPoolDecoder},
    types::{AccountKey, PoolId, RouteId},
};
use std::collections::{HashMap, HashSet};
use strategy::{
    StrategyPlane,
    guards::GuardrailConfig,
    route_registry::{ExecutionProtectionPolicy, RouteDefinition, RouteLeg, SwapSide},
};
use submit::{JitoConfig, JitoSubmitter, SubmitMode};
use thiserror::Error;

use crate::{
    config::{
        BotConfig, BuilderConfig, MessageModeConfig, RouteClassConfig, RouteLegExecutionConfig,
        RuntimeProfileConfig, SigningConfig, SigningProviderKind, SubmitModeConfig, SwapSideConfig,
    },
    route_health::SharedRouteHealth,
    runtime::{
        AltCacheService, BlockhashService, BotRuntime, ColdPathServices, HotPathPipeline,
        WalletRefreshService, WarmupCoordinator,
    },
};

const BASE_FEE_LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const MICRO_LAMPORTS_PER_LAMPORT: u64 = 1_000_000;
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Signing(#[from] SigningError),
    #[error("invalid runtime config: {detail}")]
    InvalidConfig { detail: String },
    #[error("invalid route config for {route_id}: {detail}")]
    InvalidRouteConfig { route_id: String, detail: String },
}

struct ResolvedSigner {
    owner_pubkey: String,
    signer_available: bool,
    signer: Box<dyn Signer>,
}

pub fn bootstrap(config: BotConfig) -> Result<BotRuntime, BootstrapError> {
    let route_health = std::sync::Arc::new(std::sync::Mutex::new(
        crate::route_health::RouteHealthRegistry::new(config.runtime.live_set_health.clone()),
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

    let mut strategy = StrategyPlane::new(GuardrailConfig {
        min_profit_quote_atoms: config.strategy.min_profit_quote_atoms,
        require_route_warm: config.strategy.require_route_warm,
        max_inflight_submissions: config.strategy.max_inflight_submissions,
        min_wallet_balance_lamports: config.strategy.min_wallet_balance_lamports,
        max_blockhash_slot_lag: config.strategy.max_blockhash_slot_lag,
    });

    let active_routes = config
        .routes
        .definitions
        .iter()
        .filter(|route| route_enabled_in_profile(route, config.runtime.profile))
        .collect::<Vec<_>>();
    for route in &active_routes {
        validate_runtime_route_config(route)?;
    }
    let tracked_pool_pairs = build_tracked_pool_pairs(&active_routes);

    let mut execution_registry = ExecutionRegistry::default();
    for route in active_routes {
        let min_trade_size = resolve_min_trade_size(route)?;
        let sol_quote_conversion_pool_id =
            resolve_sol_quote_conversion_pool_id(route, &tracked_pool_pairs)?;
        let route_id = RouteId(route.route_id.clone());
        let mut pool_ids = route
            .legs
            .iter()
            .map(|leg| PoolId(leg.pool_id.clone()))
            .collect::<Vec<_>>();
        if let Some(pool_id) = &sol_quote_conversion_pool_id {
            pool_ids.push(pool_id.clone());
        }
        let mut seen_pool_ids = HashSet::with_capacity(pool_ids.len());
        pool_ids.retain(|pool_id| seen_pool_ids.insert(pool_id.clone()));
        if let Ok(mut health) = route_health.lock() {
            health.register_route(route_id.clone(), &pool_ids);
        }
        state.register_route(route_id.clone(), pool_ids);
        for dependency in &route.account_dependencies {
            state.register_account_dependency(
                AccountKey(dependency.account_key.clone()),
                PoolId(dependency.pool_id.clone()),
                dependency.decoder_key.clone(),
            );
        }
        strategy.register_route(RouteDefinition {
            route_id,
            input_mint: route.input_mint.clone(),
            output_mint: route.output_mint.clone(),
            base_mint: route.base_mint.clone(),
            quote_mint: route.quote_mint.clone(),
            sol_quote_conversion_pool_id,
            legs: [
                RouteLeg {
                    venue: route.legs[0].venue.clone(),
                    pool_id: PoolId(route.legs[0].pool_id.clone()),
                    side: map_swap_side(&route.legs[0].side),
                    fee_bps: route.legs[0].fee_bps,
                },
                RouteLeg {
                    venue: route.legs[1].venue.clone(),
                    pool_id: PoolId(route.legs[1].pool_id.clone()),
                    side: map_swap_side(&route.legs[1].side),
                    fee_bps: route.legs[1].fee_bps,
                },
            ],
            min_trade_size,
            default_trade_size: route.default_trade_size,
            max_trade_size: route.max_trade_size,
            size_ladder: route.size_ladder.clone(),
            estimated_execution_cost_lamports: estimate_execution_cost_lamports(
                route,
                &config.builder,
                route.execution.default_compute_unit_limit,
                route.execution.default_compute_unit_price_micro_lamports,
                route.execution.default_jito_tip_lamports,
            ),
            execution_protection: map_execution_protection(&route.execution_protection),
        });
        execution_registry.register(BuilderRouteExecutionConfig {
            route_id: RouteId(route.route_id.clone()),
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
            default_compute_unit_price_micro_lamports: route
                .execution
                .default_compute_unit_price_micro_lamports,
            default_jito_tip_lamports: route.execution.default_jito_tip_lamports,
            max_quote_slot_lag: effective_max_quote_slot_lag(route),
            max_alt_slot_lag: route.execution.max_alt_slot_lag,
            legs: [
                map_leg_execution(&route.legs[0].execution),
                map_leg_execution(&route.legs[1].execution),
            ],
        });
    }

    let submit_mode = match config.submit.mode {
        SubmitModeConfig::SingleTransaction => SubmitMode::SingleTransaction,
        SubmitModeConfig::Bundle => SubmitMode::Bundle,
    };
    let resolved_signer = resolve_signer(&config.signing)?;
    let cold_path = ColdPathServices {
        warmup: WarmupCoordinator::default(),
        blockhash: BlockhashService {
            current_blockhash: config.state.bootstrap_blockhash.clone(),
            slot: config.state.bootstrap_blockhash_slot,
        },
        alt_cache: AltCacheService {
            revision: config.state.bootstrap_alt_revision,
            lookup_tables: Vec::new(),
        },
        wallet_refresh: WalletRefreshService {
            balance_lamports: config.signing.bootstrap_balance_lamports,
            ready: config.signing.wallet_ready,
            signer_available: resolved_signer.signer_available,
        },
    };
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
            Box::new(JitoSubmitter::new(JitoConfig {
                endpoint: config.jito.endpoint.clone(),
                ws_endpoint: config.jito.ws_endpoint.clone(),
                auth_token: config.jito.auth_token.clone(),
                bundle_enabled: config.jito.bundle_enabled,
                connect_timeout_ms: config.jito.connect_timeout_ms,
                request_timeout_ms: config.jito.request_timeout_ms,
                retry_attempts: config.jito.retry_attempts,
                retry_backoff_ms: config.jito.retry_backoff_ms,
                idempotency_cache_size: config.jito.idempotency_cache_size,
            })),
        ),
        cold_path,
        submit_mode,
        config.builder.compute_unit_limit,
        config.builder.compute_unit_price_micro_lamports,
        config.builder.jito_tip_lamports,
        route_health,
    );
    runtime.set_reconciler(OnChainReconciler::new(OnChainReconciliationConfig {
        enabled: config.reconciliation.enabled,
        rpc_http_endpoint: config.reconciliation.rpc_http_endpoint.clone(),
        rpc_ws_endpoint: config.reconciliation.rpc_ws_endpoint.clone(),
        websocket_enabled: config.reconciliation.websocket_enabled,
        websocket_timeout_ms: config.reconciliation.websocket_timeout_ms,
        search_transaction_history: config.reconciliation.search_transaction_history,
        max_pending_slots: config.reconciliation.max_pending_slots,
    }));
    runtime.apply_cold_path_seed();
    Ok(runtime)
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
    let Some(quote_mint) = route.quote_mint.as_deref() else {
        return false;
    };
    route.base_mint.is_some()
        && route.input_mint == route.output_mint
        && route.input_mint == quote_mint
        && quote_mint != SOL_MINT
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
        | RouteLegExecutionConfig::RaydiumSimplePool(_) => (
            route
                .base_mint
                .clone()
                .unwrap_or_else(|| route.input_mint.clone()),
            route
                .quote_mint
                .clone()
                .unwrap_or_else(|| route.output_mint.clone()),
        ),
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
    if route.base_mint.is_none()
        || route.quote_mint.is_none()
        || route.input_mint != route.output_mint
    {
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
        configured.min(protection.tight_max_quote_slot_lag)
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
                signer: Box::new(signer),
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
                signer: Box::new(signer),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use builder::{
        AtomicArbTransactionBuilder, BuildRequest, BuildStatus, DynamicBuildParameters,
        ExecutionRegistry, LookupTableUsageConfig, MessageFormat,
        RouteExecutionConfig as BuilderRouteExecutionConfig, TransactionBuilder,
    };
    use solana_sdk::{
        hash::hashv,
        pubkey::Pubkey,
        signer::{Signer as SolanaCurveSigner, keypair::Keypair},
    };
    use state::types::{LookupTableSnapshot, PoolId, RouteId};
    use strategy::{opportunity::OpportunityCandidate, quote::LegQuote};

    use super::{
        BootstrapError, bootstrap, effective_max_quote_slot_lag, map_leg_execution,
        map_message_mode, map_swap_side, route_enabled_in_profile,
    };
    use crate::config::{
        BotConfig, MessageModeConfig, OrcaSimplePoolLegExecutionConfig,
        RaydiumClmmLegExecutionConfig, RaydiumSimplePoolLegExecutionConfig, RouteClassConfig,
        RouteConfig, RouteExecutionConfig, RouteLegConfig, RouteLegExecutionConfig, RoutesConfig,
        SwapSideConfig,
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
            route_id: "route-a".into(),
            input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            base_mint: Some("So11111111111111111111111111111111111111112".into()),
            quote_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
            sol_quote_conversion_pool_id: Some("pool-a".into()),
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            min_trade_size: None,
            size_ladder: Vec::new(),
            execution_protection: Default::default(),
            legs: [
                RouteLegConfig {
                    venue: "orca".into(),
                    pool_id: "pool-a".into(),
                    side: SwapSideConfig::BuyBase,
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
                            user_source_token_account: test_pubkey("route-mid-ata"),
                            user_destination_token_account: test_pubkey("route-output-ata"),
                        },
                    ),
                },
            ],
            account_dependencies: Vec::new(),
            execution: route_execution(),
        }
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
        config.jito.ws_endpoint = "mock://jito-tip-stream".into();
        config.routes = RoutesConfig {
            definitions: vec![route],
        };
        config
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
                default_compute_unit_price_micro_lamports: route
                    .execution
                    .default_compute_unit_price_micro_lamports,
                default_jito_tip_lamports: route.execution.default_jito_tip_lamports,
                max_quote_slot_lag: effective_max_quote_slot_lag(route),
                max_alt_slot_lag: route.execution.max_alt_slot_lag,
                legs: [
                    map_leg_execution(&route.legs[0].execution),
                    map_leg_execution(&route.legs[1].execution),
                ],
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
        let first_output = route
            .default_trade_size
            .saturating_add(route.default_trade_size / 20 + 1);
        let second_output = first_output.saturating_add(route.default_trade_size / 20 + 1);
        OpportunityCandidate {
            route_id: RouteId(route.route_id.clone()),
            quoted_slot: 42,
            leg_snapshot_slots: [42, 42],
            trade_size: route.default_trade_size,
            active_execution_buffer_bps: None,
            expected_net_output: second_output,
            minimum_acceptable_output: route.default_trade_size.saturating_add(1),
            expected_gross_profit_quote_atoms: 1,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: 1,
            leg_quotes: [
                LegQuote {
                    venue: route.legs[0].venue.clone(),
                    pool_id: PoolId(route.legs[0].pool_id.clone()),
                    side: map_swap_side(&route.legs[0].side),
                    input_amount: route.default_trade_size,
                    output_amount: first_output,
                    fee_paid: 0,
                    current_tick_index: current_tick_index_hint(&route.legs[0].execution),
                },
                LegQuote {
                    venue: route.legs[1].venue.clone(),
                    pool_id: PoolId(route.legs[1].pool_id.clone()),
                    side: map_swap_side(&route.legs[1].side),
                    input_amount: first_output,
                    output_amount: second_output,
                    fee_paid: 0,
                    current_tick_index: current_tick_index_hint(&route.legs[1].execution),
                },
            ],
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
        let mut route = valid_route_config();
        route.base_mint = Some("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".into());
        route.sol_quote_conversion_pool_id = Some("pool-a".into());

        let error = match bootstrap(test_config(route)) {
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
    fn execution_protection_tightens_quote_slot_lag() {
        let mut route = valid_route_config();
        route.execution.max_quote_slot_lag = 32;
        route.execution_protection.enabled = true;
        route.execution_protection.tight_max_quote_slot_lag = 4;
        let route_id = RouteId(route.route_id.clone());
        let registry = execution_registry_from_config(&test_config(route));
        let registered = registry.get(&route_id).expect("route should be registered");

        assert_eq!(registered.max_quote_slot_lag, 4);
    }

    #[test]
    fn amm_fast_new_routes_compile_v0_with_lookup_tables() {
        let config = BotConfig::from_path(repo_root_path("sol_usdc_routes_amm_fast.toml")).unwrap();
        let builder = AtomicArbTransactionBuilder::new(execution_registry_from_config(&config));
        let route_ids = [
            "jto-sol-ray-jvop-orca-2uhf",
            "jto-sol-orca-2uhf-ray-jvop",
            "trump-sol-orca-ckp1-ray-gqsp",
            "trump-sol-ray-gqsp-orca-ckp1",
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

        assert_eq!(enabled.len(), 24);
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
            (
                "pSo1f9nQXWgXibFtKf7NWYxb5enAM4qfP6UJSiXRQfL",
                "So11111111111111111111111111111111111111112",
            ),
        ]
        .into_iter()
        .map(|(base, quote)| (base.to_string(), quote.to_string()))
        .collect::<BTreeSet<_>>();

        assert_eq!(pairs, expected);
    }
}
