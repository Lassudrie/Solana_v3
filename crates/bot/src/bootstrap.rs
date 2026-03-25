use builder::{
    AtomicArbTransactionBuilder, ExecutionRegistry, LookupTableUsageConfig, MessageMode,
    OrcaSimplePoolConfig, RaydiumSimplePoolConfig,
    RouteExecutionConfig as BuilderRouteExecutionConfig, VenueExecutionConfig,
};
use reconciliation::{OnChainReconciler, OnChainReconciliationConfig};
use state::{
    StatePlane,
    decoder::PoolPriceAccountDecoder,
    types::{AccountKey, PoolId, RouteId},
};
use strategy::{
    StrategyPlane,
    guards::GuardrailConfig,
    route_registry::{RouteDefinition, RouteLeg, SwapSide},
};
use submit::{JitoConfig, JitoSubmitter, SubmitMode};

use crate::{
    config::{
        BotConfig, MessageModeConfig, RouteLegExecutionConfig, SubmitModeConfig, SwapSideConfig,
    },
    runtime::{
        AltCacheService, BlockhashService, BotRuntime, ColdPathServices, HotPathPipeline,
        WalletRefreshService, WarmupCoordinator,
    },
};

pub fn bootstrap(config: BotConfig) -> BotRuntime {
    let mut state = StatePlane::new(config.state.max_snapshot_slot_lag);
    state
        .decoder_registry_mut()
        .register(PoolPriceAccountDecoder);

    let mut strategy = StrategyPlane::new(GuardrailConfig {
        min_profit_lamports: config.strategy.min_profit_lamports,
        max_snapshot_slot_lag: config.strategy.max_snapshot_slot_lag,
        require_route_warm: config.strategy.require_route_warm,
        max_inflight_submissions: config.strategy.max_inflight_submissions,
        min_wallet_balance_lamports: config.strategy.min_wallet_balance_lamports,
        max_blockhash_slot_lag: config.strategy.max_blockhash_slot_lag,
    });

    let mut execution_registry = ExecutionRegistry::default();
    for route in &config.routes.definitions {
        let route_id = RouteId(route.route_id.clone());
        let pool_ids = route
            .legs
            .iter()
            .map(|leg| PoolId(leg.pool_id.clone()))
            .collect::<Vec<_>>();
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
            legs: [
                RouteLeg {
                    venue: route.legs[0].venue.clone(),
                    pool_id: PoolId(route.legs[0].pool_id.clone()),
                    side: map_swap_side(&route.legs[0].side),
                },
                RouteLeg {
                    venue: route.legs[1].venue.clone(),
                    pool_id: PoolId(route.legs[1].pool_id.clone()),
                    side: map_swap_side(&route.legs[1].side),
                },
            ],
            default_trade_size: route.default_trade_size,
            max_trade_size: route.max_trade_size,
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
            max_quote_slot_lag: route.execution.max_quote_slot_lag,
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
    let signer = signing::LocalWalletSigner::new(
        config.signing.wallet_id.clone(),
        config.signing.keypair_path.clone(),
        config.signing.keypair_base58.clone(),
    );
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
            signer_available: signer.has_signer_material(),
        },
    };
    let owner_pubkey = if config.signing.owner_pubkey.is_empty() {
        signer.pubkey_string().unwrap_or_default()
    } else {
        config.signing.owner_pubkey.clone()
    };
    let wallet_status = if !config.signing.wallet_ready {
        signing::WalletStatus::Refreshing
    } else if signer.has_signer_material() {
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
                owner_pubkey,
                balance_lamports: config.signing.bootstrap_balance_lamports,
                status: wallet_status,
            },
            signer,
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
    runtime
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
    }
}
