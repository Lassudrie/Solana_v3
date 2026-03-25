use builder::AtomicArbTransactionBuilder;
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
    config::{BotConfig, SubmitModeConfig, SwapSideConfig},
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
    }

    let submit_mode = match config.submit.mode {
        SubmitModeConfig::SingleTransaction => SubmitMode::SingleTransaction,
        SubmitModeConfig::Bundle => SubmitMode::Bundle,
    };
    let cold_path = ColdPathServices {
        warmup: WarmupCoordinator::default(),
        blockhash: BlockhashService {
            current_blockhash: config.state.bootstrap_blockhash.clone(),
            slot: config.state.bootstrap_blockhash_slot,
        },
        alt_cache: AltCacheService {
            revision: config.state.bootstrap_alt_revision,
        },
        wallet_refresh: WalletRefreshService {
            balance_lamports: config.signing.bootstrap_balance_lamports,
            ready: config.signing.wallet_ready,
        },
    };
    let mut runtime = BotRuntime::new(
        HotPathPipeline::new(
            state,
            strategy,
            AtomicArbTransactionBuilder::default(),
            signing::HotWallet {
                wallet_id: config.signing.wallet_id.clone(),
                owner_pubkey: config.signing.owner_pubkey.clone(),
                balance_lamports: config.signing.bootstrap_balance_lamports,
                status: if config.signing.wallet_ready {
                    signing::WalletStatus::Ready
                } else {
                    signing::WalletStatus::Refreshing
                },
            },
            signing::LocalWalletSigner::new(config.signing.wallet_id.clone()),
            Box::new(JitoSubmitter::new(JitoConfig {
                endpoint: config.jito.endpoint.clone(),
                bundle_enabled: config.jito.bundle_enabled,
            })),
        ),
        cold_path,
        submit_mode,
        config.builder.compute_unit_limit,
        config.builder.compute_unit_price_micro_lamports,
        config.builder.jito_tip_lamports,
    );
    runtime.apply_cold_path_seed();
    runtime
}

fn map_swap_side(side: &SwapSideConfig) -> SwapSide {
    match side {
        SwapSideConfig::BuyBase => SwapSide::BuyBase,
        SwapSideConfig::SellBase => SwapSide::SellBase,
    }
}
