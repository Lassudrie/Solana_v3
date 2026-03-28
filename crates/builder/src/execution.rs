use std::collections::{BTreeSet, HashMap};

use domain::RouteId;
use strategy::route_registry::{RouteKind, RouteLegSequence, SwapSide};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageMode {
    V0Required,
    V0OrLegacy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupTableUsageConfig {
    pub account_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteExecutionConfig {
    pub route_id: RouteId,
    pub kind: RouteKind,
    pub message_mode: MessageMode,
    pub lookup_tables: Vec<LookupTableUsageConfig>,
    pub default_compute_unit_limit: u32,
    pub minimum_compute_unit_limit: u32,
    pub default_compute_unit_price_micro_lamports: u64,
    pub default_jito_tip_lamports: u64,
    pub max_quote_slot_lag: u64,
    pub max_alt_slot_lag: u64,
    pub legs: RouteLegSequence<VenueExecutionConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueExecutionConfig {
    OrcaSimplePool(OrcaSimplePoolConfig),
    OrcaWhirlpool(OrcaWhirlpoolConfig),
    RaydiumSimplePool(RaydiumSimplePoolConfig),
    RaydiumClmm(RaydiumClmmConfig),
}

impl VenueExecutionConfig {
    pub fn venue_name(&self) -> &'static str {
        match self {
            Self::OrcaSimplePool(_) => "orca",
            Self::OrcaWhirlpool(_) => "orca_whirlpool",
            Self::RaydiumSimplePool(_) => "raydium",
            Self::RaydiumClmm(_) => "raydium_clmm",
        }
    }

    pub fn supports_exact_out(&self, side: SwapSide) -> bool {
        match self {
            Self::OrcaSimplePool(_) => false,
            Self::OrcaWhirlpool(_) | Self::RaydiumClmm(_) => true,
            Self::RaydiumSimplePool(_) => side == SwapSide::BuyBase,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaSimplePoolConfig {
    pub program_id: String,
    pub token_program_id: String,
    pub swap_account: String,
    pub authority: String,
    pub pool_source_token_account: String,
    pub pool_destination_token_account: String,
    pub pool_mint: String,
    pub fee_account: String,
    pub user_source_token_account: String,
    pub user_destination_token_account: String,
    pub host_fee_account: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaWhirlpoolConfig {
    pub program_id: String,
    pub token_program_id: String,
    pub whirlpool: String,
    pub token_mint_a: String,
    pub token_vault_a: String,
    pub token_mint_b: String,
    pub token_vault_b: String,
    pub tick_spacing: u16,
    pub a_to_b: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumSimplePoolConfig {
    pub program_id: String,
    pub token_program_id: String,
    pub amm_pool: String,
    pub amm_authority: String,
    pub amm_open_orders: String,
    pub amm_coin_vault: String,
    pub amm_pc_vault: String,
    pub market_program: String,
    pub market: String,
    pub market_bids: String,
    pub market_asks: String,
    pub market_event_queue: String,
    pub market_coin_vault: String,
    pub market_pc_vault: String,
    pub market_vault_signer: String,
    pub user_source_token_account: Option<String>,
    pub user_destination_token_account: Option<String>,
    pub user_source_mint: Option<String>,
    pub user_destination_mint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumClmmConfig {
    pub program_id: String,
    pub token_program_id: String,
    pub token_program_2022_id: String,
    pub memo_program_id: String,
    pub pool_state: String,
    pub amm_config: String,
    pub observation_state: String,
    pub ex_bitmap_account: Option<String>,
    pub token_mint_0: String,
    pub token_vault_0: String,
    pub token_mint_1: String,
    pub token_vault_1: String,
    pub tick_spacing: u16,
    pub zero_for_one: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionRegistry {
    routes: HashMap<RouteId, RouteExecutionConfig>,
}

impl ExecutionRegistry {
    pub fn register(&mut self, route: RouteExecutionConfig) {
        self.routes.insert(route.route_id.clone(), route);
    }

    pub fn get(&self, route_id: &RouteId) -> Option<&RouteExecutionConfig> {
        self.routes.get(route_id)
    }

    pub fn all_lookup_table_keys(&self) -> Vec<String> {
        self.routes
            .values()
            .flat_map(|route| {
                route
                    .lookup_tables
                    .iter()
                    .map(|table| table.account_key.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}
