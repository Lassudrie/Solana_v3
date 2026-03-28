use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotConfig {
    pub shredstream: ShredstreamConfig,
    pub state: StateConfig,
    pub routes: RoutesConfig,
    pub strategy: StrategyConfig,
    pub builder: BuilderConfig,
    pub signing: SigningConfig,
    #[serde(default)]
    pub build_sign: BuildSignConfig,
    pub submit: SubmitConfig,
    pub jito: JitoSubmitConfig,
    #[serde(default)]
    pub rpc_submit: RpcSubmitConfig,
    pub reconciliation: ReconciliationConfig,
    pub risk: RiskConfig,
    pub runtime: RuntimeConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            shredstream: ShredstreamConfig::default(),
            state: StateConfig::default(),
            routes: RoutesConfig::default(),
            strategy: StrategyConfig::default(),
            builder: BuilderConfig::default(),
            signing: SigningConfig::default(),
            build_sign: BuildSignConfig::default(),
            submit: SubmitConfig::default(),
            jito: JitoSubmitConfig::default(),
            rpc_submit: RpcSubmitConfig::default(),
            reconciliation: ReconciliationConfig::default(),
            risk: RiskConfig::default(),
            runtime: RuntimeConfig::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing config path; pass --config <path> or set BOT_CONFIG")]
    MissingPath,
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported config format for {path}; use .toml or .json")]
    UnsupportedFormat { path: String },
    #[error("failed to parse TOML config {path}: {source}")]
    Toml {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to parse JSON config {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to deserialize merged config {path}: {source}")]
    Deserialize {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

impl BotConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => Self::from_toml_str(path, &text),
            Some("json") => Self::from_json_str(path, &text),
            _ => Err(ConfigError::UnsupportedFormat {
                path: path.display().to_string(),
            }),
        }
    }

    pub fn apply_runtime_profile_defaults(&mut self) {
        if self.runtime.profile != RuntimeProfileConfig::UltraFast {
            return;
        }

        let defaults = Self::default();
        let shredstream_live = self.runtime.event_source.mode == EventSourceMode::Shredstream;

        // Ultra-fast mode keeps the async submit path short and fail-fast.
        set_if_default(
            &mut self.build_sign.worker_count,
            defaults.build_sign.worker_count,
            8,
        );
        set_if_default(
            &mut self.build_sign.queue_capacity,
            defaults.build_sign.queue_capacity,
            256,
        );
        set_if_default(
            &mut self.build_sign.congestion_threshold_pct,
            defaults.build_sign.congestion_threshold_pct,
            90,
        );
        set_if_default(
            &mut self.submit.worker_count,
            defaults.submit.worker_count,
            8,
        );
        set_if_default(
            &mut self.submit.queue_capacity,
            defaults.submit.queue_capacity,
            256,
        );
        set_if_default(
            &mut self.submit.congestion_threshold_pct,
            defaults.submit.congestion_threshold_pct,
            90,
        );
        set_if_default(
            &mut self.submit.single_transaction_policy,
            defaults.submit.single_transaction_policy,
            SingleTransactionSubmitPolicyConfig::JitoOnly,
        );
        set_if_default(
            &mut self.jito.request_timeout_ms,
            defaults.jito.request_timeout_ms,
            250,
        );
        set_if_default(
            &mut self.jito.retry_attempts,
            defaults.jito.retry_attempts,
            1,
        );
        set_if_default(
            &mut self.jito.retry_backoff_ms,
            defaults.jito.retry_backoff_ms,
            0,
        );
        set_if_default(
            &mut self.rpc_submit.enabled,
            defaults.rpc_submit.enabled,
            false,
        );
        set_if_default(
            &mut self.rpc_submit.request_timeout_ms,
            defaults.rpc_submit.request_timeout_ms,
            250,
        );
        set_if_default(
            &mut self.rpc_submit.max_retries,
            defaults.rpc_submit.max_retries,
            0,
        );
        set_if_default(
            &mut self.runtime.control.idle_sleep_millis,
            defaults.runtime.control.idle_sleep_millis,
            0,
        );
        set_if_default(
            &mut self.runtime.control.max_events_per_tick,
            defaults.runtime.control.max_events_per_tick,
            4_096,
        );
        set_if_default(
            &mut self.runtime.parallelism.state_shard_count,
            defaults.runtime.parallelism.state_shard_count,
            16,
        );
        set_if_default(
            &mut self.runtime.parallelism.route_eval_worker_count,
            defaults.runtime.parallelism.route_eval_worker_count,
            8,
        );
        set_if_default(
            &mut self.runtime.parallelism.trigger_queue_capacity,
            defaults.runtime.parallelism.trigger_queue_capacity,
            4_096,
        );
        set_if_default(
            &mut self.reconciliation.poll_interval_millis,
            defaults.reconciliation.poll_interval_millis,
            25,
        );
        set_if_default(
            &mut self.runtime.refresh.enabled,
            defaults.runtime.refresh.enabled,
            true,
        );
        set_if_default(
            &mut self.runtime.refresh.blockhash_refresh_millis,
            defaults.runtime.refresh.blockhash_refresh_millis,
            if shredstream_live { 5_000 } else { 500 },
        );
        set_if_default(
            &mut self.runtime.refresh.slot_refresh_millis,
            defaults.runtime.refresh.slot_refresh_millis,
            if shredstream_live { 0 } else { 250 },
        );
        set_if_default(
            &mut self.runtime.refresh.alt_refresh_millis,
            defaults.runtime.refresh.alt_refresh_millis,
            if shredstream_live { 0 } else { 2_000 },
        );
        set_if_default(
            &mut self.runtime.refresh.wallet_refresh_millis,
            defaults.runtime.refresh.wallet_refresh_millis,
            if shredstream_live { 0 } else { 2_000 },
        );

        if shredstream_live {
            promote_shadow_reducer(&mut self.shredstream.reducers.raydium_simple_pool);
            promote_shadow_reducer(&mut self.shredstream.reducers.orca_whirlpool);
            promote_shadow_reducer(&mut self.shredstream.reducers.raydium_clmm);
        }
    }

    fn from_toml_str(path: &Path, text: &str) -> Result<Self, ConfigError> {
        match toml::from_str::<Self>(text) {
            Ok(config) => Ok(Self::with_applied_profile_defaults(config)),
            Err(_) => {
                let overlay =
                    toml::from_str::<toml::Value>(text).map_err(|source| ConfigError::Toml {
                        path: path.display().to_string(),
                        source,
                    })?;
                Self::from_merged_value(
                    path,
                    serde_json::to_value(overlay).expect("TOML value should serialize"),
                )
            }
        }
    }

    fn from_json_str(path: &Path, text: &str) -> Result<Self, ConfigError> {
        match serde_json::from_str::<Self>(text) {
            Ok(config) => Ok(Self::with_applied_profile_defaults(config)),
            Err(_) => {
                let overlay = serde_json::from_str::<JsonValue>(text).map_err(|source| {
                    ConfigError::Json {
                        path: path.display().to_string(),
                        source,
                    }
                })?;
                Self::from_merged_value(path, overlay)
            }
        }
    }

    fn from_merged_value(path: &Path, overlay: JsonValue) -> Result<Self, ConfigError> {
        let mut merged =
            serde_json::to_value(Self::default()).expect("BotConfig::default should serialize");
        merge_json_value(&mut merged, overlay);
        let config = serde_json::from_value(merged).map_err(|source| ConfigError::Deserialize {
            path: path.display().to_string(),
            source,
        })?;
        Ok(Self::with_applied_profile_defaults(config))
    }

    fn with_applied_profile_defaults(mut config: Self) -> Self {
        config.apply_runtime_profile_defaults();
        config
    }
}

fn set_if_default<T: PartialEq>(slot: &mut T, default: T, desired: T) {
    if *slot == default {
        *slot = desired;
    }
}

fn merge_json_value(base: &mut JsonValue, overlay: JsonValue) {
    match (base, overlay) {
        (JsonValue::Object(base_map), JsonValue::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_json_value(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_value, overlay_value) => *base_value = overlay_value,
    }
}

fn promote_shadow_reducer(mode: &mut ReducerRolloutMode) {
    if *mode == ReducerRolloutMode::Shadow {
        *mode = ReducerRolloutMode::Active;
    }
}

fn default_shredstream_idle_refresh_slot_lag() -> u64 {
    128
}

fn default_shredstream_price_impact_trigger_bps() -> u16 {
    1
}

fn default_shredstream_price_dislocation_trigger_bps() -> u16 {
    5
}

fn default_account_batch_window_millis() -> u64 {
    20
}

fn default_account_batch_parallel_rpc_requests() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ShredstreamConfig {
    #[serde(alias = "endpoint")]
    pub grpc_endpoint: String,
    pub grpc_connect_timeout_ms: u64,
    pub buffer_capacity: usize,
    pub reconnect_backoff_millis: u64,
    pub max_reconnect_backoff_millis: u64,
    #[serde(default = "default_shredstream_idle_refresh_slot_lag")]
    pub idle_refresh_slot_lag: u64,
    #[serde(default = "default_shredstream_price_impact_trigger_bps")]
    pub price_impact_trigger_bps: u16,
    #[serde(default = "default_shredstream_price_dislocation_trigger_bps")]
    pub price_dislocation_trigger_bps: u16,
    pub max_repair_in_flight: usize,
    #[serde(default)]
    pub reducers: LiveReducerConfig,
}

impl Default for ShredstreamConfig {
    fn default() -> Self {
        Self {
            grpc_endpoint: "http://127.0.0.1:50051".into(),
            grpc_connect_timeout_ms: 500,
            buffer_capacity: 4_096,
            reconnect_backoff_millis: 250,
            max_reconnect_backoff_millis: 5_000,
            idle_refresh_slot_lag: default_shredstream_idle_refresh_slot_lag(),
            price_impact_trigger_bps: default_shredstream_price_impact_trigger_bps(),
            price_dislocation_trigger_bps: default_shredstream_price_dislocation_trigger_bps(),
            max_repair_in_flight: 0,
            reducers: LiveReducerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReducerRolloutMode {
    Disabled,
    Shadow,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveReducerConfig {
    pub orca_simple_pool: ReducerRolloutMode,
    pub raydium_simple_pool: ReducerRolloutMode,
    pub orca_whirlpool: ReducerRolloutMode,
    pub raydium_clmm: ReducerRolloutMode,
}

impl Default for LiveReducerConfig {
    fn default() -> Self {
        Self {
            orca_simple_pool: ReducerRolloutMode::Active,
            raydium_simple_pool: ReducerRolloutMode::Active,
            orca_whirlpool: ReducerRolloutMode::Active,
            raydium_clmm: ReducerRolloutMode::Active,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateConfig {
    pub max_snapshot_slot_lag: u64,
    pub bootstrap_blockhash: String,
    pub bootstrap_blockhash_slot: u64,
    pub bootstrap_alt_revision: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            max_snapshot_slot_lag: 2,
            bootstrap_blockhash: "11111111111111111111111111111111".into(),
            bootstrap_blockhash_slot: 1,
            bootstrap_alt_revision: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RoutesConfig {
    pub definitions: Vec<RouteConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteClassConfig {
    AmmFastPath,
    ClmmSlowPath,
}

impl Default for RouteClassConfig {
    fn default() -> Self {
        Self::ClmmSlowPath
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RouteKindConfig {
    #[default]
    TwoLeg,
    Triangular,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteConfig {
    #[serde(default = "default_route_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub route_class: RouteClassConfig,
    #[serde(default)]
    pub kind: RouteKindConfig,
    pub route_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub base_mint: Option<String>,
    pub quote_mint: Option<String>,
    pub sol_quote_conversion_pool_id: Option<String>,
    #[serde(default)]
    pub min_profit_quote_atoms: Option<i64>,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    #[serde(default)]
    pub min_trade_size: Option<u64>,
    #[serde(default)]
    pub size_ladder: Vec<u64>,
    #[serde(default)]
    pub sizing: RouteSizingConfig,
    #[serde(default)]
    pub execution_protection: RouteExecutionProtectionConfig,
    pub legs: Vec<RouteLegConfig>,
    pub account_dependencies: Vec<AccountDependencyConfig>,
    pub execution: RouteExecutionConfig,
}

fn default_route_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SizingModeConfig {
    #[default]
    Legacy,
    EvShadow,
    EvLive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RouteSizingConfig {
    pub mode: Option<SizingModeConfig>,
    pub min_trade_floor_sol_lamports: Option<u64>,
    pub base_landing_rate_bps: Option<u16>,
    pub base_expected_shortfall_bps: Option<u16>,
    pub max_expected_shortfall_bps: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RouteExecutionProtectionConfig {
    pub enabled: bool,
    pub tight_max_quote_slot_lag: u64,
    pub base_extra_buy_leg_slippage_bps: u16,
    pub failure_step_bps: u16,
    pub max_extra_buy_leg_slippage_bps: u16,
    pub recovery_success_count: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteLegConfig {
    pub venue: String,
    pub pool_id: String,
    pub side: SwapSideConfig,
    #[serde(default)]
    pub input_mint: Option<String>,
    #[serde(default)]
    pub output_mint: Option<String>,
    pub fee_bps: Option<u16>,
    pub execution: RouteLegExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SwapSideConfig {
    BuyBase,
    SellBase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountDependencyConfig {
    pub account_key: String,
    pub pool_id: String,
    pub decoder_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteExecutionConfig {
    #[serde(default)]
    pub message_mode: MessageModeConfig,
    #[serde(default)]
    pub lookup_tables: Vec<LookupTableConfig>,
    pub default_compute_unit_limit: u32,
    #[serde(default)]
    pub minimum_compute_unit_limit: u32,
    pub default_compute_unit_price_micro_lamports: u64,
    pub default_jito_tip_lamports: u64,
    #[serde(default = "default_max_quote_slot_lag")]
    pub max_quote_slot_lag: u64,
    #[serde(default = "default_max_alt_slot_lag")]
    pub max_alt_slot_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageModeConfig {
    V0Required,
    V0OrLegacy,
}

impl Default for MessageModeConfig {
    fn default() -> Self {
        Self::V0Required
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupTableConfig {
    pub account_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteLegExecutionConfig {
    OrcaSimplePool(OrcaSimplePoolLegExecutionConfig),
    OrcaWhirlpool(OrcaWhirlpoolLegExecutionConfig),
    RaydiumSimplePool(RaydiumSimplePoolLegExecutionConfig),
    RaydiumClmm(RaydiumClmmLegExecutionConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrcaSimplePoolLegExecutionConfig {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrcaWhirlpoolLegExecutionConfig {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaydiumSimplePoolLegExecutionConfig {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaydiumClmmLegExecutionConfig {
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

fn default_max_quote_slot_lag() -> u64 {
    2
}

fn default_max_alt_slot_lag() -> u64 {
    32
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyConfig {
    #[serde(alias = "min_profit_lamports")]
    pub min_profit_quote_atoms: i64,
    #[serde(default)]
    pub min_profit_bps: u16,
    pub max_snapshot_slot_lag: u64,
    pub require_route_warm: bool,
    pub max_inflight_submissions: usize,
    pub min_wallet_balance_lamports: u64,
    pub max_blockhash_slot_lag: u64,
    #[serde(default)]
    pub sizing: StrategySizingConfig,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_profit_quote_atoms: 10,
            min_profit_bps: 0,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 64,
            sizing: StrategySizingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StrategySizingConfig {
    pub mode: SizingModeConfig,
    pub fixed_trade_size: bool,
    pub min_trade_floor_sol_lamports: u64,
    pub base_landing_rate_bps: u16,
    pub ewma_alpha_bps: u16,
    pub base_expected_shortfall_bps: u16,
    pub max_expected_shortfall_bps: u16,
    pub too_little_output_shortfall_step_bps: u16,
    pub inflight_penalty_bps_per_submission: u16,
    pub max_inflight_penalty_bps: u16,
    pub blockhash_penalty_bps_per_slot: u16,
    pub max_blockhash_penalty_bps: u16,
    pub quote_age_penalty_bps_per_slot: u16,
    pub max_quote_age_penalty_bps: u16,
    pub tick_cross_penalty_bps_per_tick: u16,
    pub max_tick_cross_penalty_bps: u16,
    pub max_reserve_usage_penalty_bps: u16,
}

impl Default for StrategySizingConfig {
    fn default() -> Self {
        Self {
            mode: SizingModeConfig::Legacy,
            fixed_trade_size: false,
            min_trade_floor_sol_lamports: 100_000_000,
            base_landing_rate_bps: 8_500,
            ewma_alpha_bps: 2_000,
            base_expected_shortfall_bps: 75,
            max_expected_shortfall_bps: 500,
            too_little_output_shortfall_step_bps: 75,
            inflight_penalty_bps_per_submission: 25,
            max_inflight_penalty_bps: 1_500,
            blockhash_penalty_bps_per_slot: 10,
            max_blockhash_penalty_bps: 1_000,
            quote_age_penalty_bps_per_slot: 15,
            max_quote_age_penalty_bps: 750,
            tick_cross_penalty_bps_per_tick: 20,
            max_tick_cross_penalty_bps: 1_000,
            max_reserve_usage_penalty_bps: 1_250,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuilderConfig {
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    #[serde(default)]
    pub jito_tip_policy: JitoTipPolicyConfig,
    #[serde(default)]
    pub jito_tip_lamports: u64,
}

impl Default for BuilderConfig {
    fn default() -> Self {
        Self {
            compute_unit_limit: 300_000,
            compute_unit_price_micro_lamports: 25_000,
            jito_tip_policy: JitoTipPolicyConfig::default(),
            jito_tip_lamports: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum JitoTipModeConfig {
    Fixed,
    #[default]
    PnlRatio,
    RiskAdjustedPnlRatio,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct JitoTipPolicyConfig {
    pub mode: JitoTipModeConfig,
    pub share_bps_of_expected_net_profit: u16,
    pub min_lamports: u64,
    pub max_lamports: u64,
}

impl Default for JitoTipPolicyConfig {
    fn default() -> Self {
        Self {
            mode: JitoTipModeConfig::PnlRatio,
            share_bps_of_expected_net_profit: 1_000,
            min_lamports: 5_000,
            max_lamports: 50_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BuildSignConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub congestion_threshold_pct: u8,
}

impl Default for BuildSignConfig {
    fn default() -> Self {
        Self {
            worker_count: default_build_sign_worker_count(),
            queue_capacity: default_build_sign_queue_capacity(),
            congestion_threshold_pct: default_build_sign_congestion_threshold_pct(),
        }
    }
}

fn default_build_sign_worker_count() -> usize {
    2
}

fn default_build_sign_queue_capacity() -> usize {
    8
}

fn default_build_sign_congestion_threshold_pct() -> u8 {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SigningProviderKind {
    Local,
    SecureUnix,
}

impl Default for SigningProviderKind {
    fn default() -> Self {
        Self::Local
    }
}

fn default_signing_connect_timeout_ms() -> u64 {
    50
}

fn default_signing_read_timeout_ms() -> u64 {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SigningConfig {
    pub wallet_id: String,
    pub owner_pubkey: String,
    #[serde(default)]
    pub provider: SigningProviderKind,
    pub keypair_path: Option<String>,
    pub keypair_base58: Option<String>,
    pub socket_path: Option<String>,
    #[serde(default = "default_signing_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_signing_read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "default_validate_execution_accounts")]
    pub validate_execution_accounts: bool,
    pub bootstrap_balance_lamports: u64,
    pub wallet_ready: bool,
}

fn default_validate_execution_accounts() -> bool {
    true
}

impl Default for SigningConfig {
    fn default() -> Self {
        Self {
            wallet_id: "hot-wallet".into(),
            owner_pubkey: String::new(),
            provider: SigningProviderKind::Local,
            keypair_path: None,
            keypair_base58: None,
            socket_path: None,
            connect_timeout_ms: default_signing_connect_timeout_ms(),
            read_timeout_ms: default_signing_read_timeout_ms(),
            validate_execution_accounts: default_validate_execution_accounts(),
            bootstrap_balance_lamports: 1_000_000,
            wallet_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SubmitConfig {
    pub mode: SubmitModeConfig,
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub congestion_threshold_pct: u8,
    pub single_transaction_policy: SingleTransactionSubmitPolicyConfig,
}

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            mode: SubmitModeConfig::SingleTransaction,
            worker_count: default_submit_worker_count(),
            queue_capacity: default_submit_queue_capacity(),
            congestion_threshold_pct: default_submit_congestion_threshold_pct(),
            single_transaction_policy: SingleTransactionSubmitPolicyConfig::default(),
        }
    }
}

fn default_submit_worker_count() -> usize {
    2
}

fn default_submit_queue_capacity() -> usize {
    8
}

fn default_submit_congestion_threshold_pct() -> u8 {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmitModeConfig {
    SingleTransaction,
    Bundle,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SingleTransactionSubmitPolicyConfig {
    #[default]
    JitoOnly,
    RpcOnly,
    Fanout,
    LeaderAware,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct JitoSubmitConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub bundle_enabled: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub retry_attempts: usize,
    pub retry_backoff_ms: u64,
    pub idempotency_cache_size: usize,
}

impl Default for JitoSubmitConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.block-engine.jito.wtf".into(),
            auth_token: None,
            bundle_enabled: true,
            connect_timeout_ms: 300,
            request_timeout_ms: 400,
            retry_attempts: 1,
            retry_backoff_ms: 0,
            idempotency_cache_size: 1_024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RpcSubmitConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub skip_preflight: bool,
    pub max_retries: usize,
    pub idempotency_cache_size: usize,
}

impl Default for RpcSubmitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            connect_timeout_ms: 200,
            request_timeout_ms: 250,
            skip_preflight: true,
            max_retries: 0,
            idempotency_cache_size: 1_024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ReconciliationConfig {
    pub enabled: bool,
    pub rpc_http_endpoint: String,
    pub rpc_ws_endpoint: String,
    pub websocket_enabled: bool,
    pub websocket_timeout_ms: u64,
    pub search_transaction_history: bool,
    pub poll_interval_millis: u64,
    pub max_pending_slots: u64,
    #[serde(default = "default_account_batch_window_millis")]
    pub account_batch_window_millis: u64,
    #[serde(default = "default_account_batch_parallel_rpc_requests")]
    pub account_batch_parallel_rpc_requests: usize,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rpc_http_endpoint: "https://api.mainnet-beta.solana.com".into(),
            rpc_ws_endpoint: "wss://api.mainnet-beta.solana.com".into(),
            websocket_enabled: true,
            websocket_timeout_ms: 5,
            search_transaction_history: true,
            poll_interval_millis: 100,
            max_pending_slots: 150,
            account_batch_window_millis: default_account_batch_window_millis(),
            account_batch_parallel_rpc_requests: default_account_batch_parallel_rpc_requests(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskConfig {
    pub kill_switch_enabled: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            kill_switch_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub profile: RuntimeProfileConfig,
    pub event_source: EventSourceConfig,
    pub health_server: HealthServerConfig,
    pub monitor_server: MonitorServerConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    pub control: RuntimeControlConfig,
    #[serde(default)]
    pub refresh: AsyncRefreshConfig,
    #[serde(default)]
    pub parallelism: RuntimeParallelismConfig,
    #[serde(default)]
    pub live_set_health: LiveSetHealthConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            profile: RuntimeProfileConfig::default(),
            event_source: EventSourceConfig::default(),
            health_server: HealthServerConfig::default(),
            monitor_server: MonitorServerConfig::default(),
            persistence: PersistenceConfig::default(),
            control: RuntimeControlConfig::default(),
            refresh: AsyncRefreshConfig::default(),
            parallelism: RuntimeParallelismConfig::default(),
            live_set_health: LiveSetHealthConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveSetHealthConfig {
    pub enabled: bool,
    pub pool_quarantine_after_refresh_failures: u32,
    pub pool_quarantine_after_repair_failures: u32,
    pub pool_quarantine_slots: u64,
    pub pool_disable_after_quarantine_count: u32,
    pub pool_disable_window_slots: u64,
    pub route_targeted_failure_cooldown_slots: u64,
    pub route_shadow_after_chain_failures: u32,
    pub route_failure_window_slots: u64,
    pub route_reentry_cooldown_slots: u64,
}

impl Default for LiveSetHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pool_quarantine_after_refresh_failures: 0,
            pool_quarantine_after_repair_failures: 3,
            pool_quarantine_slots: 512,
            pool_disable_after_quarantine_count: 3,
            pool_disable_window_slots: 4_096,
            route_targeted_failure_cooldown_slots: 16,
            route_shadow_after_chain_failures: 2,
            route_failure_window_slots: 1_024,
            route_reentry_cooldown_slots: 1_024,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfileConfig {
    Default,
    UltraFast,
}

impl Default for RuntimeProfileConfig {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AsyncRefreshConfig {
    pub enabled: bool,
    pub blockhash_refresh_millis: u64,
    #[serde(default = "default_slot_refresh_millis")]
    pub slot_refresh_millis: u64,
    pub alt_refresh_millis: u64,
    pub wallet_refresh_millis: u64,
}

fn default_slot_refresh_millis() -> u64 {
    500
}

impl Default for AsyncRefreshConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            blockhash_refresh_millis: 500,
            slot_refresh_millis: default_slot_refresh_millis(),
            alt_refresh_millis: 2_000,
            wallet_refresh_millis: 2_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransportModeConfig {
    #[default]
    Inproc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum StateUpdateModeConfig {
    #[default]
    LatestOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeParallelismConfig {
    pub transport_mode: TransportModeConfig,
    pub state_shard_count: usize,
    pub route_eval_worker_count: usize,
    pub trigger_queue_capacity: usize,
    pub state_update_mode: StateUpdateModeConfig,
}

impl Default for RuntimeParallelismConfig {
    fn default() -> Self {
        Self {
            transport_mode: TransportModeConfig::Inproc,
            state_shard_count: 16,
            route_eval_worker_count: 8,
            trigger_queue_capacity: 4_096,
            state_update_mode: StateUpdateModeConfig::LatestOnly,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSourceConfig {
    pub mode: EventSourceMode,
    pub path: Option<String>,
    pub bind_address: Option<String>,
}

impl Default for EventSourceConfig {
    fn default() -> Self {
        Self {
            mode: EventSourceMode::Disabled,
            path: None,
            bind_address: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventSourceMode {
    Disabled,
    JsonlFile,
    Shredstream,
    UdpJson,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthServerConfig {
    pub enabled: bool,
    pub bind_address: String,
}

impl Default for HealthServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "127.0.0.1:8080".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MonitorServerConfig {
    pub enabled: bool,
    pub bind_address: String,
    pub poll_interval_millis: u64,
    pub max_signal_samples: usize,
    pub max_rejections: usize,
    pub max_trades: usize,
}

impl Default for MonitorServerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "127.0.0.1:8081".into(),
            poll_interval_millis: 250,
            max_signal_samples: 4_096,
            max_rejections: 2_048,
            max_trades: 2_048,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PersistenceConfig {
    pub enabled: bool,
    pub path: String,
    pub batch_size: usize,
    pub flush_interval_millis: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "data/bot_observer.sqlite3".into(),
            batch_size: 128,
            flush_interval_millis: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeControlConfig {
    pub idle_sleep_millis: u64,
    pub max_events_per_tick: usize,
    pub kill_switch_sentinel_path: Option<String>,
}

impl Default for RuntimeControlConfig {
    fn default() -> Self {
        Self {
            idle_sleep_millis: 1,
            max_events_per_tick: 128,
            kill_switch_sentinel_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        BotConfig, EventSourceMode, ReducerRolloutMode, RouteKindConfig, RuntimeProfileConfig,
        SingleTransactionSubmitPolicyConfig,
    };

    #[test]
    fn loads_toml_config_from_path() {
        let path = temp_path("bot-config", "toml");
        let mut config = BotConfig::default();
        config.runtime.event_source.mode = EventSourceMode::JsonlFile;
        config.runtime.event_source.path = Some("/tmp/events.jsonl".into());

        fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        let loaded = BotConfig::from_path(&path).unwrap();

        assert_eq!(loaded, config);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn loads_reconciliation_account_batch_parallel_rpc_requests_override() {
        let path = temp_path("bot-config-account-batch-parallel", "toml");
        fs::write(
            &path,
            r#"
[runtime]
profile = "ultra_fast"

[runtime.event_source]
mode = "jsonl_file"
path = "/tmp/events.jsonl"

[reconciliation]
account_batch_parallel_rpc_requests = 4
"#,
        )
        .unwrap();

        let loaded = BotConfig::from_path(&path).unwrap();

        assert_eq!(loaded.reconciliation.account_batch_parallel_rpc_requests, 4);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn ultra_fast_profile_applies_runtime_defaults() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        config.apply_runtime_profile_defaults();

        assert_eq!(config.build_sign.worker_count, 8);
        assert_eq!(config.build_sign.queue_capacity, 256);
        assert_eq!(config.build_sign.congestion_threshold_pct, 90);
        assert_eq!(config.submit.worker_count, 8);
        assert_eq!(config.submit.queue_capacity, 256);
        assert_eq!(config.submit.congestion_threshold_pct, 90);
        assert_eq!(
            config.submit.single_transaction_policy,
            SingleTransactionSubmitPolicyConfig::JitoOnly
        );
        assert_eq!(config.runtime.control.idle_sleep_millis, 0);
        assert_eq!(config.runtime.control.max_events_per_tick, 4_096);
        assert_eq!(config.reconciliation.poll_interval_millis, 25);
        assert_eq!(config.reconciliation.account_batch_parallel_rpc_requests, 1);
        assert_eq!(config.jito.request_timeout_ms, 250);
        assert_eq!(config.jito.retry_attempts, 1);
        assert_eq!(config.jito.retry_backoff_ms, 0);
        assert!(!config.rpc_submit.enabled);
        assert_eq!(config.rpc_submit.request_timeout_ms, 250);
        assert_eq!(config.runtime.refresh.blockhash_refresh_millis, 500);
        assert_eq!(config.runtime.refresh.slot_refresh_millis, 250);
    }

    #[test]
    fn default_submit_and_jito_config_are_fail_fast() {
        let config = BotConfig::default();

        assert_eq!(config.submit.worker_count, 2);
        assert_eq!(config.submit.queue_capacity, 8);
        assert_eq!(config.submit.congestion_threshold_pct, 50);
        assert_eq!(
            config.submit.single_transaction_policy,
            SingleTransactionSubmitPolicyConfig::JitoOnly
        );
        assert_eq!(config.jito.request_timeout_ms, 400);
        assert_eq!(config.jito.retry_attempts, 1);
        assert_eq!(config.jito.retry_backoff_ms, 0);
        assert!(!config.rpc_submit.enabled);
        assert_eq!(config.rpc_submit.request_timeout_ms, 250);
    }

    #[test]
    fn ultra_fast_shredstream_profile_uses_lighter_refresh_budget() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        config.runtime.event_source.mode = EventSourceMode::Shredstream;
        config.apply_runtime_profile_defaults();

        assert_eq!(config.runtime.refresh.blockhash_refresh_millis, 5_000);
        assert_eq!(config.runtime.refresh.slot_refresh_millis, 0);
        assert_eq!(config.runtime.refresh.alt_refresh_millis, 0);
        assert_eq!(config.runtime.refresh.wallet_refresh_millis, 0);
        assert_eq!(
            config.shredstream.reducers.raydium_simple_pool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            config.shredstream.reducers.orca_whirlpool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            config.shredstream.reducers.raydium_clmm,
            ReducerRolloutMode::Active
        );
    }

    #[test]
    fn ultra_fast_shredstream_profile_preserves_explicit_disabled_reducers() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        config.runtime.event_source.mode = EventSourceMode::Shredstream;
        config.shredstream.reducers.raydium_simple_pool = ReducerRolloutMode::Disabled;
        config.shredstream.reducers.orca_whirlpool = ReducerRolloutMode::Disabled;
        config.shredstream.reducers.raydium_clmm = ReducerRolloutMode::Disabled;

        config.apply_runtime_profile_defaults();

        assert_eq!(
            config.shredstream.reducers.raydium_simple_pool,
            ReducerRolloutMode::Disabled
        );
        assert_eq!(
            config.shredstream.reducers.orca_whirlpool,
            ReducerRolloutMode::Disabled
        );
        assert_eq!(
            config.shredstream.reducers.raydium_clmm,
            ReducerRolloutMode::Disabled
        );
    }

    #[test]
    fn ultra_fast_profile_preserves_explicit_rpc_aggressive_overrides() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        config.runtime.event_source.mode = EventSourceMode::Shredstream;
        config.reconciliation.poll_interval_millis = 10;
        config.runtime.refresh.blockhash_refresh_millis = 250;
        config.runtime.refresh.slot_refresh_millis = 25;
        config.runtime.refresh.alt_refresh_millis = 250;
        config.runtime.refresh.wallet_refresh_millis = 500;
        config.submit.queue_capacity = 16;

        config.apply_runtime_profile_defaults();

        assert_eq!(config.reconciliation.poll_interval_millis, 10);
        assert_eq!(config.runtime.refresh.blockhash_refresh_millis, 250);
        assert_eq!(config.runtime.refresh.slot_refresh_millis, 25);
        assert_eq!(config.runtime.refresh.alt_refresh_millis, 250);
        assert_eq!(config.runtime.refresh.wallet_refresh_millis, 500);
        assert_eq!(config.submit.queue_capacity, 16);
    }

    #[test]
    fn loads_live_set_health_refresh_quarantine_overrides() {
        let path = temp_path("bot-config-live-set-health", "toml");
        let source = r#"
[runtime.live_set_health]
pool_quarantine_after_refresh_failures = 4
pool_quarantine_after_repair_failures = 2
pool_quarantine_slots = 1024
"#;

        fs::write(&path, source).unwrap();
        let loaded = BotConfig::from_path(&path).unwrap();

        assert_eq!(
            loaded
                .runtime
                .live_set_health
                .pool_quarantine_after_refresh_failures,
            4
        );
        assert_eq!(
            loaded
                .runtime
                .live_set_health
                .pool_quarantine_after_repair_failures,
            2
        );
        assert_eq!(loaded.runtime.live_set_health.pool_quarantine_slots, 1024);
    }

    #[test]
    fn loads_triangular_route_manifest_fragment() {
        let path = temp_path("bot-config-triangular", "toml");
        let source = r#"
[routes]

[[routes.definitions]]
kind = "triangular"
route_id = "tri-sol-usdc-usdt"
input_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
output_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
base_mint = "So11111111111111111111111111111111111111112"
quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
sol_quote_conversion_pool_id = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
min_profit_quote_atoms = 500
default_trade_size = 1000000
max_trade_size = 5000000
account_dependencies = []

[[routes.definitions.legs]]
venue = "orca_whirlpool"
pool_id = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
side = "buy_base"
input_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
output_mint = "So11111111111111111111111111111111111111112"
fee_bps = 4

[routes.definitions.legs.execution]
kind = "orca_whirlpool"
program_id = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
whirlpool = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
token_mint_a = "So11111111111111111111111111111111111111112"
token_vault_a = "EUuUbDcafPrmVTD5M6qoJAoyyNbihBhugADAxRMn5he9"
token_mint_b = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
token_vault_b = "2WLWEuKDgkDUccTpbwYp1GToYktiSB1cXvreHUwiSUVP"
tick_spacing = 4
a_to_b = false

[[routes.definitions.legs]]
venue = "raydium_clmm"
pool_id = "3nMFqGfpkQkS5s3x2LqAoRrB5bkZ8kDz7mrR7b4qqEgF"
side = "sell_base"
input_mint = "So11111111111111111111111111111111111111112"
output_mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
fee_bps = 4

[routes.definitions.legs.execution]
kind = "raydium_clmm"
program_id = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
token_program_2022_id = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
pool_state = "3nMFqGfpkQkS5s3x2LqAoRrB5bkZ8kDz7mrR7b4qqEgF"
amm_config = "8Fa4bQ4wBq74UeX2TnQ1m4ZjtqQ3k6eXyJ6nLr4vj3ye"
observation_state = "5xFfKLVwV1wXciP6Azh9QhP6vLwJhz6Yk5okioTZD2SW"
ex_bitmap_account = "6L7vD5tqG7H3yWkVudRwyY6B9k4tQ6N7Fv9H8iA1m2Pq"
token_mint_0 = "So11111111111111111111111111111111111111112"
token_vault_0 = "4ct7br2vTPzfdmY3S5HLtTxcGSBfn6pnw98hsS6v359A"
token_mint_1 = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
token_vault_1 = "5it83u57VRrVgc51oNV19TTmAJuffPx5GtGwQr7gQNUo"
tick_spacing = 1
zero_for_one = true

[[routes.definitions.legs]]
venue = "orca_whirlpool"
pool_id = "4fuU3xhx6sqt8gmxsMUbEoQAcfEiZ2tXYWf1XhQxy4T4"
side = "sell_base"
input_mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
output_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
fee_bps = 1

[routes.definitions.legs.execution]
kind = "orca_whirlpool"
program_id = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
whirlpool = "4fuU3xhx6sqt8gmxsMUbEoQAcfEiZ2tXYWf1XhQxy4T4"
token_mint_a = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
token_vault_a = "9SgM2Apxm7UQx6qgB2SUdkGFUTkqJCJSy9FHn5Vf3L7h"
token_mint_b = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
token_vault_b = "8vDk6X6Ka7N8xY2LBVmWHgrnJp4y8S6Wq5Zbgx5caGV5"
tick_spacing = 1
a_to_b = true

[routes.definitions.execution]
default_compute_unit_limit = 450000
minimum_compute_unit_limit = 420000
default_compute_unit_price_micro_lamports = 25000
default_jito_tip_lamports = 5000
"#;

        fs::write(&path, source).unwrap();
        let loaded = BotConfig::from_path(&path).expect("triangular manifest should load");

        assert_eq!(loaded.routes.definitions.len(), 1);
        let route = &loaded.routes.definitions[0];
        assert_eq!(route.kind, RouteKindConfig::Triangular);
        assert_eq!(route.min_profit_quote_atoms, Some(500));
        assert_eq!(route.legs.len(), 3);
        assert_eq!(
            route.legs[1].input_mint.as_deref(),
            Some("So11111111111111111111111111111111111111112")
        );
        assert_eq!(route.execution.minimum_compute_unit_limit, 420_000);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn two_leg_route_kind_defaults_when_omitted() {
        let path = temp_path("bot-config-two-leg-default", "toml");
        let source = r#"
[routes]

[[routes.definitions]]
route_id = "legacy-two-leg"
input_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
output_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
base_mint = "So11111111111111111111111111111111111111112"
quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
sol_quote_conversion_pool_id = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv"
default_trade_size = 1000000
max_trade_size = 5000000
account_dependencies = []

[[routes.definitions.legs]]
venue = "orca_whirlpool"
pool_id = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
side = "buy_base"
fee_bps = 4

[routes.definitions.legs.execution]
kind = "orca_whirlpool"
program_id = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
whirlpool = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
token_mint_a = "So11111111111111111111111111111111111111112"
token_vault_a = "EUuUbDcafPrmVTD5M6qoJAoyyNbihBhugADAxRMn5he9"
token_mint_b = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
token_vault_b = "2WLWEuKDgkDUccTpbwYp1GToYktiSB1cXvreHUwiSUVP"
tick_spacing = 4
a_to_b = false

[[routes.definitions.legs]]
venue = "raydium_clmm"
pool_id = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv"
side = "sell_base"
fee_bps = 4

[routes.definitions.legs.execution]
kind = "raydium_clmm"
program_id = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
token_program_2022_id = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
pool_state = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv"
amm_config = "3h2e43PunVA5K34vwKCLHWhZF4aZpyaC9RmxvshGAQpL"
observation_state = "3Y695CuQ8AP4anbwAqiEBeQF9KxqHFr8piEwvw3UePnQ"
ex_bitmap_account = "4NFvUKqknMpoe6CWTzK758B8ojVLzURL5pC6MtiaJ8TQ"
token_mint_0 = "So11111111111111111111111111111111111111112"
token_vault_0 = "4ct7br2vTPzfdmY3S5HLtTxcGSBfn6pnw98hsS6v359A"
token_mint_1 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
token_vault_1 = "5it83u57VRrVgc51oNV19TTmAJuffPx5GtGwQr7gQNUo"
tick_spacing = 1
zero_for_one = true

[routes.definitions.execution]
default_compute_unit_limit = 300000
default_compute_unit_price_micro_lamports = 25000
default_jito_tip_lamports = 5000
"#;

        fs::write(&path, source).unwrap();
        let loaded = BotConfig::from_path(&path).expect("legacy two-leg manifest should load");

        assert_eq!(loaded.routes.definitions.len(), 1);
        let route = &loaded.routes.definitions[0];
        assert_eq!(route.kind, RouteKindConfig::TwoLeg);
        assert_eq!(route.legs.len(), 2);
        assert_eq!(route.legs[0].input_mint, None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn live_reducer_defaults_are_active() {
        let reducers = super::LiveReducerConfig::default();

        assert_eq!(reducers.orca_simple_pool, ReducerRolloutMode::Active);
        assert_eq!(reducers.raydium_simple_pool, ReducerRolloutMode::Active);
        assert_eq!(reducers.orca_whirlpool, ReducerRolloutMode::Active);
        assert_eq!(reducers.raydium_clmm, ReducerRolloutMode::Active);
    }

    #[test]
    fn loads_partial_toml_config_from_path_by_merging_defaults() {
        let path = temp_path("bot-config-partial", "toml");
        fs::write(
            &path,
            r#"
[runtime]
profile = "ultra_fast"

[shredstream.reducers]
raydium_simple_pool = "active"
orca_simple_pool = "disabled"

[signing]
owner_pubkey = "owner-pubkey"

[routes]

[[routes.definitions]]
route_id = "route-a"
input_mint = "mint-in"
output_mint = "mint-out"
default_trade_size = 10
max_trade_size = 20
account_dependencies = []

[[routes.definitions.legs]]
venue = "raydium"
pool_id = "pool-a"
side = "buy_base"

[routes.definitions.legs.execution]
kind = "raydium_simple_pool"
program_id = "program-a"
token_program_id = "token-program"
amm_pool = "pool-a"
amm_authority = "authority-a"
amm_open_orders = "orders-a"
amm_coin_vault = "coin-vault-a"
amm_pc_vault = "pc-vault-a"
market_program = "market-program-a"
market = "market-a"
market_bids = "bids-a"
market_asks = "asks-a"
market_event_queue = "queue-a"
market_coin_vault = "market-coin-a"
market_pc_vault = "market-pc-a"
market_vault_signer = "signer-a"
user_source_token_account = "user-source-a"
user_destination_token_account = "user-destination-a"

[[routes.definitions.legs]]
venue = "raydium"
pool_id = "pool-b"
side = "sell_base"

[routes.definitions.legs.execution]
kind = "raydium_simple_pool"
program_id = "program-b"
token_program_id = "token-program"
amm_pool = "pool-b"
amm_authority = "authority-b"
amm_open_orders = "orders-b"
amm_coin_vault = "coin-vault-b"
amm_pc_vault = "pc-vault-b"
market_program = "market-program-b"
market = "market-b"
market_bids = "bids-b"
market_asks = "asks-b"
market_event_queue = "queue-b"
market_coin_vault = "market-coin-b"
market_pc_vault = "market-pc-b"
market_vault_signer = "signer-b"
user_source_token_account = "user-source-b"
user_destination_token_account = "user-destination-b"

[routes.definitions.execution]
default_compute_unit_limit = 300000
default_compute_unit_price_micro_lamports = 25000
default_jito_tip_lamports = 5000
"#,
        )
        .unwrap();

        let loaded = BotConfig::from_path(&path).unwrap();

        assert_eq!(loaded.runtime.profile, RuntimeProfileConfig::UltraFast);
        assert_eq!(
            loaded.shredstream.reducers.raydium_simple_pool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            loaded.shredstream.reducers.orca_simple_pool,
            ReducerRolloutMode::Disabled
        );
        assert_eq!(loaded.shredstream.grpc_endpoint, "http://127.0.0.1:50051");
        assert_eq!(loaded.signing.owner_pubkey, "owner-pubkey");
        assert_eq!(
            loaded.submit.single_transaction_policy,
            SingleTransactionSubmitPolicyConfig::JitoOnly
        );
        assert!(!loaded.rpc_submit.enabled);
        assert_eq!(loaded.routes.definitions.len(), 1);
        assert_eq!(loaded.routes.definitions[0].route_id, "route-a");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn loads_amm_fast_manifest_from_path_by_merging_defaults() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("sol_usdc_routes_amm_fast.toml");
        let loaded = BotConfig::from_path(path).unwrap();

        assert_eq!(loaded.runtime.profile, RuntimeProfileConfig::UltraFast);
        assert_eq!(
            loaded.shredstream.reducers.raydium_simple_pool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            loaded.shredstream.reducers.raydium_clmm,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            loaded.shredstream.reducers.orca_whirlpool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            loaded.submit.single_transaction_policy,
            SingleTransactionSubmitPolicyConfig::JitoOnly
        );
        assert!(!loaded.rpc_submit.enabled);
        assert_eq!(
            loaded.reconciliation.rpc_http_endpoint,
            "http://127.0.0.1:8899"
        );
        assert_eq!(loaded.reconciliation.rpc_ws_endpoint, "ws://127.0.0.1:8900");
        assert_eq!(loaded.reconciliation.poll_interval_millis, 10);
        assert_eq!(loaded.reconciliation.account_batch_window_millis, 5);
        assert_eq!(loaded.reconciliation.account_batch_parallel_rpc_requests, 1);
        assert_eq!(loaded.runtime.refresh.blockhash_refresh_millis, 250);
        assert_eq!(loaded.runtime.refresh.slot_refresh_millis, 25);
        assert_eq!(loaded.runtime.refresh.alt_refresh_millis, 250);
        assert_eq!(loaded.runtime.refresh.wallet_refresh_millis, 500);
        assert_eq!(loaded.submit.queue_capacity, 16);
        assert_eq!(loaded.shredstream.idle_refresh_slot_lag, 24);
        assert_eq!(loaded.shredstream.max_repair_in_flight, 4);
        assert_eq!(
            loaded.shredstream.reducers.orca_simple_pool,
            ReducerRolloutMode::Active
        );
        assert_eq!(
            loaded.signing.owner_pubkey,
            "3KD9WKqrrErCsRp7oFoJ58RLqrNJfz8jS3Do8oJ3Gzx1"
        );
        assert_eq!(loaded.routes.definitions.len(), 8);
    }

    #[test]
    fn loads_12_pairs_fast_manifest_with_local_reconciliation_endpoints() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("amm_12_pairs_fast.toml");
        let loaded = BotConfig::from_path(path).unwrap();

        assert_eq!(loaded.runtime.profile, RuntimeProfileConfig::UltraFast);
        assert_eq!(
            loaded.reconciliation.rpc_http_endpoint,
            "http://127.0.0.1:8899"
        );
        assert_eq!(loaded.reconciliation.rpc_ws_endpoint, "ws://127.0.0.1:8900");
        assert!(!loaded.rpc_submit.enabled);
    }

    #[test]
    fn loads_generated_triangular_manifest_with_closed_three_leg_routes() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("amm_12_pairs_triangular_fast.toml");
        let loaded = BotConfig::from_path(path).unwrap();

        assert_eq!(loaded.runtime.profile, RuntimeProfileConfig::UltraFast);
        assert_eq!(loaded.routes.definitions.len(), 96);
        assert!(
            loaded
                .routes
                .definitions
                .iter()
                .all(|route| route.kind == RouteKindConfig::Triangular)
        );
        assert!(
            loaded
                .routes
                .definitions
                .iter()
                .all(|route| route.legs.len() == 3)
        );
        assert!(
            loaded
                .routes
                .definitions
                .iter()
                .all(|route| route.input_mint == route.output_mint)
        );
        assert!(loaded.routes.definitions.iter().all(|route| {
            route.execution.minimum_compute_unit_limit >= 420_000
                && route
                    .legs
                    .windows(2)
                    .all(|legs| legs[0].output_mint == legs[1].input_mint)
                && route.legs[2].output_mint.as_deref() == Some(route.input_mint.as_str())
        }));
    }

    fn temp_path(prefix: &str, extension: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "{prefix}-{}-{}.{}",
            std::process::id(),
            unique_suffix(),
            extension
        ));
        path
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
    }
}
