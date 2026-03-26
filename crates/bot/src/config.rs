use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotConfig {
    pub shredstream: ShredstreamConfig,
    pub state: StateConfig,
    pub routes: RoutesConfig,
    pub strategy: StrategyConfig,
    pub builder: BuilderConfig,
    pub signing: SigningConfig,
    pub submit: SubmitConfig,
    pub jito: JitoSubmitConfig,
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
            submit: SubmitConfig::default(),
            jito: JitoSubmitConfig::default(),
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
}

impl BotConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => toml::from_str(&text).map_err(|source| ConfigError::Toml {
                path: path.display().to_string(),
                source,
            }),
            Some("json") => serde_json::from_str(&text).map_err(|source| ConfigError::Json {
                path: path.display().to_string(),
                source,
            }),
            _ => Err(ConfigError::UnsupportedFormat {
                path: path.display().to_string(),
            }),
        }
    }

    pub fn apply_runtime_profile_defaults(&mut self) {
        if self.runtime.profile != RuntimeProfileConfig::UltraFast {
            return;
        }

        self.runtime.control.idle_sleep_millis = 0;
        self.runtime.control.max_events_per_tick = 4_096;
        self.reconciliation.poll_interval_millis = 25;
        self.runtime.refresh.enabled = true;
        self.runtime.refresh.blockhash_refresh_millis = 200;
        self.runtime.refresh.slot_refresh_millis = 100;
        self.runtime.refresh.alt_refresh_millis = 1_000;
        self.runtime.refresh.wallet_refresh_millis = 1_000;
    }
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
            raydium_simple_pool: ReducerRolloutMode::Shadow,
            orca_whirlpool: ReducerRolloutMode::Shadow,
            raydium_clmm: ReducerRolloutMode::Shadow,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteConfig {
    #[serde(default = "default_route_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub route_class: RouteClassConfig,
    pub route_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub base_mint: Option<String>,
    pub quote_mint: Option<String>,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    #[serde(default)]
    pub size_ladder: Vec<u64>,
    pub legs: [RouteLegConfig; 2],
    pub account_dependencies: Vec<AccountDependencyConfig>,
    pub execution: RouteExecutionConfig,
}

fn default_route_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteLegConfig {
    pub venue: String,
    pub pool_id: String,
    pub side: SwapSideConfig,
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
    pub user_source_token_account: String,
    pub user_destination_token_account: String,
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
    pub min_profit_lamports: i64,
    pub max_snapshot_slot_lag: u64,
    pub require_route_warm: bool,
    pub max_inflight_submissions: usize,
    pub min_wallet_balance_lamports: u64,
    pub max_blockhash_slot_lag: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_profit_lamports: 10,
            max_snapshot_slot_lag: 2,
            require_route_warm: true,
            max_inflight_submissions: 64,
            min_wallet_balance_lamports: 1,
            max_blockhash_slot_lag: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuilderConfig {
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub jito_tip_lamports: u64,
}

impl Default for BuilderConfig {
    fn default() -> Self {
        Self {
            compute_unit_limit: 300_000,
            compute_unit_price_micro_lamports: 25_000,
            jito_tip_lamports: 5_000,
        }
    }
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
    pub bootstrap_balance_lamports: u64,
    pub wallet_ready: bool,
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
            bootstrap_balance_lamports: 1_000_000,
            wallet_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitConfig {
    pub mode: SubmitModeConfig,
}

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            mode: SubmitModeConfig::SingleTransaction,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmitModeConfig {
    SingleTransaction,
    Bundle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct JitoSubmitConfig {
    pub endpoint: String,
    pub ws_endpoint: String,
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
            ws_endpoint: "wss://bundles.jito.wtf/api/v1/bundles/tip_stream".into(),
            auth_token: None,
            bundle_enabled: true,
            connect_timeout_ms: 300,
            request_timeout_ms: 1_000,
            retry_attempts: 3,
            retry_backoff_ms: 50,
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
    pub control: RuntimeControlConfig,
    #[serde(default)]
    pub refresh: AsyncRefreshConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            profile: RuntimeProfileConfig::default(),
            event_source: EventSourceConfig::default(),
            health_server: HealthServerConfig::default(),
            monitor_server: MonitorServerConfig::default(),
            control: RuntimeControlConfig::default(),
            refresh: AsyncRefreshConfig::default(),
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

    use super::{BotConfig, EventSourceMode, RuntimeProfileConfig};

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
    fn ultra_fast_profile_applies_runtime_defaults() {
        let mut config = BotConfig::default();
        config.runtime.profile = RuntimeProfileConfig::UltraFast;
        config.apply_runtime_profile_defaults();

        assert_eq!(config.runtime.control.idle_sleep_millis, 0);
        assert_eq!(config.runtime.control.max_events_per_tick, 4_096);
        assert_eq!(config.reconciliation.poll_interval_millis, 25);
        assert_eq!(config.runtime.refresh.blockhash_refresh_millis, 200);
        assert_eq!(config.runtime.refresh.slot_refresh_millis, 100);
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
