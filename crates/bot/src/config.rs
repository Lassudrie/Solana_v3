use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotConfig {
    pub detection: DetectionConfig,
    pub shredstream: ShredstreamConfig,
    pub state: StateConfig,
    pub routes: RoutesConfig,
    pub strategy: StrategyConfig,
    pub builder: BuilderConfig,
    pub signing: SigningConfig,
    pub submit: SubmitConfig,
    pub jito: JitoSubmitConfig,
    pub telemetry: TelemetryConfig,
    pub reconciliation: ReconciliationConfig,
    pub risk: RiskConfig,
    pub runtime: RuntimeConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            detection: DetectionConfig::default(),
            shredstream: ShredstreamConfig::default(),
            state: StateConfig::default(),
            routes: RoutesConfig::default(),
            strategy: StrategyConfig::default(),
            builder: BuilderConfig::default(),
            signing: SigningConfig::default(),
            submit: SubmitConfig::default(),
            jito: JitoSubmitConfig::default(),
            telemetry: TelemetryConfig::default(),
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectionConfig {
    pub primary_source: String,
    pub max_batch: usize,
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            primary_source: "shredstream".into(),
            max_batch: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShredstreamConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub replay_on_gap: bool,
}

impl Default for ShredstreamConfig {
    fn default() -> Self {
        Self {
            endpoint: "udp://127.0.0.1:10000".into(),
            auth_token: None,
            replay_on_gap: false,
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
            bootstrap_blockhash: "bootstrap-blockhash".into(),
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
pub struct RouteConfig {
    pub route_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    pub legs: [RouteLegConfig; 2],
    pub account_dependencies: Vec<AccountDependencyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteLegConfig {
    pub venue: String,
    pub pool_id: String,
    pub side: SwapSideConfig,
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
pub struct SigningConfig {
    pub wallet_id: String,
    pub owner_pubkey: String,
    pub bootstrap_balance_lamports: u64,
    pub wallet_ready: bool,
}

impl Default for SigningConfig {
    fn default() -> Self {
        Self {
            wallet_id: "hot-wallet".into(),
            owner_pubkey: "wallet-owner".into(),
            bootstrap_balance_lamports: 1_000_000,
            wallet_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitConfig {
    pub mode: SubmitModeConfig,
    pub max_inflight: usize,
}

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            mode: SubmitModeConfig::SingleTransaction,
            max_inflight: 64,
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
pub struct JitoSubmitConfig {
    pub endpoint: String,
    pub bundle_enabled: bool,
}

impl Default for JitoSubmitConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.block-engine.jito.wtf".into(),
            bundle_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TelemetryConfig {
    pub enable_memory_logs: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enable_memory_logs: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationConfig {
    pub retain_history_limit: usize,
    pub classify_failures: bool,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            retain_history_limit: 10_000,
            classify_failures: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskConfig {
    pub kill_switch_enabled: bool,
    pub fail_closed: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            kill_switch_enabled: false,
            fail_closed: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub event_source: EventSourceConfig,
    pub health_server: HealthServerConfig,
    pub control: RuntimeControlConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            event_source: EventSourceConfig::default(),
            health_server: HealthServerConfig::default(),
            control: RuntimeControlConfig::default(),
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
pub struct RuntimeControlConfig {
    pub idle_sleep_millis: u64,
    pub max_events_per_tick: usize,
    pub kill_switch_sentinel_path: Option<String>,
}

impl Default for RuntimeControlConfig {
    fn default() -> Self {
        Self {
            idle_sleep_millis: 100,
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

    use super::{BotConfig, EventSourceMode};

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
