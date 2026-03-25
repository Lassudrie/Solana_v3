use std::{
    path::PathBuf,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use detection::{IngestError, MarketEventSource};
use thiserror::Error;

use crate::{
    bootstrap::bootstrap,
    config::{BotConfig, RuntimeControlConfig},
    control::{RuntimeIssue, RuntimeMode, RuntimeStatus, SharedRuntimeStatus},
    runtime::BotRuntime,
    sources::{EventSourceConfigError, build_event_source},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonExit {
    SourceExhausted,
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    EventSourceConfig(#[from] EventSourceConfigError),
    #[error("failed to start health server on {bind_address}: {source}")]
    HealthServer {
        bind_address: String,
        #[source]
        source: std::io::Error,
    },
}

pub struct BotDaemon {
    runtime: BotRuntime,
    source: Box<dyn MarketEventSource>,
    status: SharedRuntimeStatus,
    control: RuntimeControlConfig,
    kill_switch_sentinel_path: Option<PathBuf>,
    min_wallet_balance_lamports: u64,
    max_blockhash_slot_lag: u64,
}

impl BotDaemon {
    pub fn from_config(config: BotConfig) -> Result<Self, DaemonError> {
        let source = build_event_source(&config.runtime.event_source, &config.shredstream)?;
        let mut runtime = bootstrap(config.clone());
        runtime.apply_kill_switch(config.risk.kill_switch_enabled);

        let status = SharedRuntimeStatus::new(RuntimeStatus::default());
        if let Err(source) = status.spawn_http_server(&config.runtime.health_server) {
            return Err(DaemonError::HealthServer {
                bind_address: config.runtime.health_server.bind_address.clone(),
                source,
            });
        }

        let daemon = Self {
            runtime,
            source,
            status,
            control: config.runtime.control.clone(),
            kill_switch_sentinel_path: config
                .runtime
                .control
                .kill_switch_sentinel_path
                .as_ref()
                .map(PathBuf::from),
            min_wallet_balance_lamports: config.strategy.min_wallet_balance_lamports,
            max_blockhash_slot_lag: config.strategy.max_blockhash_slot_lag,
        };
        daemon.refresh_status(None, None, None);
        Ok(daemon)
    }

    pub fn status_snapshot(&self) -> RuntimeStatus {
        self.status.snapshot()
    }

    pub fn run(&mut self) -> Result<DaemonExit, DaemonError> {
        loop {
            let kill_switch_active = self.refresh_kill_switch();
            if kill_switch_active {
                self.refresh_status(None, Some(RuntimeIssue::KillSwitchActive), None);
            } else {
                self.refresh_status(None, None, None);
            }

            let mut processed = 0usize;
            for _ in 0..self.control.max_events_per_tick {
                match self.source.poll_next() {
                    Ok(Some(event)) => {
                        processed += 1;
                        if let Err(error) = self.runtime.process_event(event) {
                            let detail = error.to_string();
                            self.refresh_status(
                                Some(RuntimeMode::Degraded),
                                Some(RuntimeIssue::RuntimeFailure {
                                    detail: detail.clone(),
                                }),
                                Some(detail),
                            );
                        } else {
                            self.refresh_status(None, None, None);
                        }
                    }
                    Ok(None) => break,
                    Err(IngestError::Exhausted { .. }) => {
                        self.refresh_status(
                            Some(RuntimeMode::Stopped),
                            Some(RuntimeIssue::EventSourceExhausted),
                            None,
                        );
                        return Ok(DaemonExit::SourceExhausted);
                    }
                    Err(error) => {
                        let detail = error.to_string();
                        self.refresh_status(
                            Some(RuntimeMode::Degraded),
                            Some(RuntimeIssue::EventSourceFailure {
                                detail: detail.clone(),
                            }),
                            Some(detail),
                        );
                        break;
                    }
                }
            }

            if processed == 0 {
                thread::sleep(Duration::from_millis(self.control.idle_sleep_millis));
            }
        }
    }

    fn refresh_kill_switch(&mut self) -> bool {
        let active = self
            .kill_switch_sentinel_path
            .as_ref()
            .map(|path| path.exists())
            .unwrap_or(false);
        self.runtime.apply_kill_switch(active);
        active
    }

    fn refresh_status(
        &self,
        mode_override: Option<RuntimeMode>,
        issue_override: Option<RuntimeIssue>,
        last_error: Option<String>,
    ) {
        let execution_state = self.runtime.execution_state();
        let total_routes = self.runtime.route_count();
        let ready_routes = self.runtime.ready_route_count();
        let blockhash_slot_lag = execution_state
            .blockhash_slot
            .map(|slot| execution_state.head_slot.saturating_sub(slot));
        let derived_issue = issue_override.or_else(|| {
            if execution_state.kill_switch_enabled {
                return Some(RuntimeIssue::KillSwitchActive);
            }
            if total_routes == 0 {
                return Some(RuntimeIssue::NoRoutesConfigured);
            }
            if ready_routes < total_routes {
                return Some(RuntimeIssue::RoutesNotWarm {
                    ready_routes,
                    total_routes,
                });
            }
            if !execution_state.wallet_ready {
                return Some(RuntimeIssue::WalletNotReady);
            }
            if execution_state.wallet_balance_lamports < self.min_wallet_balance_lamports {
                return Some(RuntimeIssue::WalletBalanceTooLow {
                    current: execution_state.wallet_balance_lamports,
                    minimum: self.min_wallet_balance_lamports,
                });
            }
            match blockhash_slot_lag {
                None => Some(RuntimeIssue::BlockhashUnavailable),
                Some(slot_lag) if slot_lag > self.max_blockhash_slot_lag => {
                    Some(RuntimeIssue::BlockhashTooStale {
                        slot_lag,
                        maximum: self.max_blockhash_slot_lag,
                    })
                }
                _ => None,
            }
        });
        let ready = derived_issue.is_none();
        let mode = mode_override.unwrap_or_else(|| {
            if ready {
                RuntimeMode::Ready
            } else if matches!(
                derived_issue,
                Some(
                    RuntimeIssue::KillSwitchActive
                        | RuntimeIssue::EventSourceFailure { .. }
                        | RuntimeIssue::RuntimeFailure { .. }
                )
            ) {
                RuntimeMode::Degraded
            } else {
                RuntimeMode::Starting
            }
        });

        self.status.replace(RuntimeStatus {
            mode,
            live: true,
            ready,
            issue: derived_issue,
            kill_switch_active: execution_state.kill_switch_enabled,
            latest_slot: self.runtime.latest_slot(),
            total_routes,
            ready_routes,
            inflight_submissions: self.runtime.pending_submissions(),
            wallet_ready: execution_state.wallet_ready,
            wallet_balance_lamports: execution_state.wallet_balance_lamports,
            blockhash_slot: execution_state.blockhash_slot,
            blockhash_slot_lag,
            metrics: crate::control::RuntimeMetrics::from_snapshot(
                self.runtime.telemetry().metrics.snapshot(),
            ),
            last_error,
            updated_at_unix_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_millis(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, path::PathBuf};

    use crate::config::{
        AccountDependencyConfig, BotConfig, EventSourceMode, RouteConfig, RouteLegConfig,
        RoutesConfig, SwapSideConfig,
    };

    use super::{BotDaemon, DaemonExit};

    #[test]
    fn daemon_processes_jsonl_replay_and_reaches_submit() {
        let path = temp_path("bot-daemon-replay", "jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                replay_event("acct-a", 1, 10_150),
                replay_event("acct-b", 2, 10_080)
            ),
        )
        .unwrap();

        let mut config = bot_config(path.to_string_lossy().into_owned());
        config.runtime.health_server.enabled = false;
        let mut daemon = BotDaemon::from_config(config).unwrap();

        let exit = daemon.run().unwrap();
        let status = daemon.status_snapshot();

        assert_eq!(exit, DaemonExit::SourceExhausted);
        assert_eq!(status.total_routes, 1);
        assert_eq!(status.ready_routes, 1);
        assert_eq!(status.metrics.detect_events, 2);
        assert_eq!(status.metrics.submit_count, 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn kill_switch_sentinel_prevents_submission() {
        let path = temp_path("bot-daemon-replay-kill", "jsonl");
        let kill_switch = temp_path("bot-daemon-kill-switch", "flag");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                replay_event("acct-a", 1, 10_150),
                replay_event("acct-b", 2, 10_080)
            ),
        )
        .unwrap();
        fs::write(&kill_switch, b"1").unwrap();

        let mut config = bot_config(path.to_string_lossy().into_owned());
        config.runtime.health_server.enabled = false;
        config.runtime.control.kill_switch_sentinel_path =
            Some(kill_switch.to_string_lossy().into_owned());
        let mut daemon = BotDaemon::from_config(config).unwrap();

        let exit = daemon.run().unwrap();
        let status = daemon.status_snapshot();

        assert_eq!(exit, DaemonExit::SourceExhausted);
        assert!(status.kill_switch_active);
        assert_eq!(status.metrics.submit_count, 0);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(kill_switch);
    }

    fn bot_config(path: String) -> BotConfig {
        let mut config = BotConfig::default();
        config.routes = RoutesConfig {
            definitions: vec![RouteConfig {
                route_id: "route-a".into(),
                input_mint: "USDC".into(),
                output_mint: "USDC".into(),
                default_trade_size: 10_000,
                max_trade_size: 20_000,
                legs: [
                    RouteLegConfig {
                        venue: "orca".into(),
                        pool_id: "pool-a".into(),
                        side: SwapSideConfig::BuyBase,
                    },
                    RouteLegConfig {
                        venue: "raydium".into(),
                        pool_id: "pool-b".into(),
                        side: SwapSideConfig::SellBase,
                    },
                ],
                account_dependencies: vec![
                    AccountDependencyConfig {
                        account_key: "acct-a".into(),
                        pool_id: "pool-a".into(),
                        decoder_key: "pool-price-v1".into(),
                    },
                    AccountDependencyConfig {
                        account_key: "acct-b".into(),
                        pool_id: "pool-b".into(),
                        decoder_key: "pool-price-v1".into(),
                    },
                ],
            }],
        };
        config.runtime.event_source.mode = EventSourceMode::JsonlFile;
        config.runtime.event_source.path = Some(path);
        config
    }

    fn replay_event(pubkey: &str, sequence: u64, price_bps: u64) -> String {
        format!(
            "{{\"type\":\"account_update\",\"source\":\"replay\",\"sequence\":{sequence},\"observed_slot\":{sequence},\"pubkey\":\"{pubkey}\",\"owner\":\"owner\",\"lamports\":0,\"data_hex\":\"{}\",\"slot\":{sequence},\"write_version\":1}}",
            encode_pool_hex(price_bps, 4, 100_000)
        )
    }

    fn encode_pool_hex(price_bps: u64, fee_bps: u16, reserve_depth: u64) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&price_bps.to_le_bytes());
        bytes.extend_from_slice(&fee_bps.to_le_bytes());
        bytes.extend_from_slice(&reserve_depth.to_le_bytes());
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            write!(&mut hex, "{byte:02x}").expect("write hex");
        }
        hex
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
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
    }
}
