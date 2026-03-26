use std::{
    collections::BTreeSet,
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use detection::{EventSourceKind, IngestError, MarketEventSource, NormalizedEvent};
use thiserror::Error;

use crate::{
    bootstrap::{BootstrapError, bootstrap},
    config::{BotConfig, RuntimeControlConfig},
    control::{RuntimeIssue, RuntimeMode, RuntimeStatus, SharedRuntimeStatus},
    observer::ObserverHandle,
    refresh::AsyncStateRefresher,
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
    Bootstrap(#[from] BootstrapError),
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
    refresher: AsyncStateRefresher,
    source: Box<dyn MarketEventSource>,
    status: SharedRuntimeStatus,
    observer: ObserverHandle,
    control: RuntimeControlConfig,
    kill_switch_sentinel_path: Option<PathBuf>,
    min_wallet_balance_lamports: u64,
    max_blockhash_slot_lag: u64,
    reconciliation_poll_interval: Duration,
    last_reconcile_at: Option<Instant>,
    last_shredstream_sequence: Option<u64>,
    last_shredstream_seen_at: Option<SystemTime>,
}

impl BotDaemon {
    pub fn from_config(config: BotConfig) -> Result<Self, DaemonError> {
        let mut config = config;
        config.apply_runtime_profile_defaults();
        let source = build_event_source(&config)?;
        let mut runtime = bootstrap(config.clone())?;
        runtime.apply_kill_switch(config.risk.kill_switch_enabled);
        let lookup_table_keys = config
            .routes
            .definitions
            .iter()
            .flat_map(|route| {
                route
                    .execution
                    .lookup_tables
                    .iter()
                    .map(|table| table.account_key.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let refresher = AsyncStateRefresher::new(
            &config.runtime.refresh,
            &config.reconciliation.rpc_http_endpoint,
            runtime.wallet_pubkey(),
            &lookup_table_keys,
        );

        let status = SharedRuntimeStatus::new(RuntimeStatus::default());
        if let Err(source) = status.spawn_http_server(&config.runtime.health_server) {
            return Err(DaemonError::HealthServer {
                bind_address: config.runtime.health_server.bind_address.clone(),
                source,
            });
        }
        let observer =
            ObserverHandle::spawn(&config.runtime.monitor_server, &config).map_err(|source| {
                DaemonError::HealthServer {
                    bind_address: config.runtime.monitor_server.bind_address.clone(),
                    source,
                }
            })?;

        let daemon = Self {
            runtime,
            refresher,
            source,
            status,
            observer,
            control: config.runtime.control.clone(),
            kill_switch_sentinel_path: config
                .runtime
                .control
                .kill_switch_sentinel_path
                .as_ref()
                .map(PathBuf::from),
            min_wallet_balance_lamports: config.strategy.min_wallet_balance_lamports,
            max_blockhash_slot_lag: config.strategy.max_blockhash_slot_lag,
            reconciliation_poll_interval: Duration::from_millis(
                config.reconciliation.poll_interval_millis,
            ),
            last_reconcile_at: None,
            last_shredstream_sequence: None,
            last_shredstream_seen_at: None,
        };
        daemon.refresh_status(None, None, None);
        Ok(daemon)
    }

    pub fn status_snapshot(&self) -> RuntimeStatus {
        self.status.snapshot()
    }

    pub fn run(&mut self) -> Result<DaemonExit, DaemonError> {
        loop {
            self.runtime
                .apply_async_refresh(self.refresher.drain_snapshot());
            self.reconcile_if_due();
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
                        if event.source.source == EventSourceKind::ShredStream {
                            self.record_shredstream_metrics(&event);
                        }
                        match self.runtime.process_event(event) {
                            Ok(report) => {
                                self.observer.publish_hot_path(report);
                                self.refresh_status(None, None, None);
                            }
                            Err(error) => {
                                let detail = error.to_string();
                                self.refresh_status(
                                    Some(RuntimeMode::Degraded),
                                    Some(RuntimeIssue::RuntimeFailure {
                                        detail: detail.clone(),
                                    }),
                                    Some(detail),
                                );
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(IngestError::Exhausted { .. }) => {
                        self.reconcile_now();
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
                if self.control.idle_sleep_millis == 0 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_millis(self.control.idle_sleep_millis));
                }
            }
        }
    }

    fn reconcile_if_due(&mut self) {
        if !self.reconciliation_due() {
            return;
        }

        self.reconcile_now();
    }

    fn reconcile_now(&mut self) {
        let observed_slot = self.runtime.latest_slot();
        let transitions = self.runtime.reconcile(observed_slot);
        for transition in transitions {
            if let Some(record) = self.runtime.execution_record(&transition.submission_id) {
                self.observer.publish_trade_update(record);
            }
        }
        self.last_reconcile_at = Some(Instant::now());
    }

    fn reconciliation_due(&self) -> bool {
        if self.reconciliation_poll_interval == Duration::ZERO {
            return true;
        }

        match self.last_reconcile_at {
            None => true,
            Some(last) => last.elapsed() >= self.reconciliation_poll_interval,
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

    fn record_shredstream_metrics(&mut self, event: &NormalizedEvent) {
        let observed_at = event.latency.source_received_at;
        let normalized_latency = event
            .latency
            .normalized_at
            .duration_since(event.latency.source_received_at)
            .unwrap_or(Duration::ZERO);

        self.runtime.telemetry().metrics.record_shredstream_event(
            observed_at,
            observed_at,
            self.last_shredstream_seen_at,
            normalized_latency,
        );

        match self.last_shredstream_sequence {
            None => {}
            Some(last) if event.source.sequence > last.saturating_add(1) => {
                self.runtime
                    .telemetry()
                    .metrics
                    .increment_shredstream_sequence_gap();
            }
            Some(last) if event.source.sequence == last => {
                self.runtime
                    .telemetry()
                    .metrics
                    .increment_shredstream_sequence_duplicate();
            }
            Some(last) if event.source.sequence < last => {
                self.runtime
                    .telemetry()
                    .metrics
                    .increment_shredstream_sequence_reorder();
            }
            Some(_) => {}
        }

        self.last_shredstream_sequence = Some(event.source.sequence);
        self.last_shredstream_seen_at = Some(observed_at);
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
        let blockhash_slot_lag = execution_state.blockhash_slot_lag();
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

        let snapshot = RuntimeStatus {
            mode,
            live: true,
            ready,
            issue: derived_issue,
            kill_switch_active: execution_state.kill_switch_enabled,
            latest_slot: self.runtime.latest_slot(),
            rpc_slot: execution_state.rpc_slot,
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
        };
        self.status.replace(snapshot.clone());
        self.observer.publish_status(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use solana_sdk::{hash::hashv, pubkey::Pubkey, signer::keypair::Keypair};
    use std::{
        env, fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        sync::Arc,
        thread,
    };

    use crate::config::{
        AccountDependencyConfig, BotConfig, EventSourceMode, MessageModeConfig,
        OrcaSimplePoolLegExecutionConfig, RaydiumSimplePoolLegExecutionConfig, RouteClassConfig,
        RouteConfig, RouteExecutionConfig, RouteLegConfig, RouteLegExecutionConfig, RoutesConfig,
        SwapSideConfig,
    };

    use super::{BotDaemon, DaemonExit};

    #[test]
    fn daemon_processes_jsonl_replay_and_reaches_submit() {
        let path = temp_path("bot-daemon-replay", "jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                replay_event("acct-a", 1, 12_000),
                replay_event("acct-b", 2, 12_000)
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
                replay_event("acct-a", 1, 12_000),
                replay_event("acct-b", 2, 12_000)
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

    #[test]
    fn daemon_reconciles_pending_submissions_during_idle_loop() {
        let path = temp_path("bot-daemon-reconcile", "jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                replay_event("acct-a", 1, 12_000),
                replay_event("acct-b", 2, 12_000)
            ),
        )
        .unwrap();

        let rpc_endpoint = spawn_mock_rpc_server(|body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getSignatureStatuses" => {
                    json!({ "result": { "value": [{ "slot": 99, "err": null }] } }).to_string()
                }
                other => panic!("unexpected method {other}"),
            }
        });

        let mut config = bot_config(path.to_string_lossy().into_owned());
        config.runtime.health_server.enabled = false;
        config.reconciliation.rpc_http_endpoint = rpc_endpoint;
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        config.reconciliation.websocket_enabled = false;
        config.reconciliation.poll_interval_millis = 0;
        let mut daemon = BotDaemon::from_config(config).unwrap();

        let exit = daemon.run().unwrap();
        let status = daemon.status_snapshot();

        assert_eq!(exit, DaemonExit::SourceExhausted);
        assert_eq!(status.metrics.submit_count, 1);
        assert_eq!(status.metrics.inclusion_count, 1);
        assert_eq!(status.inflight_submissions, 0);

        let _ = fs::remove_file(path);
    }

    fn bot_config(path: String) -> BotConfig {
        let mut config = BotConfig::default();
        let keypair = Keypair::new_from_array([7; 32]);
        config.jito.endpoint = "mock://jito".into();
        config.jito.ws_endpoint = "mock://jito-tip-stream".into();
        config.builder.compute_unit_limit = 1;
        config.builder.compute_unit_price_micro_lamports = 1;
        config.builder.jito_tip_lamports = 1;
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        config.runtime.refresh.enabled = false;
        config.signing.keypair_base58 = Some(keypair.to_base58_string());
        config.routes = RoutesConfig {
            definitions: vec![RouteConfig {
                enabled: true,
                route_class: RouteClassConfig::AmmFastPath,
                route_id: "route-a".into(),
                input_mint: "USDC".into(),
                output_mint: "USDC".into(),
                base_mint: None,
                quote_mint: None,
                default_trade_size: 10_000,
                max_trade_size: 20_000,
                size_ladder: Vec::new(),
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
                                pool_destination_token_account: test_pubkey(
                                    "orca-pool-destination",
                                ),
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
                execution: RouteExecutionConfig {
                    message_mode: MessageModeConfig::V0Required,
                    lookup_tables: Vec::new(),
                    default_compute_unit_limit: 300_000,
                    default_compute_unit_price_micro_lamports: 25_000,
                    default_jito_tip_lamports: 5_000,
                    max_quote_slot_lag: 4,
                    max_alt_slot_lag: 4,
                },
            }],
        };
        config.runtime.event_source.mode = EventSourceMode::JsonlFile;
        config.runtime.event_source.path = Some(path);
        config
    }

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
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

            if let Some(end) = header_end {
                if request.len() >= end + content_length {
                    let body = &request[end..end + content_length];
                    return String::from_utf8_lossy(body).into_owned();
                }
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
}
