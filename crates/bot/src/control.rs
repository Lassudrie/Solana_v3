use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use telemetry::{MetricsSnapshot, PipelineStage};

use crate::config::HealthServerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Starting,
    Ready,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuntimeIssue {
    KillSwitchActive,
    NoRoutesConfigured,
    RoutesNotWarm {
        ready_routes: usize,
        total_routes: usize,
    },
    WalletNotReady,
    WalletBalanceTooLow {
        current: u64,
        minimum: u64,
    },
    BlockhashUnavailable,
    BlockhashTooStale {
        slot_lag: u64,
        maximum: u64,
    },
    EventSourceExhausted,
    EventSourceFailure {
        detail: String,
    },
    RuntimeFailure {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeMetrics {
    pub detect_events: u64,
    pub stale_updates: u64,
    pub rejection_count: u64,
    pub build_count: u64,
    pub submit_count: u64,
    pub inclusion_count: u64,
    pub shredstream_events: u64,
    pub shredstream_sequence_gaps: u64,
    pub shredstream_sequence_reorders: u64,
    pub shredstream_sequence_duplicates: u64,
    pub shredstream_ingest_latency_nanos: u64,
    pub shredstream_ingest_latency_count: u64,
    pub shredstream_interarrival_latency_nanos: u64,
    pub shredstream_interarrival_latency_count: u64,
    pub shredstream_events_per_second: u64,
    pub stage_latency_nanos: BTreeMap<String, u128>,
}

impl RuntimeMetrics {
    pub fn from_snapshot(snapshot: MetricsSnapshot) -> Self {
        Self {
            detect_events: snapshot.detect_events,
            stale_updates: snapshot.stale_updates,
            rejection_count: snapshot.rejection_count,
            build_count: snapshot.build_count,
            submit_count: snapshot.submit_count,
            inclusion_count: snapshot.inclusion_count,
            shredstream_events: snapshot.shredstream_events,
            shredstream_sequence_gaps: snapshot.shredstream_sequence_gaps,
            shredstream_sequence_reorders: snapshot.shredstream_sequence_reorders,
            shredstream_sequence_duplicates: snapshot.shredstream_sequence_duplicates,
            shredstream_ingest_latency_nanos: snapshot.shredstream_ingest_latency_nanos,
            shredstream_ingest_latency_count: snapshot.shredstream_ingest_latency_count,
            shredstream_interarrival_latency_nanos: snapshot.shredstream_interarrival_latency_nanos,
            shredstream_interarrival_latency_count: snapshot.shredstream_interarrival_latency_count,
            shredstream_events_per_second: snapshot.shredstream_events_per_second,
            stage_latency_nanos: snapshot
                .stage_latency_nanos
                .into_iter()
                .map(|(stage, latency)| (stage_name(stage).to_string(), latency))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeStatus {
    pub mode: RuntimeMode,
    pub live: bool,
    pub ready: bool,
    pub issue: Option<RuntimeIssue>,
    pub kill_switch_active: bool,
    pub latest_slot: u64,
    pub total_routes: usize,
    pub ready_routes: usize,
    pub inflight_submissions: usize,
    pub wallet_ready: bool,
    pub wallet_balance_lamports: u64,
    pub blockhash_slot: Option<u64>,
    pub blockhash_slot_lag: Option<u64>,
    pub metrics: RuntimeMetrics,
    pub last_error: Option<String>,
    pub updated_at_unix_millis: u128,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::Starting,
            live: true,
            ready: false,
            issue: Some(RuntimeIssue::NoRoutesConfigured),
            kill_switch_active: false,
            latest_slot: 0,
            total_routes: 0,
            ready_routes: 0,
            inflight_submissions: 0,
            wallet_ready: false,
            wallet_balance_lamports: 0,
            blockhash_slot: None,
            blockhash_slot_lag: None,
            metrics: RuntimeMetrics::from_snapshot(MetricsSnapshot {
                detect_events: 0,
                stale_updates: 0,
                rejection_count: 0,
                build_count: 0,
                submit_count: 0,
                inclusion_count: 0,
                shredstream_events: 0,
                shredstream_sequence_gaps: 0,
                shredstream_sequence_reorders: 0,
                shredstream_sequence_duplicates: 0,
                shredstream_ingest_latency_nanos: 0,
                shredstream_ingest_latency_count: 0,
                shredstream_interarrival_latency_nanos: 0,
                shredstream_interarrival_latency_count: 0,
                shredstream_events_per_second: 0,
                stage_latency_nanos: BTreeMap::new(),
            }),
            last_error: None,
            updated_at_unix_millis: now_unix_millis(),
        }
    }
}

impl RuntimeStatus {
    pub fn prometheus_metrics(&self) -> String {
        let mut output = String::new();
        output.push_str("# TYPE bot_runtime_live gauge\n");
        output.push_str(&format!("bot_runtime_live {}\n", u8::from(self.live)));
        output.push_str("# TYPE bot_runtime_ready gauge\n");
        output.push_str(&format!("bot_runtime_ready {}\n", u8::from(self.ready)));
        output.push_str("# TYPE bot_runtime_kill_switch gauge\n");
        output.push_str(&format!(
            "bot_runtime_kill_switch {}\n",
            u8::from(self.kill_switch_active)
        ));
        output.push_str("# TYPE bot_runtime_latest_slot gauge\n");
        output.push_str(&format!("bot_runtime_latest_slot {}\n", self.latest_slot));
        output.push_str("# TYPE bot_runtime_routes_total gauge\n");
        output.push_str(&format!("bot_runtime_routes_total {}\n", self.total_routes));
        output.push_str("# TYPE bot_runtime_routes_ready gauge\n");
        output.push_str(&format!("bot_runtime_routes_ready {}\n", self.ready_routes));
        output.push_str("# TYPE bot_runtime_inflight_submissions gauge\n");
        output.push_str(&format!(
            "bot_runtime_inflight_submissions {}\n",
            self.inflight_submissions
        ));
        output.push_str("# TYPE bot_runtime_wallet_ready gauge\n");
        output.push_str(&format!(
            "bot_runtime_wallet_ready {}\n",
            u8::from(self.wallet_ready)
        ));
        output.push_str("# TYPE bot_runtime_wallet_balance_lamports gauge\n");
        output.push_str(&format!(
            "bot_runtime_wallet_balance_lamports {}\n",
            self.wallet_balance_lamports
        ));
        if let Some(slot) = self.blockhash_slot {
            output.push_str("# TYPE bot_runtime_blockhash_slot gauge\n");
            output.push_str(&format!("bot_runtime_blockhash_slot {}\n", slot));
        }
        if let Some(slot_lag) = self.blockhash_slot_lag {
            output.push_str("# TYPE bot_runtime_blockhash_slot_lag gauge\n");
            output.push_str(&format!("bot_runtime_blockhash_slot_lag {}\n", slot_lag));
        }

        output.push_str("# TYPE bot_detect_events_total counter\n");
        output.push_str(&format!(
            "bot_detect_events_total {}\n",
            self.metrics.detect_events
        ));
        output.push_str("# TYPE bot_stale_updates_total counter\n");
        output.push_str(&format!(
            "bot_stale_updates_total {}\n",
            self.metrics.stale_updates
        ));
        output.push_str("# TYPE bot_rejections_total counter\n");
        output.push_str(&format!(
            "bot_rejections_total {}\n",
            self.metrics.rejection_count
        ));
        output.push_str("# TYPE bot_shredstream_events_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_events_total {}\n",
            self.metrics.shredstream_events
        ));
        output.push_str("# TYPE bot_shredstream_events_per_second gauge\n");
        output.push_str(&format!(
            "bot_shredstream_events_per_second {}\n",
            self.metrics.shredstream_events_per_second
        ));
        output.push_str("# TYPE bot_shredstream_sequence_gaps_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_sequence_gaps_total {}\n",
            self.metrics.shredstream_sequence_gaps
        ));
        output.push_str("# TYPE bot_shredstream_sequence_reorders_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_sequence_reorders_total {}\n",
            self.metrics.shredstream_sequence_reorders
        ));
        output.push_str("# TYPE bot_shredstream_sequence_duplicates_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_sequence_duplicates_total {}\n",
            self.metrics.shredstream_sequence_duplicates
        ));
        output.push_str("# TYPE bot_shredstream_ingest_latency_nanos_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_ingest_latency_nanos_total {}\n",
            self.metrics.shredstream_ingest_latency_nanos
        ));
        output.push_str("# TYPE bot_shredstream_ingest_latency_nanos_count counter\n");
        output.push_str(&format!(
            "bot_shredstream_ingest_latency_nanos_count {}\n",
            self.metrics.shredstream_ingest_latency_count
        ));
        output.push_str("# TYPE bot_shredstream_interarrival_latency_nanos_total counter\n");
        output.push_str(&format!(
            "bot_shredstream_interarrival_latency_nanos_total {}\n",
            self.metrics.shredstream_interarrival_latency_nanos
        ));
        output.push_str("# TYPE bot_shredstream_interarrival_latency_nanos_count counter\n");
        output.push_str(&format!(
            "bot_shredstream_interarrival_latency_nanos_count {}\n",
            self.metrics.shredstream_interarrival_latency_count
        ));
        output.push_str("# TYPE bot_build_total counter\n");
        output.push_str(&format!("bot_build_total {}\n", self.metrics.build_count));
        output.push_str("# TYPE bot_submit_total counter\n");
        output.push_str(&format!("bot_submit_total {}\n", self.metrics.submit_count));
        output.push_str("# TYPE bot_inclusion_total counter\n");
        output.push_str(&format!(
            "bot_inclusion_total {}\n",
            self.metrics.inclusion_count
        ));
        output.push_str("# TYPE bot_stage_latency_nanos_total counter\n");
        for (stage, latency) in &self.metrics.stage_latency_nanos {
            output.push_str(&format!(
                "bot_stage_latency_nanos_total{{stage=\"{}\"}} {}\n",
                stage, latency
            ));
        }
        output
    }
}

#[derive(Debug, Clone, Default)]
pub struct SharedRuntimeStatus {
    inner: Arc<Mutex<RuntimeStatus>>,
}

impl SharedRuntimeStatus {
    pub fn new(initial: RuntimeStatus) -> Self {
        Self {
            inner: Arc::new(Mutex::new(initial)),
        }
    }

    pub fn snapshot(&self) -> RuntimeStatus {
        self.inner.lock().expect("runtime status lock").clone()
    }

    pub fn replace(&self, status: RuntimeStatus) {
        *self.inner.lock().expect("runtime status lock") = status;
    }

    pub fn spawn_http_server(&self, config: &HealthServerConfig) -> std::io::Result<()> {
        if !config.enabled {
            return Ok(());
        }

        let listener = TcpListener::bind(&config.bind_address)?;
        let status = self.clone();
        thread::Builder::new()
            .name("bot-health-http".into())
            .spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else {
                        continue;
                    };

                    let mut buffer = [0u8; 2048];
                    let bytes_read = match stream.read(&mut buffer) {
                        Ok(bytes_read) => bytes_read,
                        Err(_) => continue,
                    };
                    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/status");
                    let snapshot = status.snapshot();
                    let (status_code, content_type, body) = response_for_path(path, &snapshot);
                    let response = format!(
                        "HTTP/1.1 {status_code}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.write_all(&body);
                    let _ = stream.flush();
                }
            })
            .expect("spawn health http thread");

        Ok(())
    }
}

fn response_for_path(
    path: &str,
    snapshot: &RuntimeStatus,
) -> (&'static str, &'static str, Vec<u8>) {
    match path {
        "/live" | "/health/live" => {
            let body = if snapshot.live {
                b"live\n".to_vec()
            } else {
                b"dead\n".to_vec()
            };
            let status = if snapshot.live {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            (status, "text/plain; charset=utf-8", body)
        }
        "/ready" | "/health/ready" => {
            let body = if snapshot.ready {
                b"ready\n".to_vec()
            } else {
                b"not_ready\n".to_vec()
            };
            let status = if snapshot.ready {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            (status, "text/plain; charset=utf-8", body)
        }
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            snapshot.prometheus_metrics().into_bytes(),
        ),
        _ => (
            "200 OK",
            "application/json; charset=utf-8",
            serde_json::to_vec(snapshot).unwrap_or_else(|_| b"{}".to_vec()),
        ),
    }
}

fn stage_name(stage: PipelineStage) -> &'static str {
    match stage {
        PipelineStage::Detect => "detect",
        PipelineStage::StateApply => "state_apply",
        PipelineStage::Quote => "quote",
        PipelineStage::Select => "select",
        PipelineStage::Build => "build",
        PipelineStage::Sign => "sign",
        PipelineStage::Submit => "submit",
        PipelineStage::Reconcile => "reconcile",
    }
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::{RuntimeIssue, RuntimeMetrics, RuntimeMode, RuntimeStatus};

    #[test]
    fn prometheus_output_contains_core_metrics() {
        let output = RuntimeStatus {
            mode: RuntimeMode::Ready,
            live: true,
            ready: true,
            issue: Some(RuntimeIssue::KillSwitchActive),
            kill_switch_active: true,
            latest_slot: 42,
            total_routes: 1,
            ready_routes: 1,
            inflight_submissions: 0,
            wallet_ready: true,
            wallet_balance_lamports: 5,
            blockhash_slot: Some(40),
            blockhash_slot_lag: Some(2),
            metrics: RuntimeMetrics {
                detect_events: 7,
                stale_updates: 1,
                rejection_count: 2,
                shredstream_events: 10,
                shredstream_sequence_gaps: 1,
                shredstream_sequence_reorders: 1,
                shredstream_sequence_duplicates: 1,
                shredstream_ingest_latency_nanos: 1200,
                shredstream_ingest_latency_count: 4,
                shredstream_interarrival_latency_nanos: 1100,
                shredstream_interarrival_latency_count: 3,
                shredstream_events_per_second: 9,
                build_count: 3,
                submit_count: 4,
                inclusion_count: 5,
                stage_latency_nanos: [("select".to_string(), 99)].into_iter().collect(),
            },
            last_error: None,
            updated_at_unix_millis: 1,
        }
        .prometheus_metrics();

        assert!(output.contains("bot_runtime_ready 1"));
        assert!(output.contains("bot_detect_events_total 7"));
        assert!(output.contains("bot_shredstream_events_total 10"));
        assert!(output.contains("bot_shredstream_events_per_second 9"));
        assert!(output.contains("bot_shredstream_sequence_gaps_total 1"));
        assert!(output.contains("bot_stage_latency_nanos_total{stage=\"select\"} 99"));
    }
}
