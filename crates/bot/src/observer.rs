use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use builder::BuildRejectionReason;
use reconciliation::{ExecutionOutcome, ExecutionRecord, FailureClass};
use serde::{Deserialize, Serialize};
use state::types::PoolSnapshot;
use strategy::{opportunity::OpportunityDecision, reasons::RejectionReason};
use submit::SubmitRejectionReason;

pub use crate::route_health::RouteMonitorView;
use crate::{
    config::{BotConfig, MonitorServerConfig, RouteClassConfig, RuntimeProfileConfig},
    control::{RuntimeMode, RuntimeStatus},
    route_health::{PoolHealthState, RouteHealthState, SharedRouteHealth},
    runtime::{HotPathReport, PipelineTrace},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolMonitorView {
    pub pool_id: String,
    pub venues: Vec<String>,
    pub route_ids: Vec<String>,
    pub health_state: PoolHealthState,
    pub price_bps: u64,
    pub fee_bps: u16,
    pub reserve_depth: u64,
    pub last_update_slot: u64,
    pub slot_lag: u64,
    pub age_slots: u64,
    pub stale: bool,
    pub confidence: String,
    pub refresh_pending: bool,
    pub refresh_attempt_count: u64,
    pub refresh_failure_count: u64,
    pub refresh_success_count: u64,
    pub consecutive_refresh_failures: u32,
    pub last_refresh_latency_ms: Option<u64>,
    pub refresh_deadline_slot_lag: Option<u64>,
    pub repair_pending: bool,
    pub repair_attempt_count: u64,
    pub repair_failure_count: u64,
    pub repair_success_count: u64,
    pub consecutive_repair_failures: u32,
    pub quarantined_until_slot: Option<u64>,
    pub disable_reason: Option<String>,
    pub last_executable_slot: Option<u64>,
    pub last_repair_latency_ms: Option<u64>,
    pub last_repair_invalidation_ms: Option<u64>,
    pub last_seen_unix_millis: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolsResponse {
    pub items: Vec<PoolMonitorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutesResponse {
    pub items: Vec<RouteMonitorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSignalSample {
    pub seq: u64,
    pub source: String,
    pub source_sequence: u64,
    pub observed_slot: u64,
    pub source_received_at_unix_millis: u128,
    pub normalized_at_unix_millis: u128,
    pub source_latency_nanos: Option<u64>,
    pub ingest_nanos: u64,
    pub queue_wait_nanos: u64,
    pub state_apply_nanos: u64,
    pub select_nanos: Option<u64>,
    pub build_nanos: Option<u64>,
    pub sign_nanos: Option<u64>,
    pub submit_nanos: Option<u64>,
    pub total_to_submit_nanos: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSignalMetric {
    pub count: usize,
    pub latest_nanos: Option<u64>,
    pub p50_nanos: Option<u64>,
    pub p95_nanos: Option<u64>,
    pub p99_nanos: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSignalsResponse {
    pub window: String,
    pub metrics: BTreeMap<String, MonitorSignalMetric>,
    pub samples: Vec<MonitorSignalSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectionEvent {
    pub seq: u64,
    pub stage: String,
    pub route_id: String,
    pub reason_code: String,
    pub reason_detail: String,
    pub observed_slot: u64,
    pub expected_profit_quote_atoms: Option<i64>,
    pub occurred_at_unix_millis: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectionsResponse {
    pub items: Vec<RejectionEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorTradeEvent {
    pub seq: u64,
    pub route_id: String,
    pub submission_id: String,
    pub signature: String,
    pub submit_status: String,
    pub outcome: String,
    pub expected_edge_quote_atoms: i64,
    pub estimated_cost_lamports: Option<u64>,
    pub estimated_cost_quote_atoms: Option<u64>,
    pub expected_pnl_quote_atoms: Option<i64>,
    pub realized_pnl_quote_atoms: Option<i64>,
    pub jito_tip_lamports: Option<u64>,
    pub active_execution_buffer_bps: Option<u16>,
    pub failure_program_id: Option<String>,
    pub failure_instruction_index: Option<u8>,
    pub failure_custom_code: Option<u32>,
    pub failure_error_name: Option<String>,
    pub endpoint: String,
    pub quoted_slot: u64,
    pub blockhash_slot: Option<u64>,
    pub submitted_slot: Option<u64>,
    pub submitted_at_unix_millis: u128,
    pub updated_at_unix_millis: u128,
    pub source_to_submit_nanos: Option<u64>,
    pub submit_to_terminal_nanos: Option<u64>,
    pub source_to_terminal_nanos: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorTradesResponse {
    pub items: Vec<MonitorTradeEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorOverview {
    pub mode: String,
    pub live: bool,
    pub ready: bool,
    pub issue: Option<String>,
    pub last_error: Option<String>,
    pub kill_switch_active: bool,
    pub latest_slot: u64,
    pub rpc_slot: Option<u64>,
    pub warm_routes: usize,
    pub tradable_routes: usize,
    pub eligible_live_routes: usize,
    pub shadow_routes: usize,
    pub total_routes: usize,
    pub quarantined_pools: usize,
    pub disabled_pools: usize,
    pub inflight_submissions: usize,
    pub wallet_ready: bool,
    pub wallet_balance_lamports: u64,
    pub blockhash_slot: Option<u64>,
    pub blockhash_slot_lag: Option<u64>,
    pub detect_events: u64,
    pub rejection_count: u64,
    pub submit_count: u64,
    pub inclusion_count: u64,
    pub submit_rejected_count: u64,
    pub shredstream_events_per_second: u64,
    pub landed_rate_bps: u64,
    pub reject_rate_bps: u64,
    pub expected_session_pnl_quote_atoms: i64,
    pub realized_session_pnl_quote_atoms: i64,
    pub observer_drop_count: u64,
    pub poll_interval_millis: u64,
    pub updated_at_unix_millis: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub overview: MonitorOverview,
    pub pools: PoolsResponse,
    pub routes: RoutesResponse,
    pub signals: MonitorSignalsResponse,
    pub rejections: RejectionsResponse,
    pub trades: MonitorTradesResponse,
}

#[derive(Debug, Clone)]
struct PoolStaticMetadata {
    venues: Vec<String>,
    route_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct PoolRepairStats {
    refresh_pending: bool,
    refresh_deadline_slot: Option<u64>,
    refresh_attempt_count: u64,
    refresh_failure_count: u64,
    refresh_success_count: u64,
    last_refresh_latency_ms: Option<u64>,
    repair_attempt_count: u64,
    repair_failure_count: u64,
    repair_success_count: u64,
    last_repair_latency_ms: Option<u64>,
    last_repair_invalidation_ms: Option<u64>,
    invalidated_at_unix_millis: Option<u128>,
}

#[derive(Debug, Clone)]
pub(crate) struct RepairEvent {
    pub pool_id: String,
    pub kind: RepairEventKind,
    pub occurred_at: SystemTime,
}

#[derive(Debug, Clone)]
pub(crate) enum RepairEventKind {
    RefreshScheduled { deadline_slot: u64 },
    RefreshAttemptStarted,
    RefreshAttemptFailed,
    RefreshAttemptSucceeded { latency_ms: u64 },
    RefreshCleared,
    RepairAttemptStarted,
    RepairAttemptFailed,
    RepairAttemptSucceeded { latency_ms: u64 },
}

#[derive(Debug)]
struct ObserverState {
    config: MonitorServerConfig,
    max_snapshot_slot_lag: u64,
    route_health: SharedRouteHealth,
    runtime_status: RuntimeStatus,
    pool_metadata: BTreeMap<String, PoolStaticMetadata>,
    repair_stats: BTreeMap<String, PoolRepairStats>,
    pools: BTreeMap<String, PoolMonitorView>,
    signals: VecDeque<MonitorSignalSample>,
    rejections: VecDeque<RejectionEvent>,
    trades: BTreeMap<String, MonitorTradeEvent>,
    trade_order: VecDeque<String>,
    cumulative_expected_pnl: i128,
    cumulative_realized_pnl: i128,
    next_seq: u64,
}

impl ObserverState {
    fn new(
        config: MonitorServerConfig,
        pool_metadata: BTreeMap<String, PoolStaticMetadata>,
        max_snapshot_slot_lag: u64,
        route_health: SharedRouteHealth,
    ) -> Self {
        Self {
            config,
            max_snapshot_slot_lag,
            route_health,
            runtime_status: RuntimeStatus::default(),
            pool_metadata,
            repair_stats: BTreeMap::new(),
            pools: BTreeMap::new(),
            signals: VecDeque::new(),
            rejections: VecDeque::new(),
            trades: BTreeMap::new(),
            trade_order: VecDeque::new(),
            cumulative_expected_pnl: 0,
            cumulative_realized_pnl: 0,
            next_seq: 1,
        }
    }

    fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn apply_status(&mut self, status: RuntimeStatus) {
        self.runtime_status = status;
        let latest_slot = self.runtime_status.latest_slot;
        let max_slot_lag = self.max_snapshot_slot_lag;
        for pool in self.pools.values_mut() {
            let slot_lag = latest_slot.saturating_sub(pool.last_update_slot);
            pool.slot_lag = slot_lag;
            pool.age_slots = slot_lag;
            pool.stale = slot_lag > max_slot_lag;
            pool.refresh_deadline_slot_lag =
                self.repair_stats.get(&pool.pool_id).and_then(|stats| {
                    stats
                        .refresh_deadline_slot
                        .map(|deadline| deadline.saturating_sub(latest_slot))
                });
        }
    }

    fn apply_hot_path(&mut self, report: HotPathReport) {
        for snapshot in report.pool_snapshots.clone() {
            self.upsert_pool(snapshot);
        }
        self.push_signal(report.pipeline_trace.clone());
        self.push_strategy_rejections(&report);
        self.push_build_rejection(&report);
        self.push_submit_rejection(&report);
        self.upsert_trade_from_report(&report);
    }

    fn apply_trade_update(&mut self, record: ExecutionRecord) {
        let Some(existing) = self.trades.get_mut(&record.submission_id.0) else {
            return;
        };
        existing.outcome = outcome_label(&record.outcome).to_string();
        existing.failure_program_id = record
            .failure_detail
            .as_ref()
            .and_then(|detail| detail.program_id.clone());
        existing.failure_instruction_index = record
            .failure_detail
            .as_ref()
            .and_then(|detail| detail.instruction_index);
        existing.failure_custom_code = record
            .failure_detail
            .as_ref()
            .and_then(|detail| detail.custom_code);
        existing.failure_error_name = record
            .failure_detail
            .as_ref()
            .and_then(|detail| detail.error_name.clone());
        existing.updated_at_unix_millis = unix_millis(record.last_updated_at);
        existing.submitted_slot = record.submitted_slot;
        existing.submit_to_terminal_nanos = duration_nanos(
            record
                .last_updated_at
                .duration_since(record.submitted_at)
                .unwrap_or(Duration::ZERO),
        );
        existing.source_to_terminal_nanos = combine_latencies(
            existing.source_to_submit_nanos,
            existing.submit_to_terminal_nanos,
        );
    }

    fn upsert_pool(&mut self, snapshot: PoolSnapshot) {
        let pool_id = snapshot.pool_id.0.clone();
        let last_seen_unix_millis = unix_millis(snapshot.derived_at);
        let repair_stats = self.repair_stats.entry(pool_id.clone()).or_default();
        if snapshot.repair_pending {
            repair_stats
                .invalidated_at_unix_millis
                .get_or_insert(last_seen_unix_millis);
        } else if snapshot.is_executable() {
            if let Some(invalidated_at) = repair_stats.invalidated_at_unix_millis.take() {
                repair_stats.last_repair_invalidation_ms = Some(
                    last_seen_unix_millis
                        .saturating_sub(invalidated_at)
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
        }
        let metadata = self.pool_metadata.get(&pool_id);
        self.pools.insert(
            pool_id.clone(),
            PoolMonitorView {
                pool_id,
                venues: metadata
                    .map(|entry| entry.venues.clone())
                    .unwrap_or_default(),
                route_ids: metadata
                    .map(|entry| entry.route_ids.clone())
                    .unwrap_or_default(),
                health_state: PoolHealthState::Degraded,
                price_bps: snapshot.price_bps,
                fee_bps: snapshot.fee_bps,
                reserve_depth: snapshot.reserve_depth,
                last_update_slot: snapshot.last_update_slot,
                slot_lag: snapshot.freshness.slot_lag,
                age_slots: snapshot
                    .freshness
                    .head_slot
                    .saturating_sub(snapshot.last_update_slot),
                stale: snapshot.freshness.is_stale,
                confidence: pool_confidence_label(snapshot.confidence).into(),
                refresh_pending: repair_stats.refresh_pending,
                refresh_attempt_count: repair_stats.refresh_attempt_count,
                refresh_failure_count: repair_stats.refresh_failure_count,
                refresh_success_count: repair_stats.refresh_success_count,
                consecutive_refresh_failures: 0,
                last_refresh_latency_ms: repair_stats.last_refresh_latency_ms,
                refresh_deadline_slot_lag: repair_stats
                    .refresh_deadline_slot
                    .map(|deadline| deadline.saturating_sub(self.runtime_status.latest_slot)),
                repair_pending: snapshot.repair_pending,
                repair_attempt_count: repair_stats.repair_attempt_count,
                repair_failure_count: repair_stats.repair_failure_count,
                repair_success_count: repair_stats.repair_success_count,
                consecutive_repair_failures: 0,
                quarantined_until_slot: None,
                disable_reason: None,
                last_executable_slot: None,
                last_repair_latency_ms: repair_stats.last_repair_latency_ms,
                last_repair_invalidation_ms: repair_stats.last_repair_invalidation_ms,
                last_seen_unix_millis,
            },
        );
    }

    fn apply_repair(&mut self, event: RepairEvent) {
        let stats = self.repair_stats.entry(event.pool_id.clone()).or_default();
        match event.kind {
            RepairEventKind::RefreshScheduled { deadline_slot } => {
                stats.refresh_pending = true;
                stats.refresh_deadline_slot = Some(deadline_slot);
            }
            RepairEventKind::RefreshAttemptStarted => {
                stats.refresh_pending = true;
                stats.refresh_attempt_count = stats.refresh_attempt_count.saturating_add(1);
            }
            RepairEventKind::RefreshAttemptFailed => {
                stats.refresh_pending = true;
                stats.refresh_failure_count = stats.refresh_failure_count.saturating_add(1);
            }
            RepairEventKind::RefreshAttemptSucceeded { latency_ms } => {
                stats.refresh_pending = false;
                stats.refresh_deadline_slot = None;
                stats.refresh_success_count = stats.refresh_success_count.saturating_add(1);
                stats.last_refresh_latency_ms = Some(latency_ms);
            }
            RepairEventKind::RefreshCleared => {
                stats.refresh_pending = false;
                stats.refresh_deadline_slot = None;
            }
            RepairEventKind::RepairAttemptStarted => {
                stats.refresh_pending = false;
                stats.refresh_deadline_slot = None;
                stats.repair_attempt_count = stats.repair_attempt_count.saturating_add(1);
            }
            RepairEventKind::RepairAttemptFailed => {
                stats.repair_failure_count = stats.repair_failure_count.saturating_add(1);
            }
            RepairEventKind::RepairAttemptSucceeded { latency_ms } => {
                stats.repair_success_count = stats.repair_success_count.saturating_add(1);
                stats.last_repair_latency_ms = Some(latency_ms);
            }
        }

        if let Some(pool) = self.pools.get_mut(&event.pool_id) {
            pool.refresh_pending = stats.refresh_pending;
            pool.refresh_attempt_count = stats.refresh_attempt_count;
            pool.refresh_failure_count = stats.refresh_failure_count;
            pool.refresh_success_count = stats.refresh_success_count;
            pool.last_refresh_latency_ms = stats.last_refresh_latency_ms;
            pool.refresh_deadline_slot_lag = stats
                .refresh_deadline_slot
                .map(|deadline| deadline.saturating_sub(self.runtime_status.latest_slot));
            pool.repair_attempt_count = stats.repair_attempt_count;
            pool.repair_failure_count = stats.repair_failure_count;
            pool.repair_success_count = stats.repair_success_count;
            pool.last_repair_latency_ms = stats.last_repair_latency_ms;
            pool.last_repair_invalidation_ms = stats.last_repair_invalidation_ms;
            pool.last_seen_unix_millis = unix_millis(event.occurred_at);
        }
    }

    fn push_signal(&mut self, trace: PipelineTrace) {
        let seq = self.next_seq();
        self.signals.push_back(MonitorSignalSample {
            seq,
            source: source_label(&trace.source).to_string(),
            source_sequence: trace.source_sequence,
            observed_slot: trace.observed_slot,
            source_received_at_unix_millis: unix_millis(trace.source_received_at),
            normalized_at_unix_millis: unix_millis(trace.normalized_at),
            source_latency_nanos: duration_nanos_opt(trace.source_latency),
            ingest_nanos: duration_nanos(trace.ingest_duration).unwrap_or(0),
            queue_wait_nanos: duration_nanos(trace.queue_wait_duration).unwrap_or(0),
            state_apply_nanos: duration_nanos(trace.state_apply_duration).unwrap_or(0),
            select_nanos: duration_nanos_opt(trace.select_duration),
            build_nanos: duration_nanos_opt(trace.build_duration),
            sign_nanos: duration_nanos_opt(trace.sign_duration),
            submit_nanos: duration_nanos_opt(trace.submit_duration),
            total_to_submit_nanos: duration_nanos_opt(trace.total_to_submit),
        });
        while self.signals.len() > self.config.max_signal_samples {
            self.signals.pop_front();
        }
    }

    fn push_strategy_rejections(&mut self, report: &HotPathReport) {
        for decision in &report.selection.decisions {
            let OpportunityDecision::Rejected { route_id, reason } = decision else {
                continue;
            };
            let seq = self.next_seq();
            self.push_rejection(RejectionEvent {
                seq,
                stage: "strategy".into(),
                route_id: route_id.0.clone(),
                reason_code: strategy_reason_code(reason).into(),
                reason_detail: strategy_reason_detail(reason),
                observed_slot: report.pipeline_trace.observed_slot,
                expected_profit_quote_atoms: strategy_expected_profit(reason),
                occurred_at_unix_millis: unix_millis(SystemTime::now()),
            });
        }
    }

    fn push_build_rejection(&mut self, report: &HotPathReport) {
        let Some(build_result) = &report.build_result else {
            return;
        };
        let Some(reason) = &build_result.rejection else {
            return;
        };
        let Some(candidate) = &report.selection.best_candidate else {
            return;
        };
        let seq = self.next_seq();
        self.push_rejection(RejectionEvent {
            seq,
            stage: "build".into(),
            route_id: candidate.route_id.0.clone(),
            reason_code: build_reason_code(reason).into(),
            reason_detail: build_reason_detail(reason),
            observed_slot: report.pipeline_trace.observed_slot,
            expected_profit_quote_atoms: Some(candidate.expected_net_profit_quote_atoms),
            occurred_at_unix_millis: unix_millis(SystemTime::now()),
        });
    }

    fn push_submit_rejection(&mut self, report: &HotPathReport) {
        let Some(submit_result) = &report.submit_result else {
            return;
        };
        let Some(reason) = &submit_result.rejection else {
            return;
        };
        let route_id = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.route_id.0.clone())
            .or_else(|| {
                report
                    .execution_record
                    .as_ref()
                    .map(|record| record.route_id.0.clone())
            })
            .unwrap_or_else(|| "unknown".into());
        let seq = self.next_seq();
        self.push_rejection(RejectionEvent {
            seq,
            stage: "submit".into(),
            route_id,
            reason_code: submit_reason_code(reason).into(),
            reason_detail: submit_reason_detail(reason),
            observed_slot: report.pipeline_trace.observed_slot,
            expected_profit_quote_atoms: report
                .selection
                .best_candidate
                .as_ref()
                .map(|candidate| candidate.expected_net_profit_quote_atoms),
            occurred_at_unix_millis: unix_millis(SystemTime::now()),
        });
    }

    fn push_rejection(&mut self, rejection: RejectionEvent) {
        self.rejections.push_back(rejection);
        while self.rejections.len() > self.config.max_rejections {
            self.rejections.pop_front();
        }
    }

    fn upsert_trade_from_report(&mut self, report: &HotPathReport) {
        let Some(record) = &report.execution_record else {
            return;
        };
        let expected_edge = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.expected_gross_profit_quote_atoms)
            .unwrap_or(0);
        let estimated_cost_quote_atoms = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.estimated_execution_cost_quote_atoms);
        let estimated_cost = report
            .build_result
            .as_ref()
            .and_then(|build| build.envelope.as_ref())
            .map(|envelope| envelope.estimated_total_cost_lamports);
        let expected_pnl = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.expected_net_profit_quote_atoms);
        let source_to_submit_nanos = duration_nanos_opt(report.pipeline_trace.total_to_submit);
        let submit_to_terminal_nanos = matches!(record.outcome, ExecutionOutcome::Pending)
            .then_some(None)
            .unwrap_or(Some(0));
        let entry = MonitorTradeEvent {
            seq: self.next_seq(),
            route_id: record.route_id.0.clone(),
            submission_id: record.submission_id.0.clone(),
            signature: record.chain_signature.clone(),
            submit_status: submit_status_label(record.submit_status).into(),
            outcome: outcome_label(&record.outcome).into(),
            expected_edge_quote_atoms: expected_edge,
            estimated_cost_lamports: estimated_cost,
            estimated_cost_quote_atoms,
            expected_pnl_quote_atoms: expected_pnl,
            realized_pnl_quote_atoms: None,
            jito_tip_lamports: report
                .build_result
                .as_ref()
                .and_then(|build| build.envelope.as_ref())
                .map(|envelope| envelope.jito_tip_lamports),
            active_execution_buffer_bps: report
                .selection
                .best_candidate
                .as_ref()
                .and_then(|candidate| candidate.active_execution_buffer_bps),
            failure_program_id: record
                .failure_detail
                .as_ref()
                .and_then(|detail| detail.program_id.clone()),
            failure_instruction_index: record
                .failure_detail
                .as_ref()
                .and_then(|detail| detail.instruction_index),
            failure_custom_code: record
                .failure_detail
                .as_ref()
                .and_then(|detail| detail.custom_code),
            failure_error_name: record
                .failure_detail
                .as_ref()
                .and_then(|detail| detail.error_name.clone()),
            endpoint: record.submit_endpoint.clone(),
            quoted_slot: record.quoted_slot,
            blockhash_slot: record.blockhash_slot,
            submitted_slot: record.submitted_slot,
            submitted_at_unix_millis: unix_millis(record.submitted_at),
            updated_at_unix_millis: unix_millis(record.last_updated_at),
            source_to_submit_nanos,
            submit_to_terminal_nanos,
            source_to_terminal_nanos: combine_latencies(
                source_to_submit_nanos,
                submit_to_terminal_nanos,
            ),
        };
        if self
            .trades
            .insert(record.submission_id.0.clone(), entry.clone())
            .is_none()
        {
            self.trade_order.push_back(record.submission_id.0.clone());
            if let Some(expected) = entry.expected_pnl_quote_atoms {
                self.cumulative_expected_pnl += expected as i128;
            }
        }
        while self.trade_order.len() > self.config.max_trades {
            if let Some(oldest) = self.trade_order.pop_front() {
                self.trades.remove(&oldest);
            }
        }
    }

    fn overview(&self, drop_count: u64) -> MonitorOverview {
        let landed_rate_bps = if self.runtime_status.metrics.submit_count == 0 {
            0
        } else {
            self.runtime_status
                .metrics
                .inclusion_count
                .saturating_mul(10_000)
                / self.runtime_status.metrics.submit_count
        };
        let reject_rate_bps = if self.runtime_status.metrics.detect_events == 0 {
            0
        } else {
            self.runtime_status
                .metrics
                .rejection_count
                .saturating_mul(10_000)
                / self.runtime_status.metrics.detect_events
        };
        MonitorOverview {
            mode: runtime_mode_label(self.runtime_status.mode).into(),
            live: self.runtime_status.live,
            ready: self.runtime_status.ready,
            issue: self.runtime_status.issue.as_ref().map(issue_label),
            last_error: self.runtime_status.last_error.clone(),
            kill_switch_active: self.runtime_status.kill_switch_active,
            latest_slot: self.runtime_status.latest_slot,
            rpc_slot: self.runtime_status.rpc_slot,
            warm_routes: self.runtime_status.warm_routes,
            tradable_routes: self.runtime_status.tradable_routes,
            eligible_live_routes: self.runtime_status.eligible_live_routes,
            shadow_routes: self.runtime_status.shadow_routes,
            total_routes: self.runtime_status.total_routes,
            quarantined_pools: self.runtime_status.quarantined_pools,
            disabled_pools: self.runtime_status.disabled_pools,
            inflight_submissions: self.runtime_status.inflight_submissions,
            wallet_ready: self.runtime_status.wallet_ready,
            wallet_balance_lamports: self.runtime_status.wallet_balance_lamports,
            blockhash_slot: self.runtime_status.blockhash_slot,
            blockhash_slot_lag: self.runtime_status.blockhash_slot_lag,
            detect_events: self.runtime_status.metrics.detect_events,
            rejection_count: self.runtime_status.metrics.rejection_count,
            submit_count: self.runtime_status.metrics.submit_count,
            inclusion_count: self.runtime_status.metrics.inclusion_count,
            submit_rejected_count: self.runtime_status.metrics.submit_rejected_count,
            shredstream_events_per_second: self
                .runtime_status
                .metrics
                .shredstream_events_per_second,
            landed_rate_bps,
            reject_rate_bps,
            expected_session_pnl_quote_atoms: clamp_i128(self.cumulative_expected_pnl),
            realized_session_pnl_quote_atoms: clamp_i128(self.cumulative_realized_pnl),
            observer_drop_count: drop_count,
            poll_interval_millis: self.config.poll_interval_millis,
            updated_at_unix_millis: self.runtime_status.updated_at_unix_millis,
        }
    }

    fn pools_response(
        &self,
        sort: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> PoolsResponse {
        let mut items = self.pools.values().cloned().collect::<Vec<_>>();
        for item in &mut items {
            self.apply_pool_health(item);
        }
        if let Some(filter) = filter {
            let filter = filter.to_ascii_lowercase();
            items.retain(|item| {
                item.pool_id.to_ascii_lowercase().contains(&filter)
                    || item
                        .venues
                        .iter()
                        .any(|venue| venue.to_ascii_lowercase().contains(&filter))
                    || item
                        .route_ids
                        .iter()
                        .any(|route_id| route_id.to_ascii_lowercase().contains(&filter))
            });
        }
        match sort.unwrap_or("freshness") {
            "depth" => items.sort_by_key(|item| std::cmp::Reverse(item.reserve_depth)),
            "price" => items.sort_by_key(|item| std::cmp::Reverse(item.price_bps)),
            "latency" | "freshness" => {
                items.sort_by_key(|item| (item.stale, std::cmp::Reverse(item.last_update_slot)))
            }
            _ => items.sort_by_key(|item| item.pool_id.clone()),
        }
        if let Some(limit) = limit {
            items.truncate(limit);
        }
        PoolsResponse { items }
    }

    fn routes_response(
        &self,
        status: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> RoutesResponse {
        let mut items = self
            .route_health
            .lock()
            .ok()
            .map(|health| health.route_views(self.runtime_status.latest_slot))
            .unwrap_or_default();
        if let Some(status) = status {
            items.retain(|item| route_status_matches(item, status));
        }
        if let Some(filter) = filter {
            let filter = filter.to_ascii_lowercase();
            items.retain(|item| {
                item.route_id.to_ascii_lowercase().contains(&filter)
                    || item
                        .pool_ids
                        .iter()
                        .any(|pool_id| pool_id.to_ascii_lowercase().contains(&filter))
                    || item
                        .blocking_reason
                        .as_ref()
                        .map(|reason| reason.to_ascii_lowercase().contains(&filter))
                        .unwrap_or(false)
            });
        }
        if let Some(limit) = limit {
            items.truncate(limit);
        }
        RoutesResponse { items }
    }

    fn signals_response(&self, window: &str, limit: Option<usize>) -> MonitorSignalsResponse {
        let horizon = parse_window(window);
        let threshold = now_unix_millis().saturating_sub(horizon.as_millis());
        let filtered = self
            .signals
            .iter()
            .filter(|sample| sample.source_received_at_unix_millis >= threshold)
            .cloned()
            .collect::<Vec<_>>();
        let mut samples = filtered.clone();
        if let Some(limit) = limit {
            let keep = samples.len().saturating_sub(limit);
            if keep > 0 {
                samples.drain(0..keep);
            }
        }
        MonitorSignalsResponse {
            window: window.to_string(),
            metrics: summarize_metrics(&filtered),
            samples,
        }
    }

    fn rejections_response(
        &self,
        stage: Option<&str>,
        since_seq: Option<u64>,
        limit: Option<usize>,
    ) -> RejectionsResponse {
        let mut items = self
            .rejections
            .iter()
            .filter(|item| since_seq.map(|since| item.seq > since).unwrap_or(true))
            .filter(|item| {
                stage
                    .map(|stage| stage == "all" || item.stage == stage)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Some(limit) = limit {
            let keep = items.len().saturating_sub(limit);
            if keep > 0 {
                items.drain(0..keep);
            }
        }
        RejectionsResponse { items }
    }

    fn trades_response(
        &self,
        status: Option<&str>,
        since_seq: Option<u64>,
        limit: Option<usize>,
    ) -> MonitorTradesResponse {
        let mut items = self
            .trade_order
            .iter()
            .filter_map(|submission_id| self.trades.get(submission_id))
            .filter(|item| since_seq.map(|since| item.seq > since).unwrap_or(true))
            .filter(|item| status_matches(item, status))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(limit) = limit {
            let keep = items.len().saturating_sub(limit);
            if keep > 0 {
                items.drain(0..keep);
            }
        }
        MonitorTradesResponse { items }
    }

    fn snapshot(&self, drop_count: u64) -> MonitorSnapshot {
        MonitorSnapshot {
            overview: self.overview(drop_count),
            pools: self.pools_response(Some("freshness"), None, Some(200)),
            routes: self.routes_response(Some("all"), None, Some(200)),
            signals: self.signals_response("1m", Some(128)),
            rejections: self.rejections_response(Some("all"), None, Some(200)),
            trades: self.trades_response(Some("all"), None, Some(200)),
        }
    }

    fn apply_pool_health(&self, item: &mut PoolMonitorView) {
        let Some(health) =
            self.route_health.lock().ok().and_then(|health| {
                health.pool_view(&item.pool_id, self.runtime_status.latest_slot)
            })
        else {
            return;
        };
        item.health_state = health.health_state;
        item.consecutive_refresh_failures = health.consecutive_refresh_failures;
        item.consecutive_repair_failures = health.consecutive_repair_failures;
        item.quarantined_until_slot = health.quarantined_until_slot;
        item.disable_reason = health.disable_reason;
        item.last_executable_slot = health.last_executable_slot;
    }
}

#[derive(Debug)]
enum ObserverEvent {
    Status(RuntimeStatus),
    HotPath(HotPathReport),
    TradeUpdate(ExecutionRecord),
    Repair(RepairEvent),
}

#[derive(Debug, Clone)]
struct SharedObserverState {
    inner: Arc<Mutex<ObserverState>>,
    drop_count: Arc<AtomicU64>,
}

impl SharedObserverState {
    fn snapshot(&self) -> MonitorSnapshot {
        self.inner
            .lock()
            .expect("observer lock")
            .snapshot(self.drop_count.load(Ordering::Relaxed))
    }

    fn overview(&self) -> MonitorOverview {
        self.inner
            .lock()
            .expect("observer lock")
            .overview(self.drop_count.load(Ordering::Relaxed))
    }

    fn pools(
        &self,
        sort: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> PoolsResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .pools_response(sort, filter, limit)
    }

    fn routes(
        &self,
        status: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> RoutesResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .routes_response(status, filter, limit)
    }

    fn signals(&self, window: &str, limit: Option<usize>) -> MonitorSignalsResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .signals_response(window, limit)
    }

    fn rejections(
        &self,
        stage: Option<&str>,
        since_seq: Option<u64>,
        limit: Option<usize>,
    ) -> RejectionsResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .rejections_response(stage, since_seq, limit)
    }

    fn trades(
        &self,
        status: Option<&str>,
        since_seq: Option<u64>,
        limit: Option<usize>,
    ) -> MonitorTradesResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .trades_response(status, since_seq, limit)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObserverHandle {
    sender: Option<SyncSender<ObserverEvent>>,
    drop_count: Option<Arc<AtomicU64>>,
}

impl ObserverHandle {
    pub fn spawn(
        config: &MonitorServerConfig,
        bot_config: &BotConfig,
        route_health: SharedRouteHealth,
    ) -> std::io::Result<Self> {
        if !config.enabled {
            return Ok(Self::default());
        }

        let pool_metadata = build_pool_metadata(bot_config);
        let state = SharedObserverState {
            inner: Arc::new(Mutex::new(ObserverState::new(
                config.clone(),
                pool_metadata,
                bot_config.state.max_snapshot_slot_lag,
                route_health,
            ))),
            drop_count: Arc::new(AtomicU64::new(0)),
        };
        let (sender, receiver) = mpsc::sync_channel(buffer_capacity(config));
        let worker_state = state.clone();
        thread::Builder::new()
            .name("bot-monitor-worker".into())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    let mut guard = worker_state.inner.lock().expect("observer lock");
                    match event {
                        ObserverEvent::Status(status) => guard.apply_status(status),
                        ObserverEvent::HotPath(report) => guard.apply_hot_path(report),
                        ObserverEvent::TradeUpdate(record) => guard.apply_trade_update(record),
                        ObserverEvent::Repair(event) => guard.apply_repair(event),
                    }
                }
            })
            .expect("spawn monitor worker");

        let drop_count = state.drop_count.clone();
        spawn_http_server(config, state)?;
        Ok(Self {
            sender: Some(sender),
            drop_count: Some(drop_count),
        })
    }

    pub fn publish_status(&self, status: RuntimeStatus) {
        self.try_send(ObserverEvent::Status(status));
    }

    pub fn publish_hot_path(&self, report: HotPathReport) {
        self.try_send(ObserverEvent::HotPath(report));
    }

    pub fn publish_trade_update(&self, record: ExecutionRecord) {
        self.try_send(ObserverEvent::TradeUpdate(record));
    }

    pub(crate) fn publish_repair(&self, event: RepairEvent) {
        self.try_send(ObserverEvent::Repair(event));
    }

    fn try_send(&self, event: ObserverEvent) {
        let Some(sender) = &self.sender else {
            return;
        };
        if let Err(TrySendError::Full(_)) = sender.try_send(event) {
            if let Some(drop_count) = &self.drop_count {
                drop_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn spawn_http_server(
    config: &MonitorServerConfig,
    state: SharedObserverState,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.bind_address)?;
    thread::Builder::new()
        .name("bot-monitor-http".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    continue;
                };
                let mut buffer = [0u8; 4096];
                let Ok(bytes_read) = stream.read(&mut buffer) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buffer[..bytes_read]);
                let raw_path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/monitor/overview");
                let (path, query) = split_path_and_query(raw_path);
                let body = match path {
                    "/monitor/overview" => serde_json::to_vec(&state.overview()).unwrap_or_default(),
                    "/monitor/pools" => serde_json::to_vec(&state.pools(
                        query.get("sort").map(String::as_str),
                        query.get("filter").map(String::as_str),
                        parse_usize(query.get("limit")),
                    ))
                    .unwrap_or_default(),
                    "/monitor/routes" => serde_json::to_vec(&state.routes(
                        query.get("status").map(String::as_str),
                        query.get("filter").map(String::as_str),
                        parse_usize(query.get("limit")),
                    ))
                    .unwrap_or_default(),
                    "/monitor/signals" => serde_json::to_vec(&state.signals(
                        query
                            .get("window")
                            .map(String::as_str)
                            .unwrap_or("1m"),
                        parse_usize(query.get("limit")),
                    ))
                    .unwrap_or_default(),
                    "/monitor/rejections" => serde_json::to_vec(&state.rejections(
                        query.get("stage").map(String::as_str),
                        parse_u64(query.get("since_seq")),
                        parse_usize(query.get("limit")),
                    ))
                    .unwrap_or_default(),
                    "/monitor/trades" => serde_json::to_vec(&state.trades(
                        query.get("status").map(String::as_str),
                        parse_u64(query.get("since_seq")),
                        parse_usize(query.get("limit")),
                    ))
                    .unwrap_or_default(),
                    "/monitor/snapshot" => {
                        serde_json::to_vec(&state.snapshot()).unwrap_or_default()
                    }
                    _ => b"{}".to_vec(),
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        })
        .expect("spawn monitor http thread");
    Ok(())
}

fn build_pool_metadata(config: &BotConfig) -> BTreeMap<String, PoolStaticMetadata> {
    let mut metadata = BTreeMap::<String, PoolStaticMetadata>::new();
    for route in &config.routes.definitions {
        if !route.enabled
            || (config.runtime.profile == RuntimeProfileConfig::UltraFast
                && route.route_class != RouteClassConfig::AmmFastPath)
        {
            continue;
        }
        for leg in &route.legs {
            let entry = metadata
                .entry(leg.pool_id.clone())
                .or_insert_with(|| PoolStaticMetadata {
                    venues: Vec::new(),
                    route_ids: Vec::new(),
                });
            if !entry.venues.iter().any(|venue| venue == &leg.venue) {
                entry.venues.push(leg.venue.clone());
            }
            if !entry
                .route_ids
                .iter()
                .any(|route_id| route_id == &route.route_id)
            {
                entry.route_ids.push(route.route_id.clone());
            }
        }
    }
    metadata
}

fn buffer_capacity(config: &MonitorServerConfig) -> usize {
    let total = config
        .max_signal_samples
        .saturating_add(config.max_rejections)
        .saturating_add(config.max_trades);
    total.clamp(512, 16_384)
}

fn summarize_metrics(samples: &[MonitorSignalSample]) -> BTreeMap<String, MonitorSignalMetric> {
    let mut metrics = BTreeMap::new();
    metrics.insert(
        "ingest".into(),
        summarize_u64(samples.iter().map(|sample| sample.ingest_nanos)),
    );
    metrics.insert(
        "queue_wait".into(),
        summarize_u64(samples.iter().map(|sample| sample.queue_wait_nanos)),
    );
    metrics.insert(
        "state_apply".into(),
        summarize_u64(samples.iter().map(|sample| sample.state_apply_nanos)),
    );
    metrics.insert(
        "select".into(),
        summarize_optional(samples.iter().map(|sample| sample.select_nanos)),
    );
    metrics.insert(
        "build".into(),
        summarize_optional(samples.iter().map(|sample| sample.build_nanos)),
    );
    metrics.insert(
        "sign".into(),
        summarize_optional(samples.iter().map(|sample| sample.sign_nanos)),
    );
    metrics.insert(
        "submit".into(),
        summarize_optional(samples.iter().map(|sample| sample.submit_nanos)),
    );
    metrics.insert(
        "source_to_submit".into(),
        summarize_optional(samples.iter().map(|sample| sample.total_to_submit_nanos)),
    );
    metrics
}

fn summarize_u64(values: impl Iterator<Item = u64>) -> MonitorSignalMetric {
    let collected = values.collect::<Vec<_>>();
    summarize_values(collected)
}

fn summarize_optional(values: impl Iterator<Item = Option<u64>>) -> MonitorSignalMetric {
    let collected = values.flatten().collect::<Vec<_>>();
    summarize_values(collected)
}

fn summarize_values(mut values: Vec<u64>) -> MonitorSignalMetric {
    values.sort_unstable();
    let latest = values.last().copied();
    MonitorSignalMetric {
        count: values.len(),
        latest_nanos: latest,
        p50_nanos: percentile(&values, 50),
        p95_nanos: percentile(&values, 95),
        p99_nanos: percentile(&values, 99),
    }
}

fn combine_latencies(base: Option<u64>, extra: Option<u64>) -> Option<u64> {
    base.zip(extra)
        .map(|(base, extra)| base.saturating_add(extra))
}

fn percentile(values: &[u64], pct: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) * pct) / 100;
    values.get(index).copied()
}

fn status_matches(item: &MonitorTradeEvent, status: Option<&str>) -> bool {
    match status.unwrap_or("all") {
        "all" => true,
        "pending" => item.outcome == "pending",
        "included" => item.outcome == "included",
        "failed" => {
            matches!(
                item.outcome.as_str(),
                "submit_rejected" | "failed" | "chain_too_little_output"
            )
        }
        other => item.submit_status == other || item.outcome == other,
    }
}

fn route_status_matches(item: &RouteMonitorView, status: &str) -> bool {
    match status {
        "all" => true,
        "eligible" => item.eligible_live,
        "shadow" => item.health_state == RouteHealthState::ShadowOnly,
        "blocked" => !item.eligible_live && item.health_state != RouteHealthState::ShadowOnly,
        other => route_health_label(item.health_state) == other,
    }
}

fn split_path_and_query(raw: &str) -> (&str, BTreeMap<String, String>) {
    let Some((path, query)) = raw.split_once('?') else {
        return (raw, BTreeMap::new());
    };
    let mut params = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(key.to_string(), value.replace("%20", " "));
    }
    (path, params)
}

fn parse_usize(value: Option<&String>) -> Option<usize> {
    value.and_then(|value| value.parse::<usize>().ok())
}

fn parse_u64(value: Option<&String>) -> Option<u64> {
    value.and_then(|value| value.parse::<u64>().ok())
}

fn parse_window(window: &str) -> Duration {
    match window {
        "10s" => Duration::from_secs(10),
        "15m" => Duration::from_secs(15 * 60),
        _ => Duration::from_secs(60),
    }
}

fn issue_label(issue: &crate::control::RuntimeIssue) -> String {
    serde_json::to_string(issue)
        .unwrap_or_else(|_| format!("{issue:?}"))
        .trim_matches('"')
        .to_string()
}

fn runtime_mode_label(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Starting => "starting",
        RuntimeMode::Ready => "ready",
        RuntimeMode::Degraded => "degraded",
        RuntimeMode::Stopped => "stopped",
    }
}

fn source_label(source: &detection::EventSourceKind) -> &'static str {
    match source {
        detection::EventSourceKind::ShredStream => "shredstream",
        detection::EventSourceKind::Replay => "replay",
        detection::EventSourceKind::Synthetic => "synthetic",
    }
}

fn submit_status_label(status: submit::SubmitStatus) -> &'static str {
    match status {
        submit::SubmitStatus::Accepted => "accepted",
        submit::SubmitStatus::Rejected => "rejected",
    }
}

fn outcome_label(outcome: &ExecutionOutcome) -> &'static str {
    match outcome {
        ExecutionOutcome::Pending => "pending",
        ExecutionOutcome::Included { .. } => "included",
        ExecutionOutcome::Failed(FailureClass::SubmitRejected) => "submit_rejected",
        ExecutionOutcome::Failed(FailureClass::ChainExecutionTooLittleOutput) => {
            "chain_too_little_output"
        }
        ExecutionOutcome::Failed(_) => "failed",
    }
}

fn strategy_reason_code(reason: &RejectionReason) -> &'static str {
    match reason {
        RejectionReason::RouteNotRegistered => "route_not_registered",
        RejectionReason::RouteNotWarm { .. } => "route_not_warm",
        RejectionReason::MissingSnapshot { .. } => "missing_snapshot",
        RejectionReason::SnapshotStale { .. } => "snapshot_stale",
        RejectionReason::QuoteStaleForExecution { .. } => "quote_stale_for_execution",
        RejectionReason::PoolRepairPending { .. } => "pool_repair_pending",
        RejectionReason::PoolStateNotExecutable { .. } => "pool_state_not_executable",
        RejectionReason::PoolQuoteModelNotExecutable { .. } => "pool_quote_model_not_executable",
        RejectionReason::ReserveUsageTooHigh { .. } => "reserve_usage_too_high",
        RejectionReason::BlockhashUnavailable => "blockhash_unavailable",
        RejectionReason::BlockhashTooStale { .. } => "blockhash_too_stale",
        RejectionReason::ProfitBelowThreshold { .. } => "profit_below_threshold",
        RejectionReason::TradeSizeTooLarge { .. } => "trade_size_too_large",
        RejectionReason::WalletNotReady => "wallet_not_ready",
        RejectionReason::WalletBalanceTooLow { .. } => "wallet_balance_too_low",
        RejectionReason::TooManyInflight { .. } => "too_many_inflight",
        RejectionReason::KillSwitchActive => "kill_switch_active",
        RejectionReason::QuoteFailed { .. } => "quote_failed",
        RejectionReason::ExecutionCostNotConvertible { .. } => "execution_cost_not_convertible",
        RejectionReason::NoImpactedRoutes => "no_impacted_routes",
        RejectionReason::RouteFilteredOut { .. } => "route_filtered_out",
    }
}

fn strategy_reason_detail(reason: &RejectionReason) -> String {
    match reason {
        RejectionReason::RouteNotWarm { status } => format!("warmup_status={status:?}"),
        RejectionReason::MissingSnapshot { pool_id } => format!("pool_id={}", pool_id.0),
        RejectionReason::SnapshotStale { pool_id, slot_lag } => {
            format!("pool_id={}, slot_lag={slot_lag}", pool_id.0)
        }
        RejectionReason::QuoteStaleForExecution {
            pool_id,
            slot_lag,
            maximum,
        } => format!(
            "pool_id={}, slot_lag={slot_lag}, maximum={maximum}",
            pool_id.0
        ),
        RejectionReason::PoolRepairPending { pool_id } => format!("pool_id={}", pool_id.0),
        RejectionReason::PoolStateNotExecutable { pool_id } => format!("pool_id={}", pool_id.0),
        RejectionReason::PoolQuoteModelNotExecutable { pool_id } => {
            format!("pool_id={}", pool_id.0)
        }
        RejectionReason::ReserveUsageTooHigh {
            pool_id,
            usage_bps,
            maximum_bps,
        } => format!(
            "pool_id={}, usage_bps={usage_bps}, maximum_bps={maximum_bps}",
            pool_id.0
        ),
        RejectionReason::BlockhashTooStale { slot_lag, maximum } => {
            format!("slot_lag={slot_lag}, maximum={maximum}")
        }
        RejectionReason::ProfitBelowThreshold { expected, minimum } => {
            format!("expected={expected}, minimum={minimum}")
        }
        RejectionReason::TradeSizeTooLarge { requested, maximum } => {
            format!("requested={requested}, maximum={maximum}")
        }
        RejectionReason::WalletBalanceTooLow { current, minimum } => {
            format!("current={current}, minimum={minimum}")
        }
        RejectionReason::TooManyInflight { current, maximum } => {
            format!("current={current}, maximum={maximum}")
        }
        RejectionReason::QuoteFailed { detail } => detail.clone(),
        RejectionReason::ExecutionCostNotConvertible { detail } => detail.clone(),
        RejectionReason::RouteFilteredOut { route_id } => format!("route_id={}", route_id.0),
        _ => format!("{reason:?}"),
    }
}

fn strategy_expected_profit(reason: &RejectionReason) -> Option<i64> {
    match reason {
        RejectionReason::ProfitBelowThreshold { expected, .. } => Some(*expected),
        _ => None,
    }
}

fn build_reason_code(reason: &BuildRejectionReason) -> &'static str {
    match reason {
        BuildRejectionReason::MissingBlockhash => "missing_blockhash",
        BuildRejectionReason::MissingFeePayer => "missing_fee_payer",
        BuildRejectionReason::KillSwitchActive => "kill_switch_active",
        BuildRejectionReason::ExecutionPathCongested => "execution_path_congested",
        BuildRejectionReason::ExecutionPathUnavailable => "execution_path_unavailable",
        BuildRejectionReason::UnsupportedRouteShape => "unsupported_route_shape",
        BuildRejectionReason::MissingRouteExecution => "missing_route_execution",
        BuildRejectionReason::MissingLookupTable => "missing_lookup_table",
        BuildRejectionReason::LookupTableStale => "lookup_table_stale",
        BuildRejectionReason::QuoteStaleForExecution => "quote_stale_for_execution",
        BuildRejectionReason::MessageCompilationFailed => "message_compilation_failed",
        BuildRejectionReason::UnsupportedVenue => "unsupported_venue",
        BuildRejectionReason::MissingExecutionHint => "missing_execution_hint",
        BuildRejectionReason::TransactionTooLarge { .. } => "transaction_too_large",
        BuildRejectionReason::TooManyAccountLocks { .. } => "too_many_account_locks",
    }
}

fn build_reason_detail(reason: &BuildRejectionReason) -> String {
    match reason {
        BuildRejectionReason::TransactionTooLarge {
            serialized_bytes,
            maximum,
        } => format!("serialized_bytes={serialized_bytes}, maximum={maximum}"),
        BuildRejectionReason::TooManyAccountLocks {
            account_locks,
            maximum,
        } => format!("account_locks={account_locks}, maximum={maximum}"),
        _ => format!("{reason:?}"),
    }
}

fn submit_reason_code(reason: &SubmitRejectionReason) -> &'static str {
    match reason {
        SubmitRejectionReason::InvalidEnvelope => "invalid_envelope",
        SubmitRejectionReason::InvalidRequest => "invalid_request",
        SubmitRejectionReason::BundleDisabled => "bundle_disabled",
        SubmitRejectionReason::Unauthorized => "unauthorized",
        SubmitRejectionReason::RateLimited => "rate_limited",
        SubmitRejectionReason::DuplicateSubmission => "duplicate_submission",
        SubmitRejectionReason::TipTooLow => "tip_too_low",
        SubmitRejectionReason::PathCongested => "path_congested",
        SubmitRejectionReason::ChannelUnavailable => "channel_unavailable",
        SubmitRejectionReason::RemoteRejected => "remote_rejected",
    }
}

fn submit_reason_detail(reason: &SubmitRejectionReason) -> String {
    format!("{reason:?}")
}

fn pool_confidence_label(confidence: state::types::PoolConfidence) -> &'static str {
    match confidence {
        state::types::PoolConfidence::Decoded => "decoded",
        state::types::PoolConfidence::Verified => "verified",
        state::types::PoolConfidence::Executable => "executable",
        state::types::PoolConfidence::Invalid => "invalid",
    }
}

fn route_health_label(state: RouteHealthState) -> &'static str {
    match state {
        RouteHealthState::Eligible => "eligible",
        RouteHealthState::BlockedPoolStale => "blocked_pool_stale",
        RouteHealthState::BlockedPoolNotExecutable => "blocked_pool_not_executable",
        RouteHealthState::BlockedPoolQuoteModelNotExecutable => {
            "blocked_pool_quote_model_not_executable"
        }
        RouteHealthState::BlockedPoolRepair => "blocked_pool_repair",
        RouteHealthState::BlockedPoolQuarantined => "blocked_pool_quarantined",
        RouteHealthState::BlockedPoolDisabled => "blocked_pool_disabled",
        RouteHealthState::ShadowOnly => "shadow_only",
    }
}

fn duration_nanos(duration: Duration) -> Option<u64> {
    u64::try_from(duration.as_nanos()).ok()
}

fn duration_nanos_opt(duration: Option<Duration>) -> Option<u64> {
    duration.and_then(duration_nanos)
}

fn unix_millis(timestamp: SystemTime) -> u128 {
    timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn now_unix_millis() -> u128 {
    unix_millis(SystemTime::now())
}

fn clamp_i128(value: i128) -> i64 {
    value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::{
        MonitorServerConfig, ObserverState, PoolStaticMetadata, RepairEvent, RepairEventKind,
        build_reason_code, parse_window, strategy_reason_code,
    };
    use builder::BuildRejectionReason;
    use detection::EventSourceKind;
    use reconciliation::{ExecutionOutcome, ExecutionRecord, InclusionStatus};
    use state::types::{PoolConfidence, PoolId, RouteId};
    use std::{
        collections::BTreeMap,
        time::{Duration, UNIX_EPOCH},
    };
    use strategy::{opportunity::SelectionOutcome, reasons::RejectionReason};
    use submit::{SubmissionId, SubmitMode, SubmitStatus};

    use crate::{
        config::LiveSetHealthConfig,
        route_health::{RouteHealthRegistry, SharedRouteHealth},
        runtime::{HotPathReport, PipelineTrace},
    };

    fn test_config() -> MonitorServerConfig {
        MonitorServerConfig {
            enabled: true,
            bind_address: "127.0.0.1:8081".into(),
            poll_interval_millis: 250,
            max_signal_samples: 2,
            max_rejections: 2,
            max_trades: 2,
        }
    }

    fn test_route_health() -> SharedRouteHealth {
        std::sync::Arc::new(std::sync::Mutex::new(RouteHealthRegistry::new(
            LiveSetHealthConfig::default(),
        )))
    }

    #[test]
    fn maps_strategy_reason_codes_stably() {
        assert_eq!(
            strategy_reason_code(&RejectionReason::KillSwitchActive),
            "kill_switch_active"
        );
        assert_eq!(
            strategy_reason_code(&RejectionReason::BlockhashUnavailable),
            "blockhash_unavailable"
        );
        assert_eq!(
            strategy_reason_code(&RejectionReason::PoolRepairPending {
                pool_id: PoolId("pool-a".into()),
            }),
            "pool_repair_pending"
        );
    }

    #[test]
    fn maps_build_reason_codes_stably() {
        assert_eq!(
            build_reason_code(&BuildRejectionReason::MissingLookupTable),
            "missing_lookup_table"
        );
        assert_eq!(
            build_reason_code(&BuildRejectionReason::TransactionTooLarge {
                serialized_bytes: 1400,
                maximum: 1232,
            }),
            "transaction_too_large"
        );
    }

    #[test]
    fn parse_window_defaults_to_one_minute() {
        assert_eq!(parse_window("1m").as_secs(), 60);
        assert_eq!(parse_window("10s").as_secs(), 10);
    }

    #[test]
    fn trims_signal_history() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            test_route_health(),
        );
        state.next_seq = 1;
        state.signals.push_back(super::MonitorSignalSample {
            seq: 1,
            source: "synthetic".into(),
            source_sequence: 1,
            observed_slot: 1,
            source_received_at_unix_millis: 1,
            normalized_at_unix_millis: 1,
            source_latency_nanos: None,
            ingest_nanos: 1,
            queue_wait_nanos: 1,
            state_apply_nanos: 1,
            select_nanos: None,
            build_nanos: None,
            sign_nanos: None,
            submit_nanos: None,
            total_to_submit_nanos: None,
        });
        state.signals.push_back(super::MonitorSignalSample {
            seq: 2,
            source: "synthetic".into(),
            source_sequence: 2,
            observed_slot: 2,
            source_received_at_unix_millis: 2,
            normalized_at_unix_millis: 2,
            source_latency_nanos: None,
            ingest_nanos: 2,
            queue_wait_nanos: 2,
            state_apply_nanos: 2,
            select_nanos: None,
            build_nanos: None,
            sign_nanos: None,
            submit_nanos: None,
            total_to_submit_nanos: None,
        });
        state.signals.push_back(super::MonitorSignalSample {
            seq: 3,
            source: "synthetic".into(),
            source_sequence: 3,
            observed_slot: 3,
            source_received_at_unix_millis: 3,
            normalized_at_unix_millis: 3,
            source_latency_nanos: None,
            ingest_nanos: 3,
            queue_wait_nanos: 3,
            state_apply_nanos: 3,
            select_nanos: None,
            build_nanos: None,
            sign_nanos: None,
            submit_nanos: None,
            total_to_submit_nanos: None,
        });
        while state.signals.len() > state.config.max_signal_samples {
            state.signals.pop_front();
        }
        assert_eq!(state.signals.len(), 2);
        assert_eq!(state.signals.front().map(|sample| sample.seq), Some(2));
    }

    #[test]
    fn recomputes_source_to_terminal_latency_when_trade_completes() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            test_route_health(),
        );
        let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
        let record = ExecutionRecord {
            route_id: RouteId("route-a".into()),
            submission_id: SubmissionId("submission-a".into()),
            chain_signature: "sig-a".into(),
            submit_mode: SubmitMode::SingleTransaction,
            submit_endpoint: "http://127.0.0.1:3000".into(),
            submit_status: SubmitStatus::Accepted,
            quoted_slot: 42,
            blockhash_slot: Some(43),
            submitted_slot: Some(44),
            inclusion_status: InclusionStatus::Submitted,
            outcome: ExecutionOutcome::Pending,
            failure_detail: None,
            submitted_at: created_at,
            last_updated_at: created_at,
        };
        let report = HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: Some(record.clone()),
            submit_leader: None,
            pipeline_trace: PipelineTrace {
                source: EventSourceKind::ShredStream,
                source_sequence: 7,
                observed_slot: 42,
                source_received_at: created_at,
                normalized_at: created_at,
                source_latency: Some(Duration::from_millis(2)),
                ingest_duration: Duration::from_millis(1),
                queue_wait_duration: Duration::from_millis(3),
                state_apply_duration: Duration::from_micros(4),
                select_duration: None,
                build_duration: None,
                sign_duration: None,
                submit_duration: None,
                total_to_submit: Some(Duration::from_millis(7)),
            },
        };

        state.apply_hot_path(report);
        let pending = state.trades.get("submission-a").unwrap();
        assert_eq!(pending.source_to_submit_nanos, Some(7_000_000));
        assert_eq!(pending.submit_to_terminal_nanos, None);
        assert_eq!(pending.source_to_terminal_nanos, None);
        assert_eq!(pending.quoted_slot, 42);
        assert_eq!(pending.blockhash_slot, Some(43));
        assert_eq!(pending.submitted_slot, Some(44));

        let completed_at = created_at.checked_add(Duration::from_millis(5)).unwrap();
        state.apply_trade_update(ExecutionRecord {
            inclusion_status: InclusionStatus::Landed { slot: 44 },
            outcome: ExecutionOutcome::Included { slot: 44 },
            last_updated_at: completed_at,
            ..record
        });

        let completed = state.trades.get("submission-a").unwrap();
        assert_eq!(completed.submit_to_terminal_nanos, Some(5_000_000));
        assert_eq!(completed.source_to_terminal_nanos, Some(12_000_000));
    }

    #[test]
    fn apply_status_refreshes_pool_freshness() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            test_route_health(),
        );

        state.upsert_pool(state::types::PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: state::types::LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 11,
            derived_at: UNIX_EPOCH.checked_add(Duration::from_millis(180)).unwrap(),
            freshness: state::types::FreshnessState::at(11, 11, 2),
        });

        let mut status = crate::control::RuntimeStatus::default();
        status.latest_slot = 20;
        state.apply_status(status);

        let pool = state.pools.get("pool-a").expect("pool view should exist");
        assert_eq!(pool.slot_lag, 9);
        assert_eq!(pool.age_slots, 9);
        assert!(pool.stale);
    }

    #[test]
    fn tracks_repair_metrics_across_pending_and_exact_snapshots() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            test_route_health(),
        );

        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RepairAttemptStarted,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(10)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RepairAttemptFailed,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(20)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RepairAttemptStarted,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(30)).unwrap(),
        });
        state.upsert_pool(state::types::PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Invalid,
            repair_pending: true,
            liquidity_model: state::types::LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 10,
            derived_at: UNIX_EPOCH.checked_add(Duration::from_millis(100)).unwrap(),
            freshness: state::types::FreshnessState::at(10, 10, 2),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RepairAttemptSucceeded { latency_ms: 45 },
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(150)).unwrap(),
        });
        state.upsert_pool(state::types::PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: state::types::LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 11,
            derived_at: UNIX_EPOCH.checked_add(Duration::from_millis(180)).unwrap(),
            freshness: state::types::FreshnessState::at(11, 11, 2),
        });

        let pool = state.pools.get("pool-a").expect("pool view should exist");
        assert!(!pool.refresh_pending);
        assert_eq!(pool.refresh_attempt_count, 0);
        assert_eq!(pool.repair_attempt_count, 2);
        assert_eq!(pool.repair_failure_count, 1);
        assert_eq!(pool.repair_success_count, 1);
        assert_eq!(pool.last_repair_latency_ms, Some(45));
        assert_eq!(pool.last_repair_invalidation_ms, Some(80));
    }

    #[test]
    fn tracks_refresh_metrics_separately_from_repair_metrics() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            test_route_health(),
        );

        state.apply_status(crate::control::RuntimeStatus {
            latest_slot: 10,
            ..Default::default()
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshScheduled { deadline_slot: 18 },
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(10)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshAttemptStarted,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(20)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshAttemptFailed,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(30)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshScheduled { deadline_slot: 19 },
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(40)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshAttemptStarted,
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(50)).unwrap(),
        });
        state.apply_repair(RepairEvent {
            pool_id: "pool-a".into(),
            kind: RepairEventKind::RefreshAttemptSucceeded { latency_ms: 12 },
            occurred_at: UNIX_EPOCH.checked_add(Duration::from_millis(60)).unwrap(),
        });
        state.upsert_pool(state::types::PoolSnapshot {
            pool_id: PoolId("pool-a".into()),
            price_bps: 10_000,
            fee_bps: 4,
            reserve_depth: 100_000,
            reserve_a: Some(100_000),
            reserve_b: Some(100_000),
            active_liquidity: 100_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: state::types::LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 11,
            derived_at: UNIX_EPOCH.checked_add(Duration::from_millis(80)).unwrap(),
            freshness: state::types::FreshnessState::at(11, 11, 2),
        });

        let pool = state.pools.get("pool-a").expect("pool view should exist");
        assert!(!pool.refresh_pending);
        assert_eq!(pool.refresh_attempt_count, 2);
        assert_eq!(pool.refresh_failure_count, 1);
        assert_eq!(pool.refresh_success_count, 1);
        assert_eq!(pool.last_refresh_latency_ms, Some(12));
        assert_eq!(pool.refresh_deadline_slot_lag, None);
        assert_eq!(pool.repair_attempt_count, 0);
    }
}
