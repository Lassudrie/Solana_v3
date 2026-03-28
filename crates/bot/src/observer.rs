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

use builder::{BuildRejectionReason, SwapAmountMode};
use reconciliation::{ExecutionOutcome, ExecutionRecord, FailureClass};
use serde::{Deserialize, Serialize};
use state::types::PoolSnapshot;
use strategy::{
    opportunity::{CandidateSelectionSource, OpportunityDecision},
    reasons::RejectionReason,
    route_registry::RouteKind,
};
use submit::SubmitRejectionReason;

pub use crate::route_health::RouteMonitorView;
use crate::{
    config::{BotConfig, MonitorServerConfig, RouteClassConfig, RuntimeProfileConfig},
    control::{RuntimeMode, RuntimeStatus},
    persistence::PersistenceHandle,
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
    pub lane: String,
    pub source: String,
    pub source_sequence: u64,
    pub observed_slot: u64,
    pub source_received_at_unix_millis: u128,
    pub normalized_at_unix_millis: u128,
    pub router_received_at_unix_millis: u128,
    pub state_published_at_unix_millis: Option<u128>,
    pub source_latency_nanos: Option<u64>,
    pub ingest_nanos: u64,
    pub queue_wait_nanos: u64,
    pub state_mailbox_age_nanos: Option<u64>,
    pub trigger_queue_wait_nanos: Option<u64>,
    pub trigger_barrier_wait_nanos: Option<u64>,
    pub state_apply_nanos: u64,
    pub route_eval_nanos: Option<u64>,
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
    pub route_kind: Option<String>,
    pub leg_count: usize,
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
    pub route_kind: String,
    pub leg_count: usize,
    pub trade_size: Option<u64>,
    pub submission_id: String,
    pub signature: String,
    pub submit_status: String,
    pub outcome: String,
    pub selected_by: String,
    pub ranking_score_quote_atoms: Option<i64>,
    pub expected_edge_quote_atoms: i64,
    pub estimated_cost_lamports: Option<u64>,
    pub estimated_cost_quote_atoms: Option<u64>,
    pub expected_pnl_quote_atoms: Option<i64>,
    pub expected_value_quote_atoms: Option<i64>,
    pub p_land_bps: Option<u16>,
    pub expected_shortfall_quote_atoms: Option<u64>,
    pub intermediate_output_amounts: Vec<u64>,
    pub leg_snapshot_slots: Vec<u64>,
    pub shadow_selected_by: Option<String>,
    pub shadow_trade_size: Option<u64>,
    pub shadow_ranking_score_quote_atoms: Option<i64>,
    pub shadow_expected_value_quote_atoms: Option<i64>,
    pub shadow_expected_pnl_quote_atoms: Option<i64>,
    pub realized_pnl_quote_atoms: Option<i64>,
    pub jito_tip_lamports: Option<u64>,
    pub active_execution_buffer_bps: Option<u16>,
    pub source_input_balance: Option<u64>,
    pub oldest_snapshot_slot: Option<u64>,
    pub quote_state_age_slots: Option<u64>,
    pub leg_ticks_crossed: Vec<u32>,
    pub leg_amount_modes: Vec<String>,
    pub leg_specified_amounts: Vec<u64>,
    pub leg_threshold_amounts: Vec<u64>,
    pub submit_leader: Option<String>,
    pub failure_bucket: Option<String>,
    pub failure_program_id: Option<String>,
    pub failure_instruction_index: Option<u8>,
    pub failure_custom_code: Option<u32>,
    pub failure_error_name: Option<String>,
    pub endpoint: String,
    pub quoted_slot: u64,
    pub blockhash_slot: Option<u64>,
    pub submitted_slot: Option<u64>,
    pub terminal_slot: Option<u64>,
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
pub struct MonitorEdgeOverview {
    pub trade_count: u64,
    pub pending_count: u64,
    pub included_count: u64,
    pub failed_count: u64,
    pub submit_rejected_count: u64,
    pub too_little_output_fail_count: u64,
    pub amount_in_above_maximum_fail_count: u64,
    pub expected_session_pnl_quote_atoms: i64,
    pub captured_expected_session_pnl_quote_atoms: i64,
    pub missed_expected_session_pnl_quote_atoms: i64,
    pub edge_capture_rate_bps: u64,
    pub realized_session_pnl_quote_atoms: i64,
    pub pnl_realization_rate_bps: u64,
    pub avg_source_to_submit_nanos: Option<u64>,
    pub avg_source_to_terminal_nanos: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorEdgeRouteView {
    pub route_id: String,
    pub route_kind: Option<String>,
    pub health_state: Option<RouteHealthState>,
    pub eligible_live: Option<bool>,
    pub blocking_reason: Option<String>,
    pub selected_count: u64,
    pub pending_count: u64,
    pub included_count: u64,
    pub failed_count: u64,
    pub submit_rejected_count: u64,
    pub too_little_output_fail_count: u64,
    pub amount_in_above_maximum_fail_count: u64,
    pub expected_pnl_quote_atoms: i64,
    pub captured_expected_pnl_quote_atoms: i64,
    pub missed_expected_pnl_quote_atoms: i64,
    pub edge_capture_rate_bps: u64,
    pub realized_pnl_quote_atoms: i64,
    pub pnl_realization_rate_bps: u64,
    pub avg_expected_pnl_quote_atoms: Option<i64>,
    pub avg_realized_pnl_quote_atoms: Option<i64>,
    pub avg_source_to_submit_nanos: Option<u64>,
    pub avg_source_to_terminal_nanos: Option<u64>,
    pub last_selected_by: Option<String>,
    pub last_p_land_bps: Option<u16>,
    pub last_active_execution_buffer_bps: Option<u16>,
    pub last_trade_size: Option<u64>,
    pub last_quoted_slot: Option<u64>,
    pub last_submitted_slot: Option<u64>,
    pub last_terminal_slot: Option<u64>,
    pub last_updated_at_unix_millis: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorEdgeResponse {
    pub overview: MonitorEdgeOverview,
    pub routes: Vec<MonitorEdgeRouteView>,
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
    pub destabilizing_unclassified_count: u64,
    pub destabilizing_candidate_count: u64,
    pub destabilizing_trigger_count: u64,
    pub destabilizing_price_impact_floor_bps: u16,
    pub destabilizing_price_dislocation_trigger_bps: u16,
    pub last_destabilizing_trigger_pool_id: Option<String>,
    pub last_destabilizing_trigger_slot: Option<u64>,
    pub last_destabilizing_trigger_price_impact_bps: Option<u16>,
    pub last_destabilizing_trigger_price_dislocation_bps: Option<u16>,
    pub last_destabilizing_trigger_net_edge_bps: Option<u16>,
    pub destabilizing_candidate_max_price_impact_bps: Option<u16>,
    pub destabilizing_candidate_p95_price_impact_bps: Option<u16>,
    pub destabilizing_candidate_max_price_dislocation_bps: Option<u16>,
    pub destabilizing_candidate_p95_price_dislocation_bps: Option<u16>,
    pub destabilizing_candidate_max_net_edge_bps: Option<u16>,
    pub destabilizing_candidate_p95_net_edge_bps: Option<u16>,
    pub landed_rate_bps: u64,
    pub reject_rate_bps: u64,
    pub expected_session_pnl_quote_atoms: i64,
    pub captured_expected_session_pnl_quote_atoms: i64,
    pub missed_expected_session_pnl_quote_atoms: i64,
    pub edge_capture_rate_bps: u64,
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

#[derive(Debug, Clone)]
pub(crate) struct DestabilizationEvent {
    pub pool_id: String,
    pub observed_slot: u64,
    pub kind: DestabilizationEventKind,
    pub input_amount: Option<u64>,
    pub estimated_price_impact_bps: Option<u16>,
    pub estimated_price_dislocation_bps: Option<u16>,
    pub estimated_net_edge_bps: Option<u16>,
    pub impact_floor_bps: u16,
    pub dislocation_trigger_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DestabilizationEventKind {
    Unclassified,
    Candidate,
    Triggered,
}

#[derive(Debug, Clone, Default)]
struct DestabilizationStats {
    unclassified_count: u64,
    candidate_count: u64,
    trigger_count: u64,
    impact_floor_bps: u16,
    dislocation_trigger_bps: u16,
    last_trigger_pool_id: Option<String>,
    last_trigger_slot: Option<u64>,
    last_trigger_price_impact_bps: Option<u16>,
    last_trigger_price_dislocation_bps: Option<u16>,
    last_trigger_net_edge_bps: Option<u16>,
    max_candidate_price_impact_bps: Option<u16>,
    recent_candidate_impacts_bps: VecDeque<u16>,
    max_candidate_price_dislocation_bps: Option<u16>,
    recent_candidate_dislocations_bps: VecDeque<u16>,
    max_candidate_net_edge_bps: Option<u16>,
    recent_candidate_net_edges_bps: VecDeque<u16>,
}

const DESTABILIZATION_IMPACT_SAMPLE_WINDOW: usize = 1024;

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
    cumulative_captured_expected_pnl: i128,
    cumulative_missed_expected_pnl: i128,
    cumulative_realized_pnl: i128,
    destabilization_stats: DestabilizationStats,
    next_seq: u64,
}

#[derive(Debug, Clone, Default)]
struct RouteEdgeAggregate {
    route_kind: Option<String>,
    selected_count: u64,
    pending_count: u64,
    included_count: u64,
    failed_count: u64,
    submit_rejected_count: u64,
    too_little_output_fail_count: u64,
    amount_in_above_maximum_fail_count: u64,
    expected_pnl_quote_atoms: i128,
    captured_expected_pnl_quote_atoms: i128,
    missed_expected_pnl_quote_atoms: i128,
    realized_pnl_quote_atoms: i128,
    realized_trade_count: u64,
    source_to_submit_nanos_total: u128,
    source_to_submit_count: u64,
    source_to_terminal_nanos_total: u128,
    source_to_terminal_count: u64,
    last_selected_by: Option<String>,
    last_p_land_bps: Option<u16>,
    last_active_execution_buffer_bps: Option<u16>,
    last_trade_size: Option<u64>,
    last_quoted_slot: Option<u64>,
    last_submitted_slot: Option<u64>,
    last_terminal_slot: Option<u64>,
    last_updated_at_unix_millis: Option<u128>,
}

impl RouteEdgeAggregate {
    fn record(&mut self, item: &MonitorTradeEvent) {
        self.route_kind = Some(item.route_kind.clone());
        self.selected_count = self.selected_count.saturating_add(1);
        if let Some(expected) = item.expected_pnl_quote_atoms {
            self.expected_pnl_quote_atoms += i128::from(expected);
        }
        if let Some(realized) = item.realized_pnl_quote_atoms {
            self.realized_pnl_quote_atoms += i128::from(realized);
            self.realized_trade_count = self.realized_trade_count.saturating_add(1);
        }
        if let Some(nanos) = item.source_to_submit_nanos {
            self.source_to_submit_nanos_total += u128::from(nanos);
            self.source_to_submit_count = self.source_to_submit_count.saturating_add(1);
        }
        if let Some(nanos) = item.source_to_terminal_nanos {
            self.source_to_terminal_nanos_total += u128::from(nanos);
            self.source_to_terminal_count = self.source_to_terminal_count.saturating_add(1);
        }
        if trade_outcome_is_included(item.outcome.as_str()) {
            self.included_count = self.included_count.saturating_add(1);
            if let Some(expected) = item.expected_pnl_quote_atoms {
                self.captured_expected_pnl_quote_atoms += i128::from(expected);
            }
        } else if trade_outcome_is_failed(item.outcome.as_str()) {
            self.failed_count = self.failed_count.saturating_add(1);
            if item.outcome == "submit_rejected" {
                self.submit_rejected_count = self.submit_rejected_count.saturating_add(1);
            }
            match item.failure_custom_code {
                Some(6_036) | Some(6_022) => {
                    self.too_little_output_fail_count =
                        self.too_little_output_fail_count.saturating_add(1);
                }
                Some(6_037) => {
                    self.amount_in_above_maximum_fail_count =
                        self.amount_in_above_maximum_fail_count.saturating_add(1);
                }
                _ => {}
            }
            if let Some(expected) = item.expected_pnl_quote_atoms {
                self.missed_expected_pnl_quote_atoms += i128::from(expected);
            }
        } else {
            self.pending_count = self.pending_count.saturating_add(1);
        }
        self.last_selected_by = Some(item.selected_by.clone());
        self.last_p_land_bps = item.p_land_bps;
        self.last_active_execution_buffer_bps = item.active_execution_buffer_bps;
        self.last_trade_size = item.trade_size;
        self.last_quoted_slot = Some(item.quoted_slot);
        self.last_submitted_slot = item.submitted_slot;
        self.last_terminal_slot = item.terminal_slot;
        self.last_updated_at_unix_millis = Some(item.updated_at_unix_millis);
    }
}

impl ObserverState {
    fn new(
        config: MonitorServerConfig,
        pool_metadata: BTreeMap<String, PoolStaticMetadata>,
        max_snapshot_slot_lag: u64,
        destabilization_impact_floor_bps: u16,
        destabilization_dislocation_trigger_bps: u16,
        route_health: SharedRouteHealth,
    ) -> Self {
        let destabilization_stats = DestabilizationStats {
            impact_floor_bps: destabilization_impact_floor_bps,
            dislocation_trigger_bps: destabilization_dislocation_trigger_bps,
            ..DestabilizationStats::default()
        };
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
            cumulative_captured_expected_pnl: 0,
            cumulative_missed_expected_pnl: 0,
            cumulative_realized_pnl: 0,
            destabilization_stats,
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
        let previous_outcome = existing.outcome.clone();
        let previous_realized_pnl_quote_atoms = existing.realized_pnl_quote_atoms;
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
        existing.terminal_slot = record.terminal_slot;
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
        existing.realized_pnl_quote_atoms = record.realized_pnl_quote_atoms;
        if !trade_outcome_is_terminal(previous_outcome.as_str())
            && trade_outcome_is_terminal(existing.outcome.as_str())
        {
            if let Some(expected) = existing.expected_pnl_quote_atoms {
                if trade_outcome_is_included(existing.outcome.as_str()) {
                    self.cumulative_captured_expected_pnl += i128::from(expected);
                } else if trade_outcome_is_failed(existing.outcome.as_str()) {
                    self.cumulative_missed_expected_pnl += i128::from(expected);
                }
            }
        }
        let realized_delta = i128::from(existing.realized_pnl_quote_atoms.unwrap_or_default())
            .saturating_sub(i128::from(
                previous_realized_pnl_quote_atoms.unwrap_or_default(),
            ));
        self.cumulative_realized_pnl = self.cumulative_realized_pnl.saturating_add(realized_delta);
    }

    fn apply_destabilization(&mut self, event: DestabilizationEvent) {
        self.destabilization_stats.impact_floor_bps = event.impact_floor_bps;
        self.destabilization_stats.dislocation_trigger_bps = event.dislocation_trigger_bps;
        match event.kind {
            DestabilizationEventKind::Unclassified => {
                self.destabilization_stats.unclassified_count = self
                    .destabilization_stats
                    .unclassified_count
                    .saturating_add(1);
            }
            DestabilizationEventKind::Candidate => {
                self.destabilization_stats.candidate_count =
                    self.destabilization_stats.candidate_count.saturating_add(1);
                if let Some(impact_bps) = event.estimated_price_impact_bps {
                    self.destabilization_stats.max_candidate_price_impact_bps = Some(
                        self.destabilization_stats
                            .max_candidate_price_impact_bps
                            .map(|current| current.max(impact_bps))
                            .unwrap_or(impact_bps),
                    );
                    self.destabilization_stats
                        .recent_candidate_impacts_bps
                        .push_back(impact_bps);
                    if self
                        .destabilization_stats
                        .recent_candidate_impacts_bps
                        .len()
                        > DESTABILIZATION_IMPACT_SAMPLE_WINDOW
                    {
                        self.destabilization_stats
                            .recent_candidate_impacts_bps
                            .pop_front();
                    }
                }
                if let Some(dislocation_bps) = event.estimated_price_dislocation_bps {
                    self.destabilization_stats
                        .max_candidate_price_dislocation_bps = Some(
                        self.destabilization_stats
                            .max_candidate_price_dislocation_bps
                            .map(|current| current.max(dislocation_bps))
                            .unwrap_or(dislocation_bps),
                    );
                    self.destabilization_stats
                        .recent_candidate_dislocations_bps
                        .push_back(dislocation_bps);
                    if self
                        .destabilization_stats
                        .recent_candidate_dislocations_bps
                        .len()
                        > DESTABILIZATION_IMPACT_SAMPLE_WINDOW
                    {
                        self.destabilization_stats
                            .recent_candidate_dislocations_bps
                            .pop_front();
                    }
                }
                if let Some(net_edge_bps) = event.estimated_net_edge_bps {
                    self.destabilization_stats.max_candidate_net_edge_bps = Some(
                        self.destabilization_stats
                            .max_candidate_net_edge_bps
                            .map(|current| current.max(net_edge_bps))
                            .unwrap_or(net_edge_bps),
                    );
                    self.destabilization_stats
                        .recent_candidate_net_edges_bps
                        .push_back(net_edge_bps);
                    if self
                        .destabilization_stats
                        .recent_candidate_net_edges_bps
                        .len()
                        > DESTABILIZATION_IMPACT_SAMPLE_WINDOW
                    {
                        self.destabilization_stats
                            .recent_candidate_net_edges_bps
                            .pop_front();
                    }
                }
            }
            DestabilizationEventKind::Triggered => {
                self.destabilization_stats.trigger_count =
                    self.destabilization_stats.trigger_count.saturating_add(1);
                self.destabilization_stats.last_trigger_pool_id = Some(event.pool_id);
                self.destabilization_stats.last_trigger_slot = Some(event.observed_slot);
                self.destabilization_stats.last_trigger_price_impact_bps =
                    event.estimated_price_impact_bps;
                self.destabilization_stats
                    .last_trigger_price_dislocation_bps = event.estimated_price_dislocation_bps;
                self.destabilization_stats.last_trigger_net_edge_bps = event.estimated_net_edge_bps;
            }
        }
        let _ = event.input_amount;
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
            lane: event_lane_label(trace.lane).to_string(),
            source: source_label(&trace.source).to_string(),
            source_sequence: trace.source_sequence,
            observed_slot: trace.observed_slot,
            source_received_at_unix_millis: unix_millis(trace.source_received_at),
            normalized_at_unix_millis: unix_millis(trace.normalized_at),
            router_received_at_unix_millis: unix_millis(trace.router_received_at),
            state_published_at_unix_millis: trace.state_published_at.map(unix_millis),
            source_latency_nanos: duration_nanos_opt(trace.source_latency),
            ingest_nanos: duration_nanos(trace.ingest_duration).unwrap_or(0),
            queue_wait_nanos: duration_nanos(trace.queue_wait_duration).unwrap_or(0),
            state_mailbox_age_nanos: duration_nanos_opt(trace.state_mailbox_age_duration),
            trigger_queue_wait_nanos: duration_nanos_opt(trace.trigger_queue_wait_duration),
            trigger_barrier_wait_nanos: duration_nanos_opt(trace.trigger_barrier_wait_duration),
            state_apply_nanos: duration_nanos(trace.state_apply_duration).unwrap_or(0),
            route_eval_nanos: duration_nanos_opt(trace.route_eval_duration),
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
            let OpportunityDecision::Rejected {
                route_id,
                route_kind,
                leg_count,
                reason,
            } = decision
            else {
                continue;
            };
            let seq = self.next_seq();
            self.push_rejection(RejectionEvent {
                seq,
                stage: "strategy".into(),
                route_id: route_id.0.clone(),
                route_kind: route_kind.map(|kind| route_kind_label(kind).into()),
                leg_count: *leg_count,
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
            route_kind: Some(route_kind_label(candidate.route_kind).into()),
            leg_count: candidate.leg_count(),
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
            route_kind: report
                .selection
                .best_candidate
                .as_ref()
                .map(|candidate| route_kind_label(candidate.route_kind).into()),
            leg_count: report
                .selection
                .best_candidate
                .as_ref()
                .map(|candidate| candidate.leg_count())
                .unwrap_or_default(),
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
        let expected_value = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.expected_value_quote_atoms);
        let p_land_bps = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.p_land_bps);
        let expected_shortfall_quote_atoms = report
            .selection
            .best_candidate
            .as_ref()
            .map(|candidate| candidate.expected_shortfall_quote_atoms);
        let shadow_candidate = report.selection.shadow_candidate.as_ref();
        let best_candidate = report.selection.best_candidate.as_ref();
        let envelope = report
            .build_result
            .as_ref()
            .and_then(|build| build.envelope.as_ref());
        let source_to_submit_nanos = duration_nanos_opt(report.pipeline_trace.total_to_submit);
        let submit_to_terminal_nanos = matches!(record.outcome, ExecutionOutcome::Pending)
            .then_some(None)
            .unwrap_or(Some(0));
        let entry = MonitorTradeEvent {
            seq: self.next_seq(),
            route_id: record.route_id.0.clone(),
            route_kind: best_candidate
                .map(|candidate| route_kind_label(candidate.route_kind).into())
                .unwrap_or_else(|| "unknown".into()),
            leg_count: best_candidate.map(|candidate| candidate.leg_count()).unwrap_or_default(),
            trade_size: best_candidate.map(|candidate| candidate.trade_size),
            submission_id: record.submission_id.0.clone(),
            signature: record.chain_signature.clone(),
            submit_status: submit_status_label(record.submit_status).into(),
            outcome: outcome_label(&record.outcome).into(),
            selected_by: best_candidate
                .map(|candidate| selection_source_label(candidate.selected_by).into())
                .unwrap_or_else(|| "unknown".into()),
            ranking_score_quote_atoms: best_candidate
                .map(|candidate| candidate.ranking_score_quote_atoms),
            expected_edge_quote_atoms: expected_edge,
            estimated_cost_lamports: estimated_cost,
            estimated_cost_quote_atoms,
            expected_pnl_quote_atoms: expected_pnl,
            expected_value_quote_atoms: expected_value,
            p_land_bps,
            expected_shortfall_quote_atoms,
            intermediate_output_amounts: best_candidate
                .map(|candidate| candidate.intermediate_output_amounts.clone())
                .unwrap_or_default(),
            leg_snapshot_slots: best_candidate
                .map(|candidate| candidate.leg_snapshot_slots.as_slice().to_vec())
                .unwrap_or_default(),
            shadow_selected_by: shadow_candidate
                .map(|candidate| selection_source_label(candidate.selected_by).into()),
            shadow_trade_size: shadow_candidate.map(|candidate| candidate.trade_size),
            shadow_ranking_score_quote_atoms: shadow_candidate
                .map(|candidate| candidate.ranking_score_quote_atoms),
            shadow_expected_value_quote_atoms: shadow_candidate
                .map(|candidate| candidate.expected_value_quote_atoms),
            shadow_expected_pnl_quote_atoms: shadow_candidate
                .map(|candidate| candidate.expected_net_profit_quote_atoms),
            realized_pnl_quote_atoms: record.realized_pnl_quote_atoms,
            jito_tip_lamports: envelope.map(|envelope| envelope.jito_tip_lamports),
            active_execution_buffer_bps: best_candidate
                .and_then(|candidate| candidate.active_execution_buffer_bps),
            source_input_balance: best_candidate.and_then(|candidate| candidate.source_input_balance),
            oldest_snapshot_slot: best_candidate.map(|candidate| candidate.oldest_relevant_snapshot_slot()),
            quote_state_age_slots: best_candidate
                .map(|candidate| candidate.quoted_slot.saturating_sub(candidate.oldest_relevant_snapshot_slot())),
            leg_ticks_crossed: best_candidate
                .map(|candidate| {
                    candidate
                        .leg_quotes
                        .as_slice()
                        .iter()
                        .map(|leg| leg.ticks_crossed)
                        .collect()
                })
                .unwrap_or_default(),
            leg_amount_modes: envelope
                .map(|envelope| {
                    envelope
                        .leg_plans
                        .as_slice()
                        .iter()
                        .map(|leg| swap_amount_mode_label(leg.amount_mode).to_string())
                        .collect()
                })
                .unwrap_or_default(),
            leg_specified_amounts: envelope
                .map(|envelope| {
                    envelope
                        .leg_plans
                        .as_slice()
                        .iter()
                        .map(|leg| leg.specified_amount)
                        .collect()
                })
                .unwrap_or_default(),
            leg_threshold_amounts: envelope
                .map(|envelope| {
                    envelope
                        .leg_plans
                        .as_slice()
                        .iter()
                        .map(|leg| leg.other_amount_threshold)
                        .collect()
                })
                .unwrap_or_default(),
            submit_leader: report.submit_leader.clone(),
            failure_bucket: failure_bucket(record),
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
            terminal_slot: record.terminal_slot,
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
            if let Some(realized) = entry.realized_pnl_quote_atoms {
                self.cumulative_realized_pnl += i128::from(realized);
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
        let edge_capture_rate_bps = ratio_bps(
            self.cumulative_captured_expected_pnl,
            self.cumulative_expected_pnl,
        );
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
            destabilizing_unclassified_count: self.destabilization_stats.unclassified_count,
            destabilizing_candidate_count: self.destabilization_stats.candidate_count,
            destabilizing_trigger_count: self.destabilization_stats.trigger_count,
            destabilizing_price_impact_floor_bps: self.destabilization_stats.impact_floor_bps,
            destabilizing_price_dislocation_trigger_bps: self
                .destabilization_stats
                .dislocation_trigger_bps,
            last_destabilizing_trigger_pool_id: self
                .destabilization_stats
                .last_trigger_pool_id
                .clone(),
            last_destabilizing_trigger_slot: self.destabilization_stats.last_trigger_slot,
            last_destabilizing_trigger_price_impact_bps: self
                .destabilization_stats
                .last_trigger_price_impact_bps,
            last_destabilizing_trigger_price_dislocation_bps: self
                .destabilization_stats
                .last_trigger_price_dislocation_bps,
            last_destabilizing_trigger_net_edge_bps: self
                .destabilization_stats
                .last_trigger_net_edge_bps,
            destabilizing_candidate_max_price_impact_bps: self
                .destabilization_stats
                .max_candidate_price_impact_bps,
            destabilizing_candidate_p95_price_impact_bps: percentile_u16(
                &self.destabilization_stats.recent_candidate_impacts_bps,
                95,
            ),
            destabilizing_candidate_max_price_dislocation_bps: self
                .destabilization_stats
                .max_candidate_price_dislocation_bps,
            destabilizing_candidate_p95_price_dislocation_bps: percentile_u16(
                &self.destabilization_stats.recent_candidate_dislocations_bps,
                95,
            ),
            destabilizing_candidate_max_net_edge_bps: self
                .destabilization_stats
                .max_candidate_net_edge_bps,
            destabilizing_candidate_p95_net_edge_bps: percentile_u16(
                &self.destabilization_stats.recent_candidate_net_edges_bps,
                95,
            ),
            landed_rate_bps,
            reject_rate_bps,
            expected_session_pnl_quote_atoms: clamp_i128(self.cumulative_expected_pnl),
            captured_expected_session_pnl_quote_atoms: clamp_i128(
                self.cumulative_captured_expected_pnl,
            ),
            missed_expected_session_pnl_quote_atoms: clamp_i128(
                self.cumulative_missed_expected_pnl,
            ),
            edge_capture_rate_bps,
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

    fn edge_response(
        &self,
        sort: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> MonitorEdgeResponse {
        let mut by_route = BTreeMap::<String, RouteEdgeAggregate>::new();
        let mut pending_count = 0u64;
        let mut included_count = 0u64;
        let mut failed_count = 0u64;
        let mut submit_rejected_count = 0u64;
        let mut too_little_output_fail_count = 0u64;
        let mut amount_in_above_maximum_fail_count = 0u64;
        let mut source_to_submit_nanos_total = 0u128;
        let mut source_to_submit_count = 0u64;
        let mut source_to_terminal_nanos_total = 0u128;
        let mut source_to_terminal_count = 0u64;

        for item in self
            .trade_order
            .iter()
            .filter_map(|submission_id| self.trades.get(submission_id))
        {
            if let Some(nanos) = item.source_to_submit_nanos {
                source_to_submit_nanos_total += u128::from(nanos);
                source_to_submit_count = source_to_submit_count.saturating_add(1);
            }
            if let Some(nanos) = item.source_to_terminal_nanos {
                source_to_terminal_nanos_total += u128::from(nanos);
                source_to_terminal_count = source_to_terminal_count.saturating_add(1);
            }
            if trade_outcome_is_included(item.outcome.as_str()) {
                included_count = included_count.saturating_add(1);
            } else if trade_outcome_is_failed(item.outcome.as_str()) {
                failed_count = failed_count.saturating_add(1);
                if item.outcome == "submit_rejected" {
                    submit_rejected_count = submit_rejected_count.saturating_add(1);
                }
                match item.failure_custom_code {
                    Some(6_036) | Some(6_022) => {
                        too_little_output_fail_count =
                            too_little_output_fail_count.saturating_add(1);
                    }
                    Some(6_037) => {
                        amount_in_above_maximum_fail_count =
                            amount_in_above_maximum_fail_count.saturating_add(1);
                    }
                    _ => {}
                }
            } else {
                pending_count = pending_count.saturating_add(1);
            }
            by_route
                .entry(item.route_id.clone())
                .or_default()
                .record(item);
        }

        let route_health = self
            .route_health
            .lock()
            .ok()
            .map(|health| {
                health
                    .route_views(self.runtime_status.latest_slot)
                    .into_iter()
                    .map(|view| (view.route_id.clone(), view))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();

        let mut routes = by_route
            .into_iter()
            .map(|(route_id, aggregate)| {
                let route_view = route_health.get(&route_id);
                MonitorEdgeRouteView {
                    route_id,
                    route_kind: aggregate.route_kind,
                    health_state: route_view.map(|view| view.health_state),
                    eligible_live: route_view.map(|view| view.eligible_live),
                    blocking_reason: route_view.and_then(|view| view.blocking_reason.clone()),
                    selected_count: aggregate.selected_count,
                    pending_count: aggregate.pending_count,
                    included_count: aggregate.included_count,
                    failed_count: aggregate.failed_count,
                    submit_rejected_count: aggregate.submit_rejected_count,
                    too_little_output_fail_count: aggregate.too_little_output_fail_count,
                    amount_in_above_maximum_fail_count: aggregate
                        .amount_in_above_maximum_fail_count,
                    expected_pnl_quote_atoms: clamp_i128(aggregate.expected_pnl_quote_atoms),
                    captured_expected_pnl_quote_atoms: clamp_i128(
                        aggregate.captured_expected_pnl_quote_atoms,
                    ),
                    missed_expected_pnl_quote_atoms: clamp_i128(
                        aggregate.missed_expected_pnl_quote_atoms,
                    ),
                    edge_capture_rate_bps: ratio_bps(
                        aggregate.captured_expected_pnl_quote_atoms,
                        aggregate.expected_pnl_quote_atoms,
                    ),
                    realized_pnl_quote_atoms: clamp_i128(aggregate.realized_pnl_quote_atoms),
                    pnl_realization_rate_bps: ratio_bps(
                        aggregate.realized_pnl_quote_atoms,
                        aggregate.expected_pnl_quote_atoms,
                    ),
                    avg_expected_pnl_quote_atoms: average_i128(
                        aggregate.expected_pnl_quote_atoms,
                        aggregate.selected_count,
                    ),
                    avg_realized_pnl_quote_atoms: average_i128(
                        aggregate.realized_pnl_quote_atoms,
                        aggregate.realized_trade_count,
                    ),
                    avg_source_to_submit_nanos: average_u128(
                        aggregate.source_to_submit_nanos_total,
                        aggregate.source_to_submit_count,
                    ),
                    avg_source_to_terminal_nanos: average_u128(
                        aggregate.source_to_terminal_nanos_total,
                        aggregate.source_to_terminal_count,
                    ),
                    last_selected_by: aggregate.last_selected_by,
                    last_p_land_bps: aggregate.last_p_land_bps,
                    last_active_execution_buffer_bps: aggregate.last_active_execution_buffer_bps,
                    last_trade_size: aggregate.last_trade_size,
                    last_quoted_slot: aggregate.last_quoted_slot,
                    last_submitted_slot: aggregate.last_submitted_slot,
                    last_terminal_slot: aggregate.last_terminal_slot,
                    last_updated_at_unix_millis: aggregate.last_updated_at_unix_millis,
                }
            })
            .collect::<Vec<_>>();

        if let Some(filter) = filter {
            let filter = filter.to_ascii_lowercase();
            routes.retain(|item| {
                item.route_id.to_ascii_lowercase().contains(&filter)
                    || item
                        .route_kind
                        .as_ref()
                        .map(|value| value.to_ascii_lowercase().contains(&filter))
                        .unwrap_or(false)
                    || item
                        .blocking_reason
                        .as_ref()
                        .map(|value| value.to_ascii_lowercase().contains(&filter))
                        .unwrap_or(false)
            });
        }

        match sort.unwrap_or("captured") {
            "expected" => routes.sort_by(|left, right| {
                right
                    .expected_pnl_quote_atoms
                    .cmp(&left.expected_pnl_quote_atoms)
                    .then_with(|| right.selected_count.cmp(&left.selected_count))
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
            "realized" => routes.sort_by(|left, right| {
                right
                    .realized_pnl_quote_atoms
                    .cmp(&left.realized_pnl_quote_atoms)
                    .then_with(|| {
                        right
                            .pnl_realization_rate_bps
                            .cmp(&left.pnl_realization_rate_bps)
                    })
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
            "realization" => routes.sort_by(|left, right| {
                right
                    .pnl_realization_rate_bps
                    .cmp(&left.pnl_realization_rate_bps)
                    .then_with(|| {
                        right
                            .realized_pnl_quote_atoms
                            .cmp(&left.realized_pnl_quote_atoms)
                    })
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
            "capture" => routes.sort_by(|left, right| {
                right
                    .edge_capture_rate_bps
                    .cmp(&left.edge_capture_rate_bps)
                    .then_with(|| {
                        right
                            .captured_expected_pnl_quote_atoms
                            .cmp(&left.captured_expected_pnl_quote_atoms)
                    })
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
            "slowest" => routes.sort_by(|left, right| {
                right
                    .avg_source_to_terminal_nanos
                    .unwrap_or(0)
                    .cmp(&left.avg_source_to_terminal_nanos.unwrap_or(0))
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
            "route" => routes.sort_by(|left, right| left.route_id.cmp(&right.route_id)),
            _ => routes.sort_by(|left, right| {
                right
                    .captured_expected_pnl_quote_atoms
                    .cmp(&left.captured_expected_pnl_quote_atoms)
                    .then_with(|| {
                        right
                            .expected_pnl_quote_atoms
                            .cmp(&left.expected_pnl_quote_atoms)
                    })
                    .then_with(|| left.route_id.cmp(&right.route_id))
            }),
        }

        if let Some(limit) = limit {
            routes.truncate(limit);
        }

        MonitorEdgeResponse {
            overview: MonitorEdgeOverview {
                trade_count: self.trade_order.len().min(self.trades.len()) as u64,
                pending_count,
                included_count,
                failed_count,
                submit_rejected_count,
                too_little_output_fail_count,
                amount_in_above_maximum_fail_count,
                expected_session_pnl_quote_atoms: clamp_i128(self.cumulative_expected_pnl),
                captured_expected_session_pnl_quote_atoms: clamp_i128(
                    self.cumulative_captured_expected_pnl,
                ),
                missed_expected_session_pnl_quote_atoms: clamp_i128(
                    self.cumulative_missed_expected_pnl,
                ),
                edge_capture_rate_bps: ratio_bps(
                    self.cumulative_captured_expected_pnl,
                    self.cumulative_expected_pnl,
                ),
                realized_session_pnl_quote_atoms: clamp_i128(self.cumulative_realized_pnl),
                pnl_realization_rate_bps: ratio_bps(
                    self.cumulative_realized_pnl,
                    self.cumulative_expected_pnl,
                ),
                avg_source_to_submit_nanos: average_u128(
                    source_to_submit_nanos_total,
                    source_to_submit_count,
                ),
                avg_source_to_terminal_nanos: average_u128(
                    source_to_terminal_nanos_total,
                    source_to_terminal_count,
                ),
            },
            routes,
        }
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
    Destabilization(DestabilizationEvent),
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

    fn edge(
        &self,
        sort: Option<&str>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> MonitorEdgeResponse {
        self.inner
            .lock()
            .expect("observer lock")
            .edge_response(sort, filter, limit)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObserverHandle {
    sender: Option<SyncSender<ObserverEvent>>,
    drop_count: Option<Arc<AtomicU64>>,
    persistence: PersistenceHandle,
}

impl ObserverHandle {
    pub fn spawn(
        config: &MonitorServerConfig,
        bot_config: &BotConfig,
        route_health: SharedRouteHealth,
    ) -> std::io::Result<Self> {
        let persistence = PersistenceHandle::spawn(&bot_config.runtime.persistence, bot_config)?;
        if !config.enabled {
            return Ok(Self {
                sender: None,
                drop_count: None,
                persistence,
            });
        }

        let pool_metadata = build_pool_metadata(bot_config);
        let state = SharedObserverState {
            inner: Arc::new(Mutex::new(ObserverState::new(
                config.clone(),
                pool_metadata,
                bot_config.state.max_snapshot_slot_lag,
                bot_config.shredstream.price_impact_trigger_bps,
                bot_config.shredstream.price_dislocation_trigger_bps,
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
                        ObserverEvent::Destabilization(event) => guard.apply_destabilization(event),
                    }
                }
            })
            .expect("spawn monitor worker");

        let drop_count = state.drop_count.clone();
        spawn_http_server(config, state)?;
        Ok(Self {
            sender: Some(sender),
            drop_count: Some(drop_count),
            persistence,
        })
    }

    pub fn publish_status(&self, status: RuntimeStatus) {
        self.try_send(ObserverEvent::Status(status));
    }

    pub fn publish_hot_path(&self, report: HotPathReport) {
        if self.sender.is_some() {
            self.try_send(ObserverEvent::HotPath(report.clone()));
        }
        self.persistence.publish_hot_path(report);
    }

    pub fn publish_trade_update(&self, record: ExecutionRecord) {
        if self.sender.is_some() {
            self.try_send(ObserverEvent::TradeUpdate(record.clone()));
        }
        self.persistence.publish_trade_update(record);
    }

    pub(crate) fn publish_repair(&self, event: RepairEvent) {
        self.try_send(ObserverEvent::Repair(event));
    }

    pub(crate) fn publish_destabilization(&self, event: DestabilizationEvent) {
        self.try_send(ObserverEvent::Destabilization(event));
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

    pub(crate) fn flush_persistence(&self, timeout: Duration) -> bool {
        self.persistence.flush(timeout)
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
                    "/monitor/edge" => serde_json::to_vec(&state.edge(
                        query.get("sort").map(String::as_str),
                        query.get("filter").map(String::as_str),
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
        "state_mailbox_age".into(),
        summarize_optional(samples.iter().map(|sample| sample.state_mailbox_age_nanos)),
    );
    metrics.insert(
        "trigger_queue_wait".into(),
        summarize_optional(samples.iter().map(|sample| sample.trigger_queue_wait_nanos)),
    );
    metrics.insert(
        "trigger_barrier_wait".into(),
        summarize_optional(samples.iter().map(|sample| sample.trigger_barrier_wait_nanos)),
    );
    metrics.insert(
        "state_apply".into(),
        summarize_u64(samples.iter().map(|sample| sample.state_apply_nanos)),
    );
    metrics.insert(
        "route_eval".into(),
        summarize_optional(samples.iter().map(|sample| sample.route_eval_nanos)),
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

fn percentile_u16(values: &VecDeque<u16>, percentile: usize) -> Option<u16> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile.min(100)) / 100;
    sorted.get(index).copied()
}

fn combine_latencies(base: Option<u64>, extra: Option<u64>) -> Option<u64> {
    base.zip(extra)
        .map(|(base, extra)| base.saturating_add(extra))
}

fn average_u128(total: u128, count: u64) -> Option<u64> {
    (count > 0).then(|| {
        (total / u128::from(count))
            .min(u128::from(u64::MAX))
            .try_into()
            .unwrap_or(u64::MAX)
    })
}

fn average_i128(total: i128, count: u64) -> Option<i64> {
    (count > 0).then(|| clamp_i128(total / i128::from(count)))
}

fn ratio_bps(numerator: i128, denominator: i128) -> u64 {
    if numerator <= 0 || denominator <= 0 {
        return 0;
    }

    let scaled = numerator
        .saturating_mul(10_000)
        .checked_div(denominator)
        .unwrap_or_default();
    scaled
        .clamp(0, i128::from(u64::MAX))
        .try_into()
        .unwrap_or(u64::MAX)
}

fn trade_outcome_is_terminal(outcome: &str) -> bool {
    outcome != "pending"
}

fn trade_outcome_is_included(outcome: &str) -> bool {
    outcome == "included"
}

fn trade_outcome_is_failed(outcome: &str) -> bool {
    trade_outcome_is_terminal(outcome) && !trade_outcome_is_included(outcome)
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
                "submit_rejected"
                    | "failed"
                    | "chain_too_little_output"
                    | "chain_amount_in_above_maximum"
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

fn event_lane_label(lane: detection::EventLane) -> &'static str {
    match lane {
        detection::EventLane::StateOnly => "state_only",
        detection::EventLane::Trigger => "trigger",
        detection::EventLane::Broadcast => "broadcast",
    }
}

fn submit_status_label(status: submit::SubmitStatus) -> &'static str {
    match status {
        submit::SubmitStatus::Accepted => "accepted",
        submit::SubmitStatus::Rejected => "rejected",
    }
}

fn swap_amount_mode_label(mode: SwapAmountMode) -> &'static str {
    match mode {
        SwapAmountMode::ExactIn => "exact_in",
        SwapAmountMode::ExactOut => "exact_out",
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
        ExecutionOutcome::Failed(FailureClass::ChainExecutionAmountInAboveMaximum) => {
            "chain_amount_in_above_maximum"
        }
        ExecutionOutcome::Failed(_) => "failed",
    }
}

fn failure_bucket(record: &ExecutionRecord) -> Option<String> {
    match &record.outcome {
        ExecutionOutcome::Pending | ExecutionOutcome::Included { .. } => None,
        ExecutionOutcome::Failed(FailureClass::SubmitRejected) => Some("submit_rejected".into()),
        ExecutionOutcome::Failed(FailureClass::TransportFailed) => Some("transport_failed".into()),
        ExecutionOutcome::Failed(FailureClass::ChainDropped) => Some("chain_dropped".into()),
        ExecutionOutcome::Failed(FailureClass::Expired) => Some("expired".into()),
        ExecutionOutcome::Failed(FailureClass::ChainExecutionTooLittleOutput) => {
            Some("amount_out_below_minimum".into())
        }
        ExecutionOutcome::Failed(FailureClass::ChainExecutionAmountInAboveMaximum) => {
            Some("amount_in_above_maximum".into())
        }
        ExecutionOutcome::Failed(FailureClass::ChainExecutionFailed)
        | ExecutionOutcome::Failed(FailureClass::Unknown) => {
            let error_name = record
                .failure_detail
                .as_ref()
                .and_then(|detail| detail.error_name.as_deref());
            match error_name {
                Some("BlockhashNotFound") => Some("blockhash_not_found".into()),
                Some("MinimumContextSlotNotReached" | "MinimumContextSlot") => {
                    Some("minimum_context_slot".into())
                }
                Some("AmountOutBelowMinimum" | "TooLittleOutputReceived") => {
                    Some("amount_out_below_minimum".into())
                }
                Some("AmountInAboveMaximum") => Some("amount_in_above_maximum".into()),
                Some(other) => Some(other.to_ascii_lowercase()),
                None => Some("chain_execution_failed".into()),
            }
        }
    }
}

fn selection_source_label(source: CandidateSelectionSource) -> &'static str {
    match source {
        CandidateSelectionSource::Legacy => "legacy",
        CandidateSelectionSource::Ev => "ev",
    }
}

fn route_kind_label(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::TwoLeg => "two_leg",
        RouteKind::Triangular => "triangular",
    }
}

fn strategy_reason_code(reason: &RejectionReason) -> &'static str {
    match reason {
        RejectionReason::RouteNotRegistered => "route_not_registered",
        RejectionReason::RouteNotWarm { .. } => "route_not_warm",
        RejectionReason::RoutePendingSubmission => "route_pending_submission",
        RejectionReason::MissingSnapshot { .. } => "missing_snapshot",
        RejectionReason::SnapshotStale { .. } => "snapshot_stale",
        RejectionReason::QuoteStaleForExecution { .. } => "quote_stale_for_execution",
        RejectionReason::PoolRepairPending { .. } => "pool_repair_pending",
        RejectionReason::PoolQuarantined { .. } => "pool_quarantined",
        RejectionReason::PoolDisabled { .. } => "pool_disabled",
        RejectionReason::PoolStateNotExecutable { .. } => "pool_state_not_executable",
        RejectionReason::PoolQuoteModelNotExecutable { .. } => "pool_quote_model_not_executable",
        RejectionReason::ReserveUsageTooHigh { .. } => "reserve_usage_too_high",
        RejectionReason::BlockhashUnavailable => "blockhash_unavailable",
        RejectionReason::BlockhashTooStale { .. } => "blockhash_too_stale",
        RejectionReason::ProfitBelowThreshold { .. } => "profit_below_threshold",
        RejectionReason::TradeSizeBelowSizingFloor { .. } => "trade_size_below_sizing_floor",
        RejectionReason::TradeSizeTooLarge { .. } => "trade_size_too_large",
        RejectionReason::SourceBalanceUnavailable { .. } => "source_balance_unavailable",
        RejectionReason::SourceBalanceTooLow { .. } => "source_balance_too_low",
        RejectionReason::WalletNotReady => "wallet_not_ready",
        RejectionReason::WalletBalanceTooLow { .. } => "wallet_balance_too_low",
        RejectionReason::TooManyInflight { .. } => "too_many_inflight",
        RejectionReason::RouteShadowed { .. } => "route_shadowed",
        RejectionReason::KillSwitchActive => "kill_switch_active",
        RejectionReason::QuoteFailed { .. } => "quote_failed",
        RejectionReason::ExecutionCostNotConvertible { .. } => "execution_cost_not_convertible",
        RejectionReason::SizingFloorNotConvertible { .. } => "sizing_floor_not_convertible",
        RejectionReason::NoImpactedRoutes => "no_impacted_routes",
        RejectionReason::RouteFilteredOut { .. } => "route_filtered_out",
    }
}

fn strategy_reason_detail(reason: &RejectionReason) -> String {
    match reason {
        RejectionReason::RouteNotWarm { status } => format!("warmup_status={status:?}"),
        RejectionReason::RoutePendingSubmission => "pending_submission=true".into(),
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
        RejectionReason::PoolQuarantined { pool_id } => format!("pool_id={}", pool_id.0),
        RejectionReason::PoolDisabled { pool_id, detail } => detail
            .as_ref()
            .map(|detail| format!("pool_id={}, detail={detail}", pool_id.0))
            .unwrap_or_else(|| format!("pool_id={}", pool_id.0)),
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
        RejectionReason::TradeSizeBelowSizingFloor { maximum, minimum } => {
            format!("maximum={maximum}, minimum={minimum}")
        }
        RejectionReason::TradeSizeTooLarge { requested, maximum } => {
            format!("requested={requested}, maximum={maximum}")
        }
        RejectionReason::SourceBalanceUnavailable { account } => {
            format!("account={account}")
        }
        RejectionReason::SourceBalanceTooLow {
            account,
            current,
            minimum,
        } => {
            format!("account={account}, current={current}, minimum={minimum}")
        }
        RejectionReason::WalletBalanceTooLow { current, minimum } => {
            format!("current={current}, minimum={minimum}")
        }
        RejectionReason::TooManyInflight { current, maximum } => {
            format!("current={current}, maximum={maximum}")
        }
        RejectionReason::RouteShadowed { until_slot } => until_slot
            .map(|until_slot| format!("until_slot={until_slot}"))
            .unwrap_or_else(|| "until_slot=unknown".into()),
        RejectionReason::QuoteFailed { detail } => detail.clone(),
        RejectionReason::ExecutionCostNotConvertible { detail } => detail.clone(),
        RejectionReason::SizingFloorNotConvertible { detail } => detail.clone(),
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
        BuildRejectionReason::UnprofitableExecutionPlan { .. } => "unprofitable_execution_plan",
        BuildRejectionReason::ComputeUnitLimitTooLow { .. } => "compute_unit_limit_too_low",
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
        BuildRejectionReason::ComputeUnitLimitTooLow { requested, minimum } => {
            format!("requested={requested}, minimum={minimum}")
        }
        BuildRejectionReason::UnprofitableExecutionPlan {
            guaranteed_output,
            minimum_output,
        } => format!("guaranteed_output={guaranteed_output}, minimum_output={minimum_output}"),
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
    use reconciliation::{ExecutionOutcome, ExecutionRecord, FailureClass, InclusionStatus};
    use state::types::{PoolConfidence, PoolId, RouteId};
    use std::{
        collections::BTreeMap,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use strategy::{
        opportunity::{CandidateSelectionSource, OpportunityCandidate, SelectionOutcome},
        quote::LegQuote,
        reasons::RejectionReason,
        route_registry::{RouteKind, SwapSide},
    };
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
            2,
        )))
    }

    fn test_candidate(expected_pnl_quote_atoms: i64) -> OpportunityCandidate {
        OpportunityCandidate {
            route_id: RouteId("route-a".into()),
            route_kind: RouteKind::TwoLeg,
            quoted_slot: 42,
            leg_snapshot_slots: [42, 42].into(),
            sol_quote_conversion_snapshot_slot: None,
            trade_size: 10_000,
            selected_by: CandidateSelectionSource::Legacy,
            ranking_score_quote_atoms: expected_pnl_quote_atoms,
            expected_value_quote_atoms: expected_pnl_quote_atoms,
            p_land_bps: 9_000,
            expected_shortfall_quote_atoms: 0,
            active_execution_buffer_bps: Some(25),
            source_input_balance: None,
            expected_net_output: 10_125,
            minimum_acceptable_output: 10_075,
            expected_gross_profit_quote_atoms: expected_pnl_quote_atoms,
            estimated_network_fee_lamports: 500,
            estimated_network_fee_quote_atoms: 5,
            jito_tip_lamports: 0,
            jito_tip_quote_atoms: 0,
            estimated_execution_cost_lamports: 500,
            estimated_execution_cost_quote_atoms: 5,
            expected_net_profit_quote_atoms: expected_pnl_quote_atoms,
            intermediate_output_amounts: vec![10_080],
            leg_quotes: [
                LegQuote {
                    venue: "orca".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 10_000,
                    output_amount: 10_050,
                    fee_paid: 0,
                    current_tick_index: None,
                    ticks_crossed: 0,
                },
                LegQuote {
                    venue: "raydium".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 10_050,
                    output_amount: 10_125,
                    fee_paid: 0,
                    current_tick_index: None,
                    ticks_crossed: 0,
                },
            ]
            .into(),
        }
    }

    fn test_record(created_at: SystemTime) -> ExecutionRecord {
        ExecutionRecord {
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
            profit_mint: Some("USDC".into()),
            wallet_owner_pubkey: Some("wallet-owner".into()),
            expected_pnl_quote_atoms: Some(125),
            estimated_execution_cost_quote_atoms: Some(5),
            realized_output_delta_quote_atoms: None,
            realized_pnl_quote_atoms: None,
            failure_detail: None,
            submitted_at: created_at,
            terminal_slot: None,
            last_updated_at: created_at,
        }
    }

    fn test_trade_report(record: ExecutionRecord, expected_pnl_quote_atoms: i64) -> HotPathReport {
        HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: Some(test_candidate(expected_pnl_quote_atoms)),
                shadow_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: Some(record),
            submit_leader: None,
            pipeline_trace: PipelineTrace {
                lane: detection::EventLane::Trigger,
                source: EventSourceKind::ShredStream,
                source_sequence: 7,
                observed_slot: 42,
                source_received_at: UNIX_EPOCH.checked_add(Duration::from_millis(1)).unwrap(),
                normalized_at: UNIX_EPOCH.checked_add(Duration::from_millis(2)).unwrap(),
                router_received_at: UNIX_EPOCH.checked_add(Duration::from_millis(2)).unwrap(),
                state_published_at: None,
                source_latency: Some(Duration::from_millis(1)),
                ingest_duration: Duration::from_millis(1),
                queue_wait_duration: Duration::from_millis(2),
                state_mailbox_age_duration: None,
                trigger_queue_wait_duration: None,
                trigger_barrier_wait_duration: None,
                state_apply_duration: Duration::from_micros(4),
                route_eval_duration: None,
                select_duration: Some(Duration::from_millis(1)),
                build_duration: Some(Duration::from_millis(1)),
                sign_duration: Some(Duration::from_millis(1)),
                submit_duration: Some(Duration::from_millis(1)),
                total_to_submit: Some(Duration::from_millis(7)),
            },
        }
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
        assert_eq!(
            strategy_reason_code(&RejectionReason::RoutePendingSubmission),
            "route_pending_submission"
        );
        assert_eq!(
            strategy_reason_code(&RejectionReason::PoolDisabled {
                pool_id: PoolId("pool-a".into()),
                detail: Some("quarantine_limit_exceeded".into()),
            }),
            "pool_disabled"
        );
        assert_eq!(
            strategy_reason_code(&RejectionReason::RouteShadowed {
                until_slot: Some(128),
            }),
            "route_shadowed"
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
            0,
            0,
            test_route_health(),
        );
        state.next_seq = 1;
        state.signals.push_back(super::MonitorSignalSample {
            seq: 1,
            lane: "trigger".into(),
            source: "synthetic".into(),
            source_sequence: 1,
            observed_slot: 1,
            source_received_at_unix_millis: 1,
            normalized_at_unix_millis: 1,
            router_received_at_unix_millis: 1,
            state_published_at_unix_millis: None,
            source_latency_nanos: None,
            ingest_nanos: 1,
            queue_wait_nanos: 1,
            state_mailbox_age_nanos: None,
            trigger_queue_wait_nanos: None,
            trigger_barrier_wait_nanos: None,
            state_apply_nanos: 1,
            route_eval_nanos: None,
            select_nanos: None,
            build_nanos: None,
            sign_nanos: None,
            submit_nanos: None,
            total_to_submit_nanos: None,
        });
        state.signals.push_back(super::MonitorSignalSample {
            seq: 2,
            lane: "trigger".into(),
            source: "synthetic".into(),
            source_sequence: 2,
            observed_slot: 2,
            source_received_at_unix_millis: 2,
            normalized_at_unix_millis: 2,
            router_received_at_unix_millis: 2,
            state_published_at_unix_millis: None,
            source_latency_nanos: None,
            ingest_nanos: 2,
            queue_wait_nanos: 2,
            state_mailbox_age_nanos: None,
            trigger_queue_wait_nanos: None,
            trigger_barrier_wait_nanos: None,
            state_apply_nanos: 2,
            route_eval_nanos: None,
            select_nanos: None,
            build_nanos: None,
            sign_nanos: None,
            submit_nanos: None,
            total_to_submit_nanos: None,
        });
        state.signals.push_back(super::MonitorSignalSample {
            seq: 3,
            lane: "trigger".into(),
            source: "synthetic".into(),
            source_sequence: 3,
            observed_slot: 3,
            source_received_at_unix_millis: 3,
            normalized_at_unix_millis: 3,
            router_received_at_unix_millis: 3,
            state_published_at_unix_millis: None,
            source_latency_nanos: None,
            ingest_nanos: 3,
            queue_wait_nanos: 3,
            state_mailbox_age_nanos: None,
            trigger_queue_wait_nanos: None,
            trigger_barrier_wait_nanos: None,
            state_apply_nanos: 3,
            route_eval_nanos: None,
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
            0,
            0,
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
            profit_mint: Some("USDC".into()),
            wallet_owner_pubkey: Some("wallet-owner".into()),
            expected_pnl_quote_atoms: Some(125),
            estimated_execution_cost_quote_atoms: Some(5),
            realized_output_delta_quote_atoms: None,
            realized_pnl_quote_atoms: None,
            failure_detail: None,
            submitted_at: created_at,
            terminal_slot: None,
            last_updated_at: created_at,
        };
        let report = HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
                shadow_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: Some(record.clone()),
            submit_leader: None,
            pipeline_trace: PipelineTrace {
                lane: detection::EventLane::Trigger,
                source: EventSourceKind::ShredStream,
                source_sequence: 7,
                observed_slot: 42,
                source_received_at: created_at,
                normalized_at: created_at,
                router_received_at: created_at,
                state_published_at: None,
                source_latency: Some(Duration::from_millis(2)),
                ingest_duration: Duration::from_millis(1),
                queue_wait_duration: Duration::from_millis(3),
                state_mailbox_age_duration: None,
                trigger_queue_wait_duration: None,
                trigger_barrier_wait_duration: None,
                state_apply_duration: Duration::from_micros(4),
                route_eval_duration: None,
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
    fn failed_trade_filter_includes_amount_in_above_maximum() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            0,
            0,
            test_route_health(),
        );
        let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
        let report = HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
                shadow_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: Some(ExecutionRecord {
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
                profit_mint: Some("USDC".into()),
                wallet_owner_pubkey: Some("wallet-owner".into()),
                expected_pnl_quote_atoms: Some(90),
                estimated_execution_cost_quote_atoms: Some(5),
                realized_output_delta_quote_atoms: None,
                realized_pnl_quote_atoms: None,
                failure_detail: None,
                submitted_at: created_at,
                terminal_slot: None,
                last_updated_at: created_at,
            }),
            submit_leader: None,
            pipeline_trace: PipelineTrace {
                lane: detection::EventLane::Trigger,
                source: EventSourceKind::ShredStream,
                source_sequence: 7,
                observed_slot: 42,
                source_received_at: created_at,
                normalized_at: created_at,
                router_received_at: created_at,
                state_published_at: None,
                source_latency: Some(Duration::from_millis(2)),
                ingest_duration: Duration::from_millis(1),
                queue_wait_duration: Duration::from_millis(3),
                state_mailbox_age_duration: None,
                trigger_queue_wait_duration: None,
                trigger_barrier_wait_duration: None,
                state_apply_duration: Duration::from_micros(4),
                route_eval_duration: None,
                select_duration: None,
                build_duration: None,
                sign_duration: None,
                submit_duration: None,
                total_to_submit: Some(Duration::from_millis(7)),
            },
        };

        state.apply_hot_path(report);
        state.apply_trade_update(ExecutionRecord {
            route_id: RouteId("route-a".into()),
            submission_id: SubmissionId("submission-a".into()),
            chain_signature: "sig-a".into(),
            submit_mode: SubmitMode::SingleTransaction,
            submit_endpoint: "http://127.0.0.1:3000".into(),
            submit_status: SubmitStatus::Accepted,
            quoted_slot: 42,
            blockhash_slot: Some(43),
            submitted_slot: Some(44),
            inclusion_status: InclusionStatus::Failed(
                reconciliation::FailureClass::ChainExecutionAmountInAboveMaximum,
            ),
            outcome: ExecutionOutcome::Failed(
                reconciliation::FailureClass::ChainExecutionAmountInAboveMaximum,
            ),
            profit_mint: Some("USDC".into()),
            wallet_owner_pubkey: Some("wallet-owner".into()),
            expected_pnl_quote_atoms: Some(90),
            estimated_execution_cost_quote_atoms: Some(5),
            realized_output_delta_quote_atoms: Some(0),
            realized_pnl_quote_atoms: Some(-5),
            failure_detail: None,
            submitted_at: created_at,
            terminal_slot: Some(44),
            last_updated_at: created_at.checked_add(Duration::from_secs(1)).unwrap(),
        });

        let failed = state.trades_response(Some("failed"), None, Some(10));
        assert_eq!(failed.items.len(), 1);
        assert_eq!(failed.items[0].submission_id, "submission-a");
        assert_eq!(failed.items[0].outcome, "chain_amount_in_above_maximum");
    }

    #[test]
    fn apply_status_refreshes_pool_freshness() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            0,
            0,
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
            0,
            0,
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
            0,
            0,
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

    #[test]
    fn included_trade_counts_as_captured_edge() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            0,
            0,
            test_route_health(),
        );
        let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
        let record = test_record(created_at);

        state.apply_hot_path(test_trade_report(record.clone(), 125));
        state.apply_trade_update(ExecutionRecord {
            inclusion_status: InclusionStatus::Landed { slot: 44 },
            outcome: ExecutionOutcome::Included { slot: 44 },
            realized_output_delta_quote_atoms: Some(125),
            realized_pnl_quote_atoms: Some(120),
            last_updated_at: created_at.checked_add(Duration::from_millis(5)).unwrap(),
            ..record
        });

        let overview = state.overview(0);
        assert_eq!(overview.expected_session_pnl_quote_atoms, 125);
        assert_eq!(overview.captured_expected_session_pnl_quote_atoms, 125);
        assert_eq!(overview.missed_expected_session_pnl_quote_atoms, 0);
        assert_eq!(overview.realized_session_pnl_quote_atoms, 120);
        assert_eq!(overview.edge_capture_rate_bps, 10_000);

        let edge = state.edge_response(Some("captured"), None, Some(10));
        assert_eq!(edge.overview.included_count, 1);
        assert_eq!(edge.overview.failed_count, 0);
        assert_eq!(edge.overview.realized_session_pnl_quote_atoms, 120);
        assert_eq!(edge.overview.pnl_realization_rate_bps, 9_600);
        assert_eq!(edge.routes.len(), 1);
        assert_eq!(edge.routes[0].included_count, 1);
        assert_eq!(edge.routes[0].captured_expected_pnl_quote_atoms, 125);
        assert_eq!(edge.routes[0].missed_expected_pnl_quote_atoms, 0);
        assert_eq!(edge.routes[0].realized_pnl_quote_atoms, 120);
        assert_eq!(edge.routes[0].pnl_realization_rate_bps, 9_600);
        assert_eq!(edge.routes[0].avg_realized_pnl_quote_atoms, Some(120));
    }

    #[test]
    fn failed_trade_counts_as_missed_edge() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            0,
            0,
            test_route_health(),
        );
        let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
        let record = test_record(created_at);

        state.apply_hot_path(test_trade_report(record.clone(), 90));
        state.apply_trade_update(ExecutionRecord {
            inclusion_status: InclusionStatus::Failed(FailureClass::ChainExecutionFailed),
            outcome: ExecutionOutcome::Failed(FailureClass::ChainExecutionFailed),
            realized_output_delta_quote_atoms: Some(0),
            realized_pnl_quote_atoms: Some(-5),
            last_updated_at: created_at.checked_add(Duration::from_millis(5)).unwrap(),
            ..record
        });

        let overview = state.overview(0);
        assert_eq!(overview.expected_session_pnl_quote_atoms, 90);
        assert_eq!(overview.captured_expected_session_pnl_quote_atoms, 0);
        assert_eq!(overview.missed_expected_session_pnl_quote_atoms, 90);
        assert_eq!(overview.realized_session_pnl_quote_atoms, -5);
        assert_eq!(overview.edge_capture_rate_bps, 0);

        let edge = state.edge_response(Some("captured"), None, Some(10));
        assert_eq!(edge.overview.included_count, 0);
        assert_eq!(edge.overview.failed_count, 1);
        assert_eq!(edge.overview.realized_session_pnl_quote_atoms, -5);
        assert_eq!(edge.overview.pnl_realization_rate_bps, 0);
        assert_eq!(edge.routes.len(), 1);
        assert_eq!(edge.routes[0].failed_count, 1);
        assert_eq!(edge.routes[0].captured_expected_pnl_quote_atoms, 0);
        assert_eq!(edge.routes[0].missed_expected_pnl_quote_atoms, 90);
        assert_eq!(edge.routes[0].realized_pnl_quote_atoms, -5);
        assert_eq!(edge.routes[0].pnl_realization_rate_bps, 0);
        assert_eq!(edge.routes[0].avg_realized_pnl_quote_atoms, Some(-5));
    }

    #[test]
    fn edge_routes_can_be_sorted_by_realized_pnl() {
        let mut state = ObserverState::new(
            test_config(),
            BTreeMap::<String, PoolStaticMetadata>::new(),
            2,
            0,
            0,
            test_route_health(),
        );
        let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();

        let record_a = test_record(created_at);
        state.apply_hot_path(test_trade_report(record_a.clone(), 100));
        state.apply_trade_update(ExecutionRecord {
            inclusion_status: InclusionStatus::Landed { slot: 44 },
            outcome: ExecutionOutcome::Included { slot: 44 },
            realized_output_delta_quote_atoms: Some(55),
            realized_pnl_quote_atoms: Some(50),
            last_updated_at: created_at.checked_add(Duration::from_millis(5)).unwrap(),
            ..record_a
        });

        let mut record_b = test_record(created_at.checked_add(Duration::from_secs(1)).unwrap());
        record_b.route_id = RouteId("route-b".into());
        record_b.submission_id = SubmissionId("submission-b".into());
        record_b.chain_signature = "sig-b".into();
        let mut report_b = test_trade_report(record_b.clone(), 80);
        report_b
            .selection
            .best_candidate
            .as_mut()
            .expect("candidate")
            .route_id = RouteId("route-b".into());
        state.apply_hot_path(report_b);
        state.apply_trade_update(ExecutionRecord {
            inclusion_status: InclusionStatus::Landed { slot: 45 },
            outcome: ExecutionOutcome::Included { slot: 45 },
            realized_output_delta_quote_atoms: Some(75),
            realized_pnl_quote_atoms: Some(70),
            last_updated_at: created_at.checked_add(Duration::from_millis(10)).unwrap(),
            ..record_b
        });

        let edge = state.edge_response(Some("realized"), None, Some(10));
        assert_eq!(edge.routes.len(), 2);
        assert_eq!(edge.routes[0].route_id, "route-b");
        assert_eq!(edge.routes[0].realized_pnl_quote_atoms, 70);
        assert_eq!(edge.routes[0].pnl_realization_rate_bps, 8_750);
        assert_eq!(edge.routes[1].route_id, "route-a");
        assert_eq!(edge.routes[1].realized_pnl_quote_atoms, 50);
        assert_eq!(edge.routes[1].pnl_realization_rate_bps, 5_000);
    }
}
