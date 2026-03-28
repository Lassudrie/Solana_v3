use std::{
    collections::HashMap,
    fs,
    path::Path,
    process,
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use builder::{BuildRejectionReason, SwapAmountMode};
use reconciliation::{ExecutionOutcome, ExecutionRecord, FailureClass};
use rusqlite::{Connection, Transaction, params};
use serde::Serialize;
use strategy::{
    opportunity::{CandidateSelectionSource, OpportunityDecision},
    reasons::RejectionReason,
    route_registry::RouteKind,
};
use submit::{SubmitRejectionReason, SubmitStatus};

use crate::{
    config::{
        BotConfig, EventSourceMode, PersistenceConfig, RuntimeProfileConfig, SubmitModeConfig,
    },
    observer::{MonitorTradeEvent, RejectionEvent},
    runtime::{HotPathReport, PipelineTrace},
};

type PersistenceResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
enum PersistenceMessage {
    HotPath(HotPathReport),
    TradeUpdate(ExecutionRecord),
    Flush(SyncSender<()>),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PersistenceHandle {
    sender: Option<Sender<PersistenceMessage>>,
}

impl PersistenceHandle {
    pub(crate) fn spawn(
        config: &PersistenceConfig,
        bot_config: &BotConfig,
    ) -> std::io::Result<Self> {
        if !config.enabled {
            return Ok(Self::default());
        }

        if let Some(parent) = Path::new(&config.path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let (sender, receiver) = mpsc::channel();
        let config = config.clone();
        let run_metadata = RunMetadata::from_config(bot_config, &config.path);
        thread::Builder::new()
            .name("bot-persistence-worker".into())
            .spawn(move || run_persistence_worker(config, run_metadata, receiver))?;

        Ok(Self {
            sender: Some(sender),
        })
    }

    pub(crate) fn publish_hot_path(&self, report: HotPathReport) {
        self.send(PersistenceMessage::HotPath(report));
    }

    pub(crate) fn publish_trade_update(&self, record: ExecutionRecord) {
        self.send(PersistenceMessage::TradeUpdate(record));
    }

    pub(crate) fn flush(&self, timeout: Duration) -> bool {
        let Some(sender) = &self.sender else {
            return true;
        };

        let (ack_tx, ack_rx) = mpsc::sync_channel(0);
        if sender.send(PersistenceMessage::Flush(ack_tx)).is_err() {
            return false;
        }
        ack_rx.recv_timeout(timeout).is_ok()
    }

    fn send(&self, message: PersistenceMessage) {
        let Some(sender) = &self.sender else {
            return;
        };
        let _ = sender.send(message);
    }
}

#[derive(Debug, Clone)]
struct RunMetadata {
    run_id: String,
    started_at_unix_millis: u128,
    pid: u32,
    runtime_profile: String,
    event_source_mode: String,
    submit_mode: String,
    monitor_enabled: bool,
    persistence_path: String,
}

impl RunMetadata {
    fn from_config(config: &BotConfig, persistence_path: &str) -> Self {
        let started_at_unix_millis = unix_millis(SystemTime::now());
        Self {
            run_id: format!("{}-{}", started_at_unix_millis, process::id(),),
            started_at_unix_millis,
            pid: process::id(),
            runtime_profile: runtime_profile_label(config.runtime.profile).into(),
            event_source_mode: event_source_mode_label(config.runtime.event_source.mode.clone())
                .into(),
            submit_mode: submit_mode_label(config.submit.mode.clone()).into(),
            monitor_enabled: config.runtime.monitor_server.enabled,
            persistence_path: persistence_path.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PersistedPipelineTrace {
    source: String,
    source_sequence: u64,
    observed_slot: u64,
    source_received_at_unix_millis: u128,
    normalized_at_unix_millis: u128,
    source_latency_nanos: Option<u64>,
    ingest_nanos: u64,
    queue_wait_nanos: u64,
    state_apply_nanos: u64,
    select_nanos: Option<u64>,
    build_nanos: Option<u64>,
    sign_nanos: Option<u64>,
    submit_nanos: Option<u64>,
    total_to_submit_nanos: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct PersistedCandidateContext {
    route_id: String,
    route_kind: String,
    leg_count: usize,
    trade_size: u64,
    selected_by: String,
    ranking_score_quote_atoms: i64,
    expected_value_quote_atoms: i64,
    p_land_bps: u16,
    expected_shortfall_quote_atoms: u64,
    active_execution_buffer_bps: Option<u16>,
    expected_edge_quote_atoms: i64,
    estimated_execution_cost_quote_atoms: u64,
    expected_pnl_quote_atoms: i64,
    intermediate_output_amounts: Vec<u64>,
    leg_snapshot_slots: Vec<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct PersistedRejectionPayload {
    rejection: RejectionEvent,
    pipeline: PersistedPipelineTrace,
    candidate: Option<PersistedCandidateContext>,
}

#[derive(Debug, Clone, Serialize)]
struct PersistedTradePayload {
    trade: MonitorTradeEvent,
    pipeline: Option<PersistedPipelineTrace>,
    submit_leader: Option<String>,
}

fn run_persistence_worker(
    config: PersistenceConfig,
    run_metadata: RunMetadata,
    receiver: Receiver<PersistenceMessage>,
) {
    if let Err(error) = run_persistence_worker_inner(config, run_metadata, receiver) {
        eprintln!("bot persistence worker stopped: {error}");
    }
}

fn run_persistence_worker_inner(
    config: PersistenceConfig,
    run_metadata: RunMetadata,
    receiver: Receiver<PersistenceMessage>,
) -> PersistenceResult<()> {
    let mut connection = Connection::open(&config.path)?;
    configure_connection(&connection)?;
    create_schema(&connection)?;
    insert_run(&connection, &run_metadata)?;

    let batch_size = config.batch_size.max(1);
    let flush_interval = Duration::from_millis(config.flush_interval_millis);
    let mut trade_cache = HashMap::new();
    let mut next_event_seq = 1u64;

    loop {
        let first = match receiver.recv() {
            Ok(message) => message,
            Err(_) => break,
        };

        let mut batch = vec![first];
        let batch_started = Instant::now();
        let mut disconnected = false;
        while batch.len() < batch_size {
            let Some(remaining) = flush_interval.checked_sub(batch_started.elapsed()) else {
                break;
            };
            match receiver.recv_timeout(remaining) {
                Ok(message) => batch.push(message),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        persist_batch(
            &mut connection,
            &run_metadata,
            &mut trade_cache,
            &mut next_event_seq,
            batch,
        )?;

        if disconnected {
            break;
        }
    }

    Ok(())
}

fn configure_connection(connection: &Connection) -> PersistenceResult<()> {
    connection.busy_timeout(Duration::from_secs(1))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

fn create_schema(connection: &Connection) -> PersistenceResult<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS bot_runs (
            run_id TEXT PRIMARY KEY,
            started_at_unix_millis INTEGER NOT NULL,
            pid INTEGER NOT NULL,
            runtime_profile TEXT NOT NULL,
            event_source_mode TEXT NOT NULL,
            submit_mode TEXT NOT NULL,
            monitor_enabled INTEGER NOT NULL,
            persistence_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rejection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_seq INTEGER NOT NULL UNIQUE,
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            route_id TEXT NOT NULL,
            route_kind TEXT,
            leg_count INTEGER NOT NULL,
            reason_code TEXT NOT NULL,
            reason_detail TEXT NOT NULL,
            observed_slot INTEGER NOT NULL,
            expected_profit_quote_atoms INTEGER,
            occurred_at_unix_millis INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS rejection_events_stage_time_idx
            ON rejection_events(stage, occurred_at_unix_millis);
        CREATE INDEX IF NOT EXISTS rejection_events_route_time_idx
            ON rejection_events(route_id, occurred_at_unix_millis);
        CREATE INDEX IF NOT EXISTS rejection_events_reason_time_idx
            ON rejection_events(reason_code, occurred_at_unix_millis);

        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_seq INTEGER NOT NULL UNIQUE,
            run_id TEXT NOT NULL,
            submission_id TEXT NOT NULL,
            audit_kind TEXT NOT NULL,
            route_id TEXT NOT NULL,
            route_kind TEXT NOT NULL,
            signature TEXT NOT NULL,
            submit_status TEXT NOT NULL,
            outcome TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            quoted_slot INTEGER NOT NULL,
            submitted_slot INTEGER,
            terminal_slot INTEGER,
            expected_pnl_quote_atoms INTEGER,
            realized_pnl_quote_atoms INTEGER,
            reported_at_unix_millis INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS trade_events_submission_seq_idx
            ON trade_events(submission_id, event_seq);
        CREATE INDEX IF NOT EXISTS trade_events_route_time_idx
            ON trade_events(route_id, reported_at_unix_millis);
        CREATE INDEX IF NOT EXISTS trade_events_outcome_time_idx
            ON trade_events(outcome, reported_at_unix_millis);

        CREATE TABLE IF NOT EXISTS trade_latest (
            submission_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            last_event_seq INTEGER NOT NULL,
            route_id TEXT NOT NULL,
            route_kind TEXT NOT NULL,
            signature TEXT NOT NULL,
            submit_status TEXT NOT NULL,
            outcome TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            quoted_slot INTEGER NOT NULL,
            submitted_slot INTEGER,
            terminal_slot INTEGER,
            expected_pnl_quote_atoms INTEGER,
            realized_pnl_quote_atoms INTEGER,
            source_to_submit_nanos INTEGER,
            source_to_terminal_nanos INTEGER,
            updated_at_unix_millis INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS trade_latest_route_idx
            ON trade_latest(route_id);
        CREATE INDEX IF NOT EXISTS trade_latest_outcome_idx
            ON trade_latest(outcome);
        CREATE INDEX IF NOT EXISTS trade_latest_updated_idx
            ON trade_latest(updated_at_unix_millis);
        ",
    )?;
    Ok(())
}

fn insert_run(connection: &Connection, run_metadata: &RunMetadata) -> PersistenceResult<()> {
    connection.execute(
        "
        INSERT OR REPLACE INTO bot_runs (
            run_id,
            started_at_unix_millis,
            pid,
            runtime_profile,
            event_source_mode,
            submit_mode,
            monitor_enabled,
            persistence_path
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ",
        params![
            run_metadata.run_id,
            sql_i64_u128(run_metadata.started_at_unix_millis),
            i64::from(run_metadata.pid),
            run_metadata.runtime_profile,
            run_metadata.event_source_mode,
            run_metadata.submit_mode,
            if run_metadata.monitor_enabled { 1 } else { 0 },
            run_metadata.persistence_path,
        ],
    )?;
    Ok(())
}

fn persist_batch(
    connection: &mut Connection,
    run_metadata: &RunMetadata,
    trade_cache: &mut HashMap<String, MonitorTradeEvent>,
    next_event_seq: &mut u64,
    batch: Vec<PersistenceMessage>,
) -> PersistenceResult<()> {
    let transaction = connection.transaction()?;
    let mut flush_acks = Vec::new();

    for message in batch {
        match message {
            PersistenceMessage::HotPath(report) => {
                if let Err(error) = persist_hot_path(
                    &transaction,
                    run_metadata,
                    trade_cache,
                    next_event_seq,
                    report,
                ) {
                    eprintln!("bot persistence hot path write failed: {error}");
                }
            }
            PersistenceMessage::TradeUpdate(record) => {
                if let Err(error) = persist_trade_update(
                    &transaction,
                    run_metadata,
                    trade_cache,
                    next_event_seq,
                    record,
                ) {
                    eprintln!("bot persistence trade update write failed: {error}");
                }
            }
            PersistenceMessage::Flush(ack) => flush_acks.push(ack),
        }
    }

    transaction.commit()?;
    for ack in flush_acks {
        let _ = ack.send(());
    }
    Ok(())
}

fn persist_hot_path(
    transaction: &Transaction<'_>,
    run_metadata: &RunMetadata,
    trade_cache: &mut HashMap<String, MonitorTradeEvent>,
    next_event_seq: &mut u64,
    report: HotPathReport,
) -> PersistenceResult<()> {
    let pipeline = persisted_pipeline_trace(&report.pipeline_trace);
    let candidate_context = persisted_candidate_context(&report);
    let occurred_at_unix_millis = unix_millis(SystemTime::now());

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
        let event = RejectionEvent {
            seq: next_seq(next_event_seq),
            stage: "strategy".into(),
            route_id: route_id.0.clone(),
            route_kind: route_kind.map(|kind| route_kind_label(kind).into()),
            leg_count: *leg_count,
            reason_code: strategy_reason_code(reason).into(),
            reason_detail: strategy_reason_detail(reason),
            observed_slot: report.pipeline_trace.observed_slot,
            expected_profit_quote_atoms: strategy_expected_profit(reason),
            occurred_at_unix_millis,
        };
        insert_rejection_event(
            transaction,
            run_metadata,
            &event,
            &pipeline,
            candidate_context.as_ref(),
        )?;
    }

    if let (Some(build_result), Some(candidate)) = (
        &report.build_result,
        report.selection.best_candidate.as_ref(),
    ) {
        if let Some(reason) = &build_result.rejection {
            let event = RejectionEvent {
                seq: next_seq(next_event_seq),
                stage: "build".into(),
                route_id: candidate.route_id.0.clone(),
                route_kind: Some(route_kind_label(candidate.route_kind).into()),
                leg_count: candidate.leg_count(),
                reason_code: build_reason_code(reason).into(),
                reason_detail: build_reason_detail(reason),
                observed_slot: report.pipeline_trace.observed_slot,
                expected_profit_quote_atoms: Some(candidate.expected_net_profit_quote_atoms),
                occurred_at_unix_millis,
            };
            insert_rejection_event(
                transaction,
                run_metadata,
                &event,
                &pipeline,
                candidate_context.as_ref(),
            )?;
        }
    }

    if let Some(submit_result) = &report.submit_result {
        if let Some(reason) = &submit_result.rejection {
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
            let event = RejectionEvent {
                seq: next_seq(next_event_seq),
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
                occurred_at_unix_millis,
            };
            insert_rejection_event(
                transaction,
                run_metadata,
                &event,
                &pipeline,
                candidate_context.as_ref(),
            )?;
        }
    }

    let event_seq = next_seq(next_event_seq);
    if let Some(trade) = trade_from_report(event_seq, &report) {
        upsert_trade(
            transaction,
            run_metadata,
            trade_cache,
            trade,
            event_seq,
            "submission",
            Some(pipeline),
            report.submit_leader,
        )?;
    }

    Ok(())
}

fn persist_trade_update(
    transaction: &Transaction<'_>,
    run_metadata: &RunMetadata,
    trade_cache: &mut HashMap<String, MonitorTradeEvent>,
    next_event_seq: &mut u64,
    record: ExecutionRecord,
) -> PersistenceResult<()> {
    let submission_id = record.submission_id.0.clone();
    let event_seq = next_seq(next_event_seq);
    let mut trade = trade_cache
        .get(&submission_id)
        .cloned()
        .unwrap_or_else(|| fallback_trade_from_record(event_seq, &record));
    apply_trade_update(&mut trade, &record);
    upsert_trade(
        transaction,
        run_metadata,
        trade_cache,
        trade,
        event_seq,
        "transition",
        None,
        None,
    )?;
    Ok(())
}

fn insert_rejection_event(
    transaction: &Transaction<'_>,
    run_metadata: &RunMetadata,
    rejection: &RejectionEvent,
    pipeline: &PersistedPipelineTrace,
    candidate: Option<&PersistedCandidateContext>,
) -> PersistenceResult<()> {
    let payload = PersistedRejectionPayload {
        rejection: rejection.clone(),
        pipeline: pipeline.clone(),
        candidate: candidate.cloned(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    transaction.execute(
        "
        INSERT INTO rejection_events (
            event_seq,
            run_id,
            stage,
            route_id,
            route_kind,
            leg_count,
            reason_code,
            reason_detail,
            observed_slot,
            expected_profit_quote_atoms,
            occurred_at_unix_millis,
            payload_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ",
        params![
            sql_i64_u64(rejection.seq),
            run_metadata.run_id,
            rejection.stage,
            rejection.route_id,
            rejection.route_kind,
            sql_i64_usize(rejection.leg_count),
            rejection.reason_code,
            rejection.reason_detail,
            sql_i64_u64(rejection.observed_slot),
            rejection.expected_profit_quote_atoms,
            sql_i64_u128(rejection.occurred_at_unix_millis),
            payload_json,
        ],
    )?;
    Ok(())
}

fn upsert_trade(
    transaction: &Transaction<'_>,
    run_metadata: &RunMetadata,
    trade_cache: &mut HashMap<String, MonitorTradeEvent>,
    trade: MonitorTradeEvent,
    event_seq: u64,
    audit_kind: &str,
    pipeline: Option<PersistedPipelineTrace>,
    submit_leader: Option<String>,
) -> PersistenceResult<()> {
    let payload = PersistedTradePayload {
        trade: trade.clone(),
        pipeline,
        submit_leader,
    };
    let payload_json = serde_json::to_string(&payload)?;
    transaction.execute(
        "
        INSERT INTO trade_events (
            event_seq,
            run_id,
            submission_id,
            audit_kind,
            route_id,
            route_kind,
            signature,
            submit_status,
            outcome,
            endpoint,
            quoted_slot,
            submitted_slot,
            terminal_slot,
            expected_pnl_quote_atoms,
            realized_pnl_quote_atoms,
            reported_at_unix_millis,
            payload_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
        ",
        params![
            sql_i64_u64(event_seq),
            run_metadata.run_id,
            trade.submission_id,
            audit_kind,
            trade.route_id,
            trade.route_kind,
            trade.signature,
            trade.submit_status,
            trade.outcome,
            trade.endpoint,
            sql_i64_u64(trade.quoted_slot),
            trade.submitted_slot.map(sql_i64_u64),
            trade.terminal_slot.map(sql_i64_u64),
            trade.expected_pnl_quote_atoms,
            trade.realized_pnl_quote_atoms,
            sql_i64_u128(trade.updated_at_unix_millis),
            payload_json.clone(),
        ],
    )?;
    transaction.execute(
        "
        INSERT INTO trade_latest (
            submission_id,
            run_id,
            last_event_seq,
            route_id,
            route_kind,
            signature,
            submit_status,
            outcome,
            endpoint,
            quoted_slot,
            submitted_slot,
            terminal_slot,
            expected_pnl_quote_atoms,
            realized_pnl_quote_atoms,
            source_to_submit_nanos,
            source_to_terminal_nanos,
            updated_at_unix_millis,
            payload_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        ON CONFLICT(submission_id) DO UPDATE SET
            run_id = excluded.run_id,
            last_event_seq = excluded.last_event_seq,
            route_id = excluded.route_id,
            route_kind = excluded.route_kind,
            signature = excluded.signature,
            submit_status = excluded.submit_status,
            outcome = excluded.outcome,
            endpoint = excluded.endpoint,
            quoted_slot = excluded.quoted_slot,
            submitted_slot = excluded.submitted_slot,
            terminal_slot = excluded.terminal_slot,
            expected_pnl_quote_atoms = excluded.expected_pnl_quote_atoms,
            realized_pnl_quote_atoms = excluded.realized_pnl_quote_atoms,
            source_to_submit_nanos = excluded.source_to_submit_nanos,
            source_to_terminal_nanos = excluded.source_to_terminal_nanos,
            updated_at_unix_millis = excluded.updated_at_unix_millis,
            payload_json = excluded.payload_json
        ",
        params![
            trade.submission_id,
            run_metadata.run_id,
            sql_i64_u64(event_seq),
            trade.route_id,
            trade.route_kind,
            trade.signature,
            trade.submit_status,
            trade.outcome,
            trade.endpoint,
            sql_i64_u64(trade.quoted_slot),
            trade.submitted_slot.map(sql_i64_u64),
            trade.terminal_slot.map(sql_i64_u64),
            trade.expected_pnl_quote_atoms,
            trade.realized_pnl_quote_atoms,
            trade.source_to_submit_nanos.map(sql_i64_u64),
            trade.source_to_terminal_nanos.map(sql_i64_u64),
            sql_i64_u128(trade.updated_at_unix_millis),
            payload_json,
        ],
    )?;
    trade_cache.insert(trade.submission_id.clone(), trade);
    Ok(())
}

fn persisted_pipeline_trace(trace: &PipelineTrace) -> PersistedPipelineTrace {
    PersistedPipelineTrace {
        source: source_label(&trace.source).into(),
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
    }
}

fn persisted_candidate_context(report: &HotPathReport) -> Option<PersistedCandidateContext> {
    let candidate = report.selection.best_candidate.as_ref()?;
    Some(PersistedCandidateContext {
        route_id: candidate.route_id.0.clone(),
        route_kind: route_kind_label(candidate.route_kind).into(),
        leg_count: candidate.leg_count(),
        trade_size: candidate.trade_size,
        selected_by: selection_source_label(candidate.selected_by).into(),
        ranking_score_quote_atoms: candidate.ranking_score_quote_atoms,
        expected_value_quote_atoms: candidate.expected_value_quote_atoms,
        p_land_bps: candidate.p_land_bps,
        expected_shortfall_quote_atoms: candidate.expected_shortfall_quote_atoms,
        active_execution_buffer_bps: candidate.active_execution_buffer_bps,
        expected_edge_quote_atoms: candidate.expected_gross_profit_quote_atoms,
        estimated_execution_cost_quote_atoms: candidate.estimated_execution_cost_quote_atoms,
        expected_pnl_quote_atoms: candidate.expected_net_profit_quote_atoms,
        intermediate_output_amounts: candidate.intermediate_output_amounts.clone(),
        leg_snapshot_slots: candidate.leg_snapshot_slots.as_slice().to_vec(),
    })
}

fn trade_from_report(seq: u64, report: &HotPathReport) -> Option<MonitorTradeEvent> {
    let record = report.execution_record.as_ref()?;
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

    Some(MonitorTradeEvent {
        seq,
        route_id: record.route_id.0.clone(),
        route_kind: best_candidate
            .map(|candidate| route_kind_label(candidate.route_kind).into())
            .unwrap_or_else(|| "unknown".into()),
        leg_count: best_candidate
            .map(|candidate| candidate.leg_count())
            .unwrap_or_default(),
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
        oldest_snapshot_slot: best_candidate
            .map(|candidate| candidate.oldest_relevant_snapshot_slot()),
        quote_state_age_slots: best_candidate.map(|candidate| {
            candidate
                .quoted_slot
                .saturating_sub(candidate.oldest_relevant_snapshot_slot())
        }),
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
    })
}

fn fallback_trade_from_record(seq: u64, record: &ExecutionRecord) -> MonitorTradeEvent {
    let submit_to_terminal_nanos = if matches!(record.outcome, ExecutionOutcome::Pending) {
        None
    } else {
        duration_nanos(
            record
                .last_updated_at
                .duration_since(record.submitted_at)
                .unwrap_or(Duration::ZERO),
        )
    };
    MonitorTradeEvent {
        seq,
        route_id: record.route_id.0.clone(),
        route_kind: "unknown".into(),
        leg_count: 0,
        trade_size: None,
        submission_id: record.submission_id.0.clone(),
        signature: record.chain_signature.clone(),
        submit_status: submit_status_label(record.submit_status).into(),
        outcome: outcome_label(&record.outcome).into(),
        selected_by: "unknown".into(),
        ranking_score_quote_atoms: None,
        expected_edge_quote_atoms: 0,
        estimated_cost_lamports: None,
        estimated_cost_quote_atoms: record.estimated_execution_cost_quote_atoms,
        expected_pnl_quote_atoms: record.expected_pnl_quote_atoms,
        expected_value_quote_atoms: None,
        p_land_bps: None,
        expected_shortfall_quote_atoms: None,
        intermediate_output_amounts: Vec::new(),
        leg_snapshot_slots: Vec::new(),
        shadow_selected_by: None,
        shadow_trade_size: None,
        shadow_ranking_score_quote_atoms: None,
        shadow_expected_value_quote_atoms: None,
        shadow_expected_pnl_quote_atoms: None,
        realized_pnl_quote_atoms: record.realized_pnl_quote_atoms,
        jito_tip_lamports: None,
        active_execution_buffer_bps: None,
        source_input_balance: None,
        oldest_snapshot_slot: None,
        quote_state_age_slots: None,
        leg_ticks_crossed: Vec::new(),
        leg_amount_modes: Vec::new(),
        leg_specified_amounts: Vec::new(),
        leg_threshold_amounts: Vec::new(),
        submit_leader: None,
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
        source_to_submit_nanos: None,
        submit_to_terminal_nanos,
        source_to_terminal_nanos: submit_to_terminal_nanos,
    }
}

fn apply_trade_update(existing: &mut MonitorTradeEvent, record: &ExecutionRecord) {
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
    existing.submit_status = submit_status_label(record.submit_status).into();
}

fn next_seq(next_event_seq: &mut u64) -> u64 {
    let value = *next_event_seq;
    *next_event_seq = value.saturating_add(1);
    value
}

fn runtime_profile_label(profile: RuntimeProfileConfig) -> &'static str {
    match profile {
        RuntimeProfileConfig::Default => "default",
        RuntimeProfileConfig::UltraFast => "ultra_fast",
    }
}

fn event_source_mode_label(mode: EventSourceMode) -> &'static str {
    match mode {
        EventSourceMode::Disabled => "disabled",
        EventSourceMode::JsonlFile => "jsonl_file",
        EventSourceMode::Shredstream => "shredstream",
        EventSourceMode::UdpJson => "udp_json",
    }
}

fn submit_mode_label(mode: SubmitModeConfig) -> &'static str {
    match mode {
        SubmitModeConfig::SingleTransaction => "single_transaction",
        SubmitModeConfig::Bundle => "bundle",
    }
}

fn source_label(source: &detection::EventSourceKind) -> &'static str {
    match source {
        detection::EventSourceKind::ShredStream => "shredstream",
        detection::EventSourceKind::Replay => "replay",
        detection::EventSourceKind::Synthetic => "synthetic",
    }
}

fn route_kind_label(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::TwoLeg => "two_leg",
        RouteKind::Triangular => "triangular",
    }
}

fn selection_source_label(source: CandidateSelectionSource) -> &'static str {
    match source {
        CandidateSelectionSource::Legacy => "legacy",
        CandidateSelectionSource::Ev => "ev",
    }
}

fn submit_status_label(status: SubmitStatus) -> &'static str {
    match status {
        SubmitStatus::Accepted => "accepted",
        SubmitStatus::Rejected => "rejected",
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

fn combine_latencies(base: Option<u64>, extra: Option<u64>) -> Option<u64> {
    base.zip(extra)
        .map(|(base, extra)| base.saturating_add(extra))
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

fn sql_i64_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn sql_i64_u128(value: u128) -> i64 {
    value.min(i64::MAX as u128) as i64
}

fn sql_i64_usize(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use builder::BuildRejectionReason;
    use detection::EventSourceKind;
    use domain::{PoolId, RouteId};
    use reconciliation::{ExecutionOutcome, ExecutionRecord, FailureClass, InclusionStatus};
    use rusqlite::{Connection, params};
    use strategy::{
        opportunity::{CandidateSelectionSource, OpportunityCandidate, SelectionOutcome},
        quote::LegQuote,
        route_registry::{RouteKind, SwapSide},
    };
    use submit::{SubmissionId, SubmitMode, SubmitResult, SubmitStatus};

    use crate::{
        config::{BotConfig, EventSourceMode},
        runtime::{HotPathReport, PipelineTrace},
    };

    use super::{
        PersistenceHandle, fallback_trade_from_record, submit_status_label, trade_from_report,
    };

    #[test]
    fn persistence_writes_rejections_and_trade_history() {
        let path = temp_path("bot-persistence", "sqlite3");
        let mut config = BotConfig::default();
        config.runtime.event_source.mode = EventSourceMode::JsonlFile;
        config.runtime.persistence.enabled = true;
        config.runtime.persistence.path = path.to_string_lossy().into_owned();
        config.runtime.persistence.flush_interval_millis = 10;

        let handle = PersistenceHandle::spawn(&config.runtime.persistence, &config).unwrap();

        let report = rejected_submission_report();
        let submission_id = report
            .execution_record
            .as_ref()
            .expect("execution record")
            .submission_id
            .0
            .clone();
        handle.publish_hot_path(report.clone());

        let mut updated_record = report.execution_record.expect("execution record");
        updated_record.inclusion_status =
            InclusionStatus::Failed(FailureClass::ChainExecutionTooLittleOutput);
        updated_record.outcome =
            ExecutionOutcome::Failed(FailureClass::ChainExecutionTooLittleOutput);
        updated_record.terminal_slot = Some(55);
        updated_record.realized_pnl_quote_atoms = Some(-12);
        updated_record.last_updated_at = UNIX_EPOCH + Duration::from_secs(2);
        handle.publish_trade_update(updated_record);

        assert!(handle.flush(Duration::from_secs(2)));

        let connection = Connection::open(&path).unwrap();
        let rejection_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM rejection_events", [], |row| {
                row.get(0)
            })
            .unwrap();
        let trade_event_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM trade_events", [], |row| row.get(0))
            .unwrap();
        let latest_outcome: String = connection
            .query_row(
                "SELECT outcome FROM trade_latest WHERE submission_id = ?1",
                params![submission_id],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(rejection_count, 2);
        assert_eq!(trade_event_count, 2);
        assert_eq!(latest_outcome, "chain_too_little_output");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn fallback_trade_preserves_submit_status() {
        let record = ExecutionRecord {
            route_id: RouteId("route-a".into()),
            submission_id: SubmissionId("submission-a".into()),
            chain_signature: "sig-a".into(),
            submit_mode: SubmitMode::SingleTransaction,
            submit_endpoint: "jito".into(),
            submit_status: SubmitStatus::Rejected,
            quoted_slot: 42,
            blockhash_slot: Some(43),
            submitted_slot: Some(44),
            terminal_slot: None,
            inclusion_status: InclusionStatus::Failed(FailureClass::SubmitRejected),
            outcome: ExecutionOutcome::Failed(FailureClass::SubmitRejected),
            profit_mint: None,
            wallet_owner_pubkey: None,
            expected_pnl_quote_atoms: Some(5),
            estimated_execution_cost_quote_atoms: Some(1),
            realized_output_delta_quote_atoms: Some(0),
            realized_pnl_quote_atoms: Some(0),
            failure_detail: None,
            submitted_at: UNIX_EPOCH,
            last_updated_at: UNIX_EPOCH,
        };

        let trade = fallback_trade_from_record(7, &record);

        assert_eq!(trade.seq, 7);
        assert_eq!(
            trade.submit_status,
            submit_status_label(SubmitStatus::Rejected)
        );
        assert_eq!(trade.outcome, "submit_rejected");
    }

    fn rejected_submission_report() -> HotPathReport {
        let submission_result = SubmitResult {
            status: SubmitStatus::Rejected,
            submission_id: SubmissionId("submission-1".into()),
            endpoint: "jito".into(),
            rejection: Some(submit::SubmitRejectionReason::RemoteRejected),
        };
        let execution_record = ExecutionRecord {
            route_id: RouteId("route-a".into()),
            submission_id: submission_result.submission_id.clone(),
            chain_signature: "sig-1".into(),
            submit_mode: SubmitMode::SingleTransaction,
            submit_endpoint: submission_result.endpoint.clone(),
            submit_status: submission_result.status,
            quoted_slot: 44,
            blockhash_slot: Some(45),
            submitted_slot: Some(46),
            terminal_slot: None,
            inclusion_status: InclusionStatus::Failed(FailureClass::SubmitRejected),
            outcome: ExecutionOutcome::Failed(FailureClass::SubmitRejected),
            profit_mint: Some("mint".into()),
            wallet_owner_pubkey: Some("wallet".into()),
            expected_pnl_quote_atoms: Some(25),
            estimated_execution_cost_quote_atoms: Some(5),
            realized_output_delta_quote_atoms: Some(0),
            realized_pnl_quote_atoms: Some(0),
            failure_detail: None,
            submitted_at: UNIX_EPOCH + Duration::from_secs(1),
            last_updated_at: UNIX_EPOCH + Duration::from_secs(1),
        };
        let report = HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: Some(test_candidate()),
                shadow_candidate: None,
            },
            build_result: Some(builder::BuildResult {
                status: builder::BuildStatus::Rejected,
                envelope: None,
                rejection: Some(BuildRejectionReason::ExecutionPathCongested),
            }),
            signed_envelope: None,
            submit_result: Some(submission_result),
            execution_record: Some(execution_record),
            submit_leader: Some("leader-1".into()),
            pipeline_trace: PipelineTrace {
                lane: detection::EventLane::Trigger,
                source: EventSourceKind::Synthetic,
                source_sequence: 10,
                observed_slot: 44,
                source_received_at: UNIX_EPOCH,
                normalized_at: UNIX_EPOCH,
                router_received_at: UNIX_EPOCH,
                state_published_at: None,
                source_latency: Some(Duration::from_millis(1)),
                ingest_duration: Duration::from_millis(1),
                queue_wait_duration: Duration::from_millis(1),
                state_mailbox_age_duration: None,
                trigger_queue_wait_duration: None,
                trigger_barrier_wait_duration: None,
                state_apply_duration: Duration::from_millis(1),
                route_eval_duration: Some(Duration::from_millis(1)),
                select_duration: Some(Duration::from_millis(1)),
                build_duration: Some(Duration::from_millis(1)),
                sign_duration: Some(Duration::from_millis(1)),
                submit_duration: Some(Duration::from_millis(1)),
                total_to_submit: Some(Duration::from_millis(4)),
            },
        };

        assert!(trade_from_report(1, &report).is_some());
        report
    }

    fn test_candidate() -> OpportunityCandidate {
        OpportunityCandidate {
            route_id: RouteId("route-a".into()),
            route_kind: RouteKind::TwoLeg,
            quoted_slot: 44,
            leg_snapshot_slots: [44, 44].into(),
            sol_quote_conversion_snapshot_slot: None,
            trade_size: 1_000,
            selected_by: CandidateSelectionSource::Ev,
            ranking_score_quote_atoms: 120,
            expected_value_quote_atoms: 90,
            p_land_bps: 7_500,
            expected_shortfall_quote_atoms: 10,
            active_execution_buffer_bps: Some(25),
            source_input_balance: None,
            expected_net_output: 1_100,
            minimum_acceptable_output: 1_050,
            expected_gross_profit_quote_atoms: 30,
            estimated_network_fee_lamports: 100,
            estimated_network_fee_quote_atoms: 1,
            jito_tip_lamports: 5_000,
            jito_tip_quote_atoms: 2,
            estimated_execution_cost_lamports: 500,
            estimated_execution_cost_quote_atoms: 5,
            expected_net_profit_quote_atoms: 25,
            intermediate_output_amounts: vec![1_050],
            leg_quotes: [
                LegQuote {
                    venue: "orca".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 1_000,
                    output_amount: 1_050,
                    fee_paid: 4,
                    current_tick_index: None,
                    ticks_crossed: 0,
                },
                LegQuote {
                    venue: "raydium".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 1_050,
                    output_amount: 1_100,
                    fee_paid: 4,
                    current_tick_index: None,
                    ticks_crossed: 0,
                },
            ]
            .into(),
        }
    }

    fn temp_path(prefix: &str, extension: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        path.push(format!("{prefix}-{unique}.{extension}"));
        path
    }
}
