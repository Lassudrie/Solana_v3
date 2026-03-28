use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use detection::{EventSourceKind, IngestError, MarketEventSource, NormalizedEvent};
use reconciliation::{
    ExecutionOutcome, ExecutionRecord, ExecutionTracker, ExecutionTransition, OnChainReconciler,
    OnChainReconciliationConfig,
};
use submit::SubmitRejectionReason;
use thiserror::Error;

use crate::{
    account_batcher::GetMultipleAccountsBatcher,
    bootstrap::{BootstrapError, bootstrap_with_health},
    build_sign_dispatch::{BuildSignDispatcher, EnqueueError as BuildSignEnqueueError},
    config::{BotConfig, RuntimeControlConfig},
    control::{RuntimeIssue, RuntimeMode, RuntimeStatus, SharedRuntimeStatus},
    observer::ObserverHandle,
    refresh::{AsyncStateRefresher, RefreshFailure},
    route_health::RouteHealthRegistry,
    runtime::{BotRuntime, PreparedHotPath},
    sources::{EventSourceConfigError, build_event_source},
    submit_dispatch::{EnqueueError, SubmitDispatcher},
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
    #[error("failed to start submit dispatcher: {source}")]
    SubmitDispatcher {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to start build/sign dispatcher: {source}")]
    BuildSignDispatcher {
        #[source]
        source: std::io::Error,
    },
}

enum ReconciliationRequest {
    TrackSubmission(ExecutionRecord),
    Tick { observed_slot: u64 },
    Shutdown,
}

enum ReconciliationResponse {
    Transition {
        observed_slot: u64,
        transition: ExecutionTransition,
        record: ExecutionRecord,
    },
    TickFinished {
        duration: Duration,
    },
}

struct ReconciliationWorker {
    request_tx: Sender<ReconciliationRequest>,
    response_rx: Receiver<ReconciliationResponse>,
    join_handle: Option<JoinHandle<()>>,
    poll_interval: Duration,
    last_tick_completed_at: Option<Instant>,
    tick_in_flight: bool,
    stopped: bool,
}

impl ReconciliationWorker {
    fn spawn(config: OnChainReconciliationConfig, poll_interval: Duration) -> Self {
        let (request_tx, request_rx) = mpsc::channel();
        let (response_tx, response_rx) = mpsc::channel();
        let join_handle = thread::spawn(move || {
            run_reconciliation_worker(config, request_rx, response_tx);
        });
        Self {
            request_tx,
            response_rx,
            join_handle: Some(join_handle),
            poll_interval,
            last_tick_completed_at: None,
            tick_in_flight: false,
            stopped: false,
        }
    }

    fn track_submission(&self, record: &ExecutionRecord) -> Result<(), ()> {
        if record.outcome != ExecutionOutcome::Pending {
            return Ok(());
        }

        self.request_tx
            .send(ReconciliationRequest::TrackSubmission(record.clone()))
            .map_err(|_| ())
    }

    fn request_tick_if_due(&mut self, observed_slot: u64) -> Result<bool, ()> {
        if !self.reconciliation_due() {
            return Ok(false);
        }
        self.request_tick(observed_slot)
    }

    fn request_tick(&mut self, observed_slot: u64) -> Result<bool, ()> {
        if self.tick_in_flight {
            return Ok(false);
        }
        self.request_tx
            .send(ReconciliationRequest::Tick { observed_slot })
            .map_err(|_| ())?;
        self.tick_in_flight = true;
        Ok(true)
    }

    fn try_next_response(&mut self) -> Result<Option<ReconciliationResponse>, ()> {
        match self.response_rx.try_recv() {
            Ok(response) => Ok(Some(response)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(()),
        }
    }

    fn wait_for_response(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ReconciliationResponse>, ()> {
        match self.response_rx.recv_timeout(timeout) {
            Ok(response) => Ok(Some(response)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(()),
        }
    }

    fn note_tick_finished(&mut self) {
        self.tick_in_flight = false;
        self.last_tick_completed_at = Some(Instant::now());
    }

    fn reconciliation_due(&self) -> bool {
        if self.tick_in_flight {
            return false;
        }
        if self.poll_interval == Duration::ZERO {
            return true;
        }

        match self.last_tick_completed_at {
            None => true,
            Some(last) => last.elapsed() >= self.poll_interval,
        }
    }

    fn is_tick_in_flight(&self) -> bool {
        self.tick_in_flight
    }

    fn shutdown(&mut self) {
        if self.stopped {
            return;
        }
        let _ = self.request_tx.send(ReconciliationRequest::Shutdown);
        self.stopped = true;
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

impl Drop for ReconciliationWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_reconciliation_worker(
    config: OnChainReconciliationConfig,
    request_rx: Receiver<ReconciliationRequest>,
    response_tx: Sender<ReconciliationResponse>,
) {
    let mut tracker = ExecutionTracker::default();
    let mut reconciler = OnChainReconciler::new(config);
    while let Ok(request) = request_rx.recv() {
        match request {
            ReconciliationRequest::TrackSubmission(record) => {
                tracker.upsert_record(record);
            }
            ReconciliationRequest::Tick { observed_slot } => {
                let reconcile_started = Instant::now();
                let transitions = reconciler.tick(&mut tracker, observed_slot);
                for transition in transitions {
                    let Some(record) = tracker.get(&transition.submission_id).cloned() else {
                        continue;
                    };
                    if response_tx
                        .send(ReconciliationResponse::Transition {
                            observed_slot,
                            transition,
                            record,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                if response_tx
                    .send(ReconciliationResponse::TickFinished {
                        duration: reconcile_started.elapsed(),
                    })
                    .is_err()
                {
                    return;
                }
            }
            ReconciliationRequest::Shutdown => break,
        }
    }
}

pub struct BotDaemon {
    runtime: BotRuntime,
    build_sign_dispatcher: BuildSignDispatcher,
    submit_dispatcher: SubmitDispatcher,
    reconciliation_worker: ReconciliationWorker,
    refresher: AsyncStateRefresher,
    source: Box<dyn MarketEventSource>,
    status: SharedRuntimeStatus,
    observer: ObserverHandle,
    control: RuntimeControlConfig,
    kill_switch_sentinel_path: Option<PathBuf>,
    min_wallet_balance_lamports: u64,
    max_blockhash_slot_lag: u64,
    last_shredstream_sequence: Option<u64>,
    last_shredstream_source_event_at: Option<SystemTime>,
}

impl BotDaemon {
    pub fn from_config(config: BotConfig) -> Result<Self, DaemonError> {
        let mut config = config;
        config.apply_runtime_profile_defaults();
        let status = SharedRuntimeStatus::new(RuntimeStatus::default());
        if let Err(source) = status.spawn_http_server(&config.runtime.health_server) {
            return Err(DaemonError::HealthServer {
                bind_address: config.runtime.health_server.bind_address.clone(),
                source,
            });
        }
        let route_health = std::sync::Arc::new(std::sync::Mutex::new(RouteHealthRegistry::new(
            config.runtime.live_set_health.clone(),
            config.state.max_snapshot_slot_lag,
        )));
        let mut runtime = bootstrap_with_health(config.clone(), route_health.clone())?;
        let observer = ObserverHandle::spawn(
            &config.runtime.monitor_server,
            &config,
            route_health.clone(),
        )
        .map_err(|source| DaemonError::HealthServer {
            bind_address: config.runtime.monitor_server.bind_address.clone(),
            source,
        })?;
        let submit_dispatcher = SubmitDispatcher::from_config(&config)
            .map_err(|source| DaemonError::SubmitDispatcher { source })?;
        let build_sign_dispatcher = BuildSignDispatcher::new(
            runtime.build_sign_pipeline(),
            config.submit.worker_count,
            config.submit.queue_capacity,
            config.submit.congestion_threshold_pct,
        )
        .map_err(|source| DaemonError::BuildSignDispatcher { source })?;
        let account_batcher = GetMultipleAccountsBatcher::new_with_window(
            &config.reconciliation.rpc_http_endpoint,
            Duration::from_millis(config.reconciliation.account_batch_window_millis),
        );
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
            account_batcher.clone(),
        );
        let lookup_table_cache = refresher.lookup_table_cache_handle();
        let source = build_event_source(
            &config,
            observer.clone(),
            route_health,
            account_batcher,
            lookup_table_cache,
        )?;
        let reconciliation_worker = ReconciliationWorker::spawn(
            OnChainReconciliationConfig {
                enabled: config.reconciliation.enabled,
                rpc_http_endpoint: config.reconciliation.rpc_http_endpoint.clone(),
                rpc_ws_endpoint: config.reconciliation.rpc_ws_endpoint.clone(),
                websocket_enabled: config.reconciliation.websocket_enabled,
                websocket_timeout_ms: config.reconciliation.websocket_timeout_ms,
                search_transaction_history: config.reconciliation.search_transaction_history,
                max_pending_slots: config.reconciliation.max_pending_slots,
            },
            Duration::from_millis(config.reconciliation.poll_interval_millis),
        );

        let daemon = Self {
            runtime,
            build_sign_dispatcher,
            submit_dispatcher,
            reconciliation_worker,
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
            last_shredstream_sequence: None,
            last_shredstream_source_event_at: None,
        };
        daemon.refresh_status(None, None, None);
        Ok(daemon)
    }

    pub fn status_snapshot(&self) -> RuntimeStatus {
        self.status.snapshot()
    }

    pub fn run(&mut self) -> Result<DaemonExit, DaemonError> {
        let max_events_per_tick = self.control.max_events_per_tick.max(1);
        let idle_wait = Duration::from_millis(self.control.idle_sleep_millis);

        loop {
            let refresh_snapshot = self.refresher.drain_snapshot();
            let refresh_failure_status = self.refresh_failure_status(&refresh_snapshot.failures);
            self.runtime.apply_async_refresh(refresh_snapshot);
            self.drain_build_sign_completions();
            self.drain_submit_completions();
            self.drain_reconciliation_updates();
            self.reconcile_if_due();
            let kill_switch_active = self.refresh_kill_switch();
            if let Some((issue, detail)) = refresh_failure_status {
                self.refresh_status(Some(RuntimeMode::Degraded), Some(issue), Some(detail));
            } else if kill_switch_active {
                self.refresh_status(None, Some(RuntimeIssue::KillSwitchActive), None);
            } else {
                self.refresh_status(None, None, None);
            }

            match self.source.wait_next(idle_wait) {
                Ok(Some(event)) => self.process_source_event(event),
                Ok(None) => continue,
                Err(IngestError::Exhausted { .. }) => {
                    self.flush_build_sign_dispatcher();
                    self.flush_submit_dispatcher();
                    self.flush_reconciliation_worker();
                    self.refresh_status(
                        Some(RuntimeMode::Stopped),
                        Some(RuntimeIssue::EventSourceExhausted),
                        None,
                    );
                    return Ok(DaemonExit::SourceExhausted);
                }
                Err(error) => {
                    self.record_source_failure(error);
                    continue;
                }
            }

            for _ in 1..max_events_per_tick {
                match self.source.poll_next() {
                    Ok(Some(event)) => self.process_source_event(event),
                    Ok(None) => break,
                    Err(IngestError::Exhausted { .. }) => {
                        self.flush_build_sign_dispatcher();
                        self.flush_submit_dispatcher();
                        self.flush_reconciliation_worker();
                        self.refresh_status(
                            Some(RuntimeMode::Stopped),
                            Some(RuntimeIssue::EventSourceExhausted),
                            None,
                        );
                        return Ok(DaemonExit::SourceExhausted);
                    }
                    Err(error) => {
                        self.record_source_failure(error);
                        break;
                    }
                }
            }

            self.drain_build_sign_completions();
            self.drain_submit_completions();
            self.drain_reconciliation_updates();
        }
    }

    fn process_source_event(&mut self, event: NormalizedEvent) {
        if event.source.source == EventSourceKind::ShredStream {
            self.record_shredstream_metrics(&event);
        }

        let additional_pending = self
            .submit_dispatcher
            .pending_count()
            .saturating_add(self.build_sign_dispatcher.pending_count());
        match self
            .runtime
            .prepare_event_for_dispatch(event, additional_pending)
        {
            Ok(PreparedHotPath::Report(report)) => {
                self.publish_hot_path_report(report);
                self.refresh_status(None, None, None);
            }
            Ok(PreparedHotPath::BuildSign(task)) => self.dispatch_build_sign(task),
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

    fn dispatch_build_sign(&mut self, task: crate::runtime::PreparedExecution) {
        match self.build_sign_dispatcher.try_enqueue(task) {
            Ok(()) => self.refresh_status(None, None, None),
            Err(BuildSignEnqueueError::Full(task)) => {
                let congestion_issue = {
                    let load = self.build_sign_dispatcher.load();
                    RuntimeIssue::ExecutionPathCongested {
                        pending: load.pending,
                        capacity: load.capacity,
                        workers: load.workers,
                    }
                };
                let report = self
                    .build_sign_dispatcher
                    .queue_rejection(task, builder::BuildRejectionReason::ExecutionPathCongested);
                self.runtime.observe_build_sign_report(&report);
                self.publish_hot_path_report(report);
                self.refresh_status(
                    Some(RuntimeMode::Degraded),
                    Some(congestion_issue),
                    Some("build/sign path congested".into()),
                );
            }
            Err(BuildSignEnqueueError::Disconnected(task)) => {
                let detail = "build/sign dispatcher unavailable".to_string();
                let report = self.build_sign_dispatcher.queue_rejection(
                    task,
                    builder::BuildRejectionReason::ExecutionPathUnavailable,
                );
                self.runtime.observe_build_sign_report(&report);
                self.publish_hot_path_report(report);
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

    fn dispatch_submission(&mut self, report: crate::runtime::HotPathReport) {
        match self.submit_dispatcher.try_enqueue(report) {
            Ok(()) => self.refresh_status(None, None, None),
            Err(EnqueueError::Full(report)) => {
                let congestion_issue = {
                    let load = self.submit_dispatcher.load();
                    RuntimeIssue::SubmitPathCongested {
                        pending: load.pending,
                        capacity: load.capacity,
                        workers: load.workers,
                    }
                };
                let rejected = self
                    .submit_dispatcher
                    .queue_rejection(&report, SubmitRejectionReason::PathCongested);
                let report = self.runtime.finalize_submission(
                    report,
                    rejected,
                    Duration::ZERO,
                    SystemTime::now(),
                );
                self.publish_hot_path_report(report);
                self.refresh_status(
                    Some(RuntimeMode::Degraded),
                    Some(congestion_issue),
                    Some("submit path congested".into()),
                );
            }
            Err(EnqueueError::Disconnected(report)) => {
                let rejected = self
                    .submit_dispatcher
                    .queue_rejection(&report, SubmitRejectionReason::ChannelUnavailable);
                let report = self.runtime.finalize_submission(
                    report,
                    rejected,
                    Duration::ZERO,
                    SystemTime::now(),
                );
                self.publish_hot_path_report(report);
                self.refresh_status(None, None, None);
            }
        }
    }

    fn drain_submit_completions(&mut self) {
        let mut drained = false;
        while let Some(completion) = self.submit_dispatcher.try_next_completion() {
            drained = true;
            let submit_result = match completion.result {
                Ok(result) => result,
                Err(_) => self.submit_dispatcher.queue_rejection(
                    &completion.report,
                    SubmitRejectionReason::ChannelUnavailable,
                ),
            };
            let report = self.runtime.finalize_submission(
                completion.report,
                submit_result,
                completion.submit_duration,
                completion.finished_at,
            );
            self.publish_hot_path_report(report);
        }
        if drained {
            self.refresh_status(None, None, None);
        }
    }

    fn drain_build_sign_completions(&mut self) {
        let mut drained = false;
        let mut had_failure = false;
        while let Some(completion) = self.build_sign_dispatcher.try_next_completion() {
            drained = true;
            self.runtime.observe_build_sign_report(&completion.report);
            if let Some(error) = completion.signing_error {
                had_failure = true;
                let detail = error.to_string();
                self.publish_hot_path_report(completion.report);
                self.refresh_status(
                    Some(RuntimeMode::Degraded),
                    Some(RuntimeIssue::RuntimeFailure {
                        detail: detail.clone(),
                    }),
                    Some(detail),
                );
                continue;
            }
            if completion.report.signed_envelope.is_some() {
                self.dispatch_submission(completion.report);
            } else {
                self.publish_hot_path_report(completion.report);
            }
        }
        if drained && !had_failure {
            self.refresh_status(None, None, None);
        }
    }

    fn flush_build_sign_dispatcher(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut had_failure = false;
        while self.build_sign_dispatcher.pending_count() > 0 {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(100));
            let Some(completion) = self.build_sign_dispatcher.wait_for_completion(timeout) else {
                continue;
            };
            self.runtime.observe_build_sign_report(&completion.report);
            if let Some(error) = completion.signing_error {
                had_failure = true;
                let detail = error.to_string();
                self.publish_hot_path_report(completion.report);
                self.refresh_status(
                    Some(RuntimeMode::Degraded),
                    Some(RuntimeIssue::RuntimeFailure {
                        detail: detail.clone(),
                    }),
                    Some(detail),
                );
                continue;
            }
            if completion.report.signed_envelope.is_some() {
                self.dispatch_submission(completion.report);
            } else {
                self.publish_hot_path_report(completion.report);
            }
        }
        if !had_failure {
            self.refresh_status(None, None, None);
        }
    }

    fn flush_submit_dispatcher(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.submit_dispatcher.pending_count() > 0 {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(100));
            let Some(completion) = self.submit_dispatcher.wait_for_completion(timeout) else {
                continue;
            };
            let submit_result = match completion.result {
                Ok(result) => result,
                Err(_) => self.submit_dispatcher.queue_rejection(
                    &completion.report,
                    SubmitRejectionReason::ChannelUnavailable,
                ),
            };
            let report = self.runtime.finalize_submission(
                completion.report,
                submit_result,
                completion.submit_duration,
                completion.finished_at,
            );
            self.publish_hot_path_report(report);
        }
        self.refresh_status(None, None, None);
    }

    fn record_source_failure(&self, error: IngestError) {
        let detail = error.to_string();
        self.refresh_status(
            Some(RuntimeMode::Degraded),
            Some(RuntimeIssue::EventSourceFailure {
                detail: detail.clone(),
            }),
            Some(detail),
        );
    }

    fn reconcile_if_due(&mut self) {
        let _ = self
            .reconciliation_worker
            .request_tick_if_due(self.runtime.latest_slot());
    }

    fn drain_reconciliation_updates(&mut self) {
        let mut drained = false;
        loop {
            let response = match self.reconciliation_worker.try_next_response() {
                Ok(Some(response)) => response,
                Ok(None) => break,
                Err(()) => break,
            };
            drained = true;
            self.handle_reconciliation_response(response);
        }
        if drained {
            self.refresh_status(None, None, None);
        }
    }

    fn handle_reconciliation_response(&mut self, response: ReconciliationResponse) {
        match response {
            ReconciliationResponse::Transition {
                observed_slot,
                transition,
                record,
            } => {
                let record = self.runtime.apply_reconciliation_transition(
                    observed_slot,
                    &transition,
                    record,
                );
                self.observer.publish_trade_update(record);
            }
            ReconciliationResponse::TickFinished { duration } => {
                self.runtime.record_reconcile_duration(duration);
                self.reconciliation_worker.note_tick_finished();
            }
        }
    }

    fn flush_reconciliation_worker(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        self.wait_for_reconciliation_idle(deadline);
        let _ = self
            .reconciliation_worker
            .request_tick(self.runtime.latest_slot());
        self.wait_for_reconciliation_idle(deadline);
        self.drain_reconciliation_updates();
        self.reconciliation_worker.shutdown();
    }

    fn wait_for_reconciliation_idle(&mut self, deadline: Instant) {
        while self.reconciliation_worker.is_tick_in_flight() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(100));
            let response = match self.reconciliation_worker.wait_for_response(timeout) {
                Ok(Some(response)) => response,
                Ok(None) => continue,
                Err(()) => break,
            };
            self.handle_reconciliation_response(response);
        }
    }

    fn publish_hot_path_report(&mut self, report: crate::runtime::HotPathReport) {
        let execution_record = report.execution_record.clone();
        self.observer.publish_hot_path(report);
        if let Some(record) = execution_record.as_ref() {
            let _ = self.reconciliation_worker.track_submission(record);
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
        let source_received_at = event.latency.source_received_at;
        let source_event_at = event.latency.source_event_at();
        let normalized_latency = event
            .latency
            .normalized_at
            .duration_since(event.latency.source_received_at)
            .unwrap_or(Duration::ZERO);

        self.runtime.telemetry().metrics.record_shredstream_event(
            source_event_at,
            source_received_at,
            self.last_shredstream_source_event_at,
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
        self.last_shredstream_source_event_at = source_event_at;
    }

    fn refresh_status(
        &self,
        mode_override: Option<RuntimeMode>,
        issue_override: Option<RuntimeIssue>,
        last_error: Option<String>,
    ) {
        let execution_state = self.runtime.execution_state();
        let total_routes = self.runtime.route_count();
        let warm_routes = self.runtime.ready_route_count();
        let tradable_routes = self.runtime.tradable_route_count();
        let health_summary = self.runtime.route_health_summary();
        let has_live_tradable_routes =
            tradable_routes > 0 && health_summary.eligible_live_routes > 0;
        let inflight_submissions = self
            .runtime
            .pending_submissions()
            .saturating_add(self.build_sign_dispatcher.pending_count())
            .saturating_add(self.submit_dispatcher.pending_count());
        let blockhash_slot_lag = execution_state.blockhash_slot_lag();
        let derived_issue = issue_override.or_else(|| {
            if execution_state.kill_switch_enabled {
                return Some(RuntimeIssue::KillSwitchActive);
            }
            if total_routes == 0 {
                return Some(RuntimeIssue::NoRoutesConfigured);
            }
            if !has_live_tradable_routes && warm_routes < total_routes {
                return Some(RuntimeIssue::RoutesNotWarm {
                    warm_routes,
                    total_routes,
                });
            }
            if !has_live_tradable_routes {
                return Some(RuntimeIssue::NoTradableRoutes {
                    tradable_routes,
                    eligible_live_routes: health_summary.eligible_live_routes,
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
                _ => self
                    .execution_path_congestion_issue()
                    .or_else(|| self.submit_path_congestion_issue()),
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
                        | RuntimeIssue::ExecutionPathCongested { .. }
                        | RuntimeIssue::SubmitPathCongested { .. }
                        | RuntimeIssue::AsyncRefreshFailed { .. }
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
            warm_routes,
            tradable_routes,
            eligible_live_routes: health_summary.eligible_live_routes,
            shadow_routes: health_summary.shadow_routes,
            quarantined_pools: health_summary.quarantined_pools,
            disabled_pools: health_summary.disabled_pools,
            inflight_submissions,
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

    fn submit_path_congestion_issue(&self) -> Option<RuntimeIssue> {
        self.submit_dispatcher
            .congestion_load()
            .map(|load| RuntimeIssue::SubmitPathCongested {
                pending: load.pending,
                capacity: load.capacity,
                workers: load.workers,
            })
    }

    fn execution_path_congestion_issue(&self) -> Option<RuntimeIssue> {
        self.build_sign_dispatcher.congestion_load().map(|load| {
            RuntimeIssue::ExecutionPathCongested {
                pending: load.pending,
                capacity: load.capacity,
                workers: load.workers,
            }
        })
    }

    fn refresh_failure_status(
        &self,
        failures: &[RefreshFailure],
    ) -> Option<(RuntimeIssue, String)> {
        let first = failures.first()?;
        let detail = failures
            .iter()
            .map(|failure| format!("{}: {}", failure.worker, failure.detail))
            .collect::<Vec<_>>()
            .join(" | ");
        Some((
            RuntimeIssue::AsyncRefreshFailed {
                worker: first.worker.to_string(),
            },
            detail,
        ))
    }
}

#[cfg(test)]
mod tests {
    use detection::{EventSourceKind, NormalizedEvent};
    use domain::{PoolSnapshotUpdate, SnapshotConfidence, types::PoolVenue};
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
        RouteConfig, RouteExecutionConfig, RouteKindConfig, RouteLegConfig,
        RouteLegExecutionConfig, RoutesConfig, SwapSideConfig,
    };

    use super::{BotDaemon, DaemonExit};

    #[test]
    fn daemon_processes_jsonl_replay_and_reaches_submit() {
        let path = temp_path("bot-daemon-replay", "jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                replay_event("pool-a", 1, 5_000),
                replay_event("pool-b", 2, 20_000)
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
        assert_eq!(status.warm_routes, 1);
        assert_eq!(status.tradable_routes, 1);
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
                replay_event("pool-a", 1, 5_000),
                replay_event("pool-b", 2, 20_000)
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
                replay_event("pool-a", 1, 5_000),
                replay_event("pool-b", 2, 20_000)
            ),
        )
        .unwrap();

        let latest_blockhash = hashv(&[b"daemon-reconcile-test-blockhash"]).to_string();
        let rpc_endpoint = spawn_mock_rpc_server(move |body| {
            let payload: Value = serde_json::from_str(body).expect("json-rpc body");
            match payload["method"].as_str().expect("method") {
                "getLatestBlockhash" => json!({
                    "result": {
                        "context": { "slot": 0 },
                        "value": { "blockhash": latest_blockhash }
                    }
                })
                .to_string(),
                "getBalance" => json!({
                    "result": {
                        "context": { "slot": 0 },
                        "value": 1_000_000_000u64
                    }
                })
                .to_string(),
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
        assert_eq!(status.metrics.submit_count, 1, "{status:?}");
        assert_eq!(status.metrics.inclusion_count, 1, "{status:?}");
        assert_eq!(status.inflight_submissions, 0, "{status:?}");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn daemon_reports_ready_when_some_routes_are_still_warming() {
        let path = temp_path("bot-daemon-partial-warm", "jsonl");
        fs::write(&path, "").unwrap();
        let mut config = bot_config(path.to_string_lossy().into_owned());
        config.runtime.health_server.enabled = false;
        config.routes.definitions.push(unwarmed_route(
            "route-b", "pool-c", "pool-d", "acct-c", "acct-d",
        ));
        let mut daemon = BotDaemon::from_config(config).unwrap();

        daemon.process_source_event(replay_snapshot_event("pool-a", 1, 5_000));
        daemon.process_source_event(replay_snapshot_event("pool-b", 2, 20_000));
        daemon.flush_build_sign_dispatcher();
        daemon.flush_submit_dispatcher();
        daemon.flush_reconciliation_worker();
        let status = daemon.status_snapshot();

        assert_eq!(status.total_routes, 2);
        assert_eq!(status.warm_routes, 1);
        assert_eq!(status.tradable_routes, 1);
        assert!(status.ready);
        assert_eq!(status.issue, None);

        let _ = fs::remove_file(path);
    }

    fn bot_config(path: String) -> BotConfig {
        let mut config = BotConfig::default();
        let keypair = Keypair::new_from_array([7; 32]);
        config.jito.endpoint = "mock://jito".into();
        config.builder.compute_unit_limit = 300_000;
        config.builder.compute_unit_price_micro_lamports = 1;
        config.builder.jito_tip_lamports = 1;
        config.signing.validate_execution_accounts = false;
        config.runtime.live_set_health.enabled = false;
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        config.runtime.refresh.enabled = false;
        config.signing.keypair_base58 = Some(keypair.to_base58_string());
        config.routes = RoutesConfig {
            definitions: vec![RouteConfig {
                enabled: true,
                route_class: RouteClassConfig::AmmFastPath,
                kind: RouteKindConfig::TwoLeg,
                route_id: "route-a".into(),
                input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                base_mint: Some("So11111111111111111111111111111111111111112".into()),
                quote_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                sol_quote_conversion_pool_id: Some("pool-a".into()),
                min_profit_quote_atoms: None,
                min_trade_size: None,
                default_trade_size: 10_000,
                max_trade_size: 20_000,
                size_ladder: Vec::new(),
                sizing: crate::config::RouteSizingConfig {
                    min_trade_floor_sol_lamports: Some(0),
                    ..Default::default()
                },
                execution_protection: Default::default(),
                legs: vec![
                    RouteLegConfig {
                        venue: "orca".into(),
                        pool_id: "pool-a".into(),
                        side: SwapSideConfig::BuyBase,
                        input_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                        output_mint: Some("So11111111111111111111111111111111111111112".into()),
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
                        input_mint: Some("So11111111111111111111111111111111111111112".into()),
                        output_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
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
                                user_source_token_account: Some(test_pubkey("route-mid-ata")),
                                user_destination_token_account: Some(test_pubkey(
                                    "route-output-ata",
                                )),
                                user_source_mint: None,
                                user_destination_mint: None,
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
                    minimum_compute_unit_limit: 0,
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

    fn unwarmed_route(
        route_id: &str,
        first_pool_id: &str,
        second_pool_id: &str,
        first_account_key: &str,
        second_account_key: &str,
    ) -> RouteConfig {
        RouteConfig {
            enabled: true,
            route_class: RouteClassConfig::AmmFastPath,
            kind: RouteKindConfig::TwoLeg,
            route_id: route_id.into(),
            input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            base_mint: Some("So11111111111111111111111111111111111111112".into()),
            quote_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
            sol_quote_conversion_pool_id: Some(first_pool_id.into()),
            min_profit_quote_atoms: None,
            min_trade_size: None,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            sizing: crate::config::RouteSizingConfig {
                min_trade_floor_sol_lamports: Some(0),
                ..Default::default()
            },
            execution_protection: Default::default(),
            legs: vec![
                RouteLegConfig {
                    venue: "orca".into(),
                    pool_id: first_pool_id.into(),
                    side: SwapSideConfig::BuyBase,
                    input_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                    output_mint: Some("So11111111111111111111111111111111111111112".into()),
                    fee_bps: None,
                    execution: RouteLegExecutionConfig::OrcaSimplePool(
                        OrcaSimplePoolLegExecutionConfig {
                            program_id: test_pubkey("orca-program-unwarmed"),
                            token_program_id: test_pubkey("spl-token-program-unwarmed"),
                            swap_account: test_pubkey("orca-swap-unwarmed"),
                            authority: test_pubkey("orca-authority-unwarmed"),
                            pool_source_token_account: test_pubkey("orca-pool-source-unwarmed"),
                            pool_destination_token_account: test_pubkey(
                                "orca-pool-destination-unwarmed",
                            ),
                            pool_mint: test_pubkey("orca-pool-mint-unwarmed"),
                            fee_account: test_pubkey("orca-fee-account-unwarmed"),
                            user_source_token_account: test_pubkey("route-input-ata-unwarmed"),
                            user_destination_token_account: test_pubkey("route-mid-ata-unwarmed"),
                            host_fee_account: None,
                        },
                    ),
                },
                RouteLegConfig {
                    venue: "raydium".into(),
                    pool_id: second_pool_id.into(),
                    side: SwapSideConfig::SellBase,
                    input_mint: Some("So11111111111111111111111111111111111111112".into()),
                    output_mint: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()),
                    fee_bps: None,
                    execution: RouteLegExecutionConfig::RaydiumSimplePool(
                        RaydiumSimplePoolLegExecutionConfig {
                            program_id: test_pubkey("raydium-program-unwarmed"),
                            token_program_id: test_pubkey("spl-token-program-unwarmed"),
                            amm_pool: test_pubkey("raydium-amm-pool-unwarmed"),
                            amm_authority: test_pubkey("raydium-amm-authority-unwarmed"),
                            amm_open_orders: test_pubkey("raydium-open-orders-unwarmed"),
                            amm_coin_vault: test_pubkey("raydium-coin-vault-unwarmed"),
                            amm_pc_vault: test_pubkey("raydium-pc-vault-unwarmed"),
                            market_program: test_pubkey("serum-program-unwarmed"),
                            market: test_pubkey("serum-market-unwarmed"),
                            market_bids: test_pubkey("serum-bids-unwarmed"),
                            market_asks: test_pubkey("serum-asks-unwarmed"),
                            market_event_queue: test_pubkey("serum-event-queue-unwarmed"),
                            market_coin_vault: test_pubkey("serum-coin-vault-unwarmed"),
                            market_pc_vault: test_pubkey("serum-pc-vault-unwarmed"),
                            market_vault_signer: test_pubkey("serum-vault-signer-unwarmed"),
                            user_source_token_account: Some(test_pubkey("route-mid-ata-unwarmed")),
                            user_destination_token_account: Some(test_pubkey(
                                "route-output-ata-unwarmed",
                            )),
                            user_source_mint: None,
                            user_destination_mint: None,
                        },
                    ),
                },
            ],
            account_dependencies: vec![
                AccountDependencyConfig {
                    account_key: first_account_key.into(),
                    pool_id: first_pool_id.into(),
                    decoder_key: "pool-price-v1".into(),
                },
                AccountDependencyConfig {
                    account_key: second_account_key.into(),
                    pool_id: second_pool_id.into(),
                    decoder_key: "pool-price-v1".into(),
                },
            ],
            execution: RouteExecutionConfig {
                message_mode: MessageModeConfig::V0Required,
                lookup_tables: Vec::new(),
                default_compute_unit_limit: 300_000,
                minimum_compute_unit_limit: 0,
                default_compute_unit_price_micro_lamports: 25_000,
                default_jito_tip_lamports: 5_000,
                max_quote_slot_lag: 4,
                max_alt_slot_lag: 4,
            },
        }
    }

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
    }

    fn replay_event(pool_id: &str, sequence: u64, price_bps: u64) -> String {
        let reserve_a = 100_000u64;
        let reserve_b = (reserve_a as u128)
            .saturating_mul(price_bps as u128)
            .checked_div(10_000)
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(reserve_a);
        let reserve_depth = reserve_a.max(reserve_b);
        format!(
            "{{\"type\":\"pool_snapshot_update\",\"source\":\"replay\",\"sequence\":{sequence},\"observed_slot\":{sequence},\"pool_id\":\"{pool_id}\",\"price_bps\":{price_bps},\"fee_bps\":4,\"reserve_depth\":{reserve_depth},\"reserve_a\":{reserve_a},\"reserve_b\":{reserve_b},\"active_liquidity\":{reserve_depth},\"sqrt_price_x64\":null,\"venue\":\"orca_simple_pool\",\"confidence\":\"executable\",\"repair_pending\":false,\"token_mint_a\":\"So11111111111111111111111111111111111111112\",\"token_mint_b\":\"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v\",\"tick_spacing\":0,\"current_tick_index\":null,\"slot\":{sequence},\"write_version\":1}}"
        )
    }

    fn replay_snapshot_event(pool_id: &str, sequence: u64, price_bps: u64) -> NormalizedEvent {
        let reserve_a = 100_000u64;
        let reserve_b = (reserve_a as u128)
            .saturating_mul(price_bps as u128)
            .checked_div(10_000)
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(reserve_a);
        NormalizedEvent::pool_snapshot_update(
            EventSourceKind::Replay,
            sequence,
            sequence,
            PoolSnapshotUpdate {
                pool_id: pool_id.into(),
                price_bps,
                fee_bps: 4,
                reserve_depth: reserve_a.max(reserve_b),
                reserve_a: Some(reserve_a),
                reserve_b: Some(reserve_b),
                active_liquidity: Some(reserve_a.max(reserve_b)),
                sqrt_price_x64: None,
                venue: PoolVenue::OrcaSimplePool,
                confidence: SnapshotConfidence::Executable,
                repair_pending: Some(false),
                token_mint_a: "So11111111111111111111111111111111111111112".into(),
                token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                tick_spacing: 0,
                current_tick_index: None,
                slot: sequence,
                write_version: 1,
            },
        )
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
