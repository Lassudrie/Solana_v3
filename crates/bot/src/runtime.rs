use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use builder::{BuildRequest, BuildResult, BuildStatus, DynamicBuildParameters, TransactionBuilder};
use domain::{
    AccountUpdateStatus, EventSourceKind, ExecutionSnapshot, LookupTableSnapshot, MarketEvent,
    NormalizedEvent, PoolSnapshot, StateApplyOutcome,
};
use reconciliation::{
    ExecutionOutcome, ExecutionRecord, ExecutionTracker, ExecutionTransition, FailureClass,
};
use signing::{HotWallet, SignedTransactionEnvelope, Signer, SigningError, SigningRequest};
use state::{StateError, StatePlane};
use strategy::{StrategyPlane, opportunity::SelectionOutcome};
use submit::{SubmitError, SubmitMode, SubmitRequest, SubmitResult, Submitter};
use telemetry::{PipelineStage, TelemetryStack};
use thiserror::Error;

use crate::execution_context::ExecutionContext;
use crate::refresh::AsyncRefreshSnapshot;
use crate::route_health::{RouteHealthSummary, SharedRouteHealth};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotPathReport {
    pub state_outcome: Option<StateApplyOutcome>,
    pub pool_snapshots: Vec<PoolSnapshot>,
    pub selection: SelectionOutcome,
    pub build_result: Option<BuildResult>,
    pub signed_envelope: Option<SignedTransactionEnvelope>,
    pub submit_result: Option<SubmitResult>,
    pub execution_record: Option<ExecutionRecord>,
    pub submit_leader: Option<String>,
    pub pipeline_trace: PipelineTrace,
}

impl HotPathReport {
    fn empty(
        state_outcome: Option<StateApplyOutcome>,
        submit_leader: Option<String>,
        pipeline_trace: PipelineTrace,
    ) -> Self {
        Self {
            state_outcome,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: None,
            submit_leader,
            pipeline_trace,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineTrace {
    pub source: EventSourceKind,
    pub source_sequence: u64,
    pub observed_slot: u64,
    pub source_received_at: SystemTime,
    pub normalized_at: SystemTime,
    pub source_latency: Option<Duration>,
    pub ingest_duration: Duration,
    pub queue_wait_duration: Duration,
    pub state_apply_duration: Duration,
    pub select_duration: Option<Duration>,
    pub build_duration: Option<Duration>,
    pub sign_duration: Option<Duration>,
    pub submit_duration: Option<Duration>,
    pub total_to_submit: Option<Duration>,
}

impl PipelineTrace {
    fn from_event(event: &NormalizedEvent) -> Self {
        let ingest_duration = event
            .latency
            .normalized_at
            .duration_since(event.latency.source_received_at)
            .unwrap_or(Duration::ZERO);
        Self {
            source: event.source.source,
            source_sequence: event.source.sequence,
            observed_slot: event.source.observed_slot,
            source_received_at: event.latency.source_received_at,
            normalized_at: event.latency.normalized_at,
            source_latency: event.latency.source_latency,
            ingest_duration,
            queue_wait_duration: Duration::ZERO,
            state_apply_duration: Duration::ZERO,
            select_duration: None,
            build_duration: None,
            sign_duration: None,
            submit_duration: None,
            total_to_submit: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Signing(#[from] SigningError),
    #[error(transparent)]
    Submit(#[from] SubmitError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedExecution {
    pub report: HotPathReport,
    pub build_request: BuildRequest,
    pub wallet: HotWallet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedHotPath {
    Report(HotPathReport),
    BuildSign(PreparedExecution),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildSignCompletion {
    pub report: HotPathReport,
    pub signing_error: Option<SigningError>,
}

pub struct HotPathPipeline {
    state: StatePlane,
    execution: ExecutionContext,
    strategy: StrategyPlane,
    builder: builder::AtomicArbTransactionBuilder,
    wallet: HotWallet,
    signer: Arc<dyn Signer>,
    submitter: Arc<dyn Submitter>,
}

impl HotPathPipeline {
    pub fn new(
        state: StatePlane,
        strategy: StrategyPlane,
        builder: builder::AtomicArbTransactionBuilder,
        wallet: HotWallet,
        signer: Arc<dyn Signer>,
        submitter: Arc<dyn Submitter>,
    ) -> Self {
        Self {
            state,
            execution: ExecutionContext::default(),
            strategy,
            builder,
            wallet,
            signer,
            submitter,
        }
    }

    fn build_sign_pipeline(&self) -> BuildSignPipeline {
        BuildSignPipeline {
            builder: self.builder.clone(),
            signer: Arc::clone(&self.signer),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BuildSignPipeline {
    builder: builder::AtomicArbTransactionBuilder,
    signer: Arc<dyn Signer>,
}

impl BuildSignPipeline {
    pub(crate) fn execute(&self, mut prepared: PreparedExecution) -> BuildSignCompletion {
        let build_started = Instant::now();
        let build_result = self.builder.build(prepared.build_request);
        prepared.report.pipeline_trace.build_duration = Some(build_started.elapsed());
        prepared.report.build_result = Some(build_result.clone());
        if build_result.status != BuildStatus::Built {
            return BuildSignCompletion {
                report: prepared.report,
                signing_error: None,
            };
        }

        let unsigned_envelope = build_result.envelope.expect("built envelope");
        let sign_started = Instant::now();
        match self.signer.sign(
            &prepared.wallet,
            SigningRequest {
                envelope: unsigned_envelope,
            },
        ) {
            Ok(signed_envelope) => {
                prepared.report.pipeline_trace.sign_duration = Some(sign_started.elapsed());
                prepared.report.signed_envelope = Some(signed_envelope);
                BuildSignCompletion {
                    report: prepared.report,
                    signing_error: None,
                }
            }
            Err(error) => {
                prepared.report.pipeline_trace.sign_duration = Some(sign_started.elapsed());
                BuildSignCompletion {
                    report: prepared.report,
                    signing_error: Some(error),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WarmupCoordinator {
    pub primed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockhashService {
    pub current_blockhash: String,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AltCacheService {
    pub revision: u64,
    pub lookup_tables: Vec<LookupTableSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletRefreshService {
    pub balance_lamports: u64,
    pub ready: bool,
    pub signer_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdPathServices {
    pub warmup: WarmupCoordinator,
    pub blockhash: BlockhashService,
    pub alt_cache: AltCacheService,
    pub wallet_refresh: WalletRefreshService,
}

impl ColdPathServices {
    pub fn seed_hot_path(&self, hot_path: &mut HotPathPipeline) {
        hot_path.execution.set_blockhash(
            self.blockhash.current_blockhash.clone(),
            self.blockhash.slot,
        );
        hot_path.execution.set_rpc_slot(self.blockhash.slot);
        hot_path.execution.set_alt_revision(self.alt_cache.revision);
        hot_path
            .execution
            .set_lookup_tables(self.alt_cache.lookup_tables.clone());
        hot_path.execution.set_wallet_state(
            self.wallet_refresh.balance_lamports,
            self.wallet_refresh.ready && self.wallet_refresh.signer_available,
        );
        hot_path.wallet.balance_lamports = self.wallet_refresh.balance_lamports;
        hot_path.wallet.status = wallet_status(
            self.wallet_refresh.ready,
            self.wallet_refresh.signer_available,
        );
    }
}

fn wallet_status(ready: bool, signer_available: bool) -> signing::WalletStatus {
    if !signer_available {
        signing::WalletStatus::MissingSigner
    } else if ready {
        signing::WalletStatus::Ready
    } else {
        signing::WalletStatus::Refreshing
    }
}

pub struct BotRuntime {
    hot_path: HotPathPipeline,
    cold_path: ColdPathServices,
    tracker: ExecutionTracker,
    telemetry: TelemetryStack,
    submit_mode: SubmitMode,
    compute_unit_limit: u32,
    compute_unit_price_micro_lamports: u64,
    jito_tip_lamports: u64,
    route_health: SharedRouteHealth,
}

impl BotRuntime {
    pub fn new(
        hot_path: HotPathPipeline,
        cold_path: ColdPathServices,
        submit_mode: SubmitMode,
        compute_unit_limit: u32,
        compute_unit_price_micro_lamports: u64,
        jito_tip_lamports: u64,
        route_health: SharedRouteHealth,
    ) -> Self {
        Self {
            hot_path,
            cold_path,
            tracker: ExecutionTracker::default(),
            telemetry: TelemetryStack::default(),
            submit_mode,
            compute_unit_limit,
            compute_unit_price_micro_lamports,
            jito_tip_lamports,
            route_health,
        }
    }

    pub fn apply_cold_path_seed(&mut self) {
        self.cold_path.seed_hot_path(&mut self.hot_path);
        self.hot_path
            .state
            .set_latest_slot(self.cold_path.blockhash.slot);
    }

    pub fn apply_async_refresh(&mut self, snapshot: AsyncRefreshSnapshot) {
        if let Some(blockhash) = snapshot.blockhash {
            self.hot_path
                .execution
                .set_blockhash(blockhash.blockhash, blockhash.slot);
            self.hot_path.execution.set_rpc_slot(blockhash.slot);
            self.hot_path.state.set_latest_slot(blockhash.slot);
        }

        if let Some(slot) = snapshot.slot {
            self.hot_path.execution.set_rpc_slot(slot.slot);
            self.hot_path.state.set_latest_slot(slot.slot);
        }

        if let Some(lookup_tables) = snapshot.lookup_tables {
            self.hot_path.execution.set_alt_revision(lookup_tables.slot);
            self.hot_path
                .execution
                .set_lookup_tables(lookup_tables.tables);
            self.hot_path.state.set_latest_slot(lookup_tables.slot);
        }

        if let Some(wallet) = snapshot.wallet {
            let signer_available = self.hot_path.signer.is_available();
            self.hot_path
                .execution
                .set_wallet_state(wallet.balance_lamports, wallet.ready && signer_available);
            self.hot_path.state.set_latest_slot(wallet.slot);
            self.hot_path.wallet.balance_lamports = wallet.balance_lamports;
            self.hot_path.wallet.status = wallet_status(wallet.ready, signer_available);
        }
    }

    pub fn telemetry(&self) -> &TelemetryStack {
        &self.telemetry
    }

    pub(crate) fn build_sign_pipeline(&self) -> BuildSignPipeline {
        self.hot_path.build_sign_pipeline()
    }

    pub fn execution_state(&self) -> ExecutionSnapshot {
        self.hot_path
            .execution
            .snapshot(self.hot_path.state.latest_slot())
    }

    pub fn latest_slot(&self) -> u64 {
        self.hot_path.state.latest_slot()
    }

    pub fn wallet_pubkey(&self) -> &str {
        &self.hot_path.wallet.owner_pubkey
    }

    pub fn route_count(&self) -> usize {
        self.hot_path.state.route_count()
    }

    pub fn ready_route_count(&self) -> usize {
        self.hot_path.state.ready_route_count()
    }

    pub fn tradable_route_count(&self) -> usize {
        self.hot_path.state.tradable_route_count()
    }

    pub fn pending_submissions(&self) -> usize {
        self.tracker.pending_count()
    }

    pub fn route_health_summary(&self) -> RouteHealthSummary {
        self.route_health
            .lock()
            .ok()
            .map(|health| health.summary(self.latest_slot()))
            .unwrap_or(RouteHealthSummary {
                eligible_live_routes: 0,
                shadow_routes: 0,
                quarantined_pools: 0,
                disabled_pools: 0,
            })
    }

    pub fn record_reconcile_duration(&self, duration: Duration) {
        self.telemetry
            .record_stage(PipelineStage::Reconcile, duration);
    }

    pub(crate) fn observe_build_sign_report(&self, report: &HotPathReport) {
        if let Some(build_duration) = report.pipeline_trace.build_duration {
            self.telemetry
                .record_stage(PipelineStage::Build, build_duration);
        }
        if let Some(sign_duration) = report.pipeline_trace.sign_duration {
            self.telemetry
                .record_stage(PipelineStage::Sign, sign_duration);
        }
        match report.build_result.as_ref().map(|result| result.status) {
            Some(BuildStatus::Built) => self.telemetry.metrics.increment_build(),
            Some(BuildStatus::Rejected) => self.telemetry.metrics.increment_rejection(),
            None => {}
        }
    }

    pub fn apply_reconciliation_transition(
        &mut self,
        observed_slot: u64,
        transition: &ExecutionTransition,
        record: ExecutionRecord,
    ) -> ExecutionRecord {
        self.tracker.upsert_record(record.clone());
        let mut route_health = self.route_health.lock().ok();
        match &transition.current_outcome {
            ExecutionOutcome::Included { .. } => self
                .hot_path
                .strategy
                .record_execution_success(&record.route_id),
            ExecutionOutcome::Failed(FailureClass::ChainExecutionTooLittleOutput) => self
                .hot_path
                .strategy
                .record_execution_too_little_output(&record.route_id),
            _ => {}
        }
        if let Some(route_health) = route_health.as_mut() {
            match &transition.current_outcome {
                ExecutionOutcome::Included { .. } => {
                    route_health.on_execution_success(&record.route_id, observed_slot);
                }
                ExecutionOutcome::Failed(class) => {
                    route_health.on_execution_failure(&record.route_id, class, observed_slot);
                }
                ExecutionOutcome::Pending => {}
            }
        }
        self.record_transition_metrics(transition);
        record
    }

    pub fn apply_kill_switch(&mut self, enabled: bool) {
        self.hot_path.execution.set_kill_switch(enabled);
    }

    pub(crate) fn prepare_event_for_dispatch(
        &mut self,
        event: NormalizedEvent,
        additional_pending_submissions: usize,
    ) -> Result<PreparedHotPath, RuntimeError> {
        let mut pipeline_trace = PipelineTrace::from_event(&event);
        let process_started_at = SystemTime::now();
        pipeline_trace.queue_wait_duration = process_started_at
            .duration_since(event.latency.normalized_at)
            .unwrap_or(Duration::ZERO);
        self.telemetry.metrics.increment_detect();
        if let MarketEvent::SlotBoundary(slot_boundary) = &event.payload {
            self.hot_path
                .execution
                .set_current_leader(slot_boundary.leader.clone());
        }
        let submit_leader = self.hot_path.execution.current_leader();

        let state_started = Instant::now();
        let state_outcome = self.hot_path.state.apply_event(&event)?;
        let state_duration = state_started.elapsed();
        pipeline_trace.state_apply_duration = state_duration;
        self.telemetry
            .record_stage(PipelineStage::StateApply, state_duration);

        if let Some(outcome) = &state_outcome {
            if outcome.update_status == AccountUpdateStatus::StaleRejected {
                self.telemetry.metrics.increment_stale();
                return Ok(PreparedHotPath::Report(HotPathReport::empty(
                    state_outcome,
                    submit_leader,
                    pipeline_trace,
                )));
            }
        } else {
            return Ok(PreparedHotPath::Report(HotPathReport::empty(
                None,
                submit_leader,
                pipeline_trace,
            )));
        }

        let state_outcome = state_outcome.expect("checked above");
        let pool_snapshots = self
            .hot_path
            .state
            .pool_snapshots_for(&state_outcome.impacted_pools);
        if let Ok(mut route_health) = self.route_health.lock() {
            for snapshot in &pool_snapshots {
                route_health.on_pool_snapshot(
                    snapshot,
                    self.hot_path
                        .state
                        .pool_has_executable_quote_model(&snapshot.pool_id),
                    pipeline_trace.observed_slot,
                );
            }
        }
        if state_outcome.impacted_routes.is_empty() {
            return Ok(PreparedHotPath::Report(HotPathReport {
                state_outcome: Some(state_outcome),
                pool_snapshots,
                selection: SelectionOutcome {
                    decisions: Vec::new(),
                    best_candidate: None,
                },
                build_result: None,
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
                submit_leader,
                pipeline_trace,
            }));
        }

        let impacted_routes = self
            .route_health
            .lock()
            .ok()
            .map(|health| {
                health.eligible_impacted_routes(
                    &state_outcome.impacted_routes,
                    pipeline_trace.observed_slot,
                )
            })
            .unwrap_or_else(|| state_outcome.impacted_routes.clone());
        if impacted_routes.is_empty() {
            self.telemetry.metrics.increment_rejection();
            return Ok(PreparedHotPath::Report(HotPathReport {
                state_outcome: Some(state_outcome),
                pool_snapshots,
                selection: SelectionOutcome {
                    decisions: Vec::new(),
                    best_candidate: None,
                },
                build_result: None,
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
                submit_leader,
                pipeline_trace,
            }));
        }

        let strategy_started = Instant::now();
        let execution_state = self
            .hot_path
            .execution
            .snapshot(self.hot_path.state.latest_slot());
        let selection = self.hot_path.strategy.evaluate(
            &self.hot_path.state,
            &execution_state,
            &impacted_routes,
            self.tracker
                .pending_count()
                .saturating_add(additional_pending_submissions),
        );
        let select_duration = strategy_started.elapsed();
        pipeline_trace.select_duration = Some(select_duration);
        self.telemetry
            .record_stage(PipelineStage::Select, select_duration);
        if selection.best_candidate.is_none() {
            self.telemetry.metrics.increment_rejection();
            return Ok(PreparedHotPath::Report(HotPathReport {
                state_outcome: Some(state_outcome),
                pool_snapshots,
                selection,
                build_result: None,
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
                submit_leader,
                pipeline_trace,
            }));
        }

        let candidate = selection
            .best_candidate
            .clone()
            .expect("candidate available");
        let build_request = BuildRequest {
            candidate,
            dynamic: DynamicBuildParameters {
                recent_blockhash: execution_state.latest_blockhash.unwrap_or_default(),
                recent_blockhash_slot: execution_state.blockhash_slot,
                head_slot: execution_state.head_slot,
                fee_payer_pubkey: self.hot_path.signer.pubkey_string()?,
                compute_unit_limit: self.compute_unit_limit,
                compute_unit_price_micro_lamports: self.compute_unit_price_micro_lamports,
                jito_tip_lamports: self.jito_tip_lamports,
                resolved_lookup_tables: execution_state.lookup_tables.clone(),
            },
        };
        Ok(PreparedHotPath::BuildSign(PreparedExecution {
            report: HotPathReport {
                state_outcome: Some(state_outcome),
                pool_snapshots,
                selection,
                build_result: None,
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
                submit_leader,
                pipeline_trace,
            },
            build_request,
            wallet: self.hot_path.wallet.clone(),
        }))
    }

    pub fn prepare_event(
        &mut self,
        event: NormalizedEvent,
        additional_pending_submissions: usize,
    ) -> Result<HotPathReport, RuntimeError> {
        match self.prepare_event_for_dispatch(event, additional_pending_submissions)? {
            PreparedHotPath::Report(report) => Ok(report),
            PreparedHotPath::BuildSign(prepared) => {
                let completion = self.hot_path.build_sign_pipeline().execute(prepared);
                self.observe_build_sign_report(&completion.report);
                if let Some(error) = completion.signing_error {
                    return Err(RuntimeError::Signing(error));
                }
                Ok(completion.report)
            }
        }
    }

    pub fn finalize_submission(
        &mut self,
        mut report: HotPathReport,
        submit_result: SubmitResult,
        submit_duration: Duration,
        submitted_at: SystemTime,
    ) -> HotPathReport {
        report.pipeline_trace.submit_duration = Some(submit_duration);
        report.pipeline_trace.total_to_submit = submitted_at
            .duration_since(report.pipeline_trace.source_received_at)
            .ok();
        self.telemetry
            .record_stage(PipelineStage::Submit, submit_duration);
        self.telemetry.metrics.increment_submit();

        let signed_envelope = report
            .signed_envelope
            .clone()
            .expect("submission finalization requires a signed envelope");
        let submitted_slot = Some(
            self.latest_slot()
                .max(signed_envelope.blockhash_slot.unwrap_or(0)),
        )
        .filter(|slot| *slot > 0);
        let execution_record = self.tracker.register_submission(
            signed_envelope.route_id.clone(),
            signed_envelope.signature.clone(),
            signed_envelope.quoted_slot,
            signed_envelope.blockhash_slot,
            submitted_slot,
            submitted_at,
            self.submit_mode,
            submit_result.clone(),
        );
        if let ExecutionOutcome::Failed(class) = &execution_record.outcome {
            if let Ok(mut route_health) = self.route_health.lock() {
                route_health.on_execution_failure(
                    &execution_record.route_id,
                    class,
                    execution_record.slot_fallback(),
                );
            }
        }
        self.record_submission_metrics(&execution_record);
        report.submit_result = Some(submit_result);
        report.execution_record = Some(execution_record);
        report
    }

    pub fn process_event(&mut self, event: NormalizedEvent) -> Result<HotPathReport, RuntimeError> {
        let report = self.prepare_event(event, 0)?;
        let Some(signed_envelope) = report.signed_envelope.clone() else {
            return Ok(report);
        };

        let submit_started = Instant::now();
        let submit_result = self.hot_path.submitter.submit(SubmitRequest {
            envelope: signed_envelope,
            mode: self.submit_mode,
            leader: report.submit_leader.clone(),
        })?;
        Ok(self.finalize_submission(
            report,
            submit_result,
            submit_started.elapsed(),
            SystemTime::now(),
        ))
    }

    fn record_submission_metrics(&self, record: &ExecutionRecord) {
        if matches!(
            record.outcome,
            ExecutionOutcome::Failed(FailureClass::SubmitRejected)
        ) {
            self.telemetry.metrics.increment_submit_rejected();
        }
    }

    fn record_transition_metrics(&self, transition: &ExecutionTransition) {
        match &transition.current_outcome {
            ExecutionOutcome::Pending => {}
            ExecutionOutcome::Included { .. } => self.telemetry.metrics.increment_inclusion(),
            ExecutionOutcome::Failed(class) => match class {
                FailureClass::SubmitRejected => self.telemetry.metrics.increment_submit_rejected(),
                FailureClass::TransportFailed => {
                    self.telemetry.metrics.increment_transport_failed()
                }
                FailureClass::Expired => self.telemetry.metrics.increment_expired(),
                FailureClass::ChainDropped
                | FailureClass::ChainExecutionTooLittleOutput
                | FailureClass::ChainExecutionFailed
                | FailureClass::Unknown => self.telemetry.metrics.increment_chain_failed(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use detection::{EventSourceKind, NormalizedEvent, PoolSnapshotUpdate, SnapshotConfidence};
    use domain::PoolVenue;
    use signerd::{SecureSignerService, SecureSignerServiceConfig};
    use signing::{
        HotWallet, SignedTransactionEnvelope, Signer as WalletSigner, SigningError, SigningRequest,
    };
    use solana_sdk::{
        hash::hashv,
        pubkey::Pubkey,
        signer::{
            Signer as SolanaCurveSigner,
            keypair::{Keypair, write_keypair_file},
        },
    };
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::Arc,
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };
    use submit::SubmitStatus;

    use super::PreparedHotPath;
    use crate::{
        bootstrap::bootstrap,
        config::{
            AccountDependencyConfig, BotConfig, MessageModeConfig,
            OrcaSimplePoolLegExecutionConfig, RaydiumSimplePoolLegExecutionConfig,
            RouteClassConfig, RouteConfig, RouteExecutionConfig, RouteLegConfig,
            RouteLegExecutionConfig, RoutesConfig, SigningProviderKind, SwapSideConfig,
        },
    };

    fn test_signing_material() -> (String, String) {
        let keypair = Keypair::new_from_array([7; 32]);
        (keypair.pubkey().to_string(), keypair.to_base58_string())
    }

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
    }

    fn snapshot_event(sequence: u64, pool_id: &str) -> NormalizedEvent {
        NormalizedEvent::pool_snapshot_update(
            EventSourceKind::Synthetic,
            sequence,
            sequence + 9,
            PoolSnapshotUpdate {
                pool_id: pool_id.into(),
                price_bps: 12_000,
                fee_bps: 4,
                reserve_depth: 100_000,
                reserve_a: Some(100_000),
                reserve_b: Some(100_000),
                active_liquidity: Some(100_000),
                sqrt_price_x64: None,
                venue: PoolVenue::OrcaSimplePool,
                confidence: SnapshotConfidence::Executable,
                repair_pending: Some(false),
                token_mint_a: "SOL".into(),
                token_mint_b: "USDC".into(),
                tick_spacing: 0,
                current_tick_index: None,
                slot: sequence + 9,
                write_version: 1,
            },
        )
    }

    fn route_execution() -> RouteExecutionConfig {
        RouteExecutionConfig {
            message_mode: MessageModeConfig::V0Required,
            lookup_tables: Vec::new(),
            default_compute_unit_limit: 300_000,
            default_compute_unit_price_micro_lamports: 25_000,
            default_jito_tip_lamports: 5_000,
            max_quote_slot_lag: 4,
            max_alt_slot_lag: 4,
        }
    }

    #[derive(Debug)]
    struct PanicSigner {
        pubkey: String,
    }

    impl WalletSigner for PanicSigner {
        fn pubkey_string(&self) -> Result<String, SigningError> {
            Ok(self.pubkey.clone())
        }

        fn is_available(&self) -> bool {
            true
        }

        fn sign(
            &self,
            _wallet: &HotWallet,
            _request: SigningRequest,
        ) -> Result<SignedTransactionEnvelope, SigningError> {
            panic!("prepare_event_for_dispatch must not call sign()");
        }
    }

    fn route_config() -> RouteConfig {
        RouteConfig {
            enabled: true,
            route_class: RouteClassConfig::AmmFastPath,
            route_id: "route-a".into(),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            base_mint: None,
            quote_mint: None,
            sol_quote_conversion_pool_id: None,
            min_trade_size: None,
            default_trade_size: 10_000,
            max_trade_size: 20_000,
            size_ladder: Vec::new(),
            execution_protection: Default::default(),
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
                            pool_destination_token_account: test_pubkey("orca-pool-destination"),
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
            execution: route_execution(),
        }
    }

    #[test]
    fn minimal_pipeline_wiring_reaches_submit() {
        let mut config = BotConfig::default();
        config.routes = RoutesConfig {
            definitions: vec![route_config()],
        };
        config.builder.compute_unit_limit = 1;
        config.builder.compute_unit_price_micro_lamports = 1;
        config.builder.jito_tip_lamports = 1;
        config.runtime.live_set_health.enabled = false;
        config.jito.endpoint = "mock://jito".into();
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        let (owner_pubkey, keypair_base58) = test_signing_material();
        config.signing.owner_pubkey = owner_pubkey;
        config.signing.keypair_base58 = Some(keypair_base58);
        let mut runtime = bootstrap(config).unwrap();

        let first = snapshot_event(1, "pool-a");
        let second = snapshot_event(2, "pool-b");

        let first_report = runtime.process_event(first).unwrap();
        assert!(first_report.submit_result.is_none());

        let second_report = runtime.process_event(second).unwrap();
        let submit = second_report.submit_result.expect("submitted");
        assert_eq!(submit.status, SubmitStatus::Accepted);
        assert!(second_report.execution_record.is_some());
    }

    #[test]
    fn prepare_event_for_dispatch_does_not_invoke_signer_inline() {
        let mut config = BotConfig::default();
        config.routes = RoutesConfig {
            definitions: vec![route_config()],
        };
        config.builder.compute_unit_limit = 1;
        config.builder.compute_unit_price_micro_lamports = 1;
        config.builder.jito_tip_lamports = 1;
        config.runtime.live_set_health.enabled = false;
        config.jito.endpoint = "mock://jito".into();
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        let (owner_pubkey, keypair_base58) = test_signing_material();
        config.signing.owner_pubkey = owner_pubkey.clone();
        config.signing.keypair_base58 = Some(keypair_base58);
        let mut runtime = bootstrap(config).unwrap();
        runtime.hot_path.signer = Arc::new(PanicSigner {
            pubkey: owner_pubkey,
        });

        let first = snapshot_event(1, "pool-a");
        let second = snapshot_event(2, "pool-b");

        let first_report = runtime.prepare_event_for_dispatch(first, 0).unwrap();
        assert!(matches!(first_report, PreparedHotPath::Report(_)));

        let prepared = runtime.prepare_event_for_dispatch(second, 0).unwrap();
        match prepared {
            PreparedHotPath::BuildSign(task) => {
                assert!(task.report.signed_envelope.is_none());
                assert!(task.report.build_result.is_none());
                assert!(task.report.selection.best_candidate.is_some());
            }
            other => panic!("expected build/sign handoff, got {other:?}"),
        }
    }

    #[test]
    fn secure_unix_pipeline_wiring_reaches_submit() {
        let mut config = BotConfig::default();
        config.routes = RoutesConfig {
            definitions: vec![route_config()],
        };
        config.builder.compute_unit_limit = 1;
        config.builder.compute_unit_price_micro_lamports = 1;
        config.builder.jito_tip_lamports = 1;
        config.runtime.live_set_health.enabled = false;
        config.jito.endpoint = "mock://jito".into();
        config.reconciliation.rpc_http_endpoint = "mock://solana-rpc".into();
        config.reconciliation.rpc_ws_endpoint = "mock://solana-ws".into();
        config.signing.provider = SigningProviderKind::SecureUnix;

        let keypair = Keypair::new_from_array([21; 32]);
        let keypair_path = temp_path("runtime-secure-signer", "json");
        let socket_path = temp_path("runtime-secure-signer", "sock");
        write_keypair_file(&keypair, &keypair_path).expect("write keypair file");
        config.signing.owner_pubkey = keypair.pubkey().to_string();
        config.signing.socket_path = Some(socket_path.to_string_lossy().into_owned());

        let service = SecureSignerService::bind(SecureSignerServiceConfig {
            socket_path: socket_path.clone(),
            keypair_path: keypair_path.clone(),
        })
        .expect("bind secure signer service");
        let signer_thread = thread::spawn(move || {
            service.serve_one().expect("serve public key request");
            service.serve_one().expect("serve sign request");
        });

        let mut runtime = bootstrap(config).unwrap();

        let first = snapshot_event(1, "pool-a");
        let second = snapshot_event(2, "pool-b");

        let first_report = runtime.process_event(first).unwrap();
        assert!(first_report.submit_result.is_none());

        let second_report = runtime.process_event(second).unwrap();
        let submit = second_report.submit_result.expect("submitted");
        assert_eq!(submit.status, SubmitStatus::Accepted);
        assert!(second_report.execution_record.is_some());

        signer_thread.join().expect("secure signer thread");
        let _ = fs::remove_file(keypair_path);
        let _ = fs::remove_file(socket_path);
    }

    fn temp_path(label: &str, extension: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        env::temp_dir().join(format!("{label}-{}-{unique}.{extension}", process::id()))
    }
}
