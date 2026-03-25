use std::time::Instant;

use builder::{BuildRequest, BuildResult, BuildStatus, DynamicBuildParameters, TransactionBuilder};
use detection::NormalizedEvent;
use reconciliation::{ExecutionRecord, ExecutionTracker};
use signing::{HotWallet, SignedTransactionEnvelope, Signer, SigningError, SigningRequest};
use state::{
    StateError, StatePlane,
    types::{AccountUpdateStatus, StateApplyOutcome},
};
use strategy::{StrategyPlane, opportunity::SelectionOutcome};
use submit::{SubmitError, SubmitMode, SubmitRequest, SubmitResult, Submitter};
use telemetry::{LogLevel, PipelineStage, TelemetryStack};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotPathReport {
    pub state_outcome: Option<StateApplyOutcome>,
    pub selection: SelectionOutcome,
    pub build_result: Option<BuildResult>,
    pub signed_envelope: Option<SignedTransactionEnvelope>,
    pub submit_result: Option<SubmitResult>,
    pub execution_record: Option<ExecutionRecord>,
}

impl HotPathReport {
    fn empty(state_outcome: Option<StateApplyOutcome>) -> Self {
        Self {
            state_outcome,
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
            },
            build_result: None,
            signed_envelope: None,
            submit_result: None,
            execution_record: None,
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

pub struct HotPathPipeline {
    state: StatePlane,
    strategy: StrategyPlane,
    builder: builder::AtomicArbTransactionBuilder,
    wallet: HotWallet,
    signer: signing::LocalWalletSigner,
    submitter: Box<dyn Submitter>,
}

impl HotPathPipeline {
    pub fn new(
        state: StatePlane,
        strategy: StrategyPlane,
        builder: builder::AtomicArbTransactionBuilder,
        wallet: HotWallet,
        signer: signing::LocalWalletSigner,
        submitter: Box<dyn Submitter>,
    ) -> Self {
        Self {
            state,
            strategy,
            builder,
            wallet,
            signer,
            submitter,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletRefreshService {
    pub balance_lamports: u64,
    pub ready: bool,
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
        hot_path.state.execution_state_mut().set_blockhash(
            self.blockhash.current_blockhash.clone(),
            self.blockhash.slot,
        );
        hot_path
            .state
            .execution_state_mut()
            .set_alt_revision(self.alt_cache.revision);
        hot_path.state.execution_state_mut().set_wallet_state(
            self.wallet_refresh.balance_lamports,
            self.wallet_refresh.ready,
        );
        hot_path.wallet.balance_lamports = self.wallet_refresh.balance_lamports;
        hot_path.wallet.status = if self.wallet_refresh.ready {
            signing::WalletStatus::Ready
        } else {
            signing::WalletStatus::Refreshing
        };
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
}

impl BotRuntime {
    pub fn new(
        hot_path: HotPathPipeline,
        cold_path: ColdPathServices,
        submit_mode: SubmitMode,
        compute_unit_limit: u32,
        compute_unit_price_micro_lamports: u64,
        jito_tip_lamports: u64,
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
        }
    }

    pub fn apply_cold_path_seed(&mut self) {
        self.cold_path.seed_hot_path(&mut self.hot_path);
        self.hot_path
            .state
            .set_latest_slot(self.cold_path.blockhash.slot);
    }

    pub fn telemetry(&self) -> &TelemetryStack {
        &self.telemetry
    }

    pub fn execution_state(&self) -> state::types::ExecutionStateSnapshot {
        self.hot_path.state.execution_state()
    }

    pub fn latest_slot(&self) -> u64 {
        self.hot_path.state.latest_slot()
    }

    pub fn route_count(&self) -> usize {
        self.hot_path.state.route_count()
    }

    pub fn ready_route_count(&self) -> usize {
        self.hot_path.state.ready_route_count()
    }

    pub fn pending_submissions(&self) -> usize {
        self.tracker.pending_count()
    }

    pub fn apply_kill_switch(&mut self, enabled: bool) {
        self.hot_path
            .state
            .execution_state_mut()
            .set_kill_switch(enabled);
    }

    pub fn process_event(&mut self, event: NormalizedEvent) -> Result<HotPathReport, RuntimeError> {
        self.telemetry.metrics.increment_detect();
        self.telemetry
            .log(LogLevel::Debug, "runtime", "event received");

        let state_started = Instant::now();
        let state_outcome = self.hot_path.state.apply_event(&event)?;
        self.telemetry
            .record_stage(PipelineStage::StateApply, state_started.elapsed());

        if let Some(outcome) = &state_outcome {
            if outcome.update_status == AccountUpdateStatus::StaleRejected {
                self.telemetry.metrics.increment_stale();
                return Ok(HotPathReport::empty(state_outcome));
            }
        } else {
            return Ok(HotPathReport::empty(None));
        }

        let state_outcome = state_outcome.expect("checked above");
        if state_outcome.impacted_routes.is_empty() {
            return Ok(HotPathReport::empty(Some(state_outcome)));
        }

        let strategy_started = Instant::now();
        let selection = self.hot_path.strategy.evaluate(
            &self.hot_path.state,
            &state_outcome.impacted_routes,
            self.tracker.pending_count(),
        );
        self.telemetry
            .record_stage(PipelineStage::Select, strategy_started.elapsed());
        if selection.best_candidate.is_none() {
            self.telemetry.metrics.increment_rejection();
            return Ok(HotPathReport {
                state_outcome: Some(state_outcome),
                selection,
                build_result: None,
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
            });
        }

        let candidate = selection
            .best_candidate
            .clone()
            .expect("candidate available");
        let execution_state = self.hot_path.state.execution_state();
        let build_started = Instant::now();
        let build_result = self.hot_path.builder.build(BuildRequest {
            candidate,
            dynamic: DynamicBuildParameters {
                recent_blockhash: execution_state.latest_blockhash.unwrap_or_default(),
                compute_unit_limit: self.compute_unit_limit,
                compute_unit_price_micro_lamports: self.compute_unit_price_micro_lamports,
                jito_tip_lamports: self.jito_tip_lamports,
                alt_revision: execution_state.alt_revision,
            },
        });
        self.telemetry
            .record_stage(PipelineStage::Build, build_started.elapsed());
        if build_result.status != BuildStatus::Built {
            self.telemetry.metrics.increment_rejection();
            return Ok(HotPathReport {
                state_outcome: Some(state_outcome),
                selection,
                build_result: Some(build_result),
                signed_envelope: None,
                submit_result: None,
                execution_record: None,
            });
        }

        self.telemetry.metrics.increment_build();
        let unsigned_envelope = build_result.envelope.clone().expect("built envelope");
        let sign_started = Instant::now();
        let signed_envelope = self.hot_path.signer.sign(
            &self.hot_path.wallet,
            SigningRequest {
                envelope: unsigned_envelope,
            },
        )?;
        self.telemetry
            .record_stage(PipelineStage::Sign, sign_started.elapsed());

        let submit_started = Instant::now();
        let submit_result = self.hot_path.submitter.submit(SubmitRequest {
            envelope: signed_envelope.clone(),
            mode: self.submit_mode,
        })?;
        self.telemetry
            .record_stage(PipelineStage::Submit, submit_started.elapsed());
        self.telemetry.metrics.increment_submit();

        let execution_record = self
            .tracker
            .register_submission(signed_envelope.route_id.clone(), submit_result.clone());

        Ok(HotPathReport {
            state_outcome: Some(state_outcome),
            selection,
            build_result: Some(build_result),
            signed_envelope: Some(signed_envelope),
            submit_result: Some(submit_result),
            execution_record: Some(execution_record),
        })
    }
}

#[cfg(test)]
mod tests {
    use detection::{AccountUpdate, EventSourceKind, NormalizedEvent};
    use submit::SubmitStatus;

    use crate::{
        bootstrap::bootstrap,
        config::{
            AccountDependencyConfig, BotConfig, RouteConfig, RouteLegConfig, RoutesConfig,
            SwapSideConfig,
        },
    };

    fn encode_pool(price_bps: u64, fee_bps: u16, reserve_depth: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&price_bps.to_le_bytes());
        data.extend_from_slice(&fee_bps.to_le_bytes());
        data.extend_from_slice(&reserve_depth.to_le_bytes());
        data
    }

    fn route_config() -> RouteConfig {
        RouteConfig {
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
        }
    }

    #[test]
    fn minimal_pipeline_wiring_reaches_submit() {
        let mut config = BotConfig::default();
        config.routes = RoutesConfig {
            definitions: vec![route_config()],
        };
        let mut runtime = bootstrap(config);

        let first = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            1,
            10,
            AccountUpdate {
                pubkey: "acct-a".into(),
                owner: "owner".into(),
                lamports: 0,
                data: encode_pool(10_150, 4, 100_000),
                slot: 10,
                write_version: 1,
            },
        );
        let second = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            2,
            11,
            AccountUpdate {
                pubkey: "acct-b".into(),
                owner: "owner".into(),
                lamports: 0,
                data: encode_pool(10_080, 4, 100_000),
                slot: 11,
                write_version: 1,
            },
        );

        let first_report = runtime.process_event(first).unwrap();
        assert!(first_report.submit_result.is_none());

        let second_report = runtime.process_event(second).unwrap();
        let submit = second_report.submit_result.expect("submitted");
        assert_eq!(submit.status, SubmitStatus::Accepted);
        assert!(second_report.execution_record.is_some());
    }
}
