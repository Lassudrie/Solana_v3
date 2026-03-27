use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use submit::{
    JitoConfig, JitoSubmitter, SubmissionId, SubmitError, SubmitMode, SubmitRejectionReason,
    SubmitRequest, SubmitResult, SubmitStatus, Submitter,
};

use crate::config::{BotConfig, SubmitModeConfig};
use crate::runtime::HotPathReport;

const API_V1_PATH: &str = "/api/v1";
const TRANSACTION_PATH: &str = "/api/v1/transactions";
const BUNDLE_PATH: &str = "/api/v1/bundles";

#[derive(Debug)]
pub(crate) struct SubmitDispatcher {
    sender: SyncSender<PendingSubmission>,
    completion_receiver: Receiver<CompletedSubmission>,
    submit_mode: SubmitMode,
    endpoint: String,
    pending_count: Arc<AtomicUsize>,
    worker_count: usize,
    queue_capacity: usize,
    congestion_threshold_count: usize,
}

#[derive(Debug)]
pub(crate) enum EnqueueError {
    Full(HotPathReport),
    Disconnected(HotPathReport),
}

#[derive(Debug)]
pub(crate) struct CompletedSubmission {
    pub report: HotPathReport,
    pub submit_duration: Duration,
    pub finished_at: SystemTime,
    pub result: Result<SubmitResult, SubmitError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubmitDispatcherLoad {
    pub pending: usize,
    pub capacity: usize,
    pub workers: usize,
}

#[derive(Debug)]
struct PendingSubmission {
    report: HotPathReport,
    request: SubmitRequest,
}

impl SubmitDispatcher {
    pub(crate) fn from_config(config: &BotConfig) -> Result<Self, std::io::Error> {
        let submit_mode = match config.submit.mode {
            SubmitModeConfig::SingleTransaction => SubmitMode::SingleTransaction,
            SubmitModeConfig::Bundle => SubmitMode::Bundle,
        };
        let submitter: Arc<dyn Submitter> = Arc::new(JitoSubmitter::new(JitoConfig {
            endpoint: config.jito.endpoint.clone(),
            ws_endpoint: config.jito.ws_endpoint.clone(),
            auth_token: config.jito.auth_token.clone(),
            bundle_enabled: config.jito.bundle_enabled,
            connect_timeout_ms: config.jito.connect_timeout_ms,
            request_timeout_ms: config.jito.request_timeout_ms,
            retry_attempts: config.jito.retry_attempts,
            retry_backoff_ms: config.jito.retry_backoff_ms,
            idempotency_cache_size: config.jito.idempotency_cache_size,
        }));
        let endpoint = request_url(&config.jito.endpoint, submit_mode);
        Self::new(
            submitter,
            submit_mode,
            endpoint,
            config.submit.worker_count,
            config.submit.queue_capacity,
            config.submit.congestion_threshold_pct,
        )
    }

    fn new(
        submitter: Arc<dyn Submitter>,
        submit_mode: SubmitMode,
        endpoint: String,
        worker_count: usize,
        queue_capacity: usize,
        congestion_threshold_pct: u8,
    ) -> Result<Self, std::io::Error> {
        let worker_count = worker_count.max(1);
        let pending_count = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let (completion_sender, completion_receiver) = mpsc::channel();
        let shared_receiver = Arc::new(Mutex::new(receiver));

        for index in 0..worker_count {
            let submitter = Arc::clone(&submitter);
            let receiver = Arc::clone(&shared_receiver);
            let completion_sender = completion_sender.clone();
            thread::Builder::new()
                .name(format!("bot-submit-worker-{index}"))
                .spawn(move || submit_worker_loop(submitter, receiver, completion_sender))?;
        }

        Ok(Self {
            sender,
            completion_receiver,
            submit_mode,
            endpoint,
            pending_count,
            worker_count,
            queue_capacity,
            congestion_threshold_count: congestion_threshold_count(
                worker_count,
                queue_capacity,
                congestion_threshold_pct,
            ),
        })
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    pub(crate) fn load(&self) -> SubmitDispatcherLoad {
        SubmitDispatcherLoad {
            pending: self.pending_count(),
            capacity: self.outstanding_capacity(),
            workers: self.worker_count,
        }
    }

    pub(crate) fn congestion_load(&self) -> Option<SubmitDispatcherLoad> {
        let load = self.load();
        (load.pending >= self.congestion_threshold_count).then_some(load)
    }

    pub(crate) fn try_enqueue(&self, report: HotPathReport) -> Result<(), EnqueueError> {
        let Some(envelope) = report.signed_envelope.clone() else {
            return Err(EnqueueError::Disconnected(report));
        };
        let pending = PendingSubmission {
            request: SubmitRequest {
                envelope,
                mode: self.submit_mode,
            },
            report,
        };
        match self.sender.try_send(pending) {
            Ok(()) => {
                self.pending_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(pending)) => Err(EnqueueError::Full(pending.report)),
            Err(TrySendError::Disconnected(pending)) => {
                Err(EnqueueError::Disconnected(pending.report))
            }
        }
    }

    pub(crate) fn try_next_completion(&self) -> Option<CompletedSubmission> {
        match self.completion_receiver.try_recv() {
            Ok(completion) => {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                Some(completion)
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    pub(crate) fn wait_for_completion(&self, timeout: Duration) -> Option<CompletedSubmission> {
        match self.completion_receiver.recv_timeout(timeout) {
            Ok(completion) => {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                Some(completion)
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => None,
        }
    }

    pub(crate) fn queue_rejection(
        &self,
        report: &HotPathReport,
        reason: SubmitRejectionReason,
    ) -> SubmitResult {
        let signature = report
            .signed_envelope
            .as_ref()
            .map(|envelope| envelope.signature.as_str())
            .unwrap_or("unknown");
        SubmitResult {
            status: SubmitStatus::Rejected,
            submission_id: SubmissionId(format!(
                "dispatch-{}-{}",
                rejection_code(&reason),
                signature_component(signature),
            )),
            endpoint: self.endpoint.clone(),
            rejection: Some(reason),
        }
    }

    fn outstanding_capacity(&self) -> usize {
        self.queue_capacity.saturating_add(self.worker_count)
    }
}

fn submit_worker_loop(
    submitter: Arc<dyn Submitter>,
    receiver: Arc<Mutex<Receiver<PendingSubmission>>>,
    completion_sender: mpsc::Sender<CompletedSubmission>,
) {
    loop {
        let pending = {
            let guard = receiver.lock().expect("submit dispatcher receiver lock");
            guard.recv()
        };
        let Ok(pending) = pending else {
            break;
        };
        let submit_started = Instant::now();
        let result = submitter.submit(pending.request);
        let completion = CompletedSubmission {
            report: pending.report,
            submit_duration: submit_started.elapsed(),
            finished_at: SystemTime::now(),
            result,
        };
        if completion_sender.send(completion).is_err() {
            break;
        }
    }
}

fn request_url(endpoint: &str, mode: SubmitMode) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    match mode {
        SubmitMode::SingleTransaction => {
            if endpoint.ends_with(TRANSACTION_PATH) {
                endpoint.to_owned()
            } else if endpoint.ends_with(API_V1_PATH) {
                format!("{endpoint}/transactions")
            } else {
                format!("{endpoint}{TRANSACTION_PATH}")
            }
        }
        SubmitMode::Bundle => {
            if endpoint.ends_with(BUNDLE_PATH) {
                endpoint.to_owned()
            } else if endpoint.ends_with(API_V1_PATH) {
                format!("{endpoint}/bundles")
            } else {
                format!("{endpoint}{BUNDLE_PATH}")
            }
        }
    }
}

fn congestion_threshold_count(
    worker_count: usize,
    queue_capacity: usize,
    congestion_threshold_pct: u8,
) -> usize {
    let capacity = queue_capacity.saturating_add(worker_count).max(1);
    let pct = usize::from(congestion_threshold_pct.clamp(1, 100));
    capacity
        .saturating_mul(pct)
        .saturating_add(99)
        .checked_div(100)
        .unwrap_or(capacity)
        .max(1)
}

fn rejection_code(reason: &SubmitRejectionReason) -> &'static str {
    match reason {
        SubmitRejectionReason::InvalidEnvelope => "invalid-envelope",
        SubmitRejectionReason::InvalidRequest => "invalid-request",
        SubmitRejectionReason::BundleDisabled => "bundle-disabled",
        SubmitRejectionReason::Unauthorized => "unauthorized",
        SubmitRejectionReason::RateLimited => "rate-limited",
        SubmitRejectionReason::DuplicateSubmission => "duplicate-submission",
        SubmitRejectionReason::TipTooLow => "tip-too-low",
        SubmitRejectionReason::PathCongested => "path-congested",
        SubmitRejectionReason::ChannelUnavailable => "channel-unavailable",
        SubmitRejectionReason::RemoteRejected => "remote-rejected",
    }
}

fn signature_component(signature: &str) -> &str {
    const MAX: usize = 16;
    let end = signature
        .char_indices()
        .nth(MAX)
        .map(|(index, _)| index)
        .unwrap_or(signature.len());
    &signature[..end]
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant, SystemTime},
    };

    use detection::EventSourceKind;
    use state::types::RouteId;
    use strategy::opportunity::SelectionOutcome;
    use submit::{
        SubmissionId, SubmitError, SubmitMode, SubmitRequest, SubmitResult, SubmitStatus, Submitter,
    };

    use super::{CompletedSubmission, SubmitDispatcher, SubmitDispatcherLoad};
    use crate::runtime::{HotPathReport, PipelineTrace};

    #[derive(Debug)]
    struct BlockingSubmitter {
        started: Arc<AtomicUsize>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl Submitter for BlockingSubmitter {
        fn submit(&self, request: SubmitRequest) -> Result<SubmitResult, SubmitError> {
            self.started.fetch_add(1, Ordering::Relaxed);
            self.release
                .lock()
                .expect("release receiver lock")
                .recv()
                .expect("release signal");
            Ok(SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId(format!("accepted-{}", request.envelope.signature)),
                endpoint: "mock://jito/api/v1/transactions".into(),
                rejection: None,
            })
        }
    }

    fn report(signature: &str) -> HotPathReport {
        HotPathReport {
            state_outcome: None,
            pool_snapshots: Vec::new(),
            selection: SelectionOutcome {
                decisions: Vec::new(),
                best_candidate: None,
            },
            build_result: None,
            signed_envelope: Some(signing::SignedTransactionEnvelope {
                route_id: RouteId("route-a".into()),
                recent_blockhash: "blockhash".into(),
                signature: signature.into(),
                signer_id: "wallet".into(),
                signed_message: vec![1, 2, 3],
                build_slot: 42,
                signed_at: SystemTime::now(),
            }),
            submit_result: None,
            execution_record: None,
            pipeline_trace: PipelineTrace {
                source: EventSourceKind::Synthetic,
                source_sequence: 1,
                observed_slot: 42,
                source_received_at: SystemTime::now(),
                normalized_at: SystemTime::now(),
                source_latency: None,
                ingest_duration: Duration::ZERO,
                queue_wait_duration: Duration::ZERO,
                state_apply_duration: Duration::ZERO,
                select_duration: None,
                build_duration: None,
                sign_duration: None,
                submit_duration: None,
                total_to_submit: None,
            },
        }
    }

    #[test]
    fn queue_rejection_uses_submit_endpoint() {
        let mut config = crate::config::BotConfig::default();
        config.jito.endpoint = "https://mainnet.block-engine.jito.wtf".into();
        let dispatcher = SubmitDispatcher::from_config(&config).unwrap();
        let rejection = dispatcher.queue_rejection(
            &report("sig-1"),
            submit::SubmitRejectionReason::ChannelUnavailable,
        );

        assert_eq!(rejection.status, SubmitStatus::Rejected);
        assert_eq!(
            rejection.endpoint,
            "https://mainnet.block-engine.jito.wtf/api/v1/transactions"
        );
        assert_eq!(
            rejection.submission_id,
            SubmissionId("dispatch-channel-unavailable-sig-1".into())
        );
    }

    #[test]
    fn dispatcher_processes_completion_and_updates_pending_count() {
        let mut config = crate::config::BotConfig::default();
        config.jito.endpoint = "mock://jito".into();
        config.jito.ws_endpoint = "mock://jito-tip-stream".into();
        let dispatcher = SubmitDispatcher::from_config(&config).unwrap();

        dispatcher.try_enqueue(report("sig-2")).unwrap();
        assert_eq!(dispatcher.pending_count(), 1);

        let CompletedSubmission { result, .. } = dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("completion");
        assert_eq!(dispatcher.pending_count(), 0);
        assert_eq!(result.unwrap().status, SubmitStatus::Accepted);
    }

    #[test]
    fn dispatcher_reports_congestion_and_rejects_when_capacity_is_exhausted() {
        let started = Arc::new(AtomicUsize::new(0));
        let (release_sender, release_receiver) = mpsc::channel();
        let dispatcher = SubmitDispatcher::new(
            Arc::new(BlockingSubmitter {
                started: Arc::clone(&started),
                release: Arc::new(Mutex::new(release_receiver)),
            }),
            SubmitMode::SingleTransaction,
            "mock://jito/api/v1/transactions".into(),
            1,
            1,
            100,
        )
        .expect("dispatcher");

        dispatcher
            .try_enqueue(report("sig-1"))
            .expect("first enqueue");
        let deadline = Instant::now() + Duration::from_secs(1);
        while started.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(started.load(Ordering::Relaxed), 1);

        dispatcher
            .try_enqueue(report("sig-2"))
            .expect("second enqueue");

        assert_eq!(
            dispatcher.congestion_load(),
            Some(SubmitDispatcherLoad {
                pending: 2,
                capacity: 2,
                workers: 1,
            })
        );

        let rejected = match dispatcher.try_enqueue(report("sig-3")) {
            Err(super::EnqueueError::Full(report)) => {
                dispatcher.queue_rejection(&report, submit::SubmitRejectionReason::PathCongested)
            }
            other => panic!("expected full queue, got {other:?}"),
        };
        assert_eq!(
            rejected.rejection,
            Some(submit::SubmitRejectionReason::PathCongested)
        );
        assert_eq!(
            rejected.submission_id,
            SubmissionId("dispatch-path-congested-sig-3".into())
        );

        release_sender.send(()).expect("release first");
        release_sender.send(()).expect("release second");

        dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("first completion");
        dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("second completion");
        assert_eq!(dispatcher.pending_count(), 0);
    }
}
