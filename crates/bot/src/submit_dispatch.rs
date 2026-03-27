use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use submit::{
    JitoConfig, JitoSubmitter, SubmissionId, SubmitAttemptOutcome, SubmitError, SubmitMode,
    SubmitRejectionReason, SubmitRequest, SubmitResult, SubmitRetryPolicy, SubmitStatus, Submitter,
};

use crate::config::{BotConfig, SubmitModeConfig};
use crate::runtime::HotPathReport;

const API_V1_PATH: &str = "/api/v1";
const TRANSACTION_PATH: &str = "/api/v1/transactions";
const BUNDLE_PATH: &str = "/api/v1/bundles";

#[derive(Debug)]
pub(crate) struct SubmitDispatcher {
    sender: Sender<CoordinatorMessage>,
    completion_receiver: Receiver<CompletedSubmission>,
    submit_mode: SubmitMode,
    endpoint: String,
    pending_count: Arc<AtomicUsize>,
    next_sequence: AtomicU64,
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
    first_enqueued_at: Instant,
    attempt_index: usize,
    sequence: u64,
}

#[derive(Debug)]
enum CoordinatorMessage {
    Enqueue(PendingSubmission),
    AttemptFinished(AttemptCompletion),
}

#[derive(Debug)]
struct AttemptCompletion {
    pending: PendingSubmission,
    finished_at: SystemTime,
    result: Result<SubmitAttemptOutcome, SubmitError>,
}

#[derive(Debug)]
struct DelayedSubmission {
    ready_at: Instant,
    pending: PendingSubmission,
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
        let retry_policy = submitter.retry_policy();
        let (sender, receiver) = mpsc::channel();
        let (worker_sender, worker_receiver) = mpsc::channel();
        let (completion_sender, completion_receiver) = mpsc::channel();
        let shared_worker_receiver = Arc::new(Mutex::new(worker_receiver));

        for index in 0..worker_count {
            let submitter = Arc::clone(&submitter);
            let worker_receiver = Arc::clone(&shared_worker_receiver);
            let sender = sender.clone();
            thread::Builder::new()
                .name(format!("bot-submit-worker-{index}"))
                .spawn(move || submit_worker_loop(submitter, worker_receiver, sender))?;
        }

        thread::Builder::new()
            .name("bot-submit-coordinator".into())
            .spawn(move || {
                submit_coordinator_loop(receiver, worker_sender, completion_sender, retry_policy)
            })?;

        Ok(Self {
            sender,
            completion_receiver,
            submit_mode,
            endpoint,
            pending_count,
            next_sequence: AtomicU64::new(0),
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
        if !self.try_reserve_capacity() {
            return Err(EnqueueError::Full(report));
        }

        let pending = PendingSubmission {
            request: SubmitRequest {
                envelope,
                mode: self.submit_mode,
            },
            report,
            first_enqueued_at: Instant::now(),
            attempt_index: 0,
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
        };
        match self.sender.send(CoordinatorMessage::Enqueue(pending)) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(CoordinatorMessage::Enqueue(pending))) => {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                Err(EnqueueError::Disconnected(pending.report))
            }
            Err(mpsc::SendError(CoordinatorMessage::AttemptFinished(_))) => unreachable!(),
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

    fn try_reserve_capacity(&self) -> bool {
        let capacity = self.outstanding_capacity();
        loop {
            let current = self.pending_count.load(Ordering::Relaxed);
            if current >= capacity {
                return false;
            }
            if self
                .pending_count
                .compare_exchange(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }
}

fn submit_worker_loop(
    submitter: Arc<dyn Submitter>,
    receiver: Arc<Mutex<Receiver<PendingSubmission>>>,
    sender: Sender<CoordinatorMessage>,
) {
    loop {
        let pending = {
            let guard = receiver.lock().expect("submit dispatcher receiver lock");
            guard.recv()
        };
        let Ok(pending) = pending else {
            break;
        };

        let result = submitter.submit_attempt(pending.request.clone());
        let message = CoordinatorMessage::AttemptFinished(AttemptCompletion {
            pending,
            finished_at: SystemTime::now(),
            result,
        });
        if sender.send(message).is_err() {
            break;
        }
    }
}

fn submit_coordinator_loop(
    receiver: Receiver<CoordinatorMessage>,
    worker_sender: Sender<PendingSubmission>,
    completion_sender: Sender<CompletedSubmission>,
    retry_policy: SubmitRetryPolicy,
) {
    let mut delayed = VecDeque::new();

    loop {
        dispatch_ready_retries(&mut delayed, &worker_sender, &completion_sender);

        let timeout = delayed
            .front()
            .map(|delayed| delayed.ready_at.saturating_duration_since(Instant::now()));
        let message = match timeout {
            Some(timeout) => match receiver.recv_timeout(timeout) {
                Ok(message) => Some(message),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            },
            None => match receiver.recv() {
                Ok(message) => Some(message),
                Err(_) => break,
            },
        };

        let Some(message) = message else {
            continue;
        };
        match message {
            CoordinatorMessage::Enqueue(pending) => {
                dispatch_pending_submission(pending, &worker_sender, &completion_sender);
            }
            CoordinatorMessage::AttemptFinished(completion) => {
                handle_attempt_completion(
                    completion,
                    &worker_sender,
                    &completion_sender,
                    retry_policy,
                    &mut delayed,
                );
            }
        }
    }
}

fn dispatch_ready_retries(
    delayed: &mut VecDeque<DelayedSubmission>,
    worker_sender: &Sender<PendingSubmission>,
    completion_sender: &Sender<CompletedSubmission>,
) {
    let now = Instant::now();
    while delayed
        .front()
        .map(|delayed| delayed.ready_at <= now)
        .unwrap_or(false)
    {
        let delayed = delayed.pop_front().expect("delayed retry");
        dispatch_pending_submission(delayed.pending, worker_sender, completion_sender);
    }
}

fn handle_attempt_completion(
    completion: AttemptCompletion,
    worker_sender: &Sender<PendingSubmission>,
    completion_sender: &Sender<CompletedSubmission>,
    retry_policy: SubmitRetryPolicy,
    delayed: &mut VecDeque<DelayedSubmission>,
) {
    let AttemptCompletion {
        mut pending,
        finished_at,
        result,
    } = completion;
    match result {
        Ok(SubmitAttemptOutcome::Terminal(result)) => {
            send_terminal_completion(pending, finished_at, Ok(result), completion_sender);
        }
        Ok(SubmitAttemptOutcome::Retry {
            terminal_result,
            retry_after,
        }) => {
            let max_attempts = retry_policy.max_attempts.max(1);
            if pending.attempt_index + 1 >= max_attempts {
                send_terminal_completion(
                    pending,
                    finished_at,
                    Ok(terminal_result),
                    completion_sender,
                );
                return;
            }

            let delay =
                retry_after.unwrap_or_else(|| retry_policy.retry_delay(pending.attempt_index));
            pending.attempt_index += 1;
            insert_delayed_submission(
                delayed,
                DelayedSubmission {
                    ready_at: Instant::now() + delay,
                    pending,
                },
            );
            dispatch_ready_retries(delayed, worker_sender, completion_sender);
        }
        Err(error) => {
            send_terminal_completion(pending, finished_at, Err(error), completion_sender);
        }
    }
}

fn dispatch_pending_submission(
    pending: PendingSubmission,
    worker_sender: &Sender<PendingSubmission>,
    completion_sender: &Sender<CompletedSubmission>,
) {
    if let Err(error) = worker_sender.send(pending) {
        send_terminal_completion(
            error.0,
            SystemTime::now(),
            Err(SubmitError::TransportUnavailable),
            completion_sender,
        );
    }
}

fn send_terminal_completion(
    pending: PendingSubmission,
    finished_at: SystemTime,
    result: Result<SubmitResult, SubmitError>,
    completion_sender: &Sender<CompletedSubmission>,
) {
    let completion = CompletedSubmission {
        report: pending.report,
        submit_duration: pending.first_enqueued_at.elapsed(),
        finished_at,
        result,
    };
    let _ = completion_sender.send(completion);
}

fn insert_delayed_submission(
    delayed: &mut VecDeque<DelayedSubmission>,
    candidate: DelayedSubmission,
) {
    let insert_at = delayed.iter().position(|existing| {
        candidate.ready_at < existing.ready_at
            || (candidate.ready_at == existing.ready_at
                && candidate.pending.sequence < existing.pending.sequence)
    });
    if let Some(index) = insert_at {
        delayed.insert(index, candidate);
    } else {
        delayed.push_back(candidate);
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
        collections::HashMap,
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
        SubmissionId, SubmitAttemptOutcome, SubmitError, SubmitMode, SubmitRequest, SubmitResult,
        SubmitRetryPolicy, SubmitStatus, Submitter,
    };

    use super::{CompletedSubmission, EnqueueError, SubmitDispatcher, SubmitDispatcherLoad};
    use crate::runtime::{HotPathReport, PipelineTrace};

    #[derive(Debug)]
    struct BlockingSubmitter {
        started: Arc<AtomicUsize>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl Submitter for BlockingSubmitter {
        fn submit_attempt(
            &self,
            request: SubmitRequest,
        ) -> Result<SubmitAttemptOutcome, SubmitError> {
            self.started.fetch_add(1, Ordering::Relaxed);
            self.release
                .lock()
                .expect("release receiver lock")
                .recv()
                .expect("release signal");
            Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId(format!("accepted-{}", request.envelope.signature)),
                endpoint: "mock://jito/api/v1/transactions".into(),
                rejection: None,
            }))
        }
    }

    #[derive(Debug)]
    struct ScriptedSubmitter {
        attempts: Mutex<HashMap<String, usize>>,
        policy: SubmitRetryPolicy,
        retry_signature: String,
        retry_delay: Duration,
    }

    impl Submitter for ScriptedSubmitter {
        fn submit_attempt(
            &self,
            request: SubmitRequest,
        ) -> Result<SubmitAttemptOutcome, SubmitError> {
            let signature = request.envelope.signature.clone();
            let mut attempts = self.attempts.lock().expect("attempts lock");
            let count = attempts.entry(signature.clone()).or_insert(0);
            let attempt = *count;
            *count += 1;
            drop(attempts);

            if signature == self.retry_signature && attempt == 0 {
                return Ok(SubmitAttemptOutcome::Retry {
                    terminal_result: SubmitResult {
                        status: SubmitStatus::Rejected,
                        submission_id: SubmissionId(format!("channel-unavailable-{signature}")),
                        endpoint: "mock://jito/api/v1/transactions".into(),
                        rejection: Some(submit::SubmitRejectionReason::ChannelUnavailable),
                    },
                    retry_after: Some(self.retry_delay),
                });
            }

            Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId(format!("accepted-{signature}-attempt-{attempt}")),
                endpoint: "mock://jito/api/v1/transactions".into(),
                rejection: None,
            }))
        }

        fn retry_policy(&self) -> SubmitRetryPolicy {
            self.policy
        }
    }

    #[derive(Debug)]
    struct AlwaysRetrySubmitter {
        policy: SubmitRetryPolicy,
    }

    impl Submitter for AlwaysRetrySubmitter {
        fn submit_attempt(
            &self,
            request: SubmitRequest,
        ) -> Result<SubmitAttemptOutcome, SubmitError> {
            Ok(SubmitAttemptOutcome::Retry {
                terminal_result: SubmitResult {
                    status: SubmitStatus::Rejected,
                    submission_id: SubmissionId(format!(
                        "channel-unavailable-{}",
                        request.envelope.signature
                    )),
                    endpoint: "mock://jito/api/v1/transactions".into(),
                    rejection: Some(submit::SubmitRejectionReason::ChannelUnavailable),
                },
                retry_after: Some(Duration::ZERO),
            })
        }

        fn retry_policy(&self) -> SubmitRetryPolicy {
            self.policy
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
            Err(EnqueueError::Full(report)) => {
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

    #[test]
    fn delayed_retry_keeps_submission_pending_and_blocks_new_enqueues() {
        let dispatcher = SubmitDispatcher::new(
            Arc::new(ScriptedSubmitter {
                attempts: Mutex::new(HashMap::new()),
                policy: SubmitRetryPolicy {
                    max_attempts: 2,
                    retry_backoff: Duration::ZERO,
                },
                retry_signature: "sig-retry".into(),
                retry_delay: Duration::from_millis(200),
            }),
            SubmitMode::SingleTransaction,
            "mock://jito/api/v1/transactions".into(),
            1,
            0,
            100,
        )
        .expect("dispatcher");

        dispatcher
            .try_enqueue(report("sig-retry"))
            .expect("enqueue retrying submission");

        let deadline = Instant::now() + Duration::from_millis(100);
        while dispatcher.pending_count() == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(20));

        assert_eq!(dispatcher.pending_count(), 1);
        assert!(matches!(
            dispatcher.try_enqueue(report("sig-next")),
            Err(EnqueueError::Full(_))
        ));

        let completion = dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("retry completion");
        assert!(completion.submit_duration >= Duration::from_millis(200));
        assert_eq!(completion.result.unwrap().status, SubmitStatus::Accepted);
    }

    #[test]
    fn retry_does_not_block_worker_on_delayed_backoff() {
        let dispatcher = SubmitDispatcher::new(
            Arc::new(ScriptedSubmitter {
                attempts: Mutex::new(HashMap::new()),
                policy: SubmitRetryPolicy {
                    max_attempts: 2,
                    retry_backoff: Duration::ZERO,
                },
                retry_signature: "sig-retry".into(),
                retry_delay: Duration::from_millis(200),
            }),
            SubmitMode::SingleTransaction,
            "mock://jito/api/v1/transactions".into(),
            1,
            1,
            100,
        )
        .expect("dispatcher");

        dispatcher
            .try_enqueue(report("sig-retry"))
            .expect("enqueue retrying submission");
        dispatcher
            .try_enqueue(report("sig-fast"))
            .expect("enqueue fast submission");

        let first_completion = dispatcher
            .wait_for_completion(Duration::from_millis(100))
            .expect("fast completion");
        assert_eq!(
            first_completion.result.unwrap().submission_id,
            SubmissionId("accepted-sig-fast-attempt-0".into())
        );

        let second_completion = dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("retry completion");
        assert_eq!(
            second_completion.result.unwrap().submission_id,
            SubmissionId("accepted-sig-retry-attempt-1".into())
        );
    }

    #[test]
    fn exhausted_retry_budget_emits_terminal_retry_result() {
        let dispatcher = SubmitDispatcher::new(
            Arc::new(AlwaysRetrySubmitter {
                policy: SubmitRetryPolicy {
                    max_attempts: 2,
                    retry_backoff: Duration::ZERO,
                },
            }),
            SubmitMode::SingleTransaction,
            "mock://jito/api/v1/transactions".into(),
            1,
            1,
            100,
        )
        .expect("dispatcher");

        dispatcher.try_enqueue(report("sig-retry")).unwrap();

        let completion = dispatcher
            .wait_for_completion(Duration::from_secs(1))
            .expect("completion");
        let result = completion.result.unwrap();
        assert_eq!(result.status, SubmitStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(submit::SubmitRejectionReason::ChannelUnavailable)
        );
    }
}
