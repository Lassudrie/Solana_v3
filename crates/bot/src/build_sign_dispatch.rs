use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    },
    thread,
    time::Duration,
};

use builder::{BuildRejectionReason, BuildResult, BuildStatus};

use crate::runtime::{BuildSignCompletion, BuildSignPipeline, HotPathReport, PreparedExecution};

#[derive(Debug)]
pub(crate) struct BuildSignDispatcher {
    sender: Sender<PreparedExecution>,
    completion_receiver: Receiver<BuildSignCompletion>,
    pending_count: Arc<AtomicUsize>,
    worker_count: usize,
    queue_capacity: usize,
    congestion_threshold_count: usize,
}

#[derive(Debug)]
pub(crate) enum EnqueueError {
    Full(PreparedExecution),
    Disconnected(PreparedExecution),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuildSignDispatcherLoad {
    pub pending: usize,
    pub capacity: usize,
    pub workers: usize,
}

impl BuildSignDispatcher {
    pub(crate) fn new(
        pipeline: BuildSignPipeline,
        worker_count: usize,
        queue_capacity: usize,
        congestion_threshold_pct: u8,
    ) -> Result<Self, std::io::Error> {
        let worker_count = worker_count.max(1);
        let pending_count = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = mpsc::channel::<PreparedExecution>();
        let (completion_sender, completion_receiver) = mpsc::channel::<BuildSignCompletion>();
        let shared_receiver = Arc::new(std::sync::Mutex::new(receiver));

        for index in 0..worker_count {
            let pipeline = pipeline.clone();
            let receiver = Arc::clone(&shared_receiver);
            let completion_sender = completion_sender.clone();
            let pending_count = Arc::clone(&pending_count);
            thread::Builder::new()
                .name(format!("bot-build-sign-worker-{index}"))
                .spawn(move || {
                    build_sign_worker_loop(pipeline, receiver, completion_sender, pending_count)
                })?;
        }

        Ok(Self {
            sender,
            completion_receiver,
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

    pub(crate) fn load(&self) -> BuildSignDispatcherLoad {
        BuildSignDispatcherLoad {
            pending: self.pending_count(),
            capacity: self.outstanding_capacity(),
            workers: self.worker_count,
        }
    }

    pub(crate) fn congestion_load(&self) -> Option<BuildSignDispatcherLoad> {
        let load = self.load();
        (load.pending >= self.congestion_threshold_count).then_some(load)
    }

    pub(crate) fn try_enqueue(&self, task: PreparedExecution) -> Result<(), EnqueueError> {
        if !self.try_reserve_capacity() {
            return Err(EnqueueError::Full(task));
        }
        match self.sender.send(task) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(task)) => {
                release_pending_slot(&self.pending_count);
                Err(EnqueueError::Disconnected(task))
            }
        }
    }

    pub(crate) fn try_next_completion(&self) -> Option<BuildSignCompletion> {
        match self.completion_receiver.try_recv() {
            Ok(completion) => Some(completion),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    pub(crate) fn wait_for_completion(&self, timeout: Duration) -> Option<BuildSignCompletion> {
        match self.completion_receiver.recv_timeout(timeout) {
            Ok(completion) => Some(completion),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => None,
        }
    }

    pub(crate) fn queue_rejection(
        &self,
        task: PreparedExecution,
        reason: BuildRejectionReason,
    ) -> HotPathReport {
        let mut report = task.report;
        report.build_result = Some(BuildResult {
            status: BuildStatus::Rejected,
            envelope: None,
            rejection: Some(reason),
        });
        report
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

fn build_sign_worker_loop(
    pipeline: BuildSignPipeline,
    receiver: Arc<std::sync::Mutex<Receiver<PreparedExecution>>>,
    completion_sender: Sender<BuildSignCompletion>,
    pending_count: Arc<AtomicUsize>,
) {
    loop {
        let task = {
            let receiver = receiver.lock().expect("build/sign receiver");
            receiver.recv()
        };
        let Ok(task) = task else {
            break;
        };
        let completion = pipeline.execute(task);
        release_pending_slot(&pending_count);
        if completion_sender.send(completion).is_err() {
            break;
        }
    }
}

fn release_pending_slot(pending_count: &Arc<AtomicUsize>) {
    pending_count
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(1))
        })
        .ok();
}

fn congestion_threshold_count(worker_count: usize, queue_capacity: usize, pct: u8) -> usize {
    let total = worker_count.saturating_add(queue_capacity).max(1);
    let pct = usize::from(pct.clamp(1, 100));
    total.saturating_mul(pct).div_ceil(100).max(1)
}
