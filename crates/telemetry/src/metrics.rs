use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PipelineStage {
    Detect,
    StateApply,
    Quote,
    Select,
    Build,
    Sign,
    Submit,
    Reconcile,
}

#[derive(Debug, Default)]
pub struct PipelineMetrics {
    detect_events: AtomicU64,
    stale_updates: AtomicU64,
    rejection_count: AtomicU64,
    build_count: AtomicU64,
    submit_count: AtomicU64,
    inclusion_count: AtomicU64,
    stage_latency_nanos: Mutex<BTreeMap<PipelineStage, u128>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub detect_events: u64,
    pub stale_updates: u64,
    pub rejection_count: u64,
    pub build_count: u64,
    pub submit_count: u64,
    pub inclusion_count: u64,
    pub stage_latency_nanos: BTreeMap<PipelineStage, u128>,
}

impl PipelineMetrics {
    pub fn record_stage_latency(&self, stage: PipelineStage, duration: Duration) {
        let mut latencies = self.stage_latency_nanos.lock().expect("latencies lock");
        let entry = latencies.entry(stage).or_insert(0);
        *entry += duration.as_nanos();
    }

    pub fn increment_detect(&self) {
        self.detect_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_stale(&self) {
        self.stale_updates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_rejection(&self) {
        self.rejection_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_build(&self) {
        self.build_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_submit(&self) {
        self.submit_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_inclusion(&self) {
        self.inclusion_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            detect_events: self.detect_events.load(Ordering::Relaxed),
            stale_updates: self.stale_updates.load(Ordering::Relaxed),
            rejection_count: self.rejection_count.load(Ordering::Relaxed),
            build_count: self.build_count.load(Ordering::Relaxed),
            submit_count: self.submit_count.load(Ordering::Relaxed),
            inclusion_count: self.inclusion_count.load(Ordering::Relaxed),
            stage_latency_nanos: self
                .stage_latency_nanos
                .lock()
                .expect("latencies lock")
                .clone(),
        }
    }
}
