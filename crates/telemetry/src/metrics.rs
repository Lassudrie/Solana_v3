use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Default)]
struct ShredstreamRateState {
    last_observed_second: u64,
    events_in_second: u64,
}

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
    submit_rejected_count: AtomicU64,
    transport_failed_count: AtomicU64,
    chain_failed_count: AtomicU64,
    expired_count: AtomicU64,
    stage_latency_nanos: Mutex<BTreeMap<PipelineStage, u128>>,
    shredstream_events: AtomicU64,
    shredstream_sequence_gaps: AtomicU64,
    shredstream_sequence_reorders: AtomicU64,
    shredstream_sequence_duplicates: AtomicU64,
    shredstream_ingest_latency_nanos: AtomicU64,
    shredstream_ingest_latency_count: AtomicU64,
    shredstream_interarrival_latency_nanos: AtomicU64,
    shredstream_interarrival_latency_count: AtomicU64,
    shredstream_events_per_second: AtomicU64,
    shredstream_rate_state: Mutex<ShredstreamRateState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub detect_events: u64,
    pub stale_updates: u64,
    pub rejection_count: u64,
    pub build_count: u64,
    pub submit_count: u64,
    pub inclusion_count: u64,
    pub submit_rejected_count: u64,
    pub transport_failed_count: u64,
    pub chain_failed_count: u64,
    pub expired_count: u64,
    pub stage_latency_nanos: BTreeMap<PipelineStage, u128>,
    pub shredstream_events: u64,
    pub shredstream_sequence_gaps: u64,
    pub shredstream_sequence_reorders: u64,
    pub shredstream_sequence_duplicates: u64,
    pub shredstream_ingest_latency_nanos: u64,
    pub shredstream_ingest_latency_count: u64,
    pub shredstream_interarrival_latency_nanos: u64,
    pub shredstream_interarrival_latency_count: u64,
    pub shredstream_events_per_second: u64,
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

    pub fn increment_submit_rejected(&self) {
        self.submit_rejected_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_transport_failed(&self) {
        self.transport_failed_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_chain_failed(&self) {
        self.chain_failed_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_expired(&self) {
        self.expired_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_shredstream_sequence_gap(&self) {
        self.shredstream_sequence_gaps
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_shredstream_sequence_reorder(&self) {
        self.shredstream_sequence_reorders
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_shredstream_sequence_duplicate(&self) {
        self.shredstream_sequence_duplicates
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_shredstream_event(
        &self,
        event_source_timestamp: SystemTime,
        source_received_at: SystemTime,
        previous_event: Option<SystemTime>,
        normalized_latency: Duration,
    ) {
        let observed_ns = as_u128_to_u64(normalized_latency.as_nanos());
        self.shredstream_events.fetch_add(1, Ordering::Relaxed);
        self.shredstream_ingest_latency_nanos
            .fetch_add(observed_ns, Ordering::Relaxed);
        self.shredstream_ingest_latency_count
            .fetch_add(1, Ordering::Relaxed);

        if let Some(previous) = previous_event {
            let interarrival = event_source_timestamp
                .duration_since(previous)
                .unwrap_or_else(|_| Duration::ZERO);
            let interarrival_ns = as_u128_to_u64(interarrival.as_nanos());
            self.shredstream_interarrival_latency_nanos
                .fetch_add(interarrival_ns, Ordering::Relaxed);
            self.shredstream_interarrival_latency_count
                .fetch_add(1, Ordering::Relaxed);
            let mut guard = self
                .shredstream_rate_state
                .lock()
                .expect("shredstream rate lock");
            let _ = source_received_at;
            let now_second = current_observed_second(source_received_at);
            if guard.last_observed_second == now_second {
                guard.events_in_second += 1;
            } else {
                guard.last_observed_second = now_second;
                guard.events_in_second = 1;
            }
            self.shredstream_events_per_second
                .store(guard.events_in_second, Ordering::Relaxed);
        } else {
            let now_second = current_observed_second(source_received_at);
            let mut guard = self
                .shredstream_rate_state
                .lock()
                .expect("shredstream rate lock");
            if guard.last_observed_second != now_second {
                guard.last_observed_second = now_second;
                guard.events_in_second = 1;
            } else {
                guard.events_in_second += 1;
            }
            self.shredstream_events_per_second
                .store(guard.events_in_second, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            detect_events: self.detect_events.load(Ordering::Relaxed),
            stale_updates: self.stale_updates.load(Ordering::Relaxed),
            rejection_count: self.rejection_count.load(Ordering::Relaxed),
            build_count: self.build_count.load(Ordering::Relaxed),
            submit_count: self.submit_count.load(Ordering::Relaxed),
            inclusion_count: self.inclusion_count.load(Ordering::Relaxed),
            submit_rejected_count: self.submit_rejected_count.load(Ordering::Relaxed),
            transport_failed_count: self.transport_failed_count.load(Ordering::Relaxed),
            chain_failed_count: self.chain_failed_count.load(Ordering::Relaxed),
            expired_count: self.expired_count.load(Ordering::Relaxed),
            stage_latency_nanos: self
                .stage_latency_nanos
                .lock()
                .expect("latencies lock")
                .clone(),
            shredstream_events: self.shredstream_events.load(Ordering::Relaxed),
            shredstream_sequence_gaps: self.shredstream_sequence_gaps.load(Ordering::Relaxed),
            shredstream_sequence_reorders: self
                .shredstream_sequence_reorders
                .load(Ordering::Relaxed),
            shredstream_sequence_duplicates: self
                .shredstream_sequence_duplicates
                .load(Ordering::Relaxed),
            shredstream_ingest_latency_nanos: self
                .shredstream_ingest_latency_nanos
                .load(Ordering::Relaxed),
            shredstream_ingest_latency_count: self
                .shredstream_ingest_latency_count
                .load(Ordering::Relaxed),
            shredstream_interarrival_latency_nanos: self
                .shredstream_interarrival_latency_nanos
                .load(Ordering::Relaxed),
            shredstream_interarrival_latency_count: self
                .shredstream_interarrival_latency_count
                .load(Ordering::Relaxed),
            shredstream_events_per_second: self
                .shredstream_events_per_second
                .load(Ordering::Relaxed),
        }
    }
}

fn as_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn current_observed_second(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
