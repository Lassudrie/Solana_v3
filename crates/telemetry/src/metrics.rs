use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const PIPELINE_STAGE_COUNT: usize = 8;

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

impl PipelineStage {
    pub const ALL: [Self; PIPELINE_STAGE_COUNT] = [
        Self::Detect,
        Self::StateApply,
        Self::Quote,
        Self::Select,
        Self::Build,
        Self::Sign,
        Self::Submit,
        Self::Reconcile,
    ];

    const fn as_index(self) -> usize {
        match self {
            Self::Detect => 0,
            Self::StateApply => 1,
            Self::Quote => 2,
            Self::Select => 3,
            Self::Build => 4,
            Self::Sign => 5,
            Self::Submit => 6,
            Self::Reconcile => 7,
        }
    }
}

#[derive(Debug)]
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
    stage_latency_nanos: [AtomicU64; PIPELINE_STAGE_COUNT],
    shredstream_events: AtomicU64,
    shredstream_sequence_gaps: AtomicU64,
    shredstream_sequence_reorders: AtomicU64,
    shredstream_sequence_duplicates: AtomicU64,
    shredstream_ingest_latency_nanos: AtomicU64,
    shredstream_ingest_latency_count: AtomicU64,
    shredstream_interarrival_latency_nanos: AtomicU64,
    shredstream_interarrival_latency_count: AtomicU64,
    shredstream_events_per_second: AtomicU64,
    shredstream_last_observed_second: AtomicU64,
    shredstream_events_in_second: AtomicU64,
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
        saturating_add_atomic(
            &self.stage_latency_nanos[stage.as_index()],
            as_u128_to_u64(duration.as_nanos()),
        );
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
        event_source_timestamp: Option<SystemTime>,
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

        if let (Some(event_source_timestamp), Some(previous)) =
            (event_source_timestamp, previous_event)
        {
            let interarrival = event_source_timestamp
                .duration_since(previous)
                .unwrap_or_else(|_| Duration::ZERO);
            let interarrival_ns = as_u128_to_u64(interarrival.as_nanos());
            self.shredstream_interarrival_latency_nanos
                .fetch_add(interarrival_ns, Ordering::Relaxed);
            self.shredstream_interarrival_latency_count
                .fetch_add(1, Ordering::Relaxed);
        }

        let now_second = current_observed_second(source_received_at);
        loop {
            let last_second = self
                .shredstream_last_observed_second
                .load(Ordering::Relaxed);
            if last_second == now_second {
                let count = self
                    .shredstream_events_in_second
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                self.shredstream_events_per_second
                    .store(count, Ordering::Relaxed);
                break;
            }

            match self.shredstream_last_observed_second.compare_exchange(
                last_second,
                now_second,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.shredstream_events_in_second
                        .store(1, Ordering::Relaxed);
                    self.shredstream_events_per_second
                        .store(1, Ordering::Relaxed);
                    break;
                }
                Err(observed_second) if observed_second == now_second => continue,
                Err(_) => continue,
            }
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
            stage_latency_nanos: PipelineStage::ALL
                .into_iter()
                .filter_map(|stage| {
                    let latency =
                        self.stage_latency_nanos[stage.as_index()].load(Ordering::Relaxed);
                    (latency > 0).then_some((stage, u128::from(latency)))
                })
                .collect(),
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

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self {
            detect_events: AtomicU64::new(0),
            stale_updates: AtomicU64::new(0),
            rejection_count: AtomicU64::new(0),
            build_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            inclusion_count: AtomicU64::new(0),
            submit_rejected_count: AtomicU64::new(0),
            transport_failed_count: AtomicU64::new(0),
            chain_failed_count: AtomicU64::new(0),
            expired_count: AtomicU64::new(0),
            stage_latency_nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            shredstream_events: AtomicU64::new(0),
            shredstream_sequence_gaps: AtomicU64::new(0),
            shredstream_sequence_reorders: AtomicU64::new(0),
            shredstream_sequence_duplicates: AtomicU64::new(0),
            shredstream_ingest_latency_nanos: AtomicU64::new(0),
            shredstream_ingest_latency_count: AtomicU64::new(0),
            shredstream_interarrival_latency_nanos: AtomicU64::new(0),
            shredstream_interarrival_latency_count: AtomicU64::new(0),
            shredstream_events_per_second: AtomicU64::new(0),
            shredstream_last_observed_second: AtomicU64::new(0),
            shredstream_events_in_second: AtomicU64::new(0),
        }
    }
}

fn as_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn saturating_add_atomic(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(delta))
    });
}

fn current_observed_second(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{PipelineMetrics, PipelineStage};

    #[test]
    fn stage_latencies_are_accumulated_without_zero_entries() {
        let metrics = PipelineMetrics::default();
        metrics.record_stage_latency(PipelineStage::Select, Duration::from_nanos(11));
        metrics.record_stage_latency(PipelineStage::Select, Duration::from_nanos(7));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.stage_latency_nanos.len(), 1);
        assert_eq!(
            snapshot.stage_latency_nanos.get(&PipelineStage::Select),
            Some(&18)
        );
    }

    #[test]
    fn shredstream_rate_uses_current_second_without_mutex_state() {
        let metrics = PipelineMetrics::default();
        let now = UNIX_EPOCH + Duration::from_secs(10);
        metrics.record_shredstream_event(None, now, None, Duration::from_nanos(1));
        metrics.record_shredstream_event(None, now, None, Duration::from_nanos(1));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.shredstream_events, 2);
        assert_eq!(snapshot.shredstream_events_per_second, 2);
    }
}
