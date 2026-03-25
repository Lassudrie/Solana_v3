pub mod logging;
pub mod metrics;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

pub use logging::{LogLevel, LoggingSink, MemoryLogSink, StructuredLogEvent};
pub use metrics::{MetricsSnapshot, PipelineMetrics, PipelineStage};

#[derive(Debug, Default)]
pub struct TelemetryStack {
    pub metrics: PipelineMetrics,
    pub logs: MemoryLogSink,
}

impl TelemetryStack {
    pub fn record_stage(&self, stage: PipelineStage, duration: Duration) {
        self.metrics.record_stage_latency(stage, duration);
    }

    pub fn log(&self, level: LogLevel, target: &str, message: impl Into<String>) {
        self.logs.emit(StructuredLogEvent {
            level,
            target: target.into(),
            message: message.into(),
            fields: BTreeMap::new(),
            emitted_at: SystemTime::now(),
        });
    }
}
