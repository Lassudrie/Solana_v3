use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredLogEvent {
    pub level: LogLevel,
    pub target: String,
    pub message: String,
    pub fields: BTreeMap<String, String>,
    pub emitted_at: SystemTime,
}

pub trait LoggingSink: Send + Sync {
    fn emit(&self, event: StructuredLogEvent);
}

#[derive(Debug)]
pub struct MemoryLogSink {
    max_level: LogLevel,
    max_events: usize,
    dropped_events: AtomicU64,
    events: Mutex<VecDeque<StructuredLogEvent>>,
}

impl MemoryLogSink {
    pub const DEFAULT_MAX_EVENTS: usize = 1024;

    pub fn new(max_level: LogLevel, max_events: usize) -> Self {
        Self {
            max_level,
            max_events,
            dropped_events: AtomicU64::new(0),
            events: Mutex::new(VecDeque::with_capacity(max_events)),
        }
    }

    pub fn enabled(&self, level: LogLevel) -> bool {
        self.max_events > 0 && level >= self.max_level
    }

    pub fn snapshot(&self) -> Vec<StructuredLogEvent> {
        self.events
            .lock()
            .expect("log lock")
            .iter()
            .cloned()
            .collect()
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }
}

impl Default for MemoryLogSink {
    fn default() -> Self {
        Self::new(LogLevel::Info, Self::DEFAULT_MAX_EVENTS)
    }
}

impl LoggingSink for MemoryLogSink {
    fn emit(&self, event: StructuredLogEvent) {
        if !self.enabled(event.level) {
            return;
        }

        let mut events = self.events.lock().expect("log lock");
        if events.len() == self.max_events {
            events.pop_front();
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
        events.push_back(event);
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::{LogLevel, LoggingSink, MemoryLogSink, StructuredLogEvent};

    #[test]
    fn default_sink_ignores_debug_logs() {
        let sink = MemoryLogSink::default();
        sink.emit(StructuredLogEvent {
            level: LogLevel::Debug,
            target: "runtime".into(),
            message: "event received".into(),
            fields: Default::default(),
            emitted_at: UNIX_EPOCH,
        });

        assert!(sink.snapshot().is_empty());
        assert_eq!(sink.dropped_events(), 0);
    }

    #[test]
    fn sink_keeps_a_bounded_log_window() {
        let sink = MemoryLogSink::new(LogLevel::Info, 2);
        for index in 0..3 {
            sink.emit(StructuredLogEvent {
                level: LogLevel::Info,
                target: "runtime".into(),
                message: format!("event-{index}"),
                fields: Default::default(),
                emitted_at: UNIX_EPOCH,
            });
        }

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].message, "event-1");
        assert_eq!(snapshot[1].message, "event-2");
        assert_eq!(sink.dropped_events(), 1);
    }
}
