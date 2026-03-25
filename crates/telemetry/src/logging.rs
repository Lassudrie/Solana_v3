use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Default)]
pub struct MemoryLogSink {
    events: Mutex<Vec<StructuredLogEvent>>,
}

impl MemoryLogSink {
    pub fn snapshot(&self) -> Vec<StructuredLogEvent> {
        self.events.lock().expect("log lock").clone()
    }
}

impl LoggingSink for MemoryLogSink {
    fn emit(&self, event: StructuredLogEvent) {
        self.events.lock().expect("log lock").push(event);
    }
}
