use crate::events::{EventSourceKind, NormalizedEvent};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("event source {kind:?} is exhausted")]
    Exhausted { kind: EventSourceKind },
    #[error("event source {kind:?} failed: {detail}")]
    SourceFailure {
        kind: EventSourceKind,
        detail: String,
    },
}

pub trait MarketEventSource: Send {
    fn source_kind(&self) -> EventSourceKind;
    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError>;
    fn wait_next(&mut self, timeout: Duration) -> Result<Option<NormalizedEvent>, IngestError>;
}
