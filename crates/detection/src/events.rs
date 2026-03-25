use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSourceKind {
    ShredStream,
    Replay,
    Synthetic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMetadata {
    pub source: EventSourceKind,
    pub sequence: u64,
    pub observed_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyMetadata {
    pub source_received_at: SystemTime,
    pub normalized_at: SystemTime,
    pub source_latency: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountUpdate {
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub slot: u64,
    pub write_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotBoundary {
    pub slot: u64,
    pub leader: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub slot: u64,
    pub note: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    AccountUpdate(AccountUpdate),
    SlotBoundary(SlotBoundary),
    Heartbeat(Heartbeat),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedEvent {
    pub payload: MarketEvent,
    pub source: SourceMetadata,
    pub latency: LatencyMetadata,
}

impl NormalizedEvent {
    pub fn account_update(
        source: EventSourceKind,
        sequence: u64,
        observed_slot: u64,
        update: AccountUpdate,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            payload: MarketEvent::AccountUpdate(update),
            source: SourceMetadata {
                source,
                sequence,
                observed_slot,
            },
            latency: LatencyMetadata {
                source_received_at: now,
                normalized_at: now,
                source_latency: Duration::ZERO,
            },
        }
    }
}
