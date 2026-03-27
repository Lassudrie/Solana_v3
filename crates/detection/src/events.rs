use std::time::SystemTime;

use serde::{Deserialize, Serialize};

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
    pub source_latency: Option<std::time::Duration>,
}

impl LatencyMetadata {
    pub fn source_event_at(&self) -> Option<SystemTime> {
        self.source_latency
            .and_then(|latency| self.source_received_at.checked_sub(latency))
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotConfidence {
    Decoded,
    Verified,
    Executable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSnapshotUpdate {
    pub pool_id: String,
    pub price_bps: u64,
    pub fee_bps: u16,
    pub reserve_depth: u64,
    pub reserve_a: Option<u64>,
    pub reserve_b: Option<u64>,
    pub active_liquidity: Option<u64>,
    pub sqrt_price_x64: Option<u128>,
    pub confidence: SnapshotConfidence,
    pub repair_pending: Option<bool>,
    pub token_mint_a: String,
    pub token_mint_b: String,
    pub tick_spacing: u16,
    pub current_tick_index: Option<i32>,
    pub slot: u64,
    pub write_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolInvalidation {
    pub pool_id: String,
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
    PoolSnapshotUpdate(PoolSnapshotUpdate),
    PoolInvalidation(PoolInvalidation),
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
        Self::with_payload(
            source,
            sequence,
            observed_slot,
            MarketEvent::AccountUpdate(update),
        )
    }

    pub fn pool_snapshot_update(
        source: EventSourceKind,
        sequence: u64,
        observed_slot: u64,
        update: PoolSnapshotUpdate,
    ) -> Self {
        Self::with_payload(
            source,
            sequence,
            observed_slot,
            MarketEvent::PoolSnapshotUpdate(update),
        )
    }

    pub fn pool_invalidation(
        source: EventSourceKind,
        sequence: u64,
        observed_slot: u64,
        invalidation: PoolInvalidation,
    ) -> Self {
        Self::with_payload(
            source,
            sequence,
            observed_slot,
            MarketEvent::PoolInvalidation(invalidation),
        )
    }

    pub fn with_payload(
        source: EventSourceKind,
        sequence: u64,
        observed_slot: u64,
        payload: MarketEvent,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            payload,
            source: SourceMetadata {
                source,
                sequence,
                observed_slot,
            },
            latency: LatencyMetadata {
                source_received_at: now,
                normalized_at: now,
                source_latency: None,
            },
        }
    }
}
