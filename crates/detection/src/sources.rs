use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::UdpSocket,
    path::Path,
    time::{Duration, SystemTime},
};

use serde::Deserialize;

use domain::PoolVenue;

use crate::{
    AccountUpdate, EventSourceKind, Heartbeat, IngestError, LatencyMetadata, MarketEvent,
    MarketEventSource, NormalizedEvent, PoolInvalidation, PoolSnapshotUpdate, SlotBoundary,
    SnapshotConfidence, SourceMetadata,
};

#[derive(Debug)]
pub struct JsonlFileEventSource {
    lines: std::io::Lines<BufReader<File>>,
    next_sequence: u64,
}

impl JsonlFileEventSource {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            lines: BufReader::new(file).lines(),
            next_sequence: 1,
        })
    }
}

impl MarketEventSource for JsonlFileEventSource {
    fn source_kind(&self) -> EventSourceKind {
        EventSourceKind::Replay
    }

    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError> {
        loop {
            let Some(line) = self.lines.next() else {
                return Err(IngestError::Exhausted {
                    kind: self.source_kind(),
                });
            };
            let line = line.map_err(|error| IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: error.to_string(),
            })?;
            if line.trim().is_empty() {
                continue;
            }
            return parse_wire_event(&line, self.source_kind(), &mut self.next_sequence)
                .map(Some)
                .map_err(|detail| IngestError::SourceFailure {
                    kind: self.source_kind(),
                    detail,
                });
        }
    }

    fn wait_next(&mut self, timeout: Duration) -> Result<Option<NormalizedEvent>, IngestError> {
        let _ = timeout;
        self.poll_next()
    }
}

#[derive(Debug)]
pub struct UdpJsonEventSource {
    socket: UdpSocket,
    next_sequence: u64,
}

impl UdpJsonEventSource {
    pub fn bind(bind_address: &str) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(bind_address)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            next_sequence: 1,
        })
    }
}

impl MarketEventSource for UdpJsonEventSource {
    fn source_kind(&self) -> EventSourceKind {
        EventSourceKind::ShredStream
    }

    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError> {
        let mut buffer = [0u8; 65_535];
        match self.socket.recv(&mut buffer) {
            Ok(bytes) => {
                let payload = String::from_utf8_lossy(&buffer[..bytes]);
                parse_wire_event(&payload, self.source_kind(), &mut self.next_sequence)
                    .map(Some)
                    .map_err(|detail| IngestError::SourceFailure {
                        kind: self.source_kind(),
                        detail,
                    })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: error.to_string(),
            }),
        }
    }

    fn wait_next(&mut self, timeout: Duration) -> Result<Option<NormalizedEvent>, IngestError> {
        if timeout.is_zero() {
            return self.poll_next();
        }

        let kind = self.source_kind();
        self.socket
            .set_nonblocking(false)
            .map_err(|error| IngestError::SourceFailure {
                kind,
                detail: error.to_string(),
            })?;
        self.socket
            .set_read_timeout(Some(timeout))
            .map_err(|error| IngestError::SourceFailure {
                kind,
                detail: error.to_string(),
            })?;

        let mut buffer = [0u8; 65_535];
        let recv_result = self.socket.recv(&mut buffer);

        let restore_timeout = self.socket.set_read_timeout(None);
        let restore_nonblocking = self.socket.set_nonblocking(true);
        if let Err(error) = restore_timeout.and(restore_nonblocking) {
            return Err(IngestError::SourceFailure {
                kind,
                detail: error.to_string(),
            });
        }

        match recv_result {
            Ok(bytes) => {
                let payload = String::from_utf8_lossy(&buffer[..bytes]);
                parse_wire_event(&payload, kind, &mut self.next_sequence)
                    .map(Some)
                    .map_err(|detail| IngestError::SourceFailure { kind, detail })
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(IngestError::SourceFailure {
                kind,
                detail: error.to_string(),
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    AccountUpdate {
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        pubkey: String,
        owner: String,
        lamports: u64,
        data_hex: String,
        slot: u64,
        write_version: u64,
    },
    PoolSnapshotUpdate {
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        pool_id: String,
        price_bps: u64,
        fee_bps: u16,
        reserve_depth: u64,
        reserve_a: Option<u64>,
        reserve_b: Option<u64>,
        active_liquidity: Option<u64>,
        sqrt_price_x64: Option<u128>,
        venue: PoolVenue,
        confidence: Option<SnapshotConfidence>,
        #[serde(default)]
        exact: Option<bool>,
        repair_pending: Option<bool>,
        token_mint_a: String,
        token_mint_b: String,
        tick_spacing: u16,
        current_tick_index: Option<i32>,
        slot: u64,
        write_version: u64,
    },
    PoolInvalidation {
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        pool_id: String,
        slot: u64,
    },
    SlotBoundary {
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        slot: u64,
        leader: Option<String>,
    },
    Heartbeat {
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        slot: u64,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireSourceKind {
    #[serde(alias = "shredstream")]
    ShredStream,
    Replay,
    Synthetic,
}

impl From<WireSourceKind> for EventSourceKind {
    fn from(value: WireSourceKind) -> Self {
        match value {
            WireSourceKind::ShredStream => EventSourceKind::ShredStream,
            WireSourceKind::Replay => EventSourceKind::Replay,
            WireSourceKind::Synthetic => EventSourceKind::Synthetic,
        }
    }
}

fn parse_wire_event(
    input: &str,
    default_kind: EventSourceKind,
    next_sequence: &mut u64,
) -> Result<NormalizedEvent, String> {
    let event: WireEvent = serde_json::from_str(input).map_err(|error| error.to_string())?;
    match event {
        WireEvent::AccountUpdate {
            source,
            sequence,
            observed_slot,
            pubkey,
            owner,
            lamports,
            data_hex,
            slot,
            write_version,
        } => Ok(normalized_event(
            source,
            sequence,
            observed_slot.unwrap_or(slot),
            default_kind,
            MarketEvent::AccountUpdate(AccountUpdate {
                pubkey,
                owner,
                lamports,
                data: decode_hex(&data_hex)?,
                slot,
                write_version,
            }),
            next_sequence,
        )),
        WireEvent::PoolSnapshotUpdate {
            source,
            sequence,
            observed_slot,
            pool_id,
            price_bps,
            fee_bps,
            reserve_depth,
            reserve_a,
            reserve_b,
            active_liquidity,
            sqrt_price_x64,
            venue,
            confidence,
            exact,
            repair_pending,
            token_mint_a,
            token_mint_b,
            tick_spacing,
            current_tick_index,
            slot,
            write_version,
        } => Ok(normalized_event(
            source,
            sequence,
            observed_slot.unwrap_or(slot),
            default_kind,
            MarketEvent::PoolSnapshotUpdate(PoolSnapshotUpdate {
                pool_id,
                price_bps,
                fee_bps,
                reserve_depth,
                reserve_a,
                reserve_b,
                active_liquidity,
                sqrt_price_x64,
                venue,
                confidence: confidence
                    .or_else(|| {
                        exact.map(|value| {
                            if value {
                                SnapshotConfidence::Executable
                            } else {
                                SnapshotConfidence::Decoded
                            }
                        })
                    })
                    .ok_or_else(|| {
                        "pool_snapshot_update requires confidence or legacy exact".to_string()
                    })?,
                repair_pending,
                token_mint_a,
                token_mint_b,
                tick_spacing,
                current_tick_index,
                slot,
                write_version,
            }),
            next_sequence,
        )),
        WireEvent::PoolInvalidation {
            source,
            sequence,
            observed_slot,
            pool_id,
            slot,
        } => Ok(normalized_event(
            source,
            sequence,
            observed_slot.unwrap_or(slot),
            default_kind,
            MarketEvent::PoolInvalidation(PoolInvalidation { pool_id }),
            next_sequence,
        )),
        WireEvent::SlotBoundary {
            source,
            sequence,
            observed_slot,
            slot,
            leader,
        } => Ok(normalized_event(
            source,
            sequence,
            observed_slot.unwrap_or(slot),
            default_kind,
            MarketEvent::SlotBoundary(SlotBoundary { slot, leader }),
            next_sequence,
        )),
        WireEvent::Heartbeat {
            source,
            sequence,
            observed_slot,
            slot,
        } => Ok(normalized_event(
            source,
            sequence,
            observed_slot.unwrap_or(slot),
            default_kind,
            MarketEvent::Heartbeat(Heartbeat { slot, note: "wire" }),
            next_sequence,
        )),
    }
}

fn normalized_event(
    source: Option<WireSourceKind>,
    sequence: Option<u64>,
    observed_slot: u64,
    default_kind: EventSourceKind,
    payload: MarketEvent,
    next_sequence: &mut u64,
) -> NormalizedEvent {
    let now = SystemTime::now();
    let sequence = sequence.unwrap_or_else(|| {
        let current = *next_sequence;
        *next_sequence += 1;
        current
    });
    NormalizedEvent {
        payload,
        source: SourceMetadata {
            source: source.map(Into::into).unwrap_or(default_kind),
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

fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    let hex = hex.trim();
    if hex.is_empty() {
        return Ok(Vec::new());
    }
    if hex.len() % 2 != 0 {
        return Err("hex payload must have an even number of characters".into());
    }
    let mut output = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.as_bytes().chunks_exact(2);
    for chunk in &mut chars {
        let slice = std::str::from_utf8(chunk).map_err(|error| error.to_string())?;
        let byte = u8::from_str_radix(slice, 16).map_err(|error| error.to_string())?;
        output.push(byte);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use domain::PoolVenue;

    use crate::{MarketEvent, MarketEventSource};

    use super::JsonlFileEventSource;

    #[test]
    fn jsonl_source_reads_account_updates() {
        let path = temp_file_path("detection-jsonl-source");
        fs::write(
            &path,
            "{\"type\":\"account_update\",\"source\":\"synthetic\",\"sequence\":7,\"observed_slot\":11,\"pubkey\":\"acct-a\",\"owner\":\"owner\",\"lamports\":0,\"data_hex\":\"010200000000000004006400000000000000\",\"slot\":11,\"write_version\":3}\n",
        )
        .unwrap();

        let mut source = JsonlFileEventSource::open(&path).unwrap();
        let event = source.poll_next().unwrap().expect("event");

        assert_eq!(event.source.sequence, 7);
        assert_eq!(event.source.observed_slot, 11);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn jsonl_source_reads_pool_snapshot_updates() {
        let path = temp_file_path("detection-jsonl-live-source");
        fs::write(
            &path,
            "{\"type\":\"pool_snapshot_update\",\"source\":\"shredstream\",\"sequence\":9,\"observed_slot\":33,\"pool_id\":\"pool-a\",\"price_bps\":10001,\"fee_bps\":4,\"reserve_depth\":5000,\"venue\":\"orca_whirlpool\",\"confidence\":\"decoded\",\"token_mint_a\":\"a\",\"token_mint_b\":\"b\",\"tick_spacing\":4,\"current_tick_index\":12,\"slot\":33,\"write_version\":2}\n",
        )
        .unwrap();

        let mut source = JsonlFileEventSource::open(&path).unwrap();
        let event = source.poll_next().unwrap().expect("event");

        assert_eq!(event.source.sequence, 9);
        match event.payload {
            MarketEvent::PoolSnapshotUpdate(update) => {
                assert_eq!(update.venue, PoolVenue::OrcaWhirlpool);
            }
            other => panic!("expected pool snapshot update, got {other:?}"),
        }

        let _ = fs::remove_file(path);
    }

    fn temp_file_path(prefix: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "{prefix}-{}-{}.jsonl",
            std::process::id(),
            unique_suffix()
        ));
        path
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
    }
}
