use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::UdpSocket,
    path::Path,
    time::{Duration, SystemTime},
};

use detection::{
    AccountUpdate, EventSourceKind, Heartbeat, IngestError, LatencyMetadata, MarketEvent,
    MarketEventSource, NormalizedEvent, ShredStreamConfig, SlotBoundary, SourceMetadata,
};
use serde::Deserialize;
use thiserror::Error;

use crate::config::{EventSourceConfig, EventSourceMode, ShredstreamConfig};

#[derive(Debug, Error)]
pub enum EventSourceConfigError {
    #[error("runtime.event_source.mode is disabled")]
    Disabled,
    #[error("runtime.event_source.path is required for jsonl_file mode")]
    MissingPath,
    #[error("runtime.event_source.bind_address is required for udp_json mode")]
    MissingBindAddress,
    #[error("failed to open event source file {path}: {source}")]
    OpenFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind udp socket on {bind_address}: {source}")]
    BindUdp {
        bind_address: String,
        #[source]
        source: std::io::Error,
    },
}

pub fn build_event_source(
    config: &EventSourceConfig,
    shredstream: &ShredstreamConfig,
) -> Result<Box<dyn MarketEventSource>, EventSourceConfigError> {
    match config.mode {
        EventSourceMode::Disabled => Err(EventSourceConfigError::Disabled),
        EventSourceMode::JsonlFile => {
            let path = config
                .path
                .as_deref()
                .ok_or(EventSourceConfigError::MissingPath)?;
            Ok(Box::new(JsonlFileEventSource::open(path)?))
        }
        EventSourceMode::Shredstream => Ok(Box::new(detection::ShredStreamSource::new(
            ShredStreamConfig {
                endpoint: shredstream.endpoint.clone(),
                auth_token: shredstream.auth_token.clone(),
            },
        ))),
        EventSourceMode::UdpJson => {
            let bind_address = config
                .bind_address
                .as_deref()
                .ok_or(EventSourceConfigError::MissingBindAddress)?;
            Ok(Box::new(UdpJsonEventSource::bind(bind_address)?))
        }
    }
}

#[derive(Debug)]
pub struct JsonlFileEventSource {
    lines: std::io::Lines<BufReader<File>>,
    next_sequence: u64,
}

impl JsonlFileEventSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EventSourceConfigError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|source| EventSourceConfigError::OpenFile {
            path: path.display().to_string(),
            source,
        })?;
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
}

#[derive(Debug)]
pub struct UdpJsonEventSource {
    socket: UdpSocket,
    next_sequence: u64,
}

impl UdpJsonEventSource {
    pub fn bind(bind_address: &str) -> Result<Self, EventSourceConfigError> {
        let socket =
            UdpSocket::bind(bind_address).map_err(|source| EventSourceConfigError::BindUdp {
                bind_address: bind_address.to_string(),
                source,
            })?;
        socket
            .set_nonblocking(true)
            .expect("set udp socket nonblocking");
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
            source_latency: Duration::ZERO,
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

    use detection::MarketEventSource;

    use super::JsonlFileEventSource;

    #[test]
    fn jsonl_source_reads_account_updates() {
        let path = temp_file_path("bot-jsonl-source");
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
