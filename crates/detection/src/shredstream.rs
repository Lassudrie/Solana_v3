use std::{
    collections::VecDeque,
    net::UdpSocket,
    time::{Duration, SystemTime},
};

use serde::Deserialize;

use crate::{
    events::{EventSourceKind, LatencyMetadata, MarketEvent, NormalizedEvent, SourceMetadata},
    ingestor::{IngestError, MarketEventSource},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredStreamConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
}

impl Default for ShredStreamConfig {
    fn default() -> Self {
        Self {
            endpoint: "udp://127.0.0.1:10000".to_string(),
            auth_token: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ShredStreamSource {
    config: ShredStreamConfig,
    socket: Option<UdpSocket>,
    bind_error: Option<String>,
    queue: VecDeque<NormalizedEvent>,
    next_sequence: u64,
    exhausted: bool,
}

impl ShredStreamSource {
    pub fn new(config: ShredStreamConfig) -> Self {
        Self {
            config,
            socket: None,
            bind_error: None,
            queue: VecDeque::new(),
            next_sequence: 1,
            exhausted: false,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }

    pub fn push_test_event(&mut self, event: NormalizedEvent) {
        self.queue.push_back(event);
    }

    pub fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }
}

impl MarketEventSource for ShredStreamSource {
    fn source_kind(&self) -> EventSourceKind {
        EventSourceKind::ShredStream
    }

    fn poll_next(&mut self) -> Result<Option<NormalizedEvent>, IngestError> {
        if let Some(event) = self.queue.pop_front() {
            return Ok(Some(event));
        }

        if self.exhausted {
            return Err(IngestError::Exhausted {
                kind: self.source_kind(),
            });
        }

        if let Some(detail) = &self.bind_error {
            return Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: detail.clone(),
            });
        }

        if self.socket.is_none() {
            if let Err(detail) = self.connect_socket() {
                self.bind_error = Some(detail.clone());
                return Err(IngestError::SourceFailure {
                    kind: self.source_kind(),
                    detail,
                });
            }
        }

        let Some(socket) = self.socket.as_mut() else {
            return Ok(None);
        };

        let mut buffer = [0u8; 65_535];
        match socket.recv(&mut buffer) {
            Ok(bytes) => {
                let payload = std::str::from_utf8(&buffer[..bytes]).map_err(|error| {
                    IngestError::SourceFailure {
                        kind: self.source_kind(),
                        detail: error.to_string(),
                    }
                })?;
                parse_wire_event(payload, self.source_kind(), &mut self.next_sequence).map(Some)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: error.to_string(),
            }),
        }
    }
}

impl ShredStreamSource {
    fn connect_socket(&mut self) -> Result<(), String> {
        let bind_address = parse_udp_address(&self.config.endpoint);
        let socket = UdpSocket::bind(bind_address)
            .map_err(|error| format!("unable to bind udp socket on {bind_address}: {error}"))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| format!("unable to enable nonblocking mode: {error}"))?;
        self.socket = Some(socket);
        Ok(())
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
) -> Result<NormalizedEvent, IngestError> {
    let event: WireEvent =
        serde_json::from_str(input).map_err(|error| IngestError::SourceFailure {
            kind: default_kind,
            detail: error.to_string(),
        })?;
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
            MarketEvent::AccountUpdate(crate::events::AccountUpdate {
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
            MarketEvent::SlotBoundary(crate::events::SlotBoundary { slot, leader }),
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
            MarketEvent::Heartbeat(crate::events::Heartbeat { slot, note: "wire" }),
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
    let source = source.map(Into::into).unwrap_or(default_kind);

    NormalizedEvent {
        payload,
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

fn parse_udp_address(endpoint: &str) -> &str {
    endpoint.strip_prefix("udp://").unwrap_or(endpoint)
}

fn decode_hex(input: &str) -> Result<Vec<u8>, IngestError> {
    if input.len() % 2 != 0 {
        return Err(IngestError::SourceFailure {
            kind: EventSourceKind::ShredStream,
            detail: "data_hex length must be even".into(),
        });
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let bytes = std::str::from_utf8(pair).map_err(|error| IngestError::SourceFailure {
            kind: EventSourceKind::ShredStream,
            detail: error.to_string(),
        })?;
        let byte = u8::from_str_radix(bytes, 16).map_err(|error| IngestError::SourceFailure {
            kind: EventSourceKind::ShredStream,
            detail: error.to_string(),
        })?;
        out.push(byte);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;

    fn pick_free_port() -> u16 {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap().port()
    }

    #[test]
    fn shim_source_emits_udp_wire_event() {
        let port = pick_free_port();
        let mut source = ShredStreamSource::new(ShredStreamConfig {
            endpoint: format!("udp://127.0.0.1:{port}"),
            auth_token: None,
        });
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

        assert_eq!(source.poll_next().unwrap(), None);
        sender
            .send_to(
                b"{\"type\":\"heartbeat\",\"slot\":123,\"observed_slot\":456}",
                format!("127.0.0.1:{port}"),
            )
            .unwrap();

        let event = source.poll_next().unwrap().expect("event");
        let payload = match event.payload {
            MarketEvent::Heartbeat(heartbeat) => heartbeat,
            other => panic!("unexpected payload {other:?}"),
        };

        assert_eq!(payload.slot, 123);
        assert_eq!(event.source.sequence, 1);
        assert_eq!(event.source.observed_slot, 456);
        assert_eq!(event.source.source, EventSourceKind::ShredStream);
    }

    #[test]
    fn queue_is_still_supported_for_tests() {
        let mut source = ShredStreamSource::new(ShredStreamConfig {
            endpoint: "udp://127.0.0.1:0".into(),
            auth_token: None,
        });
        source.push_test_event(NormalizedEvent {
            payload: MarketEvent::Heartbeat(crate::events::Heartbeat {
                slot: 9,
                note: "test",
            }),
            source: SourceMetadata {
                source: EventSourceKind::Synthetic,
                sequence: 42,
                observed_slot: 9,
            },
            latency: LatencyMetadata {
                source_received_at: SystemTime::now(),
                normalized_at: SystemTime::now(),
                source_latency: Duration::ZERO,
            },
        });
        assert!(source.poll_next().unwrap().is_some());
        source.mark_exhausted();
        assert!(matches!(
            source.poll_next(),
            Err(IngestError::Exhausted { kind }) if kind == EventSourceKind::ShredStream
        ));
    }
}
