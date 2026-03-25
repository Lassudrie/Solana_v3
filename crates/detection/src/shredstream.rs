use std::{
    collections::BTreeMap,
    collections::VecDeque,
    net::{SocketAddr, UdpSocket},
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

use crate::{
    events::{EventSourceKind, LatencyMetadata, MarketEvent, NormalizedEvent, SourceMetadata},
    ingestor::{IngestError, MarketEventSource},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredStreamConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub replay_on_gap: bool,
}

impl Default for ShredStreamConfig {
    fn default() -> Self {
        Self {
            endpoint: "udp://127.0.0.1:10000".to_string(),
            auth_token: None,
            replay_on_gap: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct ShredStreamSource {
    config: ShredStreamConfig,
    socket: Option<UdpSocket>,
    bind_error: Option<String>,
    queue: VecDeque<NormalizedEvent>,
    deferred_events: BTreeMap<u64, NormalizedEvent>,
    expected_sequence: u64,
    generated_sequence: u64,
    pending_replay_range: Option<(u64, u64)>,
    exhausted: bool,
}

impl ShredStreamSource {
    pub fn new(config: ShredStreamConfig) -> Self {
        Self {
            config,
            socket: None,
            bind_error: None,
            queue: VecDeque::new(),
            deferred_events: BTreeMap::new(),
            expected_sequence: 1,
            generated_sequence: 1,
            pending_replay_range: None,
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
        match socket.recv_from(&mut buffer) {
            Ok((bytes, sender)) => {
                let payload = std::str::from_utf8(&buffer[..bytes]).map_err(|error| {
                    IngestError::SourceFailure {
                        kind: self.source_kind(),
                        detail: error.to_string(),
                    }
                })?;
                let event = parse_wire_event(
                    payload,
                    self.source_kind(),
                    &mut self.generated_sequence,
                    self.config.auth_token.as_deref(),
                )?;
                self.ingest_event(event, sender)
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
    fn ingest_event(
        &mut self,
        event: NormalizedEvent,
        sender: SocketAddr,
    ) -> Result<Option<NormalizedEvent>, IngestError> {
        let sequence = event.source.sequence;
        if sequence < self.expected_sequence {
            return Ok(self.queue.pop_front());
        }

        if sequence > self.expected_sequence {
            if !self.config.replay_on_gap {
                return Err(IngestError::SourceFailure {
                    kind: self.source_kind(),
                    detail: format!(
                        "sequence gap detected: expected {}, received {}",
                        self.expected_sequence, sequence
                    ),
                });
            }

            self.deferred_events.entry(sequence).or_insert(event);
            self.request_replay(self.expected_sequence, sequence.saturating_sub(1), sender)?;
            return Ok(self.queue.pop_front());
        }

        self.expected_sequence = self.expected_sequence.saturating_add(1);
        self.queue.push_back(event);
        self.drain_deferred_events();
        Ok(self.queue.pop_front())
    }

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

    fn request_replay(
        &mut self,
        from_sequence: u64,
        to_sequence: u64,
        sender: SocketAddr,
    ) -> Result<(), IngestError> {
        if from_sequence > to_sequence {
            return Ok(());
        }

        let (request_from, request_to) = match self.pending_replay_range {
            Some((pending_from, pending_to))
                if from_sequence >= pending_from && to_sequence <= pending_to =>
            {
                return Ok(());
            }
            Some((pending_from, pending_to)) => {
                (pending_from.min(from_sequence), pending_to.max(to_sequence))
            }
            None => (from_sequence, to_sequence),
        };

        let request = ReplayRequest {
            message_type: "replay_request",
            auth_token: self.config.auth_token.clone(),
            from_sequence: request_from,
            to_sequence: request_to,
        };
        let payload = serde_json::to_vec(&request).map_err(|error| IngestError::SourceFailure {
            kind: self.source_kind(),
            detail: error.to_string(),
        })?;

        let Some(socket) = self.socket.as_ref() else {
            return Err(IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: "replay requested before socket initialization".into(),
            });
        };
        socket
            .send_to(&payload, sender)
            .map_err(|error| IngestError::SourceFailure {
                kind: self.source_kind(),
                detail: format!("failed to send replay request to {sender}: {error}"),
            })?;
        self.pending_replay_range = Some((request_from, request_to));
        Ok(())
    }

    fn drain_deferred_events(&mut self) {
        while let Some(event) = self.deferred_events.remove(&self.expected_sequence) {
            self.expected_sequence = self.expected_sequence.saturating_add(1);
            self.queue.push_back(event);
        }

        if let Some((_, pending_to)) = self.pending_replay_range {
            if self.expected_sequence > pending_to {
                self.pending_replay_range = None;
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct ReplayRequest {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_token: Option<String>,
    from_sequence: u64,
    to_sequence: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    AccountUpdate {
        auth_token: Option<String>,
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
        auth_token: Option<String>,
        source: Option<WireSourceKind>,
        sequence: Option<u64>,
        observed_slot: Option<u64>,
        slot: u64,
        leader: Option<String>,
    },
    Heartbeat {
        auth_token: Option<String>,
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
    expected_auth_token: Option<&str>,
) -> Result<NormalizedEvent, IngestError> {
    let event: WireEvent =
        serde_json::from_str(input).map_err(|error| IngestError::SourceFailure {
            kind: default_kind,
            detail: error.to_string(),
        })?;
    match event {
        WireEvent::AccountUpdate {
            auth_token,
            source,
            sequence,
            observed_slot,
            pubkey,
            owner,
            lamports,
            data_hex,
            slot,
            write_version,
        } => {
            validate_auth_token(expected_auth_token, auth_token.as_deref(), default_kind)?;
            Ok(normalized_event(
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
            ))
        }
        WireEvent::SlotBoundary {
            auth_token,
            source,
            sequence,
            observed_slot,
            slot,
            leader,
        } => {
            validate_auth_token(expected_auth_token, auth_token.as_deref(), default_kind)?;
            Ok(normalized_event(
                source,
                sequence,
                observed_slot.unwrap_or(slot),
                default_kind,
                MarketEvent::SlotBoundary(crate::events::SlotBoundary { slot, leader }),
                next_sequence,
            ))
        }
        WireEvent::Heartbeat {
            auth_token,
            source,
            sequence,
            observed_slot,
            slot,
        } => {
            validate_auth_token(expected_auth_token, auth_token.as_deref(), default_kind)?;
            Ok(normalized_event(
                source,
                sequence,
                observed_slot.unwrap_or(slot),
                default_kind,
                MarketEvent::Heartbeat(crate::events::Heartbeat { slot, note: "wire" }),
                next_sequence,
            ))
        }
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

fn validate_auth_token(
    expected: Option<&str>,
    received: Option<&str>,
    kind: EventSourceKind,
) -> Result<(), IngestError> {
    match expected {
        Some(expected) if received != Some(expected) => Err(IngestError::SourceFailure {
            kind,
            detail: "unauthorized shredstream datagram".into(),
        }),
        _ => Ok(()),
    }
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
    use std::thread;
    use std::time::{Duration, Instant, SystemTime};

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
            replay_on_gap: false,
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
            replay_on_gap: false,
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

    #[test]
    fn measures_udp_round_trip_latency() {
        let port = pick_free_port();
        let mut source = ShredStreamSource::new(ShredStreamConfig {
            endpoint: format!("udp://127.0.0.1:{port}"),
            auth_token: None,
            replay_on_gap: false,
        });
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

        assert_eq!(source.poll_next().unwrap(), None);
        let sent_at = SystemTime::now();
        sender
            .send_to(
                b"{\"type\":\"heartbeat\",\"slot\":123,\"observed_slot\":456}",
                format!("127.0.0.1:{port}"),
            )
            .unwrap();

        let start = Instant::now();
        let event = loop {
            if let Ok(Some(event)) = source.poll_next() {
                break event;
            }
            if start.elapsed() > Duration::from_millis(250) {
                panic!("timed out waiting for shredstream event");
            }
            thread::sleep(Duration::from_millis(1));
        };

        let normalized_latency = event
            .latency
            .normalized_at
            .duration_since(sent_at)
            .expect("event time should be after send");
        assert!(normalized_latency <= Duration::from_millis(250));
        println!(
            "shredstream udp normalized latency {:?}",
            normalized_latency
        );
        assert_eq!(event.latency.source_latency, Duration::ZERO);
    }

    #[test]
    fn rejects_udp_datagram_with_invalid_auth_token() {
        let port = pick_free_port();
        let mut source = ShredStreamSource::new(ShredStreamConfig {
            endpoint: format!("udp://127.0.0.1:{port}"),
            auth_token: Some("secret".into()),
            replay_on_gap: false,
        });
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

        assert_eq!(source.poll_next().unwrap(), None);
        sender
            .send_to(
                b"{\"type\":\"heartbeat\",\"slot\":123,\"auth_token\":\"wrong\"}",
                format!("127.0.0.1:{port}"),
            )
            .unwrap();

        assert!(matches!(
            source.poll_next(),
            Err(IngestError::SourceFailure { kind, detail })
                if kind == EventSourceKind::ShredStream && detail == "unauthorized shredstream datagram"
        ));
    }

    #[test]
    fn requests_replay_and_reorders_gap_sequence() {
        let port = pick_free_port();
        let mut source = ShredStreamSource::new(ShredStreamConfig {
            endpoint: format!("udp://127.0.0.1:{port}"),
            auth_token: Some("secret".into()),
            replay_on_gap: true,
        });
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();

        assert_eq!(source.poll_next().unwrap(), None);
        sender
            .send_to(
                b"{\"type\":\"heartbeat\",\"sequence\":2,\"slot\":2,\"auth_token\":\"secret\"}",
                format!("127.0.0.1:{port}"),
            )
            .unwrap();

        assert_eq!(source.poll_next().unwrap(), None);

        let mut replay_request_buffer = [0u8; 1024];
        let (bytes, _) = sender.recv_from(&mut replay_request_buffer).unwrap();
        let replay_request = String::from_utf8_lossy(&replay_request_buffer[..bytes]);
        assert!(replay_request.contains("\"type\":\"replay_request\""));
        assert!(replay_request.contains("\"from_sequence\":1"));
        assert!(replay_request.contains("\"to_sequence\":1"));
        assert!(replay_request.contains("\"auth_token\":\"secret\""));

        sender
            .send_to(
                b"{\"type\":\"heartbeat\",\"source\":\"replay\",\"sequence\":1,\"slot\":1,\"auth_token\":\"secret\"}",
                format!("127.0.0.1:{port}"),
            )
            .unwrap();

        let first = loop {
            if let Ok(Some(event)) = source.poll_next() {
                break event;
            }
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(first.source.sequence, 1);
        assert_eq!(first.source.source, EventSourceKind::Replay);

        let second = loop {
            if let Ok(Some(event)) = source.poll_next() {
                break event;
            }
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(second.source.sequence, 2);
        assert_eq!(second.source.source, EventSourceKind::ShredStream);
    }
}
