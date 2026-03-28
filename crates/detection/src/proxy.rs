use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};

use prost_types::Timestamp as ProstTimestamp;
use tokio::sync::broadcast;
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};
use tonic::{
    Request, Response, Status,
    transport::{Error as TransportError, Server},
};

use crate::proto::{shared, shredstream};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredstreamProxyConfig {
    pub listen_address: SocketAddr,
    pub buffer_capacity: usize,
    pub heartbeat_ttl_ms: u32,
}

impl Default for ShredstreamProxyConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:50051"
                .parse()
                .expect("default shredstream proxy listen address"),
            buffer_capacity: 4_096,
            heartbeat_ttl_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShredstreamProxyHandle {
    entry_tx: broadcast::Sender<shredstream::Entry>,
}

impl ShredstreamProxyHandle {
    pub fn publish_entry(
        &self,
        slot: u64,
        entries: impl Into<Vec<u8>>,
    ) -> Result<usize, broadcast::error::SendError<shredstream::Entry>> {
        self.publish_entry_at(slot, entries, SystemTime::now())
    }

    pub fn publish_entry_at(
        &self,
        slot: u64,
        entries: impl Into<Vec<u8>>,
        created_at: SystemTime,
    ) -> Result<usize, broadcast::error::SendError<shredstream::Entry>> {
        self.publish_proto_entry(shredstream::Entry {
            slot,
            entries: entries.into(),
            created_at: system_time_to_timestamp(created_at),
        })
    }

    pub fn publish_proto_entry(
        &self,
        entry: shredstream::Entry,
    ) -> Result<usize, broadcast::error::SendError<shredstream::Entry>> {
        self.entry_tx.send(entry)
    }
}

#[derive(Debug, Clone)]
pub struct ShredstreamProxyService {
    entry_tx: broadcast::Sender<shredstream::Entry>,
    heartbeat_ttl_ms: u32,
}

impl ShredstreamProxyService {
    pub fn new(config: &ShredstreamProxyConfig) -> Self {
        let (entry_tx, _) = broadcast::channel(config.buffer_capacity.max(16));
        Self {
            entry_tx,
            heartbeat_ttl_ms: config.heartbeat_ttl_ms.max(1),
        }
    }

    pub fn handle(&self) -> ShredstreamProxyHandle {
        ShredstreamProxyHandle {
            entry_tx: self.entry_tx.clone(),
        }
    }
}

type SubscribeEntriesStream =
    Pin<Box<dyn Stream<Item = Result<shredstream::Entry, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl shredstream::shredstream_proxy_server::ShredstreamProxy for ShredstreamProxyService {
    type SubscribeEntriesStream = SubscribeEntriesStream;

    async fn subscribe_entries(
        &self,
        _request: Request<shredstream::SubscribeEntriesRequest>,
    ) -> Result<Response<Self::SubscribeEntriesStream>, Status> {
        let stream =
            BroadcastStream::new(self.entry_tx.subscribe()).filter_map(|result| match result {
                Ok(entry) => Some(Ok(entry)),
                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                    Some(Err(Status::resource_exhausted(format!(
                        "subscriber lagged behind; dropped {skipped} entries"
                    ))))
                }
            });

        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl shredstream::shredstream_server::Shredstream for ShredstreamProxyService {
    async fn send_heartbeat(
        &self,
        request: Request<shredstream::Heartbeat>,
    ) -> Result<Response<shredstream::HeartbeatResponse>, Status> {
        let heartbeat = request.into_inner();
        validate_heartbeat_socket(heartbeat.socket.as_ref())?;
        Ok(Response::new(shredstream::HeartbeatResponse {
            ttl_ms: self.heartbeat_ttl_ms,
        }))
    }
}

pub async fn serve_shredstream_proxy<F>(
    config: ShredstreamProxyConfig,
    service: ShredstreamProxyService,
    shutdown: F,
) -> Result<(), TransportError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let heartbeat_service =
        shredstream::shredstream_server::ShredstreamServer::new(service.clone());
    let proxy_service = shredstream::shredstream_proxy_server::ShredstreamProxyServer::new(service);

    Server::builder()
        .add_service(heartbeat_service)
        .add_service(proxy_service)
        .serve_with_shutdown(config.listen_address, shutdown)
        .await
}

fn validate_heartbeat_socket(socket: Option<&shared::Socket>) -> Result<(), Status> {
    let Some(socket) = socket else {
        return Err(Status::invalid_argument("heartbeat.socket is required"));
    };
    if socket.ip.trim().is_empty() {
        return Err(Status::invalid_argument("heartbeat.socket.ip is required"));
    }
    if socket.port <= 0 {
        return Err(Status::invalid_argument(
            "heartbeat.socket.port must be positive",
        ));
    }
    Ok(())
}

fn system_time_to_timestamp(time: SystemTime) -> Option<ProstTimestamp> {
    let duration = time.duration_since(UNIX_EPOCH).ok()?;
    Some(ProstTimestamp {
        seconds: i64::try_from(duration.as_secs()).ok()?,
        nanos: i32::try_from(duration.subsec_nanos()).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use tokio_stream::StreamExt;
    use tonic::Code;

    use super::*;
    use crate::proto::shredstream::{
        Heartbeat, SubscribeEntriesRequest, shredstream_proxy_server::ShredstreamProxy as _,
        shredstream_server::Shredstream as _,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_entries_streams_published_entries() {
        let service = ShredstreamProxyService::new(&ShredstreamProxyConfig::default());
        let handle = service.handle();
        let response = service
            .subscribe_entries(Request::new(SubscribeEntriesRequest {}))
            .await
            .expect("subscribe entries");
        handle
            .publish_entry_at(42, vec![1, 2, 3], UNIX_EPOCH + Duration::from_secs(10))
            .expect("publish entry");

        let entry = response
            .into_inner()
            .next()
            .await
            .expect("stream item")
            .expect("entry");

        assert_eq!(entry.slot, 42);
        assert_eq!(entry.entries, vec![1, 2, 3]);
        let created_at = entry.created_at.expect("created_at");
        assert_eq!(created_at.seconds, 10);
        assert_eq!(created_at.nanos, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_requires_socket() {
        let service = ShredstreamProxyService::new(&ShredstreamProxyConfig::default());
        let error = service
            .send_heartbeat(Request::new(Heartbeat {
                socket: None,
                regions: Vec::new(),
            }))
            .await
            .expect_err("missing socket should fail");

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_returns_configured_ttl() {
        let service = ShredstreamProxyService::new(&ShredstreamProxyConfig {
            heartbeat_ttl_ms: 2_500,
            ..ShredstreamProxyConfig::default()
        });
        let response = service
            .send_heartbeat(Request::new(Heartbeat {
                socket: Some(shared::Socket {
                    ip: "127.0.0.1".into(),
                    port: 12_345,
                }),
                regions: vec!["fra".into()],
            }))
            .await
            .expect("heartbeat response");

        assert_eq!(response.into_inner().ttl_ms, 2_500);
    }
}
