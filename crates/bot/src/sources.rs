use detection::{JsonlFileEventSource, MarketEventSource, UdpJsonEventSource};
use thiserror::Error;

use crate::{
    account_batcher::{GetMultipleAccountsBatcher, LookupTableCacheHandle},
    config::{BotConfig, EventSourceMode},
    live::GrpcEntriesEventSource,
    observer::ObserverHandle,
    route_health::SharedRouteHealth,
};

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
    #[error("failed to initialize live shredstream source: {detail}")]
    LiveShredstream { detail: String },
}

pub(crate) fn build_event_source(
    config: &BotConfig,
    observer: ObserverHandle,
    route_health: SharedRouteHealth,
    account_batcher: GetMultipleAccountsBatcher,
    lookup_table_cache: LookupTableCacheHandle,
) -> Result<Box<dyn MarketEventSource>, EventSourceConfigError> {
    match config.runtime.event_source.mode {
        EventSourceMode::Disabled => Err(EventSourceConfigError::Disabled),
        EventSourceMode::JsonlFile => {
            let path = config
                .runtime
                .event_source
                .path
                .as_deref()
                .ok_or(EventSourceConfigError::MissingPath)?;
            Ok(Box::new(JsonlFileEventSource::open(path).map_err(
                |source| EventSourceConfigError::OpenFile {
                    path: path.to_string(),
                    source,
                },
            )?))
        }
        EventSourceMode::Shredstream => GrpcEntriesEventSource::spawn(
            config,
            observer,
            route_health,
            account_batcher,
            lookup_table_cache,
        )
        .map(|source| Box::new(source) as Box<dyn MarketEventSource>)
        .map_err(|detail| EventSourceConfigError::LiveShredstream { detail }),
        EventSourceMode::UdpJson => {
            let bind_address = config
                .runtime
                .event_source
                .bind_address
                .as_deref()
                .ok_or(EventSourceConfigError::MissingBindAddress)?;
            Ok(Box::new(UdpJsonEventSource::bind(bind_address).map_err(
                |source| EventSourceConfigError::BindUdp {
                    bind_address: bind_address.to_string(),
                    source,
                },
            )?))
        }
    }
}
