mod account_batcher;
pub mod bootstrap;
mod build_sign_dispatch;
pub mod config;
pub mod control;
pub mod daemon;
mod execution_context;
mod live;
pub mod observer;
pub mod refresh;
pub mod route_health;
mod rpc;
pub mod runtime;
pub mod sources;
mod submit_dispatch;
mod submit_factory;

pub use bootstrap::{BootstrapError, bootstrap};
pub use config::{BotConfig, ConfigError, MonitorServerConfig};
pub use control::{RuntimeIssue, RuntimeMode, RuntimeStatus};
pub use daemon::{BotDaemon, DaemonError, DaemonExit};
pub use observer::{
    MonitorEdgeOverview, MonitorEdgeResponse, MonitorEdgeRouteView, MonitorOverview,
    MonitorSignalMetric, MonitorSignalSample, MonitorSignalsResponse, MonitorSnapshot,
    MonitorTradeEvent, MonitorTradesResponse, ObserverHandle, PoolMonitorView, RejectionEvent,
    RejectionsResponse, RouteMonitorView, RoutesResponse,
};
pub use route_health::{
    PoolHealthState, PoolHealthTransition, RouteHealthRegistry, RouteHealthState,
    RouteHealthSummary, SharedRouteHealth,
};
pub use runtime::{BotRuntime, ColdPathServices, HotPathPipeline, HotPathReport, RuntimeError};
