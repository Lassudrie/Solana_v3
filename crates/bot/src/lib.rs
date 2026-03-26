pub mod bootstrap;
pub mod config;
pub mod control;
pub mod daemon;
mod live;
pub mod observer;
pub mod refresh;
pub mod runtime;
pub mod sources;

pub use bootstrap::{BootstrapError, bootstrap};
pub use config::{BotConfig, ConfigError, MonitorServerConfig};
pub use control::{RuntimeIssue, RuntimeMode, RuntimeStatus};
pub use daemon::{BotDaemon, DaemonError, DaemonExit};
pub use observer::{
    MonitorOverview, MonitorSignalMetric, MonitorSignalSample, MonitorSignalsResponse,
    MonitorSnapshot, MonitorTradeEvent, MonitorTradesResponse, ObserverHandle, PoolMonitorView,
    RejectionEvent, RejectionsResponse,
};
pub use runtime::{BotRuntime, ColdPathServices, HotPathPipeline, HotPathReport, RuntimeError};
