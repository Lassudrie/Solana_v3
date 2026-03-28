use std::{
    error::Error,
    fs,
    io::{self, Stdout},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use bot::observer::{
    MonitorEdgeResponse, MonitorEdgeRouteView, MonitorOverview, MonitorSignalsResponse,
    MonitorSnapshot, MonitorTradeEvent, MonitorTradesResponse, PoolsResponse, RejectionEvent,
    RejectionsResponse, RouteMonitorView, RoutesResponse,
};
use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Row, Table, TableState, Tabs, Wrap},
};
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;

#[derive(Debug, Parser)]
#[command(name = "botctl")]
#[command(about = "Local monitor CLI/TUI for the trading bot")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8081")]
    endpoint: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Ui,
    Status,
    Pools {
        #[arg(long, default_value = "freshness")]
        sort: String,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Signals {
        #[arg(long, default_value = "1m")]
        window: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Rejects {
        #[arg(long, default_value = "all")]
        stage: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Trades {
        #[arg(long, default_value = "all")]
        status: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Edge {
        #[arg(long, default_value = "captured")]
        sort: String,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Routes {
        #[arg(long, default_value = "all")]
        status: String,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        watch: bool,
    },
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ExportCommand {
    Snapshot {
        #[arg(long, default_value = "json")]
        format: String,
    },
    LiveManifest {
        #[arg(long)]
        source_config: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

struct ApiClient {
    endpoint: String,
    client: Client,
}

impl ApiClient {
    fn new(endpoint: String) -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
        Ok(Self { endpoint, client })
    }

    fn overview(&self) -> Result<MonitorOverview, Box<dyn Error>> {
        self.get_json("/monitor/overview")
    }

    fn pools(
        &self,
        sort: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> Result<PoolsResponse, Box<dyn Error>> {
        let mut path = format!("/monitor/pools?sort={sort}&limit={limit}");
        if let Some(filter) = filter {
            path.push_str("&filter=");
            path.push_str(filter);
        }
        self.get_json(&path)
    }

    fn signals(
        &self,
        window: &str,
        limit: usize,
    ) -> Result<MonitorSignalsResponse, Box<dyn Error>> {
        self.get_json(&format!("/monitor/signals?window={window}&limit={limit}"))
    }

    fn rejects(&self, stage: &str, limit: usize) -> Result<RejectionsResponse, Box<dyn Error>> {
        self.get_json(&format!("/monitor/rejections?stage={stage}&limit={limit}"))
    }

    fn trades(&self, status: &str, limit: usize) -> Result<MonitorTradesResponse, Box<dyn Error>> {
        self.get_json(&format!("/monitor/trades?status={status}&limit={limit}"))
    }

    fn snapshot(&self) -> Result<MonitorSnapshot, Box<dyn Error>> {
        self.get_json("/monitor/snapshot")
    }

    fn edge(
        &self,
        sort: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> Result<MonitorEdgeResponse, Box<dyn Error>> {
        let mut path = format!("/monitor/edge?sort={sort}&limit={limit}");
        if let Some(filter) = filter {
            path.push_str("&filter=");
            path.push_str(filter);
        }
        self.get_json(&path)
    }

    fn routes(
        &self,
        status: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> Result<RoutesResponse, Box<dyn Error>> {
        let mut path = format!("/monitor/routes?status={status}&limit={limit}");
        if let Some(filter) = filter {
            path.push_str("&filter=");
            path.push_str(filter);
        }
        self.get_json(&path)
    }

    fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, Box<dyn Error>> {
        let url = format!("{}{}", self.endpoint.trim_end_matches('/'), path);
        Ok(self
            .client
            .get(url)
            .send()?
            .error_for_status()?
            .json::<T>()?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TabKind {
    Overview,
    Pools,
    Signals,
    Rejects,
    Trades,
}

impl TabKind {
    fn all() -> [TabKind; 5] {
        [
            TabKind::Overview,
            TabKind::Pools,
            TabKind::Signals,
            TabKind::Rejects,
            TabKind::Trades,
        ]
    }

    fn title(self) -> &'static str {
        match self {
            TabKind::Overview => "Overview",
            TabKind::Pools => "Pools",
            TabKind::Signals => "Signals",
            TabKind::Rejects => "Rejects",
            TabKind::Trades => "Trades",
        }
    }
}

struct App {
    api: ApiClient,
    snapshot: MonitorSnapshot,
    active_tab: usize,
    selected: [usize; 5],
    body_scroll: [u16; 5],
    detail_scroll: u16,
    paused: bool,
    filter_input: String,
    input_mode: bool,
    show_detail: bool,
    pool_sort: usize,
}

impl App {
    fn new(api: ApiClient) -> Result<Self, Box<dyn Error>> {
        let snapshot = api.snapshot()?;
        Ok(Self::with_snapshot(api, snapshot))
    }

    fn with_snapshot(api: ApiClient, snapshot: MonitorSnapshot) -> Self {
        Self {
            api,
            snapshot,
            active_tab: 0,
            selected: [0; 5],
            body_scroll: [0; 5],
            detail_scroll: 0,
            paused: false,
            filter_input: String::new(),
            input_mode: false,
            show_detail: false,
            pool_sort: 0,
        }
    }

    fn refresh_interval(&self) -> Duration {
        Duration::from_millis(self.snapshot.overview.poll_interval_millis.max(1))
    }

    fn refresh(&mut self) {
        if self.paused {
            return;
        }
        if let Ok(snapshot) = self.api.snapshot() {
            self.snapshot = snapshot;
        }
    }

    fn current_tab(&self) -> TabKind {
        TabKind::all()[self.active_tab]
    }

    fn selected_mut(&mut self) -> &mut usize {
        &mut self.selected[self.active_tab]
    }

    fn selected(&self) -> usize {
        self.selected[self.active_tab]
    }

    fn activate_tab(&mut self, tab: usize) {
        let max_index = TabKind::all().len().saturating_sub(1);
        let next = tab.min(max_index);
        if self.active_tab != next {
            self.detail_scroll = 0;
        }
        self.active_tab = next;
    }

    fn cycle_tab(&mut self) {
        self.activate_tab((self.active_tab + 1) % TabKind::all().len());
    }

    fn previous_tab(&mut self) {
        self.activate_tab(self.active_tab.saturating_sub(1));
    }

    fn next_tab(&mut self) {
        self.activate_tab((self.active_tab + 1).min(TabKind::all().len().saturating_sub(1)));
    }

    fn visible_pools(&self) -> Vec<bot::observer::PoolMonitorView> {
        let mut items = self.snapshot.pools.items.clone();
        if !self.filter_input.is_empty() {
            let filter = self.filter_input.to_ascii_lowercase();
            items.retain(|item| {
                item.pool_id.to_ascii_lowercase().contains(&filter)
                    || item
                        .venues
                        .iter()
                        .any(|venue| venue.to_ascii_lowercase().contains(&filter))
                    || item
                        .route_ids
                        .iter()
                        .any(|route_id| route_id.to_ascii_lowercase().contains(&filter))
            });
        }
        match self.pool_sort % 3 {
            1 => items.sort_by_key(|item| std::cmp::Reverse(item.reserve_depth)),
            2 => items.sort_by_key(|item| std::cmp::Reverse(item.price_bps)),
            _ => items.sort_by_key(|item| (item.stale, std::cmp::Reverse(item.last_update_slot))),
        }
        items
    }

    fn visible_rejections(&self) -> Vec<RejectionEvent> {
        apply_filter(
            self.snapshot.rejections.items.clone(),
            &self.filter_input,
            |item| {
                format!(
                    "{} {} {} {}",
                    item.stage, item.route_id, item.reason_code, item.reason_detail
                )
            },
        )
    }

    fn visible_trades(&self) -> Vec<MonitorTradeEvent> {
        apply_filter(
            self.snapshot.trades.items.clone(),
            &self.filter_input,
            |item| {
                format!(
                    "{} {} {} {}",
                    item.route_id, item.submission_id, item.submit_status, item.outcome
                )
            },
        )
    }

    fn scroll_up(&mut self) {
        if self.show_detail {
            self.detail_scroll = self.detail_scroll.saturating_sub(1);
            return;
        }

        match self.current_tab() {
            TabKind::Overview => {
                self.body_scroll[self.active_tab] =
                    self.body_scroll[self.active_tab].saturating_sub(1);
            }
            _ => {
                let selected = self.selected_mut();
                *selected = selected.saturating_sub(1);
            }
        }
    }

    fn scroll_down(&mut self) {
        if self.show_detail {
            self.detail_scroll = self.detail_scroll.saturating_add(1);
            return;
        }

        match self.current_tab() {
            TabKind::Overview => {
                self.body_scroll[self.active_tab] =
                    self.body_scroll[self.active_tab].saturating_add(1);
            }
            _ => {
                let selected = self.selected_mut();
                *selected = selected.saturating_add(1);
            }
        }
    }

    fn toggle_detail(&mut self) {
        self.show_detail = !self.show_detail;
        self.detail_scroll = 0;
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let api = ApiClient::new(cli.endpoint)?;
    match cli.command {
        Command::Ui => run_ui(api),
        Command::Status => print_status(&api),
        Command::Pools {
            sort,
            filter,
            limit,
            watch,
        } => watch_or_once(watch, || print_pools(&api, &sort, filter.as_deref(), limit)),
        Command::Signals {
            window,
            limit,
            watch,
        } => watch_or_once(watch, || print_signals(&api, &window, limit)),
        Command::Rejects {
            stage,
            limit,
            watch,
        } => watch_or_once(watch, || print_rejects(&api, &stage, limit)),
        Command::Trades {
            status,
            limit,
            watch,
        } => watch_or_once(watch, || print_trades(&api, &status, limit)),
        Command::Edge {
            sort,
            filter,
            limit,
            watch,
        } => watch_or_once(watch, || print_edge(&api, &sort, filter.as_deref(), limit)),
        Command::Routes {
            status,
            filter,
            limit,
            watch,
        } => watch_or_once(watch, || {
            print_routes(&api, &status, filter.as_deref(), limit)
        }),
        Command::Export { command } => match command {
            ExportCommand::Snapshot { format } => export_snapshot(&api, &format),
            ExportCommand::LiveManifest {
                source_config,
                output,
            } => export_live_manifest(&api, &source_config, &output),
        },
    }
}

fn watch_or_once<F>(watch: bool, mut operation: F) -> Result<(), Box<dyn Error>>
where
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    if !watch {
        return operation();
    }
    loop {
        clear_stdout();
        operation()?;
        thread::sleep(Duration::from_millis(500));
    }
}

fn print_status(api: &ApiClient) -> Result<(), Box<dyn Error>> {
    let overview = api.overview()?;
    println!("mode: {}", overview.mode);
    println!("ready: {}", overview.ready);
    println!("live: {}", overview.live);
    println!("issue: {}", overview.issue.unwrap_or_else(|| "none".into()));
    println!(
        "last_error: {}",
        overview.last_error.unwrap_or_else(|| "none".into())
    );
    println!("slot: {}", overview.latest_slot);
    println!(
        "rpc_slot: {}",
        overview
            .rpc_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "blockhash_slot: {}",
        overview
            .blockhash_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "blockhash_slot_lag: {}",
        overview
            .blockhash_slot_lag
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "routes: tradable={}/{} warm={}",
        overview.tradable_routes, overview.total_routes, overview.warm_routes
    );
    println!(
        "live_set: eligible={} shadow={} quarantined_pools={} disabled_pools={}",
        overview.eligible_live_routes,
        overview.shadow_routes,
        overview.quarantined_pools,
        overview.disabled_pools
    );
    println!("inflight: {}", overview.inflight_submissions);
    println!("wallet_ready: {}", overview.wallet_ready);
    println!(
        "wallet_balance_lamports: {}",
        overview.wallet_balance_lamports
    );
    println!(
        "shredstream_eps: {}",
        overview.shredstream_events_per_second
    );
    println!(
        "destabilization: impact_floor_bps={} dislocation_trigger_bps={} candidates={} triggers={} unclassified={}",
        overview.destabilizing_price_impact_floor_bps,
        overview.destabilizing_price_dislocation_trigger_bps,
        overview.destabilizing_candidate_count,
        overview.destabilizing_trigger_count,
        overview.destabilizing_unclassified_count
    );
    println!(
        "destabilization_impacts_bps: p95={} max={}",
        overview
            .destabilizing_candidate_p95_price_impact_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .destabilizing_candidate_max_price_impact_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "destabilization_dislocations_bps: p95={} max={}",
        overview
            .destabilizing_candidate_p95_price_dislocation_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .destabilizing_candidate_max_price_dislocation_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "destabilization_net_edge_bps: p95={} max={}",
        overview
            .destabilizing_candidate_p95_net_edge_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .destabilizing_candidate_max_net_edge_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "last_destabilizing_trigger: pool={} slot={} impact_bps={} dislocation_bps={} net_edge_bps={}",
        overview
            .last_destabilizing_trigger_pool_id
            .clone()
            .unwrap_or_else(|| "none".into()),
        overview
            .last_destabilizing_trigger_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .last_destabilizing_trigger_price_impact_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .last_destabilizing_trigger_price_dislocation_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into()),
        overview
            .last_destabilizing_trigger_net_edge_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!("detect_events: {}", overview.detect_events);
    println!("rejections: {}", overview.rejection_count);
    println!("submits: {}", overview.submit_count);
    println!("inclusions: {}", overview.inclusion_count);
    println!("landed_rate_bps: {}", overview.landed_rate_bps);
    println!("reject_rate_bps: {}", overview.reject_rate_bps);
    println!(
        "expected_session_pnl_quote_atoms: {}",
        overview.expected_session_pnl_quote_atoms
    );
    println!(
        "captured_expected_session_pnl_quote_atoms: {}",
        overview.captured_expected_session_pnl_quote_atoms
    );
    println!(
        "missed_expected_session_pnl_quote_atoms: {}",
        overview.missed_expected_session_pnl_quote_atoms
    );
    println!("edge_capture_rate_bps: {}", overview.edge_capture_rate_bps);
    println!(
        "realized_session_pnl_quote_atoms: {}",
        overview.realized_session_pnl_quote_atoms
    );
    println!("observer_drops: {}", overview.observer_drop_count);
    Ok(())
}

fn print_pools(
    api: &ApiClient,
    sort: &str,
    filter: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn Error>> {
    let response = api.pools(sort, filter, limit)?;
    print_table(
        [
            "pool", "venues", "routes", "price", "fee", "depth", "age", "conf", "refresh", "repair",
        ],
        response.items.into_iter().map(|item| {
            [
                item.pool_id,
                item.venues.join(","),
                item.route_ids.join(","),
                item.price_bps.to_string(),
                item.fee_bps.to_string(),
                item.reserve_depth.to_string(),
                item.age_slots.to_string(),
                item.confidence,
                item.refresh_pending.to_string(),
                item.repair_pending.to_string(),
            ]
        }),
    );
    Ok(())
}

fn print_signals(api: &ApiClient, window: &str, limit: usize) -> Result<(), Box<dyn Error>> {
    let response = api.signals(window, limit)?;
    println!("window: {}", response.window);
    for (name, metric) in response.metrics {
        println!(
            "{name}: count={} latest={} p50={} p95={} p99={}",
            metric.count,
            display_optional_u64(metric.latest_nanos),
            display_optional_u64(metric.p50_nanos),
            display_optional_u64(metric.p95_nanos),
            display_optional_u64(metric.p99_nanos),
        );
    }
    println!();
    print_table(
        [
            "seq",
            "source",
            "slot",
            "ingest_ns",
            "queue_ns",
            "state_ns",
            "select_ns",
            "submit_ns",
            "total_ns",
        ],
        response.samples.into_iter().map(|sample| {
            [
                sample.seq.to_string(),
                sample.source,
                sample.observed_slot.to_string(),
                sample.ingest_nanos.to_string(),
                sample.queue_wait_nanos.to_string(),
                sample.state_apply_nanos.to_string(),
                display_optional_u64(sample.select_nanos),
                display_optional_u64(sample.submit_nanos),
                display_optional_u64(sample.total_to_submit_nanos),
            ]
        }),
    );
    Ok(())
}

fn print_rejects(api: &ApiClient, stage: &str, limit: usize) -> Result<(), Box<dyn Error>> {
    let response = api.rejects(stage, limit)?;
    print_table(
        ["seq", "stage", "route", "code", "profit", "slot", "detail"],
        response.items.into_iter().map(|item| {
            [
                item.seq.to_string(),
                item.stage,
                item.route_id,
                item.reason_code,
                item.expected_profit_quote_atoms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.observed_slot.to_string(),
                item.reason_detail,
            ]
        }),
    );
    Ok(())
}

fn print_trades(api: &ApiClient, status: &str, limit: usize) -> Result<(), Box<dyn Error>> {
    let response = api.trades(status, limit)?;
    print_trade_table(response.items);
    Ok(())
}

fn print_edge(
    api: &ApiClient,
    sort: &str,
    filter: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn Error>> {
    let response = api.edge(sort, filter, limit)?;
    println!("trade_count: {}", response.overview.trade_count);
    println!("pending_count: {}", response.overview.pending_count);
    println!("included_count: {}", response.overview.included_count);
    println!("failed_count: {}", response.overview.failed_count);
    println!(
        "submit_rejected_count: {}",
        response.overview.submit_rejected_count
    );
    println!(
        "expected_session_pnl_quote_atoms: {}",
        response.overview.expected_session_pnl_quote_atoms
    );
    println!(
        "captured_expected_session_pnl_quote_atoms: {}",
        response.overview.captured_expected_session_pnl_quote_atoms
    );
    println!(
        "missed_expected_session_pnl_quote_atoms: {}",
        response.overview.missed_expected_session_pnl_quote_atoms
    );
    println!(
        "edge_capture_rate_bps: {}",
        response.overview.edge_capture_rate_bps
    );
    println!(
        "realized_session_pnl_quote_atoms: {}",
        response.overview.realized_session_pnl_quote_atoms
    );
    println!(
        "pnl_realization_rate_bps: {}",
        response.overview.pnl_realization_rate_bps
    );
    println!(
        "avg_source_to_submit_nanos: {}",
        display_optional_u64(response.overview.avg_source_to_submit_nanos)
    );
    println!(
        "avg_source_to_terminal_nanos: {}",
        display_optional_u64(response.overview.avg_source_to_terminal_nanos)
    );
    println!();
    print_edge_table(response.routes);
    Ok(())
}

fn print_routes(
    api: &ApiClient,
    status: &str,
    filter: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn Error>> {
    let response = api.routes(status, filter, limit)?;
    print_table(
        [
            "route",
            "state",
            "eligible",
            "block_pool",
            "reason",
            "failures",
            "last_success",
        ],
        response.items.into_iter().map(|item| {
            [
                item.route_id,
                route_health_label(item.health_state).into(),
                item.eligible_live.to_string(),
                item.blocking_pool_id.unwrap_or_else(|| "-".into()),
                item.blocking_reason.unwrap_or_else(|| "-".into()),
                item.recent_chain_failure_count.to_string(),
                item.last_success_slot
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
            ]
        }),
    );
    Ok(())
}

fn export_snapshot(api: &ApiClient, format: &str) -> Result<(), Box<dyn Error>> {
    let snapshot = api.snapshot()?;
    match format {
        "json" => println!("{}", serde_json::to_string_pretty(&snapshot)?),
        other => return Err(format!("unsupported format: {other}").into()),
    }
    Ok(())
}

fn export_live_manifest(
    api: &ApiClient,
    source_config: &Path,
    output: &Path,
) -> Result<(), Box<dyn Error>> {
    let routes = api.routes("all", None, 10_000)?;
    let eligible = routes
        .items
        .iter()
        .filter(|item| item.eligible_live)
        .map(|item| item.route_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let excluded = routes
        .items
        .iter()
        .filter(|item| !item.eligible_live)
        .cloned()
        .collect::<Vec<_>>();

    let source_text = fs::read_to_string(source_config)?;
    let extension = source_config
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("toml");
    let rendered = match extension {
        "json" => render_filtered_json(&source_text, &eligible)?,
        _ => render_filtered_toml(&source_text, &eligible)?,
    };
    fs::write(output, rendered)?;

    let report_path = PathBuf::from(format!("{}.report.json", output.display()));
    let report = serde_json::json!({
        "included_routes": eligible,
        "excluded_routes": excluded.iter().map(route_report_entry).collect::<Vec<_>>(),
    });
    fs::write(report_path, serde_json::to_string_pretty(&report)?)?;
    println!("wrote {}", output.display());
    Ok(())
}

fn render_filtered_toml(
    source_text: &str,
    eligible: &std::collections::BTreeSet<String>,
) -> Result<String, Box<dyn Error>> {
    let mut value = toml::from_str::<toml::Value>(source_text)?;
    let Some(route_tables) = value
        .get_mut("routes")
        .and_then(|routes| routes.get_mut("definitions"))
        .and_then(toml::Value::as_array_mut)
    else {
        return Err("missing routes.definitions in source config".into());
    };
    route_tables.retain(|route| {
        route
            .get("route_id")
            .and_then(toml::Value::as_str)
            .map(|route_id| eligible.contains(route_id))
            .unwrap_or(false)
    });
    Ok(toml::to_string_pretty(&value)?)
}

fn render_filtered_json(
    source_text: &str,
    eligible: &std::collections::BTreeSet<String>,
) -> Result<String, Box<dyn Error>> {
    let mut value = serde_json::from_str::<serde_json::Value>(source_text)?;
    let Some(route_tables) = value
        .get_mut("routes")
        .and_then(|routes| routes.get_mut("definitions"))
        .and_then(serde_json::Value::as_array_mut)
    else {
        return Err("missing routes.definitions in source config".into());
    };
    route_tables.retain(|route| {
        route
            .get("route_id")
            .and_then(serde_json::Value::as_str)
            .map(|route_id| eligible.contains(route_id))
            .unwrap_or(false)
    });
    Ok(serde_json::to_string_pretty(&value)?)
}

fn route_report_entry(item: &RouteMonitorView) -> serde_json::Value {
    serde_json::json!({
        "route_id": item.route_id,
        "health_state": route_health_label(item.health_state),
        "blocking_pool_id": item.blocking_pool_id,
        "blocking_reason": item.blocking_reason,
        "recent_chain_failure_count": item.recent_chain_failure_count,
        "last_success_slot": item.last_success_slot,
    })
}

fn print_trade_table(items: Vec<MonitorTradeEvent>) {
    print_table(
        [
            "route",
            "submit",
            "outcome",
            "buffer_bps",
            "failure",
            "expected_pnl",
            "realized_pnl",
            "src_submit_ns",
            "src_term_ns",
            "tip",
            "submit_slot",
            "submission_id",
        ],
        items.into_iter().map(|item| {
            [
                item.route_id,
                item.submit_status,
                item.outcome,
                item.active_execution_buffer_bps
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.failure_error_name
                    .clone()
                    .or_else(|| item.failure_custom_code.map(|value| value.to_string()))
                    .unwrap_or_else(|| "-".into()),
                item.expected_pnl_quote_atoms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.realized_pnl_quote_atoms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "N/A".into()),
                display_optional_u64(item.source_to_submit_nanos),
                display_optional_u64(item.source_to_terminal_nanos),
                item.jito_tip_lamports
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                display_optional_u64(item.submitted_slot),
                item.submission_id,
            ]
        }),
    );
}

fn print_edge_table(items: Vec<MonitorEdgeRouteView>) {
    print_table(
        [
            "route",
            "state",
            "live",
            "sel",
            "inc",
            "fail",
            "cap_bps",
            "real_bps",
            "exp_pnl",
            "cap_pnl",
            "miss_pnl",
            "real_pnl",
            "avg_submit_ns",
            "avg_term_ns",
            "buffer_bps",
        ],
        items.into_iter().map(|item| {
            [
                item.route_id,
                item.health_state
                    .map(route_health_label)
                    .unwrap_or("unknown")
                    .to_string(),
                item.eligible_live
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.selected_count.to_string(),
                item.included_count.to_string(),
                item.failed_count.to_string(),
                item.edge_capture_rate_bps.to_string(),
                item.pnl_realization_rate_bps.to_string(),
                item.expected_pnl_quote_atoms.to_string(),
                item.captured_expected_pnl_quote_atoms.to_string(),
                item.missed_expected_pnl_quote_atoms.to_string(),
                item.realized_pnl_quote_atoms.to_string(),
                display_optional_u64(item.avg_source_to_submit_nanos),
                display_optional_u64(item.avg_source_to_terminal_nanos),
                item.last_active_execution_buffer_bps
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
            ]
        }),
    );
}

fn print_table<const N: usize>(headers: [&str; N], rows: impl Iterator<Item = [String; N]>) {
    let rows = rows.collect::<Vec<_>>();
    let mut widths = headers.map(|header| header.len());
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }
    println!(
        "{}",
        headers
            .iter()
            .enumerate()
            .map(|(index, header)| format!("{header:<width$}", width = widths[index]))
            .collect::<Vec<_>>()
            .join("  ")
    );
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, value)| format!("{value:<width$}", width = widths[index]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

fn clear_stdout() {
    print!("\x1B[2J\x1B[H");
}

fn display_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}

fn route_health_label(state: bot::RouteHealthState) -> &'static str {
    match state {
        bot::RouteHealthState::Eligible => "eligible",
        bot::RouteHealthState::BlockedPoolStale => "blocked_pool_stale",
        bot::RouteHealthState::BlockedPoolNotExecutable => "blocked_pool_not_executable",
        bot::RouteHealthState::BlockedPoolQuoteModelNotExecutable => {
            "blocked_pool_quote_model_not_executable"
        }
        bot::RouteHealthState::BlockedPoolRepair => "blocked_pool_repair",
        bot::RouteHealthState::BlockedPoolQuarantined => "blocked_pool_quarantined",
        bot::RouteHealthState::BlockedPoolDisabled => "blocked_pool_disabled",
        bot::RouteHealthState::ShadowOnly => "shadow_only",
    }
}

fn run_ui(api: ApiClient) -> Result<(), Box<dyn Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_ui_loop(&mut terminal, api);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn run_ui_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    api: ApiClient,
) -> Result<(), Box<dyn Error>> {
    let mut app = App::new(api)?;
    let mut last_refresh = Instant::now();
    loop {
        let refresh_interval = app.refresh_interval();
        if last_refresh.elapsed() >= refresh_interval {
            app.refresh();
            last_refresh = Instant::now();
        }
        terminal.draw(|frame| render(frame, &app))?;
        let timeout = refresh_interval
            .checked_sub(last_refresh.elapsed())
            .unwrap_or(Duration::ZERO);
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if handle_key(&mut app, key) {
                    return Ok(());
                }
            }
        }
    }
}

fn handle_key(app: &mut App, key: KeyEvent) -> bool {
    if app.input_mode {
        match key.code {
            KeyCode::Esc => app.input_mode = false,
            KeyCode::Enter => app.input_mode = false,
            KeyCode::Backspace => {
                app.filter_input.pop();
            }
            KeyCode::Char(ch) => app.filter_input.push(ch),
            _ => {}
        }
        return false;
    }
    match key.code {
        KeyCode::Char('q') => return true,
        KeyCode::Char('p') => app.paused = !app.paused,
        KeyCode::Char('/') => app.input_mode = true,
        KeyCode::Char('s') if matches!(app.current_tab(), TabKind::Pools) => {
            app.pool_sort = (app.pool_sort + 1) % 3;
        }
        KeyCode::Char('1') => app.activate_tab(0),
        KeyCode::Char('2') => app.activate_tab(1),
        KeyCode::Char('3') => app.activate_tab(2),
        KeyCode::Char('4') => app.activate_tab(3),
        KeyCode::Char('5') => app.activate_tab(4),
        KeyCode::Tab => app.cycle_tab(),
        KeyCode::Left => app.previous_tab(),
        KeyCode::Right => app.next_tab(),
        KeyCode::Up => app.scroll_up(),
        KeyCode::Down => app.scroll_down(),
        KeyCode::Enter => app.toggle_detail(),
        _ => {}
    }
    false
}

fn render(frame: &mut Frame, app: &App) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(10),
        ])
        .split(frame.area());

    render_header(frame, layout[0], &app.snapshot.overview);
    render_tabs(frame, layout[1], app);
    render_body(frame, layout[2], app);

    if app.show_detail {
        render_detail(frame, app);
    }
}

fn render_header(frame: &mut Frame, area: Rect, overview: &MonitorOverview) {
    let text = vec![
        Line::from(vec![
            Span::styled("mode ", Style::default().fg(Color::Gray)),
            Span::styled(overview.mode.clone(), Style::default().fg(Color::Cyan)),
            Span::raw("  "),
            Span::styled("slot ", Style::default().fg(Color::Gray)),
            Span::raw(overview.latest_slot.to_string()),
            Span::raw("  "),
            Span::styled("rpc ", Style::default().fg(Color::Gray)),
            Span::raw(
                overview
                    .rpc_slot
                    .map(|slot| slot.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Span::raw("  "),
            Span::styled("routes ", Style::default().fg(Color::Gray)),
            Span::raw(format!(
                "tradable={}/{} warm={}",
                overview.tradable_routes, overview.total_routes, overview.warm_routes
            )),
            Span::raw("  "),
            Span::styled("inflight ", Style::default().fg(Color::Gray)),
            Span::raw(overview.inflight_submissions.to_string()),
            Span::raw("  "),
            Span::styled("eps ", Style::default().fg(Color::Gray)),
            Span::raw(overview.shredstream_events_per_second.to_string()),
        ]),
        Line::from(vec![
            Span::styled("landed ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{} bps", overview.landed_rate_bps)),
            Span::raw("  "),
            Span::styled("reject ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{} bps", overview.reject_rate_bps)),
            Span::raw("  "),
            Span::styled("expected pnl ", Style::default().fg(Color::Gray)),
            Span::raw(overview.expected_session_pnl_quote_atoms.to_string()),
            Span::raw("  "),
            Span::styled("realized pnl ", Style::default().fg(Color::Gray)),
            Span::raw(overview.realized_session_pnl_quote_atoms.to_string()),
            Span::raw("  "),
            Span::styled("drops ", Style::default().fg(Color::Gray)),
            Span::raw(overview.observer_drop_count.to_string()),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title("Summary")),
        area,
    );
}

fn render_tabs(frame: &mut Frame, area: Rect, app: &App) {
    let titles = TabKind::all()
        .iter()
        .map(|tab| {
            Line::from(Span::styled(
                format!(" {} ", tab.title()),
                Style::default().fg(Color::Gray),
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Tabs::new(titles)
            .padding("", "")
            .divider(Span::raw(" "))
            .select(app.active_tab)
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Views")
                    .border_style(Style::default().fg(Color::DarkGray)),
            ),
        area,
    );
}

fn render_body(frame: &mut Frame, area: Rect, app: &App) {
    match app.current_tab() {
        TabKind::Overview => render_overview_body(frame, area, app),
        TabKind::Pools => render_pools_body(frame, area, app),
        TabKind::Signals => render_signals_body(frame, area, &app.snapshot.signals, app.selected()),
        TabKind::Rejects => render_rejects_body(frame, area, app),
        TabKind::Trades => render_trades_body(frame, area, app),
    }
}

fn render_overview_body(frame: &mut Frame, area: Rect, app: &App) {
    let body = overview_lines(app);
    let scroll = clamped_paragraph_scroll(&body, area, app.body_scroll[app.active_tab]);
    frame.render_widget(
        Paragraph::new(body)
            .scroll((scroll, 0))
            .block(Block::default().borders(Borders::ALL).title("Runtime"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn overview_lines(app: &App) -> Vec<Line<'static>> {
    let overview = &app.snapshot.overview;
    let repair_pending_rejects = app
        .snapshot
        .rejections
        .items
        .iter()
        .filter(|item| item.reason_code == "pool_repair_pending")
        .count();
    let not_executable_rejects = app
        .snapshot
        .rejections
        .items
        .iter()
        .filter(|item| item.reason_code == "pool_state_not_executable")
        .count();
    vec![
        Line::from(format!(
            "ready={} live={} issue={}",
            overview.ready,
            overview.live,
            overview.issue.clone().unwrap_or_else(|| "none".into())
        )),
        Line::from(format!(
            "wallet_ready={} balance={}",
            overview.wallet_ready, overview.wallet_balance_lamports
        )),
        Line::from(format!(
            "rpc_slot={} blockhash_slot={} blockhash_lag={}",
            overview
                .rpc_slot
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "none".into()),
            overview
                .blockhash_slot
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "none".into()),
            overview
                .blockhash_slot_lag
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "none".into()),
        )),
        Line::from(format!(
            "routes tradable={}/{} warm={}",
            overview.tradable_routes, overview.total_routes, overview.warm_routes
        )),
        Line::from(format!(
            "detect={} rejections={} submits={} included={}",
            overview.detect_events,
            overview.rejection_count,
            overview.submit_count,
            overview.inclusion_count
        )),
        Line::from(format!(
            "submit_rejected={} poll_interval_ms={}",
            overview.submit_rejected_count, overview.poll_interval_millis
        )),
        Line::from(format!(
            "rejects repair_pending={} not_executable={}",
            repair_pending_rejects, not_executable_rejects
        )),
    ]
}

fn render_pools_body(frame: &mut Frame, area: Rect, app: &App) {
    let items = app.visible_pools();
    let selected = app.selected().min(items.len().saturating_sub(1));
    let rows = items.iter().map(|item| {
        Row::new(vec![
            item.pool_id.clone(),
            item.venues.join(","),
            item.route_ids.len().to_string(),
            item.price_bps.to_string(),
            item.fee_bps.to_string(),
            item.reserve_depth.to_string(),
            item.age_slots.to_string(),
            item.confidence.clone(),
            if item.repair_pending {
                "yes".into()
            } else {
                "no".into()
            },
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Length(16),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Length(12),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new([
            "pool", "venues", "routes", "price", "fee", "depth", "age", "conf", "repair",
        ])
        .style(Style::default().fg(Color::Yellow)),
    )
    .block(Block::default().borders(Borders::ALL).title("Pools"))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_signals_body(
    frame: &mut Frame,
    area: Rect,
    signals: &MonitorSignalsResponse,
    selected: usize,
) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);
    let summary = signals
        .metrics
        .iter()
        .map(|(name, metric)| {
            Line::from(format!(
                "{name}: count={} latest={} p50={} p95={} p99={}",
                metric.count,
                display_optional_u64(metric.latest_nanos),
                display_optional_u64(metric.p50_nanos),
                display_optional_u64(metric.p95_nanos),
                display_optional_u64(metric.p99_nanos)
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title("Latency")),
        layout[0],
    );
    let rows = signals.samples.iter().map(|sample| {
        Row::new(vec![
            sample.seq.to_string(),
            sample.source.clone(),
            sample.observed_slot.to_string(),
            sample.ingest_nanos.to_string(),
            sample.queue_wait_nanos.to_string(),
            sample.state_apply_nanos.to_string(),
            display_optional_u64(sample.submit_nanos),
            display_optional_u64(sample.total_to_submit_nanos),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new([
            "seq", "source", "slot", "ingest", "queue", "state", "submit", "total",
        ])
        .style(Style::default().fg(Color::Yellow)),
    )
    .block(Block::default().borders(Borders::ALL).title("Samples"))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    let mut state = TableState::default()
        .with_selected(Some(selected.min(signals.samples.len().saturating_sub(1))));
    frame.render_stateful_widget(table, layout[1], &mut state);
}

fn render_rejects_body(frame: &mut Frame, area: Rect, app: &App) {
    let items = app.visible_rejections();
    let selected = app.selected().min(items.len().saturating_sub(1));
    let rows = items.iter().map(|item| {
        Row::new(vec![
            item.stage.clone(),
            item.route_id.clone(),
            item.reason_code.clone(),
            item.expected_profit_quote_atoms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            item.observed_slot.to_string(),
            item.reason_detail.clone(),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(22),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(["stage", "route", "code", "profit", "slot", "detail"])
            .style(Style::default().fg(Color::Yellow)),
    )
    .block(Block::default().borders(Borders::ALL).title("Rejects"))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_trades_body(frame: &mut Frame, area: Rect, app: &App) {
    let items = app.visible_trades();
    let selected = app.selected().min(items.len().saturating_sub(1));
    let rows = items.iter().map(|item| {
        Row::new(vec![
            item.route_id.clone(),
            item.submit_status.clone(),
            item.outcome.clone(),
            item.expected_pnl_quote_atoms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            item.realized_pnl_quote_atoms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "N/A".into()),
            display_optional_u64(item.source_to_submit_nanos),
            display_optional_u64(item.source_to_terminal_nanos),
            item.jito_tip_lamports
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            display_optional_u64(item.submitted_slot),
            item.submission_id.clone(),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(16),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Min(18),
        ],
    )
    .header(
        Row::new([
            "route",
            "submit",
            "outcome",
            "expected",
            "realized",
            "src->submit",
            "src->term",
            "tip",
            "slot",
            "submission",
        ])
        .style(Style::default().fg(Color::Yellow)),
    )
    .block(Block::default().borders(Borders::ALL).title("Trades"))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_detail(frame: &mut Frame, app: &App) {
    let text = detail_lines(app);
    let area = centered_rect(70, 40, frame.area());
    let scroll = clamped_paragraph_scroll(&text, area, app.detail_scroll);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(text)
            .scroll((scroll, 0))
            .block(Block::default().borders(Borders::ALL).title("Detail"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn detail_lines(app: &App) -> Vec<Line<'static>> {
    match app.current_tab() {
        TabKind::Overview => vec![Line::from("Overview has no row detail")],
        TabKind::Pools => {
            let items = app.visible_pools();
            items
                .get(app.selected())
                .map(|item| {
                    vec![
                        Line::from(format!("pool_id={}", item.pool_id)),
                        Line::from(format!("venues={}", item.venues.join(","))),
                        Line::from(format!("routes={}", item.route_ids.join(","))),
                        Line::from(format!(
                            "price_bps={} fee_bps={}",
                            item.price_bps, item.fee_bps
                        )),
                        Line::from(format!(
                            "depth={} age={} confidence={} refresh_pending={} repair_pending={}",
                            item.reserve_depth,
                            item.age_slots,
                            item.confidence,
                            item.refresh_pending,
                            item.repair_pending
                        )),
                        Line::from(format!(
                            "refresh attempts={} failures={} successes={} deadline_lag={}",
                            item.refresh_attempt_count,
                            item.refresh_failure_count,
                            item.refresh_success_count,
                            item.refresh_deadline_slot_lag
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".into())
                        )),
                        Line::from(format!(
                            "repair attempts={} failures={} successes={}",
                            item.repair_attempt_count,
                            item.repair_failure_count,
                            item.repair_success_count
                        )),
                        Line::from(format!(
                            "refresh latency_ms={}",
                            item.last_refresh_latency_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".into())
                        )),
                        Line::from(format!(
                            "repair latency_ms={} invalidation_ms={}",
                            item.last_repair_latency_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".into()),
                            item.last_repair_invalidation_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".into())
                        )),
                    ]
                })
                .unwrap_or_else(|| vec![Line::from("No selection")])
        }
        TabKind::Signals => app
            .snapshot
            .signals
            .samples
            .get(app.selected())
            .map(|item| {
                vec![
                    Line::from(format!("seq={} source={}", item.seq, item.source)),
                    Line::from(format!(
                        "slot={} ingest_ns={}",
                        item.observed_slot, item.ingest_nanos
                    )),
                    Line::from(format!("state_apply_ns={}", item.state_apply_nanos)),
                    Line::from(format!(
                        "submit_ns={}",
                        display_optional_u64(item.submit_nanos)
                    )),
                    Line::from(format!(
                        "total_to_submit_ns={}",
                        display_optional_u64(item.total_to_submit_nanos)
                    )),
                ]
            })
            .unwrap_or_else(|| vec![Line::from("No selection")]),
        TabKind::Rejects => app
            .visible_rejections()
            .get(app.selected())
            .map(|item| {
                vec![
                    Line::from(format!("stage={} route={}", item.stage, item.route_id)),
                    Line::from(format!("code={}", item.reason_code)),
                    Line::from(format!("detail={}", item.reason_detail)),
                    Line::from(format!(
                        "slot={} expected_profit={}",
                        item.observed_slot,
                        item.expected_profit_quote_atoms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into())
                    )),
                ]
            })
            .unwrap_or_else(|| vec![Line::from("No selection")]),
        TabKind::Trades => app
            .visible_trades()
            .get(app.selected())
            .map(|item| {
                vec![
                    Line::from(format!(
                        "route={} submission={}",
                        item.route_id, item.submission_id
                    )),
                    Line::from(format!(
                        "status={} outcome={}",
                        item.submit_status, item.outcome
                    )),
                    Line::from(format!(
                        "expected_pnl={} realized_pnl={}",
                        item.expected_pnl_quote_atoms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                        item.realized_pnl_quote_atoms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "N/A".into())
                    )),
                    Line::from(format!(
                        "tip={} endpoint={}",
                        item.jito_tip_lamports
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                        item.endpoint
                    )),
                    Line::from(format!(
                        "buffer_bps={} failure={} code={}",
                        item.active_execution_buffer_bps
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                        item.failure_error_name
                            .clone()
                            .unwrap_or_else(|| "-".into()),
                        item.failure_custom_code
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into())
                    )),
                ]
            })
            .unwrap_or_else(|| vec![Line::from("No selection")]),
    }
}

fn clamped_paragraph_scroll(lines: &[Line<'_>], area: Rect, requested: u16) -> u16 {
    requested.min(max_paragraph_scroll(lines, area))
}

fn max_paragraph_scroll(lines: &[Line<'_>], area: Rect) -> u16 {
    paragraph_content_height(lines, area).saturating_sub(area.height.saturating_sub(2))
}

fn paragraph_content_height(lines: &[Line<'_>], area: Rect) -> u16 {
    let inner_width = usize::from(area.width.saturating_sub(2).max(1));
    let total_height = lines
        .iter()
        .map(|line| {
            let width = line.width().max(1);
            width.div_ceil(inner_width)
        })
        .sum::<usize>();

    total_height.min(usize::from(u16::MAX)) as u16
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn apply_filter<T, F>(items: Vec<T>, filter: &str, render: F) -> Vec<T>
where
    F: Fn(&T) -> String,
{
    if filter.is_empty() {
        return items;
    }
    let filter = filter.to_ascii_lowercase();
    items
        .into_iter()
        .filter(|item| render(item).to_ascii_lowercase().contains(&filter))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;
    use ratatui::backend::TestBackend;
    use std::collections::{BTreeMap, BTreeSet};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn test_api_client() -> ApiClient {
        ApiClient {
            endpoint: "http://127.0.0.1:0".into(),
            client: Client::builder().build().expect("client"),
        }
    }

    fn test_snapshot() -> MonitorSnapshot {
        MonitorSnapshot {
            overview: MonitorOverview {
                mode: "paper".into(),
                live: false,
                ready: true,
                issue: None,
                last_error: None,
                kill_switch_active: false,
                latest_slot: 10,
                rpc_slot: Some(10),
                warm_routes: 1,
                tradable_routes: 1,
                eligible_live_routes: 1,
                shadow_routes: 0,
                total_routes: 1,
                quarantined_pools: 0,
                disabled_pools: 0,
                inflight_submissions: 0,
                wallet_ready: true,
                wallet_balance_lamports: 1,
                blockhash_slot: Some(10),
                blockhash_slot_lag: Some(0),
                detect_events: 1,
                rejection_count: 0,
                submit_count: 0,
                inclusion_count: 0,
                submit_rejected_count: 0,
                shredstream_events_per_second: 1,
                destabilizing_unclassified_count: 0,
                destabilizing_candidate_count: 0,
                destabilizing_trigger_count: 0,
                destabilizing_price_impact_floor_bps: 1,
                destabilizing_price_dislocation_trigger_bps: 5,
                last_destabilizing_trigger_pool_id: None,
                last_destabilizing_trigger_slot: None,
                last_destabilizing_trigger_price_impact_bps: None,
                last_destabilizing_trigger_price_dislocation_bps: None,
                last_destabilizing_trigger_net_edge_bps: None,
                destabilizing_candidate_max_price_impact_bps: None,
                destabilizing_candidate_p95_price_impact_bps: None,
                destabilizing_candidate_max_price_dislocation_bps: None,
                destabilizing_candidate_p95_price_dislocation_bps: None,
                destabilizing_candidate_max_net_edge_bps: None,
                destabilizing_candidate_p95_net_edge_bps: None,
                landed_rate_bps: 0,
                reject_rate_bps: 0,
                expected_session_pnl_quote_atoms: 0,
                captured_expected_session_pnl_quote_atoms: 0,
                missed_expected_session_pnl_quote_atoms: 0,
                edge_capture_rate_bps: 0,
                realized_session_pnl_quote_atoms: 0,
                observer_drop_count: 0,
                poll_interval_millis: 100,
                updated_at_unix_millis: 0,
            },
            pools: PoolsResponse {
                items: vec![bot::observer::PoolMonitorView {
                    pool_id: "pool-1".into(),
                    venues: vec!["raydium".into()],
                    route_ids: vec!["route-1".into()],
                    health_state: bot::PoolHealthState::Healthy,
                    price_bps: 100,
                    fee_bps: 3,
                    reserve_depth: 1000,
                    last_update_slot: 10,
                    slot_lag: 0,
                    age_slots: 0,
                    stale: false,
                    confidence: "high".into(),
                    refresh_pending: false,
                    refresh_attempt_count: 0,
                    refresh_failure_count: 0,
                    refresh_success_count: 0,
                    consecutive_refresh_failures: 0,
                    last_refresh_latency_ms: None,
                    refresh_deadline_slot_lag: None,
                    repair_pending: false,
                    repair_attempt_count: 0,
                    repair_failure_count: 0,
                    repair_success_count: 0,
                    consecutive_repair_failures: 0,
                    quarantined_until_slot: None,
                    disable_reason: None,
                    last_executable_slot: Some(10),
                    last_repair_latency_ms: None,
                    last_repair_invalidation_ms: None,
                    last_seen_unix_millis: 0,
                }],
            },
            routes: RoutesResponse {
                items: vec![RouteMonitorView {
                    route_id: "route-1".into(),
                    health_state: bot::RouteHealthState::Eligible,
                    eligible_live: true,
                    pool_ids: vec!["pool-1".into(), "pool-2".into()],
                    blocking_pool_id: None,
                    blocking_reason: None,
                    recent_chain_failure_count: 0,
                    last_success_slot: Some(10),
                    shadow_until_slot: None,
                }],
            },
            signals: MonitorSignalsResponse {
                window: "1m".into(),
                metrics: BTreeMap::new(),
                samples: vec![
                    bot::observer::MonitorSignalSample {
                        seq: 1,
                        source: "shred".into(),
                        source_sequence: 1,
                        observed_slot: 10,
                        source_received_at_unix_millis: 0,
                        normalized_at_unix_millis: 0,
                        source_latency_nanos: Some(1),
                        ingest_nanos: 1,
                        queue_wait_nanos: 1,
                        state_apply_nanos: 1,
                        select_nanos: Some(1),
                        build_nanos: Some(1),
                        sign_nanos: Some(1),
                        submit_nanos: Some(1),
                        total_to_submit_nanos: Some(1),
                    },
                    bot::observer::MonitorSignalSample {
                        seq: 2,
                        source: "shred".into(),
                        source_sequence: 2,
                        observed_slot: 11,
                        source_received_at_unix_millis: 0,
                        normalized_at_unix_millis: 0,
                        source_latency_nanos: Some(1),
                        ingest_nanos: 1,
                        queue_wait_nanos: 1,
                        state_apply_nanos: 1,
                        select_nanos: Some(1),
                        build_nanos: Some(1),
                        sign_nanos: Some(1),
                        submit_nanos: Some(1),
                        total_to_submit_nanos: Some(1),
                    },
                ],
            },
            rejections: RejectionsResponse {
                items: vec![RejectionEvent {
                    seq: 1,
                    stage: "strategy".into(),
                    route_id: "route-1".into(),
                    route_kind: Some("two_leg".into()),
                    leg_count: 2,
                    reason_code: "guard".into(),
                    reason_detail: "guard detail".into(),
                    observed_slot: 10,
                    expected_profit_quote_atoms: Some(1),
                    occurred_at_unix_millis: 0,
                }],
            },
            trades: MonitorTradesResponse {
                items: vec![MonitorTradeEvent {
                    seq: 1,
                    route_id: "route-1".into(),
                    route_kind: "two_leg".into(),
                    leg_count: 2,
                    submission_id: "sub-1".into(),
                    signature: "sig-1".into(),
                    submit_status: "pending".into(),
                    outcome: "pending".into(),
                    selected_by: "legacy".into(),
                    ranking_score_quote_atoms: Some(1),
                    expected_edge_quote_atoms: 1,
                    estimated_cost_lamports: Some(1),
                    estimated_cost_quote_atoms: Some(1),
                    expected_pnl_quote_atoms: Some(1),
                    expected_value_quote_atoms: Some(1),
                    p_land_bps: Some(10_000),
                    expected_shortfall_quote_atoms: Some(0),
                    intermediate_output_amounts: vec![10_100],
                    leg_snapshot_slots: vec![10, 10],
                    shadow_selected_by: Some("ev".into()),
                    shadow_trade_size: Some(10_000),
                    shadow_ranking_score_quote_atoms: Some(1),
                    shadow_expected_value_quote_atoms: Some(1),
                    shadow_expected_pnl_quote_atoms: Some(1),
                    realized_pnl_quote_atoms: None,
                    jito_tip_lamports: Some(1),
                    active_execution_buffer_bps: Some(50),
                    failure_program_id: None,
                    failure_instruction_index: None,
                    failure_custom_code: None,
                    failure_error_name: None,
                    endpoint: "jito".into(),
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    submitted_slot: Some(12),
                    submitted_at_unix_millis: 0,
                    updated_at_unix_millis: 0,
                    source_to_submit_nanos: Some(1),
                    submit_to_terminal_nanos: None,
                    source_to_terminal_nanos: None,
                }],
            },
        }
    }

    #[test]
    fn render_filtered_toml_keeps_only_eligible_routes() {
        let source = r#"
[runtime]
profile = "ultra_fast"

[routes]

[[routes.definitions]]
route_id = "keep-me"
default_trade_size = 1
max_trade_size = 1
legs = []
account_dependencies = []

[routes.definitions.execution]
default_compute_unit_limit = 1
default_compute_unit_price_micro_lamports = 1
default_jito_tip_lamports = 1

[[routes.definitions]]
route_id = "drop-me"
default_trade_size = 1
max_trade_size = 1
legs = []
account_dependencies = []

[routes.definitions.execution]
default_compute_unit_limit = 1
default_compute_unit_price_micro_lamports = 1
default_jito_tip_lamports = 1
"#;

        let eligible = BTreeSet::from(["keep-me".to_string()]);
        let rendered = render_filtered_toml(source, &eligible).expect("rendered");
        let value = toml::from_str::<toml::Value>(&rendered).expect("valid toml");
        let routes = value["routes"]["definitions"]
            .as_array()
            .expect("route definitions");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0]["route_id"].as_str(), Some("keep-me"));
    }

    fn test_app() -> App {
        App::with_snapshot(test_api_client(), test_snapshot())
    }

    #[test]
    fn left_right_switch_tabs_immediately() {
        let mut app = test_app();

        assert!(!handle_key(&mut app, key(KeyCode::Right)));

        assert_eq!(app.current_tab(), TabKind::Pools);

        handle_key(&mut app, key(KeyCode::Right));
        assert_eq!(app.current_tab(), TabKind::Signals);

        handle_key(&mut app, key(KeyCode::Left));
        assert_eq!(app.current_tab(), TabKind::Pools);
    }

    #[test]
    fn left_is_bounded_on_first_tab() {
        let mut app = test_app();

        handle_key(&mut app, key(KeyCode::Left));

        assert_eq!(app.current_tab(), TabKind::Overview);
    }

    #[test]
    fn up_and_down_move_selected_row() {
        let mut app = test_app();
        app.activate_tab(2);
        app.selected[2] = 2;

        handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(app.selected(), 1);

        handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(app.selected(), 0);

        handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(app.selected(), 0);

        handle_key(&mut app, key(KeyCode::Down));
        assert_eq!(app.selected(), 1);
    }

    #[test]
    fn enter_toggles_detail() {
        let mut app = test_app();

        handle_key(&mut app, key(KeyCode::Enter));
        assert!(app.show_detail);

        handle_key(&mut app, key(KeyCode::Enter));
        assert!(!app.show_detail);
    }

    #[test]
    fn numeric_shortcuts_and_tab_keep_cursor_synced_with_active_tab() {
        let mut app = test_app();

        handle_key(&mut app, key(KeyCode::Char('4')));
        assert_eq!(app.active_tab, 3);

        handle_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.active_tab, 4);

        handle_key(&mut app, key(KeyCode::Tab));
        assert_eq!(app.active_tab, 0);
    }

    #[test]
    fn input_mode_keeps_text_capture_priority_over_navigation() {
        let mut app = test_app();
        app.input_mode = true;

        handle_key(&mut app, key(KeyCode::Right));
        assert_eq!(app.current_tab(), TabKind::Overview);

        handle_key(&mut app, key(KeyCode::Char('x')));
        assert_eq!(app.filter_input, "x");

        handle_key(&mut app, key(KeyCode::Enter));
        assert!(!app.input_mode);
    }

    #[test]
    fn overview_uses_up_down_to_scroll_when_content_is_textual() {
        let mut app = test_app();

        handle_key(&mut app, key(KeyCode::Down));
        assert_eq!(app.body_scroll[0], 1);
        assert_eq!(app.selected[0], 0);

        handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(app.body_scroll[0], 0);
    }

    #[test]
    fn detail_popup_uses_up_down_for_scroll_instead_of_selection() {
        let mut app = test_app();
        app.activate_tab(1);
        app.selected[1] = 0;
        app.toggle_detail();

        handle_key(&mut app, key(KeyCode::Down));
        assert_eq!(app.detail_scroll, 1);
        assert_eq!(app.selected[1], 0);

        handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(app.detail_scroll, 0);
        assert_eq!(app.selected[1], 0);
    }

    #[test]
    fn overview_render_applies_scroll_offset() {
        let backend = TestBackend::new(70, 5);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut app = test_app();
        app.body_scroll[0] = 2;

        terminal
            .draw(|frame| render_overview_body(frame, frame.area(), &app))
            .expect("draw overview");

        let buffer = terminal.backend().buffer();
        let first_visible_line = (0..buffer.area.width)
            .map(|x| buffer[(x, 1)].symbol())
            .collect::<String>();

        assert!(first_visible_line.contains("rpc_slot=10"));
        assert!(!first_visible_line.contains("ready=true"));
    }

    #[test]
    fn active_tab_is_rendered_as_a_highlighted_pill() {
        let backend = TestBackend::new(60, 3);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut app = test_app();
        app.activate_tab(1);

        terminal
            .draw(|frame| render_tabs(frame, frame.area(), &app))
            .expect("draw tabs");

        let buffer = terminal.backend().buffer();
        let active_label = (0..buffer.area.width)
            .find(|&x| buffer[(x, 1)].symbol() == "P")
            .expect("pools tab");
        for x in (active_label - 1)..=(active_label + "Pools".len() as u16) {
            let cell = &buffer[(x, 1)];
            assert_eq!(cell.fg, Color::Black);
            assert_eq!(cell.bg, Color::Yellow);
            assert!(cell.modifier.contains(Modifier::BOLD));
        }

        let inactive_label = (0..buffer.area.width)
            .find(|&x| buffer[(x, 1)].symbol() == "O")
            .expect("overview tab");
        let inactive_cell = &buffer[(inactive_label, 1)];
        assert_eq!(inactive_cell.fg, Color::Gray);
        assert_eq!(inactive_cell.bg, Color::Reset);
    }
}
