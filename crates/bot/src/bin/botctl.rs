use std::{
    error::Error,
    io::{self, Stdout},
    thread,
    time::Duration,
};

use bot::observer::{
    MonitorOverview, MonitorSignalsResponse, MonitorSnapshot, MonitorTradeEvent,
    MonitorTradesResponse, PoolsResponse, RejectionEvent, RejectionsResponse,
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
    tab: usize,
    selected: [usize; 5],
    paused: bool,
    filter_input: String,
    input_mode: bool,
    show_detail: bool,
    pool_sort: usize,
}

impl App {
    fn new(api: ApiClient) -> Result<Self, Box<dyn Error>> {
        let snapshot = api.snapshot()?;
        Ok(Self {
            api,
            snapshot,
            tab: 0,
            selected: [0; 5],
            paused: false,
            filter_input: String::new(),
            input_mode: false,
            show_detail: false,
            pool_sort: 0,
        })
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
        TabKind::all()[self.tab]
    }

    fn selected_mut(&mut self) -> &mut usize {
        &mut self.selected[self.tab]
    }

    fn selected(&self) -> usize {
        self.selected[self.tab]
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
        Command::Export { command } => match command {
            ExportCommand::Snapshot { format } => export_snapshot(&api, &format),
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
        "routes: {}/{}",
        overview.ready_routes, overview.total_routes
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
    println!("detect_events: {}", overview.detect_events);
    println!("rejections: {}", overview.rejection_count);
    println!("submits: {}", overview.submit_count);
    println!("inclusions: {}", overview.inclusion_count);
    println!("landed_rate_bps: {}", overview.landed_rate_bps);
    println!("reject_rate_bps: {}", overview.reject_rate_bps);
    println!(
        "expected_session_pnl_lamports: {}",
        overview.expected_session_pnl_lamports
    );
    println!(
        "realized_session_pnl_lamports: {}",
        overview.realized_session_pnl_lamports
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
            "pool", "venues", "routes", "price", "fee", "depth", "lag", "stale",
        ],
        response.items.into_iter().map(|item| {
            [
                item.pool_id,
                item.venues.join(","),
                item.route_ids.join(","),
                item.price_bps.to_string(),
                item.fee_bps.to_string(),
                item.reserve_depth.to_string(),
                item.slot_lag.to_string(),
                item.stale.to_string(),
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
                item.expected_profit_lamports
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

fn export_snapshot(api: &ApiClient, format: &str) -> Result<(), Box<dyn Error>> {
    let snapshot = api.snapshot()?;
    match format {
        "json" => println!("{}", serde_json::to_string_pretty(&snapshot)?),
        other => return Err(format!("unsupported format: {other}").into()),
    }
    Ok(())
}

fn print_trade_table(items: Vec<MonitorTradeEvent>) {
    print_table(
        [
            "route",
            "submit",
            "outcome",
            "expected_pnl",
            "realized_pnl",
            "src_submit_ns",
            "src_term_ns",
            "tip",
            "slot",
            "submission_id",
        ],
        items.into_iter().map(|item| {
            [
                item.route_id,
                item.submit_status,
                item.outcome,
                item.expected_pnl_lamports
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.realized_pnl_lamports
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "N/A".into()),
                display_optional_u64(item.source_to_submit_nanos),
                display_optional_u64(item.source_to_terminal_nanos),
                item.jito_tip_lamports
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                item.build_slot.to_string(),
                item.submission_id,
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
    loop {
        terminal.draw(|frame| render(frame, &app))?;
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if handle_key(&mut app, key) {
                    return Ok(());
                }
            }
        } else {
            app.refresh();
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
        KeyCode::Char('1') => app.tab = 0,
        KeyCode::Char('2') => app.tab = 1,
        KeyCode::Char('3') => app.tab = 2,
        KeyCode::Char('4') => app.tab = 3,
        KeyCode::Char('5') => app.tab = 4,
        KeyCode::Tab => app.tab = (app.tab + 1) % TabKind::all().len(),
        KeyCode::Up => {
            let selected = app.selected_mut();
            *selected = selected.saturating_sub(1);
        }
        KeyCode::Down => {
            let selected = app.selected_mut();
            *selected = selected.saturating_add(1);
        }
        KeyCode::Enter => app.show_detail = !app.show_detail,
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
            Constraint::Length(2),
        ])
        .split(frame.area());

    render_header(frame, layout[0], &app.snapshot.overview);
    render_tabs(frame, layout[1], app.current_tab());
    render_body(frame, layout[2], app);
    render_footer(frame, layout[3], app);

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
                "{}/{}",
                overview.ready_routes, overview.total_routes
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
            Span::raw(overview.expected_session_pnl_lamports.to_string()),
            Span::raw("  "),
            Span::styled("realized pnl ", Style::default().fg(Color::Gray)),
            Span::raw(overview.realized_session_pnl_lamports.to_string()),
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

fn render_tabs(frame: &mut Frame, area: Rect, current: TabKind) {
    let titles = TabKind::all()
        .iter()
        .map(|tab| Line::from(tab.title()))
        .collect::<Vec<_>>();
    let selected = TabKind::all()
        .iter()
        .position(|tab| *tab == current)
        .unwrap_or(0);
    frame.render_widget(
        Tabs::new(titles)
            .select(selected)
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::ALL).title("Views")),
        area,
    );
}

fn render_body(frame: &mut Frame, area: Rect, app: &App) {
    match app.current_tab() {
        TabKind::Overview => render_overview_body(frame, area, &app.snapshot.overview),
        TabKind::Pools => render_pools_body(frame, area, app),
        TabKind::Signals => render_signals_body(frame, area, &app.snapshot.signals, app.selected()),
        TabKind::Rejects => render_rejects_body(frame, area, app),
        TabKind::Trades => render_trades_body(frame, area, app),
    }
}

fn render_overview_body(frame: &mut Frame, area: Rect, overview: &MonitorOverview) {
    let body = vec![
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
    ];
    frame.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title("Runtime"))
            .wrap(Wrap { trim: false }),
        area,
    );
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
            item.slot_lag.to_string(),
            if item.stale {
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
            Constraint::Length(6),
        ],
    )
    .header(
        Row::new([
            "pool", "venues", "routes", "price", "fee", "depth", "lag", "stale",
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
            item.expected_profit_lamports
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
            item.expected_pnl_lamports
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            item.realized_pnl_lamports
                .map(|value| value.to_string())
                .unwrap_or_else(|| "N/A".into()),
            display_optional_u64(item.source_to_submit_nanos),
            display_optional_u64(item.source_to_terminal_nanos),
            item.jito_tip_lamports
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            item.build_slot.to_string(),
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

fn render_footer(frame: &mut Frame, area: Rect, app: &App) {
    let sort = match app.pool_sort % 3 {
        1 => "depth",
        2 => "price",
        _ => "freshness",
    };
    let footer = Line::from(vec![
        Span::styled("q", Style::default().fg(Color::Yellow)),
        Span::raw(" quit  "),
        Span::styled("1-5/Tab", Style::default().fg(Color::Yellow)),
        Span::raw(" tabs  "),
        Span::styled("/", Style::default().fg(Color::Yellow)),
        Span::raw(" filter  "),
        Span::styled("s", Style::default().fg(Color::Yellow)),
        Span::raw(format!(" sort({sort})  ")),
        Span::styled("p", Style::default().fg(Color::Yellow)),
        Span::raw(format!(" pause={}  ", app.paused)),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(" detail  "),
        Span::styled("filter", Style::default().fg(Color::Gray)),
        Span::raw(format!("={}", app.filter_input)),
    ]);
    frame.render_widget(
        Paragraph::new(footer).block(Block::default().borders(Borders::ALL).title("Keys")),
        area,
    );
}

fn render_detail(frame: &mut Frame, app: &App) {
    let text = match app.current_tab() {
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
                            "depth={} lag={} stale={}",
                            item.reserve_depth, item.slot_lag, item.stale
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
                        item.expected_profit_lamports
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
                        item.expected_pnl_lamports
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                        item.realized_pnl_lamports
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
                ]
            })
            .unwrap_or_else(|| vec![Line::from("No selection")]),
    };
    let area = centered_rect(70, 40, frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Detail"))
            .wrap(Wrap { trim: false }),
        area,
    );
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
