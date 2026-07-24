use std::{
    collections::{HashMap, HashSet},
    io::{self, Stdout},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::client::MonitorClient;
use agent_cow_core::{
    ActivityEvent, ActivityKind, MachineOverview, ProviderKind, ProviderQuota,
    SessionActivityState, SessionDetail, SessionList, SessionLoadProgress, SessionQuery,
    SessionStatusKind, SessionSummary, TokenUsage, UsageOverview,
};
use anyhow::Result;
use chrono::{DateTime, Local, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::task::JoinHandle;

const FAST_LIST_LIMIT: usize = 64;
const FULL_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const UI_POLL_INTERVAL_BUSY: Duration = Duration::from_millis(100);
const UI_POLL_INTERVAL_IDLE: Duration = Duration::from_millis(250);
const FOCUS_SESSION_WINDOW_HOURS: i64 = 24;
const RECENT_SESSION_WINDOW_DAYS: i64 = 7;
const VISIBLE_STATE_REFRESH_WINDOW_MINUTES: i64 = 5;
const VISIBLE_STATE_REFRESH_BUDGET: usize = 8;

pub async fn run(
    client: Arc<dyn MonitorClient>,
    limit: Option<usize>,
    refresh_secs: u64,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, client, limit, refresh_secs).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListRefreshKind {
    Fast,
    Progressive,
    Full,
}

fn spawn_list_refresh(
    client: Arc<dyn MonitorClient>,
    limit: Option<usize>,
    kind: ListRefreshKind,
) -> ListRefreshTask {
    let (progress_tx, progress_rx) = unbounded_channel();
    let handle = tokio::spawn(async move {
        let list = client
            .list_sessions_with_progress(
                SessionQuery {
                    include_archived: false,
                    limit,
                },
                Some(progress_tx),
            )
            .await?;
        Ok((kind, list))
    });
    ListRefreshTask {
        handle,
        progress_rx,
    }
}

fn queue_progressive_refresh(
    app: &mut TuiApp,
    client: &Arc<dyn MonitorClient>,
    refresh: &mut Option<ListRefreshTask>,
) {
    let next_limit = app.next_progress_limit();
    if next_limit <= app.sessions.len() {
        app.loading_more = false;
        app.full_load_complete = true;
        return;
    }

    app.loading_more = true;
    app.loading_progress_total = app
        .loading_progress_total
        .max(app.overview.total_sessions)
        .max(app.sessions.len());
    *refresh = Some(spawn_list_refresh(
        client.clone(),
        Some(next_limit),
        ListRefreshKind::Progressive,
    ));
}

struct ListRefreshTask {
    handle: JoinHandle<Result<(ListRefreshKind, SessionList)>>,
    progress_rx: UnboundedReceiver<SessionLoadProgress>,
}

fn spawn_detail_refresh(
    client: Arc<dyn MonitorClient>,
    session_id: String,
) -> JoinHandle<Result<(String, SessionDetail)>> {
    tokio::spawn(async move {
        let detail = client.get_session(&session_id).await?;
        Ok((session_id, detail))
    })
}

fn spawn_visible_summary_refresh(
    client: Arc<dyn MonitorClient>,
    session_ids: Vec<String>,
) -> JoinHandle<Vec<SessionSummary>> {
    tokio::spawn(async move {
        let mut summaries = Vec::with_capacity(session_ids.len());
        for session_id in session_ids {
            if let Ok(detail) = client.get_session(&session_id).await {
                summaries.push(detail.summary);
            }
        }
        summaries
    })
}

fn queue_detail_refresh(
    app: &mut TuiApp,
    client: &Arc<dyn MonitorClient>,
    detail_refresh: &mut Option<JoinHandle<Result<(String, SessionDetail)>>>,
) {
    let Some(session_id) = app.selected_session_id().map(ToOwned::to_owned) else {
        app.detail_loading = false;
        app.detail_loading_session_id = None;
        return;
    };
    if app.detail_loading && app.detail_loading_session_id.as_deref() == Some(session_id.as_str()) {
        return;
    }
    if let Some(handle) = detail_refresh.take() {
        handle.abort();
    }
    let current_detail_matches =
        app.detail.as_ref().map(|detail| detail.summary.id.as_str()) == Some(session_id.as_str());
    if !current_detail_matches {
        app.detail = None;
        app.detail_scroll = 0;
    }
    app.detail_loading = true;
    app.detail_loading_session_id = Some(session_id.clone());
    *detail_refresh = Some(spawn_detail_refresh(client.clone(), session_id));
}

async fn start_live_updates(
    app: &TuiApp,
    client: &Arc<dyn MonitorClient>,
) -> Result<Option<UnboundedReceiver<SessionList>>> {
    if app.loading_more || !app.full_load_complete {
        return Ok(None);
    }

    client
        .subscribe_sessions(
            SessionQuery {
                include_archived: false,
                limit: app.requested_limit,
            },
            app.refresh_every,
        )
        .await
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    client: Arc<dyn MonitorClient>,
    limit: Option<usize>,
    refresh_secs: u64,
) -> Result<()> {
    let mut app = TuiApp::new(limit, Duration::from_secs(refresh_secs));
    let mut initial_load = spawn_list_refresh(
        client.clone(),
        Some(app.fast_limit()),
        ListRefreshKind::Fast,
    );
    let mut initial_progress = SessionLoadProgress::default();

    loop {
        while let Ok(progress) = initial_load.progress_rx.try_recv() {
            initial_progress = progress;
        }

        terminal.draw(|frame| draw_loading(frame, &initial_progress))?;

        if initial_load.handle.is_finished() {
            match initial_load.handle.await? {
                Ok((kind, response)) => app.apply_list_refresh(kind, response),
                Err(error) => {
                    app.error = Some(error.to_string());
                    app.apply_list_refresh(
                        ListRefreshKind::Fast,
                        SessionList {
                            generated_at: Utc::now(),
                            overview: UsageOverview::default(),
                            sessions: Vec::new(),
                        },
                    );
                }
            }
            break;
        }

        if event::poll(UI_POLL_INTERVAL_BUSY)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            return Ok(());
        }
    }

    let mut fast_refresh: Option<ListRefreshTask> = None;
    let mut full_refresh: Option<ListRefreshTask> = None;
    let mut detail_refresh: Option<JoinHandle<Result<(String, SessionDetail)>>> = None;
    let mut visible_summary_refresh: Option<JoinHandle<Vec<SessionSummary>>> = None;
    let mut live_updates = match start_live_updates(&app, &client).await {
        Ok(receiver) => receiver,
        Err(error) => {
            app.error = Some(error.to_string());
            None
        }
    };

    if app.should_load_more() {
        queue_progressive_refresh(&mut app, &client, &mut full_refresh);
        live_updates = None;
    }

    loop {
        if let Some(receiver) = live_updates.as_mut() {
            loop {
                match receiver.try_recv() {
                    Ok(list) => {
                        app.apply_live_update(list);
                        if app.detail_mode {
                            queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        live_updates = None;
                        break;
                    }
                }
            }
        }

        if let Some(task) = fast_refresh.as_mut() {
            while let Ok(progress) = task.progress_rx.try_recv() {
                app.apply_load_progress(progress);
            }
        }
        if let Some(handle) = fast_refresh.as_ref().map(|task| &task.handle)
            && handle.is_finished()
        {
            match fast_refresh.take().unwrap().handle.await? {
                Ok((kind, response)) => {
                    app.apply_list_refresh(kind, response);
                    if app.detail_mode {
                        queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                    }
                }
                Err(error) => {
                    app.error = Some(error.to_string());
                    app.loading_more = false;
                    app.last_full_refresh = Instant::now();
                }
            }
        }

        if let Some(task) = full_refresh.as_mut() {
            while let Ok(progress) = task.progress_rx.try_recv() {
                app.apply_load_progress(progress);
            }
        }
        if let Some(handle) = full_refresh.as_ref().map(|task| &task.handle)
            && handle.is_finished()
        {
            match full_refresh.take().unwrap().handle.await? {
                Ok((kind, response)) => {
                    app.apply_list_refresh(kind, response);
                    if kind == ListRefreshKind::Progressive && app.should_load_more() {
                        queue_progressive_refresh(&mut app, &client, &mut full_refresh);
                        live_updates = None;
                    } else if live_updates.is_none() {
                        live_updates = match start_live_updates(&app, &client).await {
                            Ok(receiver) => receiver,
                            Err(error) => {
                                app.error = Some(error.to_string());
                                None
                            }
                        };
                    }
                    if app.detail_mode {
                        queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                    }
                }
                Err(error) => {
                    app.error = Some(error.to_string());
                    app.loading_more = false;
                    app.last_full_refresh = Instant::now();
                }
            }
        }

        if let Some(handle) = detail_refresh.as_ref()
            && handle.is_finished()
        {
            match detail_refresh.take().unwrap().await? {
                Ok((session_id, detail)) => {
                    if app.detail_mode && app.selected_session_id() == Some(session_id.as_str()) {
                        app.detail = Some(detail);
                        if let Some((detail_id, updated_at)) = app
                            .detail
                            .as_ref()
                            .map(|detail| (detail.summary.id.clone(), detail.summary.updated_at))
                        {
                            app.mark_summary_hydrated(&detail_id, updated_at);
                        }
                        app.sync_detail_summary_into_sessions();
                        app.rebuild_filter(Some(session_id.as_str()));
                        app.detail_loading = false;
                        app.detail_loading_session_id = None;
                        app.error = None;
                    } else {
                        app.detail_loading = false;
                        app.detail_loading_session_id = None;
                    }
                }
                Err(error) => {
                    app.detail_loading = false;
                    app.detail_loading_session_id = None;
                    app.error = Some(error.to_string());
                }
            }
        }

        if let Some(handle) = visible_summary_refresh.as_ref()
            && handle.is_finished()
        {
            let summaries = visible_summary_refresh.take().unwrap().await?;
            app.apply_visible_summary_refresh(summaries);
        }

        terminal.draw(|frame| draw(frame, &mut app))?;

        if visible_summary_refresh.is_none()
            && fast_refresh.is_none()
            && full_refresh.is_none()
            && detail_refresh.is_none()
            && let Some(area) = sessions_table_area(
                &app,
                terminal
                    .size()
                    .map(|size| Rect::new(0, 0, size.width, size.height))?,
            )
        {
            let session_ids = app.visible_state_refresh_candidates(area);
            if !session_ids.is_empty() {
                visible_summary_refresh =
                    Some(spawn_visible_summary_refresh(client.clone(), session_ids));
            }
        }

        let poll_interval = if app.detail_loading
            || app.loading_more
            || fast_refresh.is_some()
            || full_refresh.is_some()
            || detail_refresh.is_some()
            || visible_summary_refresh.is_some()
        {
            UI_POLL_INTERVAL_BUSY
        } else {
            UI_POLL_INTERVAL_IDLE
        };

        if event::poll(poll_interval)?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            if app.filter_mode {
                match key.code {
                    KeyCode::Esc => {
                        app.clear_filter();
                        app.filter_mode = false;
                    }
                    KeyCode::Enter => app.filter_mode = false,
                    KeyCode::Backspace => app.pop_filter_char(),
                    KeyCode::Char(ch) => app.push_filter_char(ch),
                    _ => {}
                }
            } else {
                let terminal_size = terminal.size()?;
                let detail_max_scroll = app.detail_max_scroll(Rect::new(
                    0,
                    0,
                    terminal_size.width,
                    terminal_size.height,
                ));
                match key.code {
                    KeyCode::Char('q') => {
                        if app.detail_mode {
                            app.close_detail_mode();
                        } else {
                            break;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') if app.detail_mode => {
                        app.detail_scroll_down(1, detail_max_scroll)
                    }
                    KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                    KeyCode::Up | KeyCode::Char('k') if app.detail_mode => app.detail_scroll_up(1),
                    KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                    KeyCode::PageDown if app.detail_mode => {
                        app.detail_scroll_down(10, detail_max_scroll)
                    }
                    KeyCode::PageDown => app.page_down(10),
                    KeyCode::PageUp if app.detail_mode => app.detail_scroll_up(10),
                    KeyCode::PageUp => app.page_up(10),
                    KeyCode::Home | KeyCode::Char('g') if app.detail_mode => {
                        app.detail_scroll_top()
                    }
                    KeyCode::Home | KeyCode::Char('g') => app.select_first(),
                    KeyCode::End | KeyCode::Char('G') if app.detail_mode => {
                        app.detail_scroll_bottom(detail_max_scroll)
                    }
                    KeyCode::End | KeyCode::Char('G') => app.select_last(),
                    KeyCode::Char('/') if !app.detail_mode => app.begin_filter(),
                    KeyCode::Char('v') if !app.detail_mode => app.cycle_view_mode(),
                    KeyCode::Char('m') if !app.detail_mode => app.cycle_machine_scope(),
                    KeyCode::Esc if app.detail_mode => app.close_detail_mode(),
                    KeyCode::Esc => app.clear_filter(),
                    KeyCode::Char('r') if fast_refresh.is_none() && full_refresh.is_none() => {
                        if app.should_load_more() {
                            queue_progressive_refresh(&mut app, &client, &mut full_refresh);
                        } else if app.should_run_full_refresh() {
                            full_refresh = Some(spawn_list_refresh(
                                client.clone(),
                                app.requested_limit,
                                ListRefreshKind::Full,
                            ));
                        } else {
                            fast_refresh = Some(spawn_list_refresh(
                                client.clone(),
                                Some(app.fast_limit()),
                                ListRefreshKind::Fast,
                            ));
                        }
                    }
                    KeyCode::Enter if !app.detail_mode => {
                        let needs_refresh = app.open_detail_pane(DetailPane::Describe);
                        if needs_refresh {
                            queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                        }
                    }
                    KeyCode::Char('f') if app.detail_mode => {
                        let target = match app.detail_pane {
                            DetailPane::Describe => DetailPane::Follow,
                            DetailPane::Follow => DetailPane::Follow,
                        };
                        let needs_refresh = app.open_detail_pane(target);
                        if needs_refresh {
                            queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                        }
                    }
                    KeyCode::Char('f') => {
                        let needs_refresh = app.open_detail_pane(DetailPane::Follow);
                        if needs_refresh {
                            queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                        }
                    }
                    KeyCode::Char('d') if app.detail_mode => {
                        let needs_refresh = app.open_detail_pane(DetailPane::Describe);
                        if needs_refresh {
                            queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                        }
                    }
                    KeyCode::Enter => app.close_detail_mode(),
                    KeyCode::Char('o') => app.open_selected_app(&client).await,
                    _ => {}
                }
            }

            if app.selection_changed {
                if app.detail_mode {
                    queue_detail_refresh(&mut app, &client, &mut detail_refresh);
                }
                app.selection_changed = false;
            }
        }

        if live_updates.is_none()
            && app.last_refresh.elapsed() >= app.refresh_every
            && fast_refresh.is_none()
            && full_refresh.is_none()
        {
            if app.should_load_more() {
                queue_progressive_refresh(&mut app, &client, &mut full_refresh);
            } else if app.should_run_full_refresh() {
                full_refresh = Some(spawn_list_refresh(
                    client.clone(),
                    app.requested_limit,
                    ListRefreshKind::Full,
                ));
            } else {
                fast_refresh = Some(spawn_list_refresh(
                    client.clone(),
                    Some(app.fast_limit()),
                    ListRefreshKind::Fast,
                ));
            }
        }
    }

    Ok(())
}

struct TuiApp {
    sessions: Vec<SessionSummary>,
    overview: UsageOverview,
    filtered_indices: Vec<usize>,
    machine_labels_cache: Vec<String>,
    view_scoped_session_count: usize,
    next_view_scoped_session_count: usize,
    visible_total_tokens_cache: u64,
    visible_total_cost_usd_cache: f64,
    visible_has_multiple_hosts: bool,
    machine_scope: Option<String>,
    detail: Option<SessionDetail>,
    detail_loading: bool,
    detail_loading_session_id: Option<String>,
    table_state: TableState,
    detail_mode: bool,
    detail_pane: DetailPane,
    detail_scroll: u16,
    follow_stick_to_bottom: bool,
    requested_limit: Option<usize>,
    refresh_every: Duration,
    last_refresh: Instant,
    last_full_refresh: Instant,
    full_refresh_every: Duration,
    full_load_complete: bool,
    loading_more: bool,
    loading_progress_loaded: usize,
    loading_progress_total: usize,
    loading_progress_sources: HashMap<String, (usize, usize)>,
    visible_state_hydrated_at: HashMap<String, DateTime<Utc>>,
    error: Option<String>,
    notice: Option<UiNotice>,
    filter_input: String,
    filter_mode: bool,
    view_mode: SessionViewMode,
    selection_changed: bool,
}

struct UiNotice {
    message: String,
    is_error: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DetailPane {
    Describe,
    Follow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionViewMode {
    Focus,
    Recent,
    All,
}

impl SessionViewMode {
    fn label(self) -> &'static str {
        match self {
            Self::Focus => "Focus",
            Self::Recent => "Recent",
            Self::All => "All",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Focus => Self::Recent,
            Self::Recent => Self::All,
            Self::All => Self::Focus,
        }
    }

    fn hidden_hint(self) -> &'static str {
        match self {
            Self::Focus => "older sessions hidden • press v for Recent",
            Self::Recent => "older sessions hidden • press v for All",
            Self::All => "",
        }
    }
}

impl TuiApp {
    fn new(limit: Option<usize>, refresh_every: Duration) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        let now = Instant::now();

        Self {
            sessions: Vec::new(),
            overview: UsageOverview::default(),
            filtered_indices: Vec::new(),
            machine_labels_cache: Vec::new(),
            view_scoped_session_count: 0,
            next_view_scoped_session_count: 0,
            visible_total_tokens_cache: 0,
            visible_total_cost_usd_cache: 0.0,
            visible_has_multiple_hosts: false,
            machine_scope: None,
            detail: None,
            detail_loading: false,
            detail_loading_session_id: None,
            table_state,
            detail_mode: false,
            detail_pane: DetailPane::Describe,
            detail_scroll: 0,
            follow_stick_to_bottom: false,
            requested_limit: limit,
            refresh_every,
            last_refresh: now,
            last_full_refresh: now,
            full_refresh_every: FULL_REFRESH_INTERVAL,
            full_load_complete: limit.is_some(),
            loading_more: false,
            loading_progress_loaded: 0,
            loading_progress_total: 0,
            loading_progress_sources: HashMap::new(),
            visible_state_hydrated_at: HashMap::new(),
            error: None,
            notice: None,
            filter_input: String::new(),
            filter_mode: false,
            view_mode: SessionViewMode::Focus,
            selection_changed: false,
        }
    }

    fn fast_limit(&self) -> usize {
        self.requested_limit
            .unwrap_or(FAST_LIST_LIMIT)
            .clamp(1, FAST_LIST_LIMIT)
    }

    fn should_load_more(&self) -> bool {
        !self.full_load_complete && self.sessions.len() < self.overview.total_sessions
    }

    fn should_run_full_refresh(&self) -> bool {
        self.requested_limit.is_none()
            && (!self.full_load_complete
                || self.last_full_refresh.elapsed() >= self.full_refresh_every)
    }

    fn next_progress_limit(&self) -> usize {
        let total = self.overview.total_sessions.max(self.sessions.len());
        let current = self.sessions.len().max(self.fast_limit());
        if total <= current {
            return total;
        }
        (current.saturating_mul(2)).min(total)
    }

    fn apply_list_refresh(&mut self, kind: ListRefreshKind, response: SessionList) {
        match kind {
            ListRefreshKind::Fast => {
                if self.full_load_complete {
                    self.merge_session_list(response);
                } else {
                    self.replace_session_list(response);
                    self.full_load_complete = self.sessions.len() >= self.overview.total_sessions;
                    self.loading_more = self.should_load_more();
                }
            }
            ListRefreshKind::Progressive => {
                self.replace_session_list(response);
                self.full_load_complete = self.requested_limit.is_some()
                    || self.sessions.len() >= self.overview.total_sessions;
                self.loading_more = self.should_load_more();
                if self.full_load_complete {
                    self.last_full_refresh = Instant::now();
                }
            }
            ListRefreshKind::Full => {
                self.replace_session_list(response);
                self.full_load_complete = self.requested_limit.is_none()
                    || self.sessions.len() >= self.overview.total_sessions;
                self.loading_more = false;
                self.last_full_refresh = Instant::now();
            }
        }
        self.loading_progress_loaded = self.loading_progress_loaded.max(self.sessions.len());
        self.loading_progress_total = self.overview.total_sessions.max(self.sessions.len());
    }

    fn apply_load_progress(&mut self, progress: SessionLoadProgress) {
        self.loading_progress_loaded = self.loading_progress_loaded.max(progress.loaded_sessions);
        self.loading_progress_total = self.loading_progress_total.max(progress.total_sessions);
        if !progress.sources.is_empty() {
            self.loading_progress_sources = progress
                .sources
                .into_iter()
                .map(|source| {
                    (
                        source.source,
                        (source.loaded_sessions, source.total_sessions),
                    )
                })
                .collect();
        }
    }

    fn apply_live_update(&mut self, response: SessionList) {
        self.replace_session_list(response);
        self.loading_progress_loaded = self.loading_progress_loaded.max(self.sessions.len());
        self.loading_progress_total = self.overview.total_sessions.max(self.sessions.len());
        self.last_full_refresh = Instant::now();
    }

    fn replace_session_list(&mut self, response: SessionList) {
        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.sessions = response.sessions;
        self.overview = response.overview;
        self.last_refresh = Instant::now();
        self.error = None;
        self.prune_visible_state_cache();
        self.refresh_machine_labels_cache();
        self.rebuild_filter(selected_id.as_deref());

        if let Some(selected_id) = selected_id.as_deref()
            && let Some(detail) = self.detail.as_ref()
            && detail.summary.id == selected_id
            && let Some(summary) = self
                .sessions
                .iter_mut()
                .find(|session| session.id == selected_id)
            && detail.summary.updated_at >= summary.updated_at
        {
            *summary = detail.summary.clone();
        }

        if self.filtered_indices.is_empty() {
            self.detail = None;
            self.detail_loading = false;
            self.detail_loading_session_id = None;
            self.detail_mode = false;
            self.detail_scroll = 0;
            self.follow_stick_to_bottom = false;
        }
    }

    fn merge_session_list(&mut self, response: SessionList) {
        if self.sessions.is_empty() {
            self.replace_session_list(response);
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.last_refresh = Instant::now();
        self.error = None;
        self.overview.total_sessions = response.overview.total_sessions;
        self.overview.quotas = response.overview.quotas;

        for session in response.sessions {
            if let Some(existing) = self.sessions.iter_mut().find(|item| item.id == session.id) {
                *existing = session;
            } else {
                self.sessions.push(session);
            }
        }

        self.sessions
            .sort_by_key(|session| std::cmp::Reverse(session.updated_at));
        self.prune_visible_state_cache();
        self.refresh_machine_labels_cache();
        self.rebuild_filter(selected_id.as_deref());

        if let Some(selected_id) = selected_id.as_deref()
            && let Some(detail) = self.detail.as_ref()
            && detail.summary.id == selected_id
            && let Some(summary) = self
                .sessions
                .iter_mut()
                .find(|session| session.id == selected_id)
            && detail.summary.updated_at >= summary.updated_at
        {
            *summary = detail.summary.clone();
        }
    }

    fn sync_detail_summary_into_sessions(&mut self) {
        let Some(detail) = self.detail.as_ref() else {
            return;
        };
        let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.id == detail.summary.id)
        else {
            return;
        };

        *session = detail.summary.clone();
    }

    fn prune_visible_state_cache(&mut self) {
        let session_ids: HashSet<_> = self
            .sessions
            .iter()
            .map(|session| session.id.as_str())
            .collect();
        self.visible_state_hydrated_at
            .retain(|session_id, _| session_ids.contains(session_id.as_str()));
    }

    fn mark_summary_hydrated(&mut self, session_id: &str, updated_at: DateTime<Utc>) {
        self.visible_state_hydrated_at
            .insert(session_id.to_string(), updated_at);
    }

    fn apply_visible_summary_refresh(&mut self, summaries: Vec<SessionSummary>) {
        if summaries.is_empty() {
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        let mut changed = false;

        for summary in summaries {
            self.visible_state_hydrated_at
                .insert(summary.id.clone(), summary.updated_at);

            if let Some(detail) = self.detail.as_mut()
                && detail.summary.id == summary.id
                && summary.updated_at >= detail.summary.updated_at
            {
                detail.summary = summary.clone();
            }

            if let Some(existing) = self
                .sessions
                .iter_mut()
                .find(|session| session.id == summary.id)
                && summary.updated_at >= existing.updated_at
                && (existing.status.kind != summary.status.kind
                    || existing.activity_state != summary.activity_state
                    || existing.run_active != summary.run_active)
            {
                *existing = summary;
                changed = true;
            }
        }

        if changed {
            self.rebuild_filter(selected_id.as_deref());
        }
    }

    fn visible_state_refresh_candidates(&mut self, area: Rect) -> Vec<String> {
        if self.filtered_indices.is_empty() || self.detail_mode {
            return Vec::new();
        }

        let window = table_visible_window(self, area);
        if window.start >= window.end {
            return Vec::new();
        }

        let now = Utc::now();
        self.filtered_indices[window.start..window.end]
            .iter()
            .filter_map(|index| self.sessions.get(*index))
            .filter(|summary| should_refresh_visible_summary(summary, now))
            .filter(|summary| {
                self.visible_state_hydrated_at.get(&summary.id).copied() != Some(summary.updated_at)
            })
            .take(VISIBLE_STATE_REFRESH_BUDGET)
            .map(|summary| summary.id.clone())
            .collect()
    }

    fn begin_filter(&mut self) {
        self.filter_mode = true;
        self.notice = None;
    }

    fn clear_filter(&mut self) {
        if self.filter_input.is_empty() {
            self.filter_mode = false;
            self.notice = None;
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.filter_input.clear();
        self.filter_mode = false;
        self.rebuild_filter(selected_id.as_deref());
        self.notice = Some(UiNotice {
            message: "Filter cleared.".to_string(),
            is_error: false,
        });
        self.selection_changed = true;
    }

    fn push_filter_char(&mut self, ch: char) {
        if ch.is_control() {
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.filter_input.push(ch);
        self.rebuild_filter(selected_id.as_deref());
        self.notice = None;
        self.selection_changed = true;
    }

    fn pop_filter_char(&mut self) {
        if self.filter_input.is_empty() {
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.filter_input.pop();
        self.rebuild_filter(selected_id.as_deref());
        self.notice = None;
        self.selection_changed = true;
    }

    fn rebuild_filter(&mut self, preserve_id: Option<&str>) {
        let filter = self.filter_input.trim().to_lowercase();
        let now = Utc::now();
        let next_view_mode = self.view_mode.next();
        let mut filtered_indices = Vec::new();
        let mut view_scoped_session_count = 0usize;
        let mut next_view_scoped_session_count = 0usize;
        let mut visible_total_tokens = 0u64;
        let mut visible_total_cost_usd = 0.0f64;
        let mut first_visible_host: Option<&str> = None;
        let mut visible_has_multiple_hosts = false;

        for (index, session) in self.sessions.iter().enumerate() {
            if !matches_machine_scope(session, self.machine_scope.as_deref()) {
                continue;
            }

            let in_current_view = matches_view_mode(session, self.view_mode, now);
            if in_current_view {
                view_scoped_session_count += 1;
            }
            if matches_view_mode(session, next_view_mode, now) {
                next_view_scoped_session_count += 1;
            }

            if !(in_current_view && matches_text_filter(session, &filter)) {
                continue;
            }

            filtered_indices.push(index);
            visible_total_tokens = visible_total_tokens.saturating_add(session.tokens.total_tokens);
            visible_total_cost_usd += session
                .cost
                .as_ref()
                .map(|cost| cost.total_usd)
                .unwrap_or(0.0);

            if !visible_has_multiple_hosts {
                match first_visible_host {
                    None => first_visible_host = Some(session.machine_label.as_str()),
                    Some(host) if host != session.machine_label => {
                        visible_has_multiple_hosts = true
                    }
                    Some(_) => {}
                }
            }
        }

        self.filtered_indices = filtered_indices;
        self.view_scoped_session_count = view_scoped_session_count;
        self.next_view_scoped_session_count = next_view_scoped_session_count;
        self.visible_total_tokens_cache = visible_total_tokens;
        self.visible_total_cost_usd_cache = visible_total_cost_usd;
        self.visible_has_multiple_hosts = visible_has_multiple_hosts;

        if self.filtered_indices.is_empty() {
            self.table_state.select(None);
            self.detail = None;
            self.detail_loading = false;
            self.detail_loading_session_id = None;
            self.detail_mode = false;
            self.detail_scroll = 0;
            return;
        }

        let current_visible = self
            .table_state
            .selected()
            .filter(|index| *index < self.filtered_indices.len());

        let next_selected = preserve_id
            .and_then(|id| {
                self.filtered_indices
                    .iter()
                    .position(|index| self.sessions[*index].id == id)
            })
            .or(current_visible)
            .unwrap_or(0)
            .min(self.filtered_indices.len() - 1);

        self.table_state.select(Some(next_selected));
    }

    fn cycle_view_mode(&mut self) {
        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.view_mode = self.view_mode.next();
        self.rebuild_filter(selected_id.as_deref());
        self.selection_changed = true;
        self.notice = Some(UiNotice {
            message: format!("View: {}", self.view_mode.label()),
            is_error: false,
        });
    }

    fn selected_session_id(&self) -> Option<&str> {
        self.table_state
            .selected()
            .and_then(|visible_index| self.filtered_indices.get(visible_index))
            .and_then(|session_index| self.sessions.get(*session_index))
            .map(|session| session.id.as_str())
    }

    fn selected_summary(&self) -> Option<&SessionSummary> {
        self.table_state
            .selected()
            .and_then(|visible_index| self.filtered_indices.get(visible_index))
            .and_then(|session_index| self.sessions.get(*session_index))
            .or_else(|| self.detail.as_ref().map(|detail| &detail.summary))
    }

    fn refresh_machine_labels_cache(&mut self) {
        let mut labels = if !self.overview.machines.is_empty() {
            self.overview
                .machines
                .iter()
                .map(|machine| machine.machine_label.clone())
                .collect::<Vec<_>>()
        } else {
            self.sessions
                .iter()
                .map(|session| session.machine_label.clone())
                .collect::<Vec<_>>()
        };
        labels.sort();
        labels.dedup();
        self.machine_labels_cache = labels;
    }

    fn scoped_machine_overview(&self) -> Option<&MachineOverview> {
        let scope = self.machine_scope.as_deref()?;
        self.overview
            .machines
            .iter()
            .find(|machine| machine.machine_label == scope)
    }

    fn current_machine_overview(&self) -> Option<&MachineOverview> {
        self.scoped_machine_overview()
            .or(match self.overview.machines.as_slice() {
                [single] => Some(single),
                _ => None,
            })
    }

    fn active_loading_label(&self) -> Option<String> {
        if !self.loading_more {
            return None;
        }

        let shown = self.filtered_indices.len();
        Some(format!("{shown} shown"))
    }

    fn hidden_session_count(&self) -> usize {
        if !self.filter_input.is_empty() || matches!(self.view_mode, SessionViewMode::All) {
            return 0;
        }

        self.next_view_scoped_session_count
            .saturating_sub(self.view_scoped_session_count)
    }

    fn scoped_view_session_count(&self) -> usize {
        self.view_scoped_session_count
    }

    fn visible_total_tokens(&self) -> u64 {
        self.visible_total_tokens_cache
    }

    fn visible_total_cost_usd(&self) -> f64 {
        self.visible_total_cost_usd_cache
    }

    fn machine_labels(&self) -> Vec<String> {
        self.machine_labels_cache.clone()
    }

    fn select_next(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        let next = match self.table_state.selected() {
            Some(index) if index + 1 < self.filtered_indices.len() => index + 1,
            _ => 0,
        };

        self.table_state.select(Some(next));
        self.selection_changed = true;
    }

    fn select_previous(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        let previous = match self.table_state.selected() {
            Some(index) if index > 0 => index - 1,
            _ => self.filtered_indices.len() - 1,
        };

        self.table_state.select(Some(previous));
        self.selection_changed = true;
    }

    fn select_first(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        self.table_state.select(Some(0));
        self.selection_changed = true;
    }

    fn select_last(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        self.table_state
            .select(Some(self.filtered_indices.len() - 1));
        self.selection_changed = true;
    }

    fn page_down(&mut self, step: usize) {
        if self.filtered_indices.is_empty() {
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let next = (current + step).min(self.filtered_indices.len() - 1);
        self.table_state.select(Some(next));
        self.selection_changed = true;
    }

    fn page_up(&mut self, step: usize) {
        if self.filtered_indices.is_empty() {
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let previous = current.saturating_sub(step);
        self.table_state.select(Some(previous));
        self.selection_changed = true;
    }

    fn cycle_machine_scope(&mut self) {
        let labels = self.machine_labels();
        if labels.len() <= 1 {
            self.notice = Some(UiNotice {
                message: "Only one machine is available.".to_string(),
                is_error: false,
            });
            return;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.machine_scope = match self.machine_scope.as_deref() {
            None => labels.first().cloned(),
            Some(current) => labels
                .iter()
                .position(|label| label == current)
                .and_then(|index| labels.get(index + 1).cloned()),
        };

        self.rebuild_filter(selected_id.as_deref());
        self.selection_changed = true;
        self.notice = Some(UiNotice {
            message: match self.machine_scope.as_deref() {
                Some(scope) => format!("Machine scope: {scope}"),
                None => "Machine scope: all".to_string(),
            },
            is_error: false,
        });
    }

    async fn open_selected_app(&mut self, client: &Arc<dyn MonitorClient>) {
        let Some(session_id) = self.selected_session_id().map(ToOwned::to_owned) else {
            self.notice = Some(UiNotice {
                message: "No session selected.".to_string(),
                is_error: true,
            });
            return;
        };

        self.notice = Some(match client.open_session_app(&session_id).await {
            Ok(action) => UiNotice {
                message: format!("Opened {}.", action.label),
                is_error: false,
            },
            Err(error) => UiNotice {
                message: error.to_string(),
                is_error: true,
            },
        });
    }

    fn open_detail_pane(&mut self, pane: DetailPane) -> bool {
        if self.selected_summary().is_none() && self.current_machine_overview().is_none() {
            self.notice = Some(UiNotice {
                message: "No session selected.".to_string(),
                is_error: true,
            });
            return false;
        }

        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        let matches_selected_detail = selected_id.as_deref()
            == self
                .detail
                .as_ref()
                .map(|detail| detail.summary.id.as_str());

        self.detail_mode = true;
        self.detail_pane = pane;
        self.detail_scroll = 0;
        self.follow_stick_to_bottom = pane == DetailPane::Follow;
        if !matches_selected_detail {
            self.detail = None;
            self.detail_loading = false;
            self.detail_loading_session_id = None;
        }
        self.notice = None;
        !matches_selected_detail
    }

    fn close_detail_mode(&mut self) {
        self.detail_mode = false;
        self.detail_pane = DetailPane::Describe;
        self.detail_loading = false;
        self.detail_loading_session_id = None;
        self.detail_scroll = 0;
        self.follow_stick_to_bottom = false;
    }

    fn detail_scroll_up(&mut self, step: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(step);
        if self.detail_pane == DetailPane::Follow {
            self.follow_stick_to_bottom = false;
        }
    }

    fn detail_scroll_down(&mut self, step: u16, max_scroll: u16) {
        self.detail_scroll = self.detail_scroll.saturating_add(step).min(max_scroll);
        if self.detail_pane == DetailPane::Follow {
            self.follow_stick_to_bottom = self.detail_scroll >= max_scroll;
        }
    }

    fn detail_scroll_top(&mut self) {
        self.detail_scroll = 0;
        if self.detail_pane == DetailPane::Follow {
            self.follow_stick_to_bottom = false;
        }
    }

    fn detail_scroll_bottom(&mut self, max_scroll: u16) {
        self.detail_scroll = max_scroll;
        if self.detail_pane == DetailPane::Follow {
            self.follow_stick_to_bottom = true;
        }
    }

    fn show_host_column(&self) -> bool {
        self.visible_has_multiple_hosts
    }

    fn visible_host_label(&self) -> String {
        if let Some(scope) = &self.machine_scope {
            return if scope.eq_ignore_ascii_case("local") {
                "Localhost".to_string()
            } else {
                scope.clone()
            };
        }

        match self.machine_labels_cache.as_slice() {
            [] => "none".to_string(),
            [single] => {
                if single.eq_ignore_ascii_case("local") {
                    "Localhost".to_string()
                } else {
                    single.clone()
                }
            }
            _ => "All".to_string(),
        }
    }

    fn detail_max_scroll(&self, frame_area: Rect) -> u16 {
        if !self.detail_mode {
            return 0;
        }
        let footer_height = self.footer_height();
        let content_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(0),
                Constraint::Length(footer_height),
            ])
            .split(frame_area)[1];
        let Some(detail) = &self.detail else {
            return 0;
        };
        let status_height = if self.detail_pane == DetailPane::Follow {
            follow_status_height(current_detail_summary(self, detail))
        } else {
            0
        };
        let content_height =
            detail_content_height(content_area).saturating_sub(status_height) as usize;
        if content_height == 0 {
            return 0;
        }
        let rendered_lines = match self.detail_pane {
            DetailPane::Describe => {
                wrapped_line_count(&detail_lines(detail), detail_content_width(content_area))
            }
            DetailPane::Follow => wrapped_line_count(
                &follow_lines(detail, detail_content_width(content_area)),
                detail_content_width(content_area),
            ),
        };
        rendered_lines
            .saturating_sub(content_height)
            .min(u16::MAX as usize) as u16
    }

    fn footer_height(&self) -> u16 {
        u16::from(
            self.notice.is_some()
                || self.error.is_some()
                || self.filter_mode
                || self.loading_more
                || (!self.detail_mode && self.hidden_session_count() > 0)
                || self
                    .current_machine_overview()
                    .is_some_and(|machine| !machine.reachable),
        )
    }
}

fn draw(frame: &mut Frame, app: &mut TuiApp) {
    let footer_height = app.footer_height();
    let layout = split_frame_layout(frame.area(), footer_height);

    render_header_canvas(frame, layout[0], app);

    if app.detail_mode {
        render_detail_view(frame, layout[1], app);
    } else if app.filtered_indices.is_empty()
        && app
            .current_machine_overview()
            .is_some_and(|machine| !machine.reachable)
    {
        render_machine_unreachable_view(frame, layout[1], app);
    } else {
        let window = table_visible_window(app, layout[1]);
        let mut render_state = TableState::default().with_selected(window.selected);
        frame.render_stateful_widget(
            render_sessions_table(app, layout[1].width, window),
            layout[1],
            &mut render_state,
        );
    }

    if footer_height > 0 {
        frame.render_widget(render_footer(app), layout[2]);
    }
}

fn split_frame_layout(area: Rect, footer_height: u16) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Min(0),
            Constraint::Length(footer_height),
        ])
        .split(area)
        .to_vec()
}

fn sessions_table_area(app: &TuiApp, frame_area: Rect) -> Option<Rect> {
    if app.detail_mode
        || (app.filtered_indices.is_empty()
            && app
                .current_machine_overview()
                .is_some_and(|machine| !machine.reachable))
    {
        return None;
    }

    Some(split_frame_layout(frame_area, app.footer_height())[1])
}

fn detail_content_height(area: Rect) -> u16 {
    area.height.saturating_sub(2)
}

fn detail_content_width(area: Rect) -> u16 {
    area.width.saturating_sub(4).max(1)
}

fn draw_loading(frame: &mut Frame, progress: &SessionLoadProgress) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(frame.area());

    frame.render_widget(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(header_border_color())),
        frame.area(),
    );

    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Length(1),
        ])
        .split(layout[1]);

    let brand_width = body[0].width.saturating_sub(6).clamp(22, 76);
    let brand_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(brand_width),
            Constraint::Min(0),
        ])
        .split(body[0]);

    frame.render_widget(render_brand_cluster(brand_width), brand_row[1]);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "Loading sessions",
                Style::default()
                    .fg(accent_cyan())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(loading_dots_field(), Style::default().fg(accent_cyan())),
        ]))
        .alignment(Alignment::Center),
        body[2],
    );

    let progress_line = if progress.loaded_sessions > 0 {
        Line::from(vec![
            Span::styled("Scanning ", Style::default().fg(text_muted_color())),
            Span::styled(
                "recent agent sessions",
                Style::default()
                    .fg(accent_gold())
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    } else {
        Line::from(Span::styled(
            "Restoring caches and scanning recent agent sessions",
            Style::default().fg(text_muted_color()),
        ))
    };
    frame.render_widget(
        Paragraph::new(progress_line).alignment(Alignment::Center),
        body[3],
    );
}

fn render_header_canvas(frame: &mut Frame, area: Rect, app: &TuiApp) {
    frame.render_widget(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(header_border_color())),
        area,
    );

    let body = Rect {
        x: area.x,
        y: area.y.saturating_add(1),
        width: area.width,
        height: area.height.saturating_sub(1),
    };

    let action_rows = header_action_rows(app);
    let action_width = keymap_grid_width(&action_rows);
    let action_gap = u16::from(action_width > 0) * 5;
    let center_gap = u16::from(action_width > 0) * 4;
    let meta_lines = header_meta_lines(app);
    let meta_width = header_meta_width(&meta_lines);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(meta_width),
            Constraint::Length(action_gap),
            Constraint::Length(action_width),
            Constraint::Length(center_gap),
            Constraint::Min(22),
        ])
        .split(body);

    frame.render_widget(render_header_meta(meta_lines), columns[0]);
    frame.render_widget(render_action_grid(&action_rows), columns[2]);
    render_header_center(frame, columns[4], app);
}

fn render_brand_cluster(width: u16) -> Paragraph<'static> {
    Paragraph::new(Text::from(agent_cow_brand_cluster_lines(width))).alignment(Alignment::Left)
}

const HEADER_BRAND_MIN_WIDTH: u16 = 18;
const HEADER_BRAND_GAP: u16 = 3;
const HEADER_SUBSCRIPTION_MIN_WIDTH: u16 = 30;
const HEADER_SUBSCRIPTION_PREFERRED_WIDTH: u16 = 48;
const HEADER_SUBSCRIPTION_MAX_WIDTH: u16 = 58;

fn render_header_center(frame: &mut Frame, area: Rect, app: &TuiApp) {
    if app.overview.quotas.is_empty() {
        if area.width >= HEADER_BRAND_MIN_WIDTH {
            frame.render_widget(render_brand_cluster(area.width), area);
        }
        return;
    }

    let quota_min = header_subscription_min_width(&app.overview.quotas);
    let quota_pref = header_subscription_preferred_width(&app.overview.quotas);
    let can_show_brand = area.width >= quota_min + HEADER_BRAND_GAP + HEADER_BRAND_MIN_WIDTH;

    if !can_show_brand {
        frame.render_widget(render_subscription_strip(&app.overview.quotas, area), area);
        return;
    }

    let quota_width = area
        .width
        .saturating_sub(HEADER_BRAND_MIN_WIDTH + HEADER_BRAND_GAP)
        .min(quota_pref)
        .max(quota_min);
    let brand_width = area
        .width
        .saturating_sub(quota_width)
        .saturating_sub(HEADER_BRAND_GAP);
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(quota_width),
            Constraint::Length(HEADER_BRAND_GAP),
            Constraint::Length(brand_width),
        ])
        .split(area);

    frame.render_widget(
        render_subscription_strip(&app.overview.quotas, split[0]),
        split[0],
    );
    if brand_width >= HEADER_BRAND_MIN_WIDTH {
        frame.render_widget(render_brand_cluster(split[2].width), split[2]);
    }
}

fn header_subscription_min_width(quotas: &[ProviderQuota]) -> u16 {
    let longest_title = quotas
        .iter()
        .map(|quota| subscription_title(quota, false).chars().count())
        .max()
        .unwrap_or(0) as u16;

    HEADER_SUBSCRIPTION_MIN_WIDTH.max(longest_title.saturating_add(18))
}

fn header_subscription_preferred_width(quotas: &[ProviderQuota]) -> u16 {
    let longest_title = quotas
        .iter()
        .map(|quota| subscription_title(quota, true).chars().count())
        .max()
        .unwrap_or(0) as u16;

    HEADER_SUBSCRIPTION_PREFERRED_WIDTH
        .max(longest_title.saturating_add(24))
        .min(HEADER_SUBSCRIPTION_MAX_WIDTH)
}

fn render_subscription_strip(quotas: &[ProviderQuota], area: Rect) -> Paragraph<'static> {
    Paragraph::new(Text::from(subscription_strip_lines(
        quotas,
        area.width as usize,
        area.height as usize,
    )))
    .alignment(Alignment::Left)
}

fn subscription_strip_lines(
    quotas: &[ProviderQuota],
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    if width == 0 || height == 0 || quotas.is_empty() {
        return Vec::new();
    }

    let compact = width < 38;
    let bar_segments = if width >= 54 {
        10
    } else if width >= 46 {
        8
    } else if width >= 38 {
        6
    } else {
        0
    };
    let lines_per_quota = 2usize;
    let max_cards = (height / lines_per_quota).max(1);
    let visible_count = quotas.len().min(max_cards);
    let mut lines = Vec::with_capacity(height);

    for quota in quotas.iter().take(visible_count) {
        lines.push(subscription_title_line(quota, width, compact));
        lines.push(subscription_windows_line(
            quota,
            width,
            bar_segments,
            compact,
        ));
    }

    if quotas.len() > visible_count {
        let remaining = quotas.len() - visible_count;
        let mut overflow = Line::from(vec![Span::styled(
            format!("+{remaining} more"),
            Style::default()
                .fg(text_muted_color())
                .add_modifier(Modifier::ITALIC),
        )]);
        pad_line_to_width(&mut overflow, width);
        if lines.len() < height {
            lines.push(overflow);
        } else if let Some(last) = lines.last_mut() {
            *last = overflow;
        }
    }

    while lines.len() < height {
        lines.push(blank_padded_line(width));
    }

    lines.truncate(height);
    lines
}

fn subscription_title_line(quota: &ProviderQuota, width: usize, compact: bool) -> Line<'static> {
    let show_plan = !compact && width >= 34;
    let mut spans = vec![Span::styled(
        subscription_title(quota, show_plan),
        Style::default()
            .fg(accent_green())
            .add_modifier(Modifier::BOLD),
    )];

    if quota.limit_reached {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            "LIMIT",
            Style::default()
                .fg(accent_red())
                .add_modifier(Modifier::BOLD),
        ));
    }

    let mut line = Line::from(spans);
    pad_line_to_width(&mut line, width);
    line
}

fn subscription_windows_line(
    quota: &ProviderQuota,
    width: usize,
    bar_segments: usize,
    compact: bool,
) -> Line<'static> {
    let windows = visible_quota_windows(quota);

    if windows.is_empty() {
        if let Some(summary) = quota.summary.as_deref() {
            let mut line = subscription_summary_line(summary, width);
            pad_line_to_width(&mut line, width);
            return line;
        }
        let label = subscription_empty_state_label(quota, compact);
        let style =
            if label == "subscribed" || label == "subscription active" || label == "plan active" {
                Style::default()
                    .fg(accent_green())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(text_muted_color())
                    .add_modifier(Modifier::ITALIC)
            };
        let mut line = Line::from(vec![Span::styled(label, style)]);
        pad_line_to_width(&mut line, width);
        return line;
    }

    let mut spans = Vec::new();
    let max_windows = if compact { 2 } else { 3 };
    for (index, window) in windows.iter().take(max_windows).enumerate() {
        if index > 0 {
            spans.push(Span::raw("   "));
        }
        spans.extend(subscription_window_spans(window, bar_segments));
    }

    let mut line = Line::from(spans);
    pad_line_to_width(&mut line, width);
    line
}

fn visible_quota_windows(quota: &ProviderQuota) -> Vec<&agent_cow_core::QuotaWindow> {
    quota
        .windows
        .iter()
        .filter(|window| {
            !(matches!(quota.provider, agent_cow_core::ProviderKind::Claude)
                && window.label.eq_ignore_ascii_case("SN"))
        })
        .collect()
}

fn subscription_summary_line(summary: &str, width: usize) -> Line<'static> {
    let mut spans = Vec::new();
    let parts: Vec<_> = summary.split('·').map(str::trim).collect();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            spans.push(Span::styled(
                "  ·  ",
                Style::default().fg(text_muted_color()),
            ));
        }
        if let Some((label, value)) = part.split_once(' ') {
            let lower = label.to_ascii_lowercase();
            let label_style = if lower == "usage" {
                Style::default()
                    .fg(text_muted_color())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(accent_gold())
                    .add_modifier(Modifier::BOLD)
            };
            spans.push(Span::styled(label.to_string(), label_style));
            if !value.is_empty() {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(
                    value.to_string(),
                    Style::default()
                        .fg(accent_cyan())
                        .add_modifier(Modifier::BOLD),
                ));
            }
        } else {
            spans.push(Span::styled(
                part.to_string(),
                Style::default()
                    .fg(accent_cyan())
                    .add_modifier(Modifier::BOLD),
            ));
        }
    }
    let mut line = Line::from(spans);
    pad_line_to_width(&mut line, width);
    line
}

fn subscription_empty_state_label(quota: &ProviderQuota, _compact: bool) -> &'static str {
    if quota.plan.is_some() {
        "active"
    } else {
        "quota unavailable"
    }
}

fn subscription_window_spans(
    window: &agent_cow_core::QuotaWindow,
    bar_segments: usize,
) -> Vec<Span<'static>> {
    let style = quota_remaining_style(window.remaining_percent);
    let mut spans = vec![
        Span::styled(
            format!("{:<2}", truncate_chars(&window.label, 2)),
            Style::default()
                .fg(accent_gold())
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(
            format!("{:>3}%", window.remaining_percent),
            style.add_modifier(Modifier::BOLD),
        ),
    ];

    if bar_segments > 0 {
        spans.push(Span::raw(" "));
        let filled = ((window.remaining_percent as usize * bar_segments) + 99).saturating_div(100);
        for index in 0..bar_segments {
            spans.push(Span::styled(
                if index < filled { "▰" } else { "▱" },
                if index < filled {
                    style
                } else {
                    Style::default().fg(Color::Rgb(74, 81, 96))
                },
            ));
        }
    }

    spans
}

fn subscription_title(quota: &ProviderQuota, show_plan: bool) -> String {
    let provider = title_case(&quota.provider.to_string());
    if show_plan {
        quota
            .plan
            .as_deref()
            .map(short_plan_name)
            .filter(|plan| !plan.is_empty())
            .map(|plan| format!("{provider} {plan}"))
            .unwrap_or(provider)
    } else {
        provider
    }
}

fn short_plan_name(plan: &str) -> String {
    let compact = plan.trim().replace('-', " ");
    compact
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .map(title_case)
        .collect::<Vec<_>>()
        .join(" ")
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

fn quota_remaining_style(remaining_percent: u8) -> Style {
    if remaining_percent >= 70 {
        Style::default().fg(accent_green())
    } else if remaining_percent >= 35 {
        Style::default().fg(accent_gold())
    } else {
        Style::default().fg(accent_red())
    }
}

fn pad_line_to_width(line: &mut Line<'static>, width: usize) {
    let padding = width.saturating_sub(line.width());
    if padding > 0 {
        line.spans.push(Span::raw(" ".repeat(padding)));
    }
}

fn blank_padded_line(width: usize) -> Line<'static> {
    Line::from(Span::raw(" ".repeat(width)))
}

fn loading_dots() -> &'static str {
    match (Utc::now().timestamp_millis() / 250).rem_euclid(4) {
        0 => "",
        1 => ".",
        2 => "..",
        _ => "...",
    }
}

fn loading_dots_field() -> String {
    format!("{:<3}", loading_dots())
}

fn header_meta_lines(app: &TuiApp) -> Vec<Line<'static>> {
    let visible_sessions = app.filtered_indices.len();
    let view_scoped_sessions = app.scoped_view_session_count();
    let sessions_value = if !app.filter_input.is_empty() {
        format!("{visible_sessions}/{view_scoped_sessions}")
    } else {
        view_scoped_sessions.to_string()
    };
    let mut lines = vec![
        header_meta_line_with_limit("Machine", app.visible_host_label(), 15),
        header_meta_line("Sessions", sessions_value),
        header_meta_line("View", app.view_mode.label().to_string()),
    ];
    if let Some(scanning_label) = app.active_loading_label() {
        lines.push(header_meta_line("Scanning", scanning_label));
    }
    lines.extend([
        header_meta_line("Spend", format_usd_short(app.visible_total_cost_usd())),
        header_meta_line("Tokens", format_tokens_short(app.visible_total_tokens())),
    ]);
    lines
}

fn render_header_meta(lines: Vec<Line<'static>>) -> Paragraph<'static> {
    Paragraph::new(Text::from(lines))
}

fn header_meta_width(lines: &[Line<'static>]) -> u16 {
    let max_width = lines.iter().map(Line::width).max().unwrap_or(18);
    (max_width as u16).saturating_add(1).clamp(18, 28)
}

type HeaderActionRows = Vec<(Option<KeymapItem>, Option<KeymapItem>)>;

fn header_action_rows(app: &TuiApp) -> HeaderActionRows {
    if app.detail_mode {
        match app.detail_pane {
            DetailPane::Describe => vec![
                (
                    Some(action_item("enter/esc/q", "Back", true)),
                    Some(action_item("f", "Latest Conversation", true)),
                ),
                (
                    Some(action_item("j/k", "Scroll", true)),
                    Some(action_item("o", "Open", true)),
                ),
                (Some(action_item("r", "Refresh", true)), None),
            ],
            DetailPane::Follow => vec![
                (
                    Some(action_item("enter/esc/q", "Back", true)),
                    Some(action_item("d", "Describe", true)),
                ),
                (
                    Some(action_item("j/k", "Scroll", true)),
                    Some(action_item("o", "Open", true)),
                ),
                (Some(action_item("r", "Refresh", true)), None),
            ],
        }
    } else if app.filter_input.is_empty() {
        vec![
            (
                Some(action_item("enter", "Details", true)),
                Some(action_item("f", "Latest Conversation", true)),
            ),
            (
                Some(action_item("j/k", "Move", true)),
                Some(action_item("o", "Open", true)),
            ),
            (
                Some(action_item("/", "Filter", true)),
                Some(action_item("r", "Refresh", true)),
            ),
            (
                Some(action_item("v", "View", true)),
                Some(action_item("m", "Machine", true)),
            ),
            (Some(action_item("q", "Quit", true)), None),
        ]
    } else {
        vec![
            (
                Some(action_item("enter", "Details", true)),
                Some(action_item("f", "Latest Conversation", true)),
            ),
            (
                Some(action_item("j/k", "Move", true)),
                Some(action_item("o", "Open", true)),
            ),
            (
                Some(action_item("/", "Filter", true)),
                Some(action_item("r", "Refresh", true)),
            ),
            (
                Some(action_item("v", "View", true)),
                Some(action_item("m", "Machine", true)),
            ),
            (
                Some(action_item("esc", "Clear", true)),
                Some(action_item("q", "Quit", true)),
            ),
        ]
    }
}

fn render_action_grid(rows: &HeaderActionRows) -> Paragraph<'static> {
    let left_metrics = keymap_column_metrics(rows.iter().filter_map(|(left, _)| left.as_ref()));
    let right_metrics = keymap_column_metrics(rows.iter().filter_map(|(_, right)| right.as_ref()));
    let gap = if left_metrics.cell_width > 0 && right_metrics.cell_width > 0 {
        6
    } else {
        0
    };

    Paragraph::new(Text::from(
        rows.iter()
            .map(|(left, right)| {
                keymap_grid_row(
                    left.as_ref(),
                    right.as_ref(),
                    &left_metrics,
                    &right_metrics,
                    gap,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn keymap_grid_row(
    left: Option<&KeymapItem>,
    right: Option<&KeymapItem>,
    left_metrics: &KeymapColumnMetrics,
    right_metrics: &KeymapColumnMetrics,
    gap: usize,
) -> Line<'static> {
    let mut spans = keymap_cell_spans(left, left_metrics);
    if right_metrics.cell_width > 0 {
        spans.push(Span::raw(" ".repeat(gap)));
        spans.extend(keymap_cell_spans(right, right_metrics));
    }
    Line::from(spans)
}

fn keymap_cell_spans(
    item: Option<&KeymapItem>,
    metrics: &KeymapColumnMetrics,
) -> Vec<Span<'static>> {
    match item {
        Some(item) => {
            let key = item.display_key();
            let key_padding = metrics
                .key_width
                .saturating_sub(key.chars().count())
                .saturating_add(2);
            let label_width = metrics.cell_width.saturating_sub(metrics.key_width + 2);
            vec![
                Span::styled(key, item.key_style),
                Span::raw(" ".repeat(key_padding)),
                Span::styled(
                    format!("{:<width$}", item.label, width = label_width),
                    item.label_style,
                ),
            ]
        }
        None => vec![Span::raw(" ".repeat(metrics.cell_width))],
    }
}

fn agent_cow_brand_cluster_lines(width: u16) -> Vec<Line<'static>> {
    let now = Utc::now();
    let frame = animated_cow_frame(now);
    let ascii_lines = animated_cow_lines(frame.eyes, frame.mouth, frame.tail);
    brand_cluster_lines(width as usize, &ascii_lines, now)
}

fn brand_cluster_lines(
    width: usize,
    cow_lines: &[String],
    now: DateTime<Utc>,
) -> Vec<Line<'static>> {
    let cow_width = cow_lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);

    if let Some(bubble) = header_bubble_layout(width, cow_width, now) {
        return compose_brand_cluster(width, cow_lines, &bubble);
    }

    let left_pad = width_for_alignment(width, cow_width);
    cow_lines
        .iter()
        .map(|line| Line::from(vec![Span::raw(" ".repeat(left_pad)), cow_span(line)]))
        .collect()
}

fn width_for_alignment(area_width: usize, block_width: usize) -> usize {
    area_width.saturating_sub(block_width + 2)
}

struct BubbleLayout {
    line_one: String,
    line_two: String,
    inner_width: usize,
}

const HEADER_BUBBLE_INNER_WIDTH: usize = 34;
const HEADER_BUBBLE_MIN_INNER_WIDTH: usize = 24;
const HEADER_BUBBLE_ROTATE_MS: i64 = 20_000;
const HEADER_BUBBLE_TALK_MS: i64 = 2_400;

fn header_bubble_layout(
    total_width: usize,
    cow_width: usize,
    now: DateTime<Utc>,
) -> Option<BubbleLayout> {
    let connector_width = 3usize;
    let border_width = 4usize;
    let max_inner_width = total_width
        .saturating_sub(cow_width)
        .saturating_sub(connector_width)
        .saturating_sub(border_width);

    if max_inner_width < HEADER_BUBBLE_MIN_INNER_WIDTH {
        return None;
    }

    let (raw_one, raw_two) = header_bubble_message(now);
    let inner_width = HEADER_BUBBLE_INNER_WIDTH.min(max_inner_width);

    Some(BubbleLayout {
        line_one: truncate_chars(raw_one, inner_width),
        line_two: truncate_chars(raw_two, inner_width),
        inner_width,
    })
}

fn compose_brand_cluster(
    width: usize,
    cow_lines: &[String],
    bubble: &BubbleLayout,
) -> Vec<Line<'static>> {
    let cow_width = cow_lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);
    let top = format!(".{}.", "-".repeat(bubble.inner_width + 2));
    let line_one = format!(
        "| {:<width$} |",
        bubble.line_one,
        width = bubble.inner_width
    );
    let line_two = format!(
        "| {:<width$} |",
        bubble.line_two,
        width = bubble.inner_width
    );
    let bottom = format!("'{}'", "-".repeat(bubble.inner_width + 2));
    let blank = " ".repeat(bottom.chars().count());
    let gap_plain = "   ";
    let gap_tail_one = " \\ ";
    let gap_tail_two = "   ";
    let block_width = bottom.chars().count() + gap_plain.chars().count() + cow_width;
    let left_pad = width_for_alignment(width, block_width);

    vec![
        brand_cluster_line(width, left_pad, &top, gap_plain, &cow_lines[0], true),
        brand_cluster_line(width, left_pad, &line_one, gap_plain, &cow_lines[1], false),
        brand_cluster_line(width, left_pad, &line_two, gap_plain, &cow_lines[2], false),
        brand_cluster_line(width, left_pad, &bottom, gap_tail_one, &cow_lines[3], false),
        brand_cluster_line(width, left_pad, &blank, gap_tail_two, &cow_lines[4], false),
    ]
}

fn brand_cluster_line(
    total_width: usize,
    left_pad: usize,
    bubble: &str,
    connector: &str,
    cow: &str,
    top_border: bool,
) -> Line<'static> {
    let bubble_style = if top_border {
        Style::default()
            .fg(accent_cyan())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Rgb(120, 229, 255))
    };

    let used_width =
        left_pad + bubble.chars().count() + connector.chars().count() + cow.chars().count();
    let trailing = total_width.saturating_sub(used_width);

    Line::from(vec![
        Span::raw(" ".repeat(left_pad)),
        Span::styled(bubble.to_string(), bubble_style),
        Span::styled(
            connector.to_string(),
            Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
        ),
        cow_span(cow),
        Span::raw(" ".repeat(trailing)),
    ])
}

fn cow_span(value: &str) -> Span<'static> {
    Span::styled(
        value.to_string(),
        Style::default()
            .fg(accent_gold())
            .add_modifier(Modifier::BOLD),
    )
}

fn header_bubble_message(now: DateTime<Utc>) -> (&'static str, &'static str) {
    const MESSAGES: &[(&str, &str)] = &[
        ("Follow the hottest threads", "Catch who needs you next"),
        ("Watch the live agent herd", "See who is waiting now"),
        ("Keep context pressure in view", "Spot compaction early"),
        (
            "Track what the model is doing",
            "Open the thread that matters",
        ),
    ];

    let index = ((now.timestamp_millis() / HEADER_BUBBLE_ROTATE_MS)
        .rem_euclid(MESSAGES.len() as i64)) as usize;
    MESSAGES[index]
}

struct CowFrame {
    eyes: &'static str,
    mouth: &'static str,
    tail: &'static str,
}

fn animated_cow_frame(now: DateTime<Utc>) -> CowFrame {
    let blink = ((now.timestamp_millis() / 350).rem_euclid(6)) as usize;
    let cycle_ms = now.timestamp_millis().rem_euclid(HEADER_BUBBLE_ROTATE_MS);
    let mouth = if cycle_ms < HEADER_BUBBLE_TALK_MS {
        match ((cycle_ms / 240).rem_euclid(4)) as usize {
            0 => "__",
            1 => "~~",
            2 => "--",
            _ => "oo",
        }
    } else {
        "__"
    };
    let tail = match blink {
        1 | 2 => "/",
        4 | 5 => "\\",
        _ => "|",
    };
    let eyes = if matches!(blink, 2 | 5) { "--" } else { "oo" };

    CowFrame { eyes, mouth, tail }
}

fn animated_cow_lines(eyes: &str, mouth: &str, tail: &str) -> Vec<String> {
    vec![
        "^__^".to_string(),
        format!("({eyes})\\_______"),
        format!("({mouth})\\       )\\/\\"),
        format!("    ||----w {tail}"),
        "    ||     ||".to_string(),
    ]
}

fn aligned_ascii_block(lines: Vec<String>) -> Vec<String> {
    let width = lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);

    lines
        .into_iter()
        .map(|line| format!("{line:<width$}", width = width))
        .collect()
}

fn keymap_grid_width(rows: &HeaderActionRows) -> u16 {
    let left_metrics = keymap_column_metrics(rows.iter().filter_map(|(left, _)| left.as_ref()));
    let right_metrics = keymap_column_metrics(rows.iter().filter_map(|(_, right)| right.as_ref()));
    let gap = u16::from(left_metrics.cell_width > 0 && right_metrics.cell_width > 0) * 6;
    (left_metrics.cell_width as u16)
        .saturating_add(gap)
        .saturating_add(right_metrics.cell_width as u16)
}

#[derive(Default)]
struct KeymapColumnMetrics {
    key_width: usize,
    cell_width: usize,
}

fn keymap_column_metrics<'a>(items: impl Iterator<Item = &'a KeymapItem>) -> KeymapColumnMetrics {
    let mut key_width = 0;
    let mut label_width = 0;
    let mut has_items = false;

    for item in items {
        has_items = true;
        key_width = key_width.max(item.display_key().chars().count());
        label_width = label_width.max(item.label.chars().count());
    }

    if !has_items {
        return KeymapColumnMetrics::default();
    }

    KeymapColumnMetrics {
        key_width,
        cell_width: key_width + 2 + label_width,
    }
}

fn render_sessions_table(
    app: &TuiApp,
    table_width: u16,
    window: VisibleRowWindow,
) -> Table<'static> {
    let title = format!("Sessions ({})", app.filtered_indices.len());

    let show_host = app.show_host_column();
    let widths = session_table_widths(table_width, show_host);

    let mut header_cells = vec![
        Cell::from("").style(Style::default().fg(text_muted_color())),
        Cell::from(session_activity_header_label(widths.activity))
            .style(Style::default().fg(accent_green())),
        Cell::from("LAST").style(Style::default().fg(Color::Rgb(201, 210, 220))),
        Cell::from("DUR").style(Style::default().fg(accent_gold())),
        Cell::from("NAME").style(Style::default().fg(text_primary_color())),
        Cell::from("PROJECT").style(Style::default().fg(accent_cyan())),
    ];
    let mut constraints = vec![
        Constraint::Length(widths.state),
        Constraint::Length(widths.activity),
        Constraint::Length(widths.age),
        Constraint::Length(widths.duration),
        Constraint::Length(widths.name),
        Constraint::Length(widths.project),
    ];

    if show_host {
        header_cells.push(Cell::from("HOST").style(Style::default().fg(accent_cyan())));
        constraints.push(Constraint::Length(widths.host.unwrap_or(8)));
    }

    header_cells.push(Cell::from("MODEL").style(Style::default().fg(accent_blue())));
    constraints.push(Constraint::Length(widths.model));

    header_cells.push(Cell::from("COST").style(Style::default().fg(accent_green())));
    constraints.push(Constraint::Length(widths.cost));

    header_cells.push(Cell::from("$/1H").style(Style::default().fg(accent_gold())));
    constraints.push(Constraint::Length(widths.cost_hour));

    header_cells.push(Cell::from("$/1D").style(Style::default().fg(accent_red())));
    constraints.push(Constraint::Length(widths.cost_day));

    header_cells.push(Cell::from("CTX").style(Style::default().fg(accent_magenta())));
    constraints.push(Constraint::Length(widths.context));

    header_cells.push(Cell::from("TOKENS").style(Style::default().fg(accent_gold())));
    constraints.push(Constraint::Length(widths.tokens));

    let header = Row::new(header_cells).style(
        Style::default()
            .bg(Color::Rgb(52, 52, 58))
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row<'static>> = app.filtered_indices[window.start..window.end]
        .iter()
        .filter_map(|index| app.sessions.get(*index))
        .map(|session| {
            let mut cells = vec![
                Cell::from(session_state_symbol(session)).style(session_state_style(session)),
                Cell::from(session_activity_label_for_width(
                    &session.activity_state,
                    widths.activity,
                ))
                .style(session_activity_style(&session.activity_state)),
                Cell::from(relative_age(session.updated_at))
                    .style(relative_age_style(session.updated_at)),
                Cell::from(session_duration(session)).style(duration_style(session)),
                Cell::from(truncate_chars(
                    &session.title,
                    widths.name.saturating_sub(1) as usize,
                ))
                .style(Style::default().fg(text_primary_color())),
                Cell::from(truncate_chars(
                    &project_label(session),
                    widths.project.saturating_sub(1) as usize,
                ))
                .style(Style::default().fg(accent_cyan())),
            ];

            if show_host {
                cells.push(
                    Cell::from(truncate_chars(
                        &session.machine_label,
                        widths.host.unwrap_or(8).saturating_sub(1) as usize,
                    ))
                    .style(Style::default().fg(accent_cyan())),
                );
            }

            cells.push(
                Cell::from(truncate_chars(
                    session.model.as_deref().unwrap_or("n/a"),
                    widths.model.saturating_sub(1) as usize,
                ))
                .style(model_style(session.model.as_deref().unwrap_or("n/a"))),
            );

            cells.push(
                Cell::from(
                    session
                        .cost
                        .as_ref()
                        .map(|cost| format_usd_short(cost.total_usd))
                        .unwrap_or_else(|| "--".to_string()),
                )
                .style(cost_style(session.cost.as_ref().map(|cost| cost.total_usd))),
            );

            cells.push(
                Cell::from(
                    session
                        .cost
                        .as_ref()
                        .filter(|cost| cost.hour_usd > 0.0)
                        .map(|cost| format_usd_short(cost.hour_usd))
                        .unwrap_or_else(|| "--".to_string()),
                )
                .style(cost_style(session.cost.as_ref().map(|cost| cost.hour_usd))),
            );

            cells.push(
                Cell::from(
                    session
                        .cost
                        .as_ref()
                        .filter(|cost| cost.day_usd > 0.0)
                        .map(|cost| format_usd_short(cost.day_usd))
                        .unwrap_or_else(|| "--".to_string()),
                )
                .style(cost_style(session.cost.as_ref().map(|cost| cost.day_usd))),
            );

            cells.push(
                Cell::from(
                    session
                        .context_window
                        .as_ref()
                        .map(|context| format!("{}%", context.used_percent))
                        .unwrap_or_else(|| "--".to_string()),
                )
                .style(context_style(session.context_window.as_ref())),
            );

            cells.push(
                Cell::from(format_tokens_short(session.tokens.total_tokens))
                    .style(token_style(session.tokens.total_tokens)),
            );

            Row::new(cells)
        })
        .collect();

    Table::new(rows, constraints)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(panel_border_color()))
                .title(Line::from(Span::styled(
                    title,
                    Style::default()
                        .fg(accent_cyan())
                        .add_modifier(Modifier::BOLD),
                )))
                .title_alignment(Alignment::Center),
        )
        .column_spacing(1)
        .row_highlight_style(Style::default().bg(Color::Rgb(68, 68, 72)))
        .highlight_symbol("")
}

#[derive(Clone, Copy)]
struct VisibleRowWindow {
    start: usize,
    end: usize,
    selected: Option<usize>,
}

fn table_visible_window(app: &mut TuiApp, area: Rect) -> VisibleRowWindow {
    let total = app.filtered_indices.len();
    if total == 0 {
        *app.table_state.offset_mut() = 0;
        return VisibleRowWindow {
            start: 0,
            end: 0,
            selected: None,
        };
    }

    let capacity = area.height.saturating_sub(3).max(1) as usize;
    let selected = app.table_state.selected().unwrap_or(0).min(total - 1);
    let mut start = app.table_state.offset().min(total.saturating_sub(1));

    if selected < start {
        start = selected;
    } else if selected >= start + capacity {
        start = selected + 1 - capacity;
    }

    start = start.min(total.saturating_sub(capacity));
    *app.table_state.offset_mut() = start;

    let end = (start + capacity).min(total);
    VisibleRowWindow {
        start,
        end,
        selected: Some(selected.saturating_sub(start)),
    }
}

fn should_refresh_visible_summary(summary: &SessionSummary, now: DateTime<Utc>) -> bool {
    now - summary.updated_at <= chrono::Duration::minutes(VISIBLE_STATE_REFRESH_WINDOW_MINUTES)
        && !matches!(
            summary.status.kind,
            SessionStatusKind::Completed | SessionStatusKind::Failed | SessionStatusKind::Stale
        )
}

#[derive(Clone, Copy)]
struct SessionTableWidths {
    state: u16,
    activity: u16,
    duration: u16,
    cost: u16,
    cost_hour: u16,
    cost_day: u16,
    context: u16,
    name: u16,
    project: u16,
    host: Option<u16>,
    model: u16,
    tokens: u16,
    age: u16,
}

fn session_table_widths(table_width: u16, show_host: bool) -> SessionTableWidths {
    #[derive(Clone, Copy)]
    enum ColumnId {
        State,
        Activity,
        Cost,
        CostHour,
        CostDay,
        Context,
        Name,
        Project,
        Host,
        Model,
        Tokens,
        Age,
        Duration,
    }

    let mut columns = vec![
        (ColumnId::State, 1_u16, 1_u16),
        (ColumnId::Activity, 5_u16, 9_u16),
        (ColumnId::Age, 4, 5),
        (ColumnId::Duration, 4, 5),
        (ColumnId::Cost, 5, 5),
        (ColumnId::CostHour, 5, 5),
        (ColumnId::CostDay, 5, 5),
        (ColumnId::Context, 4, 5),
        (ColumnId::Name, 12_u16, 26_u16),
        (ColumnId::Project, 7_u16, 10_u16),
    ];
    if show_host {
        columns.push((ColumnId::Host, 8, 10));
    }
    columns.extend([(ColumnId::Model, 7, 15), (ColumnId::Tokens, 6, 10)]);

    let inner_width = table_width.saturating_sub(2);
    let spacing = columns.len().saturating_sub(1) as u16;
    let available = inner_width.saturating_sub(spacing);
    let min_total: u16 = columns.iter().map(|(_, min, _)| *min).sum();
    let extra = available.saturating_sub(min_total);
    let weight_total: u16 = columns.iter().map(|(_, _, weight)| *weight).sum();

    let mut resolved: Vec<(ColumnId, u16)> = columns
        .iter()
        .map(|(id, min, weight)| (*id, *min + (extra * *weight / weight_total)))
        .collect();

    let used: u16 = resolved.iter().map(|(_, width)| *width).sum();
    let mut remainder = available.saturating_sub(used);
    let mut index = 0usize;
    while remainder > 0 && !resolved.is_empty() {
        resolved[index].1 += 1;
        remainder -= 1;
        index = (index + 1) % resolved.len();
    }

    let mut widths = SessionTableWidths {
        state: 2,
        activity: 5,
        duration: 4,
        cost: 5,
        cost_hour: 5,
        cost_day: 5,
        context: 4,
        name: 18,
        project: 8,
        host: None,
        model: 7,
        tokens: 6,
        age: 4,
    };

    for (id, width) in resolved {
        match id {
            ColumnId::State => widths.state = width,
            ColumnId::Activity => widths.activity = width,
            ColumnId::Duration => widths.duration = width,
            ColumnId::Cost => widths.cost = width,
            ColumnId::CostHour => widths.cost_hour = width,
            ColumnId::CostDay => widths.cost_day = width,
            ColumnId::Context => widths.context = width,
            ColumnId::Name => widths.name = width,
            ColumnId::Project => widths.project = width,
            ColumnId::Host => widths.host = Some(width),
            ColumnId::Model => widths.model = width,
            ColumnId::Tokens => widths.tokens = width,
            ColumnId::Age => widths.age = width,
        }
    }

    widths
}

fn render_detail_view(frame: &mut Frame, area: Rect, app: &mut TuiApp) {
    if app.detail_loading && app.detail.is_none() {
        render_detail_loading(frame, area, app.detail_pane);
        return;
    }

    if app.detail.is_none()
        && app
            .current_machine_overview()
            .is_some_and(|machine| !machine.reachable)
    {
        render_machine_unreachable_view(frame, area, app);
        return;
    }

    let Some(detail) = &app.detail else {
        frame.render_widget(
            Paragraph::new("No session selected.").block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(panel_border_color()))
                    .title("detail"),
            ),
            area,
        );
        return;
    };
    let summary = current_detail_summary(app, detail);

    let title = detail_pane_title(app.detail_pane);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(panel_border_color()))
        .title(if app.detail_pane == DetailPane::Follow {
            Line::from(vec![Span::styled(
                title,
                Style::default()
                    .fg(accent_cyan())
                    .add_modifier(Modifier::BOLD),
            )])
        } else {
            Line::from(vec![
                Span::styled(
                    title,
                    Style::default()
                        .fg(accent_cyan())
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" • ", Style::default().fg(text_muted_color())),
                Span::styled(
                    short_status_label(&summary.status.kind),
                    status_style(&summary.status.kind).add_modifier(Modifier::BOLD),
                ),
            ])
        });
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let padded_inner = Rect {
        x: inner.x.saturating_add(1),
        y: inner.y,
        width: inner.width.saturating_sub(2),
        height: inner.height,
    };

    match app.detail_pane {
        DetailPane::Describe => {
            let lines = detail_lines(detail);
            let max_scroll = wrapped_line_count(&lines, padded_inner.width.max(1))
                .saturating_sub(padded_inner.height.max(1) as usize)
                .min(u16::MAX as usize) as u16;
            app.detail_scroll = app.detail_scroll.min(max_scroll);
            frame.render_widget(
                Paragraph::new(Text::from(lines))
                    .scroll((app.detail_scroll, 0))
                    .wrap(Wrap { trim: true }),
                padded_inner,
            );
        }
        DetailPane::Follow => {
            let status_line = follow_status_line(summary);
            let status_height = u16::from(status_line.is_some());
            let sections = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(status_height)])
                .split(padded_inner);
            let content_width = sections[0].width.max(1);
            let lines = follow_lines(detail, content_width);
            let max_scroll = wrapped_line_count(&lines, content_width)
                .saturating_sub(sections[0].height.max(1) as usize)
                .min(u16::MAX as usize) as u16;
            if app.follow_stick_to_bottom {
                app.detail_scroll = max_scroll;
            } else {
                app.detail_scroll = app.detail_scroll.min(max_scroll);
            }
            frame.render_widget(
                Paragraph::new(Text::from(lines))
                    .scroll((app.detail_scroll, 0))
                    .wrap(Wrap { trim: true }),
                sections[0],
            );
            if let Some(status_line) = status_line {
                frame.render_widget(Paragraph::new(status_line), sections[1]);
            }
        }
    }
}

fn current_detail_summary<'a>(_app: &'a TuiApp, detail: &'a SessionDetail) -> &'a SessionSummary {
    &detail.summary
}

fn render_detail_loading(frame: &mut Frame, area: Rect, pane: DetailPane) {
    let title = detail_pane_title(pane);
    let loading_label = match pane {
        DetailPane::Describe => "Loading details",
        DetailPane::Follow => "Loading latest conversation",
    };
    let loading_hint = match pane {
        DetailPane::Describe => "Fetching session detail and recent activity",
        DetailPane::Follow => "Following the latest user and assistant exchange",
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(panel_border_color()))
        .title(Line::from(vec![
            Span::styled(
                title,
                Style::default()
                    .fg(accent_cyan())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" • ", Style::default().fg(text_muted_color())),
            Span::styled("loading", Style::default().fg(accent_gold())),
        ]));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(5),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner);

    let brand_width = body[1].width.min(18);
    let brand_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(brand_width),
            Constraint::Min(0),
        ])
        .split(body[1]);

    frame.render_widget(render_brand_cluster(brand_width), brand_row[1]);
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            format!("{loading_label}{}", loading_dots_field()),
            Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
        )]))
        .alignment(Alignment::Center),
        body[2],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            loading_hint,
            Style::default().fg(text_muted_color()),
        )]))
        .alignment(Alignment::Center),
        body[3],
    );
}

fn render_machine_unreachable_view(frame: &mut Frame, area: Rect, app: &TuiApp) {
    let title = if app.detail_mode {
        detail_pane_title(app.detail_pane).to_string()
    } else {
        format!("Sessions ({})", app.filtered_indices.len())
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(panel_border_color()))
        .title(Line::from(Span::styled(
            title,
            Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
        )));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let machine_label = app.visible_host_label();
    let error = app
        .current_machine_overview()
        .and_then(|machine| machine.error.as_deref())
        .unwrap_or("Machine temporarily unreachable");
    let cow_lines = aligned_ascii_block(animated_cow_lines("oo", "__", "|"));
    let lines = vec![
        Line::from(Span::styled(
            truncate_chars(
                &format!("{machine_label} is temporarily unreachable"),
                inner.width.saturating_sub(2) as usize,
            ),
            Style::default()
                .fg(accent_red())
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            truncate_chars(
                "Agent Cow will keep retrying in the background.",
                inner.width.saturating_sub(2) as usize,
            ),
            Style::default().fg(text_primary_color()),
        )),
        Line::from(""),
        Line::from(Span::styled(
            truncate_chars(error, inner.width.saturating_sub(2) as usize),
            Style::default().fg(text_muted_color()),
        )),
        Line::from(""),
        Line::from(Span::styled(
            cow_lines[0].clone(),
            Style::default().fg(accent_gold()),
        )),
        Line::from(Span::styled(
            cow_lines[1].clone(),
            Style::default().fg(accent_gold()),
        )),
        Line::from(Span::styled(
            cow_lines[2].clone(),
            Style::default().fg(accent_gold()),
        )),
        Line::from(Span::styled(
            cow_lines[3].clone(),
            Style::default().fg(accent_gold()),
        )),
        Line::from(Span::styled(
            cow_lines[4].clone(),
            Style::default().fg(accent_gold()),
        )),
    ];

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn detail_pane_title(pane: DetailPane) -> &'static str {
    match pane {
        DetailPane::Describe => "Describe",
        DetailPane::Follow => "Latest Conversation",
    }
}

fn footer_line(app: &TuiApp) -> Line<'static> {
    if let Some(notice) = &app.notice {
        Line::from(Span::styled(
            notice.message.clone(),
            Style::default().fg(if notice.is_error {
                accent_red()
            } else {
                accent_green()
            }),
        ))
    } else if let Some(error) = &app.error {
        Line::from(Span::styled(
            error.clone(),
            Style::default().fg(accent_red()),
        ))
    } else if app.filter_mode {
        Line::from(vec![
            Span::styled("Filter: ", Style::default().fg(text_muted_color())),
            Span::styled(
                format!("/{}_", app.filter_input),
                Style::default().fg(accent_gold()),
            ),
        ])
    } else if let Some(scanning_label) = app.active_loading_label() {
        Line::from(vec![
            Span::styled(
                "Scanning sessions",
                Style::default()
                    .fg(accent_cyan())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(loading_dots_field(), Style::default().fg(accent_cyan())),
            Span::styled(
                format!(" {}", scanning_label),
                Style::default().fg(text_muted_color()),
            ),
        ])
    } else if let Some(machine) = app
        .current_machine_overview()
        .filter(|machine| !machine.reachable)
    {
        Line::from(vec![
            Span::styled(
                truncate_chars(&machine.machine_label, 40),
                Style::default()
                    .fg(accent_red())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " is temporarily unreachable",
                Style::default().fg(accent_red()),
            ),
            Span::styled(
                "  •  retrying automatically",
                Style::default().fg(text_muted_color()),
            ),
        ])
    } else if !app.detail_mode && app.hidden_session_count() > 0 {
        Line::from(vec![
            Span::styled(
                format!("{} ", app.hidden_session_count()),
                Style::default()
                    .fg(accent_gold())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                app.view_mode.hidden_hint(),
                Style::default().fg(text_muted_color()),
            ),
        ])
    } else if let Some(summary) = app.selected_summary() {
        Line::from(vec![
            Span::styled("State: ", Style::default().fg(text_muted_color())),
            Span::styled(
                truncate_chars(&summary.status.reason, 120),
                Style::default().fg(text_primary_color()),
            ),
        ])
    } else {
        Line::from("")
    }
}

fn render_footer(app: &TuiApp) -> Paragraph<'static> {
    let line = footer_line(app);
    Paragraph::new(Text::from(vec![line]))
}

fn detail_lines(detail: &SessionDetail) -> Vec<Line<'static>> {
    let summary = &detail.summary;
    let mut lines = vec![
        Line::from(Span::styled(
            truncate_chars(&summary.title, 220),
            Style::default()
                .fg(text_primary_color())
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        section_header("Overview"),
        Line::from(vec![
            info_label("Status"),
            Span::styled(
                short_status_label(&summary.status.kind),
                status_style(&summary.status.kind),
            ),
            subtle_bullet(),
            info_label("Confidence"),
            Span::styled(
                summary.status.confidence.to_string(),
                Style::default().fg(text_muted_color()),
            ),
        ]),
        Line::from(vec![
            info_label("Started"),
            Span::raw(summary.created_at.format("%Y-%m-%d %H:%M").to_string()),
            subtle_bullet(),
            info_label("Updated"),
            Span::styled(
                format!("{} ago", relative_age(summary.updated_at)),
                Style::default().fg(text_primary_color()),
            ),
            subtle_bullet(),
            info_label("Duration"),
            Span::styled(session_duration(summary), duration_style(summary)),
        ]),
        Line::from(vec![
            info_label("Model"),
            Span::styled(
                summary.model.clone().unwrap_or_else(|| "n/a".to_string()),
                model_style(summary.model.as_deref().unwrap_or("n/a")),
            ),
            subtle_bullet(),
            info_label("Provider"),
            Span::raw(summary.provider.to_string()),
            subtle_bullet(),
            info_label("Machine"),
            Span::raw(summary.machine_label.clone()),
        ]),
        Line::from(""),
        section_header("Usage"),
        Line::from(vec![
            info_label("Cost"),
            Span::styled(
                summary
                    .cost
                    .as_ref()
                    .map(|cost| format!("{} total", format_usd_short(cost.total_usd)))
                    .unwrap_or_else(|| "n/a".to_string()),
                Style::default()
                    .fg(accent_green())
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    if let Some(cost) = &summary.cost {
        let mut parts = vec![
            format!("1h {}", format_usd_short(cost.hour_usd)),
            format!("1d {}", format_usd_short(cost.day_usd)),
            format!("input {}", format_usd_short(cost.input_usd)),
        ];
        if cost.cache_creation_input_usd > 0.0 {
            parts.push(format!(
                "cache+ {}",
                format_usd_short(cost.cache_creation_input_usd)
            ));
        }
        parts.push(format!(
            "cached {}",
            format_usd_short(cost.cached_input_usd)
        ));
        parts.push(format!("output {}", format_usd_short(cost.output_usd)));
        lines.push(Line::from(vec![
            indent(),
            Span::styled(parts.join("  •  "), Style::default().fg(text_muted_color())),
        ]));
    }

    lines.push(Line::from(vec![
        info_label("Context"),
        Span::styled(
            summary
                .context_window
                .as_ref()
                .map(format_context_window)
                .unwrap_or_else(|| "n/a".to_string()),
            context_style(summary.context_window.as_ref()),
        ),
    ]));

    if let Some(context) = &summary.context_window {
        lines.push(context_bar_line(context));
    }

    lines.extend([
        Line::from(vec![
            info_label("Tokens"),
            Span::styled(
                token_breakdown(&summary.tokens),
                Style::default().fg(text_primary_color()),
            ),
        ]),
        Line::from(vec![
            info_label("Turns"),
            Span::styled(
                format!(
                    "{} active  •  {} pending tools",
                    detail.active_turns, detail.pending_tool_calls
                ),
                Style::default().fg(text_primary_color()),
            ),
        ]),
        Line::from(""),
        section_header("Workspace"),
        meta_line("Workdir", summary.cwd.clone()),
    ]);

    if let Some(branch) = &summary.git_branch {
        lines.push(meta_line("Branch", branch.clone()));
    }

    lines.push(Line::from(""));
    lines.push(section_header("Execution"));
    lines.push(meta_line("Reason", summary.status.reason.clone()));
    lines.push(meta_line("Tools", render_tool_summary(&detail.tool_stats)));
    lines.push(Line::from(""));
    lines.push(section_header("Recent Activity"));

    let mut recent: Vec<_> = detail.recent_events.iter().rev().take(6).collect();
    recent.reverse();

    if recent.is_empty() {
        lines.push(Line::from("  no recent events"));
    } else {
        for event in recent {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {} ", local_timestamp(event.timestamp)),
                    Style::default().fg(text_muted_color()),
                ),
                Span::styled(
                    format!("{:<10}", activity_label(&event.kind)),
                    activity_style(&event.kind),
                ),
                Span::raw(" "),
                Span::raw(truncate_chars(&event.summary, 120)),
            ]));
        }
    }

    if detail.last_user_message.is_some() || detail.last_assistant_message.is_some() {
        lines.push(Line::from(""));
        lines.push(section_header("Messages"));
    }

    if let Some(message) = &detail.last_user_message {
        lines.push(meta_line("Last user", truncate_chars(message, 140)));
    }

    if let Some(message) = &detail.last_assistant_message {
        lines.push(meta_line("Last reply", truncate_chars(message, 140)));
    }

    lines
}

fn follow_lines(detail: &SessionDetail, width: u16) -> Vec<Line<'static>> {
    let timeline = follow_timeline_events(detail);
    let mut lines = Vec::new();

    if timeline.is_empty() {
        lines.push(Line::from(vec![
            indent(),
            Span::styled(
                "No recent conversation captured.",
                Style::default().fg(text_muted_color()),
            ),
        ]));
        return lines;
    }

    for event in timeline {
        lines.extend(follow_event_block(detail, &event, width));
    }

    lines
}

fn follow_status_height(summary: &SessionSummary) -> u16 {
    u16::from(follow_status_line(summary).is_some())
}

fn follow_status_line(summary: &SessionSummary) -> Option<Line<'static>> {
    let status = follow_status(summary)?;
    let label = if status.animated() {
        format!("{}{}", status.label(), loading_dots())
    } else {
        status.label().to_string()
    };

    Some(Line::from(vec![Span::styled(label, status.style())]))
}

#[derive(Clone, Copy)]
enum FollowStatus {
    Compacting,
    Exploring,
    Thinking,
    Working,
    Waiting,
    Idle,
}

impl FollowStatus {
    fn from_activity_state(state: &SessionActivityState) -> Self {
        match state {
            SessionActivityState::Compacting => Self::Compacting,
            SessionActivityState::Exploring => Self::Exploring,
            SessionActivityState::Thinking => Self::Thinking,
            SessionActivityState::Working => Self::Working,
            SessionActivityState::Waiting => Self::Waiting,
            SessionActivityState::Idle => Self::Idle,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Compacting => "Compacting",
            Self::Exploring => "Exploring",
            Self::Thinking => "Thinking",
            Self::Working => "Working",
            Self::Waiting => "Waiting for you",
            Self::Idle => "Idle",
        }
    }

    fn style(self) -> Style {
        match self {
            Self::Compacting => Style::default()
                .fg(accent_gold())
                .add_modifier(Modifier::BOLD),
            Self::Exploring => Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
            Self::Thinking => Style::default()
                .fg(accent_green())
                .add_modifier(Modifier::BOLD),
            Self::Working => Style::default()
                .fg(accent_blue())
                .add_modifier(Modifier::BOLD),
            Self::Waiting => Style::default()
                .fg(accent_magenta())
                .add_modifier(Modifier::BOLD),
            Self::Idle => Style::default().fg(text_muted_color()),
        }
    }

    fn animated(self) -> bool {
        matches!(
            self,
            Self::Compacting | Self::Exploring | Self::Thinking | Self::Working
        )
    }
}

fn follow_status(summary: &SessionSummary) -> Option<FollowStatus> {
    Some(FollowStatus::from_activity_state(&summary.activity_state))
}

fn follow_timeline_events(detail: &SessionDetail) -> Vec<ActivityEvent> {
    let conversation = latest_conversation_events(detail);
    let conversation_start = conversation.first().map(|event| event.timestamp);
    let mut timeline: Vec<ActivityEvent> = if conversation.is_empty() {
        detail
            .recent_events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    ActivityKind::User | ActivityKind::Assistant | ActivityKind::ToolCall
                )
            })
            .cloned()
            .collect()
    } else {
        let mut events: Vec<ActivityEvent> = conversation.into_iter().cloned().collect();
        events.extend(
            detail
                .recent_events
                .iter()
                .filter(|event| {
                    matches!(event.kind, ActivityKind::ToolCall)
                        && conversation_start.is_none_or(|ts| event.timestamp >= ts)
                })
                .cloned(),
        );
        events
    };

    timeline.sort_by_key(|event| event.timestamp);
    timeline = dedupe_follow_timeline(timeline);

    if timeline.len() > 20 {
        timeline = timeline.split_off(timeline.len() - 20);
    }

    timeline
}

fn latest_conversation_events(detail: &SessionDetail) -> Vec<&ActivityEvent> {
    let deduped: Vec<_> = detail
        .recent_conversation
        .iter()
        .filter(|event| matches!(event.kind, ActivityKind::User | ActivityKind::Assistant))
        .fold(Vec::<&ActivityEvent>::new(), |mut acc, event| {
            let is_duplicate = acc.last().is_some_and(|previous| {
                previous.kind == event.kind && previous.summary == event.summary
            });
            if !is_duplicate {
                acc.push(event);
            }
            acc
        });

    if deduped.is_empty() {
        return Vec::new();
    }

    let start = deduped
        .iter()
        .rposition(|event| matches!(event.kind, ActivityKind::User))
        .unwrap_or_else(|| deduped.len().saturating_sub(12));
    let mut tail = deduped[start..].to_vec();

    if tail.len() > 18 {
        let keep_head = usize::from(matches!(
            tail.first().map(|event| &event.kind),
            Some(ActivityKind::User)
        ));
        let keep_tail = 18usize.saturating_sub(keep_head);
        let mut trimmed = Vec::with_capacity(keep_head + keep_tail);
        if keep_head == 1 {
            trimmed.push(tail[0]);
        }
        let tail_start = tail.len().saturating_sub(keep_tail);
        trimmed.extend_from_slice(&tail[tail_start..]);
        tail = trimmed;
    }

    tail
}

fn dedupe_follow_timeline(events: Vec<ActivityEvent>) -> Vec<ActivityEvent> {
    let mut deduped = Vec::with_capacity(events.len());

    for event in events {
        let is_duplicate = deduped.last().is_some_and(|previous: &ActivityEvent| {
            previous.kind == event.kind && previous.summary == event.summary
        });

        if !is_duplicate {
            deduped.push(event);
        }
    }

    deduped
}

fn follow_event_block(
    detail: &SessionDetail,
    event: &ActivityEvent,
    width: u16,
) -> Vec<Line<'static>> {
    match event.kind {
        ActivityKind::ToolCall => follow_tool_call_block(detail, event, width),
        ActivityKind::ToolResult | ActivityKind::System => follow_text_block(
            detail,
            event,
            width,
            Style::default().fg(text_muted_color()),
        ),
        ActivityKind::User | ActivityKind::Assistant => follow_text_block(
            detail,
            event,
            width,
            Style::default().fg(text_primary_color()),
        ),
    }
}

fn follow_header_line(
    timestamp: DateTime<Utc>,
    speaker: String,
    speaker_style: Style,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            local_timestamp(timestamp),
            Style::default().fg(text_muted_color()),
        ),
        Span::raw("  "),
        Span::styled(speaker, speaker_style.add_modifier(Modifier::BOLD)),
    ])
}

fn push_follow_body(lines: &mut Vec<Line<'static>>, text: &str, width: usize, style: Style) {
    for line in wrap_display_text(text, width) {
        if line.is_empty() {
            lines.push(Line::from(""));
        } else {
            lines.push(Line::from(vec![
                Span::raw("        "),
                Span::styled(line, style),
            ]));
        }
    }
}

fn follow_text_block(
    detail: &SessionDetail,
    event: &ActivityEvent,
    width: u16,
    body_style: Style,
) -> Vec<Line<'static>> {
    let mut lines = vec![follow_header_line(
        event.timestamp,
        follow_sender_name(detail, &event.kind),
        follow_sender_style(detail, &event.kind),
    )];

    push_follow_body(
        &mut lines,
        &event.summary,
        follow_body_width(width),
        body_style,
    );

    lines.push(Line::from(""));
    lines
}

fn follow_tool_call_block(
    detail: &SessionDetail,
    event: &ActivityEvent,
    width: u16,
) -> Vec<Line<'static>> {
    let (tool_name, body_lines) = parse_tool_call_block(&event.summary);
    let mut lines = vec![follow_header_line(
        event.timestamp,
        follow_sender_name(detail, &ActivityKind::Assistant),
        follow_sender_style(detail, &ActivityKind::Assistant),
    )];

    lines.push(Line::from(vec![
        Span::raw("        "),
        Span::styled("used ", Style::default().fg(text_muted_color())),
        Span::styled(
            tool_name,
            Style::default()
                .fg(accent_gold())
                .add_modifier(Modifier::BOLD),
        ),
    ]));

    let detail_style = Style::default().fg(Color::Rgb(176, 208, 233));
    let body_width = follow_body_width(width);
    for body_line in body_lines {
        push_follow_body(&mut lines, &body_line, body_width, detail_style);
    }

    lines.push(Line::from(""));
    lines
}

fn parse_tool_call_block(summary: &str) -> (String, Vec<String>) {
    if let Some(rest) = summary.strip_prefix("exec_command  ") {
        return match rest.rsplit_once("  in ") {
            Some((command, workdir)) => (
                "exec_command".to_string(),
                vec![command.to_string(), format!("in {workdir}")],
            ),
            None => ("exec_command".to_string(), vec![rest.to_string()]),
        };
    }

    if let Some(rest) = summary.strip_prefix("write_stdin  ") {
        return match rest.rsplit_once("  session ") {
            Some((action, session)) => (
                "write_stdin".to_string(),
                vec![action.to_string(), format!("session {session}")],
            ),
            None => ("write_stdin".to_string(), vec![rest.to_string()]),
        };
    }

    ("tool".to_string(), vec![summary.to_string()])
}

fn follow_body_width(width: u16) -> usize {
    width.saturating_sub(10).max(28) as usize
}

fn follow_sender_name(detail: &SessionDetail, kind: &ActivityKind) -> String {
    match kind {
        ActivityKind::User => "You".to_string(),
        ActivityKind::Assistant => detail
            .summary
            .model
            .clone()
            .unwrap_or_else(|| detail.summary.provider.to_string()),
        ActivityKind::ToolCall => "Tool".to_string(),
        ActivityKind::ToolResult => "Result".to_string(),
        ActivityKind::System => "System".to_string(),
    }
}

fn follow_sender_style(detail: &SessionDetail, kind: &ActivityKind) -> Style {
    match kind {
        ActivityKind::User => follow_style(kind),
        ActivityKind::Assistant => detail
            .summary
            .model
            .as_deref()
            .map(model_style)
            .unwrap_or_else(|| model_style(&detail.summary.provider.to_string())),
        _ => follow_style(kind),
    }
}

fn wrap_display_text(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut wrapped = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            if !wrapped
                .last()
                .is_some_and(|previous: &String| previous.is_empty())
            {
                wrapped.push(String::new());
            }
            continue;
        }

        let mut current = String::new();
        for word in line.split_whitespace() {
            let candidate_len = if current.is_empty() {
                word.chars().count()
            } else {
                current.chars().count() + 1 + word.chars().count()
            };

            if !current.is_empty() && candidate_len > width {
                wrapped.push(current);
                current = word.to_string();
            } else if current.is_empty() {
                current = word.to_string();
            } else {
                current.push(' ');
                current.push_str(word);
            }
        }

        if !current.is_empty() {
            wrapped.push(current);
        }
    }

    if wrapped.is_empty() {
        vec![String::new()]
    } else {
        wrapped
    }
}

fn wrapped_line_count(lines: &[Line<'static>], width: u16) -> usize {
    let width = width.max(1) as usize;
    lines
        .iter()
        .map(|line| {
            let line_width = line.width();
            if line_width == 0 {
                1
            } else {
                line_width.div_ceil(width)
            }
        })
        .sum()
}

fn matches_text_filter(session: &SessionSummary, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }

    let haystack = [
        session.title.as_str(),
        session.cwd.as_str(),
        session.machine_label.as_str(),
        session.model.as_deref().unwrap_or_default(),
        &session.provider.to_string(),
        &session.status.kind.to_string(),
    ]
    .join(" ")
    .to_lowercase();

    haystack.contains(filter)
}

fn matches_machine_scope(session: &SessionSummary, scope: Option<&str>) -> bool {
    match scope {
        Some(scope) => session.machine_label == scope,
        None => true,
    }
}

fn matches_view_mode(
    session: &SessionSummary,
    view_mode: SessionViewMode,
    now: DateTime<Utc>,
) -> bool {
    match view_mode {
        SessionViewMode::All => is_meaningful_session(session),
        SessionViewMode::Recent => {
            is_meaningful_session(session)
                && session.updated_at >= now - chrono::Duration::days(RECENT_SESSION_WINDOW_DAYS)
        }
        SessionViewMode::Focus => {
            is_meaningful_session(session)
                && (session.updated_at >= now - chrono::Duration::hours(FOCUS_SESSION_WINDOW_HOURS)
                    || matches!(
                        session.status.kind,
                        SessionStatusKind::Running
                            | SessionStatusKind::ToolBusy
                            | SessionStatusKind::WaitingInput
                    )
                    || !matches!(session.activity_state, SessionActivityState::Idle))
        }
    }
}

fn is_meaningful_session(session: &SessionSummary) -> bool {
    if session.model.is_none() {
        return false;
    }

    match session.provider {
        ProviderKind::Codex => true,
        ProviderKind::Claude => !is_claude_noise_session(session),
        ProviderKind::Opencode => !is_opencode_noise_session(session),
    }
}

fn is_claude_noise_session(session: &SessionSummary) -> bool {
    if session
        .rollout_path
        .as_deref()
        .is_some_and(|path| path.contains("/subagents/"))
    {
        return true;
    }

    let normalized = session.title.trim().to_ascii_lowercase();
    if normalized.starts_with("do not browse, inspect files, or run shell commands.") {
        return true;
    }

    matches!(
        normalized.as_str(),
        "exit" | "/exit" | "/status" | "/status/exit" | "statusline"
    )
}

fn is_opencode_noise_session(session: &SessionSummary) -> bool {
    session.agent_role.as_deref() == Some("subagent")
        || session.title.to_ascii_lowercase().contains("subagent")
}

fn meta_line(label: &str, value: String) -> Line<'static> {
    Line::from(vec![
        info_label(label),
        Span::styled(
            value,
            Style::default()
                .fg(text_primary_color())
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn header_meta_line(label: &str, value: String) -> Line<'static> {
    header_meta_line_with_limit(label, value, 8)
}

fn header_meta_line_with_limit(label: &str, value: String, max_chars: usize) -> Line<'static> {
    meta_line(label, truncate_chars(&value, max_chars))
}

fn section_header(label: &str) -> Line<'static> {
    Line::from(Span::styled(
        label.to_string(),
        Style::default()
            .fg(accent_cyan())
            .add_modifier(Modifier::BOLD),
    ))
}

fn subtle_bullet() -> Span<'static> {
    Span::styled("  •  ", Style::default().fg(text_muted_color()))
}

fn indent() -> Span<'static> {
    Span::raw("          ")
}

fn context_bar_line(context: &agent_cow_core::ContextWindowUsage) -> Line<'static> {
    let filled = ((context.used_percent as usize * 24) / 100).min(24);
    let mut spans = vec![indent()];

    for index in 0..24 {
        let style = if index < filled {
            context_style(Some(context))
        } else {
            Style::default().fg(Color::Rgb(86, 90, 102))
        };
        spans.push(Span::styled("━", style));
    }

    spans.push(Span::styled(
        format!("  {}%", context.used_percent),
        Style::default().fg(text_muted_color()),
    ));

    Line::from(spans)
}

fn info_label(label: &str) -> Span<'static> {
    Span::styled(
        format!("{:<10}", format!("{label}:")),
        Style::default().fg(accent_gold()),
    )
}

#[derive(Clone)]
struct KeymapItem {
    key: String,
    label: String,
    key_style: Style,
    label_style: Style,
}

impl KeymapItem {
    fn display_key(&self) -> String {
        format!("<{}>", self.key)
    }
}

#[derive(Clone, Copy)]
enum ActionTone {
    Primary,
    Secondary,
    Quiet,
}

fn action_tone(key: &str) -> ActionTone {
    match key {
        "enter" | "enter/esc" | "o" => ActionTone::Primary,
        "q" | "esc" => ActionTone::Quiet,
        _ => ActionTone::Secondary,
    }
}

fn action_item(key: &str, label: &str, enabled: bool) -> KeymapItem {
    let tone = action_tone(key);
    let key_style = if enabled {
        match tone {
            ActionTone::Primary => Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
            ActionTone::Secondary => Style::default()
                .fg(accent_blue())
                .add_modifier(Modifier::BOLD),
            ActionTone::Quiet => Style::default().fg(Color::Rgb(140, 160, 185)),
        }
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let label_style = if enabled {
        match tone {
            ActionTone::Primary => Style::default()
                .fg(text_primary_color())
                .add_modifier(Modifier::BOLD),
            ActionTone::Secondary => Style::default().fg(Color::Rgb(198, 206, 218)),
            ActionTone::Quiet => Style::default()
                .fg(text_muted_color())
                .add_modifier(Modifier::DIM),
        }
    } else {
        Style::default().fg(Color::DarkGray)
    };

    KeymapItem {
        key: key.to_string(),
        label: label.to_string(),
        key_style,
        label_style,
    }
}

fn short_status_label(kind: &SessionStatusKind) -> &'static str {
    match kind {
        SessionStatusKind::Running => "running",
        SessionStatusKind::ToolBusy => "busy",
        SessionStatusKind::WaitingInput => "waiting",
        SessionStatusKind::Idle => "idle",
        SessionStatusKind::Stale => "stale",
        SessionStatusKind::Completed => "done",
        SessionStatusKind::Failed => "failed",
        SessionStatusKind::Unknown => "unknown",
    }
}

fn project_label(session: &SessionSummary) -> String {
    std::path::Path::new(&session.cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("n/a")
        .to_string()
}

fn header_border_color() -> Color {
    Color::Rgb(46, 210, 153)
}

fn panel_border_color() -> Color {
    Color::Rgb(110, 102, 186)
}

fn accent_blue() -> Color {
    Color::Rgb(94, 174, 255)
}

fn accent_cyan() -> Color {
    Color::Rgb(111, 224, 240)
}

fn accent_green() -> Color {
    Color::Rgb(121, 226, 156)
}

fn accent_gold() -> Color {
    Color::Rgb(255, 198, 79)
}

fn accent_magenta() -> Color {
    Color::Rgb(220, 135, 255)
}

fn accent_red() -> Color {
    Color::Rgb(255, 107, 129)
}

fn text_primary_color() -> Color {
    Color::Rgb(236, 240, 247)
}

fn text_muted_color() -> Color {
    Color::Rgb(162, 170, 186)
}

fn session_is_live(session: &SessionSummary) -> bool {
    session.run_active
        && matches!(
            session.status.kind,
            SessionStatusKind::Running | SessionStatusKind::ToolBusy
        )
}

fn session_elapsed(session: &SessionSummary) -> chrono::Duration {
    let start = session.created_at;
    let end = if session_is_live(session) {
        Utc::now()
    } else {
        session.updated_at
    };

    if end < start {
        chrono::Duration::zero()
    } else {
        end.signed_duration_since(start)
    }
}

fn relative_age_style(updated_at: DateTime<Utc>) -> Style {
    let age = Utc::now() - updated_at;
    if age.num_minutes() < 5 {
        Style::default()
            .fg(text_primary_color())
            .add_modifier(Modifier::BOLD)
    } else if age.num_hours() < 1 {
        Style::default().fg(accent_green())
    } else if age.num_hours() < 12 {
        Style::default().fg(accent_gold())
    } else {
        Style::default().fg(text_muted_color())
    }
}

fn duration_style(session: &SessionSummary) -> Style {
    let duration = session_elapsed(session);

    if duration.num_hours() < 1 {
        Style::default().fg(accent_green())
    } else if duration.num_hours() < 8 {
        Style::default().fg(accent_cyan())
    } else if duration.num_days() < 1 {
        Style::default().fg(accent_gold())
    } else {
        Style::default().fg(accent_magenta())
    }
}

fn cost_style(total_cost: Option<f64>) -> Style {
    match total_cost.unwrap_or(0.0) {
        value if value >= 25.0 => Style::default()
            .fg(accent_red())
            .add_modifier(Modifier::BOLD),
        value if value >= 5.0 => Style::default().fg(accent_gold()),
        value if value > 0.0 => Style::default().fg(accent_green()),
        _ => Style::default().fg(text_muted_color()),
    }
}

fn context_style(context: Option<&agent_cow_core::ContextWindowUsage>) -> Style {
    let used_percent = context.map(|context| context.used_percent).unwrap_or(0);
    if used_percent >= 85 {
        Style::default()
            .fg(accent_red())
            .add_modifier(Modifier::BOLD)
    } else if used_percent >= 65 {
        Style::default().fg(accent_gold())
    } else if context.is_some() {
        Style::default().fg(accent_magenta())
    } else {
        Style::default().fg(text_muted_color())
    }
}

fn model_style(model: &str) -> Style {
    let model = model.to_ascii_lowercase();
    if model.contains("codex") {
        Style::default().fg(Color::Rgb(255, 166, 92))
    } else if model.contains("gpt-5") {
        Style::default().fg(accent_blue())
    } else if model.contains("claude") {
        Style::default().fg(accent_magenta())
    } else {
        Style::default().fg(Color::Rgb(196, 206, 222))
    }
}

fn token_style(total_tokens: u64) -> Style {
    if total_tokens >= 10_000_000 {
        Style::default()
            .fg(Color::Rgb(255, 122, 122))
            .add_modifier(Modifier::BOLD)
    } else if total_tokens >= 1_000_000 {
        Style::default().fg(accent_gold())
    } else if total_tokens >= 100_000 {
        Style::default().fg(accent_green())
    } else {
        Style::default().fg(text_muted_color())
    }
}

fn activity_label(kind: &ActivityKind) -> &'static str {
    match kind {
        ActivityKind::User => "user",
        ActivityKind::Assistant => "assistant",
        ActivityKind::ToolCall => "tool",
        ActivityKind::ToolResult => "result",
        ActivityKind::System => "system",
    }
}

fn activity_style(kind: &ActivityKind) -> Style {
    match kind {
        ActivityKind::User => Style::default().fg(accent_blue()),
        ActivityKind::Assistant => Style::default().fg(accent_green()),
        ActivityKind::ToolCall => Style::default().fg(accent_gold()),
        ActivityKind::ToolResult => Style::default().fg(accent_magenta()),
        ActivityKind::System => Style::default().fg(text_muted_color()),
    }
}

fn follow_style(kind: &ActivityKind) -> Style {
    match kind {
        ActivityKind::User => Style::default()
            .fg(accent_blue())
            .add_modifier(Modifier::BOLD),
        ActivityKind::Assistant => Style::default()
            .fg(accent_green())
            .add_modifier(Modifier::BOLD),
        ActivityKind::ToolCall => Style::default().fg(accent_gold()),
        ActivityKind::ToolResult => Style::default().fg(accent_magenta()),
        ActivityKind::System => Style::default().fg(text_muted_color()),
    }
}

fn session_activity_header_label(width: u16) -> &'static str {
    match width {
        0..=3 => "ST",
        4..=6 => "ACT",
        _ => "STATE",
    }
}

fn session_activity_label_for_width(state: &SessionActivityState, width: u16) -> &'static str {
    let full = session_activity_label(state);
    if width as usize >= full.len() {
        return full;
    }

    match width {
        0..=1 => match state {
            SessionActivityState::Thinking => "T",
            SessionActivityState::Working => "K",
            SessionActivityState::Exploring => "E",
            SessionActivityState::Compacting => "C",
            SessionActivityState::Waiting => "W",
            SessionActivityState::Idle => "I",
        },
        2..=4 => match state {
            SessionActivityState::Thinking => "Thnk",
            SessionActivityState::Working => "Work",
            SessionActivityState::Exploring => "Expl",
            SessionActivityState::Compacting => "Comp",
            SessionActivityState::Waiting => "Wait",
            SessionActivityState::Idle => "Idle",
        },
        _ => match state {
            SessionActivityState::Thinking => "Think",
            SessionActivityState::Working => "Working",
            SessionActivityState::Exploring => "Explore",
            SessionActivityState::Compacting => "Compact",
            SessionActivityState::Waiting => "Waiting",
            SessionActivityState::Idle => "Idle",
        },
    }
}

fn session_activity_label(state: &SessionActivityState) -> &'static str {
    match state {
        SessionActivityState::Thinking => "Thinking",
        SessionActivityState::Working => "Working",
        SessionActivityState::Exploring => "Exploring",
        SessionActivityState::Compacting => "Compacting",
        SessionActivityState::Waiting => "Waiting",
        SessionActivityState::Idle => "Idle",
    }
}

fn session_activity_style(state: &SessionActivityState) -> Style {
    match state {
        SessionActivityState::Thinking => Style::default()
            .fg(accent_green())
            .add_modifier(Modifier::BOLD),
        SessionActivityState::Working => Style::default()
            .fg(accent_blue())
            .add_modifier(Modifier::BOLD),
        SessionActivityState::Exploring => Style::default()
            .fg(accent_cyan())
            .add_modifier(Modifier::BOLD),
        SessionActivityState::Compacting => Style::default()
            .fg(accent_gold())
            .add_modifier(Modifier::BOLD),
        SessionActivityState::Waiting => Style::default()
            .fg(accent_magenta())
            .add_modifier(Modifier::BOLD),
        SessionActivityState::Idle => Style::default().fg(text_muted_color()),
    }
}

fn status_style(kind: &SessionStatusKind) -> Style {
    match kind {
        SessionStatusKind::Running => Style::default().fg(accent_green()),
        SessionStatusKind::ToolBusy => Style::default().fg(accent_gold()),
        SessionStatusKind::WaitingInput => Style::default().fg(accent_magenta()),
        SessionStatusKind::Idle => Style::default().fg(Color::Rgb(165, 174, 189)),
        SessionStatusKind::Stale => Style::default().fg(Color::Rgb(124, 130, 143)),
        SessionStatusKind::Completed => Style::default().fg(Color::Rgb(109, 214, 156)),
        SessionStatusKind::Failed => Style::default().fg(accent_red()),
        SessionStatusKind::Unknown => Style::default().fg(text_muted_color()),
    }
}

fn needs_attention(summary: &SessionSummary) -> bool {
    matches!(summary.status.kind, SessionStatusKind::WaitingInput)
}

fn session_state_symbol(summary: &SessionSummary) -> &'static str {
    if needs_attention(summary) {
        "!"
    } else {
        status_symbol(&summary.status.kind)
    }
}

fn session_state_style(summary: &SessionSummary) -> Style {
    if needs_attention(summary) {
        Style::default()
            .fg(accent_red())
            .add_modifier(Modifier::BOLD)
    } else {
        status_style(&summary.status.kind)
    }
}

fn status_symbol(kind: &SessionStatusKind) -> &'static str {
    match kind {
        SessionStatusKind::Running => "●",
        SessionStatusKind::ToolBusy => "◉",
        SessionStatusKind::WaitingInput => "◆",
        SessionStatusKind::Idle => "◌",
        SessionStatusKind::Stale => "○",
        SessionStatusKind::Completed => "✓",
        SessionStatusKind::Failed => "✕",
        SessionStatusKind::Unknown => "?",
    }
}

fn token_breakdown(tokens: &TokenUsage) -> String {
    let mut parts = vec![format!(
        "total {}",
        format_tokens_short(tokens.total_tokens)
    )];

    if let Some(input) = tokens.input_tokens {
        parts.push(format!("in {}", format_tokens_short(input)));
    }

    if let Some(cache_creation) = tokens.cache_creation_input_tokens
        && cache_creation > 0
    {
        parts.push(format!("cache+ {}", format_tokens_short(cache_creation)));
    }

    if let Some(output) = tokens.output_tokens {
        parts.push(format!("out {}", format_tokens_short(output)));
    }

    if let Some(reasoning) = tokens.reasoning_output_tokens {
        parts.push(format!("reason {}", format_tokens_short(reasoning)));
    }

    parts.join("  ")
}

fn render_tool_summary(tools: &[agent_cow_core::ToolCallStat]) -> String {
    if tools.is_empty() {
        return "none".to_string();
    }

    tools
        .iter()
        .take(5)
        .map(|tool| format!("{} x{}", tool.name, tool.count))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_context_window(context: &agent_cow_core::ContextWindowUsage) -> String {
    format!(
        "{} / {} used  •  {} left  •  {}%",
        format_tokens_short(context.used_tokens),
        format_tokens_short(context.limit_tokens),
        format_tokens_short(context.remaining_tokens),
        context.used_percent
    )
}

fn relative_age(timestamp: DateTime<Utc>) -> String {
    let delta = Utc::now() - timestamp;

    if delta.num_seconds() < 60 {
        format!("{}s", delta.num_seconds())
    } else if delta.num_minutes() < 60 {
        format!("{}m", delta.num_minutes())
    } else if delta.num_hours() < 24 {
        format!("{}h", delta.num_hours())
    } else {
        format!("{}d", delta.num_days())
    }
}

fn local_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.with_timezone(&Local).format("%H:%M").to_string()
}

fn session_duration(session: &SessionSummary) -> String {
    let delta = session_elapsed(session);

    if delta.num_minutes() < 1 {
        format!("{}s", delta.num_seconds().max(0))
    } else if delta.num_hours() < 1 {
        format!("{}m", delta.num_minutes())
    } else if delta.num_days() < 1 {
        format!("{}h", delta.num_hours())
    } else if delta.num_days() < 7 {
        format!("{}d", delta.num_days())
    } else {
        format!("{}w", delta.num_weeks())
    }
}

fn format_tokens_short(tokens: u64) -> String {
    if tokens >= 1_000_000_000 {
        format!("{:.1}B", tokens as f64 / 1_000_000_000.0)
    } else if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.0}K", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn format_usd_short(value: f64) -> String {
    if value >= 100.0 {
        format!("${value:.0}")
    } else if value >= 10.0 {
        format!("${value:.1}")
    } else if value >= 1.0 {
        format!("${value:.2}")
    } else if value >= 0.01 {
        format!("${value:.3}")
    } else if value > 0.0 {
        format!("${value:.4}")
    } else {
        "$0".to_string()
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text.to_string();
    }

    if max_chars <= 3 {
        return "...".chars().take(max_chars).collect();
    }

    let truncated: String = text.chars().take(max_chars - 3).collect();
    format!("{truncated}...")
}

#[cfg(test)]
mod tests {
    use super::{
        ListRefreshKind, SessionViewMode, TuiApp, animated_cow_lines, brand_cluster_lines,
        current_detail_summary, footer_line, header_meta_lines, render_machine_unreachable_view,
    };
    use agent_cow_core::{
        MachineOverview, ProviderKind, SessionActivityState, SessionDetail, SessionList,
        SessionLoadProgress, SessionStatus, SessionStatusKind, SessionSummary, StatusConfidence,
        TokenUsage, UsageOverview,
    };
    use chrono::{TimeZone, Utc};
    use ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line};
    use std::{collections::HashMap, time::Duration};

    fn flatten_line(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    fn buffer_row(backend: &TestBackend, row: u16) -> String {
        (0..backend.buffer().area.width)
            .map(|column| backend.buffer()[(column, row)].symbol())
            .collect()
    }

    fn fixture_session(id: &str, machine_label: &str) -> SessionSummary {
        let now = Utc::now() - chrono::Duration::days(2);
        SessionSummary {
            id: id.to_string(),
            machine_id: machine_label.to_string(),
            machine_label: machine_label.to_string(),
            provider: ProviderKind::Codex,
            title: "session".to_string(),
            cwd: "/tmp".to_string(),
            created_at: now,
            updated_at: now,
            run_started_at: None,
            run_active: false,
            archived: false,
            model: Some("gpt-5.4".to_string()),
            agent_role: None,
            git_branch: None,
            git_origin_url: None,
            tokens: TokenUsage::default(),
            cost: None,
            context_window: None,
            status: SessionStatus {
                kind: SessionStatusKind::Stale,
                reason: "stale".to_string(),
                confidence: StatusConfidence::Inferred,
            },
            activity_state: SessionActivityState::Idle,
            rollout_path: None,
            navigation: Vec::new(),
        }
    }

    fn fixture_claude_session(
        id: &str,
        machine_label: &str,
        title: &str,
        rollout_path: Option<&str>,
    ) -> SessionSummary {
        let mut session = fixture_session(id, machine_label);
        session.provider = ProviderKind::Claude;
        session.title = title.to_string();
        session.model = Some("claude-opus-4-6".to_string());
        session.rollout_path = rollout_path.map(ToOwned::to_owned);
        session
    }

    #[test]
    fn animated_cow_lines_match_standard_body() {
        let lines = animated_cow_lines("oo", "__", "|");

        assert_eq!(lines[0], "^__^");
        assert_eq!(lines[1], "(oo)\\_______");
        assert_eq!(lines[2], "(__)\\       )\\/\\");
        assert_eq!(lines[3], "    ||----w |");
        assert_eq!(lines[4], "    ||     ||");
    }

    #[test]
    fn brand_cluster_keeps_cow_alignment_across_messages() {
        let cow = animated_cow_lines("oo", "__", "|");
        let first = brand_cluster_lines(
            70,
            &cow,
            Utc.with_ymd_and_hms(2026, 4, 11, 10, 0, 0).unwrap(),
        );
        let second = brand_cluster_lines(
            70,
            &cow,
            Utc.with_ymd_and_hms(2026, 4, 11, 10, 1, 0).unwrap(),
        );

        let first_strings = first.iter().map(flatten_line).collect::<Vec<_>>();
        let second_strings = second.iter().map(flatten_line).collect::<Vec<_>>();

        for row in 0..5 {
            let first_cow = first_strings[row]
                .find('^')
                .or_else(|| first_strings[row].find('('))
                .or_else(|| first_strings[row].find('|'));
            let second_cow = second_strings[row]
                .find('^')
                .or_else(|| second_strings[row].find('('))
                .or_else(|| second_strings[row].find('|'));
            assert_eq!(first_cow, second_cow, "row {row} cow column drifted");
        }
    }

    #[test]
    fn animated_cow_mouth_keeps_row_width_constant() {
        let closed = animated_cow_lines("oo", "__", "|");
        let talking = animated_cow_lines("oo", "~~", "|");

        assert_eq!(closed[2].chars().count(), talking[2].chars().count());
    }

    #[test]
    fn unreachable_view_keeps_cow_rows_in_one_block() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.overview.machines = vec![MachineOverview {
            source: "remote1".to_string(),
            machine_id: "192.168.0.73".to_string(),
            machine_label: "192.168.0.73".to_string(),
            reachable: false,
            error: Some("remote session stream disconnected".to_string()),
        }];
        app.refresh_machine_labels_cache();

        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_machine_unreachable_view(frame, frame.area(), &app))
            .unwrap();

        let rows = (0..20)
            .map(|row| buffer_row(terminal.backend(), row))
            .collect::<Vec<_>>();
        let head = rows
            .iter()
            .find_map(|row| row.find("^__^"))
            .expect("cow head should render");
        let body = rows
            .iter()
            .find_map(|row| row.find("(oo)\\_______"))
            .expect("cow body should render");
        let mouth = rows
            .iter()
            .find_map(|row| row.find("(__)\\       )\\/\\"))
            .expect("cow mouth should render");
        let legs = rows
            .iter()
            .find_map(|row| row.find("||----w |"))
            .expect("cow legs should render");

        assert_eq!(head, body);
        assert_eq!(body, mouth);
        assert_eq!(legs, body + 4);
    }

    #[test]
    fn apply_load_progress_stays_monotonic_across_staged_loads() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.loading_more = true;
        app.loading_progress_loaded = 64;
        app.loading_progress_total = 377;

        app.apply_load_progress(SessionLoadProgress {
            loaded_sessions: 0,
            total_sessions: 377,
            sources: Vec::new(),
        });
        assert_eq!(app.loading_progress_loaded, 64);

        app.apply_load_progress(SessionLoadProgress {
            loaded_sessions: 72,
            total_sessions: 377,
            sources: Vec::new(),
        });
        assert_eq!(app.loading_progress_loaded, 72);
    }

    #[test]
    fn header_meta_lines_show_scanning_while_background_loading() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.loading_more = true;
        app.loading_progress_loaded = 72;
        app.loading_progress_total = 377;

        let lines = header_meta_lines(&app)
            .into_iter()
            .map(|line| flatten_line(&line))
            .collect::<Vec<_>>();

        assert!(lines.iter().any(|line| line.contains("Scanning")));
        assert!(lines.iter().any(|line| line.contains("0 shown")));
    }

    #[test]
    fn apply_list_refresh_does_not_move_scanning_progress_backwards() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.loading_more = true;
        app.loading_progress_loaded = 145;
        app.loading_progress_total = 377;

        app.apply_list_refresh(
            ListRefreshKind::Progressive,
            SessionList {
                generated_at: Utc.with_ymd_and_hms(2026, 4, 12, 10, 0, 0).unwrap(),
                overview: UsageOverview {
                    total_sessions: 377,
                    ..UsageOverview::default()
                },
                sessions: Vec::new(),
            },
        );

        assert_eq!(app.loading_progress_loaded, 145);
    }

    #[test]
    fn machine_scope_uses_scoped_loading_progress_in_header_and_footer() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.view_mode = SessionViewMode::All;
        app.sessions = vec![
            fixture_session("local|codex:1", "local"),
            fixture_session("remote1|codex:2", "host2"),
        ];
        app.machine_scope = Some("host2".to_string());
        app.loading_more = true;
        app.loading_progress_sources = HashMap::from([
            ("local".to_string(), (296, 377)),
            ("remote1".to_string(), (120, 377)),
        ]);
        app.rebuild_filter(None);

        let header = header_meta_lines(&app)
            .into_iter()
            .map(|line| flatten_line(&line))
            .collect::<Vec<_>>();
        let footer_text = flatten_line(&footer_line(&app));

        assert!(
            header
                .iter()
                .any(|line| line.contains("Sessions") && line.contains("1"))
        );
        assert!(
            header
                .iter()
                .any(|line| line.contains("Scanning") && line.contains("1 shown"))
        );
        assert!(footer_text.contains("1 shown"));
        assert!(!footer_text.contains("754"));
    }

    #[test]
    fn focus_view_hides_old_stale_sessions_by_default() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.sessions = vec![fixture_session("local|codex:1", "local")];
        app.rebuild_filter(None);

        assert!(app.filtered_indices.is_empty());

        let header = header_meta_lines(&app)
            .into_iter()
            .map(|line| flatten_line(&line))
            .collect::<Vec<_>>();
        let footer_text = flatten_line(&footer_line(&app));

        assert!(
            header
                .iter()
                .any(|line| line.contains("View") && line.contains("Focus"))
        );
        assert!(
            header
                .iter()
                .any(|line| line.contains("Sessions") && line.contains("0"))
        );
        assert!(footer_text.contains("1 older sessions hidden"));
    }

    #[test]
    fn cycling_view_mode_reveals_old_sessions() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.sessions = vec![fixture_session("local|codex:1", "local")];
        app.rebuild_filter(None);
        assert!(app.filtered_indices.is_empty());

        app.cycle_view_mode();
        assert_eq!(app.view_mode, SessionViewMode::Recent);
        assert!(app.filtered_indices.contains(&0));
    }

    #[test]
    fn all_view_hides_claude_noise() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.view_mode = SessionViewMode::All;
        app.sessions = vec![
            fixture_claude_session(
                "local|claude:1",
                "local",
                "exit",
                Some("/tmp/project/subagents/agent-123.jsonl"),
            ),
            fixture_claude_session(
                "local|claude:2",
                "local",
                "Do not browse, inspect files, or run shell commands. Use only the provided bundle.",
                Some("/tmp/project/session-automation.jsonl"),
            ),
            fixture_claude_session(
                "local|claude:3",
                "local",
                "useful session",
                Some("/tmp/project/session.jsonl"),
            ),
        ];
        app.rebuild_filter(None);

        assert_eq!(app.filtered_indices, vec![2]);
        assert_eq!(app.hidden_session_count(), 0);
    }

    #[test]
    fn all_view_hides_sessions_without_model() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.view_mode = SessionViewMode::All;
        let mut unknown_model = fixture_session("local|codex:1", "local");
        unknown_model.model = None;
        let known_model = fixture_session("local|codex:2", "local");
        app.sessions = vec![unknown_model, known_model];

        app.rebuild_filter(None);

        assert_eq!(app.filtered_indices, vec![1]);
    }

    #[test]
    fn detail_summary_is_canonical_for_selected_session() {
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        let mut list_summary = fixture_session("local|codex:1", "local");
        list_summary.activity_state = SessionActivityState::Exploring;
        let mut detail_summary = list_summary.clone();
        detail_summary.activity_state = SessionActivityState::Thinking;
        app.sessions = vec![list_summary];
        app.rebuild_filter(Some("local|codex:1"));
        app.table_state.select(Some(0));
        app.detail = Some(SessionDetail {
            summary: detail_summary,
            recent_events: Vec::new(),
            recent_conversation: Vec::new(),
            tool_stats: Vec::new(),
            last_user_message: None,
            last_assistant_message: None,
            active_turns: 0,
            pending_tool_calls: 0,
        });

        app.sync_detail_summary_into_sessions();

        assert_eq!(
            app.sessions[0].activity_state,
            SessionActivityState::Thinking
        );
        assert_eq!(
            current_detail_summary(&app, app.detail.as_ref().unwrap()).activity_state,
            SessionActivityState::Thinking
        );
    }

    #[test]
    fn visible_state_refresh_candidates_are_limited_to_visible_idle_rows() {
        let now = Utc::now();
        let mut app = TuiApp::new(None, Duration::from_secs(5));
        app.view_mode = SessionViewMode::All;
        app.sessions = (0..5)
            .map(|index| {
                let mut session = fixture_session(&format!("local|codex:{index}"), "local");
                session.status.kind = SessionStatusKind::Idle;
                session.status.reason = "idle".to_string();
                session.updated_at = now;
                session
            })
            .collect();
        app.rebuild_filter(None);
        app.table_state.select(Some(0));

        let candidates = app.visible_state_refresh_candidates(Rect::new(0, 0, 120, 6));

        assert_eq!(
            candidates,
            vec![
                "local|codex:0".to_string(),
                "local|codex:1".to_string(),
                "local|codex:2".to_string(),
            ]
        );

        app.visible_state_hydrated_at
            .insert("local|codex:1".to_string(), now);

        let candidates = app.visible_state_refresh_candidates(Rect::new(0, 0, 120, 6));

        assert_eq!(
            candidates,
            vec!["local|codex:0".to_string(), "local|codex:2".to_string(),]
        );
    }
}
