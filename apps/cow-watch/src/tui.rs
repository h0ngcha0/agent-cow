use std::{
    io::{self, Stdout},
    time::{Duration, Instant},
};

use crate::open;
use anyhow::Result;
use chrono::{DateTime, Utc};
use cow_watch_core::{
    ActivityKind, MonitorService, NavigationKind, SessionDetail, SessionList, SessionQuery,
    SessionStatusKind, SessionSummary, TokenUsage, UsageOverview,
};
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
use tokio::task::JoinHandle;

pub async fn run(service: MonitorService, limit: Option<usize>, refresh_secs: u64) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, service, limit, refresh_secs).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn spawn_list_refresh(
    service: MonitorService,
    limit: Option<usize>,
) -> JoinHandle<Result<SessionList>> {
    tokio::spawn(async move {
        service
            .list_sessions(SessionQuery {
                include_archived: false,
                limit,
            })
            .await
    })
}

fn spawn_detail_refresh(
    service: MonitorService,
    session_id: String,
) -> JoinHandle<Result<(String, SessionDetail)>> {
    tokio::spawn(async move {
        let detail = service.get_session(&session_id).await?;
        Ok((session_id, detail))
    })
}

fn queue_detail_refresh(
    app: &TuiApp,
    service: &MonitorService,
    detail_refresh: &mut Option<JoinHandle<Result<(String, SessionDetail)>>>,
) {
    let Some(session_id) = app.selected_session_id().map(ToOwned::to_owned) else {
        return;
    };
    if let Some(handle) = detail_refresh.take() {
        handle.abort();
    }
    *detail_refresh = Some(spawn_detail_refresh(service.clone(), session_id));
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    service: MonitorService,
    limit: Option<usize>,
    refresh_secs: u64,
) -> Result<()> {
    let mut app = TuiApp::new(limit, Duration::from_secs(refresh_secs));
    let initial_load = spawn_list_refresh(service.clone(), app.limit);

    loop {
        terminal.draw(draw_loading)?;

        if initial_load.is_finished() {
            let response = initial_load.await??;
            app.apply_session_list(response);
            break;
        }

        if event::poll(Duration::from_millis(60))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            return Ok(());
        }
    }

    let mut list_refresh: Option<JoinHandle<Result<SessionList>>> = None;
    let mut detail_refresh: Option<JoinHandle<Result<(String, SessionDetail)>>> = None;

    loop {
        if let Some(handle) = list_refresh.as_ref()
            && handle.is_finished()
        {
            let response = list_refresh.take().unwrap().await??;
            app.apply_session_list(response);
            if app.detail_mode {
                queue_detail_refresh(&app, &service, &mut detail_refresh);
            }
        }

        if let Some(handle) = detail_refresh.as_ref()
            && handle.is_finished()
        {
            let (session_id, detail) = detail_refresh.take().unwrap().await??;
            if app.detail_mode && app.selected_session_id() == Some(session_id.as_str()) {
                app.detail = Some(detail);
                app.error = None;
            }
        }

        terminal.draw(|frame| draw(frame, &mut app))?;

        if event::poll(Duration::from_millis(60))?
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
                    KeyCode::Char('q') => break,
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
                    KeyCode::Esc if app.detail_mode => app.close_detail_mode(),
                    KeyCode::Esc => app.clear_filter(),
                    KeyCode::Char('r') => {
                        if list_refresh.is_none() {
                            list_refresh = Some(spawn_list_refresh(service.clone(), app.limit));
                        }
                    }
                    KeyCode::Enter => {
                        let entering_detail = app.toggle_detail_mode();
                        if entering_detail {
                            queue_detail_refresh(&app, &service, &mut detail_refresh);
                        }
                    }
                    KeyCode::Char('o') => app.open_selected_app(),
                    KeyCode::Char('f') => app.open_selected_working_directory(),
                    _ => {}
                }
            }

            if app.selection_changed {
                if app.detail_mode {
                    queue_detail_refresh(&app, &service, &mut detail_refresh);
                }
                app.selection_changed = false;
            }
        }

        if app.last_refresh.elapsed() >= app.refresh_every && list_refresh.is_none() {
            list_refresh = Some(spawn_list_refresh(service.clone(), app.limit));
        }
    }

    Ok(())
}

struct TuiApp {
    sessions: Vec<SessionSummary>,
    overview: UsageOverview,
    filtered_indices: Vec<usize>,
    detail: Option<SessionDetail>,
    table_state: TableState,
    detail_mode: bool,
    detail_scroll: u16,
    local_machine_id: String,
    limit: Option<usize>,
    refresh_every: Duration,
    last_refresh: Instant,
    error: Option<String>,
    notice: Option<UiNotice>,
    filter_input: String,
    filter_mode: bool,
    selection_changed: bool,
}

struct UiNotice {
    message: String,
    is_error: bool,
}

impl TuiApp {
    fn new(limit: Option<usize>, refresh_every: Duration) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));

        Self {
            sessions: Vec::new(),
            overview: UsageOverview::default(),
            filtered_indices: Vec::new(),
            detail: None,
            table_state,
            detail_mode: false,
            detail_scroll: 0,
            local_machine_id: open::local_machine_id(),
            limit,
            refresh_every,
            last_refresh: Instant::now(),
            error: None,
            notice: None,
            filter_input: String::new(),
            filter_mode: false,
            selection_changed: false,
        }
    }

    fn apply_session_list(&mut self, response: SessionList) {
        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        self.sessions = response.sessions;
        self.overview = response.overview;
        self.last_refresh = Instant::now();
        self.error = None;
        self.rebuild_filter(selected_id.as_deref());

        if self.filtered_indices.is_empty() {
            self.detail = None;
            self.detail_mode = false;
            self.detail_scroll = 0;
        }
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
        self.filtered_indices = self
            .sessions
            .iter()
            .enumerate()
            .filter_map(|(index, session)| matches_text_filter(session, &filter).then_some(index))
            .collect();

        if self.filtered_indices.is_empty() {
            self.table_state.select(None);
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

    fn visible_sessions(&self) -> impl Iterator<Item = &SessionSummary> {
        self.filtered_indices
            .iter()
            .filter_map(|index| self.sessions.get(*index))
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

    fn open_selected_app(&mut self) {
        let Some(summary) = self.selected_summary().cloned() else {
            self.notice = Some(UiNotice {
                message: "No session selected.".to_string(),
                is_error: true,
            });
            return;
        };

        self.notice = Some(
            match open::open_session_app(&summary, &self.local_machine_id) {
                Ok(action) => UiNotice {
                    message: format!("Opened {}.", action.label),
                    is_error: false,
                },
                Err(error) => UiNotice {
                    message: error.to_string(),
                    is_error: true,
                },
            },
        );
    }

    fn open_selected_working_directory(&mut self) {
        let Some(summary) = self.selected_summary().cloned() else {
            self.notice = Some(UiNotice {
                message: "No session selected.".to_string(),
                is_error: true,
            });
            return;
        };

        self.notice = Some(
            match open::open_session_navigation(&summary, NavigationKind::WorkingDirectory) {
                Ok(action) => UiNotice {
                    message: format!("Opened {}.", action.label),
                    is_error: false,
                },
                Err(error) => UiNotice {
                    message: error.to_string(),
                    is_error: true,
                },
            },
        );
    }

    fn toggle_detail_mode(&mut self) -> bool {
        if self.selected_summary().is_none() {
            self.notice = Some(UiNotice {
                message: "No session selected.".to_string(),
                is_error: true,
            });
            return false;
        }

        let entering = !self.detail_mode;
        self.detail_mode = entering;
        if entering {
            self.detail_scroll = 0;
        } else {
            self.detail = None;
            self.detail_scroll = 0;
        }
        self.notice = None;
        entering
    }

    fn close_detail_mode(&mut self) {
        self.detail_mode = false;
        self.detail_scroll = 0;
    }

    fn detail_scroll_up(&mut self, step: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(step);
    }

    fn detail_scroll_down(&mut self, step: u16, max_scroll: u16) {
        self.detail_scroll = self.detail_scroll.saturating_add(step).min(max_scroll);
    }

    fn detail_scroll_top(&mut self) {
        self.detail_scroll = 0;
    }

    fn detail_scroll_bottom(&mut self, max_scroll: u16) {
        self.detail_scroll = max_scroll;
    }

    fn show_host_column(&self) -> bool {
        has_multiple_strings(
            self.visible_sessions()
                .map(|session| session.machine_label.as_str()),
        )
    }

    fn show_provider_column(&self) -> bool {
        has_multiple_strings(
            self.visible_sessions()
                .map(|session| session.provider.to_string()),
        )
    }

    fn visible_host_label(&self) -> String {
        aggregate_label(
            self.visible_sessions()
                .map(|session| session.machine_label.clone()),
            "hosts",
        )
        .unwrap_or_else(|| "none".to_string())
    }

    fn detail_max_scroll(&self, frame_area: Rect) -> u16 {
        if !self.detail_mode {
            return 0;
        }
        let content_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(0),
                Constraint::Length(2),
            ])
            .split(frame_area)[1];
        let Some(detail) = &self.detail else {
            return 0;
        };
        let content_height = detail_content_height(content_area) as usize;
        if content_height == 0 {
            return 0;
        }
        let rendered_lines =
            wrapped_line_count(&detail_lines(detail), detail_content_width(content_area));
        rendered_lines
            .saturating_sub(content_height)
            .min(u16::MAX as usize) as u16
    }
}

fn draw(frame: &mut Frame, app: &mut TuiApp) {
    let wide_header = frame.area().width >= 150;
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(frame.area());

    if wide_header {
        render_header_canvas(frame, layout[0], app);
    } else {
        let header = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(22),
                Constraint::Percentage(60),
                Constraint::Percentage(18),
            ])
            .split(layout[0]);

        frame.render_widget(render_header_meta(app), header[0]);
        render_header_actions(frame, header[1], app);
        frame.render_widget(render_brand_cluster(header[2].width), header[2]);
    }

    if app.detail_mode {
        render_detail_view(frame, layout[1], app);
    } else {
        let window = table_visible_window(app, layout[1]);
        let mut render_state = TableState::default().with_selected(window.selected);
        frame.render_stateful_widget(
            render_sessions_table(app, layout[1].width, window),
            layout[1],
            &mut render_state,
        );
    }

    frame.render_widget(render_footer(app), layout[2]);
}

fn detail_content_height(area: Rect) -> u16 {
    area.height.saturating_sub(2)
}

fn detail_content_width(area: Rect) -> u16 {
    area.width.saturating_sub(2).max(1)
}

fn draw_loading(frame: &mut Frame) {
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

    let brand_width = body[0].width.min(22);
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
        Paragraph::new(Line::from(vec![Span::styled(
            format!("Loading sessions{}", loading_dots()),
            Style::default()
                .fg(accent_cyan())
                .add_modifier(Modifier::BOLD),
        )]))
        .alignment(Alignment::Center),
        body[2],
    );

    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            "Restoring caches and scanning recent Codex rollouts",
            Style::default().fg(text_muted_color()),
        )]))
        .alignment(Alignment::Center),
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

    let (action_column_a, action_column_b, action_column_c) = header_action_columns(app);
    let action_a_width = keymap_column_width(&action_column_a, 10);
    let action_b_width = keymap_column_width(&action_column_b, 7);
    let action_c_width = keymap_column_width(&action_column_c, 8);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(18),
            Constraint::Length(4),
            Constraint::Length(action_a_width),
            Constraint::Length(4),
            Constraint::Length(action_b_width),
            Constraint::Length(4),
            Constraint::Length(action_c_width),
            Constraint::Min(0),
            Constraint::Length(20),
            Constraint::Length(2),
        ])
        .split(body);

    frame.render_widget(render_header_meta(app), columns[0]);
    frame.render_widget(render_action_column(&action_column_a, 10), columns[2]);
    frame.render_widget(render_action_column(&action_column_b, 7), columns[4]);
    frame.render_widget(render_action_column(&action_column_c, 8), columns[6]);
    frame.render_widget(render_brand_cluster(columns[8].width), columns[8]);
}

fn render_action_column(items: &[Option<KeymapItem>], key_width: usize) -> Paragraph<'static> {
    Paragraph::new(Text::from(
        items
            .iter()
            .cloned()
            .map(|item| keymap_grid_line(item, key_width))
            .collect::<Vec<_>>(),
    ))
}

fn render_brand_cluster(width: u16) -> Paragraph<'static> {
    Paragraph::new(Text::from(cow_watch_brand_cluster_lines(width))).alignment(Alignment::Left)
}

fn loading_dots() -> &'static str {
    match (Utc::now().timestamp_millis() / 250).rem_euclid(4) {
        0 => "",
        1 => ".",
        2 => "..",
        _ => "...",
    }
}

fn header_meta_lines(app: &TuiApp) -> Vec<Line<'static>> {
    vec![
        header_meta_line("Host", app.visible_host_label()),
        header_meta_line(
            "Sessions",
            format!("{}/{}", app.filtered_indices.len(), app.sessions.len()),
        ),
        header_meta_line("Spend", format_usd_short(app.overview.total_cost_usd)),
        header_meta_line("Tokens", format_tokens_short(app.overview.total_tokens)),
    ]
}

fn render_header_meta(app: &TuiApp) -> Paragraph<'static> {
    Paragraph::new(Text::from(header_meta_lines(app)))
}

type HeaderActionColumns = (
    Vec<Option<KeymapItem>>,
    Vec<Option<KeymapItem>>,
    Vec<Option<KeymapItem>>,
);

fn header_action_columns(app: &TuiApp) -> HeaderActionColumns {
    let primary = vec![
        Some(action_item(
            "enter",
            if app.detail_mode { "Back" } else { "Describe" },
            true,
        )),
        Some(action_item("/", "Filter", !app.detail_mode)),
        Some(action_item(
            "esc",
            if app.detail_mode { "Back" } else { "Clear" },
            true,
        )),
        Some(action_item("r", "Refresh", true)),
        None,
        None,
        None,
    ];
    let secondary = vec![
        Some(action_item("o", "Open", true)),
        Some(action_item("f", "Folder", true)),
        Some(action_item("j/k", "Move", true)),
        Some(action_item("Home", "Top", true)),
        Some(action_item("End", "End", true)),
        None,
        None,
    ];
    let tertiary = vec![
        Some(action_item("PgUp", "Page Up", true)),
        Some(action_item("PgDn", "Page Down", true)),
        Some(action_item("q", "Quit", true)),
        None,
        None,
        None,
        None,
    ];

    (primary, secondary, tertiary)
}

fn render_header_actions(frame: &mut Frame, area: Rect, app: &TuiApp) {
    let (action_column_a, action_column_b, action_column_c) = header_action_columns(app);
    render_header_actions_compact(
        frame,
        area,
        &action_column_a,
        &action_column_b,
        &action_column_c,
    );
}

fn render_header_actions_compact(
    frame: &mut Frame,
    area: Rect,
    action_column_a: &[Option<KeymapItem>],
    action_column_b: &[Option<KeymapItem>],
    action_column_c: &[Option<KeymapItem>],
) {
    let action_a_width = keymap_column_width(action_column_a, 10);
    let action_b_width = keymap_column_width(action_column_b, 7);
    let action_c_width = keymap_column_width(action_column_c, 8);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(action_a_width),
            Constraint::Length(1),
            Constraint::Length(action_b_width),
            Constraint::Length(1),
            Constraint::Length(action_c_width),
        ])
        .split(area);

    frame.render_widget(render_action_column(action_column_a, 10), columns[0]);
    frame.render_widget(render_action_column(action_column_b, 7), columns[2]);
    frame.render_widget(render_action_column(action_column_c, 8), columns[4]);
}

fn keymap_grid_line(item: Option<KeymapItem>, key_width: usize) -> Line<'static> {
    match item {
        Some(item) => {
            let key = format!("<{}>", item.key);
            Line::from(vec![
                Span::styled(format!("{key:<width$}", width = key_width), item.key_style),
                Span::styled(item.label, item.label_style),
            ])
        }
        None => Line::from(""),
    }
}

fn cow_watch_brand_cluster_lines(width: u16) -> Vec<Line<'static>> {
    let frame = animated_cow_frame(Utc::now());
    let area_width = width as usize;
    let ascii_lines = animated_cow_lines(frame.eyes, frame.tail);
    let block_width = ascii_lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);
    let left_pad = width_for_alignment(area_width, block_width);
    ascii_lines
        .into_iter()
        .map(|line| positioned_brand_line(left_pad, &line))
        .collect()
}

fn positioned_brand_line(offset: usize, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::raw(" ".repeat(offset)),
        Span::styled(
            value.to_string(),
            Style::default()
                .fg(accent_gold())
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn width_for_alignment(area_width: usize, block_width: usize) -> usize {
    area_width.saturating_sub(block_width + 2)
}

struct CowFrame {
    eyes: &'static str,
    tail: &'static str,
}

fn animated_cow_frame(now: DateTime<Utc>) -> CowFrame {
    let blink = ((now.timestamp_millis() / 350).rem_euclid(6)) as usize;
    let tail = match blink {
        1 | 2 => "/",
        4 | 5 => "\\",
        _ => "|",
    };
    let eyes = if matches!(blink, 2 | 5) { "--" } else { "oo" };

    CowFrame { eyes, tail }
}

fn animated_cow_lines(eyes: &str, tail: &str) -> Vec<String> {
    vec![
        "^__^".to_string(),
        format!("({eyes})\\_______"),
        "(__)\\       )\\/\\".to_string(),
        format!("    ||----w {tail}"),
        "    ||     ||".to_string(),
    ]
}

fn keymap_column_width(items: &[Option<KeymapItem>], key_width: usize) -> u16 {
    items
        .iter()
        .filter_map(|item| item.as_ref())
        .map(|item| (key_width + item.label.chars().count()) as u16)
        .max()
        .unwrap_or(key_width as u16)
}

fn render_sessions_table(
    app: &TuiApp,
    table_width: u16,
    window: VisibleRowWindow,
) -> Table<'static> {
    let title = format!("Sessions ({})", app.filtered_indices.len());

    let show_host = app.show_host_column();
    let show_provider = app.show_provider_column();
    let widths = session_table_widths(table_width, show_provider, show_host);

    let mut header_cells = vec![
        Cell::from("").style(Style::default().fg(text_muted_color())),
        Cell::from("LAST").style(Style::default().fg(Color::Rgb(201, 210, 220))),
        Cell::from("DUR").style(Style::default().fg(accent_gold())),
        Cell::from("NAME").style(Style::default().fg(text_primary_color())),
        Cell::from("PROJECT").style(Style::default().fg(accent_cyan())),
    ];
    let mut constraints = vec![
        Constraint::Length(widths.state),
        Constraint::Length(widths.age),
        Constraint::Length(widths.duration),
        Constraint::Length(widths.name),
        Constraint::Length(widths.project),
    ];

    if show_provider {
        header_cells.push(Cell::from("PVD").style(Style::default().fg(accent_magenta())));
        constraints.push(Constraint::Length(widths.provider.unwrap_or(4)));
    }

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
                Cell::from(status_symbol(&session.status.kind))
                    .style(status_style(&session.status.kind)),
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

            if show_provider {
                cells.push(
                    Cell::from(truncate_chars(
                        &short_provider(session),
                        widths.provider.unwrap_or(4).saturating_sub(1) as usize,
                    ))
                    .style(Style::default().fg(accent_magenta())),
                );
            }

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

#[derive(Clone, Copy)]
struct SessionTableWidths {
    state: u16,
    duration: u16,
    cost: u16,
    cost_hour: u16,
    cost_day: u16,
    context: u16,
    name: u16,
    project: u16,
    provider: Option<u16>,
    host: Option<u16>,
    model: u16,
    tokens: u16,
    age: u16,
}

fn session_table_widths(
    table_width: u16,
    show_provider: bool,
    show_host: bool,
) -> SessionTableWidths {
    #[derive(Clone, Copy)]
    enum ColumnId {
        State,
        Cost,
        CostHour,
        CostDay,
        Context,
        Name,
        Project,
        Provider,
        Host,
        Model,
        Tokens,
        Age,
        Duration,
    }

    let mut columns = vec![
        (ColumnId::State, 1_u16, 1_u16),
        (ColumnId::Age, 4, 5),
        (ColumnId::Duration, 4, 5),
        (ColumnId::Cost, 6, 5),
        (ColumnId::CostHour, 6, 5),
        (ColumnId::CostDay, 6, 5),
        (ColumnId::Context, 4, 5),
        (ColumnId::Name, 16_u16, 26_u16),
        (ColumnId::Project, 8_u16, 10_u16),
    ];
    if show_provider {
        columns.push((ColumnId::Provider, 4, 6));
    }
    if show_host {
        columns.push((ColumnId::Host, 8, 10));
    }
    columns.extend([(ColumnId::Model, 9, 15), (ColumnId::Tokens, 7, 10)]);

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
        state: 1,
        duration: 4,
        cost: 6,
        cost_hour: 6,
        cost_day: 6,
        context: 4,
        name: 24,
        project: 10,
        provider: None,
        host: None,
        model: 8,
        tokens: 7,
        age: 4,
    };

    for (id, width) in resolved {
        match id {
            ColumnId::State => widths.state = width,
            ColumnId::Duration => widths.duration = width,
            ColumnId::Cost => widths.cost = width,
            ColumnId::CostHour => widths.cost_hour = width,
            ColumnId::CostDay => widths.cost_day = width,
            ColumnId::Context => widths.context = width,
            ColumnId::Name => widths.name = width,
            ColumnId::Project => widths.project = width,
            ColumnId::Provider => widths.provider = Some(width),
            ColumnId::Host => widths.host = Some(width),
            ColumnId::Model => widths.model = width,
            ColumnId::Tokens => widths.tokens = width,
            ColumnId::Age => widths.age = width,
        }
    }

    widths
}

fn render_detail_view(frame: &mut Frame, area: Rect, app: &mut TuiApp) {
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

    let lines = detail_lines(detail);
    let max_scroll = wrapped_line_count(&lines, detail_content_width(area))
        .saturating_sub(detail_content_height(area) as usize)
        .min(u16::MAX as usize) as u16;
    app.detail_scroll = app.detail_scroll.min(max_scroll);

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(panel_border_color()))
                    .title(Line::from(vec![
                        Span::styled(
                            "Describe",
                            Style::default()
                                .fg(accent_cyan())
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(" • ", Style::default().fg(text_muted_color())),
                        Span::styled(
                            short_status_label(&detail.summary.status.kind),
                            status_style(&detail.summary.status.kind).add_modifier(Modifier::BOLD),
                        ),
                    ])),
            )
            .scroll((app.detail_scroll, 0))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_footer(app: &TuiApp) -> Paragraph<'static> {
    let line1 = if let Some(notice) = &app.notice {
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
    };

    let line2 = if app.detail_mode {
        Line::from("j/k scroll  •  PgUp/PgDn page  •  g/G top/end  •  enter/esc back  •  q quit")
    } else {
        Line::from("j/k move  •  PgUp/PgDn jump  •  enter details  •  o open app  •  / filter")
    };

    Paragraph::new(Text::from(vec![line1, line2]))
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
        lines.push(Line::from(vec![
            indent(),
            Span::styled(
                format!(
                    "1h {}  •  1d {}  •  input {}  •  cached {}  •  output {}",
                    format_usd_short(cost.hour_usd),
                    format_usd_short(cost.day_usd),
                    format_usd_short(cost.input_usd),
                    format_usd_short(cost.cached_input_usd),
                    format_usd_short(cost.output_usd)
                ),
                Style::default().fg(text_muted_color()),
            ),
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
                    format!("  {} ", event.timestamp.format("%H:%M")),
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
    meta_line(label, truncate_chars(&value, 8))
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

fn context_bar_line(context: &cow_watch_core::ContextWindowUsage) -> Line<'static> {
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

fn action_item(key: &str, label: &str, enabled: bool) -> KeymapItem {
    let key_style = if enabled {
        Style::default().fg(accent_blue())
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let label_style = if enabled {
        Style::default().fg(text_muted_color())
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

fn short_provider(session: &SessionSummary) -> String {
    session.provider.to_string().chars().take(3).collect()
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

fn context_style(context: Option<&cow_watch_core::ContextWindowUsage>) -> Style {
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

    if let Some(output) = tokens.output_tokens {
        parts.push(format!("out {}", format_tokens_short(output)));
    }

    if let Some(reasoning) = tokens.reasoning_output_tokens {
        parts.push(format!("reason {}", format_tokens_short(reasoning)));
    }

    parts.join("  ")
}

fn render_tool_summary(tools: &[cow_watch_core::ToolCallStat]) -> String {
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

fn format_context_window(context: &cow_watch_core::ContextWindowUsage) -> String {
    format!(
        "{} / {} used  •  {} left  •  {}%",
        format_tokens_short(context.used_tokens),
        format_tokens_short(context.limit_tokens),
        format_tokens_short(context.remaining_tokens),
        context.used_percent
    )
}

fn aggregate_label<I>(mut values: I, plural_label: &str) -> Option<String>
where
    I: Iterator<Item = String>,
{
    let first = values.next()?;
    let mut count = 1usize;
    let mut mixed = false;

    for value in values {
        count += 1;
        if value != first {
            mixed = true;
        }
    }

    if mixed {
        Some(format!("{count} {plural_label}"))
    } else {
        Some(first)
    }
}

fn has_multiple_strings<I, S>(mut values: I) -> bool
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let Some(first) = values.next() else {
        return false;
    };
    let first = first.as_ref().to_string();
    values.any(|value| value.as_ref() != first)
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
    use super::animated_cow_lines;

    #[test]
    fn animated_cow_lines_match_standard_body() {
        let lines = animated_cow_lines("oo", "|");

        assert_eq!(lines[0], "^__^");
        assert_eq!(lines[1], "(oo)\\_______");
        assert_eq!(lines[2], "(__)\\       )\\/\\");
        assert_eq!(lines[3], "    ||----w |");
        assert_eq!(lines[4], "    ||     ||");
    }
}
