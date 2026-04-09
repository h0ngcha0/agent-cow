use std::{
    io::{self, Stdout},
    time::{Duration, Instant},
};

use crate::open;
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use eaglewatch_core::{
    MonitorService, NavigationKind, SessionDetail, SessionQuery, SessionStatusKind, SessionSummary,
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};

pub async fn run(service: MonitorService, limit: usize, refresh_secs: u64) -> Result<()> {
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

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    service: MonitorService,
    limit: usize,
    refresh_secs: u64,
) -> Result<()> {
    let mut app = TuiApp::new(limit, Duration::from_secs(refresh_secs));
    app.reload(&service).await?;

    loop {
        terminal.draw(|frame| draw(frame, &mut app))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                    KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                    KeyCode::Char('r') => app.reload(&service).await?,
                    KeyCode::Enter | KeyCode::Char('o') => app.open_selected_app(),
                    KeyCode::Char('f') => app.open_selected_working_directory(),
                    _ => {}
                }

                if app.selection_changed {
                    app.refresh_detail(&service).await?;
                    app.selection_changed = false;
                }
            }
        }

        if app.last_refresh.elapsed() >= app.refresh_every {
            app.reload(&service).await?;
        }
    }

    Ok(())
}

struct TuiApp {
    sessions: Vec<SessionSummary>,
    detail: Option<SessionDetail>,
    list_state: ListState,
    local_machine_id: String,
    limit: usize,
    refresh_every: Duration,
    last_refresh: Instant,
    error: Option<String>,
    notice: Option<UiNotice>,
    selection_changed: bool,
}

struct UiNotice {
    message: String,
    is_error: bool,
}

impl TuiApp {
    fn new(limit: usize, refresh_every: Duration) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));

        Self {
            sessions: Vec::new(),
            detail: None,
            list_state,
            local_machine_id: open::local_machine_id(),
            limit,
            refresh_every,
            last_refresh: Instant::now(),
            error: None,
            notice: None,
            selection_changed: false,
        }
    }

    async fn reload(&mut self, service: &MonitorService) -> Result<()> {
        let selected_id = self.selected_session_id().map(ToOwned::to_owned);
        let response = service
            .list_sessions(SessionQuery {
                include_archived: false,
                limit: Some(self.limit),
            })
            .await?;

        self.sessions = response.sessions;
        self.last_refresh = Instant::now();
        self.error = None;

        let selected_index = selected_id
            .and_then(|selected_id| {
                self.sessions
                    .iter()
                    .position(|session| session.id == selected_id)
            })
            .unwrap_or(0);

        if self.sessions.is_empty() {
            self.list_state.select(None);
            self.detail = None;
        } else {
            self.list_state
                .select(Some(selected_index.min(self.sessions.len() - 1)));
            self.refresh_detail(service).await?;
        }

        Ok(())
    }

    async fn refresh_detail(&mut self, service: &MonitorService) -> Result<()> {
        let Some(session_id) = self.selected_session_id().map(ToOwned::to_owned) else {
            self.detail = None;
            return Ok(());
        };

        match service.get_session(&session_id).await {
            Ok(detail) => {
                self.detail = Some(detail);
                self.error = None;
            }
            Err(error) => {
                self.error = Some(error.to_string());
            }
        }

        Ok(())
    }

    fn selected_session_id(&self) -> Option<&str> {
        self.list_state
            .selected()
            .and_then(|index| self.sessions.get(index))
            .map(|session| session.id.as_str())
    }

    fn selected_summary(&self) -> Option<&SessionSummary> {
        self.detail
            .as_ref()
            .map(|detail| &detail.summary)
            .or_else(|| {
                self.list_state
                    .selected()
                    .and_then(|index| self.sessions.get(index))
            })
    }

    fn select_next(&mut self) {
        if self.sessions.is_empty() {
            return;
        }

        let next = match self.list_state.selected() {
            Some(index) if index + 1 < self.sessions.len() => index + 1,
            _ => 0,
        };

        self.list_state.select(Some(next));
        self.selection_changed = true;
    }

    fn select_previous(&mut self) {
        if self.sessions.is_empty() {
            return;
        }

        let previous = match self.list_state.selected() {
            Some(index) if index > 0 => index - 1,
            _ => self.sessions.len() - 1,
        };

        self.list_state.select(Some(previous));
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
}

fn draw(frame: &mut Frame, app: &mut TuiApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(layout[1]);

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "eaglewatch",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  local Codex observability · shared Rust core"),
    ]))
    .block(Block::default().borders(Borders::ALL).title("Overview"));
    frame.render_widget(header, layout[0]);

    let items: Vec<ListItem> = app
        .sessions
        .iter()
        .map(|session| {
            let status = format!("{}", session.status.kind);
            let line1 = Line::from(vec![
                Span::styled(
                    format!("{status:<14}"),
                    Style::default().fg(status_color(&session.status.kind)),
                ),
                Span::styled(
                    session.title.clone(),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
            ]);
            let line2 = Line::from(vec![
                Span::styled(
                    format!("{:>8} tokens", session.tokens.total_tokens),
                    Style::default().fg(Color::Yellow),
                ),
                Span::raw("  "),
                Span::styled(session.cwd.clone(), Style::default().fg(Color::Gray)),
            ]);
            ListItem::new(Text::from(vec![line1, line2]))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Sessions"))
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(17, 34, 51))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("› ");
    frame.render_stateful_widget(list, body[0], &mut app.list_state);

    let detail_text = render_detail_text(app);
    let detail = Paragraph::new(detail_text)
        .block(Block::default().borders(Borders::ALL).title("Detail"))
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, body[1]);

    let mut footer_spans = vec![Span::styled(
        "q quit • j/k move • enter/o open in app • f folder • r refresh",
        Style::default().fg(Color::Gray),
    )];
    if let Some(notice) = &app.notice {
        footer_spans.push(Span::raw("  •  "));
        footer_spans.push(Span::styled(
            notice.message.clone(),
            Style::default().fg(if notice.is_error {
                Color::Red
            } else {
                Color::Green
            }),
        ));
    }

    let footer = Paragraph::new(Line::from(footer_spans));
    frame.render_widget(footer, layout[2]);
}

fn render_detail_text(app: &TuiApp) -> Text<'static> {
    if let Some(error) = &app.error {
        return Text::from(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(Color::Red),
        )));
    }

    let Some(detail) = &app.detail else {
        return Text::from("No session selected.");
    };

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "{} ({})",
            detail.summary.status.kind, detail.summary.status.confidence
        )),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Reason: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(detail.summary.status.reason.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Model: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(
            detail
                .summary
                .model
                .clone()
                .unwrap_or_else(|| "n/a".to_string()),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Tokens: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(detail.summary.tokens.total_tokens.to_string()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("CWD: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(detail.summary.cwd.clone()),
    ]));
    lines.push(Line::from(""));

    if let Some(message) = &detail.last_assistant_message {
        lines.push(Line::from(Span::styled(
            "Last assistant message",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(message.clone()));
        lines.push(Line::from(""));
    }

    if !detail.tool_stats.is_empty() {
        lines.push(Line::from(Span::styled(
            "Top tools",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for tool in detail.tool_stats.iter().take(6) {
            lines.push(Line::from(format!(
                "  {:<20} {:>4} calls",
                tool.name, tool.count
            )));
        }
        lines.push(Line::from(""));
    }

    lines.push(Line::from(Span::styled(
        "Recent activity",
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
    )));
    for event in detail.recent_events.iter().rev().take(14) {
        lines.push(Line::from(format!(
            "  {} {:<11} {}",
            event.timestamp.format("%H:%M:%S"),
            format!("{:?}", event.kind).to_lowercase(),
            event.summary
        )));
    }

    Text::from(lines)
}

fn status_color(kind: &SessionStatusKind) -> Color {
    match kind {
        SessionStatusKind::Running => Color::Cyan,
        SessionStatusKind::ToolBusy => Color::LightYellow,
        SessionStatusKind::WaitingInput => Color::LightMagenta,
        SessionStatusKind::Completed => Color::Green,
        SessionStatusKind::Stale | SessionStatusKind::Idle => Color::Gray,
        SessionStatusKind::Failed => Color::Red,
        SessionStatusKind::Unknown => Color::White,
    }
}
