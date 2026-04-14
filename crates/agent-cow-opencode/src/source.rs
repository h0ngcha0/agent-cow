use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};

use agent_cow_core::{
    ActivityEvent, ActivityKind, NavigationKind, NavigationTarget, PricingSource, ProviderKind,
    SessionActivityState, SessionCost, SessionDetail, SessionList, SessionLoadProgress,
    SessionQuery, SessionRuntimeEvidence, SessionRuntimeState, SessionRuntimeWindows,
    SessionSource, SessionStatus, SessionStatusKind, SessionSummary, StatusConfidence, TokenUsage,
    ToolCallStat, UsageOverview, derive_session_runtime_state,
};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use directories::BaseDirs;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

const RECENT_EVENT_LIMIT: usize = 48;
const RECENT_CONVERSATION_LIMIT: usize = 32;
const RUNNING_TTL_SECONDS: i64 = 90;
const STALE_AFTER_MINUTES: i64 = 20;
const ACTIVITY_WINDOW_SECONDS: i64 = 30;
const TOOL_BUSY_ACTIVITY_WINDOW_SECONDS: i64 = 75;
const COMPACTION_ACTIVITY_WINDOW_SECONDS: i64 = 12;

#[derive(Clone, Debug)]
pub struct OpenCodeSource {
    opencode_home: PathBuf,
    db_path: PathBuf,
    machine_id: String,
    machine_label: String,
}

#[derive(Clone, Debug)]
struct SessionRow {
    id: String,
    parent_id: Option<String>,
    project_id: String,
    title: String,
    directory: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    time_compacting: Option<DateTime<Utc>>,
    archived: bool,
}

#[derive(Clone, Debug)]
struct MessageRow {
    time_created: DateTime<Utc>,
    data: Value,
}

#[derive(Clone, Debug)]
struct PartRow {
    time_created: DateTime<Utc>,
    data: Value,
}

#[derive(Clone, Debug, Default)]
struct SessionAnalysis {
    model: Option<String>,
    provider_id: Option<String>,
    agent_role: Option<String>,
    tokens: TokenUsage,
    cost: Option<SessionCost>,
    recent_events: Vec<ActivityEvent>,
    recent_conversation: Vec<ActivityEvent>,
    last_user_message: Option<(DateTime<Utc>, String)>,
    last_assistant_message: Option<(DateTime<Utc>, String)>,
    tool_stats: HashMap<String, ToolCallStat>,
    active_turns: usize,
    pending_tool_calls: usize,
    pending_exploration_tool_calls: usize,
    run_active: bool,
    latest_tool: Option<(DateTime<Utc>, String)>,
    latest_assistant_feedback_at: Option<DateTime<Utc>>,
    latest_reasoning_at: Option<DateTime<Utc>>,
    latest_compaction_at: Option<DateTime<Utc>>,
    finished: bool,
}

#[derive(Clone, Debug)]
struct OpenCodePricing {
    input_per_million: f64,
    cache_creation_input_per_million: f64,
    cached_input_per_million: f64,
    output_per_million: f64,
}

impl OpenCodeSource {
    pub fn new(opencode_home: impl Into<PathBuf>) -> Self {
        let configured_path = expand_known_path_vars(opencode_home.into());
        let opencode_home = normalize_opencode_home(configured_path.clone());
        let machine_label = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "local".to_string());

        Self {
            db_path: resolve_opencode_db_path(&opencode_home, &configured_path),
            opencode_home,
            machine_id: machine_label.clone(),
            machine_label,
        }
    }

    pub fn from_default_home() -> Result<Self> {
        let base_dirs =
            BaseDirs::new().ok_or_else(|| anyhow!("could not resolve a home directory"))?;
        let xdg_data = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dirs.home_dir().join(".local").join("share"));
        Ok(Self::new(xdg_data.join("opencode")))
    }

    fn open_db(&self) -> Result<Connection> {
        let connection = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| {
            format!(
                "failed to open OpenCode database `{}`",
                self.db_path.display()
            )
        })?;
        let _ = connection.busy_timeout(StdDuration::from_millis(250));
        Ok(connection)
    }

    fn load_session_rows(
        &self,
        conn: &Connection,
        include_archived: bool,
    ) -> Result<Vec<SessionRow>> {
        let sql = if include_archived {
            "select id, project_id, parent_id, title, directory, time_created, time_updated, time_compacting, time_archived from session order by time_updated desc"
        } else {
            "select id, project_id, parent_id, title, directory, time_created, time_updated, time_compacting, time_archived from session where time_archived is null order by time_updated desc"
        };

        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(SessionRow {
                id: row.get(0)?,
                project_id: row.get(1)?,
                parent_id: row.get(2)?,
                title: row.get(3)?,
                directory: row.get(4)?,
                created_at: timestamp_millis_to_utc(row.get::<_, i64>(5)?),
                updated_at: timestamp_millis_to_utc(row.get::<_, i64>(6)?),
                time_compacting: row.get::<_, Option<i64>>(7)?.map(timestamp_millis_to_utc),
                archived: row.get::<_, Option<i64>>(8)?.is_some(),
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn load_messages(&self, conn: &Connection, session_id: &str) -> Result<Vec<MessageRow>> {
        let mut stmt = conn.prepare(
            "select id, time_created, time_updated, data from message where session_id = ? order by time_created asc",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut result = Vec::new();
        for row in rows {
            let (_id, time_created, _time_updated, data) = row?;
            result.push(MessageRow {
                time_created: timestamp_millis_to_utc(time_created),
                data: serde_json::from_str(&data).with_context(|| {
                    format!("failed to parse OpenCode message for session `{session_id}`")
                })?,
            });
        }
        Ok(result)
    }

    fn load_parts(&self, conn: &Connection, session_id: &str) -> Result<Vec<PartRow>> {
        let mut stmt = conn.prepare(
            "select time_created, data from part where session_id = ? order by time_created asc",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut result = Vec::new();
        for row in rows {
            let (time_created, data) = row?;
            result.push(PartRow {
                time_created: timestamp_millis_to_utc(time_created),
                data: serde_json::from_str(&data).with_context(|| {
                    format!("failed to parse OpenCode part for session `{session_id}`")
                })?,
            });
        }
        Ok(result)
    }

    fn analyze_session(
        &self,
        row: &SessionRow,
        messages: &[MessageRow],
        parts: &[PartRow],
    ) -> SessionAnalysis {
        let mut analysis = SessionAnalysis::default();
        let now = Utc::now();
        let mut total_cost_usd = 0.0f64;
        let mut hour_cost_usd = 0.0f64;
        let mut day_cost_usd = 0.0f64;
        let mut open_steps = 0usize;

        for message in messages {
            let role = message
                .data
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let timestamp = message
                .data
                .get("time")
                .and_then(|time| time.get("created"))
                .and_then(Value::as_i64)
                .map(timestamp_millis_to_utc)
                .unwrap_or(message.time_created);

            if role == "user" {
                if let Some(text) = user_message_text(&message.data).or_else(|| {
                    let title = normalize_single_line_text(&row.title);
                    (!title.is_empty()).then_some(title)
                }) {
                    let event = ActivityEvent {
                        timestamp,
                        kind: ActivityKind::User,
                        summary: text.clone(),
                    };
                    push_recent_event(
                        &mut analysis.recent_events,
                        event.clone(),
                        RECENT_EVENT_LIMIT,
                    );
                    push_recent_event(
                        &mut analysis.recent_conversation,
                        event,
                        RECENT_CONVERSATION_LIMIT,
                    );
                    analysis.last_user_message = Some((timestamp, text));
                }

                if analysis.model.is_none() {
                    analysis.model = message
                        .data
                        .get("model")
                        .and_then(|model| model.get("modelID"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    analysis.provider_id = message
                        .data
                        .get("model")
                        .and_then(|model| model.get("providerID"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    analysis.agent_role = message
                        .data
                        .get("agent")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
                continue;
            }

            if role != "assistant" {
                continue;
            }

            let model = message
                .data
                .get("modelID")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    message
                        .data
                        .get("model")
                        .and_then(|model| model.get("modelID"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                });
            let provider_id = message
                .data
                .get("providerID")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    message
                        .data
                        .get("model")
                        .and_then(|model| model.get("providerID"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                });
            if analysis.model.is_none() {
                analysis.model = model.clone();
            }
            if analysis.provider_id.is_none() {
                analysis.provider_id = provider_id.clone();
            }
            if analysis.agent_role.is_none() {
                analysis.agent_role = message
                    .data
                    .get("agent")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }

            if let Some(delta) = parse_opencode_tokens(message.data.get("tokens")) {
                accumulate_token_usage(&mut analysis.tokens, &delta);

                let estimated_cost = message
                    .data
                    .get("cost")
                    .and_then(Value::as_f64)
                    .filter(|cost| *cost > 0.0)
                    .or_else(|| {
                        resolve_pricing(provider_id.as_deref(), model.as_deref())
                            .map(|pricing| estimate_token_cost(&delta, &pricing))
                    });

                if let Some(cost) = estimated_cost {
                    total_cost_usd += cost;
                    if timestamp >= now - Duration::hours(1) {
                        hour_cost_usd += cost;
                    }
                    if timestamp >= now - Duration::days(1) {
                        day_cost_usd += cost;
                    }
                }
            }

            let finish = message.data.get("finish").and_then(Value::as_str);
            if finish == Some("stop") {
                analysis.finished = true;
            }
        }

        for part in parts {
            let kind = part
                .data
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match kind {
                "tool" => {
                    let tool_name = part
                        .data
                        .get("tool")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string();
                    let status = part
                        .data
                        .get("state")
                        .and_then(|state| state.get("status"))
                        .and_then(Value::as_str)
                        .unwrap_or("completed");
                    let summary = format!("used {tool_name}");
                    push_recent_event(
                        &mut analysis.recent_events,
                        ActivityEvent {
                            timestamp: part.time_created,
                            kind: ActivityKind::ToolCall,
                            summary,
                        },
                        RECENT_EVENT_LIMIT,
                    );
                    analysis.latest_tool = Some((part.time_created, tool_name.clone()));
                    let entry =
                        analysis
                            .tool_stats
                            .entry(tool_name.clone())
                            .or_insert(ToolCallStat {
                                name: tool_name.clone(),
                                count: 0,
                                last_seen: None,
                            });
                    entry.count += 1;
                    entry.last_seen = Some(part.time_created);
                    if status != "completed" {
                        analysis.pending_tool_calls += 1;
                        if is_exploration_tool(&tool_name) {
                            analysis.pending_exploration_tool_calls += 1;
                        }
                    }
                }
                "text" => {
                    if let Some(text) = part.data.get("text").and_then(Value::as_str) {
                        let text = normalize_single_line_text(text);
                        if !text.is_empty() {
                            let event = ActivityEvent {
                                timestamp: part.time_created,
                                kind: ActivityKind::Assistant,
                                summary: text.clone(),
                            };
                            push_recent_event(
                                &mut analysis.recent_events,
                                event.clone(),
                                RECENT_EVENT_LIMIT,
                            );
                            push_recent_event(
                                &mut analysis.recent_conversation,
                                event,
                                RECENT_CONVERSATION_LIMIT,
                            );
                            analysis.latest_assistant_feedback_at = Some(part.time_created);
                            analysis.last_assistant_message = Some((part.time_created, text));
                        }
                    }
                }
                "reasoning" => {
                    analysis.latest_reasoning_at = Some(part.time_created);
                }
                "step-start" => {
                    open_steps = open_steps.saturating_add(1);
                }
                "step-finish" => {
                    if let Some(reason) = part.data.get("reason").and_then(Value::as_str) {
                        open_steps = open_steps.saturating_sub(1);
                        if reason == "stop" {
                            analysis.finished = true;
                        }
                        push_recent_event(
                            &mut analysis.recent_events,
                            ActivityEvent {
                                timestamp: part.time_created,
                                kind: ActivityKind::System,
                                summary: format!("Step finished ({reason})"),
                            },
                            RECENT_EVENT_LIMIT,
                        );
                    }
                }
                "patch" => {
                    push_recent_event(
                        &mut analysis.recent_events,
                        ActivityEvent {
                            timestamp: part.time_created,
                            kind: ActivityKind::System,
                            summary: "Prepared patch".to_string(),
                        },
                        RECENT_EVENT_LIMIT,
                    );
                }
                "file" => {
                    push_recent_event(
                        &mut analysis.recent_events,
                        ActivityEvent {
                            timestamp: part.time_created,
                            kind: ActivityKind::System,
                            summary: "Generated file output".to_string(),
                        },
                        RECENT_EVENT_LIMIT,
                    );
                }
                _ => {}
            }
        }

        analysis.active_turns = open_steps;
        analysis.run_active = !analysis.finished
            && (analysis.pending_tool_calls > 0
                || analysis.active_turns > 0
                || analysis
                    .latest_reasoning_at
                    .is_some_and(|ts| ts >= now - Duration::seconds(RUNNING_TTL_SECONDS)));

        if total_cost_usd > 0.0 {
            analysis.cost = Some(SessionCost {
                input_usd: 0.0,
                cache_creation_input_usd: 0.0,
                cached_input_usd: 0.0,
                output_usd: 0.0,
                total_usd: total_cost_usd,
                hour_usd: hour_cost_usd,
                day_usd: day_cost_usd,
                pricing_source: PricingSource::BuiltIn,
            });
        }

        if row.parent_id.is_some() {
            analysis.agent_role = Some("subagent".to_string());
        }
        if let Some(compacting_at) = row.time_compacting {
            analysis.latest_compaction_at = Some(compacting_at);
        }

        analysis
    }

    fn session_summary(&self, conn: &Connection, row: &SessionRow) -> Result<SessionSummary> {
        let messages = self.load_messages(conn, &row.id)?;
        let parts = self.load_parts(conn, &row.id)?;
        let analysis = self.analyze_session(row, &messages, &parts);
        let cost = analysis.cost.clone();
        let runtime = derive_runtime_state(row.updated_at, row.archived, &analysis, Utc::now());
        let status = derive_status_from_runtime(row, &analysis, &runtime);
        let activity_state = runtime.activity_state.clone();

        Ok(SessionSummary {
            id: row.id.clone(),
            machine_id: self.machine_id.clone(),
            machine_label: self.machine_label.clone(),
            provider: ProviderKind::Opencode,
            title: row.title.clone(),
            cwd: row.directory.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            run_started_at: analysis
                .last_user_message
                .as_ref()
                .map(|(timestamp, _)| *timestamp),
            run_active: runtime.run_active,
            archived: row.archived,
            model: analysis.model,
            agent_role: analysis.agent_role,
            git_branch: None,
            git_origin_url: None,
            tokens: analysis.tokens,
            cost,
            context_window: None,
            status,
            activity_state,
            rollout_path: Some(
                session_storage_path(&self.opencode_home, &row.project_id, &row.id)
                    .display()
                    .to_string(),
            ),
            navigation: navigation_targets(
                &self.opencode_home,
                &row.project_id,
                &row.id,
                &row.directory,
            ),
        })
    }
}

#[async_trait]
impl SessionSource for OpenCodeSource {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        self.list_sessions_with_progress(query, None).await
    }

    async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        let conn = self.open_db()?;
        let rows = self.load_session_rows(&conn, query.include_archived)?;
        let total = rows.len();
        let limited_rows = if let Some(limit) = query.limit {
            rows.into_iter().take(limit).collect::<Vec<_>>()
        } else {
            rows
        };

        let mut sessions = Vec::with_capacity(limited_rows.len());
        for (index, row) in limited_rows.iter().enumerate() {
            sessions.push(self.session_summary(&conn, row)?);
            if let Some(progress) = &progress {
                let _ = progress.send(SessionLoadProgress {
                    loaded_sessions: index + 1,
                    total_sessions: total,
                    sources: vec![agent_cow_core::SessionLoadSourceProgress {
                        source: "opencode".to_string(),
                        loaded_sessions: index + 1,
                        total_sessions: total,
                    }],
                });
            }
        }

        let overview = UsageOverview {
            total_sessions: total,
            total_tokens: sessions
                .iter()
                .map(|session| session.tokens.total_tokens)
                .sum(),
            total_cost_usd: sessions
                .iter()
                .map(|session| {
                    session
                        .cost
                        .as_ref()
                        .map(|cost| cost.total_usd)
                        .unwrap_or(0.0)
                })
                .sum(),
            sessions_with_cost: sessions
                .iter()
                .filter(|session| session.cost.is_some())
                .count(),
            sessions_with_context: 0,
            quotas: Vec::new(),
            machines: Vec::new(),
        };

        Ok(SessionList {
            generated_at: Utc::now(),
            overview,
            sessions,
        })
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let conn = self.open_db()?;
        let row = self
            .load_session_rows(&conn, true)?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| anyhow!("no OpenCode session was found for id `{id}`"))?;
        let messages = self.load_messages(&conn, &row.id)?;
        let parts = self.load_parts(&conn, &row.id)?;
        let analysis = self.analyze_session(&row, &messages, &parts);
        let summary = self.session_summary(&conn, &row)?;

        Ok(SessionDetail {
            summary,
            recent_events: analysis.recent_events,
            recent_conversation: analysis.recent_conversation,
            tool_stats: analysis.tool_stats.into_values().collect(),
            last_user_message: analysis.last_user_message.map(|(_, text)| text),
            last_assistant_message: analysis.last_assistant_message.map(|(_, text)| text),
            active_turns: analysis.active_turns,
            pending_tool_calls: analysis.pending_tool_calls,
        })
    }
}

fn derive_runtime_state(
    updated_at: DateTime<Utc>,
    archived: bool,
    analysis: &SessionAnalysis,
    now: DateTime<Utc>,
) -> SessionRuntimeState {
    let evidence = SessionRuntimeEvidence {
        completed: archived || analysis.finished,
        failed: false,
        waiting_input: false,
        open_turns: analysis.active_turns,
        open_tool_calls: analysis.pending_tool_calls,
        open_exploration_tool_calls: analysis.pending_exploration_tool_calls,
        thinking_signal_at: analysis.latest_reasoning_at,
        compaction_signal_at: analysis.latest_compaction_at,
        latest_exploration_signal_at: analysis
            .latest_tool
            .as_ref()
            .and_then(|(timestamp, tool)| is_exploration_tool(tool).then_some(*timestamp)),
        latest_tool_call_at: analysis
            .latest_tool
            .as_ref()
            .map(|(timestamp, _)| *timestamp),
    };
    derive_session_runtime_state(
        &evidence,
        updated_at,
        now,
        SessionRuntimeWindows {
            stale_after_minutes: STALE_AFTER_MINUTES,
            activity_window_seconds: ACTIVITY_WINDOW_SECONDS,
            tool_busy_activity_window_seconds: TOOL_BUSY_ACTIVITY_WINDOW_SECONDS,
            compaction_window_seconds: COMPACTION_ACTIVITY_WINDOW_SECONDS,
        },
    )
}

#[cfg(test)]
fn derive_status(row: &SessionRow, analysis: &SessionAnalysis) -> SessionStatus {
    let runtime = derive_runtime_state(row.updated_at, row.archived, analysis, Utc::now());
    derive_status_from_runtime(row, analysis, &runtime)
}

fn derive_status_from_runtime(
    _row: &SessionRow,
    _analysis: &SessionAnalysis,
    runtime: &SessionRuntimeState,
) -> SessionStatus {
    if matches!(runtime.status_kind, SessionStatusKind::ToolBusy) {
        return SessionStatus {
            kind: SessionStatusKind::ToolBusy,
            confidence: StatusConfidence::Exact,
            reason: "An OpenCode tool call is still running".to_string(),
        };
    }

    if matches!(runtime.status_kind, SessionStatusKind::Running) {
        return SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: if runtime.activity_state == SessionActivityState::Compacting {
                "OpenCode is compacting the context window".to_string()
            } else if runtime.activity_state == SessionActivityState::Thinking {
                "OpenCode still has in-flight reasoning".to_string()
            } else {
                "An OpenCode assistant turn is still active".to_string()
            },
        };
    }

    if matches!(runtime.status_kind, SessionStatusKind::Completed) {
        return SessionStatus {
            kind: SessionStatusKind::Completed,
            confidence: StatusConfidence::Inferred,
            reason: "The last OpenCode step finished cleanly".to_string(),
        };
    }

    if matches!(runtime.status_kind, SessionStatusKind::Idle) {
        return SessionStatus {
            kind: SessionStatusKind::Idle,
            confidence: StatusConfidence::Inferred,
            reason: "The session has not emitted activity recently".to_string(),
        };
    }

    if matches!(runtime.status_kind, SessionStatusKind::Stale) {
        SessionStatus {
            kind: SessionStatusKind::Stale,
            confidence: StatusConfidence::Inferred,
            reason: "The session has been inactive for a while".to_string(),
        }
    } else {
        SessionStatus {
            kind: runtime.status_kind.clone(),
            confidence: StatusConfidence::Inferred,
            reason: "The OpenCode runtime could not be derived cleanly".to_string(),
        }
    }
}

#[cfg(test)]
fn derive_activity_state(analysis: &SessionAnalysis) -> SessionActivityState {
    derive_runtime_state(Utc::now(), false, analysis, Utc::now()).activity_state
}

fn navigation_targets(
    opencode_home: &Path,
    project_id: &str,
    session_id: &str,
    cwd: &str,
) -> Vec<NavigationTarget> {
    let mut targets = Vec::new();
    if !cwd.is_empty() {
        targets.push(NavigationTarget {
            kind: NavigationKind::WorkingDirectory,
            label: "Working Directory".to_string(),
            target: cwd.to_string(),
        });
    }

    let session_path = session_storage_path(opencode_home, project_id, session_id);
    if session_path.exists() {
        targets.push(NavigationTarget {
            kind: NavigationKind::RolloutPath,
            label: "Session File".to_string(),
            target: session_path.display().to_string(),
        });
    }

    targets
}

fn parse_opencode_tokens(value: Option<&Value>) -> Option<TokenUsage> {
    let value = value?;
    let input_tokens = value.get("input").and_then(Value::as_u64).unwrap_or(0);
    let output_tokens = value.get("output").and_then(Value::as_u64).unwrap_or(0);
    let reasoning_output_tokens = value.get("reasoning").and_then(Value::as_u64).unwrap_or(0);
    let cache = value.get("cache");
    let cache_creation_input_tokens = cache
        .and_then(|cache| cache.get("write"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached_input_tokens = cache
        .and_then(|cache| cache.get("read"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let total_tokens = input_tokens
        .saturating_add(cache_creation_input_tokens)
        .saturating_add(cached_input_tokens)
        .saturating_add(output_tokens);
    if total_tokens == 0 {
        return None;
    }

    Some(TokenUsage {
        total_tokens,
        input_tokens: Some(input_tokens),
        cache_creation_input_tokens: Some(cache_creation_input_tokens),
        cached_input_tokens: Some(cached_input_tokens),
        output_tokens: Some(output_tokens),
        reasoning_output_tokens: Some(reasoning_output_tokens),
    })
}

fn accumulate_token_usage(total: &mut TokenUsage, delta: &TokenUsage) {
    total.input_tokens = Some(
        total
            .input_tokens
            .unwrap_or(0)
            .saturating_add(delta.input_tokens.unwrap_or(0)),
    );
    total.cache_creation_input_tokens = Some(
        total
            .cache_creation_input_tokens
            .unwrap_or(0)
            .saturating_add(delta.cache_creation_input_tokens.unwrap_or(0)),
    );
    total.cached_input_tokens = Some(
        total
            .cached_input_tokens
            .unwrap_or(0)
            .saturating_add(delta.cached_input_tokens.unwrap_or(0)),
    );
    total.output_tokens = Some(
        total
            .output_tokens
            .unwrap_or(0)
            .saturating_add(delta.output_tokens.unwrap_or(0)),
    );
    total.reasoning_output_tokens = Some(
        total
            .reasoning_output_tokens
            .unwrap_or(0)
            .saturating_add(delta.reasoning_output_tokens.unwrap_or(0)),
    );
    total.total_tokens = total
        .input_tokens
        .unwrap_or(0)
        .saturating_add(total.cache_creation_input_tokens.unwrap_or(0))
        .saturating_add(total.cached_input_tokens.unwrap_or(0))
        .saturating_add(total.output_tokens.unwrap_or(0));
}

fn resolve_pricing(provider_id: Option<&str>, model: Option<&str>) -> Option<OpenCodePricing> {
    let provider_id = provider_id?.to_ascii_lowercase();
    let model = model?.to_ascii_lowercase();

    if provider_id == "anthropic" {
        if model.contains("opus-4-6") {
            return Some(OpenCodePricing {
                input_per_million: 5.0,
                cache_creation_input_per_million: 6.25,
                cached_input_per_million: 0.5,
                output_per_million: 25.0,
            });
        }
        if model.contains("opus") {
            return Some(OpenCodePricing {
                input_per_million: 15.0,
                cache_creation_input_per_million: 18.75,
                cached_input_per_million: 1.5,
                output_per_million: 75.0,
            });
        }
        if model.contains("sonnet") {
            return Some(OpenCodePricing {
                input_per_million: 3.0,
                cache_creation_input_per_million: 3.75,
                cached_input_per_million: 0.3,
                output_per_million: 15.0,
            });
        }
        if model.contains("haiku") {
            return Some(OpenCodePricing {
                input_per_million: 1.0,
                cache_creation_input_per_million: 1.25,
                cached_input_per_million: 0.1,
                output_per_million: 5.0,
            });
        }
    }

    if provider_id == "openai" {
        if model == "gpt-5.4" {
            return Some(OpenCodePricing {
                input_per_million: 2.5,
                cache_creation_input_per_million: 2.5,
                cached_input_per_million: 0.25,
                output_per_million: 15.0,
            });
        }
        if model == "gpt-5.4-mini" {
            return Some(OpenCodePricing {
                input_per_million: 0.75,
                cache_creation_input_per_million: 0.75,
                cached_input_per_million: 0.075,
                output_per_million: 4.5,
            });
        }
        if model.contains("codex") {
            return Some(OpenCodePricing {
                input_per_million: 1.75,
                cache_creation_input_per_million: 1.75,
                cached_input_per_million: 0.175,
                output_per_million: 14.0,
            });
        }
    }

    None
}

fn estimate_token_cost(tokens: &TokenUsage, pricing: &OpenCodePricing) -> f64 {
    token_cost(tokens.input_tokens.unwrap_or(0), pricing.input_per_million)
        + token_cost(
            tokens.cache_creation_input_tokens.unwrap_or(0),
            pricing.cache_creation_input_per_million,
        )
        + token_cost(
            tokens.cached_input_tokens.unwrap_or(0),
            pricing.cached_input_per_million,
        )
        + token_cost(
            tokens.output_tokens.unwrap_or(0),
            pricing.output_per_million,
        )
}

fn token_cost(tokens: u64, price_per_million: f64) -> f64 {
    (tokens as f64 / 1_000_000.0) * price_per_million
}

fn push_recent_event(target: &mut Vec<ActivityEvent>, event: ActivityEvent, limit: usize) {
    target.push(event);
    if target.len() > limit {
        let overflow = target.len() - limit;
        target.drain(0..overflow);
    }
}

fn user_message_text(data: &Value) -> Option<String> {
    let title = data
        .get("summary")
        .and_then(|summary| summary.get("title"))
        .and_then(Value::as_str)
        .map(normalize_single_line_text)
        .filter(|text| !text.is_empty());

    title.or_else(|| {
        data.get("prompt")
            .and_then(Value::as_str)
            .map(normalize_single_line_text)
            .filter(|text| !text.is_empty())
    })
}

fn normalize_single_line_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_exploration_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read" | "grep" | "glob" | "task" | "ls" | "find" | "search"
    )
}

fn session_storage_path(opencode_home: &Path, project_id: &str, session_id: &str) -> PathBuf {
    opencode_home
        .join("storage")
        .join("session")
        .join(project_id)
        .join(format!("{session_id}.json"))
}

fn timestamp_millis_to_utc(value: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(value)
        .single()
        .unwrap_or_else(Utc::now)
}

fn expand_known_path_vars(path: PathBuf) -> PathBuf {
    let raw = path.to_string_lossy();
    if (raw == "~" || raw.starts_with("~/"))
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(raw.trim_start_matches("~/"));
    }
    path
}

fn normalize_opencode_home(path: PathBuf) -> PathBuf {
    let expanded = expand_known_path_vars(path);
    if expanded
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "opencode.db")
    {
        return expanded
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
    }
    expanded
}

fn resolve_opencode_db_path(opencode_home: &Path, configured_path: &Path) -> PathBuf {
    if configured_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "opencode.db")
    {
        return configured_path.to_path_buf();
    }

    if let Some(db) = std::env::var_os("OPENCODE_DB") {
        let db = expand_known_path_vars(PathBuf::from(db));
        if db.is_absolute() {
            return db;
        }
        return opencode_home.join(db);
    }

    opencode_home.join("opencode.db")
}

#[cfg(test)]
mod tests {
    use super::{
        SessionAnalysis, SessionRow, derive_activity_state, derive_status, parse_opencode_tokens,
    };
    use agent_cow_core::{SessionActivityState, SessionStatusKind};
    use chrono::{Duration, Utc};
    use serde_json::json;

    #[test]
    fn parse_opencode_tokens_includes_cache_buckets() {
        let tokens = parse_opencode_tokens(Some(&json!({
            "input": 10,
            "output": 20,
            "reasoning": 5,
            "cache": {
                "read": 30,
                "write": 40
            }
        })))
        .expect("tokens");

        assert_eq!(tokens.input_tokens, Some(10));
        assert_eq!(tokens.cache_creation_input_tokens, Some(40));
        assert_eq!(tokens.cached_input_tokens, Some(30));
        assert_eq!(tokens.output_tokens, Some(20));
        assert_eq!(tokens.reasoning_output_tokens, Some(5));
        assert_eq!(tokens.total_tokens, 100);
    }

    #[test]
    fn stale_non_tool_opencode_runs_become_idle() {
        let now = Utc::now();
        let analysis = SessionAnalysis {
            run_active: true,
            latest_reasoning_at: Some(now - Duration::seconds(45)),
            ..SessionAnalysis::default()
        };

        assert_eq!(derive_activity_state(&analysis), SessionActivityState::Idle);
    }

    #[test]
    fn finished_opencode_sessions_are_idle_not_working() {
        let now = Utc::now();
        let analysis = SessionAnalysis {
            finished: true,
            latest_assistant_feedback_at: Some(now - Duration::seconds(2)),
            run_active: false,
            ..SessionAnalysis::default()
        };

        assert_eq!(derive_activity_state(&analysis), SessionActivityState::Idle);
    }

    #[test]
    fn finished_opencode_sessions_are_completed_not_running() {
        let now = Utc::now();
        let row = SessionRow {
            id: "session".to_string(),
            parent_id: None,
            project_id: "global".to_string(),
            title: "session".to_string(),
            directory: "/tmp".to_string(),
            created_at: now - Duration::minutes(1),
            updated_at: now - Duration::seconds(1),
            time_compacting: None,
            archived: false,
        };
        let analysis = SessionAnalysis {
            finished: true,
            latest_assistant_feedback_at: Some(now - Duration::seconds(1)),
            ..SessionAnalysis::default()
        };

        assert_eq!(
            derive_status(&row, &analysis).kind,
            SessionStatusKind::Completed
        );
    }

    #[test]
    fn opencode_compaction_stays_above_later_generic_feedback() {
        let now = Utc::now();
        let analysis = SessionAnalysis {
            run_active: true,
            latest_compaction_at: Some(now - Duration::seconds(5)),
            latest_assistant_feedback_at: Some(now - Duration::seconds(1)),
            ..SessionAnalysis::default()
        };

        assert_eq!(
            derive_activity_state(&analysis),
            SessionActivityState::Compacting
        );
    }
}
