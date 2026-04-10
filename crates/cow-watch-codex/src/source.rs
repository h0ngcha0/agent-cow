use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cow_watch_core::{
    ActivityEvent, ActivityKind, NavigationKind, NavigationTarget, ProviderKind, SessionDetail,
    SessionQuery, SessionSource, SessionStatus, SessionStatusKind, SessionSummary,
    StatusConfidence, TokenUsage, ToolCallStat,
};
use directories::BaseDirs;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

const SUMMARY_TAIL_LINES: usize = 96;
const DETAIL_TAIL_LINES: usize = 320;
const RECENT_EVENT_LIMIT: usize = 48;
const RUNNING_TTL_SECONDS: i64 = 90;
const STALE_AFTER_MINUTES: i64 = 20;

#[derive(Clone, Debug)]
pub struct CodexSource {
    configured_path: PathBuf,
    codex_home: PathBuf,
    machine_id: String,
    machine_label: String,
    static_cache: Arc<Mutex<HashMap<PathBuf, TranscriptStatic>>>,
}

#[derive(Clone, Debug)]
struct ThreadRow {
    id: String,
    rollout_path: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    cwd: String,
    title: String,
    tokens_used: u64,
    archived: bool,
    git_branch: Option<String>,
    git_origin_url: Option<String>,
    agent_role: Option<String>,
    model: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct TranscriptStatic {
    id: String,
    created_at: Option<DateTime<Utc>>,
    cwd: String,
    title: String,
    git_branch: Option<String>,
    git_origin_url: Option<String>,
    model: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct RolloutHint {
    active_turns: usize,
    pending_call_ids: HashSet<String>,
    pending_call_names: HashMap<String, String>,
    last_token_usage: Option<TokenUsage>,
    last_user_message: Option<(DateTime<Utc>, String)>,
    last_assistant_message: Option<(DateTime<Utc>, String)>,
    recent_events: Vec<ActivityEvent>,
    tool_stats: HashMap<String, ToolCallStat>,
}

impl CodexSource {
    pub fn new(codex_home: impl Into<PathBuf>) -> Self {
        let machine_label = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "local".to_string());
        let configured_path = expand_known_path_vars(codex_home.into());
        let codex_home = normalize_codex_home(configured_path.clone());

        Self {
            configured_path,
            codex_home,
            machine_id: machine_label.clone(),
            machine_label,
            static_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_default_home() -> Result<Self> {
        let base_dirs =
            BaseDirs::new().ok_or_else(|| anyhow!("could not resolve a home directory"))?;
        Ok(Self::new(base_dirs.home_dir().join(".codex")))
    }

    fn open_state_db(&self) -> Result<Connection> {
        let mut attempts = Vec::new();
        let mut seen = HashSet::new();

        for input in self.candidate_inputs() {
            match state_db_candidates_for_input(&input) {
                Ok(paths) if paths.is_empty() => {
                    attempts.push(format!(
                        "{} (no state_*.sqlite database found)",
                        input.display()
                    ));
                }
                Ok(paths) => {
                    for path in paths {
                        if !seen.insert(path.clone()) {
                            continue;
                        }

                        match open_sqlite_read_only(&path) {
                            Ok(connection) => return Ok(connection),
                            Err(error) => {
                                attempts.push(format!("{} ({error})", path.display()));

                                match snapshot_sqlite_database(&path)
                                    .and_then(|snapshot| open_sqlite_read_only(&snapshot))
                                {
                                    Ok(connection) => return Ok(connection),
                                    Err(snapshot_error) => attempts.push(format!(
                                        "{} [snapshot] ({snapshot_error})",
                                        path.display()
                                    )),
                                }
                            }
                        }
                    }
                }
                Err(error) => attempts.push(format!("{} ({error})", input.display())),
            }
        }

        Err(anyhow!(
            "unable to open Codex state database; tried: {}",
            attempts.join(" | ")
        ))
    }

    fn load_threads_from_db(&self) -> Result<Vec<ThreadRow>> {
        let connection = self.open_state_db()?;
        let mut statement = connection.prepare(
            "SELECT id, rollout_path, created_at, updated_at, cwd, title, tokens_used, archived, git_branch, git_origin_url, agent_role, model
             FROM threads
             ORDER BY updated_at DESC",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(ThreadRow {
                id: row.get(0)?,
                rollout_path: row.get(1)?,
                created_at: unix_timestamp_to_utc(row.get::<_, i64>(2)?)?,
                updated_at: unix_timestamp_to_utc(row.get::<_, i64>(3)?)?,
                cwd: row.get(4)?,
                title: row.get(5)?,
                tokens_used: row.get::<_, i64>(6)?.max(0) as u64,
                archived: row.get::<_, i64>(7)? != 0,
                git_branch: row.get(8)?,
                git_origin_url: row.get(9)?,
                agent_role: row.get(10)?,
                model: row.get(11)?,
            })
        })?;

        let collected: rusqlite::Result<Vec<_>> = rows.collect();
        collected.context("failed to collect rows from Codex threads table")
    }

    fn load_threads(&self) -> Result<Vec<ThreadRow>> {
        let mut rows = self.discover_threads_from_rollouts()?;
        let mut sqlite_rows_by_id = self
            .load_threads_from_db()
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.id.clone(), row))
            .collect::<HashMap<_, _>>();
        let mut seen_ids = HashSet::new();

        for row in &mut rows {
            if let Some(sqlite_row) = sqlite_rows_by_id.remove(&row.id) {
                enrich_thread_from_db(row, sqlite_row);
            }
            seen_ids.insert(row.id.clone());
        }

        rows.extend(
            sqlite_rows_by_id
                .into_values()
                .filter(|row| seen_ids.insert(row.id.clone())),
        );
        rows.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        Ok(rows)
    }

    fn thread_by_id(&self, id: &str) -> Result<ThreadRow> {
        self.load_threads()?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| anyhow!("session {id} was not found"))
    }

    fn discover_threads_from_rollouts(&self) -> Result<Vec<ThreadRow>> {
        let mut rows_by_id: HashMap<String, ThreadRow> = HashMap::new();

        for path in self.rollout_paths()? {
            let row = match self.summarize_rollout_file(&path) {
                Ok(row) => row,
                Err(_) => continue,
            };

            match rows_by_id.get(&row.id) {
                Some(existing) if existing.updated_at >= row.updated_at => {}
                _ => {
                    rows_by_id.insert(row.id.clone(), row);
                }
            }
        }

        Ok(rows_by_id.into_values().collect())
    }

    fn summarize_rollout_file(&self, path: &Path) -> Result<ThreadRow> {
        let static_parts = self.transcript_static(path)?;
        let updated_at = file_modified_at(path)
            .or(static_parts.created_at)
            .unwrap_or_else(Utc::now);
        let created_at = static_parts.created_at.unwrap_or(updated_at);
        let id = if static_parts.id.is_empty() {
            fallback_session_id(path)
        } else {
            static_parts.id.clone()
        };
        let title = if static_parts.title.is_empty() {
            fallback_title(&static_parts.cwd, &id)
        } else {
            static_parts.title.clone()
        };

        Ok(ThreadRow {
            id,
            rollout_path: path.to_string_lossy().into_owned(),
            created_at,
            updated_at,
            cwd: static_parts.cwd,
            title,
            tokens_used: 0,
            archived: false,
            git_branch: static_parts.git_branch,
            git_origin_url: static_parts.git_origin_url,
            agent_role: None,
            model: static_parts.model,
        })
    }

    fn transcript_static(&self, path: &Path) -> Result<TranscriptStatic> {
        if let Ok(cache) = self.static_cache.lock()
            && let Some(cached) = cache.get(path)
        {
            return Ok(cached.clone());
        }

        let static_parts = parse_transcript_static(path)?;

        if let Ok(mut cache) = self.static_cache.lock() {
            cache.insert(path.to_path_buf(), static_parts.clone());
        }

        Ok(static_parts)
    }

    fn rollout_paths(&self) -> Result<Vec<PathBuf>> {
        let mut roots = Vec::new();
        let mut seen_roots = HashSet::new();

        for input in self.candidate_inputs() {
            let home = normalize_codex_home(input);
            let root = if home
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "sessions")
            {
                home
            } else {
                home.join("sessions")
            };

            if seen_roots.insert(root.clone()) {
                roots.push(root);
            }
        }

        let mut paths = Vec::new();
        for root in roots {
            collect_rollout_paths(&root, &mut paths)?;
        }

        let mut seen_paths = HashSet::new();
        paths.retain(|path| seen_paths.insert(path.clone()));
        Ok(paths)
    }

    fn candidate_inputs(&self) -> Vec<PathBuf> {
        let mut inputs = Vec::new();
        let mut seen = HashSet::new();

        for input in [
            Some(self.configured_path.clone()),
            Some(self.codex_home.clone()),
            std::env::var_os("COW_WATCH_CODEX_HOME").map(PathBuf::from),
            std::env::var_os("CODEX_HOME").map(PathBuf::from),
            BaseDirs::new().map(|dirs| dirs.home_dir().join(".codex")),
        ]
        .into_iter()
        .flatten()
        .map(expand_known_path_vars)
        {
            if seen.insert(input.clone()) {
                inputs.push(input);
            }
        }

        inputs
    }
}

#[async_trait]
impl SessionSource for CodexSource {
    async fn list_sessions(&self, query: SessionQuery) -> Result<Vec<SessionSummary>> {
        let now = Utc::now();
        let mut sessions = Vec::new();

        for row in self.load_threads()? {
            if !query.include_archived && row.archived {
                continue;
            }

            let hint = if row.archived {
                None
            } else {
                analyze_rollout(&PathBuf::from(&row.rollout_path), ReadMode::Summary).ok()
            };

            sessions.push(build_summary(
                &self.machine_id,
                &self.machine_label,
                &row,
                hint.as_ref(),
                now,
            ));

            if let Some(limit) = query.limit
                && sessions.len() >= limit
            {
                break;
            }
        }

        Ok(sessions)
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let now = Utc::now();
        let row = self.thread_by_id(id)?;
        let hint = analyze_rollout(&PathBuf::from(&row.rollout_path), ReadMode::Detail)?;
        let summary = build_summary(
            &self.machine_id,
            &self.machine_label,
            &row,
            Some(&hint),
            now,
        );

        let mut tool_stats: Vec<_> = hint.tool_stats.into_values().collect();
        tool_stats.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.name.cmp(&right.name))
        });

        Ok(SessionDetail {
            summary,
            recent_events: hint.recent_events,
            tool_stats,
            last_user_message: hint.last_user_message.map(|(_, text)| text),
            last_assistant_message: hint.last_assistant_message.map(|(_, text)| text),
            active_turns: hint.active_turns,
            pending_tool_calls: hint.pending_call_ids.len(),
        })
    }
}

#[derive(Copy, Clone, Debug)]
enum ReadMode {
    Summary,
    Detail,
}

fn build_summary(
    machine_id: &str,
    machine_label: &str,
    row: &ThreadRow,
    hint: Option<&RolloutHint>,
    now: DateTime<Utc>,
) -> SessionSummary {
    let mut tokens = hint
        .and_then(|hint| hint.last_token_usage.clone())
        .unwrap_or_default();
    tokens.total_tokens = tokens.total_tokens.max(row.tokens_used);

    let status = derive_status(row, hint, now);
    let rollout_path = (!row.rollout_path.is_empty()).then_some(row.rollout_path.clone());
    let navigation = build_navigation(row, rollout_path.clone());

    SessionSummary {
        id: row.id.clone(),
        machine_id: machine_id.to_string(),
        machine_label: machine_label.to_string(),
        provider: ProviderKind::Codex,
        title: normalize_title(&row.title),
        cwd: row.cwd.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        archived: row.archived,
        model: row.model.clone(),
        agent_role: row.agent_role.clone(),
        git_branch: row.git_branch.clone(),
        git_origin_url: row.git_origin_url.clone(),
        tokens,
        status,
        rollout_path,
        navigation,
    }
}

fn build_navigation(row: &ThreadRow, rollout_path: Option<String>) -> Vec<NavigationTarget> {
    let mut navigation = vec![NavigationTarget {
        kind: NavigationKind::ThreadId,
        label: "Thread ID".to_string(),
        target: row.id.clone(),
    }];

    if let Some(rollout_path) = rollout_path {
        navigation.push(NavigationTarget {
            kind: NavigationKind::RolloutPath,
            label: "Rollout Log".to_string(),
            target: rollout_path,
        });
    }

    if !row.cwd.is_empty() {
        navigation.push(NavigationTarget {
            kind: NavigationKind::WorkingDirectory,
            label: "Working Directory".to_string(),
            target: row.cwd.clone(),
        });
    }

    navigation
}

fn derive_status(row: &ThreadRow, hint: Option<&RolloutHint>, now: DateTime<Utc>) -> SessionStatus {
    let idle_for = now - row.updated_at;

    if row.archived {
        return SessionStatus {
            kind: SessionStatusKind::Completed,
            confidence: StatusConfidence::Exact,
            reason: "Codex marked the thread as archived".to_string(),
        };
    }

    if let Some(hint) = hint {
        if !hint.pending_call_ids.is_empty() {
            let tool_name = hint
                .pending_call_ids
                .iter()
                .find_map(|call_id| hint.pending_call_names.get(call_id))
                .cloned()
                .unwrap_or_else(|| "tool".to_string());

            return SessionStatus {
                kind: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                    SessionStatusKind::ToolBusy
                } else {
                    SessionStatusKind::Stale
                },
                confidence: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                    StatusConfidence::Exact
                } else {
                    StatusConfidence::Inferred
                },
                reason: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                    format!("Waiting on `{tool_name}` tool output")
                } else {
                    format!(
                        "A `{tool_name}` call is still open in the trace tail, but the session has been quiet for {} minutes",
                        idle_for.num_minutes()
                    )
                },
            };
        }

        if looks_like_waiting_input(
            hint.last_assistant_message.as_ref(),
            hint.last_user_message.as_ref(),
        ) {
            return SessionStatus {
                kind: SessionStatusKind::WaitingInput,
                confidence: StatusConfidence::Inferred,
                reason: "Last assistant message looks like a request for user input".to_string(),
            };
        }

        if hint.active_turns > 0 {
            return SessionStatus {
                kind: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                    SessionStatusKind::Running
                } else {
                    SessionStatusKind::Stale
                },
                confidence: StatusConfidence::Inferred,
                reason: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                    "An active turn is still open".to_string()
                } else {
                    format!(
                        "The trace still shows an open turn, but the session has been quiet for {} minutes",
                        idle_for.num_minutes()
                    )
                },
            };
        }
    }

    if idle_for <= Duration::seconds(RUNNING_TTL_SECONDS) {
        SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: "Recent session activity is still inside the running TTL".to_string(),
        }
    } else if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
        SessionStatus {
            kind: SessionStatusKind::Idle,
            confidence: StatusConfidence::Inferred,
            reason: "No active tool or turn signal was found in the latest trace tail".to_string(),
        }
    } else {
        SessionStatus {
            kind: SessionStatusKind::Stale,
            confidence: StatusConfidence::Inferred,
            reason: "The session has not emitted activity recently".to_string(),
        }
    }
}

fn enrich_thread_from_db(row: &mut ThreadRow, sqlite_row: ThreadRow) {
    if row.rollout_path.is_empty() && !sqlite_row.rollout_path.is_empty() {
        row.rollout_path = sqlite_row.rollout_path;
    }
    if sqlite_row.created_at < row.created_at {
        row.created_at = sqlite_row.created_at;
    }
    if sqlite_row.updated_at > row.updated_at {
        row.updated_at = sqlite_row.updated_at;
    }
    if row.cwd.is_empty() && !sqlite_row.cwd.is_empty() {
        row.cwd = sqlite_row.cwd;
    }
    if !sqlite_row.title.is_empty() {
        row.title = sqlite_row.title;
    }
    if sqlite_row.tokens_used > row.tokens_used {
        row.tokens_used = sqlite_row.tokens_used;
    }
    row.archived |= sqlite_row.archived;
    if row.git_branch.is_none() {
        row.git_branch = sqlite_row.git_branch;
    }
    if row.git_origin_url.is_none() {
        row.git_origin_url = sqlite_row.git_origin_url;
    }
    if row.agent_role.is_none() {
        row.agent_role = sqlite_row.agent_role;
    }
    if row.model.is_none() {
        row.model = sqlite_row.model;
    }
}

fn analyze_rollout(path: &Path, mode: ReadMode) -> Result<RolloutHint> {
    let lines = read_rollout_lines(path, mode)?;
    let mut hint = RolloutHint::default();
    let mut recent_events = VecDeque::with_capacity(RECENT_EVENT_LIMIT);

    for line in lines {
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339);
        let root_kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);

        match root_kind {
            "event_msg" => {
                let event_kind = payload
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match event_kind {
                    "task_started" => {
                        hint.active_turns += 1;
                        if let Some(timestamp) = timestamp {
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::System,
                                    summary: "Task started".to_string(),
                                },
                            );
                        }
                    }
                    "task_complete" => {
                        hint.active_turns = hint.active_turns.saturating_sub(1);
                        if let Some(timestamp) = timestamp {
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::System,
                                    summary: "Task completed".to_string(),
                                },
                            );
                        }
                    }
                    "token_count" => {
                        if let Some(token_usage) = payload
                            .get("info")
                            .and_then(|info| info.get("total_token_usage"))
                            .and_then(parse_token_usage)
                        {
                            hint.last_token_usage = Some(token_usage);
                        }
                    }
                    "agent_message" => {
                        if let (Some(timestamp), Some(message)) =
                            (timestamp, payload.get("message").and_then(Value::as_str))
                        {
                            let message = compact_text(message);
                            hint.last_assistant_message = Some((timestamp, message.clone()));
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::Assistant,
                                    summary: message,
                                },
                            );
                        }
                    }
                    "user_message" => {
                        if let (Some(timestamp), Some(message)) =
                            (timestamp, payload.get("message").and_then(Value::as_str))
                        {
                            let message = compact_text(message);
                            hint.last_user_message = Some((timestamp, message.clone()));
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::User,
                                    summary: message,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
            "response_item" => {
                let response_kind = payload
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match response_kind {
                    "function_call" => {
                        let call_id = payload
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let name = payload
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("tool")
                            .to_string();

                        if !call_id.is_empty() {
                            hint.pending_call_ids.insert(call_id.clone());
                            hint.pending_call_names.insert(call_id, name.clone());
                        }

                        if let Some(timestamp) = timestamp {
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::ToolCall,
                                    summary: format!("Called `{name}`"),
                                },
                            );
                        }

                        let entry = hint.tool_stats.entry(name.clone()).or_insert(ToolCallStat {
                            name,
                            count: 0,
                            last_seen: None,
                        });
                        entry.count += 1;
                        entry.last_seen = timestamp;
                    }
                    "function_call_output" => {
                        if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
                            let tool_name = hint.pending_call_names.get(call_id).cloned();
                            hint.pending_call_ids.remove(call_id);

                            if let Some(timestamp) = timestamp {
                                push_recent_event(
                                    &mut recent_events,
                                    ActivityEvent {
                                        timestamp,
                                        kind: ActivityKind::ToolResult,
                                        summary: match tool_name {
                                            Some(tool_name) => format!("Finished `{tool_name}`"),
                                            None => "Tool call finished".to_string(),
                                        },
                                    },
                                );
                            }
                        }
                    }
                    "message" => {
                        if let (Some(timestamp), Some(message)) =
                            (timestamp, extract_message_text(payload))
                        {
                            let message = compact_text(&message);
                            hint.last_assistant_message = Some((timestamp, message.clone()));
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::Assistant,
                                    summary: message,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    hint.recent_events = recent_events.into_iter().collect();
    Ok(hint)
}

fn parse_transcript_static(path: &Path) -> Result<TranscriptStatic> {
    let mut static_parts = TranscriptStatic {
        id: fallback_session_id(path),
        ..TranscriptStatic::default()
    };

    for line in read_first_lines(path, 64)? {
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let root_kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);

        match root_kind {
            "session_meta" => {
                if let Some(id) = payload.get("id").and_then(Value::as_str) {
                    static_parts.id = id.to_string();
                }
                if static_parts.created_at.is_none() {
                    static_parts.created_at = payload
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .and_then(parse_rfc3339)
                        .or_else(|| {
                            value
                                .get("timestamp")
                                .and_then(Value::as_str)
                                .and_then(parse_rfc3339)
                        });
                }
                if static_parts.cwd.is_empty() {
                    static_parts.cwd = payload
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                if static_parts.git_branch.is_none() {
                    static_parts.git_branch = payload
                        .get("git")
                        .and_then(|git| git.get("branch"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
                if static_parts.git_origin_url.is_none() {
                    static_parts.git_origin_url = payload
                        .get("git")
                        .and_then(|git| git.get("repository_url"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
            }
            "turn_context" => {
                if static_parts.cwd.is_empty() {
                    static_parts.cwd = payload
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                if static_parts.model.is_none() {
                    static_parts.model = payload
                        .get("model")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
            }
            "event_msg" => {
                if static_parts.title.is_empty()
                    && payload
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| kind == "user_message")
                    && let Some(message) = payload.get("message").and_then(Value::as_str)
                    && let Some(title) = user_title_candidate(message)
                {
                    static_parts.title = title;
                }
            }
            "response_item" => {
                if static_parts.title.is_empty()
                    && payload
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| kind == "message")
                    && payload
                        .get("role")
                        .and_then(Value::as_str)
                        .is_some_and(|role| role == "user")
                    && let Some(message) = extract_message_text(payload)
                    && let Some(title) = user_title_candidate(&message)
                {
                    static_parts.title = title;
                }
            }
            _ => {}
        }

        if !static_parts.id.is_empty()
            && static_parts.created_at.is_some()
            && !static_parts.cwd.is_empty()
            && !static_parts.title.is_empty()
            && static_parts.model.is_some()
            && static_parts.git_branch.is_some()
            && static_parts.git_origin_url.is_some()
        {
            break;
        }
    }

    Ok(static_parts)
}

fn user_title_candidate(message: &str) -> Option<String> {
    let trimmed = message.trim();
    if trimmed.is_empty() || trimmed.starts_with("<environment_context>") {
        return None;
    }

    let relevant =
        if trimmed.contains("Generate a concise UI title") && trimmed.contains("User prompt:") {
            trimmed
                .split_once("User prompt:")
                .map(|(_, prompt)| prompt.trim())
                .unwrap_or(trimmed)
        } else {
            trimmed
        };

    let title = normalize_title(relevant);
    if title.is_empty() { None } else { Some(title) }
}

fn fallback_session_id(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    if stem.len() >= 36 {
        return stem[stem.len() - 36..].to_string();
    }
    stem.to_string()
}

fn fallback_title(cwd: &str, id: &str) -> String {
    if !cwd.is_empty() {
        if let Some(name) = Path::new(cwd).file_name().and_then(|name| name.to_str())
            && !name.is_empty()
        {
            return format!("Session: {name}");
        }
        return format!("Session: {}", compact_text(cwd));
    }

    if id.is_empty() {
        "Session".to_string()
    } else {
        format!("Session {id}")
    }
}

fn parse_token_usage(value: &Value) -> Option<TokenUsage> {
    Some(TokenUsage {
        total_tokens: value.get("total_tokens")?.as_u64()?,
        input_tokens: value.get("input_tokens").and_then(Value::as_u64),
        cached_input_tokens: value.get("cached_input_tokens").and_then(Value::as_u64),
        output_tokens: value.get("output_tokens").and_then(Value::as_u64),
        reasoning_output_tokens: value.get("reasoning_output_tokens").and_then(Value::as_u64),
    })
}

fn push_recent_event(events: &mut VecDeque<ActivityEvent>, event: ActivityEvent) {
    if events.len() == RECENT_EVENT_LIMIT {
        events.pop_front();
    }
    events.push_back(event);
}

fn looks_like_waiting_input(
    assistant_message: Option<&(DateTime<Utc>, String)>,
    user_message: Option<&(DateTime<Utc>, String)>,
) -> bool {
    let Some((assistant_timestamp, assistant_message)) = assistant_message else {
        return false;
    };

    if let Some((user_timestamp, _)) = user_message
        && assistant_timestamp <= user_timestamp
    {
        return false;
    }

    let lowercase = assistant_message.to_lowercase();
    assistant_message.trim_end().ends_with('?')
        || lowercase.contains("need your input")
        || lowercase.contains("please provide")
        || lowercase.contains("how would you like")
        || lowercase.contains("which option")
        || lowercase.contains("what should")
}

fn extract_message_text(payload: &Value) -> Option<String> {
    if let Some(message) = payload.get("message").and_then(Value::as_str) {
        return Some(message.to_string());
    }

    let content = payload.get("content")?.as_array()?;
    let mut chunks = Vec::new();

    for item in content {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            chunks.push(text.trim());
        } else if let Some(text) = item
            .get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.first())
            .and_then(|item| item.get("text"))
            .and_then(Value::as_str)
        {
            chunks.push(text.trim());
        }
    }

    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

fn compact_text(text: &str) -> String {
    let text = text
        .split_whitespace()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    truncate_chars(&text, 140)
}

fn normalize_title(text: &str) -> String {
    let normalized = normalize_inline_text(text);
    compact_automation_title(&normalized).unwrap_or(normalized)
}

fn compact_automation_title(text: &str) -> Option<String> {
    let body = text.strip_prefix("Automation:")?.trim();

    if let Some(id) = automation_id(body) {
        let label = pascal_case_label(id);
        if !label.is_empty() {
            return Some(format!("Automation: {label}"));
        }
    }

    let name = body
        .split_once("Automation ID:")
        .map(|(name, _)| name.trim())
        .unwrap_or(body);
    let label = pascal_case_label(name);

    if label.is_empty() {
        Some("Automation".to_string())
    } else {
        Some(format!("Automation: {label}"))
    }
}

fn normalize_inline_text(text: &str) -> String {
    collapse_markdown_links(text)
        .split_whitespace()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn collapse_markdown_links(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find('[') {
        output.push_str(&rest[..start]);

        let Some(label_end) = rest[start + 1..].find(']') else {
            output.push_str(&rest[start..]);
            return output;
        };
        let label_end = start + 1 + label_end;
        let label = &rest[start + 1..label_end];

        let Some(after_label) = rest[label_end + 1..].strip_prefix('(') else {
            output.push_str(&rest[start..=label_end]);
            rest = &rest[label_end + 1..];
            continue;
        };

        let Some(url_end) = after_label.find(')') else {
            output.push_str(&rest[start..]);
            return output;
        };

        output.push_str(label);
        rest = &after_label[url_end + 1..];
    }

    output.push_str(rest);
    output
}

fn automation_id(text: &str) -> Option<&str> {
    text.split_once("Automation ID:")
        .map(|(_, tail)| tail.trim())
        .and_then(|tail| tail.split_whitespace().next())
}

fn pascal_case_label(text: &str) -> String {
    let mut normalized = String::new();

    for part in text.split(|ch: char| !ch.is_alphanumeric()) {
        if part.is_empty() {
            continue;
        }

        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            normalized.extend(first.to_uppercase());
        }
        for ch in chars {
            normalized.extend(ch.to_lowercase());
        }
    }

    normalized
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

fn normalize_codex_home(path: PathBuf) -> PathBuf {
    let expanded = expand_known_path_vars(path);
    if expanded
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sqlite"))
    {
        return expanded.parent().map(Path::to_path_buf).unwrap_or(expanded);
    }

    expanded
}

fn state_db_candidates_for_input(input: &Path) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }

    if !input.exists() {
        return Ok(Vec::new());
    }

    let entries = fs::read_dir(input)
        .with_context(|| format!("failed to read Codex home {}", input.display()))?;

    let mut candidates: Vec<(u32, PathBuf)> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_type = entry.file_type().ok()?;
            if !file_type.is_file() {
                return None;
            }

            let file_name = entry.file_name();
            let file_name = file_name.to_str()?;
            let version = parse_state_db_version(file_name)?;
            Some((version, entry.path()))
        })
        .collect();

    candidates.sort_by(|left, right| right.0.cmp(&left.0));

    let preferred = input.join("state_5.sqlite");
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();

    if preferred.is_file() && seen.insert(preferred.clone()) {
        ordered.push(preferred);
    }

    for (_, path) in candidates {
        if seen.insert(path.clone()) {
            ordered.push(path);
        }
    }

    Ok(ordered)
}

fn open_sqlite_read_only(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open {}", path.display()))
}

fn snapshot_sqlite_database(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("invalid SQLite path {}", path.display()))?;
    let snapshot_root = std::env::temp_dir().join("cow-watch-sqlite");
    fs::create_dir_all(&snapshot_root)
        .with_context(|| format!("failed to create {}", snapshot_root.display()))?;

    let unique = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let snapshot_dir = snapshot_root.join(unique);
    fs::create_dir_all(&snapshot_dir)
        .with_context(|| format!("failed to create {}", snapshot_dir.display()))?;

    let snapshot_db = snapshot_dir.join(file_name);
    fs::copy(path, &snapshot_db).with_context(|| {
        format!(
            "failed to copy {} to {}",
            path.display(),
            snapshot_db.display()
        )
    })?;

    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));
        if sidecar.is_file() {
            let snapshot_sidecar = snapshot_dir.join(
                sidecar
                    .file_name()
                    .ok_or_else(|| anyhow!("invalid sidecar path {}", sidecar.display()))?,
            );
            fs::copy(&sidecar, &snapshot_sidecar).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    sidecar.display(),
                    snapshot_sidecar.display()
                )
            })?;
        }
    }

    Ok(snapshot_db)
}

fn expand_known_path_vars(path: PathBuf) -> PathBuf {
    let mut value = path.to_string_lossy().into_owned();
    let home_dir = BaseDirs::new().map(|dirs| dirs.home_dir().to_string_lossy().into_owned());

    if value == "~" {
        if let Some(home_dir) = home_dir {
            return PathBuf::from(home_dir);
        }
        return PathBuf::from(value);
    }

    if value.starts_with("~/")
        && let Some(home_dir) = home_dir.as_ref()
    {
        value = format!("{home_dir}/{}", &value[2..]);
    }

    if let Some(home_dir) = home_dir.as_ref() {
        value = value.replace("${HOME}", home_dir);
        value = value.replace("$HOME", home_dir);
    }

    if let Ok(codex_home) = std::env::var("CODEX_HOME") {
        value = value.replace("${CODEX_HOME}", &codex_home);
        value = value.replace("$CODEX_HOME", &codex_home);
    }

    PathBuf::from(value)
}

fn parse_state_db_version(file_name: &str) -> Option<u32> {
    file_name
        .strip_prefix("state_")?
        .strip_suffix(".sqlite")?
        .parse()
        .ok()
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn unix_timestamp_to_utc(timestamp: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::from_timestamp(timestamp, 0).ok_or(rusqlite::Error::IntegralValueOutOfRange(0, 0))
}

fn read_rollout_lines(path: &Path, mode: ReadMode) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let metadata = path
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let file_size = metadata.len();

    let line_limit = match mode {
        ReadMode::Summary => SUMMARY_TAIL_LINES,
        ReadMode::Detail => DETAIL_TAIL_LINES,
    };

    if file_size <= 2_000_000 {
        let mut text = String::new();
        File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?
            .read_to_string(&mut text)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return Ok(text.lines().map(ToOwned::to_owned).collect());
    }

    tail_lines(path, line_limit)
}

fn read_first_lines(path: &Path, max_lines: usize) -> Result<Vec<String>> {
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?,
    );
    let mut lines = Vec::new();

    for line in reader.lines().take(max_lines) {
        let line = line.with_context(|| format!("failed to read {}", path.display()))?;
        if !line.trim().is_empty() {
            lines.push(line);
        }
    }

    Ok(lines)
}

fn tail_lines(path: &Path, line_limit: usize) -> Result<Vec<String>> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut position = file.seek(SeekFrom::End(0))?;
    let mut buffer = Vec::new();
    let mut newline_count = 0;

    while position > 0 && newline_count <= line_limit {
        let chunk_size = usize::min(8192, position as usize);
        position -= chunk_size as u64;
        file.seek(SeekFrom::Start(position))?;

        let mut chunk = vec![0_u8; chunk_size];
        file.read_exact(&mut chunk)?;
        newline_count += chunk.iter().filter(|byte| **byte == b'\n').count();
        buffer.splice(0..0, chunk);
    }

    let text = String::from_utf8_lossy(&buffer).into_owned();
    let mut lines: Vec<String> = text.lines().map(ToOwned::to_owned).collect();
    if lines.len() > line_limit {
        lines = lines.split_off(lines.len() - line_limit);
    }
    Ok(lines)
}

fn collect_rollout_paths(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    let entries =
        fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))?;

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        let path = entry.path();

        if file_type.is_dir() {
            collect_rollout_paths(&path, output)?;
            continue;
        }

        if file_type.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
        {
            output.push(path);
        }
    }

    Ok(())
}

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    path.metadata()
        .ok()?
        .modified()
        .ok()
        .map(DateTime::<Utc>::from)
}

#[cfg(test)]
mod tests {
    use super::{
        looks_like_waiting_input, normalize_codex_home, normalize_title, parse_state_db_version,
        parse_transcript_static, state_db_candidates_for_input, user_title_candidate,
    };
    use chrono::Utc;
    use directories::BaseDirs;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn waiting_input_needs_newer_assistant_message() {
        let now = Utc::now();
        let assistant = (now, "How would you like to proceed?".to_string());
        let user = (now + chrono::Duration::seconds(1), "do it".to_string());

        assert!(!looks_like_waiting_input(Some(&assistant), Some(&user)));
    }

    #[test]
    fn waiting_input_detects_question_prompt() {
        let now = Utc::now();
        let assistant = (now, "Which repo should I inspect first?".to_string());

        assert!(looks_like_waiting_input(Some(&assistant), None));
    }

    #[test]
    fn normalize_title_compacts_automation_id() {
        let title = "Automation: Sync Linear Automation ID: sync-linear Automation memory: $CODEX_HOME/automations/sync-linear/memory.md Last run: never";

        assert_eq!(normalize_title(title), "Automation: SyncLinear");
    }

    #[test]
    fn normalize_title_keeps_non_automation_titles() {
        let title = "Investigate Docker image startup failure for backend service";

        assert_eq!(normalize_title(title), title);
    }

    #[test]
    fn normalize_title_collapses_markdown_skill_links() {
        let title =
            "[$superpowers](/Users/h0ngcha0/.codex/skills/superpowers/SKILL.md) Build the monitor";

        assert_eq!(normalize_title(title), "$superpowers Build the monitor");
    }

    #[test]
    fn normalize_codex_home_expands_tilde() {
        let Some(base_dirs) = BaseDirs::new() else {
            return;
        };

        assert_eq!(
            normalize_codex_home(PathBuf::from("~/.codex")),
            base_dirs.home_dir().join(".codex")
        );
    }

    #[test]
    fn normalize_codex_home_accepts_db_file_path() {
        let input = PathBuf::from("/tmp/state_9.sqlite");

        assert_eq!(normalize_codex_home(input), PathBuf::from("/tmp"));
    }

    #[test]
    fn state_db_version_parser_only_accepts_state_sqlite_names() {
        assert_eq!(parse_state_db_version("state_5.sqlite"), Some(5));
        assert_eq!(parse_state_db_version("state_12.sqlite"), Some(12));
        assert_eq!(parse_state_db_version("state.sqlite"), None);
        assert_eq!(parse_state_db_version("notes.txt"), None);
    }

    #[test]
    fn state_db_candidates_accept_direct_file_path() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-test-file-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("state_9.sqlite");
        fs::write(&path, b"").unwrap();

        assert_eq!(state_db_candidates_for_input(&path).unwrap(), vec![path]);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn state_db_candidates_prefer_state_5_then_highest_version() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-test-{unique}"));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("state_3.sqlite"), b"").unwrap();
        fs::write(root.join("state_5.sqlite"), b"").unwrap();
        fs::write(root.join("state_7.sqlite"), b"").unwrap();

        let candidates = state_db_candidates_for_input(&root).unwrap();
        assert_eq!(
            candidates,
            vec![
                root.join("state_5.sqlite"),
                root.join("state_7.sqlite"),
                root.join("state_3.sqlite"),
            ]
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn user_title_candidate_extracts_prompt_from_title_generation_session() {
        let prompt = "You are a helpful assistant.\nGenerate a concise UI title.\n\nUser prompt:\nfix the startup error";

        assert_eq!(
            user_title_candidate(prompt).as_deref(),
            Some("fix the startup error")
        );
    }

    #[test]
    fn parse_transcript_static_reads_rollout_metadata() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-transcript-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path =
            root.join("rollout-2026-04-10T16-00-35-019d77b1-d1ca-7c90-82fa-927613e48567.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-04-10T14:00:35.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"019d77b1-d1ca-7c90-82fa-927613e48567\",\"timestamp\":\"2026-04-10T14:00:35.000Z\",\"cwd\":\"/tmp/project\",\"git\":{\"branch\":\"main\",\"repository_url\":\"git@example.com:repo.git\"}}}\n",
                "{\"timestamp\":\"2026-04-10T14:00:36.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"<environment_context>\\n  <cwd>/tmp/project</cwd>\\n</environment_context>\"}]}}\n",
                "{\"timestamp\":\"2026-04-10T14:00:37.000Z\",\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/tmp/project\",\"model\":\"gpt-5.4\"}}\n",
                "{\"timestamp\":\"2026-04-10T14:00:38.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"[$superpowers](/tmp/SKILL.md) Build the monitor\"}}\n"
            ),
        )
        .unwrap();

        let parsed = parse_transcript_static(&path).unwrap();
        assert_eq!(parsed.id, "019d77b1-d1ca-7c90-82fa-927613e48567");
        assert_eq!(parsed.cwd, "/tmp/project");
        assert_eq!(parsed.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(parsed.git_branch.as_deref(), Some("main"));
        assert_eq!(
            parsed.git_origin_url.as_deref(),
            Some("git@example.com:repo.git")
        );
        assert_eq!(parsed.title, "$superpowers Build the monitor");

        fs::remove_dir_all(root).unwrap();
    }
}
