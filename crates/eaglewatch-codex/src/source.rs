use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use directories::BaseDirs;
use eaglewatch_core::{
    ActivityEvent, ActivityKind, NavigationKind, NavigationTarget, ProviderKind, SessionDetail,
    SessionQuery, SessionSource, SessionStatus, SessionStatusKind, SessionSummary,
    StatusConfidence, TokenUsage, ToolCallStat,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

const SUMMARY_TAIL_LINES: usize = 96;
const DETAIL_TAIL_LINES: usize = 320;
const RECENT_EVENT_LIMIT: usize = 48;
const RUNNING_TTL_SECONDS: i64 = 90;
const STALE_AFTER_MINUTES: i64 = 20;

#[derive(Clone, Debug)]
pub struct CodexSource {
    codex_home: PathBuf,
    machine_id: String,
    machine_label: String,
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

        Self {
            codex_home: codex_home.into(),
            machine_id: machine_label.clone(),
            machine_label,
        }
    }

    pub fn from_default_home() -> Result<Self> {
        let base_dirs =
            BaseDirs::new().ok_or_else(|| anyhow!("could not resolve a home directory"))?;
        Ok(Self::new(base_dirs.home_dir().join(".codex")))
    }

    fn state_db_path(&self) -> PathBuf {
        self.codex_home.join("state_5.sqlite")
    }

    fn open_state_db(&self) -> Result<Connection> {
        Connection::open_with_flags(
            self.state_db_path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open {:?}", self.state_db_path()))
    }

    fn load_threads(&self) -> Result<Vec<ThreadRow>> {
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

    fn thread_by_id(&self, id: &str) -> Result<ThreadRow> {
        self.load_threads()?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| anyhow!("session {id} was not found"))
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

            if let Some(limit) = query.limit {
                if sessions.len() >= limit {
                    break;
                }
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
    tokens.total_tokens = row.tokens_used;

    let status = derive_status(row, hint, now);
    let rollout_path = (!row.rollout_path.is_empty()).then_some(row.rollout_path.clone());
    let navigation = build_navigation(row, rollout_path.clone());

    SessionSummary {
        id: row.id.clone(),
        machine_id: machine_id.to_string(),
        machine_label: machine_label.to_string(),
        provider: ProviderKind::Codex,
        title: normalize_inline_text(&row.title),
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

    if let Some((user_timestamp, _)) = user_message {
        if assistant_timestamp <= user_timestamp {
            return false;
        }
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

fn normalize_inline_text(text: &str) -> String {
    text.split_whitespace()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
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

#[cfg(test)]
mod tests {
    use super::looks_like_waiting_input;
    use chrono::Utc;

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
}
