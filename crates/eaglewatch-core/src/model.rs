use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Codex,
    Claude,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codex => f.write_str("codex"),
            Self::Claude => f.write_str("claude"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatusKind {
    Running,
    ToolBusy,
    WaitingInput,
    Idle,
    Stale,
    Completed,
    Failed,
    Unknown,
}

impl fmt::Display for SessionStatusKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Running => "running",
            Self::ToolBusy => "tool_busy",
            Self::WaitingInput => "waiting_input",
            Self::Idle => "idle",
            Self::Stale => "stale",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        };
        f.write_str(label)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StatusConfidence {
    Exact,
    Inferred,
    Unknown,
}

impl fmt::Display for StatusConfidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact => f.write_str("exact"),
            Self::Inferred => f.write_str("inferred"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionStatus {
    pub kind: SessionStatusKind,
    pub confidence: StatusConfidence,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub total_tokens: u64,
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_output_tokens: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NavigationKind {
    ThreadId,
    RolloutPath,
    WorkingDirectory,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NavigationTarget {
    pub kind: NavigationKind,
    pub label: String,
    pub target: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    User,
    Assistant,
    ToolCall,
    ToolResult,
    System,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityEvent {
    pub timestamp: DateTime<Utc>,
    pub kind: ActivityKind,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCallStat {
    pub name: String,
    pub count: u32,
    pub last_seen: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub machine_id: String,
    pub machine_label: String,
    pub provider: ProviderKind,
    pub title: String,
    pub cwd: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived: bool,
    pub model: Option<String>,
    pub agent_role: Option<String>,
    pub git_branch: Option<String>,
    pub git_origin_url: Option<String>,
    pub tokens: TokenUsage,
    pub status: SessionStatus,
    pub rollout_path: Option<String>,
    pub navigation: Vec<NavigationTarget>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionDetail {
    pub summary: SessionSummary,
    pub recent_events: Vec<ActivityEvent>,
    pub tool_stats: Vec<ToolCallStat>,
    pub last_user_message: Option<String>,
    pub last_assistant_message: Option<String>,
    pub active_turns: usize,
    pub pending_tool_calls: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionList {
    pub generated_at: DateTime<Utc>,
    pub sessions: Vec<SessionSummary>,
}
