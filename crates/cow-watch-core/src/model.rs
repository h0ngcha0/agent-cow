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

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SessionCost {
    pub input_usd: f64,
    pub cached_input_usd: f64,
    pub output_usd: f64,
    pub total_usd: f64,
    pub hour_usd: f64,
    pub day_usd: f64,
    pub pricing_source: PricingSource,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PricingSource {
    BuiltIn,
    LiteLlm,
    #[default]
    Unknown,
}

impl fmt::Display for PricingSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BuiltIn => f.write_str("built_in"),
            Self::LiteLlm => f.write_str("litellm"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextWindowUsage {
    pub used_tokens: u64,
    pub limit_tokens: u64,
    pub remaining_tokens: u64,
    pub used_percent: u8,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaWindow {
    pub label: String,
    pub used_percent: u8,
    pub remaining_percent: u8,
    pub reset_at: Option<DateTime<Utc>>,
    pub window_minutes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderQuota {
    pub provider: ProviderKind,
    pub plan: Option<String>,
    pub windows: Vec<QuotaWindow>,
    pub limit_reached: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageOverview {
    pub total_tokens: u64,
    pub total_cost_usd: f64,
    pub sessions_with_cost: usize,
    pub sessions_with_context: usize,
    pub quotas: Vec<ProviderQuota>,
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
    pub run_started_at: Option<DateTime<Utc>>,
    pub run_active: bool,
    pub archived: bool,
    pub model: Option<String>,
    pub agent_role: Option<String>,
    pub git_branch: Option<String>,
    pub git_origin_url: Option<String>,
    pub tokens: TokenUsage,
    pub cost: Option<SessionCost>,
    pub context_window: Option<ContextWindowUsage>,
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
    pub overview: UsageOverview,
    pub sessions: Vec<SessionSummary>,
}
