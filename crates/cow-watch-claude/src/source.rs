use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Local, Utc};
use cow_watch_core::{
    ActivityEvent, ActivityKind, ContextWindowUsage, NavigationKind, NavigationTarget,
    PricingSource, ProviderKind, ProviderQuota, SessionActivityState, SessionCost, SessionDetail,
    SessionList, SessionQuery, SessionSource, SessionStatus, SessionStatusKind, SessionSummary,
    StatusConfidence, TokenUsage, ToolCallStat, UsageOverview,
};
use directories::BaseDirs;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const SUMMARY_TAIL_LINES: usize = 384;
const DETAIL_TAIL_LINES: usize = 512;
const RECENT_EVENT_LIMIT: usize = 48;
const RECENT_CONVERSATION_LIMIT: usize = 32;
const RUNNING_TTL_SECONDS: i64 = 90;
const STALE_AFTER_MINUTES: i64 = 20;
const ACTIVITY_WINDOW_SECONDS: i64 = 30;
const TOOL_BUSY_ACTIVITY_WINDOW_SECONDS: i64 = 75;
const COMPACTION_ACTIVITY_WINDOW_SECONDS: i64 = 12;
const CLAUDE_DEFAULT_CONTEXT_WINDOW: u64 = 200_000;
const SESSION_DISCOVERY_CACHE_TTL: StdDuration = StdDuration::from_secs(5);
const LITELLM_PRICING_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
const LITELLM_PRICING_CACHE_TTL: StdDuration = StdDuration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct ClaudeSource {
    claude_home: PathBuf,
    projects_dir: PathBuf,
    machine_id: String,
    machine_label: String,
    pricing_client: Client,
    pricing_cache: Arc<Mutex<Option<LitellmPricingCache>>>,
    pricing_refresh_in_flight: Arc<AtomicBool>,
    static_cache: Arc<Mutex<HashMap<PathBuf, TranscriptStatic>>>,
    summary_cache: Arc<Mutex<HashMap<PathBuf, SummaryHintCacheEntry>>>,
    summary_cache_dirty: Arc<AtomicBool>,
    sessions_cache: Arc<Mutex<Option<SessionsCache>>>,
}

#[derive(Clone, Debug)]
struct SessionRow {
    id: String,
    transcript_path: PathBuf,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    cwd: String,
    title: String,
    model: Option<String>,
    git_branch: Option<String>,
    is_sidechain: bool,
}

#[derive(Clone, Debug, Default)]
struct TranscriptStatic {
    id: String,
    created_at: Option<DateTime<Utc>>,
    cwd: String,
    title: String,
    git_branch: Option<String>,
    model: Option<String>,
    is_sidechain: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TranscriptHint {
    pending_tool_ids: HashSet<String>,
    pending_tool_names: HashMap<String, String>,
    run_started_at: Option<DateTime<Utc>>,
    run_active: bool,
    cumulative_token_usage: Option<TokenUsage>,
    cost_by_day: HashMap<String, RawCostBucket>,
    cost_by_hour: HashMap<String, RawCostBucket>,
    latest_context_used_tokens: Option<u64>,
    latest_model: Option<String>,
    recent_compaction_at: Option<DateTime<Utc>>,
    last_user_message: Option<(DateTime<Utc>, String)>,
    last_assistant_message: Option<(DateTime<Utc>, String)>,
    recent_events: Vec<ActivityEvent>,
    recent_conversation: Vec<ActivityEvent>,
    tool_stats: HashMap<String, ToolCallStat>,
    latest_tool_call_at: Option<DateTime<Utc>>,
    latest_thinking_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RawCostBucket {
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
}

#[derive(Clone, Debug)]
struct ClaudePricing {
    input_per_million: f64,
    cached_input_per_million: f64,
    output_per_million: f64,
    max_input_tokens: Option<u64>,
    source: PricingSource,
}

type LitellmPricingMap = HashMap<String, LitellmPricingEntry>;

#[derive(Clone, Debug)]
struct LitellmPricingCache {
    fetched_at_epoch_ms: i64,
    entries: Arc<LitellmPricingMap>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LitellmPricingCacheFile {
    fetched_at_epoch_ms: i64,
    entries: LitellmPricingMap,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LitellmPricingEntry {
    input_cost_per_token: Option<f64>,
    cache_read_input_token_cost: Option<f64>,
    output_cost_per_token: Option<f64>,
    max_input_tokens: Option<u64>,
    max_tokens: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SummaryHintCacheEntry {
    modified_at_epoch_ms: i64,
    file_len: u64,
    hint: TranscriptHint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SummaryHintCacheFile {
    entries: Vec<SummaryHintCacheRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SummaryHintCacheRecord {
    path: String,
    entry: SummaryHintCacheEntry,
}

#[derive(Clone, Debug)]
struct SessionsCache {
    fetched_at_epoch_ms: i64,
    rows: Vec<SessionRow>,
}

#[derive(Debug, Deserialize)]
struct ClaudeAccountSnapshot {
    #[serde(rename = "hasAvailableSubscription")]
    has_available_subscription: Option<bool>,
    #[serde(rename = "oauthAccount")]
    oauth_account: Option<ClaudeOauthAccount>,
}

#[derive(Debug, Deserialize)]
struct ClaudeOauthAccount {
    #[serde(rename = "billingType")]
    billing_type: Option<String>,
    #[serde(rename = "subscriptionCreatedAt")]
    subscription_created_at: Option<String>,
}

#[derive(Default)]
struct UsageIndex {
    cumulative_token_usage: Option<TokenUsage>,
    cost_by_day: HashMap<String, RawCostBucket>,
    cost_by_hour: HashMap<String, RawCostBucket>,
}

#[derive(Debug, Deserialize)]
struct SessionsIndexFile {
    #[serde(rename = "originalPath")]
    original_path: Option<String>,
    #[serde(default)]
    entries: Vec<SessionIndexEntry>,
}

#[derive(Clone, Debug, Deserialize)]
struct SessionIndexEntry {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "fullPath")]
    _full_path: String,
    #[serde(rename = "fileMtime")]
    file_mtime: Option<i64>,
    #[serde(rename = "firstPrompt")]
    first_prompt: Option<String>,
    summary: Option<String>,
    created: Option<String>,
    modified: Option<String>,
    #[serde(rename = "projectPath")]
    project_path: Option<String>,
    #[serde(rename = "gitBranch")]
    git_branch: Option<String>,
    #[serde(rename = "isSidechain", default)]
    is_sidechain: bool,
}

#[derive(Copy, Clone, Debug)]
enum ReadMode {
    Summary,
    Detail,
}

impl ClaudeSource {
    pub fn new(claude_home: impl Into<PathBuf>) -> Self {
        let machine_label = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "local".to_string());
        let claude_home = normalize_claude_home(expand_known_path_vars(claude_home.into()));
        let projects_dir = claude_projects_dir(&claude_home);
        let summary_cache = load_summary_cache_from_disk().unwrap_or_default();

        Self {
            claude_home,
            projects_dir,
            machine_id: machine_label.clone(),
            machine_label,
            pricing_client: Client::builder()
                .user_agent("cow-watch/0.1")
                .build()
                .expect("reqwest client should build"),
            pricing_cache: Arc::new(Mutex::new(None)),
            pricing_refresh_in_flight: Arc::new(AtomicBool::new(false)),
            static_cache: Arc::new(Mutex::new(HashMap::new())),
            summary_cache: Arc::new(Mutex::new(summary_cache)),
            summary_cache_dirty: Arc::new(AtomicBool::new(false)),
            sessions_cache: Arc::new(Mutex::new(None)),
        }
    }

    pub fn from_default_home() -> Result<Self> {
        let base_dirs =
            BaseDirs::new().ok_or_else(|| anyhow!("could not resolve a home directory"))?;
        Ok(Self::new(base_dirs.home_dir().join(".claude")))
    }

    fn projects_exist(&self) -> bool {
        self.projects_dir.exists()
    }

    fn subscription_quota(&self) -> Option<ProviderQuota> {
        load_claude_subscription_quota(&self.claude_home)
    }

    fn load_sessions(&self) -> Result<Vec<SessionRow>> {
        if let Some(cache) = self.sessions_cache.lock().expect("lock poisoned").clone()
            && is_sessions_cache_fresh(cache.fetched_at_epoch_ms)
        {
            return Ok(cache.rows);
        }

        let mut rows = Vec::new();
        if self.projects_exist() {
            collect_sessions_from_projects(&self.projects_dir, &mut rows, self)?;
        }
        rows.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));

        *self.sessions_cache.lock().expect("lock poisoned") = Some(SessionsCache {
            fetched_at_epoch_ms: now_epoch_millis(),
            rows: rows.clone(),
        });
        Ok(rows)
    }

    fn session_by_id(&self, id: &str) -> Result<SessionRow> {
        self.load_sessions()?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| anyhow!("no Claude session was found for id `{id}`"))
    }

    fn transcript_static(&self, path: &Path) -> Result<TranscriptStatic> {
        if let Some(static_data) = self
            .static_cache
            .lock()
            .expect("lock poisoned")
            .get(path)
            .cloned()
        {
            return Ok(static_data);
        }

        let static_data = parse_transcript_static(path)?;
        self.static_cache
            .lock()
            .expect("lock poisoned")
            .insert(path.to_path_buf(), static_data.clone());
        Ok(static_data)
    }

    fn summary_hint(&self, path: &Path) -> Result<TranscriptHint> {
        let signature = transcript_cache_signature(path)?;
        if let Some(entry) = self
            .summary_cache
            .lock()
            .expect("lock poisoned")
            .get(path)
            .cloned()
            && entry.modified_at_epoch_ms == signature.0
            && entry.file_len == signature.1
        {
            return Ok(entry.hint);
        }

        let hint = analyze_transcript(path, ReadMode::Summary)?;
        self.summary_cache.lock().expect("lock poisoned").insert(
            path.to_path_buf(),
            SummaryHintCacheEntry {
                modified_at_epoch_ms: signature.0,
                file_len: signature.1,
                hint: hint.clone(),
            },
        );
        self.summary_cache_dirty.store(true, Ordering::Relaxed);
        Ok(hint)
    }

    fn persist_summary_cache_if_dirty(&self) {
        if !self.summary_cache_dirty.swap(false, Ordering::Relaxed) {
            return;
        }

        let cache = self.summary_cache.lock().expect("lock poisoned");
        if let Err(error) = store_summary_cache_to_disk(&cache) {
            tracing::debug!("failed to persist Claude summary cache: {error:#}");
        }
    }

    async fn litellm_pricing(&self) -> Option<Arc<LitellmPricingMap>> {
        if let Some(cache) = self.pricing_cache.lock().expect("lock poisoned").as_ref()
            && is_pricing_cache_fresh(cache.fetched_at_epoch_ms)
        {
            return Some(cache.entries.clone());
        }

        if let Some(cache) = load_pricing_cache_from_disk()
            .ok()
            .flatten()
            .filter(|cache| is_pricing_cache_fresh(cache.fetched_at_epoch_ms))
        {
            let entries = cache.entries.clone();
            *self.pricing_cache.lock().expect("lock poisoned") = Some(cache);
            return Some(entries);
        }

        if self
            .pricing_refresh_in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return self
                .pricing_cache
                .lock()
                .expect("lock poisoned")
                .as_ref()
                .map(|cache| cache.entries.clone());
        }

        let response = self.pricing_client.get(LITELLM_PRICING_URL).send().await;
        self.pricing_refresh_in_flight
            .store(false, Ordering::SeqCst);

        let Ok(response) = response else {
            return self
                .pricing_cache
                .lock()
                .expect("lock poisoned")
                .as_ref()
                .map(|cache| cache.entries.clone());
        };

        let Ok(value) = response.json::<Value>().await else {
            return self
                .pricing_cache
                .lock()
                .expect("lock poisoned")
                .as_ref()
                .map(|cache| cache.entries.clone());
        };

        let Some(entries) = parse_litellm_pricing_map(&value) else {
            return self
                .pricing_cache
                .lock()
                .expect("lock poisoned")
                .as_ref()
                .map(|cache| cache.entries.clone());
        };

        let cache = LitellmPricingCache {
            fetched_at_epoch_ms: now_epoch_millis(),
            entries: Arc::new(entries),
        };
        let entries = cache.entries.clone();
        *self.pricing_cache.lock().expect("lock poisoned") = Some(cache.clone());
        let _ = store_pricing_cache_to_disk(&cache);
        Some(entries)
    }
}

#[async_trait]
impl SessionSource for ClaudeSource {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        let now = Utc::now();
        let litellm_pricing = self.litellm_pricing().await;
        let subscription = self.subscription_quota();
        let mut sessions = Vec::new();

        for row in self.load_sessions()? {
            let hint = self.summary_hint(&row.transcript_path)?;
            let summary = build_summary(
                &self.machine_id,
                &self.machine_label,
                &row,
                &hint,
                litellm_pricing.as_deref(),
                now,
            );
            sessions.push(summary);
        }

        sessions.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        if let Some(limit) = query.limit {
            sessions.truncate(limit);
        }

        self.persist_summary_cache_if_dirty();

        Ok(SessionList {
            generated_at: now,
            overview: build_overview(&sessions, subscription),
            sessions,
        })
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let now = Utc::now();
        let row = self.session_by_id(id)?;
        let hint = analyze_transcript(&row.transcript_path, ReadMode::Detail)?;
        let litellm_pricing = self.litellm_pricing().await;
        let summary = build_summary(
            &self.machine_id,
            &self.machine_label,
            &row,
            &hint,
            litellm_pricing.as_deref(),
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
            recent_conversation: hint.recent_conversation,
            tool_stats,
            last_user_message: hint.last_user_message.map(|(_, text)| text),
            last_assistant_message: hint.last_assistant_message.map(|(_, text)| text),
            active_turns: usize::from(hint.run_active),
            pending_tool_calls: hint.pending_tool_ids.len(),
        })
    }
}

fn build_summary(
    machine_id: &str,
    machine_label: &str,
    row: &SessionRow,
    hint: &TranscriptHint,
    litellm_pricing: Option<&LitellmPricingMap>,
    now: DateTime<Utc>,
) -> SessionSummary {
    let model = row.model.clone().or_else(|| hint.latest_model.clone());
    let pricing = model
        .as_deref()
        .and_then(|model| resolve_claude_pricing(model, litellm_pricing));
    let tokens = hint.cumulative_token_usage.clone().unwrap_or_default();
    let context_window = derive_context_window_usage(
        hint.latest_context_used_tokens,
        model.as_deref(),
        litellm_pricing,
    );
    let cost = pricing.as_ref().and_then(|pricing| {
        estimate_session_cost(
            &tokens,
            pricing,
            current_hour_cost_usd(&hint.cost_by_hour, pricing, now),
            current_day_cost_usd(&hint.cost_by_day, pricing, now),
        )
    });
    let status = derive_status(row, hint, now);
    let activity_state =
        derive_activity_state(&status, hint, context_window.as_ref(), row.updated_at, now);

    SessionSummary {
        id: row.id.clone(),
        machine_id: machine_id.to_string(),
        machine_label: machine_label.to_string(),
        provider: ProviderKind::Claude,
        title: normalize_title(&row.title),
        cwd: row.cwd.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        run_started_at: hint.run_started_at,
        run_active: hint.run_active,
        archived: false,
        model,
        agent_role: row.is_sidechain.then_some("sidechain".to_string()),
        git_branch: row.git_branch.clone(),
        git_origin_url: None,
        tokens,
        cost,
        context_window,
        status,
        activity_state,
        rollout_path: Some(row.transcript_path.to_string_lossy().into_owned()),
        navigation: build_navigation(row),
    }
}

fn derive_status(row: &SessionRow, hint: &TranscriptHint, now: DateTime<Utc>) -> SessionStatus {
    let idle_for = now - row.updated_at;

    if !hint.pending_tool_ids.is_empty() {
        let tool_name = hint
            .pending_tool_ids
            .iter()
            .find_map(|call_id| hint.pending_tool_names.get(call_id))
            .cloned()
            .unwrap_or_else(|| "tool".to_string());

        return SessionStatus {
            kind: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                SessionStatusKind::ToolBusy
            } else {
                SessionStatusKind::Stale
            },
            confidence: StatusConfidence::Inferred,
            reason: if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
                format!("Waiting on `{tool_name}` tool output")
            } else {
                format!("A `{tool_name}` call is still open in the latest Claude transcript tail")
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
            reason: "Last Claude assistant message looks like a request for user input".to_string(),
        };
    }

    if hint.run_active || idle_for <= Duration::seconds(RUNNING_TTL_SECONDS) {
        return SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: if hint.run_active {
                "Claude still has recent in-flight reasoning or tool activity".to_string()
            } else {
                "Recent Claude activity is still inside the running TTL".to_string()
            },
        };
    }

    if idle_for <= Duration::minutes(STALE_AFTER_MINUTES) {
        SessionStatus {
            kind: SessionStatusKind::Idle,
            confidence: StatusConfidence::Inferred,
            reason:
                "No active Claude tool or reasoning signal was found in the latest transcript tail"
                    .to_string(),
        }
    } else {
        SessionStatus {
            kind: SessionStatusKind::Stale,
            confidence: StatusConfidence::Inferred,
            reason: "The Claude session has not emitted activity recently".to_string(),
        }
    }
}

fn derive_activity_state(
    status: &SessionStatus,
    hint: &TranscriptHint,
    _context_window: Option<&ContextWindowUsage>,
    updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> SessionActivityState {
    if matches!(status.kind, SessionStatusKind::WaitingInput) {
        return SessionActivityState::Waiting;
    }

    if matches!(
        status.kind,
        SessionStatusKind::Completed | SessionStatusKind::Failed | SessionStatusKind::Stale
    ) {
        return SessionActivityState::Idle;
    }

    let recent_window = if !hint.pending_tool_ids.is_empty() {
        Duration::seconds(TOOL_BUSY_ACTIVITY_WINDOW_SECONDS)
    } else {
        Duration::seconds(ACTIVITY_WINDOW_SECONDS)
    };

    let has_live_signal = hint.run_active
        || !hint.pending_tool_ids.is_empty()
        || hint
            .latest_thinking_at
            .is_some_and(|timestamp| now - timestamp <= recent_window);
    if !has_live_signal && now - updated_at > recent_window {
        return SessionActivityState::Idle;
    }

    if hint.recent_compaction_at.is_some_and(|timestamp| {
        now - timestamp <= Duration::seconds(COMPACTION_ACTIVITY_WINDOW_SECONDS)
    }) && hint
        .recent_events
        .iter()
        .rev()
        .find(|event| !matches!(event.kind, ActivityKind::User))
        .is_some_and(is_compaction_event)
    {
        return SessionActivityState::Compacting;
    }

    if hint
        .recent_events
        .iter()
        .rev()
        .find(|event| matches!(event.kind, ActivityKind::ToolCall))
        .is_some_and(|event| {
            now - event.timestamp <= recent_window && is_exploration_tool(&event.summary)
        })
    {
        return SessionActivityState::Exploring;
    }

    if matches!(
        status.kind,
        SessionStatusKind::Running | SessionStatusKind::ToolBusy
    ) {
        SessionActivityState::Thinking
    } else {
        SessionActivityState::Idle
    }
}

fn is_compaction_event(event: &ActivityEvent) -> bool {
    matches!(event.kind, ActivityKind::System) && looks_like_compaction_signal(&event.summary)
}

fn looks_like_compaction_signal(text: &str) -> bool {
    let lowered = text.to_ascii_lowercase();
    lowered.contains("context compacted")
        || lowered.contains("compacting")
        || lowered.contains("auto compact")
        || lowered.contains("auto-compact")
        || lowered.contains("auto compaction")
        || lowered.contains("auto-compaction")
}

fn build_navigation(row: &SessionRow) -> Vec<NavigationTarget> {
    if row.cwd.is_empty() {
        Vec::new()
    } else {
        vec![NavigationTarget {
            kind: NavigationKind::WorkingDirectory,
            label: "Working Directory".to_string(),
            target: row.cwd.clone(),
        }]
    }
}

fn build_overview(
    sessions: &[SessionSummary],
    subscription: Option<ProviderQuota>,
) -> UsageOverview {
    UsageOverview {
        total_tokens: sessions
            .iter()
            .map(|session| session.tokens.total_tokens)
            .sum(),
        total_cost_usd: sessions
            .iter()
            .filter_map(|session| session.cost.as_ref().map(|cost| cost.total_usd))
            .sum(),
        sessions_with_cost: sessions
            .iter()
            .filter(|session| session.cost.is_some())
            .count(),
        sessions_with_context: sessions
            .iter()
            .filter(|session| session.context_window.is_some())
            .count(),
        quotas: subscription.into_iter().collect(),
    }
}

fn load_claude_subscription_quota(claude_home: &Path) -> Option<ProviderQuota> {
    let snapshot = load_latest_claude_account_snapshot(claude_home)?;
    let billing_type = snapshot
        .oauth_account
        .as_ref()
        .and_then(|account| account.billing_type.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let has_subscription = snapshot.has_available_subscription.unwrap_or(false)
        || billing_type == Some("stripe_subscription")
        || snapshot
            .oauth_account
            .as_ref()
            .and_then(|account| account.subscription_created_at.as_deref())
            .is_some();

    if !has_subscription {
        return None;
    }

    Some(ProviderQuota {
        provider: ProviderKind::Claude,
        plan: Some(match billing_type {
            Some("stripe_subscription") => "Subscription".to_string(),
            Some(other) => other
                .replace('_', " ")
                .split_whitespace()
                .map(|part| {
                    let mut chars = part.chars();
                    let Some(first) = chars.next() else {
                        return String::new();
                    };
                    format!(
                        "{}{}",
                        first.to_uppercase(),
                        chars.as_str().to_ascii_lowercase()
                    )
                })
                .collect::<Vec<_>>()
                .join(" "),
            None => "Subscription".to_string(),
        }),
        windows: Vec::new(),
        limit_reached: false,
    })
}

fn resolve_claude_pricing(
    model: &str,
    litellm_pricing: Option<&LitellmPricingMap>,
) -> Option<ClaudePricing> {
    if let Some(entry) = litellm_pricing.and_then(|entries| find_litellm_entry(model, entries))
        && let Some(pricing) = litellm_to_claude_pricing(entry)
    {
        return Some(pricing);
    }

    let lowered = model.to_ascii_lowercase();
    if lowered.contains("opus") {
        return Some(ClaudePricing {
            input_per_million: 15.0,
            cached_input_per_million: 1.5,
            output_per_million: 75.0,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }
    if lowered.contains("sonnet") {
        return Some(ClaudePricing {
            input_per_million: 3.0,
            cached_input_per_million: 0.3,
            output_per_million: 15.0,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }
    if lowered.contains("haiku") {
        return Some(ClaudePricing {
            input_per_million: 0.8,
            cached_input_per_million: 0.08,
            output_per_million: 4.0,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }

    None
}

fn builtin_context_window(model: &str) -> u64 {
    let lowered = model.to_ascii_lowercase();
    if lowered.contains("1m") {
        1_000_000
    } else {
        CLAUDE_DEFAULT_CONTEXT_WINDOW
    }
}

fn litellm_to_claude_pricing(entry: &LitellmPricingEntry) -> Option<ClaudePricing> {
    Some(ClaudePricing {
        input_per_million: entry.input_cost_per_token? * 1_000_000.0,
        cached_input_per_million: entry.cache_read_input_token_cost.unwrap_or(0.0) * 1_000_000.0,
        output_per_million: entry.output_cost_per_token? * 1_000_000.0,
        max_input_tokens: entry.max_input_tokens.or(entry.max_tokens),
        source: PricingSource::LiteLlm,
    })
}

fn find_litellm_entry<'a>(
    model: &str,
    entries: &'a LitellmPricingMap,
) -> Option<&'a LitellmPricingEntry> {
    entries
        .get(model)
        .or_else(|| entries.get(&format!("anthropic/{model}")))
        .or_else(|| {
            let lowered = model.to_ascii_lowercase();
            entries
                .iter()
                .find(|(key, _)| key.to_ascii_lowercase().contains(&lowered))
                .map(|(_, value)| value)
        })
}

fn derive_context_window_usage(
    used_tokens: Option<u64>,
    model: Option<&str>,
    litellm_pricing: Option<&LitellmPricingMap>,
) -> Option<ContextWindowUsage> {
    let used_tokens = used_tokens?;
    let limit_tokens = model
        .and_then(|model| {
            resolve_claude_pricing(model, litellm_pricing)
                .and_then(|pricing| pricing.max_input_tokens)
        })
        .unwrap_or_else(|| {
            model
                .map(builtin_context_window)
                .unwrap_or(CLAUDE_DEFAULT_CONTEXT_WINDOW)
        })
        .max(1);
    let remaining_tokens = limit_tokens.saturating_sub(used_tokens);
    let used_percent = ((used_tokens as f64 / limit_tokens as f64) * 100.0)
        .round()
        .min(u8::MAX as f64) as u8;

    Some(ContextWindowUsage {
        used_tokens,
        limit_tokens,
        remaining_tokens,
        used_percent,
    })
}

fn estimate_session_cost(
    tokens: &TokenUsage,
    pricing: &ClaudePricing,
    hour_usd: f64,
    day_usd: f64,
) -> Option<SessionCost> {
    let input_tokens = tokens.input_tokens?;
    let cached_input_tokens = tokens.cached_input_tokens.unwrap_or(0);
    let output_tokens = tokens.output_tokens?;
    let uncached_input_tokens = input_tokens.saturating_sub(cached_input_tokens);

    let input_usd = token_cost(uncached_input_tokens, pricing.input_per_million);
    let cached_input_usd = token_cost(cached_input_tokens, pricing.cached_input_per_million);
    let output_usd = token_cost(output_tokens, pricing.output_per_million);

    Some(SessionCost {
        input_usd,
        cached_input_usd,
        output_usd,
        total_usd: input_usd + cached_input_usd + output_usd,
        hour_usd,
        day_usd,
        pricing_source: pricing.source.clone(),
    })
}

fn estimate_raw_cost_bucket(bucket: &RawCostBucket, pricing: &ClaudePricing) -> f64 {
    let uncached_input_tokens = bucket
        .input_tokens
        .saturating_sub(bucket.cached_input_tokens);
    token_cost(uncached_input_tokens, pricing.input_per_million)
        + token_cost(bucket.cached_input_tokens, pricing.cached_input_per_million)
        + token_cost(bucket.output_tokens, pricing.output_per_million)
}

fn current_hour_cost_usd(
    buckets: &HashMap<String, RawCostBucket>,
    pricing: &ClaudePricing,
    now: DateTime<Utc>,
) -> f64 {
    let hour_key = local_hour_key(now);
    buckets
        .get(&hour_key)
        .map(|bucket| estimate_raw_cost_bucket(bucket, pricing))
        .unwrap_or(0.0)
}

fn current_day_cost_usd(
    buckets: &HashMap<String, RawCostBucket>,
    pricing: &ClaudePricing,
    now: DateTime<Utc>,
) -> f64 {
    let today_key = local_date_key(now);
    buckets
        .iter()
        .filter(|(day, _)| day.as_str() >= today_key.as_str())
        .map(|(_, bucket)| estimate_raw_cost_bucket(bucket, pricing))
        .sum()
}

fn token_cost(tokens: u64, rate_per_million: f64) -> f64 {
    (tokens as f64 / 1_000_000.0) * rate_per_million
}

fn collect_sessions_from_projects(
    projects_dir: &Path,
    output: &mut Vec<SessionRow>,
    source: &ClaudeSource,
) -> Result<()> {
    let entries = fs::read_dir(projects_dir)
        .with_context(|| format!("failed to read {}", projects_dir.display()))?;

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_dir() {
            continue;
        }

        let index_path = entry.path().join("sessions-index.json");
        if !index_path.exists() {
            continue;
        }

        let raw = match fs::read_to_string(&index_path) {
            Ok(raw) => raw,
            Err(_) => continue,
        };
        let parsed: SessionsIndexFile = match serde_json::from_str(&raw) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };

        let index_entries: HashMap<String, SessionIndexEntry> = parsed
            .entries
            .into_iter()
            .map(|entry| (entry.session_id.clone(), entry))
            .collect();
        let project_path_fallback = parsed.original_path.filter(|path| !path.is_empty());

        let transcripts = fs::read_dir(entry.path())
            .with_context(|| format!("failed to read {}", entry.path().display()))?;

        for transcript_path in transcripts
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        {
            let static_data = source
                .transcript_static(&transcript_path)
                .unwrap_or_else(|_| TranscriptStatic::default());
            let session_id = if static_data.id.is_empty() {
                fallback_session_id(&transcript_path)
            } else {
                static_data.id.clone()
            };
            let index_entry = index_entries.get(&session_id);
            let created_at = index_entry
                .and_then(|entry| entry.created.as_deref())
                .and_then(parse_rfc3339)
                .or(static_data.created_at)
                .or_else(|| file_modified_at(&transcript_path))
                .unwrap_or_else(Utc::now);
            let updated_at = file_modified_at(&transcript_path)
                .or_else(|| {
                    index_entry
                        .and_then(|entry| entry.modified.as_deref())
                        .and_then(parse_rfc3339)
                })
                .or_else(|| {
                    index_entry.and_then(|entry| entry.file_mtime.and_then(epoch_millis_to_utc))
                })
                .unwrap_or(created_at);
            let cwd = (!static_data.cwd.is_empty())
                .then_some(static_data.cwd.clone())
                .or_else(|| {
                    index_entry
                        .and_then(|entry| entry.project_path.clone())
                        .filter(|path| !path.is_empty())
                })
                .or_else(|| project_path_fallback.clone())
                .unwrap_or_default();
            let title = index_entry
                .and_then(|entry| entry.summary.clone().or(entry.first_prompt.clone()))
                .and_then(|text| user_title_candidate(&text))
                .or_else(|| (!static_data.title.is_empty()).then_some(static_data.title.clone()))
                .unwrap_or_else(|| fallback_title(&cwd, &session_id));

            output.push(SessionRow {
                id: session_id,
                transcript_path,
                created_at,
                updated_at,
                cwd,
                title,
                model: static_data.model,
                git_branch: static_data
                    .git_branch
                    .clone()
                    .or_else(|| index_entry.and_then(|entry| entry.git_branch.clone())),
                is_sidechain: static_data.is_sidechain
                    || index_entry.is_some_and(|entry| entry.is_sidechain),
            });
        }
    }

    Ok(())
}

fn analyze_transcript(path: &Path, mode: ReadMode) -> Result<TranscriptHint> {
    let lines = read_transcript_lines(path, mode)?;
    let usage_index = stream_usage_index(path)?;
    let mut hint = TranscriptHint {
        cumulative_token_usage: usage_index.cumulative_token_usage,
        cost_by_day: usage_index.cost_by_day,
        cost_by_hour: usage_index.cost_by_hour,
        ..TranscriptHint::default()
    };
    let mut recent_events = VecDeque::with_capacity(RECENT_EVENT_LIMIT);
    let mut recent_conversation = VecDeque::with_capacity(RECENT_CONVERSATION_LIMIT);

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

        match root_kind {
            "user" => {
                let Some(timestamp) = timestamp else {
                    continue;
                };
                let message = value.get("message").unwrap_or(&Value::Null);
                if let Some(text) = extract_claude_user_text(message) {
                    remember_latest_timestamp(&mut hint.run_started_at, timestamp);
                    record_message(
                        &mut hint,
                        &mut recent_events,
                        &mut recent_conversation,
                        timestamp,
                        ActivityKind::User,
                        &text,
                    );
                } else {
                    for tool_result in extract_tool_results(message) {
                        if let Some(tool_use_id) = tool_result.tool_use_id.as_deref() {
                            let tool_name = hint.pending_tool_names.get(tool_use_id).cloned();
                            hint.pending_tool_ids.remove(tool_use_id);
                            if let Some(tool_name) = tool_name {
                                push_recent_event(
                                    &mut recent_events,
                                    ActivityEvent {
                                        timestamp,
                                        kind: ActivityKind::ToolResult,
                                        summary: format!("Finished `{tool_name}`"),
                                    },
                                );
                            }
                        }
                    }
                }
            }
            "assistant" => {
                let Some(timestamp) = timestamp else {
                    continue;
                };
                let message = value.get("message").unwrap_or(&Value::Null);
                let model = message
                    .get("model")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if hint.latest_model.is_none() {
                    hint.latest_model = model.clone();
                }

                if let Some(delta) = parse_claude_usage(message) {
                    hint.latest_context_used_tokens = delta.input_tokens;
                }

                let mut text_chunks = Vec::new();
                for item in message
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
                    match item_type {
                        "thinking" => {
                            hint.latest_thinking_at = Some(timestamp);
                        }
                        "text" => {
                            if let Some(text) = item.get("text").and_then(Value::as_str)
                                && !text.trim().is_empty()
                            {
                                text_chunks.push(text.trim().to_string());
                                if looks_like_compaction_signal(text) {
                                    hint.recent_compaction_at = Some(timestamp);
                                }
                            }
                        }
                        "tool_use" => {
                            let call_id = item
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            let name = item
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("tool")
                                .to_string();

                            if !call_id.is_empty() {
                                hint.pending_tool_ids.insert(call_id.clone());
                                hint.pending_tool_names.insert(call_id, name.clone());
                            }

                            hint.latest_tool_call_at = Some(timestamp);
                            remember_latest_timestamp(&mut hint.run_started_at, timestamp);
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::ToolCall,
                                    summary: summarize_claude_tool_call(
                                        &name,
                                        item.get("input").unwrap_or(&Value::Null),
                                        value
                                            .get("cwd")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default(),
                                    ),
                                },
                            );

                            let entry =
                                hint.tool_stats.entry(name.clone()).or_insert(ToolCallStat {
                                    name,
                                    count: 0,
                                    last_seen: None,
                                });
                            entry.count += 1;
                            entry.last_seen = Some(timestamp);
                        }
                        _ => {}
                    }
                }

                if !text_chunks.is_empty() {
                    let message = text_chunks.join("\n\n");
                    record_message(
                        &mut hint,
                        &mut recent_events,
                        &mut recent_conversation,
                        timestamp,
                        ActivityKind::Assistant,
                        &message,
                    );
                }
            }
            "progress" | "system" => {
                if let Some(text) = compact_progress_summary(&value)
                    && let Some(timestamp) = timestamp
                {
                    if looks_like_compaction_signal(&text) {
                        hint.recent_compaction_at = Some(timestamp);
                    }
                    push_recent_event(
                        &mut recent_events,
                        ActivityEvent {
                            timestamp,
                            kind: ActivityKind::System,
                            summary: text,
                        },
                    );
                }
            }
            _ => {}
        }
    }

    hint.run_active = !hint.pending_tool_ids.is_empty()
        || hint
            .latest_thinking_at
            .zip(
                hint.last_assistant_message
                    .as_ref()
                    .map(|(timestamp, _)| *timestamp),
            )
            .is_some_and(|(thinking_at, assistant_at)| thinking_at >= assistant_at);

    hint.recent_events = recent_events.into_iter().collect();
    hint.recent_conversation = recent_conversation.into_iter().collect();
    Ok(hint)
}

fn parse_claude_usage(message: &Value) -> Option<TokenUsage> {
    let usage = message.get("usage")?;
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation_input_tokens = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read_input_tokens = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_input_tokens = input_tokens
        .saturating_add(cache_creation_input_tokens)
        .saturating_add(cache_read_input_tokens);
    let total_tokens = total_input_tokens.saturating_add(output_tokens);

    if total_tokens == 0 {
        return None;
    }

    Some(TokenUsage {
        total_tokens,
        input_tokens: Some(total_input_tokens),
        cached_input_tokens: Some(cache_read_input_tokens),
        output_tokens: Some(output_tokens),
        reasoning_output_tokens: None,
    })
}

fn stream_usage_index(path: &Path) -> Result<UsageIndex> {
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?,
    );
    let mut cumulative = TokenUsage::default();
    let mut saw_usage = false;
    let mut cost_by_day = HashMap::new();
    let mut cost_by_hour = HashMap::new();

    for line in reader.lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => continue,
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if value.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(message) = value.get("message") else {
            continue;
        };
        let Some(delta) = parse_claude_usage(message) else {
            continue;
        };

        saw_usage = true;
        accumulate_token_usage(&mut cumulative, &delta);
        if let Some(timestamp) = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339)
        {
            add_raw_cost_bucket(&mut cost_by_day, local_date_key(timestamp), &delta);
            add_raw_cost_bucket(&mut cost_by_hour, local_hour_key(timestamp), &delta);
        }
    }

    Ok(UsageIndex {
        cumulative_token_usage: saw_usage.then_some(cumulative),
        cost_by_day,
        cost_by_hour,
    })
}

fn accumulate_token_usage(total: &mut TokenUsage, delta: &TokenUsage) {
    total.total_tokens = total.total_tokens.saturating_add(delta.total_tokens);
    total.input_tokens = Some(
        total
            .input_tokens
            .unwrap_or(0)
            .saturating_add(delta.input_tokens.unwrap_or(0)),
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
}

fn add_raw_cost_bucket(
    buckets: &mut HashMap<String, RawCostBucket>,
    key: String,
    delta: &TokenUsage,
) {
    let bucket = buckets.entry(key).or_default();
    bucket.input_tokens = bucket
        .input_tokens
        .saturating_add(delta.input_tokens.unwrap_or(0));
    bucket.cached_input_tokens = bucket
        .cached_input_tokens
        .saturating_add(delta.cached_input_tokens.unwrap_or(0));
    bucket.output_tokens = bucket
        .output_tokens
        .saturating_add(delta.output_tokens.unwrap_or(0));
}

fn parse_transcript_static(path: &Path) -> Result<TranscriptStatic> {
    let mut static_parts = TranscriptStatic {
        id: fallback_session_id(path),
        ..TranscriptStatic::default()
    };

    for line in read_first_lines(path, 32)? {
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let root_kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match root_kind {
            "progress" => {
                if static_parts.id.is_empty() {
                    static_parts.id = value
                        .get("sessionId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| fallback_session_id(path));
                }
                if static_parts.cwd.is_empty() {
                    static_parts.cwd = value
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                static_parts.is_sidechain |= value
                    .get("isSidechain")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            }
            "user" => {
                if static_parts.created_at.is_none() {
                    static_parts.created_at = value
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .and_then(parse_rfc3339);
                }
                if static_parts.cwd.is_empty() {
                    static_parts.cwd = value
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                if static_parts.git_branch.is_none() {
                    static_parts.git_branch = value
                        .get("gitBranch")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
                if static_parts.id.is_empty() {
                    static_parts.id = value
                        .get("sessionId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| fallback_session_id(path));
                }
                static_parts.is_sidechain |= value
                    .get("isSidechain")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if static_parts.title.is_empty()
                    && let Some(text) =
                        extract_claude_user_text(value.get("message").unwrap_or(&Value::Null))
                    && let Some(title) = user_title_candidate(&text)
                {
                    static_parts.title = title;
                }
            }
            "assistant" => {
                if static_parts.model.is_none() {
                    static_parts.model = value
                        .get("message")
                        .and_then(|message| message.get("model"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
            }
            _ => {}
        }
    }

    Ok(static_parts)
}

fn extract_claude_user_text(message: &Value) -> Option<String> {
    match message.get("content")? {
        Value::String(text) => Some(text.to_string()),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n\n");
            (!text.trim().is_empty()).then_some(text)
        }
        _ => None,
    }
}

#[derive(Default)]
struct ToolResult {
    tool_use_id: Option<String>,
}

fn extract_tool_results(message: &Value) -> Vec<ToolResult> {
    message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"))
        .map(|item| ToolResult {
            tool_use_id: item
                .get("tool_use_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
        .collect()
}

fn compact_progress_summary(value: &Value) -> Option<String> {
    let data = value.get("data")?;
    let hook = data
        .get("hookEvent")
        .or_else(|| data.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let command = data
        .get("command")
        .and_then(Value::as_str)
        .map(normalize_single_line_text);

    if hook.is_empty() && command.is_none() {
        return None;
    }

    match command {
        Some(command) if !hook.is_empty() => Some(format!("{hook}  {command}")),
        Some(command) => Some(command),
        None => Some(hook.to_string()),
    }
}

fn remember_latest_timestamp(slot: &mut Option<DateTime<Utc>>, timestamp: DateTime<Utc>) {
    if slot.is_none_or(|current| timestamp > current) {
        *slot = Some(timestamp);
    }
}

fn record_message(
    hint: &mut TranscriptHint,
    recent_events: &mut VecDeque<ActivityEvent>,
    recent_conversation: &mut VecDeque<ActivityEvent>,
    timestamp: DateTime<Utc>,
    kind: ActivityKind,
    message: &str,
) {
    let full_message = normalize_message_text(message);
    let compact_message = compact_text(message);

    match kind {
        ActivityKind::User => hint.last_user_message = Some((timestamp, full_message.clone())),
        ActivityKind::Assistant => {
            hint.last_assistant_message = Some((timestamp, full_message.clone()))
        }
        _ => {}
    }

    push_recent_event(
        recent_events,
        ActivityEvent {
            timestamp,
            kind: kind.clone(),
            summary: compact_message,
        },
    );
    push_recent_conversation(
        recent_conversation,
        ActivityEvent {
            timestamp,
            kind,
            summary: full_message,
        },
    );
}

fn summarize_claude_tool_call(name: &str, input: &Value, cwd: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "bash" => {
            summarize_bash_tool(input, cwd).unwrap_or_else(|| "Bash  run shell command".to_string())
        }
        "read" => summarize_read_tool(input).unwrap_or_else(|| "Read  file".to_string()),
        "grep" => summarize_grep_tool(input).unwrap_or_else(|| "Grep  search".to_string()),
        "glob" => summarize_glob_tool(input).unwrap_or_else(|| "Glob  match files".to_string()),
        "edit" | "multiedit" | "write" => {
            summarize_file_tool(name, input).unwrap_or_else(|| format!("{name}  file change"))
        }
        _ => summarize_generic_tool(name, input),
    }
}

fn summarize_bash_tool(input: &Value, cwd: &str) -> Option<String> {
    let command = input
        .get("command")
        .and_then(Value::as_str)
        .map(normalize_single_line_text)?;
    Some(format!(
        "Bash  {}  in {}",
        truncate_chars(&command, 180),
        display_path(cwd)
    ))
}

fn summarize_read_tool(input: &Value) -> Option<String> {
    let path = input
        .get("file_path")
        .or_else(|| input.get("path"))
        .and_then(Value::as_str)?;
    let offset = input.get("offset").and_then(Value::as_i64);
    let limit = input.get("limit").and_then(Value::as_i64);
    let suffix = match (offset, limit) {
        (Some(offset), Some(limit)) => format!("  at {offset} +{limit}"),
        (Some(offset), None) => format!("  at {offset}"),
        _ => String::new(),
    };
    Some(format!("Read  {}{}", display_path(path), suffix))
}

fn summarize_grep_tool(input: &Value) -> Option<String> {
    let pattern = input.get("pattern").and_then(Value::as_str)?;
    let path = input
        .get("path")
        .or_else(|| input.get("file_path"))
        .and_then(Value::as_str);
    Some(match path {
        Some(path) => format!(
            "Grep  {}  in {}",
            truncate_chars(&normalize_single_line_text(pattern), 80),
            display_path(path)
        ),
        None => format!(
            "Grep  {}",
            truncate_chars(&normalize_single_line_text(pattern), 120)
        ),
    })
}

fn summarize_glob_tool(input: &Value) -> Option<String> {
    let pattern = input.get("pattern").and_then(Value::as_str)?;
    Some(format!(
        "Glob  {}",
        truncate_chars(&normalize_single_line_text(pattern), 140)
    ))
}

fn summarize_file_tool(name: &str, input: &Value) -> Option<String> {
    let path = input
        .get("file_path")
        .or_else(|| input.get("path"))
        .and_then(Value::as_str)?;
    Some(format!("{name}  {}", display_path(path)))
}

fn summarize_generic_tool(name: &str, input: &Value) -> String {
    let input_preview = input
        .as_object()
        .and_then(|object| object.iter().next())
        .map(|(key, value)| {
            let value = match value {
                Value::String(text) => truncate_chars(&normalize_single_line_text(text), 96),
                _ => truncate_chars(&normalize_single_line_text(&value.to_string()), 96),
            };
            format!("{key}={value}")
        });

    match input_preview {
        Some(preview) => format!("{name}  {preview}"),
        None => format!("Called `{name}`"),
    }
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
        || lowercase.contains("what should i ")
        || lowercase.contains("what should we ")
        || lowercase.contains("what should happen next")
}

fn is_exploration_tool(summary: &str) -> bool {
    let lowered = summary.to_ascii_lowercase();
    ["bash", "read", "grep", "glob"]
        .iter()
        .any(|needle| lowered.starts_with(needle))
}

fn push_recent_event(events: &mut VecDeque<ActivityEvent>, event: ActivityEvent) {
    if events.len() == RECENT_EVENT_LIMIT {
        events.pop_front();
    }
    events.push_back(event);
}

fn push_recent_conversation(events: &mut VecDeque<ActivityEvent>, event: ActivityEvent) {
    if let Some(previous) = events.back_mut()
        && previous.kind == event.kind
        && previous.summary == event.summary
    {
        previous.timestamp = event.timestamp;
        return;
    }
    if events.len() == RECENT_CONVERSATION_LIMIT {
        events.pop_front();
    }
    events.push_back(event);
}

fn local_date_key(timestamp: DateTime<Utc>) -> String {
    timestamp
        .with_timezone(&Local)
        .format("%Y-%m-%d")
        .to_string()
}

fn local_hour_key(timestamp: DateTime<Utc>) -> String {
    timestamp
        .with_timezone(&Local)
        .format("%Y-%m-%dT%H")
        .to_string()
}

fn user_title_candidate(message: &str) -> Option<String> {
    let trimmed = message.trim();
    if trimmed.is_empty() || trimmed.starts_with("<environment_context>") {
        return None;
    }
    let title = normalize_title(trimmed);
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

fn normalize_message_text(text: &str) -> String {
    let text = normalize_claude_markup(text);
    let mut lines = Vec::new();
    let mut previous_blank = false;

    for raw_line in collapse_markdown_links(&text).lines() {
        let normalized = raw_line
            .split_whitespace()
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join(" ");

        if normalized.is_empty() {
            if !lines.is_empty() && !previous_blank {
                lines.push(String::new());
            }
            previous_blank = true;
            continue;
        }

        lines.push(normalized);
        previous_blank = false;
    }

    if lines.is_empty() {
        String::new()
    } else {
        lines.join("\n")
    }
}

fn compact_text(text: &str) -> String {
    let text = normalize_single_line_text(text);
    truncate_chars(&text, 140)
}

fn normalize_single_line_text(text: &str) -> String {
    let normalized = normalize_claude_markup(text);
    collapse_markdown_links(&normalized)
        .split_whitespace()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_title(text: &str) -> String {
    normalize_single_line_text(text)
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
        let Some(target_end) = after_label.find(')') else {
            output.push_str(&rest[start..]);
            return output;
        };

        output.push_str(label);
        rest = &after_label[target_end + 1..];
    }

    output.push_str(rest);
    output
}

fn normalize_claude_markup(text: &str) -> String {
    if let Some(args) = extract_xml_tag(text, "command-args").filter(|args| !args.trim().is_empty())
    {
        return args.trim().to_string();
    }

    if let Some(message) =
        extract_xml_tag(text, "command-message").filter(|msg| !msg.trim().is_empty())
    {
        return message.trim().to_string();
    }

    strip_xml_tag(text, "local-command-caveat")
}

fn extract_xml_tag(text: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)?;
    let rest = &text[start + open.len()..];
    let end = rest.find(&close)?;
    Some(rest[..end].to_string())
}

fn strip_xml_tag(text: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut output = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find(&open) {
        output.push_str(&rest[..start]);
        let after_open = &rest[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            return output;
        };
        rest = &after_open[end + close.len()..];
    }

    output.push_str(rest);
    output
}

fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_string()
    } else {
        text.chars()
            .take(limit.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

fn display_path(path: &str) -> String {
    std::env::var("HOME")
        .ok()
        .and_then(|home| path.strip_prefix(&home).map(|suffix| format!("~{suffix}")))
        .unwrap_or_else(|| path.to_string())
}

fn expand_known_path_vars(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    path
}

fn normalize_claude_home(path: PathBuf) -> PathBuf {
    if path.file_name().and_then(|name| name.to_str()) == Some("projects") {
        path.parent().map(Path::to_path_buf).unwrap_or(path)
    } else {
        path
    }
}

fn claude_projects_dir(path: &Path) -> PathBuf {
    if path.file_name().and_then(|name| name.to_str()) == Some("projects") {
        path.to_path_buf()
    } else {
        path.join("projects")
    }
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn epoch_millis_to_utc(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
}

fn transcript_cache_signature(path: &Path) -> Result<(i64, u64)> {
    let metadata = path
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let modified_at_epoch_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();

    Ok((modified_at_epoch_ms, metadata.len()))
}

fn read_transcript_lines(path: &Path, mode: ReadMode) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let line_limit = match mode {
        ReadMode::Summary => SUMMARY_TAIL_LINES,
        ReadMode::Detail => DETAIL_TAIL_LINES,
    };

    let metadata = path
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let file_size = metadata.len();
    if matches!(mode, ReadMode::Summary) || file_size > 2_000_000 {
        return tail_lines(path, line_limit);
    }

    let mut text = String::new();
    File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .read_to_string(&mut text)
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(text.lines().map(ToOwned::to_owned).collect())
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

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    path.metadata()
        .ok()?
        .modified()
        .ok()
        .map(DateTime::<Utc>::from)
}

fn load_summary_cache_from_disk() -> Result<HashMap<PathBuf, SummaryHintCacheEntry>> {
    let Some(path) = summary_cache_path() else {
        return Ok(HashMap::new());
    };
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read summary cache {}", path.display()))?;
    let parsed: SummaryHintCacheFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse summary cache {}", path.display()))?;

    Ok(parsed
        .entries
        .into_iter()
        .map(|record| (PathBuf::from(record.path), record.entry))
        .collect())
}

fn store_summary_cache_to_disk(cache: &HashMap<PathBuf, SummaryHintCacheEntry>) -> Result<()> {
    let Some(path) = summary_cache_path() else {
        return Ok(());
    };

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let payload = SummaryHintCacheFile {
        entries: cache
            .iter()
            .map(|(path, entry)| SummaryHintCacheRecord {
                path: path.to_string_lossy().into_owned(),
                entry: entry.clone(),
            })
            .collect(),
    };

    let raw = serde_json::to_string(&payload)
        .with_context(|| format!("failed to serialize summary cache {}", path.display()))?;
    fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn pricing_cache_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    Some(
        base_dirs
            .cache_dir()
            .join("cow-watch")
            .join("litellm_pricing.json"),
    )
}

fn summary_cache_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    Some(
        base_dirs
            .cache_dir()
            .join("cow-watch")
            .join("claude_summary_hints.json"),
    )
}

fn latest_claude_backup_path(claude_home: &Path) -> Option<PathBuf> {
    let backups_dir = claude_home.join("backups");
    let entries = fs::read_dir(backups_dir).ok()?;
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            if !name.starts_with(".claude.json.backup.") {
                return None;
            }
            let sort_key = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            Some((sort_key, path))
        })
        .max_by_key(|(sort_key, _)| *sort_key)
        .map(|(_, path)| path)
}

fn load_latest_claude_account_snapshot(claude_home: &Path) -> Option<ClaudeAccountSnapshot> {
    let path = latest_claude_backup_path(claude_home)?;
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn load_pricing_cache_from_disk() -> Result<Option<LitellmPricingCache>> {
    let Some(path) = pricing_cache_path() else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read pricing cache {}", path.display()))?;
    let parsed: LitellmPricingCacheFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse pricing cache {}", path.display()))?;
    Ok(Some(LitellmPricingCache {
        fetched_at_epoch_ms: parsed.fetched_at_epoch_ms,
        entries: Arc::new(parsed.entries),
    }))
}

fn store_pricing_cache_to_disk(cache: &LitellmPricingCache) -> Result<()> {
    let Some(path) = pricing_cache_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let payload = LitellmPricingCacheFile {
        fetched_at_epoch_ms: cache.fetched_at_epoch_ms,
        entries: (*cache.entries).clone(),
    };
    let raw = serde_json::to_string(&payload)
        .with_context(|| format!("failed to serialize pricing cache {}", path.display()))?;
    fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn parse_litellm_pricing_map(value: &Value) -> Option<LitellmPricingMap> {
    let object = value.as_object()?;
    let mut entries = LitellmPricingMap::new();

    for (model, entry) in object {
        let Some(entry_object) = entry.as_object() else {
            continue;
        };
        entries.insert(
            model.clone(),
            LitellmPricingEntry {
                input_cost_per_token: entry_object
                    .get("input_cost_per_token")
                    .and_then(Value::as_f64),
                cache_read_input_token_cost: entry_object
                    .get("cache_read_input_token_cost")
                    .and_then(Value::as_f64),
                output_cost_per_token: entry_object
                    .get("output_cost_per_token")
                    .and_then(Value::as_f64),
                max_input_tokens: entry_object.get("max_input_tokens").and_then(Value::as_u64),
                max_tokens: entry_object.get("max_tokens").and_then(Value::as_u64),
            },
        );
    }

    (!entries.is_empty()).then_some(entries)
}

fn is_pricing_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= LITELLM_PRICING_CACHE_TTL.as_millis() as i64
}

fn is_sessions_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= SESSION_DISCOVERY_CACHE_TTL.as_millis() as i64
}

fn now_epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        TranscriptHint, analyze_transcript, derive_activity_state, looks_like_compaction_signal,
        normalize_message_text, normalize_title,
    };
    use chrono::Utc;
    use cow_watch_core::{
        ActivityEvent, ActivityKind, SessionActivityState, SessionStatus, SessionStatusKind,
        StatusConfidence,
    };
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn normalize_title_prefers_command_args() {
        let raw = "<command-message>review</command-message> <command-name>/review</command-name> <command-args>review the power-controller in git diff</command-args>";
        assert_eq!(
            normalize_title(raw),
            "review the power-controller in git diff"
        );
    }

    #[test]
    fn normalize_message_text_strips_local_command_caveat() {
        let raw = "<local-command-caveat>Caveat text</local-command-caveat>\nreal message";
        assert_eq!(normalize_message_text(raw), "real message");
    }

    #[test]
    fn compaction_signal_detection_is_specific() {
        assert!(looks_like_compaction_signal("Context compacted"));
        assert!(looks_like_compaction_signal("Compacting conversation"));
        assert!(!looks_like_compaction_signal(
            "Keep context pressure in view and spot compaction early"
        ));
    }

    #[test]
    fn derive_activity_state_needs_explicit_recent_compaction_signal() {
        let now = Utc::now();
        let status = SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: "running".to_string(),
        };
        let hint = TranscriptHint {
            run_active: true,
            latest_thinking_at: Some(now - chrono::Duration::seconds(2)),
            recent_events: vec![ActivityEvent {
                timestamp: now - chrono::Duration::seconds(2),
                kind: ActivityKind::System,
                summary: "thinking".to_string(),
            }],
            ..TranscriptHint::default()
        };

        let activity = derive_activity_state(
            &status,
            &hint,
            None,
            now - chrono::Duration::seconds(2),
            now,
        );

        assert_eq!(activity, SessionActivityState::Thinking);
    }

    #[test]
    fn derive_activity_state_compacts_only_while_signal_is_latest() {
        let now = Utc::now();
        let status = SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: "running".to_string(),
        };
        let compacted_at = now - chrono::Duration::seconds(2);
        let compacting_hint = TranscriptHint {
            run_active: true,
            latest_thinking_at: Some(compacted_at),
            recent_compaction_at: Some(compacted_at),
            recent_events: vec![ActivityEvent {
                timestamp: compacted_at,
                kind: ActivityKind::System,
                summary: "Context compacted".to_string(),
            }],
            ..TranscriptHint::default()
        };

        let compacting = derive_activity_state(
            &status,
            &compacting_hint,
            None,
            now - chrono::Duration::seconds(2),
            now,
        );
        assert_eq!(compacting, SessionActivityState::Compacting);

        let thinking_hint = TranscriptHint {
            recent_events: vec![
                ActivityEvent {
                    timestamp: compacted_at,
                    kind: ActivityKind::System,
                    summary: "Context compacted".to_string(),
                },
                ActivityEvent {
                    timestamp: now - chrono::Duration::seconds(1),
                    kind: ActivityKind::System,
                    summary: "Task started".to_string(),
                },
            ],
            ..compacting_hint
        };

        let thinking = derive_activity_state(
            &status,
            &thinking_hint,
            None,
            now - chrono::Duration::seconds(1),
            now,
        );
        assert_eq!(thinking, SessionActivityState::Thinking);
    }

    #[test]
    fn analyze_transcript_tracks_recent_compaction_signal() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-claude-compaction-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("session.jsonl");
        fs::write(
            &path,
            "{\"type\":\"progress\",\"timestamp\":\"2026-04-11T16:00:06.461Z\",\"data\":{\"hookEvent\":\"Context compacted\"}}\n",
        )
        .unwrap();

        let hint = analyze_transcript(&path, super::ReadMode::Summary).unwrap();

        assert_eq!(
            hint.recent_compaction_at,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-04-11T16:00:06.461Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
        assert!(
            hint.recent_events
                .iter()
                .any(|event| event.summary == "Context compacted")
        );

        fs::remove_dir_all(root).unwrap();
    }
}
