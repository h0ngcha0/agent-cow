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
    PricingSource, ProviderKind, ProviderQuota, QuotaWindow, SessionActivityState, SessionCost,
    SessionDetail, SessionList, SessionQuery, SessionSource, SessionStatus, SessionStatusKind,
    SessionSummary, StatusConfidence, TokenUsage, ToolCallStat, UsageOverview,
};
use directories::BaseDirs;
use reqwest::Client;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const SUMMARY_TAIL_LINES: usize = 384;
const DETAIL_TAIL_LINES: usize = 320;
const RECENT_EVENT_LIMIT: usize = 48;
const RECENT_CONVERSATION_LIMIT: usize = 32;
const RUNNING_TTL_SECONDS: i64 = 90;
const STALE_AFTER_MINUTES: i64 = 20;
const CODEX_DEFAULT_CONTEXT_WINDOW: u64 = 258_400;
const CODEX_AUTOCOMPACT_THRESHOLD: f64 = 0.835;
const LITELLM_PRICING_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
const LITELLM_PRICING_CACHE_TTL: StdDuration = StdDuration::from_secs(24 * 60 * 60);
const THREAD_DISCOVERY_CACHE_TTL: StdDuration = StdDuration::from_secs(5);

#[derive(Clone, Debug)]
pub struct CodexSource {
    configured_path: PathBuf,
    codex_home: PathBuf,
    machine_id: String,
    machine_label: String,
    pricing_client: Client,
    pricing_cache: Arc<Mutex<Option<LitellmPricingCache>>>,
    pricing_refresh_in_flight: Arc<AtomicBool>,
    static_cache: Arc<Mutex<HashMap<PathBuf, TranscriptStatic>>>,
    summary_cache: Arc<Mutex<HashMap<PathBuf, SummaryHintCacheEntry>>>,
    summary_cache_dirty: Arc<AtomicBool>,
    threads_cache: Arc<Mutex<Option<ThreadsCache>>>,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct RolloutHint {
    active_turns: usize,
    pending_call_ids: HashSet<String>,
    pending_call_names: HashMap<String, String>,
    run_started_at: Option<DateTime<Utc>>,
    run_active: bool,
    cumulative_token_usage: Option<TokenUsage>,
    cost_by_day: HashMap<String, RawCostBucket>,
    cost_by_hour: HashMap<String, RawCostBucket>,
    context_window: Option<ContextWindowUsage>,
    quota: Option<ProviderQuota>,
    last_user_message: Option<(DateTime<Utc>, String)>,
    last_assistant_message: Option<(DateTime<Utc>, String)>,
    recent_events: Vec<ActivityEvent>,
    recent_conversation: Vec<ActivityEvent>,
    tool_stats: HashMap<String, ToolCallStat>,
}

#[derive(Clone, Debug)]
struct CodexPricing {
    input_per_million: f64,
    cached_input_per_million: f64,
    output_per_million: f64,
    source: PricingSource,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RawCostBucket {
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
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
    hint: RolloutHint,
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

#[derive(Default)]
struct UsageIndex {
    cumulative_token_usage: Option<TokenUsage>,
    cost_by_day: HashMap<String, RawCostBucket>,
    cost_by_hour: HashMap<String, RawCostBucket>,
}

#[derive(Clone, Debug)]
struct ThreadsCache {
    fetched_at_epoch_ms: i64,
    rows: Vec<ThreadRow>,
}

impl CodexSource {
    pub fn new(codex_home: impl Into<PathBuf>) -> Self {
        let machine_label = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "local".to_string());
        let configured_path = expand_known_path_vars(codex_home.into());
        let codex_home = normalize_codex_home(configured_path.clone());
        let summary_cache = load_summary_cache_from_disk().unwrap_or_default();

        Self {
            configured_path,
            codex_home,
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
            threads_cache: Arc::new(Mutex::new(None)),
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
        if let Ok(cache) = self.threads_cache.lock()
            && let Some(cache) = cache.as_ref()
            && is_threads_cache_fresh(cache.fetched_at_epoch_ms)
        {
            return Ok(cache.rows.clone());
        }

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

        if let Ok(mut cache) = self.threads_cache.lock() {
            *cache = Some(ThreadsCache {
                fetched_at_epoch_ms: now_epoch_millis(),
                rows: rows.clone(),
            });
        }

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

    fn summary_hint(&self, path: &Path) -> Option<RolloutHint> {
        let (modified_at_epoch_ms, file_len) = rollout_cache_signature(path).ok()?;

        if let Ok(cache) = self.summary_cache.lock()
            && let Some(entry) = cache.get(path)
            && entry.modified_at_epoch_ms == modified_at_epoch_ms
            && entry.file_len == file_len
        {
            return Some(entry.hint.clone());
        }

        let hint = analyze_rollout(path, ReadMode::Summary).ok()?;

        if let Ok(mut cache) = self.summary_cache.lock() {
            cache.insert(
                path.to_path_buf(),
                SummaryHintCacheEntry {
                    modified_at_epoch_ms,
                    file_len,
                    hint: hint.clone(),
                },
            );
        }
        self.summary_cache_dirty.store(true, Ordering::SeqCst);

        Some(hint)
    }

    fn persist_summary_cache_if_dirty(&self) {
        if !self.summary_cache_dirty.load(Ordering::SeqCst) {
            return;
        }

        let snapshot = if let Ok(cache) = self.summary_cache.lock() {
            cache.clone()
        } else {
            return;
        };

        if store_summary_cache_to_disk(&snapshot).is_ok() {
            self.summary_cache_dirty.store(false, Ordering::SeqCst);
        }
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

    async fn litellm_pricing(&self) -> Option<Arc<LitellmPricingMap>> {
        if let Some(entries) = self.cached_pricing_entries(true) {
            return Some(entries);
        }

        let disk_cache = self.load_pricing_cache_from_disk();
        if let Some(cache) = &disk_cache {
            self.replace_pricing_cache(cache.clone());
            if !is_pricing_cache_fresh(cache.fetched_at_epoch_ms) {
                self.refresh_pricing_cache_in_background();
            }
            return Some(cache.entries.clone());
        }

        if let Some(entries) = self.cached_pricing_entries(false) {
            self.refresh_pricing_cache_in_background();
            return Some(entries);
        }

        self.refresh_pricing_cache_in_background();
        None
    }

    fn cached_pricing_entries(&self, require_fresh: bool) -> Option<Arc<LitellmPricingMap>> {
        let guard = self.pricing_cache.lock().ok()?;
        let cache = guard.as_ref()?;
        if require_fresh && !is_pricing_cache_fresh(cache.fetched_at_epoch_ms) {
            return None;
        }
        Some(cache.entries.clone())
    }

    fn replace_pricing_cache(&self, cache: LitellmPricingCache) {
        if let Ok(mut guard) = self.pricing_cache.lock() {
            *guard = Some(cache);
        }
    }

    fn refresh_pricing_cache_in_background(&self) {
        if self
            .pricing_refresh_in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let source = self.clone();
        tokio::spawn(async move {
            if let Some(cache) = source.fetch_pricing_cache().await {
                source.replace_pricing_cache(cache.clone());
                source.store_pricing_cache_to_disk(&cache);
            }
            source
                .pricing_refresh_in_flight
                .store(false, Ordering::SeqCst);
        });
    }

    async fn fetch_pricing_cache(&self) -> Option<LitellmPricingCache> {
        let response = self
            .pricing_client
            .get(LITELLM_PRICING_URL)
            .send()
            .await
            .ok()?;
        let response = response.error_for_status().ok()?;
        let payload = response.json::<Value>().await.ok()?;
        let entries = parse_litellm_pricing_map(&payload)?;

        Some(LitellmPricingCache {
            fetched_at_epoch_ms: now_epoch_millis(),
            entries: Arc::new(entries),
        })
    }

    fn load_pricing_cache_from_disk(&self) -> Option<LitellmPricingCache> {
        let path = pricing_cache_path()?;
        let raw = fs::read_to_string(path).ok()?;
        let parsed = serde_json::from_str::<LitellmPricingCacheFile>(&raw).ok()?;
        Some(LitellmPricingCache {
            fetched_at_epoch_ms: parsed.fetched_at_epoch_ms,
            entries: Arc::new(parsed.entries),
        })
    }

    fn store_pricing_cache_to_disk(&self, cache: &LitellmPricingCache) {
        let Some(path) = pricing_cache_path() else {
            return;
        };

        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        let payload = LitellmPricingCacheFile {
            fetched_at_epoch_ms: cache.fetched_at_epoch_ms,
            entries: (*cache.entries).clone(),
        };

        if let Ok(raw) = serde_json::to_string(&payload) {
            let _ = fs::write(path, raw);
        }
    }
}

#[async_trait]
impl SessionSource for CodexSource {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        let now = Utc::now();
        let mut sessions = Vec::new();
        let mut latest_quota: Option<(DateTime<Utc>, ProviderQuota)> = None;
        let litellm_pricing = self.litellm_pricing().await;

        for row in self.load_threads()? {
            if !query.include_archived && row.archived {
                continue;
            }

            let hint = if row.archived {
                None
            } else {
                self.summary_hint(Path::new(&row.rollout_path))
            };

            let summary = build_summary(
                &self.machine_id,
                &self.machine_label,
                &row,
                hint.as_ref(),
                litellm_pricing.as_deref(),
                now,
            );

            if let Some(quota) = hint.as_ref().and_then(|hint| hint.quota.clone()) {
                let should_replace = latest_quota
                    .as_ref()
                    .is_none_or(|(updated_at, _)| row.updated_at >= *updated_at);
                if should_replace {
                    latest_quota = Some((row.updated_at, quota));
                }
            }

            sessions.push(summary);

            if let Some(limit) = query.limit
                && sessions.len() >= limit
            {
                break;
            }
        }

        self.persist_summary_cache_if_dirty();

        Ok(SessionList {
            generated_at: now,
            overview: build_overview(&sessions, latest_quota.map(|(_, quota)| quota)),
            sessions,
        })
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let now = Utc::now();
        let row = self.thread_by_id(id)?;
        let hint = analyze_rollout(&PathBuf::from(&row.rollout_path), ReadMode::Detail)?;
        let litellm_pricing = self.litellm_pricing().await;
        let summary = build_summary(
            &self.machine_id,
            &self.machine_label,
            &row,
            Some(&hint),
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
    litellm_pricing: Option<&LitellmPricingMap>,
    now: DateTime<Utc>,
) -> SessionSummary {
    let mut tokens = hint
        .and_then(|hint| hint.cumulative_token_usage.clone())
        .unwrap_or_default();
    tokens.total_tokens = tokens.total_tokens.max(row.tokens_used);

    let pricing = row
        .model
        .as_deref()
        .and_then(|model| resolve_codex_pricing(model, litellm_pricing));
    let cost = pricing.as_ref().and_then(|pricing| {
        estimate_session_cost(
            &tokens,
            pricing,
            hint.map(|hint| current_hour_cost_usd(&hint.cost_by_hour, pricing, now))
                .unwrap_or(0.0),
            hint.map(|hint| current_day_cost_usd(&hint.cost_by_day, pricing, now))
                .unwrap_or(0.0),
        )
    });
    let status = derive_status(row, hint, now);
    let activity_state = derive_activity_state(&status, hint, now);
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
        run_started_at: hint.and_then(|hint| hint.run_started_at),
        run_active: hint.is_some_and(|hint| hint.run_active),
        archived: row.archived,
        model: row.model.clone(),
        agent_role: row.agent_role.clone(),
        git_branch: row.git_branch.clone(),
        git_origin_url: row.git_origin_url.clone(),
        tokens,
        cost,
        context_window: hint.and_then(|hint| hint.context_window.clone()),
        status,
        activity_state,
        rollout_path,
        navigation,
    }
}

fn derive_activity_state(
    status: &SessionStatus,
    hint: Option<&RolloutHint>,
    now: DateTime<Utc>,
) -> SessionActivityState {
    if matches!(status.kind, SessionStatusKind::WaitingInput) {
        return SessionActivityState::Waiting;
    }

    let Some(hint) = hint else {
        return SessionActivityState::Idle;
    };

    let has_live_signal =
        hint.run_active || hint.active_turns > 0 || !hint.pending_call_ids.is_empty();
    if !has_live_signal {
        return SessionActivityState::Idle;
    }

    let recent_window = Duration::seconds(8);
    let has_recent_feedback = hint
        .recent_events
        .iter()
        .rev()
        .find(|event| !matches!(event.kind, ActivityKind::User))
        .is_some_and(|event| now - event.timestamp <= recent_window);

    if !has_recent_feedback {
        return SessionActivityState::Idle;
    }

    if hint
        .context_window
        .as_ref()
        .is_some_and(|context| context.used_percent >= 100)
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

    SessionActivityState::Thinking
}

fn is_exploration_tool(summary: &str) -> bool {
    let lowered = format!(" {summary} ").to_ascii_lowercase();
    if !lowered.contains("exec_command") {
        return false;
    }

    [
        " rg ",
        " rg -",
        " sed ",
        " cat ",
        " ls ",
        " find ",
        " head ",
        " tail ",
        " wc ",
        " nl ",
        " git diff",
        " git show",
        " git status",
        " grep ",
        " jq ",
        " fd ",
        " tree ",
        " stat ",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
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

fn build_overview(sessions: &[SessionSummary], quota: Option<ProviderQuota>) -> UsageOverview {
    let total_tokens = sessions
        .iter()
        .map(|session| session.tokens.total_tokens)
        .sum();
    let total_cost_usd = sessions
        .iter()
        .filter_map(|session| session.cost.as_ref().map(|cost| cost.total_usd))
        .sum();
    let sessions_with_cost = sessions
        .iter()
        .filter(|session| session.cost.is_some())
        .count();
    let sessions_with_context = sessions
        .iter()
        .filter(|session| session.context_window.is_some())
        .count();

    UsageOverview {
        total_tokens,
        total_cost_usd,
        sessions_with_cost,
        sessions_with_context,
        quotas: quota.into_iter().collect(),
    }
}

fn resolve_codex_pricing(
    model: &str,
    litellm_pricing: Option<&LitellmPricingMap>,
) -> Option<CodexPricing> {
    if let Some(entry) = litellm_pricing.and_then(|entries| find_litellm_entry(model, entries))
        && let Some(pricing) = litellm_to_codex_pricing(entry)
    {
        return Some(pricing);
    }

    match model.to_ascii_lowercase().as_str() {
        "gpt-5.4" => Some(CodexPricing {
            input_per_million: 2.5,
            cached_input_per_million: 0.25,
            output_per_million: 15.0,
            source: PricingSource::BuiltIn,
        }),
        "gpt-5.4-mini" => Some(CodexPricing {
            input_per_million: 0.75,
            cached_input_per_million: 0.075,
            output_per_million: 4.5,
            source: PricingSource::BuiltIn,
        }),
        "gpt-5.3-codex" => Some(CodexPricing {
            input_per_million: 1.75,
            cached_input_per_million: 0.175,
            output_per_million: 14.0,
            source: PricingSource::BuiltIn,
        }),
        "codex-mini-latest" => Some(CodexPricing {
            input_per_million: 1.5,
            cached_input_per_million: 0.375,
            output_per_million: 6.0,
            source: PricingSource::BuiltIn,
        }),
        _ => None,
    }
}

fn find_litellm_entry<'a>(
    model: &str,
    entries: &'a LitellmPricingMap,
) -> Option<&'a LitellmPricingEntry> {
    entries
        .get(model)
        .or_else(|| entries.get(&format!("openai/{model}")))
        .or_else(|| {
            entries
                .iter()
                .find(|(key, _)| key.contains(model))
                .map(|(_, value)| value)
        })
}

fn litellm_to_codex_pricing(entry: &LitellmPricingEntry) -> Option<CodexPricing> {
    Some(CodexPricing {
        input_per_million: entry.input_cost_per_token? * 1_000_000.0,
        cached_input_per_million: entry.cache_read_input_token_cost.unwrap_or(0.0) * 1_000_000.0,
        output_per_million: entry.output_cost_per_token? * 1_000_000.0,
        source: PricingSource::LiteLlm,
    })
}

fn estimate_session_cost(
    tokens: &TokenUsage,
    pricing: &CodexPricing,
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

fn estimate_raw_cost_bucket(bucket: &RawCostBucket, pricing: &CodexPricing) -> f64 {
    let uncached_input_tokens = bucket
        .input_tokens
        .saturating_sub(bucket.cached_input_tokens);
    token_cost(uncached_input_tokens, pricing.input_per_million)
        + token_cost(bucket.cached_input_tokens, pricing.cached_input_per_million)
        + token_cost(bucket.output_tokens, pricing.output_per_million)
}

fn current_hour_cost_usd(
    buckets: &HashMap<String, RawCostBucket>,
    pricing: &CodexPricing,
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
    pricing: &CodexPricing,
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
    let usage_index = stream_usage_index(path)?;
    let mut hint = RolloutHint {
        cumulative_token_usage: usage_index.cumulative_token_usage,
        cost_by_day: usage_index.cost_by_day,
        cost_by_hour: usage_index.cost_by_hour,
        ..RolloutHint::default()
    };
    let mut recent_events = VecDeque::with_capacity(RECENT_EVENT_LIMIT);
    let mut recent_conversation = VecDeque::with_capacity(RECENT_CONVERSATION_LIMIT);
    let mut open_turns = HashMap::<String, DateTime<Utc>>::new();
    let mut unnamed_open_turns = Vec::<DateTime<Utc>>::new();
    let mut latest_tool_call_at = None;
    let mut latest_turn_id = None::<String>;
    let mut latest_turn_first_seen_at = None;

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

        if let (Some(timestamp), Some(turn_id)) = (timestamp, extract_turn_id(payload)) {
            match latest_turn_id.as_deref() {
                Some(current_turn_id) if current_turn_id == turn_id => {}
                _ => {
                    latest_turn_id = Some(turn_id.to_string());
                    latest_turn_first_seen_at = Some(timestamp);
                }
            }
        }

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
                            remember_latest_timestamp(&mut hint.run_started_at, timestamp);
                            if let Some(turn_id) = payload
                                .get("turn_id")
                                .and_then(Value::as_str)
                                .filter(|turn_id| !turn_id.is_empty())
                            {
                                open_turns.insert(turn_id.to_string(), timestamp);
                            } else {
                                unnamed_open_turns.push(timestamp);
                            }
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
                        if let Some(turn_id) = payload
                            .get("turn_id")
                            .and_then(Value::as_str)
                            .filter(|turn_id| !turn_id.is_empty())
                        {
                            open_turns.remove(turn_id);
                        } else {
                            unnamed_open_turns.pop();
                        }
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
                        if let Some(info) = payload.get("info")
                            && let Some(context_window) = parse_context_window_usage(info)
                        {
                            hint.context_window = Some(context_window);
                        }

                        if let Some(quota) = payload.get("rate_limits").and_then(parse_codex_quota)
                        {
                            hint.quota = Some(quota);
                        }
                    }
                    "agent_message" => {
                        if let (Some(timestamp), Some(message)) =
                            (timestamp, payload.get("message").and_then(Value::as_str))
                        {
                            record_message(
                                &mut hint,
                                &mut recent_events,
                                &mut recent_conversation,
                                timestamp,
                                ActivityKind::Assistant,
                                message,
                            );
                        }
                    }
                    "user_message" => {
                        if let (Some(timestamp), Some(message)) =
                            (timestamp, payload.get("message").and_then(Value::as_str))
                        {
                            remember_latest_timestamp(&mut hint.run_started_at, timestamp);
                            record_message(
                                &mut hint,
                                &mut recent_events,
                                &mut recent_conversation,
                                timestamp,
                                ActivityKind::User,
                                message,
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
                            remember_latest_timestamp(&mut latest_tool_call_at, timestamp);
                            push_recent_event(
                                &mut recent_events,
                                ActivityEvent {
                                    timestamp,
                                    kind: ActivityKind::ToolCall,
                                    summary: summarize_tool_call(
                                        &name,
                                        payload.get("arguments").and_then(Value::as_str),
                                    ),
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
                    _ => {}
                }
            }
            _ => {}
        }
    }

    let latest_open_turn_started_at = open_turns
        .values()
        .copied()
        .chain(unnamed_open_turns.iter().copied())
        .max();
    hint.run_active = latest_open_turn_started_at.is_some() || !hint.pending_call_ids.is_empty();
    if hint.run_started_at.is_none() {
        hint.run_started_at = latest_turn_first_seen_at
            .or(latest_open_turn_started_at)
            .or_else(|| {
                (!hint.pending_call_ids.is_empty())
                    .then_some(latest_tool_call_at)
                    .flatten()
            });
    }

    hint.recent_events = recent_events.into_iter().collect();
    hint.recent_conversation = recent_conversation.into_iter().collect();
    Ok(hint)
}

fn remember_latest_timestamp(slot: &mut Option<DateTime<Utc>>, timestamp: DateTime<Utc>) {
    if slot.is_none_or(|current| timestamp > current) {
        *slot = Some(timestamp);
    }
}

fn extract_turn_id(payload: &Value) -> Option<&str> {
    payload
        .get("turn_id")
        .and_then(Value::as_str)
        .filter(|turn_id| !turn_id.is_empty())
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
    total.reasoning_output_tokens = Some(
        total
            .reasoning_output_tokens
            .unwrap_or(0)
            .saturating_add(delta.reasoning_output_tokens.unwrap_or(0)),
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

fn stream_usage_index(path: &Path) -> Result<UsageIndex> {
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?,
    );
    let mut cumulative = TokenUsage::default();
    let mut saw_delta = false;
    let mut latest_total = None;
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
        if value.get("type").and_then(Value::as_str) != Some("event_msg") {
            continue;
        }
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if payload.get("type").and_then(Value::as_str) != Some("token_count") {
            continue;
        }
        let Some(info) = payload.get("info") else {
            continue;
        };

        if let Some(delta) = info.get("last_token_usage").and_then(parse_token_usage) {
            accumulate_token_usage(&mut cumulative, &delta);
            saw_delta = true;
            if let Some(timestamp) = value
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(parse_rfc3339)
            {
                add_raw_cost_bucket(&mut cost_by_day, local_date_key(timestamp), &delta);
                add_raw_cost_bucket(&mut cost_by_hour, local_hour_key(timestamp), &delta);
            }
        }

        if let Some(total) = info.get("total_token_usage").and_then(parse_token_usage) {
            latest_total = Some(total);
        }
    }

    Ok(UsageIndex {
        cumulative_token_usage: if saw_delta {
            Some(cumulative)
        } else {
            latest_total
        },
        cost_by_day,
        cost_by_hour,
    })
}

fn parse_context_window_usage(info: &Value) -> Option<ContextWindowUsage> {
    let used_tokens = info
        .get("last_token_usage")
        .or_else(|| info.get("total_token_usage"))
        .and_then(|value| value.get("input_tokens"))
        .and_then(Value::as_u64)?;
    let raw_limit_tokens = info
        .get("model_context_window")
        .and_then(Value::as_u64)
        .unwrap_or(CODEX_DEFAULT_CONTEXT_WINDOW);
    let limit_tokens = ((raw_limit_tokens as f64) * CODEX_AUTOCOMPACT_THRESHOLD).round() as u64;
    let remaining_tokens = limit_tokens.saturating_sub(used_tokens);
    let used_percent = if limit_tokens == 0 {
        0
    } else {
        ((used_tokens as f64 / limit_tokens as f64) * 100.0).round() as u16
    }
    .min(u8::MAX as u16) as u8;

    Some(ContextWindowUsage {
        used_tokens,
        limit_tokens: limit_tokens.max(1),
        remaining_tokens,
        used_percent,
    })
}

fn parse_codex_quota(value: &Value) -> Option<ProviderQuota> {
    let mut windows = Vec::new();

    if let Some(primary) = value
        .get("primary")
        .and_then(parse_quota_window)
        .or_else(|| value.get("primary_window").and_then(parse_quota_window))
    {
        windows.push(QuotaWindow {
            label: "5h".to_string(),
            ..primary
        });
    }

    if let Some(secondary) = value
        .get("secondary")
        .and_then(parse_quota_window)
        .or_else(|| value.get("secondary_window").and_then(parse_quota_window))
    {
        windows.push(QuotaWindow {
            label: "7d".to_string(),
            ..secondary
        });
    }

    if windows.is_empty() && value.get("plan_type").is_none() {
        return None;
    }

    Some(ProviderQuota {
        provider: ProviderKind::Codex,
        plan: value
            .get("plan_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        windows,
        limit_reached: value
            .get("limit_reached")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_quota_window(value: &Value) -> Option<QuotaWindow> {
    let used_percent = value
        .get("used_percent")
        .and_then(Value::as_f64)
        .map(|value| value.round() as u16)
        .or_else(|| {
            value
                .get("utilization")
                .and_then(Value::as_f64)
                .map(|value| (value * 100.0).round() as u16)
        })?
        .min(100) as u8;

    let reset_at = value
        .get("resets_at")
        .or_else(|| value.get("reset_at"))
        .and_then(parse_reset_at)
        .or_else(|| {
            value
                .get("resets_in_seconds")
                .and_then(Value::as_i64)
                .map(|seconds| Utc::now() + Duration::seconds(seconds))
        });
    let window_minutes = value
        .get("window_minutes")
        .and_then(Value::as_u64)
        .or_else(|| {
            value
                .get("limit_window_seconds")
                .and_then(Value::as_u64)
                .map(|seconds| seconds / 60)
        });

    Some(QuotaWindow {
        label: String::new(),
        used_percent,
        remaining_percent: 100_u8.saturating_sub(used_percent),
        reset_at,
        window_minutes,
    })
}

fn parse_reset_at(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(text) => parse_rfc3339(text),
        Value::Number(number) => {
            let seconds = number.as_i64()?;
            unix_timestamp_to_utc(seconds).ok()
        }
        _ => None,
    }
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
            .join("summary_hints.json"),
    )
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

fn is_pricing_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= LITELLM_PRICING_CACHE_TTL.as_millis() as i64
}

fn is_threads_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= THREAD_DISCOVERY_CACHE_TTL.as_millis() as i64
}

fn now_epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
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

fn record_message(
    hint: &mut RolloutHint,
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

fn summarize_tool_call(name: &str, arguments: Option<&str>) -> String {
    match name {
        "exec_command" => {
            summarize_exec_command(arguments).unwrap_or_else(|| format!("exec_command  {}", "run"))
        }
        "write_stdin" => summarize_write_stdin(arguments)
            .unwrap_or_else(|| "write_stdin  session input".to_string()),
        _ => format!("Called `{name}`"),
    }
}

fn summarize_exec_command(arguments: Option<&str>) -> Option<String> {
    let arguments = arguments?;
    let value: Value = serde_json::from_str(arguments).ok()?;
    let command = value
        .get("cmd")
        .and_then(Value::as_str)
        .map(normalize_single_line_text)
        .or_else(|| {
            value.get("command").and_then(Value::as_array).map(|parts| {
                parts
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
        })?;
    let command = truncate_chars(&command, 140);
    let workdir = value
        .get("workdir")
        .and_then(Value::as_str)
        .map(display_path)
        .unwrap_or_else(|| ".".to_string());
    Some(format!("exec_command  {command}  in {workdir}"))
}

fn summarize_write_stdin(arguments: Option<&str>) -> Option<String> {
    let arguments = arguments?;
    let value: Value = serde_json::from_str(arguments).ok()?;
    let session_id = value.get("session_id").and_then(Value::as_i64)?;
    let chars = value
        .get("chars")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let action = if chars.is_empty() {
        "poll".to_string()
    } else {
        let normalized = normalize_single_line_text(chars);
        if normalized.chars().count() <= 16 {
            format!("send {:?}", normalized)
        } else {
            format!("send {} chars", normalized.chars().count())
        }
    };
    Some(format!("write_stdin  {action}  session {session_id}"))
}

fn normalize_message_text(text: &str) -> String {
    let mut lines = Vec::new();
    let mut previous_blank = false;

    for raw_line in collapse_markdown_links(text).lines() {
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

fn normalize_single_line_text(text: &str) -> String {
    normalize_inline_text(text)
}

fn compact_text(text: &str) -> String {
    let text = normalize_single_line_text(text);

    truncate_chars(&text, 140)
}

fn display_path(path: &str) -> String {
    std::env::var("HOME")
        .ok()
        .and_then(|home| path.strip_prefix(&home).map(|suffix| format!("~{suffix}")))
        .unwrap_or_else(|| path.to_string())
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

fn rollout_cache_signature(path: &Path) -> Result<(i64, u64)> {
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

fn read_rollout_lines(path: &Path, mode: ReadMode) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let line_limit = match mode {
        ReadMode::Summary => SUMMARY_TAIL_LINES,
        ReadMode::Detail => DETAIL_TAIL_LINES,
    };

    if matches!(mode, ReadMode::Summary) {
        return tail_lines(path, line_limit);
    }

    let metadata = path
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let file_size = metadata.len();

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
        ReadMode, analyze_rollout, local_date_key, local_hour_key, looks_like_waiting_input,
        normalize_codex_home, normalize_title, parse_context_window_usage, parse_state_db_version,
        parse_transcript_static, state_db_candidates_for_input, stream_usage_index,
        user_title_candidate,
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

    #[test]
    fn analyze_rollout_tracks_latest_run_start_instead_of_thread_start() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-rollout-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path =
            root.join("rollout-2026-04-10T20-55-21-019d73f2-20fb-70f2-9ba5-13810d786a22.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-04-09T20:32:21.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"019d73f2-20fb-70f2-9ba5-13810d786a22\",\"timestamp\":\"2026-04-09T20:32:21.000Z\",\"cwd\":\"/tmp/project\"}}\n",
                "{\"timestamp\":\"2026-04-09T20:32:22.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"old-turn\"}}\n",
                "{\"timestamp\":\"2026-04-09T20:35:22.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"old-turn\"}}\n",
                "{\"timestamp\":\"2026-04-10T20:55:21.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"new-turn\"}}\n",
                "{\"timestamp\":\"2026-04-10T20:55:21.500Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"check duration\"}}\n",
                "{\"timestamp\":\"2026-04-10T20:56:00.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"exec_command\"}}\n"
            ),
        )
        .unwrap();

        let hint = analyze_rollout(&path, ReadMode::Summary).unwrap();
        let run_started_at = hint.run_started_at.unwrap();

        assert_eq!(run_started_at.to_rfc3339(), "2026-04-10T20:55:21.500+00:00");
        assert!(hint.run_active);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn analyze_rollout_prefers_latest_signal_over_older_open_turns() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-rollout-latest-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path =
            root.join("rollout-2026-04-10T20-59-43-019d73f2-20fb-70f2-9ba5-13810d786a22.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-04-10T20:59:43.761Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"old-open\"}}\n",
                "{\"timestamp\":\"2026-04-10T21:10:00.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call_old\",\"name\":\"exec_command\",\"turn_id\":\"old-open\"}}\n",
                "{\"timestamp\":\"2026-04-11T04:36:21.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"7h, it should be at most a few minutes\",\"turn_id\":\"new-turn\"}}\n",
                "{\"timestamp\":\"2026-04-11T04:36:47.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call_new\",\"name\":\"exec_command\",\"turn_id\":\"new-turn\"}}\n"
            ),
        )
        .unwrap();

        let hint = analyze_rollout(&path, ReadMode::Summary).unwrap();
        let run_started_at = hint.run_started_at.unwrap();

        assert_eq!(run_started_at.to_rfc3339(), "2026-04-11T04:36:21+00:00");
        assert!(hint.run_active);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn analyze_rollout_accumulates_lifetime_tokens_from_last_usage() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-rollout-tokens-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("rollout-2026-04-11T05-07-04-ctx.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-04-11T05:00:00.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":3,\"output_tokens\":2,\"reasoning_output_tokens\":1,\"total_tokens\":12},\"last_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":3,\"output_tokens\":2,\"reasoning_output_tokens\":1,\"total_tokens\":12},\"model_context_window\":258400}}}\n",
                "{\"timestamp\":\"2026-04-11T05:01:00.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":25,\"cached_input_tokens\":8,\"output_tokens\":5,\"reasoning_output_tokens\":1,\"total_tokens\":30},\"last_token_usage\":{\"input_tokens\":15,\"cached_input_tokens\":5,\"output_tokens\":3,\"reasoning_output_tokens\":0,\"total_tokens\":18},\"model_context_window\":258400}}}\n"
            ),
        )
        .unwrap();

        let hint = analyze_rollout(&path, ReadMode::Summary).unwrap();
        let tokens = hint.cumulative_token_usage.unwrap();

        assert_eq!(tokens.input_tokens, Some(25));
        assert_eq!(tokens.cached_input_tokens, Some(8));
        assert_eq!(tokens.output_tokens, Some(5));
        assert_eq!(tokens.reasoning_output_tokens, Some(1));
        assert_eq!(tokens.total_tokens, 30);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parse_context_window_usage_uses_autocompact_threshold() {
        let info = serde_json::json!({
            "last_token_usage": {
                "input_tokens": 222_238,
                "cached_input_tokens": 0,
                "output_tokens": 0,
                "reasoning_output_tokens": 0,
                "total_tokens": 222_238
            },
            "model_context_window": 258_400
        });

        let context = parse_context_window_usage(&info).unwrap();

        assert_eq!(context.limit_tokens, 215_764);
        assert_eq!(context.used_tokens, 222_238);
        assert_eq!(context.remaining_tokens, 0);
        assert_eq!(context.used_percent, 103);
    }

    #[test]
    fn stream_usage_index_tracks_current_hour_and_day_buckets() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("cow-watch-rollout-cost-buckets-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("rollout-2026-04-11T05-07-04-costs.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-04-11T10:05:00.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"last_token_usage\":{\"input_tokens\":1000,\"cached_input_tokens\":400,\"output_tokens\":20,\"reasoning_output_tokens\":0,\"total_tokens\":1020},\"total_token_usage\":{\"input_tokens\":1000,\"cached_input_tokens\":400,\"output_tokens\":20,\"reasoning_output_tokens\":0,\"total_tokens\":1020}}}}\n",
                "{\"timestamp\":\"2026-04-11T10:45:00.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"last_token_usage\":{\"input_tokens\":500,\"cached_input_tokens\":100,\"output_tokens\":10,\"reasoning_output_tokens\":0,\"total_tokens\":510},\"total_token_usage\":{\"input_tokens\":1500,\"cached_input_tokens\":500,\"output_tokens\":30,\"reasoning_output_tokens\":0,\"total_tokens\":1530}}}}\n"
            ),
        )
        .unwrap();

        let usage = stream_usage_index(&path).unwrap();
        let cumulative = usage.cumulative_token_usage.unwrap();
        let timestamp = chrono::DateTime::parse_from_rfc3339("2026-04-11T10:05:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let day_key = local_date_key(timestamp);
        let hour_key = local_hour_key(timestamp);

        assert_eq!(cumulative.total_tokens, 1530);
        assert_eq!(usage.cost_by_day.get(&day_key).unwrap().input_tokens, 1500);
        assert_eq!(
            usage.cost_by_day.get(&day_key).unwrap().cached_input_tokens,
            500
        );
        assert_eq!(usage.cost_by_day.get(&day_key).unwrap().output_tokens, 30);
        assert_eq!(
            usage.cost_by_hour.get(&hour_key).unwrap().input_tokens,
            1500
        );

        fs::remove_dir_all(root).unwrap();
    }
}
