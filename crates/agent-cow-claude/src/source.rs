use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_cow_core::{
    ActivityEvent, ActivityKind, ContextWindowUsage, NavigationKind, NavigationTarget,
    PricingSource, ProviderKind, ProviderQuota, SessionActivityState, SessionCost, SessionDetail,
    SessionList, SessionLoadProgress, SessionQuery, SessionSource, SessionStatus,
    SessionStatusKind, SessionSummary, StatusConfidence, TokenUsage, ToolCallStat, UsageOverview,
};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, NaiveTime, TimeZone, Utc};
use directories::BaseDirs;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

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
const CLAUDE_TIERED_THRESHOLD_TOKENS: u64 = 200_000;
const SESSION_DISCOVERY_CACHE_TTL: StdDuration = StdDuration::from_secs(15);
const SESSION_DISCOVERY_DISK_CACHE_TTL: StdDuration = StdDuration::from_secs(90);
const CLAUDE_STATUS_CACHE_TTL: StdDuration = StdDuration::from_secs(15 * 60);
const CLAUDE_STATUS_FAILURE_BACKOFF: StdDuration = StdDuration::from_secs(30 * 60);
const CLAUDE_STATUS_PROBE_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const CLAUDE_STATUS_PYTHON_PROBE: &str = r#"
import json, os, select, subprocess, time

proc = subprocess.Popen(
    ['script', '-q', '/dev/null', 'zsh', '-lc', 'claude'],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
)
fd = proc.stdout.fileno()
os.set_blocking(fd, False)

def read_for(seconds):
    end = time.time() + seconds
    chunks = []
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.1)
        if fd in r:
            try:
                data = os.read(fd, 65536)
            except BlockingIOError:
                continue
            if not data:
                break
            chunks.append(data)
    return b''.join(chunks)

def strip(data):
    plain = []
    i = 0
    while i < len(data):
        b = data[i]
        if b == 0x1b:
            i += 1
            if i >= len(data):
                break
            if data[i] == ord('['):
                params_start = i + 1
                i += 1
                cmd = None
                while i < len(data):
                    byte = data[i]
                    i += 1
                    if 0x40 <= byte <= 0x7e:
                        cmd = byte
                        break
                if cmd == ord('C'):
                    params = data[params_start:i-1].decode('utf-8', 'ignore')
                    count = int((params.split(';')[0] or '1'))
                    plain.extend(b' ' * count)
            elif data[i] == ord(']'):
                i += 1
                while i < len(data):
                    byte = data[i]
                    i += 1
                    if byte == 0x07:
                        break
                    if byte == 0x1b and i < len(data) and data[i] == ord('\\'):
                        i += 1
                        break
            else:
                i += 1
        elif b == ord('\r'):
            plain.append(ord('\n'))
            i += 1
        elif b == 0x08:
            i += 1
        elif b < 0x20 and b not in (ord('\n'), ord('\t')):
            i += 1
        else:
            plain.append(b)
            i += 1
    return bytes(plain).decode('utf-8', 'ignore').replace('\xa0', ' ')

def wait_for(needles, seconds):
    end = time.time() + seconds
    chunks = []
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.1)
        if fd in r:
            try:
                data = os.read(fd, 65536)
            except BlockingIOError:
                continue
            if not data:
                break
            chunks.append(data)
            joined = b''.join(chunks)
            text = strip(joined)
            if all(needle in text for needle in needles):
                break
    return b''.join(chunks)

def send(data):
    proc.stdin.write(data)
    proc.stdin.flush()

out = b''
out += read_for(2.0)
send(b'\r'); out += read_for(0.7)
send(b'/status\r'); out += read_for(1.5)
send(b'\t'); out += read_for(1.0)
send(b'\t'); out += wait_for(['Current week (all models)', '% used'], 8.0)
send(b'\x1b'); out += read_for(0.2)
send(b'/exit\r'); out += read_for(0.3)

try:
    proc.terminate()
except Exception:
    pass
try:
    proc.wait(timeout=1)
except Exception:
    try:
        proc.kill()
    except Exception:
        pass

lines = [' '.join(line.split()) for line in strip(out).splitlines() if line.strip()]

def parse_used_percent(line):
    if '% used' not in line:
        return None
    prefix = line.split('% used')[0]
    digits = ''
    for ch in reversed(prefix):
        if ch.isdigit():
            digits = ch + digits
        elif digits:
            break
    return int(digits) if digits else None

def parse_reset_text(line):
    if line.startswith('Resets '):
        return line[len('Resets '):].strip()
    if 'Europe/Stockholm' in line:
        return line.strip()
    return None

week_all_index = next((i for i in range(len(lines) - 1, -1, -1) if 'Current week (all models)' in lines[i]), None)
sonnet_index = next((i for i in range(len(lines) - 1, -1, -1) if 'Current week (Sonnet only)' in lines[i]), None)

def previous_block(before_index):
    if before_index is None:
        return None
    used_index = next((i for i in range(before_index - 1, -1, -1) if parse_used_percent(lines[i]) is not None), None)
    if used_index is None:
        return None
    reset = next((parse_reset_text(lines[i]) for i in range(used_index + 1, before_index) if parse_reset_text(lines[i]) is not None), None)
    return {'used_percent': parse_used_percent(lines[used_index]), 'reset_text': reset}

def next_block(title_index):
    if title_index is None:
        return None
    used_index = next((i for i in range(title_index + 1, len(lines)) if parse_used_percent(lines[i]) is not None), None)
    if used_index is None:
        return None
    reset = next((parse_reset_text(lines[i]) for i in range(used_index + 1, min(len(lines), used_index + 4)) if parse_reset_text(lines[i]) is not None), None)
    return {'used_percent': parse_used_percent(lines[used_index]), 'reset_text': reset}

login_method = next((line.split('Login method:', 1)[1].strip() for line in reversed(lines) if line.startswith('Login method:')), None)
if login_method and login_method.endswith(' Account'):
    login_method = login_method[:-8]
if login_method and login_method.startswith('Claude '):
    login_method = login_method[len('Claude '):]

windows = []
session = previous_block(week_all_index)
if session:
    session['label'] = 'CS'
    session['window_minutes'] = None
    windows.append(session)
week_all = next_block(week_all_index)
if week_all:
    week_all['label'] = '7D'
    week_all['window_minutes'] = 10080
    windows.append(week_all)
sonnet = next_block(sonnet_index)
if sonnet:
    sonnet['label'] = 'SO'
    sonnet['window_minutes'] = 10080
    windows.append(sonnet)

print(json.dumps({'plan': login_method, 'windows': windows}))
"#;
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
    status_refresh_in_flight: Arc<AtomicBool>,
    static_cache: Arc<Mutex<HashMap<PathBuf, TranscriptStatic>>>,
    summary_cache: Arc<Mutex<Option<HashMap<PathBuf, SummaryHintCacheEntry>>>>,
    summary_cache_dirty: Arc<AtomicBool>,
    sessions_cache: Arc<Mutex<Option<SessionsCache>>>,
    status_cache: Arc<Mutex<Option<ClaudeStatusCache>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    cache_creation_input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
}

#[derive(Clone, Debug)]
struct ClaudePricing {
    input_per_million: f64,
    input_per_million_above_200k: Option<f64>,
    cache_creation_input_per_million: f64,
    cache_creation_input_per_million_above_200k: Option<f64>,
    cached_input_per_million: f64,
    cached_input_per_million_above_200k: Option<f64>,
    output_per_million: f64,
    output_per_million_above_200k: Option<f64>,
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
    input_cost_per_token_above_200k_tokens: Option<f64>,
    cache_creation_input_token_cost: Option<f64>,
    cache_creation_input_token_cost_above_200k_tokens: Option<f64>,
    cache_read_input_token_cost: Option<f64>,
    cache_read_input_token_cost_above_200k_tokens: Option<f64>,
    output_cost_per_token: Option<f64>,
    output_cost_per_token_above_200k_tokens: Option<f64>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionsCache {
    fetched_at_epoch_ms: i64,
    rows: Vec<SessionRow>,
}

#[derive(Clone, Debug)]
struct ClaudeStatusCache {
    fetched_at_epoch_ms: i64,
    last_attempt_epoch_ms: i64,
    quota: ProviderQuota,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClaudeStatusCacheFile {
    fetched_at_epoch_ms: i64,
    #[serde(default)]
    last_attempt_epoch_ms: i64,
    quota: ProviderQuota,
}

#[derive(Debug, Deserialize)]
struct ClaudeStatusProbePayload {
    plan: Option<String>,
    #[serde(default)]
    windows: Vec<ClaudeStatusProbeWindow>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStatusProbeWindow {
    label: String,
    used_percent: u8,
    reset_text: Option<String>,
    window_minutes: Option<u64>,
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

#[derive(Default)]
struct ClaudeQuotaUsage {
    five_hour_usd: f64,
    seven_day_usd: f64,
}

#[derive(Debug, Default, Deserialize)]
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
        let status_cache = load_status_cache_from_disk().unwrap_or(None);

        Self {
            claude_home,
            projects_dir,
            machine_id: machine_label.clone(),
            machine_label,
            pricing_client: Client::builder()
                .user_agent("agent-cow/0.1")
                .build()
                .expect("reqwest client should build"),
            pricing_cache: Arc::new(Mutex::new(None)),
            pricing_refresh_in_flight: Arc::new(AtomicBool::new(false)),
            status_refresh_in_flight: Arc::new(AtomicBool::new(false)),
            static_cache: Arc::new(Mutex::new(HashMap::new())),
            summary_cache: Arc::new(Mutex::new(None)),
            summary_cache_dirty: Arc::new(AtomicBool::new(false)),
            sessions_cache: Arc::new(Mutex::new(None)),
            status_cache: Arc::new(Mutex::new(status_cache)),
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

    async fn subscription_quota(&self) -> Option<ProviderQuota> {
        let fallback = load_claude_subscription_quota(&self.claude_home);
        if fallback.is_none() && !self.projects_exist() {
            return None;
        }
        let cached = self.status_cache.lock().expect("lock poisoned").clone();

        if let Some(cache) = cached.as_ref().filter(|cache| {
            is_status_cache_fresh(cache.fetched_at_epoch_ms) && !cache.quota.windows.is_empty()
        }) {
            return merge_claude_quota(fallback, Some(cache.quota.clone()));
        }

        if cached
            .as_ref()
            .is_some_and(|cache| is_status_probe_in_backoff(cache.last_attempt_epoch_ms))
        {
            return merge_claude_quota(fallback, cached.map(|cache| cache.quota));
        }

        if self
            .status_refresh_in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let claude_home = self.claude_home.clone();
            let status_cache = self.status_cache.clone();
            let refresh_flag = self.status_refresh_in_flight.clone();
            let previous_cache = cached.clone();
            let fallback_for_refresh = fallback.clone();
            thread::spawn(move || {
                let attempted_at = now_epoch_millis();
                let probed = match probe_claude_status_quota(&claude_home) {
                    Ok(quota) => quota,
                    Err(error) => {
                        tracing::debug!("failed to probe Claude /status usage: {error:#}");
                        None
                    }
                };
                let cache = if let Some(quota) = probed.filter(|quota| !quota.windows.is_empty()) {
                    Some(ClaudeStatusCache {
                        fetched_at_epoch_ms: attempted_at,
                        last_attempt_epoch_ms: attempted_at,
                        quota: normalize_claude_quota_labels(quota),
                    })
                } else {
                    previous_cache
                        .clone()
                        .or_else(|| {
                            fallback_for_refresh.clone().map(|quota| ClaudeStatusCache {
                                fetched_at_epoch_ms: 0,
                                last_attempt_epoch_ms: attempted_at,
                                quota: normalize_claude_quota_labels(quota),
                            })
                        })
                        .map(|mut cache| {
                            cache.last_attempt_epoch_ms = attempted_at;
                            cache
                        })
                };

                if let Some(cache) = cache {
                    *status_cache.lock().expect("lock poisoned") = Some(cache.clone());
                    let _ = store_status_cache_to_disk(&cache);
                }
                refresh_flag.store(false, Ordering::SeqCst);
            });
        }

        merge_claude_quota(fallback, cached.map(|cache| cache.quota))
    }

    fn load_sessions(&self) -> Result<Vec<SessionRow>> {
        if let Some(cache) = self.sessions_cache.lock().expect("lock poisoned").clone()
            && is_sessions_cache_fresh(cache.fetched_at_epoch_ms)
            && should_use_cached_sessions(cache.rows.len(), &self.projects_dir)
        {
            return Ok(cache.rows);
        }

        if let Some(cache) = load_sessions_cache_from_disk()?
            && is_sessions_disk_cache_fresh(cache.fetched_at_epoch_ms)
            && should_use_cached_sessions(cache.rows.len(), &self.projects_dir)
        {
            *self.sessions_cache.lock().expect("lock poisoned") = Some(cache.clone());
            return Ok(cache.rows);
        }

        let mut rows = Vec::new();
        if self.projects_exist() {
            collect_sessions_from_projects(&self.projects_dir, &mut rows, self)?;
        }
        rows.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));

        let cache = SessionsCache {
            fetched_at_epoch_ms: now_epoch_millis(),
            rows: rows.clone(),
        };
        *self.sessions_cache.lock().expect("lock poisoned") = Some(cache.clone());
        let _ = store_sessions_cache_to_disk(&cache);
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
        {
            let mut cache = self.summary_cache.lock().expect("lock poisoned");
            if cache.is_none() {
                *cache = Some(load_summary_cache_from_disk().unwrap_or_default());
            }
            if let Some(entry) = cache.as_ref().and_then(|cache| cache.get(path)).cloned()
                && entry.modified_at_epoch_ms == signature.0
                && entry.file_len == signature.1
            {
                return Ok(entry.hint);
            }
        }

        let hint = analyze_transcript(path, ReadMode::Summary)?;
        self.summary_cache
            .lock()
            .expect("lock poisoned")
            .get_or_insert_with(HashMap::new)
            .insert(
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
        let empty_cache = HashMap::new();
        let cache_ref = cache.as_ref().unwrap_or(&empty_cache);
        if let Err(error) = store_summary_cache_to_disk(cache_ref) {
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
        self.list_sessions_with_progress(query, None).await
    }

    async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        let now = Utc::now();
        let mut litellm_pricing = None;
        let mut subscription = self.subscription_quota().await;
        let mut quota_usage = ClaudeQuotaUsage::default();
        let mut sessions = Vec::new();
        let rows = self.load_sessions()?;
        let total_sessions = rows.len();
        emit_session_progress(progress.as_ref(), 0, total_sessions);
        let mut loaded_sessions = 0usize;

        for row in rows {
            let hint = self.summary_hint(&row.transcript_path)?;
            let pricing_model = row.model.as_deref().or(hint.latest_model.as_deref());
            if litellm_pricing.is_none()
                && pricing_model.is_some_and(|model| builtin_claude_pricing(model).is_none())
            {
                litellm_pricing = self.litellm_pricing().await;
            }
            quota_usage.observe(&hint, pricing_model, litellm_pricing.as_deref(), now);
            let summary = build_summary(
                &self.machine_id,
                &self.machine_label,
                &row,
                &hint,
                litellm_pricing.as_deref(),
                now,
            );
            sessions.push(summary);
            loaded_sessions += 1;
            if loaded_sessions == total_sessions || loaded_sessions.is_multiple_of(8) {
                emit_session_progress(progress.as_ref(), loaded_sessions, total_sessions);
            }

            if let Some(limit) = query.limit
                && sessions.len() >= limit
            {
                break;
            }
        }

        if let Some(quota) = subscription.as_mut()
            && quota.windows.is_empty()
        {
            quota.summary = quota_usage.summary_text();
        }

        self.persist_summary_cache_if_dirty();

        Ok(SessionList {
            generated_at: now,
            overview: build_overview(&sessions, subscription, total_sessions),
            sessions,
        })
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let now = Utc::now();
        let row = self.session_by_id(id)?;
        let hint = analyze_transcript(&row.transcript_path, ReadMode::Detail)?;
        let litellm_pricing = if row
            .model
            .as_deref()
            .or(hint.latest_model.as_deref())
            .is_some_and(|model| builtin_claude_pricing(model).is_none())
        {
            self.litellm_pricing().await
        } else {
            None
        };
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

fn emit_session_progress(
    progress: Option<&UnboundedSender<SessionLoadProgress>>,
    loaded_sessions: usize,
    total_sessions: usize,
) {
    if let Some(progress) = progress {
        let _ = progress.send(SessionLoadProgress {
            loaded_sessions,
            total_sessions,
            sources: Vec::new(),
        });
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
    _updated_at: DateTime<Utc>,
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
    if !has_live_signal {
        return SessionActivityState::Idle;
    }

    let latest_feedback = latest_recent_non_user_event(&hint.recent_events, now, recent_window);

    if latest_feedback.is_some_and(is_compaction_event)
        && hint.recent_compaction_at.is_some_and(|timestamp| {
            now - timestamp <= Duration::seconds(COMPACTION_ACTIVITY_WINDOW_SECONDS)
        })
    {
        return SessionActivityState::Compacting;
    }

    if latest_feedback
        .filter(|event| matches!(event.kind, ActivityKind::ToolCall))
        .is_some_and(|event| is_exploration_tool(&event.summary))
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

fn latest_recent_non_user_event(
    events: &[ActivityEvent],
    now: DateTime<Utc>,
    recent_window: Duration,
) -> Option<&ActivityEvent> {
    events.iter().rev().find(|event| {
        !matches!(event.kind, ActivityKind::User) && now - event.timestamp <= recent_window
    })
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
    let mut navigation = Vec::new();

    if !row.id.is_empty() {
        navigation.push(NavigationTarget {
            kind: NavigationKind::ThreadId,
            label: "Conversation".to_string(),
            target: row.id.clone(),
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

fn build_overview(
    sessions: &[SessionSummary],
    subscription: Option<ProviderQuota>,
    total_sessions: usize,
) -> UsageOverview {
    UsageOverview {
        total_sessions,
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
        summary: None,
        windows: Vec::new(),
        limit_reached: false,
    })
}

fn merge_claude_quota(
    fallback: Option<ProviderQuota>,
    exact: Option<ProviderQuota>,
) -> Option<ProviderQuota> {
    let mut quota = exact.or(fallback.clone())?;

    if quota.plan.is_none() {
        quota.plan = fallback.as_ref().and_then(|item| item.plan.clone());
    }
    if quota.summary.is_none() {
        quota.summary = fallback.and_then(|item| item.summary);
    }
    quota.limit_reached |= quota
        .windows
        .iter()
        .any(|window| window.remaining_percent == 0);
    Some(quota)
}

fn probe_claude_status_quota(claude_home: &Path) -> Result<Option<ProviderQuota>> {
    let fallback = load_claude_subscription_quota(claude_home);
    if let Some(quota) = probe_claude_status_quota_with_python(fallback.as_ref(), Local::now())? {
        return Ok(Some(quota));
    }
    let capture = capture_claude_status_usage()?;
    Ok(parse_claude_status_quota(
        &capture,
        fallback.as_ref(),
        Local::now(),
    ))
}

fn probe_claude_status_quota_with_python(
    fallback: Option<&ProviderQuota>,
    now_local: DateTime<Local>,
) -> Result<Option<ProviderQuota>> {
    let mut command = Command::new("python3");
    apply_sanitized_probe_env(&mut command);
    command.args(["-c", CLAUDE_STATUS_PYTHON_PROBE]);
    let output = match command.output() {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("failed to start Python Claude status probe"),
    };

    if !output.status.success() {
        return Ok(None);
    }

    let payload: ClaudeStatusProbePayload = match serde_json::from_slice(&output.stdout) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::debug!("failed to parse Python Claude status probe output: {error:#}");
            return Ok(None);
        }
    };

    if payload.windows.is_empty() {
        return Ok(None);
    }

    let windows = payload
        .windows
        .into_iter()
        .map(|window| agent_cow_core::QuotaWindow {
            label: window.label,
            used_percent: window.used_percent,
            remaining_percent: 100_u8.saturating_sub(window.used_percent),
            reset_at: window
                .reset_text
                .as_deref()
                .and_then(|text| parse_claude_reset_at(text, now_local)),
            window_minutes: window.window_minutes,
        })
        .collect::<Vec<_>>();

    Ok(Some(ProviderQuota {
        provider: ProviderKind::Claude,
        plan: payload
            .plan
            .or_else(|| fallback.and_then(|quota| quota.plan.clone())),
        summary: None,
        limit_reached: windows.iter().any(|window| window.remaining_percent == 0),
        windows,
    }))
}

fn capture_claude_status_usage() -> Result<String> {
    let mut command = Command::new("script");
    apply_sanitized_probe_env(&mut command);
    command.args(["-q", "/dev/null", "zsh", "-lc", "claude"]);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start Claude status probe")?;
    let mut stdin = child
        .stdin
        .take()
        .context("Claude status probe stdin missing")?;
    let mut stdout = child
        .stdout
        .take()
        .context("Claude status probe stdout missing")?;

    let output = Arc::new(Mutex::new(Vec::<u8>::new()));
    let reader_output = output.clone();
    let reader = thread::spawn(move || -> Result<()> {
        let mut chunk = [0_u8; 16 * 1024];
        loop {
            match stdout.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => reader_output
                    .lock()
                    .expect("lock poisoned")
                    .extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(error).context("failed to read Claude status probe output");
                }
            }
        }
        Ok(())
    });

    thread::sleep(StdDuration::from_millis(1_000));
    write_probe_keys(&mut stdin, b"\r", StdDuration::from_millis(400))?;
    write_probe_keys(&mut stdin, b"/status\r", StdDuration::from_millis(600))?;
    write_probe_keys(&mut stdin, b"\t", StdDuration::from_millis(400))?;
    write_probe_keys(&mut stdin, b"\t", StdDuration::from_millis(250))?;

    let deadline = Instant::now() + CLAUDE_STATUS_PROBE_TIMEOUT;
    while Instant::now() < deadline {
        thread::sleep(StdDuration::from_millis(100));
        let snapshot = output.lock().expect("lock poisoned").clone();
        let text = strip_terminal_sequences(&snapshot);
        if text.contains("Current session")
            && text.contains("Current week (all models)")
            && text.contains("% used")
        {
            break;
        }
    }

    let _ = write_probe_keys(&mut stdin, b"\x1b", StdDuration::from_millis(100));
    let _ = write_probe_keys(&mut stdin, b"/exit\r", StdDuration::from_millis(150));
    drop(stdin);

    let wait_deadline = Instant::now() + StdDuration::from_millis(800);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < wait_deadline => {
                thread::sleep(StdDuration::from_millis(50));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            Err(error) => return Err(error).context("failed to wait for Claude status probe"),
        }
    }

    reader
        .join()
        .map_err(|_| anyhow!("Claude status probe reader thread panicked"))??;

    let captured = output.lock().expect("lock poisoned").clone();
    Ok(String::from_utf8_lossy(&captured).into_owned())
}

fn write_probe_keys(stdin: &mut impl Write, bytes: &[u8], settle_for: StdDuration) -> Result<()> {
    stdin.write_all(bytes)?;
    stdin.flush()?;
    thread::sleep(settle_for);
    Ok(())
}

fn apply_sanitized_probe_env(command: &mut Command) {
    command.env_clear();
    for key in [
        "HOME", "PATH", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR",
    ] {
        if let Ok(value) = std::env::var(key) {
            command.env(key, value);
        }
    }
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/zsh".to_string());
    command.env("SHELL", shell);
}

fn parse_claude_status_quota(
    capture: &str,
    fallback: Option<&ProviderQuota>,
    now_local: DateTime<Local>,
) -> Option<ProviderQuota> {
    let cleaned = strip_terminal_sequences(capture.as_bytes()).replace('\u{a0}', " ");
    let lines: Vec<String> = cleaned
        .lines()
        .map(normalize_single_line_text)
        .filter(|line| !line.is_empty())
        .collect();

    let week_all_index = lines
        .iter()
        .rposition(|line| line.contains("Current week (all models)"));
    let sonnet_index = lines
        .iter()
        .rposition(|line| line.contains("Current week (Sonnet only)"));

    let windows = [
        week_all_index
            .and_then(|index| parse_previous_usage_block(&lines, index, "5H", None, now_local)),
        week_all_index.and_then(|index| {
            parse_usage_block_after(&lines, index, "7d", Some(7 * 24 * 60), now_local)
        }),
        sonnet_index.and_then(|index| {
            parse_usage_block_after(&lines, index, "SN", Some(7 * 24 * 60), now_local)
        }),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    if windows.is_empty() {
        return fallback.cloned();
    }

    Some(ProviderQuota {
        provider: ProviderKind::Claude,
        plan: parse_claude_plan_from_status(&lines)
            .or_else(|| fallback.and_then(|quota| quota.plan.clone())),
        summary: None,
        limit_reached: windows.iter().any(|window| window.remaining_percent == 0),
        windows,
    })
}

fn parse_previous_usage_block(
    lines: &[String],
    before_index: usize,
    label: &str,
    window_minutes: Option<u64>,
    now_local: DateTime<Local>,
) -> Option<agent_cow_core::QuotaWindow> {
    let used_index = (0..before_index)
        .rev()
        .find(|index| parse_used_percent(&lines[*index]).is_some())?;
    build_quota_window(
        label,
        parse_used_percent(&lines[used_index])?,
        lines
            .iter()
            .take(before_index)
            .skip(used_index + 1)
            .find_map(|line| parse_reset_text(line)),
        window_minutes,
        now_local,
    )
}

fn parse_usage_block_after(
    lines: &[String],
    title_index: usize,
    label: &str,
    window_minutes: Option<u64>,
    now_local: DateTime<Local>,
) -> Option<agent_cow_core::QuotaWindow> {
    let used_index = lines
        .iter()
        .enumerate()
        .skip(title_index + 1)
        .find(|(_, line)| parse_used_percent(line).is_some())
        .map(|(index, _)| index)?;
    build_quota_window(
        label,
        parse_used_percent(&lines[used_index])?,
        lines
            .iter()
            .skip(used_index + 1)
            .take(3)
            .find_map(|line| parse_reset_text(line)),
        window_minutes,
        now_local,
    )
}

fn build_quota_window(
    label: &str,
    used_percent: u8,
    reset_text: Option<String>,
    window_minutes: Option<u64>,
    now_local: DateTime<Local>,
) -> Option<agent_cow_core::QuotaWindow> {
    Some(agent_cow_core::QuotaWindow {
        label: label.to_string(),
        used_percent,
        remaining_percent: 100_u8.saturating_sub(used_percent),
        reset_at: reset_text
            .as_deref()
            .and_then(|text| parse_claude_reset_at(text, now_local)),
        window_minutes,
    })
}

fn parse_reset_text(line: &str) -> Option<String> {
    if let Some(text) = line.strip_prefix("Resets ") {
        return Some(text.trim().to_string());
    }
    if line.to_ascii_lowercase().contains("europe/stockholm") {
        return Some(line.trim().to_string());
    }
    None
}

fn parse_used_percent(line: &str) -> Option<u8> {
    let end = line.find("% used")?;
    let prefix = &line[..end];
    let digits = prefix
        .chars()
        .rev()
        .take_while(|char| char.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    digits.parse::<u8>().ok()
}

fn parse_claude_plan_from_status(lines: &[String]) -> Option<String> {
    let login_method = lines
        .iter()
        .rev()
        .find_map(|line| line.strip_prefix("Login method:"))
        .map(str::trim)?;
    let login_method = login_method
        .strip_suffix(" Account")
        .unwrap_or(login_method);
    let login_method = login_method.strip_prefix("Claude ").unwrap_or(login_method);
    (!login_method.is_empty()).then(|| login_method.to_string())
}

fn parse_claude_reset_at(text: &str, now_local: DateTime<Local>) -> Option<DateTime<Utc>> {
    let cleaned = text
        .split(" (")
        .next()
        .unwrap_or(text)
        .trim()
        .replace(" at ", ", ");

    if let Some((month_day, clock)) = cleaned.split_once(',') {
        let date = NaiveDate::parse_from_str(
            &format!("{} {}", now_local.year(), month_day.trim()),
            "%Y %b %d",
        )
        .ok()?;
        let time = parse_claude_clock(clock)?;
        let local = Local
            .from_local_datetime(&date.and_time(time))
            .single()
            .or_else(|| Local.from_local_datetime(&date.and_time(time)).earliest())?;
        return Some(local.with_timezone(&Utc));
    }

    let time = parse_claude_clock(&cleaned)?;
    let today = now_local.date_naive();
    let today_local = Local
        .from_local_datetime(&today.and_time(time))
        .single()
        .or_else(|| Local.from_local_datetime(&today.and_time(time)).earliest())?;
    let resolved = if today_local < now_local {
        let tomorrow = today.succ_opt()?;
        Local
            .from_local_datetime(&tomorrow.and_time(time))
            .single()
            .or_else(|| {
                Local
                    .from_local_datetime(&tomorrow.and_time(time))
                    .earliest()
            })?
    } else {
        today_local
    };
    Some(resolved.with_timezone(&Utc))
}

fn parse_claude_clock(text: &str) -> Option<NaiveTime> {
    let compact = text.trim().to_ascii_lowercase().replace(' ', "");
    let meridiem = if compact.ends_with("am") {
        "am"
    } else if compact.ends_with("pm") {
        "pm"
    } else {
        return None;
    };
    let body = compact.strip_suffix(meridiem)?;
    let (hour, minute) = if let Some((hour, minute)) = body.split_once(':') {
        (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?)
    } else {
        (body.parse::<u32>().ok()?, 0)
    };
    if !(1..=12).contains(&hour) || minute > 59 {
        return None;
    }
    let hour24 = match (meridiem, hour) {
        ("am", 12) => 0,
        ("am", hour) => hour,
        ("pm", 12) => 12,
        ("pm", hour) => hour + 12,
        _ => unreachable!(),
    };
    NaiveTime::from_hms_opt(hour24, minute, 0)
}

fn strip_terminal_sequences(input: &[u8]) -> String {
    let mut plain = Vec::with_capacity(input.len());
    let mut index = 0;

    while index < input.len() {
        match input[index] {
            0x1b => {
                index += 1;
                if index >= input.len() {
                    break;
                }
                match input[index] {
                    b'[' => {
                        let params_start = index + 1;
                        index += 1;
                        let mut command = None;
                        while index < input.len() {
                            let byte = input[index];
                            index += 1;
                            if (0x40..=0x7e).contains(&byte) {
                                command = Some(byte);
                                break;
                            }
                        }
                        if matches!(command, Some(b'C')) {
                            let params = std::str::from_utf8(&input[params_start..index - 1])
                                .ok()
                                .unwrap_or_default();
                            let count = params
                                .split(';')
                                .next()
                                .and_then(|item| item.parse::<usize>().ok())
                                .unwrap_or(1);
                            plain.extend(std::iter::repeat_n(b' ', count));
                        }
                    }
                    b']' => {
                        index += 1;
                        while index < input.len() {
                            let byte = input[index];
                            index += 1;
                            if byte == 0x07 {
                                break;
                            }
                            if byte == 0x1b && input.get(index) == Some(&b'\\') {
                                index += 1;
                                break;
                            }
                        }
                    }
                    _ => index += 1,
                }
            }
            b'\r' => {
                plain.push(b'\n');
                index += 1;
            }
            0x08 => {
                index += 1;
            }
            byte if byte < 0x20 && byte != b'\n' && byte != b'\t' => {
                index += 1;
            }
            byte => {
                plain.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&plain).into_owned()
}

impl ClaudeQuotaUsage {
    fn observe(
        &mut self,
        hint: &TranscriptHint,
        model: Option<&str>,
        litellm_pricing: Option<&LitellmPricingMap>,
        now: DateTime<Utc>,
    ) {
        let Some(model) = model else {
            return;
        };
        let Some(pricing) = resolve_claude_pricing(model, litellm_pricing) else {
            return;
        };

        for key in recent_hour_keys(now, 5) {
            if let Some(bucket) = hint.cost_by_hour.get(&key) {
                self.five_hour_usd += estimate_raw_cost_bucket(bucket, &pricing);
            }
        }

        for key in recent_date_keys(now, 7) {
            if let Some(bucket) = hint.cost_by_day.get(&key) {
                self.seven_day_usd += estimate_raw_cost_bucket(bucket, &pricing);
            }
        }
    }

    fn summary_text(&self) -> Option<String> {
        if self.five_hour_usd < 0.0005 && self.seven_day_usd < 0.0005 {
            return None;
        }

        Some(format!(
            "5h {} · 7d {}",
            format_usd_compact(self.five_hour_usd),
            format_usd_compact(self.seven_day_usd)
        ))
    }
}

fn resolve_claude_pricing(
    model: &str,
    litellm_pricing: Option<&LitellmPricingMap>,
) -> Option<ClaudePricing> {
    if let Some(pricing) = builtin_claude_pricing(model) {
        return Some(pricing);
    }

    if let Some(entry) = litellm_pricing.and_then(|entries| find_litellm_entry(model, entries))
        && let Some(pricing) = litellm_to_claude_pricing(entry)
    {
        return Some(pricing);
    }

    None
}

fn builtin_claude_pricing(model: &str) -> Option<ClaudePricing> {
    let lowered = model.to_ascii_lowercase();

    if lowered.contains("opus-4-6") {
        return Some(ClaudePricing {
            input_per_million: 5.0,
            input_per_million_above_200k: None,
            cache_creation_input_per_million: 6.25,
            cache_creation_input_per_million_above_200k: None,
            cached_input_per_million: 0.5,
            cached_input_per_million_above_200k: None,
            output_per_million: 25.0,
            output_per_million_above_200k: None,
            max_input_tokens: Some(1_000_000),
            source: PricingSource::BuiltIn,
        });
    }

    if lowered.contains("opus") {
        return Some(ClaudePricing {
            input_per_million: 15.0,
            input_per_million_above_200k: None,
            cache_creation_input_per_million: 18.75,
            cache_creation_input_per_million_above_200k: None,
            cached_input_per_million: 1.5,
            cached_input_per_million_above_200k: None,
            output_per_million: 75.0,
            output_per_million_above_200k: None,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }

    if lowered.contains("sonnet") {
        return Some(ClaudePricing {
            input_per_million: 3.0,
            input_per_million_above_200k: None,
            cache_creation_input_per_million: 3.75,
            cache_creation_input_per_million_above_200k: None,
            cached_input_per_million: 0.3,
            cached_input_per_million_above_200k: None,
            output_per_million: 15.0,
            output_per_million_above_200k: None,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }

    if lowered.contains("haiku") {
        return Some(ClaudePricing {
            input_per_million: 1.0,
            input_per_million_above_200k: None,
            cache_creation_input_per_million: 1.25,
            cache_creation_input_per_million_above_200k: None,
            cached_input_per_million: 0.1,
            cached_input_per_million_above_200k: None,
            output_per_million: 5.0,
            output_per_million_above_200k: None,
            max_input_tokens: Some(builtin_context_window(model)),
            source: PricingSource::BuiltIn,
        });
    }

    None
}

fn builtin_context_window(model: &str) -> u64 {
    let lowered = model.to_ascii_lowercase();
    if lowered.contains("opus-4-6") || lowered.contains("1m") {
        1_000_000
    } else {
        CLAUDE_DEFAULT_CONTEXT_WINDOW
    }
}

fn litellm_to_claude_pricing(entry: &LitellmPricingEntry) -> Option<ClaudePricing> {
    Some(ClaudePricing {
        input_per_million: entry.input_cost_per_token? * 1_000_000.0,
        input_per_million_above_200k: entry
            .input_cost_per_token_above_200k_tokens
            .map(|value| value * 1_000_000.0),
        cache_creation_input_per_million: entry
            .cache_creation_input_token_cost
            .unwrap_or(entry.input_cost_per_token?)
            * 1_000_000.0,
        cache_creation_input_per_million_above_200k: entry
            .cache_creation_input_token_cost_above_200k_tokens
            .map(|value| value * 1_000_000.0),
        cached_input_per_million: entry.cache_read_input_token_cost.unwrap_or(0.0) * 1_000_000.0,
        cached_input_per_million_above_200k: entry
            .cache_read_input_token_cost_above_200k_tokens
            .map(|value| value * 1_000_000.0),
        output_per_million: entry.output_cost_per_token? * 1_000_000.0,
        output_per_million_above_200k: entry
            .output_cost_per_token_above_200k_tokens
            .map(|value| value * 1_000_000.0),
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
    let cache_creation_input_tokens = tokens.cache_creation_input_tokens.unwrap_or(0);
    let cached_input_tokens = tokens.cached_input_tokens.unwrap_or(0);
    let output_tokens = tokens.output_tokens?;

    let input_usd = token_cost_tiered(
        input_tokens,
        pricing.input_per_million,
        pricing.input_per_million_above_200k,
    );
    let cache_creation_input_usd = token_cost_tiered(
        cache_creation_input_tokens,
        pricing.cache_creation_input_per_million,
        pricing.cache_creation_input_per_million_above_200k,
    );
    let cached_input_usd = token_cost_tiered(
        cached_input_tokens,
        pricing.cached_input_per_million,
        pricing.cached_input_per_million_above_200k,
    );
    let output_usd = token_cost_tiered(
        output_tokens,
        pricing.output_per_million,
        pricing.output_per_million_above_200k,
    );

    Some(SessionCost {
        input_usd,
        cache_creation_input_usd,
        cached_input_usd,
        output_usd,
        total_usd: input_usd + cache_creation_input_usd + cached_input_usd + output_usd,
        hour_usd,
        day_usd,
        pricing_source: pricing.source.clone(),
    })
}

fn estimate_raw_cost_bucket(bucket: &RawCostBucket, pricing: &ClaudePricing) -> f64 {
    token_cost_tiered(
        bucket.input_tokens,
        pricing.input_per_million,
        pricing.input_per_million_above_200k,
    ) + token_cost_tiered(
        bucket.cache_creation_input_tokens,
        pricing.cache_creation_input_per_million,
        pricing.cache_creation_input_per_million_above_200k,
    ) + token_cost_tiered(
        bucket.cached_input_tokens,
        pricing.cached_input_per_million,
        pricing.cached_input_per_million_above_200k,
    ) + token_cost_tiered(
        bucket.output_tokens,
        pricing.output_per_million,
        pricing.output_per_million_above_200k,
    )
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

fn token_cost_tiered(tokens: u64, base_per_million: f64, tiered_per_million: Option<f64>) -> f64 {
    if tokens == 0 {
        return 0.0;
    }

    if let Some(tiered_per_million) = tiered_per_million
        && tokens > CLAUDE_TIERED_THRESHOLD_TOKENS
    {
        let below_threshold = CLAUDE_TIERED_THRESHOLD_TOKENS;
        let above_threshold = tokens.saturating_sub(CLAUDE_TIERED_THRESHOLD_TOKENS);
        return token_cost(below_threshold, base_per_million)
            + token_cost(above_threshold, tiered_per_million);
    }

    token_cost(tokens, base_per_million)
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
        let parsed: SessionsIndexFile = fs::read_to_string(&index_path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();

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
            let fallback_id = fallback_session_id(&transcript_path);
            let mut static_data = TranscriptStatic::default();
            let mut session_id = fallback_id.clone();
            let mut index_entry = index_entries.get(&session_id);

            if index_entry.is_none() {
                static_data = source
                    .transcript_static(&transcript_path)
                    .unwrap_or_else(|_| TranscriptStatic::default());
                if !static_data.id.is_empty() {
                    session_id = static_data.id.clone();
                    index_entry = index_entries.get(&session_id);
                }
            }

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
                    hint.latest_context_used_tokens = Some(total_claude_input_tokens(&delta));
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
        input_tokens: Some(input_tokens),
        cache_creation_input_tokens: Some(cache_creation_input_tokens),
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
    total.total_tokens = total
        .input_tokens
        .unwrap_or(0)
        .saturating_add(total.cache_creation_input_tokens.unwrap_or(0))
        .saturating_add(total.cached_input_tokens.unwrap_or(0))
        .saturating_add(total.output_tokens.unwrap_or(0));
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
    bucket.cache_creation_input_tokens = bucket
        .cache_creation_input_tokens
        .saturating_add(delta.cache_creation_input_tokens.unwrap_or(0));
    bucket.cached_input_tokens = bucket
        .cached_input_tokens
        .saturating_add(delta.cached_input_tokens.unwrap_or(0));
    bucket.output_tokens = bucket
        .output_tokens
        .saturating_add(delta.output_tokens.unwrap_or(0));
}

fn total_claude_input_tokens(tokens: &TokenUsage) -> u64 {
    tokens
        .input_tokens
        .unwrap_or(0)
        .saturating_add(tokens.cache_creation_input_tokens.unwrap_or(0))
        .saturating_add(tokens.cached_input_tokens.unwrap_or(0))
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

fn recent_hour_keys(now: DateTime<Utc>, hours: usize) -> Vec<String> {
    (0..hours)
        .map(|offset| local_hour_key(now - Duration::hours(offset as i64)))
        .collect()
}

fn recent_date_keys(now: DateTime<Utc>, days: usize) -> Vec<String> {
    (0..days)
        .map(|offset| local_date_key(now - Duration::days(offset as i64)))
        .collect()
}

fn format_usd_compact(value: f64) -> String {
    if value < 0.0005 {
        "$0".to_string()
    } else if value >= 100.0 {
        format!("${value:.0}")
    } else if value >= 10.0 {
        format!("${value:.1}")
    } else if value >= 1.0 {
        format!("${value:.2}")
    } else {
        format!("${value:.3}")
    }
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

fn should_use_cached_sessions(row_count: usize, projects_dir: &Path) -> bool {
    !(row_count == 0 && projects_contain_transcripts(projects_dir))
}

fn projects_contain_transcripts(projects_dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(projects_dir) else {
        return false;
    };

    for entry in entries.filter_map(Result::ok) {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }

        let Ok(children) = fs::read_dir(entry.path()) else {
            continue;
        };
        if children
            .filter_map(Result::ok)
            .any(|child| child.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        {
            return true;
        }
    }

    false
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
            .join("agent-cow")
            .join("litellm_pricing.json"),
    )
}

fn status_cache_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    Some(
        base_dirs
            .cache_dir()
            .join("agent-cow")
            .join("claude_status.json"),
    )
}

fn sessions_cache_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    Some(
        base_dirs
            .cache_dir()
            .join("agent-cow")
            .join("claude_sessions.json"),
    )
}

fn summary_cache_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    Some(
        base_dirs
            .cache_dir()
            .join("agent-cow")
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

fn load_status_cache_from_disk() -> Result<Option<ClaudeStatusCache>> {
    let Some(path) = status_cache_path() else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read status cache {}", path.display()))?;
    let parsed: ClaudeStatusCacheFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse status cache {}", path.display()))?;
    Ok(Some(ClaudeStatusCache {
        fetched_at_epoch_ms: parsed.fetched_at_epoch_ms,
        last_attempt_epoch_ms: if parsed.last_attempt_epoch_ms != 0 {
            parsed.last_attempt_epoch_ms
        } else {
            parsed.fetched_at_epoch_ms
        },
        quota: normalize_claude_quota_labels(parsed.quota),
    }))
}

fn load_sessions_cache_from_disk() -> Result<Option<SessionsCache>> {
    let Some(path) = sessions_cache_path() else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read sessions cache {}", path.display()))?;
    let parsed = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse sessions cache {}", path.display()))?;
    Ok(Some(parsed))
}

fn store_sessions_cache_to_disk(cache: &SessionsCache) -> Result<()> {
    let Some(path) = sessions_cache_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let raw = serde_json::to_string(cache)
        .with_context(|| format!("failed to serialize sessions cache {}", path.display()))?;
    fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn store_status_cache_to_disk(cache: &ClaudeStatusCache) -> Result<()> {
    let Some(path) = status_cache_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let payload = ClaudeStatusCacheFile {
        fetched_at_epoch_ms: cache.fetched_at_epoch_ms,
        last_attempt_epoch_ms: cache.last_attempt_epoch_ms,
        quota: normalize_claude_quota_labels(cache.quota.clone()),
    };
    let raw = serde_json::to_string(&payload)
        .with_context(|| format!("failed to serialize status cache {}", path.display()))?;
    fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
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

fn normalize_claude_quota_labels(mut quota: ProviderQuota) -> ProviderQuota {
    if quota.provider != ProviderKind::Claude {
        return quota;
    }

    for window in &mut quota.windows {
        window.label = match window.label.as_str() {
            "CS" => "5H".to_string(),
            "7D" | "WK" => "7d".to_string(),
            "SO" => "SN".to_string(),
            other => other.to_string(),
        };
    }

    quota
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
                input_cost_per_token_above_200k_tokens: entry_object
                    .get("input_cost_per_token_above_200k_tokens")
                    .and_then(Value::as_f64),
                cache_creation_input_token_cost: entry_object
                    .get("cache_creation_input_token_cost")
                    .and_then(Value::as_f64),
                cache_creation_input_token_cost_above_200k_tokens: entry_object
                    .get("cache_creation_input_token_cost_above_200k_tokens")
                    .and_then(Value::as_f64),
                cache_read_input_token_cost: entry_object
                    .get("cache_read_input_token_cost")
                    .and_then(Value::as_f64),
                cache_read_input_token_cost_above_200k_tokens: entry_object
                    .get("cache_read_input_token_cost_above_200k_tokens")
                    .and_then(Value::as_f64),
                output_cost_per_token: entry_object
                    .get("output_cost_per_token")
                    .and_then(Value::as_f64),
                output_cost_per_token_above_200k_tokens: entry_object
                    .get("output_cost_per_token_above_200k_tokens")
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

fn is_status_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= CLAUDE_STATUS_CACHE_TTL.as_millis() as i64
}

fn is_status_probe_in_backoff(last_attempt_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(last_attempt_epoch_ms);
    age_ms >= 0 && age_ms <= CLAUDE_STATUS_FAILURE_BACKOFF.as_millis() as i64
}

fn is_sessions_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= SESSION_DISCOVERY_CACHE_TTL.as_millis() as i64
}

fn is_sessions_disk_cache_fresh(fetched_at_epoch_ms: i64) -> bool {
    let age_ms = now_epoch_millis().saturating_sub(fetched_at_epoch_ms);
    age_ms >= 0 && age_ms <= SESSION_DISCOVERY_DISK_CACHE_TTL.as_millis() as i64
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
        ClaudePricing, ClaudeSource, TranscriptHint, analyze_transcript, builtin_claude_pricing,
        collect_sessions_from_projects, derive_activity_state, estimate_session_cost,
        looks_like_compaction_signal, normalize_message_text, normalize_title,
        parse_claude_status_quota, parse_claude_usage, should_use_cached_sessions,
    };
    use agent_cow_core::{
        ActivityEvent, ActivityKind, PricingSource, ProviderKind, ProviderQuota,
        SessionActivityState, SessionStatus, SessionStatusKind, StatusConfidence,
    };
    use chrono::{Local, TimeZone, Utc};
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
    fn derive_activity_state_does_not_think_from_running_ttl_alone() {
        let now = Utc::now();
        let status = SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: "running ttl".to_string(),
        };
        let hint = TranscriptHint {
            recent_events: vec![ActivityEvent {
                timestamp: now - chrono::Duration::seconds(4),
                kind: ActivityKind::Assistant,
                summary: "Done".to_string(),
            }],
            ..TranscriptHint::default()
        };

        let activity = derive_activity_state(
            &status,
            &hint,
            None,
            now - chrono::Duration::seconds(4),
            now,
        );

        assert_eq!(activity, SessionActivityState::Idle);
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
    fn derive_activity_state_stops_exploring_after_later_feedback() {
        let now = Utc::now();
        let status = SessionStatus {
            kind: SessionStatusKind::Running,
            confidence: StatusConfidence::Inferred,
            reason: "running".to_string(),
        };
        let hint = TranscriptHint {
            run_active: true,
            latest_thinking_at: Some(now - chrono::Duration::seconds(1)),
            recent_events: vec![
                ActivityEvent {
                    timestamp: now - chrono::Duration::seconds(3),
                    kind: ActivityKind::ToolCall,
                    summary: "exec_command  rg -n foo src  in ~/repo".to_string(),
                },
                ActivityEvent {
                    timestamp: now - chrono::Duration::seconds(1),
                    kind: ActivityKind::System,
                    summary: "thinking".to_string(),
                },
            ],
            ..TranscriptHint::default()
        };

        let activity = derive_activity_state(
            &status,
            &hint,
            None,
            now - chrono::Duration::seconds(1),
            now,
        );

        assert_eq!(activity, SessionActivityState::Thinking);
    }

    #[test]
    fn analyze_transcript_tracks_recent_compaction_signal() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("agent-cow-claude-compaction-{unique}"));
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

    #[test]
    fn collect_sessions_from_projects_reads_transcripts_without_index_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("agent-cow-claude-projects-{unique}"));
        let projects_dir = root.join("projects");
        let project_dir = projects_dir.join("-tmp-demo");
        fs::create_dir_all(&project_dir).unwrap();

        let session_id = "12345678-1234-1234-1234-123456789abc";
        let transcript_path = project_dir.join(format!("{session_id}.jsonl"));
        fs::write(
            &transcript_path,
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"2026-04-13T08:00:00.000Z\",\"cwd\":\"/tmp/demo\",\"sessionId\":\"{session_id}\",\"message\":{{\"content\":\"Investigate listing bug\"}}}}\n"
            ),
        )
        .unwrap();

        let source = ClaudeSource::new(&root);
        let mut rows = Vec::new();
        collect_sessions_from_projects(&projects_dir, &mut rows, &source).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, session_id);
        assert_eq!(rows[0].cwd, "/tmp/demo");
        assert_eq!(rows[0].title, "Investigate listing bug");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn should_not_use_empty_cache_when_transcripts_exist() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("agent-cow-claude-cache-{unique}"));
        let projects_dir = root.join("projects");
        let project_dir = projects_dir.join("-tmp-demo");
        fs::create_dir_all(&project_dir).unwrap();

        let session_id = "12345678-1234-1234-1234-123456789abc";
        let transcript_path = project_dir.join(format!("{session_id}.jsonl"));
        fs::write(
            &transcript_path,
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"2026-04-13T08:00:00.000Z\",\"cwd\":\"/tmp/demo\",\"sessionId\":\"{session_id}\",\"message\":{{\"content\":\"Investigate listing bug\"}}}}\n"
            ),
        )
        .unwrap();

        assert!(!should_use_cached_sessions(0, &projects_dir));
        assert!(should_use_cached_sessions(1, &projects_dir));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parse_claude_usage_tracks_cache_creation_separately() {
        let message = serde_json::json!({
            "usage": {
                "input_tokens": 3,
                "cache_creation_input_tokens": 10324,
                "cache_read_input_tokens": 11519,
                "output_tokens": 31
            }
        });

        let usage = parse_claude_usage(&message).unwrap();

        assert_eq!(usage.total_tokens, 21877);
        assert_eq!(usage.input_tokens, Some(3));
        assert_eq!(usage.cache_creation_input_tokens, Some(10324));
        assert_eq!(usage.cached_input_tokens, Some(11519));
        assert_eq!(usage.output_tokens, Some(31));
    }

    #[test]
    fn estimate_session_cost_counts_cache_creation_separately() {
        let tokens = agent_cow_core::TokenUsage {
            total_tokens: 10_000,
            input_tokens: Some(4_000),
            cache_creation_input_tokens: Some(2_000),
            cached_input_tokens: Some(3_000),
            output_tokens: Some(1_000),
            reasoning_output_tokens: None,
        };
        let pricing = ClaudePricing {
            input_per_million: 100.0,
            input_per_million_above_200k: None,
            cache_creation_input_per_million: 200.0,
            cache_creation_input_per_million_above_200k: None,
            cached_input_per_million: 50.0,
            cached_input_per_million_above_200k: None,
            output_per_million: 300.0,
            output_per_million_above_200k: None,
            max_input_tokens: Some(1_000_000),
            source: PricingSource::BuiltIn,
        };

        let cost = estimate_session_cost(&tokens, &pricing, 0.0, 0.0).unwrap();

        assert!((cost.input_usd - 0.4).abs() < 1e-9);
        assert!((cost.cache_creation_input_usd - 0.4).abs() < 1e-9);
        assert!((cost.cached_input_usd - 0.15).abs() < 1e-9);
        assert!((cost.output_usd - 0.3).abs() < 1e-9);
        assert!((cost.total_usd - 1.25).abs() < 1e-9);
    }

    #[test]
    fn estimate_session_cost_matches_ccusage_for_claude_opus_4_6() {
        let tokens = agent_cow_core::TokenUsage {
            total_tokens: 349_061,
            input_tokens: Some(22),
            cache_creation_input_tokens: Some(66_623),
            cached_input_tokens: Some(280_214),
            output_tokens: Some(2_202),
            reasoning_output_tokens: None,
        };
        let pricing = builtin_claude_pricing("claude-opus-4-6").unwrap();

        let cost = estimate_session_cost(&tokens, &pricing, 0.0, 0.0).unwrap();

        assert!((cost.total_usd - 0.61166075).abs() < 1e-9);
        assert!((cost.cache_creation_input_usd - 0.41639375).abs() < 1e-9);
    }

    #[test]
    fn parse_claude_status_quota_extracts_exact_usage_windows() {
        let capture = r#"
            Login method: Claude Max Account

            Current session
            █                                                  2% used
            Resets 11pm (Europe/Stockholm)

            Current week (all models)
            █▌                                                 3% used
            Resets Apr 15, 7pm (Europe/Stockholm)

            Current week (Sonnet only)
                                                              0% used
            Resets Apr 16, 9pm (Europe/Stockholm)
        "#;
        let fallback = ProviderQuota {
            provider: ProviderKind::Claude,
            plan: Some("Subscription".to_string()),
            summary: None,
            windows: Vec::new(),
            limit_reached: false,
        };
        let now_local = Local.with_ymd_and_hms(2026, 4, 11, 12, 0, 0).unwrap();

        let quota = parse_claude_status_quota(capture, Some(&fallback), now_local).unwrap();

        assert_eq!(quota.plan.as_deref(), Some("Max"));
        assert_eq!(quota.windows.len(), 3);
        assert_eq!(quota.windows[0].label, "5H");
        assert_eq!(quota.windows[0].used_percent, 2);
        assert_eq!(quota.windows[0].remaining_percent, 98);
        assert_eq!(quota.windows[1].label, "7d");
        assert_eq!(quota.windows[1].remaining_percent, 97);
        assert_eq!(quota.windows[2].label, "SN");
        assert_eq!(quota.windows[2].remaining_percent, 100);
    }

    #[test]
    fn parse_claude_status_quota_falls_back_when_usage_block_is_missing() {
        let fallback = ProviderQuota {
            provider: ProviderKind::Claude,
            plan: Some("Subscription".to_string()),
            summary: None,
            windows: Vec::new(),
            limit_reached: false,
        };
        let now_local = Local.with_ymd_and_hms(2026, 4, 11, 12, 0, 0).unwrap();

        let quota = parse_claude_status_quota(
            "Login method: Claude Max Account",
            Some(&fallback),
            now_local,
        )
        .unwrap();

        assert_eq!(quota.plan.as_deref(), Some("Subscription"));
        assert!(quota.windows.is_empty());
    }
}
