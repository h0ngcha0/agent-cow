use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use agent_cow_core::{
    MachineOverview, MonitorService, ProviderQuota, SessionDetail, SessionList,
    SessionLoadProgress, SessionQuery, SessionSummary, UsageOverview,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinSet,
    time::{MissedTickBehavior, sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::open;

const REMOTE_STREAM_RECONNECT_MIN: Duration = Duration::from_secs(1);
const REMOTE_STREAM_RECONNECT_MAX: Duration = Duration::from_secs(30);
const REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REMOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const REMOTE_HTTP_RETRY_MIN: Duration = Duration::from_secs(5);
const REMOTE_HTTP_RETRY_MAX: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenActionResponse {
    pub label: String,
    pub target: String,
}

#[async_trait]
pub trait MonitorClient: Send + Sync {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList>;

    async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        let list = self.list_sessions(query).await?;
        if let Some(progress) = progress {
            let _ = progress.send(SessionLoadProgress {
                loaded_sessions: list.sessions.len(),
                total_sessions: list.overview.total_sessions,
                sources: Vec::new(),
            });
        }
        Ok(list)
    }

    async fn latest_session(&self) -> Result<SessionDetail> {
        let list = self
            .list_sessions(SessionQuery {
                include_archived: true,
                limit: Some(1),
            })
            .await?;

        let latest = list
            .sessions
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no sessions were found"))?;

        self.get_session(&latest.id).await
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail>;

    async fn open_session_app(&self, id: &str) -> Result<OpenActionResponse>;

    async fn subscribe_sessions(
        &self,
        _query: SessionQuery,
        _refresh_every: Duration,
    ) -> Result<Option<UnboundedReceiver<SessionList>>> {
        Ok(None)
    }
}

#[derive(Clone)]
pub struct LocalMonitorClient {
    service: MonitorService,
    local_machine_id: String,
}

impl LocalMonitorClient {
    pub fn new(service: MonitorService) -> Self {
        Self {
            service,
            local_machine_id: open::local_machine_id(),
        }
    }
}

#[async_trait]
impl MonitorClient for LocalMonitorClient {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        self.service.list_sessions(query).await
    }

    async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        self.service
            .list_sessions_with_progress(query, progress)
            .await
    }

    async fn latest_session(&self) -> Result<SessionDetail> {
        self.service.latest_session().await
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        self.service.get_session(id).await
    }

    async fn open_session_app(&self, id: &str) -> Result<OpenActionResponse> {
        let session = self.service.get_session(id).await?;
        let action = open::open_session_app(&session.summary, &self.local_machine_id)?;
        Ok(OpenActionResponse {
            label: action.label,
            target: action.target,
        })
    }

    async fn subscribe_sessions(
        &self,
        query: SessionQuery,
        refresh_every: Duration,
    ) -> Result<Option<UnboundedReceiver<SessionList>>> {
        let service = self.service.clone();
        let (tx, rx) = unbounded_channel();
        let interval = refresh_every.max(Duration::from_secs(1));

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last_signature: Option<String> = None;

            loop {
                if tx.is_closed() {
                    break;
                }

                ticker.tick().await;
                let Ok(list) = service.list_sessions(query.clone()).await else {
                    continue;
                };

                let Ok(signature) = session_list_signature(&list) else {
                    continue;
                };

                if last_signature.as_deref() == Some(signature.as_str()) {
                    continue;
                }

                last_signature = Some(signature);
                if tx.send(list).is_err() {
                    break;
                }
            }
        });

        Ok(Some(rx))
    }
}

#[derive(Clone)]
pub struct RemoteMonitorClient {
    http: reqwest::Client,
    base_url: reqwest::Url,
    availability: Arc<Mutex<RemoteAvailability>>,
}

#[derive(Clone, Debug)]
struct RemoteAvailability {
    next_http_retry_at: Option<Instant>,
    http_backoff: Duration,
    last_error: Option<String>,
}

impl RemoteMonitorClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let mut base_url = reqwest::Url::parse(base_url)?;
        if !base_url.path().ends_with('/') {
            let mut path = base_url.path().to_string();
            path.push('/');
            base_url.set_path(&path);
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(REMOTE_CONNECT_TIMEOUT)
                .timeout(REMOTE_REQUEST_TIMEOUT)
                .build()?,
            base_url,
            availability: Arc::new(Mutex::new(RemoteAvailability {
                next_http_retry_at: None,
                http_backoff: REMOTE_HTTP_RETRY_MIN,
                last_error: None,
            })),
        })
    }

    pub fn fallback_machine_label(&self) -> String {
        self.base_url
            .host_str()
            .filter(|host| !host.is_empty())
            .unwrap_or("remote")
            .to_string()
    }

    fn offline_error(&self) -> Option<String> {
        self.availability
            .lock()
            .expect("lock poisoned")
            .last_error
            .clone()
    }

    fn offline_machine(&self, error: Option<String>) -> MachineOverview {
        let label = self.fallback_machine_label();
        MachineOverview {
            source: String::new(),
            machine_id: label.clone(),
            machine_label: label,
            reachable: false,
            error,
        }
    }

    fn offline_session_list(&self, error: Option<String>) -> SessionList {
        SessionList {
            generated_at: Utc::now(),
            overview: UsageOverview {
                machines: vec![self.offline_machine(error)],
                ..UsageOverview::default()
            },
            sessions: Vec::new(),
        }
    }

    fn note_http_success(&self) {
        let mut availability = self.availability.lock().expect("lock poisoned");
        availability.next_http_retry_at = None;
        availability.http_backoff = REMOTE_HTTP_RETRY_MIN;
        availability.last_error = None;
    }

    fn note_http_failure(&self, error: String) {
        let mut availability = self.availability.lock().expect("lock poisoned");
        availability.last_error = Some(error);
        availability.next_http_retry_at = Some(Instant::now() + availability.http_backoff);
        availability.http_backoff = (availability.http_backoff * 2).min(REMOTE_HTTP_RETRY_MAX);
    }

    fn current_http_backoff_error(&self) -> Option<anyhow::Error> {
        let availability = self.availability.lock().expect("lock poisoned");
        let next_retry_at = availability.next_http_retry_at?;
        if Instant::now() >= next_retry_at {
            return None;
        }

        let seconds = next_retry_at
            .saturating_duration_since(Instant::now())
            .as_secs()
            .max(1);
        let last_error = availability
            .last_error
            .as_deref()
            .unwrap_or("machine temporarily unreachable");
        Some(anyhow!(
            "machine temporarily unreachable, retrying in {seconds}s: {last_error}"
        ))
    }

    fn endpoint(&self, path: &str) -> Result<reqwest::Url> {
        Ok(self.base_url.join(path)?)
    }

    fn session_endpoint(&self, id: &str, suffix: Option<&str>) -> Result<reqwest::Url> {
        let mut url = self.base_url.join("api/sessions")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow!("invalid base URL"))?;
            segments.push(id);
            if let Some(suffix) = suffix {
                segments.push(suffix);
            }
        }
        Ok(url)
    }

    fn websocket_endpoint(&self, path: &str) -> Result<reqwest::Url> {
        let mut url = self.endpoint(path)?;
        match url.scheme() {
            "http" => {
                url.set_scheme("ws")
                    .map_err(|_| anyhow!("failed to convert HTTP endpoint to websocket"))?;
            }
            "https" => {
                url.set_scheme("wss")
                    .map_err(|_| anyhow!("failed to convert HTTPS endpoint to websocket"))?;
            }
            "ws" | "wss" => {}
            other => {
                return Err(anyhow!(
                    "unsupported URL scheme `{other}` for websocket stream"
                ));
            }
        }
        Ok(url)
    }

    fn stream_endpoint(
        &self,
        query: SessionQuery,
        refresh_every: Duration,
    ) -> Result<reqwest::Url> {
        let mut url = self.websocket_endpoint("api/stream")?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair(
                "include_archived",
                if query.include_archived {
                    "true"
                } else {
                    "false"
                },
            );
            if let Some(limit) = query.limit {
                pairs.append_pair("limit", &limit.to_string());
            }
            pairs.append_pair(
                "interval_ms",
                &refresh_every.as_millis().clamp(1_000, 15_000).to_string(),
            );
        }
        Ok(url)
    }

    async fn decode_json<T: for<'de> Deserialize<'de>>(response: reqwest::Response) -> Result<T> {
        if response.status().is_success() {
            return Ok(response.json().await?);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let error = serde_json::from_str::<ApiErrorBody>(&body)
            .map(|body| body.error)
            .unwrap_or(body);
        Err(anyhow!("agent request failed ({status}): {error}"))
    }
}

#[derive(Deserialize)]
struct ApiErrorBody {
    error: String,
}

#[async_trait]
impl MonitorClient for RemoteMonitorClient {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        if let Some(error) = self.current_http_backoff_error() {
            return Err(error);
        }

        let mut request = self
            .http
            .get(self.endpoint("api/sessions")?)
            .query(&[("include_archived", query.include_archived)]);
        if let Some(limit) = query.limit {
            request = request.query(&[("limit", limit)]);
        }
        let response = request.send().await.map_err(|error| {
            let message = error.to_string();
            self.note_http_failure(message.clone());
            anyhow!(message)
        })?;
        let list = Self::decode_json(response).await.map_err(|error| {
            let message = error.to_string();
            self.note_http_failure(message.clone());
            anyhow!(message)
        })?;
        self.note_http_success();
        Ok(list)
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        if let Some(error) = self.current_http_backoff_error() {
            return Err(error);
        }

        let response = self
            .http
            .get(self.session_endpoint(id, None)?)
            .send()
            .await
            .map_err(|error| {
                let message = error.to_string();
                self.note_http_failure(message.clone());
                anyhow!(message)
            })?;
        let detail = Self::decode_json(response).await.map_err(|error| {
            let message = error.to_string();
            self.note_http_failure(message.clone());
            anyhow!(message)
        })?;
        self.note_http_success();
        Ok(detail)
    }

    async fn open_session_app(&self, id: &str) -> Result<OpenActionResponse> {
        if let Some(error) = self.current_http_backoff_error() {
            return Err(error);
        }

        let response = self
            .http
            .post(self.session_endpoint(id, Some("open-app"))?)
            .send()
            .await
            .map_err(|error| {
                let message = error.to_string();
                self.note_http_failure(message.clone());
                anyhow!(message)
            })?;
        let action = Self::decode_json(response).await.map_err(|error| {
            let message = error.to_string();
            self.note_http_failure(message.clone());
            anyhow!(message)
        })?;
        self.note_http_success();
        Ok(action)
    }

    async fn subscribe_sessions(
        &self,
        query: SessionQuery,
        refresh_every: Duration,
    ) -> Result<Option<UnboundedReceiver<SessionList>>> {
        let stream_url = self.stream_endpoint(query, refresh_every)?;
        let (tx, rx) = unbounded_channel();
        let client = self.clone();

        tokio::spawn(async move {
            let mut reconnect_delay = REMOTE_STREAM_RECONNECT_MIN;
            let mut last_signature: Option<String> = None;

            loop {
                match timeout(REMOTE_CONNECT_TIMEOUT, connect_async(stream_url.as_str())).await {
                    Ok(Ok((stream, _))) => {
                        client.note_http_success();
                        reconnect_delay = REMOTE_STREAM_RECONNECT_MIN;
                        let (_, mut read) = stream.split();

                        while let Some(message) = read.next().await {
                            let Ok(message) = message else {
                                break;
                            };

                            match message {
                                Message::Text(text) => {
                                    let Ok(list) = serde_json::from_str::<SessionList>(&text)
                                    else {
                                        continue;
                                    };
                                    let Ok(signature) = session_list_signature(&list) else {
                                        continue;
                                    };
                                    last_signature = Some(signature);
                                    if tx.send(list).is_err() {
                                        return;
                                    }
                                }
                                Message::Close(_) => break,
                                _ => {}
                            }
                        }
                        client.note_http_failure("remote session stream disconnected".to_string());
                    }
                    Ok(Err(error)) => {
                        client.note_http_failure(error.to_string());
                        tracing::debug!(%stream_url, ?error, "remote session stream connect failed");
                    }
                    Err(_) => {
                        client.note_http_failure(
                            "remote session stream connect timed out".to_string(),
                        );
                        tracing::debug!(%stream_url, "remote session stream connect timed out");
                    }
                }

                let offline = client.offline_session_list(client.offline_error());
                if let Ok(signature) = session_list_signature(&offline)
                    && last_signature.as_deref() != Some(signature.as_str())
                {
                    last_signature = Some(signature);
                    if tx.send(offline).is_err() {
                        return;
                    }
                }

                if tx.is_closed() {
                    return;
                }

                sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(REMOTE_STREAM_RECONNECT_MAX);
            }
        });

        Ok(Some(rx))
    }
}

#[derive(Clone)]
struct NamespacedClient {
    namespace: String,
    fallback_machine_label: Option<String>,
    client: Arc<dyn MonitorClient>,
}

#[derive(Clone, Default)]
pub struct MultiMonitorClient {
    clients: Vec<NamespacedClient>,
}

impl MultiMonitorClient {
    pub fn new() -> Self {
        Self {
            clients: Vec::new(),
        }
    }

    pub fn push_client(
        &mut self,
        namespace: impl Into<String>,
        fallback_machine_label: Option<String>,
        client: Arc<dyn MonitorClient>,
    ) {
        self.clients.push(NamespacedClient {
            namespace: namespace.into(),
            fallback_machine_label,
            client,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

#[async_trait]
impl MonitorClient for MultiMonitorClient {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        self.list_sessions_with_progress(query, None).await
    }

    async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        let mut sessions = Vec::new();
        let mut quotas = Vec::new();
        let mut machines = Vec::new();
        let mut total_sessions = 0usize;
        let mut join_set = JoinSet::new();
        let progress_state = Arc::new(Mutex::new(HashMap::<String, (usize, usize)>::new()));

        for client in &self.clients {
            let client = client.clone();
            let query = query.clone();
            let progress_tx = progress.clone();
            let progress_state = progress_state.clone();
            let namespace = client.namespace.clone();
            let (client_progress_tx, mut client_progress_rx) =
                unbounded_channel::<SessionLoadProgress>();
            if let Some(progress_tx) = progress_tx {
                tokio::spawn(async move {
                    while let Some(update) = client_progress_rx.recv().await {
                        let aggregate = {
                            let mut state = progress_state.lock().expect("lock poisoned");
                            state.insert(
                                namespace.clone(),
                                (update.loaded_sessions, update.total_sessions),
                            );
                            SessionLoadProgress {
                                loaded_sessions: state.values().map(|(loaded, _)| *loaded).sum(),
                                total_sessions: state.values().map(|(_, total)| *total).sum(),
                                sources: state
                                    .iter()
                                    .map(|(source, (loaded_sessions, total_sessions))| {
                                        agent_cow_core::SessionLoadSourceProgress {
                                            source: source.clone(),
                                            loaded_sessions: *loaded_sessions,
                                            total_sessions: *total_sessions,
                                        }
                                    })
                                    .collect(),
                            }
                        };
                        let _ = progress_tx.send(aggregate);
                    }
                });
            }

            join_set.spawn(async move {
                let result = client
                    .client
                    .list_sessions_with_progress(query, Some(client_progress_tx))
                    .await;
                (client.namespace, client.fallback_machine_label, result)
            });
        }

        while let Some(result) = join_set.join_next().await {
            let (namespace, fallback_machine_label, result) = result?;
            match result {
                Ok(mut list) => {
                    total_sessions += list.overview.total_sessions;
                    quotas.append(&mut list.overview.quotas);
                    machines.extend(namespace_machines(
                        &namespace,
                        fallback_machine_label.as_deref(),
                        &list.overview.machines,
                        &list.sessions,
                    ));
                    for mut session in list.sessions {
                        namespace_summary(
                            &namespace,
                            fallback_machine_label.as_deref(),
                            &mut session,
                        );
                        sessions.push(session);
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        source = %namespace,
                        ?error,
                        "monitor source list failed; keeping source offline"
                    );
                    machines.push(source_machine_overview(
                        &namespace,
                        fallback_machine_label.as_deref(),
                        &[],
                        false,
                        Some("Machine temporarily unreachable".to_string()),
                    ));
                }
            }
        }

        sessions.sort_by_key(|session| Reverse(session.updated_at));
        if let Some(limit) = query.limit {
            sessions.truncate(limit);
        }

        let list = SessionList {
            generated_at: Utc::now(),
            overview: build_combined_overview(&sessions, quotas, total_sessions, machines),
            sessions,
        };

        if let Some(progress) = progress {
            let _ = progress.send(SessionLoadProgress {
                loaded_sessions: list.sessions.len(),
                total_sessions: list.overview.total_sessions,
                sources: Vec::new(),
            });
        }

        Ok(list)
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let (namespace, raw_id) = id
            .split_once('|')
            .ok_or_else(|| anyhow!("multi-client session ids must be namespaced, got `{id}`"))?;

        let client = self
            .clients
            .iter()
            .find(|client| client.namespace == namespace)
            .ok_or_else(|| anyhow!("no monitor client registered for namespace `{namespace}`"))?;

        let mut detail = client.client.get_session(raw_id).await?;
        namespace_summary(
            namespace,
            client.fallback_machine_label.as_deref(),
            &mut detail.summary,
        );
        Ok(detail)
    }

    async fn open_session_app(&self, id: &str) -> Result<OpenActionResponse> {
        let (namespace, raw_id) = id
            .split_once('|')
            .ok_or_else(|| anyhow!("multi-client session ids must be namespaced, got `{id}`"))?;

        let client = self
            .clients
            .iter()
            .find(|client| client.namespace == namespace)
            .ok_or_else(|| anyhow!("no monitor client registered for namespace `{namespace}`"))?;

        client.client.open_session_app(raw_id).await
    }

    async fn subscribe_sessions(
        &self,
        query: SessionQuery,
        refresh_every: Duration,
    ) -> Result<Option<UnboundedReceiver<SessionList>>> {
        let mut subscriptions = Vec::new();
        let mut latest_lists = HashMap::<String, (Option<String>, SessionList)>::new();

        for client in &self.clients {
            let offline = SessionList {
                generated_at: Utc::now(),
                overview: UsageOverview {
                    machines: vec![source_machine_overview(
                        &client.namespace,
                        client.fallback_machine_label.as_deref(),
                        &[],
                        false,
                        Some("Machine temporarily unreachable".to_string()),
                    )],
                    ..UsageOverview::default()
                },
                sessions: Vec::new(),
            };
            latest_lists.insert(
                client.namespace.clone(),
                (client.fallback_machine_label.clone(), offline),
            );

            match client
                .client
                .subscribe_sessions(query.clone(), refresh_every)
                .await
            {
                Ok(Some(receiver)) => subscriptions.push((
                    client.namespace.clone(),
                    client.fallback_machine_label.clone(),
                    receiver,
                )),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(
                        source = %client.namespace,
                        ?error,
                        "monitor source subscribe failed; keeping source offline"
                    );
                    latest_lists.insert(
                        client.namespace.clone(),
                        (
                            client.fallback_machine_label.clone(),
                            SessionList {
                                generated_at: Utc::now(),
                                overview: UsageOverview {
                                    machines: vec![source_machine_overview(
                                        &client.namespace,
                                        client.fallback_machine_label.as_deref(),
                                        &[],
                                        false,
                                        Some("Machine temporarily unreachable".to_string()),
                                    )],
                                    ..UsageOverview::default()
                                },
                                sessions: Vec::new(),
                            },
                        ),
                    );
                }
            }
        }

        if subscriptions.is_empty() {
            return Ok(None);
        }

        let (tx, rx) = unbounded_channel();
        let (update_tx, mut update_rx) = unbounded_channel();

        for (namespace, fallback_machine_label, mut receiver) in subscriptions {
            let update_tx = update_tx.clone();
            tokio::spawn(async move {
                while let Some(list) = receiver.recv().await {
                    if update_tx
                        .send((namespace.clone(), fallback_machine_label.clone(), list))
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        drop(update_tx);

        tokio::spawn(async move {
            while let Some((namespace, fallback_machine_label, list)) = update_rx.recv().await {
                latest_lists.insert(namespace, (fallback_machine_label, list));
                let combined = build_combined_session_list(
                    latest_lists
                        .iter()
                        .map(|(namespace, (fallback_machine_label, list))| {
                            (namespace.as_str(), fallback_machine_label.as_deref(), list)
                        }),
                    query.limit,
                );

                if tx.send(combined).is_err() {
                    break;
                }
            }
        });

        Ok(Some(rx))
    }
}

fn namespace_summary(
    namespace: &str,
    fallback_machine_label: Option<&str>,
    summary: &mut SessionSummary,
) {
    if !summary.id.contains('|') {
        summary.id = format!("{namespace}|{}", summary.id);
    }

    if let Some(label) = fallback_machine_label {
        let replace_label = summary.machine_label.trim().is_empty()
            || summary.machine_label.eq_ignore_ascii_case("local");
        if replace_label {
            summary.machine_label = label.to_string();
        }

        let replace_id = summary.machine_id.trim().is_empty()
            || summary.machine_id.eq_ignore_ascii_case("local");
        if replace_id {
            summary.machine_id = label.to_string();
        }
    }
}

fn build_combined_overview(
    sessions: &[SessionSummary],
    quotas: Vec<ProviderQuota>,
    total_sessions: usize,
    mut machines: Vec<MachineOverview>,
) -> UsageOverview {
    machines.sort_by(|left, right| left.machine_label.cmp(&right.machine_label));
    machines.dedup_by(|left, right| left.source == right.source);
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
        quotas,
        machines,
    }
}

fn build_combined_session_list<'a>(
    lists: impl Iterator<Item = (&'a str, Option<&'a str>, &'a SessionList)>,
    limit: Option<usize>,
) -> SessionList {
    let mut sessions = Vec::new();
    let mut quotas = Vec::new();
    let mut machines = Vec::new();
    let mut total_sessions = 0usize;

    for (namespace, fallback_machine_label, list) in lists {
        total_sessions += list.overview.total_sessions;
        quotas.extend(list.overview.quotas.clone());
        machines.extend(namespace_machines(
            namespace,
            fallback_machine_label,
            &list.overview.machines,
            &list.sessions,
        ));
        for mut session in list.sessions.clone() {
            namespace_summary(namespace, fallback_machine_label, &mut session);
            sessions.push(session);
        }
    }

    sessions.sort_by_key(|session| Reverse(session.updated_at));
    if let Some(limit) = limit {
        sessions.truncate(limit);
    }

    SessionList {
        generated_at: Utc::now(),
        overview: build_combined_overview(&sessions, quotas, total_sessions, machines),
        sessions,
    }
}

fn namespace_machine(
    namespace: &str,
    fallback_machine_label: Option<&str>,
    machine: &MachineOverview,
) -> MachineOverview {
    let mut machine = machine.clone();
    machine.source = namespace.to_string();

    if let Some(label) = fallback_machine_label {
        let replace_label = machine.machine_label.trim().is_empty()
            || machine.machine_label.eq_ignore_ascii_case("local");
        if replace_label {
            machine.machine_label = label.to_string();
        }

        let replace_id = machine.machine_id.trim().is_empty()
            || machine.machine_id.eq_ignore_ascii_case("local");
        if replace_id {
            machine.machine_id = label.to_string();
        }
    }

    machine
}

fn source_machine_overview(
    namespace: &str,
    fallback_machine_label: Option<&str>,
    sessions: &[SessionSummary],
    reachable: bool,
    error: Option<String>,
) -> MachineOverview {
    let session = sessions.first();
    let machine_label = session
        .map(|session| session.machine_label.as_str())
        .filter(|label| !label.trim().is_empty() && !label.eq_ignore_ascii_case("local"))
        .or(fallback_machine_label)
        .unwrap_or(namespace)
        .to_string();
    let machine_id = session
        .map(|session| session.machine_id.as_str())
        .filter(|id| !id.trim().is_empty() && !id.eq_ignore_ascii_case("local"))
        .unwrap_or(machine_label.as_str())
        .to_string();

    MachineOverview {
        source: namespace.to_string(),
        machine_id,
        machine_label,
        reachable,
        error,
    }
}

fn namespace_machines(
    namespace: &str,
    fallback_machine_label: Option<&str>,
    machines: &[MachineOverview],
    sessions: &[SessionSummary],
) -> Vec<MachineOverview> {
    if !machines.is_empty() {
        return machines
            .iter()
            .map(|machine| namespace_machine(namespace, fallback_machine_label, machine))
            .collect();
    }

    vec![source_machine_overview(
        namespace,
        fallback_machine_label,
        sessions,
        true,
        None,
    )]
}

fn session_list_signature(list: &SessionList) -> Result<String> {
    Ok(serde_json::to_string(&(&list.overview, &list.sessions))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_cow_core::{
        SessionActivityState, SessionCost, SessionStatus, SessionStatusKind, StatusConfidence,
        TokenUsage, UsageOverview,
    };

    fn fixture_summary(machine_id: &str, machine_label: &str) -> SessionSummary {
        SessionSummary {
            id: "codex:abc".to_string(),
            provider: agent_cow_core::ProviderKind::Codex,
            machine_id: machine_id.to_string(),
            machine_label: machine_label.to_string(),
            title: "test".to_string(),
            cwd: "/tmp".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            run_started_at: None,
            run_active: false,
            status: SessionStatus {
                kind: SessionStatusKind::Running,
                confidence: StatusConfidence::Exact,
                reason: String::new(),
            },
            activity_state: SessionActivityState::Thinking,
            archived: false,
            model: Some("gpt-5.4".to_string()),
            agent_role: None,
            git_branch: None,
            git_origin_url: None,
            tokens: TokenUsage {
                total_tokens: 1,
                ..TokenUsage::default()
            },
            cost: Some(SessionCost::default()),
            context_window: None,
            rollout_path: None,
            navigation: Vec::new(),
        }
    }

    #[test]
    fn remote_local_label_falls_back_to_machine_host() {
        let mut summary = fixture_summary("local", "local");
        namespace_summary("remote1", Some("192.168.1.10"), &mut summary);
        assert_eq!(summary.machine_label, "192.168.1.10");
        assert_eq!(summary.machine_id, "192.168.1.10");
        assert_eq!(summary.id, "remote1|codex:abc");
    }

    #[test]
    fn explicit_remote_machine_label_is_preserved() {
        let mut summary = fixture_summary("buildbox", "buildbox");
        namespace_summary("remote1", Some("192.168.1.10"), &mut summary);
        assert_eq!(summary.machine_label, "buildbox");
        assert_eq!(summary.machine_id, "buildbox");
    }

    #[test]
    fn combined_session_list_keeps_unreachable_machine_visible() {
        let offline = SessionList {
            generated_at: Utc::now(),
            overview: UsageOverview {
                machines: vec![MachineOverview {
                    source: String::new(),
                    machine_id: "192.168.1.10".to_string(),
                    machine_label: "192.168.1.10".to_string(),
                    reachable: false,
                    error: Some("Machine temporarily unreachable".to_string()),
                }],
                ..UsageOverview::default()
            },
            sessions: Vec::new(),
        };

        let combined = build_combined_session_list(
            [("remote1", Some("192.168.1.10"), &offline)].into_iter(),
            None,
        );

        assert!(combined.sessions.is_empty());
        assert_eq!(combined.overview.machines.len(), 1);
        assert_eq!(combined.overview.machines[0].machine_label, "192.168.1.10");
        assert!(!combined.overview.machines[0].reachable);
    }
}
