use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_cow_core::{
    MonitorService, ProviderQuota, SessionDetail, SessionList, SessionLoadProgress, SessionQuery,
    SessionSummary, UsageOverview,
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
        })
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
        let mut request = self
            .http
            .get(self.endpoint("api/sessions")?)
            .query(&[("include_archived", query.include_archived)]);
        if let Some(limit) = query.limit {
            request = request.query(&[("limit", limit)]);
        }
        let response = request.send().await?;
        Self::decode_json(response).await
    }

    async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        let response = self
            .http
            .get(self.session_endpoint(id, None)?)
            .send()
            .await?;
        Self::decode_json(response).await
    }

    async fn open_session_app(&self, id: &str) -> Result<OpenActionResponse> {
        let response = self
            .http
            .post(self.session_endpoint(id, Some("open-app"))?)
            .send()
            .await?;
        Self::decode_json(response).await
    }

    async fn subscribe_sessions(
        &self,
        query: SessionQuery,
        refresh_every: Duration,
    ) -> Result<Option<UnboundedReceiver<SessionList>>> {
        let stream_url = self.stream_endpoint(query, refresh_every)?;
        let (tx, rx) = unbounded_channel();

        tokio::spawn(async move {
            let mut reconnect_delay = REMOTE_STREAM_RECONNECT_MIN;

            loop {
                match timeout(REMOTE_CONNECT_TIMEOUT, connect_async(stream_url.as_str())).await {
                    Ok(Ok((stream, _))) => {
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
                                    if tx.send(list).is_err() {
                                        return;
                                    }
                                }
                                Message::Close(_) => break,
                                _ => {}
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(%stream_url, ?error, "remote session stream connect failed");
                    }
                    Err(_) => {
                        tracing::debug!(%stream_url, "remote session stream connect timed out");
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

    pub fn push_client(&mut self, namespace: impl Into<String>, client: Arc<dyn MonitorClient>) {
        self.clients.push(NamespacedClient {
            namespace: namespace.into(),
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
                let list = client
                    .client
                    .list_sessions_with_progress(query, Some(client_progress_tx))
                    .await?;
                Ok::<_, anyhow::Error>((client.namespace, list))
            });
        }

        while let Some(result) = join_set.join_next().await {
            let (namespace, mut list) = result??;
            total_sessions += list.overview.total_sessions;
            quotas.append(&mut list.overview.quotas);
            for mut session in list.sessions {
                namespace_summary(&namespace, &mut session);
                sessions.push(session);
            }
        }

        sessions.sort_by_key(|session| Reverse(session.updated_at));
        if let Some(limit) = query.limit {
            sessions.truncate(limit);
        }

        let list = SessionList {
            generated_at: Utc::now(),
            overview: build_combined_overview(&sessions, quotas, total_sessions),
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
        namespace_summary(namespace, &mut detail.summary);
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

        for client in &self.clients {
            if let Some(receiver) = client
                .client
                .subscribe_sessions(query.clone(), refresh_every)
                .await?
            {
                subscriptions.push((client.namespace.clone(), receiver));
            }
        }

        if subscriptions.is_empty() {
            return Ok(None);
        }

        let expected_clients = subscriptions.len();
        let (tx, rx) = unbounded_channel();
        let (update_tx, mut update_rx) = unbounded_channel();

        for (namespace, mut receiver) in subscriptions {
            let update_tx = update_tx.clone();
            tokio::spawn(async move {
                while let Some(list) = receiver.recv().await {
                    if update_tx.send((namespace.clone(), list)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(update_tx);

        tokio::spawn(async move {
            let mut latest_lists = HashMap::<String, SessionList>::new();

            while let Some((namespace, list)) = update_rx.recv().await {
                latest_lists.insert(namespace, list);
                if latest_lists.len() < expected_clients {
                    continue;
                }

                let combined = build_combined_session_list(
                    latest_lists
                        .iter()
                        .map(|(namespace, list)| (namespace.as_str(), list)),
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

fn namespace_summary(namespace: &str, summary: &mut SessionSummary) {
    if !summary.id.contains('|') {
        summary.id = format!("{namespace}|{}", summary.id);
    }
}

fn build_combined_overview(
    sessions: &[SessionSummary],
    quotas: Vec<ProviderQuota>,
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
        quotas,
    }
}

fn build_combined_session_list<'a>(
    lists: impl Iterator<Item = (&'a str, &'a SessionList)>,
    limit: Option<usize>,
) -> SessionList {
    let mut sessions = Vec::new();
    let mut quotas = Vec::new();
    let mut total_sessions = 0usize;

    for (namespace, list) in lists {
        total_sessions += list.overview.total_sessions;
        quotas.extend(list.overview.quotas.clone());
        for mut session in list.sessions.clone() {
            namespace_summary(namespace, &mut session);
            sessions.push(session);
        }
    }

    sessions.sort_by_key(|session| Reverse(session.updated_at));
    if let Some(limit) = limit {
        sessions.truncate(limit);
    }

    SessionList {
        generated_at: Utc::now(),
        overview: build_combined_overview(&sessions, quotas, total_sessions),
        sessions,
    }
}

fn session_list_signature(list: &SessionList) -> Result<String> {
    Ok(serde_json::to_string(&(&list.overview, &list.sessions))?)
}
