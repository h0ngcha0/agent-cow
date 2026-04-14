use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;

use crate::{SessionDetail, SessionList, SessionSummary, UsageOverview};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionQuery {
    pub include_archived: bool,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionLoadSourceProgress {
    pub source: String,
    pub loaded_sessions: usize,
    pub total_sessions: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionLoadProgress {
    pub loaded_sessions: usize,
    pub total_sessions: usize,
    pub sources: Vec<SessionLoadSourceProgress>,
}

#[async_trait]
pub trait SessionSource: Send + Sync {
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
    async fn get_session(&self, id: &str) -> Result<SessionDetail>;
}

#[derive(Clone)]
pub struct CombinedSource {
    sources: Vec<NamespacedSource>,
}

impl Default for CombinedSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct NamespacedSource {
    namespace: String,
    source: Arc<dyn SessionSource>,
}

impl CombinedSource {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn with_source(
        mut self,
        namespace: impl Into<String>,
        source: Arc<dyn SessionSource>,
    ) -> Self {
        self.push_source(namespace, source);
        self
    }

    pub fn push_source(&mut self, namespace: impl Into<String>, source: Arc<dyn SessionSource>) {
        self.sources.push(NamespacedSource {
            namespace: namespace.into(),
            source,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

#[async_trait]
impl SessionSource for CombinedSource {
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

        for source in &self.sources {
            let source = source.clone();
            let query = query.clone();
            let progress_tx = progress.clone();
            let progress_state = progress_state.clone();
            let namespace = source.namespace.clone();
            let (provider_progress_tx, mut provider_progress_rx) =
                unbounded_channel::<SessionLoadProgress>();
            if let Some(progress_tx) = progress_tx {
                tokio::spawn(async move {
                    while let Some(update) = provider_progress_rx.recv().await {
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
                                        SessionLoadSourceProgress {
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
                let list = source
                    .source
                    .list_sessions_with_progress(
                        SessionQuery {
                            include_archived: query.include_archived,
                            limit: query.limit,
                        },
                        Some(provider_progress_tx),
                    )
                    .await?;
                Ok::<_, anyhow::Error>((source.namespace, list))
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
            .split_once(':')
            .ok_or_else(|| anyhow!("combined session ids must be namespaced, got `{id}`"))?;

        let source = self
            .sources
            .iter()
            .find(|source| source.namespace == namespace)
            .ok_or_else(|| anyhow!("no session source registered for namespace `{namespace}`"))?;

        let mut detail = source.source.get_session(raw_id).await?;
        namespace_summary(namespace, &mut detail.summary);
        Ok(detail)
    }
}

#[derive(Clone)]
pub struct MonitorService {
    source: Arc<dyn SessionSource>,
}

impl MonitorService {
    pub fn new(source: Arc<dyn SessionSource>) -> Self {
        Self { source }
    }

    pub async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList> {
        self.source.list_sessions(query).await
    }

    pub async fn list_sessions_with_progress(
        &self,
        query: SessionQuery,
        progress: Option<UnboundedSender<SessionLoadProgress>>,
    ) -> Result<SessionList> {
        self.source
            .list_sessions_with_progress(query, progress)
            .await
    }

    pub async fn latest_session(&self) -> Result<SessionDetail> {
        let list = self
            .source
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

        self.source.get_session(&latest.id).await
    }

    pub async fn get_session(&self, id: &str) -> Result<SessionDetail> {
        self.source.get_session(id).await
    }
}

fn namespace_summary(namespace: &str, summary: &mut SessionSummary) {
    if !summary.id.starts_with(namespace) {
        summary.id = format!("{namespace}:{}", summary.id);
    }
}

fn build_combined_overview(
    sessions: &[SessionSummary],
    quotas: Vec<crate::ProviderQuota>,
    total_sessions: usize,
) -> UsageOverview {
    let mut machines = HashMap::<String, crate::MachineOverview>::new();
    for session in sessions {
        machines
            .entry(session.machine_id.clone())
            .or_insert_with(|| crate::MachineOverview {
                source: session.machine_id.clone(),
                machine_id: session.machine_id.clone(),
                machine_label: session.machine_label.clone(),
                reachable: true,
                error: None,
            });
    }

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
        machines: machines.into_values().collect(),
    }
}
