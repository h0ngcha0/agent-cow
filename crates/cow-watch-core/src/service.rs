use std::{cmp::Reverse, sync::Arc};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{SessionDetail, SessionList, SessionSummary, UsageOverview};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionQuery {
    pub include_archived: bool,
    pub limit: Option<usize>,
}

#[async_trait]
pub trait SessionSource: Send + Sync {
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionList>;
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
        let mut sessions = Vec::new();
        let mut quotas = Vec::new();

        for source in &self.sources {
            let mut list = source
                .source
                .list_sessions(SessionQuery {
                    include_archived: query.include_archived,
                    limit: None,
                })
                .await?;

            quotas.append(&mut list.overview.quotas);
            for mut session in list.sessions {
                namespace_summary(&source.namespace, &mut session);
                sessions.push(session);
            }
        }

        sessions.sort_by_key(|session| Reverse(session.updated_at));
        if let Some(limit) = query.limit {
            sessions.truncate(limit);
        }

        Ok(SessionList {
            generated_at: Utc::now(),
            overview: build_combined_overview(&sessions, quotas),
            sessions,
        })
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
        quotas,
    }
}
