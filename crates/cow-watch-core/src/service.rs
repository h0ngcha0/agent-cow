use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{SessionDetail, SessionList};

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
