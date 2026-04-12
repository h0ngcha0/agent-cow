use std::{net::SocketAddr, sync::Arc};

use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cow_watch_core::{MonitorService, NavigationKind, SessionQuery};
use serde::Deserialize;

use crate::{client::OpenActionResponse, open};

#[derive(Clone)]
struct ApiState {
    service: Arc<MonitorService>,
    local_machine_id: String,
}

#[derive(Deserialize)]
struct ListParams {
    include_archived: Option<bool>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct OpenParams {
    kind: String,
}

pub async fn run(service: MonitorService, bind: SocketAddr) -> Result<()> {
    let state = ApiState {
        service: Arc::new(service),
        local_machine_id: open::local_machine_id(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/sessions", get(list_sessions))
        .route("/api/sessions/{id}", get(get_session))
        .route("/api/sessions/{id}/open", post(open_session_target))
        .route("/api/sessions/{id}/open-app", post(open_session_app))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("agent listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

async fn list_sessions(
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Result<Json<cow_watch_core::SessionList>, ApiError> {
    let sessions = state
        .service
        .list_sessions(SessionQuery {
            include_archived: params.include_archived.unwrap_or(false),
            limit: params.limit,
        })
        .await?;

    Ok(Json(sessions))
}

async fn get_session(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<cow_watch_core::SessionDetail>, ApiError> {
    Ok(Json(state.service.get_session(&id).await?))
}

async fn open_session_target(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<OpenParams>,
) -> Result<Json<OpenActionResponse>, ApiError> {
    let session = state.service.get_session(&id).await?;
    let action =
        open::open_session_navigation(&session.summary, parse_navigation_kind(&params.kind)?)?;

    Ok(Json(OpenActionResponse {
        label: action.label,
        target: action.target,
    }))
}

async fn open_session_app(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<OpenActionResponse>, ApiError> {
    let session = state.service.get_session(&id).await?;

    if session.summary.machine_id != state.local_machine_id {
        return Err(anyhow!("opening provider apps is only supported for local sessions").into());
    }

    let action = open::open_session_app(&session.summary, &state.local_machine_id)?;

    Ok(Json(OpenActionResponse {
        label: action.label,
        target: action.target,
    }))
}

fn parse_navigation_kind(value: &str) -> Result<NavigationKind> {
    match value {
        "thread_id" => Ok(NavigationKind::ThreadId),
        "rollout_path" => Ok(NavigationKind::RolloutPath),
        "working_directory" => Ok(NavigationKind::WorkingDirectory),
        _ => Err(anyhow!("unsupported navigation kind `{value}`")),
    }
}

struct ApiError(anyhow::Error);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": self.0.to_string(),
        });
        (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
    }
}
