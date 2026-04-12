use std::{net::SocketAddr, sync::Arc, time::Duration};

use agent_cow_core::{MonitorService, NavigationKind, SessionQuery};
use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    extract::{
        Path as AxumPath, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::MissedTickBehavior;

use crate::{client::OpenActionResponse, open};

const STREAM_REFRESH_MIN: Duration = Duration::from_secs(1);
const STREAM_REFRESH_MAX: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct ApiState {
    service: Arc<MonitorService>,
    local_machine_id: String,
}

#[derive(Deserialize)]
struct ListParams {
    include_archived: Option<bool>,
    limit: Option<usize>,
    interval_ms: Option<u64>,
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
        .route("/api/stream", get(stream_sessions))
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
) -> Result<Json<agent_cow_core::SessionList>, ApiError> {
    let sessions = state
        .service
        .list_sessions(SessionQuery {
            include_archived: params.include_archived.unwrap_or(false),
            limit: params.limit,
        })
        .await?;

    Ok(Json(sessions))
}

async fn stream_sessions(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Response {
    let query = SessionQuery {
        include_archived: params.include_archived.unwrap_or(false),
        limit: params.limit,
    };
    let interval = params
        .interval_ms
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(5))
        .clamp(STREAM_REFRESH_MIN, STREAM_REFRESH_MAX);

    ws.on_upgrade(move |socket| stream_sessions_socket(socket, state, query, interval))
}

async fn get_session(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<agent_cow_core::SessionDetail>, ApiError> {
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

async fn stream_sessions_socket(
    socket: WebSocket,
    state: ApiState,
    query: SessionQuery,
    interval: Duration,
) {
    let (mut sender, mut receiver) = socket.split();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_signature: Option<String> = None;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let list = match state.service.list_sessions(query.clone()).await {
                    Ok(list) => list,
                    Err(error) => {
                        tracing::warn!(?error, "session stream refresh failed");
                        continue;
                    }
                };

                let signature = match session_list_signature(&list) {
                    Ok(signature) => signature,
                    Err(error) => {
                        tracing::warn!(?error, "session stream signature failed");
                        continue;
                    }
                };

                if last_signature.as_deref() == Some(signature.as_str()) {
                    continue;
                }

                let payload = match serde_json::to_string(&list) {
                    Ok(payload) => payload,
                    Err(error) => {
                        tracing::warn!(?error, "session stream serialization failed");
                        continue;
                    }
                };

                if sender.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }

                last_signature = Some(signature);
            }
            message = receiver.next() => match message {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(error)) => {
                    tracing::debug!(?error, "session stream receive failed");
                    break;
                }
                _ => {}
            }
        }
    }
}

fn session_list_signature(list: &agent_cow_core::SessionList) -> Result<String> {
    Ok(serde_json::to_string(&(&list.overview, &list.sessions))?)
}
