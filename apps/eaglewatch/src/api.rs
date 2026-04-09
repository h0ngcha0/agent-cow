use std::{net::SocketAddr, sync::Arc};

use crate::open;
use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use eaglewatch_core::{MonitorService, NavigationKind, SessionQuery};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize)]
struct OpenResponse {
    ok: bool,
    label: String,
    target: String,
}

pub async fn run(service: MonitorService, bind: SocketAddr) -> Result<()> {
    let state = ApiState {
        service: Arc::new(service),
        local_machine_id: open::local_machine_id(),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/sessions", get(list_sessions))
        .route("/api/sessions/{id}", get(get_session))
        .route("/api/sessions/{id}/open", post(open_session_target))
        .route("/api/sessions/{id}/open-app", post(open_session_app))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

async fn list_sessions(
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Result<Json<eaglewatch_core::SessionList>, ApiError> {
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
) -> Result<Json<eaglewatch_core::SessionDetail>, ApiError> {
    let session = state.service.get_session(&id).await?;
    Ok(Json(session))
}

async fn open_session_target(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<OpenParams>,
) -> Result<Json<OpenResponse>, ApiError> {
    let session = state.service.get_session(&id).await?;
    let action =
        open::open_session_navigation(&session.summary, parse_navigation_kind(&params.kind)?)?;

    Ok(Json(OpenResponse {
        ok: true,
        label: action.label,
        target: action.target,
    }))
}

async fn open_session_app(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<OpenResponse>, ApiError> {
    let session = state.service.get_session(&id).await?;

    if session.summary.machine_id != state.local_machine_id {
        return Err(anyhow!("opening provider apps is only supported for local sessions").into());
    }

    let action = open::open_session_app(&session.summary, &state.local_machine_id)?;

    Ok(Json(OpenResponse {
        ok: true,
        label: action.label,
        target: action.target,
    }))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
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

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>EagleWatch</title>
    <style>
      :root {
        --bg: #08111c;
        --bg-elevated: rgba(18, 29, 44, 0.82);
        --border: rgba(141, 166, 194, 0.18);
        --text: #ebf2ff;
        --muted: #98adc6;
        --accent: #4fd1c5;
        --accent-2: #f6ad55;
        --danger: #fc8181;
        --ok: #68d391;
      }

      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top left, rgba(79, 209, 197, 0.16), transparent 28%),
          radial-gradient(circle at top right, rgba(246, 173, 85, 0.18), transparent 32%),
          linear-gradient(180deg, #08111c 0%, #050b14 100%);
        color: var(--text);
        font-family: "Iosevka Aile", "IBM Plex Sans", "SF Pro Display", sans-serif;
      }

      .shell {
        display: grid;
        grid-template-columns: minmax(360px, 44vw) 1fr;
        gap: 18px;
        padding: 20px;
      }

      .panel {
        background: var(--bg-elevated);
        border: 1px solid var(--border);
        border-radius: 20px;
        backdrop-filter: blur(14px);
        box-shadow: 0 14px 40px rgba(0, 0, 0, 0.25);
      }

      .hero {
        padding: 20px 22px 14px;
      }

      .hero h1 {
        margin: 0;
        font-size: 28px;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }

      .hero p {
        margin: 8px 0 0;
        color: var(--muted);
        max-width: 60ch;
      }

      .sessions {
        overflow: hidden;
      }

      .sessions header,
      .detail header {
        padding: 16px 20px;
        border-bottom: 1px solid var(--border);
        display: flex;
        align-items: center;
        justify-content: space-between;
      }

      .session-list {
        max-height: calc(100vh - 170px);
        overflow: auto;
      }

      .session-row {
        padding: 14px 20px;
        border-bottom: 1px solid rgba(141, 166, 194, 0.12);
        cursor: pointer;
        transition: background 140ms ease, transform 140ms ease;
      }

      .session-row:hover,
      .session-row.active {
        background: rgba(79, 209, 197, 0.08);
      }

      .session-row.active {
        box-shadow: inset 3px 0 0 var(--accent);
      }

      .title {
        font-weight: 600;
        line-height: 1.4;
        display: -webkit-box;
        -webkit-box-orient: vertical;
        -webkit-line-clamp: 3;
        overflow: hidden;
      }

      .meta,
      .detail-grid,
      .events li,
      .tools li {
        color: var(--muted);
      }

      .meta {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin-top: 8px;
        font-size: 13px;
      }

      .status {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .status.running,
      .status.tool_busy { background: rgba(79, 209, 197, 0.14); color: var(--accent); }
      .status.completed { background: rgba(104, 211, 145, 0.14); color: var(--ok); }
      .status.waiting_input { background: rgba(246, 173, 85, 0.14); color: var(--accent-2); }
      .status.stale,
      .status.idle { background: rgba(152, 173, 198, 0.12); color: var(--muted); }
      .status.failed { background: rgba(252, 129, 129, 0.14); color: var(--danger); }

      .detail {
        display: flex;
        flex-direction: column;
        min-height: calc(100vh - 40px);
      }

      .detail-body {
        padding: 20px;
        overflow: auto;
      }

      .detail-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px 18px;
        margin-bottom: 20px;
      }

      .detail-grid strong,
      .section h2 {
        color: var(--text);
      }

      .section {
        margin-top: 18px;
      }

      .actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 12px;
      }

      .action-button {
        appearance: none;
        border: 1px solid var(--border);
        background: rgba(79, 209, 197, 0.08);
        color: var(--text);
        border-radius: 999px;
        padding: 9px 14px;
        cursor: pointer;
        font: inherit;
        transition: background 140ms ease, transform 140ms ease;
      }

      .action-button:hover {
        background: rgba(79, 209, 197, 0.16);
        transform: translateY(-1px);
      }

      .action-button:disabled {
        opacity: 0.55;
        cursor: default;
        transform: none;
      }

      .notice {
        margin-top: 12px;
        color: var(--muted);
        font-size: 13px;
      }

      .error {
        color: var(--danger);
      }

      .section h2 {
        font-size: 13px;
        margin: 0 0 10px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .events,
      .tools {
        list-style: none;
        padding: 0;
        margin: 0;
        display: grid;
        gap: 10px;
      }

      code {
        font-family: "Berkeley Mono", "SF Mono", monospace;
        color: var(--text);
      }

      @media (max-width: 960px) {
        .shell {
          grid-template-columns: 1fr;
        }
        .detail {
          min-height: unset;
        }
        .session-list {
          max-height: unset;
        }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="panel sessions">
        <div class="hero">
          <h1>EagleWatch</h1>
        </div>
        <header>
          <strong>Sessions</strong>
          <span id="session-count">loading…</span>
        </header>
        <div id="session-list" class="session-list"></div>
      </section>

      <section class="panel detail">
        <header>
          <strong id="detail-title">Select a session</strong>
          <span id="detail-status" class="status idle">idle</span>
        </header>
        <div id="detail-body" class="detail-body">
          <p class="meta">Choose a session to inspect tokens, state inference, tools, and recent activity.</p>
        </div>
      </section>
    </div>

    <script>
      let selectedId = null;
      let sessionsCache = [];

      function relativeTime(dateString) {
        const deltaSeconds = Math.floor((Date.now() - new Date(dateString).getTime()) / 1000);
        if (deltaSeconds < 60) return `${deltaSeconds}s ago`;
        if (deltaSeconds < 3600) return `${Math.floor(deltaSeconds / 60)}m ago`;
        if (deltaSeconds < 86400) return `${Math.floor(deltaSeconds / 3600)}h ago`;
        return `${Math.floor(deltaSeconds / 86400)}d ago`;
      }

      function statusClass(kind) {
        return (kind || "idle").replace(/[^a-z_]/g, "");
      }

      function truncateText(text, maxChars) {
        const chars = Array.from(text || "");
        if (chars.length <= maxChars) return text;
        if (maxChars <= 3) return ".".repeat(maxChars);
        return `${chars.slice(0, maxChars - 3).join("")}...`;
      }

      function renderSessions(sessions) {
        sessionsCache = sessions;
        const root = document.getElementById("session-list");
        document.getElementById("session-count").textContent = `${sessions.length} visible`;
        root.innerHTML = "";

        sessions.forEach((session) => {
          const row = document.createElement("div");
          row.className = `session-row ${session.id === selectedId ? "active" : ""}`;
          row.innerHTML = `
            <div class="title">${truncateText(session.title, 170)}</div>
            <div class="meta">
              <span class="status ${statusClass(session.status.kind)}">${session.status.kind}</span>
              <span>${session.tokens.total_tokens.toLocaleString()} tokens</span>
              <span>${relativeTime(session.updated_at)}</span>
              <span><code>${session.cwd}</code></span>
            </div>
          `;
          row.onclick = () => {
            selectedId = session.id;
            renderSessions(sessions);
            loadDetail(session.id);
          };
          root.appendChild(row);
        });

        if (!selectedId && sessions[0]) {
          selectedId = sessions[0].id;
          renderSessions(sessions);
          loadDetail(selectedId);
        }
      }

      function renderDetail(detail) {
        document.getElementById("detail-title").textContent = detail.summary.title;
        const status = document.getElementById("detail-status");
        status.className = `status ${statusClass(detail.summary.status.kind)}`;
        status.textContent = detail.summary.status.kind;

        const supportedNavigation = detail.summary.navigation.filter((target) =>
          target.kind === "working_directory"
        );

        const actions = supportedNavigation.map((target) => `
          <button class="action-button" type="button" onclick="openNavigation('${detail.summary.id}', '${target.kind}', this)">
            ${target.label}
          </button>
        `).join("");

        const appLabel = providerAppLabel(detail.summary);
        const appAction = appLabel ? `
          <button class="action-button" type="button" onclick="openApp('${detail.summary.id}', this)">
            Open in ${appLabel}
          </button>
        ` : "";

        const events = detail.recent_events.slice().reverse().map((event) => `
          <li><strong>${new Date(event.timestamp).toLocaleTimeString()}</strong> ${event.summary}</li>
        `).join("");

        const tools = detail.tool_stats.slice(0, 8).map((tool) => `
          <li><strong>${tool.name}</strong> · ${tool.count} calls</li>
        `).join("");

        document.getElementById("detail-body").innerHTML = `
          <div class="detail-grid">
            <div><strong>Provider</strong><br>${detail.summary.provider}</div>
            <div><strong>Status</strong><br>${detail.summary.status.kind} (${detail.summary.status.confidence})</div>
            <div><strong>Tokens</strong><br>${detail.summary.tokens.total_tokens.toLocaleString()}</div>
            <div><strong>Created</strong><br>${new Date(detail.summary.created_at).toLocaleString()}</div>
            <div><strong>Updated</strong><br>${relativeTime(detail.summary.updated_at)}</div>
            <div><strong>Model</strong><br>${detail.summary.model || "n/a"}</div>
            <div><strong>Active Turns</strong><br>${detail.active_turns}</div>
            <div><strong>Pending Tools</strong><br>${detail.pending_tool_calls}</div>
            <div><strong>CWD</strong><br><code>${detail.summary.cwd}</code></div>
          </div>

          <div class="section">
            <h2>Status Reason</h2>
            <div>${detail.summary.status.reason}</div>
          </div>

          ${(appAction || actions) ? `
            <div class="section">
              <h2>Open</h2>
              <div class="actions">${appAction}${actions}</div>
              <div id="open-notice" class="notice"></div>
            </div>
          ` : ""}

          ${detail.last_assistant_message ? `
            <div class="section">
              <h2>Last Assistant Message</h2>
              <div>${detail.last_assistant_message}</div>
            </div>
          ` : ""}

          ${detail.last_user_message ? `
            <div class="section">
              <h2>Last User Message</h2>
              <div>${detail.last_user_message}</div>
            </div>
          ` : ""}

          <div class="section">
            <h2>Recent Activity</h2>
            <ul class="events">${events || "<li>No recent activity captured.</li>"}</ul>
          </div>

          <div class="section">
            <h2>Top Tools</h2>
            <ul class="tools">${tools || "<li>No tool calls captured.</li>"}</ul>
          </div>
        `;
      }

      async function loadSessions() {
        try {
          const response = await fetch("/api/sessions?limit=60");
          if (!response.ok) {
            throw new Error(`session list failed (${response.status})`);
          }
          const data = await response.json();
          renderSessions(data.sessions);
        } catch (error) {
          document.getElementById("session-count").textContent = "failed";
          document.getElementById("session-list").innerHTML = `<div class="session-row"><div class="title error">${error.message}</div></div>`;
        }
      }

      async function loadDetail(id) {
        try {
          const response = await fetch(`/api/sessions/${id}`);
          if (!response.ok) {
            throw new Error(`session detail failed (${response.status})`);
          }
          const data = await response.json();
          renderDetail(data);
        } catch (error) {
          document.getElementById("detail-body").innerHTML = `<p class="meta error">${error.message}</p>`;
        }
      }

      function providerAppLabel(summary) {
        if (summary.provider === "codex") return "Codex";
        if (summary.provider === "claude") return "Claude";
        return null;
      }

      async function openNavigation(sessionId, kind, button) {
        const notice = document.getElementById("open-notice");
        if (notice) {
          notice.textContent = "";
          notice.className = "notice";
        }

        button.disabled = true;
        try {
          const response = await fetch(`/api/sessions/${sessionId}/open?kind=${encodeURIComponent(kind)}`, {
            method: "POST"
          });

          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.error || `open failed (${response.status})`);
          }

          if (notice) {
            notice.textContent = `Opened ${data.label}.`;
          }
        } catch (error) {
          if (notice) {
            notice.textContent = error.message;
            notice.className = "notice error";
          }
        } finally {
          button.disabled = false;
        }
      }

      async function openApp(sessionId, button) {
        const notice = document.getElementById("open-notice");
        if (notice) {
          notice.textContent = "";
          notice.className = "notice";
        }

        button.disabled = true;
        try {
          const response = await fetch(`/api/sessions/${sessionId}/open-app`, {
            method: "POST"
          });

          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.error || `open app failed (${response.status})`);
          }

          if (notice) {
            notice.textContent = `Opened ${data.label}.`;
          }
        } catch (error) {
          if (notice) {
            notice.textContent = error.message;
            notice.className = "notice error";
          }
        } finally {
          button.disabled = false;
        }
      }

      loadSessions();
      setInterval(loadSessions, 5000);
    </script>
  </body>
</html>"#;
