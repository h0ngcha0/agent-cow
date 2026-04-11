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
use cow_watch_core::{MonitorService, NavigationKind, SessionQuery};
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
    <title>cow-watch</title>
    <style>
      :root {
        color-scheme: dark;
        --bg: #081117;
        --panel: #0d181e;
        --panel-2: #11252e;
        --border: rgba(128, 167, 173, 0.18);
        --text: #e5f0f2;
        --muted: #8ba7ad;
        --accent: #49d3b6;
        --accent-2: #69dcff;
        --ok: #7be089;
        --warn: #f5d45e;
        --danger: #ff7f8a;
        --shadow: rgba(0, 0, 0, 0.34);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top left, rgba(73, 211, 182, 0.09), transparent 28%),
          radial-gradient(circle at top right, rgba(105, 220, 255, 0.06), transparent 24%),
          linear-gradient(180deg, #091117 0%, #060f14 100%);
        color: var(--text);
        font: 14px/1.45 "Berkeley Mono", "SF Mono", "JetBrains Mono", monospace;
      }

      button,
      input {
        font: inherit;
      }

      .shell {
        display: grid;
        grid-template-columns: minmax(760px, 1.25fr) minmax(420px, 0.75fr);
        gap: 16px;
        padding: 16px;
        min-height: 100vh;
      }

      .panel {
        border: 1px solid var(--border);
        border-radius: 16px;
        background: linear-gradient(180deg, rgba(13, 24, 30, 0.97), rgba(9, 18, 23, 0.97));
        box-shadow: 0 18px 48px var(--shadow);
        overflow: hidden;
      }

      .sessions,
      .detail {
        display: flex;
        flex-direction: column;
        min-height: calc(100vh - 32px);
      }

      .hero,
      .toolbar,
      .detail > header {
        padding: 14px 18px;
        border-bottom: 1px solid rgba(128, 167, 173, 0.12);
      }

      .hero {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }

      .hero h1 {
        margin: 0;
        font-size: 18px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .hero-meta {
        display: flex;
        gap: 14px;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .pulse-strip {
        display: grid;
        grid-template-columns: repeat(9, minmax(0, 1fr));
        gap: 10px;
        padding: 12px 18px;
        border-bottom: 1px solid rgba(128, 167, 173, 0.12);
        background: rgba(17, 37, 46, 0.42);
      }

      .pulse-chip {
        border: 1px solid rgba(128, 167, 173, 0.12);
        border-radius: 10px;
        padding: 10px 12px;
        background: rgba(8, 17, 23, 0.46);
      }

      .pulse-chip span {
        display: block;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .pulse-chip strong {
        display: block;
        margin-top: 4px;
        font-size: 16px;
      }

      .toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
      }

      .toolbar strong,
      .detail > header strong {
        font-size: 13px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .toolbar-right {
        display: flex;
        align-items: center;
        gap: 12px;
        color: var(--muted);
      }

      .filter-input {
        width: min(320px, 42vw);
        border: 1px solid var(--border);
        background: rgba(8, 17, 23, 0.82);
        color: var(--text);
        border-radius: 10px;
        padding: 9px 11px;
        outline: none;
      }

      .filter-input:focus {
        border-color: rgba(73, 211, 182, 0.55);
        box-shadow: 0 0 0 3px rgba(73, 211, 182, 0.12);
      }

      .table-wrap {
        flex: 1;
        overflow: auto;
      }

      .session-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
      }

      .session-table thead th {
        position: sticky;
        top: 0;
        z-index: 1;
        padding: 10px 12px;
        background: rgba(9, 17, 23, 0.97);
        color: var(--muted);
        border-bottom: 1px solid rgba(128, 167, 173, 0.14);
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        text-align: left;
      }

      .session-table tbody tr {
        cursor: pointer;
        transition: background 120ms ease;
      }

      .session-table tbody tr:hover,
      .session-table tbody tr.active {
        background: rgba(17, 37, 46, 0.68);
      }

      .session-table tbody tr.active {
        box-shadow: inset 3px 0 0 var(--accent);
      }

      .session-table td {
        padding: 11px 12px;
        border-bottom: 1px solid rgba(128, 167, 173, 0.08);
        vertical-align: top;
        color: var(--text);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      .session-table td.title-cell {
        white-space: normal;
      }

      .session-table td.title-cell strong {
        display: -webkit-box;
        -webkit-box-orient: vertical;
        -webkit-line-clamp: 2;
        overflow: hidden;
        line-height: 1.35;
      }

      .session-table td.title-cell .subline {
        margin-top: 4px;
        color: var(--muted);
        font-size: 12px;
      }

      .mono {
        font-family: "Berkeley Mono", "SF Mono", "JetBrains Mono", monospace;
      }

      .status {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 66px;
        padding: 3px 8px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .status.running { background: rgba(73, 211, 182, 0.14); color: var(--accent); }
      .status.tool_busy { background: rgba(245, 212, 94, 0.14); color: var(--warn); }
      .status.waiting_input { background: rgba(105, 220, 255, 0.14); color: var(--accent-2); }
      .status.idle,
      .status.stale { background: rgba(128, 167, 173, 0.12); color: var(--muted); }
      .status.completed { background: rgba(123, 224, 137, 0.14); color: var(--ok); }
      .status.failed { background: rgba(255, 127, 138, 0.14); color: var(--danger); }

      .detail > header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }

      .detail-body {
        flex: 1;
        overflow: auto;
        padding: 16px;
        display: grid;
        gap: 14px;
        align-content: start;
      }

      .detail-block {
        border: 1px solid rgba(128, 167, 173, 0.14);
        border-radius: 12px;
        background: rgba(17, 37, 46, 0.3);
        padding: 14px;
      }

      .detail-block h2 {
        margin: 0 0 10px;
        font-size: 12px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .inspector-title {
        font-size: 16px;
        font-weight: 700;
        line-height: 1.35;
        margin-bottom: 10px;
      }

      .detail-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px 16px;
      }

      .detail-grid strong {
        display: block;
        margin-bottom: 4px;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .summary-line {
        color: var(--muted);
        margin-top: 10px;
      }

      .actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }

      .action-button {
        appearance: none;
        border: 1px solid var(--border);
        background: rgba(73, 211, 182, 0.08);
        color: var(--text);
        border-radius: 999px;
        padding: 8px 13px;
        cursor: pointer;
        transition: background 120ms ease, transform 120ms ease;
      }

      .action-button:hover {
        background: rgba(73, 211, 182, 0.18);
        transform: translateY(-1px);
      }

      .action-button:disabled {
        opacity: 0.55;
        cursor: default;
        transform: none;
      }

      .notice {
        margin-top: 10px;
        color: var(--muted);
        font-size: 12px;
      }

      .error {
        color: var(--danger);
      }

      .timeline,
      .tool-list {
        list-style: none;
        padding: 0;
        margin: 0;
        display: grid;
        gap: 8px;
      }

      .timeline li,
      .tool-list li {
        padding: 8px 10px;
        border-radius: 10px;
        background: rgba(8, 17, 23, 0.44);
      }

      .timeline time {
        color: var(--muted);
        margin-right: 8px;
      }

      .empty-state {
        padding: 18px;
        color: var(--muted);
      }

      @media (max-width: 1200px) {
        .shell {
          grid-template-columns: 1fr;
        }

        .sessions,
        .detail {
          min-height: unset;
        }
      }

      @media (max-width: 880px) {
        .pulse-strip {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }

        .toolbar {
          flex-direction: column;
          align-items: stretch;
        }

        .toolbar-right {
          justify-content: space-between;
        }

        .filter-input {
          width: 100%;
        }

        .detail-grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="panel sessions">
        <div class="hero">
          <h1>cow-watch</h1>
          <div class="hero-meta">
            <span id="hero-spend">spend --</span>
            <span id="hero-tokens">tokens --</span>
            <span id="hero-quota">quota --</span>
            <span id="last-sync">syncing…</span>
          </div>
        </div>
        <div id="pulse-strip" class="pulse-strip"></div>
        <div class="toolbar">
          <strong>Sessions</strong>
          <div class="toolbar-right">
            <input id="filter-input" class="filter-input" type="text" placeholder="/ filter sessions by title, cwd, provider, model" />
            <span id="session-count">loading…</span>
          </div>
        </div>
        <div class="table-wrap">
          <table class="session-table">
            <thead>
              <tr>
                <th style="width:78px;">St</th>
                <th style="width:140px;">Project</th>
                <th style="width:150px;">Model</th>
                <th style="width:82px;">Cost</th>
                <th style="width:82px;">$/1H</th>
                <th style="width:82px;">$/1D</th>
                <th style="width:72px;">Ctx</th>
                <th style="width:96px;">Tokens</th>
                <th style="width:74px;">Age</th>
                <th>Session</th>
              </tr>
            </thead>
            <tbody id="session-table-body"></tbody>
          </table>
        </div>
      </section>

      <section class="panel detail">
        <header>
          <strong id="detail-title">Select a session</strong>
          <span id="detail-status" class="status idle">idle</span>
        </header>
        <div id="detail-body" class="detail-body">
          <div class="detail-block">
            <div class="summary-line">Choose a session to inspect tokens, activity, and open targets.</div>
          </div>
        </div>
      </section>
    </div>

    <script>
      let selectedId = null;
      let sessionsCache = [];
      let overviewCache = null;
      let filterQuery = "";

      function escapeHtml(value) {
        return String(value ?? "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      }

      function relativeTime(dateString) {
        const deltaSeconds = Math.floor((Date.now() - new Date(dateString).getTime()) / 1000);
        if (deltaSeconds < 60) return `${deltaSeconds}s`;
        if (deltaSeconds < 3600) return `${Math.floor(deltaSeconds / 60)}m`;
        if (deltaSeconds < 86400) return `${Math.floor(deltaSeconds / 3600)}h`;
        return `${Math.floor(deltaSeconds / 86400)}d`;
      }

      function statusClass(kind) {
        return (kind || "idle").replace(/[^a-z_]/g, "");
      }

      function truncateText(text, maxChars) {
        const chars = Array.from(text || "");
        if (chars.length <= maxChars) return text || "";
        if (maxChars <= 3) return ".".repeat(maxChars);
        return `${chars.slice(0, maxChars - 3).join("")}...`;
      }

      function pathTail(path) {
        if (!path) return "n/a";
        const parts = path.split("/").filter(Boolean);
        return parts[parts.length - 1] || path;
      }

      function titleCase(value) {
        return String(value || "")
          .split(/[_\s-]+/)
          .filter(Boolean)
          .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
          .join(" ");
      }

      function formatTokens(tokens) {
        if (tokens >= 1_000_000_000) return `${(tokens / 1_000_000_000).toFixed(1)}B`;
        if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M`;
        if (tokens >= 1_000) return `${Math.round(tokens / 1_000)}K`;
        return `${tokens}`;
      }

      function formatUsd(value) {
        if (!value || value <= 0) return "$0";
        if (value >= 100) return `$${value.toFixed(0)}`;
        if (value >= 10) return `$${value.toFixed(1)}`;
        if (value >= 1) return `$${value.toFixed(2)}`;
        if (value >= 0.01) return `$${value.toFixed(3)}`;
        return `$${value.toFixed(4)}`;
      }

      function formatUsdWindow(value) {
        if (!value || value <= 0) return "--";
        return formatUsd(value);
      }

      function formatContext(context) {
        if (!context) return "--";
        return `${context.used_percent}%`;
      }

      function quotaSummary(quotas) {
        const items = (quotas || []).map((quota) => {
          const provider = titleCase(quota.provider || "provider");
          const parts = [];
          if (quota.plan) parts.push(quota.plan);
          if ((quota.windows || []).length) {
            for (const window of quota.windows) {
              parts.push(`${window.label} ${window.used_percent}%`);
            }
          } else if (quota.plan) {
            parts.push("active");
          }
          if (quota.limit_reached) parts.push("limit");
          return `${provider} ${parts.join(" ")}`.trim();
        });
        return items.length ? items.join("  ·  ") : "quota n/a";
      }

      function pulseCounts(sessions) {
        return sessions.reduce((acc, session) => {
          const key = session.status.kind || "unknown";
          acc[key] = (acc[key] || 0) + 1;
          return acc;
        }, {
          running: 0,
          tool_busy: 0,
          waiting_input: 0,
          idle: 0,
          stale: 0,
          completed: 0,
          failed: 0,
          unknown: 0
        });
      }

      function filterSessions(sessions) {
        const query = filterQuery.trim().toLowerCase();
        if (!query) return sessions;

        return sessions.filter((session) => {
          const haystack = [
            session.title,
            session.cwd,
            session.provider,
            session.model || "",
            session.status.kind
          ].join(" ").toLowerCase();
          return haystack.includes(query);
        });
      }

      function renderPulseStrip(sessions, overview) {
        const counts = pulseCounts(sessions);
        const root = document.getElementById("pulse-strip");
        const metrics = [
          ["SPEND", formatUsd(overview?.total_cost_usd || 0)],
          ["TOKENS", formatTokens(overview?.total_tokens || 0)],
          ["RUN", counts.running],
          ["BUSY", counts.tool_busy],
          ["WAIT", counts.waiting_input],
          ["IDLE", counts.idle],
          ["STALE", counts.stale],
          ["DONE", counts.completed],
          ["FAIL", counts.failed]
        ];

        root.innerHTML = metrics.map(([label, value]) => `
          <div class="pulse-chip">
            <span>${label}</span>
            <strong>${value}</strong>
          </div>
        `).join("");
      }

      function renderSessions(sessions, overview) {
        sessionsCache = sessions;
        overviewCache = overview || null;
        const filtered = filterSessions(sessions);
        const tbody = document.getElementById("session-table-body");

        renderPulseStrip(sessions, overview);
        document.getElementById("session-count").textContent = `${filtered.length}/${sessions.length} visible`;
        document.getElementById("last-sync").textContent = `sync ${new Date().toLocaleTimeString()}`;
        document.getElementById("hero-spend").textContent = `spend ${formatUsd(overview?.total_cost_usd || 0)}`;
        document.getElementById("hero-tokens").textContent = `tokens ${formatTokens(overview?.total_tokens || 0)}`;
        document.getElementById("hero-quota").textContent = quotaSummary(overview?.quotas);

        if (!filtered.some((session) => session.id === selectedId)) {
          selectedId = filtered[0]?.id || null;
        }

        if (!filtered.length) {
          tbody.innerHTML = `
            <tr>
              <td class="empty-state" colspan="10">No sessions match the current filter.</td>
            </tr>
          `;
          document.getElementById("detail-title").textContent = "No matching session";
          document.getElementById("detail-status").className = "status idle";
          document.getElementById("detail-status").textContent = "idle";
          document.getElementById("detail-body").innerHTML = `
            <div class="detail-block">
              <div class="summary-line">Adjust the filter or wait for fresh session data.</div>
            </div>
          `;
          return;
        }

          tbody.innerHTML = filtered.map((session) => `
          <tr class="${session.id === selectedId ? "active" : ""}" data-session-id="${escapeHtml(session.id)}">
            <td><span class="status ${statusClass(session.status.kind)}">${escapeHtml(session.status.kind)}</span></td>
            <td class="mono">${escapeHtml(pathTail(session.cwd))}</td>
            <td>${escapeHtml(session.model || "n/a")}</td>
            <td>${escapeHtml(formatUsd(session.cost?.total_usd || 0))}</td>
            <td>${escapeHtml(formatUsdWindow(session.cost?.hour_usd || 0))}</td>
            <td>${escapeHtml(formatUsdWindow(session.cost?.day_usd || 0))}</td>
            <td>${escapeHtml(formatContext(session.context_window))}</td>
            <td>${escapeHtml(formatTokens(session.tokens.total_tokens))}</td>
            <td>${escapeHtml(relativeTime(session.updated_at))}</td>
            <td class="title-cell">
              <strong>${escapeHtml(truncateText(session.title, 150))}</strong>
              <div class="subline">${escapeHtml(truncateText(`${session.cwd}${session.context_window ? ` · ${session.context_window.used_tokens.toLocaleString()}/${session.context_window.limit_tokens.toLocaleString()}` : ""}`, 120))}</div>
            </td>
          </tr>
        `).join("");

        tbody.querySelectorAll("tr[data-session-id]").forEach((row) => {
          row.addEventListener("click", () => {
            selectedId = row.dataset.sessionId;
            renderSessions(sessionsCache, overview);
            loadDetail(selectedId);
          });
        });

        if (selectedId) {
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
            ${escapeHtml(target.label)}
          </button>
        `).join("");

        const appLabel = providerAppLabel(detail.summary);
        const appAction = appLabel ? `
          <button class="action-button" type="button" onclick="openApp('${detail.summary.id}', this)">
            Open in ${escapeHtml(appLabel)}
          </button>
        ` : "";

        const events = detail.recent_events.slice().reverse().map((event) => `
          <li><time>${new Date(event.timestamp).toLocaleTimeString()}</time>${escapeHtml(event.summary)}</li>
        `).join("");

        const tools = detail.tool_stats.slice(0, 8).map((tool) => `
          <li><strong>${escapeHtml(tool.name)}</strong> · ${escapeHtml(tool.count)} calls</li>
        `).join("");

        document.getElementById("detail-body").innerHTML = `
          <div class="detail-block">
            <div class="inspector-title">${escapeHtml(detail.summary.title)}</div>
            <div class="detail-grid">
              <div><strong>Provider</strong>${escapeHtml(detail.summary.provider)}</div>
              <div><strong>Status</strong>${escapeHtml(detail.summary.status.kind)} (${escapeHtml(detail.summary.status.confidence)})</div>
              <div><strong>Tokens</strong>${detail.summary.tokens.total_tokens.toLocaleString()}</div>
              <div><strong>Cost</strong>${escapeHtml(formatUsd(detail.summary.cost?.total_usd || 0))}</div>
              <div><strong>$/1H</strong>${escapeHtml(formatUsdWindow(detail.summary.cost?.hour_usd || 0))}</div>
              <div><strong>$/1D</strong>${escapeHtml(formatUsdWindow(detail.summary.cost?.day_usd || 0))}</div>
              <div><strong>Updated</strong>${escapeHtml(relativeTime(detail.summary.updated_at))}</div>
              <div><strong>Model</strong>${escapeHtml(detail.summary.model || "n/a")}</div>
              <div><strong>Machine</strong>${escapeHtml(detail.summary.machine_label)}</div>
              <div><strong>Context</strong>${detail.summary.context_window ? `${detail.summary.context_window.used_tokens.toLocaleString()} / ${detail.summary.context_window.limit_tokens.toLocaleString()} (${detail.summary.context_window.used_percent}%)` : "n/a"}</div>
              <div><strong>Remaining</strong>${detail.summary.context_window ? detail.summary.context_window.remaining_tokens.toLocaleString() : "n/a"}</div>
              <div><strong>Active Turns</strong>${escapeHtml(detail.active_turns)}</div>
              <div><strong>Pending Tools</strong>${escapeHtml(detail.pending_tool_calls)}</div>
              <div><strong>Workdir</strong><span class="mono">${escapeHtml(detail.summary.cwd)}</span></div>
              <div><strong>Branch</strong>${escapeHtml(detail.summary.git_branch || "n/a")}</div>
            </div>
            <div class="summary-line">${escapeHtml(detail.summary.status.reason)}</div>
            ${detail.summary.cost ? `<div class="summary-line">1h ${escapeHtml(formatUsdWindow(detail.summary.cost.hour_usd))} · 1d ${escapeHtml(formatUsdWindow(detail.summary.cost.day_usd))} · input ${escapeHtml(formatUsd(detail.summary.cost.input_usd))} · cached ${escapeHtml(formatUsd(detail.summary.cost.cached_input_usd))} · output ${escapeHtml(formatUsd(detail.summary.cost.output_usd))} · ${escapeHtml(detail.summary.cost.pricing_source)}</div>` : ""}
          </div>

          ${(appAction || actions) ? `
            <div class="detail-block">
              <h2>Ops</h2>
              <div class="actions">${appAction}${actions}</div>
              <div id="open-notice" class="notice"></div>
            </div>
          ` : ""}

          ${detail.last_assistant_message ? `
            <div class="detail-block">
              <h2>Assistant</h2>
              <div>${escapeHtml(detail.last_assistant_message)}</div>
            </div>
          ` : ""}

          <div class="detail-block">
            <h2>Recent Activity</h2>
            <ul class="timeline">${events || "<li>No recent activity captured.</li>"}</ul>
          </div>

          <div class="detail-block">
            <h2>Top Tools</h2>
            <ul class="tool-list">${tools || "<li>No tool calls captured.</li>"}</ul>
          </div>
        `;
      }

      async function loadSessions() {
        try {
          const response = await fetch("/api/sessions");
          if (!response.ok) {
            throw new Error(`session list failed (${response.status})`);
          }
          const data = await response.json();
          renderSessions(data.sessions, data.overview);
        } catch (error) {
          document.getElementById("session-count").textContent = "failed";
          document.getElementById("session-table-body").innerHTML = `
            <tr>
              <td class="empty-state error" colspan="10">${escapeHtml(error.message)}</td>
            </tr>
          `;
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
          document.getElementById("detail-body").innerHTML = `
            <div class="detail-block error">${escapeHtml(error.message)}</div>
          `;
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

      document.getElementById("filter-input").addEventListener("input", (event) => {
        filterQuery = event.target.value || "";
        renderSessions(sessionsCache, overviewCache);
      });

      loadSessions();
      setInterval(loadSessions, 5000);
    </script>
  </body>
</html>"#;
