mod api;
mod client;
mod open;
mod tui;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use agent_cow_claude::ClaudeSource;
use agent_cow_codex::CodexSource;
use agent_cow_core::{CombinedSource, MonitorService, SessionDetail, SessionQuery, SessionSummary};
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use client::{LocalMonitorClient, MonitorClient, MultiMonitorClient, RemoteMonitorClient};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(name = "agent-cow")]
#[command(about = "Local-first observability for coding-agent sessions")]
struct Cli {
    #[arg(long, env = "AGENT_COW_CODEX_HOME")]
    codex_home: Option<PathBuf>,

    #[arg(long, env = "AGENT_COW_CLAUDE_HOME")]
    claude_home: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Sessions {
        #[command(flatten)]
        client: ClientOptions,
        #[command(subcommand)]
        command: SessionsCommand,
    },
    Tui {
        #[command(flatten)]
        client: ClientOptions,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 5)]
        refresh_secs: u64,
    },
    Agent {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
    },
}

#[derive(Args, Clone, Debug, Default)]
struct ClientOptions {
    #[arg(long = "machine", alias = "agent", value_name = "URL")]
    machines: Vec<String>,
    #[arg(long)]
    no_local: bool,
}

#[derive(Subcommand, Debug)]
enum SessionsCommand {
    List {
        #[arg(long, default_value_t = 30)]
        limit: usize,
        #[arg(long)]
        include_archived: bool,
        #[arg(long)]
        json: bool,
    },
    Latest {
        #[arg(long)]
        json: bool,
    },
    Show {
        session_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let codex_home = cli
        .codex_home
        .or_else(|| std::env::var_os("CODEX_HOME").map(PathBuf::from));
    let claude_home = cli
        .claude_home
        .or_else(|| std::env::var_os("CLAUDE_HOME").map(PathBuf::from));

    let local_service = build_local_service(codex_home, claude_home)?;

    match cli.command {
        Command::Sessions { client, command } => {
            let client = build_monitor_client(local_service.clone(), &client)?;
            match command {
                SessionsCommand::List {
                    limit,
                    include_archived,
                    json,
                } => {
                    let sessions = client
                        .list_sessions(SessionQuery {
                            include_archived,
                            limit: Some(limit),
                        })
                        .await?;

                    if json {
                        println!("{}", serde_json::to_string_pretty(&sessions)?);
                    } else {
                        print_sessions(&sessions.sessions);
                    }
                }
                SessionsCommand::Latest { json } => {
                    let session = client.latest_session().await?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&session)?);
                    } else {
                        print_session_detail(&session);
                    }
                }
                SessionsCommand::Show { session_id, json } => {
                    let session = client.get_session(&session_id).await?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&session)?);
                    } else {
                        print_session_detail(&session);
                    }
                }
            }
        }
        Command::Tui {
            client,
            limit,
            refresh_secs,
        } => {
            let client = build_monitor_client(local_service, &client)?;
            tui::run(client, limit, refresh_secs).await?;
        }
        Command::Agent { bind } => {
            let service = local_service.ok_or_else(|| {
                anyhow::anyhow!("no provider homes were found; looked for ~/.codex and ~/.claude")
            })?;
            api::run(service, bind).await?;
        }
    }

    Ok(())
}

fn build_local_service(
    codex_home: Option<PathBuf>,
    claude_home: Option<PathBuf>,
) -> Result<Option<MonitorService>> {
    let mut source = CombinedSource::new();
    if let Some(path) = codex_home {
        source.push_source("codex", Arc::new(CodexSource::new(path)));
    } else if let Some(path) = default_provider_home(".codex").filter(|path| path.exists()) {
        source.push_source("codex", Arc::new(CodexSource::new(path)));
    }

    if let Some(path) = claude_home {
        source.push_source("claude", Arc::new(ClaudeSource::new(path)));
    } else if let Some(path) = default_provider_home(".claude").filter(|path| path.exists()) {
        source.push_source("claude", Arc::new(ClaudeSource::new(path)));
    }

    if source.is_empty() {
        return Ok(None);
    }

    Ok(Some(MonitorService::new(Arc::new(source))))
}

fn build_monitor_client(
    local_service: Option<MonitorService>,
    options: &ClientOptions,
) -> Result<Arc<dyn MonitorClient>> {
    let mut client = MultiMonitorClient::new();

    if !options.no_local
        && let Some(local_service) = local_service
    {
        client.push_client(
            "local",
            None,
            Arc::new(LocalMonitorClient::new(local_service)),
        );
    }

    for (index, machine) in options.machines.iter().enumerate() {
        let remote = RemoteMonitorClient::new(machine)?;
        let fallback_machine_label = Some(remote.fallback_machine_label());
        client.push_client(
            format!("remote{}", index + 1),
            fallback_machine_label,
            Arc::new(remote),
        );
    }

    if client.is_empty() {
        return Err(anyhow::anyhow!(
            "no monitor sources are available; add --machine or ensure ~/.codex or ~/.claude exist"
        ));
    }

    Ok(Arc::new(client))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("agent_cow=info,agent_cow_codex=info,agent_cow_claude=info")
    });

    fmt().with_env_filter(filter).without_time().init();
}

fn default_provider_home(dir_name: &str) -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(dir_name))
}

fn print_sessions(sessions: &[SessionSummary]) {
    println!(
        "{:<8} {:<14} {:>8} {:>8} {:>8} {:>7} {:>10} {:<8} {:<18} title",
        "provider", "status", "cost", "$/1h", "$/1d", "ctx", "tokens", "archived", "updated"
    );
    println!("{}", "-".repeat(143));

    for session in sessions {
        println!(
            "{:<8} {:<14} {:>8} {:>8} {:>8} {:>7} {:>10} {:<8} {:<18} {}",
            session.provider,
            session.status.kind,
            session
                .cost
                .as_ref()
                .map(|cost| format_usd_short(cost.total_usd))
                .unwrap_or_else(|| "--".to_string()),
            session
                .cost
                .as_ref()
                .filter(|cost| cost.hour_usd > 0.0)
                .map(|cost| format_usd_short(cost.hour_usd))
                .unwrap_or_else(|| "--".to_string()),
            session
                .cost
                .as_ref()
                .filter(|cost| cost.day_usd > 0.0)
                .map(|cost| format_usd_short(cost.day_usd))
                .unwrap_or_else(|| "--".to_string()),
            session
                .context_window
                .as_ref()
                .map(|context| format!("{}%", context.used_percent))
                .unwrap_or_else(|| "--".to_string()),
            session.tokens.total_tokens,
            if session.archived { "yes" } else { "no" },
            relative_time(session.updated_at),
            truncate(&session.title, 80),
        );
    }
}

fn print_session_detail(detail: &SessionDetail) {
    let summary = &detail.summary;
    println!("ID: {}", summary.id);
    println!("Title: {}", summary.title);
    println!("Provider: {}", summary.provider);
    println!("Machine: {}", summary.machine_label);
    println!(
        "Status: {} ({}) - {}",
        summary.status.kind, summary.status.confidence, summary.status.reason
    );
    println!("Tokens: {}", summary.tokens.total_tokens);
    if let Some(cost) = &summary.cost {
        let mut parts = vec![
            format!("$/1h ${:.4}", cost.hour_usd),
            format!("$/1d ${:.4}", cost.day_usd),
            format!("input ${:.4}", cost.input_usd),
        ];
        if cost.cache_creation_input_usd > 0.0 {
            parts.push(format!("cache+ ${:.4}", cost.cache_creation_input_usd));
        }
        parts.push(format!("cached ${:.4}", cost.cached_input_usd));
        parts.push(format!("output ${:.4}", cost.output_usd));
        parts.push(cost.pricing_source.to_string());
        println!("Cost: ${:.4} ({})", cost.total_usd, parts.join(", "));
    }
    if let Some(context) = &summary.context_window {
        println!(
            "Context: {} / {} used ({} remaining, {}%)",
            context.used_tokens,
            context.limit_tokens,
            context.remaining_tokens,
            context.used_percent
        );
    }
    println!("Created: {}", summary.created_at);
    println!("Updated: {}", summary.updated_at);
    println!("CWD: {}", summary.cwd);
    if let Some(model) = &summary.model {
        println!("Model: {model}");
    }
    if let Some(role) = &summary.agent_role {
        println!("Agent Role: {role}");
    }
    if let Some(branch) = &summary.git_branch {
        println!("Git Branch: {branch}");
    }
    println!("Active Turns: {}", detail.active_turns);
    println!("Pending Tool Calls: {}", detail.pending_tool_calls);

    if let Some(message) = &detail.last_user_message {
        println!("\nLast User Message:\n{message}");
    }

    if let Some(message) = &detail.last_assistant_message {
        println!("\nLast Assistant Message:\n{message}");
    }

    if !detail.tool_stats.is_empty() {
        println!("\nTop Tool Calls:");
        for tool in detail.tool_stats.iter().take(8) {
            println!(
                "  - {:<20} {:>4} last seen {}",
                tool.name,
                tool.count,
                tool.last_seen
                    .map(relative_time)
                    .unwrap_or_else(|| "n/a".to_string())
            );
        }
    }

    if !detail.recent_events.is_empty() {
        println!("\nRecent Activity:");
        for event in detail.recent_events.iter().rev().take(16) {
            println!(
                "  - {} {:<11} {}",
                event.timestamp.format("%Y-%m-%d %H:%M:%S"),
                format!("{:?}", event.kind).to_lowercase(),
                event.summary
            );
        }
    }
}

fn relative_time(timestamp: DateTime<Utc>) -> String {
    let delta = Utc::now() - timestamp;

    if delta.num_seconds() < 60 {
        format!("{}s ago", delta.num_seconds())
    } else if delta.num_minutes() < 60 {
        format!("{}m ago", delta.num_minutes())
    } else if delta.num_hours() < 24 {
        format!("{}h ago", delta.num_hours())
    } else {
        format!("{}d ago", delta.num_days())
    }
}

fn truncate(input: &str, max_len: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_len {
        return input.to_string();
    }

    if max_len <= 3 {
        return "...".chars().take(max_len).collect();
    }

    let truncated: String = input.chars().take(max_len - 3).collect();
    format!("{truncated}...")
}

fn format_usd_short(value: f64) -> String {
    if value >= 100.0 {
        format!("${value:.0}")
    } else if value >= 10.0 {
        format!("${value:.1}")
    } else if value >= 1.0 {
        format!("${value:.2}")
    } else if value >= 0.01 {
        format!("${value:.3}")
    } else {
        format!("${value:.4}")
    }
}
