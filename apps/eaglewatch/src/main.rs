mod api;
mod tui;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use eaglewatch_codex::CodexSource;
use eaglewatch_core::{MonitorService, SessionDetail, SessionQuery, SessionSummary};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(name = "eaglewatch")]
#[command(about = "Local-first observability for coding-agent sessions")]
struct Cli {
    #[arg(long, env = "EAGLEWATCH_CODEX_HOME")]
    codex_home: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    Tui {
        #[arg(long, default_value_t = 40)]
        limit: usize,
        #[arg(long, default_value_t = 5)]
        refresh_secs: u64,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1:7878")]
        bind: SocketAddr,
    },
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
    let source = match cli.codex_home {
        Some(path) => CodexSource::new(path),
        None => CodexSource::from_default_home()?,
    };
    let service = MonitorService::new(Arc::new(source));

    match cli.command {
        Command::Sessions { command } => match command {
            SessionsCommand::List {
                limit,
                include_archived,
                json,
            } => {
                let sessions = service
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
                let session = service.latest_session().await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&session)?);
                } else {
                    print_session_detail(&session);
                }
            }
            SessionsCommand::Show { session_id, json } => {
                let session = service.get_session(&session_id).await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&session)?);
                } else {
                    print_session_detail(&session);
                }
            }
        },
        Command::Tui {
            limit,
            refresh_secs,
        } => {
            tui::run(service, limit, refresh_secs).await?;
        }
        Command::Serve { bind } => {
            api::run(service, bind).await?;
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("eaglewatch=info,eaglewatch_codex=info"));

    fmt().with_env_filter(filter).without_time().init();
}

fn print_sessions(sessions: &[SessionSummary]) {
    println!(
        "{:<14} {:>10} {:<8} {:<18} {}",
        "status", "tokens", "archived", "updated", "title"
    );
    println!("{}", "-".repeat(96));

    for session in sessions {
        println!(
            "{:<14} {:>10} {:<8} {:<18} {}",
            session.status.kind,
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
