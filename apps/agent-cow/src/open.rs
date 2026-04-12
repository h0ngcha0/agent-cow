use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use agent_cow_core::{NavigationKind, ProviderKind, SessionSummary};
use anyhow::{Context, Result, anyhow};

pub struct OpenAction {
    pub label: String,
    #[allow(dead_code)]
    pub target: String,
}

pub fn local_machine_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

#[allow(dead_code)]
pub fn open_session_navigation(
    summary: &SessionSummary,
    kind: NavigationKind,
) -> Result<OpenAction> {
    let target = summary
        .navigation
        .iter()
        .find(|target| target.kind == kind)
        .ok_or_else(|| {
            anyhow!(
                "navigation target `{:?}` is not available for this session",
                kind
            )
        })?;

    match target.kind {
        NavigationKind::WorkingDirectory | NavigationKind::RolloutPath => {
            open_target(&target.target)?;
        }
        NavigationKind::ThreadId => {
            return Err(anyhow!(
                "opening a Codex thread directly is not supported yet"
            ));
        }
    }

    Ok(OpenAction {
        label: target.label.clone(),
        target: target.target.clone(),
    })
}

pub fn open_session_app(summary: &SessionSummary, local_machine_id: &str) -> Result<OpenAction> {
    if summary.machine_id != local_machine_id {
        return Err(anyhow!(
            "opening provider apps is only supported for local sessions"
        ));
    }

    let (label, target) = provider_app_target(summary)
        .ok_or_else(|| anyhow!("this provider does not expose a local app target yet"))?;

    open_uri(&target)?;
    activate_provider_app(summary.provider.clone())?;

    Ok(OpenAction { label, target })
}

fn provider_app_target(summary: &SessionSummary) -> Option<(String, String)> {
    match summary.provider {
        ProviderKind::Codex => {
            let thread_id = summary
                .navigation
                .iter()
                .find(|target| target.kind == NavigationKind::ThreadId)
                .map(|target| target.target.as_str())
                .filter(|target| !target.is_empty())
                .unwrap_or(summary.id.as_str());

            Some(("Codex".to_string(), format!("codex://threads/{thread_id}")))
        }
        ProviderKind::Claude => {
            let conversation_id = summary
                .navigation
                .iter()
                .find(|target| target.kind == NavigationKind::ThreadId)
                .map(|target| target.target.as_str())
                .filter(|target| !target.is_empty())
                .or_else(|| summary.id.strip_prefix("claude:"))
                .filter(|target| !target.is_empty());

            let target = conversation_id
                .map(claude_app_uri_for_session)
                .unwrap_or_else(|| "claude://".to_string());

            Some(("Claude".to_string(), target))
        }
    }
}

fn claude_app_uri_for_session(session_id: &str) -> String {
    if claude_imported_session_exists(session_id) {
        format!("claude://claude.ai/claude-code-desktop/local_{session_id}")
    } else {
        format!("claude://resume?session={session_id}")
    }
}

fn claude_imported_session_exists(session_id: &str) -> bool {
    let Some(home) = std::env::var_os("HOME") else {
        return false;
    };

    let root = PathBuf::from(home)
        .join("Library")
        .join("Application Support")
        .join("Claude")
        .join("claude-code-sessions");

    claude_imported_session_exists_in_root(&root, session_id)
}

fn claude_imported_session_exists_in_root(root: &Path, session_id: &str) -> bool {
    let wanted = format!("local_{session_id}.json");
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == wanted)
            {
                return true;
            }
        }
    }

    false
}

#[allow(dead_code)]
fn open_target(target: &str) -> Result<()> {
    let path = Path::new(target);
    let metadata = fs::metadata(path)
        .with_context(|| format!("target does not exist or cannot be read: `{target}`"))?;

    open_with_system(target, metadata.is_file())
}

fn open_uri(target: &str) -> Result<()> {
    open_with_system(target, false)
}

fn activate_provider_app(provider: ProviderKind) -> Result<()> {
    #[cfg(not(target_os = "macos"))]
    let _ = provider;

    #[cfg(target_os = "macos")]
    {
        let app_name = match provider {
            ProviderKind::Codex => "Codex",
            ProviderKind::Claude => "Claude",
        };

        let script = format!("tell application \"{app_name}\" to activate");
        let status = Command::new("osascript")
            .args(["-e", &script])
            .status()
            .with_context(|| format!("failed to activate `{app_name}`"))?;

        if !status.success() {
            return Err(anyhow!("failed to activate `{app_name}`"));
        }
    }

    Ok(())
}

fn open_with_system(target: &str, reveal_file: bool) -> Result<()> {
    #[cfg(not(target_os = "macos"))]
    let _ = reveal_file;

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        if reveal_file {
            command.args(["-R", target]);
        } else {
            command.arg(target);
        }
        command
    };

    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(target);
        command
    };

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command.args(["/C", "start", "", target]);
        command
    };

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        return Err(anyhow!(
            "open action is not supported on this operating system"
        ));
    }

    let status = command
        .status()
        .with_context(|| format!("failed to launch system open command for `{target}`"))?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("system open command failed for `{target}`"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use agent_cow_core::{
        NavigationTarget, ProviderKind, SessionActivityState, SessionStatus, SessionStatusKind,
        SessionSummary, StatusConfidence, TokenUsage,
    };
    use chrono::Utc;

    use super::{claude_imported_session_exists_in_root, provider_app_target};

    fn sample_summary(provider: ProviderKind) -> SessionSummary {
        SessionSummary {
            id: "claude:00948ef4-2a8d-4375-95f3-a37ee3bb3ad2".to_string(),
            machine_id: "local".to_string(),
            machine_label: "Local".to_string(),
            provider,
            title: "sample".to_string(),
            cwd: String::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            run_started_at: None,
            run_active: false,
            archived: false,
            model: None,
            agent_role: None,
            git_branch: None,
            git_origin_url: None,
            tokens: TokenUsage::default(),
            cost: None,
            context_window: None,
            status: SessionStatus {
                kind: SessionStatusKind::Idle,
                confidence: StatusConfidence::Inferred,
                reason: "test".to_string(),
            },
            activity_state: SessionActivityState::Idle,
            rollout_path: None,
            navigation: Vec::new(),
        }
    }

    #[test]
    fn claude_app_target_uses_conversation_deeplink() {
        let mut summary = sample_summary(ProviderKind::Claude);
        summary.navigation.push(NavigationTarget {
            kind: agent_cow_core::NavigationKind::ThreadId,
            label: "Conversation".to_string(),
            target: "00948ef4-2a8d-4375-95f3-a37ee3bb3ad2".to_string(),
        });

        let (_, target) = provider_app_target(&summary).expect("expected app target");

        assert!(
            target == "claude://resume?session=00948ef4-2a8d-4375-95f3-a37ee3bb3ad2"
                || target
                    == "claude://claude.ai/claude-code-desktop/local_00948ef4-2a8d-4375-95f3-a37ee3bb3ad2"
        );
    }

    #[test]
    fn claude_app_target_falls_back_to_namespaced_id() {
        let summary = sample_summary(ProviderKind::Claude);

        let (_, target) = provider_app_target(&summary).expect("expected app target");

        assert!(
            target == "claude://resume?session=00948ef4-2a8d-4375-95f3-a37ee3bb3ad2"
                || target
                    == "claude://claude.ai/claude-code-desktop/local_00948ef4-2a8d-4375-95f3-a37ee3bb3ad2"
        );
    }

    #[test]
    fn detects_imported_claude_session_from_store() {
        let root = std::env::temp_dir().join(format!("agent-cow-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let nested = root.join("account").join("workspace");
        fs::create_dir_all(&nested).expect("create nested store");
        fs::write(
            nested.join("local_00948ef4-2a8d-4375-95f3-a37ee3bb3ad2.json"),
            "{}",
        )
        .expect("write imported session marker");

        assert!(claude_imported_session_exists_in_root(
            &root,
            "00948ef4-2a8d-4375-95f3-a37ee3bb3ad2"
        ));

        let _ = fs::remove_dir_all(&root);
    }
}
