use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result, anyhow};
use eaglewatch_core::{NavigationKind, ProviderKind, SessionSummary};

pub struct OpenAction {
    pub label: String,
    pub target: String,
}

pub fn local_machine_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

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
        ProviderKind::Claude => Some(("Claude".to_string(), "claude://".to_string())),
    }
}

fn open_target(target: &str) -> Result<()> {
    let path = Path::new(target);
    let metadata = fs::metadata(path)
        .with_context(|| format!("target does not exist or cannot be read: `{target}`"))?;

    open_with_system(target, metadata.is_file())
}

fn open_uri(target: &str) -> Result<()> {
    open_with_system(target, false)
}

fn open_with_system(target: &str, reveal_file: bool) -> Result<()> {
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
