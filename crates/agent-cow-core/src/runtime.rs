use chrono::{DateTime, Duration, Utc};

use crate::{SessionActivityState, SessionStatusKind};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionRuntimeEvidence {
    pub completed: bool,
    pub failed: bool,
    pub waiting_input: bool,
    pub open_turns: usize,
    pub open_tool_calls: usize,
    pub open_exploration_tool_calls: usize,
    pub thinking_signal_at: Option<DateTime<Utc>>,
    pub compaction_signal_at: Option<DateTime<Utc>>,
    pub latest_exploration_signal_at: Option<DateTime<Utc>>,
    pub latest_tool_call_at: Option<DateTime<Utc>>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SessionRuntimeWindows {
    pub stale_after_minutes: i64,
    pub activity_window_seconds: i64,
    pub tool_busy_activity_window_seconds: i64,
    pub compaction_window_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRuntimeState {
    pub status_kind: SessionStatusKind,
    pub activity_state: SessionActivityState,
    pub run_active: bool,
    pub active_turns: usize,
    pub pending_tool_calls: usize,
}

pub fn derive_session_runtime_state(
    evidence: &SessionRuntimeEvidence,
    updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
    windows: SessionRuntimeWindows,
) -> SessionRuntimeState {
    let idle_for = now - updated_at;
    let recent_activity_window = if evidence.open_tool_calls > 0 {
        Duration::seconds(windows.tool_busy_activity_window_seconds)
    } else {
        Duration::seconds(windows.activity_window_seconds)
    };
    let compaction_open = evidence
        .compaction_signal_at
        .filter(|timestamp| {
            now - *timestamp <= Duration::seconds(windows.compaction_window_seconds)
        })
        .is_some_and(|timestamp| {
            evidence
                .latest_tool_call_at
                .is_none_or(|latest_tool_call_at| timestamp >= latest_tool_call_at)
        });
    let exploring_recent = evidence
        .latest_exploration_signal_at
        .filter(|timestamp| now - *timestamp <= recent_activity_window)
        .is_some_and(|timestamp| {
            evidence
                .latest_tool_call_at
                .is_none_or(|latest_tool_call_at| timestamp >= latest_tool_call_at)
        });
    let thinking_signal_recent = evidence
        .thinking_signal_at
        .is_some_and(|timestamp| now - timestamp <= recent_activity_window);
    let run_active = evidence.waiting_input
        || evidence.open_turns > 0
        || evidence.open_tool_calls > 0
        || compaction_open;

    let status_kind = if evidence.waiting_input {
        SessionStatusKind::WaitingInput
    } else if evidence.open_tool_calls > 0 {
        if idle_for <= Duration::minutes(windows.stale_after_minutes) {
            SessionStatusKind::ToolBusy
        } else {
            SessionStatusKind::Stale
        }
    } else if evidence.open_turns > 0 || compaction_open {
        if idle_for <= Duration::minutes(windows.stale_after_minutes) {
            SessionStatusKind::Running
        } else {
            SessionStatusKind::Stale
        }
    } else if evidence.failed {
        SessionStatusKind::Failed
    } else if evidence.completed {
        SessionStatusKind::Completed
    } else if idle_for <= Duration::minutes(windows.stale_after_minutes) {
        SessionStatusKind::Idle
    } else {
        SessionStatusKind::Stale
    };

    let activity_state = match status_kind {
        SessionStatusKind::WaitingInput => SessionActivityState::Waiting,
        SessionStatusKind::Completed
        | SessionStatusKind::Failed
        | SessionStatusKind::Idle
        | SessionStatusKind::Stale
        | SessionStatusKind::Unknown => SessionActivityState::Idle,
        SessionStatusKind::ToolBusy => {
            if evidence.open_exploration_tool_calls > 0 {
                SessionActivityState::Exploring
            } else {
                SessionActivityState::Working
            }
        }
        SessionStatusKind::Running => {
            if compaction_open && evidence.open_tool_calls == 0 {
                SessionActivityState::Compacting
            } else if evidence.open_exploration_tool_calls > 0 || exploring_recent {
                SessionActivityState::Exploring
            } else if evidence.open_tool_calls > 0 {
                SessionActivityState::Working
            } else if evidence.open_turns > 0 && thinking_signal_recent {
                SessionActivityState::Thinking
            } else if evidence.open_turns > 0 {
                SessionActivityState::Working
            } else {
                SessionActivityState::Idle
            }
        }
    };

    SessionRuntimeState {
        status_kind,
        activity_state,
        run_active,
        active_turns: evidence.open_turns,
        pending_tool_calls: evidence.open_tool_calls,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{SessionActivityState, SessionStatusKind};

    use super::{SessionRuntimeEvidence, SessionRuntimeWindows, derive_session_runtime_state};

    fn windows() -> SessionRuntimeWindows {
        SessionRuntimeWindows {
            stale_after_minutes: 20,
            activity_window_seconds: 30,
            tool_busy_activity_window_seconds: 75,
            compaction_window_seconds: 12,
        }
    }

    #[test]
    fn tool_calls_drive_working() {
        let now = Utc::now();
        let state = derive_session_runtime_state(
            &SessionRuntimeEvidence {
                open_turns: 1,
                open_tool_calls: 1,
                latest_tool_call_at: Some(now),
                ..SessionRuntimeEvidence::default()
            },
            now,
            now,
            windows(),
        );

        assert_eq!(state.status_kind, SessionStatusKind::ToolBusy);
        assert_eq!(state.activity_state, SessionActivityState::Working);
    }

    #[test]
    fn explicit_thinking_requires_open_turn() {
        let now = Utc::now();
        let state = derive_session_runtime_state(
            &SessionRuntimeEvidence {
                thinking_signal_at: Some(now),
                ..SessionRuntimeEvidence::default()
            },
            now,
            now,
            windows(),
        );

        assert_eq!(state.status_kind, SessionStatusKind::Idle);
        assert_eq!(state.activity_state, SessionActivityState::Idle);
    }

    #[test]
    fn open_turn_without_thinking_signal_is_working() {
        let now = Utc::now();
        let state = derive_session_runtime_state(
            &SessionRuntimeEvidence {
                open_turns: 1,
                ..SessionRuntimeEvidence::default()
            },
            now,
            now,
            windows(),
        );

        assert_eq!(state.status_kind, SessionStatusKind::Running);
        assert_eq!(state.activity_state, SessionActivityState::Working);
    }

    #[test]
    fn open_turn_with_recent_thinking_signal_is_thinking() {
        let now = Utc::now();
        let state = derive_session_runtime_state(
            &SessionRuntimeEvidence {
                open_turns: 1,
                thinking_signal_at: Some(now),
                ..SessionRuntimeEvidence::default()
            },
            now,
            now,
            windows(),
        );

        assert_eq!(state.status_kind, SessionStatusKind::Running);
        assert_eq!(state.activity_state, SessionActivityState::Thinking);
    }
}
