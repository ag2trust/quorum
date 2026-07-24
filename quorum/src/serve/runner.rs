//! Unified runner process abstraction over Claude CLI and Codex CLI agents.
//!
//! The daemon's tick loop operates on `RunnerProc` so spawn, drain, and teardown
//! paths share structure while each runner retains its own CLI contract.

use super::agent::AgentProc;
use super::codex_agent::CodexProc;
use super::codex_stream;
use super::stream;

/// Wrapper over a Claude or Codex CLI child process.
pub enum RunnerProc {
    Claude(AgentProc),
    Codex(CodexProc),
}

/// Raw event from either runner, preserving the original type for session logging.
pub enum RawEvent {
    Claude(stream::Event),
    Codex(codex_stream::Event),
}

/// Common post-turn-end data extracted from either runner's terminal event.
pub(crate) struct TurnEndResult {
    pub turn_tokens: i64,
    /// Session-cumulative USD cost. `None` for Codex (never fabricated).
    pub session_cost_usd: Option<f64>,
    pub is_error: bool,
    pub error_text: Option<String>,
}

impl RunnerProc {
    pub async fn kill_and_reap(self) {
        match self {
            Self::Claude(p) => p.kill_and_reap().await,
            Self::Codex(p) => p.kill_and_reap().await,
        }
    }

    pub fn pid(&self) -> Option<i32> {
        match self {
            Self::Claude(p) => p.pid(),
            Self::Codex(p) => p.pid(),
        }
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match self {
            Self::Claude(p) => p.try_wait(),
            Self::Codex(p) => p.try_wait(),
        }
    }

    /// Read the next event from the underlying runner. Returns `None` on EOF.
    pub async fn next_event(&mut self) -> Option<RawEvent> {
        match self {
            Self::Claude(p) => p.next_event().await.map(RawEvent::Claude),
            Self::Codex(p) => p.next_event().await.map(RawEvent::Codex),
        }
    }

    /// Feed a user turn to a persistent Claude process. Returns an error on
    /// Codex — Codex is turn-oriented; use respawn instead.
    pub async fn feed_turn(&mut self, json_turn: &str) -> std::io::Result<()> {
        match self {
            Self::Claude(p) => p.feed_turn(json_turn).await,
            Self::Codex(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Codex is turn-oriented — cannot feed_turn; respawn required",
            )),
        }
    }

    pub fn is_codex(&self) -> bool {
        matches!(self, Self::Codex(_))
    }
}

impl RawEvent {
    /// Serialize this event for session log streaming (stream.jsonl).
    pub fn to_json_line(&self) -> Option<String> {
        match self {
            Self::Claude(e) => serde_json::to_string(e).ok(),
            Self::Codex(e) => serde_json::to_string(e).ok(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_event_claude_serializes() {
        let event = stream::Event::ToolUse {
            name: "Bash".into(),
            input: serde_json::json!({"command": "ls"}),
        };
        let raw = RawEvent::Claude(event);
        let json = raw.to_json_line().unwrap();
        assert!(json.contains("Bash"));
    }

    #[test]
    fn raw_event_codex_serializes() {
        let event = codex_stream::Event::TurnStarted;
        let raw = RawEvent::Codex(event);
        let json = raw.to_json_line().unwrap();
        assert!(json.contains("turn.started"));
    }

    #[test]
    fn turn_end_result_codex_no_cost() {
        let r = TurnEndResult {
            turn_tokens: 5000,
            session_cost_usd: None,
            is_error: false,
            error_text: None,
        };
        assert!(
            r.session_cost_usd.is_none(),
            "Codex must not fabricate cost"
        );
    }
}
