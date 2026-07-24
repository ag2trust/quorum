//! Parse `codex exec --json` JSONL events from Codex CLI stdout.
//!
//! Codex emits one JSON object per line. The top-level `type` field selects the
//! event variant; items carry an `item` sub-object with its own `type`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Top-level events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    #[serde(rename = "thread.started")]
    ThreadStarted { thread_id: String },

    #[serde(rename = "turn.started")]
    TurnStarted,

    #[serde(rename = "item.started")]
    ItemStarted { item: Item },

    #[serde(rename = "item.completed")]
    ItemCompleted { item: Item },

    #[serde(rename = "turn.completed")]
    TurnCompleted {
        #[serde(default)]
        usage: Option<Usage>,
    },

    #[serde(rename = "turn.failed")]
    TurnFailed {
        #[serde(default)]
        error: Option<TurnError>,
    },

    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        message: String,
    },

    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// Items (carried by item.started / item.completed)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Item {
    #[serde(rename = "agent_message")]
    AgentMessage {
        id: String,
        #[serde(default)]
        text: String,
    },

    #[serde(rename = "command_execution")]
    CommandExecution {
        id: String,
        #[serde(default)]
        command: String,
        #[serde(default)]
        aggregated_output: String,
        #[serde(default)]
        exit_code: Option<i32>,
        #[serde(default)]
        status: String,
    },

    #[serde(rename = "file_change")]
    FileChange {
        id: String,
        #[serde(default)]
        changes: Vec<FileChangeEntry>,
        #[serde(default)]
        status: String,
    },

    #[serde(rename = "mcp_call")]
    McpCall {
        id: String,
        #[serde(flatten)]
        extra: serde_json::Value,
    },

    #[serde(rename = "error")]
    Error {
        id: String,
        #[serde(default)]
        message: String,
    },

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileChangeEntry {
    pub path: String,
    #[serde(default)]
    pub kind: String,
}

// ---------------------------------------------------------------------------
// Usage & error sub-objects
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub cache_write_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub reasoning_output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnError {
    #[serde(default)]
    pub message: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub fn parse_line(line: &str) -> Option<Event> {
    serde_json::from_str(line).ok()
}

/// True when the event is a terminal success signal.
pub fn is_terminal_success(event: &Event) -> bool {
    matches!(event, Event::TurnCompleted { .. })
}

/// True when the event is a terminal failure signal.
pub fn is_terminal_failure(event: &Event) -> bool {
    matches!(event, Event::TurnFailed { .. })
}

/// Extract plain text from an agent_message item.
pub fn agent_message_text(item: &Item) -> Option<&str> {
    match item {
        Item::AgentMessage { text, .. } if !text.is_empty() => Some(text.as_str()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Fixtures (real Codex JSONL captured from codex-cli 0.145.0) ──────

    const THREAD_STARTED: &str =
        r#"{"type":"thread.started","thread_id":"019f9508-91f0-7f60-b59c-d5d6227fc0f1"}"#;

    const TURN_STARTED: &str = r#"{"type":"turn.started"}"#;

    const AGENT_MESSAGE: &str =
        r#"{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"hello"}}"#;

    const COMMAND_STARTED: &str = r#"{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/bin/zsh -lc 'ls /tmp'","aggregated_output":"","exit_code":null,"status":"in_progress"}}"#;

    const COMMAND_COMPLETED: &str = r#"{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"/bin/zsh -lc 'ls /tmp'","aggregated_output":"a.txt\nb.txt\n","exit_code":0,"status":"completed"}}"#;

    const FILE_CHANGE_STARTED: &str = r#"{"type":"item.started","item":{"id":"item_1","type":"file_change","changes":[{"path":"/tmp/test.txt","kind":"add"}],"status":"in_progress"}}"#;

    const FILE_CHANGE_COMPLETED: &str = r#"{"type":"item.completed","item":{"id":"item_1","type":"file_change","changes":[{"path":"/tmp/test.txt","kind":"add"}],"status":"completed"}}"#;

    const ITEM_ERROR: &str = r#"{"type":"item.completed","item":{"id":"item_0","type":"error","message":"Model metadata for `o4-mini` not found. Defaulting to fallback metadata; this can degrade performance and cause issues."}}"#;

    const TURN_COMPLETED: &str = r#"{"type":"turn.completed","usage":{"input_tokens":15326,"cached_input_tokens":13056,"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":0}}"#;

    const TURN_FAILED: &str = r#"{"type":"turn.failed","error":{"message":"request failed"}}"#;

    const TOP_LEVEL_ERROR: &str = r#"{"type":"error","message":"something went wrong"}"#;

    const TURN_COMPLETED_NO_USAGE: &str = r#"{"type":"turn.completed"}"#;

    // ── thread.started ───────────────────────────────────────────────────

    #[test]
    fn parse_thread_started() {
        let event = parse_line(THREAD_STARTED).unwrap();
        match event {
            Event::ThreadStarted { thread_id } => {
                assert_eq!(thread_id, "019f9508-91f0-7f60-b59c-d5d6227fc0f1");
            }
            _ => panic!("expected ThreadStarted"),
        }
    }

    #[test]
    fn missing_thread_id_is_parse_failure() {
        let line = r#"{"type":"thread.started"}"#;
        assert!(
            parse_line(line).is_none(),
            "thread.started without thread_id must fail"
        );
    }

    // ── turn.started ─────────────────────────────────────────────────────

    #[test]
    fn parse_turn_started() {
        let event = parse_line(TURN_STARTED).unwrap();
        assert!(matches!(event, Event::TurnStarted));
    }

    // ── item.completed: agent_message ────────────────────────────────────

    #[test]
    fn parse_agent_message() {
        let event = parse_line(AGENT_MESSAGE).unwrap();
        match &event {
            Event::ItemCompleted { item } => {
                assert_eq!(agent_message_text(item), Some("hello"));
            }
            _ => panic!("expected ItemCompleted"),
        }
    }

    // ── item.started / item.completed: command_execution ─────────────────

    #[test]
    fn parse_command_started() {
        let event = parse_line(COMMAND_STARTED).unwrap();
        match event {
            Event::ItemStarted {
                item:
                    Item::CommandExecution {
                        status, exit_code, ..
                    },
            } => {
                assert_eq!(status, "in_progress");
                assert!(exit_code.is_none());
            }
            _ => panic!("expected ItemStarted/CommandExecution"),
        }
    }

    #[test]
    fn parse_command_completed() {
        let event = parse_line(COMMAND_COMPLETED).unwrap();
        match event {
            Event::ItemCompleted {
                item:
                    Item::CommandExecution {
                        exit_code,
                        aggregated_output,
                        ..
                    },
            } => {
                assert_eq!(exit_code, Some(0));
                assert!(aggregated_output.contains("a.txt"));
            }
            _ => panic!("expected ItemCompleted/CommandExecution"),
        }
    }

    // ── item.started / item.completed: file_change ───────────────────────

    #[test]
    fn parse_file_change_started() {
        let event = parse_line(FILE_CHANGE_STARTED).unwrap();
        match event {
            Event::ItemStarted {
                item: Item::FileChange {
                    changes, status, ..
                },
            } => {
                assert_eq!(status, "in_progress");
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].kind, "add");
            }
            _ => panic!("expected ItemStarted/FileChange"),
        }
    }

    #[test]
    fn parse_file_change_completed() {
        let event = parse_line(FILE_CHANGE_COMPLETED).unwrap();
        match event {
            Event::ItemCompleted {
                item: Item::FileChange {
                    changes, status, ..
                },
            } => {
                assert_eq!(status, "completed");
                assert_eq!(changes[0].path, "/tmp/test.txt");
            }
            _ => panic!("expected ItemCompleted/FileChange"),
        }
    }

    // ── item.completed: error ────────────────────────────────────────────

    #[test]
    fn parse_item_error() {
        let event = parse_line(ITEM_ERROR).unwrap();
        match event {
            Event::ItemCompleted {
                item: Item::Error { message, .. },
            } => {
                assert!(message.contains("o4-mini"));
            }
            _ => panic!("expected ItemCompleted/Error"),
        }
    }

    #[test]
    fn item_error_is_observable_not_terminal() {
        let event = parse_line(ITEM_ERROR).unwrap();
        assert!(
            !is_terminal_success(&event),
            "item error must not be terminal success"
        );
        assert!(
            !is_terminal_failure(&event),
            "item error must not be terminal failure"
        );
    }

    // ── turn.completed ───────────────────────────────────────────────────

    #[test]
    fn parse_turn_completed_with_usage() {
        let event = parse_line(TURN_COMPLETED).unwrap();
        match event {
            Event::TurnCompleted { usage } => {
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 15326);
                assert_eq!(u.output_tokens, 5);
                assert_eq!(u.cached_input_tokens, 13056);
            }
            _ => panic!("expected TurnCompleted"),
        }
    }

    #[test]
    fn parse_turn_completed_without_usage() {
        let event = parse_line(TURN_COMPLETED_NO_USAGE).unwrap();
        match event {
            Event::TurnCompleted { usage } => assert!(usage.is_none()),
            _ => panic!("expected TurnCompleted"),
        }
    }

    #[test]
    fn turn_completed_is_terminal_success() {
        let event = parse_line(TURN_COMPLETED).unwrap();
        assert!(is_terminal_success(&event));
        assert!(!is_terminal_failure(&event));
    }

    // ── turn.failed ──────────────────────────────────────────────────────

    #[test]
    fn parse_turn_failed() {
        let event = parse_line(TURN_FAILED).unwrap();
        match event {
            Event::TurnFailed { error } => {
                assert_eq!(error.unwrap().message, "request failed");
            }
            _ => panic!("expected TurnFailed"),
        }
    }

    #[test]
    fn turn_failed_is_terminal_failure() {
        let event = parse_line(TURN_FAILED).unwrap();
        assert!(is_terminal_failure(&event));
        assert!(!is_terminal_success(&event));
    }

    // ── top-level error ──────────────────────────────────────────────────

    #[test]
    fn parse_top_level_error() {
        let event = parse_line(TOP_LEVEL_ERROR).unwrap();
        match event {
            Event::Error { message } => {
                assert_eq!(message, "something went wrong");
            }
            _ => panic!("expected Error"),
        }
    }

    // ── unknown events ───────────────────────────────────────────────────

    #[test]
    fn unknown_event_type_parses_as_unknown() {
        let line = r#"{"type":"session.config","config":{}}"#;
        let event = parse_line(line).unwrap();
        assert!(matches!(event, Event::Unknown));
    }

    #[test]
    fn unknown_event_cannot_create_lifecycle_completion() {
        let event = parse_line(r#"{"type":"future.event","data":42}"#).unwrap();
        assert!(!is_terminal_success(&event));
        assert!(!is_terminal_failure(&event));
    }

    // ── unknown item types ───────────────────────────────────────────────

    #[test]
    fn unknown_item_type_parses_as_item_unknown() {
        let line = r#"{"type":"item.completed","item":{"type":"future_tool","id":"x"}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::ItemCompleted { item } => assert!(matches!(item, Item::Unknown)),
            _ => panic!("expected ItemCompleted"),
        }
    }

    // ── malformed input ──────────────────────────────────────────────────

    #[test]
    fn invalid_json_returns_none() {
        assert!(parse_line("not json").is_none());
        assert!(parse_line("").is_none());
    }

    // ── negative verification: compound scenarios ────────────────────────

    /// item error + turn.completed + exit 0 = successful run.
    #[test]
    fn item_error_followed_by_turn_completed_is_success() {
        let events: Vec<Event> = [ITEM_ERROR, TURN_COMPLETED]
            .iter()
            .filter_map(|l| parse_line(l))
            .collect();

        assert_eq!(events.len(), 2);
        assert!(!is_terminal_failure(&events[0]));
        assert!(is_terminal_success(&events[1]));
    }

    /// EOF before terminal event = failure (no terminal success seen).
    #[test]
    fn eof_before_terminal_is_failure() {
        let events: Vec<Event> = [THREAD_STARTED, TURN_STARTED, AGENT_MESSAGE]
            .iter()
            .filter_map(|l| parse_line(l))
            .collect();

        assert!(
            !events.iter().any(is_terminal_success),
            "no terminal success before EOF"
        );
    }

    /// Full successful conversation: thread.started -> turn.started ->
    /// agent_message -> turn.completed
    #[test]
    fn full_success_sequence() {
        let events: Vec<Event> = [THREAD_STARTED, TURN_STARTED, AGENT_MESSAGE, TURN_COMPLETED]
            .iter()
            .filter_map(|l| parse_line(l))
            .collect();

        assert_eq!(events.len(), 4);
        match &events[0] {
            Event::ThreadStarted { thread_id } => {
                assert!(!thread_id.is_empty());
            }
            _ => panic!("first event must be ThreadStarted"),
        }
        assert!(is_terminal_success(events.last().unwrap()));
    }

    /// Sequence with item error that still succeeds overall.
    #[test]
    fn error_then_work_then_success() {
        let lines = [
            THREAD_STARTED,
            TURN_STARTED,
            ITEM_ERROR,
            AGENT_MESSAGE,
            TURN_COMPLETED,
        ];
        let events: Vec<Event> = lines.iter().filter_map(|l| parse_line(l)).collect();
        assert_eq!(events.len(), 5);
        let terminal = events.last().unwrap();
        assert!(is_terminal_success(terminal));
    }

    /// MCP call item parses (forward-compatible).
    #[test]
    fn mcp_call_item_parses() {
        let line = r#"{"type":"item.completed","item":{"id":"item_3","type":"mcp_call","server":"my-server","tool":"read","args":{}}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::ItemCompleted {
                item: Item::McpCall { id, .. },
            } => {
                assert_eq!(id, "item_3");
            }
            _ => panic!("expected ItemCompleted/McpCall"),
        }
    }
}
