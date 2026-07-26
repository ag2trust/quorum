//! Runner boundary: closed AgentKind enum and normalized AgentEvent.
//!
//! The daemon lifecycle consumes `AgentEvent`, never provider-specific event
//! shapes. Raw JSONL lines are preserved verbatim in stream.jsonl; only fields
//! Quorum consumes are parsed into normalized events. Unknown events are inert.
//!
//! `journal.session_id` is an opaque runner continuation ID: Claude receives a
//! Quorum-generated UUID before spawn; Codex will persist the thread ID from
//! `thread.started`. The column name is retained for schema compatibility.

use super::agent::AgentProc;
use super::codex_agent::CodexProc;
use super::{codex_stream, stream};

/// Closed runner enum — supporting another runner requires an explicit code
/// change, not configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentKind {
    Claude,
    Codex,
}

/// Provider-specific process transport behind the normalized runner boundary.
pub enum RunnerProc {
    Claude(AgentProc),
    Codex(CodexProc),
}

impl RunnerProc {
    pub fn kind(&self) -> AgentKind {
        match self {
            Self::Claude(_) => AgentKind::Claude,
            Self::Codex(_) => AgentKind::Codex,
        }
    }

    pub async fn kill_and_reap(self) {
        match self {
            Self::Claude(proc) => proc.kill_and_reap().await,
            Self::Codex(proc) => proc.kill_and_reap().await,
        }
    }

    pub fn pid(&self) -> Option<i32> {
        match self {
            Self::Claude(proc) => proc.pid(),
            Self::Codex(proc) => proc.pid(),
        }
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match self {
            Self::Claude(proc) => proc.try_wait(),
            Self::Codex(proc) => proc.try_wait(),
        }
    }

    pub async fn next_raw_line(&mut self) -> Option<String> {
        match self {
            Self::Claude(proc) => proc.next_raw_line().await,
            Self::Codex(proc) => proc.next_raw_line().await,
        }
    }

    pub async fn feed_turn(&mut self, turn: &str) -> std::io::Result<()> {
        match self {
            Self::Claude(proc) => proc.feed_turn(turn).await,
            Self::Codex(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Codex is turn-oriented — respawn with its thread ID",
            )),
        }
    }

    pub fn is_codex(&self) -> bool {
        matches!(self, Self::Codex(_))
    }
}

impl AgentKind {
    /// Resolve provider from the model string chosen for a managed run.
    /// Known Codex/OpenAI prefixes route to Codex; everything else
    /// (including short Claude aliases like "sonnet") defaults to Claude.
    pub fn for_model(model: &str) -> Self {
        if model.starts_with("o1-")
            || model.starts_with("o3-")
            || model.starts_with("o4-")
            || model.starts_with("gpt-")
        {
            Self::Codex
        } else {
            Self::Claude
        }
    }
}

impl std::fmt::Display for AgentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claude => f.write_str("claude"),
            Self::Codex => f.write_str("codex"),
        }
    }
}

/// Normalized event consumed by the daemon lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    ThreadStarted {
        thread_id: String,
    },
    /// Runner session identity established. For Claude this fires on the
    /// first assistant event (identity is pre-spawn); for Codex it will fire
    /// on `thread.started`.
    AssistantText {
        text: String,
    },
    Activity {
        kind: ActivityKind,
        summary: String,
    },
    /// Terminal success. `cost_usd` is provider-optional (Claude provides
    /// session-cumulative USD; Codex does not).
    TurnCompleted {
        usage: Option<TokenUsage>,
        cost_usd: Option<f64>,
    },
    TurnFailed {
        message: String,
        usage: Option<TokenUsage>,
        cost_usd: Option<f64>,
    },
    /// Mid-turn usage snapshot (Claude reports per-message usage on assistant
    /// events). Not terminal.
    MidTurnUsage {
        tokens: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    ToolUse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// Normalize a raw Claude stream-json line into zero or more `AgentEvent`s.
///
/// A single Claude line can produce multiple normalized events (e.g. an
/// Assistant message with inline tool_use blocks yields both AssistantText
/// and Activity events). Unknown/unparseable lines produce an empty vec —
/// they are inert and must not advance lifecycle state.
pub fn normalize_claude_line(raw: &str) -> Vec<AgentEvent> {
    let event = match stream::parse_line(raw) {
        Some(e) => e,
        None => return vec![],
    };

    match event {
        stream::Event::Result {
            result,
            usage,
            total_cost_usd,
            is_error,
            ..
        } => {
            let tok = usage.map(|u| TokenUsage {
                input_tokens: u.input_tokens,
                output_tokens: u.output_tokens,
            });
            if is_error.unwrap_or(false) {
                let message = stream::result_text(&result);
                vec![AgentEvent::TurnFailed {
                    message: if message.is_empty() {
                        "agent returned an error result".into()
                    } else {
                        message
                    },
                    usage: tok,
                    cost_usd: total_cost_usd,
                }]
            } else {
                vec![AgentEvent::TurnCompleted {
                    usage: tok,
                    cost_usd: total_cost_usd,
                }]
            }
        }

        stream::Event::Assistant { message } => {
            let mut events = Vec::new();

            if let Some(text) = stream::assistant_text(&message) {
                events.push(AgentEvent::AssistantText { text });
            }

            // Inline tool_use blocks in content array
            if let Some(blocks) = message.get("content").and_then(|c| c.as_array()) {
                for block in blocks {
                    if block.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                        if let Some(name) = block.get("name").and_then(|n| n.as_str()) {
                            let input = block
                                .get("input")
                                .cloned()
                                .unwrap_or(serde_json::Value::Null);
                            let summary = tool_summary(name, &input);
                            events.push(AgentEvent::Activity {
                                kind: ActivityKind::ToolUse,
                                summary,
                            });
                        }
                    }
                }
            }

            // Mid-turn usage on assistant messages
            if let Some(usage) = message.get("usage") {
                let input = usage
                    .get("input_tokens")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let output = usage
                    .get("output_tokens")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                if input + output > 0 {
                    events.push(AgentEvent::MidTurnUsage {
                        tokens: input + output,
                    });
                }
            }

            events
        }

        stream::Event::ToolUse { name, input } => {
            let summary = tool_summary(&name, &input);
            vec![AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary,
            }]
        }

        stream::Event::Other => vec![],
    }
}

pub fn normalize_codex_line(raw: &str) -> Vec<AgentEvent> {
    let event = match codex_stream::parse_line(raw) {
        Some(event) => event,
        None => return vec![],
    };
    match event {
        codex_stream::Event::ThreadStarted { thread_id } => {
            vec![AgentEvent::ThreadStarted { thread_id }]
        }
        codex_stream::Event::ItemStarted { item } | codex_stream::Event::ItemCompleted { item } => {
            match item {
                codex_stream::Item::AgentMessage { text, .. } if !text.is_empty() => {
                    vec![AgentEvent::AssistantText { text }]
                }
                codex_stream::Item::CommandExecution { command, .. } => {
                    vec![AgentEvent::Activity {
                        kind: ActivityKind::ToolUse,
                        summary: tool_summary("command", &serde_json::json!({"command": command})),
                    }]
                }
                codex_stream::Item::FileChange { changes, .. } => {
                    let path = changes
                        .first()
                        .map(|change| change.path.as_str())
                        .unwrap_or("file");
                    vec![AgentEvent::Activity {
                        kind: ActivityKind::ToolUse,
                        summary: tool_summary(
                            "file_change",
                            &serde_json::json!({"file_path": path}),
                        ),
                    }]
                }
                _ => vec![],
            }
        }
        codex_stream::Event::TurnCompleted { usage } => vec![AgentEvent::TurnCompleted {
            usage: usage.map(|usage| TokenUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            }),
            cost_usd: None,
        }],
        codex_stream::Event::TurnFailed { error } => vec![AgentEvent::TurnFailed {
            message: error
                .map(|error| error.message)
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| "Codex turn failed".into()),
            usage: None,
            cost_usd: None,
        }],
        // Codex emits top-level Error events for retryable transport/reconnect
        // warnings before a later terminal turn event. Lifecycle state changes
        // only on turn.completed/turn.failed or authoritative process exit.
        codex_stream::Event::Error { .. } => vec![],
        _ => vec![],
    }
}

pub fn normalize_line(kind: AgentKind, raw: &str) -> Vec<AgentEvent> {
    match kind {
        AgentKind::Claude => normalize_claude_line(raw),
        AgentKind::Codex => normalize_codex_line(raw),
    }
}

/// Compact tool label for live stats (matches existing `now_label` behavior).
fn tool_summary(name: &str, input: &serde_json::Value) -> String {
    let snippet = match name {
        "Bash" => input
            .get("command")
            .and_then(|c| c.as_str())
            .map(|c| c.split_whitespace().take(3).collect::<Vec<_>>().join(" ")),
        "Read" | "Write" | "Edit" => input
            .get("file_path")
            .and_then(|p| p.as_str())
            .map(|p| p.rsplit('/').next().unwrap_or(p).to_string()),
        "Grep" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Glob" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Agent" => input
            .get("description")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };
    match snippet {
        Some(s) => {
            let full = format!("{name}: {s}");
            truncate_label(&full, 24)
        }
        None => name.to_string(),
    }
}

fn truncate_label(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let t: String = s.chars().take(max - 1).collect();
        format!("{t}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_result_success() {
        let line = r#"{"type":"result","result":"done","usage":{"input_tokens":100,"output_tokens":50},"total_cost_usd":0.05}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnCompleted { usage, cost_usd } => {
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 100);
                assert_eq!(u.output_tokens, 50);
                assert!((cost_usd.unwrap() - 0.05).abs() < f64::EPSILON);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn normalize_result_error_carries_usage_and_cost() {
        let line = r#"{"type":"result","result":"quota exceeded","is_error":true,"usage":{"input_tokens":500,"output_tokens":200},"total_cost_usd":0.03}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnFailed {
                message,
                usage,
                cost_usd,
            } => {
                assert_eq!(message, "quota exceeded");
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 500);
                assert_eq!(u.output_tokens, 200);
                assert!((cost_usd.unwrap() - 0.03).abs() < f64::EPSILON);
            }
            other => panic!("expected TurnFailed, got {other:?}"),
        }
    }

    #[test]
    fn normalize_result_error_without_usage() {
        let line = r#"{"type":"result","result":"","is_error":true}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnFailed {
                message,
                usage,
                cost_usd,
            } => {
                assert_eq!(message, "agent returned an error result");
                assert!(usage.is_none());
                assert!(cost_usd.is_none());
            }
            other => panic!("expected TurnFailed, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_text() {
        let line = r#"{"type":"assistant","message":{"content":"hello world"}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::AssistantText { text } => assert_eq!(text, "hello world"),
            other => panic!("expected AssistantText, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_with_content_array() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"},{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}}]}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], AgentEvent::AssistantText { text } if text == "hi"));
        match &events[1] {
            AgentEvent::Activity { kind, summary } => {
                assert_eq!(*kind, ActivityKind::ToolUse);
                assert!(summary.contains("Bash"));
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_with_mid_turn_usage() {
        let line = r#"{"type":"assistant","message":{"content":"text","usage":{"input_tokens":200,"output_tokens":100}}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], AgentEvent::AssistantText { .. }));
        match &events[1] {
            AgentEvent::MidTurnUsage { tokens } => assert_eq!(*tokens, 300),
            other => panic!("expected MidTurnUsage, got {other:?}"),
        }
    }

    #[test]
    fn normalize_top_level_tool_use() {
        let line = r#"{"type":"tool_use","name":"Read","input":{"file_path":"/src/main.rs"}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Activity { kind, summary } => {
                assert_eq!(*kind, ActivityKind::ToolUse);
                assert!(summary.contains("Read"));
                assert!(summary.contains("main.rs"));
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn normalize_unknown_event_type_is_inert() {
        let line = r#"{"type":"system","message":"init"}"#;
        let events = normalize_claude_line(line);
        assert!(events.is_empty(), "unknown events must be inert");
    }

    #[test]
    fn normalize_invalid_json_is_inert() {
        assert!(normalize_claude_line("not json").is_empty());
        assert!(normalize_claude_line("").is_empty());
        assert!(normalize_claude_line("{broken").is_empty());
    }

    #[test]
    fn normalize_result_without_usage() {
        let line = r#"{"type":"result","result":"ok"}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnCompleted { usage, cost_usd } => {
                assert!(usage.is_none());
                assert!(cost_usd.is_none());
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn normalize_thinking_only_assistant_is_empty() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"internal"}]}}"#;
        let events = normalize_claude_line(line);
        assert!(
            events.is_empty(),
            "thinking-only assistant should produce no events"
        );
    }

    #[test]
    fn agent_kind_display() {
        assert_eq!(AgentKind::Claude.to_string(), "claude");
        assert_eq!(AgentKind::Codex.to_string(), "codex");
    }

    #[test]
    fn for_model_claude_prefix() {
        assert_eq!(AgentKind::for_model("claude-sonnet-5"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("claude-opus-4-6"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("claude-opus-4-7"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("claude-opus-4-8"), AgentKind::Claude);
    }

    #[test]
    fn for_model_codex_models() {
        assert_eq!(AgentKind::for_model("o4-mini"), AgentKind::Codex);
        assert_eq!(AgentKind::for_model("gpt-4o"), AgentKind::Codex);
        assert_eq!(AgentKind::for_model("gpt-5.6-codex"), AgentKind::Codex);
    }

    #[test]
    fn for_model_unknown_defaults_claude() {
        assert_eq!(AgentKind::for_model("unknown-model"), AgentKind::Claude);
    }

    #[test]
    fn for_model_short_claude_aliases() {
        assert_eq!(AgentKind::for_model("sonnet"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("sonnet-5"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-46"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-47"), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-48"), AgentKind::Claude);
    }

    #[test]
    fn tool_summary_truncation() {
        let summary = tool_summary(
            "Bash",
            &serde_json::json!({"command": "this is a very long command that should be truncated"}),
        );
        assert!(summary.chars().count() <= 24, "summary too long: {summary}");
    }

    #[test]
    fn raw_line_preservation_round_trip() {
        let raw = r#"{"type":"result","result":"done","extra_field":"preserved","usage":{"input_tokens":1,"output_tokens":2}}"#;
        let events = normalize_claude_line(raw);
        assert_eq!(events.len(), 1);
        // The raw line is distinct from re-serialized — it contains extra_field
        assert!(raw.contains("extra_field"));
    }
}
