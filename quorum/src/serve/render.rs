//! Shared renderer: turn stream events into clean human-readable text.
//!
//! Used by both transcript.md appends and `quorum tail`.

use super::runner::AgentEvent;
use super::session_log::{SanitizedField, SanitizedSessionEvent};
use super::stream::Event;

/// Render a stream event into human-readable markdown text.
/// Returns `None` for events that produce no visible output (e.g. `Other`).
pub fn render_event(event: &Event) -> Option<String> {
    match event {
        Event::Assistant { message } => render_assistant(message),
        Event::ToolUse { name, input } => Some(render_tool_use(name, input)),
        Event::Result {
            usage,
            total_cost_usd,
            ..
        } => {
            let tokens = usage.as_ref().map_or(0, |u| u.live_total_tokens());
            let cost_str = total_cost_usd
                .map(|c| format!(" · ${c:.4}"))
                .unwrap_or_default();
            Some(format!("---\n*Turn complete: {tokens} tokens{cost_str}*\n"))
        }
        Event::Other => None,
    }
}

/// Render one closed sanitized session event for a decomposition planner tail.
///
/// This deliberately renders only the event's closed categories and structural
/// field summaries. It must never reintroduce source prompt, environment, or
/// provider/tool payloads that the session-log boundary excluded.
pub fn render_sanitized_session_event(event: &SanitizedSessionEvent) -> String {
    match event {
        SanitizedSessionEvent::ProviderLifecycle { provider, phase } => {
            format!("> Provider {} {}\n", label(provider), label(phase))
        }
        SanitizedSessionEvent::TurnLifecycle { turn, phase } => {
            format!("> Turn {turn} {}\n", label(phase))
        }
        SanitizedSessionEvent::CommandSummary {
            command,
            outcome,
            details,
        } => format!(
            "> Command {} {} ({})\n",
            label(command),
            label(outcome),
            sanitized_field_summary(details)
        ),
        SanitizedSessionEvent::ToolSummary {
            tool,
            outcome,
            details,
        } => format!(
            "> Tool {} {} ({})\n",
            label(tool),
            label(outcome),
            sanitized_field_summary(details)
        ),
        SanitizedSessionEvent::TerminalResponse { status, response } => format!(
            "> Terminal response {} ({})\n",
            label(status),
            sanitized_field_summary(response)
        ),
        SanitizedSessionEvent::ProviderFailure {
            provider,
            kind,
            details,
        } => format!(
            "> Provider {} failed: {} ({})\n",
            label(provider),
            label(kind),
            sanitized_field_summary(details)
        ),
        SanitizedSessionEvent::SemanticRejection { kind, details } => format!(
            "> Rejected: {} ({})\n",
            label(kind),
            sanitized_field_summary(details)
        ),
        SanitizedSessionEvent::Completion { outcome } => {
            format!("> Session {}\n", label(outcome))
        }
    }
}

fn sanitized_field_summary(field: &SanitizedField) -> String {
    match field {
        SanitizedField::Structural {
            shape,
            captured_bytes,
            truncation,
        } => field_summary(&label(shape), *captured_bytes, truncation.is_some()),
        SanitizedField::Malformed {
            captured_bytes,
            truncation,
        } => field_summary("malformed", *captured_bytes, truncation.is_some()),
    }
}

fn field_summary(kind: &str, bytes: usize, truncated: bool) -> String {
    let truncation = if truncated { ", truncated" } else { "" };
    format!("{kind}, {bytes} bytes{truncation}")
}

fn label(value: &impl std::fmt::Debug) -> String {
    format!("{value:?}").to_lowercase()
}

fn render_assistant(message: &serde_json::Value) -> Option<String> {
    let content = message.get("content")?;

    if let Some(text) = content.as_str() {
        if text.is_empty() {
            return None;
        }
        return Some(format!("## Assistant\n\n{text}\n"));
    }

    let blocks = content.as_array()?;
    let mut parts = Vec::new();

    for block in blocks {
        let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match block_type {
            "text" => {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    if !text.is_empty() {
                        parts.push(text.to_string());
                    }
                }
            }
            "tool_use" => {
                let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                let input = block
                    .get("input")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                parts.push(render_tool_use(name, &input));
            }
            _ => {}
        }
    }

    if parts.is_empty() {
        return None;
    }

    Some(format!("## Assistant\n\n{}\n", parts.join("\n")))
}

fn render_tool_use(name: &str, input: &serde_json::Value) -> String {
    let snippet = tool_snippet(name, input);
    match snippet {
        Some(s) => format!("> {name}: {s}"),
        None => format!("> {name}"),
    }
}

fn tool_snippet(name: &str, input: &serde_json::Value) -> Option<String> {
    match name {
        "Bash" => {
            let cmd = input.get("command").and_then(|c| c.as_str())?;
            let first_word = cmd.split_whitespace().take(4).collect::<Vec<_>>().join(" ");
            Some(truncate(&first_word, 60))
        }
        "Read" | "Write" | "Edit" => {
            let path = input.get("file_path").and_then(|p| p.as_str())?;
            Some(basename(path).to_string())
        }
        "Grep" => {
            let pattern = input.get("pattern").and_then(|p| p.as_str())?;
            Some(truncate(pattern, 40))
        }
        "Glob" => {
            let pattern = input.get("pattern").and_then(|p| p.as_str())?;
            Some(truncate(pattern, 40))
        }
        "Skill" => {
            let skill = input.get("skill").and_then(|s| s.as_str())?;
            Some(skill.to_string())
        }
        "Agent" => {
            let desc = input
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("subagent");
            Some(truncate(desc, 40))
        }
        _ => {
            if let Some(obj) = input.as_object() {
                if let Some((key, val)) = obj.iter().next() {
                    if let Some(s) = val.as_str() {
                        return Some(format!("{key}={}", truncate(s, 30)));
                    }
                }
            }
            None
        }
    }
}

/// Render a normalized AgentEvent for transcript.md.
pub fn render_agent_event(event: &AgentEvent) -> Option<String> {
    match event {
        AgentEvent::ThreadStarted { .. } => None,
        AgentEvent::AssistantText { text } | AgentEvent::CompletedAssistantText { text, .. } => {
            if text.is_empty() {
                None
            } else {
                Some(format!("## Assistant\n\n{text}\n"))
            }
        }
        AgentEvent::Activity { summary, .. } => Some(format!("> {summary}")),
        AgentEvent::TurnCompleted { usage, cost_usd } => {
            let tokens = usage.map_or(0, |u| u.live_total_tokens());
            let cost_str = cost_usd.map(|c| format!(" · ${c:.4}")).unwrap_or_default();
            Some(format!("---\n*Turn complete: {tokens} tokens{cost_str}*\n"))
        }
        AgentEvent::TurnFailed { message, .. } => Some(format!("---\n*Turn failed: {message}*\n")),
        AgentEvent::MidTurnUsage { .. } => None,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max).collect();
        format!("{truncated}…")
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::session_log::{
        ProviderLifecyclePhase, SanitizedCommandKind, SanitizedCompletionOutcome, SanitizedField,
        SanitizedProvider, SanitizedProviderFailureKind, SanitizedRejectionKind,
        SanitizedSummaryOutcome, SanitizedTerminalStatus, SanitizedToolKind, TurnLifecyclePhase,
    };
    use serde_json::json;

    #[test]
    fn sanitized_planner_events_render_useful_redacted_progress() {
        let secret = "sk-planner-secret-must-not-appear";
        let events = [
            SanitizedSessionEvent::ProviderLifecycle {
                provider: SanitizedProvider::Codex,
                phase: ProviderLifecyclePhase::Started,
            },
            SanitizedSessionEvent::TurnLifecycle {
                turn: 3,
                phase: TurnLifecyclePhase::Continued,
            },
            SanitizedSessionEvent::CommandSummary {
                command: SanitizedCommandKind::Shell,
                outcome: SanitizedSummaryOutcome::Succeeded,
                details: SanitizedField::from_text(secret),
            },
            SanitizedSessionEvent::ToolSummary {
                tool: SanitizedToolKind::Bash,
                outcome: SanitizedSummaryOutcome::Failed,
                details: SanitizedField::from_json(&json!({"credential": secret})),
            },
            SanitizedSessionEvent::TerminalResponse {
                status: SanitizedTerminalStatus::Success,
                response: SanitizedField::from_text(secret),
            },
            SanitizedSessionEvent::ProviderFailure {
                provider: SanitizedProvider::Codex,
                kind: SanitizedProviderFailureKind::Protocol,
                details: SanitizedField::from_text(secret),
            },
            SanitizedSessionEvent::SemanticRejection {
                kind: SanitizedRejectionKind::Validation,
                details: SanitizedField::from_text(secret),
            },
            SanitizedSessionEvent::Completion {
                outcome: SanitizedCompletionOutcome::Completed,
            },
        ];

        let rendered = events
            .iter()
            .map(render_sanitized_session_event)
            .collect::<String>();

        for progress in [
            "Provider codex started",
            "Turn 3 continued",
            "Command shell succeeded",
            "Tool bash failed",
            "Terminal response success",
            "Provider codex failed: protocol",
            "Rejected: validation",
            "Session completed",
        ] {
            assert!(
                rendered.contains(progress),
                "missing {progress}: {rendered}"
            );
        }
        assert!(rendered.contains("string, 33 bytes"));
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("credential"));
    }

    #[test]
    fn assistant_array_content_renders_text() {
        let event = Event::Assistant {
            message: json!({
                "content": [
                    {"type": "text", "text": "Hello from the assistant."},
                ]
            }),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("Hello from the assistant."));
        assert!(rendered.contains("## Assistant"));
    }

    #[test]
    fn assistant_array_with_tool_use_block() {
        let event = Event::Assistant {
            message: json!({
                "content": [
                    {"type": "tool_use", "name": "Bash", "input": {"command": "cargo test"}},
                ]
            }),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("> Bash: cargo test"));
    }

    #[test]
    fn assistant_mixed_content() {
        let event = Event::Assistant {
            message: json!({
                "content": [
                    {"type": "text", "text": "Let me check."},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/a/b/foo.rs"}},
                ]
            }),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("Let me check."));
        assert!(rendered.contains("> Read: foo.rs"));
    }

    #[test]
    fn assistant_string_content_still_works() {
        let event = Event::Assistant {
            message: json!({"content": "plain string"}),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("plain string"));
    }

    #[test]
    fn assistant_thinking_block_ignored() {
        let event = Event::Assistant {
            message: json!({
                "content": [
                    {"type": "thinking", "thinking": "internal reasoning"},
                ]
            }),
        };
        assert!(render_event(&event).is_none());
    }

    #[test]
    fn assistant_empty_content_returns_none() {
        let event = Event::Assistant {
            message: json!({"content": []}),
        };
        assert!(render_event(&event).is_none());
    }

    #[test]
    fn top_level_tool_use_renders() {
        let event = Event::ToolUse {
            name: "Bash".into(),
            input: json!({"command": "ls -la"}),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("> Bash: ls -la"));
    }

    #[test]
    fn result_event_renders() {
        let event = Event::Result {
            result: json!({}),
            usage: Some(super::super::stream::Usage {
                input_tokens: 200,
                cache_read_input_tokens: 900,
                cache_creation_input_tokens: 50,
                output_tokens: 100,
            }),
            total_cost_usd: Some(0.05),
            num_turns: None,
            duration_ms: None,
            is_error: None,
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("300 tokens"));
        assert!(!rendered.contains("1250 tokens"));
        assert!(rendered.contains("$0.0500"));
    }

    #[test]
    fn other_event_returns_none() {
        assert!(render_event(&Event::Other).is_none());
    }

    #[test]
    fn skill_tool_snippet() {
        let event = Event::ToolUse {
            name: "Skill".into(),
            input: json!({"skill": "code-review:code-review", "args": "168"}),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("> Skill: code-review:code-review"));
    }

    #[test]
    fn truncate_multibyte_utf8() {
        assert_eq!(truncate("héllo", 3), "hél…");
        assert_eq!(truncate("café", 4), "café");
        assert_eq!(truncate("café", 3), "caf…");
        // emoji (4-byte chars) — must not panic at any max
        let emoji = "🎉🎊🎈🎁";
        assert_eq!(truncate(emoji, 2), "🎉🎊…");
        assert_eq!(truncate(emoji, 1), "🎉…");
        assert_eq!(truncate(emoji, 4), "🎉🎊🎈🎁");
    }

    #[test]
    fn real_stream_assistant_event() {
        let line = r#"{"type":"assistant","message":{"content":[{"text":"I'll review PR #168. Let me start by invoking the code review skill.","type":"text"}],"id":"msg_01","model":"claude-opus-4-6","role":"assistant","type":"message","usage":{"input_tokens":3,"output_tokens":8}}}"#;
        let event: Event = serde_json::from_str(line).unwrap();
        let rendered = render_event(&event).unwrap();
        assert!(
            rendered.contains("I'll review PR #168"),
            "real stream assistant event must render: {rendered}"
        );
    }

    #[test]
    fn render_agent_event_assistant_text() {
        let event = AgentEvent::AssistantText {
            text: "hello".into(),
        };
        let rendered = render_agent_event(&event).unwrap();
        assert!(rendered.contains("hello"));
        assert!(rendered.contains("## Assistant"));
    }

    #[test]
    fn render_agent_event_activity() {
        let event = AgentEvent::Activity {
            kind: super::super::runner::ActivityKind::ToolUse,
            summary: "Bash: cargo test".into(),
        };
        let rendered = render_agent_event(&event).unwrap();
        assert!(rendered.contains("> Bash: cargo test"));
    }

    #[test]
    fn render_agent_event_turn_completed() {
        let event = AgentEvent::TurnCompleted {
            usage: Some(super::super::runner::TokenUsage {
                input_tokens: 200,
                output_tokens: 100,
                ..Default::default()
            }),
            cost_usd: Some(0.05),
        };
        let rendered = render_agent_event(&event).unwrap();
        assert!(rendered.contains("300 tokens"));
        assert!(rendered.contains("$0.0500"));
    }

    #[test]
    fn render_agent_event_turn_failed() {
        let event = AgentEvent::TurnFailed {
            message: "boom".into(),
            usage: None,
            cost_usd: None,
        };
        let rendered = render_agent_event(&event).unwrap();
        assert!(rendered.contains("Turn failed: boom"));
    }

    #[test]
    fn render_agent_event_mid_turn_usage_invisible() {
        let event = AgentEvent::MidTurnUsage { tokens: 500 };
        assert!(render_agent_event(&event).is_none());
    }
}
