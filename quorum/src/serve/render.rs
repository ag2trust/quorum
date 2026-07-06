//! Shared renderer: turn stream events into clean human-readable text.
//!
//! Used by both transcript.md appends and `quorum tail`.

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
            let tokens = usage
                .as_ref()
                .map_or(0, |u| u.input_tokens + u.output_tokens);
            let cost_str = total_cost_usd
                .map(|c| format!(" · ${c:.4}"))
                .unwrap_or_default();
            Some(format!("---\n*Turn complete: {tokens} tokens{cost_str}*\n"))
        }
        Event::Other => None,
    }
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
    use serde_json::json;

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
                output_tokens: 100,
            }),
            total_cost_usd: Some(0.05),
            num_turns: None,
            duration_ms: None,
            is_error: None,
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("300 tokens"));
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
    fn truncate_multibyte_utf8_does_not_panic() {
        let event = Event::ToolUse {
            name: "Bash".into(),
            input: json!({"command": "echo héllo wörld über alles café"}),
        };
        let rendered = render_event(&event).unwrap();
        assert!(rendered.contains("> Bash:"));
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
}
