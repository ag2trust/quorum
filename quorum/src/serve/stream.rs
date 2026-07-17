//! Parse stream-json events from claude's stdout (one JSON object per line).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    #[serde(rename = "assistant")]
    Assistant {
        #[serde(default)]
        message: serde_json::Value,
    },
    #[serde(rename = "tool_use")]
    ToolUse {
        #[serde(default)]
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    #[serde(rename = "result")]
    Result {
        #[serde(default)]
        result: serde_json::Value,
        #[serde(default)]
        usage: Option<Usage>,
        #[serde(default)]
        total_cost_usd: Option<f64>,
        #[serde(default)]
        num_turns: Option<u64>,
        #[serde(default)]
        duration_ms: Option<u64>,
        #[serde(default)]
        is_error: Option<bool>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

pub fn parse_line(line: &str) -> Option<Event> {
    serde_json::from_str(line).ok()
}

/// Extract the plain text emitted by an assistant turn, regardless of
/// stream-json content shape.
///
/// Real Claude assistant messages carry `content` as an ARRAY of typed blocks
/// (`{"type":"text","text":"..."}`, `{"type":"tool_use",...}`, `{"type":"thinking",...}`).
/// A minority of stubbed/legacy streams send `content` as a plain string. Both
/// shapes must accumulate correctly; anything else (thinking blocks, tool_use
/// blocks, `content` absent) yields `None`, so the caller's accumulator only
/// grows with real user-visible text.
///
/// Used by the classifier, the CLI `review-interpret` command, and the daemon
/// post-merge interpreter — all three previously accepted `content` only when
/// it was a string and would silently drop every block-array turn, losing the
/// response body before the terminal Result event.
pub fn assistant_text(message: &serde_json::Value) -> Option<String> {
    let content = message.get("content")?;

    if let Some(s) = content.as_str() {
        return if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        };
    }

    let blocks = content.as_array()?;
    let mut out = String::new();
    for block in blocks {
        if block.get("type").and_then(|t| t.as_str()) != Some("text") {
            continue;
        }
        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
            out.push_str(text);
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Coerce a Result-event `result` field into plain text. Real streams send
/// either a string (`"result":"..."`), an object/array (JSON payload),
/// or — rarely — the same content-block array shape as an assistant message.
/// The stringify fallback keeps parseable JSON intact so downstream JSON
/// extraction (fenced blocks, direct object) still works.
pub fn result_text(result: &serde_json::Value) -> String {
    if let Some(s) = result.as_str() {
        return s.to_string();
    }
    // Some agents wrap the final answer in a content-block array on the
    // Result event too — flatten to the concatenated text if so.
    if let Some(blocks) = result.as_array() {
        let mut has_text_block = false;
        let mut out = String::new();
        for block in blocks {
            if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    has_text_block = true;
                    out.push_str(text);
                }
            }
        }
        if has_text_block {
            return out;
        }
    }
    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_result_event() {
        let line =
            r#"{"type":"result","result":"done","usage":{"input_tokens":100,"output_tokens":50}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::Result { usage, .. } => {
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 100);
                assert_eq!(u.output_tokens, 50);
            }
            _ => panic!("expected Result event"),
        }
    }

    #[test]
    fn parse_assistant_event() {
        let line = r#"{"type":"assistant","message":{"content":"hello"}}"#;
        let event = parse_line(line).unwrap();
        assert!(matches!(event, Event::Assistant { .. }));
    }

    #[test]
    fn parse_tool_use_event() {
        let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"ls"}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::ToolUse { name, .. } => assert_eq!(name, "Bash"),
            _ => panic!("expected ToolUse event"),
        }
    }

    #[test]
    fn parse_assistant_with_content_array() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"},{"type":"tool_use","name":"Bash","input":{"command":"ls"}}]}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::Assistant { message } => {
                let blocks = message.get("content").unwrap().as_array().unwrap();
                assert_eq!(blocks.len(), 2);
                assert_eq!(blocks[1].get("name").unwrap().as_str().unwrap(), "Bash");
            }
            _ => panic!("expected Assistant event"),
        }
    }

    #[test]
    fn parse_unknown_type_returns_other() {
        let line = r#"{"type":"system","message":"init"}"#;
        let event = parse_line(line).unwrap();
        assert!(matches!(event, Event::Other));
    }

    #[test]
    fn invalid_json_returns_none() {
        assert!(parse_line("not json").is_none());
        assert!(parse_line("").is_none());
    }

    #[test]
    fn result_without_usage() {
        let line = r#"{"type":"result","result":"ok"}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::Result { usage, .. } => assert!(usage.is_none()),
            _ => panic!("expected Result event"),
        }
    }

    #[test]
    fn parse_result_with_cost_fields() {
        let line = r#"{"type":"result","result":"done","usage":{"input_tokens":1000,"output_tokens":500},"total_cost_usd":0.05,"num_turns":3,"duration_ms":12000,"is_error":false}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::Result {
                total_cost_usd,
                num_turns,
                duration_ms,
                is_error,
                ..
            } => {
                assert!((total_cost_usd.unwrap() - 0.05).abs() < f64::EPSILON);
                assert_eq!(num_turns, Some(3));
                assert_eq!(duration_ms, Some(12000));
                assert_eq!(is_error, Some(false));
            }
            _ => panic!("expected Result event"),
        }
    }

    #[test]
    fn assistant_text_extracts_from_string_content() {
        let msg = serde_json::json!({"content": "hello there"});
        assert_eq!(assistant_text(&msg).as_deref(), Some("hello there"));
    }

    #[test]
    fn assistant_text_extracts_from_content_block_array() {
        let msg = serde_json::json!({
            "content": [
                {"type": "text", "text": "part one "},
                {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
                {"type": "text", "text": "part two"},
            ]
        });
        assert_eq!(assistant_text(&msg).as_deref(), Some("part one part two"));
    }

    #[test]
    fn assistant_text_ignores_thinking_and_tool_use_blocks() {
        let msg = serde_json::json!({
            "content": [
                {"type": "thinking", "thinking": "internal"},
                {"type": "tool_use", "name": "Bash", "input": {}},
            ]
        });
        assert!(assistant_text(&msg).is_none());
    }

    #[test]
    fn assistant_text_none_when_content_missing() {
        assert!(assistant_text(&serde_json::json!({})).is_none());
    }

    /// Real stream-json assistant event from Claude — regression pin for #127.
    /// Prior code accepted `content` only when it was a string, so it silently
    /// dropped this shape and left the interpreter response empty.
    #[test]
    fn assistant_text_parses_real_stream_event() {
        let line = r#"{"type":"assistant","message":{"content":[{"text":"{\"findings\": []}","type":"text"}],"id":"msg_01","role":"assistant","type":"message"}}"#;
        let event: Event = serde_json::from_str(line).unwrap();
        let Event::Assistant { message } = event else {
            panic!("expected assistant event");
        };
        assert_eq!(
            assistant_text(&message).as_deref(),
            Some(r#"{"findings": []}"#)
        );
    }

    #[test]
    fn result_text_handles_string() {
        let v = serde_json::json!("plain");
        assert_eq!(result_text(&v), "plain");
    }

    #[test]
    fn result_text_handles_content_block_array() {
        let v = serde_json::json!([
            {"type": "text", "text": "hello "},
            {"type": "text", "text": "world"},
        ]);
        assert_eq!(result_text(&v), "hello world");
    }

    #[test]
    fn result_text_stringifies_json_object() {
        let v = serde_json::json!({"findings": []});
        assert_eq!(result_text(&v), r#"{"findings":[]}"#);
    }

    #[test]
    fn parse_result_without_cost_fields_defaults_none() {
        let line =
            r#"{"type":"result","result":"done","usage":{"input_tokens":100,"output_tokens":50}}"#;
        let event = parse_line(line).unwrap();
        match event {
            Event::Result {
                total_cost_usd,
                num_turns,
                duration_ms,
                is_error,
                ..
            } => {
                assert!(total_cost_usd.is_none());
                assert!(num_turns.is_none());
                assert!(duration_ms.is_none());
                assert!(is_error.is_none());
            }
            _ => panic!("expected Result event"),
        }
    }
}
