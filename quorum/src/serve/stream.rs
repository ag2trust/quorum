//! Parse stream-json events from claude's stdout (one JSON object per line).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}
