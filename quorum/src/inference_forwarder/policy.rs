//! Request admission for the inference forwarder.
//!
//! A sandboxed run may ask the upstream for text and for calls to tools the
//! run itself executes. It may not make the upstream act on its behalf:
//! fetch a URL, run code, reach an MCP server, or read a stored file. The
//! policy is an allowlist: a request field, content block, or tool kind that
//! is not named here is refused before anything is forwarded, so a feature
//! the upstream adds later is closed until it is reviewed. Every message
//! returned from this module is safe to send back to the caller and never
//! quotes request content beyond a short, sanitized identifier.

use serde_json::{Map, Value};

/// Largest `max_tokens` a request may ask for. It bounds what one request
/// can reserve from a run's allowance.
const MAX_OUTPUT_TOKENS: u64 = 1_000_000;

/// Top-level request fields a run may send. None of them makes the upstream
/// reach anything outside the request.
const TOP_LEVEL_FIELDS: [&str; 15] = [
    "model",
    "max_tokens",
    "messages",
    "system",
    "tools",
    "tool_choice",
    "stream",
    "temperature",
    "top_p",
    "top_k",
    "stop_sequences",
    "metadata",
    "thinking",
    "output_config",
    "cache_control",
];

/// The field that configures server-side context edits. Admitted only for
/// compaction, and only when the operator enabled it.
const CONTEXT_MANAGEMENT: &str = "context_management";
/// Prefix of the context edit types that compact (summarize) a conversation.
const COMPACTION_EDIT_PREFIX: &str = "compact_";
/// Prefix of the `anthropic-beta` values that enable compaction.
pub(super) const COMPACTION_BETA_PREFIX: &str = "compact-";

/// Operator-configured limits on what a run may request.
#[derive(Debug, Clone, Default)]
pub(super) struct Policy {
    /// `None` admits any model; `Some` admits exactly the listed ids.
    pub(super) allowed_models: Option<Vec<String>>,
    /// `anthropic-beta` values a run may send. Empty by default: prompt
    /// caching and tools the run executes itself need no beta.
    pub(super) allowed_betas: Vec<String>,
    /// Whether a run may ask the upstream to compact its conversation. The
    /// upstream then does extra billable work the request does not show.
    pub(super) allow_compaction: bool,
}

/// The parts of an admitted request the forwarder needs afterwards.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Admitted {
    pub(super) model: String,
    /// The request's output ceiling.
    pub(super) max_tokens: u64,
    /// Whether the request lets the upstream compact the conversation.
    pub(super) compaction: bool,
}

/// Printable, bounded form of a caller-supplied identifier for an error
/// message or a log line.
pub(super) fn printable(value: &str) -> String {
    let mut out: String = value
        .chars()
        .take(64)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '@' | '/') {
                c
            } else {
                '?'
            }
        })
        .collect();
    if value.chars().count() > 64 {
        out.push_str("...");
    }
    out
}

fn unavailable(what: &str, name: &str) -> String {
    format!(
        "{what} {} is not available through this forwarder",
        printable(name)
    )
}

impl Policy {
    /// Admit or refuse one parsed request body. Every request must state a
    /// bounded `max_tokens`: it is what the run's token allowance is held
    /// against while the request is in flight.
    pub(super) fn admit(&self, body: &Value) -> Result<Admitted, String> {
        let body = body
            .as_object()
            .ok_or_else(|| "request body must be a JSON object".to_string())?;
        for field in body.keys() {
            let known = TOP_LEVEL_FIELDS.contains(&field.as_str())
                || (self.allow_compaction && field == CONTEXT_MANAGEMENT);
            if !known {
                return Err(unavailable("request field", field));
            }
        }
        let model = match body.get("model") {
            Some(Value::String(model)) if !model.is_empty() => model.clone(),
            _ => return Err("model must be a non-empty string".into()),
        };
        if let Some(allowed) = &self.allowed_models {
            if !allowed.contains(&model) {
                return Err(format!(
                    "model {} is not allowed by this forwarder",
                    printable(&model)
                ));
            }
        }
        let max_tokens = match body.get("max_tokens").and_then(Value::as_u64) {
            Some(max_tokens) if max_tokens <= MAX_OUTPUT_TOKENS => max_tokens,
            _ => {
                return Err(format!(
                    "max_tokens must be an integer from 0 to {MAX_OUTPUT_TOKENS}"
                ))
            }
        };
        let compaction = match body.get(CONTEXT_MANAGEMENT) {
            None => false,
            Some(value) => {
                check_context_management(value)?;
                true
            }
        };
        check_tools(body)?;
        match body.get("system") {
            None | Some(Value::Null | Value::String(_)) => {}
            Some(Value::Array(blocks)) => {
                for block in blocks {
                    check_block(block, &["text"])?;
                }
            }
            Some(_) => return Err("system must be a string or an array of text blocks".into()),
        }
        let messages = match body.get("messages") {
            Some(Value::Array(messages)) => messages,
            _ => return Err("messages must be an array".into()),
        };
        for message in messages {
            self.check_message(message)?;
        }
        Ok(Admitted {
            model,
            max_tokens,
            compaction,
        })
    }

    fn check_message(&self, message: &Value) -> Result<(), String> {
        let message = message
            .as_object()
            .ok_or_else(|| "messages entries must be objects".to_string())?;
        let from_assistant = message.get("role").and_then(Value::as_str) == Some("assistant");
        // A compaction block is the upstream's own summary of earlier turns,
        // replayed by the caller. It is accepted only where the upstream
        // would have written it, and only with compaction enabled.
        let allowed: &[&str] = if self.allow_compaction && from_assistant {
            &[
                "text",
                "tool_use",
                "tool_result",
                "thinking",
                "redacted_thinking",
                "compaction",
            ]
        } else {
            &[
                "text",
                "tool_use",
                "tool_result",
                "thinking",
                "redacted_thinking",
            ]
        };
        match message.get("content") {
            Some(Value::String(_)) => Ok(()),
            Some(Value::Array(blocks)) => blocks
                .iter()
                .try_for_each(|block| check_block(block, allowed)),
            // A single block given without its array is held to the same rules.
            Some(block @ Value::Object(_)) => check_block(block, allowed),
            _ => Err("message content must be a string or an array of content blocks".into()),
        }
    }

    /// Reduce the `anthropic-beta` header values of one request to the
    /// comma-joined list to forward, or refuse the request if it names a
    /// beta the operator has not allowed. Refusing instead of stripping keeps
    /// a request from silently running with different semantics.
    pub(super) fn admit_betas<'a>(
        &self,
        values: impl Iterator<Item = &'a str>,
    ) -> Result<Option<String>, String> {
        let mut admitted: Vec<&str> = Vec::new();
        for beta in values.flat_map(|value| value.split(',')).map(str::trim) {
            if beta.is_empty() {
                continue;
            }
            if !self.allowed_betas.iter().any(|entry| entry == beta) {
                return Err(format!(
                    "anthropic-beta {} is not allowed by this forwarder",
                    printable(beta)
                ));
            }
            if !admitted.contains(&beta) {
                admitted.push(beta);
            }
        }
        Ok((!admitted.is_empty()).then(|| admitted.join(",")))
    }
}

/// `context_management` may carry compaction edits and nothing else: every
/// other edit type, and every other key, is refused.
fn check_context_management(value: &Value) -> Result<(), String> {
    let config = value
        .as_object()
        .ok_or_else(|| "context_management must be an object".to_string())?;
    if let Some(field) = config.keys().find(|field| *field != "edits") {
        return Err(unavailable("context_management field", field));
    }
    let edits = config
        .get("edits")
        .and_then(Value::as_array)
        .ok_or_else(|| "context_management.edits must be an array".to_string())?;
    for edit in edits {
        match edit.get("type").and_then(Value::as_str) {
            Some(kind) if kind.starts_with(COMPACTION_EDIT_PREFIX) => {}
            Some(kind) => return Err(unavailable("context edit", kind)),
            None => return Err("context_management edits must have a type".into()),
        }
    }
    Ok(())
}

/// Only tools the caller executes are admitted: entries with no `type` or
/// `type: "custom"`. Every other type is defined by the upstream, and many of
/// those run on the upstream's side with network or code-execution reach.
fn check_tools(body: &Map<String, Value>) -> Result<(), String> {
    let tools = match body.get("tools") {
        None | Some(Value::Null) => return Ok(()),
        Some(Value::Array(tools)) => tools,
        Some(_) => return Err("tools must be an array".into()),
    };
    for tool in tools {
        let tool = tool
            .as_object()
            .ok_or_else(|| "tools entries must be objects".to_string())?;
        match tool.get("type") {
            None => {}
            Some(Value::String(kind)) if kind == "custom" => {}
            Some(Value::String(kind)) => {
                return Err(format!(
                    "tool type {} is not available through this forwarder; only custom tools are",
                    printable(kind)
                ))
            }
            Some(_) => return Err("tool type must be a string".into()),
        }
    }
    Ok(())
}

/// Admit one content block if its type is in `allowed`. Blocks that point at
/// anything outside the request are refused whatever their type, and so is
/// every type whose token cost its size does not bound (images, documents):
/// none of those is on any allowlist. Model-authored `tool_use` input is
/// opaque data and is not walked.
fn check_block(block: &Value, allowed: &[&str]) -> Result<(), String> {
    let block = block
        .as_object()
        .ok_or_else(|| "content blocks must be objects".to_string())?;
    let kind = block
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "content blocks must have a type".to_string())?;
    if !allowed.contains(&kind) {
        return Err(unavailable("content block type", kind));
    }
    for field in ["source", "file_id"] {
        if block.contains_key(field) {
            return Err(unavailable("content block field", field));
        }
    }
    match kind {
        // A tool result carries text only: a string, or text blocks.
        "tool_result" => match block.get("content") {
            None | Some(Value::Null | Value::String(_)) => Ok(()),
            Some(Value::Array(blocks)) => blocks
                .iter()
                .try_for_each(|inner| check_block(inner, &["text"])),
            Some(_) => {
                Err("tool_result content must be a string or an array of text blocks".into())
            }
        },
        "compaction" => match block.get("content") {
            None | Some(Value::Null | Value::String(_)) => Ok(()),
            Some(_) => Err("compaction content must be a string".into()),
        },
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> Value {
        json!({
            "model": "model-a",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hello"}]
        })
    }

    fn with(field: &str, value: Value) -> Value {
        let mut body = base();
        body[field] = value;
        body
    }

    fn user(block: Value) -> Value {
        with("messages", json!([{"role": "user", "content": [block]}]))
    }

    fn refused(body: &Value) -> String {
        Policy::default().admit(body).unwrap_err()
    }

    fn compacting() -> Policy {
        Policy {
            allow_compaction: true,
            ..Policy::default()
        }
    }

    #[test]
    fn plain_requests_and_custom_tools_are_admitted() {
        let policy = Policy::default();
        let admitted = policy.admit(&base()).unwrap();
        assert_eq!(admitted.model, "model-a");
        assert_eq!(admitted.max_tokens, 16);
        assert!(!admitted.compaction);
        let tools = json!([
            {"name": "read", "description": "d", "input_schema": {"type": "object"},
             "cache_control": {"type": "ephemeral"}},
            {"type": "custom", "name": "write", "input_schema": {"type": "object"}}
        ]);
        policy.admit(&with("tools", tools)).unwrap();
        let messages = json!([
            {"role": "user", "content": [
                {"type": "text", "text": "t", "cache_control": {"type": "ephemeral"}}
            ]},
            // A model-authored tool input may use any key names it likes.
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "", "signature": "s"},
                {"type": "redacted_thinking", "data": "d"},
                {"type": "tool_use", "id": "t1", "name": "read",
                 "input": {"file_id": "f", "source": {"type": "url", "url": "http://x"}}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": [{"type": "text", "text": "ok"}]},
                {"type": "tool_result", "tool_use_id": "t2", "content": "ok", "is_error": true},
                {"type": "tool_result", "tool_use_id": "t3"}
            ]},
            // A single block given without its array.
            {"role": "user", "content": {"type": "text", "text": "t"}}
        ]);
        policy.admit(&with("messages", messages)).unwrap();
        policy
            .admit(&with("system", json!([{"type": "text", "text": "s"}])))
            .unwrap();
        policy.admit(&with("system", json!("s"))).unwrap();
        // Every allowlisted top-level field at once.
        let mut full = base();
        for field in TOP_LEVEL_FIELDS {
            if full.get(field).is_none() {
                full[field] = json!(null);
            }
        }
        full["stream"] = json!(true);
        full["thinking"] = json!({"type": "adaptive"});
        full["output_config"] = json!({"effort": "high"});
        policy.admit(&full).unwrap();
    }

    #[test]
    fn unknown_top_level_fields_are_refused_by_name() {
        for field in [
            "mcp_servers",
            "container",
            "fallbacks",
            "context_management",
            "compaction",
            "thread",
            "service_tier",
            "betas",
            "Model",
            "some_future_field",
        ] {
            for value in [json!(null), json!([]), json!({"id": "c"}), json!("c")] {
                let message = refused(&with(field, value));
                assert_eq!(
                    message,
                    format!("request field {field} is not available through this forwarder")
                );
            }
        }
        let message = refused(&with("bad\nfield \"x\"", json!(1)));
        assert_eq!(
            message,
            "request field bad?field??x? is not available through this forwarder"
        );
    }

    #[test]
    fn upstream_defined_tools_are_refused() {
        for kind in [
            "web_search_20250305",
            "web_search_20260209",
            "web_fetch_20250910",
            "code_execution_20250825",
            "computer_20250124",
            "bash_20250124",
            "text_editor_20250728",
            "mcp_toolset",
            "memory_20250818",
            "tool_search_tool_regex_20251119",
            "Custom",
            "",
        ] {
            let message = refused(&with("tools", json!([{"type": kind, "name": "x"}])));
            assert!(message.contains("only custom tools"), "{kind}: {message}");
        }
        // One bad entry anywhere refuses the request.
        let mixed = json!([{"name": "ok", "input_schema": {}}, {"type": "web_search_20250305", "name": "w"}]);
        refused(&with("tools", mixed));
        for malformed in [json!({}), json!("x"), json!([1]), json!([{"type": 1}])] {
            refused(&with("tools", malformed));
        }
    }

    #[test]
    fn unknown_content_block_types_are_refused_wherever_blocks_appear() {
        let blocks = [
            json!({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}}),
            json!({"type": "image", "source": {"type": "url", "url": "http://internal/x"}}),
            json!({"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": "AA=="}}),
            json!({"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": "x"}}),
            json!({"type": "document", "source": {"type": "file", "file_id": "file_1"}}),
            json!({"type": "container_upload", "file_id": "file_1"}),
            json!({"type": "server_tool_use", "id": "s", "name": "web_search", "input": {}}),
            json!({"type": "web_search_tool_result", "tool_use_id": "s", "content": []}),
            json!({"type": "mcp_tool_use", "id": "m", "name": "n", "server_name": "s", "input": {}}),
            json!({"type": "search_result", "source": "http://x", "title": "t", "content": []}),
            json!({"type": "compaction", "content": "summary"}),
            json!({"type": "some_future_block"}),
            json!({"type": "Text", "text": "t"}),
            json!({"text": "untyped"}),
            json!("not an object"),
        ];
        for block in blocks {
            // Directly in a message, as an array entry and as a bare object.
            refused(&user(block.clone()));
            if block.is_object() {
                refused(&with(
                    "messages",
                    json!([{"role": "user", "content": block}]),
                ));
            }
            refused(&with(
                "messages",
                json!([{"role": "assistant", "content": [block]}]),
            ));
            // Nested in a tool result.
            refused(&user(
                json!({"type": "tool_result", "tool_use_id": "t", "content": [block]}),
            ));
            // In the system prompt.
            refused(&with("system", json!([block])));
        }
        // A tool result holds text only: no nested tool results or tool use,
        // and no object-valued content.
        for content in [
            json!([{"type": "tool_result", "tool_use_id": "t", "content": "x"}]),
            json!([{"type": "tool_use", "id": "t", "name": "n", "input": {}}]),
            json!([{"type": "thinking", "thinking": "t"}]),
            json!({"type": "text", "text": "x"}),
            json!({"type": "image", "source": {"type": "url", "url": "http://x"}}),
            json!(7),
        ] {
            refused(&user(
                json!({"type": "tool_result", "tool_use_id": "t", "content": content}),
            ));
        }
        // The system prompt holds text only.
        refused(&with(
            "system",
            json!([{"type": "tool_use", "id": "t", "name": "n", "input": {}}]),
        ));
        refused(&with("system", json!({"type": "text", "text": "s"})));
        // An allowed block may not point outside the request either.
        refused(&user(json!({"type": "text", "text": "t", "file_id": "f"})));
        refused(&user(
            json!({"type": "text", "text": "t", "source": {"type": "url", "url": "http://x"}}),
        ));
        let message = refused(&user(json!({"type": "image"})));
        assert_eq!(
            message,
            "content block type image is not available through this forwarder"
        );
        for malformed in [json!(null), json!("x"), json!({}), json!([1])] {
            refused(&with("messages", malformed));
        }
        refused(&with("messages", json!([{"role": "user", "content": 7}])));
        refused(&with("messages", json!([{"role": "user"}])));
    }

    #[test]
    fn compaction_is_refused_unless_enabled_and_admits_only_compaction_edits() {
        let compact = json!({"edits": [{"type": "compact_20260112",
            "trigger": {"type": "input_tokens", "value": 100000}}]});
        let replayed = json!([
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": [
                {"type": "compaction", "content": "summary", "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": "t"}]}
        ]);
        // Off by default: both the field and the block are refused.
        assert!(refused(&with("context_management", compact.clone()))
            .starts_with("request field context_management"));
        assert!(refused(&with("messages", replayed.clone()))
            .starts_with("content block type compaction"));

        let policy = compacting();
        assert!(!policy.admit(&base()).unwrap().compaction);
        let admitted = policy.admit(&with("context_management", compact)).unwrap();
        assert!(admitted.compaction);
        policy.admit(&with("messages", replayed)).unwrap();
        // Enabling compaction opens nothing else.
        for edits in [
            json!({"edits": [{"type": "clear_tool_uses_20250919"}]}),
            json!({"edits": [{"type": "compact_20260112"}, {"type": "clear_thinking_20251015"}]}),
            json!({"edits": [{"type": "Compact_20260112"}]}),
            json!({"edits": [{}]}),
            json!({"edits": "compact_20260112"}),
            json!({"edits": [{"type": "compact_20260112"}], "other": 1}),
            json!({}),
            json!(null),
            json!([]),
        ] {
            policy
                .admit(&with("context_management", edits.clone()))
                .expect_err(&edits.to_string());
        }
        let message = policy
            .admit(&with(
                "context_management",
                json!({"edits": [{"type": "clear_tool_uses_20250919"}]}),
            ))
            .unwrap_err();
        assert_eq!(
            message,
            "context edit clear_tool_uses_20250919 is not available through this forwarder"
        );
        // A compaction block is accepted only as replayed assistant content.
        for (role, content) in [
            ("user", json!([{"type": "compaction", "content": "s"}])),
            (
                "assistant",
                json!([{"type": "compaction", "content": [{"type": "text", "text": "s"}]}]),
            ),
            (
                "assistant",
                json!([{"type": "compaction", "content": "s", "file_id": "f"}]),
            ),
        ] {
            policy
                .admit(&with(
                    "messages",
                    json!([{"role": role, "content": content}]),
                ))
                .unwrap_err();
        }
        policy
            .admit(&with(
                "system",
                json!([{"type": "compaction", "content": "s"}]),
            ))
            .unwrap_err();
        for field in ["mcp_servers", "container", "compaction"] {
            policy.admit(&with(field, json!({}))).unwrap_err();
        }
        policy
            .admit(&user(
                json!({"type": "image", "source": {"type": "base64", "data": "AA=="}}),
            ))
            .unwrap_err();
    }

    #[test]
    fn models_are_held_to_the_allowlist() {
        for missing in [json!(null), json!(7), json!("")] {
            refused(&with("model", missing));
        }
        refused(&json!([]));
        let policy = Policy {
            allowed_models: Some(vec!["model-a".into()]),
            ..Policy::default()
        };
        policy.admit(&base()).unwrap();
        let message = policy
            .admit(&with("model", json!("model-b\n\"x")))
            .unwrap_err();
        assert_eq!(message, "model model-b??x is not allowed by this forwarder");
    }

    #[test]
    fn requests_must_state_a_bounded_max_tokens() {
        let policy = Policy::default();
        for max_tokens in [0, MAX_OUTPUT_TOKENS] {
            let admitted = policy.admit(&with("max_tokens", json!(max_tokens)));
            assert_eq!(admitted.unwrap().max_tokens, max_tokens);
        }
        let mut missing = base();
        missing.as_object_mut().unwrap().remove("max_tokens");
        for body in [
            missing,
            with("max_tokens", json!(MAX_OUTPUT_TOKENS + 1)),
            with("max_tokens", json!(u64::MAX)),
            with("max_tokens", json!(-1)),
            with("max_tokens", json!(1.5)),
            with("max_tokens", json!("16")),
            with("max_tokens", json!(null)),
        ] {
            let message = policy.admit(&body).unwrap_err();
            assert!(message.starts_with("max_tokens must be"), "{message}");
        }
    }

    #[test]
    fn betas_must_all_be_allowed() {
        let none = Policy::default();
        assert_eq!(none.admit_betas([].into_iter()), Ok(None));
        assert_eq!(none.admit_betas([" , "].into_iter()), Ok(None));
        let message = none
            .admit_betas(["code-execution-2025-08-25"].into_iter())
            .unwrap_err();
        assert!(message.contains("code-execution-2025-08-25"), "{message}");

        let some = Policy {
            allowed_betas: vec!["beta-a".into(), "beta-b".into()],
            ..Policy::default()
        };
        assert_eq!(
            some.admit_betas(["beta-b, beta-a", "beta-a"].into_iter()),
            Ok(Some("beta-b,beta-a".into()))
        );
        some.admit_betas(["beta-a,beta-c"].into_iter()).unwrap_err();
        some.admit_betas(["beta-a", "BETA-B"].into_iter())
            .unwrap_err();
    }
}
