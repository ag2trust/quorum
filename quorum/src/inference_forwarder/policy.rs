//! Request admission for the inference forwarder.
//!
//! A sandboxed run may ask the upstream for text and for calls to tools the
//! run itself executes. It may not make the upstream act on its behalf:
//! fetch a URL, run code, reach an MCP server, or read a stored file. Those
//! requests are refused here, before anything is forwarded. Every message
//! returned from this module is safe to send back to the caller and never
//! quotes request content beyond a short, sanitized identifier.

use serde_json::{Map, Value};

/// Nesting allowed below one message's `content`. Real requests nest two or
/// three levels (a tool result holding a document holding text).
const MAX_CONTENT_DEPTH: usize = 8;

/// Operator-configured limits on what a run may request.
#[derive(Debug, Clone, Default)]
pub(super) struct Policy {
    /// `None` admits any model; `Some` admits exactly the listed ids.
    pub(super) allowed_models: Option<Vec<String>>,
    /// `anthropic-beta` values a run may send. Empty by default: prompt
    /// caching and tools the run executes itself need no beta.
    pub(super) allowed_betas: Vec<String>,
}

/// Largest `max_tokens` a request may ask for. It bounds what one request
/// can reserve from a run's allowance.
const MAX_OUTPUT_TOKENS: u64 = 1_000_000;

/// The parts of an admitted request the forwarder needs afterwards.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Admitted {
    pub(super) model: String,
    /// The request's output ceiling; `None` only where it was not required.
    pub(super) max_tokens: Option<u64>,
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

impl Policy {
    /// Admit or refuse one parsed request body. A request that generates
    /// output must state a bounded `max_tokens`: it is what the run's token
    /// allowance is held against while the request is in flight.
    pub(super) fn admit(&self, body: &Value, generates: bool) -> Result<Admitted, String> {
        let body = body
            .as_object()
            .ok_or_else(|| "request body must be a JSON object".to_string())?;
        let model = match body.get("model") {
            Some(Value::String(model)) if !model.is_empty() => model.clone(),
            _ => return Err("model must be a non-empty string".into()),
        };
        self.check_model(&model)?;
        let max_tokens = match body.get("max_tokens").map(Value::as_u64) {
            Some(Some(max_tokens)) if max_tokens <= MAX_OUTPUT_TOKENS => Some(max_tokens),
            None if !generates => None,
            _ => {
                return Err(format!(
                    "max_tokens must be an integer from 0 to {MAX_OUTPUT_TOKENS}"
                ))
            }
        };
        for field in ["mcp_servers", "container"] {
            if body.contains_key(field) {
                return Err(format!("{field} is not available through this forwarder"));
            }
        }
        self.check_fallbacks(body)?;
        check_tools(body)?;
        if let Some(system) = body.get("system") {
            check_blocks(system, 0)?;
        }
        match body.get("messages") {
            None | Some(Value::Null) => {}
            Some(Value::Array(messages)) => {
                for message in messages {
                    if let Some(content) = message.get("content") {
                        check_blocks(content, 0)?;
                    }
                }
            }
            Some(_) => return Err("messages must be an array".into()),
        }
        Ok(Admitted { model, max_tokens })
    }

    fn check_model(&self, model: &str) -> Result<(), String> {
        match &self.allowed_models {
            Some(allowed) if !allowed.iter().any(|entry| entry == model) => Err(format!(
                "model {} is not allowed by this forwarder",
                printable(model)
            )),
            _ => Ok(()),
        }
    }

    /// A fallback model is another model the request may run on, so it is
    /// held to the same allowlist. The form that lets the upstream choose
    /// cannot be checked and is refused whenever models are restricted.
    fn check_fallbacks(&self, body: &Map<String, Value>) -> Result<(), String> {
        if self.allowed_models.is_none() {
            return Ok(());
        }
        match body.get("fallbacks") {
            None | Some(Value::Null) => Ok(()),
            Some(Value::Array(entries)) => {
                for entry in entries {
                    match entry.get("model") {
                        Some(Value::String(model)) => self.check_model(model)?,
                        _ => return Err("fallbacks entries must name a model".into()),
                    }
                }
                Ok(())
            }
            Some(_) => Err("fallbacks must list allowed models explicitly".into()),
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

/// Refuse content that makes the upstream read something the caller did not
/// send inline (a source fetched by URL, a stored file, a container upload)
/// and content whose token cost its size does not bound (images and
/// documents). Model-authored `tool_use` input is opaque data and is not
/// walked.
fn check_blocks(content: &Value, depth: usize) -> Result<(), String> {
    let Some(blocks) = content.as_array() else {
        return Ok(());
    };
    if depth > MAX_CONTENT_DEPTH {
        return Err("content is nested too deeply".into());
    }
    for block in blocks {
        let Some(block) = block.as_object() else {
            continue;
        };
        if block.contains_key("file_id") {
            return Err("file references are not available through this forwarder".into());
        }
        match block.get("type").and_then(Value::as_str) {
            Some("container_upload") => {
                return Err("container uploads are not available through this forwarder".into())
            }
            // Images and documents cost tokens by dimensions and pages, not
            // by size, so no bound on a request's input could be held
            // against the run's allowance. They are refused even inline.
            Some(kind @ ("image" | "document")) => {
                return Err(format!(
                    "{kind} content is not available through this forwarder; send text"
                ))
            }
            _ => {}
        }
        match block.get("source") {
            None | Some(Value::Null) => {}
            Some(Value::Object(source)) => {
                if source.contains_key("file_id") {
                    return Err("file sources are not available through this forwarder".into());
                }
                match source.get("type").and_then(Value::as_str) {
                    Some("base64" | "text") => {}
                    Some("content") => {
                        if let Some(inner) = source.get("content") {
                            check_blocks(inner, depth + 1)?;
                        }
                    }
                    Some(kind) => {
                        return Err(format!(
                        "{} sources are not available through this forwarder; send content inline",
                        printable(kind)
                    ))
                    }
                    None => return Err("content source must have a type".into()),
                }
            }
            Some(_) => return Err("content source must be an object".into()),
        }
        if let Some(inner) = block.get("content") {
            check_blocks(inner, depth + 1)?;
        }
    }
    Ok(())
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

    fn refused(body: &Value) -> String {
        Policy::default().admit(body, true).unwrap_err()
    }

    #[test]
    fn plain_requests_and_custom_tools_are_admitted() {
        let policy = Policy::default();
        assert_eq!(policy.admit(&base(), true).unwrap().model, "model-a");
        let tools = json!([
            {"name": "read", "description": "d", "input_schema": {"type": "object"},
             "cache_control": {"type": "ephemeral"}},
            {"type": "custom", "name": "write", "input_schema": {"type": "object"}}
        ]);
        policy.admit(&with("tools", tools), true).unwrap();
        // Text, tool results, and cache breakpoints are ordinary.
        let messages = json!([
            {"role": "user", "content": [
                {"type": "text", "text": "t", "cache_control": {"type": "ephemeral"}}
            ]},
            // A model-authored tool input may use any key names it likes.
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "t1", "name": "read",
                 "input": {"file_id": "f", "source": {"type": "url", "url": "http://x"}}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": [{"type": "text", "text": "ok"}]}
            ]}
        ]);
        policy.admit(&with("messages", messages), true).unwrap();
        policy
            .admit(
                &with("system", json!([{"type": "text", "text": "s"}])),
                true,
            )
            .unwrap();
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
    fn upstream_side_resources_are_refused() {
        for field in ["mcp_servers", "container"] {
            for value in [json!([]), json!(null), json!({"id": "c"}), json!("c")] {
                let message = refused(&with(field, value));
                assert!(message.starts_with(field), "{message}");
            }
        }
    }

    #[test]
    fn remote_and_stored_sources_are_refused_at_any_depth() {
        let url_image =
            json!({"type": "image", "source": {"type": "url", "url": "http://internal/x"}});
        let file_doc = json!({"type": "document", "source": {"type": "file", "file_id": "file_1"}});
        let bare_file_id = json!({"type": "document", "file_id": "file_1"});
        let untyped = json!({"type": "document", "source": {"url": "http://internal/x"}});
        let upload = json!({"type": "container_upload"});
        for block in [url_image, file_doc, bare_file_id, untyped, upload] {
            let top = json!([{"role": "user", "content": [block]}]);
            refused(&with("messages", top));
            let nested = json!([{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t", "content": [block]}
            ]}]);
            refused(&with("messages", nested));
            let in_document = json!([{"role": "user", "content": [
                {"type": "document", "source": {"type": "content", "content": [block]}}
            ]}]);
            refused(&with("messages", in_document));
            refused(&with("system", json!([block])));
        }
        refused(&with("messages", json!("not an array")));
    }

    #[test]
    fn images_and_documents_are_refused_even_inline() {
        let image = json!({"type": "image",
            "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}});
        let pdf = json!({"type": "document",
            "source": {"type": "base64", "media_type": "application/pdf", "data": "AA=="}});
        let text_document = json!({"type": "document",
            "source": {"type": "text", "media_type": "text/plain", "data": "x"}});
        let content_document = json!({"type": "document",
            "source": {"type": "content", "content": [{"type": "text", "text": "x"}]}});
        for (block, kind) in [
            (image, "image"),
            (pdf, "document"),
            (text_document, "document"),
            (content_document, "document"),
        ] {
            let top = json!([{"role": "user", "content": [block]}]);
            let message = refused(&with("messages", top));
            assert!(message.starts_with(&format!("{kind} content")), "{message}");
            let nested = json!([{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t", "content": [block]}
            ]}]);
            refused(&with("messages", nested));
        }
    }

    #[test]
    fn deeply_nested_content_is_refused() {
        let mut content = json!([{"type": "text", "text": "x"}]);
        for _ in 0..=MAX_CONTENT_DEPTH {
            content = json!([{"type": "tool_result", "tool_use_id": "t", "content": content}]);
        }
        let message = refused(&with(
            "messages",
            json!([{"role": "user", "content": content}]),
        ));
        assert!(message.contains("nested too deeply"), "{message}");
    }

    #[test]
    fn model_allowlist_applies_to_the_model_and_its_fallbacks() {
        for missing in [json!(null), json!(7), json!("")] {
            refused(&with("model", missing));
        }
        refused(&json!([]));
        let policy = Policy {
            allowed_models: Some(vec!["model-a".into()]),
            ..Policy::default()
        };
        policy.admit(&base(), true).unwrap();
        let message = policy
            .admit(&with("model", json!("model-b\n\"x")), true)
            .unwrap_err();
        assert_eq!(message, "model model-b??x is not allowed by this forwarder");
        policy
            .admit(&with("fallbacks", json!([{"model": "model-a"}])), true)
            .unwrap();
        for fallbacks in [json!([{"model": "model-b"}]), json!("default"), json!([{}])] {
            policy
                .admit(&with("fallbacks", fallbacks), true)
                .unwrap_err();
        }
        // Without a model allowlist, fallbacks are the upstream's business.
        Policy::default()
            .admit(&with("fallbacks", json!("default")), true)
            .unwrap();
    }

    #[test]
    fn generating_requests_must_state_a_bounded_max_tokens() {
        let policy = Policy::default();
        assert_eq!(policy.admit(&base(), true).unwrap().max_tokens, Some(16));
        for max_tokens in [0, MAX_OUTPUT_TOKENS] {
            let admitted = policy.admit(&with("max_tokens", json!(max_tokens)), true);
            assert_eq!(admitted.unwrap().max_tokens, Some(max_tokens));
        }
        let mut missing = base();
        missing.as_object_mut().unwrap().remove("max_tokens");
        for body in [
            missing.clone(),
            with("max_tokens", json!(MAX_OUTPUT_TOKENS + 1)),
            with("max_tokens", json!(u64::MAX)),
            with("max_tokens", json!(-1)),
            with("max_tokens", json!(1.5)),
            with("max_tokens", json!("16")),
            with("max_tokens", json!(null)),
        ] {
            let message = policy.admit(&body, true).unwrap_err();
            assert!(message.starts_with("max_tokens must be"), "{message}");
        }
        // Token counting generates nothing and takes no max_tokens.
        assert_eq!(policy.admit(&missing, false).unwrap().max_tokens, None);
        policy
            .admit(&with("max_tokens", json!("16")), false)
            .unwrap_err();
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
