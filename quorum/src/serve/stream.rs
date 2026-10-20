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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

impl Usage {
    /// Legacy live display/cap measurement. Cache activity remains available
    /// in the stream breakdown but does not change the established gauge.
    pub fn live_total_tokens(&self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
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

/// Upper bound on distinct provider tool-call ids and background-task ids
/// tracked per category. Ids beyond the bound are not tracked, so a flood can
/// only make the slot look *less* busy (normal idle watchdog), never pin it.
pub const MAX_TRACKED_PROVIDER_WORK: usize = 256;
/// Liveness summaries are diagnostics for the session transcript only.
pub const MAX_LIVENESS_SUMMARY_CHARS: usize = 120;

fn bounded_summary(text: String) -> String {
    if text.chars().count() <= MAX_LIVENESS_SUMMARY_CHARS {
        text
    } else {
        let mut out: String = text.chars().take(MAX_LIVENESS_SUMMARY_CHARS - 1).collect();
        out.push('…');
        out
    }
}

fn content_blocks(value: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    value
        .pointer("/message/content")
        .and_then(|content| content.as_array())
        .into_iter()
        .flatten()
}

fn tool_result_ids(value: &serde_json::Value) -> impl Iterator<Item = &str> {
    content_blocks(value)
        .filter(|block| block.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
        .filter_map(|block| block.get("tool_use_id").and_then(|id| id.as_str()))
}

/// Claude stream-json lines that prove the agent is still making progress but
/// carry no turn semantics: `user` lines holding `tool_result` blocks (a tool
/// call finished) and every `system` line (background task lifecycle
/// `task_started` / `task_updated` / `task_notification` /
/// `background_tasks_changed`, hooks, init, compaction status). Returns a
/// bounded transcript summary, or `None` for lines that are not liveness.
pub fn claude_liveness_summary(raw: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    match value.get("type").and_then(|t| t.as_str())? {
        "user" => {
            let results = tool_result_ids(&value).count();
            (results > 0).then(|| bounded_summary(format!("tool result ×{results}")))
        }
        "system" => {
            let subtype = value
                .get("subtype")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown");
            let detail = value
                .get("summary")
                .or_else(|| value.get("description"))
                .and_then(|d| d.as_str())
                .or_else(|| value.pointer("/patch/status").and_then(|s| s.as_str()));
            Some(bounded_summary(match detail {
                Some(detail) => format!("system {subtype}: {detail}"),
                None => format!("system {subtype}"),
            }))
        }
        _ => None,
    }
}

/// How a Claude stdout line relates to the turn the daemon fed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineAttribution {
    /// Not a `result` line.
    Ordinary,
    /// The `result` that ends the daemon-fed turn.
    EndsFedTurn,
    /// A `result` for a turn the daemon did not feed (self-continuation after
    /// a background-task notification, or stale backlog). It must never
    /// complete or fail a daemon turn.
    Unattributed,
}

#[derive(Debug)]
struct FedTurn {
    command_uuid: Option<String>,
    started: bool,
}

/// Stateful view of one persistent Claude child's stdout: outstanding work
/// (tool calls awaiting a `tool_result`, background tasks not yet finished)
/// and exact attribution of `result` lines to daemon-fed turns.
///
/// Attribution: a fed turn is open from `begin_fed_turn` until its result.
/// When the CLI announces `command_lifecycle` records (2.1.304 does for every
/// user message carrying a `uuid`), a result is attributed only after the
/// daemon's own command reported `started`; earlier results belong to a turn
/// the daemon did not feed. Without lifecycle support the first result after
/// a feed is attributed (legacy behaviour). Results with no fed turn open are
/// never attributed.
#[derive(Debug, Default)]
pub struct ClaudeTurnTracker {
    outstanding_tools: std::collections::BTreeSet<String>,
    background_tasks: std::collections::BTreeSet<String>,
    lifecycle_supported: bool,
    fed: Option<FedTurn>,
}

impl ClaudeTurnTracker {
    /// A freshly spawned child is always followed by its first feed; an
    /// implicit open turn keeps pre-feed fixtures and the first-turn result
    /// attributable exactly as before.
    pub fn with_implicit_turn() -> Self {
        let mut tracker = Self::default();
        tracker.begin_fed_turn(None);
        tracker
    }

    pub fn begin_fed_turn(&mut self, command_uuid: Option<String>) {
        self.fed = Some(FedTurn {
            command_uuid,
            started: false,
        });
    }

    pub fn has_outstanding_work(&self) -> bool {
        !self.outstanding_tools.is_empty() || !self.background_tasks.is_empty()
    }

    #[cfg(test)]
    pub fn tracked_len(&self) -> usize {
        self.outstanding_tools.len() + self.background_tasks.len()
    }

    fn insert_bounded(set: &mut std::collections::BTreeSet<String>, id: &str) {
        if set.len() < MAX_TRACKED_PROVIDER_WORK {
            set.insert(id.to_string());
        }
    }

    pub fn observe_line(&mut self, raw: &str) -> LineAttribution {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
            return LineAttribution::Ordinary;
        };
        let str_field = |name: &str| value.get(name).and_then(|v| v.as_str());
        match str_field("type") {
            Some("assistant") => {
                for block in content_blocks(&value) {
                    if block.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                        if let Some(id) = block.get("id").and_then(|id| id.as_str()) {
                            Self::insert_bounded(&mut self.outstanding_tools, id);
                        }
                    }
                }
            }
            Some("user") => {
                for id in tool_result_ids(&value) {
                    self.outstanding_tools.remove(id);
                }
            }
            Some("system") => match (str_field("subtype"), str_field("task_id")) {
                (Some("task_started"), Some(task_id)) => {
                    Self::insert_bounded(&mut self.background_tasks, task_id);
                }
                (Some("task_updated"), Some(task_id)) => {
                    let status = value.pointer("/patch/status").and_then(|s| s.as_str());
                    if status.is_some_and(|status| !matches!(status, "running" | "pending")) {
                        self.background_tasks.remove(task_id);
                    }
                }
                (Some("task_notification"), Some(task_id)) => {
                    self.background_tasks.remove(task_id);
                }
                (Some("background_tasks_changed"), _) => {
                    self.background_tasks.clear();
                    let ids = value
                        .get("tasks")
                        .and_then(|tasks| tasks.as_array())
                        .into_iter()
                        .flatten()
                        .filter_map(|task| task.get("task_id").and_then(|id| id.as_str()));
                    for id in ids {
                        Self::insert_bounded(&mut self.background_tasks, id);
                    }
                }
                _ => {}
            },
            Some("command_lifecycle") => {
                self.lifecycle_supported = true;
                if let Some(fed) = self.fed.as_mut() {
                    let ours = fed.command_uuid.is_some()
                        && fed.command_uuid.as_deref() == str_field("command_uuid");
                    if ours && matches!(str_field("state"), Some("started" | "completed")) {
                        fed.started = true;
                    }
                }
            }
            Some("result") => {
                // A finished provider turn has no live foreground tool call.
                self.outstanding_tools.clear();
                let attributable = self.fed.as_ref().is_some_and(|fed| {
                    !self.lifecycle_supported || fed.command_uuid.is_none() || fed.started
                });
                return if attributable {
                    self.fed = None;
                    LineAttribution::EndsFedTurn
                } else {
                    LineAttribution::Unattributed
                };
            }
            _ => {}
        }
        LineAttribution::Ordinary
    }
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

    // ── Provider-boundary fixtures (Claude Code 2.1.304, captured live) ──
    // `claude -p --output-format stream-json --verbose --input-format
    // stream-json` with a prompt that runs `sleep N` via Bash
    // `run_in_background`. Hook lines were dropped and init lines trimmed;
    // event shapes are otherwise verbatim.
    const BETWEEN_TURNS: &str =
        include_str!("testdata/claude_2_1_304_background_between_turns.jsonl");
    const COMMAND_LIFECYCLE: &str = include_str!("testdata/claude_2_1_304_command_lifecycle.jsonl");
    const QUEUED_MID_TURN: &str = include_str!("testdata/claude_2_1_304_queued_mid_turn.jsonl");

    fn fixture_line<'a>(
        fixture: &'a str,
        kind: &str,
        subtype: Option<&str>,
        nth: usize,
    ) -> &'a str {
        fixture
            .lines()
            .filter(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                value.get("type").and_then(|v| v.as_str()) == Some(kind)
                    && subtype
                        .is_none_or(|s| value.get("subtype").and_then(|v| v.as_str()) == Some(s))
            })
            .nth(nth)
            .unwrap_or_else(|| panic!("fixture lacks {kind}/{subtype:?} #{nth}"))
    }

    #[test]
    fn liveness_summary_covers_tool_results_and_background_task_events() {
        let tool_result = fixture_line(BETWEEN_TURNS, "user", None, 0);
        assert!(claude_liveness_summary(tool_result).is_some());
        for subtype in [
            "task_started",
            "task_updated",
            "task_notification",
            "background_tasks_changed",
            "init",
        ] {
            let line = fixture_line(BETWEEN_TURNS, "system", Some(subtype), 0);
            let summary = claude_liveness_summary(line)
                .unwrap_or_else(|| panic!("{subtype} must count as liveness"));
            assert!(summary.chars().count() <= MAX_LIVENESS_SUMMARY_CHARS);
        }
        // Lines with an established meaning elsewhere, or without any
        // evidence of agent progress, stay out of the liveness channel.
        assert!(claude_liveness_summary(fixture_line(BETWEEN_TURNS, "result", None, 0)).is_none());
        assert!(
            claude_liveness_summary(fixture_line(BETWEEN_TURNS, "assistant", None, 0)).is_none()
        );
        assert!(
            claude_liveness_summary(fixture_line(BETWEEN_TURNS, "rate_limit_event", None, 0))
                .is_none()
        );
        assert!(claude_liveness_summary(fixture_line(
            COMMAND_LIFECYCLE,
            "command_lifecycle",
            None,
            0
        ))
        .is_none());
        assert!(claude_liveness_summary(r#"{"type":"mystery"}"#).is_none());
        assert!(claude_liveness_summary("not json").is_none());
        // A user line without a tool_result block is not tool progress.
        assert!(claude_liveness_summary(
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#
        )
        .is_none());
    }

    #[test]
    fn tracker_holds_background_task_outstanding_across_turn_result() {
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(None);
        let mut saw_result = false;
        for line in BETWEEN_TURNS.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let kind = value["type"].as_str().unwrap();
            let subtype = value.get("subtype").and_then(|v| v.as_str());
            let attribution = tracker.observe_line(line);
            match (kind, subtype) {
                ("system", Some("task_started")) => {
                    assert!(tracker.has_outstanding_work(), "started background task");
                }
                ("result", _) if !saw_result => {
                    saw_result = true;
                    assert_eq!(attribution, LineAttribution::EndsFedTurn);
                    assert!(
                        tracker.has_outstanding_work(),
                        "the background task outlives the turn that started it"
                    );
                }
                ("system", Some("task_notification")) => {
                    assert!(
                        !tracker.has_outstanding_work(),
                        "notification ends the task"
                    );
                }
                ("result", _) => {
                    assert_eq!(
                        attribution,
                        LineAttribution::Unattributed,
                        "a self-continued provider turn was never fed by the daemon"
                    );
                }
                _ => {}
            }
        }
        assert!(saw_result);
        assert!(!tracker.has_outstanding_work());
    }

    #[test]
    fn tracker_tool_use_is_outstanding_until_its_tool_result() {
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(None);
        let tool_use = fixture_line(BETWEEN_TURNS, "assistant", None, 0);
        assert_eq!(tracker.observe_line(tool_use), LineAttribution::Ordinary);
        assert!(tracker.has_outstanding_work());
        // An unrelated tool_result must not clear the outstanding id.
        tracker.observe_line(
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_other","content":"x"}]}}"#,
        );
        assert!(tracker.has_outstanding_work());
        tracker.observe_line(fixture_line(BETWEEN_TURNS, "user", None, 0));
        assert!(!tracker.has_outstanding_work());
    }

    #[test]
    fn tracker_snapshot_and_terminal_update_clear_background_tasks() {
        let mut tracker = ClaudeTurnTracker::default();
        tracker.observe_line(fixture_line(
            BETWEEN_TURNS,
            "system",
            Some("task_started"),
            0,
        ));
        assert!(tracker.has_outstanding_work());
        tracker.observe_line(fixture_line(
            BETWEEN_TURNS,
            "system",
            Some("task_updated"),
            0,
        ));
        assert!(
            !tracker.has_outstanding_work(),
            "completed patch ends the task"
        );

        tracker.observe_line(fixture_line(
            BETWEEN_TURNS,
            "system",
            Some("background_tasks_changed"),
            0,
        ));
        assert!(
            tracker.has_outstanding_work(),
            "snapshot lists a running task"
        );
        tracker.observe_line(fixture_line(
            BETWEEN_TURNS,
            "system",
            Some("background_tasks_changed"),
            1,
        ));
        assert!(
            !tracker.has_outstanding_work(),
            "empty snapshot is authoritative"
        );

        // A non-terminal patch keeps the task outstanding.
        tracker.observe_line(fixture_line(
            BETWEEN_TURNS,
            "system",
            Some("task_started"),
            0,
        ));
        tracker.observe_line(
            r#"{"type":"system","subtype":"task_updated","task_id":"b5l9efi28","patch":{"status":"running"}}"#,
        );
        assert!(tracker.has_outstanding_work());
    }

    #[test]
    fn tracker_result_clears_tool_uses_but_not_background_tasks() {
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(None);
        tracker.observe_line(
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_orphan","name":"Bash","input":{}}]}}"#,
        );
        assert!(tracker.has_outstanding_work());
        tracker.observe_line(r#"{"type":"result","result":"done"}"#);
        assert!(
            !tracker.has_outstanding_work(),
            "a finished turn has no live tool call"
        );
    }

    #[test]
    fn tracker_bounds_tracked_ids() {
        let mut tracker = ClaudeTurnTracker::default();
        for i in 0..(MAX_TRACKED_PROVIDER_WORK * 2) {
            tracker.observe_line(&format!(
                r#"{{"type":"system","subtype":"task_started","task_id":"t{i}"}}"#
            ));
            tracker.observe_line(&format!(
                r#"{{"type":"assistant","message":{{"content":[{{"type":"tool_use","id":"u{i}","name":"Bash","input":{{}}}}]}}}}"#
            ));
        }
        assert!(tracker.tracked_len() <= 2 * MAX_TRACKED_PROVIDER_WORK);
    }

    #[test]
    fn tracker_attributes_result_only_after_fed_command_started() {
        let fed = "33333333-3333-4333-8333-333333333333";
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(Some(fed.into()));
        let mut attributions = Vec::new();
        for line in COMMAND_LIFECYCLE.lines() {
            attributions.push(tracker.observe_line(line));
        }
        assert!(attributions.contains(&LineAttribution::EndsFedTurn));

        // Negative: a provider that announces command lifecycles but never
        // starts the daemon's command must not have any result attributed.
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(Some("44444444-4444-4444-8444-444444444444".into()));
        for line in COMMAND_LIFECYCLE.lines() {
            assert_ne!(
                tracker.observe_line(line),
                LineAttribution::EndsFedTurn,
                "result for another command must stay unattributed: {line}"
            );
        }
    }

    #[test]
    fn tracker_attributes_injected_command_result_to_the_fed_turn() {
        // The daemon's command (2222…) arrived while an earlier command was
        // mid-turn; Claude injected it and emitted one result covering both.
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(Some("11111111-1111-4111-8111-111111111111".into()));
        let mut lines = QUEUED_MID_TURN.lines();
        // Consume the earlier command's opening lines, then the daemon feeds.
        for line in lines.by_ref() {
            tracker.observe_line(line);
            if line.contains("\"type\":\"assistant\"") {
                break;
            }
        }
        tracker.begin_fed_turn(Some("22222222-2222-4222-8222-222222222222".into()));
        let terminals: Vec<_> = lines
            .map(|line| tracker.observe_line(line))
            .filter(|a| *a != LineAttribution::Ordinary)
            .collect();
        assert_eq!(terminals, vec![LineAttribution::EndsFedTurn]);
    }

    #[test]
    fn tracker_without_fed_turn_never_attributes_results() {
        let mut tracker = ClaudeTurnTracker::default();
        tracker.begin_fed_turn(None);
        assert_eq!(
            tracker.observe_line(r#"{"type":"result","result":"a"}"#),
            LineAttribution::EndsFedTurn
        );
        assert_eq!(
            tracker.observe_line(r#"{"type":"result","result":"b","is_error":true}"#),
            LineAttribution::Unattributed
        );
    }
}
