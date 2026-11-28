//! External runner stream mapping and exit check: a pure, per-turn state
//! machine that turns the declared JSONL protocol into normalized
//! [`AgentEvent`] values, and a pure exit-check function that decides the
//! turn's result from the terminal event and the observed process exit.
//!
//! Contract: `docs/2026-11-20-external-cli-runner-technical-spec.md`,
//! "Stream mapping", "Terminal event", "Exit check", and "Limits and
//! accounting". Nothing here spawns a process or reads stderr; the process
//! adapter feeds stdout lines and the exit observation in.
//!
//! Metadata-only: no failure detail or `TurnFailed` message carries text from
//! the stream. Messages are built only from names declared in `statuses` and
//! `error_kinds`, or are fixed strings.
//!
//! Like Grok, the terminal event is withheld until the exit check passes:
//! [`ExternalStream::on_line`] never returns a terminal event; it records a
//! [`TerminalSeen`], and [`exit_check`] releases at most one terminal event.

use super::runner::{ActivityKind, AgentEvent, FailureDisposition, RunnerFailure, TokenUsage};
use crate::external_runner_spec::{ExternalSpec, Outcome, Scalar, StatusDisposition};
use serde_json::Value;
use std::collections::BTreeMap;

/// Activity summaries use the same bound as the built-in adapters'
/// `tool_summary` labels (`runner.rs`).
const ACTIVITY_SUMMARY_CHARS: usize = 24;
const ACTIVITY_FALLBACK_SUMMARY: &str = "tool";
const LIVENESS_SUMMARY: &str = "external protocol line";

pub const UNDECLARED_ERROR_KIND: &str = "undeclared-error-kind";
pub const UNDECLARED_TERMINAL_STATUS: &str = "undeclared-terminal-status";

// Fixed failure details (spec "Stream mapping", "Terminal event", "Exit check").
pub const DETAIL_LINE_TOO_LONG: &str = "protocol violation: line exceeds max_line_bytes";
pub const DETAIL_UNSUPPORTED_FORMAT: &str = "unsupported stream format";
pub const DETAIL_SECOND_TERMINAL: &str = "protocol violation: second terminal event";
pub const DETAIL_LINE_AFTER_TERMINAL: &str =
    "protocol violation: protocol line after terminal event";
pub const DETAIL_MALFORMED_USAGE: &str = "protocol violation: malformed terminal usage";
pub const DETAIL_MALFORMED_FINAL_TEXT: &str = "protocol violation: malformed terminal final text";
pub const DETAIL_UNDECLARED_STATUS: &str = "undeclared terminal status";
pub const DETAIL_NO_EXIT_AFTER_TERMINAL: &str = "no exit after terminal event";
pub const DETAIL_EXIT_CONTRADICTS: &str = "exit code contradicts terminal status";
pub const DETAIL_BLOCKED_UNSUPPORTED: &str = "blocked outcome is not supported";
pub const DETAIL_EOF_BEFORE_TERMINAL: &str =
    "provider reached EOF before a terminal protocol event";

fn failure(disposition: FailureDisposition, detail: impl Into<String>) -> RunnerFailure {
    RunnerFailure::classified(disposition, detail, std::io::ErrorKind::Other)
}

/// What one stdout line produced.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LineStep {
    /// Normalized events, in order. Never a terminal event.
    pub events: Vec<AgentEvent>,
    /// The line was not a protocol line (not a JSON object). The adapter
    /// records it in diagnostics; it has no other effect.
    pub non_protocol: bool,
    /// The line stopped the run: the adapter must kill the process tree and
    /// record this failure. Every later line is ignored.
    pub kill: Option<RunnerFailure>,
}

/// The error kind a terminal event carried, as far as Quorum may name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKindSeen {
    /// No `error_kind` pointer is declared, or the line has no value (or
    /// `null`) there.
    Absent,
    /// A name declared in `error_kinds`.
    Declared(String),
    /// Any other value; written only as [`UNDECLARED_ERROR_KIND`].
    Undeclared,
}

/// The accepted terminal event, held until the exit check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSeen {
    /// The status name when it is a string declared in `statuses`; `None`
    /// for an undeclared or non-string status.
    pub status: Option<String>,
    pub error_kind: ErrorKindSeen,
    pub usage: Option<TokenUsage>,
    pub final_text: Option<String>,
}

/// How the run's process ended, as the adapter observed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitObservation {
    /// The process exited with this code.
    Code(i32),
    /// The process died from signal N; it counts as exit 128+N.
    Signal(i32),
    /// Quorum killed the tree (watchdog, cancel, drain, or protocol violation).
    KilledByQuorum,
    /// The process did not exit within `terminal_exit_secs` after the
    /// terminal event.
    NoExitAfterTerminal,
}

impl ExitObservation {
    #[cfg(unix)]
    pub fn from_status(status: std::process::ExitStatus) -> Self {
        use std::os::unix::process::ExitStatusExt;
        match (status.code(), status.signal()) {
            (Some(code), _) => Self::Code(code),
            (None, Some(signal)) => Self::Signal(signal),
            // Neither code nor signal: no exit value can match a status.
            (None, None) => Self::Code(-1),
        }
    }

    /// The exit number the spec compares: the code, or 128+N for signal N.
    fn number(self) -> Option<i64> {
        match self {
            Self::Code(code) => Some(i64::from(code)),
            Self::Signal(signal) => Some(128 + i64::from(signal)),
            Self::KilledByQuorum | Self::NoExitAfterTerminal => None,
        }
    }
}

/// The turn's result after the exit check.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExitVerdict {
    /// The adapter must kill the process tree.
    pub kill: bool,
    /// The single terminal event released for this turn, if any.
    pub event: Option<AgentEvent>,
    /// The run's terminal text (success only).
    pub terminal_text: Option<String>,
    /// The failure to record; `None` on success and on a kill by Quorum,
    /// whose existing handling owns the result.
    pub failure: Option<RunnerFailure>,
}

/// Per-turn stream state. One value per process.
#[derive(Debug)]
pub struct ExternalStream<'a> {
    spec: &'a ExternalSpec,
    line_number: u64,
    saw_protocol_line: bool,
    terminal: Option<TerminalSeen>,
    stopped: bool,
}

impl<'a> ExternalStream<'a> {
    pub fn new(spec: &'a ExternalSpec) -> Self {
        Self {
            spec,
            line_number: 0,
            saw_protocol_line: false,
            terminal: None,
            stopped: false,
        }
    }

    pub fn terminal(&self) -> Option<&TerminalSeen> {
        self.terminal.as_ref()
    }

    pub fn saw_protocol_line(&self) -> bool {
        self.saw_protocol_line
    }

    /// The exit check for this turn (see [`exit_check`]).
    pub fn exit_check(&self, exit: ExitObservation) -> ExitVerdict {
        exit_check(
            self.spec,
            self.terminal.as_ref(),
            self.saw_protocol_line,
            exit,
        )
    }

    fn violation(&mut self, detail: &'static str, events: Vec<AgentEvent>) -> LineStep {
        self.stopped = true;
        LineStep {
            events,
            non_protocol: false,
            kill: Some(failure(FailureDisposition::Unclassified, detail)),
        }
    }

    /// Report a line longer than `max_line_bytes` without buffering it.
    pub fn on_overlong_line(&mut self) -> LineStep {
        self.line_number += 1;
        if self.stopped {
            return LineStep::default();
        }
        self.violation(DETAIL_LINE_TOO_LONG, Vec::new())
    }

    /// Handle one stdout line (a trailing `\n` or `\r\n` is ignored).
    pub fn on_line(&mut self, line: &[u8]) -> LineStep {
        let line = line.strip_suffix(b"\n").unwrap_or(line);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let too_long =
            u64::try_from(line.len()).map_or(true, |len| len > self.spec.stream.max_line_bytes);
        if too_long {
            return self.on_overlong_line();
        }
        self.line_number += 1;
        if self.stopped {
            return LineStep::default();
        }
        let value = match serde_json::from_slice::<Value>(line) {
            Ok(value @ Value::Object(_)) => value,
            _ => {
                return LineStep {
                    non_protocol: true,
                    ..LineStep::default()
                }
            }
        };
        let first = !self.saw_protocol_line;
        self.saw_protocol_line = true;
        let mut events = vec![AgentEvent::Liveness {
            summary: LIVENESS_SUMMARY.into(),
        }];

        if self.terminal.is_some() {
            let stream = &self.spec.stream;
            let detail = if event_type(&value, &stream.type_pointer)
                == Some(stream.terminal.event_type.as_str())
            {
                DETAIL_SECOND_TERMINAL
            } else {
                DETAIL_LINE_AFTER_TERMINAL
            };
            return self.violation(detail, events);
        }

        let stream = &self.spec.stream;
        let kind = event_type(&value, &stream.type_pointer);
        if first {
            if let Some(start) = stream.start.as_ref() {
                let format_ok = match (start.format.as_deref(), start.accept_formats.as_deref()) {
                    (Some(pointer), Some(accepted)) => value
                        .pointer(pointer)
                        .is_some_and(|found| accepted.iter().any(|a| scalar_eq(a, found))),
                    _ => true,
                };
                if kind != Some(start.event_type.as_str()) || !format_ok {
                    self.stopped = true;
                    return LineStep {
                        events,
                        non_protocol: false,
                        kill: Some(failure(
                            FailureDisposition::NonFailover,
                            DETAIL_UNSUPPORTED_FORMAT,
                        )),
                    };
                }
            }
        }

        if let Some(text) = stream.text.as_ref() {
            if kind == Some(text.event_type.as_str()) && when_matches(&value, &text.when) {
                if let Some(body) = value.pointer(&text.text).and_then(Value::as_str) {
                    let item_id = text
                        .id
                        .as_deref()
                        .and_then(|p| value.pointer(p))
                        .and_then(Value::as_str)
                        .map_or_else(|| self.line_number.to_string(), str::to_string);
                    events.push(AgentEvent::CompletedAssistantText {
                        item_id,
                        text: body.to_string(),
                    });
                }
            }
        }
        if let Some(activity) = stream.activity.as_ref() {
            if kind == Some(activity.event_type.as_str()) && when_matches(&value, &activity.when) {
                let summary = activity
                    .summary
                    .as_deref()
                    .and_then(|p| value.pointer(p))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(ACTIVITY_FALLBACK_SUMMARY);
                events.push(AgentEvent::Activity {
                    kind: ActivityKind::ToolUse,
                    summary: truncate_chars(summary, ACTIVITY_SUMMARY_CHARS),
                });
            }
        }

        if kind == Some(stream.terminal.event_type.as_str()) {
            match self.read_terminal(&value) {
                Ok(seen) => self.terminal = Some(seen),
                Err(detail) => return self.violation(detail, events),
            }
        }

        LineStep {
            events,
            non_protocol: false,
            kill: None,
        }
    }

    fn read_terminal(&self, value: &Value) -> Result<TerminalSeen, &'static str> {
        let terminal = &self.spec.stream.terminal;
        let status = value
            .pointer(&terminal.status)
            .and_then(Value::as_str)
            .filter(|name| self.spec.statuses.contains_key(*name))
            .map(str::to_string);
        let error_kind = match terminal
            .error_kind
            .as_deref()
            .and_then(|p| value.pointer(p))
        {
            // JSON `null` reports no kind, the same as a missing member.
            None | Some(Value::Null) => ErrorKindSeen::Absent,
            Some(Value::String(kind)) if self.spec.error_kinds.contains_key(kind) => {
                ErrorKindSeen::Declared(kind.clone())
            }
            Some(_) => ErrorKindSeen::Undeclared,
        };
        let usage = read_usage(value, &terminal.usage)?;
        let final_text = match terminal
            .final_text
            .as_deref()
            .and_then(|p| value.pointer(p))
        {
            None => None,
            Some(Value::String(text)) if !text.contains('\0') => Some(text.clone()),
            Some(_) => return Err(DETAIL_MALFORMED_FINAL_TEXT),
        };
        Ok(TerminalSeen {
            status,
            error_kind,
            usage,
            final_text,
        })
    }
}

/// Spec "Terminal event" > usage and "Limits and accounting": each declared
/// pointer must hold a non-negative integer; a missing one counts as 0.
/// `None` when the declaration names no usage pointer at all.
fn read_usage(
    value: &Value,
    usage: &crate::external_runner_spec::UsageFile,
) -> Result<Option<TokenUsage>, &'static str> {
    let pointers = [
        usage.uncached_input.as_deref(),
        usage.cached_input.as_deref(),
        usage.cache_write_input.as_deref(),
        usage.output.as_deref(),
    ];
    if pointers.iter().all(Option::is_none) {
        return Ok(None);
    }
    let mut counts = [0u64; 4];
    for (count, pointer) in counts.iter_mut().zip(pointers) {
        match pointer.and_then(|p| value.pointer(p)) {
            None => {}
            Some(found) => *count = found.as_u64().ok_or(DETAIL_MALFORMED_USAGE)?,
        }
    }
    let [uncached, cached, cache_write, output] = counts;
    Ok(Some(TokenUsage {
        input_tokens: uncached.saturating_add(cached).saturating_add(cache_write),
        uncached_input_tokens: uncached,
        cached_input_tokens: cached,
        cache_write_input_tokens: cache_write,
        output_tokens: output,
        reasoning_tokens: 0,
    }))
}

fn event_type<'v>(value: &'v Value, pointer: &str) -> Option<&'v str> {
    value.pointer(pointer).and_then(Value::as_str)
}

fn scalar_eq(expected: &Scalar, found: &Value) -> bool {
    match (expected, found) {
        (Scalar::Bool(b), Value::Bool(f)) => b == f,
        (Scalar::Integer(n), Value::Number(f)) => f.as_i64() == Some(*n),
        (Scalar::String(s), Value::String(f)) => s == f,
        _ => false,
    }
}

fn when_matches(value: &Value, when: &BTreeMap<String, Scalar>) -> bool {
    when.iter().all(|(pointer, expected)| {
        value
            .pointer(pointer)
            .is_some_and(|found| scalar_eq(expected, found))
    })
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}

/// `<status>` or `<status>/<error kind>`, from declared names only.
fn terminal_message(status: &str, error_kind: &ErrorKindSeen) -> String {
    match error_kind {
        ErrorKindSeen::Absent => status.to_string(),
        ErrorKindSeen::Declared(kind) => format!("{status}/{kind}"),
        ErrorKindSeen::Undeclared => format!("{status}/{UNDECLARED_ERROR_KIND}"),
    }
}

/// Spec "Exit check". Pure: decides the turn's result from the terminal
/// event (if any), whether any protocol line was seen, and the observed exit.
pub fn exit_check(
    spec: &ExternalSpec,
    terminal: Option<&TerminalSeen>,
    saw_protocol_line: bool,
    exit: ExitObservation,
) -> ExitVerdict {
    if exit == ExitObservation::KilledByQuorum {
        // Existing kill handling applies; any terminal event is ignored.
        return ExitVerdict::default();
    }
    let Some(terminal) = terminal else {
        return no_terminal(spec, saw_protocol_line, exit);
    };
    let failed = |message: &str, disposition, detail: &str| ExitVerdict {
        kill: false,
        event: Some(AgentEvent::TurnFailed {
            message: message.to_string(),
            usage: terminal.usage,
            cost_usd: None,
        }),
        terminal_text: None,
        failure: Some(failure(disposition, detail)),
    };
    let Some(number) = exit.number() else {
        let mut verdict = failed(
            DETAIL_NO_EXIT_AFTER_TERMINAL,
            FailureDisposition::Unclassified,
            DETAIL_NO_EXIT_AFTER_TERMINAL,
        );
        verdict.kill = true;
        return verdict;
    };
    let Some((name, status)) = terminal
        .status
        .as_deref()
        .and_then(|name| spec.statuses.get_key_value(name))
    else {
        return failed(
            UNDECLARED_TERMINAL_STATUS,
            FailureDisposition::NonFailover,
            DETAIL_UNDECLARED_STATUS,
        );
    };
    if number != i64::from(status.exit) {
        return failed(
            DETAIL_EXIT_CONTRADICTS,
            FailureDisposition::Unclassified,
            DETAIL_EXIT_CONTRADICTS,
        );
    }
    let message = terminal_message(name, &terminal.error_kind);
    match status.outcome {
        Outcome::Success => ExitVerdict {
            kill: false,
            event: Some(AgentEvent::TurnCompleted {
                usage: terminal.usage,
                cost_usd: None,
            }),
            terminal_text: terminal.final_text.clone(),
            failure: None,
        },
        Outcome::Cancelled => failed(&message, FailureDisposition::RetryableSameRoute, &message),
        Outcome::Failure => {
            let disposition = match status.disposition {
                Some(StatusDisposition::Fixed(disposition)) => disposition,
                Some(StatusDisposition::ByErrorKind) => match &terminal.error_kind {
                    ErrorKindSeen::Declared(kind) => spec
                        .error_kinds
                        .get(kind)
                        .copied()
                        .unwrap_or(FailureDisposition::NonFailover),
                    ErrorKindSeen::Absent | ErrorKindSeen::Undeclared => {
                        FailureDisposition::NonFailover
                    }
                },
                // Validation requires a disposition for the failure outcome.
                None => FailureDisposition::Unclassified,
            };
            failed(&message, disposition, &message)
        }
        // Refused by validation until the blocked outcome lands; fail closed.
        Outcome::Blocked => failed(
            &message,
            FailureDisposition::NonFailover,
            DETAIL_BLOCKED_UNSUPPORTED,
        ),
    }
}

/// No terminal event: `exits_without_terminal`, else today's rule
/// (`FailureTracker::classify_exit`, `runner.rs`).
fn no_terminal(spec: &ExternalSpec, saw_protocol_line: bool, exit: ExitObservation) -> ExitVerdict {
    let Some(number) = exit.number() else {
        return ExitVerdict {
            kill: true,
            failure: Some(failure(
                FailureDisposition::Unclassified,
                DETAIL_NO_EXIT_AFTER_TERMINAL,
            )),
            ..ExitVerdict::default()
        };
    };
    let declared = u8::try_from(number)
        .ok()
        .and_then(|code| spec.exits_without_terminal.get(&code).copied());
    let failure = match declared {
        Some(disposition) => failure(
            disposition,
            format!("external runner exited with {number} without a terminal event"),
        ),
        None if number == 0 || saw_protocol_line => failure(
            FailureDisposition::RetryableSameRoute,
            DETAIL_EOF_BEFORE_TERMINAL,
        ),
        None => failure(
            FailureDisposition::NonFailover,
            format!("external runner exited with {number} without other failure evidence"),
        ),
    };
    ExitVerdict {
        failure: Some(failure),
        ..ExitVerdict::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_runner_spec::{tests::VALID, ExternalRunnerFile};

    #[derive(serde::Deserialize)]
    struct Wrapper {
        external_runner: ExternalRunnerFile,
    }

    fn spec_from(src: &str) -> ExternalSpec {
        let file = toml::from_str::<Wrapper>(src).unwrap().external_runner;
        ExternalSpec::from_file(&file, None).unwrap()
    }

    fn spec() -> ExternalSpec {
        spec_from(VALID)
    }

    fn edited(from: &str, to: &str) -> ExternalSpec {
        assert!(VALID.contains(from), "fixture lacks {from:?}");
        spec_from(&VALID.replacen(from, to, 1))
    }

    const START: &str = r#"{"type":"run.started","format":2}"#;

    fn liveness() -> AgentEvent {
        AgentEvent::Liveness {
            summary: LIVENESS_SUMMARY.into(),
        }
    }

    fn feed<'a>(stream: &mut ExternalStream<'a>, lines: &[&str]) -> Vec<LineStep> {
        lines.iter().map(|l| stream.on_line(l.as_bytes())).collect()
    }

    fn assert_failure(f: &Option<RunnerFailure>, disposition: FailureDisposition, detail: &str) {
        let f = f.as_ref().expect("failure expected");
        assert_eq!(f.disposition(), disposition);
        assert_eq!(f.detail(), detail);
    }

    #[test]
    fn every_protocol_line_emits_liveness_and_maps_text_and_activity() {
        let spec = spec();
        let mut s = ExternalStream::new(&spec);
        let steps = feed(
            &mut s,
            &[
                START,
                r#"{"type":"item.completed","item":{"type":"message","id":"m1","text":"hello"}}"#,
                r#"{"type":"item.completed","item":{"type":"message","text":"no id"}}"#,
                r#"{"type":"item.completed","item":{"type":"reasoning","text":"hidden"}}"#,
                r#"{"type":"item.started","item":{"type":"tool_call","name":"a_very_long_tool_name_beyond_the_bound"}}"#,
                r#"{"type":"item.started","item":{"type":"tool_call"}}"#,
                r#"{"type":"unknown.event","extra":{"deep":[1,2,3]}}"#,
                r#"{"no_type":true}"#,
            ],
        );
        for step in &steps {
            assert!(step.kill.is_none());
            assert!(!step.non_protocol);
            assert_eq!(step.events[0], liveness());
        }
        assert_eq!(steps[0].events.len(), 1);
        assert_eq!(
            steps[1].events[1],
            AgentEvent::CompletedAssistantText {
                item_id: "m1".into(),
                text: "hello".into()
            }
        );
        // Absent id: the line number.
        assert_eq!(
            steps[2].events[1],
            AgentEvent::CompletedAssistantText {
                item_id: "3".into(),
                text: "no id".into()
            }
        );
        // `when` mismatch: liveness only.
        assert_eq!(steps[3].events.len(), 1);
        match &steps[4].events[1] {
            AgentEvent::Activity { kind, summary } => {
                assert_eq!(*kind, ActivityKind::ToolUse);
                assert_eq!(summary.chars().count(), ACTIVITY_SUMMARY_CHARS);
                assert!(summary.starts_with("a_very_long_tool_name") && summary.ends_with('…'));
            }
            other => panic!("expected activity, got {other:?}"),
        }
        assert_eq!(
            steps[5].events[1],
            AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary: ACTIVITY_FALLBACK_SUMMARY.into()
            }
        );
        assert_eq!(steps[6].events, vec![liveness()]);
        assert_eq!(steps[7].events, vec![liveness()]);
        assert!(s.terminal().is_none());
        assert!(s.saw_protocol_line());
    }

    #[test]
    fn non_object_lines_are_ignored_diagnostics_not_protocol_lines() {
        let spec = spec();
        let mut s = ExternalStream::new(&spec);
        for line in ["not json", "", "[1,2]", "\"str\"", "42", "{truncated"] {
            let step = s.on_line(line.as_bytes());
            assert!(step.non_protocol, "{line:?}");
            assert!(step.events.is_empty() && step.kill.is_none(), "{line:?}");
        }
        assert!(s.on_line(&[0xff, 0xfe, b'{', b'}']).non_protocol);
        assert!(!s.saw_protocol_line());
        // The start check still applies to the first *protocol* line.
        let step = s.on_line(START.as_bytes());
        assert!(step.kill.is_none());
        // A non-protocol line after the terminal is not a violation.
        s.on_line(br#"{"type":"run.completed","status":"completed"}"#);
        let step = s.on_line(b"trailing banner");
        assert!(step.non_protocol && step.kill.is_none());
        assert!(s.exit_check(ExitObservation::Code(0)).failure.is_none());
    }

    #[test]
    fn protocol_violations_kill_with_fixed_metadata_only_details() {
        struct Case {
            name: &'static str,
            lines: Vec<String>,
            disposition: FailureDisposition,
            detail: &'static str,
        }
        let long = format!(r#"{{"type":"item.completed","pad":"{}"}}"#, "x".repeat(64));
        let small = edited("max_line_bytes = 1048576", "max_line_bytes = 64");
        let secret = "SECRET-ERROR-TEXT";
        let cases = vec![
            Case {
                name: "format mismatch",
                lines: vec![format!(
                    r#"{{"type":"run.started","format":3,"msg":"{secret}"}}"#
                )],
                disposition: FailureDisposition::NonFailover,
                detail: DETAIL_UNSUPPORTED_FORMAT,
            },
            Case {
                name: "format missing",
                lines: vec![r#"{"type":"run.started"}"#.into()],
                disposition: FailureDisposition::NonFailover,
                detail: DETAIL_UNSUPPORTED_FORMAT,
            },
            Case {
                name: "format wrong type",
                lines: vec![r#"{"type":"run.started","format":"2"}"#.into()],
                disposition: FailureDisposition::NonFailover,
                detail: DETAIL_UNSUPPORTED_FORMAT,
            },
            Case {
                name: "first line is not start",
                lines: vec![r#"{"type":"item.started"}"#.into()],
                disposition: FailureDisposition::NonFailover,
                detail: DETAIL_UNSUPPORTED_FORMAT,
            },
            Case {
                name: "second terminal",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed"}"#.into(),
                    format!(r#"{{"type":"run.completed","status":"{secret}"}}"#),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_SECOND_TERMINAL,
            },
            Case {
                name: "protocol line after terminal",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed"}"#.into(),
                    format!(
                        r#"{{"type":"item.completed","item":{{"type":"message","text":"{secret}"}}}}"#
                    ),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_LINE_AFTER_TERMINAL,
            },
            Case {
                name: "usage negative",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed","usage":{"input":-1}}"#.into(),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_MALFORMED_USAGE,
            },
            Case {
                name: "usage string",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed","usage":{"output":"7"}}"#
                        .into(),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_MALFORMED_USAGE,
            },
            Case {
                name: "usage float",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed","usage":{"cache_read":1.5}}"#
                        .into(),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_MALFORMED_USAGE,
            },
            Case {
                name: "final text not a string",
                lines: vec![
                    START.into(),
                    r#"{"type":"run.completed","status":"completed","final_text":{"a":1}}"#.into(),
                ],
                disposition: FailureDisposition::Unclassified,
                detail: DETAIL_MALFORMED_FINAL_TEXT,
            },
        ];
        for case in cases {
            let spec = spec();
            let mut s = ExternalStream::new(&spec);
            let mut steps: Vec<LineStep> =
                case.lines.iter().map(|l| s.on_line(l.as_bytes())).collect();
            let last = steps.pop().unwrap();
            for earlier in &steps {
                assert!(earlier.kill.is_none(), "{}", case.name);
            }
            assert_eq!(last.events.first(), Some(&liveness()), "{}", case.name);
            assert_failure(&last.kill, case.disposition, case.detail);
            let kill = last.kill.as_ref().unwrap();
            assert!(!kill.detail().contains(secret), "{}", case.name);
            // Stopped: later lines are ignored.
            let after = s.on_line(br#"{"type":"run.completed","status":"completed"}"#);
            assert_eq!(after, LineStep::default(), "{}", case.name);
            // The kill path owns the result; any terminal is ignored.
            assert_eq!(
                s.exit_check(ExitObservation::KilledByQuorum),
                ExitVerdict::default()
            );
        }

        // Over-long line, before and after parsing would succeed.
        let mut s = ExternalStream::new(&small);
        assert!(s.on_line(START.as_bytes()).kill.is_none());
        let step = s.on_line(long.as_bytes());
        assert!(step.events.is_empty());
        assert_failure(
            &step.kill,
            FailureDisposition::Unclassified,
            DETAIL_LINE_TOO_LONG,
        );
        assert_eq!(s.on_line(START.as_bytes()), LineStep::default());
        // Exactly max_line_bytes is allowed; the terminator is not counted.
        let mut s = ExternalStream::new(&small);
        let prefix = r#"{"type":"run.started","format":2,"p":""#;
        let exact = format!("{prefix}{}\"}}", "y".repeat(64 - prefix.len() - 2));
        assert_eq!(exact.len(), 64);
        assert!(s.on_line(format!("{exact}\r\n").as_bytes()).kill.is_none());
        let mut s = ExternalStream::new(&small);
        assert_failure(
            &s.on_overlong_line().kill,
            FailureDisposition::Unclassified,
            DETAIL_LINE_TOO_LONG,
        );
    }

    #[test]
    fn without_start_any_first_line_is_accepted() {
        let spec = edited(
            "start = { type = \"run.started\", format = \"/format\", accept_formats = [2] }\n",
            "",
        );
        let mut s = ExternalStream::new(&spec);
        assert!(s.on_line(br#"{"type":"item.started"}"#).kill.is_none());
    }

    fn run(spec: &ExternalSpec, terminal: &str, exit: ExitObservation) -> ExitVerdict {
        let mut s = ExternalStream::new(spec);
        assert!(s.on_line(START.as_bytes()).kill.is_none());
        let step = s.on_line(terminal.as_bytes());
        assert!(step.kill.is_none(), "{terminal}");
        // The terminal event is withheld until the exit check.
        assert_eq!(step.events, vec![liveness()]);
        s.exit_check(exit)
    }

    fn failed_message(verdict: &ExitVerdict) -> &str {
        match verdict.event.as_ref() {
            Some(AgentEvent::TurnFailed { message, .. }) => message,
            other => panic!("expected TurnFailed, got {other:?}"),
        }
    }

    #[test]
    fn terminal_statuses_map_to_outcomes_with_declared_dispositions() {
        use FailureDisposition::*;
        let spec = spec();
        // (status, error kind JSON, exit, expected message, expected disposition)
        let cases: &[(&str, &str, i32, &str, FailureDisposition)] = &[
            ("cancelled", "null", 143, "cancelled", RetryableSameRoute),
            ("limit_reached", "null", 3, "limit_reached", NonFailover),
            ("internal_error", "null", 1, "internal_error", NonFailover),
            ("mcp_error", "null", 6, "mcp_error", RetryableSameRoute),
            (
                "provider_error",
                r#""auth""#,
                4,
                "provider_error/auth",
                ProviderUnavailable,
            ),
            (
                "provider_error",
                r#""model_unavailable""#,
                4,
                "provider_error/model_unavailable",
                ProfileUnavailable,
            ),
            (
                "provider_error",
                r#""overloaded""#,
                4,
                "provider_error/overloaded",
                RetryableSameRoute,
            ),
            (
                "provider_error",
                r#""bad_request""#,
                4,
                "provider_error/bad_request",
                NonFailover,
            ),
            (
                "provider_error",
                r#""SECRET free text""#,
                4,
                "provider_error/undeclared-error-kind",
                NonFailover,
            ),
            (
                "provider_error",
                "17",
                4,
                "provider_error/undeclared-error-kind",
                NonFailover,
            ),
            (
                "internal_error",
                r#""auth""#,
                1,
                "internal_error/auth",
                NonFailover,
            ),
        ];
        for &(status, kind, exit, message, disposition) in cases {
            let line = format!(
                r#"{{"type":"run.completed","status":"{status}","error":{{"kind":{kind},"message":"SECRET free text"}},"usage":{{"input":1,"output":2}}}}"#
            );
            let v = run(&spec, &line, ExitObservation::Code(exit));
            assert!(!v.kill);
            assert_eq!(failed_message(&v), message);
            assert_failure(&v.failure, disposition, message);
            assert!(v.terminal_text.is_none());
            assert!(!format!("{v:?}").contains("SECRET"), "{status}/{kind}");
        }
        // A by_error_kind status with no kind at all: NonFailover, fixed text.
        let line = r#"{"type":"run.completed","status":"provider_error"}"#;
        let v = run(&spec, line, ExitObservation::Code(4));
        assert_eq!(failed_message(&v), "provider_error");
        assert_failure(&v.failure, NonFailover, "provider_error");

        // Success releases TurnCompleted and the terminal text.
        let line = r#"{"type":"run.completed","status":"completed","final_text":"done","usage":{"input":10,"cache_read":5,"cache_write":2,"output":7}}"#;
        let v = run(&spec, line, ExitObservation::Code(0));
        assert_eq!(
            v,
            ExitVerdict {
                kill: false,
                event: Some(AgentEvent::TurnCompleted {
                    usage: Some(TokenUsage {
                        input_tokens: 17,
                        uncached_input_tokens: 10,
                        cached_input_tokens: 5,
                        cache_write_input_tokens: 2,
                        output_tokens: 7,
                        reasoning_tokens: 0,
                    }),
                    cost_usd: None,
                }),
                terminal_text: Some("done".into()),
                failure: None,
            }
        );
    }

    #[test]
    fn undeclared_or_non_string_status_is_non_failover_with_fixed_text() {
        let spec = spec();
        for status in [r#""SECRET-status""#, "7", "null", r#"{"a":1}"#] {
            let line = format!(r#"{{"type":"run.completed","status":{status}}}"#);
            for exit in [ExitObservation::Code(0), ExitObservation::Code(9)] {
                let v = run(&spec, &line, exit);
                assert_eq!(failed_message(&v), UNDECLARED_TERMINAL_STATUS);
                assert_failure(
                    &v.failure,
                    FailureDisposition::NonFailover,
                    DETAIL_UNDECLARED_STATUS,
                );
                assert!(!format!("{v:?}").contains("SECRET"));
            }
        }
        // A missing status pointer is undeclared too.
        let v = run(
            &spec,
            r#"{"type":"run.completed"}"#,
            ExitObservation::Code(0),
        );
        assert_failure(
            &v.failure,
            FailureDisposition::NonFailover,
            DETAIL_UNDECLARED_STATUS,
        );
    }

    #[test]
    fn usage_maps_to_token_usage_with_missing_pointers_as_zero() {
        let spec = spec();
        let v = run(
            &spec,
            r#"{"type":"run.completed","status":"completed","usage":{"output":4}}"#,
            ExitObservation::Code(0),
        );
        assert_eq!(
            v.event,
            Some(AgentEvent::TurnCompleted {
                usage: Some(TokenUsage {
                    output_tokens: 4,
                    ..TokenUsage::default()
                }),
                cost_usd: None
            })
        );
        // Huge counters saturate the input sum instead of wrapping.
        let line = format!(
            r#"{{"type":"run.completed","status":"completed","usage":{{"input":{m},"cache_read":{m}}}}}"#,
            m = u64::MAX
        );
        match run(&spec, &line, ExitObservation::Code(0)).event {
            Some(AgentEvent::TurnCompleted { usage: Some(u), .. }) => {
                assert_eq!(u.input_tokens, u64::MAX);
                assert_eq!(u.uncached_input_tokens, u64::MAX);
            }
            other => panic!("{other:?}"),
        }
        // No usage pointers declared: no usage reported.
        let spec = edited(
            "usage = { uncached_input = \"/usage/input\", cached_input = \"/usage/cache_read\", cache_write_input = \"/usage/cache_write\", output = \"/usage/output\" }\n",
            "",
        );
        let v = run(
            &spec,
            r#"{"type":"run.completed","status":"completed","usage":"x"}"#,
            ExitObservation::Code(0),
        );
        assert_eq!(
            v.event,
            Some(AgentEvent::TurnCompleted {
                usage: None,
                cost_usd: None
            })
        );
    }

    #[test]
    fn exit_check_after_terminal() {
        use FailureDisposition::*;
        let spec = spec();
        let done = r#"{"type":"run.completed","status":"completed","usage":{"output":3}}"#;
        let cancelled = r#"{"type":"run.completed","status":"cancelled"}"#;
        let usage = Some(TokenUsage {
            output_tokens: 3,
            ..TokenUsage::default()
        });

        // Contradicting exit code: Unclassified whatever the status claimed.
        for (line, exit) in [(done, 1), (done, 143), (cancelled, 0), (done, -1)] {
            let v = run(&spec, line, ExitObservation::Code(exit));
            assert!(!v.kill);
            assert_eq!(failed_message(&v), DETAIL_EXIT_CONTRADICTS);
            assert_failure(&v.failure, Unclassified, DETAIL_EXIT_CONTRADICTS);
            assert!(v.terminal_text.is_none());
        }
        // Signal death counts as 128+N: SIGTERM (15) matches `cancelled` (143).
        let v = run(&spec, cancelled, ExitObservation::Signal(15));
        assert_failure(&v.failure, RetryableSameRoute, "cancelled");
        // SIGKILL (137) contradicts `completed`.
        let v = run(&spec, done, ExitObservation::Signal(9));
        assert_failure(&v.failure, Unclassified, DETAIL_EXIT_CONTRADICTS);
        // No exit within terminal_exit_secs: kill + Unclassified; usage kept.
        let v = run(&spec, done, ExitObservation::NoExitAfterTerminal);
        assert!(v.kill);
        assert_eq!(
            v.event,
            Some(AgentEvent::TurnFailed {
                message: DETAIL_NO_EXIT_AFTER_TERMINAL.into(),
                usage,
                cost_usd: None
            })
        );
        assert_failure(&v.failure, Unclassified, DETAIL_NO_EXIT_AFTER_TERMINAL);
        // Killed by Quorum after a terminal: the terminal is ignored.
        assert_eq!(
            run(&spec, done, ExitObservation::KilledByQuorum),
            ExitVerdict::default()
        );
    }

    #[test]
    fn exit_check_without_terminal() {
        use FailureDisposition::*;
        let spec = spec();
        let check = |saw, exit| exit_check(&spec, None, saw, exit);

        // Declared exits_without_terminal entry wins, with or without lines.
        for saw in [false, true] {
            let v = check(saw, ExitObservation::Code(2));
            assert!(v.event.is_none() && !v.kill);
            assert_eq!(v.failure.as_ref().unwrap().disposition(), NonFailover);
        }
        // EOF after protocol lines, or exit 0: RetryableSameRoute (today's rule).
        assert_failure(
            &check(true, ExitObservation::Code(9)).failure,
            RetryableSameRoute,
            DETAIL_EOF_BEFORE_TERMINAL,
        );
        assert_failure(
            &check(false, ExitObservation::Code(0)).failure,
            RetryableSameRoute,
            DETAIL_EOF_BEFORE_TERMINAL,
        );
        assert_failure(
            &check(true, ExitObservation::Signal(9)).failure,
            RetryableSameRoute,
            DETAIL_EOF_BEFORE_TERMINAL,
        );
        // Nothing seen and a non-zero, undeclared exit: NonFailover.
        for exit in [
            ExitObservation::Code(9),
            ExitObservation::Signal(9),
            ExitObservation::Code(-1),
        ] {
            let v = check(false, exit);
            assert_eq!(v.failure.as_ref().unwrap().disposition(), NonFailover);
            assert!(v.event.is_none());
        }
        // A signal's 128+N can be declared too.
        let spec = edited(
            "2 = \"non_failover\"",
            "2 = \"non_failover\"\n137 = \"retryable_same_route\"",
        );
        let v = exit_check(&spec, None, false, ExitObservation::Signal(9));
        assert_eq!(
            v.failure.as_ref().unwrap().disposition(),
            RetryableSameRoute
        );
        // Killed by Quorum: existing kill handling.
        assert_eq!(
            exit_check(&spec, None, true, ExitObservation::KilledByQuorum),
            ExitVerdict::default()
        );

        // Through the stream: EOF without terminal, with and without entries.
        let spec = self::spec();
        let mut s = ExternalStream::new(&spec);
        s.on_line(START.as_bytes());
        assert_failure(
            &s.exit_check(ExitObservation::Code(1)).failure,
            RetryableSameRoute,
            DETAIL_EOF_BEFORE_TERMINAL,
        );
        assert_eq!(
            s.exit_check(ExitObservation::Code(2))
                .failure
                .unwrap()
                .disposition(),
            NonFailover
        );
    }

    #[cfg(unix)]
    #[test]
    fn exit_observation_from_status_reports_signal_as_128_plus_n() {
        use std::os::unix::process::ExitStatusExt;
        let exited = std::process::ExitStatus::from_raw(3 << 8);
        assert_eq!(
            ExitObservation::from_status(exited),
            ExitObservation::Code(3)
        );
        let signaled = std::process::ExitStatus::from_raw(15);
        assert_eq!(
            ExitObservation::from_status(signaled),
            ExitObservation::Signal(15)
        );
        assert_eq!(ExitObservation::Signal(15).number(), Some(143));
    }
}
