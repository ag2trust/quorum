//! Per-agent session log: hierarchical files (stream.jsonl, transcript.md, meta.json).
//!
//! Each agent session gets its own directory under
//! `{log_dir}/{agent}-{start_ts}[-nonce]/`. `stream.jsonl` captures raw events
//! and bounded sanitized events, `transcript.md` formats assistant output for
//! human reading, and `meta.json` identifies the session while it is active
//! and summarizes it on finalize.

// The closed sanitized API is intentionally not wired into the existing raw
// provider-log paths in this batch. Those callers change in their own scoped
// work, so keep this module's future-facing API from tripping the binary's
// dead-code gate in the meantime.
#![allow(dead_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::render;
use super::runner::AgentEvent;

/// Maximum source bytes represented by one sanitized field. The source itself
/// is never retained: this cap only bounds the structural metadata we write.
pub const MAX_SANITIZED_FIELD_BYTES: usize = 256;
/// Maximum closed sanitized-event records retained for one session.
pub const MAX_SANITIZED_RECORDS_PER_SESSION: usize = 256;
const MAX_SANITIZED_RECORD_BYTES: usize = 1024;
static LOG_DIR_NONCE: AtomicU64 = AtomicU64::new(0);

/// Explicit marker written when a source field exceeds its retained bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedTruncation {
    Truncated,
}

/// A closed provider set used in durable sanitized session events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedProvider {
    Claude,
    Codex,
    Grok,
    Other,
}

/// A provider lifecycle boundary. It intentionally has no free-text payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLifecyclePhase {
    Started,
    Ready,
    Stopped,
}

/// A turn lifecycle boundary. It intentionally has no free-text payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnLifecyclePhase {
    Started,
    Continued,
    Ended,
    Cancelled,
}

/// The structural shape of an untrusted field. Values are never serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedFieldShape {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

/// A bounded structural/redacted representation of an untrusted field.
///
/// The constructors inspect values only in memory. They retain neither text,
/// object keys, nor values, which keeps prompts, environment values,
/// credentials, and provider/tool output out of durable logs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "summary", rename_all = "snake_case", deny_unknown_fields)]
pub enum SanitizedField {
    Structural {
        shape: SanitizedFieldShape,
        captured_bytes: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        truncation: Option<SanitizedTruncation>,
    },
    Malformed {
        captured_bytes: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        truncation: Option<SanitizedTruncation>,
    },
}

impl SanitizedField {
    /// Capture only the shape and capped byte count of arbitrary text.
    pub fn from_text(value: &str) -> Self {
        Self::structural(SanitizedFieldShape::String, value.len())
    }

    /// Capture only the shape and capped serialized byte count of a JSON value.
    pub fn from_json(value: &serde_json::Value) -> Self {
        match serde_json::to_vec(value) {
            Ok(json) => Self::structural(json_shape(value), json.len()),
            // `serde_json::Value` normally serializes, but fail closed if a
            // future Value-like input cannot be represented.
            Err(_) => Self::malformed(MAX_SANITIZED_FIELD_BYTES, true),
        }
    }

    /// Capture a JSON field without retaining malformed input or parse errors.
    pub fn from_json_text(value: &str) -> Self {
        match serde_json::from_str(value) {
            Ok(json) => Self::from_json(&json),
            Err(_) => Self::malformed(
                value.len().min(MAX_SANITIZED_FIELD_BYTES),
                value.len() > MAX_SANITIZED_FIELD_BYTES,
            ),
        }
    }

    fn structural(shape: SanitizedFieldShape, bytes: usize) -> Self {
        let (captured_bytes, truncated) = bounded_byte_len(bytes);
        Self::Structural {
            shape,
            captured_bytes,
            truncation: truncated.then_some(SanitizedTruncation::Truncated),
        }
    }

    fn malformed(captured_bytes: usize, truncated: bool) -> Self {
        Self::Malformed {
            captured_bytes: captured_bytes.min(MAX_SANITIZED_FIELD_BYTES),
            truncation: truncated.then_some(SanitizedTruncation::Truncated),
        }
    }

    fn bounded(&self) -> Self {
        match self {
            Self::Structural {
                shape,
                captured_bytes,
                truncation,
            } => {
                let (captured_bytes, capped) = bounded_byte_len(*captured_bytes);
                Self::Structural {
                    shape: *shape,
                    captured_bytes,
                    truncation: (*truncation).or(capped.then_some(SanitizedTruncation::Truncated)),
                }
            }
            Self::Malformed {
                captured_bytes,
                truncation,
            } => {
                let (captured_bytes, capped) = bounded_byte_len(*captured_bytes);
                Self::Malformed {
                    captured_bytes,
                    truncation: (*truncation).or(capped.then_some(SanitizedTruncation::Truncated)),
                }
            }
        }
    }
}

fn bounded_byte_len(bytes: usize) -> (usize, bool) {
    (
        bytes.min(MAX_SANITIZED_FIELD_BYTES),
        bytes > MAX_SANITIZED_FIELD_BYTES,
    )
}

fn json_shape(value: &serde_json::Value) -> SanitizedFieldShape {
    match value {
        serde_json::Value::Null => SanitizedFieldShape::Null,
        serde_json::Value::Bool(_) => SanitizedFieldShape::Boolean,
        serde_json::Value::Number(_) => SanitizedFieldShape::Number,
        serde_json::Value::String(_) => SanitizedFieldShape::String,
        serde_json::Value::Array(_) => SanitizedFieldShape::Array,
        serde_json::Value::Object(_) => SanitizedFieldShape::Object,
    }
}

/// Closed categories for a command summary. Raw command strings are not part
/// of the durable event shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedCommandKind {
    Shell,
    Read,
    Write,
    Edit,
    Search,
    Other,
}

/// Closed categories for a tool summary. Raw tool names are not part of the
/// durable event shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedToolKind {
    Bash,
    Read,
    Write,
    Edit,
    Grep,
    Glob,
    Skill,
    Agent,
    Other,
}

/// The outcome of a bounded command or tool summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedSummaryOutcome {
    Started,
    Succeeded,
    Failed,
    Cancelled,
}

/// Terminal-response status, without retaining its response text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedTerminalStatus {
    Success,
    Error,
    Incomplete,
}

/// Closed provider-failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedProviderFailureKind {
    Authentication,
    Protocol,
    Transport,
    Timeout,
    Exit,
    Other,
}

/// Closed semantic-rejection categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedRejectionKind {
    InvalidSubmission,
    MissingSubmission,
    Policy,
    Validation,
    Other,
}

/// Closed completion outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedCompletionOutcome {
    Completed,
    Failed,
    Cancelled,
}

/// A closed, durable-safe session event.
///
/// This type deliberately permits only lifecycle categories, numeric turn
/// indices, and [`SanitizedField`] summaries. It is `deny_unknown_fields` so a
/// provider payload cannot gain a durable escape hatch through a future or
/// malformed field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum SanitizedSessionEvent {
    ProviderLifecycle {
        provider: SanitizedProvider,
        phase: ProviderLifecyclePhase,
    },
    TurnLifecycle {
        turn: u32,
        phase: TurnLifecyclePhase,
    },
    CommandSummary {
        command: SanitizedCommandKind,
        outcome: SanitizedSummaryOutcome,
        details: SanitizedField,
    },
    ToolSummary {
        tool: SanitizedToolKind,
        outcome: SanitizedSummaryOutcome,
        details: SanitizedField,
    },
    /// Assistant text turn (Codex `item.*/agent_message`, Claude `assistant`).
    /// Kept distinct from `ToolSummary` so status counters do not misreport an
    /// assistant-only turn as tool use.
    AssistantMessage {
        details: SanitizedField,
    },
    TerminalResponse {
        status: SanitizedTerminalStatus,
        response: SanitizedField,
    },
    ProviderFailure {
        provider: SanitizedProvider,
        kind: SanitizedProviderFailureKind,
        details: SanitizedField,
    },
    SemanticRejection {
        kind: SanitizedRejectionKind,
        details: SanitizedField,
    },
    Completion {
        outcome: SanitizedCompletionOutcome,
    },
}

impl SanitizedSessionEvent {
    fn bounded(&self) -> Self {
        match self {
            Self::ProviderLifecycle { provider, phase } => Self::ProviderLifecycle {
                provider: *provider,
                phase: *phase,
            },
            Self::TurnLifecycle { turn, phase } => Self::TurnLifecycle {
                turn: *turn,
                phase: *phase,
            },
            Self::CommandSummary {
                command,
                outcome,
                details,
            } => Self::CommandSummary {
                command: *command,
                outcome: *outcome,
                details: details.bounded(),
            },
            Self::ToolSummary {
                tool,
                outcome,
                details,
            } => Self::ToolSummary {
                tool: *tool,
                outcome: *outcome,
                details: details.bounded(),
            },
            Self::AssistantMessage { details } => Self::AssistantMessage {
                details: details.bounded(),
            },
            Self::TerminalResponse { status, response } => Self::TerminalResponse {
                status: *status,
                response: response.bounded(),
            },
            Self::ProviderFailure {
                provider,
                kind,
                details,
            } => Self::ProviderFailure {
                provider: *provider,
                kind: *kind,
                details: details.bounded(),
            },
            Self::SemanticRejection { kind, details } => Self::SemanticRejection {
                kind: *kind,
                details: details.bounded(),
            },
            Self::Completion { outcome } => Self::Completion { outcome: *outcome },
        }
    }

    fn summary_line(&self) -> &'static str {
        match self {
            Self::ProviderLifecycle { .. } => "- Provider lifecycle event",
            Self::TurnLifecycle { .. } => "- Turn lifecycle event",
            Self::CommandSummary { .. } => "- Command summary",
            Self::ToolSummary { .. } => "- Tool summary",
            Self::AssistantMessage { .. } => "- Assistant message",
            Self::TerminalResponse { .. } => "- Terminal response summary",
            Self::ProviderFailure { .. } => "- Provider failure summary",
            Self::SemanticRejection { .. } => "- Semantic rejection summary",
            Self::Completion { .. } => "- Session completion",
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::ProviderLifecycle { .. } => "provider_lifecycle",
            Self::TurnLifecycle { .. } => "turn_lifecycle",
            Self::CommandSummary { .. } => "command_summary",
            Self::ToolSummary { .. } => "tool_summary",
            Self::AssistantMessage { .. } => "assistant_message",
            Self::TerminalResponse { .. } => "terminal_response",
            Self::ProviderFailure { .. } => "provider_failure",
            Self::SemanticRejection { .. } => "semantic_rejection",
            Self::Completion { .. } => "completion",
        }
    }
}

pub struct SessionLog {
    dir: PathBuf,
    stream_file: File,
    transcript_file: File,
    meta: SessionMeta,
    sanitized_record_count: usize,
}

#[derive(serde::Serialize)]
struct SessionMeta {
    agent: String,
    role: String,
    task_id: Option<i64>,
    session_id: String,
    branch: String,
    start_time: i64,
    end_time: Option<i64>,
    cost_tokens: i64,
    /// Provider cost is optional. In particular, Codex's runner protocol does
    /// not supply USD, so serializing zero here would fabricate a measurement.
    cost_usd: Option<f64>,
    final_phase: String,
    verdict: Option<String>,
    rework_count: u32,
}

impl SessionLog {
    pub fn create(
        log_dir: &Path,
        agent: &str,
        role: &str,
        task_id: Option<i64>,
        session_id: &str,
        branch: &str,
        start_time: i64,
    ) -> io::Result<Self> {
        let dir = create_session_dir(log_dir, agent, start_time)?;

        let stream_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("stream.jsonl"))?;

        let mut transcript_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("transcript.md"))?;

        writeln!(transcript_file, "# Session: {agent} ({role})")?;
        if let Some(tid) = task_id {
            writeln!(transcript_file, "Task #{tid} · branch `{branch}`\n")?;
        } else {
            writeln!(transcript_file, "Branch `{branch}`\n")?;
        }

        let meta = SessionMeta {
            agent: agent.to_string(),
            role: role.to_string(),
            task_id,
            session_id: session_id.to_string(),
            branch: branch.to_string(),
            start_time,
            end_time: None,
            cost_tokens: 0,
            cost_usd: None,
            final_phase: "working".to_string(),
            verdict: None,
            rework_count: 0,
        };

        let log = SessionLog {
            dir,
            stream_file,
            transcript_file,
            meta,
            sanitized_record_count: 0,
        };
        // The task detail API verifies a run link against this metadata. Write
        // it before the daemon persists the durable run so active sessions are
        // linkable too, not only sessions that reached finalization.
        log.write_meta()?;
        Ok(log)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append one closed, durable-safe event to the regular session stream and
    /// its one-line transcript. Returns `false` after the per-session record
    /// limit has been reached; no additional provider data is then retained.
    pub fn log_sanitized_event(&mut self, event: &SanitizedSessionEvent) -> bool {
        if self.sanitized_record_count >= MAX_SANITIZED_RECORDS_PER_SESSION {
            return false;
        }

        let event = event.bounded();
        let json = match serde_json::to_vec(&event) {
            Ok(json) if json.len() <= MAX_SANITIZED_RECORD_BYTES => json,
            // Every current variant is bounded by construction. Keep a
            // defensive fixed-size record in case a future change breaks that
            // property rather than leaking a new unbounded field to disk.
            _ => format!(
                r#"{{"event":"{}","truncation":"record_truncated"}}"#,
                event.kind_name()
            )
            .into_bytes(),
        };

        let stream_result = self
            .stream_file
            .write_all(&json)
            .and_then(|_| self.stream_file.write_all(b"\n"));
        let transcript_result = writeln!(self.transcript_file, "{}", event.summary_line());
        if stream_result.is_err() || transcript_result.is_err() {
            return false;
        }

        self.sanitized_record_count += 1;
        let _ = self.transcript_file.flush();
        let _ = self.stream_file.flush();
        true
    }

    #[cfg(test)]
    pub fn log_event(&mut self, event: &super::stream::Event) {
        if let Ok(json) = serde_json::to_string(event) {
            let _ = writeln!(self.stream_file, "{json}");
        }

        if let Some(rendered) = render::render_event(event) {
            let _ = writeln!(self.transcript_file, "{rendered}");
        }

        let _ = self.transcript_file.flush();
        let _ = self.stream_file.flush();
    }

    /// Write a raw provider line verbatim to stream.jsonl, then render
    /// normalized events to transcript.md.
    pub fn log_raw_and_normalized(&mut self, raw_line: &str, events: &[AgentEvent]) {
        let _ = writeln!(self.stream_file, "{raw_line}");
        let _ = self.stream_file.flush();

        for event in events {
            if let Some(rendered) = render::render_agent_event(event) {
                let _ = writeln!(self.transcript_file, "{rendered}");
            }
        }
        let _ = self.transcript_file.flush();
    }

    pub fn set_phase(&mut self, phase: &str) {
        self.meta.final_phase = phase.to_string();
    }

    pub fn update_cost(&mut self, tokens: i64, cost_usd: Option<f64>) {
        self.meta.cost_tokens = tokens;
        self.meta.cost_usd = cost_usd;
    }

    pub fn log_rework(&mut self, round: u32) {
        self.meta.rework_count = round;
        let _ = writeln!(self.transcript_file, "\n# Rework round {round}\n");
    }

    pub fn finalize(&mut self, verdict: Option<&str>) {
        self.meta.end_time = Some(super::now_unix());
        self.meta.verdict = verdict.map(|s| s.to_string());

        let _ = self.write_meta();
    }

    fn write_meta(&self) -> io::Result<()> {
        let meta_path = self.dir.join("meta.json");
        if let Ok(json) = serde_json::to_string_pretty(&self.meta) {
            fs::write(meta_path, json)?;
        }
        Ok(())
    }
}

fn create_session_dir(log_dir: &Path, agent: &str, start_time: i64) -> io::Result<PathBuf> {
    fs::create_dir_all(log_dir)?;
    let base = format!("{agent}-{start_time}");
    let legacy_dir = log_dir.join(&base);
    match fs::create_dir(&legacy_dir) {
        Ok(()) => return Ok(legacy_dir),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }

    loop {
        let nonce = LOG_DIR_NONCE.fetch_add(1, Ordering::Relaxed);
        let dir = log_dir.join(format!("{base}-{nonce}"));
        match fs::create_dir(&dir) {
            Ok(()) => return Ok(dir),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

/// Delete session log directories older than `max_age_secs`.
pub fn sweep_logs(log_dir: &Path, max_age_secs: u64) -> io::Result<u64> {
    let mut removed = 0u64;
    let entries = match fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let meta_path = path.join("meta.json");
        let age = if meta_path.exists() {
            age_from_meta(&meta_path).unwrap_or_else(|| age_from_mtime(&path))
        } else {
            age_from_mtime(&path)
        };
        if age > max_age_secs {
            let _ = fs::remove_dir_all(&path);
            removed += 1;
        }
    }
    Ok(removed)
}

fn age_from_meta(path: &Path) -> Option<u64> {
    let data = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&data).ok()?;
    let end_time = v.get("end_time")?.as_i64()?;
    let now = super::now_unix();
    Some((now - end_time).max(0) as u64)
}

fn age_from_mtime(path: &Path) -> u64 {
    path.metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::stream::Event;
    use tempfile::TempDir;

    #[test]
    fn session_log_creates_files_and_finalizes() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path();

        let mut log = SessionLog::create(
            log_dir,
            "TestAgent",
            "worker",
            Some(42),
            "sess-1",
            "feat/test",
            1000,
        )
        .unwrap();

        assert!(log.dir().join("stream.jsonl").exists());
        assert!(log.dir().join("transcript.md").exists());
        let active_meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(log.dir().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(active_meta["agent"], "TestAgent");
        assert_eq!(active_meta["role"], "worker");
        assert_eq!(active_meta["task_id"], 42);
        assert_eq!(active_meta["start_time"], 1000);
        assert_eq!(active_meta["end_time"], serde_json::Value::Null);

        let event = Event::Assistant {
            message: serde_json::json!({"content": "Hello world"}),
        };
        log.log_event(&event);

        let tool_event = Event::ToolUse {
            name: "Bash".into(),
            input: serde_json::json!({"command": "ls"}),
        };
        log.log_event(&tool_event);

        log.update_cost(500, Some(0.05));
        log.set_phase("awaiting-review");
        log.finalize(Some("approved"));

        assert!(log.dir().join("meta.json").exists());

        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(log.dir().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(meta["agent"], "TestAgent");
        assert_eq!(meta["role"], "worker");
        assert_eq!(meta["task_id"], 42);
        assert_eq!(meta["cost_tokens"], 500);
        assert_eq!(meta["cost_usd"], 0.05);
        assert_eq!(meta["final_phase"], "awaiting-review");
        assert_eq!(meta["verdict"], "approved");

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        let lines: Vec<&str> = stream.lines().collect();
        assert_eq!(lines.len(), 2);

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("Hello world"));
        assert!(transcript.contains("> Bash:"));
    }

    #[test]
    fn session_meta_keeps_unreported_cost_null() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SessionLog::create(
            dir.path(),
            "CodexAgent",
            "worker",
            Some(7),
            "sess-codex",
            "feat/cost",
            1000,
        )
        .unwrap();

        log.update_cost(123, None);
        log.finalize(None);

        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(log.dir().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(meta["cost_tokens"], 123);
        assert_eq!(meta["cost_usd"], serde_json::Value::Null);
    }

    #[test]
    fn sanitized_events_are_closed_bounded_and_never_retain_field_values() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();
        let credential = "sk-session-log-secret-value";
        let oversized = format!("{credential}{}", "x".repeat(MAX_SANITIZED_FIELD_BYTES * 4));
        let malformed = format!(r#"{{"token":"{oversized}""#);

        let events = [
            SanitizedSessionEvent::ProviderLifecycle {
                provider: SanitizedProvider::Codex,
                phase: ProviderLifecyclePhase::Started,
            },
            SanitizedSessionEvent::TurnLifecycle {
                turn: 1,
                phase: TurnLifecyclePhase::Started,
            },
            SanitizedSessionEvent::CommandSummary {
                command: SanitizedCommandKind::Shell,
                outcome: SanitizedSummaryOutcome::Succeeded,
                details: SanitizedField::from_json(&serde_json::json!({
                    "command": oversized,
                    "api_key": credential,
                })),
            },
            SanitizedSessionEvent::ToolSummary {
                tool: SanitizedToolKind::Bash,
                outcome: SanitizedSummaryOutcome::Failed,
                details: SanitizedField::from_text(&oversized),
            },
            SanitizedSessionEvent::AssistantMessage {
                details: SanitizedField::from_text(credential),
            },
            SanitizedSessionEvent::TerminalResponse {
                status: SanitizedTerminalStatus::Success,
                response: SanitizedField::from_text(credential),
            },
            SanitizedSessionEvent::ProviderFailure {
                provider: SanitizedProvider::Codex,
                kind: SanitizedProviderFailureKind::Protocol,
                details: SanitizedField::from_json_text(&malformed),
            },
            SanitizedSessionEvent::SemanticRejection {
                kind: SanitizedRejectionKind::Validation,
                details: SanitizedField::from_json(&serde_json::json!({"env": credential})),
            },
            SanitizedSessionEvent::Completion {
                outcome: SanitizedCompletionOutcome::Completed,
            },
        ];

        for event in &events {
            assert!(log.log_sanitized_event(event));
        }

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        let meta = fs::read_to_string(log.dir().join("meta.json")).unwrap();
        for durable_file in [&stream, &transcript, &meta] {
            assert!(
                !durable_file.contains(credential),
                "secret leaked: {durable_file}"
            );
            assert!(
                !durable_file.contains("api_key"),
                "untrusted field name leaked: {durable_file}"
            );
        }

        let records: Vec<&str> = stream.lines().collect();
        assert_eq!(records.len(), events.len());
        assert!(records
            .iter()
            .all(|record| record.len() <= MAX_SANITIZED_RECORD_BYTES));
        for record in &records {
            let record: serde_json::Value = serde_json::from_str(record).unwrap();
            for field in ["details", "response"] {
                if let Some(bytes) = record
                    .get(field)
                    .and_then(|summary| summary.get("captured_bytes"))
                    .and_then(serde_json::Value::as_u64)
                {
                    assert!(bytes <= MAX_SANITIZED_FIELD_BYTES as u64);
                }
            }
        }
        assert!(stream.contains(r#""truncation":"truncated""#));
        assert!(stream.contains(r#""summary":"malformed""#));
        assert_eq!(
            transcript
                .lines()
                .filter(|line| line.starts_with("- "))
                .count(),
            events.len(),
            "each sanitized event has exactly one rendered transcript line"
        );

        assert!(serde_json::from_str::<SanitizedSessionEvent>(
            r#"{"event":"completion","outcome":"completed","provider_payload":"must-not-serialize"}"#
        )
        .is_err());
    }

    #[test]
    fn sanitized_event_record_limit_drops_later_events() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();
        let event = SanitizedSessionEvent::SemanticRejection {
            kind: SanitizedRejectionKind::Validation,
            details: SanitizedField::from_text("credential-shaped-but-not-retained"),
        };

        for index in 0..MAX_SANITIZED_RECORDS_PER_SESSION + 1 {
            assert_eq!(
                log.log_sanitized_event(&event),
                index < MAX_SANITIZED_RECORDS_PER_SESSION
            );
        }

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert_eq!(stream.lines().count(), MAX_SANITIZED_RECORDS_PER_SESSION);
        assert!(stream
            .lines()
            .all(|line| line.len() <= MAX_SANITIZED_RECORD_BYTES));
    }

    #[test]
    fn same_start_time_creates_unique_session_directories() {
        let dir = TempDir::new().unwrap();
        let first =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "first", "b", 1000).unwrap();
        let second =
            SessionLog::create(dir.path(), "Agent", "worker", Some(2), "second", "b", 1000)
                .unwrap();

        assert_ne!(first.dir(), second.dir());
        assert!(first.dir().starts_with(dir.path()));
        assert!(second.dir().starts_with(dir.path()));
        assert_eq!(first.dir().file_name().unwrap(), "Agent-1000");
        assert!(second
            .dir()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("Agent-1000-"));
    }

    #[test]
    fn sweep_logs_removes_old_directories() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path();

        // Create a "session" dir with a meta.json that has an old end_time.
        let old_dir = log_dir.join("OldAgent-100");
        fs::create_dir_all(&old_dir).unwrap();
        let old_meta = serde_json::json!({
            "agent": "OldAgent",
            "end_time": 100,
        });
        fs::write(old_dir.join("meta.json"), old_meta.to_string()).unwrap();

        // Create a "session" dir with a recent end_time.
        let new_dir = log_dir.join("NewAgent-9999999999");
        fs::create_dir_all(&new_dir).unwrap();
        let new_meta = serde_json::json!({
            "agent": "NewAgent",
            "end_time": 9999999999i64,
        });
        fs::write(new_dir.join("meta.json"), new_meta.to_string()).unwrap();

        let removed = sweep_logs(log_dir, 86400).unwrap();
        assert!(removed >= 1);
        assert!(!old_dir.exists(), "old session dir should be removed");
        assert!(new_dir.exists(), "new session dir should be kept");
    }

    #[test]
    fn sweep_logs_handles_missing_dir() {
        let dir = TempDir::new().unwrap();
        let nonexistent = dir.path().join("nope");
        let removed = sweep_logs(&nonexistent, 86400).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn result_event_appears_in_stream_and_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let result_event = Event::Result {
            result: serde_json::json!({}),
            usage: Some(super::super::stream::Usage {
                input_tokens: 200,
                output_tokens: 100,
                ..Default::default()
            }),
            total_cost_usd: Some(0.0123),
            num_turns: Some(1),
            duration_ms: Some(5000),
            is_error: Some(false),
        };
        log.log_event(&result_event);

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert_eq!(stream.lines().count(), 1);
        assert!(stream.contains("\"result\""));

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("300 tokens"));
        assert!(transcript.contains("$0.0123"));
    }

    #[test]
    fn assistant_array_content_renders_to_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let event = Event::Assistant {
            message: serde_json::json!({
                "content": [
                    {"type": "text", "text": "I'll check the code."},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/src/main.rs"}},
                ]
            }),
        };
        log.log_event(&event);

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            transcript.contains("I'll check the code."),
            "array text blocks must render: {transcript}"
        );
        assert!(
            transcript.contains("> Read: main.rs"),
            "array tool_use blocks must render: {transcript}"
        );
    }

    #[test]
    fn log_rework_writes_header() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();
        log.log_rework(2);
        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("Rework round 2"));
    }

    #[test]
    fn log_event_flushes_immediately() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let event = Event::ToolUse {
            name: "Bash".into(),
            input: serde_json::json!({"command": "cargo test"}),
        };
        log.log_event(&event);

        // Read from a separate file handle — verifies data is flushed to OS
        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            transcript.contains("> Bash:"),
            "transcript must be visible to other readers immediately after log_event"
        );
        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert!(
            stream.contains("tool_use"),
            "stream.jsonl must be visible immediately after log_event"
        );
    }

    #[test]
    fn raw_line_preserved_verbatim() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let raw = r#"{"type":"result","result":"done","extra_provider_field":true,"usage":{"input_tokens":10,"output_tokens":5}}"#;
        let events = crate::serve::runner::normalize_claude_line(raw);
        log.log_raw_and_normalized(raw, &events);

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert_eq!(
            stream.trim(),
            raw,
            "stream.jsonl must contain the raw line verbatim, not re-serialized"
        );
        assert!(
            stream.contains("extra_provider_field"),
            "extra fields must survive in stream.jsonl"
        );
    }

    #[test]
    fn normalized_event_renders_to_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let raw = r#"{"type":"assistant","message":{"content":"hello normalized"}}"#;
        let events = crate::serve::runner::normalize_claude_line(raw);
        log.log_raw_and_normalized(raw, &events);

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            transcript.contains("hello normalized"),
            "normalized text must render to transcript"
        );
    }

    #[test]
    fn unknown_event_logged_raw_no_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let raw = r#"{"type":"system","message":"init","custom":42}"#;
        let events = crate::serve::runner::normalize_claude_line(raw);
        assert!(events.is_empty());
        log.log_raw_and_normalized(raw, &events);

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert!(
            stream.contains("custom"),
            "unknown events must still appear in stream.jsonl"
        );

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            !transcript.contains("custom"),
            "unknown events must not render to transcript"
        );
    }
}
