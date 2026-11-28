//! External runner process adapter: spawns the operator-declared CLI for one
//! turn, feeds its stdout through the [`ExternalStream`] state machine, and
//! applies the exit check.
//!
//! Contract: `docs/2026-11-20-external-cli-runner-technical-spec.md`,
//! "Launch", "Stream mapping", "Terminal event", and "Exit check".
//!
//! - The executable is started directly (no shell) with the argv and layered
//!   environment from [`external_launch`]; the prompt is written to stdin,
//!   which is then closed.
//! - stdout is read one bounded line at a time. A protocol violation kills
//!   the process tree. Once the terminal event is seen, [`ExternalProc::next_raw_line`]
//!   waits at most `terminal_exit_secs` for the process to exit and for stdout
//!   to reach EOF, then kills it. Lines still unread when the exit is
//!   observed go through the stream before the exit check.
//! - stderr is read only to keep the pipe drained and is retained as line and
//!   byte counts. Its text is never classified and never stored.
//!
//! Metadata-only records: [`ExternalProc::next_raw_line`] never returns a raw
//! CLI line. It returns one `quorum.external` JSON record per line that carries
//! the normalized events, the record kind, and a fixed or declared-name failure
//! detail. [`normalize_line`] decodes that record, so the stream log holds no
//! text from the CLI's error fields.

use super::external_launch;
use super::external_stream::{ExitObservation, ExternalStream, LineStep};
use super::proctree;
use super::runner::{
    gated_command, ActivityKind, AdapterConfig, AgentEvent, AgentKind, CapturedOutput,
    FailureDisposition, FailureTracker, LaunchGate, LaunchRequest, ManagedRunEnv, NormalizedLine,
    RunnerFailure, TokenUsage, EXIT_EVIDENCE_TIMEOUT,
};
use crate::external_runner_spec::{AccessLevel, ExternalSpec};
use quorum_core::tasks::RunnerBlocked;
use serde_json::{json, Value};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

/// `type` of every record [`ExternalProc::next_raw_line`] returns.
pub const RECORD_TYPE: &str = "quorum.external";

const RECORD_PROTOCOL: &str = "protocol";
const RECORD_NON_PROTOCOL: &str = "non_protocol";
const RECORD_VIOLATION: &str = "violation";
const RECORD_EXIT_CHECK: &str = "exit_check";

pub const DETAIL_NO_SPEC: &str =
    "external runner launch refused: no [external_runner] declaration is configured";
pub const DETAIL_NO_EXECUTABLE: &str =
    "external runner launch refused: neither agent_bin nor external_runner.executable is set";
pub const DETAIL_CONTINUATION: &str =
    "external runner launch refused: the external runner has no provider continuation";

fn refusal(detail: &'static str) -> RunnerFailure {
    RunnerFailure::classified(
        FailureDisposition::NonFailover,
        detail,
        std::io::ErrorKind::InvalidInput,
    )
}

#[derive(Default)]
struct StderrCounts {
    lines: usize,
    bytes: usize,
}

/// One bounded stdout read.
enum ReadLine {
    Line(Vec<u8>),
    /// The line exceeded the bound; nothing beyond the bound was buffered.
    Overlong,
    Eof,
}

/// Read one line into `buf`, keeping at most `max + 1` bytes (the extra byte
/// is a possible `\r` before `\n`; [`ExternalStream::on_line`] applies the
/// exact bound). Cancellation-safe: partial bytes stay in `buf` and
/// `fill_buf` is cancellation-safe.
async fn read_line(
    reader: &mut BufReader<ChildStdout>,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<ReadLine> {
    loop {
        let (consumed, outcome) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                if buf.is_empty() {
                    return Ok(ReadLine::Eof);
                }
                return Ok(ReadLine::Line(std::mem::take(buf)));
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let chunk = &available[..newline.unwrap_or(available.len())];
            let consumed = chunk.len() + usize::from(newline.is_some());
            if buf.len().saturating_add(chunk.len()) > max.saturating_add(1) {
                *buf = Vec::new();
                (consumed, Some(ReadLine::Overlong))
            } else {
                buf.extend_from_slice(chunk);
                (
                    consumed,
                    newline.map(|_| ReadLine::Line(std::mem::take(buf))),
                )
            }
        };
        reader.consume(consumed);
        if let Some(outcome) = outcome {
            return Ok(outcome);
        }
    }
}

async fn count_stderr(
    mut stderr: tokio::process::ChildStderr,
    counts: Arc<Mutex<StderrCounts>>,
    failures: FailureTracker,
) {
    let mut chunk = [0u8; 4096];
    let mut partial = false;
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => {
                let newlines = chunk[..read].iter().filter(|b| **b == b'\n').count();
                partial = chunk[read - 1] != b'\n';
                let mut counts = counts.lock().expect("stderr counts poisoned");
                counts.bytes = counts.bytes.saturating_add(read);
                counts.lines = counts.lines.saturating_add(newlines);
            }
            Err(error) => {
                failures.note_incomplete_evidence(format!(
                    "external runner stderr evidence could not be read: {}",
                    error.kind()
                ));
                break;
            }
        }
    }
    if partial {
        let mut counts = counts.lock().expect("stderr counts poisoned");
        counts.lines = counts.lines.saturating_add(1);
    }
}

/// One spawned external CLI turn.
pub struct ExternalProc {
    child: Child,
    process_group_id: libc::pid_t,
    process_group_reaped: bool,
    tree: proctree::TreeHandle,
    reader: BufReader<ChildStdout>,
    line_buffer: Vec<u8>,
    max_line: usize,
    terminal_exit: std::time::Duration,
    stream: ExternalStream<Arc<ExternalSpec>>,
    /// Set when the terminal event is accepted.
    terminal_deadline: Option<tokio::time::Instant>,
    stdout_eof: bool,
    /// Exit observed after the terminal event; the exit check waits for the
    /// stdout drain.
    exited: Option<std::process::ExitStatus>,
    /// No more records: a violation stopped the run, the exit check ran, or
    /// stdout ended without a terminal event.
    finished: bool,
    /// The exit check (or a violation) decided this turn's result.
    decided: bool,
    killed_by_quorum: bool,
    /// The blocked outcome released by the exit check (read-write runs only).
    blocked: Option<RunnerBlocked>,
    failures: FailureTracker,
    stderr: Arc<Mutex<StderrCounts>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    _stdin_task: tokio::task::JoinHandle<()>,
    _run_env: ManagedRunEnv,
}

/// Spec, program, and argv for one launch.
fn prepare(
    request: &LaunchRequest<'_>,
    config: &AdapterConfig<'_>,
) -> Result<(Arc<ExternalSpec>, String, Vec<String>), RunnerFailure> {
    let spec = config.external.ok_or_else(|| refusal(DETAIL_NO_SPEC))?;
    if request.continuation_id.is_some() {
        return Err(refusal(DETAIL_CONTINUATION));
    }
    // Spec "agent_bin": the effective agent_bin replaces the executable.
    let program = config
        .executable
        .or(spec.executable.as_deref())
        .ok_or_else(|| refusal(DETAIL_NO_EXECUTABLE))?
        .to_string();
    let argv = external_launch::argv(spec, request)?;
    Ok((Arc::new(spec.clone()), program, argv))
}

impl ExternalProc {
    pub fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        let (spec, program, argv) = prepare(request, config)?;
        let mut command = Command::new(&program);
        command.args(argv);
        Self::spawn(command, spec, request)
    }

    pub fn launch_gated(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<(Self, LaunchGate)> {
        let (spec, program, argv) = prepare(request, config)?;
        let (mut command, gate) = gated_command(AgentKind::External, &program)?;
        command.args(argv);
        let proc = Self::spawn(command, spec, request)?;
        let gate = gate.bind(proc.process_group_id);
        Ok((proc, gate))
    }

    fn spawn(
        mut command: Command,
        spec: Arc<ExternalSpec>,
        request: &LaunchRequest<'_>,
    ) -> std::io::Result<Self> {
        let run_env = ManagedRunEnv::create()?;
        external_launch::configure_environment(&mut command, &spec, request, &run_env)?;
        command
            .current_dir(request.worktree)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut tree = proctree::apply(&mut command);
        let mut child = command.spawn()?;
        let process_group_id = child.id().ok_or_else(|| {
            std::io::Error::other("spawned external runner has no process-group ID")
        })? as libc::pid_t;
        tree.bind_leader(process_group_id as u32);

        // A background writer cannot deadlock the launch on pipe capacity;
        // dropping the handle at the end closes stdin. A write error means
        // the CLI exited early, which the exit check reports.
        let mut stdin = child.stdin.take().expect("stdin was piped");
        let prompt = request.prompt.to_string();
        let stdin_task = tokio::spawn(async move {
            let _ = stdin.write_all(prompt.as_bytes()).await;
            let _ = stdin.shutdown().await;
        });

        let failures = FailureTracker::for_kind(AgentKind::External);
        let counts = Arc::new(Mutex::new(StderrCounts::default()));
        let stderr = child.stderr.take().expect("stderr was piped");
        let stderr_task = tokio::spawn(count_stderr(stderr, counts.clone(), failures.clone()));
        let reader = BufReader::new(child.stdout.take().expect("stdout was piped"));
        let max_line = usize::try_from(spec.stream.max_line_bytes).unwrap_or(usize::MAX);
        let terminal_exit = std::time::Duration::from_secs(spec.stream.terminal_exit_secs);
        let read_write = external_launch::access_level(request.managed.map(|m| m.role))
            == AccessLevel::ReadWrite;
        Ok(Self {
            child,
            process_group_id,
            process_group_reaped: false,
            tree,
            reader,
            line_buffer: Vec::new(),
            max_line,
            terminal_exit,
            stream: ExternalStream::new(spec).allowing_blocked(read_write),
            terminal_deadline: None,
            stdout_eof: false,
            exited: None,
            finished: false,
            decided: false,
            killed_by_quorum: false,
            blocked: None,
            failures,
            stderr: counts,
            stderr_task: Some(stderr_task),
            _stdin_task: stdin_task,
            _run_env: run_env,
        })
    }

    /// Next metadata-only record, or `None` once the turn has no more.
    ///
    /// After the terminal event this waits for the process to exit, at most
    /// `terminal_exit_secs` from the terminal event, and then returns the
    /// exit-check record carrying the turn's single terminal event. Callers
    /// therefore read until `None` before treating the turn as finished.
    pub async fn next_raw_line(&mut self) -> Option<String> {
        enum Next {
            Read(std::io::Result<ReadLine>),
            Exited(std::io::Result<std::process::ExitStatus>),
            Deadline,
        }
        loop {
            if self.finished {
                return None;
            }
            // Stream mapping §6: the exit check runs only once stdout has
            // been drained to EOF, so a trailing protocol line written just
            // before a prompt exit is still a violation.
            if let (Some(status), true) = (self.exited, self.stdout_eof) {
                return self.finish_exit_check(ExitObservation::from_status(status));
            }
            let next = match self.terminal_deadline {
                None => Next::Read(
                    read_line(&mut self.reader, &mut self.line_buffer, self.max_line).await,
                ),
                // `Child::wait`, `read_line`, and `sleep_until` are all
                // cancellation-safe, so the losing branches lose no state.
                // After the exit, only the stdout drain and the remaining
                // `terminal_exit_secs` deadline race.
                Some(deadline) => tokio::select! {
                    biased;
                    status = self.child.wait(), if self.exited.is_none() => Next::Exited(status),
                    read = read_line(&mut self.reader, &mut self.line_buffer, self.max_line),
                        if !self.stdout_eof => Next::Read(read),
                    () = tokio::time::sleep_until(deadline) => Next::Deadline,
                },
            };
            match next {
                Next::Read(Ok(ReadLine::Eof)) => {
                    self.stdout_eof = true;
                    if self.terminal_deadline.is_none() {
                        // No terminal event: the caller observes the exit and
                        // `classify_pre_authoritative_exit` applies the check.
                        self.finished = true;
                        return None;
                    }
                }
                Next::Read(Ok(ReadLine::Line(bytes))) => {
                    let step = self.stream.on_line(&bytes);
                    return Some(self.apply_step(step));
                }
                Next::Read(Ok(ReadLine::Overlong)) => {
                    let step = self.stream.on_overlong_line();
                    return Some(self.apply_step(step));
                }
                Next::Read(Err(error)) => {
                    self.failures.observe_protocol_read_error(format!(
                        "external runner stdout could not be read: {}",
                        error.kind()
                    ));
                    self.decided = true;
                    self.finished = true;
                    self.kill_tree();
                    return None;
                }
                Next::Exited(Ok(status)) => self.exited = Some(status),
                Next::Exited(Err(error)) => {
                    self.failures.note_incomplete_evidence(format!(
                        "external runner exit status could not be read: {}",
                        error.kind()
                    ));
                    self.finished = true;
                    return None;
                }
                // Also reached when the process exited but stdout stayed open
                // (a descendant holds it): the stream did not end within the
                // bound, so the fail-safe result is the same kill.
                Next::Deadline => {
                    return self.finish_exit_check(ExitObservation::NoExitAfterTerminal)
                }
            }
        }
    }

    /// [`Self::next_raw_line`] for callers with a byte allowance. The CLI's
    /// own lines are already bounded by `max_line_bytes`; this bounds the
    /// record handed to the caller.
    pub async fn next_raw_line_bounded(
        &mut self,
        max_bytes: usize,
    ) -> std::io::Result<Option<String>> {
        match self.next_raw_line().await {
            Some(record) if record.len() > max_bytes => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("external runner record exceeded the {max_bytes}-byte read limit"),
            )),
            record => Ok(record),
        }
    }

    fn apply_step(&mut self, step: LineStep) -> String {
        if self.terminal_deadline.is_none() && self.stream.terminal().is_some() {
            self.terminal_deadline = Some(tokio::time::Instant::now() + self.terminal_exit);
        }
        let kind = if step.kill.is_some() {
            RECORD_VIOLATION
        } else if step.non_protocol {
            RECORD_NON_PROTOCOL
        } else {
            RECORD_PROTOCOL
        };
        if let Some(failure) = &step.kill {
            self.kill_tree();
            self.failures.record_decided_failure(failure.clone());
            self.decided = true;
            self.finished = true;
        }
        encode(kind, &step.events, None, step.kill.as_ref())
    }

    fn finish_exit_check(&mut self, exit: ExitObservation) -> Option<String> {
        self.finished = true;
        if self.decided {
            return None;
        }
        let exit = if self.killed_by_quorum {
            ExitObservation::KilledByQuorum
        } else {
            exit
        };
        let verdict = self.stream.exit_check(exit);
        if verdict.kill {
            self.kill_tree();
        }
        self.decided = true;
        match (&verdict.failure, &verdict.event) {
            (Some(failure), _) => self.failures.record_decided_failure(failure.clone()),
            (None, Some(AgentEvent::TurnCompleted { .. })) => {
                self.failures.record_terminal_success()
            }
            // A blocked outcome is a declared, exit-checked terminal: no
            // runner failure. The lifecycle parks from the retained question.
            (
                None,
                Some(AgentEvent::TurnBlocked {
                    question, context, ..
                }),
            ) => {
                self.failures.record_terminal_success();
                self.blocked = Some(RunnerBlocked {
                    question: question.clone(),
                    context: context.clone(),
                });
            }
            (None, _) => {}
        }
        if verdict.event.is_none() && verdict.failure.is_none() {
            return None;
        }
        Some(encode(
            RECORD_EXIT_CHECK,
            verdict.event.as_slice(),
            verdict.terminal_text.as_deref(),
            verdict.failure.as_ref(),
        ))
    }

    /// Kill the whole tree now. Every later exit check takes the
    /// killed-by-Quorum path.
    pub fn kill_tree(&mut self) {
        self.killed_by_quorum = true;
        proctree::kill_tree_now(&self.tree, !self.process_group_reaped);
    }

    /// Spec "Exit check" for an exited process: a decided result wins; a run
    /// Quorum killed has no runner failure of its own; otherwise the check
    /// runs on `status` (the caller observed the exit before draining
    /// records).
    pub fn classify_pre_authoritative_exit(
        &self,
        status: std::process::ExitStatus,
    ) -> Option<RunnerFailure> {
        if let Some(failure) = self.failures.observed_failure() {
            return Some(failure);
        }
        if self.decided || self.killed_by_quorum {
            return None;
        }
        self.stream
            .exit_check(ExitObservation::from_status(status))
            .failure
    }

    pub fn observed_pre_authoritative_failure(&self) -> Option<RunnerFailure> {
        self.failures.observed_failure()
    }

    /// Whether this run wrote at least one protocol line (spec "Resume or
    /// fresh start": a resume that ends before any marks its session unusable).
    pub fn saw_protocol_line(&self) -> bool {
        self.stream.saw_protocol_line()
    }

    /// The turn's blocked outcome, once the exit check released it.
    pub fn runner_blocked(&self) -> Option<&RunnerBlocked> {
        self.blocked.as_ref()
    }

    pub(super) fn failure_tracker(&self) -> FailureTracker {
        self.failures.clone()
    }

    pub(super) async fn finish_stderr_until(&mut self, deadline: tokio::time::Instant) -> bool {
        let Some(mut task) = self.stderr_task.take() else {
            return true;
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                task.abort();
                let _ = task.await;
                false
            }
        }
    }

    /// stderr evidence as counts only; resets the counts.
    pub fn drain_diagnostics(&mut self) -> Vec<CapturedOutput> {
        let mut counts = self.stderr.lock().expect("stderr counts poisoned");
        let StderrCounts { lines, bytes } = std::mem::take(&mut *counts);
        if lines == 0 && bytes == 0 {
            return Vec::new();
        }
        vec![CapturedOutput::StderrWithheld { lines, bytes }]
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }

    pub fn process_group_id(&self) -> i32 {
        self.process_group_id
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub async fn kill_and_reap(mut self) -> Vec<CapturedOutput> {
        self.kill_tree();
        if self.child.wait().await.is_ok() {
            self.process_group_reaped = true;
        }
        let _ = proctree::reap_tree(
            &self.tree,
            tokio::time::Instant::now() + proctree::TREE_REAP_TIMEOUT,
        )
        .await;
        self.finish_stderr_until(tokio::time::Instant::now() + EXIT_EVIDENCE_TIMEOUT)
            .await;
        self.drain_diagnostics()
    }
}

impl Drop for ExternalProc {
    fn drop(&mut self) {
        if !self.process_group_reaped {
            proctree::kill_tree_now(&self.tree, true);
        }
    }
}

fn usage_json(usage: &TokenUsage) -> Value {
    json!({
        "input": usage.input_tokens,
        "uncached_input": usage.uncached_input_tokens,
        "cached_input": usage.cached_input_tokens,
        "cache_write_input": usage.cache_write_input_tokens,
        "output": usage.output_tokens,
        "reasoning": usage.reasoning_tokens,
    })
}

fn usage_from(value: Option<&Value>) -> Option<TokenUsage> {
    let value = value.filter(|v| v.is_object())?;
    let field = |name: &str| value.get(name).and_then(Value::as_u64).unwrap_or(0);
    Some(TokenUsage {
        input_tokens: field("input"),
        uncached_input_tokens: field("uncached_input"),
        cached_input_tokens: field("cached_input"),
        cache_write_input_tokens: field("cache_write_input"),
        output_tokens: field("output"),
        reasoning_tokens: field("reasoning"),
    })
}

/// Build one record. Only events the external stream produces are encoded.
fn encode(
    kind: &str,
    events: &[AgentEvent],
    terminal_text: Option<&str>,
    failure: Option<&RunnerFailure>,
) -> String {
    let events: Vec<Value> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Liveness { summary } => {
                Some(json!({"event": "liveness", "summary": summary}))
            }
            AgentEvent::CompletedAssistantText { item_id, text } => {
                Some(json!({"event": "text", "item_id": item_id, "text": text}))
            }
            AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary,
            } => Some(json!({"event": "activity", "summary": summary})),
            AgentEvent::TurnCompleted { usage, .. } => {
                Some(json!({"event": "turn_completed", "usage": usage.as_ref().map(usage_json)}))
            }
            AgentEvent::TurnFailed { message, usage, .. } => Some(json!({
                "event": "turn_failed",
                "message": message,
                "usage": usage.as_ref().map(usage_json),
            })),
            // Metadata only: the question and its context are kept on the
            // process ([`ExternalProc::runner_blocked`]) and stored once, in
            // the park note, never in the stream log.
            AgentEvent::TurnBlocked { usage, .. } => Some(json!({
                "event": "turn_blocked",
                "usage": usage.as_ref().map(usage_json),
            })),
            _ => None,
        })
        .collect();
    let mut record = json!({"type": RECORD_TYPE, "record": kind, "events": events});
    if let Some(text) = terminal_text {
        record["terminal_text"] = json!(text);
    }
    if let Some(failure) = failure {
        // Details are fixed strings or declared names (external_stream.rs).
        record["disposition"] = json!(failure.disposition().to_string());
        record["detail"] = json!(failure.detail());
    }
    record.to_string()
}

fn decode_event(value: &Value) -> Option<AgentEvent> {
    let text = |name: &str| value.get(name).and_then(Value::as_str).map(str::to_string);
    match value.get("event").and_then(Value::as_str)? {
        "liveness" => Some(AgentEvent::Liveness {
            summary: text("summary")?,
        }),
        "text" => Some(AgentEvent::CompletedAssistantText {
            item_id: text("item_id")?,
            text: text("text")?,
        }),
        "activity" => Some(AgentEvent::Activity {
            kind: ActivityKind::ToolUse,
            summary: text("summary")?,
        }),
        "turn_completed" => Some(AgentEvent::TurnCompleted {
            usage: usage_from(value.get("usage")),
            cost_usd: None,
        }),
        "turn_failed" => Some(AgentEvent::TurnFailed {
            message: text("message")?,
            usage: usage_from(value.get("usage")),
            cost_usd: None,
        }),
        // The record carries no question text (see `encode`).
        "turn_blocked" => Some(AgentEvent::TurnBlocked {
            question: String::new(),
            context: Vec::new(),
            usage: usage_from(value.get("usage")),
        }),
        _ => None,
    }
}

/// Decode a record returned by [`ExternalProc::next_raw_line`]. Anything else
/// normalizes to nothing.
pub fn normalize_line(raw: &str) -> NormalizedLine {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return NormalizedLine {
            events: Vec::new(),
            terminal_text: None,
        };
    };
    if value.get("type").and_then(Value::as_str) != Some(RECORD_TYPE) {
        return NormalizedLine {
            events: Vec::new(),
            terminal_text: None,
        };
    }
    NormalizedLine {
        events: value
            .get("events")
            .and_then(Value::as_array)
            .map(|events| events.iter().filter_map(decode_event).collect())
            .unwrap_or_default(),
        terminal_text: value
            .get("terminal_text")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_runner_spec::{
        tests::{with_session, VALID},
        ArgPart, ArgTemplate, EnvSource, ExternalRunnerFile, Placeholder,
    };
    use crate::serve::external_launch::SESSION_DIR_ENV;
    use crate::serve::external_stream::{
        DETAIL_EOF_BEFORE_TERMINAL, DETAIL_EXIT_CONTRADICTS, DETAIL_LINE_AFTER_TERMINAL,
        DETAIL_LINE_TOO_LONG, DETAIL_NO_EXIT_AFTER_TERMINAL, DETAIL_SECOND_TERMINAL,
        DETAIL_UNDECLARED_STATUS, DETAIL_UNSUPPORTED_FORMAT,
    };
    use crate::serve::runner::{
        assert_run_private_gh_config, check_agent_mcp_wiring, AgentMcpRole, LaunchMode,
        ManagedLaunch, RunnerProc, AGENT_MCP_URL_ENV,
    };
    use std::path::Path;
    use std::time::Duration;

    const FAKE_CLI: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/serve/testdata/external/fake_cli.sh"
    );
    const URL: &str = "http://127.0.0.1:4100/mcp";
    const PROMPT: &str = "PROMPT-TEXT line one\nline two";

    #[derive(serde::Deserialize)]
    struct Wrapper {
        external_runner: ExternalRunnerFile,
    }

    /// The spec example pointed at the fake CLI, with a small line bound and
    /// a one-second terminal exit wait. `executable = false` drops the
    /// declared executable so the fake CLI must come from `agent_bin`.
    fn spec(executable: bool) -> ExternalSpec {
        spec_from_src(VALID, executable)
    }

    /// [`spec`] built from `base` instead of the spec example.
    fn spec_from_src(base: &str, executable: bool) -> ExternalSpec {
        let mut src = base
            .replacen("/usr/local/bin/fake-cli", FAKE_CLI, 1)
            .replacen(
                "max_line_bytes = 1048576",
                "max_line_bytes = 256\nterminal_exit_secs = 1",
                1,
            );
        assert!(src.contains(FAKE_CLI) && src.contains("terminal_exit_secs"));
        if !executable {
            src = src.replacen(&format!("executable = \"{FAKE_CLI}\"\n"), "", 1);
            assert!(!src.contains(FAKE_CLI));
        }
        let file = toml::from_str::<Wrapper>(&src).unwrap().external_runner;
        ExternalSpec::from_file(&file, None).unwrap()
    }

    fn config<'a>(spec: Option<&'a ExternalSpec>, agent_bin: Option<&'a str>) -> AdapterConfig<'a> {
        AdapterConfig {
            executable: agent_bin,
            claude_bare: false,
            claude_allowed_tools: "",
            codex_sandbox: "read-only",
            grok: Default::default(),
            external: spec,
        }
    }

    fn request<'a>(
        worktree: &'a Path,
        environment: &'a [(String, String)],
        managed: Option<ManagedLaunch<'a>>,
    ) -> LaunchRequest<'a> {
        LaunchRequest {
            runner: AgentKind::External,
            model: "m-1",
            effort: "high",
            worktree,
            prompt: PROMPT,
            environment,
            mode: LaunchMode::Normal,
            continuation_id: None,
            managed,
        }
    }

    fn scenario_env(scenario: &str) -> Vec<(String, String)> {
        vec![("FAKE_CLI_SCENARIO".into(), scenario.into())]
    }

    struct Run {
        records: Vec<String>,
        events: Vec<AgentEvent>,
        terminal_text: Option<String>,
        outcome: Option<RunnerFailure>,
        evidence: Vec<CapturedOutput>,
    }

    impl Run {
        fn terminal_events(&self) -> Vec<&AgentEvent> {
            self.events
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
                    )
                })
                .collect()
        }
    }

    async fn wait_exit(proc: &mut RunnerProc) -> std::process::ExitStatus {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = proc.try_wait().unwrap() {
                return status;
            }
            assert!(tokio::time::Instant::now() < deadline, "no exit");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Drive one turn the way a lifecycle consumer does: read records until
    /// `None`, observe the exit, finalize evidence, and classify.
    async fn drive(mut proc: RunnerProc) -> Run {
        let mut records = Vec::new();
        let mut events = Vec::new();
        let mut terminal_text = None;
        while let Some(raw) = tokio::time::timeout(Duration::from_secs(20), proc.next_raw_line())
            .await
            .expect("record within bound")
        {
            let line = proc.normalize_line(&raw);
            assert_eq!(proc.normalize_stream_line(&raw), line.events);
            events.extend(line.events);
            terminal_text = line.terminal_text.or(terminal_text);
            records.push(raw);
        }
        let status = wait_exit(&mut proc).await;
        let evidence = proc.finalize_pre_authoritative_evidence().await;
        let outcome = proc.classify_pre_authoritative_exit(status);
        assert_eq!(proc.kind(), AgentKind::External);
        Run {
            records,
            events,
            terminal_text,
            outcome,
            evidence,
        }
    }

    async fn launch_scenario(scenario: &str, via_agent_bin: bool) -> Run {
        let dir = tempfile::tempdir().unwrap();
        let spec = spec(!via_agent_bin);
        let env = scenario_env(scenario);
        let agent_bin = via_agent_bin.then_some(FAKE_CLI);
        let proc = RunnerProc::launch(
            &request(dir.path(), &env, None),
            &config(Some(&spec), agent_bin),
        )
        .await
        .unwrap_or_else(|error| panic!("{scenario}: launch failed: {error}"));
        drive(proc).await
    }

    /// A terminal without usage values: the declared pointers are missing,
    /// so each count is 0.
    fn failed(message: &str) -> Option<AgentEvent> {
        Some(AgentEvent::TurnFailed {
            message: message.into(),
            usage: Some(TokenUsage::default()),
            cost_usd: None,
        })
    }

    /// (scenario, expected outcome, expected single terminal event)
    type Case = (
        &'static str,
        Option<(FailureDisposition, &'static str)>,
        Option<AgentEvent>,
    );

    #[tokio::test]
    async fn fake_cli_scenarios_yield_spec_dispositions() {
        use FailureDisposition::*;
        let success_usage = TokenUsage {
            input_tokens: 13,
            uncached_input_tokens: 10,
            cached_input_tokens: 2,
            cache_write_input_tokens: 1,
            output_tokens: 5,
            reasoning_tokens: 0,
        };
        let completed = Some(AgentEvent::TurnCompleted {
            usage: Some(success_usage),
            cost_usd: None,
        });
        // (scenario, expected outcome, expected single terminal event)
        let cases: Vec<Case> = vec![
            ("success", None, completed.clone()),
            ("non_json", None, completed.clone()),
            ("overlong", Some((Unclassified, DETAIL_LINE_TOO_LONG)), None),
            (
                "format_mismatch",
                Some((NonFailover, DETAIL_UNSUPPORTED_FORMAT)),
                None,
            ),
            (
                "second_terminal",
                Some((Unclassified, DETAIL_SECOND_TERMINAL)),
                None,
            ),
            (
                "line_after_terminal",
                Some((Unclassified, DETAIL_LINE_AFTER_TERMINAL)),
                None,
            ),
            // The exit is observed before the trailing line is read; stdout
            // is still drained through the stream before the exit check.
            (
                "second_terminal_then_exit",
                Some((Unclassified, DETAIL_SECOND_TERMINAL)),
                None,
            ),
            (
                "line_after_terminal_then_exit",
                Some((Unclassified, DETAIL_LINE_AFTER_TERMINAL)),
                None,
            ),
            // A non-protocol line after the terminal is not a violation.
            ("non_json_after_terminal_then_exit", None, completed.clone()),
            (
                "undeclared_status",
                Some((NonFailover, DETAIL_UNDECLARED_STATUS)),
                failed("undeclared-terminal-status"),
            ),
            (
                "undeclared_error_kind",
                Some((NonFailover, "provider_error/undeclared-error-kind")),
                failed("provider_error/undeclared-error-kind"),
            ),
            (
                "declared_error_kind",
                Some((ProviderUnavailable, "provider_error/auth")),
                failed("provider_error/auth"),
            ),
            (
                "contradicting_exit",
                Some((Unclassified, DETAIL_EXIT_CONTRADICTS)),
                Some(AgentEvent::TurnFailed {
                    message: DETAIL_EXIT_CONTRADICTS.into(),
                    usage: Some(success_usage),
                    cost_usd: None,
                }),
            ),
            // SIGTERM counts as exit 128+15, the declared `cancelled` exit.
            (
                "signal_death",
                Some((RetryableSameRoute, "cancelled")),
                failed("cancelled"),
            ),
            (
                "no_exit_after_terminal",
                Some((Unclassified, DETAIL_NO_EXIT_AFTER_TERMINAL)),
                Some(AgentEvent::TurnFailed {
                    message: DETAIL_NO_EXIT_AFTER_TERMINAL.into(),
                    usage: Some(success_usage),
                    cost_usd: None,
                }),
            ),
            // The CLI exits but stdout never reaches EOF within the bound.
            (
                "stdout_held_after_exit",
                Some((Unclassified, DETAIL_NO_EXIT_AFTER_TERMINAL)),
                Some(AgentEvent::TurnFailed {
                    message: DETAIL_NO_EXIT_AFTER_TERMINAL.into(),
                    usage: Some(success_usage),
                    cost_usd: None,
                }),
            ),
            (
                "eof_without_terminal",
                Some((RetryableSameRoute, DETAIL_EOF_BEFORE_TERMINAL)),
                None,
            ),
            (
                "exit_without_terminal",
                Some((
                    NonFailover,
                    "external runner exited with 2 without a terminal event",
                )),
                None,
            ),
        ];
        for via_agent_bin in [false, true] {
            for (scenario, want_outcome, want_terminal) in &cases {
                let label = format!("{scenario} (agent_bin: {via_agent_bin})");
                let run = launch_scenario(scenario, via_agent_bin).await;
                let got = run
                    .outcome
                    .as_ref()
                    .map(|f| (f.disposition(), f.detail().to_string()));
                assert_eq!(
                    got,
                    want_outcome.map(|(d, detail)| (d, detail.to_string())),
                    "{label}"
                );
                let terminal = run.terminal_events();
                assert_eq!(
                    terminal,
                    want_terminal.iter().collect::<Vec<_>>(),
                    "{label}: exactly the spec's terminal event"
                );
                if !matches!(*scenario, "exit_without_terminal") {
                    assert!(
                        matches!(run.events.first(), Some(AgentEvent::Liveness { .. })),
                        "{label}: protocol lines refresh liveness"
                    );
                }
                for record in &run.records {
                    assert!(!record.contains("secret-text"), "{label}: {record}");
                    let value: Value = serde_json::from_str(record).unwrap();
                    assert_eq!(value["type"], RECORD_TYPE, "{label}");
                }
                for line in &run.evidence {
                    let session = line.session_line();
                    assert!(!session.contains("secret-text"), "{label}: {session}");
                    assert!(
                        matches!(line, CapturedOutput::StderrWithheld { .. }),
                        "{label}: stderr is kept only as counts: {session}"
                    );
                }
                match *scenario {
                    "success" => {
                        assert_eq!(run.terminal_text.as_deref(), Some("final answer"));
                        assert!(run.events.contains(&AgentEvent::Activity {
                            kind: ActivityKind::ToolUse,
                            summary: "Bash".into(),
                        }));
                        assert!(run.events.contains(&AgentEvent::CompletedAssistantText {
                            item_id: "m1".into(),
                            text: "hello".into(),
                        }));
                        assert!(
                            matches!(
                                run.evidence.as_slice(),
                                [CapturedOutput::StderrWithheld {
                                    lines: 1,
                                    bytes: 19
                                }]
                            ),
                            "{label}: stderr line and byte counts"
                        );
                    }
                    "non_json" | "non_json_after_terminal_then_exit" => {
                        let want = if *scenario == "non_json" { 2 } else { 1 };
                        let non_protocol = run
                            .records
                            .iter()
                            .filter(|r| r.contains(RECORD_NON_PROTOCOL))
                            .count();
                        assert_eq!(non_protocol, want, "{label}");
                        assert_eq!(run.terminal_text.as_deref(), Some("final answer"));
                    }
                    "exit_without_terminal" => {
                        assert!(run.records.is_empty(), "{label}");
                        assert!(
                            matches!(
                                run.evidence.as_slice(),
                                [CapturedOutput::StderrWithheld { lines: 1, .. }]
                            ),
                            "{label}: bounded stderr evidence"
                        );
                    }
                    _ => assert_eq!(run.terminal_text, None, "{label}"),
                }
                if want_terminal.is_none()
                    && want_outcome.is_some()
                    && *scenario != "eof_without_terminal"
                    && *scenario != "exit_without_terminal"
                {
                    let violation = run.records.last().unwrap();
                    assert!(violation.contains(RECORD_VIOLATION), "{label}: {violation}");
                }
            }
        }
    }

    /// Deterministic form of the `*_then_exit` cases: the consumer pauses
    /// after the terminal record, so the exit is already observable and the
    /// trailing line is still unread when the next record is requested.
    #[tokio::test]
    async fn trailing_lines_are_drained_after_an_observed_exit() {
        use FailureDisposition::*;
        let cases = [
            (
                "second_terminal_then_exit",
                Some((Unclassified, DETAIL_SECOND_TERMINAL)),
            ),
            (
                "line_after_terminal_then_exit",
                Some((Unclassified, DETAIL_LINE_AFTER_TERMINAL)),
            ),
            ("non_json_after_terminal_then_exit", None),
        ];
        for (scenario, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let spec = spec(true);
            let env = scenario_env(scenario);
            let mut proc =
                ExternalProc::launch(&request(dir.path(), &env, None), &config(Some(&spec), None))
                    .unwrap();
            // Start line, then the terminal line.
            for _ in 0..2 {
                let record = proc.next_raw_line().await.expect(scenario);
                assert!(record.contains(RECORD_PROTOCOL), "{scenario}: {record}");
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while proc.try_wait().unwrap().is_none() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{scenario}: no exit"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let run = drive(RunnerProc::External(proc)).await;
            let got = run
                .outcome
                .as_ref()
                .map(|f| (f.disposition(), f.detail().to_string()));
            assert_eq!(
                got,
                want.map(|(d, detail)| (d, detail.to_string())),
                "{scenario}"
            );
            match want {
                Some(_) => {
                    assert!(run.terminal_events().is_empty(), "{scenario}");
                    let last = run.records.last().unwrap();
                    assert!(last.contains(RECORD_VIOLATION), "{scenario}: {last}");
                }
                None => {
                    assert!(
                        matches!(
                            run.terminal_events().as_slice(),
                            [AgentEvent::TurnCompleted { .. }]
                        ),
                        "{scenario}"
                    );
                    assert!(run.records[0].contains(RECORD_NON_PROTOCOL), "{scenario}");
                }
            }
        }
    }

    #[tokio::test]
    async fn quorum_kill_takes_the_killed_by_quorum_path() {
        // Before and after the terminal event: neither a terminal event nor
        // a runner failure is reported for a tree Quorum killed itself.
        for (scenario, records_before_kill) in [("hang", 1), ("no_exit_after_terminal", 2)] {
            let dir = tempfile::tempdir().unwrap();
            let spec = spec(true);
            let env = scenario_env(scenario);
            let mut proc =
                ExternalProc::launch(&request(dir.path(), &env, None), &config(Some(&spec), None))
                    .unwrap();
            for _ in 0..records_before_kill {
                assert!(proc.next_raw_line().await.is_some(), "{scenario}");
            }
            proc.kill_tree();
            assert_eq!(proc.next_raw_line().await, None, "{scenario}");
            let mut proc = RunnerProc::External(proc);
            let status = wait_exit(&mut proc).await;
            assert_eq!(
                proc.classify_pre_authoritative_exit(status),
                None,
                "{scenario}"
            );
            assert_eq!(
                proc.observed_pre_authoritative_failure(),
                None,
                "{scenario}"
            );
        }
        // kill_and_reap returns stderr evidence as counts only.
        let dir = tempfile::tempdir().unwrap();
        let spec = spec(true);
        let env = scenario_env("hang");
        let mut proc =
            RunnerProc::launch(&request(dir.path(), &env, None), &config(Some(&spec), None))
                .await
                .unwrap();
        assert!(proc.next_raw_line().await.is_some());
        let captured = proc.kill_and_reap().await;
        assert!(captured
            .iter()
            .all(|line| matches!(line, CapturedOutput::StderrWithheld { .. })));
    }

    #[tokio::test]
    async fn launch_writes_prompt_to_stdin_and_withholds_refused_env() {
        let dir = tempfile::tempdir().unwrap();
        let observe = tempfile::tempdir().unwrap();
        let db = dir.path().join("quorum.db");
        let spec = spec(true);
        let mut env: Vec<(String, String)> = [
            ("QUORUM_REPO", "owner/repo"),
            ("QUORUM_AGENT", "Bolt-test"),
            ("QUORUM_RUN_ID", "cap-123"),
            ("QUORUM_AGENT_ENDPOINT", "/tmp/quorum-agent.sock"),
            (AGENT_MCP_URL_ENV, URL),
            ("GH_TOKEN", "ghp_ambient"),
            ("GITHUB_TOKEN", "ghp_ambient"),
            ("GH_ENTERPRISE_TOKEN", "ghp_ambient"),
            ("GITHUB_ENTERPRISE_TOKEN", "ghp_ambient"),
            ("QUORUM_HOME", "/ambient/quorum-home"),
            ("GH_CONFIG_DIR", "/ambient/gh-config"),
            (crate::orchestrator_mcp::TOKEN_ENV, "operator-token"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        env.extend(scenario_env("success"));
        env.push((
            "FAKE_CLI_OBSERVE".into(),
            observe.path().to_str().unwrap().into(),
        ));
        let managed = Some(ManagedLaunch {
            role: AgentMcpRole::Worker,
            db_path: &db,
        });
        let proc = RunnerProc::launch(
            &request(dir.path(), &env, managed),
            &config(Some(&spec), None),
        )
        .await
        .unwrap();
        let run = drive(proc).await;
        assert_eq!(run.outcome, None);

        let stdin = std::fs::read_to_string(observe.path().join("stdin")).unwrap();
        assert_eq!(
            stdin, PROMPT,
            "prompt written to stdin verbatim, then closed"
        );
        let argv = std::fs::read_to_string(observe.path().join("argv")).unwrap();
        let workspace = dir.path().to_str().unwrap();
        let want = [
            "exec",
            "--workspace",
            workspace,
            "--model",
            "m-1",
            "--effort",
            "high",
            "-",
            "--tools",
            "read-write",
            "--mcp-url",
            URL,
        ];
        assert_eq!(argv.lines().collect::<Vec<_>>(), want);
        let observed = std::fs::read_to_string(observe.path().join("env")).unwrap();
        for name in [
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "GH_ENTERPRISE_TOKEN",
            "GITHUB_ENTERPRISE_TOKEN",
            "QUORUM_HOME",
            crate::orchestrator_mcp::TOKEN_ENV,
        ] {
            assert!(
                !observed.lines().any(|l| l.starts_with(&format!("{name}="))),
                "{name} reached the child"
            );
        }
        assert!(observed.lines().any(|l| l == "FAKE_MCP_TOKEN=cap-123"));
        assert!(observed.lines().any(|l| l == "QUORUM_RUN_ID=cap-123"));
        let gh_config: String = observed
            .lines()
            .filter_map(|l| l.strip_prefix("GH_CONFIG_DIR="))
            .map(|dir| format!("gh_config={dir}\n"))
            .collect();
        assert_run_private_gh_config(&gh_config, "/ambient/gh-config");

        // A declaration that bypassed validation with a refused name is
        // refused before any child exists.
        for name in ["PATH", "LD_PRELOAD", "QUORUM_HOME", "GH_TOKEN"] {
            let observe = tempfile::tempdir().unwrap();
            let mut env = env.clone();
            env.retain(|(k, _)| k != "FAKE_CLI_OBSERVE");
            env.push((
                "FAKE_CLI_OBSERVE".into(),
                observe.path().to_str().unwrap().into(),
            ));
            let mut bad = spec.clone();
            bad.env.insert(name.into(), EnvSource::Repo);
            let error = match RunnerProc::launch(
                &request(dir.path(), &env, managed),
                &config(Some(&bad), None),
            )
            .await
            {
                Ok(_) => panic!("{name}: launched"),
                Err(error) => error,
            };
            assert_eq!(
                error.disposition(),
                FailureDisposition::NonFailover,
                "{name}"
            );
            assert!(
                error.detail().contains("refused environment variable"),
                "{name}"
            );
            assert!(
                !observe.path().join("env").exists(),
                "{name}: child spawned"
            );
        }
    }

    /// Launch the fake CLI under `spec` as `role` with a supplied slot
    /// directory; return its observed argv lines and `env` output.
    async fn observe_slot_launch(
        spec: &ExternalSpec,
        role: Option<AgentMcpRole>,
        slot_dir: &str,
        workspace: &Path,
    ) -> (Vec<String>, String) {
        let observe = tempfile::tempdir().unwrap();
        let db = workspace.join("quorum.db");
        let mut env = match role {
            Some(_) => managed_env("success"),
            None => scenario_env("success"),
        };
        env.push((SESSION_DIR_ENV.into(), slot_dir.into()));
        env.push((
            "FAKE_CLI_OBSERVE".into(),
            observe.path().to_str().unwrap().into(),
        ));
        let managed = role.map(|role| ManagedLaunch { role, db_path: &db });
        let proc = RunnerProc::launch(
            &request(workspace, &env, managed),
            &config(Some(spec), None),
        )
        .await
        .unwrap();
        let run = drive(proc).await;
        assert_eq!(run.outcome, None, "{role:?}");
        let argv = std::fs::read_to_string(observe.path().join("argv")).unwrap();
        let observed = std::fs::read_to_string(observe.path().join("env")).unwrap();
        (argv.lines().map(str::to_string).collect(), observed)
    }

    #[tokio::test]
    async fn slot_runs_receive_session_argv_and_dir_and_others_do_not() {
        let slots = tempfile::tempdir().unwrap();
        let slot_dir = slots.path().join("7").join("worker");
        let slot_dir = slot_dir.to_str().unwrap();
        let session_file = format!("{slot_dir}/session");
        let dir_line = format!("{SESSION_DIR_ENV}={slot_dir}");
        let dir_prefix = format!("{SESSION_DIR_ENV}=");
        let with_sessions = spec_from_src(&with_session(), true);
        let without = spec(true);
        let workspace = tempfile::tempdir().unwrap();
        let workspace = workspace.path();
        for role in [
            Some(AgentMcpRole::Worker),
            Some(AgentMcpRole::Reviewer),
            Some(AgentMcpRole::Planner),
            None,
        ] {
            let slot = matches!(role, Some(AgentMcpRole::Worker | AgentMcpRole::Reviewer));
            let (argv, env) = observe_slot_launch(&with_sessions, role, slot_dir, workspace).await;
            let (plain_argv, plain_env) =
                observe_slot_launch(&without, role, slot_dir, workspace).await;
            if slot {
                // args_session comes last, after args_mcp.
                let n = argv.len();
                assert_eq!(&argv[n - 4..n - 2], ["--mcp-url", URL]);
                assert_eq!(&argv[n - 2..], ["--session", session_file.as_str()]);
                assert_eq!(&argv[..n - 2], &plain_argv[..], "{role:?}");
                assert!(env.lines().any(|l| l == dir_line), "{role:?}: {env}");
            } else {
                assert_eq!(argv, plain_argv, "{role:?}");
                assert!(!argv.iter().any(|a| a.contains(slot_dir)), "{role:?}");
                assert!(
                    !env.lines().any(|l| l.starts_with(&dir_prefix)),
                    "{role:?}: {env}"
                );
            }
            // Sessions disabled: neither session argv nor the directory.
            assert!(!plain_argv.iter().any(|a| a == "--session"), "{role:?}");
            assert!(
                !plain_env.lines().any(|l| l.starts_with(&dir_prefix)),
                "{role:?}: {plain_env}"
            );
        }
        // The adapter only forms the path; it never creates the session file.
        assert!(!Path::new(&session_file).exists());
    }

    #[tokio::test]
    async fn gated_launch_runs_only_after_release() {
        let dir = tempfile::tempdir().unwrap();
        let spec = spec(true);
        let env = scenario_env("success");
        let (proc, gate) =
            RunnerProc::launch_gated(&request(dir.path(), &env, None), &config(Some(&spec), None))
                .await
                .unwrap();
        let proc = gate.release(proc).await.unwrap();
        let run = drive(proc).await;
        assert_eq!(run.outcome, None);
        assert_eq!(run.terminal_text.as_deref(), Some("final answer"));
    }

    #[tokio::test]
    async fn launch_refusals_are_non_failover() {
        let dir = tempfile::tempdir().unwrap();
        let env = scenario_env("success");
        let spec_without_executable = spec(false);
        let mut session_spec = spec(true);
        session_spec
            .args
            .push(ArgTemplate(vec![ArgPart::Placeholder(
                Placeholder::SessionFile,
            )]));
        let with_continuation = LaunchRequest {
            continuation_id: Some("prior"),
            ..request(dir.path(), &env, None)
        };
        let full = spec(true);
        let cases: Vec<(LaunchRequest<'_>, AdapterConfig<'_>, &str)> = vec![
            (request(dir.path(), &env, None), config(None, Some(FAKE_CLI)), DETAIL_NO_SPEC),
            (
                request(dir.path(), &env, None),
                config(Some(&spec_without_executable), None),
                DETAIL_NO_EXECUTABLE,
            ),
            (with_continuation, config(Some(&full), None), DETAIL_CONTINUATION),
            // `{session_file}` outside `args_session` is refused at launch.
            (
                request(dir.path(), &env, None),
                config(Some(&session_spec), None),
                "external runner launch refused: placeholder {session_file} is allowed only in external_runner.args_session, found in external_runner.args",
            ),
        ];
        for (request, config, detail) in cases {
            let error = match RunnerProc::launch(&request, &config).await {
                Ok(_) => panic!("{detail}: launched"),
                Err(error) => error,
            };
            assert_eq!(
                error.disposition(),
                FailureDisposition::NonFailover,
                "{detail}"
            );
            assert_eq!(error.detail(), detail);
        }
    }

    #[tokio::test]
    async fn agent_mcp_wiring_requires_the_url_for_external() {
        let complete: Vec<(String, String)> = [
            ("QUORUM_REPO", "owner/repo"),
            ("QUORUM_AGENT", "Bolt-test"),
            ("QUORUM_RUN_ID", "cap-123"),
            ("QUORUM_AGENT_ENDPOINT", "/tmp/quorum-agent.sock"),
            (AGENT_MCP_URL_ENV, URL),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let without_url: Vec<(String, String)> = complete
            .iter()
            .filter(|(k, _)| k != AGENT_MCP_URL_ENV)
            .cloned()
            .collect();
        let advertised = crate::agent_mcp::advertised_tool_names;
        check_agent_mcp_wiring(
            AgentKind::External,
            AgentMcpRole::Worker,
            &complete,
            advertised,
        )
        .unwrap();
        let error = check_agent_mcp_wiring(
            AgentKind::External,
            AgentMcpRole::Worker,
            &without_url,
            advertised,
        )
        .unwrap_err();
        assert_eq!(error.disposition(), FailureDisposition::NonFailover);
        assert!(error.detail().contains(AGENT_MCP_URL_ENV), "{error}");
        // Built-in stdio transports are unchanged.
        check_agent_mcp_wiring(
            AgentKind::Codex,
            AgentMcpRole::Worker,
            &without_url,
            advertised,
        )
        .unwrap();

        // The launch boundary refuses the same run before spawning.
        let dir = tempfile::tempdir().unwrap();
        let observe = tempfile::tempdir().unwrap();
        let db = dir.path().join("quorum.db");
        let spec = spec(true);
        let mut env = without_url;
        env.push((
            "FAKE_CLI_OBSERVE".into(),
            observe.path().to_str().unwrap().into(),
        ));
        let managed = Some(ManagedLaunch {
            role: AgentMcpRole::Reviewer,
            db_path: &db,
        });
        let error = match RunnerProc::launch(
            &request(dir.path(), &env, managed),
            &config(Some(&spec), None),
        )
        .await
        {
            Ok(_) => panic!("launched without the agent MCP URL"),
            Err(error) => error,
        };
        assert!(
            error.detail().starts_with("agent MCP launch check failed"),
            "{error}"
        );
        assert!(!observe.path().join("env").exists(), "child spawned");
    }

    fn managed_env(scenario: &str) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = [
            ("QUORUM_REPO", "owner/repo"),
            ("QUORUM_AGENT", "Bolt-test"),
            ("QUORUM_RUN_ID", "cap-123"),
            ("QUORUM_AGENT_ENDPOINT", "/tmp/quorum-agent.sock"),
            (AGENT_MCP_URL_ENV, URL),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        env.extend(scenario_env(scenario));
        env
    }

    /// Launch `scenario` as `role` (`None`: restricted) and drive it; also
    /// return the retained blocked outcome.
    async fn launch_as(scenario: &str, role: Option<AgentMcpRole>) -> (Run, Option<RunnerBlocked>) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("quorum.db");
        let spec = spec(true);
        // A restricted run carries no managed run environment.
        let env = match role {
            Some(_) => managed_env(scenario),
            None => scenario_env(scenario),
        };
        let managed = role.map(|role| ManagedLaunch { role, db_path: &db });
        let mut proc = RunnerProc::launch(
            &request(dir.path(), &env, managed),
            &config(Some(&spec), None),
        )
        .await
        .unwrap_or_else(|error| panic!("{scenario}: launch failed: {error}"));
        // Read the records first so the retained outcome can be inspected
        // before `drive` consumes the process.
        let mut records = Vec::new();
        while let Some(raw) = tokio::time::timeout(Duration::from_secs(20), proc.next_raw_line())
            .await
            .expect("record within bound")
        {
            records.push(raw);
        }
        let blocked = proc.runner_blocked().cloned();
        let mut run = drive(proc).await;
        run.events = records
            .iter()
            .flat_map(|raw| normalize_line(raw).events)
            .collect();
        run.records = records;
        (run, blocked)
    }

    #[tokio::test]
    async fn blocked_worker_run_retains_question_and_records_metadata_only() {
        let (run, blocked) = launch_as("blocked", Some(AgentMcpRole::Worker)).await;
        assert!(run.outcome.is_none(), "{:?}", run.outcome);
        // Empty and non-string context values are dropped; order is kept.
        assert_eq!(
            blocked,
            Some(RunnerBlocked {
                question: "Which schema version?".into(),
                context: vec!["two migrations conflict".into()],
            })
        );
        let terminal: Vec<_> = run
            .events
            .iter()
            .filter(|e| matches!(e, AgentEvent::TurnBlocked { .. }))
            .collect();
        assert_eq!(
            terminal,
            vec![&AgentEvent::TurnBlocked {
                question: String::new(),
                context: Vec::new(),
                usage: Some(TokenUsage {
                    input_tokens: 4,
                    uncached_input_tokens: 4,
                    output_tokens: 2,
                    ..TokenUsage::default()
                }),
            }]
        );
        assert!(run.terminal_events().is_empty());
        for record in &run.records {
            assert!(!record.contains("schema version"), "{record}");
            assert!(!record.contains("migrations conflict"), "{record}");
        }
    }

    #[tokio::test]
    async fn blocked_outcome_fails_closed_without_question_contradicting_exit_or_writing_role() {
        use crate::serve::external_stream::{
            DETAIL_BLOCKED_OUTSIDE_WRITING_ROLE, DETAIL_BLOCKED_WITHOUT_QUESTION,
        };
        use FailureDisposition::*;
        let cases: &[(&str, Option<AgentMcpRole>, FailureDisposition, &str)] = &[
            (
                "blocked_without_question",
                Some(AgentMcpRole::Worker),
                NonFailover,
                DETAIL_BLOCKED_WITHOUT_QUESTION,
            ),
            (
                "blocked_contradicting_exit",
                Some(AgentMcpRole::Worker),
                Unclassified,
                DETAIL_EXIT_CONTRADICTS,
            ),
            (
                "blocked",
                Some(AgentMcpRole::Reviewer),
                NonFailover,
                DETAIL_BLOCKED_OUTSIDE_WRITING_ROLE,
            ),
            (
                "blocked",
                Some(AgentMcpRole::Planner),
                NonFailover,
                DETAIL_BLOCKED_OUTSIDE_WRITING_ROLE,
            ),
            (
                "blocked",
                None,
                NonFailover,
                DETAIL_BLOCKED_OUTSIDE_WRITING_ROLE,
            ),
        ];
        for &(scenario, role, disposition, detail) in cases {
            let (run, blocked) = launch_as(scenario, role).await;
            let outcome = run.outcome.as_ref().expect("failure expected");
            assert_eq!(outcome.disposition(), disposition, "{scenario}/{role:?}");
            assert_eq!(outcome.detail(), detail, "{scenario}/{role:?}");
            assert!(blocked.is_none(), "{scenario}/{role:?}");
            assert!(
                !run.events
                    .iter()
                    .any(|e| matches!(e, AgentEvent::TurnBlocked { .. })),
                "{scenario}/{role:?}"
            );
            for record in &run.records {
                assert!(!record.contains("schema version"), "{record}");
            }
        }
    }

    #[test]
    fn records_round_trip_and_raw_lines_normalize_to_nothing() {
        let usage = TokenUsage {
            input_tokens: 3,
            uncached_input_tokens: 1,
            cached_input_tokens: 1,
            cache_write_input_tokens: 1,
            output_tokens: 7,
            reasoning_tokens: 0,
        };
        let events = vec![
            AgentEvent::Liveness {
                summary: "external protocol line".into(),
            },
            AgentEvent::TurnCompleted {
                usage: Some(usage),
                cost_usd: None,
            },
        ];
        let record = encode(RECORD_EXIT_CHECK, &events, Some("done"), None);
        let line = normalize_line(&record);
        assert_eq!(line.events, events);
        assert_eq!(line.terminal_text.as_deref(), Some("done"));
        assert_eq!(
            crate::serve::runner::normalize_line(AgentKind::External, &record),
            events
        );
        // A raw CLI line, even one shaped like a terminal event, is inert.
        let raw = r#"{"type":"run.completed","status":"completed"}"#;
        assert!(normalize_line(raw).events.is_empty());
        assert!(normalize_line("not json").events.is_empty());
    }
}
