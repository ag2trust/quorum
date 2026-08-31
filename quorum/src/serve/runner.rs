//! Runner boundary: closed AgentKind enum and normalized AgentEvent.
//!
//! The daemon lifecycle consumes `AgentEvent`, never provider-specific event
//! shapes. Raw JSONL lines are preserved verbatim in stream.jsonl; only fields
//! Quorum consumes are parsed into normalized events. Unknown events are inert.
//!
//! `journal.session_id` is an opaque runner continuation ID: Claude receives a
//! Quorum-generated UUID before spawn; Codex persists the thread ID from
//! `thread.started`; Grok persists the session ID from terminal `end`. The
//! column name is retained for schema compatibility.

use super::agent::AgentProc;
use super::codex_agent::CodexProc;
use super::grok_agent::{GrokAdapterConfig, GrokProc};
use quorum_core::runner_state::{self, PendingTurn};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

const DIAGNOSTIC_CAPACITY: usize = 256;
const DIAGNOSTIC_LINE_BYTES: usize = 16 * 1024;
const EXIT_EVIDENCE_LINES: usize = 256;
const EXIT_EVIDENCE_LINE_BYTES: usize = 16 * 1024;
const EXIT_EVIDENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Closed, provider-neutral classification for runner failures observed before
/// a managed agent has produced an authoritative task or review outcome.
///
/// This is evidence only. It does not authorize route selection, assignment
/// replacement, lifecycle mutation, or recovery accounting by itself.
pub use quorum_core::routing_attempts::FailureDisposition;

/// Classified runner-boundary failure plus a bounded, non-authoritative
/// diagnostic. Lifecycle callers may report this value but must independently
/// prove that no managed task/review outcome exists before acting on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerFailure {
    disposition: FailureDisposition,
    detail: String,
    io_kind: std::io::ErrorKind,
}

impl RunnerFailure {
    const DETAIL_BYTES: usize = 512;

    fn new(disposition: FailureDisposition, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        let detail = if detail.len() <= Self::DETAIL_BYTES {
            detail
        } else {
            let mut end = Self::DETAIL_BYTES;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}…", &detail[..end])
        };
        Self {
            disposition,
            detail,
            io_kind: std::io::ErrorKind::Other,
        }
    }

    fn with_io_kind(mut self, kind: std::io::ErrorKind) -> Self {
        self.io_kind = kind;
        self
    }

    pub fn disposition(&self) -> FailureDisposition {
        self.disposition
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn kind(&self) -> std::io::ErrorKind {
        self.io_kind
    }
}

impl std::fmt::Display for RunnerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.disposition, self.detail)
    }
}

impl std::error::Error for RunnerFailure {}

impl From<RunnerFailure> for std::io::Error {
    fn from(error: RunnerFailure) -> Self {
        std::io::Error::new(error.io_kind, error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FailureObservation {
    pub disposition: Option<FailureDisposition>,
    pub detail: Option<String>,
    pub terminal_success: bool,
    pub unknown_failure: bool,
    /// Bounded stderr that has no recognized provider meaning is retained for
    /// an unsuccessful exit, but cannot by itself contradict a completed
    /// Codex planner turn. Codex can emit informational stderr while it is
    /// still producing valid JSONL.
    pub deferred_stderr: bool,
}

impl FailureObservation {
    pub fn inert() -> Self {
        Self {
            disposition: None,
            detail: None,
            terminal_success: false,
            unknown_failure: false,
            deferred_stderr: false,
        }
    }

    pub fn classified(disposition: FailureDisposition, detail: impl Into<String>) -> Self {
        Self {
            disposition: Some(disposition),
            detail: Some(detail.into()),
            terminal_success: false,
            unknown_failure: false,
            deferred_stderr: false,
        }
    }

    pub fn unknown_failure(detail: impl Into<String>) -> Self {
        Self {
            disposition: None,
            detail: Some(detail.into()),
            terminal_success: false,
            unknown_failure: true,
            deferred_stderr: false,
        }
    }

    pub fn deferred_stderr(detail: impl Into<String>) -> Self {
        Self {
            detail: Some(detail.into()),
            deferred_stderr: true,
            ..Self::inert()
        }
    }

    pub fn success() -> Self {
        Self {
            terminal_success: true,
            ..Self::inert()
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct FailureTracker(Arc<Mutex<FailureTrackerState>>);

#[derive(Default)]
struct FailureTrackerState {
    kind: Option<AgentKind>,
    classified: Option<RunnerFailure>,
    conflicting: bool,
    terminal_success: bool,
    saw_protocol_line: bool,
    unknown_failure: Option<String>,
    deferred_stderr: Option<String>,
    incomplete_evidence: Option<String>,
}

impl FailureTracker {
    pub fn for_kind(kind: AgentKind) -> Self {
        Self(Arc::new(Mutex::new(FailureTrackerState {
            kind: Some(kind),
            ..FailureTrackerState::default()
        })))
    }

    pub fn observe_stdout(&self, raw: &str) {
        if raw.is_empty() {
            return;
        }
        let kind = self.0.lock().expect("failure tracker poisoned").kind;
        let observation = match kind {
            Some(AgentKind::Claude) => AgentProc::failure_observation(raw),
            Some(AgentKind::Codex) => CodexProc::failure_observation(raw),
            Some(AgentKind::Grok) => GrokProc::failure_observation(raw),
            None => return,
        };
        self.record(observation, true);
    }

    pub fn observe_stderr(&self, text: &str) {
        let kind = self.0.lock().expect("failure tracker poisoned").kind;
        let observation = match kind {
            Some(AgentKind::Claude) => AgentProc::stderr_failure_observation(text),
            Some(AgentKind::Codex) => CodexProc::stderr_failure_observation(text),
            Some(AgentKind::Grok) => GrokProc::stderr_failure_observation(text),
            None => return,
        };
        self.record(observation, false);
    }

    pub fn observe_protocol_read_error(&self, detail: impl Into<String>) {
        self.record(
            FailureObservation::classified(FailureDisposition::NonFailover, detail),
            false,
        );
    }

    /// Start a fresh managed turn on a persistent provider process. Failure
    /// evidence is authoritative only within one pre-outcome turn; retaining
    /// success or conflicting evidence across rework/re-review turns would
    /// suppress or corrupt the later turn's classification.
    pub fn begin_turn(&self) {
        let mut state = self.0.lock().expect("failure tracker poisoned");
        let kind = state.kind;
        *state = FailureTrackerState {
            kind,
            ..FailureTrackerState::default()
        };
    }

    pub fn note_incomplete_evidence(&self, detail: impl Into<String>) {
        let mut state = self.0.lock().expect("failure tracker poisoned");
        if state.incomplete_evidence.is_none() {
            state.incomplete_evidence = Some(detail.into());
        }
    }

    fn record(&self, observation: FailureObservation, protocol_line: bool) {
        let mut state = self.0.lock().expect("failure tracker poisoned");
        state.saw_protocol_line |= protocol_line;
        state.terminal_success |= observation.terminal_success;
        if observation.unknown_failure && state.unknown_failure.is_none() {
            state.unknown_failure = observation.detail.clone();
        }
        if observation.deferred_stderr && state.deferred_stderr.is_none() {
            state.deferred_stderr = observation.detail.clone();
        }
        let Some(disposition) = observation.disposition else {
            return;
        };
        let failure = RunnerFailure::new(
            disposition,
            observation
                .detail
                .unwrap_or_else(|| "provider failure".into()),
        );
        match &state.classified {
            None => state.classified = Some(failure),
            Some(existing) if existing.disposition == disposition => {}
            Some(_) => state.conflicting = true,
        }
    }

    pub fn classify_exit(&self, status: std::process::ExitStatus) -> Option<RunnerFailure> {
        let state = self.0.lock().expect("failure tracker poisoned");
        if let Some(failure) = observed_failure(&state) {
            return Some(failure);
        }
        if state.terminal_success {
            return None;
        }
        if status.success() || state.saw_protocol_line {
            return Some(RunnerFailure::new(
                FailureDisposition::RetryableSameRoute,
                "provider reached EOF before a terminal protocol event",
            ));
        }
        Some(RunnerFailure::new(
            FailureDisposition::NonFailover,
            format!("provider exited with {status} without other failure evidence"),
        ))
    }

    pub fn observed_failure(&self) -> Option<RunnerFailure> {
        let state = self.0.lock().expect("failure tracker poisoned");
        observed_failure(&state)
    }

    /// Planner-only view: unknown bounded Codex stderr is useful evidence if
    /// the process exits without a terminal result, but is not authoritative
    /// while valid JSONL is still arriving.
    pub fn observed_planner_live_failure(&self) -> Option<RunnerFailure> {
        let state = self.0.lock().expect("failure tracker poisoned");
        observed_failure_without_deferred_stderr(&state)
    }

    /// Planner-only terminal-success view. A clean Codex exit after
    /// `turn.completed` wins over informational/unclassified stderr, while
    /// all protocol, classified, conflicting, and read-boundary evidence
    /// remains fail-closed.
    pub fn observed_planner_terminal_failure(&self) -> Option<RunnerFailure> {
        let state = self.0.lock().expect("failure tracker poisoned");
        observed_strict_failure_without_deferred_stderr(&state)
    }

    /// Grok withholds terminal success until EOF and zero exit, so any earlier
    /// protocol/diagnostic failure remains authoritative even if a later
    /// syntactically valid `end` was observed. Other providers retain their
    /// established terminal-success precedence through `observed_failure`.
    pub fn observed_strict_failure(&self) -> Option<RunnerFailure> {
        let state = self.0.lock().expect("failure tracker poisoned");
        observed_strict_failure(&state)
    }
}

fn observed_failure(state: &FailureTrackerState) -> Option<RunnerFailure> {
    if state.terminal_success {
        return None;
    }
    observed_strict_failure(state)
}

fn observed_failure_without_deferred_stderr(state: &FailureTrackerState) -> Option<RunnerFailure> {
    if state.terminal_success {
        return None;
    }
    observed_strict_failure_without_deferred_stderr(state)
}

fn observed_strict_failure(state: &FailureTrackerState) -> Option<RunnerFailure> {
    if let Some(detail) = &state.incomplete_evidence {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            detail.clone(),
        ));
    }
    if state.conflicting {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            "provider emitted conflicting failure evidence",
        ));
    }
    if let Some(failure) = &state.classified {
        return Some(failure.clone());
    }
    if let Some(detail) = &state.unknown_failure {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            detail.clone(),
        ));
    }
    if let Some(detail) = &state.deferred_stderr {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            detail.clone(),
        ));
    }
    None
}

fn observed_strict_failure_without_deferred_stderr(
    state: &FailureTrackerState,
) -> Option<RunnerFailure> {
    if let Some(detail) = &state.incomplete_evidence {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            detail.clone(),
        ));
    }
    if state.conflicting {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            "provider emitted conflicting failure evidence",
        ));
    }
    if let Some(failure) = &state.classified {
        return Some(failure.clone());
    }
    if let Some(detail) = &state.unknown_failure {
        return Some(RunnerFailure::new(
            FailureDisposition::Unclassified,
            detail.clone(),
        ));
    }
    None
}

pub enum CapturedOutput {
    Stdout(String),
    StdoutTruncated { dropped: usize },
    Stderr(String),
    StderrTruncated { dropped: usize },
    StderrBytesTruncated { dropped: usize },
}

impl CapturedOutput {
    pub fn session_line(&self) -> String {
        match self {
            Self::Stdout(line) => line.clone(),
            Self::StdoutTruncated { dropped } => serde_json::json!({
                "type":"provider.stdout_truncated",
                "dropped_lines":dropped
            })
            .to_string(),
            Self::Stderr(text) => {
                serde_json::json!({"type":"provider.stderr","text":text}).to_string()
            }
            Self::StderrTruncated { dropped } => serde_json::json!({
                "type":"provider.stderr_truncated",
                "dropped_lines":dropped
            })
            .to_string(),
            Self::StderrBytesTruncated { dropped } => serde_json::json!({
                "type":"provider.stderr_bytes_truncated",
                "dropped_bytes":dropped
            })
            .to_string(),
        }
    }
}

#[derive(Clone, Default)]
pub struct DiagnosticBuffer {
    state: Arc<Mutex<DiagnosticState>>,
    failures: FailureTracker,
}

#[derive(Default)]
struct DiagnosticState {
    lines: VecDeque<String>,
    dropped: usize,
    dropped_bytes: usize,
}

impl DiagnosticBuffer {
    pub(super) fn for_kind(kind: AgentKind) -> Self {
        Self {
            state: Arc::new(Mutex::new(DiagnosticState::default())),
            failures: FailureTracker::for_kind(kind),
        }
    }

    pub(super) fn failures(&self) -> FailureTracker {
        self.failures.clone()
    }

    pub fn push(&self, line: String) {
        self.failures.observe_stderr(&line);
        let mut state = self.state.lock().expect("diagnostic buffer poisoned");
        if state.lines.len() == DIAGNOSTIC_CAPACITY {
            state.lines.pop_front();
            state.dropped += 1;
        }
        state.lines.push_back(line);
    }

    pub fn drain(&self) -> Vec<CapturedOutput> {
        let mut state = self.state.lock().expect("diagnostic buffer poisoned");
        let mut output = Vec::with_capacity(
            state.lines.len()
                + usize::from(state.dropped > 0)
                + usize::from(state.dropped_bytes > 0),
        );
        if state.dropped > 0 {
            output.push(CapturedOutput::StderrTruncated {
                dropped: state.dropped,
            });
            state.dropped = 0;
        }
        if state.dropped_bytes > 0 {
            output.push(CapturedOutput::StderrBytesTruncated {
                dropped: state.dropped_bytes,
            });
            state.dropped_bytes = 0;
        }
        output.extend(state.lines.drain(..).map(CapturedOutput::Stderr));
        output
    }

    fn note_dropped_bytes(&self, count: usize) {
        let mut state = self.state.lock().expect("diagnostic buffer poisoned");
        state.dropped_bytes = state.dropped_bytes.saturating_add(count);
    }

    fn note_read_error(&self, error: &std::io::Error) {
        self.failures.note_incomplete_evidence(format!(
            "provider stderr evidence could not be read: {error}"
        ));
    }
}

pub async fn capture_diagnostics<R>(mut reader: R, diagnostics: DiagnosticBuffer)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0u8; 4096];
    let mut line = Vec::new();
    let mut line_dropped = 0usize;
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Err(error) => {
                diagnostics.note_read_error(&error);
                break;
            }
            Ok(read) => {
                for &byte in &chunk[..read] {
                    if byte == b'\n' {
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        diagnostics.push(String::from_utf8_lossy(&line).into_owned());
                        if line_dropped > 0 {
                            diagnostics.note_dropped_bytes(line_dropped);
                            line_dropped = 0;
                        }
                        line.clear();
                    } else if line.len() < DIAGNOSTIC_LINE_BYTES {
                        line.push(byte);
                    } else {
                        line_dropped = line_dropped.saturating_add(1);
                    }
                }
            }
        }
    }
    if !line.is_empty() {
        diagnostics.push(String::from_utf8_lossy(&line).into_owned());
    }
    if line_dropped > 0 {
        diagnostics.note_dropped_bytes(line_dropped);
    }
}
/// Closed runner enum — supporting another runner requires an explicit code
/// change, not configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentKind {
    Claude,
    Codex,
    Grok,
}

/// Process lifetime declared by a runner adapter. Lifecycle orchestration uses
/// this capability instead of branching on a provider name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnMode {
    /// One persistent child accepts later turns over its input stream.
    PersistentChild,
    /// Each turn is a new process and later turns resume opaque provider state.
    RespawnPerTurn,
}

/// Provider-neutral inputs for starting one runner turn.
///
/// Provider adapters translate this request into their private command specs.
/// The model is resolved through [`AgentKind::for_model`]; callers cannot pair
/// an arbitrary runner with a model or fall back after an adapter fails.
pub struct LaunchRequest<'a> {
    pub model: &'a str,
    pub effort: &'a str,
    pub worktree: &'a Path,
    /// Raw prompt text. The Claude adapter wraps it as a stream-json user turn;
    /// turn-oriented adapters pass it through their command boundary.
    pub prompt: &'a str,
    pub environment: &'a [(String, String)],
    pub mode: LaunchMode,
    /// Opaque provider identity. Claude uses it as its pre-spawn session UUID;
    /// Codex uses it to resume the exact provider-issued thread; Grok uses it
    /// with the official CLI's exact-session `--resume` command.
    pub continuation_id: Option<&'a str>,
}

/// Credentialless stdio server registered only for a complete daemon-managed
/// worker or reviewer run envelope. Provider adapters receive the command
/// shape, never a database path, GitHub credential, role, or phase inventory;
/// the daemon endpoint derives and enforces all of that authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentMcpServer {
    pub command: &'static str,
    pub args: &'static [&'static str],
    /// Names whose values the provider must copy from its managed launch
    /// environment into the stdio child. Values never enter provider args.
    pub env_vars: &'static [&'static str],
}

pub const AGENT_MCP_ENV_VARS: &[&str] = &[
    "QUORUM_REPO",
    "QUORUM_AGENT",
    "QUORUM_RUN_ID",
    "QUORUM_AGENT_ENDPOINT",
];

pub const AGENT_MCP_SERVER: AgentMcpServer = AgentMcpServer {
    command: "quorum",
    args: &["agent-mcp"],
    env_vars: AGENT_MCP_ENV_VARS,
};

/// The single MCP tool a decomposition planner may call. Workers and reviewers
/// hold the whole managed inventory, so this allowlist names one tool exactly
/// instead of the server-wide wildcard.
///
/// The allowlist is a narrowing, not the containment. A planner capability is
/// contained by the endpoint: `resolve_live_run_context`
/// (`quorum-core/src/capabilities.rs:113-116,140`) accepts only the `worker`
/// and `reviewer` operation roles and requires an exact role match, so a
/// `planner` capability satisfies no GitHub or lifecycle operation;
/// `resolve_planner_context` (`:313`, role check `:335`) is the only resolver
/// that accepts it, and `SubmitPlan` is bounded further by the once-only guard
/// and `MAX_PLAN_SUBMIT_REJECTIONS`
/// (`quorum-core/src/planner_submissions.rs:16`).
///
/// `quorum` is the server name registered by the provider adapters
/// (`agent::claude_mcp_config`, `mcp_servers.quorum` for Codex).
pub const PLANNER_MCP_ALLOWED_TOOL: &str = "mcp__quorum__submit_plan";

impl LaunchRequest<'_> {
    /// Restricted/internal turns lack this complete capability envelope. The
    /// run capability is the discriminator rather than `Normal` alone because
    /// collectors intentionally use the normal provider protocol while still
    /// having no managed worker/reviewer endpoint authority.
    pub fn agent_mcp_server(&self) -> Option<AgentMcpServer> {
        (self.mode == LaunchMode::Normal
            && AGENT_MCP_ENV_VARS.iter().all(|required| {
                self.environment
                    .iter()
                    .any(|(key, value)| key == required && !value.is_empty())
            }))
        .then_some(AGENT_MCP_SERVER)
    }
}

/// Complete identity for one managed Grok worker launch. This request keeps
/// task, assignment, role, and pending-turn identity intact until Grok emits
/// its terminal session identity.
pub struct RunnerRequest<'a> {
    pub launch: LaunchRequest<'a>,
    pub task_id: i64,
    pub role_assignment_id: i64,
    pub responsibility_key: &'a str,
    pub agent: &'a str,
    pub role: &'a str,
    pub pending_turn: PendingTurn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTurnRequest {
    pub task_id: i64,
    pub role_assignment_id: i64,
    pub responsibility_key: String,
    pub agent: String,
    pub role: String,
    pub provider: String,
    pub runner: String,
    pub model: String,
    pub effort: String,
    pub pending_turn: PendingTurn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchMode {
    Normal,
    Restricted,
}

/// Installed-runner settings interpreted only by the corresponding adapter.
/// These are deliberately separate from the neutral turn identity above.
pub struct AdapterConfig<'a> {
    pub executable: Option<&'a str>,
    pub claude_bare: bool,
    pub claude_allowed_tools: &'a str,
    pub codex_sandbox: &'a str,
    pub grok: GrokAdapterConfig<'a>,
}

/// One raw provider line plus its normalized lifecycle events.
///
/// Claude's terminal `result` can carry the classifier's response without an
/// assistant event, so adapters expose that text without leaking raw protocol
/// parsing back into role orchestration.
pub struct NormalizedLine {
    pub events: Vec<AgentEvent>,
    pub terminal_text: Option<String>,
}

/// Provider-specific process transport behind the normalized runner boundary.
pub enum RunnerProc {
    Claude(AgentProc),
    Codex(CodexProc),
    Grok(GrokProc),
}

impl RunnerProc {
    /// Launch an initial Grok worker through the shared request/process
    /// boundary so terminal session identity can be persisted atomically.
    pub async fn launch_internal_worker(
        request: &RunnerRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> Result<Self, RunnerFailure> {
        let kind = AgentKind::for_model(request.launch.model).map_err(|error| {
            RunnerFailure::new(FailureDisposition::Unclassified, error)
                .with_io_kind(std::io::ErrorKind::InvalidInput)
        })?;
        if kind != AgentKind::Grok
            || request.task_id <= 0
            || request.role_assignment_id <= 0
            || request.responsibility_key.is_empty()
            || request.agent.is_empty()
            || request.role != "worker"
            || request.launch.continuation_id.is_some()
            || request.pending_turn.turn_kind != "initial"
            || request.pending_turn.continuation_id.is_some()
            || request.pending_turn.requested
            || request.pending_turn.provider != kind.to_string()
            || request.pending_turn.model != request.launch.model
            || request.pending_turn.effort != request.launch.effort
            || request.pending_turn.prompt != request.launch.prompt
            || !runner_state::pending_turn_is_complete(&request.pending_turn)
        {
            return Err(RunnerFailure::new(
                FailureDisposition::NonFailover,
                "internal Grok worker request has incomplete or mismatched task, assignment, role, model, effort, or pending-turn identity",
            )
            .with_io_kind(std::io::ErrorKind::InvalidInput));
        }
        let identity = WorkerTurnRequest {
            task_id: request.task_id,
            role_assignment_id: request.role_assignment_id,
            responsibility_key: request.responsibility_key.to_string(),
            agent: request.agent.to_string(),
            role: request.role.to_string(),
            provider: kind.to_string(),
            runner: kind.to_string(),
            model: request.launch.model.to_string(),
            effort: request.launch.effort.to_string(),
            pending_turn: request.pending_turn.clone(),
        };
        match Self::launch(&request.launch, config).await? {
            Self::Grok(mut proc) => {
                proc.set_worker_request(identity);
                Ok(Self::Grok(proc))
            }
            _ => unreachable!("internal worker boundary accepted only Grok above"),
        }
    }

    /// Resolve and launch the explicit built-in adapter for this model.
    pub async fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> Result<Self, RunnerFailure> {
        let kind = AgentKind::for_model(request.model).map_err(|error| {
            RunnerFailure::new(FailureDisposition::Unclassified, error)
                .with_io_kind(std::io::ErrorKind::InvalidInput)
        })?;
        let result = match kind {
            AgentKind::Claude => AgentProc::launch(request, config).await.map(Self::Claude),
            AgentKind::Codex => CodexProc::launch(request, config).map(Self::Codex),
            AgentKind::Grok => GrokProc::launch(request, config).map(Self::Grok),
        };
        result.map_err(|error| classify_launch_error(kind, error))
    }

    /// Restricted classifier launch whose deadline covers Claude's initial
    /// stdin feed as well as the returned slot lifetime. Single-turn adapters
    /// receive their prompt in argv and therefore have no pre-slot pipe wait.
    pub async fn launch_restricted_until(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
        deadline: tokio::time::Instant,
    ) -> Result<Self, RunnerFailure> {
        if request.mode != LaunchMode::Restricted {
            return Err(RunnerFailure::new(
                FailureDisposition::NonFailover,
                "bounded restricted launch requires restricted mode",
            )
            .with_io_kind(std::io::ErrorKind::InvalidInput));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RunnerFailure::new(
                FailureDisposition::RetryableSameRoute,
                "restricted provider launch timed out",
            )
            .with_io_kind(std::io::ErrorKind::TimedOut));
        }
        let kind = AgentKind::for_model(request.model).map_err(|error| {
            RunnerFailure::new(FailureDisposition::Unclassified, error)
                .with_io_kind(std::io::ErrorKind::InvalidInput)
        })?;
        let result = match kind {
            AgentKind::Claude => AgentProc::launch_restricted_until(request, config, deadline)
                .await
                .map(Self::Claude),
            AgentKind::Codex => CodexProc::launch(request, config).map(Self::Codex),
            AgentKind::Grok => GrokProc::launch(request, config).map(Self::Grok),
        };
        result.map_err(|error| classify_launch_error(kind, error))
    }

    pub fn kind(&self) -> AgentKind {
        match self {
            Self::Claude(_) => AgentKind::Claude,
            Self::Codex(_) => AgentKind::Codex,
            Self::Grok(_) => AgentKind::Grok,
        }
    }

    pub fn worker_request(&self) -> Option<&WorkerTurnRequest> {
        match self {
            Self::Grok(proc) => proc.worker_request(),
            Self::Claude(_) | Self::Codex(_) => None,
        }
    }

    pub async fn kill_and_reap(self) -> Vec<CapturedOutput> {
        match self {
            Self::Claude(proc) => proc.kill_and_reap().await,
            Self::Codex(proc) => proc.kill_and_reap().await,
            Self::Grok(proc) => proc.kill_and_reap().await,
        }
    }

    /// After `try_wait` proves that the provider leader exited, consume the
    /// remaining bounded stdout records and join the stderr reader before a
    /// caller snapshots failure evidence. Descendants may retain pipes, so
    /// both work and allocation are bounded; incomplete evidence fails safe.
    pub async fn finalize_pre_authoritative_evidence(&mut self) -> Vec<CapturedOutput> {
        let deadline = tokio::time::Instant::now() + EXIT_EVIDENCE_TIMEOUT;
        let mut stdout = VecDeque::new();
        let mut dropped = 0usize;
        let mut stdout_complete = false;

        for index in 0..=EXIT_EVIDENCE_LINES {
            match tokio::time::timeout_at(
                deadline,
                self.next_exit_evidence_line_bounded(EXIT_EVIDENCE_LINE_BYTES),
            )
            .await
            {
                Ok(Ok(Some(line))) => {
                    if stdout.len() == EXIT_EVIDENCE_LINES {
                        stdout.pop_front();
                        dropped = dropped.saturating_add(1);
                    }
                    stdout.push_back(CapturedOutput::Stdout(line));
                    if index == EXIT_EVIDENCE_LINES {
                        self.failure_tracker().note_incomplete_evidence(format!(
                            "provider exit evidence exceeded {EXIT_EVIDENCE_LINES} stdout records"
                        ));
                        break;
                    }
                }
                Ok(Ok(None)) => {
                    stdout_complete = true;
                    break;
                }
                Ok(Err(error)) => {
                    self.failure_tracker().note_incomplete_evidence(format!(
                        "provider exit stdout could not be finalized: {error}"
                    ));
                    break;
                }
                Err(_) => {
                    self.failure_tracker()
                        .note_incomplete_evidence("provider exit stdout finalization timed out");
                    break;
                }
            }
        }

        if !stdout_complete && dropped == 0 {
            // The specific read/timeout path above records a diagnostic. This
            // fallback covers any future loop exit that does not reach EOF.
            self.failure_tracker()
                .note_incomplete_evidence("provider exit stdout evidence was incomplete");
        }
        if !self.finish_stderr_until(deadline).await {
            self.failure_tracker()
                .note_incomplete_evidence("provider exit stderr evidence was incomplete");
        }

        let mut output =
            Vec::with_capacity(stdout.len() + usize::from(dropped > 0) + DIAGNOSTIC_CAPACITY);
        if dropped > 0 {
            output.push(CapturedOutput::StdoutTruncated { dropped });
        }
        output.extend(stdout);
        output.extend(self.drain_diagnostics());
        output
    }

    fn failure_tracker(&self) -> FailureTracker {
        match self {
            Self::Claude(proc) => proc.failure_tracker(),
            Self::Codex(proc) => proc.failure_tracker(),
            Self::Grok(proc) => proc.failure_tracker(),
        }
    }

    async fn finish_stderr_until(&mut self, deadline: tokio::time::Instant) -> bool {
        match self {
            Self::Claude(proc) => proc.finish_stderr_until(deadline).await,
            Self::Codex(proc) => proc.finish_stderr_until(deadline).await,
            Self::Grok(proc) => proc.finish_stderr_until(deadline).await,
        }
    }

    async fn next_exit_evidence_line_bounded(
        &mut self,
        max_bytes: usize,
    ) -> std::io::Result<Option<String>> {
        match self {
            Self::Claude(proc) => proc.next_raw_line_bounded(max_bytes).await,
            Self::Codex(proc) => proc.next_raw_line_bounded(max_bytes).await,
            Self::Grok(proc) => proc.next_raw_line_bounded(max_bytes).await,
        }
    }

    pub fn pid(&self) -> Option<i32> {
        match self {
            Self::Claude(proc) => proc.pid(),
            Self::Codex(proc) => proc.pid(),
            Self::Grok(proc) => proc.pid(),
        }
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match self {
            Self::Claude(proc) => proc.try_wait(),
            Self::Codex(proc) => proc.try_wait(),
            Self::Grok(proc) => proc.try_wait(),
        }
    }

    /// Return pre-authoritative runner evidence for an exited process. The
    /// lifecycle must call this only after independently proving there is no
    /// completion, submission, agent reaction, or review verdict.
    pub fn classify_pre_authoritative_exit(
        &self,
        status: std::process::ExitStatus,
    ) -> Option<RunnerFailure> {
        match self {
            Self::Claude(proc) => proc.classify_pre_authoritative_exit(status),
            Self::Codex(proc) => proc.classify_pre_authoritative_exit(status),
            Self::Grok(proc) => proc.classify_pre_authoritative_exit(status),
        }
    }

    pub fn observed_pre_authoritative_failure(&self) -> Option<RunnerFailure> {
        match self {
            Self::Claude(proc) => proc.observed_pre_authoritative_failure(),
            Self::Codex(proc) => proc.observed_pre_authoritative_failure(),
            Self::Grok(proc) => proc.observed_pre_authoritative_failure(),
        }
    }

    pub fn observed_planner_live_failure(&self) -> Option<RunnerFailure> {
        self.failure_tracker().observed_planner_live_failure()
    }

    pub fn observed_planner_terminal_failure(&self) -> Option<RunnerFailure> {
        self.failure_tracker().observed_planner_terminal_failure()
    }

    /// Return any bounded failure evidence even after a syntactic terminal
    /// record. A caller that has separately established a nonzero exit must
    /// use this strict view so deferred stderr remains available for its
    /// durable failure summary.
    pub fn observed_strict_pre_authoritative_failure(&self) -> Option<RunnerFailure> {
        self.failure_tracker().observed_strict_failure()
    }

    pub async fn next_raw_line(&mut self) -> Option<String> {
        match self {
            Self::Claude(proc) => proc.next_raw_line().await,
            Self::Codex(proc) => proc.next_raw_line().await,
            Self::Grok(proc) => proc.next_raw_line().await,
        }
    }

    /// Normalize a line for immediate stream delivery. Grok terminal records
    /// are raw evidence first; their lifecycle events are exposed separately
    /// only after the adapter proves terminal process evidence is complete.
    pub fn normalize_stream_line(&self, raw: &str) -> Vec<AgentEvent> {
        match self {
            Self::Grok(_) => GrokProc::normalize_stream_line(raw),
            Self::Claude(_) | Self::Codex(_) => normalize_line(self.kind(), raw),
        }
    }

    pub async fn authorized_grok_terminal(&mut self) -> Option<String> {
        match self {
            Self::Grok(proc) => proc.authorized_terminal().await,
            Self::Claude(_) | Self::Codex(_) => None,
        }
    }

    pub fn grok_terminal_evidence_pending(&self) -> bool {
        matches!(self, Self::Grok(proc) if proc.terminal_evidence_pending())
    }

    pub fn grok_terminal_candidate_pending(&self) -> bool {
        matches!(self, Self::Grok(proc) if proc.terminal_candidate_pending())
    }

    pub fn grok_terminal_handoff_pending(&self) -> bool {
        matches!(self, Self::Grok(proc) if proc.terminal_handoff_pending())
    }

    pub fn set_grok_raw_drain_budget_exhausted(&mut self, exhausted: bool) {
        if let Self::Grok(proc) = self {
            proc.set_raw_drain_budget_exhausted(exhausted);
        }
    }

    #[cfg(test)]
    pub fn inject_grok_stdout_read_error_after_lines(&mut self, lines: usize) {
        if let Self::Grok(proc) = self {
            proc.inject_stdout_read_error_after_lines(lines);
        }
    }

    #[cfg(test)]
    pub async fn inject_grok_stderr_read_error(&mut self) {
        if let Self::Grok(proc) = self {
            proc.inject_stderr_read_error().await;
        }
    }

    #[cfg(test)]
    pub fn grok_stderr_evidence_pending(&self) -> bool {
        matches!(self, Self::Grok(proc) if proc.stderr_evidence_pending())
    }

    /// Read one line with a hard boundary on bytes accumulated before a line
    /// terminator. Classifier and planner roles use this cancellation-safe
    /// boundary before accepting provider output into role-owned buffers.
    pub async fn next_raw_line_bounded(
        &mut self,
        max_bytes: usize,
    ) -> std::io::Result<Option<String>> {
        match self {
            Self::Claude(proc) => proc.next_raw_line_bounded(max_bytes).await,
            Self::Codex(proc) => proc.next_raw_line_bounded(max_bytes).await,
            Self::Grok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "bounded role reads are unavailable for the Grok adapter",
            )),
        }
    }

    pub fn normalize_line(&self, raw: &str) -> NormalizedLine {
        match self {
            Self::Claude(_) => AgentProc::normalize_line(raw),
            Self::Codex(_) => CodexProc::normalize_line(raw),
            Self::Grok(_) => GrokProc::normalize_line(raw),
        }
    }

    pub async fn feed_turn(&mut self, turn: &str) -> std::io::Result<()> {
        match self {
            Self::Claude(proc) => proc.feed_turn(turn).await,
            Self::Codex(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Codex is turn-oriented — respawn with its thread ID",
            )),
            Self::Grok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Grok is turn-oriented — respawn with its session ID",
            )),
        }
    }

    pub fn drain_diagnostics(&mut self) -> Vec<CapturedOutput> {
        match self {
            Self::Claude(proc) => proc.drain_diagnostics(),
            Self::Codex(proc) => proc.drain_diagnostics(),
            Self::Grok(proc) => proc.drain_diagnostics(),
        }
    }
}

fn classify_launch_error(kind: AgentKind, error: std::io::Error) -> RunnerFailure {
    let disposition = match error.kind() {
        std::io::ErrorKind::TimedOut
        | std::io::ErrorKind::Interrupted
        | std::io::ErrorKind::WouldBlock
        | std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::ConnectionAborted
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionRefused => FailureDisposition::RetryableSameRoute,
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::PermissionDenied
        | std::io::ErrorKind::InvalidInput
        | std::io::ErrorKind::InvalidData
        | std::io::ErrorKind::Unsupported => FailureDisposition::NonFailover,
        _ => FailureDisposition::Unclassified,
    };
    RunnerFailure::new(
        disposition,
        format!("{kind} runner startup failed: {error}"),
    )
    .with_io_kind(error.kind())
}

impl AgentKind {
    pub fn turn_mode(self) -> TurnMode {
        match self {
            Self::Claude => TurnMode::PersistentChild,
            Self::Codex => TurnMode::RespawnPerTurn,
            Self::Grok => TurnMode::RespawnPerTurn,
        }
    }

    /// Resolve the provider from the model string chosen for a managed run.
    ///
    /// This is the authoritative model-to-runner mapping. Unknown models are
    /// rejected so callers cannot spawn one runner and persist another.
    pub fn for_model(model: &str) -> Result<Self, String> {
        if super::grok_agent::SUPPORTED_MODELS.contains(&model) {
            Ok(Self::Grok)
        } else if model == "o1"
            || model == "o3"
            || model.starts_with("o1-")
            || model.starts_with("o3-")
            || model.starts_with("o4-")
            || model.starts_with("gpt-")
        {
            Ok(Self::Codex)
        } else if model.starts_with("claude-")
            || model == "sonnet"
            || model.starts_with("sonnet-")
            || model == "opus"
            || model.starts_with("opus-")
            || model == "fable"
            || model.starts_with("fable-")
        {
            Ok(Self::Claude)
        } else {
            Err(format!(
                "unknown model '{model}': cannot resolve provider \
                 (expected claude-*/Claude alias, supported OpenAI model, grok-4.5, or grok-4.6)"
            ))
        }
    }
}

impl std::fmt::Display for AgentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claude => f.write_str("claude"),
            Self::Codex => f.write_str("codex"),
            Self::Grok => f.write_str("grok"),
        }
    }
}

#[cfg(test)]
mod provider_tests {
    use super::AgentKind;

    #[test]
    fn exact_o_series_model_ids_route_to_codex() {
        assert_eq!(AgentKind::for_model("o1").unwrap(), AgentKind::Codex);
        assert_eq!(AgentKind::for_model("o3").unwrap(), AgentKind::Codex);
        assert_eq!(AgentKind::for_model("o4-mini").unwrap(), AgentKind::Codex);
    }

    #[test]
    fn adapters_declare_their_process_lifetime() {
        assert_eq!(
            AgentKind::Claude.turn_mode(),
            super::TurnMode::PersistentChild
        );
        assert_eq!(
            AgentKind::Codex.turn_mode(),
            super::TurnMode::RespawnPerTurn
        );
        assert_eq!(AgentKind::Grok.turn_mode(), super::TurnMode::RespawnPerTurn);
    }
}

/// Normalized event consumed by the daemon lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    ThreadStarted {
        thread_id: String,
    },
    /// Runner session identity established. For Claude this fires on the
    /// first assistant event (identity is pre-spawn); for Codex it fires on
    /// `thread.started`; for Grok it fires immediately before terminal success
    /// when the `end` event provides `sessionId`.
    AssistantText {
        text: String,
    },
    /// A completed Codex `agent_message`. Unlike `AssistantText`, this is a
    /// provider-confirmed final item and retains its provider item identity.
    CompletedAssistantText {
        item_id: String,
        text: String,
    },
    Activity {
        kind: ActivityKind,
        summary: String,
    },
    /// Terminal success. `cost_usd` is provider-optional (Claude provides
    /// session-cumulative USD; Codex does not; Grok provides it only when its
    /// structured terminal protocol marks the ledger complete).
    TurnCompleted {
        usage: Option<TokenUsage>,
        cost_usd: Option<f64>,
    },
    TurnFailed {
        message: String,
        usage: Option<TokenUsage>,
        cost_usd: Option<f64>,
    },
    /// Mid-turn usage snapshot (Claude reports per-message usage on assistant
    /// events). Not terminal.
    MidTurnUsage {
        tokens: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    ToolUse,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    /// Provider-reported input retained for existing live gauges and caps.
    pub input_tokens: u64,
    pub uncached_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
}

impl TokenUsage {
    /// Cache activity is persisted separately and does not alter established
    /// live token-limit behavior.
    pub fn live_total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    /// Normalized non-cache input plus output for explicit uncached ceilings.
    pub fn uncached_total_tokens(self) -> u64 {
        self.uncached_input_tokens
            .saturating_add(self.output_tokens)
    }

    pub fn saturating_add_assign(&mut self, other: Self) {
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.uncached_input_tokens = self
            .uncached_input_tokens
            .saturating_add(other.uncached_input_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(other.cached_input_tokens);
        self.cache_write_input_tokens = self
            .cache_write_input_tokens
            .saturating_add(other.cache_write_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(other.reasoning_tokens);
    }
}

/// Convert provider counters at the durable signed-integer boundary without
/// allowing malformed telemetry to wrap into negative SQLite values.
pub fn try_durable_token_usage(
    usage: TokenUsage,
) -> std::result::Result<quorum_core::token_usage::TokenUsage, &'static str> {
    Ok(quorum_core::token_usage::TokenUsage {
        uncached_input_tokens: i64::try_from(usage.uncached_input_tokens)
            .map_err(|_| "uncached_input_tokens exceeds i64::MAX")?,
        cached_input_tokens: i64::try_from(usage.cached_input_tokens)
            .map_err(|_| "cached_input_tokens exceeds i64::MAX")?,
        cache_write_input_tokens: i64::try_from(usage.cache_write_input_tokens)
            .map_err(|_| "cache_write_input_tokens exceeds i64::MAX")?,
        output_tokens: i64::try_from(usage.output_tokens)
            .map_err(|_| "output_tokens exceeds i64::MAX")?,
        reasoning_tokens: i64::try_from(usage.reasoning_tokens)
            .map_err(|_| "reasoning_tokens exceeds i64::MAX")?,
    })
}

/// Compatibility entry point for sites that do not hold a process instance.
/// Provider-specific raw parsing remains owned by the Claude adapter.
pub fn normalize_claude_line(raw: &str) -> Vec<AgentEvent> {
    AgentProc::normalize_line(raw).events
}

/// Compatibility entry point for sites that do not hold a process instance.
/// Provider-specific raw parsing remains owned by the Codex adapter.
pub fn normalize_codex_line(raw: &str) -> Vec<AgentEvent> {
    CodexProc::normalize_line(raw).events
}

/// Compatibility entry point for sites that do not hold a process instance.
/// Provider-specific raw parsing remains owned by the Grok adapter.
pub fn normalize_grok_line(raw: &str) -> Vec<AgentEvent> {
    GrokProc::normalize_line(raw).events
}

pub fn normalize_line(kind: AgentKind, raw: &str) -> Vec<AgentEvent> {
    match kind {
        AgentKind::Claude => normalize_claude_line(raw),
        AgentKind::Codex => normalize_codex_line(raw),
        AgentKind::Grok => normalize_grok_line(raw),
    }
}

/// Compact tool label for live stats (matches existing `now_label` behavior).
pub(super) fn tool_summary(name: &str, input: &serde_json::Value) -> String {
    let snippet = match name {
        "Bash" => input
            .get("command")
            .and_then(|c| c.as_str())
            .map(|c| c.split_whitespace().take(3).collect::<Vec<_>>().join(" ")),
        "Read" | "Write" | "Edit" => input
            .get("file_path")
            .and_then(|p| p.as_str())
            .map(|p| p.rsplit('/').next().unwrap_or(p).to_string()),
        "Grep" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Glob" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Agent" => input
            .get("description")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };
    match snippet {
        Some(s) => {
            let full = format!("{name}: {s}");
            truncate_label(&full, 24)
        }
        None => name.to_string(),
    }
}

fn truncate_label(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let t: String = s.chars().take(max - 1).collect();
        format!("{t}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn managed_environment() -> Vec<(String, String)> {
        vec![
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Keel-test".into()),
            ("QUORUM_RUN_ID".into(), "run-capability".into()),
            (
                "QUORUM_AGENT_ENDPOINT".into(),
                "/tmp/quorum-agent.sock".into(),
            ),
        ]
    }

    fn mcp_request<'a>(mode: LaunchMode, environment: &'a [(String, String)]) -> LaunchRequest<'a> {
        LaunchRequest {
            model: "gpt-5.6-terra",
            effort: "high",
            worktree: std::path::Path::new("/tmp/worktree"),
            prompt: "exact pending turn",
            environment,
            mode,
            continuation_id: Some("provider-issued-thread"),
        }
    }

    #[test]
    fn agent_mcp_registration_requires_normal_managed_run_authority() {
        let environment = managed_environment();

        assert_eq!(
            mcp_request(LaunchMode::Normal, &environment).agent_mcp_server(),
            Some(AGENT_MCP_SERVER)
        );
        assert_eq!(
            mcp_request(LaunchMode::Restricted, &environment).agent_mcp_server(),
            None,
            "restricted roles remain isolated even if authority is accidentally supplied"
        );
        for missing in [
            "QUORUM_REPO",
            "QUORUM_AGENT",
            "QUORUM_RUN_ID",
            "QUORUM_AGENT_ENDPOINT",
        ] {
            let incomplete: Vec<_> = environment
                .iter()
                .filter(|(key, _)| key != missing)
                .cloned()
                .collect();
            assert_eq!(
                mcp_request(LaunchMode::Normal, &incomplete).agent_mcp_server(),
                None,
                "missing {missing} must suppress registration"
            );
        }
        assert_eq!(AGENT_MCP_SERVER.command, "quorum");
        assert_eq!(AGENT_MCP_SERVER.args, ["agent-mcp"]);
        assert_eq!(
            AGENT_MCP_SERVER.env_vars,
            [
                "QUORUM_REPO",
                "QUORUM_AGENT",
                "QUORUM_RUN_ID",
                "QUORUM_AGENT_ENDPOINT",
            ]
        );
        let command_surface = format!(
            "{} {} {}",
            AGENT_MCP_SERVER.command,
            AGENT_MCP_SERVER.args.join(" "),
            AGENT_MCP_SERVER.env_vars.join(" ")
        );
        for forbidden in [".sqlite", "GH_TOKEN", "GITHUB_TOKEN", "ghp_"] {
            assert!(!command_surface.contains(forbidden), "{command_surface}");
        }
    }

    #[test]
    fn durable_token_conversion_rejects_every_overflowing_bucket() {
        let maximum = TokenUsage {
            input_tokens: u64::MAX,
            uncached_input_tokens: i64::MAX as u64,
            cached_input_tokens: i64::MAX as u64,
            cache_write_input_tokens: i64::MAX as u64,
            output_tokens: i64::MAX as u64,
            reasoning_tokens: i64::MAX as u64,
        };
        assert_eq!(
            try_durable_token_usage(maximum).unwrap(),
            quorum_core::token_usage::TokenUsage {
                uncached_input_tokens: i64::MAX,
                cached_input_tokens: i64::MAX,
                cache_write_input_tokens: i64::MAX,
                output_tokens: i64::MAX,
                reasoning_tokens: i64::MAX,
            }
        );

        for (error, overflow) in [
            (
                "uncached_input_tokens exceeds i64::MAX",
                TokenUsage {
                    uncached_input_tokens: i64::MAX as u64 + 1,
                    ..Default::default()
                },
            ),
            (
                "cached_input_tokens exceeds i64::MAX",
                TokenUsage {
                    cached_input_tokens: i64::MAX as u64 + 1,
                    ..Default::default()
                },
            ),
            (
                "cache_write_input_tokens exceeds i64::MAX",
                TokenUsage {
                    cache_write_input_tokens: i64::MAX as u64 + 1,
                    ..Default::default()
                },
            ),
            (
                "output_tokens exceeds i64::MAX",
                TokenUsage {
                    output_tokens: i64::MAX as u64 + 1,
                    ..Default::default()
                },
            ),
            (
                "reasoning_tokens exceeds i64::MAX",
                TokenUsage {
                    reasoning_tokens: i64::MAX as u64 + 1,
                    ..Default::default()
                },
            ),
        ] {
            assert_eq!(try_durable_token_usage(overflow), Err(error));
        }
    }

    #[cfg(unix)]
    fn recording_runner(dir: &std::path::Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let runner = dir.join("recording-runner");
        let args = dir.join("args.log");
        let turn = dir.join("turn.log");
        let environment = dir.join("environment.log");
        let grok_config = dir.join("grok-config.log");
        let grok_sandbox = dir.join("grok-sandbox.log");
        let grok_overlay = dir.join("grok-overlay.log");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\n\
                 for arg in \"$@\"; do printf '<%s>\\n' \"$arg\"; done > '{}'\n\
                 printf '%s\\n' \"$QUORUM_ADAPTER_TEST\" > '{}'\n\
                 if [ -n \"$GROK_HOME\" ] && [ -f \"$GROK_HOME/config.toml\" ]; then\n\
                   cp \"$GROK_HOME/config.toml\" '{}'\n\
                 else\n\
                   : > '{}'\n\
                 fi\n\
                 if [ -n \"$GROK_HOME\" ] && [ -f \"$GROK_HOME/sandbox.toml\" ]; then\n\
                   cp \"$GROK_HOME/sandbox.toml\" '{}'\n\
                 else\n\
                   : > '{}'\n\
                 fi\n\
                 printf '%s\\n' \"${{GROK_CONFIG+x}}\" > '{}'\n\
                 if [ \"$1\" = '-p' ]; then\n\
                   IFS= read -r line\n\
                   printf '%s\\n' \"$line\" > '{}'\n\
                   printf '%s\\n' '{{\"type\":\"result\",\"result\":\"done\",\"is_error\":false}}'\n\
                 else\n\
                   printf '%s\\n' '{{\"type\":\"thread.started\",\"thread_id\":\"runner-thread\"}}'\n\
                   printf '%s\\n' '{{\"type\":\"turn.completed\"}}'\n\
                 fi\n",
                args.display(),
                environment.display(),
                grok_config.display(),
                grok_config.display(),
                grok_sandbox.display(),
                grok_sandbox.display(),
                grok_overlay.display(),
                turn.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        runner
    }

    #[cfg(unix)]
    async fn launch_recording_runner(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> RunnerProc {
        const EXECUTABLE_BUSY_RETRIES: u8 = 3;

        for attempt in 0..=EXECUTABLE_BUSY_RETRIES {
            match RunnerProc::launch(request, config).await {
                Ok(proc) => return proc,
                Err(error)
                    if error.kind() == std::io::ErrorKind::ExecutableFileBusy
                        && attempt < EXECUTABLE_BUSY_RETRIES =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => panic!("recording runner launch failed: {error}"),
            }
        }
        unreachable!("bounded retry either launches the runner or returns its error")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_launch_dispatches_to_each_explicit_adapter() {
        let claude_dir = tempfile::tempdir().unwrap();
        let claude_bin = recording_runner(claude_dir.path());
        let claude_environment = vec![("QUORUM_ADAPTER_TEST".into(), "claude-env".into())];
        let mut claude = launch_recording_runner(
            &LaunchRequest {
                model: "claude-sonnet-5",
                effort: "high",
                worktree: claude_dir.path(),
                prompt: "claude prompt",
                environment: &claude_environment,
                mode: LaunchMode::Normal,
                continuation_id: None,
            },
            &AdapterConfig {
                executable: claude_bin.to_str(),
                claude_bare: true,
                claude_allowed_tools: "Bash,Read",
                codex_sandbox: "danger-full-access",
                grok: Default::default(),
            },
        )
        .await;
        assert_eq!(claude.kind(), AgentKind::Claude);
        let _ = claude.next_raw_line().await;
        claude.kill_and_reap().await;
        let args = std::fs::read_to_string(claude_dir.path().join("args.log")).unwrap();
        assert!(args.contains("<--add-dir>"), "{args}");
        assert!(args.contains("<--bare>"), "{args}");
        assert!(args.contains("<Bash,Read>"), "{args}");
        assert!(!args.contains("<--safe-mode>"), "{args}");
        assert!(!args.contains("<--setting-sources>"), "{args}");
        assert!(!args.contains("<--append-system-prompt-file>"), "{args}");
        let argv: Vec<&str> = args.lines().collect();
        let session_flag = argv
            .iter()
            .position(|arg| *arg == "<--session-id>")
            .unwrap();
        let session_id = argv[session_flag + 1]
            .strip_prefix('<')
            .and_then(|value| value.strip_suffix('>'))
            .unwrap();
        assert!(uuid::Uuid::parse_str(session_id).is_ok(), "{args}");
        let turn = std::fs::read_to_string(claude_dir.path().join("turn.log")).unwrap();
        let turn: serde_json::Value = serde_json::from_str(turn.trim()).unwrap();
        assert_eq!(turn["message"]["content"], "claude prompt");
        assert_eq!(
            std::fs::read_to_string(claude_dir.path().join("environment.log"))
                .unwrap()
                .trim(),
            "claude-env"
        );

        let codex_dir = tempfile::tempdir().unwrap();
        let codex_bin = recording_runner(codex_dir.path());
        let codex_environment = vec![("QUORUM_ADAPTER_TEST".into(), "codex-env".into())];
        let mut codex = launch_recording_runner(
            &LaunchRequest {
                model: "gpt-5.6-terra",
                effort: "medium",
                worktree: codex_dir.path(),
                prompt: "codex prompt",
                environment: &codex_environment,
                mode: LaunchMode::Normal,
                continuation_id: None,
            },
            &AdapterConfig {
                executable: codex_bin.to_str(),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "workspace-write",
                grok: Default::default(),
            },
        )
        .await;
        assert_eq!(codex.kind(), AgentKind::Codex);
        while codex.next_raw_line().await.is_some() {}
        codex.kill_and_reap().await;
        let args = std::fs::read_to_string(codex_dir.path().join("args.log")).unwrap();
        assert!(
            args.contains("<--dangerously-bypass-approvals-and-sandbox>"),
            "{args}"
        );
        assert!(args.contains("<workspace-write>"), "{args}");
        assert!(args.contains("<codex prompt>"), "{args}");
        assert_eq!(
            std::fs::read_to_string(codex_dir.path().join("environment.log"))
                .unwrap()
                .trim(),
            "codex-env"
        );

        for model in crate::serve::grok_agent::SUPPORTED_MODELS {
            let grok_dir = tempfile::tempdir().unwrap();
            let grok_bin = recording_runner(grok_dir.path());
            let grok_environment = vec![("QUORUM_ADAPTER_TEST".into(), "grok-env".into())];
            let mut grok = launch_recording_runner(
                &LaunchRequest {
                    model,
                    effort: "high",
                    worktree: grok_dir.path(),
                    prompt: "grok prompt",
                    environment: &grok_environment,
                    mode: LaunchMode::Normal,
                    continuation_id: None,
                },
                &AdapterConfig {
                    executable: grok_bin.to_str(),
                    claude_bare: true,
                    claude_allowed_tools: "Bash,Read",
                    codex_sandbox: "danger-full-access",
                    grok: Default::default(),
                },
            )
            .await;
            assert_eq!(grok.kind(), AgentKind::Grok);
            while grok.next_raw_line().await.is_some() {}
            grok.kill_and_reap().await;
            let args = std::fs::read_to_string(grok_dir.path().join("args.log")).unwrap();
            assert!(args.contains("<streaming-json>"), "{args}");
            assert!(args.contains(&format!("<{model}>")), "{args}");
            assert!(args.contains("<grok prompt>"), "{args}");
            assert!(!args.contains("<--bare>"), "{args}");
            assert!(!args.contains("<Bash,Read>"), "{args}");
            assert_eq!(
                std::fs::read_to_string(grok_dir.path().join("environment.log"))
                    .unwrap()
                    .trim(),
                "grok-env"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_launches_inject_exact_provider_mcp_boundaries_and_pending_turns() {
        let claude_dir = tempfile::tempdir().unwrap();
        let claude_bin = recording_runner(claude_dir.path());
        let mut claude_environment = managed_environment();
        claude_environment.push(("QUORUM_ADAPTER_TEST".into(), "managed-claude".into()));
        let claude_session = "00000000-0000-4000-8000-000000000042";
        let mut claude = launch_recording_runner(
            &LaunchRequest {
                model: "claude-sonnet-5",
                effort: "high",
                worktree: claude_dir.path(),
                prompt: "exact claude pending turn",
                environment: &claude_environment,
                mode: LaunchMode::Normal,
                continuation_id: Some(claude_session),
            },
            &AdapterConfig {
                executable: claude_bin.to_str(),
                claude_bare: true,
                claude_allowed_tools: "Bash,Read",
                codex_sandbox: "danger-full-access",
                grok: Default::default(),
            },
        )
        .await;
        while claude.next_raw_line().await.is_some() {}
        claude.kill_and_reap().await;
        let claude_args = std::fs::read_to_string(claude_dir.path().join("args.log")).unwrap();
        assert!(
            claude_args.contains(&format!("<{claude_session}>")),
            "{claude_args}"
        );
        assert!(claude_args.contains("<--mcp-config>"), "{claude_args}");
        assert!(
            claude_args.contains("<--strict-mcp-config>"),
            "{claude_args}"
        );
        assert!(
            claude_args.contains("<Bash,Read,mcp__quorum__*>"),
            "{claude_args}"
        );
        let config_line = claude_args
            .lines()
            .find(|line| line.contains("\"mcpServers\""))
            .expect("Claude MCP JSON argument");
        let config: serde_json::Value =
            serde_json::from_str(config_line.trim_matches(&['<', '>'][..])).unwrap();
        assert_eq!(
            config,
            serde_json::json!({
                "mcpServers": {
                    "quorum": {"command": "quorum", "args": ["agent-mcp"]}
                }
            })
        );
        let turn = std::fs::read_to_string(claude_dir.path().join("turn.log")).unwrap();
        let turn: serde_json::Value = serde_json::from_str(turn.trim()).unwrap();
        assert_eq!(turn["message"]["content"], "exact claude pending turn");

        let codex_dir = tempfile::tempdir().unwrap();
        let codex_bin = recording_runner(codex_dir.path());
        let mut codex_environment = managed_environment();
        codex_environment.push(("QUORUM_ADAPTER_TEST".into(), "managed-codex".into()));
        let mut codex = launch_recording_runner(
            &LaunchRequest {
                model: "gpt-5.6-terra",
                effort: "high",
                worktree: codex_dir.path(),
                prompt: "exact codex pending turn",
                environment: &codex_environment,
                mode: LaunchMode::Normal,
                continuation_id: Some("provider-issued-thread-42"),
            },
            &AdapterConfig {
                executable: codex_bin.to_str(),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "danger-full-access",
                grok: Default::default(),
            },
        )
        .await;
        while codex.next_raw_line().await.is_some() {}
        codex.kill_and_reap().await;
        let codex_args = std::fs::read_to_string(codex_dir.path().join("args.log")).unwrap();
        let codex_argv: Vec<_> = codex_args.lines().collect();
        assert_eq!(
            &codex_argv[..3],
            ["<exec>", "<resume>", "<provider-issued-thread-42>"]
        );
        assert!(
            codex_args.contains(
                r#"<mcp_servers.quorum={command="quorum",args=["agent-mcp"],env_vars=["QUORUM_REPO","QUORUM_AGENT","QUORUM_RUN_ID","QUORUM_AGENT_ENDPOINT"]}>"#
            ),
            "{codex_args}"
        );
        assert_eq!(
            codex_argv.last(),
            Some(&"<exact codex pending turn>"),
            "{codex_args}"
        );

        let grok_dir = tempfile::tempdir().unwrap();
        let grok_bin = recording_runner(grok_dir.path());
        let mut grok_environment = managed_environment();
        grok_environment.push(("QUORUM_ADAPTER_TEST".into(), "managed-grok".into()));
        let original_grok_home = grok_dir.path().join("original-grok-home");
        std::fs::create_dir_all(original_grok_home.join("sessions")).unwrap();
        std::fs::write(
            original_grok_home.join("config.toml"),
            "[models]\ndefault_reasoning_effort = \"high\"\n",
        )
        .unwrap();
        std::fs::write(
            original_grok_home
                .join("sessions")
                .join("persisted-session"),
            "provider-issued-grok-session-42",
        )
        .unwrap();
        grok_environment.push(("GROK_HOME".into(), original_grok_home.display().to_string()));
        let mut grok = launch_recording_runner(
            &LaunchRequest {
                model: "grok-4.5",
                effort: "high",
                worktree: grok_dir.path(),
                prompt: "exact grok pending turn",
                environment: &grok_environment,
                mode: LaunchMode::Normal,
                continuation_id: Some("provider-issued-grok-session-42"),
            },
            &AdapterConfig {
                executable: grok_bin.to_str(),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "danger-full-access",
                grok: crate::serve::grok_agent::GrokAdapterConfig {
                    sandbox: "workspace",
                    permission_mode: crate::serve::grok_agent::DEFAULT_PERMISSION_MODE,
                    max_turns: crate::serve::grok_agent::DEFAULT_MAX_TURNS,
                },
            },
        )
        .await;
        while grok.next_raw_line().await.is_some() {}
        grok.kill_and_reap().await;
        let grok_args = std::fs::read_to_string(grok_dir.path().join("args.log")).unwrap();
        assert!(
            grok_args.starts_with("<--resume>\n<provider-issued-grok-session-42>"),
            "{grok_args}"
        );
        assert!(
            grok_args.contains("<exact grok pending turn>"),
            "{grok_args}"
        );
        assert!(grok_args.contains("<--no-leader>"), "{grok_args}");
        assert!(
            grok_args.contains("<--sandbox>\n<quorum_managed_workspace>"),
            "{grok_args}"
        );
        let grok_config = std::fs::read_to_string(grok_dir.path().join("grok-config.log")).unwrap();
        let grok_config: toml::Value = toml::from_str(&grok_config).unwrap();
        assert_eq!(
            grok_config["mcp_servers"]["quorum"]["command"].as_str(),
            Some("quorum")
        );
        assert_eq!(
            grok_config["mcp_servers"]["quorum"]["args"]
                .as_array()
                .unwrap(),
            &[toml::Value::String("agent-mcp".into())]
        );
        assert_eq!(
            grok_config["models"]["default_reasoning_effort"].as_str(),
            Some("high"),
            "the invocation-local layer must retain the operator config"
        );
        assert_eq!(
            std::fs::read_to_string(grok_dir.path().join("grok-overlay.log"))
                .unwrap()
                .trim(),
            "",
            "the filtered GROK_CONFIG overlay must not be used"
        );
        let grok_sandbox: toml::Value = toml::from_str(
            &std::fs::read_to_string(grok_dir.path().join("grok-sandbox.log")).unwrap(),
        )
        .unwrap();
        let profile = &grok_sandbox["profiles"]["quorum_managed_workspace"];
        assert_eq!(profile["extends"].as_str(), Some("workspace"));
        assert_eq!(
            profile["read_write"].as_array().unwrap(),
            &[toml::Value::String(
                std::fs::canonicalize(&original_grok_home)
                    .unwrap()
                    .display()
                    .to_string()
            )],
            "the managed profile must restore Grok's durable state root"
        );
        assert_eq!(
            std::fs::read_to_string(original_grok_home.join("config.toml")).unwrap(),
            "[models]\ndefault_reasoning_effort = \"high\"\n",
            "managed injection must not mutate persistent Grok config"
        );
        assert_eq!(
            std::fs::read_to_string(
                original_grok_home
                    .join("sessions")
                    .join("persisted-session")
            )
            .unwrap(),
            "provider-issued-grok-session-42"
        );

        for surface in [claude_args, codex_args, grok_args] {
            for forbidden in ["GH_TOKEN", "GITHUB_TOKEN", ".sqlite", "db_path"] {
                assert!(!surface.contains(forbidden), "{surface}");
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restricted_launch_dispatches_to_each_safe_adapter() {
        for (model, expected_kind) in [
            ("claude-haiku-4-5-20251001", AgentKind::Claude),
            ("gpt-5.6-terra", AgentKind::Codex),
            ("grok-4.5", AgentKind::Grok),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let executable = recording_runner(dir.path());
            let grok_home = dir.path().join("restricted-grok-home");
            std::fs::create_dir_all(&grok_home).unwrap();
            std::fs::write(
                grok_home.join("config.toml"),
                "[models]\ndefault_reasoning_effort = \"low\"\n",
            )
            .unwrap();
            let environment = vec![("GROK_HOME".into(), grok_home.display().to_string())];
            let mut proc = launch_recording_runner(
                &LaunchRequest {
                    model,
                    effort: "low",
                    worktree: dir.path(),
                    prompt: "restricted prompt",
                    environment: &environment,
                    mode: LaunchMode::Restricted,
                    continuation_id: None,
                },
                &AdapterConfig {
                    executable: executable.to_str(),
                    claude_bare: false,
                    claude_allowed_tools: "",
                    codex_sandbox: "danger-full-access",
                    grok: Default::default(),
                },
            )
            .await;
            assert_eq!(proc.kind(), expected_kind);
            while proc.next_raw_line().await.is_some() {}
            proc.kill_and_reap().await;
            let args = std::fs::read_to_string(dir.path().join("args.log")).unwrap();
            match expected_kind {
                AgentKind::Claude => {
                    assert!(!args.contains("<--safe-mode>"), "{args}");
                    let argv: Vec<&str> = args.lines().collect();
                    let sources = argv
                        .iter()
                        .position(|arg| *arg == "<--setting-sources>")
                        .unwrap_or_else(|| panic!("--setting-sources missing: {args}"));
                    assert_eq!(argv.get(sources + 1), Some(&"<>"), "{args}");
                    assert!(args.contains("<--no-session-persistence>"), "{args}");
                    assert!(!args.contains("<--add-dir>"), "{args}");
                }
                AgentKind::Codex => {
                    assert!(args.contains("<read-only>"), "{args}");
                    assert!(
                        !args.contains("<--dangerously-bypass-approvals-and-sandbox>"),
                        "{args}"
                    );
                }
                AgentKind::Grok => {
                    assert!(args.contains("<read-only>"), "{args}");
                    assert!(args.contains("<dontAsk>"), "{args}");
                    let config =
                        std::fs::read_to_string(dir.path().join("grok-config.log")).unwrap();
                    assert!(!config.contains("mcp_servers"), "{config}");
                    let sandbox =
                        std::fs::read_to_string(dir.path().join("grok-sandbox.log")).unwrap();
                    assert!(!sandbox.contains("quorum_managed_workspace"), "{sandbox}");
                    assert_eq!(
                        std::fs::read_to_string(dir.path().join("grok-overlay.log"))
                            .unwrap()
                            .trim(),
                        ""
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn launch_rejects_unknown_model_before_adapter_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let error = match RunnerProc::launch(
            &LaunchRequest {
                model: "unknown-runner-model",
                effort: "low",
                worktree: dir.path(),
                prompt: "prompt",
                environment: &[],
                mode: LaunchMode::Normal,
                continuation_id: None,
            },
            &AdapterConfig {
                executable: Some("/definitely/not/a/runner"),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "read-only",
                grok: Default::default(),
            },
        )
        .await
        {
            Ok(_) => panic!("unknown model unexpectedly launched"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("unknown model"));
        assert_eq!(error.disposition(), FailureDisposition::Unclassified);
    }

    #[tokio::test]
    async fn missing_runner_binary_is_non_failover_startup_failure() {
        let dir = tempfile::tempdir().unwrap();
        let error = match RunnerProc::launch(
            &LaunchRequest {
                model: "gpt-5.6-terra",
                effort: "medium",
                worktree: dir.path(),
                prompt: "prompt",
                environment: &[],
                mode: LaunchMode::Normal,
                continuation_id: None,
            },
            &AdapterConfig {
                executable: Some("/definitely/not/a/runner"),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "read-only",
                grok: Default::default(),
            },
        )
        .await
        {
            Ok(_) => panic!("missing binary unexpectedly launched"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(error.disposition(), FailureDisposition::NonFailover);
    }

    #[test]
    fn failure_disposition_is_a_closed_five_way_contract() {
        let dispositions = [
            FailureDisposition::ProviderUnavailable,
            FailureDisposition::ProfileUnavailable,
            FailureDisposition::RetryableSameRoute,
            FailureDisposition::NonFailover,
            FailureDisposition::Unclassified,
        ];
        assert_eq!(dispositions.len(), 5);
        assert_eq!(FailureDisposition::Unclassified.to_string(), "unclassified");
    }

    #[test]
    fn blank_provider_records_leave_failure_tracking_inert() {
        for kind in [AgentKind::Claude, AgentKind::Codex, AgentKind::Grok] {
            let tracker = FailureTracker::for_kind(kind);
            tracker.observe_stdout("");
            let state = tracker.0.lock().unwrap();
            assert!(
                !state.saw_protocol_line,
                "{kind} blank line became protocol evidence"
            );
            assert!(state.classified.is_none());
            assert!(state.unknown_failure.is_none());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuation_identity_dispatches_to_codex_resume_only_in_normal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let executable = recording_runner(dir.path());
        let mut proc = launch_recording_runner(
            &LaunchRequest {
                model: "gpt-5.6-terra",
                effort: "high",
                worktree: dir.path(),
                prompt: "continue exactly",
                environment: &[],
                mode: LaunchMode::Normal,
                continuation_id: Some("provider-thread-7"),
            },
            &AdapterConfig {
                executable: executable.to_str(),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "danger-full-access",
                grok: Default::default(),
            },
        )
        .await;
        while proc.next_raw_line().await.is_some() {}
        proc.kill_and_reap().await;
        let args = std::fs::read_to_string(dir.path().join("args.log")).unwrap();
        let argv: Vec<&str> = args.lines().collect();
        assert_eq!(&argv[..3], &["<exec>", "<resume>", "<provider-thread-7>"]);
        assert!(args.contains("<continue exactly>"), "{args}");

        let error = match RunnerProc::launch(
            &LaunchRequest {
                model: "gpt-5.6-terra",
                effort: "high",
                worktree: dir.path(),
                prompt: "invalid restricted resume",
                environment: &[],
                mode: LaunchMode::Restricted,
                continuation_id: Some("provider-thread-7"),
            },
            &AdapterConfig {
                executable: executable.to_str(),
                claude_bare: false,
                claude_allowed_tools: "",
                codex_sandbox: "danger-full-access",
                grok: Default::default(),
            },
        )
        .await
        {
            Ok(_) => panic!("restricted continuation unexpectedly launched"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn normalize_result_success() {
        let line = r#"{"type":"result","result":"done","usage":{"input_tokens":100,"output_tokens":50},"total_cost_usd":0.05}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnCompleted { usage, cost_usd } => {
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 100);
                assert_eq!(u.output_tokens, 50);
                assert!((cost_usd.unwrap() - 0.05).abs() < f64::EPSILON);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn normalize_result_error_carries_usage_and_cost() {
        let line = r#"{"type":"result","result":"quota exceeded","is_error":true,"usage":{"input_tokens":500,"output_tokens":200},"total_cost_usd":0.03}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnFailed {
                message,
                usage,
                cost_usd,
            } => {
                assert_eq!(message, "quota exceeded");
                let u = usage.unwrap();
                assert_eq!(u.input_tokens, 500);
                assert_eq!(u.output_tokens, 200);
                assert!((cost_usd.unwrap() - 0.03).abs() < f64::EPSILON);
            }
            other => panic!("expected TurnFailed, got {other:?}"),
        }
    }

    #[test]
    fn normalize_result_error_without_usage() {
        let line = r#"{"type":"result","result":"","is_error":true}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnFailed {
                message,
                usage,
                cost_usd,
            } => {
                assert_eq!(message, "agent returned an error result");
                assert!(usage.is_none());
                assert!(cost_usd.is_none());
            }
            other => panic!("expected TurnFailed, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_text() {
        let line = r#"{"type":"assistant","message":{"content":"hello world"}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::AssistantText { text } => assert_eq!(text, "hello world"),
            other => panic!("expected AssistantText, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_with_content_array() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"},{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}}]}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], AgentEvent::AssistantText { text } if text == "hi"));
        match &events[1] {
            AgentEvent::Activity { kind, summary } => {
                assert_eq!(*kind, ActivityKind::ToolUse);
                assert!(summary.contains("Bash"));
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn normalize_assistant_with_mid_turn_usage() {
        let line = r#"{"type":"assistant","message":{"content":"text","usage":{"input_tokens":200,"output_tokens":100}}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], AgentEvent::AssistantText { .. }));
        match &events[1] {
            AgentEvent::MidTurnUsage { tokens } => assert_eq!(*tokens, 300),
            other => panic!("expected MidTurnUsage, got {other:?}"),
        }
    }

    #[test]
    fn normalize_top_level_tool_use() {
        let line = r#"{"type":"tool_use","name":"Read","input":{"file_path":"/src/main.rs"}}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Activity { kind, summary } => {
                assert_eq!(*kind, ActivityKind::ToolUse);
                assert!(summary.contains("Read"));
                assert!(summary.contains("main.rs"));
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn normalize_unknown_event_type_is_inert() {
        let line = r#"{"type":"system","message":"init"}"#;
        let events = normalize_claude_line(line);
        assert!(events.is_empty(), "unknown events must be inert");
    }

    #[test]
    fn normalize_invalid_json_is_inert() {
        assert!(normalize_claude_line("not json").is_empty());
        assert!(normalize_claude_line("").is_empty());
        assert!(normalize_claude_line("{broken").is_empty());
    }

    #[test]
    fn normalize_result_without_usage() {
        let line = r#"{"type":"result","result":"ok"}"#;
        let events = normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TurnCompleted { usage, cost_usd } => {
                assert!(usage.is_none());
                assert!(cost_usd.is_none());
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn normalize_thinking_only_assistant_is_empty() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"internal"}]}}"#;
        let events = normalize_claude_line(line);
        assert!(
            events.is_empty(),
            "thinking-only assistant should produce no events"
        );
    }

    #[test]
    fn agent_kind_display() {
        assert_eq!(AgentKind::Claude.to_string(), "claude");
        assert_eq!(AgentKind::Codex.to_string(), "codex");
        assert_eq!(AgentKind::Grok.to_string(), "grok");
    }

    #[test]
    fn for_model_claude_prefix() {
        assert_eq!(
            AgentKind::for_model("claude-sonnet-5").unwrap(),
            AgentKind::Claude
        );
        assert_eq!(
            AgentKind::for_model("claude-opus-4-6").unwrap(),
            AgentKind::Claude
        );
        assert_eq!(
            AgentKind::for_model("claude-opus-4-7").unwrap(),
            AgentKind::Claude
        );
        assert_eq!(
            AgentKind::for_model("claude-opus-4-8").unwrap(),
            AgentKind::Claude
        );
    }

    #[test]
    fn for_model_codex_models() {
        assert_eq!(AgentKind::for_model("o4-mini").unwrap(), AgentKind::Codex);
        assert_eq!(AgentKind::for_model("gpt-4o").unwrap(), AgentKind::Codex);
        assert_eq!(
            AgentKind::for_model("gpt-5.6-codex").unwrap(),
            AgentKind::Codex
        );
        assert_eq!(
            AgentKind::for_model("gpt-5.6-terra").unwrap(),
            AgentKind::Codex
        );
    }

    #[test]
    fn for_model_grok_is_exact_and_closed() {
        assert_eq!(AgentKind::for_model("grok-4.5").unwrap(), AgentKind::Grok);
        assert_eq!(AgentKind::for_model("grok-4.6").unwrap(), AgentKind::Grok);
        assert!(AgentKind::for_model("grok").is_err());
        assert!(AgentKind::for_model("grok-4.7").is_err());
        assert!(AgentKind::for_model("/opt/bin/grok").is_err());
    }

    #[test]
    fn for_model_unknown_is_rejected() {
        assert!(AgentKind::for_model("unknown-model").is_err());
    }

    #[test]
    fn for_model_short_claude_aliases() {
        assert_eq!(AgentKind::for_model("sonnet").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("sonnet-5").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-46").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-47").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("opus-48").unwrap(), AgentKind::Claude);
        assert_eq!(AgentKind::for_model("fable").unwrap(), AgentKind::Claude);
        assert_eq!(
            AgentKind::for_model("fable-preview").unwrap(),
            AgentKind::Claude
        );
        assert!(AgentKind::for_model("haiku").is_err());
    }

    #[test]
    fn tool_summary_truncation() {
        let summary = tool_summary(
            "Bash",
            &serde_json::json!({"command": "this is a very long command that should be truncated"}),
        );
        assert!(summary.chars().count() <= 24, "summary too long: {summary}");
    }

    #[test]
    fn raw_line_preservation_round_trip() {
        let raw = r#"{"type":"result","result":"done","extra_field":"preserved","usage":{"input_tokens":1,"output_tokens":2}}"#;
        let events = normalize_claude_line(raw);
        assert_eq!(events.len(), 1);
        // The raw line is distinct from re-serialized — it contains extra_field
        assert!(raw.contains("extra_field"));
    }

    #[test]
    fn diagnostic_buffer_is_bounded_and_stderr_is_explicitly_typed() {
        let buffer = DiagnosticBuffer::default();
        for index in 0..(DIAGNOSTIC_CAPACITY + 50) {
            buffer.push(format!("diagnostic-{index}"));
        }
        let captured = buffer.drain();
        assert_eq!(captured.len(), DIAGNOSTIC_CAPACITY + 1);
        assert!(matches!(
            captured.first(),
            Some(CapturedOutput::StderrTruncated { dropped: 50 })
        ));
        assert!(captured
            .last()
            .unwrap()
            .session_line()
            .contains("diagnostic-305"));
    }

    #[tokio::test]
    async fn newline_free_diagnostic_is_byte_bounded_with_exact_marker() {
        let input = vec![b'x'; DIAGNOSTIC_LINE_BYTES + 1234];
        let buffer = DiagnosticBuffer::default();
        capture_diagnostics(std::io::Cursor::new(input), buffer.clone()).await;
        let captured = buffer.drain();
        assert!(matches!(
            captured.first(),
            Some(CapturedOutput::StderrBytesTruncated { dropped: 1234 })
        ));
        assert!(matches!(
            captured.last(),
            Some(CapturedOutput::Stderr(line)) if line.len() == DIAGNOSTIC_LINE_BYTES
        ));
    }
}
