//! Codex CLI process management: spec, command builder, spawn.
//!
//! Analogous to `agent.rs` (Claude CLI boundary) but targeting
//! `codex exec --json`. The Codex CLI does not use session UUIDs for first
//! runs — the thread ID is provider-issued and emitted in the first
//! `thread.started` JSONL event.

use super::codex_stream::{self, Event};
use super::runner::{
    capture_diagnostics, tool_summary, ActivityKind, AdapterConfig, AgentEvent, AgentKind,
    AgentMcpServer, CapturedOutput, DiagnosticBuffer, FailureDisposition, FailureObservation,
    FailureTracker, LaunchMode, LaunchRequest, NormalizedLine, RunnerFailure, TokenUsage,
};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

pub struct CodexSpec {
    pub model: String,
    pub effort: String,
    pub sandbox: String,
    pub worktree: PathBuf,
    pub prompt: String,
    pub env_vars: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Command builders (pinned argument shapes)
// ---------------------------------------------------------------------------

/// Build the argument list for `codex exec --json` (first turn).
pub fn exec_args(spec: &CodexSpec) -> Vec<String> {
    exec_args_configured(spec, None)
}

fn exec_args_configured(spec: &CodexSpec, agent_mcp: Option<AgentMcpServer>) -> Vec<String> {
    let mut args = vec![
        "exec".into(),
        "--json".into(),
        "--model".into(),
        spec.model.clone(),
        "-c".into(),
        format!("model_reasoning_effort={}", spec.effort),
        "-s".into(),
        spec.sandbox.clone(),
        "--dangerously-bypass-approvals-and-sandbox".into(),
        "-C".into(),
        spec.worktree.display().to_string(),
        "--skip-git-repo-check".into(),
        "--ignore-user-config".into(),
    ];
    append_agent_mcp_override(&mut args, agent_mcp);
    args.push(spec.prompt.clone());
    args
}

/// Build the argument list for `codex exec resume <thread_id> --json`
/// (continuation turn).
pub fn resume_args(thread_id: &str, model: &str, effort: &str, prompt: &str) -> Vec<String> {
    resume_args_configured(thread_id, model, effort, prompt, None)
}

fn resume_args_configured(
    thread_id: &str,
    model: &str,
    effort: &str,
    prompt: &str,
    agent_mcp: Option<AgentMcpServer>,
) -> Vec<String> {
    let mut args = vec![
        "exec".into(),
        "resume".into(),
        thread_id.into(),
        "--json".into(),
        "--model".into(),
        model.into(),
        "-c".into(),
        format!("model_reasoning_effort={effort}"),
        "--dangerously-bypass-approvals-and-sandbox".into(),
        "--skip-git-repo-check".into(),
        "--ignore-user-config".into(),
    ];
    append_agent_mcp_override(&mut args, agent_mcp);
    args.push(prompt.into());
    args
}

fn append_agent_mcp_override(args: &mut Vec<String>, server: Option<AgentMcpServer>) {
    let Some(server) = server else {
        return;
    };
    let server_args = server
        .args
        .iter()
        .map(|arg| serde_json::to_string(arg).expect("static MCP argument serializes"))
        .collect::<Vec<_>>()
        .join(",");
    let env_vars = server
        .env_vars
        .iter()
        .map(|name| serde_json::to_string(name).expect("static MCP environment name serializes"))
        .collect::<Vec<_>>()
        .join(",");
    args.extend([
        "-c".into(),
        format!(
            "mcp_servers.quorum={{command={},args=[{}],env_vars=[{}]}}",
            serde_json::to_string(server.command).expect("static MCP command serializes"),
            server_args,
            env_vars
        ),
    ]);
}

/// Build the restricted single-turn argument list used by classifiers.
///
/// This must retain Codex's own read-only sandbox and omit the worker/reviewer
/// approval-and-sandbox bypass.
pub fn restricted_exec_args(spec: &CodexSpec) -> Vec<String> {
    vec![
        "exec".into(),
        "--json".into(),
        "--model".into(),
        spec.model.clone(),
        "-c".into(),
        format!("model_reasoning_effort={}", spec.effort),
        "-s".into(),
        "read-only".into(),
        "-C".into(),
        spec.worktree.display().to_string(),
        "--skip-git-repo-check".into(),
        "--ignore-user-config".into(),
        spec.prompt.clone(),
    ]
}

/// Planner-specific Codex arguments. This pins the provider's mechanism-level
/// read-only sandbox and never includes the worker escape hatch.
pub fn planner_exec_args(spec: &CodexSpec) -> Vec<String> {
    planner_exec_args_configured(spec, None)
}

/// The planner argument shape with an optional `submit_plan` MCP server.
/// `-s read-only` and every other pinned isolation flag are unchanged by its
/// presence.
///
/// `-s read-only` sandboxes model-generated shell commands; it does not remove
/// the shell, so this argument shape is not what stops a Codex planner from
/// using its run envelope directly. That containment is the endpoint's: a
/// `planner` capability is honored by `SubmitPlan` alone
/// (`quorum-core/src/capabilities.rs:113-116,140,313-335`), under a once-only
/// guard and a bounded rejection budget.
fn planner_exec_args_configured(
    spec: &CodexSpec,
    agent_mcp: Option<AgentMcpServer>,
) -> Vec<String> {
    let mut args = vec![
        "exec".into(),
        "--json".into(),
        "--model".into(),
        spec.model.clone(),
        "-c".into(),
        format!("model_reasoning_effort={}", spec.effort),
        "-s".into(),
        "read-only".into(),
        "-C".into(),
        spec.worktree.display().to_string(),
        "--skip-git-repo-check".into(),
        "--ephemeral".into(),
        "--ignore-user-config".into(),
        "--ignore-rules".into(),
    ];
    append_agent_mcp_override(&mut args, agent_mcp);
    args.push(spec.prompt.clone());
    args
}

// ---------------------------------------------------------------------------
// Process wrapper
// ---------------------------------------------------------------------------

pub struct CodexProc {
    child: Child,
    reader: BufReader<tokio::process::ChildStdout>,
    line_buffer: Vec<u8>,
    diagnostics: DiagnosticBuffer,
    failures: FailureTracker,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
}

impl CodexProc {
    /// Translate a neutral runner request into one Codex `exec` process.
    pub fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        if request.mode == LaunchMode::Restricted && request.continuation_id.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "restricted Codex launch cannot resume a prior thread",
            ));
        }
        if let Some(thread_id) = request.continuation_id {
            return Self::spawn_resume(
                thread_id,
                request.model,
                request.effort,
                config.codex_sandbox,
                request.worktree,
                request.prompt,
                request.environment,
                request.agent_mcp_server(),
                config.executable,
            );
        }
        let spec = CodexSpec {
            model: request.model.to_string(),
            effort: request.effort.to_string(),
            sandbox: config.codex_sandbox.to_string(),
            worktree: request.worktree.to_path_buf(),
            prompt: request.prompt.to_string(),
            env_vars: request.environment.to_vec(),
        };
        match request.mode {
            LaunchMode::Normal => {
                Self::spawn_configured(&spec, request.agent_mcp_server(), config.executable)
            }
            LaunchMode::Restricted => Self::spawn_restricted(&spec, config.executable),
        }
    }

    pub fn normalize_line(raw: &str) -> NormalizedLine {
        NormalizedLine {
            events: codex_stream::parse_line(raw)
                .map(normalize_event)
                .unwrap_or_default(),
            terminal_text: None,
        }
    }

    pub(super) fn failure_observation(raw: &str) -> FailureObservation {
        if raw.is_empty() {
            return FailureObservation::inert();
        }
        let Some(event) = codex_stream::parse_line(raw) else {
            return FailureObservation::classified(
                FailureDisposition::NonFailover,
                "Codex emitted malformed JSONL protocol",
            );
        };
        match event {
            Event::TurnCompleted { .. } => FailureObservation::success(),
            Event::TurnFailed { error } => {
                let message = error.map(|error| error.message).unwrap_or_default();
                classify_codex_error_text(&message).unwrap_or_else(|| {
                    FailureObservation::unknown_failure(
                        "Codex turn.failed did not match a bounded provider signal",
                    )
                })
            }
            Event::Error { message } => classify_codex_error_text(&message).unwrap_or_else(|| {
                FailureObservation::unknown_failure(
                    "Codex error event did not match a bounded provider signal",
                )
            }),
            _ => FailureObservation::inert(),
        }
    }

    pub(super) fn stderr_failure_observation(text: &str) -> FailureObservation {
        if text.len() > 16 * 1024 {
            return FailureObservation::unknown_failure(
                "Codex stderr exceeded classification bound",
            );
        }
        if text.starts_with("error: unexpected argument")
            || text.starts_with("error: invalid value")
            || text.starts_with("error: unrecognized option")
            || text.starts_with("Usage: codex exec")
        {
            return FailureObservation::classified(
                FailureDisposition::NonFailover,
                "Codex rejected the execution protocol or arguments",
            );
        }
        // Authentication and availability are classified from Codex's JSONL
        // error records, not its timestamped tracing stderr.
        if text.is_empty() {
            FailureObservation::inert()
        } else {
            FailureObservation::deferred_stderr(
                "Codex stderr did not match a bounded provider signal",
            )
        }
    }

    pub fn classify_pre_authoritative_exit(
        &self,
        status: std::process::ExitStatus,
    ) -> Option<RunnerFailure> {
        self.failures.classify_exit(status)
    }

    pub fn observed_pre_authoritative_failure(&self) -> Option<RunnerFailure> {
        self.failures.observed_failure()
    }

    pub fn observed_planner_live_failure(&self) -> Option<RunnerFailure> {
        self.failures.observed_planner_live_failure()
    }

    pub fn observed_planner_terminal_failure(&self) -> Option<RunnerFailure> {
        self.failures.observed_planner_terminal_failure()
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

    /// Read-only, single-turn planner boundary. Kept separate from worker
    /// spawning so future worker flags cannot silently weaken planning.
    ///
    /// `agent_mcp` is `Some` only for a planner carrying a complete managed run
    /// envelope; it adds the `submit_plan` tool and nothing else. The read-only
    /// sandbox and every other isolation flag are unaffected.
    pub fn spawn_planner(
        spec: &CodexSpec,
        codex_bin: Option<&str>,
        agent_mcp: Option<AgentMcpServer>,
    ) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let mut cmd = Command::new(bin);
        cmd.args(planner_exec_args_configured(spec, agent_mcp));
        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
        }
        // A planner without an MCP server holds no endpoint authority, so every
        // coordination name is removed. A planner with one must keep its run
        // envelope: Codex copies exactly `AGENT_MCP_ENV_VARS` out of this
        // process environment into the stdio MCP child.
        if agent_mcp.is_some() {
            strip_planner_mcp_ambient_authority(&mut cmd);
        } else {
            strip_planner_coordination_env(&mut cmd);
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(&spec.worktree);
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let reader = BufReader::new(child.stdout.take().expect("stdout was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Codex);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        Ok(Self {
            child,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        })
    }

    /// Restricted single-turn mode for classifiers.  It deliberately omits the
    /// normal worker escape hatch that disables Codex sandbox/approval policy.
    pub fn spawn_restricted(spec: &CodexSpec, codex_bin: Option<&str>) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let args = restricted_exec_args(spec);
        let mut cmd = Command::new(bin);
        cmd.args(&args);
        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(&spec.worktree);
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let reader = BufReader::new(child.stdout.take().expect("stdout was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Codex);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        Ok(Self {
            child,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_resume(
        thread_id: &str,
        model: &str,
        effort: &str,
        _sandbox: &str,
        worktree: &std::path::Path,
        prompt: &str,
        env_vars: &[(String, String)],
        agent_mcp: Option<AgentMcpServer>,
        codex_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let args = resume_args_configured(thread_id, model, effort, prompt, agent_mcp);
        let mut cmd = Command::new(bin);
        cmd.args(&args);
        for (k, v) in env_vars {
            cmd.env(k, v);
        }
        cmd.current_dir(worktree);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().expect("stdout was piped");
        let reader = BufReader::new(stdout);
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Codex);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });

        Ok(Self {
            child,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        })
    }

    pub fn spawn(spec: &CodexSpec, codex_bin: Option<&str>) -> std::io::Result<Self> {
        Self::spawn_configured(spec, None, codex_bin)
    }

    fn spawn_configured(
        spec: &CodexSpec,
        agent_mcp: Option<AgentMcpServer>,
        codex_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let args = exec_args_configured(spec, agent_mcp);
        let mut cmd = Command::new(bin);
        cmd.args(&args);
        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
        }
        cmd.current_dir(&spec.worktree);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().expect("stdout was piped");
        let reader = BufReader::new(stdout);
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Codex);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });

        Ok(Self {
            child,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        })
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        loop {
            match self.read_raw_line(None).await {
                Ok(Some(line)) => {
                    if let Some(event) = codex_stream::parse_line(&line) {
                        return Some(event);
                    }
                }
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }

    /// Return the next provider JSONL line verbatim. Normalization belongs at
    /// the shared runner boundary so logs retain fields Quorum does not parse.
    pub async fn next_raw_line(&mut self) -> Option<String> {
        self.read_raw_line(None).await.ok().flatten()
    }

    /// Enforce the caller's remaining stdout allowance before an unterminated
    /// JSONL record can allocate beyond it. The persistent buffer also makes
    /// repeated timeout polling cancellation-safe.
    pub async fn next_raw_line_bounded(
        &mut self,
        max_bytes: usize,
    ) -> std::io::Result<Option<String>> {
        self.read_raw_line(Some(max_bytes)).await
    }

    async fn read_raw_line(&mut self, max_bytes: Option<usize>) -> std::io::Result<Option<String>> {
        loop {
            let available = self.reader.fill_buf().await?;
            if available.is_empty() {
                if self.line_buffer.is_empty() {
                    return Ok(None);
                }
                return self.take_buffered_line();
            }

            if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
                if max_bytes
                    .is_some_and(|limit| self.line_buffer.len().saturating_add(newline) > limit)
                {
                    return Err(codex_line_limit_error(max_bytes.expect("checked limit")));
                }
                self.line_buffer.extend_from_slice(&available[..newline]);
                self.reader.consume(newline + 1);
                return self.take_buffered_line();
            }

            if max_bytes
                .is_some_and(|limit| self.line_buffer.len().saturating_add(available.len()) > limit)
            {
                return Err(codex_line_limit_error(max_bytes.expect("checked limit")));
            }
            let consumed = available.len();
            self.line_buffer.extend_from_slice(available);
            self.reader.consume(consumed);
        }
    }

    fn take_buffered_line(&mut self) -> std::io::Result<Option<String>> {
        if self.line_buffer.last() == Some(&b'\r') {
            self.line_buffer.pop();
        }
        let bytes = std::mem::take(&mut self.line_buffer);
        String::from_utf8(bytes)
            .map(|line| {
                self.failures.observe_stdout(&line);
                Some(line)
            })
            .map_err(|error| {
                self.failures
                    .observe_protocol_read_error(format!("Codex stdout is not UTF-8: {error}"));
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Codex stdout is not UTF-8: {error}"),
                )
            })
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub fn drain_diagnostics(&mut self) -> Vec<CapturedOutput> {
        self.diagnostics.drain()
    }

    pub async fn kill_and_reap(mut self) -> Vec<CapturedOutput> {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
        let _ = self.child.wait().await;
        let mut terminal = Vec::new();
        while let Ok(Some(line)) = self.read_raw_line(None).await {
            terminal.push(CapturedOutput::Stdout(line));
        }
        let diagnostics = self.diagnostics.clone();
        if let Some(stderr_task) = self.stderr_task.take() {
            let _ = stderr_task.await;
        }
        terminal.extend(diagnostics.drain());
        terminal
    }
}

fn classify_codex_error_text(message: &str) -> Option<FailureObservation> {
    if message.is_empty() || message.len() > 16 * 1024 {
        return None;
    }
    if (message.contains("401 Unauthorized")
        && message.contains("Missing bearer or basic authentication in header"))
        || message.contains("Incorrect API key provided")
        || message == "Not logged in"
    {
        return Some(FailureObservation::classified(
            FailureDisposition::ProviderUnavailable,
            "Codex reported provider authentication unavailable",
        ));
    }
    if message.contains("model")
        && (message.contains("does not exist or you do not have access")
            || message.contains("model_not_found")
            || message.contains("not available for this account"))
    {
        return Some(FailureObservation::classified(
            FailureDisposition::ProfileUnavailable,
            "Codex reported the selected model unavailable",
        ));
    }
    if message.contains("429 Too Many Requests")
        && (message.contains("insufficient_quota") || message.contains("billing_hard_limit"))
    {
        return Some(FailureObservation::classified(
            FailureDisposition::ProviderUnavailable,
            "Codex reported account quota unavailable",
        ));
    }
    if [
        "500 Internal Server Error",
        "502 Bad Gateway",
        "503 Service Unavailable",
    ]
    .iter()
    .any(|signal| message.contains(signal))
    {
        return Some(FailureObservation::classified(
            FailureDisposition::ProviderUnavailable,
            "Codex reported a provider outage",
        ));
    }
    if [
        "connection reset by peer",
        "error sending request",
        "connection timed out",
        "operation timed out",
    ]
    .iter()
    .any(|signal| message.to_ascii_lowercase().contains(signal))
    {
        return Some(FailureObservation::classified(
            FailureDisposition::RetryableSameRoute,
            "Codex reported a retryable transport failure",
        ));
    }
    None
}

fn codex_line_limit_error(limit: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("Codex stdout line exceeded {limit}-byte limit"),
    )
}

fn normalize_event(event: Event) -> Vec<AgentEvent> {
    match event {
        Event::ThreadStarted { thread_id } => vec![AgentEvent::ThreadStarted { thread_id }],
        Event::ItemStarted {
            item: codex_stream::Item::AgentMessage { text, .. },
        } if !text.is_empty() => vec![AgentEvent::AssistantText { text }],
        Event::ItemCompleted {
            item: codex_stream::Item::AgentMessage { id, text },
        } => vec![AgentEvent::CompletedAssistantText { item_id: id, text }],
        Event::ItemStarted { item } | Event::ItemCompleted { item } => match item {
            codex_stream::Item::CommandExecution { command, .. } => vec![AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary: tool_summary("command", &serde_json::json!({"command": command})),
            }],
            codex_stream::Item::FileChange { changes, .. } => {
                let path = changes
                    .first()
                    .map(|change| change.path.as_str())
                    .unwrap_or("file");
                vec![AgentEvent::Activity {
                    kind: ActivityKind::ToolUse,
                    summary: tool_summary("file_change", &serde_json::json!({"file_path": path})),
                }]
            }
            _ => vec![],
        },
        Event::TurnCompleted { usage } => vec![AgentEvent::TurnCompleted {
            usage: usage.map(|usage| TokenUsage {
                input_tokens: usage.input_tokens,
                uncached_input_tokens: usage.input_tokens.saturating_sub(usage.cached_input_tokens),
                cached_input_tokens: usage.cached_input_tokens,
                cache_write_input_tokens: usage.cache_write_input_tokens,
                output_tokens: usage.output_tokens,
                reasoning_tokens: usage.reasoning_output_tokens,
            }),
            cost_usd: None,
        }],
        Event::TurnFailed { error } => vec![AgentEvent::TurnFailed {
            message: error
                .map(|error| error.message)
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| "Codex turn failed".into()),
            usage: None,
            cost_usd: None,
        }],
        // Top-level errors can be retryable transport warnings. Only the
        // authoritative terminal turn events advance lifecycle state.
        Event::Error { .. } => vec![],
        _ => vec![],
    }
}

fn strip_planner_coordination_env(cmd: &mut Command) {
    for name in [
        "QUORUM_AGENT",
        "QUORUM_HOME",
        "QUORUM_REPO",
        "QUORUM_RUN_ID",
        "GH_TOKEN",
        "GITHUB_TOKEN",
    ] {
        cmd.env_remove(name);
    }
}

/// Ambient authority removed from an MCP-carrying planner. The managed run
/// envelope survives because the stdio MCP child needs it; the database home
/// and every GitHub credential do not reach the provider or its child.
fn strip_planner_mcp_ambient_authority(cmd: &mut Command) {
    for name in ["QUORUM_HOME", "GH_TOKEN", "GITHUB_TOKEN", "GH_CONFIG_DIR"] {
        cmd.env_remove(name);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn shell_proc(script: &str) -> CodexProc {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
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
        let mut child = command.spawn().unwrap();
        let reader = BufReader::new(child.stdout.take().unwrap());
        let stderr = BufReader::new(child.stderr.take().unwrap());
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Codex);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        CodexProc {
            child,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        }
    }

    async fn wait_for_exit(proc: &mut CodexProc) -> std::process::ExitStatus {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(status) = proc.try_wait().unwrap() {
                    return status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixture process did not exit")
    }

    fn test_spec() -> CodexSpec {
        CodexSpec {
            model: "o4-mini".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: PathBuf::from("/tmp"),
            prompt: "say hello".into(),
            env_vars: vec![],
        }
    }

    // ── Pinned exec argument shape ───────────────────────────────────────

    #[test]
    fn exec_args_shape() {
        let spec = test_spec();
        let args = exec_args(&spec);
        assert_eq!(args[0], "exec");
        assert_eq!(args[1], "--json");
        assert_eq!(args[2], "--model");
        assert_eq!(args[3], "o4-mini");
        assert_eq!(args[4], "-c");
        assert_eq!(args[5], "model_reasoning_effort=high");
        assert_eq!(args[6], "-s");
        assert_eq!(args[7], "read-only");
        assert_eq!(args[8], "--dangerously-bypass-approvals-and-sandbox");
        assert_eq!(args[9], "-C");
        assert_eq!(args[10], "/tmp");
        assert_eq!(args[11], "--skip-git-repo-check");
        assert_eq!(args[12], "--ignore-user-config");
        assert_eq!(args[13], "say hello");
        assert_eq!(args.len(), 14);
    }

    #[test]
    fn agent_message_normalization_distinguishes_started_from_completed() {
        let text = "complete response\nwith exact bytes: \u{00e9}";
        let started = normalize_event(Event::ItemStarted {
            item: codex_stream::Item::AgentMessage {
                id: "item_42".into(),
                text: text.into(),
            },
        });
        let completed = normalize_event(Event::ItemCompleted {
            item: codex_stream::Item::AgentMessage {
                id: "item_42".into(),
                text: text.into(),
            },
        });

        assert_eq!(
            started,
            vec![AgentEvent::AssistantText { text: text.into() }]
        );
        assert_eq!(
            completed,
            vec![AgentEvent::CompletedAssistantText {
                item_id: "item_42".into(),
                text: text.into(),
            }]
        );
    }

    #[test]
    fn exec_args_contain_json_flag() {
        let args = exec_args(&test_spec());
        assert!(args.contains(&"--json".to_string()));
    }

    #[test]
    fn exec_args_contain_approval_bypass() {
        let args = exec_args(&test_spec());
        assert!(args.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));
    }

    #[test]
    fn exec_args_contain_model() {
        let args = exec_args(&test_spec());
        let pos = args.iter().position(|a| a == "--model").unwrap();
        assert_eq!(args[pos + 1], "o4-mini");
    }

    #[test]
    fn exec_args_contain_sandbox() {
        let args = exec_args(&test_spec());
        let pos = args.iter().position(|a| a == "-s").unwrap();
        assert_eq!(args[pos + 1], "read-only");
    }

    #[test]
    fn exec_args_contain_cwd() {
        let args = exec_args(&test_spec());
        let pos = args.iter().position(|a| a == "-C").unwrap();
        assert_eq!(args[pos + 1], "/tmp");
    }

    #[test]
    fn exec_args_contain_effort() {
        let args = exec_args(&test_spec());
        let pos = args.iter().position(|a| a == "-c").unwrap();
        assert_eq!(args[pos + 1], "model_reasoning_effort=high");
    }

    #[test]
    fn restricted_exec_args_pin_classifier_security_boundary() {
        let args = restricted_exec_args(&test_spec());
        assert_eq!(
            args,
            [
                "exec",
                "--json",
                "--model",
                "o4-mini",
                "-c",
                "model_reasoning_effort=high",
                "-s",
                "read-only",
                "-C",
                "/tmp",
                "--skip-git-repo-check",
                "--ignore-user-config",
                "say hello",
            ]
        );
        assert!(!args.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));
    }

    #[test]
    fn planner_exec_args_pin_read_only_frontier_boundary() {
        let mut spec = test_spec();
        spec.model = "gpt-5.6-sol".into();
        spec.effort = "high".into();
        let args = planner_exec_args(&spec);
        assert_eq!(
            args,
            [
                "exec",
                "--json",
                "--model",
                "gpt-5.6-sol",
                "-c",
                "model_reasoning_effort=high",
                "-s",
                "read-only",
                "-C",
                "/tmp",
                "--skip-git-repo-check",
                "--ephemeral",
                "--ignore-user-config",
                "--ignore-rules",
                "say hello",
            ]
        );
        for forbidden in [
            "--approve-for-me",
            "--dangerously-bypass-approvals-and-sandbox",
            "--dangerously-bypass-hook-trust",
            "--add-dir",
            "workspace-write",
            "danger-full-access",
            "resume",
            "--last",
        ] {
            assert!(
                !args.iter().any(|arg| arg == forbidden),
                "planner boundary contains forbidden argument {forbidden}"
            );
        }
    }

    /// The `submit_plan` server is additive: it must not relax the planner's
    /// own read-only sandbox or reintroduce any bypass flag.
    #[test]
    fn codex_planner_with_mcp_keeps_read_only_sandbox() {
        let mut spec = test_spec();
        spec.model = "gpt-5.6-sol".into();
        let args =
            planner_exec_args_configured(&spec, Some(crate::serve::runner::AGENT_MCP_SERVER));

        let sandbox = args
            .iter()
            .position(|arg| arg == "-s")
            .expect("planner sandbox flag missing");
        assert_eq!(args[sandbox + 1], "read-only");
        assert!(
            args.iter().any(|arg| arg
                == r#"mcp_servers.quorum={command="quorum",args=["agent-mcp"],env_vars=["QUORUM_REPO","QUORUM_AGENT","QUORUM_RUN_ID","QUORUM_AGENT_ENDPOINT"]}"#),
            "{args:?}"
        );
        assert_eq!(args.last().unwrap(), "say hello");
        for forbidden in [
            "--approve-for-me",
            "--dangerously-bypass-approvals-and-sandbox",
            "--dangerously-bypass-hook-trust",
            "--add-dir",
            "workspace-write",
            "danger-full-access",
        ] {
            assert!(
                !args.iter().any(|arg| arg == forbidden),
                "MCP-carrying planner contains forbidden argument {forbidden}"
            );
        }
        // Every isolation flag of the tool-less shape is still present.
        for pinned in [
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
        ] {
            assert!(args.iter().any(|arg| arg == pinned), "{args:?}");
        }
    }

    /// Codex copies exactly `AGENT_MCP_ENV_VARS` from this process into the
    /// stdio MCP child, so an MCP-carrying planner keeps its run envelope while
    /// a tool-less planner keeps carrying none.
    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_run_envelope_survives_only_with_mcp() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let fake_codex = tmp.path().join("fake-codex");
        std::fs::write(
            &fake_codex,
            r#"#!/bin/sh
printf '{"repo":"%s","agent":"%s","run":"%s","endpoint":"%s","home":"%s","gh":"%s","openai":"%s"}\n' "$QUORUM_REPO" "$QUORUM_AGENT" "$QUORUM_RUN_ID" "$QUORUM_AGENT_ENDPOINT" "$QUORUM_HOME" "$GH_TOKEN" "$OPENAI_API_KEY"
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake_codex).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_codex, permissions).unwrap();

        for (agent_mcp, expect_envelope) in [
            (Some(crate::serve::runner::AGENT_MCP_SERVER), true),
            (None, false),
        ] {
            let spec = CodexSpec {
                worktree: tmp.path().to_path_buf(),
                env_vars: vec![
                    ("QUORUM_REPO".into(), "owner/repo".into()),
                    ("QUORUM_AGENT".into(), "Planner-test".into()),
                    ("QUORUM_RUN_ID".into(), "run-capability".into()),
                    (
                        "QUORUM_AGENT_ENDPOINT".into(),
                        "/tmp/quorum-planner.sock".into(),
                    ),
                    ("QUORUM_HOME".into(), "home-authority".into()),
                    ("GH_TOKEN".into(), "gh-authority".into()),
                    ("OPENAI_API_KEY".into(), "provider-auth".into()),
                ],
                ..test_spec()
            };
            let mut proc = CodexProc::spawn_planner(&spec, fake_codex.to_str(), agent_mcp).unwrap();
            let line =
                tokio::time::timeout(std::time::Duration::from_secs(5), proc.next_raw_line())
                    .await
                    .expect("fake Codex did not emit its environment")
                    .expect("fake Codex exited without emitting its environment");
            let _ = proc.kill_and_reap().await;
            let observed: serde_json::Value = serde_json::from_str(&line).unwrap();

            if expect_envelope {
                assert_eq!(observed["repo"], "owner/repo");
                assert_eq!(observed["agent"], "Planner-test");
                assert_eq!(observed["run"], "run-capability");
                assert_eq!(observed["endpoint"], "/tmp/quorum-planner.sock");
            } else {
                for name in ["repo", "agent", "run"] {
                    assert_eq!(observed[name], "", "{name} reached a tool-less planner");
                }
            }
            assert_eq!(observed["home"], "");
            assert_eq!(observed["gh"], "");
            // Provider authentication is untouched in both shapes.
            assert_eq!(observed["openai"], "provider-auth");
        }
    }

    // ── Pinned resume argument shape ─────────────────────────────────────

    #[test]
    fn resume_args_shape() {
        let args = resume_args("019f-thread-id", "o4-mini", "high", "continue");
        assert_eq!(args[0], "exec");
        assert_eq!(args[1], "resume");
        assert_eq!(args[2], "019f-thread-id");
        assert_eq!(args[3], "--json");
        assert_eq!(args[4], "--model");
        assert_eq!(args[5], "o4-mini");
        assert_eq!(args[6], "-c");
        assert_eq!(args[7], "model_reasoning_effort=high");
        assert_eq!(args[8], "--dangerously-bypass-approvals-and-sandbox");
        assert_eq!(args[9], "--skip-git-repo-check");
        assert_eq!(args[10], "--ignore-user-config");
        assert_eq!(args[11], "continue");
        assert_eq!(args.len(), 12);
    }

    #[test]
    fn resume_args_thread_id_position() {
        let args = resume_args("my-thread", "gpt-4o", "medium", "go");
        assert_eq!(
            args[2], "my-thread",
            "thread_id must be positional arg after 'resume'"
        );
    }

    #[test]
    fn resume_args_carry_json_flag() {
        let args = resume_args("tid", "m", "h", "p");
        assert!(args.contains(&"--json".to_string()));
    }

    // ── No session UUID pre-assignment ───────────────────────────────────

    #[test]
    fn exec_args_do_not_contain_session_id() {
        let args = exec_args(&test_spec());
        assert!(
            !args
                .iter()
                .any(|a| a == "--session-id" || a.starts_with("--session")),
            "Codex thread ID is provider-issued, not pre-assigned"
        );
    }

    // ── No ephemeral flag ────────────────────────────────────────────────

    #[test]
    fn exec_args_do_not_contain_ephemeral() {
        let args = exec_args(&test_spec());
        assert!(!args.contains(&"--ephemeral".to_string()));
    }

    // ── Spec carries env vars ────────────────────────────────────────────

    #[test]
    fn spec_carries_env_vars() {
        let spec = CodexSpec {
            env_vars: vec![("FOO".into(), "bar".into())],
            ..test_spec()
        };
        assert_eq!(spec.env_vars[0], ("FOO".into(), "bar".into()));
    }

    #[tokio::test]
    async fn planner_spawn_strips_coordination_authority_but_preserves_provider_auth() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let fake_codex = tmp.path().join("fake-codex");
        std::fs::write(
            &fake_codex,
            r#"#!/bin/sh
printf '{"quorum_agent":"%s","quorum_home":"%s","quorum_repo":"%s","quorum_run_id":"%s","gh_token":"%s","github_token":"%s","openai_api_key":"%s","codex_home":"%s","harmless":"%s"}\n' "$QUORUM_AGENT" "$QUORUM_HOME" "$QUORUM_REPO" "$QUORUM_RUN_ID" "$GH_TOKEN" "$GITHUB_TOKEN" "$OPENAI_API_KEY" "$CODEX_HOME" "$HARMLESS"
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake_codex).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_codex, permissions).unwrap();

        let spec = CodexSpec {
            worktree: tmp.path().to_path_buf(),
            env_vars: vec![
                ("QUORUM_AGENT".into(), "agent-authority".into()),
                ("QUORUM_HOME".into(), "home-authority".into()),
                ("QUORUM_REPO".into(), "repo-authority".into()),
                ("QUORUM_RUN_ID".into(), "run-authority".into()),
                ("GH_TOKEN".into(), "gh-authority".into()),
                ("GITHUB_TOKEN".into(), "github-authority".into()),
                ("OPENAI_API_KEY".into(), "provider-auth".into()),
                ("CODEX_HOME".into(), "provider-home".into()),
                ("HARMLESS".into(), "preserved".into()),
            ],
            ..test_spec()
        };
        let mut proc = CodexProc::spawn_planner(&spec, fake_codex.to_str(), None).unwrap();
        let line = tokio::time::timeout(std::time::Duration::from_secs(5), proc.next_raw_line())
            .await
            .expect("fake Codex did not emit its environment")
            .expect("fake Codex exited without emitting its environment");
        let _ = proc.kill_and_reap().await;
        let env: serde_json::Value = serde_json::from_str(&line).unwrap();

        for stripped in [
            "quorum_agent",
            "quorum_home",
            "quorum_repo",
            "quorum_run_id",
            "gh_token",
            "github_token",
        ] {
            assert_eq!(env[stripped], "", "{stripped} reached the planner child");
        }
        assert_eq!(env["openai_api_key"], "provider-auth");
        assert_eq!(env["codex_home"], "provider-home");
        assert_eq!(env["harmless"], "preserved");
    }

    // ── Zero-token real-binary contract tests ────────────────────────────
    //
    // Analogous to the Claude boundary tests in agent.rs: spawn the real
    // installed codex binary with blanked auth to verify argument acceptance
    // at zero API cost. Skipped when codex is not on PATH.

    fn codex_available() -> bool {
        std::process::Command::new("codex")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn no_auth_env(codex_home: &std::path::Path) -> Vec<(String, String)> {
        vec![
            ("OPENAI_API_KEY".into(), String::new()),
            ("CODEX_HOME".into(), codex_home.display().to_string()),
        ]
    }

    fn managed_mcp_env(
        codex_home: &std::path::Path,
        shim_dir: &std::path::Path,
    ) -> Vec<(String, String)> {
        let inherited_path = std::env::var("PATH").unwrap_or_default();
        let mut environment = no_auth_env(codex_home);
        environment.extend([
            (
                "PATH".into(),
                format!("{}:{inherited_path}", shim_dir.display()),
            ),
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Lever-test".into()),
            ("QUORUM_RUN_ID".into(), "run-capability".into()),
            (
                "QUORUM_AGENT_ENDPOINT".into(),
                "/tmp/quorum-agent-test.sock".into(),
            ),
        ]);
        environment
    }

    async fn wait_for_mcp_launches(path: &std::path::Path, expected: usize) -> String {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let capture = std::fs::read_to_string(path).unwrap_or_default();
                if capture.matches("launch-end\n").count() >= expected {
                    return capture;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("real Codex did not launch the configured MCP child")
    }

    /// Codex 0.148 clears an MCP child's environment before copying its
    /// defaults and the names in `env_vars`. Exercise that real provider
    /// boundary for a fresh provider-issued thread and its exact resume.
    #[tokio::test]
    async fn real_cli_forwards_managed_environment_to_fresh_and_resumed_mcp_children() {
        use std::os::unix::fs::PermissionsExt;

        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let capture = tmp.path().join("mcp-environment.log");
        let fake_quorum = tmp.path().join("quorum");
        std::fs::write(
            &fake_quorum,
            format!(
                r#"#!/bin/sh
{{
  printf 'arg=%s\n' "$1"
  printf 'repo=%s\n' "$QUORUM_REPO"
  printf 'agent=%s\n' "$QUORUM_AGENT"
  printf 'run=%s\n' "$QUORUM_RUN_ID"
  printf 'endpoint=%s\n' "$QUORUM_AGENT_ENDPOINT"
  printf 'gh=%s\n' "$GH_TOKEN"
  printf 'github=%s\n' "$GITHUB_TOKEN"
  printf 'launch-end\n'
}} >> '{}'
while IFS= read -r request; do :; done
"#,
                capture.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake_quorum).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_quorum, permissions).unwrap();

        let codex_home = tmp.path().join("codex-home");
        std::fs::create_dir(&codex_home).unwrap();
        let environment = managed_mcp_env(&codex_home, tmp.path());
        let spec = CodexSpec {
            model: "o4-mini".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: tmp.path().to_path_buf(),
            prompt: "exact fresh pending turn".into(),
            env_vars: environment.clone(),
        };

        let mut fresh =
            CodexProc::spawn_configured(&spec, Some(crate::serve::runner::AGENT_MCP_SERVER), None)
                .expect("spawn fresh Codex with invocation-local MCP");
        let thread_id =
            match tokio::time::timeout(std::time::Duration::from_secs(10), fresh.next_event())
                .await
                .expect("fresh Codex emitted no provider thread identity")
            {
                Some(Event::ThreadStarted { thread_id }) => thread_id,
                other => panic!("fresh Codex did not issue a thread identity: {other:?}"),
            };
        wait_for_mcp_launches(&capture, 1).await;
        let _ = fresh.kill_and_reap().await;

        let resumed = CodexProc::spawn_resume(
            &thread_id,
            "o4-mini",
            "high",
            "read-only",
            tmp.path(),
            "exact resumed pending turn",
            &environment,
            Some(crate::serve::runner::AGENT_MCP_SERVER),
            None,
        )
        .expect("resume exact provider-issued Codex thread with invocation-local MCP");
        let captured = wait_for_mcp_launches(&capture, 2).await;
        let _ = resumed.kill_and_reap().await;

        let launches: Vec<_> = captured.split("launch-end\n").collect();
        assert!(launches.len() >= 3, "{captured}");
        for launch in &launches[..2] {
            assert!(launch.contains("arg=agent-mcp\n"), "{launch}");
            assert!(launch.contains("repo=owner/repo\n"), "{launch}");
            assert!(launch.contains("agent=Lever-test\n"), "{launch}");
            assert!(launch.contains("run=run-capability\n"), "{launch}");
            assert!(
                launch.contains("endpoint=/tmp/quorum-agent-test.sock\n"),
                "{launch}"
            );
            assert!(launch.contains("gh=\n"), "{launch}");
            assert!(launch.contains("github=\n"), "{launch}");
        }

        let resume = resume_args_configured(
            &thread_id,
            "o4-mini",
            "high",
            "exact resumed pending turn",
            Some(crate::serve::runner::AGENT_MCP_SERVER),
        );
        assert_eq!(&resume[..3], ["exec", "resume", thread_id.as_str()]);
        assert_eq!(resume.last().unwrap(), "exact resumed pending turn");
    }

    /// Positive contract: `codex exec --json` with production args must emit
    /// at least a thread.started event before dying on auth. Any event back
    /// proves arguments parsed; instant exit with zero events is the
    /// crash-loop class.
    ///
    /// Codex retries auth ~10 times with exponential backoff before emitting
    /// turn.failed, so this test needs a generous timeout.
    #[tokio::test]
    async fn real_cli_accepts_exec_args() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let spec = CodexSpec {
            model: "o4-mini".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: PathBuf::from("/tmp"),
            prompt: "ping".into(),
            env_vars: no_auth_env(tmp.path()),
        };
        let mut proc =
            CodexProc::spawn_configured(&spec, Some(crate::serve::runner::AGENT_MCP_SERVER), None)
                .expect("spawn codex with invocation-local MCP");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("codex produced no event within 60s — args may hang the CLI");
        let _terminal_output = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "codex exited without emitting any JSONL event — exec argument \
             surface was rejected at CLI validation (crash-loop class)"
        );
    }

    /// Positive zero-token contract for the classifier-specific restricted
    /// launch shape. An emitted JSONL event proves the real CLI accepted the
    /// arguments before the blank auth environment stops useful work.
    #[tokio::test]
    async fn real_cli_accepts_restricted_classifier_args() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let codex_home = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let spec = CodexSpec {
            model: "o4-mini".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: worktree.path().to_path_buf(),
            prompt: "ping".into(),
            env_vars: no_auth_env(codex_home.path()),
        };
        let mut proc = CodexProc::spawn_restricted(&spec, None).expect("spawn restricted codex");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("restricted codex produced no event within 60s");
        let _terminal_output = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "codex exited without JSONL — restricted classifier arguments were rejected"
        );
    }

    /// Real-binary reachability check for the planner's `submit_plan` door.
    ///
    /// `-s read-only` is the planner's mechanism-level isolation. `submit_plan`
    /// is useless if that sandbox also stops the stdio MCP child from reaching
    /// the daemon's Unix socket, and a fake provider cannot answer that — only
    /// the installed `codex` binary can. This launches the real planner shape
    /// with the MCP override pointed at a stand-in `quorum` that connects to a
    /// temporary Unix socket, and asserts the connect succeeds.
    ///
    /// `#[ignore]` by default: it needs the real binary plus `python3`, and it
    /// runs a provider process. If the sandbox ever blocks the connect, record
    /// the finding — do NOT loosen the sandbox.
    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "real codex binary; run with --ignored"]
    async fn codex_planner_sandbox_allows_mcp_socket() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        if std::process::Command::new("python3")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| !status.success())
            .unwrap_or(true)
        {
            eprintln!("skipped: no python3 to drive the socket probe");
            return;
        }

        // Short path: macOS caps `sun_path` at 104 bytes, well under a
        // `/var/folders` temp directory plus a file name.
        let socket = std::path::PathBuf::from(format!(
            "/tmp/quorum-planner-mcp-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&socket);
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind probe socket");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_in_thread = Arc::clone(&accepted);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stream.is_err() {
                    return;
                }
                accepted_in_thread.fetch_add(1, Ordering::SeqCst);
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("connected");
        let probe_log = tmp.path().join("probe.log");
        let fake_quorum = tmp.path().join("quorum");
        std::fs::write(
            &fake_quorum,
            format!(
                r#"#!/bin/sh
python3 - <<'PROBE' >> '{log}' 2>&1
import socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect({socket:?})
sock.sendall(b"probe")
open({marker:?}, "w").write("connected")
print("connect-ok")
PROBE
if [ $? -ne 0 ]; then printf 'connect-failed\n' >> '{log}'; fi
while IFS= read -r _line; do :; done
"#,
                log = probe_log.display(),
                socket = socket.display().to_string(),
                marker = marker.display().to_string(),
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake_quorum).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_quorum, permissions).unwrap();

        let codex_home = tmp.path().join("codex-home");
        std::fs::create_dir(&codex_home).unwrap();
        let mut environment = managed_mcp_env(&codex_home, tmp.path());
        environment.push(("QUORUM_AGENT_ENDPOINT".into(), socket.display().to_string()));
        let spec = CodexSpec {
            model: "gpt-5.6-sol".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: tmp.path().to_path_buf(),
            prompt: "call submit_plan".into(),
            env_vars: environment,
        };

        let proc =
            CodexProc::spawn_planner(&spec, None, Some(crate::serve::runner::AGENT_MCP_SERVER))
                .expect("spawn real planner codex with the submit_plan MCP override");
        let connected = tokio::time::timeout(std::time::Duration::from_secs(45), async {
            loop {
                if marker.exists() {
                    return true;
                }
                if std::fs::read_to_string(&probe_log)
                    .unwrap_or_default()
                    .contains("connect-failed")
                {
                    return false;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await;
        let _ = proc.kill_and_reap().await;
        let log = std::fs::read_to_string(&probe_log).unwrap_or_default();
        let _ = std::fs::remove_file(&socket);

        assert_eq!(
            connected,
            Ok(true),
            "the codex read-only planner sandbox blocked the MCP child's Unix \
             socket connect (probe log: {log}). Record this finding — do not \
             loosen the sandbox."
        );
        assert!(
            accepted.load(Ordering::SeqCst) >= 1,
            "no connection reached the listener (probe log: {log})"
        );
    }

    /// Positive zero-token contract for the planner-specific launch shape.
    /// An emitted JSONL event proves the installed CLI accepted every pinned
    /// isolation flag before the blank authentication environment stops work.
    #[tokio::test]
    async fn real_cli_accepts_planner_args() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let codex_home = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let spec = CodexSpec {
            model: "gpt-5.6-sol".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: worktree.path().to_path_buf(),
            prompt: "return an empty JSON object".into(),
            env_vars: no_auth_env(codex_home.path()),
        };
        let mut proc = CodexProc::spawn_planner(&spec, None, None).expect("spawn planner codex");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("planner codex produced no event within 60s");
        let _ = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "codex rejected the planner isolation arguments before authentication"
        );
    }

    /// The planner supplies its prompt positionally while stdin is `/dev/null`.
    /// Current Codex releases can write an informational notice about that
    /// stdin shape before the first JSONL event. Keep this a real-binary
    /// boundary check without requiring a network-authenticated turn: the
    /// spawned no-auth process must eventually report its structured failure
    /// despite the captured notice.
    #[tokio::test]
    async fn real_cli_planner_stderr_notice_does_not_mask_structured_auth_failure() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let codex_home = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let spec = CodexSpec {
            model: "gpt-5.6-sol".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: worktree.path().to_path_buf(),
            prompt: "return an empty JSON object".into(),
            env_vars: no_auth_env(codex_home.path()),
        };
        let mut proc = CodexProc::spawn_planner(&spec, None, None).expect("spawn planner codex");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_raw_line())
            .await
            .expect("planner codex produced no JSONL event within 60s");
        assert!(event.is_some(), "planner codex exited before JSONL startup");

        let stderr = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let output = proc.drain_diagnostics();
                if output
                    .iter()
                    .any(|line| matches!(line, CapturedOutput::Stderr(text) if !text.is_empty()))
                {
                    return output;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("planner stdin shape emitted no bounded informational stderr");
        assert!(stderr
            .iter()
            .any(|line| matches!(line, CapturedOutput::Stderr(text) if !text.is_empty())));

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let raw = proc
                    .next_raw_line()
                    .await
                    .expect("planner Codex ended before a structured auth/provider failure");
                if matches!(
                    codex_stream::parse_line(&raw),
                    Some(Event::TurnFailed { .. } | Event::Error { .. })
                ) {
                    return;
                }
            }
        })
        .await;
        assert!(
            terminal.is_ok(),
            "planner Codex did not report a structured auth/provider failure within 60s"
        );
        assert_eq!(
            proc.observed_planner_live_failure()
                .expect("structured auth failure remains authoritative")
                .disposition(),
            FailureDisposition::ProviderUnavailable,
        );
        let _ = proc.kill_and_reap().await;
    }

    /// Positive contract: the first event from `codex exec --json` must be
    /// thread.started with a non-empty thread_id. Missing thread ID is a
    /// loud startup failure.
    #[tokio::test]
    async fn real_cli_emits_thread_started_first() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let spec = CodexSpec {
            model: "o4-mini".into(),
            effort: "high".into(),
            sandbox: "read-only".into(),
            worktree: PathBuf::from("/tmp"),
            prompt: "ping".into(),
            env_vars: no_auth_env(tmp.path()),
        };
        let mut proc = CodexProc::spawn(&spec, None).expect("spawn codex");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("no event within 60s");
        let _terminal_output = proc.kill_and_reap().await;
        match event {
            Some(Event::ThreadStarted { thread_id }) => {
                assert!(!thread_id.is_empty(), "thread_id must not be empty");
            }
            other => panic!("first event must be ThreadStarted, got: {other:?}"),
        }
    }

    /// Positive contract: `codex exec resume` argument shape is accepted.
    /// We pass a fake thread_id — the CLI should parse args fine and fail
    /// later on lookup/auth, still emitting JSONL.
    #[tokio::test]
    async fn real_cli_accepts_resume_args() {
        if !codex_available() {
            eprintln!("skipped: no codex binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let args = resume_args_configured(
            "00000000-0000-0000-0000-000000000000",
            "o4-mini",
            "high",
            "continue",
            Some(crate::serve::runner::AGENT_MCP_SERVER),
        );
        let mut cmd = std::process::Command::new("codex");
        cmd.args(&args);
        for (k, v) in no_auth_env(tmp.path()) {
            cmd.env(&k, &v);
        }
        cmd.current_dir("/tmp");
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().expect("spawn codex resume");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll codex resume") {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                child.kill().expect("kill bounded codex resume canary");
                let _ = child.wait();
                break None;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        // Remaining alive past the short parser boundary proves the CLI
        // accepted the shape. Empty CODEX_HOME and API key prevent auth reuse.
        let Some(status) = status else {
            return;
        };
        let output = child
            .wait_with_output()
            .expect("collect codex resume output");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !status.success() && stdout.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let is_arg_error = stderr.contains("Usage:")
                || stderr.contains("unexpected argument")
                || stderr.contains("invalid argument")
                || stderr.contains("unrecognized option");
            assert!(
                !is_arg_error,
                "codex rejected resume argument shape: {stderr}"
            );
        }
    }

    #[test]
    fn real_codex_auth_fixture_is_provider_wide() {
        // Captured from codex-cli 0.145.0 with an empty CODEX_HOME/API key.
        let fixture = r#"{"type":"turn.failed","error":{"message":"unexpected status 401 Unauthorized: Missing bearer or basic authentication in header, url: https://api.openai.com/v1/responses"}}"#;
        assert_eq!(
            CodexProc::failure_observation(fixture).disposition,
            Some(FailureDisposition::ProviderUnavailable)
        );
    }

    #[test]
    fn codex_model_unavailable_fixture_is_profile_scoped() {
        let fixture = r#"{"type":"turn.failed","error":{"message":"The model `gpt-future` does not exist or you do not have access to it."}}"#;
        assert_eq!(
            CodexProc::failure_observation(fixture).disposition,
            Some(FailureDisposition::ProfileUnavailable)
        );
    }

    #[tokio::test]
    async fn protocol_nonzero_malformed_early_eof_and_unknown_are_bounded() {
        let mut nonzero = shell_proc("exit 7").await;
        assert!(nonzero.next_raw_line().await.is_none());
        let status = wait_for_exit(&mut nonzero).await;
        assert!(
            nonzero
                .finish_stderr_until(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        );
        assert_eq!(
            nonzero
                .classify_pre_authoritative_exit(status)
                .unwrap()
                .disposition(),
            FailureDisposition::NonFailover
        );

        let mut ordinary =
            shell_proc("echo 'error: unexpected argument --future' >&2; exit 2").await;
        assert!(ordinary.next_raw_line().await.is_none());
        let status = wait_for_exit(&mut ordinary).await;
        assert!(
            ordinary
                .finish_stderr_until(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        );
        assert_eq!(
            ordinary
                .classify_pre_authoritative_exit(status)
                .unwrap()
                .disposition(),
            FailureDisposition::NonFailover
        );

        let mut malformed = shell_proc("printf '%s\\n' 'not-json'").await;
        while malformed.next_raw_line().await.is_some() {}
        let status = wait_for_exit(&mut malformed).await;
        assert_eq!(
            malformed
                .classify_pre_authoritative_exit(status)
                .unwrap()
                .disposition(),
            FailureDisposition::NonFailover
        );

        let mut early_eof = shell_proc(
            "printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}'",
        )
        .await;
        while early_eof.next_raw_line().await.is_some() {}
        let status = wait_for_exit(&mut early_eof).await;
        assert_eq!(
            early_eof
                .classify_pre_authoritative_exit(status)
                .unwrap()
                .disposition(),
            FailureDisposition::RetryableSameRoute
        );

        let mut unknown = shell_proc("echo 'future provider diagnostic' >&2; exit 1").await;
        assert!(unknown.next_raw_line().await.is_none());
        let status = wait_for_exit(&mut unknown).await;
        assert!(
            unknown
                .finish_stderr_until(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        );
        let failure = unknown.classify_pre_authoritative_exit(status).unwrap();
        assert_eq!(failure.disposition(), FailureDisposition::Unclassified);
    }

    #[tokio::test]
    async fn exit_finalization_drains_past_tick_cap_and_joins_stderr() {
        let prefix = "i=0; while [ $i -lt 70 ]; do printf '%s\\n' \
            '{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}'; \
            i=$((i+1)); done;";

        let mut failed = shell_proc(&format!(
            "{prefix} printf '%s\\n' \
             '{{\"type\":\"turn.failed\",\"error\":{{\"message\":\"unexpected status 401 Unauthorized: Missing bearer or basic authentication in header\"}}}}'; exit 1"
        ))
        .await;
        for _ in 0..64 {
            assert!(failed.next_raw_line().await.is_some());
        }
        let failed_status = wait_for_exit(&mut failed).await;
        let mut failed = crate::serve::runner::RunnerProc::Codex(failed);
        let terminal = failed.finalize_pre_authoritative_evidence().await;
        assert!(!terminal.is_empty());
        assert_eq!(
            failed
                .classify_pre_authoritative_exit(failed_status)
                .unwrap()
                .disposition(),
            FailureDisposition::ProviderUnavailable
        );
        failed.kill_and_reap().await;

        let mut succeeded = shell_proc(&format!(
            "{prefix} printf '%s\\n' '{{\"type\":\"turn.completed\"}}'"
        ))
        .await;
        for _ in 0..64 {
            assert!(succeeded.next_raw_line().await.is_some());
        }
        let succeeded_status = wait_for_exit(&mut succeeded).await;
        let mut succeeded = crate::serve::runner::RunnerProc::Codex(succeeded);
        succeeded.finalize_pre_authoritative_evidence().await;
        assert!(succeeded
            .classify_pre_authoritative_exit(succeeded_status)
            .is_none());
        succeeded.kill_and_reap().await;

        let mut delayed_stderr = shell_proc(&format!("{prefix} exit 1")).await;
        if let Some(task) = delayed_stderr.stderr_task.take() {
            task.abort();
            let _ = task.await;
        }
        let delayed_failures = delayed_stderr.failures.clone();
        delayed_stderr.stderr_task = Some(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            delayed_failures.observe_stderr("future stderr boundary");
        }));
        for _ in 0..64 {
            assert!(delayed_stderr.next_raw_line().await.is_some());
        }
        let stderr_status = wait_for_exit(&mut delayed_stderr).await;
        assert_eq!(
            delayed_stderr
                .classify_pre_authoritative_exit(stderr_status)
                .unwrap()
                .disposition(),
            FailureDisposition::RetryableSameRoute,
            "fixture must reproduce the pre-finalization stderr race"
        );
        let mut delayed_stderr = crate::serve::runner::RunnerProc::Codex(delayed_stderr);
        delayed_stderr.finalize_pre_authoritative_evidence().await;
        assert_eq!(
            delayed_stderr
                .classify_pre_authoritative_exit(stderr_status)
                .unwrap()
                .disposition(),
            FailureDisposition::Unclassified
        );
        delayed_stderr.kill_and_reap().await;
    }

    #[tokio::test]
    async fn semantic_failures_and_review_text_cannot_be_runner_failures() {
        let script = "printf '%s\\n' \
            '{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}' \
            '{\"type\":\"item.completed\",\"item\":{\"id\":\"cmd\",\"type\":\"command_execution\",\"command\":\"cargo test\",\"aggregated_output\":\"test failed\",\"exit_code\":1,\"status\":\"failed\"}}' \
            '{\"type\":\"item.completed\",\"item\":{\"id\":\"msg\",\"type\":\"agent_message\",\"text\":\"reported failed blocked needs-info; BLOCKING review finding\"}}' \
            '{\"type\":\"turn.completed\"}'";
        let mut proc = shell_proc(script).await;
        while proc.next_raw_line().await.is_some() {}
        let status = wait_for_exit(&mut proc).await;
        assert!(proc.classify_pre_authoritative_exit(status).is_none());
    }

    #[tokio::test]
    async fn codex_boundary_bounds_stderr_drains_terminal_output_and_reaps() {
        // 17618 bytes must stay above runner::DIAGNOSTIC_LINE_BYTES (16384) and 306
        // lines above runner::DIAGNOSTIC_CAPACITY (256) — both constants are private
        // to runner, so mirror the margins its own unit tests use (+1234 / +50).
        // `exec sleep` (not `sleep`) so the shell never forks a grandchild: killpg
        // races a concurrent fork(), and a child born after the kernel's group scan
        // starts with an empty pending-signal set, survives as an orphan, and holds
        // the stderr pipe open until it exits on its own — a 30s hang, not a deadlock.
        // One write makes observing provider.ready proof that the terminal record is queued.
        let mut proc = shell_proc(
            "printf '\\377invalid\\n' >&2; \
             head -c 17618 /dev/zero | tr '\\000' x >&2; echo >&2; \
             i=0; while [ $i -lt 306 ]; do echo codex-stderr-$i >&2; i=$((i+1)); done; \
             printf '%s\n%s\n' '{\"type\":\"provider.ready\"}' '{\"type\":\"turn.completed\"}'; \
             exec sleep 30",
        )
        .await;
        let pid = proc.pid().unwrap();
        // 30s catches a genuine deadlock without reporting CPU contention as one.
        let ready = tokio::time::timeout(std::time::Duration::from_secs(30), proc.next_raw_line())
            .await
            .expect("provider never finished stderr")
            .expect("provider stdout ended before ready");
        assert!(ready.contains("provider.ready"));
        let output = tokio::time::timeout(std::time::Duration::from_secs(30), proc.kill_and_reap())
            .await
            .expect("kill_and_reap deadlocked");
        assert!(
            output.len() <= 259,
            "two markers, bounded stderr, and one stdout"
        );
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::StderrTruncated { dropped } if *dropped > 0)
        ));
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::StderrBytesTruncated { dropped } if *dropped > 0)
        ));
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::Stdout(text) if text.contains("turn.completed"))
        ));
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::Stderr(text) if text == "codex-stderr-305")
        ));
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
    }
}
