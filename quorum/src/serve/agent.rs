//! AgentProc: spawn, feed, read, and kill one claude child process.

use super::runner::{
    capture_diagnostics, tool_summary, ActivityKind, AdapterConfig, AgentEvent, AgentKind,
    AgentMcpServer, CapturedOutput, DiagnosticBuffer, FailureDisposition, FailureObservation,
    FailureTracker, LaunchMode, LaunchRequest, NormalizedLine, RunnerFailure, TokenUsage,
};
use super::stream::{self, Event};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

pub struct AgentSpec {
    #[allow(dead_code)] // consumed when runner dispatch is added
    pub kind: AgentKind,
    pub model: String,
    pub effort: String,
    pub session_id: String,
    pub worktree: PathBuf,
    pub bare: bool,
    pub allowed_tools: String,
    pub env_vars: Vec<(String, String)>,
}

/// Fresh session id for a spawned agent. The claude CLI validates
/// `--session-id` as a UUID and exits before the first turn on anything else
/// ("Invalid session ID. Must be a valid UUID."), which the daemon only sees
/// as "process exited without response" — observed live 2026-07-10 as a
/// classifier respawn-loop. Every `AgentSpec.session_id` must come from here.
pub fn new_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct AgentProc {
    child: Child,
    stdin: tokio::process::ChildStdin,
    reader: BufReader<tokio::process::ChildStdout>,
    line_buffer: Vec<u8>,
    diagnostics: DiagnosticBuffer,
    failures: FailureTracker,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
}

/// Tool allowlist for spawned agents (dontAsk auto-denies everything else).
/// `Skill` is required so reviewers can invoke the pinned `pr-review` skill
/// (#206) — without it the Skill call is silently denied and the review
/// degrades to an unstructured read.
pub(crate) const ALLOWED_TOOLS: &str = "Bash,Read,Edit,Write,Glob,Grep,TodoWrite,WebFetch,Skill";
const AGENT_MCP_ALLOWED_TOOL: &str = "mcp__quorum__*";
/// Build a stream-json user turn. The claude CLI requires `message.role` and
/// exits 1 on the first message without it — every turn fed to an agent MUST
/// go through this helper (first live run died instantly on a role-less turn).
pub fn user_turn(content: &str) -> String {
    serde_json::json!({
        "type": "user",
        "message": { "role": "user", "content": content }
    })
    .to_string()
}

fn claude_mcp_config(server: AgentMcpServer) -> String {
    serde_json::json!({
        "mcpServers": {
            "quorum": {
                "command": server.command,
                "args": server.args,
            }
        }
    })
    .to_string()
}

impl AgentProc {
    /// Translate a neutral runner request into Claude's persistent stream-json
    /// process and feed its initial user turn.
    pub async fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        Self::launch_with_deadline(request, config, None).await
    }

    /// Launch a restricted role whose total turn deadline includes feeding
    /// the initial stdin payload. A provider that never reads cannot strand
    /// the caller before slot ownership exists: expiry kills and reaps the
    /// child before returning.
    pub async fn launch_restricted_until(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
        deadline: tokio::time::Instant,
    ) -> std::io::Result<Self> {
        if request.mode != LaunchMode::Restricted {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bounded restricted launch requires restricted mode",
            ));
        }
        Self::launch_with_deadline(request, config, Some(deadline)).await
    }

    async fn launch_with_deadline(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
        deadline: Option<tokio::time::Instant>,
    ) -> std::io::Result<Self> {
        let spec = AgentSpec {
            kind: AgentKind::Claude,
            model: request.model.to_string(),
            effort: request.effort.to_string(),
            session_id: request
                .continuation_id
                .map(str::to_owned)
                .unwrap_or_else(new_session_id),
            worktree: request.worktree.to_path_buf(),
            bare: config.claude_bare,
            allowed_tools: config.claude_allowed_tools.to_string(),
            env_vars: request.environment.to_vec(),
        };
        let mut proc = match request.mode {
            LaunchMode::Normal => Self::spawn_configured(
                &spec,
                config.executable,
                RestrictedMode::None,
                request.agent_mcp_server(),
            )?,
            LaunchMode::Restricted => Self::spawn_restricted(&spec, config.executable)?,
        };
        let turn = user_turn(request.prompt);
        let fed = match deadline {
            Some(deadline) => proc.feed_turn_until(&turn, deadline).await,
            None => proc.feed_turn(&turn).await,
        };
        if let Err(error) = fed {
            let _ = proc.kill_and_reap().await;
            return Err(error);
        }
        Ok(proc)
    }

    pub fn normalize_line(raw: &str) -> NormalizedLine {
        let event = stream::parse_line(raw);
        let terminal_text = match &event {
            Some(Event::Result { result, .. }) => Some(stream::result_text(result)),
            _ => None,
        };
        NormalizedLine {
            events: event.map(normalize_event).unwrap_or_default(),
            terminal_text,
        }
    }

    pub(super) fn failure_observation(raw: &str) -> FailureObservation {
        if raw.is_empty() {
            return FailureObservation::inert();
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
            return FailureObservation::classified(
                FailureDisposition::NonFailover,
                "Claude emitted malformed stream-json",
            );
        };
        match value.get("type").and_then(|kind| kind.as_str()) {
            Some("assistant") => {
                let error = value.get("error").and_then(|error| error.as_str());
                match error {
                    Some("authentication_failed" | "billing_error" | "credit_balance_too_low") => {
                        FailureObservation::classified(
                            FailureDisposition::ProviderUnavailable,
                            format!(
                                "Claude structured provider error: {}",
                                error.expect("matched Some")
                            ),
                        )
                    }
                    Some("rate_limit") if claude_quota_text(&claude_assistant_text(&value)) => {
                        FailureObservation::classified(
                            FailureDisposition::ProviderUnavailable,
                            "Claude reported an account/session quota limit",
                        )
                    }
                    Some("rate_limit") => FailureObservation::unknown_failure(
                        "Claude rate limit did not prove account/session quota",
                    ),
                    Some("model_not_found" | "invalid_model" | "model_unavailable") => {
                        FailureObservation::classified(
                            FailureDisposition::ProfileUnavailable,
                            format!(
                                "Claude structured model error: {}",
                                error.expect("matched Some")
                            ),
                        )
                    }
                    Some("overloaded_error") => FailureObservation::classified(
                        FailureDisposition::ProviderUnavailable,
                        "Claude reported provider overload",
                    ),
                    Some("connection_error") => FailureObservation::classified(
                        FailureDisposition::RetryableSameRoute,
                        "Claude reported a connection error",
                    ),
                    Some(other)
                        if value.get("is_api_error_message").and_then(|v| v.as_bool())
                            == Some(true) =>
                    {
                        FailureObservation::unknown_failure(format!(
                            "Claude emitted unclassified API error code {other}"
                        ))
                    }
                    _ => FailureObservation::inert(),
                }
            }
            Some("rate_limit_event")
                if value
                    .pointer("/rate_limit_info/status")
                    .and_then(|v| v.as_str())
                    == Some("rejected") =>
            {
                FailureObservation::classified(
                    FailureDisposition::ProviderUnavailable,
                    "Claude rejected the request at the account quota boundary",
                )
            }
            Some("result") if value.get("is_error").and_then(|v| v.as_bool()) == Some(true) => {
                let status = value.get("api_error_status").and_then(|v| v.as_u64());
                let result = value.get("result").and_then(|v| v.as_str()).unwrap_or("");
                if status == Some(429) && claude_quota_text(result) {
                    return FailureObservation::classified(
                        FailureDisposition::ProviderUnavailable,
                        "Claude returned API status 429 with account/session quota evidence",
                    );
                }
                if status == Some(429) {
                    return FailureObservation::unknown_failure(
                        "Claude API status 429 did not prove account/session quota",
                    );
                }
                if matches!(status, Some(500 | 502 | 503 | 529)) {
                    return FailureObservation::classified(
                        FailureDisposition::ProviderUnavailable,
                        format!(
                            "Claude provider outage status {}",
                            status.expect("matched Some")
                        ),
                    );
                }
                if status == Some(404) && claude_model_unavailable_text(result) {
                    return FailureObservation::classified(
                        FailureDisposition::ProfileUnavailable,
                        "Claude reported the selected model unavailable",
                    );
                }
                if claude_auth_text(result) {
                    return FailureObservation::classified(
                        FailureDisposition::ProviderUnavailable,
                        "Claude reported missing authentication",
                    );
                }
                FailureObservation::unknown_failure(
                    "Claude terminal error did not match a bounded provider signal",
                )
            }
            Some("result") => FailureObservation::success(),
            _ => FailureObservation::inert(),
        }
    }

    pub(super) fn stderr_failure_observation(text: &str) -> FailureObservation {
        if text.len() > 16 * 1024 {
            return FailureObservation::unknown_failure(
                "Claude stderr exceeded classification bound",
            );
        }
        if text == "Invalid session ID. Must be a valid UUID."
            || text.starts_with("error: unexpected argument")
            || text.starts_with("error: invalid value")
            || text.starts_with("error: unrecognized option")
        {
            return FailureObservation::classified(
                FailureDisposition::NonFailover,
                "Claude rejected the execution protocol or arguments",
            );
        }
        if text.is_empty() {
            FailureObservation::inert()
        } else {
            FailureObservation::unknown_failure(
                "Claude stderr did not match a bounded provider signal",
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

    pub fn spawn(spec: &AgentSpec, agent_bin: Option<&str>) -> std::io::Result<Self> {
        Self::spawn_configured(spec, agent_bin, RestrictedMode::None, None)
    }

    /// Spawn a closed-book classifier while preserving the configured auth
    /// path. `--setting-sources ""` loads no user/project settings, hooks, or
    /// plugins, without the credential restrictions of `--bare`; the empty
    /// tool surface prevents the classifier from acquiring context beyond its
    /// supplied turn and the repo CLAUDE.md injected below.
    pub fn spawn_restricted(spec: &AgentSpec, agent_bin: Option<&str>) -> std::io::Result<Self> {
        Self::spawn_configured(spec, agent_bin, RestrictedMode::ClosedBook, None)
    }

    /// Spawn a read-only planning turn. Unlike the closed-book classifier,
    /// the planner may inspect the frozen repository, but cannot invoke Bash,
    /// write files, persist a provider session, or load user/project
    /// settings, hooks, or plugins (`--setting-sources ""`). The repo
    /// CLAUDE.md is injected explicitly via `--append-system-prompt-file`
    /// when present, since Claude's native CLAUDE.md discovery is gated on
    /// the `project` settings source that this closes.
    ///
    /// `agent_mcp` is `Some` only for a planner carrying a complete managed run
    /// envelope. It adds exactly one tool — `submit_plan` — and the planner
    /// keeps its read-only `--tools` surface.
    ///
    /// The tool surface narrows what the planner reaches for; it is not what
    /// bounds the capability now in its environment. A `planner` capability is
    /// honored by `SubmitPlan` alone — `resolve_live_run_context`
    /// (`quorum-core/src/capabilities.rs:113-116,140`) admits only the `worker`
    /// and `reviewer` roles — under a once-only guard and a bounded rejection
    /// budget.
    #[allow(dead_code)] // consumed by the pending daemon decomposition coordinator
    pub fn spawn_planner(
        spec: &AgentSpec,
        agent_bin: Option<&str>,
        agent_mcp: Option<AgentMcpServer>,
    ) -> std::io::Result<Self> {
        Self::spawn_configured(spec, agent_bin, RestrictedMode::Planner, agent_mcp)
    }

    fn spawn_configured(
        spec: &AgentSpec,
        agent_bin: Option<&str>,
        restricted: RestrictedMode,
        agent_mcp: Option<AgentMcpServer>,
    ) -> std::io::Result<Self> {
        let bin = agent_bin.unwrap_or("claude");
        let mut cmd = Command::new(bin);
        cmd.arg("-p")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--model")
            .arg(&spec.model)
            .arg("--effort")
            .arg(&spec.effort);

        cmd.arg("--session-id").arg(&spec.session_id);

        if let Some(server) = agent_mcp {
            cmd.arg("--mcp-config")
                .arg(claude_mcp_config(server))
                .arg("--strict-mcp-config");
        }

        if restricted != RestrictedMode::None {
            cmd.arg("--setting-sources")
                .arg("")
                .arg("--disable-slash-commands")
                .arg("--tools")
                .arg(match restricted {
                    RestrictedMode::Planner => "Read,Glob,Grep",
                    _ => "",
                })
                .arg("--no-session-persistence");
            if restricted == RestrictedMode::Planner {
                cmd.arg("--add-dir").arg(&spec.worktree);
            }
            // `--setting-sources ""` gates Claude's native CLAUDE.md
            // discovery along with settings/hooks/plugins, so inject the
            // repo CLAUDE.md explicitly — but only when it exists; the CLI
            // fails argument validation on a missing
            // `--append-system-prompt-file` path.
            let claude_md = spec.worktree.join("CLAUDE.md");
            if claude_md.is_file() {
                cmd.arg("--append-system-prompt-file").arg(&claude_md);
            }
        } else {
            cmd.arg("--add-dir").arg(&spec.worktree);
        }

        // In dontAsk mode every tool call OUTSIDE the allowlist is auto-denied
        // (there is no human to ask). Without --allowedTools a managed agent
        // cannot edit files, run git/gh, or signal `quorum submit` — it stalls
        // forever in awaiting-review (observed second live run). Restricted
        // classifier spawns pass an empty list in addition to `--tools ""`.
        //
        // A planner never receives the server-wide wildcard: its MCP surface is
        // exactly `submit_plan`, so any other managed tool the server advertises
        // stays auto-denied.
        let mcp_allowed_tool = match restricted {
            RestrictedMode::Planner => crate::serve::runner::PLANNER_MCP_ALLOWED_TOOL,
            _ => AGENT_MCP_ALLOWED_TOOL,
        };
        let allowed_tools = match agent_mcp {
            Some(_) if spec.allowed_tools.is_empty() => mcp_allowed_tool.to_string(),
            Some(_)
                if !spec
                    .allowed_tools
                    .split(',')
                    .any(|tool| tool == mcp_allowed_tool) =>
            {
                format!("{},{}", spec.allowed_tools, mcp_allowed_tool)
            }
            _ => spec.allowed_tools.clone(),
        };
        cmd.arg("--permission-mode")
            .arg("dontAsk")
            .arg("--allowedTools")
            .arg(allowed_tools);

        if spec.bare {
            cmd.arg("--bare");
        }

        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
        }
        if agent_mcp.is_some() {
            strip_managed_mcp_authority(&mut cmd);
        }
        // A planner without an MCP server holds no endpoint authority at all,
        // so every coordination name is removed. A planner with one must keep
        // its run envelope: `claude_mcp_config` carries no `env` block, so the
        // stdio MCP child inherits these names from this process. GitHub and
        // daemon-home authority is still stripped above.
        if restricted == RestrictedMode::Planner && agent_mcp.is_none() {
            strip_coordination_env(&mut cmd);
        }

        cmd.current_dir(&spec.worktree);
        cmd.stdin(Stdio::piped())
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
        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let reader = BufReader::new(stdout);
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Claude);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });

        Ok(Self {
            child,
            stdin,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        })
    }

    pub async fn feed_turn(&mut self, json_turn: &str) -> std::io::Result<()> {
        self.failures.begin_turn();
        self.stdin.write_all(json_turn.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    pub async fn feed_turn_until(
        &mut self,
        json_turn: &str,
        deadline: tokio::time::Instant,
    ) -> std::io::Result<()> {
        match tokio::time::timeout_at(deadline, self.feed_turn(json_turn)).await {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "provider stdin feed timed out",
            )),
        }
    }

    /// Return the next raw stdout line, or `None` on EOF/error.
    /// Used by the daemon to preserve verbatim JSONL in session logs.
    pub async fn next_raw_line(&mut self) -> Option<String> {
        self.read_raw_line(None).await.ok().flatten()
    }

    /// Read one provider line while bounding the partial, not-yet-delimited
    /// allocation. The buffer lives on the process so cancelling a polling
    /// future cannot discard bytes already consumed from the pipe.
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
                    return Err(line_limit_error(max_bytes.expect("checked limit")));
                }
                self.line_buffer.extend_from_slice(&available[..newline]);
                self.reader.consume(newline + 1);
                return self.take_buffered_line();
            }

            if max_bytes
                .is_some_and(|limit| self.line_buffer.len().saturating_add(available.len()) > limit)
            {
                return Err(line_limit_error(max_bytes.expect("checked limit")));
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
                    .observe_protocol_read_error(format!("Claude stdout is not UTF-8: {error}"));
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("provider stdout is not UTF-8: {error}"),
                )
            })
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        loop {
            match self.read_raw_line(None).await {
                Ok(Some(line)) => {
                    if let Some(event) = stream::parse_line(&line) {
                        return Some(event);
                    }
                }
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }

    /// Non-blocking check for child exit. Returns `Some(status)` if the child
    /// has already terminated, `None` if still running. `try_wait` also reaps
    /// the child on the caller's behalf when it has exited.
    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub fn drain_diagnostics(&mut self) -> Vec<CapturedOutput> {
        self.diagnostics.drain()
    }

    #[cfg(test)]
    pub fn from_parts(
        child: Child,
        stdin: tokio::process::ChildStdin,
        reader: BufReader<tokio::process::ChildStdout>,
    ) -> Self {
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Claude);
        let failures = diagnostics.failures();
        Self {
            child,
            stdin,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: None,
        }
    }

    pub async fn kill_and_reap(mut self) -> Vec<CapturedOutput> {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
        // Reap the child to avoid zombie accumulation
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

fn claude_auth_text(text: &str) -> bool {
    text.len() <= 4096
        && (text == "Not logged in · Please run /login"
            || text == "Not logged in. Please run /login")
}

fn claude_assistant_text(value: &serde_json::Value) -> String {
    let Some(content) = value.pointer("/message/content").and_then(|v| v.as_array()) else {
        return String::new();
    };
    let mut text = String::new();
    for fragment in content
        .iter()
        .filter_map(|item| item.get("text").and_then(|value| value.as_str()))
    {
        let separator = usize::from(!text.is_empty());
        if text
            .len()
            .saturating_add(separator)
            .saturating_add(fragment.len())
            > 4096
        {
            return String::new();
        }
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(fragment);
    }
    text
}

fn claude_quota_text(text: &str) -> bool {
    if text.is_empty() || text.len() > 4096 {
        return false;
    }
    let text = text.to_ascii_lowercase();
    text.contains("session limit")
        || text.contains("account quota")
        || text.contains("credit balance is too low")
        || text.contains("billing hard limit")
        || text.contains("spend limit")
}

fn claude_model_unavailable_text(text: &str) -> bool {
    text.len() <= 4096
        && text.contains("model")
        && (text.contains("not found")
            || text.contains("does not exist")
            || text.contains("not available"))
}

fn line_limit_error(limit: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("provider stdout line exceeded {limit}-byte limit"),
    )
}

fn normalize_event(event: Event) -> Vec<AgentEvent> {
    match event {
        Event::Result {
            result,
            usage,
            total_cost_usd,
            is_error,
            ..
        } => {
            let usage = usage.map(|usage| TokenUsage {
                input_tokens: usage.input_tokens,
                uncached_input_tokens: usage
                    .input_tokens
                    .saturating_sub(usage.cache_read_input_tokens),
                cached_input_tokens: usage.cache_read_input_tokens,
                cache_write_input_tokens: usage.cache_creation_input_tokens,
                output_tokens: usage.output_tokens,
                reasoning_tokens: 0,
            });
            if is_error.unwrap_or(false) {
                let message = stream::result_text(&result);
                vec![AgentEvent::TurnFailed {
                    message: if message.is_empty() {
                        "agent returned an error result".into()
                    } else {
                        message
                    },
                    usage,
                    cost_usd: total_cost_usd,
                }]
            } else {
                vec![AgentEvent::TurnCompleted {
                    usage,
                    cost_usd: total_cost_usd,
                }]
            }
        }
        Event::Assistant { message } => {
            let mut events = Vec::new();
            if let Some(text) = stream::assistant_text(&message) {
                events.push(AgentEvent::AssistantText { text });
            }
            if let Some(blocks) = message
                .get("content")
                .and_then(|content| content.as_array())
            {
                for block in blocks {
                    if block.get("type").and_then(|kind| kind.as_str()) == Some("tool_use") {
                        if let Some(name) = block.get("name").and_then(|name| name.as_str()) {
                            let input = block
                                .get("input")
                                .cloned()
                                .unwrap_or(serde_json::Value::Null);
                            events.push(AgentEvent::Activity {
                                kind: ActivityKind::ToolUse,
                                summary: tool_summary(name, &input),
                            });
                        }
                    }
                }
            }
            if let Some(usage) = message.get("usage") {
                let input = usage
                    .get("input_tokens")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                let output = usage
                    .get("output_tokens")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                if input + output > 0 {
                    events.push(AgentEvent::MidTurnUsage {
                        tokens: input + output,
                    });
                }
            }
            events
        }
        Event::ToolUse { name, input } => vec![AgentEvent::Activity {
            kind: ActivityKind::ToolUse,
            summary: tool_summary(&name, &input),
        }],
        Event::Other => vec![],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RestrictedMode {
    None,
    ClosedBook,
    Planner,
}

/// The daemon endpoint is capability-scoped by the four QUORUM_* run fields.
/// Do not let the provider forward unrelated operator credentials to its
/// nested stdio child: the endpoint must never acquire ambient GitHub or
/// Quorum-home authority merely because Claude happens to inherit it.
fn strip_managed_mcp_authority(cmd: &mut Command) {
    for name in ["GH_TOKEN", "GITHUB_TOKEN", "QUORUM_HOME", "GH_CONFIG_DIR"] {
        cmd.env_remove(name);
    }
}

fn strip_coordination_env(cmd: &mut Command) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_usage_separates_uncached_and_cache_read_input() {
        let event = stream::parse_line(
            r#"{"type":"result","result":"done","usage":{"input_tokens":100,"cache_read_input_tokens":80,"cache_creation_input_tokens":5,"output_tokens":7}}"#,
        )
        .unwrap();
        let events = normalize_event(event);
        match events.as_slice() {
            [AgentEvent::TurnCompleted {
                usage: Some(usage), ..
            }] => {
                assert_eq!(usage.input_tokens, 100);
                assert_eq!(usage.uncached_input_tokens, 20);
                assert_eq!(usage.cached_input_tokens, 80);
                assert_eq!(usage.cache_write_input_tokens, 5);
                assert_eq!(usage.output_tokens, 7);
            }
            other => panic!("expected completed event, got {other:?}"),
        }
    }

    async fn shell_proc(script: &str) -> AgentProc {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(script)
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
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let reader = BufReader::new(child.stdout.take().unwrap());
        let stderr = BufReader::new(child.stderr.take().unwrap());
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Claude);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        AgentProc {
            child,
            stdin,
            reader,
            line_buffer: Vec::new(),
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
        }
    }

    /// The claude CLI rejects any non-UUID --session-id before the first turn;
    /// a formatted string here respawn-loops the daemon (observed 2026-07-10).
    #[test]
    fn session_id_is_valid_uuid() {
        let sid = new_session_id();
        assert!(
            uuid::Uuid::parse_str(&sid).is_ok(),
            "claude CLI rejects any non-UUID --session-id, got: {sid}"
        );
    }

    /// Zero-token contract tests against the REAL installed claude CLI.
    ///
    /// Both 2026-07-10 live incidents (non-UUID --session-id crash-loop, then
    /// bare-agent "Not logged in" crash-loop) failed at the CLI boundary
    /// *before* any API call — fake_agent accepts anything, so only the real
    /// binary can catch them. These tests guarantee zero token spend by
    /// pointing CLAUDE_CONFIG_DIR at an empty dir and blanking every
    /// credential env var: the run can reach auth, never the API.
    ///
    /// Skipped (pass with a note) when no `claude` is on PATH (e.g. CI).
    fn claude_available() -> bool {
        std::process::Command::new("claude")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn no_auth_env(tmp: &std::path::Path) -> Vec<(String, String)> {
        vec![
            ("CLAUDE_CONFIG_DIR".into(), tmp.display().to_string()),
            ("ANTHROPIC_API_KEY".into(), String::new()),
            ("ANTHROPIC_AUTH_TOKEN".into(), String::new()),
            ("CLAUDE_CODE_OAUTH_TOKEN".into(), String::new()),
        ]
    }

    fn classifier_spec(worktree: &std::path::Path, bare: bool) -> AgentSpec {
        AgentSpec {
            kind: AgentKind::Claude,
            model: crate::serve::classifier::CLASSIFIER_MODEL.to_string(),
            effort: crate::serve::classifier::CLASSIFIER_EFFORT.to_string(),
            session_id: new_session_id(),
            worktree: worktree.to_path_buf(),
            bare,
            allowed_tools: String::new(),
            env_vars: vec![],
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_mcp_claude_child_scrubs_ambient_authority() {
        use std::ffi::OsString;
        use std::os::unix::fs::PermissionsExt;

        static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

        struct EnvRestore {
            name: &'static str,
            previous: Option<OsString>,
        }

        impl EnvRestore {
            fn poison(name: &'static str, value: &str) -> Self {
                let previous = std::env::var_os(name);
                std::env::set_var(name, value);
                Self { name, previous }
            }
        }

        impl Drop for EnvRestore {
            fn drop(&mut self) {
                if let Some(value) = self.previous.take() {
                    std::env::set_var(self.name, value);
                } else {
                    std::env::remove_var(self.name);
                }
            }
        }

        let _environment = ENV_LOCK.lock().await;
        let _gh = EnvRestore::poison("GH_TOKEN", "ambient-gh-token");
        let _github = EnvRestore::poison("GITHUB_TOKEN", "ambient-github-token");
        let _quorum_home = EnvRestore::poison("QUORUM_HOME", "/ambient/quorum-home");
        let _gh_config = EnvRestore::poison("GH_CONFIG_DIR", "/ambient/gh-config");

        let root = tempfile::tempdir().unwrap();
        let capture = root.path().join("mcp-child-environment.log");
        let fake_quorum = root.path().join("quorum");
        std::fs::write(
            &fake_quorum,
            format!(
                "#!/bin/sh\n\\
                 {{\n\\
                   printf 'arg=%s\\n' \"$1\"\n\\
                   printf 'repo=%s\\n' \"$QUORUM_REPO\"\n\\
                   printf 'agent=%s\\n' \"$QUORUM_AGENT\"\n\\
                   printf 'run=%s\\n' \"$QUORUM_RUN_ID\"\n\\
                   printf 'endpoint=%s\\n' \"$QUORUM_AGENT_ENDPOINT\"\n\\
                   printf 'gh=%s\\n' \"$GH_TOKEN\"\n\\
                   printf 'github=%s\\n' \"$GITHUB_TOKEN\"\n\\
                   printf 'quorum_home=%s\\n' \"$QUORUM_HOME\"\n\\
                   printf 'gh_config=%s\\n' \"$GH_CONFIG_DIR\"\n\\
                 }} > '{}'\n",
                capture.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_quorum, std::fs::Permissions::from_mode(0o755)).unwrap();
        let fake_claude = root.path().join("claude");
        std::fs::write(
            &fake_claude,
            "#!/bin/sh\nquorum agent-mcp\nprintf '%s\\n' '{\"type\":\"result\",\"result\":\"done\",\"is_error\":false}'\nwhile IFS= read -r _line; do :; done\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_claude, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut spec = classifier_spec(root.path(), true);
        spec.env_vars = vec![
            (
                "PATH".into(),
                format!(
                    "{}:{}",
                    root.path().display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            ),
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Worker-test".into()),
            ("QUORUM_RUN_ID".into(), "run-capability".into()),
            (
                "QUORUM_AGENT_ENDPOINT".into(),
                "/tmp/quorum-agent.sock".into(),
            ),
        ];
        let proc = AgentProc::spawn_configured(
            &spec,
            fake_claude.to_str(),
            RestrictedMode::None,
            Some(crate::serve::runner::AGENT_MCP_SERVER),
        )
        .unwrap();
        for _ in 0..100 {
            if capture.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let _ = proc.kill_and_reap().await;

        let captured = std::fs::read_to_string(&capture)
            .expect("managed Claude did not launch its configured MCP child");
        assert!(captured.contains("arg=agent-mcp\n"), "{captured}");
        assert!(captured.contains("repo=owner/repo\n"), "{captured}");
        assert!(captured.contains("agent=Worker-test\n"), "{captured}");
        assert!(captured.contains("run=run-capability\n"), "{captured}");
        assert!(
            captured.contains("endpoint=/tmp/quorum-agent.sock\n"),
            "{captured}"
        );
        for absent in ["gh=\n", "github=\n", "quorum_home=\n", "gh_config=\n"] {
            assert!(captured.contains(absent), "{captured}");
        }
    }

    /// Positive contract: a production-built spec must clear the CLI's
    /// argument validation. Any stream event back (init, assistant,
    /// result — even an auth-failure result) proves the args parsed;
    /// instant exit with no events is exactly the crash-loop signature.
    #[tokio::test]
    async fn real_cli_accepts_production_agent_spec_args() {
        if !claude_available() {
            eprintln!("skipped: no claude binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let mut spec = classifier_spec(tmp.path(), true);
        spec.env_vars = no_auth_env(tmp.path());
        spec.env_vars.extend([
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Keel-test".into()),
            ("QUORUM_RUN_ID".into(), "run-capability".into()),
            (
                "QUORUM_AGENT_ENDPOINT".into(),
                "/tmp/quorum-agent-mcp-boundary.sock".into(),
            ),
        ]);

        let mut proc = AgentProc::spawn_configured(
            &spec,
            None,
            RestrictedMode::None,
            Some(crate::serve::runner::AGENT_MCP_SERVER),
        )
        .expect("spawn claude with strict MCP config");
        proc.feed_turn(&user_turn("ping")).await.expect("feed turn");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("claude produced no event within 60s — args may hang the CLI");
        let _terminal_output = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "claude exited without emitting any stream event — the AgentSpec \
             argument surface was rejected at CLI validation (crash-loop class)"
        );
    }

    /// The closed-book Claude launch surface must remain accepted by the real
    /// CLI while using normal (non-bare) auth semantics.
    #[tokio::test]
    async fn real_cli_accepts_restricted_classifier_spec_args() {
        if !claude_available() {
            eprintln!("skipped: no claude binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let mut spec = classifier_spec(tmp.path(), false);
        spec.env_vars = no_auth_env(tmp.path());

        let mut proc = AgentProc::spawn_restricted(&spec, None).expect("spawn claude");
        proc.feed_turn(&user_turn("ping")).await.expect("feed turn");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("restricted claude produced no event within 60s — args may hang the CLI");
        let _terminal_output = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "claude exited without emitting any stream event — the restricted \
             classifier argument surface was rejected at CLI validation"
        );
    }

    /// Planner-specific read-only arguments must clear the real CLI parser.
    /// Blanked credentials make this a zero-token boundary test.
    #[tokio::test]
    async fn real_cli_accepts_restricted_planner_spec_args() {
        if !claude_available() {
            eprintln!("skipped: no claude binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let mut spec = classifier_spec(tmp.path(), false);
        spec.model = "claude-opus-4-6".into();
        spec.effort = "high".into();
        spec.allowed_tools = "Read,Glob,Grep".into();
        spec.env_vars = no_auth_env(tmp.path());

        let mut proc = AgentProc::spawn_planner(&spec, None, None).expect("spawn planner claude");
        proc.feed_turn(&user_turn("return an empty JSON object"))
            .await
            .expect("feed planner turn");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("planner claude produced no event within 60s");
        let _ = proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "claude rejected the planner argument boundary before authentication"
        );
    }

    /// Pins the bug this PR fixes: `--safe-mode` accepted `--mcp-config`
    /// without error but silently never spawned the inline MCP server, so
    /// `submit_plan` never existed in the planner's session. This spawns the
    /// REAL claude CLI with the planner+MCP arg set (now
    /// `--setting-sources ""` based) and a marker-script MCP server, and
    /// asserts the marker file appears — proving the server actually
    /// spawns under the new flag set. Zero-token: credentials are blanked,
    /// so if MCP spawn turns out to be gated behind auth in this
    /// environment, this falls back to asserting the full new arg set is
    /// accepted by the CLI (reaches the auth stage) rather than rejected at
    /// argument parsing.
    #[cfg(unix)]
    #[tokio::test]
    async fn real_cli_spawns_planner_mcp_server_under_new_flags() {
        use std::os::unix::fs::PermissionsExt;

        if !claude_available() {
            eprintln!("skipped: no claude binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("mcp-spawned");
        let marker_script = tmp.path().join("mcp-marker.sh");
        std::fs::write(
            &marker_script,
            format!("#!/bin/sh\ntouch '{}'\nexec cat\n", marker.display()),
        )
        .unwrap();
        std::fs::set_permissions(&marker_script, std::fs::Permissions::from_mode(0o755)).unwrap();

        // AgentMcpServer's fields are 'static str/slice; leaking is the
        // simplest way to hand it a tempdir-scoped command path in a test.
        let command: &'static str = Box::leak(marker_script.display().to_string().into_boxed_str());
        let server = AgentMcpServer {
            command,
            args: &[],
            env_vars: &[],
        };

        let mut spec = classifier_spec(tmp.path(), false);
        spec.model = "claude-opus-4-6".into();
        spec.effort = "high".into();
        spec.allowed_tools = "Read,Glob,Grep".into();
        spec.env_vars = no_auth_env(tmp.path());

        let mut proc = AgentProc::spawn_planner(&spec, None, Some(server))
            .expect("spawn planner claude with mcp marker server");
        proc.feed_turn(&user_turn("return an empty JSON object"))
            .await
            .expect("feed planner turn");
        let event =
            tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event()).await;
        let _ = proc.kill_and_reap().await;

        if marker.exists() {
            // Best outcome: proves the inline MCP server actually spawned
            // under the new --setting-sources flag set.
            return;
        }
        // Documented fallback: MCP server spawn may be gated behind auth in
        // a no-auth environment. Confirm the CLI at least accepted the full
        // new arg set (reached the auth stage) instead of rejecting it at
        // argument validation.
        match event {
            Ok(Some(_)) => {}
            other => panic!(
                "claude neither spawned the MCP marker nor emitted a stream event \
                 (arg set may have been rejected at CLI validation): {other:?}"
            ),
        }
    }

    fn planner_spec(worktree: &std::path::Path) -> AgentSpec {
        let mut spec = classifier_spec(worktree, false);
        spec.model = "claude-opus-4-6".into();
        spec.effort = "high".into();
        spec.allowed_tools = "Read,Glob,Grep".into();
        spec
    }

    /// Capture the exact argv a planner spawn hands the Claude CLI. The fake
    /// binary renames its capture into place so a partially written file can
    /// never be read as the complete argument surface.
    #[cfg(unix)]
    async fn capture_planner_args(
        dir: &std::path::Path,
        spec: &AgentSpec,
        agent_mcp: Option<AgentMcpServer>,
    ) -> Vec<String> {
        use std::os::unix::fs::PermissionsExt;

        let args_path = dir.join("planner-args");
        let fake = dir.join("claude");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{0}.tmp'\nmv '{0}.tmp' '{0}'\n\
                 while IFS= read -r _line; do :; done\n",
                args_path.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let proc = AgentProc::spawn_planner(spec, fake.to_str(), agent_mcp).unwrap();
        for _ in 0..200 {
            if args_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let captured = std::fs::read_to_string(&args_path)
            .expect("fake planner did not capture its bounded argument surface");
        let _ = proc.kill_and_reap().await;
        captured.lines().map(str::to_string).collect()
    }

    fn flag_value<'a>(args: &'a [String], flag: &str) -> &'a str {
        let index = args
            .iter()
            .position(|arg| arg == flag)
            .unwrap_or_else(|| panic!("{flag} missing from planner arguments {args:?}"));
        args.get(index + 1)
            .unwrap_or_else(|| panic!("{flag} carried no value in {args:?}"))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_launch_passes_no_provider_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = planner_spec(tmp.path());
        let args = capture_planner_args(tmp.path(), &spec, None).await;
        // Owner decision: no provider spend ceiling on planning turns for now.
        assert!(!args.iter().any(|arg| arg == "--max-budget-usd"));
    }

    /// The planner's MCP surface is exactly one tool. The server-wide wildcard
    /// workers and reviewers carry would hand the planner every managed
    /// GitHub and lifecycle operation the endpoint advertises.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_planner_with_mcp_allows_only_submit_plan() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = planner_spec(tmp.path());
        let args = capture_planner_args(
            tmp.path(),
            &spec,
            Some(crate::serve::runner::AGENT_MCP_SERVER),
        )
        .await;

        assert!(
            args.iter().any(|arg| arg == "--strict-mcp-config"),
            "{args:?}"
        );
        let config: serde_json::Value =
            serde_json::from_str(flag_value(&args, "--mcp-config")).expect("mcp config is JSON");
        assert_eq!(config["mcpServers"]["quorum"]["command"], "quorum");
        assert_eq!(
            config["mcpServers"]["quorum"]["args"],
            serde_json::json!(["agent-mcp"])
        );
        assert_eq!(
            crate::serve::runner::PLANNER_MCP_ALLOWED_TOOL,
            "mcp__quorum__submit_plan"
        );

        let allowed = flag_value(&args, "--allowedTools");
        assert!(
            allowed
                .split(',')
                .any(|tool| tool == crate::serve::runner::PLANNER_MCP_ALLOWED_TOOL),
            "planner allowlist {allowed} omits submit_plan"
        );
        assert_eq!(
            allowed
                .split(',')
                .filter(|tool| tool.starts_with("mcp__"))
                .collect::<Vec<_>>(),
            vec![crate::serve::runner::PLANNER_MCP_ALLOWED_TOOL],
            "planner MCP allowlist {allowed} must agree with its quorum server key"
        );
        assert!(
            !allowed.contains("mcp__quorum__*"),
            "planner allowlist {allowed} carries the server-wide wildcard"
        );
        // The read-only inspection surface must survive: dontAsk auto-denies
        // every tool outside --allowedTools.
        for tool in ["Read", "Glob", "Grep"] {
            assert!(
                allowed.split(',').any(|allowed| allowed == tool),
                "planner allowlist {allowed} dropped {tool}"
            );
        }
        assert_eq!(flag_value(&args, "--tools"), "Read,Glob,Grep");
        assert_eq!(flag_value(&args, "--setting-sources"), "");
        assert!(
            !args.iter().any(|arg| arg == "--safe-mode"),
            "planner still carries the flag that silently suppressed its MCP server: {args:?}"
        );
    }

    /// `None` must leave the historical planner surface byte-identical: no MCP
    /// flags and no MCP tool anywhere in the allowlist.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_planner_without_envelope_has_no_mcp() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = planner_spec(tmp.path());
        let args = capture_planner_args(tmp.path(), &spec, None).await;

        for absent in ["--mcp-config", "--strict-mcp-config"] {
            assert!(
                !args.iter().any(|arg| arg == absent),
                "tool-less planner carries {absent}: {args:?}"
            );
        }
        assert!(
            !args.iter().any(|arg| arg.contains("mcp__")),
            "tool-less planner carries an MCP tool: {args:?}"
        );
        assert_eq!(flag_value(&args, "--allowedTools"), "Read,Glob,Grep");
        assert_eq!(flag_value(&args, "--tools"), "Read,Glob,Grep");
        assert_eq!(flag_value(&args, "--setting-sources"), "");
        assert!(
            !args.iter().any(|arg| arg == "--safe-mode"),
            "tool-less planner still carries --safe-mode: {args:?}"
        );
    }

    /// `--setting-sources ""` gates Claude's native CLAUDE.md discovery, so a
    /// restricted spawn must inject it explicitly — but only when the file
    /// actually exists in the spec's worktree; a missing path would fail CLI
    /// argument validation for `--append-system-prompt-file`.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_planner_injects_claude_md_only_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = planner_spec(tmp.path());
        let args = capture_planner_args(tmp.path(), &spec, None).await;
        assert!(
            !args.iter().any(|arg| arg == "--append-system-prompt-file"),
            "planner injected a nonexistent CLAUDE.md: {args:?}"
        );

        let with_claude_md = tempfile::tempdir().unwrap();
        std::fs::write(
            with_claude_md.path().join("CLAUDE.md"),
            "# Repo customizations\n",
        )
        .unwrap();
        let spec_with_md = planner_spec(with_claude_md.path());
        let args_with_md = capture_planner_args(with_claude_md.path(), &spec_with_md, None).await;
        let injected = flag_value(&args_with_md, "--append-system-prompt-file");
        assert_eq!(
            std::fs::canonicalize(injected).unwrap(),
            std::fs::canonicalize(with_claude_md.path().join("CLAUDE.md")).unwrap(),
        );
    }

    /// The stdio MCP child inherits the planner process environment, so the run
    /// envelope must survive exactly when the tool is attached — and never when
    /// it is not. GitHub credentials never survive either way.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_planner_run_envelope_survives_only_with_mcp() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let capture = tmp.path().join("env");
        let fake = tmp.path().join("claude");
        std::fs::write(
            &fake,
            format!(
                r#"#!/bin/sh
printf '{{"repo":"%s","agent":"%s","run":"%s","endpoint":"%s","home":"%s","gh":"%s"}}\n' \
  "$QUORUM_REPO" "$QUORUM_AGENT" "$QUORUM_RUN_ID" "$QUORUM_AGENT_ENDPOINT" \
  "$QUORUM_HOME" "$GH_TOKEN" > '{0}.tmp'
mv '{0}.tmp' '{0}'
while IFS= read -r _line; do :; done
"#,
                capture.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let envelope = vec![
            ("QUORUM_REPO".to_string(), "owner/repo".to_string()),
            ("QUORUM_AGENT".to_string(), "Planner-test".to_string()),
            ("QUORUM_RUN_ID".to_string(), "run-capability".to_string()),
            (
                "QUORUM_AGENT_ENDPOINT".to_string(),
                "/tmp/quorum-planner.sock".to_string(),
            ),
            ("QUORUM_HOME".to_string(), "home-authority".to_string()),
            ("GH_TOKEN".to_string(), "gh-authority".to_string()),
        ];

        for (agent_mcp, expect_envelope) in [
            (Some(crate::serve::runner::AGENT_MCP_SERVER), true),
            (None, false),
        ] {
            let _ = std::fs::remove_file(&capture);
            let mut spec = planner_spec(tmp.path());
            spec.env_vars = envelope.clone();
            let proc = AgentProc::spawn_planner(&spec, fake.to_str(), agent_mcp).unwrap();
            for _ in 0..200 {
                if capture.exists() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            let observed: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&capture).expect("planner did not report its environment"),
            )
            .unwrap();
            let _ = proc.kill_and_reap().await;

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
            // Never reachable in either shape.
            assert_eq!(observed["home"], "");
            assert_eq!(observed["gh"], "");
        }
    }

    /// Negative control pinning the #297 failure mode: a non-UUID session id
    /// must make the CLI exit with NO stream events. If this ever starts
    /// emitting events, the CLI relaxed its validation and the positive
    /// test's discriminator needs a rethink.
    #[tokio::test]
    async fn real_cli_rejects_non_uuid_session_id() {
        if !claude_available() {
            eprintln!("skipped: no claude binary on PATH");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let mut spec = classifier_spec(tmp.path(), true);
        spec.session_id = "classifier-1".into();
        spec.env_vars = no_auth_env(tmp.path());

        let mut proc = AgentProc::spawn(&spec, None).expect("spawn claude");
        let _ = proc.feed_turn(&user_turn("ping")).await; // may fail: process already dead
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("claude neither exited nor emitted within 60s");
        let _terminal_output = proc.kill_and_reap().await;
        assert!(
            event.is_none(),
            "claude accepted a non-UUID --session-id — CLI validation changed"
        );
    }

    /// #206: reviewers are instructed to invoke the pinned `pr-review` skill;
    /// without `Skill` in the allowlist the invocation is auto-denied under
    /// dontAsk and the review silently degrades to an unstructured read.
    #[test]
    fn allowed_tools_include_skill() {
        assert!(ALLOWED_TOOLS.split(',').any(|t| t == "Skill"));
    }

    /// #220: allowed_tools flows through AgentSpec — a custom list must reach
    /// the spawn site unchanged (not silently replaced by the default constant).
    #[test]
    fn agent_spec_carries_allowed_tools() {
        let spec = AgentSpec {
            kind: AgentKind::Claude,
            model: "opus".into(),
            effort: "high".into(),
            session_id: "sid".into(),
            worktree: PathBuf::from("/tmp"),
            bare: false,
            allowed_tools: "Bash,Read".to_string(),
            env_vars: vec![],
        };
        assert_eq!(spec.allowed_tools, "Bash,Read");
    }

    /// Default ALLOWED_TOOLS constant contains the baseline tool set.
    #[test]
    fn default_allowed_tools_contains_baseline() {
        let tools: Vec<&str> = ALLOWED_TOOLS.split(',').collect();
        for expected in ["Bash", "Read", "Edit", "Write", "Glob", "Grep"] {
            assert!(tools.contains(&expected), "missing {expected}");
        }
    }

    #[test]
    fn user_turn_has_type_role_and_content() {
        let turn = user_turn("hello world");
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        assert_eq!(parsed["type"], "user");
        assert_eq!(
            parsed["message"]["role"], "user",
            "claude CLI exits 1 on turns without message.role"
        );
        assert_eq!(parsed["message"]["content"], "hello world");
    }

    #[test]
    fn real_claude_quota_and_auth_fixtures_are_provider_wide() {
        // Captured from Claude Code 2.1.229 at the real stream-json boundary.
        let quota = r#"{"type":"assistant","message":{"model":"<synthetic>","content":[{"type":"text","text":"You've hit your session limit"}]},"error":"rate_limit","is_api_error_message":true}"#;
        let auth = r#"{"type":"assistant","message":{"model":"<synthetic>","content":[{"type":"text","text":"Not logged in · Please run /login"}]},"error":"authentication_failed","is_api_error_message":true}"#;
        for fixture in [quota, auth] {
            assert_eq!(
                AgentProc::failure_observation(fixture).disposition,
                Some(FailureDisposition::ProviderUnavailable)
            );
        }
    }

    #[test]
    fn claude_http_429_requires_account_or_session_quota_evidence() {
        let quota = r#"{"type":"result","result":"You've hit your session limit","is_error":true,"api_error_status":429}"#;
        assert_eq!(
            AgentProc::failure_observation(quota).disposition,
            Some(FailureDisposition::ProviderUnavailable)
        );

        for transient in [
            r#"{"type":"result","result":"upstream rate limit exceeded","is_error":true,"api_error_status":429}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"upstream rate limit exceeded"}]},"error":"rate_limit","is_api_error_message":true}"#,
        ] {
            let observation = AgentProc::failure_observation(transient);
            assert_eq!(observation.disposition, None);
            assert!(observation.unknown_failure);
        }
    }

    #[test]
    fn claude_model_fixture_is_profile_scoped_and_unknown_is_fail_safe() {
        let model = r#"{"type":"assistant","message":{"model":"<synthetic>"},"error":"model_not_found","is_api_error_message":true}"#;
        assert_eq!(
            AgentProc::failure_observation(model).disposition,
            Some(FailureDisposition::ProfileUnavailable)
        );

        let unknown = r#"{"type":"assistant","message":{"content":"future failure"},"error":"future_provider_code","is_api_error_message":true}"#;
        let observation = AgentProc::failure_observation(unknown);
        assert!(observation.disposition.is_none());
        assert!(observation.unknown_failure);
    }

    #[test]
    fn claude_semantic_text_never_enters_provider_taxonomy() {
        for text in [
            "cargo test failed; I will fix the compilation error",
            "I reported failed through quorum react",
            "I reported blocked through quorum react",
            "I reported needs-info through quorum react",
            "BLOCKING review finding: the state transition is unsafe",
        ] {
            let line = serde_json::json!({
                "type": "assistant",
                "message": {"content": text}
            })
            .to_string();
            let observation = AgentProc::failure_observation(&line);
            assert_eq!(observation, FailureObservation::inert(), "{text}");
        }
    }

    #[tokio::test]
    async fn claude_quota_is_available_before_persistent_child_exit() {
        let mut proc = shell_proc(
            "printf '%s\\n' \
             '{\"type\":\"assistant\",\"message\":{\"model\":\"<synthetic>\"},\"error\":\"rate_limit\",\"is_api_error_message\":true}' \
             '{\"type\":\"result\",\"result\":\"session limit\",\"is_error\":true,\"api_error_status\":429}'; \
             exec sleep 30",
        )
        .await;
        let _ = proc.next_raw_line().await;
        let _ = proc.next_raw_line().await;
        assert_eq!(
            proc.observed_pre_authoritative_failure()
                .unwrap()
                .disposition(),
            FailureDisposition::ProviderUnavailable
        );
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn persistent_claude_tracker_is_scoped_to_each_managed_turn() {
        let mut proc = shell_proc(
            "IFS= read -r first; \
             printf '%s\\n' '{\"type\":\"result\",\"result\":\"submitted\",\"is_error\":false}'; \
             IFS= read -r second; \
             printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"Not logged in · Please run /login\"}]},\"error\":\"authentication_failed\",\"is_api_error_message\":true}'; \
             exec sleep 30",
        )
        .await;

        proc.feed_turn(&user_turn("initial work")).await.unwrap();
        let _ = proc.next_raw_line().await.unwrap();
        assert!(proc.observed_pre_authoritative_failure().is_none());

        proc.feed_turn(&user_turn("review requested changes"))
            .await
            .unwrap();
        let _ = proc.next_raw_line().await.unwrap();
        assert_eq!(
            proc.observed_pre_authoritative_failure()
                .unwrap()
                .disposition(),
            FailureDisposition::ProviderUnavailable
        );
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn claude_boundary_bounds_stderr_drains_terminal_output_and_reaps() {
        // 17618 bytes must stay above runner::DIAGNOSTIC_LINE_BYTES (16384) and 306
        // lines above runner::DIAGNOSTIC_CAPACITY (256) — both constants are private
        // to runner, so mirror the margins its own unit tests use (+1234 / +50).
        // `exec sleep` (not `sleep`) so the shell never forks a grandchild: killpg
        // races a concurrent fork(), and a child born after the kernel's group scan
        // starts with an empty pending-signal set, survives as an orphan, and holds
        // the stderr pipe open until it exits on its own — a 30s hang, not a deadlock.
        let mut proc = shell_proc(
            "printf '\\377invalid\\n' >&2; \
             head -c 17618 /dev/zero | tr '\\000' x >&2; echo >&2; \
             i=0; while [ $i -lt 306 ]; do echo claude-stderr-$i >&2; i=$((i+1)); done; \
             printf '%s\\n%s\\n' '{\"type\":\"provider.ready\"}' \
               '{\"type\":\"result\",\"result\":\"trailing\"}'; \
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
        assert!(output
            .iter()
            .any(|line| matches!(line, CapturedOutput::Stdout(text) if text.contains("trailing"))));
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::Stderr(text) if text == "claude-stderr-305")
        ));
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
    }
}
