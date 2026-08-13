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
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

const DIAGNOSTIC_CAPACITY: usize = 256;
const DIAGNOSTIC_LINE_BYTES: usize = 16 * 1024;

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
pub struct DiagnosticBuffer(Arc<Mutex<DiagnosticState>>);

#[derive(Default)]
struct DiagnosticState {
    lines: VecDeque<String>,
    dropped: usize,
    dropped_bytes: usize,
}

impl DiagnosticBuffer {
    pub fn push(&self, line: String) {
        let mut state = self.0.lock().expect("diagnostic buffer poisoned");
        if state.lines.len() == DIAGNOSTIC_CAPACITY {
            state.lines.pop_front();
            state.dropped += 1;
        }
        state.lines.push_back(line);
    }

    pub fn drain(&self) -> Vec<CapturedOutput> {
        let mut state = self.0.lock().expect("diagnostic buffer poisoned");
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
        let mut state = self.0.lock().expect("diagnostic buffer poisoned");
        state.dropped_bytes = state.dropped_bytes.saturating_add(count);
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
            Ok(0) | Err(_) => break,
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
    /// Resolve and launch the explicit built-in adapter for this model.
    pub async fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        let kind = AgentKind::for_model(request.model)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        match kind {
            AgentKind::Claude => AgentProc::launch(request, config).await.map(Self::Claude),
            AgentKind::Codex => CodexProc::launch(request, config).map(Self::Codex),
            AgentKind::Grok => GrokProc::launch(request, config).map(Self::Grok),
        }
    }

    /// Restricted classifier launch whose deadline covers Claude's initial
    /// stdin feed as well as the returned slot lifetime. Single-turn adapters
    /// receive their prompt in argv and therefore have no pre-slot pipe wait.
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
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "restricted provider launch timed out",
            ));
        }
        let kind = AgentKind::for_model(request.model)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        match kind {
            AgentKind::Claude => AgentProc::launch_restricted_until(request, config, deadline)
                .await
                .map(Self::Claude),
            AgentKind::Codex => CodexProc::launch(request, config).map(Self::Codex),
            AgentKind::Grok => GrokProc::launch(request, config).map(Self::Grok),
        }
    }

    pub fn kind(&self) -> AgentKind {
        match self {
            Self::Claude(_) => AgentKind::Claude,
            Self::Codex(_) => AgentKind::Codex,
            Self::Grok(_) => AgentKind::Grok,
        }
    }

    pub async fn kill_and_reap(self) -> Vec<CapturedOutput> {
        match self {
            Self::Claude(proc) => proc.kill_and_reap().await,
            Self::Codex(proc) => proc.kill_and_reap().await,
            Self::Grok(proc) => proc.kill_and_reap().await,
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

    pub async fn next_raw_line(&mut self) -> Option<String> {
        match self {
            Self::Claude(proc) => proc.next_raw_line().await,
            Self::Codex(proc) => proc.next_raw_line().await,
            Self::Grok(proc) => proc.next_raw_line().await,
        }
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
        if model == super::grok_agent::SUPPORTED_MODEL {
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
                 (expected claude-*/Claude alias, supported OpenAI model, or grok-4.5)"
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
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

    #[cfg(unix)]
    fn recording_runner(dir: &std::path::Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let runner = dir.join("recording-runner");
        let args = dir.join("args.log");
        let turn = dir.join("turn.log");
        let environment = dir.join("environment.log");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\n\
                 for arg in \"$@\"; do printf '<%s>\\n' \"$arg\"; done > '{}'\n\
                 printf '%s\\n' \"$QUORUM_ADAPTER_TEST\" > '{}'\n\
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
                turn.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        runner
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_launch_dispatches_to_each_explicit_adapter() {
        let claude_dir = tempfile::tempdir().unwrap();
        let claude_bin = recording_runner(claude_dir.path());
        let claude_environment = vec![("QUORUM_ADAPTER_TEST".into(), "claude-env".into())];
        let mut claude = RunnerProc::launch(
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
        .await
        .unwrap();
        assert_eq!(claude.kind(), AgentKind::Claude);
        let _ = claude.next_raw_line().await;
        claude.kill_and_reap().await;
        let args = std::fs::read_to_string(claude_dir.path().join("args.log")).unwrap();
        assert!(args.contains("<--add-dir>"), "{args}");
        assert!(args.contains("<--bare>"), "{args}");
        assert!(args.contains("<Bash,Read>"), "{args}");
        assert!(!args.contains("<--safe-mode>"), "{args}");
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
        let mut codex = RunnerProc::launch(
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
        .await
        .unwrap();
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

        let grok_dir = tempfile::tempdir().unwrap();
        let grok_bin = recording_runner(grok_dir.path());
        let grok_environment = vec![("QUORUM_ADAPTER_TEST".into(), "grok-env".into())];
        let mut grok = RunnerProc::launch(
            &LaunchRequest {
                model: "grok-4.5",
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
        .await
        .unwrap();
        assert_eq!(grok.kind(), AgentKind::Grok);
        while grok.next_raw_line().await.is_some() {}
        grok.kill_and_reap().await;
        let args = std::fs::read_to_string(grok_dir.path().join("args.log")).unwrap();
        assert!(args.contains("<streaming-json>"), "{args}");
        assert!(args.contains("<grok-4.5>"), "{args}");
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

    #[cfg(unix)]
    #[tokio::test]
    async fn restricted_launch_dispatches_to_each_safe_adapter() {
        for (model, expected_kind) in [
            ("claude-haiku-4-5-20251001", AgentKind::Claude),
            ("gpt-5.6-terra", AgentKind::Codex),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let executable = recording_runner(dir.path());
            let mut proc = RunnerProc::launch(
                &LaunchRequest {
                    model,
                    effort: "low",
                    worktree: dir.path(),
                    prompt: "restricted prompt",
                    environment: &[],
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
            .await
            .unwrap();
            assert_eq!(proc.kind(), expected_kind);
            while proc.next_raw_line().await.is_some() {}
            proc.kill_and_reap().await;
            let args = std::fs::read_to_string(dir.path().join("args.log")).unwrap();
            match expected_kind {
                AgentKind::Claude => {
                    assert!(args.contains("<--safe-mode>"), "{args}");
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
                AgentKind::Grok => unreachable!("fixture contains only Claude and Codex"),
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
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuation_identity_dispatches_to_codex_resume_only_in_normal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let executable = recording_runner(dir.path());
        let mut proc = RunnerProc::launch(
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
        .await
        .unwrap();
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
        assert!(AgentKind::for_model("grok").is_err());
        assert!(AgentKind::for_model("grok-4.6").is_err());
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
