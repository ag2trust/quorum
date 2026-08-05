//! Official Grok Build CLI transport: command construction, bounded process IO,
//! and normalization of the native headless `streaming-json` protocol.
//!
//! This adapter intentionally uses only `grok -p`/`--resume`. It does not use
//! ACP server internals, infer the runner from an executable name, or emulate
//! Claude/Codex flags.

use super::runner::{
    capture_diagnostics, tool_summary, ActivityKind, AdapterConfig, AgentEvent, CapturedOutput,
    DiagnosticBuffer, LaunchMode, LaunchRequest, NormalizedLine, TokenUsage,
};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdout, Command};

pub const SUPPORTED_MODEL: &str = "grok-4.5";
pub const SUPPORTED_EFFORTS: &[&str] = &["low", "medium", "high"];
pub const DEFAULT_SANDBOX: &str = "workspace";
pub const DEFAULT_PERMISSION_MODE: &str = "bypassPermissions";
pub const DEFAULT_MAX_TURNS: u32 = 64;
pub const MAX_CONFIGURED_TURNS: u32 = 256;

const RESTRICTED_SANDBOX: &str = "read-only";
const RESTRICTED_PERMISSION_MODE: &str = "dontAsk";
const RESTRICTED_MAX_TURNS: u32 = 8;
const STDOUT_LINE_BYTES: usize = 1024 * 1024;
const TERMINAL_STDOUT_LINES: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct GrokAdapterConfig<'a> {
    pub sandbox: &'a str,
    pub permission_mode: &'a str,
    pub max_turns: u32,
}

impl Default for GrokAdapterConfig<'static> {
    fn default() -> Self {
        Self {
            sandbox: DEFAULT_SANDBOX,
            permission_mode: DEFAULT_PERMISSION_MODE,
            max_turns: DEFAULT_MAX_TURNS,
        }
    }
}

impl GrokAdapterConfig<'_> {
    pub fn validate(self) -> Result<(), String> {
        if !matches!(self.sandbox, "off" | "workspace") {
            return Err(format!(
                "unsupported Grok sandbox '{}': the transport adapter accepts only 'off' or \
                 'workspace' for coding turns",
                self.sandbox
            ));
        }
        if self.permission_mode != DEFAULT_PERMISSION_MODE {
            return Err(format!(
                "unsupported Grok permission_mode '{}': unattended coding turns require \
                 'bypassPermissions'",
                self.permission_mode
            ));
        }
        if !(1..=MAX_CONFIGURED_TURNS).contains(&self.max_turns) {
            return Err(format!(
                "unsupported Grok max_turns {}: expected 1..={MAX_CONFIGURED_TURNS}",
                self.max_turns
            ));
        }
        Ok(())
    }
}

pub struct GrokSpec {
    pub model: String,
    pub effort: String,
    pub worktree: PathBuf,
    pub prompt: String,
    pub env_vars: Vec<(String, String)>,
    pub sandbox: String,
    pub permission_mode: String,
    pub max_turns: u32,
}

impl GrokSpec {
    fn validate(&self) -> std::io::Result<()> {
        if self.model != SUPPORTED_MODEL {
            return Err(invalid_input(format!(
                "unsupported Grok model '{}': expected '{SUPPORTED_MODEL}'",
                self.model
            )));
        }
        if !SUPPORTED_EFFORTS.contains(&self.effort.as_str()) {
            return Err(invalid_input(format!(
                "unsupported Grok effort '{}': expected one of {}",
                self.effort,
                SUPPORTED_EFFORTS.join(", ")
            )));
        }
        GrokAdapterConfig {
            sandbox: &self.sandbox,
            permission_mode: &self.permission_mode,
            max_turns: self.max_turns,
        }
        .validate()
        .map_err(invalid_input)
    }
}

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

fn common_args(
    spec: &GrokSpec,
    sandbox: &str,
    permission_mode: &str,
    max_turns: u32,
) -> Vec<String> {
    vec![
        "-p".into(),
        spec.prompt.clone(),
        "--output-format".into(),
        "streaming-json".into(),
        "--model".into(),
        spec.model.clone(),
        "--reasoning-effort".into(),
        spec.effort.clone(),
        "--permission-mode".into(),
        permission_mode.into(),
        "--sandbox".into(),
        sandbox.into(),
        "--max-turns".into(),
        max_turns.to_string(),
        "--verbatim".into(),
    ]
}

/// Pinned first-turn argument shape for the official headless CLI.
pub fn headless_args(spec: &GrokSpec, mode: LaunchMode) -> std::io::Result<Vec<String>> {
    spec.validate()?;
    Ok(match mode {
        LaunchMode::Normal => {
            common_args(spec, &spec.sandbox, &spec.permission_mode, spec.max_turns)
        }
        LaunchMode::Restricted => common_args(
            spec,
            RESTRICTED_SANDBOX,
            RESTRICTED_PERMISSION_MODE,
            spec.max_turns.min(RESTRICTED_MAX_TURNS),
        ),
    })
}

/// Pinned exact-session continuation shape. Grok persists the original
/// sandbox with the session and refuses a mismatched profile itself; Quorum
/// supplies the same validated configuration on every turn.
pub fn resume_args(session_id: &str, spec: &GrokSpec) -> std::io::Result<Vec<String>> {
    spec.validate()?;
    if session_id.trim().is_empty() {
        return Err(invalid_input("Grok continuation session ID is empty"));
    }
    let mut args = vec!["--resume".into(), session_id.into()];
    args.extend(common_args(
        spec,
        &spec.sandbox,
        &spec.permission_mode,
        spec.max_turns,
    ));
    Ok(args)
}

struct BoundedStdout {
    reader: ChildStdout,
    chunk: Box<[u8; 8192]>,
    position: usize,
    filled: usize,
    line: Vec<u8>,
    dropped: usize,
    eof: bool,
}

impl BoundedStdout {
    fn new(reader: ChildStdout) -> Self {
        Self {
            reader,
            chunk: Box::new([0; 8192]),
            position: 0,
            filled: 0,
            line: Vec::new(),
            dropped: 0,
            eof: false,
        }
    }

    async fn next_line(&mut self) -> Option<String> {
        loop {
            while self.position < self.filled {
                let byte = self.chunk[self.position];
                self.position += 1;
                if byte == b'\n' {
                    return Some(self.finish_line());
                }
                if self.line.len() < STDOUT_LINE_BYTES {
                    self.line.push(byte);
                } else {
                    self.dropped = self.dropped.saturating_add(1);
                }
            }

            if self.eof {
                if self.line.is_empty() && self.dropped == 0 {
                    return None;
                }
                return Some(self.finish_line());
            }

            match self.reader.read(&mut self.chunk[..]).await {
                Ok(0) | Err(_) => self.eof = true,
                Ok(read) => {
                    self.position = 0;
                    self.filled = read;
                }
            }
        }
    }

    fn finish_line(&mut self) -> String {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.dropped > 0 {
            let dropped = self.dropped;
            self.line.clear();
            self.dropped = 0;
            return serde_json::json!({
                "type": "provider.stdout_bytes_truncated",
                "dropped_bytes": dropped,
                "line_limit_bytes": STDOUT_LINE_BYTES,
            })
            .to_string();
        }
        let line = match std::str::from_utf8(&self.line) {
            Ok(line) => line.to_string(),
            Err(error) => serde_json::json!({
                "type": "provider.stdout_invalid_utf8",
                "line_bytes": self.line.len(),
                "valid_up_to": error.valid_up_to(),
                "invalid_sequence_bytes": error.error_len(),
            })
            .to_string(),
        };
        self.line.clear();
        line
    }
}

pub struct GrokProc {
    child: Child,
    process_group_id: libc::pid_t,
    reader: BoundedStdout,
    diagnostics: DiagnosticBuffer,
    stderr_task: tokio::task::JoinHandle<()>,
}

impl GrokProc {
    pub fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        if request.mode == LaunchMode::Restricted && request.continuation_id.is_some() {
            return Err(invalid_input(
                "restricted Grok launch cannot resume a prior session",
            ));
        }
        let spec = GrokSpec {
            model: request.model.to_string(),
            effort: request.effort.to_string(),
            worktree: request.worktree.to_path_buf(),
            prompt: request.prompt.to_string(),
            env_vars: request.environment.to_vec(),
            sandbox: config.grok.sandbox.to_string(),
            permission_mode: config.grok.permission_mode.to_string(),
            max_turns: config.grok.max_turns,
        };
        let args = match request.continuation_id {
            Some(session_id) => resume_args(session_id, &spec)?,
            None => headless_args(&spec, request.mode)?,
        };
        Self::spawn_command(&spec, &args, config.executable)
    }

    #[cfg(test)]
    fn spawn(spec: &GrokSpec, grok_bin: Option<&str>) -> std::io::Result<Self> {
        let args = headless_args(spec, LaunchMode::Normal)?;
        Self::spawn_command(spec, &args, grok_bin)
    }

    fn spawn_command(
        spec: &GrokSpec,
        args: &[String],
        grok_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let mut command = Command::new(grok_bin.unwrap_or("grok"));
        command.args(args);
        for (key, value) in &spec.env_vars {
            command.env(key, value);
        }
        command
            .current_dir(&spec.worktree)
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

        let mut child = command.spawn()?;
        // `Child::id()` becomes `None` after `try_wait()` observes leader
        // exit, but descendants may still hold the process group's pipes.
        // Persist the group ID before any caller can reap the leader.
        let process_group_id = child
            .id()
            .ok_or_else(|| std::io::Error::other("spawned Grok process has no process-group ID"))?
            as libc::pid_t;
        let reader = BoundedStdout::new(child.stdout.take().expect("stdout was piped"));
        let diagnostics = DiagnosticBuffer::default();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = child.stderr.take().expect("stderr was piped");
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        Ok(Self {
            child,
            process_group_id,
            reader,
            diagnostics,
            stderr_task,
        })
    }

    pub fn normalize_line(raw: &str) -> NormalizedLine {
        NormalizedLine {
            events: normalize_grok_line(raw),
            terminal_text: None,
        }
    }

    pub async fn next_raw_line(&mut self) -> Option<String> {
        self.reader.next_line().await
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
        // Always target the spawn-time process group, even when `try_wait`
        // already observed and reaped the leader. Descendants can otherwise
        // retain stdout/stderr and make the drains below wait forever.
        unsafe {
            libc::killpg(self.process_group_id, libc::SIGKILL);
        }
        let _ = self.child.wait().await;

        let mut terminal = VecDeque::new();
        let mut dropped = 0usize;
        while let Some(line) = self.reader.next_line().await {
            if terminal.len() == TERMINAL_STDOUT_LINES {
                terminal.pop_front();
                dropped = dropped.saturating_add(1);
            }
            terminal.push_back(CapturedOutput::Stdout(line));
        }
        let diagnostics = self.diagnostics.clone();
        let _ = self.stderr_task.await;
        let mut output = Vec::with_capacity(terminal.len() + usize::from(dropped > 0));
        if dropped > 0 {
            output.push(CapturedOutput::StdoutTruncated { dropped });
        }
        output.extend(terminal);
        output.extend(diagnostics.drain());
        output
    }
}

pub fn normalize_grok_line(raw: &str) -> Vec<AgentEvent> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Vec::new();
    };
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("text") => value
            .get("data")
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .map(|text| vec![AgentEvent::AssistantText { text: text.into() }])
            .unwrap_or_default(),
        Some("tool_call") => {
            let name = value
                .get("toolName")
                .or_else(|| value.get("title"))
                .or_else(|| value.get("kind"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty())
                .unwrap_or("tool");
            let input = value.get("rawInput").unwrap_or(&serde_json::Value::Null);
            vec![AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary: tool_summary(name, input),
            }]
        }
        Some("end") => normalize_end(&value),
        Some("error") => vec![AgentEvent::TurnFailed {
            message: value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .filter(|message| !message.is_empty())
                .unwrap_or("Grok turn failed")
                .to_string(),
            usage: terminal_usage(&value),
            cost_usd: terminal_cost(&value),
        }],
        _ => Vec::new(),
    }
}

fn normalize_end(value: &serde_json::Value) -> Vec<AgentEvent> {
    let Some(session_id) = value
        .get("sessionId")
        .and_then(serde_json::Value::as_str)
        .filter(|session_id| !session_id.trim().is_empty())
    else {
        return vec![AgentEvent::TurnFailed {
            message: "Grok end event missing sessionId".into(),
            usage: terminal_usage(value),
            cost_usd: terminal_cost(value),
        }];
    };
    vec![
        AgentEvent::ThreadStarted {
            thread_id: session_id.to_string(),
        },
        AgentEvent::TurnCompleted {
            usage: terminal_usage(value),
            cost_usd: terminal_cost(value),
        },
    ]
}

fn terminal_usage(value: &serde_json::Value) -> Option<TokenUsage> {
    if value
        .get("usage_is_incomplete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let usage = value.get("usage")?;
    let input = usage.get("input_tokens")?.as_u64()?;
    let cache_read = usage
        .get("cache_read_input_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let cache_write = usage
        .get("cache_creation_input_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    Some(TokenUsage {
        input_tokens: input.saturating_add(cache_read).saturating_add(cache_write),
        output_tokens: usage.get("output_tokens")?.as_u64()?,
    })
}

fn terminal_cost(value: &serde_json::Value) -> Option<f64> {
    if value
        .get("cost_is_partial")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || value
            .get("usage_is_incomplete")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        return None;
    }
    value
        .get("total_cost_usd")
        .and_then(serde_json::Value::as_f64)
        .filter(|cost| cost.is_finite() && *cost >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    // The installed CLI's unauthenticated protocol startup and invalid-key
    // startup contend when they run concurrently, occasionally delaying one
    // past the bounded assertion window. They exercise one real binary
    // serially while the pure argument and fixture tests remain parallel.
    static REAL_CLI_PROTOCOL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_spec(worktree: &std::path::Path) -> GrokSpec {
        GrokSpec {
            model: SUPPORTED_MODEL.into(),
            effort: "high".into(),
            worktree: worktree.to_path_buf(),
            prompt: "inspect the repository".into(),
            env_vars: Vec::new(),
            sandbox: DEFAULT_SANDBOX.into(),
            permission_mode: DEFAULT_PERMISSION_MODE.into(),
            max_turns: DEFAULT_MAX_TURNS,
        }
    }

    #[test]
    fn initial_argument_shape_is_pinned() {
        let spec = test_spec(std::path::Path::new("/tmp/repo"));
        assert_eq!(
            headless_args(&spec, LaunchMode::Normal).unwrap(),
            [
                "-p",
                "inspect the repository",
                "--output-format",
                "streaming-json",
                "--model",
                "grok-4.5",
                "--reasoning-effort",
                "high",
                "--permission-mode",
                "bypassPermissions",
                "--sandbox",
                "workspace",
                "--max-turns",
                "64",
                "--verbatim",
            ]
        );
    }

    #[test]
    fn continuation_argument_shape_is_pinned() {
        let spec = test_spec(std::path::Path::new("/tmp/repo"));
        let args = resume_args("019f-session", &spec).unwrap();
        assert_eq!(
            &args[..4],
            ["--resume", "019f-session", "-p", "inspect the repository"]
        );
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "workspace"]));
        assert!(!args.iter().any(|arg| arg.contains("allowedTools")));
        assert!(!args.iter().any(|arg| arg == "--bare"));
    }

    #[test]
    fn restricted_shape_uses_native_fail_closed_controls() {
        let mut spec = test_spec(std::path::Path::new("/tmp/repo"));
        spec.max_turns = 200;
        let args = headless_args(&spec, LaunchMode::Restricted).unwrap();
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "read-only"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--permission-mode", "dontAsk"]));
        assert!(args.windows(2).any(|pair| pair == ["--max-turns", "8"]));
    }

    #[test]
    fn models_efforts_and_unsupported_safety_combinations_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut spec = test_spec(dir.path());
        spec.model = "grok-future".into();
        assert_eq!(
            headless_args(&spec, LaunchMode::Normal).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        spec.model = SUPPORTED_MODEL.into();
        spec.effort = "xhigh".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.effort = "high".into();
        spec.sandbox = "custom".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.sandbox = DEFAULT_SANDBOX.into();
        spec.permission_mode = "auto".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.permission_mode = DEFAULT_PERMISSION_MODE.into();
        spec.max_turns = 0;
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
    }

    #[test]
    fn fixture_session_identity_and_terminal_success() {
        let raw = r#"{"type":"end","stopReason":"EndTurn","sessionId":"sess-1","requestId":"req-1","usage":{"input_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":3,"output_tokens":4},"total_cost_usd":0.0125}"#;
        let events = normalize_grok_line(raw);
        assert_eq!(
            events[0],
            AgentEvent::ThreadStarted {
                thread_id: "sess-1".into()
            }
        );
        assert_eq!(
            events[1],
            AgentEvent::TurnCompleted {
                usage: Some(TokenUsage {
                    input_tokens: 33,
                    output_tokens: 4
                }),
                cost_usd: Some(0.0125),
            }
        );
    }

    #[test]
    fn fixture_assistant_text_and_activity() {
        assert_eq!(
            normalize_grok_line(r#"{"type":"text","data":"working"}"#),
            vec![AgentEvent::AssistantText {
                text: "working".into()
            }]
        );
        let events = normalize_grok_line(
            r#"{"type":"tool_call","toolCallId":"call-1","title":"Run","kind":"execute","toolName":"run_terminal_cmd","rawInput":{"command":"cargo test"}}"#,
        );
        assert!(matches!(
            &events[0],
            AgentEvent::Activity { kind: ActivityKind::ToolUse, summary } if summary == "run_terminal_cmd"
        ));
    }

    #[test]
    fn fixture_terminal_failure_and_incomplete_spend() {
        let raw = r#"{"type":"error","message":"authentication failed","usage":{"input_tokens":8,"output_tokens":2},"total_cost_usd":0.1,"usage_is_incomplete":true,"cost_is_partial":true}"#;
        assert_eq!(
            normalize_grok_line(raw),
            vec![AgentEvent::TurnFailed {
                message: "authentication failed".into(),
                usage: None,
                cost_usd: None,
            }]
        );
    }

    #[test]
    fn fixture_partial_cost_does_not_hide_complete_tokens_or_invent_usd() {
        let raw = r#"{"type":"end","sessionId":"sess-2","usage":{"input_tokens":8,"cache_read_input_tokens":4,"output_tokens":2},"total_cost_usd":0.1,"cost_is_partial":true}"#;
        assert_eq!(
            normalize_grok_line(raw)[1],
            AgentEvent::TurnCompleted {
                usage: Some(TokenUsage {
                    input_tokens: 12,
                    output_tokens: 2,
                }),
                cost_usd: None,
            }
        );
    }

    #[test]
    fn fixture_end_without_session_fails_closed() {
        for raw in [
            r#"{"type":"end","stopReason":"end_turn"}"#,
            r#"{"type":"end","sessionId":"   "}"#,
        ] {
            assert!(matches!(
                normalize_grok_line(raw).as_slice(),
                [AgentEvent::TurnFailed { message, .. }] if message.contains("missing sessionId")
            ));
        }
    }

    #[test]
    fn fixture_unknown_and_malformed_lines_are_inert() {
        for raw in [
            r#"{"type":"thought","data":"private"}"#,
            r#"{"type":"tool_call_update","toolCallId":"1","status":"completed"}"#,
            r#"{"type":"future_event","extra":true}"#,
            "not-json",
            "{broken",
            "",
        ] {
            assert!(normalize_grok_line(raw).is_empty(), "raw={raw}");
        }
    }

    #[tokio::test]
    async fn raw_lines_are_returned_exactly_without_the_line_terminator() {
        let expected = r#"  {"type":"future_event","extra":true}  "#;
        let mut proc = shell_proc(&format!("printf '%s\\n' '{}'", expected)).await;
        assert_eq!(proc.next_raw_line().await.as_deref(), Some(expected));
        assert!(proc.next_raw_line().await.is_none());
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn invalid_utf8_cannot_be_repaired_into_authoritative_terminal_json() {
        let mut proc = shell_proc("printf '{\"type\":\"end\",\"sessionId\":\"\\377\"}\\n'").await;
        let raw = proc.next_raw_line().await.unwrap();
        let marker: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker["type"], "provider.stdout_invalid_utf8");
        assert_eq!(marker["invalid_sequence_bytes"], 1);
        assert!(!raw.contains('\u{fffd}'));
        assert!(normalize_grok_line(&raw).is_empty());
        assert!(proc.next_raw_line().await.is_none());
        proc.kill_and_reap().await;
    }

    async fn shell_proc(script: &str) -> GrokProc {
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
        let process_group_id = child.id().unwrap() as libc::pid_t;
        let reader = BoundedStdout::new(child.stdout.take().unwrap());
        let diagnostics = DiagnosticBuffer::default();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = child.stderr.take().unwrap();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        GrokProc {
            child,
            process_group_id,
            reader,
            diagnostics,
            stderr_task,
        }
    }

    async fn wait_status(proc: &mut GrokProc) -> std::process::ExitStatus {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(status) = proc.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("process did not exit")
    }

    #[tokio::test]
    async fn eof_without_terminal_event_never_fabricates_success() {
        let mut proc =
            shell_proc("printf '%s\\n' '{\"type\":\"text\",\"data\":\"partial\"}'").await;
        let raw = proc.next_raw_line().await.unwrap();
        assert!(matches!(
            normalize_grok_line(&raw).as_slice(),
            [AgentEvent::AssistantText { .. }]
        ));
        assert!(proc.next_raw_line().await.is_none());
        assert!(wait_status(&mut proc).await.success());
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn nonzero_exit_without_terminal_event_never_fabricates_failure_line() {
        let mut proc = shell_proc("exit 7").await;
        assert!(proc.next_raw_line().await.is_none());
        let status = wait_status(&mut proc).await;
        assert_eq!(status.code(), Some(7));
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn stdout_and_stderr_capture_are_bounded_and_process_group_is_reaped() {
        let mut proc = shell_proc(
            "head -c 1050000 /dev/zero | tr '\\000' x; echo; \
             i=0; while [ $i -lt 306 ]; do echo grok-stderr-$i >&2; i=$((i+1)); done; \
             printf '%s\\n' '{\"type\":\"text\",\"data\":\"tail\"}'; exec sleep 30",
        )
        .await;
        let pid = proc.pid().unwrap();
        let marker = tokio::time::timeout(std::time::Duration::from_secs(30), proc.next_raw_line())
            .await
            .unwrap()
            .unwrap();
        assert!(marker.contains("provider.stdout_bytes_truncated"));
        let tail = proc.next_raw_line().await.unwrap();
        assert!(tail.contains("tail"));
        let output = proc.kill_and_reap().await;
        assert!(output.len() <= 258);
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::StderrTruncated { dropped } if *dropped > 0)
        ));
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
    }

    #[tokio::test]
    async fn teardown_kills_stored_process_group_after_leader_is_reaped() {
        let mut proc = shell_proc("trap '' HUP; (trap '' HUP; sleep 30) & exit 0").await;
        let process_group_id = proc.process_group_id;

        let status = wait_status(&mut proc).await;
        assert!(status.success());
        assert!(
            proc.child.id().is_none(),
            "test must exercise teardown after try_wait loses the leader ID"
        );
        assert_eq!(
            unsafe { libc::killpg(process_group_id, 0) },
            0,
            "a descendant must still hold the process group and inherited pipes"
        );

        let output = tokio::time::timeout(std::time::Duration::from_secs(5), proc.kill_and_reap())
            .await
            .expect("post-exit teardown hung on descendant-held pipes");
        assert!(output.is_empty());

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while unsafe { libc::killpg(process_group_id, 0) } == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Grok descendant process group was not reaped");
    }

    fn grok_available() -> bool {
        std::process::Command::new("grok")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn no_auth_env(root: &std::path::Path) -> Vec<(String, String)> {
        let home = root.join("home");
        let grok_home = root.join("grok-home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&grok_home).unwrap();
        vec![
            ("HOME".into(), home.display().to_string()),
            ("GROK_HOME".into(), grok_home.display().to_string()),
            ("XAI_API_KEY".into(), String::new()),
            ("RUST_LOG".into(), "off".into()),
        ]
    }

    #[tokio::test]
    async fn real_cli_accepts_headless_protocol_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let _guard = REAL_CLI_PROTOCOL_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        let mut proc = GrokProc::spawn(&spec, None).expect("spawn grok");
        let raw = tokio::time::timeout(std::time::Duration::from_secs(30), proc.next_raw_line())
            .await
            .expect("grok headless protocol did not start")
            .expect("grok rejected arguments without structured output");
        assert!(
            serde_json::from_str::<serde_json::Value>(&raw).is_ok(),
            "{raw}"
        );
        assert!(matches!(
            normalize_grok_line(&raw).as_slice(),
            [AgentEvent::TurnFailed { .. }]
        ));
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn real_cli_auth_failure_is_structured_and_does_not_echo_api_key() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let _guard = REAL_CLI_PROTOCOL_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let secret = "quorum-invalid-xai-key-must-not-be-printed";
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        spec.env_vars
            .iter_mut()
            .find(|(key, _)| key == "XAI_API_KEY")
            .unwrap()
            .1 = secret.into();
        let mut proc = GrokProc::spawn(&spec, None).expect("spawn grok");
        let raw = tokio::time::timeout(std::time::Duration::from_secs(30), proc.next_raw_line())
            .await
            .expect("grok auth failure did not reach the structured protocol")
            .expect("grok auth failure closed stdout without an error event");
        assert!(matches!(
            normalize_grok_line(&raw).as_slice(),
            [AgentEvent::TurnFailed { .. }]
        ));
        assert!(!raw.contains(secret), "API key leaked in structured output");
        let captured = proc.kill_and_reap().await;
        assert!(captured
            .iter()
            .all(|line| !line.session_line().contains(secret)));
    }

    #[test]
    fn real_cli_accepts_exact_resume_placement_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        let mut args = resume_args("00000000-0000-0000-0000-000000000000", &spec).unwrap();
        args.push("--help".into());
        let output = std::process::Command::new("grok")
            .args(args)
            .envs(spec.env_vars.iter().map(|(key, value)| (key, value)))
            .current_dir(&spec.worktree)
            .stdin(Stdio::null())
            .output()
            .expect("run grok resume parser");
        assert!(output.status.success(), "{:?}", output.status);
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("--resume"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[test]
    fn real_cli_rejects_invalid_permission_mode_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let output = std::process::Command::new("grok")
            .args([
                "-p",
                "noop",
                "--output-format",
                "streaming-json",
                "--permission-mode",
                "definitely-invalid",
            ])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("invalid value"), "{stderr}");
    }

    #[test]
    fn synthetic_signal_status_is_not_success() {
        assert!(!std::process::ExitStatus::from_raw(libc::SIGKILL).success());
    }
}
