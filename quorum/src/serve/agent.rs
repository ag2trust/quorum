//! AgentProc: spawn, feed, read, and kill one claude child process.

use super::runner::{capture_diagnostics, AgentKind, CapturedOutput, DiagnosticBuffer};
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
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    diagnostics: DiagnosticBuffer,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
}

/// Tool allowlist for spawned agents (dontAsk auto-denies everything else).
/// `Skill` is required so reviewers can invoke the pinned `pr-review` skill
/// (#206) — without it the Skill call is silently denied and the review
/// degrades to an unstructured read.
pub(crate) const ALLOWED_TOOLS: &str = "Bash,Read,Edit,Write,Glob,Grep,TodoWrite,WebFetch,Skill";

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

impl AgentProc {
    pub fn spawn(spec: &AgentSpec, agent_bin: Option<&str>) -> std::io::Result<Self> {
        Self::spawn_configured(spec, agent_bin, false)
    }

    /// Spawn a closed-book classifier while preserving the configured auth
    /// path. Safe mode suppresses user/project customizations without the
    /// credential restrictions of `--bare`; the empty tool surface prevents
    /// the classifier from acquiring context beyond its supplied turn.
    pub fn spawn_restricted(spec: &AgentSpec, agent_bin: Option<&str>) -> std::io::Result<Self> {
        Self::spawn_configured(spec, agent_bin, true)
    }

    fn spawn_configured(
        spec: &AgentSpec,
        agent_bin: Option<&str>,
        restricted: bool,
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

        if restricted {
            cmd.arg("--safe-mode")
                .arg("--disable-slash-commands")
                .arg("--tools")
                .arg("")
                .arg("--no-session-persistence");
        } else {
            cmd.arg("--add-dir").arg(&spec.worktree);
        }

        // In dontAsk mode every tool call OUTSIDE the allowlist is auto-denied
        // (there is no human to ask). Without --allowedTools a managed agent
        // cannot edit files, run git/gh, or signal `quorum submit` — it stalls
        // forever in awaiting-review (observed second live run). Restricted
        // classifier spawns pass an empty list in addition to `--tools ""`.
        cmd.arg("--permission-mode")
            .arg("dontAsk")
            .arg("--allowedTools")
            .arg(&spec.allowed_tools);

        if spec.bare {
            cmd.arg("--bare");
        }

        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
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
        let reader = BufReader::new(stdout).lines();
        let stderr = BufReader::new(child.stderr.take().expect("stderr was piped"));
        let diagnostics = DiagnosticBuffer::default();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });

        Ok(Self {
            child,
            stdin,
            reader,
            diagnostics,
            stderr_task: Some(stderr_task),
        })
    }

    pub async fn feed_turn(&mut self, json_turn: &str) -> std::io::Result<()> {
        self.stdin.write_all(json_turn.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Return the next raw stdout line, or `None` on EOF/error.
    /// Used by the daemon to preserve verbatim JSONL in session logs.
    pub async fn next_raw_line(&mut self) -> Option<String> {
        match self.reader.next_line().await {
            Ok(Some(line)) => Some(line),
            _ => None,
        }
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        loop {
            match self.reader.next_line().await {
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
        reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    ) -> Self {
        let diagnostics = DiagnosticBuffer::default();
        Self {
            child,
            stdin,
            reader,
            diagnostics,
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
        while let Ok(Some(line)) = self.reader.next_line().await {
            terminal.push(CapturedOutput::Stdout(line));
        }
        let diagnostics = self.diagnostics.clone();
        if let Some(stderr_task) = self.stderr_task {
            let _ = stderr_task.await;
        }
        terminal.extend(diagnostics.drain());
        terminal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let reader = BufReader::new(child.stdout.take().unwrap()).lines();
        let stderr = BufReader::new(child.stderr.take().unwrap());
        let diagnostics = DiagnosticBuffer::default();
        let stderr_diagnostics = diagnostics.clone();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        AgentProc {
            child,
            stdin,
            reader,
            diagnostics,
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
        let mut spec = crate::serve::classifier::classifier_spec(tmp.path(), true);
        spec.env_vars = no_auth_env(tmp.path());

        let mut proc = AgentProc::spawn(&spec, None).expect("spawn claude");
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
        let mut spec = crate::serve::classifier::classifier_spec(tmp.path(), false);
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
        let mut spec = crate::serve::classifier::classifier_spec(tmp.path(), true);
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
             echo '{\"type\":\"provider.ready\"}'; \
             echo '{\"type\":\"result\",\"result\":\"trailing\"}'; \
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
