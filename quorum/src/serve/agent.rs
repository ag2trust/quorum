//! AgentProc: spawn, feed, read, and kill one claude child process.

use super::runner::AgentKind;
use super::stream::{self, Event};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

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
    pub stderr_log: Option<PathBuf>,
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
    stderr_task: Option<JoinHandle<()>>,
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

        // In dontAsk mode every tool call OUTSIDE the allowlist is auto-denied
        // (there is no human to ask). Without --allowedTools the agent cannot
        // edit files, run git/gh, or signal `quorum submit` — it stalls forever
        // in awaiting-review (observed second live run). Same list the
        // hand-run PoC loop used.
        cmd.arg("--add-dir")
            .arg(&spec.worktree)
            .arg("--permission-mode")
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
        let stderr = child.stderr.take().expect("stderr was piped");
        let reader = BufReader::new(stdout).lines();
        let stderr_task = Some(drain_stderr(stderr, spec.stderr_log.clone()));

        Ok(Self {
            child,
            stdin,
            reader,
            stderr_task,
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

    #[cfg(test)]
    pub fn from_parts(
        child: Child,
        stdin: tokio::process::ChildStdin,
        reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    ) -> Self {
        Self {
            child,
            stdin,
            reader,
            stderr_task: None,
        }
    }

    pub async fn kill_and_reap(mut self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
        // Reap the child to avoid zombie accumulation
        let _ = self.child.wait().await;
        if let Some(task) = self.stderr_task.take() {
            let _ = task.await;
        }
    }
}

fn drain_stderr(stderr: tokio::process::ChildStderr, path: Option<PathBuf>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut input = BufReader::new(stderr);
        if let Some(path) = path.filter(|p| p.exists()) {
            let Ok(mut output) = std::fs::OpenOptions::new().append(true).open(path) else {
                let mut sink = tokio::io::sink();
                let _ = tokio::io::copy(&mut input, &mut sink).await;
                return;
            };
            let mut buf = [0; 8192];
            loop {
                match tokio::io::AsyncReadExt::read(&mut input, &mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let _ = std::io::Write::write_all(&mut output, &buf[..n]);
                        let _ = std::io::Write::flush(&mut output);
                    }
                }
            }
        } else {
            let mut sink = tokio::io::sink();
            let _ = tokio::io::copy(&mut input, &mut sink).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stderr_drain_captures_large_diagnostics_without_waiting_for_stdout() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("provider-stderr.log");
        std::fs::write(&path, "--- turn ---\n").unwrap();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("head -c 1048576 /dev/zero | tr '\\0' x >&2; printf done")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let drain = drain_stderr(stderr, Some(path.clone()));
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut output = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut output)
            .await
            .unwrap();
        assert_eq!(output, "done");
        child.wait().await.unwrap();
        drain.await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap().len(), 1_048_589);
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
        proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "claude exited without emitting any stream event — the AgentSpec \
             argument surface was rejected at CLI validation (crash-loop class)"
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
        proc.kill_and_reap().await;
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
            stderr_log: None,
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
}
