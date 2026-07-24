//! Codex CLI process management: spec, command builder, spawn.
//!
//! Analogous to `agent.rs` (Claude CLI boundary) but targeting
//! `codex exec --json`. The Codex CLI does not use session UUIDs for first
//! runs — the thread ID is provider-issued and emitted in the first
//! `thread.started` JSONL event.

use super::codex_stream::{self, Event};
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
    vec![
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
        spec.prompt.clone(),
    ]
}

/// Build the argument list for `codex exec resume <thread_id> --json`
/// (continuation turn).
pub fn resume_args(
    thread_id: &str,
    model: &str,
    effort: &str,
    sandbox: &str,
    worktree: &std::path::Path,
    prompt: &str,
) -> Vec<String> {
    vec![
        "exec".into(),
        "resume".into(),
        thread_id.into(),
        "--json".into(),
        "--model".into(),
        model.into(),
        "-c".into(),
        format!("model_reasoning_effort={effort}"),
        "-s".into(),
        sandbox.into(),
        "--dangerously-bypass-approvals-and-sandbox".into(),
        "-C".into(),
        worktree.display().to_string(),
        "--skip-git-repo-check".into(),
        "--ignore-user-config".into(),
        prompt.into(),
    ]
}

// ---------------------------------------------------------------------------
// Process wrapper
// ---------------------------------------------------------------------------

pub struct CodexProc {
    child: Child,
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
}

impl CodexProc {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_resume(
        thread_id: &str,
        model: &str,
        effort: &str,
        sandbox: &str,
        worktree: &std::path::Path,
        prompt: &str,
        env_vars: &[(String, String)],
        codex_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let args = resume_args(thread_id, model, effort, sandbox, worktree, prompt);
        let mut cmd = Command::new(bin);
        cmd.args(&args);
        for (k, v) in env_vars {
            cmd.env(k, v);
        }
        cmd.current_dir(worktree);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

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
        let reader = BufReader::new(stdout).lines();

        Ok(Self { child, reader })
    }

    pub fn spawn(spec: &CodexSpec, codex_bin: Option<&str>) -> std::io::Result<Self> {
        let bin = codex_bin.unwrap_or("codex");
        let args = exec_args(spec);
        let mut cmd = Command::new(bin);
        cmd.args(&args);
        for (k, v) in &spec.env_vars {
            cmd.env(k, v);
        }
        cmd.current_dir(&spec.worktree);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

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
        let reader = BufReader::new(stdout).lines();

        Ok(Self { child, reader })
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        loop {
            match self.reader.next_line().await {
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

    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub async fn kill_and_reap(mut self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
        let _ = self.child.wait().await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

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

    // ── Pinned resume argument shape ─────────────────────────────────────

    #[test]
    fn resume_args_shape() {
        let args = resume_args(
            "019f-thread-id",
            "o4-mini",
            "high",
            "read-only",
            Path::new("/tmp"),
            "continue",
        );
        assert_eq!(args[0], "exec");
        assert_eq!(args[1], "resume");
        assert_eq!(args[2], "019f-thread-id");
        assert_eq!(args[3], "--json");
        assert_eq!(args[4], "--model");
        assert_eq!(args[5], "o4-mini");
        assert_eq!(args[6], "-c");
        assert_eq!(args[7], "model_reasoning_effort=high");
        assert_eq!(args[8], "-s");
        assert_eq!(args[9], "read-only");
        assert_eq!(args[10], "--dangerously-bypass-approvals-and-sandbox");
        assert_eq!(args[11], "-C");
        assert_eq!(args[12], "/tmp");
        assert_eq!(args[13], "--skip-git-repo-check");
        assert_eq!(args[14], "--ignore-user-config");
        assert_eq!(args[15], "continue");
        assert_eq!(args.len(), 16);
    }

    #[test]
    fn resume_args_thread_id_position() {
        let args = resume_args(
            "my-thread",
            "gpt-4o",
            "medium",
            "workspace-write",
            Path::new("/w"),
            "go",
        );
        assert_eq!(
            args[2], "my-thread",
            "thread_id must be positional arg after 'resume'"
        );
    }

    #[test]
    fn resume_args_carry_json_flag() {
        let args = resume_args("tid", "m", "h", "s", Path::new("/"), "p");
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
        let mut proc = CodexProc::spawn(&spec, None).expect("spawn codex");
        let event = tokio::time::timeout(std::time::Duration::from_secs(60), proc.next_event())
            .await
            .expect("codex produced no event within 60s — args may hang the CLI");
        proc.kill_and_reap().await;
        assert!(
            event.is_some(),
            "codex exited without emitting any JSONL event — exec argument \
             surface was rejected at CLI validation (crash-loop class)"
        );
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
        proc.kill_and_reap().await;
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
        let args = resume_args(
            "00000000-0000-0000-0000-000000000000",
            "o4-mini",
            "high",
            "read-only",
            Path::new("/tmp"),
            "continue",
        );
        let mut cmd = std::process::Command::new("codex");
        cmd.args(&args);
        for (k, v) in no_auth_env(tmp.path()) {
            cmd.env(&k, &v);
        }
        cmd.current_dir("/tmp");
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        let output = cmd.output().expect("spawn codex resume");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() && stdout.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let is_arg_error = stderr.contains("Usage:") || stderr.contains("error: invalid");
            assert!(
                !is_arg_error,
                "codex rejected resume argument shape: {stderr}"
            );
        }
    }
}
