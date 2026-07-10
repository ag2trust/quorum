//! AgentProc: spawn, feed, read, and kill one claude child process.

use super::stream::{self, Event};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

pub struct AgentSpec {
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
        // edit files, run git/gh, or signal `quorum done` — it stalls forever
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
        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let reader = BufReader::new(stdout).lines();

        Ok(Self {
            child,
            stdin,
            reader,
        })
    }

    pub async fn feed_turn(&mut self, json_turn: &str) -> std::io::Result<()> {
        self.stdin.write_all(json_turn.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
