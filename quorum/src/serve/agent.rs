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
    pub resume: bool,
}

pub struct AgentProc {
    child: Child,
    stdin: tokio::process::ChildStdin,
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
}

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

        if spec.resume {
            cmd.arg("--resume").arg(&spec.session_id);
        } else {
            cmd.arg("--session-id").arg(&spec.session_id);
        }

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
            .arg("Bash,Read,Edit,Write,Glob,Grep,TodoWrite,WebFetch");

        if spec.bare {
            cmd.arg("--bare");
        }

        // Run the agent inside its worktree — `--add-dir` only grants access,
        // it does not set the working directory (first live run: workers ran
        // in the daemon's cwd). Inherit stderr so CLI-level failures surface
        // in the daemon log instead of vanishing (same run: exit-1 cause lost).
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
