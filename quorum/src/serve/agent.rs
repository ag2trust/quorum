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
}

pub struct AgentProc {
    child: Child,
    stdin: tokio::process::ChildStdin,
    reader: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
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
            .arg(&spec.effort)
            .arg("--session-id")
            .arg(&spec.session_id)
            .arg("--add-dir")
            .arg(&spec.worktree)
            .arg("--permission-mode")
            .arg("dontAsk");

        if spec.bare {
            cmd.arg("--bare");
        }

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

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

    /// Non-blocking check for child exit. Returns `Some(status)` if the child
    /// has already terminated, `None` if still running. `try_wait` also reaps
    /// the child on the caller's behalf when it has exited.
    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
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
