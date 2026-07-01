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
    pub allowlist: Vec<String>,
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

        if !spec.allowlist.is_empty() {
            cmd.arg("--allowedTools").arg(spec.allowlist.join(","));
        }

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        unsafe {
            cmd.pre_exec(|| {
                libc::setpgid(0, 0);
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

    #[allow(dead_code)]
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn kill(self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
        // Drop closes stdin, which should cause the child to exit.
    }
}
