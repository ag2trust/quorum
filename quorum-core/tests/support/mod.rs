#[allow(dead_code)] // The helper-side inclusion uses the protocol validation limits.
#[path = "../../test-support/protocol.rs"]
pub mod protocol;

use protocol::{Operation, MAX_CAPTURE_BYTES, MAX_INPUT_BYTES};
use serde::Serialize;
use std::fmt;
use std::io::{Read, Write};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const HELPER_PATH: &str = env!("CARGO_BIN_EXE_quorum-core-test-helper");
#[allow(dead_code)] // The race target supplies a longer contention timeout directly.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[allow(dead_code)] // The race target must spawn every contender before waiting.
pub fn run<T: Serialize>(operation: Operation, input: &T) -> Result<HelperOutput, LaunchError> {
    spawn(operation, input)?.wait(DEFAULT_TIMEOUT)
}

pub fn spawn<T: Serialize>(operation: Operation, input: &T) -> Result<RunningHelper, LaunchError> {
    let bytes = serde_json::to_vec(input)
        .map_err(|error| LaunchError::Setup(format!("serialize helper input: {error}")))?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(LaunchError::Setup(format!(
            "helper input exceeds {MAX_INPUT_BYTES} bytes"
        )));
    }
    let mut child = Command::new(HELPER_PATH)
        .arg(operation.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| LaunchError::Setup(format!("spawn helper: {error}")))?;
    let write_result = child
        .stdin
        .take()
        .ok_or_else(|| LaunchError::Setup("helper stdin was not piped".into()))
        .and_then(|mut stdin| {
            stdin
                .write_all(&bytes)
                .map_err(|error| LaunchError::Setup(format!("write helper input: {error}")))
        });
    if let Err(error) = write_result {
        child.kill().ok();
        child.wait().ok();
        return Err(error);
    }
    Ok(RunningHelper {
        child,
        reaped: false,
    })
}

pub struct RunningHelper {
    child: Child,
    reaped: bool,
}

impl RunningHelper {
    pub fn wait(mut self, timeout: Duration) -> Result<HelperOutput, LaunchError> {
        let deadline = Instant::now() + timeout;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.reaped = true;
                    break status;
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    self.child.kill().ok();
                    self.child.wait().ok();
                    self.reaped = true;
                    return Err(LaunchError::Timeout(timeout));
                }
                Err(error) => {
                    self.child.kill().ok();
                    self.child.wait().ok();
                    self.reaped = true;
                    return Err(LaunchError::Setup(format!("wait for helper: {error}")));
                }
            }
        };
        let stdout = read_bounded(
            self.child
                .stdout
                .take()
                .ok_or_else(|| LaunchError::Setup("helper stdout was not piped".into()))?,
            "stdout",
        )?;
        let stderr = read_bounded(
            self.child
                .stderr
                .take()
                .ok_or_else(|| LaunchError::Setup("helper stderr was not piped".into()))?,
            "stderr",
        )?;
        Ok(HelperOutput {
            status,
            stdout,
            stderr,
        })
    }
}

impl Drop for RunningHelper {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        if self.child.try_wait().ok().flatten().is_none() {
            self.child.kill().ok();
        }
        self.child.wait().ok();
        self.reaped = true;
    }
}

#[derive(Debug)]
pub struct HelperOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl HelperOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.stdout).expect("helper stdout must be one JSON value")
    }
}

#[derive(Debug)]
pub enum LaunchError {
    Setup(String),
    Timeout(Duration),
    OutputTooLarge(&'static str),
}

impl fmt::Display for LaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(message) => formatter.write_str(message),
            Self::Timeout(timeout) => write!(formatter, "helper exceeded {timeout:?}"),
            Self::OutputTooLarge(stream) => {
                write!(
                    formatter,
                    "helper {stream} exceeds {MAX_CAPTURE_BYTES} bytes"
                )
            }
        }
    }
}

fn read_bounded(stream: impl Read, label: &'static str) -> Result<Vec<u8>, LaunchError> {
    let mut bytes = Vec::new();
    stream
        .take((MAX_CAPTURE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| LaunchError::Setup(format!("read helper {label}: {error}")))?;
    if bytes.len() > MAX_CAPTURE_BYTES {
        return Err(LaunchError::OutputTooLarge(label));
    }
    Ok(bytes)
}
