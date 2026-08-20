//! Narrow client for daemon-owned managed-run completion operations.
//!
//! This module intentionally exposes no repository locator or general RPC
//! mechanism. Managed clients receive only the daemon-injected Unix socket and
//! their run capability; the daemon derives every authoritative target.

use quorum_core::error::{QuorumError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

const ENDPOINT_ENV: &str = "QUORUM_AGENT_ENDPOINT";
const PROTOCOL_VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct Request<'a> {
    version: u8,
    capability: &'a str,
    operation: Operation<'a>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Operation<'a> {
    AppendNote {
        note: &'a str,
    },
    Submit {
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verdict: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        feedback: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        feedback_json: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blocking: Option<u32>,
    },
    React {
        state: &'a str,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u8,
    ok: bool,
    #[serde(default)]
    result: Option<ResponseResult>,
    #[serde(default)]
    error: Option<ResponseError>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ResponseResult {
    TaskNote { note_id: i64 },
    Mailbox { mailbox_id: i64 },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseError {
    code: String,
    message: String,
}

pub struct Submit<'a> {
    pub capability: &'a str,
    pub summary: Option<&'a str>,
    pub verdict: Option<&'a str>,
    pub feedback: Option<&'a str>,
    pub feedback_json: Option<&'a str>,
    pub blocking: Option<u32>,
}

pub fn submit(request: Submit<'_>) -> Result<i64> {
    mailbox_id(exchange(
        request.capability,
        Operation::Submit {
            summary: request.summary,
            verdict: request.verdict,
            feedback: request.feedback,
            feedback_json: request.feedback_json,
            blocking: request.blocking,
        },
    )?)
}

pub fn react(capability: &str, state: &str) -> Result<i64> {
    mailbox_id(exchange(capability, Operation::React { state })?)
}

/// Append a capability-scoped progress note. The daemon derives the task and
/// agent; callers provide only their run capability and bounded note text.
pub fn append_note(capability: &str, note: &str) -> Result<i64> {
    if note.is_empty() || note.contains('\0') || note.len() > MAX_FRAME_BYTES {
        return Err(QuorumError::Usage(
            "managed progress note must be non-empty, within the endpoint limit, and contain no NUL"
                .into(),
        ));
    }
    note_id(exchange(capability, Operation::AppendNote { note })?)
}

fn exchange(capability: &str, operation: Operation<'_>) -> Result<ResponseResult> {
    let endpoint = endpoint()?;
    let request = Request {
        version: PROTOCOL_VERSION,
        capability,
        operation,
    };
    let body = serde_json::to_vec(&request).map_err(|error| {
        QuorumError::Io(format!("failed to encode agent endpoint request: {error}"))
    })?;
    if body.is_empty() || body.len() > MAX_FRAME_BYTES {
        return Err(QuorumError::Usage(
            "managed completion request exceeds endpoint size limit".into(),
        ));
    }

    let mut stream = UnixStream::connect(&endpoint).map_err(endpoint_io)?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(endpoint_io)?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(endpoint_io)?;
    stream
        .write_all(&(body.len() as u32).to_be_bytes())
        .and_then(|()| stream.write_all(&body))
        .map_err(endpoint_io)?;

    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).map_err(endpoint_io)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(QuorumError::Io(
            "agent endpoint returned malformed response frame".into(),
        ));
    }
    let mut response_body = vec![0; length];
    stream.read_exact(&mut response_body).map_err(endpoint_io)?;
    let response: Response = serde_json::from_slice(&response_body)
        .map_err(|_| QuorumError::Io("agent endpoint returned malformed response".into()))?;
    if response.version != PROTOCOL_VERSION {
        return Err(QuorumError::Io(
            "agent endpoint returned unsupported response version".into(),
        ));
    }
    match (response.ok, response.result, response.error) {
        (true, Some(result), None) => Ok(result),
        (false, None, Some(error)) if !error.code.is_empty() && !error.message.is_empty() => {
            endpoint_rejection(error.code)
        }
        _ => Err(QuorumError::Io(
            "agent endpoint returned malformed response".into(),
        )),
    }
}

fn mailbox_id(result: ResponseResult) -> Result<i64> {
    match result {
        ResponseResult::Mailbox { mailbox_id } if mailbox_id > 0 => Ok(mailbox_id),
        _ => Err(QuorumError::Io(
            "agent endpoint returned malformed response".into(),
        )),
    }
}

fn note_id(result: ResponseResult) -> Result<i64> {
    match result {
        ResponseResult::TaskNote { note_id } if note_id > 0 => Ok(note_id),
        _ => Err(QuorumError::Io(
            "agent endpoint returned malformed response".into(),
        )),
    }
}

fn endpoint_rejection<T>(code: String) -> Result<T> {
    match code.as_str() {
        "unauthorized" | "invalid_operation" | "forbidden_operation" | "operation_unavailable" => {
            Err(QuorumError::Usage(format!(
                "agent endpoint rejected managed completion: {code}"
            )))
        }
        _ => Err(QuorumError::Io(format!(
            "agent endpoint failed managed completion: {code}"
        ))),
    }
}

fn endpoint() -> Result<PathBuf> {
    let value = std::env::var_os(ENDPOINT_ENV).ok_or_else(|| {
        QuorumError::Io("managed completion requires QUORUM_AGENT_ENDPOINT".into())
    })?;
    if value.is_empty() {
        return Err(QuorumError::Io(
            "managed completion requires QUORUM_AGENT_ENDPOINT".into(),
        ));
    }
    Ok(PathBuf::from(value))
}

fn endpoint_io(error: std::io::Error) -> QuorumError {
    let detail = if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        "request timed out"
    } else {
        "request failed"
    };
    QuorumError::Io(format!("agent endpoint {detail}: {error}"))
}
