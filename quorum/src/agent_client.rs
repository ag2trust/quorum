//! Narrow client for daemon-owned managed-run completion operations.
//!
//! This module intentionally exposes no repository locator or general RPC
//! mechanism. Managed clients receive only the daemon-injected Unix socket and
//! their run capability; the daemon derives every authoritative target.

use quorum_core::error::{QuorumError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
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
        task_id: i64,
        agent: &'a str,
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
    SubmitPlan {
        response: &'a serde_json::Value,
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
    PlanAccepted { graph_id: i64 },
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

pub struct AppendNote<'a> {
    pub capability: &'a str,
    pub task_id: i64,
    pub agent: &'a str,
    pub note: &'a str,
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

/// Outcome of one planner plan submission.
///
/// A rejection is a value, not an error: the planner is expected to read the
/// endpoint's own validator text and resubmit a corrected plan within the same
/// turn, so the code and message are relayed verbatim rather than flattened
/// into a client-side error string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitPlanOutcome {
    Accepted { graph_id: i64 },
    Rejected { code: String, message: String },
}

/// Submit a plan through a `planner` run capability. The endpoint owns every
/// validation decision; this client neither inspects nor rewrites the plan.
///
/// The endpoint is supplied explicitly because the caller is the `submit_plan`
/// MCP tool, which already holds the daemon-injected locator for its run.
pub fn submit_plan(
    endpoint: &Path,
    capability: &str,
    response: &serde_json::Value,
) -> Result<SubmitPlanOutcome> {
    match exchange_at(endpoint, capability, Operation::SubmitPlan { response })? {
        Ok(ResponseResult::PlanAccepted { graph_id }) if graph_id > 0 => {
            Ok(SubmitPlanOutcome::Accepted { graph_id })
        }
        Ok(_) => Err(QuorumError::Io(
            "agent endpoint returned malformed response".into(),
        )),
        Err(error) => Ok(SubmitPlanOutcome::Rejected {
            code: error.code,
            message: error.message,
        }),
    }
}

/// Append a capability-scoped progress note. The daemon derives authority from
/// the capability and verifies the prompt-compatible identity flags before it
/// appends the bounded note text.
pub fn append_note(request: AppendNote<'_>) -> Result<i64> {
    if request.note.is_empty()
        || request.note.contains('\0')
        || request.note.len() > MAX_FRAME_BYTES
    {
        return Err(QuorumError::Usage(
            "managed progress note must be non-empty, within the endpoint limit, and contain no NUL"
                .into(),
        ));
    }
    note_id(exchange(
        request.capability,
        Operation::AppendNote {
            task_id: request.task_id,
            agent: request.agent,
            note: request.note,
        },
    )?)
}

fn exchange(capability: &str, operation: Operation<'_>) -> Result<ResponseResult> {
    match exchange_at(&endpoint()?, capability, operation)? {
        Ok(result) => Ok(result),
        Err(error) => endpoint_rejection(error.code),
    }
}

/// Round-trip one operation against an explicit endpoint. An endpoint
/// rejection is returned as a value so callers that must relay the daemon's
/// own message can do so verbatim; transport and framing faults stay errors.
#[allow(clippy::type_complexity)]
fn exchange_at(
    endpoint: &Path,
    capability: &str,
    operation: Operation<'_>,
) -> Result<std::result::Result<ResponseResult, ResponseError>> {
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

    let mut stream = UnixStream::connect(endpoint).map_err(endpoint_io)?;
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
        (true, Some(result), None) => Ok(Ok(result)),
        (false, None, Some(error)) if !error.code.is_empty() && !error.message.is_empty() => {
            Ok(Err(error))
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

/// The daemon-injected endpoint locator for this managed run.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::agent_endpoint::{locator, AgentEndpoint};
    use serde_json::json;

    /// A frozen planning graph whose live planner run holds `planner-cap`.
    /// The repo root exists on disk so writable-path resolution can
    /// canonicalize it, and the source task declares dependency 7.
    fn planner_fixture(dir: &Path) -> (PathBuf, PathBuf) {
        let db_path = dir.join("quorum.db");
        let repo_dir = dir.join("repo");
        std::fs::create_dir(&repo_dir).unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO tasks
             (id,title,status,assignee,created_by,created_at,updated_at,depends_on)
             VALUES (1,'planner source','open','Planner','test',10,10,'[7]')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('planner-cap',1,'Planner','planner',10)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_decompositions
             (id,source_task_id,state,active,freeze_active,planner_session_id,
              planned_source_revision,created_at,updated_at)
             VALUES (5,1,'planning',0,1,'planner-cap',3,10,10)",
            [],
        )
        .unwrap();
        drop(conn);
        (db_path, repo_dir)
    }

    fn plan_task(key: &str, path: &str, prerequisites: Vec<&str>) -> serde_json::Value {
        json!({
            "key": key,
            "title": format!("implement {key}"),
            "implementation_delta": format!("edit {path}"),
            "affected_paths": [path],
            "observable_outcome": format!("{path} exposes the new behavior"),
            "deliverables": [{"kind": "write", "path": path}],
            "acceptance_criteria": ["the new behavior is covered by a test"],
            "source_constraints": ["do not change unrelated modules"],
            "verification_expectations": ["cargo test passes"],
            "non_goals": ["no unrelated refactors"],
            "prerequisites": prerequisites,
        })
    }

    /// End-to-end over the real Unix socket: the client's request framing and
    /// serde tagging must be what the daemon endpoint accepts, an accepted
    /// plan must come back as `Accepted`, and a rejected one must relay the
    /// endpoint's own validator text verbatim so the planner can correct it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_round_trips_over_the_endpoint_socket() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, repo_dir) = planner_fixture(dir.path());
        let endpoint = AgentEndpoint::start(
            &db_path,
            "test/repo",
            &repo_dir,
            crate::serve::planner::WritablePathResolver::default(),
        )
        .await
        .unwrap();
        let socket = locator(&db_path);

        let undersized = json!({
            "outcome": "plan",
            "tasks": [plan_task("only", "src/only.rs", vec![])],
        });
        let rejected = {
            let socket = socket.clone();
            tokio::task::spawn_blocking(move || {
                submit_plan(&socket, "planner-cap", &undersized).unwrap()
            })
            .await
            .unwrap()
        };
        match rejected {
            SubmitPlanOutcome::Rejected { code, message } => {
                assert_eq!(code, "invalid_plan");
                assert!(
                    message.contains("plan must contain between 2 and 8 tasks"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected a rejection, got {other:?}"),
        }

        let plan = json!({
            "outcome": "plan",
            "tasks": [
                plan_task("first", "src/first.rs", vec!["source:7"]),
                plan_task("second", "src/second.rs", vec!["first"]),
            ],
        });
        let expected = plan.clone();
        let accepted = {
            let socket = socket.clone();
            tokio::task::spawn_blocking(move || submit_plan(&socket, "planner-cap", &plan).unwrap())
                .await
                .unwrap()
        };
        assert_eq!(accepted, SubmitPlanOutcome::Accepted { graph_id: 5 });

        let stored: String = quorum_core::db::open(&db_path)
            .unwrap()
            .query_row(
                "SELECT response_json FROM planner_submissions WHERE run_id='planner-cap'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
            expected
        );

        endpoint.shutdown().await;
    }
}
