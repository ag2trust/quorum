mod protocol;

use protocol::{
    AllocateRoleInput, ApplyGraphEventInput, Barrier, CancelSourceGraphInput, ClaimCleanupInput,
    ClaimTaskInput, GraphEvent, MaterializeAssessmentInput, Operation, EXIT_INTERNAL,
    EXIT_NEGATIVE, EXIT_SUCCESS, EXIT_USAGE, MAX_BARRIER_WAIT_MS, MAX_INPUT_BYTES, MAX_PATH_BYTES,
    MAX_TEXT_BYTES,
};
use quorum_core::decomposition::SourceCancellation;
use quorum_core::error::QuorumError;
use quorum_core::lifecycle::Event;
use quorum_core::role_assignments::{
    AssignmentRequest, ModelProfile, ValidatedPool, WeightedProfile,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::{Duration, Instant};

fn main() -> ExitCode {
    let code = match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("quorum-core-test-helper: {}", bounded_error(&error.message));
            error.code
        }
    };
    ExitCode::from(u8::try_from(code).unwrap_or(EXIT_INTERNAL as u8))
}

struct HelperError {
    code: i32,
    message: String,
}

impl HelperError {
    fn usage(message: impl Into<String>) -> Self {
        Self {
            code: EXIT_USAGE,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: EXIT_INTERNAL,
            message: message.into(),
        }
    }
}

impl From<QuorumError> for HelperError {
    fn from(error: QuorumError) -> Self {
        Self {
            code: error.exit_code(),
            message: error.to_string(),
        }
    }
}

fn run() -> Result<i32, HelperError> {
    let operation = parse_operation(std::env::args_os())?;
    let input = read_input()?;
    let result = match operation {
        Operation::AllocateRole => allocate_role(parse(&input)?)?,
        Operation::ClaimTask => claim_task(parse(&input)?)?,
        Operation::CancelSourceGraph => cancel_source_graph(parse(&input)?)?,
        Operation::ApplyGraphEvent => apply_graph_event(parse(&input)?)?,
        Operation::ClaimCleanup => claim_cleanup(parse(&input)?)?,
        Operation::MaterializeAssessment => materialize_assessment(parse(&input)?)?,
    };
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, &result.response)
        .map_err(|error| HelperError::internal(format!("write response: {error}")))?;
    stdout
        .write_all(b"\n")
        .map_err(|error| HelperError::internal(format!("write response: {error}")))?;
    Ok(if result.positive {
        EXIT_SUCCESS
    } else {
        EXIT_NEGATIVE
    })
}

struct OperationResult {
    response: Value,
    positive: bool,
}

impl OperationResult {
    fn positive(response: Value) -> Self {
        Self {
            response,
            positive: true,
        }
    }

    fn race(response: Value, won: bool) -> Self {
        Self {
            response,
            positive: won,
        }
    }
}

fn parse_operation(mut args: impl Iterator<Item = OsString>) -> Result<Operation, HelperError> {
    let _program = args.next();
    let raw = args
        .next()
        .ok_or_else(|| HelperError::usage("missing operation"))?;
    if args.next().is_some() {
        return Err(HelperError::usage("expected exactly one operation"));
    }
    let raw = raw
        .to_str()
        .ok_or_else(|| HelperError::usage("operation is not valid UTF-8"))?;
    Operation::from_str(raw).map_err(|()| HelperError::usage(format!("unknown operation: {raw}")))
}

fn read_input() -> Result<Vec<u8>, HelperError> {
    let mut bytes = Vec::new();
    io::stdin()
        .lock()
        .take((MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| HelperError::internal(format!("read stdin: {error}")))?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(HelperError::usage(format!(
            "input exceeds {MAX_INPUT_BYTES} bytes"
        )));
    }
    if bytes.is_empty() {
        return Err(HelperError::usage("missing JSON input"));
    }
    Ok(bytes)
}

fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, HelperError> {
    serde_json::from_slice(bytes)
        .map_err(|error| HelperError::usage(format!("invalid JSON input: {error}")))
}

fn allocate_role(input: AllocateRoleInput) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    if input.index > 100_000 {
        return Err(HelperError::usage("allocation index exceeds 100000"));
    }
    wait_at_barrier(&input.barrier)?;
    let index = if input.same_responsibility {
        0
    } else {
        input.index
    };
    let mut conn = quorum_core::db::open(&input.db_path)?;
    let assignment = quorum_core::role_assignments::assign_or_get_with_seed(
        &mut conn,
        &AssignmentRequest {
            responsibility_key: format!("worker:task:{index}"),
            task_id: Some(index as i64 + 1),
            pr_number: None,
            role: "worker".into(),
            review_stage: None,
            complexity: Some("M".into()),
        },
        &allocation_pool(),
        input.index as u64,
        10,
    )?;
    Ok(OperationResult::positive(
        json!({"assignment_id": assignment.id}),
    ))
}

fn allocation_pool() -> ValidatedPool {
    ValidatedPool {
        pool_key: "worker.M".into(),
        policy_generation: "g1".into(),
        profiles: vec![
            WeightedProfile {
                profile: ModelProfile {
                    id: "a".into(),
                    provider: "codex".into(),
                    runner: "codex".into(),
                    model: "a-model".into(),
                    effort: "high".into(),
                },
                percent: 80,
            },
            WeightedProfile {
                profile: ModelProfile {
                    id: "b".into(),
                    provider: "claude".into(),
                    runner: "claude".into(),
                    model: "b-model".into(),
                    effort: "medium".into(),
                },
                percent: 20,
            },
        ],
    }
}

fn claim_task(input: ClaimTaskInput) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    validate_positive("task id", input.task_id)?;
    validate_text("agent", &input.agent)?;
    wait_at_barrier(&input.barrier)?;
    let mut conn = quorum_core::db::open(&input.db_path)?;
    let won = quorum_core::tasks::claim(&mut conn, &input.agent, Some(input.task_id), &[], 60, 10)?
        .is_some();
    Ok(OperationResult::race(json!({"won": won}), won))
}

fn cancel_source_graph(input: CancelSourceGraphInput) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    validate_positive("source task id", input.source_task_id)?;
    validate_positive("expected revision", input.expected_revision)?;
    validate_text("caller", &input.caller)?;
    wait_at_barrier(&input.barrier)?;
    let mut conn = quorum_core::db::open(&input.db_path)?;
    let outcome = quorum_core::decomposition::cancel_source_graph(
        &mut conn,
        &input.caller,
        input.source_task_id,
        Some(input.expected_revision),
        input.now,
    )?;
    let (outcome, positive) = match outcome {
        SourceCancellation::Cancelled => ("cancelled", true),
        SourceCancellation::Rejected => ("rejected", false),
        SourceCancellation::NotGraphSource => ("not-graph-source", false),
    };
    Ok(OperationResult::race(json!({"outcome": outcome}), positive))
}

fn apply_graph_event(input: ApplyGraphEventInput) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    validate_positive("task id", input.task_id)?;
    wait_at_barrier(&input.barrier)?;
    let (actor, event) = match input.event {
        GraphEvent::Submit => ("worker", Event::SignaledDone { pr: "42".into() }),
        GraphEvent::Review => ("reviewer", Event::VerdictApprove),
        GraphEvent::Merge => ("system", Event::MergeSucceeded),
    };
    let mut conn = quorum_core::db::open(&input.db_path)?;
    let won =
        match quorum_core::tasks::apply_event(&mut conn, actor, input.task_id, &event, input.now) {
            Ok(_) => true,
            Err(QuorumError::NotHolder) => false,
            Err(error @ QuorumError::Usage(_)) => {
                let cancelled = quorum_core::tasks::get(&conn, input.task_id)?
                    .is_some_and(|task| task.status == "cancelled");
                if !cancelled {
                    return Err(error.into());
                }
                false
            }
            Err(error) => return Err(error.into()),
        };
    Ok(OperationResult::race(json!({"won": won}), won))
}

fn claim_cleanup(input: ClaimCleanupInput) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    wait_at_barrier(&input.barrier)?;
    let mut conn = quorum_core::db::open(&input.db_path)?;
    let work = quorum_core::decomposition_cleanup::claim_next(&mut conn, input.now)?;
    Ok(match work {
        Some(work) => OperationResult::positive(json!({
            "won": true,
            "graph_id": work.key.graph_id,
            "task_id": work.key.task_id,
            "artifact_kind": work.key.artifact_kind,
            "artifact_ref": work.key.artifact_ref,
            "attempt": work.attempt,
        })),
        None => OperationResult::race(json!({"won": false}), false),
    })
}

fn materialize_assessment(
    input: MaterializeAssessmentInput,
) -> Result<OperationResult, HelperError> {
    validate_path("database", &input.db_path)?;
    wait_at_barrier(&input.barrier)?;
    let scope_kind =
        quorum_core::review_followup_assessments::FollowupScopeKind::from_str(&input.scope_kind)?;
    let value = quorum_core::review_followup_assessments::NewReviewFollowupAssessment::new(
        scope_kind,
        input.scope_id,
        input.source_task_id,
        input.artifact_ids,
        input.now,
    )?;
    let mut conn = quorum_core::db::open(&input.db_path)?;
    conn.pragma_update(None, "foreign_keys", true)
        .map_err(QuorumError::from)?;
    let assessment =
        quorum_core::review_followup_assessments::materialize_assessment(&mut conn, &value)?;
    Ok(match assessment {
        Some(assessment) => OperationResult::positive(json!({
            "won": true,
            "assessment_id": assessment.id(),
        })),
        None => OperationResult::race(json!({"won": false}), false),
    })
}

fn wait_at_barrier(barrier: &Barrier) -> Result<(), HelperError> {
    validate_path("ready barrier", &barrier.ready_path)?;
    validate_path("go barrier", &barrier.go_path)?;
    if barrier.ready_path == barrier.go_path {
        return Err(HelperError::usage("barrier paths must be distinct"));
    }
    if !(1..=MAX_BARRIER_WAIT_MS).contains(&barrier.timeout_ms) {
        return Err(HelperError::usage(format!(
            "barrier timeout must be 1..={MAX_BARRIER_WAIT_MS} ms"
        )));
    }
    std::fs::write(&barrier.ready_path, b"ready")
        .map_err(|error| HelperError::internal(format!("write ready barrier: {error}")))?;
    let deadline = Instant::now() + Duration::from_millis(barrier.timeout_ms);
    loop {
        if barrier.go_path.is_file() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(HelperError::internal("timed out waiting for go barrier"));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn validate_path(label: &str, path: &Path) -> Result<(), HelperError> {
    let value = path
        .to_str()
        .ok_or_else(|| HelperError::usage(format!("{label} path is not valid UTF-8")))?;
    if value.is_empty() || value.contains('\0') || value.len() > MAX_PATH_BYTES {
        return Err(HelperError::usage(format!(
            "{label} path must be 1..={MAX_PATH_BYTES} bytes without NUL"
        )));
    }
    Ok(())
}

fn validate_text(label: &str, value: &str) -> Result<(), HelperError> {
    if value.is_empty() || value.contains('\0') || value.len() > MAX_TEXT_BYTES {
        return Err(HelperError::usage(format!(
            "{label} must be 1..={MAX_TEXT_BYTES} bytes without NUL"
        )));
    }
    Ok(())
}

fn validate_positive(label: &str, value: i64) -> Result<(), HelperError> {
    if value <= 0 {
        return Err(HelperError::usage(format!("{label} must be positive")));
    }
    Ok(())
}

fn bounded_error(message: &str) -> &str {
    const MAX_ERROR_BYTES: usize = 4 * 1024;
    if message.len() <= MAX_ERROR_BYTES {
        return message;
    }
    let mut boundary = MAX_ERROR_BYTES;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &message[..boundary]
}
