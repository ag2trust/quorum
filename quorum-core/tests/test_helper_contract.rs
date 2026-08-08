mod support;

use std::process::{Command, Stdio};
use std::time::Duration;
use support::protocol::{
    AllocateRoleInput, ApplyGraphEventInput, Barrier, CancelSourceGraphInput, ClaimCleanupInput,
    ClaimTaskInput, GraphEvent, Operation, EXIT_INTERNAL, EXIT_NEGATIVE, EXIT_SUCCESS, EXIT_USAGE,
    MAX_INPUT_BYTES,
};

fn file_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quorum.db");
    let conn = quorum_core::db::open(&path).unwrap();
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
         VALUES (1,'allocation target','open','owner',1,1),
                (999,'event target','merging','owner',1,1)",
        [],
    )
    .unwrap();
    (dir, path)
}

fn open_barrier(dir: &std::path::Path, name: &str) -> Barrier {
    let ready_path = dir.join(format!("{name}-ready"));
    let go_path = dir.join(format!("{name}-go"));
    std::fs::write(&go_path, b"go").unwrap();
    Barrier {
        ready_path,
        go_path,
        timeout_ms: 1_000,
    }
}

#[test]
fn every_scaffolded_operation_runs_in_the_dedicated_executable() {
    let (dir, db_path) = file_db();
    let cases = [
        (
            support::run(
                Operation::AllocateRole,
                &AllocateRoleInput {
                    db_path: db_path.clone(),
                    index: 0,
                    same_responsibility: false,
                },
            )
            .unwrap(),
            EXIT_SUCCESS,
        ),
        (
            support::run(
                Operation::ClaimTask,
                &ClaimTaskInput {
                    db_path: db_path.clone(),
                    task_id: 999,
                    agent: "worker".into(),
                    barrier: open_barrier(dir.path(), "claim"),
                },
            )
            .unwrap(),
            EXIT_NEGATIVE,
        ),
        (
            support::run(
                Operation::CancelSourceGraph,
                &CancelSourceGraphInput {
                    db_path: db_path.clone(),
                    caller: "owner".into(),
                    source_task_id: 999,
                    expected_revision: 1,
                    now: 10,
                    barrier: open_barrier(dir.path(), "cancel"),
                },
            )
            .unwrap(),
            EXIT_NEGATIVE,
        ),
        (
            support::run(
                Operation::ApplyGraphEvent,
                &ApplyGraphEventInput {
                    db_path: db_path.clone(),
                    task_id: 999,
                    event: GraphEvent::Merge,
                    now: 10,
                    barrier: open_barrier(dir.path(), "event"),
                },
            )
            .unwrap(),
            EXIT_SUCCESS,
        ),
        (
            support::run(
                Operation::ClaimCleanup,
                &ClaimCleanupInput { db_path, now: 10 },
            )
            .unwrap(),
            EXIT_NEGATIVE,
        ),
    ];

    for (output, expected_code) in cases {
        assert_eq!(output.status.code(), Some(expected_code));
        assert!(output.stderr.is_empty());
        assert!(output.json().is_object());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(!stdout.contains("running "));
        assert!(!stdout.contains("test result:"));
    }
}

#[test]
fn helper_internal_failures_use_the_stable_internal_exit() {
    let (dir, db_path) = file_db();
    let output = support::run(
        Operation::ClaimTask,
        &ClaimTaskInput {
            db_path,
            task_id: 999,
            agent: "worker".into(),
            barrier: Barrier {
                ready_path: dir.path().join("internal-ready"),
                go_path: dir.path().join("internal-go"),
                timeout_ms: 10,
            },
        },
    )
    .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_INTERNAL));
    assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));
}

#[test]
fn absent_graph_event_task_is_a_stable_usage_error() {
    let (dir, db_path) = file_db();
    let output = support::run(
        Operation::ApplyGraphEvent,
        &ApplyGraphEventInput {
            db_path,
            task_id: 404,
            event: GraphEvent::Merge,
            now: 10,
            barrier: open_barrier(dir.path(), "absent-event"),
        },
    )
    .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_USAGE));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("task 404 not found"));
}

#[test]
fn unknown_operation_is_a_stable_usage_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_quorum-core-test-helper"))
        .arg("not-an-operation")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_USAGE));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown operation"));
}

#[test]
fn missing_and_oversized_inputs_fail_loudly() {
    let missing = Command::new(env!("CARGO_BIN_EXE_quorum-core-test-helper"))
        .arg(Operation::ClaimCleanup.as_str())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(EXIT_USAGE));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing JSON input"));

    let mut malformed = Command::new(env!("CARGO_BIN_EXE_quorum-core-test-helper"))
        .arg(Operation::ClaimCleanup.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write;
    malformed
        .stdin
        .take()
        .unwrap()
        .write_all(br#"{"db_path":1}"#)
        .unwrap();
    let malformed = malformed.wait_with_output().unwrap();
    assert_eq!(malformed.status.code(), Some(EXIT_USAGE));
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("invalid JSON input"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_quorum-core-test-helper"))
        .arg(Operation::ClaimCleanup.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&vec![b'x'; MAX_INPUT_BYTES + 1])
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(EXIT_USAGE));
    assert!(String::from_utf8_lossy(&output.stderr).contains("input exceeds"));
}

#[test]
fn launcher_times_out_and_reaps_a_waiting_helper() {
    let (dir, db_path) = file_db();
    let running = support::spawn(
        Operation::ClaimTask,
        &ClaimTaskInput {
            db_path,
            task_id: 999,
            agent: "worker".into(),
            barrier: Barrier {
                ready_path: dir.path().join("ready"),
                go_path: dir.path().join("never-created"),
                timeout_ms: 30_000,
            },
        },
    )
    .unwrap();
    let error = running.wait(Duration::from_millis(100)).unwrap_err();
    assert!(error.to_string().contains("exceeded"));
}
