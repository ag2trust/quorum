mod support;

use quorum_core::decomposition::{self, BeginPlanning, PlannedChild, SourceCancellation};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{
    ApplyGraphEventInput, Barrier, CancelSourceGraphInput, ClaimTaskInput, GraphEvent, Operation,
    EXIT_NEGATIVE, EXIT_SUCCESS,
};

const CHILD_TIMEOUT: Duration = Duration::from_secs(10);
const BARRIER_TIMEOUT: Duration = Duration::from_secs(10);

fn planned_child(key: &str) -> PlannedChild {
    PlannedChild {
        local_key: key.into(),
        title: key.into(),
        body: format!("deliver {key}"),
        labels: None,
        classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
        prerequisite_keys: Vec::new(),
        source_dependency_ids: Vec::new(),
    }
}

fn begin_graph(conn: &mut Connection) -> i64 {
    let graph = decomposition::begin_planning(
        conn,
        &BeginPlanning {
            source_task_id: 1,
            expected_revision: 1,
            provider: "codex",
            model: "sol",
            frozen_base_sha: "abc",
            now: 2,
        },
    )
    .unwrap()
    .unwrap();
    assert!(decomposition::set_frozen_phase(
        conn,
        graph,
        "freeze-requested",
        "preclassifying",
        None,
        2,
    )
    .unwrap());
    graph
}

fn graph_with_children(dir: &tempfile::TempDir, child_keys: &[&str]) -> (PathBuf, i64, Vec<i64>) {
    let db_path = dir.path().join("quorum.db");
    let mut conn = quorum_core::db::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
         VALUES ('large','open','owner',1,1)",
        [],
    )
    .unwrap();
    let graph = begin_graph(&mut conn);
    let children = child_keys
        .iter()
        .map(|key| planned_child(key))
        .collect::<Vec<_>>();
    let ids = decomposition::materialize_graph(&mut conn, graph, 1, &children, 4)
        .unwrap()
        .unwrap();
    drop(conn);
    (db_path, graph, ids)
}

fn barrier(dir: &Path, name: &str, go_path: &Path) -> Barrier {
    Barrier {
        ready_path: dir.join(format!("{name}-ready")),
        go_path: go_path.to_path_buf(),
        timeout_ms: BARRIER_TIMEOUT.as_millis() as u64,
    }
}

fn release_when_ready(ready_paths: &[PathBuf], go_path: &Path) {
    let deadline = Instant::now() + BARRIER_TIMEOUT;
    loop {
        let missing = ready_paths
            .iter()
            .filter(|path| !path.is_file())
            .collect::<Vec<_>>();
        if missing.is_empty() {
            std::fs::write(go_path, b"go").unwrap();
            return;
        }
        assert!(
            Instant::now() < deadline,
            "helpers did not reach simultaneous-start barrier; missing={missing:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_child(label: &str, child: support::RunningHelper) -> support::HelperOutput {
    child
        .wait(CHILD_TIMEOUT)
        .unwrap_or_else(|error| panic!("{label} helper failed or was reaped: {error}"))
}

fn bool_race_outcome(label: &str, output: support::HelperOutput) -> bool {
    let code = output.status.code();
    assert!(
        matches!(code, Some(EXIT_SUCCESS | EXIT_NEGATIVE)),
        "{label} helper failed: {output}"
    );
    assert!(output.stderr.is_empty(), "{label} helper failed: {output}");
    let won = output
        .json()
        .get("won")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_else(|| panic!("{label} helper omitted boolean outcome: {output}"));
    assert_eq!(
        code,
        Some(if won { EXIT_SUCCESS } else { EXIT_NEGATIVE }),
        "{label} helper exit disagrees with response: {output}"
    );
    won
}

fn cancellation_outcome(label: &str, output: support::HelperOutput) -> SourceCancellation {
    let code = output.status.code();
    assert!(
        matches!(code, Some(EXIT_SUCCESS | EXIT_NEGATIVE)),
        "{label} helper failed: {output}"
    );
    assert!(output.stderr.is_empty(), "{label} helper failed: {output}");
    let outcome = match output
        .json()
        .get("outcome")
        .and_then(serde_json::Value::as_str)
    {
        Some("cancelled") => SourceCancellation::Cancelled,
        Some("rejected") => SourceCancellation::Rejected,
        Some("not-graph-source") => SourceCancellation::NotGraphSource,
        _ => panic!("{label} helper omitted cancellation outcome: {output}"),
    };
    assert_eq!(
        code,
        Some(if outcome == SourceCancellation::Cancelled {
            EXIT_SUCCESS
        } else {
            EXIT_NEGATIVE
        }),
        "{label} helper exit disagrees with response: {output}"
    );
    outcome
}

fn race_cancel_with_event(
    db_path: &Path,
    task_id: i64,
    event: GraphEvent,
    dir: &Path,
) -> (bool, SourceCancellation) {
    let go_path = dir.join("event-go");
    let event_barrier = barrier(dir, "event", &go_path);
    let cancel_barrier = barrier(dir, "cancel", &go_path);
    let ready_paths = vec![
        event_barrier.ready_path.clone(),
        cancel_barrier.ready_path.clone(),
    ];
    let event_child = support::spawn(
        Operation::ApplyGraphEvent,
        &ApplyGraphEventInput {
            db_path: db_path.to_path_buf(),
            task_id,
            event,
            now: 10,
            barrier: event_barrier,
        },
    )
    .unwrap_or_else(|error| panic!("spawn event helper: {error}"));
    let cancel_child = support::spawn(
        Operation::CancelSourceGraph,
        &CancelSourceGraphInput {
            db_path: db_path.to_path_buf(),
            caller: "owner".into(),
            source_task_id: 1,
            expected_revision: 1,
            now: 11,
            barrier: cancel_barrier,
        },
    )
    .unwrap_or_else(|error| panic!("spawn cancellation helper: {error}"));
    release_when_ready(&ready_paths, &go_path);
    let event = bool_race_outcome("event", wait_for_child("event", event_child));
    let cancel = cancellation_outcome("cancellation", wait_for_child("cancellation", cancel_child));
    (event, cancel)
}

#[test]
fn real_process_cancel_racing_child_claim_leaves_no_authority() {
    for iteration in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, graph, ids) = graph_with_children(&dir, &["a", "b"]);
        let go_path = dir.path().join("go");
        let claim_barrier = barrier(dir.path(), "claim", &go_path);
        let cancel_barrier = barrier(dir.path(), "cancel", &go_path);
        let ready_paths = vec![
            claim_barrier.ready_path.clone(),
            cancel_barrier.ready_path.clone(),
        ];
        let claim = support::spawn(
            Operation::ClaimTask,
            &ClaimTaskInput {
                db_path: db_path.clone(),
                task_id: ids[0],
                agent: "process-worker".into(),
                barrier: claim_barrier,
            },
        )
        .unwrap_or_else(|error| panic!("spawn claim helper: {error}"));
        let cancel = support::spawn(
            Operation::CancelSourceGraph,
            &CancelSourceGraphInput {
                db_path: db_path.clone(),
                caller: "owner".into(),
                source_task_id: 1,
                expected_revision: 1,
                now: 11,
                barrier: cancel_barrier,
            },
        )
        .unwrap_or_else(|error| panic!("spawn cancellation helper: {error}"));
        release_when_ready(&ready_paths, &go_path);
        let _claim_won = bool_race_outcome("claim", wait_for_child("claim", claim));
        assert_eq!(
            cancellation_outcome("cancellation", wait_for_child("cancellation", cancel)),
            SourceCancellation::Cancelled,
            "iteration {iteration}"
        );

        let conn = quorum_core::db::open(&db_path).unwrap();
        let authority: (i64, i64, i64) = conn
            .query_row(
                "SELECT
                   (SELECT count(*) FROM claims WHERE active=1),
                   (SELECT count(*) FROM run_capabilities WHERE revoked_at IS NULL
                      AND task_id IN (SELECT task_id FROM task_graph_members WHERE graph_id=?1)),
                   (SELECT active FROM task_decompositions WHERE id=?1)",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(authority, (0, 0, 0), "iteration {iteration}");
    }
}

#[test]
fn real_process_cancel_racing_submit_revokes_winner_and_stale_submit_is_inert() {
    for iteration in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, graph, ids) = graph_with_children(&dir, &["a", "b"]);
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "UPDATE tasks SET status='working',assignee='worker' WHERE id=?1",
            [ids[0]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active)
             VALUES (?1,'worker',4,100,1)",
            [format!("task#{}", ids[0])],
        )
        .unwrap();
        quorum_core::capabilities::issue(&mut conn, "worker-run", ids[0], "worker", "worker", 4)
            .unwrap();
        drop(conn);

        let (_submit_won, cancel) =
            race_cancel_with_event(&db_path, ids[0], GraphEvent::Submit, dir.path());
        assert_eq!(
            cancel,
            SourceCancellation::Cancelled,
            "iteration {iteration}"
        );
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let state: (String, String, i64, i64) = conn
            .query_row(
                "SELECT d.state,t.status,
                   (SELECT count(*) FROM claims WHERE active=1),
                   (SELECT count(*) FROM run_capabilities WHERE revoked_at IS NULL
                      AND task_id=?2)
                 FROM task_decompositions d JOIN tasks t ON t.id=?2 WHERE d.id=?1",
                params![graph, ids[0]],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, ("cancelled".into(), "cancelled".into(), 0, 0));
        let events_before: i64 = conn
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert!(quorum_core::tasks::apply_event(
            &mut conn,
            "worker",
            ids[0],
            &quorum_core::lifecycle::Event::SignaledDone { pr: "42".into() },
            12,
        )
        .is_err());
        let events_after: i64 = conn
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_before, events_after, "iteration {iteration}");
    }
}

#[test]
fn real_process_cancel_racing_review_revokes_authority_and_stale_review_is_inert() {
    for iteration in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _graph, ids) = graph_with_children(&dir, &["a", "b"]);
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "UPDATE tasks SET status='in-review',reviewer='reviewer' WHERE id=?1",
            [ids[0]],
        )
        .unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            "review-run",
            ids[0],
            "reviewer",
            "reviewer",
            4,
        )
        .unwrap();
        drop(conn);

        let (_review_won, cancel) =
            race_cancel_with_event(&db_path, ids[0], GraphEvent::Review, dir.path());
        assert_eq!(
            cancel,
            SourceCancellation::Cancelled,
            "iteration {iteration}"
        );
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let authority: (String, i64) = conn
            .query_row(
                "SELECT t.status,
                   (SELECT count(*) FROM run_capabilities WHERE revoked_at IS NULL
                      AND task_id=?1)
                 FROM tasks t WHERE t.id=?1",
                [ids[0]],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(authority, ("cancelled".into(), 0));
        let events_before: i64 = conn
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert!(quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer",
            ids[0],
            &quorum_core::lifecycle::Event::VerdictApprove,
            12,
        )
        .is_err());
        let events_after: i64 = conn
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_before, events_after, "iteration {iteration}");
    }
}

#[test]
fn real_process_cancel_racing_final_merge_has_mutually_exclusive_terminal_outcomes() {
    for iteration in 0..16 {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, graph, ids) = graph_with_children(&dir, &["a", "b"]);
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [ids[0]])
            .unwrap();
        conn.execute("UPDATE tasks SET status='merging' WHERE id=?1", [ids[1]])
            .unwrap();
        drop(conn);

        let (merge_won, cancel) =
            race_cancel_with_event(&db_path, ids[1], GraphEvent::Merge, dir.path());
        let cancel_won = cancel == SourceCancellation::Cancelled;
        assert_ne!(merge_won, cancel_won, "iteration {iteration}");
        let conn = quorum_core::db::open(&db_path).unwrap();
        let state: (String, String, String, String) = conn
            .query_row(
                "SELECT d.state,source.status,first.status,last.status
                 FROM task_decompositions d
                 JOIN tasks source ON source.id=d.source_task_id
                 JOIN tasks first ON first.id=?2 JOIN tasks last ON last.id=?3
                 WHERE d.id=?1",
                params![graph, ids[0], ids[1]],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        if merge_won {
            assert_eq!(cancel, SourceCancellation::Rejected);
            assert_eq!(
                state,
                (
                    "completed".into(),
                    "done".into(),
                    "done".into(),
                    "done".into()
                )
            );
            let cleanup: i64 = conn
                .query_row("SELECT count(*) FROM decomposition_cleanup", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(cleanup, 0);
        } else {
            assert_eq!(cancel, SourceCancellation::Cancelled);
            assert_eq!(
                state,
                (
                    "cancelled".into(),
                    "cancelled".into(),
                    "done".into(),
                    "cancelled".into()
                )
            );
        }
    }
}

#[test]
fn real_process_child_claims_never_exceed_two() {
    let dir = tempfile::tempdir().unwrap();
    let (db_path, _graph, ids) = graph_with_children(&dir, &["a", "b", "c"]);
    let go_path = dir.path().join("go");
    let mut children = Vec::new();
    let mut ready_paths = Vec::new();
    for (index, task_id) in ids.into_iter().enumerate() {
        let claim_barrier = barrier(dir.path(), &format!("claim-{index}"), &go_path);
        ready_paths.push(claim_barrier.ready_path.clone());
        let child = support::spawn(
            Operation::ClaimTask,
            &ClaimTaskInput {
                db_path: db_path.clone(),
                task_id,
                agent: format!("process-{index}"),
                barrier: claim_barrier,
            },
        )
        .unwrap_or_else(|error| panic!("spawn claim helper {index}: {error}"));
        children.push((index, child));
    }
    release_when_ready(&ready_paths, &go_path);
    let winners = children
        .into_iter()
        .map(|(index, child)| {
            bool_race_outcome(
                &format!("claim {index}"),
                wait_for_child(&format!("claim {index}"), child),
            )
        })
        .filter(|won| *won)
        .count();
    assert_eq!(winners, 2);
}
