mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{
    AllocateRoleInput, Barrier, ClaimCleanupInput, Operation, EXIT_NEGATIVE, EXIT_SUCCESS,
};

const BARRIER_TIMEOUT_MS: u64 = 30_000;
const PARENT_TIMEOUT: Duration = Duration::from_secs(30);

fn barrier(ready_path: PathBuf, go_path: &Path) -> Barrier {
    Barrier {
        ready_path,
        go_path: go_path.to_path_buf(),
        timeout_ms: BARRIER_TIMEOUT_MS,
    }
}

fn release_when_ready(ready_paths: &[PathBuf], go_path: &Path) {
    let deadline = Instant::now() + PARENT_TIMEOUT;
    while !ready_paths.iter().all(|path| path.is_file()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for subprocess helpers: {ready_paths:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(go_path, b"go").unwrap();
}

fn count(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn real_process_allocations_are_atomic(rounds: usize, racers: usize) {
    for same_responsibility in [true, false] {
        for round in 0..rounds {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("quorum.db");
            quorum_core::db::open(&db_path).unwrap();
            let go_path = dir.path().join("go");
            let ready_paths = (0..racers)
                .map(|index| dir.path().join(format!("ready-{index}")))
                .collect::<Vec<_>>();
            let helpers = ready_paths
                .iter()
                .enumerate()
                .map(|(index, ready_path)| {
                    support::spawn(
                        Operation::AllocateRole,
                        &AllocateRoleInput {
                            db_path: db_path.clone(),
                            index,
                            same_responsibility,
                            barrier: barrier(ready_path.clone(), &go_path),
                        },
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();

            release_when_ready(&ready_paths, &go_path);
            let outputs = helpers
                .into_iter()
                .map(|helper| helper.wait(PARENT_TIMEOUT).unwrap())
                .collect::<Vec<_>>();
            assert!(
                outputs.iter().all(|output| {
                    output.status.code() == Some(EXIT_SUCCESS) && output.stderr.is_empty()
                }),
                "round {round}, same={same_responsibility}: {outputs:?}"
            );
            let assignment_ids = outputs
                .iter()
                .map(|output| {
                    output.json()["assignment_id"]
                        .as_i64()
                        .expect("successful allocation returns an assignment id")
                })
                .collect::<HashSet<_>>();

            let conn = quorum_core::db::open(&db_path).unwrap();
            let expected = if same_responsibility {
                1
            } else {
                racers as i64
            };
            assert_eq!(
                count(&conn, "role_assignments"),
                expected,
                "round {round}, same={same_responsibility}"
            );
            assert_eq!(
                conn.query_row("SELECT next_slot FROM routing_cursors", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                expected,
                "round {round}, same={same_responsibility}"
            );
            assert_eq!(assignment_ids.len() as i64, expected);
            assert_eq!(count(&conn, "errors"), 0);
        }
    }
}

#[test]
fn real_process_allocation_smoke_is_atomic() {
    real_process_allocations_are_atomic(1, 2);
}

fn seed_cleanup(conn: &rusqlite::Connection) {
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
         VALUES (1,'source','cancelled','owner',1,1),(2,'child','cancelled','owner',1,1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,
             planned_source_revision,created_at,updated_at)
         VALUES (1,1,'cancelled',0,0,1,1,1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
         VALUES (1,2,'child',1,0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO decomposition_cleanup(
             graph_id,task_id,artifact_kind,artifact_ref,updated_at)
         VALUES (1,2,'process',?1,1)",
        [r#"{"agent":"a","pid":42,"session_id":"s"}"#],
    )
    .unwrap();
}

fn real_process_cleanup_claim_has_exactly_one_winner(iterations: usize) {
    for iteration in 0..iterations {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("quorum-{iteration}.db"));
        let conn = quorum_core::db::open(&db_path).unwrap();
        seed_cleanup(&conn);
        drop(conn);

        let go_path = dir.path().join("go");
        let ready_paths = [dir.path().join("ready-a"), dir.path().join("ready-b")];
        let helpers = ready_paths
            .iter()
            .map(|ready_path| {
                support::spawn(
                    Operation::ClaimCleanup,
                    &ClaimCleanupInput {
                        db_path: db_path.clone(),
                        now: 20,
                        barrier: barrier(ready_path.clone(), &go_path),
                    },
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        release_when_ready(&ready_paths, &go_path);
        let outputs = helpers
            .into_iter()
            .map(|helper| helper.wait(PARENT_TIMEOUT).unwrap())
            .collect::<Vec<_>>();
        assert!(
            outputs.iter().all(|output| output.stderr.is_empty()),
            "iteration {iteration}: {outputs:?}"
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_SUCCESS)
                    && output.json()["won"] == true)
                .count(),
            1,
            "iteration {iteration}"
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_NEGATIVE)
                    && output.json()["won"] == false)
                .count(),
            1,
            "iteration {iteration}"
        );

        let conn = quorum_core::db::open(&db_path).unwrap();
        assert_eq!(count(&conn, "decomposition_cleanup"), 1);
        let state: (String, i64) = conn
            .query_row(
                "SELECT state,attempts FROM decomposition_cleanup",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("running".into(), 1), "iteration {iteration}");
        assert_eq!(count(&conn, "errors"), 0);
    }
}

#[test]
fn real_process_cleanup_claim_smoke_has_exactly_one_winner() {
    real_process_cleanup_claim_has_exactly_one_winner(1);
}

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_real_process_helper_races() {
    real_process_allocations_are_atomic(3, 8);
    real_process_cleanup_claim_has_exactly_one_winner(8);
}
