mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{
    AllocateRoleInput, Barrier, ClaimCleanupInput, ClaimProviderRetryReworkInput, Operation,
    EXIT_NEGATIVE, EXIT_SUCCESS,
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

#[test]
fn repeated_real_process_allocations_are_atomic() {
    const ROUNDS: usize = 3;
    const RACERS: usize = 8;

    for same_responsibility in [true, false] {
        for round in 0..ROUNDS {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("quorum.db");
            quorum_core::db::open(&db_path).unwrap();
            let go_path = dir.path().join("go");
            let ready_paths = (0..RACERS)
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
                RACERS as i64
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

#[test]
fn concurrent_processes_have_exactly_one_cleanup_claim_winner() {
    for iteration in 0..8 {
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
fn n_process_provider_rework_claim_exactly_one_winner() {
    const RACERS: usize = 12;
    const ROUNDS: usize = 3;
    const TTL: i64 = 300;

    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("quorum.db");
        let now = 10_000 + round as i64;
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "provider retry race",
            None,
            0,
            None,
            Some(r#"{"codex_retry_requested":true}"#),
            None,
            None,
            now - 1,
        )
        .unwrap();
        quorum_core::classify::store_classifications(
            &mut conn,
            &[quorum_core::classify::TaskClassification {
                task_id,
                cx_est: 3,
                size: "M".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "integration-test:v2",
            now,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework', assignee=NULL WHERE id=?1",
            rusqlite::params![task_id],
        )
        .unwrap();
        drop(conn);

        let go_path = dir.path().join("go");
        let agents: Vec<String> = (0..RACERS)
            .map(|index| format!("retry-{round}-{index}"))
            .collect();
        let ready_paths = (0..RACERS)
            .map(|index| dir.path().join(format!("ready-{index}")))
            .collect::<Vec<_>>();
        let helpers = agents
            .iter()
            .zip(&ready_paths)
            .map(|(agent, ready_path)| {
                support::spawn(
                    Operation::ClaimProviderRetryRework,
                    &ClaimProviderRetryReworkInput {
                        db_path: db_path.clone(),
                        task_id,
                        agent: agent.clone(),
                        ttl: TTL,
                        now,
                        barrier: barrier(ready_path.clone(), &go_path),
                    },
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        release_when_ready(&ready_paths, &go_path);

        let outcomes: Vec<_> = agents
            .iter()
            .zip(helpers)
            .map(|(agent, helper)| (agent, helper.wait(PARENT_TIMEOUT).unwrap()))
            .collect();
        let winners: Vec<_> = outcomes
            .iter()
            .filter(|(_, output)| output.status.code() == Some(0))
            .map(|(agent, _)| (*agent).clone())
            .collect();
        assert_eq!(
            winners.len(),
            1,
            "round {round}: expected one winner; outcomes={}",
            render_provider_retry_outcomes(&outcomes)
        );
        assert!(
            outcomes
                .iter()
                .all(|(_, output)| matches!(output.status.code(), Some(0 | 1))),
            "round {round}: losers must be clean; outcomes={}",
            render_provider_retry_outcomes(&outcomes)
        );
        for (agent, output) in &outcomes {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let expected = serde_json::json!({
                "ok": output.status.code() == Some(0),
                "agent": agent,
            })
            .to_string();
            assert!(
                stdout.lines().any(|line| line == expected),
                "round {round}: missing stable helper JSON {expected:?}; stdout={stdout:?}"
            );
        }

        let winner = &winners[0];
        let conn = quorum_core::db::open(&db_path).unwrap();
        let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.assignee.as_deref(), Some(winner.as_str()));
        let active: Vec<String> = conn
            .prepare(
                "SELECT holder FROM claims
                 WHERE target=?1 AND active=1 AND expires_at>?2",
            )
            .unwrap()
            .query_map(rusqlite::params![format!("task#{task_id}"), now], |row| {
                row.get(0)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(active, vec![winner.clone()]);
        let errors: i64 = conn
            .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(errors, 0, "round {round}: normal losers logged errors");
    }
}

fn render_provider_retry_outcomes(outcomes: &[(&String, support::HelperOutput)]) -> String {
    outcomes
        .iter()
        .map(|(agent, output)| {
            format!(
                "{agent}: code={:?} stdout={:?} stderr={:?}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}
