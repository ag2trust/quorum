mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{
    AcquireDaemonLockInput, AllocateRoleInput, Barrier, ClaimCleanupInput,
    ClaimGithubOperationInput, ClaimProviderRetryReworkInput, ClaimReviewCleanupInput,
    DiscardRunnerRetryInput, Operation, PendingTurnInput, ProbeReviewSlotInput,
    RetryProviderBlockedInput, EXIT_NEGATIVE, EXIT_SUCCESS,
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

fn provider_rework_claim_has_exactly_one_winner(rounds: usize, racers: usize) {
    const TTL: i64 = 300;

    for round in 0..rounds {
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
                size_reason: "bounded test classification rationale".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
                risk_flags: vec![],
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
        let agents: Vec<String> = (0..racers)
            .map(|index| format!("retry-{round}-{index}"))
            .collect();
        let ready_paths = (0..racers)
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

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_real_process_helper_races() {
    real_process_allocations_are_atomic(3, 8);
    real_process_cleanup_claim_has_exactly_one_winner(8);
    real_process_github_operation_claim_has_exactly_one_winner(20, 8);
    real_process_review_create_claim_races_cleanup(40, 8);
    real_process_review_slot_probe_is_unique(20, 8);
}

fn seed_github_operation(conn: &mut rusqlite::Connection) {
    conn.execute_batch(
        "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
         VALUES (1,'src','working','owner',1,1);
         INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
         VALUES ('run-1',1,'Worker','worker',1);
         INSERT INTO github_collaboration_attempts(
             attempt_id,task_id,agent,role,pr_number,lifecycle_generation,
             active_run_id,state,created_at,updated_at,expires_at
         ) VALUES ('att-1',1,'Worker','worker',7,3,'run-1','active',1,1,999);",
    )
    .unwrap();
    let request = quorum_core::github_operations::NewGithubOperation {
        operation_id: "op-race",
        client_request_id: "req-race",
        attempt_id: "att-1",
        created_by_run_id: "run-1",
        task_id: 1,
        agent: "Worker",
        role: "worker",
        pr_number: 7,
        head_sha: None,
        reviewer_launch_sha: None,
        lifecycle_generation: Some(3),
        kind: "add_issue_comment",
        request_json: r#"{"body":"hi"}"#,
        deadline_at: 500,
        review_sequence: None,
        group_key: None,
        github_marker: None,
        created_at: 1,
        expires_at: 999,
    };
    assert!(quorum_core::github_operations::enqueue(conn, &request).unwrap());
}

/// Cross-process GitHub-operation claim canary: `racers` OS processes race
/// `github_operations::claim_next` on one queued write behind a start
/// barrier. Exactly one claims, starts, and settles it (exit 0); every other
/// racer observes a clean negative (exit 1) and no `errors` row is written.
fn real_process_github_operation_claim_has_exactly_one_winner(iterations: usize, racers: usize) {
    const NOW: i64 = 100;
    for iteration in 0..iterations {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("quorum-{iteration}.db"));
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        seed_github_operation(&mut conn);
        drop(conn);

        let go_path = dir.path().join("go");
        let ready_paths = (0..racers)
            .map(|index| dir.path().join(format!("ready-{index}")))
            .collect::<Vec<_>>();
        let helpers = ready_paths
            .iter()
            .map(|ready_path| {
                support::spawn(
                    Operation::ClaimGithubOp,
                    &ClaimGithubOperationInput {
                        db_path: db_path.clone(),
                        now: NOW,
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
        let winners = outputs
            .iter()
            .filter(|output| output.status.code() == Some(EXIT_SUCCESS))
            .collect::<Vec<_>>();
        assert_eq!(winners.len(), 1, "iteration {iteration}: {outputs:?}");
        let winner = winners[0].json();
        assert_eq!(winner["won"], true);
        assert_eq!(winner["operation_id"], "op-race");
        assert_eq!(winner["attempt"], 1);
        assert_eq!(winner["started"], true);
        assert_eq!(winner["settled"], true);
        assert_eq!(
            outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_NEGATIVE)
                    && output.json()["won"] == false)
                .count(),
            racers - 1,
            "iteration {iteration}: {outputs:?}"
        );

        let conn = quorum_core::db::open(&db_path).unwrap();
        let row = quorum_core::github_operations::get_by_operation_id(&conn, "op-race")
            .unwrap()
            .unwrap();
        assert_eq!(row.attempts, 1, "iteration {iteration}");
        assert_eq!(row.state, "succeeded");
        assert_eq!(row.send_state, "confirmed");
        assert!(!row.active);
        assert_eq!(count(&conn, "errors"), 0);
    }
}

#[test]
fn real_process_github_operation_claim_smoke_has_exactly_one_winner() {
    real_process_github_operation_claim_has_exactly_one_winner(2, 4);
}

/// One reviewer attempt owning the `(scope-1, PR 7)` slot with its queued
/// pending-review create (review sequence 0).
fn seed_owned_review_create(conn: &mut rusqlite::Connection) {
    conn.execute_batch(
        "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
         VALUES (1,'src','in-review','owner',1,1);
         INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
         VALUES ('run-1',1,'Reviewer','reviewer',1);
         INSERT INTO github_collaboration_attempts(
             attempt_id,task_id,agent,role,pr_number,head_sha,lifecycle_generation,
             active_run_id,review_owner_marker,state,created_at,updated_at,expires_at
         ) VALUES ('att-1',1,'Reviewer','reviewer',7,'sha-1',3,'run-1','marker-1','active',
                   1,1,999);
         INSERT INTO github_review_publication_slots(
             publisher_scope,pr_number,task_id,attempt_id,state,review_owner_marker,
             created_at,updated_at
         ) VALUES ('scope-1',7,1,'att-1','owned','marker-1',1,1);",
    )
    .unwrap();
    let request = quorum_core::github_operations::NewGithubOperation {
        operation_id: "op-create",
        client_request_id: "req-create",
        attempt_id: "att-1",
        created_by_run_id: "run-1",
        task_id: 1,
        agent: "Reviewer",
        role: "reviewer",
        pr_number: 7,
        head_sha: Some("sha-1"),
        reviewer_launch_sha: Some("sha-1"),
        lifecycle_generation: Some(3),
        kind: "pull_request_review_write",
        request_json: r#"{"method":"create"}"#,
        deadline_at: 500,
        review_sequence: Some(0),
        group_key: None,
        github_marker: None,
        created_at: 1,
        expires_at: 999,
    };
    assert!(quorum_core::github_operations::enqueue(conn, &request).unwrap());
}

/// Cross-process pending-review create claim vs cleanup canary. Half the
/// racers run the outbox claim (claim + start + settle), half move the owned
/// slot to `cleanup_required` and try to claim cleanup. Whichever transaction
/// wins, the create is sent at most once, only after `ambiguous` was persisted
/// on both coupled rows, and a cleanup claim that won first cancels it unsent.
fn real_process_review_create_claim_races_cleanup(iterations: usize, racers: usize) {
    const NOW: i64 = 100;
    let mut create_first = 0;
    let mut cleanup_first = 0;
    for iteration in 0..iterations {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("quorum-{iteration}.db"));
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        seed_owned_review_create(&mut conn);
        drop(conn);

        let go_path = dir.path().join("go");
        let ready_paths = (0..racers)
            .map(|index| dir.path().join(format!("ready-{index}")))
            .collect::<Vec<_>>();
        let helpers = ready_paths
            .iter()
            .enumerate()
            .map(|(index, ready_path)| {
                let barrier = barrier(ready_path.clone(), &go_path);
                // Alternate sides, and alternate which side spawns first.
                if (index + iteration) % 2 == 0 {
                    support::spawn(
                        Operation::ClaimGithubOp,
                        &ClaimGithubOperationInput {
                            db_path: db_path.clone(),
                            now: NOW,
                            barrier,
                        },
                    )
                } else {
                    support::spawn(
                        Operation::ClaimReviewCleanup,
                        &ClaimReviewCleanupInput {
                            db_path: db_path.clone(),
                            attempt_id: "att-1".into(),
                            now: NOW,
                            barrier,
                        },
                    )
                }
                .map(|helper| ((index + iteration) % 2 == 0, helper))
                .unwrap()
            })
            .collect::<Vec<_>>();

        release_when_ready(&ready_paths, &go_path);
        let outputs = helpers
            .into_iter()
            .map(|(is_create, helper)| (is_create, helper.wait(PARENT_TIMEOUT).unwrap()))
            .collect::<Vec<_>>();
        assert!(
            outputs.iter().all(|(_, output)| output.stderr.is_empty()
                && matches!(output.status.code(), Some(EXIT_SUCCESS | EXIT_NEGATIVE))),
            "iteration {iteration}: {outputs:?}"
        );
        let create_wins = outputs
            .iter()
            .filter(|(is_create, output)| *is_create && output.status.code() == Some(EXIT_SUCCESS))
            .collect::<Vec<_>>();
        let cleanup_wins = outputs
            .iter()
            .filter(|(is_create, output)| !*is_create && output.status.code() == Some(EXIT_SUCCESS))
            .count();
        let transitions = outputs
            .iter()
            .filter(|(is_create, output)| !*is_create && output.json()["transitioned"] == true)
            .count();
        assert!(create_wins.len() <= 1, "iteration {iteration}: {outputs:?}");
        assert!(cleanup_wins <= 1, "iteration {iteration}: {outputs:?}");
        assert_eq!(transitions, 1, "iteration {iteration}: {outputs:?}");

        let conn = quorum_core::db::open(&db_path).unwrap();
        let create = quorum_core::github_operations::get_by_operation_id(&conn, "op-create")
            .unwrap()
            .unwrap();
        let (slot_state, slot_send): (String, String) = conn
            .query_row(
                "SELECT state, create_send_state FROM github_review_publication_slots
                 WHERE publisher_scope='scope-1' AND pr_number=7",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(create.attempts <= 1, "iteration {iteration}");
        assert!(!create.active, "iteration {iteration}");
        // The coupled state machine agrees after every interleaving.
        assert_eq!(create.send_state, slot_send, "iteration {iteration}");
        match create_wins.first() {
            Some((_, winner)) => {
                create_first += 1;
                let winner = winner.json();
                assert_eq!(create.attempts, 1);
                if winner["started"] == true {
                    // Sent: ambiguity was persisted before I/O and confirmed.
                    assert_eq!(winner["settled"], true);
                    assert_eq!(
                        (create.state.as_str(), create.send_state.as_str()),
                        ("succeeded", "confirmed")
                    );
                    assert!(create.execution_started_at.is_some());
                } else {
                    // Cleanup won between claim and start: never sent.
                    assert_eq!(
                        (create.state.as_str(), create.send_state.as_str()),
                        ("cancelled", "definitely_unsent")
                    );
                    assert!(create.execution_started_at.is_none());
                }
                assert_ne!(slot_state, "owned", "iteration {iteration}");
            }
            None => {
                cleanup_first += 1;
                // Cleanup won the transaction race: the create was refused and
                // cancelled while provably unsent.
                assert_eq!(create.attempts, 0, "iteration {iteration}");
                assert_eq!(
                    (create.state.as_str(), create.send_state.as_str()),
                    ("cancelled", "not_started"),
                    "iteration {iteration}"
                );
                assert!(create.execution_started_at.is_none());
                assert_eq!(cleanup_wins, 1, "iteration {iteration}: {outputs:?}");
                assert_eq!(slot_state, "cleanup_running");
            }
        }
        assert_eq!(count(&conn, "errors"), 0);
    }
    eprintln!(
        "review create/cleanup race: create_first={create_first} cleanup_first={cleanup_first}"
    );
}

#[test]
fn real_process_review_create_claim_smoke_races_cleanup() {
    real_process_review_create_claim_races_cleanup(6, 4);
}

/// Cross-process pending-review slot uniqueness canary. Same-scope racers
/// contend on `PRIMARY KEY(publisher_scope, pr_number)`: exactly one probe
/// claims and owns. Distinct-scope racers each claim their own slot but
/// contend on one attempt: `UNIQUE(attempt_id)` lets exactly one own it.
fn real_process_review_slot_probe_is_unique(iterations: usize, racers: usize) {
    const NOW: i64 = 100;
    for iteration in 0..iterations {
        for same_scope in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join(format!("quorum-{iteration}.db"));
            let conn = quorum_core::db::open(&db_path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                 VALUES (1,'src','in-review','owner',1,1);
                 INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
                 VALUES ('run-1',1,'Reviewer','reviewer',1);
                 INSERT INTO github_collaboration_attempts(
                     attempt_id,task_id,agent,role,pr_number,head_sha,lifecycle_generation,
                     active_run_id,state,created_at,updated_at,expires_at
                 ) VALUES ('att-1',1,'Reviewer','reviewer',7,'sha-1',3,'run-1','active',
                           1,1,999);",
            )
            .unwrap();
            drop(conn);
            let go_path = dir.path().join("go");
            let ready_paths = (0..racers)
                .map(|index| dir.path().join(format!("ready-{index}")))
                .collect::<Vec<_>>();
            let helpers = ready_paths
                .iter()
                .enumerate()
                .map(|(index, ready_path)| {
                    support::spawn(
                        Operation::ProbeReviewSlot,
                        &ProbeReviewSlotInput {
                            db_path: db_path.clone(),
                            publisher_scope: if same_scope {
                                "scope".into()
                            } else {
                                format!("scope-{index}")
                            },
                            attempt_id: "att-1".into(),
                            now: NOW,
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
            let context = format!("iteration {iteration} same_scope={same_scope}: {outputs:?}");
            assert!(
                outputs.iter().all(|output| output.stderr.is_empty()),
                "{context}"
            );
            let owners = outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_SUCCESS))
                .count();
            let claimed = outputs
                .iter()
                .filter(|output| output.json()["claimed"] == true)
                .count();
            assert_eq!(owners, 1, "{context}");
            assert_eq!(
                outputs
                    .iter()
                    .filter(|output| output.status.code() == Some(EXIT_NEGATIVE))
                    .count(),
                racers - 1,
                "{context}"
            );
            assert_eq!(claimed, if same_scope { 1 } else { racers }, "{context}");

            let conn = quorum_core::db::open(&db_path).unwrap();
            let owned: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM github_review_publication_slots
                     WHERE state='owned' AND attempt_id='att-1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(owned, 1, "{context}");
            assert_eq!(
                count(&conn, "github_review_publication_slots"),
                if same_scope { 1 } else { racers as i64 },
                "{context}"
            );
            assert_eq!(count(&conn, "errors"), 0);
        }
    }
}

#[test]
fn real_process_review_slot_probe_smoke_is_unique() {
    real_process_review_slot_probe_is_unique(2, 4);
}

/// Cross-process daemon-lock canary. Each round has `racers` distinct OS
/// processes race `daemon_lock::try_acquire` on the same fresh DB behind a
/// simultaneous-start barrier. The atomicity guarantee is that exactly one
/// process observes `Acquired` (exit 0) and the rest observe
/// `Held { .. }` (exit 1), every round — a real-process complement to the
/// in-process concurrency test in `daemon_lock`.
fn real_process_daemon_lock_has_exactly_one_winner(iterations: usize, racers: usize) {
    const STALE_SECS: i64 = 30;
    const NOW: i64 = 5_000;

    for iteration in 0..iterations {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("quorum-{iteration}.db"));
        // Pre-create the DB (schema install) so the racers don't contend on
        // migration on top of the lock race.
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
                    Operation::AcquireDaemonLock,
                    &AcquireDaemonLockInput {
                        db_path: db_path.clone(),
                        // Distinct diagnostic PIDs; identity is the fresh
                        // per-process instance id the helper mints.
                        pid: 1_000 + index as i64,
                        now: NOW,
                        stale_secs: STALE_SECS,
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
            "iteration {iteration}: helper wrote to stderr; outputs={outputs:?}"
        );
        let winners = outputs
            .iter()
            .filter(|output| {
                output.status.code() == Some(EXIT_SUCCESS) && output.json()["won"] == true
            })
            .count();
        let losers = outputs
            .iter()
            .filter(|output| {
                output.status.code() == Some(EXIT_NEGATIVE) && output.json()["won"] == false
            })
            .count();
        assert_eq!(
            winners, 1,
            "iteration {iteration}: expected exactly one Acquired; outputs={outputs:?}"
        );
        assert_eq!(
            losers,
            racers - 1,
            "iteration {iteration}: every other process must observe Held; outputs={outputs:?}"
        );
        for output in &outputs {
            if output.status.code() == Some(EXIT_NEGATIVE) {
                let json = output.json();
                assert!(
                    json.get("holder_pid").and_then(|v| v.as_i64()).is_some(),
                    "iteration {iteration}: Held helper omitted holder_pid; outputs={outputs:?}"
                );
                assert!(
                    json.get("heartbeat_age").and_then(|v| v.as_i64()).is_some(),
                    "iteration {iteration}: Held helper omitted heartbeat_age; outputs={outputs:?}"
                );
            }
        }

        // The winning identity must be the one stored in daemon_lock.
        let winner_id = outputs
            .iter()
            .find(|output| output.status.code() == Some(EXIT_SUCCESS))
            .and_then(|output| output.json()["instance_id"].as_str().map(str::to_owned))
            .expect("winner must emit its instance id");
        let winner_pid = outputs
            .iter()
            .find(|output| output.status.code() == Some(EXIT_SUCCESS))
            .and_then(|output| output.json()["pid"].as_i64())
            .expect("winner must emit its pid");
        let conn = quorum_core::db::open(&db_path).unwrap();
        let row: (i64, Option<String>) = conn
            .query_row(
                "SELECT pid, instance_id FROM daemon_lock WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (winner_pid, Some(winner_id)),
            "iteration {iteration}: stored lock row must match the winning process"
        );
        assert_eq!(count(&conn, "errors"), 0, "iteration {iteration}");
    }
}

/// Default gate: several iterations with a modest fan-out. Kept repeated
/// enough to expose flakes on the atomic acquire path per the repo
/// concurrency-evidence policy, while still cheap to run on every push.
#[test]
fn real_process_daemon_lock_smoke_has_exactly_one_winner() {
    real_process_daemon_lock_has_exactly_one_winner(4, 4);
}

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_real_process_daemon_lock_race() {
    real_process_daemon_lock_has_exactly_one_winner(12, 8);
}

#[test]
fn real_process_provider_rework_claim_smoke_has_exactly_one_winner() {
    provider_rework_claim_has_exactly_one_winner(1, 2);
}

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_provider_rework_claim_race() {
    provider_rework_claim_has_exactly_one_winner(3, 12);
}

fn discard_turn() -> PendingTurnInput {
    PendingTurnInput {
        provider: "codex".into(),
        model: "gpt-5".into(),
        effort: "high".into(),
        prompt: "finish exact turn".into(),
        turn_kind: "rework".into(),
        continuation_id: Some("thread-live".into()),
        requested: true,
    }
}

/// Real-process race between the core `task-retry` path and the guarded
/// discard of an unresumable runner retry. The seeded refs make both
/// individually admissible (a provider block beside a requested turn), and
/// either winner invalidates every other racer, so each round must have
/// exactly one winner, clean losers, and no `errors` rows.
fn task_retry_and_runner_retry_discard_have_exactly_one_winner(rounds: usize, per_kind: usize) {
    const TTL: i64 = 3_600;

    for round in 0..rounds {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("quorum.db");
        let now = 20_000 + round as i64;
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "runner retry discard race",
            None,
            0,
            None,
            None,
            None,
            None,
            now - 3,
        )
        .unwrap();
        quorum_core::classify::store_classifications(
            &mut conn,
            &[quorum_core::classify::TaskClassification {
                task_id,
                cx_est: 3,
                size: "M".into(),
                size_reason: "bounded test classification rationale".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
                risk_flags: vec![],
            }],
            "integration-test:v2",
            now - 3,
        )
        .unwrap();
        quorum_core::tasks::claim(&mut conn, "worker", Some(task_id), &[], TTL, now - 2)
            .unwrap()
            .expect("seed worker claims the task");
        let turn = discard_turn();
        let refs = serde_json::json!({
            "runner_provider_block": {"provider": "codex", "reason": "quota"},
            "runner_retry": turn,
            "runner_continuation": {"provider": "codex", "id": "thread-live"},
        });
        quorum_core::tasks::update_refs_daemon(&mut conn, task_id, &refs.to_string(), now - 1)
            .unwrap();
        drop(conn);

        let go_path = dir.path().join("go");
        let mut labels = Vec::new();
        let mut ready_paths = Vec::new();
        let mut helpers = Vec::new();
        for index in 0..per_kind {
            let ready = dir.path().join(format!("ready-retry-{index}"));
            helpers.push(
                support::spawn(
                    Operation::RetryProviderBlocked,
                    &RetryProviderBlockedInput {
                        db_path: db_path.clone(),
                        task_id,
                        by: format!("operator-{index}"),
                        now,
                        barrier: barrier(ready.clone(), &go_path),
                    },
                )
                .unwrap(),
            );
            labels.push(format!("retry-{index}"));
            ready_paths.push(ready);

            let ready = dir.path().join(format!("ready-discard-{index}"));
            helpers.push(
                support::spawn(
                    Operation::DiscardRunnerRetry,
                    &DiscardRunnerRetryInput {
                        db_path: db_path.clone(),
                        task_id,
                        holder: "worker".into(),
                        expected_turn: discard_turn(),
                        new_profile_id: "worker.M.fallback".into(),
                        now,
                        barrier: barrier(ready.clone(), &go_path),
                    },
                )
                .unwrap(),
            );
            labels.push(format!("discard-{index}"));
            ready_paths.push(ready);
        }
        release_when_ready(&ready_paths, &go_path);

        let outcomes: Vec<_> = labels
            .iter()
            .zip(helpers)
            .map(|(label, helper)| (label, helper.wait(PARENT_TIMEOUT).unwrap()))
            .collect();
        let rendered = render_provider_retry_outcomes(&outcomes);
        let winners: Vec<&String> = outcomes
            .iter()
            .filter(|(_, output)| output.status.code() == Some(EXIT_SUCCESS))
            .map(|(label, _)| *label)
            .collect();
        assert_eq!(winners.len(), 1, "round {round}: {rendered}");
        assert!(
            outcomes.iter().all(|(_, output)| matches!(
                output.status.code(),
                Some(EXIT_SUCCESS | EXIT_NEGATIVE)
            )),
            "round {round}: losers must be clean; {rendered}"
        );
        for (_, output) in &outcomes {
            assert_eq!(
                output.json()["ok"],
                output.status.code() == Some(EXIT_SUCCESS),
                "round {round}: {rendered}"
            );
        }

        let conn = quorum_core::db::open(&db_path).unwrap();
        let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        let notes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_notes WHERE task_id=?1 AND agent='daemon'
                   AND body LIKE 'runner retry discarded:%'",
                rusqlite::params![task_id],
                |row| row.get(0),
            )
            .unwrap();
        if winners[0].starts_with("discard-") {
            assert_eq!(task.status, "working", "round {round}");
            assert_eq!(task.assignee.as_deref(), Some("worker"), "round {round}");
            for key in [
                "runner_retry",
                "runner_continuation",
                "runner_provider_block",
            ] {
                assert!(refs.get(key).is_none(), "round {round}: {key} in {refs}");
            }
            assert_eq!(notes, 1, "round {round}");
        } else {
            assert_eq!(task.status, "open", "round {round}");
            assert_eq!(task.assignee, None, "round {round}");
            assert_eq!(refs["runner_retry"]["requested"], true, "round {round}");
            assert!(refs.get("runner_provider_block").is_none(), "round {round}");
            assert_eq!(notes, 0, "round {round}");
        }
        assert_eq!(count(&conn, "errors"), 0, "round {round}: {rendered}");
    }
}

#[test]
fn real_process_task_retry_and_runner_retry_discard_smoke_has_exactly_one_winner() {
    task_retry_and_runner_retry_discard_have_exactly_one_winner(2, 2);
}

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_task_retry_and_runner_retry_discard_race() {
    task_retry_and_runner_retry_discard_have_exactly_one_winner(12, 6);
}
