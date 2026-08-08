//! Cross-process canary for the daemon-private provider-rework claim.

mod support;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{Barrier, ClaimProviderRetryInput, Operation, EXIT_NEGATIVE, EXIT_SUCCESS};

const RACERS: usize = 12;
const ROUNDS: usize = 3;
const TTL: i64 = 300;
const CHILD_TIMEOUT: Duration = Duration::from_secs(10);
const BARRIER_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn n_process_provider_rework_claim_exactly_one_winner() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("quorum.db");
        let go_path = dir.path().join("start");
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

        let agents: Vec<String> = (0..RACERS)
            .map(|index| format!("retry-{round}-{index}"))
            .collect();
        let ready_paths: Vec<PathBuf> = agents
            .iter()
            .map(|agent| dir.path().join(format!("{agent}-ready")))
            .collect();
        let children: Vec<_> = agents
            .iter()
            .zip(&ready_paths)
            .map(|(agent, ready_path)| {
                support::spawn(
                    Operation::ClaimProviderRetry,
                    &ClaimProviderRetryInput {
                        db_path: db_path.clone(),
                        task_id,
                        agent: agent.clone(),
                        ttl: TTL,
                        now,
                        barrier: Barrier {
                            ready_path: ready_path.clone(),
                            go_path: go_path.clone(),
                            timeout_ms: BARRIER_TIMEOUT.as_millis() as u64,
                        },
                    },
                )
                .unwrap_or_else(|error| panic!("round {round}: spawn helper {agent}: {error}"))
            })
            .collect();
        release_when_ready(round, &ready_paths, &go_path);

        let outcomes: Vec<_> = agents
            .iter()
            .zip(children)
            .map(|(agent, child)| {
                let output = child.wait(CHILD_TIMEOUT).unwrap_or_else(|error| {
                    panic!("round {round}: helper {agent} failed or was reaped: {error}")
                });
                (agent, output)
            })
            .collect();
        let winners: Vec<_> = outcomes
            .iter()
            .filter(|(_, output)| output.status.code() == Some(EXIT_SUCCESS))
            .map(|(agent, _)| (*agent).clone())
            .collect();
        assert_eq!(
            winners.len(),
            1,
            "round {round}: expected one winner; outcomes={}",
            render_outcomes(&outcomes)
        );
        assert!(
            outcomes.iter().all(|(_, output)| matches!(
                output.status.code(),
                Some(EXIT_SUCCESS | EXIT_NEGATIVE)
            )),
            "round {round}: losers must be clean; outcomes={}",
            render_outcomes(&outcomes)
        );
        for (agent, output) in &outcomes {
            assert!(
                output.stderr.is_empty(),
                "round {round}: helper {agent} wrote stderr; outcomes={}",
                render_outcomes(&outcomes)
            );
            let expected = serde_json::json!({
                "ok": output.status.code() == Some(EXIT_SUCCESS),
                "agent": agent,
            });
            assert_eq!(
                output.json(),
                expected,
                "round {round}: helper {agent} returned unstable JSON; outcomes={}",
                render_outcomes(&outcomes)
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

fn release_when_ready(round: usize, ready_paths: &[PathBuf], go_path: &Path) {
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
            "round {round}: helpers did not reach simultaneous-start barrier; missing={missing:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn render_outcomes(outcomes: &[(&String, support::HelperOutput)]) -> String {
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
