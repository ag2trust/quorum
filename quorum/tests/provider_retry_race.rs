//! Cross-process canary for the daemon-private provider-rework claim.
//!
//! The helper is an entry point in this integration-test executable, not a
//! Cargo binary or production CLI command, so it cannot become public surface.

use std::io::Read;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const TTL: i64 = 300;
const CHILD_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CAPTURE_BYTES: usize = 64 * 1024;

#[test]
fn retry_claim_subprocess() {
    let Ok(db_path) = std::env::var("QUORUM_TEST_RETRY_DB") else {
        return;
    };
    let task_id: i64 = std::env::var("QUORUM_TEST_RETRY_TASK")
        .unwrap()
        .parse()
        .unwrap();
    let agent = std::env::var("QUORUM_TEST_RETRY_AGENT").unwrap();
    let gate = std::env::var("QUORUM_TEST_RETRY_GATE").unwrap();
    let now: i64 = std::env::var("QUORUM_TEST_RETRY_NOW")
        .unwrap()
        .parse()
        .unwrap();

    while !std::path::Path::new(&gate).exists() {
        std::thread::sleep(Duration::from_millis(1));
    }

    let result = quorum_core::db::open(std::path::Path::new(&db_path)).and_then(|mut conn| {
        quorum_core::tasks::claim_provider_retry_rework(&mut conn, &agent, task_id, TTL, now)
    });
    match result {
        Ok(Some(_)) => {
            println!("{}", serde_json::json!({"ok": true, "agent": agent}));
            std::process::exit(0);
        }
        Ok(None) => {
            println!("{}", serde_json::json!({"ok": false, "agent": agent}));
            std::process::exit(1);
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({"error": error.to_string(), "agent": agent})
            );
            std::process::exit(3);
        }
    }
}

fn n_process_provider_rework_claim_has_exactly_one_winner(rounds: usize, racers: usize) {
    let test_exe = std::env::current_exe().unwrap();

    for round in 0..rounds {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("quorum.db");
        let gate_path = dir.path().join("start");
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

        let agents: Vec<String> = (0..racers)
            .map(|index| format!("retry-{round}-{index}"))
            .collect();
        let mut children = Vec::with_capacity(agents.len());
        for agent in &agents {
            let child = Command::new(&test_exe)
                .args(["--exact", "retry_claim_subprocess", "--nocapture"])
                .env("QUORUM_TEST_RETRY_DB", &db_path)
                .env("QUORUM_TEST_RETRY_TASK", task_id.to_string())
                .env("QUORUM_TEST_RETRY_AGENT", agent)
                .env("QUORUM_TEST_RETRY_GATE", &gate_path)
                .env("QUORUM_TEST_RETRY_NOW", now.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            children.push(RunningChild::new(child));
        }
        std::fs::write(&gate_path, b"go").unwrap();

        let outcomes: Vec<_> = agents
            .iter()
            .zip(children)
            .map(|(agent, child)| (agent, child.wait(CHILD_TIMEOUT)))
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
            render_outcomes(&outcomes)
        );
        assert!(
            outcomes
                .iter()
                .all(|(_, output)| matches!(output.status.code(), Some(0 | 1))),
            "round {round}: losers must be clean; outcomes={}",
            render_outcomes(&outcomes)
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

struct RunningChild {
    child: Child,
    reaped: bool,
}

impl RunningChild {
    fn new(child: Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn wait(mut self, timeout: Duration) -> Output {
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
                    self.kill_and_reap();
                    panic!("provider retry helper exceeded {timeout:?}");
                }
                Err(error) => {
                    self.kill_and_reap();
                    panic!("wait for provider retry helper: {error}");
                }
            }
        };
        Output {
            status,
            stdout: read_bounded(
                self.child
                    .stdout
                    .take()
                    .expect("helper stdout must be piped"),
                "stdout",
            ),
            stderr: read_bounded(
                self.child
                    .stderr
                    .take()
                    .expect("helper stderr must be piped"),
                "stderr",
            ),
        }
    }

    fn kill_and_reap(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        self.reaped = true;
    }
}

impl Drop for RunningChild {
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

fn read_bounded(mut reader: impl Read, stream: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take((MAX_CAPTURE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .unwrap_or_else(|error| panic!("read provider retry helper {stream}: {error}"));
    assert!(
        bytes.len() <= MAX_CAPTURE_BYTES,
        "provider retry helper {stream} exceeds {MAX_CAPTURE_BYTES} bytes"
    );
    bytes
}

#[test]
fn real_process_provider_rework_claim_smoke_has_exactly_one_winner() {
    n_process_provider_rework_claim_has_exactly_one_winner(1, 2);
}

#[test]
#[ignore = "stress lane: run scripts/stress-process-canaries.sh"]
fn stress_repeats_provider_rework_claim_race() {
    n_process_provider_rework_claim_has_exactly_one_winner(3, 12);
}

fn render_outcomes(outcomes: &[(&String, std::process::Output)]) -> String {
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
