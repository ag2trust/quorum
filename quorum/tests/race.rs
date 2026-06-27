//! The load-bearing invariant: N separate OS processes racing `task-claim` on one task produce
//! exactly one winner. The task lease reuses the same atomic claims primitive
//! (`UNIQUE(target) WHERE active=1`) the queue is built on. This is the canary — if it ever
//! flakes, stop and investigate before anything else.

use std::process::{Command, Stdio};

#[test]
fn n_processes_exactly_one_winner() {
    let home = tempfile::tempdir().unwrap();

    // Initialize, then create the single task every racer will contend for. This isolates the
    // *claim* race from the create/migrate race (the latter is covered by
    // cli_init::concurrent_init_is_safe) and from the task-create itself.
    assert_cmd::Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();
    assert_cmd::Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .args(["task-create", "--created-by", "boss", "--title", "race-me"])
        .assert()
        .success();

    let bin = assert_cmd::cargo::cargo_bin("quorum");
    let n = 20;

    // Spawn all children first (maximize overlap), then wait. Every child races to claim the
    // same task#1 — exactly one may win the lease.
    let children: Vec<_> = (0..n)
        .map(|i| {
            Command::new(&bin)
                .env("QUORUM_HOME", home.path())
                .args([
                    "task-claim",
                    "--agent",
                    &format!("a{i}"),
                    "--task-id",
                    "1",
                    "--ttl",
                    "5m",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn quorum task-claim")
        })
        .collect();

    let wins = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .filter(|out| out.status.success()) // exit 0 == won
        .count();

    assert_eq!(wins, 1, "exactly one process must win the claim");

    // And the store agrees: exactly one active lease row for the task.
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1, "exactly one active claim row");

    // A normal race must never log an error (guards the boundary-corpse → exit-3 regression).
    let errs: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(errs, 0, "a normal race must not log any errors");
}
