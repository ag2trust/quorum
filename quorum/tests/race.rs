//! The load-bearing invariant: N separate OS processes racing `claim` on one target produce
//! exactly one winner. This is the canary — if it ever flakes, stop and investigate before
//! anything else.

use std::process::{Command, Stdio};

#[test]
fn n_processes_exactly_one_winner() {
    let home = tempfile::tempdir().unwrap();

    // Initialize first to isolate the *claim* race from the create/migrate race (the latter
    // is covered by cli_init::concurrent_init_is_safe).
    assert_cmd::Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();

    let bin = assert_cmd::cargo::cargo_bin("quorum");
    let n = 20;

    // Spawn all children first (maximize overlap), then wait.
    let children: Vec<_> = (0..n)
        .map(|i| {
            Command::new(&bin)
                .env("QUORUM_HOME", home.path())
                .args([
                    "claim",
                    "--agent",
                    &format!("a{i}"),
                    "--target",
                    "pr#1",
                    "--ttl",
                    "5m",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn quorum claim")
        })
        .collect();

    let wins = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .filter(|out| out.status.success()) // exit 0 == won
        .count();

    assert_eq!(wins, 1, "exactly one process must win the claim");

    // And the store agrees: exactly one active row for the target.
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='pr#1' AND active=1",
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
