//! Integration tests for `quorum roster`.
//!
//! There is no write command yet (claims arrive in Phase 3), so the end-to-end "a write
//! bumps presence" assertion lives in Phase 3. Here we verify the read path: roster on a
//! fresh DB is an empty JSON array and exits 0.

use assert_cmd::Command;

#[test]
fn roster_empty_on_fresh_db() {
    let home = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::diff("[]\n"));
}
