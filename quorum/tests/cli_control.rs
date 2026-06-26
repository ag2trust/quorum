//! End-to-end CLI tests for the emergency stop primitive (issue #6):
//! `quorum stop`, `quorum resume`, `quorum stops`. Verifies the JSON contract surface +
//! exit-code semantics + that the message feed isn't polluted by control state.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn stop_global_then_stops_lists_then_resume_clears() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["stop", "--by", "cto"])
        .arg("--reason-stdin")
        .write_stdin("deploy in flight\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"global\""))
        .stdout(predicates::str::contains("\"by\":\"cto\""));

    quorum(home.path())
        .args(["stops"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"global\""));

    // Resume clears it; emits a `stop_cleared` event readable via `quorum log`.
    quorum(home.path())
        .args(["resume", "--by", "cto"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ok\":true"))
        .stdout(predicates::str::contains("\"cleared\""));

    // No active stops post-resume.
    quorum(home.path())
        .args(["stops"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));

    // The stop_cleared event landed in the EVENT LOG (not the feed).
    quorum(home.path())
        .args(["log", "--refs", "global"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"kind\":\"stop_cleared\""));
}

#[test]
fn stop_requires_reason_via_stdin_or_file() {
    // Invariant #10: free text never as a flag. No --reason → exit 2 (usage error).
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args(["stop", "--by", "cto"])
        .assert()
        .code(2);
}

#[test]
fn resume_with_no_active_stop_exits_1_clean() {
    // Clean "didn't get it" exit 1, not an error. Mirrors how `task-claim` reports nothing
    // claimable.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args(["resume", "--by", "cto"])
        .assert()
        .code(1)
        .stdout(predicates::str::contains("no active stop"));
}

#[test]
fn agent_scoped_stop_does_not_affect_others_in_listing() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["stop", "--agent", "Alice", "--by", "cto"])
        .arg("--reason-stdin")
        .write_stdin("rate-limited\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"agent:Alice\""));

    // `quorum stops` lists the targeted stop, NOT a global one.
    quorum(home.path())
        .args(["stops"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"agent:Alice\""))
        .stdout(predicates::str::contains("\"scope\":\"global\"").count(0));
}

#[test]
fn global_and_targeted_can_coexist() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["stop", "--by", "cto"])
        .arg("--reason-stdin")
        .write_stdin("all hands\n")
        .assert()
        .success();
    quorum(home.path())
        .args(["stop", "--agent", "Alice", "--by", "cto"])
        .arg("--reason-stdin")
        .write_stdin("and you specifically\n")
        .assert()
        .success();

    // Both rows visible (deterministic order: agent:Alice before global per ASCII sort).
    quorum(home.path())
        .args(["stops"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"agent:Alice\""))
        .stdout(predicates::str::contains("\"scope\":\"global\""));

    // Clearing global does NOT clear the targeted stop.
    quorum(home.path())
        .args(["resume", "--by", "cto"])
        .assert()
        .success();
    quorum(home.path())
        .args(["stops"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"scope\":\"agent:Alice\""))
        .stdout(predicates::str::contains("\"scope\":\"global\"").count(0));
}

#[test]
fn message_feed_is_not_polluted_by_stop_events() {
    // `stop_cleared` events live in the EVENT LOG (`quorum log`), NOT the agent-to-agent
    // message feed. Issue #4 separated those streams so auto-events don't drown human
    // messages; the same separation applies to stop signals.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["stop", "--by", "cto"])
        .arg("--reason-stdin")
        .write_stdin("x\n")
        .assert()
        .success();
    quorum(home.path())
        .args(["resume", "--by", "cto"])
        .assert()
        .success();
    quorum(home.path())
        .args(["read", "--agent", "Bystander"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
}
