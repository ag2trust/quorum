//! Integration coverage for planner session tails.

use assert_cmd::Command;
use quorum_core::journal::{self, JournalEntry};
use std::process::Command as ProcessCommand;

fn quorum(home: &std::path::Path) -> Command {
    let mut command = Command::cargo_bin("quorum").unwrap();
    command
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT_ENDPOINT");
    command
}

#[cfg(unix)]
#[test]
fn tail_renders_sanitized_live_fake_codex_planner_attempt() {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();

    let fake_codex = home.path().join("fake-codex");
    std::fs::write(&fake_codex, "#!/bin/sh\nwhile :; do sleep 1; done\n").unwrap();
    std::fs::set_permissions(&fake_codex, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut provider = ProcessCommand::new(&fake_codex).spawn().unwrap();

    let log_dir = home.path().join("planner-log");
    std::fs::create_dir(&log_dir).unwrap();
    let stream = concat!(
        r#"{"event":"provider_lifecycle","provider":"codex","phase":"started"}"#,
        "\n",
        r#"{"event":"command_summary","command":"shell","outcome":"succeeded","details":{"summary":"structural","shape":"string","captured_bytes":30}}"#,
        "\n",
        r#"{"event":"terminal_response","status":"success","response":{"summary":"structural","shape":"object","captured_bytes":256,"truncation":"truncated"}}"#,
        "\n",
        r#"{"event":"completion","outcome":"completed"}"#,
        "\n"
    );
    std::fs::write(log_dir.join("stream.jsonl"), stream).unwrap();

    let db_path = home.path().join("repos/test__repo/quorum.db");
    let mut conn = quorum_core::db::open(&db_path).unwrap();
    journal::upsert(
        &mut conn,
        &JournalEntry {
            agent: "decomposition-planner-17".into(),
            role: "planner".into(),
            task_id: Some(42),
            session_id: "fake-codex-session".into(),
            worktree: None,
            branch: None,
            phase: "planning".into(),
            cost_tokens: 0,
            agent_state: None,
            cost_usd: 0.0,
            log_dir: Some(log_dir.to_string_lossy().into_owned()),
            pid: Some(provider.id() as i32),
            pr: None,
            rework_count: 0,
            provider: Some("codex".into()),
            continuation_id: None,
            local_branch: None,
        },
    )
    .unwrap();
    drop(conn);

    let rendered = quorum(home.path())
        .args(["tail", "decomposition-planner-17"])
        .output()
        .unwrap();
    assert!(rendered.status.success(), "{rendered:?}");
    let rendered = String::from_utf8(rendered.stdout).unwrap();
    for progress in [
        "Provider codex started",
        "Command shell succeeded",
        "Terminal response success",
        "Session completed",
    ] {
        assert!(
            rendered.contains(progress),
            "missing {progress}: {rendered}"
        );
    }
    assert!(!rendered.contains("sk-tail-secret-must-not-appear"));

    let raw = quorum(home.path())
        .args(["tail", "decomposition-planner-17", "--raw"])
        .output()
        .unwrap();
    assert!(raw.status.success(), "{raw:?}");
    let raw = String::from_utf8(raw.stdout).unwrap();
    assert_eq!(raw, stream);
    assert!(!raw.contains("sk-tail-secret-must-not-appear"));

    provider.kill().unwrap();
    provider.wait().unwrap();
}
