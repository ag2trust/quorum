//! Startup smoke test (spec §4, MANDATORY, gated QUORUM_E2E=1).
//!
//! Spawns a real `claude` with the persistent stdin-fed invocation and verifies:
//! 1. Two-turn persistence on one PID
//! 2. Context carry-over across turns (turn 2 references turn 1)
//!
//! Run: QUORUM_E2E=1 cargo test -p quorum smoke_e2e -- --nocapture

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn smoke_two_turn_persistence() {
    if std::env::var("QUORUM_E2E").unwrap_or_default() != "1" {
        eprintln!("QUORUM_E2E not set — skipping real-claude smoke test");
        return;
    }

    let session_id = uuid::Uuid::new_v4().to_string();
    let tmp = tempfile::tempdir().unwrap();

    let mut child = Command::new("claude")
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            "haiku",
            "--session-id",
            &session_id,
            "--add-dir",
            &tmp.path().to_string_lossy(),
            "--permission-mode",
            "dontAsk",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn claude — is it installed?");

    let pid = child.id();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    // Read stdout on a dedicated thread so timeouts are enforceable
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // Turn 1: ask it to reply with a specific word
    let turn1 = serde_json::json!({
        "type": "user",
        "message": {"content": "Reply with exactly the word PINEAPPLE and nothing else."}
    });
    writeln!(stdin, "{}", turn1).unwrap();
    stdin.flush().unwrap();

    // Wait for result event
    let mut got_result = false;
    while let Ok(line) = rx.recv_timeout(Duration::from_secs(30)) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            if v["type"] == "result" {
                got_result = true;
                break;
            }
        }
    }
    assert!(got_result, "turn 1 did not produce a result event");

    // Verify same PID (process still alive)
    assert_eq!(child.id(), pid, "PID changed — process was restarted");

    // Turn 2: ask what we told it to reply with
    let turn2 = serde_json::json!({
        "type": "user",
        "message": {"content": "What word did I just ask you to reply with in the previous turn?"}
    });
    writeln!(stdin, "{}", turn2).unwrap();
    stdin.flush().unwrap();

    // Collect turn 2 response
    let mut response_text = String::new();
    while let Ok(line) = rx.recv_timeout(Duration::from_secs(30)) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            if v["type"] == "assistant" {
                if let Some(content) = v["message"]["content"].as_str() {
                    response_text.push_str(content);
                }
            }
            if v["type"] == "result" {
                break;
            }
        }
    }

    assert!(
        response_text.to_uppercase().contains("PINEAPPLE"),
        "turn 2 response did not reference turn 1 context (got: {response_text})"
    );

    // Verify same PID
    assert_eq!(child.id(), pid, "PID changed between turns");

    // Clean up
    drop(stdin);
    child.kill().ok();
    child.wait().ok();
}
