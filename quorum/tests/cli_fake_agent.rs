//! Integration test: fake_agent speaks stream-json correctly.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

#[test]
fn fake_agent_responds_to_turn() {
    let mut child = Command::new(cargo_bin("fake-agent"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn fake-agent");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // Feed a user turn
    let turn = serde_json::json!({
        "type": "user",
        "message": {"content": "Do the task"}
    });
    writeln!(stdin, "{}", turn).unwrap();
    stdin.flush().unwrap();

    // Read assistant event
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["type"], "assistant");
    assert!(event["message"]["content"]
        .as_str()
        .unwrap()
        .contains("Working"));

    // Read result event
    line.clear();
    reader.read_line(&mut line).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["type"], "result");
    assert!(event["usage"]["input_tokens"].as_u64().unwrap() > 0);

    // Feed a second turn
    let turn2 = serde_json::json!({
        "type": "user",
        "message": {"content": "Continue"}
    });
    writeln!(stdin, "{}", turn2).unwrap();
    stdin.flush().unwrap();

    // Read second response
    line.clear();
    reader.read_line(&mut line).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["type"], "assistant");

    line.clear();
    reader.read_line(&mut line).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["type"], "result");

    // Close stdin to terminate
    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success());
}

#[test]
fn fake_agent_handles_review_failed() {
    let mut child = Command::new(cargo_bin("fake-agent"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn fake-agent");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // First turn
    let turn = serde_json::json!({"type": "user", "message": {"content": "Start"}});
    writeln!(stdin, "{}", turn).unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap(); // assistant
    line.clear();
    reader.read_line(&mut line).unwrap(); // result

    // Review failed turn
    let turn2 = serde_json::json!({"type": "user", "message": {"content": "REVIEW FAILED: fix X"}});
    writeln!(stdin, "{}", turn2).unwrap();

    line.clear();
    reader.read_line(&mut line).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert!(event["message"]["content"]
        .as_str()
        .unwrap()
        .contains("Fixing"));

    drop(stdin);
    child.wait().unwrap();
}
