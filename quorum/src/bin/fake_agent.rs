//! Fake agent binary for daemon CI tests.
//!
//! Reads stream-json user turns from stdin (one JSON per line), emits a scripted
//! stream-json response for each: an assistant text event followed by a result event.
//! Deterministic, no network. Accepts `--session-id`, `--model`, `--effort`, etc.
//! (all ignored — the daemon passes them but the fake doesn't need them).
//!
//! Behaviour:
//! - Turn 1: emits assistant "Working on task..." + result with usage
//! - Any turn containing "REVIEW FAILED": emits assistant "Fixing..." + result
//! - Any turn containing "DIE_MID_TURN": emits assistant, then exits(1) BEFORE
//!   emitting the result (simulates a crashed agent that never signals done —
//!   used by the death-detection test).
//! - Any other turn: emits assistant "Acknowledged" + result
//! - Stays alive between turns (persistent stdin-fed mode).
//!
//! Modes (env-var triggered, composable):
//! - `FAKE_AGENT_EMIT_TOOL_USE=1`: emits 2-3 tool_use stream events per turn
//!   before the result, so the daemon's tool_count/now_label code paths get exercised.
//! - `FAKE_AGENT_SIDE_EFFECTS=1`: after the result event on turn 1, calls
//!   `quorum submit --agent $QUORUM_AGENT` as a real subprocess, creating a mailbox row.
//! - `FAKE_AGENT_ERROR_RESULT=N`: emits `is_error: true` on the first N turns,
//!   then succeeds on subsequent turns. Used to test the daemon's error-refeed logic.

use std::io::{self, BufRead, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

const SUBMIT_ATTEMPTS: u32 = 3;

fn emit_assistant(text: &str) {
    let msg = serde_json::json!({
        "type": "assistant",
        "message": {"content": text}
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn emit_tool_use(name: &str, input: serde_json::Value) {
    let msg = serde_json::json!({
        "type": "tool_use",
        "name": name,
        "input": input
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn emit_result(turn: u32, cumulative_cost: f64, is_error: bool) {
    let input_tokens = 500 * turn as u64;
    let output_tokens = 200 * turn as u64;
    let result_text = if is_error {
        format!("API Error: Connection closed mid-response (turn {turn})")
    } else {
        format!("turn-{turn}-complete")
    };
    let msg = serde_json::json!({
        "type": "result",
        "result": result_text,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
        "total_cost_usd": cumulative_cost,
        "num_turns": turn,
        "duration_ms": 1000 * turn as u64,
        "is_error": is_error,
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

/// Answer a classifier prompt: one classification per `### Task #<id>`.
/// Test-only size variation records whether --bare reached the process.
/// The JSON goes in the Result event's `result` field — the daemon's drain
/// replaces accumulated assistant text with a non-empty result text.
fn emit_classifier_result(line: &str, bare: bool) {
    let mut ids: Vec<i64> = Vec::new();
    for chunk in line.split("### Task #").skip(1) {
        let digits: String = chunk.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(id) = digits.parse::<i64>() {
            ids.push(id);
        }
    }
    let tasks: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "task_id": id,
                "complexity": 2,
                "size": if bare { "M" } else { "S" },
                "size_reason": "test fixture is one focused execution seam",
                "ready": true,
                "not_ready_reason": null,
                "duplicate_of": [],
            })
        })
        .collect();
    let msg = serde_json::json!({
        "type": "result",
        "result": serde_json::json!({ "tasks": tasks }).to_string(),
        "usage": { "input_tokens": 100, "output_tokens": 50 },
        "total_cost_usd": 0.001,
        "num_turns": 1,
        "duration_ms": 100,
        "is_error": false,
    });
    println!("{msg}");
    io::stdout().flush().ok();
}

/// Answer a post-merge collector prompt with a canned findings envelope. Fixed
/// evidence ids let integration tests assert provenance passes through end-to-end.
/// Tests may override the primary and repair payloads independently to exercise
/// the collector's bounded response-repair path.
fn emit_collector_result(fail: bool, response_override: Option<String>) {
    let (result, is_error) = if fail {
        (
            serde_json::Value::String("Collector fake failure".to_string()),
            true,
        )
    } else if let Some(response) = response_override {
        (serde_json::Value::String(response), false)
    } else {
        let findings = serde_json::json!({
            "findings": [
                {
                    "reviewer": "fake-reviewer",
                    "kind": "blocking",
                    "author_pushback": false,
                    "pushback_accepted": null,
                    "severity": "major",
                    "text": "Missing null check on config path",
                    "source_endpoint": "pulls",
                    "addressed_status": "addressed",
                    "evidence": [{"kind": "review_comment", "id": 101}]
                },
                {
                    "reviewer": "fake-reviewer",
                    "kind": "suggestion",
                    "author_pushback": true,
                    "pushback_accepted": true,
                    "severity": "nit",
                    "text": "Rename var to something less generic",
                    "source_endpoint": "issues",
                    "addressed_status": "unaddressed",
                    "evidence": [{"kind": "issue_comment", "id": 202}]
                }
            ],
            "followup_artifacts": []
        });
        (serde_json::Value::String(findings.to_string()), false)
    };
    let msg = serde_json::json!({
        "type": "result",
        "result": result,
        "usage": {"input_tokens": 500, "output_tokens": 100},
        "total_cost_usd": 0.002,
        "num_turns": 1,
        "duration_ms": 100,
        "is_error": is_error,
    });
    println!("{msg}");
    io::stdout().flush().ok();
}

fn quorum_bin_path() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("cannot resolve own exe path");
    exe.parent().expect("exe has no parent dir").join("quorum")
}

fn retry_submit(mut submit: impl FnMut() -> bool, mut wait: impl FnMut(Duration)) -> bool {
    for attempt in 1..=SUBMIT_ATTEMPTS {
        if submit() {
            return true;
        }
        if attempt < SUBMIT_ATTEMPTS {
            wait(Duration::from_millis(250 * u64::from(attempt)));
        }
    }
    false
}

fn run_quorum_submit() {
    let agent = match std::env::var("QUORUM_AGENT") {
        Ok(a) => a,
        Err(_) => {
            eprintln!("fake-agent: QUORUM_AGENT not set, skipping submit side-effect");
            return;
        }
    };
    let bin = quorum_bin_path();
    let submitted = retry_submit(
        || {
            Command::new(&bin)
                .args(["submit", "--agent", &agent])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        },
        std::thread::sleep,
    );
    if submitted {
        eprintln!("fake-agent: quorum submit succeeded for {agent}");
    } else {
        eprintln!("fake-agent: quorum submit failed after {SUBMIT_ATTEMPTS} attempts");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bare = args.iter().any(|a| a == "--bare");
    let wants_tool_use = std::env::var("FAKE_AGENT_EMIT_TOOL_USE")
        .map(|v| v == "1")
        .unwrap_or(false);
    let side_effects = std::env::var("FAKE_AGENT_SIDE_EFFECTS")
        .map(|v| v == "1")
        .unwrap_or(false);
    let delay: Option<Duration> = std::env::var("FAKE_AGENT_DELAY_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs);
    let error_result_count: u32 = std::env::var("FAKE_AGENT_ERROR_RESULT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let stdin = io::stdin();
    let mut turn: u32 = 0;
    let mut cumulative_cost: f64 = 0.0;
    let cost_per_token = 0.00001_f64;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        if line.trim().is_empty() {
            continue;
        }

        // Try to parse as JSON to validate it's a real turn
        if serde_json::from_str::<serde_json::Value>(&line).is_err() {
            continue;
        }

        if let Ok(path) = std::env::var("FAKE_AGENT_PROMPT_LOG") {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let _ = writeln!(file, "{line}");
            }
        }

        turn += 1;

        // Classifier turn: respond with valid classification JSON for every
        // `### Task #<id>` in the prompt.
        if line.contains("You are a task classifier") {
            emit_classifier_result(&line, bare);
            continue;
        }

        // Post-merge review-analytics collector prompt. If the caller opted
        // into failure mode via FAKE_AGENT_COLLECTOR_FAIL, emit is_error=true
        // so the daemon's failure path (record_failure + errlog) exercises.
        // Otherwise emit a fixed-shape findings envelope so integration tests
        // can assert the row landed and covers both endpoints.
        if line.contains("post-merge review-analytics classifier") {
            let fail = std::env::var("FAKE_AGENT_COLLECTOR_FAIL")
                .map(|v| v == "1")
                .unwrap_or(false);
            let response_override = std::env::var(if line.contains("## Response repair") {
                "FAKE_AGENT_COLLECTOR_REPAIR_RESPONSE"
            } else {
                "FAKE_AGENT_COLLECTOR_RESPONSE"
            })
            .ok();
            emit_collector_result(fail, response_override);
            continue;
        }

        let die_mid_turn = line.contains("DIE_MID_TURN");

        if turn == 1 {
            let msg = if bare {
                "Working on task... [bare]"
            } else {
                "Working on task..."
            };
            emit_assistant(msg);
        } else if line.contains("REVIEW FAILED") || line.contains("REVIEW_FAILED") {
            emit_assistant("Fixing review feedback...");
        } else {
            emit_assistant("Acknowledged");
        }

        if wants_tool_use {
            emit_tool_use("Bash", serde_json::json!({"command": "cargo test"}));
            emit_tool_use("Read", serde_json::json!({"file_path": "/src/main.rs"}));
            emit_tool_use(
                "Grep",
                serde_json::json!({"pattern": "fn main", "path": "/src"}),
            );
        }

        if die_mid_turn {
            std::process::exit(1);
        }

        if let Some(d) = delay {
            std::thread::sleep(d);
        }

        let input_tokens = 500 * turn as u64;
        let output_tokens = 200 * turn as u64;
        cumulative_cost += (input_tokens + output_tokens) as f64 * cost_per_token;

        let is_error = error_result_count > 0 && turn <= error_result_count;
        if side_effects && turn == 1 {
            run_quorum_submit();
        }
        emit_result(turn, cumulative_cost, is_error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_side_effect_retries_with_bounded_backoff() {
        let mut attempts = 0;
        let mut waits = Vec::new();
        let submitted = retry_submit(
            || {
                attempts += 1;
                attempts == SUBMIT_ATTEMPTS
            },
            |duration| waits.push(duration),
        );

        assert!(submitted);
        assert_eq!(attempts, SUBMIT_ATTEMPTS);
        assert_eq!(
            waits,
            [Duration::from_millis(250), Duration::from_millis(500)]
        );
    }
}
