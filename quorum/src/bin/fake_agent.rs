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
//! - Any other turn: emits assistant "Acknowledged" + result
//! - Stays alive between turns (persistent stdin-fed mode).

use std::io::{self, BufRead, Write};

fn emit_assistant(text: &str) {
    let msg = serde_json::json!({
        "type": "assistant",
        "message": {"content": text}
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn emit_result(turn: u32) {
    let msg = serde_json::json!({
        "type": "result",
        "result": format!("turn-{turn}-complete"),
        "usage": {
            "input_tokens": 500 * turn as u64,
            "output_tokens": 200 * turn as u64,
        }
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn main() {
    // Ignore all CLI flags — we only care about stdin/stdout.
    // The daemon passes --model, --effort, --session-id, etc.

    let stdin = io::stdin();
    let mut turn: u32 = 0;

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

        turn += 1;

        if turn == 1 {
            emit_assistant("Working on task...");
        } else if line.contains("REVIEW FAILED") || line.contains("REVIEW_FAILED") {
            emit_assistant("Fixing review feedback...");
        } else {
            emit_assistant("Acknowledged");
        }

        emit_result(turn);
    }
}
