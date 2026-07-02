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
    let input_tokens = 500 * turn as u64;
    let output_tokens = 200 * turn as u64;
    let cost_per_token = 0.00001_f64;
    let total_cost = (input_tokens + output_tokens) as f64 * cost_per_token;
    let msg = serde_json::json!({
        "type": "result",
        "result": format!("turn-{turn}-complete"),
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
        "total_cost_usd": total_cost,
        "num_turns": turn,
        "duration_ms": 1000 * turn as u64,
        "is_error": false,
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bare = args.iter().any(|a| a == "--bare");

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

        if die_mid_turn {
            std::process::exit(1);
        }

        emit_result(turn);
    }
}
