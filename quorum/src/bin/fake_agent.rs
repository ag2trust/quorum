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
//!   `quorum done --agent $QUORUM_AGENT` as a real subprocess, creating a mailbox row.

use std::io::{self, BufRead, Write};
use std::process::Command;

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

fn emit_result(turn: u32, cumulative_cost: f64) {
    let input_tokens = 500 * turn as u64;
    let output_tokens = 200 * turn as u64;
    let msg = serde_json::json!({
        "type": "result",
        "result": format!("turn-{turn}-complete"),
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
        "total_cost_usd": cumulative_cost,
        "num_turns": turn,
        "duration_ms": 1000 * turn as u64,
        "is_error": false,
    });
    println!("{}", msg);
    io::stdout().flush().ok();
}

fn quorum_bin_path() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("cannot resolve own exe path");
    exe.parent().expect("exe has no parent dir").join("quorum")
}

fn run_quorum_done() {
    let agent = match std::env::var("QUORUM_AGENT") {
        Ok(a) => a,
        Err(_) => {
            eprintln!("fake-agent: QUORUM_AGENT not set, skipping done side-effect");
            return;
        }
    };
    let bin = quorum_bin_path();
    let status = Command::new(&bin)
        .args(["done", "--agent", &agent])
        .status();
    match status {
        Ok(s) if s.success() => {
            eprintln!("fake-agent: quorum done succeeded for {agent}");
        }
        Ok(s) => {
            eprintln!("fake-agent: quorum done exited {s} for {agent}");
        }
        Err(e) => {
            eprintln!("fake-agent: quorum done failed to run: {e}");
        }
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

        let input_tokens = 500 * turn as u64;
        let output_tokens = 200 * turn as u64;
        cumulative_cost += (input_tokens + output_tokens) as f64 * cost_per_token;

        emit_result(turn, cumulative_cost);

        if side_effects && turn == 1 {
            run_quorum_done();
        }
    }
}
