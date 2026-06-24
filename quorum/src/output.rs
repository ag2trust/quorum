//! JSON output. Every command emits machine-readable JSON by default; errors go to stderr
//! as `{"error": "..."}` while the process exit code carries the real signal.

use quorum_core::error::QuorumError;
use serde::Serialize;

/// Print a value as a single line of JSON to stdout.
pub fn emit<T: Serialize>(v: &T) {
    // Serialization of our own owned types cannot fail; fall back defensively.
    match serde_json::to_string(v) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!(
            "{}",
            serde_json::json!({ "error": format!("serialize: {e}") })
        ),
    }
}

/// Print an error as JSON to stderr.
pub fn emit_err(e: &QuorumError) {
    eprintln!("{}", serde_json::json!({ "error": e.to_string() }));
}
