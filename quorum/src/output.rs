//! JSON output. Every command emits machine-readable JSON by default; errors go to stderr
//! as `{"error": "..."}` while the process exit code carries the real signal.

use quorum_core::error::QuorumError;
use serde::Serialize;

/// Print a value as a single line of JSON to stdout.
///
/// Serialization of our own owned types (no `IntoIter`/`io::Error` payloads, no map keys that
/// fail UTF-8) cannot fail — `serde_json::to_string` only errs on map-key encoding, IO, or
/// custom `Serialize` impls that return `Err`. None of those apply here, so we treat a failure
/// as a programmer bug rather than carrying a runtime fallback for an impossible state.
pub fn emit<T: Serialize>(v: &T) {
    let s = serde_json::to_string(v).expect("serialize owned types cannot fail");
    println!("{s}");
}

/// Print an error as JSON to stderr.
pub fn emit_err(e: &QuorumError) {
    eprintln!("{}", serde_json::json!({ "error": e.to_string() }));
}
