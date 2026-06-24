//! `quorum` — daemon-less CLI for local agent coordination.
//!
//! Each invocation opens the SQLite store, performs one atomic operation, prints JSON, and
//! exits with a stable code: 0 success · 1 clean "didn't get it"/not-holder · 2 usage/bad
//! input · 3 internal/DB/migration error.

mod input;
mod output;

use quorum_core::error::Result;

/// Dispatch a single command, returning the success exit code (0, or 1 for a clean miss).
/// Replaced by clap dispatch in later phases.
fn run() -> Result<i32> {
    Ok(0)
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            output::emit_err(&e);
            std::process::exit(e.exit_code());
        }
    }
}
