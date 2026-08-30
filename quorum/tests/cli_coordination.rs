//! Consolidated non-daemon CLI tests for agent-visible coordination state.
//!
//! Former integration targets map directly to same-named modules in this harness. For
//! example, filter one test with `cargo test --test cli_coordination cli_sync::TEST_NAME`.

mod common;

#[path = "suites/cli_control.rs"]
mod cli_control;
#[path = "suites/cli_pinned.rs"]
mod cli_pinned;
#[path = "suites/cli_presence.rs"]
mod cli_presence;
#[path = "suites/cli_sync.rs"]
mod cli_sync;
