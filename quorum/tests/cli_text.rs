//! Consolidated non-daemon CLI tests for persisted text input and output streams.
//!
//! Former integration targets map directly to same-named modules in this harness. For
//! example, filter one test with `cargo test --test cli_text cli_feed::TEST_NAME`.

mod common;

#[path = "suites/cli_feed.rs"]
mod cli_feed;
#[path = "suites/cli_input_safety.rs"]
mod cli_input_safety;
#[path = "suites/cli_log.rs"]
mod cli_log;
