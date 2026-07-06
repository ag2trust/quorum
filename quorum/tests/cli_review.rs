//! Review-verdict integration tests.
//!
//! The old auto-spawn + sticky-rework review machinery was removed in the
//! lifecycle refactor (PR 3/4). Verdict routing now goes through
//! `lifecycle::transition()` via `tasks::apply_event()`. New lifecycle tests
//! live in `quorum-core/src/tasks.rs` (unit) and will be covered by end-to-end
//! CLI tests once the daemon drives the full cycle (PR 3).
