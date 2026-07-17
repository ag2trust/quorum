//! quorum-core — store + domain logic for the Quorum coordination tool.
//!
//! See `docs/2026-06-23-quorum-design.md` for the design of record.

pub mod activity;
pub mod agent_runs;
pub mod agents;
pub mod approvals;
pub mod branches;
pub mod claims;
pub mod classify;
pub mod clock;
pub mod complexity;
pub mod control;
pub mod daemon_lock;
pub mod db;
pub mod drift;
pub mod errlog;
pub mod error;
pub mod events;
pub mod feed;
pub mod inspect;
pub mod journal;
pub mod lifecycle;
pub mod mailbox;
pub mod perf;
pub mod pinned;
pub mod review_audits;
pub mod review_findings;
pub mod review_interpret_jobs;
pub mod stats;
pub mod sweep;
pub mod sync;
pub mod task_messages;
pub mod tasks;
