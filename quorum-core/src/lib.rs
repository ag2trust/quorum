//! quorum-core — store + domain logic for the Quorum coordination tool.
//!
//! See `docs/2026-06-23-quorum-design.md` for the design of record.

pub use rusqlite::Connection;

pub mod activity;
pub mod agent_runs;
pub mod agents;
pub mod approvals;
pub mod branches;
pub mod capabilities;
pub mod claims;
pub mod classify;
pub mod clock;
pub mod collaboration_admission;
pub mod complexity;
pub mod control;
pub mod daemon_lock;
pub mod db;
pub mod decomposition;
pub mod decomposition_cleanup;
pub mod decomposition_review;
pub mod drift;
pub mod errlog;
pub mod error;
pub mod events;
pub mod fallback_launch;
pub mod feed;
pub mod github_operations;
pub mod inspect;
pub mod journal;
pub mod lifecycle;
pub mod mailbox;
pub mod model_tiers;
pub mod perf;
pub mod pinned;
pub mod planner_submissions;
pub mod pr_targets;
pub mod provision_attempts;
pub mod review_audits;
pub mod review_findings;
pub mod review_followup_assessments;
pub mod review_followup_eligibility;
pub mod review_followup_graph_eligibility;
pub mod review_followup_reads;
pub mod review_followup_writes;
pub mod review_followups;
pub mod review_interpret_jobs;
pub mod role_assignments;
pub mod routing_attempts;
pub mod runner_state;
pub mod stats;
pub mod sweep;
pub mod sync;
pub mod task_messages;
pub mod tasks;
pub mod token_usage;
