//! quorum-core — store + domain logic for the Quorum coordination tool.
//!
//! See `docs/2026-06-23-quorum-design.md` for the design of record.

pub mod agents;
pub mod claims;
pub mod clock;
pub mod db;
pub mod errlog;
pub mod error;
pub mod sweep;
pub mod tasks;
