//! Closed wire protocol shared by the private helper and its test-side launcher.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

pub const MAX_INPUT_BYTES: usize = 16 * 1024;
#[allow(dead_code)] // Used by the launcher-side inclusion of this shared protocol.
pub const MAX_CAPTURE_BYTES: usize = 16 * 1024;
pub const MAX_PATH_BYTES: usize = 4 * 1024;
pub const MAX_TEXT_BYTES: usize = 1024;
pub const MAX_BARRIER_WAIT_MS: u64 = 30_000;

pub const EXIT_SUCCESS: i32 = 0;
pub const EXIT_NEGATIVE: i32 = 1;
pub const EXIT_USAGE: i32 = 2;
pub const EXIT_INTERNAL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    AllocateRole,
    ClaimTask,
    CancelSourceGraph,
    ApplyGraphEvent,
    ClaimCleanup,
}

impl Operation {
    pub const ALL: [Self; 5] = [
        Self::AllocateRole,
        Self::ClaimTask,
        Self::CancelSourceGraph,
        Self::ApplyGraphEvent,
        Self::ClaimCleanup,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllocateRole => "allocate-role",
            Self::ClaimTask => "claim-task",
            Self::CancelSourceGraph => "cancel-source-graph",
            Self::ApplyGraphEvent => "apply-graph-event",
            Self::ClaimCleanup => "claim-cleanup",
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Operation {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.as_str() == value)
            .ok_or(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Barrier {
    pub ready_path: PathBuf,
    pub go_path: PathBuf,
    pub timeout_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocateRoleInput {
    pub db_path: PathBuf,
    pub index: usize,
    pub same_responsibility: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimTaskInput {
    pub db_path: PathBuf,
    pub task_id: i64,
    pub agent: String,
    pub barrier: Barrier,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelSourceGraphInput {
    pub db_path: PathBuf,
    pub caller: String,
    pub source_task_id: i64,
    pub expected_revision: i64,
    pub now: i64,
    pub barrier: Barrier,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GraphEvent {
    Submit,
    Review,
    Merge,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyGraphEventInput {
    pub db_path: PathBuf,
    pub task_id: i64,
    pub event: GraphEvent,
    pub now: i64,
    pub barrier: Barrier,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimCleanupInput {
    pub db_path: PathBuf,
    pub now: i64,
}
