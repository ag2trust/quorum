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
    ClaimProviderRetryRework,
    CancelSourceGraph,
    ApplyGraphEvent,
    ClaimCleanup,
    MaterializeAssessment,
    AcquireDaemonLock,
    AdmitGraph,
    RetryProviderBlocked,
    DiscardRunnerRetry,
}

impl Operation {
    pub const ALL: [Self; 11] = [
        Self::AllocateRole,
        Self::ClaimTask,
        Self::ClaimProviderRetryRework,
        Self::CancelSourceGraph,
        Self::ApplyGraphEvent,
        Self::ClaimCleanup,
        Self::MaterializeAssessment,
        Self::AcquireDaemonLock,
        Self::AdmitGraph,
        Self::RetryProviderBlocked,
        Self::DiscardRunnerRetry,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllocateRole => "allocate-role",
            Self::ClaimTask => "claim-task",
            Self::ClaimProviderRetryRework => "claim-provider-retry-rework",
            Self::CancelSourceGraph => "cancel-source-graph",
            Self::ApplyGraphEvent => "apply-graph-event",
            Self::ClaimCleanup => "claim-cleanup",
            Self::MaterializeAssessment => "materialize-assessment",
            Self::AcquireDaemonLock => "acquire-daemon-lock",
            Self::AdmitGraph => "admit-graph",
            Self::RetryProviderBlocked => "retry-provider-blocked",
            Self::DiscardRunnerRetry => "discard-runner-retry",
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
    pub barrier: Barrier,
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
pub struct ClaimProviderRetryReworkInput {
    pub db_path: PathBuf,
    pub task_id: i64,
    pub agent: String,
    pub ttl: i64,
    pub now: i64,
    pub barrier: Barrier,
}

/// The core `task-retry` path: activate a provider-blocked pending turn.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryProviderBlockedInput {
    pub db_path: PathBuf,
    pub task_id: i64,
    pub by: String,
    pub now: i64,
    pub barrier: Barrier,
}

/// Exact pending turn identity, mirroring `quorum_core::runner_state::PendingTurn`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingTurnInput {
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub prompt: String,
    pub turn_kind: String,
    pub continuation_id: Option<String>,
    pub requested: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscardRunnerRetryInput {
    pub db_path: PathBuf,
    pub task_id: i64,
    pub holder: String,
    pub expected_turn: PendingTurnInput,
    pub new_profile_id: String,
    pub now: i64,
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
    pub barrier: Barrier,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireDaemonLockInput {
    pub db_path: PathBuf,
    pub pid: i64,
    pub now: i64,
    pub stale_secs: i64,
    pub barrier: Barrier,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializeAssessmentInput {
    pub db_path: PathBuf,
    pub scope_kind: String,
    pub scope_id: i64,
    pub source_task_id: i64,
    pub artifact_ids: Vec<i64>,
    pub now: i64,
    pub barrier: Barrier,
}

/// One counted-graph admission path raced by `admit-graph`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GraphAdmission {
    /// `begin_planning` for an open source task (`target_id` = source task id).
    BeginPlanning,
    /// `reacquire_freeze` for a provider-backoff graph (`target_id` = graph id).
    ReacquireFreeze,
    /// Two-child materialization activating a planning graph (`target_id` = graph id).
    Activate,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmitGraphInput {
    pub db_path: PathBuf,
    pub admission: GraphAdmission,
    pub target_id: i64,
    pub max_active_graphs: usize,
    pub now: i64,
    pub barrier: Barrier,
}
