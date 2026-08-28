//! Daemon-issued run capabilities: a per-run identity binding (run_id, task, agent, role).
//!
//! The daemon creates a capability when spawning a worker/reviewer. Submit and
//! report operations require a valid (non-revoked) capability whose derived
//! identity matches the operation. This replaces authorization by caller-supplied
//! agent name alone.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::routing_attempts::{exclusions, ValidatedFallbackAttribution};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RunCapability {
    pub run_id: String,
    pub task_id: i64,
    pub agent: String,
    pub role: String,
    pub created_at: i64,
    pub revoked_at: Option<i64>,
    /// Exact fallback-run linkage. NULL is retained for legacy and ordinary
    /// managed capabilities that predate this narrower authority evidence.
    pub agent_run_id: Option<i64>,
}

/// The collaboration surface available to one authoritative managed run.
///
/// This is deliberately narrower than the task lifecycle status. A worker
/// with no daemon-owned PR is an initial worker; once an existing or published
/// PR is durably associated with the task, its collaboration phase is rework.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LiveRunPhase {
    InitialWorker,
    ReworkWorker,
    Reviewer,
}

/// Identity and target derived from daemon-owned state for one live run.
///
/// Callers supply only the capability and the role required by the operation.
/// Task, agent, PR, review revision, and phase are never request authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveRunContext {
    pub run_id: String,
    pub agent_run_id: i64,
    pub task_id: i64,
    pub agent: String,
    pub role: String,
    pub pr: Option<i64>,
    pub review_revision: Option<String>,
    pub phase: LiveRunPhase,
}

#[derive(Debug)]
struct LiveAgentRun {
    id: i64,
    task_id: i64,
    agent: String,
    role: String,
    ended_at: Option<i64>,
    review_pr: Option<i64>,
    review_revision: Option<String>,
}

#[derive(Debug)]
struct AuthorityTask {
    status: String,
    assignee: Option<String>,
    reviewer: Option<String>,
    refs: Option<String>,
    continue_pr: Option<i64>,
}

fn authority_error(message: impl Into<String>) -> QuorumError {
    QuorumError::Usage(format!("run authority rejected: {}", message.into()))
}

fn task_pr(task: &AuthorityTask) -> Result<Option<i64>> {
    let refs_pr = match task.refs.as_deref() {
        None => None,
        Some(raw) => {
            let refs: serde_json::Value = serde_json::from_str(raw)
                .map_err(|_| authority_error("task references are invalid"))?;
            let refs = refs
                .as_object()
                .ok_or_else(|| authority_error("task references are invalid"))?;
            match refs.get("pr") {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => {
                    let pr = value
                        .as_i64()
                        .filter(|pr| *pr > 0)
                        .ok_or_else(|| authority_error("task PR is invalid"))?;
                    Some(pr)
                }
            }
        }
    };
    if refs_pr.is_some() && task.continue_pr.is_some() && refs_pr != task.continue_pr {
        return Err(authority_error("task PR associations disagree"));
    }
    Ok(refs_pr.or(task.continue_pr))
}

/// Resolve authoritative context for a capability-backed managed operation.
///
/// `operation_role` is a closed authorization assertion (`worker` or
/// `reviewer`), not caller identity. The returned context is derived entirely
/// from the capability, its matching live `agent_runs` row, and current task
/// state. This function performs no writes and is suitable for use inside a
/// caller-owned transaction that later admits an operation.
pub fn resolve_live_run_context(
    conn: &Connection,
    run_id: &str,
    operation_role: &str,
) -> Result<LiveRunContext> {
    if !matches!(operation_role, "worker" | "reviewer") {
        return Err(authority_error("unknown operation role"));
    }

    let capability = conn
        .query_row(
            "SELECT run_id, task_id, agent, role, created_at, revoked_at, agent_run_id
             FROM run_capabilities WHERE run_id=?1",
            [run_id],
            |row| {
                Ok(RunCapability {
                    run_id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent: row.get(2)?,
                    role: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                    agent_run_id: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| authority_error("unknown capability"))?;
    if capability.revoked_at.is_some() {
        return Err(authority_error("capability is revoked"));
    }
    if capability.role != operation_role {
        return Err(authority_error("operation role does not match capability"));
    }

    let live_run = if let Some(agent_run_id) = capability.agent_run_id {
        // Attributed fallback runs carry an exact immutable link from the
        // capability to this one agent_runs row. Prefer it to every legacy
        // name/timestamp inference path so a recycled participant cannot
        // inherit another fallback run's authority.
        conn.query_row(
            "SELECT id,task_id,agent_name,role,ended_at,review_pr,review_head_sha
             FROM agent_runs WHERE id=?1 AND ended_at IS NULL",
            [agent_run_id],
            |row| {
                Ok(LiveAgentRun {
                    id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent: row.get(2)?,
                    role: row.get(3)?,
                    ended_at: row.get(4)?,
                    review_pr: row.get(5)?,
                    review_revision: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| authority_error("no matching live capability-bound run"))?
    } else if capability.role == "reviewer" {
        conn.query_row(
            "SELECT id,task_id,agent_name,role,ended_at,review_pr,review_head_sha
             FROM agent_runs
             WHERE review_cap_run_id=?1 AND role='reviewer' AND ended_at IS NULL",
            [&capability.run_id],
            |row| {
                Ok(LiveAgentRun {
                    id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent: row.get(2)?,
                    role: row.get(3)?,
                    ended_at: row.get(4)?,
                    review_pr: row.get(5)?,
                    review_revision: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| authority_error("no matching live reviewer run"))?
    } else {
        // Worker runs predate the reviewer-only exact launch columns. Preserve
        // every candidate after capability issuance so an ended earlier run
        // cannot be hidden by filtering directly to a later live row. Without
        // an exact worker capability foreign key, any second historical or
        // live candidate must fail closed instead of being paired by reusable
        // agent name.
        let active_capabilities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM run_capabilities
             WHERE task_id=?1 AND agent=?2 AND role='worker' AND revoked_at IS NULL",
            params![capability.task_id, capability.agent],
            |row| row.get(0),
        )?;
        if active_capabilities != 1 {
            return Err(authority_error("worker capability is ambiguous"));
        }
        let mut statement = conn.prepare(
            "SELECT id,task_id,agent_name,role,ended_at
             FROM agent_runs
             WHERE task_id=?1 AND agent_name=?2 AND role='worker'
               AND spawned_at>=?3
             ORDER BY spawned_at,id LIMIT 2",
        )?;
        let runs = statement
            .query_map(
                params![capability.task_id, capability.agent, capability.created_at],
                |row| {
                    Ok(LiveAgentRun {
                        id: row.get(0)?,
                        task_id: row.get(1)?,
                        agent: row.get(2)?,
                        role: row.get(3)?,
                        ended_at: row.get(4)?,
                        review_pr: None,
                        review_revision: None,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if runs.len() != 1 {
            return Err(authority_error("worker run history is ambiguous"));
        }
        let run = runs.into_iter().next().expect("one worker run");
        if run.ended_at.is_some() {
            return Err(authority_error("matching worker run has ended"));
        }
        run
    };

    if live_run.task_id != capability.task_id
        || live_run.agent != capability.agent
        || live_run.role != capability.role
    {
        return Err(authority_error("capability and live run do not match"));
    }

    let task = conn
        .query_row(
            "SELECT status,assignee,reviewer,refs,continue_pr FROM tasks WHERE id=?1",
            [capability.task_id],
            |row| {
                Ok(AuthorityTask {
                    status: row.get(0)?,
                    assignee: row.get(1)?,
                    reviewer: row.get(2)?,
                    refs: row.get(3)?,
                    continue_pr: row.get(4)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| authority_error("task does not exist"))?;
    let durable_pr = task_pr(&task)?;

    let (pr, review_revision, phase) = match operation_role {
        "worker" => {
            if !matches!(task.status.as_str(), "working" | "rework")
                || task.assignee.as_deref() != Some(capability.agent.as_str())
            {
                return Err(authority_error(
                    "worker does not own the current task phase",
                ));
            }
            if task.status == "rework" && durable_pr.is_none() {
                return Err(authority_error("rework task has no durable PR"));
            }
            let phase = if durable_pr.is_some() {
                LiveRunPhase::ReworkWorker
            } else {
                LiveRunPhase::InitialWorker
            };
            (durable_pr, None, phase)
        }
        "reviewer" => {
            if task.status != "in-review"
                || task.reviewer.as_deref() != Some(capability.agent.as_str())
            {
                return Err(authority_error(
                    "reviewer does not own the current task phase",
                ));
            }
            let launch_pr = live_run
                .review_pr
                .filter(|pr| *pr > 0)
                .ok_or_else(|| authority_error("reviewer launch PR is invalid"))?;
            if durable_pr != Some(launch_pr) {
                return Err(authority_error("reviewer launch PR does not match task"));
            }
            let revision = live_run
                .review_revision
                .as_deref()
                .filter(|sha| sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()))
                .ok_or_else(|| authority_error("reviewer launch revision is invalid"))?
                .to_string();
            (Some(launch_pr), Some(revision), LiveRunPhase::Reviewer)
        }
        _ => unreachable!("operation role validated above"),
    };

    Ok(LiveRunContext {
        run_id: capability.run_id,
        agent_run_id: live_run.id,
        task_id: capability.task_id,
        agent: capability.agent,
        role: capability.role,
        pr,
        review_revision,
        phase,
    })
}

/// Identity and target derived from daemon-owned state for one live planner run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlannerRunContext {
    pub run_id: String,
    pub task_id: i64,
    pub agent: String,
    pub graph_id: i64,
    pub source_revision: i64,
}

/// Resolve authoritative context for a capability-backed `SubmitPlan`
/// operation. The capability must exist, be unrevoked, and carry
/// `role='planner'`. The `task_decompositions` row for the capability's task
/// must be the live frozen-planning graph (`freeze_active=1, active=0`) whose
/// `planner_session_id` matches this run — the session id recorded by
/// `decomposition::set_frozen_phase` at spawn. This function performs no
/// writes and is suitable for use inside a caller-owned transaction that
/// later admits an operation.
pub fn resolve_planner_context(conn: &Connection, run_id: &str) -> Result<PlannerRunContext> {
    let capability = conn
        .query_row(
            "SELECT run_id, task_id, agent, role, created_at, revoked_at, agent_run_id
             FROM run_capabilities WHERE run_id=?1",
            [run_id],
            |row| {
                Ok(RunCapability {
                    run_id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent: row.get(2)?,
                    role: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                    agent_run_id: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| authority_error("unknown capability"))?;
    if capability.revoked_at.is_some() {
        return Err(authority_error("capability is revoked"));
    }
    if capability.role != "planner" {
        return Err(authority_error("operation role does not match capability"));
    }

    let graph = conn
        .query_row(
            "SELECT id, planned_source_revision FROM task_decompositions
             WHERE source_task_id=?1 AND freeze_active=1 AND active=0 AND planner_session_id=?2",
            params![capability.task_id, capability.run_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .ok_or_else(|| authority_error("planner run is not the live planner for its graph"))?;

    Ok(PlannerRunContext {
        run_id: capability.run_id,
        task_id: capability.task_id,
        agent: capability.agent,
        graph_id: graph.0,
        source_revision: graph.1,
    })
}

/// Issue a new run capability. The daemon calls this at spawn time.
pub fn issue(
    conn: &mut Connection,
    run_id: &str,
    task_id: i64,
    agent: &str,
    role: &str,
    now: i64,
) -> Result<()> {
    let tx = begin_immediate(conn)?;
    issue_tx(&tx, run_id, task_id, agent, role, now)?;
    tx.commit()?;
    Ok(())
}

/// Issue a run capability inside a caller-owned transaction, so issuance can
/// share one serialization point with the daemon-owned state that authorizes
/// it (see `decomposition::issue_planner_run`).
pub fn issue_tx(
    tx: &Transaction<'_>,
    run_id: &str,
    task_id: i64,
    agent: &str,
    role: &str,
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO run_capabilities (run_id, task_id, agent, role, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![run_id, task_id, agent, role, now],
    )?;
    Ok(())
}

/// Issue and bind a capability for one already-attributed fallback run.
///
/// The capability is not merely named after the same agent: its immutable
/// `agent_run_id` links to the exact `agent_runs` row that re-proves the
/// fallback token's task, responsibility, role, PR-stage, assignment, and
/// configured route. A clean `None` means the row, token, run id, or existing
/// binding did not agree; no capability is written in that case.
pub fn issue_attributed_alternate(
    conn: &mut Connection,
    token: &ValidatedFallbackAttribution,
    agent_run_id: i64,
    run_id: &str,
    agent: &str,
    now: i64,
) -> Result<Option<RunCapability>> {
    let tx = begin_immediate(conn)?;
    let capability = issue_attributed_alternate_tx(&tx, token, agent_run_id, run_id, agent, now)?;
    tx.commit()?;
    Ok(capability)
}

/// Transactional form of [`issue_attributed_alternate`].
///
/// This is deliberately composable with fallback-attempt installation: a
/// caller can create the attributed `agent_runs` evidence and issue this
/// capability under the same `BEGIN IMMEDIATE` transaction, before it makes
/// any provider call.
pub fn issue_attributed_alternate_tx(
    tx: &Transaction<'_>,
    token: &ValidatedFallbackAttribution,
    agent_run_id: i64,
    run_id: &str,
    agent: &str,
    now: i64,
) -> Result<Option<RunCapability>> {
    let Some(task_id) = token.task_id() else {
        return Err(QuorumError::Usage(
            "attributed alternate capability requires a task-scoped role assignment".into(),
        ));
    };
    if agent_run_id <= 0
        || task_id <= 0
        || run_id.is_empty()
        || run_id.contains('\0')
        || agent.is_empty()
        || agent.contains('\0')
        || now < 0
        || !matches!(token.role(), "worker" | "reviewer")
    {
        return Err(QuorumError::Usage(
            "invalid attributed alternate capability identity".into(),
        ));
    }
    let sub_role = match (token.role(), token.review_stage()) {
        ("worker", None) | ("reviewer", Some("r1")) => None,
        ("reviewer", Some("r2")) => Some("r2"),
        _ => return Ok(None),
    };

    // Expired fallback evidence cannot regain authority merely because its
    // historical run row still exists. This mirrors the attributed-run writer
    // and leaves that immutable history intact on clean rejection.
    if exclusions(tx, token.responsibility_key())?.excludes(token.profile()) {
        return Ok(None);
    }

    // Re-prove every link from the opaque token to the persisted row before
    // capability issuance. `agent_name` is intentionally exact: participant
    // identity belongs to the run evidence and cannot be changed by replaying
    // a valid route for a different fallback agent.
    let profile = token.profile();
    let run_matches: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM agent_runs AS run
             JOIN role_assignments AS assignment ON assignment.id=run.role_assignment_id
             WHERE run.id=?1
               AND run.task_id=?2
               AND run.agent_name=?3
               AND run.role=?4
               AND run.sub_role IS ?5
               AND run.ended_at IS NULL
               AND run.role_assignment_id=?6
               AND run.configured_profile_id=?7
               AND run.configured_provider=?8
               AND run.configured_model=?9
               AND run.configured_effort=?10
               AND run.provider=?8
               AND run.model=?9
               AND run.effort=?10
               AND assignment.task_id IS ?2
               AND assignment.responsibility_key=?11
               AND assignment.role=?4
               AND assignment.pr_number IS ?12
               AND assignment.review_stage IS ?13
               AND EXISTS(SELECT 1 FROM tasks WHERE id=?2)
         )",
        params![
            agent_run_id,
            task_id,
            agent,
            token.role(),
            sub_role,
            token.assignment_id(),
            profile.id,
            profile.provider,
            profile.model,
            profile.effort,
            token.responsibility_key(),
            token.pr_number(),
            token.review_stage(),
        ],
        |row| row.get(0),
    )?;
    if !run_matches {
        return Ok(None);
    }

    let existing = tx
        .query_row(
            "SELECT run_id,task_id,agent,role,created_at,revoked_at,agent_run_id
             FROM run_capabilities WHERE run_id=?1",
            [run_id],
            |row| {
                Ok(RunCapability {
                    run_id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent: row.get(2)?,
                    role: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                    agent_run_id: row.get(6)?,
                })
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        return Ok((existing.task_id == task_id
            && existing.agent == agent
            && existing.role == token.role()
            && existing.agent_run_id == Some(agent_run_id)
            && existing.revoked_at.is_none())
        .then_some(existing));
    }

    // The partial UNIQUE index on `agent_run_id` prevents a second fresh
    // capability from being attached to the same process evidence. Check it
    // before insertion so replay/concurrent mismatch is a clean negative
    // outcome, never a leaked half-issued capability.
    let already_bound: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM run_capabilities WHERE agent_run_id=?1)",
        [agent_run_id],
        |row| row.get(0),
    )?;
    if already_bound {
        return Ok(None);
    }

    tx.execute(
        "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,agent_run_id)
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![run_id, task_id, agent, token.role(), now, agent_run_id],
    )?;
    Ok(Some(RunCapability {
        run_id: run_id.to_string(),
        task_id,
        agent: agent.to_string(),
        role: token.role().to_string(),
        created_at: now,
        revoked_at: None,
        agent_run_id: Some(agent_run_id),
    }))
}

/// Validate a run capability: it must exist, not be revoked, and its derived
/// identity must match the expected agent + role (+ optionally task). Returns
/// the capability on success, or an error describing the mismatch.
///
/// Role is checked so a `worker`-role capability cannot sign a reviewer-shaped
/// submit (or vice-versa) — the module contract is "agent + role + task", not
/// agent alone. `expected_task_id` is `Option<i64>` because callers on the CLI
/// boundary (submit, report) don't always know the task id; when provided it
/// is enforced.
pub fn validate(
    conn: &Connection,
    run_id: &str,
    expected_agent: &str,
    expected_role: &str,
    expected_task_id: Option<i64>,
) -> Result<RunCapability> {
    let cap = conn
        .query_row(
            "SELECT run_id, task_id, agent, role, created_at, revoked_at, agent_run_id
             FROM run_capabilities WHERE run_id = ?1",
            params![run_id],
            |r| {
                Ok(RunCapability {
                    run_id: r.get(0)?,
                    task_id: r.get(1)?,
                    agent: r.get(2)?,
                    role: r.get(3)?,
                    created_at: r.get(4)?,
                    revoked_at: r.get(5)?,
                    agent_run_id: r.get(6)?,
                })
            },
        )
        .optional()?;
    let c = match cap {
        None => {
            return Err(QuorumError::Usage(format!(
                "unknown run_id '{run_id}' — not issued by this daemon"
            )));
        }
        Some(c) if c.revoked_at.is_some() => {
            return Err(QuorumError::Usage(format!(
                "run_id '{run_id}' has been revoked"
            )));
        }
        Some(c) => c,
    };
    if c.agent != expected_agent {
        return Err(QuorumError::Usage(format!(
            "run_id '{run_id}' agent mismatch: capability='{}', request='{expected_agent}'",
            c.agent
        )));
    }
    if c.role != expected_role {
        return Err(QuorumError::Usage(format!(
            "run_id '{run_id}' role mismatch: capability='{}', request='{expected_role}'",
            c.role
        )));
    }
    if let Some(tid) = expected_task_id {
        if c.task_id != tid {
            return Err(QuorumError::Usage(format!(
                "run_id '{run_id}' task mismatch: capability={}, request={tid}",
                c.task_id
            )));
        }
    }
    Ok(c)
}

/// Revoke a capability (e.g. on agent death or task terminal transition).
pub fn revoke(conn: &mut Connection, run_id: &str, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let revoked = revoke_tx(&tx, run_id, now)?;
    tx.commit()?;
    Ok(revoked)
}

/// Revoke one exact run capability within a caller-owned transaction.
///
/// This keeps capability revocation composable with the other authority
/// mutations a controlled shutdown must make atomically.
pub(crate) fn revoke_tx(tx: &Transaction<'_>, run_id: &str, now: i64) -> Result<bool> {
    let changed = tx.execute(
        "UPDATE run_capabilities SET revoked_at = ?1
         WHERE run_id = ?2 AND revoked_at IS NULL",
        params![now, run_id],
    )?;
    Ok(changed > 0)
}

/// Revoke a capability only when its immutable identity matches the supplied
/// managed run tuple. A matching, previously revoked capability returns
/// `false`; an unknown or mismatched run is rejected before any authority
/// mutation can be committed.
pub(crate) fn revoke_for_agent_task_tx(
    tx: &Transaction<'_>,
    run_id: &str,
    agent: &str,
    task_id: i64,
    now: i64,
) -> Result<bool> {
    let matches_run = tx
        .query_row(
            "SELECT 1 FROM run_capabilities
             WHERE run_id=?1 AND agent=?2 AND task_id=?3",
            params![run_id, agent, task_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !matches_run {
        return Err(QuorumError::Usage(format!(
            "run_id '{run_id}' does not belong to agent '{agent}' on task {task_id}"
        )));
    }
    revoke_tx(tx, run_id, now)
}

/// Check whether one exact managed-run capability is still active. Unknown or
/// mismatched identities are rejected so a recycled agent name cannot make a
/// retiring process appear to own a successor's authority.
pub(crate) fn managed_run_is_active_tx(
    tx: &Transaction<'_>,
    run_id: &str,
    agent: &str,
    task_id: i64,
    role: &str,
) -> Result<bool> {
    tx.query_row(
        "SELECT revoked_at IS NULL FROM run_capabilities
         WHERE run_id=?1 AND agent=?2 AND task_id=?3 AND role=?4",
        params![run_id, agent, task_id, role],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| {
        QuorumError::Usage(format!(
            "run_id '{run_id}' does not belong to {role} agent '{agent}' on task {task_id}"
        ))
    })
}

/// Revoke one exact managed-run capability within a caller-owned transaction.
pub(crate) fn revoke_managed_run_tx(
    tx: &Transaction<'_>,
    run_id: &str,
    agent: &str,
    task_id: i64,
    role: &str,
    now: i64,
) -> Result<bool> {
    // Validate the immutable identity even when the capability was already
    // revoked. Idempotent cleanup must not gain authority through a name match.
    let _ = managed_run_is_active_tx(tx, run_id, agent, task_id, role)?;
    revoke_tx(tx, run_id, now)
}

/// Revoke all active capabilities for an agent. Used on agent death/recovery.
pub fn revoke_all_for_agent(conn: &mut Connection, agent: &str, now: i64) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE run_capabilities SET revoked_at = ?1
         WHERE agent = ?2 AND revoked_at IS NULL",
        params![now, agent],
    )?;
    tx.commit()?;
    Ok(changed)
}

/// Look up an active (non-revoked) capability for a given agent+task+role triple.
/// Used by `apply_event` to authorize worker/reviewer actions when the name-based
/// check (assignee/reviewer) doesn't match — e.g. a replacement worker whose
/// author field was preserved for branch-naming provenance.
pub fn active_for_agent_task(
    conn: &Connection,
    agent: &str,
    task_id: i64,
    role: &str,
) -> Result<Option<RunCapability>> {
    conn.query_row(
        "SELECT run_id, task_id, agent, role, created_at, revoked_at, agent_run_id
         FROM run_capabilities
         WHERE agent = ?1 AND task_id = ?2 AND role = ?3 AND revoked_at IS NULL
         ORDER BY created_at DESC LIMIT 1",
        params![agent, task_id, role],
        |r| {
            Ok(RunCapability {
                run_id: r.get(0)?,
                task_id: r.get(1)?,
                agent: r.get(2)?,
                role: r.get(3)?,
                created_at: r.get(4)?,
                revoked_at: r.get(5)?,
                agent_run_id: r.get(6)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Look up the active (non-revoked) capability for a given agent name.
/// Returns None if no active capability exists.
pub fn active_for_agent(conn: &Connection, agent: &str) -> Result<Option<RunCapability>> {
    conn.query_row(
        "SELECT run_id, task_id, agent, role, created_at, revoked_at, agent_run_id
         FROM run_capabilities
         WHERE agent = ?1 AND revoked_at IS NULL
         ORDER BY created_at DESC LIMIT 1",
        params![agent],
        |r| {
            Ok(RunCapability {
                run_id: r.get(0)?,
                task_id: r.get(1)?,
                agent: r.get(2)?,
                role: r.get(3)?,
                created_at: r.get(4)?,
                revoked_at: r.get(5)?,
                agent_run_id: r.get(6)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    const REVIEW_SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn insert_authority_task(
        conn: &Connection,
        id: i64,
        status: &str,
        assignee: Option<&str>,
        reviewer: Option<&str>,
        pr: Option<i64>,
        continue_pr: Option<i64>,
    ) {
        let refs = pr.map(|pr| serde_json::json!({"pr": pr}).to_string());
        conn.execute(
            "INSERT INTO tasks(
                 id,title,status,priority,assignee,created_by,created_at,updated_at,
                 refs,author,reviewer,continue_pr
             ) VALUES (?1,'authority test',?2,0,?3,'test',1,1,?4,?3,?5,?6)",
            params![id, status, assignee, refs, reviewer, continue_pr],
        )
        .unwrap();
    }

    fn insert_live_worker(conn: &mut Connection, task_id: i64, agent: &str, run_id: &str) -> i64 {
        issue(conn, run_id, task_id, agent, "worker", 10).unwrap();
        crate::agent_runs::insert(conn, task_id, agent, "worker", "model", "high", "codex", 10)
            .unwrap()
    }

    fn insert_live_reviewer(
        conn: &mut Connection,
        task_id: i64,
        agent: &str,
        run_id: &str,
        pr: i64,
    ) -> i64 {
        issue(conn, run_id, task_id, agent, "reviewer", 10).unwrap();
        crate::agent_runs::insert_reviewer_with_launch(
            conn, task_id, agent, "model", "high", "codex", None, 10, None, run_id, pr, REVIEW_SHA,
        )
        .unwrap()
        .unwrap()
    }

    fn mailbox_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM mailbox", [], |row| row.get(0))
            .unwrap()
    }

    fn assert_resolver_rejects_without_mailbox_write(
        conn: &Connection,
        run_id: &str,
        operation_role: &str,
    ) {
        let before = mailbox_count(conn);
        assert!(resolve_live_run_context(conn, run_id, operation_role).is_err());
        assert_eq!(mailbox_count(conn), before);
    }

    #[test]
    fn resolve_live_worker_derives_initial_and_rework_context() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        let agent_run_id = insert_live_worker(&mut c, 1, "Worker", "worker-run");

        let initial = resolve_live_run_context(&c, "worker-run", "worker").unwrap();
        assert_eq!(
            initial,
            LiveRunContext {
                run_id: "worker-run".into(),
                agent_run_id,
                task_id: 1,
                agent: "Worker".into(),
                role: "worker".into(),
                pr: None,
                review_revision: None,
                phase: LiveRunPhase::InitialWorker,
            }
        );

        c.execute(
            "UPDATE tasks SET status='rework',refs=json_object('pr',71) WHERE id=1",
            [],
        )
        .unwrap();
        let rework = resolve_live_run_context(&c, "worker-run", "worker").unwrap();
        assert_eq!(rework.pr, Some(71));
        assert_eq!(rework.review_revision, None);
        assert_eq!(rework.phase, LiveRunPhase::ReworkWorker);
        assert_eq!(mailbox_count(&c), 0);
    }

    #[test]
    fn resolve_live_reviewer_derives_immutable_launch_context() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "in-review", None, Some("Reviewer"), Some(71), None);
        let agent_run_id = insert_live_reviewer(&mut c, 1, "Reviewer", "review-run", 71);

        let context = resolve_live_run_context(&c, "review-run", "reviewer").unwrap();
        assert_eq!(
            context,
            LiveRunContext {
                run_id: "review-run".into(),
                agent_run_id,
                task_id: 1,
                agent: "Reviewer".into(),
                role: "reviewer".into(),
                pr: Some(71),
                review_revision: Some(REVIEW_SHA.into()),
                phase: LiveRunPhase::Reviewer,
            }
        );
        assert_eq!(mailbox_count(&c), 0);
    }

    #[test]
    fn resolve_live_run_rejects_unknown_capability_without_mailbox_write() {
        let (_d, c) = open_tmp();
        assert_resolver_rejects_without_mailbox_write(&c, "unknown", "worker");
    }

    #[test]
    fn resolve_live_run_rejects_revoked_capability_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        insert_live_worker(&mut c, 1, "Worker", "revoked-run");
        revoke(&mut c, "revoked-run", 20).unwrap();
        assert_resolver_rejects_without_mailbox_write(&c, "revoked-run", "worker");
    }

    #[test]
    fn resolve_live_run_rejects_ended_agent_run_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        let agent_run_id = insert_live_worker(&mut c, 1, "Worker", "ended-run");
        c.execute(
            "UPDATE agent_runs SET ended_at=20,end_reason='completed' WHERE id=?1",
            [agent_run_id],
        )
        .unwrap();
        assert_resolver_rejects_without_mailbox_write(&c, "ended-run", "worker");
    }

    #[test]
    fn resolve_live_run_rejects_ended_worker_capability_reused_by_later_run() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        let first_run_id = insert_live_worker(&mut c, 1, "Worker", "stale-capability");
        c.execute(
            "UPDATE agent_runs SET ended_at=20,end_reason='completed' WHERE id=?1",
            [first_run_id],
        )
        .unwrap();

        // A later process can have a durable run row without its capability
        // when capability issuance fails. The still-active earlier capability
        // must not be paired with this newer live row by reusable identity.
        crate::agent_runs::insert(&c, 1, "Worker", "worker", "model", "high", "codex", 30).unwrap();

        assert_resolver_rejects_without_mailbox_write(&c, "stale-capability", "worker");
    }

    #[test]
    fn resolve_live_run_rejects_cross_run_reviewer_binding_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "in-review", None, Some("Reviewer"), Some(71), None);
        issue(&mut c, "requested-run", 1, "Reviewer", "reviewer", 10).unwrap();
        crate::agent_runs::insert_reviewer_with_launch(
            &c,
            1,
            "Reviewer",
            "model",
            "high",
            "codex",
            None,
            10,
            None,
            "different-run",
            71,
            REVIEW_SHA,
        )
        .unwrap()
        .unwrap();
        assert_resolver_rejects_without_mailbox_write(&c, "requested-run", "reviewer");
    }

    #[test]
    fn resolve_live_run_rejects_task_mismatched_run_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "in-review", None, Some("Reviewer"), Some(71), None);
        insert_authority_task(&c, 2, "in-review", None, Some("Reviewer"), Some(71), None);
        issue(&mut c, "task-run", 1, "Reviewer", "reviewer", 10).unwrap();
        crate::agent_runs::insert_reviewer_with_launch(
            &c, 2, "Reviewer", "model", "high", "codex", None, 10, None, "task-run", 71, REVIEW_SHA,
        )
        .unwrap()
        .unwrap();
        assert_resolver_rejects_without_mailbox_write(&c, "task-run", "reviewer");
    }

    #[test]
    fn resolve_live_run_rejects_wrong_operation_role_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        insert_live_worker(&mut c, 1, "Worker", "role-run");
        assert_resolver_rejects_without_mailbox_write(&c, "role-run", "reviewer");
        assert_resolver_rejects_without_mailbox_write(&c, "role-run", "admin");
    }

    #[test]
    fn resolve_live_run_rejects_wrong_task_phase_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "in-review", Some("Worker"), None, Some(71), None);
        insert_live_worker(&mut c, 1, "Worker", "phase-run");
        assert_resolver_rejects_without_mailbox_write(&c, "phase-run", "worker");
    }

    #[test]
    fn resolve_live_run_rejects_mismatched_reviewer_pr_without_mailbox_write() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "in-review", None, Some("Reviewer"), Some(72), None);
        insert_live_reviewer(&mut c, 1, "Reviewer", "pr-run", 71);
        assert_resolver_rejects_without_mailbox_write(&c, "pr-run", "reviewer");
    }

    #[test]
    fn resolve_live_worker_rejects_ambiguous_cross_run_capability() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        insert_live_worker(&mut c, 1, "Worker", "first-run");
        issue(&mut c, "second-run", 1, "Worker", "worker", 11).unwrap();
        assert_resolver_rejects_without_mailbox_write(&c, "first-run", "worker");
        assert_resolver_rejects_without_mailbox_write(&c, "second-run", "worker");
    }

    #[test]
    fn issue_and_validate_round_trips() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-001", 42, "Worker-1", "worker", 1000).unwrap();
        let cap = validate(&c, "run-001", "Worker-1", "worker", Some(42)).unwrap();
        assert_eq!(cap.task_id, 42);
        assert_eq!(cap.agent, "Worker-1");
        assert_eq!(cap.role, "worker");
        assert!(cap.revoked_at.is_none());
    }

    #[test]
    fn validate_unknown_run_id_fails() {
        let (_d, c) = open_tmp();
        let err = validate(&c, "nonexistent", "any", "worker", None).unwrap_err();
        assert!(
            format!("{err}").contains("unknown run_id"),
            "expected unknown error, got: {err}"
        );
    }

    #[test]
    fn revoke_then_validate_fails() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-002", 10, "Agent-A", "reviewer", 1000).unwrap();
        let revoked = revoke(&mut c, "run-002", 2000).unwrap();
        assert!(revoked);
        let err = validate(&c, "run-002", "Agent-A", "reviewer", None).unwrap_err();
        assert!(format!("{err}").contains("revoked"));
    }

    #[test]
    fn revoke_idempotent() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-003", 5, "X", "worker", 1000).unwrap();
        assert!(revoke(&mut c, "run-003", 2000).unwrap());
        assert!(!revoke(&mut c, "run-003", 3000).unwrap());
    }

    #[test]
    fn revoke_all_for_agent_scoped() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-a1", 1, "Alpha", "worker", 1000).unwrap();
        issue(&mut c, "run-a2", 2, "Alpha", "reviewer", 1100).unwrap();
        issue(&mut c, "run-b1", 3, "Beta", "worker", 1200).unwrap();
        let count = revoke_all_for_agent(&mut c, "Alpha", 2000).unwrap();
        assert_eq!(count, 2);
        assert!(validate(&c, "run-a1", "Alpha", "worker", None).is_err());
        assert!(validate(&c, "run-a2", "Alpha", "reviewer", None).is_err());
        assert!(validate(&c, "run-b1", "Beta", "worker", None).is_ok());
    }

    #[test]
    fn validate_rejects_wrong_agent() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-agent", 1, "Real", "worker", 1000).unwrap();
        let err = validate(&c, "run-agent", "Impostor", "worker", None).unwrap_err();
        assert!(
            format!("{err}").contains("agent mismatch"),
            "expected agent mismatch, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_wrong_role() {
        // A worker-role capability must not be usable for a reviewer-shaped submit:
        // the module contract is agent+role+task, not agent alone.
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-role", 1, "Multi", "worker", 1000).unwrap();
        let err = validate(&c, "run-role", "Multi", "reviewer", None).unwrap_err();
        assert!(
            format!("{err}").contains("role mismatch"),
            "expected role mismatch, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_wrong_task_when_provided() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-task", 42, "T", "worker", 1000).unwrap();
        // Some(mismatch) rejects; None skips the check.
        assert!(validate(&c, "run-task", "T", "worker", Some(43)).is_err());
        assert!(validate(&c, "run-task", "T", "worker", Some(42)).is_ok());
        assert!(validate(&c, "run-task", "T", "worker", None).is_ok());
    }

    #[test]
    fn active_for_agent_returns_latest() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-old", 1, "X", "worker", 1000).unwrap();
        revoke(&mut c, "run-old", 1500).unwrap();
        issue(&mut c, "run-new", 2, "X", "worker", 2000).unwrap();
        let cap = active_for_agent(&c, "X").unwrap().unwrap();
        assert_eq!(cap.run_id, "run-new");
        assert_eq!(cap.task_id, 2);
    }

    #[test]
    fn active_for_agent_none_when_all_revoked() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-x", 1, "Y", "worker", 1000).unwrap();
        revoke(&mut c, "run-x", 2000).unwrap();
        assert!(active_for_agent(&c, "Y").unwrap().is_none());
    }

    #[test]
    fn active_for_agent_task_filters_correctly() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-t1", 10, "W", "worker", 1000).unwrap();
        issue(&mut c, "run-t2", 20, "W", "worker", 1100).unwrap();
        issue(&mut c, "run-rv", 10, "W", "reviewer", 1200).unwrap();

        // Exact match
        let cap = active_for_agent_task(&c, "W", 10, "worker")
            .unwrap()
            .unwrap();
        assert_eq!(cap.run_id, "run-t1");

        // Different task
        let cap = active_for_agent_task(&c, "W", 20, "worker")
            .unwrap()
            .unwrap();
        assert_eq!(cap.run_id, "run-t2");

        // Wrong role
        assert!(
            active_for_agent_task(&c, "W", 10, "reviewer")
                .unwrap()
                .unwrap()
                .run_id
                == "run-rv"
        );

        // No match
        assert!(active_for_agent_task(&c, "W", 99, "worker")
            .unwrap()
            .is_none());
        assert!(active_for_agent_task(&c, "Z", 10, "worker")
            .unwrap()
            .is_none());

        // Revoked capability not returned
        revoke(&mut c, "run-t1", 2000).unwrap();
        assert!(active_for_agent_task(&c, "W", 10, "worker")
            .unwrap()
            .is_none());
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_graph(
        conn: &Connection,
        graph_id: i64,
        source_task_id: i64,
        active: i64,
        freeze_active: i64,
        planner_session_id: Option<&str>,
        planned_source_revision: i64,
    ) {
        conn.execute(
            "INSERT INTO task_decompositions(
                 id,source_task_id,state,active,freeze_active,
                 planner_session_id,planned_source_revision,created_at,updated_at
             ) VALUES (?1,?2,'planning',?3,?4,?5,?6,1,1)",
            params![
                graph_id,
                source_task_id,
                active,
                freeze_active,
                planner_session_id,
                planned_source_revision
            ],
        )
        .unwrap();
    }

    fn insert_frozen_graph(
        conn: &Connection,
        graph_id: i64,
        source_task_id: i64,
        planner_session_id: Option<&str>,
        planned_source_revision: i64,
    ) {
        insert_graph(
            conn,
            graph_id,
            source_task_id,
            0,
            1,
            planner_session_id,
            planned_source_revision,
        );
    }

    #[test]
    fn planner_context_resolves_live_planner() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        insert_frozen_graph(&c, 5, 1, Some("planner-run"), 2);

        let context = resolve_planner_context(&c, "planner-run").unwrap();
        assert_eq!(
            context,
            PlannerRunContext {
                run_id: "planner-run".into(),
                task_id: 1,
                agent: "Planner".into(),
                graph_id: 5,
                source_revision: 2,
            }
        );
    }

    #[test]
    fn planner_context_rejects_worker_role() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", Some("Worker"), None, None, None);
        insert_live_worker(&mut c, 1, "Worker", "worker-run");
        insert_frozen_graph(&c, 5, 1, Some("worker-run"), 2);

        let err = resolve_planner_context(&c, "worker-run").unwrap_err();
        assert!(
            err.to_string()
                .contains("operation role does not match capability"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn planner_context_rejects_stale_session() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        insert_frozen_graph(&c, 5, 1, Some("other"), 2);

        let err = resolve_planner_context(&c, "planner-run").unwrap_err();
        assert!(
            err.to_string().contains("not the live planner"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn planner_context_rejects_active_graph() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        // active=1 alongside freeze_active=1 is not a state task_decompositions'
        // invariants allow to persist (freeze_active clears once a graph activates),
        // but it isolates the resolver's `AND active=0` clause: this row keeps
        // freeze_active=1 and the matching planner_session_id, so only the `active=0`
        // guard stands between it and a match. Deleting `AND active=0` from the
        // resolver's query would make this row resolve successfully.
        insert_graph(&c, 5, 1, 1, 1, Some("planner-run"), 2);

        let err = resolve_planner_context(&c, "planner-run").unwrap_err();
        assert!(
            err.to_string().contains("not the live planner"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn planner_context_rejects_unfrozen_graph() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        // active=0 and freeze_active=0 is the graph's rest state before a freeze is
        // requested. This row keeps active=0 and the matching planner_session_id, so
        // only the `AND freeze_active=1` guard stands between it and a match. Deleting
        // `AND freeze_active=1` from the resolver's query would make this row resolve
        // successfully.
        insert_graph(&c, 5, 1, 0, 0, Some("planner-run"), 2);

        let err = resolve_planner_context(&c, "planner-run").unwrap_err();
        assert!(
            err.to_string().contains("not the live planner"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn planner_context_rejects_revoked() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        insert_frozen_graph(&c, 5, 1, Some("planner-run"), 2);
        revoke(&mut c, "planner-run", 20).unwrap();

        let err = resolve_planner_context(&c, "planner-run").unwrap_err();
        assert!(
            err.to_string().contains("revoked"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn live_run_context_rejects_planner_role() {
        let (_d, mut c) = open_tmp();
        insert_authority_task(&c, 1, "working", None, None, None, None);
        issue(&mut c, "planner-run", 1, "Planner", "planner", 10).unwrap();
        insert_frozen_graph(&c, 5, 1, Some("planner-run"), 2);

        for operation_role in ["worker", "reviewer"] {
            let err = resolve_live_run_context(&c, "planner-run", operation_role).unwrap_err();
            assert!(
                err.to_string()
                    .contains("operation role does not match capability"),
                "unexpected error for operation_role={operation_role}: {err}"
            );
        }
    }
}
