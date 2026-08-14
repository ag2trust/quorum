//! Atomic storage authority for bounded, one-level task decomposition.
//!
//! Provider execution and semantic proposal validation live above this module.
//! These functions defensively enforce the invariants that must survive races:
//! one freeze, one active graph, source revision stability, complete all-at-once
//! materialization, bounded attempt ledgers, and top-down cancellation.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Transaction};
use std::collections::{HashMap, HashSet};

pub const MAX_PROPOSAL_ATTEMPTS: i64 = 3;
pub const MAX_PROVIDER_FAILURES: i64 = 3;
pub const MIN_CHILDREN: usize = 2;
pub const MAX_CHILDREN: usize = 8;
const MAX_CLEANUP_ARTIFACT_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginPlanning<'a> {
    pub source_task_id: i64,
    pub expected_revision: i64,
    pub provider: &'a str,
    pub model: &'a str,
    pub frozen_base_sha: &'a str,
    pub now: i64,
}

#[derive(Debug, Clone)]
pub struct BeginRoutedPlanning<'a> {
    pub source_task_id: i64,
    pub expected_revision: i64,
    pub assignment: &'a crate::role_assignments::AssignmentRequest,
    pub pool: &'a crate::role_assignments::ValidatedPool,
    pub seed: u64,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChild {
    pub local_key: String,
    pub title: String,
    pub body: String,
    pub labels: Option<String>,
    /// Complete, independently-produced classifier refs. The core rechecks the
    /// dispatch-bearing fields instead of trusting a planner validation flag.
    pub classification_refs: String,
    pub prerequisite_keys: Vec<String>,
    pub source_dependency_ids: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupIntent {
    pub task_id: i64,
    pub artifact_kind: String,
    pub artifact_ref: String,
}

/// Result of attempting the externally-authorized source cancellation path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCancellation {
    /// The task has never been a decomposition source; ordinary task cancellation applies.
    NotGraphSource,
    /// The graph was cancelled and all unfinished execution authority was revoked.
    Cancelled,
    /// The request was stale, unauthorized, or an idempotent replay.
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBlocker<'a> {
    pub task_id: i64,
    pub reviewer: &'a str,
    pub run_id: &'a str,
    pub category: &'a str,
    pub violated_boundary: &'a str,
    pub evidence: &'a [String],
    pub now: i64,
}

pub const GRAPH_BLOCKER_CATEGORY_BOUNDARY_VIOLATION: &str = "boundary-violation";

/// Operator-selected authority for adopting one already-merged continuation
/// as the delivery of a failed generated child. The IDs are deliberately
/// exact inputs: no title, label, note, or shared-PR discovery is permitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitRecoveryAdoption<'a> {
    pub original_child_id: i64,
    pub recovery_task_id: i64,
    pub authorized_by: &'a str,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryDelivery {
    graph_id: i64,
    decomposition_source_id: i64,
    source_revision: i64,
    pr_number: i64,
    merged_head_sha: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartupReconcileResult {
    pub healthy: usize,
    pub reset: usize,
    pub held: usize,
}

/// Reconcile durable decomposition state before generic daemon recovery.
/// Provider retries created before the frozen-base reset fix may retain a SHA
/// in `draining`; clear only that impossible retry state so the guarded bind
/// can capture a fresh base. Incomplete materialized graphs with no delivery
/// evidence reset safely; once evidence exists, inconsistency is held for
/// owner intervention.
pub fn reconcile_startup_graphs(conn: &mut Connection, now: i64) -> Result<StartupReconcileResult> {
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE task_decompositions SET frozen_base_sha=NULL,updated_at=?1
         WHERE state='draining' AND active=0 AND freeze_active=1
           AND provider_failures>0 AND frozen_base_sha IS NOT NULL",
        [now],
    )?;
    tx.commit().map_err(map_sql_err)?;

    let graph_ids = {
        let mut stmt = conn.prepare(
            "SELECT id FROM task_decompositions WHERE state IN ('active','blocked') ORDER BY id",
        )?;
        let graph_ids = stmt
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        graph_ids
    };
    let mut result = StartupReconcileResult::default();
    for graph_id in graph_ids {
        let consistent: bool = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM task_decompositions d JOIN tasks source ON source.id=d.source_task_id
                WHERE d.id=?1 AND d.active=1 AND d.accepted_plan_revision IS NOT NULL
                  AND source.status='decomposed'
                  AND (SELECT count(*) FROM task_graph_members m
                       WHERE m.graph_id=d.id AND m.active=1
                         AND m.plan_revision=d.accepted_plan_revision) BETWEEN 2 AND 8
                  AND NOT EXISTS(
                      SELECT 1 FROM task_graph_members m LEFT JOIN tasks child ON child.id=m.task_id
                      WHERE m.graph_id=d.id AND m.active=1 AND
                        (m.plan_revision!=d.accepted_plan_revision OR child.id IS NULL)
                  )
            )",
            [graph_id],
            |row| row.get(0),
        )?;
        if consistent {
            result.healthy += 1;
            continue;
        }
        if recovery_reset(
            conn,
            graph_id,
            "startup found an inconsistent task graph",
            now,
        )? {
            reacquire_freeze(conn, graph_id, now)?;
            result.reset += 1;
            continue;
        }
        let tx = begin_immediate(conn)?;
        let source_id: Option<i64> = tx
            .query_row(
                "SELECT source_task_id FROM task_decompositions
                 WHERE id=?1 AND state IN ('active','blocked')",
                [graph_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(source_id) = source_id {
            tx.execute(
                "UPDATE task_decompositions SET state='held',active=0,freeze_active=0,
                 hold_code='inconsistent-graph-with-delivery-evidence',
                 hold_summary='startup found inconsistent graph after delivery evidence',updated_at=?2
                 WHERE id=?1",
                params![graph_id, now],
            )?;
            tx.execute(
                "UPDATE tasks SET status='failed',assignee=NULL,reviewer=NULL,updated_at=?2
                 WHERE id=?1 AND status!='done'",
                params![source_id, now],
            )?;
            result.held += 1;
        }
        tx.commit().map_err(map_sql_err)?;
    }
    Ok(result)
}

fn is_unique_constraint(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(f, _) if f.code == ErrorCode::ConstraintViolation)
}

/// Acquire the repository planning freeze and move one stable source revision
/// to `planning`. `Ok(None)` is the expected result of a stale/racing request.
pub fn begin_planning(conn: &mut Connection, input: &BeginPlanning<'_>) -> Result<Option<i64>> {
    let tx = begin_immediate(conn)?;
    let eligible: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL
           AND review_only=0 AND continue_pr IS NULL
           AND NOT EXISTS (SELECT 1 FROM reviewer_provision_reservations)
           AND NOT EXISTS (SELECT 1 FROM task_decompositions
                           WHERE state IN ('active','blocked') OR active=1))",
        params![input.source_task_id, input.expected_revision],
        |row| row.get(0),
    )?;
    if !eligible {
        return Ok(None);
    }
    let inserted = tx.execute(
        "INSERT INTO task_decompositions(
             source_task_id,state,active,freeze_active,planned_source_revision,
             planner_provider,planner_model,frozen_base_sha,created_at,updated_at)
         VALUES (?1,'freeze-requested',0,1,?2,?3,?4,NULL,?5,?5)",
        params![
            input.source_task_id,
            input.expected_revision,
            input.provider,
            input.model,
            input.now
        ],
    );
    if matches!(&inserted, Err(error) if is_unique_constraint(error)) {
        return Ok(None);
    }
    inserted.map_err(map_sql_err)?;
    let graph_id = tx.last_insert_rowid();
    let changed = tx.execute(
        "UPDATE tasks SET status='planning', updated_at=?3
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL
           AND review_only=0 AND continue_pr IS NULL",
        params![input.source_task_id, input.expected_revision, input.now],
    )?;
    if changed != 1 {
        return Ok(None);
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(graph_id))
}

/// Atomically win planning authority, consume one planner allocation turn, and bind the
/// resulting immutable assignment to the new aggregate. A stale/lost planning request
/// returns `Ok(None)` before touching routing state.
pub fn begin_routed_planning(
    conn: &mut Connection,
    input: &BeginRoutedPlanning<'_>,
) -> Result<Option<(i64, crate::role_assignments::RoleAssignment)>> {
    let expected_key = format!(
        "planner:task:{}:revision:{}",
        input.source_task_id, input.expected_revision
    );
    if input.assignment.role != "planner"
        || input.assignment.task_id != Some(input.source_task_id)
        || input.assignment.pr_number.is_some()
        || input.assignment.review_stage.is_some()
        || input.assignment.responsibility_key != expected_key
    {
        return Err(QuorumError::Usage(
            "planner assignment does not match source responsibility".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let eligible: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL
           AND review_only=0 AND continue_pr IS NULL
           AND NOT EXISTS (SELECT 1 FROM reviewer_provision_reservations)
           AND NOT EXISTS (SELECT 1 FROM task_decompositions
                           WHERE state IN ('active','blocked') OR active=1))",
        params![input.source_task_id, input.expected_revision],
        |row| row.get(0),
    )?;
    if !eligible {
        return Ok(None);
    }
    let assignment = crate::role_assignments::assign_or_get_tx(
        &tx,
        input.assignment,
        input.pool,
        input.seed,
        input.now,
    )?;
    let inserted = insert_routed_planning_evidence(&tx, input, &assignment);
    if matches!(&inserted, Err(QuorumError::Db(error)) if is_unique_constraint(error)) {
        return Ok(None);
    }
    inserted?;
    let graph_id = tx.last_insert_rowid();
    let changed = tx.execute(
        "UPDATE tasks SET status='planning',updated_at=?3
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL
           AND review_only=0 AND continue_pr IS NULL",
        params![input.source_task_id, input.expected_revision, input.now],
    )?;
    if changed != 1 {
        return Ok(None);
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(Some((graph_id, assignment)))
}

const ROUTED_PLANNING_INSERT: &str = "INSERT INTO task_decompositions(
        source_task_id,state,active,freeze_active,planned_source_revision,
        planner_provider,planner_model,planner_assignment_id,frozen_base_sha,
        created_at,updated_at)
    SELECT :source_task_id,'freeze-requested',0,1,:source_revision,
        :planner_provider,:planner_model,:quorum_assignment_id,NULL,:now,:now
    /* quorum-role-assignment-guard */";

fn insert_routed_planning_evidence(
    tx: &Transaction<'_>,
    input: &BeginRoutedPlanning<'_>,
    assignment: &crate::role_assignments::RoleAssignment,
) -> Result<()> {
    let responsibility = format!(
        "planner:task:{}:revision:{}",
        input.source_task_id, input.expected_revision
    );
    let context = crate::role_assignments::EvidenceAssignmentContext {
        role_assignment_id: Some(assignment.id),
        task_id: Some(input.source_task_id),
        responsibility_key: &responsibility,
        role: "planner",
        provider: &assignment.provider,
        runner: &assignment.runner,
        model: &assignment.model,
        effort: &assignment.effort,
    };
    crate::role_assignments::guarded_evidence_insert(
        tx,
        "planner aggregate",
        &context,
        ROUTED_PLANNING_INSERT,
        &[
            (":source_task_id", &input.source_task_id),
            (":source_revision", &input.expected_revision),
            (":planner_provider", &assignment.provider),
            (":planner_model", &assignment.model),
            (":now", &input.now),
        ],
    )
}

/// Link a planning aggregate to the durable routing assignment that selected its
/// executable profile. The guarded join prevents a caller from attaching another task,
/// revision, or role's assignment. Rebinding to a different assignment fails closed.
pub fn bind_planner_assignment(
    conn: &mut Connection,
    graph_id: i64,
    assignment_id: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let valid: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM task_decompositions d
             JOIN role_assignments a ON a.id=?2
             WHERE d.id=?1 AND a.role='planner'
               AND a.task_id=d.source_task_id
               AND a.responsibility_key=(
                   'planner:task:' || d.source_task_id || ':revision:' || d.planned_source_revision)
               AND a.provider=d.planner_provider AND a.model=d.planner_model
               AND (d.planner_assignment_id IS NULL OR d.planner_assignment_id=a.id)
         )",
        params![graph_id, assignment_id],
        |row| row.get(0),
    )?;
    if !valid {
        return Ok(false);
    }
    let changed = tx.execute(
        "UPDATE task_decompositions SET planner_assignment_id=?2,updated_at=?3
         WHERE id=?1 AND (planner_assignment_id IS NULL OR planner_assignment_id=?2)",
        params![graph_id, assignment_id, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed == 1)
}

pub fn bind_frozen_base_and_enter_planning(
    conn: &mut Connection,
    graph_id: i64,
    sha: &str,
    now: i64,
) -> Result<bool> {
    if !matches!(sha.len(), 40 | 64) || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(QuorumError::Usage("invalid frozen base SHA".into()));
    }
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE task_decompositions SET state='planning',frozen_base_sha=?2,updated_at=?3
         WHERE id=?1 AND state='draining' AND freeze_active=1 AND active=0
           AND frozen_base_sha IS NULL",
        params![graph_id, sha, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed == 1)
}

/// Record one bounded planning result. A valid blocker holds immediately and
/// consumes neither retry budget. Returns the new ordinal for retry attempts.
pub fn record_attempt(
    conn: &mut Connection,
    graph_id: i64,
    kind: &str,
    reason_code: &str,
    summary: &str,
    now: i64,
) -> Result<Option<i64>> {
    if summary.len() > 2048 || reason_code.is_empty() || reason_code.len() > 128 {
        return Err(QuorumError::Usage(
            "invalid bounded decomposition attempt".into(),
        ));
    }
    if !matches!(kind, "proposal" | "provider" | "blocker") {
        return Err(QuorumError::Usage(
            "invalid decomposition attempt kind".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let row: Option<(i64, i64, i64, i64)> = tx
        .query_row(
            "SELECT source_task_id,planned_source_revision,proposal_attempts,provider_failures
             FROM task_decompositions WHERE id=?1 AND active=0
               AND state NOT IN ('held','completed','cancelled')",
            [graph_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((source_id, source_revision, proposals, providers)) = row else {
        return Ok(None);
    };
    if kind == "blocker" {
        tx.execute(
            "UPDATE task_decompositions SET state='held',freeze_active=0,
                 hold_code=?2,hold_summary=?3,updated_at=?4 WHERE id=?1",
            params![graph_id, reason_code, summary, now],
        )?;
        tx.execute(
            "UPDATE tasks SET status='failed',updated_at=?2
             WHERE id=?1 AND status='planning'",
            params![source_id, now],
        )?;
        tx.execute(
            "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
                 reason_code,summary,created_at) VALUES (?1,?2,'blocker',1,?3,?4,?5)",
            params![graph_id, source_revision, reason_code, summary, now],
        )?;
        tx.commit().map_err(map_sql_err)?;
        return Ok(Some(1));
    }
    let (current, cap, column) = if kind == "proposal" {
        (proposals, MAX_PROPOSAL_ATTEMPTS, "proposal_attempts")
    } else {
        (providers, MAX_PROVIDER_FAILURES, "provider_failures")
    };
    if current >= cap {
        return Ok(None);
    }
    let ordinal = current + 1;
    tx.execute(
        "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
             reason_code,summary,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            graph_id,
            source_revision,
            kind,
            ordinal,
            reason_code,
            summary,
            now
        ],
    )?;
    let sql = format!("UPDATE task_decompositions SET {column}=?2,updated_at=?3 WHERE id=?1");
    tx.execute(&sql, params![graph_id, ordinal, now])?;
    if ordinal == cap {
        tx.execute(
            "UPDATE task_decompositions SET state='held',freeze_active=0,
                 hold_code=?2,hold_summary=?3,updated_at=?4 WHERE id=?1",
            params![graph_id, format!("{kind}-attempts-exhausted"), summary, now],
        )?;
        tx.execute(
            "UPDATE tasks SET status='failed',updated_at=?2
             WHERE id=?1 AND status='planning'",
            params![source_id, now],
        )?;
    } else if kind == "provider" {
        tx.execute(
            "UPDATE task_decompositions SET state='provider-backoff',freeze_active=0,
                 frozen_base_sha=NULL,updated_at=?2 WHERE id=?1",
            params![graph_id, now],
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(ordinal))
}

/// Reacquire the freeze before retrying a provider failure or resuming a
/// recovery-reset aggregate. The partial unique index is the repository-wide
/// race authority. `Ok(false)` is a normal loss.
pub fn reacquire_freeze(conn: &mut Connection, graph_id: i64, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE task_decompositions SET state='freeze-requested',freeze_active=1,updated_at=?2
         WHERE id=?1 AND state IN ('provider-backoff','freeze-requested')
           AND active=0 AND freeze_active=0",
        params![graph_id, now],
    );
    if matches!(&changed, Err(error) if is_unique_constraint(error)) {
        return Ok(false);
    }
    let changed = changed.map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed == 1)
}

/// Advance a freeze-owning aggregate through its daemon phases. This is a
/// guarded state write, not an orchestration policy decision.
pub fn set_frozen_phase(
    conn: &mut Connection,
    graph_id: i64,
    expected: &str,
    next: &str,
    planner_session_id: Option<&str>,
    now: i64,
) -> Result<bool> {
    const PHASES: &[&str] = &[
        "freeze-requested",
        "draining",
        "planning",
        "validating",
        "preclassifying",
    ];
    if !PHASES.contains(&expected) || !PHASES.contains(&next) {
        return Err(QuorumError::Usage("invalid frozen planning phase".into()));
    }
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE task_decompositions SET state=?3,planner_session_id=?4,updated_at=?5
         WHERE id=?1 AND state=?2 AND freeze_active=1 AND active=0",
        params![graph_id, expected, next, planner_session_id, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed == 1)
}

/// Durably accept one bounded provider proposal before leaving planning. This
/// makes validating and preclassifying restart-resumable without charging a
/// semantic rejection budget.
pub fn accept_proposal(
    conn: &mut Connection,
    graph_id: i64,
    proposal_json: &str,
    now: i64,
) -> Result<bool> {
    if proposal_json.is_empty()
        || proposal_json.len() > 65_536
        || serde_json::from_str::<serde_json::Value>(proposal_json).is_err()
    {
        return Err(QuorumError::Usage(
            "invalid bounded accepted decomposition proposal".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE task_decompositions
         SET state='validating',accepted_proposal_json=?2,updated_at=?3
         WHERE id=?1 AND state='planning' AND freeze_active=1 AND active=0",
        params![graph_id, proposal_json, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed == 1)
}

fn classification_is_small_dispatchable(raw: &str) -> bool {
    let owned = Some(raw.to_owned());
    if !crate::tasks::classification_is_complete(&owned) {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    let size = value.get("cx_size").and_then(|v| v.as_str());
    let ready = value.get("cx_ready").and_then(|v| v.as_bool());
    let duplicate = value
        .get("cx_dup_of")
        .and_then(|v| v.as_array())
        .is_some_and(|values| !values.is_empty());
    matches!(size, Some("S" | "M")) && ready == Some(true) && !duplicate
}

/// Atomically create every generated task and member. The returned IDs follow
/// proposal order. No child row can survive a failed validation or insert.
pub fn materialize_graph(
    conn: &mut Connection,
    graph_id: i64,
    expected_source_revision: i64,
    children: &[PlannedChild],
    now: i64,
) -> Result<Option<Vec<i64>>> {
    if !(MIN_CHILDREN..=MAX_CHILDREN).contains(&children.len()) {
        return Err(QuorumError::Usage(
            "a decomposition requires 2 to 8 children".into(),
        ));
    }
    let keys: HashSet<&str> = children
        .iter()
        .map(|child| child.local_key.as_str())
        .collect();
    if keys.len() != children.len()
        || children.iter().any(|child| {
            child.local_key.is_empty()
                || child.title.trim().is_empty()
                || !classification_is_small_dispatchable(&child.classification_refs)
                || child
                    .prerequisite_keys
                    .iter()
                    .any(|key| key == &child.local_key || !keys.contains(key.as_str()))
        })
    {
        return Err(QuorumError::Usage(
            "invalid or unclassified decomposition children".into(),
        ));
    }
    // Kahn traversal rejects cycles before the write transaction.
    let mut remaining: HashMap<&str, usize> = children
        .iter()
        .map(|child| (child.local_key.as_str(), child.prerequisite_keys.len()))
        .collect();
    let mut completed = HashSet::new();
    loop {
        let ready: Vec<&str> = remaining
            .iter()
            .filter(|(key, degree)| **degree == 0 && !completed.contains(**key))
            .map(|(key, _)| *key)
            .collect();
        if ready.is_empty() {
            break;
        }
        for key in ready {
            completed.insert(key);
            for child in children {
                if child.prerequisite_keys.iter().any(|dep| dep == key) {
                    *remaining.get_mut(child.local_key.as_str()).unwrap() -= 1;
                }
            }
        }
    }
    if completed.len() != children.len() {
        return Err(QuorumError::Usage(
            "decomposition graph contains a cycle".into(),
        ));
    }

    let tx = begin_immediate(conn)?;
    let aggregate: Option<(i64, i64, i64, i64, String, Option<String>)> = tx
        .query_row(
            "SELECT d.source_task_id,d.planned_source_revision,d.plan_revision,
                    t.priority,t.created_by,t.depends_on
             FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id
             WHERE d.id=?1 AND d.state IN ('planning','validating','preclassifying')
               AND d.freeze_active=1 AND d.active=0 AND t.status='planning'
               AND t.revision=?2",
            params![graph_id, expected_source_revision],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((source_id, planned_revision, plan_revision, priority, creator, source_depends_on)) =
        aggregate
    else {
        return Ok(None);
    };
    if planned_revision != expected_source_revision {
        return Ok(None);
    }
    let existing_active: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_graph_members WHERE graph_id=?1 AND active=1)",
        [graph_id],
        |r| r.get(0),
    )?;
    if existing_active {
        return Ok(None);
    }
    let source_dependencies: HashSet<i64> = source_depends_on
        .as_deref()
        .map(serde_json::from_str::<Vec<i64>>)
        .transpose()
        .map_err(|_| QuorumError::Usage("source task has invalid dependencies".into()))?
        .unwrap_or_default()
        .into_iter()
        .collect();
    for dependency in children
        .iter()
        .flat_map(|child| child.source_dependency_ids.iter())
    {
        if !source_dependencies.contains(dependency) {
            return Err(QuorumError::Usage(
                "generated task dependency is not a source dependency".into(),
            ));
        }
        let done: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1 AND status='done')",
            [dependency],
            |r| r.get(0),
        )?;
        if !done {
            return Err(QuorumError::Usage(
                "generated source dependency is not done".into(),
            ));
        }
    }

    let mut ids = Vec::with_capacity(children.len());
    let mut by_key = HashMap::new();
    for child in children {
        tx.execute(
            "INSERT INTO tasks(title,body,status,priority,labels,created_by,created_at,
                 updated_at,refs,depends_on) VALUES (?1,?2,'open',?3,?4,?5,?6,?6,?7,'[]')",
            params![
                child.title,
                child.body,
                priority,
                child.labels,
                creator,
                now,
                child.classification_refs
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (?1,?2,?3,?4,1)",
            params![graph_id, id, child.local_key, plan_revision],
        )?;
        ids.push(id);
        by_key.insert(child.local_key.as_str(), id);
    }
    for (child, id) in children.iter().zip(ids.iter()) {
        let mut dependencies = child.source_dependency_ids.clone();
        dependencies.extend(
            child
                .prerequisite_keys
                .iter()
                .map(|key| by_key[key.as_str()]),
        );
        dependencies.sort_unstable();
        dependencies.dedup();
        for dependency in &dependencies {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
                [dependency],
                |r| r.get(0),
            )?;
            if !exists || *dependency == source_id || *dependency == *id {
                return Err(QuorumError::Usage(
                    "invalid generated task dependency".into(),
                ));
            }
        }
        tx.execute(
            "UPDATE tasks SET depends_on=?2 WHERE id=?1",
            params![id, serde_json::to_string(&dependencies).unwrap()],
        )?;
    }
    let changed = tx.execute(
        "UPDATE task_decompositions SET state='active',active=1,freeze_active=0,
             accepted_plan_revision=plan_revision,accepted_proposal_json=NULL,updated_at=?2
         WHERE id=?1 AND active=0 AND freeze_active=1",
        params![graph_id, now],
    );
    if matches!(&changed, Err(error) if is_unique_constraint(error)) {
        return Ok(None);
    }
    if changed.map_err(map_sql_err)? != 1 {
        return Ok(None);
    }
    tx.execute(
        "UPDATE tasks SET status='decomposed',updated_at=?2
         WHERE id=?1 AND status='planning'",
        params![source_id, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(ids))
}

/// Cancel a source graph and revoke all unfinished generated execution
/// authority in one transaction. External cleanup happens from durable intents.
pub fn cancel_graph(
    conn: &mut Connection,
    source_task_id: i64,
    intents: &[CleanupIntent],
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let graph_id: Option<i64> = tx
        .query_row(
            "SELECT id FROM task_decompositions WHERE source_task_id=?1
             AND state NOT IN ('completed','cancelled')",
            [source_task_id],
            |r| r.get(0),
        )
        .optional()?;
    let Some(graph_id) = graph_id else {
        return Ok(false);
    };
    cancel_graph_in_tx(&tx, graph_id, source_task_id, intents, now)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

/// Cancel a materialized graph through its source task using external creator
/// or current-assignee authority. Identity, revision, artifact ownership, and
/// revocation are all checked while holding the same immediate write transaction.
pub fn cancel_source_graph(
    conn: &mut Connection,
    caller: &str,
    source_task_id: i64,
    expected_revision: Option<i64>,
    now: i64,
) -> Result<SourceCancellation> {
    if caller.contains('\0') {
        return Err(QuorumError::BadInput(
            "embedded NUL in source cancellation caller".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let source: Option<(i64, String, i64, String, Option<String>, i64)> = tx
        .query_row(
            "SELECT d.id,d.state,d.active,t.created_by,t.assignee,t.revision
             FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id
             WHERE d.source_task_id=?1",
            [source_task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((graph_id, state, active, creator, assignee, revision)) = source else {
        tx.commit()?;
        return Ok(SourceCancellation::NotGraphSource);
    };
    let Some(expected_revision) = expected_revision else {
        return Err(QuorumError::Usage(
            "decomposed source cancellation requires --expected-revision".into(),
        ));
    };
    if creator != caller && assignee.as_deref() != Some(caller)
        || revision != expected_revision
        || active != 1
        || !matches!(state.as_str(), "active" | "blocked")
    {
        tx.commit()?;
        return Ok(SourceCancellation::Rejected);
    }

    crate::agents::touch(&tx, caller, now)?;
    let intents = graph_cleanup_intents(&tx, graph_id)?;
    cancel_graph_in_tx(&tx, graph_id, source_task_id, &intents, now)?;
    crate::events::emit(
        &tx,
        "task_cancelled",
        &format!("task#{source_task_id}"),
        &format!("cancelled by {caller}"),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(SourceCancellation::Cancelled)
}

fn graph_cleanup_intents(tx: &Transaction<'_>, graph_id: i64) -> Result<Vec<CleanupIntent>> {
    let mut intents = Vec::new();
    let mut stmt = tx.prepare(
        "SELECT m.task_id,b.worktree,b.branch,p.pr_number,p.head_ref,p.head_sha,p.is_fork,
                b.provenance_sha,
                CASE WHEN json_valid(t.refs)
                     THEN json_extract(t.refs,'$.daemon_publication.branch') END,
                CASE WHEN json_valid(t.refs)
                     THEN json_extract(t.refs,'$.daemon_publication.local_sha') END,
                b.id,b.allocated_by,b.allocated_at
         FROM task_graph_members m
         JOIN tasks t ON t.id=m.task_id AND t.status!='done'
         LEFT JOIN task_branches b ON b.task_id=m.task_id
         LEFT JOIN pr_targets p ON p.task_id=m.task_id
              AND json_valid(t.refs)
              AND CAST(json_extract(t.refs,'$.pr') AS TEXT)=CAST(p.pr_number AS TEXT)
         WHERE m.graph_id=?1 ORDER BY m.task_id",
    )?;
    let rows = stmt.query_map([graph_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<i64>>(12)?,
        ))
    })?;
    for row in rows {
        let (
            task_id,
            worktree,
            branch,
            pr,
            head_ref,
            head_sha,
            is_fork,
            provenance_sha,
            publication_branch,
            publication_sha,
            allocation_id,
            allocated_by,
            allocated_at,
        ) = row?;
        if let (Some(path), Some(name)) = (&worktree, &branch) {
            intents.push(CleanupIntent {
                task_id,
                artifact_kind: "worktree".into(),
                artifact_ref: serde_json::json!({"branch": name, "path": path}).to_string(),
            });
        }
        let definitive_sha = branch.as_ref().and_then(|name| {
            if publication_branch.as_ref() == Some(name) {
                publication_sha.as_ref()
            } else if is_fork == Some(0) && head_ref.as_ref() == Some(name) {
                head_sha.as_ref()
            } else {
                None
            }
        });
        if let (Some(name), Some(expected_sha)) = (&branch, definitive_sha) {
            intents.push(CleanupIntent {
                task_id,
                artifact_kind: "branch".into(),
                artifact_ref: serde_json::json!({
                    "expected_sha": expected_sha,
                    "name": name,
                })
                .to_string(),
            });
        } else if let (
            Some(name),
            Some(path),
            Some(provenance_sha),
            Some(allocation_id),
            Some(allocated_by),
            Some(allocated_at),
        ) = (
            &branch,
            &worktree,
            &provenance_sha,
            allocation_id,
            allocated_by,
            allocated_at,
        ) {
            intents.push(CleanupIntent {
                task_id,
                artifact_kind: "branch-discovery".into(),
                artifact_ref: serde_json::json!({
                    "allocated_at": allocated_at,
                    "allocated_by": allocated_by,
                    "allocation_id": allocation_id,
                    "name": name,
                    "path": path,
                    "provenance_sha": provenance_sha,
                })
                .to_string(),
            });
        }
        if let (Some(pr_number), Some(head_ref), Some(head_sha)) = (pr, head_ref, head_sha) {
            intents.push(CleanupIntent {
                task_id,
                artifact_kind: "proposed-change".into(),
                artifact_ref: serde_json::json!({
                    "head_ref": head_ref,
                    "head_sha": head_sha,
                    "pr_number": pr_number,
                })
                .to_string(),
            });
        }
    }
    drop(stmt);

    let mut stmt = tx.prepare(
        "SELECT j.task_id,j.agent,j.session_id,j.pid
         FROM journal j JOIN task_graph_members m ON m.task_id=j.task_id
         WHERE m.graph_id=?1 AND j.pid IS NOT NULL ORDER BY j.task_id,j.agent",
    )?;
    let rows = stmt.query_map([graph_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    for row in rows {
        let (task_id, agent, session_id, pid) = row?;
        intents.push(CleanupIntent {
            task_id,
            artifact_kind: "process".into(),
            artifact_ref: serde_json::json!({
                "agent": agent,
                "session_id": session_id,
                "pid": pid,
            })
            .to_string(),
        });
    }
    Ok(intents)
}

fn cancel_graph_in_tx(
    tx: &Transaction<'_>,
    graph_id: i64,
    source_task_id: i64,
    intents: &[CleanupIntent],
    now: i64,
) -> Result<()> {
    cancel_unfinished_members(tx, graph_id, now)?;
    for intent in intents {
        validate_cleanup_intent(intent)?;
        let member: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM task_graph_members
             WHERE graph_id=?1 AND task_id=?2)",
            params![graph_id, intent.task_id],
            |r| r.get(0),
        )?;
        if !member {
            return Err(QuorumError::Usage(
                "cleanup intent is outside the graph".into(),
            ));
        }
        tx.execute(
            "INSERT OR IGNORE INTO decomposition_cleanup(
                 graph_id,task_id,artifact_kind,artifact_ref,updated_at)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                graph_id,
                intent.task_id,
                intent.artifact_kind,
                intent.artifact_ref,
                now
            ],
        )?;
    }
    tx.execute(
        "UPDATE task_graph_members SET active=0 WHERE graph_id=?1",
        [graph_id],
    )?;
    tx.execute(
        "UPDATE task_decompositions SET state='cancelled',active=0,freeze_active=0,updated_at=?2
         WHERE id=?1",
        params![graph_id, now],
    )?;
    tx.execute(
        "UPDATE tasks SET status='cancelled',assignee=NULL,updated_at=?2
         WHERE id=?1 AND status!='done'",
        params![source_task_id, now],
    )?;
    Ok(())
}

fn validate_cleanup_intent(intent: &CleanupIntent) -> Result<()> {
    fn valid_sha(value: &str) -> bool {
        matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    }
    if intent.artifact_ref.contains('\0')
        || intent.artifact_ref.len() > MAX_CLEANUP_ARTIFACT_BYTES
        || !matches!(
            intent.artifact_kind.as_str(),
            "process" | "proposed-change" | "worktree" | "branch" | "branch-discovery"
        )
    {
        return Err(QuorumError::Usage("invalid cleanup intent".into()));
    }
    let value: serde_json::Value = serde_json::from_str(&intent.artifact_ref)
        .map_err(|_| QuorumError::Usage("invalid cleanup intent JSON".into()))?;
    let Some(object) = value.as_object() else {
        return Err(QuorumError::Usage(
            "cleanup intent must be a JSON object".into(),
        ));
    };
    let valid = match intent.artifact_kind.as_str() {
        "process" => {
            object.len() == 3
                && object.get("agent").and_then(|v| v.as_str()).is_some()
                && object.get("session_id").and_then(|v| v.as_str()).is_some()
                && object
                    .get("pid")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|v| v > 0)
        }
        "proposed-change" => {
            object.len() == 3
                && object
                    .get("pr_number")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|v| v > 0)
                && object.get("head_ref").and_then(|v| v.as_str()).is_some()
                && object.get("head_sha").and_then(|v| v.as_str()).is_some()
        }
        "worktree" => {
            object.len() == 2
                && object.get("path").and_then(|v| v.as_str()).is_some()
                && object.get("branch").and_then(|v| v.as_str()).is_some()
        }
        "branch" => {
            object.len() == 2
                && object.get("name").and_then(|v| v.as_str()).is_some()
                && object
                    .get("expected_sha")
                    .and_then(|v| v.as_str())
                    .is_some_and(valid_sha)
        }
        "branch-discovery" => {
            object.len() == 6
                && object
                    .get("allocation_id")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|v| v > 0)
                && object.get("name").and_then(|v| v.as_str()).is_some()
                && object.get("path").and_then(|v| v.as_str()).is_some()
                && object
                    .get("allocated_by")
                    .and_then(|v| v.as_str())
                    .is_some()
                && object
                    .get("allocated_at")
                    .and_then(|v| v.as_i64())
                    .is_some()
                && object
                    .get("provenance_sha")
                    .and_then(|v| v.as_str())
                    .is_some_and(valid_sha)
        }
        _ => false,
    };
    if !valid
        || object
            .values()
            .filter_map(|value| value.as_str())
            .any(|value| value.is_empty() || value.contains('\0'))
    {
        return Err(QuorumError::Usage(
            "cleanup intent has invalid fields".into(),
        ));
    }
    Ok(())
}

fn cancel_unfinished_members(tx: &Transaction<'_>, graph_id: i64, now: i64) -> Result<()> {
    tx.execute(
        "UPDATE claims SET active=0 WHERE active=1 AND target IN (
             SELECT 'task#' || task_id FROM task_graph_members WHERE graph_id=?1)",
        [graph_id],
    )?;
    tx.execute(
        "UPDATE run_capabilities SET revoked_at=?2 WHERE revoked_at IS NULL AND task_id IN (
             SELECT task_id FROM task_graph_members WHERE graph_id=?1)",
        params![graph_id, now],
    )?;
    tx.execute(
        "UPDATE tasks SET status='cancelled',assignee=NULL,updated_at=?2
         WHERE status!='done' AND id IN (
             SELECT task_id FROM task_graph_members WHERE graph_id=?1)",
        params![graph_id, now],
    )?;
    Ok(())
}

/// Record a reviewer-confirmed decomposition defect and revoke only the
/// affected review authority. The graph remains active-but-blocked so the
/// repository-wide active-graph exclusion remains authoritative.
pub fn block_graph(conn: &mut Connection, blocker: &GraphBlocker<'_>) -> Result<bool> {
    if [
        blocker.reviewer,
        blocker.run_id,
        blocker.category,
        blocker.violated_boundary,
    ]
    .iter()
    .any(|text| text.contains('\0'))
        || blocker.evidence.iter().any(|item| item.contains('\0'))
    {
        return Err(QuorumError::BadInput(
            "embedded NUL in graph-blocker input".into(),
        ));
    }
    if blocker.category != GRAPH_BLOCKER_CATEGORY_BOUNDARY_VIOLATION
        || blocker.violated_boundary.trim().is_empty()
        || blocker.violated_boundary.len() > 1024
        || blocker.evidence.is_empty()
        || blocker.evidence.len() > 8
        || blocker
            .evidence
            .iter()
            .any(|item| item.trim().is_empty() || item.len() > 1024)
    {
        return Err(QuorumError::Usage(
            "invalid bounded graph-blocker evidence".into(),
        ));
    }
    let summary = serde_json::json!({
        "affected_task": blocker.task_id,
        "violated_boundary": blocker.violated_boundary,
        "evidence": blocker.evidence,
    })
    .to_string();
    if summary.len() > 8192 {
        return Err(QuorumError::Usage(
            "graph-blocker evidence exceeds bounded storage".into(),
        ));
    }

    let tx = begin_immediate(conn)?;
    let graph: Option<(i64, i64, i64)> = tx
        .query_row(
            "SELECT d.id,d.planned_source_revision,review_run.id
             FROM task_graph_members m
             JOIN task_decompositions d ON d.id=m.graph_id
             JOIN tasks child ON child.id=m.task_id
             JOIN tasks source ON source.id=d.source_task_id
             JOIN run_capabilities capability
               ON capability.run_id=?3 AND capability.task_id=m.task_id
              AND capability.agent=?2 AND capability.role='reviewer'
              AND capability.revoked_at IS NULL
             JOIN agent_runs review_run
               ON review_run.task_id=m.task_id AND review_run.agent_name=?2
              AND review_run.role='reviewer' AND review_run.ended_at IS NULL
             WHERE m.task_id=?1 AND m.active=1 AND d.state='active' AND d.active=1
               AND source.status='decomposed' AND child.status='in-review'
               AND child.reviewer=?2
               AND capability.rowid=(
                   SELECT current_capability.rowid FROM run_capabilities current_capability
                   WHERE current_capability.task_id=m.task_id
                     AND current_capability.role='reviewer'
                   ORDER BY current_capability.created_at DESC,current_capability.rowid DESC
                   LIMIT 1
               )
               AND review_run.id=(
                   SELECT MAX(current_run.id) FROM agent_runs current_run
                   WHERE current_run.task_id=m.task_id AND current_run.role='reviewer'
               )",
            params![blocker.task_id, blocker.reviewer, blocker.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((graph_id, source_revision, agent_run_id)) = graph else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    };
    let ordinal: i64 = tx.query_row(
        "SELECT COALESCE(MAX(ordinal),0)+1 FROM decomposition_attempts
         WHERE graph_id=?1 AND source_revision=?2 AND kind='blocker'",
        params![graph_id, source_revision],
        |row| row.get(0),
    )?;
    tx.execute(
        "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
             reason_code,summary,created_at)
         VALUES (?1,?2,'blocker',?3,?4,?5,?6)",
        params![
            graph_id,
            source_revision,
            ordinal,
            blocker.category,
            summary,
            blocker.now
        ],
    )?;
    tx.execute(
        "UPDATE tasks SET status='failed',assignee=NULL,reviewer=NULL,updated_at=?2
         WHERE id=?1 AND status='in-review' AND reviewer=?3",
        params![blocker.task_id, blocker.now, blocker.reviewer],
    )?;
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
        [format!("task#{}", blocker.task_id)],
    )?;
    let revoked = tx.execute(
        "UPDATE run_capabilities SET revoked_at=?2
         WHERE run_id=?1 AND task_id=?3 AND agent=?4 AND role='reviewer'
           AND revoked_at IS NULL",
        params![
            blocker.run_id,
            blocker.now,
            blocker.task_id,
            blocker.reviewer
        ],
    )?;
    if revoked != 1 {
        return Err(QuorumError::Io(
            "reviewer capability changed during graph-blocker transaction".into(),
        ));
    }
    let closed_run = tx.execute(
        "UPDATE agent_runs SET ended_at=?2,end_reason='graph-blocker'
         WHERE id=?1 AND ended_at IS NULL",
        params![agent_run_id, blocker.now],
    )?;
    if closed_run != 1 {
        return Err(QuorumError::Io(
            "reviewer run changed during graph-blocker transaction".into(),
        ));
    }
    tx.execute(
        "UPDATE task_decompositions SET state='blocked',hold_code=?2,
             hold_summary=?3,updated_at=?4 WHERE id=?1 AND state='active' AND active=1",
        params![graph_id, blocker.category, summary, blocker.now],
    )?;
    crate::events::emit(
        &tx,
        "task_graph_blocked",
        &format!("task#{}", blocker.task_id),
        &format!("graph blocked by {}", blocker.reviewer),
        blocker.now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

/// Adopt the exact managed delivery of a continuation task for one failed
/// generated child.
///
/// This is intentionally an evidence-heavy, incident-recovery primitive.  It
/// does not infer relationships from titles, notes, labels, or a shared PR
/// alone.  The original graph membership, explicit source-task provenance,
/// daemon publication handoff, managed review, merge transition, and exact PR
/// head all have to agree inside one `BEGIN IMMEDIATE` transaction. A blocked
/// graph still accepts a managed recovery delivery; blocking stops new
/// execution authority without discarding recovery evidence. A stale,
/// incomplete, or replayed request is a clean negative.
pub fn adopt_recovery_delivery(
    conn: &mut Connection,
    original_child_id: i64,
    recovery_task_id: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let delivery: Option<RecoveryDelivery> = tx
        .query_row(
            "SELECT graph.id,graph.source_task_id,graph.planned_source_revision,
                    recovery_target.pr_number,recovery_target.head_sha
             FROM task_graph_members member
             JOIN task_decompositions graph ON graph.id=member.graph_id
             JOIN tasks source ON source.id=graph.source_task_id
             JOIN tasks original ON original.id=member.task_id
             JOIN tasks recovery ON recovery.id=?2
             JOIN pr_targets original_target ON original_target.task_id=original.id
             JOIN pr_targets recovery_target ON recovery_target.task_id=recovery.id
             JOIN r2_sampling_decisions sampling
               ON sampling.pr_number=recovery_target.pr_number
              AND sampling.head_sha=recovery_target.head_sha
              AND sampling.task_id=recovery.id
             WHERE original.id=?1 AND original.id!=recovery.id
               AND original.status='failed'
               AND member.active=1 AND member.plan_revision=graph.accepted_plan_revision
               AND graph.state IN ('active','blocked') AND graph.active=1
               AND source.status='decomposed'
               AND NOT EXISTS (
                    SELECT 1 FROM task_graph_members sibling
                    JOIN tasks sibling_task ON sibling_task.id=sibling.task_id
                    WHERE sibling.graph_id=graph.id AND sibling.active=1
                      AND sibling.task_id!=original.id
                      AND sibling_task.status!='done'
               )
               AND json_valid(original.refs)
               AND json_type(original.refs,'$.pr')='integer'
               AND json_extract(original.refs,'$.pr')=original_target.pr_number
               AND original_target.pr_number=recovery_target.pr_number
               AND original_target.head_ref!=''
               AND length(original_target.head_sha) IN (40,64)
               AND original_target.head_sha NOT GLOB '*[^0-9A-Fa-f]*'
               AND recovery.status='done' AND recovery.review_only=0
               AND recovery.continue_pr=recovery_target.pr_number
               AND json_valid(recovery.refs)
               AND json_type(recovery.refs,'$.pr')='integer'
               AND json_extract(recovery.refs,'$.pr')=recovery_target.pr_number
               AND json_type(recovery.refs,'$.source_task')='integer'
               AND json_extract(recovery.refs,'$.source_task')=original.id
               AND json_type(recovery.refs,'$.daemon_publication') IS NULL
               AND (
                    (json_type(original.refs,'$.repo') IS NULL
                     AND json_type(recovery.refs,'$.repo') IS NULL)
                    OR
                    (json_type(original.refs,'$.repo')='text'
                     AND json_type(recovery.refs,'$.repo')='text'
                     AND json_extract(original.refs,'$.repo')=
                         json_extract(recovery.refs,'$.repo'))
               )
               AND recovery_target.head_ref!=''
               AND length(recovery_target.head_sha) IN (40,64)
               AND recovery_target.head_sha NOT GLOB '*[^0-9A-Fa-f]*'
               AND EXISTS (
                    SELECT 1 FROM agent_runs worker
                    JOIN role_assignments assignment
                      ON assignment.id=worker.role_assignment_id
                    JOIN events published
                      ON published.subject='task#' || recovery.id
                     AND published.kind='task_in_review'
                     AND published.expires_at>?3
                     AND published.body='by ' || worker.agent_name
                     AND published.ts BETWEEN worker.spawned_at AND worker.ended_at
                    WHERE worker.task_id=recovery.id AND worker.role='worker'
                      AND worker.sub_role IS NULL AND worker.ended_at IS NOT NULL
                      AND worker.end_reason='completed'
                      AND assignment.task_id=recovery.id
                      AND assignment.role='worker'
                      AND assignment.pr_number IS NULL
                      AND EXISTS (
                           SELECT 1 FROM events merging
                           WHERE merging.subject=published.subject
                             AND merging.kind='task_merging'
                             AND merging.expires_at>?3
                             AND merging.seq>published.seq
                      )
               )
               AND EXISTS (
                    SELECT 1 FROM agent_runs reviewer
                    JOIN role_assignments assignment
                      ON assignment.id=reviewer.role_assignment_id
                    WHERE reviewer.task_id=recovery.id AND reviewer.role='reviewer'
                      AND reviewer.ended_at IS NOT NULL
                      AND reviewer.end_reason='verdict:approved'
                      AND reviewer.review_pr=recovery_target.pr_number
                      AND reviewer.review_head_sha=recovery_target.head_sha
                      AND assignment.task_id=recovery.id
                      AND assignment.pr_number=recovery_target.pr_number
                      AND assignment.role='reviewer'
                      AND ((sampling.required=0 AND reviewer.sub_role IS NULL
                            AND assignment.review_stage='r1')
                           OR (sampling.required=1 AND reviewer.sub_role='r2'
                            AND assignment.review_stage='r2'))
               )
               AND EXISTS (
                    SELECT 1 FROM events merging
                    JOIN events done ON done.subject=merging.subject
                                    AND done.kind='task_done'
                                    AND done.expires_at>?3
                                    AND done.seq>merging.seq
                    WHERE merging.subject='task#' || recovery.id
                      AND merging.kind='task_merging'
                      AND merging.expires_at>?3
            )",
            params![original_child_id, recovery_task_id, now],
            |row| {
                Ok(RecoveryDelivery {
                    graph_id: row.get(0)?,
                    decomposition_source_id: row.get(1)?,
                    source_revision: row.get(2)?,
                    pr_number: row.get(3)?,
                    merged_head_sha: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(delivery) = delivery else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    };

    finalize_recovery_delivery(
        &tx,
        original_child_id,
        recovery_task_id,
        &delivery,
        None,
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

/// Explicitly authorize the exact durable delivery of a completed managed
/// continuation for the final failed member of an active decomposition.
///
/// This is the operator recovery path for a known pair, not a discovery
/// mechanism and not general task equivalence. It substitutes durable daemon
/// evidence for the automatic path's short-lived events: a completed managed
/// worker must precede the persisted final PR target, an approved managed
/// reviewer must be bound to that exact target head, and daemon-owned merged
/// completion provenance must close the chain. Conflicting `source_task`
/// metadata is rejected; absent metadata grants no authority beyond this
/// explicit call. Every predicate and the final lifecycle transition are
/// serialized by one `BEGIN IMMEDIATE` transaction.
pub fn adopt_explicit_recovery_delivery(
    conn: &mut Connection,
    input: &ExplicitRecoveryAdoption<'_>,
) -> Result<bool> {
    let authorized_by = input.authorized_by.trim();
    if input.original_child_id <= 0 || input.recovery_task_id <= 0 {
        return Err(QuorumError::Usage(
            "recovery task IDs must be positive".into(),
        ));
    }
    if authorized_by.is_empty() || authorized_by.len() > 128 {
        return Err(QuorumError::Usage(
            "recovery operator identity must be 1-128 bytes".into(),
        ));
    }
    if authorized_by.contains('\0') {
        return Err(QuorumError::BadInput(
            "recovery operator identity contains NUL".into(),
        ));
    }

    let tx = begin_immediate(conn)?;
    let delivery: Option<RecoveryDelivery> = tx
        .query_row(
            "SELECT graph.id,graph.source_task_id,graph.planned_source_revision,
                    recovery_target.pr_number,recovery_target.head_sha
             FROM task_graph_members member
             JOIN task_decompositions graph ON graph.id=member.graph_id
             JOIN tasks source ON source.id=graph.source_task_id
             JOIN tasks original ON original.id=member.task_id
             JOIN tasks recovery ON recovery.id=?2
             JOIN pr_targets original_target ON original_target.task_id=original.id
             JOIN pr_targets recovery_target ON recovery_target.task_id=recovery.id
             JOIN r2_sampling_decisions sampling
               ON sampling.pr_number=recovery_target.pr_number
              AND sampling.head_sha=recovery_target.head_sha
              AND sampling.task_id=recovery.id
             WHERE original.id=?1 AND original.id!=recovery.id
               AND original.status='failed'
               AND member.active=1 AND member.plan_revision=graph.accepted_plan_revision
               AND graph.state='active' AND graph.active=1
               AND source.status='decomposed'
               AND NOT EXISTS (
                    SELECT 1 FROM task_graph_members sibling
                    JOIN tasks sibling_task ON sibling_task.id=sibling.task_id
                    WHERE sibling.graph_id=graph.id AND sibling.active=1
                      AND sibling.task_id!=original.id
                      AND sibling_task.status!='done'
               )
               AND json_valid(original.refs)
               AND json_type(original.refs,'$.pr')='integer'
               AND json_extract(original.refs,'$.pr')=original_target.pr_number
               AND original_target.pr_number=recovery_target.pr_number
               AND original_target.head_ref!=''
               AND original_target.head_ref=recovery_target.head_ref
               AND length(original_target.head_sha) IN (40,64)
               AND original_target.head_sha NOT GLOB '*[^0-9A-Fa-f]*'
               AND recovery.status='done' AND recovery.review_only=0
               AND recovery.completion_provenance=?3
               AND recovery.continue_pr=recovery_target.pr_number
               AND json_valid(recovery.refs)
               AND json_type(recovery.refs,'$.pr')='integer'
               AND json_extract(recovery.refs,'$.pr')=recovery_target.pr_number
               AND (
                    json_type(recovery.refs,'$.source_task') IS NULL
                    OR (
                        json_type(recovery.refs,'$.source_task')='integer'
                        AND json_extract(recovery.refs,'$.source_task')=original.id
                    )
               )
               AND json_type(recovery.refs,'$.daemon_publication') IS NULL
               AND (
                    (json_type(original.refs,'$.repo') IS NULL
                     AND json_type(recovery.refs,'$.repo') IS NULL)
                    OR
                    (json_type(original.refs,'$.repo')='text'
                     AND json_type(recovery.refs,'$.repo')='text'
                     AND json_extract(original.refs,'$.repo')=
                         json_extract(recovery.refs,'$.repo'))
               )
               AND recovery_target.head_ref!=''
               AND length(recovery_target.head_sha) IN (40,64)
               AND recovery_target.head_sha NOT GLOB '*[^0-9A-Fa-f]*'
               AND EXISTS (
                    SELECT 1 FROM agent_runs worker
                    JOIN role_assignments assignment
                      ON assignment.id=worker.role_assignment_id
                    WHERE worker.task_id=recovery.id AND worker.role='worker'
                      AND worker.sub_role IS NULL AND worker.ended_at IS NOT NULL
                      AND worker.end_reason='completed'
                      AND worker.ended_at<=recovery_target.resolved_at
                      AND worker.id=(
                           SELECT MAX(latest_worker.id) FROM agent_runs latest_worker
                           WHERE latest_worker.task_id=recovery.id
                             AND latest_worker.role='worker'
                             AND latest_worker.sub_role IS NULL
                      )
                      AND assignment.task_id=recovery.id
                      AND assignment.role='worker'
                      AND assignment.pr_number IS NULL
               )
               AND EXISTS (
                    SELECT 1 FROM agent_runs reviewer
                    JOIN role_assignments assignment
                      ON assignment.id=reviewer.role_assignment_id
                    WHERE reviewer.task_id=recovery.id AND reviewer.role='reviewer'
                      AND reviewer.ended_at IS NOT NULL
                      AND reviewer.end_reason='verdict:approved'
                      AND reviewer.spawned_at>=recovery_target.resolved_at
                      AND reviewer.ended_at<=recovery.updated_at
                      AND reviewer.id=(
                           SELECT MAX(latest_reviewer.id) FROM agent_runs latest_reviewer
                           WHERE latest_reviewer.task_id=recovery.id
                             AND latest_reviewer.role='reviewer'
                      )
                      AND reviewer.review_pr=recovery_target.pr_number
                      AND reviewer.review_head_sha=recovery_target.head_sha
                      AND assignment.task_id=recovery.id
                      AND assignment.pr_number=recovery_target.pr_number
                      AND assignment.role='reviewer'
                      AND ((sampling.required=0 AND reviewer.sub_role IS NULL
                            AND assignment.review_stage='r1')
                           OR (sampling.required=1 AND reviewer.sub_role='r2'
                            AND assignment.review_stage='r2'))
               )",
            params![
                input.original_child_id,
                input.recovery_task_id,
                crate::tasks::COMPLETION_PROVENANCE_MERGED
            ],
            |row| {
                Ok(RecoveryDelivery {
                    graph_id: row.get(0)?,
                    decomposition_source_id: row.get(1)?,
                    source_revision: row.get(2)?,
                    pr_number: row.get(3)?,
                    merged_head_sha: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(delivery) = delivery else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    };

    let ordinal: i64 = tx.query_row(
        "SELECT COALESCE(MAX(ordinal),0)+1 FROM decomposition_attempts
         WHERE graph_id=?1 AND source_revision=?2 AND kind='recovery'",
        params![delivery.graph_id, delivery.source_revision],
        |row| row.get(0),
    )?;
    let summary = serde_json::json!({
        "authority": "explicit-operator",
        "authorized_by": authorized_by,
        "decomposition_source": delivery.decomposition_source_id,
        "original_child": input.original_child_id,
        "recovery_task": input.recovery_task_id,
        "pr": delivery.pr_number,
        "merged_head_sha": delivery.merged_head_sha,
    })
    .to_string();
    tx.execute(
        "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
             reason_code,summary,created_at)
         VALUES (?1,?2,'recovery',?3,'explicit-delivery-adoption',?4,?5)",
        params![
            delivery.graph_id,
            delivery.source_revision,
            ordinal,
            summary,
            input.now
        ],
    )?;

    finalize_recovery_delivery(
        &tx,
        input.original_child_id,
        input.recovery_task_id,
        &delivery,
        Some(authorized_by),
        input.now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

fn finalize_recovery_delivery(
    tx: &Transaction<'_>,
    original_child_id: i64,
    recovery_task_id: i64,
    delivery: &RecoveryDelivery,
    explicit_authority: Option<&str>,
    now: i64,
) -> Result<()> {
    let mut recovery_provenance = serde_json::json!({
        "source_task": original_child_id,
        "recovery_task": recovery_task_id,
        "pr": delivery.pr_number,
        "merged_head_sha": delivery.merged_head_sha,
        "adopted_at": now,
    });
    if let Some(authorized_by) = explicit_authority {
        recovery_provenance["authority"] = serde_json::json!("explicit-operator");
        recovery_provenance["authorized_by"] = serde_json::json!(authorized_by);
        recovery_provenance["decomposition_source"] =
            serde_json::json!(delivery.decomposition_source_id);
    }
    let recovery_provenance = recovery_provenance.to_string();

    let changed = tx.execute(
        "UPDATE tasks
         SET status='done',assignee=NULL,updated_at=?2,
             completion_provenance=?3,
             refs=json_set(
                 json_remove(refs,
                     '$.daemon_parked',
                     '$.daemon_parked_reason',
                     '$.daemon_resume_status',
                     '$.daemon_publication'),
                 '$.recovery_delivery',
                 json(?4))
         WHERE id=?1 AND status='failed'",
        params![
            original_child_id,
            now,
            crate::tasks::COMPLETION_PROVENANCE_MERGED,
            recovery_provenance
        ],
    )?;
    if changed != 1 {
        return Err(QuorumError::Io(
            "recovery child changed during adoption transaction".into(),
        ));
    }
    crate::events::emit(
        tx,
        "task_done",
        &format!("task#{original_child_id}"),
        &format!(
            "adopted recovery task #{recovery_task_id} for PR #{} at {}",
            delivery.pr_number, delivery.merged_head_sha
        ),
        now,
    )?;
    complete_graph_if_final_child(tx, original_child_id, now)?;
    Ok(())
}

/// Fold graph completion into the transaction that marks a generated child
/// done. This is deliberately transaction-scoped for the final-child race.
pub(crate) fn complete_graph_if_final_child(
    tx: &Transaction<'_>,
    task_id: i64,
    now: i64,
) -> Result<bool> {
    let graph: Option<(i64, i64)> = tx
        .query_row(
            "SELECT d.id,d.source_task_id
             FROM task_graph_members m
             JOIN task_decompositions d ON d.id=m.graph_id
             WHERE m.task_id=?1 AND m.active=1 AND d.state='active' AND d.active=1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((graph_id, source_id)) = graph else {
        return Ok(false);
    };
    let unfinished: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_graph_members m
         JOIN tasks child ON child.id=m.task_id
         WHERE m.graph_id=?1 AND m.active=1 AND child.status!='done')",
        [graph_id],
        |row| row.get(0),
    )?;
    if unfinished {
        return Ok(false);
    }
    let graph_changed = tx.execute(
        "UPDATE task_decompositions SET state='completed',active=0,freeze_active=0,updated_at=?2
         WHERE id=?1 AND state='active' AND active=1",
        params![graph_id, now],
    )?;
    if graph_changed != 1 {
        return Ok(false);
    }
    let source_changed = tx.execute(
        "UPDATE tasks SET status='done',assignee=NULL,updated_at=?2
         WHERE id=?1 AND status='decomposed'",
        params![source_id, now],
    )?;
    if source_changed != 1 {
        return Err(QuorumError::Io(
            "active graph lost decomposed source completion authority".into(),
        ));
    }
    crate::events::emit(
        tx,
        "task_graph_completed",
        &format!("task#{source_id}"),
        "all generated children are done",
        now,
    )?;
    Ok(true)
}

/// Fold a terminal generated-child park into its active graph aggregate.
///
/// Callers own the surrounding `BEGIN IMMEDIATE` transaction so the child
/// park, graph blocker, event, and owner alert become visible together. A
/// staged retry/remediation keeps the graph active because the failed child
/// still has daemon-owned continuation authority.
pub(crate) fn block_graph_if_child_failed(
    conn: &Connection,
    task_id: i64,
    reason: &str,
    now: i64,
) -> Result<bool> {
    if reason.contains('\0') {
        return Err(QuorumError::BadInput(
            "embedded NUL in graph child failure reason".into(),
        ));
    }
    let graph: Option<(i64, Option<String>)> = conn
        .query_row(
            "SELECT d.id,child.refs
             FROM task_graph_members m
             JOIN task_decompositions d ON d.id=m.graph_id
             JOIN tasks child ON child.id=m.task_id
             WHERE m.task_id=?1 AND m.active=1
               AND d.state='active' AND d.active=1
               AND child.status='failed'",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((graph_id, refs)) = graph else {
        return Ok(false);
    };

    if let Some(refs) = refs {
        let refs: serde_json::Value = serde_json::from_str(&refs)
            .map_err(|error| QuorumError::Io(format!("invalid persisted refs JSON: {error}")))?;
        let runner_retry_requested = crate::runner_state::retry_requested(&refs);
        let daemon_rework_retry_requested = refs
            .get(crate::tasks::PARKED_REWORK_RETRY_REF)
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        let ci_remediation_requested = refs
            .get(crate::tasks::CI_REMEDIATION_REQUESTED_REF)
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        if runner_retry_requested || daemon_rework_retry_requested || ci_remediation_requested {
            return Ok(false);
        }
    }

    let summary = format!("generated child task #{task_id} failed: {reason}");
    let graph_changed = conn.execute(
        "UPDATE task_decompositions
         SET state='blocked',hold_code='generated-child-failed',hold_summary=?2,updated_at=?3
         WHERE id=?1 AND state='active' AND active=1",
        params![graph_id, summary, now],
    )?;
    if graph_changed != 1 {
        return Ok(false);
    }
    crate::events::emit(
        conn,
        "task_graph_blocked",
        &format!("task#{task_id}"),
        &summary,
        now,
    )?;
    crate::tasks::alert_owner_of_park(
        conn,
        task_id,
        &format!("task graph blocked after generated child failed: {reason}"),
        now,
    )?;
    Ok(true)
}

/// Fail-safe recovery reset. It is refused once any member has durable delivery
/// evidence. History stays attached to the same aggregate and plan revision.
pub fn recovery_reset(
    conn: &mut Connection,
    graph_id: i64,
    summary: &str,
    now: i64,
) -> Result<bool> {
    if summary.is_empty() || summary.len() > 2048 {
        return Err(QuorumError::Usage(
            "invalid bounded recovery summary".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let row: Option<(i64, i64, i64)> = tx
        .query_row(
            "SELECT source_task_id,planned_source_revision,plan_revision
         FROM task_decompositions WHERE id=?1 AND state NOT IN ('completed','cancelled')",
            [graph_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((source_id, source_revision, plan_revision)) = row else {
        return Ok(false);
    };
    let evidence: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM task_graph_members m JOIN tasks t ON t.id=m.task_id
           WHERE m.graph_id=?1 AND (t.status NOT IN ('open','cancelled') OR t.assignee IS NOT NULL)
           UNION ALL SELECT 1 FROM task_graph_members m JOIN agent_runs r ON r.task_id=m.task_id
           WHERE m.graph_id=?1
           UNION ALL SELECT 1 FROM task_graph_members m JOIN pr_targets p ON p.task_id=m.task_id
           WHERE m.graph_id=?1)",
        [graph_id],
        |r| r.get(0),
    )?;
    if evidence {
        return Ok(false);
    }
    cancel_unfinished_members(&tx, graph_id, now)?;
    tx.execute(
        "UPDATE task_graph_members SET active=0 WHERE graph_id=?1",
        [graph_id],
    )?;
    tx.execute(
        "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
             reason_code,summary,created_at) VALUES (?1,?2,'recovery',?3,'inconsistent-graph',?4,?5)",
        params![graph_id, source_revision, plan_revision, summary, now],
    )?;
    tx.execute(
        "UPDATE task_decompositions SET state='freeze-requested',active=0,freeze_active=0,
             accepted_plan_revision=NULL,plan_revision=plan_revision+1,updated_at=?2 WHERE id=?1",
        params![graph_id, now],
    )?;
    tx.execute(
        "UPDATE tasks SET status='planning',assignee=NULL,updated_at=?2 WHERE id=?1",
        params![source_id, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::migrate(&conn).unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('large','open','owner',1,1)",
            [],
        )
        .unwrap();
        conn
    }

    fn file_setup() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("decomposition.db")).unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('large','open','owner',1,1)",
            [],
        )
        .unwrap();
        (dir, conn)
    }

    fn begin(conn: &mut Connection) -> i64 {
        let graph = begin_planning(
            conn,
            &BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        assert!(
            set_frozen_phase(conn, graph, "freeze-requested", "preclassifying", None, 2).unwrap()
        );
        graph
    }

    fn planner_pool() -> crate::role_assignments::ValidatedPool {
        crate::role_assignments::ValidatedPool {
            pool_key: "planner".into(),
            policy_generation: "g1".into(),
            profiles: vec![crate::role_assignments::WeightedProfile {
                profile: crate::role_assignments::ModelProfile {
                    id: "planner-sol".into(),
                    provider: "codex".into(),
                    runner: "codex".into(),
                    model: "sol".into(),
                    effort: "high".into(),
                },
                percent: 100,
            }],
        }
    }

    fn planner_request() -> crate::role_assignments::AssignmentRequest {
        crate::role_assignments::AssignmentRequest {
            responsibility_key: "planner:task:1:revision:1".into(),
            task_id: Some(1),
            pr_number: None,
            role: "planner".into(),
            review_stage: None,
            complexity: None,
        }
    }

    #[test]
    fn routed_planning_binds_assignment_in_authority_transaction() {
        let (_dir, mut conn) = file_setup();
        let request = planner_request();
        let pool = planner_pool();
        let (graph, assignment) = begin_routed_planning(
            &mut conn,
            &BeginRoutedPlanning {
                source_task_id: 1,
                expected_revision: 1,
                assignment: &request,
                pool: &pool,
                seed: 7,
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        let stored: (i64, String, String) = conn
            .query_row(
                "SELECT planner_assignment_id,planner_provider,planner_model
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (assignment.id, "codex".into(), "sol".into()));
    }

    #[test]
    fn planning_authority_rejects_review_only_and_continuation_sources() {
        for (review_only, continue_pr) in [(1, None), (0, Some(529))] {
            for routed in [false, true] {
                let (_dir, mut conn) = file_setup();
                conn.execute(
                    "UPDATE tasks SET review_only=?1,continue_pr=?2 WHERE id=1",
                    params![review_only, continue_pr],
                )
                .unwrap();

                let graph = if routed {
                    let request = planner_request();
                    let pool = planner_pool();
                    begin_routed_planning(
                        &mut conn,
                        &BeginRoutedPlanning {
                            source_task_id: 1,
                            expected_revision: 1,
                            assignment: &request,
                            pool: &pool,
                            seed: 7,
                            now: 2,
                        },
                    )
                    .unwrap()
                    .map(|(graph, _)| graph)
                } else {
                    begin_planning(
                        &mut conn,
                        &BeginPlanning {
                            source_task_id: 1,
                            expected_revision: 1,
                            provider: "codex",
                            model: "sol",
                            frozen_base_sha: "abc",
                            now: 2,
                        },
                    )
                    .unwrap()
                };

                assert_eq!(graph, None);
                let state: (String, i64, i64) = conn
                    .query_row(
                        "SELECT status,
                                (SELECT count(*) FROM task_decompositions),
                                (SELECT count(*) FROM role_assignments)
                         FROM tasks WHERE id=1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
                assert_eq!(state, ("open".into(), 0, 0));
            }
        }
    }

    #[test]
    fn routed_planning_rejects_wrong_context_assignment_without_partial_mutation() {
        let (_dir, mut conn) = file_setup();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,profile_id,
                 provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (71,'collector:pr:71',71,71,'collector','profile',
                     'codex','codex','sol','high','collector','g1',1)",
            [],
        )
        .unwrap();
        let assignment = crate::role_assignments::get(&conn, 71).unwrap().unwrap();
        let request = planner_request();
        let pool = planner_pool();
        let input = BeginRoutedPlanning {
            source_task_id: 1,
            expected_revision: 1,
            assignment: &request,
            pool: &pool,
            seed: 7,
            now: 2,
        };
        let result = (|| -> Result<()> {
            let tx = begin_immediate(&mut conn)?;
            tx.execute("UPDATE tasks SET status='planning' WHERE id=1", [])?;
            insert_routed_planning_evidence(&tx, &input, &assignment)?;
            tx.commit().map_err(map_sql_err)?;
            Ok(())
        })();

        assert!(result.is_err());
        let state: (String, i64) = conn
            .query_row(
                "SELECT status,(SELECT count(*) FROM task_decompositions)
                 FROM tasks WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("open".into(), 0));
    }

    #[test]
    fn lost_planning_authority_consumes_no_assignment_or_cursor_turn() {
        let mut conn = setup();
        conn.execute("UPDATE tasks SET status='working' WHERE id=1", [])
            .unwrap();
        let request = planner_request();
        let pool = planner_pool();
        assert!(begin_routed_planning(
            &mut conn,
            &BeginRoutedPlanning {
                source_task_id: 1,
                expected_revision: 1,
                assignment: &request,
                pool: &pool,
                seed: 7,
                now: 2,
            },
        )
        .unwrap()
        .is_none());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM routing_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    fn child(key: &str, deps: &[&str]) -> PlannedChild {
        PlannedChild {
            local_key: key.into(), title: key.into(), body: format!("deliver {key}"), labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: deps.iter().map(|s| (*s).into()).collect(),
            source_dependency_ids: vec![],
        }
    }

    fn make_reviewable_graph(conn: &mut Connection) -> (i64, Vec<i64>) {
        let graph = begin(conn);
        let ids = materialize_graph(conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='in-review',reviewer='r',assignee='r' WHERE id=?1",
            [ids[0]],
        )
        .unwrap();
        (graph, ids)
    }

    fn add_reviewer_authority(
        conn: &mut Connection,
        task_id: i64,
        agent: &str,
        run_id: &str,
        role: &str,
        now: i64,
    ) -> i64 {
        let agent_run = crate::agent_runs::insert(
            conn, task_id, agent, "reviewer", "model", "high", "codex", now,
        )
        .unwrap();
        crate::capabilities::issue(conn, run_id, task_id, agent, role, now).unwrap();
        agent_run
    }

    fn graph_mutation_state(conn: &Connection, graph: i64, child: i64) -> (String, String, i64) {
        conn.query_row(
            "SELECT d.state,t.status,
                    (SELECT count(*) FROM decomposition_attempts a WHERE a.graph_id=d.id)
             FROM task_decompositions d JOIN tasks t ON t.id=?2 WHERE d.id=?1",
            params![graph, child],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
    }

    const RECOVERY_PR: i64 = 526;
    const ORIGINAL_HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RECOVERY_HEAD: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    struct RecoveryFixture {
        _dir: tempfile::TempDir,
        db_path: std::path::PathBuf,
        conn: Connection,
        graph: i64,
        original: i64,
        recovery: i64,
        dependent: i64,
        siblings: Vec<i64>,
    }

    type RecoveryEvidenceMutation = Box<dyn FnOnce(&mut RecoveryFixture)>;

    impl RecoveryFixture {
        fn new() -> Self {
            let (dir, mut conn) = file_setup();
            let db_path = dir.path().join("decomposition.db");
            let graph = begin(&mut conn);
            let children = [
                child("one", &[]),
                child("two", &[]),
                child("three", &[]),
                child("failed", &[]),
            ];
            let ids = materialize_graph(&mut conn, graph, 1, &children, 4)
                .unwrap()
                .unwrap();
            let original = ids[3];
            let siblings = ids[..3].to_vec();
            for (ordinal, sibling) in siblings.iter().enumerate() {
                conn.execute(
                    "UPDATE tasks
                     SET status='done',refs=json_object('pr',?2),
                         completion_provenance=?3
                     WHERE id=?1",
                    params![
                        sibling,
                        RECOVERY_PR - 3 + ordinal as i64,
                        crate::tasks::COMPLETION_PROVENANCE_MERGED
                    ],
                )
                .unwrap();
            }
            conn.execute(
                "UPDATE tasks SET status='failed',refs=?2 WHERE id=?1",
                params![
                    original,
                    serde_json::json!({
                        "daemon_parked": true,
                        "daemon_parked_reason": "publication failed",
                        "daemon_publication": {
                            "branch": "daemon/original",
                            "local_sha": ORIGINAL_HEAD,
                            "pr": RECOVERY_PR,
                            "stage": "intent"
                        },
                        "daemon_resume_status": "rework",
                        "pr": RECOVERY_PR
                    })
                    .to_string()
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (?1,?2,'daemon/original',?3,0,8)",
                params![original, RECOVERY_PR, ORIGINAL_HEAD],
            )
            .unwrap();

            conn.execute(
                "INSERT INTO tasks(title,status,labels,created_by,created_at,updated_at,refs,
                     review_only,continue_pr)
                 VALUES ('exact continuation','done','[\"type:recovery\"]','owner',9,40,?1,0,?2)",
                params![
                    serde_json::json!({
                        "pr": RECOVERY_PR,
                        "source_task": original
                    })
                    .to_string(),
                    RECOVERY_PR
                ],
            )
            .unwrap();
            let recovery = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (?1,?2,'daemon/original',?3,0,25)",
                params![recovery, RECOVERY_PR, RECOVERY_HEAD],
            )
            .unwrap();

            conn.execute(
                "INSERT INTO role_assignments(
                     responsibility_key,task_id,pr_number,role,review_stage,complexity,
                     profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES (?1,?2,NULL,'worker',NULL,'M','worker','codex','codex','sol','high',
                         'worker','test',9)",
                params![format!("worker:task:{recovery}:revision:1"), recovery],
            )
            .unwrap();
            let worker_assignment = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
                     role_assignment_id,spawned_at,ended_at,end_reason)
                 VALUES (?1,'worker','worker','sol','high','codex',?2,10,20,'completed')",
                params![recovery, worker_assignment],
            )
            .unwrap();
            crate::events::emit(
                &conn,
                "task_in_review",
                &format!("task#{recovery}"),
                "by worker",
                15,
            )
            .unwrap();

            conn.execute(
                "INSERT INTO role_assignments(
                     responsibility_key,task_id,pr_number,role,review_stage,complexity,
                     profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES (?1,?2,?3,'reviewer','r1','M','reviewer','codex','codex','sol','high',
                         'reviewer','test',21)",
                params![
                    format!("reviewer:task:{recovery}:r1"),
                    recovery,
                    RECOVERY_PR
                ],
            )
            .unwrap();
            let reviewer_assignment = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
                     role_assignment_id,spawned_at,ended_at,end_reason,review_cap_run_id,
                     review_pr,review_head_sha)
                 VALUES (?1,'reviewer','reviewer','sol','high','codex',?2,21,40,
                         'verdict:approved','review-cap',?3,?4)",
                params![recovery, reviewer_assignment, RECOVERY_PR, RECOVERY_HEAD],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO r2_sampling_decisions(pr_number,head_sha,task_id,required,created_at)
                 VALUES (?1,?2,?3,0,29)",
                params![RECOVERY_PR, RECOVERY_HEAD, recovery],
            )
            .unwrap();
            crate::events::emit(
                &conn,
                "task_merging",
                &format!("task#{recovery}"),
                "by reviewer",
                30,
            )
            .unwrap();
            crate::events::emit(
                &conn,
                "task_done",
                &format!("task#{recovery}"),
                "by system",
                40,
            )
            .unwrap();

            conn.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at,refs,depends_on)
                 VALUES ('dependent','open','owner',41,41,'{}','[1]')",
                [],
            )
            .unwrap();
            let dependent = conn.last_insert_rowid();

            Self {
                _dir: dir,
                db_path,
                conn,
                graph,
                original,
                recovery,
                dependent,
                siblings,
            }
        }

        fn adoption(&mut self) -> bool {
            adopt_recovery_delivery(&mut self.conn, self.original, self.recovery, 50).unwrap()
        }

        fn make_explicit_eligible(&mut self) {
            self.conn
                .execute(
                    "UPDATE tasks
                     SET completion_provenance=?2,
                         refs=json_remove(refs,'$.source_task')
                     WHERE id=?1",
                    params![self.recovery, crate::tasks::COMPLETION_PROVENANCE_MERGED],
                )
                .unwrap();
            self.conn
                .execute(
                    "UPDATE pr_targets SET resolved_at=20 WHERE task_id=?1",
                    [self.recovery],
                )
                .unwrap();
        }

        fn explicit_adoption(&mut self, now: i64) -> bool {
            adopt_explicit_recovery_delivery(
                &mut self.conn,
                &ExplicitRecoveryAdoption {
                    original_child_id: self.original,
                    recovery_task_id: self.recovery,
                    authorized_by: "operator",
                    now,
                },
            )
            .unwrap()
        }
    }

    #[test]
    fn planning_and_materialization_are_atomic_and_single_use() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        assert!(begin_planning(
            &mut conn,
            &BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 3,
            }
        )
        .unwrap()
        .is_none());
        let ids = materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a", &[]), child("b", &["a"])],
            4,
        )
        .unwrap()
        .unwrap();
        assert_eq!(ids.len(), 2);
        assert!(
            materialize_graph(&mut conn, graph, 1, &[child("c", &[]), child("d", &[])], 5)
                .unwrap()
                .is_none()
        );
        let source: String = conn
            .query_row("SELECT status FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(source, "decomposed");
        let deps: String = conn
            .query_row("SELECT depends_on FROM tasks WHERE id=?1", [ids[1]], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<i64>>(&deps).unwrap(),
            vec![ids[0]]
        );
    }

    #[test]
    fn frozen_sha_is_bound_only_at_drain_to_planning_handoff() {
        let mut conn = setup();
        let graph = begin_planning(
            &mut conn,
            &BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "must-not-be-stored",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        let initially_bound: Option<String> = conn
            .query_row(
                "SELECT frozen_base_sha FROM task_decompositions WHERE id=?1",
                [graph],
                |row| row.get(0),
            )
            .unwrap();
        assert!(initially_bound.is_none());
        set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 3).unwrap();
        let drained_head = "0123456789abcdef0123456789abcdef01234567";
        assert!(bind_frozen_base_and_enter_planning(&mut conn, graph, drained_head, 4).unwrap());
        assert!(!bind_frozen_base_and_enter_planning(&mut conn, graph, drained_head, 5).unwrap());
        let stored: (String, String) = conn
            .query_row(
                "SELECT state,frozen_base_sha FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("planning".into(), drained_head.into()));
    }

    #[test]
    fn concurrent_sources_have_one_freeze_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quorum.db");
        let conn = crate::db::open(&path).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('one','open','owner',1,1);
             INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('two','open','owner',1,1);",
        )
        .unwrap();
        drop(conn);
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (1..=2)
            .map(|source_task_id| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    begin_planning(
                        &mut conn,
                        &BeginPlanning {
                            source_task_id,
                            expected_revision: 1,
                            provider: "codex",
                            model: "sol",
                            frozen_base_sha: "abc",
                            now: 2,
                        },
                    )
                    .unwrap()
                    .is_some()
                })
            })
            .collect();
        let wins = handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>();
        assert_eq!(wins, 1);
        let conn = crate::db::open(&path).unwrap();
        let freezes: i64 = conn
            .query_row(
                "SELECT count(*) FROM task_decompositions WHERE freeze_active=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(freezes, 1);
    }

    #[test]
    fn concurrent_reviewer_reservation_and_planning_freeze_have_one_authority() {
        use std::sync::{Arc, Barrier};
        for iteration in 0..32 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("review-freeze-{iteration}.db"));
            let mut conn = crate::db::open(&path).unwrap();
            let source = crate::tasks::create(
                &mut conn, "owner", "large", None, 1, None, None, None, None, 1,
            )
            .unwrap();
            let review = crate::tasks::create(
                &mut conn, "owner", "review", None, 1, None, None, None, None, 1,
            )
            .unwrap();
            conn.execute(
                "UPDATE tasks SET status='in-review',refs=json_object(
                    'cx_est',2,'cx_size','S','cx_ready',true,
                    'cx_not_ready_reason',NULL,'cx_by','test:v2') WHERE id=?1",
                [review],
            )
            .unwrap();
            drop(conn);
            let barrier = Arc::new(Barrier::new(2));
            let reserve_path = path.clone();
            let reserve_barrier = Arc::clone(&barrier);
            let reserve = std::thread::spawn(move || {
                let mut conn = crate::db::open(&reserve_path).unwrap();
                reserve_barrier.wait();
                crate::tasks::reserve_reviewer_provision(&mut conn, review, "review-token", "r1", 2)
                    .unwrap()
            });
            let plan_path = path.clone();
            let plan_barrier = Arc::clone(&barrier);
            let plan = std::thread::spawn(move || {
                let mut conn = crate::db::open(&plan_path).unwrap();
                plan_barrier.wait();
                begin_planning(
                    &mut conn,
                    &BeginPlanning {
                        source_task_id: source,
                        expected_revision: 1,
                        provider: "codex",
                        model: "sol",
                        frozen_base_sha: "abc",
                        now: 2,
                    },
                )
                .unwrap()
                .is_some()
            });
            let reserved = reserve.join().unwrap();
            let planned = plan.join().unwrap();
            assert_ne!(reserved, planned);
            if reserved {
                let mut conn = crate::db::open(&path).unwrap();
                assert!(crate::tasks::release_reviewer_provision(
                    &mut conn,
                    review,
                    "review-token"
                )
                .unwrap());
                assert!(begin_planning(
                    &mut conn,
                    &BeginPlanning {
                        source_task_id: source,
                        expected_revision: 1,
                        provider: "codex",
                        model: "sol",
                        frozen_base_sha: "abc",
                        now: 3,
                    },
                )
                .unwrap()
                .is_some());
            }
        }
    }

    #[test]
    fn invalid_cycle_creates_nothing() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        assert!(materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a", &["b"]), child("b", &["a"])],
            4
        )
        .is_err());
        let count: i64 = conn
            .query_row("SELECT count(*) FROM task_graph_members", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn materialization_rejects_dependency_outside_source_and_rolls_back() {
        let mut conn = setup();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('unrelated','done','owner',1,1)",
            [],
        )
        .unwrap();
        let graph = begin(&mut conn);
        let mut first = child("a", &[]);
        first.source_dependency_ids = vec![2];

        assert!(materialize_graph(&mut conn, graph, 1, &[first, child("b", &[])], 4).is_err());
        let children: i64 = conn
            .query_row("SELECT count(*) FROM task_graph_members", [], |r| r.get(0))
            .unwrap();
        let tasks: i64 = conn
            .query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(children, 0);
        assert_eq!(tasks, 2);
    }

    #[test]
    fn materialization_accepts_done_source_dependency() {
        let mut conn = setup();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('prerequisite','done','owner',1,1)",
            [],
        )
        .unwrap();
        conn.execute("UPDATE tasks SET depends_on='[2]' WHERE id=1", [])
            .unwrap();
        let graph = begin(&mut conn);
        let mut first = child("a", &[]);
        first.source_dependency_ids = vec![2];

        let ids = materialize_graph(&mut conn, graph, 1, &[first, child("b", &[])], 4)
            .unwrap()
            .unwrap();
        let dependencies: String = conn
            .query_row("SELECT depends_on FROM tasks WHERE id=?1", [ids[0]], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(dependencies, "[2]");
    }

    #[test]
    fn materialization_rejects_unfinished_source_dependency() {
        let mut conn = setup();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('prerequisite','open','owner',1,1)",
            [],
        )
        .unwrap();
        conn.execute("UPDATE tasks SET depends_on='[2]' WHERE id=1", [])
            .unwrap();
        let graph = begin(&mut conn);
        let mut first = child("a", &[]);
        first.source_dependency_ids = vec![2];

        assert!(materialize_graph(&mut conn, graph, 1, &[first, child("b", &[])], 4).is_err());
        let children: i64 = conn
            .query_row("SELECT count(*) FROM task_graph_members", [], |r| r.get(0))
            .unwrap();
        assert_eq!(children, 0);
    }

    #[test]
    fn blocker_does_not_consume_retry_budgets() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        assert_eq!(
            record_attempt(
                &mut conn,
                graph,
                "blocker",
                "scope",
                "owner decision required",
                4
            )
            .unwrap(),
            Some(1)
        );
        let state: (String,i64,i64,i64) = conn.query_row(
            "SELECT state,freeze_active,proposal_attempts,provider_failures FROM task_decompositions WHERE id=?1",
            [graph], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        assert_eq!(state, ("held".into(), 0, 0, 0));
    }

    #[test]
    fn provider_failure_releases_freeze_and_third_failure_holds() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        assert_eq!(
            record_attempt(
                &mut conn,
                graph,
                "provider",
                "timeout",
                "bounded timeout",
                3
            )
            .unwrap(),
            Some(1)
        );
        let state: (String, i64) = conn
            .query_row(
                "SELECT state,freeze_active FROM task_decompositions WHERE id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("provider-backoff".into(), 0));
        assert!(reacquire_freeze(&mut conn, graph, 4).unwrap());
        assert!(
            set_frozen_phase(&mut conn, graph, "freeze-requested", "planning", None, 5).unwrap()
        );
        record_attempt(&mut conn, graph, "provider", "timeout", "second", 6).unwrap();
        assert!(reacquire_freeze(&mut conn, graph, 7).unwrap());
        set_frozen_phase(&mut conn, graph, "freeze-requested", "planning", None, 8).unwrap();
        record_attempt(&mut conn, graph, "provider", "timeout", "third", 9).unwrap();
        let held: (String, i64, String) = conn
            .query_row(
                "SELECT d.state,d.provider_failures,t.status
                 FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id WHERE d.id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(held, ("held".into(), 3, "failed".into()));
    }

    #[test]
    fn provider_retry_rebinds_a_fresh_frozen_base_after_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("provider-retry-frozen-base.db");
        {
            let conn = crate::db::open(&path).unwrap();
            conn.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('large','open','owner',1,1)",
                [],
            )
            .unwrap();
        }

        let graph = {
            let mut conn = crate::db::open(&path).unwrap();
            begin_planning(
                &mut conn,
                &BeginPlanning {
                    source_task_id: 1,
                    expected_revision: 1,
                    provider: "codex",
                    model: "sol",
                    frozen_base_sha: "ignored-until-drained",
                    now: 2,
                },
            )
            .unwrap()
            .unwrap()
        };
        let first_sha = "0123456789abcdef0123456789abcdef01234567";
        {
            let mut conn = crate::db::open(&path).unwrap();
            assert!(
                set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 3,)
                    .unwrap()
            );
            assert!(bind_frozen_base_and_enter_planning(&mut conn, graph, first_sha, 4).unwrap());
        }
        {
            let mut conn = crate::db::open(&path).unwrap();
            assert_eq!(
                record_attempt(
                    &mut conn,
                    graph,
                    "provider",
                    "timeout",
                    "planner timed out",
                    5,
                )
                .unwrap(),
                Some(1)
            );
        }
        {
            let conn = crate::db::open(&path).unwrap();
            let state: (String, i64, Option<String>, i64) = conn
                .query_row(
                    "SELECT state,freeze_active,frozen_base_sha,provider_failures
                     FROM task_decompositions WHERE id=?1",
                    [graph],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(state, ("provider-backoff".into(), 0, None, 1));
        }
        {
            let mut conn = crate::db::open(&path).unwrap();
            assert!(reacquire_freeze(&mut conn, graph, 6).unwrap());
            assert!(
                set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 7,)
                    .unwrap()
            );
            let retry_sha = "fedcba9876543210fedcba9876543210fedcba98";
            assert!(bind_frozen_base_and_enter_planning(&mut conn, graph, retry_sha, 8).unwrap());
        }
        let conn = crate::db::open(&path).unwrap();
        let rebound: (String, String, i64) = conn
            .query_row(
                "SELECT state,frozen_base_sha,provider_failures
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            rebound,
            (
                "planning".into(),
                "fedcba9876543210fedcba9876543210fedcba98".into(),
                1,
            )
        );
    }

    #[test]
    fn concurrent_provider_retry_reacquire_has_one_clean_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("provider-retry-reacquire-race.db");
        let mut conn = crate::db::open(&path).unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('large','open','owner',1,1)",
            [],
        )
        .unwrap();
        let graph = begin_planning(
            &mut conn,
            &BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "ignored-until-drained",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 3).unwrap();
        bind_frozen_base_and_enter_planning(
            &mut conn,
            graph,
            "0123456789abcdef0123456789abcdef01234567",
            4,
        )
        .unwrap();
        record_attempt(
            &mut conn,
            graph,
            "provider",
            "timeout",
            "planner timed out",
            5,
        )
        .unwrap();
        drop(conn);

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    reacquire_freeze(&mut conn, graph, 6).unwrap()
                })
            })
            .collect();
        let winners = handles
            .into_iter()
            .map(|handle| handle.join().unwrap() as usize)
            .sum::<usize>();
        assert_eq!(winners, 1);

        let conn = crate::db::open(&path).unwrap();
        let state: (String, i64, Option<String>, i64) = conn
            .query_row(
                "SELECT state,freeze_active,frozen_base_sha,provider_failures
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, ("freeze-requested".into(), 1, None, 1));
    }

    #[test]
    fn cancellation_preserves_done_member_and_revokes_unfinished() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [ids[0]])
            .unwrap();
        conn.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active) VALUES (?1,'w',4,99,1)",
            [format!("task#{}", ids[1])],
        )
        .unwrap();
        assert!(cancel_graph(&mut conn, 1, &[], 5).unwrap());
        let statuses: Vec<String> = conn
            .prepare("SELECT status FROM tasks WHERE id IN (?1,?2) ORDER BY id")
            .unwrap()
            .query_map(params![ids[0], ids[1]], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(statuses, vec!["done", "cancelled"]);
        let active: i64 = conn
            .query_row("SELECT count(*) FROM claims WHERE active=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(active, 0);
    }

    #[test]
    fn authorized_source_cancellation_derives_only_owned_unfinished_artifacts() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='done',refs='{\"pr\":41}' WHERE id=?1",
            [ids[0]],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='working',assignee='worker',refs='{\"pr\":42}' WHERE id=?1",
            [ids[1]],
        )
        .unwrap();
        for (task, branch, worktree) in [
            (ids[0], "daemon/done", "/tmp/done"),
            (ids[1], "daemon/live", "/tmp/live"),
        ] {
            conn.execute(
                "INSERT INTO task_branches(task_id,branch,worktree,allocated_by,allocated_at)
                 VALUES (?1,?2,?3,'daemon',4)",
                params![task, branch, worktree],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
             VALUES (?1,42,'daemon/live',?2,0,4)",
            params![ids[1], "a".repeat(40)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at)
             VALUES ('worker','worker',?1,'session-live','working',1234,4)",
            [ids[1]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at)
             VALUES ('done-worker','worker',?1,'session-done','working',1235,4)",
            [ids[0]],
        )
        .unwrap();

        assert_eq!(
            cancel_source_graph(&mut conn, "owner", 1, Some(1), 5).unwrap(),
            SourceCancellation::Cancelled
        );
        let intents: Vec<(i64, String, String)> = conn
            .prepare(
                "SELECT task_id,artifact_kind,artifact_ref FROM decomposition_cleanup
                 ORDER BY artifact_kind",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(intents.len(), 5);
        assert!(intents.iter().any(|intent| {
            intent.0 == ids[1]
                && intent.1 == "branch"
                && serde_json::from_str::<serde_json::Value>(&intent.2).unwrap()
                    == serde_json::json!({
                        "expected_sha":"a".repeat(40),"name":"daemon/live"
                    })
        }));
        assert!(intents.iter().any(|intent| {
            intent.0 == ids[1]
                && intent.1 == "worktree"
                && serde_json::from_str::<serde_json::Value>(&intent.2).unwrap()
                    == serde_json::json!({"branch":"daemon/live","path":"/tmp/live"})
        }));
        assert!(intents.iter().any(|intent| {
            intent.0 == ids[1]
                && intent.1 == "proposed-change"
                && serde_json::from_str::<serde_json::Value>(&intent.2).unwrap()
                    == serde_json::json!({
                        "head_ref":"daemon/live","head_sha":"a".repeat(40),"pr_number":42
                    })
        }));
        assert!(intents.iter().any(|intent| {
            intent.0 == ids[1]
                && intent.1 == "process"
                && serde_json::from_str::<serde_json::Value>(&intent.2).unwrap()["pid"] == 1234
        }));
        assert!(intents.iter().any(|intent| {
            intent.0 == ids[0]
                && intent.1 == "process"
                && serde_json::from_str::<serde_json::Value>(&intent.2).unwrap()["pid"] == 1235
        }));
        assert!(!intents
            .iter()
            .any(|intent| intent.0 == ids[0] && intent.1 != "process"));
        assert_eq!(
            intents
                .iter()
                .filter(|intent| intent.0 == ids[1] && intent.1 == "branch")
                .count(),
            1,
            "PR-backed allocation has exactly one definitive branch intent"
        );
    }

    #[test]
    fn prepublication_allocation_creates_worktree_and_discovery_intents() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        let provenance = "b".repeat(40);
        assert!(crate::branches::record_exact_allocation(
            &mut conn,
            ids[0],
            "daemon/prepublication",
            "/tmp/prepublication",
            "worker",
            &provenance,
            4,
        )
        .unwrap());
        let allocation_id: i64 = conn
            .query_row(
                "SELECT id FROM task_branches WHERE task_id=?1",
                [ids[0]],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            cancel_source_graph(&mut conn, "owner", 1, Some(1), 5).unwrap(),
            SourceCancellation::Cancelled
        );
        let intents: Vec<(String, serde_json::Value)> = conn
            .prepare(
                "SELECT artifact_kind,artifact_ref FROM decomposition_cleanup
                 WHERE task_id=?1 ORDER BY artifact_kind",
            )
            .unwrap()
            .query_map([ids[0]], |row| {
                let kind: String = row.get(0)?;
                let artifact: String = row.get(1)?;
                Ok((kind, serde_json::from_str(&artifact).unwrap()))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(
            intents,
            vec![
                (
                    "branch-discovery".into(),
                    serde_json::json!({
                        "allocated_at":4,
                        "allocated_by":"worker",
                        "allocation_id":allocation_id,
                        "name":"daemon/prepublication",
                        "path":"/tmp/prepublication",
                        "provenance_sha":provenance,
                    }),
                ),
                (
                    "worktree".into(),
                    serde_json::json!({
                        "branch":"daemon/prepublication","path":"/tmp/prepublication"
                    }),
                ),
            ]
        );
    }

    #[test]
    fn source_cancellation_rejects_wrong_creator_stale_revision_and_replay() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        assert_eq!(
            cancel_source_graph(&mut conn, "stranger", 1, Some(1), 5).unwrap(),
            SourceCancellation::Rejected
        );
        assert_eq!(
            cancel_source_graph(&mut conn, "owner", 1, Some(2), 5).unwrap(),
            SourceCancellation::Rejected
        );
        assert_eq!(
            cancel_source_graph(&mut conn, "owner", 1, Some(1), 5).unwrap(),
            SourceCancellation::Cancelled
        );
        assert_eq!(
            cancel_source_graph(&mut conn, "owner", 1, Some(1), 6).unwrap(),
            SourceCancellation::Rejected
        );
        let source_status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(source_status, "cancelled");
    }

    #[test]
    fn current_source_assignee_can_cancel_but_stranger_cannot() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET assignee='delegate' WHERE id=1", [])
            .unwrap();
        assert_eq!(
            cancel_source_graph(&mut conn, "stranger", 1, Some(1), 5).unwrap(),
            SourceCancellation::Rejected
        );
        assert_eq!(
            cancel_source_graph(&mut conn, "delegate", 1, Some(1), 6).unwrap(),
            SourceCancellation::Cancelled
        );
        let source: (String, Option<String>) = conn
            .query_row("SELECT status,assignee FROM tasks WHERE id=1", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(source, ("cancelled".into(), None));
    }

    #[test]
    fn source_cancellation_requires_expected_revision() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        assert!(matches!(
            cancel_source_graph(&mut conn, "owner", 1, None, 5),
            Err(QuorumError::Usage(_))
        ));
    }

    #[test]
    fn cleanup_intents_reject_unknown_malformed_nul_and_oversize_without_mutation() {
        for (kind, artifact_ref) in [
            ("unknown", r#"{"value":"x"}"#.to_string()),
            ("process", "not-json".to_string()),
            (
                "worktree",
                serde_json::json!({"branch":"daemon/live","path":"bad\0path"}).to_string(),
            ),
            (
                "branch",
                serde_json::json!({
                    "expected_sha":"a".repeat(MAX_CLEANUP_ARTIFACT_BYTES),
                    "name":"daemon/live"
                })
                .to_string(),
            ),
        ] {
            let mut conn = setup();
            let graph = begin(&mut conn);
            let ids =
                materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
                    .unwrap()
                    .unwrap();
            let result = cancel_graph(
                &mut conn,
                1,
                &[CleanupIntent {
                    task_id: ids[0],
                    artifact_kind: kind.into(),
                    artifact_ref,
                }],
                5,
            );
            assert!(matches!(result, Err(QuorumError::Usage(_))));
            let unchanged: (String, String, i64) = conn
                .query_row(
                    "SELECT d.state,t.status,
                       (SELECT count(*) FROM decomposition_cleanup WHERE graph_id=d.id)
                     FROM task_decompositions d JOIN tasks t ON t.id=?2 WHERE d.id=?1",
                    params![graph, ids[0]],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(unchanged, ("active".into(), "open".into(), 0));
        }
    }

    #[test]
    fn recovery_reset_is_refused_after_delivery_evidence() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='working' WHERE id=?1", [ids[0]])
            .unwrap();
        assert!(!recovery_reset(&mut conn, graph, "inconsistent", 5).unwrap());
        conn.execute("UPDATE tasks SET status='open' WHERE id=?1", [ids[0]])
            .unwrap();
        assert!(recovery_reset(&mut conn, graph, "inconsistent", 6).unwrap());
        let state: (String, i64) = conn
            .query_row(
                "SELECT state,plan_revision FROM task_decompositions WHERE id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("freeze-requested".into(), 2));
    }

    #[test]
    fn startup_reconciliation_keeps_complete_active_and_blocked_graphs() {
        for blocked in [false, true] {
            let mut conn = setup();
            let graph = begin(&mut conn);
            let ids = materialize_graph(
                &mut conn,
                graph,
                1,
                &[child("a", &[]), child("b", &["a"])],
                4,
            )
            .unwrap()
            .unwrap();
            if blocked {
                conn.execute(
                    "UPDATE task_decompositions SET state='blocked' WHERE id=?1",
                    [graph],
                )
                .unwrap();
                conn.execute("UPDATE tasks SET status='failed' WHERE id=?1", [ids[0]])
                    .unwrap();
            }
            let result = reconcile_startup_graphs(&mut conn, 5).unwrap();
            assert_eq!(result.healthy, 1);
            assert_eq!(result.reset, 0);
            assert_eq!(result.held, 0);
        }
    }

    #[test]
    fn startup_reconciliation_repairs_pre_fix_provider_retry_drain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale-provider-retry.db");
        let graph = {
            let mut conn = crate::db::open(&path).unwrap();
            conn.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('large','open','owner',1,1)",
                [],
            )
            .unwrap();
            let graph = begin_planning(
                &mut conn,
                &BeginPlanning {
                    source_task_id: 1,
                    expected_revision: 1,
                    provider: "claude",
                    model: "opus",
                    frozen_base_sha: "ignored-until-drained",
                    now: 2,
                },
            )
            .unwrap()
            .unwrap();
            set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 3).unwrap();
            bind_frozen_base_and_enter_planning(
                &mut conn,
                graph,
                "0123456789abcdef0123456789abcdef01234567",
                4,
            )
            .unwrap();
            record_attempt(
                &mut conn,
                graph,
                "provider",
                "protocol",
                "missing outcome",
                5,
            )
            .unwrap();
            reacquire_freeze(&mut conn, graph, 6).unwrap();
            set_frozen_phase(&mut conn, graph, "freeze-requested", "draining", None, 7).unwrap();
            conn.execute(
                "UPDATE task_decompositions SET frozen_base_sha=?2 WHERE id=?1",
                params![graph, "0123456789abcdef0123456789abcdef01234567"],
            )
            .unwrap();
            graph
        };

        let mut conn = crate::db::open(&path).unwrap();
        reconcile_startup_graphs(&mut conn, 8).unwrap();
        assert!(bind_frozen_base_and_enter_planning(
            &mut conn,
            graph,
            "fedcba9876543210fedcba9876543210fedcba98",
            9,
        )
        .unwrap());
        let state: (String, String, i64) = conn
            .query_row(
                "SELECT state,frozen_base_sha,provider_failures
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            state,
            (
                "planning".into(),
                "fedcba9876543210fedcba9876543210fedcba98".into(),
                1,
            )
        );
    }

    #[test]
    fn startup_reconciliation_holds_incomplete_graph_with_delivery_evidence() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a", &[]), child("b", &["a"])],
            4,
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "DELETE FROM task_graph_members WHERE graph_id=?1 AND task_id=?2",
            params![graph, ids[1]],
        )
        .unwrap();
        conn.execute("UPDATE tasks SET status='working' WHERE id=?1", [ids[0]])
            .unwrap();
        let result = reconcile_startup_graphs(&mut conn, 5).unwrap();
        assert_eq!(result.held, 1);
        let state: (String, i64) = conn
            .query_row(
                "SELECT state,active FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("held".into(), 0));
    }

    #[test]
    fn recovery_reset_reacquires_freeze_and_can_materialize_again() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();

        assert!(recovery_reset(&mut conn, graph, "inconsistent", 5).unwrap());
        assert!(reacquire_freeze(&mut conn, graph, 6).unwrap());
        assert!(
            set_frozen_phase(&mut conn, graph, "freeze-requested", "planning", None, 7).unwrap()
        );
        assert!(set_frozen_phase(&mut conn, graph, "planning", "preclassifying", None, 8).unwrap());
        let replacement =
            materialize_graph(&mut conn, graph, 1, &[child("c", &[]), child("d", &[])], 9)
                .unwrap()
                .unwrap();
        assert_eq!(replacement.len(), 2);
        let retained_history: i64 = conn
            .query_row(
                "SELECT count(*) FROM task_graph_members
                 WHERE graph_id=?1 AND active=0 AND plan_revision=1",
                [graph],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_history, 2);
        let state: (String, i64, i64) = conn
            .query_row(
                "SELECT state,active,freeze_active FROM task_decompositions WHERE id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, ("active".into(), 1, 0));

        let before_delivery = reconcile_startup_graphs(&mut conn, 10).unwrap();
        assert_eq!(
            before_delivery,
            StartupReconcileResult {
                healthy: 1,
                reset: 0,
                held: 0,
            }
        );

        conn.execute(
            "UPDATE tasks SET status='working' WHERE id=?1",
            [replacement[0]],
        )
        .unwrap();
        let after_delivery = reconcile_startup_graphs(&mut conn, 11).unwrap();
        assert_eq!(
            after_delivery,
            StartupReconcileResult {
                healthy: 1,
                reset: 0,
                held: 0,
            }
        );
        let final_state: (String, i64) = conn
            .query_row(
                "SELECT state,active FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(final_state, ("active".into(), 1));
    }

    #[test]
    fn graph_children_preempt_unrelated_work_and_only_two_can_start() {
        let mut conn = setup();
        conn.execute("UPDATE tasks SET priority=1 WHERE id=1", [])
            .unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,priority,created_by,created_at,updated_at,refs)
             VALUES ('urgent unrelated','open',99,'owner',1,1,?1)",
            [child("unused", &[]).classification_refs],
        )
        .unwrap();
        let graph = begin(&mut conn);
        let ids = materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a", &[]), child("b", &[]), child("c", &[])],
            4,
        )
        .unwrap()
        .unwrap();

        let first = crate::tasks::claim(&mut conn, "w1", None, &[], 60, 5)
            .unwrap()
            .unwrap();
        assert!(ids.contains(&first.id), "graph work must sort first");
        let second = crate::tasks::claim(&mut conn, "w2", None, &[], 60, 6)
            .unwrap()
            .unwrap();
        assert!(ids.contains(&second.id));
        let third = crate::tasks::claim(&mut conn, "w3", None, &[], 60, 7)
            .unwrap()
            .unwrap();
        assert_eq!(third.title, "urgent unrelated");
        assert_eq!(
            ids.iter()
                .filter(|id| {
                    conn.query_row(
                        "SELECT status='working' FROM tasks WHERE id=?1",
                        [id],
                        |r| r.get::<_, bool>(0),
                    )
                    .unwrap()
                })
                .count(),
            2
        );
    }

    #[test]
    fn failed_sibling_stops_new_implementation_claims() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a", &[]), child("b", &[]), child("c", &[])],
            4,
        )
        .unwrap()
        .unwrap();
        crate::tasks::claim(&mut conn, "w1", Some(ids[0]), &[], 60, 5)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='failed' WHERE id=?1", [ids[1]])
            .unwrap();

        assert!(
            crate::tasks::claim(&mut conn, "w2", Some(ids[2]), &[], 60, 6)
                .unwrap()
                .is_none()
        );
        let first_status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=?1", [ids[0]], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(first_status, "working", "active sibling keeps authority");
    }

    #[test]
    fn failed_child_blocks_active_graph_and_alerts_owner_atomically() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        let reason = "push rejected after protected-branch update";
        let tx = begin_immediate(&mut conn).unwrap();
        tx.execute(
            "UPDATE tasks SET status='failed',refs=?2,updated_at=5 WHERE id=?1",
            params![
                ids[0],
                serde_json::json!({
                    "daemon_parked": true,
                    "daemon_parked_reason": reason,
                    "daemon_resume_status": "rework"
                })
                .to_string()
            ],
        )
        .unwrap();
        assert!(block_graph_if_child_failed(&tx, ids[0], reason, 5).unwrap());
        tx.commit().unwrap();

        let aggregate: (String, i64, String, String) = conn
            .query_row(
                "SELECT state,active,hold_code,hold_summary
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            aggregate,
            (
                "blocked".into(),
                1,
                "generated-child-failed".into(),
                format!("generated child task #{} failed: {reason}", ids[0]),
            )
        );
        let event: (String, String, String) = conn
            .query_row(
                "SELECT kind,subject,body FROM events
                 WHERE kind='task_graph_blocked'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(event.0, "task_graph_blocked");
        assert_eq!(event.1, format!("task#{}", ids[0]));
        assert!(event.2.contains(&format!("task #{}", ids[0])));
        assert!(event.2.contains(reason));
        let alert: (String, String, String, String) = conn
            .query_row(
                "SELECT kind,recipient,refs,body FROM messages
                 WHERE kind='alert' AND recipient='owner'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(alert.0, "alert");
        assert_eq!(alert.1, "owner");
        assert_eq!(alert.2, format!("task:{}", ids[0]));
        assert!(alert.3.contains("task graph blocked"));
        assert!(alert.3.contains(reason));
    }

    #[test]
    fn failed_child_with_retry_or_remediation_marker_keeps_graph_active() {
        for refs in [
            serde_json::json!({"runner_retry": {"requested": true}}),
            serde_json::json!({"codex_retry_requested": true}),
            serde_json::json!({"daemon_rework_retry_requested": true}),
            serde_json::json!({"ci_remediation_requested": true}),
        ] {
            let mut conn = setup();
            let graph = begin(&mut conn);
            let ids =
                materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
                    .unwrap()
                    .unwrap();
            let tx = begin_immediate(&mut conn).unwrap();
            tx.execute(
                "UPDATE tasks SET status='failed',refs=?2,updated_at=5 WHERE id=?1",
                params![ids[0], refs.to_string()],
            )
            .unwrap();
            assert!(!block_graph_if_child_failed(&tx, ids[0], "retry staged", 5).unwrap());
            tx.commit().unwrap();

            let state: (String, i64, Option<String>, Option<String>) = conn
                .query_row(
                    "SELECT state,active,hold_code,hold_summary
                     FROM task_decompositions WHERE id=?1",
                    [graph],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(state, ("active".into(), 1, None, None));
            let side_effects: (i64, i64) = conn
                .query_row(
                    "SELECT
                         (SELECT count(*) FROM events WHERE kind='task_graph_blocked'),
                         (SELECT count(*) FROM messages
                          WHERE kind='alert' AND recipient='owner')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(side_effects, (0, 0));
        }
    }

    #[test]
    fn failed_child_graph_block_is_idempotent() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='failed',refs='{}',updated_at=5 WHERE id=?1",
            [ids[0]],
        )
        .unwrap();

        let tx = begin_immediate(&mut conn).unwrap();
        assert!(block_graph_if_child_failed(&tx, ids[0], "terminal park", 5).unwrap());
        tx.commit().unwrap();
        let replay = begin_immediate(&mut conn).unwrap();
        assert!(!block_graph_if_child_failed(&replay, ids[0], "terminal park", 6).unwrap());
        replay.commit().unwrap();

        let state: (String, i64, i64, i64) = conn
            .query_row(
                "SELECT state,updated_at,
                        (SELECT count(*) FROM events WHERE kind='task_graph_blocked'),
                        (SELECT count(*) FROM messages
                         WHERE kind='alert' AND recipient='owner')
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, ("blocked".into(), 5, 1, 1));
    }

    #[test]
    fn graph_blocker_is_atomic_and_stale_signal_is_clean() {
        let mut conn = setup();
        let (graph, ids) = make_reviewable_graph(&mut conn);
        let agent_run = add_reviewer_authority(&mut conn, ids[0], "r", "review-run", "reviewer", 4);
        let evidence = vec!["diff moves sibling-owned schema work into this child".into()];
        assert!(block_graph(
            &mut conn,
            &GraphBlocker {
                task_id: ids[0],
                reviewer: "r",
                run_id: "review-run",
                category: "boundary-violation",
                violated_boundary: "child must not absorb sibling scope",
                evidence: &evidence,
                now: 5,
            }
        )
        .unwrap());
        assert!(!block_graph(
            &mut conn,
            &GraphBlocker {
                task_id: ids[0],
                reviewer: "r",
                run_id: "review-run",
                category: "boundary-violation",
                violated_boundary: "child must not absorb sibling scope",
                evidence: &evidence,
                now: 6,
            }
        )
        .unwrap());
        let state: (String, i64, String, String) = conn
            .query_row(
                "SELECT d.state,d.active,source.status,child.status
                 FROM task_decompositions d
                 JOIN tasks source ON source.id=d.source_task_id
                 JOIN tasks child ON child.id=?2 WHERE d.id=?1",
                params![graph, ids[0]],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            state,
            ("blocked".into(), 1, "decomposed".into(), "failed".into())
        );
        assert!(
            crate::tasks::claim(&mut conn, "w", Some(ids[1]), &[], 60, 7)
                .unwrap()
                .is_none()
        );
        let authority: (Option<i64>, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT c.revoked_at,r.ended_at,r.end_reason FROM run_capabilities c
                 JOIN agent_runs r ON r.id=?2 WHERE c.run_id=?1",
                params!["review-run", agent_run],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(authority, (Some(5), Some(5), Some("graph-blocker".into())));
    }

    #[test]
    fn concurrent_graph_blocker_replay_has_one_atomic_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph-blocker-race.db");
        let mut conn = crate::db::open(&path).unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('large','open','owner',1,1)",
            [],
        )
        .unwrap();
        let (graph, ids) = make_reviewable_graph(&mut conn);
        add_reviewer_authority(&mut conn, ids[0], "r", "review-run", "reviewer", 4);
        drop(conn);

        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for now in [5, 6] {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            let task_id = ids[0];
            handles.push(std::thread::spawn(move || {
                let mut conn = crate::db::open(&path).unwrap();
                let evidence = vec!["diff crosses the assigned child boundary".into()];
                barrier.wait();
                block_graph(
                    &mut conn,
                    &GraphBlocker {
                        task_id,
                        reviewer: "r",
                        run_id: "review-run",
                        category: "boundary-violation",
                        violated_boundary: "parser-only child",
                        evidence: &evidence,
                        now,
                    },
                )
                .unwrap()
            }));
        }
        let winners = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1);

        let conn = crate::db::open(&path).unwrap();
        assert_eq!(
            graph_mutation_state(&conn, graph, ids[0]),
            ("blocked".into(), "failed".into(), 1)
        );
        let live_capabilities: i64 = conn
            .query_row(
                "SELECT count(*) FROM run_capabilities
                 WHERE run_id='review-run' AND revoked_at IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(live_capabilities, 0);
    }

    #[test]
    fn graph_blocker_rejects_unknown_revoked_wrong_and_superseded_runs_without_mutation() {
        enum InvalidRun {
            Unknown,
            Revoked,
            WrongRole,
            WrongTask,
            WrongAgent,
            Superseded,
        }
        for case in [
            InvalidRun::Unknown,
            InvalidRun::Revoked,
            InvalidRun::WrongRole,
            InvalidRun::WrongTask,
            InvalidRun::WrongAgent,
            InvalidRun::Superseded,
        ] {
            let mut conn = setup();
            let (graph, ids) = make_reviewable_graph(&mut conn);
            let attempted_run = match case {
                InvalidRun::Unknown => {
                    crate::agent_runs::insert(
                        &conn, ids[0], "r", "reviewer", "model", "high", "codex", 4,
                    )
                    .unwrap();
                    "unknown"
                }
                InvalidRun::Revoked => {
                    add_reviewer_authority(&mut conn, ids[0], "r", "revoked", "reviewer", 4);
                    crate::capabilities::revoke(&mut conn, "revoked", 5).unwrap();
                    "revoked"
                }
                InvalidRun::WrongRole => {
                    add_reviewer_authority(&mut conn, ids[0], "r", "worker-run", "worker", 4);
                    "worker-run"
                }
                InvalidRun::WrongTask => {
                    crate::agent_runs::insert(
                        &conn, ids[0], "r", "reviewer", "model", "high", "codex", 4,
                    )
                    .unwrap();
                    crate::capabilities::issue(&mut conn, "other-task", ids[1], "r", "reviewer", 4)
                        .unwrap();
                    "other-task"
                }
                InvalidRun::WrongAgent => {
                    crate::agent_runs::insert(
                        &conn, ids[0], "r", "reviewer", "model", "high", "codex", 4,
                    )
                    .unwrap();
                    crate::capabilities::issue(
                        &mut conn,
                        "other-agent",
                        ids[0],
                        "impostor",
                        "reviewer",
                        4,
                    )
                    .unwrap();
                    "other-agent"
                }
                InvalidRun::Superseded => {
                    add_reviewer_authority(&mut conn, ids[0], "r", "old-run", "reviewer", 4);
                    add_reviewer_authority(&mut conn, ids[0], "r", "new-run", "reviewer", 5);
                    "old-run"
                }
            };
            let before = graph_mutation_state(&conn, graph, ids[0]);
            let evidence = vec!["concrete repository evidence".into()];
            assert!(!block_graph(
                &mut conn,
                &GraphBlocker {
                    task_id: ids[0],
                    reviewer: "r",
                    run_id: attempted_run,
                    category: "boundary-violation",
                    violated_boundary: "assigned boundary",
                    evidence: &evidence,
                    now: 10,
                }
            )
            .unwrap());
            assert_eq!(graph_mutation_state(&conn, graph, ids[0]), before);
        }
    }

    #[test]
    fn graph_blocker_rejects_nul_in_every_text_field_without_mutation() {
        for field in 0..5 {
            let mut conn = setup();
            let (graph, ids) = make_reviewable_graph(&mut conn);
            add_reviewer_authority(&mut conn, ids[0], "r", "review-run", "reviewer", 4);
            let mut reviewer = "r".to_string();
            let mut run_id = "review-run".to_string();
            let mut category = "boundary-violation".to_string();
            let mut boundary = "assigned boundary".to_string();
            let mut evidence = vec!["concrete repository evidence".to_string()];
            match field {
                0 => reviewer.push('\0'),
                1 => run_id.push('\0'),
                2 => category.push('\0'),
                3 => boundary.push('\0'),
                4 => evidence[0].push('\0'),
                _ => unreachable!(),
            }
            let before = graph_mutation_state(&conn, graph, ids[0]);
            let error = block_graph(
                &mut conn,
                &GraphBlocker {
                    task_id: ids[0],
                    reviewer: &reviewer,
                    run_id: &run_id,
                    category: &category,
                    violated_boundary: &boundary,
                    evidence: &evidence,
                    now: 10,
                },
            )
            .unwrap_err();
            assert!(matches!(error, QuorumError::BadInput(_)));
            assert_eq!(graph_mutation_state(&conn, graph, ids[0]), before);
        }
    }

    #[test]
    fn graph_blocker_core_rejects_unsupported_category_without_mutation() {
        let mut conn = setup();
        let (graph, ids) = make_reviewable_graph(&mut conn);
        add_reviewer_authority(&mut conn, ids[0], "r", "review-run", "reviewer", 4);
        let before = graph_mutation_state(&conn, graph, ids[0]);
        let evidence = vec!["concrete diff evidence".into()];
        assert!(block_graph(
            &mut conn,
            &GraphBlocker {
                task_id: ids[0],
                reviewer: "r",
                run_id: "review-run",
                category: "invented-category",
                violated_boundary: "assigned boundary",
                evidence: &evidence,
                now: 5,
            },
        )
        .is_err());
        assert_eq!(graph_mutation_state(&conn, graph, ids[0]), before);
    }

    #[test]
    fn final_child_merge_completes_graph_and_source_atomically() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        assert!(crate::tasks::close_after_merge(&mut conn, ids[0], "merged", 5).unwrap());
        let midway: (String, String) = conn
            .query_row(
                "SELECT d.state,t.status FROM task_decompositions d
                 JOIN tasks t ON t.id=d.source_task_id WHERE d.id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(midway, ("active".into(), "decomposed".into()));

        assert!(crate::tasks::close_after_merge(&mut conn, ids[1], "merged", 6).unwrap());
        let completed: (String, i64, String) = conn
            .query_row(
                "SELECT d.state,d.active,t.status FROM task_decompositions d
                 JOIN tasks t ON t.id=d.source_task_id WHERE d.id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(completed, ("completed".into(), 0, "done".into()));
    }

    #[test]
    fn exact_continuation_adoption_completes_incident_graph_and_releases_dependent() {
        let mut fixture = RecoveryFixture::new();
        let recovery_before: (String, String) = fixture
            .conn
            .query_row(
                "SELECT status,refs FROM tasks WHERE id=?1",
                [fixture.recovery],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert!(fixture.adoption());
        assert_graph_completed(
            &fixture.conn,
            fixture.graph,
            1,
            &[
                fixture.siblings[0],
                fixture.siblings[1],
                fixture.siblings[2],
                fixture.original,
            ],
        );
        let (original_refs, original_provenance): (String, Option<String>) = fixture
            .conn
            .query_row(
                "SELECT refs,completion_provenance FROM tasks WHERE id=?1",
                [fixture.original],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let original_refs: serde_json::Value = serde_json::from_str(&original_refs).unwrap();
        assert_eq!(original_refs["pr"], RECOVERY_PR);
        assert_eq!(
            original_provenance.as_deref(),
            Some(crate::tasks::COMPLETION_PROVENANCE_MERGED)
        );
        assert_eq!(
            original_refs["recovery_delivery"],
            serde_json::json!({
                "source_task": fixture.original,
                "recovery_task": fixture.recovery,
                "pr": RECOVERY_PR,
                "merged_head_sha": RECOVERY_HEAD,
                "adopted_at": 50
            })
        );
        for key in [
            "daemon_parked",
            "daemon_parked_reason",
            "daemon_resume_status",
            "daemon_publication",
        ] {
            assert!(original_refs.get(key).is_none(), "stale {key} survived");
        }
        let recovery_after: (String, String) = fixture
            .conn
            .query_row(
                "SELECT status,refs FROM tasks WHERE id=?1",
                [fixture.recovery],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(recovery_before, recovery_after);
        assert!(
            crate::tasks::get(&fixture.conn, fixture.dependent)
                .unwrap()
                .unwrap()
                .ready
        );
        let events: (i64, i64) = fixture
            .conn
            .query_row(
                "SELECT
                   count(*) FILTER (WHERE kind='task_done' AND subject=?1),
                   count(*) FILTER (WHERE kind='task_graph_completed' AND subject='task#1')
                 FROM events",
                [format!("task#{}", fixture.original)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(events, (1, 1));
        assert_eq!(
            crate::review_followup_graph_eligibility::classify_graph_assessment(
                &fixture.conn,
                fixture.graph,
                "followups-v1",
            )
            .unwrap(),
            crate::review_followup_graph_eligibility::GraphAssessmentEligibility::WaitingInterpretation
        );
    }

    #[test]
    fn exact_continuation_adoption_succeeds_on_blocked_graph_without_completing_it() {
        let mut fixture = RecoveryFixture::new();
        fixture
            .conn
            .execute(
                "UPDATE task_decompositions SET state='blocked' WHERE id=?1",
                [fixture.graph],
            )
            .unwrap();

        assert!(fixture.adoption());

        let state: (String, String, i64, String) = fixture
            .conn
            .query_row(
                "SELECT original.status,graph.state,graph.active,source.status
                 FROM tasks original
                 JOIN task_graph_members member ON member.task_id=original.id
                 JOIN task_decompositions graph ON graph.id=member.graph_id
                 JOIN tasks source ON source.id=graph.source_task_id
                 WHERE original.id=?1",
                [fixture.original],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            state,
            ("done".into(), "blocked".into(), 1, "decomposed".into())
        );

        let completed_events: i64 = fixture
            .conn
            .query_row(
                "SELECT count(*) FROM events
                 WHERE kind='task_graph_completed' AND subject='task#1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(completed_events, 0);
    }

    #[test]
    fn explicit_recovery_adopts_durable_delivery_after_event_expiry() {
        let mut fixture = RecoveryFixture::new();
        fixture.make_explicit_eligible();

        // The automatic path remains bound to live event evidence. The
        // explicit operator path must not revive that discovery mechanism; it
        // authorizes only this exact pair from the durable managed chain.
        assert!(!adopt_recovery_delivery(
            &mut fixture.conn,
            fixture.original,
            fixture.recovery,
            90_000,
        )
        .unwrap());
        assert!(fixture.explicit_adoption(90_000));

        assert_graph_completed(
            &fixture.conn,
            fixture.graph,
            1,
            &[
                fixture.siblings[0],
                fixture.siblings[1],
                fixture.siblings[2],
                fixture.original,
            ],
        );
        assert!(
            crate::tasks::get(&fixture.conn, fixture.dependent)
                .unwrap()
                .unwrap()
                .ready
        );

        let refs: String = fixture
            .conn
            .query_row(
                "SELECT refs FROM tasks WHERE id=?1",
                [fixture.original],
                |row| row.get(0),
            )
            .unwrap();
        let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(refs["recovery_delivery"]["authority"], "explicit-operator");
        assert_eq!(refs["recovery_delivery"]["authorized_by"], "operator");
        assert_eq!(refs["recovery_delivery"]["decomposition_source"], 1);
        assert_eq!(refs["recovery_delivery"]["merged_head_sha"], RECOVERY_HEAD);

        let (reason, summary): (String, String) = fixture
            .conn
            .query_row(
                "SELECT reason_code,summary FROM decomposition_attempts
                 WHERE graph_id=?1 AND kind='recovery'",
                [fixture.graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(reason, "explicit-delivery-adoption");
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(summary["decomposition_source"], 1);
        assert_eq!(summary["original_child"], fixture.original);
        assert_eq!(summary["recovery_task"], fixture.recovery);
        assert_eq!(summary["authorized_by"], "operator");

        assert!(!fixture.explicit_adoption(90_001));
        let audit_count: i64 = fixture
            .conn
            .query_row(
                "SELECT count(*) FROM decomposition_attempts
                 WHERE graph_id=?1 AND kind='recovery'
                   AND reason_code='explicit-delivery-adoption'",
                [fixture.graph],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_count, 1, "replay must not duplicate provenance");
    }

    #[test]
    fn explicit_recovery_fails_closed_for_inconsistent_durable_evidence() {
        let cases: Vec<(&str, RecoveryEvidenceMutation)> = vec![
            (
                "non-failed child",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET status='open' WHERE id=?1",
                            [fixture.original],
                        )
                        .unwrap();
                }),
            ),
            (
                "recovery task not completed",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET status='failed' WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "conflicting source provenance",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.source_task',999)
                             WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "wrong PR",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET continue_pr=?2 WHERE id=?1",
                            params![fixture.recovery, RECOVERY_PR + 1],
                        )
                        .unwrap();
                }),
            ),
            (
                "wrong repository",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.repo','owner/repo')
                             WHERE id=?1",
                            [fixture.original],
                        )
                        .unwrap();
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.repo','other/repo')
                             WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "missing publication",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE agent_runs SET end_reason='failed'
                             WHERE task_id=?1 AND role='worker'",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "wrong approved head",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE agent_runs SET review_head_sha=?2
                             WHERE task_id=?1 AND role='reviewer'",
                            params![fixture.recovery, "cccccccccccccccccccccccccccccccccccccccc"],
                        )
                        .unwrap();
                }),
            ),
            (
                "missing managed approval",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE agent_runs SET end_reason='completed'
                             WHERE task_id=?1 AND role='reviewer'",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "missing merge evidence",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET completion_provenance=NULL WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "unfinished sibling",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET status='open' WHERE id=?1",
                            [fixture.siblings[0]],
                        )
                        .unwrap();
                }),
            ),
        ];

        for (name, mutate) in cases {
            let mut fixture = RecoveryFixture::new();
            fixture.make_explicit_eligible();
            mutate(&mut fixture);
            assert!(
                !fixture.explicit_adoption(90_000),
                "{name} unexpectedly adopted"
            );
            let state: (String, String, String, i64) = fixture
                .conn
                .query_row(
                    "SELECT child.status,graph.state,source.status,
                            (SELECT count(*) FROM decomposition_attempts
                             WHERE graph_id=graph.id AND kind='recovery'
                               AND reason_code='explicit-delivery-adoption')
                     FROM tasks child
                     JOIN task_graph_members member ON member.task_id=child.id
                     JOIN task_decompositions graph ON graph.id=member.graph_id
                     JOIN tasks source ON source.id=graph.source_task_id
                     WHERE child.id=?1",
                    [fixture.original],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(state.1, "active", "{name} mutated graph state");
            assert_eq!(state.2, "decomposed", "{name} mutated source state");
            assert_eq!(state.3, 0, "{name} persisted recovery authority");
        }
    }

    #[test]
    fn explicit_recovery_rejects_nonmember() {
        let mut fixture = RecoveryFixture::new();
        fixture.make_explicit_eligible();
        fixture
            .conn
            .execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at,refs)
                 VALUES ('not a member','failed','owner',1,1,?1)",
                [serde_json::json!({"pr": RECOVERY_PR}).to_string()],
            )
            .unwrap();
        let nonmember = fixture.conn.last_insert_rowid();
        fixture
            .conn
            .execute(
                "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (?1,?2,'daemon/original',?3,0,8)",
                params![nonmember, RECOVERY_PR, ORIGINAL_HEAD],
            )
            .unwrap();

        assert!(!adopt_explicit_recovery_delivery(
            &mut fixture.conn,
            &ExplicitRecoveryAdoption {
                original_child_id: nonmember,
                recovery_task_id: fixture.recovery,
                authorized_by: "operator",
                now: 90_000,
            },
        )
        .unwrap());
    }

    #[test]
    fn explicit_recovery_rejects_invalid_operator_identity_without_mutation() {
        for identity in ["", "operator\0forged"] {
            let mut fixture = RecoveryFixture::new();
            fixture.make_explicit_eligible();
            assert!(adopt_explicit_recovery_delivery(
                &mut fixture.conn,
                &ExplicitRecoveryAdoption {
                    original_child_id: fixture.original,
                    recovery_task_id: fixture.recovery,
                    authorized_by: identity,
                    now: 90_000,
                },
            )
            .is_err());
            let state: (String, String, i64) = fixture
                .conn
                .query_row(
                    "SELECT child.status,graph.state,
                            (SELECT count(*) FROM decomposition_attempts
                             WHERE graph_id=graph.id AND kind='recovery')
                     FROM tasks child
                     JOIN task_graph_members member ON member.task_id=child.id
                     JOIN task_decompositions graph ON graph.id=member.graph_id
                     WHERE child.id=?1",
                    [fixture.original],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(state, ("failed".into(), "active".into(), 0));
        }
    }

    #[test]
    fn continuation_adoption_rejects_inconsistent_or_incomplete_evidence_without_mutation() {
        let cases: Vec<(&str, RecoveryEvidenceMutation)> = vec![
            (
                "wrong PR",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET continue_pr=?2 WHERE id=?1",
                            params![fixture.recovery, RECOVERY_PR + 1],
                        )
                        .unwrap();
                }),
            ),
            (
                "wrong repository",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.repo','owner/repo') WHERE id=?1",
                            [fixture.original],
                        )
                        .unwrap();
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.repo','other/repo') WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "wrong source provenance",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET refs=json_set(refs,'$.source_task',999) WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "inconsistent authoritative SHA",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE agent_runs SET review_head_sha=?2
                             WHERE task_id=?1 AND role='reviewer'",
                            params![fixture.recovery, "cccccccccccccccccccccccccccccccccccccccc"],
                        )
                        .unwrap();
                }),
            ),
            (
                "unpublished recovery",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "DELETE FROM events WHERE subject=?1 AND kind='task_in_review'",
                            [format!("task#{}", fixture.recovery)],
                        )
                        .unwrap();
                }),
            ),
            (
                "unmerged recovery",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "DELETE FROM events WHERE subject=?1 AND kind='task_merging'",
                            [format!("task#{}", fixture.recovery)],
                        )
                        .unwrap();
                }),
            ),
            (
                "publication event expired at the exact adoption boundary",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE events SET expires_at=50
                             WHERE subject=?1 AND kind='task_in_review'",
                            [format!("task#{}", fixture.recovery)],
                        )
                        .unwrap();
                }),
            ),
            (
                "merging event expired at the exact adoption boundary",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE events SET expires_at=50
                             WHERE subject=?1 AND kind='task_merging'",
                            [format!("task#{}", fixture.recovery)],
                        )
                        .unwrap();
                }),
            ),
            (
                "done event expired at the exact adoption boundary",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE events SET expires_at=50
                             WHERE subject=?1 AND kind='task_done'",
                            [format!("task#{}", fixture.recovery)],
                        )
                        .unwrap();
                }),
            ),
            (
                "unreviewed recovery",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE agent_runs SET end_reason='completed'
                             WHERE task_id=?1 AND role='reviewer'",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "failed recovery",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET status='failed' WHERE id=?1",
                            [fixture.recovery],
                        )
                        .unwrap();
                }),
            ),
            (
                "partial graph",
                Box::new(|fixture| {
                    fixture
                        .conn
                        .execute(
                            "UPDATE tasks SET status='open' WHERE id=?1",
                            [fixture.siblings[0]],
                        )
                        .unwrap();
                }),
            ),
        ];

        for (name, mutate) in cases {
            let mut fixture = RecoveryFixture::new();
            mutate(&mut fixture);
            assert!(!fixture.adoption(), "{name} unexpectedly adopted");
            let state: (String, String, String) = fixture
                .conn
                .query_row(
                    "SELECT child.status,graph.state,source.status
                     FROM tasks child
                     JOIN task_graph_members member ON member.task_id=child.id
                     JOIN task_decompositions graph ON graph.id=member.graph_id
                     JOIN tasks source ON source.id=graph.source_task_id
                     WHERE child.id=?1",
                    [fixture.original],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                state,
                ("failed".into(), "active".into(), "decomposed".into()),
                "{name} mutated lifecycle state"
            );
            let emitted: i64 = fixture
                .conn
                .query_row(
                    "SELECT count(*) FROM events
                     WHERE kind='task_done' AND subject=?1",
                    [format!("task#{}", fixture.original)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(emitted, 0, "{name} emitted an adoption event");
        }
    }

    #[test]
    fn continuation_adoption_rejects_nonmember_and_replay() {
        let mut fixture = RecoveryFixture::new();
        fixture
            .conn
            .execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at,refs)
                 VALUES ('not a member','failed','owner',1,1,?1)",
                [serde_json::json!({"pr": RECOVERY_PR}).to_string()],
            )
            .unwrap();
        let nonmember = fixture.conn.last_insert_rowid();
        fixture
            .conn
            .execute(
                "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (?1,?2,'daemon/original',?3,0,1)",
                params![nonmember, RECOVERY_PR, ORIGINAL_HEAD],
            )
            .unwrap();
        fixture
            .conn
            .execute(
                "UPDATE tasks SET refs=json_set(refs,'$.source_task',?2) WHERE id=?1",
                params![fixture.recovery, nonmember],
            )
            .unwrap();
        assert!(
            !adopt_recovery_delivery(&mut fixture.conn, nonmember, fixture.recovery, 49).unwrap()
        );
        fixture
            .conn
            .execute(
                "UPDATE tasks SET refs=json_set(refs,'$.source_task',?2) WHERE id=?1",
                params![fixture.recovery, fixture.original],
            )
            .unwrap();
        assert!(fixture.adoption());
        assert!(!adopt_recovery_delivery(
            &mut fixture.conn,
            fixture.original,
            fixture.recovery,
            51
        )
        .unwrap());
        let count: i64 = fixture
            .conn
            .query_row(
                "SELECT count(*) FROM events WHERE kind='task_done' AND subject=?1",
                [format!("task#{}", fixture.original)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn concurrent_exact_continuation_adopters_have_one_winner() {
        use std::sync::{Arc, Barrier};

        for round in 0..8 {
            let fixture = RecoveryFixture::new();
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();
            for _ in 0..2 {
                let path = fixture.db_path.clone();
                let barrier = Arc::clone(&barrier);
                let original = fixture.original;
                let recovery = fixture.recovery;
                handles.push(std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    adopt_recovery_delivery(&mut conn, original, recovery, 50).unwrap()
                }));
            }
            barrier.wait();
            let outcomes: Vec<bool> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect();
            assert_eq!(
                outcomes.iter().filter(|outcome| **outcome).count(),
                1,
                "round {round}: {outcomes:?}"
            );
            let count: i64 = fixture
                .conn
                .query_row(
                    "SELECT count(*) FROM events WHERE kind='task_done' AND subject=?1",
                    [format!("task#{}", fixture.original)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "round {round}");
        }
    }

    #[test]
    fn concurrent_explicit_recovery_adopters_have_one_winner() {
        use std::sync::{Arc, Barrier};

        for round in 0..8 {
            let mut fixture = RecoveryFixture::new();
            fixture.make_explicit_eligible();
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();
            for operator in ["operator-a", "operator-b"] {
                let path = fixture.db_path.clone();
                let barrier = Arc::clone(&barrier);
                let original = fixture.original;
                let recovery = fixture.recovery;
                handles.push(std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    adopt_explicit_recovery_delivery(
                        &mut conn,
                        &ExplicitRecoveryAdoption {
                            original_child_id: original,
                            recovery_task_id: recovery,
                            authorized_by: operator,
                            now: 90_000,
                        },
                    )
                    .unwrap()
                }));
            }
            barrier.wait();
            let outcomes: Vec<bool> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect();
            assert_eq!(
                outcomes.iter().filter(|outcome| **outcome).count(),
                1,
                "round {round}: {outcomes:?}"
            );
            let provenance_count: i64 = fixture
                .conn
                .query_row(
                    "SELECT count(*) FROM decomposition_attempts
                     WHERE graph_id=?1 AND kind='recovery'
                       AND reason_code='explicit-delivery-adoption'",
                    [fixture.graph],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(provenance_count, 1, "round {round}");
        }
    }

    fn assert_graph_completed(conn: &Connection, graph: i64, source: i64, children: &[i64]) {
        let graph_state: (String, i64) = conn
            .query_row(
                "SELECT state,active FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(graph_state, ("completed".into(), 0));
        let source_status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=?1", [source], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_status, "done");
        for child in children {
            let status: String = conn
                .query_row("SELECT status FROM tasks WHERE id=?1", [child], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(status, "done");
        }
    }

    #[test]
    fn merge_succeeded_event_completes_graph_and_source_atomically() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [ids[0]])
            .unwrap();
        conn.execute("UPDATE tasks SET status='merging' WHERE id=?1", [ids[1]])
            .unwrap();

        crate::tasks::apply_event(
            &mut conn,
            "system",
            ids[1],
            &crate::lifecycle::Event::MergeSucceeded,
            5,
        )
        .unwrap();

        assert_graph_completed(&conn, graph, 1, &ids);
    }

    #[test]
    fn pr_found_merged_event_completes_graph_and_source_atomically() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [ids[0]])
            .unwrap();
        conn.execute("UPDATE tasks SET status='in-review' WHERE id=?1", [ids[1]])
            .unwrap();

        crate::tasks::apply_event(
            &mut conn,
            "system",
            ids[1],
            &crate::lifecycle::Event::PrFoundMerged,
            5,
        )
        .unwrap();

        assert_graph_completed(&conn, graph, 1, &ids);
    }

    #[test]
    fn merge_succeeded_rolls_back_when_graph_source_authority_is_lost() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [ids[0]])
            .unwrap();
        conn.execute("UPDATE tasks SET status='merging' WHERE id=?1", [ids[1]])
            .unwrap();
        conn.execute("UPDATE tasks SET status='done' WHERE id=1", [])
            .unwrap();

        let error = match crate::tasks::apply_event(
            &mut conn,
            "system",
            ids[1],
            &crate::lifecycle::Event::MergeSucceeded,
            5,
        ) {
            Ok(_) => panic!("lost source authority must reject final child completion"),
            Err(error) => error,
        };
        assert!(matches!(error, QuorumError::Io(_)));
        let state: (String, i64, String) = conn
            .query_row(
                "SELECT d.state,d.active,t.status FROM task_decompositions d
                 JOIN tasks t ON t.id=?2 WHERE d.id=?1",
                params![graph, ids[1]],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, ("active".into(), 1, "merging".into()));
    }

    #[test]
    fn manual_close_rejects_active_source_without_mutation() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();

        let error = crate::tasks::close_manual(&mut conn, "owner", 1, "obsolete", 5).unwrap_err();
        assert!(matches!(error, QuorumError::Usage(_)));
        let state: (String, i64, String) = conn
            .query_row(
                "SELECT d.state,d.active,t.status FROM task_decompositions d
                 JOIN tasks t ON t.id=d.source_task_id WHERE d.id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, ("active".into(), 1, "decomposed".into()));
        assert!(ids.iter().all(|id| {
            conn.query_row("SELECT status='open' FROM tasks WHERE id=?1", [id], |row| {
                row.get::<_, bool>(0)
            })
            .unwrap()
        }));
    }

    #[test]
    fn manual_final_child_close_completes_graph_and_source_atomically() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();

        crate::tasks::close_manual(&mut conn, "owner", ids[0], "fixed elsewhere", 5)
            .unwrap()
            .unwrap();
        let midway: (String, String) = conn
            .query_row(
                "SELECT d.state,t.status FROM task_decompositions d
                 JOIN tasks t ON t.id=d.source_task_id WHERE d.id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(midway, ("active".into(), "decomposed".into()));

        crate::tasks::close_manual(&mut conn, "owner", ids[1], "merged externally", 6)
            .unwrap()
            .unwrap();
        assert_graph_completed(&conn, graph, 1, &ids);
    }

    #[test]
    fn real_file_concurrent_child_claims_never_exceed_two() {
        use std::sync::{Arc, Barrier};

        for round in 0..10 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("quorum.db");
            let mut conn = crate::db::open(&path).unwrap();
            conn.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('large','open','owner',1,1)",
                [],
            )
            .unwrap();
            let graph = begin(&mut conn);
            let ids = materialize_graph(
                &mut conn,
                graph,
                1,
                &[child("a", &[]), child("b", &[]), child("c", &[])],
                4,
            )
            .unwrap()
            .unwrap();
            drop(conn);

            let barrier = Arc::new(Barrier::new(3));
            let handles: Vec<_> = ids
                .into_iter()
                .enumerate()
                .map(|(index, id)| {
                    let path = path.clone();
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut conn = crate::db::open(&path).unwrap();
                        barrier.wait();
                        crate::tasks::claim(
                            &mut conn,
                            &format!("w{index}"),
                            Some(id),
                            &[],
                            60,
                            10 + round,
                        )
                        .unwrap()
                        .is_some()
                    })
                })
                .collect();
            let winners = handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>();
            assert_eq!(winners, 2, "round {round}");
        }
    }
}
