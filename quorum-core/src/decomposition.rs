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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginPlanning<'a> {
    pub source_task_id: i64,
    pub expected_revision: i64,
    pub provider: &'a str,
    pub model: &'a str,
    pub frozen_base_sha: &'a str,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBlocker<'a> {
    pub task_id: i64,
    pub reviewer: &'a str,
    pub category: &'a str,
    pub violated_boundary: &'a str,
    pub evidence: &'a [String],
    pub now: i64,
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
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL)",
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
         VALUES (?1,'freeze-requested',0,1,?2,?3,?4,?5,?6,?6)",
        params![
            input.source_task_id,
            input.expected_revision,
            input.provider,
            input.model,
            input.frozen_base_sha,
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
         WHERE id=?1 AND status='open' AND revision=?2 AND assignee IS NULL",
        params![input.source_task_id, input.expected_revision, input.now],
    )?;
    if changed != 1 {
        return Ok(None);
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(graph_id))
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
                 updated_at=?2 WHERE id=?1",
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
             accepted_plan_revision=plan_revision,updated_at=?2
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
    cancel_unfinished_members(&tx, graph_id, now)?;
    for intent in intents {
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
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
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
    if blocker.category.trim().is_empty()
        || blocker.category.len() > 128
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
    let graph: Option<(i64, i64)> = tx
        .query_row(
            "SELECT d.id,d.planned_source_revision
             FROM task_graph_members m
             JOIN task_decompositions d ON d.id=m.graph_id
             JOIN tasks child ON child.id=m.task_id
             JOIN tasks source ON source.id=d.source_task_id
             WHERE m.task_id=?1 AND m.active=1 AND d.state='active' AND d.active=1
               AND source.status='decomposed' AND child.status='in-review'
               AND child.reviewer=?2",
            params![blocker.task_id, blocker.reviewer],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((graph_id, source_revision)) = graph else {
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
    tx.execute(
        "UPDATE run_capabilities SET revoked_at=?2
         WHERE task_id=?1 AND role='reviewer' AND revoked_at IS NULL",
        params![blocker.task_id, blocker.now],
    )?;
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
        "all generated children merged",
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

    fn child(key: &str, deps: &[&str]) -> PlannedChild {
        PlannedChild {
            local_key: key.into(), title: key.into(), body: format!("deliver {key}"), labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: deps.iter().map(|s| (*s).into()).collect(),
            source_dependency_ids: vec![],
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
        let state: (String, i64, i64) = conn
            .query_row(
                "SELECT state,active,freeze_active FROM task_decompositions WHERE id=?1",
                [graph],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, ("active".into(), 1, 0));
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
    fn graph_blocker_is_atomic_and_stale_signal_is_clean() {
        let mut conn = setup();
        let graph = begin(&mut conn);
        let ids = materialize_graph(&mut conn, graph, 1, &[child("a", &[]), child("b", &[])], 4)
            .unwrap()
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='in-review',reviewer='r',assignee='r' WHERE id=?1",
            [ids[0]],
        )
        .unwrap();
        let evidence = vec!["diff moves sibling-owned schema work into this child".into()];
        assert!(block_graph(
            &mut conn,
            &GraphBlocker {
                task_id: ids[0],
                reviewer: "r",
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
