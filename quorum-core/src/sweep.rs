//! Physical reclamation of expired rows.
//!
//! Expiry is *logical* first — every read filters `expires_at > now`, so expired rows are
//! invisible immediately. This module is housekeeping that reclaims the disk. [`sweep_on_write`]
//! is the bounded, opportunistic sweep every mutation runs; [`sweep_all`] is the unbounded
//! explicit sweep (`quorum sweep`) plus a WAL checkpoint.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

/// Done tasks are reclaimed this long after entering `done`. Default; Phase 6 config overrides.
pub const DONE_TASK_TTL_SECS: i64 = 7 * 24 * 3600;

/// Durable token telemetry outlives task reclamation and is physically swept
/// after a bounded retention window.
pub const TOKEN_USAGE_RETENTION_SECS: i64 = 30 * 24 * 3600;

/// Max rows reclaimed per table by an opportunistic sweep-on-write.
pub const SWEEP_LIMIT: usize = 100;

/// Short grace window (seconds) for rework tasks between the VerdictChanges
/// lease release and the remediation worker's claim installation. The reaper
/// skips rework tasks updated within this window. Not a substitute for the
/// lease — the daemon must install the remediation claim promptly.
pub const REWORK_PROVISIONING_GRACE_SECS: i64 = 60;

/// Reaper: return any `working` or `rework` task whose lease has lapsed (no active, unexpired
/// lease on `task#<id>`) back to `open`, clearing the assignee, and emit a `task_reclaimed`
/// event per task to the event log.
pub fn reap_lapsed_tasks(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    // sweep_on_write and sweep_all already call us inside their write
    // transaction. Direct callers do not, so own an IMMEDIATE transaction in
    // that case: parking a review-only remediation updates the task, note,
    // lease, event, and owner alert as one indivisible state change.
    if conn.is_autocommit() {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
        reap_lapsed_tasks_in_tx(&tx, now, limit)?;
        tx.commit()?;
        return Ok(());
    }
    reap_lapsed_tasks_in_tx(conn, now, limit)
}

fn reap_lapsed_tasks_in_tx(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    // (id, assignee, status, review_only, had_any_lease_ever, refs)
    #[allow(clippy::type_complexity)]
    let lapsed: Vec<(i64, Option<String>, String, bool, bool, Option<String>)> = {
        let mut stmt = conn.prepare(
            "SELECT t.id, t.assignee, t.status, t.review_only,
                    EXISTS(SELECT 1 FROM claims c WHERE c.target = 'task#' || t.id) AS had_lease,
                    t.refs
             FROM tasks t
             WHERE t.status IN ('working', 'rework') AND NOT EXISTS (
                 SELECT 1 FROM claims c
                 WHERE c.target = 'task#' || t.id AND c.active=1 AND c.expires_at > ?1
             )
             AND (
                 t.status != 'rework'
                 OR CASE
                     WHEN json_valid(t.refs)
                     THEN COALESCE(
                         json_extract(t.refs, '$.ci_remediation_requested'),
                         0
                     )
                     ELSE 0
                 END != 1
             )
             AND NOT (
                 t.status = 'rework'
                 AND t.assignee IS NULL
                 AND json_valid(t.refs)
                 AND (
                     COALESCE(json_type(t.refs, '$.daemon_rework_retry_requested')='true', 0)
                     OR CASE WHEN json_type(t.refs, '$.runner_retry') IS NOT NULL
                         THEN COALESCE(
                             json_type(t.refs, '$.runner_retry.requested')='true', 0
                         )
                         ELSE COALESCE(
                             json_type(t.refs, '$.codex_retry_requested')='true', 0
                         )
                     END
                 )
             )
             AND NOT (status = 'rework' AND updated_at > ?1 - ?3)
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(
                params![now, limit as i64, REWORK_PROVISIONING_GRACE_SECS],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get::<_, i64>(3)? != 0,
                        r.get::<_, i64>(4)? != 0,
                        r.get(5)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    for (id, prev, status, review_only, had_lease, refs) in &lapsed {
        let target = format!("task#{id}");
        let reason = if *had_lease {
            "lease lapsed"
        } else {
            "no lease installed"
        };
        if *review_only && status == "rework" {
            // A lapsed remediation lease must not hand the task back to
            // review: the replacement reviewer re-judges the unchanged PR
            // head and its changes verdict burns a rework round with zero
            // remediation applied. Park instead (same contract as the
            // lifecycle layer); the resulting terminal park is owner-gated.
            let park_reason = format!("remediation {reason}");
            let parked_refs =
                crate::tasks::set_parked_refs(refs.as_deref(), &park_reason, "rework")?;
            conn.execute(
                "UPDATE tasks SET status='failed', assignee=NULL, refs=?2, updated_at=?3 WHERE id=?1",
                params![id, parked_refs, now],
            )?;
            conn.execute(
                "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'daemon', ?3)",
                params![id, now, format!("parked: {park_reason}")],
            )?;
            conn.execute(
                "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
                params![target],
            )?;
            crate::events::emit(conn, "task_parked", &target, &park_reason, now)?;
            crate::tasks::alert_owner_of_park(conn, *id, &park_reason, now)?;
            crate::decomposition::block_graph_if_child_failed(conn, *id, &park_reason, now)?;
            continue;
        }
        // Only implementation tasks reach here: review_only tasks are never
        // `working` (they enter the lifecycle at in-review), so the park
        // branch above already consumed every review_only row this query can
        // return.
        let preserve_rework = status == "rework"
            && refs
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .is_some_and(|value| {
                    value
                        .get(crate::tasks::PARKED_REWORK_RETRY_REF)
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                        || crate::runner_state::retry_requested(&value)
                });
        let recovered_status = if preserve_rework { "rework" } else { "open" };
        conn.execute(
            "UPDATE tasks SET status=?1, assignee=NULL, updated_at=?2 WHERE id=?3",
            params![recovered_status, now, id],
        )?;
        // Clear any lingering (now-expired) lease row so the next claim starts clean.
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![target],
        )?;
        let body = match prev {
            Some(a) => format!("reclaimed from {a} ({reason}) → {recovered_status}"),
            None => format!("reclaimed ({reason}) → {recovered_status}"),
        };
        crate::events::emit(conn, "task_reclaimed", &target, &body, now)?;
    }
    Ok(())
}

fn delete_bounded(conn: &Connection, table: &str, now: i64, limit: usize) -> Result<()> {
    // `table` is always a string literal from this module — never user input.
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN \
         (SELECT rowid FROM {table} WHERE expires_at <= ?1 LIMIT ?2)"
    );
    conn.execute(&sql, params![now, limit as i64])?;
    Ok(())
}

pub(crate) fn sweep_token_usage_on_write(conn: &Connection, now: i64) -> Result<()> {
    delete_token_usage_bounded(conn, now, SWEEP_LIMIT)
}

fn delete_token_usage_bounded(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    let cutoff = now.saturating_sub(TOKEN_USAGE_RETENTION_SECS);
    conn.execute(
        "DELETE FROM token_usage_run_tasks WHERE run_id IN (
             SELECT r.id FROM token_usage_runs r
             WHERE r.recorded_at <= ?1
               AND (r.agent_run_id IS NULL OR NOT EXISTS (
                   SELECT 1 FROM agent_runs a
                   WHERE a.id=r.agent_run_id AND a.ended_at IS NULL
               ))
             ORDER BY r.id LIMIT ?2
         )",
        params![cutoff, limit as i64],
    )?;
    conn.execute(
        "DELETE FROM token_usage_runs WHERE id IN (
             SELECT r.id FROM token_usage_runs r
             WHERE r.recorded_at <= ?1
               AND (r.agent_run_id IS NULL OR NOT EXISTS (
                   SELECT 1 FROM agent_runs a
                   WHERE a.id=r.agent_run_id AND a.ended_at IS NULL
               ))
             ORDER BY r.id LIMIT ?2
         )",
        params![cutoff, limit as i64],
    )?;
    Ok(())
}

fn delete_all_expired_token_usage(conn: &Connection, now: i64) -> Result<()> {
    let cutoff = now.saturating_sub(TOKEN_USAGE_RETENTION_SECS);
    conn.execute(
        "DELETE FROM token_usage_run_tasks WHERE run_id IN (
             SELECT r.id FROM token_usage_runs r
             WHERE r.recorded_at <= ?1
               AND (r.agent_run_id IS NULL OR NOT EXISTS (
                   SELECT 1 FROM agent_runs a
                   WHERE a.id=r.agent_run_id AND a.ended_at IS NULL
               ))
         )",
        params![cutoff],
    )?;
    conn.execute(
        "DELETE FROM token_usage_runs AS r
         WHERE r.recorded_at <= ?1
           AND (r.agent_run_id IS NULL OR NOT EXISTS (
               SELECT 1 FROM agent_runs a
               WHERE a.id=r.agent_run_id AND a.ended_at IS NULL
           ))",
        params![cutoff],
    )?;
    Ok(())
}

/// Delete, at most `limit` rows at a time, from task-owned operational tables whose task has
/// passed the done-task retention horizon. Keeping each child delete independently bounded is
/// important: one old task can have far more than `limit` historical rows.
fn delete_reclaimable_task_rows_bounded(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    let eligible = "SELECT id FROM tasks WHERE status='done' AND updated_at < ?1";
    for table in [
        "agent_runs",
        "task_branches",
        "task_notes",
        "mailbox",
        "journal",
        "approvals",
        "reviewer_provision_attempts",
        "pr_targets",
    ] {
        let sql = format!(
            "DELETE FROM {table} WHERE rowid IN \
             (SELECT rowid FROM {table} WHERE task_id IN ({eligible}) LIMIT ?2)"
        );
        conn.execute(&sql, params![now - DONE_TASK_TTL_SECS, limit as i64])?;
    }
    // Deliveries are keyed through their task message, rather than directly by task ID.
    conn.execute(
        "DELETE FROM task_message_deliveries WHERE rowid IN \
         (SELECT d.rowid FROM task_message_deliveries d
          JOIN task_messages m ON m.id=d.message_id
          WHERE m.task_id IN (SELECT id FROM tasks WHERE status='done' AND updated_at < ?1)
          LIMIT ?2)",
        params![now - DONE_TASK_TTL_SECS, limit as i64],
    )?;
    conn.execute(
        "DELETE FROM task_messages WHERE rowid IN \
         (SELECT rowid FROM task_messages
          WHERE task_id IN (SELECT id FROM tasks WHERE status='done' AND updated_at < ?1)
            AND NOT EXISTS (SELECT 1 FROM task_message_deliveries d WHERE d.message_id=task_messages.id)
          LIMIT ?2)",
        params![now - DONE_TASK_TTL_SECS, limit as i64],
    )?;
    Ok(())
}

/// Remove bounded, already-orphaned operational rows left by older sweep versions. Durable
/// analytics and dead-letter evidence intentionally do not participate: review audits/findings,
/// run capabilities, and capped review-interpret jobs remain operator-visible after task
/// reclamation.
fn delete_orphaned_task_rows_bounded(conn: &Connection, limit: usize) -> Result<()> {
    for table in [
        "agent_runs",
        "task_branches",
        "task_notes",
        "mailbox",
        "journal",
        "approvals",
        "reviewer_provision_attempts",
        "pr_targets",
    ] {
        let sql = format!(
            "DELETE FROM {table} WHERE rowid IN \
             (SELECT rowid FROM {table} WHERE task_id IS NOT NULL \
              AND NOT EXISTS (SELECT 1 FROM tasks WHERE tasks.id={table}.task_id) LIMIT ?1)"
        );
        conn.execute(&sql, params![limit as i64])?;
    }
    conn.execute(
        "DELETE FROM task_messages WHERE rowid IN \
         (SELECT rowid FROM task_messages WHERE NOT EXISTS \
          (SELECT 1 FROM tasks WHERE tasks.id=task_messages.task_id) LIMIT ?1)",
        params![limit as i64],
    )?;
    // Remove deliveries after their orphaned message parents so one explicit sweep reclaims
    // both levels of legacy task-message data.
    conn.execute(
        "DELETE FROM task_message_deliveries WHERE rowid IN \
         (SELECT d.rowid FROM task_message_deliveries d
          WHERE NOT EXISTS (SELECT 1 FROM task_messages m WHERE m.id=d.message_id)
          LIMIT ?1)",
        params![limit as i64],
    )?;
    Ok(())
}

/// Durable schema tables whose `REFERENCES tasks(id)` columns pin the task row.
/// Sweep must retain the task while any of these still reference it — deleting
/// the provenance to make GC succeed would silently discard decomposition and
/// review-follow-up history. Add here whenever a new durable FK to tasks(id) is
/// introduced; the FK inventory test below fails when a new one is missed.
const DURABLE_TASK_REF_TABLES: &[(&str, &str)] = &[
    ("cancelled_dependency_reconciliation", "cancelled_task_id"),
    // A collaboration group owns its shared retention window. The task stays
    // present until that group expires and bounded housekeeping removes the
    // operation children before their parent attempt.
    ("collaboration_attempts", "task_id"),
    ("github_operations", "task_id"),
    ("task_decompositions", "source_task_id"),
    ("task_graph_members", "task_id"),
    ("decomposition_cleanup", "task_id"),
    ("reviewer_provision_reservations", "task_id"),
    ("review_followup_batches", "task_id"),
    ("review_followup_batches", "source_task_id"),
    ("review_followup_artifacts", "linked_task_id"),
    ("review_followup_artifacts", "created_task_id"),
    ("review_followup_assessments", "source_task_id"),
];

/// Operational task-owned tables whose rows are reclaimed alongside the parent
/// task. The parent delete waits until each of these is empty for the task, so
/// bounded child sweeps drive the reclamation instead of one giant cascade.
const OPERATIONAL_TASK_REF_TABLES: &[&str] = &[
    "agent_runs",
    "task_branches",
    "task_notes",
    "task_messages",
    "mailbox",
    "journal",
    "approvals",
    "reviewer_provision_attempts",
    "pr_targets",
];

/// Build the operational + durable guard predicates that keep sweep from
/// deleting a task while any child row still references it. Guards reference
/// `tasks.id` so the caller composes them into a `DELETE FROM tasks` shape.
///
/// The two families are:
///   • operational — bounded child cleanup drains these each sweep, so the parent waits
///     until the fanout is fully swept rather than letting one write delete unbounded rows.
///   • durable — decomposition and review-follow-up provenance is intentionally retained;
///     these tables enforce FKs on tasks(id), so deleting a still-referenced task raises
///     `FOREIGN KEY constraint failed` and turns every subsequent mutation on the DB into
///     an exit-3 failure (schema-46 regression, see task #395).
fn task_ref_guard_predicates() -> String {
    let mut guards = String::new();
    for table in OPERATIONAL_TASK_REF_TABLES {
        guards.push_str(&format!(
            " AND NOT EXISTS (SELECT 1 FROM {table} x WHERE x.task_id=tasks.id)"
        ));
    }
    for (table, column) in DURABLE_TASK_REF_TABLES {
        guards.push_str(&format!(
            " AND NOT EXISTS (SELECT 1 FROM {table} x WHERE x.{column}=tasks.id)"
        ));
    }
    guards
}

/// Primary-key page used to drive opportunistic task reclamation. Keep age,
/// status, and reference predicates out of this query: adding them can make
/// SQLite choose a secondary index and sort every qualifying historical row
/// before applying `LIMIT`, which defeats the writer-lock bound.
const RECLAIMABLE_TASK_WINDOW_SQL: &str =
    "SELECT id FROM tasks WHERE id > ?1 ORDER BY id ASC LIMIT ?2";

/// Opportunistic per-mutation sweep: reclaim at most `limit` aged done tasks
/// whose operational children are drained and whose durable references are
/// gone.
///
/// The raw task-ID window is bounded by [`SWEEP_LIMIT`] *before* age, status,
/// or guard predicates apply. `sweep_cursors('reclaimable_tasks')` supplies a
/// rotating primary-key starting point (`id > cursor`), and the exact query is
/// pinned by a query-plan regression below. Ever-growing retained history can
/// therefore never inflate per-mutation candidate discovery: each task ID
/// consumes one slot in one window, then the cursor advances past it. A short
/// tail window resets the cursor to 0 immediately so tasks that become eligible
/// after an earlier pass are revisited without starvation.
///
/// Task #395 acceptance boundary: candidate discovery is one bounded primary-
/// key range page, and at most `SWEEP_LIMIT` task IDs reach the fixed guard set.
fn delete_reclaimable_tasks_bounded(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    let cursor: i64 = conn
        .query_row(
            "SELECT value FROM sweep_cursors WHERE name='reclaimable_tasks'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);

    let candidates: Vec<i64> = conn
        .prepare(RECLAIMABLE_TASK_WINDOW_SQL)?
        .query_map(params![cursor, limit as i64], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // Advance after a full window. A short window proves this primary-key page
    // reached the current tail, so wrap immediately and revisit the beginning
    // on the next write. This remains fair even while new tasks are appended.
    // A no-op sweep (no candidates and cursor already 0) leaves the cursor
    // alone so a fresh DB does not accumulate a sweep_cursors row before it
    // has anything to reclaim.
    let new_cursor: Option<i64> = if let Some(&max) = candidates.last() {
        Some(if candidates.len() < limit { 0 } else { max })
    } else if cursor > 0 {
        Some(0)
    } else {
        None
    };
    if let Some(v) = new_cursor {
        conn.execute(
            "INSERT INTO sweep_cursors(name, value) VALUES ('reclaimable_tasks', ?1)
             ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            [v],
        )?;
    }

    if candidates.is_empty() {
        return Ok(());
    }

    // Apply eligibility and guards only to the bounded raw-ID window. Per-row
    // durable NOT EXISTS probes are served by the indexes materialized in
    // schema.sql; no age/status predicate participates in candidate discovery.
    let guards = task_ref_guard_predicates();
    let placeholders = candidates.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "DELETE FROM tasks
         WHERE id IN ({placeholders})
           AND status='done' AND updated_at < ?{guards}"
    );
    let horizon = now - DONE_TASK_TTL_SECS;
    let mut bound: Vec<&dyn rusqlite::ToSql> = candidates
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();
    bound.push(&horizon);
    conn.execute(&sql, bound.as_slice())?;
    Ok(())
}

/// Explicit unbounded reclamation used by `quorum sweep`. No candidate cap,
/// no cursor — every aged done task whose guards pass is deleted in one
/// statement. Guard probes stay indexed; SQLite's LIMIT-free DELETE avoids
/// the SQLITE_MAX_VARIABLE_NUMBER cap that a materialized `IN (...)` would
/// hit at scale. Resets the opportunistic cursor so subsequent sweep-on-write
/// mutations restart from the beginning after a full explicit sweep.
fn delete_reclaimable_tasks_unbounded(conn: &Connection, now: i64) -> Result<()> {
    let guards = task_ref_guard_predicates();
    let sql = format!("DELETE FROM tasks WHERE status='done' AND updated_at < ?1{guards}");
    conn.execute(&sql, params![now - DONE_TASK_TTL_SECS])?;
    conn.execute(
        "DELETE FROM sweep_cursors WHERE name='reclaimable_tasks'",
        [],
    )?;
    Ok(())
}

/// Park unleased open or rework tasks whose dependencies cannot currently be satisfied: every
/// dep is terminal (done/failed/cancelled) but at least one is failed or cancelled. They stay
/// excluded from provisioning until an explicit retry, without losing their dependency context.
pub fn cascade_dead_deps(conn: &Connection, now: i64, limit: usize) -> Result<usize> {
    // sweep_on_write and sweep_all already call us inside their write
    // transaction. Direct callers must make the terminal task update, note,
    // event, and owner alert equally indivisible.
    if conn.is_autocommit() {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
        let count = cascade_dead_deps_in_tx(&tx, now, limit)?;
        tx.commit()?;
        return Ok(count);
    }
    cascade_dead_deps_in_tx(conn, now, limit)
}

fn cascade_dead_deps_in_tx(conn: &Connection, now: i64, limit: usize) -> Result<usize> {
    // Pick the failing dep to name in the park reason. A cancelled dep is
    // terminal-terminal — the dependent can never dispatch without operator
    // disposition — so it wins over a merely-failed dep, which may still be
    // retried into `done`. Selecting a specific failed/cancelled dep also
    // avoids the pre-existing hazard of naming a `done` sibling dep just
    // because it happened to come first in the json_each iteration.
    let doomed: Vec<(i64, Option<i64>, Option<i64>, String)> = {
        let mut stmt = conn.prepare(
            "SELECT t.id,
                    (SELECT j.value FROM json_each(t.depends_on) j
                     JOIN tasks d ON d.id = j.value
                     WHERE d.status IN ('cancelled')
                     ORDER BY j.value LIMIT 1) AS cancelled_dep,
                    (SELECT j.value FROM json_each(t.depends_on) j
                     JOIN tasks d ON d.id = j.value
                     WHERE d.status IN ('failed')
                     ORDER BY j.value LIMIT 1) AS failed_dep,
                    t.status
             FROM tasks t
             WHERE t.status IN ('open', 'rework')
               AND t.depends_on IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM claims c
                   WHERE c.target = 'task#' || t.id AND c.active=1 AND c.expires_at > ?1
               )
               -- every dep is terminal …
               AND NOT EXISTS (
                   SELECT 1 FROM json_each(t.depends_on) j2
                   LEFT JOIN tasks d ON d.id = j2.value
                   WHERE d.status NOT IN ('done','failed','cancelled')
                      OR d.id IS NULL
               )
               -- … and at least one dep is NOT done
               AND EXISTS (
                   SELECT 1 FROM json_each(t.depends_on) j3
                   JOIN tasks d ON d.id = j3.value
                   WHERE d.status IN ('failed','cancelled')
               )
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![now, limit as i64], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    let mut count = 0usize;
    for (task_id, cancelled_dep, failed_dep, resume_status) in &doomed {
        let (named_dep, unsatisfiable) = match (cancelled_dep, failed_dep) {
            (Some(id), _) => (*id, true),
            (None, Some(id)) => (*id, false),
            // WHERE-clause already guarantees at least one failed/cancelled dep.
            (None, None) => continue,
        };
        let target = format!("task#{task_id}");
        let reason = if unsatisfiable {
            format!("dependency #{named_dep} is cancelled — unsatisfiable")
        } else {
            format!("dependency #{named_dep} is terminal-not-done")
        };
        conn.execute(
            "UPDATE tasks
             SET status='failed',
                 assignee=NULL,
                 refs=json_set(
                     COALESCE(refs, '{}'),
                     '$.daemon_parked', json('true'),
                     '$.daemon_parked_reason', ?1,
                     '$.daemon_parked_unsatisfiable', json(?5),
                     '$.daemon_resume_status', ?2
                 ),
                 updated_at=?3
             WHERE id=?4",
            params![
                reason,
                resume_status,
                now,
                task_id,
                if unsatisfiable { "true" } else { "false" },
            ],
        )?;
        crate::decomposition::block_graph_if_child_failed(conn, *task_id, &reason, now)?;
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![target],
        )?;
        conn.execute(
            "INSERT INTO task_notes(task_id, ts, agent, body)
             VALUES (?1, ?2, 'daemon', ?3)",
            params![task_id, now, format!("parked: {reason}")],
        )?;
        crate::events::emit(conn, "task_parked", &target, &reason, now)?;
        crate::tasks::alert_owner_of_park(conn, *task_id, &reason, now)?;
        count += 1;
    }
    Ok(count)
}

/// Bounded sweep run opportunistically inside every mutation's transaction. The `LIMIT`
/// keeps a large backlog from making one command's transaction pathologically long.
pub fn sweep_on_write(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    // Correctness first: reclaim lost-agent tasks before the housekeeping deletes (a lapsed
    // `claimed` task must become re-claimable on the next write).
    reap_lapsed_tasks(conn, now, limit)?;
    cascade_dead_deps(conn, now, limit)?;
    crate::tasks::converge_cancelled_dependency_reconciliation(conn, now, limit)?;
    delete_bounded(conn, "messages", now, limit)?;
    delete_bounded(conn, "events", now, limit)?;
    delete_bounded(conn, "errors", now, limit)?;
    // Deletes expired claims of any `active` value: an expired `active=1` row is already
    // logically dead (the read-filter hid it), and removing it just frees the partial index.
    delete_bounded(conn, "claims", now, limit)?;
    // Issue #101 (experimental PostToolUse hook): TTL the activity-stats tables
    // alongside the rest. Both are stats-only — losing rows past TTL is by design.
    delete_bounded(conn, "agent_sessions", now, limit)?;
    delete_bounded(conn, "activity_events", now, limit)?;
    delete_token_usage_bounded(conn, now, limit)?;
    // A GitHub operation's retention is owned by its collaboration attempt,
    // so child operations must be swept before their parent attempt.
    delete_bounded(conn, "github_operations", now, limit)?;
    delete_bounded(conn, "collaboration_attempts", now, limit)?;
    crate::task_messages::expire_stale_deliveries(conn, now, limit)?;
    delete_bounded(conn, "task_messages", now, limit)?;
    delete_reclaimable_task_rows_bounded(conn, now, limit)?;
    delete_orphaned_task_rows_bounded(conn, limit)?;
    delete_reclaimable_tasks_bounded(conn, now, limit)?;
    Ok(())
}

/// Unbounded sweep + `wal_checkpoint(TRUNCATE)`. Backs `quorum sweep`.
pub fn sweep_all(conn: &Connection, now: i64) -> Result<()> {
    // Keep task children and their parent atomic here too. Sweep-on-write already receives its
    // caller's write transaction; explicit sweep owns this transaction itself.
    // `unchecked_transaction()` defaults to BEGIN DEFERRED. This sweep reads while reaping
    // before its first delete, so a concurrent writer could otherwise commit between those
    // operations and make SQLite reject the later upgrade with SQLITE_BUSY_SNAPSHOT.
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    reap_lapsed_tasks(&tx, now, usize::MAX)?;
    cascade_dead_deps(&tx, now, usize::MAX)?;
    while tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM cancelled_dependency_reconciliation)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        crate::tasks::converge_cancelled_dependency_reconciliation(
            &tx,
            now,
            crate::tasks::CONVERGE_LIMIT,
        )?;
    }
    tx.execute("DELETE FROM messages WHERE expires_at <= ?1", params![now])?;
    tx.execute("DELETE FROM events WHERE expires_at <= ?1", params![now])?;
    tx.execute("DELETE FROM errors WHERE expires_at <= ?1", params![now])?;
    tx.execute("DELETE FROM claims WHERE expires_at <= ?1", params![now])?;
    // Issue #101 — see sweep_on_write for rationale.
    tx.execute(
        "DELETE FROM agent_sessions WHERE expires_at <= ?1",
        params![now],
    )?;
    tx.execute(
        "DELETE FROM activity_events WHERE expires_at <= ?1",
        params![now],
    )?;
    delete_all_expired_token_usage(&tx, now)?;
    tx.execute(
        "DELETE FROM github_operations WHERE expires_at <= ?1",
        params![now],
    )?;
    tx.execute(
        "DELETE FROM collaboration_attempts WHERE expires_at <= ?1",
        params![now],
    )?;
    crate::task_messages::expire_stale_deliveries(&tx, now, usize::MAX)?;
    tx.execute(
        "DELETE FROM task_messages WHERE expires_at <= ?1",
        params![now],
    )?;
    let unbounded = i64::MAX as usize;
    delete_reclaimable_task_rows_bounded(&tx, now, unbounded)?;
    delete_orphaned_task_rows_bounded(&tx, unbounded)?;
    delete_reclaimable_tasks_unbounded(&tx, now)?;
    tx.commit()?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn ready_claim(c: &mut Connection, agent: &str, task_id: i64, ttl: i64, now: i64) {
        crate::classify::store_classifications(
            c,
            &[crate::classify::TaskClassification {
                task_id,
                cx_est: 3,
                size: "M".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "unit-test:v2",
            now,
        )
        .unwrap();
        crate::tasks::claim(c, agent, Some(task_id), &[], ttl, now).unwrap();
    }

    fn active_graph_rework_child(c: &mut Connection, refs: Option<&str>) -> (i64, i64) {
        let source = crate::tasks::create(
            c,
            "owner",
            "decomposition source",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let graph = crate::decomposition::begin_planning(
            c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 1000,
            },
        )
        .unwrap()
        .unwrap();
        assert!(crate::decomposition::set_frozen_phase(
            c,
            graph,
            "freeze-requested",
            "preclassifying",
            None,
            1000,
        )
        .unwrap());
        let children = ["a", "b"].map(|key| crate::decomposition::PlannedChild {
            local_key: key.into(),
            title: format!("child {key}"),
            body: format!("deliver {key}"),
            labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: vec![],
            source_dependency_ids: vec![],
        });
        let child = crate::decomposition::materialize_graph(c, graph, 1, &children, 1000)
            .unwrap()
            .unwrap()[0];
        c.execute(
            "UPDATE tasks SET status='rework',review_only=1,assignee='W1',reviewer='R1',refs=?2,
                    updated_at=1000 WHERE id=?1",
            params![child, refs],
        )
        .unwrap();
        c.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active)
             VALUES (?1,'W1',1000,1050,1)",
            params![format!("task#{child}")],
        )
        .unwrap();
        (graph, child)
    }

    #[test]
    fn sweep_removes_expired_keeps_live() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO messages(ts,author,topic,kind,body,expires_at)
             VALUES (1,'a','hub','info','expired',10), (1,'a','hub','info','live',9999)",
            [],
        )
        .unwrap();
        sweep_on_write(&c, 100, 100).unwrap();
        let bodies: Vec<String> = c
            .prepare("SELECT body FROM messages ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(bodies, vec!["live".to_string()]);
    }

    #[test]
    fn usage_outlives_task_reclamation_then_full_sweep_drains_populated_history() {
        let (_d, mut c) = open_tmp();
        let task_id = reclaimable_task(&mut c, "historic telemetry");
        for ordinal in 0..=(SWEEP_LIMIT as i64) {
            crate::token_usage::record(
                &mut c,
                None,
                "classifier",
                &[task_id],
                None,
                "codex",
                "gpt",
                "medium",
                crate::token_usage::TokenUsage {
                    uncached_input_tokens: ordinal,
                    output_tokens: 1,
                    ..Default::default()
                },
                1,
            )
            .unwrap();
        }

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM tasks WHERE id=?1", [task_id], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            0,
            "the completed task should be reclaimed after seven days"
        );
        assert_eq!(
            crate::token_usage::for_task(&c, task_id).unwrap().len(),
            SWEEP_LIMIT + 1,
            "usage mappings must remain queryable through the longer retention window"
        );

        sweep_all(&c, TOKEN_USAGE_RETENTION_SECS + 1).unwrap();
        for table in ["token_usage_run_tasks", "token_usage_runs"] {
            assert_eq!(
                c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
                0,
                "full sweep must drain all expired rows from {table}"
            );
        }
    }

    #[test]
    fn active_managed_usage_snapshot_is_not_expired_before_recovery_can_reload_it() {
        let (_d, mut c) = open_tmp();
        let task_id = crate::tasks::create(
            &mut c, "owner", "dormant", None, 0, None, None, None, None, 1,
        )
        .unwrap();
        let run_id =
            crate::agent_runs::insert(&c, task_id, "Dormant", "worker", "gpt", "high", "codex", 1)
                .unwrap();
        crate::token_usage::record(
            &mut c,
            Some(run_id),
            "worker",
            &[task_id],
            Some(464),
            "codex",
            "gpt",
            "high",
            crate::token_usage::TokenUsage {
                cached_input_tokens: 900,
                output_tokens: 20,
                ..Default::default()
            },
            1,
        )
        .unwrap();

        let after_retention = TOKEN_USAGE_RETENTION_SECS + 1;
        sweep_all(&c, after_retention).unwrap();
        assert!(crate::token_usage::usage_for_agent_run(&c, run_id)
            .unwrap()
            .is_some());

        crate::agent_runs::close(&c, run_id, after_retention, "done").unwrap();
        sweep_all(&c, after_retention).unwrap();
        assert!(crate::token_usage::usage_for_agent_run(&c, run_id)
            .unwrap()
            .is_none());
    }

    fn reclaimable_task(c: &mut Connection, title: &str) -> i64 {
        let id =
            crate::tasks::create(c, "boss", title, None, 0, None, None, None, None, 1).unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=0 WHERE id=?1",
            [id],
        )
        .unwrap();
        id
    }

    fn insert_run(c: &Connection, task_id: i64, agent: &str) -> i64 {
        crate::agent_runs::insert(c, task_id, agent, "worker", "model", "high", "codex", 1).unwrap()
    }

    #[test]
    fn sweep_reclaims_task_agent_runs_without_touching_live_task_runs() {
        let (_d, mut c) = open_tmp();
        let reclaimed = reclaimable_task(&mut c, "reclaim me");
        let live = crate::tasks::create(
            &mut c, "boss", "keep me", None, 0, None, None, None, None, 1,
        )
        .unwrap();
        insert_run(&c, reclaimed, "old-run");
        insert_run(&c, live, "live-run");

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();

        let old_count: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id=?1",
                [reclaimed],
                |r| r.get(0),
            )
            .unwrap();
        let live_count: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id=?1",
                [live],
                |r| r.get(0),
            )
            .unwrap();
        let reclaimed_task_count: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [reclaimed], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(old_count, 0, "task reclamation must remove its agent runs");
        assert_eq!(reclaimed_task_count, 0, "reclaimable task must be deleted");
        assert_eq!(live_count, 1, "must not delete runs for a live task");
    }

    #[test]
    fn sweep_reclaims_preexisting_orphan_agent_runs() {
        let (_d, c) = open_tmp();
        insert_run(&c, 99_999, "orphan-run");

        sweep_on_write(&c, 1, SWEEP_LIMIT).unwrap();

        let count: i64 = c
            .query_row("SELECT count(*) FROM agent_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "normal sweep must clean old orphaned runs");
    }

    #[test]
    fn sweep_all_reclaims_legacy_orphan_task_message_and_delivery_together() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO task_messages(task_id, sender_run_id, sender_agent, kind, body,
                 recipient_count, created_at, expires_at)
             VALUES (99999, 1, 'old-run', 'direct', 'legacy', 1, 1, 9999)",
            [],
        )
        .unwrap();
        let message_id = c.last_insert_rowid();
        c.execute(
            "INSERT INTO task_message_deliveries(message_id, recipient_run_id, recipient_agent,
                 created_at, updated_at)
             VALUES (?1, 2, 'recipient', 1, 1)",
            [message_id],
        )
        .unwrap();

        sweep_all(&c, 1).unwrap();

        let messages: i64 = c
            .query_row("SELECT count(*) FROM task_messages", [], |r| r.get(0))
            .unwrap();
        let deliveries: i64 = c
            .query_row("SELECT count(*) FROM task_message_deliveries", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(messages, 0, "legacy orphan task message must be reclaimed");
        assert_eq!(
            deliveries, 0,
            "its delivery must be reclaimed by the same explicit sweep"
        );
    }

    #[test]
    fn sweep_task_cleanup_is_bounded() {
        let (_d, mut c) = open_tmp();
        let task = reclaimable_task(&mut c, "many runs");
        insert_run(&c, task, "first");
        insert_run(&c, task, "second");

        sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, 1).unwrap();

        let runs_left: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id=?1",
                [task],
                |r| r.get(0),
            )
            .unwrap();
        let task_left: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [task], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            runs_left, 1,
            "agent-run cleanup must honor the per-sweep limit"
        );
        assert_eq!(
            task_left, 1,
            "parent waits until its bounded child cleanup finishes"
        );
    }

    #[test]
    fn sweep_retains_review_audits_for_stratum_coverage() {
        let (_d, mut c) = open_tmp();
        let task = reclaimable_task(&mut c, "audited task");
        c.execute(
            "INSERT INTO review_audits(task_id, pr_number, r1_run_id, r2_run_id,
               r1_reviewer, r2_reviewer, model, effort, cx_bucket, missed_count,
               overcaught_count, r1_verdict, created_at)
             VALUES (?1, 7, 1, 2, 'r1', 'r2', 'model', 'high', '2', 0, 0, 'approved', 1)",
            [task],
        )
        .unwrap();

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();

        let audit_count: i64 = c
            .query_row(
                "SELECT count(*) FROM review_audits WHERE task_id=?1",
                [task],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            audit_count, 1,
            "audit history must survive task reclamation"
        );
    }

    fn dead_lettered_interpret_job_for_reclaimable_task(c: &mut Connection) -> (i64, i64) {
        let task_id = reclaimable_task(c, "dead-lettered interpretation");
        let pr_number = 99_001;
        crate::review_interpret_jobs::enqueue(c, pr_number, task_id, None, "v1").unwrap();
        for _ in 0..crate::review_interpret_jobs::MAX_ATTEMPTS {
            crate::review_interpret_jobs::mark_error(c, pr_number, "collector failed").unwrap();
        }
        (task_id, pr_number)
    }

    fn assert_dead_letter_survives_task_reclamation(c: &Connection, task_id: i64, pr_number: i64) {
        let task_count: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [task_id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            task_count, 0,
            "the expired done task must still be reclaimable"
        );
        let dead = crate::review_interpret_jobs::over_cap(c).unwrap();
        assert_eq!(
            dead.len(),
            1,
            "dead letter must remain operator-visible after task sweep"
        );
        assert_eq!(dead[0].pr_number, pr_number);
        assert_eq!(dead[0].last_error.as_deref(), Some("collector failed"));
    }

    #[test]
    fn sweep_on_write_retains_dead_lettered_interpret_jobs_after_task_reclamation() {
        let (_d, mut c) = open_tmp();
        let (task_id, pr_number) = dead_lettered_interpret_job_for_reclaimable_task(&mut c);

        sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();

        assert_dead_letter_survives_task_reclamation(&c, task_id, pr_number);
    }

    #[test]
    fn sweep_all_retains_dead_lettered_interpret_jobs_after_task_reclamation() {
        let (_d, mut c) = open_tmp();
        let (task_id, pr_number) = dead_lettered_interpret_job_for_reclaimable_task(&mut c);

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();

        assert_dead_letter_survives_task_reclamation(&c, task_id, pr_number);
    }

    #[test]
    fn reaper_returns_lapsed_claimed_task_to_open_with_event() {
        let (_d, mut c) = open_tmp();
        // A claimed task with a short lease (dead at 1100).
        let id = crate::tasks::create(&mut c, "boss", "x", None, 0, None, None, None, None, 1000)
            .unwrap();
        ready_claim(&mut c, "A", id, 100, 1000);
        // Before expiry: reaper leaves it alone.
        reap_lapsed_tasks(&c, 1050, SWEEP_LIMIT).unwrap();
        assert_eq!(
            crate::tasks::get(&c, id).unwrap().unwrap().status,
            "working"
        );
        // After the lease lapses: reaper returns it to open, clears assignee, emits a
        // `task_reclaimed` event to the EVENT LOG (not the message feed).
        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        let target = format!("task#{id}");
        let evs = crate::events::list(&c, 0, Some(&target), 10, 1100).unwrap();
        let reclaimed = evs.iter().filter(|e| e.kind == "task_reclaimed").count();
        assert_eq!(reclaimed, 1, "exactly one task_reclaimed event");
        // The message feed is NOT polluted with auto-events.
        let msg_count: i64 = c
            .query_row(
                "SELECT count(*) FROM messages WHERE body LIKE '%reclaimed%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            msg_count, 0,
            "reaper events must NOT appear on the message feed"
        );
        // Idempotent: a now-open task is not reaped again (no duplicate event).
        reap_lapsed_tasks(&c, 1200, SWEEP_LIMIT).unwrap();
        let evs2 = crate::events::list(&c, 0, Some(&target), 10, 1200).unwrap();
        let reclaimed2 = evs2.iter().filter(|e| e.kind == "task_reclaimed").count();
        assert_eq!(
            reclaimed2, 1,
            "reaper must not re-fire on an already-open task"
        );
    }

    #[test]
    fn sweep_deletes_at_exact_expiry_boundary() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO messages(ts,author,topic,kind,body,expires_at)
             VALUES (1,'a','hub','info','boundary',100), (1,'a','hub','info','live',101)",
            [],
        )
        .unwrap();
        sweep_on_write(&c, 100, 100).unwrap();
        let bodies: Vec<String> = c
            .prepare("SELECT body FROM messages ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(bodies, vec!["live"], "expires_at == now must be swept");
    }

    #[test]
    fn sweep_all_deletes_at_exact_expiry_boundary() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO events(ts,kind,subject,body,expires_at)
             VALUES (1,'test','s','boundary',100), (1,'test','s','live',101)",
            [],
        )
        .unwrap();
        sweep_all(&c, 100).unwrap();
        let bodies: Vec<String> = c
            .prepare("SELECT body FROM events ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            bodies,
            vec!["live"],
            "sweep_all: expires_at == now must be swept"
        );
    }

    #[test]
    fn reaper_respects_limit() {
        let (_d, mut c) = open_tmp();
        let mut ids = Vec::new();
        for i in 0..5 {
            let id = crate::tasks::create(
                &mut c,
                "boss",
                &format!("task-{i}"),
                None,
                0,
                None,
                None,
                None,
                None,
                1000,
            )
            .unwrap();
            // Use DISTINCT agents per task so #55's auto-renew-on-touch doesn't extend
            // the prior agents' leases at each iteration (every claim() calls
            // agents::touch which renews the caller's OTHER active leases). With one
            // agent per task, each lease lapses independently at TTL=100 → all 5 lapse
            // at now=1100 as the reaper test expects.
            let agent = format!("A{i}");
            ready_claim(&mut c, &agent, id, 100, 1000);
            ids.push(id);
        }
        // All 5 leases lapse at 1100. Reap with limit=2: only 2 reaped.
        reap_lapsed_tasks(&c, 1100, 2).unwrap();
        let open: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE status='open'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let working: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks WHERE status='working'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(open, 2, "exactly 2 reaped to open");
        assert_eq!(working, 3, "3 remain working (limit respected)");
        // Second call reaps 2 more.
        reap_lapsed_tasks(&c, 1100, 2).unwrap();
        let open2: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE status='open'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(open2, 4, "4 total reaped after two batches");
        // Third call reaps the last 1.
        reap_lapsed_tasks(&c, 1100, 2).unwrap();
        let open3: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE status='open'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(open3, 5, "all 5 reaped after three batches");
    }

    #[test]
    fn cascade_parks_task_blocked_by_cancelled_dep() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        // Cancel the dependency.
        c.execute(
            "UPDATE tasks SET status='cancelled', updated_at=200 WHERE id=?1",
            params![dep],
        )
        .unwrap();
        // Before cascade: child is still open.
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "open"
        );
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 1);
        let t = crate::tasks::get(&c, child).unwrap().unwrap();
        assert_eq!(t.status, "failed", "child must be parked by cascade");
        let refs: serde_json::Value = serde_json::from_str(t.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["daemon_parked"], true);
        assert_eq!(refs["daemon_resume_status"], "open");
        // Event emitted.
        let target = format!("task#{child}");
        let evs = crate::events::list(&c, 0, Some(&target), 10, 300).unwrap();
        assert!(
            evs.iter().any(|e| e.kind == "task_parked"),
            "task_parked event must be emitted"
        );
        let alert: (String, String, String, String, i64, String) = c
            .query_row(
                "SELECT author, kind, body, refs, expires_at, recipient
                 FROM messages WHERE refs=?1",
                params![format!("task:{child}")],
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
            .expect("dependency park must alert the owner");
        assert_eq!(alert.0, "daemon");
        assert_eq!(alert.1, "alert");
        assert!(alert
            .2
            .contains(&format!("dependency #{dep} is cancelled — unsatisfiable")));
        assert!(alert.2.contains("quorum task-retry"));
        assert_eq!(refs["daemon_parked_unsatisfiable"], true);
        assert_eq!(alert.3, format!("task:{child}"));
        assert_eq!(
            alert.4,
            300 + crate::feed::DEFAULT_MESSAGE_TTL_SECS,
            "park alert uses the default message TTL"
        );
        assert_eq!(alert.5, "owner");
    }

    /// Task #473: a merely-failed dep (recoverable) parks with the
    /// terminal-not-done reason and daemon_parked_unsatisfiable=false; a
    /// cancelled dep (unsatisfiable) uses the "cancelled — unsatisfiable"
    /// reason and unsatisfiable=true. When both are present, cancelled wins
    /// because it is terminal-terminal and drives the operator disposition.
    #[test]
    fn cascade_reason_distinguishes_cancelled_from_failed_deps() {
        let (_d, mut c) = open_tmp();
        let failed_dep = crate::tasks::create(
            &mut c,
            "boss",
            "failed dep",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let cancelled_dep = crate::tasks::create(
            &mut c,
            "boss",
            "cancelled dep",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let failed_only = crate::tasks::create(
            &mut c,
            "boss",
            "child failed dep",
            None,
            0,
            None,
            None,
            Some(&format!("[{failed_dep}]")),
            None,
            100,
        )
        .unwrap();
        let mixed = crate::tasks::create(
            &mut c,
            "boss",
            "child mixed deps",
            None,
            0,
            None,
            None,
            Some(&format!("[{failed_dep},{cancelled_dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', updated_at=200 WHERE id=?1",
            params![failed_dep],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled', updated_at=200 WHERE id=?1",
            params![cancelled_dep],
        )
        .unwrap();
        assert_eq!(cascade_dead_deps(&c, 300, SWEEP_LIMIT).unwrap(), 2);

        let f = crate::tasks::get(&c, failed_only).unwrap().unwrap();
        let f_refs: serde_json::Value = serde_json::from_str(f.refs.as_deref().unwrap()).unwrap();
        assert_eq!(f_refs["daemon_parked"], true);
        assert_eq!(f_refs["daemon_parked_unsatisfiable"], false);
        assert_eq!(
            f_refs["daemon_parked_reason"],
            serde_json::Value::String(format!("dependency #{failed_dep} is terminal-not-done"))
        );

        let m = crate::tasks::get(&c, mixed).unwrap().unwrap();
        let m_refs: serde_json::Value = serde_json::from_str(m.refs.as_deref().unwrap()).unwrap();
        assert_eq!(m_refs["daemon_parked_unsatisfiable"], true);
        assert_eq!(
            m_refs["daemon_parked_reason"],
            serde_json::Value::String(format!(
                "dependency #{cancelled_dep} is cancelled — unsatisfiable"
            )),
            "cancelled dep must be preferred over failed sibling in the reason"
        );
    }

    /// Task #473 R6: the runtime convergence for
    /// failed→open→cancelled deps is exercised by
    /// `tasks::converge_parked_dependents_of_cancelled` — see
    /// `converge_parked_dependents_of_cancelled_upgrades_stale_park` in
    /// tasks.rs. The primary `cascade_dead_deps` no longer scans all failed
    /// tasks per mutation (R6 blocker 2 removed that unbounded pass); this
    /// module's remaining tests only cover the primary open/rework park.
    fn active_graph_with_dependency_park_child(
        c: &mut Connection,
        child_refs: Option<&str>,
    ) -> (i64, i64, i64) {
        let source =
            crate::tasks::create(c, "owner", "source", None, 0, None, None, None, None, 100)
                .unwrap();
        c.execute("UPDATE tasks SET status='decomposed' WHERE id=?1", [source])
            .unwrap();
        let dependency = crate::tasks::create(
            c,
            "owner",
            "failed dependency",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let child = crate::tasks::create(
            c,
            "owner",
            "generated child",
            None,
            0,
            None,
            child_refs,
            Some(&format!("[{dependency}]")),
            None,
            100,
        )
        .unwrap();
        let sibling = crate::tasks::create(
            c,
            "owner",
            "generated sibling",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 plan_revision,accepted_plan_revision,created_at,updated_at)
             VALUES (?1,'active',1,0,1,1,1,100,100)",
            [source],
        )
        .unwrap();
        let graph = c.last_insert_rowid();
        c.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (?1,?2,'child',1,1),(?1,?3,'sibling',1,1)",
            params![graph, child, sibling],
        )
        .unwrap();
        (graph, child, dependency)
    }

    #[test]
    fn dependency_park_blocks_active_generated_child_graph_in_same_transaction() {
        let (_d, mut c) = open_tmp();
        let (graph, child, dependency) = active_graph_with_dependency_park_child(&mut c, None);
        c.execute(
            "UPDATE tasks SET status='failed',updated_at=200 WHERE id=?1",
            [dependency],
        )
        .unwrap();

        assert_eq!(cascade_dead_deps(&c, 300, SWEEP_LIMIT).unwrap(), 1);

        let reason = format!("dependency #{dependency} is terminal-not-done");
        let aggregate: (String, String, String, i64) = c
            .query_row(
                "SELECT state,hold_code,hold_summary,updated_at
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(aggregate.0, "blocked");
        assert_eq!(aggregate.1, "generated-child-failed");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&aggregate.2).unwrap(),
            serde_json::json!({"affected_task": child, "reason": reason})
        );
        assert_eq!(aggregate.3, 300);
        let event: (String, String, String) = c
            .query_row(
                "SELECT kind,subject,body FROM events WHERE kind='task_graph_blocked'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(event.0, "task_graph_blocked");
        assert_eq!(event.1, format!("task#{child}"));
        assert!(event.2.contains(&reason));
        let graph_alerts: i64 = c
            .query_row(
                "SELECT count(*) FROM messages
                 WHERE author='daemon' AND kind='alert' AND recipient='owner'
                   AND refs=?1 AND body LIKE '%task graph blocked%'",
                [format!("task:{child}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(graph_alerts, 1);
    }

    #[test]
    fn dependency_park_with_active_retry_keeps_generated_child_graph_active() {
        for refs in [
            r#"{"runner_retry":{"requested":true}}"#,
            r#"{"codex_retry_requested":true}"#,
        ] {
            let (_d, mut c) = open_tmp();
            let (graph, child, dependency) =
                active_graph_with_dependency_park_child(&mut c, Some(refs));
            c.execute(
                "UPDATE tasks SET status='rework',updated_at=200 WHERE id=?1",
                [child],
            )
            .unwrap();
            c.execute(
                "UPDATE tasks SET status='failed',updated_at=200 WHERE id=?1",
                [dependency],
            )
            .unwrap();

            assert_eq!(cascade_dead_deps(&c, 300, SWEEP_LIMIT).unwrap(), 1);

            let state: (String, Option<String>, Option<String>) = c
                .query_row(
                    "SELECT state,hold_code,hold_summary FROM task_decompositions WHERE id=?1",
                    [graph],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(state, ("active".into(), None, None));
            let transitions: i64 = c
                .query_row(
                    "SELECT count(*) FROM events WHERE kind='task_graph_blocked'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(transitions, 0);
        }
    }

    #[test]
    fn cascade_park_rolls_back_when_owner_alert_fails() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', updated_at=200 WHERE id=?1",
            params![dep],
        )
        .unwrap();
        c.execute_batch(
            "CREATE TRIGGER fail_owner_alert
             BEFORE INSERT ON messages
             WHEN NEW.kind='alert' AND NEW.recipient='owner'
             BEGIN
                 SELECT RAISE(ABORT, 'injected owner alert failure');
             END;",
        )
        .unwrap();
        let target = format!("task#{child}");
        let notes_before: i64 = c
            .query_row(
                "SELECT count(*) FROM task_notes WHERE task_id=?1",
                params![child],
                |row| row.get(0),
            )
            .unwrap();
        let events_before: i64 = c
            .query_row(
                "SELECT count(*) FROM events WHERE subject=?1",
                params![target],
                |row| row.get(0),
            )
            .unwrap();
        let messages_before: i64 = c
            .query_row(
                "SELECT count(*) FROM messages WHERE refs=?1",
                params![format!("task:{child}")],
                |row| row.get(0),
            )
            .unwrap();

        let error = cascade_dead_deps(&c, 300, 100).unwrap_err();
        assert!(error.to_string().contains("injected owner alert failure"));
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "open",
            "the task update rolls back with the alert"
        );
        let notes_after: i64 = c
            .query_row(
                "SELECT count(*) FROM task_notes WHERE task_id=?1",
                params![child],
                |row| row.get(0),
            )
            .unwrap();
        let events_after: i64 = c
            .query_row(
                "SELECT count(*) FROM events WHERE subject=?1",
                params![target],
                |row| row.get(0),
            )
            .unwrap();
        let messages_after: i64 = c
            .query_row(
                "SELECT count(*) FROM messages WHERE refs=?1",
                params![format!("task:{child}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            notes_after, notes_before,
            "task note must roll back with the park"
        );
        assert_eq!(
            events_after, events_before,
            "event must roll back with the park"
        );
        assert_eq!(
            messages_after, messages_before,
            "alert must not be inserted when the park rolls back"
        );
    }

    #[test]
    fn cascade_ignores_task_with_done_dep() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=200 WHERE id=?1",
            params![dep],
        )
        .unwrap();
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 0, "done dep should not trigger cascade");
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "open"
        );
    }

    #[test]
    fn cascade_ignores_task_with_non_terminal_dep() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='rework' WHERE id=?1",
            params![child],
        )
        .unwrap();
        // dep is still open (non-terminal).
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 0, "non-terminal dep should not trigger cascade");
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "rework"
        );
    }

    #[test]
    fn cascade_handles_mixed_deps_one_cancelled_one_still_working() {
        let (_d, mut c) = open_tmp();
        let dep1 =
            crate::tasks::create(&mut c, "boss", "dep1", None, 0, None, None, None, None, 100)
                .unwrap();
        let dep2 =
            crate::tasks::create(&mut c, "boss", "dep2", None, 0, None, None, None, None, 100)
                .unwrap();
        let _child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep1},{dep2}]")),
            None,
            100,
        )
        .unwrap();
        // dep1 cancelled, dep2 still working.
        c.execute(
            "UPDATE tasks SET status='cancelled', updated_at=200 WHERE id=?1",
            params![dep1],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='working', updated_at=200 WHERE id=?1",
            params![dep2],
        )
        .unwrap();
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 0, "should not cascade when a dep is still non-terminal");
    }

    #[test]
    fn cascade_fires_when_all_deps_terminal_but_one_failed() {
        let (_d, mut c) = open_tmp();
        let dep1 =
            crate::tasks::create(&mut c, "boss", "dep1", None, 0, None, None, None, None, 100)
                .unwrap();
        let dep2 =
            crate::tasks::create(&mut c, "boss", "dep2", None, 0, None, None, None, None, 100)
                .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep1},{dep2}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=200 WHERE id=?1",
            params![dep1],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', updated_at=200 WHERE id=?1",
            params![dep2],
        )
        .unwrap();
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "failed"
        );
    }

    #[test]
    fn cascade_is_transitive() {
        let (_d, mut c) = open_tmp();
        let a = crate::tasks::create(&mut c, "boss", "a", None, 0, None, None, None, None, 100)
            .unwrap();
        let b = crate::tasks::create(
            &mut c,
            "boss",
            "b",
            None,
            0,
            None,
            None,
            Some(&format!("[{a}]")),
            None,
            100,
        )
        .unwrap();
        let ch = crate::tasks::create(
            &mut c,
            "boss",
            "c",
            None,
            0,
            None,
            None,
            Some(&format!("[{b}]")),
            None,
            100,
        )
        .unwrap();
        // Cancel a.
        c.execute(
            "UPDATE tasks SET status='cancelled', updated_at=200 WHERE id=?1",
            params![a],
        )
        .unwrap();
        // First cascade: b parked.
        cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(crate::tasks::get(&c, b).unwrap().unwrap().status, "failed");
        // c still open (b just became failed, need another sweep).
        // Second cascade: c parked.
        cascade_dead_deps(&c, 400, 100).unwrap();
        assert_eq!(crate::tasks::get(&c, ch).unwrap().unwrap().status, "failed");
    }

    // ── Review-only reaper recovery (table-driven) ─────────────────

    #[test]
    fn reaper_review_only_and_impl_destinations() {
        // Table-driven: implementation tasks recover to open; review_only
        // rework parks (failed + daemon_parked) — bouncing to in-review would
        // re-review the unchanged PR head and burn a rework round per bounce.
        struct Case {
            review_only: bool,
            expected_status: &'static str,
            label: &'static str,
        }
        let cases = [
            Case {
                review_only: false,
                expected_status: "open",
                label: "implementation→open",
            },
            Case {
                review_only: true,
                expected_status: "failed",
                label: "review_only rework→parked",
            },
        ];
        for case in &cases {
            let (_d, mut c) = open_tmp();
            let id = if case.review_only {
                let id = crate::tasks::create(
                    &mut c,
                    "boss",
                    "review task",
                    None,
                    0,
                    None,
                    None,
                    None,
                    Some(42),
                    1000,
                )
                .unwrap();
                // Move to rework to simulate VerdictChanges lifecycle path
                c.execute(
                    "UPDATE tasks SET status='rework', assignee='W1', reviewer='R1' WHERE id=?1",
                    params![id],
                )
                .unwrap();
                id
            } else {
                let id = crate::tasks::create(
                    &mut c,
                    "boss",
                    "impl task",
                    None,
                    0,
                    None,
                    None,
                    None,
                    None,
                    1000,
                )
                .unwrap();
                ready_claim(&mut c, "W1", id, 100, 1000);
                id
            };
            // Ensure a claim exists that will lapse
            if case.review_only {
                c.execute(
                    "INSERT INTO claims(target, holder, ts, expires_at, active) \
                     VALUES (?1, 'W1', 1000, 1100, 1)",
                    params![format!("task#{id}")],
                )
                .unwrap();
            }

            reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
            let t = crate::tasks::get(&c, id).unwrap().unwrap();
            assert_eq!(t.status, case.expected_status, "{}", case.label);
            assert!(
                t.assignee.is_none(),
                "{}: assignee must be cleared",
                case.label
            );

            let target = format!("task#{id}");
            let evs = crate::events::list(&c, 0, Some(&target), 10, 1100).unwrap();
            if case.review_only {
                // Parked, never reclaimed: durable owner-gated markers plus
                // task_parked event and loud owner alert.
                let refs: serde_json::Value =
                    serde_json::from_str(t.refs.as_deref().unwrap()).unwrap();
                assert_eq!(refs["daemon_parked"], true, "{}", case.label);
                assert_eq!(refs["daemon_resume_status"], "rework", "{}", case.label);
                assert!(
                    refs.get("daemon_parked_head_check").is_none(),
                    "{}",
                    case.label
                );
                let parked = &evs
                    .iter()
                    .find(|e| e.kind == "task_parked")
                    .expect("task_parked event missing")
                    .body;
                assert!(
                    parked.contains("lease lapsed"),
                    "{}: park event must say 'lease lapsed', got: {parked}",
                    case.label
                );
                assert!(
                    !evs.iter().any(|e| e.kind == "task_reclaimed"),
                    "{}: parked task must not also emit task_reclaimed",
                    case.label
                );
            } else {
                let body = &evs
                    .iter()
                    .find(|e| e.kind == "task_reclaimed")
                    .expect("task_reclaimed event missing")
                    .body;
                assert!(
                    body.contains(case.expected_status),
                    "{}: event body must report '{}'",
                    case.label,
                    case.expected_status
                );
                assert!(
                    body.contains("lease lapsed"),
                    "{}: event body must say 'lease lapsed'",
                    case.label
                );
            }
        }
    }

    #[test]
    fn reaper_review_only_rework_park_preserves_round_and_alerts_owner() {
        // A lapsed remediation lease parks (never bounces to review): the
        // round budget is untouched, provenance survives for `task-retry`,
        // and the failure is loud (owner alert).
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(
            &mut c,
            "boss",
            "review task",
            None,
            0,
            None,
            None,
            None,
            Some(42),
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='rework', assignee='W1', reviewer='R1', rework_round=2 WHERE id=?1",
            params![id],
        )
        .unwrap();
        c.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) \
             VALUES (?1, 'W1', 1000, 1050, 1)",
            params![format!("task#{id}")],
        )
        .unwrap();

        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "failed");
        assert!(t.review_only, "review_only flag must be preserved");
        assert_eq!(
            t.rework_round, 2,
            "infra failure must not consume a rework round"
        );
        assert!(t.assignee.is_none(), "assignee cleared by the park");
        let active_claims: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                params![format!("task#{id}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 0, "lapsed lease deactivated");
        // Loud failure: owner alert with the retry hint.
        let msgs = crate::feed::peek(&c, None, None, 10, 1100).unwrap();
        assert!(
            msgs.iter().any(|m| m.kind == "alert"
                && m.recipient.as_deref() == Some("owner")
                && m.body.contains("task-retry")),
            "owner alert with retry hint missing"
        );
        // Explicit retry resumes the remediation flow with the round intact.
        let resumed = crate::tasks::retry_parked(&mut c, id, "boss", true, 1200)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.status, "rework");
        assert_eq!(resumed.rework_round, 2);
    }

    #[test]
    fn reaper_park_blocks_active_graph_with_failed_child_and_alert() {
        let (_d, mut c) = open_tmp();
        let (graph, child) = active_graph_rework_child(&mut c, None);

        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();

        let graph_state: (String, i64, String, String) = c
            .query_row(
                "SELECT state,active,hold_code,hold_summary FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(graph_state.0, "blocked");
        assert_eq!(graph_state.1, 1);
        assert_eq!(graph_state.2, "generated-child-failed");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&graph_state.3).unwrap(),
            serde_json::json!({
                "affected_task": child,
                "reason": "remediation lease lapsed",
            })
        );
        let graph_event: (String, String) = c
            .query_row(
                "SELECT subject,body FROM events WHERE kind='task_graph_blocked'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(graph_event.0, format!("task#{child}"));
        assert!(graph_event.1.contains(&format!("task #{child}")));
        let alerts: i64 = c
            .query_row(
                "SELECT count(*) FROM messages WHERE kind='alert' AND recipient='owner'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(alerts >= 1, "blocked graph must leave an owner alert");
    }

    #[test]
    fn reaper_park_does_not_block_graph_when_child_has_active_retry_marker() {
        let (_d, mut c) = open_tmp();
        let (graph, child) =
            active_graph_rework_child(&mut c, Some(r#"{"runner_retry":{"requested":true}}"#));

        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();

        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "failed",
            "the child is parked before its retry marker suppresses graph blocking"
        );

        let graph_state: (String, Option<String>, Option<String>) = c
            .query_row(
                "SELECT state,hold_code,hold_summary FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(graph_state, ("active".into(), None, None));
        let graph_events: i64 = c
            .query_row(
                "SELECT count(*) FROM events WHERE kind='task_graph_blocked'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(graph_events, 0);
    }

    #[test]
    fn reaper_review_only_park_rolls_back_every_write_on_alert_failure() {
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(
            &mut c,
            "boss",
            "review task",
            None,
            0,
            None,
            None,
            None,
            Some(42),
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks
             SET status='rework', assignee='W1', reviewer='R1', rework_round=2
             WHERE id=?1",
            params![id],
        )
        .unwrap();
        let target = format!("task#{id}");
        c.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active)
             VALUES (?1, 'W1', 1000, 1050, 1)",
            params![target],
        )
        .unwrap();
        let refs_before = crate::tasks::get(&c, id).unwrap().unwrap().refs;
        let notes_before: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM task_notes WHERE task_id=?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        let events_before: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1",
                params![target],
                |row| row.get(0),
            )
            .unwrap();
        // The alert is the final write in the park sequence. Force it to fail
        // so the test proves all earlier task/note/claim/event writes roll back.
        c.execute_batch(
            "CREATE TRIGGER fail_owner_alert
             BEFORE INSERT ON messages
             WHEN NEW.kind='alert' AND NEW.recipient='owner'
             BEGIN
                 SELECT RAISE(ABORT, 'injected owner alert failure');
             END;",
        )
        .unwrap();

        let error = reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap_err();
        assert!(
            error.to_string().contains("injected owner alert failure"),
            "unexpected error: {error}"
        );
        let task = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.assignee.as_deref(), Some("W1"));
        assert_eq!(task.refs, refs_before);
        let active_claims: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                params![target],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 1, "lease deactivation must roll back");
        let notes_after: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM task_notes WHERE task_id=?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notes_after, notes_before, "park note must roll back");
        let events_after: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1",
                params![target],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events_after, events_before, "park event must roll back");
    }

    #[test]
    fn reaper_distinguishes_no_lease_from_expired_lease() {
        let (_d, mut c) = open_tmp();
        // Task in working with NO claim ever created (orphan scenario)
        let id = crate::tasks::create(
            &mut c, "boss", "orphan", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='working', assignee='W1' WHERE id=?1",
            params![id],
        )
        .unwrap();

        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        let target = format!("task#{id}");
        let evs = crate::events::list(&c, 0, Some(&target), 10, 1100).unwrap();
        let body = &evs
            .iter()
            .find(|e| e.kind == "task_reclaimed")
            .expect("task_reclaimed event missing")
            .body;
        assert!(
            body.contains("no lease installed"),
            "must say 'no lease installed' when no claim exists, got: {body}"
        );
        assert!(
            !body.contains("lease lapsed"),
            "must NOT say 'lease lapsed' when no claim existed"
        );
    }

    #[test]
    fn sweep_rollback_undoes_reaper_changes() {
        // A rejected lifecycle transition (or any failure) in the same transaction
        // as a sweep must roll back the reaper's changes.
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(
            &mut c, "boss", "impl", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        ready_claim(&mut c, "A", id, 100, 1000);
        assert_eq!(
            crate::tasks::get(&c, id).unwrap().unwrap().status,
            "working"
        );

        // Begin explicit transaction, run sweep, then rollback
        c.execute_batch("BEGIN IMMEDIATE").unwrap();
        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        assert_eq!(
            crate::tasks::get(&c, id).unwrap().unwrap().status,
            "open",
            "reaper must have transitioned within the txn"
        );
        c.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            crate::tasks::get(&c, id).unwrap().unwrap().status,
            "working",
            "rollback must undo the reaper's transition"
        );
    }

    #[test]
    fn sweep_respects_limit() {
        let (_d, c) = open_tmp();
        for i in 0..5 {
            c.execute(
                "INSERT INTO messages(ts,author,topic,kind,body,expires_at)
                 VALUES (1,'a','hub','info',?1,10)",
                params![format!("m{i}")],
            )
            .unwrap();
        }
        sweep_on_write(&c, 100, 2).unwrap(); // only 2 of 5 expired rows reclaimed
        let n: i64 = c
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);
    }

    // ── provisioning grace (#199) ───────────────────────────────────────────

    #[test]
    fn reaper_skips_rework_within_provisioning_grace() {
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000)
            .unwrap();
        ready_claim(&mut c, "W1", id, 100, 1000);

        // Simulate VerdictChanges: status=rework, lease released, updated_at=1300.
        c.execute(
            "UPDATE tasks SET status='rework', updated_at=?1 WHERE id=?2",
            params![1300, id],
        )
        .unwrap();
        c.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![format!("task#{id}")],
        )
        .unwrap();

        // Within grace window (updated_at=1300, now=1350, grace=60 → 1300 > 1290).
        reap_lapsed_tasks(&c, 1350, SWEEP_LIMIT).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "rework", "rework within grace must not be reaped");

        // Past grace window (now=1400, 1300 > 1340 is false).
        reap_lapsed_tasks(&c, 1400, SWEEP_LIMIT).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "open", "rework past grace must be reaped");
    }

    #[test]
    fn reaper_preserves_dependency_blocked_runner_retry_until_rework_push() {
        let (_d, mut c) = open_tmp();
        let dependency = crate::tasks::create(
            &mut c,
            "owner",
            "pending dependency",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let task_id = crate::tasks::create(
            &mut c,
            "owner",
            "runner retry",
            None,
            0,
            None,
            Some(
                r#"{"runner_retry":{"provider":"codex","model":"gpt-5","effort":"high","prompt":"finish","turn_kind":"rework","continuation_id":"thread-1","requested":true}}"#,
            ),
            Some(&format!("[{dependency}]")),
            None,
            1000,
        )
        .unwrap();
        crate::classify::store_classifications(
            &mut c,
            &[crate::classify::TaskClassification {
                task_id,
                cx_est: 3,
                size: "M".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "unit-test:v2",
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='rework', updated_at=1000 WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        // This is after the provisioning grace. The blocked provider retry
        // must remain rework so its eventual ReworkPushed is legal.
        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        assert_eq!(
            crate::tasks::get(&c, task_id).unwrap().unwrap().status,
            "rework"
        );

        c.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            params![dependency],
        )
        .unwrap();
        // A write can sweep after the dependency resolves but before the
        // replacement worker claims. Durable provider retry identity must
        // survive that ready-but-unclaimed interval.
        reap_lapsed_tasks(&c, 1101, SWEEP_LIMIT).unwrap();
        assert_eq!(
            crate::tasks::get(&c, task_id).unwrap().unwrap().status,
            "rework"
        );
        crate::tasks::claim_provider_retry_rework(&mut c, "codex", task_id, 10, 1101)
            .unwrap()
            .expect("resolved runner retry must claim in rework");

        // If the replacement worker's lease lapses, the durable retry marker
        // must not exempt the claimed row forever. Recovery clears the stale
        // assignee while preserving rework semantics for another replacement.
        reap_lapsed_tasks(&c, 1162, SWEEP_LIMIT).unwrap();
        let recovered = crate::tasks::get(&c, task_id).unwrap().unwrap();
        assert_eq!(recovered.status, "rework");
        assert!(
            recovered.assignee.is_none(),
            "lapsed replacement assignee must be cleared"
        );
        let refs: serde_json::Value =
            serde_json::from_str(recovered.refs.as_deref().unwrap()).unwrap();
        assert!(crate::runner_state::retry_requested(&refs));
        let active_claims: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                params![format!("task#{task_id}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 0, "lapsed replacement lease is deactivated");

        crate::tasks::claim_provider_retry_rework(&mut c, "replacement", task_id, 3600, 1163)
            .unwrap()
            .expect("recovered runner retry must be claimable again in rework");
        let pushed = crate::tasks::apply_event(
            &mut c,
            "replacement",
            task_id,
            &crate::lifecycle::Event::ReworkPushed,
            1164,
        )
        .unwrap();
        assert_eq!(pushed.task.status, "in-review");
    }

    #[test]
    fn reaper_ignores_stale_codex_request_when_neutral_retry_is_not_requested() {
        let (_d, mut c) = open_tmp();
        let task_id = crate::tasks::create(
            &mut c,
            "owner",
            "blocked retry",
            None,
            0,
            None,
            Some(
                r#"{"runner_retry":{"provider":"codex","model":"gpt-5","effort":"high","prompt":"finish","turn_kind":"rework","continuation_id":"thread-new","requested":false},"codex_retry_requested":true}"#,
            ),
            None,
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='rework', updated_at=1000 WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        reap_lapsed_tasks(&c, 1100, SWEEP_LIMIT).unwrap();
        let reaped = crate::tasks::get(&c, task_id).unwrap().unwrap();
        assert_eq!(reaped.status, "open");
        assert!(!crate::runner_state::retry_requested(
            &serde_json::from_str(reaped.refs.as_deref().unwrap()).unwrap()
        ));
    }

    #[test]
    fn reaper_does_not_grace_working_tasks() {
        // The provisioning grace only applies to rework, not working.
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000)
            .unwrap();
        ready_claim(&mut c, "W1", id, 100, 1000);

        // Expire the lease and keep status=working, updated_at recent.
        c.execute(
            "UPDATE tasks SET updated_at=?1 WHERE id=?2",
            params![1200, id],
        )
        .unwrap();

        // now=1200, lease expired at 1100 — working task gets reaped despite
        // recent updated_at because grace only covers rework.
        reap_lapsed_tasks(&c, 1200, SWEEP_LIMIT).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(
            t.status, "open",
            "working task must not get provisioning grace"
        );
    }

    // ── Durable FK protection (task #395) ──────────────────────────────
    //
    // Schema 46 adds real REFERENCES tasks(id) columns on the decomposition
    // and review-follow-up tables. If sweep deletes an aged done task while
    // one of those rows still references it, FK enforcement raises
    // `FOREIGN KEY constraint failed`; the sweep runs inside every mutation's
    // transaction, so a single referenced-but-aged done task turns every
    // subsequent write on the DB into an exit-3 failure until the task's
    // updated_at is nudged past the retention horizon.

    fn open_tmp_fk() -> (tempfile::TempDir, Connection) {
        let (dir, c) = open_tmp();
        c.pragma_update(None, "foreign_keys", true).unwrap();
        (dir, c)
    }

    fn aged_done_task(c: &mut Connection, title: &str) -> i64 {
        // Age the task past the done-task retention horizon so sweep considers
        // it reclaimable. Uses `updated_at=0` and now = DONE_TASK_TTL_SECS+1
        // (the pattern from `reclaimable_task`).
        let id =
            crate::tasks::create(c, "boss", title, None, 0, None, None, None, None, 1).unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=0 WHERE id=?1",
            [id],
        )
        .unwrap();
        id
    }

    fn insert_decomposition(c: &Connection, source_task_id: i64) {
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id, state, planned_source_revision, created_at, updated_at)
             VALUES (?1, 'completed', 1, 1, 1)",
            [source_task_id],
        )
        .unwrap();
    }

    fn insert_graph_member(c: &Connection, source_task_id: i64, member_task_id: i64) {
        insert_decomposition(c, source_task_id);
        let graph_id: i64 = c
            .query_row(
                "SELECT id FROM task_decompositions WHERE source_task_id=?1",
                [source_task_id],
                |r| r.get(0),
            )
            .unwrap();
        c.execute(
            "INSERT INTO task_graph_members(graph_id, task_id, local_key, plan_revision, active)
             VALUES (?1, ?2, 'k', 1, 0)",
            [graph_id, member_task_id],
        )
        .unwrap();
    }

    fn insert_followup_batch(c: &Connection, pr: i64, task_id: i64, source_task_id: i64) {
        c.execute(
            "INSERT INTO review_followup_batches(
                 pr_number, task_id, source_task_id, collector_version,
                 artifact_count, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'v1', 0, 'collected', 1, 1)",
            [pr, task_id, source_task_id],
        )
        .unwrap();
    }

    fn insert_followup_artifact_with_link(
        c: &Connection,
        pr: i64,
        ordinal: i64,
        column: &str,
        linked_task_id: i64,
    ) {
        assert!(
            column == "linked_task_id" || column == "created_task_id",
            "unknown artifact link column: {column}"
        );
        let disposition = if column == "linked_task_id" {
            "linked"
        } else {
            "created"
        };
        let sql = format!(
            "INSERT INTO review_followup_artifacts(
                 pr_number, ordinal, technical_impact, scope_relationship,
                 concern, non_blocking_reason, affected_behavior,
                 desired_outcome, verification_expectations, evidence_ids,
                 disposition, {column}, created_at, updated_at)
             VALUES (?1, ?2, 'minor', 'defense_in_depth',
                 'c', 'nbr', 'ab', 'do', 've', '[]',
                 '{disposition}', ?3, 1, 1)"
        );
        c.execute(&sql, [pr, ordinal, linked_task_id]).unwrap();
    }

    fn assert_task_survives(c: &Connection, id: i64, guard: &str) {
        let count: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [id], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "task #{id} must survive sweep while {guard} references it"
        );
    }

    #[test]
    fn sweep_on_write_retains_task_referenced_by_task_decomposition() {
        let (_d, mut c) = open_tmp_fk();
        let source = aged_done_task(&mut c, "aged source");
        insert_decomposition(&c, source);

        // Without the durable-ref guard the sweep raises FOREIGN KEY constraint
        // failed inside sweep_on_write, poisoning every future mutation.
        sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();
        assert_task_survives(&c, source, "task_decompositions.source_task_id");

        // The recovered DB accepts a subsequent task creation.
        let follow_up = crate::tasks::create(
            &mut c,
            "boss",
            "follow up",
            None,
            0,
            None,
            None,
            None,
            None,
            DONE_TASK_TTL_SECS + 2,
        )
        .unwrap();
        assert!(follow_up > 0);
    }

    #[test]
    fn sweep_all_retains_task_referenced_by_task_decomposition() {
        let (_d, mut c) = open_tmp_fk();
        let source = aged_done_task(&mut c, "aged source");
        insert_decomposition(&c, source);

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();
        assert_task_survives(&c, source, "task_decompositions.source_task_id");
    }

    #[test]
    fn sweep_retains_every_durable_task_reference() {
        // Table-driven mechanism check: for every durable REFERENCES tasks(id)
        // relationship, plant a fresh aged done task, insert the referencing
        // row, sweep, and prove the task survives. When a new durable FK is
        // added the inventory test below fails until this list grows with it.
        struct Case {
            label: &'static str,
            plant: fn(&Connection, i64),
        }
        fn decomp(c: &Connection, t: i64) {
            insert_decomposition(c, t);
        }
        fn graph_member(c: &Connection, t: i64) {
            let source_id: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('graph src','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_graph_member(c, source_id, t);
        }
        fn cleanup(c: &Connection, t: i64) {
            let source_id: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('cleanup src','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_decomposition(c, source_id);
            let graph_id: i64 = c
                .query_row(
                    "SELECT id FROM task_decompositions WHERE source_task_id=?1",
                    [source_id],
                    |r| r.get(0),
                )
                .unwrap();
            c.execute(
                "INSERT INTO decomposition_cleanup(
                     graph_id, task_id, artifact_kind, artifact_ref, updated_at)
                 VALUES (?1, ?2, 'branch', 'ref', 1)",
                [graph_id, t],
            )
            .unwrap();
        }
        fn reservation(c: &Connection, t: i64) {
            c.execute(
                "INSERT INTO reviewer_provision_reservations(task_id, token, role, created_at)
                 VALUES (?1, 'tok', 'r1', 1)",
                [t],
            )
            .unwrap();
        }
        fn batch_task(c: &Connection, t: i64) {
            let source_id: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('batch src','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_followup_batch(c, 1001, t, source_id);
        }
        fn batch_source(c: &Connection, t: i64) {
            let task_id: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('batch owner','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_followup_batch(c, 1002, task_id, t);
        }
        fn artifact_linked(c: &Connection, t: i64) {
            let owner: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('art owner','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_followup_batch(c, 1003, owner, owner);
            insert_followup_artifact_with_link(c, 1003, 1, "linked_task_id", t);
        }
        fn artifact_created(c: &Connection, t: i64) {
            let owner: i64 = c
                .query_row(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                     VALUES ('art owner','done','owner',1,1) RETURNING id",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            insert_followup_batch(c, 1004, owner, owner);
            insert_followup_artifact_with_link(c, 1004, 1, "created_task_id", t);
        }
        fn assessment(c: &Connection, t: i64) {
            c.execute(
                "INSERT INTO review_followup_assessments(
                     target, scope_kind, scope_id, source_task_id, state,
                     created_at, updated_at)
                 VALUES ('t', 'task', 1, ?1, 'pending', 1, 1)",
                [t],
            )
            .unwrap();
        }
        let cases = [
            Case {
                label: "task_decompositions.source_task_id",
                plant: decomp,
            },
            Case {
                label: "task_graph_members.task_id",
                plant: graph_member,
            },
            Case {
                label: "decomposition_cleanup.task_id",
                plant: cleanup,
            },
            Case {
                label: "reviewer_provision_reservations.task_id",
                plant: reservation,
            },
            Case {
                label: "review_followup_batches.task_id",
                plant: batch_task,
            },
            Case {
                label: "review_followup_batches.source_task_id",
                plant: batch_source,
            },
            Case {
                label: "review_followup_artifacts.linked_task_id",
                plant: artifact_linked,
            },
            Case {
                label: "review_followup_artifacts.created_task_id",
                plant: artifact_created,
            },
            Case {
                label: "review_followup_assessments.source_task_id",
                plant: assessment,
            },
        ];
        for case in &cases {
            let (_d, mut c) = open_tmp_fk();
            let task = aged_done_task(&mut c, case.label);
            (case.plant)(&c, task);

            sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();
            assert_task_survives(&c, task, case.label);
            sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();
            assert_task_survives(&c, task, case.label);
        }
    }

    #[test]
    fn unreferenced_aged_done_task_is_still_reclaimed() {
        // The durable-ref guard must not turn every aged done task into a
        // permanent row: an unreferenced one still reclaims under FK
        // enforcement.
        let (_d, mut c) = open_tmp_fk();
        let orphan = aged_done_task(&mut c, "orphan");

        sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();
        let count: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [orphan], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "unreferenced aged done task must reclaim");
    }

    #[test]
    fn sweep_candidate_discovery_uses_primary_key_page_without_history_sort() {
        // Task #395 remediation, blocker #3: filtering age/status in the
        // cursor query made SQLite choose tasks_reviewing_newest and sort the
        // full qualifying history before LIMIT. Pin the exact production
        // query to a rowid range seek with no temporary ordering structure.
        let (_d, c) = open_tmp_fk();
        let plan: Vec<String> = c
            .prepare(&format!("EXPLAIN QUERY PLAN {RECLAIMABLE_TASK_WINDOW_SQL}"))
            .unwrap()
            .query_map(params![0, SWEEP_LIMIT as i64], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            plan.iter().any(|detail| {
                detail.contains("INTEGER PRIMARY KEY") && detail.contains("rowid>?")
            }),
            "candidate discovery must seek the tasks primary key above the rotating cursor: \
             {plan:?}"
        );
        assert!(
            plan.iter().all(|detail| !detail.contains("TEMP B-TREE")),
            "candidate discovery must not enumerate and sort retained task history: {plan:?}"
        );
    }

    #[test]
    fn sweep_on_write_examines_at_most_sweep_limit_candidates_per_call() {
        // Task #395 remediation: every write must expose at most SWEEP_LIMIT
        // raw IDs to eligibility and reference guards. Create 3*SWEEP_LIMIT
        // pinned aged done tasks, run one sweep, and confirm the cursor moves
        // across exactly one primary-key window.
        let (_d, mut c) = open_tmp_fk();
        let n = SWEEP_LIMIT * 3;
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let id = aged_done_task(&mut c, &format!("pinned {i}"));
            insert_decomposition(&c, id);
            ids.push(id);
        }
        // tasks::create runs opportunistic sweep itself; isolate the one call
        // under test from cursor movement incurred while building the fixture.
        c.execute(
            "DELETE FROM sweep_cursors WHERE name='reclaimable_tasks'",
            [],
        )
        .unwrap();
        let first = *ids.first().unwrap();
        let expected_cursor = first + SWEEP_LIMIT as i64 - 1;

        sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();

        let cursor: i64 = c
            .query_row(
                "SELECT value FROM sweep_cursors WHERE name='reclaimable_tasks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor, expected_cursor,
            "cursor must advance by exactly one SWEEP_LIMIT-sized raw-ID window; \
             retained pinned provenance cannot inflate write-lock time. \
             cursor={cursor}, expected={expected_cursor}"
        );
        let survivors: i64 = c
            .query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            survivors, n as i64,
            "every pinned task must survive — no FK violation, no unintended reclamation"
        );
    }

    #[test]
    fn sweep_on_write_eventually_reclaims_unreferenced_task_behind_pinned_wall() {
        // Boundedness must not cause starvation. Plant SWEEP_LIMIT+5 pinned
        // aged done tasks at the front (low ids) and one unreferenced aged
        // done task at the tail. The unreferenced task never reclaims in the
        // first sweep (cursor examines only pinned rows), but after the
        // cursor advances past the pinned block and eventually wraps, the
        // sweep reaches and deletes the unreferenced tail task.
        let (_d, mut c) = open_tmp_fk();
        let pinned_count = SWEEP_LIMIT + 5;
        for i in 0..pinned_count {
            let id = aged_done_task(&mut c, &format!("pinned {i}"));
            insert_decomposition(&c, id);
        }
        let tail = aged_done_task(&mut c, "tail unreferenced");

        // Drive sweeps until the tail is reclaimed. Cap the loop at a
        // generous multiple of the pinned block so a real starvation bug
        // still fails loudly rather than hanging the test.
        let mut sweeps = 0;
        let cap = pinned_count * 3;
        loop {
            sweep_on_write(&c, DONE_TASK_TTL_SECS + 1, SWEEP_LIMIT).unwrap();
            sweeps += 1;
            let alive: i64 = c
                .query_row("SELECT count(*) FROM tasks WHERE id=?1", [tail], |r| {
                    r.get(0)
                })
                .unwrap();
            if alive == 0 {
                break;
            }
            assert!(
                sweeps < cap,
                "unreferenced tail task starved behind pinned wall — the \
                 rotating cursor must eventually visit every id even while \
                 SWEEP_LIMIT bounds per-call work"
            );
        }
        let pinned_survivors: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE status='done'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            pinned_survivors, pinned_count as i64,
            "every pinned task must still survive the loop"
        );
    }

    #[test]
    fn sweep_all_ignores_cursor_and_reclaims_everything_in_one_call() {
        // Explicit sweep_all is documented as unbounded. Even if the
        // opportunistic cursor was left partway through the aged done range
        // by an earlier sweep_on_write, one sweep_all must reclaim every
        // unreferenced aged done task (guards still retain pinned ones) and
        // clear the cursor so subsequent opportunistic sweeps restart clean.
        let (_d, mut c) = open_tmp_fk();
        let unref_low = aged_done_task(&mut c, "unref low");
        let pinned_mid = aged_done_task(&mut c, "pinned mid");
        insert_decomposition(&c, pinned_mid);
        let unref_high = aged_done_task(&mut c, "unref high");
        // Pre-set the cursor past unref_low so a naive cursor-honoring
        // sweep_all would skip it.
        c.execute(
            "INSERT INTO sweep_cursors(name, value)
             VALUES ('reclaimable_tasks', ?1)
             ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            [pinned_mid],
        )
        .unwrap();

        sweep_all(&c, DONE_TASK_TTL_SECS + 1).unwrap();

        let low: i64 = c
            .query_row("SELECT count(*) FROM tasks WHERE id=?1", [unref_low], |r| {
                r.get(0)
            })
            .unwrap();
        let high: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks WHERE id=?1",
                [unref_high],
                |r| r.get(0),
            )
            .unwrap();
        let pinned: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks WHERE id=?1",
                [pinned_mid],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            low, 0,
            "sweep_all must reclaim unreferenced task before cursor"
        );
        assert_eq!(
            high, 0,
            "sweep_all must reclaim unreferenced task after cursor"
        );
        assert_eq!(pinned, 1, "pinned task must still be retained");
        let cursor_rows: i64 = c
            .query_row(
                "SELECT count(*) FROM sweep_cursors WHERE name='reclaimable_tasks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor_rows, 0,
            "sweep_all must clear the opportunistic cursor after a full pass"
        );
    }

    #[test]
    fn every_durable_task_ref_column_has_a_lookup_index() {
        // Task #395 remediation: schema 48 adds a lookup index for every
        // durable REFERENCES tasks(id) column so sweep_on_write's
        // `NOT EXISTS (SELECT 1 FROM x WHERE x.col=t.id)` guard is served by
        // an index rather than a scan of retained provenance. The guard runs
        // inside every mutation's write transaction and the outer LIMIT lets
        // it examine SWEEP_LIMIT candidate tasks, so an unindexed column
        // would give SWEEP_LIMIT × |retained| work per write.
        //
        // Assert the mechanism directly (via sqlite_master / index_info)
        // rather than through EXPLAIN QUERY PLAN — the planner falls back to
        // a full covering-PK scan on an empty table regardless of which
        // secondary indexes exist, so a plan-based check would silently pass
        // an unindexed column here.
        let (_d, c) = open_tmp_fk();
        for (table, column) in DURABLE_TASK_REF_TABLES {
            // `INTEGER PRIMARY KEY` is an alias for rowid, which is inherently
            // O(log n) for `WHERE column=?` even though PRAGMA index_list
            // reports no explicit secondary index. Skip those columns —
            // they're already index-bounded by SQLite's rowid.
            let is_rowid_alias: bool = c
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| {
                    // (cid, name, type, notnull, dflt_value, pk)
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(5)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .any(|(name, ty, pk)| name == *column && pk == 1 && ty.to_uppercase() == "INTEGER");
            if is_rowid_alias {
                continue;
            }
            let indexes: Vec<String> = c
                .prepare(&format!("PRAGMA index_list({table})"))
                .unwrap()
                // (seq, name, unique, origin, partial)
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let mut served_by: Option<String> = None;
            for index_name in &indexes {
                let mut stmt = c
                    .prepare(&format!("PRAGMA index_info({index_name})"))
                    .unwrap();
                let leading: Option<String> = stmt
                    .query_map([], |row| {
                        // (seqno, cid, name)
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
                    .into_iter()
                    .find(|(seqno, _)| *seqno == 0)
                    .map(|(_, name)| name);
                if leading.as_deref() == Some(column) {
                    served_by = Some(index_name.clone());
                    break;
                }
            }
            assert!(
                served_by.is_some(),
                "{table}.{column} has no index whose leading column is \
                 {column} — sweep_on_write's durable-reference guard would \
                 fall back to a full scan of {table} inside every mutation's \
                 write transaction. Add a `CREATE INDEX` on ({column}) to \
                 schema.sql (partial `WHERE {column} IS NOT NULL` is fine \
                 for nullable columns) and, if this is a new durable FK, \
                 bump SCHEMA_VERSION so existing databases materialize it. \
                 Existing indexes on {table}: {indexes:?}"
            );
        }
    }

    #[test]
    fn durable_task_ref_inventory_covers_every_fk_on_tasks() {
        // Mechanism-level assertion: enumerate every FK in the live schema
        // that points at tasks(id) and require it to appear in
        // DURABLE_TASK_REF_TABLES. A new REFERENCES tasks(id) added without
        // updating the sweep guard fails here, preventing schema drift from
        // reintroducing the sweep-on-write foreign-key regression.
        let (_d, c) = open_tmp_fk();
        let tables: Vec<String> = c
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let mut discovered: Vec<(String, String)> = Vec::new();
        for table in &tables {
            let mut stmt = c
                .prepare(&format!("PRAGMA foreign_key_list({table})"))
                .unwrap();
            let rows: Vec<(String, String)> = stmt
                .query_map([], |r| {
                    // (id, seq, table, from, to, on_update, on_delete, match)
                    let referenced_table: String = r.get(2)?;
                    let from_column: String = r.get(3)?;
                    Ok((referenced_table, from_column))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            for (ref_table, from_column) in rows {
                if ref_table == "tasks" {
                    discovered.push((table.clone(), from_column));
                }
            }
        }
        discovered.sort();
        let mut expected: Vec<(String, String)> = DURABLE_TASK_REF_TABLES
            .iter()
            .map(|(t, c)| ((*t).to_string(), (*c).to_string()))
            .collect();
        expected.sort();
        assert_eq!(
            discovered, expected,
            "REFERENCES tasks(id) inventory drifted — update \
             DURABLE_TASK_REF_TABLES (and the sweep guard covers it \
             automatically) or, if the new FK's rows are safe to delete \
             with the task, extend OPERATIONAL_TASK_REF_TABLES + the \
             per-table bounded delete instead"
        );
    }

    #[test]
    fn reaper_never_converts_durable_ci_remediation_to_generic_open() {
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(
            &mut c,
            "boss",
            "durable CI remediation",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        ready_claim(&mut c, "W1", id, 100, 1000);
        crate::tasks::apply_event(
            &mut c,
            "W1",
            id,
            &crate::lifecycle::Event::SignaledDone { pr: "453".into() },
            1050,
        )
        .unwrap();
        crate::tasks::apply_checks_failed_with_remediation(
            &mut c,
            id,
            453,
            "abc123",
            &["test".into()],
            "fix test",
            1060,
        )
        .unwrap();

        reap_lapsed_tasks(&c, 5000, SWEEP_LIMIT).unwrap();
        let task = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(
            task.status, "rework",
            "durable same-PR remediation must never fall through to generic open"
        );
        assert!(crate::tasks::ci_remediation_intent(task.refs.as_deref())
            .unwrap()
            .is_some());
    }
}
