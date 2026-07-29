//! Physical reclamation of expired rows.
//!
//! Expiry is *logical* first — every read filters `expires_at > now`, so expired rows are
//! invisible immediately. This module is housekeeping that reclaims the disk. [`sweep_on_write`]
//! is the bounded, opportunistic sweep every mutation runs; [`sweep_all`] is the unbounded
//! explicit sweep (`quorum sweep`) plus a WAL checkpoint.

use crate::error::Result;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};

/// Done tasks are reclaimed this long after entering `done`. Default; Phase 6 config overrides.
pub const DONE_TASK_TTL_SECS: i64 = 7 * 24 * 3600;

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
    // (id, assignee, review_only, had_any_lease_ever)
    let lapsed: Vec<(i64, Option<String>, bool, bool)> = {
        let mut stmt = conn.prepare(
            "SELECT t.id, t.assignee, t.review_only,
                    EXISTS(SELECT 1 FROM claims c WHERE c.target = 'task#' || t.id) AS had_lease
             FROM tasks t
             WHERE t.status IN ('working', 'rework') AND NOT EXISTS (
                 SELECT 1 FROM claims c
                 WHERE c.target = 'task#' || t.id AND c.active=1 AND c.expires_at > ?1
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
                        r.get::<_, i64>(2)? != 0,
                        r.get::<_, i64>(3)? != 0,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    for (id, prev, review_only, had_lease) in &lapsed {
        let target = format!("task#{id}");
        if *review_only {
            // Review-only tasks recover to in-review so Phase 5b can reattach a reviewer.
            // Clear reviewer so the orphan detector sees it as unattended.
            conn.execute(
                "UPDATE tasks SET status='in-review', assignee=NULL, reviewer=NULL, updated_at=?1 WHERE id=?2",
                params![now, id],
            )?;
        } else {
            conn.execute(
                "UPDATE tasks SET status='open', assignee=NULL, updated_at=?1 WHERE id=?2",
                params![now, id],
            )?;
        }
        // Clear any lingering (now-expired) lease row so the next claim starts clean.
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![target],
        )?;
        let dest = if *review_only { "in-review" } else { "open" };
        let reason = if *had_lease {
            "lease lapsed"
        } else {
            "no lease installed"
        };
        let body = match prev {
            Some(a) => format!("reclaimed from {a} ({reason}) → {dest}"),
            None => format!("reclaimed ({reason}) → {dest}"),
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

fn delete_reclaimable_tasks_bounded(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    // Do not remove a parent until bounded child cleanup has caught up. This makes a large task
    // take several opportunistic sweeps rather than letting one write delete an unbounded fanout.
    conn.execute(
        "DELETE FROM tasks WHERE rowid IN \
         (SELECT t.rowid FROM tasks t
          WHERE t.status='done' AND t.updated_at < ?1
            AND NOT EXISTS (SELECT 1 FROM agent_runs x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM task_branches x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM task_notes x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM task_messages x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM mailbox x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM journal x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM approvals x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM reviewer_provision_attempts x WHERE x.task_id=t.id)
            AND NOT EXISTS (SELECT 1 FROM pr_targets x WHERE x.task_id=t.id)
          LIMIT ?2)",
        params![now - DONE_TASK_TTL_SECS, limit as i64],
    )?;
    Ok(())
}

/// Park open tasks whose dependencies cannot currently be satisfied: every dep is terminal
/// (done/failed/cancelled) but at least one is failed or cancelled. They stay excluded from
/// provisioning until an explicit retry, without losing their dependency context.
pub fn cascade_dead_deps(conn: &Connection, now: i64, limit: usize) -> Result<usize> {
    let doomed: Vec<(i64, i64)> = {
        let mut stmt = conn.prepare(
            "SELECT t.id, je.value AS dep_id
             FROM tasks t, json_each(t.depends_on) je
             WHERE t.status = 'open'
               AND t.depends_on IS NOT NULL
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
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    let mut count = 0usize;
    let mut seen = std::collections::HashSet::new();
    for (task_id, failed_dep) in &doomed {
        if !seen.insert(*task_id) {
            continue;
        }
        let target = format!("task#{task_id}");
        let reason = format!("dependency #{failed_dep} is terminal-not-done");
        conn.execute(
            "UPDATE tasks
             SET status='failed',
                 assignee=NULL,
                 refs=json_set(
                     COALESCE(refs, '{}'),
                     '$.daemon_parked', json('true'),
                     '$.daemon_parked_reason', ?1,
                     '$.daemon_resume_status', 'open'
                 ),
                 updated_at=?2
             WHERE id=?3",
            params![reason, now, task_id],
        )?;
        conn.execute(
            "INSERT INTO task_notes(task_id, ts, agent, body)
             VALUES (?1, ?2, 'daemon', ?3)",
            params![task_id, now, format!("parked: {reason}")],
        )?;
        crate::events::emit(conn, "task_parked", &target, &reason, now)?;
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
    crate::task_messages::expire_stale_deliveries(&tx, now, usize::MAX)?;
    tx.execute(
        "DELETE FROM task_messages WHERE expires_at <= ?1",
        params![now],
    )?;
    let unbounded = i64::MAX as usize;
    delete_reclaimable_task_rows_bounded(&tx, now, unbounded)?;
    delete_orphaned_task_rows_bounded(&tx, unbounded)?;
    delete_reclaimable_tasks_bounded(&tx, now, unbounded)?;
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
        crate::tasks::claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap();
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
            crate::tasks::claim(&mut c, &agent, Some(id), &[], 100, 1000).unwrap();
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
        // dep is still open (non-terminal).
        let n = cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(n, 0, "non-terminal dep should not trigger cascade");
        assert_eq!(
            crate::tasks::get(&c, child).unwrap().unwrap().status,
            "open"
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
        // Table-driven: review_only tasks recover to in-review,
        // implementation tasks recover to open.
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
                expected_status: "in-review",
                label: "review_only→in-review",
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
                crate::tasks::claim(&mut c, "W1", Some(id), &[], 100, 1000).unwrap();
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

            // Event body reports actual destination
            let target = format!("task#{id}");
            let evs = crate::events::list(&c, 0, Some(&target), 10, 1100).unwrap();
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

    #[test]
    fn reaper_review_only_clears_reviewer_for_phase_5b() {
        // Phase 5b reattaches reviewers to orphan in-review tasks with no live
        // worker/reviewer. Clearing reviewer=NULL is required for that detection.
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
            "UPDATE tasks SET status='rework', assignee='W1', reviewer='R1' WHERE id=?1",
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
        assert_eq!(t.status, "in-review");
        assert!(
            t.reviewer.is_none(),
            "reviewer must be cleared so Phase 5b can reattach"
        );
        // PR, author provenance, and rework_round are preserved
        assert!(t.review_only, "review_only flag must be preserved");
        assert_eq!(t.rework_round, 0, "rework_round must be preserved");
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
        crate::tasks::claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap();
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
        crate::tasks::claim(&mut c, "W1", Some(id), &[], 100, 1000).unwrap();

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
    fn reaper_does_not_grace_working_tasks() {
        // The provisioning grace only applies to rework, not working.
        let (_d, mut c) = open_tmp();
        let id = crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000)
            .unwrap();
        crate::tasks::claim(&mut c, "W1", Some(id), &[], 100, 1000).unwrap();

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
}
