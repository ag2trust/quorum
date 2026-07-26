//! Physical reclamation of expired rows.
//!
//! Expiry is *logical* first — every read filters `expires_at > now`, so expired rows are
//! invisible immediately. This module is housekeeping that reclaims the disk. [`sweep_on_write`]
//! is the bounded, opportunistic sweep every mutation runs; [`sweep_all`] is the unbounded
//! explicit sweep (`quorum sweep`) plus a WAL checkpoint.

use crate::error::Result;
use rusqlite::{params, Connection};

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
    let lapsed: Vec<(i64, Option<String>)> = {
        let mut stmt = conn.prepare(
            "SELECT id, assignee FROM tasks
             WHERE status IN ('working', 'rework') AND NOT EXISTS (
                 SELECT 1 FROM claims c
                 WHERE c.target = 'task#' || tasks.id AND c.active=1 AND c.expires_at > ?1
             )
             AND NOT (status = 'rework' AND updated_at > ?1 - ?3)
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(
                params![now, limit as i64, REWORK_PROVISIONING_GRACE_SECS],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    for (id, prev) in &lapsed {
        let target = format!("task#{id}");
        conn.execute(
            "UPDATE tasks SET status='open', assignee=NULL, updated_at=?1 WHERE id=?2",
            params![now, id],
        )?;
        // Clear any lingering (now-expired) lease row so the next claim starts clean.
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![target],
        )?;
        // Emit to the event log. Body carries the prev_assignee so consumers can identify
        // whose work returned to the queue without parsing JSON; `subject = task#<id>` makes
        // it filterable via `quorum log --refs task#<id>`.
        let body = match prev {
            Some(a) => format!("reclaimed from {a} (lease lapsed) → open"),
            None => "reclaimed (lease lapsed) → open".to_string(),
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

/// Cancel open tasks whose dependencies can never be satisfied: every dep is terminal
/// (done/failed/cancelled) but at least one is failed or cancelled. Without this, a
/// cancelled dependency permanently blocks its dependents (#57 G1).
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
        conn.execute(
            "UPDATE tasks SET status='cancelled', assignee=NULL, updated_at=?1 WHERE id=?2",
            params![now, task_id],
        )?;
        let target = format!("task#{task_id}");
        let body = format!("dep-cascade: dependency #{failed_dep} is terminal-not-done");
        crate::events::emit(conn, "dep_cascade", &target, &body, now)?;
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
    conn.execute(
        "DELETE FROM tasks WHERE rowid IN \
         (SELECT rowid FROM tasks WHERE status='done' AND updated_at < ?1 LIMIT ?2)",
        params![now - DONE_TASK_TTL_SECS, limit as i64],
    )?;
    Ok(())
}

/// Unbounded sweep + `wal_checkpoint(TRUNCATE)`. Backs `quorum sweep`.
pub fn sweep_all(conn: &Connection, now: i64) -> Result<()> {
    reap_lapsed_tasks(conn, now, usize::MAX)?;
    cascade_dead_deps(conn, now, usize::MAX)?;
    conn.execute("DELETE FROM messages WHERE expires_at <= ?1", params![now])?;
    conn.execute("DELETE FROM events WHERE expires_at <= ?1", params![now])?;
    conn.execute("DELETE FROM errors WHERE expires_at <= ?1", params![now])?;
    conn.execute("DELETE FROM claims WHERE expires_at <= ?1", params![now])?;
    // Issue #101 — see sweep_on_write for rationale.
    conn.execute(
        "DELETE FROM agent_sessions WHERE expires_at <= ?1",
        params![now],
    )?;
    conn.execute(
        "DELETE FROM activity_events WHERE expires_at <= ?1",
        params![now],
    )?;
    crate::task_messages::expire_stale_deliveries(conn, now, usize::MAX)?;
    conn.execute(
        "DELETE FROM task_messages WHERE expires_at <= ?1",
        params![now],
    )?;
    conn.execute(
        "DELETE FROM tasks WHERE status='done' AND updated_at < ?1",
        params![now - DONE_TASK_TTL_SECS],
    )?;
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
    fn cascade_cancels_task_blocked_by_cancelled_dep() {
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
        assert_eq!(t.status, "cancelled", "child must be cancelled by cascade");
        // Event emitted.
        let target = format!("task#{child}");
        let evs = crate::events::list(&c, 0, Some(&target), 10, 300).unwrap();
        assert!(
            evs.iter().any(|e| e.kind == "dep_cascade"),
            "dep_cascade event must be emitted"
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
            "cancelled"
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
        // First cascade: b cancelled.
        cascade_dead_deps(&c, 300, 100).unwrap();
        assert_eq!(
            crate::tasks::get(&c, b).unwrap().unwrap().status,
            "cancelled"
        );
        // c still open (b just became cancelled, need another sweep).
        // Second cascade: c cancelled.
        cascade_dead_deps(&c, 400, 100).unwrap();
        assert_eq!(
            crate::tasks::get(&c, ch).unwrap().unwrap().status,
            "cancelled"
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
