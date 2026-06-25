//! The shared work queue.
//!
//! Tasks move through five states: `open → claimed → done → closed`, plus terminal
//! `cancelled`. The agent's write footprint per task is exactly two calls — `task-claim`
//! (`open → claimed`) then `task-update --status done` (`claimed → done`). The reviewer
//! (issue #10) drives `done → closed` (terminal) or `done → open` (rework); this module
//! exposes `closed` as a valid value but does not own that transition.
//!
//! Claiming is atomic via a guarded `UPDATE ... WHERE status='open' RETURNING`, so two agents
//! can never claim the same task. A claim also takes a **renewable lease** on `task#<id>`
//! (reusing the `claims` table): the assignee `renew`s on long work, and a lapsed lease lets
//! the sweep-on-write reaper return the task to `open` (see `sweep::reap_lapsed_tasks`).
//! `done` tasks are physically reclaimed by the sweeper after a TTL.

use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

/// Valid task statuses. `open`/`claimed` are system-driven (claim/release/reaper); `done` is
/// the executor's only settable status; `closed`/`cancelled` are terminal (review #10 / cancel).
pub const STATUSES: &[&str] = &["open", "claimed", "done", "closed", "cancelled"];

/// The lease target string for a task id — the key under which a task's renewable claim-lease
/// lives in the shared `claims` table.
pub fn lease_target(id: i64) -> String {
    format!("task#{id}")
}

/// Deactivate any live lease on a task within an existing transaction. Idempotent.
fn deactivate_lease(tx: &rusqlite::Transaction, id: i64, now: i64) -> Result<()> {
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at > ?2",
        params![lease_target(id), now],
    )?;
    Ok(())
}

/// A task row.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub body: Option<String>,
    pub status: String,
    pub priority: i64,
    pub labels: Option<String>,
    pub assignee: Option<String>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub refs: Option<String>,
}

/// Fields a [`update`] may change. `None` leaves the field untouched.
///
/// Note: there is no `assignee` field — reassignment is intentionally not a `task-update`
/// operation under the lease model (it would leave the `task#<id>` lease with the old holder,
/// so the new assignee couldn't renew and the reaper could reclaim under them). Hand-off is
/// `task-release` (→ `open`) followed by a fresh `task-claim`.
#[derive(Default)]
pub struct TaskUpdate<'a> {
    pub status: Option<&'a str>,
    pub body: Option<&'a str>,
    pub refs: Option<&'a str>,
}

const COLS: &str =
    "id, title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs";

fn row_to_task(r: &Row) -> rusqlite::Result<Task> {
    Ok(Task {
        id: r.get(0)?,
        title: r.get(1)?,
        body: r.get(2)?,
        status: r.get(3)?,
        priority: r.get(4)?,
        labels: r.get(5)?,
        assignee: r.get(6)?,
        created_by: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
        refs: r.get(10)?,
    })
}

fn begin(conn: &mut Connection) -> Result<rusqlite::Transaction<'_>> {
    Ok(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
}

/// Create a new `open` task. Returns its id.
#[allow(clippy::too_many_arguments)]
pub fn create(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    now: i64,
) -> Result<i64> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, created_by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs)
         VALUES (?1, ?2, 'open', ?3, ?4, NULL, ?5, ?6, ?6, ?7)",
        params![title, body, priority, labels, created_by, now, refs],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

/// Atomically claim a task and take a renewable `ttl`-second lease on it. With `task_id`,
/// claims that specific open task; without, claims the highest-priority open task. Returns
/// `None` if nothing claimable (already taken / none open).
///
/// The guarded `UPDATE ... WHERE status='open'` is the single-winner gate; the lease row
/// (`target='task#<id>'`) is written in the same transaction so claim + lease are atomic. The
/// lease lets a lost agent's task be reaped back to `open` (see `sweep::reap_lapsed_tasks`).
pub fn claim(
    conn: &mut Connection,
    agent: &str,
    task_id: Option<i64>,
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let task = match task_id {
        Some(id) => tx
            .query_row(
                &format!(
                    "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                     WHERE id=?3 AND status='open' RETURNING {COLS}"
                ),
                params![agent, now, id],
                row_to_task,
            )
            .optional()?,
        None => tx
            .query_row(
                &format!(
                    "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                     WHERE id=(SELECT id FROM tasks WHERE status='open'
                               ORDER BY priority DESC, id ASC LIMIT 1)
                     RETURNING {COLS}"
                ),
                params![agent, now],
                row_to_task,
            )
            .optional()?,
    };
    if let Some(t) = &task {
        // Take the lease in the same txn. Reap a dead lease first (a reopened task may carry a
        // stale row), then insert active. Only the single status-winner reaches here, so the
        // partial unique index won't collide.
        let target = lease_target(t.id);
        tx.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
            params![target, now],
        )?;
        tx.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
            params![target, agent, now, now + ttl],
        )?;
    }
    tx.commit()?;
    Ok(task)
}

/// Update a task. Fails loud ([`QuorumError::NotHolder`]) if `agent` is not the assignee.
///
/// The only status an agent may set is `done` (the executor's submit, `claimed → done`) —
/// there is no agent-set intermediate state. Other transitions have dedicated paths: `open`/
/// `claimed` are system-driven (claim/release/reaper), `cancelled` via [`cancel`], and
/// `closed`/reopen are the reviewer's (issue #10). Any other status value is a usage error.
/// Setting `done` deactivates the task's lease (the work is submitted; no reclaim needed).
pub fn update(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    fields: &TaskUpdate,
    now: i64,
) -> Result<Task> {
    if let Some(s) = fields.status {
        if !STATUSES.contains(&s) {
            return Err(QuorumError::Usage(format!("invalid status: {s}")));
        }
        if s != "done" {
            return Err(QuorumError::Usage(format!(
                "task-update can only set status 'done'; use task-release (→open), \
                 task-cancel (→cancelled), or review automation (→closed) for {s}"
            )));
        }
    }
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    // COALESCE keeps the existing value when a field is None. Guard on assignee = caller.
    let n = tx.execute(
        "UPDATE tasks SET
            status   = COALESCE(?2, status),
            body     = COALESCE(?3, body),
            refs     = COALESCE(?4, refs),
            updated_at = ?5
         WHERE id=?1 AND assignee=?6",
        params![id, fields.status, fields.body, fields.refs, now, agent],
    )?;
    if n == 0 {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }
    // Submitting `done` ends the active phase: drop the lease so the reaper can't reclaim the
    // task and so a future reopen (#10) can take a fresh lease without a stale-row collision.
    if fields.status == Some("done") {
        deactivate_lease(&tx, id, now)?;
    }
    let task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    tx.commit()?;
    Ok(task)
}

/// Release a claimed task back to `open` (give-up). Fails loud ([`QuorumError::NotHolder`]) if
/// `agent` is not the current assignee of a `claimed` task. Drops the lease and clears the
/// assignee so any agent can re-claim.
pub fn release(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let n = tx.execute(
        "UPDATE tasks SET status='open', assignee=NULL, updated_at=?1
         WHERE id=?2 AND assignee=?3 AND status='claimed'",
        params![now, id, agent],
    )?;
    if n == 0 {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }
    deactivate_lease(&tx, id, now)?;
    let task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    tx.commit()?;
    Ok(task)
}

/// Extend the lease on a task you hold. Fails loud if `agent` is not the active, unexpired
/// holder of a `claimed` task (lapsed lease → must re-claim). Returns the task.
pub fn renew(conn: &mut Connection, agent: &str, id: i64, ttl: i64, now: i64) -> Result<Task> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let n = tx.execute(
        "UPDATE claims SET expires_at=?1
         WHERE target=?2 AND holder=?3 AND active=1 AND expires_at > ?4",
        params![now + ttl, lease_target(id), agent, now],
    )?;
    if n == 0 {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }
    let task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    tx.commit()?;
    Ok(task)
}

/// Cancel a task (terminal won't-do). Guard: the **creator OR the assignee** may cancel (a
/// wider guard than `done`, which is assignee-only). Already-terminal tasks
/// (`closed`/`cancelled`) cannot be cancelled. Drops the lease. Fails loud
/// ([`QuorumError::NotHolder`]) if the task is missing, terminal, or `agent` is neither
/// creator nor assignee.
///
/// Per-dependent notification (one event per blocked dependent) lands with the dependency
/// model (#2); there is no dependency schema yet, so nothing to notify today.
pub fn cancel(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let n = tx.execute(
        "UPDATE tasks SET status='cancelled', updated_at=?1
         WHERE id=?2 AND (created_by=?3 OR assignee=?3)
               AND status NOT IN ('cancelled', 'closed')",
        params![now, id, agent],
    )?;
    if n == 0 {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }
    deactivate_lease(&tx, id, now)?;
    let task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    tx.commit()?;
    Ok(task)
}

/// List tasks, optionally filtered by status, label, and/or assignee. Read-only.
pub fn list(
    conn: &Connection,
    status: Option<&str>,
    label: Option<&str>,
    assignee: Option<&str>,
) -> Result<Vec<Task>> {
    // Dynamic but fully parameterized — no value is interpolated into SQL.
    let label_pat = label.map(|l| format!("%\"{l}\"%"));
    let mut sql = format!("SELECT {COLS} FROM tasks WHERE 1=1");
    if status.is_some() {
        sql.push_str(" AND status=:status");
    }
    if label_pat.is_some() {
        sql.push_str(" AND labels LIKE :label");
    }
    if assignee.is_some() {
        sql.push_str(" AND assignee=:assignee");
    }
    sql.push_str(" ORDER BY priority DESC, id ASC");

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<(&str, &dyn rusqlite::ToSql)> = {
        let mut v: Vec<(&str, &dyn rusqlite::ToSql)> = Vec::new();
        if let Some(s) = &status {
            v.push((":status", s));
        }
        if let Some(p) = &label_pat {
            v.push((":label", p));
        }
        if let Some(a) = &assignee {
            v.push((":assignee", a));
        }
        v
    };
    let tasks = stmt
        .query_map(&params[..], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tasks)
}

/// Fetch a single task by id.
pub fn get(conn: &Connection, id: i64) -> Result<Option<Task>> {
    let task = conn
        .query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )
        .optional()?;
    Ok(task)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    const TTL: i64 = 3600;

    /// Is there a live (active, unexpired) lease on this task?
    fn has_live_lease(c: &Connection, id: i64, now: i64) -> bool {
        c.query_row(
            "SELECT count(*) FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
            params![lease_target(id), now],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn create_then_claim_sets_assignee_and_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "fix bug", None, 0, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", Some(id), TTL, 1000).unwrap().unwrap();
        assert_eq!(t.status, "claimed");
        assert_eq!(t.assignee.as_deref(), Some("A"));
        // The claim took a renewable lease on task#<id>.
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn second_claim_of_same_task_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        assert!(claim(&mut c, "A", Some(id), TTL, 1000).unwrap().is_some());
        assert!(claim(&mut c, "B", Some(id), TTL, 1000).unwrap().is_none());
    }

    #[test]
    fn claim_without_id_picks_highest_priority() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "low", None, 1, None, None, 1000).unwrap();
        create(&mut c, "boss", "high", None, 9, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", None, TTL, 1000).unwrap().unwrap();
        assert_eq!(t.title, "high");
    }

    #[test]
    fn claim_nothing_open_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(claim(&mut c, "A", None, TTL, 1000).unwrap().is_none());
    }

    #[test]
    fn update_by_assignee_sets_done_and_drops_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        let t = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        assert_eq!(t.status, "done");
        assert_eq!(t.updated_at, 1100);
        // Submitting `done` drops the lease.
        assert!(!has_live_lease(&c, id, 1100));
    }

    #[test]
    fn update_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "B",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
    }

    #[test]
    fn update_rejects_invalid_status() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("frobnicate"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)));
    }

    #[test]
    fn update_rejects_non_done_status() {
        // No agent-set intermediate state: only `done` is settable via task-update.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        for bad in ["open", "claimed", "closed", "cancelled"] {
            let err = update(
                &mut c,
                "A",
                id,
                &TaskUpdate {
                    status: Some(bad),
                    ..Default::default()
                },
                1100,
            )
            .unwrap_err();
            assert!(
                matches!(err, QuorumError::Usage(_)),
                "status {bad} must be rejected"
            );
        }
    }

    #[test]
    fn release_returns_task_to_open_and_drops_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        let t = release(&mut c, "A", id, 1100).unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        assert!(!has_live_lease(&c, id, 1100));
        // Released task is re-claimable by anyone.
        assert!(claim(&mut c, "B", Some(id), TTL, 1200).unwrap().is_some());
    }

    #[test]
    fn release_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        assert!(matches!(
            release(&mut c, "B", id, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn renew_extends_lease_for_holder_only() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), 100, 1000).unwrap(); // expires at 1100
                                                          // Holder renews near expiry → lease lives past the old boundary.
        renew(&mut c, "A", id, 100, 1090).unwrap(); // now expires at 1190
        assert!(has_live_lease(&c, id, 1150));
        // A non-holder cannot renew.
        assert!(matches!(
            renew(&mut c, "B", id, 100, 1150).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn renew_fails_once_lease_lapsed() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), 100, 1000).unwrap(); // dead at 1100
                                                          // At/after expiry the lease is gone → must re-claim, renew fails loud.
        assert!(matches!(
            renew(&mut c, "A", id, 100, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn cancel_allowed_for_creator_or_assignee() {
        let (_d, mut c) = open_tmp();
        // Creator can cancel an open task.
        let id1 = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        assert_eq!(
            cancel(&mut c, "boss", id1, 1100).unwrap().status,
            "cancelled"
        );
        // Assignee can cancel a claimed task (and the lease is dropped).
        let id2 = create(&mut c, "boss", "y", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id2), TTL, 1000).unwrap();
        assert_eq!(cancel(&mut c, "A", id2, 1100).unwrap().status, "cancelled");
        assert!(!has_live_lease(&c, id2, 1100));
    }

    #[test]
    fn cancel_rejected_for_stranger_and_terminal() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), TTL, 1000).unwrap();
        // A bystander (neither creator nor assignee) cannot cancel.
        assert!(matches!(
            cancel(&mut c, "C", id, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
        // Already terminal → cannot cancel again.
        cancel(&mut c, "boss", id, 1100).unwrap();
        assert!(matches!(
            cancel(&mut c, "boss", id, 1200).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn list_filters_by_status_and_label() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "a", None, 0, Some("[\"ui\"]"), None, 1000).unwrap();
        create(&mut c, "boss", "b", None, 0, Some("[\"api\"]"), None, 1000).unwrap();
        assert_eq!(list(&c, Some("open"), None, None).unwrap().len(), 2);
        assert_eq!(list(&c, None, Some("ui"), None).unwrap().len(), 1);
        assert_eq!(list(&c, Some("done"), None, None).unwrap().len(), 0);
    }

    #[test]
    fn get_returns_task_or_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        assert!(get(&c, id).unwrap().is_some());
        assert!(get(&c, 9999).unwrap().is_none());
    }
}
