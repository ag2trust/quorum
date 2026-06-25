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
///
/// `depends_on` is a JSON array of task ids this task waits on; `None` = no deps. The value
/// is validated at create-time (must be a JSON array of i64), so reads NEVER fault on bad
/// JSON.
///
/// `ready` is **derived** (true iff every listed dep exists and is `closed`) — populated by
/// [`get`], [`list`], and [`update`]; [`claim`] always sees `true` (a non-ready task can never
/// be claimed). A task with no deps is always ready.
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
    pub depends_on: Option<String>,
    pub ready: bool,
}

/// One append-only breadcrumb attached to a task. Ordered by `id` (= insertion order).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Note {
    pub id: i64,
    pub ts: i64,
    pub agent: String,
    pub body: String,
}

/// A task plus its notes, returned by [`get_with_notes`] (used by the CLI `task-get`). The
/// `Task` fields are inlined into the JSON output so it stays a single flat object plus a
/// `notes` array — easy for an agent to read in one go.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskDetail {
    #[serde(flatten)]
    pub task: Task,
    pub notes: Vec<Note>,
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
    "id, title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs, depends_on";

fn row_to_task(r: &Row) -> rusqlite::Result<Task> {
    // `ready` is set false by default and filled in by the caller (get/list/update/claim).
    // Materializing it requires another query, which the raw row reader can't do.
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
        depends_on: r.get(11)?,
        ready: false,
    })
}

/// Validate `--depends-on` at the boundary: parse as a JSON array of i64 and return a clean
/// usage error if it doesn't. Storing unvalidated JSON would let one bad row poison every
/// `task-list`/`task-get`/`task-cancel` call (json_each errors propagate from `compute_ready`)
/// — including the cancel that would otherwise let an operator recover. Reject early; never
/// store bad JSON.
fn validate_depends_on(s: &str) -> Result<()> {
    serde_json::from_str::<Vec<i64>>(s).map_err(|e| {
        QuorumError::Usage(format!(
            "--depends-on must be a JSON array of task ids (e.g. '[1,3]'): {e}"
        ))
    })?;
    Ok(())
}

/// Compute whether a task's deps are all `closed`. `None`/empty deps = ready.
///
/// Issue #2 alignment: a dependent unblocks only when every dep is **`closed`** (reviewed +
/// finalized per #9/#10), not `done` (submitted/unreviewed). A missing or `cancelled` dep is
/// treated as unmet — never satisfied — so the dependent stays unclaimable.
///
/// Safe-by-construction: bad JSON can never reach this function because [`validate_depends_on`]
/// rejects malformed input in [`create`].
fn compute_ready(conn: &Connection, depends_on: &Option<String>) -> Result<bool> {
    let Some(json) = depends_on.as_deref() else {
        return Ok(true);
    };
    let unmet: i64 = conn.query_row(
        "SELECT count(*) FROM json_each(?1)
         WHERE NOT EXISTS (
             SELECT 1 FROM tasks d WHERE d.id = json_each.value AND d.status = 'closed'
         )",
        params![json],
        |r| r.get(0),
    )?;
    Ok(unmet == 0)
}

fn begin(conn: &mut Connection) -> Result<rusqlite::Transaction<'_>> {
    Ok(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
}

/// Create a new `open` task. Returns its id.
///
/// `depends_on` is a JSON array of task ids this task waits on (e.g. `"[1,3]"`); `None` =
/// no deps. Validated at the boundary as a JSON array of i64 — malformed input is rejected
/// with a usage error (exit 2) BEFORE the row lands, so no read path can be poisoned.
#[allow(clippy::too_many_arguments)]
pub fn create(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    depends_on: Option<&str>,
    now: i64,
) -> Result<i64> {
    // Reject malformed depends_on at the boundary. See validate_depends_on for why.
    if let Some(s) = depends_on {
        validate_depends_on(s)?;
    }
    let tx = begin(conn)?;
    crate::agents::touch(&tx, created_by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs, depends_on)
         VALUES (?1, ?2, 'open', ?3, ?4, NULL, ?5, ?6, ?6, ?7, ?8)",
        params![title, body, priority, labels, created_by, now, refs, depends_on],
    )?;
    let id = tx.last_insert_rowid();
    let body_str = match labels {
        Some(l) => format!("created (prio {priority}, labels {l})"),
        None => format!("created (prio {priority})"),
    };
    crate::events::emit(&tx, "task_created", &lease_target(id), &body_str, now)?;
    tx.commit()?;
    Ok(id)
}

/// Atomically claim a task and take a renewable `ttl`-second lease on it. With `task_id`,
/// claims that specific open task; without, claims the highest-priority open task whose
/// `labels` contain every label in `match_labels` (empty slice = no label filter, current
/// behaviour). Returns `None` if nothing claimable (already taken / none open / no match).
///
/// The guarded `UPDATE ... WHERE status='open'` is the single-winner gate; the lease row
/// (`target='task#<id>'`) is written in the same transaction so claim + lease are atomic. The
/// lease lets a lost agent's task be reaped back to `open` (see `sweep::reap_lapsed_tasks`).
///
/// `match_labels` is intentionally AND-only and exact-match (no expression grammar) — quorum
/// stays agnostic to what a label means; pattern is `%"<label>"%` against the JSON-array
/// `labels` column, identical to [`list`].
pub fn claim(
    conn: &mut Connection,
    agent: &str,
    task_id: Option<i64>,
    match_labels: &[&str],
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    // #2 dep-gate clause: claimable only when every dep id is `closed` (or no deps).
    // Gate on `closed` (reviewed + finalized per #9/#10), NOT `done` (submitted/unreviewed) —
    // don't let dependents build on unreviewed work. Composed with the match-label filter as
    // additional ANDs; both are pure-narrowing.
    const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
        SELECT 1 FROM json_each(depends_on) je
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'closed'
        )
    ))";
    let mut task = match task_id {
        Some(id) => tx
            .query_row(
                // Even with an explicit --task-id, the dep gate applies: the acceptance bar
                // is "a task with an unmet dep is never returned by task-claim." An agent that
                // truly needs to bypass the gate can wait or cancel the dep.
                &format!(
                    "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                     WHERE id=?3 AND status='open' AND {DEP_READY_CLAUSE}
                     RETURNING {COLS}"
                ),
                params![agent, now, id],
                row_to_task,
            )
            .optional()?,
        None => {
            // Build the open-task selector with one `AND labels LIKE ?N` per match-label,
            // plus the dep-ready clause. Patterns are bound as parameters; only the
            // placeholder count is interpolated (no value reaches SQL as a string).
            let mut selector =
                format!("SELECT id FROM tasks WHERE status='open' AND {DEP_READY_CLAUSE}");
            for i in 0..match_labels.len() {
                // Params are 1-indexed; ?1 = agent, ?2 = now, so label params start at ?3.
                use std::fmt::Write as _;
                let _ = write!(selector, " AND labels LIKE ?{}", i + 3);
            }
            selector.push_str(" ORDER BY priority DESC, id ASC LIMIT 1");
            let sql = format!(
                "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                 WHERE id=({selector}) RETURNING {COLS}"
            );
            let label_pats: Vec<String> =
                match_labels.iter().map(|l| format!("%\"{l}\"%")).collect();
            let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&agent, &now];
            for p in &label_pats {
                bind.push(p);
            }
            tx.query_row(&sql, &bind[..], row_to_task).optional()?
        }
    };
    if let Some(t) = &mut task {
        // A claim only fires when the row was claimable, so deps are by construction met.
        t.ready = true;
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
        crate::events::emit(
            &tx,
            "task_claimed",
            &target,
            &format!("taken by {agent}"),
            now,
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
        crate::events::emit(
            &tx,
            "task_done",
            &lease_target(id),
            &format!("done by {agent}"),
            now,
        )?;
    }
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
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
    crate::events::emit(
        &tx,
        "task_released",
        &lease_target(id),
        &format!("released by {agent}"),
        now,
    )?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
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
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    crate::events::emit(
        &tx,
        "task_renewed",
        &lease_target(id),
        &format!("renewed by {agent}"),
        now,
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
    crate::events::emit(
        &tx,
        "task_cancelled",
        &lease_target(id),
        &format!("cancelled by {agent}"),
        now,
    )?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
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
    let mut tasks: Vec<Task> = stmt
        .query_map(&params[..], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for t in &mut tasks {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(tasks)
}

/// Fetch a single task by id. `ready` is filled per the dependency rule (#2).
pub fn get(conn: &Connection, id: i64) -> Result<Option<Task>> {
    let mut task = conn
        .query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )
        .optional()?;
    if let Some(t) = &mut task {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(task)
}

/// Fetch a single task and its append-only note history (oldest first).
pub fn get_with_notes(conn: &Connection, id: i64) -> Result<Option<TaskDetail>> {
    let Some(task) = get(conn, id)? else {
        return Ok(None);
    };
    let notes = notes_for(conn, id)?;
    Ok(Some(TaskDetail { task, notes }))
}

/// Append a breadcrumb to `task_id`. Returns the new note's id, or `None` if the task does
/// not exist (mapped to a clean exit 1 by the CLI, matching `task-get` semantics). Notes are
/// append-only — there is no edit/delete. Any agent may add a note: notes are public
/// context for whoever picks the task up next, so the assignee guard from [`update`] does
/// not apply.
pub fn add_note(
    conn: &mut Connection,
    agent: &str,
    task_id: i64,
    body: &str,
    now: i64,
) -> Result<Option<i64>> {
    let tx = begin(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    // Verify the task exists *inside the transaction* so it can't disappear between the
    // check and the INSERT. SQLite has no FK enforced here (v1 invariant: no FKs), so this
    // is the load-bearing existence check.
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
        params![task_id],
        |r| r.get(0),
    )?;
    if !exists {
        tx.commit()?;
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![task_id, now, agent, body],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(Some(id))
}

fn notes_for(conn: &Connection, task_id: i64) -> Result<Vec<Note>> {
    let mut stmt = conn
        .prepare("SELECT id, ts, agent, body FROM task_notes WHERE task_id=?1 ORDER BY id ASC")?;
    let notes = stmt
        .query_map(params![task_id], |r| {
            Ok(Note {
                id: r.get(0)?,
                ts: r.get(1)?,
                agent: r.get(2)?,
                body: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(notes)
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
        let id = create(&mut c, "boss", "fix bug", None, 0, None, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "claimed");
        assert_eq!(t.assignee.as_deref(), Some("A"));
        // The claim took a renewable lease on task#<id>.
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn second_claim_of_same_task_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert!(claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_some());
        assert!(claim(&mut c, "B", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn claim_without_id_picks_highest_priority() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "low", None, 1, None, None, None, 1000).unwrap();
        create(&mut c, "boss", "high", None, 9, None, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(t.title, "high");
    }

    #[test]
    fn claim_nothing_open_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
    }

    // -- --match-label (issue #1) -----------------------------------------------------------

    #[test]
    fn match_label_filters_to_matching_task() {
        let (_d, mut c) = open_tmp();
        // Two open tasks; the higher-priority one lacks the requested label.
        create(
            &mut c,
            "boss",
            "high-no-label",
            None,
            9,
            Some(r#"["tier:opus-46"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "low-with-label",
            None,
            1,
            Some(r#"["tier:opus-47","lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
        assert_eq!(t.title, "low-with-label");
    }

    #[test]
    fn match_label_no_match_is_none_not_error() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "rust",
            None,
            5,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        // Requesting a label nothing carries → clean None (caller exits 1), not an error.
        assert!(claim(&mut c, "A", None, &["lang:python"], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn match_label_is_and_across_repeats() {
        let (_d, mut c) = open_tmp();
        // Only one task carries BOTH labels; AND must skip the singletons.
        create(
            &mut c,
            "boss",
            "rust-only",
            None,
            9,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "tier-only",
            None,
            9,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "rust-and-tier",
            None,
            5,
            Some(r#"["lang:rust","tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["lang:rust", "tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    #[test]
    fn match_label_picks_highest_priority_among_matches() {
        let (_d, mut c) = open_tmp();
        // Three matching tasks at different priorities; claim must pick the top.
        create(
            &mut c,
            "boss",
            "low",
            None,
            1,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "high",
            None,
            9,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "mid",
            None,
            5,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["k"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    #[test]
    fn match_label_takes_lease_just_like_unfiltered_claim() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "x",
            None,
            0,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["k"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, id);
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn update_by_assignee_sets_done_and_drops_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
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
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
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
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
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
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
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
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = release(&mut c, "A", id, 1100).unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        assert!(!has_live_lease(&c, id, 1100));
        // Released task is re-claimable by anyone.
        assert!(claim(&mut c, "B", Some(id), &[], TTL, 1200)
            .unwrap()
            .is_some());
    }

    #[test]
    fn release_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        assert!(matches!(
            release(&mut c, "B", id, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn renew_extends_lease_for_holder_only() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap(); // expires at 1100
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
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap(); // dead at 1100
                                                               // At/after expiry the lease is gone → must re-claim, renew fails loud.
        assert!(matches!(
            renew(&mut c, "A", id, 100, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn renew_emits_task_renewed_event() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap();
        renew(&mut c, "A", id, 200, 1050).unwrap();
        let evs = crate::events::list(&c, 0, Some(&format!("task#{id}")), 10, 1050).unwrap();
        let renewed: Vec<_> = evs.iter().filter(|e| e.kind == "task_renewed").collect();
        assert_eq!(renewed.len(), 1);
        assert!(renewed[0].body.contains("renewed by A"));
    }

    #[test]
    fn cancel_allowed_for_creator_or_assignee() {
        let (_d, mut c) = open_tmp();
        // Creator can cancel an open task.
        let id1 = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert_eq!(
            cancel(&mut c, "boss", id1, 1100).unwrap().status,
            "cancelled"
        );
        // Assignee can cancel a claimed task (and the lease is dropped).
        let id2 = create(&mut c, "boss", "y", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id2), &[], TTL, 1000).unwrap();
        assert_eq!(cancel(&mut c, "A", id2, 1100).unwrap().status, "cancelled");
        assert!(!has_live_lease(&c, id2, 1100));
    }

    #[test]
    fn cancel_rejected_for_stranger_and_terminal() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
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
        create(
            &mut c,
            "boss",
            "a",
            None,
            0,
            Some("[\"ui\"]"),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "b",
            None,
            0,
            Some("[\"api\"]"),
            None,
            None,
            1000,
        )
        .unwrap();
        assert_eq!(list(&c, Some("open"), None, None).unwrap().len(), 2);
        assert_eq!(list(&c, None, Some("ui"), None).unwrap().len(), 1);
        assert_eq!(list(&c, Some("done"), None, None).unwrap().len(), 0);
    }

    #[test]
    fn get_returns_task_or_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert!(get(&c, id).unwrap().is_some());
        assert!(get(&c, 9999).unwrap().is_none());
    }

    // -- task dependencies (issue #2) -------------------------------------------------------

    /// Force a task to `closed` directly in the DB — there is no public `close()` helper yet
    /// (that's the review automation, issue #10). Bypasses the executor/reviewer split so the
    /// dep-gate tests can simulate the resolved state without staging the whole #10 machinery.
    fn force_close(c: &Connection, id: i64) {
        c.execute("UPDATE tasks SET status='closed' WHERE id=?1", params![id])
            .unwrap();
    }

    #[test]
    fn create_rejects_malformed_depends_on_before_storing() {
        // Cobble-x7M's blocking finding on #18 v1: unvalidated --depends-on poisons every
        // subsequent task-list/task-get/task-cancel via compute_ready -> json_each. Fix:
        // reject malformed JSON at the boundary, never store it.
        let (_d, mut c) = open_tmp();
        // Forgot the brackets (Cobble's repro). Must be a clean Usage error (exit 2).
        let err = create(
            &mut c,
            "boss",
            "bad",
            None,
            0,
            None,
            None,
            Some("1,2"),
            1000,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)), "got {err:?}");
        // And nothing was inserted — list is empty, queue not poisoned.
        assert_eq!(list(&c, None, None, None).unwrap().len(), 0);
        // Other malformed shapes — string, object, array-of-strings — all rejected.
        for bad in &["garbage", "{}", r#"["one"]"#, "[1, 2,"] {
            assert!(matches!(
                create(&mut c, "boss", "x", None, 0, None, None, Some(bad), 1000).unwrap_err(),
                QuorumError::Usage(_)
            ));
        }
        // Empty array and well-formed deps still accepted.
        assert!(create(
            &mut c,
            "boss",
            "ok-empty",
            None,
            0,
            None,
            None,
            Some("[]"),
            1000
        )
        .is_ok());
        assert!(create(
            &mut c,
            "boss",
            "ok-one",
            None,
            0,
            None,
            None,
            Some("[1]"),
            1000
        )
        .is_ok());
    }

    #[test]
    fn auto_pick_skips_task_with_unmet_dep() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 5, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            5,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        // Auto-pick: only `dep` is eligible. The dependent must be invisible to auto-pick
        // until its dep is `closed`.
        let first = claim(&mut c, "A", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(first.id, dep);
        // dep is now `claimed`, dependent stays gated.
        assert!(claim(&mut c, "B", None, &[], TTL, 1000).unwrap().is_none());
        // Forcing dep → done is *not enough* per the #9/#10 alignment: gate is on `closed`.
        c.execute("UPDATE tasks SET status='done' WHERE id=?1", params![dep])
            .unwrap();
        assert!(claim(&mut c, "B", None, &[], TTL, 1000).unwrap().is_none());
        // Once dep is `closed`, the dependent becomes claimable.
        force_close(&c, dep);
        let unblocked = claim(&mut c, "B", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(unblocked.id, dependent);
    }

    #[test]
    fn explicit_task_id_also_respects_dep_gate() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
        force_close(&c, dep);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_some());
    }

    #[test]
    fn missing_dep_id_blocks_forever() {
        let (_d, mut c) = open_tmp();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some("[9999]"),
            1000,
        )
        .unwrap();
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cancelled_dep_blocks_dependent() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        cancel(&mut c, "boss", dep, 1100).unwrap();
        assert!(claim(&mut c, "A", None, &[], TTL, 1100).unwrap().is_none());
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1100)
            .unwrap()
            .is_none());
    }

    #[test]
    fn multiple_deps_all_must_be_closed() {
        let (_d, mut c) = open_tmp();
        let d1 = create(&mut c, "boss", "d1", None, 0, None, None, None, 1000).unwrap();
        let d2 = create(&mut c, "boss", "d2", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            9,
            None,
            None,
            Some(&format!("[{d1},{d2}]")),
            1000,
        )
        .unwrap();
        force_close(&c, d1);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
        force_close(&c, d2);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_some());
    }

    #[test]
    fn get_surfaces_depends_on_and_ready() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        let solo = get(&c, dep).unwrap().unwrap();
        assert!(solo.depends_on.is_none());
        assert!(solo.ready);
        let t = get(&c, dependent).unwrap().unwrap();
        assert_eq!(t.depends_on.as_deref(), Some(format!("[{dep}]").as_str()));
        assert!(!t.ready);
        force_close(&c, dep);
        let t = get(&c, dependent).unwrap().unwrap();
        assert!(t.ready);
    }

    #[test]
    fn dep_gate_composes_with_match_label_filter() {
        // Two AND clauses (dep-ready + label-match) co-exist in the same auto-pick selector.
        // Smoke-test the composition: only a task matching BOTH label AND ready-deps wins.
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        // (a) labeled but gated by an unmet dep
        create(
            &mut c,
            "boss",
            "labeled-gated",
            None,
            9,
            Some(r#"["tier:opus-47"]"#),
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        // (b) ready but wrong label
        create(
            &mut c,
            "boss",
            "ready-no-label",
            None,
            9,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        // (c) labeled AND ready
        let want = create(
            &mut c,
            "boss",
            "labeled-and-ready",
            None,
            1,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want, "claim must pick the labeled+ready task");
    }

    // -- Task notes (issue #3) -----------------------------------------------------------

    #[test]
    fn add_note_appends_and_get_with_notes_returns_in_order() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let n1 = add_note(&mut c, "A", id, "first crumb", 1100)
            .unwrap()
            .unwrap();
        let n2 = add_note(&mut c, "B", id, "second crumb", 1200)
            .unwrap()
            .unwrap();
        assert!(n2 > n1);

        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes.len(), 2);
        assert_eq!(detail.notes[0].agent, "A");
        assert_eq!(detail.notes[0].body, "first crumb");
        assert_eq!(detail.notes[0].ts, 1100);
        assert_eq!(detail.notes[1].agent, "B");
        assert_eq!(detail.notes[1].body, "second crumb");
        assert_eq!(detail.notes[1].ts, 1200);
    }

    #[test]
    fn add_note_on_missing_task_returns_none() {
        let (_d, mut c) = open_tmp();
        assert!(add_note(&mut c, "A", 9999, "into the void", 1000)
            .unwrap()
            .is_none());
        // and nothing was inserted
        let n: i64 = c
            .query_row("SELECT count(*) FROM task_notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn notes_preserve_byte_exact_body() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let body = "héllo \"world\"\n`$x`\nmultiline\n";
        add_note(&mut c, "A", id, body, 1100).unwrap().unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes[0].body, body);
    }

    #[test]
    fn anyone_can_add_a_note_no_assignee_guard() {
        // Notes are public breadcrumbs — the assignee restriction from `update` does NOT
        // apply, so a non-assignee agent can append. (Differentiates the contract clearly:
        // `update` mutates the task; `add_note` annotates it.)
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // B is not the assignee but can still leave a note
        let nid = add_note(&mut c, "B", id, "from a watcher", 1100)
            .unwrap()
            .unwrap();
        assert!(nid > 0);
    }

    #[test]
    fn notes_are_not_swept_when_other_rows_expire() {
        // Notes have no expires_at column — the sweeper's TTL pass cannot evict them.
        // Verify by running a sweep at a far-future `now` and seeing the note persist.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        add_note(&mut c, "A", id, "durable", 1100).unwrap().unwrap();
        crate::sweep::sweep_all(&c, 9_999_999).unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes.len(), 1);
        assert_eq!(detail.notes[0].body, "durable");
    }

    #[test]
    fn task_without_notes_yields_empty_notes_array() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert!(detail.notes.is_empty());
    }

    #[test]
    fn get_with_notes_on_missing_task_is_none() {
        let (_d, c) = open_tmp();
        assert!(get_with_notes(&c, 9999).unwrap().is_none());
    }
}
