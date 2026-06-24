//! The shared work queue.
//!
//! Tasks have a lifecycle (`open` → `claimed`/`in_progress`/`blocked` → `done`/`cancelled`).
//! Claiming is atomic via a guarded `UPDATE ... WHERE status='open' RETURNING`, so two agents
//! can never claim the same task. Done tasks are reclaimed by the sweeper after a TTL.

use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

/// Valid task statuses.
pub const STATUSES: &[&str] = &[
    "open",
    "claimed",
    "in_progress",
    "blocked",
    "done",
    "cancelled",
];

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
#[derive(Default)]
pub struct TaskUpdate<'a> {
    pub status: Option<&'a str>,
    pub body: Option<&'a str>,
    pub refs: Option<&'a str>,
    pub assignee: Option<&'a str>,
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

/// Atomically claim a task. With `task_id`, claims that specific open task; without, claims
/// the highest-priority open task. Returns `None` if nothing claimable (already taken / none
/// open).
pub fn claim(
    conn: &mut Connection,
    agent: &str,
    task_id: Option<i64>,
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
    tx.commit()?;
    Ok(task)
}

/// Update a task. Fails loud ([`QuorumError::NotHolder`]) if `agent` is not the assignee.
/// An invalid status is a usage error.
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
            assignee = COALESCE(?5, assignee),
            updated_at = ?6
         WHERE id=?1 AND assignee=?7",
        params![
            id,
            fields.status,
            fields.body,
            fields.refs,
            fields.assignee,
            now,
            agent
        ],
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

    #[test]
    fn create_then_claim_sets_assignee() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "fix bug", None, 0, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", Some(id), 1000).unwrap().unwrap();
        assert_eq!(t.status, "claimed");
        assert_eq!(t.assignee.as_deref(), Some("A"));
    }

    #[test]
    fn second_claim_of_same_task_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        assert!(claim(&mut c, "A", Some(id), 1000).unwrap().is_some());
        assert!(claim(&mut c, "B", Some(id), 1000).unwrap().is_none());
    }

    #[test]
    fn claim_without_id_picks_highest_priority() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "low", None, 1, None, None, 1000).unwrap();
        create(&mut c, "boss", "high", None, 9, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", None, 1000).unwrap().unwrap();
        assert_eq!(t.title, "high");
    }

    #[test]
    fn claim_nothing_open_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(claim(&mut c, "A", None, 1000).unwrap().is_none());
    }

    #[test]
    fn update_by_assignee_changes_status() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), 1000).unwrap();
        let t = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("in_progress"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        assert_eq!(t.status, "in_progress");
        assert_eq!(t.updated_at, 1100);
    }

    #[test]
    fn update_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), 1000).unwrap();
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
        claim(&mut c, "A", Some(id), 1000).unwrap();
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
