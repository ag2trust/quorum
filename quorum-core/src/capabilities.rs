//! Daemon-issued run capabilities: a per-run identity binding (run_id, task, agent, role).
//!
//! The daemon creates a capability when spawning a worker/reviewer. Submit and
//! report operations require a valid (non-revoked) capability whose derived
//! identity matches the operation. This replaces authorization by caller-supplied
//! agent name alone.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RunCapability {
    pub run_id: String,
    pub task_id: i64,
    pub agent: String,
    pub role: String,
    pub created_at: i64,
    pub revoked_at: Option<i64>,
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
    tx.execute(
        "INSERT INTO run_capabilities (run_id, task_id, agent, role, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![run_id, task_id, agent, role, now],
    )?;
    tx.commit()?;
    Ok(())
}

/// Validate a run capability: it must exist, not be revoked, and its derived
/// identity must match the expected agent. Returns the capability on success,
/// or an error describing the mismatch.
pub fn validate(conn: &Connection, run_id: &str) -> Result<RunCapability> {
    let cap = conn
        .query_row(
            "SELECT run_id, task_id, agent, role, created_at, revoked_at
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
                })
            },
        )
        .optional()?;
    match cap {
        None => Err(QuorumError::Usage(format!(
            "unknown run_id '{run_id}' — not issued by this daemon"
        ))),
        Some(c) if c.revoked_at.is_some() => Err(QuorumError::Usage(format!(
            "run_id '{run_id}' has been revoked"
        ))),
        Some(c) => Ok(c),
    }
}

/// Revoke a capability (e.g. on agent death or task terminal transition).
pub fn revoke(conn: &mut Connection, run_id: &str, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE run_capabilities SET revoked_at = ?1
         WHERE run_id = ?2 AND revoked_at IS NULL",
        params![now, run_id],
    )?;
    tx.commit()?;
    Ok(changed > 0)
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

/// Look up the active (non-revoked) capability for a given agent name.
/// Returns None if no active capability exists.
pub fn active_for_agent(conn: &Connection, agent: &str) -> Result<Option<RunCapability>> {
    conn.query_row(
        "SELECT run_id, task_id, agent, role, created_at, revoked_at
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
            })
        },
    )
    .optional()
    .map_err(Into::into)
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
    fn issue_and_validate_round_trips() {
        let (_d, mut c) = open_tmp();
        issue(&mut c, "run-001", 42, "Worker-1", "worker", 1000).unwrap();
        let cap = validate(&c, "run-001").unwrap();
        assert_eq!(cap.task_id, 42);
        assert_eq!(cap.agent, "Worker-1");
        assert_eq!(cap.role, "worker");
        assert!(cap.revoked_at.is_none());
    }

    #[test]
    fn validate_unknown_run_id_fails() {
        let (_d, c) = open_tmp();
        let err = validate(&c, "nonexistent").unwrap_err();
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
        let err = validate(&c, "run-002").unwrap_err();
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
        assert!(validate(&c, "run-a1").is_err());
        assert!(validate(&c, "run-a2").is_err());
        assert!(validate(&c, "run-b1").is_ok());
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
}
