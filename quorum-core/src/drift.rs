//! Unbacked-PR drift detector — surfaces open PRs with no task backing and
//! twin PRs (two open PRs associated with the same task).
//!
//! The daemon calls [`detect`] periodically (~15 min) with the output of
//! `gh pr list` and the current task→PR mapping from the DB. Detection is
//! pure (no side effects); the daemon handles event emission and dedup.

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// An open PR from `gh pr list --json number,title,headRefName`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GhPr {
    pub number: i64,
    pub title: String,
    #[serde(rename = "headRefName")]
    pub head_ref_name: String,
}

/// An open PR that no non-terminal task references.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UnbackedPr {
    pub number: i64,
    pub title: String,
    pub branch: String,
}

/// Two or more open PRs associated with the same task.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TwinPr {
    pub task_id: i64,
    pub pr_numbers: Vec<i64>,
}

/// Result of a drift detection pass.
#[derive(Debug, Clone, Default)]
pub struct DriftResult {
    pub unbacked: Vec<UnbackedPr>,
    pub twins: Vec<TwinPr>,
}

/// Parse a daemon branch name (`daemon/<agent>-t<N>`) and return the task ID.
fn parse_daemon_branch_task_id(branch: &str) -> Option<i64> {
    let suffix = branch.strip_prefix("daemon/")?;
    let t_pos = suffix.rfind("-t")?;
    suffix[t_pos + 2..].parse::<i64>().ok()
}

/// Detect unbacked and twin PRs.
///
/// `open_prs`: all open PRs from `gh pr list`.
/// `task_prs`: `(task_id, pr_number)` pairs from non-terminal tasks with refs.pr set.
/// `task_branches`: `(task_id, branch)` pairs from the task_branches table for
///   non-terminal tasks — used for branch-based twin matching.
/// `active_task_ids`: IDs of all non-terminal tasks — used to suppress false positives
///   during the create→done window when a daemon branch encodes its task ID.
pub fn detect(
    open_prs: &[GhPr],
    task_prs: &[(i64, i64)],
    task_branches: &[(i64, String)],
    active_task_ids: &HashSet<i64>,
) -> DriftResult {
    let backed_pr_numbers: HashSet<i64> = task_prs.iter().map(|(_, pr)| *pr).collect();

    let branch_task_set: HashSet<i64> = task_branches.iter().map(|(tid, _)| *tid).collect();

    let unbacked: Vec<UnbackedPr> = open_prs
        .iter()
        .filter(|pr| !backed_pr_numbers.contains(&pr.number))
        .filter(|pr| {
            // Suppress if the PR's branch is allocated to a non-terminal task
            if let Some(&_tid) = task_branches.iter().find_map(|(tid, b)| {
                if b == &pr.head_ref_name {
                    Some(tid)
                } else {
                    None
                }
            }) {
                return false;
            }
            // Suppress if branch matches daemon/<agent>-t<N> and task N is active
            if let Some(task_id) = parse_daemon_branch_task_id(&pr.head_ref_name) {
                if active_task_ids.contains(&task_id) || branch_task_set.contains(&task_id) {
                    return false;
                }
            }
            true
        })
        .map(|pr| UnbackedPr {
            number: pr.number,
            title: pr.title.clone(),
            branch: pr.head_ref_name.clone(),
        })
        .collect();

    // Twin detection: group open PRs by their associated task.
    // A PR is associated with a task by refs.pr match OR branch match.
    let pr_to_task_by_ref: HashMap<i64, i64> =
        task_prs.iter().map(|(tid, pr)| (*pr, *tid)).collect();
    let branch_to_task: HashMap<&str, i64> = task_branches
        .iter()
        .map(|(tid, b)| (b.as_str(), *tid))
        .collect();

    let mut task_to_open_prs: HashMap<i64, Vec<i64>> = HashMap::new();
    for pr in open_prs {
        if let Some(&tid) = pr_to_task_by_ref.get(&pr.number) {
            task_to_open_prs.entry(tid).or_default().push(pr.number);
        }
        if let Some(&tid) = branch_to_task.get(pr.head_ref_name.as_str()) {
            let prs = task_to_open_prs.entry(tid).or_default();
            if !prs.contains(&pr.number) {
                prs.push(pr.number);
            }
        }
    }
    // Also check: a task's refs.pr might point to a closed PR while an open PR
    // exists on the same branch — but the task_prs list only has open-to-task
    // refs, so we're already covered above.

    let mut twins: Vec<TwinPr> = task_to_open_prs
        .into_iter()
        .filter(|(_, prs)| prs.len() >= 2)
        .map(|(tid, mut prs)| {
            prs.sort();
            TwinPr {
                task_id: tid,
                pr_numbers: prs,
            }
        })
        .collect();
    twins.sort_by_key(|t| t.task_id);

    DriftResult { unbacked, twins }
}

/// Query non-terminal tasks with refs.pr set. Returns (task_id, pr_number) pairs.
pub fn task_pr_refs(conn: &Connection) -> Result<Vec<(i64, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT id, refs FROM tasks WHERE status NOT IN ('done', 'failed', 'cancelled', 'closed') AND refs IS NOT NULL",
    )?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (id, refs_json) in rows {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&refs_json) {
            if let Some(pr) = v.get("pr").and_then(|p| {
                p.as_i64()
                    .or_else(|| p.as_str().and_then(|s| s.parse().ok()))
            }) {
                result.push((id, pr));
            }
        }
    }
    Ok(result)
}

/// Query IDs of all non-terminal tasks (used for daemon-branch correlation).
pub fn active_task_ids(conn: &Connection) -> Result<HashSet<i64>> {
    let mut stmt = conn.prepare(
        "SELECT id FROM tasks WHERE status NOT IN ('done', 'failed', 'cancelled', 'closed')",
    )?;
    let ids = stmt
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    Ok(ids)
}

/// Query branch allocations for non-terminal tasks.
pub fn task_branch_allocations(conn: &Connection) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "SELECT tb.task_id, tb.branch FROM task_branches tb
         INNER JOIN tasks t ON t.id = tb.task_id
         WHERE t.status NOT IN ('done', 'failed', 'cancelled', 'closed')",
    )?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Check whether an unbacked_pr or twin_pr event for the given subject already
/// exists in the events table (unexpired).
pub fn already_alerted(conn: &Connection, kind: &str, subject: &str, now: i64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE kind = ?1 AND subject = ?2 AND expires_at > ?3",
        params![kind, subject, now],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

/// Emit drift events for newly detected unbacked/twin PRs (deduped against
/// existing events), then revoke any stale events whose subjects are no longer
/// in the current drift result.
pub fn emit_drift_events(conn: &mut Connection, drift: &DriftResult, now: i64) -> Result<()> {
    for u in &drift.unbacked {
        let subject = format!("pr#{}", u.number);
        if !already_alerted(conn, "unbacked_pr", &subject, now)? {
            let body = serde_json::json!({
                "title": u.title,
                "branch": u.branch,
            })
            .to_string();
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            crate::events::emit(&tx, "unbacked_pr", &subject, &body, now)?;
            tx.commit()?;
        }
    }
    for t in &drift.twins {
        let subject = format!("task#{}", t.task_id);
        if !already_alerted(conn, "twin_pr", &subject, now)? {
            let body = serde_json::json!({ "prs": t.pr_numbers }).to_string();
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            crate::events::emit(&tx, "twin_pr", &subject, &body, now)?;
            tx.commit()?;
        }
    }
    revoke_resolved_drift_events(conn, drift, now)?;
    Ok(())
}

/// Expire unexpired drift events whose subjects are no longer in the current
/// drift result. Sets `expires_at = now` so they disappear from status immediately.
pub fn revoke_resolved_drift_events(
    conn: &mut Connection,
    drift: &DriftResult,
    now: i64,
) -> Result<()> {
    let current_unbacked: HashSet<String> = drift
        .unbacked
        .iter()
        .map(|u| format!("pr#{}", u.number))
        .collect();
    let current_twins: HashSet<String> = drift
        .twins
        .iter()
        .map(|t| format!("task#{}", t.task_id))
        .collect();

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    {
        let mut stmt = tx
            .prepare("SELECT subject FROM events WHERE kind = 'unbacked_pr' AND expires_at > ?1")?;
        let stale_subjects: Vec<String> = stmt
            .query_map(params![now], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|s| !current_unbacked.contains(s))
            .collect();
        for subject in &stale_subjects {
            tx.execute(
                "UPDATE events SET expires_at = ?1 WHERE kind = 'unbacked_pr' AND subject = ?2 AND expires_at > ?1",
                params![now, subject],
            )?;
        }
    }

    {
        let mut stmt =
            tx.prepare("SELECT subject FROM events WHERE kind = 'twin_pr' AND expires_at > ?1")?;
        let stale_subjects: Vec<String> = stmt
            .query_map(params![now], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|s| !current_twins.contains(s))
            .collect();
        for subject in &stale_subjects {
            tx.execute(
                "UPDATE events SET expires_at = ?1 WHERE kind = 'twin_pr' AND subject = ?2 AND expires_at > ?1",
                params![now, subject],
            )?;
        }
    }

    tx.commit()?;
    Ok(())
}

/// Query unexpired unbacked_pr events for the status display.
pub fn unbacked_pr_events(conn: &Connection, now: i64) -> Result<Vec<UnbackedPr>> {
    let mut stmt = conn.prepare(
        "SELECT subject, body FROM events WHERE kind = 'unbacked_pr' AND expires_at > ?1
         ORDER BY seq ASC",
    )?;
    let rows = stmt
        .query_map(params![now], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (subject, body) in rows {
        let number = subject
            .strip_prefix("pr#")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            result.push(UnbackedPr {
                number,
                title: v
                    .get("title")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                branch: v
                    .get("branch")
                    .and_then(|b| b.as_str())
                    .unwrap_or("")
                    .to_string(),
            });
        }
    }
    Ok(result)
}

/// Query unexpired twin_pr events for the status display.
pub fn twin_pr_events(conn: &Connection, now: i64) -> Result<Vec<TwinPr>> {
    let mut stmt = conn.prepare(
        "SELECT subject, body FROM events WHERE kind = 'twin_pr' AND expires_at > ?1
         ORDER BY seq ASC",
    )?;
    let rows = stmt
        .query_map(params![now], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (subject, body) in rows {
        let task_id = subject
            .strip_prefix("task#")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            let prs = v
                .get("prs")
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|n| n.as_i64()).collect())
                .unwrap_or_default();
            result.push(TwinPr {
                task_id,
                pr_numbers: prs,
            });
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::TransactionBehavior;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    #[test]
    fn detect_unbacked_ignores_backed_prs() {
        let open_prs = vec![
            GhPr {
                number: 100,
                title: "backed".into(),
                head_ref_name: "feat/a".into(),
            },
            GhPr {
                number: 200,
                title: "orphan".into(),
                head_ref_name: "feat/b".into(),
            },
            GhPr {
                number: 300,
                title: "also backed".into(),
                head_ref_name: "feat/c".into(),
            },
        ];
        let task_prs = vec![(1, 100), (2, 300)];
        let result = detect(&open_prs, &task_prs, &[], &HashSet::new());
        assert_eq!(result.unbacked.len(), 1);
        assert_eq!(result.unbacked[0].number, 200);
        assert_eq!(result.unbacked[0].title, "orphan");
    }

    #[test]
    fn detect_unbacked_all_backed() {
        let open_prs = vec![GhPr {
            number: 10,
            title: "a".into(),
            head_ref_name: "x".into(),
        }];
        let task_prs = vec![(1, 10)];
        let result = detect(&open_prs, &task_prs, &[], &HashSet::new());
        assert!(result.unbacked.is_empty());
        assert!(result.twins.is_empty());
    }

    #[test]
    fn detect_twin_by_refs_pr() {
        let open_prs = vec![
            GhPr {
                number: 10,
                title: "orig".into(),
                head_ref_name: "feat/a".into(),
            },
            GhPr {
                number: 11,
                title: "dup".into(),
                head_ref_name: "feat/a-v2".into(),
            },
        ];
        // Both PRs backed by the same task
        let task_prs = vec![(5, 10), (5, 11)];
        let result = detect(&open_prs, &task_prs, &[], &HashSet::new());
        assert!(result.unbacked.is_empty());
        assert_eq!(result.twins.len(), 1);
        assert_eq!(result.twins[0].task_id, 5);
        assert_eq!(result.twins[0].pr_numbers, vec![10, 11]);
    }

    #[test]
    fn detect_twin_by_branch_match() {
        let open_prs = vec![
            GhPr {
                number: 10,
                title: "orig".into(),
                head_ref_name: "daemon/feat-w1".into(),
            },
            GhPr {
                number: 20,
                title: "twin".into(),
                head_ref_name: "daemon/feat-w1".into(),
            },
        ];
        // Only PR 10 referenced by task, but PR 20 shares the branch
        let task_prs = vec![(5, 10)];
        let task_branches = vec![(5, "daemon/feat-w1".to_string())];
        let result = detect(&open_prs, &task_prs, &task_branches, &HashSet::new());
        assert_eq!(result.twins.len(), 1);
        assert_eq!(result.twins[0].task_id, 5);
        assert_eq!(result.twins[0].pr_numbers, vec![10, 20]);
    }

    #[test]
    fn detect_twin_branch_plus_ref_no_duplicate() {
        let open_prs = vec![GhPr {
            number: 10,
            title: "a".into(),
            head_ref_name: "daemon/feat-w1".into(),
        }];
        let task_prs = vec![(5, 10)];
        let task_branches = vec![(5, "daemon/feat-w1".to_string())];
        let result = detect(&open_prs, &task_prs, &task_branches, &HashSet::new());
        // One PR matched by both ref and branch — should NOT be twin
        assert!(result.twins.is_empty());
    }

    #[test]
    fn detect_empty_inputs() {
        let result = detect(&[], &[], &[], &HashSet::new());
        assert!(result.unbacked.is_empty());
        assert!(result.twins.is_empty());
    }

    #[test]
    fn task_pr_refs_from_db() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO tasks (title, status, priority, created_by, created_at, updated_at, refs)
             VALUES ('t1', 'working', 0, 'test', 1, 1, '{\"pr\":42}')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO tasks (title, status, priority, created_by, created_at, updated_at, refs)
             VALUES ('t2', 'done', 0, 'test', 1, 1, '{\"pr\":99}')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO tasks (title, status, priority, created_by, created_at, updated_at)
             VALUES ('t3', 'open', 0, 'test', 1, 1)",
            [],
        )
        .unwrap();
        let refs = task_pr_refs(&c).unwrap();
        assert_eq!(refs.len(), 1, "terminal task should be excluded");
        assert_eq!(refs[0], (1, 42));
    }

    #[test]
    fn task_pr_refs_excludes_all_terminal_statuses() {
        let (_d, c) = open_tmp();
        for (i, status) in ["done", "failed", "cancelled", "closed"].iter().enumerate() {
            c.execute(
                &format!(
                    "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at, refs)
                     VALUES ({}, 'term-{}', '{}', 0, 'test', 1, 1, '{{\"pr\":{}}}')",
                    100 + i, status, status, 500 + i
                ),
                [],
            )
            .unwrap();
        }
        c.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at, refs)
             VALUES (200, 'active', 'working', 0, 'test', 1, 1, '{\"pr\":999}')",
            [],
        )
        .unwrap();
        let refs = task_pr_refs(&c).unwrap();
        assert_eq!(refs.len(), 1, "only non-terminal task should appear");
        assert_eq!(refs[0], (200, 999));
    }

    #[test]
    fn already_alerted_dedup() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        assert!(!already_alerted(&c, "unbacked_pr", "pr#42", now).unwrap());
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        crate::events::emit(&tx, "unbacked_pr", "pr#42", "{}", now).unwrap();
        tx.commit().unwrap();
        assert!(already_alerted(&c, "unbacked_pr", "pr#42", now).unwrap());
    }

    #[test]
    fn unbacked_pr_events_roundtrip() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let body = serde_json::json!({"title": "orphan PR", "branch": "feat/orphan"}).to_string();
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        crate::events::emit(&tx, "unbacked_pr", "pr#42", &body, now).unwrap();
        tx.commit().unwrap();
        let prs = unbacked_pr_events(&c, now).unwrap();
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].number, 42);
        assert_eq!(prs[0].title, "orphan PR");
        assert_eq!(prs[0].branch, "feat/orphan");
    }

    #[test]
    fn twin_pr_events_roundtrip() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let body = serde_json::json!({"prs": [10, 11]}).to_string();
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        crate::events::emit(&tx, "twin_pr", "task#5", &body, now).unwrap();
        tx.commit().unwrap();
        let twins = twin_pr_events(&c, now).unwrap();
        assert_eq!(twins.len(), 1);
        assert_eq!(twins[0].task_id, 5);
        assert_eq!(twins[0].pr_numbers, vec![10, 11]);
    }

    #[test]
    fn emit_drift_events_dedup() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let drift = DriftResult {
            unbacked: vec![UnbackedPr {
                number: 42,
                title: "orphan".into(),
                branch: "feat/x".into(),
            }],
            twins: vec![TwinPr {
                task_id: 5,
                pr_numbers: vec![10, 11],
            }],
        };
        emit_drift_events(&mut c, &drift, now).unwrap();
        let prs = unbacked_pr_events(&c, now).unwrap();
        assert_eq!(prs.len(), 1);
        let twins = twin_pr_events(&c, now).unwrap();
        assert_eq!(twins.len(), 1);

        // Second call should NOT create duplicates
        emit_drift_events(&mut c, &drift, now).unwrap();
        let prs2 = unbacked_pr_events(&c, now).unwrap();
        assert_eq!(prs2.len(), 1, "dedup should prevent duplicate events");
        let twins2 = twin_pr_events(&c, now).unwrap();
        assert_eq!(twins2.len(), 1, "dedup should prevent duplicate events");
    }

    #[test]
    fn revoke_unbacked_pr_on_merge() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let drift = DriftResult {
            unbacked: vec![
                UnbackedPr {
                    number: 42,
                    title: "orphan".into(),
                    branch: "feat/x".into(),
                },
                UnbackedPr {
                    number: 99,
                    title: "another".into(),
                    branch: "feat/y".into(),
                },
            ],
            twins: vec![],
        };
        emit_drift_events(&mut c, &drift, now).unwrap();
        assert_eq!(unbacked_pr_events(&c, now).unwrap().len(), 2);

        // PR #42 merges — next drift pass no longer lists it
        let drift2 = DriftResult {
            unbacked: vec![UnbackedPr {
                number: 99,
                title: "another".into(),
                branch: "feat/y".into(),
            }],
            twins: vec![],
        };
        emit_drift_events(&mut c, &drift2, now).unwrap();
        let remaining = unbacked_pr_events(&c, now).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].number, 99);
    }

    #[test]
    fn revoke_unbacked_pr_on_task_backing() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let drift = DriftResult {
            unbacked: vec![UnbackedPr {
                number: 50,
                title: "orphan".into(),
                branch: "feat/z".into(),
            }],
            twins: vec![],
        };
        emit_drift_events(&mut c, &drift, now).unwrap();
        assert_eq!(unbacked_pr_events(&c, now).unwrap().len(), 1);

        // PR #50 gains a backing task — next drift pass returns empty
        let drift2 = DriftResult::default();
        emit_drift_events(&mut c, &drift2, now).unwrap();
        assert!(unbacked_pr_events(&c, now).unwrap().is_empty());
    }

    #[test]
    fn revoke_twin_pr_on_resolution() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let drift = DriftResult {
            unbacked: vec![],
            twins: vec![TwinPr {
                task_id: 5,
                pr_numbers: vec![10, 11],
            }],
        };
        emit_drift_events(&mut c, &drift, now).unwrap();
        assert_eq!(twin_pr_events(&c, now).unwrap().len(), 1);

        // One twin PR closes — no longer twin
        let drift2 = DriftResult::default();
        emit_drift_events(&mut c, &drift2, now).unwrap();
        assert!(twin_pr_events(&c, now).unwrap().is_empty());
    }

    #[test]
    fn revoke_does_not_affect_still_active_events() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let drift = DriftResult {
            unbacked: vec![
                UnbackedPr {
                    number: 1,
                    title: "a".into(),
                    branch: "b1".into(),
                },
                UnbackedPr {
                    number: 2,
                    title: "b".into(),
                    branch: "b2".into(),
                },
            ],
            twins: vec![TwinPr {
                task_id: 10,
                pr_numbers: vec![20, 21],
            }],
        };
        emit_drift_events(&mut c, &drift, now).unwrap();

        // Same drift result — nothing revoked
        emit_drift_events(&mut c, &drift, now).unwrap();
        assert_eq!(unbacked_pr_events(&c, now).unwrap().len(), 2);
        assert_eq!(twin_pr_events(&c, now).unwrap().len(), 1);
    }

    #[test]
    fn expired_events_not_returned() {
        let (_d, mut c) = open_tmp();
        let now = 1000;
        let body = serde_json::json!({"title": "old", "branch": "x"}).to_string();
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        crate::events::emit(&tx, "unbacked_pr", "pr#1", &body, now).unwrap();
        tx.commit().unwrap();
        let far_future = now + crate::events::EVENT_TTL_SECS + 1;
        let prs = unbacked_pr_events(&c, far_future).unwrap();
        assert!(prs.is_empty(), "expired events should not appear");
    }

    #[test]
    fn parse_daemon_branch_task_id_valid() {
        assert_eq!(
            parse_daemon_branch_task_id("daemon/bolt-rw4r-t38"),
            Some(38)
        );
        assert_eq!(
            parse_daemon_branch_task_id("daemon/pivot-8xbr-t67"),
            Some(67)
        );
        assert_eq!(parse_daemon_branch_task_id("daemon/a-t1"), Some(1));
    }

    #[test]
    fn parse_daemon_branch_task_id_invalid() {
        assert_eq!(parse_daemon_branch_task_id("feat/something"), None);
        assert_eq!(parse_daemon_branch_task_id("daemon/no-task-suffix"), None);
        assert_eq!(parse_daemon_branch_task_id("daemon/"), None);
        assert_eq!(parse_daemon_branch_task_id(""), None);
    }

    #[test]
    fn detect_suppresses_unbacked_during_create_done_window() {
        // PR on daemon branch for task 38 (active) — should be suppressed
        let open_prs = vec![GhPr {
            number: 3598,
            title: "feat: something".into(),
            head_ref_name: "daemon/bolt-rw4r-t38".into(),
        }];
        let task_prs = vec![]; // no refs.pr set yet (create→done window)
        let active = HashSet::from([38]);
        let result = detect(&open_prs, &task_prs, &[], &active);
        assert!(
            result.unbacked.is_empty(),
            "should suppress during create→done window"
        );
    }

    #[test]
    fn detect_flags_unbacked_when_task_terminal() {
        // PR on daemon branch for task 38, but task 38 is NOT in active set (terminal)
        let open_prs = vec![GhPr {
            number: 3598,
            title: "feat: something".into(),
            head_ref_name: "daemon/bolt-rw4r-t38".into(),
        }];
        let task_prs = vec![];
        let active = HashSet::new(); // task 38 is terminal/gone
        let result = detect(&open_prs, &task_prs, &[], &active);
        assert_eq!(
            result.unbacked.len(),
            1,
            "should flag when task is terminal"
        );
        assert_eq!(result.unbacked[0].number, 3598);
    }

    #[test]
    fn detect_suppresses_unbacked_via_task_branches_match() {
        // PR on a branch that's allocated to an active task via task_branches table
        let open_prs = vec![GhPr {
            number: 100,
            title: "feat: allocated".into(),
            head_ref_name: "feat/cool-thing-bolt-q8x".into(),
        }];
        let task_prs = vec![]; // no refs.pr yet
        let task_branches = vec![(42, "feat/cool-thing-bolt-q8x".to_string())];
        let result = detect(&open_prs, &task_prs, &task_branches, &HashSet::new());
        assert!(
            result.unbacked.is_empty(),
            "should suppress when branch is allocated to active task"
        );
    }

    #[test]
    fn active_task_ids_from_db() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at)
             VALUES (10, 'working', 'working', 0, 'test', 1, 1)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at)
             VALUES (20, 'done', 'done', 0, 'test', 1, 1)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at)
             VALUES (30, 'open', 'open', 0, 'test', 1, 1)",
            [],
        )
        .unwrap();
        let ids = active_task_ids(&c).unwrap();
        assert!(ids.contains(&10));
        assert!(!ids.contains(&20));
        assert!(ids.contains(&30));
    }
}
