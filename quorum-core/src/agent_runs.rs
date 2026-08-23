//! Agent-performance capture: one row per daemon-spawned agent process.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewLaunch {
    pub agent_run_id: i64,
    pub task_id: i64,
    pub agent_name: String,
    pub cap_run_id: String,
    pub pr: i64,
    pub head_sha: String,
    pub review_role: String,
}

/// Bind the immutable daemon-captured review target to the exact persisted
/// reviewer run before lifecycle attachment becomes visible.
#[cfg(test)]
pub fn bind_review_launch(
    conn: &Connection,
    agent_run_id: i64,
    cap_run_id: &str,
    pr: i64,
    head_sha: &str,
) -> Result<bool> {
    if cap_run_id.is_empty()
        || cap_run_id.contains('\0')
        || pr <= 0
        || head_sha.len() != 40
        || !head_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(false);
    }
    Ok(conn.execute(
        "UPDATE agent_runs SET review_cap_run_id=?2,review_pr=?3,review_head_sha=?4
         WHERE id=?1 AND role='reviewer' AND ended_at IS NULL
           AND review_cap_run_id IS NULL AND review_pr IS NULL AND review_head_sha IS NULL",
        params![agent_run_id, cap_run_id, pr, head_sha],
    )? == 1)
}

/// Atomically create a reviewer run together with its immutable launch
/// authority. No observer can see an authoritative run without the binding.
#[allow(clippy::too_many_arguments)]
pub fn insert_reviewer_with_launch(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    model: &str,
    effort: &str,
    provider: &str,
    role_assignment_id: Option<i64>,
    spawned_at: i64,
    sub_role: Option<&str>,
    cap_run_id: &str,
    pr: i64,
    head_sha: &str,
) -> Result<Option<i64>> {
    if cap_run_id.is_empty()
        || cap_run_id.contains('\0')
        || pr <= 0
        || head_sha.len() != 40
        || !head_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !matches!(sub_role, None | Some("r2"))
    {
        return Ok(None);
    }
    conn.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
             role_assignment_id,spawned_at,sub_role,review_cap_run_id,review_pr,review_head_sha)
         VALUES (?1,?2,'reviewer',?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            task_id,
            agent_name,
            model,
            effort,
            provider,
            role_assignment_id,
            spawned_at,
            sub_role,
            cap_run_id,
            pr,
            head_sha
        ],
    )?;
    Ok(Some(conn.last_insert_rowid()))
}

pub fn review_launch_for_capability(
    conn: &Connection,
    cap_run_id: &str,
) -> Result<Option<ReviewLaunch>> {
    Ok(conn
        .query_row(
            "SELECT id,task_id,agent_name,review_cap_run_id,review_pr,review_head_sha,
                    CASE WHEN sub_role='r2' THEN 'r2' ELSE 'r1' END
         FROM agent_runs WHERE review_cap_run_id=?1 AND role='reviewer' AND ended_at IS NULL",
            [cap_run_id],
            |row| {
                Ok(ReviewLaunch {
                    agent_run_id: row.get(0)?,
                    task_id: row.get(1)?,
                    agent_name: row.get(2)?,
                    cap_run_id: row.get(3)?,
                    pr: row.get(4)?,
                    head_sha: row.get(5)?,
                    review_role: row.get(6)?,
                })
            },
        )
        .optional()?)
}

/// Resolve immutable launch authority when startup has only the exact
/// reviewer/task/PR identity from a stranded mailbox signal. Ambiguous open
/// runs fail closed instead of choosing whichever launch happened most
/// recently.
pub fn review_launch_for_reviewer(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    pr: i64,
) -> Result<Option<ReviewLaunch>> {
    let mut stmt = conn.prepare(
        "SELECT id,task_id,agent_name,review_cap_run_id,review_pr,review_head_sha,
                CASE WHEN sub_role='r2' THEN 'r2' ELSE 'r1' END
         FROM agent_runs
         WHERE task_id=?1 AND agent_name=?2 AND role='reviewer' AND review_pr=?3
           AND ended_at IS NULL
         ORDER BY id DESC LIMIT 2",
    )?;
    let rows = stmt.query_map(params![task_id, agent_name, pr], |row| {
        Ok(ReviewLaunch {
            agent_run_id: row.get(0)?,
            task_id: row.get(1)?,
            agent_name: row.get(2)?,
            cap_run_id: row.get(3)?,
            pr: row.get(4)?,
            head_sha: row.get(5)?,
            review_role: row.get(6)?,
        })
    })?;
    let launches = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(if launches.len() == 1 {
        launches.into_iter().next()
    } else {
        None
    })
}
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentRun {
    pub id: i64,
    pub agent: String,
    pub role: String,
    pub sub_role: Option<String>,
    pub model: String,
    pub effort: String,
    pub provider: Option<String>,
    pub role_assignment_id: Option<i64>,
    pub spawned_at: i64,
    pub ended_at: Option<i64>,
    pub end_reason: Option<String>,
}

/// Insert a new run row at spawn time. Returns the row id.
#[allow(clippy::too_many_arguments)]
pub fn insert(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    role: &str,
    model: &str,
    effort: &str,
    provider: &str,
    spawned_at: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, provider,
             role_assignment_id, spawned_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7)",
        params![task_id, agent_name, role, model, effort, provider, spawned_at],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Insert canonical worker-run evidence only when its assignment link matches
/// the complete responsibility and executable profile used for the spawn.
#[allow(clippy::too_many_arguments)]
pub fn insert_worker_with_assignment(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    responsibility_key: &str,
    model: &str,
    effort: &str,
    provider: &str,
    runner: &str,
    role_assignment_id: Option<i64>,
    spawned_at: i64,
) -> Result<i64> {
    let context = crate::role_assignments::EvidenceAssignmentContext {
        role_assignment_id,
        task_id: Some(task_id),
        responsibility_key,
        role: "worker",
        provider,
        runner,
        model,
        effort,
    };
    crate::role_assignments::guarded_evidence_insert(
        conn,
        "worker agent run",
        &context,
        "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, provider,
             role_assignment_id, spawned_at)
         SELECT :task_id, :agent_name, 'worker', :model, :effort, :provider,
                :quorum_assignment_id, :spawned_at
         /* quorum-role-assignment-guard */",
        &[
            (":task_id", &task_id),
            (":agent_name", &agent_name),
            (":model", &model),
            (":effort", &effort),
            (":provider", &provider),
            (":spawned_at", &spawned_at),
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Insert an R2 audit run (sub_role='r2'). Returns the row id.
#[allow(clippy::too_many_arguments)]
pub fn insert_r2(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    model: &str,
    effort: &str,
    provider: &str,
    spawned_at: i64,
) -> Result<i64> {
    insert_r2_with_assignment(
        conn, task_id, agent_name, model, effort, provider, None, spawned_at,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn insert_r2_with_assignment(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    model: &str,
    effort: &str,
    provider: &str,
    role_assignment_id: Option<i64>,
    spawned_at: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, provider,
             role_assignment_id, spawned_at, sub_role)
         VALUES (?1, ?2, 'reviewer', ?3, ?4, ?5, ?6, ?7, 'r2')",
        params![
            task_id,
            agent_name,
            model,
            effort,
            provider,
            role_assignment_id,
            spawned_at
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Return the immutable execution layout used by the first worker agent_run
/// for a task, if any. Remediation resumes this snapshot rather than routing a
/// new worker profile under the daemon's current policy.
pub fn first_worker(conn: &Connection, task_id: i64) -> Result<Option<AgentRun>> {
    conn.query_row(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id, spawned_at, ended_at, end_reason
         FROM agent_runs
         WHERE task_id = ?1 AND role = 'worker'
         ORDER BY spawned_at ASC LIMIT 1",
        params![task_id],
        |r| {
            Ok(AgentRun {
                id: r.get(0)?,
                agent: r.get(1)?,
                role: r.get(2)?,
                sub_role: r.get(3)?,
                model: r.get(4)?,
                effort: r.get(5)?,
                provider: r.get(6)?,
                role_assignment_id: r.get(7)?,
                spawned_at: r.get(8)?,
                ended_at: r.get(9)?,
                end_reason: r.get(10)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Return the model used by the first worker agent_run for a task, if any.
pub fn worker_model(conn: &Connection, task_id: i64) -> Result<Option<String>> {
    Ok(first_worker(conn, task_id)?.map(|run| run.model))
}

/// Return the provider used by the first worker agent_run for a task, if any.
pub fn worker_provider(conn: &Connection, task_id: i64) -> Result<Option<String>> {
    Ok(first_worker(conn, task_id)?.and_then(|run| run.provider))
}

/// Latest interrupted reviewer run for a role, if any.
///
/// An open row represents a daemon crash. `drain` represents a clean daemon
/// shutdown that intentionally left an in-review task for restart recovery.
pub fn interrupted_reviewer(
    conn: &Connection,
    task_id: i64,
    is_r2: bool,
) -> Result<Option<AgentRun>> {
    conn.query_row(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id, spawned_at, ended_at, end_reason
         FROM agent_runs
         WHERE task_id = ?1
           AND role = 'reviewer'
           AND ((?2 = 1 AND sub_role = 'r2') OR (?2 = 0 AND sub_role IS NULL))
           AND (ended_at IS NULL OR end_reason = 'drain')
         ORDER BY id DESC
         LIMIT 1",
        params![task_id, is_r2],
        |r| {
            Ok(AgentRun {
                id: r.get(0)?,
                agent: r.get(1)?,
                role: r.get(2)?,
                sub_role: r.get(3)?,
                model: r.get(4)?,
                effort: r.get(5)?,
                provider: r.get(6)?,
                role_assignment_id: r.get(7)?,
                spawned_at: r.get(8)?,
                ended_at: r.get(9)?,
                end_reason: r.get(10)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// All runs for a task, ordered by id.
pub fn runs_for_task(conn: &Connection, task_id: i64) -> Result<Vec<AgentRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id, spawned_at, ended_at, end_reason \
         FROM agent_runs WHERE task_id = ?1 ORDER BY id ASC",
    )?;
    let runs = stmt
        .query_map(params![task_id], |r| {
            Ok(AgentRun {
                id: r.get(0)?,
                agent: r.get(1)?,
                role: r.get(2)?,
                sub_role: r.get(3)?,
                model: r.get(4)?,
                effort: r.get(5)?,
                provider: r.get(6)?,
                role_assignment_id: r.get(7)?,
                spawned_at: r.get(8)?,
                ended_at: r.get(9)?,
                end_reason: r.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
}

/// Latest run identity for a task, without materializing its complete history.
pub fn latest_for_task(conn: &Connection, task_id: i64) -> Result<Option<AgentRun>> {
    conn.query_row(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id, spawned_at, ended_at, end_reason
         FROM agent_runs WHERE task_id = ?1 ORDER BY spawned_at DESC, id DESC LIMIT 1",
        params![task_id],
        |r| Ok(AgentRun {
            id: r.get(0)?, agent: r.get(1)?, role: r.get(2)?, sub_role: r.get(3)?,
            model: r.get(4)?, effort: r.get(5)?, provider: r.get(6)?, role_assignment_id: r.get(7)?,
            spawned_at: r.get(8)?, ended_at: r.get(9)?, end_reason: r.get(10)?,
        }),
    ).optional().map_err(Into::into)
}

/// Close an open run row at teardown/terminal.
pub fn close(conn: &Connection, run_id: i64, ended_at: i64, end_reason: &str) -> Result<()> {
    conn.execute(
        "UPDATE agent_runs SET ended_at = ?1, end_reason = ?2 WHERE id = ?3",
        params![ended_at, end_reason, run_id],
    )?;
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
    fn insert_and_close_round_trip() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c,
            "boss",
            "test-task",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let run_id = insert(
            &c,
            tid,
            "Alice",
            "worker",
            "claude-opus-4-6",
            "high",
            "claude",
            100,
        )
        .unwrap();
        assert!(run_id > 0);

        close(&c, run_id, 200, "done").unwrap();

        let (ended, reason): (i64, String) = c
            .query_row(
                "SELECT ended_at, end_reason FROM agent_runs WHERE id = ?1",
                params![run_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ended, 200);
        assert_eq!(reason, "done");
    }

    #[test]
    fn review_launch_is_immutable_capability_bound_and_fail_closed() {
        let (_d, c) = open_tmp();
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let run = insert(&c, 7, "R", "reviewer", "model", "high", "codex", 10).unwrap();
        assert!(bind_review_launch(&c, run, "cap-7", 71, sha).unwrap());
        assert!(!bind_review_launch(&c, run, "other", 72, sha).unwrap());
        assert!(!bind_review_launch(&c, run, "bad", 71, "not-a-sha").unwrap());
        assert_eq!(
            review_launch_for_capability(&c, "cap-7").unwrap(),
            Some(ReviewLaunch {
                agent_run_id: run,
                task_id: 7,
                agent_name: "R".into(),
                cap_run_id: "cap-7".into(),
                pr: 71,
                head_sha: sha.into(),
                review_role: "r1".into(),
            })
        );
        assert_eq!(review_launch_for_capability(&c, "other").unwrap(), None);
        close(&c, run, 20, "done").unwrap();
        assert_eq!(review_launch_for_capability(&c, "cap-7").unwrap(), None);
    }

    #[test]
    fn reviewer_insert_persists_launch_authority_in_one_row() {
        let (_d, c) = open_tmp();
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let run = insert_reviewer_with_launch(
            &c,
            9,
            "R2",
            "model",
            "high",
            "codex",
            None,
            10,
            Some("r2"),
            "cap-9",
            79,
            sha,
        )
        .unwrap()
        .unwrap();
        let launch = review_launch_for_capability(&c, "cap-9").unwrap().unwrap();
        assert_eq!(launch.agent_run_id, run);
        assert_eq!(launch.review_role, "r2");
        assert_eq!(
            (launch.task_id, launch.pr, launch.head_sha.as_str()),
            (9, 79, sha)
        );
        assert_eq!(
            review_launch_for_reviewer(&c, 9, "R2", 79)
                .unwrap()
                .unwrap()
                .agent_run_id,
            run
        );
        insert_reviewer_with_launch(
            &c,
            9,
            "R2",
            "model",
            "high",
            "codex",
            None,
            11,
            Some("r2"),
            "cap-10",
            79,
            sha,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            review_launch_for_reviewer(&c, 9, "R2", 79).unwrap(),
            None,
            "ambiguous stranded reviewer launches must fail closed"
        );
        assert!(insert_reviewer_with_launch(
            &c, 10, "bad", "model", "high", "codex", None, 11, None, "", 79, sha,
        )
        .unwrap()
        .is_none());
        let bad_rows: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE agent_name='bad'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bad_rows, 0);
    }

    #[test]
    fn worker_assignment_insert_succeeds_for_matching_context() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c, "boss", "routed", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        let responsibility_key = format!("worker:task:{tid}:revision:1");
        c.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (42,?1,?2,'worker','sol','codex','codex','gpt-5.6-sol','high',
                     'worker:M','g1',100)",
            params![responsibility_key, tid],
        )
        .unwrap();
        let run_id = insert_worker_with_assignment(
            &c,
            tid,
            "Routed",
            &responsibility_key,
            "gpt-5.6-sol",
            "high",
            "codex",
            "codex",
            Some(42),
            101,
        )
        .unwrap();
        let run = latest_for_task(&c, tid).unwrap().unwrap();
        assert_eq!(run.id, run_id);
        assert_eq!(run.role_assignment_id, Some(42));
        assert_eq!(
            worker_model(&c, tid).unwrap().as_deref(),
            Some("gpt-5.6-sol")
        );
    }

    #[test]
    fn worker_assignment_insert_rejects_wrong_context_and_rolls_back_lifecycle() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c, "boss", "guarded", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        let other_tid = crate::tasks::create(
            &mut c,
            "boss",
            "different worker context",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let other_responsibility = format!("worker:task:{other_tid}:revision:1");
        c.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (42,?1,?2,'worker','sol','codex','codex','gpt-5.6-sol','high',
                     'worker:M','g1',100)",
            params![other_responsibility, other_tid],
        )
        .unwrap();

        let result = (|| -> Result<()> {
            let tx = crate::db::begin_immediate(&mut c)?;
            tx.execute(
                "UPDATE tasks SET status='working',updated_at=101 WHERE id=?1",
                [tid],
            )?;
            insert_worker_with_assignment(
                &tx,
                tid,
                "WrongContext",
                &format!("worker:task:{tid}:revision:1"),
                "gpt-5.6-sol",
                "high",
                "codex",
                "codex",
                Some(42),
                101,
            )?;
            tx.commit().map_err(crate::db::map_sql_err)?;
            Ok(())
        })();

        assert!(result.is_err());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM agent_runs WHERE role='worker'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        assert_eq!(
            c.query_row("SELECT status FROM tasks WHERE id=?1", [tid], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            "open"
        );
    }

    #[test]
    fn role_check_constraint_rejects_invalid() {
        let (_d, c) = open_tmp();
        let result = c.execute(
            "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, spawned_at)
             VALUES (1, 'A', 'invalid', 'model', 'high', 100)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn multiple_runs_per_task() {
        let (_d, c) = open_tmp();
        let r1 = insert(&c, 1, "Alice", "worker", "model-a", "high", "claude", 100).unwrap();
        let r2 = insert(&c, 1, "Bob", "reviewer", "model-b", "medium", "claude", 200).unwrap();
        assert_ne!(r1, r2);

        let count: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn worker_model_returns_first_worker() {
        let (_d, c) = open_tmp();
        assert_eq!(worker_model(&c, 999).unwrap(), None);

        insert(
            &c,
            1,
            "Alice",
            "worker",
            "claude-opus-4-6",
            "high",
            "claude",
            100,
        )
        .unwrap();
        insert(
            &c,
            1,
            "Bob",
            "reviewer",
            "claude-opus-4-8",
            "medium",
            "claude",
            200,
        )
        .unwrap();
        insert(
            &c,
            1,
            "Carol",
            "worker",
            "claude-opus-4-7",
            "high",
            "claude",
            300,
        )
        .unwrap();

        assert_eq!(
            worker_model(&c, 1).unwrap().as_deref(),
            Some("claude-opus-4-6")
        );
        let snapshot = first_worker(&c, 1).unwrap().unwrap();
        assert_eq!(
            (
                snapshot.agent.as_str(),
                snapshot.model.as_str(),
                snapshot.effort.as_str(),
                snapshot.provider.as_deref(),
            ),
            ("Alice", "claude-opus-4-6", "high", Some("claude")),
            "remediation must recover the complete first-worker layout"
        );
    }

    #[test]
    fn end_reason_distinguishes_cleanup_causes() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c,
            "boss",
            "test-task",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        let reasons = [
            "submitted",
            "awaiting_merge",
            "idle_reaped",
            "crashed",
            "agent_failed",
            "merged",
            "done",
        ];
        let mut run_ids = Vec::new();
        for (i, &reason) in reasons.iter().enumerate() {
            let rid = insert(
                &c,
                tid,
                &format!("Agent-{i}"),
                "worker",
                "model",
                "high",
                "claude",
                100 + i as i64,
            )
            .unwrap();
            close(&c, rid, 200 + i as i64, reason).unwrap();
            run_ids.push(rid);
        }

        for (rid, &expected) in run_ids.iter().zip(reasons.iter()) {
            let actual: String = c
                .query_row(
                    "SELECT end_reason FROM agent_runs WHERE id = ?1",
                    params![rid],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                actual, expected,
                "run {rid} should have end_reason={expected}"
            );
        }
    }
}
