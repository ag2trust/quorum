//! Agent-performance capture: one row per daemon-spawned agent process.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::role_assignments::AssignmentIdentity;
use crate::routing_attempts::{exclusions, ValidatedFallbackAttribution};
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
    pub configured_profile_id: Option<String>,
    pub configured_provider: Option<String>,
    pub configured_model: Option<String>,
    pub configured_effort: Option<String>,
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

/// Create or reuse the agent_runs row that records the deterministic alternate
/// route named by an immutable [`ValidatedFallbackAttribution`] token.
///
/// The row persists both `role_assignment_id` (the original immutable
/// assignment) and the exact configured alternate profile (in both
/// `model/effort/provider` and the `configured_*` snapshot columns). The insert
/// runs inside `BEGIN IMMEDIATE`; the assignment identity is re-verified
/// against the persisted row, the routing-attempt exclusions are recomputed
/// from live evidence, and the token is rejected when the assignment no longer
/// matches, when the selected profile is now excluded, or when the token's
/// evidence disagrees with an existing row for the same assignment/profile.
///
/// Replay is idempotent: calling twice with the same token returns the same
/// row without inserting a duplicate. The `UNIQUE(role_assignment_id,
/// configured_profile_id)` partial index and the `BEGIN IMMEDIATE` guard mean
/// concurrent mismatched attributions cannot both win — one row is committed
/// per (assignment, configured profile), and stale evidence fails closed rather
/// than inserting a divergent row. This primitive never touches
/// `run_capabilities` or task lifecycle; the daemon binds those separately.
pub fn insert_alternate_with_attribution(
    conn: &mut Connection,
    token: &ValidatedFallbackAttribution,
    agent_name: &str,
    spawned_at: i64,
) -> Result<i64> {
    if agent_name.is_empty() || agent_name.len() > 1024 || agent_name.contains('\0') {
        return Err(QuorumError::Usage("invalid agent name".into()));
    }
    if spawned_at < 0 {
        return Err(QuorumError::Usage(
            "attributed alternate run spawned_at must be non-negative".into(),
        ));
    }
    let task_id = token.task_id().ok_or_else(|| {
        QuorumError::Usage("attributed alternate run requires a task-scoped role assignment".into())
    })?;
    if !matches!(token.role(), "worker" | "reviewer") {
        return Err(QuorumError::Usage(
            "attributed alternate run role must be worker or reviewer".into(),
        ));
    }
    let sub_role: Option<&str> = match (token.role(), token.review_stage()) {
        ("reviewer", Some("r2")) => Some("r2"),
        _ => None,
    };

    let tx = begin_immediate(conn)?;

    let assignment = crate::role_assignments::get(&tx, token.assignment_id())?
        .ok_or_else(|| QuorumError::Io("attributed alternate assignment is missing".into()))?;
    let identity = AssignmentIdentity {
        task_id: token.task_id(),
        responsibility_key: token.responsibility_key(),
        role: token.role(),
        pr_number: token.pr_number(),
        review_stage: token.review_stage(),
    };
    if !assignment.matches_identity(&identity) || assignment.role != token.role() {
        return Err(QuorumError::Io(
            "attributed alternate assignment identity mismatch".into(),
        ));
    }

    // Reuse: an existing row for the same (assignment, configured profile) is
    // authoritative. Verify it agrees with the token before returning, so a
    // replay with tampered evidence still fails closed.
    let existing = fetch_configured_route(&tx, token.assignment_id(), &token.profile().id)?;

    if let Some(row) = existing {
        let id = row.id;
        let profile = token.profile();
        if row.role != token.role()
            || row.sub_role.as_deref() != sub_role
            || row.model != profile.model
            || row.effort != profile.effort
            || row.provider.as_deref() != Some(profile.provider.as_str())
            || row.configured_provider.as_deref() != Some(profile.provider.as_str())
            || row.configured_model.as_deref() != Some(profile.model.as_str())
            || row.configured_effort.as_deref() != Some(profile.effort.as_str())
        {
            return Err(QuorumError::Io(
                "attributed alternate replay conflicts with existing run".into(),
            ));
        }
        tx.commit().map_err(map_sql_err)?;
        return Ok(id);
    }

    // Recompute the exclusion set inside the write transaction so a later
    // classified attempt that invalidates this alternate cannot slip through as
    // an expired token.
    let current = exclusions(&tx, token.responsibility_key())?;
    if current.excludes(token.profile()) {
        return Err(QuorumError::Io(
            "attributed alternate route was invalidated by later routing evidence".into(),
        ));
    }

    let profile = token.profile();
    tx.execute(
        "INSERT INTO agent_runs(
             task_id, agent_name, role, sub_role, model, effort, provider,
             role_assignment_id, spawned_at,
             configured_profile_id, configured_provider, configured_model, configured_effort)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            task_id,
            agent_name,
            token.role(),
            sub_role,
            profile.model,
            profile.effort,
            profile.provider,
            token.assignment_id(),
            spawned_at,
            profile.id,
            profile.provider,
            profile.model,
            profile.effort,
        ],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit().map_err(map_sql_err)?;
    Ok(id)
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
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id,
                configured_profile_id, configured_provider, configured_model, configured_effort,
                spawned_at, ended_at, end_reason
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
                configured_profile_id: r.get(8)?,
                configured_provider: r.get(9)?,
                configured_model: r.get(10)?,
                configured_effort: r.get(11)?,
                spawned_at: r.get(12)?,
                ended_at: r.get(13)?,
                end_reason: r.get(14)?,
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
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id,
                configured_profile_id, configured_provider, configured_model, configured_effort,
                spawned_at, ended_at, end_reason
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
                configured_profile_id: r.get(8)?,
                configured_provider: r.get(9)?,
                configured_model: r.get(10)?,
                configured_effort: r.get(11)?,
                spawned_at: r.get(12)?,
                ended_at: r.get(13)?,
                end_reason: r.get(14)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// All runs for a task, ordered by id.
pub fn runs_for_task(conn: &Connection, task_id: i64) -> Result<Vec<AgentRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id,
                configured_profile_id, configured_provider, configured_model, configured_effort,
                spawned_at, ended_at, end_reason \
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
                configured_profile_id: r.get(8)?,
                configured_provider: r.get(9)?,
                configured_model: r.get(10)?,
                configured_effort: r.get(11)?,
                spawned_at: r.get(12)?,
                ended_at: r.get(13)?,
                end_reason: r.get(14)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
}

/// Fetch the row (if any) that already attributes `profile_id` as an alternate
/// route for `role_assignment_id`. Used by
/// [`insert_alternate_with_attribution`] to detect idempotent replays and
/// concurrent duplicate attributions.
pub(crate) fn fetch_configured_route(
    conn: &Connection,
    role_assignment_id: i64,
    profile_id: &str,
) -> Result<Option<AgentRun>> {
    conn.query_row(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id,
                configured_profile_id, configured_provider, configured_model, configured_effort,
                spawned_at, ended_at, end_reason
         FROM agent_runs
         WHERE role_assignment_id = ?1 AND configured_profile_id = ?2",
        params![role_assignment_id, profile_id],
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
                configured_profile_id: r.get(8)?,
                configured_provider: r.get(9)?,
                configured_model: r.get(10)?,
                configured_effort: r.get(11)?,
                spawned_at: r.get(12)?,
                ended_at: r.get(13)?,
                end_reason: r.get(14)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Latest run identity for a task, without materializing its complete history.
pub fn latest_for_task(conn: &Connection, task_id: i64) -> Result<Option<AgentRun>> {
    conn.query_row(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, role_assignment_id,
                configured_profile_id, configured_provider, configured_model, configured_effort,
                spawned_at, ended_at, end_reason
         FROM agent_runs WHERE task_id = ?1 ORDER BY spawned_at DESC, id DESC LIMIT 1",
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
                configured_profile_id: r.get(8)?,
                configured_provider: r.get(9)?,
                configured_model: r.get(10)?,
                configured_effort: r.get(11)?,
                spawned_at: r.get(12)?,
                ended_at: r.get(13)?,
                end_reason: r.get(14)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
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
        let run = latest_for_task(&c, tid).unwrap().unwrap();
        assert_eq!(run.configured_profile_id, None);
        assert_eq!(run.configured_provider, None);
        assert_eq!(run.configured_model, None);
        assert_eq!(run.configured_effort, None);
    }

    #[test]
    fn configured_route_fixture_round_trips_with_immutable_assignment() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (42,'worker:task:7:revision:1',7,'worker','original','claude','claude',
                     'opus','high','worker:M','generation-1',1)",
            [],
        )
        .unwrap();
        // This internal fixture deliberately bypasses the future guarded writer.
        c.execute(
            "INSERT INTO agent_runs(
                 task_id,agent_name,role,model,effort,provider,role_assignment_id,spawned_at,
                 configured_profile_id,configured_provider,configured_model,configured_effort)
             VALUES (7,'alternate','worker','gpt-5.6-sol','medium','codex',42,2,
                     'sol','codex','gpt-5.6-sol','medium')",
            [],
        )
        .unwrap();

        let run = latest_for_task(&c, 7).unwrap().unwrap();
        assert_eq!(run.role_assignment_id, Some(42));
        assert_eq!(run.configured_profile_id.as_deref(), Some("sol"));
        assert_eq!(run.configured_provider.as_deref(), Some("codex"));
        assert_eq!(run.configured_model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(run.configured_effort.as_deref(), Some("medium"));
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

    mod alternate_attribution {
        use super::*;
        use crate::role_assignments::{
            AssignmentIdentity, ModelProfile, ValidatedPool, WeightedProfile,
        };
        use crate::routing_attempts::{
            record, select_alternate, validate_fallback_attribution, AlternateRoute,
            FailureDisposition, FallbackAttributionInput, RecordRoutingAttempt,
            ValidatedFallbackAttribution,
        };
        use std::sync::{Arc, Barrier};

        fn worker_pool() -> ValidatedPool {
            ValidatedPool {
                pool_key: "worker.M".into(),
                policy_generation: "gen-1".into(),
                profiles: vec![
                    WeightedProfile {
                        profile: ModelProfile {
                            id: "opus".into(),
                            provider: "claude".into(),
                            runner: "claude".into(),
                            model: "claude-opus-4-8".into(),
                            effort: "high".into(),
                        },
                        percent: 50,
                    },
                    WeightedProfile {
                        profile: ModelProfile {
                            id: "sonnet".into(),
                            provider: "claude".into(),
                            runner: "claude".into(),
                            model: "claude-sonnet-4-6".into(),
                            effort: "medium".into(),
                        },
                        percent: 30,
                    },
                    WeightedProfile {
                        profile: ModelProfile {
                            id: "sol".into(),
                            provider: "codex".into(),
                            runner: "codex".into(),
                            model: "gpt-5.6-sol".into(),
                            effort: "high".into(),
                        },
                        percent: 20,
                    },
                ],
            }
        }

        fn seed_worker_assignment(
            conn: &Connection,
            assignment_id: i64,
            task_id: i64,
            pool: &ValidatedPool,
            responsibility: &str,
        ) {
            conn.execute(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                 VALUES (?1,'attribution fixture','working','test',1,1)",
                [task_id],
            )
            .unwrap();
            let primary = &pool.profiles[0].profile;
            conn.execute(
                "INSERT INTO role_assignments(
                     id,responsibility_key,task_id,role,complexity,profile_id,provider,runner,
                     model,effort,pool_key,policy_generation,created_at)
                 VALUES (?1,?2,?3,'worker','M',?4,?5,?6,?7,?8,?9,?10,1)",
                params![
                    assignment_id,
                    responsibility,
                    task_id,
                    primary.id,
                    primary.provider,
                    primary.runner,
                    primary.model,
                    primary.effort,
                    pool.pool_key,
                    pool.policy_generation,
                ],
            )
            .unwrap();
        }

        fn seed_reviewer_assignment(
            conn: &Connection,
            assignment_id: i64,
            task_id: i64,
            pr: i64,
            stage: &str,
            pool: &ValidatedPool,
            responsibility: &str,
        ) {
            conn.execute(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                 VALUES (?1,'attribution fixture','working','test',1,1)",
                [task_id],
            )
            .unwrap();
            let primary = &pool.profiles[0].profile;
            conn.execute(
                "INSERT INTO role_assignments(
                     id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                     profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES (?1,?2,?3,?4,'reviewer',?5,'M',?6,?7,?8,?9,?10,?11,?12,1)",
                params![
                    assignment_id,
                    responsibility,
                    task_id,
                    pr,
                    stage,
                    primary.id,
                    primary.provider,
                    primary.runner,
                    primary.model,
                    primary.effort,
                    pool.pool_key,
                    pool.policy_generation,
                ],
            )
            .unwrap();
        }

        fn issue_alternate_token(
            conn: &mut Connection,
            assignment_id: i64,
            responsibility: &str,
            pool: &ValidatedPool,
            failed_index: usize,
            disposition: FailureDisposition,
            identity: AssignmentIdentity<'_>,
        ) -> ValidatedFallbackAttribution {
            record(
                conn,
                &RecordRoutingAttempt {
                    role_assignment_id: assignment_id,
                    responsibility_key: responsibility,
                    profile: &pool.profiles[failed_index].profile,
                    failure_disposition: Some(disposition),
                    recorded_at: 10,
                },
                pool,
            )
            .unwrap();
            let attempts = crate::routing_attempts::list(conn, responsibility).unwrap();
            let attempt = attempts.last().unwrap().clone();
            let assignment = crate::role_assignments::get(conn, assignment_id)
                .unwrap()
                .unwrap();
            let excluded = crate::routing_attempts::exclusions(conn, responsibility).unwrap();
            let selected = match select_alternate(&assignment, pool, &excluded).unwrap() {
                AlternateRoute::Selected(profile) => profile,
                other => panic!("expected an alternate selection, got {other:?}"),
            };
            let input = FallbackAttributionInput {
                assignment: &assignment,
                identity,
                eligible_pool: pool,
                attempt: &attempt,
                exclusions: &excluded,
                selected_profile: &selected,
            };
            validate_fallback_attribution(&input)
                .unwrap()
                .expect("valid fallback evidence must issue a token")
        }

        #[test]
        fn happy_path_persists_alternate_bindings_and_original_route_unchanged() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:7";
            seed_worker_assignment(&c, 42, 7, &pool, responsibility);

            let token = issue_alternate_token(
                &mut c,
                42,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(7),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );
            let run_id =
                insert_alternate_with_attribution(&mut c, &token, "Dowel-9nmu", 100).unwrap();

            let row = latest_for_task(&c, 7).unwrap().unwrap();
            assert_eq!(row.id, run_id);
            assert_eq!(row.agent, "Dowel-9nmu");
            assert_eq!(row.role, "worker");
            assert_eq!(row.sub_role, None);
            // Excluded provider=claude → selected=sol (the codex profile).
            assert_eq!(row.model, "gpt-5.6-sol");
            assert_eq!(row.effort, "high");
            assert_eq!(row.provider.as_deref(), Some("codex"));
            assert_eq!(row.role_assignment_id, Some(42));
            assert_eq!(row.configured_profile_id.as_deref(), Some("sol"));
            assert_eq!(row.configured_provider.as_deref(), Some("codex"));
            assert_eq!(row.configured_model.as_deref(), Some("gpt-5.6-sol"));
            assert_eq!(row.configured_effort.as_deref(), Some("high"));
            assert_eq!(row.spawned_at, 100);
            let task_id: i64 = c
                .query_row(
                    "SELECT task_id FROM agent_runs WHERE id=?1",
                    [run_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(task_id, 7);

            // Original-route insertion for the same assignment stays compatible.
            let responsibility_key = format!("worker:task:{}:revision:1", 7);
            c.execute(
                "INSERT INTO role_assignments(
                     id,responsibility_key,task_id,role,profile_id,provider,runner,
                     model,effort,pool_key,policy_generation,created_at)
                 VALUES (43,?1,7,'worker','opus','claude','claude','claude-opus-4-8','high',
                         'worker.M','gen-1',1)",
                [&responsibility_key],
            )
            .unwrap();
            let original_run = insert_worker_with_assignment(
                &c,
                7,
                "OriginalWorker",
                &responsibility_key,
                "claude-opus-4-8",
                "high",
                "claude",
                "claude",
                Some(43),
                200,
            )
            .unwrap();
            assert!(original_run > 0);
            let original = latest_for_task(&c, 7).unwrap().unwrap();
            assert_eq!(original.role_assignment_id, Some(43));
            assert_eq!(original.configured_profile_id, None);
        }

        #[test]
        fn replay_is_idempotent_and_conflicting_replay_fails_closed() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:11";
            seed_worker_assignment(&c, 11, 11, &pool, responsibility);

            let token = issue_alternate_token(
                &mut c,
                11,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(11),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );
            let first = insert_alternate_with_attribution(&mut c, &token, "Alice", 100).unwrap();
            let replay = insert_alternate_with_attribution(&mut c, &token, "Alice", 100).unwrap();
            assert_eq!(first, replay);
            // Different agent_name / spawned_at with the SAME token still fold to
            // the same attributed row — the (assignment, configured profile) pair
            // is the reuse identity.
            let replay_with_different_process =
                insert_alternate_with_attribution(&mut c, &token, "Different-Agent", 999).unwrap();
            assert_eq!(first, replay_with_different_process);
            let count: i64 = c
                .query_row(
                    "SELECT count(*) FROM agent_runs WHERE role_assignment_id=11
                     AND configured_profile_id IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);

            // Corrupt the persisted row to simulate a tampered replay path — the
            // stored profile no longer agrees with the token, so replay rejects.
            c.execute(
                "UPDATE agent_runs SET configured_model='tampered' WHERE id=?1",
                [first],
            )
            .unwrap();
            let result = insert_alternate_with_attribution(&mut c, &token, "Alice", 100);
            assert!(result.is_err());
        }

        #[test]
        fn missing_or_identity_mismatched_assignment_fails_closed() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:5";
            seed_worker_assignment(&c, 5, 5, &pool, responsibility);
            let token = issue_alternate_token(
                &mut c,
                5,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProfileUnavailable,
                AssignmentIdentity {
                    task_id: Some(5),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );

            // Rebind the assignment's responsibility identity, simulating a
            // mismatched-token / rebound assignment window.
            c.execute(
                "UPDATE role_assignments SET responsibility_key='worker:rebound' WHERE id=5",
                [],
            )
            .unwrap();
            assert!(insert_alternate_with_attribution(&mut c, &token, "Alice", 100).is_err());

            // A token whose assignment_id names an id that does not exist here
            // also fails closed. routing_attempts are immutable-by-trigger and
            // FK back to role_assignments, so cover the "missing" branch by
            // replaying the token into a fresh DB where that id was never
            // created.
            let fresh_dir = tempfile::tempdir().unwrap();
            let mut fresh = crate::db::open(&fresh_dir.path().join("fresh.db")).unwrap();
            assert!(insert_alternate_with_attribution(&mut fresh, &token, "Alice", 100).is_err());
        }

        #[test]
        fn expired_token_from_later_routing_attempt_fails_closed() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:12";
            seed_worker_assignment(&c, 12, 12, &pool, responsibility);
            let token = issue_alternate_token(
                &mut c,
                12,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(12),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );
            // Selected alternate was `sol`; a later profile-unavailable attempt for
            // `sol` invalidates the token, expiring it before the row lands.
            record(
                &mut c,
                &RecordRoutingAttempt {
                    role_assignment_id: 12,
                    responsibility_key: responsibility,
                    profile: &pool.profiles[2].profile,
                    failure_disposition: Some(FailureDisposition::ProfileUnavailable),
                    recorded_at: 15,
                },
                &pool,
            )
            .unwrap();

            let result = insert_alternate_with_attribution(&mut c, &token, "Alice", 100);
            assert!(result.is_err(), "expired token must be rejected");
            let inserted: i64 = c
                .query_row(
                    "SELECT count(*) FROM agent_runs WHERE role_assignment_id=12",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(inserted, 0);
        }

        #[test]
        fn reviewer_stage_r2_is_persisted_as_sub_role_and_r1_stays_null() {
            let (_d, mut c) = open_tmp();
            let pool = ValidatedPool {
                pool_key: "reviewer.M.r2".into(),
                policy_generation: "gen-1".into(),
                ..worker_pool()
            };
            let r2_responsibility = "reviewer:task:20:pr:8:r2";
            seed_reviewer_assignment(&c, 20, 20, 8, "r2", &pool, r2_responsibility);
            let token = issue_alternate_token(
                &mut c,
                20,
                r2_responsibility,
                &pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(20),
                    responsibility_key: r2_responsibility,
                    role: "reviewer",
                    pr_number: Some(8),
                    review_stage: Some("r2"),
                },
            );
            let run = insert_alternate_with_attribution(&mut c, &token, "R2-Alt", 100).unwrap();
            let sub_role: Option<String> = c
                .query_row(
                    "SELECT sub_role FROM agent_runs WHERE id=?1",
                    [run],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(sub_role.as_deref(), Some("r2"));

            let r1_pool = ValidatedPool {
                pool_key: "reviewer.M.r1".into(),
                policy_generation: "gen-1".into(),
                ..worker_pool()
            };
            let r1_responsibility = "reviewer:task:21:pr:9:r1";
            seed_reviewer_assignment(&c, 21, 21, 9, "r1", &r1_pool, r1_responsibility);
            let r1_token = issue_alternate_token(
                &mut c,
                21,
                r1_responsibility,
                &r1_pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(21),
                    responsibility_key: r1_responsibility,
                    role: "reviewer",
                    pr_number: Some(9),
                    review_stage: Some("r1"),
                },
            );
            let r1_run =
                insert_alternate_with_attribution(&mut c, &r1_token, "R1-Alt", 101).unwrap();
            let r1_sub: Option<String> = c
                .query_row(
                    "SELECT sub_role FROM agent_runs WHERE id=?1",
                    [r1_run],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(r1_sub, None, "r1 must not populate the r2-only sub_role");
        }

        #[test]
        fn invalid_input_and_taskless_token_fail_usage() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:33";
            seed_worker_assignment(&c, 33, 33, &pool, responsibility);
            let token = issue_alternate_token(
                &mut c,
                33,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProviderUnavailable,
                AssignmentIdentity {
                    task_id: Some(33),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );

            assert!(matches!(
                insert_alternate_with_attribution(&mut c, &token, "", 100),
                Err(QuorumError::Usage(_))
            ));
            assert!(matches!(
                insert_alternate_with_attribution(&mut c, &token, "bad\0name", 100),
                Err(QuorumError::Usage(_))
            ));
            assert!(matches!(
                insert_alternate_with_attribution(&mut c, &token, "Alice", -1),
                Err(QuorumError::Usage(_))
            ));

            // Task-less role assignments cannot attribute an agent_runs row
            // because agent_runs.task_id is NOT NULL. Construct such an
            // assignment directly and drive the validator to prove the writer
            // fails closed on a valid-shaped but task-less token.
            c.execute(
                "INSERT INTO role_assignments(
                     id,responsibility_key,task_id,role,profile_id,provider,runner,
                     model,effort,pool_key,policy_generation,created_at)
                 VALUES (44,'worker:taskless',NULL,'worker','opus','claude','claude',
                         'claude-opus-4-8','high','worker.M','gen-1',1)",
                [],
            )
            .unwrap();
            let taskless_assignment = crate::role_assignments::get(&c, 44).unwrap().unwrap();
            record(
                &mut c,
                &RecordRoutingAttempt {
                    role_assignment_id: 44,
                    responsibility_key: "worker:taskless",
                    profile: &pool.profiles[0].profile,
                    failure_disposition: Some(FailureDisposition::ProviderUnavailable),
                    recorded_at: 5,
                },
                &pool,
            )
            .unwrap();
            let taskless_attempts = crate::routing_attempts::list(&c, "worker:taskless").unwrap();
            let taskless_excluded =
                crate::routing_attempts::exclusions(&c, "worker:taskless").unwrap();
            let taskless_selected =
                match select_alternate(&taskless_assignment, &pool, &taskless_excluded).unwrap() {
                    AlternateRoute::Selected(profile) => profile,
                    other => panic!("expected alternate selection, got {other:?}"),
                };
            let taskless_token = validate_fallback_attribution(&FallbackAttributionInput {
                assignment: &taskless_assignment,
                identity: AssignmentIdentity {
                    task_id: None,
                    responsibility_key: "worker:taskless",
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
                eligible_pool: &pool,
                attempt: taskless_attempts.last().unwrap(),
                exclusions: &taskless_excluded,
                selected_profile: &taskless_selected,
            })
            .unwrap()
            .expect("valid task-less evidence must issue a token");
            assert!(matches!(
                insert_alternate_with_attribution(&mut c, &taskless_token, "Alice", 100),
                Err(QuorumError::Usage(_))
            ));
        }

        #[test]
        fn concurrent_same_evidence_converges_on_one_attributed_row() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("concurrent-same.db");
            let pool = worker_pool();
            let responsibility = "worker:task:70";
            let token = {
                let mut c = crate::db::open(&path).unwrap();
                seed_worker_assignment(&c, 70, 70, &pool, responsibility);
                issue_alternate_token(
                    &mut c,
                    70,
                    responsibility,
                    &pool,
                    0,
                    FailureDisposition::ProviderUnavailable,
                    AssignmentIdentity {
                        task_id: Some(70),
                        responsibility_key: responsibility,
                        role: "worker",
                        pr_number: None,
                        review_stage: None,
                    },
                )
            };

            let barrier = Arc::new(Barrier::new(8));
            let mut handles = Vec::new();
            for thread in 0..8 {
                let path = path.clone();
                let token = token.clone();
                let barrier = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    insert_alternate_with_attribution(
                        &mut conn,
                        &token,
                        &format!("agent-{thread}"),
                        100 + thread,
                    )
                    .unwrap()
                }));
            }
            let ids: std::collections::HashSet<i64> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect();
            assert_eq!(ids.len(), 1);

            let conn = crate::db::open(&path).unwrap();
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM agent_runs
                     WHERE role_assignment_id=70 AND configured_profile_id IS NOT NULL",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                1
            );
        }

        #[test]
        fn concurrent_stale_and_current_tokens_cannot_both_write_for_the_same_evidence() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("concurrent-mismatch.db");
            let pool = worker_pool();
            let responsibility = "worker:task:80";
            let (stale_token, current_token) = {
                let mut c = crate::db::open(&path).unwrap();
                seed_worker_assignment(&c, 80, 80, &pool, responsibility);
                // ProfileUnavailable excludes just `opus`, leaving `sonnet`
                // (30%) as the stale token's selected alternate.
                let stale = issue_alternate_token(
                    &mut c,
                    80,
                    responsibility,
                    &pool,
                    0,
                    FailureDisposition::ProfileUnavailable,
                    AssignmentIdentity {
                        task_id: Some(80),
                        responsibility_key: responsibility,
                        role: "worker",
                        pr_number: None,
                        review_stage: None,
                    },
                );
                // Later evidence excludes `sonnet` (the stale token's selected
                // profile). A fresh token now selects the next executable
                // alternate (`sol`).
                record(
                    &mut c,
                    &RecordRoutingAttempt {
                        role_assignment_id: 80,
                        responsibility_key: responsibility,
                        profile: &pool.profiles[1].profile,
                        failure_disposition: Some(FailureDisposition::ProfileUnavailable),
                        recorded_at: 20,
                    },
                    &pool,
                )
                .unwrap();
                let assignment = crate::role_assignments::get(&c, 80).unwrap().unwrap();
                let excluded = crate::routing_attempts::exclusions(&c, responsibility).unwrap();
                let selected = match select_alternate(&assignment, &pool, &excluded).unwrap() {
                    AlternateRoute::Selected(profile) => profile,
                    other => panic!("expected an alternate selection, got {other:?}"),
                };
                let attempts = crate::routing_attempts::list(&c, responsibility).unwrap();
                // Use the ORIGINAL classified attempt for the current token
                // (the one that first excluded the primary profile); the second
                // profile-unavailable attempt is what makes the stale token's
                // selection now excluded.
                let attempt = attempts
                    .iter()
                    .find(|a| a.profile.id == pool.profiles[0].profile.id)
                    .unwrap()
                    .clone();
                let current = validate_fallback_attribution(&FallbackAttributionInput {
                    assignment: &assignment,
                    identity: AssignmentIdentity {
                        task_id: Some(80),
                        responsibility_key: responsibility,
                        role: "worker",
                        pr_number: None,
                        review_stage: None,
                    },
                    eligible_pool: &pool,
                    attempt: &attempt,
                    exclusions: &excluded,
                    selected_profile: &selected,
                })
                .unwrap()
                .expect("current fresh evidence must issue a token");
                assert_ne!(stale.profile(), current.profile());
                (stale, current)
            };

            let barrier = Arc::new(Barrier::new(2));
            let stale_path = path.clone();
            let current_path = path.clone();
            let stale_barrier = Arc::clone(&barrier);
            let current_barrier = Arc::clone(&barrier);
            let stale_handle = std::thread::spawn(move || {
                let mut conn = crate::db::open(&stale_path).unwrap();
                stale_barrier.wait();
                insert_alternate_with_attribution(&mut conn, &stale_token, "stale", 100)
            });
            let current_handle = std::thread::spawn(move || {
                let mut conn = crate::db::open(&current_path).unwrap();
                current_barrier.wait();
                insert_alternate_with_attribution(&mut conn, &current_token, "current", 101)
            });
            let stale_result = stale_handle.join().unwrap();
            let current_result = current_handle.join().unwrap();

            // The stale token's profile is now excluded; only the current
            // token's selection wins. The current token always succeeds.
            assert!(
                stale_result.is_err(),
                "the stale token must fail closed instead of writing a mismatched attribution"
            );
            assert!(current_result.is_ok());
            let rows: Vec<(i64, String)> = crate::db::open(&path)
                .unwrap()
                .prepare(
                    "SELECT id,configured_profile_id FROM agent_runs
                     WHERE role_assignment_id=80 AND configured_profile_id IS NOT NULL
                     ORDER BY id",
                )
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, current_result.unwrap());
        }

        #[test]
        fn distinct_alternates_over_time_produce_distinct_rows_for_the_same_assignment() {
            let (_d, mut c) = open_tmp();
            let pool = worker_pool();
            let responsibility = "worker:task:91";
            seed_worker_assignment(&c, 91, 91, &pool, responsibility);

            // Exclude just the primary so both `sonnet` (30%) and `sol` (20%)
            // remain executable alternates.
            let first_token = issue_alternate_token(
                &mut c,
                91,
                responsibility,
                &pool,
                0,
                FailureDisposition::ProfileUnavailable,
                AssignmentIdentity {
                    task_id: Some(91),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
            );
            let first_run =
                insert_alternate_with_attribution(&mut c, &first_token, "first", 100).unwrap();

            // The first alternate (`sonnet`) is now unavailable — the daemon
            // records its classified failure and selects the next executable
            // profile (`sol`).
            record(
                &mut c,
                &RecordRoutingAttempt {
                    role_assignment_id: 91,
                    responsibility_key: responsibility,
                    profile: &pool.profiles[1].profile,
                    failure_disposition: Some(FailureDisposition::ProfileUnavailable),
                    recorded_at: 20,
                },
                &pool,
            )
            .unwrap();
            let assignment = crate::role_assignments::get(&c, 91).unwrap().unwrap();
            let excluded = crate::routing_attempts::exclusions(&c, responsibility).unwrap();
            let selected = match select_alternate(&assignment, &pool, &excluded).unwrap() {
                AlternateRoute::Selected(profile) => profile,
                other => panic!("expected an alternate selection, got {other:?}"),
            };
            let attempts = crate::routing_attempts::list(&c, responsibility).unwrap();
            let attempt = attempts
                .iter()
                .find(|a| a.profile.id == pool.profiles[0].profile.id)
                .unwrap()
                .clone();
            let second_token = validate_fallback_attribution(&FallbackAttributionInput {
                assignment: &assignment,
                identity: AssignmentIdentity {
                    task_id: Some(91),
                    responsibility_key: responsibility,
                    role: "worker",
                    pr_number: None,
                    review_stage: None,
                },
                eligible_pool: &pool,
                attempt: &attempt,
                exclusions: &excluded,
                selected_profile: &selected,
            })
            .unwrap()
            .expect("fresh evidence must issue a distinct alternate token");
            assert_ne!(first_token.profile(), second_token.profile());

            let second_run =
                insert_alternate_with_attribution(&mut c, &second_token, "second", 200).unwrap();
            assert_ne!(first_run, second_run);
            let count: i64 = c
                .query_row(
                    "SELECT count(*) FROM agent_runs
                     WHERE role_assignment_id=91 AND configured_profile_id IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 2);
        }
    }
}
