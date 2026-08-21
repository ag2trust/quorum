//! Bounded, authoritative context for reviews of generated graph children.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::role_assignments::{guarded_evidence_insert, EvidenceAssignmentContext};
use rusqlite::{params, Connection, OptionalExtension, ToSql};
use serde::Serialize;
use std::collections::HashSet;

pub const MAX_REVIEW_PREREQUISITES: usize = 32;
pub const MAX_REVIEW_FIELD_BYTES: usize = 4 * 1024;
pub const MAX_REVIEW_CONTEXT_BYTES: usize = 24 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewPrerequisite {
    pub task_id: i64,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphReviewContext {
    pub graph_id: i64,
    pub plan_revision: i64,
    pub task_id: i64,
    pub local_key: String,
    pub assigned_title: String,
    pub assigned_requirements: String,
    pub direct_prerequisites: Vec<ReviewPrerequisite>,
}

struct MemberRow {
    graph_id: i64,
    plan_revision: i64,
    local_key: String,
    title: String,
    body: String,
    active: i64,
    graph_active: i64,
    graph_state: String,
    accepted_revision: Option<i64>,
    source_status: String,
}

impl GraphReviewContext {
    pub fn to_bounded_json(&self) -> Result<String> {
        let raw = serde_json::to_string(self)
            .map_err(|error| QuorumError::Io(format!("graph review context: {error}")))?;
        if raw.len() > MAX_REVIEW_CONTEXT_BYTES {
            return Err(QuorumError::Usage(
                "generated review context exceeds bounded size".into(),
            ));
        }
        Ok(raw)
    }
}

fn valid_field(value: &str) -> bool {
    !value.contains('\0') && value.len() <= MAX_REVIEW_FIELD_BYTES
}

/// Load review scope only for a current generated child. An ordinary task has
/// no membership and returns `None`; stale or corrupt membership fails loud.
pub fn load(conn: &Connection, task_id: i64) -> Result<Option<GraphReviewContext>> {
    let member: Option<MemberRow> = conn
        .query_row(
            "SELECT m.graph_id,m.plan_revision,m.local_key,t.title,t.body,m.active,
                    d.active,d.state,d.accepted_plan_revision,source.status
             FROM task_graph_members m
             JOIN tasks t ON t.id=m.task_id
             JOIN task_decompositions d ON d.id=m.graph_id
             JOIN tasks source ON source.id=d.source_task_id
             WHERE m.task_id=?1",
            [task_id],
            |row| {
                Ok(MemberRow {
                    graph_id: row.get(0)?,
                    plan_revision: row.get(1)?,
                    local_key: row.get(2)?,
                    title: row.get(3)?,
                    body: row.get(4)?,
                    active: row.get(5)?,
                    graph_active: row.get(6)?,
                    graph_state: row.get(7)?,
                    accepted_revision: row.get(8)?,
                    source_status: row.get(9)?,
                })
            },
        )
        .optional()?;
    let Some(member) = member else {
        return Ok(None);
    };
    if member.active != 1
        || member.graph_active != 1
        || member.graph_state != "active"
        || member.accepted_revision != Some(member.plan_revision)
        || member.source_status != "decomposed"
    {
        return Err(QuorumError::Usage(
            "generated review task is not in the current active graph plan".into(),
        ));
    }
    // The body is the structured assignment assembled from multiple bounded planner fields.
    // Its aggregate bound is enforced by `to_bounded_json` below.
    if !valid_field(&member.local_key) || !valid_field(&member.title) || member.body.contains('\0')
    {
        return Err(QuorumError::Usage(
            "generated review assignment contains invalid bounded text".into(),
        ));
    }

    let depends_on: Option<String> = conn.query_row(
        "SELECT depends_on FROM tasks WHERE id=?1",
        [task_id],
        |row| row.get(0),
    )?;
    let dependency_ids: Vec<i64> = serde_json::from_str(depends_on.as_deref().unwrap_or("[]"))
        .map_err(|_| QuorumError::Usage("generated task has malformed dependencies".into()))?;
    if dependency_ids.len() > MAX_REVIEW_PREREQUISITES {
        return Err(QuorumError::Usage(
            "generated review task has too many direct prerequisites".into(),
        ));
    }
    if dependency_ids.iter().any(|id| *id <= 0)
        || dependency_ids.iter().collect::<HashSet<_>>().len() != dependency_ids.len()
    {
        return Err(QuorumError::Usage(
            "generated review task has invalid direct prerequisites".into(),
        ));
    }
    let mut direct_prerequisites = Vec::with_capacity(dependency_ids.len());
    for dependency_id in dependency_ids {
        let prerequisite: Option<(String, String)> = conn
            .query_row(
                "SELECT title,status FROM tasks WHERE id=?1",
                [dependency_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((title, status)) = prerequisite else {
            return Err(QuorumError::Usage(
                "generated review task has a missing direct prerequisite".into(),
            ));
        };
        if !valid_field(&title)
            || !valid_field(&status)
            || status.parse::<crate::lifecycle::Status>().is_err()
        {
            return Err(QuorumError::Usage(
                "generated review prerequisite contains invalid bounded text".into(),
            ));
        }
        direct_prerequisites.push(ReviewPrerequisite {
            task_id: dependency_id,
            title,
            status,
        });
    }
    let context = GraphReviewContext {
        graph_id: member.graph_id,
        plan_revision: member.plan_revision,
        task_id,
        local_key: member.local_key,
        assigned_title: member.title,
        assigned_requirements: member.body,
        direct_prerequisites,
    };
    context.to_bounded_json()?;
    Ok(Some(context))
}

/// Atomically load current generated-child scope and issue its reviewer run
/// capability. Source cancellation and reviewer authority therefore have a
/// single SQLite serialization point. Ordinary tasks still issue normally.
pub fn load_and_issue_capability(
    conn: &mut Connection,
    task_id: i64,
    run_id: &str,
    agent: &str,
    now: i64,
) -> Result<Option<String>> {
    if run_id.is_empty()
        || run_id.contains('\0')
        || agent.is_empty()
        || agent.contains('\0')
        || task_id <= 0
    {
        return Err(QuorumError::BadInput(
            "invalid reviewer capability identity".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let context = load(&tx, task_id)?
        .map(|context| context.to_bounded_json())
        .transpose()?;
    tx.execute(
        "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
         VALUES (?1,?2,?3,'reviewer',?4)",
        params![run_id, task_id, agent, now],
    )?;
    tx.commit()?;
    Ok(context)
}

/// Persist the spawned reviewer run only while its capability and graph
/// authority are still current. This is the post-spawn half of provisioning:
/// cancellation either revokes first (no run is inserted) or observes the
/// durable run/journal and cleanup owns the process.
#[allow(clippy::too_many_arguments)]
pub fn persist_reviewer_run_if_current(
    conn: &mut Connection,
    task_id: i64,
    agent: &str,
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
    let tx = begin_immediate(conn)?;
    let authority_current: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM run_capabilities c
             WHERE c.run_id=?3 AND c.task_id=?1 AND c.agent=?2
               AND c.role='reviewer' AND c.revoked_at IS NULL
               AND (NOT EXISTS(SELECT 1 FROM task_graph_members WHERE task_id=?1)
                    OR EXISTS(SELECT 1 FROM task_graph_members m
                              JOIN task_decompositions d ON d.id=m.graph_id
                              JOIN tasks source ON source.id=d.source_task_id
                              WHERE m.task_id=?1 AND m.active=1 AND d.active=1
                                AND d.state='active' AND source.status='decomposed'
                                AND d.accepted_plan_revision=m.plan_revision))
         )",
        params![task_id, agent, cap_run_id],
        |row| row.get(0),
    )?;
    if !authority_current {
        return Ok(None);
    }

    let review_stage = if sub_role == Some("r2") { "r2" } else { "r1" };
    let responsibility_key = format!("reviewer:task:{task_id}:{review_stage}");
    let context = EvidenceAssignmentContext {
        role_assignment_id,
        task_id: Some(task_id),
        responsibility_key: &responsibility_key,
        role: "reviewer",
        provider,
        runner: provider,
        model,
        effort,
    };
    let parameters: [(&str, &dyn ToSql); 10] = [
        (":task_id", &task_id),
        (":agent", &agent),
        (":model", &model),
        (":effort", &effort),
        (":provider", &provider),
        (":spawned_at", &spawned_at),
        (":sub_role", &sub_role),
        (":cap_run_id", &cap_run_id),
        (":pr", &pr),
        (":head_sha", &head_sha),
    ];
    guarded_evidence_insert(
        &tx,
        "reviewer run",
        &context,
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
             role_assignment_id,spawned_at,sub_role,review_cap_run_id,review_pr,review_head_sha)
         SELECT :task_id,:agent,'reviewer',:model,:effort,:provider,
                :quorum_assignment_id,:spawned_at,:sub_role,:cap_run_id,:pr,:head_sha
         /* quorum-role-assignment-guard */
           AND (:quorum_assignment_id IS NULL OR EXISTS(
               SELECT 1 FROM role_assignments AS reviewer_assignment
               WHERE reviewer_assignment.id=:quorum_assignment_id
                 AND reviewer_assignment.pr_number=:pr
                 AND reviewer_assignment.review_stage=CASE
                     WHEN :sub_role='r2' THEN 'r2' ELSE 'r1' END
                 AND reviewer_assignment.complexity IS NOT NULL
           ))
           AND EXISTS(SELECT 1 FROM run_capabilities c
                      WHERE c.run_id=:cap_run_id AND c.task_id=:task_id AND c.agent=:agent
                        AND c.role='reviewer' AND c.revoked_at IS NULL)
           AND (NOT EXISTS(SELECT 1 FROM task_graph_members WHERE task_id=:task_id)
                OR EXISTS(SELECT 1 FROM task_graph_members m
                          JOIN task_decompositions d ON d.id=m.graph_id
                          JOIN tasks source ON source.id=d.source_task_id
                          WHERE m.task_id=:task_id AND m.active=1 AND d.active=1
                            AND d.state='active' AND source.status='decomposed'
                            AND d.accepted_plan_revision=m.plan_revision))",
        &parameters,
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::sync::{Arc, Barrier};

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::migrate(&conn).unwrap();
        seed(&conn);
        conn
    }

    fn file_fixture() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("review.db")).unwrap();
        seed(&conn);
        (dir, conn)
    }

    fn seed(conn: &Connection) {
        for (id, title, body, status, depends) in [
            (1, "source", "source outcome", "decomposed", None),
            (2, "direct prerequisite", "prereq body", "done", None),
            (
                3,
                "assigned child",
                "only implement parser",
                "in-review",
                Some("[2]"),
            ),
            (4, "unrelated sibling", "do not leak me", "open", None),
            (5, "ordinary", "ordinary body", "in-review", None),
        ] {
            conn.execute(
                "INSERT INTO tasks(id,title,body,status,created_by,created_at,updated_at,depends_on)
                 VALUES (?1,?2,?3,?4,'owner',1,1,?5)",
                params![id, title, body, status, depends],
            ).unwrap();
        }
        conn.execute(
            "INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,
                 planned_source_revision,plan_revision,accepted_plan_revision,created_at,updated_at)
             VALUES (9,1,'active',1,0,1,2,2,1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (9,3,'parser',2,1),(9,4,'cli',2,1)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn loads_only_assignment_and_direct_prerequisites() {
        let context = load(&fixture(), 3).unwrap().unwrap();
        let json = context.to_bounded_json().unwrap();
        assert!(json.contains("only implement parser"));
        assert!(json.contains("direct prerequisite"));
        assert!(!json.contains("unrelated sibling"));
        assert!(!json.contains("do not leak me"));
    }

    #[test]
    fn ordinary_task_has_no_graph_context() {
        assert_eq!(load(&fixture(), 5).unwrap(), None);
    }

    #[test]
    fn stale_plan_and_malformed_or_unbounded_inputs_fail_loud() {
        let conn = fixture();
        conn.execute(
            "UPDATE task_graph_members SET plan_revision=1 WHERE task_id=3",
            [],
        )
        .unwrap();
        assert!(load(&conn, 3).is_err());

        let conn = fixture();
        conn.execute("UPDATE task_decompositions SET active=0 WHERE id=9", [])
            .unwrap();
        assert!(load(&conn, 3).is_err());

        let conn = fixture();
        conn.execute("UPDATE tasks SET status='corrupt' WHERE id=2", [])
            .unwrap();
        assert!(load(&conn, 3).is_err());

        let conn = fixture();
        conn.execute("UPDATE tasks SET depends_on='not-json' WHERE id=3", [])
            .unwrap();
        assert!(load(&conn, 3).is_err());

        let conn = fixture();
        conn.execute("UPDATE tasks SET body=?1 WHERE id=3", ["x\0y"])
            .unwrap();
        assert!(load(&conn, 3).is_err());

        let conn = fixture();
        let ids: Vec<i64> = (1..=(MAX_REVIEW_PREREQUISITES as i64 + 1)).collect();
        conn.execute(
            "UPDATE tasks SET depends_on=?1 WHERE id=3",
            [serde_json::to_string(&ids).unwrap()],
        )
        .unwrap();
        assert!(load(&conn, 3).is_err());
    }

    #[test]
    fn assignment_body_uses_total_context_bound_not_scalar_field_bound() {
        let conn = fixture();
        let body = "x".repeat(MAX_REVIEW_FIELD_BYTES + 1);
        conn.execute("UPDATE tasks SET body=?1 WHERE id=3", [&body])
            .unwrap();

        let context = load(&conn, 3).unwrap().unwrap();
        assert_eq!(context.assigned_requirements, body);
        assert!(context.to_bounded_json().unwrap().len() <= MAX_REVIEW_CONTEXT_BYTES);

        let oversized = "x".repeat(MAX_REVIEW_CONTEXT_BYTES);
        conn.execute("UPDATE tasks SET body=?1 WHERE id=3", [&oversized])
            .unwrap();
        assert!(load(&conn, 3).is_err());
    }

    #[test]
    fn corrupt_context_issues_no_reviewer_capability() {
        let mut conn = fixture();
        conn.execute("UPDATE task_decompositions SET active=0 WHERE id=9", [])
            .unwrap();
        assert!(load_and_issue_capability(&mut conn, 3, "cap", "R", 10).is_err());
        let capabilities: i64 = conn
            .query_row("SELECT count(*) FROM run_capabilities", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(capabilities, 0);
    }

    #[test]
    fn routed_r1_and_r2_runs_match_assignment_semantics() {
        let (_dir, mut conn) = file_fixture();
        for (id, agent, stage, cap, sub_role) in [
            (41, "R1", "r1", "cap-r1", None),
            (42, "R2", "r2", "cap-r2", Some("r2")),
        ] {
            load_and_issue_capability(&mut conn, 5, cap, agent, 10).unwrap();
            conn.execute(
                "INSERT INTO role_assignments(
                     id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                     profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES (?1,'reviewer:task:5:' || ?2,5,71,'reviewer',?2,'M',
                         'profile','codex','codex','model','high',
                         'reviewer.M.' || ?2,'g1',1)",
                params![id, stage],
            )
            .unwrap();
            assert!(persist_reviewer_run_if_current(
                &mut conn,
                5,
                agent,
                "model",
                "high",
                "codex",
                Some(id),
                11,
                sub_role,
                cap,
                71,
                "0123456789abcdef0123456789abcdef01234567",
            )
            .unwrap()
            .is_some());
        }
        let rows: Vec<(String, Option<String>, i64)> = conn
            .prepare("SELECT agent_name,sub_role,role_assignment_id FROM agent_runs ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("R1".into(), None, 41),
                ("R2".into(), Some("r2".into()), 42)
            ]
        );
    }

    #[test]
    fn historical_null_assignment_reviewer_run_remains_valid() {
        let (_dir, mut conn) = file_fixture();
        load_and_issue_capability(&mut conn, 5, "historical-cap", "Historical-R1", 10).unwrap();

        let run_id = persist_reviewer_run_if_current(
            &mut conn,
            5,
            "Historical-R1",
            "legacy-model",
            "high",
            "claude",
            None,
            11,
            None,
            "historical-cap",
            71,
            "0123456789abcdef0123456789abcdef01234567",
        )
        .unwrap()
        .unwrap();
        let stored_assignment: Option<i64> = conn
            .query_row(
                "SELECT role_assignment_id FROM agent_runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_assignment, None);
    }

    #[test]
    fn mismatched_reviewer_assignment_inserts_no_run_or_lifecycle_change() {
        let (_dir, mut conn) = file_fixture();
        load_and_issue_capability(&mut conn, 5, "cap", "R", 10).unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                 profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (51,'reviewer:task:5:r2',5,71,'reviewer','r2','M',
                     'profile','codex','codex','model','high','reviewer.M.r2','g1',1)",
            [],
        )
        .unwrap();

        assert!(persist_reviewer_run_if_current(
            &mut conn,
            5,
            "R",
            "model",
            "high",
            "codex",
            Some(51),
            11,
            None,
            "cap",
            71,
            "0123456789abcdef0123456789abcdef01234567",
        )
        .is_err());
        let state: (String, Option<String>, i64, i64) = conn
            .query_row(
                "SELECT status,reviewer,
                        (SELECT count(*) FROM agent_runs),
                        (SELECT count(*) FROM run_capabilities
                         WHERE run_id='cap' AND revoked_at IS NULL)
                 FROM tasks WHERE id=5",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, ("in-review".into(), None, 0, 1));
    }

    #[test]
    fn cancellation_between_issue_and_post_spawn_persistence_wins_cleanly() {
        let mut conn = fixture();
        assert!(load_and_issue_capability(&mut conn, 3, "cap", "R", 10)
            .unwrap()
            .is_some());
        conn.execute(
            "UPDATE run_capabilities SET revoked_at=11 WHERE run_id='cap'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE task_decompositions SET state='cancelled',active=0 WHERE id=9",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE task_graph_members SET active=0 WHERE graph_id=9",
            [],
        )
        .unwrap();
        assert_eq!(
            persist_reviewer_run_if_current(
                &mut conn,
                3,
                "R",
                "model",
                "high",
                "claude",
                None,
                10,
                None,
                "cap",
                7,
                "0123456789abcdef0123456789abcdef01234567",
            )
            .unwrap(),
            None
        );
        let runs: i64 = conn
            .query_row("SELECT count(*) FROM agent_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(runs, 0);
    }

    #[test]
    fn cancellation_and_post_spawn_persistence_serialize_without_live_authority() {
        for iteration in 0..25 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("race-{iteration}.db"));
            let mut conn = crate::db::open(&path).unwrap();
            seed(&conn);
            load_and_issue_capability(&mut conn, 3, "cap", "R", 10).unwrap();
            drop(conn);

            let barrier = Arc::new(Barrier::new(2));
            let cancel_path = path.clone();
            let cancel_barrier = Arc::clone(&barrier);
            let cancel = std::thread::spawn(move || {
                let mut conn = crate::db::open(&cancel_path).unwrap();
                cancel_barrier.wait();
                let tx = begin_immediate(&mut conn).unwrap();
                tx.execute(
                    "UPDATE run_capabilities SET revoked_at=11 WHERE run_id='cap'",
                    [],
                )
                .unwrap();
                tx.execute(
                    "UPDATE task_decompositions SET state='cancelled',active=0 WHERE id=9",
                    [],
                )
                .unwrap();
                tx.execute(
                    "UPDATE task_graph_members SET active=0 WHERE graph_id=9",
                    [],
                )
                .unwrap();
                tx.commit().unwrap();
            });
            let persist_path = path.clone();
            let persist_barrier = Arc::clone(&barrier);
            let persist = std::thread::spawn(move || {
                let mut conn = crate::db::open(&persist_path).unwrap();
                persist_barrier.wait();
                persist_reviewer_run_if_current(
                    &mut conn,
                    3,
                    "R",
                    "model",
                    "high",
                    "claude",
                    None,
                    10,
                    None,
                    "cap",
                    7,
                    "0123456789abcdef0123456789abcdef01234567",
                )
                .unwrap()
            });
            cancel.join().unwrap();
            let _persisted = persist.join().unwrap();

            let conn = crate::db::open(&path).unwrap();
            let authority: (i64, i64, i64) = conn
                .query_row(
                    "SELECT d.active,m.active,c.revoked_at IS NULL
                     FROM task_decompositions d
                     JOIN task_graph_members m ON m.graph_id=d.id AND m.task_id=3
                     JOIN run_capabilities c ON c.run_id='cap' WHERE d.id=9",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(authority, (0, 0, 0));
            let live_runs: i64 = conn
                .query_row(
                    "SELECT count(*) FROM agent_runs r JOIN run_capabilities c
                     ON c.run_id=r.review_cap_run_id
                     WHERE r.ended_at IS NULL AND c.revoked_at IS NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(live_runs, 0);
        }
    }
}
