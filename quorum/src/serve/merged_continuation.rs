//! Bounded daemon reconciliation for merged continuation deliveries.
//!
//! Candidate discovery uses one short read connection and only durable Quorum
//! records. The connection is dropped before the guarded core write is invoked
//! for each candidate, so no external lookup or long read can overlap an
//! adoption transaction.

use std::path::{Path, PathBuf};

use quorum_core::error::{QuorumError, Result};
use rusqlite::{params, Connection};

/// One active graph has at most eight generated children. Keep the pass to one
/// graph-sized batch so ordinary daemon work cannot be delayed by historical
/// recovery rows.
const RECONCILE_BATCH_LIMIT: i64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    original_child_id: i64,
    recovery_task_id: i64,
    pr_number: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReconcileOutcome {
    pub(crate) examined: usize,
    pub(crate) adopted: usize,
}

pub(crate) async fn startup(db_path: &Path) -> Result<ReconcileOutcome> {
    reconcile(db_path.to_path_buf(), super::now_unix()).await
}

pub(crate) async fn tick(db_path: &Path) -> Result<ReconcileOutcome> {
    reconcile(db_path.to_path_buf(), super::now_unix()).await
}

async fn reconcile(db_path: PathBuf, now: i64) -> Result<ReconcileOutcome> {
    tokio::task::spawn_blocking(move || reconcile_blocking(&db_path, now))
        .await
        .map_err(|error| {
            QuorumError::Io(format!(
                "merged-continuation reconciliation join failed: {error}"
            ))
        })?
}

fn reconcile_blocking(db_path: &Path, now: i64) -> Result<ReconcileOutcome> {
    // Finish the entire bounded evidence read, finalize its statement, and
    // close the connection before acquiring any write authority below.
    let candidates = {
        let conn = quorum_core::db::open(db_path)?;
        select_candidates(&conn, now)?
    };

    let mut outcome = ReconcileOutcome {
        examined: candidates.len(),
        adopted: 0,
    };
    for candidate in candidates {
        let mut conn = quorum_core::db::open(db_path)?;
        if quorum_core::decomposition::adopt_recovery_delivery(
            &mut conn,
            candidate.original_child_id,
            candidate.recovery_task_id,
            now,
        )? {
            outcome.adopted += 1;
            super::log(&format!(
                "merged-continuation: adopted task #{} from recovery task #{} on PR #{}",
                candidate.original_child_id, candidate.recovery_task_id, candidate.pr_number
            ));
        }
    }
    Ok(outcome)
}

/// Find only incident-shaped pairs using persisted graph, PR-target,
/// publication, and merge evidence. This is deliberately a prefilter rather
/// than lifecycle authority: `adopt_recovery_delivery` revalidates every core
/// invariant under its own `BEGIN IMMEDIATE` transaction.
fn select_candidates(conn: &Connection, now: i64) -> Result<Vec<Candidate>> {
    let mut stmt = conn.prepare(
        "SELECT original.id,recovery.id,recovery_target.pr_number
         FROM task_graph_members member
         JOIN task_decompositions graph ON graph.id=member.graph_id
         JOIN tasks source ON source.id=graph.source_task_id
         JOIN tasks original ON original.id=member.task_id
         JOIN pr_targets original_target ON original_target.task_id=original.id
         JOIN tasks recovery
           ON recovery.status='done'
          AND recovery.review_only=0
          AND recovery.continue_pr IS NOT NULL
          AND json_valid(recovery.refs)
          AND json_type(recovery.refs,'$.source_task')='integer'
          AND json_extract(recovery.refs,'$.source_task')=original.id
         JOIN pr_targets recovery_target
           ON recovery_target.task_id=recovery.id
          AND recovery_target.pr_number=recovery.continue_pr
          AND recovery_target.pr_number=original_target.pr_number
         WHERE original.status='failed'
           AND member.active=1
           AND member.plan_revision=graph.accepted_plan_revision
           AND graph.state='active' AND graph.active=1
           AND source.status='decomposed'
           AND json_type(recovery.refs,'$.pr')='integer'
           AND json_extract(recovery.refs,'$.pr')=recovery_target.pr_number
           AND EXISTS (
               SELECT 1 FROM events published
               WHERE published.subject='task#' || recovery.id
                 AND published.kind='task_in_review'
                 AND published.expires_at>?1
           )
           AND EXISTS (
               SELECT 1 FROM events merging
               JOIN events done ON done.subject=merging.subject
                               AND done.kind='task_done'
                               AND done.expires_at>?1
                               AND done.seq>merging.seq
               WHERE merging.subject='task#' || recovery.id
                 AND merging.kind='task_merging'
                 AND merging.expires_at>?1
           )
         ORDER BY original.id ASC,recovery.id ASC
         LIMIT ?2",
    )?;
    let candidates = stmt
        .query_map(params![now, RECONCILE_BATCH_LIMIT], |row| {
            Ok(Candidate {
                original_child_id: row.get(0)?,
                recovery_task_id: row.get(1)?,
                pr_number: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const RECOVERY_HEAD: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const PR: i64 = 526;
    const NOW: i64 = 50;
    const LIVE_UNTIL: i64 = 4_000_000_000;

    struct IncidentFixture {
        _dir: tempfile::TempDir,
        db_path: PathBuf,
    }

    impl IncidentFixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("merged-continuation.db");
            let conn = quorum_core::db::open(&db_path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs,depends_on)
                 VALUES
                   (299,'graph source','decomposed','owner',1,1,'{}',NULL),
                   (300,'released dependent','open','owner',1,1,
                    '{\"cx_est\":2,\"cx_size\":\"S\",\"cx_ready\":true,\"cx_not_ready_reason\":null,\"cx_by\":\"test:v2\"}',
                    '[299]'),
                   (304,'sibling one','done','owner',1,1,'{}',NULL),
                   (305,'sibling two','done','owner',1,1,'{}',NULL),
                   (306,'sibling three','done','owner',1,1,'{}',NULL),
                   (307,'failed child','failed','owner',1,1,
                    '{\"pr\":526,\"daemon_parked\":true,\"daemon_parked_reason\":\"publication failed\",\"daemon_resume_status\":\"rework\",\"daemon_publication\":{\"pr\":526}}',NULL),
                   (320,'merged continuation','done','owner',9,40,
                    '{\"pr\":526,\"source_task\":307}',NULL);

                 INSERT INTO task_decompositions(
                     id,source_task_id,state,active,freeze_active,planned_source_revision,
                     plan_revision,accepted_plan_revision,created_at,updated_at)
                 VALUES (4,299,'active',1,0,1,1,1,1,1);
                 INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
                 VALUES (4,304,'one',1,1),(4,305,'two',1,1),
                        (4,306,'three',1,1),(4,307,'failed',1,1);
                 INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (307,526,'daemon/original','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,8),
                        (320,526,'daemon/original','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',0,25);
                 UPDATE tasks SET continue_pr=526 WHERE id=320;

                 INSERT INTO role_assignments(
                     id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                     profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES
                   (700,'worker:task:320:revision:1',320,NULL,'worker',NULL,'M',
                    'worker','codex','codex','sol','high','worker','test',9),
                   (701,'reviewer:task:320:r1',320,526,'reviewer','r1','M',
                    'reviewer','codex','codex','sol','high','reviewer','test',21);
                 INSERT INTO agent_runs(
                     task_id,agent_name,role,model,effort,spawned_at,ended_at,end_reason,
                     sub_role,provider,review_cap_run_id,review_pr,review_head_sha,role_assignment_id)
                 VALUES
                   (320,'worker','worker','sol','high',10,20,'completed',NULL,'codex',NULL,NULL,NULL,700),
                   (320,'reviewer','reviewer','sol','high',21,40,'verdict:approved',NULL,'codex',
                    'review-cap',526,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',701);
                 INSERT INTO r2_sampling_decisions(pr_number,head_sha,task_id,required,created_at)
                 VALUES (526,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',320,0,29);
                 INSERT INTO events(ts,kind,subject,body,expires_at)
                 VALUES (15,'task_in_review','task#320','by worker',4000000000),
                        (30,'task_merging','task#320','by reviewer',4000000000),
                        (40,'task_done','task#320','by system',4000000000);",
            )
            .unwrap();
            Self { _dir: dir, db_path }
        }

        fn status(&self, task_id: i64) -> String {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .query_row("SELECT status FROM tasks WHERE id=?1", [task_id], |row| {
                    row.get(0)
                })
                .unwrap()
        }
    }

    fn assert_incident_released(fixture: &IncidentFixture) {
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        let graph: (String, i64) = conn
            .query_row(
                "SELECT state,active FROM task_decompositions WHERE id=4",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(fixture.status(307), "done", "#307 must adopt #320");
        assert_eq!(fixture.status(299), "done", "#299 aggregate must finish");
        assert_eq!(graph, ("completed".into(), 0));

        drop(conn);
        let mut conn = quorum_core::db::open(&fixture.db_path).unwrap();
        let released = quorum_core::tasks::claim(&mut conn, "next", Some(300), &[], 60, 60)
            .unwrap()
            .expect("#300 must be claimable after #299 completes");
        assert_eq!(released.id, 300);
    }

    #[tokio::test]
    async fn startup_reconciles_incident_307_and_releases_299_300() {
        let fixture = IncidentFixture::new();
        let outcome = startup(&fixture.db_path).await.unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome {
                examined: 1,
                adopted: 1
            }
        );
        assert_incident_released(&fixture);
    }

    #[tokio::test]
    async fn normal_tick_reconciles_incident_shaped_state() {
        let fixture = IncidentFixture::new();
        let outcome = tick(&fixture.db_path).await.unwrap();
        assert_eq!(outcome.adopted, 1);
        assert_incident_released(&fixture);
    }

    #[test]
    fn candidate_batch_is_bounded_and_deterministically_ordered() {
        let fixture = IncidentFixture::new();
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        for task_id in 321..330 {
            conn.execute(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs,continue_pr)
                 VALUES (?1,'extra recovery','done','owner',1,40,?2,526)",
                params![task_id, json!({"pr": PR, "source_task": 307}).to_string()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
                 VALUES (?1,526,'daemon/original',?2,0,25)",
                params![task_id, RECOVERY_HEAD],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO events(ts,kind,subject,body,expires_at)
                 VALUES (15,'task_in_review',?1,'by worker',?2),
                        (30,'task_merging',?1,'by reviewer',?2),
                        (40,'task_done',?1,'by system',?2)",
                params![format!("task#{task_id}"), LIVE_UNTIL],
            )
            .unwrap();
        }

        let first = select_candidates(&conn, NOW).unwrap();
        let second = select_candidates(&conn, NOW).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), RECONCILE_BATCH_LIMIT as usize);
        assert_eq!(
            first
                .iter()
                .map(|candidate| candidate.recovery_task_id)
                .collect::<Vec<_>>(),
            (320..328).collect::<Vec<_>>()
        );
        assert!(first
            .iter()
            .all(|candidate| candidate.original_child_id == 307));
    }

    #[tokio::test]
    async fn unrelated_and_incomplete_candidates_remain_unchanged() {
        let fixture = IncidentFixture::new();
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        conn.execute("UPDATE tasks SET refs='{\"pr\":526}' WHERE id=320", [])
            .unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs)
             VALUES (400,'unrelated failed','failed','owner',1,1,'{}'),
                    (401,'unrelated recovery','done','owner',1,1,
                     '{\"pr\":700,\"source_task\":400}'),
                    (321,'incomplete recovery','done','owner',1,1,
                     '{\"pr\":526,\"source_task\":307}');
             UPDATE tasks SET continue_pr=700 WHERE id=401;
             UPDATE tasks SET continue_pr=526 WHERE id=321;
             INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at)
             VALUES (401,700,'other','cccccccccccccccccccccccccccccccccccccccc',0,1),
                    (321,526,'daemon/original','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',0,1);
             INSERT INTO events(ts,kind,subject,body,expires_at)
             VALUES (15,'task_in_review','task#321','by worker',4000000000),
                    (30,'task_merging','task#321','by reviewer',4000000000),
                    (40,'task_done','task#321','by system',4000000000);",
        )
        .unwrap();
        drop(conn);

        let outcome = reconcile(fixture.db_path.clone(), NOW).await.unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome {
                examined: 1,
                adopted: 0
            },
            "the daemon prefilter should find the incomplete managed delivery, while the core guard refuses it"
        );
        assert_eq!(fixture.status(307), "failed");
        assert_eq!(fixture.status(400), "failed");
        assert_eq!(fixture.status(321), "done");
    }

    #[tokio::test]
    async fn restart_before_or_after_guarded_application_converges_idempotently() {
        let fixture = IncidentFixture::new();

        // A crash after the short read but before core application leaves no
        // cursor or mutation to repair; the restarted pass finds the same row.
        {
            let conn = quorum_core::db::open(&fixture.db_path).unwrap();
            assert_eq!(select_candidates(&conn, NOW).unwrap().len(), 1);
        }
        assert_eq!(fixture.status(307), "failed");

        let first_restart = reconcile(fixture.db_path.clone(), NOW).await.unwrap();
        assert_eq!(first_restart.adopted, 1);

        // A crash after commit replays as a clean no-op because #345 changed
        // the failed child and graph in the same transaction.
        let second_restart = reconcile(fixture.db_path.clone(), NOW + 1).await.unwrap();
        assert_eq!(second_restart, ReconcileOutcome::default());
        assert_eq!(fixture.status(307), "done");
        assert_eq!(fixture.status(299), "done");
    }

    #[test]
    fn candidate_evidence_read_is_autocommit_and_releases_connection_before_apply() {
        let fixture = IncidentFixture::new();
        let candidates = {
            let conn = quorum_core::db::open(&fixture.db_path).unwrap();
            assert!(conn.is_autocommit());
            let candidates = select_candidates(&conn, NOW).unwrap();
            assert!(conn.is_autocommit());
            candidates
        };
        assert_eq!(candidates.len(), 1);

        // The evidence connection is gone before a writer is acquired. The
        // production path has no network-capable dependency between these
        // phases and calls the guarded primitive only after this boundary.
        let mut writer = quorum_core::db::open(&fixture.db_path).unwrap();
        let tx = writer
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        tx.commit().unwrap();
    }
}
