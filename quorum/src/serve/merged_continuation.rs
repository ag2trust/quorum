//! Bounded daemon reconciliation for merged continuation deliveries.
//!
//! Discovery consumes a fixed-size page of the durable lifecycle-event stream.
//! Its persisted monotonic cursor makes rejected deliveries unable to starve
//! later ones, while advancing only after the guarded core calls gives crash
//! replay at-least-once semantics. The evidence connection is dropped before
//! any adoption transaction is opened.

use std::path::{Path, PathBuf};

use quorum_core::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};

/// Page the append-only event sequence itself, rather than applying a limit
/// after a join over terminal task history. This bounds both rows inspected and
/// candidate applications on every startup/tick pass.
const EVENT_PAGE_LIMIT: i64 = 8;
const CURSOR_AGENT: &str = "quorum-serve";
const CURSOR_TOPIC: &str = "events:merged-continuation:v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    original_child_id: i64,
    recovery_task_id: i64,
    pr_number: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReconcileOutcome {
    pub(crate) scanned: usize,
    pub(crate) examined: usize,
    pub(crate) adopted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventPage {
    through_seq: Option<i64>,
    scanned: usize,
    candidates: Vec<Candidate>,
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
    // Finish the fixed event page and all persisted-evidence prefiltering,
    // finalize every statement, and close the connection before acquiring any
    // write authority below.
    let page = {
        let conn = quorum_core::db::open(db_path)?;
        select_event_page(&conn, now)?
    };

    let mut outcome = ReconcileOutcome {
        scanned: page.scanned,
        examined: page.candidates.len(),
        adopted: 0,
    };
    for candidate in page.candidates {
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

    // Ack only after every core application in the page succeeds. A crash or
    // error before this point replays the page; a crash after an adoption but
    // before this ack replays a clean core no-op. MAX preserves monotonicity if
    // a stale caller ever races a newer pass.
    if let Some(through_seq) = page.through_seq {
        let mut conn = quorum_core::db::open(db_path)?;
        advance_cursor(&mut conn, through_seq)?;
    }
    Ok(outcome)
}

/// Read at most [`EVENT_PAGE_LIMIT`] physical rows in sequence order. Filtering
/// happens after that limit, so a large terminal-task history cannot turn one
/// daemon pass into an unbounded scan. `task_done` is the terminal durable
/// lifecycle record: all publication, review, merge, and PR-target evidence for
/// a legitimate managed delivery precedes it.
fn select_event_page(conn: &Connection, now: i64) -> Result<EventPage> {
    let cursor = conn
        .query_row(
            "SELECT last_seq FROM cursors WHERE agent_id=?1 AND topic=?2",
            params![CURSOR_AGENT, CURSOR_TOPIC],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let events = {
        let mut stmt = conn.prepare(
            "SELECT seq,kind,subject,expires_at FROM events
             WHERE seq>?1 ORDER BY seq ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cursor, EVENT_PAGE_LIMIT], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let through_seq = events.last().map(|event| event.0);
    let mut candidates = Vec::new();
    for (_, kind, subject, expires_at) in &events {
        if kind != "task_done" || *expires_at <= now {
            continue;
        }
        let Some(recovery_task_id) = subject
            .strip_prefix("task#")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|task_id| *task_id > 0)
        else {
            continue;
        };
        if let Some(candidate) = select_candidate(conn, recovery_task_id, now)? {
            candidates.push(candidate);
        }
    }

    Ok(EventPage {
        through_seq,
        scanned: events.len(),
        candidates,
    })
}

/// Resolve one event-identified delivery using persisted graph, PR-target,
/// publication, and merge evidence. This is deliberately a prefilter rather
/// than lifecycle authority: `adopt_recovery_delivery` revalidates every core
/// invariant under its own `BEGIN IMMEDIATE` transaction.
fn select_candidate(
    conn: &Connection,
    recovery_task_id: i64,
    now: i64,
) -> Result<Option<Candidate>> {
    conn.query_row(
        "SELECT original.id,recovery.id,recovery_target.pr_number
         FROM tasks recovery
         JOIN pr_targets recovery_target ON recovery_target.task_id=recovery.id
         JOIN tasks original
           ON original.id=json_extract(recovery.refs,'$.source_task')
         JOIN pr_targets original_target
           ON original_target.task_id=original.id
          AND original_target.pr_number=recovery_target.pr_number
         JOIN task_graph_members member ON member.task_id=original.id
         JOIN task_decompositions graph ON graph.id=member.graph_id
         JOIN tasks source ON source.id=graph.source_task_id
         WHERE recovery.id=?1
           AND recovery.status='done'
           AND recovery.review_only=0
           AND recovery.continue_pr=recovery_target.pr_number
           AND json_valid(recovery.refs)
           AND json_type(recovery.refs,'$.source_task')='integer'
           AND original.status='failed'
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
                 AND published.expires_at>?2
           )
           AND EXISTS (
               SELECT 1 FROM events merging
               JOIN events done ON done.subject=merging.subject
                               AND done.kind='task_done'
                               AND done.expires_at>?2
                               AND done.seq>merging.seq
               WHERE merging.subject='task#' || recovery.id
                 AND merging.kind='task_merging'
                 AND merging.expires_at>?2
           )",
        params![recovery_task_id, now],
        |row| {
            Ok(Candidate {
                original_child_id: row.get(0)?,
                recovery_task_id: row.get(1)?,
                pr_number: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn advance_cursor(conn: &mut Connection, through_seq: i64) -> Result<()> {
    let tx = quorum_core::db::begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO cursors(agent_id,topic,last_seq) VALUES (?1,?2,?3)
         ON CONFLICT(agent_id,topic)
         DO UPDATE SET last_seq=MAX(last_seq,excluded.last_seq)",
        params![CURSOR_AGENT, CURSOR_TOPIC, through_seq],
    )?;
    tx.commit()?;
    Ok(())
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

        fn cursor(&self) -> Option<i64> {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .query_row(
                    "SELECT last_seq FROM cursors WHERE agent_id=?1 AND topic=?2",
                    params![CURSOR_AGENT, CURSOR_TOPIC],
                    |row| row.get(0),
                )
                .optional()
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
        let outcome = super::super::reconcile_merged_continuations(
            &fixture.db_path,
            super::super::MergedContinuationTrigger::Startup,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome {
                scanned: 3,
                examined: 1,
                adopted: 1
            }
        );
        assert_incident_released(&fixture);
    }

    #[tokio::test]
    async fn normal_tick_reconciles_incident_shaped_state() {
        let fixture = IncidentFixture::new();
        let outcome = super::super::reconcile_merged_continuations(
            &fixture.db_path,
            super::super::MergedContinuationTrigger::Tick,
        )
        .await
        .unwrap();
        assert_eq!(outcome.adopted, 1);
        assert_incident_released(&fixture);
    }

    #[tokio::test]
    async fn production_wiring_is_startup_fail_open_and_tick_error_propagating() {
        let directory_instead_of_database = tempfile::tempdir().unwrap();
        let startup = super::super::reconcile_merged_continuations(
            directory_instead_of_database.path(),
            super::super::MergedContinuationTrigger::Startup,
        )
        .await
        .unwrap();
        assert_eq!(startup, ReconcileOutcome::default());

        let tick = super::super::reconcile_merged_continuations(
            directory_instead_of_database.path(),
            super::super::MergedContinuationTrigger::Tick,
        )
        .await;
        assert!(tick.is_err());
    }

    #[test]
    fn event_page_driver_uses_sequence_primary_key_without_sorting_history() {
        let fixture = IncidentFixture::new();
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        let mut stmt = conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT seq,kind,subject,expires_at FROM events
                 WHERE seq>?1 ORDER BY seq ASC LIMIT ?2",
            )
            .unwrap();
        let details = stmt
            .query_map(params![0, EVENT_PAGE_LIMIT], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("INTEGER PRIMARY KEY") && detail.contains("rowid>?")),
            "event page must seek the monotonic sequence: {details:?}"
        );
        assert!(
            details.iter().all(|detail| !detail.contains("TEMP B-TREE")),
            "event page must not sort task/event history: {details:?}"
        );
    }

    #[tokio::test]
    async fn event_pages_are_bounded_deterministic_and_rejected_pairs_cannot_starve_valid_later_delivery(
    ) {
        let fixture = IncidentFixture::new();
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        conn.execute("DELETE FROM events WHERE subject='task#320'", [])
            .unwrap();
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
        conn.execute(
            "INSERT INTO events(ts,kind,subject,body,expires_at)
             VALUES (15,'task_in_review','task#320','by worker',?1),
                    (30,'task_merging','task#320','by reviewer',?1),
                    (40,'task_done','task#320','by system',?1)",
            [LIVE_UNTIL],
        )
        .unwrap();

        let first = select_event_page(&conn, NOW).unwrap();
        let second = select_event_page(&conn, NOW).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.scanned, EVENT_PAGE_LIMIT as usize);
        assert_eq!(
            first
                .candidates
                .iter()
                .map(|candidate| candidate.recovery_task_id)
                .collect::<Vec<_>>(),
            vec![321, 322]
        );
        drop(conn);

        let mut previous_cursor = 0;
        let mut adopted = false;
        for _ in 0..8 {
            let outcome = reconcile(fixture.db_path.clone(), NOW).await.unwrap();
            assert!(outcome.scanned <= EVENT_PAGE_LIMIT as usize);
            let cursor = fixture.cursor().unwrap();
            assert!(cursor > previous_cursor, "each nonempty page must advance");
            previous_cursor = cursor;
            if outcome.adopted == 1 {
                adopted = true;
                break;
            }
        }
        assert!(
            adopted,
            "nine durable rejected pairs must not hide the later valid delivery"
        );
        assert_incident_released(&fixture);
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
                scanned: 6,
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
        let page = {
            let conn = quorum_core::db::open(&fixture.db_path).unwrap();
            select_event_page(&conn, NOW).unwrap()
        };
        assert_eq!(page.candidates.len(), 1);
        assert_eq!(fixture.cursor(), None);
        assert_eq!(fixture.status(307), "failed");

        let first_restart = reconcile(fixture.db_path.clone(), NOW).await.unwrap();
        assert_eq!(first_restart.adopted, 1);
        assert_eq!(fixture.cursor(), page.through_seq);

        // Model a crash after the core commit but before the page ack by
        // rewinding only this daemon-owned cursor. The restarted page is a
        // clean no-op because #345 committed child and graph completion
        // together, then it is acknowledged again.
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        conn.execute(
            "UPDATE cursors SET last_seq=0 WHERE agent_id=?1 AND topic=?2",
            params![CURSOR_AGENT, CURSOR_TOPIC],
        )
        .unwrap();
        drop(conn);
        let second_restart = reconcile(fixture.db_path.clone(), NOW + 1).await.unwrap();
        assert_eq!(second_restart.adopted, 0);
        assert!(second_restart.scanned >= 3);
        assert!(second_restart.scanned <= EVENT_PAGE_LIMIT as usize);
        assert!(fixture.cursor() >= page.through_seq);
        assert_eq!(fixture.status(307), "done");
        assert_eq!(fixture.status(299), "done");
    }

    #[test]
    fn candidate_evidence_read_is_autocommit_and_releases_connection_before_apply() {
        let fixture = IncidentFixture::new();
        let page = {
            let conn = quorum_core::db::open(&fixture.db_path).unwrap();
            assert!(conn.is_autocommit());
            let page = select_event_page(&conn, NOW).unwrap();
            assert!(conn.is_autocommit());
            page
        };
        assert_eq!(page.candidates.len(), 1);

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
