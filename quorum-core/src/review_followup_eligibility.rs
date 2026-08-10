//! Short, read-only eligibility checks for dormant review follow-up assessment.
//!
//! These queries classify existing task/decomposition state only. They do not
//! create assessment aggregates, materialize memberships, or schedule work.

use crate::error::Result;
use rusqlite::Connection;

/// Whether `task_id` is an ordinary task whose durable state records a merged
/// delivery.
///
/// An ordinary task is neither a decomposition source nor a generated graph
/// member. Its `done` status must retain both the positive PR association and
/// the daemon-owned merged provenance written by an authoritative completion
/// path. The single bounded query uses primary/unique indexes and opens no
/// transaction.
pub fn ordinary_task_done_through_merge(conn: &Connection, task_id: i64) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM tasks task
             WHERE task.id=?1
               AND task.status='done'
               AND task.completion_provenance='merged'
               AND json_valid(task.refs)
               AND json_type(task.refs,'$.pr')='integer'
               AND json_extract(task.refs,'$.pr')>0
               AND NOT EXISTS(
                   SELECT 1 FROM task_decompositions decomposition
                   WHERE decomposition.source_task_id=task.id
               )
               AND NOT EXISTS(
                   SELECT 1 FROM task_graph_members member
                   WHERE member.task_id=task.id
               )
         )",
        [task_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Event;
    use rusqlite::params;
    use tempfile::TempDir;

    fn database() -> (TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("quorum.db")).unwrap();
        (dir, conn)
    }

    fn create_task(conn: &mut Connection, title: &str) -> i64 {
        crate::tasks::create(conn, "owner", title, None, 0, None, None, None, None, 100).unwrap()
    }

    fn complete_merge(conn: &mut Connection, task_id: i64, pr_number: i64) {
        conn.execute(
            "UPDATE tasks SET status='merging',refs=json_object('pr',?2) WHERE id=?1",
            params![task_id, pr_number],
        )
        .unwrap();
        crate::tasks::apply_event(conn, "daemon", task_id, &Event::MergeSucceeded, 101).unwrap();
    }

    fn completion_provenance(conn: &Connection, task_id: i64) -> Option<String> {
        conn.query_row(
            "SELECT completion_provenance FROM tasks WHERE id=?1",
            [task_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn ordinary_done_through_merge_is_positive_without_side_effects() {
        let (_dir, mut conn) = database();
        let task_id = create_task(&mut conn, "ordinary merged delivery");
        complete_merge(&mut conn, task_id, 41);
        let changes_before: i64 = conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();

        assert!(conn.is_autocommit());
        assert!(ordinary_task_done_through_merge(&conn, task_id).unwrap());
        assert!(
            conn.is_autocommit(),
            "eligibility read must not open a transaction"
        );
        let changes_after: i64 = conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            changes_after, changes_before,
            "eligibility read must not write"
        );
        let assessment_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM review_followup_assessments",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(assessment_count, 0);
    }

    #[test]
    fn every_authoritative_merge_completion_path_is_eligible() {
        let (_dir, mut conn) = database();

        let daemon_confirmed = create_task(&mut conn, "daemon-confirmed merge");
        complete_merge(&mut conn, daemon_confirmed, 41);

        let externally_observed = create_task(&mut conn, "externally observed merge");
        conn.execute(
            "UPDATE tasks SET status='in-review',refs=json_object('pr',42) WHERE id=?1",
            [externally_observed],
        )
        .unwrap();
        crate::tasks::apply_event(
            &mut conn,
            "daemon",
            externally_observed,
            &Event::PrFoundMerged,
            102,
        )
        .unwrap();

        let merge_recovery = create_task(&mut conn, "merge recovery");
        conn.execute(
            "UPDATE tasks SET refs=json_object('pr',43) WHERE id=?1",
            [merge_recovery],
        )
        .unwrap();
        assert!(crate::tasks::close_after_merge(
            &mut conn,
            merge_recovery,
            "daemon recovered authoritative merge",
            103,
        )
        .unwrap());

        for task_id in [daemon_confirmed, externally_observed, merge_recovery] {
            assert_eq!(
                completion_provenance(&conn, task_id).as_deref(),
                Some(crate::tasks::COMPLETION_PROVENANCE_MERGED)
            );
            assert!(ordinary_task_done_through_merge(&conn, task_id).unwrap());
        }
    }

    #[test]
    fn manual_close_retains_pr_but_is_ineligible() {
        let (_dir, mut conn) = database();
        let task_id = create_task(&mut conn, "manual close with retained PR");
        conn.execute(
            "UPDATE tasks SET refs=json_object('pr',51) WHERE id=?1",
            [task_id],
        )
        .unwrap();

        crate::tasks::close_manual(
            &mut conn,
            "owner",
            task_id,
            "resolved outside the managed merge lifecycle",
            102,
        )
        .unwrap()
        .unwrap();

        let (status, pr, provenance): (String, i64, Option<String>) = conn
            .query_row(
                "SELECT status,json_extract(refs,'$.pr'),completion_provenance
                 FROM tasks WHERE id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "done");
        assert_eq!(pr, 51, "task-close must retain the existing PR reference");
        assert_eq!(
            provenance.as_deref(),
            Some(crate::tasks::COMPLETION_PROVENANCE_MANUAL)
        );
        assert!(!ordinary_task_done_through_merge(&conn, task_id).unwrap());
    }

    #[test]
    fn non_merged_and_non_ordinary_tasks_are_negative() {
        let (_dir, mut conn) = database();

        let open_with_pr = create_task(&mut conn, "not done");
        conn.execute(
            "UPDATE tasks SET refs=json_object('pr',51) WHERE id=?1",
            [open_with_pr],
        )
        .unwrap();

        let legacy_done_with_pr = create_task(&mut conn, "legacy unknown completion");
        conn.execute(
            "UPDATE tasks SET status='done',refs=json_object('pr',50) WHERE id=?1",
            [legacy_done_with_pr],
        )
        .unwrap();

        let source = create_task(&mut conn, "decomposition source");
        complete_merge(&mut conn, source, 52);
        conn.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 created_at,updated_at)
             VALUES (?1,'completed',0,0,1,100,101)",
            [source],
        )
        .unwrap();

        let graph_source = create_task(&mut conn, "graph source");
        let child = create_task(&mut conn, "generated child");
        complete_merge(&mut conn, child, 53);
        conn.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 accepted_plan_revision,created_at,updated_at)
             VALUES (?1,'cancelled',0,0,1,1,100,101)",
            [graph_source],
        )
        .unwrap();
        let graph_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (?1,?2,'child',1,0)",
            params![graph_id, child],
        )
        .unwrap();

        assert_eq!(completion_provenance(&conn, legacy_done_with_pr), None);
        for task_id in [open_with_pr, legacy_done_with_pr, source, child, i64::MAX] {
            assert!(
                !ordinary_task_done_through_merge(&conn, task_id).unwrap(),
                "task #{task_id} must not satisfy ordinary merged eligibility"
            );
        }
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM review_followup_assessments",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0,
            "eligibility negatives must not create assessment work"
        );
    }
}
