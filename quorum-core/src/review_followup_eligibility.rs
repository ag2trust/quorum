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
/// member. Its `done` status must retain the positive PR association written
/// before the merge transition. The single bounded query uses primary/unique
/// indexes and opens no transaction.
pub fn ordinary_task_done_through_merge(conn: &Connection, task_id: i64) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM tasks task
             WHERE task.id=?1
               AND task.status='done'
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
    fn non_merged_and_non_ordinary_tasks_are_negative() {
        let (_dir, mut conn) = database();

        let open_with_pr = create_task(&mut conn, "not done");
        conn.execute(
            "UPDATE tasks SET refs=json_object('pr',51) WHERE id=?1",
            [open_with_pr],
        )
        .unwrap();

        let done_without_pr = create_task(&mut conn, "manual completion");
        conn.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            [done_without_pr],
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

        for task_id in [open_with_pr, done_without_pr, source, child, i64::MAX] {
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
