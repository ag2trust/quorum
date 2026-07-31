//! Task classifier — authoritative complexity, execution size, readiness, and
//! duplicate hints.  A complete classification gates worker dispatch.

use crate::complexity;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Per-task classification output from the classifier agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClassification {
    pub task_id: i64,
    #[serde(rename = "complexity", alias = "cx_est")]
    pub cx_est: i64,
    pub size: String,
    pub ready: bool,
    pub not_ready_reason: Option<String>,
    #[serde(default, alias = "cx_dup_of")]
    pub duplicate_of: Vec<i64>,
}

/// Batch response from the classifier agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierResponse {
    pub tasks: Vec<TaskClassification>,
}

/// Minimal task info needed for classification input.
#[derive(Debug, Clone, Serialize)]
pub struct TaskForClassification {
    pub id: i64,
    pub title: String,
    pub body: Option<String>,
    pub dependencies: Vec<String>,
    pub recovery_notes: Vec<String>,
}

const VALID_SIZES: &[&str] = &["S", "M", "L", "XL"];
pub const CLASSIFICATION_BATCH_LIMIT: usize = 20;
pub const DUP_CONTEXT_LIMIT: usize = 60;
const TITLE_CHAR_LIMIT: usize = 300;
const BODY_CHAR_LIMIT: usize = 2_000;
const DEPENDENCY_TITLE_CHAR_LIMIT: usize = 240;
const DEPENDENCY_LIMIT: usize = 8;
const RECOVERY_NOTE_CHAR_LIMIT: usize = 600;
const RECOVERY_NOTE_LIMIT: usize = 4;
const DUP_BODY_CHAR_LIMIT: usize = 200;

/// SQL counterpart of [`crate::tasks::classification_is_complete`]. Keep this
/// predicate strict: malformed and partial v2 refs must remain classifier
/// candidates instead of becoming permanently undispatchable queue entries.
const INCOMPLETE_CLASSIFICATION_PREDICATE: &str = r#"
    CASE WHEN refs IS NULL OR NOT json_valid(refs) THEN 1
    ELSE NOT COALESCE((
        json_type(refs, '$.cx_est') = 'integer'
        AND json_extract(refs, '$.cx_est') BETWEEN 1 AND 5
        AND json_type(refs, '$.cx_size') = 'text'
        AND json_extract(refs, '$.cx_size') IN ('S', 'M', 'L', 'XL')
        AND json_type(refs, '$.cx_ready') IN ('true', 'false')
        AND (
            (json_type(refs, '$.cx_ready') = 'true'
             AND json_type(refs, '$.cx_not_ready_reason') = 'null')
            OR
            (json_type(refs, '$.cx_ready') = 'false'
             AND json_type(refs, '$.cx_not_ready_reason') = 'text'
             AND length(trim(json_extract(refs, '$.cx_not_ready_reason'))) > 0)
        )
    ), 0)
    END
"#;

/// Query active tasks and policy-parked tasks whose v2 classification is
/// incomplete. Malformed legacy refs are candidates rather than a query error.
pub fn unclassified_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let query = format!(
        "SELECT id, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
         WHERE (status IN ('open', 'working', 'in-review', 'rework', 'merging')
                OR CASE WHEN status='failed' AND json_valid(refs)
                        THEN json_extract(refs, '$.classifier_policy_parked')=1
                        ELSE 0 END)
         AND {INCOMPLETE_CLASSIFICATION_PREDICATE}
         ORDER BY id
         LIMIT ?3"
    );
    let mut stmt = conn.prepare(&query)?;
    let tasks = stmt
        .query_map(
            params![
                TITLE_CHAR_LIMIT as i64,
                BODY_CHAR_LIMIT as i64,
                CLASSIFICATION_BATCH_LIMIT as i64
            ],
            |row| {
                Ok(TaskForClassification {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    dependencies: vec![],
                    recovery_notes: vec![],
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    tasks
        .into_iter()
        .map(|task| enrich_task(conn, task))
        .collect()
}

/// Add only bounded coordination context.  This deliberately reads task rows and
/// durable notes, never repository contents or external state.
fn enrich_task(
    conn: &Connection,
    mut task: TaskForClassification,
) -> Result<TaskForClassification> {
    let deps: Option<String> = conn.query_row(
        "SELECT depends_on FROM tasks WHERE id=?1",
        params![task.id],
        |r| r.get(0),
    )?;
    if let Some(deps) = deps {
        let mut stmt = conn.prepare(
            "SELECT id, substr(title, 1, ?2), status FROM tasks
             WHERE id IN (SELECT value FROM json_each(?1))
             ORDER BY id LIMIT ?3",
        )?;
        task.dependencies = stmt
            .query_map(
                params![
                    deps,
                    DEPENDENCY_TITLE_CHAR_LIMIT as i64,
                    DEPENDENCY_LIMIT as i64
                ],
                |r| {
                    Ok(format!(
                        "#{} {} ({})",
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?
                    ))
                },
            )?
            .collect::<rusqlite::Result<_>>()?;
    }
    let mut stmt = conn.prepare(
        "SELECT substr(body, 1, ?2) FROM task_notes
         WHERE task_id=?1 ORDER BY id DESC LIMIT ?3",
    )?;
    task.recovery_notes = stmt
        .query_map(
            params![
                task.id,
                RECOVERY_NOTE_CHAR_LIMIT as i64,
                RECOVERY_NOTE_LIMIT as i64
            ],
            |r| r.get::<_, String>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(task)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    format!("{}…", s.chars().take(max).collect::<String>())
}

/// Check whether a specific task lacks cx_est in refs.
pub fn task_missing_cx(conn: &Connection, task_id: i64) -> Result<Option<TaskForClassification>> {
    let query = format!(
        "SELECT id, substr(title, 1, ?2), substr(body, 1, ?3) FROM tasks
         WHERE id = ?1
         AND {INCOMPLETE_CLASSIFICATION_PREDICATE}"
    );
    conn.query_row(
        &query,
        params![task_id, TITLE_CHAR_LIMIT as i64, BODY_CHAR_LIMIT as i64],
        |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                title: row.get(1)?,
                body: row.get(2)?,
                dependencies: vec![],
                recovery_notes: vec![],
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// All open/working tasks (for dup-detection context).
pub fn dup_context_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let mut stmt = conn.prepare(
        "SELECT id, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
         WHERE status IN ('open', 'working')
         ORDER BY id
         LIMIT ?3",
    )?;
    let tasks = stmt
        .query_map(
            params![
                TITLE_CHAR_LIMIT as i64,
                DUP_BODY_CHAR_LIMIT as i64,
                DUP_CONTEXT_LIMIT as i64
            ],
            |row| {
                Ok(TaskForClassification {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    dependencies: vec![],
                    recovery_notes: vec![],
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tasks)
}

/// Query one stable, bounded page of tasks in any status whose v2
/// classification is incomplete or malformed — for `--backfill`.
pub fn tasks_missing_cx_all(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let query = format!(
        "SELECT id, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
         WHERE {INCOMPLETE_CLASSIFICATION_PREDICATE}
         ORDER BY id
         LIMIT ?3"
    );
    let mut stmt = conn.prepare(&query)?;
    let tasks = stmt
        .query_map(
            params![
                TITLE_CHAR_LIMIT as i64,
                BODY_CHAR_LIMIT as i64,
                CLASSIFICATION_BATCH_LIMIT as i64
            ],
            |row| {
                Ok(TaskForClassification {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    dependencies: vec![],
                    recovery_notes: vec![],
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    tasks
        .into_iter()
        .map(|task| enrich_task(conn, task))
        .collect()
}

/// Store classification results into task refs and add notes for flags/dups.
pub fn store_classifications(
    conn: &mut Connection,
    results: &[TaskClassification],
    classifier_provenance: &str,
    now: i64,
) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let mut stored = 0;

    for result in results {
        if !valid(result) {
            continue;
        }

        let current_refs: Option<String> = tx
            .query_row(
                "SELECT refs FROM tasks WHERE id = ?1",
                params![result.task_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        let sanitized = sanitize(result);
        let new_refs = merge_cx_into_refs(&current_refs, &sanitized, classifier_provenance);

        let n = tx.execute(
            "UPDATE tasks SET refs = ?1, updated_at = ?2 WHERE id = ?3",
            params![new_refs, now, result.task_id],
        )?;

        if n > 0 {
            stored += 1;

            if !sanitized.duplicate_of.is_empty() || !sanitized.ready {
                let note = build_classifier_note(&sanitized);
                tx.execute(
                    "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'classifier', ?3)",
                    params![result.task_id, now, note],
                )?;
            }
            if let Some(reason) = parking_reason(&sanitized) {
                crate::tasks::park_classified_task_tx(&tx, result.task_id, reason, now)?;
            } else {
                crate::tasks::restore_classified_task_tx(&tx, result.task_id, now)?;
            }
        }
    }

    tx.commit()?;
    Ok(stored)
}

fn sanitize(result: &TaskClassification) -> TaskClassification {
    TaskClassification {
        task_id: result.task_id,
        cx_est: result.cx_est.clamp(1, 5),
        size: result.size.clone(),
        ready: result.ready,
        not_ready_reason: result
            .not_ready_reason
            .as_ref()
            .map(|s| s.trim().to_string()),
        duplicate_of: result.duplicate_of.clone(),
    }
}

pub fn valid(result: &TaskClassification) -> bool {
    (1..=5).contains(&result.cx_est)
        && VALID_SIZES.contains(&result.size.as_str())
        && if result.ready {
            result.not_ready_reason.is_none()
        } else {
            result
                .not_ready_reason
                .as_ref()
                .is_some_and(|s| !s.trim().is_empty())
        }
}

/// Validate the semantic contract for one provider response before any result
/// is committed. Coverage must be exact so missing, duplicate, unexpected, or
/// malformed items enter provider backoff rather than being silently skipped.
pub fn validate_batch(
    results: &[TaskClassification],
    expected_task_ids: &[i64],
) -> std::result::Result<(), String> {
    if results.len() != expected_task_ids.len() {
        return Err(format!(
            "classifier returned {} tasks for {} requested tasks",
            results.len(),
            expected_task_ids.len()
        ));
    }
    let expected: HashSet<i64> = expected_task_ids.iter().copied().collect();
    if expected.len() != expected_task_ids.len() {
        return Err("classifier request contained duplicate task ids".into());
    }
    let mut seen = HashSet::with_capacity(results.len());
    for result in results {
        if !seen.insert(result.task_id) {
            return Err(format!(
                "classifier returned duplicate task #{}",
                result.task_id
            ));
        }
        if !expected.contains(&result.task_id) {
            return Err(format!(
                "classifier returned unexpected task #{}",
                result.task_id
            ));
        }
        if !valid(result) {
            return Err(format!(
                "classifier returned an invalid classification for task #{}",
                result.task_id
            ));
        }
    }
    Ok(())
}

fn parking_reason(result: &TaskClassification) -> Option<&str> {
    if !result.ready {
        return result.not_ready_reason.as_deref();
    }
    if result.size == "XL" {
        return Some(
            "execution size XL exceeds automatic dispatch policy; split or rescope into new tasks",
        );
    }
    if result.cx_est == 5 && result.size == "L" {
        return Some("complexity 5 with size L exceeds automatic dispatch policy; split or rescope into new tasks");
    }
    None
}

fn merge_cx_into_refs(
    existing: &Option<String>,
    result: &TaskClassification,
    version: &str,
) -> String {
    let mut obj = match existing.as_deref() {
        Some(s) => {
            serde_json::from_str::<serde_json::Value>(s).unwrap_or_else(|_| serde_json::json!({}))
        }
        None => serde_json::json!({}),
    };

    // Malformed candidates intentionally include valid JSON scalars/arrays.
    // They carry no preservable named refs, so normalize them to an object
    // before installing the authoritative classification fields.
    if !obj.is_object() {
        obj = serde_json::json!({});
    }
    let map = obj
        .as_object_mut()
        .expect("classifier refs normalized to an object");
    map.insert("cx_est".into(), serde_json::json!(result.cx_est));
    map.insert("cx_size".into(), serde_json::json!(result.size));
    map.insert("cx_ready".into(), serde_json::json!(result.ready));
    map.insert(
        "cx_not_ready_reason".into(),
        serde_json::json!(result.not_ready_reason),
    );
    map.insert("cx_by".into(), serde_json::json!(version));
    map.remove("cx_flags");
    map.remove("cx_tags");
    if !result.duplicate_of.is_empty() {
        map.insert("cx_dup_of".into(), serde_json::json!(result.duplicate_of));
    } else {
        map.remove("cx_dup_of");
    }

    obj.to_string()
}

fn build_classifier_note(result: &TaskClassification) -> String {
    let mut parts = Vec::new();

    if !result.ready {
        parts.push(format!(
            "not ready — {}",
            result
                .not_ready_reason
                .as_deref()
                .unwrap_or("missing reason")
        ));
    }

    if !result.duplicate_of.is_empty() {
        let ids: Vec<String> = result
            .duplicate_of
            .iter()
            .map(|id| format!("#{id}"))
            .collect();
        parts.push(format!("possible duplicate of {}", ids.join(", ")));
    }

    format!("classifier: {}", parts.join("; "))
}

/// Build the classifier prompt for a batch of tasks.
/// `dup_context` includes titles+snippets of other open/working tasks for dup detection.
pub fn build_prompt(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
) -> String {
    build_prompt_with_recommendations(
        tasks,
        dup_context,
        &complexity::recommendation_lines(complexity::RecommendationProvider::Claude),
    )
}

/// Build the classifier prompt with the active provider's routing guidance.
/// Recommendations describe Quorum's operational policy only; they do not
/// alter the classifier's required complexity-only response.
pub fn build_prompt_with_recommendations(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
    recommendations: &str,
) -> String {
    let mut prompt = classifier_rubric(recommendations);

    prompt.push_str("\n\n## Tasks to classify\n\n");
    for t in tasks.iter().take(CLASSIFICATION_BATCH_LIMIT) {
        prompt.push_str(&format!("### Task #{}\n", t.id));
        prompt.push_str(&format!(
            "**Title:** {}\n",
            truncate(&t.title, TITLE_CHAR_LIMIT)
        ));
        if let Some(body) = &t.body {
            let truncated = truncate(body, BODY_CHAR_LIMIT);
            prompt.push_str(&format!("**Body:**\n{truncated}\n"));
        }
        prompt.push('\n');
        if !t.dependencies.is_empty() {
            prompt.push_str(&format!(
                "**Dependencies:** {}\n",
                t.dependencies
                    .iter()
                    .take(DEPENDENCY_LIMIT)
                    .map(|dependency| truncate(dependency, DEPENDENCY_TITLE_CHAR_LIMIT + 32))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        if !t.recovery_notes.is_empty() {
            prompt.push_str(&format!(
                "**Recovery context:** {}\n",
                t.recovery_notes
                    .iter()
                    .take(RECOVERY_NOTE_LIMIT)
                    .map(|note| truncate(note, RECOVERY_NOTE_CHAR_LIMIT))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }
    }

    if !dup_context.is_empty() {
        prompt.push_str("## Other open/working tasks (for duplicate detection)\n\n");
        for t in dup_context.iter().take(DUP_CONTEXT_LIMIT) {
            let snippet = t
                .body
                .as_deref()
                .map(|body| truncate(body, DUP_BODY_CHAR_LIMIT))
                .unwrap_or_default();
            prompt.push_str(&format!(
                "- #{}: {} — {snippet}\n",
                t.id,
                truncate(&t.title, TITLE_CHAR_LIMIT)
            ));
        }
    }

    prompt.push_str("\n\nRespond with ONLY valid JSON, no markdown fences, no explanation.\n");

    prompt
}

/// Build the classifier rubric text from the shared complexity constants.
fn classifier_rubric(recommendations: &str) -> String {
    let rubric_lines = crate::complexity::rubric_lines();
    format!(
        r#"You are a task classifier for an AI agent coordination system. For each task, produce:

1. **complexity** (integer 1-5): difficulty of the hardest reasoning/implementation problem, independent of volume.
{rubric_lines}

The active daemon's operational routing policy for these levels is:
{recommendations}
This is not a cross-vendor benchmark and does not change the required output.
2. **size**: execution surface only: S focused/local; M bounded coherent work; L broad cross-component coherent delivery; XL compound work needing decomposition. Do not estimate human time.
3. **ready** (boolean): true unless the intended outcome cannot be determined without an unstated product decision or open-ended investigation. Normal repository inspection, finding files, tracing implementation, and bounded engineering judgment are expected. Never reject merely because files, implementation details, or full architecture context are absent. If false, provide a concrete **not_ready_reason**; if true, it must be null.
4. **duplicate_of** (optional array): only genuine duplicates among supplied active tasks.

You are closed-book: use only this prompt, do not inspect the repository, Git history, diffs, CI, or external systems.

Output format (JSON array wrapped in an object):
{{"tasks": [{{"task_id": 1, "complexity": 3, "size": "M", "ready": true, "not_ready_reason": null, "duplicate_of": []}}]}}"#
    )
}

/// Stable classifier provenance string for `cx_by`.
///
/// The model is part of the identifier so classification quality can be grouped
/// by the model that actually produced it. `v2` identifies the explicit
/// complexity/size/readiness contract.
pub fn classifier_provenance(model: &str) -> String {
    format!("{model}:v2")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classified(task_id: i64, cx_est: i64) -> TaskClassification {
        TaskClassification {
            task_id,
            cx_est,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: vec![],
        }
    }

    #[test]
    fn merge_cx_into_empty_refs() {
        let result = classified(1, 3);
        let refs = merge_cx_into_refs(&None, &result, "haiku-45:v1");
        let v: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(v["cx_est"], 3);
        assert_eq!(v["cx_by"], "haiku-45:v1");
        assert_eq!(v["cx_size"], "M");
        assert_eq!(v["cx_ready"], true);
    }

    #[test]
    fn merge_cx_preserves_existing_pr() {
        let existing = Some(r#"{"pr":42}"#.to_string());
        let result = classified(1, 2);
        let refs = merge_cx_into_refs(&existing, &result, "haiku-45:v1");
        let v: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(v["pr"], 42);
        assert_eq!(v["cx_est"], 2);
    }

    #[test]
    fn classifier_provenance_identifies_the_model() {
        assert_eq!(classifier_provenance("gpt-5.6-luna"), "gpt-5.6-luna:v2");
        assert_eq!(classifier_provenance("gpt-5.6-terra"), "gpt-5.6-terra:v2");
    }

    #[test]
    fn sanitize_trims_not_ready_reason() {
        let mut result = classified(1, 3);
        result.ready = false;
        result.not_ready_reason = Some("  outcome ambiguous  ".into());
        let clean = sanitize(&result);
        assert_eq!(clean.not_ready_reason.as_deref(), Some("outcome ambiguous"));
    }

    #[test]
    fn sanitize_clamps_cx_est() {
        let result = classified(1, 7);
        let clean = sanitize(&result);
        assert_eq!(clean.cx_est, 5);
    }

    #[test]
    fn build_classifier_note_readiness_and_dups() {
        let mut result = classified(1, 3);
        result.ready = false;
        result.not_ready_reason = Some("expected outcome missing".into());
        result.duplicate_of = vec![47, 48];
        let note = build_classifier_note(&result);
        assert!(note.contains("not ready"));
        assert!(note.contains("#47"));
        assert!(note.contains("#48"));
    }

    #[test]
    fn parse_classifier_response() {
        let json = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size":"M", "ready":true, "not_ready_reason":null, "duplicate_of":[]}]}"#;
        let resp: ClassifierResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tasks.len(), 1);
        assert_eq!(resp.tasks[0].cx_est, 3);
    }

    #[test]
    fn build_prompt_includes_tasks_and_context() {
        let tasks = vec![TaskForClassification {
            id: 1,
            title: "Fix bug".into(),
            body: Some("Fix the thing".into()),
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let ctx = vec![TaskForClassification {
            id: 2,
            title: "Other task".into(),
            body: Some("Do something".into()),
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let prompt = build_prompt(&tasks, &ctx);
        assert!(prompt.contains("Task #1"));
        assert!(prompt.contains("Fix bug"));
        assert!(prompt.contains("#2: Other task"));
    }

    #[test]
    fn provider_specific_prompt_contains_only_its_routing_ladder() {
        let prompt = build_prompt_with_recommendations(
            &[],
            &[],
            &crate::complexity::recommendation_lines(
                crate::complexity::RecommendationProvider::Codex,
            ),
        );
        assert!(prompt.contains("gpt-5.6-sol / high"));
        assert!(!prompt.contains("claude-opus-4-8"));
    }

    #[test]
    fn ready_without_dups_produces_no_note() {
        let result = classified(1, 3);
        assert!(result.ready && result.duplicate_of.is_empty());
    }

    #[test]
    fn invalid_readiness_contract_is_rejected() {
        let mut result = classified(1, 3);
        result.ready = false;
        assert!(!valid(&result));
    }

    fn open_tmp() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn create_task(conn: &mut rusqlite::Connection, title: &str, seq: i64) -> i64 {
        let now = 1_000_000 + seq;
        crate::tasks::create(
            conn,
            "test-agent",
            title,
            None,
            5,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap()
    }

    #[test]
    fn store_and_query_classification() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Test task", 1);

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(unclassified.len(), 1);
        assert_eq!(unclassified[0].id, task_id);

        let results = vec![classified(task_id, 3)];
        let stored = store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();
        assert_eq!(stored, 1);

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(unclassified.len(), 0);

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["cx_est"], 3);
        assert_eq!(refs["cx_by"], "haiku-45:v1");
        assert_eq!(refs["cx_size"], "M");

        let notes = crate::tasks::get_with_notes(&conn, task_id)
            .unwrap()
            .unwrap()
            .notes;
        assert_eq!(notes.len(), 0);
    }

    #[test]
    fn malformed_refs_remain_classifiable() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Legacy malformed refs", 1);
        conn.execute("UPDATE tasks SET refs='{' WHERE id=?1", params![task_id])
            .unwrap();

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(
            unclassified.iter().map(|task| task.id).collect::<Vec<_>>(),
            [task_id]
        );
    }

    #[test]
    fn non_object_refs_are_normalized_during_persistence() {
        for (seq, raw_refs) in [(1, "[]"), (2, "null")] {
            let (_dir, mut conn) = open_tmp();
            let task_id = create_task(&mut conn, "non-object refs", seq);
            conn.execute(
                "UPDATE tasks SET refs=?2 WHERE id=?1",
                params![task_id, raw_refs],
            )
            .unwrap();
            assert_eq!(unclassified_tasks(&conn).unwrap()[0].id, task_id);

            let stored =
                store_classifications(&mut conn, &[classified(task_id, 3)], "test:v2", 20).unwrap();
            assert_eq!(stored, 1);
            let refs = crate::tasks::get(&conn, task_id)
                .unwrap()
                .unwrap()
                .refs
                .unwrap();
            let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
            assert!(refs.is_object());
            assert_eq!(refs["cx_est"], 3);
            assert_eq!(refs["cx_size"], "M");
            assert_eq!(refs["cx_ready"], true);
        }
    }

    #[test]
    fn store_classifications_records_each_classifier_model() {
        let (_dir, mut conn) = open_tmp();
        let luna_task = create_task(&mut conn, "Luna task", 1);
        let terra_task = create_task(&mut conn, "Terra task", 2);
        let result = |task_id| vec![classified(task_id, 2)];

        store_classifications(
            &mut conn,
            &result(luna_task),
            &classifier_provenance("gpt-5.6-luna"),
            2_000_000,
        )
        .unwrap();
        store_classifications(
            &mut conn,
            &result(terra_task),
            &classifier_provenance("gpt-5.6-terra"),
            2_000_001,
        )
        .unwrap();

        let cx_by = |task_id| {
            let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
            let refs: serde_json::Value =
                serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
            refs["cx_by"].as_str().unwrap().to_string()
        };
        let luna_by = cx_by(luna_task);
        let terra_by = cx_by(terra_task);
        assert_eq!(luna_by, "gpt-5.6-luna:v2");
        assert_eq!(terra_by, "gpt-5.6-terra:v2");
        assert_ne!(luna_by, terra_by);
    }

    #[test]
    fn category_five_classification_atomically_parks_without_run_or_error() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Architectural task", 1);
        let mut parked = classified(task_id, 5);
        parked.size = "L".into();
        let results = vec![parked];

        assert_eq!(
            store_classifications(&mut conn, &results, "gpt-5.6-luna:v2", 2_000_000).unwrap(),
            1
        );

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(task.status, "failed");
        assert_eq!(task.assignee, None);
        assert_eq!(refs["cx_est"], 5);
        assert_eq!(refs["cx_by"], "gpt-5.6-luna:v2");
        assert_eq!(refs["daemon_parked"], true);
        assert_eq!(refs["daemon_resume_status"], "open");
        assert!(refs["daemon_parked_reason"]
            .as_str()
            .unwrap()
            .contains("complexity 5"));
        let active_claims: i64 = conn
            .query_row(
                "SELECT count(*) FROM claims WHERE target=?1 AND active=1",
                params![format!("task#{task_id}")],
                |row| row.get(0),
            )
            .unwrap();
        let runs: i64 = conn
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id=?1",
                params![task_id],
                |row| row.get(0),
            )
            .unwrap();
        let errors: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        let parked_events: i64 = conn
            .query_row(
                "SELECT count(*) FROM events WHERE kind='task_parked' AND subject=?1",
                params![format!("task#{task_id}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 0);
        assert_eq!(runs, 0);
        assert_eq!(errors, 0);
        assert_eq!(parked_events, 1);
        assert!(
            crate::tasks::retry_parked(&mut conn, task_id, "operator", true, 2_000_001)
                .unwrap()
                .is_none(),
            "unchanged category-5 task must not retry into a dispatch loop"
        );
    }

    #[test]
    fn store_preserves_existing_pr_ref() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Task with PR", 1);
        conn.execute(
            "UPDATE tasks SET refs = '{\"pr\":42}' WHERE id = ?1",
            params![task_id],
        )
        .unwrap();

        let results = vec![classified(task_id, 2)];
        store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 42);
        assert_eq!(refs["cx_est"], 2);
    }

    #[test]
    fn out_of_range_cx_est_rejected() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Task", 1);

        let results = vec![classified(task_id, 0)];
        let stored = store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();
        assert_eq!(stored, 0);

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(unclassified.len(), 1);
    }

    #[test]
    fn no_note_when_no_flags_or_dups() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Clean task", 1);

        let results = vec![classified(task_id, 2)];
        store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();

        let notes = crate::tasks::get_with_notes(&conn, task_id)
            .unwrap()
            .unwrap()
            .notes;
        assert_eq!(notes.len(), 0);
    }

    #[test]
    fn dup_hint_creates_note() {
        let (_dir, mut conn) = open_tmp();
        let t1 = create_task(&mut conn, "Task A", 1);
        let t2 = create_task(&mut conn, "Task B", 2);

        let mut dup = classified(t1, 3);
        dup.duplicate_of = vec![t2];
        let results = vec![dup];
        store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();

        let notes = crate::tasks::get_with_notes(&conn, t1)
            .unwrap()
            .unwrap()
            .notes;
        assert_eq!(notes.len(), 1);
        assert!(notes[0].body.contains(&format!("#{t2}")));
    }

    #[test]
    fn tasks_missing_cx_all_includes_all_statuses() {
        let (_dir, mut conn) = open_tmp();
        let t1 = create_task(&mut conn, "Open task", 1);
        let _t2 = create_task(&mut conn, "Done task", 2);

        // Classify t1 only
        let results = vec![classified(t1, 2)];
        store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();

        let missing = tasks_missing_cx_all(&conn).unwrap();
        assert_eq!(missing.len(), 1);
    }

    #[test]
    fn backfill_candidates_include_bounded_dependency_and_recovery_context() {
        let (_dir, mut conn) = open_tmp();
        let dependency_id = create_task(&mut conn, "dependency title", 1);
        let task_id = create_task(&mut conn, "backfill target", 2);
        conn.execute(
            "UPDATE tasks SET depends_on=?2 WHERE id=?1",
            params![task_id, serde_json::json!([dependency_id]).to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_notes(task_id, ts, agent, body)
             VALUES (?1, 10, 'daemon', 'bounded recovery evidence')",
            params![task_id],
        )
        .unwrap();

        let tasks = tasks_missing_cx_all(&conn).unwrap();
        let task = tasks.iter().find(|task| task.id == task_id).unwrap();
        assert_eq!(task.dependencies.len(), 1);
        assert!(task.dependencies[0].contains("dependency title"));
        assert_eq!(task.recovery_notes, vec!["bounded recovery evidence"]);
    }

    #[test]
    fn atomic_claim_gate_requires_complete_dispatchable_classification() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "gated", 1);
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 10)
                .unwrap()
                .is_none()
        );
        let mut allowed = classified(task_id, 5);
        allowed.size = "M".into();
        store_classifications(&mut conn, &[allowed], "test:v2", 11).unwrap();
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 12)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn retry_requests_reclassification_and_dispatchable_result_restores_status() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "rescope", 1);
        let mut parked = classified(task_id, 5);
        parked.size = "L".into();
        parked.duplicate_of = vec![99];
        store_classifications(&mut conn, &[parked], "test:v2", 10).unwrap();
        let parked_task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let parked_refs: serde_json::Value =
            serde_json::from_str(parked_task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(parked_refs["classifier_policy_parked"], true);
        assert!(
            crate::tasks::retry_parked(&mut conn, task_id, "owner", true, 11)
                .unwrap()
                .is_none()
        );
        let retry_refs = crate::tasks::get(&conn, task_id)
            .unwrap()
            .unwrap()
            .refs
            .unwrap();
        let retry_refs: serde_json::Value = serde_json::from_str(&retry_refs).unwrap();
        assert_eq!(retry_refs["classifier_policy_parked"], true);
        for key in [
            "cx_est",
            "cx_size",
            "cx_ready",
            "cx_not_ready_reason",
            "cx_by",
            "cx_dup_of",
        ] {
            assert!(
                retry_refs.get(key).is_none(),
                "retry left stale classifier field {key}"
            );
        }
        assert!(unclassified_tasks(&conn)
            .unwrap()
            .iter()
            .any(|t| t.id == task_id));
        store_classifications(&mut conn, &[classified(task_id, 5)], "test:v2", 12).unwrap();
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "open");
    }

    #[test]
    fn generic_daemon_park_is_not_restored_by_reclassification() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "generic recovery park", 1);
        crate::tasks::park(
            &mut conn,
            task_id,
            "provider recovery exhausted",
            "open",
            10,
        )
        .unwrap();

        store_classifications(&mut conn, &[classified(task_id, 3)], "test:v2", 11).unwrap();
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "failed");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["daemon_parked"], true);
        assert!(refs.get("classifier_policy_parked").is_none());
    }

    #[test]
    fn malformed_v2_refs_remain_candidates_for_live_and_backfill_queries() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "malformed v2", 1);
        conn.execute(
            "UPDATE tasks
             SET refs='{\"cx_est\":3,\"cx_size\":\"BAD\",\"cx_ready\":true,\"cx_not_ready_reason\":null}'
             WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        assert_eq!(unclassified_tasks(&conn).unwrap()[0].id, task_id);
        assert_eq!(tasks_missing_cx_all(&conn).unwrap()[0].id, task_id);
        let refs = crate::tasks::get(&conn, task_id).unwrap().unwrap().refs;
        assert!(!crate::tasks::classification_is_dispatchable(&refs));
    }

    #[test]
    fn malformed_higher_queue_entry_does_not_starve_valid_claim_candidate() {
        let (_dir, mut conn) = open_tmp();
        let malformed = create_task(&mut conn, "malformed first", 1);
        let valid_task = create_task(&mut conn, "valid second", 2);
        conn.execute(
            "UPDATE tasks
             SET refs='{\"cx_est\":3,\"cx_size\":\"BAD\",\"cx_ready\":true,\"cx_not_ready_reason\":null}'
             WHERE id=?1",
            params![malformed],
        )
        .unwrap();
        store_classifications(&mut conn, &[classified(valid_task, 3)], "test:v2", 20).unwrap();

        let claimed = crate::tasks::claim(&mut conn, "worker", None, &[], 60, 21)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, valid_task);
    }

    #[test]
    fn classifier_inputs_are_stably_bounded_and_truncated() {
        let (_dir, mut conn) = open_tmp();
        let total = DUP_CONTEXT_LIMIT + 5;
        for seq in 0..total {
            create_task(&mut conn, &format!("task {seq}"), seq as i64);
        }
        conn.execute(
            "UPDATE tasks SET title=?1, body=?2 WHERE id=(SELECT min(id) FROM tasks)",
            params![
                "T".repeat(TITLE_CHAR_LIMIT + 100),
                "B".repeat(BODY_CHAR_LIMIT + 100)
            ],
        )
        .unwrap();

        let batch = unclassified_tasks(&conn).unwrap();
        assert_eq!(batch.len(), CLASSIFICATION_BATCH_LIMIT);
        assert!(batch.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert!(batch[0].title.chars().count() <= TITLE_CHAR_LIMIT);
        assert!(batch[0].body.as_deref().unwrap().chars().count() <= BODY_CHAR_LIMIT);

        let dup_context = dup_context_tasks(&conn).unwrap();
        assert_eq!(dup_context.len(), DUP_CONTEXT_LIMIT);
        assert!(dup_context.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert!(dup_context[0].body.as_deref().unwrap().chars().count() <= DUP_BODY_CHAR_LIMIT);
    }

    #[test]
    fn validate_batch_rejects_missing_duplicate_unexpected_and_invalid_items() {
        assert!(validate_batch(&[classified(1, 3), classified(2, 3)], &[1, 2]).is_ok());
        assert!(validate_batch(&[classified(1, 3)], &[1, 2]).is_err());
        assert!(validate_batch(&[classified(1, 3), classified(1, 3)], &[1, 2]).is_err());
        assert!(validate_batch(&[classified(1, 3), classified(3, 3)], &[1, 2]).is_err());
        assert!(validate_batch(&[classified(1, 0)], &[1]).is_err());
    }

    #[test]
    fn classifier_prompt_contains_shared_rubric_descriptions() {
        let rubric = classifier_rubric(&complexity::recommendation_lines(
            complexity::RecommendationProvider::Claude,
        ));
        for (level, label, desc, _time) in &crate::complexity::RUBRIC {
            assert!(
                rubric.contains(&format!("{level}: {label}")) && rubric.contains(*desc),
                "classifier rubric missing level {level} ({label} — {desc})"
            );
        }
    }

    #[test]
    fn skill_file_contains_shared_rubric_descriptions() {
        let skill = include_str!("../../.claude/skills/quorum/SKILL.md");
        for (level, label, desc, _reserved) in &crate::complexity::RUBRIC {
            assert!(
                skill.contains(&format!("- {level}: {label} — {desc}")),
                "skill file missing canonical rubric line for level {level}"
            );
        }
        assert!(!skill.contains("min agent work"));
        assert!(!skill.contains("15-30 min"));
        assert!(!skill.contains("30-60 min"));
        assert!(!skill.contains("> 60 min"));
    }
}

#[cfg(test)]
mod redesigned_tests {
    use super::*;

    fn result(cx_est: i64, size: &str, ready: bool, reason: Option<&str>) -> TaskClassification {
        TaskClassification {
            task_id: 1,
            cx_est,
            size: size.into(),
            ready,
            not_ready_reason: reason.map(str::to_string),
            duplicate_of: vec![],
        }
    }

    #[test]
    fn readiness_reason_and_size_are_strict() {
        assert!(valid(&result(4, "M", true, None)));
        assert!(valid(&result(4, "M", false, Some("outcome is ambiguous"))));
        assert!(!valid(&result(4, "M", false, None)));
        assert!(!valid(&result(4, "bad", true, None)));
    }

    #[test]
    fn policy_allows_focused_complexity_five() {
        assert!(parking_reason(&result(5, "S", true, None)).is_none());
        assert!(parking_reason(&result(5, "M", true, None)).is_none());
        assert!(parking_reason(&result(5, "L", true, None)).is_some());
        assert!(parking_reason(&result(2, "XL", true, None)).is_some());
    }

    #[test]
    fn prompt_is_closed_book_and_permissive() {
        let p = classifier_rubric("");
        assert!(p.contains("closed-book"));
        assert!(p.contains("Never reject merely because files"));
    }
}
