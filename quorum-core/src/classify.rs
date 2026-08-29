//! Task classifier — authoritative complexity, execution size, readiness, and
//! duplicate hints.  A complete classification gates worker dispatch.

use crate::complexity;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Per-task classification output from the classifier agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClassification {
    pub task_id: i64,
    #[serde(rename = "complexity", alias = "cx_est")]
    pub cx_est: i64,
    pub size: String,
    /// Bounded, artifact-specific rationale for the selected execution size.
    /// Required for every v3 verdict so a later planning iteration can correct
    /// the concrete breadth the classifier observed.
    pub size_reason: String,
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
    #[serde(skip)]
    pub revision: i64,
    pub title: String,
    pub body: Option<String>,
    pub dependencies: Vec<String>,
    pub recovery_notes: Vec<String>,
}

/// An internally-derived identity for the exact bounded task input given to a
/// classifier turn.  It deliberately does not come from the provider response:
/// a result is useful only if this identity still matches inside the later
/// persistence transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassificationInput {
    pub task_id: i64,
    revision: i64,
    fingerprint: String,
}

/// Snapshot classifier inputs before starting a provider turn.
pub fn classification_inputs(tasks: &[TaskForClassification]) -> Vec<ClassificationInput> {
    tasks
        .iter()
        .map(|task| ClassificationInput {
            task_id: task.id,
            revision: task.revision,
            // `TaskForClassification` is a struct (not a map), so serde emits
            // a stable field order.  Keep the entire bounded prompt input,
            // including stable dependency context and recovery notes, in the
            // identity rather than relying on generic `updated_at`.
            fingerprint: serde_json::to_string(task)
                .expect("TaskForClassification always serializes"),
        })
        .collect()
}

const VALID_SIZES: &[&str] = &["S", "M", "L", "XL"];
pub const MAX_SIZE_REASON_BYTES: usize = 1024;
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
             AND length(trim(json_extract(refs, '$.cx_not_ready_reason'))) > 0
             AND instr(json_extract(refs, '$.cx_not_ready_reason'), char(0)) = 0)
        )
    ), 0)
    END
"#;

/// Query active tasks and policy-parked tasks whose v2 classification is
/// incomplete. Malformed legacy refs are candidates rather than a query error.
pub fn unclassified_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let query = format!(
        "SELECT id, revision, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
         WHERE (status IN ('open', 'working', 'in-review', 'rework', 'merging')
                OR CASE WHEN status='failed' AND json_valid(refs)
                        THEN json_extract(refs, '$.classifier_policy_parked')=1
                        ELSE 0 END)
         AND {INCOMPLETE_CLASSIFICATION_PREDICATE}
         ORDER BY priority DESC, id
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
                    revision: row.get(1)?,
                    title: row.get(2)?,
                    body: row.get(3)?,
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
            "SELECT id, substr(title, 1, ?2) FROM tasks
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
                        "#{} {}",
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?
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
        "SELECT id, revision, substr(title, 1, ?2), substr(body, 1, ?3) FROM tasks
         WHERE id = ?1
         AND {INCOMPLETE_CLASSIFICATION_PREDICATE}"
    );
    conn.query_row(
        &query,
        params![task_id, TITLE_CHAR_LIMIT as i64, BODY_CHAR_LIMIT as i64],
        |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                revision: row.get(1)?,
                title: row.get(2)?,
                body: row.get(3)?,
                dependencies: vec![],
                recovery_notes: vec![],
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Read the exact bounded classifier input for one task, regardless of whether
/// it is currently eligible.  Persistence uses this inside its write
/// transaction to reject an input that changed while a provider turn ran.
fn classifier_input_for_task(
    conn: &Connection,
    task_id: i64,
) -> Result<Option<TaskForClassification>> {
    let task = conn
        .query_row(
            "SELECT id, revision, substr(title, 1, ?2), substr(body, 1, ?3) FROM tasks WHERE id=?1",
            params![task_id, TITLE_CHAR_LIMIT as i64, BODY_CHAR_LIMIT as i64],
            |row| {
                Ok(TaskForClassification {
                    id: row.get(0)?,
                    revision: row.get(1)?,
                    title: row.get(2)?,
                    body: row.get(3)?,
                    dependencies: vec![],
                    recovery_notes: vec![],
                })
            },
        )
        .optional()?;
    task.map(|task| enrich_task(conn, task)).transpose()
}

/// All open/working tasks (for dup-detection context).
pub fn dup_context_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let mut stmt = conn.prepare(
        "SELECT id, revision, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
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
                    revision: row.get(1)?,
                    title: row.get(2)?,
                    body: row.get(3)?,
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
        "SELECT id, revision, substr(title, 1, ?1), substr(body, 1, ?2) FROM tasks
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
                    revision: row.get(1)?,
                    title: row.get(2)?,
                    body: row.get(3)?,
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
    let task_ids: Vec<i64> = results.iter().map(|result| result.task_id).collect();
    let mut inputs = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        if let Some(input) = classifier_input_for_task(conn, task_id)? {
            inputs.push(input);
        }
    }
    let expected = classification_inputs(&inputs);
    store_classifications_for_inputs(conn, results, &expected, classifier_provenance, now)
}

/// Store provider output only for inputs that still match the snapshot taken
/// before the provider turn.  A stale output is an expected clean negative: it
/// is not persisted, does not park/dispatch the changed task, and leaves that
/// task eligible for the next classifier pass.  The incomplete-classification
/// guard also makes the first accepted concurrent attempt win, so an older
/// attempt cannot overwrite a newer accepted result.
pub fn store_classifications_for_inputs(
    conn: &mut Connection,
    results: &[TaskClassification],
    expected_inputs: &[ClassificationInput],
    classifier_provenance: &str,
    now: i64,
) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let stored = store_classifications_tx(
        &tx,
        results,
        expected_inputs,
        classifier_provenance,
        now,
        None,
    )?;
    tx.commit()?;
    Ok(stored)
}

/// Atomic variant used by the daemon: accepts each classification and stamps the
/// per-task rework ceiling in the *same* transaction. Classification acceptance is
/// the earliest per-task adoption point, so the immutable adoption-time cap must
/// land or roll back with the accepted refs — a crash between two separate
/// transactions would leave the task dispatchable at the compiled default despite
/// a configured `max_rework`.
pub fn store_classifications_and_stamp_rework_cap(
    conn: &mut Connection,
    results: &[TaskClassification],
    expected_inputs: &[ClassificationInput],
    classifier_provenance: &str,
    now: i64,
    rework_cap: u32,
) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let stored = store_classifications_tx(
        &tx,
        results,
        expected_inputs,
        classifier_provenance,
        now,
        Some(rework_cap),
    )?;
    tx.commit()?;
    Ok(stored)
}

fn store_classifications_tx(
    tx: &Transaction<'_>,
    results: &[TaskClassification],
    expected_inputs: &[ClassificationInput],
    classifier_provenance: &str,
    now: i64,
    rework_cap: Option<u32>,
) -> Result<usize> {
    let mut stored = 0;
    let expected: std::collections::HashMap<i64, (i64, &str)> = expected_inputs
        .iter()
        .map(|input| (input.task_id, (input.revision, input.fingerprint.as_str())))
        .collect();

    for result in results {
        if !valid(result) {
            continue;
        }

        let Some((expected_revision, expected_fingerprint)) = expected.get(&result.task_id) else {
            continue;
        };

        // Do not overwrite a complete classification written by another
        // concurrent attempt.  The predicate is deliberately evaluated in
        // this same immediate transaction as the eventual UPDATE.
        let eligibility_query =
            format!("SELECT refs FROM tasks WHERE id=?1 AND {INCOMPLETE_CLASSIFICATION_PREDICATE}");
        let current_refs: Option<Option<String>> = tx
            .query_row(&eligibility_query, params![result.task_id], |row| {
                row.get(0)
            })
            .optional()?;
        let Some(current_refs) = current_refs else {
            continue;
        };

        let Some(current_input) = classifier_input_for_task(tx, result.task_id)? else {
            continue;
        };
        if classification_inputs(std::slice::from_ref(&current_input))[0].fingerprint
            != *expected_fingerprint
        {
            continue;
        }

        let sanitized = sanitize(result);
        let new_refs = merge_cx_into_refs(&current_refs, &sanitized, classifier_provenance);

        let n = tx.execute(
            "UPDATE tasks SET refs = ?1, updated_at = ?2 WHERE id = ?3 AND revision = ?4",
            params![new_refs, now, result.task_id, expected_revision],
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
            let (review_only, continue_pr): (bool, Option<i64>) = tx.query_row(
                "SELECT review_only, continue_pr FROM tasks WHERE id=?1",
                params![result.task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if let Some(reason) = parking_reason(&sanitized, review_only, continue_pr.is_some()) {
                crate::tasks::park_classified_task_tx(tx, result.task_id, reason, now)?;
            } else {
                crate::tasks::restore_classified_task_tx(tx, result.task_id, now)?;
            }

            // Stamp the immutable adoption-time rework ceiling in the same
            // transaction as the accepted classification: both must land or
            // roll back together.
            if let Some(cap) = rework_cap {
                tx.execute(
                    "UPDATE tasks SET rework_cap = ?2, updated_at = ?3 \
                     WHERE id = ?1 AND rework_cap IS NULL",
                    params![result.task_id, i64::from(cap), now],
                )?;
            }
        }
    }

    Ok(stored)
}

fn sanitize(result: &TaskClassification) -> TaskClassification {
    TaskClassification {
        task_id: result.task_id,
        cx_est: result.cx_est.clamp(1, 5),
        size: result.size.clone(),
        size_reason: result.size_reason.trim().to_string(),
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
        && !result.size_reason.trim().is_empty()
        && result.size_reason.len() <= MAX_SIZE_REASON_BYTES
        && !result.size_reason.contains('\0')
        && if result.ready {
            result.not_ready_reason.is_none()
        } else {
            result
                .not_ready_reason
                .as_ref()
                .is_some_and(|s| !s.trim().is_empty() && !s.contains('\0'))
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

fn parking_reason(
    result: &TaskClassification,
    review_only: bool,
    continue_pr: bool,
) -> Option<&str> {
    if !result.ready {
        return result.not_ready_reason.as_deref();
    }
    if !review_only && !continue_pr && result.size == "XL" && result.cx_est <= 3 {
        return Some(crate::tasks::LOW_COMPLEXITY_XL_PARK_REASON);
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
    map.insert(
        "cx_size_reason".into(),
        serde_json::json!(result.size_reason),
    );
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
                "**Dependencies (scheduler-enforced assumptions):** {}\n",
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
3. **size_reason**: a required, concrete rationale of at most {MAX_SIZE_REASON_BYTES} UTF-8 bytes tied to this exact task artifact. Name the implementation surfaces or responsibilities that make the selected size fit better than the adjacent sizes; do not merely restate the rubric or task title. For L/XL, identify independently deliverable seams that make the artifact broad or compound. For S/M, identify the focused or bounded coherent seam. This rationale is durable review feedback for a later planning iteration.
4. **ready** (boolean): true unless the intended outcome cannot be determined without an unstated product decision or open-ended investigation. Normal repository inspection, finding files, tracing implementation, and bounded engineering judgment are expected. Never reject merely because files, implementation details, or full architecture context are absent. Declared dependencies are scheduler-enforced assumptions whose required outcomes will be satisfied before execution. Use their bounded context to understand assumed outcomes, scope, complexity, and duplication, but never return ready=false merely because a dependency is currently incomplete; dependency ordering is not classifier authority. If false, provide a concrete **not_ready_reason**; if true, it must be null.
5. **duplicate_of** (optional array): only genuine duplicates among supplied active tasks.

You are closed-book: use only this prompt, do not inspect the repository, Git history, diffs, CI, or external systems.

Output format (JSON array wrapped in an object):
{{"tasks": [{{"task_id": 1, "complexity": 3, "size": "M", "size_reason": "The change is one bounded storage seam with focused callers and tests.", "ready": true, "not_ready_reason": null, "duplicate_of": []}}]}}"#
    )
}

/// Stable classifier provenance string for `cx_by`.
///
/// The model is part of the identifier so classification quality can be grouped
/// by the model that actually produced it. `v3` adds the required bounded
/// rationale for the size verdict to the explicit classification contract.
pub fn classifier_provenance(model: &str) -> String {
    format!("{model}:v3")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classified(task_id: i64, cx_est: i64) -> TaskClassification {
        TaskClassification {
            task_id,
            cx_est,
            size: "M".into(),
            size_reason: "bounded test classification rationale".into(),
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
        assert_eq!(v["cx_size_reason"], "bounded test classification rationale");
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
        assert_eq!(classifier_provenance("gpt-5.6-luna"), "gpt-5.6-luna:v3");
        assert_eq!(classifier_provenance("gpt-5.6-terra"), "gpt-5.6-terra:v3");
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
    fn sanitize_trims_size_reason() {
        let mut result = classified(1, 3);
        result.size_reason = "  one bounded storage seam  ".into();
        let clean = sanitize(&result);
        assert_eq!(clean.size_reason, "one bounded storage seam");
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
        let json = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size":"M", "size_reason":"one bounded seam", "ready":true, "not_ready_reason":null, "duplicate_of":[]}]}"#;
        let resp: ClassifierResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tasks.len(), 1);
        assert_eq!(resp.tasks[0].cx_est, 3);
    }

    #[test]
    fn build_prompt_includes_tasks_and_context() {
        let tasks = vec![TaskForClassification {
            id: 1,
            revision: 1,
            title: "Fix bug".into(),
            body: Some("Fix the thing".into()),
            dependencies: vec!["#3 Establish prerequisite".into()],
            recovery_notes: vec![],
        }];
        let ctx = vec![TaskForClassification {
            id: 2,
            revision: 1,
            title: "Other task".into(),
            body: Some("Do something".into()),
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let prompt = build_prompt(&tasks, &ctx);
        assert!(prompt.contains("Task #1"));
        assert!(prompt.contains("Fix bug"));
        assert!(prompt.contains("Dependencies (scheduler-enforced assumptions)"));
        assert!(prompt.contains("#3 Establish prerequisite"));
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

    #[test]
    fn nul_readiness_reason_is_rejected_without_persistence() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "NUL classifier output", 1);
        let mut result = classified(task_id, 3);
        result.ready = false;
        result.not_ready_reason = Some("missing\0acceptance criteria".into());

        assert!(!valid(&result));
        assert!(validate_batch(std::slice::from_ref(&result), &[task_id]).is_err());
        assert_eq!(
            store_classifications(&mut conn, &[result], "test:v2", 2_000_000).unwrap(),
            0,
            "defensive persistence boundary must skip NUL-bearing output"
        );
        let task = crate::tasks::get_with_notes(&conn, task_id)
            .unwrap()
            .unwrap();
        assert!(task.task.refs.is_none());
        assert!(task.notes.is_empty());

        let legacy_refs = serde_json::json!({
            "cx_est": 3,
            "cx_size": "M",
            "cx_ready": false,
            "cx_not_ready_reason": "missing\0acceptance criteria",
            "cx_by": "legacy:v2"
        })
        .to_string();
        conn.execute(
            "UPDATE tasks SET refs=?2 WHERE id=?1",
            params![task_id, legacy_refs],
        )
        .unwrap();
        let refs = crate::tasks::get(&conn, task_id).unwrap().unwrap().refs;
        assert!(!crate::tasks::classification_is_complete(&refs));
        assert!(
            unclassified_tasks(&conn)
                .unwrap()
                .iter()
                .any(|task| task.id == task_id),
            "legacy NUL-bearing refs must be reclassified"
        );
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

    fn create_dependent_task(
        conn: &mut rusqlite::Connection,
        title: &str,
        body: &str,
        dependency: i64,
        seq: i64,
    ) -> i64 {
        let dependencies = serde_json::json!([dependency]).to_string();
        crate::tasks::create(
            conn,
            "test-agent",
            title,
            Some(body),
            5,
            None,
            None,
            Some(&dependencies),
            None,
            1_000_000 + seq,
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
    fn ready_classification_with_open_dependency_remains_dependency_gated() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create_task(&mut conn, "Establish stable prerequisite", 1);
        let task_id = create_dependent_task(
            &mut conn,
            "Implement the dependent behavior",
            "Observed: behavior is absent. Expected: add the specified behavior and focused regression coverage.",
            dependency,
            2,
        );

        assert_eq!(
            store_classifications(&mut conn, &[classified(task_id, 3)], "test:v2", 2_000_000,)
                .unwrap(),
            1
        );
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["cx_ready"], true);
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 2_000_001)
                .unwrap()
                .is_none(),
            "open dependency must remain the scheduling authority"
        );

        conn.execute(
            "UPDATE tasks SET status='done', updated_at=?2 WHERE id=?1",
            params![dependency, 2_000_001],
        )
        .unwrap();
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 2_000_002)
                .unwrap()
                .is_some(),
            "completed dependency must release the already-ready task"
        );
    }

    #[test]
    fn dependency_lifecycle_changes_do_not_stale_or_invalidate_classification() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create_task(&mut conn, "Produce prerequisite outcome", 1);
        let task_id = create_dependent_task(
            &mut conn,
            "Consume prerequisite outcome",
            "Use the prerequisite outcome to implement the fully specified consumer behavior.",
            dependency,
            2,
        );

        let before_task = unclassified_tasks(&conn)
            .unwrap()
            .into_iter()
            .find(|task| task.id == task_id)
            .unwrap();
        assert_eq!(
            before_task.dependencies,
            vec![format!("#{dependency} Produce prerequisite outcome")]
        );
        let before = classification_inputs(std::slice::from_ref(&before_task));

        conn.execute(
            "UPDATE tasks SET status='done', updated_at=?2 WHERE id=?1",
            params![dependency, 2_000_000],
        )
        .unwrap();
        let after_task = unclassified_tasks(&conn)
            .unwrap()
            .into_iter()
            .find(|task| task.id == task_id)
            .unwrap();
        let after = classification_inputs(std::slice::from_ref(&after_task));
        assert_eq!(before[0].fingerprint, after[0].fingerprint);
        assert_eq!(
            store_classifications_for_inputs(
                &mut conn,
                &[classified(task_id, 3)],
                &before,
                "test:v2",
                2_000_000,
            )
            .unwrap(),
            1,
            "dependency completion during a classifier turn must not stale its result"
        );

        conn.execute(
            "UPDATE tasks SET status='open', updated_at=?2 WHERE id=?1",
            params![dependency, 2_000_001],
        )
        .unwrap();
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert!(crate::tasks::classification_is_complete(&task.refs));
        assert!(!unclassified_tasks(&conn)
            .unwrap()
            .iter()
            .any(|task| task.id == task_id));
    }

    #[test]
    fn ambiguous_task_with_done_dependency_remains_not_ready() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create_task(&mut conn, "Finished prerequisite", 1);
        conn.execute(
            "UPDATE tasks SET status='done', updated_at=?2 WHERE id=?1",
            params![dependency, 2_000_000],
        )
        .unwrap();
        let task_id = create_dependent_task(
            &mut conn,
            "Change product behavior",
            "Change the behavior, but no desired outcome or decision is specified.",
            dependency,
            2,
        );
        let reason = "desired product behavior is not specified";
        let mut result = classified(task_id, 2);
        result.ready = false;
        result.not_ready_reason = Some(reason.into());

        assert_eq!(
            store_classifications(&mut conn, &[result], "test:v2", 2_000_000).unwrap(),
            1
        );
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["cx_ready"], false);
        assert_eq!(refs["cx_not_ready_reason"], reason);
        assert_eq!(task.status, "failed");
    }

    #[test]
    fn stale_body_edit_result_is_not_persisted_or_dispatchable() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "body race", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );

        crate::tasks::update(
            &mut conn,
            "test-agent",
            task_id,
            &crate::tasks::TaskUpdate {
                body: Some("new requirements"),
                expected_revision: Some(1),
                ..Default::default()
            },
            2_000_001,
        )
        .unwrap();

        assert_eq!(
            store_classifications_for_inputs(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_002,
            )
            .unwrap(),
            0
        );
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert!(!crate::tasks::classification_is_complete(&task.refs));
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 2_000_003)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn stale_dependency_edit_result_is_not_persisted() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create_task(&mut conn, "dependency", 1);
        let task_id = create_task(&mut conn, "dependency race", 2);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        let dependencies = format!("[{dependency}]");

        crate::tasks::update(
            &mut conn,
            "test-agent",
            task_id,
            &crate::tasks::TaskUpdate {
                depends_on: Some(&dependencies),
                expected_revision: Some(1),
                ..Default::default()
            },
            2_000_001,
        )
        .unwrap();

        assert_eq!(
            store_classifications_for_inputs(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_002,
            )
            .unwrap(),
            0
        );
        assert!(unclassified_tasks(&conn)
            .unwrap()
            .iter()
            .any(|task| task.id == task_id));
    }

    #[test]
    fn revision_change_rejects_stale_result_when_prompt_input_is_unchanged() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "revision race", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        crate::tasks::update(
            &mut conn,
            "test-agent",
            task_id,
            &crate::tasks::TaskUpdate {
                refs: Some(r#"{"external":true}"#),
                expected_revision: Some(1),
                ..Default::default()
            },
            2_000_001,
        )
        .unwrap();

        assert_eq!(
            store_classifications_for_inputs(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_002,
            )
            .unwrap(),
            0
        );
        assert_eq!(
            crate::tasks::get(&conn, task_id).unwrap().unwrap().revision,
            2
        );
    }

    #[test]
    fn relevant_edit_invalidates_completed_classification() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "completed then edited", 1);
        assert_eq!(
            store_classifications(&mut conn, &[classified(task_id, 3)], "test:v2", 2_000_000)
                .unwrap(),
            1
        );

        crate::tasks::update(
            &mut conn,
            "test-agent",
            task_id,
            &crate::tasks::TaskUpdate {
                body: Some("materially changed"),
                expected_revision: Some(1),
                ..Default::default()
            },
            2_000_001,
        )
        .unwrap();

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert!(!crate::tasks::classification_is_complete(&task.refs));
        assert!(
            crate::tasks::claim(&mut conn, "worker", Some(task_id), &[], 60, 2_000_002)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unchanged_input_stores_normally_with_a_snapshot() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "unchanged", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            store_classifications_for_inputs(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_000,
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn store_and_stamp_rework_cap_persists_cap_with_accepted_classification() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "stamped adoption", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            store_classifications_and_stamp_rework_cap(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_000,
                10,
            )
            .unwrap(),
            1
        );
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert!(crate::tasks::classification_is_complete(&task.refs));
        // Adoption-time cap landed with the accepted refs in one write.
        assert_eq!(task.rework_cap, Some(10));
    }

    #[test]
    fn store_and_stamp_rework_cap_leaves_cap_unset_when_classification_rejected() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "rejected refs", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );

        // Invalidate the snapshot so the classifier result is rejected.
        crate::tasks::update(
            &mut conn,
            "test-agent",
            task_id,
            &crate::tasks::TaskUpdate {
                body: Some("materially changed"),
                expected_revision: Some(1),
                ..Default::default()
            },
            2_000_001,
        )
        .unwrap();

        assert_eq!(
            store_classifications_and_stamp_rework_cap(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_002,
                10,
            )
            .unwrap(),
            0
        );
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        // Neither the refs nor the cap moved: they land or roll back together.
        assert!(!crate::tasks::classification_is_complete(&task.refs));
        assert_eq!(task.rework_cap, None);
    }

    #[test]
    fn store_and_stamp_rework_cap_preserves_first_stamped_value() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "cap already stamped", 1);
        // Pre-stamp with 7, mimicking a task adopted under an earlier config.
        assert!(crate::tasks::stamp_rework_cap(&mut conn, task_id, 7, 2_000_000).unwrap());

        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            store_classifications_and_stamp_rework_cap(
                &mut conn,
                &[classified(task_id, 3)],
                &pending,
                "test:v2",
                2_000_001,
                10,
            )
            .unwrap(),
            1
        );
        // Immutable-once: the WHERE rework_cap IS NULL guard preserves the
        // earlier adoption-time value even when reconstructing classification
        // under a newer config.
        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.rework_cap, Some(7));
    }

    #[test]
    fn older_concurrent_attempt_cannot_overwrite_newer_accepted_result() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "concurrent attempts", 1);
        let pending = classification_inputs(
            &task_missing_cx(&conn, task_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        let newer = classified(task_id, 4);
        let older = classified(task_id, 2);

        assert_eq!(
            store_classifications_for_inputs(&mut conn, &[newer], &pending, "newer:v2", 2_000_001,)
                .unwrap(),
            1
        );
        assert_eq!(
            store_classifications_for_inputs(&mut conn, &[older], &pending, "older:v2", 2_000_002,)
                .unwrap(),
            0
        );
        let refs = crate::tasks::get(&conn, task_id)
            .unwrap()
            .unwrap()
            .refs
            .unwrap();
        let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(refs["cx_est"], 4);
        assert_eq!(refs["cx_by"], "newer:v2");
    }

    #[test]
    fn concurrent_connections_preserve_newer_accepted_classification() {
        use std::sync::{mpsc, Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut setup = crate::db::open(&db_path).unwrap();

        // Repeat against independent SQLite connections: this is the storage
        // race that the in-process ordering test above models more directly.
        for round in 0..8 {
            let task_id = create_task(&mut setup, "concurrent sqlite attempts", round + 1);
            let pending = classification_inputs(
                &task_missing_cx(&setup, task_id)
                    .unwrap()
                    .into_iter()
                    .collect::<Vec<_>>(),
            );
            let barrier = Arc::new(Barrier::new(2));
            let (newer_done, older_wait) = mpsc::channel();
            let newer_db = db_path.clone();
            let newer_inputs = pending.clone();
            let newer_barrier = Arc::clone(&barrier);
            let newer = std::thread::spawn(move || {
                let mut conn = crate::db::open(&newer_db).unwrap();
                newer_barrier.wait();
                let stored = store_classifications_for_inputs(
                    &mut conn,
                    &[classified(task_id, 4)],
                    &newer_inputs,
                    "newer:v2",
                    2_001_000 + round,
                )
                .unwrap();
                newer_done.send(()).unwrap();
                stored
            });
            let older_db = db_path.clone();
            let older_inputs = pending;
            let older = std::thread::spawn(move || {
                let mut conn = crate::db::open(&older_db).unwrap();
                barrier.wait();
                older_wait.recv().unwrap();
                store_classifications_for_inputs(
                    &mut conn,
                    &[classified(task_id, 2)],
                    &older_inputs,
                    "older:v2",
                    2_002_000 + round,
                )
                .unwrap()
            });

            assert_eq!(newer.join().unwrap(), 1);
            assert_eq!(older.join().unwrap(), 0);
            let refs = crate::tasks::get(&setup, task_id)
                .unwrap()
                .unwrap()
                .refs
                .unwrap();
            let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
            assert_eq!(refs["cx_est"], 4);
            assert_eq!(refs["cx_by"], "newer:v2");
        }
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
        assert_eq!(luna_by, "gpt-5.6-luna:v3");
        assert_eq!(terra_by, "gpt-5.6-terra:v3");
        assert_ne!(luna_by, terra_by);
    }

    #[test]
    fn large_review_only_classification_remains_in_review_without_parking() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Architectural task", 1);
        conn.execute(
            "UPDATE tasks SET review_only=1, status='in-review' WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        let mut classified_large = classified(task_id, 2);
        classified_large.size = "XL".into();
        let results = vec![classified_large];

        assert_eq!(
            store_classifications(&mut conn, &results, "gpt-5.6-luna:v2", 2_000_000).unwrap(),
            1
        );

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(task.status, "in-review");
        assert_eq!(task.assignee, None);
        assert_eq!(refs["cx_est"], 2);
        assert_eq!(refs["cx_by"], "gpt-5.6-luna:v2");
        assert!(refs.get("daemon_parked").is_none());
        assert!(crate::tasks::classification_is_dispatchable(
            &task.refs,
            task.review_only,
            task.continue_pr
        ));
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
        assert_eq!(parked_events, 0);
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
        parked.ready = false;
        parked.not_ready_reason = Some("outcome needs clarification".into());
        parked.duplicate_of = vec![99];
        store_classifications(&mut conn, &[parked], "test:v2", 10).unwrap();
        let parked_task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let parked_refs: serde_json::Value =
            serde_json::from_str(parked_task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(parked_refs["classifier_policy_parked"], true);
        conn.execute(
            "UPDATE tasks SET recovery_attempts=3 WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        let retry = crate::tasks::retry_parked(&mut conn, task_id, "owner", true, 11)
            .unwrap()
            .expect("policy retry accepted while awaiting reclassification");
        assert_eq!(retry.status, "failed");
        assert_eq!(retry.recovery_attempts, 0);
        let retry_refs = retry.refs.unwrap();
        let retry_refs: serde_json::Value = serde_json::from_str(&retry_refs).unwrap();
        assert_eq!(retry_refs["classifier_policy_parked"], true);
        for key in [
            "cx_est",
            "cx_size",
            "cx_size_reason",
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
        let retry_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE kind='task_retry' AND subject=?1",
                params![crate::tasks::lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retry_events, 1);
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
    fn legacy_v2_classification_without_size_reason_remains_dispatch_compatible() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "legacy v2", 1);
        let refs = r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"gpt-5.6-luna:v2"}"#;
        conn.execute(
            "UPDATE tasks SET refs=?2 WHERE id=?1",
            params![task_id, refs],
        )
        .unwrap();

        assert!(!unclassified_tasks(&conn)
            .unwrap()
            .iter()
            .any(|task| task.id == task_id));
        assert!(crate::tasks::classification_is_dispatchable(
            &Some(refs.into()),
            false,
            None
        ));
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
        assert!(!crate::tasks::classification_is_dispatchable(
            &refs, false, None
        ));
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
            size_reason: "bounded test classification rationale".into(),
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

        let mut missing_size_reason = result(4, "M", true, None);
        missing_size_reason.size_reason = "  ".into();
        assert!(!valid(&missing_size_reason));

        let mut nul_size_reason = result(4, "M", true, None);
        nul_size_reason.size_reason = "storage\0seam".into();
        assert!(!valid(&nul_size_reason));

        let mut oversized_size_reason = result(4, "M", true, None);
        oversized_size_reason.size_reason = "é".repeat(MAX_SIZE_REASON_BYTES);
        assert!(!valid(&oversized_size_reason));
    }

    #[test]
    fn policy_allows_focused_complexity_five() {
        assert!(parking_reason(&result(5, "S", true, None), false, false).is_none());
        assert!(parking_reason(&result(5, "M", true, None), false, false).is_none());
        assert!(parking_reason(&result(5, "L", true, None), false, false).is_none());
        assert!(parking_reason(&result(2, "XL", true, None), false, false).is_some());
        assert!(parking_reason(&result(2, "XL", true, None), false, true).is_none());
        assert!(parking_reason(&result(2, "XL", true, None), true, false).is_none());
    }

    #[test]
    fn prompt_is_closed_book_and_permissive() {
        let p = classifier_rubric("");
        assert!(p.contains("closed-book"));
        assert!(p.contains("Never reject merely because files"));
        assert!(p.contains("Declared dependencies are scheduler-enforced assumptions"));
        assert!(p.contains(
            "never return ready=false merely because a dependency is currently incomplete"
        ));
        assert!(p.contains("intended outcome cannot be determined"));
        assert!(p.contains("size_reason"));
        assert!(p.contains("independently deliverable seams"));
        assert!(p.contains("durable review feedback"));
    }
}
