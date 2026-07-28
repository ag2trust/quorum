//! Task classifier — complexity scoring, shape-lint flags, type tags, and
//! duplicate-of hints. Observational v1: outputs are stored in `refs` and
//! surfaced as notes; nothing acts on them automatically.

use crate::complexity;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

/// Per-task classification output from the classifier agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClassification {
    pub task_id: i64,
    pub cx_est: i64,
    #[serde(default)]
    pub cx_flags: Vec<String>,
    #[serde(default)]
    pub cx_tags: Vec<String>,
    #[serde(default)]
    pub cx_dup_of: Vec<i64>,
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
}

const VALID_FLAGS: &[&str] = &["oversized", "underspecified"];
const VALID_KINDS: &[&str] = &[
    "kind:bug",
    "kind:feature",
    "kind:test",
    "kind:docs",
    "kind:refactor",
    "kind:infra",
    "kind:chore",
];

/// Query open/working/terminal tasks that have no `cx_est` in refs.
/// Terminal tasks are included so the classifier catches them within one tick
/// of reaching done/failed/cancelled (terminal fallback).
pub fn unclassified_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, body FROM tasks
         WHERE status IN ('open', 'working', 'in-review', 'rework', 'merging',
                          'done', 'failed', 'cancelled')
         AND (refs IS NULL OR json_extract(refs, '$.cx_est') IS NULL)",
    )?;
    let tasks = stmt
        .query_map([], |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                title: row.get(1)?,
                body: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tasks)
}

/// Check whether a specific task lacks cx_est in refs.
pub fn task_missing_cx(conn: &Connection, task_id: i64) -> Result<Option<TaskForClassification>> {
    conn.query_row(
        "SELECT id, title, body FROM tasks
         WHERE id = ?1
         AND (refs IS NULL OR json_extract(refs, '$.cx_est') IS NULL)",
        params![task_id],
        |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                title: row.get(1)?,
                body: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// All open/working tasks (for dup-detection context).
pub fn dup_context_tasks(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, body FROM tasks
         WHERE status IN ('open', 'working')
         ORDER BY id",
    )?;
    let tasks = stmt
        .query_map([], |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                title: row.get(1)?,
                body: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tasks)
}

/// Query ALL tasks (any status) missing cx_est — for `--backfill`.
pub fn tasks_missing_cx_all(conn: &Connection) -> Result<Vec<TaskForClassification>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, body FROM tasks
         WHERE refs IS NULL OR json_extract(refs, '$.cx_est') IS NULL
         ORDER BY id",
    )?;
    let tasks = stmt
        .query_map([], |row| {
            Ok(TaskForClassification {
                id: row.get(0)?,
                title: row.get(1)?,
                body: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tasks)
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
        if result.cx_est < 1 || result.cx_est > 5 {
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

            if !sanitized.cx_flags.is_empty() || !sanitized.cx_dup_of.is_empty() {
                let note = build_classifier_note(&sanitized);
                tx.execute(
                    "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'classifier', ?3)",
                    params![result.task_id, now, note],
                )?;
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
        cx_flags: result
            .cx_flags
            .iter()
            .filter(|f| VALID_FLAGS.contains(&f.as_str()))
            .cloned()
            .collect(),
        cx_tags: result
            .cx_tags
            .iter()
            .filter(|t| VALID_KINDS.contains(&t.as_str()) || t.starts_with("area:"))
            .cloned()
            .collect(),
        cx_dup_of: result.cx_dup_of.clone(),
    }
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

    let map = obj.as_object_mut().unwrap();
    map.insert("cx_est".into(), serde_json::json!(result.cx_est));
    map.insert("cx_by".into(), serde_json::json!(version));

    if !result.cx_flags.is_empty() {
        map.insert("cx_flags".into(), serde_json::json!(result.cx_flags));
    }
    if !result.cx_tags.is_empty() {
        map.insert("cx_tags".into(), serde_json::json!(result.cx_tags));
    }
    if !result.cx_dup_of.is_empty() {
        map.insert("cx_dup_of".into(), serde_json::json!(result.cx_dup_of));
    }

    obj.to_string()
}

fn build_classifier_note(result: &TaskClassification) -> String {
    let mut parts = Vec::new();

    for flag in &result.cx_flags {
        match flag.as_str() {
            "oversized" => parts.push("oversized — looks > ~30-45 min of agent work".to_string()),
            "underspecified" => {
                parts.push("underspecified — no acceptance criteria / ambiguous scope".to_string())
            }
            other => parts.push(other.to_string()),
        }
    }

    if !result.cx_dup_of.is_empty() {
        let ids: Vec<String> = result.cx_dup_of.iter().map(|id| format!("#{id}")).collect();
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
    for t in tasks {
        prompt.push_str(&format!("### Task #{}\n", t.id));
        prompt.push_str(&format!("**Title:** {}\n", t.title));
        if let Some(body) = &t.body {
            let truncated = if body.len() > 2000 {
                format!(
                    "{}…",
                    &body[..body.char_indices().nth(2000).map_or(body.len(), |(i, _)| i)]
                )
            } else {
                body.clone()
            };
            prompt.push_str(&format!("**Body:**\n{truncated}\n"));
        }
        prompt.push('\n');
    }

    if !dup_context.is_empty() {
        prompt.push_str("## Other open/working tasks (for duplicate detection)\n\n");
        for t in dup_context {
            let snippet = t
                .body
                .as_deref()
                .map(|b| {
                    if b.len() > 200 {
                        format!(
                            "{}…",
                            &b[..b.char_indices().nth(200).map_or(b.len(), |(i, _)| i)]
                        )
                    } else {
                        b.to_string()
                    }
                })
                .unwrap_or_default();
            prompt.push_str(&format!("- #{}: {} — {snippet}\n", t.id, t.title));
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

1. **cx_est** (integer 1-5): Complexity estimate based on the task description AS WRITTEN at creation time.
{rubric_lines}

The active daemon's operational routing policy for these levels is:
{recommendations}
This is not a cross-vendor benchmark and does not change the required output.

2. **cx_flags** (array of strings, may be empty): Shape-lint flags.
   - "oversized": Task looks like > ~30-45 min of agent work (complexity 4-5 with broad scope)
   - "underspecified": No acceptance criteria, ambiguous scope, or missing file pointers

3. **cx_tags** (array of strings, may be empty): Normalized type/area tags.
   - Kind (pick one): "kind:bug", "kind:feature", "kind:test", "kind:docs", "kind:refactor", "kind:infra", "kind:chore"
   - Area (optional, pick relevant): "area:<component>" where component matches the codebase area (e.g. "area:daemon", "area:cli", "area:store", "area:lifecycle")

4. **cx_dup_of** (array of task IDs, may be empty): IDs of other open/working tasks this one substantially overlaps with. Only flag genuine duplicates, not related tasks.

Score based ONLY on the task description — never on execution outcomes, diffs, or agent performance.

Output format (JSON array wrapped in an object):
{{"tasks": [{{"task_id": 1, "cx_est": 3, "cx_flags": [], "cx_tags": ["kind:feature", "area:daemon"], "cx_dup_of": []}}]}}"#
    )
}

/// Stable classifier provenance string for `cx_by`.
///
/// The model is part of the identifier so classification quality can be grouped
/// by the model that actually produced it. `v1` distinguishes this prompt and
/// parser contract from future revisions.
pub fn classifier_provenance(model: &str) -> String {
    format!("{model}:v1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_cx_into_empty_refs() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 3,
            cx_flags: vec!["oversized".into()],
            cx_tags: vec!["kind:feature".into()],
            cx_dup_of: vec![],
        };
        let refs = merge_cx_into_refs(&None, &result, "haiku-45:v1");
        let v: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(v["cx_est"], 3);
        assert_eq!(v["cx_by"], "haiku-45:v1");
        assert_eq!(v["cx_flags"][0], "oversized");
    }

    #[test]
    fn merge_cx_preserves_existing_pr() {
        let existing = Some(r#"{"pr":42}"#.to_string());
        let result = TaskClassification {
            task_id: 1,
            cx_est: 2,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![],
        };
        let refs = merge_cx_into_refs(&existing, &result, "haiku-45:v1");
        let v: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(v["pr"], 42);
        assert_eq!(v["cx_est"], 2);
    }

    #[test]
    fn classifier_provenance_identifies_the_model() {
        assert_eq!(classifier_provenance("gpt-5.6-luna"), "gpt-5.6-luna:v1");
        assert_eq!(classifier_provenance("gpt-5.6-terra"), "gpt-5.6-terra:v1");
    }

    #[test]
    fn sanitize_strips_invalid_flags() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 3,
            cx_flags: vec!["oversized".into(), "bogus".into()],
            cx_tags: vec!["kind:feature".into(), "invalid".into()],
            cx_dup_of: vec![],
        };
        let clean = sanitize(&result);
        assert_eq!(clean.cx_flags, vec!["oversized"]);
        assert_eq!(clean.cx_tags, vec!["kind:feature"]);
    }

    #[test]
    fn sanitize_clamps_cx_est() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 7,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![],
        };
        let clean = sanitize(&result);
        assert_eq!(clean.cx_est, 5);
    }

    #[test]
    fn build_classifier_note_flags_and_dups() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 3,
            cx_flags: vec!["underspecified".into()],
            cx_tags: vec![],
            cx_dup_of: vec![47, 48],
        };
        let note = build_classifier_note(&result);
        assert!(note.contains("underspecified"));
        assert!(note.contains("#47"));
        assert!(note.contains("#48"));
    }

    #[test]
    fn parse_classifier_response() {
        let json = r#"{"tasks": [{"task_id": 1, "cx_est": 3, "cx_flags": ["oversized"], "cx_tags": ["kind:feature"], "cx_dup_of": []}]}"#;
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
        }];
        let ctx = vec![TaskForClassification {
            id: 2,
            title: "Other task".into(),
            body: Some("Do something".into()),
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
    fn empty_flags_and_dups_produce_no_note() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 3,
            cx_flags: vec![],
            cx_tags: vec!["kind:feature".into()],
            cx_dup_of: vec![],
        };
        // No note should be generated — flags and dups are empty
        assert!(result.cx_flags.is_empty() && result.cx_dup_of.is_empty());
    }

    #[test]
    fn area_tags_pass_sanitization() {
        let result = TaskClassification {
            task_id: 1,
            cx_est: 3,
            cx_flags: vec![],
            cx_tags: vec!["area:daemon".into(), "area:cli".into()],
            cx_dup_of: vec![],
        };
        let clean = sanitize(&result);
        assert_eq!(clean.cx_tags, vec!["area:daemon", "area:cli"]);
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

        let results = vec![TaskClassification {
            task_id,
            cx_est: 3,
            cx_flags: vec!["oversized".into()],
            cx_tags: vec!["kind:feature".into()],
            cx_dup_of: vec![],
        }];
        let stored = store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();
        assert_eq!(stored, 1);

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(unclassified.len(), 0);

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["cx_est"], 3);
        assert_eq!(refs["cx_by"], "haiku-45:v1");

        let notes = crate::tasks::get_with_notes(&conn, task_id)
            .unwrap()
            .unwrap()
            .notes;
        assert_eq!(notes.len(), 1);
        assert!(notes[0].body.contains("oversized"));
    }

    #[test]
    fn store_classifications_records_each_classifier_model() {
        let (_dir, mut conn) = open_tmp();
        let luna_task = create_task(&mut conn, "Luna task", 1);
        let terra_task = create_task(&mut conn, "Terra task", 2);
        let result = |task_id| {
            vec![TaskClassification {
                task_id,
                cx_est: 2,
                cx_flags: vec![],
                cx_tags: vec![],
                cx_dup_of: vec![],
            }]
        };

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
        assert_eq!(luna_by, "gpt-5.6-luna:v1");
        assert_eq!(terra_by, "gpt-5.6-terra:v1");
        assert_ne!(luna_by, terra_by);
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

        let results = vec![TaskClassification {
            task_id,
            cx_est: 2,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![],
        }];
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

        let results = vec![TaskClassification {
            task_id,
            cx_est: 0,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![],
        }];
        let stored = store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();
        assert_eq!(stored, 0);

        let unclassified = unclassified_tasks(&conn).unwrap();
        assert_eq!(unclassified.len(), 1);
    }

    #[test]
    fn no_note_when_no_flags_or_dups() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create_task(&mut conn, "Clean task", 1);

        let results = vec![TaskClassification {
            task_id,
            cx_est: 2,
            cx_flags: vec![],
            cx_tags: vec!["kind:bug".into()],
            cx_dup_of: vec![],
        }];
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

        let results = vec![TaskClassification {
            task_id: t1,
            cx_est: 3,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![t2],
        }];
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
        let results = vec![TaskClassification {
            task_id: t1,
            cx_est: 2,
            cx_flags: vec![],
            cx_tags: vec![],
            cx_dup_of: vec![],
        }];
        store_classifications(&mut conn, &results, "haiku-45:v1", 2_000_000).unwrap();

        let missing = tasks_missing_cx_all(&conn).unwrap();
        assert_eq!(missing.len(), 1);
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
        for (_level, _label, desc, _time) in &crate::complexity::RUBRIC {
            assert!(
                skill.contains(*desc),
                "skill file missing rubric description: {desc}"
            );
        }
    }
}
