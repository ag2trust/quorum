//! Coordinator operations with more than one step, shared by the CLI commands
//! and the orchestrator MCP tools so both run the same checks and the same
//! storage calls. Single-call operations (`task-list`, `log`, a cancelling
//! `task-update`) have no wrapper here: both callers invoke the core function.

use quorum_core::error::{QuorumError, Result};
use std::path::Path;

/// One `task-create` request. JSON-typed fields are already serialized.
pub struct NewTask<'a> {
    pub created_by: &'a str,
    pub title: &'a str,
    pub body: Option<&'a str>,
    pub priority: Option<i64>,
    pub labels: Option<&'a str>,
    pub refs: Option<&'a str>,
    pub depends_on: Option<&'a str>,
    pub review_pr: Option<i64>,
    pub review_only: Option<i64>,
    pub continue_pr: Option<i64>,
    pub base_branch: Option<&'a str>,
}

/// The creator-input checks that need neither a repository nor a database.
pub fn validate_new_task(task: &NewTask) -> Result<()> {
    // An explicit body that resolves empty/whitespace-only is bad input.
    // Omitting the body entirely remains valid.
    if task.body.is_some_and(|text| text.trim().is_empty()) {
        return Err(QuorumError::Usage(
            "explicit task body (--body-stdin/--body-file) must not be empty or whitespace-only"
                .into(),
        ));
    }
    quorum_core::tasks::validate_creator_labels(task.labels)?;
    quorum_core::tasks::validate_creator_refs(task.refs)
}

/// Create one task in `repo`'s database and return its id. The caller has
/// already run [`validate_new_task`].
pub fn task_create(repo: &str, task: &NewTask, now: i64) -> Result<i64> {
    // The serve configuration is read even when a base branch is given, so a
    // malformed one fails task intake loudly instead of only its default.
    let configured = crate::serve_config::task_create_base_branch(repo)?;
    let target_branch = task.base_branch.map_or(configured, str::to_string);
    quorum_core::tasks::validate_target_branch(&target_branch)?;
    let mut conn = quorum_core::db::open(&crate::paths::ensure_repo_dir(repo)?)?;
    match task.review_only {
        Some(pr) => quorum_core::tasks::create_review_only_no_rework(
            &mut conn,
            task.created_by,
            task.title,
            task.body,
            task.priority.unwrap_or(0),
            task.labels,
            task.refs,
            task.depends_on,
            pr,
            Some(&target_branch),
            now,
        ),
        None => quorum_core::tasks::create_with_continue_pr_and_target_branch(
            &mut conn,
            task.created_by,
            task.title,
            task.body,
            task.priority.unwrap_or(0),
            task.labels,
            task.refs,
            task.depends_on,
            task.review_pr,
            task.continue_pr,
            Some(&target_branch),
            now,
        ),
    }
}

/// `task-get` output: the task, its notes and runs, and any arbiter verdict.
#[derive(serde::Serialize)]
pub struct TaskView {
    #[serde(flatten)]
    pub task: quorum_core::tasks::TaskDetail,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arbiter: Option<quorum_core::stats::ArbiterVerdict>,
}

pub fn task_get(db: &Path, task_id: i64) -> Result<Option<TaskView>> {
    let conn = quorum_core::db::open(db)?;
    match quorum_core::tasks::get_with_notes(&conn, task_id)? {
        Some(task) => Ok(Some(TaskView {
            task,
            arbiter: quorum_core::stats::arbiter_verdict(&conn, task_id)?,
        })),
        None => Ok(None),
    }
}

/// Queue one message for delivery to a daemon-managed agent as a turn.
pub fn message(db: &Path, from: String, to: String, payload: String) -> Result<i64> {
    let mut conn = quorum_core::db::open(db)?;
    let row = quorum_core::mailbox::MailboxRow {
        agent: from,
        kind: quorum_core::mailbox::MailboxKind::Message,
        task_id: None,
        pr: None,
        verdict: None,
        feedback: None,
        note: None,
        to_agent: Some(to),
        payload: Some(payload),
    };
    quorum_core::mailbox::append(&mut conn, &row)
}

fn emitted<T: serde::Serialize>(value: &T, exit: i32) -> (String, i32) {
    (
        serde_json::to_string(value).expect("serialize owned types cannot fail"),
        exit,
    )
}

/// `task-retry`: resume a daemon-parked or provider-blocked task, or retry an
/// eligible exhausted decomposition plan. Returns the JSON the command emits
/// and its exit code (0 retried, 1 refused).
pub fn task_retry(db: &Path, task_id: i64, by: &str, now: i64) -> Result<(String, i32)> {
    let mut conn = quorum_core::db::open(db)?;
    let cancelled = quorum_core::tasks::cancelled_dep_ids(&conn, task_id)?;
    if !cancelled.is_empty() {
        return Ok(emitted(
            &serde_json::json!({
                "ok": false,
                "reason": "dependency cancelled — unsatisfiable; \
                           edit depends_on or close the dependent",
                "cancelled_deps": cancelled,
            }),
            1,
        ));
    }
    if let Some(reason) = quorum_core::tasks::reviewer_cap_retry_refusal(&conn, task_id)? {
        return Ok(emitted(
            &serde_json::json!({
                "ok": false,
                "outcome": "task-retry-rejected",
                "reason": reason,
            }),
            1,
        ));
    }
    let retried = match quorum_core::tasks::retry_parked(&mut conn, task_id, by, true, now)? {
        some @ Some(_) => some,
        None => quorum_core::tasks::retry_provider_blocked(&mut conn, task_id, by, now)?,
    };
    if let Some(task) = retried {
        return Ok(emitted(&quorum_core::tasks::TaskCompact::from(&task), 0));
    }
    Ok(
        match quorum_core::decomposition::retry_exhausted_planning(&mut conn, task_id, by, now)? {
            quorum_core::decomposition::PlanningRetryOutcome::Retried {
                graph_id,
                generation,
            } => {
                let task = quorum_core::tasks::get(&conn, task_id)?.ok_or_else(|| {
                    QuorumError::Io(format!(
                        "retried decomposition source #{task_id} disappeared"
                    ))
                })?;
                emitted(
                    &serde_json::json!({
                        "ok": true,
                        "outcome": "decomposition-planning-retried",
                        "graph_id": graph_id,
                        "retry_generation": generation,
                        "task": quorum_core::tasks::TaskCompact::from(&task),
                    }),
                    0,
                )
            }
            quorum_core::decomposition::PlanningRetryOutcome::RetryCapExhausted { retry_count } => {
                emitted(
                    &serde_json::json!({
                        "ok": false,
                        "outcome": "decomposition-planning-retry-rejected",
                        "reason": "operator retry cap exhausted; inspect the planning failures and rescope or close the source",
                        "retry_count": retry_count,
                        "retry_cap": quorum_core::decomposition::MAX_OPERATOR_RETRIES,
                    }),
                    1,
                )
            }
            quorum_core::decomposition::PlanningRetryOutcome::NotEligible => emitted(
                &serde_json::json!({
                    "ok": false,
                    "outcome": "task-retry-rejected",
                    "reason": "task is not daemon-parked, provider-blocked, or an eligible exhausted decomposition source",
                }),
                1,
            ),
        },
    )
}
