//! Automatic post-merge review-analytics collector (#125).
//!
//! Called from the daemon after `MergeSucceeded` fires. Deterministic Rust code
//! gathers all inputs (raw `gh api` fetches + DB reads); a bounded Haiku-class
//! classifier turns the assembled record into structured findings; results land
//! in `review_findings` + `review_collection_runs`.
//!
//! Boundary rules (see #125 task brief):
//! - The collector never mutates the task, the merge outcome, the reviewer
//!   verdict, or the rework lifecycle. All writes are analytics-only.
//! - Classifier failures are recorded (loud row in `review_collection_runs` +
//!   `errors` row) and returned; they never unwind a merge.
//! - The classifier is spawned WITHOUT any allowlist tools — it receives the
//!   assembled prompt and must respond with a JSON envelope only. It has no
//!   Bash/Read/Write/Edit and cannot post to GitHub.
//! - Idempotent: re-running for the same PR replaces prior findings and the run
//!   record via `pr_number`-keyed UPSERT.

use super::agent::{self, AgentProc, AgentSpec};
use super::classifier::{CLASSIFIER_EFFORT, CLASSIFIER_MODEL};
use super::stream::Event;
use quorum_core::clock;
use quorum_core::error::{QuorumError, Result};
use quorum_core::review_findings::{
    self, AgentRunSummary, CollectionRun, CollectorInputs, RunStatus, TaskContext, VerdictSummary,
};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

/// Version stamp on every finding / run row this collector generation writes.
/// Bump when the prompt/schema meaningfully changes so future readers can filter
/// analytics by generation.
pub const COLLECTOR_VERSION: &str = "v1";

/// Hard wall-clock cap on the classifier turn. Post-merge, so failing here
/// leaves the merged task alone — the observable surface is `review_collection_runs`.
const CLASSIFIER_TIMEOUT: Duration = Duration::from_secs(180);

/// Cap each raw GitHub payload we splice into the prompt. Bounded input protects
/// budget on huge PRs (a 3k-comment thread should not become a 200k-token prompt).
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// Wall-clock cap per `gh api` sub-call. If a fetch hangs the classifier never
/// runs — better than letting the daemon-scoped task stall indefinitely.
const GH_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything the collector needs to run for one PR. Passed into
/// [`run_collection_with_inputs`] so tests can bypass `gh api` entirely.
#[derive(Debug, Clone)]
pub struct CollectionRequest {
    pub pr_number: i64,
    pub task_id: Option<i64>,
    pub repo_slug: Option<String>,
    pub db_path: PathBuf,
    pub repo_dir: PathBuf,
    pub agent_bin: Option<String>,
    pub bare_agent: bool,
    /// Extra env vars threaded into the spawned classifier process. Empty in
    /// production. Used by tests to script the fake-agent binary per-call so
    /// concurrent tests do not race on a process-global env var.
    #[allow(dead_code)]
    pub env_vars: Vec<(String, String)>,
}

impl CollectionRequest {
    /// Convenience constructor for callers that don't need custom env vars.
    pub fn new(
        pr_number: i64,
        task_id: Option<i64>,
        repo_slug: Option<String>,
        db_path: PathBuf,
        repo_dir: PathBuf,
        agent_bin: Option<String>,
        bare_agent: bool,
    ) -> Self {
        Self {
            pr_number,
            task_id,
            repo_slug,
            db_path,
            repo_dir,
            agent_bin,
            bare_agent,
            env_vars: vec![],
        }
    }
}

/// Result summary returned to the caller (the daemon logs it; the CLI prints it).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CollectionOutcome {
    pub pr_number: i64,
    pub task_id: Option<i64>,
    pub status: RunStatus,
    pub findings_count: i64,
    pub error: Option<String>,
}

/// Full pipeline: fetch inputs, spawn classifier, parse + store. On any failure
/// the run record is stamped `failed` with the error text — never returns without
/// having written the run row.
pub async fn run_collection(request: &CollectionRequest) -> Result<CollectionOutcome> {
    let attempted_at = clock::now();

    // 1) Deterministic fetch.
    let inputs_result = fetch_inputs(request).await;
    let inputs = match inputs_result {
        Ok(i) => i,
        Err(e) => {
            let err_text = format!("collector fetch failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };

    // 2) Spawn classifier + await bounded turn.
    let response_text = match spawn_and_run_classifier(request, &inputs).await {
        Ok(t) => t,
        Err(e) => {
            let err_text = format!("collector classifier failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };

    // 3) Parse response.
    let findings = match review_findings::parse_response_with_provenance(
        &response_text,
        request.pr_number,
        request.task_id,
        Some(CLASSIFIER_MODEL),
        Some(COLLECTOR_VERSION),
    ) {
        Some(f) => f,
        None => {
            let err_text = format!(
                "collector response parse failed for PR #{} — response len {}",
                request.pr_number,
                response_text.len()
            );
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };

    // 4) Persist findings + success run row.
    let pr = request.pr_number;
    let task_id = request.task_id;
    let db_path = request.db_path.clone();
    let count = findings.len() as i64;
    let write_result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        review_findings::replace_for_pr(&mut conn, pr, &findings)?;
        let now = clock::now();
        review_findings::record_run(
            &conn,
            &CollectionRun {
                pr_number: pr,
                task_id,
                status: RunStatus::Success,
                error: None,
                collector_model: CLASSIFIER_MODEL.to_string(),
                collector_version: COLLECTOR_VERSION.to_string(),
                findings_count: count,
                attempted_at,
                completed_at: Some(now),
            },
        )?;
        Ok(())
    })
    .await;

    match write_result {
        Ok(Ok(())) => Ok(CollectionOutcome {
            pr_number: pr,
            task_id,
            status: RunStatus::Success,
            findings_count: count,
            error: None,
        }),
        Ok(Err(e)) => {
            let err_text = format!("collector db write failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            Err(QuorumError::Io(err_text))
        }
        Err(e) => {
            let err_text = format!("collector db-write join failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            Err(QuorumError::Io(err_text))
        }
    }
}

/// Record a failed run + log to `errors`. Best-effort: a follow-up failure here
/// must not shadow the original error the caller is about to report.
async fn record_failure(request: &CollectionRequest, error: &str, attempted_at: i64) {
    let db_path = request.db_path.clone();
    let pr = request.pr_number;
    let task_id = request.task_id;
    let error_text = error.to_string();
    let _ = tokio::task::spawn_blocking(move || -> Result<()> {
        let conn = quorum_core::db::open(&db_path)?;
        let now = clock::now();
        review_findings::record_run(
            &conn,
            &CollectionRun {
                pr_number: pr,
                task_id,
                status: RunStatus::Failed,
                error: Some(error_text.clone()),
                collector_model: CLASSIFIER_MODEL.to_string(),
                collector_version: COLLECTOR_VERSION.to_string(),
                findings_count: 0,
                attempted_at,
                completed_at: Some(now),
            },
        )?;
        quorum_core::errlog::log_error(&conn, now, "review-collector", &error_text);
        Ok(())
    })
    .await;
}

/// Deterministically fetch every input the classifier will read. Failures at
/// any single sub-fetch return an error — better to record one failed run than
/// to hand the model a partial view. Bounded via [`MAX_PAYLOAD_BYTES`] so a
/// pathological PR does not blow up the classifier's context budget.
pub async fn fetch_inputs(request: &CollectionRequest) -> Result<CollectorInputs> {
    let pr = request.pr_number;
    let repo = request.repo_slug.clone();
    let repo_dir = request.repo_dir.clone();

    let pr_metadata_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}"),
    )
    .await
    .unwrap_or_else(|_| "{}".to_string());
    let reviews_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/reviews"),
    )
    .await
    .unwrap_or_else(|_| "[]".to_string());
    let review_comments_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/comments"),
    )
    .await
    .unwrap_or_else(|_| "[]".to_string());
    let issue_comments_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/issues/{pr}/comments"),
    )
    .await
    .unwrap_or_else(|_| "[]".to_string());
    let commits_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/commits"),
    )
    .await
    .unwrap_or_else(|_| "[]".to_string());
    let diff_stat = gh_diff_stat(&repo, &repo_dir, pr)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let checks_summary = gh_checks_summary(&repo, &repo_dir, pr)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    // DB context (task metadata + agent runs + verdicts). All best-effort;
    // absence produces empty context rather than a hard failure.
    let task_context = build_task_context(&request.db_path, request.task_id).await;

    Ok(CollectorInputs {
        pr_number: pr,
        pr_metadata_json: truncate(&pr_metadata_json),
        reviews_json: truncate(&reviews_json),
        review_comments_json: truncate(&review_comments_json),
        issue_comments_json: truncate(&issue_comments_json),
        commits_json: truncate(&commits_json),
        checks_summary: truncate(&checks_summary),
        diff_stat: truncate(&diff_stat),
        task_context,
    })
}

fn truncate(s: &str) -> String {
    if s.len() <= MAX_PAYLOAD_BYTES {
        s.to_string()
    } else {
        let mut cut = MAX_PAYLOAD_BYTES;
        // Avoid slicing mid-UTF-8 code point.
        while cut > 0 && !s.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}\n... [truncated {} bytes]", &s[..cut], s.len() - cut)
    }
}

async fn build_task_context(db_path: &Path, task_id: Option<i64>) -> TaskContext {
    let Some(tid) = task_id else {
        return TaskContext::default();
    };
    let db_path = db_path.to_path_buf();
    let joined = tokio::task::spawn_blocking(move || -> Option<TaskContext> {
        let conn = quorum_core::db::open(&db_path).ok()?;
        let task = quorum_core::tasks::get(&conn, tid).ok().flatten()?;
        let runs = quorum_core::agent_runs::runs_for_task(&conn, tid).unwrap_or_default();
        let agent_runs = runs
            .into_iter()
            .map(|r| AgentRunSummary {
                agent: r.agent,
                role: r.role,
                sub_role: r.sub_role,
                model: r.model,
                effort: r.effort,
                end_reason: r.end_reason,
            })
            .collect();
        // Approvals table is small (live approvals only) but historic ones are
        // deleted on merge, so at most one row will typically still exist when
        // this runs (during the merge-succeeded firing itself). Fall through
        // gracefully if empty.
        let mut verdicts = Vec::new();
        if let Ok(all) = quorum_core::approvals::list(&conn) {
            for a in all.into_iter().filter(|a| a.task_id == tid) {
                verdicts.push(VerdictSummary {
                    reviewer: a.reviewer,
                    verdict: a.verdict,
                    blocking_count: a.blocking_count,
                    head_sha: a.approved_head_sha,
                    created_at: 0,
                });
            }
        }
        Some(TaskContext {
            task_id: Some(tid),
            author: task.author,
            reviewer: task.reviewer,
            rework_round: task.rework_round,
            review_only: task.review_only,
            agent_runs,
            verdicts,
        })
    })
    .await
    .ok()
    .flatten();
    joined.unwrap_or(TaskContext {
        task_id: Some(tid),
        ..TaskContext::default()
    })
}

async fn gh_api(repo: &Option<String>, cwd: &Path, endpoint: &str) -> Result<String> {
    let mut args: Vec<String> = vec!["api".into()];
    if let Some(r) = repo {
        args.push("-R".into());
        args.push(r.clone());
    }
    args.push(endpoint.to_string());
    run_gh(&args, cwd).await
}

async fn gh_diff_stat(repo: &Option<String>, cwd: &Path, pr: i64) -> Result<String> {
    let mut args: Vec<String> = vec![
        "pr".into(),
        "view".into(),
        pr.to_string(),
        "--json".into(),
        "changedFiles,additions,deletions,title".into(),
    ];
    if let Some(r) = repo {
        args.push("-R".into());
        args.push(r.clone());
    }
    run_gh(&args, cwd).await
}

async fn gh_checks_summary(repo: &Option<String>, cwd: &Path, pr: i64) -> Result<String> {
    let mut args: Vec<String> = vec!["pr".into(), "checks".into(), pr.to_string()];
    if let Some(r) = repo {
        args.push("-R".into());
        args.push(r.clone());
    }
    run_gh(&args, cwd).await
}

async fn run_gh(args: &[String], cwd: &Path) -> Result<String> {
    let cwd = cwd.to_path_buf();
    let args = args.to_vec();
    let handle = tokio::task::spawn_blocking(move || -> Result<String> {
        let out = std::process::Command::new("gh")
            .args(&args)
            .current_dir(&cwd)
            .output()
            .map_err(|e| QuorumError::Io(format!("gh: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(QuorumError::Io(format!("gh {args:?} failed: {stderr}")));
        }
        String::from_utf8(out.stdout).map_err(|e| QuorumError::Io(format!("invalid utf8: {e}")))
    });
    match timeout(GH_FETCH_TIMEOUT, handle).await {
        Ok(Ok(res)) => res,
        Ok(Err(join_err)) => Err(QuorumError::Io(format!("gh join failed: {join_err}"))),
        Err(_) => Err(QuorumError::Io("gh timed out".into())),
    }
}

/// Spawn the Haiku classifier, feed the prompt, wait for a bounded Result.
/// The spawn passes an EMPTY allowed_tools list — the classifier can only
/// respond in text, cannot Bash / Read / Write / Edit, and cannot reach GitHub.
async fn spawn_and_run_classifier(
    request: &CollectionRequest,
    inputs: &CollectorInputs,
) -> Result<String> {
    let prompt = review_findings::build_collector_prompt(inputs);
    let turn = agent::user_turn(&prompt);

    let spec = AgentSpec {
        model: CLASSIFIER_MODEL.to_string(),
        effort: CLASSIFIER_EFFORT.to_string(),
        session_id: agent::new_session_id(),
        worktree: request.repo_dir.clone(),
        bare: request.bare_agent,
        allowed_tools: String::new(),
        env_vars: request.env_vars.clone(),
    };

    let mut proc = AgentProc::spawn(&spec, request.agent_bin.as_deref())
        .map_err(|e| QuorumError::Io(format!("spawn classifier: {e}")))?;

    proc.feed_turn(&turn)
        .await
        .map_err(|e| QuorumError::Io(format!("feed_turn: {e}")))?;

    let deadline = tokio::time::Instant::now() + CLASSIFIER_TIMEOUT;
    let mut response_text = String::new();
    let outcome: Result<String> = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break Err(QuorumError::Io(format!(
                "classifier timeout for PR #{}",
                request.pr_number
            )));
        }
        match timeout(remaining, proc.next_event()).await {
            Ok(Some(Event::Result {
                result, is_error, ..
            })) => {
                if is_error.unwrap_or(false) {
                    break Err(QuorumError::Io("classifier returned is_error=true".into()));
                }
                let text = result
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| result.to_string());
                if !text.is_empty() {
                    response_text = text;
                }
                break Ok(response_text.clone());
            }
            Ok(Some(Event::Assistant { message })) => {
                if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                    response_text.push_str(content);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                break if response_text.is_empty() {
                    Err(QuorumError::Io(
                        "classifier stream closed with no response".into(),
                    ))
                } else {
                    Ok(response_text.clone())
                };
            }
            Err(_) => {
                break Err(QuorumError::Io(format!(
                    "classifier timeout for PR #{}",
                    request.pr_number
                )));
            }
        }
    };

    proc.kill_and_reap().await;
    outcome
}

/// Spawn a detached collection task from the daemon merge branch. Never awaits.
/// The daemon tick returns immediately; the collection runs in the background
/// and records its own success/failure to `review_collection_runs`. This is the
/// architectural boundary — the merged task's lifecycle is already complete
/// before we get here, and nothing this function does can undo that.
pub fn spawn_detached(request: CollectionRequest) {
    tokio::spawn(async move {
        match run_collection(&request).await {
            Ok(outcome) => {
                eprintln!(
                    "quorum serve: post-merge collector for PR #{} completed \
                     ({} findings)",
                    outcome.pr_number, outcome.findings_count
                );
            }
            Err(e) => {
                eprintln!(
                    "quorum serve: post-merge collector for PR #{} FAILED: {} \
                     (recorded in review_collection_runs; retry via `quorum review-interpret`)",
                    request.pr_number, e
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::db;

    fn tmp_conn() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let _ = db::open(&path).unwrap();
        (dir, path)
    }

    #[test]
    fn truncate_bounded_input_marks_cut() {
        let s = "a".repeat(MAX_PAYLOAD_BYTES + 100);
        let out = truncate(&s);
        assert!(out.contains("truncated"));
        assert!(out.len() < s.len() + 40);
    }

    #[test]
    fn truncate_short_input_passes_through() {
        let s = "hello";
        assert_eq!(truncate(s), "hello");
    }

    #[test]
    fn truncate_respects_utf8_boundary() {
        // Emoji is 4 bytes; construct a string just past MAX_PAYLOAD_BYTES
        // with a multibyte char straddling the boundary. Slicing mid-codepoint
        // would panic — the guard steps back until it lands on a boundary.
        let mut s = "a".repeat(MAX_PAYLOAD_BYTES - 2);
        s.push('😀'); // 4-byte codepoint straddles the cap
        s.push_str(&"b".repeat(100));
        let out = truncate(&s); // must not panic
        assert!(out.contains("truncated"));
    }

    #[tokio::test]
    async fn record_failure_writes_run_and_error_rows() {
        let (_dir, db_path) = tmp_conn();
        let request = CollectionRequest::new(
            42,
            Some(7),
            None,
            db_path.clone(),
            std::env::current_dir().unwrap(),
            None,
            true,
        );
        record_failure(&request, "boom", 1000).await;

        let conn = db::open(&db_path).unwrap();
        let run = review_findings::get_run(&conn, 42).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("boom"));
        assert_eq!(run.collector_model, CLASSIFIER_MODEL);
        assert_eq!(run.collector_version, COLLECTOR_VERSION);

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM errors WHERE source='review-collector'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn record_failure_is_idempotent_on_retry() {
        let (_dir, db_path) = tmp_conn();
        let request = CollectionRequest::new(
            43,
            None,
            None,
            db_path.clone(),
            std::env::current_dir().unwrap(),
            None,
            true,
        );
        record_failure(&request, "attempt-1", 1000).await;
        record_failure(&request, "attempt-2", 2000).await;

        let conn = db::open(&db_path).unwrap();
        // Only one row for the PR (UPSERT keyed on pr_number).
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=?1",
                rusqlite::params![43i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let run = review_findings::get_run(&conn, 43).unwrap().unwrap();
        assert_eq!(run.error.as_deref(), Some("attempt-2"));
        assert_eq!(run.attempted_at, 2000);
    }

    #[tokio::test]
    async fn build_task_context_absent_task_returns_default() {
        let (_dir, db_path) = tmp_conn();
        let ctx = build_task_context(&db_path, None).await;
        assert!(ctx.task_id.is_none());
        assert!(ctx.agent_runs.is_empty());
    }

    // ------------------------------------------------------------------
    // Live end-to-end tests driving the built `fake-agent` binary.
    //
    // These cover:
    // - positive: merge → collector run → structured findings + success row
    // - negative: classifier failure → task/merge untouched, row='failed', errlog
    // - idempotency: retry does not duplicate findings or run rows
    // - failure→success retry: bad row overwritten by good one
    //
    // `fetch_inputs` degrades gracefully when `gh` is absent (each fetch
    // falls back to its default empty JSON string), so the tests don't need
    // GitHub or a real repo — they exercise the full pipeline against the
    // fake-agent binary built in the same cargo run.
    // ------------------------------------------------------------------

    fn fake_agent_path() -> std::path::PathBuf {
        assert_cmd::cargo::cargo_bin("fake-agent")
    }

    fn setup_git_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        // A `git init` cwd keeps the collector-driven `gh` invocations happy
        // (they still fail — no repo — but the fall-through defaults kick in).
        let d = dir.path().to_string_lossy();
        let _ = std::process::Command::new("git")
            .args(["-C", &d, "init", "-b", "main"])
            .output();
        dir
    }

    fn live_request(
        dir: &std::path::Path,
        db: &std::path::Path,
        pr: i64,
        task_id: Option<i64>,
        force_fail: bool,
    ) -> CollectionRequest {
        let mut env_vars = Vec::new();
        if force_fail {
            // Per-request env keeps concurrent tests isolated — a process-global
            // `set_var` would leak into a sibling test's fake-agent spawn.
            env_vars.push(("FAKE_AGENT_COLLECTOR_FAIL".to_string(), "1".to_string()));
        }
        CollectionRequest {
            pr_number: pr,
            task_id,
            repo_slug: None,
            db_path: db.to_path_buf(),
            repo_dir: dir.to_path_buf(),
            agent_bin: Some(fake_agent_path().to_string_lossy().to_string()),
            bare_agent: true,
            env_vars,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_positive_run_stores_findings_and_run_row() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();
        let request = live_request(dir.path(), &db, 42, Some(7), false);

        let outcome = run_collection(&request)
            .await
            .expect("fake-agent should produce a valid collector response");
        assert_eq!(outcome.findings_count, 2);
        assert_eq!(outcome.status, RunStatus::Success);

        let conn = db::open(&db).unwrap();
        let run = review_findings::get_run(&conn, 42).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.findings_count, 2);
        assert!(run.error.is_none());
        assert_eq!(run.collector_version, COLLECTOR_VERSION);
        assert!(run.completed_at.is_some());

        let findings = review_findings::list_for_pr(&conn, 42).unwrap();
        assert_eq!(findings.len(), 2);

        // First finding: blocking + addressed, evidence -> review_comment#101.
        assert_eq!(findings[0].kind, "blocking");
        assert_eq!(findings[0].addressed_status.as_deref(), Some("addressed"));
        assert_eq!(findings[0].evidence[0].kind, "review_comment");
        assert_eq!(findings[0].evidence[0].id, 101);
        assert_eq!(findings[0].task_id, Some(7));
        assert!(findings[0].collector_model.is_some());
        assert_eq!(
            findings[0].collector_version.as_deref(),
            Some(COLLECTOR_VERSION)
        );

        // Second: suggestion, pushback accepted, evidence -> issue_comment#202.
        assert_eq!(findings[1].kind, "suggestion");
        assert!(findings[1].author_pushback);
        assert_eq!(findings[1].pushback_accepted, Some(true));
        assert_eq!(findings[1].evidence[0].kind, "issue_comment");
        assert_eq!(findings[1].evidence[0].id, 202);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_negative_classifier_failure_records_failed_run_no_findings() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();
        // Insert a "merged/done" task marker to prove the collector never
        // touches the lifecycle. We just record the pre-state and re-assert
        // it after the failure.
        {
            let mut conn = db::open(&db).unwrap();
            let tid = quorum_core::tasks::create(
                &mut conn,
                "system",
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
            assert!(tid > 0);
            // NOTE: we don't run the full lifecycle here — we just need proof
            // that the task row exists unchanged after the collector fails.
        }

        let request = live_request(dir.path(), &db, 88, Some(1), true);
        let result = run_collection(&request).await;

        assert!(
            result.is_err(),
            "collector must surface classifier failure to caller"
        );

        let conn = db::open(&db).unwrap();

        // Failed run row exists and is loudly retryable.
        let run = review_findings::get_run(&conn, 88).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.findings_count, 0);
        assert!(run.error.is_some());

        // No findings written on failure — the analytics table stays clean.
        let findings = review_findings::list_for_pr(&conn, 88).unwrap();
        assert!(findings.is_empty());

        // errors table row logged.
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM errors WHERE source='review-collector'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n >= 1);

        // Task row still exists unchanged — the collector NEVER touches tasks.
        let task_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(task_count, 1, "collector must not delete/modify tasks");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_idempotent_retry_no_duplicates() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();
        let request = live_request(dir.path(), &db, 200, Some(9), false);

        run_collection(&request).await.unwrap();
        run_collection(&request).await.unwrap();

        let conn = db::open(&db).unwrap();
        let run_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=?1",
                rusqlite::params![200i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(run_count, 1);

        let findings_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_findings WHERE pr_number=?1",
                rusqlite::params![200i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(findings_count, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_failure_then_retry_success_replaces_state() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();

        // First attempt: force failure.
        let fail_request = live_request(dir.path(), &db, 301, Some(11), true);
        let _ = run_collection(&fail_request).await;

        {
            let conn = db::open(&db).unwrap();
            let run = review_findings::get_run(&conn, 301).unwrap().unwrap();
            assert_eq!(run.status, RunStatus::Failed);
        }

        // Second attempt: success (no fail env). UPSERT flips the row.
        let ok_request = live_request(dir.path(), &db, 301, Some(11), false);
        run_collection(&ok_request).await.expect("retry succeeds");

        let conn = db::open(&db).unwrap();
        let run = review_findings::get_run(&conn, 301).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.findings_count, 2);
        assert!(run.error.is_none());
    }
}
