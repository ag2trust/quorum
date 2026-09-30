//! Doctor agent — config-gated troubleshooter for non-deterministic stalls.
//!
//! When `doctor_enabled = true`, the daemon spawns a one-shot doctor agent for
//! tasks that are stalled with no active worker or reviewer and no quorum-side
//! error. The doctor investigates via quorum CLI commands and posts findings
//! to the mailbox + a report file. Default OFF — the deterministic ladder
//! (stir → reap → terminal+notify) is the complete behavior when disabled.

use super::agent::{self, AgentProc, AgentSpec};
use super::{log, stream};
use quorum_core::journal::JournalEntry;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DOCTOR_MODEL: &str = "claude-sonnet-4-20250514";
pub const DOCTOR_EFFORT: &str = "medium";

/// Journal role for the doctor process. Restart recovery kills and deletes
/// these rows; they never enter worker/reviewer task recovery.
pub const JOURNAL_ROLE: &str = "doctor";

/// Upper bound on the whole doctor reap — kill, reap, confirmation, and
/// journal delete — matching the restart-recovery reap timeout, so no exit
/// path (including the exit-75 self-update) waits longer on the doctor.
const REAP_TIMEOUT: Duration = Duration::from_secs(5);

/// In-flight doctor state, persisted across ticks.
pub struct DoctorSlot {
    pub proc: AgentProc,
    pub task_id: i64,
    pub session_id: String,
    /// Spawn-time pid (= process group). Captured once because the child
    /// handle forgets its pid after `try_wait` reaps an exited leader.
    pub pid: Option<i32>,
    pub response_text: String,
}

/// Stable journal key for the doctor investigating `task_id`.
/// The prefix is reserved in the name pool (`names::is_reserved`), so no
/// worker/reviewer identity can share this `journal.agent` primary key.
pub fn journal_agent(task_id: i64) -> String {
    format!("{}{task_id}", super::names::RESERVED_DOCTOR_PREFIX)
}

/// Journal row that lets restart recovery kill an orphaned doctor. The task
/// is carried only in the agent key: `task_id` stays NULL so task-keyed
/// journal readers never mistake the doctor for the task's worker/reviewer.
pub fn journal_entry(slot: &DoctorSlot) -> JournalEntry {
    JournalEntry {
        agent: journal_agent(slot.task_id),
        role: JOURNAL_ROLE.into(),
        task_id: None,
        session_id: slot.session_id.clone(),
        worktree: None,
        branch: None,
        phase: JOURNAL_ROLE.into(),
        cost_tokens: 0,
        agent_state: None,
        cost_usd: 0.0,
        log_dir: None,
        pid: slot.pid,
        pr: None,
        rework_count: 0,
        provider: Some(super::runner::AgentKind::Claude.to_string()),
        continuation_id: None,
        local_branch: None,
    }
}

/// Kill and reap the in-flight doctor's process group, then delete its
/// journal row. Reap-once: the slot is taken, so later calls are no-ops.
///
/// Fail-safe and bounded by [`REAP_TIMEOUT`] end to end: the row — restart
/// recovery's only evidence of the process group — is deleted only after the
/// group is confirmed dead, and a delete still pending at the deadline is
/// abandoned rather than awaited. The delete is guarded by the exact session
/// and pid, so it never removes a row another daemon wrote after a takeover.
pub async fn reap_doctor_slot(db_path: &Path, doctor_slot: &mut Option<DoctorSlot>) {
    reap_doctor_slot_within(db_path, doctor_slot, REAP_TIMEOUT).await;
}

async fn reap_doctor_slot_within(
    db_path: &Path,
    doctor_slot: &mut Option<DoctorSlot>,
    bound: Duration,
) {
    let Some(DoctorSlot {
        proc,
        task_id,
        session_id,
        pid,
        ..
    }) = doctor_slot.take()
    else {
        return;
    };
    let deadline = tokio::time::Instant::now() + bound;
    // On expiry the dropped `AgentProc` SIGKILLs the group again (Drop guard).
    let reaped = tokio::time::timeout_at(deadline, proc.kill_and_reap())
        .await
        .is_ok();
    let confirmed = reaped
        && match pid {
            Some(pgid) => process_group_gone_by(pgid, deadline).await,
            None => true,
        };
    if !confirmed {
        log(&format!(
            "doctor: process group of task #{task_id} not confirmed dead within {}s — \
             keeping its journal row for restart recovery",
            bound.as_secs()
        ));
        return;
    }
    let agent = journal_agent(task_id);
    let path = db_path.to_path_buf();
    let delete = tokio::task::spawn_blocking(move || -> quorum_core::error::Result<()> {
        let mut conn = quorum_core::db::open(&path)?;
        let tx = quorum_core::db::begin_immediate(&mut conn)?;
        tx.execute(
            "DELETE FROM journal WHERE agent=?1 AND role=?2 AND session_id=?3 AND pid IS ?4",
            rusqlite::params![agent, JOURNAL_ROLE, session_id, pid],
        )?;
        tx.commit()?;
        Ok(())
    });
    match tokio::time::timeout_at(deadline, delete).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => log(&format!(
            "doctor: journal delete for task #{task_id} failed: {error}"
        )),
        Ok(Err(error)) => log(&format!(
            "doctor: journal delete for task #{task_id} join failed: {error}"
        )),
        Err(_) => log(&format!(
            "doctor: journal delete for task #{task_id} still pending at the {}s bound — \
             not waiting; restart recovery removes the row if it never lands",
            bound.as_secs()
        )),
    }
}

/// Poll until no process remains in `pgid` or `deadline` passes. SIGKILLed
/// descendants are reparented and reaped asynchronously, so absence is only
/// confirmed by `ESRCH`.
async fn process_group_gone_by(pgid: i32, deadline: tokio::time::Instant) -> bool {
    if pgid <= 0 {
        return false;
    }
    loop {
        let gone = unsafe { libc::killpg(pgid, 0) } != 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        if gone {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Evidence bundle passed to the doctor's prompt.
pub struct EvidenceBundle {
    pub task_id: i64,
    pub task_title: String,
    pub task_status: String,
    pub task_body: Option<String>,
    pub author: Option<String>,
    pub pr: Option<i64>,
    pub worktree_path: Option<String>,
    pub repo: String,
}

/// Build the doctor's user turn from an evidence bundle.
pub fn doctor_turn(evidence: &EvidenceBundle) -> String {
    let mut prompt = String::with_capacity(2048);

    prompt.push_str("You are a doctor agent — a one-shot troubleshooter for a stalled task.\n\n");
    prompt.push_str("## Situation\n\n");
    prompt.push_str(&format!(
        "Task #{} is stalled in status `{}` with no active worker or reviewer.\n",
        evidence.task_id, evidence.task_status
    ));
    prompt.push_str(&format!("Title: {}\n", evidence.task_title));
    if let Some(body) = &evidence.task_body {
        let end = body
            .char_indices()
            .map(|(i, _)| i)
            .nth(500)
            .unwrap_or(body.len());
        let truncated = &body[..end];
        prompt.push_str(&format!("Body (truncated): {truncated}\n"));
    }
    if let Some(author) = &evidence.author {
        prompt.push_str(&format!("Author: {author}\n"));
    }
    if let Some(pr) = evidence.pr {
        prompt.push_str(&format!("PR: #{pr}\n"));
    }
    if let Some(wt) = &evidence.worktree_path {
        prompt.push_str(&format!("Worktree: {wt}\n"));
    }
    prompt.push_str(&format!("Repo: {}\n", evidence.repo));

    prompt.push_str("\n## Your job\n\n");
    prompt.push_str(&format!(
        "1. Run `quorum task-get --task-id {} --json` to get current task state.\n",
        evidence.task_id
    ));
    prompt.push_str("2. Run `quorum status --json` to see daemon/agent state.\n");
    prompt.push_str("3. Check if there's a worktree or branch still present.\n");
    prompt.push_str("4. Look at recent quorum events for this task.\n");
    prompt.push_str("5. Diagnose why the task is stuck (common causes: worktree gone, ");
    prompt.push_str("branch deleted, PR closed externally, agent crashed without cleanup).\n\n");

    prompt.push_str("## Output contract\n\n");
    prompt.push_str("- Post your findings via: `quorum post --agent Doctor --body-file <path>`\n");
    prompt.push_str("- If you can fix the stall, do so via quorum commands ");
    prompt.push_str("(task-update, react, post). Do NOT edit code.\n");
    prompt.push_str("- Write a report to: ~/.quorum/reports/ as ");
    prompt.push_str(&format!(
        "`$(date +%Y-%m-%d)-task{}.md`\n",
        evidence.task_id
    ));
    prompt.push_str("- This is a single turn — do your investigation and report, then stop.\n");

    agent::user_turn(&prompt)
}

/// Spawn a doctor agent. Runs in repo_dir (not a worktree — read-only investigation).
pub fn spawn_doctor(
    task_id: i64,
    repo_dir: &Path,
    agent_bin: Option<&str>,
    bare: bool,
    allowed_tools: &str,
    repo: &str,
) -> std::io::Result<DoctorSlot> {
    let session_id = agent::new_session_id();
    let spec = AgentSpec {
        kind: super::runner::AgentKind::Claude,
        model: DOCTOR_MODEL.to_string(),
        effort: DOCTOR_EFFORT.to_string(),
        session_id: session_id.clone(),
        worktree: repo_dir.to_path_buf(),
        bare,
        allowed_tools: allowed_tools.to_string(),
        env_vars: vec![
            ("QUORUM_REPO".into(), repo.into()),
            ("QUORUM_AGENT".into(), "Doctor".into()),
        ],
    };

    let proc = AgentProc::spawn(&spec, agent_bin)?;
    let pid = proc.pid();

    Ok(DoctorSlot {
        proc,
        task_id,
        session_id,
        pid,
        response_text: String::new(),
    })
}

/// Drain events from the doctor (non-blocking, bounded). Returns Some when done.
pub async fn drain_doctor_events(slot: &mut DoctorSlot) -> Option<DoctorResult> {
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), slot.proc.next_event()).await
    {
        match &event {
            stream::Event::Result {
                result, is_error, ..
            } => {
                if is_error.unwrap_or(false) {
                    return Some(DoctorResult::Error("doctor agent returned an error".into()));
                }
                let text = result
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| result.to_string());
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(DoctorResult::Done(slot.response_text.clone()));
            }
            stream::Event::Assistant { message } => {
                if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                    slot.response_text.push_str(content);
                }
            }
            _ => {}
        }
    }
    None
}

pub enum DoctorResult {
    Done(String),
    Error(String),
}

/// Reports dir under quorum home (~/.quorum/reports/).
/// Called by the doctor agent via Bash, not directly from daemon code.
#[allow(dead_code)]
pub fn ensure_reports_dir() -> std::io::Result<PathBuf> {
    let base = crate::paths::home_dir().map_err(|e| std::io::Error::other(e.to_string()))?;
    let dir = base.join("reports");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_turn_contains_task_id() {
        let evidence = EvidenceBundle {
            task_id: 42,
            task_title: "fix the widget".into(),
            task_status: "working".into(),
            task_body: Some("needs investigation".into()),
            author: Some("Bolt-1".into()),
            pr: Some(99),
            worktree_path: Some("/tmp/wt".into()),
            repo: "test/repo".into(),
        };
        let turn = doctor_turn(&evidence);
        assert!(turn.contains("42"), "turn should reference task id");
        assert!(turn.contains("fix the widget"), "turn should include title");
        assert!(turn.contains("working"), "turn should include status");
    }

    #[test]
    fn doctor_turn_handles_missing_optionals() {
        let evidence = EvidenceBundle {
            task_id: 1,
            task_title: "test".into(),
            task_status: "in-review".into(),
            task_body: None,
            author: None,
            pr: None,
            worktree_path: None,
            repo: "test/repo".into(),
        };
        let turn = doctor_turn(&evidence);
        assert!(turn.contains("Task #1"), "should include task id");
        assert!(!turn.contains("Author:"), "no author when None");
    }

    #[test]
    fn ensure_reports_dir_creates_path() {
        let dir = ensure_reports_dir();
        assert!(dir.is_ok());
        assert!(dir.unwrap().ends_with("reports"));
    }

    // ── Doctor process ownership: journal + reap on every exit path ─────

    use quorum_core::journal;

    fn write_executable(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Fake Claude binary: leaves a descendant in its process group, then
    /// either stays alive (`live`) or exits leaving the descendant behind.
    fn fake_doctor(dir: &Path, live: bool) -> PathBuf {
        let bin = dir.join(if live { "doctor-live" } else { "doctor-exit" });
        let tail = if live { "exec sleep 300" } else { "exit 0" };
        write_executable(&bin, &format!("#!/bin/sh\nsleep 300 &\n{tail}\n"));
        bin
    }

    fn group_alive(pgid: i32) -> bool {
        unsafe { libc::killpg(pgid, 0) == 0 }
    }

    /// SIGKILLed descendants are reparented and reaped asynchronously; allow
    /// a bounded window for the group to disappear.
    async fn group_gone(pgid: i32) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while group_alive(pgid) {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        true
    }

    fn doctor_rows(db: &Path) -> Vec<JournalEntry> {
        let conn = quorum_core::db::open(db).unwrap();
        journal::list_in_flight(&conn)
            .unwrap()
            .into_iter()
            .filter(|entry| entry.role == JOURNAL_ROLE)
            .collect()
    }

    async fn journaled_doctor(dir: &Path, db: &Path, live: bool) -> Option<DoctorSlot> {
        let bin = fake_doctor(dir, live);
        let slot = spawn_doctor(7, dir, bin.to_str(), true, "Bash", "owner/repo").unwrap();
        super::super::persist_worker_journal(db, journal_entry(&slot))
            .await
            .unwrap();
        Some(slot)
    }

    #[tokio::test]
    async fn journal_entry_is_task_neutral_doctor_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, true).await;
        let pid = slot.as_ref().unwrap().proc.pid().unwrap();

        let rows = doctor_rows(&db);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].agent, "doctor-7");
        assert!(
            super::super::names::is_reserved(&rows[0].agent),
            "no pooled agent identity may share the doctor's journal key"
        );
        assert_eq!(rows[0].pid, Some(pid));
        assert_eq!(rows[0].task_id, None, "task-keyed readers must ignore it");
        assert_eq!(rows[0].worktree, None, "repo_dir is not a managed worktree");
        assert_eq!(
            rows[0].session_id,
            slot.as_ref().unwrap().session_id,
            "journal carries the spawn session"
        );

        reap_doctor_slot(&db, &mut slot).await;
    }

    #[tokio::test]
    async fn reap_without_doctor_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("never-opened.db");
        let mut slot = None;
        reap_doctor_slot(&db, &mut slot).await;
        assert!(!db.exists(), "an empty slot must not touch the database");
    }

    #[tokio::test]
    async fn reap_kills_live_doctor_group_and_deletes_row_once() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, true).await;
        let pgid = slot.as_ref().unwrap().proc.pid().unwrap();
        assert!(group_alive(pgid));

        reap_doctor_slot(&db, &mut slot).await;
        assert!(slot.is_none(), "reap takes the slot");
        assert!(group_gone(pgid).await, "doctor process group must be dead");
        assert!(
            doctor_rows(&db).is_empty(),
            "doctor journal row must be gone"
        );

        // Reap-once: a second exit path finds nothing to do.
        reap_doctor_slot(&db, &mut slot).await;
    }

    #[tokio::test]
    async fn reap_kills_descendants_of_exited_unreaped_doctor() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, false).await;
        let pgid = slot.as_ref().unwrap().proc.pid().unwrap();
        // The leader exits, but its descendant keeps the group alive.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !matches!(slot.as_mut().unwrap().proc.try_wait(), Ok(Some(_))) {
            assert!(std::time::Instant::now() < deadline, "leader never exited");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(group_alive(pgid), "descendant outlives the leader");

        reap_doctor_slot(&db, &mut slot).await;
        assert!(group_gone(pgid).await, "descendant must be killed");
        assert!(doctor_rows(&db).is_empty());
    }

    #[tokio::test]
    async fn reap_row_delete_is_guarded_by_exact_session_and_pid() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, true).await;
        // A daemon that took over this DB journaled its own doctor for the
        // same task under the same key.
        let mut successor = journal_entry(slot.as_ref().unwrap());
        successor.session_id = "successor-session".into();
        successor.pid = Some(i32::MAX);
        {
            let mut conn = quorum_core::db::open(&db).unwrap();
            journal::upsert(&mut conn, &successor).unwrap();
        }

        reap_doctor_slot(&db, &mut slot).await;
        let rows = doctor_rows(&db);
        assert_eq!(rows.len(), 1, "the successor's row must survive");
        assert_eq!(rows[0].session_id, "successor-session");
    }

    #[tokio::test]
    async fn reap_against_too_new_schema_still_kills_within_bound() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, true).await;
        let pgid = slot.as_ref().unwrap().proc.pid().unwrap();
        bump_schema(&db);

        let started = std::time::Instant::now();
        reap_doctor_slot(&db, &mut slot).await;
        assert!(
            started.elapsed() < REAP_TIMEOUT,
            "exit-75 reap must stay bounded"
        );
        assert!(group_gone(pgid).await, "doctor process group must be dead");
    }

    /// Slow reap path: a descendant that leaves the doctor's process group
    /// but keeps its stdout open means `kill_and_reap` never sees EOF. The
    /// helper must give up at the bound and keep the row as restart evidence.
    #[tokio::test]
    async fn unconfirmed_reap_keeps_recovery_row_within_bound() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let escaped_pid = dir.path().join("escaped.pid");
        let bin = dir.path().join("doctor-escape");
        write_executable(
            &bin,
            &format!(
                "#!/bin/sh\nperl -e 'setpgrp(0,0); open(my $f, \">\", $ARGV[0]) or die; \
                 print $f $$; close $f; sleep 300' '{}' &\nexec sleep 300\n",
                escaped_pid.display()
            ),
        );
        let slot = spawn_doctor(7, dir.path(), bin.to_str(), true, "Bash", "owner/repo").unwrap();
        super::super::persist_worker_journal(&db, journal_entry(&slot))
            .await
            .unwrap();
        let mut slot = Some(slot);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let escaped: i32 = loop {
            if let Some(pid) = std::fs::read_to_string(&escaped_pid)
                .ok()
                .and_then(|pid| pid.trim().parse().ok())
            {
                break pid;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "escapee never started"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        let started = std::time::Instant::now();
        reap_doctor_slot_within(&db, &mut slot, Duration::from_secs(1)).await;
        let elapsed = started.elapsed();
        unsafe { libc::kill(escaped, libc::SIGKILL) };
        assert!(slot.is_none(), "reap still takes the slot");
        assert!(
            elapsed < Duration::from_secs(3),
            "reap overran its bound: {elapsed:?}"
        );
        assert_eq!(
            doctor_rows(&db).len(),
            1,
            "an unconfirmed reap must keep the journal row for restart recovery"
        );
    }

    /// Slow delete path: a writer holding the DB lock must not hold the exit
    /// past the bound once the group is confirmed dead.
    #[tokio::test]
    async fn blocked_journal_delete_is_abandoned_at_bound() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("q.db");
        let mut slot = journaled_doctor(dir.path(), &db, true).await;
        let pgid = slot.as_ref().unwrap().pid.unwrap();
        let blocker = quorum_core::db::open(&db).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let started = std::time::Instant::now();
        reap_doctor_slot_within(&db, &mut slot, Duration::from_secs(1)).await;
        let elapsed = started.elapsed();
        blocker.execute_batch("ROLLBACK").unwrap();
        drop(blocker);
        assert!(
            elapsed < Duration::from_millis(2500),
            "blocked delete held the exit for {elapsed:?}"
        );
        assert!(group_gone(pgid).await, "doctor process group must be dead");
    }

    fn bump_schema(db: &Path) {
        let conn = rusqlite::Connection::open(db).unwrap();
        conn.pragma_update(None, "user_version", quorum_core::db::SCHEMA_VERSION + 1)
            .unwrap();
    }

    // ── Real tick loop: phase 8b spawns and journals, exit paths reap ────

    struct LoopFixture {
        dir: tempfile::TempDir,
        config: super::super::ServeConfig,
        instance: String,
    }

    fn loop_fixture() -> LoopFixture {
        let dir = tempfile::tempdir().unwrap();
        let (config, instance) = loop_config(dir.path());
        LoopFixture {
            dir,
            config,
            instance,
        }
    }

    fn loop_config(root: &Path) -> (super::super::ServeConfig, String) {
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        for args in [
            &["init", "-b", "main"][..],
            &["config", "user.email", "test@example.com"],
            &["config", "user.name", "Test"],
            &["commit", "--allow-empty", "-m", "init"],
        ] {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?} failed");
        }
        let worktree_base = root.join("worktrees");
        std::fs::create_dir_all(&worktree_base).unwrap();

        // A stalled in-review task with no PR: no Phase 5 reviewer claims it,
        // so Phase 8b selects it for the doctor.
        let db_path = root.join("quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = super::super::now_unix();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "stalled",
            None,
            0,
            None,
            Some(r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        conn.execute("UPDATE tasks SET status='in-review' WHERE id=?1", [task_id])
            .unwrap();
        let instance = quorum_core::daemon_lock::new_instance_id();
        assert_eq!(
            quorum_core::daemon_lock::try_acquire(
                &mut conn,
                std::process::id() as i64,
                &instance,
                now,
                30,
            )
            .unwrap(),
            quorum_core::daemon_lock::AcquireResult::Acquired,
        );
        drop(conn);

        let sentinel = root.join("sentinel");
        std::fs::write(&sentinel, "").unwrap();
        let bin = fake_doctor(root, true);
        let profile = crate::serve_config::ModelProfile {
            runner: "claude".into(),
            model: "claude-sonnet-5".into(),
            effort: "high".into(),
        };
        let pool = std::collections::BTreeMap::from([("test".to_string(), 100)]);
        let config = super::super::ServeConfig {
            db_path,
            cap: 1,
            model_profiles: std::collections::BTreeMap::from([("test".to_string(), profile)]),
            routing: crate::serve_config::RoutingPolicy {
                classifier: pool.clone(),
                planner: pool.clone(),
                arbiter: pool.clone(),
                collector: pool.clone(),
                worker: (1..=5)
                    .map(|level| (level.to_string(), pool.clone()))
                    .collect(),
                reviewer: (1..=5)
                    .map(|level| (level.to_string(), pool.clone()))
                    .collect(),
            },
            repo_dir: repo,
            worktree_base,
            names_file: None,
            agent_bin: Some(bin.to_string_lossy().into_owned()),
            merge_executor: std::sync::Arc::new(super::super::merge::CommandMergeExecutor {
                command: "true".into(),
                checks_cmd: None,
                mergeability_cmd: None,
            }),
            bare_agent: true,
            limits: super::super::CostLimits::default(),
            log_dir: None,
            self_update_drain: false,
            drain_timeout_secs: 1,
            self_repo: None,
            sha_poll_interval_secs: 60,
            merge_checks_timeout_secs: 1,
            merge_checks_poll_secs: 1,
            repo: "owner/repo".into(),
            base_branch: "main".into(),
            self_update_branch: "main".into(),
            exit_when_gone: Some(sentinel),
            required_jobs: Vec::new(),
            master_ci_gate: false,
            master_ci_timeout_secs: 1,
            allowed_tools: None,
            doctor_enabled: true,
            resource_monitor: crate::resource_health::ResourceMonitorConfig::default(),
            r2_enabled: false,
            r2_target_per_stratum: 0,
            r2_steady_state_p: 0.0,
            max_rework: quorum_core::lifecycle::REWORK_CAP,
            codex_sandbox: "danger-full-access".into(),
            grok: Default::default(),
            pr_target_program: None,
        };
        (config, instance)
    }

    async fn wait_for_doctor_row(db: &Path) -> JournalEntry {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(row) = doctor_rows(db).pop() {
                return row;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "phase 8b never journaled a doctor"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Run the real tick loop until phase 8b journals a live doctor, fire
    /// `trigger`, and return the loop's result with the doctor's pgid.
    async fn run_loop_and_trigger(
        fixture: &LoopFixture,
        trigger: impl FnOnce(&LoopFixture),
    ) -> (quorum_core::error::Result<i32>, i32) {
        let exit_loop = super::super::tick_loop(
            &fixture.config,
            std::process::id() as i64,
            fixture.instance.clone(),
            super::super::planner::WritablePathResolver::default(),
        );
        let driver = async {
            let row = wait_for_doctor_row(&fixture.config.db_path).await;
            let pgid = row.pid.expect("doctor pid is journaled");
            assert!(group_alive(pgid), "journaled doctor is live");
            trigger(fixture);
            pgid
        };
        let (result, pgid) = tokio::time::timeout(Duration::from_secs(120), async {
            tokio::join!(exit_loop, driver)
        })
        .await
        .expect("tick loop must exit after the trigger");
        (result, pgid)
    }

    /// The full tick-loop future is too deep for the default test stack.
    fn on_big_stack(body: impl std::future::Future<Output = ()> + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(body)
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn exit_when_gone_reaps_journaled_doctor() {
        on_big_stack(exit_when_gone_body());
    }

    async fn exit_when_gone_body() {
        let fixture = loop_fixture();
        let (result, pgid) = run_loop_and_trigger(&fixture, |fixture| {
            std::fs::remove_file(fixture.config.exit_when_gone.as_ref().unwrap()).unwrap();
        })
        .await;
        assert_eq!(result.unwrap(), 1);
        assert!(group_gone(pgid).await, "doctor process group must be dead");
        assert!(doctor_rows(&fixture.config.db_path).is_empty());
        drop(fixture.dir);
    }

    #[test]
    fn lock_stolen_reaps_journaled_doctor() {
        on_big_stack(lock_stolen_body());
    }

    async fn lock_stolen_body() {
        let fixture = loop_fixture();
        let (result, pgid) = run_loop_and_trigger(&fixture, |fixture| {
            let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            conn.execute("UPDATE daemon_lock SET instance_id='thief' WHERE id=1", [])
                .unwrap();
        })
        .await;
        assert_eq!(result.unwrap(), 1);
        assert!(group_gone(pgid).await, "doctor process group must be dead");
        assert!(doctor_rows(&fixture.config.db_path).is_empty());
    }

    /// A too-new schema surfaces either through the ExitSelfUpdate arm (exit
    /// 75) or as a propagated `SchemaTooNew` that `serve` maps to exit 75;
    /// both must leave no doctor process. The journal row cannot be deleted
    /// through a too-new schema, so restart recovery removes it.
    #[test]
    fn schema_too_new_exit_reaps_doctor_group() {
        on_big_stack(schema_too_new_body());
    }

    async fn schema_too_new_body() {
        let fixture = loop_fixture();
        let (result, pgid) = run_loop_and_trigger(&fixture, |fixture| {
            bump_schema(&fixture.config.db_path);
        })
        .await;
        match result {
            Ok(code) => assert_eq!(code, super::super::EXIT_SELF_UPDATE),
            Err(error) => assert!(
                matches!(error, quorum_core::error::QuorumError::SchemaTooNew { .. }),
                "{error}"
            ),
        }
        assert!(group_gone(pgid).await, "doctor process group must be dead");
    }

    /// Configured custom names share `journal.agent` with the doctor key, so
    /// a names file naming a doctor key must refuse to start the daemon
    /// before any agent — and any journal row — exists.
    #[test]
    fn reserved_names_file_refuses_daemon_start() {
        on_big_stack(reserved_names_body());
    }

    async fn reserved_names_body() {
        let mut fixture = loop_fixture();
        let names = fixture.dir.path().join("names.txt");
        std::fs::write(&names, "Alpha\ndoctor-1\nGamma\n").unwrap();
        fixture.config.names_file = Some(names);
        let error = super::super::tick_loop(
            &fixture.config,
            std::process::id() as i64,
            fixture.instance.clone(),
            super::super::planner::WritablePathResolver::default(),
        )
        .await
        .expect_err("a reserved configured name must refuse startup");
        assert!(error.to_string().contains("reserved name"), "{error}");
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        assert!(journal::list_in_flight(&conn).unwrap().is_empty());
    }

    const SIGNAL_CHILD_ROOT: &str = "QUORUM_DOCTOR_SIGNAL_CHILD_ROOT";

    /// Child half of the signal test: the daemon's signal handlers are
    /// process-wide, so the loop runs in its own test process.
    #[test]
    #[ignore = "spawned by signal_shutdown_reaps_journaled_doctor"]
    fn signal_shutdown_child() {
        let Some(root) = std::env::var_os(SIGNAL_CHILD_ROOT).map(PathBuf::from) else {
            return;
        };
        on_big_stack(async move {
            let (config, instance) = loop_config(&root);
            std::fs::write(root.join("ready"), "").unwrap();
            let exit = super::super::tick_loop(
                &config,
                std::process::id() as i64,
                instance,
                super::super::planner::WritablePathResolver::default(),
            )
            .await
            .unwrap();
            assert_eq!(exit, 0, "signal with no in-flight agents exits 0");
        });
    }

    #[test]
    fn signal_shutdown_reaps_journaled_doctor() {
        on_big_stack(signal_shutdown_body());
    }

    async fn signal_shutdown_body() {
        let root = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "serve::doctor::tests::signal_shutdown_child",
                "--ignored",
                "--test-threads=1",
            ])
            .env(SIGNAL_CHILD_ROOT, root.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        struct KillOnDrop(u32);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                unsafe { libc::kill(self.0 as i32, libc::SIGKILL) };
            }
        }
        let _guard = KillOnDrop(child.id());

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while !root.path().join("ready").exists() {
            assert!(std::time::Instant::now() < deadline, "child never started");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let db = root.path().join("quorum.db");
        let pgid = wait_for_doctor_row(&db)
            .await
            .pid
            .expect("doctor pid is journaled");
        assert!(group_alive(pgid), "journaled doctor is live");

        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "daemon ignored the shutdown signal"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(status.success(), "child daemon test failed: {status:?}");
        assert!(group_gone(pgid).await, "doctor process group must be dead");
        assert!(
            doctor_rows(&db).is_empty(),
            "doctor journal row must be gone"
        );
    }
}
