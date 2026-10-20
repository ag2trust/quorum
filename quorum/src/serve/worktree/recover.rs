//! Worker-work preservation ("recover refs").
//!
//! Every daemon teardown of a worker worktree force-removes the checkout and
//! most paths also delete the local branch. Workers commit but never push, so
//! both uncommitted edits and committed-but-unpushed commits would be lost.
//! Before any such destruction the daemon snapshots the work into a
//! daemon-owned ref namespace:
//!
//! ```text
//! refs/quorum-recover/t<task_id>/<unix_millis:013>/<agent>/<reason>
//! ```
//!
//! The namespace is outside `refs/heads/` so `git branch -D`, branch sweeps,
//! and ordinary pushes never touch it, and outside `recover/*` branches an
//! operator may create by hand. Any ref keeps its objects reachable after the
//! worktree is pruned.
//!
//! Trust: managed workers share this repository's ref store and can write any
//! ref, including this namespace. A ref is therefore never authority by name.
//! The serving layer records every ref this module creates (`recover_refs`
//! table) and passes only recorded refs back in as `trusted`; resume
//! re-verifies that the ref still names the recorded SHA (and, for a
//! snapshot, the recorded parent and daemon identity). Unrecorded refs are
//! never resumed and never count as preserving a worker's commits.
//!
//! The snapshot uses plumbing only (temporary `GIT_INDEX_FILE`, `add`,
//! `write-tree`, `commit-tree`, `update-ref`): no hooks run, the worktree's
//! real index and files are untouched, and the ref is written last with a
//! create-only CAS. A daemon that dies mid-snapshot leaves the worktree intact
//! and no partial ref; the next teardown attempt simply snapshots again.

use super::{git_diagnostic, run_git, WorktreeManager};
use std::path::Path;

pub(crate) const RECOVER_REF_ROOT: &str = "refs/quorum-recover";
/// Bounded retention: newest recorded snapshots kept per task.
pub(crate) const RECOVER_REFS_PER_TASK: usize = 2;
/// Unattributed refs are never resumed; keep a small bounded tail for
/// manual recovery.
const UNATTRIBUTED_KEEP: usize = 8;
const RECOVER_COMPONENT_MAX: usize = 48;
const UNATTRIBUTED_BUCKET: &str = "unattributed";
const SNAPSHOT_IDENTITY_NAME: &str = "quorum-daemon";
const SNAPSHOT_IDENTITY_EMAIL: &str = "quorum-daemon@localhost";
const SNAPSHOT_SUBJECT_PREFIX: &str = "quorum recover snapshot";

/// Who and why a worktree is being torn down. `task_id` selects the bucket
/// and must come from daemon-controlled state, never from worker-writable
/// data such as the checked-out branch name. `trusted` lists the recorded
/// recover refs of that task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverIdentity {
    pub task_id: Option<i64>,
    pub agent: String,
    pub reason: String,
    pub trusted: Vec<RecoverRef>,
}

impl RecoverIdentity {
    pub fn new(task_id: i64, agent: &str, reason: &str) -> Self {
        Self {
            task_id: Some(task_id),
            agent: agent.to_string(),
            reason: reason.to_string(),
            trusted: Vec::new(),
        }
    }

    pub fn with_trusted(mut self, trusted: Vec<RecoverRef>) -> Self {
        self.trusted = trusted;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverRef {
    pub refname: String,
    pub sha: String,
    /// `Some(parent)` when `sha` is a daemon snapshot commit of uncommitted
    /// state on top of `parent`; `None` for pinned worker commits.
    pub snapshot_parent: Option<String>,
    /// Untracked files could not be captured; only tracked changes are in it.
    pub tracked_only: bool,
    /// Why untracked files were skipped (only on a freshly created ref).
    pub untracked_skip: Option<String>,
}

impl RecoverRef {
    pub fn listed(refname: &str, sha: &str) -> Self {
        Self {
            refname: refname.to_string(),
            sha: sha.to_string(),
            snapshot_parent: None,
            tracked_only: false,
            untracked_skip: None,
        }
    }

    /// `(agent, reason)` encoded in the ref leaf.
    pub fn provenance(&self) -> (String, String) {
        let mut parts = self.refname.rsplit('/');
        let reason = parts.next().unwrap_or("unknown").to_string();
        let agent = parts.next().unwrap_or("unknown").to_string();
        (agent, reason)
    }
}

/// Why a release did not complete. `Preserve` means nothing was destroyed:
/// the worktree and branch are left for a later attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    Preserve(String),
    Remove(String),
}

impl std::fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReleaseError::Preserve(e) => write!(f, "work preservation failed (worktree kept): {e}"),
            ReleaseError::Remove(e) => write!(f, "worktree release failed: {e}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverResume {
    /// No recorded recover ref exists for the task.
    None,
    /// The newest recover ref is already contained in the provisioning start.
    AlreadyIncorporated(RecoverRef),
    /// The provisioning start now points at the recover ref's commits. When
    /// `restore_uncommitted`, the snapshot's uncommitted changes must still be
    /// restored into the provisioned worktree as working-tree changes.
    Resumed {
        recover: RecoverRef,
        previous: Option<String>,
        restore_uncommitted: bool,
    },
    /// The recover ref does not descend from the provisioning start; left as is.
    Stale { recover: RecoverRef, onto: String },
}

/// Task id of a recover ref (`refs/quorum-recover/t<id>/...`).
pub(crate) fn recover_ref_task_id(refname: &str) -> Option<i64> {
    let rest = refname.strip_prefix(RECOVER_REF_ROOT)?.strip_prefix("/t")?;
    let (digits, _) = rest.split_once('/')?;
    digits.parse::<i64>().ok().filter(|id| *id > 0)
}

pub(crate) fn recover_bucket(task_id: Option<i64>) -> String {
    match task_id {
        Some(id) if id > 0 => format!("{RECOVER_REF_ROOT}/t{id}"),
        _ => format!("{RECOVER_REF_ROOT}/{UNATTRIBUTED_BUCKET}"),
    }
}

fn recover_component(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(RECOVER_COMPONENT_MAX)
        .collect();
    let cleaned = cleaned.trim_matches(|c| c == '-' || c == '_').to_string();
    if cleaned.is_empty() {
        "unknown".into()
    } else {
        cleaned
    }
}

/// Attribute a worktree to a task using only daemon-controlled provisioning
/// records `(task_id, branch, worktree)`: the registered branch AND the path
/// must both equal one record. Directory and branch names are worker-forgeable
/// (`git worktree add <root>/<x>-t<id> -b daemon/<x>-t<id>`), so no name-based
/// shortcut exists. No match means unattributed (never trusted or resumed).
pub(crate) fn daemon_attribution(
    path: &Path,
    registered_branch: Option<&str>,
    allocations: &[(i64, String, String)],
) -> Option<i64> {
    let branch = registered_branch?.strip_prefix("refs/heads/")?;
    let canonical = std::fs::canonicalize(path).ok()?;
    allocations
        .iter()
        .find(|(_, allocated_branch, worktree)| {
            allocated_branch == branch
                && std::fs::canonicalize(worktree).ok().as_ref() == Some(&canonical)
        })
        .map(|(task_id, _, _)| *task_id)
        .filter(|task_id| *task_id > 0)
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

impl WorktreeManager {
    /// Snapshot any work that destroying `worktree_dir` and/or deleting
    /// `deleting_branch` would lose. Returns the refs created (empty when the
    /// worktree is clean and nothing is unique to the branch, or when an
    /// identical trusted snapshot already exists). Caller MUST hold
    /// `self.lock`.
    pub(super) async fn preserve_unlocked(
        &self,
        repo_dir: &Path,
        worktree_dir: Option<&Path>,
        deleting_branch: Option<&str>,
        identity: &RecoverIdentity,
    ) -> Result<Vec<RecoverRef>, String> {
        let bucket = recover_bucket(identity.task_id);
        // (commit, snapshot_parent, untracked_skip)
        let mut candidates: Vec<(String, Option<String>, Option<String>)> = Vec::new();
        let mut worktree_head = None;

        if let Some(worktree) = worktree_dir {
            if self.is_worktree_root_unlocked(worktree).await? {
                let head = self
                    .rev_parse_unlocked(worktree, "HEAD^{commit}")
                    .await?
                    .ok_or_else(|| {
                        format!("worktree {} has no resolvable HEAD", worktree.display())
                    })?;
                let head_tree = self
                    .rev_parse_unlocked(worktree, "HEAD^{tree}")
                    .await?
                    .ok_or_else(|| "worktree HEAD tree is unresolvable".to_string())?;
                let (tree, untracked_skip) = self.snapshot_tree_unlocked(worktree).await?;
                if tree != head_tree {
                    if !self
                        .trusted_snapshot_exists_unlocked(repo_dir, &identity.trusted, &tree, &head)
                        .await?
                    {
                        let commit = self
                            .commit_snapshot_unlocked(
                                repo_dir,
                                &tree,
                                &head,
                                identity,
                                untracked_skip.is_some(),
                            )
                            .await?;
                        candidates.push((commit, Some(head.clone()), untracked_skip));
                    }
                } else {
                    candidates.push((head.clone(), None, untracked_skip));
                }
                worktree_head = Some(head);
            }
        }
        if let Some(branch) = deleting_branch {
            if let Some(tip) = self
                .rev_parse_unlocked(repo_dir, &format!("refs/heads/{branch}^{{commit}}"))
                .await?
            {
                if worktree_head.as_deref() != Some(tip.as_str()) {
                    candidates.push((tip, None, None));
                }
            }
        }

        let mut created = Vec::new();
        let millis = now_millis();
        for (index, (commit, snapshot_parent, untracked_skip)) in candidates.into_iter().enumerate()
        {
            if snapshot_parent.is_none()
                && !self
                    .commit_unique_unlocked(repo_dir, &commit, deleting_branch, &identity.trusted)
                    .await?
            {
                continue;
            }
            let tracked_only = snapshot_parent.is_some() && untracked_skip.is_some();
            let reason = if tracked_only {
                format!("{}-tracked-only", identity.reason)
            } else {
                identity.reason.clone()
            };
            let refname = format!(
                "{bucket}/{:013}/{}/{}",
                millis + index as u128,
                recover_component(&identity.agent),
                recover_component(&reason),
            );
            let mut update = self.git_cmd(repo_dir);
            update.args([
                "update-ref",
                "-m",
                SNAPSHOT_SUBJECT_PREFIX,
                &refname,
                &commit,
                "",
            ]);
            let out = run_git(
                update,
                self.local_timeout,
                "git update-ref recover snapshot",
            )
            .await?;
            if !out.status.success() {
                return Err(format!(
                    "cannot create recover ref {refname}: {}",
                    git_diagnostic(&out.stderr)
                ));
            }
            created.push(RecoverRef {
                refname,
                sha: commit,
                snapshot_parent,
                tracked_only,
                untracked_skip: if tracked_only { untracked_skip } else { None },
            });
        }
        if !created.is_empty() && identity.task_id.is_none() {
            self.prune_bucket_unlocked(repo_dir, &bucket, UNATTRIBUTED_KEEP)
                .await?;
        }
        Ok(created)
    }

    /// Preserve, then force-remove the worktree and optionally delete its
    /// branch. A preservation failure destroys nothing.
    pub async fn release_preserving(
        &self,
        repo_dir: &Path,
        worktree_dir: &Path,
        branch: &str,
        delete_branch: bool,
        identity: &RecoverIdentity,
    ) -> Result<Vec<RecoverRef>, ReleaseError> {
        let _guard = self.lock.lock().await;
        let deleting = delete_branch.then_some(branch);
        let created = self
            .preserve_unlocked(repo_dir, Some(worktree_dir), deleting, identity)
            .await
            .map_err(ReleaseError::Preserve)?;

        let mut remove_error = None;
        match std::fs::symlink_metadata(worktree_dir) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                let mut cmd = self.git_cmd(repo_dir);
                cmd.args(["worktree", "remove"])
                    .arg(worktree_dir)
                    .arg("--force");
                match run_git(cmd, self.local_timeout, "git worktree remove").await {
                    Ok(out) if out.status.success() => {}
                    Ok(out) => {
                        remove_error = Some(format!(
                            "git worktree remove failed: {}",
                            git_diagnostic(&out.stderr)
                        ))
                    }
                    Err(error) => remove_error = Some(error),
                }
            }
        }
        if let Some(branch) = deleting {
            if remove_error.is_none() {
                let mut cmd = self.git_cmd(repo_dir);
                cmd.args(["branch", "-D", branch]);
                match run_git(cmd, self.local_timeout, "git branch -D").await {
                    Ok(out) if out.status.success() => {}
                    Ok(out) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        if !stderr.contains("not found") {
                            remove_error = Some(format!("git branch -D {branch} failed: {stderr}"));
                        }
                    }
                    Err(error) => remove_error = Some(error),
                }
            }
        }
        match remove_error {
            Some(error) => Err(ReleaseError::Remove(error)),
            None => Ok(created),
        }
    }

    /// The branch ref registered for the worktree at `path`: `None` when the
    /// path is not a registered worktree, `Some(None)` when it is detached.
    pub async fn registered_branch(
        &self,
        repo_dir: &Path,
        path: &Path,
    ) -> Result<Option<Option<String>>, String> {
        let _guard = self.lock.lock().await;
        let Ok(canonical) = std::fs::canonicalize(path) else {
            return Ok(None);
        };
        Ok(self
            .list_worktrees_unlocked(repo_dir)
            .await?
            .into_iter()
            .find(|registration| {
                std::fs::canonicalize(&registration.path).ok().as_ref() == Some(&canonical)
            })
            .map(|registration| registration.branch))
    }

    /// Every ref in the recover namespace, newest first within each bucket.
    /// Includes refs the daemon did not create; callers must not trust them.
    pub async fn list_recover_refs(&self, repo_dir: &Path) -> Result<Vec<RecoverRef>, String> {
        let _guard = self.lock.lock().await;
        self.list_bucket_unlocked(repo_dir, RECOVER_REF_ROOT).await
    }

    /// Delete one recover ref if it still names `sha`. A missing ref is
    /// success; a ref moved to another SHA is left alone and reported.
    /// Refs outside the recover namespace are refused.
    pub async fn delete_recover_ref(
        &self,
        repo_dir: &Path,
        recover: &RecoverRef,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        self.delete_recover_ref_unlocked(repo_dir, recover).await
    }

    /// Point the task's local branch at a trusted recover ref's commits
    /// before a fresh worker provisions it. With an existing branch the
    /// recover ref must descend from the branch tip (fast-forward only);
    /// without one it must descend from `base_rev` (the task's allocation
    /// provenance, or the remote base for a legacy allocation). A snapshot
    /// of uncommitted state positions the branch at the snapshot's parent;
    /// the caller restores the uncommitted changes after provisioning.
    pub async fn resume_branch_from_recover(
        &self,
        repo_dir: &Path,
        recover: &RecoverRef,
        branch: &str,
        base_rev: &str,
    ) -> Result<RecoverResume, String> {
        let _guard = self.lock.lock().await;
        self.verify_trusted_unlocked(repo_dir, recover).await?;
        let branch_ref = format!("refs/heads/{branch}");
        let tip = self
            .rev_parse_unlocked(repo_dir, &format!("{branch_ref}^{{commit}}"))
            .await?;
        let onto = match tip.as_deref() {
            Some(tip) => tip.to_string(),
            None => self
                .rev_parse_unlocked(repo_dir, &format!("{base_rev}^{{commit}}"))
                .await?
                .ok_or_else(|| format!("resume base {base_rev} is unresolvable"))?,
        };
        if self
            .is_ancestor_unlocked(repo_dir, &recover.sha, &onto)
            .await?
        {
            return Ok(RecoverResume::AlreadyIncorporated(recover.clone()));
        }
        if !self
            .is_ancestor_unlocked(repo_dir, &onto, &recover.sha)
            .await?
        {
            return Ok(RecoverResume::Stale {
                recover: recover.clone(),
                onto,
            });
        }
        let target = recover
            .snapshot_parent
            .clone()
            .unwrap_or_else(|| recover.sha.clone());
        if tip.as_deref() != Some(target.as_str()) {
            if tip.is_some()
                && self
                    .branch_checked_out_unlocked(repo_dir, &branch_ref)
                    .await?
            {
                return Err(format!(
                    "cannot resume {branch} from {}: branch is checked out in a worktree",
                    recover.refname
                ));
            }
            let mut update = self.git_cmd(repo_dir);
            update.args([
                "update-ref",
                "-m",
                "quorum resume from recover snapshot",
                &branch_ref,
                &target,
                tip.as_deref().unwrap_or(""),
            ]);
            let out = run_git(update, self.local_timeout, "git update-ref resume branch").await?;
            if !out.status.success() {
                return Err(format!(
                    "cannot resume {branch} from {}: {}",
                    recover.refname,
                    git_diagnostic(&out.stderr)
                ));
            }
        }
        Ok(RecoverResume::Resumed {
            recover: recover.clone(),
            previous: tip,
            restore_uncommitted: recover.snapshot_parent.is_some(),
        })
    }

    /// Fast-forward a freshly provisioned continuation worktree (HEAD at the
    /// PR head) to a trusted recover ref's commits when it descends from
    /// HEAD. A snapshot's uncommitted changes are restored by the caller
    /// (after base integration) with [`Self::restore_snapshot_as_uncommitted`].
    pub async fn resume_worktree_from_recover(
        &self,
        repo_dir: &Path,
        worktree_dir: &Path,
        recover: &RecoverRef,
    ) -> Result<RecoverResume, String> {
        let _guard = self.lock.lock().await;
        self.verify_trusted_unlocked(repo_dir, recover).await?;
        let head = self
            .rev_parse_unlocked(worktree_dir, "HEAD^{commit}")
            .await?
            .ok_or_else(|| "continuation worktree has no HEAD".to_string())?;
        if self
            .is_ancestor_unlocked(repo_dir, &recover.sha, &head)
            .await?
        {
            return Ok(RecoverResume::AlreadyIncorporated(recover.clone()));
        }
        if !self
            .is_ancestor_unlocked(repo_dir, &head, &recover.sha)
            .await?
        {
            return Ok(RecoverResume::Stale {
                recover: recover.clone(),
                onto: head,
            });
        }
        let target = recover
            .snapshot_parent
            .clone()
            .unwrap_or_else(|| recover.sha.clone());
        if target != head {
            let mut reset = self.git_cmd(worktree_dir);
            reset.args(["reset", "--keep", &target]);
            let out = run_git(reset, self.local_timeout, "git reset --keep recover").await?;
            if !out.status.success() {
                return Err(format!(
                    "cannot fast-forward worktree to {}: {}",
                    recover.refname,
                    git_diagnostic(&out.stderr)
                ));
            }
        }
        Ok(RecoverResume::Resumed {
            recover: recover.clone(),
            previous: Some(head),
            restore_uncommitted: recover.snapshot_parent.is_some(),
        })
    }

    /// Re-apply a trusted snapshot's uncommitted changes to `worktree_dir` as
    /// unstaged working-tree changes (new files come back untracked); no
    /// commit is created. Returns the paths the snapshot added. On a conflict
    /// the worktree is restored to its prior clean state and an error returned.
    pub async fn restore_snapshot_as_uncommitted(
        &self,
        repo_dir: &Path,
        worktree_dir: &Path,
        recover: &RecoverRef,
    ) -> Result<Vec<String>, String> {
        let _guard = self.lock.lock().await;
        let parent = recover
            .snapshot_parent
            .as_deref()
            .ok_or_else(|| format!("{} is not a snapshot", recover.refname))?;
        self.verify_trusted_unlocked(repo_dir, recover).await?;
        let mut pick = self.git_cmd(worktree_dir);
        pick.env("GIT_AUTHOR_NAME", SNAPSHOT_IDENTITY_NAME)
            .env("GIT_AUTHOR_EMAIL", SNAPSHOT_IDENTITY_EMAIL)
            .env("GIT_COMMITTER_NAME", SNAPSHOT_IDENTITY_NAME)
            .env("GIT_COMMITTER_EMAIL", SNAPSHOT_IDENTITY_EMAIL)
            .args(["cherry-pick", "--no-commit", &recover.sha]);
        let out = run_git(pick, self.local_timeout, "git cherry-pick snapshot").await?;
        if !out.status.success() {
            let mut abort = self.git_cmd(worktree_dir);
            abort.args(["cherry-pick", "--abort"]);
            let _ = run_git(abort, self.local_timeout, "git cherry-pick --abort").await;
            return Err(format!(
                "snapshot {} does not apply cleanly: {}",
                recover.refname,
                git_diagnostic(&out.stderr)
            ));
        }
        let mut unstage = self.git_cmd(worktree_dir);
        unstage.args(["reset", "-q"]);
        let out = run_git(unstage, self.local_timeout, "git reset unstage snapshot").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot unstage restored snapshot: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        let mut added = self.git_cmd(repo_dir);
        added.args([
            "diff",
            "--no-renames",
            "--name-only",
            "--diff-filter=A",
            parent,
            &recover.sha,
        ]);
        let out = run_git(added, self.local_timeout, "git diff snapshot added paths").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot list snapshot paths: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_string)
            .collect())
    }

    // ── helpers (caller MUST hold `self.lock`) ──────────────────────────────

    /// A recorded ref must still name its recorded SHA; a recorded snapshot
    /// must be a single-parent daemon commit on the recorded parent.
    async fn verify_trusted_unlocked(
        &self,
        repo_dir: &Path,
        recover: &RecoverRef,
    ) -> Result<(), String> {
        let current = self.rev_parse_unlocked(repo_dir, &recover.refname).await?;
        if current.as_deref() != Some(recover.sha.as_str()) {
            return Err(format!(
                "recover ref {} no longer names recorded commit {} (found {current:?})",
                recover.refname, recover.sha
            ));
        }
        if let Some(parent) = recover.snapshot_parent.as_deref() {
            let mut show = self.git_cmd(repo_dir);
            show.args(["log", "-1", "--format=%an%x00%P%x00%s", &recover.sha]);
            let out = run_git(show, self.local_timeout, "git inspect snapshot").await?;
            let text = String::from_utf8_lossy(&out.stdout);
            let mut fields = text.trim_end_matches('\n').split('\0');
            let author = fields.next().unwrap_or_default();
            let parents = fields.next().unwrap_or_default();
            let subject = fields.next().unwrap_or_default();
            if !out.status.success()
                || author != SNAPSHOT_IDENTITY_NAME
                || parents != parent
                || !subject.starts_with(SNAPSHOT_SUBJECT_PREFIX)
            {
                return Err(format!(
                    "recover ref {} is not the recorded daemon snapshot on {parent}",
                    recover.refname
                ));
            }
        }
        Ok(())
    }

    async fn rev_parse_unlocked(&self, dir: &Path, rev: &str) -> Result<Option<String>, String> {
        let mut cmd = self.git_cmd(dir);
        cmd.args(["rev-parse", "--verify", "--quiet", rev]);
        let out = run_git(cmd, self.local_timeout, "git rev-parse").await?;
        match out.status.code() {
            Some(0) => Ok(Some(
                String::from_utf8_lossy(&out.stdout).trim().to_string(),
            )),
            Some(1) => Ok(None),
            _ => Err(format!(
                "git rev-parse {rev} failed: {}",
                git_diagnostic(&out.stderr)
            )),
        }
    }

    /// True only when `dir` exists and is itself the top level of a Git
    /// worktree; never resolve a parent repository for a stray directory.
    async fn is_worktree_root_unlocked(&self, dir: &Path) -> Result<bool, String> {
        match std::fs::symlink_metadata(dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(format!("cannot inspect {}: {error}", dir.display())),
        }
        let mut cmd = self.git_cmd(dir);
        cmd.args(["rev-parse", "--show-toplevel"]);
        let out = run_git(cmd, self.local_timeout, "git rev-parse --show-toplevel").await?;
        if !out.status.success() {
            return Ok(false);
        }
        let top = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let top = std::fs::canonicalize(&top).ok();
        let dir = std::fs::canonicalize(dir).ok();
        Ok(top.is_some() && top == dir)
    }

    /// Tree of the worktree's full state built in a throwaway index. When
    /// capturing untracked files fails (e.g. a huge untracked tree times
    /// out), fall back to tracked changes only and report why; fail only if
    /// even that fails.
    async fn snapshot_tree_unlocked(
        &self,
        worktree: &Path,
    ) -> Result<(String, Option<String>), String> {
        match self.build_tree_unlocked(worktree, "-A").await {
            Ok(tree) => Ok((tree, None)),
            Err(full) => match self.build_tree_unlocked(worktree, "-u").await {
                Ok(tree) => Ok((tree, Some(full))),
                Err(tracked) => Err(format!("{full}; tracked-only fallback: {tracked}")),
            },
        }
    }

    async fn build_tree_unlocked(&self, worktree: &Path, add_mode: &str) -> Result<String, String> {
        let scratch = tempfile::tempdir()
            .map_err(|error| format!("cannot create snapshot index dir: {error}"))?;
        let index = scratch.path().join("index");
        let steps: [(&[&str], &str); 3] = [
            (&["read-tree", "HEAD"], "git read-tree snapshot"),
            (&["add", add_mode, "--", "."], "git add snapshot"),
            (&["write-tree"], "git write-tree snapshot"),
        ];
        let mut tree = String::new();
        for (args, label) in steps {
            let mut cmd = self.git_cmd(worktree);
            cmd.env("GIT_INDEX_FILE", &index).args(args);
            let out = run_git(cmd, self.local_timeout, label).await?;
            if !out.status.success() {
                return Err(format!("{label} failed: {}", git_diagnostic(&out.stderr)));
            }
            tree = String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
        if tree.is_empty() {
            return Err("git write-tree snapshot returned no tree".into());
        }
        Ok(tree)
    }

    async fn commit_snapshot_unlocked(
        &self,
        repo_dir: &Path,
        tree: &str,
        parent: &str,
        identity: &RecoverIdentity,
        tracked_only: bool,
    ) -> Result<String, String> {
        let message = format!(
            "{SNAPSHOT_SUBJECT_PREFIX}: task {} agent {} ({})\n\n\
             Uncommitted worktree state preserved by the daemon before teardown.{}\n",
            identity
                .task_id
                .map_or_else(|| "unknown".to_string(), |id| format!("#{id}")),
            recover_component(&identity.agent),
            recover_component(&identity.reason),
            if tracked_only {
                " Untracked files were NOT captured."
            } else {
                ""
            },
        );
        let mut cmd = self.git_cmd(repo_dir);
        cmd.env("GIT_AUTHOR_NAME", SNAPSHOT_IDENTITY_NAME)
            .env("GIT_AUTHOR_EMAIL", SNAPSHOT_IDENTITY_EMAIL)
            .env("GIT_COMMITTER_NAME", SNAPSHOT_IDENTITY_NAME)
            .env("GIT_COMMITTER_EMAIL", SNAPSHOT_IDENTITY_EMAIL)
            .args([
                "commit-tree",
                "--no-gpg-sign",
                tree,
                "-p",
                parent,
                "-m",
                &message,
            ]);
        let out = run_git(cmd, self.local_timeout, "git commit-tree snapshot").await?;
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !out.status.success() || sha.is_empty() {
            return Err(format!(
                "git commit-tree snapshot failed: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(sha)
    }

    /// A trusted snapshot that still exists with the same tree on `parent`.
    async fn trusted_snapshot_exists_unlocked(
        &self,
        repo_dir: &Path,
        trusted: &[RecoverRef],
        tree: &str,
        parent: &str,
    ) -> Result<bool, String> {
        for recover in trusted {
            if recover.snapshot_parent.as_deref() != Some(parent) {
                continue;
            }
            if self
                .rev_parse_unlocked(repo_dir, &recover.refname)
                .await?
                .as_deref()
                != Some(recover.sha.as_str())
            {
                continue;
            }
            if self
                .rev_parse_unlocked(repo_dir, &format!("{}^{{tree}}", recover.sha))
                .await?
                .as_deref()
                == Some(tree)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// True when `commit` has history reachable from no ref other than the
    /// branch about to be deleted (i.e. deleting it would lose commits).
    /// Remote-tracking, PR, and publication refs count as preserving; the
    /// worker-writable recover namespace counts only through trusted refs.
    async fn commit_unique_unlocked(
        &self,
        repo_dir: &Path,
        commit: &str,
        deleting_branch: Option<&str>,
        trusted: &[RecoverRef],
    ) -> Result<bool, String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["rev-list", "-n", "1", commit, "--not"]);
        if let Some(branch) = deleting_branch {
            cmd.arg(format!("--exclude=refs/heads/{branch}"));
        }
        cmd.arg(format!("--exclude={RECOVER_REF_ROOT}/*"));
        // Every ref, but not `--all`: that also counts worktree HEADs, and
        // the worktree being destroyed must not vouch for its own commits.
        cmd.arg("--glob=refs/*");
        for recover in trusted {
            cmd.arg(&recover.sha);
        }
        let out = run_git(cmd, self.local_timeout, "git rev-list unique work").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot determine unpushed work: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(!String::from_utf8_lossy(&out.stdout).trim().is_empty())
    }

    async fn is_ancestor_unlocked(
        &self,
        repo_dir: &Path,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool, String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["merge-base", "--is-ancestor", ancestor, descendant]);
        let out = run_git(cmd, self.local_timeout, "git merge-base --is-ancestor").await?;
        match out.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(format!(
                "git merge-base --is-ancestor {ancestor} {descendant} failed: {}",
                git_diagnostic(&out.stderr)
            )),
        }
    }

    async fn branch_checked_out_unlocked(
        &self,
        repo_dir: &Path,
        branch_ref: &str,
    ) -> Result<bool, String> {
        Ok(self
            .list_worktrees_unlocked(repo_dir)
            .await?
            .iter()
            .any(|registration| registration.branch.as_deref() == Some(branch_ref)))
    }

    /// Refs under `prefix`, newest first (fixed-width millis sort).
    async fn list_bucket_unlocked(
        &self,
        repo_dir: &Path,
        prefix: &str,
    ) -> Result<Vec<RecoverRef>, String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args([
            "for-each-ref",
            "--sort=-refname",
            "--format=%(refname) %(objectname)",
            prefix,
        ]);
        let out = run_git(cmd, self.local_timeout, "git list recover refs").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot list recover refs: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|line| {
                let (refname, sha) = line
                    .split_once(' ')
                    .ok_or_else(|| format!("malformed recover ref listing: {line}"))?;
                Ok(RecoverRef::listed(refname, sha))
            })
            .collect()
    }

    async fn prune_bucket_unlocked(
        &self,
        repo_dir: &Path,
        bucket: &str,
        keep: usize,
    ) -> Result<(), String> {
        let refs = self.list_bucket_unlocked(repo_dir, bucket).await?;
        for recover in refs.iter().skip(keep) {
            self.delete_recover_ref_unlocked(repo_dir, recover).await?;
        }
        Ok(())
    }

    async fn delete_recover_ref_unlocked(
        &self,
        repo_dir: &Path,
        recover: &RecoverRef,
    ) -> Result<(), String> {
        if !recover.refname.starts_with(&format!("{RECOVER_REF_ROOT}/")) {
            return Err(format!(
                "refusing to delete non-recover ref {}",
                recover.refname
            ));
        }
        match self.rev_parse_unlocked(repo_dir, &recover.refname).await? {
            None => return Ok(()),
            Some(current) if current != recover.sha => {
                return Err(format!(
                    "recover ref {} moved to {current}; left in place",
                    recover.refname
                ))
            }
            Some(_) => {}
        }
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["update-ref", "-d", &recover.refname, &recover.sha]);
        let out = run_git(cmd, self.local_timeout, "git delete recover ref").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot delete recover ref {}: {}",
                recover.refname,
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
