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
//! operator may create by hand, so retention can never delete those. Any ref
//! keeps its objects reachable after the worktree is pruned.
//!
//! The snapshot uses plumbing only (temporary `GIT_INDEX_FILE`, `add -A`,
//! `write-tree`, `commit-tree`, `update-ref`): no hooks run, the worktree's
//! real index and files are untouched, and the ref is written last with a
//! create-only CAS. A daemon that dies mid-snapshot therefore leaves the
//! worktree intact and no partial ref; the next teardown attempt (including
//! startup GC) simply snapshots again.

use super::{git_diagnostic, run_git, WorktreeManager};
use std::path::Path;

pub(crate) const RECOVER_REF_ROOT: &str = "refs/quorum-recover";
/// Bounded retention: newest snapshots kept per task bucket.
pub(crate) const RECOVER_REFS_PER_TASK: usize = 2;
const RECOVER_COMPONENT_MAX: usize = 48;
const UNATTRIBUTED_BUCKET: &str = "unattributed";
const SNAPSHOT_IDENTITY_NAME: &str = "quorum-daemon";
const SNAPSHOT_IDENTITY_EMAIL: &str = "quorum-daemon@localhost";

/// Who and why a worktree is being torn down. `task_id` selects the bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverIdentity {
    pub task_id: Option<i64>,
    pub agent: String,
    pub reason: String,
}

impl RecoverIdentity {
    pub fn new(task_id: i64, agent: &str, reason: &str) -> Self {
        Self {
            task_id: Some(task_id),
            agent: agent.to_string(),
            reason: reason.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverRef {
    pub refname: String,
    pub sha: String,
}

impl RecoverRef {
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
    /// No recover ref exists for the task.
    None,
    /// The newest recover ref is already contained in the provisioning start.
    AlreadyIncorporated(RecoverRef),
    /// The provisioning start now points at the recover ref.
    Resumed {
        recover: RecoverRef,
        previous: Option<String>,
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

/// Task id from a daemon worker branch or worktree name (`...-t<id>`).
pub(crate) fn task_id_from_name(name: &str) -> Option<i64> {
    let (_, digits) = name.rsplit_once("-t")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<i64>().ok().filter(|id| *id > 0)
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
    /// identical snapshot already exists). Caller MUST hold `self.lock`.
    pub(super) async fn preserve_unlocked(
        &self,
        repo_dir: &Path,
        worktree_dir: Option<&Path>,
        deleting_branch: Option<&str>,
        identity: &RecoverIdentity,
    ) -> Result<Vec<RecoverRef>, String> {
        let bucket = recover_bucket(identity.task_id);
        // (commit, freshly_created_snapshot)
        let mut candidates: Vec<(String, bool)> = Vec::new();
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
                let tree = self.snapshot_tree_unlocked(worktree).await?;
                if tree != head_tree {
                    match self
                        .find_equivalent_snapshot_unlocked(repo_dir, &bucket, &tree, &head)
                        .await?
                    {
                        Some(_) => {}
                        None => {
                            let commit = self
                                .commit_snapshot_unlocked(repo_dir, &tree, &head, identity)
                                .await?;
                            candidates.push((commit, true));
                        }
                    }
                } else {
                    candidates.push((head.clone(), false));
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
                    candidates.push((tip, false));
                }
            }
        }

        let mut created = Vec::new();
        let millis = now_millis();
        for (index, (commit, fresh)) in candidates.into_iter().enumerate() {
            if !fresh
                && !self
                    .commit_unique_unlocked(repo_dir, &commit, deleting_branch)
                    .await?
            {
                continue;
            }
            let refname = format!(
                "{bucket}/{:013}/{}/{}",
                millis + index as u128,
                recover_component(&identity.agent),
                recover_component(&identity.reason),
            );
            let mut update = self.git_cmd(repo_dir);
            update.args([
                "update-ref",
                "-m",
                "quorum recover snapshot",
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
            });
        }
        if !created.is_empty() {
            self.prune_bucket_unlocked(repo_dir, &bucket, RECOVER_REFS_PER_TASK)
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

    /// Every recover ref, newest first within each bucket.
    pub async fn list_recover_refs(&self, repo_dir: &Path) -> Result<Vec<RecoverRef>, String> {
        let _guard = self.lock.lock().await;
        self.list_bucket_unlocked(repo_dir, RECOVER_REF_ROOT).await
    }

    /// Newest recover ref for `task_id`.
    #[cfg(test)]
    pub async fn latest_recover_ref(
        &self,
        repo_dir: &Path,
        task_id: i64,
    ) -> Result<Option<RecoverRef>, String> {
        let _guard = self.lock.lock().await;
        Ok(self
            .list_bucket_unlocked(repo_dir, &recover_bucket(Some(task_id)))
            .await?
            .into_iter()
            .next())
    }

    /// Delete exactly these recover refs (CAS on the listed SHA). Refs outside
    /// the recover namespace are refused.
    pub async fn delete_recover_refs(
        &self,
        repo_dir: &Path,
        refs: &[RecoverRef],
    ) -> Result<usize, String> {
        let _guard = self.lock.lock().await;
        let mut deleted = 0;
        for recover in refs {
            self.delete_recover_ref_unlocked(repo_dir, recover).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Point the task's local branch at its newest recover ref before a
    /// fresh worker provisions it. With an existing branch the recover ref
    /// must descend from the branch tip (fast-forward only); without one it
    /// must descend from `base_rev` (the task's allocation provenance, or
    /// the remote base for a legacy allocation without one).
    pub async fn resume_branch_from_recover(
        &self,
        repo_dir: &Path,
        task_id: i64,
        branch: &str,
        base_rev: &str,
    ) -> Result<RecoverResume, String> {
        let _guard = self.lock.lock().await;
        let Some(recover) = self
            .list_bucket_unlocked(repo_dir, &recover_bucket(Some(task_id)))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(RecoverResume::None);
        };
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
            return Ok(RecoverResume::AlreadyIncorporated(recover));
        }
        if !self
            .is_ancestor_unlocked(repo_dir, &onto, &recover.sha)
            .await?
        {
            return Ok(RecoverResume::Stale { recover, onto });
        }
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
            &recover.sha,
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
        Ok(RecoverResume::Resumed {
            recover,
            previous: tip,
        })
    }

    /// Fast-forward a freshly provisioned continuation worktree (HEAD at the
    /// PR head) to the task's newest recover ref when it descends from HEAD.
    pub async fn resume_worktree_from_recover(
        &self,
        repo_dir: &Path,
        worktree_dir: &Path,
        task_id: i64,
    ) -> Result<RecoverResume, String> {
        let _guard = self.lock.lock().await;
        let Some(recover) = self
            .list_bucket_unlocked(repo_dir, &recover_bucket(Some(task_id)))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(RecoverResume::None);
        };
        let head = self
            .rev_parse_unlocked(worktree_dir, "HEAD^{commit}")
            .await?
            .ok_or_else(|| "continuation worktree has no HEAD".to_string())?;
        if self
            .is_ancestor_unlocked(repo_dir, &recover.sha, &head)
            .await?
        {
            return Ok(RecoverResume::AlreadyIncorporated(recover));
        }
        if !self
            .is_ancestor_unlocked(repo_dir, &head, &recover.sha)
            .await?
        {
            return Ok(RecoverResume::Stale {
                recover,
                onto: head,
            });
        }
        let mut reset = self.git_cmd(worktree_dir);
        reset.args(["reset", "--keep", &recover.sha]);
        let out = run_git(reset, self.local_timeout, "git reset --keep recover").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot fast-forward worktree to {}: {}",
                recover.refname,
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(RecoverResume::Resumed {
            recover,
            previous: Some(head),
        })
    }

    // ── helpers (caller MUST hold `self.lock`) ──────────────────────────────

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

    /// Tree of the worktree's full state (tracked edits, deletions, and
    /// untracked-not-ignored files) built in a throwaway index.
    async fn snapshot_tree_unlocked(&self, worktree: &Path) -> Result<String, String> {
        let scratch = tempfile::tempdir()
            .map_err(|error| format!("cannot create snapshot index dir: {error}"))?;
        let index = scratch.path().join("index");
        let steps: [(&[&str], &str); 3] = [
            (&["read-tree", "HEAD"], "git read-tree snapshot"),
            (&["add", "-A", "--", "."], "git add snapshot"),
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
    ) -> Result<String, String> {
        let message = format!(
            "quorum recover snapshot: task {} agent {} ({})\n\n\
             Uncommitted worktree state preserved by the daemon before teardown.\n",
            identity
                .task_id
                .map_or_else(|| "unknown".to_string(), |id| format!("#{id}")),
            recover_component(&identity.agent),
            recover_component(&identity.reason),
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

    /// An existing snapshot in `bucket` with the same tree on the same parent.
    async fn find_equivalent_snapshot_unlocked(
        &self,
        repo_dir: &Path,
        bucket: &str,
        tree: &str,
        parent: &str,
    ) -> Result<Option<String>, String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args([
            "for-each-ref",
            "--format=%(refname) %(tree) %(parent)",
            bucket,
        ]);
        let out = run_git(cmd, self.local_timeout, "git list recover snapshots").await?;
        if !out.status.success() {
            return Err(format!(
                "cannot list recover refs: {}",
                git_diagnostic(&out.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .find_map(|line| {
                let mut parts = line.split(' ');
                let refname = parts.next()?;
                (parts.next() == Some(tree)
                    && parts.next() == Some(parent)
                    && parts.next().is_none())
                .then(|| refname.to_string())
            }))
    }

    /// True when `commit` has history reachable from no ref other than the
    /// branch about to be deleted (i.e. deleting it would lose commits).
    /// Remote-tracking, PR, publication, and earlier recover refs all count
    /// as already preserved.
    async fn commit_unique_unlocked(
        &self,
        repo_dir: &Path,
        commit: &str,
        deleting_branch: Option<&str>,
    ) -> Result<bool, String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["rev-list", "-n", "1", commit, "--not"]);
        if let Some(branch) = deleting_branch {
            cmd.arg(format!("--exclude=refs/heads/{branch}"));
        }
        // Every ref, but not `--all`: that also counts worktree HEADs, and
        // the worktree being destroyed must not vouch for its own commits.
        cmd.arg("--glob=refs/*");
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

    /// Recover refs under `prefix`, newest first (fixed-width millis sort).
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
                Ok(RecoverRef {
                    refname: refname.to_string(),
                    sha: sha.to_string(),
                })
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
