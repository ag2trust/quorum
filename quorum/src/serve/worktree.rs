//! Serialized git worktree operations for agent isolation.

use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Mutex;

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_LOCAL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct WorktreeManager {
    lock: Mutex<()>,
    git_bin: PathBuf,
    fetch_timeout: Duration,
    local_timeout: Duration,
}

/// Run a git subprocess with a bounded timeout. On timeout the child is killed
/// via `kill_on_drop` (SIGKILL) before this function returns, so the caller's
/// mutex guard remains held until the child is dead.
async fn run_git(
    mut cmd: Command,
    timeout: Duration,
    label: &str,
) -> Result<std::process::Output, String> {
    cmd.kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(format!("{label}: {e}")),
        Err(_) => Err(format!("{label}: timed out after {}s", timeout.as_secs())),
    }
}

impl WorktreeManager {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            git_bin: PathBuf::from("git"),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
            local_timeout: DEFAULT_LOCAL_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_config(git_bin: PathBuf, fetch_timeout: Duration, local_timeout: Duration) -> Self {
        Self {
            lock: Mutex::new(()),
            git_bin,
            fetch_timeout,
            local_timeout,
        }
    }

    fn git_cmd(&self, repo_dir: &Path) -> Command {
        let mut cmd = Command::new(&self.git_bin);
        cmd.arg("-C").arg(repo_dir);
        cmd
    }

    fn publication_ref(task_id: i64) -> String {
        format!("refs/quorum-publication/task-{task_id}")
    }

    /// Pin the immutable source commit named by a durable publication intent.
    ///
    /// Publication failures tear down run-local worktrees and branches. This
    /// daemon-owned ref keeps the exact object reachable across that cleanup,
    /// including when a remediation retry receives a different local branch.
    pub async fn pin_publication_source(
        &self,
        repo_dir: &Path,
        task_id: i64,
        source_sha: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let publication_ref = Self::publication_ref(task_id);

        let mut resolve = self.git_cmd(repo_dir);
        resolve.args(["rev-parse", "--verify", &format!("{source_sha}^{{commit}}")]);
        let resolved = run_git(
            resolve,
            self.local_timeout,
            "git rev-parse publication source",
        )
        .await?;
        let resolved_sha = String::from_utf8_lossy(&resolved.stdout).trim().to_string();
        if !resolved.status.success() || resolved_sha != source_sha {
            return Err(format!(
                "publication source {source_sha} is not an exact local commit"
            ));
        }

        let mut update = self.git_cmd(repo_dir);
        update.args(["update-ref", &publication_ref, source_sha]);
        let updated = run_git(
            update,
            self.local_timeout,
            "git update-ref publication source",
        )
        .await?;
        if !updated.status.success() {
            return Err(format!(
                "cannot pin publication source {source_sha}: {}",
                String::from_utf8_lossy(&updated.stderr)
            ));
        }
        Ok(())
    }

    /// Retire a publication pin only when it still names the completed source.
    /// A mismatched ref is left intact and logged by the best-effort caller.
    pub async fn retire_publication_source(
        &self,
        repo_dir: &Path,
        task_id: i64,
        source_sha: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let publication_ref = Self::publication_ref(task_id);
        let mut delete = self.git_cmd(repo_dir);
        delete.args(["update-ref", "-d", &publication_ref, source_sha]);
        let deleted = run_git(
            delete,
            self.local_timeout,
            "git update-ref retire publication source",
        )
        .await?;
        if !deleted.status.success() {
            return Err(format!(
                "cannot retire publication source {source_sha}: {}",
                String::from_utf8_lossy(&deleted.stderr)
            ));
        }
        Ok(())
    }

    /// Build a daemon-only push command that bypasses the worktree's poisoned
    /// `pushurl`. Managed agents' ordinary pushes hit that best-effort lockout;
    /// the daemon obtains the normal fetch URL for its explicit protocol push.
    /// This is not credential isolation: D4 must enforce that separately.
    /// Caller MUST hold `self.lock`.
    async fn daemon_push_cmd(
        &self,
        worktree_dir: &Path,
        refspec: &str,
        lease: &str,
    ) -> Result<Command, String> {
        let mut get_url = self.git_cmd(worktree_dir);
        get_url.args(["remote", "get-url", "origin"]);
        let url = run_git(get_url, self.local_timeout, "git remote get-url origin").await?;
        if !url.status.success() {
            return Err(format!(
                "git remote get-url origin failed: {}",
                String::from_utf8_lossy(&url.stderr)
            ));
        }
        let push_url = String::from_utf8_lossy(&url.stdout).trim().to_string();
        if push_url.is_empty() {
            return Err("git remote get-url origin returned an empty URL".into());
        }
        let mut push = Command::new(&self.git_bin);
        push.arg("-C")
            .arg(worktree_dir)
            .args(["push", &push_url, lease, refspec]);
        Ok(push)
    }

    /// Check if a local branch exists. Caller MUST hold `self.lock`.
    async fn branch_exists_unlocked(&self, repo_dir: &Path, branch: &str) -> bool {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["rev-parse", "--verify", &format!("refs/heads/{branch}")]);
        match run_git(cmd, self.local_timeout, "git rev-parse --verify").await {
            Ok(out) => out.status.success(),
            Err(_) => false,
        }
    }

    /// Find the worktree path that has `branch` checked out, if any.
    /// Returns `None` when the branch is free or on any git error.
    /// Caller MUST hold `self.lock`.
    async fn find_worktree_for_branch_unlocked(
        &self,
        repo_dir: &Path,
        branch: &str,
    ) -> Option<String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["worktree", "list", "--porcelain"]);
        let out = match run_git(cmd, self.local_timeout, "git worktree list").await {
            Ok(out) if out.status.success() => out,
            _ => return None,
        };
        let stdout = String::from_utf8_lossy(&out.stdout);
        let target_ref = format!("branch refs/heads/{branch}");
        let mut current_wt: Option<String> = None;
        for line in stdout.lines() {
            if let Some(path) = line.strip_prefix("worktree ") {
                current_wt = Some(path.to_string());
            } else if line == target_ref {
                return current_wt;
            } else if line.is_empty() {
                current_wt = None;
            }
        }
        None
    }

    pub async fn provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        base_ref: &str,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            // Reuse existing branch — preserves commits from a prior incarnation
            let mut cmd = self.git_cmd(repo_dir);
            cmd.args(["worktree", "add"]);
            cmd.arg(&wt_path).arg(branch);
            let add = run_git(cmd, self.local_timeout, "git worktree add (reuse branch)").await?;
            if !add.status.success() {
                return Err(format!(
                    "git worktree add (reuse branch) failed: {}",
                    String::from_utf8_lossy(&add.stderr)
                ));
            }
        } else {
            let mut cmd = self.git_cmd(repo_dir);
            cmd.args(["worktree", "add", "-b", branch]);
            cmd.arg(&wt_path).arg(base_ref);
            let add = run_git(cmd, self.local_timeout, "git worktree add").await?;
            if !add.status.success() {
                return Err(format!(
                    "git worktree add failed: {}",
                    String::from_utf8_lossy(&add.stderr)
                ));
            }
        }

        Ok(wt_path)
    }

    pub async fn fetch_and_provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        remote_branch: &str,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let mut fetch_cmd = self.git_cmd(repo_dir);
        fetch_cmd.args(["fetch", "origin", remote_branch]);
        let fetch = run_git(fetch_cmd, self.fetch_timeout, "git fetch").await?;

        if !fetch.status.success() {
            return Err(format!(
                "git fetch origin {remote_branch} failed: {}",
                String::from_utf8_lossy(&fetch.stderr)
            ));
        }

        let base_ref = format!("origin/{remote_branch}");
        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            // Delete stale review branch so it can be recreated at the remote head
            let mut del_cmd = self.git_cmd(repo_dir);
            del_cmd.args(["branch", "-D", branch]);
            let _ = run_git(del_cmd, self.local_timeout, "git branch -D (stale)").await;
        }

        let mut add_cmd = self.git_cmd(repo_dir);
        add_cmd.args(["worktree", "add", "-b", branch]);
        add_cmd.arg(&wt_path).arg(&base_ref);
        let add = run_git(add_cmd, self.local_timeout, "git worktree add").await?;

        if !add.status.success() {
            return Err(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&add.stderr)
            ));
        }

        Ok(wt_path)
    }

    /// Fetch a PR head via `refs/pull/<pr>/head` and provision a worktree.
    /// Works for both same-repo and fork PRs (GitHub exposes this ref
    /// regardless of head repository).
    pub async fn fetch_pr_and_provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        pr: i64,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let refspec = format!("+refs/pull/{pr}/head:refs/quorum-pr/{pr}");
        let mut fetch_cmd = self.git_cmd(repo_dir);
        fetch_cmd.args(["fetch", "origin", &refspec]);
        let fetch = run_git(fetch_cmd, self.fetch_timeout, "git fetch pr ref").await?;
        if !fetch.status.success() {
            return Err(format!(
                "git fetch origin {refspec} failed: {}",
                String::from_utf8_lossy(&fetch.stderr)
            ));
        }

        let base_ref = format!("refs/quorum-pr/{pr}");
        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            let mut del_cmd = self.git_cmd(repo_dir);
            del_cmd.args(["branch", "-D", branch]);
            let _ = run_git(del_cmd, self.local_timeout, "git branch -D (stale)").await;
        }

        let mut add_cmd = self.git_cmd(repo_dir);
        add_cmd.args(["worktree", "add", "-b", branch]);
        add_cmd.arg(&wt_path).arg(&base_ref);
        let add = run_git(add_cmd, self.local_timeout, "git worktree add").await?;
        if !add.status.success() {
            return Err(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&add.stderr)
            ));
        }

        Ok(wt_path)
    }

    /// Write one git config setting inside `dir`. Caller MUST hold `self.lock`.
    async fn set_config(&self, dir: &Path, args: &[&str]) -> Result<(), String> {
        let mut cmd = Command::new(&self.git_bin);
        cmd.arg("-C").arg(dir).arg("config").args(args);
        let out = run_git(cmd, self.local_timeout, "git config").await?;
        if !out.status.success() {
            return Err(format!(
                "git config {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(())
    }

    /// Enable per-worktree config, which is a prerequisite for every
    /// `git config --worktree` write below.
    ///
    /// NOTE: this mutates the SHARED repository config permanently
    /// (`extensions.worktreeConfig` lives in the common config, not a worktree
    /// one). Git then reads `core.bare` / `core.worktree` from the common
    /// config for every worktree, so refuse when `core.worktree` is present or
    /// `core.bare` is true rather than changing how the user's checkout
    /// resolves its work tree.
    /// Caller MUST hold `self.lock`.
    async fn enable_worktree_config(&self, worktree_dir: &Path) -> Result<(), String> {
        let mut worktree_cmd = Command::new(&self.git_bin);
        worktree_cmd
            .arg("-C")
            .arg(worktree_dir)
            .args(["config", "--get", "core.worktree"]);
        let worktree = run_git(
            worktree_cmd,
            self.local_timeout,
            "git config --get core.worktree",
        )
        .await?;
        match worktree.status.code() {
            Some(0) => {
                let value = String::from_utf8_lossy(&worktree.stdout).trim().to_string();
                return Err(format!(
                    "refusing to enable extensions.worktreeConfig: core.worktree={value} is set \
                     in the shared repo config"
                ));
            }
            Some(1) => {}
            _ => {
                return Err(format!(
                    "git config --get core.worktree failed: {}",
                    String::from_utf8_lossy(&worktree.stderr)
                ));
            }
        }

        let mut bare_cmd = Command::new(&self.git_bin);
        bare_cmd
            .arg("-C")
            .arg(worktree_dir)
            .args(["config", "--bool", "--get", "core.bare"]);
        let bare = run_git(
            bare_cmd,
            self.local_timeout,
            "git config --bool --get core.bare",
        )
        .await?;
        match bare.status.code() {
            Some(0) if String::from_utf8_lossy(&bare.stdout).trim() == "false" => {}
            Some(0) => {
                return Err(
                    "refusing to enable extensions.worktreeConfig: core.bare=true is set \
                     in the shared repo config"
                        .to_string(),
                );
            }
            Some(1) => {}
            _ => {
                return Err(format!(
                    "git config --bool --get core.bare failed: {}",
                    String::from_utf8_lossy(&bare.stderr)
                ));
            }
        }
        self.set_config(worktree_dir, &["extensions.worktreeConfig", "true"])
            .await
    }

    /// Make an unqualified push from `worktree_dir` fail. Reviewers read code
    /// and post GitHub comments; they never push. Worktree-scoped so the shared
    /// checkout and every other worktree keep their real push URL.
    ///
    /// Defense in depth, not an authority boundary: an agent that types a full
    /// remote URL, or uses `gh`, can still write. Also mutates the shared repo
    /// config via [`Self::enable_worktree_config`].
    pub async fn disable_push(&self, worktree_dir: &Path) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        self.enable_worktree_config(worktree_dir).await?;
        self.set_config(
            worktree_dir,
            &[
                "--worktree",
                "remote.origin.pushurl",
                "push-disabled://daemon-owns-push",
            ],
        )
        .await?;
        Ok(())
    }

    /// Verify that a worktree HEAD matches an expected SHA. Does not take the
    /// serialization lock — this is a read-only check on the worktree dir.
    pub async fn verify_head_sha(
        &self,
        worktree_dir: &Path,
        expected_sha: &str,
    ) -> Result<(), String> {
        let mut cmd = Command::new(&self.git_bin);
        cmd.arg("-C").arg(worktree_dir).args(["rev-parse", "HEAD"]);
        let out = run_git(cmd, self.local_timeout, "git rev-parse HEAD").await?;
        if !out.status.success() {
            return Err(format!(
                "git rev-parse HEAD failed in {}: {}",
                worktree_dir.display(),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if actual != expected_sha {
            return Err(format!(
                "worktree HEAD SHA mismatch: expected {expected_sha}, got {actual}"
            ));
        }
        Ok(())
    }

    pub async fn head_sha(&self, worktree_dir: &Path) -> Result<String, String> {
        let mut cmd = self.git_cmd(worktree_dir);
        cmd.args(["rev-parse", "HEAD"]);
        let out = run_git(cmd, self.local_timeout, "git rev-parse HEAD").await?;
        if !out.status.success() {
            return Err(format!(
                "git rev-parse HEAD failed in {}: {}",
                worktree_dir.display(),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if sha.is_empty() {
            return Err("git rev-parse HEAD returned an empty SHA".into());
        }
        Ok(sha)
    }

    /// Publish the worktree's exact current commit to an already-authoritative
    /// same-repository PR head, then prove the remote accepted that commit.
    ///
    /// The expected SHA comes from a live PR lookup. We fetch and compare it
    /// immediately before the push so a stale or retargeted PR head is never
    /// overwritten. A rejected push is followed by a best-effort refetch for
    /// diagnostics; callers must park rather than transition lifecycle.
    pub async fn push_to_pr_head(
        &self,
        worktree_dir: &Path,
        remote_branch: &str,
        expected_remote_sha: &str,
        source_sha: &str,
    ) -> Result<String, String> {
        let _guard = self.lock.lock().await;
        let remote_ref = format!("refs/heads/{remote_branch}");

        let mut fetch = self.git_cmd(worktree_dir);
        fetch.args(["fetch", "origin", &remote_ref]);
        let fetched = run_git(fetch, self.fetch_timeout, "git fetch PR head").await?;
        if !fetched.status.success() {
            return Err(format!(
                "git fetch origin {remote_ref} failed: {}",
                String::from_utf8_lossy(&fetched.stderr)
            ));
        }

        let mut remote_sha = self.git_cmd(worktree_dir);
        remote_sha.args(["rev-parse", "FETCH_HEAD"]);
        let remote = run_git(remote_sha, self.local_timeout, "git rev-parse FETCH_HEAD").await?;
        if !remote.status.success() {
            return Err(format!(
                "git rev-parse FETCH_HEAD failed: {}",
                String::from_utf8_lossy(&remote.stderr)
            ));
        }
        let actual_remote_sha = String::from_utf8_lossy(&remote.stdout).trim().to_string();
        if actual_remote_sha != expected_remote_sha {
            return Err(format!(
                "PR head changed before daemon push: expected {expected_remote_sha}, got {actual_remote_sha}"
            ));
        }

        let mut resolve_source = self.git_cmd(worktree_dir);
        resolve_source.args(["rev-parse", "--verify", &format!("{source_sha}^{{commit}}")]);
        let source = run_git(
            resolve_source,
            self.local_timeout,
            "git rev-parse publication source",
        )
        .await?;
        if !source.status.success() {
            return Err(format!(
                "publication source {source_sha} is not a local commit: {}",
                String::from_utf8_lossy(&source.stderr)
            ));
        }
        let resolved_source = String::from_utf8_lossy(&source.stdout).trim().to_string();
        if resolved_source != source_sha {
            return Err(format!(
                "publication source did not resolve exactly: expected {source_sha}, got {resolved_source}"
            ));
        }

        let refspec = format!("{source_sha}:{remote_ref}");
        let lease = format!("--force-with-lease={remote_ref}:{expected_remote_sha}");
        let push = self.daemon_push_cmd(worktree_dir, &refspec, &lease).await?;
        let pushed = run_git(push, self.fetch_timeout, "git push PR head").await?;
        if !pushed.status.success() {
            let mut refresh = self.git_cmd(worktree_dir);
            refresh.args(["fetch", "origin", &remote_ref]);
            let _ = run_git(refresh, self.fetch_timeout, "git refetch rejected PR push").await;
            return Err(format!(
                "daemon push to {remote_ref} rejected: {}",
                String::from_utf8_lossy(&pushed.stderr)
            ));
        }

        let mut verify = self.git_cmd(worktree_dir);
        verify.args(["ls-remote", "--exit-code", "origin", &remote_ref]);
        let verified = run_git(verify, self.fetch_timeout, "git ls-remote PR head").await?;
        if !verified.status.success() {
            return Err(format!(
                "cannot verify daemon push to {remote_ref}: {}",
                String::from_utf8_lossy(&verified.stderr)
            ));
        }
        let verified_stdout = String::from_utf8_lossy(&verified.stdout);
        let verified_sha = verified_stdout
            .split_whitespace()
            .next()
            .unwrap_or_default();
        if verified_sha != source_sha {
            return Err(format!(
                "daemon push verification mismatch for {remote_ref}: expected {source_sha}, got {verified_sha}"
            ));
        }
        Ok(source_sha.to_string())
    }

    /// Publish a new daemon-owned branch and verify its remote SHA. This is
    /// only for the first delivery, before a PR exists and therefore before a
    /// PR head can become authoritative. Existing remote branches are
    /// ambiguous and are rejected rather than overwritten.
    pub async fn push_new_branch(
        &self,
        worktree_dir: &Path,
        branch: &str,
        source_sha: &str,
    ) -> Result<String, String> {
        let _guard = self.lock.lock().await;
        let remote_ref = format!("refs/heads/{branch}");
        let mut resolve_source = self.git_cmd(worktree_dir);
        resolve_source.args(["rev-parse", "--verify", &format!("{source_sha}^{{commit}}")]);
        let source = run_git(
            resolve_source,
            self.local_timeout,
            "git rev-parse publication source",
        )
        .await?;
        if !source.status.success() {
            return Err(format!(
                "publication source {source_sha} is not a local commit: {}",
                String::from_utf8_lossy(&source.stderr)
            ));
        }
        let resolved_source = String::from_utf8_lossy(&source.stdout).trim().to_string();
        if resolved_source != source_sha {
            return Err(format!(
                "publication source did not resolve exactly: expected {source_sha}, got {resolved_source}"
            ));
        }

        let mut existing = self.git_cmd(worktree_dir);
        existing.args(["ls-remote", "origin", &remote_ref]);
        let existing = run_git(existing, self.fetch_timeout, "git ls-remote new branch").await?;
        if !existing.status.success() {
            return Err(format!(
                "cannot inspect new branch {remote_ref}: {}",
                String::from_utf8_lossy(&existing.stderr)
            ));
        }
        if !existing.stdout.is_empty() {
            let existing_sha = String::from_utf8_lossy(&existing.stdout)
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string();
            if existing_sha == source_sha {
                return Ok(source_sha.to_string());
            }
            return Err(format!(
                "remote branch {remote_ref} already exists at {existing_sha}; refusing ambiguous push"
            ));
        }

        let refspec = format!("{source_sha}:{remote_ref}");
        let lease = format!("--force-with-lease={remote_ref}:");
        let push = self.daemon_push_cmd(worktree_dir, &refspec, &lease).await?;
        let pushed = run_git(push, self.fetch_timeout, "git push new branch").await?;
        if !pushed.status.success() {
            return Err(format!(
                "daemon push to new branch {remote_ref} rejected: {}",
                String::from_utf8_lossy(&pushed.stderr)
            ));
        }
        let mut verify = self.git_cmd(worktree_dir);
        verify.args(["ls-remote", "--exit-code", "origin", &remote_ref]);
        let verified = run_git(verify, self.fetch_timeout, "git ls-remote new branch").await?;
        let verified_stdout = String::from_utf8_lossy(&verified.stdout);
        let verified_sha = verified_stdout
            .split_whitespace()
            .next()
            .unwrap_or_default();
        if !verified.status.success() || verified_sha != source_sha {
            return Err(format!(
                "daemon push verification mismatch for new branch {remote_ref}: expected {source_sha}, got {verified_sha}"
            ));
        }
        Ok(source_sha.to_string())
    }

    pub async fn gc_orphaned(
        &self,
        repo_dir: &Path,
        worktree_base: &Path,
        active_worktrees: &[&str],
    ) -> Vec<String> {
        let _guard = self.lock.lock().await;

        let mut prune_cmd = self.git_cmd(repo_dir);
        prune_cmd.args(["worktree", "prune"]);
        if let Err(e) = run_git(prune_cmd, self.local_timeout, "git worktree prune").await {
            eprintln!("warn: {e}");
        }

        let entries = match std::fs::read_dir(worktree_base) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        let mut removed = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let path_str = path.to_string_lossy().to_string();
            if active_worktrees.iter().any(|a| *a == path_str) {
                continue;
            }

            let mut rm_cmd = self.git_cmd(repo_dir);
            rm_cmd.args(["worktree", "remove", &path_str, "--force"]);
            let git_ok = match run_git(rm_cmd, self.local_timeout, "git worktree remove").await {
                Ok(out) if out.status.success() => true,
                Ok(_) => false,
                Err(e) => {
                    eprintln!("warn: {e}");
                    false
                }
            };

            if git_ok || std::fs::remove_dir_all(&path).is_ok() {
                removed.push(path_str);
            }
        }
        removed
    }

    pub async fn remove(&self, repo_dir: &Path, worktree_dir: &Path) -> Result<(), String> {
        let _guard = self.lock.lock().await;

        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["worktree", "remove"]);
        cmd.arg(worktree_dir).arg("--force");
        let rm = run_git(cmd, self.local_timeout, "git worktree remove").await?;

        if !rm.status.success() {
            return Err(format!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&rm.stderr)
            ));
        }

        Ok(())
    }

    /// Delete a local branch. Best-effort: logs but does not propagate errors
    /// (the branch may not exist if the worktree was never fully provisioned).
    pub async fn delete_branch(&self, repo_dir: &Path, branch: &str) {
        let _guard = self.lock.lock().await;

        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["branch", "-D", branch]);
        match run_git(cmd, self.local_timeout, "git branch -D").await {
            Ok(o) if o.status.success() => {}
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                if !stderr.contains("not found") {
                    eprintln!("warn: git branch -D {branch} failed: {stderr}");
                }
            }
            Err(e) => {
                eprintln!("warn: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use std::sync::Arc;

    fn init_git_repo(dir: &Path) {
        let d = dir.to_string_lossy();
        let init = StdCommand::new("git")
            .args(["-C", &d, "init", "-b", "main"])
            .output()
            .unwrap();
        assert!(init.status.success(), "git init failed");
        StdCommand::new("git")
            .args(["-C", &d, "config", "user.email", "test@test.com"])
            .output()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "config", "user.name", "Test"])
            .output()
            .unwrap();
        let commit = StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "init"])
            .output()
            .unwrap();
        assert!(
            commit.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&commit.stderr)
        );
    }

    #[cfg(unix)]
    fn create_hanging_shim(dir: &Path) -> PathBuf {
        let shim = dir.join("git-hang");
        std::fs::write(&shim, "#!/bin/sh\nexec sleep 3600\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        shim
    }

    #[cfg(unix)]
    fn short_timeouts() -> (Duration, Duration) {
        (Duration::from_millis(300), Duration::from_millis(300))
    }

    #[tokio::test]
    async fn provision_and_remove() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("test-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .provision(repo_dir.path(), "test-branch", &wt_path, "main")
            .await;
        assert!(result.is_ok(), "provision failed: {:?}", result.err());
        assert!(wt_path.exists());

        let rm_result = mgr.remove(repo_dir.path(), &wt_path).await;
        assert!(rm_result.is_ok(), "remove failed: {:?}", rm_result.err());
        assert!(!wt_path.exists());
    }

    fn git_rev_parse(dir: &Path, rev: &str) -> String {
        let out = StdCommand::new("git")
            .args(["-C", &dir.to_string_lossy(), "rev-parse", rev])
            .output()
            .unwrap();
        assert!(out.status.success(), "rev-parse {rev} failed");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn fetch_and_provision_uses_branch_head_not_main() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let d = repo_dir.path().to_string_lossy().to_string();

        // Add origin pointing to self (mirrors integration test setup)
        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        // Create a feature branch with an extra commit ahead of main
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/test-pr"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr commit"])
            .status()
            .unwrap();

        let feature_head = git_rev_parse(repo_dir.path(), "feature/test-pr");
        let main_head = git_rev_parse(repo_dir.path(), "main");
        assert_ne!(feature_head, main_head, "feature must be ahead of main");

        // Switch back to main so the worktree can check out the feature branch
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("reviewer-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_and_provision(
                repo_dir.path(),
                "review/pr-1-TestReviewer",
                &wt_path,
                "feature/test-pr",
            )
            .await;
        assert!(
            result.is_ok(),
            "fetch_and_provision failed: {:?}",
            result.err()
        );

        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(
            wt_head, feature_head,
            "reviewer worktree HEAD should be at the PR branch head, not main"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn provision_remove_delete_branch_then_reprovision() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("worker-wt");
        let branch = "daemon/bellows-t42";

        let mgr = WorktreeManager::new();

        // First cycle: provision, remove worktree, delete branch
        mgr.provision(repo_dir.path(), branch, &wt_path, "main")
            .await
            .expect("first provision should succeed");
        assert!(wt_path.exists());

        mgr.remove(repo_dir.path(), &wt_path)
            .await
            .expect("remove should succeed");
        mgr.delete_branch(repo_dir.path(), branch).await;
        assert!(!wt_path.exists());

        // Second cycle: same branch name must succeed
        let result = mgr
            .provision(repo_dir.path(), branch, &wt_path, "main")
            .await;
        assert!(
            result.is_ok(),
            "re-provision with same branch should succeed after delete_branch: {:?}",
            result.err()
        );
        assert!(wt_path.exists());

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// Recovery scenario: worktree removed but branch survives. provision()
    /// must reuse the existing branch and preserve its commits.
    #[tokio::test]
    async fn provision_reuses_existing_branch_preserving_commits() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("worker-wt");
        let branch = "daemon/alloy-t9";

        let mgr = WorktreeManager::new();

        // First cycle: provision, make a commit in the worktree, then remove
        // worktree WITHOUT deleting branch (simulates recovery GC).
        mgr.provision(repo_dir.path(), branch, &wt_path, "main")
            .await
            .expect("first provision");
        let d = wt_path.to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "worker commit"])
            .status()
            .unwrap();
        let worker_head = git_rev_parse(&wt_path, "HEAD");

        mgr.remove(repo_dir.path(), &wt_path).await.unwrap();
        // Branch still exists (recovery doesn't delete branches)
        let branch_check = StdCommand::new("git")
            .args([
                "-C",
                &repo_dir.path().to_string_lossy(),
                "rev-parse",
                "--verify",
                &format!("refs/heads/{branch}"),
            ])
            .output()
            .unwrap();
        assert!(branch_check.status.success(), "branch must survive removal");

        // Second cycle: provision should reuse the existing branch
        let wt_path2 = wt_dir.path().join("worker-wt-2");
        let result = mgr
            .provision(repo_dir.path(), branch, &wt_path2, "main")
            .await;
        assert!(
            result.is_ok(),
            "provision with existing branch should succeed: {:?}",
            result.err()
        );

        // Worker commit must be preserved
        let head_after = git_rev_parse(&wt_path2, "HEAD");
        assert_eq!(
            head_after, worker_head,
            "reused branch must preserve the worker's commits"
        );

        mgr.remove(repo_dir.path(), &wt_path2).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// Collision: provision must fail when the branch is checked out in
    /// another worktree.
    #[tokio::test]
    async fn provision_detects_worktree_collision() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt1 = wt_dir.path().join("wt-1");
        let wt2 = wt_dir.path().join("wt-2");
        let branch = "daemon/collide-t1";

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), branch, &wt1, "main")
            .await
            .unwrap();

        let result = mgr.provision(repo_dir.path(), branch, &wt2, "main").await;
        assert!(result.is_err(), "should detect collision");
        let err = result.unwrap_err();
        assert!(
            err.contains("branch collision"),
            "error should mention collision, got: {err}"
        );

        mgr.remove(repo_dir.path(), &wt1).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// fetch_and_provision must handle a stale local branch left from a
    /// prior reviewer incarnation (delete + recreate at remote head).
    #[tokio::test]
    async fn fetch_and_provision_cleans_stale_branch() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());
        let d = repo_dir.path().to_string_lossy().to_string();

        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        // Create a feature branch
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/pr-branch"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr work"])
            .status()
            .unwrap();
        let feature_head = git_rev_parse(repo_dir.path(), "feature/pr-branch");

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        // Create a stale local review branch (simulates surviving branch)
        StdCommand::new("git")
            .args(["-C", &d, "branch", "review/pr-1-Rev0", "main"])
            .status()
            .unwrap();
        let stale_head = git_rev_parse(repo_dir.path(), "review/pr-1-Rev0");
        assert_ne!(stale_head, feature_head, "stale branch should differ");

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("reviewer-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_and_provision(
                repo_dir.path(),
                "review/pr-1-Rev0",
                &wt_path,
                "feature/pr-branch",
            )
            .await;
        assert!(
            result.is_ok(),
            "fetch_and_provision with stale branch should succeed: {:?}",
            result.err()
        );

        // Must point at the remote head, not the stale branch
        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(
            wt_head, feature_head,
            "reviewer worktree must be at remote head, not stale branch"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn delete_branch_nonexistent_does_not_error() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let mgr = WorktreeManager::new();
        // Should not panic or error — best-effort
        mgr.delete_branch(repo_dir.path(), "no-such-branch").await;
    }

    #[tokio::test]
    async fn gc_removes_orphaned_worktrees() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_base = tempfile::tempdir().unwrap();
        let mgr = WorktreeManager::new();

        // Create two worktrees
        let wt1 = wt_base.path().join("active-wt");
        let wt2 = wt_base.path().join("orphan-wt");
        mgr.provision(repo_dir.path(), "branch-active", &wt1, "main")
            .await
            .unwrap();
        mgr.provision(repo_dir.path(), "branch-orphan", &wt2, "main")
            .await
            .unwrap();
        assert!(wt1.exists());
        assert!(wt2.exists());

        // GC with only wt1 as active — wt2 should be removed
        let active = [wt1.to_string_lossy().to_string()];
        let active_refs: Vec<&str> = active.iter().map(|s| s.as_str()).collect();
        let removed = mgr
            .gc_orphaned(repo_dir.path(), wt_base.path(), &active_refs)
            .await;

        assert_eq!(removed.len(), 1);
        assert!(wt1.exists(), "active worktree should NOT be removed");
        assert!(!wt2.exists(), "orphaned worktree should be removed");

        // Clean up
        mgr.remove(repo_dir.path(), &wt1).await.ok();
    }

    // --- Timeout / reap tests (require Unix shims) ---

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_hanging_subprocess() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);
        let tmp = tempfile::tempdir().unwrap();
        let wt = tmp.path().join("wt");

        let start = std::time::Instant::now();
        let result = mgr.provision(tmp.path(), "branch", &wt, "main").await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("timed out"),
            "expected timeout error, got: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "should have timed out quickly, took {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_git_timeout_kills_and_returns() {
        // Directly test run_git: a `sleep 3600` must not block beyond the timeout.
        // If kill_on_drop failed, this call would hang for an hour.
        let mut cmd = Command::new("sleep");
        cmd.arg("3600");

        let start = std::time::Instant::now();
        let result = run_git(cmd, Duration::from_millis(300), "sleep").await;
        let elapsed = start.elapsed();

        assert!(result.unwrap_err().contains("timed out"));
        assert!(
            elapsed < Duration::from_secs(5),
            "run_git should return on timeout, took {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_releases_mutex_for_subsequent_operations() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);
        let tmp = tempfile::tempdir().unwrap();

        // First call times out
        let r1 = mgr
            .provision(tmp.path(), "b1", &tmp.path().join("wt1"), "main")
            .await;
        assert!(r1.unwrap_err().contains("timed out"));

        // Second call also times out — proves mutex was released after first timeout
        let r2 = mgr
            .provision(tmp.path(), "b2", &tmp.path().join("wt2"), "main")
            .await;
        assert!(r2.unwrap_err().contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_concurrent_callers_progress() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = Arc::new(WorktreeManager::with_config(shim, ft, lt));
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_path_buf();

        let m1 = mgr.clone();
        let p1 = base.clone();
        let t1 =
            tokio::spawn(async move { m1.provision(&p1, "b1", &p1.join("wt1"), "main").await });

        let m2 = mgr.clone();
        let p2 = base.clone();
        let t2 =
            tokio::spawn(async move { m2.provision(&p2, "b2", &p2.join("wt2"), "main").await });

        let (r1, r2) = tokio::join!(t1, t2);
        assert!(r1.unwrap().unwrap_err().contains("timed out"));
        assert!(r2.unwrap().unwrap_err().contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_no_orphan_worktree_corruption() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("test-wt");

        // Provision with real git
        let real_mgr = WorktreeManager::new();
        real_mgr
            .provision(repo_dir.path(), "test-branch", &wt_path, "main")
            .await
            .unwrap();
        assert!(wt_path.exists());

        // Try to remove with hanging shim — will timeout
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let hang_mgr = WorktreeManager::with_config(shim, ft, lt);
        let result = hang_mgr.remove(repo_dir.path(), &wt_path).await;
        assert!(result.unwrap_err().contains("timed out"));

        // Worktree still intact — no corruption from killed subprocess
        assert!(wt_path.exists());

        // Real git can still clean up successfully
        real_mgr.remove(repo_dir.path(), &wt_path).await.unwrap();
        assert!(!wt_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn gc_orphaned_timeout_falls_through_to_fs_cleanup() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_base = tempfile::tempdir().unwrap();
        let orphan = wt_base.path().join("orphan-wt");
        std::fs::create_dir(&orphan).unwrap();

        // Hanging shim — git commands timeout, but fs fallback still works
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);

        let removed = mgr.gc_orphaned(repo_dir.path(), wt_base.path(), &[]).await;
        assert!(
            removed.contains(&orphan.to_string_lossy().to_string()),
            "orphan should be cleaned up via fs fallback"
        );
        assert!(!orphan.exists());
    }

    #[tokio::test]
    async fn verify_head_sha_matches() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("verify-wt");

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), "verify-branch", &wt_path, "main")
            .await
            .unwrap();

        let expected_sha = git_rev_parse(&wt_path, "HEAD");
        assert!(mgr.verify_head_sha(&wt_path, &expected_sha).await.is_ok());

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn verify_head_sha_mismatch_fails() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("mismatch-wt");

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), "mismatch-branch", &wt_path, "main")
            .await
            .unwrap();

        let result = mgr
            .verify_head_sha(&wt_path, "0000000000000000000000000000000000000000")
            .await;
        assert!(result.is_err(), "must fail on SHA mismatch");
        let err = result.unwrap_err();
        assert!(
            err.contains("mismatch"),
            "error should mention mismatch: {err}"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    /// Repo with a real bare `origin` remote and `main` pushed. Returns
    /// (repo dir, bare remote dir) inside `base`.
    fn init_repo_with_bare_remote(base: &Path) -> (PathBuf, PathBuf) {
        let bare = base.join("origin.git");
        let repo = base.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        assert!(
            StdCommand::new("git")
                .args(["init", "--bare", "-b", "main", &bare.to_string_lossy()])
                .output()
                .unwrap()
                .status
                .success(),
            "git init --bare failed"
        );
        init_git_repo(&repo);
        let d = repo.to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &bare.to_string_lossy()])
            .status()
            .unwrap();
        assert!(
            StdCommand::new("git")
                .args(["-C", &d, "push", "origin", "main"])
                .output()
                .unwrap()
                .status
                .success(),
            "push main failed"
        );
        (repo, bare)
    }

    /// Create `branch` in the repo with one commit and push it to origin,
    /// leaving the repo back on `main`. Returns the pushed tip SHA.
    fn push_branch(repo: &Path, branch: &str) -> String {
        let d = repo.to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", branch])
            .output()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr work"])
            .output()
            .unwrap();
        assert!(
            StdCommand::new("git")
                .args(["-C", &d, "push", "origin", branch])
                .output()
                .unwrap()
                .status
                .success(),
            "push {branch} failed"
        );
        let tip = git_rev_parse(repo, branch);
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .output()
            .unwrap();
        tip
    }

    fn git_output(dir: &Path, args: &[&str]) -> std::process::Output {
        let mut cmd = StdCommand::new("git");
        cmd.arg("-C").arg(dir).args(args);
        cmd.output().unwrap()
    }

    /// Regression (2026-07-29 mass rework burn): a PR head branch held in
    /// someone else's worktree must not block remediation provisioning. The
    /// daemon checks out a run-unique local name and only fetches the PR head.
    #[tokio::test]
    async fn remediation_provisions_while_pr_head_held_by_other_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, _bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/some-pr-branch";
        let remote_tip = push_branch(&repo, pr_head);

        // An external worktree (a human's checkout) holds the PR head branch.
        let external_wt = tmp.path().join("human-wt");
        assert!(
            git_output(
                &repo,
                &["worktree", "add", &external_wt.to_string_lossy(), pr_head],
            )
            .status
            .success(),
            "external worktree add failed"
        );

        let mgr = WorktreeManager::new();
        let local_branch = "remediation/Alloy-t235";
        let wt_path = tmp.path().join("remediation-wt");
        let result = mgr
            .fetch_and_provision(&repo, local_branch, &wt_path, pr_head)
            .await;
        assert!(
            result.is_ok(),
            "remediation provisioning must not collide with an externally \
             held PR head: {:?}",
            result.err()
        );
        assert_eq!(
            git_rev_parse(&wt_path, "HEAD"),
            remote_tip,
            "remediation worktree must sit at the PR head tip"
        );
        let current = String::from_utf8_lossy(
            &git_output(&wt_path, &["rev-parse", "--abbrev-ref", "HEAD"]).stdout,
        )
        .trim()
        .to_string();
        assert_eq!(
            current, local_branch,
            "remediation worktree must check out the namespaced local branch"
        );

        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, local_branch).await;
    }

    #[tokio::test]
    async fn daemon_push_to_pr_head_rejects_stale_authoritative_sha() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/stale-pr";
        let remote_tip = push_branch(&repo, pr_head);
        let mgr = WorktreeManager::new();
        let wt_path = tmp.path().join("remediation-wt");
        mgr.fetch_and_provision(&repo, "remediation/Brass-t10", &wt_path, pr_head)
            .await
            .expect("provision");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "fix"])
                .status
                .success()
        );
        let source_sha = git_rev_parse(&wt_path, "HEAD");

        let result = mgr
            .push_to_pr_head(&wt_path, pr_head, "not-the-authoritative-sha", &source_sha)
            .await;
        assert!(
            result.is_err(),
            "stale PR head must fail closed: {result:?}"
        );
        assert_eq!(
            git_rev_parse(&bare, pr_head),
            remote_tip,
            "a rejected daemon push must leave the remote PR head unchanged"
        );

        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, "remediation/Brass-t10").await;
    }

    #[tokio::test]
    async fn daemon_push_uses_durable_source_when_head_mutates_after_intent() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/source-sha";
        let remote_tip = push_branch(&repo, pr_head);
        let mgr = WorktreeManager::new();
        let wt_path = tmp.path().join("remediation-wt");
        mgr.fetch_and_provision(&repo, "remediation/Source-t14", &wt_path, pr_head)
            .await
            .expect("provision");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "intent A"])
                .status
                .success()
        );
        let intent_sha = git_rev_parse(&wt_path, "HEAD");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "later B"])
                .status
                .success()
        );
        let mutable_head = git_rev_parse(&wt_path, "HEAD");
        assert_ne!(intent_sha, mutable_head);

        mgr.push_to_pr_head(&wt_path, pr_head, &remote_tip, &intent_sha)
            .await
            .expect("exact durable source must publish");
        assert_eq!(git_rev_parse(&bare, pr_head), intent_sha);
        assert_ne!(git_rev_parse(&bare, pr_head), mutable_head);
    }

    /// Regression for PR #483 remediation review: a publication failure may
    /// remove the source worktree/branch before an operator retries the parked
    /// task. The retry can use a different run-local remediation branch, but
    /// it must still publish intent A rather than its replacement HEAD B.
    #[tokio::test]
    async fn publication_pin_replays_exact_source_after_branch_cleanup_and_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/publication-retry";
        let remote_tip = push_branch(&repo, pr_head);
        let mgr = WorktreeManager::new();

        let first_wt = tmp.path().join("first-remediation");
        let first_branch = "remediation/First-t263";
        mgr.fetch_and_provision(&repo, first_branch, &first_wt, pr_head)
            .await
            .expect("first remediation provision");
        assert!(
            git_output(&first_wt, &["commit", "--allow-empty", "-m", "intent A"])
                .status
                .success()
        );
        let intent_sha = git_rev_parse(&first_wt, "HEAD");
        mgr.pin_publication_source(&first_wt, 263, &intent_sha)
            .await
            .expect("pin exact publication source before remote operations");

        // Publication parks and normal slot cleanup removes all run-local
        // reachability. Only the daemon-owned pin is allowed to retain A.
        mgr.remove(&repo, &first_wt)
            .await
            .expect("remove first worktree");
        mgr.delete_branch(&repo, first_branch).await;
        assert!(!git_output(
            &repo,
            &[
                "show-ref",
                "--verify",
                &format!("refs/heads/{first_branch}")
            ]
        )
        .status
        .success());

        let retry_wt = tmp.path().join("retry-remediation");
        let retry_branch = "remediation/Retry-t263";
        mgr.fetch_and_provision(&repo, retry_branch, &retry_wt, pr_head)
            .await
            .expect("replacement remediation provision");
        assert!(git_output(
            &retry_wt,
            &["commit", "--allow-empty", "-m", "replacement HEAD B"]
        )
        .status
        .success());
        let replacement_head = git_rev_parse(&retry_wt, "HEAD");
        assert_ne!(replacement_head, intent_sha);

        mgr.push_to_pr_head(&retry_wt, pr_head, &remote_tip, &intent_sha)
            .await
            .expect("task retry must replay the durable intent source");
        assert_eq!(git_rev_parse(&bare, pr_head), intent_sha);
        assert_ne!(git_rev_parse(&bare, pr_head), replacement_head);

        mgr.retire_publication_source(&repo, 263, &intent_sha)
            .await
            .expect("successful lifecycle handoff retires the pin");
        assert!(
            !git_output(
                &repo,
                &[
                    "show-ref",
                    "--verify",
                    &WorktreeManager::publication_ref(263),
                ],
            )
            .status
            .success(),
            "retired publication source must not leak a permanent Git ref"
        );
    }

    #[tokio::test]
    async fn force_with_lease_rejects_writer_racing_between_fetch_and_push() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/raced-pr";
        let remote_tip = push_branch(&repo, pr_head);
        let wt_path = tmp.path().join("remediation-wt");
        WorktreeManager::new()
            .fetch_and_provision(&repo, "remediation/Rivet-t11", &wt_path, pr_head)
            .await
            .expect("provision");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "daemon fix"])
                .status
                .success()
        );

        let rival = tmp.path().join("rival");
        assert!(StdCommand::new("git")
            .args(["clone", &bare.to_string_lossy(), &rival.to_string_lossy()])
            .output()
            .unwrap()
            .status
            .success());
        git_output(&rival, &["config", "user.email", "rival@example.com"]);
        git_output(&rival, &["config", "user.name", "Rival"]);
        git_output(&rival, &["checkout", pr_head]);
        assert!(
            git_output(&rival, &["commit", "--allow-empty", "-m", "racing writer"])
                .status
                .success()
        );
        let rival_sha = git_rev_parse(&rival, "HEAD");

        let real_git = String::from_utf8_lossy(
            &StdCommand::new("sh")
                .args(["-c", "command -v git"])
                .output()
                .unwrap()
                .stdout,
        )
        .trim()
        .to_string();
        let shim = tmp.path().join("git-race");
        std::fs::write(
            &shim,
            format!(
                "#!/bin/sh\ncase \" $* \" in *\" push \"*) \"{real_git}\" -C \"{}\" push origin HEAD:refs/heads/{pr_head} >/dev/null 2>&1 ;; esac\nexec \"{real_git}\" \"$@\"\n",
                rival.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mgr =
            WorktreeManager::with_config(shim, Duration::from_secs(10), Duration::from_secs(10));
        let source_sha = git_rev_parse(&wt_path, "HEAD");
        let result = mgr
            .push_to_pr_head(&wt_path, pr_head, &remote_tip, &source_sha)
            .await;
        assert!(result.is_err(), "racing writer must defeat the lease");
        assert_eq!(
            git_rev_parse(&bare, pr_head),
            rival_sha,
            "failed daemon push must preserve the racing writer's commit"
        );
    }

    #[tokio::test]
    async fn zero_lease_rejects_writer_racing_to_create_initial_branch() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let branch = "daemon/new-t12";
        let wt_path = tmp.path().join("worker-wt");
        WorktreeManager::new()
            .provision(&repo, branch, &wt_path, "origin/main")
            .await
            .expect("provision");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "daemon work"])
                .status
                .success()
        );

        let rival = tmp.path().join("rival");
        assert!(StdCommand::new("git")
            .args(["clone", &bare.to_string_lossy(), &rival.to_string_lossy()])
            .output()
            .unwrap()
            .status
            .success());
        git_output(&rival, &["config", "user.email", "rival@example.com"]);
        git_output(&rival, &["config", "user.name", "Rival"]);
        assert!(
            git_output(&rival, &["commit", "--allow-empty", "-m", "claim branch"])
                .status
                .success()
        );
        let rival_sha = git_rev_parse(&rival, "HEAD");
        let real_git = String::from_utf8_lossy(
            &StdCommand::new("sh")
                .args(["-c", "command -v git"])
                .output()
                .unwrap()
                .stdout,
        )
        .trim()
        .to_string();
        let shim = tmp.path().join("git-zero-race");
        std::fs::write(
            &shim,
            format!(
                "#!/bin/sh\ncase \" $* \" in *\" push \"*) \"{real_git}\" -C \"{}\" push origin HEAD:refs/heads/{branch} >/dev/null 2>&1 ;; esac\nexec \"{real_git}\" \"$@\"\n",
                rival.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mgr =
            WorktreeManager::with_config(shim, Duration::from_secs(10), Duration::from_secs(10));
        let source_sha = git_rev_parse(&wt_path, "HEAD");
        let result = mgr.push_new_branch(&wt_path, branch, &source_sha).await;
        assert!(
            result.is_err(),
            "racing branch creation must defeat zero lease"
        );
        assert_eq!(git_rev_parse(&bare, branch), rival_sha);
    }

    #[tokio::test]
    async fn initial_push_reconciles_crash_after_remote_update() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let branch = "daemon/crash-window-t13";
        let wt_path = tmp.path().join("worker-wt");
        let mgr = WorktreeManager::new();
        mgr.provision(&repo, branch, &wt_path, "origin/main")
            .await
            .expect("provision");
        assert!(
            git_output(&wt_path, &["commit", "--allow-empty", "-m", "work"])
                .status
                .success()
        );
        let local_sha = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(
            mgr.push_new_branch(&wt_path, branch, &local_sha)
                .await
                .unwrap(),
            local_sha
        );
        // Simulate restart after the remote accepted the push but before the
        // daemon persisted the "pushed" stage.
        assert_eq!(
            mgr.push_new_branch(&wt_path, branch, &local_sha)
                .await
                .unwrap(),
            local_sha
        );
        assert_eq!(git_rev_parse(&bare, branch), local_sha);
    }

    /// The protocol reserves publication for the daemon and best-effort blocks
    /// ordinary agent pushes; credential enforcement is the separate D4 boundary.
    #[tokio::test]
    async fn daemon_push_to_pr_head_updates_only_authoritative_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/upstream-pr";
        let remote_tip = push_branch(&repo, pr_head);

        let mgr = WorktreeManager::new();
        let local_branch = "remediation/Bolt-t9";
        let wt_path = tmp.path().join("remediation-wt");
        mgr.fetch_and_provision(&repo, local_branch, &wt_path, pr_head)
            .await
            .expect("provision");
        mgr.disable_push(&wt_path)
            .await
            .expect("disable agent push");

        assert!(
            git_output(
                &wt_path,
                &["commit", "--allow-empty", "-m", "remediation fix"]
            )
            .status
            .success(),
            "commit in remediation worktree failed"
        );
        let new_tip = git_rev_parse(&wt_path, "HEAD");
        assert_ne!(new_tip, remote_tip);
        let push = git_output(&wt_path, &["push"]);
        assert!(
            !push.status.success(),
            "agent plain git push must fail: {}",
            String::from_utf8_lossy(&push.stderr)
        );

        let pushed = mgr
            .push_to_pr_head(&wt_path, pr_head, &remote_tip, &new_tip)
            .await
            .expect("daemon push must succeed");
        assert_eq!(pushed, new_tip);

        assert_eq!(
            git_rev_parse(&bare, pr_head),
            new_tip,
            "daemon push must advance the PR head branch"
        );

        assert!(
            !git_output(
                &bare,
                &[
                    "rev-parse",
                    "--verify",
                    &format!("refs/heads/{local_branch}")
                ]
            )
            .status
            .success(),
            "push must NOT create a remote branch for the local namespaced name"
        );

        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, local_branch).await;
    }

    /// The shared repo config must not be reconfigured when `core.bare` or
    /// `core.worktree` is already set — enabling per-worktree config changes
    /// how git resolves those keys for every worktree.
    #[tokio::test]
    async fn worktree_config_refuses_when_core_worktree_is_set() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, _bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/core-worktree";
        push_branch(&repo, pr_head);

        let mgr = WorktreeManager::new();
        let local_branch = "remediation/quill-t3";
        let wt_path = tmp.path().join("remediation-wt");
        mgr.fetch_and_provision(&repo, local_branch, &wt_path, pr_head)
            .await
            .expect("provision");
        git_output(&repo, &["config", "core.worktree", &repo.to_string_lossy()]);

        let result = mgr.disable_push(&wt_path).await;
        assert!(result.is_err(), "must refuse, got {result:?}");
        assert!(
            !git_output(&repo, &["config", "--get", "extensions.worktreeConfig"])
                .status
                .success(),
            "refusal must not have enabled the extension"
        );

        StdCommand::new("git")
            .args([
                "config",
                "--file",
                &repo.join(".git/config").to_string_lossy(),
                "--unset",
                "core.worktree",
            ])
            .output()
            .unwrap();
        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, local_branch).await;
    }

    #[tokio::test]
    async fn worktree_config_refuses_when_core_worktree_is_false() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, _bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/core-worktree-false";
        push_branch(&repo, pr_head);

        let mgr = WorktreeManager::new();
        let local_branch = "remediation/quill-t4";
        let wt_path = tmp.path().join("remediation-wt");
        mgr.fetch_and_provision(&repo, local_branch, &wt_path, pr_head)
            .await
            .expect("provision");
        git_output(&repo, &["config", "core.worktree", "false"]);

        let result = mgr.disable_push(&wt_path).await;
        assert!(result.is_err(), "must refuse, got {result:?}");
        assert!(
            !git_output(&repo, &["config", "--get", "extensions.worktreeConfig"])
                .status
                .success(),
            "refusal must not have enabled the extension"
        );

        StdCommand::new("git")
            .args([
                "config",
                "--file",
                &repo.join(".git/config").to_string_lossy(),
                "--unset",
                "core.worktree",
            ])
            .output()
            .unwrap();
        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, local_branch).await;
    }

    #[tokio::test]
    async fn worktree_config_accepts_falseish_core_bare() {
        for falseish in ["false", "0", "no", "off"] {
            let tmp = tempfile::tempdir().unwrap();
            let (repo, _bare) = init_repo_with_bare_remote(tmp.path());
            let pr_head = "fix/core-bare-false";
            push_branch(&repo, pr_head);

            let mgr = WorktreeManager::new();
            let local_branch = "remediation/quill-t5";
            let wt_path = tmp.path().join("remediation-wt");
            mgr.fetch_and_provision(&repo, local_branch, &wt_path, pr_head)
                .await
                .expect("provision");
            git_output(&repo, &["config", "core.bare", falseish]);

            mgr.disable_push(&wt_path)
                .await
                .unwrap_or_else(|e| panic!("core.bare={falseish} must be accepted: {e}"));

            mgr.remove(&repo, &wt_path).await.ok();
            mgr.delete_branch(&repo, local_branch).await;
        }
    }

    /// Reviewers read and comment; they never push. The lockout must block
    /// pushes while leaving fetch working.
    #[tokio::test]
    async fn disable_push_blocks_reviewer_pushes_but_not_fetch() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, _bare) = init_repo_with_bare_remote(tmp.path());
        let pr_head = "fix/reviewed-pr";
        push_branch(&repo, pr_head);

        let mgr = WorktreeManager::new();
        let review_branch = "review/pr-7-Lever";
        let wt_path = tmp.path().join("reviewer-wt");
        mgr.fetch_and_provision(&repo, review_branch, &wt_path, pr_head)
            .await
            .expect("provision");
        mgr.disable_push(&wt_path).await.expect("disable push");

        assert!(git_output(
            &wt_path,
            &["commit", "--allow-empty", "-m", "reviewer edit"]
        )
        .status
        .success());
        let push = git_output(&wt_path, &["push", "origin", "HEAD:refs/heads/whatever"]);
        assert!(
            !push.status.success(),
            "reviewer push must fail, got success: {}",
            String::from_utf8_lossy(&push.stdout)
        );

        let fetch = git_output(&wt_path, &["fetch", "origin", pr_head]);
        assert!(
            fetch.status.success(),
            "reviewer fetch must still work: {}",
            String::from_utf8_lossy(&fetch.stderr)
        );

        // The lockout is worktree-scoped: the shared checkout can still push.
        assert!(
            git_output(&repo, &["config", "--get", "remote.origin.pushurl"])
                .stdout
                .is_empty(),
            "pushurl must not be set repo-wide"
        );

        mgr.remove(&repo, &wt_path).await.ok();
        mgr.delete_branch(&repo, review_branch).await;
    }

    #[tokio::test]
    async fn fetch_pr_and_provision_same_repo() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());
        let d = repo_dir.path().to_string_lossy().to_string();

        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/pr-42"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr commit"])
            .status()
            .unwrap();
        let feature_head = git_rev_parse(repo_dir.path(), "feature/pr-42");

        // Simulate GitHub's refs/pull/<pr>/head by creating the ref manually
        StdCommand::new("git")
            .args(["-C", &d, "update-ref", "refs/pull/42/head", &feature_head])
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("pr-review-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_pr_and_provision(repo_dir.path(), "review/pr-42-test", &wt_path, 42)
            .await;
        assert!(
            result.is_ok(),
            "fetch_pr_and_provision failed: {:?}",
            result.err()
        );

        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(wt_head, feature_head, "worktree must be at PR head");

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }
}
