//! Serialized git worktree operations for agent isolation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::Mutex;

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_LOCAL_TIMEOUT: Duration = Duration::from_secs(30);
const GIT_PIPE_LIMIT: usize = 1024 * 1024;

pub struct WorktreeManager {
    lock: Mutex<()>,
    git_bin: PathBuf,
    fetch_timeout: Duration,
    local_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationBaseMerge {
    Clean,
    Conflicted,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PublicationRefReconcileResult {
    pub kept: usize,
    pub restored: usize,
    pub retired: usize,
}

struct GitPipeOutput {
    bytes: Vec<u8>,
    exceeded_limit: bool,
}

async fn drain_git_pipe<R>(
    mut pipe: R,
    limit: usize,
    pipe_name: &'static str,
    overflow_tx: tokio::sync::mpsc::Sender<&'static str>,
) -> std::io::Result<GitPipeOutput>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0_u8; 8192];
    let mut exceeded_limit = false;
    loop {
        let count = pipe.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let retained = count.min(limit.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&buffer[..retained]);
        if retained < count && !exceeded_limit {
            exceeded_limit = true;
            let _ = overflow_tx.try_send(pipe_name);
        }
    }
    Ok(GitPipeOutput {
        bytes,
        exceeded_limit,
    })
}

/// Run a git subprocess with fixed output bounds and cancellation-safe process
/// ownership. A timeout or output overflow explicitly kills and reaps the
/// child; kill-on-drop covers daemon shutdown while the command is live.
async fn run_git(
    cmd: Command,
    timeout: Duration,
    label: &str,
) -> Result<std::process::Output, String> {
    run_git_with_limit(cmd, timeout, GIT_PIPE_LIMIT, label).await
}

async fn run_git_with_limit(
    mut cmd: Command,
    timeout: Duration,
    pipe_limit: usize,
    label: &str,
) -> Result<std::process::Output, String> {
    cmd.kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|error| format!("{label}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{label}: stdout pipe unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{label}: stderr pipe unavailable"))?;
    let (overflow_tx, mut overflow_rx) = tokio::sync::mpsc::channel(2);
    let mut stdout_reader = tokio::spawn(drain_git_pipe(
        stdout,
        pipe_limit,
        "stdout",
        overflow_tx.clone(),
    ));
    let mut stderr_reader = tokio::spawn(drain_git_pipe(stderr, pipe_limit, "stderr", overflow_tx));

    let deadline = tokio::time::Instant::now() + timeout;
    let status = tokio::select! {
        result = child.wait() => match result {
            Ok(status) => status,
            Err(error) => {
                let _ = child.kill().await;
                stdout_reader.abort();
                stderr_reader.abort();
                return Err(format!("{label}: {error}"));
            }
        },
        Some(pipe_name) = overflow_rx.recv() => {
            let kill_error = child.kill().await.err();
            stdout_reader.abort();
            stderr_reader.abort();
            return Err(match kill_error {
                Some(error) => format!(
                    "{label}: {pipe_name} exceeded {pipe_limit}-byte limit and kill/reap failed: {error}"
                ),
                None => format!("{label}: {pipe_name} exceeded {pipe_limit}-byte limit"),
            });
        },
        _ = tokio::time::sleep_until(deadline) => {
            let kill_error = child.kill().await.err();
            stdout_reader.abort();
            stderr_reader.abort();
            return Err(match kill_error {
                Some(error) => format!(
                    "{label}: timed out after {}s and kill/reap failed: {error}",
                    timeout.as_secs()
                ),
                None => format!("{label}: timed out after {}s", timeout.as_secs()),
            });
        }
    };

    let readers = async {
        let stdout = (&mut stdout_reader)
            .await
            .map_err(|error| format!("{label}: stdout join: {error}"))?
            .map_err(|error| format!("{label}: stdout read: {error}"))?;
        let stderr = (&mut stderr_reader)
            .await
            .map_err(|error| format!("{label}: stderr join: {error}"))?
            .map_err(|error| format!("{label}: stderr read: {error}"))?;
        Ok::<_, String>((stdout, stderr))
    };
    let (stdout, stderr) = match tokio::time::timeout_at(deadline, readers).await {
        Ok(result) => result?,
        Err(_) => {
            stdout_reader.abort();
            stderr_reader.abort();
            return Err(format!(
                "{label}: output collection timed out after {}s",
                timeout.as_secs()
            ));
        }
    };
    if stdout.exceeded_limit {
        return Err(format!("{label}: stdout exceeded {pipe_limit}-byte limit"));
    }
    if stderr.exceeded_limit {
        return Err(format!("{label}: stderr exceeded {pipe_limit}-byte limit"));
    }
    Ok(std::process::Output {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    })
}

/// Stdin-capable boundary. The owning task is detached on caller cancellation,
/// so it still reaches bounded kill/reap instead of dropping a live child.
async fn run_git_with_input(
    mut cmd: Command,
    input: Vec<u8>,
    timeout: Duration,
    label: &'static str,
) -> Result<std::process::Output, String> {
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        cmd.kill_on_drop(true).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("{label}: {e}"))?;
        let stdout = child.stdout.take().ok_or_else(|| format!("{label}: stdout unavailable"))?;
        let stderr = child.stderr.take().ok_or_else(|| format!("{label}: stderr unavailable"))?;
        let (overflow_tx, mut overflow_rx) = tokio::sync::mpsc::channel(2);
        let mut stdout_reader = tokio::spawn(drain_git_pipe(stdout, GIT_PIPE_LIMIT, "stdout", overflow_tx.clone()));
        let mut stderr_reader = tokio::spawn(drain_git_pipe(stderr, GIT_PIPE_LIMIT, "stderr", overflow_tx));
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&input).await.map_err(|e| format!("{label}: stdin: {e}"))?;
            stdin.shutdown().await.map_err(|e| format!("{label}: stdin shutdown: {e}"))?;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        let status = tokio::select! {
            result = child.wait() => result.map_err(|e| format!("{label}: wait: {e}"))?,
            Some(pipe) = overflow_rx.recv() => {
                let kill = child.kill().await.err();
                stdout_reader.abort(); stderr_reader.abort();
                return Err(match kill { Some(e) => format!("{label}: {pipe} overflow; kill/reap: {e}"), None => format!("{label}: {pipe} exceeded {GIT_PIPE_LIMIT}-byte limit") });
            }
            _ = tokio::time::sleep_until(deadline) => {
                let kill = child.kill().await.err();
                stdout_reader.abort(); stderr_reader.abort();
                return Err(match kill { Some(e) => format!("{label}: timeout; kill/reap: {e}"), None => format!("{label}: timed out after {}s", timeout.as_secs()) });
            }
        };
        let readers = async {
            let stdout = (&mut stdout_reader).await.map_err(|e| format!("{label}: stdout join: {e}"))?.map_err(|e| format!("{label}: stdout: {e}"))?;
            let stderr = (&mut stderr_reader).await.map_err(|e| format!("{label}: stderr join: {e}"))?.map_err(|e| format!("{label}: stderr: {e}"))?;
            Ok::<_, String>((stdout, stderr))
        };
        let (stdout, stderr) = match tokio::time::timeout_at(deadline, readers).await {
            Ok(result) => result?,
            Err(_) => { stdout_reader.abort(); stderr_reader.abort(); return Err(format!("{label}: output collection timed out")); }
        };
        if stdout.exceeded_limit || stderr.exceeded_limit { return Err(format!("{label}: output exceeded {GIT_PIPE_LIMIT}-byte limit")); }
        Ok(std::process::Output { status, stdout: stdout.bytes, stderr: stderr.bytes })
    }).await.map_err(|e| format!("{label}: owner join: {e}"))?
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

    /// Make one bounded batch of task-scoped reachability pins match durable
    /// publication intents.
    ///
    /// The caller supplies a cursor-bounded database batch. Missing/mismatched
    /// pins are restored to the intent SHA, while pins without a retained
    /// intent are deleted with an exact-old-SHA guard. Passing exact ref
    /// patterns keeps Git output bounded by the batch size.
    pub async fn reconcile_publication_sources(
        &self,
        repo_dir: &Path,
        expected: &HashMap<i64, Option<String>>,
    ) -> Result<PublicationRefReconcileResult, String> {
        if expected.is_empty() {
            return Ok(PublicationRefReconcileResult::default());
        }
        let _guard = self.lock.lock().await;
        let mut list = self.git_cmd(repo_dir);
        list.args(["for-each-ref", "--format=%(refname) %(objectname)"]);
        for &task_id in expected.keys() {
            list.arg(Self::publication_ref(task_id));
        }
        let listed = run_git(list, self.local_timeout, "git list publication sources").await?;
        if !listed.status.success() {
            return Err(format!(
                "cannot list publication sources: {}",
                String::from_utf8_lossy(&listed.stderr)
            ));
        }

        let mut actual = HashMap::new();
        for line in String::from_utf8_lossy(&listed.stdout).lines() {
            let Some((refname, sha)) = line.split_once(' ') else {
                return Err(format!("malformed publication ref listing: {line}"));
            };
            let task_id = refname
                .strip_prefix("refs/quorum-publication/task-")
                .and_then(|id| id.parse::<i64>().ok())
                .filter(|id| *id > 0)
                .ok_or_else(|| format!("unexpected publication ref in bounded listing: {line}"))?;
            actual.insert(task_id, sha.to_string());
        }

        let mut result = PublicationRefReconcileResult::default();
        for (&task_id, expected_sha) in expected {
            let refname = Self::publication_ref(task_id);
            let current = actual.remove(&task_id);
            let Some(expected_sha) = expected_sha else {
                if let Some(actual_sha) = current {
                    let mut delete = self.git_cmd(repo_dir);
                    delete.args(["update-ref", "-d", &refname, &actual_sha]);
                    let deleted = run_git(
                        delete,
                        self.local_timeout,
                        "git retire orphan publication source",
                    )
                    .await?;
                    if !deleted.status.success() {
                        return Err(format!(
                            "cannot retire orphan publication source {refname}: {}",
                            String::from_utf8_lossy(&deleted.stderr)
                        ));
                    }
                    result.retired += 1;
                }
                continue;
            };
            match current {
                Some(actual_sha) if actual_sha == *expected_sha => {
                    result.kept += 1;
                }
                current => {
                    let mut resolve = self.git_cmd(repo_dir);
                    resolve.args([
                        "rev-parse",
                        "--verify",
                        &format!("{expected_sha}^{{commit}}"),
                    ]);
                    let resolved = run_git(
                        resolve,
                        self.local_timeout,
                        "git resolve publication intent source",
                    )
                    .await?;
                    if !resolved.status.success()
                        || String::from_utf8_lossy(&resolved.stdout).trim() != expected_sha
                    {
                        return Err(format!(
                            "publication intent for task #{task_id} names unavailable commit {expected_sha}"
                        ));
                    }

                    let old_sha = current
                        .as_deref()
                        .unwrap_or("0000000000000000000000000000000000000000");
                    let mut update = self.git_cmd(repo_dir);
                    update.args(["update-ref", &refname, expected_sha, old_sha]);
                    let updated =
                        run_git(update, self.local_timeout, "git restore publication source")
                            .await?;
                    if !updated.status.success() {
                        return Err(format!(
                            "cannot restore publication source for task #{task_id}: {}",
                            String::from_utf8_lossy(&updated.stderr)
                        ));
                    }
                    result.restored += 1;
                }
            }
        }
        Ok(result)
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

    /// Refresh and merge the configured base into an exact continuation PR
    /// checkout. A content conflict is a prepared worker state, not a setup
    /// failure: Git leaves `MERGE_HEAD` plus the index/worktree conflicts for
    /// the worker to resolve. Every other non-zero merge result fails loud.
    pub async fn integrate_continuation_base(
        &self,
        worktree_dir: &Path,
        base_branch: &str,
    ) -> Result<ContinuationBaseMerge, String> {
        let _guard = self.lock.lock().await;
        let remote_ref = format!("refs/heads/{base_branch}");
        let tracking_ref = format!("refs/remotes/origin/{base_branch}");
        let refspec = format!("+{remote_ref}:{tracking_ref}");

        let mut fetch = self.git_cmd(worktree_dir);
        fetch.args(["fetch", "origin", &refspec]);
        let fetched = run_git(fetch, self.fetch_timeout, "git fetch continuation base").await?;
        if !fetched.status.success() {
            return Err(format!(
                "git fetch origin {remote_ref} failed: {}",
                String::from_utf8_lossy(&fetched.stderr)
            ));
        }

        let base_ref = format!("origin/{base_branch}");
        let mut merge = self.git_cmd(worktree_dir);
        merge.args(["merge", "--no-edit", &base_ref]);
        let merged = run_git(merge, self.local_timeout, "git merge continuation base").await?;
        if merged.status.success() {
            return Ok(ContinuationBaseMerge::Clean);
        }

        let mut merge_head = self.git_cmd(worktree_dir);
        merge_head.args(["rev-parse", "--verify", "MERGE_HEAD"]);
        let merge_head = run_git(
            merge_head,
            self.local_timeout,
            "git verify continuation merge conflict",
        )
        .await?;
        let mut conflicts = self.git_cmd(worktree_dir);
        conflicts.args(["diff", "--name-only", "--diff-filter=U"]);
        let conflicts = run_git(
            conflicts,
            self.local_timeout,
            "git list continuation merge conflicts",
        )
        .await?;
        if merge_head.status.success() && conflicts.status.success() && !conflicts.stdout.is_empty()
        {
            Ok(ContinuationBaseMerge::Conflicted)
        } else {
            Err(format!(
                "git merge {base_ref} failed without leaving a resolvable merge: {}",
                String::from_utf8_lossy(&merged.stderr)
            ))
        }
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

    /// Remove only when git still reports the exact path/branch binding captured
    /// by a durable cleanup intent. A missing worktree is idempotent success.
    pub async fn remove_exact(
        &self,
        repo_dir: &Path,
        worktree_dir: &Path,
        branch: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let mut list = self.git_cmd(repo_dir);
        list.args(["worktree", "list", "--porcelain"]);
        let out = run_git(list, self.local_timeout, "git worktree list").await?;
        if !out.status.success() {
            return Err(format!(
                "git worktree list failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let expected_ref = format!("refs/heads/{branch}");
        let expected_path = std::fs::canonicalize(worktree_dir).ok();
        let mut found = None;
        let mut path = None;
        let mut bound_branch = None;
        for line in String::from_utf8_lossy(&out.stdout)
            .lines()
            .chain(std::iter::once(""))
        {
            if let Some(value) = line.strip_prefix("worktree ") {
                path = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("branch ") {
                bound_branch = Some(value.to_string());
            } else if line.is_empty() {
                if path.as_deref().is_some_and(|listed| {
                    let listed = std::fs::canonicalize(listed).ok();
                    listed.is_some() && listed == expected_path
                }) {
                    found = Some(bound_branch.clone());
                }
                path = None;
                bound_branch = None;
            }
        }
        let Some(actual) = found else {
            return match std::fs::symlink_metadata(worktree_dir) {
                Ok(_) => Err("worktree path exists without exact git registration".into()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(format!("cannot prove worktree path absence: {error}")),
            };
        };
        if actual.as_deref() != Some(expected_ref.as_str()) {
            return Err(format!(
                "worktree identity mismatch: expected {expected_ref}, found {actual:?}"
            ));
        }
        let mut rm = self.git_cmd(repo_dir);
        rm.args(["worktree", "remove"])
            .arg(worktree_dir)
            .arg("--force");
        let out = run_git(rm, self.local_timeout, "git worktree remove").await?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    }

    /// Delete an exact local branch object. Missing is success; moved refs fail.
    #[cfg(test)]
    pub async fn delete_branch_exact(
        &self,
        repo_dir: &Path,
        branch: &str,
        expected_sha: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let mut resolve = self.git_cmd(repo_dir);
        resolve.args(["rev-parse", "--verify", &format!("refs/heads/{branch}")]);
        let out = run_git(resolve, self.local_timeout, "git rev-parse cleanup branch").await?;
        if !out.status.success() {
            return Ok(());
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if actual != expected_sha {
            return Err(format!(
                "branch SHA mismatch: expected {expected_sha}, found {actual}"
            ));
        }
        let mut del = self.git_cmd(repo_dir);
        del.args([
            "update-ref",
            "-d",
            &format!("refs/heads/{branch}"),
            expected_sha,
        ]);
        let out = run_git(
            del,
            self.local_timeout,
            "git update-ref delete cleanup branch",
        )
        .await?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "git update-ref delete {branch} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    }

    /// Resolve a daemon branch only when its current head descends from the
    /// immutable allocation provenance. Missing branches are idempotent.
    pub async fn discover_branch_head(
        &self,
        repo_dir: &Path,
        branch: &str,
        provenance_sha: &str,
    ) -> Result<Option<String>, String> {
        let _guard = self.lock.lock().await;
        let mut resolve = self.git_cmd(repo_dir);
        resolve.args(["rev-parse", "--verify", &format!("refs/heads/{branch}")]);
        let out = run_git(resolve, self.local_timeout, "git resolve cleanup branch").await?;
        if !out.status.success() {
            return Ok(None);
        }
        let current = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let mut ancestry = self.git_cmd(repo_dir);
        ancestry.args(["merge-base", "--is-ancestor", provenance_sha, &current]);
        let out = run_git(
            ancestry,
            self.local_timeout,
            "git validate cleanup ancestry",
        )
        .await?;
        if !out.status.success() {
            return Err(format!(
                "branch {branch} is not a descendant of allocation provenance {provenance_sha}"
            ));
        }
        Ok(Some(current))
    }

    pub async fn resolve_ref_sha(
        &self,
        repo_dir: &Path,
        reference: &str,
    ) -> Result<String, String> {
        let _guard = self.lock.lock().await;
        let mut resolve = self.git_cmd(repo_dir);
        resolve.args(["rev-parse", "--verify", reference]);
        let out = run_git(
            resolve,
            self.local_timeout,
            "git resolve provisioning provenance",
        )
        .await?;
        if !out.status.success() {
            return Err(format!(
                "cannot resolve provisioning provenance {reference}"
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .trim()
            .to_ascii_lowercase())
    }

    /// Atomically tombstone the exact old object and delete its branch. A
    /// replay consumes the tombstone without touching a recreated branch.
    pub async fn delete_branch_with_tombstone(
        &self,
        repo_dir: &Path,
        remote_url: &str,
        branch: &str,
        expected_sha: &str,
        tombstone_ref: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let remote_ref = format!("refs/heads/{branch}");
        let remote_tombstone_ref = format!(
            "refs/heads/quorum-cleanup/{}",
            tombstone_ref
                .strip_prefix("refs/quorum/cleanup/")
                .ok_or_else(|| "invalid cleanup tombstone namespace".to_string())?
        );
        let mut remote_refs = self.git_cmd(repo_dir);
        remote_refs.args(["ls-remote", remote_url, &remote_ref, &remote_tombstone_ref]);
        let remote_out = run_git(
            remote_refs,
            self.fetch_timeout,
            "git resolve remote cleanup refs",
        )
        .await?;
        if !remote_out.status.success() {
            return Err(format!(
                "cannot resolve remote cleanup refs: {}",
                String::from_utf8_lossy(&remote_out.stderr)
            ));
        }
        let mut remote_branch_sha = None;
        let mut remote_tombstone_sha = None;
        for line in String::from_utf8_lossy(&remote_out.stdout).lines() {
            let mut fields = line.split_whitespace();
            let sha = fields.next().unwrap_or_default();
            match fields.next().unwrap_or_default() {
                name if name == remote_ref => remote_branch_sha = Some(sha.to_string()),
                name if name == remote_tombstone_ref => {
                    remote_tombstone_sha = Some(sha.to_string())
                }
                _ => return Err("unexpected remote cleanup ref response".into()),
            }
        }
        if let Some(actual) = remote_tombstone_sha {
            if actual != expected_sha {
                return Err("remote cleanup tombstone identity mismatch".into());
            }
        } else {
            if let Some(actual) = remote_branch_sha.as_deref() {
                if actual != expected_sha {
                    return Err(format!(
                        "remote branch SHA mismatch: expected {expected_sha}, found {actual}"
                    ));
                }
            }
            let tombstone_refspec = format!("{expected_sha}:{remote_tombstone_ref}");
            let tombstone_lease = format!("--force-with-lease={remote_tombstone_ref}:");
            let branch_lease = remote_branch_sha
                .as_ref()
                .map(|_| format!("--force-with-lease={remote_ref}:{expected_sha}"));
            let mut remote_delete = self.git_cmd(repo_dir);
            remote_delete.arg("push").arg("--atomic").arg(remote_url);
            remote_delete.arg(&tombstone_lease);
            if let Some(lease) = &branch_lease {
                remote_delete.arg(lease);
            }
            remote_delete.arg(&tombstone_refspec);
            if remote_branch_sha.is_some() {
                remote_delete.arg(format!(":{remote_ref}"));
            }
            let deleted = run_git(
                remote_delete,
                self.fetch_timeout,
                "git tombstone and delete remote cleanup branch",
            )
            .await?;
            if !deleted.status.success() {
                return Err(format!(
                    "remote cleanup branch CAS deletion failed: {}",
                    String::from_utf8_lossy(&deleted.stderr)
                ));
            }
        }
        let mut tombstone = self.git_cmd(repo_dir);
        tombstone.args(["rev-parse", "--verify", tombstone_ref]);
        let tombstone_out = run_git(
            tombstone,
            self.local_timeout,
            "git resolve cleanup tombstone",
        )
        .await?;
        if tombstone_out.status.success() {
            let actual = String::from_utf8_lossy(&tombstone_out.stdout)
                .trim()
                .to_string();
            if actual != expected_sha {
                return Err("cleanup tombstone identity mismatch".into());
            }
            // The atomic transaction already deleted the leased branch. Keep
            // the tombstone through durable DB settlement so a same-name
            // recreation can never be mistaken for the original lease.
            return Ok(());
        }
        let mut branch_ref = self.git_cmd(repo_dir);
        branch_ref.args(["rev-parse", "--verify", &format!("refs/heads/{branch}")]);
        let branch_out = run_git(
            branch_ref,
            self.local_timeout,
            "git resolve cleanup branch delete",
        )
        .await?;
        let branch_present = branch_out.status.success();
        if branch_present {
            let actual = String::from_utf8_lossy(&branch_out.stdout)
                .trim()
                .to_string();
            if actual != expected_sha {
                return Err(format!(
                    "branch SHA mismatch: expected {expected_sha}, found {actual}"
                ));
            }
        }
        let mut command = self.git_cmd(repo_dir);
        command.args(["update-ref", "--stdin"]);
        let null_oid = "0".repeat(expected_sha.len());
        let branch_op = if branch_present {
            format!("delete refs/heads/{branch} {expected_sha}\n")
        } else {
            format!("verify refs/heads/{branch} {null_oid}\n")
        };
        let input =
            format!("start\ncreate {tombstone_ref} {expected_sha}\n{branch_op}prepare\ncommit\n")
                .into_bytes();
        let out = run_git_with_input(
            command,
            input,
            self.local_timeout,
            "git update-ref cleanup transaction",
        )
        .await?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "git update-ref transaction failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    }

    /// Retire a settled cleanup tombstone only at its exact leased object.
    pub async fn retire_cleanup_tombstone(
        &self,
        repo_dir: &Path,
        remote_url: &str,
        tombstone_ref: &str,
        expected_sha: &str,
    ) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        let remote_tombstone_ref = format!(
            "refs/heads/quorum-cleanup/{}",
            tombstone_ref
                .strip_prefix("refs/quorum/cleanup/")
                .ok_or_else(|| "invalid cleanup tombstone namespace".to_string())?
        );
        let mut remote_resolve = self.git_cmd(repo_dir);
        remote_resolve.args(["ls-remote", remote_url, &remote_tombstone_ref]);
        let resolved = run_git(
            remote_resolve,
            self.fetch_timeout,
            "git resolve settled remote cleanup tombstone",
        )
        .await?;
        if !resolved.status.success() {
            return Err(format!(
                "cannot resolve settled remote cleanup tombstone: {}",
                String::from_utf8_lossy(&resolved.stderr)
            ));
        }
        let actual = String::from_utf8_lossy(&resolved.stdout)
            .split_whitespace()
            .next()
            .map(str::to_string);
        if let Some(actual) = actual.as_deref() {
            if actual != expected_sha {
                return Err(format!(
                    "settled remote cleanup tombstone mismatch: expected {expected_sha}, found {actual}"
                ));
            }
        } else {
            return self
                .retire_local_cleanup_tombstone(repo_dir, tombstone_ref, expected_sha)
                .await;
        }
        let mut remote_delete = self.git_cmd(repo_dir);
        remote_delete
            .arg("push")
            .arg(remote_url)
            .arg(format!(
                "--force-with-lease={remote_tombstone_ref}:{expected_sha}"
            ))
            .arg(format!(":{remote_tombstone_ref}"));
        let remote_out = run_git(
            remote_delete,
            self.fetch_timeout,
            "git retire remote cleanup tombstone",
        )
        .await?;
        if !remote_out.status.success() {
            return Err(format!(
                "remote cleanup tombstone CAS deletion failed: {}",
                String::from_utf8_lossy(&remote_out.stderr)
            ));
        }
        self.retire_local_cleanup_tombstone(repo_dir, tombstone_ref, expected_sha)
            .await
    }

    async fn retire_local_cleanup_tombstone(
        &self,
        repo_dir: &Path,
        tombstone_ref: &str,
        expected_sha: &str,
    ) -> Result<(), String> {
        let mut resolve = self.git_cmd(repo_dir);
        resolve.args(["rev-parse", "--verify", tombstone_ref]);
        let out = run_git(
            resolve,
            self.local_timeout,
            "git resolve settled cleanup tombstone",
        )
        .await?;
        if !out.status.success() {
            return Ok(());
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if actual != expected_sha {
            return Err(format!(
                "settled cleanup tombstone mismatch: expected {expected_sha}, found {actual}"
            ));
        }
        let mut delete = self.git_cmd(repo_dir);
        delete.args(["update-ref", "-d", tombstone_ref, expected_sha]);
        let out = run_git(
            delete,
            self.local_timeout,
            "git retire settled cleanup tombstone",
        )
        .await?;
        if out.status.success() {
            Ok(())
        } else {
            Err("settled cleanup tombstone CAS deletion failed".into())
        }
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
    async fn run_git_timeout_kills_reaps_and_returns() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("child.pid");
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo $$ > \"$PID_FILE\"; exec sleep 3600"])
            .env("PID_FILE", &pid_file);

        let start = std::time::Instant::now();
        let result = run_git(cmd, Duration::from_millis(300), "sleep").await;
        let elapsed = start.elapsed();

        assert!(result.unwrap_err().contains("timed out"));
        assert!(
            elapsed < Duration::from_secs(5),
            "run_git should return on timeout, took {elapsed:?}"
        );
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let alive = StdCommand::new("kill")
            .args(["-0", pid.trim()])
            .status()
            .unwrap()
            .success();
        assert!(!alive, "timed-out git subprocess {pid:?} was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdin_git_boundary_timeout_kills_and_reaps() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("stdin-child.pid");
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo $$ > \"$PID_FILE\"; exec sleep 3600"])
            .env("PID_FILE", &pid_file);
        let error = run_git_with_input(
            cmd,
            b"transaction\n".to_vec(),
            Duration::from_millis(300),
            "stdin fake git",
        )
        .await
        .unwrap_err();
        assert!(error.contains("timed out"));
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let alive = StdCommand::new("kill")
            .args(["-0", pid.trim()])
            .status()
            .unwrap()
            .success();
        assert!(!alive, "stdin-boundary subprocess was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdin_git_boundary_cancellation_retains_process_ownership() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("cancel-child.pid");
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo $$ > \"$PID_FILE\"; exec sleep 3600"])
            .env("PID_FILE", &pid_file);
        let owner = tokio::spawn(run_git_with_input(
            cmd,
            Vec::new(),
            Duration::from_millis(300),
            "cancel fake git",
        ));
        while !pid_file.exists() {
            tokio::task::yield_now().await;
        }
        owner.abort();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let alive = StdCommand::new("kill")
            .args(["-0", pid.trim()])
            .status()
            .unwrap()
            .success();
        assert!(
            !alive,
            "cancelled caller orphaned stdin-boundary subprocess"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdin_git_boundary_continuous_output_is_bounded_and_reaped() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("noisy-stdin-child.pid");
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "echo $$ > \"$PID_FILE\"; while :; do printf 0123456789; printf abcdefghij >&2; done",
        ])
        .env("PID_FILE", &pid_file);
        let error = run_git_with_input(
            cmd,
            b"transaction\n".to_vec(),
            Duration::from_secs(5),
            "noisy stdin fake git",
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("exceeded") || error.contains("overflow"),
            "unexpected error: {error}"
        );
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let alive = StdCommand::new("kill")
            .args(["-0", pid.trim()])
            .status()
            .unwrap()
            .success();
        assert!(
            !alive,
            "overflowing stdin-boundary subprocess was not reaped"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_git_bounds_continuous_stdout_and_stderr_and_reaps() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("child.pid");
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "echo $$ > \"$PID_FILE\"; while :; do printf 0123456789; printf abcdefghij >&2; done",
        ])
        .env("PID_FILE", &pid_file);

        let start = std::time::Instant::now();
        let error = run_git_with_limit(cmd, Duration::from_secs(5), 4096, "noisy git")
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert!(
            error.contains("exceeded 4096-byte limit"),
            "unexpected output-limit error: {error}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "output limit should stop the child promptly, took {elapsed:?}"
        );
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let alive = StdCommand::new("kill")
            .args(["-0", pid.trim()])
            .status()
            .unwrap()
            .success();
        assert!(
            !alive,
            "overproducing git subprocess {pid:?} was not reaped"
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

    #[cfg(unix)]
    fn install_repository_pre_push_hook(repo: &Path, base: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let hooks = base.join("hooks");
        std::fs::create_dir(&hooks).unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.githooks/pre-push");
        let installed = hooks.join("pre-push");
        std::fs::copy(&source, &installed)
            .unwrap_or_else(|error| panic!("copy {}: {error}", source.display()));
        std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(git_output(
            repo,
            &["config", "core.hooksPath", &hooks.to_string_lossy()]
        )
        .status
        .success());
    }

    #[cfg(unix)]
    fn add_continuation_fixture_files(repo: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let preflight = repo.join("preflight.sh");
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../preflight.sh");
        std::fs::copy(&source, &preflight)
            .unwrap_or_else(|error| panic!("copy {}: {error}", source.display()));
        std::fs::set_permissions(&preflight, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            repo.join("Cargo.toml"),
            "[package]\nname = \"continuation-hook-fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::create_dir(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/lib.rs"), "pub fn formatted() {}\n").unwrap();
        std::fs::write(repo.join("shared.txt"), "base\n").unwrap();
        assert!(git_output(repo, &["add", "."]).status.success());
        assert!(git_output(repo, &["commit", "-m", "continuation fixture"])
            .status
            .success());
        assert!(git_output(repo, &["push", "origin", "main"])
            .status
            .success());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuation_clean_base_merge_publishes_fast_forward_through_real_hook() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        add_continuation_fixture_files(&repo);
        let pr_head = "fix/clean-continuation";

        assert!(git_output(&repo, &["checkout", "-b", pr_head])
            .status
            .success());
        std::fs::write(repo.join("feature.txt"), "feature\n").unwrap();
        assert!(git_output(&repo, &["add", "feature.txt"]).status.success());
        assert!(git_output(&repo, &["commit", "-m", "PR work"])
            .status
            .success());
        assert!(git_output(&repo, &["push", "origin", pr_head])
            .status
            .success());
        let remote_pr_head = git_rev_parse(&repo, "HEAD");

        assert!(git_output(&repo, &["checkout", "main"]).status.success());
        std::fs::write(repo.join("base-only-a.txt"), "advanced base A\n").unwrap();
        assert!(git_output(&repo, &["add", "base-only-a.txt"])
            .status
            .success());
        assert!(git_output(
            &repo,
            &[
                "commit",
                "-m",
                "advance base cleanly A",
                "-m",
                "Co-Authored-By: Base-A <base-a@example.invalid>",
            ]
        )
        .status
        .success());
        std::fs::write(repo.join("base-only-b.txt"), "advanced base B\n").unwrap();
        assert!(git_output(&repo, &["add", "base-only-b.txt"])
            .status
            .success());
        assert!(git_output(
            &repo,
            &[
                "commit",
                "-m",
                "advance base cleanly B",
                "-m",
                "Co-Authored-By: Base-B <base-b@example.invalid>",
            ]
        )
        .status
        .success());
        assert!(git_output(&repo, &["push", "origin", "main"])
            .status
            .success());
        let base_head = git_rev_parse(&repo, "HEAD");

        let mgr = WorktreeManager::new();
        let worktree = tmp.path().join("clean-continuation-wt");
        mgr.fetch_and_provision(&repo, "remediation/Clean-t353", &worktree, pr_head)
            .await
            .expect("provision exact PR head");
        mgr.verify_head_sha(&worktree, &remote_pr_head)
            .await
            .expect("verify exact PR head before integration");
        assert_eq!(
            mgr.integrate_continuation_base(&worktree, "main")
                .await
                .expect("clean base integration"),
            ContinuationBaseMerge::Clean
        );
        std::fs::write(worktree.join("worker.txt"), "worker continuation\n").unwrap();
        assert!(git_output(&worktree, &["add", "worker.txt"])
            .status
            .success());
        assert!(git_output(
            &worktree,
            &[
                "commit",
                "-m",
                "finish clean continuation",
                "-m",
                "Co-Authored-By: Continue-Worker <continue-worker@example.invalid>",
            ]
        )
        .status
        .success());
        let integrated_head = git_rev_parse(&worktree, "HEAD");
        for ancestor in [&remote_pr_head, &base_head] {
            assert!(
                git_output(
                    &worktree,
                    &["merge-base", "--is-ancestor", ancestor, &integrated_head]
                )
                .status
                .success(),
                "integrated continuation must contain {ancestor}"
            );
        }

        install_repository_pre_push_hook(&repo, tmp.path());
        mgr.disable_push(&worktree)
            .await
            .expect("worker push lockout");
        mgr.push_to_pr_head(&worktree, pr_head, &remote_pr_head, &integrated_head)
            .await
            .expect("daemon publication must pass the real ff-only pre-push hook");
        assert_eq!(git_rev_parse(&bare, pr_head), integrated_head);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuation_conflict_remains_mergeable_and_publishes_through_real_hook() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, bare) = init_repo_with_bare_remote(tmp.path());
        add_continuation_fixture_files(&repo);
        let pr_head = "fix/conflicting-continuation";

        assert!(git_output(&repo, &["checkout", "-b", pr_head])
            .status
            .success());
        std::fs::write(repo.join("shared.txt"), "PR version\n").unwrap();
        assert!(git_output(&repo, &["add", "shared.txt"]).status.success());
        assert!(git_output(&repo, &["commit", "-m", "PR conflicting work"])
            .status
            .success());
        assert!(git_output(&repo, &["push", "origin", pr_head])
            .status
            .success());
        let remote_pr_head = git_rev_parse(&repo, "HEAD");

        assert!(git_output(&repo, &["checkout", "main"]).status.success());
        std::fs::write(repo.join("base-only.txt"), "base session A\n").unwrap();
        assert!(git_output(&repo, &["add", "base-only.txt"])
            .status
            .success());
        assert!(git_output(
            &repo,
            &[
                "commit",
                "-m",
                "advance base session A",
                "-m",
                "Co-Authored-By: Base-A <base-a@example.invalid>",
            ]
        )
        .status
        .success());
        std::fs::write(repo.join("shared.txt"), "base version\n").unwrap();
        assert!(git_output(&repo, &["add", "shared.txt"]).status.success());
        assert!(git_output(
            &repo,
            &[
                "commit",
                "-m",
                "advance base conflict",
                "-m",
                "Co-Authored-By: Base-B <base-b@example.invalid>",
            ]
        )
        .status
        .success());
        assert!(git_output(&repo, &["push", "origin", "main"])
            .status
            .success());
        let base_head = git_rev_parse(&repo, "HEAD");

        let mgr = WorktreeManager::new();
        let worktree = tmp.path().join("conflicting-continuation-wt");
        mgr.fetch_and_provision(&repo, "remediation/Conflict-t353", &worktree, pr_head)
            .await
            .expect("provision exact PR head");
        mgr.verify_head_sha(&worktree, &remote_pr_head)
            .await
            .expect("verify exact PR head before integration");
        assert_eq!(
            mgr.integrate_continuation_base(&worktree, "main")
                .await
                .expect("conflict is a prepared continuation state"),
            ContinuationBaseMerge::Conflicted
        );
        assert_eq!(git_rev_parse(&worktree, "HEAD"), remote_pr_head);
        assert_eq!(git_rev_parse(&worktree, "MERGE_HEAD"), base_head);
        assert!(
            !git_output(&worktree, &["diff", "--quiet", "--diff-filter=U"])
                .status
                .success(),
            "prepared worktree must retain unresolved conflict state"
        );

        std::fs::write(worktree.join("shared.txt"), "resolved continuation\n").unwrap();
        assert!(git_output(&worktree, &["add", "shared.txt"])
            .status
            .success());
        assert!(git_output(
            &worktree,
            &[
                "commit",
                "-m",
                "Merge origin/main into continuation",
                "-m",
                "Co-Authored-By: Continue-Worker <continue-worker@example.invalid>",
            ]
        )
        .status
        .success());
        let resolved_head = git_rev_parse(&worktree, "HEAD");
        for ancestor in [&remote_pr_head, &base_head] {
            assert!(
                git_output(
                    &worktree,
                    &["merge-base", "--is-ancestor", ancestor, &resolved_head]
                )
                .status
                .success(),
                "resolved merge must contain {ancestor}"
            );
        }

        install_repository_pre_push_hook(&repo, tmp.path());
        mgr.disable_push(&worktree)
            .await
            .expect("worker push lockout");
        mgr.push_to_pr_head(&worktree, pr_head, &remote_pr_head, &resolved_head)
            .await
            .expect("resolved conflict must publish without parking");
        assert_eq!(git_rev_parse(&bare, pr_head), resolved_head);
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
    async fn publication_ref_reconciliation_restores_intents_and_retires_orphans() {
        let tmp = tempfile::tempdir().unwrap();
        let (repo, _) = init_repo_with_bare_remote(tmp.path());
        let mgr = WorktreeManager::new();
        let expected_sha = git_rev_parse(&repo, "HEAD");
        assert!(
            git_output(&repo, &["commit", "--allow-empty", "-m", "other source"])
                .status
                .success()
        );
        let other_sha = git_rev_parse(&repo, "HEAD");

        mgr.pin_publication_source(&repo, 1, &expected_sha)
            .await
            .unwrap();
        mgr.pin_publication_source(&repo, 2, &other_sha)
            .await
            .unwrap();
        mgr.pin_publication_source(&repo, 3, &other_sha)
            .await
            .unwrap();

        let expected = HashMap::from([
            (1, Some(expected_sha.clone())),
            (2, Some(expected_sha.clone())),
            (3, None),
            (4, Some(expected_sha.clone())),
        ]);
        let outcome = mgr
            .reconcile_publication_sources(&repo, &expected)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            PublicationRefReconcileResult {
                kept: 1,
                restored: 2,
                retired: 1,
            }
        );
        assert_eq!(
            git_rev_parse(&repo, &WorktreeManager::publication_ref(1)),
            expected_sha
        );
        assert_eq!(
            git_rev_parse(&repo, &WorktreeManager::publication_ref(2)),
            expected_sha
        );
        assert_eq!(
            git_rev_parse(&repo, &WorktreeManager::publication_ref(4)),
            expected_sha
        );
        assert!(
            !git_output(
                &repo,
                &["show-ref", "--verify", &WorktreeManager::publication_ref(3),],
            )
            .status
            .success(),
            "a post-lifecycle or cancelled-task orphan must be retired"
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

    #[tokio::test]
    async fn remove_exact_rejects_branch_mismatch_then_replays_absent() {
        let repo = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let base = tempfile::tempdir().unwrap();
        let path = base.path().join("owned");
        let mgr = WorktreeManager::new();
        mgr.provision(repo.path(), "daemon/owned", &path, "main")
            .await
            .unwrap();
        let error = mgr
            .remove_exact(repo.path(), &path, "daemon/other")
            .await
            .unwrap_err();
        assert!(error.contains("identity mismatch"));
        assert!(path.exists(), "mismatched binding must survive");
        mgr.remove_exact(repo.path(), &path, "daemon/owned")
            .await
            .unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(mgr
            .remove_exact(repo.path(), &path, "daemon/owned")
            .await
            .unwrap_err()
            .contains("without exact git registration"));
        std::fs::remove_dir(&path).unwrap();
        mgr.remove_exact(repo.path(), &path, "daemon/owned")
            .await
            .unwrap();
        assert!(!path.exists());
        mgr.remove_exact(repo.path(), &path, "daemon/owned")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_branch_exact_requires_unchanged_sha_and_missing_replays() {
        let repo = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let d = repo.path().to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["-C", &d, "branch", "daemon/owned"])
            .status()
            .unwrap();
        let expected = git_rev_parse(repo.path(), "daemon/owned");
        let mgr = WorktreeManager::new();
        let error = mgr
            .delete_branch_exact(
                repo.path(),
                "daemon/owned",
                "0000000000000000000000000000000000000000",
            )
            .await
            .unwrap_err();
        assert!(error.contains("SHA mismatch"));
        assert_eq!(git_rev_parse(repo.path(), "daemon/owned"), expected);
        mgr.delete_branch_exact(repo.path(), "daemon/owned", &expected)
            .await
            .unwrap();
        mgr.delete_branch_exact(repo.path(), "daemon/owned", &expected)
            .await
            .unwrap();
        assert_eq!(
            git_rev_parse(repo.path(), "main"),
            expected,
            "base must survive feature cleanup"
        );
    }

    #[tokio::test]
    async fn discovery_and_tombstone_delete_are_cas_safe_and_replay_safe() {
        let repo = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let d = repo.path().to_string_lossy().to_string();
        let provenance = git_rev_parse(repo.path(), "main");
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "daemon/discovery"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "worker commit"])
            .status()
            .unwrap();
        let worker_sha = git_rev_parse(repo.path(), "daemon/discovery");
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();
        let mgr = WorktreeManager::new();
        assert_eq!(
            mgr.discover_branch_head(repo.path(), "daemon/discovery", &provenance)
                .await
                .unwrap(),
            Some(worker_sha.clone())
        );
        let tombstone = "refs/quorum/cleanup/1/2/3";
        mgr.delete_branch_with_tombstone(
            repo.path(),
            &d,
            "daemon/discovery",
            &worker_sha,
            tombstone,
        )
        .await
        .unwrap();
        // Simulate crash before DB settlement followed by same-name recreation.
        StdCommand::new("git")
            .args(["-C", &d, "branch", "daemon/discovery", "main"])
            .status()
            .unwrap();
        mgr.delete_branch_with_tombstone(
            repo.path(),
            &d,
            "daemon/discovery",
            &worker_sha,
            tombstone,
        )
        .await
        .unwrap();
        assert_eq!(
            git_rev_parse(repo.path(), "daemon/discovery"),
            provenance,
            "tombstone replay must not touch recreated branch"
        );
        // Simulate restart after DB completion but before best-effort retire.
        mgr.retire_cleanup_tombstone(repo.path(), &d, tombstone, &worker_sha)
            .await
            .unwrap();
        assert_eq!(git_rev_parse(repo.path(), "daemon/discovery"), provenance);
        let moved = mgr
            .delete_branch_with_tombstone(
                repo.path(),
                &d,
                "daemon/discovery",
                &worker_sha,
                "refs/quorum/cleanup/1/2/4",
            )
            .await
            .unwrap_err();
        assert!(moved.contains("SHA mismatch"));
    }

    #[tokio::test]
    async fn remote_tombstone_delete_is_cas_safe_and_replay_safe() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let remote = dir.path().join("remote.git");
        std::fs::create_dir(&repo).unwrap();
        init_git_repo(&repo);
        let d = repo.to_string_lossy().to_string();
        let remote_url = remote.to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["init", "--bare", &remote_url])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "daemon/remote"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "remote worker"])
            .status()
            .unwrap();
        let expected = git_rev_parse(&repo, "daemon/remote");
        StdCommand::new("git")
            .args([
                "-C",
                &d,
                "push",
                &remote_url,
                "daemon/remote:refs/heads/daemon/remote",
            ])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        let manager = WorktreeManager::new();
        let tombstone = "refs/quorum/cleanup/4/5/6";
        manager
            .delete_branch_with_tombstone(&repo, &remote_url, "daemon/remote", &expected, tombstone)
            .await
            .unwrap();
        assert!(!git_output(
            &remote,
            &["show-ref", "--verify", "refs/heads/daemon/remote"]
        )
        .status
        .success());
        assert_eq!(
            String::from_utf8_lossy(
                &git_output(&remote, &["rev-parse", "refs/heads/quorum-cleanup/4/5/6"]).stdout
            )
            .trim(),
            expected
        );

        // A crash before DB settlement may be followed by same-name recreation,
        // even at the same object. The durable remote tombstone makes replay inert.
        StdCommand::new("git")
            .args([
                "-C",
                &d,
                "push",
                &remote_url,
                &format!("{expected}:refs/heads/daemon/remote"),
            ])
            .status()
            .unwrap();
        manager
            .delete_branch_with_tombstone(&repo, &remote_url, "daemon/remote", &expected, tombstone)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(
                &git_output(&remote, &["rev-parse", "refs/heads/daemon/remote"]).stdout
            )
            .trim(),
            expected
        );
        manager
            .retire_cleanup_tombstone(&repo, &remote_url, tombstone, &expected)
            .await
            .unwrap();
        assert!(!git_output(
            &remote,
            &["show-ref", "--verify", "refs/heads/quorum-cleanup/4/5/6"]
        )
        .status
        .success());

        let replacement = git_rev_parse(&repo, "main");
        StdCommand::new("git")
            .args([
                "-C",
                &d,
                "push",
                "--force",
                &remote_url,
                &format!("{replacement}:refs/heads/daemon/remote"),
            ])
            .status()
            .unwrap();
        let error = manager
            .delete_branch_with_tombstone(
                &repo,
                &remote_url,
                "daemon/remote",
                &expected,
                "refs/quorum/cleanup/4/5/7",
            )
            .await
            .unwrap_err();
        assert!(error.contains("remote branch SHA mismatch"));
        assert_eq!(
            String::from_utf8_lossy(
                &git_output(&remote, &["rev-parse", "refs/heads/daemon/remote"]).stdout
            )
            .trim(),
            replacement
        );
    }

    #[tokio::test]
    async fn absent_finalized_branch_gets_tombstone_before_recreation() {
        let repo = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let d = repo.path().to_string_lossy().to_string();
        let expected = git_rev_parse(repo.path(), "main");
        let mgr = WorktreeManager::new();
        let tombstone = "refs/quorum/cleanup/9/8/7";
        mgr.delete_branch_with_tombstone(repo.path(), &d, "daemon/absent", &expected, tombstone)
            .await
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "branch", "daemon/absent", "main"])
            .status()
            .unwrap();
        mgr.delete_branch_with_tombstone(repo.path(), &d, "daemon/absent", &expected, tombstone)
            .await
            .unwrap();
        assert_eq!(git_rev_parse(repo.path(), "daemon/absent"), expected);
    }
}
