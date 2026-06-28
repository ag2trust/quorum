//! Per-(task, project) branch + worktree allocation (issue #98).
//!
//! Centralizes anti-collision branch naming so it lives in ONE place instead of every agent
//! constructing it from a convention. Quorum knows the task, the claiming agent, and the
//! project (`refs.repo`) — so it's the right allocator.
//!
//! The contract on `allocate_for_task` is idempotent on `(task_id, repo)`:
//! - **Fresh task** → derive a new `<type>/<slug>-<agent-lc>` branch + per-project worktree
//!   dir, insert into `task_branches`, return with `existed=false`.
//! - **Reopened (rework) task** → row already exists from the original claim → return the
//!   SAME branch + worktree with `existed=true`, so the agent re-creates its worktree on
//!   the existing PR branch (no reconstruction, no guessing).
//!
//! The UNIQUE indices on the table guarantee one allocation per (task, project) AND that
//! two tasks in the same project can never share a branch — even if their titles slugify
//! identically (the agent-name suffix avoids it for distinct agents; the unique index is
//! the defense for the pathological case of two same-named agents on different tasks).

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// One allocation row. `existed` distinguishes a fresh allocation from a reuse — agents
/// branch on this to decide create-new vs reuse-existing for their worktree.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BranchAllocation {
    pub branch: String,
    pub worktree: String,
    pub existed: bool,
}

/// The default repo when a task's `refs.repo` is missing or unparseable. Matches the
/// ag2trust CLAUDE.md multi-repo recipe: empty/missing `refs.repo` ⇒ behave against
/// `ag2trust/ag2trust` (the "default" project).
pub const DEFAULT_REPO: &str = "ag2trust/ag2trust";

/// Maximum slugified-title length before the agent suffix is appended. Long enough to keep
/// titles human-recognizable, short enough that the full branch fits comfortably below the
/// 250-char Git ref limit even with a long agent name. (50 + slash + type + dash + agent =
/// well under 100 in practice.)
const SLUG_MAX: usize = 50;

/// Look up an existing allocation for `(task_id, repo)`. Read-only — no allocation.
pub fn lookup(conn: &Connection, task_id: i64, repo: &str) -> Result<Option<BranchAllocation>> {
    Ok(conn
        .query_row(
            "SELECT branch, worktree FROM task_branches WHERE task_id=?1 AND repo=?2",
            params![task_id, repo],
            |r| {
                Ok(BranchAllocation {
                    branch: r.get(0)?,
                    worktree: r.get(1)?,
                    existed: true,
                })
            },
        )
        .optional()?)
}

/// Allocate or reuse a branch+worktree for `(task_id, repo)`. Idempotent.
///
/// `branch_hint` overrides the slugified title for the topic portion (still combined with
/// the derived type prefix and agent suffix). Pass `None` to derive from the title.
///
/// `allocator` is the agent recorded as `allocated_by` (audit trail); the branch suffix is
/// also derived from it on a fresh allocation. A reused allocation keeps its original
/// suffix — the branch is identified by the PR, not by which agent is currently working it.
#[allow(clippy::too_many_arguments)]
pub fn allocate_for_task(
    conn: &mut Connection,
    task_id: i64,
    repo: &str,
    allocator: &str,
    title: &str,
    labels: Option<&str>,
    branch_hint: Option<&str>,
    now: i64,
) -> Result<BranchAllocation> {
    // Hot path: existing allocation (rework / re-claim) — one read, no write lock.
    if let Some(existing) = lookup(conn, task_id, repo)? {
        return Ok(existing);
    }
    // Cold path: fresh allocation. Take the write lock and insert with collision retry —
    // the UNIQUE(repo, branch) index guards against the (unlikely) case of two tasks
    // resolving to the same `<type>/<slug>-<agent>` name (e.g., two agents with the same
    // sanitized name, two tasks with the same title). On a hit, append a numeric suffix
    // and retry. The bound is tiny (3 tries) — if we burn through it, the agent will
    // re-call with a different topic/hint.
    let topic = branch_hint
        .filter(|h| !h.trim().is_empty())
        .map(slugify)
        .unwrap_or_else(|| slugify(title));
    let prefix = derive_type(labels);
    let suffix = sanitize_agent(allocator);
    let base_branch = format!("{prefix}/{topic}-{suffix}");
    let tx = crate::db::begin_immediate(conn)?;
    // Recheck inside the txn — another writer may have allocated between our lookup and
    // BEGIN IMMEDIATE. (Common pattern in quorum; same shape as task-claim's recheck.)
    if let Some(existing) = tx
        .query_row(
            "SELECT branch, worktree FROM task_branches WHERE task_id=?1 AND repo=?2",
            params![task_id, repo],
            |r| {
                Ok(BranchAllocation {
                    branch: r.get(0)?,
                    worktree: r.get(1)?,
                    existed: true,
                })
            },
        )
        .optional()?
    {
        tx.commit()?;
        return Ok(existing);
    }
    let mut branch = base_branch.clone();
    let mut attempt = 0u32;
    loop {
        let worktree = worktree_path(repo, &branch);
        let res = tx.execute(
            "INSERT INTO task_branches(task_id, repo, branch, worktree, allocated_by, allocated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![task_id, repo, branch, worktree, allocator, now],
        );
        match res {
            Ok(_) => {
                tx.commit()?;
                return Ok(BranchAllocation {
                    branch,
                    worktree,
                    existed: false,
                });
            }
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE && attempt < 3 =>
            {
                // (repo, branch) collision — extremely rare with agent suffixes. Append
                // a numeric suffix and retry. The (task_id, repo) UNIQUE can't hit here
                // because we just verified above that no row exists.
                attempt += 1;
                branch = format!("{base_branch}-{attempt}");
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Per-project worktree directory convention. Recommendation, not mandate — the agent may
/// override locally, but defaulting here keeps the convention consistent across the fleet.
///
/// - `ag2trust/ag2trust` (and any repo without a specific override) → `.claude/worktrees/<basename>`
///   (relative to the repo root; the harness creates worktrees here).
/// - `ag2trust/quorum` → `~/dev/quorum-wt/<basename>` (sibling clone, per quorum CLAUDE.md §3).
///
/// `basename` is the branch name with the type prefix stripped (e.g.
/// `feat/foo-larkspur` → `foo-larkspur`), so the path stays short and Git-safe.
pub fn worktree_path(repo: &str, branch: &str) -> String {
    let basename = branch.rsplit_once('/').map(|(_, b)| b).unwrap_or(branch);
    match repo {
        "ag2trust/quorum" => format!("~/dev/quorum-wt/{basename}"),
        _ => format!(".claude/worktrees/{basename}"),
    }
}

/// Extract the project (`refs.repo`) from a task's `refs` JSON string. Returns
/// [`DEFAULT_REPO`] when refs is missing, unparseable, or has no `repo` field — same
/// fallback as the ag2trust CLAUDE.md multi-repo recipe.
pub fn repo_from_refs(refs: Option<&str>) -> String {
    let Some(refs) = refs else {
        return DEFAULT_REPO.to_string();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(refs) else {
        return DEFAULT_REPO.to_string();
    };
    v.get("repo")
        .and_then(|r| r.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| DEFAULT_REPO.to_string())
}

/// Optional `branch_hint` field inside a task's `refs` JSON. Lets the task creator
/// override the slugified-title topic without changing the title or labels.
pub fn branch_hint_from_refs(refs: Option<&str>) -> Option<String> {
    let refs = refs?;
    let v = serde_json::from_str::<serde_json::Value>(refs).ok()?;
    v.get("branch_hint")
        .and_then(|h| h.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

/// Derive the conventional commit prefix (`feat`/`fix`/`docs`/`chore`) from a task's
/// `labels` JSON-array string. Looks for `kind:*` labels; falls back to `chore` so a
/// label-less task still gets a sane default rather than a missing prefix.
pub fn derive_type(labels: Option<&str>) -> &'static str {
    let Some(s) = labels else { return "chore" };
    // Cheap substring check, mirroring tasks.rs's SQL `LIKE '%"kind:X"%'` pattern.
    let has = |needle: &str| s.contains(&format!("\"{needle}\""));
    if has("kind:bug") || has("kind:fix") {
        "fix"
    } else if has("kind:docs") {
        "docs"
    } else if has("kind:chore") {
        "chore"
    } else if has("kind:enhancement") || has("kind:feat") || has("kind:feature") {
        "feat"
    } else {
        "chore"
    }
}

/// Slugify a task title into a branch-safe topic segment. Steps:
/// 1. Strip a leading `[bracket]` tag (e.g. `[quorum]` prefix).
/// 2. Strip a trailing `(#NN)` issue ref (common in CTO-routed titles).
/// 3. Lowercase, replace non-alphanumeric runs with `-`, trim leading/trailing dashes.
/// 4. Truncate to [`SLUG_MAX`] chars (preserving a trailing dash trim).
///
/// An empty result falls back to `task` so the branch is always non-empty.
pub fn slugify(title: &str) -> String {
    let mut s = title.trim().to_string();
    // 1. Strip leading [bracket-tag].
    if let Some(rest) = s.strip_prefix('[') {
        if let Some((_, after)) = rest.split_once(']') {
            s = after.trim().to_string();
        }
    }
    // 2. Strip trailing (#NN) issue ref.
    if let Some(open) = s.rfind("(#") {
        if s.ends_with(')') {
            s = s[..open].trim().to_string();
        }
    }
    // 3. Lowercase + dashify.
    let mut out = String::with_capacity(s.len());
    let mut last_dash = true;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    // 4. Trim trailing dash, truncate, trim trailing dash again.
    let trimmed = out.trim_end_matches('-');
    let mut s: String = trimmed.chars().take(SLUG_MAX).collect();
    while s.ends_with('-') {
        s.pop();
    }
    if s.is_empty() {
        "task".to_string()
    } else {
        s
    }
}

/// Sanitize an agent name for use in a branch suffix: lowercase, replace non-alphanumeric
/// with `-`, collapse, trim. Empty result falls back to `agent`.
fn sanitize_agent(agent: &str) -> String {
    let mut out = String::with_capacity(agent.len());
    let mut last_dash = true;
    for ch in agent.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    let s = out.trim_end_matches('-').to_string();
    if s.is_empty() {
        "agent".to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn fresh_db() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        let conn = db::open(&p).unwrap();
        (dir, conn)
    }

    #[test]
    fn slugify_strips_bracket_prefix_and_trailing_issue_ref() {
        let s = slugify("[quorum] task-claim suggests branch/worktree (#98)");
        assert_eq!(s, "task-claim-suggests-branch-worktree");
    }

    #[test]
    fn slugify_lowercases_and_dashifies() {
        assert_eq!(slugify("Fix Foo+Bar Bug!!"), "fix-foo-bar-bug");
    }

    #[test]
    fn slugify_truncates_long_titles() {
        let long = "a".repeat(80);
        let s = slugify(&long);
        assert_eq!(s.len(), SLUG_MAX);
    }

    #[test]
    fn slugify_empty_falls_back_to_task() {
        assert_eq!(slugify(""), "task");
        assert_eq!(slugify("!!!"), "task");
    }

    #[test]
    fn derive_type_picks_from_kind_label() {
        assert_eq!(derive_type(Some("[\"kind:bug\"]")), "fix");
        assert_eq!(derive_type(Some("[\"kind:fix\"]")), "fix");
        assert_eq!(derive_type(Some("[\"kind:docs\"]")), "docs");
        assert_eq!(derive_type(Some("[\"kind:enhancement\"]")), "feat");
        assert_eq!(derive_type(Some("[\"kind:feat\"]")), "feat");
        assert_eq!(derive_type(Some("[\"kind:feature\"]")), "feat");
        assert_eq!(derive_type(Some("[\"tier:opus-47\"]")), "chore");
        assert_eq!(derive_type(None), "chore");
    }

    #[test]
    fn worktree_path_per_project() {
        assert_eq!(
            worktree_path("ag2trust/quorum", "feat/foo-bar"),
            "~/dev/quorum-wt/foo-bar"
        );
        assert_eq!(
            worktree_path("ag2trust/ag2trust", "fix/baz-qux"),
            ".claude/worktrees/baz-qux"
        );
        // Unknown repo → default convention.
        assert_eq!(
            worktree_path("other/thing", "feat/x"),
            ".claude/worktrees/x"
        );
    }

    #[test]
    fn repo_from_refs_extracts_or_defaults() {
        assert_eq!(
            repo_from_refs(Some(r#"{"repo":"ag2trust/quorum"}"#)),
            "ag2trust/quorum"
        );
        assert_eq!(repo_from_refs(Some(r#"{"issue":42}"#)), DEFAULT_REPO);
        assert_eq!(repo_from_refs(Some("not json")), DEFAULT_REPO);
        assert_eq!(repo_from_refs(None), DEFAULT_REPO);
    }

    #[test]
    fn branch_hint_from_refs_optional() {
        assert_eq!(
            branch_hint_from_refs(Some(r#"{"branch_hint":"my-topic"}"#)),
            Some("my-topic".to_string())
        );
        assert_eq!(branch_hint_from_refs(Some(r#"{"issue":1}"#)), None);
        assert_eq!(branch_hint_from_refs(None), None);
    }

    #[test]
    fn allocate_fresh_returns_new_branch() {
        let (_dir, mut conn) = fresh_db();
        let a = allocate_for_task(
            &mut conn,
            42,
            "ag2trust/quorum",
            "Larkspur-q8X",
            "[quorum] something cool",
            Some("[\"kind:enhancement\"]"),
            None,
            100,
        )
        .unwrap();
        assert!(!a.existed);
        assert_eq!(a.branch, "feat/something-cool-larkspur-q8x");
        assert_eq!(a.worktree, "~/dev/quorum-wt/something-cool-larkspur-q8x");
    }

    #[test]
    fn allocate_reopen_returns_same_branch_regardless_of_caller() {
        let (_dir, mut conn) = fresh_db();
        let first = allocate_for_task(
            &mut conn,
            7,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "Fix the thing",
            Some("[\"kind:bug\"]"),
            None,
            100,
        )
        .unwrap();
        assert!(!first.existed);
        // A different agent re-claims (e.g., after release/reopen) — gets the SAME branch.
        let second = allocate_for_task(
            &mut conn,
            7,
            "ag2trust/ag2trust",
            "Brioche-x82f",
            "Fix the thing",
            Some("[\"kind:bug\"]"),
            None,
            200,
        )
        .unwrap();
        assert!(second.existed);
        assert_eq!(first.branch, second.branch);
        assert_eq!(first.worktree, second.worktree);
    }

    #[test]
    fn allocate_same_title_different_repos_both_allocate() {
        let (_dir, mut conn) = fresh_db();
        let a = allocate_for_task(
            &mut conn,
            1,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "Same title",
            None,
            None,
            100,
        )
        .unwrap();
        let b = allocate_for_task(
            &mut conn,
            2,
            "ag2trust/quorum",
            "Larkspur-q8X",
            "Same title",
            None,
            None,
            100,
        )
        .unwrap();
        assert!(!a.existed && !b.existed);
        // Same branch string is fine across DIFFERENT repos — the UNIQUE is (repo, branch).
        assert_eq!(a.branch, b.branch);
        // Worktree convention differs.
        assert_ne!(a.worktree, b.worktree);
    }

    #[test]
    fn allocate_collision_within_repo_gets_numeric_suffix() {
        // Force a collision by allocating two tasks that resolve to the same `<type>/<slug>-<agent>`
        // — same allocator, same title, same labels, same repo.
        let (_dir, mut conn) = fresh_db();
        let a = allocate_for_task(
            &mut conn,
            10,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "Collide me",
            None,
            None,
            100,
        )
        .unwrap();
        let b = allocate_for_task(
            &mut conn,
            11,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "Collide me",
            None,
            None,
            100,
        )
        .unwrap();
        assert_ne!(a.branch, b.branch);
        assert!(
            b.branch.ends_with("-1"),
            "expected numeric suffix, got {}",
            b.branch
        );
    }

    #[test]
    fn allocate_uses_branch_hint_when_present() {
        let (_dir, mut conn) = fresh_db();
        let a = allocate_for_task(
            &mut conn,
            5,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "Some really long noisy title",
            Some("[\"kind:enhancement\"]"),
            Some("clean-slug"),
            100,
        )
        .unwrap();
        assert_eq!(a.branch, "feat/clean-slug-larkspur-q8x");
    }

    #[test]
    fn lookup_returns_existing() {
        let (_dir, mut conn) = fresh_db();
        let _ = allocate_for_task(
            &mut conn,
            33,
            "ag2trust/ag2trust",
            "Larkspur-q8X",
            "title",
            None,
            None,
            100,
        )
        .unwrap();
        let got = lookup(&conn, 33, "ag2trust/ag2trust").unwrap().unwrap();
        assert!(got.existed);
        assert!(got.branch.starts_with("chore/title-"));
        // Different repo → no allocation yet.
        assert!(lookup(&conn, 33, "ag2trust/quorum").unwrap().is_none());
    }
}
