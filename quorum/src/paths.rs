//! Filesystem locations. `QUORUM_HOME` overrides the default `~/.quorum/` (used by tests
//! and power users); otherwise we resolve the real home directory.
//!
//! DB path is per-repo: `~/.quorum/repos/<owner>__<name>/quorum.db`. The repo identity
//! is resolved from `QUORUM_REPO` env var (set by the daemon for workers), then from the
//! cwd's git checkout origin remote. Resolution failure is exit 2 (no silent default).

use quorum_core::error::{QuorumError, Result};
use std::path::PathBuf;

/// The Quorum home directory (`$QUORUM_HOME` or `~/.quorum`).
pub fn home_dir() -> Result<PathBuf> {
    if let Some(h) = std::env::var_os("QUORUM_HOME") {
        return Ok(PathBuf::from(h));
    }
    let base = directories::BaseDirs::new()
        .ok_or_else(|| QuorumError::Io("cannot resolve home directory".into()))?;
    Ok(base.home_dir().join(".quorum"))
}

/// Resolve the repo identity (`owner/name`) for DB path computation.
///
/// 1. `QUORUM_REPO` env var (set by the daemon for spawned workers/reviewers).
/// 2. cwd git detection: parse the `origin` remote URL from the enclosing repo.
/// 3. Neither → exit 2 error.
pub fn resolve_repo() -> Result<String> {
    if let Ok(repo) = std::env::var("QUORUM_REPO") {
        let repo = repo.trim().to_string();
        if !repo.is_empty() {
            return Ok(repo);
        }
    }
    if let Some(repo) = detect_repo_from_git()? {
        return Ok(repo);
    }
    Err(QuorumError::Usage(
        "cannot resolve repo: set QUORUM_REPO=owner/name or run inside a git checkout \
         with an origin remote"
            .into(),
    ))
}

/// Detect the repo identity from the cwd's git checkout by parsing the `origin`
/// remote URL. Works from linked worktrees (git-common-dir shares remotes).
fn detect_repo_from_git() -> Result<Option<String>> {
    let output = std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .stderr(std::process::Stdio::null())
        .output();
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return Ok(None),
    };
    let url = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(parse_repo_from_url(&url))
}

/// Parse `owner/name` from a git remote URL.
/// Supports `git@github.com:owner/name.git` and `https://github.com/owner/name(.git)`.
fn parse_repo_from_url(url: &str) -> Option<String> {
    // SSH form: git@github.com:owner/name.git
    if let Some(rest) = url.strip_prefix("git@") {
        let colon_pos = rest.find(':')?;
        let path = &rest[colon_pos + 1..];
        let path = path.strip_suffix(".git").unwrap_or(path);
        if path.contains('/') && !path.is_empty() {
            return Some(path.to_string());
        }
    }
    // HTTPS form: https://github.com/owner/name(.git)
    if url.starts_with("https://") || url.starts_with("http://") {
        let without_scheme = url.split("//").nth(1)?;
        let slash_pos = without_scheme.find('/')?;
        let path = &without_scheme[slash_pos + 1..];
        let path = path.strip_suffix(".git").unwrap_or(path);
        let path = path.strip_suffix('/').unwrap_or(path);
        if path.matches('/').count() == 1 && !path.is_empty() {
            return Some(path.to_string());
        }
    }
    None
}

/// Compute the DB path for a given repo slug.
pub fn db_path_for_repo(repo: &str) -> Result<PathBuf> {
    let slug = repo.replace('/', "__");
    Ok(home_dir()?.join("repos").join(slug).join("quorum.db"))
}

/// Path to the SQLite database file (resolves repo from env/cwd).
/// Creates the parent directory if it doesn't exist.
pub fn db_path() -> Result<PathBuf> {
    let repo = resolve_repo()?;
    ensure_repo_dir(&repo)
}

/// Path to the optional config file (global, not per-repo).
pub fn config_path() -> Result<PathBuf> {
    Ok(home_dir()?.join("config.toml"))
}

/// Create the home directory if absent; returns its path.
pub fn ensure_home() -> Result<PathBuf> {
    let h = home_dir()?;
    std::fs::create_dir_all(&h).map_err(|e| QuorumError::Io(e.to_string()))?;
    Ok(h)
}

/// Create the per-repo DB directory if absent; returns the DB path.
pub fn ensure_repo_dir(repo: &str) -> Result<PathBuf> {
    let db = db_path_for_repo(repo)?;
    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent).map_err(|e| QuorumError::Io(e.to_string()))?;
    }
    Ok(db)
}

/// Return the git working-tree root for the current directory, or `None` if not
/// inside a git repo. Works from linked worktrees.
pub fn git_toplevel() -> Option<std::path::PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if s.is_empty() {
        return None;
    }
    Some(std::path::PathBuf::from(s))
}

/// Try to resolve repo slug (env var then git), returning `None` instead of
/// erroring when neither is available.
pub fn try_resolve_repo() -> Option<String> {
    resolve_repo().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::sync::{Mutex, MutexGuard};

    // Environment variables are process-global, so tests which mutate Quorum's
    // path inputs must not overlap under libtest's default parallel execution.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn parse_ssh_url() {
        assert_eq!(
            parse_repo_from_url("git@github.com:ag2trust/quorum.git"),
            Some("ag2trust/quorum".into())
        );
    }

    #[test]
    fn parse_ssh_url_no_dot_git() {
        assert_eq!(
            parse_repo_from_url("git@github.com:ag2trust/quorum"),
            Some("ag2trust/quorum".into())
        );
    }

    #[test]
    fn parse_https_url() {
        assert_eq!(
            parse_repo_from_url("https://github.com/ag2trust/quorum.git"),
            Some("ag2trust/quorum".into())
        );
    }

    #[test]
    fn parse_https_url_no_dot_git() {
        assert_eq!(
            parse_repo_from_url("https://github.com/ag2trust/quorum"),
            Some("ag2trust/quorum".into())
        );
    }

    #[test]
    fn parse_https_url_trailing_slash() {
        assert_eq!(
            parse_repo_from_url("https://github.com/ag2trust/quorum/"),
            Some("ag2trust/quorum".into())
        );
    }

    #[test]
    fn parse_invalid_url_returns_none() {
        assert_eq!(parse_repo_from_url("not-a-url"), None);
        assert_eq!(parse_repo_from_url(""), None);
    }

    #[test]
    fn db_path_for_repo_uses_double_underscore() {
        let _lock = lock_env();
        let _home = EnvVarGuard::set("QUORUM_HOME", "/tmp/qtest");
        let p = db_path_for_repo("ag2trust/quorum").unwrap();
        assert_eq!(
            p,
            PathBuf::from("/tmp/qtest/repos/ag2trust__quorum/quorum.db")
        );
    }

    #[test]
    fn resolve_repo_from_env_var() {
        let _lock = lock_env();
        let _repo = EnvVarGuard::set("QUORUM_REPO", "ag2trust/quorum");
        let repo = resolve_repo().unwrap();
        assert_eq!(repo, "ag2trust/quorum");
    }

    #[test]
    fn resolve_repo_empty_env_var_falls_through() {
        let _lock = lock_env();
        let _repo = EnvVarGuard::set("QUORUM_REPO", "");
        // Should fall through to git detection (which may or may not work
        // depending on the test environment — we just verify it doesn't
        // return empty string).
        if let Ok(repo) = resolve_repo() {
            assert!(!repo.is_empty());
        }
    }

    #[test]
    fn env_var_guard_restores_previous_value_after_panic() {
        let _lock = lock_env();
        let key = "QUORUM_HOME";
        let previous = std::env::var_os(key);

        let result = std::panic::catch_unwind(|| {
            let _home = EnvVarGuard::set(key, "/tmp/qtest-panic");
            panic!("exercise unwind restoration");
        });

        assert!(result.is_err());
        assert_eq!(std::env::var_os(key), previous);
    }
}
