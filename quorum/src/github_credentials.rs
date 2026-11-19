//! GitHub credential source for daemon-owned `gh` children.
//!
//! Every daemon-side GitHub operation asks one [`GithubCredentialSource`] for a
//! token at call time and hands it to exactly one child process through
//! `GH_TOKEN`. The token never enters argv, logs, error messages, or agent
//! environments.
//!
//! [`LocalDefaultCredentialSource`] is the local default: it reads a configured
//! token file, or otherwise returns the token `gh` itself would use. Any other
//! implementation of the trait can replace it without touching call sites,
//! because callers only hold an `Arc<dyn GithubCredentialSource>`.

use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Upper bound on one `gh auth token` lookup.
pub const DEFAULT_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest `gh auth token` stdout accepted, in bytes.
const MAX_LOOKUP_OUTPUT_BYTES: u64 = 64 * 1024;

/// Bound on the stderr excerpt quoted in a lookup failure.
const MAX_STDERR_EXCERPT_CHARS: usize = 200;

/// A GitHub token for one operation. `Debug` and `Display` are redacted; the
/// secret is reachable only through [`GithubToken::expose_secret`], which the
/// `apply_to_*` helpers use to set `GH_TOKEN` on a single child.
#[derive(Clone, PartialEq, Eq)]
pub struct GithubToken(String);

impl GithubToken {
    pub fn new(secret: String) -> Self {
        Self(secret)
    }

    /// The raw secret. Only for setting `GH_TOKEN` on one child process.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Set `GH_TOKEN` on one `std::process::Command`.
    pub fn apply_to_command(&self, cmd: &mut std::process::Command) {
        cmd.env("GH_TOKEN", self.expose_secret());
    }

    /// Set `GH_TOKEN` on one `tokio::process::Command`.
    #[allow(dead_code)] // consumed by sibling migrations of async gh call sites
    pub fn apply_to_tokio_command(&self, cmd: &mut tokio::process::Command) {
        cmd.env("GH_TOKEN", self.expose_secret());
    }
}

impl fmt::Debug for GithubToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GithubToken(<redacted>)")
    }
}

impl fmt::Display for GithubToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Why a credential source could not produce a token. Messages describe the
/// source, never a token value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GithubCredentialError {
    /// A configured token file could not be read.
    TokenFileUnreadable { path: PathBuf, reason: String },
    /// A configured token file is empty after trimming.
    TokenFileEmpty { path: PathBuf },
    /// `gh auth token` could not run, failed, or printed no token.
    LookupFailed { reason: String },
    /// `gh auth token` did not finish within its bound and was killed.
    LookupTimedOut { timeout: Duration },
}

impl fmt::Display for GithubCredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TokenFileUnreadable { path, reason } => write!(
                f,
                "GitHub token file {} is unreadable: {reason}",
                path.display()
            ),
            Self::TokenFileEmpty { path } => {
                write!(f, "GitHub token file {} is empty", path.display())
            }
            Self::LookupFailed { reason } => {
                write!(f, "GitHub token lookup (gh auth token) failed: {reason}")
            }
            Self::LookupTimedOut { timeout } => write!(
                f,
                "GitHub token lookup (gh auth token) timed out after {timeout:?}"
            ),
        }
    }
}

impl std::error::Error for GithubCredentialError {}

/// Supplies the GitHub token for the daemon's configured repository, one
/// operation at a time. Implementations must bound any fetch and must never
/// put the token into an error.
pub trait GithubCredentialSource: Send + Sync {
    fn token(&self) -> Result<GithubToken, GithubCredentialError>;
}

/// The local default credential source, replaceable by any other
/// [`GithubCredentialSource`].
///
/// With a configured token file it reads and trims the file on every call; an
/// unreadable or empty file is an error, never a fallback to ambient `gh`
/// auth. Without one it returns the token `gh` would use, via a bounded
/// `gh auth token` (no token in argv, `GIT_TERMINAL_PROMPT=0`).
#[derive(Debug, Clone)]
pub struct LocalDefaultCredentialSource {
    token_file: Option<PathBuf>,
    gh_program: PathBuf,
    /// Arguments placed before `auth token` (test seam; empty in production).
    gh_prefix_args: Vec<PathBuf>,
    timeout: Duration,
}

impl LocalDefaultCredentialSource {
    pub fn new(token_file: Option<PathBuf>) -> Self {
        Self {
            token_file,
            gh_program: PathBuf::from("gh"),
            gh_prefix_args: Vec::new(),
            timeout: DEFAULT_LOOKUP_TIMEOUT,
        }
    }

    /// Test seam: substitute the `gh` binary and lookup bound.
    #[cfg(test)]
    pub fn with_gh_program(mut self, gh_program: PathBuf, timeout: Duration) -> Self {
        self.gh_program = gh_program;
        self.timeout = timeout;
        self
    }

    /// Test seam: run `gh auth token` as `sh <script> auth token`. Reading the
    /// script through `sh` avoids ETXTBSY when tests exec a freshly written file.
    #[cfg(test)]
    pub fn with_gh_script(mut self, script: PathBuf, timeout: Duration) -> Self {
        self.gh_program = PathBuf::from("/bin/sh");
        self.gh_prefix_args = vec![script];
        self.timeout = timeout;
        self
    }

    fn read_token_file(path: &std::path::Path) -> Result<GithubToken, GithubCredentialError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            GithubCredentialError::TokenFileUnreadable {
                path: path.to_path_buf(),
                reason: e.to_string(),
            }
        })?;
        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return Err(GithubCredentialError::TokenFileEmpty {
                path: path.to_path_buf(),
            });
        }
        Ok(GithubToken::new(trimmed.to_string()))
    }

    fn lookup_gh_token(&self) -> Result<GithubToken, GithubCredentialError> {
        use std::io::Read as _;
        use std::process::Stdio;

        let mut child = std::process::Command::new(&self.gh_program)
            .args(&self.gh_prefix_args)
            .args(["auth", "token"])
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| GithubCredentialError::LookupFailed {
                reason: format!("failed to run gh: {e}"),
            })?;

        // The token is tiny, so output fits in the pipe buffer; a child that
        // writes more blocks and is killed at the deadline.
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(GithubCredentialError::LookupTimedOut {
                        timeout: self.timeout,
                    });
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(GithubCredentialError::LookupFailed {
                        reason: format!("failed to wait for gh: {e}"),
                    });
                }
            }
        };

        if !status.success() {
            let mut stderr = String::new();
            if let Some(pipe) = child.stderr.take() {
                let _ = pipe
                    .take(MAX_LOOKUP_OUTPUT_BYTES)
                    .read_to_string(&mut stderr);
            }
            let excerpt: String = stderr
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(MAX_STDERR_EXCERPT_CHARS)
                .collect();
            return Err(GithubCredentialError::LookupFailed {
                reason: if excerpt.is_empty() {
                    format!("gh exited with {status}")
                } else {
                    format!("gh exited with {status}: {excerpt}")
                },
            });
        }

        let mut stdout = String::new();
        if let Some(pipe) = child.stdout.take() {
            pipe.take(MAX_LOOKUP_OUTPUT_BYTES)
                .read_to_string(&mut stdout)
                .map_err(|e| GithubCredentialError::LookupFailed {
                    reason: format!("failed to read gh output: {e}"),
                })?;
        }
        let trimmed = stdout.trim();
        if trimmed.is_empty() {
            return Err(GithubCredentialError::LookupFailed {
                reason: "gh printed no token".into(),
            });
        }
        Ok(GithubToken::new(trimmed.to_string()))
    }
}

impl GithubCredentialSource for LocalDefaultCredentialSource {
    fn token(&self) -> Result<GithubToken, GithubCredentialError> {
        match &self.token_file {
            Some(path) => Self::read_token_file(path),
            None => self.lookup_gh_token(),
        }
    }
}

/// Test-only source: a fixed token or a forced failure, counting calls.
#[cfg(test)]
pub struct FakeCredentialSource {
    outcome: Result<GithubToken, GithubCredentialError>,
    calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FakeCredentialSource {
    pub fn token(secret: &str) -> Self {
        Self {
            outcome: Ok(GithubToken::new(secret.to_string())),
            calls: Default::default(),
        }
    }

    pub fn failing(error: GithubCredentialError) -> Self {
        Self {
            outcome: Err(error),
            calls: Default::default(),
        }
    }

    pub fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl GithubCredentialSource for FakeCredentialSource {
    fn token(&self) -> Result<GithubToken, GithubCredentialError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.outcome.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "ghp_supersecretvalue123";

    fn fake_gh(dir: &std::path::Path, body: &str) -> PathBuf {
        let path = dir.join("gh.sh");
        std::fs::write(&path, format!("{body}\n")).unwrap();
        path
    }

    #[test]
    fn token_file_is_read_and_trimmed_at_call_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  first-token \n").unwrap();
        let source = LocalDefaultCredentialSource::new(Some(path.clone()));
        assert_eq!(source.token().unwrap().expose_secret(), "first-token");

        // Rotation is observed on the next call; nothing is cached.
        std::fs::write(&path, "second-token\n").unwrap();
        assert_eq!(source.token().unwrap().expose_secret(), "second-token");
    }

    #[test]
    fn missing_token_file_is_a_typed_error_not_a_fallback() {
        let dir = tempfile::tempdir().unwrap();
        // A gh that would succeed proves the file error does not fall back.
        let gh = fake_gh(dir.path(), &format!("echo {SECRET}"));
        let source =
            LocalDefaultCredentialSource::new(Some(PathBuf::from("/nonexistent/path/token")))
                .with_gh_script(gh, Duration::from_secs(5));
        assert!(matches!(
            source.token(),
            Err(GithubCredentialError::TokenFileUnreadable { .. })
        ));
    }

    #[test]
    fn empty_token_file_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, " \n\t\n").unwrap();
        let source = LocalDefaultCredentialSource::new(Some(path));
        assert!(matches!(
            source.token(),
            Err(GithubCredentialError::TokenFileEmpty { .. })
        ));
    }

    #[test]
    fn no_token_file_returns_gh_auth_token_output() {
        let dir = tempfile::tempdir().unwrap();
        // Assert the exact argv and that no token is passed on the command line.
        let gh = fake_gh(
            dir.path(),
            &format!(
                "[ \"$#\" = 2 ] && [ \"$1\" = auth ] && [ \"$2\" = token ] \
                 && [ \"$GIT_TERMINAL_PROMPT\" = 0 ] || exit 9\necho '  {SECRET}  '"
            ),
        );
        let source =
            LocalDefaultCredentialSource::new(None).with_gh_script(gh, Duration::from_secs(5));
        assert_eq!(source.token().unwrap().expose_secret(), SECRET);
    }

    #[test]
    fn gh_lookup_failure_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let gh = fake_gh(dir.path(), "echo 'no oauth token found' >&2\nexit 1");
        let source =
            LocalDefaultCredentialSource::new(None).with_gh_script(gh, Duration::from_secs(5));
        match source.token() {
            Err(GithubCredentialError::LookupFailed { reason }) => {
                assert!(reason.contains("no oauth token found"), "{reason}")
            }
            other => panic!("expected LookupFailed, got {other:?}"),
        }
    }

    #[test]
    fn gh_lookup_empty_output_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let gh = fake_gh(dir.path(), "echo");
        let source =
            LocalDefaultCredentialSource::new(None).with_gh_script(gh, Duration::from_secs(5));
        assert!(matches!(
            source.token(),
            Err(GithubCredentialError::LookupFailed { .. })
        ));
    }

    #[test]
    fn missing_gh_binary_is_a_typed_error() {
        let source = LocalDefaultCredentialSource::new(None)
            .with_gh_program(PathBuf::from("/nonexistent/bin/gh"), Duration::from_secs(5));
        assert!(matches!(
            source.token(),
            Err(GithubCredentialError::LookupFailed { .. })
        ));
    }

    #[test]
    fn gh_lookup_is_bounded_and_times_out_with_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let gh = fake_gh(dir.path(), "exec sleep 30");
        let timeout = Duration::from_millis(300);
        let source = LocalDefaultCredentialSource::new(None).with_gh_script(gh, timeout);
        let started = Instant::now();
        assert_eq!(
            source.token(),
            Err(GithubCredentialError::LookupTimedOut { timeout })
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "lookup must be bounded, took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn token_formatting_is_redacted() {
        let token = GithubToken::new(SECRET.into());
        for rendered in [
            format!("{token}"),
            format!("{token:?}"),
            format!("{token:#?}"),
        ] {
            assert!(!rendered.contains(SECRET), "{rendered}");
        }
        assert_eq!(token.expose_secret(), SECRET);
    }

    #[test]
    fn credential_errors_never_render_the_token() {
        let dir = tempfile::tempdir().unwrap();
        // gh prints a token to stdout but fails: the error must not quote it.
        let gh = fake_gh(dir.path(), &format!("echo {SECRET}\nexit 1"));
        let source =
            LocalDefaultCredentialSource::new(None).with_gh_script(gh, Duration::from_secs(5));
        let error = source.token().unwrap_err();
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains(SECRET), "{rendered}");
        }
    }

    #[test]
    fn apply_helpers_set_gh_token_on_one_child() {
        let token = GithubToken::new(SECRET.into());
        let mut cmd = std::process::Command::new("gh");
        token.apply_to_command(&mut cmd);
        let env: Vec<_> = cmd.get_envs().collect();
        assert_eq!(
            env,
            vec![(
                std::ffi::OsStr::new("GH_TOKEN"),
                Some(std::ffi::OsStr::new(SECRET))
            )]
        );
        assert_eq!(cmd.get_args().count(), 0, "token must never enter argv");

        let mut tokio_cmd = tokio::process::Command::new("gh");
        token.apply_to_tokio_command(&mut tokio_cmd);
        let env: Vec<_> = tokio_cmd.as_std().get_envs().collect();
        assert_eq!(
            env,
            vec![(
                std::ffi::OsStr::new("GH_TOKEN"),
                Some(std::ffi::OsStr::new(SECRET))
            )]
        );
    }

    #[test]
    fn fake_source_counts_calls_and_can_fail() {
        let ok = FakeCredentialSource::token(SECRET);
        assert_eq!(ok.token().unwrap().expose_secret(), SECRET);
        assert_eq!(ok.calls(), 1);
        let failing = FakeCredentialSource::failing(GithubCredentialError::LookupTimedOut {
            timeout: Duration::from_secs(1),
        });
        assert!(failing.token().is_err());
        assert_eq!(failing.calls(), 1);
    }
}
