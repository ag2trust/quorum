//! Shared Git process construction.
//!
//! [`git_command`] is the legacy constructor every daemon Git call uses today.
//! [`HardenedGit`], [`LocalGit`], and [`TransportGit`] are the hardened
//! profiles from the sandboxed-workspace specification ("Hardened git
//! invocation"): a cleared environment, `--git-dir` only, a daemon-owned empty
//! working directory, a fixed `-c` list, and `--end-of-options` before data.
//! They are additive; no daemon path uses them yet.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{QuorumError, Result};

/// Construct a Git command with filesystem monitoring disabled.
///
/// A repository-local fsmonitor daemon can be hung or misconfigured. Disabling
/// it for Quorum-owned Git probes keeps daemon and CLI operations bounded.
pub fn git_command(bin: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(bin);
    command.args(["-c", "core.fsmonitor=false"]);
    command
}

/// Lowest git release the hardened constructor supports. `GIT_ATTR_SOURCE`
/// arrived in 2.41 and `merge-tree --write-tree` with an attribute source was
/// unreliable before 2.43.
pub const GIT_FLOOR: GitVersion = GitVersion {
    major: 2,
    minor: 43,
    patch: 0,
};
/// The empty tree in a SHA-1 repository.
pub const EMPTY_TREE_SHA1: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
/// The empty tree in a SHA-256 repository.
pub const EMPTY_TREE_SHA256: &str =
    "6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321";
/// Total `.gitattributes` input [`AttrTree::sanitized`] reads before it falls
/// back to the empty tree.
pub const ATTR_TREE_INPUT_LIMIT: u64 = 1024 * 1024;
/// Number of `.gitattributes` files [`AttrTree::sanitized`] reads before it
/// falls back to the empty tree. Bounds the `hash-object`/`mktree` processes.
pub const ATTR_TREE_FILE_LIMIT: usize = 256;
/// Number of directories (one `mktree` each) [`AttrTree::sanitized`] builds
/// before it falls back to the empty tree.
pub const ATTR_TREE_DIR_LIMIT: usize = 1024;
/// Output bound for the tree listing [`AttrTree::sanitized`] reads.
const ATTR_TREE_LISTING_LIMIT: usize = 16 * 1024 * 1024;
/// One deadline for every process [`AttrTree::sanitized`] runs.
const ATTR_TREE_TIMEOUT: Duration = Duration::from_secs(60);
const VERSION_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_OUTPUT_LIMIT: usize = 64 * 1024;
/// Git ignores `.gitattributes` lines longer than this.
const ATTR_MAX_LINE_LENGTH: usize = 2048;
const ATTR_BLANK: &[u8] = b" \t\r\n";
/// Variables the `inherit` transport profile copies from the daemon, besides
/// every `LC_*`. Never tokens, `QUORUM_*`, or any `GIT_*`.
const INHERIT_ALLOWLIST: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "TMPDIR",
    "LANG",
    "SSH_AUTH_SOCK",
];
/// Subcommands that produce diffs and therefore always get
/// `--no-ext-diff --no-textconv --no-color`.
const DIFF_SUBCOMMANDS: &[&str] = &["diff", "diff-tree", "diff-index", "show", "log"];

/// Object format of the repository a [`LocalGit`] or [`TransportGit`] names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectFormat {
    Sha1,
    Sha256,
}

impl ObjectFormat {
    pub fn empty_tree(self) -> &'static str {
        match self {
            ObjectFormat::Sha1 => EMPTY_TREE_SHA1,
            ObjectFormat::Sha256 => EMPTY_TREE_SHA256,
        }
    }

    fn hex_len(self) -> usize {
        match self {
            ObjectFormat::Sha1 => 40,
            ObjectFormat::Sha256 => 64,
        }
    }
}

/// Workspace mode that decides how the git floor is enforced at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceMode {
    /// Below the floor: a warning, and startup continues (best-effort hardening).
    LinkedWorktree,
    /// Below the floor: startup fails with exit 2.
    Clone,
}

/// Which configuration files the transport profile reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportConfig {
    /// The operator's global and system git configuration (credential
    /// helpers, SSH configuration), guarded against URL rewriting.
    Inherit,
    /// No configuration outside the repository named by `--git-dir`.
    Hermetic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GitVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl GitVersion {
    /// Parse `git --version` output such as `git version 2.43.0` or
    /// `git version 2.39.5 (Apple Git-154)`.
    pub fn parse(output: &str) -> Option<Self> {
        let version = output.trim().strip_prefix("git version ")?;
        let version = version.split_whitespace().next()?;
        let mut parts = version.split('.');
        let number = |part: Option<&str>| -> Option<u32> {
            let digits: String = part?.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        };
        let major = number(parts.next())?;
        let minor = number(parts.next())?;
        let patch = number(parts.next()).unwrap_or(0);
        Some(GitVersion {
            major,
            minor,
            patch,
        })
    }
}

impl std::fmt::Display for GitVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Validate a full object name: 40 or 64 lowercase hex characters.
pub fn validate_sha(sha: &str) -> Result<()> {
    if (sha.len() == 40 || sha.len() == 64)
        && sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(QuorumError::Usage(format!("invalid object name {sha:?}")))
    }
}

/// Validate a full ref name (`refs/...`) with git's `check-ref-format` rules,
/// in Rust, before it reaches git.
pub fn validate_ref_name(name: &str) -> Result<()> {
    let invalid = || QuorumError::Usage(format!("invalid ref name {name:?}"));
    if name.len() > 1024 || !name.starts_with("refs/") {
        return Err(invalid());
    }
    if name.ends_with('/')
        || name.ends_with('.')
        || name.contains("..")
        || name.contains("@{")
        || name.contains("//")
    {
        return Err(invalid());
    }
    if name.chars().any(|c| {
        c.is_ascii_control() || matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    }) {
        return Err(invalid());
    }
    for component in name.split('/') {
        if component.is_empty() || component.starts_with('.') || component.ends_with(".lock") {
            return Err(invalid());
        }
    }
    Ok(())
}

/// Validate `remote_url` and return the protocol scheme it uses.
///
/// Accepts `https://`, `ssh://`, scp-like `user@host:path`, and `file://`.
/// Rejects transport-helper syntax (`<name>::`), a leading `-`, control
/// characters, and embedded credentials.
pub fn remote_url_scheme(url: &str) -> Result<&'static str> {
    let reject = |why: &str| Err(QuorumError::Usage(format!("remote_url {url:?}: {why}")));
    if url.is_empty() || url.starts_with('-') {
        return reject("must be a URL");
    }
    if url.chars().any(char::is_control) {
        return reject("contains control characters");
    }
    if let Some(index) = url.find("::") {
        if url[..index]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
        {
            return reject("transport-helper syntax is not allowed");
        }
    }
    let authority = |rest: &str| rest.split('/').next().unwrap_or("").to_string();
    if let Some(rest) = url.strip_prefix("https://") {
        let authority = authority(rest);
        if authority.is_empty() {
            return reject("has no host");
        }
        if authority.contains('@') {
            return reject("embeds credentials");
        }
        return Ok("https");
    }
    if let Some(rest) = url.strip_prefix("ssh://") {
        let authority = authority(rest);
        if authority.is_empty() {
            return reject("has no host");
        }
        if let Some((userinfo, _)) = authority.rsplit_once('@') {
            if userinfo.contains(':') {
                return reject("embeds credentials");
            }
        }
        return Ok("ssh");
    }
    if let Some(rest) = url.strip_prefix("file://") {
        if !rest.starts_with('/') {
            return reject("file URL must name an absolute path");
        }
        return Ok("file");
    }
    if url.contains("://") {
        return reject("unsupported scheme");
    }
    // scp-like: [user@]host:path, where the host part has no '/'.
    match url.split_once(':') {
        Some((host, path)) if !host.is_empty() && !host.contains('/') && !path.is_empty() => {
            if path.split('/').next().unwrap_or("").contains('@') {
                return reject("embeds credentials");
            }
            Ok("ssh")
        }
        _ => reject("unsupported scheme"),
    }
}

/// Startup context shared by every hardened git command: the git binary, the
/// daemon state directory, and the environment captured from the daemon.
#[derive(Clone, Debug)]
pub struct HardenedGit {
    git_bin: PathBuf,
    state_dir: PathBuf,
    path: Option<OsString>,
    inherit_env: Vec<(OsString, OsString)>,
    version: GitVersion,
    floor_warning: Option<String>,
}

impl HardenedGit {
    /// Startup checks, applied wherever the constructors run: creates or
    /// verifies `<state dir>/empty` and `<state dir>/empty-hooks` (mode 0500,
    /// empty), then checks `git --version` against [`GIT_FLOOR`].
    ///
    /// Below the floor, [`WorkspaceMode::Clone`] fails with exit 2 and
    /// [`WorkspaceMode::LinkedWorktree`] records a warning (see
    /// [`HardenedGit::floor_warning`]) for the caller to log.
    pub fn startup(
        git_bin: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        mode: WorkspaceMode,
    ) -> Result<Self> {
        Self::startup_with_env(git_bin, state_dir, mode, std::env::vars_os())
    }

    /// [`HardenedGit::startup`] with an explicit daemon environment snapshot.
    pub fn startup_with_env(
        git_bin: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        mode: WorkspaceMode,
        parent_env: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Result<Self> {
        let mut git = Self::unchecked(git_bin.into(), state_dir.into(), parent_env);
        prepare_empty_dir(&git.empty_dir())?;
        prepare_empty_dir(&git.hooks_dir())?;
        let output = git
            .base_command(ObjectFormat::Sha1.empty_tree(), "file", false)
            .arg("--version")
            .output_bounded(None, STARTUP_OUTPUT_LIMIT, VERSION_TIMEOUT)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            return Err(QuorumError::Io(format!(
                "git --version failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        git.version = GitVersion::parse(&stdout).ok_or_else(|| {
            QuorumError::Io(format!("unrecognized git --version output {stdout:?}"))
        })?;
        if git.version < GIT_FLOOR {
            let found = git.version;
            match mode {
                WorkspaceMode::Clone => {
                    return Err(QuorumError::Usage(format!(
                        "git {found} is below the required floor {}.{} for clone workspace mode",
                        GIT_FLOOR.major, GIT_FLOOR.minor
                    )));
                }
                WorkspaceMode::LinkedWorktree => {
                    git.floor_warning = Some(format!(
                        "git {found} is below the hardened-git floor {}.{}; attribute isolation \
                         is inactive",
                        GIT_FLOOR.major, GIT_FLOOR.minor
                    ));
                }
            }
        }
        Ok(git)
    }

    fn unchecked(
        git_bin: PathBuf,
        state_dir: PathBuf,
        parent_env: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Self {
        let mut path = None;
        let mut inherit_env = Vec::new();
        for (key, value) in parent_env {
            if key == "PATH" {
                path = Some(value.clone());
            }
            let allowed = key
                .to_str()
                .is_some_and(|key| INHERIT_ALLOWLIST.contains(&key) || key.starts_with("LC_"));
            if allowed {
                inherit_env.push((key, value));
            }
        }
        inherit_env.sort();
        HardenedGit {
            git_bin,
            state_dir,
            path,
            inherit_env,
            version: GIT_FLOOR,
            floor_warning: None,
        }
    }

    pub fn version(&self) -> GitVersion {
        self.version
    }

    /// The below-floor warning a linked-worktree startup must log.
    pub fn floor_warning(&self) -> Option<&str> {
        self.floor_warning.as_deref()
    }

    /// `<state dir>/empty`: the working directory of every hardened command.
    pub fn empty_dir(&self) -> PathBuf {
        self.state_dir.join("empty")
    }

    /// `<state dir>/empty-hooks`: the only `core.hooksPath` git sees.
    pub fn hooks_dir(&self) -> PathBuf {
        self.state_dir.join("empty-hooks")
    }

    /// The local profile for the repository at `git_dir`.
    pub fn local(&self, git_dir: impl Into<PathBuf>, format: ObjectFormat) -> LocalGit {
        LocalGit {
            ctx: self.clone(),
            git_dir: git_dir.into(),
            format,
        }
    }

    /// The transport profile against `remote_url`. Validates the URL and, for
    /// every configuration mode, runs the URL-rewrite check before returning.
    pub fn transport(
        &self,
        git_dir: impl Into<PathBuf>,
        format: ObjectFormat,
        remote_url: &str,
        config: TransportConfig,
    ) -> Result<TransportGit> {
        let scheme = remote_url_scheme(remote_url)?;
        let transport = TransportGit {
            ctx: self.clone(),
            git_dir: git_dir.into(),
            format,
            remote_url: remote_url.to_string(),
            scheme,
            config,
        };
        transport.verify_remote_url()?;
        Ok(transport)
    }

    /// Environment shared by every profile after the inherited variables.
    fn git_env(&self, attr_source: &str, scheme: &str, read_config: bool) -> Vec<(&str, OsString)> {
        let mut env: Vec<(&str, OsString)> = Vec::new();
        if !read_config {
            env.push(("GIT_CONFIG_NOSYSTEM", "1".into()));
            env.push(("GIT_CONFIG_GLOBAL", "/dev/null".into()));
        }
        env.extend([
            ("GIT_ATTR_NOSYSTEM", "1".into()),
            ("GIT_ATTR_SOURCE", attr_source.into()),
            ("GIT_ALLOW_PROTOCOL", scheme.into()),
            ("GIT_CEILING_DIRECTORIES", self.state_dir.clone().into()),
            ("GIT_TERMINAL_PROMPT", "0".into()),
            ("GIT_OPTIONAL_LOCKS", "0".into()),
            ("GIT_NO_REPLACE_OBJECTS", "1".into()),
            ("GIT_EDITOR", "false".into()),
            ("GIT_SEQUENCE_EDITOR", "false".into()),
            ("GIT_PAGER", "cat".into()),
        ]);
        env
    }

    /// A command with the profile environment and no repository or `-c` list.
    /// `inherit` selects the `inherit` transport environment.
    fn base_command(&self, attr_source: &str, scheme: &str, inherit: bool) -> GitCommand {
        let mut env: Vec<(OsString, OsString)> = Vec::new();
        if inherit {
            env.extend(self.inherit_env.iter().cloned());
        } else {
            if let Some(path) = &self.path {
                env.push(("PATH".into(), path.clone()));
            }
            env.push(("HOME".into(), self.empty_dir().into()));
            env.push(("LC_ALL".into(), "C".into()));
            env.push(("TZ".into(), "UTC".into()));
        }
        env.extend(
            self.git_env(attr_source, scheme, inherit)
                .into_iter()
                .map(|(key, value)| (OsString::from(key), value)),
        );
        GitCommand {
            program: self.git_bin.clone(),
            cwd: self.empty_dir(),
            env,
            argv: Vec::new(),
            data_started: false,
        }
    }

    /// `--git-dir`, the `-c` list, the subcommand, and its mandatory options.
    fn repo_command(
        &self,
        git_dir: &Path,
        sub: &'static str,
        attr_source: &str,
        scheme: &str,
        config: Option<TransportConfig>,
    ) -> GitCommand {
        let inherit = config == Some(TransportConfig::Inherit);
        let mut command = self.base_command(attr_source, scheme, inherit);
        let mut git_dir_arg = OsString::from("--git-dir=");
        git_dir_arg.push(git_dir);
        command.argv.push(git_dir_arg);
        for setting in self.config_args(scheme, inherit) {
            command.argv.push("-c".into());
            command.argv.push(setting.into());
        }
        command.argv.push(sub.into());
        if sub == "archive" {
            command.argv.push("--worktree-attributes".into());
        }
        if DIFF_SUBCOMMANDS.contains(&sub) {
            for flag in ["--no-ext-diff", "--no-textconv", "--no-color"] {
                command.argv.push(flag.into());
            }
        }
        command
    }

    /// The spec's command-line configuration, in order.
    fn config_args(&self, scheme: &str, inherit: bool) -> Vec<String> {
        let mut hooks = String::from("core.hooksPath=");
        hooks.push_str(&self.hooks_dir().to_string_lossy());
        let mut args = vec![
            hooks,
            "core.fsmonitor=false".into(),
            "core.untrackedCache=false".into(),
            "core.alternateRefsCommand=".into(),
            "core.alternateRefsPrefixes=".into(),
            "core.protectHFS=true".into(),
            "core.protectNTFS=true".into(),
            "protocol.allow=never".into(),
            format!("protocol.{scheme}.allow=always"),
            "transfer.fsckObjects=true".into(),
            "fetch.fsckObjects=true".into(),
            "receive.fsckObjects=true".into(),
            "submodule.recurse=false".into(),
            "fetch.recurseSubmodules=false".into(),
            "push.recurseSubmodules=no".into(),
            "gc.auto=0".into(),
            "maintenance.auto=false".into(),
            "fetch.writeCommitGraph=false".into(),
            "core.bigFileThreshold=16m".into(),
            "core.deltaBaseCacheLimit=32m".into(),
            "pack.threads=1".into(),
            "commit.gpgSign=false".into(),
            "tag.gpgSign=false".into(),
            "push.gpgSign=false".into(),
            "gpg.program=false".into(),
            "gpg.ssh.program=false".into(),
            "gpg.x509.program=false".into(),
            "diff.external=".into(),
            "core.pager=cat".into(),
        ];
        if !inherit {
            args.push("credential.helper=".into());
        }
        args
    }
}

/// Local profile: every command that reads or writes objects. Allows only the
/// `file` protocol and reads no configuration outside `--git-dir`.
#[derive(Clone, Debug)]
pub struct LocalGit {
    ctx: HardenedGit,
    git_dir: PathBuf,
    format: ObjectFormat,
}

impl LocalGit {
    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    pub fn object_format(&self) -> ObjectFormat {
        self.format
    }

    /// `git <sub>` with `GIT_ATTR_SOURCE` set to the empty tree.
    pub fn command(&self, sub: &'static str) -> GitCommand {
        self.with_attr_source(sub, self.format.empty_tree())
    }

    /// `git merge-tree` reading the sanitized attribute tree of `<ours>`.
    pub fn merge_tree(&self, attrs: &AttrTree) -> GitCommand {
        self.with_attr_source("merge-tree", &attrs.sha)
    }

    /// `git show --remerge-diff` reading the sanitized attribute tree of the
    /// merge's first parent.
    pub fn remerge_diff(&self, attrs: &AttrTree) -> GitCommand {
        self.with_attr_source("show", &attrs.sha)
            .arg("--remerge-diff")
    }

    fn with_attr_source(&self, sub: &'static str, attr_source: &str) -> GitCommand {
        self.ctx
            .repo_command(&self.git_dir, sub, attr_source, "file", None)
    }
}

/// Transport profile: fetch, push, and `ls-remote` against `remote_url`. Allows
/// only the protocol of `remote_url`.
#[derive(Clone, Debug)]
pub struct TransportGit {
    ctx: HardenedGit,
    git_dir: PathBuf,
    format: ObjectFormat,
    remote_url: String,
    scheme: &'static str,
    config: TransportConfig,
}

impl TransportGit {
    pub fn remote_url(&self) -> &str {
        &self.remote_url
    }

    pub fn scheme(&self) -> &'static str {
        self.scheme
    }

    /// `git <sub>` under the transport profile.
    pub fn command(&self, sub: &'static str) -> GitCommand {
        self.ctx.repo_command(
            &self.git_dir,
            sub,
            self.format.empty_tree(),
            self.scheme,
            Some(self.config),
        )
    }

    /// Refuse a rewritten remote URL (exit 2). Run before the first transport
    /// command and before every push:
    ///
    /// - `ls-remote --get-url` must print `remote_url` unchanged (`insteadOf`);
    /// - no `pushInsteadOf` prefix may match `remote_url`;
    /// - no `remote.<remote_url>.*` section may exist: git treats a URL that
    ///   names a configured remote as that remote, so its `pushurl`,
    ///   `receivepack`, or `uploadpack` would apply.
    pub fn verify_remote_url(&self) -> Result<()> {
        let output = self
            .command("ls-remote")
            .arg("--get-url")
            .data(&self.remote_url)
            .output_bounded(None, STARTUP_OUTPUT_LIMIT, STARTUP_TIMEOUT)?;
        if !output.status.success() {
            return Err(QuorumError::Io(format!(
                "ls-remote --get-url failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let resolved = String::from_utf8_lossy(&output.stdout);
        let resolved = resolved.trim_end_matches('\n');
        if resolved != self.remote_url {
            return Err(QuorumError::Usage(format!(
                "remote_url {:?} is rewritten to {resolved:?} by url.*.insteadOf configuration",
                self.remote_url
            )));
        }
        let output = self
            .command("config")
            .args([
                "-z",
                "--get-regexp",
                r"^(url\..*\.(push)?insteadof|remote\..*\..*)$",
            ])
            .output_bounded(None, STARTUP_OUTPUT_LIMIT, STARTUP_TIMEOUT)?;
        match output.status.code() {
            Some(0) => {}
            Some(1) => return Ok(()),
            _ => {
                return Err(QuorumError::Io(format!(
                    "git config --get-regexp failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )))
            }
        }
        for entry in output.stdout.split(|b| *b == 0).filter(|e| !e.is_empty()) {
            let entry = String::from_utf8_lossy(entry);
            let Some((key, prefix)) = entry.split_once('\n') else {
                continue;
            };
            if key.starts_with(&format!("remote.{}.", self.remote_url)) {
                return Err(QuorumError::Usage(format!(
                    "remote_url {:?} names a configured remote ({key})",
                    self.remote_url
                )));
            }
            if key.ends_with(".pushinsteadof") && self.remote_url.starts_with(prefix) {
                return Err(QuorumError::Usage(format!(
                    "remote_url {:?} is rewritten for push by {key} = {prefix:?}",
                    self.remote_url
                )));
            }
        }
        Ok(())
    }
}

/// One hardened git invocation. Daemon-owned options go through
/// [`GitCommand::arg`]; data (SHAs, ref names, URLs, refspecs) goes through
/// [`GitCommand::data`], [`GitCommand::sha`], or [`GitCommand::ref_name`],
/// which place it after `--end-of-options`.
#[derive(Debug)]
pub struct GitCommand {
    program: PathBuf,
    cwd: PathBuf,
    env: Vec<(OsString, OsString)>,
    argv: Vec<OsString>,
    data_started: bool,
}

impl GitCommand {
    /// Append a daemon-owned option.
    ///
    /// # Panics
    /// If data has already been appended: an option after `--end-of-options`
    /// would be read as data.
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        assert!(
            !self.data_started,
            "git option appended after --end-of-options"
        );
        self.argv.push(arg.as_ref().to_owned());
        self
    }

    pub fn args<I, S>(self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        args.into_iter().fold(self, GitCommand::arg)
    }

    /// Append untrusted data after `--end-of-options`.
    pub fn data(mut self, data: impl AsRef<OsStr>) -> Self {
        if !self.data_started {
            self.argv.push("--end-of-options".into());
            self.data_started = true;
        }
        self.argv.push(data.as_ref().to_owned());
        self
    }

    /// Append a validated full object name as data.
    pub fn sha(self, sha: &str) -> Result<Self> {
        validate_sha(sha)?;
        Ok(self.data(sha))
    }

    /// Append a validated full ref name as data.
    pub fn ref_name(self, name: &str) -> Result<Self> {
        validate_ref_name(name)?;
        Ok(self.data(name))
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn current_dir(&self) -> &Path {
        &self.cwd
    }

    /// Exact argument vector after the program name.
    pub fn argv(&self) -> &[OsString] {
        &self.argv
    }

    /// The complete child environment; nothing else is inherited.
    pub fn env(&self) -> &[(OsString, OsString)] {
        &self.env
    }

    /// Value of one variable in the child environment.
    pub fn env_value(&self, key: &str) -> Option<&OsStr> {
        self.env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_os_str())
    }

    /// A process with a cleared environment, the profile environment, and the
    /// empty working directory. Callers that need async execution convert it
    /// with `tokio::process::Command::from`.
    pub fn into_command(self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .env_clear()
            .envs(self.env)
            .current_dir(self.cwd)
            .args(self.argv);
        command
    }

    /// Run to completion with `stdin`, each output pipe bounded to `limit`
    /// bytes and the whole run to `timeout`. Overflow or timeout kills and
    /// reaps the child and returns an error. A non-zero exit is returned in
    /// the output for the caller to classify.
    pub fn output_bounded(
        self,
        stdin: Option<Vec<u8>>,
        limit: usize,
        timeout: Duration,
    ) -> Result<Output> {
        let label = self
            .argv
            .iter()
            .find(|arg| {
                let arg = arg.as_bytes();
                !arg.starts_with(b"-") && !arg.contains(&b'=')
            })
            .map(|arg| arg.to_string_lossy().into_owned())
            .unwrap_or_else(|| "git".into());
        let mut command = self.into_command();
        command
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| QuorumError::Io(format!("spawn git {label}: {error}")))?;
        let overflow = Arc::new(AtomicBool::new(false));
        let writer = match (stdin, child.stdin.take()) {
            (Some(bytes), Some(mut pipe)) => Some(std::thread::spawn(move || {
                // A child that exits early closes the pipe; its status reports why.
                let _ = pipe.write_all(&bytes);
            })),
            _ => None,
        };
        let stdout = spawn_bounded_reader(child.stdout.take(), limit, overflow.clone());
        let stderr = spawn_bounded_reader(child.stderr.take(), limit, overflow.clone());
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| QuorumError::Io(format!("wait git {label}: {error}")))?
            {
                break status;
            }
            let reason = if overflow.load(Ordering::SeqCst) {
                Some(format!("output exceeded {limit}-byte limit"))
            } else if Instant::now() >= deadline {
                Some(format!("timed out after {}s", timeout.as_secs()))
            } else {
                None
            };
            if let Some(reason) = reason {
                let _ = child.kill();
                let _ = child.wait();
                return Err(QuorumError::Io(format!("git {label}: {reason}")));
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        if let Some(writer) = writer {
            let _ = writer.join();
        }
        let stdout = stdout.join().unwrap_or_default();
        let stderr = stderr.join().unwrap_or_default();
        if overflow.load(Ordering::SeqCst) {
            return Err(QuorumError::Io(format!(
                "git {label}: output exceeded {limit}-byte limit"
            )));
        }
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

fn spawn_bounded_reader(
    pipe: Option<impl Read + Send + 'static>,
    limit: usize,
    overflow: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let Some(pipe) = pipe else {
            return buf;
        };
        let mut pipe = pipe.take(limit as u64 + 1);
        if pipe.read_to_end(&mut buf).is_ok() && buf.len() > limit {
            overflow.store(true, Ordering::SeqCst);
            buf.truncate(limit);
        }
        buf
    })
}

/// Create `path` as an empty directory with mode 0500, or verify an existing
/// one is a real, empty directory and reset its mode to 0500.
fn prepare_empty_dir(path: &Path) -> Result<()> {
    let io = |what: &str, error: std::io::Error| {
        QuorumError::Io(format!("{what} {}: {error}", path.display()))
    };
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => std::fs::DirBuilder::new()
            .mode(0o500)
            .create(path)
            .map_err(|error| io("create", error))?,
        Err(error) => return Err(io("inspect", error)),
        Ok(meta) if !meta.file_type().is_dir() => {
            return Err(QuorumError::Io(format!(
                "{} must be a directory, not a file or symlink",
                path.display()
            )));
        }
        Ok(_) => {}
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500))
        .map_err(|error| io("chmod", error))?;
    let mut entries = std::fs::read_dir(path).map_err(|error| io("read", error))?;
    if entries.next().is_some() {
        return Err(QuorumError::Io(format!(
            "{} must be empty; refusing a planted entry",
            path.display()
        )));
    }
    Ok(())
}

/// `mktree` entries of one directory: name to (`<mode> <type>`, object name).
type TreeEntries = BTreeMap<Vec<u8>, (&'static str, String)>;

/// An attribute tree for `GIT_ATTR_SOURCE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttrTree {
    sha: String,
    bound_exceeded: Option<String>,
}

impl AttrTree {
    /// The empty tree: no repository attributes apply.
    pub fn empty(format: ObjectFormat) -> Self {
        AttrTree {
            sha: format.empty_tree().to_string(),
            bound_exceeded: None,
        }
    }

    /// The sanitized attribute tree of `ours` (a commit or tree SHA): every
    /// `.gitattributes` blob keeps only the built-in `merge` attributes, at its
    /// original path. Over [`ATTR_TREE_INPUT_LIMIT`] of input,
    /// [`ATTR_TREE_FILE_LIMIT`] files, or [`ATTR_TREE_DIR_LIMIT`] directories
    /// it is the empty tree, and
    /// [`AttrTree::bound_exceeded`] carries the detail for an event.
    ///
    /// Use it only for `merge-tree` and `show --remerge-diff`.
    pub fn sanitized(git: &LocalGit, ours: &str) -> Result<Self> {
        let deadline = Instant::now() + ATTR_TREE_TIMEOUT;
        let remaining = || {
            deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
                .ok_or_else(|| {
                    QuorumError::Io(format!(
                        "sanitized attribute tree of {ours}: timed out after {}s",
                        ATTR_TREE_TIMEOUT.as_secs()
                    ))
                })
        };
        let over = |detail: String| AttrTree {
            sha: git.format.empty_tree().to_string(),
            bound_exceeded: Some(detail),
        };
        let listing = match git
            .command("ls-tree")
            .args(["-r", "-l", "-z"])
            .sha(ours)?
            .output_bounded(None, ATTR_TREE_LISTING_LIMIT, remaining()?)
        {
            Ok(output) => checked(output, "ls-tree")?,
            Err(error) if error.to_string().contains("byte limit") => {
                return Ok(over(format!("tree listing of {ours}: {error}")));
            }
            Err(error) => return Err(error),
        };

        let mut files: Vec<(Vec<u8>, String)> = Vec::new();
        let mut total: u64 = 0;
        for entry in listing.split(|b| *b == 0).filter(|e| !e.is_empty()) {
            let Some(tab) = entry.iter().position(|b| *b == b'\t') else {
                continue;
            };
            let (meta, path) = (&entry[..tab], &entry[tab + 1..]);
            let meta = String::from_utf8_lossy(meta);
            let fields: Vec<&str> = meta.split_whitespace().collect();
            let [mode, kind, sha, size] = fields[..] else {
                continue;
            };
            let name = path.rsplit(|b| *b == b'/').next().unwrap_or(path);
            if kind != "blob" || name != b".gitattributes" || !matches!(mode, "100644" | "100755") {
                continue;
            }
            total += size.parse::<u64>().unwrap_or(u64::MAX / 2);
            files.push((path.to_vec(), sha.to_string()));
        }
        if total > ATTR_TREE_INPUT_LIMIT {
            return Ok(over(format!(
                ".gitattributes input of {ours} is {total} bytes, over the \
                 {ATTR_TREE_INPUT_LIMIT}-byte bound"
            )));
        }
        if files.len() > ATTR_TREE_FILE_LIMIT {
            return Ok(over(format!(
                "{ours} has {} .gitattributes files, over the {ATTR_TREE_FILE_LIMIT}-file bound",
                files.len()
            )));
        }
        if files.is_empty() {
            return Ok(AttrTree::empty(git.format));
        }
        let mut all_dirs: HashSet<&[u8]> = HashSet::new();
        for (path, _) in &files {
            let mut dir = split_parent(path).0;
            while all_dirs.insert(dir) && !dir.is_empty() {
                dir = split_parent(dir).0;
            }
        }
        if all_dirs.len() > ATTR_TREE_DIR_LIMIT {
            return Ok(over(format!(
                "{ours} has .gitattributes files in {} directories, over the \
                 {ATTR_TREE_DIR_LIMIT}-directory bound",
                all_dirs.len()
            )));
        }

        // Read every blob in one `cat-file --batch`.
        let mut request = Vec::new();
        for (_, sha) in &files {
            request.extend_from_slice(sha.as_bytes());
            request.push(b'\n');
        }
        let batch = checked(
            git.command("cat-file").arg("--batch").output_bounded(
                Some(request),
                ATTR_TREE_INPUT_LIMIT as usize + files.len() * 128,
                remaining()?,
            )?,
            "cat-file --batch",
        )?;
        let mut contents: HashMap<String, Vec<u8>> = HashMap::new();
        let mut rest = batch.as_slice();
        while !rest.is_empty() {
            let newline = rest
                .iter()
                .position(|b| *b == b'\n')
                .ok_or_else(|| QuorumError::Io("cat-file --batch: truncated header".into()))?;
            let header = String::from_utf8_lossy(&rest[..newline]).into_owned();
            let fields: Vec<&str> = header.split(' ').collect();
            let [sha, "blob", size] = fields[..] else {
                return Err(QuorumError::Io(format!(
                    "cat-file --batch: unexpected header {header:?}"
                )));
            };
            let size: usize = size
                .parse()
                .map_err(|_| QuorumError::Io(format!("cat-file --batch: bad size {header:?}")))?;
            let body_start = newline + 1;
            if rest.len() < body_start + size + 1 {
                return Err(QuorumError::Io("cat-file --batch: truncated body".into()));
            }
            contents.insert(
                sha.to_string(),
                rest[body_start..body_start + size].to_vec(),
            );
            rest = &rest[body_start + size + 1..];
        }

        // Write each distinct sanitized blob once and place it at its path.
        let mut blob_shas: HashMap<Vec<u8>, String> = HashMap::new();
        let mut dirs: BTreeMap<Vec<u8>, TreeEntries> = BTreeMap::new();
        dirs.insert(Vec::new(), BTreeMap::new());
        for (path, sha) in &files {
            let content = contents
                .get(sha)
                .ok_or_else(|| QuorumError::Io(format!("cat-file --batch: missing blob {sha}")))?;
            let sanitized = sanitize_gitattributes(content);
            if sanitized.is_empty() {
                continue;
            }
            let blob = match blob_shas.get(&sanitized) {
                Some(blob) => blob.clone(),
                None => {
                    let output = git
                        .command("hash-object")
                        .args(["-w", "--no-filters", "--stdin"])
                        .output_bounded(
                            Some(sanitized.clone()),
                            STARTUP_OUTPUT_LIMIT,
                            remaining()?,
                        )?;
                    let blob = parse_sha(&checked(output, "hash-object")?, git.format)?;
                    blob_shas.insert(sanitized, blob.clone());
                    blob
                }
            };
            let (dir, name) = split_parent(path);
            dirs.entry(dir.to_vec())
                .or_default()
                .insert(name.to_vec(), ("100644 blob", blob));
            let mut ancestor = dir;
            while !ancestor.is_empty() {
                let (parent, _) = split_parent(ancestor);
                dirs.entry(ancestor.to_vec()).or_default();
                dirs.entry(parent.to_vec()).or_default();
                ancestor = parent;
            }
        }

        // Assemble trees bottom-up with `mktree`.
        let mut order: Vec<Vec<u8>> = dirs.keys().cloned().collect();
        order.sort_by_key(|dir| {
            std::cmp::Reverse(
                dir.iter().filter(|b| **b == b'/').count() + usize::from(!dir.is_empty()),
            )
        });
        let mut root = git.format.empty_tree().to_string();
        for dir in order {
            let entries = dirs.remove(&dir).unwrap_or_default();
            let mut input = Vec::new();
            for (name, (kind, sha)) in &entries {
                input.extend_from_slice(kind.as_bytes());
                input.push(b' ');
                input.extend_from_slice(sha.as_bytes());
                input.push(b'\t');
                input.extend_from_slice(name);
                input.push(0);
            }
            let output = git.command("mktree").arg("-z").output_bounded(
                Some(input),
                STARTUP_OUTPUT_LIMIT,
                remaining()?,
            )?;
            let tree = parse_sha(&checked(output, "mktree")?, git.format)?;
            if dir.is_empty() {
                root = tree;
            } else {
                let (parent, name) = split_parent(&dir);
                dirs.entry(parent.to_vec())
                    .or_default()
                    .insert(name.to_vec(), ("040000 tree", tree));
            }
        }
        Ok(AttrTree {
            sha: root,
            bound_exceeded: None,
        })
    }

    pub fn sha(&self) -> &str {
        &self.sha
    }

    /// Set when the input bound was exceeded and the empty tree was used; the
    /// caller records it as an event.
    pub fn bound_exceeded(&self) -> Option<&str> {
        self.bound_exceeded.as_deref()
    }
}

fn checked(output: Output, what: &str) -> Result<Vec<u8>> {
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(QuorumError::Io(format!(
            "git {what} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn parse_sha(stdout: &[u8], format: ObjectFormat) -> Result<String> {
    let sha = String::from_utf8_lossy(stdout).trim().to_string();
    validate_sha(&sha)?;
    if sha.len() != format.hex_len() {
        return Err(QuorumError::Io(format!(
            "object name {sha} does not match the repository object format"
        )));
    }
    Ok(sha)
}

fn split_parent(path: &[u8]) -> (&[u8], &[u8]) {
    match path.iter().rposition(|b| *b == b'/') {
        Some(index) => (&path[..index], &path[index + 1..]),
        None => (&[], path),
    }
}

/// Keep only the built-in merge attributes of one `.gitattributes` file.
///
/// Each kept line is its pattern plus `merge`, `-merge`, `!merge`,
/// `merge=text`, `merge=binary`, or `merge=union`, in order; the built-in
/// `binary` macro becomes `-merge`. Everything else (other attributes,
/// including `conflict-marker-size`, other `merge=` values, macro definitions,
/// negative patterns, comments, over-long lines) is dropped.
fn sanitize_gitattributes(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for line in input.split(|b| *b == b'\n') {
        if line.len() > ATTR_MAX_LINE_LENGTH {
            continue;
        }
        let start = line
            .iter()
            .position(|b| !ATTR_BLANK.contains(b))
            .unwrap_or(line.len());
        let line = &line[start..];
        if line.is_empty() || line[0] == b'#' || line.starts_with(b"[attr]") {
            continue;
        }
        // Mirror attr.c: a pattern that unquotes cleanly is the quoted span;
        // otherwise (including invalid escapes) git reads the raw bytes up to
        // the first blank, quote characters and all.
        let (pattern_end, name) = match unquote_c_style(line) {
            Some((end, unquoted)) => (end, unquoted),
            None => {
                let end = line
                    .iter()
                    .position(|b| ATTR_BLANK.contains(b))
                    .unwrap_or(line.len());
                (end, line[..end].to_vec())
            }
        };
        if name.starts_with(b"!") || name.starts_with(b"[attr]") {
            continue;
        }
        let (pattern, states) = line.split_at(pattern_end);
        let kept: Vec<&[u8]> = states
            .split(|b| ATTR_BLANK.contains(b))
            .filter_map(|state| match state {
                b"merge" | b"-merge" | b"!merge" | b"merge=text" | b"merge=binary"
                | b"merge=union" => Some(state),
                b"binary" => Some(b"-merge".as_slice()),
                _ => None,
            })
            .collect();
        if kept.is_empty() {
            continue;
        }
        out.extend_from_slice(pattern);
        for state in kept {
            out.push(b' ');
            out.extend_from_slice(state);
        }
        out.push(b'\n');
    }
    out
}

/// Git's `unquote_c_style` applied to the start of `line`: the end (exclusive)
/// of the closing quote and the unquoted bytes, or `None` when the line does
/// not start with a quote or the quoted span is unterminated or contains an
/// escape git rejects (anything but `\a\b\f\n\r\t\v\\\"` and `\[0-3][0-7][0-7]`).
fn unquote_c_style(line: &[u8]) -> Option<(usize, Vec<u8>)> {
    if line.first() != Some(&b'"') {
        return None;
    }
    let mut out = Vec::new();
    let mut index = 1;
    loop {
        match *line.get(index)? {
            b'"' => return Some((index + 1, out)),
            b'\\' => {
                let escaped = *line.get(index + 1)?;
                index += 2;
                let byte = match escaped {
                    b'a' => 0x07,
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => 0x0b,
                    b'\\' | b'"' => escaped,
                    b'0'..=b'3' => {
                        let octal = |offset: usize| {
                            line.get(index + offset)
                                .copied()
                                .filter(|digit| (b'0'..=b'7').contains(digit))
                                .map(|digit| digit - b'0')
                        };
                        let (middle, low) = (octal(0)?, octal(1)?);
                        index += 2;
                        ((escaped - b'0') << 6) | (middle << 3) | low
                    }
                    _ => return None,
                };
                out.push(byte);
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn production_sources(dir: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "tests") {
                    continue;
                }
                production_sources(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && path.file_name().is_none_or(|name| name != "tests.rs")
            {
                sources.push(path);
            }
        }
    }

    #[test]
    fn disables_fsmonitor_before_git_subcommands() {
        let command = git_command("git");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["-c", "core.fsmonitor=false"]
        );
    }

    #[test]
    fn production_git_commands_use_the_shared_constructor() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut sources = Vec::new();
        production_sources(&workspace.join("quorum-core/src"), &mut sources);
        production_sources(&workspace.join("quorum/src"), &mut sources);

        for source_path in sources {
            let source = std::fs::read_to_string(&source_path).unwrap();
            // Unit-test modules intentionally construct their own fixture Git
            // commands. Production source precedes the module in this project.
            let production = source.split("#[cfg(test)]\nmod tests {").next().unwrap();
            for bypass in [
                "Command::new(\"git\")",
                "Command::new(git_bin)",
                "Command::new(&git_bin)",
            ] {
                assert!(
                    !production.contains(bypass),
                    "production Git command bypasses git_command in {}: {bypass}",
                    source_path.display()
                );
            }
        }
    }

    fn parent_env() -> Vec<(OsString, OsString)> {
        [
            ("PATH", "/daemon/bin:/usr/bin"),
            ("HOME", "/home/operator"),
            ("USER", "operator"),
            ("LOGNAME", "operator"),
            ("TMPDIR", "/tmp/operator"),
            ("LANG", "en_US.UTF-8"),
            ("LC_CTYPE", "en_US.UTF-8"),
            ("SSH_AUTH_SOCK", "/tmp/agent.sock"),
            ("GH_TOKEN", "gh-secret"),
            ("GITHUB_TOKEN", "github-secret"),
            ("QUORUM_DB", "/state/quorum.db"),
            ("GIT_DIR", "/attacker/.git"),
            ("GIT_CONFIG_PARAMETERS", "'core.hooksPath'='/attacker'"),
            ("GIT_SSH_COMMAND", "/attacker/ssh"),
            ("GIT_ASKPASS", "/attacker/askpass"),
            ("SSH_ASKPASS", "/attacker/askpass"),
            ("XDG_CONFIG_HOME", "/attacker/xdg"),
            ("UNRELATED", "value"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }

    fn ctx() -> HardenedGit {
        HardenedGit::unchecked("/usr/bin/git".into(), "/state".into(), parent_env())
    }

    fn strings(values: &[OsString]) -> Vec<String> {
        values
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect()
    }

    fn env_strings(command: &GitCommand) -> Vec<(String, String)> {
        command
            .env()
            .iter()
            .map(|(k, v)| (k.to_str().unwrap().into(), v.to_str().unwrap().into()))
            .collect()
    }

    fn expected_config(scheme: &str, credential_reset: bool) -> Vec<String> {
        let mut settings = vec![
            "core.hooksPath=/state/empty-hooks".to_string(),
            "core.fsmonitor=false".into(),
            "core.untrackedCache=false".into(),
            "core.alternateRefsCommand=".into(),
            "core.alternateRefsPrefixes=".into(),
            "core.protectHFS=true".into(),
            "core.protectNTFS=true".into(),
            "protocol.allow=never".into(),
            format!("protocol.{scheme}.allow=always"),
            "transfer.fsckObjects=true".into(),
            "fetch.fsckObjects=true".into(),
            "receive.fsckObjects=true".into(),
            "submodule.recurse=false".into(),
            "fetch.recurseSubmodules=false".into(),
            "push.recurseSubmodules=no".into(),
            "gc.auto=0".into(),
            "maintenance.auto=false".into(),
            "fetch.writeCommitGraph=false".into(),
            "core.bigFileThreshold=16m".into(),
            "core.deltaBaseCacheLimit=32m".into(),
            "pack.threads=1".into(),
            "commit.gpgSign=false".into(),
            "tag.gpgSign=false".into(),
            "push.gpgSign=false".into(),
            "gpg.program=false".into(),
            "gpg.ssh.program=false".into(),
            "gpg.x509.program=false".into(),
            "diff.external=".into(),
            "core.pager=cat".into(),
        ];
        if credential_reset {
            settings.push("credential.helper=".into());
        }
        let mut argv = vec!["--git-dir=/state/repository.git".to_string()];
        for setting in settings {
            argv.push("-c".into());
            argv.push(setting);
        }
        argv
    }

    fn git_tail(attr_source: &str, scheme: &str) -> Vec<(String, String)> {
        [
            ("GIT_ATTR_NOSYSTEM", "1"),
            ("GIT_ATTR_SOURCE", attr_source),
            ("GIT_ALLOW_PROTOCOL", scheme),
            ("GIT_CEILING_DIRECTORIES", "/state"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_NO_REPLACE_OBJECTS", "1"),
            ("GIT_EDITOR", "false"),
            ("GIT_SEQUENCE_EDITOR", "false"),
            ("GIT_PAGER", "cat"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn hermetic_env(scheme: &str) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = [
            ("PATH", "/daemon/bin:/usr/bin"),
            ("HOME", "/state/empty"),
            ("LC_ALL", "C"),
            ("TZ", "UTC"),
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        env.extend(git_tail(EMPTY_TREE_SHA1, scheme));
        env
    }

    fn transport(config: TransportConfig) -> TransportGit {
        TransportGit {
            ctx: ctx(),
            git_dir: "/state/repository.git".into(),
            format: ObjectFormat::Sha1,
            remote_url: "https://github.com/owner/name.git".into(),
            scheme: "https",
            config,
        }
    }

    #[test]
    fn local_profile_has_exact_argv_cwd_and_env() {
        let git = ctx().local("/state/repository.git", ObjectFormat::Sha1);
        let command = git
            .command("update-ref")
            .arg("--no-deref")
            .ref_name("refs/quorum/tasks/7/head")
            .unwrap()
            .sha("0123456789abcdef0123456789abcdef01234567")
            .unwrap();
        let mut expected = expected_config("file", true);
        expected.extend(
            [
                "update-ref",
                "--no-deref",
                "--end-of-options",
                "refs/quorum/tasks/7/head",
                "0123456789abcdef0123456789abcdef01234567",
            ]
            .map(String::from),
        );
        assert_eq!(strings(command.argv()), expected);
        assert_eq!(command.program(), Path::new("/usr/bin/git"));
        assert_eq!(command.current_dir(), Path::new("/state/empty"));
        assert_eq!(env_strings(&command), hermetic_env("file"));
        assert!(!strings(command.argv()).iter().any(|a| a == "-C"));
    }

    #[test]
    fn inherit_transport_passes_only_its_allowlist() {
        let command = transport(TransportConfig::Inherit)
            .command("fetch")
            .data("https://github.com/owner/name.git");
        let mut expected = expected_config("https", false);
        expected.extend(
            [
                "fetch",
                "--end-of-options",
                "https://github.com/owner/name.git",
            ]
            .map(String::from),
        );
        assert_eq!(strings(command.argv()), expected);
        let mut env: Vec<(String, String)> = [
            ("HOME", "/home/operator"),
            ("LANG", "en_US.UTF-8"),
            ("LC_CTYPE", "en_US.UTF-8"),
            ("LOGNAME", "operator"),
            ("PATH", "/daemon/bin:/usr/bin"),
            ("SSH_AUTH_SOCK", "/tmp/agent.sock"),
            ("TMPDIR", "/tmp/operator"),
            ("USER", "operator"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        env.extend(git_tail(EMPTY_TREE_SHA1, "https"));
        assert_eq!(env_strings(&command), env);
        for (key, _) in env_strings(&command) {
            assert!(
                !matches!(
                    key.as_str(),
                    "GH_TOKEN" | "GITHUB_TOKEN" | "GIT_SSH_COMMAND" | "GIT_ASKPASS"
                ) && !key.starts_with("QUORUM_"),
                "inherit leaked {key}"
            );
            assert!(
                !key.starts_with("GIT_") || git_tail("", "").iter().any(|(k, _)| *k == key),
                "inherit leaked {key}"
            );
        }
    }

    #[test]
    fn hermetic_transport_clears_everything() {
        let command = transport(TransportConfig::Hermetic).command("push");
        let mut expected = expected_config("https", true);
        expected.push("push".into());
        assert_eq!(strings(command.argv()), expected);
        assert_eq!(env_strings(&command), hermetic_env("https"));
    }

    #[test]
    fn archive_and_diff_commands_get_mandatory_flags() {
        let git = ctx().local("/state/repository.git", ObjectFormat::Sha1);
        let tail = |command: GitCommand| {
            strings(command.argv())
                .into_iter()
                .skip(expected_config("file", true).len())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            tail(git.command("archive").arg("--format=tar")),
            ["archive", "--worktree-attributes", "--format=tar"]
        );
        for sub in ["diff", "diff-tree", "show", "log"] {
            assert_eq!(
                tail(git.command(sub))[1..],
                ["--no-ext-diff", "--no-textconv", "--no-color"]
            );
        }
        let attrs = AttrTree {
            sha: "1111111111111111111111111111111111111111".into(),
            bound_exceeded: None,
        };
        assert_eq!(
            tail(git.remerge_diff(&attrs)),
            [
                "show",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--remerge-diff"
            ]
        );
    }

    #[test]
    fn sha256_repositories_use_the_sha256_empty_tree() {
        let git = ctx().local("/state/repository.git", ObjectFormat::Sha256);
        assert_eq!(
            git.command("diff").env_value("GIT_ATTR_SOURCE").unwrap(),
            EMPTY_TREE_SHA256
        );
    }

    #[test]
    #[should_panic(expected = "after --end-of-options")]
    fn options_after_data_are_refused() {
        let git = ctx().local("/state/repository.git", ObjectFormat::Sha1);
        let _ = git
            .command("fetch")
            .data("file:///x")
            .arg("--upload-pack=x");
    }

    #[test]
    fn sha_validator_accepts_only_full_lowercase_hex() {
        validate_sha(&"a".repeat(40)).unwrap();
        validate_sha(&"0".repeat(64)).unwrap();
        for bad in [
            "",
            "abc",
            &"A".repeat(40),
            &"g".repeat(40),
            &"a".repeat(41),
            "--upload-pack=x",
            "HEAD",
        ] {
            assert_eq!(validate_sha(bad).unwrap_err().exit_code(), 2, "{bad}");
        }
    }

    #[test]
    fn ref_validator_follows_check_ref_format() {
        for good in [
            "refs/heads/main",
            "refs/quorum/tasks/12/head",
            "refs/remotes/origin/feat/x-1",
        ] {
            validate_ref_name(good).unwrap();
        }
        for bad in [
            "main",
            "--upload-pack=x",
            "refs/heads/",
            "refs/heads/a..b",
            "refs/heads/a.lock",
            "refs/heads/.hidden",
            "refs/heads/a b",
            "refs/heads/a~1",
            "refs/heads/a^",
            "refs/heads/a:b",
            "refs/heads/a?",
            "refs/heads/a*",
            "refs/heads/a[",
            "refs/heads/a\\b",
            "refs/heads/a@{1}",
            "refs/heads//a",
            "refs/heads/a.",
            "refs/heads/a\u{7f}",
        ] {
            assert_eq!(validate_ref_name(bad).unwrap_err().exit_code(), 2, "{bad}");
        }
    }

    #[test]
    fn remote_url_validation() {
        assert_eq!(
            remote_url_scheme("https://github.com/o/n.git").unwrap(),
            "https"
        );
        assert_eq!(remote_url_scheme("ssh://git@host/o/n.git").unwrap(), "ssh");
        assert_eq!(remote_url_scheme("git@github.com:o/n.git").unwrap(), "ssh");
        assert_eq!(remote_url_scheme("file:///srv/repo.git").unwrap(), "file");
        for bad in [
            "",
            "-oProxyCommand=x",
            "ext::sh -c touch% /tmp/x",
            "fd::3",
            "https://token@github.com/o/n.git",
            "https://user:pass@github.com/o/n.git",
            "ssh://user:pass@host/o/n.git",
            "https://github.com/o/n\n.git",
            "http://github.com/o/n.git",
            "git://github.com/o/n.git",
            "file://relative",
            "/srv/repo.git",
        ] {
            assert_eq!(
                remote_url_scheme(bad).unwrap_err().exit_code(),
                2,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn version_parsing() {
        let parse = |s: &str| GitVersion::parse(s).unwrap().to_string();
        assert_eq!(parse("git version 2.43.0\n"), "2.43.0");
        assert_eq!(parse("git version 2.42"), "2.42.0");
        assert_eq!(parse("git version 2.39.5 (Apple Git-154)"), "2.39.5");
        assert_eq!(parse("git version 2.45.1.windows.1"), "2.45.1");
        assert!(GitVersion::parse("hub version 2.43").is_none());
        assert!(GitVersion::parse("git version 2.42").unwrap() < GIT_FLOOR);
        assert!(GitVersion::parse("git version 2.43.0").unwrap() >= GIT_FLOOR);
    }

    #[test]
    fn sanitizer_keeps_only_builtin_merge_attributes() {
        let input = b"# comment\n\
            [attr]evil merge=evil\n\
            *.u merge=union conflict-marker-size=50000000 filter=t diff=t\n\
            *.b merge=binary eol=crlf\n\
            *.t merge=text export-subst\n\
            *.n -merge\n\
            *.x !merge\n\
            *.m merge\n\
            *.bin binary\n\
            *.e merge=evil evil export-ignore\n\
            !neg merge=union\n\
            \"quoted name\" merge=union\r\n\
            \t  indented merge=union\n";
        assert_eq!(
            String::from_utf8(sanitize_gitattributes(input)).unwrap(),
            "*.u merge=union\n\
             *.b merge=binary\n\
             *.t merge=text\n\
             *.n -merge\n\
             *.x !merge\n\
             *.m merge\n\
             *.bin -merge\n\
             \"quoted name\" merge=union\n\
             indented merge=union\n"
        );
        let long = format!("*.l {} merge=union\n", "x".repeat(ATTR_MAX_LINE_LENGTH));
        assert!(sanitize_gitattributes(long.as_bytes()).is_empty());
    }

    #[test]
    fn sanitizer_splits_invalid_quoted_patterns_at_the_first_blank_like_git() {
        // git's unquote_c_style rejects these escapes, so git reads the raw
        // pattern up to the first blank and the rest as attribute states.
        let input = br#""u\q conflict-marker-size=50000000" merge
"u\08 conflict-marker-size=7" merge
"u\4007 conflict-marker-size=7" merge
"u\0 conflict-marker-size=7" merge
"open conflict-marker-size=7 merge
"esc\" merge=union
"ok\101\t\"\\" merge=union conflict-marker-size=7
"\041neg" merge=union
"[attr]merge conflict-marker-size=7" merge
"#
        .as_slice();
        assert_eq!(
            String::from_utf8(sanitize_gitattributes(input)).unwrap(),
            r#""u\q merge
"u\08 merge
"u\4007 merge
"u\0 merge
"open merge
"esc\" merge=union
"ok\101\t\"\\" merge=union
"#
        );
    }

    #[test]
    fn unquote_c_style_matches_git() {
        assert_eq!(
            unquote_c_style(br#""a\a\b\f\n\r\t\v\\\"\000\377z" rest"#),
            Some((
                br#""a\a\b\f\n\r\t\v\\\"\000\377z""#.len(),
                b"a\x07\x08\x0c\n\r\t\x0b\\\"\x00\xffz".to_vec()
            ))
        );
        for rejected in [
            br#""\q""#.as_slice(),
            br#""\08""#,
            br#""\400""#,
            br#""\0""#,
            br#""\"#,
            br#""open"#,
            b"plain",
        ] {
            assert_eq!(unquote_c_style(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn empty_dir_startup_check_refuses_planted_entries() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("empty");
        prepare_empty_dir(&dir).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o500);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(dir.join("planted"), "x").unwrap();
        assert_eq!(prepare_empty_dir(&dir).unwrap_err().exit_code(), 3);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();

        let link = root.path().join("link");
        std::os::unix::fs::symlink(root.path(), &link).unwrap();
        assert_eq!(prepare_empty_dir(&link).unwrap_err().exit_code(), 3);
    }
}
