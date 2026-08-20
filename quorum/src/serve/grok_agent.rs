//! Official Grok Build CLI transport: command construction, bounded process IO,
//! and normalization of the native headless `streaming-json` protocol.
//!
//! This adapter intentionally uses only `grok -p`/`--resume`. It does not use
//! ACP server internals, infer the runner from an executable name, or emulate
//! Claude/Codex flags.

use super::runner::{
    capture_diagnostics, tool_summary, ActivityKind, AdapterConfig, AgentEvent, AgentKind,
    AgentMcpServer, CapturedOutput, DiagnosticBuffer, FailureDisposition, FailureObservation,
    FailureTracker, LaunchMode, LaunchRequest, NormalizedLine, RunnerFailure, TokenUsage,
    WorkerTurnRequest,
};
use std::collections::VecDeque;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{symlink, OpenOptionsExt};
use std::path::PathBuf;
#[cfg(test)]
use std::pin::Pin;
use std::process::Stdio;
#[cfg(test)]
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;
#[cfg(test)]
use tokio::io::ReadBuf;
use tokio::process::{Child, ChildStdout, Command};

pub const SUPPORTED_MODEL: &str = "grok-4.5";
pub const SUPPORTED_EFFORTS: &[&str] = &["low", "medium", "high"];
pub const DEFAULT_SANDBOX: &str = "workspace";
pub const DEFAULT_PERMISSION_MODE: &str = "bypassPermissions";
pub const DEFAULT_MAX_TURNS: u32 = 64;
pub const MAX_CONFIGURED_TURNS: u32 = 256;

const RESTRICTED_SANDBOX: &str = "read-only";
const RESTRICTED_PERMISSION_MODE: &str = "dontAsk";
const RESTRICTED_MAX_TURNS: u32 = 8;
const MANAGED_WORKSPACE_SANDBOX: &str = "quorum_managed_workspace";
const STDOUT_LINE_BYTES: usize = 1024 * 1024;
const TERMINAL_STDOUT_LINES: usize = 256;
const MAX_SESSION_ID_BYTES: usize = 1024;
const AUTH_STATE_FILE: &str = "auth.json";
const AUTH_LOCK_FILE: &str = "auth.json.lock";
const MANAGED_CONFIG_FILES: &[&str] = &["config.toml", "managed_config.toml", "requirements.toml"];
const SYSTEM_GROK_CONFIG_DIR: &str = "/etc/grok";
const SYSTEM_MANAGED_CONFIG_FILES: &[&str] = &["managed_config.toml", "requirements.toml"];

#[cfg(test)]
struct InjectedStderrReadError;

#[cfg(test)]
impl tokio::io::AsyncRead for InjectedStderrReadError {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::other(
            "injected Grok stderr read error",
        )))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GrokAdapterConfig<'a> {
    pub sandbox: &'a str,
    pub permission_mode: &'a str,
    pub max_turns: u32,
}

impl Default for GrokAdapterConfig<'static> {
    fn default() -> Self {
        Self {
            sandbox: DEFAULT_SANDBOX,
            permission_mode: DEFAULT_PERMISSION_MODE,
            max_turns: DEFAULT_MAX_TURNS,
        }
    }
}

impl GrokAdapterConfig<'_> {
    pub fn validate(self) -> Result<(), String> {
        if !matches!(self.sandbox, "off" | "workspace") {
            return Err(format!(
                "unsupported Grok sandbox '{}': the transport adapter accepts only 'off' or \
                 'workspace' for coding turns",
                self.sandbox
            ));
        }
        if self.permission_mode != DEFAULT_PERMISSION_MODE {
            return Err(format!(
                "unsupported Grok permission_mode '{}': unattended coding turns require \
                 'bypassPermissions'",
                self.permission_mode
            ));
        }
        if !(1..=MAX_CONFIGURED_TURNS).contains(&self.max_turns) {
            return Err(format!(
                "unsupported Grok max_turns {}: expected 1..={MAX_CONFIGURED_TURNS}",
                self.max_turns
            ));
        }
        Ok(())
    }
}

pub struct GrokSpec {
    pub model: String,
    pub effort: String,
    pub worktree: PathBuf,
    pub prompt: String,
    pub env_vars: Vec<(String, String)>,
    pub sandbox: String,
    pub permission_mode: String,
    pub max_turns: u32,
}

impl GrokSpec {
    fn validate(&self) -> std::io::Result<()> {
        if self.model != SUPPORTED_MODEL {
            return Err(invalid_input(format!(
                "unsupported Grok model '{}': expected '{SUPPORTED_MODEL}'",
                self.model
            )));
        }
        if !SUPPORTED_EFFORTS.contains(&self.effort.as_str()) {
            return Err(invalid_input(format!(
                "unsupported Grok effort '{}': expected one of {}",
                self.effort,
                SUPPORTED_EFFORTS.join(", ")
            )));
        }
        GrokAdapterConfig {
            sandbox: &self.sandbox,
            permission_mode: &self.permission_mode,
            max_turns: self.max_turns,
        }
        .validate()
        .map_err(invalid_input)
    }
}

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

struct GrokMcpConfigHome {
    root: tempfile::TempDir,
    source_home: PathBuf,
    auth_snapshot: Option<Vec<u8>>,
    sandbox_profile: Option<&'static str>,
}

impl GrokMcpConfigHome {
    fn create(spec: &GrokSpec, server: AgentMcpServer) -> std::io::Result<Self> {
        Self::create_with_system_config_dir(
            spec,
            server,
            std::path::Path::new(SYSTEM_GROK_CONFIG_DIR),
        )
    }

    fn create_with_system_config_dir(
        spec: &GrokSpec,
        server: AgentMcpServer,
        system_config_dir: &std::path::Path,
    ) -> std::io::Result<Self> {
        if let Some(path) = system_mcp_or_plugin_source(system_config_dir)? {
            return Err(invalid_input(format!(
                "managed Grok MCP refuses system MCP/plugin source '{}'",
                path.display()
            )));
        }
        if let Some(path) = project_mcp_source(&spec.worktree)? {
            return Err(invalid_input(format!(
                "managed Grok MCP refuses project MCP source '{}'",
                path.display()
            )));
        }
        let source_home = grok_home_for_child(spec)?;
        std::fs::create_dir_all(&source_home)?;
        let source_home = std::fs::canonicalize(source_home)?;
        let source_home_text = source_home.to_str().ok_or_else(|| {
            invalid_input("Grok home must be valid UTF-8 for managed configuration")
        })?;
        // Session storage must survive the invocation-local config home so a
        // later `--resume` resolves the exact provider-issued identity.
        std::fs::create_dir_all(source_home.join("sessions"))?;
        // Take the same lock Grok uses before snapshotting auth state. It is
        // released before the provider starts; teardown compares this exact
        // snapshot and never restores an unchanged stale copy over a newer
        // managed refresh.
        let auth_lock = acquire_auth_lock(&source_home)?;

        let linked_entries = std::fs::read_dir(&source_home)?
            .map(|entry| entry.map(|entry| (entry.file_name(), entry.path())))
            .collect::<Result<Vec<_>, _>>()?;
        // Keep the shadow under the provider's real home so Grok's workspace
        // sandbox sees the same trusted state root it uses without injection.
        let root = tempfile::Builder::new()
            .prefix("quorum-config-")
            .tempdir_in(&source_home)?;
        let mut auth_snapshot = None;
        for (name, path) in linked_entries {
            if matches!(
                name.to_str(),
                Some(
                    "config.toml"
                        | "managed_config.toml"
                        | "requirements.toml"
                        | "sandbox.toml"
                        | "hooks"
                        | "hooks-paths"
                        | "plugins"
                        | "plugin-data"
                        | "trusted_folders.toml"
                )
            ) || name.to_string_lossy().starts_with("quorum-config-")
            {
                continue;
            }
            // Grok refreshes OAuth by atomically replacing auth.json. A
            // rename would replace a symlink in this disposable home instead
            // of its target, so materialize it and persist it on teardown.
            // Other state, including the provider's auth lock, remains linked
            // to the durable home.
            if name == AUTH_STATE_FILE {
                auth_snapshot = Some(copy_auth_state(&path, &root.path().join(&name))?);
                continue;
            }
            symlink(path, root.path().join(name))?;
        }
        drop(auth_lock);

        // Workspace-family profiles kernel-protect these two direct global
        // hook sources and refuse symlinks there. Keep real nodes in the
        // process home, referencing the original hook directory through the
        // registry so the operator's restrictions remain active.
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(root.path().join("hooks"))?;
        }
        #[cfg(not(unix))]
        std::fs::create_dir(root.path().join("hooks"))?;

        let source_hooks = source_home.join("hooks");
        let mut hooks_paths = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(root.path().join("hooks-paths"))?;
        match std::fs::symlink_metadata(&source_hooks) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_input("Grok hooks directory must not be a symlink"));
            }
            Ok(metadata) if metadata.is_dir() => {
                hooks_paths.write_all(source_home_text.as_bytes())?;
                hooks_paths.write_all(b"/hooks")?;
                hooks_paths.write_all(b"\n")?;
            }
            Ok(_) => return Err(invalid_input("Grok hooks path must be a directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let source_hooks_paths = source_home.join("hooks-paths");
        match std::fs::symlink_metadata(&source_hooks_paths) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_input(
                    "Grok hooks-paths registry must not be a symlink",
                ));
            }
            Ok(metadata) if metadata.is_file() => {
                let mut source = std::fs::File::open(source_hooks_paths)?;
                std::io::copy(&mut source, &mut hooks_paths)?;
            }
            Ok(_) => return Err(invalid_input("Grok hooks-paths registry must be a file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        hooks_paths.sync_all()?;

        // Grok merges every full config layer. Give the provider private
        // copies with every MCP-producing surface removed, then register only
        // the daemon endpoint in its user layer. The normal user home remains
        // untouched for later un-managed invocations and exact-session state.
        write_managed_config_layer(
            &source_home.join("config.toml"),
            &root.path().join("config.toml"),
            Some(server),
        )?;
        for filename in MANAGED_CONFIG_FILES.iter().skip(1) {
            write_managed_config_layer(
                &source_home.join(filename),
                &root.path().join(filename),
                None,
            )?;
        }

        let sandbox_profile =
            (spec.sandbox == DEFAULT_SANDBOX).then_some(MANAGED_WORKSPACE_SANDBOX);
        if let Some(profile) = sandbox_profile {
            let sandbox = toml::Value::Table(toml::Table::from_iter([(
                "profiles".into(),
                toml::Value::Table(toml::Table::from_iter([(
                    profile.into(),
                    toml::Value::Table(toml::Table::from_iter([
                        (
                            "extends".into(),
                            toml::Value::String(DEFAULT_SANDBOX.into()),
                        ),
                        (
                            "read_write".into(),
                            toml::Value::Array(vec![toml::Value::String(source_home_text.into())]),
                        ),
                    ])),
                )])),
            )]));
            let rendered = toml::to_string_pretty(&sandbox).map_err(|error| {
                std::io::Error::other(format!(
                    "failed to render Grok managed sandbox config: {error}"
                ))
            })?;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(root.path().join("sandbox.toml"))?;
            file.write_all(rendered.as_bytes())?;
            file.sync_all()?;
        }

        Ok(Self {
            root,
            source_home,
            auth_snapshot,
            sandbox_profile,
        })
    }

    fn path(&self) -> &std::path::Path {
        self.root.path()
    }

    fn sandbox_profile(&self) -> Option<&'static str> {
        self.sandbox_profile
    }

    fn persist_auth_state(&self) -> std::io::Result<()> {
        let _lock = acquire_auth_lock(&self.source_home)?;
        let auth_state = self.root.path().join(AUTH_STATE_FILE);
        let durable_auth_state = self.source_home.join(AUTH_STATE_FILE);
        let Some((permissions, contents)) = read_auth_state(&auth_state)? else {
            // Preserve a concurrent refresh or logout. An invocation that
            // removed its own unchanged snapshot may only remove the durable
            // state if no other managed invocation changed it first.
            if self.auth_snapshot.is_some()
                && read_auth_state(&durable_auth_state)?.map(|(_, contents)| contents)
                    == self.auth_snapshot
            {
                std::fs::remove_file(durable_auth_state)?;
            }
            return Ok(());
        };
        // A private auth copy that was not refreshed must never overwrite a
        // newer durable credential written by another managed Grok home.
        if self.auth_snapshot.as_deref() == Some(contents.as_slice()) {
            return Ok(());
        }

        let mut replacement = tempfile::Builder::new()
            .prefix(".quorum-auth-")
            .tempfile_in(&self.source_home)?;
        replacement.as_file().set_permissions(permissions)?;
        replacement.as_file_mut().write_all(&contents)?;
        replacement.as_file_mut().sync_all()?;
        replacement
            .persist(durable_auth_state)
            .map_err(|error| error.error)?;
        Ok(())
    }
}

/// Grok treats the current worktree as an additional MCP configuration layer.
/// A private provider home cannot replace those files, and allowing them would
/// create tools that bypass the daemon's role/phase inventory. Reject a managed
/// launch before the provider sees such a source rather than silently merging
/// or merely hiding it behind a client-side discovery policy.
fn project_mcp_source(worktree: &std::path::Path) -> std::io::Result<Option<PathBuf>> {
    let worktree = std::fs::canonicalize(worktree)?;
    let mut directory = worktree.clone();
    let mut directories = vec![directory.clone()];
    loop {
        // Grok's project walk is bounded by the discovered Git worktree. A
        // linked worktree uses a .git file, so metadata rather than is_dir is
        // deliberately used here.
        if directory.join(".git").exists() {
            break;
        }
        let Some(parent) = directory.parent() else {
            // Outside Git, Grok considers only its direct current directory.
            directories.truncate(1);
            break;
        };
        directory = parent.to_path_buf();
        directories.push(directory.clone());
    }
    for directory in directories {
        for path in [
            directory.join(".grok/config.toml"),
            directory.join(".mcp.json"),
            directory.join(".cursor/mcp.json"),
        ] {
            if has_project_mcp_servers(&path)? {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

/// Grok loads organization-controlled managed and requirements files from an
/// absolute system directory in addition to the invocation's private home.
/// Those layers cannot be replaced with private copies. Refuse a managed
/// launch if they could register provider-native tools, rather than letting
/// them bypass the daemon-derived role/phase inventory.
fn system_mcp_or_plugin_source(
    system_config_dir: &std::path::Path,
) -> std::io::Result<Option<PathBuf>> {
    for filename in SYSTEM_MANAGED_CONFIG_FILES {
        let path = system_config_dir.join(filename);
        if has_toml_mcp_or_plugin_source(&path, "system")? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn has_project_mcp_servers(path: &std::path::Path) -> std::io::Result<bool> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if path
        .extension()
        .is_some_and(|extension| extension == "toml")
    {
        let config = toml::from_str::<toml::Value>(&contents).map_err(|error| {
            invalid_input(format!(
                "Grok project config '{}' is invalid: {error}",
                path.display()
            ))
        })?;
        let root = config
            .as_table()
            .ok_or_else(|| invalid_input("Grok project config root must be a TOML table"))?;
        return Ok(["mcp_servers", "mcpServers"]
            .into_iter()
            .any(|key| root.get(key).is_some_and(toml_value_is_nonempty_table)));
    }
    let config = serde_json::from_str::<serde_json::Value>(&contents).map_err(|error| {
        invalid_input(format!(
            "Grok project MCP config '{}' is invalid: {error}",
            path.display()
        ))
    })?;
    Ok(config.get("mcpServers").is_some_and(|servers| {
        servers
            .as_object()
            .is_some_and(|servers| !servers.is_empty())
    }))
}

fn has_toml_mcp_or_plugin_source(path: &std::path::Path, scope: &str) -> std::io::Result<bool> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let config = toml::from_str::<toml::Value>(&contents).map_err(|error| {
        invalid_input(format!(
            "Grok {scope} config '{}' is invalid: {error}",
            path.display()
        ))
    })?;
    let root = config
        .as_table()
        .ok_or_else(|| invalid_input(format!("Grok {scope} config root must be a TOML table")))?;
    Ok([
        "mcp_servers",
        "mcpServers",
        "plugins",
        "marketplace",
        "marketplaces",
    ]
    .into_iter()
    .any(|key| root.get(key).is_some_and(toml_value_is_nonempty_table)))
}

fn toml_value_is_nonempty_table(value: &toml::Value) -> bool {
    value.as_table().is_some_and(|table| !table.is_empty())
}

/// Copy one Grok full-config layer into the invocation home after stripping
/// sources that can add provider-owned MCP servers. The ordinary user layer
/// receives the single daemon-owned endpoint; lower-priority layers receive no
/// MCP or plugin source at all.
fn write_managed_config_layer(
    source: &std::path::Path,
    destination: &std::path::Path,
    server: Option<AgentMcpServer>,
) -> std::io::Result<()> {
    let mut config = match std::fs::read_to_string(source) {
        Ok(contents) => toml::from_str::<toml::Value>(&contents).map_err(|error| {
            invalid_input(format!(
                "Grok config '{}' is invalid: {error}",
                source.display()
            ))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && server.is_some() => {
            toml::Value::Table(toml::Table::new())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let root = config
        .as_table_mut()
        .ok_or_else(|| invalid_input("Grok config root must be a TOML table"))?;
    for key in [
        "mcp_servers",
        "mcpServers",
        "plugins",
        "marketplace",
        "marketplaces",
    ] {
        root.remove(key);
    }
    // Plugins and marketplaces can contribute stdio MCP servers independently
    // of the native server table, so a managed invocation must not retain
    // their discovery paths.
    if let Some(server) = server {
        root.insert(
            "mcp_servers".into(),
            toml::Value::Table(toml::Table::from_iter([(
                "github".into(),
                toml::Value::Table(toml::Table::from_iter([
                    ("command".into(), toml::Value::String(server.command.into())),
                    (
                        "args".into(),
                        toml::Value::Array(
                            server
                                .args
                                .iter()
                                .map(|arg| toml::Value::String((*arg).into()))
                                .collect(),
                        ),
                    ),
                ])),
            )])),
        );
    }
    let rendered = toml::to_string_pretty(&config).map_err(|error| {
        std::io::Error::other(format!("failed to render Grok MCP config: {error}"))
    })?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(destination)?;
    file.write_all(rendered.as_bytes())?;
    file.sync_all()
}

/// Apply the complete managed-MCP process boundary. Grok starts stdio MCP
/// servers with its own environment, so removing authority at this process
/// boundary also removes it from the nested `quorum agent-mcp` child.
fn configure_managed_mcp_environment(command: &mut Command, home: &GrokMcpConfigHome) {
    for name in ["GH_TOKEN", "GITHUB_TOKEN", "QUORUM_HOME", "GH_CONFIG_DIR"] {
        command.env_remove(name);
    }
    // `HOME` is used by the official compatibility loaders. Point it at the
    // private config home so an operator's ~/.claude.json or ~/.cursor/mcp.json
    // cannot leak a second MCP server into the managed run.
    command
        .env("GROK_HOME", home.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path().join("xdg-config"))
        // These are the provider's documented source gates. The environment
        // precedence makes them immune to settings in copied config layers.
        .env("GROK_CLAUDE_MCPS_ENABLED", "0")
        .env("GROK_CURSOR_MCPS_ENABLED", "0")
        // Project .grok/.mcp sources are allowed only after an explicit trust
        // grant. The managed home intentionally carries no trust-store link.
        .env("GROK_FOLDER_TRUST", "1");
}

fn acquire_auth_lock(source_home: &std::path::Path) -> std::io::Result<std::fs::File> {
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(source_home.join(AUTH_LOCK_FILE))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(lock)
}

impl Drop for GrokMcpConfigHome {
    fn drop(&mut self) {
        // GrokProc keeps this home through kill-and-reap, so this runs only
        // after the provider can no longer update its invocation-local file.
        // Drop cannot surface an error; a later launch still retains the
        // previous durable credential if the handoff itself fails.
        let _ = self.persist_auth_state();
    }
}

fn copy_auth_state(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> std::io::Result<Vec<u8>> {
    let (permissions, contents) = read_auth_state(source)?
        .ok_or_else(|| invalid_input("Grok auth state disappeared during setup"))?;
    let mut destination = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(destination)?;
    destination.set_permissions(permissions)?;
    destination.write_all(&contents)?;
    destination.sync_all()?;
    Ok(contents)
}

fn read_auth_state(
    path: &std::path::Path,
) -> std::io::Result<Option<(std::fs::Permissions, Vec<u8>)>> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() {
        return Err(invalid_input("Grok auth state must be a regular file"));
    }
    Ok(Some((metadata.permissions(), std::fs::read(path)?)))
}

fn args_with_managed_sandbox(
    args: &[String],
    profile: Option<&str>,
) -> std::io::Result<Vec<String>> {
    let mut args = args.to_vec();
    let Some(profile) = profile else {
        return Ok(args);
    };
    let sandbox = args
        .iter()
        .position(|arg| arg == "--sandbox")
        .and_then(|position| args.get_mut(position + 1))
        .ok_or_else(|| invalid_input("managed Grok launch omitted its sandbox argument"))?;
    *sandbox = profile.into();
    Ok(args)
}

fn grok_home_for_child(spec: &GrokSpec) -> std::io::Result<PathBuf> {
    fn requested<'a>(spec: &'a GrokSpec, key: &str) -> Option<&'a str> {
        spec.env_vars
            .iter()
            .rev()
            .find_map(|(candidate, value)| (candidate == key).then_some(value.as_str()))
    }

    let grok_home = match requested(spec, "GROK_HOME") {
        Some(value) => (!value.is_empty()).then(|| PathBuf::from(value)),
        None => std::env::var_os("GROK_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
    };
    let path = if let Some(path) = grok_home {
        path
    } else {
        let home = match requested(spec, "HOME") {
            Some(value) => (!value.is_empty()).then(|| PathBuf::from(value)),
            None => std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
        }
        .or_else(|| directories::BaseDirs::new().map(|base| base.home_dir().to_path_buf()))
        .ok_or_else(|| invalid_input("cannot resolve Grok home for managed MCP config"))?;
        home.join(".grok")
    };
    Ok(if path.is_absolute() {
        path
    } else {
        spec.worktree.join(path)
    })
}

fn common_args(
    spec: &GrokSpec,
    sandbox: &str,
    permission_mode: &str,
    max_turns: u32,
) -> Vec<String> {
    vec![
        "-p".into(),
        spec.prompt.clone(),
        "--output-format".into(),
        "streaming-json".into(),
        "--model".into(),
        spec.model.clone(),
        "--reasoning-effort".into(),
        spec.effort.clone(),
        "--permission-mode".into(),
        permission_mode.into(),
        "--sandbox".into(),
        sandbox.into(),
        "--max-turns".into(),
        max_turns.to_string(),
        "--verbatim".into(),
    ]
}

/// Pinned first-turn argument shape for the official headless CLI.
pub fn headless_args(spec: &GrokSpec, mode: LaunchMode) -> std::io::Result<Vec<String>> {
    spec.validate()?;
    Ok(match mode {
        LaunchMode::Normal => {
            common_args(spec, &spec.sandbox, &spec.permission_mode, spec.max_turns)
        }
        LaunchMode::Restricted => common_args(
            spec,
            RESTRICTED_SANDBOX,
            RESTRICTED_PERMISSION_MODE,
            spec.max_turns.min(RESTRICTED_MAX_TURNS),
        ),
    })
}

/// Pinned exact-session continuation shape. Grok persists the original
/// sandbox with the session and refuses a mismatched profile itself; Quorum
/// supplies the same validated configuration on every turn.
pub fn resume_args(session_id: &str, spec: &GrokSpec) -> std::io::Result<Vec<String>> {
    spec.validate()?;
    if !valid_session_id(session_id) {
        return Err(invalid_input("Grok continuation session ID is malformed"));
    }
    let mut args = vec!["--resume".into(), session_id.into()];
    args.extend(common_args(
        spec,
        &spec.sandbox,
        &spec.permission_mode,
        spec.max_turns,
    ));
    Ok(args)
}

struct BoundedStdout {
    reader: ChildStdout,
    chunk: Box<[u8; 8192]>,
    position: usize,
    filled: usize,
    line: Vec<u8>,
    dropped: usize,
    eof: bool,
    clean_eof: bool,
    read_error: Option<String>,
    #[cfg(test)]
    lines_returned: usize,
    #[cfg(test)]
    injected_read_error_after_lines: Option<usize>,
}

impl BoundedStdout {
    fn new(reader: ChildStdout) -> Self {
        Self {
            reader,
            chunk: Box::new([0; 8192]),
            position: 0,
            filled: 0,
            line: Vec::new(),
            dropped: 0,
            eof: false,
            clean_eof: false,
            read_error: None,
            #[cfg(test)]
            lines_returned: 0,
            #[cfg(test)]
            injected_read_error_after_lines: None,
        }
    }

    async fn next_line(&mut self) -> Option<String> {
        loop {
            while self.position < self.filled {
                let byte = self.chunk[self.position];
                self.position += 1;
                if byte == b'\n' {
                    return Some(self.finish_line());
                }
                if self.line.len() < STDOUT_LINE_BYTES {
                    self.line.push(byte);
                } else {
                    self.dropped = self.dropped.saturating_add(1);
                }
            }

            if self.eof {
                if self.line.is_empty() && self.dropped == 0 {
                    return None;
                }
                return Some(self.finish_line());
            }

            #[cfg(test)]
            if self
                .injected_read_error_after_lines
                .is_some_and(|lines| self.lines_returned >= lines)
            {
                self.injected_read_error_after_lines = None;
                self.read_error = Some("injected Grok stdout read error".into());
                self.eof = true;
                continue;
            }

            match self.reader.read(&mut self.chunk[..]).await {
                Ok(0) => {
                    self.eof = true;
                    self.clean_eof = true;
                }
                Err(error) => {
                    self.eof = true;
                    self.read_error = Some(error.to_string());
                }
                Ok(read) => {
                    self.position = 0;
                    self.filled = read;
                }
            }
        }
    }

    fn finish_line(&mut self) -> String {
        #[cfg(test)]
        {
            self.lines_returned = self.lines_returned.saturating_add(1);
        }
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.dropped > 0 {
            let dropped = self.dropped;
            self.line.clear();
            self.dropped = 0;
            return serde_json::json!({
                "type": "provider.stdout_bytes_truncated",
                "dropped_bytes": dropped,
                "line_limit_bytes": STDOUT_LINE_BYTES,
            })
            .to_string();
        }
        let line = match std::str::from_utf8(&self.line) {
            Ok(line) => line.to_string(),
            Err(error) => serde_json::json!({
                "type": "provider.stdout_invalid_utf8",
                "line_bytes": self.line.len(),
                "valid_up_to": error.valid_up_to(),
                "invalid_sequence_bytes": error.error_len(),
            })
            .to_string(),
        };
        self.line.clear();
        line
    }

    fn reached_clean_eof(&self) -> bool {
        self.clean_eof
    }

    fn take_read_error(&mut self) -> Option<String> {
        self.read_error.take()
    }

    #[cfg(test)]
    fn inject_read_error_after_lines(&mut self, lines: usize) {
        self.injected_read_error_after_lines = Some(lines);
    }
}

pub struct GrokProc {
    child: Child,
    process_group_id: libc::pid_t,
    reader: BoundedStdout,
    diagnostics: DiagnosticBuffer,
    failures: FailureTracker,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    pending_terminal: Option<String>,
    terminal_rejected: bool,
    stdout_complete: bool,
    terminal_exit_status: Option<std::process::ExitStatus>,
    worker_request: Option<Box<WorkerTurnRequest>>,
    // Keeps the invocation-local full config layer alive until the child and
    // its MCP subprocess have been killed and reaped.
    _mcp_config_home: Option<GrokMcpConfigHome>,
}

impl GrokProc {
    pub fn launch(
        request: &LaunchRequest<'_>,
        config: &AdapterConfig<'_>,
    ) -> std::io::Result<Self> {
        if request.mode == LaunchMode::Restricted && request.continuation_id.is_some() {
            return Err(invalid_input(
                "restricted Grok launch cannot resume a prior session",
            ));
        }
        let spec = GrokSpec {
            model: request.model.to_string(),
            effort: request.effort.to_string(),
            worktree: request.worktree.to_path_buf(),
            prompt: request.prompt.to_string(),
            env_vars: request.environment.to_vec(),
            sandbox: config.grok.sandbox.to_string(),
            permission_mode: config.grok.permission_mode.to_string(),
            max_turns: config.grok.max_turns,
        };
        let args = match request.continuation_id {
            Some(session_id) => resume_args(session_id, &spec)?,
            None => headless_args(&spec, request.mode)?,
        };
        Self::spawn_command(&spec, &args, request.agent_mcp_server(), config.executable)
    }

    #[cfg(test)]
    fn spawn(spec: &GrokSpec, grok_bin: Option<&str>) -> std::io::Result<Self> {
        let args = headless_args(spec, LaunchMode::Normal)?;
        Self::spawn_command(spec, &args, None, grok_bin)
    }

    fn spawn_command(
        spec: &GrokSpec,
        args: &[String],
        agent_mcp: Option<AgentMcpServer>,
        grok_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let has_agent_mcp = agent_mcp.is_some();
        let mcp_config_home = agent_mcp
            .map(|server| GrokMcpConfigHome::create(spec, server))
            .transpose()?;
        let args = args_with_managed_sandbox(
            args,
            mcp_config_home
                .as_ref()
                .and_then(GrokMcpConfigHome::sandbox_profile),
        )?;
        let mut command = Command::new(grok_bin.unwrap_or("grok"));
        command.args(args);
        // A shared leader resolves its own persistent config and would discard
        // this process-scoped layer. Managed MCP turns therefore stay on the
        // dedicated headless process that owns their exact run capability.
        if has_agent_mcp {
            command.arg("--no-leader");
        }
        for (key, value) in &spec.env_vars {
            command.env(key, value);
        }
        // `GROK_CONFIG` deliberately drops code-execution tables including
        // mcp_servers. Point this process at a private full config.toml layer.
        // Its custom workspace profile grants the original Grok state root,
        // preserving durable session/auth writes through the linked entries.
        if let Some(home) = &mcp_config_home {
            configure_managed_mcp_environment(&mut command, home);
        }
        command
            .current_dir(&spec.worktree)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = command.spawn()?;
        // `Child::id()` becomes `None` after `try_wait()` observes leader
        // exit, but descendants may still hold the process group's pipes.
        // Persist the group ID before any caller can reap the leader.
        let process_group_id = child
            .id()
            .ok_or_else(|| std::io::Error::other("spawned Grok process has no process-group ID"))?
            as libc::pid_t;
        let reader = BoundedStdout::new(child.stdout.take().expect("stdout was piped"));
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Grok);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = child.stderr.take().expect("stderr was piped");
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        Ok(Self {
            child,
            process_group_id,
            reader,
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
            pending_terminal: None,
            terminal_rejected: false,
            stdout_complete: false,
            terminal_exit_status: None,
            worker_request: None,
            _mcp_config_home: mcp_config_home,
        })
    }

    pub fn normalize_line(raw: &str) -> NormalizedLine {
        NormalizedLine {
            events: normalize_grok_line(raw),
            terminal_text: None,
        }
    }

    pub(super) fn failure_observation(raw: &str) -> FailureObservation {
        if raw.is_empty() {
            return FailureObservation::inert();
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
            return FailureObservation::classified(
                FailureDisposition::NonFailover,
                "Grok emitted malformed streaming-json",
            );
        };
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("end")
                if value
                    .get("sessionId")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(valid_session_id) =>
            {
                FailureObservation::success()
            }
            Some("end" | "provider.stdout_invalid_utf8" | "provider.stdout_bytes_truncated") => {
                FailureObservation::classified(
                    FailureDisposition::NonFailover,
                    "Grok terminal protocol was invalid",
                )
            }
            Some("error") => FailureObservation::unknown_failure(
                "Grok terminal error has no enabled availability classifier",
            ),
            _ => FailureObservation::inert(),
        }
    }

    pub(super) fn stderr_failure_observation(text: &str) -> FailureObservation {
        if text.is_empty() {
            FailureObservation::inert()
        } else {
            FailureObservation::unknown_failure(
                "Grok stderr has no enabled availability classifier",
            )
        }
    }

    pub fn classify_pre_authoritative_exit(
        &self,
        status: std::process::ExitStatus,
    ) -> Option<RunnerFailure> {
        if let Some(failure) = self.failures.observed_strict_failure() {
            return Some(failure);
        }
        self.failures.classify_exit(status)
    }

    pub fn observed_pre_authoritative_failure(&self) -> Option<RunnerFailure> {
        self.failures.observed_strict_failure()
    }

    pub(super) fn failure_tracker(&self) -> FailureTracker {
        self.failures.clone()
    }

    #[allow(dead_code)] // dormant internal boundary; managed Grok routing is still rejected
    pub(super) fn set_worker_request(&mut self, request: WorkerTurnRequest) {
        self.worker_request = Some(Box::new(request));
    }

    pub(super) fn worker_request(&self) -> Option<&WorkerTurnRequest> {
        self.worker_request.as_deref()
    }

    pub(super) async fn finish_stderr_until(&mut self, deadline: tokio::time::Instant) -> bool {
        let Some(mut task) = self.stderr_task.take() else {
            return true;
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                task.abort();
                let _ = task.await;
                false
            }
        }
    }

    pub async fn next_raw_line(&mut self) -> Option<String> {
        let line = self.reader.next_line().await;
        if let Some(error) = self.reader.take_read_error() {
            self.failures.note_incomplete_evidence(format!(
                "Grok terminal stdout evidence could not be read: {error}"
            ));
            self.terminal_rejected = true;
        }
        let Some(raw) = line else {
            self.stdout_complete = self.reader.reached_clean_eof();
            return None;
        };

        self.failures.observe_stdout(&raw);
        if self.pending_terminal.is_some() {
            let detail = match terminal_session_id(&raw) {
                Some(_) => "Grok emitted a duplicate or conflicting terminal session ID",
                None => "Grok emitted output after its terminal session event",
            };
            self.failures.note_incomplete_evidence(detail);
            self.terminal_rejected = true;
        } else if terminal_session_id(&raw).is_some() {
            // Preserve the raw terminal record immediately. Lifecycle events
            // remain withheld until `authorized_terminal` proves clean
            // EOF, zero exit, and complete stderr evidence.
            self.pending_terminal = Some(raw.clone());
        }
        Some(raw)
    }

    /// Return a buffered Grok terminal record only after every source of
    /// contradictory process evidence is complete. This method never waits
    /// on a running child or stderr task, so polling cancellation cannot lose
    /// the pending provider identity.
    pub(super) async fn authorized_terminal(&mut self) -> Option<String> {
        if self.pending_terminal.is_none() || self.terminal_rejected || !self.stdout_complete {
            return None;
        }

        if self.terminal_exit_status.is_none() {
            match self.child.try_wait() {
                Ok(Some(status)) => self.terminal_exit_status = Some(status),
                Ok(None) => return None,
                Err(error) => {
                    self.failures.note_incomplete_evidence(format!(
                        "Grok terminal process status was unavailable: {error}"
                    ));
                    self.terminal_rejected = true;
                    return None;
                }
            }
        }
        let status = self
            .terminal_exit_status
            .expect("terminal exit status was checked above");
        if !status.success() {
            self.failures.note_incomplete_evidence(format!(
                "Grok emitted terminal success but exited with {status}"
            ));
            self.terminal_rejected = true;
            return None;
        }

        if self
            .stderr_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return None;
        }
        if let Some(task) = self.stderr_task.take() {
            if let Err(error) = task.await {
                self.failures.note_incomplete_evidence(format!(
                    "Grok terminal stderr evidence could not be finalized: {error}"
                ));
                self.terminal_rejected = true;
                return None;
            }
        }

        if self.failures.observed_strict_failure().is_some() {
            self.terminal_rejected = true;
            return None;
        }
        // Keep the candidate until slot teardown. A caller may be cancelled
        // or encounter a persistence error after this pure authorization
        // decision; retaining the exact provider record makes that handoff
        // retryable instead of silently losing the identity.
        self.pending_terminal.clone()
    }

    pub(super) fn terminal_evidence_pending(&self) -> bool {
        self.pending_terminal.is_some() && !self.terminal_rejected && self.stdout_complete
    }

    pub(super) fn terminal_candidate_pending(&self) -> bool {
        self.pending_terminal.is_some() && !self.terminal_rejected
    }

    pub(super) fn normalize_stream_line(raw: &str) -> Vec<AgentEvent> {
        if terminal_session_id(raw).is_some() {
            Vec::new()
        } else {
            normalize_grok_line(raw)
        }
    }

    #[cfg(test)]
    pub(super) fn inject_stdout_read_error_after_lines(&mut self, lines: usize) {
        self.reader.inject_read_error_after_lines(lines);
    }

    #[cfg(test)]
    pub(super) async fn inject_stderr_read_error(&mut self) {
        if let Some(task) = self.stderr_task.take() {
            task.abort();
            let _ = task.await;
        }
        let diagnostics = self.diagnostics.clone();
        self.stderr_task = Some(tokio::spawn(async move {
            capture_diagnostics(InjectedStderrReadError, diagnostics).await
        }));
    }

    #[cfg(test)]
    pub(super) fn stderr_evidence_pending(&self) -> bool {
        self.stderr_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    pub async fn next_raw_line_bounded(
        &mut self,
        max_bytes: usize,
    ) -> std::io::Result<Option<String>> {
        let line = self.next_raw_line().await;
        if line.as_ref().is_some_and(|line| line.len() > max_bytes) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Grok stdout record exceeded {max_bytes} bytes"),
            ));
        }
        Ok(line)
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub fn drain_diagnostics(&mut self) -> Vec<CapturedOutput> {
        self.diagnostics.drain()
    }

    pub async fn kill_and_reap(mut self) -> Vec<CapturedOutput> {
        // Always target the spawn-time process group, even when `try_wait`
        // already observed and reaped the leader. Descendants can otherwise
        // retain stdout/stderr and make the drains below wait forever.
        unsafe {
            libc::killpg(self.process_group_id, libc::SIGKILL);
        }
        let _ = self.child.wait().await;

        let mut terminal = VecDeque::new();
        let mut dropped = 0usize;
        while let Some(line) = self.reader.next_line().await {
            if terminal.len() == TERMINAL_STDOUT_LINES {
                terminal.pop_front();
                dropped = dropped.saturating_add(1);
            }
            terminal.push_back(CapturedOutput::Stdout(line));
        }
        let diagnostics = self.diagnostics.clone();
        if let Some(stderr_task) = self.stderr_task.take() {
            let _ = stderr_task.await;
        }
        let mut output = Vec::with_capacity(terminal.len() + usize::from(dropped > 0));
        if dropped > 0 {
            output.push(CapturedOutput::StdoutTruncated { dropped });
        }
        output.extend(terminal);
        output.extend(diagnostics.drain());
        output
    }
}

pub fn normalize_grok_line(raw: &str) -> Vec<AgentEvent> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Vec::new();
    };
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("text") => value
            .get("data")
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .map(|text| vec![AgentEvent::AssistantText { text: text.into() }])
            .unwrap_or_default(),
        Some("tool_call") => {
            let name = value
                .get("toolName")
                .or_else(|| value.get("title"))
                .or_else(|| value.get("kind"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty())
                .unwrap_or("tool");
            let input = value.get("rawInput").unwrap_or(&serde_json::Value::Null);
            vec![AgentEvent::Activity {
                kind: ActivityKind::ToolUse,
                summary: tool_summary(name, input),
            }]
        }
        Some("end") => normalize_end(&value),
        Some("error") => vec![AgentEvent::TurnFailed {
            message: value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .filter(|message| !message.is_empty())
                .unwrap_or("Grok turn failed")
                .to_string(),
            usage: terminal_usage(&value),
            cost_usd: terminal_cost(&value),
        }],
        _ => Vec::new(),
    }
}

fn normalize_end(value: &serde_json::Value) -> Vec<AgentEvent> {
    let Some(session_id) = value.get("sessionId").and_then(serde_json::Value::as_str) else {
        return vec![AgentEvent::TurnFailed {
            message: "Grok end event missing sessionId".into(),
            usage: terminal_usage(value),
            cost_usd: terminal_cost(value),
        }];
    };
    if !valid_session_id(session_id) {
        return vec![AgentEvent::TurnFailed {
            message: "Grok end event missing sessionId or sessionId malformed".into(),
            usage: terminal_usage(value),
            cost_usd: terminal_cost(value),
        }];
    }
    vec![
        AgentEvent::ThreadStarted {
            thread_id: session_id.to_string(),
        },
        AgentEvent::TurnCompleted {
            usage: terminal_usage(value),
            cost_usd: terminal_cost(value),
        },
    ]
}

fn terminal_session_id(raw: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("end") {
        return None;
    }
    value
        .get("sessionId")
        .and_then(serde_json::Value::as_str)
        .filter(|session_id| valid_session_id(session_id))
        .map(str::to_string)
}

fn valid_session_id(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id.len() <= MAX_SESSION_ID_BYTES
        && session_id.trim() == session_id
        && !session_id.chars().any(char::is_control)
}

fn terminal_usage(value: &serde_json::Value) -> Option<TokenUsage> {
    if value
        .get("usage_is_incomplete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let usage = value.get("usage")?;
    let input = usage.get("input_tokens")?.as_u64()?;
    let cache_read = usage
        .get("cache_read_input_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let cache_write = usage
        .get("cache_creation_input_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    Some(TokenUsage {
        input_tokens: input.saturating_add(cache_read).saturating_add(cache_write),
        uncached_input_tokens: input,
        cached_input_tokens: cache_read,
        cache_write_input_tokens: cache_write,
        output_tokens: usage.get("output_tokens")?.as_u64()?,
        reasoning_tokens: usage
            .get("reasoning_output_tokens")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    })
}

fn terminal_cost(value: &serde_json::Value) -> Option<f64> {
    if value
        .get("cost_is_partial")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || value
            .get("usage_is_incomplete")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        return None;
    }
    value
        .get("total_cost_usd")
        .and_then(serde_json::Value::as_f64)
        .filter(|cost| cost.is_finite() && *cost >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    // The installed CLI's unauthenticated protocol startup and invalid-key
    // startup contend when they run concurrently, occasionally delaying one
    // past the bounded assertion window. They exercise one real binary
    // serially while the pure argument and fixture tests remain parallel.
    static REAL_CLI_PROTOCOL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_spec(worktree: &std::path::Path) -> GrokSpec {
        GrokSpec {
            model: SUPPORTED_MODEL.into(),
            effort: "high".into(),
            worktree: worktree.to_path_buf(),
            prompt: "inspect the repository".into(),
            env_vars: Vec::new(),
            sandbox: DEFAULT_SANDBOX.into(),
            permission_mode: DEFAULT_PERMISSION_MODE.into(),
            max_turns: DEFAULT_MAX_TURNS,
        }
    }

    #[test]
    fn managed_mcp_uses_full_config_layer_and_preserves_provider_state() {
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let original_home = root.path().join("grok-home");
        std::fs::create_dir_all(original_home.join("sessions")).unwrap();
        std::fs::write(
            original_home.join("config.toml"),
            "[models]\ndefault_reasoning_effort = \"high\"\n\
             [plugins]\npaths = [\"/operator/plugins\"]\n\
             [[marketplace.sources]]\nname = \"operator\"\ngit = \"https://example.invalid/operator\"\n\
             [mcp_servers.operator]\ncommand = \"operator-mcp\"\n",
        )
        .unwrap();
        std::fs::write(
            original_home.join("managed_config.toml"),
            "[plugins]\npaths = [\"/managed/plugins\"]\n\
             [mcp_servers.managed_operator]\ncommand = \"managed-mcp\"\n",
        )
        .unwrap();
        std::fs::write(
            original_home.join("requirements.toml"),
            "[plugins]\npaths = [\"/required/plugins\"]\n\
             [mcp_servers.required_operator]\ncommand = \"required-mcp\"\n",
        )
        .unwrap();
        std::fs::write(
            original_home.join("sessions").join("existing"),
            "session-42",
        )
        .unwrap();
        std::fs::write(original_home.join("auth-state"), "preserved").unwrap();
        std::fs::create_dir(original_home.join("hooks")).unwrap();
        std::fs::write(original_home.join("hooks").join("guard.json"), "restricted").unwrap();
        std::fs::write(original_home.join("hooks-paths"), "/operator/guard-hooks\n").unwrap();

        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![("GROK_HOME".into(), original_home.display().to_string())];
        let managed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();

        let config: toml::Value =
            toml::from_str(&std::fs::read_to_string(managed.path().join("config.toml")).unwrap())
                .unwrap();
        assert_eq!(
            config["models"]["default_reasoning_effort"].as_str(),
            Some("high")
        );
        assert_eq!(
            config["mcp_servers"]["github"]["command"].as_str(),
            Some("quorum")
        );
        assert_eq!(
            config["mcp_servers"]["github"]["args"].as_array().unwrap(),
            &[toml::Value::String("agent-mcp".into())]
        );
        assert_eq!(config["mcp_servers"].as_table().unwrap().len(), 1);
        assert!(config["mcp_servers"].get("operator").is_none());
        assert!(config.get("plugins").is_none());
        assert!(config.get("marketplace").is_none());
        for config_name in ["managed_config.toml", "requirements.toml"] {
            let layer: toml::Value =
                toml::from_str(&std::fs::read_to_string(managed.path().join(config_name)).unwrap())
                    .unwrap();
            assert!(layer.get("mcp_servers").is_none(), "{config_name}: {layer}");
            assert!(layer.get("plugins").is_none(), "{config_name}: {layer}");
            assert!(layer.get("marketplace").is_none(), "{config_name}: {layer}");
        }
        let sandbox: toml::Value =
            toml::from_str(&std::fs::read_to_string(managed.path().join("sandbox.toml")).unwrap())
                .unwrap();
        let profile = &sandbox["profiles"][MANAGED_WORKSPACE_SANDBOX];
        assert_eq!(profile["extends"].as_str(), Some(DEFAULT_SANDBOX));
        assert_eq!(
            profile["read_write"].as_array().unwrap(),
            &[toml::Value::String(
                std::fs::canonicalize(&original_home)
                    .unwrap()
                    .display()
                    .to_string()
            )]
        );
        let managed_args = args_with_managed_sandbox(
            &headless_args(&spec, LaunchMode::Normal).unwrap(),
            managed.sandbox_profile(),
        )
        .unwrap();
        assert!(managed_args
            .windows(2)
            .any(|pair| { pair == ["--sandbox", MANAGED_WORKSPACE_SANDBOX] }));
        assert!(std::fs::symlink_metadata(managed.path().join("sessions"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(std::fs::symlink_metadata(managed.path().join("hooks"))
            .unwrap()
            .file_type()
            .is_dir());
        assert!(
            !std::fs::symlink_metadata(managed.path().join("hooks-paths"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read_to_string(managed.path().join("hooks-paths")).unwrap(),
            format!(
                "{}/hooks\n/operator/guard-hooks\n",
                std::fs::canonicalize(&original_home).unwrap().display()
            )
        );
        assert_eq!(
            std::fs::read_to_string(managed.path().join("sessions").join("existing")).unwrap(),
            "session-42"
        );
        std::fs::write(managed.path().join("sessions").join("continued"), "same-id").unwrap();
        assert_eq!(
            std::fs::read_to_string(original_home.join("sessions").join("continued")).unwrap(),
            "same-id"
        );
        assert_eq!(
            std::fs::read_to_string(managed.path().join("auth-state")).unwrap(),
            "preserved"
        );
        assert_eq!(
            std::fs::read_to_string(original_home.join("config.toml")).unwrap(),
            "[models]\ndefault_reasoning_effort = \"high\"\n\
             [plugins]\npaths = [\"/operator/plugins\"]\n\
             [[marketplace.sources]]\nname = \"operator\"\ngit = \"https://example.invalid/operator\"\n\
             [mcp_servers.operator]\ncommand = \"operator-mcp\"\n"
        );
    }

    #[test]
    fn managed_mcp_persists_atomic_auth_refresh_for_exact_resume() {
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let original_home = root.path().join("grok-home");
        std::fs::create_dir_all(&original_home).unwrap();
        std::fs::write(original_home.join(AUTH_STATE_FILE), "initial-refresh-token").unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![("GROK_HOME".into(), original_home.display().to_string())];

        let fresh =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        assert!(
            !std::fs::symlink_metadata(fresh.path().join(AUTH_STATE_FILE))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(std::fs::symlink_metadata(fresh.path().join(AUTH_LOCK_FILE))
            .unwrap()
            .file_type()
            .is_symlink());

        // This is the OAuth provider's write-temp-and-rename pattern. It
        // must survive destruction of the invocation-local config home.
        let replacement = fresh.path().join("auth.json.refresh");
        std::fs::write(&replacement, "refreshed-token").unwrap();
        std::fs::rename(&replacement, fresh.path().join(AUTH_STATE_FILE)).unwrap();
        drop(fresh);
        assert_eq!(
            std::fs::read_to_string(original_home.join(AUTH_STATE_FILE)).unwrap(),
            "refreshed-token"
        );

        let resumed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        assert_eq!(
            std::fs::read_to_string(resumed.path().join(AUTH_STATE_FILE)).unwrap(),
            "refreshed-token"
        );
    }

    #[test]
    fn managed_mcp_never_restores_an_unchanged_stale_auth_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let original_home = root.path().join("grok-home");
        std::fs::create_dir_all(&original_home).unwrap();
        std::fs::write(original_home.join(AUTH_STATE_FILE), "initial-refresh-token").unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![("GROK_HOME".into(), original_home.display().to_string())];

        let older =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let refreshed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let replacement = refreshed.path().join("auth.json.refresh");
        std::fs::write(&replacement, "newer-refresh-token").unwrap();
        std::fs::rename(&replacement, refreshed.path().join(AUTH_STATE_FILE)).unwrap();
        drop(refreshed);
        assert_eq!(
            std::fs::read_to_string(original_home.join(AUTH_STATE_FILE)).unwrap(),
            "newer-refresh-token"
        );

        // The older home did not refresh. Its teardown must observe that its
        // private snapshot is unchanged and leave the newer durable token.
        drop(older);
        assert_eq!(
            std::fs::read_to_string(original_home.join(AUTH_STATE_FILE)).unwrap(),
            "newer-refresh-token"
        );
        let resumed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        assert_eq!(
            std::fs::read_to_string(resumed.path().join(AUTH_STATE_FILE)).unwrap(),
            "newer-refresh-token"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn managed_workspace_sandbox_persists_fresh_state_for_exact_resume() {
        fn seatbelt_literal(path: &std::path::Path) -> String {
            path.display()
                .to_string()
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        }

        fn seatbelt_profile(managed: &GrokMcpConfigHome) -> String {
            let sandbox: toml::Value = toml::from_str(
                &std::fs::read_to_string(managed.path().join("sandbox.toml")).unwrap(),
            )
            .unwrap();
            let mut writable = vec![managed.path().to_path_buf()];
            writable.extend(
                sandbox["profiles"][MANAGED_WORKSPACE_SANDBOX]["read_write"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|path| std::path::PathBuf::from(path.as_str().unwrap())),
            );
            let grants = writable
                .iter()
                .map(|path| {
                    format!(
                        "(allow file-write* (subpath \"{}\"))",
                        seatbelt_literal(path)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!("(version 1)\n(allow default)\n(deny file-write*)\n{grants}\n")
        }

        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let original_home = root.path().join("grok-home");
        std::fs::create_dir_all(&original_home).unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![("GROK_HOME".into(), original_home.display().to_string())];
        let session_id = "provider-issued-grok-session-42";

        let fresh =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let status = std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                &seatbelt_profile(&fresh),
                "/bin/sh",
                "-c",
                "mkdir -p \"$GROK_HOME/sessions\" && printf fresh > \"$GROK_HOME/sessions/$SESSION_ID\"",
            ])
            .env("GROK_HOME", fresh.path())
            .env("SESSION_ID", session_id)
            .status()
            .unwrap();
        assert!(status.success(), "fresh sandboxed state write failed");
        drop(fresh);

        let resumed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let status = std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                &seatbelt_profile(&resumed),
                "/bin/sh",
                "-c",
                "test \"$(cat \"$GROK_HOME/sessions/$SESSION_ID\")\" = fresh && printf resumed > \"$GROK_HOME/sessions/$SESSION_ID\"",
            ])
            .env("GROK_HOME", resumed.path())
            .env("SESSION_ID", session_id)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "sandboxed resume did not find fresh state"
        );
        assert_eq!(
            std::fs::read_to_string(original_home.join("sessions").join(session_id)).unwrap(),
            "resumed"
        );
    }

    #[test]
    fn initial_argument_shape_is_pinned() {
        let spec = test_spec(std::path::Path::new("/tmp/repo"));
        assert_eq!(
            headless_args(&spec, LaunchMode::Normal).unwrap(),
            [
                "-p",
                "inspect the repository",
                "--output-format",
                "streaming-json",
                "--model",
                "grok-4.5",
                "--reasoning-effort",
                "high",
                "--permission-mode",
                "bypassPermissions",
                "--sandbox",
                "workspace",
                "--max-turns",
                "64",
                "--verbatim",
            ]
        );
    }

    #[test]
    fn continuation_argument_shape_is_pinned() {
        let spec = test_spec(std::path::Path::new("/tmp/repo"));
        let args = resume_args("019f-session", &spec).unwrap();
        assert_eq!(
            &args[..4],
            ["--resume", "019f-session", "-p", "inspect the repository"]
        );
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "workspace"]));
        assert!(!args.iter().any(|arg| arg.contains("allowedTools")));
        assert!(!args.iter().any(|arg| arg == "--bare"));
    }

    #[test]
    fn restricted_shape_uses_native_fail_closed_controls() {
        let mut spec = test_spec(std::path::Path::new("/tmp/repo"));
        spec.max_turns = 200;
        let args = headless_args(&spec, LaunchMode::Restricted).unwrap();
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "read-only"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--permission-mode", "dontAsk"]));
        assert!(args.windows(2).any(|pair| pair == ["--max-turns", "8"]));
    }

    #[test]
    fn models_efforts_and_unsupported_safety_combinations_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut spec = test_spec(dir.path());
        spec.model = "grok-future".into();
        assert_eq!(
            headless_args(&spec, LaunchMode::Normal).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        spec.model = SUPPORTED_MODEL.into();
        spec.effort = "xhigh".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.effort = "high".into();
        spec.sandbox = "custom".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.sandbox = DEFAULT_SANDBOX.into();
        spec.permission_mode = "auto".into();
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
        spec.permission_mode = DEFAULT_PERMISSION_MODE.into();
        spec.max_turns = 0;
        assert!(headless_args(&spec, LaunchMode::Normal).is_err());
    }

    #[test]
    fn fixture_session_identity_and_terminal_success() {
        let raw = r#"{"type":"end","stopReason":"EndTurn","sessionId":"sess-1","requestId":"req-1","usage":{"input_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":3,"output_tokens":4},"total_cost_usd":0.0125}"#;
        let events = normalize_grok_line(raw);
        assert_eq!(
            events[0],
            AgentEvent::ThreadStarted {
                thread_id: "sess-1".into()
            }
        );
        assert_eq!(
            events[1],
            AgentEvent::TurnCompleted {
                usage: Some(TokenUsage {
                    input_tokens: 33,
                    uncached_input_tokens: 10,
                    cached_input_tokens: 20,
                    cache_write_input_tokens: 3,
                    output_tokens: 4,
                    ..Default::default()
                }),
                cost_usd: Some(0.0125),
            }
        );
    }

    #[test]
    fn fixture_assistant_text_and_activity() {
        assert_eq!(
            normalize_grok_line(r#"{"type":"text","data":"working"}"#),
            vec![AgentEvent::AssistantText {
                text: "working".into()
            }]
        );
        let events = normalize_grok_line(
            r#"{"type":"tool_call","toolCallId":"call-1","title":"Run","kind":"execute","toolName":"run_terminal_cmd","rawInput":{"command":"cargo test"}}"#,
        );
        assert!(matches!(
            &events[0],
            AgentEvent::Activity { kind: ActivityKind::ToolUse, summary } if summary == "run_terminal_cmd"
        ));
    }

    #[test]
    fn fixture_terminal_failure_and_incomplete_spend() {
        let raw = r#"{"type":"error","message":"authentication failed","usage":{"input_tokens":8,"output_tokens":2},"total_cost_usd":0.1,"usage_is_incomplete":true,"cost_is_partial":true}"#;
        assert_eq!(
            normalize_grok_line(raw),
            vec![AgentEvent::TurnFailed {
                message: "authentication failed".into(),
                usage: None,
                cost_usd: None,
            }]
        );
    }

    #[test]
    fn fixture_partial_cost_does_not_hide_complete_tokens_or_invent_usd() {
        let raw = r#"{"type":"end","sessionId":"sess-2","usage":{"input_tokens":8,"cache_read_input_tokens":4,"output_tokens":2},"total_cost_usd":0.1,"cost_is_partial":true}"#;
        assert_eq!(
            normalize_grok_line(raw)[1],
            AgentEvent::TurnCompleted {
                usage: Some(TokenUsage {
                    input_tokens: 12,
                    uncached_input_tokens: 8,
                    cached_input_tokens: 4,
                    output_tokens: 2,
                    ..Default::default()
                }),
                cost_usd: None,
            }
        );
    }

    #[test]
    fn fixture_end_without_session_fails_closed() {
        for raw in [
            r#"{"type":"end","stopReason":"end_turn"}"#,
            r#"{"type":"end","sessionId":"   "}"#,
        ] {
            assert!(matches!(
                normalize_grok_line(raw).as_slice(),
                [AgentEvent::TurnFailed { message, .. }] if message.contains("missing sessionId")
            ));
        }
    }

    #[test]
    fn fixture_unknown_and_malformed_lines_are_inert() {
        for raw in [
            r#"{"type":"thought","data":"private"}"#,
            r#"{"type":"tool_call_update","toolCallId":"1","status":"completed"}"#,
            r#"{"type":"future_event","extra":true}"#,
            "not-json",
            "{broken",
            "",
        ] {
            assert!(normalize_grok_line(raw).is_empty(), "raw={raw}");
        }
    }

    #[tokio::test]
    async fn raw_lines_are_returned_exactly_without_the_line_terminator() {
        let expected = r#"  {"type":"future_event","extra":true}  "#;
        let mut proc = shell_proc(&format!("printf '%s\\n' '{}'", expected)).await;
        assert_eq!(proc.next_raw_line().await.as_deref(), Some(expected));
        assert!(proc.next_raw_line().await.is_none());
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn invalid_utf8_cannot_be_repaired_into_authoritative_terminal_json() {
        let mut proc = shell_proc("printf '{\"type\":\"end\",\"sessionId\":\"\\377\"}\\n'").await;
        let raw = proc.next_raw_line().await.unwrap();
        let marker: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker["type"], "provider.stdout_invalid_utf8");
        assert_eq!(marker["invalid_sequence_bytes"], 1);
        assert!(!raw.contains('\u{fffd}'));
        assert!(normalize_grok_line(&raw).is_empty());
        assert!(proc.next_raw_line().await.is_none());
        proc.kill_and_reap().await;
    }

    async fn shell_proc(script: &str) -> GrokProc {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let process_group_id = child.id().unwrap() as libc::pid_t;
        let reader = BoundedStdout::new(child.stdout.take().unwrap());
        let diagnostics = DiagnosticBuffer::for_kind(AgentKind::Grok);
        let failures = diagnostics.failures();
        let stderr_diagnostics = diagnostics.clone();
        let stderr = child.stderr.take().unwrap();
        let stderr_task =
            tokio::spawn(async move { capture_diagnostics(stderr, stderr_diagnostics).await });
        GrokProc {
            child,
            process_group_id,
            reader,
            diagnostics,
            failures,
            stderr_task: Some(stderr_task),
            pending_terminal: None,
            terminal_rejected: false,
            stdout_complete: false,
            terminal_exit_status: None,
            worker_request: None,
            _mcp_config_home: None,
        }
    }

    async fn wait_status(proc: &mut GrokProc) -> std::process::ExitStatus {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(status) = proc.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("process did not exit")
    }

    #[tokio::test]
    async fn eof_without_terminal_event_never_fabricates_success() {
        let mut proc =
            shell_proc("printf '%s\\n' '{\"type\":\"text\",\"data\":\"partial\"}'").await;
        let raw = proc.next_raw_line().await.unwrap();
        assert!(matches!(
            normalize_grok_line(&raw).as_slice(),
            [AgentEvent::AssistantText { .. }]
        ));
        assert!(proc.next_raw_line().await.is_none());
        assert!(wait_status(&mut proc).await.success());
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn nonzero_exit_without_terminal_event_never_fabricates_failure_line() {
        let mut proc = shell_proc("exit 7").await;
        assert!(proc.next_raw_line().await.is_none());
        let status = wait_status(&mut proc).await;
        assert_eq!(status.code(), Some(7));
        proc.kill_and_reap().await;
    }

    #[tokio::test]
    async fn stdout_and_stderr_capture_are_bounded_and_process_group_is_reaped() {
        let mut proc = shell_proc(
            "head -c 1050000 /dev/zero | tr '\\000' x; echo; \
             i=0; while [ $i -lt 306 ]; do echo grok-stderr-$i >&2; i=$((i+1)); done; \
             printf '%s\\n' '{\"type\":\"text\",\"data\":\"tail\"}'; exec sleep 30",
        )
        .await;
        let pid = proc.pid().unwrap();
        let marker = tokio::time::timeout(std::time::Duration::from_secs(30), proc.next_raw_line())
            .await
            .unwrap()
            .unwrap();
        assert!(marker.contains("provider.stdout_bytes_truncated"));
        assert_eq!(
            proc.observed_pre_authoritative_failure()
                .unwrap()
                .disposition(),
            FailureDisposition::NonFailover,
            "discarding an overlong Grok record is a protocol failure, not retryable EOF"
        );
        let tail = proc.next_raw_line().await.unwrap();
        assert!(tail.contains("tail"));
        let output = proc.kill_and_reap().await;
        assert!(output.len() <= 258);
        assert!(output.iter().any(
            |line| matches!(line, CapturedOutput::StderrTruncated { dropped } if *dropped > 0)
        ));
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
    }

    #[tokio::test]
    async fn teardown_kills_stored_process_group_after_leader_is_reaped() {
        let mut proc = shell_proc("trap '' HUP; (trap '' HUP; sleep 30) & exit 0").await;
        let process_group_id = proc.process_group_id;

        let status = wait_status(&mut proc).await;
        assert!(status.success());
        assert!(
            proc.child.id().is_none(),
            "test must exercise teardown after try_wait loses the leader ID"
        );
        assert_eq!(
            unsafe { libc::killpg(process_group_id, 0) },
            0,
            "a descendant must still hold the process group and inherited pipes"
        );

        let output = tokio::time::timeout(std::time::Duration::from_secs(15), proc.kill_and_reap())
            .await
            .expect("post-exit teardown hung on descendant-held pipes");
        assert!(output.is_empty());

        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while unsafe { libc::killpg(process_group_id, 0) } == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Grok descendant process group was not reaped");
    }

    fn grok_available() -> bool {
        std::process::Command::new("grok")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn no_auth_env(root: &std::path::Path) -> Vec<(String, String)> {
        let home = root.join("home");
        let grok_home = root.join("grok-home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&grok_home).unwrap();
        vec![
            ("HOME".into(), home.display().to_string()),
            ("GROK_HOME".into(), grok_home.display().to_string()),
            ("XAI_API_KEY".into(), String::new()),
            ("RUST_LOG".into(), "off".into()),
        ]
    }

    async fn wait_for_real_cli_terminal_failure(proc: &mut GrokProc) -> Vec<String> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut lines = Vec::new();
            while lines.len() < 16 {
                let raw = proc
                    .next_raw_line()
                    .await
                    .expect("grok closed stdout without an error event");
                assert!(
                    serde_json::from_str::<serde_json::Value>(&raw).is_ok(),
                    "{raw}"
                );
                let terminal = matches!(
                    normalize_grok_line(&raw).as_slice(),
                    [AgentEvent::TurnFailed { .. }]
                );
                lines.push(raw);
                if terminal {
                    return lines;
                }
            }
            panic!("grok emitted 16 structured nonterminal lines without an auth failure");
        })
        .await
        .expect("grok auth failure did not reach the structured protocol")
    }

    #[tokio::test]
    async fn real_cli_accepts_headless_protocol_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let _guard = REAL_CLI_PROTOCOL_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        let args = headless_args(&spec, LaunchMode::Normal).unwrap();
        let mut proc = GrokProc::spawn_command(
            &spec,
            &args,
            Some(crate::serve::runner::AGENT_MCP_SERVER),
            None,
        )
        .expect("spawn grok with invocation-local MCP config");
        let _lines = wait_for_real_cli_terminal_failure(&mut proc).await;
        proc.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_mcp_grok_child_scrubs_ambient_authority_on_fresh_and_resume() {
        use std::ffi::OsString;
        use std::os::unix::fs::PermissionsExt;

        static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

        struct EnvRestore {
            name: &'static str,
            previous: Option<OsString>,
        }

        impl EnvRestore {
            fn poison(name: &'static str, value: &str) -> Self {
                let previous = std::env::var_os(name);
                std::env::set_var(name, value);
                Self { name, previous }
            }
        }

        impl Drop for EnvRestore {
            fn drop(&mut self) {
                if let Some(value) = self.previous.take() {
                    std::env::set_var(self.name, value);
                } else {
                    std::env::remove_var(self.name);
                }
            }
        }

        let _environment = ENV_LOCK.lock().await;
        let _gh = EnvRestore::poison("GH_TOKEN", "ambient-gh-token");
        let _github = EnvRestore::poison("GITHUB_TOKEN", "ambient-github-token");
        let _quorum_home = EnvRestore::poison("QUORUM_HOME", "/ambient/quorum-home");
        let _gh_config = EnvRestore::poison("GH_CONFIG_DIR", "/ambient/gh-config");

        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let capture = root.path().join("mcp-child-environment.log");
        let fake_quorum = root.path().join("quorum");
        std::fs::write(
            &fake_quorum,
            format!(
                "#!/bin/sh\n\
                 {{\n\
                   printf 'arg=%s\\n' \"$1\"\n\
                   printf 'repo=%s\\n' \"$QUORUM_REPO\"\n\
                   printf 'agent=%s\\n' \"$QUORUM_AGENT\"\n\
                   printf 'run=%s\\n' \"$QUORUM_RUN_ID\"\n\
                   printf 'endpoint=%s\\n' \"$QUORUM_AGENT_ENDPOINT\"\n\
                   printf 'gh=%s\\n' \"$GH_TOKEN\"\n\
                   printf 'github=%s\\n' \"$GITHUB_TOKEN\"\n\
                   printf 'quorum_home=%s\\n' \"$QUORUM_HOME\"\n\
                   printf 'gh_config=%s\\n' \"$GH_CONFIG_DIR\"\n\
                   printf 'home=%s\\n' \"$HOME\"\n\
                   printf 'launch-end\\n'\n\
                 }} >> '{}'\n",
                capture.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_quorum, std::fs::Permissions::from_mode(0o755)).unwrap();
        let fake_grok = root.path().join("grok");
        std::fs::write(
            &fake_grok,
            "#!/bin/sh\nquorum agent-mcp\nprintf '%s\\n' '{\"type\":\"end\",\"sessionId\":\"provider-issued-session\"}'\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_grok, std::fs::Permissions::from_mode(0o755)).unwrap();

        let original_home = root.path().join("original-home");
        let original_grok_home = root.path().join("original-grok-home");
        std::fs::create_dir_all(&original_home).unwrap();
        std::fs::create_dir_all(&original_grok_home).unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![
            (
                "PATH".into(),
                format!(
                    "{}:{}",
                    root.path().display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            ),
            ("HOME".into(), original_home.display().to_string()),
            ("GROK_HOME".into(), original_grok_home.display().to_string()),
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Worker-test".into()),
            ("QUORUM_RUN_ID".into(), "run-capability".into()),
            (
                "QUORUM_AGENT_ENDPOINT".into(),
                "/tmp/quorum-agent.sock".into(),
            ),
        ];

        for args in [
            headless_args(&spec, LaunchMode::Normal).unwrap(),
            resume_args("provider-issued-session", &spec).unwrap(),
        ] {
            let mut proc = GrokProc::spawn_command(
                &spec,
                &args,
                Some(crate::serve::runner::AGENT_MCP_SERVER),
                fake_grok.to_str(),
            )
            .unwrap();
            while proc.next_raw_line().await.is_some() {}
            proc.kill_and_reap().await;
        }

        let captured = std::fs::read_to_string(capture).unwrap();
        let launches: Vec<_> = captured.split("launch-end\n").collect();
        assert!(launches.len() >= 3, "{captured}");
        for launch in &launches[..2] {
            assert!(launch.contains("arg=agent-mcp\n"), "{launch}");
            assert!(launch.contains("repo=owner/repo\n"), "{launch}");
            assert!(launch.contains("agent=Worker-test\n"), "{launch}");
            assert!(launch.contains("run=run-capability\n"), "{launch}");
            assert!(
                launch.contains("endpoint=/tmp/quorum-agent.sock\n"),
                "{launch}"
            );
            for absent in ["gh=\n", "github=\n", "quorum_home=\n", "gh_config=\n"] {
                assert!(launch.contains(absent), "{launch}");
            }
            assert!(
                !launch.contains(&format!("home={}\n", original_home.display())),
                "managed child retained the operator home: {launch}"
            );
        }
    }

    #[test]
    fn managed_mcp_rejects_every_project_mcp_source() {
        let root = tempfile::tempdir().unwrap();
        let worktree = root.path().join("worktree");
        std::fs::create_dir(&worktree).unwrap();
        assert!(std::process::Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&worktree)
            .status()
            .unwrap()
            .success());
        let mut spec = test_spec(&worktree);
        spec.env_vars = vec![(
            "GROK_HOME".into(),
            root.path().join("grok-home").display().to_string(),
        )];

        for (path, contents) in [
            (
                worktree.join(".grok/config.toml"),
                "[mcp_servers.project]\ncommand = \"project-mcp\"",
            ),
            (
                worktree.join(".mcp.json"),
                r#"{"mcpServers":{"project":{"command":"project-mcp"}}}"#,
            ),
            (
                worktree.join(".cursor/mcp.json"),
                r#"{"mcpServers":{"project":{"command":"project-mcp"}}}"#,
            ),
        ] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, contents).unwrap();
            let error = GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER)
                .err()
                .expect("managed Grok must reject project MCP source");
            assert!(
                error.to_string().contains(&path.display().to_string()),
                "{error}"
            );
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn managed_mcp_rejects_system_mcp_and_plugin_sources() {
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = vec![(
            "GROK_HOME".into(),
            root.path().join("grok-home").display().to_string(),
        )];

        for (filename, contents) in [
            (
                "managed_config.toml",
                "[mcp_servers.operator]\ncommand = \"operator-mcp\"\n",
            ),
            (
                "requirements.toml",
                "[plugins]\npaths = [\"/operator/plugins\"]\n",
            ),
            (
                "managed_config.toml",
                "[[marketplace.sources]]\nname = \"operator\"\ngit = \"https://example.invalid/operator\"\n",
            ),
        ] {
            let system_config_dir = tempfile::tempdir_in(root.path()).unwrap();
            let source = system_config_dir.path().join(filename);
            std::fs::write(&source, contents).unwrap();
            let error = GrokMcpConfigHome::create_with_system_config_dir(
                &spec,
                crate::serve::runner::AGENT_MCP_SERVER,
                system_config_dir.path(),
            )
            .err()
            .expect("managed Grok must reject a system tool source");
            assert!(
                error.to_string().contains(&source.display().to_string()),
                "{error}"
            );
        }
    }

    #[test]
    fn real_cli_inspect_exposes_only_the_managed_mcp_layer() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.env_vars = no_auth_env(root.path());
        let source_home = grok_home_for_child(&spec).unwrap();
        let source_config = source_home.join("config.toml");
        std::fs::write(
            source_config,
            "[mcp_servers.operator]\ncommand = \"operator-mcp\"\n",
        )
        .unwrap();
        std::fs::write(
            source_home.join("managed_config.toml"),
            "[mcp_servers.managed_operator]\ncommand = \"managed-mcp\"\n",
        )
        .unwrap();
        std::fs::write(
            source_home.join("requirements.toml"),
            "[mcp_servers.required_operator]\ncommand = \"required-mcp\"\n",
        )
        .unwrap();
        let managed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let mut command = std::process::Command::new("grok");
        command
            .args(["inspect", "--json"])
            .envs(spec.env_vars.iter().map(|(key, value)| (key, value)));
        for name in ["GH_TOKEN", "GITHUB_TOKEN", "QUORUM_HOME", "GH_CONFIG_DIR"] {
            command.env_remove(name);
        }
        command
            .env("GROK_HOME", managed.path())
            .env("HOME", managed.path())
            .env("XDG_CONFIG_HOME", managed.path().join("xdg-config"))
            .env("GROK_CLAUDE_MCPS_ENABLED", "0")
            .env("GROK_CURSOR_MCPS_ENABLED", "0")
            .env("GROK_FOLDER_TRUST", "1");
        let output = command
            .current_dir(&spec.worktree)
            .stdin(Stdio::null())
            .output()
            .expect("run official Grok config inspection");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let mcp_servers = report["mcpServers"].as_array().unwrap();
        assert_eq!(mcp_servers.len(), 1, "{report}");
        let github = mcp_servers
            .iter()
            .find(|server| server["name"] == "github")
            .expect("official Grok loader omitted managed github MCP server");
        assert_eq!(github["transport"], "stdio");
        assert!(
            github["target"]
                .as_str()
                .is_some_and(|target| target.contains("quorum")),
            "{github}"
        );
    }

    #[tokio::test]
    async fn real_cli_auth_failure_is_structured_and_does_not_echo_api_key() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let _guard = REAL_CLI_PROTOCOL_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let secret = "quorum-invalid-xai-key-must-not-be-printed";
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        spec.env_vars
            .iter_mut()
            .find(|(key, _)| key == "XAI_API_KEY")
            .unwrap()
            .1 = secret.into();
        let mut proc = GrokProc::spawn(&spec, None).expect("spawn grok");
        let lines = wait_for_real_cli_terminal_failure(&mut proc).await;
        assert!(
            lines.iter().all(|line| !line.contains(secret)),
            "API key leaked in structured output"
        );
        let captured = proc.kill_and_reap().await;
        assert!(captured
            .iter()
            .all(|line| !line.session_line().contains(secret)));
    }

    #[test]
    fn real_cli_accepts_exact_resume_placement_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let worktree = tempfile::tempdir().unwrap();
        let mut spec = test_spec(worktree.path());
        spec.prompt = "noop".into();
        spec.env_vars = no_auth_env(root.path());
        let mut args = resume_args("00000000-0000-0000-0000-000000000000", &spec).unwrap();
        args.push("--no-leader".into());
        args.push("--help".into());
        let managed =
            GrokMcpConfigHome::create(&spec, crate::serve::runner::AGENT_MCP_SERVER).unwrap();
        let output = std::process::Command::new("grok")
            .args(args)
            .envs(spec.env_vars.iter().map(|(key, value)| (key, value)))
            .env("GROK_HOME", managed.path())
            .current_dir(&spec.worktree)
            .stdin(Stdio::null())
            .output()
            .expect("run grok resume parser");
        assert!(output.status.success(), "{:?}", output.status);
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("--resume"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[test]
    fn real_cli_rejects_invalid_permission_mode_without_model_spend() {
        if !grok_available() {
            eprintln!("skipped: no official grok binary on PATH");
            return;
        }
        let output = std::process::Command::new("grok")
            .args([
                "-p",
                "noop",
                "--output-format",
                "streaming-json",
                "--permission-mode",
                "definitely-invalid",
            ])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("invalid value"), "{stderr}");
    }

    #[test]
    fn synthetic_signal_status_is_not_success() {
        assert!(!std::process::ExitStatus::from_raw(libc::SIGKILL).success());
    }
}
