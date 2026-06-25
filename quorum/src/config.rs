//! Optional config at `~/.quorum/config.toml`. Missing → built-in defaults. Malformed →
//! fail loud (exit 3), per the fail-safe principle.

use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// An agent is "online" if it acted within this many seconds.
    pub online_window_secs: i64,
    /// Default message TTL when `--ttl` is omitted.
    pub message_ttl_secs: i64,
    /// Default task-claim lease TTL when `--ttl` is omitted. The assignee renews on long work;
    /// a lapsed lease lets the reaper return the task to `open`.
    pub task_lease_ttl_secs: i64,
    /// Default page size for read/peek.
    pub read_limit: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            online_window_secs: 300,
            message_ttl_secs: 48 * 3600,
            task_lease_ttl_secs: 3600,
            read_limit: 100,
        }
    }
}

/// The default config file contents, written by `quorum init`.
pub const DEFAULT_TOML: &str = "\
# Quorum config. Delete any line to use its built-in default.
online_window_secs   = 300        # agent considered online if active within 5 min
message_ttl_secs     = 172800     # 48h
task_lease_ttl_secs  = 3600       # 1h task-claim lease; assignee renews on long work
read_limit           = 100        # default page size for read/peek
";

/// Load config from `path`. Missing file → defaults; malformed → fail loud (exit 3).
pub fn load(path: &Path) -> Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(s) => toml::from_str(&s)
            .map_err(|e| QuorumError::Io(format!("malformed config {}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(QuorumError::Io(e.to_string())),
    }
}
