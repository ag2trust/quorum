//! Filesystem locations. `QUORUM_HOME` overrides the default `~/.quorum/` (used by tests
//! and power users); otherwise we resolve the real home directory.

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

/// Path to the SQLite database file.
pub fn db_path() -> Result<PathBuf> {
    Ok(home_dir()?.join("quorum.db"))
}

/// Path to the optional config file.
pub fn config_path() -> Result<PathBuf> {
    Ok(home_dir()?.join("config.toml"))
}

/// Create the home directory if absent; returns its path.
pub fn ensure_home() -> Result<PathBuf> {
    let h = home_dir()?;
    std::fs::create_dir_all(&h).map_err(|e| QuorumError::Io(e.to_string()))?;
    Ok(h)
}
