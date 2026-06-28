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
    /// Single source of truth for each default lives in the core module that USES it
    /// (`agents`/`feed`/`tasks`); this `Default` impl + the `DEFAULT_TOML` string below are
    /// thin re-exports. The `default_toml_matches_default_config` test pins all three to stay
    /// consistent so a value change in one place can never silently drift from the others.
    fn default() -> Self {
        Self {
            online_window_secs: quorum_core::agents::ONLINE_WINDOW_SECS,
            message_ttl_secs: quorum_core::feed::DEFAULT_MESSAGE_TTL_SECS,
            task_lease_ttl_secs: quorum_core::tasks::DEFAULT_LEASE_TTL_SECS,
            read_limit: quorum_core::feed::DEFAULT_READ_LIMIT,
        }
    }
}

/// The default config file contents, written by `quorum init`. The values MUST match
/// `Config::default()` — verified by `default_toml_matches_default_config`.
pub const DEFAULT_TOML: &str = "\
# Quorum config. Delete any line to use its built-in default.
online_window_secs   = 900        # agent considered online if active within 15 min
message_ttl_secs     = 172800     # 48h
task_lease_ttl_secs  = 3600       # 1h task-claim lease; assignee renews on long work
read_limit           = 100        # default page size for read/peek
";

/// Load config from `path`. Missing file → defaults; malformed → fail loud (exit 3).
pub fn load(path: &Path) -> Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let cfg: Config = toml::from_str(&s).map_err(|e| {
                QuorumError::Io(format!("malformed config {}: {e}", path.display()))
            })?;
            validate(&cfg, path)?;
            Ok(cfg)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(QuorumError::Io(e.to_string())),
    }
}

/// Reject out-of-range TTL defaults. `message_ttl_secs` / `task_lease_ttl_secs` feed the write
/// sites directly when `--ttl` is omitted (main.rs `None =>` arms), bypassing `parse_ttl`'s
/// clamp — so without this, a huge config value reintroduces the #22 overflow (`now + ttl`
/// wraps into the past → two holders of one target). Bounding them to the same `MAX_TTL_SECS`
/// ceiling makes EVERY TTL input path overflow-safe. A bad value is malformed config → exit 3,
/// consistent with the toml-parse failure above.
fn validate(cfg: &Config, path: &Path) -> Result<()> {
    for (field, v) in [
        ("message_ttl_secs", cfg.message_ttl_secs),
        ("task_lease_ttl_secs", cfg.task_lease_ttl_secs),
    ] {
        if v <= 0 || v > crate::MAX_TTL_SECS {
            return Err(QuorumError::Io(format!(
                "malformed config {}: {field} must be 1..={} (got {v})",
                path.display(),
                crate::MAX_TTL_SECS
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_overflow_ttl() {
        // A huge config TTL (the bypass vector behind #22) is rejected as malformed → exit 3.
        let cfg = Config {
            task_lease_ttl_secs: i64::MAX,
            ..Default::default()
        };
        assert_eq!(validate(&cfg, Path::new("cfg")).unwrap_err().exit_code(), 3);

        let cfg = Config {
            message_ttl_secs: crate::MAX_TTL_SECS + 1,
            ..Default::default()
        };
        assert!(validate(&cfg, Path::new("cfg")).is_err());

        // Non-positive is also malformed.
        let cfg = Config {
            message_ttl_secs: 0,
            ..Default::default()
        };
        assert!(validate(&cfg, Path::new("cfg")).is_err());
    }

    #[test]
    fn default_toml_matches_default_config() {
        // Pins the three sources of every default — the core constant (e.g.
        // `quorum_core::agents::ONLINE_WINDOW_SECS`), `Config::default()`, and the
        // `DEFAULT_TOML` string written by `quorum init`. If a value changes in one place
        // without the others, this fails and forces the author to update them together.
        let parsed: Config = toml::from_str(DEFAULT_TOML).unwrap();
        let d = Config::default();
        assert_eq!(parsed.online_window_secs, d.online_window_secs);
        assert_eq!(parsed.message_ttl_secs, d.message_ttl_secs);
        assert_eq!(parsed.task_lease_ttl_secs, d.task_lease_ttl_secs);
        assert_eq!(parsed.read_limit, d.read_limit);
    }

    #[test]
    fn validate_accepts_defaults_and_ceiling() {
        assert!(validate(&Config::default(), Path::new("cfg")).is_ok());
        let cfg = Config {
            message_ttl_secs: crate::MAX_TTL_SECS,
            task_lease_ttl_secs: crate::MAX_TTL_SECS,
            ..Default::default()
        };
        assert!(validate(&cfg, Path::new("cfg")).is_ok());
    }
}
