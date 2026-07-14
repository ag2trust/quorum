//! TOML config file for `quorum serve`. Missing file → built-in defaults.
//! Malformed or unknown keys → fail loud (exit 2).
//!
//! CLI flags override config-file values (explicit wins). The resolved config
//! and each value's source (file vs flag vs default) are logged at startup.

use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use std::path::Path;

/// Deserializable TOML config for `quorum serve`. Every field is optional —
/// missing fields fall back to built-in defaults; CLI flags override everything.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeFileConfig {
    pub cap: Option<usize>,
    pub repo_dir: Option<String>,
    pub worktree_base: Option<String>,
    pub names_file: Option<String>,
    pub agent_bin: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub merge_token_file: Option<String>,
    pub no_bare_agent: Option<bool>,
    pub max_turn_tokens: Option<i64>,
    pub max_task_tokens: Option<i64>,
    pub max_turn_cost_usd: Option<f64>,
    pub max_task_cost_usd: Option<f64>,
    pub max_turn_wall_secs: Option<u64>,
    pub max_task_wall_secs: Option<u64>,
    pub idle_timeout_secs: Option<u64>,
    pub allowed_tools: Option<String>,
    pub log_dir: Option<String>,
    pub self_update_drain: Option<bool>,
    pub drain_timeout_secs: Option<u64>,
    pub self_repo: Option<String>,
    pub sha_poll_interval_secs: Option<u64>,
    pub repo: Option<String>,
    pub base_branch: Option<String>,
    pub merge_checks_timeout_secs: Option<u64>,
    pub merge_checks_poll_secs: Option<u64>,
    pub required_jobs: Option<Vec<String>>,
    pub master_ci_gate: Option<bool>,
    pub master_ci_timeout_secs: Option<u64>,
    pub doctor_enabled: Option<bool>,
    // ponytail: R2 review-audit knobs — defaults in ServeConfig resolution
    pub r2_enabled: Option<bool>,
    pub r2_target_per_stratum: Option<i64>,
    pub r2_steady_state_p: Option<f64>,
    pub r2_blocking: Option<bool>,
}

/// Load serve config from `path`. Malformed / unknown keys → exit 2.
/// When `explicit` is true (user passed --config), missing file → exit 2.
/// When false (auto-discovered default path), missing file → built-in defaults.
pub fn load(path: &Path, explicit: bool) -> Result<ServeFileConfig> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let cfg: ServeFileConfig = toml::from_str(&s).map_err(|e| {
                QuorumError::Usage(format!("bad serve config {}: {e}", path.display()))
            })?;
            Ok(cfg)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if explicit {
                Err(QuorumError::Usage(format!(
                    "serve config not found: {}",
                    path.display()
                )))
            } else {
                Ok(ServeFileConfig::default())
            }
        }
        Err(e) => Err(QuorumError::Io(format!(
            "cannot read serve config {}: {e}",
            path.display()
        ))),
    }
}

/// Tracks where each resolved config value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Default,
    File,
    Flag,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Default => write!(f, "default"),
            Source::File => write!(f, "file"),
            Source::Flag => write!(f, "flag"),
        }
    }
}

/// A resolved value with its source.
#[derive(Debug, Clone)]
pub struct Sourced<T> {
    pub value: T,
    pub source: Source,
}

impl<T: std::fmt::Display> std::fmt::Display for Sourced<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.value, self.source)
    }
}

/// Resolve: flag > file > default.
pub fn resolve_val<T: Copy>(flag: Option<T>, file: Option<T>, default: T) -> Sourced<T> {
    if let Some(v) = flag {
        return Sourced {
            value: v,
            source: Source::Flag,
        };
    }
    if let Some(v) = file {
        return Sourced {
            value: v,
            source: Source::File,
        };
    }
    Sourced {
        value: default,
        source: Source::Default,
    }
}

pub fn resolve_str(flag: Option<&str>, file: Option<&str>, default: &str) -> Sourced<String> {
    if let Some(v) = flag {
        return Sourced {
            value: v.to_string(),
            source: Source::Flag,
        };
    }
    if let Some(v) = file {
        return Sourced {
            value: v.to_string(),
            source: Source::File,
        };
    }
    Sourced {
        value: default.to_string(),
        source: Source::Default,
    }
}

pub fn resolve_opt<T: Copy>(flag: Option<T>, file: Option<T>) -> Sourced<Option<T>> {
    if flag.is_some() {
        return Sourced {
            value: flag,
            source: Source::Flag,
        };
    }
    if file.is_some() {
        return Sourced {
            value: file,
            source: Source::File,
        };
    }
    Sourced {
        value: None,
        source: Source::Default,
    }
}

pub fn resolve_opt_str(flag: Option<&str>, file: Option<&str>) -> Sourced<Option<String>> {
    if let Some(v) = flag {
        return Sourced {
            value: Some(v.to_string()),
            source: Source::Flag,
        };
    }
    if let Some(v) = file {
        return Sourced {
            value: Some(v.to_string()),
            source: Source::File,
        };
    }
    Sourced {
        value: None,
        source: Source::Default,
    }
}

pub fn resolve_bool(flag: bool, file: Option<bool>, default: bool) -> Sourced<bool> {
    if flag {
        return Sourced {
            value: true,
            source: Source::Flag,
        };
    }
    if let Some(v) = file {
        return Sourced {
            value: v,
            source: Source::File,
        };
    }
    Sourced {
        value: default,
        source: Source::Default,
    }
}

/// All resolved values needed for the startup banner.
#[allow(clippy::struct_field_names)]
pub struct BannerData<'a> {
    pub config_path: Option<&'a str>,
    pub repo: &'a Sourced<String>,
    pub repo_dir: &'a Sourced<String>,
    pub worktree_base: &'a Sourced<String>,
    pub base_branch: &'a Sourced<String>,
    pub cap: &'a Sourced<usize>,
    pub model: &'a Sourced<String>,
    pub effort: &'a Sourced<String>,
    pub log_dir: &'a Sourced<Option<String>>,
    pub no_bare_agent: &'a Sourced<bool>,
    pub self_update_drain: &'a Sourced<bool>,
    pub drain_timeout_secs: &'a Sourced<u64>,
    pub max_turn_wall_secs: &'a Sourced<Option<u64>>,
    pub max_task_wall_secs: &'a Sourced<Option<u64>>,
    pub idle_timeout_secs: &'a Sourced<Option<u64>>,
    pub max_turn_tokens: &'a Sourced<Option<i64>>,
    pub max_task_tokens: &'a Sourced<Option<i64>>,
    pub max_turn_cost_usd: &'a Sourced<Option<f64>>,
    pub max_task_cost_usd: &'a Sourced<Option<f64>>,
    pub merge_checks_timeout_secs: &'a Sourced<u64>,
    pub required_jobs: &'a [String],
    pub master_ci_gate: &'a Sourced<bool>,
    pub master_ci_timeout_secs: &'a Sourced<u64>,
    pub doctor_enabled: &'a Sourced<bool>,
}

/// Format the startup banner showing resolved config + sources.
pub fn banner(d: &BannerData<'_>) -> String {
    let mut lines = Vec::new();
    lines.push("─── resolved serve config ───".to_string());
    if let Some(p) = d.config_path {
        lines.push(format!("  config file:               {p}"));
    } else {
        lines.push("  config file:               (none)".to_string());
    }
    lines.push(format!("  repo:                      {}", d.repo));
    lines.push(format!("  repo_dir:                  {}", d.repo_dir));
    lines.push(format!("  worktree_base:             {}", d.worktree_base));
    lines.push(format!("  base_branch:               {}", d.base_branch));
    lines.push(format!("  cap:                       {}", d.cap));
    lines.push(format!("  model:                     {}", d.model));
    lines.push(format!("  effort:                    {}", d.effort));
    match &d.log_dir.value {
        Some(v) => lines.push(format!(
            "  log_dir:                   {} ({})",
            v, d.log_dir.source
        )),
        None => lines.push(format!(
            "  log_dir:                   (auto) ({})",
            d.log_dir.source
        )),
    }
    lines.push(format!("  no_bare_agent:             {}", d.no_bare_agent));
    lines.push(format!(
        "  self_update_drain:         {}",
        d.self_update_drain
    ));
    lines.push(format!(
        "  drain_timeout_secs:        {}",
        d.drain_timeout_secs
    ));
    fn opt_u64(s: &Sourced<Option<u64>>) -> String {
        match s.value {
            Some(v) => format!("{v} ({src})", src = s.source),
            None => format!("unlimited ({src})", src = s.source),
        }
    }
    fn opt_i64(s: &Sourced<Option<i64>>) -> String {
        match s.value {
            Some(v) => format!("{v} ({src})", src = s.source),
            None => format!("unlimited ({src})", src = s.source),
        }
    }
    fn opt_f64(s: &Sourced<Option<f64>>) -> String {
        match s.value {
            Some(v) => format!("{v} ({src})", src = s.source),
            None => format!("unlimited ({src})", src = s.source),
        }
    }
    lines.push(format!(
        "  max_turn_wall_secs:        {}",
        opt_u64(d.max_turn_wall_secs)
    ));
    lines.push(format!(
        "  max_task_wall_secs:        {}",
        opt_u64(d.max_task_wall_secs)
    ));
    lines.push(format!(
        "  idle_timeout_secs:         {}",
        match d.idle_timeout_secs.value {
            Some(v) => format!("{v} ({src})", src = d.idle_timeout_secs.source),
            None => format!("300 ({src}, default)", src = d.idle_timeout_secs.source),
        }
    ));
    lines.push(format!(
        "  max_turn_tokens:           {}",
        opt_i64(d.max_turn_tokens)
    ));
    lines.push(format!(
        "  max_task_tokens:           {}",
        opt_i64(d.max_task_tokens)
    ));
    lines.push(format!(
        "  max_turn_cost_usd:         {}",
        opt_f64(d.max_turn_cost_usd)
    ));
    lines.push(format!(
        "  max_task_cost_usd:         {}",
        opt_f64(d.max_task_cost_usd)
    ));
    lines.push(format!(
        "  merge_checks_timeout_secs: {}",
        d.merge_checks_timeout_secs
    ));
    if d.required_jobs.is_empty() {
        lines.push("  required_jobs:             (none)".to_string());
    } else {
        lines.push(format!(
            "  required_jobs:             [{}]",
            d.required_jobs.join(", ")
        ));
    }
    lines.push(format!("  master_ci_gate:            {}", d.master_ci_gate));
    lines.push(format!(
        "  master_ci_timeout_secs:    {}",
        d.master_ci_timeout_secs
    ));
    lines.push(format!("  doctor_enabled:            {}", d.doctor_enabled));
    lines.push("─────────────────────────────".to_string());
    lines.join("\n")
}

/// Default config file path for a given repo: `~/.quorum/serve/<owner>__<repo>.toml`
pub fn default_config_path(repo: &str) -> Result<std::path::PathBuf> {
    let slug = repo.replace('/', "__");
    Ok(crate::paths::home_dir()?
        .join("serve")
        .join(format!("{slug}.toml")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_implicit_returns_defaults() {
        let cfg = load(Path::new("/nonexistent/path/serve.toml"), false).unwrap();
        assert!(cfg.cap.is_none());
        assert!(cfg.model.is_none());
    }

    #[test]
    fn load_missing_explicit_fails_loud() {
        let err = load(Path::new("/nonexistent/path/serve.toml"), true).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "error should say not found: {msg}"
        );
    }

    #[test]
    fn load_rejects_unknown_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "bogus_key = 42\n").unwrap();
        let err = load(&path, false).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_key"),
            "error should name the bad key: {msg}"
        );
    }

    #[test]
    fn load_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            r#"
cap = 8
model = "opus-48"
effort = "max"
max_turn_wall_secs = 2700
max_task_wall_secs = 14400
repo = "ag2trust/quorum"
repo_dir = "/home/user/dev/quorum"
worktree_base = "/home/user/.quorum/serve/quorum/worktrees"
log_dir = "/home/user/.quorum/serve/quorum/logs"
"#,
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.cap, Some(8));
        assert_eq!(cfg.model.as_deref(), Some("opus-48"));
        assert_eq!(cfg.max_turn_wall_secs, Some(2700));
        assert!(cfg.doctor_enabled.is_none());
    }

    #[test]
    fn load_doctor_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            "repo = \"test/repo\"\nrepo_dir = \"/tmp\"\nworktree_base = \"/tmp/wt\"\ndoctor_enabled = true\n",
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.doctor_enabled, Some(true));
    }

    #[test]
    fn resolve_flag_beats_file() {
        let s = resolve_val(Some(2usize), Some(8), 4);
        assert_eq!(s.value, 2);
        assert_eq!(s.source, Source::Flag);
    }

    #[test]
    fn resolve_file_beats_default() {
        let s = resolve_val::<usize>(None, Some(8), 4);
        assert_eq!(s.value, 8);
        assert_eq!(s.source, Source::File);
    }

    #[test]
    fn resolve_falls_to_default() {
        let s = resolve_val::<usize>(None, None, 4);
        assert_eq!(s.value, 4);
        assert_eq!(s.source, Source::Default);
    }

    #[test]
    fn default_serve_scaffold_parses_cleanly() {
        // The scaffold written by `quorum init` is all-comments — it must parse as
        // empty defaults (no unknown keys, no syntax errors).
        let cfg: ServeFileConfig = toml::from_str(crate::DEFAULT_SERVE_TOML).unwrap();
        assert!(cfg.cap.is_none());
        assert!(cfg.r2_enabled.is_none());
        assert!(cfg.model.is_none());
    }

    #[test]
    fn banner_shows_config_path() {
        let b = banner(&BannerData {
            config_path: Some("/path/to/config.toml"),
            repo: &Sourced {
                value: "test/repo".into(),
                source: Source::Flag,
            },
            repo_dir: &Sourced {
                value: "/repo".into(),
                source: Source::Flag,
            },
            worktree_base: &Sourced {
                value: "/wt".into(),
                source: Source::File,
            },
            base_branch: &Sourced {
                value: "main".into(),
                source: Source::Default,
            },
            cap: &Sourced {
                value: 8,
                source: Source::File,
            },
            model: &Sourced {
                value: "sonnet".into(),
                source: Source::Default,
            },
            effort: &Sourced {
                value: "high".into(),
                source: Source::Default,
            },
            log_dir: &Sourced {
                value: None,
                source: Source::Default,
            },
            no_bare_agent: &Sourced {
                value: false,
                source: Source::Default,
            },
            self_update_drain: &Sourced {
                value: false,
                source: Source::Default,
            },
            drain_timeout_secs: &Sourced {
                value: 900,
                source: Source::Default,
            },
            max_turn_wall_secs: &Sourced {
                value: Some(2700),
                source: Source::File,
            },
            max_task_wall_secs: &Sourced {
                value: None,
                source: Source::Default,
            },
            idle_timeout_secs: &Sourced {
                value: None,
                source: Source::Default,
            },
            max_turn_tokens: &Sourced {
                value: None,
                source: Source::Default,
            },
            max_task_tokens: &Sourced {
                value: None,
                source: Source::Default,
            },
            max_turn_cost_usd: &Sourced {
                value: None,
                source: Source::Default,
            },
            max_task_cost_usd: &Sourced {
                value: None,
                source: Source::Default,
            },
            merge_checks_timeout_secs: &Sourced {
                value: 900,
                source: Source::Default,
            },
            required_jobs: &[],
            master_ci_gate: &Sourced {
                value: false,
                source: Source::Default,
            },
            master_ci_timeout_secs: &Sourced {
                value: 300,
                source: Source::Default,
            },
            doctor_enabled: &Sourced {
                value: false,
                source: Source::Default,
            },
        });
        assert!(
            b.contains("config file:               /path/to/config.toml"),
            "{b}"
        );
        assert!(b.contains("8 (file)"), "cap should show file source: {b}");
        assert!(
            b.contains("2700 (file)"),
            "wall secs should show file source: {b}"
        );
        assert!(b.contains("(default)"), "defaults should be labeled: {b}");
    }
}
