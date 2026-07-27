//! TOML config file for `quorum serve`. Missing file → built-in defaults.
//! Malformed or unknown keys → fail loud (exit 2).
//!
//! CLI flags override config-file values (explicit wins). The resolved config
//! and each value's source (file vs flag vs default) are logged at startup.

use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use std::path::Path;

/// Which CLI runner the daemon uses for all spawned agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerKind {
    Claude,
    Codex,
}

impl std::fmt::Display for RunnerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claude => write!(f, "claude"),
            Self::Codex => write!(f, "codex"),
        }
    }
}

impl RunnerKind {
    pub fn from_str_opt(s: Option<&str>) -> Result<Self> {
        match s {
            None | Some("claude") => Ok(Self::Claude),
            Some("codex") => Ok(Self::Codex),
            Some(other) => Err(QuorumError::Usage(format!(
                "bad agent value: \"{other}\" (expected \"claude\" or \"codex\")"
            ))),
        }
    }
}

/// Deserializable TOML config for `quorum serve`. Every field is optional —
/// missing fields fall back to built-in defaults; CLI flags override everything.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeFileConfig {
    /// Optional provider-wide runner selection. Unlike the legacy `agent`
    /// setting, this also constrains every role-specific model.
    pub provider: Option<String>,
    pub agent: Option<String>,
    pub cap: Option<usize>,
    pub repo_dir: Option<String>,
    pub worktree_base: Option<String>,
    pub names_file: Option<String>,
    pub agent_bin: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub worker_model: Option<String>,
    pub worker_effort: Option<String>,
    pub review_model: Option<String>,
    pub review_effort: Option<String>,
    pub classifier_model: Option<String>,
    pub classifier_effort: Option<String>,
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
    // R2 is mandatory (#159) — sampling config removed. Fields kept for TOML compat.
    #[allow(dead_code)]
    pub r2_enabled: Option<bool>,
    #[allow(dead_code)]
    pub r2_target_per_stratum: Option<i64>,
    #[allow(dead_code)]
    pub r2_steady_state_p: Option<f64>,
    /// Per-complexity suggested model/effort (keys "1".."5", values "tier/effort").
    pub suggested_models: Option<std::collections::HashMap<String, String>>,
    /// #172: minimum worker model tier floor ("sonnet-5"|"opus-46"|"opus-47"|"opus-48").
    /// A worker resolving below this is bumped up to it at spawn. None = no floor.
    pub min_model: Option<String>,
    /// #172: minimum worker effort floor ("medium"|"high"). None = no floor.
    pub min_effort: Option<String>,
    /// Runner-specific Codex configuration.
    pub codex: Option<CodexFileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleConfig {
    pub provider: RunnerKind,
    pub provider_explicit: bool,
    pub worker_model: String,
    pub worker_effort: String,
    pub review_model: String,
    pub review_effort: String,
    pub classifier_model: String,
    pub classifier_effort: String,
}

pub fn resolve_roles(
    file: &ServeFileConfig,
    cli_agent: Option<&str>,
    legacy_model: &str,
    legacy_effort: &str,
) -> Result<RoleConfig> {
    if let (Some(agent), Some(provider)) = (file.agent.as_deref(), file.provider.as_deref()) {
        if agent != provider {
            return Err(QuorumError::Usage(format!(
                "conflicting agent=\"{agent}\" and provider=\"{provider}\""
            )));
        }
    }
    if let (Some(agent), Some(provider)) = (cli_agent, file.provider.as_deref()) {
        if agent != provider {
            return Err(QuorumError::Usage(format!(
                "conflicting --agent \"{agent}\" and provider=\"{provider}\""
            )));
        }
    }

    let provider_name = file
        .provider
        .as_deref()
        .or(cli_agent)
        .or(file.agent.as_deref());
    let provider = RunnerKind::from_str_opt(provider_name)?;
    let provider_explicit = file.provider.is_some();

    let (
        worker_default_model,
        worker_default_effort,
        review_default_model,
        review_default_effort,
        classifier_default_model,
        classifier_default_effort,
    ) = if provider_explicit && provider == RunnerKind::Codex {
        (
            "gpt-5.6-terra",
            "medium",
            "gpt-5.6-terra",
            "high",
            "gpt-5.6-terra",
            "medium",
        )
    } else {
        (
            legacy_model,
            legacy_effort,
            legacy_model,
            legacy_effort,
            "claude-haiku-4-5-20251001",
            "low",
        )
    };

    let roles = RoleConfig {
        provider,
        provider_explicit,
        worker_model: file
            .worker_model
            .clone()
            .unwrap_or_else(|| worker_default_model.into()),
        worker_effort: file
            .worker_effort
            .clone()
            .unwrap_or_else(|| worker_default_effort.into()),
        review_model: file
            .review_model
            .clone()
            .unwrap_or_else(|| review_default_model.into()),
        review_effort: file
            .review_effort
            .clone()
            .unwrap_or_else(|| review_default_effort.into()),
        classifier_model: file
            .classifier_model
            .clone()
            .unwrap_or_else(|| classifier_default_model.into()),
        classifier_effort: file
            .classifier_effort
            .clone()
            .unwrap_or_else(|| classifier_default_effort.into()),
    };

    let role_models_explicit = file.worker_model.is_some()
        || file.review_model.is_some()
        || file.classifier_model.is_some();
    if provider_explicit || role_models_explicit {
        for (role, model) in [
            ("worker", roles.worker_model.as_str()),
            ("review", roles.review_model.as_str()),
            ("classifier", roles.classifier_model.as_str()),
        ] {
            let actual =
                crate::serve::runner::AgentKind::for_model(model).map_err(QuorumError::Usage)?;
            let expected = match provider {
                RunnerKind::Claude => crate::serve::runner::AgentKind::Claude,
                RunnerKind::Codex => crate::serve::runner::AgentKind::Codex,
            };
            if actual != expected {
                return Err(QuorumError::Usage(format!(
                    "{role}_model \"{model}\" does not match provider=\"{provider}\""
                )));
            }
        }
    }
    Ok(roles)
}

/// `[codex]` section in serve config.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexFileConfig {
    /// Sandbox mode for Codex workers (default: "danger-full-access").
    pub sandbox: Option<String>,
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

/// Validate explicit per-complexity routing overrides. The accepted tier
/// vocabulary is owned by `quorum_core::model_tiers`, shared with task labels.
/// Only Quorum's supported medium/high efforts are valid.
pub fn validate_suggested_models(
    suggested_models: &std::collections::HashMap<String, String>,
) -> Result<()> {
    for (level, selection) in suggested_models {
        let valid_level = matches!(level.as_str(), "1" | "2" | "3" | "4" | "5");
        let valid_selection = selection.split_once('/').is_some_and(|(tier, effort)| {
            quorum_core::model_tiers::model_id_for_tier(tier).is_some()
                && matches!(effort, "medium" | "high")
        });
        if !valid_level || !valid_selection {
            return Err(QuorumError::Usage(format!(
                "bad suggested_models entry: {level} = \"{selection}\" \
                 (expected key 1-5, value supported-tier/medium|high; tiers: {})",
                quorum_core::model_tiers::known_tiers(),
            )));
        }
    }
    Ok(())
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
    pub agent: RunnerKind,
    pub repo: &'a Sourced<String>,
    pub repo_dir: &'a Sourced<String>,
    pub worktree_base: &'a Sourced<String>,
    pub base_branch: &'a Sourced<String>,
    pub cap: &'a Sourced<usize>,
    pub model: &'a Sourced<String>,
    pub effort: &'a Sourced<String>,
    pub review_model: &'a str,
    pub review_effort: &'a str,
    pub classifier_model: &'a str,
    pub classifier_effort: &'a str,
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
    /// #172: worker model/effort floor (full model id + effort), or None = off.
    pub min_model: Option<&'a str>,
    pub min_effort: Option<&'a str>,
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
    lines.push(format!("  agent:                     {}", d.agent));
    lines.push(format!("  repo:                      {}", d.repo));
    lines.push(format!("  repo_dir:                  {}", d.repo_dir));
    lines.push(format!("  worktree_base:             {}", d.worktree_base));
    lines.push(format!("  base_branch:               {}", d.base_branch));
    lines.push(format!("  cap:                       {}", d.cap));
    lines.push(format!("  model:                     {}", d.model));
    lines.push(format!("  effort:                    {}", d.effort));
    lines.push(format!("  review_model:              {}", d.review_model));
    lines.push(format!("  review_effort:             {}", d.review_effort));
    lines.push(format!(
        "  classifier_model:          {}",
        d.classifier_model
    ));
    lines.push(format!(
        "  classifier_effort:         {}",
        d.classifier_effort
    ));
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
    lines.push(format!(
        "  min_model:                 {}",
        d.min_model.unwrap_or("(none)")
    ));
    lines.push(format!(
        "  min_effort:                {}",
        d.min_effort.unwrap_or("(none)")
    ));
    lines.push("─────────────────────────────".to_string());
    lines.join("\n")
}

/// #172: validate + convert the worker model/effort floor from config strings.
/// `min_model` tier → full model id; `min_effort` must be "medium"|"high".
/// Bad values → Usage error (exit 2), consistent with the rest of serve config.
/// Returns (model_id, effort); either is None when its input is None (no floor).
pub fn resolve_floor(
    min_model: Option<&str>,
    min_effort: Option<&str>,
) -> Result<(Option<String>, Option<String>)> {
    let model = match min_model {
        Some(tier) => Some(
            quorum_core::model_tiers::claude_model_id_for_tier(tier)
                .ok_or_else(|| {
                    QuorumError::Usage(format!(
                        "bad min_model: \"{tier}\" (expected Claude tier: {})",
                        quorum_core::model_tiers::known_claude_tiers(),
                    ))
                })?
                .to_string(),
        ),
        None => None,
    };
    let effort = match min_effort {
        Some(e) if e == "medium" || e == "high" => Some(e.to_string()),
        Some(e) => {
            return Err(QuorumError::Usage(format!(
                "bad min_effort: \"{e}\" (expected medium|high)"
            )))
        }
        None => None,
    };
    Ok((model, effort))
}

/// `min_model` is a Claude tier floor. Strict Codex mode cannot apply it
/// without silently switching providers, so reject that configuration at
/// startup instead of poisoning every worker task.
pub fn validate_provider_floor(
    kind: RunnerKind,
    provider_explicit: bool,
    min_model: Option<&str>,
) -> Result<()> {
    if provider_explicit && kind == RunnerKind::Codex && min_model.is_some() {
        return Err(QuorumError::Usage(
            "provider=\"codex\" cannot use min_model — min_model accepts Claude tiers only".into(),
        ));
    }
    Ok(())
}

/// Validate that Codex runner is not combined with USD safety limits.
/// Codex does not expose reliable per-turn USD cost; fabricating it would be unsafe.
pub fn validate_codex_limits(
    kind: RunnerKind,
    max_turn_cost_usd: Option<f64>,
    max_task_cost_usd: Option<f64>,
) -> Result<()> {
    if kind == RunnerKind::Codex {
        if let Some(v) = max_turn_cost_usd {
            return Err(QuorumError::Usage(format!(
                "agent=codex cannot use max_turn_cost_usd ({v}) — \
                 Codex does not expose per-turn USD cost; use token or wall-clock limits"
            )));
        }
        if let Some(v) = max_task_cost_usd {
            return Err(QuorumError::Usage(format!(
                "agent=codex cannot use max_task_cost_usd ({v}) — \
                 Codex does not expose per-turn USD cost; use token or wall-clock limits"
            )));
        }
    }
    Ok(())
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
    fn explicit_codex_provider_gets_role_defaults() {
        let cfg: ServeFileConfig = toml::from_str("provider = \"codex\"\n").unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.provider, RunnerKind::Codex);
        assert!(roles.provider_explicit);
        assert_eq!(
            (
                roles.worker_model.as_str(),
                roles.worker_effort.as_str(),
                roles.review_model.as_str(),
                roles.review_effort.as_str(),
                roles.classifier_model.as_str(),
                roles.classifier_effort.as_str(),
            ),
            (
                "gpt-5.6-terra",
                "medium",
                "gpt-5.6-terra",
                "high",
                "gpt-5.6-terra",
                "medium",
            )
        );
    }

    #[test]
    fn absent_provider_preserves_legacy_defaults() {
        let cfg = ServeFileConfig::default();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.provider, RunnerKind::Claude);
        assert!(!roles.provider_explicit);
        assert_eq!(roles.worker_model, "sonnet");
        assert_eq!(roles.review_model, "sonnet");
        assert_eq!(roles.classifier_model, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn explicit_provider_rejects_role_model_mismatch() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\nreview_model = \"claude-opus-4-8\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("review_model"), "{err}");
    }

    #[test]
    fn explicit_provider_rejects_unknown_role_model() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\nworker_model = \"mystery\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("unknown model"), "{err}");
    }

    #[test]
    fn conflicting_legacy_agent_and_provider_fail() {
        let cfg: ServeFileConfig =
            toml::from_str("agent = \"claude\"\nprovider = \"codex\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("conflicting"), "{err}");
    }

    #[test]
    fn load_suggested_models_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            r#"
repo = "test/repo"
repo_dir = "/tmp"
worktree_base = "/tmp/wt"

[suggested_models]
"3" = "opus-48/high"
"5" = "opus-47/medium"
"#,
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        let sm = cfg.suggested_models.unwrap();
        assert_eq!(sm.get("3").unwrap(), "opus-48/high");
        assert_eq!(sm.get("5").unwrap(), "opus-47/medium");
    }

    #[test]
    fn suggested_models_accepts_every_closed_tier_at_supported_efforts() {
        let mut selections = std::collections::HashMap::new();
        for (index, tier) in quorum_core::model_tiers::MODEL_TIERS.iter().enumerate() {
            selections.insert(
                ((index % 5) + 1).to_string(),
                format!(
                    "{}/{}",
                    tier.tier,
                    if index % 2 == 0 { "medium" } else { "high" }
                ),
            );
            assert!(validate_suggested_models(&selections).is_ok(), "{tier:?}");
            selections.clear();
        }
    }

    #[test]
    fn suggested_models_rejects_unknown_tiers_and_unsupported_efforts_with_exit_2() {
        for selection in ["unknown/high", "terra/low", "sol/xhigh", "luna/pro"] {
            let mut selections = std::collections::HashMap::new();
            selections.insert("1".into(), selection.into());
            let err = validate_suggested_models(&selections).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{selection}: {err}");
        }
    }

    #[test]
    fn resolve_floor_valid_converts_tier() {
        let (m, e) = resolve_floor(Some("opus-47"), Some("high")).unwrap();
        assert_eq!(m.as_deref(), Some("claude-opus-4-7"));
        assert_eq!(e.as_deref(), Some("high"));
    }

    #[test]
    fn resolve_floor_none_is_no_floor() {
        let (m, e) = resolve_floor(None, None).unwrap();
        assert!(m.is_none() && e.is_none());
    }

    #[test]
    fn resolve_floor_codex_and_unknown_models_exit_2() {
        for model in ["terra", "opus-99"] {
            let err = resolve_floor(Some(model), None).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{model}: {err}");
            assert!(
                err.to_string()
                    .contains("expected Claude tier: sonnet-5|opus-46|opus-47|opus-48"),
                "{model}: {err}"
            );
        }
    }

    #[test]
    fn resolve_floor_bad_effort_exits_2() {
        let err = resolve_floor(Some("opus-47"), Some("low")).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("min_effort"), "{err}");
    }

    #[test]
    fn strict_codex_rejects_claude_only_model_floor() {
        let err = validate_provider_floor(RunnerKind::Codex, true, Some("opus-47")).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string()
                .contains("provider=\"codex\" cannot use min_model"),
            "{err}"
        );
        validate_provider_floor(RunnerKind::Claude, true, Some("opus-47")).unwrap();
        validate_provider_floor(RunnerKind::Codex, true, None).unwrap();
        validate_provider_floor(RunnerKind::Codex, false, Some("opus-47")).unwrap();
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
            agent: RunnerKind::Claude,
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
            review_model: "sonnet",
            review_effort: "high",
            classifier_model: "claude-haiku-4-5-20251001",
            classifier_effort: "low",
            log_dir: &Sourced {
                value: None,
                source: Source::Default,
            },
            no_bare_agent: &Sourced {
                value: true,
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
            min_model: None,
            min_effort: None,
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
