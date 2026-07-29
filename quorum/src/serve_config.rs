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

// Keep the field declarations and the set inspected by the consumption guard in
// one macro invocation. Adding a field extends `DECLARED_SERVE_FILE_CONFIG_KEYS`
// automatically; the guard test then fails until it is either consumed at
// runtime or deliberately registered as deprecated.
macro_rules! declare_serve_file_config {
    ($( $(#[$meta:meta])* $field:ident: $ty:ty, )*) => {
        /// Deserializable TOML config for `quorum serve`. Every field is optional —
        /// missing fields fall back to built-in defaults; CLI flags override everything.
        #[derive(Debug, Default, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ServeFileConfig {
            $(
                $(#[$meta])*
                pub $field: $ty,
            )*
            // This exists only to prove the guard catches a field with no
            // runtime consumer. It is explicitly deprecated in test builds.
            #[cfg(test)]
            pub test_only_unconsumed: Option<bool>,
        }

        const DECLARED_SERVE_FILE_CONFIG_KEYS: &[&str] = &[
            $(stringify!($field),)*
            #[cfg(test)]
            "test_only_unconsumed",
        ];
    };
}

declare_serve_file_config! {
    /// Optional provider-wide runner selection. Unlike the legacy `agent`
    /// setting, this also constrains every role-specific model.
    provider: Option<String>,
    agent: Option<String>,
    cap: Option<usize>,
    repo_dir: Option<String>,
    worktree_base: Option<String>,
    names_file: Option<String>,
    agent_bin: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    worker_model: Option<String>,
    worker_effort: Option<String>,
    review_model: Option<String>,
    review_effort: Option<String>,
    classifier_model: Option<String>,
    classifier_effort: Option<String>,
    collector_model: Option<String>,
    collector_effort: Option<String>,
    merge_token_file: Option<String>,
    no_bare_agent: Option<bool>,
    max_turn_tokens: Option<i64>,
    max_task_tokens: Option<i64>,
    max_turn_cost_usd: Option<f64>,
    max_task_cost_usd: Option<f64>,
    max_turn_wall_secs: Option<u64>,
    max_task_wall_secs: Option<u64>,
    idle_timeout_secs: Option<u64>,
    allowed_tools: Option<String>,
    log_dir: Option<String>,
    self_update_drain: Option<bool>,
    drain_timeout_secs: Option<u64>,
    self_repo: Option<String>,
    sha_poll_interval_secs: Option<u64>,
    repo: Option<String>,
    base_branch: Option<String>,
    merge_checks_timeout_secs: Option<u64>,
    merge_checks_poll_secs: Option<u64>,
    required_jobs: Option<Vec<String>>,
    master_ci_gate: Option<bool>,
    master_ci_timeout_secs: Option<u64>,
    doctor_enabled: Option<bool>,
    /// Whether deterministic R2 sampling participates. `false` keeps R2 mandatory.
    r2_enabled: Option<bool>,
    /// Guaranteed coverage floor per (model, effort, complexity) stratum.
    r2_target_per_stratum: Option<i64>,
    /// Sampling probability once a stratum reaches its coverage floor.
    r2_steady_state_p: Option<f64>,
    /// Per-complexity suggested model/effort (keys "1".."5", values "tier/effort").
    suggested_models: Option<std::collections::HashMap<String, String>>,
    /// #172: minimum worker model tier floor ("sonnet-5"|"opus-46"|"opus-47"|"opus-48").
    /// A worker resolving below this is bumped up to it at spawn. None = no floor.
    min_model: Option<String>,
    /// #172: minimum worker effort floor ("medium"|"high"). None = no floor.
    min_effort: Option<String>,
    /// Runner-specific Codex configuration.
    codex: Option<CodexFileConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigKeyDisposition {
    Runtime,
    Deprecated,
}

// This is the explicit bridge between file keys and the runtime resolution
// paths in `main.rs` / `resolve_roles`. A deprecated key remains parseable for
// compatibility, but startup warns that it has no effect.
const SERVE_FILE_CONFIG_KEY_REGISTRY: &[(&str, ConfigKeyDisposition)] = &[
    ("provider", ConfigKeyDisposition::Runtime),
    ("agent", ConfigKeyDisposition::Runtime),
    ("cap", ConfigKeyDisposition::Runtime),
    ("repo_dir", ConfigKeyDisposition::Runtime),
    ("worktree_base", ConfigKeyDisposition::Runtime),
    ("names_file", ConfigKeyDisposition::Runtime),
    ("agent_bin", ConfigKeyDisposition::Runtime),
    ("model", ConfigKeyDisposition::Runtime),
    ("effort", ConfigKeyDisposition::Runtime),
    ("worker_model", ConfigKeyDisposition::Runtime),
    ("worker_effort", ConfigKeyDisposition::Runtime),
    ("review_model", ConfigKeyDisposition::Runtime),
    ("review_effort", ConfigKeyDisposition::Runtime),
    ("classifier_model", ConfigKeyDisposition::Runtime),
    ("classifier_effort", ConfigKeyDisposition::Runtime),
    ("collector_model", ConfigKeyDisposition::Runtime),
    ("collector_effort", ConfigKeyDisposition::Runtime),
    ("merge_token_file", ConfigKeyDisposition::Runtime),
    ("no_bare_agent", ConfigKeyDisposition::Runtime),
    ("max_turn_tokens", ConfigKeyDisposition::Runtime),
    ("max_task_tokens", ConfigKeyDisposition::Runtime),
    ("max_turn_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_task_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_turn_wall_secs", ConfigKeyDisposition::Runtime),
    ("max_task_wall_secs", ConfigKeyDisposition::Runtime),
    ("idle_timeout_secs", ConfigKeyDisposition::Runtime),
    ("allowed_tools", ConfigKeyDisposition::Runtime),
    ("log_dir", ConfigKeyDisposition::Runtime),
    ("self_update_drain", ConfigKeyDisposition::Runtime),
    ("drain_timeout_secs", ConfigKeyDisposition::Runtime),
    ("self_repo", ConfigKeyDisposition::Runtime),
    ("sha_poll_interval_secs", ConfigKeyDisposition::Runtime),
    ("repo", ConfigKeyDisposition::Runtime),
    ("base_branch", ConfigKeyDisposition::Runtime),
    ("merge_checks_timeout_secs", ConfigKeyDisposition::Runtime),
    ("merge_checks_poll_secs", ConfigKeyDisposition::Runtime),
    ("required_jobs", ConfigKeyDisposition::Runtime),
    ("master_ci_gate", ConfigKeyDisposition::Runtime),
    ("master_ci_timeout_secs", ConfigKeyDisposition::Runtime),
    ("doctor_enabled", ConfigKeyDisposition::Runtime),
    ("r2_enabled", ConfigKeyDisposition::Runtime),
    ("r2_target_per_stratum", ConfigKeyDisposition::Runtime),
    ("r2_steady_state_p", ConfigKeyDisposition::Runtime),
    ("suggested_models", ConfigKeyDisposition::Runtime),
    ("min_model", ConfigKeyDisposition::Runtime),
    ("min_effort", ConfigKeyDisposition::Runtime),
    ("codex", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("test_only_unconsumed", ConfigKeyDisposition::Deprecated),
];

fn validate_config_key_registry(
    declared: &[&str],
    registry: &[(&str, ConfigKeyDisposition)],
) -> Result<()> {
    use std::collections::BTreeSet;

    let declared: BTreeSet<_> = declared.iter().copied().collect();
    let mut registered = BTreeSet::new();
    for (key, _) in registry {
        if !registered.insert(*key) {
            return Err(QuorumError::Io(format!(
                "serve config key registry registers \"{key}\" more than once"
            )));
        }
    }
    let missing: Vec<_> = declared.difference(&registered).copied().collect();
    let unknown: Vec<_> = registered.difference(&declared).copied().collect();
    if missing.is_empty() && unknown.is_empty() {
        return Ok(());
    }
    Err(QuorumError::Io(format!(
        "serve config key registry drift: unclassified declared keys [{}]; registrations for undeclared keys [{}]",
        missing.join(", "),
        unknown.join(", "),
    )))
}

fn validate_serve_file_config_key_registry() -> Result<()> {
    validate_config_key_registry(
        DECLARED_SERVE_FILE_CONFIG_KEYS,
        SERVE_FILE_CONFIG_KEY_REGISTRY,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleConfig {
    pub provider: RunnerKind,
    pub provider_explicit: bool,
    /// Whether the reviewer model was explicitly selected, independently of
    /// the worker provider. This preserves an intentional cross-provider
    /// reviewer when `provider` remains on its legacy default.
    pub review_model_explicit: bool,
    pub worker_model: String,
    pub worker_effort: String,
    pub review_model: String,
    pub review_effort: String,
    pub classifier_model: String,
    pub classifier_effort: String,
    pub collector_model: String,
    pub collector_effort: String,
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
            "gpt-5.6-luna",
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

    let classifier_model = file
        .classifier_model
        .clone()
        .unwrap_or_else(|| classifier_default_model.into());
    let classifier_effort = file
        .classifier_effort
        .clone()
        .unwrap_or_else(|| classifier_default_effort.into());
    let roles = RoleConfig {
        provider,
        provider_explicit,
        review_model_explicit: file.review_model.is_some(),
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
        collector_model: file
            .collector_model
            .clone()
            .unwrap_or_else(|| classifier_model.clone()),
        collector_effort: file
            .collector_effort
            .clone()
            .unwrap_or_else(|| classifier_effort.clone()),
        classifier_model,
        classifier_effort,
    };

    let role_models_explicit = file.worker_model.is_some()
        || file.review_model.is_some()
        || file.classifier_model.is_some()
        || file.collector_model.is_some();
    if provider_explicit || role_models_explicit {
        // `review_model` may intentionally select the other supported provider:
        // reviewer spawning resolves its provider from this model. The remaining
        // daemon roles stay pinned to the explicit provider.
        for (role, model) in [
            ("worker", roles.worker_model.as_str()),
            ("classifier", roles.classifier_model.as_str()),
            ("collector", roles.collector_model.as_str()),
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

        crate::serve::runner::AgentKind::for_model(&roles.review_model)
            .map_err(QuorumError::Usage)?;
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
    validate_serve_file_config_key_registry()?;
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let cfg: ServeFileConfig = toml::from_str(&s).map_err(|e| {
                QuorumError::Usage(format!("bad serve config {}: {e}", path.display()))
            })?;
            warn_for_deprecated_keys(&s)?;
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

fn warn_for_deprecated_keys(toml_source: &str) -> Result<()> {
    let value: toml::Value = toml::from_str(toml_source).map_err(|e| {
        QuorumError::Io(format!(
            "validated serve config could not be inspected: {e}"
        ))
    })?;
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    for (key, disposition) in SERVE_FILE_CONFIG_KEY_REGISTRY {
        if *disposition == ConfigKeyDisposition::Deprecated && table.contains_key(*key) {
            eprintln!("quorum serve: WARNING: deprecated config key \"{key}\" has no effect");
        }
    }
    Ok(())
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

/// Validate the opt-in R2 sampling knobs. Invalid values are configuration
/// errors, never values to silently clamp into a different review policy.
pub fn validate_r2_sampling(target_per_stratum: i64, steady_state_p: f64) -> Result<()> {
    if target_per_stratum < 0 {
        return Err(QuorumError::Usage(format!(
            "r2_target_per_stratum must be >= 0 (got {target_per_stratum})"
        )));
    }
    if !(0.0..=1.0).contains(&steady_state_p) {
        return Err(QuorumError::Usage(format!(
            "r2_steady_state_p must be in 0.0..=1.0 (got {steady_state_p})"
        )));
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
    pub collector_model: &'a str,
    pub collector_effort: &'a str,
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
    lines.push(format!(
        "  collector_model:           {}",
        d.collector_model
    ));
    lines.push(format!(
        "  collector_effort:          {}",
        d.collector_effort
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
    fn config_key_registry_covers_every_declared_field() {
        validate_serve_file_config_key_registry().unwrap();
    }

    #[test]
    fn config_key_registry_rejects_an_unconsumed_test_only_field() {
        let registry: Vec<_> = SERVE_FILE_CONFIG_KEY_REGISTRY
            .iter()
            .copied()
            .filter(|(key, _)| *key != "test_only_unconsumed")
            .collect();
        let err =
            validate_config_key_registry(DECLARED_SERVE_FILE_CONFIG_KEYS, &registry).unwrap_err();
        assert_eq!(err.exit_code(), 3);
        assert!(
            err.to_string().contains("test_only_unconsumed"),
            "guard should name the unconsumed field: {err}"
        );
    }

    #[test]
    fn deprecated_key_warns_and_still_loads_for_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "test_only_unconsumed = true\n").unwrap();

        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.test_only_unconsumed, Some(true));
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
                "gpt-5.6-luna",
                "medium",
            )
        );
    }

    #[test]
    fn explicit_classifier_model_overrides_codex_default() {
        let cfg: ServeFileConfig = toml::from_str(
            "provider = \"codex\"\nclassifier_model = \"gpt-5.6-sol\"\nclassifier_effort = \"high\"\n",
        )
        .unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();

        assert_eq!(roles.worker_model, "gpt-5.6-terra");
        assert_eq!(roles.worker_effort, "medium");
        assert_eq!(roles.review_model, "gpt-5.6-terra");
        assert_eq!(roles.review_effort, "high");
        assert_eq!(roles.classifier_model, "gpt-5.6-sol");
        assert_eq!(roles.classifier_effort, "high");
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
        assert_eq!(roles.collector_model, roles.classifier_model);
        assert_eq!(roles.collector_effort, roles.classifier_effort);
    }

    #[test]
    fn collector_roles_default_to_classifier_values_when_absent() {
        let cfg: ServeFileConfig = toml::from_str(
            "classifier_model = \"claude-sonnet-5\"\nclassifier_effort = \"high\"\n",
        )
        .unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.collector_model, roles.classifier_model);
        assert_eq!(roles.collector_effort, roles.classifier_effort);
    }

    #[test]
    fn explicit_collector_roles_override_classifier_values() {
        let cfg: ServeFileConfig = toml::from_str("classifier_model = \"claude-haiku-4-5-20251001\"\nclassifier_effort = \"low\"\ncollector_model = \"claude-opus-4-8\"\ncollector_effort = \"high\"\n").unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.collector_model, "claude-opus-4-8");
        assert_eq!(roles.collector_effort, "high");
    }

    #[test]
    fn explicit_provider_allows_cross_provider_review_model() {
        let cfg: ServeFileConfig = toml::from_str(
            "provider = \"codex\"\nworker_model = \"gpt-5.6-terra\"\nreview_model = \"claude-opus-4-8\"\n",
        )
        .unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.worker_model, "gpt-5.6-terra");
        assert_eq!(roles.review_model, "claude-opus-4-8");
        assert!(roles.review_model_explicit);
    }

    #[test]
    fn no_provider_explicit_cross_provider_review_model_is_preserved() {
        let cfg: ServeFileConfig =
            toml::from_str("review_model = \"gpt-5.6-terra\"\nreview_effort = \"high\"\n").unwrap();
        let roles = resolve_roles(&cfg, None, "claude-opus-4-8", "medium").unwrap();

        assert!(!roles.provider_explicit);
        assert!(roles.review_model_explicit);
        assert_eq!(roles.review_model, "gpt-5.6-terra");
        assert_eq!(roles.review_effort, "high");
    }

    #[test]
    fn explicit_provider_rejects_unknown_review_model() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\nreview_model = \"mystery\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("unknown model"), "{err}");
    }

    #[test]
    fn explicit_provider_rejects_worker_model_mismatch() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\nworker_model = \"claude-opus-4-8\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("worker_model"), "{err}");
    }

    #[test]
    fn explicit_provider_rejects_classifier_model_mismatch() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\nclassifier_model = \"claude-sonnet-5\"\n")
                .unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("classifier_model"), "{err}");
    }

    #[test]
    fn explicit_provider_rejects_collector_model_mismatch() {
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"codex\"\ncollector_model = \"claude-opus-4-8\"\n")
                .unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("collector_model"), "{err}");
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
    fn r2_sampling_validation_rejects_invalid_values_with_usage_exit() {
        for (target, probability) in [(0, 1.5), (-1, 0.30)] {
            let err = validate_r2_sampling(target, probability).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{err}");
        }
        validate_r2_sampling(0, 0.0).unwrap();
        validate_r2_sampling(3, 1.0).unwrap();
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
            collector_model: "claude-haiku-4-5-20251001",
            collector_effort: "low",
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
