//! TOML config file for `quorum serve`. Missing file → built-in defaults.
//! Malformed or unknown keys → fail loud (exit 2).
//!
//! CLI flags override config-file values (explicit wins). The resolved config
//! and each value's source (file vs flag vs default) are logged at startup.

use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

fn default_profile_effort() -> String {
    "high".to_string()
}

/// Stable, owner-defined executable model identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfile {
    pub runner: String,
    pub model: String,
    #[serde(default = "default_profile_effort")]
    pub effort: String,
}

/// Integer percentages keyed by model-profile identity.
pub type PercentagePool = BTreeMap<String, u8>;

/// Complete routing policy. R1 and R2 use the same reviewer pool but keep
/// independent allocation state in the persistence layer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPolicy {
    pub classifier: PercentagePool,
    pub planner: PercentagePool,
    pub collector: PercentagePool,
    pub worker: BTreeMap<String, PercentagePool>,
    pub reviewer: BTreeMap<String, PercentagePool>,
}

/// Which CLI runner the daemon uses for all spawned agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RunnerKind {
    Claude,
    Codex,
    Grok,
}

impl std::fmt::Display for RunnerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claude => write!(f, "claude"),
            Self::Codex => write!(f, "codex"),
            Self::Grok => write!(f, "grok"),
        }
    }
}

impl RunnerKind {
    pub fn from_str_opt(s: Option<&str>) -> Result<Self> {
        match s {
            None | Some("claude") => Ok(Self::Claude),
            Some("codex") => Ok(Self::Codex),
            Some("grok") => Ok(Self::Grok),
            Some(other) => Err(QuorumError::Usage(format!(
                "bad agent value: \"{other}\" (expected \"claude\", \"codex\", or \"grok\")"
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

        #[allow(unused_doc_comments)]
        const DECLARED_SERVE_FILE_CONFIG_KEYS: &[&str] = &[
            $($(#[$meta])* stringify!($field),)*
            #[cfg(test)]
            "test_only_unconsumed",
        ];
    };
}

declare_serve_file_config! {
    /// Named executable models referenced by the routing policy.
    model_profiles: Option<BTreeMap<String, ModelProfile>>,
    /// Required, coherent routing policy for every managed role.
    routing: Option<RoutingPolicy>,
    /// Optional provider-wide runner selection. Unlike the legacy `agent`
    /// setting, this also constrains every role-specific model.
    #[cfg(test)]
    provider: Option<String>,
    #[cfg(test)]
    agent: Option<String>,
    cap: Option<usize>,
    repo_dir: Option<String>,
    worktree_base: Option<String>,
    names_file: Option<String>,
    agent_bin: Option<String>,
    #[cfg(test)]
    model: Option<String>,
    #[cfg(test)]
    worker_model: Option<String>,
    #[cfg(test)]
    worker_effort: Option<String>,
    #[cfg(test)]
    review_model: Option<String>,
    #[cfg(test)]
    review_effort: Option<String>,
    #[cfg(test)]
    classifier_model: Option<String>,
    #[cfg(test)]
    classifier_effort: Option<String>,
    #[cfg(test)]
    collector_model: Option<String>,
    #[cfg(test)]
    collector_effort: Option<String>,
    merge_token_file: Option<String>,
    no_bare_agent: Option<bool>,
    max_turn_tokens: Option<i64>,
    max_task_tokens: Option<i64>,
    max_turn_cost_usd: Option<f64>,
    max_task_cost_usd: Option<f64>,
    max_turn_wall_secs: Option<u64>,
    max_idle_secs: Option<u64>,
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
    /// Runner-specific Codex configuration.
    codex: Option<CodexFileConfig>,
    /// Transport-only Grok adapter configuration. Managed Grok roles remain disabled.
    grok: Option<GrokFileConfig>,
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
    ("model_profiles", ConfigKeyDisposition::Runtime),
    ("routing", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("provider", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("agent", ConfigKeyDisposition::Runtime),
    ("cap", ConfigKeyDisposition::Runtime),
    ("repo_dir", ConfigKeyDisposition::Runtime),
    ("worktree_base", ConfigKeyDisposition::Runtime),
    ("names_file", ConfigKeyDisposition::Runtime),
    ("agent_bin", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("model", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("worker_model", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("worker_effort", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("review_model", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("review_effort", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("classifier_model", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("classifier_effort", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("collector_model", ConfigKeyDisposition::Runtime),
    #[cfg(test)]
    ("collector_effort", ConfigKeyDisposition::Runtime),
    ("merge_token_file", ConfigKeyDisposition::Runtime),
    ("no_bare_agent", ConfigKeyDisposition::Runtime),
    ("max_turn_tokens", ConfigKeyDisposition::Runtime),
    ("max_task_tokens", ConfigKeyDisposition::Runtime),
    ("max_turn_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_task_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_turn_wall_secs", ConfigKeyDisposition::Runtime),
    ("max_idle_secs", ConfigKeyDisposition::Runtime),
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
    ("codex", ConfigKeyDisposition::Runtime),
    ("grok", ConfigKeyDisposition::Runtime),
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

#[cfg(test)]
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

#[cfg(test)]
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
    if provider == RunnerKind::Grok {
        return Err(QuorumError::Usage(
            "provider=\"grok\" is not enabled for managed lifecycle roles; \
             the built-in Grok transport is validation-only"
                .into(),
        ));
    }
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
    for (role, model) in [
        ("worker", roles.worker_model.as_str()),
        ("review", roles.review_model.as_str()),
        ("classifier", roles.classifier_model.as_str()),
        ("collector", roles.collector_model.as_str()),
    ] {
        if crate::serve::runner::AgentKind::for_model(model)
            .is_ok_and(|kind| kind == crate::serve::runner::AgentKind::Grok)
        {
            return Err(QuorumError::Usage(format!(
                "{role}_model \"{model}\" selects Grok, but managed Grok lifecycle roles are not enabled"
            )));
        }
    }
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
                RunnerKind::Grok => unreachable!("Grok provider was rejected above"),
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

/// `[grok]` transport settings. These are validated now so enabling managed
/// roles later cannot inherit permissive or misspelled values silently.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrokFileConfig {
    pub sandbox: Option<String>,
    pub permission_mode: Option<String>,
    pub max_turns: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokResolvedConfig {
    pub sandbox: String,
    pub permission_mode: String,
    pub max_turns: u32,
}

pub fn resolve_grok_adapter(file: Option<&GrokFileConfig>) -> Result<GrokResolvedConfig> {
    let resolved = GrokResolvedConfig {
        sandbox: file
            .and_then(|config| config.sandbox.clone())
            .unwrap_or_else(|| crate::serve::grok_agent::DEFAULT_SANDBOX.into()),
        permission_mode: file
            .and_then(|config| config.permission_mode.clone())
            .unwrap_or_else(|| crate::serve::grok_agent::DEFAULT_PERMISSION_MODE.into()),
        max_turns: file
            .and_then(|config| config.max_turns)
            .unwrap_or(crate::serve::grok_agent::DEFAULT_MAX_TURNS),
    };
    crate::serve::grok_agent::GrokAdapterConfig {
        sandbox: &resolved.sandbox,
        permission_mode: &resolved.permission_mode,
        max_turns: resolved.max_turns,
    }
    .validate()
    .map_err(QuorumError::Usage)?;
    Ok(resolved)
}

/// Load serve config from `path`. Malformed / unknown keys → exit 2.
/// When `explicit` is true (user passed --config), missing file → exit 2.
/// When false (auto-discovered default path), missing file → built-in defaults.
pub fn load(path: &Path, explicit: bool) -> Result<ServeFileConfig> {
    validate_serve_file_config_key_registry()?;
    match std::fs::read_to_string(path) {
        Ok(s) => {
            reject_legacy_routing_keys(&s, path)?;
            let cfg: ServeFileConfig = toml::from_str(&s).map_err(|e| {
                QuorumError::Usage(format!("bad serve config {}: {e}", path.display()))
            })?;
            resolve_grok_adapter(cfg.grok.as_ref())?;
            validate_model_routing(&cfg)?;
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

const LEGACY_ROUTING_KEYS: &[&str] = &[
    "provider",
    "agent",
    "model",
    "effort",
    "worker_model",
    "worker_effort",
    "review_model",
    "review_effort",
    "classifier_model",
    "classifier_effort",
    "collector_model",
    "collector_effort",
    "suggested_models",
    "min_model",
    "min_effort",
];

fn reject_legacy_routing_keys(toml_source: &str, path: &Path) -> Result<()> {
    let value: toml::Value = toml::from_str(toml_source)
        .map_err(|e| QuorumError::Usage(format!("bad serve config {}: {e}", path.display())))?;
    let Some(table) = value.as_table() else {
        return Err(QuorumError::Usage(
            "serve config must be a TOML table".into(),
        ));
    };
    if let Some(key) = LEGACY_ROUTING_KEYS
        .iter()
        .find(|key| table.contains_key(**key))
    {
        return Err(QuorumError::Usage(format!(
            "legacy routing key \"{key}\" is not supported; configure model_profiles and routing"
        )));
    }
    Ok(())
}

/// Validate the hard-cutover model-routing configuration before the daemon can
/// claim work.
pub fn validate_model_routing(config: &ServeFileConfig) -> Result<()> {
    let profiles = config
        .model_profiles
        .as_ref()
        .ok_or_else(|| QuorumError::Usage("serve config requires [model_profiles]".into()))?;
    if profiles.is_empty() {
        return Err(QuorumError::Usage(
            "model_profiles must contain at least one profile".into(),
        ));
    }
    for (name, profile) in profiles {
        validate_durable_routing_text("model profile name", name)?;
        validate_durable_routing_text("model", &profile.model)?;
        validate_durable_routing_text("effort", &profile.effort)?;
        let expected = RunnerKind::from_str_opt(Some(&profile.runner))?;
        let actual = crate::serve::runner::AgentKind::for_model(&profile.model)
            .map_err(|error| QuorumError::Usage(format!("model profile \"{name}\": {error}")))?;
        let expected = match expected {
            RunnerKind::Claude => crate::serve::runner::AgentKind::Claude,
            RunnerKind::Codex => crate::serve::runner::AgentKind::Codex,
            RunnerKind::Grok => {
                return Err(QuorumError::Usage(format!(
                    "model profile \"{name}\" selects Grok, but managed Grok lifecycle roles are not enabled"
                )))
            }
        };
        if actual != expected {
            return Err(QuorumError::Usage(format!(
                "model profile \"{name}\" model \"{}\" does not match runner \"{}\"",
                profile.model, profile.runner
            )));
        }
        let effort_supported = match expected {
            crate::serve::runner::AgentKind::Claude => {
                matches!(profile.effort.as_str(), "low" | "medium" | "high")
            }
            crate::serve::runner::AgentKind::Codex => {
                matches!(profile.effort.as_str(), "low" | "medium" | "high" | "xhigh")
            }
            crate::serve::runner::AgentKind::Grok => false,
        };
        if !effort_supported {
            return Err(QuorumError::Usage(format!(
                "model profile \"{name}\" has unsupported effort \"{}\" for runner \"{}\"",
                profile.effort, profile.runner
            )));
        }
    }
    validate_agent_bin_for_profiles(config.agent_bin.as_deref(), profiles)?;

    let routing = config
        .routing
        .as_ref()
        .ok_or_else(|| QuorumError::Usage("serve config requires [routing]".into()))?;
    validate_percentage_pool("classifier", &routing.classifier, profiles)?;
    validate_percentage_pool("planner", &routing.planner, profiles)?;
    for profile_id in routing.planner.keys() {
        if profiles[profile_id].runner == "codex" {
            return Err(QuorumError::Usage(format!(
                "routing.planner profile \"{profile_id}\" uses unsupported runner \"codex\""
            )));
        }
    }
    validate_percentage_pool("collector", &routing.collector, profiles)?;
    validate_complexity_pools("worker", &routing.worker, profiles)?;
    validate_complexity_pools("reviewer", &routing.reviewer, profiles)?;
    validate_routed_cost_limits(config, config.max_turn_cost_usd, config.max_task_cost_usd)?;
    Ok(())
}

/// A custom executable belongs to one provider CLI. Validate it after every
/// config source has been resolved, because `--agent-bin` is merged after the
/// TOML file itself has been validated.
pub fn validate_agent_bin_for_profiles(
    agent_bin: Option<&str>,
    profiles: &BTreeMap<String, ModelProfile>,
) -> Result<()> {
    if agent_bin.is_none() {
        return Ok(());
    }
    let configured_runners: std::collections::BTreeSet<&str> = profiles
        .values()
        .map(|profile| profile.runner.as_str())
        .collect();
    if configured_runners.len() > 1 {
        return Err(QuorumError::Usage(
            "agent_bin cannot be used when model_profiles span multiple runners".into(),
        ));
    }
    Ok(())
}

pub fn validate_routed_cost_limits(
    config: &ServeFileConfig,
    max_turn_cost_usd: Option<f64>,
    max_task_cost_usd: Option<f64>,
) -> Result<()> {
    let Some(profiles) = config.model_profiles.as_ref() else {
        return Ok(());
    };
    let Some(routing) = config.routing.as_ref() else {
        return Ok(());
    };
    if (max_turn_cost_usd.is_some() || max_task_cost_usd.is_some())
        && referenced_profile_ids(routing).any(|profile_id| profiles[profile_id].runner == "codex")
    {
        return Err(QuorumError::Usage(
            "USD cost limits are unsupported when routing can select a Codex profile".into(),
        ));
    }
    Ok(())
}

fn referenced_profile_ids(routing: &RoutingPolicy) -> impl Iterator<Item = &String> {
    routing
        .classifier
        .keys()
        .chain(routing.planner.keys())
        .chain(routing.collector.keys())
        .chain(routing.worker.values().flat_map(|pool| pool.keys()))
        .chain(routing.reviewer.values().flat_map(|pool| pool.keys()))
}

fn validate_complexity_pools(
    role: &str,
    pools: &BTreeMap<String, PercentagePool>,
    profiles: &BTreeMap<String, ModelProfile>,
) -> Result<()> {
    for level in 1..=5 {
        let key = level.to_string();
        let pool = pools.get(&key).ok_or_else(|| {
            QuorumError::Usage(format!("routing.{role} requires complexity pool \"{key}\""))
        })?;
        validate_percentage_pool(&format!("{role}.{key}"), pool, profiles)?;
    }
    if let Some(extra) = pools
        .keys()
        .find(|key| !matches!(key.as_str(), "1" | "2" | "3" | "4" | "5"))
    {
        return Err(QuorumError::Usage(format!(
            "routing.{role} has unsupported complexity pool \"{extra}\""
        )));
    }
    Ok(())
}

fn validate_percentage_pool(
    name: &str,
    pool: &PercentagePool,
    profiles: &BTreeMap<String, ModelProfile>,
) -> Result<()> {
    if pool.is_empty() {
        return Err(QuorumError::Usage(format!(
            "routing.{name} must not be empty"
        )));
    }
    let mut total = 0_u16;
    for (profile, percentage) in pool {
        if !profiles.contains_key(profile) {
            return Err(QuorumError::Usage(format!(
                "routing.{name} references unknown model profile \"{profile}\""
            )));
        }
        if *percentage == 0 {
            return Err(QuorumError::Usage(format!(
                "routing.{name} profile \"{profile}\" must have a positive percentage"
            )));
        }
        total += u16::from(*percentage);
    }
    if total != 100 {
        return Err(QuorumError::Usage(format!(
            "routing.{name} percentages total {total}, expected 100"
        )));
    }
    let generation = routing_generation(name, pool, profiles)?;
    validate_durable_routing_text("routing policy generation", &generation)?;
    Ok(())
}

fn validate_durable_routing_text(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains('\0') {
        return Err(QuorumError::Usage(format!(
            "{label} must be non-empty, contain no NUL, and be at most 1024 bytes"
        )));
    }
    Ok(())
}

pub(crate) fn routing_generation(
    pool_key: &str,
    percentages: &BTreeMap<String, u8>,
    profiles: &BTreeMap<String, ModelProfile>,
) -> Result<String> {
    let mut entries = Vec::with_capacity(percentages.len());
    for (profile_id, percentage) in percentages {
        let profile = profiles.get(profile_id).ok_or_else(|| {
            QuorumError::Usage(format!(
                "routing pool {pool_key} references unknown profile {profile_id}"
            ))
        })?;
        entries.push(serde_json::json!({
            "profile": profile_id,
            "percentage": percentage,
            "runner": profile.runner,
            "model": profile.model,
            "effort": profile.effort,
        }));
    }
    serde_json::to_string(&serde_json::json!({"pool": pool_key, "entries": entries}))
        .map_err(|error| QuorumError::Io(format!("routing policy identity failed: {error}")))
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
#[cfg(test)]
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
    pub repo: &'a Sourced<String>,
    pub repo_dir: &'a Sourced<String>,
    pub worktree_base: &'a Sourced<String>,
    pub base_branch: &'a Sourced<String>,
    pub cap: &'a Sourced<usize>,
    pub model_profiles: &'a BTreeMap<String, ModelProfile>,
    pub routing: &'a RoutingPolicy,
    pub log_dir: &'a Sourced<Option<String>>,
    pub no_bare_agent: &'a Sourced<bool>,
    pub self_update_drain: &'a Sourced<bool>,
    pub drain_timeout_secs: &'a Sourced<u64>,
    pub max_idle_secs: &'a Sourced<Option<u64>>,
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
    lines.push(format!(
        "  model_profiles:            {}",
        d.model_profiles.len()
    ));
    lines.push(format!(
        "  routing pools:             classifier={}, planner={}, collector={}, worker=5, reviewer=5",
        d.routing.classifier.len(),
        d.routing.planner.len(),
        d.routing.collector.len()
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
        "  max_idle_secs:             {}",
        match d.max_idle_secs.value {
            Some(v) => format!("{v} ({src})", src = d.max_idle_secs.source),
            None => format!("900 ({src})", src = d.max_idle_secs.source),
        }
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

/// #172: validate + convert the worker model/effort floor from config strings.
/// `min_model` tier → full model id; `min_effort` must be "medium"|"high".
/// Bad values → Usage error (exit 2), consistent with the rest of serve config.
/// Returns (model_id, effort); either is None when its input is None (no floor).
#[cfg(test)]
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
#[cfg(test)]
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

    const VALID_ROUTING: &str = r#"
[model_profiles.primary]
runner = "codex"
model = "gpt-5.6-sol"

[model_profiles.planner]
runner = "claude"
model = "claude-opus-4-8"

[routing.classifier]
primary = 100
[routing.planner]
planner = 100
[routing.collector]
primary = 100
[routing.worker.1]
primary = 100
[routing.worker.2]
primary = 100
[routing.worker.3]
primary = 100
[routing.worker.4]
primary = 100
[routing.worker.5]
primary = 100
[routing.reviewer.1]
primary = 100
[routing.reviewer.2]
primary = 100
[routing.reviewer.3]
primary = 100
[routing.reviewer.4]
primary = 100
[routing.reviewer.5]
primary = 100
"#;

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
        std::fs::write(
            &path,
            format!("test_only_unconsumed = true\n{VALID_ROUTING}"),
        )
        .unwrap();

        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.test_only_unconsumed, Some(true));
    }

    #[test]
    fn load_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            format!(
                r#"
cap = 8
max_turn_wall_secs = 2700
max_idle_secs = 900
max_task_wall_secs = 14400
repo = "ag2trust/quorum"
repo_dir = "/home/user/dev/quorum"
worktree_base = "/home/user/.quorum/serve/quorum/worktrees"
log_dir = "/home/user/.quorum/serve/quorum/logs"
{VALID_ROUTING}"#
            ),
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.cap, Some(8));
        assert_eq!(
            cfg.model_profiles.as_ref().unwrap()["primary"].effort,
            "high"
        );
        assert_eq!(cfg.max_turn_wall_secs, Some(2700));
        assert_eq!(cfg.max_idle_secs, Some(900));
        assert!(cfg.doctor_enabled.is_none());
    }

    #[test]
    fn load_doctor_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            format!("repo = \"test/repo\"\nrepo_dir = \"/tmp\"\nworktree_base = \"/tmp/wt\"\ndoctor_enabled = true\n{VALID_ROUTING}"),
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.doctor_enabled, Some(true));
    }

    #[test]
    fn grok_transport_configuration_is_closed_and_validated() {
        let cfg: ServeFileConfig = toml::from_str(
            "[grok]\nsandbox = \"off\"\npermission_mode = \"bypassPermissions\"\nmax_turns = 12\n",
        )
        .unwrap();
        assert_eq!(
            resolve_grok_adapter(cfg.grok.as_ref()).unwrap(),
            GrokResolvedConfig {
                sandbox: "off".into(),
                permission_mode: "bypassPermissions".into(),
                max_turns: 12,
            }
        );
        for source in [
            "[grok]\nsandbox = \"custom\"\n",
            "[grok]\npermission_mode = \"auto\"\n",
            "[grok]\nmax_turns = 0\n",
            "[grok]\nmax_turns = 257\n",
        ] {
            let cfg: ServeFileConfig = toml::from_str(source).unwrap();
            let error = resolve_grok_adapter(cfg.grok.as_ref()).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{source}: {error}");
        }
    }

    #[test]
    fn load_rejects_invalid_grok_transport_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "[grok]\npermission_mode = \"default\"\n").unwrap();
        let error = load(&path, true).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert!(error.to_string().contains("permission_mode"), "{error}");
    }

    #[test]
    fn grok_provider_and_role_models_remain_transport_only() {
        assert_eq!(
            RunnerKind::from_str_opt(Some("grok")).unwrap(),
            RunnerKind::Grok
        );
        assert_eq!(RunnerKind::Grok.to_string(), "grok");

        for (source, cli_agent) in [
            ("provider = \"grok\"\n", None),
            ("agent = \"grok\"\n", None),
            ("", Some("grok")),
        ] {
            let config: ServeFileConfig = toml::from_str(source).unwrap();
            let error = resolve_roles(&config, cli_agent, "sonnet", "high").unwrap_err();
            assert!(error.to_string().contains("not enabled"), "{error}");
        }

        for key in [
            "worker_model",
            "review_model",
            "classifier_model",
            "collector_model",
        ] {
            let cfg: ServeFileConfig = toml::from_str(&format!("{key} = \"grok-4.5\"\n")).unwrap();
            let error = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
            assert!(error.to_string().contains("not enabled"), "{key}: {error}");
        }
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
    fn load_rejects_legacy_suggested_models() {
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
        let err = load(&path, true).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("legacy routing key"), "{err}");
    }

    #[test]
    fn malformed_config_error_keeps_file_path_context() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken-serve.toml");
        std::fs::write(&path, "routing = [").unwrap();
        let err = load(&path, true).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string().contains(&path.display().to_string()),
            "{err}"
        );
    }

    #[test]
    fn routing_accepts_integer_percentages_and_defaults_effort() {
        let cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        validate_model_routing(&cfg).unwrap();
        assert_eq!(cfg.model_profiles.unwrap()["primary"].effort, "high");
    }

    #[test]
    fn routing_rejects_profile_ids_that_durable_assignments_cannot_store() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let oversized = "p".repeat(1025);
        let profiles = cfg.model_profiles.as_mut().unwrap();
        let profile = profiles.remove("primary").unwrap();
        profiles.insert(oversized.clone(), profile);
        let routing = cfg.routing.as_mut().unwrap();
        for pool in std::iter::once(&mut routing.classifier)
            .chain(std::iter::once(&mut routing.collector))
            .chain(routing.worker.values_mut())
            .chain(routing.reviewer.values_mut())
        {
            pool.remove("primary");
            pool.insert(oversized.clone(), 100);
        }
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("at most 1024 bytes"), "{err}");
    }

    #[test]
    fn routing_rejects_policy_identity_that_durable_assignments_cannot_store() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profiles = cfg.model_profiles.as_mut().unwrap();
        let template = profiles["primary"].clone();
        let classifier = &mut cfg.routing.as_mut().unwrap().classifier;
        classifier.clear();
        for index in 0..20 {
            let id = format!("classifier-profile-{index:02}");
            profiles.insert(id.clone(), template.clone());
            classifier.insert(id, 5);
        }
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("routing policy generation")
                && err.to_string().contains("at most 1024 bytes"),
            "{err}"
        );
    }

    #[test]
    fn routing_rejects_usd_limits_when_any_pool_can_select_codex() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.max_turn_cost_usd = Some(1.0);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("USD cost limits"));

        cfg.max_turn_cost_usd = None;
        cfg.max_task_cost_usd = Some(10.0);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("USD cost limits"));
    }

    #[test]
    fn routing_rejects_missing_role_and_complexity_pools() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.routing.as_mut().unwrap().collector.clear();
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("collector must not be empty"),
            "{err}"
        );

        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.routing.as_mut().unwrap().worker.remove("4");
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("complexity pool \"4\""), "{err}");
    }

    #[test]
    fn routing_rejects_bad_percentages_and_unknown_profiles() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.routing
            .as_mut()
            .unwrap()
            .classifier
            .insert("primary".into(), 99);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("total 99"), "{err}");

        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let pool = &mut cfg.routing.as_mut().unwrap().planner;
        pool.clear();
        pool.insert("missing".into(), 100);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("unknown model profile"), "{err}");
    }

    #[test]
    fn routing_rejects_runner_model_mismatch() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap()
            .runner = "claude".into();
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("does not match runner"), "{err}");
    }

    #[test]
    fn routing_rejects_effort_unsupported_by_runner() {
        let mut codex: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        codex
            .model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap()
            .effort = "max".into();
        let err = validate_model_routing(&codex).unwrap_err();
        assert!(err.to_string().contains("unsupported effort"), "{err}");

        let mut claude: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profile = claude
            .model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap();
        profile.runner = "claude".into();
        profile.model = "claude-opus-4-8".into();
        profile.effort = "xhigh".into();
        let err = validate_model_routing(&claude).unwrap_err();
        assert!(err.to_string().contains("unsupported effort"), "{err}");
    }

    #[test]
    fn routing_accepts_codex_xhigh_and_shared_efforts() {
        for effort in ["low", "medium", "high", "xhigh"] {
            let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
            cfg.model_profiles
                .as_mut()
                .unwrap()
                .get_mut("primary")
                .unwrap()
                .effort = effort.into();
            validate_model_routing(&cfg).unwrap();
        }
    }

    #[test]
    fn agent_bin_rejects_profiles_spanning_runners() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.agent_bin = Some("/custom/provider-cli".into());
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("span multiple runners"), "{err}");

        cfg.agent_bin = None;
        validate_model_routing(&cfg).unwrap();

        let err = validate_agent_bin_for_profiles(
            Some("/custom/provider-cli"),
            cfg.model_profiles.as_ref().unwrap(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("span multiple runners"), "{err}");
    }

    #[test]
    fn planner_rejects_codex_profile_until_launch_boundary_supports_it() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let planner = &mut cfg.routing.as_mut().unwrap().planner;
        planner.clear();
        planner.insert("primary".into(), 100);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("routing.planner")
                && err.to_string().contains("unsupported runner \"codex\""),
            "{err}"
        );
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
        // `quorum init` emits a complete active routing policy so a fresh repository
        // can start after only its repository paths are supplied.
        let cfg: ServeFileConfig = toml::from_str(crate::DEFAULT_SERVE_TOML).unwrap();
        assert!(cfg.cap.is_none());
        assert!(cfg.r2_enabled.is_none());
        assert!(cfg.model_profiles.is_some());
        assert!(cfg.routing.is_some());
        validate_model_routing(&cfg).unwrap();
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
        let routing_cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
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
            model_profiles: routing_cfg.model_profiles.as_ref().unwrap(),
            routing: routing_cfg.routing.as_ref().unwrap(),
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
            max_idle_secs: &Sourced {
                value: None,
                source: Source::Default,
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
        assert!(
            b.contains("max_idle_secs:             900 (default)"),
            "max_idle_secs should default to 900: {b}"
        );
        assert!(b.contains("(default)"), "defaults should be labeled: {b}");
    }
}
