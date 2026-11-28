//! TOML config file for `quorum serve`. Missing file → built-in defaults.
//! Malformed or unknown keys → fail loud (exit 2).
//!
//! Corresponding CLI flags override config-file values (explicit wins); some
//! diagnostics are intentionally file-only. The resolved config and each
//! value's source (file vs flag vs default) are logged at startup.

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
    /// Plan-review Arbiter pool. Absent `[routing.arbiter]` blocks deserialize
    /// to an empty map (the `#[serde(default)]` sentinel), which
    /// [`RoutingPolicy::arbiter_pool`] resolves to the planner pool. Profile
    /// IDs are owner-defined, so no fixed default value could validate against
    /// an arbitrary config's `[model_profiles]`; mirroring the planner pool is
    /// the only default that keeps every existing config parsing and
    /// validating unchanged while carrying the planner's Claude-Opus intent.
    #[serde(default)]
    pub arbiter: PercentagePool,
    pub collector: PercentagePool,
    pub worker: BTreeMap<String, PercentagePool>,
    pub reviewer: BTreeMap<String, PercentagePool>,
}

impl RoutingPolicy {
    /// Effective Arbiter routing pool. An unset `[routing.arbiter]` block
    /// (empty map) falls back to the planner pool, so the Arbiter resolves to
    /// the same model profile distribution as the planner unless an operator
    /// configures an explicit override. Dormant: no runtime path resolves an
    /// Arbiter assignment yet.
    pub fn arbiter_pool(&self) -> &PercentagePool {
        if self.arbiter.is_empty() {
            &self.planner
        } else {
            &self.arbiter
        }
    }
}

/// Which CLI runner the daemon uses for all spawned agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RunnerKind {
    Claude,
    Codex,
    Grok,
    /// Operator-declared external CLI runner.
    External,
}

impl std::fmt::Display for RunnerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claude => write!(f, "claude"),
            Self::Codex => write!(f, "codex"),
            Self::Grok => write!(f, "grok"),
            Self::External => write!(f, "external"),
        }
    }
}

impl RunnerKind {
    /// The launch adapter kind for this configured runner.
    pub fn agent_kind(self) -> crate::serve::runner::AgentKind {
        match self {
            Self::Claude => crate::serve::runner::AgentKind::Claude,
            Self::Codex => crate::serve::runner::AgentKind::Codex,
            Self::Grok => crate::serve::runner::AgentKind::Grok,
            Self::External => crate::serve::runner::AgentKind::External,
        }
    }

    pub fn from_str_opt(s: Option<&str>) -> Result<Self> {
        match s {
            None | Some("claude") => Ok(Self::Claude),
            Some("codex") => Ok(Self::Codex),
            Some("grok") => Ok(Self::Grok),
            Some("external") => Ok(Self::External),
            Some(other) => Err(QuorumError::Usage(format!(
                "bad agent value: \"{other}\" (expected \"claude\", \"codex\", \"grok\", or \"external\")"
            ))),
        }
    }
}

/// Token counters used by the managed-agent token watchdog ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TokenLimitBasis {
    /// Provider-reported input plus output tokens; the historical behavior.
    #[default]
    Raw,
    /// Normalized uncached input plus output tokens.
    Uncached,
}

impl std::fmt::Display for TokenLimitBasis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raw => write!(f, "raw"),
            Self::Uncached => write!(f, "uncached"),
        }
    }
}

impl TokenLimitBasis {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "raw" => Ok(Self::Raw),
            "uncached" => Ok(Self::Uncached),
            other => Err(QuorumError::Usage(format!(
                "token_limit_basis must be \"raw\" or \"uncached\", got \"{other}\""
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
        #[serde(default, deny_unknown_fields)]
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
    /// Root of per-task session directories. Absolute. Default:
    /// `<state dir>/sessions`. Restart-only: see spec "Sessions" > "Layout".
    session_base: Option<String>,
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
    token_limit_basis: Option<String>,
    max_turn_cost_usd: Option<f64>,
    max_task_cost_usd: Option<f64>,
    max_turn_wall_secs: Option<u64>,
    max_idle_secs: Option<u64>,
    max_busy_idle_secs: Option<u64>,
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
    /// Directed source/target branch pairs eligible for daemon branch sync.
    /// Empty is the safe default: no pair can be enqueued until an owner opts in.
    sync_pairs: Vec<[String; 2]>,
    self_update_branch: Option<String>,
    merge_checks_timeout_secs: Option<u64>,
    merge_checks_poll_secs: Option<u64>,
    required_jobs: Option<Vec<String>>,
    master_ci_gate: Option<bool>,
    master_ci_timeout_secs: Option<u64>,
    doctor_enabled: Option<bool>,
    /// Cadence for observational host memory/disk sampling.
    resource_poll_secs: Option<u64>,
    disk_warn_free_gib: Option<u64>,
    disk_critical_free_gib: Option<u64>,
    memory_warn_available_pct: Option<u8>,
    memory_critical_available_pct: Option<u8>,
    /// Whether deterministic R2 sampling participates. `false` keeps R2 mandatory.
    r2_enabled: Option<bool>,
    /// Guaranteed coverage floor per (model, effort, complexity) stratum.
    r2_target_per_stratum: Option<i64>,
    /// Sampling probability once a stratum reaches its coverage floor.
    r2_steady_state_p: Option<f64>,
    /// Maximum rework rounds before a task fails. Stamped onto each task at
    /// adoption, immutable thereafter; unset falls back to the compiled default.
    max_rework: Option<u32>,
    /// Children of one decomposition graph that may be `working` at once.
    /// Unset defaults to `cap`; values above `cap` are bounded by `cap`.
    max_graph_children_working: Option<i64>,
    /// Concurrent reviewer processes the daemon may run. Unset defaults to
    /// `max(cap, 2)`; values outside `1..=32` are bad config.
    reviewer_cap: Option<i64>,
    /// Graphs that may be active or planning at once across the daemon.
    /// Unset defaults to 2; values below 1 are bad config.
    max_active_graphs: Option<i64>,
    /// Runner-specific Codex configuration.
    codex: Option<CodexFileConfig>,
    /// Grok adapter configuration for managed worker roles.
    grok: Option<GrokFileConfig>,
    /// Socket address of the daemon-embedded agent MCP listener. Unset binds
    /// loopback on an ephemeral port (`127.0.0.1:0`).
    agent_mcp_listen: Option<std::net::SocketAddr>,
    /// `Host` names agent MCP requests may carry, by orchestrator MCP's
    /// `--allowed-host` rules. The only way to permit a non-loopback bind.
    agent_mcp_allowed_hosts: Option<Vec<String>>,
    /// Declared listen address of the separately run `orchestrator-mcp`.
    /// Serve never runs it; the address only lets startup refuse an agent MCP
    /// listener on the same socket address.
    orchestrator_mcp_listen: Option<std::net::SocketAddr>,
    /// The one external CLI declaration. Restart-only.
    external_runner: Option<crate::external_runner_spec::ExternalRunnerFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigKeyDisposition {
    Runtime,
    /// Consumed at startup only. A reload that changes it is refused and the
    /// running configuration stays in force.
    RestartOnly,
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
    ("session_base", ConfigKeyDisposition::RestartOnly),
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
    ("token_limit_basis", ConfigKeyDisposition::Runtime),
    ("max_turn_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_task_cost_usd", ConfigKeyDisposition::Runtime),
    ("max_turn_wall_secs", ConfigKeyDisposition::Runtime),
    ("max_idle_secs", ConfigKeyDisposition::Runtime),
    ("max_busy_idle_secs", ConfigKeyDisposition::Runtime),
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
    ("sync_pairs", ConfigKeyDisposition::Runtime),
    ("self_update_branch", ConfigKeyDisposition::Runtime),
    ("merge_checks_timeout_secs", ConfigKeyDisposition::Runtime),
    ("merge_checks_poll_secs", ConfigKeyDisposition::Runtime),
    ("required_jobs", ConfigKeyDisposition::Runtime),
    ("master_ci_gate", ConfigKeyDisposition::Runtime),
    ("master_ci_timeout_secs", ConfigKeyDisposition::Runtime),
    ("doctor_enabled", ConfigKeyDisposition::Runtime),
    ("resource_poll_secs", ConfigKeyDisposition::Runtime),
    ("disk_warn_free_gib", ConfigKeyDisposition::Runtime),
    ("disk_critical_free_gib", ConfigKeyDisposition::Runtime),
    ("memory_warn_available_pct", ConfigKeyDisposition::Runtime),
    (
        "memory_critical_available_pct",
        ConfigKeyDisposition::Runtime,
    ),
    ("r2_enabled", ConfigKeyDisposition::Runtime),
    ("r2_target_per_stratum", ConfigKeyDisposition::Runtime),
    ("r2_steady_state_p", ConfigKeyDisposition::Runtime),
    ("max_rework", ConfigKeyDisposition::Runtime),
    ("max_graph_children_working", ConfigKeyDisposition::Runtime),
    ("reviewer_cap", ConfigKeyDisposition::Runtime),
    ("max_active_graphs", ConfigKeyDisposition::Runtime),
    ("codex", ConfigKeyDisposition::Runtime),
    ("grok", ConfigKeyDisposition::Runtime),
    ("agent_mcp_listen", ConfigKeyDisposition::Runtime),
    ("agent_mcp_allowed_hosts", ConfigKeyDisposition::Runtime),
    ("orchestrator_mcp_listen", ConfigKeyDisposition::Runtime),
    ("external_runner", ConfigKeyDisposition::RestartOnly),
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
    let provider_explicit = provider_name.is_some();

    let (
        worker_default_model,
        worker_default_effort,
        review_default_model,
        review_default_effort,
        classifier_default_model,
        classifier_default_effort,
    ) = match (provider_explicit, provider) {
        (true, RunnerKind::Codex) => (
            "gpt-5.6-terra",
            "medium",
            "gpt-5.6-terra",
            "high",
            "gpt-5.6-luna",
            "medium",
        ),
        // Provider selection controls workers only for Grok. Reviewer,
        // classifier, and collector roles retain their non-Grok defaults.
        (true, RunnerKind::Grok) => (
            "grok-4.5",
            "high",
            legacy_model,
            legacy_effort,
            "claude-haiku-4-5-20251001",
            "low",
        ),
        _ => (
            legacy_model,
            legacy_effort,
            legacy_model,
            legacy_effort,
            "claude-haiku-4-5-20251001",
            "low",
        ),
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
    // An external provider is its own runner authority: its role models are
    // opaque strings passed to the CLI verbatim and never resolved through
    // the built-in model mapping. Built-in providers keep deriving the kind
    // from the model so their mismatch checks are unchanged.
    let role_kind = |model: &str| -> std::result::Result<crate::serve::runner::AgentKind, String> {
        if provider == RunnerKind::External {
            Ok(crate::serve::runner::AgentKind::External)
        } else {
            crate::serve::runner::AgentKind::for_model(model)
        }
    };
    for (role, model) in [
        ("review", roles.review_model.as_str()),
        ("classifier", roles.classifier_model.as_str()),
        ("collector", roles.collector_model.as_str()),
    ] {
        if role_kind(model).is_ok_and(|kind| kind == crate::serve::runner::AgentKind::Grok) {
            return Err(QuorumError::Usage(format!(
                "{role}_model \"{model}\" selects Grok, but Grok is enabled only for managed workers"
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
            let actual = role_kind(model).map_err(QuorumError::Usage)?;
            if (provider == RunnerKind::Grok && role != "worker")
                || (role == "worker" && actual == crate::serve::runner::AgentKind::Grok)
            {
                continue;
            }
            let expected = provider.agent_kind();
            if actual != expected {
                return Err(QuorumError::Usage(format!(
                    "{role}_model \"{model}\" does not match provider=\"{provider}\""
                )));
            }
        }

        role_kind(&roles.review_model).map_err(QuorumError::Usage)?;
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

impl Default for GrokResolvedConfig {
    fn default() -> Self {
        Self {
            sandbox: crate::serve::grok_agent::DEFAULT_SANDBOX.into(),
            permission_mode: crate::serve::grok_agent::DEFAULT_PERMISSION_MODE.into(),
            max_turns: crate::serve::grok_agent::DEFAULT_MAX_TURNS,
        }
    }
}

/// The default agent MCP bind: loopback, kernel-assigned port.
pub const DEFAULT_AGENT_MCP_LISTEN: std::net::SocketAddr =
    std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

/// Validated bind settings for the daemon-embedded agent MCP listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpServeConfig {
    pub listen: std::net::SocketAddr,
    /// Operator-configured hosts, plus the IP literal of
    /// [`agent_mcp_client_addr`] when the bind would not otherwise admit it,
    /// so the exported `QUORUM_AGENT_MCP_URL` always passes the listener's
    /// `Host` check. The loopback names are added at bind for a loopback
    /// address.
    pub allowed_hosts: Vec<String>,
}

/// The address managed runs dial to reach an agent MCP listener bound at
/// `bound`. A wildcard (unspecified) bind is reached over the loopback
/// address of the same family; any other bind is reached at its own address.
pub fn agent_mcp_client_addr(bound: std::net::SocketAddr) -> std::net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let ip = match bound.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    std::net::SocketAddr::new(ip, bound.port())
}

/// The `QUORUM_AGENT_MCP_URL` exported to managed runs for a listener bound
/// at `bound`. Its authority is [`agent_mcp_client_addr`], which
/// [`resolve_agent_mcp`] guarantees the listener's allowed hosts admit.
pub fn agent_mcp_url(bound: std::net::SocketAddr) -> String {
    format!("http://{}/mcp", agent_mcp_client_addr(bound))
}

/// An IP as a `Host` authority without a port (IPv6 bracketed).
fn ip_host_literal(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(ip) => ip.to_string(),
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

/// Resolve the agent MCP listener settings. Refused with a usage error (exit
/// 2): an address equal to the declared orchestrator MCP address, since the
/// two listeners never share a socket, and a non-loopback bind without an
/// explicit allowed host. There is no implicit wildcard bind.
///
/// The operator's hosts are what opt a non-loopback bind in; the daemon then
/// also admits the IP literal its own managed runs dial, so the exported URL
/// is never refused with 403. An IP literal is not a DNS-rebinding vector,
/// and the run capability bearer, not the `Host` header, is the authority.
pub fn resolve_agent_mcp(cfg: &ServeFileConfig) -> Result<AgentMcpServeConfig> {
    let listen = cfg.agent_mcp_listen.unwrap_or(DEFAULT_AGENT_MCP_LISTEN);
    let mut allowed_hosts = cfg.agent_mcp_allowed_hosts.clone().unwrap_or_default();
    if cfg.orchestrator_mcp_listen == Some(listen) {
        return Err(QuorumError::Usage(format!(
            "agent_mcp_listen {listen} equals orchestrator_mcp_listen: the agent MCP and \
             orchestrator MCP must use separate listeners"
        )));
    }
    let admitted = crate::agent_mcp::agent_allowed_hosts(listen, allowed_hosts.clone())?;
    let client_ip = agent_mcp_client_addr(listen).ip();
    let client_host = ip_host_literal(client_ip);
    let normalized = |host: &str| host.trim_matches(['[', ']']).to_ascii_lowercase();
    if !admitted
        .iter()
        .any(|host| normalized(host) == normalized(&client_host))
    {
        allowed_hosts.push(client_host.clone());
        crate::agent_mcp::agent_allowed_hosts(listen, allowed_hosts.clone()).map_err(|error| {
            QuorumError::Usage(format!(
                "{error}; the daemon also admits {client_host}, the address its managed runs \
                 use to reach agent_mcp_listen {listen}"
            ))
        })?;
    }
    Ok(AgentMcpServeConfig {
        listen,
        allowed_hosts,
    })
}

pub fn resolve_grok_adapter(file: Option<&GrokFileConfig>) -> Result<GrokResolvedConfig> {
    let defaults = GrokResolvedConfig::default();
    let resolved = GrokResolvedConfig {
        sandbox: file
            .and_then(|config| config.sandbox.clone())
            .unwrap_or(defaults.sandbox),
        permission_mode: file
            .and_then(|config| config.permission_mode.clone())
            .unwrap_or(defaults.permission_mode),
        max_turns: file
            .and_then(|config| config.max_turns)
            .unwrap_or(defaults.max_turns),
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
            validate_sync_pairs(&cfg.sync_pairs)?;
            resolve_resource_monitor_config(&cfg)?;
            resolve_external_runner(&cfg)?;
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

/// Default subdirectory name for `session_base` when resolved from the state
/// directory (the per-repository directory holding `quorum.db`).
const SESSION_BASE_DIR_NAME: &str = "sessions";

/// Resolve and validate `session_base`: the root of per-task session
/// directories (spec "Sessions" > "Layout"). Default: `<state_dir>/sessions`,
/// which sits inside the state directory on purpose, so that a bare config
/// keeps working unchanged. Refused (exit 2): not absolute; equal to or
/// inside `worktree_base`; or containing `worktree_base` or `state_dir`.
/// Creates the directory with mode 0700 when missing; refuses a symlink or a
/// non-directory left at that path. `session_base` is restart-only — see
/// `SERVE_FILE_CONFIG_KEY_REGISTRY`.
pub fn resolve_session_base(
    configured: Option<&str>,
    state_dir: &Path,
    worktree_base: &Path,
) -> Result<std::path::PathBuf> {
    let session_base = match configured {
        Some(value) => std::path::PathBuf::from(value),
        None => state_dir.join(SESSION_BASE_DIR_NAME),
    };
    if !session_base.is_absolute() {
        return Err(QuorumError::Usage(format!(
            "session_base must be an absolute path, got \"{}\"",
            session_base.display()
        )));
    }
    if path_contains(&session_base, worktree_base) || path_contains(worktree_base, &session_base) {
        return Err(QuorumError::Usage(format!(
            "session_base \"{}\" must not equal, be inside, or contain worktree_base \"{}\"",
            session_base.display(),
            worktree_base.display()
        )));
    }
    if path_contains(&session_base, state_dir) {
        return Err(QuorumError::Usage(format!(
            "session_base \"{}\" must not contain the state directory \"{}\"",
            session_base.display(),
            state_dir.display()
        )));
    }
    create_session_base_dir(&session_base)?;
    Ok(session_base)
}

/// True when `ancestor` lexically equals or contains `path`, compared
/// component-by-component without touching the filesystem.  `session_base`
/// may not exist yet, so this cannot canonicalize either side.
fn path_contains(ancestor: &Path, path: &Path) -> bool {
    let ancestor: Vec<_> = ancestor.components().collect();
    let path: Vec<_> = path.components().collect();
    ancestor.len() <= path.len() && ancestor.iter().zip(path.iter()).all(|(a, b)| a == b)
}

/// Create `session_base` with mode 0700 if missing; refuse a symlink or a
/// non-directory already at that path.
fn create_session_base_dir(session_base: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::symlink_metadata(session_base) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(QuorumError::Usage(format!(
                    "session_base \"{}\" must not be a symlink",
                    session_base.display()
                )));
            }
            if !metadata.is_dir() {
                return Err(QuorumError::Usage(format!(
                    "session_base \"{}\" must be a directory",
                    session_base.display()
                )));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(session_base).map_err(|error| {
                QuorumError::Io(format!(
                    "failed to create session_base \"{}\": {error}",
                    session_base.display()
                ))
            })?;
            std::fs::set_permissions(session_base, std::fs::Permissions::from_mode(0o700)).map_err(
                |error| {
                    QuorumError::Io(format!(
                        "failed to set session_base \"{}\" permissions: {error}",
                        session_base.display()
                    ))
                },
            )
        }
        Err(error) => Err(QuorumError::Io(format!(
            "failed to inspect session_base \"{}\": {error}",
            session_base.display()
        ))),
    }
}

/// The validated `[external_runner]` declaration and its fingerprint, or
/// `None` when the table is absent. Every table-local rule is a usage error.
pub fn resolve_external_runner(
    cfg: &ServeFileConfig,
) -> Result<Option<crate::external_runner_spec::ExternalSpec>> {
    cfg.external_runner
        .as_ref()
        .map(|file| {
            crate::external_runner_spec::ExternalSpec::from_file(file, cfg.agent_bin.as_deref())
        })
        .transpose()
}

/// Replace `running` with the config at `path` only when the candidate passes
/// every startup rule and leaves every restart-only key unchanged. On any
/// refusal `running` is untouched. The daemon has no live reload trigger yet;
/// this is the one gate any reload path must use.
#[cfg_attr(not(test), allow(dead_code))]
pub fn reload(running: &mut ServeFileConfig, path: &Path) -> Result<()> {
    let candidate = load(path, true)?;
    for (key, disposition) in SERVE_FILE_CONFIG_KEY_REGISTRY {
        if *disposition == ConfigKeyDisposition::RestartOnly
            && restart_only_key_changed(key, running, &candidate)
        {
            return Err(QuorumError::Usage(format!(
                "serve config key \"{key}\" is restart-only; the running configuration stays in force until restart"
            )));
        }
    }
    *running = candidate;
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn restart_only_key_changed(
    key: &str,
    running: &ServeFileConfig,
    candidate: &ServeFileConfig,
) -> bool {
    match key {
        "session_base" => running.session_base != candidate.session_base,
        "external_runner" => {
            let filled =
                |cfg: &ServeFileConfig| cfg.external_runner.as_ref().map(|f| f.with_defaults());
            filled(running) != filled(candidate)
        }
        // A restart-only key without a comparison is treated as changed.
        _ => true,
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
        let expected = RunnerKind::from_str_opt(Some(&profile.runner))?.agent_kind();
        // External profiles take their runner from the profile alone; the
        // model string is passed to the CLI verbatim and never resolved.
        let actual = if expected == crate::serve::runner::AgentKind::External {
            expected
        } else {
            crate::serve::runner::AgentKind::for_model(&profile.model)
                .map_err(|error| QuorumError::Usage(format!("model profile \"{name}\": {error}")))?
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
            crate::serve::runner::AgentKind::Grok => {
                matches!(profile.effort.as_str(), "low" | "medium" | "high")
            }
            crate::serve::runner::AgentKind::External => external_effort_is_valid(&profile.effort),
        };
        if !effort_supported && expected == crate::serve::runner::AgentKind::External {
            return Err(QuorumError::Usage(format!(
                "model_profiles.{name}.effort \"{}\" must match [a-z0-9_-]{{1,32}} for runner \"external\"",
                profile.effort
            )));
        }
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
    validate_percentage_pool("arbiter", routing.arbiter_pool(), profiles)?;
    validate_percentage_pool("collector", &routing.collector, profiles)?;
    validate_complexity_pools("worker", &routing.worker, profiles)?;
    validate_complexity_pools("reviewer", &routing.reviewer, profiles)?;
    validate_grok_role_gate("classifier", &routing.classifier, profiles)?;
    validate_grok_role_gate("planner", &routing.planner, profiles)?;
    validate_grok_role_gate("arbiter", routing.arbiter_pool(), profiles)?;
    validate_grok_role_gate("collector", &routing.collector, profiles)?;
    for (complexity, pool) in &routing.reviewer {
        validate_grok_role_gate(&format!("reviewer.{complexity}"), pool, profiles)?;
    }
    validate_external_routing(config, profiles, routing)?;
    resolve_token_limit_basis(config.token_limit_basis.as_deref())?;
    validate_token_ceilings(config.max_turn_tokens, config.max_task_tokens)?;
    validate_routed_cost_limits(config, config.max_turn_cost_usd, config.max_task_cost_usd)?;
    Ok(())
}

/// Cross-config rules for `runner = "external"` profiles (spec
/// `docs/2026-11-20-external-cli-runner-technical-spec.md`, "Validation",
/// "Access levels", "agent_bin"). A config without external profiles passes
/// untouched.
fn validate_external_routing(
    config: &ServeFileConfig,
    profiles: &BTreeMap<String, ModelProfile>,
    routing: &RoutingPolicy,
) -> Result<()> {
    use crate::external_runner_spec::{
        check_external_profile, check_routed_role, AccessLevel, RoleLaunch,
    };
    let is_external = |id: &str| profiles.get(id).is_some_and(|p| p.runner == "external");
    for (name, profile) in profiles.iter().filter(|(name, _)| is_external(name)) {
        check_external_profile(
            config.external_runner.as_ref(),
            config.agent_bin.as_deref(),
            name,
            &profile.effort,
        )?;
    }
    // Spec "Access levels": managed worker runs are read-write with the agent
    // MCP; reviewer and planner runs are read-only with it; classifier,
    // collector, and arbiter runs are restricted without it.
    const WORKER: RoleLaunch = RoleLaunch {
        access: AccessLevel::ReadWrite,
        agent_mcp: true,
        final_text: false,
    };
    const READ_ONLY_MCP: RoleLaunch = RoleLaunch {
        access: AccessLevel::ReadOnly,
        agent_mcp: true,
        final_text: false,
    };
    const RESTRICTED: RoleLaunch = RoleLaunch {
        access: AccessLevel::Restricted,
        agent_mcp: false,
        final_text: false,
    };
    const RESTRICTED_FINAL_TEXT: RoleLaunch = RoleLaunch {
        final_text: true,
        ..RESTRICTED
    };
    let mut pools: Vec<(String, &PercentagePool, RoleLaunch)> = vec![
        (
            "classifier".into(),
            &routing.classifier,
            RESTRICTED_FINAL_TEXT,
        ),
        ("planner".into(), &routing.planner, READ_ONLY_MCP),
        (
            "arbiter".into(),
            routing.arbiter_pool(),
            RESTRICTED_FINAL_TEXT,
        ),
        ("collector".into(), &routing.collector, RESTRICTED),
    ];
    pools.extend(
        routing
            .worker
            .iter()
            .map(|(level, pool)| (format!("worker.{level}"), pool, WORKER)),
    );
    pools.extend(
        routing
            .reviewer
            .iter()
            .map(|(level, pool)| (format!("reviewer.{level}"), pool, READ_ONLY_MCP)),
    );
    for (role, pool, launch) in pools {
        for profile_id in pool.keys().filter(|id| is_external(id)) {
            // check_external_profile already refused a missing declaration.
            if let Some(decl) = config.external_runner.as_ref() {
                check_routed_role(decl, &role, profile_id, launch)?;
            }
        }
    }
    Ok(())
}

/// Grok Build is available only to managed worker pools. Keeping this check at
/// the routing boundary means a valid Grok profile cannot accidentally become
/// selectable by a planner, reviewer, classifier, or collector.
fn validate_grok_role_gate(
    role: &str,
    pool: &PercentagePool,
    profiles: &BTreeMap<String, ModelProfile>,
) -> Result<()> {
    if let Some((profile_id, _)) = pool.iter().find(|(profile_id, _)| {
        profiles
            .get(*profile_id)
            .is_some_and(|profile| profile.runner == "grok")
    }) {
        return Err(QuorumError::Usage(format!(
            "routing.{role} profile \"{profile_id}\" selects Grok, which is enabled only for worker roles"
        )));
    }
    Ok(())
}

/// A custom executable belongs to one provider CLI. Validate it after every
/// config source has been resolved, because `--agent-bin` is merged after the
/// TOML file itself has been validated.
/// External-profile effort syntax: `[a-z0-9_-]{1,32}`.
fn external_effort_is_valid(effort: &str) -> bool {
    (1..=32).contains(&effort.len())
        && effort
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

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
        && referenced_profile_ids(routing)
            .any(|profile_id| matches!(profiles[profile_id].runner.as_str(), "codex" | "grok"))
    {
        return Err(QuorumError::Usage(
            "USD cost limits are unsupported when routing can select a Codex or Grok profile"
                .into(),
        ));
    }
    // Spec "Validation": the external runner reports no cost.
    let key = if max_turn_cost_usd.is_some() {
        "max_turn_cost_usd"
    } else {
        "max_task_cost_usd"
    };
    if let Some(profile_id) = referenced_profile_ids(routing)
        .find(|profile_id| profiles[*profile_id].runner == "external")
        .filter(|_| max_turn_cost_usd.is_some() || max_task_cost_usd.is_some())
    {
        return Err(QuorumError::Usage(format!(
            "serve config {key}: USD cost limits are unsupported when routing can select external profile \"{profile_id}\"; the external runner reports no cost"
        )));
    }
    Ok(())
}

fn referenced_profile_ids(routing: &RoutingPolicy) -> impl Iterator<Item = &String> {
    routing
        .classifier
        .keys()
        .chain(routing.planner.keys())
        .chain(routing.arbiter_pool().keys())
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

/// Validate the configured rework ceiling. A cap of zero would fail every task
/// on its first requested change, so it is rejected as a configuration error.
pub fn validate_max_rework(max_rework: u32) -> Result<()> {
    if max_rework == 0 {
        return Err(QuorumError::Usage("max_rework must be >= 1 (got 0)".into()));
    }
    Ok(())
}

/// Resolve the per-graph working-children limit. Unset defaults to the worker
/// `cap`; zero or negative values are bad config. Values above `cap` are kept
/// as configured: the worker cap already bounds concurrent claims.
pub fn resolve_max_graph_children_working(configured: Option<i64>, cap: usize) -> Result<usize> {
    match configured {
        None => Ok(cap.max(1)),
        Some(limit) if limit < 1 => Err(QuorumError::Usage(format!(
            "max_graph_children_working must be >= 1 (got {limit})"
        ))),
        Some(limit) => Ok(usize::try_from(limit).unwrap_or(usize::MAX)),
    }
}

/// Worker `cap` used when neither `--cap` nor the serve config sets one.
pub const DEFAULT_CAP: usize = 4;

/// Effective per-graph working-children limit as resolved from a serve config
/// file alone, for read-only consumers (`quorum web`) that must agree with the
/// daemon's dispatch predicate. A `--cap` passed only on the `serve` command
/// line is not visible here.
pub fn file_graph_children_working_limit(cfg: &ServeFileConfig) -> Result<i64> {
    let limit = resolve_max_graph_children_working(
        cfg.max_graph_children_working,
        cfg.cap.unwrap_or(DEFAULT_CAP),
    )?;
    Ok(i64::try_from(limit).unwrap_or(i64::MAX))
}

/// Inclusive bounds for `reviewer_cap`.
pub const REVIEWER_CAP_MIN: i64 = 1;
pub const REVIEWER_CAP_MAX: i64 = 32;

/// Resolve the daemon-wide concurrent reviewer-process bound. Unset defaults
/// to `max(cap, 2)` so a single-worker daemon can still run an R1 and an R2
/// side by side; values outside `1..=32` are bad config.
pub fn resolve_reviewer_cap(configured: Option<i64>, cap: usize) -> Result<usize> {
    match configured {
        None => Ok(cap.max(2)),
        Some(limit) if !(REVIEWER_CAP_MIN..=REVIEWER_CAP_MAX).contains(&limit) => {
            Err(QuorumError::Usage(format!(
                "reviewer_cap must be between {REVIEWER_CAP_MIN} and {REVIEWER_CAP_MAX} (got {limit})"
            )))
        }
        // In range 1..=32, so the conversion cannot fail.
        Some(limit) => Ok(usize::try_from(limit).unwrap_or(1)),
    }
}

/// Effective reviewer cap as resolved from a serve config file alone, for
/// read-only consumers that must agree with the daemon's dispatch gate. A
/// `--cap` passed only on the `serve` command line is not visible here.
pub fn file_reviewer_cap(cfg: &ServeFileConfig) -> Result<i64> {
    let limit = resolve_reviewer_cap(cfg.reviewer_cap, cfg.cap.unwrap_or(DEFAULT_CAP))?;
    Ok(i64::try_from(limit).unwrap_or(i64::MAX))
}

/// Default `max_active_graphs` when the key is absent.
pub const DEFAULT_MAX_ACTIVE_GRAPHS: i64 = 2;

/// Resolve the daemon-wide cap on graphs that may be active or planning at
/// once. Unset defaults to 2; zero or negative values are bad config.
pub fn resolve_max_active_graphs(configured: Option<i64>) -> Result<usize> {
    match configured {
        None => Ok(DEFAULT_MAX_ACTIVE_GRAPHS as usize),
        Some(limit) if limit < 1 => Err(QuorumError::Usage(format!(
            "max_active_graphs must be >= 1 (got {limit})"
        ))),
        Some(limit) => Ok(usize::try_from(limit).unwrap_or(usize::MAX)),
    }
}

/// Validate the owner-configured directed branch-sync pairs. The same branch
/// rules as task targets apply, and self-sync is never meaningful.
pub fn validate_sync_pairs(sync_pairs: &[[String; 2]]) -> Result<()> {
    for [from, to] in sync_pairs {
        quorum_core::tasks::validate_target_branch(from)?;
        quorum_core::tasks::validate_target_branch(to)?;
        if from == to {
            return Err(QuorumError::Usage(format!(
                "sync_pairs entry {from:?} -> {to:?} must name two distinct branches"
            )));
        }
    }
    Ok(())
}

/// Resolve and validate the daemon's observational host-resource monitor.
pub fn resolve_resource_monitor_config(
    file: &ServeFileConfig,
) -> Result<crate::resource_health::ResourceMonitorConfig> {
    use crate::resource_health::{
        ResourceMonitorConfig, DEFAULT_DISK_CRITICAL_FREE_GIB, DEFAULT_DISK_WARN_FREE_GIB,
        DEFAULT_MEMORY_CRITICAL_AVAILABLE_PCT, DEFAULT_MEMORY_WARN_AVAILABLE_PCT,
        DEFAULT_RESOURCE_POLL_SECS, MAX_RESOURCE_POLL_SECS, MIN_RESOURCE_POLL_SECS,
    };

    let resolved = ResourceMonitorConfig {
        poll_secs: file
            .resource_poll_secs
            .unwrap_or(DEFAULT_RESOURCE_POLL_SECS),
        disk_warn_free_gib: file
            .disk_warn_free_gib
            .unwrap_or(DEFAULT_DISK_WARN_FREE_GIB),
        disk_critical_free_gib: file
            .disk_critical_free_gib
            .unwrap_or(DEFAULT_DISK_CRITICAL_FREE_GIB),
        memory_warn_available_pct: file
            .memory_warn_available_pct
            .unwrap_or(DEFAULT_MEMORY_WARN_AVAILABLE_PCT),
        memory_critical_available_pct: file
            .memory_critical_available_pct
            .unwrap_or(DEFAULT_MEMORY_CRITICAL_AVAILABLE_PCT),
    };
    if !(MIN_RESOURCE_POLL_SECS..=MAX_RESOURCE_POLL_SECS).contains(&resolved.poll_secs) {
        return Err(QuorumError::Usage(format!(
            "resource_poll_secs must be between {MIN_RESOURCE_POLL_SECS} and {MAX_RESOURCE_POLL_SECS}, got {}",
            resolved.poll_secs
        )));
    }
    if resolved.disk_critical_free_gib == 0
        || resolved.disk_critical_free_gib >= resolved.disk_warn_free_gib
    {
        return Err(QuorumError::Usage(format!(
            "disk_critical_free_gib must be greater than zero and less than disk_warn_free_gib (got critical={}, warn={})",
            resolved.disk_critical_free_gib, resolved.disk_warn_free_gib
        )));
    }
    if resolved.memory_critical_available_pct == 0
        || resolved.memory_warn_available_pct > 100
        || resolved.memory_critical_available_pct >= resolved.memory_warn_available_pct
    {
        return Err(QuorumError::Usage(format!(
            "memory_critical_available_pct must be greater than zero and less than memory_warn_available_pct, and warning must be at most 100 (got critical={}, warn={})",
            resolved.memory_critical_available_pct, resolved.memory_warn_available_pct
        )));
    }
    Ok(resolved)
}

/// Resource settings needed by a read-only status process. Unlike daemon
/// startup, this does not require or validate model routing: status remains
/// usable while the daemon is absent. Unknown/malformed TOML still fails loud
/// to callers, which may choose the documented fail-open defaults.
pub struct StatusResourceConfig {
    pub monitor: crate::resource_health::ResourceMonitorConfig,
    pub worktree_base: Option<std::path::PathBuf>,
}

pub fn status_resource_config(repo: &str) -> Result<StatusResourceConfig> {
    status_resource_config_at(&default_config_path(repo)?)
}

fn status_resource_config_at(path: &Path) -> Result<StatusResourceConfig> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StatusResourceConfig {
                monitor: crate::resource_health::ResourceMonitorConfig::default(),
                worktree_base: None,
            });
        }
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "cannot read serve config {}: {error}",
                path.display()
            )));
        }
    };
    let file: ServeFileConfig = toml::from_str(&contents).map_err(|error| {
        QuorumError::Usage(format!("bad serve config {}: {error}", path.display()))
    })?;
    let monitor = resolve_resource_monitor_config(&file)?;
    Ok(StatusResourceConfig {
        monitor,
        worktree_base: file.worktree_base.map(std::path::PathBuf::from),
    })
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

/// Idle ceiling applied instead of `max_idle_secs` while a managed agent has
/// an outstanding tool call or background task (long test suites run by the
/// provider in the background). Always finite: a hung tool is still reaped.
pub const DEFAULT_MAX_BUSY_IDLE_SECS: u64 = 3600;
/// Upper bound for `max_busy_idle_secs`; "busy" must never mean "immortal".
pub const MAX_BUSY_IDLE_SECS_CEILING: u64 = 86_400;

pub fn validate_busy_idle_secs(seconds: u64) -> std::result::Result<(), String> {
    if seconds == 0 || seconds > MAX_BUSY_IDLE_SECS_CEILING {
        return Err(format!(
            "max_busy_idle_secs must be between 1 and {MAX_BUSY_IDLE_SECS_CEILING} seconds"
        ));
    }
    Ok(())
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

/// Resolve the modern idle ceiling and its legacy turn-wall alias without
/// losing the normal CLI-over-file precedence between the two names.
pub fn resolve_idle_limit<T: Copy>(
    max_idle_flag: Option<T>,
    max_turn_wall_flag: Option<T>,
    max_idle_file: Option<T>,
    max_turn_wall_file: Option<T>,
) -> Sourced<Option<T>> {
    for (value, source) in [
        (max_idle_flag, Source::Flag),
        (max_turn_wall_flag, Source::Flag),
        (max_idle_file, Source::File),
        (max_turn_wall_file, Source::File),
    ] {
        if value.is_some() {
            return Sourced { value, source };
        }
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

/// Resolve the file-only token watchdog accounting policy. Missing preserves
/// the historical raw provider input + output behavior.
pub fn resolve_token_limit_basis(file: Option<&str>) -> Result<Sourced<TokenLimitBasis>> {
    match file {
        Some(value) => Ok(Sourced {
            value: TokenLimitBasis::parse(value)?,
            source: Source::File,
        }),
        None => Ok(Sourced {
            value: TokenLimitBasis::Raw,
            source: Source::Default,
        }),
    }
}

/// Reject unusable ceilings before the daemon can claim work.
pub fn validate_token_ceilings(
    max_turn_tokens: Option<i64>,
    max_task_tokens: Option<i64>,
) -> Result<()> {
    for (name, value) in [
        ("max_turn_tokens", max_turn_tokens),
        ("max_task_tokens", max_task_tokens),
    ] {
        if let Some(value) = value.filter(|value| *value <= 0) {
            return Err(QuorumError::Usage(format!(
                "{name} must be greater than zero, got {value}"
            )));
        }
    }
    Ok(())
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
    pub self_update_branch: &'a Sourced<String>,
    pub cap: &'a Sourced<usize>,
    pub reviewer_cap: &'a Sourced<usize>,
    pub model_profiles: &'a BTreeMap<String, ModelProfile>,
    pub routing: &'a RoutingPolicy,
    pub log_dir: &'a Sourced<Option<String>>,
    pub no_bare_agent: &'a Sourced<bool>,
    pub self_update_drain: &'a Sourced<bool>,
    pub drain_timeout_secs: &'a Sourced<u64>,
    pub max_idle_secs: &'a Sourced<Option<u64>>,
    pub max_busy_idle_secs: &'a Sourced<u64>,
    pub max_task_wall_secs: &'a Sourced<Option<u64>>,
    pub idle_timeout_secs: &'a Sourced<Option<u64>>,
    pub max_turn_tokens: &'a Sourced<Option<i64>>,
    pub max_task_tokens: &'a Sourced<Option<i64>>,
    pub token_limit_basis: &'a Sourced<TokenLimitBasis>,
    pub max_turn_cost_usd: &'a Sourced<Option<f64>>,
    pub max_task_cost_usd: &'a Sourced<Option<f64>>,
    pub merge_checks_timeout_secs: &'a Sourced<u64>,
    pub required_jobs: &'a [String],
    pub master_ci_gate: &'a Sourced<bool>,
    pub master_ci_timeout_secs: &'a Sourced<u64>,
    pub doctor_enabled: &'a Sourced<bool>,
    pub resource_poll_secs: &'a Sourced<u64>,
    pub disk_warn_free_gib: &'a Sourced<u64>,
    pub disk_critical_free_gib: &'a Sourced<u64>,
    pub memory_warn_available_pct: &'a Sourced<u8>,
    pub memory_critical_available_pct: &'a Sourced<u8>,
}

/// Format the startup banner showing resolved config + sources.
/// Idle limit as the runtime resolves it (serve/mod.rs): `max_idle_secs`, then
/// the legacy `idle_timeout_secs`, then 900. Returns the value and its origin.
fn effective_idle_limit(
    max_idle_secs: Option<u64>,
    idle_timeout_secs: Option<u64>,
) -> (u64, &'static str) {
    match (max_idle_secs, idle_timeout_secs) {
        (Some(v), _) => (v, "max_idle_secs"),
        (None, Some(v)) => (v, "idle_timeout_secs"),
        (None, None) => (900, "default"),
    }
}

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
    lines.push(format!(
        "  self_update_branch:        {}",
        d.self_update_branch
    ));
    lines.push(format!("  cap:                       {}", d.cap));
    lines.push(format!("  reviewer_cap:              {}", d.reviewer_cap));
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
        "  max_busy_idle_secs:        {} ({})",
        d.max_busy_idle_secs.value, d.max_busy_idle_secs.source
    ));
    lines.push(format!(
        "  max_task_wall_secs:        {}",
        opt_u64(d.max_task_wall_secs)
    ));
    lines.push(format!(
        "  idle_timeout_secs:         {}",
        match d.idle_timeout_secs.value {
            Some(v) => format!("{v} ({src}, legacy)", src = d.idle_timeout_secs.source),
            None => format!("unset ({src}, legacy)", src = d.idle_timeout_secs.source),
        }
    ));
    let (effective_idle, effective_origin) =
        effective_idle_limit(d.max_idle_secs.value, d.idle_timeout_secs.value);
    lines.push(format!(
        "  effective idle limit:      {effective_idle} ({effective_origin}; max_idle_secs overrides idle_timeout_secs, default 900)"
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
        "  token_limit_basis:         {}",
        d.token_limit_basis
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
    lines.push(format!(
        "  resource_poll_secs:        {}",
        d.resource_poll_secs
    ));
    lines.push(format!(
        "  disk free GiB warn/crit:   {}/{}",
        d.disk_warn_free_gib, d.disk_critical_free_gib
    ));
    lines.push(format!(
        "  memory avail % warn/crit:  {}/{}",
        d.memory_warn_available_pct, d.memory_critical_available_pct
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

/// Resolve the base branch that a task created outside the daemon should store.
///
/// This does not require the daemon's routing configuration, but it deserializes
/// the same config shape so unknown keys fail exactly as they do for `serve`.
/// It uses the same per-repository config location and built-in `main` fallback.
pub fn task_create_base_branch(repo: &str) -> Result<String> {
    let path = default_config_path(repo)?;
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok("main".into()),
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "cannot read serve config {}: {error}",
                path.display()
            )))
        }
    };
    let config: ServeFileConfig = toml::from_str(&contents).map_err(|error| {
        QuorumError::Usage(format!("bad serve config {}: {error}", path.display()))
    })?;
    Ok(config.base_branch.unwrap_or_else(|| "main".into()))
}

/// Read the configured directed branch-sync pairs for one repository without
/// requiring daemon-only model-routing configuration. This is the public CLI's
/// authorization boundary for `quorum branch-sync`.
pub fn branch_sync_pairs(repo: &str) -> Result<Vec<[String; 2]>> {
    let path = default_config_path(repo)?;
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "cannot read serve config {}: {error}",
                path.display()
            )))
        }
    };
    let config: ServeFileConfig = toml::from_str(&contents).map_err(|error| {
        QuorumError::Usage(format!("bad serve config {}: {error}", path.display()))
    })?;
    validate_sync_pairs(&config.sync_pairs)?;
    Ok(config.sync_pairs)
}

/// Resolve the rework ceiling that a supported non-daemon classification writer
/// (currently `quorum classify --backfill`) must stamp when it makes a task
/// newly dispatchable.
///
/// Uses the same per-repository config location and validation as `serve`.
/// When the config file is absent or does not set `max_rework`, this falls back
/// to the compiled [`quorum_core::lifecycle::REWORK_CAP`], matching serve.
pub fn resolve_max_rework(repo: &str) -> Result<u32> {
    resolve_max_rework_at(&default_config_path(repo)?)
}

/// Path-parameterised variant of [`resolve_max_rework`]. Directly testable.
pub fn resolve_max_rework_at(path: &Path) -> Result<u32> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(quorum_core::lifecycle::REWORK_CAP);
        }
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "cannot read serve config {}: {error}",
                path.display()
            )))
        }
    };
    let config: ServeFileConfig = toml::from_str(&contents).map_err(|error| {
        QuorumError::Usage(format!("bad serve config {}: {error}", path.display()))
    })?;
    let cap = config
        .max_rework
        .unwrap_or(quorum_core::lifecycle::REWORK_CAP);
    validate_max_rework(cap)?;
    Ok(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent_mcp_config(source: &str) -> Result<AgentMcpServeConfig> {
        let cfg: ServeFileConfig = toml::from_str(source).expect("valid TOML");
        resolve_agent_mcp(&cfg)
    }

    #[test]
    fn agent_mcp_defaults_to_an_ephemeral_loopback_port() {
        let resolved = agent_mcp_config("").unwrap();
        assert_eq!(resolved.listen, "127.0.0.1:0".parse().unwrap());
        assert!(resolved.listen.ip().is_loopback());
        assert!(resolved.allowed_hosts.is_empty());
    }

    #[test]
    fn agent_mcp_on_the_orchestrator_address_is_a_usage_error() {
        let error = agent_mcp_config(
            "agent_mcp_listen = \"127.0.0.1:8811\"\norchestrator_mcp_listen = \"127.0.0.1:8811\"\n",
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), 2, "{error}");
        // Distinct addresses are accepted.
        agent_mcp_config(
            "agent_mcp_listen = \"127.0.0.1:8812\"\norchestrator_mcp_listen = \"127.0.0.1:8811\"\n",
        )
        .unwrap();
    }

    #[test]
    fn non_loopback_agent_mcp_requires_explicit_allowed_hosts() {
        for listen in ["0.0.0.0:8812", "[::]:8812", "192.0.2.7:8812"] {
            let error =
                agent_mcp_config(&format!("agent_mcp_listen = \"{listen}\"\n")).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{listen}: {error}");
            let error = agent_mcp_config(&format!(
                "agent_mcp_listen = \"{listen}\"\nagent_mcp_allowed_hosts = []\n"
            ))
            .unwrap_err();
            assert_eq!(error.exit_code(), 2, "{listen}: {error}");
        }
        let resolved = agent_mcp_config(
            "agent_mcp_listen = \"0.0.0.0:8812\"\nagent_mcp_allowed_hosts = [\"quorum.internal:8812\"]\n",
        )
        .unwrap();
        // The operator's host opts the bind in; the daemon adds the loopback
        // literal its managed runs dial for a wildcard bind.
        assert_eq!(
            resolved.allowed_hosts,
            ["quorum.internal:8812", "127.0.0.1"]
        );
        let error = agent_mcp_config(
            "agent_mcp_listen = \"0.0.0.0:8812\"\nagent_mcp_allowed_hosts = [\"bad host\"]\n",
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), 2, "{error}");
    }

    #[test]
    fn exported_agent_mcp_authority_is_always_admitted() {
        let addr = |s: &str| s.parse::<std::net::SocketAddr>().unwrap();
        assert_eq!(
            agent_mcp_url(addr("0.0.0.0:8812")),
            "http://127.0.0.1:8812/mcp"
        );
        assert_eq!(agent_mcp_url(addr("[::]:8812")), "http://[::1]:8812/mcp");
        assert_eq!(
            agent_mcp_url(addr("192.0.2.7:8812")),
            "http://192.0.2.7:8812/mcp"
        );
        assert_eq!(
            agent_mcp_url(addr("127.0.0.1:4100")),
            "http://127.0.0.1:4100/mcp"
        );

        let hosts = |listen: &str| {
            agent_mcp_config(&format!(
                "agent_mcp_listen = \"{listen}\"\nagent_mcp_allowed_hosts = [\"quorum.internal\"]\n"
            ))
            .unwrap()
            .allowed_hosts
        };
        assert_eq!(hosts("[::]:8812"), ["quorum.internal", "[::1]"]);
        assert_eq!(hosts("192.0.2.7:8812"), ["quorum.internal", "192.0.2.7"]);
        // Loopback binds already admit 127.0.0.1 and ::1 at bind time.
        assert_eq!(hosts("127.0.0.1:8812"), ["quorum.internal"]);
        assert_eq!(hosts("[::1]:8812"), ["quorum.internal"]);
        // Other 127/8 addresses are loopback but not default names.
        assert_eq!(hosts("127.0.0.2:8812"), ["quorum.internal", "127.0.0.2"]);

        // A full operator list leaves no room for the client literal: refused
        // at startup (exit 2), never a listener whose URL answers 403.
        let full: Vec<String> = (0..16).map(|i| format!("\"h{i}\"")).collect();
        let error = agent_mcp_config(&format!(
            "agent_mcp_listen = \"0.0.0.0:8812\"\nagent_mcp_allowed_hosts = [{}]\n",
            full.join(",")
        ))
        .unwrap_err();
        assert_eq!(error.exit_code(), 2, "{error}");
    }

    /// One raw HTTP/1.1 `tools/list` exchange against `url` with the URL's
    /// own authority as `Host`; returns the status code and body.
    async fn post_tools_list(url: &str, bearer: &str, host: Option<&str>) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let rest = url.strip_prefix("http://").expect("http URL");
        let (authority, path) = rest.split_at(rest.find('/').expect("URL path"));
        let target: std::net::SocketAddr = authority.parse().expect("socket authority");
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\n\
             Authorization: Bearer {bearer}\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            host.unwrap_or(authority),
            body.len()
        );
        let mut stream = tokio::net::TcpStream::connect(target).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("response within bound")
        .unwrap();
        let response = String::from_utf8_lossy(&response).into_owned();
        let code = response
            .strip_prefix("HTTP/1.1 ")
            .and_then(|rest| rest.get(..3))
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("not an HTTP response: {response:?}"));
        (code, response)
    }

    /// The supported non-loopback opt-in: a wildcard bind with an operator
    /// host. The URL the daemon exports must be served (200), not refused by
    /// the Host allowlist, while an unlisted Host is still refused.
    #[tokio::test]
    async fn non_loopback_opt_in_exports_a_url_the_listener_serves() {
        let resolved = agent_mcp_config(
            "agent_mcp_listen = \"0.0.0.0:0\"\nagent_mcp_allowed_hosts = [\"quorum.internal:8812\"]\n",
        )
        .unwrap();
        let listener =
            crate::agent_mcp::AgentMcpListener::bind(resolved.listen, resolved.allowed_hosts)
                .await
                .unwrap();
        let bound = listener.local_addr();
        assert!(bound.ip().is_unspecified());
        let url = agent_mcp_url(bound);
        assert_eq!(url, format!("http://127.0.0.1:{}/mcp", bound.port()));

        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let serving = tokio::spawn(listener.serve(
            std::path::PathBuf::from("/nonexistent/quorum-agent-endpoint.sock"),
            "owner/repository".into(),
            async move {
                let _ = stopped.await;
            },
        ));

        let (code, response) = post_tools_list(&url, "run-1", None).await;
        assert_eq!(code, 200, "{response}");
        assert!(response.contains("\"jsonrpc\""), "{response}");
        // The operator's configured authority is still admitted.
        let (code, response) = post_tools_list(&url, "run-1", Some("quorum.internal:8812")).await;
        assert_eq!(code, 200, "{response}");
        // An unlisted Host is still refused: the allowlist was not widened
        // beyond the client literal.
        let (code, response) = post_tools_list(&url, "run-1", Some("evil.example")).await;
        assert_eq!(code, 403, "{response}");

        stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), serving)
            .await
            .expect("serving future returns after shutdown")
            .unwrap();
    }

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
    fn resource_monitor_defaults_and_custom_values_validate() {
        let defaults = resolve_resource_monitor_config(&ServeFileConfig::default()).unwrap();
        assert_eq!(
            defaults,
            crate::resource_health::ResourceMonitorConfig::default()
        );

        let custom: ServeFileConfig = toml::from_str(
            "resource_poll_secs = 45\ndisk_warn_free_gib = 120\ndisk_critical_free_gib = 60\nmemory_warn_available_pct = 20\nmemory_critical_available_pct = 10\n",
        )
        .unwrap();
        let custom = resolve_resource_monitor_config(&custom).unwrap();
        assert_eq!(custom.poll_secs, 45);
        assert_eq!(custom.disk_warn_free_gib, 120);
        assert_eq!(custom.memory_critical_available_pct, 10);
    }

    #[test]
    fn resource_monitor_rejects_bad_interval_and_threshold_ordering() {
        for (source, expected) in [
            ("resource_poll_secs = 4\n", "resource_poll_secs"),
            (
                "disk_warn_free_gib = 40\ndisk_critical_free_gib = 40\n",
                "disk_critical_free_gib",
            ),
            (
                "memory_warn_available_pct = 8\nmemory_critical_available_pct = 15\n",
                "memory_critical_available_pct",
            ),
            (
                "memory_warn_available_pct = 101\nmemory_critical_available_pct = 8\n",
                "warning must be at most 100",
            ),
        ] {
            let file: ServeFileConfig = toml::from_str(source).unwrap();
            let error = resolve_resource_monitor_config(&file).unwrap_err();
            assert_eq!(error.exit_code(), 2);
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn status_resource_config_reads_worktree_path_without_routing_requirement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            "worktree_base = \"/tmp/quorum-worktrees\"\nresource_poll_secs = 60\n",
        )
        .unwrap();

        let status = status_resource_config_at(&path).unwrap();
        assert_eq!(status.monitor.poll_secs, 60);
        assert_eq!(
            status.worktree_base,
            Some(std::path::PathBuf::from("/tmp/quorum-worktrees"))
        );
    }

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
    fn resolve_idle_limit_preserves_source_precedence_across_aliases() {
        let modern_flag = resolve_idle_limit(Some(900), Some(60), Some(300), Some(120));
        assert_eq!(modern_flag.value, Some(900));
        assert_eq!(modern_flag.source, Source::Flag);

        let legacy_flag_over_file = resolve_idle_limit(None, Some(60), Some(900), Some(120));
        assert_eq!(legacy_flag_over_file.value, Some(60));
        assert_eq!(legacy_flag_over_file.source, Source::Flag);

        let modern_file = resolve_idle_limit(None, None, Some(900), Some(120));
        assert_eq!(modern_file.value, Some(900));
        assert_eq!(modern_file.source, Source::File);

        let default = resolve_idle_limit::<u64>(None, None, None, None);
        assert_eq!(default.value, None);
        assert_eq!(default.source, Source::Default);
    }

    #[test]
    fn token_limit_basis_defaults_to_raw_and_parses_uncached() {
        let default = resolve_token_limit_basis(None).unwrap();
        assert_eq!(default.value, TokenLimitBasis::Raw);
        assert_eq!(default.source, Source::Default);

        let uncached = resolve_token_limit_basis(Some("uncached")).unwrap();
        assert_eq!(uncached.value, TokenLimitBasis::Uncached);
        assert_eq!(uncached.source, Source::File);
    }

    #[test]
    fn token_limit_policy_rejects_invalid_basis_and_nonpositive_ceilings() {
        let invalid_basis: ServeFileConfig =
            toml::from_str(&format!("token_limit_basis = \"cached\"\n{VALID_ROUTING}")).unwrap();
        let basis_err = validate_model_routing(&invalid_basis).unwrap_err();
        assert_eq!(basis_err.exit_code(), 2);
        assert!(basis_err.to_string().contains("token_limit_basis"));

        for (turn, task) in [(Some(0), None), (None, Some(-1))] {
            let err = validate_token_ceilings(turn, task).unwrap_err();
            assert_eq!(err.exit_code(), 2);
            assert!(err.to_string().contains("greater than zero"));
        }
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
        assert_eq!(cfg.max_busy_idle_secs, None);
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
        assert_eq!(
            resolve_grok_adapter(None).unwrap(),
            GrokResolvedConfig {
                sandbox: crate::serve::grok_agent::DEFAULT_SANDBOX.into(),
                permission_mode: crate::serve::grok_agent::DEFAULT_PERMISSION_MODE.into(),
                max_turns: crate::serve::grok_agent::DEFAULT_MAX_TURNS,
            }
        );

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
    fn grok_is_available_only_for_legacy_worker_selection() {
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
            let roles = resolve_roles(&config, cli_agent, "sonnet", "high").unwrap();
            assert_eq!(roles.worker_model, "grok-4.5");
        }

        for model in ["grok-4.5", "grok-4.6", "grok-4.7"] {
            let worker: ServeFileConfig =
                toml::from_str(&format!("worker_model = \"{model}\"\n")).unwrap();
            assert_eq!(
                resolve_roles(&worker, None, "sonnet", "high")
                    .unwrap()
                    .worker_model,
                model
            );
            for key in ["review_model", "classifier_model", "collector_model"] {
                let cfg: ServeFileConfig =
                    toml::from_str(&format!("{key} = \"{model}\"\n")).unwrap();
                let error = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("enabled only for managed workers"),
                    "{key}/{model}: {error}"
                );
            }
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
    fn external_provider_role_models_are_not_resolved_through_for_model() {
        // "gpt-5" maps to Codex through the built-in mapping; under an
        // external provider it is an opaque model and must not be rejected.
        let cfg: ServeFileConfig = toml::from_str(
            "provider = \"external\"\nworker_model = \"gpt-5\"\nreview_model = \"gpt-5\"\n\
             classifier_model = \"gpt-5\"\ncollector_model = \"grok-4.5\"\n",
        )
        .unwrap();
        let roles = resolve_roles(&cfg, None, "sonnet", "high").unwrap();
        assert_eq!(roles.provider, RunnerKind::External);
        assert_eq!(roles.worker_model, "gpt-5");
        assert_eq!(roles.review_model, "gpt-5");
        assert_eq!(roles.collector_model, "grok-4.5");

        // A built-in provider keeps its model mismatch check.
        let cfg: ServeFileConfig =
            toml::from_str("provider = \"claude\"\nworker_model = \"gpt-5\"\n").unwrap();
        let err = resolve_roles(&cfg, None, "sonnet", "high").unwrap_err();
        assert!(err.to_string().contains("does not match provider"), "{err}");
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
    fn routing_rejects_usd_limits_when_worker_can_select_grok() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profiles = cfg.model_profiles.as_mut().unwrap();
        profiles.get_mut("primary").unwrap().runner = "claude".into();
        profiles.get_mut("primary").unwrap().model = "claude-sonnet-4-6".into();
        profiles.insert(
            "grok-worker".into(),
            ModelProfile {
                runner: "grok".into(),
                model: "grok-4.5".into(),
                effort: "high".into(),
            },
        );
        for pool in cfg.routing.as_mut().unwrap().worker.values_mut() {
            pool.clear();
            pool.insert("grok-worker".into(), 100);
        }

        cfg.max_turn_cost_usd = Some(1.0);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("Grok"), "{err}");

        cfg.max_turn_cost_usd = None;
        cfg.max_task_cost_usd = Some(10.0);
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("Grok"), "{err}");
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
    fn routing_external_profile_skips_model_cross_check() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profile = cfg
            .model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap();
        profile.runner = "external".into();
        profile.model = "gpt-5".into();
        profile.effort = "deep_think-2".into();
        // Any [a-z0-9_-]{1,32} effort is accepted when `efforts` is absent.
        cfg.external_runner = Some(external_decl(&["efforts"]));
        validate_model_routing(&cfg).unwrap();
        let runner = RunnerKind::from_str_opt(Some("external")).unwrap();
        assert_eq!(runner, RunnerKind::External);
        assert_eq!(
            runner.agent_kind(),
            crate::serve::runner::AgentKind::External
        );
        assert_ne!(
            runner.agent_kind(),
            crate::serve::runner::AgentKind::for_model("gpt-5").unwrap()
        );
        assert_eq!(RunnerKind::External.to_string(), "external");
    }

    #[test]
    fn routing_builtin_profile_mismatch_still_refused_beside_external() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profiles = cfg.model_profiles.as_mut().unwrap();
        let mut external = profiles.get("primary").unwrap().clone();
        external.runner = "external".into();
        profiles.insert("ext".into(), external);
        profiles.get_mut("primary").unwrap().runner = "claude".into();
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(err.to_string().contains("does not match runner"), "{err}");
    }

    #[test]
    fn routing_rejects_external_effort_outside_pattern() {
        for effort in ["", "High", "a b", "x.y", &"a".repeat(33)] {
            let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
            let profile = cfg
                .model_profiles
                .as_mut()
                .unwrap()
                .get_mut("primary")
                .unwrap();
            profile.runner = "external".into();
            profile.effort = effort.to_string();
            let err = validate_model_routing(&cfg).unwrap_err();
            assert!(matches!(err, QuorumError::Usage(_)), "{err}");
            if !effort.is_empty() {
                assert!(
                    err.to_string().contains("model_profiles.primary.effort"),
                    "{effort:?}: {err}"
                );
            }
        }
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let profile = cfg
            .model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap();
        profile.runner = "external".into();
        profile.effort = "a".repeat(32);
        cfg.external_runner = Some(external_decl(&["efforts"]));
        validate_model_routing(&cfg).unwrap();
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
    fn worker_allows_grok_but_other_managed_roles_reject_it() {
        fn config_with_grok_worker(model: &str) -> ServeFileConfig {
            let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
            cfg.model_profiles.as_mut().unwrap().insert(
                "grok-worker".into(),
                ModelProfile {
                    runner: "grok".into(),
                    model: model.into(),
                    effort: "high".into(),
                },
            );
            for pool in cfg.routing.as_mut().unwrap().worker.values_mut() {
                pool.clear();
                pool.insert("grok-worker".into(), 100);
            }
            cfg
        }

        for model in ["grok-4.5", "grok-4.6", "grok-4.7"] {
            validate_model_routing(&config_with_grok_worker(model)).unwrap();

            for role in [
                "classifier",
                "planner",
                "arbiter",
                "collector",
                "reviewer.1",
            ] {
                let mut cfg = config_with_grok_worker(model);
                let routing = cfg.routing.as_mut().unwrap();
                let pool = match role {
                    "classifier" => &mut routing.classifier,
                    "planner" => &mut routing.planner,
                    "arbiter" => &mut routing.arbiter,
                    "collector" => &mut routing.collector,
                    "reviewer.1" => routing.reviewer.get_mut("1").unwrap(),
                    _ => unreachable!(),
                };
                pool.clear();
                pool.insert("grok-worker".into(), 100);
                let err = validate_model_routing(&cfg).unwrap_err();
                assert!(err.to_string().contains(role), "{role}/{model}: {err}");
            }
        }
    }

    #[test]
    fn routing_rejects_unknown_grok_model() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.model_profiles.as_mut().unwrap().insert(
            "grok-unknown".into(),
            ModelProfile {
                runner: "grok".into(),
                model: "grok-4.7-build-fast".into(),
                effort: "high".into(),
            },
        );
        let error = validate_model_routing(&cfg).unwrap_err();
        assert!(error.to_string().contains("unknown model"), "{error}");
    }

    #[test]
    fn planner_accepts_matching_codex_profile_and_rejects_mismatch() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let planner = &mut cfg.routing.as_mut().unwrap().planner;
        planner.clear();
        planner.insert("primary".into(), 100);
        validate_model_routing(&cfg).unwrap();

        cfg.model_profiles
            .as_mut()
            .unwrap()
            .get_mut("primary")
            .unwrap()
            .runner = "claude".into();
        let err = validate_model_routing(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("does not match runner \"claude\""),
            "{err}"
        );
    }

    #[test]
    fn arbiter_defaults_to_planner_pool_when_absent_and_validates() {
        // VALID_ROUTING carries no [routing.arbiter] block, so the field
        // deserializes to the empty sentinel and the effective pool mirrors
        // the planner pool — existing configs keep validating unchanged.
        let cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        validate_model_routing(&cfg).unwrap();
        let routing = cfg.routing.as_ref().unwrap();
        assert!(
            routing.arbiter.is_empty(),
            "no explicit [routing.arbiter] block was configured"
        );
        assert_eq!(
            routing.arbiter_pool(),
            &routing.planner,
            "absent arbiter pool mirrors the planner pool"
        );
        let total: u16 = routing
            .arbiter_pool()
            .values()
            .map(|percentage| u16::from(*percentage))
            .sum();
        assert_eq!(total, 100, "effective arbiter pool totals 100");
    }

    #[test]
    fn arbiter_pool_must_total_100() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        {
            let routing = cfg.routing.as_mut().unwrap();
            routing.arbiter.clear();
            // `planner` is a valid Claude profile; only the total is wrong.
            routing.arbiter.insert("planner".into(), 90);
        }
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string().contains("routing.arbiter") && err.to_string().contains("total"),
            "{err}"
        );
    }

    #[test]
    fn grok_rejected_in_arbiter_role() {
        let mut cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        cfg.model_profiles.as_mut().unwrap().insert(
            "grok-worker".into(),
            ModelProfile {
                runner: "grok".into(),
                model: "grok-4.5".into(),
                effort: "high".into(),
            },
        );
        {
            let routing = cfg.routing.as_mut().unwrap();
            routing.arbiter.clear();
            routing.arbiter.insert("grok-worker".into(), 100);
        }
        let err = validate_model_routing(&cfg).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string().contains("routing.arbiter") && err.to_string().contains("Grok"),
            "{err}"
        );
    }

    #[test]
    fn suggested_models_accepts_every_closed_tier_at_supported_efforts() {
        assert!(quorum_core::model_tiers::MODEL_TIERS
            .iter()
            .any(|tier| tier.tier == "opus-55" && tier.model_id == "claude-opus-5-5"));
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
                    .contains("expected Claude tier: sonnet-5|opus-46|opus-47|opus-48|opus-55"),
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
        assert!(cfg.sync_pairs.is_empty());
        assert!(cfg.model_profiles.is_some());
        assert!(cfg.routing.is_some());
        validate_model_routing(&cfg).unwrap();
    }

    #[test]
    fn sync_pairs_are_empty_by_default_and_validate_as_directed_branch_pairs() {
        validate_sync_pairs(&ServeFileConfig::default().sync_pairs).unwrap();
        let configured: ServeFileConfig =
            toml::from_str("sync_pairs = [[\"main\", \"develop\"]]\n").unwrap();
        assert_eq!(
            configured.sync_pairs,
            vec![["main".to_string(), "develop".to_string()]]
        );
        validate_sync_pairs(&configured.sync_pairs).unwrap();

        for source in [
            "sync_pairs = [[\"main\", \"main\"]]\n",
            "sync_pairs = [[\"-not-a-branch\", \"develop\"]]\n",
        ] {
            let config: ServeFileConfig = toml::from_str(source).unwrap();
            let error = validate_sync_pairs(&config.sync_pairs).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{error}");
        }
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
    fn max_rework_validation_rejects_zero_with_usage_exit() {
        let err = validate_max_rework(0).unwrap_err();
        assert_eq!(err.exit_code(), 2, "{err}");
        validate_max_rework(1).unwrap();
        validate_max_rework(10).unwrap();
    }

    #[test]
    fn file_graph_children_working_limit_matches_daemon_resolution() {
        let parse = |src: &str| toml::from_str::<ServeFileConfig>(src).unwrap();
        // Absent cap and key → the daemon's default cap, not the legacy 2.
        assert_eq!(
            file_graph_children_working_limit(&parse("")).unwrap(),
            DEFAULT_CAP as i64
        );
        assert_eq!(
            file_graph_children_working_limit(&parse("cap = 6\n")).unwrap(),
            6
        );
        assert_eq!(
            file_graph_children_working_limit(&parse("cap = 6\nmax_graph_children_working = 1\n"))
                .unwrap(),
            1
        );
        assert!(matches!(
            file_graph_children_working_limit(&parse("max_graph_children_working = 0\n")),
            Err(QuorumError::Usage(_))
        ));
    }

    #[test]
    fn reviewer_cap_defaults_to_max_cap_two_and_rejects_out_of_range() {
        let parse = |toml: &str| -> Option<i64> {
            toml::from_str::<ServeFileConfig>(toml)
                .unwrap()
                .reviewer_cap
        };
        assert_eq!(resolve_reviewer_cap(None, 1).unwrap(), 2);
        assert_eq!(resolve_reviewer_cap(None, 4).unwrap(), 4);
        assert_eq!(resolve_reviewer_cap(parse(""), 0).unwrap(), 2);
        assert_eq!(
            resolve_reviewer_cap(parse("reviewer_cap = 1\n"), 4).unwrap(),
            1
        );
        assert_eq!(
            resolve_reviewer_cap(parse("reviewer_cap = 32\n"), 4).unwrap(),
            32
        );
        for bad in [0, -1, 33] {
            let err = resolve_reviewer_cap(Some(bad), 4).unwrap_err();
            assert!(matches!(err, QuorumError::Usage(_)), "{err}");
            assert_eq!(err.exit_code(), 2, "{err}");
            assert!(err.to_string().contains("reviewer_cap"), "{err}");
        }
        assert_eq!(
            file_reviewer_cap(&parse_cfg("")).unwrap(),
            DEFAULT_CAP as i64
        );
        assert_eq!(file_reviewer_cap(&parse_cfg("cap = 1\n")).unwrap(), 2);
        assert_eq!(
            file_reviewer_cap(&parse_cfg("cap = 1\nreviewer_cap = 5\n")).unwrap(),
            5
        );
        assert!(matches!(
            file_reviewer_cap(&parse_cfg("reviewer_cap = 33\n")),
            Err(QuorumError::Usage(_))
        ));
    }

    fn parse_cfg(src: &str) -> ServeFileConfig {
        toml::from_str::<ServeFileConfig>(src).unwrap()
    }

    #[test]
    fn max_graph_children_working_defaults_to_cap_and_rejects_non_positive() {
        let parse = |toml: &str| -> Option<i64> {
            toml::from_str::<ServeFileConfig>(toml)
                .unwrap()
                .max_graph_children_working
        };
        // Absent key → worker cap.
        assert_eq!(resolve_max_graph_children_working(parse(""), 4).unwrap(), 4);
        assert_eq!(resolve_max_graph_children_working(parse(""), 7).unwrap(), 7);
        // Explicit values, including above cap, are kept.
        let one = parse("max_graph_children_working = 1\n");
        assert_eq!(resolve_max_graph_children_working(one, 4).unwrap(), 1);
        let above = parse("max_graph_children_working = 9\n");
        assert_eq!(resolve_max_graph_children_working(above, 4).unwrap(), 9);
        // Zero and negative are bad config → exit 2.
        for bad in [
            "max_graph_children_working = 0\n",
            "max_graph_children_working = -3\n",
        ] {
            let err = resolve_max_graph_children_working(parse(bad), 4).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{err}");
            assert!(
                err.to_string().contains("max_graph_children_working"),
                "{err}"
            );
        }
    }

    #[test]
    fn max_active_graphs_defaults_to_two_and_rejects_non_positive_or_non_integer() {
        let parse = |toml: &str| -> Option<i64> {
            toml::from_str::<ServeFileConfig>(toml)
                .unwrap()
                .max_active_graphs
        };
        // Absent key → default of 2.
        assert_eq!(resolve_max_active_graphs(parse("")).unwrap(), 2);
        // Minimum accepted value.
        assert_eq!(
            resolve_max_active_graphs(parse("max_active_graphs = 1\n")).unwrap(),
            1
        );
        // A valid larger value is kept as configured.
        assert_eq!(
            resolve_max_active_graphs(parse("max_active_graphs = 5\n")).unwrap(),
            5
        );
        // Zero and negative are bad config → exit 2.
        for bad in ["max_active_graphs = 0\n", "max_active_graphs = -1\n"] {
            let err = resolve_max_active_graphs(parse(bad)).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{err}");
            assert!(err.to_string().contains("max_active_graphs"), "{err}");
        }
        // A non-integer value fails TOML deserialization itself (wrong type),
        // which `load` maps to a usage error before resolution runs.
        let err = toml::from_str::<ServeFileConfig>("max_active_graphs = \"two\"\n").unwrap_err();
        assert!(err.to_string().contains("max_active_graphs"), "{err}");
    }

    #[test]
    fn resolve_max_rework_at_missing_file_returns_compiled_default() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent.toml");
        assert_eq!(
            resolve_max_rework_at(&missing).unwrap(),
            quorum_core::lifecycle::REWORK_CAP
        );
    }

    #[test]
    fn resolve_max_rework_at_unset_key_returns_compiled_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "repo = \"owner/repo\"\n").unwrap();
        assert_eq!(
            resolve_max_rework_at(&path).unwrap(),
            quorum_core::lifecycle::REWORK_CAP
        );
    }

    #[test]
    fn resolve_max_rework_at_set_value_is_returned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "max_rework = 10\n").unwrap();
        assert_eq!(resolve_max_rework_at(&path).unwrap(), 10);
    }

    #[test]
    fn resolve_max_rework_at_zero_is_a_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "max_rework = 0\n").unwrap();
        let err = resolve_max_rework_at(&path).unwrap_err();
        assert_eq!(err.exit_code(), 2, "{err}");
    }

    #[test]
    fn resolve_max_rework_at_bad_toml_is_a_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(&path, "not = valid = toml\n").unwrap();
        let err = resolve_max_rework_at(&path).unwrap_err();
        assert_eq!(err.exit_code(), 2, "{err}");
    }

    #[test]
    fn effective_idle_limit_prefers_max_idle_then_legacy_then_900() {
        assert_eq!(
            effective_idle_limit(Some(60), Some(300)),
            (60, "max_idle_secs")
        );
        assert_eq!(
            effective_idle_limit(None, Some(300)),
            (300, "idle_timeout_secs")
        );
        assert_eq!(effective_idle_limit(None, None), (900, "default"));
    }

    #[test]
    fn banner_shows_config_path() {
        let routing_cfg: ServeFileConfig = toml::from_str(VALID_ROUTING).unwrap();
        let token_limit_basis = Sourced {
            value: TokenLimitBasis::Raw,
            source: Source::Default,
        };
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
            self_update_branch: &Sourced {
                value: "main".into(),
                source: Source::Default,
            },
            cap: &Sourced {
                value: 8,
                source: Source::File,
            },
            reviewer_cap: &Sourced {
                value: 8,
                source: Source::Default,
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
            max_busy_idle_secs: &Sourced {
                value: DEFAULT_MAX_BUSY_IDLE_SECS,
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
            token_limit_basis: &token_limit_basis,
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
            resource_poll_secs: &Sourced {
                value: crate::resource_health::DEFAULT_RESOURCE_POLL_SECS,
                source: Source::Default,
            },
            disk_warn_free_gib: &Sourced {
                value: crate::resource_health::DEFAULT_DISK_WARN_FREE_GIB,
                source: Source::Default,
            },
            disk_critical_free_gib: &Sourced {
                value: crate::resource_health::DEFAULT_DISK_CRITICAL_FREE_GIB,
                source: Source::Default,
            },
            memory_warn_available_pct: &Sourced {
                value: crate::resource_health::DEFAULT_MEMORY_WARN_AVAILABLE_PCT,
                source: Source::Default,
            },
            memory_critical_available_pct: &Sourced {
                value: crate::resource_health::DEFAULT_MEMORY_CRITICAL_AVAILABLE_PCT,
                source: Source::Default,
            },
        });
        assert!(
            b.contains("config file:               /path/to/config.toml"),
            "{b}"
        );
        assert!(b.contains("8 (file)"), "cap should show file source: {b}");
        assert!(
            b.contains("reviewer_cap:              8 (default)"),
            "reviewer_cap should be shown with its source: {b}"
        );
        assert!(
            b.contains("self_update_branch:        main (default)"),
            "self-update branch should be shown with its source: {b}"
        );
        assert!(
            b.contains("token_limit_basis:         raw (default)"),
            "raw token basis should be shown: {b}"
        );
        assert!(
            !b.contains("max_turn_wall_secs"),
            "deprecated turn-wall ceiling must not appear in the resolved banner: {b}"
        );
        assert!(
            b.contains("max_idle_secs:             900 (default)"),
            "max_idle_secs should default to 900: {b}"
        );
        assert!(
            b.contains("max_busy_idle_secs:        3600 (default)"),
            "max_busy_idle_secs should default to 3600: {b}"
        );
        assert!(b.contains("(default)"), "defaults should be labeled: {b}");
        assert!(
            b.contains("idle_timeout_secs:         unset (default, legacy)"),
            "unset legacy idle timeout must not claim a 300s default: {b}"
        );
        assert!(!b.contains("300 (default, default)"), "{b}");
        assert!(
            b.contains("effective idle limit:      900 (default; max_idle_secs overrides"),
            "effective idle limit should be 900 by default: {b}"
        );
    }

    // ── session_base ─────────────────────────────────────────────────

    fn session_base_dirs(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let state_dir = dir.join("state");
        let worktree_base = dir.join("worktrees");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::create_dir_all(&worktree_base).unwrap();
        (state_dir, worktree_base)
    }

    #[test]
    fn session_base_default_resolves_under_the_state_dir_and_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let resolved = resolve_session_base(None, &state_dir, &worktree_base).unwrap();
        assert_eq!(resolved, state_dir.join("sessions"));
        let metadata = std::fs::symlink_metadata(&resolved).unwrap();
        assert!(metadata.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        }
    }

    #[test]
    fn session_base_missing_directory_is_created_with_default_configured_value() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let configured = dir.path().join("elsewhere").join("sessions");
        assert!(!configured.exists());
        let resolved = resolve_session_base(
            Some(configured.to_str().unwrap()),
            &state_dir,
            &worktree_base,
        )
        .unwrap();
        assert_eq!(resolved, configured);
        assert!(std::fs::symlink_metadata(&configured).unwrap().is_dir());
    }

    #[test]
    fn session_base_refuses_a_relative_path() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let err = resolve_session_base(Some("relative/sessions"), &state_dir, &worktree_base)
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("session_base"), "{err}");
        assert!(err.to_string().contains("absolute"), "{err}");
    }

    #[test]
    fn session_base_refuses_equal_to_worktree_base() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let err = resolve_session_base(
            Some(worktree_base.to_str().unwrap()),
            &state_dir,
            &worktree_base,
        )
        .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("session_base"), "{err}");
        assert!(err.to_string().contains("worktree_base"), "{err}");
    }

    #[test]
    fn session_base_refuses_inside_worktree_base() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let inside = worktree_base.join("sessions");
        let err = resolve_session_base(Some(inside.to_str().unwrap()), &state_dir, &worktree_base)
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("worktree_base"), "{err}");
    }

    #[test]
    fn session_base_refuses_containing_worktree_base() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let containing = worktree_base.parent().unwrap().to_path_buf();
        let err = resolve_session_base(
            Some(containing.to_str().unwrap()),
            &state_dir,
            &worktree_base,
        )
        .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("worktree_base"), "{err}");
    }

    #[test]
    fn session_base_refuses_containing_the_state_dir() {
        // state_dir nests one level deeper than worktree_base so that an
        // ancestor of state_dir is not also an ancestor of worktree_base,
        // isolating the "contains the state directory" rule from the
        // worktree_base containment rule.
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("nested").join("state");
        let worktree_base = dir.path().join("worktrees");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::create_dir_all(&worktree_base).unwrap();
        let containing = state_dir.parent().unwrap().to_path_buf();
        let err = resolve_session_base(
            Some(containing.to_str().unwrap()),
            &state_dir,
            &worktree_base,
        )
        .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("state directory"), "{err}");
    }

    #[test]
    fn session_base_allows_the_default_location_inside_the_state_dir() {
        // The default (<state_dir>/sessions) is a descendant of state_dir,
        // not an ancestor, so it must not trip the "contains the state
        // directory" refusal.
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let inside_state_dir = state_dir.join("sessions");
        resolve_session_base(
            Some(inside_state_dir.to_str().unwrap()),
            &state_dir,
            &worktree_base,
        )
        .unwrap();
    }

    #[test]
    fn session_base_refuses_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let real_target = dir.path().join("real-sessions");
        std::fs::create_dir_all(&real_target).unwrap();
        let link = dir.path().join("linked-sessions");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_target, &link).unwrap();
        let err = resolve_session_base(Some(link.to_str().unwrap()), &state_dir, &worktree_base)
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    #[test]
    fn session_base_refuses_a_non_directory() {
        let dir = tempfile::tempdir().unwrap();
        let (state_dir, worktree_base) = session_base_dirs(dir.path());
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let err = resolve_session_base(Some(file.to_str().unwrap()), &state_dir, &worktree_base)
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("directory"), "{err}");
    }

    fn write_serve_config(dir: &Path, extra: &str) -> std::path::PathBuf {
        let path = dir.join("serve.toml");
        std::fs::write(&path, format!("{extra}\n{VALID_ROUTING}")).unwrap();
        path
    }

    #[test]
    fn reload_refuses_a_session_base_change_and_keeps_the_running_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_serve_config(dir.path(), "cap = 3\nsession_base = \"/srv/a\"\n");
        let mut running = load(&path, true).unwrap();
        assert_eq!(running.session_base.as_deref(), Some("/srv/a"));

        let changed_path = write_serve_config(dir.path(), "cap = 4\nsession_base = \"/srv/b\"\n");
        let err = reload(&mut running, &changed_path).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("session_base"), "{err}");
        assert!(err.to_string().contains("restart-only"), "{err}");
        assert_eq!(running.cap, Some(3), "running config must stay in force");
        assert_eq!(running.session_base.as_deref(), Some("/srv/a"));
    }

    #[test]
    fn reload_without_a_session_base_change_applies_runtime_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_serve_config(dir.path(), "cap = 3\nsession_base = \"/srv/a\"\n");
        let mut running = load(&path, true).unwrap();

        let same_path = write_serve_config(dir.path(), "cap = 5\nsession_base = \"/srv/a\"\n");
        reload(&mut running, &same_path).unwrap();
        assert_eq!(running.cap, Some(5));
        assert_eq!(running.session_base.as_deref(), Some("/srv/a"));
    }

    fn external_fingerprint(cfg: &ServeFileConfig) -> Option<String> {
        resolve_external_runner(cfg)
            .unwrap()
            .map(|spec| spec.fingerprint)
    }

    #[test]
    fn external_runner_declaration_loads_into_a_validated_spec() {
        let dir = tempfile::tempdir().unwrap();
        let declaration = crate::external_runner_spec::tests::VALID;
        let cfg = load(&write_serve_config(dir.path(), declaration), true).unwrap();
        let spec = resolve_external_runner(&cfg).unwrap().expect("declared");
        assert_eq!(spec.stream.terminal_exit_secs, 30);

        // The config's agent_bin participates in the fingerprint. (The
        // fixture routing spans runners, so agent_bin is set directly.)
        let mut cfg_bin = load(&write_serve_config(dir.path(), declaration), true).unwrap();
        cfg_bin.agent_bin = Some("/opt/wrapper".into());
        assert_ne!(external_fingerprint(&cfg_bin), Some(spec.fingerprint));

        // Absent: no spec, exactly as before.
        let cfg = load(&write_serve_config(dir.path(), ""), true).unwrap();
        assert!(cfg.external_runner.is_none());
        assert!(resolve_external_runner(&cfg).unwrap().is_none());
    }

    #[test]
    fn invalid_external_runner_is_refused_at_load_naming_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let declaration = crate::external_runner_spec::tests::VALID.replacen(
            "\"/usr/local/bin/fake-cli\"",
            "\"fake-cli\"",
            1,
        );
        let err = load(&write_serve_config(dir.path(), &declaration), true).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string().contains("external_runner.executable"),
            "{err}"
        );
    }

    #[test]
    fn reload_refuses_any_external_runner_change_and_keeps_the_running_config() {
        let dir = tempfile::tempdir().unwrap();
        let declaration = crate::external_runner_spec::tests::VALID;
        let path = write_serve_config(dir.path(), &format!("cap = 3\n{declaration}"));
        let mut running = load(&path, true).unwrap();
        let before = external_fingerprint(&running);

        let assert_refused = |running: &mut ServeFileConfig, extra: &str, needle: &str| {
            let path = write_serve_config(dir.path(), extra);
            let err = reload(running, &path).unwrap_err();
            assert_eq!(err.exit_code(), 2);
            assert!(err.to_string().contains(needle), "{err}");
            assert_eq!(running.cap, Some(3), "running config replaced");
            assert_eq!(external_fingerprint(running), before);
        };
        // A changed declaration, alongside a runtime change.
        let changed = declaration.replacen("max_line_bytes = 1048576", "max_line_bytes = 2048", 1);
        assert_refused(
            &mut running,
            &format!("cap = 4\n{changed}"),
            "\"external_runner\" is restart-only",
        );
        // Removing the table is a change.
        assert_refused(
            &mut running,
            "cap = 4\n",
            "\"external_runner\" is restart-only",
        );
        // A declaration that fails a rule is refused with the startup message.
        let invalid = declaration.replacen("\"/usr/local/bin/fake-cli\"", "\"fake-cli\"", 1);
        let startup = load(&write_serve_config(dir.path(), &invalid), true).unwrap_err();
        assert_refused(&mut running, &invalid, &startup.to_string());

        // Unchanged declaration (explicit default included): runtime keys apply.
        let same = declaration.replacen(
            "max_line_bytes = 1048576",
            "max_line_bytes = 1048576\nterminal_exit_secs = 30",
            1,
        );
        let path = write_serve_config(dir.path(), &format!("cap = 5\n{same}"));
        reload(&mut running, &path).unwrap();
        assert_eq!(running.cap, Some(5));
        assert_eq!(external_fingerprint(&running), before);
    }

    #[test]
    fn reload_without_external_runner_applies_runtime_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut running = load(&write_serve_config(dir.path(), "cap = 3\n"), true).unwrap();
        reload(&mut running, &write_serve_config(dir.path(), "cap = 6\n")).unwrap();
        assert_eq!(running.cap, Some(6));
        assert!(running.external_runner.is_none());
        // Adding the table is a restart-only change.
        let err = reload(
            &mut running,
            &write_serve_config(dir.path(), crate::external_runner_spec::tests::VALID),
        )
        .unwrap_err();
        assert!(err.to_string().contains("restart-only"), "{err}");
        assert!(running.external_runner.is_none());
    }

    /// The external declaration fixture without the lines starting with any
    /// of `drop` (each prefix must match, so a stale fixture cannot pass).
    fn external_decl_source(drop: &[&str]) -> String {
        let src = crate::external_runner_spec::tests::VALID;
        for prefix in drop {
            assert!(
                src.lines().any(|line| line.starts_with(prefix)),
                "fixture lacks {prefix:?}"
            );
        }
        src.lines()
            .filter(|line| !drop.iter().any(|prefix| line.starts_with(prefix)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn external_decl(drop: &[&str]) -> crate::external_runner_spec::ExternalRunnerFile {
        let cfg: ServeFileConfig = toml::from_str(&external_decl_source(drop)).unwrap();
        cfg.external_runner.unwrap()
    }

    /// VALID_ROUTING with `primary` (classifier, collector, arbiter-free,
    /// worker, reviewer) on the external runner beside the Claude planner.
    fn external_routing() -> String {
        let from = "runner = \"codex\"\nmodel = \"gpt-5.6-sol\"";
        assert!(VALID_ROUTING.contains(from));
        VALID_ROUTING.replacen(from, "runner = \"external\"\nmodel = \"fake-model-1\"", 1)
    }

    fn external_config(top: &str, routing: &str, drop: &[&str]) -> ServeFileConfig {
        toml::from_str(&format!("{top}\n{routing}\n{}", external_decl_source(drop))).unwrap()
    }

    fn assert_refused_naming(cfg: &ServeFileConfig, key: &str) {
        let err = validate_model_routing(cfg).expect_err("config should be refused");
        assert_eq!(err.exit_code(), 2, "{err}");
        assert!(
            err.to_string().contains(key),
            "error should name {key}: {err}"
        );
    }

    #[test]
    fn mixed_external_and_builtin_config_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        std::fs::write(
            &path,
            format!("{}\n{}", external_routing(), external_decl_source(&[])),
        )
        .unwrap();
        let cfg = load(&path, true).unwrap();
        assert_eq!(cfg.model_profiles.unwrap()["primary"].runner, "external");
        assert!(cfg.external_runner.is_some());
    }

    #[test]
    fn external_profile_requires_executable_or_agent_bin() {
        // No declaration at all.
        let mut cfg: ServeFileConfig = toml::from_str(&external_routing()).unwrap();
        assert_refused_naming(&cfg, "external_runner.executable");
        // A declaration without executable, and no agent_bin.
        cfg.external_runner = Some(external_decl(&["executable"]));
        assert_refused_naming(&cfg, "external_runner.executable");

        // agent_bin replaces executable. The single-runner rule still holds,
        // so every profile is external here.
        let all_external = external_routing().replacen(
            "runner = \"claude\"\nmodel = \"claude-opus-4-8\"",
            "runner = \"external\"\nmodel = \"fake-model-2\"",
            1,
        );
        let mut cfg = external_config("", &all_external, &["executable"]);
        assert_refused_naming(&cfg, "external_runner.executable");
        cfg.agent_bin = Some("/opt/fake-wrapper".into());
        validate_model_routing(&cfg).unwrap();

        // Mixed runners with agent_bin stay refused by the unchanged rule.
        let mut cfg = external_config("", &external_routing(), &[]);
        cfg.agent_bin = Some("/opt/fake-wrapper".into());
        assert_refused_naming(&cfg, "agent_bin");
    }

    #[test]
    fn external_effort_must_be_a_declared_effort() {
        let routing = external_routing().replacen(
            "model = \"fake-model-1\"",
            "model = \"fake-model-1\"\neffort = \"xhigh\"",
            1,
        );
        let cfg = external_config("", &routing, &[]);
        assert_refused_naming(&cfg, "model_profiles.primary.effort");
        // Without `efforts`, any [a-z0-9_-]{1,32} value is accepted.
        let cfg = external_config("", &routing, &["efforts"]);
        validate_model_routing(&cfg).unwrap();
    }

    #[test]
    fn external_profile_routed_to_a_role_requires_its_access_args() {
        // primary serves workers (read-write), reviewers (read-only), and the
        // classifier and collector (restricted).
        for key in ["args_read_write", "args_read_only", "args_restricted"] {
            let cfg = external_config("", &external_routing(), &[key]);
            assert_refused_naming(&cfg, &format!("external_runner.{key}"));
        }
        // An access level no routed role uses needs no list: route only the
        // restricted roles to the external profile.
        let mut only_restricted = String::from(
            "[model_profiles.primary]\nrunner = \"external\"\nmodel = \"fake-model-1\"\n\
             [model_profiles.planner]\nrunner = \"claude\"\nmodel = \"claude-opus-4-8\"\n\
             [routing.classifier]\nprimary = 100\n[routing.collector]\nprimary = 100\n\
             [routing.planner]\nplanner = 100\n",
        );
        for role in ["worker", "reviewer"] {
            for level in 1..=5 {
                only_restricted.push_str(&format!("[routing.{role}.{level}]\nplanner = 100\n"));
            }
        }
        let cfg = external_config(
            "",
            &only_restricted,
            &["args_read_write", "args_read_only", "args_mcp"],
        );
        validate_model_routing(&cfg).unwrap();
    }

    #[test]
    fn external_profile_routed_to_an_agent_mcp_role_requires_args_mcp() {
        let cfg = external_config("", &external_routing(), &["args_mcp"]);
        assert_refused_naming(&cfg, "external_runner.args_mcp");
    }

    #[test]
    fn external_profile_routed_to_classifier_or_arbiter_requires_final_text() {
        let cfg = external_config("", &external_routing(), &["final_text"]);
        assert_refused_naming(&cfg, "external_runner.stream.terminal.final_text");

        // Arbiter: classifier on the Claude planner profile, arbiter external.
        let arbiter = external_routing().replacen(
            "[routing.classifier]\nprimary = 100",
            "[routing.classifier]\nplanner = 100\n[routing.arbiter]\nprimary = 100",
            1,
        );
        let cfg = external_config("", &arbiter, &["final_text"]);
        assert_refused_naming(&cfg, "external_runner.stream.terminal.final_text");

        // Neither classifier nor arbiter external: final_text is optional.
        let neither = external_routing().replacen(
            "[routing.classifier]\nprimary = 100",
            "[routing.classifier]\nplanner = 100",
            1,
        );
        let cfg = external_config("", &neither, &["final_text"]);
        validate_model_routing(&cfg).unwrap();
    }

    #[test]
    fn usd_cost_limit_refused_when_routing_can_select_an_external_profile() {
        for key in ["max_turn_cost_usd", "max_task_cost_usd"] {
            let cfg = external_config(&format!("{key} = 1.0"), &external_routing(), &[]);
            assert_refused_naming(&cfg, key);
            let err = validate_routed_cost_limits(&cfg, Some(1.0), None).unwrap_err();
            assert_eq!(err.exit_code(), 2);
        }
        // A declared but unrouted external profile does not block USD limits.
        let unrouted = VALID_ROUTING.replacen(
            "[routing.classifier]",
            "[model_profiles.ext]\nrunner = \"external\"\nmodel = \"fake-model-1\"\n\n[routing.classifier]",
            1,
        );
        let cfg: ServeFileConfig = toml::from_str(&unrouted).unwrap();
        validate_routed_cost_limits(&cfg, Some(1.0), None).unwrap_err(); // codex still routed
        let unrouted_claude = unrouted.replacen(
            "runner = \"codex\"\nmodel = \"gpt-5.6-sol\"",
            "runner = \"claude\"\nmodel = \"claude-sonnet-4-6\"",
            1,
        );
        let cfg: ServeFileConfig = toml::from_str(&unrouted_claude).unwrap();
        validate_routed_cost_limits(&cfg, Some(1.0), Some(2.0)).unwrap();
    }

    #[test]
    fn reload_failing_an_external_cross_rule_keeps_the_running_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.toml");
        let write = |top: &str, routing: &str| {
            std::fs::write(
                &path,
                format!("{top}\n{routing}\n{}", external_decl_source(&[])),
            )
            .unwrap();
        };
        write("cap = 3", &external_routing());
        let mut running = load(&path, true).unwrap();
        let before = format!("{running:?}");

        write("cap = 7\nmax_turn_cost_usd = 1.0", &external_routing());
        let err = reload(&mut running, &path).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("max_turn_cost_usd"), "{err}");
        assert_eq!(format!("{running:?}"), before);

        let bad_effort = external_routing().replacen(
            "model = \"fake-model-1\"",
            "model = \"fake-model-1\"\neffort = \"xhigh\"",
            1,
        );
        write("cap = 7", &bad_effort);
        let err = reload(&mut running, &path).unwrap_err();
        assert!(
            err.to_string().contains("model_profiles.primary.effort"),
            "{err}"
        );
        assert_eq!(format!("{running:?}"), before);

        write("cap = 7", &external_routing());
        reload(&mut running, &path).unwrap();
        assert_eq!(running.cap, Some(7));
    }
}
