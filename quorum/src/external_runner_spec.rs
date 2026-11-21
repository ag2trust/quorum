//! The `[external_runner]` serve-config declaration: its file shape, the
//! table-local validation rules, and the declaration fingerprint.
//!
//! Contract: `docs/2026-11-20-external-cli-runner-technical-spec.md`,
//! "Configuration" > "Validation", and "Terms" (Fingerprint). Rules that need
//! the rest of the serve config (profiles, roles, cost limits, and whether
//! `executable` or `agent_bin` supplies the program) are not checked here.

// Slice 1 only parses and validates. The adapter and the session record
// consume the validated spec and its fingerprint in later slices.
#![cfg_attr(not(test), allow(dead_code))]

use quorum_core::error::{QuorumError, Result};
use quorum_core::routing_attempts::FailureDisposition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const DEFAULT_TERMINAL_EXIT_SECS: u64 = 30;

/// Largest integer every JCS (IEEE-754 double) consumer reads exactly.
const MAX_EXACT_INTEGER: u64 = 1 << 53;

// ---------------------------------------------------------------------------
// File shape
// ---------------------------------------------------------------------------

/// The `[external_runner]` table as written. `Option` fields that the
/// validator requires are optional here so a missing key is refused with its
/// full dotted name rather than a generic parse error.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalRunnerFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub efforts: Option<Vec<String>>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_read_write: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_read_only: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_restricted: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_mcp: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_session: Option<Vec<String>>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<StreamFile>,
    #[serde(default)]
    pub statuses: BTreeMap<String, StatusFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kinds: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub exits_without_terminal: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamFile {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_pointer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_line_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_exit_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<StartFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<TextFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<ActivityFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<TerminalFile>,
}

/// An exact-match value for `when` conditions and `accept_formats`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Scalar {
    Bool(bool),
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartFile {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accept_formats: Option<Vec<Scalar>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TextFile {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub when: BTreeMap<String, Scalar>,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityFile {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub when: BTreeMap<String, Scalar>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalFile {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question_context: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UsageFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncached_input: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatusFile {
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,
}

impl ExternalRunnerFile {
    /// The parsed table with every default filled in. Both the fingerprint
    /// and the restart-only reload comparison use this form, so writing a
    /// default explicitly is not a change.
    pub fn with_defaults(&self) -> Self {
        let mut filled = self.clone();
        if let Some(stream) = filled.stream.as_mut() {
            stream
                .terminal_exit_secs
                .get_or_insert(DEFAULT_TERMINAL_EXIT_SECS);
        }
        filled
    }
}

// ---------------------------------------------------------------------------
// Validated spec
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placeholder {
    Workspace,
    Model,
    Effort,
    AgentMcpUrl,
    SessionFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgPart {
    Literal(String),
    Placeholder(Placeholder),
}

/// One argv element: literal text with placeholders substituted in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgTemplate(pub Vec<ArgPart>);

/// The run field an `env` entry takes its value from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvSource {
    RunCapability,
    AgentMcpUrl,
    Repo,
    Agent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Blocked,
    Failure,
    Cancelled,
}

/// How a `failure` status picks its failure disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusDisposition {
    Fixed(FailureDisposition),
    ByErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSpec {
    pub outcome: Outcome,
    /// The exit code the status must accompany (128+N for signal N).
    pub exit: u8,
    /// `Some` exactly for the `failure` outcome.
    pub disposition: Option<StatusDisposition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSpec {
    pub event_type: String,
    pub status: String,
    pub final_text: Option<String>,
    pub error_kind: Option<String>,
    pub question: Option<String>,
    pub question_context: Vec<String>,
    pub usage: UsageFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSpec {
    pub type_pointer: String,
    pub max_line_bytes: u64,
    pub terminal_exit_secs: u64,
    pub start: Option<StartFile>,
    pub text: Option<TextFile>,
    pub activity: Option<ActivityFile>,
    pub terminal: TerminalSpec,
}

/// A validated `[external_runner]` declaration with defaults filled in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalSpec {
    pub executable: Option<String>,
    pub efforts: Option<Vec<String>>,
    pub args: Vec<ArgTemplate>,
    pub args_read_write: Option<Vec<ArgTemplate>>,
    pub args_read_only: Option<Vec<ArgTemplate>>,
    pub args_restricted: Option<Vec<ArgTemplate>>,
    pub args_mcp: Option<Vec<ArgTemplate>>,
    pub args_session: Option<Vec<ArgTemplate>>,
    pub env: BTreeMap<String, EnvSource>,
    pub stream: StreamSpec,
    pub statuses: BTreeMap<String, StatusSpec>,
    pub error_kinds: BTreeMap<String, FailureDisposition>,
    pub exits_without_terminal: BTreeMap<u8, FailureDisposition>,
    /// See [`fingerprint`].
    pub fingerprint: String,
}

fn refuse(key: &str, why: impl std::fmt::Display) -> QuorumError {
    QuorumError::Usage(format!("serve config {key}: {why}"))
}

/// Environment names the managed run environment (`managed_run_environment`)
/// or `ManagedRunEnv` set, beyond the refused `QUORUM_` prefix.
const MANAGED_RUN_SET_ENV: &[&str] = &["GH_CONFIG_DIR"];
const REFUSED_ENV_NAMES: &[&str] = &["PATH", "HOME"];
const REFUSED_ENV_PREFIXES: &[&str] = &["QUORUM_", "LD_", "DYLD_"];

impl ExternalSpec {
    /// Validate every table-local rule and compute the fingerprint.
    /// `agent_bin` is the serve config's effective `agent_bin`.
    pub fn from_file(file: &ExternalRunnerFile, agent_bin: Option<&str>) -> Result<Self> {
        let file = file.with_defaults();
        reject_nul(
            "external_runner",
            &serde_json::to_value(&file).map_err(|e| {
                QuorumError::Io(format!("external_runner could not be inspected: {e}"))
            })?,
        )?;

        if let Some(executable) = file.executable.as_deref() {
            if !std::path::Path::new(executable).is_absolute() {
                return Err(refuse(
                    "external_runner.executable",
                    "must be an absolute path",
                ));
            }
        }

        if let Some(efforts) = file.efforts.as_ref() {
            if efforts.is_empty() {
                return Err(refuse(
                    "external_runner.efforts",
                    "must list at least one effort when present",
                ));
            }
            let mut seen = BTreeSet::new();
            for (i, effort) in efforts.iter().enumerate() {
                let key = format!("external_runner.efforts[{i}]");
                if !valid_effort(effort) {
                    return Err(refuse(&key, "must match [a-z0-9_-]{1,32}"));
                }
                if !seen.insert(effort.as_str()) {
                    return Err(refuse(&key, "duplicates an earlier effort"));
                }
            }
        }

        let args = parse_arg_list("args", &file.args)?;
        let args_read_write = parse_opt_arg_list("args_read_write", &file.args_read_write)?;
        let args_read_only = parse_opt_arg_list("args_read_only", &file.args_read_only)?;
        let args_restricted = parse_opt_arg_list("args_restricted", &file.args_restricted)?;
        let args_mcp = parse_opt_arg_list("args_mcp", &file.args_mcp)?;
        let args_session = parse_opt_arg_list("args_session", &file.args_session)?;

        let mut env = BTreeMap::new();
        for (name, value) in &file.env {
            let key = format!("external_runner.env.{name}");
            check_env_name(&key, name)?;
            let source = match value.as_str() {
                "run_capability" => EnvSource::RunCapability,
                "agent_mcp_url" => EnvSource::AgentMcpUrl,
                "repo" => EnvSource::Repo,
                "agent" => EnvSource::Agent,
                _ => {
                    return Err(refuse(
                        &key,
                        "value must be one of run_capability, agent_mcp_url, repo, agent",
                    ))
                }
            };
            env.insert(name.clone(), source);
        }

        let stream = validate_stream(file.stream.as_ref())?;

        let error_kinds = match file.error_kinds.as_ref() {
            None => BTreeMap::new(),
            Some(kinds) => {
                let mut parsed = BTreeMap::new();
                for (kind, value) in kinds {
                    let key = format!("external_runner.error_kinds.{kind}");
                    parsed.insert(kind.clone(), parse_disposition(&key, value)?);
                }
                parsed
            }
        };

        let mut statuses = BTreeMap::new();
        let mut exits = BTreeMap::new();
        for (name, status) in &file.statuses {
            let key = format!("external_runner.statuses.{name}");
            let outcome = match status.outcome.as_str() {
                "success" => Outcome::Success,
                "blocked" => Outcome::Blocked,
                "failure" => Outcome::Failure,
                "cancelled" => Outcome::Cancelled,
                _ => {
                    return Err(refuse(
                        &format!("{key}.outcome"),
                        "must be one of success, blocked, failure, cancelled",
                    ))
                }
            };
            let Some(exit) = status.exit else {
                return Err(refuse(&format!("{key}.exit"), "is required"));
            };
            let exit = exit_code(&format!("{key}.exit"), exit)?;
            if let Some(other) = exits.insert(exit, name.clone()) {
                return Err(refuse(
                    &format!("{key}.exit"),
                    format!("exit {exit} is already used by external_runner.statuses.{other}"),
                ));
            }
            let disposition_key = format!("{key}.disposition");
            let disposition = match (outcome, status.disposition.as_deref()) {
                (Outcome::Failure, None) => {
                    return Err(refuse(
                        &disposition_key,
                        "is required for the failure outcome",
                    ))
                }
                (Outcome::Failure, Some("by_error_kind")) => {
                    if stream.terminal.error_kind.is_none() {
                        return Err(refuse(
                            &disposition_key,
                            "by_error_kind requires external_runner.stream.terminal.error_kind",
                        ));
                    }
                    if file.error_kinds.is_none() {
                        return Err(refuse(
                            &disposition_key,
                            "by_error_kind requires an external_runner.error_kinds table",
                        ));
                    }
                    Some(StatusDisposition::ByErrorKind)
                }
                (Outcome::Failure, Some(value)) => Some(StatusDisposition::Fixed(
                    parse_disposition(&disposition_key, value)?,
                )),
                (_, Some(_)) => {
                    return Err(refuse(
                        &disposition_key,
                        "is allowed only for the failure outcome",
                    ))
                }
                (_, None) => None,
            };
            if outcome == Outcome::Blocked && stream.terminal.question.is_none() {
                return Err(refuse(
                    &format!("{key}.outcome"),
                    "the blocked outcome requires external_runner.stream.terminal.question",
                ));
            }
            statuses.insert(
                name.clone(),
                StatusSpec {
                    outcome,
                    exit,
                    disposition,
                },
            );
        }
        if !statuses.values().any(|s| s.outcome == Outcome::Success) {
            return Err(refuse(
                "external_runner.statuses",
                "at least one status must have the success outcome",
            ));
        }

        let mut exits_without_terminal = BTreeMap::new();
        for (code, value) in &file.exits_without_terminal {
            let key = format!("external_runner.exits_without_terminal.{code}");
            let exit = match code.parse::<u8>() {
                Ok(exit) if exit.to_string() == *code => exit,
                _ => return Err(refuse(&key, "key must be an exit code 0-255")),
            };
            if exit == 0 {
                return Err(refuse(&key, "exit 0 cannot be listed"));
            }
            if let Some(status) = exits.get(&exit) {
                return Err(refuse(
                    &key,
                    format!("exit {exit} is external_runner.statuses.{status}.exit"),
                ));
            }
            let disposition = parse_disposition(&key, value)?;
            if !matches!(
                disposition,
                FailureDisposition::NonFailover | FailureDisposition::RetryableSameRoute
            ) {
                return Err(refuse(
                    &key,
                    "must be non_failover or retryable_same_route; failover needs a terminal event",
                ));
            }
            exits_without_terminal.insert(exit, disposition);
        }

        // Until the blocked outcome (slice 4) and resume (slice 6) land.
        if let Some((name, _)) = statuses.iter().find(|(_, s)| s.outcome == Outcome::Blocked) {
            return Err(refuse(
                &format!("external_runner.statuses.{name}.outcome"),
                "the blocked outcome is not supported yet",
            ));
        }
        if file.args_session.is_some() {
            return Err(refuse(
                "external_runner.args_session",
                "sessions are not supported yet",
            ));
        }

        Ok(Self {
            executable: file.executable.clone(),
            efforts: file.efforts.clone(),
            args,
            args_read_write,
            args_read_only,
            args_restricted,
            args_mcp,
            args_session,
            env,
            stream,
            statuses,
            error_kinds,
            exits_without_terminal,
            fingerprint: fingerprint(&file, agent_bin)?,
        })
    }
}

fn valid_effort(effort: &str) -> bool {
    (1..=32).contains(&effort.len())
        && effort
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

fn exit_code(key: &str, exit: i64) -> Result<u8> {
    u8::try_from(exit).map_err(|_| refuse(key, "must be an exit code 0-255"))
}

fn parse_disposition(key: &str, value: &str) -> Result<FailureDisposition> {
    match value {
        "provider_unavailable" => Ok(FailureDisposition::ProviderUnavailable),
        "profile_unavailable" => Ok(FailureDisposition::ProfileUnavailable),
        "retryable_same_route" => Ok(FailureDisposition::RetryableSameRoute),
        "non_failover" => Ok(FailureDisposition::NonFailover),
        _ => Err(refuse(
            key,
            "disposition must be one of provider_unavailable, profile_unavailable, retryable_same_route, non_failover",
        )),
    }
}

fn check_env_name(key: &str, name: &str) -> Result<()> {
    let mut bytes = name.bytes();
    let valid = matches!(bytes.next(), Some(b) if b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid {
        return Err(refuse(key, "is not a valid environment name"));
    }
    if crate::serve::runner::MANAGED_RUN_REMOVED_ENV.contains(&name) {
        return Err(refuse(key, "is removed from every managed run"));
    }
    if MANAGED_RUN_SET_ENV.contains(&name) || REFUSED_ENV_NAMES.contains(&name) {
        return Err(refuse(key, "is set by the managed run environment"));
    }
    if let Some(prefix) = REFUSED_ENV_PREFIXES.iter().find(|p| name.starts_with(**p)) {
        return Err(refuse(
            key,
            format!("names starting with {prefix} are refused"),
        ));
    }
    Ok(())
}

fn parse_opt_arg_list(list: &str, args: &Option<Vec<String>>) -> Result<Option<Vec<ArgTemplate>>> {
    args.as_ref().map(|a| parse_arg_list(list, a)).transpose()
}

fn parse_arg_list(list: &str, args: &[String]) -> Result<Vec<ArgTemplate>> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| parse_arg(list, &format!("external_runner.{list}[{i}]"), arg))
        .collect()
}

/// Parse one argv element. `{{` and `}}` are literal braces; `{name}` is a
/// placeholder; any other brace is unbalanced.
fn parse_arg(list: &str, key: &str, arg: &str) -> Result<ArgTemplate> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut chars = arg.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                literal.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                literal.push('}');
            }
            '}' => return Err(refuse(key, "has an unbalanced '}'")),
            '{' => {
                let mut name = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some('{') | None => return Err(refuse(key, "has an unbalanced '{'")),
                        Some(c) => name.push(c),
                    }
                }
                let placeholder = match name.as_str() {
                    "workspace" => Placeholder::Workspace,
                    "model" => Placeholder::Model,
                    "effort" => Placeholder::Effort,
                    "agent_mcp_url" => Placeholder::AgentMcpUrl,
                    "session_file" => Placeholder::SessionFile,
                    _ => return Err(refuse(key, format!("unknown placeholder {{{name}}}"))),
                };
                let allowed = match placeholder {
                    Placeholder::Workspace | Placeholder::Model | Placeholder::Effort => true,
                    Placeholder::AgentMcpUrl => list == "args_mcp",
                    Placeholder::SessionFile => list == "args_session",
                };
                if !allowed {
                    let only = if placeholder == Placeholder::AgentMcpUrl {
                        "args_mcp"
                    } else {
                        "args_session"
                    };
                    return Err(refuse(
                        key,
                        format!("placeholder {{{name}}} is allowed only in external_runner.{only}"),
                    ));
                }
                if !literal.is_empty() {
                    parts.push(ArgPart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(ArgPart::Placeholder(placeholder));
            }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() {
        parts.push(ArgPart::Literal(literal));
    }
    Ok(ArgTemplate(parts))
}

/// RFC 6901: empty, or `/`-separated tokens where `~` is followed by `0` or `1`.
fn check_pointer(key: &str, pointer: &str) -> Result<()> {
    let malformed = || refuse(key, "is not a valid JSON Pointer (RFC 6901)");
    if pointer.is_empty() {
        return Ok(());
    }
    if !pointer.starts_with('/') {
        return Err(malformed());
    }
    let mut chars = pointer.chars();
    while let Some(c) = chars.next() {
        if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return Err(malformed());
        }
    }
    Ok(())
}

fn check_opt_pointer(key: &str, pointer: Option<&String>) -> Result<()> {
    pointer.map_or(Ok(()), |p| check_pointer(key, p))
}

fn check_when(key: &str, when: &BTreeMap<String, Scalar>) -> Result<()> {
    for (pointer, value) in when {
        check_pointer(&format!("{key}.when[{pointer:?}]"), pointer)?;
        check_scalar(&format!("{key}.when[{pointer:?}]"), value)?;
    }
    Ok(())
}

fn check_scalar(key: &str, value: &Scalar) -> Result<()> {
    if let Scalar::Integer(n) = value {
        if n.unsigned_abs() > MAX_EXACT_INTEGER {
            return Err(refuse(key, "integer magnitude must not exceed 2^53"));
        }
    }
    Ok(())
}

fn validate_stream(stream: Option<&StreamFile>) -> Result<StreamSpec> {
    let Some(stream) = stream else {
        return Err(refuse("external_runner.stream", "is required"));
    };
    let Some(type_pointer) = stream.type_pointer.clone() else {
        return Err(refuse("external_runner.stream.type", "is required"));
    };
    check_pointer("external_runner.stream.type", &type_pointer)?;
    let max_line_bytes = match stream.max_line_bytes {
        None => {
            return Err(refuse(
                "external_runner.stream.max_line_bytes",
                "is required",
            ))
        }
        Some(0) => {
            return Err(refuse(
                "external_runner.stream.max_line_bytes",
                "must be positive",
            ))
        }
        Some(n) if n > MAX_EXACT_INTEGER => {
            return Err(refuse(
                "external_runner.stream.max_line_bytes",
                "must not exceed 2^53",
            ))
        }
        Some(n) => n,
    };
    let terminal_exit_secs = stream
        .terminal_exit_secs
        .unwrap_or(DEFAULT_TERMINAL_EXIT_SECS);
    if terminal_exit_secs == 0 || terminal_exit_secs > MAX_EXACT_INTEGER {
        return Err(refuse(
            "external_runner.stream.terminal_exit_secs",
            "must be between 1 and 2^53",
        ));
    }

    if let Some(start) = stream.start.as_ref() {
        check_opt_pointer("external_runner.stream.start.format", start.format.as_ref())?;
        match (&start.format, &start.accept_formats) {
            (Some(_), None) => {
                return Err(refuse(
                    "external_runner.stream.start.accept_formats",
                    "is required when start.format is declared",
                ))
            }
            (None, Some(_)) => {
                return Err(refuse(
                    "external_runner.stream.start.format",
                    "is required when start.accept_formats is declared",
                ))
            }
            _ => {}
        }
        for (i, value) in start.accept_formats.iter().flatten().enumerate() {
            check_scalar(
                &format!("external_runner.stream.start.accept_formats[{i}]"),
                value,
            )?;
        }
    }
    if let Some(text) = stream.text.as_ref() {
        check_when("external_runner.stream.text", &text.when)?;
        check_pointer("external_runner.stream.text.text", &text.text)?;
        check_opt_pointer("external_runner.stream.text.id", text.id.as_ref())?;
    }
    if let Some(activity) = stream.activity.as_ref() {
        check_when("external_runner.stream.activity", &activity.when)?;
        check_opt_pointer(
            "external_runner.stream.activity.summary",
            activity.summary.as_ref(),
        )?;
    }

    let Some(terminal) = stream.terminal.as_ref() else {
        return Err(refuse("external_runner.stream.terminal", "is required"));
    };
    let Some(event_type) = terminal.event_type.clone() else {
        return Err(refuse(
            "external_runner.stream.terminal.type",
            "is required",
        ));
    };
    let Some(status) = terminal.status.clone() else {
        return Err(refuse(
            "external_runner.stream.terminal.status",
            "is required",
        ));
    };
    check_pointer("external_runner.stream.terminal.status", &status)?;
    for (name, pointer) in [
        ("final_text", terminal.final_text.as_ref()),
        ("error_kind", terminal.error_kind.as_ref()),
        ("question", terminal.question.as_ref()),
    ] {
        check_opt_pointer(&format!("external_runner.stream.terminal.{name}"), pointer)?;
    }
    for (i, pointer) in terminal.question_context.iter().flatten().enumerate() {
        check_pointer(
            &format!("external_runner.stream.terminal.question_context[{i}]"),
            pointer,
        )?;
    }
    let usage = terminal.usage.clone().unwrap_or_default();
    for (name, pointer) in [
        ("uncached_input", usage.uncached_input.as_ref()),
        ("cached_input", usage.cached_input.as_ref()),
        ("cache_write_input", usage.cache_write_input.as_ref()),
        ("output", usage.output.as_ref()),
    ] {
        check_opt_pointer(
            &format!("external_runner.stream.terminal.usage.{name}"),
            pointer,
        )?;
    }

    Ok(StreamSpec {
        type_pointer,
        max_line_bytes,
        terminal_exit_secs,
        start: stream.start.clone(),
        text: stream.text.clone(),
        activity: stream.activity.clone(),
        terminal: TerminalSpec {
            event_type,
            status,
            final_text: terminal.final_text.clone(),
            error_kind: terminal.error_kind.clone(),
            question: terminal.question.clone(),
            question_context: terminal.question_context.clone().unwrap_or_default(),
            usage,
        },
    })
}

/// Invariant 9: no embedded NUL in any key or string of the table.
fn reject_nul(path: &str, value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::String(s) if s.contains('\0') => {
            Err(refuse(path, "must not contain NUL"))
        }
        serde_json::Value::Array(items) => items
            .iter()
            .enumerate()
            .try_for_each(|(i, item)| reject_nul(&format!("{path}[{i}]"), item)),
        serde_json::Value::Object(members) => members.iter().try_for_each(|(k, v)| {
            if k.contains('\0') {
                return Err(refuse(path, "a key must not contain NUL"));
            }
            reject_nul(&format!("{path}.{k}"), v)
        }),
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Fingerprint
// ---------------------------------------------------------------------------

/// Lowercase hex SHA-256 of the RFC 8785 (JCS) canonical JSON of the parsed
/// table after defaults, plus a top-level `"agent_bin"` member holding the
/// effective `agent_bin` or `null` (spec "Terms").
pub fn fingerprint(file: &ExternalRunnerFile, agent_bin: Option<&str>) -> Result<String> {
    let mut value = serde_json::to_value(file.with_defaults())
        .map_err(|e| QuorumError::Io(format!("external_runner could not be serialized: {e}")))?;
    let Some(members) = value.as_object_mut() else {
        return Err(QuorumError::Io(
            "external_runner did not serialize to an object".into(),
        ));
    };
    members.insert(
        "agent_bin".into(),
        agent_bin.map_or(serde_json::Value::Null, |bin| bin.into()),
    );
    let canonical = canonical_json(&value)?;
    Ok(sha256_hex(canonical.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// RFC 8785 canonical JSON for values without fractional numbers: no
/// whitespace, members sorted by UTF-16 code units, ECMAScript string
/// escaping (which `serde_json` string output matches), and integers within
/// the exactly representable double range.
fn canonical_json(value: &serde_json::Value) -> Result<String> {
    let mut out = String::new();
    write_canonical(value, &mut out)?;
    Ok(out)
}

fn write_canonical(value: &serde_json::Value, out: &mut String) -> Result<()> {
    use serde_json::Value;
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            let exact = n
                .as_i64()
                .map(|i| i.unsigned_abs())
                .or_else(|| n.as_u64())
                .filter(|m| *m <= MAX_EXACT_INTEGER);
            if exact.is_none() {
                return Err(QuorumError::Usage(
                    "serve config external_runner: numbers must be integers within 2^53".into(),
                ));
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => write_string(s, out)?,
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out)?;
            }
            out.push(']');
        }
        Value::Object(members) => {
            let mut sorted: Vec<_> = members.iter().collect();
            sorted.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, (key, member)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out)?;
                out.push(':');
                write_canonical(member, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn write_string(s: &str, out: &mut String) -> Result<()> {
    let encoded = serde_json::to_string(s)
        .map_err(|e| QuorumError::Io(format!("external_runner string encoding: {e}")))?;
    out.push_str(&encoded);
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The spec's example declaration, minus the parts gated on later slices
    /// (the blocked status and `args_session`).
    pub(crate) const VALID: &str = r#"
[external_runner]
executable = "/usr/local/bin/fake-cli"
efforts = ["low", "medium", "high"]
args            = ["exec", "--workspace", "{workspace}", "--model", "{model}", "--effort", "{effort}", "-"]
args_read_write = ["--tools", "read-write"]
args_read_only  = ["--tools", "read-only"]
args_restricted = ["--tools", "none"]
args_mcp        = ["--mcp-url", "{agent_mcp_url}"]

[external_runner.env]
FAKE_MCP_TOKEN = "run_capability"

[external_runner.stream]
type = "/type"
max_line_bytes = 1048576
start = { type = "run.started", format = "/format", accept_formats = [2] }
text     = { type = "item.completed", when = { "/item/type" = "message" }, text = "/item/text", id = "/item/id" }
activity = { type = "item.started", when = { "/item/type" = "tool_call" }, summary = "/item/name" }

[external_runner.stream.terminal]
type          = "run.completed"
status        = "/status"
final_text    = "/final_text"
error_kind    = "/error/kind"
question      = "/blocker/question"
question_context = ["/blocker/reason", "/blocker/summary", "/blocker/details"]
usage = { uncached_input = "/usage/input", cached_input = "/usage/cache_read", cache_write_input = "/usage/cache_write", output = "/usage/output" }

[external_runner.statuses]
completed      = { outcome = "success",   exit = 0 }
cancelled      = { outcome = "cancelled", exit = 143 }
limit_reached  = { outcome = "failure",   exit = 3, disposition = "non_failover" }
internal_error = { outcome = "failure",   exit = 1, disposition = "non_failover" }
mcp_error      = { outcome = "failure",   exit = 6, disposition = "retryable_same_route" }
provider_error = { outcome = "failure",   exit = 4, disposition = "by_error_kind" }

[external_runner.error_kinds]
auth              = "provider_unavailable"
model_unavailable = "profile_unavailable"
overloaded        = "retryable_same_route"
bad_request       = "non_failover"

[external_runner.exits_without_terminal]
2 = "non_failover"
"#;

    #[derive(Deserialize)]
    struct Wrapper {
        external_runner: ExternalRunnerFile,
    }

    fn parse(src: &str) -> ExternalRunnerFile {
        toml::from_str::<Wrapper>(src).unwrap().external_runner
    }

    fn spec(src: &str) -> Result<ExternalSpec> {
        ExternalSpec::from_file(&parse(src), None)
    }

    /// Apply a textual edit to the valid declaration; panics if `from` is
    /// absent so a stale fixture cannot pass vacuously.
    fn edit(from: &str, to: &str) -> String {
        assert!(VALID.contains(from), "fixture lacks {from:?}");
        VALID.replacen(from, to, 1)
    }

    fn assert_refused(src: &str, key: &str) {
        let err = spec(src).expect_err("declaration should be refused");
        assert_eq!(err.exit_code(), 2, "{err}");
        let msg = err.to_string();
        assert!(msg.contains(key), "error should name {key}: {msg}");
    }

    #[test]
    fn valid_declaration_loads_with_defaults() {
        let spec = spec(VALID).unwrap();
        assert_eq!(spec.stream.terminal_exit_secs, DEFAULT_TERMINAL_EXIT_SECS);
        assert_eq!(spec.stream.max_line_bytes, 1_048_576);
        assert_eq!(spec.env["FAKE_MCP_TOKEN"], EnvSource::RunCapability);
        assert_eq!(
            spec.statuses["provider_error"].disposition,
            Some(StatusDisposition::ByErrorKind)
        );
        assert_eq!(spec.statuses["cancelled"].exit, 143);
        assert_eq!(
            spec.exits_without_terminal[&2],
            FailureDisposition::NonFailover
        );
        assert_eq!(
            spec.args_mcp.unwrap()[1],
            ArgTemplate(vec![ArgPart::Placeholder(Placeholder::AgentMcpUrl)])
        );
        assert_eq!(spec.fingerprint.len(), 64);
        assert!(spec
            .fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    }

    #[test]
    fn placeholders_substitute_inside_an_element_with_brace_escapes() {
        let arg = parse_arg("args", "k", "{{x}}--ws={workspace}/{model}}}").unwrap();
        assert_eq!(
            arg,
            ArgTemplate(vec![
                ArgPart::Literal("{x}--ws=".into()),
                ArgPart::Placeholder(Placeholder::Workspace),
                ArgPart::Literal("/".into()),
                ArgPart::Placeholder(Placeholder::Model),
                ArgPart::Literal("}".into()),
            ])
        );
        let src = edit(r#""-"]"#, r#""-", "{{literal}}"]"#);
        spec(&src).unwrap();
    }

    #[test]
    fn refuses_a_relative_executable() {
        assert_refused(
            &edit(r#""/usr/local/bin/fake-cli""#, r#""bin/fake-cli""#),
            "external_runner.executable",
        );
    }

    #[test]
    fn refuses_bad_effort_lists() {
        assert_refused(
            &edit(r#""high"]"#, r#""High"]"#),
            "external_runner.efforts[2]",
        );
        assert_refused(
            &edit(r#"["low", "medium", "high"]"#, "[]"),
            "external_runner.efforts",
        );
    }

    #[test]
    fn refuses_unknown_and_misplaced_placeholders_and_unbalanced_braces() {
        assert_refused(
            &edit(r#""{effort}""#, r#""{reasoning}""#),
            "external_runner.args[6]",
        );
        assert_refused(
            &edit(r#""{effort}""#, r#""{agent_mcp_url}""#),
            "external_runner.args[6]",
        );
        assert_refused(
            &edit(r#""{effort}""#, r#""{session_file}""#),
            "external_runner.args[6]",
        );
        assert_refused(
            &edit(
                r#"["--tools", "read-only"]"#,
                r#"["--mcp={agent_mcp_url}"]"#,
            ),
            "external_runner.args_read_only[0]",
        );
        assert_refused(
            &edit(r#""{agent_mcp_url}"]"#, r#""{session_file}"]"#),
            "external_runner.args_mcp[1]",
        );
        for bad in [r#""{effort""#, r#""effort}""#, r#""{ef{fort}""#, r#""{}}""#] {
            assert_refused(&edit(r#""{effort}""#, bad), "external_runner.args[6]");
        }
    }

    #[test]
    fn refuses_unsafe_env_names_and_unknown_values() {
        let mut names: Vec<String> = [
            "1BAD",
            "BAD-NAME",
            "",
            "PATH",
            "HOME",
            "GH_CONFIG_DIR",
            "QUORUM_ANYTHING",
            crate::serve::runner::AGENT_MCP_URL_ENV,
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        names.extend(
            crate::serve::runner::MANAGED_RUN_REMOVED_ENV
                .iter()
                .map(|s| s.to_string()),
        );
        for name in names {
            let src = edit("FAKE_MCP_TOKEN = ", &format!("{name:?} = "));
            assert_refused(&src, &format!("external_runner.env.{name}"));
        }
        assert_refused(
            &edit(r#"= "run_capability""#, r#"= "orchestrator_token""#),
            "external_runner.env.FAKE_MCP_TOKEN",
        );
    }

    #[test]
    fn refuses_malformed_json_pointers() {
        for (from, to, key) in [
            (
                r#"type = "/type""#,
                r#"type = "type""#,
                "external_runner.stream.type",
            ),
            (
                r#"status        = "/status""#,
                r#"status        = "/st~2atus""#,
                "external_runner.stream.terminal.status",
            ),
            (
                r#""/blocker/details""#,
                r#""/blocker/~""#,
                "external_runner.stream.terminal.question_context[2]",
            ),
            (
                r#""/usage/output""#,
                r#""usage/output""#,
                "external_runner.stream.terminal.usage.output",
            ),
            (
                r#""/item/text""#,
                r#""item""#,
                "external_runner.stream.text.text",
            ),
            (
                r#"when = { "/item/type" = "message" }"#,
                r#"when = { "item/type" = "message" }"#,
                "external_runner.stream.text.when",
            ),
            (
                r#""/format""#,
                r#""format""#,
                "external_runner.stream.start.format",
            ),
            (
                r#""/error/kind""#,
                r#""error/kind""#,
                "external_runner.stream.terminal.error_kind",
            ),
        ] {
            assert_refused(&edit(from, to), key);
        }
        // Escapes ~0 and ~1 are valid.
        spec(&edit(r#""/item/name""#, r#""/item/a~0b~1c""#)).unwrap();
    }

    #[test]
    fn refuses_missing_stream_type_terminal_type_and_status() {
        assert_refused(
            &edit("type = \"/type\"\n", ""),
            "external_runner.stream.type",
        );
        assert_refused(
            &edit("type          = \"run.completed\"\n", ""),
            "external_runner.stream.terminal.type",
        );
        assert_refused(
            &edit("status        = \"/status\"\n", ""),
            "external_runner.stream.terminal.status",
        );
    }

    #[test]
    fn refuses_status_exit_rules() {
        assert_refused(
            &edit(
                r#"outcome = "cancelled", exit = 143"#,
                r#"outcome = "cancelled""#,
            ),
            "external_runner.statuses.cancelled.exit",
        );
        let err = spec(&edit("exit = 143", "exit = 3")).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.to_string().contains("external_runner.statuses.")
                && err.to_string().contains(".exit"),
            "{err}"
        );
        assert_refused(
            &edit(
                r#"outcome = "success",   exit = 0"#,
                r#"outcome = "failure", exit = 0, disposition = "non_failover""#,
            ),
            "external_runner.statuses: at least one status must have the success outcome",
        );
        assert_refused(
            &edit("exit = 143", "exit = 256"),
            "external_runner.statuses.cancelled.exit",
        );
    }

    #[test]
    fn refuses_blocked_status_without_question_then_refuses_blocked_until_slice_4() {
        let blocked = edit(
            "[external_runner.statuses]\n",
            "[external_runner.statuses]\nblocked = { outcome = \"blocked\", exit = 5 }\n",
        );
        let no_question = blocked.replacen("question      = \"/blocker/question\"\n", "", 1);
        let err = spec(&no_question).unwrap_err();
        assert!(err
            .to_string()
            .contains("external_runner.statuses.blocked.outcome"));
        assert!(err.to_string().contains("terminal.question"), "{err}");
        let err = spec(&blocked).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err
            .to_string()
            .contains("external_runner.statuses.blocked.outcome"));
        assert!(err.to_string().contains("not supported yet"), "{err}");
    }

    #[test]
    fn refuses_args_session_until_slice_6() {
        let src = edit(
            "args_mcp        =",
            "args_session = [\"--session\", \"{session_file}\"]\nargs_mcp        =",
        );
        assert_refused(&src, "external_runner.args_session");
        assert!(spec(&src)
            .unwrap_err()
            .to_string()
            .contains("not supported yet"));
    }

    #[test]
    fn refuses_bad_dispositions() {
        for bad in ["unclassified", "failover", "by_error_kind"] {
            assert_refused(
                &edit(
                    r#"auth              = "provider_unavailable""#,
                    &format!("auth = {bad:?}"),
                ),
                "external_runner.error_kinds.auth",
            );
        }
        assert_refused(
            &edit(
                r#"exit = 1, disposition = "non_failover""#,
                r#"exit = 1, disposition = "unclassified""#,
            ),
            "external_runner.statuses.internal_error.disposition",
        );
        assert_refused(
            &edit(r#"exit = 1, disposition = "non_failover""#, "exit = 1"),
            "external_runner.statuses.internal_error.disposition",
        );
        assert_refused(
            &edit(
                "exit = 143 }",
                r#"exit = 143, disposition = "non_failover" }"#,
            ),
            "external_runner.statuses.cancelled.disposition",
        );
    }

    #[test]
    fn refuses_by_error_kind_without_error_kind_pointer_or_table() {
        assert_refused(
            &edit("error_kind    = \"/error/kind\"\n", ""),
            "external_runner.statuses.provider_error.disposition",
        );
        let no_table = edit("[external_runner.error_kinds]\n", "")
            .replacen("auth              = \"provider_unavailable\"\n", "", 1)
            .replacen("model_unavailable = \"profile_unavailable\"\n", "", 1)
            .replacen("overloaded        = \"retryable_same_route\"\n", "", 1)
            .replacen("bad_request       = \"non_failover\"\n", "", 1);
        let err = spec(&no_table).unwrap_err();
        assert!(
            err.to_string()
                .contains("external_runner.statuses.provider_error.disposition")
                && err.to_string().contains("error_kinds table"),
            "{err}"
        );
    }

    #[test]
    fn refuses_bad_exits_without_terminal() {
        assert_refused(
            &edit(r#"2 = "non_failover""#, r#"0 = "non_failover""#),
            "external_runner.exits_without_terminal.0",
        );
        assert_refused(
            &edit(r#"2 = "non_failover""#, r#"4 = "non_failover""#),
            "external_runner.exits_without_terminal.4",
        );
        for bad in [
            "provider_unavailable",
            "profile_unavailable",
            "unclassified",
        ] {
            assert_refused(
                &edit(r#"2 = "non_failover""#, &format!("2 = {bad:?}")),
                "external_runner.exits_without_terminal.2",
            );
        }
        assert_refused(
            &edit(r#"2 = "non_failover""#, r#""02" = "non_failover""#),
            "external_runner.exits_without_terminal.02",
        );
        spec(&edit(
            r#"2 = "non_failover""#,
            r#"2 = "retryable_same_route""#,
        ))
        .unwrap();
    }

    #[test]
    fn refuses_embedded_nul() {
        assert_refused(
            &edit(r#""--tools", "none""#, r#""--tools", "no\u0000ne""#),
            "external_runner.args_restricted[1]",
        );
    }

    #[test]
    fn refuses_unknown_keys() {
        let err = toml::from_str::<Wrapper>(&edit(
            "[external_runner.env]",
            "bogus = 1\n[external_runner.env]",
        ))
        .err()
        .expect("unknown key refused");
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    #[test]
    fn fingerprint_is_stable_across_key_order_and_explicit_defaults() {
        let a = r#"
[external_runner]
executable = "/bin/fake"
args = ["a"]
[external_runner.stream]
type = "/type"
max_line_bytes = 10
terminal = { type = "done", status = "/status" }
[external_runner.statuses]
ok = { outcome = "success", exit = 0 }
bad = { outcome = "failure", exit = 1, disposition = "non_failover" }
"#;
        let b = r#"
[external_runner.statuses]
bad = { disposition = "non_failover", exit = 1, outcome = "failure" }
ok = { exit = 0, outcome = "success" }
[external_runner.stream]
terminal_exit_secs = 30
terminal = { status = "/status", type = "done" }
max_line_bytes = 10
type = "/type"
[external_runner]
args = ["a"]
executable = "/bin/fake"
"#;
        let fa = spec(a).unwrap().fingerprint;
        assert_eq!(fa, spec(b).unwrap().fingerprint);
        // Pin the canonical form: defaults filled, members sorted, no
        // whitespace, and a top-level agent_bin member.
        let canonical = {
            let mut v = serde_json::to_value(parse(a).with_defaults()).unwrap();
            v.as_object_mut()
                .unwrap()
                .insert("agent_bin".into(), serde_json::Value::Null);
            canonical_json(&v).unwrap()
        };
        assert_eq!(
            canonical,
            r#"{"agent_bin":null,"args":["a"],"env":{},"executable":"/bin/fake","exits_without_terminal":{},"statuses":{"bad":{"disposition":"non_failover","exit":1,"outcome":"failure"},"ok":{"exit":0,"outcome":"success"}},"stream":{"max_line_bytes":10,"terminal":{"status":"/status","type":"done"},"terminal_exit_secs":30,"type":"/type"}}"#
        );
        assert_eq!(fa, sha256_hex(canonical.as_bytes()));
    }

    #[test]
    fn fingerprint_changes_with_every_field_and_with_agent_bin() {
        let base = spec(VALID).unwrap().fingerprint;
        let mutations = [
            (
                r#""/usr/local/bin/fake-cli""#,
                r#""/usr/local/bin/other-cli""#,
            ),
            (r#""high"]"#, r#""xhigh"]"#),
            (r#""-"]"#, r#""--"]"#),
            (r#"["--tools", "read-write"]"#, r#"["--tools", "rw"]"#),
            (r#"["--tools", "read-only"]"#, r#"["--tools", "ro"]"#),
            (r#"["--tools", "none"]"#, r#"["--tools", "off"]"#),
            (r#""--mcp-url""#, r#""--mcp""#),
            (r#"= "run_capability""#, r#"= "agent""#),
            (r#"type = "/type""#, r#"type = "/kind""#),
            ("max_line_bytes = 1048576", "max_line_bytes = 1048577"),
            (
                "max_line_bytes = 1048576",
                "max_line_bytes = 1048576\nterminal_exit_secs = 31",
            ),
            ("accept_formats = [2]", "accept_formats = [3]"),
            (r#""/item/text""#, r#""/item/body""#),
            (r#"summary = "/item/name""#, r#"summary = "/item/title""#),
            (r#""run.completed""#, r#""run.done""#),
            (r#""/final_text""#, r#""/final""#),
            (r#""/blocker/question""#, r#""/blocker/q""#),
            (r#""/usage/output""#, r#""/usage/out""#),
            ("exit = 143", "exit = 130"),
            (
                r#"exit = 6, disposition = "retryable_same_route""#,
                r#"exit = 6, disposition = "non_failover""#,
            ),
            (
                r#"overloaded        = "retryable_same_route""#,
                r#"overloaded = "non_failover""#,
            ),
            (r#"2 = "non_failover""#, r#"2 = "retryable_same_route""#),
        ];
        for (from, to) in mutations {
            let changed = spec(&edit(from, to)).unwrap().fingerprint;
            assert_ne!(changed, base, "{from} -> {to} must change the fingerprint");
        }
        let file = parse(VALID);
        let with_bin = ExternalSpec::from_file(&file, Some("/opt/wrapper")).unwrap();
        let other_bin = ExternalSpec::from_file(&file, Some("/opt/other")).unwrap();
        assert_ne!(with_bin.fingerprint, base, "agent_bin set vs null");
        assert_ne!(with_bin.fingerprint, other_bin.fingerprint);
        // Removing an optional list is a change too.
        let no_mcp = edit(
            "args_mcp        = [\"--mcp-url\", \"{agent_mcp_url}\"]\n",
            "",
        );
        assert_ne!(spec(&no_mcp).unwrap().fingerprint, base);
    }

    #[test]
    fn canonical_json_follows_rfc_8785() {
        // Members sort by UTF-16 code units: U+1F600 (a surrogate pair,
        // 0xD83D...) sorts before U+FB01 even though its UTF-8 bytes sort after.
        let value = serde_json::json!({
            "\u{fb01}": 1,
            "\u{1f600}": 2,
            "b": [true, null, -7],
            "a": "\u{1}\u{1f}\t\n\"\\/é\u{7f}",
        });
        assert_eq!(
            canonical_json(&value).unwrap(),
            "{\"a\":\"\\u0001\\u001f\\t\\n\\\"\\\\/é\u{7f}\",\"b\":[true,null,-7],\"\u{1f600}\":2,\"\u{fb01}\":1}"
        );
        assert!(canonical_json(&serde_json::json!(1.5)).is_err());
        assert!(canonical_json(&serde_json::json!(MAX_EXACT_INTEGER + 1)).is_err());
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
