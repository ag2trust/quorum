//! External runner launch assembly: the access level, the argv vector, and
//! the layered child environment for one run. Pure: nothing here spawns a
//! process; the process adapter applies the results to its command.
//!
//! Contract: `docs/2026-11-20-external-cli-runner-technical-spec.md`,
//! "Access levels", "Launch", "Roles", and "Sessions".
//!
//! Metadata-only: the prompt never enters argv (the adapter writes it to
//! stdin), no placeholder carries the run capability, and failure details
//! name only declared keys, never run values.

use super::runner::{
    AgentMcpRole, FailureDisposition, LaunchRequest, ManagedRunEnv, RunnerFailure,
    AGENT_MCP_URL_ENV, MANAGED_RUN_REMOVED_ENV,
};
use super::session_store::SESSION_FILE;
use crate::external_runner_spec::{
    AccessLevel, ArgPart, ArgTemplate, EnvSource, ExternalSpec, Placeholder,
};
use std::path::Path;
use tokio::process::Command;

/// Spec "Launch": a slot run's process receives its slot directory under this
/// name. The daemon supplies it as a launch environment entry on slot runs;
/// the adapter honors it only for a slot role with sessions enabled and
/// withholds it from every other run. The `QUORUM_` prefix keeps the
/// operator's `env` table from setting it.
pub const SESSION_DIR_ENV: &str = "QUORUM_SESSION_DIR";

/// Names a declared mapping may never set. Startup validation
/// (`external_runner_spec.rs`, `check_env_name`) already refuses them; this
/// launch-time check keeps a spec that bypassed validation from reaching the
/// child with one.
const REFUSED_ENV_NAMES: &[&str] = &["PATH", "HOME", "GH_CONFIG_DIR"];
const REFUSED_ENV_PREFIXES: &[&str] = &["QUORUM_", "LD_", "DYLD_"];

/// Spec "Access levels": the access level follows the managed launch role.
pub fn access_level(managed: Option<AgentMcpRole>) -> AccessLevel {
    match managed {
        Some(AgentMcpRole::Worker) => AccessLevel::ReadWrite,
        Some(AgentMcpRole::Reviewer | AgentMcpRole::Planner) => AccessLevel::ReadOnly,
        None => AccessLevel::Restricted,
    }
}

/// Spec "Access levels": every managed launch is wired to the agent MCP;
/// unmanaged (restricted) launches are not.
pub fn wired_to_agent_mcp(managed: Option<AgentMcpRole>) -> bool {
    managed.is_some()
}

fn launch_failure(detail: impl Into<String>) -> RunnerFailure {
    RunnerFailure::classified(
        FailureDisposition::NonFailover,
        format!("external runner launch refused: {}", detail.into()),
        std::io::ErrorKind::InvalidInput,
    )
}

/// Spec "Roles": the worker and reviewer launches are the slot runs
/// (`worker`, `reviewer-r1`, `reviewer-r2`). Planner and unmanaged launches
/// (classifier, arbiter, collector) have no slot.
fn has_slot(managed: Option<AgentMcpRole>) -> bool {
    matches!(managed, Some(AgentMcpRole::Worker | AgentMcpRole::Reviewer))
}

/// Spec "Sessions": the slot directory in effect for this run, present only
/// when sessions are enabled (`args_session` declared), the run has a slot,
/// and the daemon supplied the directory. The daemon never touches the
/// session file here; this only forms its path.
fn session_dir<'a>(
    spec: &ExternalSpec,
    request: &LaunchRequest<'a>,
) -> Result<Option<&'a str>, RunnerFailure> {
    if spec.args_session.is_none() || !has_slot(request.managed.map(|m| m.role)) {
        return Ok(None);
    }
    match env_value(request.environment, SESSION_DIR_ENV) {
        Some(dir) if Path::new(dir).is_absolute() => Ok(Some(dir)),
        Some(_) => Err(launch_failure(format!(
            "{SESSION_DIR_ENV} must be an absolute path"
        ))),
        None => Ok(None),
    }
}

/// The effective (last, as `Command::env` applies them) non-empty value of
/// `name` in a launch environment.
fn env_value<'a>(environment: &'a [(String, String)], name: &str) -> Option<&'a str> {
    environment
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
        .filter(|value| !value.is_empty())
}

/// Values the argv placeholders draw from.
struct ArgValues<'a> {
    workspace: &'a str,
    model: &'a str,
    effort: &'a str,
    agent_mcp_url: Option<&'a str>,
    session_file: Option<String>,
}

/// Substitute one parsed element. `{{`/`}}` escapes were resolved to literal
/// braces by the parser, so literal parts are copied verbatim and substituted
/// values are never re-scanned for placeholders.
fn substitute(
    list: &str,
    arg: &ArgTemplate,
    values: &ArgValues<'_>,
) -> Result<String, RunnerFailure> {
    let mut out = String::new();
    for part in &arg.0 {
        match part {
            ArgPart::Literal(text) => out.push_str(text),
            ArgPart::Placeholder(Placeholder::Workspace) => out.push_str(values.workspace),
            ArgPart::Placeholder(Placeholder::Model) => out.push_str(values.model),
            ArgPart::Placeholder(Placeholder::Effort) => out.push_str(values.effort),
            ArgPart::Placeholder(Placeholder::AgentMcpUrl) => match values.agent_mcp_url {
                Some(url) if list == "args_mcp" => out.push_str(url),
                Some(_) => {
                    return Err(launch_failure(format!(
                        "placeholder {{agent_mcp_url}} is allowed only in external_runner.args_mcp, found in external_runner.{list}"
                    )))
                }
                None => {
                    return Err(launch_failure(format!(
                        "{AGENT_MCP_URL_ENV} is required by external_runner.{list}"
                    )))
                }
            },
            ArgPart::Placeholder(Placeholder::SessionFile) => match &values.session_file {
                Some(path) if list == "args_session" => out.push_str(path),
                _ => {
                    return Err(launch_failure(format!(
                        "placeholder {{session_file}} is allowed only in external_runner.args_session, found in external_runner.{list}"
                    )))
                }
            },
        }
    }
    Ok(out)
}

/// Spec "Launch": `args`, then the access level's `args_*` list, then
/// `args_mcp` when the run is wired to the agent MCP, then `args_session`
/// when the run has a slot and sessions are enabled. The result is a direct
/// argv vector (after the program) for a command started without a shell.
pub fn argv(
    spec: &ExternalSpec,
    request: &LaunchRequest<'_>,
) -> Result<Vec<String>, RunnerFailure> {
    let managed = request.managed.map(|m| m.role);
    let workspace = request
        .worktree
        .to_str()
        .ok_or_else(|| launch_failure("the run's workspace path is not valid UTF-8"))?;
    let wired = wired_to_agent_mcp(managed);
    let session_dir = session_dir(spec, request)?;
    let session_file = session_dir
        .map(|dir| {
            Path::new(dir)
                .join(SESSION_FILE)
                .into_os_string()
                .into_string()
                .map_err(|_| launch_failure("the slot session file path is not valid UTF-8"))
        })
        .transpose()?;
    let values = ArgValues {
        workspace,
        model: request.model,
        effort: request.effort,
        agent_mcp_url: if wired {
            env_value(request.environment, AGENT_MCP_URL_ENV)
        } else {
            None
        },
        session_file,
    };
    let (access_name, access_list) = match access_level(managed) {
        AccessLevel::ReadWrite => ("args_read_write", &spec.args_read_write),
        AccessLevel::ReadOnly => ("args_read_only", &spec.args_read_only),
        AccessLevel::Restricted => ("args_restricted", &spec.args_restricted),
    };
    let access_list = access_list
        .as_ref()
        .ok_or_else(|| launch_failure(format!("external_runner.{access_name} is not declared")))?;
    let mut lists: Vec<(&str, &[ArgTemplate])> =
        vec![("args", &spec.args), (access_name, access_list)];
    if wired {
        let mcp = spec
            .args_mcp
            .as_ref()
            .ok_or_else(|| launch_failure("external_runner.args_mcp is not declared"))?;
        lists.push(("args_mcp", mcp));
    }
    if let (Some(_), Some(session)) = (session_dir, &spec.args_session) {
        lists.push(("args_session", session));
    }
    let mut out = Vec::new();
    for (list, args) in lists {
        for arg in args {
            out.push(substitute(list, arg, &values)?);
        }
    }
    Ok(out)
}

fn refused_env_name(name: &str) -> bool {
    MANAGED_RUN_REMOVED_ENV.contains(&name)
        || REFUSED_ENV_NAMES.contains(&name)
        || REFUSED_ENV_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Spec "Launch": today's managed run environment (as given by the launch
/// request), followed by the declared `env` mappings. A mapping whose run
/// field is absent from this run (for example the run capability of an
/// unmanaged launch) is omitted. [`SESSION_DIR_ENV`] is kept only for a slot
/// run with sessions enabled. [`configure_environment`] applies
/// `ManagedRunEnv` after these.
pub fn environment(
    spec: &ExternalSpec,
    request: &LaunchRequest<'_>,
) -> Result<Vec<(String, String)>, RunnerFailure> {
    let keep_session_dir = session_dir(spec, request)?.is_some();
    let mut out: Vec<(String, String)> = request
        .environment
        .iter()
        .filter(|(key, _)| keep_session_dir || key != SESSION_DIR_ENV)
        .cloned()
        .collect();
    for (name, source) in &spec.env {
        if refused_env_name(name) {
            return Err(launch_failure(format!(
                "external_runner.env.{name} names a refused environment variable"
            )));
        }
        let field = match source {
            EnvSource::RunCapability => "QUORUM_RUN_ID",
            EnvSource::AgentMcpUrl => AGENT_MCP_URL_ENV,
            EnvSource::Repo => "QUORUM_REPO",
            EnvSource::Agent => "QUORUM_AGENT",
        };
        if let Some(value) = env_value(request.environment, field) {
            out.push((name.clone(), value.to_string()));
        }
    }
    Ok(out)
}

/// Apply the layered environment to `command`, with `ManagedRunEnv` last so
/// its removals and private `GH_CONFIG_DIR` win over every earlier layer. A
/// run without a slot directory never inherits [`SESSION_DIR_ENV`] from the
/// daemon's own environment either.
pub(crate) fn configure_environment(
    command: &mut Command,
    spec: &ExternalSpec,
    request: &LaunchRequest<'_>,
    run_env: &ManagedRunEnv,
) -> Result<(), RunnerFailure> {
    let environment = environment(spec, request)?;
    if !environment.iter().any(|(key, _)| key == SESSION_DIR_ENV) {
        command.env_remove(SESSION_DIR_ENV);
    }
    command.envs(environment);
    run_env.apply(command);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_runner_spec::{
        tests::{with_session, VALID},
        ExternalRunnerFile,
    };
    use crate::serve::runner::{AgentKind, LaunchMode, ManagedLaunch};
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::path::Path;

    #[derive(serde::Deserialize)]
    struct Wrapper {
        external_runner: ExternalRunnerFile,
    }

    fn spec_from(src: &str) -> ExternalSpec {
        let file = toml::from_str::<Wrapper>(src).unwrap().external_runner;
        ExternalSpec::from_file(&file, None).unwrap()
    }

    fn edited(from: &str, to: &str) -> ExternalSpec {
        assert!(VALID.contains(from), "fixture lacks {from:?}");
        spec_from(&VALID.replacen(from, to, 1))
    }

    const URL: &str = "http://127.0.0.1:4100/mcp";

    fn managed_env() -> Vec<(String, String)> {
        vec![
            ("QUORUM_REPO".into(), "owner/repo".into()),
            ("QUORUM_AGENT".into(), "Bolt".into()),
            ("QUORUM_AGENT_ENDPOINT".into(), "/state/agent.sock".into()),
            ("QUORUM_RUN_ID".into(), "cap-123".into()),
            (AGENT_MCP_URL_ENV.into(), URL.into()),
        ]
    }

    fn request<'a>(
        role: Option<AgentMcpRole>,
        environment: &'a [(String, String)],
    ) -> LaunchRequest<'a> {
        LaunchRequest {
            runner: AgentKind::External,
            model: "m-1",
            effort: "high",
            worktree: Path::new("/work/tree"),
            prompt: "SECRET PROMPT TEXT",
            environment,
            mode: if role.is_some() {
                LaunchMode::Normal
            } else {
                LaunchMode::Restricted
            },
            continuation_id: None,
            managed: role.map(|role| ManagedLaunch {
                role,
                db_path: Path::new("/state/quorum.db"),
            }),
        }
    }

    const BASE: [&str; 8] = [
        "exec",
        "--workspace",
        "/work/tree",
        "--model",
        "m-1",
        "--effort",
        "high",
        "-",
    ];

    #[test]
    fn access_level_follows_managed_role() {
        assert_eq!(
            access_level(Some(AgentMcpRole::Worker)),
            AccessLevel::ReadWrite
        );
        assert_eq!(
            access_level(Some(AgentMcpRole::Reviewer)),
            AccessLevel::ReadOnly
        );
        assert_eq!(
            access_level(Some(AgentMcpRole::Planner)),
            AccessLevel::ReadOnly
        );
        assert_eq!(access_level(None), AccessLevel::Restricted);
        assert!(wired_to_agent_mcp(Some(AgentMcpRole::Worker)));
        assert!(!wired_to_agent_mcp(None));
    }

    #[test]
    fn argv_is_args_then_access_list_then_mcp() {
        let spec = spec_from(VALID);
        let env = managed_env();
        let cases: [(Option<AgentMcpRole>, &[&str]); 4] = [
            (
                Some(AgentMcpRole::Worker),
                &["--tools", "read-write", "--mcp-url", URL],
            ),
            (
                Some(AgentMcpRole::Reviewer),
                &["--tools", "read-only", "--mcp-url", URL],
            ),
            (
                Some(AgentMcpRole::Planner),
                &["--tools", "read-only", "--mcp-url", URL],
            ),
            // Unmanaged: restricted list and no `args_mcp`, even though the
            // environment carries a URL.
            (None, &["--tools", "none"]),
        ];
        for (role, tail) in cases {
            let got = argv(&spec, &request(role, &env)).unwrap();
            let want: Vec<String> = BASE.iter().chain(tail).map(|s| s.to_string()).collect();
            assert_eq!(got, want, "role {role:?}");
            assert!(!got.iter().any(|a| a.contains("SECRET")), "prompt in argv");
            assert!(
                !got.iter().any(|a| a.contains("cap-123")),
                "capability in argv"
            );
        }
    }

    #[test]
    fn argv_without_mcp_list_needs_no_url_when_unwired() {
        let spec = edited(
            "args_mcp        = [\"--mcp-url\", \"{agent_mcp_url}\"]\n",
            "",
        );
        let env: Vec<(String, String)> = vec![];
        let got = argv(&spec, &request(None, &env)).unwrap();
        assert_eq!(got.last().unwrap(), "none");
        // Wired runs refuse a missing list (validation normally prevents it).
        let err = argv(&spec, &request(Some(AgentMcpRole::Worker), &managed_env())).unwrap_err();
        assert_eq!(
            err.detail(),
            "external runner launch refused: external_runner.args_mcp is not declared"
        );
        assert_eq!(err.disposition(), FailureDisposition::NonFailover);
    }

    #[test]
    fn argv_refuses_missing_access_list_and_missing_url() {
        let spec = edited("args_read_only  = [\"--tools\", \"read-only\"]\n", "");
        let err = argv(
            &spec,
            &request(Some(AgentMcpRole::Reviewer), &managed_env()),
        )
        .unwrap_err();
        assert_eq!(
            err.detail(),
            "external runner launch refused: external_runner.args_read_only is not declared"
        );
        let env: Vec<(String, String)> = managed_env()
            .into_iter()
            .filter(|(k, _)| k != AGENT_MCP_URL_ENV)
            .collect();
        let err = argv(
            &spec_from(VALID),
            &request(Some(AgentMcpRole::Worker), &env),
        )
        .unwrap_err();
        assert_eq!(
            err.detail(),
            "external runner launch refused: QUORUM_AGENT_MCP_URL is required by external_runner.args_mcp"
        );
    }

    #[test]
    fn placeholders_and_brace_escapes() {
        let cases: &[(&str, &str)] = &[
            ("--ws={workspace}", "--ws=/work/tree"),
            ("{model}:{effort}", "m-1:high"),
            ("{{literal}}", "{literal}"),
            ("{{{model}}}", "{m-1}"),
            ("}}{{", "}{"),
            ("a{{b}}c{effort}", "a{b}chigh"),
            ("plain", "plain"),
            ("", ""),
        ];
        for (template, want) in cases {
            let spec = edited(
                r#"args            = ["exec","#,
                &format!("args            = [{template:?}, \"exec\","),
            );
            let got = argv(&spec, &request(None, &[])).unwrap();
            assert_eq!(got[0], *want, "template {template:?}");
            assert_eq!(got[1], "exec");
        }
    }

    #[test]
    fn substituted_values_are_not_rescanned() {
        let spec = spec_from(VALID);
        let mut req = request(None, &[]);
        req.model = "{effort}}}{{";
        let got = argv(&spec, &req).unwrap();
        assert_eq!(got[4], "{effort}}}{{");
        // A value with spaces stays one argv element: no shell, no splitting.
        req.worktree = Path::new("/work/a tree; rm -rf /");
        let got = argv(&spec, &req).unwrap();
        assert_eq!(got[2], "/work/a tree; rm -rf /");
        assert_eq!(got.len(), BASE.len() + 2);
    }

    const SLOT_DIR: &str = "/state/sessions/7/worker";

    fn slot_env() -> Vec<(String, String)> {
        let mut env = managed_env();
        env.push((SESSION_DIR_ENV.into(), SLOT_DIR.into()));
        env
    }

    const ALL_ROLES: [Option<AgentMcpRole>; 4] = [
        Some(AgentMcpRole::Worker),
        Some(AgentMcpRole::Reviewer),
        Some(AgentMcpRole::Planner),
        None,
    ];

    #[test]
    fn slot_runs_append_args_session_last_and_keep_the_slot_dir() {
        let spec = spec_from(&with_session());
        let env = slot_env();
        for (role, tail) in [
            (
                AgentMcpRole::Worker,
                ["--tools", "read-write", "--mcp-url", URL],
            ),
            (
                AgentMcpRole::Reviewer,
                ["--tools", "read-only", "--mcp-url", URL],
            ),
        ] {
            let req = request(Some(role), &env);
            let got = argv(&spec, &req).unwrap();
            let want: Vec<String> = BASE
                .iter()
                .chain(&tail)
                .chain(&["--session", "/state/sessions/7/worker/session"])
                .map(|s| s.to_string())
                .collect();
            assert_eq!(got, want, "role {role:?}");
            let got = environment(&spec, &req).unwrap();
            assert_eq!(&got[..env.len()], &env[..], "{role:?}");
        }
    }

    #[test]
    fn non_slot_runs_get_no_session_args_or_dir() {
        // Planner, classifier, arbiter, and collector have no slot: even a
        // supplied directory yields neither session argv nor the variable.
        let spec = spec_from(&with_session());
        let env = slot_env();
        for role in [Some(AgentMcpRole::Planner), None] {
            let req = request(role, &env);
            let got = argv(&spec, &req).unwrap();
            assert!(!got.iter().any(|a| a == "--session" || a.contains(SLOT_DIR)));
            assert_eq!(got, argv(&spec_from(VALID), &req).unwrap(), "{role:?}");
            let got = environment(&spec, &req).unwrap();
            assert!(!got.iter().any(|(k, _)| k == SESSION_DIR_ENV), "{role:?}");
        }
    }

    #[test]
    fn sessions_disabled_or_no_slot_dir_is_todays_launch() {
        // Without `args_session`, argv and environment are byte-identical to
        // a run that never carried a slot directory.
        let spec = spec_from(VALID);
        let (plain, slot) = (managed_env(), slot_env());
        for role in ALL_ROLES {
            let today = request(role, &plain);
            let with_dir = request(role, &slot);
            assert_eq!(
                argv(&spec, &with_dir).unwrap(),
                argv(&spec, &today).unwrap()
            );
            assert_eq!(
                environment(&spec, &with_dir).unwrap(),
                environment(&spec, &today).unwrap()
            );
            let got = environment(&spec, &today).unwrap();
            assert_eq!(&got[..plain.len()], &plain[..]);
            assert!(!got.iter().any(|(k, _)| k == SESSION_DIR_ENV));
        }
        // Sessions enabled, but the daemon supplied no slot directory.
        let session_spec = spec_from(&with_session());
        for role in ALL_ROLES {
            let req = request(role, &plain);
            assert_eq!(
                argv(&session_spec, &req).unwrap(),
                argv(&spec, &req).unwrap()
            );
            assert_eq!(
                environment(&session_spec, &req).unwrap(),
                environment(&spec, &req).unwrap()
            );
        }
    }

    #[test]
    fn relative_slot_dir_is_refused_without_echoing_it() {
        let spec = spec_from(&with_session());
        let mut env = managed_env();
        env.push((SESSION_DIR_ENV.into(), "rel/slot-xyz".into()));
        let req = request(Some(AgentMcpRole::Worker), &env);
        for err in [
            argv(&spec, &req).unwrap_err(),
            environment(&spec, &req).unwrap_err(),
        ] {
            assert_eq!(
                err.detail(),
                "external runner launch refused: QUORUM_SESSION_DIR must be an absolute path"
            );
            assert_eq!(err.disposition(), FailureDisposition::NonFailover);
        }
    }

    #[test]
    fn session_file_placeholder_outside_args_session_is_refused() {
        // Validation refuses this; a spec that bypassed it is refused at
        // launch whether or not the run has a slot directory.
        let mut spec = spec_from(&with_session());
        spec.args.push(ArgTemplate(vec![ArgPart::Placeholder(
            Placeholder::SessionFile,
        )]));
        let env = slot_env();
        for role in ALL_ROLES {
            let err = argv(&spec, &request(role, &env)).unwrap_err();
            assert_eq!(
                err.detail(),
                "external runner launch refused: placeholder {session_file} is allowed only in external_runner.args_session, found in external_runner.args"
            );
        }
    }

    #[test]
    fn agent_mcp_url_outside_args_mcp_is_refused() {
        let mut spec = spec_from(VALID);
        spec.args.push(ArgTemplate(vec![ArgPart::Placeholder(
            Placeholder::AgentMcpUrl,
        )]));
        let err = argv(&spec, &request(Some(AgentMcpRole::Worker), &managed_env())).unwrap_err();
        assert!(err
            .detail()
            .contains("allowed only in external_runner.args_mcp"));
        assert!(!err.detail().contains(URL));
    }

    #[test]
    fn environment_layers_managed_then_declared() {
        let spec = edited(
            "FAKE_MCP_TOKEN = \"run_capability\"",
            "FAKE_MCP_TOKEN = \"run_capability\"\nFAKE_URL = \"agent_mcp_url\"\nFAKE_REPO = \"repo\"\nFAKE_AGENT = \"agent\"",
        );
        let env = managed_env();
        let got = environment(&spec, &request(Some(AgentMcpRole::Worker), &env)).unwrap();
        assert_eq!(&got[..env.len()], &env[..], "managed environment first");
        let declared: HashMap<&str, &str> = got[env.len()..]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            declared,
            HashMap::from([
                ("FAKE_MCP_TOKEN", "cap-123"),
                ("FAKE_URL", URL),
                ("FAKE_REPO", "owner/repo"),
                ("FAKE_AGENT", "Bolt"),
            ])
        );
        // Unmanaged runs carry no capability, so its mapping is omitted.
        let env: Vec<(String, String)> = managed_env()
            .into_iter()
            .filter(|(k, _)| k != "QUORUM_RUN_ID")
            .collect();
        let got = environment(&spec, &request(None, &env)).unwrap();
        assert!(!got.iter().any(|(k, _)| k == "FAKE_MCP_TOKEN"));
        assert!(got
            .iter()
            .any(|(k, v)| k == "FAKE_REPO" && v == "owner/repo"));
    }

    #[tokio::test]
    async fn managed_run_env_applies_last_and_wins() {
        let spec = spec_from(VALID);
        let mut env = managed_env();
        env.push(("GH_CONFIG_DIR".into(), "/ambient/gh".into()));
        env.push(("GH_TOKEN".into(), "ambient-token".into()));
        let run_env = ManagedRunEnv::create().unwrap();
        let mut command = Command::new("/bin/true");
        configure_environment(
            &mut command,
            &spec,
            &request(Some(AgentMcpRole::Worker), &env),
            &run_env,
        )
        .unwrap();
        let envs: HashMap<&OsStr, Option<&OsStr>> = command.as_std().get_envs().collect();
        assert_eq!(
            envs[OsStr::new("GH_CONFIG_DIR")],
            Some(run_env.gh_config_dir().as_os_str())
        );
        assert_eq!(
            envs[OsStr::new("GH_TOKEN")],
            None,
            "removed by ManagedRunEnv"
        );
        assert_eq!(
            envs[OsStr::new("FAKE_MCP_TOKEN")],
            Some(OsStr::new("cap-123"))
        );
        assert_eq!(
            envs[OsStr::new("QUORUM_RUN_ID")],
            Some(OsStr::new("cap-123"))
        );
    }

    #[tokio::test]
    async fn slot_dir_survives_managed_run_env_and_is_withheld_otherwise() {
        let spec = spec_from(&with_session());
        let env = slot_env();
        for role in ALL_ROLES {
            let run_env = ManagedRunEnv::create().unwrap();
            let mut command = Command::new("/bin/true");
            configure_environment(&mut command, &spec, &request(role, &env), &run_env).unwrap();
            let envs: HashMap<&OsStr, Option<&OsStr>> = command.as_std().get_envs().collect();
            let want = match role {
                Some(AgentMcpRole::Worker | AgentMcpRole::Reviewer) => Some(OsStr::new(SLOT_DIR)),
                // Explicitly removed, so not inherited from the daemon either.
                _ => None,
            };
            assert_eq!(envs[OsStr::new(SESSION_DIR_ENV)], want, "{role:?}");
        }
    }

    #[test]
    fn refused_names_never_reach_the_child() {
        let env = managed_env();
        for name in [
            "PATH",
            "HOME",
            "GH_CONFIG_DIR",
            "GH_TOKEN",
            "QUORUM_HOME",
            "QUORUM_RUN_ID",
            SESSION_DIR_ENV,
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
        ] {
            // Validation refuses the name...
            let src = VALID.replacen("FAKE_MCP_TOKEN", name, 1);
            let file = toml::from_str::<Wrapper>(&src).unwrap().external_runner;
            assert!(
                ExternalSpec::from_file(&file, None).is_err(),
                "{name} validated"
            );
            // ...and a spec that bypassed validation is refused at launch.
            let mut spec = spec_from(VALID);
            spec.env.insert(name.to_string(), EnvSource::Repo);
            let err = environment(&spec, &request(Some(AgentMcpRole::Worker), &env)).unwrap_err();
            assert_eq!(
                err.detail(),
                format!(
                    "external runner launch refused: external_runner.env.{name} names a refused environment variable"
                )
            );
        }
    }
}
