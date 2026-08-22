//! Bounded plan-review "Arbiter" verdict schema and protocol boundary.
//!
//! The Arbiter is a single-shot, stateless reviewer that judges the planner's
//! proposal before children materialize. This module carries only the closed
//! verdict schema and its parser; the daemon coordinator wiring lands in a
//! later implementation slice, so the runtime API is dormant for now.
#![allow(dead_code)]

use super::agent::{self, AgentProc, AgentSpec};
use super::codex_agent::{CodexProc, CodexSpec};
use super::runner::{AgentEvent, AgentKind, CapturedOutput, RunnerProc};
use super::session_log::SessionLog;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// Byte cap for free-text verdict fields. Mirrors the planner's per-field text
/// budget so a verdict shares the same bounded-text discipline.
const MAX_TEXT_BYTES: usize = 8 * 1024;

/// Reuse the planner's 64 KiB whole-response ceiling so a verdict cannot grow
/// past the durable-response envelope, plus the shared prompt/stdout ceilings so
/// the Arbiter transport inherits the planner's bounded-I/O discipline.
use super::planner::{MAX_PROMPT_BYTES, MAX_RESPONSE_BYTES, MAX_STDOUT_BYTES};

/// Access the planner's source/proposal types and shared caps so the Arbiter
/// prompt shares the same bounded-input discipline as `planner::build_prompt`.
use super::planner;

/// The Arbiter is a single-shot reviewer; give it the same generous per-turn
/// ceiling the planner uses so a slow provider is bounded, not fatal.
const ARBITER_TIMEOUT: Duration = Duration::from_secs(600);

/// Bound the durable failure summary the transport surfaces. A failure carries
/// only a compact, bounded reason string (never provider payload text).
const MAX_FAILURE_SUMMARY_BYTES: usize = 2048;

/// Closed reviewer verdict. `Approve` lets the proposal materialize; `Changes`
/// records findings the planner re-proposes against; `RejectSource` parks the
/// source for a required human/coordinator decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbiterVerdict {
    Approve,
    Changes {
        findings: Vec<ArbiterFinding>,
    },
    RejectSource {
        reason: String,
        required_decision: String,
    },
}

/// A single plan-review finding. `blocking` distinguishes a mandatory revision
/// from advisory feedback; `child_key` ties the finding to a proposed child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbiterFinding {
    pub blocking: bool,
    pub summary: String,
    pub child_key: String,
}

/// Parse outcomes: a provider/protocol failure (fail closed to a re-attempt)
/// versus a well-formed-but-invalid verdict shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbiterParseError {
    Provider(String),
    Semantic(String),
}

impl std::fmt::Display for ArbiterParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(s) => write!(f, "provider failure: {s}"),
            Self::Semantic(s) => write!(f, "semantic rejection: {s}"),
        }
    }
}

/// Closed wire form of the verdict. `deny_unknown_fields` and a snake_case,
/// internally-tagged `outcome` reject any shape drift.
#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum WireVerdict {
    // A struct variant (rather than a unit variant) so `deny_unknown_fields`
    // rejects stray keys on an `approve`; internally-tagged unit variants
    // silently ignore extra fields.
    Approve {},
    Changes {
        findings: Vec<WireFinding>,
    },
    RejectSource {
        reason: String,
        required_decision: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFinding {
    blocking: bool,
    summary: String,
    child_key: String,
}

/// Parse exactly one closed verdict object.
///
/// Mirrors `planner::parse_response` framing: an empty/whitespace body, a
/// wrapper, or trailing bytes after the single object is a provider failure
/// (fail closed to a re-attempt, never a silent approve); well-formed JSON of
/// the wrong shape or an unknown `outcome` is a semantic rejection.
pub fn parse_verdict(text: &str) -> Result<ArbiterVerdict, ArbiterParseError> {
    if text.len() > MAX_RESPONSE_BYTES {
        return Err(ArbiterParseError::Provider("verdict exceeds 64 KiB".into()));
    }
    let trimmed = text.trim();
    if trimmed.is_empty() || !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(ArbiterParseError::Provider(
            "verdict must be exactly one JSON object without wrappers".into(),
        ));
    }
    // Stage 1: reject anything that is not exactly one JSON value. `from_str`
    // requires end-of-input after the value, so a second object or trailing
    // bytes fail here as a provider failure.
    let value: serde_json::Value = serde_json::from_str(trimmed)
        .map_err(|e| ArbiterParseError::Provider(format!("verdict is not one JSON object: {e}")))?;
    // Stage 2: a well-formed JSON value that does not match the closed shape or
    // carries an unknown `outcome` is a semantic rejection, not a provider fault.
    let wire: WireVerdict = serde_json::from_value(value)
        .map_err(|e| ArbiterParseError::Semantic(format!("invalid verdict shape: {e}")))?;
    Ok(match wire {
        WireVerdict::Approve {} => ArbiterVerdict::Approve,
        WireVerdict::Changes { findings } => ArbiterVerdict::Changes {
            findings: findings
                .into_iter()
                .map(|f| ArbiterFinding {
                    blocking: f.blocking,
                    summary: truncate_utf8(&f.summary, MAX_TEXT_BYTES).to_owned(),
                    child_key: f.child_key,
                })
                .collect(),
        },
        WireVerdict::RejectSource {
            reason,
            required_decision,
        } => ArbiterVerdict::RejectSource {
            reason: truncate_utf8(&reason, MAX_TEXT_BYTES).to_owned(),
            required_decision: truncate_utf8(&required_decision, MAX_TEXT_BYTES).to_owned(),
        },
    })
}

/// True when any finding is blocking (a blocker forces a `changes` re-proposal).
pub fn has_blocking(findings: &[ArbiterFinding]) -> bool {
    findings.iter().any(|finding| finding.blocking)
}

/// Build one bounded, closed-book Arbiter review turn.
///
/// The Arbiter judges the planner's `proposal_json` against the authoritative
/// `source` and a caller-provided `structural_summary` (the daemon's own
/// dependency/coverage digest), then emits exactly one closed verdict. The echo
/// of the proposal and the structural summary are byte-capped so the whole
/// prompt stays well under the planner's 128 KiB envelope even for oversized
/// input.
pub fn build_arbiter_prompt(
    source: &planner::PlanningSource<'_>,
    proposal_json: &str,
    structural_summary: &str,
) -> String {
    let source_json = serde_json::to_string(source).expect("planning source serializes");
    // Cap the proposal echo at the planner's whole-response envelope (the
    // largest a well-formed proposal can be) and the structural summary at the
    // per-field text budget. Sum: prose (~3 KiB) + 64 KiB + 8 KiB + bounded
    // source stays comfortably under MAX_PROMPT_BYTES.
    let proposal = truncate_utf8(proposal_json, MAX_RESPONSE_BYTES);
    let summary = truncate_utf8(structural_summary, MAX_TEXT_BYTES);
    format!(
        "You are Quorum's plan-review Arbiter: a single-shot, stateless reviewer that judges the \
         planner's proposed decomposition before any child materializes. Judge the PROPOSAL against \
         the authoritative SOURCE and the STRUCTURAL_SUMMARY on four mandates: faithfulness, \
         coverage and non-overlap, coherence, and decomposability. \
         Faithfulness: every source requirement and constraint must be carried by some child; \
         nothing load-bearing may be dropped, weakened, or paraphrased away. \
         Coverage and non-overlap: the children together must cover the source's scope without \
         gaps and without two children owning the same work. \
         Coherence: dependencies must be sane and acyclic, every deliverable must be in-repo, and \
         each child must have an observable outcome and acceptance criteria. \
         Decomposability: if the source is too vague or underspecified to split safely, do not \
         approve or request changes — return reject_source naming the decision the source owner \
         must make. \
         Return exactly one valid JSON object and nothing else. Use no markdown or commentary. \
         The object must be exactly one of these closed shapes: do not omit, rename, or add \
         fields.\n\
         APPROVE={{\"outcome\":\"approve\"}}\n\
         CHANGES={{\"outcome\":\"changes\",\"findings\":[{{\"blocking\":<bool>,\"summary\":\"<text>\",\"child_key\":\"<key-or-empty>\"}}]}}\n\
         REJECT_SOURCE={{\"outcome\":\"reject_source\",\"reason\":\"<text>\",\"required_decision\":\"<text>\"}}\n\
         Use approve only when every mandate holds. Use changes with at least one blocking finding \
         when a mandate is violated but the source itself is decomposable; cite the offending \
         child_key or leave it empty for a whole-plan finding. Use reject_source only when the \
         source cannot be split safely.\n\nSOURCE={source_json}\n\nPROPOSAL={proposal}\n\nSTRUCTURAL_SUMMARY={summary}"
    )
}

/// Truncate to at most `max_bytes` on a UTF-8 char boundary. Duplicated from
/// the planner's private helper to keep this dormant module self-contained.
fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

// ---------------------------------------------------------------------------
// Arbiter slot transport
//
// This mirrors `planner::spawn_planner`/`poll_planner` (the agent-slot pattern)
// so the Arbiter shares the planner's spawn machinery, bounded-I/O discipline,
// and — critically — the same terminal-message selection: Claude replaces its
// buffer on the terminal `Result`; Codex keeps only the latest provider-
// confirmed completed `agent_message` (the #59 fix), never provisional
// narration. The small poll loop is duplicated here on purpose: the plan
// requires mirroring it as-is rather than widening the planner's private
// surface or refactoring shared code and risking a planner regression. This
// transport is dormant — no runtime path calls it yet.
// ---------------------------------------------------------------------------

/// A spawned single-shot Arbiter provider slot. Mirrors `PlannerSlot`'s fields:
/// the child process, the accumulated terminal response, the turn deadline
/// anchor, the bounded stdout counter, and the Codex latest-completed-message
/// terminal-candidate flag.
pub struct ArbiterSlot {
    pub proc: RunnerProc,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub usage: super::runner::TokenUsage,
    pub response_text: String,
    started_at: tokio::time::Instant,
    stdout_bytes: usize,
    codex_terminal_candidate: bool,
    session_log: Option<SessionLog>,
}

impl ArbiterSlot {
    pub fn pid(&self) -> Option<i32> {
        self.proc.pid()
    }

    pub fn start_session_log(
        &mut self,
        log_dir: &Path,
        agent: &str,
        task_id: i64,
        session_id: &str,
        branch: &str,
        started_at: i64,
    ) -> std::io::Result<()> {
        self.session_log = Some(SessionLog::create(
            log_dir,
            agent,
            "arbiter",
            Some(task_id),
            session_id,
            branch,
            started_at,
        )?);
        Ok(())
    }

    pub fn log_dir(&self) -> Option<&Path> {
        self.session_log.as_ref().map(|log| log.dir())
    }

    pub async fn kill_and_reap(mut self) {
        let output = self.proc.kill_and_reap().await;
        super::persist_terminal_output(&mut self.session_log, output);
        if let Some(session_log) = self.session_log.as_mut() {
            session_log.finalize(None);
        }
    }
}

/// Terminal transport outcome. A valid verdict is `Done`; every other terminal
/// condition — provider/protocol/timeout failure, a `Provider` parse error, or
/// (fail closed) a `Semantic` parse error / malformed verdict — is
/// `ProviderFailed`. A malformed verdict is a fault; the transport never
/// fabricates a `Changes` verdict from one.
#[derive(Debug)]
pub enum ArbiterPoll {
    Done(ArbiterVerdict),
    ProviderFailed(String),
}

/// Spawn only the provider selected by the durable role assignment. There is no
/// fallback or model substitution. Mirrors `spawn_planner`'s signature and spawn
/// machinery; `bare` threads the bare-agent flag through the Claude spawn path.
pub async fn spawn_arbiter(
    provider: AgentKind,
    model: &str,
    effort: &str,
    repo: &Path,
    prompt: &str,
    bare: bool,
    provider_bin: Option<&str>,
) -> std::io::Result<ArbiterSlot> {
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "arbiter prompt exceeds 128 KiB",
        ));
    }
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + ARBITER_TIMEOUT;
    let proc =
        match provider {
            AgentKind::Codex => {
                let spec = CodexSpec {
                    model: model.into(),
                    effort: effort.into(),
                    sandbox: "read-only".into(),
                    worktree: repo.to_path_buf(),
                    prompt: prompt.into(),
                    env_vars: vec![],
                };
                RunnerProc::Codex(CodexProc::spawn_planner(&spec, provider_bin)?)
            }
            AgentKind::Grok => return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Grok plan-review arbiter refused: managed Grok lifecycle roles are not enabled",
            )),
            AgentKind::Claude => {
                let spec = AgentSpec {
                    kind: AgentKind::Claude,
                    model: model.into(),
                    effort: effort.into(),
                    session_id: agent::new_session_id(),
                    worktree: repo.to_path_buf(),
                    bare,
                    allowed_tools: "Read,Glob,Grep".into(),
                    env_vars: vec![],
                };
                let mut proc = AgentProc::spawn_planner(&spec, provider_bin)?;
                if let Err(error) = proc
                    .feed_turn_until(&agent::user_turn(prompt), deadline)
                    .await
                {
                    let _ = proc.kill_and_reap().await;
                    return Err(error);
                }
                RunnerProc::Claude(proc)
            }
        };
    Ok(ArbiterSlot {
        proc,
        provider: provider.to_string(),
        model: model.into(),
        effort: effort.into(),
        usage: super::runner::TokenUsage::default(),
        response_text: String::new(),
        started_at,
        stdout_bytes: 0,
        codex_terminal_candidate: false,
        session_log: None,
    })
}

/// Build a bounded `ProviderFailed` outcome from a compact reason. The reason is
/// truncated on a UTF-8 boundary; provider payload text never enters it.
fn provider_failure(reason: &str) -> ArbiterPoll {
    ArbiterPoll::ProviderFailed(truncate_utf8(reason, MAX_FAILURE_SUMMARY_BYTES).to_owned())
}

fn stderr_capture_was_truncated(output: &[CapturedOutput]) -> bool {
    output.iter().any(|line| {
        matches!(
            line,
            CapturedOutput::StderrTruncated { .. } | CapturedOutput::StderrBytesTruncated { .. }
        )
    })
}

/// Map the accumulated terminal response to an outcome.
///
/// **Fail closed:** a `Provider` parse error (empty/wrapped/trailing bytes) and
/// a `Semantic` parse error (well-formed-but-invalid verdict shape) both map to
/// `ProviderFailed`. A malformed verdict is a fault, never a fabricated
/// `Changes` verdict.
fn parsed_poll(slot: &ArbiterSlot) -> ArbiterPoll {
    match parse_verdict(&slot.response_text) {
        Ok(verdict) => ArbiterPoll::Done(verdict),
        Err(ArbiterParseError::Provider(error)) => provider_failure(&error),
        Err(ArbiterParseError::Semantic(error)) => provider_failure(&error),
    }
}

/// Drain a bounded amount of output. Timeout and output violations are provider
/// failures; the caller must kill and reap the returned terminal slot. Mirrors
/// `poll_planner` terminal-message handling as-is.
pub async fn poll_arbiter(slot: &mut ArbiterSlot) -> Option<ArbiterPoll> {
    if slot.started_at.elapsed() >= ARBITER_TIMEOUT {
        return Some(provider_failure("arbiter timed out"));
    }
    let remaining = ARBITER_TIMEOUT.saturating_sub(slot.started_at.elapsed());
    let poll_for = remaining.min(Duration::from_secs(2));
    let mut stdout_complete = false;
    loop {
        let line_limit = MAX_STDOUT_BYTES
            .saturating_sub(slot.stdout_bytes)
            .saturating_sub(1);
        let raw = match tokio::time::timeout(poll_for, slot.proc.next_raw_line_bounded(line_limit))
            .await
        {
            Err(_) => break,
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => {
                stdout_complete = true;
                break;
            }
            Ok(Err(error)) => {
                let exceeded = error.to_string().contains("exceeded");
                if exceeded {
                    slot.stdout_bytes = MAX_STDOUT_BYTES.saturating_add(1);
                }
                let reason = if exceeded {
                    format!("arbiter stdout exceeded {} KiB", MAX_STDOUT_BYTES / 1024)
                } else {
                    format!("arbiter stdout read failed: {error}")
                };
                return Some(provider_failure(&reason));
            }
        };
        let events = match slot.proc.kind() {
            AgentKind::Claude => super::runner::normalize_claude_line(&raw),
            AgentKind::Codex => super::runner::normalize_codex_line(&raw),
            AgentKind::Grok => super::runner::normalize_grok_line(&raw),
        };
        if let Some(session_log) = slot.session_log.as_mut() {
            session_log.log_raw_and_normalized(&raw, &events);
        }
        slot.stdout_bytes = slot.stdout_bytes.saturating_add(raw.len() + 1);
        for event in super::runner::normalize_line(slot.proc.kind(), &raw) {
            match event {
                AgentEvent::TurnCompleted {
                    usage: Some(usage), ..
                }
                | AgentEvent::TurnFailed {
                    usage: Some(usage), ..
                } => slot.usage.saturating_add_assign(usage),
                _ => {}
            }
        }
        if slot.stdout_bytes > MAX_STDOUT_BYTES {
            return Some(provider_failure(&format!(
                "arbiter stdout exceeded {} KiB",
                MAX_STDOUT_BYTES / 1024
            )));
        }
        if slot.codex_terminal_candidate {
            return Some(provider_failure(
                "arbiter provider emitted output after terminal response",
            ));
        }
        if slot.proc.kind() == AgentKind::Codex {
            if let Some(failure) = slot.proc.observed_planner_live_failure() {
                return Some(provider_failure(&format!(
                    "arbiter provider protocol failed ({})",
                    failure.disposition()
                )));
            }
        }
        if slot.proc.kind() == AgentKind::Claude {
            if let Some(super::stream::Event::Result {
                result, is_error, ..
            }) = super::stream::parse_line(&raw)
            {
                if is_error.unwrap_or(false) {
                    return Some(provider_failure("arbiter provider returned an error"));
                }
                // Claude streams its terminal response as the `Result` payload:
                // replace the buffer so only the terminal message is judged.
                let text = super::stream::result_text(&result);
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(parsed_poll(slot));
            }
        }
        for event in events {
            match event {
                AgentEvent::TurnFailed { .. } => {
                    return Some(provider_failure("arbiter provider turn failed"));
                }
                AgentEvent::TurnCompleted { .. } => {
                    if slot.proc.kind() == AgentKind::Codex {
                        // Codex's terminal is the completed turn; the response is
                        // the latest completed agent_message captured below, not
                        // any provisional started message.
                        slot.codex_terminal_candidate = true;
                    } else {
                        return Some(parsed_poll(slot));
                    }
                }
                AgentEvent::AssistantText { text } => {
                    // Each Claude assistant event is a distinct message, so retain
                    // only the latest one: the final message is the response
                    // candidate. Codex started agent-message events are provisional
                    // and ignored in favor of the provider-confirmed completed event.
                    if slot.proc.kind() == AgentKind::Claude {
                        if text.len() > MAX_RESPONSE_BYTES {
                            return Some(provider_failure("arbiter response exceeded 64 KiB"));
                        }
                        slot.response_text = text;
                    }
                }
                AgentEvent::CompletedAssistantText { text, .. } => {
                    if text.len() > MAX_RESPONSE_BYTES {
                        return Some(provider_failure("arbiter response exceeded 64 KiB"));
                    }
                    // Retain only the latest completed message (the #59 fix): a
                    // later completed message overwrites an earlier candidate.
                    slot.response_text = text;
                }
                _ => {}
            }
        }
        if slot.started_at.elapsed() >= ARBITER_TIMEOUT {
            return Some(provider_failure("arbiter timed out"));
        }
    }
    let status = match slot.proc.try_wait() {
        Ok(status) => status,
        Err(error) => {
            return Some(provider_failure(&format!(
                "arbiter process status unavailable: {error}"
            )));
        }
    };
    if slot.proc.kind() == AgentKind::Codex && slot.codex_terminal_candidate && stdout_complete {
        let status = status?;
        let evidence = slot.proc.finalize_pre_authoritative_evidence().await;
        if !status.success() {
            if let Some(failure) = slot.proc.observed_strict_pre_authoritative_failure() {
                return Some(provider_failure(&format!(
                    "arbiter provider exited unsuccessfully after terminal response ({}) at {status}: {}",
                    failure.disposition(),
                    failure.detail()
                )));
            }
            return Some(provider_failure(&format!(
                "arbiter provider exited unsuccessfully after terminal response: {status}"
            )));
        }
        if stderr_capture_was_truncated(&evidence) {
            return Some(provider_failure(
                "arbiter stderr exceeded bounded diagnostic capture",
            ));
        }
        if let Some(failure) = slot.proc.observed_planner_terminal_failure() {
            return Some(provider_failure(&format!(
                "arbiter provider terminal evidence failed ({})",
                failure.disposition()
            )));
        }
        return Some(parsed_poll(slot));
    }
    if let Some(status) = status {
        let evidence = slot.proc.finalize_pre_authoritative_evidence().await;
        if stderr_capture_was_truncated(&evidence) {
            return Some(provider_failure(
                "arbiter stderr exceeded bounded diagnostic capture",
            ));
        }
        if let Some(failure) = slot.proc.classify_pre_authoritative_exit(status) {
            return Some(provider_failure(&format!(
                "arbiter provider exited without a terminal response ({}): {}",
                failure.disposition(),
                failure.detail()
            )));
        }
        return Some(provider_failure(
            "arbiter exited without a terminal response",
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Bounded ceiling for the fake-provider poll loop in tests.
    #[cfg(unix)]
    const TEST_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(15);

    // A Codex terminal stream whose latest completed agent_message is a valid
    // `approve` verdict. An earlier provisional narration message and a later
    // provisional (started) message bracket it to prove the latest-completed
    // selection (#59): the transport must judge only the completed verdict.
    #[cfg(unix)]
    const APPROVE_STREAM: &str = concat!(
        r#"{"type":"item.completed","item":{"type":"agent_message","id":"m1","text":"exploring the proposal"}}"#,
        "\n",
        r#"{"type":"item.completed","item":{"type":"agent_message","id":"m2","text":"{\"outcome\":\"approve\"}"}}"#,
        "\n",
        r#"{"type":"item.started","item":{"type":"agent_message","id":"m3","text":"trailing narration"}}"#,
        "\n",
        r#"{"type":"turn.completed"}"#,
        "\n",
    );

    // A Codex failure stream: the turn fails without a terminal verdict.
    #[cfg(unix)]
    const ERROR_STREAM: &str = concat!(
        r#"{"type":"turn.failed","error":{"message":"provider failed"}}"#,
        "\n",
    );

    #[cfg(unix)]
    fn executable_script(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let runner = dir.join(name);
        std::fs::write(&runner, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        runner
    }

    #[cfg(unix)]
    async fn poll_to_terminal(slot: &mut ArbiterSlot) -> ArbiterPoll {
        tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                if let Some(outcome) = poll_arbiter(slot).await {
                    break outcome;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("arbiter did not reach a terminal outcome")
    }

    #[cfg(unix)]
    async fn spawn_fake_arbiter(dir: &Path, stdout: &str) -> ArbiterSlot {
        let stdout_path = dir.join("stdout.jsonl");
        std::fs::write(&stdout_path, stdout).unwrap();
        let runner = executable_script(
            dir,
            "codex",
            &format!("exec /bin/cat '{}'", stdout_path.display()),
        );
        spawn_arbiter(
            AgentKind::Codex,
            planner::CODEX_PLANNER_MODEL,
            planner::PLANNER_EFFORT,
            dir,
            "bounded prompt",
            false,
            runner.to_str(),
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    async fn spawn_fake_claude_arbiter(dir: &Path, stdout: &str) -> ArbiterSlot {
        let stdout_path = dir.join("stdout.jsonl");
        std::fs::write(&stdout_path, stdout).unwrap();
        let runner = executable_script(
            dir,
            "claude",
            // Claude receives its first turn through stdin. Keeping the fixture alive until
            // that line is written prevents an eager `cat` exit from turning this provider
            // protocol test into a scheduler-dependent BrokenPipe.
            &format!(
                "read -r _ || true\nexec /bin/cat '{}'",
                stdout_path.display()
            ),
        );
        spawn_arbiter(
            AgentKind::Claude,
            planner::CLAUDE_PLANNER_MODEL,
            planner::PLANNER_EFFORT,
            dir,
            "bounded prompt",
            false,
            runner.to_str(),
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    fn claude_stream(events: &[serde_json::Value]) -> String {
        let mut stream = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        stream.push_str("\n{\"type\":\"result\",\"result\":\"\",\"is_error\":false}\n");
        stream
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fake_agent_approve_reaches_done() {
        let dir = tempfile::tempdir().unwrap();
        let mut slot = spawn_fake_arbiter(dir.path(), APPROVE_STREAM).await;
        match poll_to_terminal(&mut slot).await {
            ArbiterPoll::Done(ArbiterVerdict::Approve) => {}
            other => panic!("expected approve, got {other:?}"),
        }
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn no_poll_reap_persists_buffered_provider_stream() {
        let dir = tempfile::tempdir().unwrap();
        let raw = r#"{"type":"turn.failed","error":{"message":"provider failed"}}"#;
        let mut slot = spawn_fake_arbiter(dir.path(), &format!("{raw}\n")).await;
        slot.start_session_log(
            dir.path(),
            "decomposition-arbiter-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            while slot.proc.try_wait().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("arbiter provider did not exit");

        slot.kill_and_reap().await;
        assert_eq!(
            std::fs::read_to_string(log_dir.join("stream.jsonl")).unwrap(),
            format!("{raw}\n")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_arbiter_uses_only_the_final_assistant_message() {
        let dir = tempfile::tempdir().unwrap();
        let output = claude_stream(&[
            serde_json::json!({"type": "assistant", "message": {"content": "Let me inspect the proposal."}}),
            serde_json::json!({"type": "tool_use", "name": "Read", "input": {"file_path": "src/core.rs"}}),
            serde_json::json!({"type": "assistant", "message": {"content": "The proposal is clear."}}),
            serde_json::json!({"type": "assistant", "message": {"content": "{\"outcome\":\"approve\"}"}}),
        ]);

        let mut slot = spawn_fake_claude_arbiter(dir.path(), &output).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            ArbiterPoll::Done(ArbiterVerdict::Approve)
        ));
        assert_eq!(slot.response_text, r#"{"outcome":"approve"}"#);
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_arbiter_narration_only_is_a_provider_failure() {
        let dir = tempfile::tempdir().unwrap();
        let output = claude_stream(&[serde_json::json!({
            "type": "assistant",
            "message": {"content": "Let me inspect the proposal."}
        })]);

        let mut slot = spawn_fake_claude_arbiter(dir.path(), &output).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            ArbiterPoll::ProviderFailed(_)
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_arbiter_rejects_an_oversized_final_message() {
        let dir = tempfile::tempdir().unwrap();
        let output = claude_stream(&[serde_json::json!({
            "type": "assistant",
            "message": {"content": "x".repeat(MAX_RESPONSE_BYTES + 1)}
        })]);

        let mut slot = spawn_fake_claude_arbiter(dir.path(), &output).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            ArbiterPoll::ProviderFailed(ref message) if message.contains("response exceeded")
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn provider_error_maps_to_provider_failed() {
        let dir = tempfile::tempdir().unwrap();
        let mut slot = spawn_fake_arbiter(dir.path(), ERROR_STREAM).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            ArbiterPoll::ProviderFailed(_)
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_verdict_fails_closed_to_provider_failed() {
        // A well-formed-but-invalid verdict shape (unknown outcome) is a
        // `Semantic` parse error. Fail closed: it must map to ProviderFailed,
        // never a fabricated Changes verdict.
        let dir = tempfile::tempdir().unwrap();
        let stream = concat!(
            r#"{"type":"item.completed","item":{"type":"agent_message","id":"m1","text":"{\"outcome\":\"maybe\"}"}}"#,
            "\n",
            r#"{"type":"turn.completed"}"#,
            "\n",
        );
        let mut slot = spawn_fake_arbiter(dir.path(), stream).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            ArbiterPoll::ProviderFailed(_)
        ));
        slot.kill_and_reap().await;
    }

    #[test]
    fn parses_approve() {
        let v = parse_verdict(r#"{"outcome":"approve"}"#).unwrap();
        assert!(matches!(v, ArbiterVerdict::Approve));
    }

    #[test]
    fn parses_changes_with_blocking() {
        let v = parse_verdict(
            r#"{"outcome":"changes","findings":[{"blocking":true,"summary":"dropped constraint X","child_key":"c1"}]}"#,
        )
        .unwrap();
        match v {
            ArbiterVerdict::Changes { findings } => {
                assert_eq!(findings.len(), 1);
                assert!(has_blocking(&findings));
            }
            _ => panic!("expected changes"),
        }
    }

    #[test]
    fn parses_reject_source() {
        let v = parse_verdict(
            r#"{"outcome":"reject_source","reason":"too vague","required_decision":"clarify scope"}"#,
        )
        .unwrap();
        assert!(matches!(v, ArbiterVerdict::RejectSource { .. }));
    }

    #[test]
    fn rejects_wrapped_or_extra_json() {
        assert!(matches!(
            parse_verdict(r#"here is the verdict: {"outcome":"approve"}"#),
            Err(ArbiterParseError::Provider(_))
        ));
        assert!(matches!(
            parse_verdict(r#"{"outcome":"approve"}{"outcome":"approve"}"#),
            Err(ArbiterParseError::Provider(_))
        ));
    }

    #[test]
    fn rejects_unknown_outcome_and_empty() {
        assert!(matches!(
            parse_verdict(r#"{"outcome":"maybe"}"#),
            Err(ArbiterParseError::Semantic(_))
        ));
        assert!(matches!(
            parse_verdict(""),
            Err(ArbiterParseError::Provider(_))
        ));
    }

    #[test]
    fn whitespace_only_is_provider_failure() {
        assert!(matches!(
            parse_verdict("   \n\t  "),
            Err(ArbiterParseError::Provider(_))
        ));
    }

    #[test]
    fn unknown_field_is_semantic_rejection() {
        assert!(matches!(
            parse_verdict(r#"{"outcome":"approve","extra":true}"#),
            Err(ArbiterParseError::Semantic(_))
        ));
    }

    #[test]
    fn no_blocking_findings_reports_false() {
        let v = parse_verdict(
            r#"{"outcome":"changes","findings":[{"blocking":false,"summary":"nit","child_key":"c1"}]}"#,
        )
        .unwrap();
        let ArbiterVerdict::Changes { findings } = v else {
            panic!("expected changes");
        };
        assert!(!has_blocking(&findings));
    }

    #[test]
    fn oversized_verdict_is_provider_failure() {
        let big = format!(
            r#"{{"outcome":"reject_source","reason":"{}","required_decision":"x"}}"#,
            "y".repeat(MAX_RESPONSE_BYTES)
        );
        assert!(matches!(
            parse_verdict(&big),
            Err(ArbiterParseError::Provider(_))
        ));
    }

    #[test]
    fn prompt_contains_mandate_source_and_shapes() {
        let src = planner::PlanningSource {
            task_id: 7,
            revision: 1,
            title: "Do X",
            body: Some("preserve `foo`"),
            dependencies: &[],
        };
        let p = build_arbiter_prompt(&src, r#"{"outcome":"plan","tasks":[]}"#, "structural: ok");
        for needle in [
            "faithful",
            "coverage",
            "reject_source",
            "\"outcome\":\"approve\"",
            "SOURCE=",
            "PROPOSAL=",
        ] {
            assert!(p.contains(needle), "missing {needle}");
        }
    }

    #[test]
    fn prompt_bounds_oversized_proposal() {
        let src = planner::PlanningSource {
            task_id: 1,
            revision: 1,
            title: "t",
            body: None,
            dependencies: &[],
        };
        let big = "x".repeat(200_000);
        let p = build_arbiter_prompt(&src, &big, "");
        assert!(p.len() < 128 * 1024, "prompt must stay bounded");
    }

    #[test]
    fn prompt_bounds_oversized_summary() {
        let src = planner::PlanningSource {
            task_id: 1,
            revision: 1,
            title: "t",
            body: None,
            dependencies: &[],
        };
        let big_summary = "s".repeat(200_000);
        let p = build_arbiter_prompt(&src, r#"{"outcome":"plan","tasks":[]}"#, &big_summary);
        assert!(p.len() < 128 * 1024, "prompt must stay bounded");
    }

    #[test]
    fn oversized_summary_is_truncated() {
        let summary = "z".repeat(MAX_TEXT_BYTES * 2);
        let text = format!(
            r#"{{"outcome":"changes","findings":[{{"blocking":true,"summary":"{summary}","child_key":"c1"}}]}}"#
        );
        let ArbiterVerdict::Changes { findings } = parse_verdict(&text).unwrap() else {
            panic!("expected changes");
        };
        assert!(findings[0].summary.len() <= MAX_TEXT_BYTES);
    }
}
