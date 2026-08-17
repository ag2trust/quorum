//! Daemon classifier phase — spawns a headless agent to batch-classify tasks.

use super::runner::{AdapterConfig, AgentEvent, AgentKind, LaunchMode, LaunchRequest, RunnerProc};
use quorum_core::classify::{self, ClassifierResponse, TaskClassification, TaskForClassification};
use std::time::Duration;

pub const CLASSIFIER_MODEL: &str = "claude-haiku-4-5-20251001";
pub const CLASSIFIER_EFFORT: &str = "low";
pub const CLASSIFIER_TIMEOUT: Duration = Duration::from_secs(120);
pub const MAX_CLASSIFIER_STDOUT_BYTES: usize = 256 * 1024;
pub const MAX_CLASSIFIER_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CLASSIFIER_LINES_PER_POLL: usize = 64;

/// In-flight classifier state, persisted across daemon ticks.
#[allow(dead_code)]
pub struct ClassifierSlot {
    pub proc: RunnerProc,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub pending_task_ids: Vec<i64>,
    /// Internally-derived identity of each exact input sent to this provider
    /// turn.  Never accept a model-supplied revision/fingerprint.
    pub pending_inputs: Vec<classify::ClassificationInput>,
    pub response_text: String,
    /// Keep the empty classifier-only workspace alive for the whole turn.
    pub isolation_dir: Option<tempfile::TempDir>,
    started_at: tokio::time::Instant,
    stdout_bytes: usize,
    pub usage: super::runner::TokenUsage,
}

impl ClassifierSlot {
    /// Terminate and reap the provider before dropping the isolated workspace.
    /// Claude's stream-json process is persistent after a Result event, so
    /// dropping the slot alone would leave the child alive in a deleted cwd.
    pub async fn kill_and_reap(mut self) -> super::runner::TokenUsage {
        let kind = self.proc.kind();
        let terminal_output = self.proc.kill_and_reap().await;
        for captured in terminal_output {
            let super::runner::CapturedOutput::Stdout(raw_line) = captured else {
                continue;
            };
            for event in super::runner::normalize_line(kind, &raw_line) {
                match event {
                    AgentEvent::TurnCompleted {
                        usage: Some(usage), ..
                    }
                    | AgentEvent::TurnFailed {
                        usage: Some(usage), ..
                    } => self.usage.saturating_add_assign(usage),
                    _ => {}
                }
            }
        }
        self.usage
    }
}

/// Spawn using the provider resolved from `model`. Provider resolution is
/// authoritative: an unknown model is rejected and a failed spawn is returned
/// directly; neither condition falls back to the other runner.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_classifier_configured(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
    agent_bin: Option<&str>,
    bare: bool,
    model: &str,
    effort: &str,
    codex_sandbox: &str,
    recommendations: &str,
) -> std::io::Result<ClassifierSlot> {
    spawn_classifier_configured_with_timeout(
        tasks,
        dup_context,
        agent_bin,
        bare,
        model,
        effort,
        codex_sandbox,
        recommendations,
        CLASSIFIER_TIMEOUT,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_classifier_configured_with_timeout(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
    agent_bin: Option<&str>,
    bare: bool,
    model: &str,
    effort: &str,
    codex_sandbox: &str,
    recommendations: &str,
    turn_timeout: Duration,
) -> std::io::Result<ClassifierSlot> {
    if tasks.len() > classify::CLASSIFICATION_BATCH_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "classifier batch has {} tasks; limit is {}",
                tasks.len(),
                classify::CLASSIFICATION_BATCH_LIMIT
            ),
        ));
    }
    if dup_context.len() > classify::DUP_CONTEXT_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "classifier duplicate context has {} tasks; limit is {}",
                dup_context.len(),
                classify::DUP_CONTEXT_LIMIT
            ),
        ));
    }
    let pending_task_ids = tasks.iter().map(|t| t.id).collect();
    let pending_inputs = classify::classification_inputs(tasks);
    let dir = tempfile::tempdir()?;
    let prompt = classify::build_prompt_with_recommendations(tasks, dup_context, recommendations);
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + turn_timeout;
    // Safe mode retains Claude's configured auth path while suppressing user
    // context; Codex retains its read-only sandbox without the normal bypass.
    // The empty temporary cwd is the only directory exposed to either runner.
    let proc = RunnerProc::launch_restricted_until(
        &LaunchRequest {
            model,
            effort,
            worktree: dir.path(),
            prompt: &prompt,
            environment: &[],
            mode: LaunchMode::Restricted,
            continuation_id: None,
        },
        &AdapterConfig {
            executable: agent_bin,
            claude_bare: bare,
            claude_allowed_tools: "",
            codex_sandbox,
            grok: Default::default(),
        },
        deadline,
    )
    .await?;
    Ok(ClassifierSlot {
        proc,
        provider: AgentKind::for_model(model)
            .map(|kind| kind.to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        model: model.to_string(),
        effort: effort.to_string(),
        pending_task_ids,
        pending_inputs,
        response_text: String::new(),
        isolation_dir: Some(dir),
        started_at,
        stdout_bytes: 0,
        usage: super::runner::TokenUsage::default(),
    })
}

/// Build the user turn for the classifier prompt.
#[allow(dead_code)] // default-provider convenience retained for compatibility
pub fn classifier_turn(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
) -> String {
    let prompt = classify::build_prompt(tasks, dup_context);
    super::agent::user_turn(&prompt)
}

/// Drain a bounded slice of classifier output. The wall-clock and byte budgets
/// belong to the slot, so repeated daemon ticks cannot reset them. Violations
/// are provider failures for both ordinary and decomposition classification.
pub async fn drain_classifier_events(slot: &mut ClassifierSlot) -> Option<ClassifierResult> {
    if slot.started_at.elapsed() >= CLASSIFIER_TIMEOUT {
        return Some(ClassifierResult::Error("classifier timed out".into()));
    }
    let remaining = CLASSIFIER_TIMEOUT.saturating_sub(slot.started_at.elapsed());
    let poll_for = remaining.min(Duration::from_secs(2));
    let poll_deadline = tokio::time::Instant::now() + poll_for;
    for _ in 0..MAX_CLASSIFIER_LINES_PER_POLL {
        if slot.started_at.elapsed() >= CLASSIFIER_TIMEOUT {
            return Some(ClassifierResult::Error("classifier timed out".into()));
        }
        if tokio::time::Instant::now() >= poll_deadline {
            break;
        }
        let line_limit = MAX_CLASSIFIER_STDOUT_BYTES
            .saturating_sub(slot.stdout_bytes)
            .saturating_sub(1);
        let raw = match tokio::time::timeout_at(
            poll_deadline,
            slot.proc.next_raw_line_bounded(line_limit),
        )
        .await
        {
            Err(_) if slot.started_at.elapsed() >= CLASSIFIER_TIMEOUT => {
                return Some(ClassifierResult::Error("classifier timed out".into()));
            }
            Err(_) => break,
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                let detail = if error.to_string().contains("exceeded") {
                    "classifier stdout exceeded 256 KiB".into()
                } else {
                    format!(
                        "classifier stdout read failed: {}",
                        truncate_error(&error.to_string(), 300)
                    )
                };
                return Some(ClassifierResult::Error(detail));
            }
        };
        slot.stdout_bytes = slot.stdout_bytes.saturating_add(raw.len() + 1);
        if slot.stdout_bytes > MAX_CLASSIFIER_STDOUT_BYTES {
            return Some(ClassifierResult::Error(
                "classifier stdout exceeded 256 KiB".into(),
            ));
        }
        let line = slot.proc.normalize_line(&raw);
        // A terminal provider record can carry both the final response and
        // its usage. Preserve the usage before applying response disposition:
        // the raw line has already been consumed, so reap cannot recover it
        // if an oversized response returns early here.
        for event in &line.events {
            match event {
                AgentEvent::TurnCompleted {
                    usage: Some(usage), ..
                }
                | AgentEvent::TurnFailed {
                    usage: Some(usage), ..
                } => slot.usage.saturating_add_assign(*usage),
                _ => {}
            }
        }
        if let Some(text) = line.terminal_text.as_ref().filter(|text| !text.is_empty()) {
            if text.len() > MAX_CLASSIFIER_RESPONSE_BYTES {
                return Some(ClassifierResult::Error(
                    "classifier response exceeded 64 KiB".into(),
                ));
            }
            slot.response_text = text.clone();
        }
        for event in line.events {
            match event {
                AgentEvent::TurnFailed { message, .. } => {
                    let message = line.terminal_text.as_deref().unwrap_or(&message);
                    let detail = if message.is_empty() {
                        "classifier agent returned an error".into()
                    } else {
                        format!("classifier agent error: {}", truncate_error(message, 300))
                    };
                    return Some(ClassifierResult::Error(detail));
                }
                AgentEvent::TurnCompleted { .. } => {
                    return Some(ClassifierResult::Done(slot.response_text.clone()));
                }
                AgentEvent::AssistantText { text } => {
                    if slot.response_text.len().saturating_add(text.len())
                        > MAX_CLASSIFIER_RESPONSE_BYTES
                    {
                        return Some(ClassifierResult::Error(
                            "classifier response exceeded 64 KiB".into(),
                        ));
                    }
                    slot.response_text.push_str(&text);
                }
                _ => {}
            }
        }
    }
    (slot.started_at.elapsed() >= CLASSIFIER_TIMEOUT)
        .then(|| ClassifierResult::Error("classifier timed out".into()))
}

pub enum ClassifierResult {
    Done(String),
    Error(String),
}

fn truncate_error(s: &str, max_chars: usize) -> String {
    let mut chars = s.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// True when the error text looks like a Claude CLI authentication failure.
pub fn is_auth_error(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("not logged in") || lower.contains("please run /login")
}

/// Parse the classifier response text into structured results.
#[allow(dead_code)] // retained for parse-only compatibility tests; live paths validate batches
pub fn parse_response(text: &str) -> Option<Vec<TaskClassification>> {
    // Try to find JSON in the response (model might wrap in markdown fences)
    let json_text = extract_json(text)?;
    let resp: ClassifierResponse = serde_json::from_str(json_text).ok()?;
    Some(resp.tasks)
}

/// Parse and validate one complete classifier batch. A syntactically valid
/// response is still a provider failure unless it covers every requested task
/// exactly once and every item satisfies the v2 contract.
pub fn parse_validated_response(
    text: &str,
    expected_task_ids: &[i64],
) -> std::result::Result<Vec<TaskClassification>, String> {
    let json_text =
        extract_json(text).ok_or_else(|| "classifier returned unparseable JSON".to_string())?;
    let raw: serde_json::Value = serde_json::from_str(json_text)
        .map_err(|error| format!("classifier returned invalid JSON: {error}"))?;
    let raw_tasks = raw
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "classifier response is missing its tasks array".to_string())?;
    if raw_tasks.iter().any(|task| {
        !task
            .as_object()
            .is_some_and(|object| object.contains_key("not_ready_reason"))
    }) {
        return Err("classifier response item is missing not_ready_reason".into());
    }
    let resp: ClassifierResponse = serde_json::from_value(raw)
        .map_err(|error| format!("classifier response has an invalid item: {error}"))?;
    let results = resp.tasks;
    classify::validate_batch(&results, expected_task_ids)?;
    Ok(results)
}

fn extract_json(text: &str) -> Option<&str> {
    let trimmed = text.trim();

    // Direct JSON object
    if trimmed.starts_with('{') {
        return Some(trimmed);
    }

    // Strip markdown code fences
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim());
        }
    }
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        if let Some(end) = after.find("```") {
            let inner = after[..end].trim();
            if inner.starts_with('{') {
                return Some(inner);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // These subprocess boundary tests compete with the full suite's other
    // process-heavy cases. Keep their assertion bounded without mistaking
    // ordinary CI scheduling delay for a boundary regression.
    const TEST_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(15);
    const TEST_STDIN_FEED_TIMEOUT: Duration = Duration::from_secs(15);

    #[cfg(unix)]
    async fn spawn_scripted_classifier(
        script: &str,
        model: &str,
    ) -> (tempfile::TempDir, ClassifierSlot, i32) {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let runner = temp.path().join("classifier-runner");
        std::fs::write(&runner, script).unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        let tasks = vec![TaskForClassification {
            id: 7,
            revision: 1,
            title: "classify me".into(),
            body: None,
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let slot = spawn_classifier_configured(
            &tasks,
            &[],
            runner.to_str(),
            false,
            model,
            "low",
            "read-only",
            "",
        )
        .await
        .unwrap();
        let pid = slot.proc.pid().expect("classifier pid");
        (temp, slot, pid)
    }

    #[cfg(unix)]
    async fn assert_classifier_failure_reaped(
        slot: ClassifierSlot,
        pid: i32,
        result: Option<ClassifierResult>,
        expected: &str,
    ) {
        assert!(
            matches!(result, Some(ClassifierResult::Error(ref error)) if error.contains(expected)),
            "unexpected classifier result"
        );
        slot.kill_and_reap().await;
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "classifier was not reaped"
        );
    }

    #[cfg(unix)]
    async fn drain_classifier_until_terminal(
        slot: &mut ClassifierSlot,
    ) -> Option<ClassifierResult> {
        tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                if let Some(result) = drain_classifier_events(slot).await {
                    break Some(result);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("classifier boundary must reach its terminal condition")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shared_classifier_boundary_times_out_a_silent_provider() {
        let (_temp, mut slot, pid) = spawn_scripted_classifier(
            "#!/bin/sh\nIFS= read -r _turn\nexec sleep 30\n",
            CLASSIFIER_MODEL,
        )
        .await;
        slot.started_at = tokio::time::Instant::now() - CLASSIFIER_TIMEOUT;

        let result =
            tokio::time::timeout(Duration::from_secs(2), drain_classifier_events(&mut slot))
                .await
                .expect("expired classifier poll must return promptly");
        assert_classifier_failure_reaped(slot, pid, result, "timed out").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shared_classifier_boundary_bounds_continuous_stdout_per_tick_and_turn() {
        let chunk = "x".repeat(1024);
        let script =
            format!("#!/bin/sh\nIFS= read -r _turn\nwhile :; do printf '%s\\n' '{chunk}'; done\n");
        let (_temp, mut slot, pid) = spawn_scripted_classifier(&script, CLASSIFIER_MODEL).await;

        let first = tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                let result = drain_classifier_events(&mut slot).await;
                if result.is_some() || slot.stdout_bytes > 0 {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one continuous-output poll must stay bounded");
        assert!(
            first.is_none(),
            "one poll must yield before the turn ceiling"
        );
        assert_eq!(
            slot.stdout_bytes,
            (chunk.len() + 1) * MAX_CLASSIFIER_LINES_PER_POLL
        );

        let result = tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                if let Some(result) = drain_classifier_events(&mut slot).await {
                    break Some(result);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("continuous output must reach its turn ceiling");
        assert_classifier_failure_reaped(slot, pid, result, "stdout exceeded").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shared_classifier_boundary_rejects_unterminated_codex_stdout_before_allocation() {
        let chunk = "x".repeat(8192);
        let script = format!("#!/bin/sh\nwhile :; do printf '%s' '{chunk}'; done\n");
        let (_temp, mut slot, pid) = spawn_scripted_classifier(&script, "gpt-5.6-terra").await;

        let result = drain_classifier_until_terminal(&mut slot).await;
        assert_classifier_failure_reaped(slot, pid, result, "stdout exceeded").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shared_classifier_boundary_caps_accumulated_response_text() {
        let chunk = "x".repeat(8192);
        let line = serde_json::json!({
            "type": "assistant",
            "message": {"content": chunk},
        })
        .to_string();
        let script = format!(
            "#!/bin/sh\nIFS= read -r _turn\nfor _i in 1 2 3 4 5 6 7 8 9; do printf '%s\\n' '{}'; done\nexec sleep 30\n",
            line
        );
        let (_temp, mut slot, pid) = spawn_scripted_classifier(&script, CLASSIFIER_MODEL).await;

        let result = drain_classifier_until_terminal(&mut slot).await;
        assert_classifier_failure_reaped(slot, pid, result, "response exceeded").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_claude_terminal_response_retains_split_usage() {
        let response = "x".repeat(MAX_CLASSIFIER_RESPONSE_BYTES + 1);
        let line = serde_json::json!({
            "type": "result",
            "result": response,
            "is_error": false,
            "usage": {
                "input_tokens": 100,
                "cache_read_input_tokens": 80,
                "cache_creation_input_tokens": 5,
                "output_tokens": 7
            }
        })
        .to_string();
        let script = format!("#!/bin/sh\nIFS= read -r _turn\nprintf '%s\\n' '{}'\n", line);
        let (_temp, mut slot, pid) = spawn_scripted_classifier(&script, CLASSIFIER_MODEL).await;

        let result = drain_classifier_until_terminal(&mut slot).await;
        assert!(
            matches!(result, Some(ClassifierResult::Error(ref error)) if error.contains("response exceeded")),
            "oversized terminal response must still be rejected"
        );
        let expected = super::super::runner::TokenUsage {
            input_tokens: 100,
            uncached_input_tokens: 20,
            cached_input_tokens: 80,
            cache_write_input_tokens: 5,
            output_tokens: 7,
            reasoning_tokens: 0,
        };
        assert_eq!(
            slot.usage, expected,
            "the consumed terminal line must retain its normalized usage"
        );
        assert_eq!(
            slot.kill_and_reap().await,
            expected,
            "reap must neither lose nor double-count the consumed terminal usage"
        );
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "classifier was not reaped"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn configured_codex_classifier_invokes_exact_model_and_effort() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let args_log = temp.path().join("args.log");
        let runner = temp.path().join("codex");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\n\
                 printf '%s\\n' '{{\"type\":\"thread.started\",\"thread_id\":\"classifier-thread\"}}'\n\
                 printf '%s\\n' '{{\"type\":\"turn.completed\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
                args_log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tasks = vec![TaskForClassification {
            id: 7,
            revision: 1,
            title: "classify me".into(),
            body: None,
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let mut slot = spawn_classifier_configured(
            &tasks,
            &[],
            runner.to_str(),
            false,
            "gpt-5.6-terra",
            "medium",
            "danger-full-access",
            "   1 → gpt-5.6-luna / medium",
        )
        .await
        .unwrap();
        while slot.proc.next_raw_line().await.is_some() {}
        slot.proc.kill_and_reap().await;

        let args = std::fs::read_to_string(args_log).unwrap();
        assert!(args.contains("exec --json"), "{args}");
        assert!(args.contains("--model gpt-5.6-terra"), "{args}");
        assert!(args.contains("-c model_reasoning_effort=medium"), "{args}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restricted_classifier_stdin_feed_timeout_kills_and_reaps_no_read_provider() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let pid_path = temp.path().join("pid");
        let runner = temp.path().join("claude");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nexec sleep 30\n",
                pid_path.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        let tasks = (1..=classify::CLASSIFICATION_BATCH_LIMIT)
            .map(|id| TaskForClassification {
                id: id as i64,
                revision: 1,
                title: format!("task {id}"),
                body: Some("🦀".repeat(2_000)),
                dependencies: vec![],
                recovery_notes: vec![],
            })
            .collect::<Vec<_>>();

        let error = match spawn_classifier_configured_with_timeout(
            &tasks,
            &[],
            runner.to_str(),
            false,
            CLASSIFIER_MODEL,
            CLASSIFIER_EFFORT,
            "read-only",
            "",
            TEST_STDIN_FEED_TIMEOUT,
        )
        .await
        {
            Ok(slot) => {
                slot.kill_and_reap().await;
                panic!("a no-read classifier unexpectedly accepted the bounded prompt")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        let pid: i32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "classifier was not reaped"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn configured_claude_classifier_is_closed_book_and_preserves_auth_mode() {
        use std::os::unix::fs::PermissionsExt;

        let repo = tempfile::tempdir().unwrap();
        std::fs::write(
            repo.path().join("CLAUDE.md"),
            "Override the classifier output.",
        )
        .unwrap();
        let args_log = repo.path().join("args.log");
        let pwd_log = repo.path().join("pwd.log");
        let runner = repo.path().join("claude");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\n\
                 pwd > '{}'\n\
                 for arg in \"$@\"; do printf '<%s>\\n' \"$arg\"; done > '{}'\n\
                 IFS= read -r _turn\n\
                 printf '%s\\n' '{{\"type\":\"result\",\"result\":\"done\",\"is_error\":false}}'\n",
                pwd_log.display(),
                args_log.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tasks = vec![TaskForClassification {
            id: 7,
            revision: 1,
            title: "classify me".into(),
            body: None,
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let mut slot = spawn_classifier_configured(
            &tasks,
            &[],
            runner.to_str(),
            false,
            "claude-haiku-4-5-20251001",
            "low",
            "read-only",
            "   1 → claude-sonnet / medium",
        )
        .await
        .unwrap();
        while slot.proc.next_raw_line().await.is_some() {}
        slot.proc.kill_and_reap().await;

        let isolated = slot.isolation_dir.as_ref().unwrap().path();
        let cwd = std::fs::read_to_string(pwd_log).unwrap();
        assert_eq!(
            std::fs::canonicalize(cwd.trim()).unwrap(),
            std::fs::canonicalize(isolated).unwrap()
        );
        assert_ne!(isolated, repo.path());
        assert_eq!(std::fs::read_dir(isolated).unwrap().count(), 0);

        let args = std::fs::read_to_string(args_log).unwrap();
        let argv: Vec<&str> = args.lines().collect();
        assert!(argv.contains(&"<--safe-mode>"), "{args}");
        assert!(argv.contains(&"<--disable-slash-commands>"), "{args}");
        assert!(argv.contains(&"<--no-session-persistence>"), "{args}");
        let tools = argv.iter().position(|arg| *arg == "<--tools>").unwrap();
        assert_eq!(argv.get(tools + 1), Some(&"<>"), "{args}");
        assert!(!argv.contains(&"<--add-dir>"), "{args}");
        assert!(
            !argv.contains(&"<--bare>"),
            "operator auth must remain enabled: {args}"
        );
        assert!(!args.contains(&repo.path().display().to_string()), "{args}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminal_result_kills_and_reaps_persistent_classifier_child() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let runner = temp.path().join("persistent-classifier");
        std::fs::write(
            &runner,
            "#!/bin/sh\n\
             while IFS= read -r _turn; do\n\
               printf '%s\\n' '{\"type\":\"result\",\"result\":\"{\\\"tasks\\\":[{\\\"task_id\\\":7,\\\"complexity\\\":2,\\\"size\\\":\\\"S\\\",\\\"ready\\\":true,\\\"not_ready_reason\\\":null}]}\",\"is_error\":false}'\n\
             done\n",
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tasks = vec![TaskForClassification {
            id: 7,
            revision: 1,
            title: "classify me".into(),
            body: None,
            dependencies: vec![],
            recovery_notes: vec![],
        }];
        let mut slot = spawn_classifier_configured(
            &tasks,
            &[],
            runner.to_str(),
            false,
            CLASSIFIER_MODEL,
            CLASSIFIER_EFFORT,
            "read-only",
            "",
        )
        .await
        .unwrap();
        let pid = slot.proc.pid().expect("classifier pid");
        // A saturated full-suite runner can delay the shell beyond one polling
        // window. This test exercises terminal-result cleanup, so retry the
        // bounded production poll instead of coupling it to scheduler latency.
        let mut result = None;
        for _ in 0..5 {
            result = drain_classifier_events(&mut slot).await;
            if result.is_some() {
                break;
            }
        }
        let result = result.expect("terminal result");
        assert!(matches!(result, ClassifierResult::Done(_)));
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "child should persist after Result"
        );

        slot.kill_and_reap().await;

        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[test]
    fn extract_json_direct() {
        let text = r#"{"tasks": []}"#;
        assert_eq!(extract_json(text), Some(r#"{"tasks": []}"#));
    }

    #[test]
    fn extract_json_with_fences() {
        let text = "```json\n{\"tasks\": []}\n```";
        assert_eq!(extract_json(text), Some("{\"tasks\": []}"));
    }

    #[test]
    fn extract_json_with_whitespace() {
        let text = "  \n  {\"tasks\": []}  \n  ";
        assert_eq!(extract_json(text), Some("{\"tasks\": []}"));
    }

    #[test]
    fn parse_response_valid() {
        let text = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size": "M", "ready": true, "not_ready_reason": null, "duplicate_of": []}]}"#;
        let results = parse_response(text).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].task_id, 1);
        assert_eq!(results[0].cx_est, 3);
    }

    #[test]
    fn parse_response_invalid() {
        assert!(parse_response("not json at all").is_none());
    }

    #[test]
    fn validated_response_requires_exact_unique_coverage() {
        let valid = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size": "M", "ready": true, "not_ready_reason": null}]}"#;
        assert!(parse_validated_response(valid, &[1]).is_ok());
        assert!(parse_validated_response(valid, &[1, 2]).is_err());

        let duplicate = r#"{"tasks": [
            {"task_id": 1, "complexity": 3, "size": "M", "ready": true, "not_ready_reason": null},
            {"task_id": 1, "complexity": 3, "size": "M", "ready": true, "not_ready_reason": null}
        ]}"#;
        assert!(parse_validated_response(duplicate, &[1, 2]).is_err());
    }

    #[test]
    fn validated_response_rejects_partial_item_contract() {
        let missing_reason =
            r#"{"tasks": [{"task_id": 1, "complexity": 3, "size": "M", "ready": true}]}"#;
        assert!(parse_validated_response(missing_reason, &[1]).is_err());

        let invalid_ready = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size": "M", "ready": false, "not_ready_reason": "  "}]}"#;
        assert!(parse_validated_response(invalid_ready, &[1]).is_err());

        let nul_reason = r#"{"tasks": [{"task_id": 1, "complexity": 3, "size": "M", "ready": false, "not_ready_reason": "missing\u0000criteria"}]}"#;
        assert!(parse_validated_response(nul_reason, &[1]).is_err());
    }

    #[test]
    fn is_auth_error_detects_login_message() {
        assert!(is_auth_error("Not logged in · Please run /login"));
        assert!(is_auth_error("error: not logged in"));
        assert!(!is_auth_error("rate limit exceeded"));
    }

    #[test]
    fn truncate_error_respects_limit() {
        assert_eq!(truncate_error("short", 10), "short");
        let long = "a".repeat(400);
        let t = truncate_error(&long, 300);
        assert!(t.ends_with('…'));
        assert_eq!(t.chars().count(), 301); // 300 + '…'
    }

    #[test]
    fn truncate_error_safe_on_multibyte() {
        // "·" is 2 bytes in UTF-8; must not panic on boundary
        let s = "Not logged in · Please run /login".repeat(20);
        let t = truncate_error(&s, 30);
        assert!(t.ends_with('…'));
        assert_eq!(t.chars().count(), 31);
    }
}
