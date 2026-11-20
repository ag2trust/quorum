# External CLI Runner — Technical Specification

**Date:** 2026-11-20
**Status:** Proposed
**Implementation base:** `origin/develop` at `579b3bb`
**Related:** `docs/2026-11-12-sandboxed-workspace-technical-spec.md` (session directory, launcher
seam). This spec amends its session directory: see "Sessions".

## Problem

Quorum drives exactly three agent CLIs. `AgentKind` is a closed enum of Claude, Codex, and Grok
(`quorum/src/serve/runner.rs:543-551`), and its doc comment says supporting another runner
"requires an explicit code change, not configuration". Each variant owns a private adapter that
builds the command line, parses that CLI's stream, and classifies its failures. The runner is
derived from the model name (`AgentKind::for_model`, `runner.rs:1604-1631`) at every launch
entry point, so an unknown model is refused before any process starts.

`agent_bin` replaces only the executable (`agent_bin_for_kind`, `quorum/src/serve/mod.rs:4154`).
Whatever runs in its place must still speak the Claude, Codex, or Grok protocol, byte for byte.
The design document says the same of the MCP contract: it holds "for the default
Claude-compatible binary and any `agent_bin` substituted for it"
(`docs/2026-06-23-quorum-design.md:105`).

An operator who wants Quorum to drive any other headless agent CLI (an in-house agent, a CLI
run inside a sandbox, a new vendor's tool) must therefore fork Quorum or write a shim that
impersonates one of the three protocols, including their failure text. Such a shim is fragile,
because Quorum's failure classification reads protocol details and error wording
(`agent.rs:325-460`, `codex_agent.rs:897`).

Two adjacent gaps make this worse:

- **A question has nowhere to go but MCP.** A run can ask for input only by calling `react` with
  `needs-info` through the agent MCP (`quorum/src/agent_mcp.rs:475-480`). A CLI that ends with
  its own "I am blocked, here is my question" result has no mapping, and its exit is classified
  as a failure.
- **An answer does not resume anything.** After a needs-info park the worker slot is gone, so a
  `message_agent` sent to answer it is consumed and dropped (`mod.rs:18917-18920`). The owner's
  only path is `task-retry` (`quorum/src/coordinator.rs:126`), which starts a new run that sees
  the question and any answer only as task notes in its prompt (`load_worker_task_context_sync`,
  `mod.rs:160`). The context the asking run had is lost.

## Goals

- A fourth runner, **external**, drives any headless CLI described by a declaration in the serve
  configuration: how to start it, how to read its event stream, and what its terminal statuses,
  error kinds, and exit codes mean to Quorum.
- The external runner is one fixed adapter in Quorum's code. The declaration is data. This keeps
  the closed-enum rule of `docs/2026-06-23-quorum-design.md:2343`: there is no provider trait and
  no plugin loading.
- A CLI's declared **blocked** result parks the task as needs-info and stores the CLI's question
  on the task. The model does not have to call `react`.
- A new **answer** operation, on the CLI and the orchestrator MCP, records the owner's answer and
  resumes the task.
- For a CLI that keeps its conversation in a session file, Quorum allocates one per task and
  role slot, outside the workspace, passes its path on every run, and alone decides whether a run
  resumes or starts fresh. Quorum never reads the file.
- Failure classification stays fail-closed: anything undeclared, contradictory, or malformed
  grants no failover.
- Daemons that do not configure the external runner keep today's behavior, except for the new
  `task-answer` operation and its prompt bookkeeping (see "Compatibility").

## Non-goals

- Changing the Claude, Codex, or Grok adapters, or porting them to declarations.
- A plugin API, dynamic libraries, scripting, or any code supplied by configuration.
- More than one external declaration per daemon (see "Open questions").
- Running the Doctor through the external runner. Doctor stays hard-wired to Claude
  (`quorum/src/serve/doctor.rs:256`) and is off by default.
- Workspace isolation, read-only role materials, and how a confined run reaches the agent MCP.
  Those belong to clone mode (`docs/2026-11-12-sandboxed-workspace-technical-spec.md`).
- Mid-run token accounting for CLIs that report usage only at the end of a run.
- Reading, validating, migrating, or bounding the content of a session file.
- Questions from read-only roles (see "Open questions").

## Terms

| Term | Meaning |
|---|---|
| **Declaration** | The `[external_runner]` table in the serve configuration. |
| **Fingerprint** | Lowercase hex SHA-256 of the RFC 8785 (JCS) canonical JSON form of the parsed `[external_runner]` table, after defaults are filled in, plus a top-level `"agent_bin"` member holding the effective `agent_bin` or `null`. Recorded with each session. |
| **Protocol line** | One line of the CLI's stdout that parses as a JSON object. |
| **Terminal event** | The one protocol line that ends a run and carries its status. |
| **Outcome** | What a terminal status means to Quorum: `success`, `blocked`, `failure`, or `cancelled`. |
| **Access level** | `read-write`, `read-only`, or `restricted`, derived from the launch (see "Access levels"). |
| **Slot** | The role position a session belongs to: `worker`, `reviewer-r1`, or `reviewer-r2`. These are the positions that already keep a continuation (`quorum-core/src/runner_state.rs:10-12`). |
| **Session file** | `<state dir>/sessions/<task id>/<slot>/session`. Written by runs; untrusted; never read by the daemon. |
| **Session record** | Quorum's durable record of a slot's session, in task refs (see "Session record"). |
| **Resume** | A run started with an existing session file and a short continuation prompt. |
| **Fresh start** | A run started after the daemon has removed the session file, with the full role prompt. |

## Current-state summary

| Concern | Today | Where |
|---|---|---|
| Runner selection | Derived from the model string at every entry point | `runner.rs:1082, 1134, 1160, 1200`; `classifier.rs:157`; `collector.rs:1016` |
| Profile check | `profile.runner` must equal `for_model(profile.model)`; per-runner effort sets | `quorum/src/serve_config.rs:794-817` |
| Config runner names | `claude`, `codex`, `grok` | `serve_config.rs:68` |
| Adapters | `RunnerProc::{Claude, Codex, Grok}`, matched everywhere | `runner.rs:1069` |
| Neutral inputs | `LaunchRequest` (no runner field, by design), `LaunchMode`, `TurnMode` | `runner.rs:792-815`, `:1043`, `:782-790` |
| MCP roles | `AgentMcpRole::{Worker, Reviewer, Planner}`; classifier, collector, arbiter run with `managed: None` | `agent_mcp.rs:867-871`; `collector.rs:1755-1770`; `classifier.rs:141` |
| Normalized events | `AgentEvent` | `runner.rs:1671` |
| Failure dispositions | `ProviderUnavailable`, `ProfileUnavailable`, `RetryableSameRoute`, `NonFailover`, `Unclassified` | `quorum-core/src/routing_attempts.rs:20-31` |
| Exit without evidence | Exit 0, or any protocol line without a terminal: `RetryableSameRoute`; otherwise `NonFailover` | `runner.rs:273-291` |
| Run environment | Removes the orchestrator token, GitHub tokens, `QUORUM_HOME`; private `GH_CONFIG_DIR` | `runner.rs:699-746` |
| Teardown | Process-tree kill, with no SIGTERM stage and no grace | `quorum/src/serve/proctree.rs:57` |
| Exit precedence | `submit_work` outcome, then `react` state, then exit classification | `quorum-core/src/tasks.rs:5380-5452` |
| needs-info park | `react` only; note `parked: needs-info …`; status `failed` with `daemon_parked` | `tasks.rs:5548-5590` |
| Retry | `coordinator::task_retry` checks for a cancelled dependency and the reviewer cap, then calls `retry_parked`, which strips the park refs | `coordinator.rs:126-150`; `tasks.rs:6676`, `:6960-6980` |
| Continuations | `runner_continuation*` refs as `ContinuationIdentity {provider, id}`; a fresh Grok initial worker refuses any worker continuation | `runner_state.rs:17-21`; `mod.rs:21649-21656` |
| Notes in prompts | Each note capped at 4 KiB; the 4 newest are shown; non-creator notes are labelled | `mod.rs:83-120` |
| `agent_bin` | Only when every configured profile uses one runner | `serve_config.rs:873-890` |
| USD limits | Refused when routing can select Codex or Grok | `serve_config.rs:908` |

## Design overview

```
serve config ──► [external_runner] declaration ──► validated ExternalSpec (+ fingerprint)
                                                           │
LaunchRequest{runner} ──► RunnerProc::External ──► argv + env ──► agent_bin or executable
                          ▲                                    │ stdout (JSONL)
                          │                                    ▼
           AgentEvent ◄── declarative stream mapping ◄── protocol lines
                          │
                          └─ terminal status ─► outcome ─► success | failure(disposition)
                                                           | blocked ─► needs-info park
```

`AgentKind` gains a fourth variant, `External`, with `TurnMode::RespawnPerTurn`: each turn is a
new process, as for Codex and Grok. `RunnerKind` accepts `"external"`.

## Runner identity

The runner comes from the routed profile, not from the model string.

- `LaunchRequest` gains `runner: AgentKind`. Every caller sets it from the profile it routed,
  and the `RunnerProc` entry points (`runner.rs:1082, 1134, 1160, 1200`) dispatch on it. The doc
  comment at `runner.rs:792` changes accordingly.
- For a built-in runner, the entry points still check `for_model(model) == runner` and refuse a
  mismatch, so "cannot spawn one runner and persist another" still holds.
- For `External`, `for_model` is never called. The model string is passed through verbatim.
- `validate_model_routing` (`serve_config.rs:794-817`) skips the model cross-check for external
  profiles. Effort for an external profile must match `[a-z0-9_-]{1,32}`, and, if the declaration
  lists `efforts`, be one of them.
- The other places that hold a runner kind take it from the profile or from the durable record,
  never from the model: `classifier.rs:157`, `collector.rs:1016`, `serve_config.rs:521-557`, slot
  state and recovery (`quorum/src/serve/recovery.rs:35`, `mod.rs:20279`), and the journal
  `provider` column, which records `"external"`.
- `agent_bin_for_kind` (`mod.rs:4154-4176`) learns `"external"`.

Slice 1 must find every remaining `for_model` call outside tests and convert it. The list above
is the audit at `579b3bb`.

## Configuration

```toml
# One external CLI per daemon. Absent: the external runner is unavailable.
# Restart-only: a reload that changes this table is refused and the old one stays in force.
[external_runner]
# Absolute path. agent_bin, when set, takes precedence (see "agent_bin").
executable = "/usr/local/bin/example-agent"
# Optional allowed effort values. Absent: any value matching [a-z0-9_-]{1,32}.
efforts = ["low", "medium", "high"]
# The prompt is always written to stdin, which is then closed. No other mode exists.
# Arguments are literal argv elements. Placeholders are substituted inside an element; there is
# no shell and no splitting. "{{" and "}}" produce literal braces.
args            = ["exec", "--workspace", "{workspace}", "--model", "{model}", "--effort", "{effort}", "-"]
args_read_write = ["--tools", "read-write"]   # see "Access levels"
args_read_only  = ["--tools", "read-only"]
args_restricted = ["--tools", "none"]
args_mcp        = ["--mcp-url", "{agent_mcp_url}"]   # only for runs wired to the agent MCP
args_session    = ["--session", "{session_file}"]    # only for slot runs; omit to disable sessions

# Extra environment for the CLI, by name, from a fixed set of run fields. The standard
# QUORUM_* run variables are still set.
[external_runner.env]
EXAMPLE_MCP_TOKEN = "run_capability"   # one of: run_capability, agent_mcp_url, repo, agent

[external_runner.stream]
type = "/type"                 # JSON Pointer (RFC 6901) to the event discriminator
max_line_bytes = 1048576       # longer lines are a protocol violation
# Seconds to wait for the process to exit after the terminal event. Default 30.
terminal_exit_secs = 30
# Optional first event. When declared, it must be the first protocol line.
start = { type = "run.started", format = "/format", accept_formats = [2] }
# Assistant text and tool activity. `when` adds exact-match conditions on other pointers.
text     = { type = "item.completed", when = { "/item/type" = "message" }, text = "/item/text", id = "/item/id" }
activity = { type = "item.started", when = { "/item/type" = "tool_call" }, summary = "/item/name" }

[external_runner.stream.terminal]
type          = "run.completed"
status        = "/status"
final_text    = "/final_text"       # the run's terminal text (classifier, arbiter)
error_kind    = "/error/kind"
question      = "/blocker/question" # required when a status has the blocked outcome
question_context = ["/blocker/reason", "/blocker/summary", "/blocker/details"]
usage = { uncached_input = "/usage/input", cached_input = "/usage/cache_read", cache_write_input = "/usage/cache_write", output = "/usage/output" }

# Every terminal status the CLI can emit, with the exit code it must accompany. A process killed
# by signal N counts as exit 128+N.
[external_runner.statuses]
completed      = { outcome = "success",   exit = 0 }
blocked        = { outcome = "blocked",   exit = 5 }
cancelled      = { outcome = "cancelled", exit = 143 }
limit_reached  = { outcome = "failure",   exit = 3, disposition = "non_failover" }
internal_error = { outcome = "failure",   exit = 1, disposition = "non_failover" }
mcp_error      = { outcome = "failure",   exit = 6, disposition = "retryable_same_route" }
provider_error = { outcome = "failure",   exit = 4, disposition = "by_error_kind" }

# Used only by statuses whose disposition is "by_error_kind".
[external_runner.error_kinds]
auth              = "provider_unavailable"
billing           = "provider_unavailable"
rate_limited      = "provider_unavailable"
model_unavailable = "profile_unavailable"
overloaded        = "retryable_same_route"
network           = "retryable_same_route"
bad_request       = "non_failover"
refused           = "non_failover"

# Non-zero exit codes the CLI uses without writing a terminal event (for example, a usage error
# reported only on stderr).
[external_runner.exits_without_terminal]
2 = "non_failover"

[model_profiles.example-high]
runner = "external"
model  = "example-model-1"     # passed verbatim as {model}
effort = "high"                # passed verbatim as {effort}
```

### Placeholders

| Placeholder | Value | Allowed in |
|---|---|---|
| `{workspace}` | The run's workspace path | any args list |
| `{model}`, `{effort}` | From the routed profile | any args list |
| `{agent_mcp_url}` | `QUORUM_AGENT_MCP_URL` of the run | `args_mcp` only |
| `{session_file}` | The slot's session file path | `args_session` only |

No placeholder carries the run capability. Secrets reach the CLI only through the environment,
so they never appear in argv, process listings, or logs.

### Validation

Startup refuses the configuration (exit 2), naming the key, when any rule below fails. A reload
that fails any rule, or that changes `[external_runner]` at all, is rejected with the same
message, and the running configuration stays in force. `[external_runner]` is registered as a
restart-only key in the reload dispositions (`serve_config.rs:285-345`).

- `executable` is not absolute, or neither `executable` nor `agent_bin` is set while a profile
  uses `runner = "external"`.
- A placeholder is unknown, appears outside the lists the table above allows, or a brace is
  unbalanced.
- An `env` name is not a valid environment name; is in `MANAGED_RUN_REMOVED_ENV`
  (`runner.rs:699`); is set by the managed run environment or by `ManagedRunEnv` (including
  `GH_CONFIG_DIR`); starts with `QUORUM_`, `LD_`, or `DYLD_`; or is `PATH` or `HOME`. A value is
  not one of the four run fields.
- A JSON Pointer is malformed, or `stream.type` or the terminal's `type` or `status` is missing.
- A status has no `exit`, two statuses share an `exit`, or no status has the `success` outcome.
- A status has the blocked outcome and `terminal.question` is absent.
- A disposition names something other than `provider_unavailable`, `profile_unavailable`,
  `retryable_same_route`, or `non_failover`. `unclassified` is never configurable.
- `by_error_kind` is used without `terminal.error_kind` or an `error_kinds` table.
- `exits_without_terminal` lists exit 0, lists a status's `exit`, or maps a code to anything but
  `non_failover` or `retryable_same_route`. Failover needs a terminal event.
- A profile routed to a role whose access level is `read-write`, `read-only`, or `restricted`
  has no matching `args_*` list. A profile routed to a role wired to the agent MCP has no
  `args_mcp`.
- A profile is routed to the classifier or arbiter and `terminal.final_text` is absent.
- A USD cost limit is configured and routing can select an external profile. This is the same
  rule as for Codex and Grok (`serve_config.rs:908`); the external runner reports no cost.
- Until slice 4 lands, any status with the blocked outcome. Until slice 6 lands, `args_session`.

### `agent_bin`

`agent_bin` keeps its meaning: it replaces the executable. When every configured profile uses
the external runner, `agent_bin`, if set, is started with the declared argv in place of
`executable`. The existing single-runner rule (`validate_agent_bin_for_profiles`,
`serve_config.rs:873`) is unchanged. A daemon may mix external and built-in profiles when
`agent_bin` is unset.

## Access levels

The access level comes from how the daemon already launches each role, not from a new role list:

| Launch | Roles today | Access level | MCP |
|---|---|---|---|
| `managed: Some(AgentMcpRole::Worker)` | worker (initial, rework, remediation, continuation) | `read-write` | `args_mcp` |
| `managed: Some(AgentMcpRole::Reviewer \| Planner)` | reviewer, planner | `read-only` | `args_mcp` |
| `managed: None` | classifier, collector, arbiter | `restricted` | none |

`restricted` must grant no tools, matching today's `LaunchMode::Restricted` and the collector's
empty tool list (`collector.rs:1755-1770`). Quorum cannot verify what a CLI's arguments grant;
the declaration is the operator's statement of it.

## Launch

For each run, the adapter builds argv in this order: `args`, the access level's `args_*` list,
`args_mcp` if the run is wired to the agent MCP, and `args_session` if the run has a slot and
sessions are enabled.

The environment is today's managed run environment (`managed_run_environment`, `mod.rs:4215`),
followed by the `env` table, with `ManagedRunEnv` applied last, as for every adapter. A slot run's
process also receives `QUORUM_SESSION_DIR`, the slot directory. It is a path, not a credential,
so an `agent_bin` that starts the CLI elsewhere can provide that one directory.

`check_agent_mcp_wiring` (`runner.rs:857`) requires the URL for the external runner, as it does
for Claude.

The adapter writes the prompt to stdin and closes it. It reads stdout line by line, and keeps
stderr only as bounded exit evidence (`EXIT_EVIDENCE_LINES`, `EXIT_EVIDENCE_LINE_BYTES`,
`runner.rs:27-29`). The external adapter never classifies failures from stderr text.

Teardown is the existing process-tree kill, with no SIGTERM stage (`proctree.rs:57`). A CLI that
keeps a session file must therefore save it after each step, so a kill loses at most the step in
progress. The serve configuration example documents this requirement.

## Stream mapping

Each stdout line is handled in order:

1. A line longer than `max_line_bytes` is a protocol violation.
2. A line that is not a JSON object is not a protocol line. It is recorded in diagnostics and
   otherwise ignored.
3. Every protocol line emits `AgentEvent::Liveness`, which refreshes the idle watchdog.
4. If `start` is declared, the first protocol line must match it, and its `format` value, if
   declared, must be in `accept_formats`. A mismatch stops the run: the daemon kills the tree and
   records `NonFailover` ("unsupported stream format").
5. A line matching `text` emits `CompletedAssistantText { item_id, text }`. `item_id` comes from
   the `id` pointer, or is `"<line number>"` when `id` is absent. A line matching `activity`
   emits `Activity { kind: ToolUse }`, with the summary truncated to the existing activity bound.
6. A line matching the terminal `type` is the terminal event (see below). A second terminal
   event, or any protocol line after it, is a protocol violation.
7. Anything else is ignored. Unknown event types and unknown fields are expected.

A protocol violation kills the tree and records `Unclassified` with a fixed detail. No text from
the offending line is stored.

The external runner emits no `ThreadStarted`, and writes no `runner_continuation*` ref. Its
session is identified by the slot (see "Sessions").

### Terminal event

The adapter reads the status and looks it up in `statuses`. An unknown status, or one that is not
a string, gives `NonFailover` ("undeclared terminal status"). Otherwise the status gives an
outcome, which takes effect only after the exit check below passes:

| Outcome | Event | Failure |
|---|---|---|
| `success` | `TurnCompleted { usage, cost_usd: None }`; `terminal_text` from `final_text` | none |
| `failure` | `TurnFailed { message, usage }` | the status's disposition, or the `error_kinds` entry; an unknown error kind gives `NonFailover` |
| `blocked` | `TurnBlocked { question, context, usage }` (new variant) | none; see "Blocked outcome" |
| `cancelled` | `TurnFailed { message, usage }` | `RetryableSameRoute` |

`message` is `<status>` or `<status>/<error kind>`, using only names declared in `statuses` and
`error_kinds`. An undeclared error kind is written as the fixed text `undeclared-error-kind`.
Quorum stores no free-text value from the CLI's error fields.

Usage: each declared pointer must hold a non-negative integer, and a missing pointer counts as 0.
The values fill `uncached_input_tokens`, `cached_input_tokens`, `cache_write_input_tokens`, and
`output_tokens` of `TokenUsage` (`runner.rs:1731-1739`). `input_tokens` is the sum of the three
input fields, and `reasoning_tokens` is 0. Any other value type is a protocol violation.

### Exit check

- **Run killed by Quorum** (watchdog, cancel, drain, or protocol violation): existing kill
  handling applies. Any terminal event is ignored.
- **Terminal event seen:** the daemon waits up to `terminal_exit_secs` for the process to exit.
  If it does not exit, the daemon kills the tree, and the result is `Unclassified` ("no exit
  after terminal event"). If it exits, the exit code (or 128+N for signal N) must equal the
  status's declared `exit`. If it does not, the result is `Unclassified` ("exit code contradicts
  terminal status"), whatever the status claimed.
- **No terminal event:** a code listed in `exits_without_terminal` uses its disposition.
  Otherwise today's rule applies (`runner.rs:273-291`): exit 0, or any protocol line seen, gives
  `RetryableSameRoute`; anything else gives `NonFailover`.
- **Launch errors** keep today's classification (`classify_launch_error`, `runner.rs:1562`).

The dispositions feed the existing routing, failover, provider-block, and retry machinery
unchanged.

## Blocked outcome

A blocked outcome that passed the exit check, from a `read-write` run, parks the task as
needs-info. It is evaluated at the same point as the existing exit park, and after the outcomes
that already take precedence there (`tasks.rs:5380-5452`):

1. A `submit_work` outcome, pending or recorded, wins. The blocked outcome is ignored, apart from
   a diagnostic.
2. A `react` with any state (`failed`, `blocked`, or `needs-info`) wins and parks as today. The
   runner's question is discarded.
3. Otherwise the runner's blocked outcome parks the task.

The park has the same transaction shape as a `react` park (`park_worker_on_reaction_tx`,
`tasks.rs:5548`):

- status `failed`, `assignee` cleared, lease deactivated;
- refs `daemon_parked`, `parked_reason = "needs-info"`, and `daemon_resume_status` as today, plus
  `daemon_parked_by = "runner-blocked"`;
- one `task_notes` row from `daemon` with the body
  `parked: needs-info\n\nworker <agent> question:\n<question>`, followed by each non-empty
  `question_context` value on its own line;
- the `task_parked` event, the owner alert, and graph blocking for a graph child, as today.

The question and its context are untrusted text. They must be non-empty strings without NUL.
The whole note body is cut at 4 KiB, on a character boundary, so that a later fresh-start prompt
shows it in full (`mod.rs:83-86`). A missing or empty question gives a `NonFailover` failure
("blocked without question"), not a park.

`set_parked_refs` (`tasks.rs:5691-5736`) and `retry_parked` (`tasks.rs:6960-6980`) also clear
`daemon_parked_by`, so it never outlives its park.

The slot's session file and session record are kept, so the answer can resume them.

A blocked outcome from a `read-only` or `restricted` run is a `NonFailover` failure with the
fixed detail "blocked outside a writing role". The question is not stored.

## Answer

A new coordinator operation answers a needs-info park and resumes the task. It works for every
runner.

```sh
quorum task-answer --repo owner/name --task-id <N> --by <identity> --body-stdin
```

The orchestrator MCP gets a matching tool, `task_answer { task_id, answer }`, which acts as the
orchestrator's identity, like `task_retry`.

`task-answer` runs the same pre-checks as `coordinator::task_retry` (`coordinator.rs:126-150`):
a cancelled dependency and the reviewer cap, with the same JSON shapes and exit 1. It also
refuses, with exit 1 and
`{"ok": false, "outcome": "task-answer-rejected", "reason": "task is not waiting for an answer"}`,
any task that is not daemon-parked with `parked_reason = "needs-info"`. It validates the answer:
non-empty after trimming, no NUL, and at most 4 KiB minus the 8-byte `answer:\n` prefix, so the
note fits the per-note prompt cap (`mod.rs:86`). An invalid answer exits 2.

Then, in one transaction, it:

1. inserts a `task_notes` row from `<identity>` with the body `answer:\n<text>`;
2. sets the ref `pending_resume = {"kind": "answer", "note_id": <id>}`;
3. restores the task as `retry_parked` does, with `reset_recovery_budget = true`.

This needs `retry_parked` split into a `retry_parked_tx` that runs inside a caller's
transaction. The decomposition-freeze check stays inside it (`tasks.rs:6771-6779`), so for
`task-answer`, as for `task-retry`, a frozen graph surfaces as that `Err` (a usage error) and the
whole transaction rolls back. On success the command prints
`{"ok": true, "task_id": <N>, "answer_note_id": <id>}` and exits 0.

The orchestrator `task_answer` tool maps results the way `task_retry` does
(`orchestrator_mcp.rs:783-792`). Exit 1 refusals return the refusal JSON with
`code: "rejected"`. A missing task returns `code: "not_found"`. An invalid answer and the freeze
error propagate as the tool error `task_retry` already returns for its own usage errors, with the
bounded message. No new code is added.

`retry_parked` itself changes on every daemon: when the park it restores has
`parked_reason = "needs-info"`, it sets `pending_resume = {"kind": "unanswered"}` in the same
statement. `task-answer` then overwrites that value with its `answer` value inside its own
transaction. Other parks set nothing. Its signature is unchanged.

### `pending_resume` lifecycle

- **Consumed** in the transaction that admits the task's next worker process. A park tears the
  slot down, so a restored task always comes back through a spawn, never through
  `feed_worker_turn`. Known spawn paths include the initial-worker admission
  (`spawn_admitted_worker`, `mod.rs:26638`) and remediation (launch at `mod.rs:33012`). Slice 5
  must audit and name every path that spawns a worker for an `open` or `rework` task, including
  replacement workers for `rework`, and consume the ref on each. The admitted run's prompt is
  chosen from it (see "Resume or fresh start"), and the ref is removed in the same transaction.
- **Cleared** by any later park (`set_parked_refs`), and by every write of a terminal status:
  `apply_event` with `Event::Cancelled`, the cancelled-dependency convergence
  (`tasks.rs:6374-6413`), `task-close`, and `done`. The clearing belongs in the shared helper that
  writes the terminal status, so a new terminal path cannot miss it.
- **Precedence:** when the task was parked in rework, `retry_parked` restores `rework` with
  `daemon_rework_retry_requested` (`tasks.rs:6943-6957`). `pending_resume` decides the prompt
  over the rework turn text.

For the Claude, Codex, and Grok runners, `pending_resume` changes nothing but its own removal.
Those runs start as `task-retry` runs do today, and the prompt's task notes carry the question
and the answer.

### Authority of the answer

The answer has exactly the authority of a task note from `<identity>`. In a fresh-start prompt it
is labelled by `label_task_note` (`mod.rs:96-120`) like any other note. In a resumed run it is
delivered as `Answer from <identity>:\n<text>`, with the identity truncated to
`NOTE_AUTHOR_LABEL_LIMIT`. It is never presented as a system or task-contract instruction.

`message_agent` keeps its current behavior.

## Sessions

Sessions are enabled when `args_session` is declared. They apply only to slot runs.

### Layout

```
<state dir>/sessions/<task id>/<slot>/session
```

The state directory is the per-repository directory that holds `quorum.db`
(`quorum/src/paths.rs:86-89`). This amends the clone-mode spec's "session directory"
(`docs/2026-11-12-sandboxed-workspace-technical-spec.md:87, 668`): it remains per task and outside
every workspace, and it is partitioned per slot. The launcher seam receives the slot directory,
not the task directory. The path does not depend on the workspace mode.

Partitioning matters only where runs are confined. A confined run gets one slot directory, so a
reviewer cannot read or alter the worker's conversation. Under the local launcher every run has
the daemon's user and can reach the whole state directory. There, partitioning prevents
accidents, not attacks.

### Daemon rules

- The daemon creates `sessions/`, `<task id>/`, and `<slot>/` with mode `0700`. Each component
  must be a real directory, checked with `lstat` and opened with `O_NOFOLLOW | O_DIRECTORY`.
  A symlink or a non-directory anywhere fails the launch with `NonFailover`.
- The daemon never opens, reads, parses, copies, or hashes the session file. The only operations
  on it are `fstatat(AT_SYMLINK_NOFOLLOW)` and `unlinkat`, both relative to the slot directory's
  descriptor.
- `{session_file}` is always the same path for a slot. Whether the CLI resumes depends only on
  whether the file exists when the run starts.
- A fresh start removes the file first. If it is a directory, the daemon removes it with the
  symlink-safe recursive removal of the clone-mode spec ("9. Cleanup"). If removal fails, the
  launch fails with `NonFailover`.
- The task's whole `sessions/<task id>/` tree is removed, with the same symlink-safe removal, when
  the task becomes `done` or `cancelled`, or is closed with `task-close`. Startup removes trees
  whose task is terminal or does not exist. Parked and failed tasks keep theirs.
- Quorum does not bound session file size. The operator, or the confinement around the run,
  does.

### Session record

A new ref, `runner_external_sessions`, holds one entry per slot that has run the external
runner:

```json
{ "worker": { "profile": "<profile id>", "fingerprint": "<hex>", "usable": true } }
```

It is separate from the `runner_continuation*` refs and their `ContinuationIdentity`
(`runner_state.rs:17-21`), which keep their meaning for the built-in runners. Its writes go
through its own helper, so `clear_provider_retry` and `set_continuation`
(`runner_state.rs:307-320`) never touch it.

- **Written** in the launch transaction of every external slot run.
- **Runner change:** when a slot's run launches on a built-in runner, that slot's external entry
  is removed and its session file is unlinked. When it launches on the external runner, the
  launch transaction removes that slot's `runner_continuation*` ref. This is new behavior; today's
  code keeps stale continuations, which is why a fresh Grok initial worker refuses one
  (`mod.rs:21649-21656`). Because the external runner never writes `runner_continuation*`, a
  later Grok initial worker sees no external state.
- **Journal:** an external run's journal row records `provider = "external"` and
  `continuation_id = <slot>`. Restart recovery accepts a dormant external worker for in-place
  rebuild when the slot's entry exists, is `usable`, and names the current profile and
  fingerprint. This replaces the refs-id equality check (`recovery.rs:330-337`) for external
  rows.

### Resume or fresh start

A run **resumes** only when all of these hold:

1. sessions are enabled and the run has a slot;
2. the slot's entry in `runner_external_sessions` exists, is `usable`, and names the routed
   profile and the current fingerprint;
3. `fstatat` shows the session file is a regular file of non-zero size;
4. the run is one of: an admission with `pending_resume`; rework after review; review
   remediation; a reviewer continuation; a provider-block retry turn; or the next turn of a
   dormant worker rebuilt in place after restart.

Every other run is a **fresh start**. That includes the first run of a slot, a run on a different
profile (failover or re-routing), a changed declaration, a task reopened after `AgentFailed` or
a stale lease, and a daemon restart that does not rebuild a dormant worker in place
(`recovery.rs:330`).

This spec does not change routing. Worker role assignments are durable per task revision
(`worker:task:{id}:revision:{n}`, `mod.rs:4547`; `role_assignments.rs:282-285`), so a task
restored by `task-answer` or `task-retry` normally keeps its assigned profile, and the existing
fallback and exclusion machinery (`routing_attempts`) still decides any change. Condition 2
only compares the profile the run actually launches on with the session entry's profile. If
they differ for any reason, the run is a fresh start.

Resumed-run prompts:

| Run | Prompt |
|---|---|
| `pending_resume.kind = "answer"` | `Answer from <identity>:\n<text>` |
| `pending_resume.kind = "unanswered"` | `No answer was provided. Continue with your best judgment, or ask again.` |
| Rework, remediation, reviewer continuation, provider retry, rebuilt dormant turn | The turn text Quorum feeds Codex and Grok today (`feed_worker_turn`, `mod.rs:20734`) |

A fresh start uses the full role prompt, which includes task notes.

**A resume that fails before any protocol line** marks the slot's entry `usable: false` in the
failure transaction. An example is a CLI that refuses a corrupt or locked session file with a
usage exit. The next run of that slot is then a fresh start, and the bounded retry budget
applies as for any other failure. A run that wrote protocol lines leaves the entry unchanged.

## Roles

| Role | External allowed | Slot | Access level | Blocked outcome |
|---|---|---|---|---|
| Worker (all forms) | yes | `worker` | `read-write` | parks as needs-info |
| Reviewer R1 / R2 | yes | `reviewer-r1` / `reviewer-r2` | `read-only` | failure |
| Planner | yes | none | `read-only` | failure |
| Classifier, arbiter | yes, with `final_text` | none | `restricted` | failure |
| Collector | yes | none | `restricted` | failure |
| Doctor | no | — | — | — |

Unlike Grok (`validate_grok_role_gate`, `serve_config.rs:853`), the external runner is not
limited to workers, because the operator declares what the CLI can do.

## Limits and accounting

- The existing wall-clock and idle limits apply (`max_idle_secs`, `max_turn_wall_secs`,
  `max_task_wall_secs`). A CLI that writes nothing during a long tool call trips `max_idle_secs`,
  so the configuration example says to set it above the CLI's longest silent step.
- Token usage is counted when the terminal event arrives. With no mid-turn usage, one run can
  exceed a Quorum token budget before Quorum sees it, so operators should also cap the run in the
  CLI's own arguments.
- No cost is reported, so USD limits are refused (see "Validation").
- The raw stream is kept in the session log under the existing 64 MiB cap
  (`quorum/src/serve/session_log.rs:35`).

## Security properties

- **No new authority for the CLI.** It gets the same managed run environment as every runner,
  plus only the declared names, each mapped from one of four fixed run fields. Names that Quorum
  removes from runs or sets itself cannot be reintroduced, and loader variables are refused.
- **No secrets in argv.** The capability is reachable only through the environment.
- **Untrusted stream.** Every value read from stdout is bounded, typed, and fail-closed. Only
  the question, its context, the final text, the status, and the error kind are stored, each as
  bounded data. Quorum stores no free-text error message.
- **Untrusted session file.** The daemon never reads it. It handles the path only with no-follow,
  descriptor-relative operations inside a daemon-created `0700` directory.
- **No configuration-driven code.** The declaration selects arguments and maps values. It cannot
  run a command in the daemon.

Residual risks:

- Within one slot, an earlier run of the same task can tamper with the session a later run
  resumes. Both runs have the same authority and serve the same task, so this is accepted.
- A descendant that left the run's process group and scrubbed its environment survives teardown
  (see the limitations in `proctree.rs`). It can recreate the session file after a fresh-start
  unlink, so the next run may resume a conversation Quorum meant to discard. This is the same
  boundary as for every runner. Confinement closes it.
- Under the local launcher, slot partitioning does not separate runs (see "Layout").

## Compatibility

Without `[external_runner]`, the adapter, stream mapping, session store, and access levels are
unreachable. Two pieces reach every daemon:

- `task-answer` and the orchestrator `task_answer` tool. They act only on a daemon-parked
  needs-info task and refuse everything else without writing.
- `pending_resume` bookkeeping on the built-in paths: `retry_parked` writes it when restoring a
  needs-info park, terminal-status writes clear it, and every worker spawn path removes it. Slice 5 must test, for each built-in runner, that
  prompts, routing, and continuations are byte-for-byte unchanged with `pending_resume` present
  and absent.

`daemon_parked_by` is a new ref, cleared wherever the other park refs are.

## Tests

- **Configuration:**
  - every validation rule has a refusal test;
  - a declaration copied from the example loads;
  - a reload that changes `[external_runner]` is rejected, and the old declaration stays.
- **Runner identity:**
  - an external profile whose model is `gpt-5` launches the external adapter;
  - a built-in profile with a mismatched model is still refused at launch;
  - no non-test `for_model` call remains on a path an external run reaches.
- **Adapter**, with a fake CLI script as `executable` or `agent_bin`, the pattern existing daemon
  tests use:
  - argv order and placeholder substitution for each access level, with brace escapes and no
    shell;
  - environment mapping and every refused name;
  - stdin is written and then closed.
- **Stream mapping**, from table-driven fixtures under `quorum/src/serve/testdata/external/`:
  - each event kind, and unknown events and fields;
  - an over-long line, a non-JSON line, and a format mismatch;
  - a second terminal event, and a protocol line after the terminal event;
  - usage typing.
- **Exit check:**
  - every declared status, with a matching exit and with a contradicting one;
  - signal deaths, using 128+N;
  - no exit within `terminal_exit_secs`;
  - an undeclared status, and an undeclared error kind;
  - `exits_without_terminal`, EOF without a terminal event, and a kill by Quorum after a terminal
    event.
- **Blocked:**
  - a worker parks with the question note;
  - the precedence rules (`submit_work`, each `react` state) hold;
  - a missing question fails the run, and an over-long note is truncated;
  - a reviewer's blocked outcome fails the run;
  - `daemon_parked_by` is cleared on retry and on later parks.
- **Answer:**
  - the CLI command and the MCP tool, on parked and non-parked tasks;
  - each `task-retry` pre-check;
  - the transaction is atomic: a failure after the note insert leaves no note;
  - `pending_resume` is consumed on each admission path, and cleared by park, cancel, and
    `task-close`;
  - a rework-phase park;
  - built-in runners are unchanged.
- **Sessions:**
  - the layout and directory modes;
  - symlink and directory substitution at each path component and at the file;
  - fresh-start removal, including a removal failure;
  - each resume condition, including a profile change and a fingerprint change;
  - `usable: false` after a resume that wrote no protocol line;
  - a runner change in each direction, including external to Grok;
  - restart recovery of a dormant external worker;
  - tree removal on `done`, `cancelled`, `task-close`, and at startup.

## Implementation slices

Each slice can be reviewed on its own and leaves the default path unchanged.

1. **Runner identity and configuration.** `LaunchRequest.runner` and every derivation site,
   `RunnerKind::External`, `[external_runner]` parsing and validation, and the reload
   disposition. Nothing launches yet. This is the largest slice.
2. **Session store.** The directory layout, the no-follow helpers, and terminal-task and startup
   removal. No run uses it yet.
3. **Adapter and stream mapping.** `AgentKind::External`, `RunnerProc::External`, argv,
   environment, access levels, stream mapping, the terminal and exit checks, and fixture tests.
   Sessions and blocked statuses stay refused by validation.
4. **Blocked outcome.** `AgentEvent::TurnBlocked`, the precedence rules, the park, and
   `daemon_parked_by`.
5. **Answer.** `retry_parked_tx`, `task-answer`, the orchestrator `task_answer` tool, and
   `pending_resume` for all runners, with the built-in prompts unchanged.
6. **Resume.** The session record, the runner-change rules, the resume-or-fresh decision, the
   resumed-run prompts, `usable` marking, and recovery of dormant external
   workers. Enables `args_session`.
7. **Documentation.** `docs/serve-config.example.toml`, the runner section of
   `docs/2026-06-23-quorum-design.md`, the coordinator text in the skill for `task-answer`, and
   the amendment note in the clone-mode spec.

Dependencies:
- Slices 1 and 2 can proceed in parallel.
- Slice 3 needs 1.
- Slice 4 needs 3.
- Slice 5 is independent of 2 through 4.
- Slice 6 needs 2, 3, 4, and 5.
- Slice 7 comes last.

## Open questions

1. **Several external declarations.** Should a daemon support named declarations
   (`[external_runners.<name>]`, `runner = "external:<name>"`)? One per daemon matches the single
   `agent_bin`. Recommendation: not now. The format can add names later without breaking the
   single form.
2. **Questions from read-only roles.** Should a reviewer's blocked outcome park the review for
   an answer instead of failing it? Recommendation: not now. Review parks recover differently,
   and no current role needs it.
3. **Unanswered retry prompt.** When `task-retry` resumes a needs-info park, is the fixed
   "no answer" text acceptable? The alternative is a fresh start, which loses the context the
   question was asked in.
4. **Answer through `message_agent`.** Should a `message_agent` to the worker that asked count as
   an answer while its task is parked needs-info? Recommendation: no. An answer is a lifecycle
   operation with its own validation and audit row, and a message to a former agent identity is
   ambiguous after reassignment.
