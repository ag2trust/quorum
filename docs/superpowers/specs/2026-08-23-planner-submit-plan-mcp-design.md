# Planner `submit_plan` over MCP — design

Date: 2026-08-23. Target branch: `main` (in-flight planner/Arbiter batch).

## Problem

The decomposition planner reports its plan as free text. `planner::parse_response`
(`quorum/src/serve/planner.rs:345`) requires the final assistant text to be exactly one
JSON object. Conversational providers prefix prose; task #58 graph 15 failed three
consecutive attempts with `response must be exactly one JSON object without wrappers`
although each transcript ended with a complete, valid plan. Each failure is a full
respawn and consumes one of `MAX_PROVIDER_FAILURES = 3`.

## Invariant introduced

Any managed agent that holds a tool surface reports outcomes **only** through the
authorized daemon endpoint (`quorum/src/serve/agent_endpoint.rs`). Agent text is
never consulted for lifecycle outcomes. Tool-less single-shot extraction calls
(classifier, collector) keep tolerant JSON extraction; they are out of scope.

Reviewers and workers already satisfy this via `quorum submit` → Unix socket. The
planner is sandboxed without a shell (`Read,Glob,Grep` / `-s read-only`), so its door
is an MCP tool. Arbiter follows in a later spec.

## Design

### 1. Endpoint: `Operation::SubmitPlan`

- `agent_endpoint.rs` gains `Operation::SubmitPlan { response: serde_json::Value }`.
- Authority: `resolve_planner_context` (section 3). The planner run is issued a
  `run_capabilities` row with `role='planner'` before spawn (pattern:
  `quorum-core/src/decomposition_review.rs:225-240`). A planner capability is accepted
  by `SubmitPlan` only; `Protocol`, `AppendNote`, `Submit`, `React` reject it.
- Validation, all at call time (before the write transaction, see section 3):
  1. byte bound `MAX_RESPONSE_BYTES` (existing);
  2. `serde_json` into `PlannerResponse` (deny unknown fields, existing type);
  3. `validate_semantics` and `validate_for_source` (existing functions, moved to a
     location callable from the endpoint; the planning source is loaded from
     `task_decompositions` by graph id resolved from the capability).
  Any failure → `Response::error("invalid_plan", <message>)`, nothing recorded.
- Storage: new table (forward-only, idempotent migration)
  `planner_submissions(run_id TEXT PRIMARY KEY, graph_id INTEGER NOT NULL,
  response_json TEXT, rejections INTEGER NOT NULL DEFAULT 0, accepted_at INTEGER)`.
  A row is created on the first call for the run.
- Acceptance: guarded
  `UPDATE planner_submissions SET response_json=?, accepted_at=? WHERE run_id=? AND
  response_json IS NULL RETURNING run_id`; zero rows → `already_submitted`, the first
  submission stands.
- Bound: each rejected call increments `rejections` in the same transaction. When
  `rejections >= MAX_PLAN_SUBMIT_REJECTIONS = 5` the call returns
  `submit_budget_exhausted` without validating.

### 2. MCP tool: `submit_plan`

- `quorum/src/agent_mcp.rs` adds tool `submit_plan` with input schema
  `{ "response": <object> }` proxied to `Operation::SubmitPlan`. The schema description
  carries the PLAN/BLOCKER shapes currently inlined in the planner prompt.
- The endpoint result is returned verbatim as the tool result so the agent sees
  validation messages and can resubmit within the turn.

### 3. Spawn wiring

- `AgentProc::spawn_planner` and `CodexProc::spawn_planner` accept
  `Option<AgentMcpServer>` and pass it through `spawn_configured` /
  `planner_exec_args`. Claude: `RestrictedMode::Planner` keeps `--tools Read,Glob,Grep`
  and the MCP allowlist adds `mcp__quorum__submit_plan` only (no `mcp__github__*`).
  Codex: `-s read-only` stays; the MCP server override is appended as for other roles.
  Verify with a real-binary argument test that the Codex read-only sandbox permits the
  stdio MCP child to connect to the Unix socket; if it does not, record the finding and
  stop — do not loosen the sandbox.
- The planner capability envelope (`QUORUM_REPO`, `QUORUM_AGENT`, `QUORUM_RUN_ID`,
  `QUORUM_AGENT_ENDPOINT`) is placed in the planner process environment, exactly as
  for workers/reviewers, because the stdio MCP child inherits it
  (`claude_mcp_config` at `agent.rs:62` carries no `env` block; Codex forwards via
  `env_vars`). The planner has no shell, so the token is reachable only through its
  file-read tools. Moving the envelope into provider `env` blocks is a follow-up.
- Authority for `SubmitPlan` uses a dedicated resolver
  `quorum_core::capabilities::resolve_planner_context(conn, run_id)` returning
  `{ run_id, task_id, graph_id, source_revision }`: capability exists, not revoked,
  `role='planner'`, and `task_decompositions` has a row with `source_task_id=task_id`,
  `freeze_active=1`, `active=0`, `planner_session_id=run_id` (the session id recorded
  by `decomposition::set_frozen_phase` at spawn). `resolve_live_run_context` keeps rejecting
  `planner` for every other operation, so no `LiveRunPhase::Planner` is needed.
- `validate_for_source` is async and touches the filesystem; the endpoint validates
  before `BEGIN IMMEDIATE` and only records inside the transaction. The rejection
  counter and once-only guard are checked inside the transaction.
- `spawn_planner` in `planner.rs` mints the run id via `agent::new_session_id()`,
  issues the `planner` capability, then spawns. Capability issuance and spawn follow
  the reviewer provisioning pattern (issue → spawn → persist).

### 4. Outcome consumption

- `PlannerSlot::poll` no longer harvests `response_text`. On process exit the
  coordinator (`serve/mod.rs` planner poll site) reads `planner_submissions` for the
  run id:
  - row present → `PlannerPoll::Done(response)` → existing `accept_proposal` / Arbiter
    path, unchanged;
  - no row → `PlannerPoll::ProviderFailed("planner exited without submit_plan")`.
  `SemanticRejected` is no longer produced by the poll (semantic errors are returned
  in-turn); the variant remains for the Arbiter rejection path.
- `parse_response` and text-harvest code are deleted with their tests. Text output
  is never a fallback.
- Retry budgets, `PRIOR_REJECTIONS`, and the Arbiter loop are unchanged.

### 5. Prompt

`planner::build_prompt` replaces "Return exactly one valid JSON object … no markdown
or commentary" with: call the `submit_plan` tool exactly once with the PLAN or BLOCKER
object; the tool returns validation errors — fix and call again; do not print the plan
as text. Shape definitions stay in the prompt as well as the tool schema.

## Security

No new capability reaches the planner process beyond one tool. The planner
capability is honored by `SubmitPlan` only; every GitHub and lifecycle operation
rejects it. Rejected-call and byte bounds prevent endpoint hammering.

## Testing

- Endpoint unit tests: accept, reject (each validator), once-only, rejection budget,
  wrong role (reviewer/worker token → `unauthorized`), planner token denied on every
  other operation.
- MCP tool test (`quorum/tests/agent_mcp.rs` pattern): `tools/list` exposes
  `submit_plan`; call proxies and returns endpoint errors verbatim.
- Real-process planner tests (`planner.rs` fake Codex/Claude harness): fake provider
  calls the endpoint via `quorum agent-mcp` and exits → `Done`; fake provider prints a
  perfect JSON object and exits without calling → `ProviderFailed`.
- Provider argument contract tests for both `spawn_planner` variants.
- Migration test: forward from current `user_version`, idempotent.

## Out of scope

Arbiter verdict tool; worker/reviewer `submit`/`react`/notes over MCP; keeping a
planner session alive across Arbiter rounds; removing `QUORUM_RUN_ID` from worker
shells.
