# Planner `submit_plan` over MCP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The decomposition planner reports its plan or blocker through an MCP tool that writes to the daemon endpoint; planner text output is never parsed.

**Architecture:** A new `planner_submissions` table stores one accepted response per planner run. `agent_endpoint.rs` gains `Operation::SubmitPlan`, authorized by a dedicated `resolve_planner_context` resolver over `run_capabilities` (`role='planner'`). The MCP shell exposes `submit_plan`; both provider `spawn_planner` paths attach the MCP server; the coordinator reads the table on planner exit instead of parsing text.

**Tech Stack:** Rust, rusqlite (bundled, WAL), tokio, rmcp MCP shell (`quorum agent-mcp`), real-process fake-provider tests.

**Spec:** `docs/superpowers/specs/2026-08-23-planner-submit-plan-mcp-design.md`

## Global Constraints

- Every PR targets `main`; worktree from `origin/main` (`git worktree add -b <branch> ~/dev/quorum-wt/<branch> origin/main`). Never edit `~/dev/quorum`.
- Run `rtk proxy ./preflight.sh` before every push. Never bypass the pre-push hook.
- Conventional commit subjects; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Invariants: claims/writes inside `BEGIN IMMEDIATE`; guarded `UPDATE … RETURNING`; migrations forward-only and idempotent; no filesystem/network work while holding a write transaction; bound every retry and row.
- Tests assert state, exit code, or JSON — never only a log line.
- `MAX_RESPONSE_BYTES` (existing, 64 KiB) and `MAX_PLAN_SUBMIT_REJECTIONS = 5` are the only bounds introduced/used.
- Each task below is one PR. Do not merge tasks into one PR.

---

### Task 1 (S): Core storage — `planner_submissions` migration and module

**Files:**
- Modify: `quorum-core/src/db.rs` (`SCHEMA_VERSION` 58 → 59; add migration step following the existing per-version pattern near the v58 step)
- Modify: `quorum-core/src/schema.sql` (add table so fresh DBs match)
- Create: `quorum-core/src/planner_submissions.rs`
- Modify: `quorum-core/src/lib.rs` (`pub mod planner_submissions;`)
- Test: unit tests inside `planner_submissions.rs`; migration test in `db.rs` tests module

**Interfaces:**
- Produces:
  ```rust
  pub const MAX_PLAN_SUBMIT_REJECTIONS: i64 = 5;
  pub enum SubmitOutcome { Accepted, AlreadySubmitted, BudgetExhausted }
  /// Inside caller's BEGIN IMMEDIATE tx. Creates the row if missing.
  pub fn record_accepted(tx: &Transaction, run_id: &str, graph_id: i64, response_json: &str, now: i64) -> Result<SubmitOutcome>;
  /// Inside caller's tx. Creates the row if missing; increments rejections. Returns new count.
  pub fn record_rejection(tx: &Transaction, run_id: &str, graph_id: i64) -> Result<i64>;
  /// Read-only. `true` when rejections >= MAX_PLAN_SUBMIT_REJECTIONS.
  pub fn budget_exhausted(conn: &Connection, run_id: &str) -> Result<bool>;
  /// Read-only. Accepted response JSON for the run, if any.
  pub fn accepted_response(conn: &Connection, run_id: &str) -> Result<Option<String>>;
  ```

- [ ] **Step 1: Write failing migration test** in `quorum-core/src/db.rs` tests: open a temp DB at v58 (use the existing helper that builds a prior-version DB; grep `user_version=` in the tests module for the pattern), run `migrate`, assert `PRAGMA user_version == 59` and `SELECT name FROM sqlite_master WHERE name='planner_submissions'` returns one row; run `migrate` again and assert no error (idempotent).
- [ ] **Step 2: Run** `cargo test -p quorum-core db::` — expect FAIL (table missing / version mismatch).
- [ ] **Step 3: Implement migration.** `SCHEMA_VERSION = 59`. Migration SQL:
  ```sql
  CREATE TABLE IF NOT EXISTS planner_submissions (
    run_id TEXT PRIMARY KEY,
    graph_id INTEGER NOT NULL,
    response_json TEXT,
    rejections INTEGER NOT NULL DEFAULT 0,
    accepted_at INTEGER
  );
  ```
  Add the same DDL to `schema.sql`. This is additive (no table rebuild) so no `foreign_keys` toggling is needed.
- [ ] **Step 4: Run** `cargo test -p quorum-core db::` — expect PASS.
- [ ] **Step 5: Write failing module tests** in `planner_submissions.rs`:
  - `accepts_once_then_reports_already_submitted`: `record_accepted` → `Accepted`; second call with different JSON → `AlreadySubmitted`; `accepted_response` returns the first JSON.
  - `rejections_count_and_exhaust_budget`: 4× `record_rejection` → `budget_exhausted == false`; 5th → `true`; `record_accepted` after exhaustion → `BudgetExhausted` and `accepted_response == None`.
  - `accepted_response_is_none_for_unknown_run`.
- [ ] **Step 6: Run** `cargo test -p quorum-core planner_submissions::` — expect FAIL (module missing).
- [ ] **Step 7: Implement module.** `record_accepted`: `INSERT OR IGNORE INTO planner_submissions(run_id,graph_id) VALUES(?1,?2)`; if `rejections >= MAX` return `BudgetExhausted`; then `UPDATE planner_submissions SET response_json=?1, accepted_at=?2 WHERE run_id=?3 AND response_json IS NULL RETURNING run_id` — one row → `Accepted`, zero → `AlreadySubmitted`. `record_rejection`: `INSERT OR IGNORE` then `UPDATE … SET rejections=rejections+1 WHERE run_id=?1 RETURNING rejections`.
- [ ] **Step 8: Run** `cargo test -p quorum-core` — expect PASS.
- [ ] **Step 9: Commit** `feat(core): add planner_submissions storage`. Run `rtk proxy ./preflight.sh`, push, `gh pr create --base main`.

---

### Task 2 (S): Planner capability resolver

**Files:**
- Modify: `quorum-core/src/capabilities.rs` (add after `resolve_live_run_context`)
- Test: `capabilities.rs` tests module (follow the fixtures at `capabilities.rs:600-660` which insert `run_capabilities`/`tasks` rows)

**Interfaces:**
- Consumes: `run_capabilities` columns `(run_id, task_id, agent, role, created_at, revoked_at)`; `task_decompositions` columns `(id, source_task_id, freeze_active, active, planner_session_id, planned_source_revision)`; `decomposition::set_frozen_phase` (`quorum-core/src/decomposition.rs:~1100`) is what stores `planner_session_id`.
- Produces:
  ```rust
  pub struct PlannerRunContext { pub run_id: String, pub task_id: i64, pub agent: String, pub graph_id: i64, pub source_revision: i64 }
  pub fn resolve_planner_context(conn: &Connection, run_id: &str) -> Result<PlannerRunContext>;
  ```
  Errors use the existing `authority_error(...)` helper with messages: `"unknown capability"`, `"capability is revoked"`, `"operation role does not match capability"`, `"planner run is not the live planner for its graph"`.

- [ ] **Step 1: Write failing tests:**
  - `planner_context_resolves_live_planner`: insert capability role `planner`, graph row `freeze_active=1, active=0, planner_session_id=<run_id>, planned_source_revision=2` → Ok with matching `graph_id`, `source_revision`.
  - `planner_context_rejects_worker_role`: capability role `worker` → Err containing `"operation role does not match capability"`.
  - `planner_context_rejects_stale_session`: graph `planner_session_id='other'` → Err containing `"not the live planner"`.
  - `planner_context_rejects_revoked`: `revoked_at` set → Err containing `"revoked"`.
  - `live_run_context_rejects_planner_role`: `resolve_live_run_context(conn, planner_run, "worker")` → Err (role mismatch) — pins that other operations keep rejecting planner tokens.
- [ ] **Step 2: Run** `cargo test -p quorum-core capabilities::planner_context` — expect FAIL.
- [ ] **Step 3: Implement** with one `SELECT … FROM run_capabilities WHERE run_id=?1` then one `SELECT id, planned_source_revision FROM task_decompositions WHERE source_task_id=?1 AND freeze_active=1 AND active=0 AND planner_session_id=?2`.
- [ ] **Step 4: Run** tests — expect PASS.
- [ ] **Step 5: Commit** `feat(core): resolve planner run capability`. Preflight, push, PR to `main`.

---

### Task 3 (M): Endpoint `SubmitPlan` operation

**Files:**
- Modify: `quorum/src/serve/agent_endpoint.rs` (`enum Operation` at `:50`; handler `match` near `:524`; tests module which already issues capabilities at `:812`)
- Modify: `quorum/src/serve/planner.rs` — make `validate_semantics` `pub(crate)` (currently private at `:386`); `validate_for_source` is already `pub`. No behavior change to these functions.
- Modify: `quorum/src/agent_client.rs` (add `submit_plan` client request, mirroring `submit` at `:29-131`)
- Test: `agent_endpoint.rs` tests; `quorum/tests/agent_endpoint.rs` integration (pattern at `:378-431`)

**Interfaces:**
- Consumes: Task 1 `planner_submissions::{record_accepted, record_rejection, budget_exhausted, SubmitOutcome}`; Task 2 `capabilities::resolve_planner_context`; `planner::{PlannerResponse, validate_semantics, validate_for_source, WritablePathResolver, MAX_RESPONSE_BYTES}`.
- Produces:
  - Request: `{"capability": "<run_id>", "operation": {"kind": "submit_plan", "response": {...}}}` (match the existing serde tagging used by `Operation`).
  - Success result: `ResponseResult::PlanAccepted { graph_id: i64 }`.
  - Error codes (strings in `ResponseError.code`): `invalid_plan` (message = validator text), `already_submitted`, `submit_budget_exhausted`, `unauthorized` (from resolver).
  - `agent_client::submit_plan(endpoint: &Path, capability: &str, response: &serde_json::Value) -> Result<SubmitPlanResult, ClientError>`.

- [ ] **Step 1: Write failing endpoint tests** (each spins the endpoint against a temp DB exactly as the existing `Submit` tests do, and seeds a planner capability + planning graph + a minimal `tasks` row with a repo worktree path):
  - `submit_plan_accepts_valid_plan_once`: valid plan → `PlanAccepted{graph_id}`; `planner_submissions.response_json` equals the submitted JSON; second valid call → error `already_submitted`.
  - `submit_plan_returns_validator_message_and_counts_rejection`: plan with 1 task (below the 2-task minimum in `validate_plan_tasks`) → error `invalid_plan`, message contains the validator's text; `rejections == 1`; `response_json IS NULL`.
  - `submit_plan_exhausts_rejection_budget`: 5 invalid calls → 5th still `invalid_plan`; 6th → `submit_budget_exhausted` even with a valid plan.
  - `submit_plan_rejects_oversized_payload`: `response` serialized > 64 KiB → `invalid_plan`, rejection counted.
  - `submit_plan_rejects_worker_capability`: worker token → `unauthorized`.
  - `planner_capability_rejected_on_other_operations`: planner token on `Submit`, `React`, `AppendNote`, `Protocol{PullRequestRead}` → `unauthorized`.
- [ ] **Step 2: Run** `cargo test -p quorum agent_endpoint::submit_plan` — expect FAIL (variant missing).
- [ ] **Step 3: Implement handler** in this order, **outside** any write transaction: (a) `resolve_planner_context` on a short read connection; (b) `budget_exhausted` → error; (c) serialize `response` and check `MAX_RESPONSE_BYTES`; (d) `serde_json::from_value::<PlannerResponse>`; (e) `validate_semantics`; (f) for `PlannerResponse::Plan`, load source dependency ids (same query the coordinator uses before calling `validate_for_source` at `serve/mod.rs:~6656`) and the repo root, then `validate_for_source(..).await` with `WritablePathResolver::default()`. Any failure in (c)–(f): open `BEGIN IMMEDIATE`, `record_rejection`, commit, return `invalid_plan`. Success: `BEGIN IMMEDIATE`, re-run `resolve_planner_context` on the tx (authority may have changed), `record_accepted` → map `Accepted`→`PlanAccepted`, `AlreadySubmitted`/`BudgetExhausted`→ their codes; commit.
- [ ] **Step 4: Run** tests — expect PASS.
- [ ] **Step 5: Add `agent_client::submit_plan`** and one round-trip integration test in `quorum/tests/agent_endpoint.rs` using the real socket.
- [ ] **Step 6: Run** `cargo test -p quorum agent_endpoint` — expect PASS.
- [ ] **Step 7: Commit** `feat(endpoint): accept planner submissions via submit_plan`. Preflight, push, PR to `main`.

---

### Task 4 (S): MCP tool `submit_plan`

**Files:**
- Modify: `quorum/src/agent_mcp.rs` (tool table `:52-219`; `call_tool` `:535`)
- Test: `quorum/tests/agent_mcp.rs` (pattern `:415-542`)

**Interfaces:**
- Consumes: Task 3 `agent_client::submit_plan`, error codes.
- Produces: MCP tool `submit_plan`, input schema
  ```json
  {"type":"object","required":["response"],"properties":{"response":{"type":"object","description":"Exactly the PLAN or BLOCKER object: {\"outcome\":\"plan\",\"tasks\":[...]} or {\"outcome\":\"blocker\",...}. Unknown fields are rejected."}}}
  ```
  Tool result on success: text `{"accepted":true,"graph_id":N}`; on endpoint error: `is_error=true`, text `{"code":"invalid_plan","message":"..."}` (verbatim endpoint code/message). Tool description text must include the same PLAN/BLOCKER shape sentences as `planner::build_prompt` (copy them; do not paraphrase).

- [ ] **Step 1: Write failing tests:** `tools_list_exposes_submit_plan` (name present, schema has `response`); `submit_plan_proxies_and_returns_endpoint_error_verbatim` (fake endpoint returns `invalid_plan` → tool result `is_error` with the same code/message); `submit_plan_success_returns_graph_id`.
- [ ] **Step 2: Run** `cargo test -p quorum --test agent_mcp submit_plan` — expect FAIL.
- [ ] **Step 3: Implement** as a new tool entry; keep the existing `ProtocolOperation` tools untouched.
- [ ] **Step 4: Run** tests — expect PASS.
- [ ] **Step 5: Commit** `feat(mcp): expose submit_plan tool`. Preflight, push, PR to `main`.

---

### Task 5 (M): Attach the MCP server to planner spawns

**Files:**
- Modify: `quorum/src/serve/agent.rs` (`spawn_planner` `:339`; `spawn_configured` allowlist `:391-407`; `RestrictedMode::Planner` `:375`)
- Modify: `quorum/src/serve/codex_agent.rs` (`spawn_planner` `:323`, `planner_exec_args` `:~146`, `append_agent_mcp_override` `:93`)
- Modify: `quorum/src/serve/planner.rs` (`spawn_planner` `:709` / `spawn_planner_with_timeout` `:732` gain `run: Option<PlannerRunEnvelope>`)
- Modify: `quorum/src/serve/runner.rs` (add `PLANNER_MCP_ALLOWED_TOOL`)
- Test: existing arg-contract tests in `agent.rs`/`codex_agent.rs` tests modules; `quorum/tests/cli_serve_provider_lifecycle.rs:85-120`

**Interfaces:**
- Consumes: `runner::{AgentMcpServer, AGENT_MCP_SERVER, AGENT_MCP_ENV_VARS}`.
- Produces:
  ```rust
  pub struct PlannerRunEnvelope { pub run_id: String, pub agent: String, pub repo: PathBuf, pub endpoint: PathBuf }
  // planner.rs
  pub async fn spawn_planner(provider, model, effort, repo, prompt, bare, provider_bin, envelope: Option<&PlannerRunEnvelope>) -> io::Result<PlannerSlot>
  // agent.rs / codex_agent.rs
  pub fn spawn_planner(spec: &AgentSpec, agent_bin: Option<&str>, agent_mcp: Option<AgentMcpServer>) -> io::Result<Self>
  ```
  When `envelope` is `Some`, the four env vars are set on the planner process and `AGENT_MCP_SERVER` is passed. Claude allowlist for `RestrictedMode::Planner` with MCP = `"mcp__quorum__submit_plan"` only (server is named `quorum` in `claude_mcp_config`; do not add `mcp__quorum__*`). `--tools Read,Glob,Grep` unchanged. Codex: `-s read-only` unchanged; MCP override appended.

- [ ] **Step 1: Write failing arg tests:** `claude_planner_with_mcp_allows_only_submit_plan` (args contain `--mcp-config`, `--strict-mcp-config`, `--allowedTools mcp__quorum__submit_plan`, `--tools Read,Glob,Grep`, and NOT `mcp__quorum__*`); `claude_planner_without_envelope_has_no_mcp`; `codex_planner_with_mcp_keeps_read_only_sandbox` (args contain `-s read-only` and the `mcp_servers.quorum=` override).
- [ ] **Step 2: Run** the provider tests — expect FAIL.
- [ ] **Step 3: Implement** threading `agent_mcp` through both `spawn_planner` paths and env injection in `planner::spawn_planner_with_timeout`.
- [ ] **Step 4: Run** tests — expect PASS.
- [ ] **Step 5: Real-binary sandbox check (Codex).** Add an `#[ignore]`-by-default real-binary test (pattern: existing real-binary tests in `codex_agent.rs`) that launches `codex exec -s read-only` with the MCP override pointing at a fake `quorum agent-mcp` which connects to a temp Unix socket, and asserts the connection succeeds. Run it once locally with `rtk proxy cargo test -p quorum codex_planner_sandbox_allows_mcp_socket -- --ignored --nocapture` and paste the result in the PR body. **If the sandbox blocks the socket, stop, record the finding in the PR, and do not loosen the sandbox.**
- [ ] **Step 6: Commit** `feat(serve): attach submit_plan MCP server to planner spawns`. Preflight, push, PR to `main`.

---

### Task 6 (M): Cutover — coordinator reads submissions, text parsing removed, prompt updated

**Files:**
- Modify: `quorum/src/serve/mod.rs` (planner spawn site `:~6594`: mint `run_id = agent::new_session_id()`, issue capability `capabilities::issue(.., role="planner")` inside the same tx that calls `set_frozen_phase` with that session id, build `PlannerRunEnvelope`; planner poll site `:~6147-6199`)
- Modify: `quorum/src/serve/planner.rs` (`PlannerSlot::poll`: drop `response_text` harvest and `parse_response`; `build_prompt` `:146-191`; delete `parse_response` and its tests `:1382-1427`, Claude/Codex terminal-text selection tests `:1727-1911`)
- Test: `planner.rs` fake-provider harness (`spawn_fake_codex` `:1147`, `spawn_fake_claude` `:1169`); `serve/mod.rs` planner tests at `:34425, :34788, :37184`; `quorum-core/tests/decomposition_process.rs`

**Interfaces:**
- Consumes: Task 1 `planner_submissions::accepted_response`; Task 5 `PlannerRunEnvelope`; `capabilities::issue`.
- Produces: `PlannerPoll::Done(PlannerResponse)` is constructed from `accepted_response(run_id)`; `PlannerPoll::ProviderFailed("planner exited without submit_plan")` when absent. `PlannerPoll::SemanticRejected` is no longer produced by the poll.

- [ ] **Step 1: Write failing real-process tests** in `planner.rs`:
  - `fake_provider_submitting_via_endpoint_yields_done`: fake Codex/Claude script that runs `quorum agent-mcp`-equivalent — simplest is a script invoking the test binary's `agent_client::submit_plan` helper via a tiny `quorum`-compatible subcommand already used by endpoint tests (reuse whatever the `agent_endpoint` integration tests use to drive the socket) — then exits 0; assert `PlannerPoll::Done` with the submitted plan.
  - `fake_provider_printing_perfect_json_without_submit_is_provider_failed`: stdout is exactly `{"outcome":"plan",...}` and exit 0 → `ProviderFailed` whose message contains `without submit_plan`.
  - `prompt_instructs_submit_plan_tool_and_forbids_text_plan`: `build_prompt` contains `submit_plan` and does not contain `Return exactly one valid JSON object`.
- [ ] **Step 2: Run** `cargo test -p quorum planner::` — expect FAIL.
- [ ] **Step 3: Implement coordinator wiring** (capability issue + envelope at spawn; read `accepted_response` at exit), **delete** `parse_response`/text harvest/`response_text` field and their tests, **rewrite** prompt tail per spec §5. Update `serve/mod.rs` planner tests that previously fed JSON text to the fake provider to use the endpoint path instead.
- [ ] **Step 4: Run** `cargo test -p quorum` and `cargo test -p quorum-core` — expect PASS; `grep -rn "parse_response" quorum/src/serve/planner.rs` returns nothing.
- [ ] **Step 5: Design-spec touch:** add one paragraph to the planner section of `docs/2026-06-23-quorum-design.md` stating the planner reports via `submit_plan` and text is never parsed (grep `exactly one JSON object` in that doc and replace).
- [ ] **Step 6: Commit** `feat(serve): planner reports plans through submit_plan`. Preflight, push, PR to `main`.

---

## Review gate per PR

Each PR gets a fresh reviewer subagent (not the author) that: reads the spec + this task, runs `gh pr diff`, checks the task's test list is present and asserts state, confirms no text-parsing fallback was left behind (Task 6), and classifies findings BLOCKING/advisory. Zero blockers → approve; otherwise request changes with file:line feedback and hand back to the author subagent.
