# GitHub Collaboration MCP — Technical Specification

**Date:** 2026-08-17
**Status:** Proposed
**Product source:** `quorum-pml` GitHub coordination and Proposed Change delivery behaviors
**Implementation base:** `origin/develop` at `11047a4e`

## Problem

Managed workers and reviewers currently use ordinary `gh` commands to read and write the
authoritative pull-request conversation. The daemon reserves publication, formal review, and
merge authority, but direct agent GitHub access is still an instruction-level boundary. Review
content also crosses shell and CLI argument boundaries, which makes multiline Markdown, inline
anchors, and threaded replies unnecessarily fragile.

The initial PR body is only `Daemon-owned delivery for task #N.`. The accepted task body,
delivered-change summary, and verification evidence are absent, while reviewer summaries and
rework responses depend on each agent independently discovering the correct GitHub command and
format.

Quorum needs a credentialless managed-agent MCP surface that feels like GitHub's established MCP
and API vocabulary while deriving authority from the current daemon-issued run. Agents continue
to author the conversation; Quorum validates and publishes it without exposing GitHub
credentials or allowing comment text to transition lifecycle.

## Goals

- Preserve ordinary local `git` work for status, diff, commits, and history.
- Give managed agents familiar GitHub-shaped MCP tools for PR reads, general comments, pending
  reviews, inline comments, review-thread replies, and reviewer-owned thread resolution.
- Bind every operation to the live run's repository, task, role, PR, and applicable revision.
- Preserve authored GitHub-flavored Markdown structure without shell quoting or argument
  interpolation.
- Keep GitHub credentials, formal APPROVE/REQUEST_CHANGES, publication, and merge outside managed
  agent processes.
- Make remote operations bounded, idempotent, restart-safe, and distinguishable from lifecycle
  rework during a GitHub outage.
- Render a useful initial PR description from accepted task context and a bounded delivery report.
- Keep the public Quorum contract provider-neutral and hosting-unaware.

## Non-goals

- The outside/task-creator MCP, remote user authentication, or distributed task submission.
- A general GitHub REST/GraphQL proxy or arbitrary repository selector.
- Generic issue creation, editing, closing, labels, assignments, milestones, projects, releases,
  Actions control, or repository administration.
- Replacing local `git` with MCP tools.
- Treating GitHub comment or review prose as a lifecycle signal.
- Changing R1/R2 selection, exact-SHA attestation, rework limits, formal review, CI, or merge
  policy.
- Resolving Hosted's final GitHub actor/ruleset design. The one-App author/self-approval question
  remains a separate permission and branch-policy audit.
- Claiming physical credential isolation for an unrestricted local run sharing the operator's OS
  identity and configuration.

## Deployment profiles

The managed-agent protocol is identical in both profiles; only containment differs.

### Local permissive

The daemon injects and instructs the MCP surface. Existing operator `git`/`gh` configuration may
still be reachable because the managed process shares the operator's machine. Quorum does not use
that reachability as authority and does not claim it is a security boundary.

### Isolated runtime

The agent runtime contains the worktree, local Git tooling, the selected coding runner, and the
credentialless Quorum MCP adapter. It receives no GitHub token, GitHub CLI configuration, SSH
agent, credential helper, authenticated clone URL, or daemon-side repository credential.

The runner receives one narrow local Quorum agent endpoint plus its run capability. Managed
`quorum submit` and `quorum react` use that same endpoint without becoming MCP tools, so the
container does not need the repository database merely to signal its existing lifecycle state. The
credentialed Repository Service and Quorum database remain across a process/container boundary
inaccessible to the managed runner. If the daemon and runner share an OS boundary from which the
runner can read daemon environment, files, database, or credentialed sockets, the deployment MUST
NOT claim physical GitHub credential isolation. Hosted supplies the credentialed adapter or broker
without making public Quorum aware of accounts, sessions, KMS, installation identity, or
control-plane state.

## Authority model

`QUORUM_RUN_ID` already names a daemon-issued row in `run_capabilities`. The MCP adapter reuses
that capability. It is not GitHub authority: compromise grants only the same bounded operations
already available to that exact managed run and stops working when the daemon revokes the run.

The MCP adapter reads `QUORUM_REPO`, `QUORUM_AGENT`, `QUORUM_RUN_ID`, and
`QUORUM_AGENT_ENDPOINT` from its inherited environment. The daemon derives task and role from
`run_capabilities`; it never accepts those values from tool arguments. Familiar `owner`, `repo`,
and PR-number arguments remain in GitHub-shaped schemas, but they are assertions that MUST match
the derived target.

Reviewer operations additionally require the immutable `agent_runs.review_pr` and
`agent_runs.review_head_sha` launch binding. Rework-worker operations require the task's
daemon-owned PR association and current worker-phase capability. Initial workers have no PR tools
until a Proposed Change exists.

The adapter submits closed typed requests to a daemon-owned local agent endpoint; it does not open
the Quorum database. Capability validation and durable request creation occur in one
`BEGIN IMMEDIATE` transaction in the daemon.
Revoked, ended, wrong-role, wrong-task, wrong-PR, or wrong-revision calls are clean authorization
failures and enqueue nothing. GitHub calls never occur while a database transaction is open.

The initial endpoint is a local framed-JSON IPC channel, implemented as an owner-only Unix-domain
socket on supported runtimes. It does not listen on TCP. Each bounded request carries the run
capability and one closed operation; responses use closed bounded schemas. The daemon exposes
only existing `submit`/`react` signals and the role-scoped MCP operations through this channel—no
SQL, filesystem path, arbitrary CLI, or raw GitHub request. A container runtime may project this
single socket into the runner without projecting the database or Repository Service socket.

## MCP process and provider injection

`quorum agent-mcp` is a short-lived tools-only MCP server over stdio. It uses the official Rust
MCP SDK's stdio server transport at a Cargo.lock-pinned version and forwards typed operations to
the daemon through the injected local agent endpoint. The endpoint is not a Repository Service
or arbitrary database channel. Stdout is protocol-only; bounded diagnostics go to stderr and
never include request bodies, responses, or capabilities.

The logical MCP server name is `github`, yielding familiar client-visible names such as
`mcp__github__pull_request_read`. The daemon injects only this per-run server into ordinary worker
and reviewer launches:

- Claude receives an explicit generated `--mcp-config` plus `--strict-mcp-config`; this remains
  explicit when `--bare` is active.
- Codex receives an invocation-local MCP configuration override. It MUST NOT mutate the operator's
  persistent Codex configuration and MUST preserve the provider-issued thread ID on resume.
- Restricted classifiers, planners, collectors, and doctors receive no GitHub MCP.

Provider CLI syntax is covered by real installed-binary argument tests. Fake runners are
insufficient because malformed MCP configuration fails before their protocol begins.

## Role-scoped tool inventory

The server advertises only tools valid for the derived live run. An agent cannot discover a
forbidden mutation and cannot enable a larger toolset through arguments or environment.

| Tool | Reviewer | Rework worker | Initial worker |
|---|---:|---:|---:|
| `pull_request_read` | yes | yes | no |
| `add_issue_comment` | yes | yes | no |
| `pull_request_review_write` | yes | no | no |
| `add_comment_to_pending_review` | yes | no | no |
| `add_reply_to_pull_request_comment` | yes | yes | no |
| `resolve_review_thread` | yes | no | no |
| `github_operation_read` | yes | yes | no |
| `delivery_report_write` | no | yes | yes |

The GitHub-shaped names and core parameters follow the official GitHub MCP server where their
semantics fit. Quorum descriptions state the narrowed authority and expected workflow.

### `pull_request_read`

Accepted methods are `get`, `get_diff`, `get_files`, `get_review_comments`, `get_reviews`, and
`get_comments`. CI/status/check methods remain daemon-owned and are not advertised to reviewers.
Pagination inputs are bounded. Review-comment results include numeric comment ID, thread node ID,
path, revision, diff side, line/range, nearby code, resolved state, and outdated state when
GitHub provides them.

### `add_issue_comment`

GitHub models a general PR comment through the issue-comment endpoint. The supplied issue number
must equal the bound PR. `body` is authored GitHub-flavored Markdown. This tool cannot target a
standalone issue in the first increment.

### `pull_request_review_write`

Only `method=create` and `method=submit_pending` are accepted.

- `create` requires `commitID` equal to the immutable reviewer launch SHA and creates or resumes
  the exact run's pending review.
- `submit_pending` requires `event=COMMENT`. `APPROVE`, `REQUEST_CHANGES`, dismissal, deletion,
  and arbitrary review IDs are rejected.
- The submitted `body` is the authored complete-review summary. Publication does not transition
  the Managed Task; the existing explicit `quorum submit` verdict remains separate.

At most one live pending review belongs to one reviewer run. A restarted adapter resumes the
durable request/review identity rather than creating a second review.

### `add_comment_to_pending_review`

The reviewer supplies `path`, `line`, `side`, optional `startLine`/`startSide`, `subjectType`, and
Markdown `body`. Quorum verifies that the anchor exists on the immutable launch SHA before the
daemon publishes it. An invalid or outdated anchor is rejected rather than downgraded silently
to a general comment.

### `add_reply_to_pull_request_comment`

The numeric `commentId` must resolve to a review comment on the bound PR. The daemon posts the
reply through GitHub's reply endpoint so it remains in that thread. Rework workers may respond to
findings; reviewers may resolve, reaffirm, or discuss them. A missing, foreign, or general issue
comment ID is rejected.

### `resolve_review_thread`

Only the current reviewer may resolve a thread returned by `pull_request_read`. The thread must
belong to the bound PR. Workers cannot resolve findings against themselves. Unresolve is deferred
until a concrete workflow needs it.

### `delivery_report_write`

This Quorum-owned tool records bounded structured delivery evidence before completion:

```json
{
  "summary": "Concise delivered outcome in GitHub-flavored Markdown",
  "changes": ["Bounded concrete change"],
  "verification": [
    {"command": "cargo test -p quorum-core", "outcome": "passed"}
  ],
  "risks_or_notes": ["Optional bounded note"]
}
```

The report is run/task scoped, last-write-wins only for the same live worker run, and immutable
after the completion signal is accepted. It is evidence supplied by the worker, not proof that a
command ran. Missing reports remain valid and render explicitly as not reported.

## Markdown fidelity and presentation

All authored bodies enter as JSON strings. Quorum validates UTF-8 by construction, rejects NUL,
applies fixed per-body and aggregate byte/scalar limits, and sends bodies as JSON API input or an
equivalent non-shell body stream. It MUST NOT interpolate authored text into shell syntax or a
command argument.

Quorum preserves line breaks, blank lines, indentation, lists, tables, block quotes, links,
inline code, fenced code, and suggestion fences. It may append a blank-line-separated hidden
idempotency marker and may prepend a stable visible attribution block to a review summary. Those
additions must not enter or split authored Markdown constructs.

Reviewer prompts keep the current complete-review and cumulative-disposition contracts. They now
teach the pending-review sequence:

1. create a pending review for the launch SHA;
2. add every line-specific finding inline;
3. submit that review with one complete Markdown summary using `event=COMMENT`; and
4. only after publication succeeds, submit the separate matching Quorum verdict.

Rework prompts require replies on the applicable inline threads for fixed, rebutted, accepted, or
still-open blockers. Cross-cutting responses may use a general PR comment.

## Durable GitHub-operation outbox

A forward-only migration adds a bounded repository-local outbox. Names below are normative; exact
column order is not.

```sql
CREATE TABLE github_agent_operations (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id       TEXT NOT NULL UNIQUE,
  client_request_id  TEXT NOT NULL,
  run_id             TEXT NOT NULL REFERENCES run_capabilities(run_id),
  task_id            INTEGER NOT NULL REFERENCES tasks(id),
  agent               TEXT NOT NULL,
  role                TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
  pr_number           INTEGER NOT NULL,
  head_sha            TEXT,
  kind                TEXT NOT NULL,
  request_json        TEXT NOT NULL,
  state               TEXT NOT NULL,
  attempts            INTEGER NOT NULL DEFAULT 0,
  next_attempt_at     INTEGER,
  github_marker       TEXT,
  response_json       TEXT,
  error_kind          TEXT,
  error_summary       TEXT,
  created_at          INTEGER NOT NULL,
  updated_at          INTEGER NOT NULL,
  expires_at          INTEGER NOT NULL,
  UNIQUE(run_id, client_request_id)
);
```

States are `queued`, `running`, `succeeded`, and `failed`. Requests are immutable after enqueue;
only execution state/result fields change. `request_json` and `response_json` use closed,
kind-specific schemas and fixed aggregate limits. Terminal rows expire after a bounded retention
window; live rows do not become logically dead while queued or running.

Every mutation accepts an optional `clientRequestId`, matching the role of an idempotency key. If
it is absent, the adapter derives it from the run, operation kind, target, and canonical closed-
schema request. Identical retries in one run therefore return the original durable `operation_id`
without relying on agent discipline; a caller supplies a distinct ID only when intentionally
repeating identical content. Every write body receives a hidden operation marker where GitHub
permits one. After a crash in the ambiguous post-request/pre-commit window, the daemon queries the
exact PR/review/thread for that marker before retrying.

The single daemon claims one queued operation with a guarded `UPDATE ... RETURNING` inside
`BEGIN IMMEDIATE`, commits, performs the network call, then records the bounded result in a new
transaction. Startup converts orphaned `running` rows back to reconciliation-required queued
work. Network calls, sleeps, and response parsing never hold a DB transaction.

`github_operation_read` returns `queued`, `running`, `succeeded`, or `failed` for an operation
belonging to the caller's run. A tool call waits at most the existing 30-second publication bound;
if unfinished it returns the operation ID and a pending result rather than blocking the agent or
creating a second request.

## Repository Service execution

All operation kinds call one daemon-owned `RepositoryService` interface. The initial self-hosted
adapter may use the installed `gh` binary and its existing daemon-side authentication, but bodies
must cross stdin/JSON rather than argv. Command execution reuses the existing 30-second timeout,
kill/reap behavior, and 1 MiB stdout/stderr caps.

The interface is provider-neutral at Quorum's boundary. A Hosted adapter may send the same typed
operation to a trusted credential broker that mints exact-repository, minimum-permission,
short-lived GitHub App installation authority. The public runtime never receives Hosted user,
installation, KMS, or session identity.

Formal approval, request-changes, branch publication, cleanup, checks, and merge may share the
Repository Service implementation later, but agent tools never expose those methods. The first
increment must not combine this refactor with a semantic merge-flow rewrite.

## Failure and outage behavior

GitHub transport failures, timeouts, 5xx responses, secondary rate limiting, and unavailable API
components are infrastructure outcomes. They MUST NOT:

- create an `errors` row for an expected retryable outcome;
- submit a review verdict;
- produce `VerdictChanges`, `ChecksFailed`, or `AgentFailed`;
- consume rework or reviewer-provision budgets; or
- duplicate a comment, inline finding, reply, or submitted review.

Retryable operations remain queued with bounded exponential backoff and jitter. Permanent closed-
schema input errors fail the operation and return actionable tool feedback. Authorization loss,
repository deselection, credential failure, or an invalid current PR target fails closed and is
surfaced as an infrastructure/authority condition for operator recovery; it is never rewritten as
code rework.

A reviewer lifecycle submit is atomically rejected unless that run's pending review operation is
already `succeeded` for the immutable launch SHA. This is a core submit guard, not a prompt rule.
If the coding-runner turn ends while GitHub publication remains pending, the daemon retains the
operation for reconciliation and does not infer a verdict from the model's text.

## Initial PR description

After publishing the exact worker commit and before creating the initial PR, the daemon renders:

```markdown
## Outcome

<delivery-report summary, or explicit not reported>

## Changes

<bounded delivery-report changes, or explicit not reported>

## Verification

<command/outcome entries, or explicit not reported>

## Task

**Quorum task:** #<id> — <accepted title>

<accepted task body, or explicit no task body>

## Notes

<bounded risks/notes when present>
```

The accepted task title/body are copied from daemon-owned task state, not agent input. Rendering
is deterministic and escapes only text that would break Quorum-owned structure; authored Markdown
inside designated fields remains intact. Raw transcripts, progress notes, provider output, and
credentials never enter the body. Existing-PR continuation/review-only tasks do not overwrite an
externally authored body in the first increment.

## Security and negative evidence

Required tests include:

- a worker cannot advertise or invoke reviewer-only tools;
- a reviewer cannot request APPROVE, REQUEST_CHANGES, merge, PR creation, or a raw API call;
- run A cannot address task, PR, revision, operation, comment, or thread belonging to run B;
- the local agent endpoint rejects public/admin CLI commands, raw SQL, and unknown operations;
- revoked and ended capabilities enqueue nothing;
- moved heads reject new inline review work and cannot authorize a verdict;
- invalid path/line/side/range and foreign/outdated thread anchors fail visibly;
- authored Markdown with lists, tables, fences, suggestions, Unicode, quotes, and newlines reaches
  the fake GitHub boundary structurally unchanged;
- NUL, invalid schema, oversized bodies/results, excessive pages/comments, and unknown fields fail;
- crash-after-GitHub-success reconciliation produces exactly one visible contribution;
- concurrent duplicate requests produce exactly one outbox row and GitHub write;
- repeated real-process races against one DB preserve SQLite invariants;
- GitHub timeout/5xx leaves lifecycle and rework counters unchanged;
- normal Claude and Codex launches receive the scoped MCP, while restricted roles do not;
- real installed Claude/Codex binaries accept initial and continuation MCP launch arguments without
  making a provider API call; and
- isolated-runtime scans find no GitHub token in agent environment, args, config, worktree, Git
  configuration, MCP messages, logs, SQLite bodies, or crash output.

## Delivery slices

Every implementation task is deliberately small or medium and independently reviewable.

1. **M — Agent endpoint, MCP shell, and capability-derived inventory.** Add the narrow daemon
   endpoint, route managed `submit`/`react` without semantic changes, add the stdio server and
   closed schemas, derive role/target in the daemon, and prove the adapter has no direct database
   or GitHub execution access.
2. **M — Durable operation outbox and daemon executor skeleton.** Add the migration, atomic
   enqueue/claim/result/recovery paths, bounds, retention, and fake Repository Service.
3. **M — PR read and general-comment operations.** Add bounded reads, exact target checks,
   Markdown-safe general comments, markers, and outage classification.
4. **M — Pending reviews and inline findings.** Add create/resume/submit COMMENT review,
   validated single/multiline anchors for the launch SHA, and the core publication-before-verdict
   submit guard.
5. **S — Thread replies and reviewer resolution.** Add exact-PR comment lookup, true threaded
   replies, outdated context, and reviewer-only resolution.
6. **S — Claude per-run MCP injection.** Add explicit strict config for initial/resumed managed
   roles and real-binary boundary tests.
7. **M — Codex per-run MCP injection.** Add invocation-local config for initial/resumed threads
   and real-binary boundary tests.
8. **M — Managed prompt and credential-boundary migration.** Replace direct `gh` collaboration
   instructions, preserve local `git`, scrub inherited GitHub auth/config, and add negative tests.
9. **M — Delivery reports and initial PR renderer.** Persist bounded reports and render accepted
   task, changes, verification, and notes without changing existing-PR bodies.
10. **M — Isolated-runtime GitHub boundary.** Update the public Docker contract and smoke tests so
    agent runtime inputs contain no GitHub credential and remote writes require the daemon/broker.

Slices 1 and 2 are foundations. Slice 3 depends on both. Slice 4 depends on 3. Slice 5 depends on
4. Slices 6 and 7 depend on 1 and can proceed independently. Slice 8 depends on 3–7. Slice 9
depends on 1–3. Slice 10 depends on 2, 6, 7, and 8. No task may broaden itself into the outside MCP
or Hosted control-plane implementation.

## Rollout

The MCP is enabled for managed agents only when the daemon executor and applicable role toolset
are ready. There is no silent fallback to direct `gh` inside a managed prompt: a missing MCP is a
loud provider/configuration failure. Operators may continue using ordinary `gh` outside managed
runs.

During migration, formal daemon GitHub commands and the collector retain their existing paths.
After all managed collaboration prompts use MCP and negative tests pass, the design may remove the
legacy statement that agents own direct GitHub command execution. Human-visible PR history remains
the authoritative review conversation throughout.
