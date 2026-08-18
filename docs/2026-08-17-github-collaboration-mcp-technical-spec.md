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
- Bind every enqueue, read, and execution claim to the live run's repository, task, role, PR, and
  applicable revision.
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
already available to that exact managed run. Revocation immediately prevents that capability
from enqueueing, reading, or adopting work. A durable operation may survive only through the
daemon-owned exact-continuation handoff defined below; possession of an old capability never
performs that handoff.

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

Each new logical managed turn receives a daemon-generated `collaboration_attempt_id` bound to its
exact task, agent, role, PR, lifecycle generation, and reviewer launch SHA when applicable. The ID
is persisted with the daemon's pending-turn/recovery state; it is not accepted from an MCP
argument or inferred from the provider's prose. A process restart or provider retry that resumes
that exact pending turn retains the collaboration attempt even though it receives a fresh
`agent_runs` row and run capability. A new rework round, re-review launch, role, PR, launch SHA, or
provider turn receives a new attempt.

Only the daemon provisioning path may adopt an interrupted attempt. In the same transaction that
issues the replacement capability, it must prove the persisted pending-turn identity, task,
agent, role, PR, lifecycle generation, launch SHA, and provider continuation all still match,
that no other live capability owns the attempt, and that the task still permits the turn. It then
replaces the attempt's `active_run_id`. The old run remains revoked. A mismatch revokes the
attempt and cancels its unclaimed writes; it never falls back to a new attempt or copies rows.
This is the durable handoff for Codex thread continuation and daemon restart, not a general
cross-run lookup facility.

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
  the exact collaboration attempt's pending review.
- `submit_pending` requires `event=COMMENT`. `APPROVE`, `REQUEST_CHANGES`, dismissal, deletion,
  and arbitrary review IDs are rejected.
- The submitted `body` is the authored complete-review summary. Publication does not transition
  the Managed Task; the existing explicit `quorum submit` verdict remains separate.

GitHub's pending review is a slot for one publishing actor on one PR, not an independent draft per
Quorum attempt. Quorum grants that remote slot to at most one reviewer collaboration attempt. A
fresh-capability resume of the exact pending reviewer turn adopts the attempt's durable
request/review identity rather than creating a second review. A different launch SHA, re-review,
or R1/R2 role cannot adopt the draft; before it starts, the daemon must complete the owned-orphan
cleanup protocol below. Deletion remains unavailable to agents but is a narrow daemon
housekeeping operation.

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

A forward-only migration adds bounded repository-local collaboration attempts and an outbox.
Names below are normative; exact column order is not.

```sql
CREATE TABLE github_collaboration_attempts (
  attempt_id           TEXT PRIMARY KEY,
  task_id              INTEGER NOT NULL REFERENCES tasks(id),
  agent                 TEXT NOT NULL,
  role                  TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
  pr_number             INTEGER NOT NULL,
  head_sha              TEXT,
  lifecycle_generation  INTEGER NOT NULL,
  active_run_id         TEXT REFERENCES run_capabilities(run_id),
  review_owner_marker    TEXT UNIQUE,
  state                 TEXT NOT NULL
                        CHECK(state IN ('active','awaiting_resume','completed','revoked')),
  review_sealed         INTEGER NOT NULL DEFAULT 0 CHECK(review_sealed IN (0,1)),
  next_review_sequence  INTEGER NOT NULL DEFAULT 0,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  expires_at            INTEGER NOT NULL,
  UNIQUE(active_run_id)
);

CREATE TABLE github_agent_operations (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id       TEXT NOT NULL UNIQUE,
  client_request_id  TEXT NOT NULL,
  attempt_id          TEXT NOT NULL REFERENCES github_collaboration_attempts(attempt_id),
  created_by_run_id   TEXT NOT NULL REFERENCES run_capabilities(run_id),
  task_id            INTEGER NOT NULL REFERENCES tasks(id),
  agent               TEXT NOT NULL,
  role                TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
  pr_number           INTEGER NOT NULL,
  head_sha            TEXT,
  kind                TEXT NOT NULL,
  request_json        TEXT NOT NULL,
  state               TEXT NOT NULL
                      CHECK(state IN ('queued','running','succeeded','failed','cancelled')),
  attempts            INTEGER NOT NULL DEFAULT 0,
  next_attempt_at     INTEGER,
  deadline_at         INTEGER NOT NULL,
  review_sequence     INTEGER,
  github_marker       TEXT,
  response_json       TEXT,
  error_kind          TEXT,
  error_summary       TEXT,
  completed_after_revocation INTEGER NOT NULL DEFAULT 0
                                     CHECK(completed_after_revocation IN (0,1)),
  created_at          INTEGER NOT NULL,
  updated_at          INTEGER NOT NULL,
  expires_at          INTEGER NOT NULL,
  UNIQUE(attempt_id, client_request_id),
  UNIQUE(attempt_id, review_sequence)
);

CREATE TABLE github_review_publication_slots (
  publisher_scope      TEXT NOT NULL,
  pr_number            INTEGER NOT NULL,
  task_id              INTEGER NOT NULL REFERENCES tasks(id),
  attempt_id           TEXT REFERENCES github_collaboration_attempts(attempt_id),
  state                TEXT NOT NULL
                       CHECK(state IN ('probing','owned','cleanup_required',
                                       'cleanup_running','blocked')),
  pending_review_id    TEXT,
  review_owner_marker  TEXT,
  cleanup_attempts     INTEGER NOT NULL DEFAULT 0,
  next_cleanup_at      INTEGER,
  cleanup_deadline_at  INTEGER,
  error_kind           TEXT,
  error_summary        TEXT,
  created_at           INTEGER NOT NULL,
  updated_at           INTEGER NOT NULL,
  PRIMARY KEY(publisher_scope, pr_number),
  UNIQUE(attempt_id)
);
```

Attempt states are `active`, `awaiting_resume`, `completed`, and `revoked`. Operation states are
`queued`, `running`, `succeeded`, `failed`, and `cancelled`. Requests are immutable after enqueue;
only execution state/result fields change. `request_json` and `response_json` use closed,
kind-specific schemas and fixed aggregate limits. Terminal rows expire after a seven-day
retention window. Nonterminal rows have a one-hour execution deadline and therefore cannot remain
live indefinitely. An attempt becomes terminal with its exact lifecycle turn and expires only
after all of its rows are terminal and any owned remote pending-review slot has been released;
`active` and `awaiting_resume` attempts remain tied to the persisted exact pending turn rather
than becoming reusable identities.

Every mutation accepts an optional `clientRequestId`, matching the role of an idempotency key. If
it is absent, the adapter derives it from the collaboration attempt, operation kind, target, and
canonical closed-schema request. The `operation_id` and hidden marker are deterministic from the
attempt and client request ID. Identical retries across fresh-capability resumes of the exact turn
therefore return the original durable operation without relying on agent discipline; a caller
supplies a distinct ID only when intentionally repeating identical content. Every write body
receives the marker where GitHub permits one. Before a row's first send, after a crash in the
ambiguous post-request/pre-commit window, and before any resend, the daemon queries the exact
PR/review/thread for that marker or an operation-specific idempotent state predicate. Expiry or
re-creation of a row does not change its marker.

Admission is checked in the enqueue transaction after logically expired terminal rows are swept.
The fixed initial limits are 64 nonexpired operations created by one run, 128 per collaboration
attempt, 512 per task, and 4,096 in the repository database. All four counts include terminal
retention rows so persistent storage, not only active work, is bounded. A request that would
exceed any limit returns a typed `capacity_exceeded` result and creates no row. Limits are closed
daemon constants, not agent input or configuration that a managed run can raise.

Attempt creation is separately capped at 16 nonexpired attempts per task and 1,024 in the
repository database, including terminal-retention rows. It returns the same fail-closed capacity
outcome before provisioning if either cap is reached. Thus the identity table cannot become an
unbounded side channel around the operation-row limits. Publication-slot admission is separately
capped at 16 rows per task and 1,024 in the repository. A slot row is deleted after successful
release, so probing or blocked cleanup cannot create an independent unbounded row class even
before its first reviewer attempt is created.

The single daemon claims one queued operation with a guarded `UPDATE ... RETURNING` inside
`BEGIN IMMEDIATE`. Claim eligibility atomically revalidates that the attempt is active, its
`active_run_id` names a live capability, and the capability, task phase, agent, role, PR,
lifecycle generation, and reviewer launch SHA still match. A queued row that fails revalidation
becomes `cancelled`; it is never sent. Task cancellation, authoritative phase exit, launch-head
invalidation, or non-resumable teardown revokes affected attempts and cancels all their queued
rows in the same lifecycle transaction. A resumable process interruption instead changes the
attempt to `awaiting_resume`; no row is claimable until the exact-continuation adoption transaction
reactivates it.

After claim commits, the daemon performs the network call and records the bounded result in a new
transaction. The claim commit is the local point of no return: cancellation or revocation racing
after it cannot recall a request already handed to GitHub. Such a call may produce one remote
write, but its row is marked `completed_after_revocation=1`, it cannot satisfy a lifecycle guard,
and no later operation in that attempt is sent. For a reviewer mutation, the executor first fetches
the current remote PR head without a transaction, then the claim transaction requires that
observation to equal the launch SHA and that the local authority generation has not changed. A
head move discovered there revokes the attempt. GitHub cannot participate in the SQLite
transaction: the unavoidable remote head-move race after that final observation may produce one
stale write, but a later mismatch has the same completed-after-revocation disposition and can
never authorize a verdict.

On graceful shutdown the daemon stops claiming new rows and gives a claimed Repository Service
call only its existing 30-second kill/reap bound. Startup treats every orphaned `running` row as
reconciliation-required. If its attempt is active or later exactly adopted, the executor checks
the marker before resend. If the attempt is revoked, startup may perform the read-only marker
check to record whether the already-authorized call landed, but it never resends; absent evidence
becomes `cancelled`. Network calls, sleeps, and response parsing never hold a DB transaction.

Each operation permits at most eight claim/reconciliation cycles and at most one hour from
`created_at`; the earlier limit wins. Backoff is exponential with jitter and a fixed maximum.
Crossing either bound atomically changes the row to `failed` with `retry_exhausted`, and no
automatic path sends it again. Saturation and exhaustion are infrastructure outcomes: they create
no `errors` row and consume no rework, reviewer-provision, or provider-retry budget. The caller
receives the terminal result. If an exhausted operation is required by a pending lifecycle submit,
the daemon uses the existing durable parking path and preserves the exact attempt and marker for
operator recovery; recovery reconciles and requeues that same operation rather than minting a new
ID.

`github_operation_read` returns `queued`, `running`, `succeeded`, `failed`, or `cancelled` only
when the caller is the live `active_run_id` of the operation's attempt. A freshly provisioned exact
continuation can therefore observe adopted operations; a revoked prior run cannot. A tool call
waits at most the existing 30-second publication bound; if unfinished it returns the operation ID
and a pending result rather than blocking the agent or creating a second request.

### Daemon-owned pending-review slot and cleanup

`publisher_scope` is an opaque, stable Repository Service namespace: credentials that GitHub
treats as the same pending-review actor MUST resolve to the same scope. It is daemon-derived,
never agent input, and contains no Hosted account, installation, session, or KMS identity. This
contract does not choose the eventual GitHub App actor; it only requires an adapter to preserve
the namespace equivalence needed for safe serialization and cleanup.

Every reviewer attempt gets an unguessable daemon-owned `review_owner_marker` with at least 128
bits of randomness that is not returned to the agent. The `create` operation initializes the
pending review body with that hidden marker, and `submit_pending` appends the same marker after the
authored summary. A successful or reconciled create persists the remote review ID in the
publication slot. If the create response is lost, the marker, publisher scope, PR, and immutable
launch SHA are the recovery identity; the daemon never guesses from review position or body text.

Before provisioning the first reviewer or any distinct re-review/R1/R2 attempt, the daemon
atomically inserts the unique `(publisher_scope, pr_number)` slot in `probing` state. It commits,
uses the Repository Service to read that publisher's current pending review, and then performs one
of these guarded dispositions:

- no pending review: create the new collaboration attempt and capability and change the slot to
  `owned` by that attempt in one `BEGIN IMMEDIATE` transaction;
- one pending review whose stored ID (when known), owner marker, PR, publisher scope, and original
  launch SHA match a terminal Quorum attempt: change the slot to `cleanup_required` for that exact
  attempt; or
- a missing/foreign marker, mismatched known ID or SHA, multiple candidates, or an active different
  attempt: change the slot to `blocked`, delete nothing, and provision no reviewer.

Exact-continuation adoption is the only exception to the distinct-attempt probe: it requires the
existing `owned` slot to name the same collaboration attempt in addition to all prior adoption
checks. A terminal attempt, task cancellation, head invalidation, failed/cancelled review
predecessor after create, or create completed after revocation changes an `owned` slot to
`cleanup_required` in the same result/lifecycle transaction. Cleanup remains required even when
the owning task is terminal; cancellation removes agent authority, not daemon housekeeping.

`cleanup_required` is not yet claimable while any network operation for the old review attempt is
still running or reconciliation-required. The daemon first lets the bounded child finish or
kills and reaps it, records/reconciles its terminal outcome, and proves that no queued row can be
sent. In particular, an in-flight create cannot pass an empty-review probe and land after the slot
is released. A late create result persists its review ID and leaves the slot cleanup-required;
it never replaces a newer owner.

The daemon claims cleanup with a guarded `cleanup_required -> cleanup_running` update under
`BEGIN IMMEDIATE`, commits, and performs no more than a bounded read/delete/re-read sequence
through the Repository Service. It deletes only the current pending review with the exact stored
owner marker, PR, publisher scope, launch SHA, and remote ID when known. No pending review, or a
404 after an exact delete, is idempotent cleanup success. A timeout or crash after delete is
reconciled by re-reading: absence completes cleanup, while the same exact marker may be retried.
A foreign or ambiguous review is never deleted and makes the slot `blocked`.

Cleanup and the initial empty-slot probe share a maximum of eight claim/reconciliation cycles and
one hour per recovery generation, use bounded backoff, the existing 30-second command kill/reap
and output limits, and never hold a database transaction across GitHub I/O. Exhaustion changes the
slot to `blocked`; it does not create a verdict, lifecycle transition, rework, or reviewer/provider
budget charge. A nonterminal task waiting for the slot uses the existing durable infrastructure
parking path. A terminal owner retains a bounded status-visible cleanup blocker. An
operator-authorized retry of a later parked task may reset the same slot's cleanup generation
and rebind only its waiting-task pointer after infrastructure repair, but it does not create a new
slot, owner marker, review, or automatic unbounded retry path. Transitioning an owned attempt to
cleanup starts a fresh bounded cleanup generation; time spent in the valid owned review turn does
not consume its cleanup deadline.

Graceful shutdown stops new probe and cleanup claims. A claimed Repository Service child receives
the same bounded kill/reap treatment as an agent operation, and the signal itself neither releases
the slot nor changes lifecycle. Startup treats `probing` and `cleanup_running` as
reconciliation-required, retaining the exact marker, review ID, attempt count, and deadline; it
reads remote state before any delete or slot release.

Only after marker-verified deletion or confirmed absence does the daemon delete the slot row. A
distinct attempt then repeats the fresh probe before atomically acquiring the slot. This barrier
applies on startup, re-review, and the R1-to-R2 handoff, so a new reviewer can neither attach to
stale draft content nor bypass cleanup during a GitHub outage. The cleanup result is publication
housekeeping only and never substitutes for, infers, or posts a formal review verdict.

A successful `submit_pending` result, including marker reconciliation after a crash, releases its
attempt's owned slot because the review is no longer pending. A distinct attempt still performs
the fresh remote probe, which fails closed if GitHub contradicts that recorded result.

### Pending-review ordering

Review publication is one durable sequence within the reviewer collaboration attempt. `create`
atomically receives sequence zero. Each accepted inline finding receives the next sequence. An
accepted `submit_pending` receives the final sequence and sets `review_sealed=1` in the same
transaction; after sealing, new inline findings and a second submit are rejected. The executor may
claim a review-sequence row only after every lower sequence has `succeeded` without
`completed_after_revocation`. A queued, running, or backing-off predecessor blocks later rows; a
failed or cancelled predecessor terminally fails its dependents without sending them to GitHub.
If create may have landed, that failure also schedules the daemon-owned cleanup barrier before any
distinct reviewer attempt.

This serializes create, all accepted inline findings in enqueue order, and final COMMENT
submission even when later rows are otherwise due first. The reviewer lifecycle submit guard
requires the exact current attempt to be sealed, every sequence through the final submit to have
succeeded without post-revocation completion, the final operation to be `submit_pending`, and the
immutable launch SHA still to equal a freshly fetched remote PR head. After that bounded fetch,
the database proof and unchanged-authority check occur in one `BEGIN IMMEDIATE` transaction; the
same unavoidable post-fetch remote race as the existing launch-SHA attestation remains. A pending
summary alone, or a succeeded submit with any missing/queued/running/failed/cancelled predecessor,
cannot authorize the verdict.

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

Retryable operations remain queued only within the fixed attempt and wall-clock bounds above,
using bounded exponential backoff and jitter. Permanent closed-schema input errors fail the
operation and return actionable tool feedback. Authorization loss, repository deselection,
credential failure, or an invalid current PR target fails closed and is surfaced as an
infrastructure/authority condition for operator recovery; it is never rewritten as code rework.

A reviewer lifecycle submit is atomically rejected unless the sealed review sequence satisfies
the complete predecessor and immutable-SHA guard above. This is a core submit guard, not a prompt
rule. Every worker or reviewer lifecycle submit is also rejected while its collaboration attempt
has any accepted mutation queued, running, failed, cancelled, or succeeded only after revocation.
If the coding-runner turn ends while GitHub publication remains pending, the daemon preserves the
exact attempt as `awaiting_resume` for bounded reconciliation and does not infer completion or a
verdict from the model's text.

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
- queued writes are cancelled on task cancellation, phase exit, non-resumable revocation, and
  reviewer head movement; a running write in each race is reconciled at most once and never
  authorizes lifecycle after revocation;
- graceful and forced shutdown with queued/running writes preserves the point-of-no-return and
  reconciliation rules without inferring a lifecycle outcome;
- a fresh capability for the exact persisted Claude/Codex continuation adopts the same attempt,
  operation IDs, markers, and pending review, while a changed task/role/PR/SHA/turn cannot adopt;
- crash after pending-review create, cancellation/head movement racing create, and a failed or
  cancelled post-create sequence all leave a marker-owned cleanup barrier that survives restart;
- cleanup cannot claim until every old review network child has completed or been killed, reaped,
  and reconciled, so an in-flight create cannot land after confirmed absence and slot release;
- cleanup deletes only the exact daemon-owned pending review, reconciles crash-after-delete as
  success, and blocks without deletion on a foreign/missing marker, mismatched ID/SHA, or outage;
- shutdown during probe/delete retains the slot and startup reconciles it before any new reviewer;
- re-review and R1-to-R2 provisioning cannot begin until the prior publisher/PR slot is confirmed
  empty, and cleanup exhaustion consumes no lifecycle or reviewer/provider budget;
- moved heads reject new inline review work and cannot authorize a verdict;
- invalid path/line/side/range and foreign/outdated thread anchors fail visibly;
- authored Markdown with lists, tables, fences, suggestions, Unicode, quotes, and newlines reaches
  the fake GitHub boundary structurally unchanged;
- NUL, invalid schema, oversized bodies/results, excessive pages/comments, and unknown fields fail;
- crash-after-GitHub-success reconciliation produces exactly one visible contribution;
- concurrent duplicate requests produce exactly one outbox row and GitHub write;
- distinct-ID saturation at the run, attempt, task, and repository caps rejects admission without
  a row, attempt-table saturation rejects provisioning without a row, and prolonged outage
  reaches the retry-count/age bound with no further retries or lifecycle budget consumption;
- a delayed or backing-off first inline finding prevents every later inline finding and pending-
  review submit from being claimed, and the lifecycle verdict remains rejected;
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
   endpoint, stable collaboration-attempt issuance and exact-continuation adoption, route managed
   `submit`/`react` without semantic changes, add the stdio server and closed schemas, derive
   role/target in the daemon, and prove the adapter has no direct database or GitHub execution
   access.
2. **M — Durable operation outbox and daemon executor skeleton.** Add the migration, atomic
   enqueue/claim/revalidation/revocation/result/recovery paths, fixed admission and retry bounds,
   retention, publication-slot reservation, bounded cleanup/status skeleton, shutdown
   reconciliation, and fake Repository Service.
3. **M — PR read and general-comment operations.** Add bounded reads, exact target checks,
   Markdown-safe general comments, markers, and outage classification.
4. **M — Pending reviews and inline findings.** Add create/resume/submit COMMENT review,
   validated single/multiline anchors for the launch SHA, durable sequence/seal dependencies, and
   marker-verified orphan-draft cleanup across restart/re-review/R1-to-R2, plus the complete-
   publication-before-verdict submit guard.
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
