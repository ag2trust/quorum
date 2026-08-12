# Bounded Task Decomposition — Technical Specification

**Date:** 2026-07-31
**Status:** Proposed
**Product source:** `quorum-pml` commit `b6d4e4b`
**Implementation prerequisite:** PR #485 (explicit classifier size and admission readiness)

## Problem

Large implementation tasks repeatedly discover blockers only after delivery has started. A
single coding run cannot reliably hold, inspect, implement, and verify an L or XL outcome in
one bounded session. Rework then becomes accidental planning.

Quorum must turn each admission-ready L or XL implementation task into one bounded,
preclassified DAG before implementation starts. Planning must preserve the source outcome,
fail closed when safe boundaries cannot be found, and never expose a partial graph.

## Scope and non-goals

This feature adds daemon-owned decomposition planning, atomic graph materialization,
graph-aware scheduling, graph cancellation/recovery, and read-only graph status.

It does not add a general workflow engine, recursive decomposition, owner approval of valid
plans, manual graph editing, cross-repository graphs, or planner delivery authority. Review-only
L/XL work is held for external splitting. Generated tasks are always S or M and cannot be
decomposed.

## Terms

- **Admission ready:** classifier-owned determination that scope is sufficiently clear and
  bounded for its assigned path. It is independent of dependency completion.
- **Runtime ready:** all task dependencies are done and every atomic claim guard passes.
- **Source:** the original L/XL implementation task.
- **Planning cycle:** durable pre-materialization state for one source revision.
- **Task graph:** the one atomically materialized set of generated tasks and prerequisite edges.
- **Active graph:** a materialized graph not completed or cancelled. A blocked graph remains
  active and prevents another repository graph.

## Lifecycle

Add `planning` and `decomposed` as nonterminal, unclaimable source task states.

```text
open -> planning -> decomposed -> done
          |             |
          v             v
        failed       cancelled
```

`failed` during planning is a durable hold using the existing daemon parking contract. It is
used for a valid Planning Blocker, exhausted proposal attempts, exhausted provider failures, or
unsafe recovered state. A materialized graph blocker does not move the source out of
`decomposed`; it marks the graph blocked and fails the affected child. Only source cancellation
and a replacement source can normally recover a graph after generated delivery has started.

There is one narrow incident-recovery exception for a failed generated child whose exact work
was subsequently delivered by a separate managed continuation task. An explicit core call may
adopt that delivery only when the failed task is the final unfinished member of the still-active
graph; both tasks identify the same repository and PR; the continuation has creator-selected
`continue_pr` authority and explicit `source_task` provenance; and its daemon publication,
managed approval, merge transition, PR target, and approved head SHA all agree. Publication and
merge events are short-lived evidence: every required event must be live at the transaction's
`now` (`expires_at > now`), so an event at its expiry boundary is rejected regardless of whether
housekeeping has swept it. One `BEGIN IMMEDIATE` transaction rechecks all evidence, marks only
the failed child done with explicit recovery provenance, preserves its PR association, and runs
ordinary final-child graph completion. The continuation row remains unchanged. No match, replay,
or losing concurrent caller changes state or emits an event; the winner emits the ordinary
bounded child and graph completion events exactly once.

The daemon invokes this primitive automatically after graph consistency reconciliation and before
generic stateless lifecycle recovery or provisioning, once at startup and once per ordinary tick.
Discovery reads at most eight physical lifecycle-event rows in ascending sequence order from a
dedicated persisted cursor. It filters for a live terminal `task_done` record, then resolves the candidate's
explicit `source_task`, active graph membership, PR targets, and live publication/merge evidence
through primary-key/subject-indexed short reads. It neither scans all done tasks nor performs
network I/O. The read connection closes before any guarded core write begins.

The daemon acknowledges the event page monotonically only after every candidate application
and retry-marker write returns successfully. The short read also records whether another active
sibling is unfinished. If that snapshot is partial and the guarded call remains a clean negative,
the daemon records one idempotent, TTL-bounded event marker for the exact child/recovery pair. A
later sibling `task_done` event drains live markers in ascending event sequence through bounded
active-graph and subject-indexed reads. A second monotonic cursor advances after each settled pair;
at most eight pairs are applied per pass, and a full batch retains the sibling trigger so the next
pass continues with the next-oldest marker. If another sibling remains unfinished, the trigger is
consumed without advancing pending markers and the next sibling completion retries them. Every
settled page advances rather than letting a stalled graph starve later deliveries. A crash before
application leaves the cursor unchanged; a crash after the core, marker, or pending-cursor commit
but before acknowledgement replays an idempotent no-op. A startup-pass error is logged and does not
block other recovery, while a normal-tick error follows the ordinary tick error policy; neither
advances the page. Candidate discovery grants no authority beyond the guarded core predicate and
cannot recover a `blocked` graph: graph-blocker scope defects still require source cancellation
and a replacement source.

Every event transition that marks a generated child done, including the final child merge, and
every permitted manual child close checks graph completion in the same transaction. If every
graph member is done, that transaction marks the source and graph done atomically. Manual close
rejects an active graph source, which must use graph cancellation. Source dependents remain
blocked until the source transition commits.

## Durable model

Use authoritative tables rather than `tasks.refs`. Refs remain a projection for compatibility
and display, not lifecycle authority.

Add `revision INTEGER NOT NULL DEFAULT 1` and `edit_count INTEGER NOT NULL DEFAULT 0` to
`tasks`. Every externally editable task advances `revision` with compare-and-swap semantics;
the three-edit cap is enforced for a task once classification is pending or complete. Classifier
input captures the revision and its write transaction stores results only while it still
matches. This task-level authority exists before Quorum knows whether a task is L/XL and lets an
edit invalidate an in-flight classifier turn without first creating decomposition state.

### `task_decompositions`

One durable aggregate per source:

```sql
CREATE TABLE task_decompositions (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  source_task_id         INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
  state                  TEXT NOT NULL,
  active                 INTEGER NOT NULL DEFAULT 0,
  freeze_active          INTEGER NOT NULL DEFAULT 0,
  planned_source_revision INTEGER NOT NULL,
  proposal_attempts      INTEGER NOT NULL DEFAULT 0,
  provider_failures      INTEGER NOT NULL DEFAULT 0,
  planner_provider       TEXT,
  planner_model          TEXT,
  planner_session_id     TEXT,
  frozen_base_sha        TEXT,
  accepted_plan_revision INTEGER,
  hold_code              TEXT,
  hold_summary           TEXT,
  created_at             INTEGER NOT NULL,
  updated_at             INTEGER NOT NULL
);

CREATE UNIQUE INDEX one_active_task_graph
  ON task_decompositions(active) WHERE active = 1;
CREATE UNIQUE INDEX one_planning_freeze
  ON task_decompositions(freeze_active) WHERE freeze_active = 1;
```

States are `freeze-requested`, `draining`, `planning`, `validating`, `preclassifying`,
`provider-backoff`, `held`, `active`, `blocked`, `completed`, and `cancelled`. `active=1` is set
only for a materialized `active` or `blocked` graph. `freeze_active=1` covers the stable-repo
planning window. `planned_source_revision` is only the captured task revision for compare-and-swap
validation; `tasks.revision` and `tasks.edit_count` remain authoritative. Partial unique indexes
are the race authority.

### Members, attempts, and cleanup

```sql
CREATE TABLE task_graph_members (
  graph_id      INTEGER NOT NULL REFERENCES task_decompositions(id),
  task_id       INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
  local_key     TEXT NOT NULL,
  plan_revision INTEGER NOT NULL,
  active        INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (graph_id, plan_revision, local_key)
);

CREATE TABLE decomposition_attempts (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  graph_id         INTEGER NOT NULL REFERENCES task_decompositions(id),
  source_revision  INTEGER NOT NULL,
  kind             TEXT NOT NULL,
  ordinal          INTEGER NOT NULL,
  reason_code      TEXT NOT NULL,
  summary          TEXT NOT NULL,
  created_at       INTEGER NOT NULL,
  UNIQUE (graph_id, source_revision, kind, ordinal)
);

CREATE TABLE decomposition_cleanup (
  graph_id       INTEGER NOT NULL REFERENCES task_decompositions(id),
  task_id        INTEGER NOT NULL REFERENCES tasks(id),
  artifact_kind  TEXT NOT NULL,
  artifact_ref   TEXT NOT NULL,
  state          TEXT NOT NULL DEFAULT 'pending',
  attempts       INTEGER NOT NULL DEFAULT 0,
  last_error     TEXT,
  updated_at     INTEGER NOT NULL,
  PRIMARY KEY (graph_id, task_id, artifact_kind, artifact_ref)
);
```

Child prerequisites continue to use validated `tasks.depends_on`. Membership supplies source,
planner, and plan-revision provenance. Attempt rows contain only bounded structured summaries:
at most three semantic rejections and three provider failures per source revision. Full prompts
and transcripts are never stored. Cleanup intents make post-commit GitHub/filesystem cleanup
idempotent and restartable.

Inactive membership rows retain recovery audit history and are never scheduled. At most one
plan revision per graph may have active members, enforced by materialization checks under the
write lock. The aggregate remains the source's one Task Graph across a safe pre-delivery replan.

The migration is additive and forward-only under the normal `BEGIN IMMEDIATE` migration lock.

## Admission and repository freeze

The daemon selects planning candidates by priority, then task ID. A candidate must be an open,
unclaimed, admission-ready L/XL implementation task whose dependencies are done. It must not be
review-only or generated work. Review-only work always routes directly to reviewer provisioning
at any classified size; S/M implementation work follows normal dispatch regardless of complexity.
The atomic planning transaction rechecks `review_only=0` and `continue_pr IS NULL`, so neither
PR-bound entry shape can become a decomposition source even if a stale caller selects it.

Starting a cycle atomically moves the source to `planning`, records `freeze-requested`, and sets
`freeze_active=1`. Every worker, reviewer, remediation, and merge-start authority check must
consult the freeze inside its existing `BEGIN IMMEDIATE` transaction. In-memory daemon phase
checks are an optimization only.
Reviewer external provisioning uses a short-lived durable reservation acquired by that
transaction; freeze acquisition is refused while any reservation is live, and every success or
failure path releases it.

After the freeze commits, already active managed work may finish, including protected merge.
Nothing new is provisioned. Planning starts only when live slots and durable in-flight journal
rows are empty. This coordinator is separate from self-update `DrainState`, which drains and
exits the process.

A semantic plan rejection retains the freeze. A provider failure clears it during bounded
backoff; retry reacquires it and drains again. A valid blocker or exhausted budget clears the
freeze and parks the source. Atomic materialization clears it after committing the graph.

## Planner boundary and protocol

The active configured provider determines the fixed frontier model:

- Codex: Sol, high reasoning.
- Claude: Opus, high reasoning.

There is no fallback or model downgrade. If the exact model or a mechanism-level sandbox is
unavailable, planning fails under the provider budget.

The planner runs against the frozen base revision with:

- a read-only repository view;
- no network namespace access;
- no Quorum binary, database path, run capability, or coordination environment;
- source-directed inspection guidance capped at five Grep/Glob calls and ten focused Read calls;
- a 128 KiB streamed-stdout ceiling, 64 KiB response ceiling, and 600-second wall-clock
  ceiling; no provider USD ceiling is set;
- bounded stderr diagnostics;
- process-group kill and reap on timeout/cancellation.

This requires a planner-specific provider spawn boundary. Worker commands and any Codex path
that bypasses sandbox enforcement are forbidden. Real provider-binary argument tests are
required; `fake_agent` alone cannot prove pre-protocol isolation.

The accepted response is exactly one closed JSON object:

```json
{"outcome":"plan","tasks":[...]}
```

or:

```json
{"outcome":"blocker","category":"...","evidence":["..."],"required_decision":"...","why_no_safe_split":"..."}
```

A plan contains 2–8 uniquely keyed tasks. Each task has a title, concrete implementation delta,
affected repository paths, observable outcome, acceptance criteria, applicable source constraints,
verification expectations, explicit non-goals, byte-exact preserved literals, and prerequisite
local keys or source dependency IDs. The planner receives the same S/M execution-size rubric as
the classifier. It inspects from source-named paths and symbols under bounded search/read guidance,
and separates independently deliverable code or ownership seams rather than turning preserved
outcomes into standalone work. A blocker must be concrete. Markdown wrappers, unknown fields,
multiple outcomes, oversized output, malformed JSON, and sandbox violations are provider
failures.

Literal preservation is byte-exact and bounded by the 8 KiB `preserved_literals` field.
Inline/fenced Markdown code and quoted values attached to the words `literal`, `label`, `tag`, or
`message` are deterministically extracted from the source title and body only when they fit that
field. Every extracted value must appear unchanged in at least one child's
`preserved_literals`; every planner-declared preserved literal must occur in the source bytes.
Missing or normalized values reject the proposal before classification. Larger source-marked
values remain planner context but are not preservation-field requirements, because no valid
proposal can carry them. Source authors should use those explicit forms whenever spelling is
load-bearing and fits the field bound.

A syntactically valid blocker that lacks a supported category, concrete evidence, required
decision, or the explanation of why no safe split exists is a semantic rejection, not a valid
blocker and not a provider failure.

Each semantic retry starts a fresh planner session and receives the source plus only the bounded
structured rejection summaries. Provider continuation identity is not trusted across a released
freeze. An interrupted provider call is a provider failure, never a semantic rejection.

## Validation and preclassification

Deterministic validation rejects the complete proposal for:

- fewer than two or more than eight tasks;
- duplicate keys, self-edges, cycles, or unknown dependencies;
- empty required fields or synthetic integration work;
- missing or modified byte-exact source-marked literals;
- dependencies that are not real delivery prerequisites;
- classifier-reported duplication of existing work.

Duplicate authority is fail-closed: any child classifier duplicate reference rejects the whole
plan; deterministic exact/normalized identity checks may reject earlier but never modify the
existing task.

All proposed children are classified together before any child row exists. Classification uses
temporary proposal keys, not task IDs. Every result must be present, admission-ready,
implementation work, nonduplicate, and size S or M. Runtime readiness is deliberately not
required: a child may wait for another generated prerequisite. Any missing, malformed, L/XL,
not-ready, duplicate, or extra classification rejects the entire plan as a semantic proposal.

Semantic rejection increments only `proposal_attempts`; provider/protocol/sandbox failure
increments only `provider_failures`. Each cap is three per unchanged source revision. A valid
Planning Blocker is immediately held, consumes neither budget, and receives no second opinion.

## Atomic materialization

`materialize_graph` runs in one `BEGIN IMMEDIATE` transaction and rechecks the source revision,
source state, freeze ownership, plan limits, classifications, graph uniqueness, and repository
active-graph uniqueness. It then:

1. creates all children with inherited priority and creator;
2. attaches final independent classifications and planner provenance;
3. resolves local keys and writes all prerequisite edges;
4. writes every membership row;
5. moves the source from `planning` to `decomposed`;
6. marks the graph active and clears the freeze;
7. emits bounded lifecycle events.

Any failure rolls back everything. Generated implementation cannot observe or claim a partial
graph. Repeated materialization or restart loses cleanly to the source and active-graph unique
constraints and creates nothing.

## Graph scheduling and child delivery

Generated tasks use the ordinary independent implementation, review, rework, and protected
merge lifecycle. Their precomputed classification supplies their own provider/model routing.
Review prompts include the assigned source requirements and direct prerequisites and prohibit
moving sibling scope into the reviewed child.

Inside the atomic claim transaction, a generated child additionally requires:

- source status `decomposed` and graph state `active`;
- no failed sibling and no graph blocker;
- every ordinary dependency done;
- fewer than two working implementation siblings.

Already active children keep authority after a sibling fails and may finish. Later claims fail
cleanly. Eligible graph children sort before unrelated open work, then retain normal priority/ID
ordering. If no graph child is eligible, unrelated work may use idle capacity. Existing active
unrelated work is never interrupted and graph work reserves no idle slot.

A reviewer-confirmed decomposition defect is a distinct graph-blocker verdict. It atomically
fails the affected child and blocks the graph; it does not consume ordinary rework. No new child
starts. The source stays decomposed until explicitly cancelled, after which a replacement source
is required.

Managed reviewers signal this only through a closed
`submit --verdict graph-blocker --feedback-json <bounded-json>` outcome containing category,
affected task, violated assigned boundary, and concrete diff/repository evidence. The command is
authorized by the reviewer's immutable run capability and accepts the affected task only when it
is the current generated child for that run. The daemon rechecks current head, graph membership,
and schema, then uses one core transaction to record the evidence, fail that child, set graph
state `blocked`, and release its review authority. Invalid or stale signals are rejected without
lifecycle change. `blocked` retains `active=1` until source cancellation.

## Editing

All externally authorized task edits use an expected task revision. Before materialization, an
accepted edit atomically stops pending admission, discards structured pending artifacts, applies
the complete requested task update, advances the revision and edit count, resets both planning
budgets, and restarts classification. Quorum does not categorize changes.

Stale expected revisions and exact replay requests are clean negatives and do not increment the
counter. The fourth accepted edit is rejected while processing of the latest accepted revision
continues. After materialization, external edits to source or child task fields and dependencies
are rejected; daemon-owned lifecycle fields, notes, evidence, and cleanup bookkeeping remain
writable.

## Cancellation and cleanup

Direct generated-child cancellation is rejected. Source cancellation uses one immediate
transaction to mark the graph non-runnable, cancel every unfinished child, revoke live claims and
run capabilities, record durable cleanup intents, and mark the source cancelled. Done children,
events, reviews, approvals, and merged-delivery records remain intact.

Every daemon-owned child branch allocation durably records its exact worktree, branch name, and
immutable provisioning commit before Git provisioning. The provisioning commit is provenance,
not deletion authority: an unpublished worker may advance the branch before cancellation. When no
pinned publication or validated PR head supplies an exact deletion SHA, cancellation records a
non-destructive `branch-discovery` intent containing the complete immutable allocation identity.
After process, proposed-change, and worktree cleanup, the daemon revalidates that allocation and
provenance, resolves the now-quiescent ref, and atomically replaces discovery with a finalized
old-OID-bound branch deletion intent. Ref deletion uses compare-and-delete semantics and a
deterministic cleanup tombstone so crash replay cannot delete a branch recreated at the same name.

After commit, the daemon kills and reaps active graph processes and executes cleanup intents:
close unmerged proposed changes, remove temporary worktrees, and remove only validated revocable
branches. Each action is idempotent, bounded, and retried after restart. Cleanup failure is loud
but cannot restore graph execution authority.

## Recovery and upgrade

Decomposition reconciliation runs before generic lifecycle recovery or provisioning.

- An active freeze remains authoritative and resumes its source first.
- `provider-backoff` does not freeze delivery; retry must reacquire and drain.
- A complete valid active graph resumes without recreation.
- Incomplete or inconsistent state is held and starts nothing.
- An inconsistent graph may replan only if no child has ever been claimed, created a proposed
  change, or merged and proposal budget remains.
- Any evidence of generated delivery forbids automatic replanning and requires cancellation plus
  replacement except for the exact-continuation adoption predicate described under Lifecycle.
  After consistency checks, startup and tick reconciliation discover that case through the
  bounded monotonic event cursor and invoke the guarded transaction; no shared-PR or inferred
  provenance match is sufficient.
- Generic recovery must not reset planning/decomposed sources or treat children as unrelated.
- Pre-feature unrelated tasks enter the current admission policy when next eligible. A complete
  existing graph resumes; incomplete/inconsistent graphs are held with a visible reason.

Materialization is one transaction, so normal crashes produce either no graph or a complete graph.
Recovery consistency checks defend against manual corruption and older/experimental rows.

For an inconsistent graph with zero delivery evidence, recovery performs one fail-safe reset
transaction: mark every extant member inactive, cancel and de-authorize every unstarted generated
task, clear accepted-plan fields, advance the plan revision, clear `active` and `freeze_active`,
move the same decomposition aggregate to `freeze-requested`, and retain a bounded recovery
summary. The normal coordinator then reacquires `freeze_active`, drains managed delivery, and
enters planning before any provider call. A later materialization inserts a new active member set
under that same graph ID and new plan revision. It never deletes history or creates a second source
graph. If any child has claim, run, PR, review, or merge evidence, this reset is forbidden and the
graph is held for cancellation/replacement.

## Read-only status

Status and inspect projections expose source/child membership, prerequisite edges, graph state,
counts by child lifecycle state, completion ratio, current blockers, failed children, proposal
and provider attempt counts, final bounded reasons, planner model/provider, and accepted plan
revision. They expose no mutation controls and no prompt/transcript.

## Implementation seams

1. **Core storage and authority:** schema migration, `decomposition` module, lifecycle states,
   atomic admission/materialization/edit/claim/completion/cancellation/recovery primitives.
2. **Planner and classifier boundary:** closed protocol, hardened provider spawns, bounded process
   lifecycle, proposed-task batch classification.
3. **Daemon orchestration and cleanup:** freeze/drain phases, recovery ordering, graph blocker,
   cleanup intents, worker/reviewer integration.
4. **Read surface:** status/inspect/cockpit projections after the storage contract is stable.

The seams may land as separate commits but must ship behind one coherent schema and lifecycle
contract. PR #485 must be merged first; implementation is rebased from the resulting `origin/main`
before code changes begin.

## Required evidence

- Fresh-schema and populated previous-version migration tests.
- Real-file, repeated concurrent processes proving one planner/graph and at most two child claims.
- Claim/freeze, edit/materialize, cancel/submit, and final-child-completion race tests.
- Fault injection at each materialization step proving no partial task or membership rows.
- Restart tests for every planning phase, provider backoff, active/blocked graph, and cleanup.
- Negative tests for all plan/protocol/sandbox/retry/edit/cancellation rules.
- Real Claude and Codex CLI argument/auth/sandbox boundary tests.
- Full `rtk proxy ./preflight.sh` before submission.
