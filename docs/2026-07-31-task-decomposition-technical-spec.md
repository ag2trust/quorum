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
and a replacement source can recover a graph after generated delivery has started.

The final child merge transaction marks the child done and, if every graph member is done,
marks the source and graph done atomically. Source dependents remain blocked until that source
transition commits.

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
review-only or generated work. S/M work follows normal dispatch regardless of complexity.

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
- bounded stdout, stderr, response bytes, and wall-clock time;
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

A plan contains 2–8 uniquely keyed tasks. Each task has title, observable outcome, acceptance
criteria, applicable source constraints, verification expectations, and prerequisite local keys
or source dependency IDs. A blocker must be concrete. Markdown wrappers, unknown fields,
multiple outcomes, oversized output, malformed JSON, and sandbox violations are provider
failures.

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
- empty/no-op work or synthetic integration work;
- missing source outcome/constraint coverage;
- unrelated scope or weakened source constraints;
- dependencies that are not real delivery prerequisites;
- duplication of existing work.

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
  replacement.
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
