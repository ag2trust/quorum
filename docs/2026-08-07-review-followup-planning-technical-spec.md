# Review Follow-up Planning — Technical Specification

**Date:** 2026-08-07  
**Status:** Implemented prospectively
**Product source:** `ag2trust/quorum-pml` PR #1  
**Implementation base:** `origin/main` / `origin/develop` at `7b38ac8c`

## Problem

Quorum currently gives each review finding one lifecycle-sensitive classification:
BLOCKING or advisory. The reviewer prompt also makes several concrete failure classes
presumptively BLOCKING without first asking whether the failure is inside the current task,
supported operating assumptions, or established threat model. This correctly catches real
defects but can turn valid adjacent hardening observations into repeated rework on one PR.

The post-merge collector already normalizes review findings into durable analytics. Those rows
are replaced on re-interpretation and have no downstream lifecycle. Quorum needs a separate,
durable path that preserves substantive non-blocking evidence, lets the configured Planning Agent
assess it against source intent and existing GitHub issues, and creates only bounded,
execution-ready GitHub issues through daemon authority.

## Scope and non-goals

This feature adds:

- independent technical-impact and merge-disposition guidance for reviewers;
- post-merge extraction of evidence-backed Follow-up Artifacts;
- durable artifact batches and assessment jobs;
- ordinary-task and Task Graph assessment eligibility;
- a second closed Planning Agent operation for follow-up assessment;
- daemon validation and atomic create/link/dismiss/defer intent staging;
- daemon-owned, marker-idempotent GitHub issue creation; and
- durable inspection data for artifacts, decisions, provenance, and issue relationships.

It does not add:

- an MCP server, protocol tools, or external-agent authentication;
- interactive task-creator behavior;
- a new reviewer transport or replacement for current PR collaboration;
- direct GitHub, Quorum, database, or network access for the collector model or Planning Agent;
- a semantic fingerprint or identity-deduplication table;
- manual artifact editing, manual assessment, reassessment, pruning, or ticket triage;
- historical backfill for PRs merged before this collector generation; or
- a second planning role, live planner-session retention, direct issue-creation MCP authority, or
  a general workflow engine.

The authoritative final review record remains the current PR record. A separate agent-interface
workstream may later change how managed agents write that record without changing the artifact,
collector, planner, or materialization contracts here.

## Terms

- **Technical impact:** `critical`, `major`, `minor`, or `nit`; how serious the concrete
  failure is if its stated assumptions hold.
- **Merge disposition:** `blocking` or `follow-up`; whether the current Proposed Change must
  resolve the finding before merge.
- **Scope relationship:** why a follow-up is separate: `pre_existing`, `out_of_scope`,
  `threat_model_expansion`, `defense_in_depth`, `future_requirement`, or `design_debt`.
- **Follow-up batch:** the immutable artifact set produced by the first successful current-
  generation interpretation of one merged PR.
- **Assessment scope:** either one ordinary merged task or one completed/cancelled Task Graph.
- **Planning Lineage:** bounded source task, accepted decomposition, delivery, and review
  provenance reconstructed from durable state for a follow-up planner turn.

## Reviewer classification contract

Review prompts retain two lifecycle dispositions because `quorum submit` still attests only a
blocking count. Every substantive finding is first assigned technical impact, then independently
assigned merge disposition.

A finding is BLOCKING only when merging the exact change would leave the assigned primary
outcome false, violate an applicable repository invariant, or introduce or materially worsen
supported behavior, and its assumptions fit the established operating or threat model.

Real pre-existing, adjacent, defense-in-depth, future, or stronger-threat-model concerns are
FOLLOW-UP unless an explicit current contract makes them blocking. For documentation changes,
reviewers require the smallest accurate statement of supported behavior rather than an exhaustive
inventory of implementation exceptions; pre-existing edge behavior merely revealed by the change
stays FOLLOW-UP when the primary outcome can remain accurate without cataloguing or fixing it.

Resource exhaustion, data loss, corruption, security-boundary failures, and stuck processing are
presumptively `major` or `critical` technical impact. Their category alone does not decide merge
disposition. Each BLOCKING finding states why this PR is unsafe to merge; each FOLLOW-UP finding
states why it is safe to defer.

The PR summary uses a collector-readable shape without making prose a protocol:

```text
BLOCKING: <N>
FOLLOW-UP: <N>

For each finding:
- impact
- merge disposition
- concrete failure and assumptions
- scope relationship
- why it blocks or does not block
- affected product behavior
- desired future outcome and verification, for follow-ups
```

Only BLOCKING findings contribute to `--blocking`. A review with zero blockers and one or more
follow-ups submits `approved --blocking 0`. Reviewers never create or modify follow-up issues.

When a complete audit has any candidate blocker, every R1/R2 reviewer first calls synchronous
`quorum review-draft --blocking N --feedback ...`. The daemon binds the draft to the exact task,
PR, reviewed head, role, reviewer, and run, then returns neutral second-assessment guidance in the
same turn. The reviewer rechecks every candidate against task scope, repository invariants,
supported behavior, sibling paths, and the follow-up boundary, updates the PR review when a
disposition changes, and only then submits the final verdict. The draft is non-authoritative and
does not start rework. A positive-blocker `changes` verdict without that exact durable checkpoint
fails closed; zero-blocker approvals do not require one.

## Collector protocol

The collector remains a response-only, post-merge interpreter. Deterministic Rust code fetches and
bounds the same final PR and task context it uses today. The model gets no tools. The closed output
becomes:

```json
{
  "findings": [
    {
      "reviewer": "login",
      "kind": "blocking",
      "author_pushback": false,
      "pushback_accepted": null,
      "severity": "major",
      "text": "Concrete finding summary",
      "source_endpoint": "pulls",
      "addressed_status": "addressed",
      "evidence": [{"kind":"review_comment","id":123}]
    }
  ],
  "followup_artifacts": [
    {
      "source_finding_index": 0,
      "technical_impact": "major",
      "scope_relationship": "threat_model_expansion",
      "concern": {
        "failure_mode": "Requests deadlock",
        "trigger_or_assumption": "Concurrent shutdown overlaps request handling"
      },
      "non_blocking_reason": "Why the merged PR did not need to resolve it",
      "affected_behavior": "Product behavior affected by the concern",
      "desired_outcome": {
        "observable_behavior": "Requests complete",
        "observation_condition": "The retry resumes after shutdown"
      },
      "verification_expectations": ["Evidence that would prove the outcome"],
      "evidence": [{"kind":"review_comment","id":123}]
    }
  ]
}
```

Collector validation requires:

- at most 128 findings and 32 follow-up artifacts per PR;
- closed enum values and no unknown response fields;
- closed concern and desired-outcome objects with no unknown fields, separately required
  failure/assumption and observable/condition text, and an 8 KiB bound on each canonical value;
- non-empty bounded text fields (8 KiB each, 64 KiB total artifact JSON);
- one through eight bounded verification expectations per artifact;
- at least one concrete GitHub evidence row per finding and artifact;
- evidence IDs present in the deterministic fetched input;
- streaming evidence-ID extraction without a full JSON DOM, with at most 4 MiB of aggregate
  evidence JSON inspected and at most 20,480 fetched records indexed for validation;
- a response-local zero-based source finding index for every artifact, selecting exactly one
  `suggestion`/non-blocking finding and sharing at least one evidence row with that finding;
- no artifact whose selected source finding says it was fixed, withdrawn, or accepted as invalid;
- no vague improvement artifact lacking both a concrete concern and desired outcome.

The source finding index exists only in the bounded collector response and is validated before
durable artifact construction. It does not turn replaceable `review_findings` row IDs into
lifecycle authority. The collector canonicalizes the two required concern components and the two
required desired-outcome components into the durable `concern` and `desired_outcome` strings.
Unstructured prose, including generic reliability directives, is not a valid protocol value.

Malformed JSON, unknown fields, invalid evidence, or invalid artifact semantics fails the complete
interpretation. A failed run preserves the last successful findings and artifacts verbatim.

## Durable model

Artifacts are not added to `review_findings`. Those rows remain replaceable analytics and their IDs
remain unsuitable as lifecycle authority.

### `review_followup_batches`

One immutable successful artifact snapshot per PR:

```sql
CREATE TABLE review_followup_batches (
  pr_number          INTEGER PRIMARY KEY,
  task_id            INTEGER NOT NULL REFERENCES tasks(id),
  graph_id           INTEGER REFERENCES task_decompositions(id),
  source_task_id     INTEGER NOT NULL REFERENCES tasks(id),
  collector_version  TEXT NOT NULL,
  artifact_count     INTEGER NOT NULL,
  state              TEXT NOT NULL,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
);
```

States are `collected`, `assessing`, and `resolved`. `artifact_count=0` is valid and records that
interpretation succeeded with no follow-up work. `graph_id` is the originating graph when the task
is a generated member; `source_task_id` is the graph source or the ordinary task itself.

The first successful current-generation interpretation inserts the batch and artifacts. Later
manual or automatic interpretation may replace analytics but never changes an existing batch or
resurrects resolved artifacts. A future product decision to reassess historical review evidence
requires a separate forward-only design; a collector-version bump alone does not authorize it.

### `review_followup_artifacts`

```sql
CREATE TABLE review_followup_artifacts (
  id                         INTEGER PRIMARY KEY AUTOINCREMENT,
  pr_number                  INTEGER NOT NULL REFERENCES review_followup_batches(pr_number),
  ordinal                    INTEGER NOT NULL,
  technical_impact           TEXT NOT NULL,
  scope_relationship         TEXT NOT NULL,
  concern                    TEXT NOT NULL,
  non_blocking_reason        TEXT NOT NULL,
  affected_behavior          TEXT NOT NULL,
  desired_outcome            TEXT NOT NULL,
  verification_expectations  TEXT NOT NULL,
  evidence_ids               TEXT NOT NULL,
  disposition                TEXT,
  disposition_reason         TEXT,
  linked_task_id             INTEGER REFERENCES tasks(id),
  created_task_id            INTEGER REFERENCES tasks(id),
  created_at                 INTEGER NOT NULL,
  updated_at                 INTEGER NOT NULL,
  UNIQUE(pr_number, ordinal)
);
```

`technical_impact` is checked against the four-value enum. `scope_relationship` is checked against
the six-value enum. `disposition` is null until application, then exactly one of `created`,
`linked`, `dismissed`, or `deferred`. Exactly one of `linked_task_id` or `created_task_id` is set
for its matching disposition; both are null for dismissal and deferral. JSON arrays are parsed and
bounded again at every core write/read boundary.

### `review_followup_assessments`

```sql
CREATE TABLE review_followup_assessments (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  target                TEXT NOT NULL,
  scope_kind            TEXT NOT NULL,
  scope_id              INTEGER NOT NULL,
  source_task_id        INTEGER NOT NULL REFERENCES tasks(id),
  state                 TEXT NOT NULL,
  active                INTEGER NOT NULL DEFAULT 0,
  membership_sealed     INTEGER NOT NULL DEFAULT 1,
  proposal_attempts     INTEGER NOT NULL DEFAULT 0,
  provider_failures     INTEGER NOT NULL DEFAULT 0,
  planner_provider      TEXT,
  planner_model         TEXT,
  planner_assignment_id INTEGER REFERENCES role_assignments(id),
  base_sha              TEXT,
  hold_code             TEXT,
  hold_summary          TEXT,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  UNIQUE(scope_kind, scope_id)
);
CREATE UNIQUE INDEX one_active_followup_assessment
  ON review_followup_assessments(target) WHERE active = 1;
```

`scope_kind` is `task` or `graph`; `scope_id` is the ordinary task ID or graph ID. `target` is
`followup:task:<id>` or `followup:graph:<id>`. States are `pending`, `planning`,
`provider-backoff`, `held`, and `completed`. `membership_sealed` is opened only by the core
materializer and irreversibly sealed before its transaction commits; all other inserts default to
sealed. The partial unique index provides atomic per-target planning authority. Planning claims and
final application use `BEGIN IMMEDIATE` and guarded `UPDATE ... RETURNING`; lost races are clean
negatives.

### `review_followup_assessment_artifacts`

```sql
CREATE TABLE review_followup_assessment_artifacts (
  assessment_id INTEGER NOT NULL REFERENCES review_followup_assessments(id),
  artifact_id   INTEGER NOT NULL UNIQUE REFERENCES review_followup_artifacts(id),
  PRIMARY KEY(assessment_id, artifact_id)
);
```

Membership is materialized before the provider call and never changes. A storage trigger permits
membership inserts only while the parent assessment is unsealed, and another trigger prevents a
sealed assessment from reopening. The unique artifact ID prevents one artifact from entering two
assessments without semantic fingerprinting.

All migrations are additive, forward-only, idempotent under the normal migration write lock, and
preserve `rusqlite` bundled behavior.

`review_collection_runs` gains `followup_count INTEGER NOT NULL DEFAULT 0`. Pre-feature rows keep
zero and remain analytics-only. A manual `review-interpret` invocation without a task association
may refresh analytics but cannot create a follow-up batch; automatic merge jobs and their retries
always carry the authoritative task ID.

## Successful interpretation transaction

The current collector writes findings and the run record through separate calls. The new success
path uses one core transaction:

1. validate the complete response before opening a transaction;
2. `BEGIN IMMEDIATE`;
3. delete and insert `review_findings` for the PR;
4. if no follow-up batch exists, insert the immutable batch and all artifacts;
5. UPSERT the successful `review_collection_runs` row with both counts;
6. commit.

If a batch already exists, step 4 is a no-op and the existing artifacts remain immutable. Any
failure rolls back findings, artifacts, and the success row together. The queue row is deleted by
normal reconciliation only after it observes a matching successful current-version run.

## Assessment eligibility

Eligibility reconciliation is read-short and runs after collector retry reconciliation. It creates
assessment and membership rows in one `BEGIN IMMEDIATE` transaction.

### Ordinary tasks

An ordinary task is eligible when:

- its task is `done` with daemon-owned `completion_provenance=merged` and a positive PR
  association; manual and legacy/unknown completion provenance fail closed;
- its current-generation collection run succeeded;
- its immutable batch exists; and
- `artifact_count > 0`.

`artifact_count=0` permanently skips assessment for that PR.

### Task Graphs

Generated-child merges collect artifacts but never create child assessment jobs. A graph becomes
eligible when:

- the graph is `completed`, or it is `cancelled` with at least one merged child;
- every merged generated child has `completion_provenance=merged` and a PR association;
- every such PR has a successful current-generation collection run and immutable batch; and
- the union contains at least one unresolved artifact.

The graph assessment includes all artifacts from all merged children. A graph with successful
zero-artifact batches skips assessment. A terminal graph with an exhausted or missing collector
job remains visibly `waiting-interpretation`; it never reopens the source or changes child state.

The assessment uses the graph source task as `source_task_id`. Ordinary tasks use themselves.
Concurrent reconciliation loses to `UNIQUE(scope_kind,scope_id)` or the unique artifact membership
and exits cleanly without errors.

## Planning Lineage and bounded input

Follow-up planning is a fresh bounded turn. Availability or memory of the decomposition planner
session is never required. The daemon reconstructs:

- source task title and body;
- the sealed ordinary-task or accepted-graph artifact membership;
- every artifact's originating PR, technical impact, scope relationship, concern,
  non-blocking rationale, desired outcome, verification expectations, and evidence IDs; and
- up to 128 existing GitHub issues, with exact number, canonical URL, and title.

The complete prompt is capped at 128 KiB and deterministically ordered. If the required sealed
membership cannot fit, the daemon does not spawn the planner and holds the assessment as
`input-too-large`; it never silently drops an artifact. The issue inventory is fetched outside a
database transaction and link validation later requires both the supplied number and URL.

The captured `base_sha` is provenance, not a merge gate: follow-up tasks express product outcomes
and pass through fresh admission/classification. Base movement during the turn does not invalidate
an otherwise valid assessment and does not freeze managed delivery.

## Planner operation and response

Follow-up assessment uses the configured `planner` routing pool through the hardened restricted
response boundary: an empty temporary workspace, no tools, no network or GitHub capability, no
Quorum/database capability, a 120-second timeout, 64 KiB response cap, and 256 KiB stdout cap.
It does not acquire the decomposition delivery freeze or retain a provider continuation.
Decomposition candidates have scheduling priority over pending follow-up assessments.

The closed follow-up response is:

```json
{
  "outcome": "assessment",
  "decisions": [
    {
      "decision": "create",
      "artifact_ids": [10, 11],
      "reason": "One root concern and desired outcome",
      "issue": {
        "title": "Define and enforce the supported state-write threat boundary",
        "issue_type": "documentation",
        "observable_outcome": "The supported threat boundary is explicit and enforced",
        "acceptance_criteria": ["..."],
        "source_constraints": ["..."],
        "verification_expectations": ["..."]
      }
    },
    {
      "decision": "link",
      "artifact_ids": [12],
      "reason": "Existing work has the same concern and outcome",
      "existing_issue_number": 42,
      "existing_issue_url": "https://github.com/owner/repo/issues/42"
    },
    {
      "decision": "dismiss",
      "artifact_ids": [13],
      "reason": "Current repository state already resolves the concern",
      "category": "already_resolved"
    },
    {
      "decision": "defer",
      "artifact_ids": [14],
      "reason": "Product intent is not decided",
      "required_decision": "Whether hostile same-user mutation is supported"
    }
  ]
}
```

Dismiss categories are `invalid`, `obsolete`, `already_resolved`, and `out_of_product`.
One create decision produces exactly one issue; an artifact cannot produce multiple issues. Issue
types are exactly `bug`, `enhancement`, `documentation`, `tests`, or `cleanup`.

Deterministic validation requires:

- one through 32 decisions and no unknown fields;
- every member artifact exactly once across all `artifact_ids`;
- each decision has one through 32 unique artifact IDs and a bounded reason;
- a created issue has title, type, observable outcome, one through eight acceptance criteria,
  source constraints, and verification expectations;
- no more than eight created issues per assessment;
- a linked issue's positive number and canonical URL exactly match the supplied inventory;
- dismiss and defer carry their required closed fields; and
- no decision mixes created/linked IDs or embeds lifecycle state.

The planner performs semantic grouping and comparison. There is deliberately no deterministic
title fingerprint or embedding dedupe. The `review-followup` provenance label, one closed type
label, and impact-derived `priority:high|medium|low` label are computed by Quorum, not the model.

## Retry and failure budgets

Follow-up assessment copies decomposition's independent budgets:

- at most three semantic proposal rejections; and
- at most three provider/protocol/sandbox failures.

A semantic rejection resumes from a fresh session with the immutable lineage, artifact membership,
and bounded structured rejection summaries. A provider failure enters bounded backoff, then starts a
fresh session. Provider failures do not consume proposal attempts. No model downgrade or fallback is
permitted beyond normal configured planner routing.

Exhaustion moves the assessment to `held`, clears `active`, retains the final bounded reason, and
emits a health alert. Artifacts remain undisposed and the originating delivery remains terminal.
There is no manual retry command in this feature.

## Atomic plan staging and daemon issue application

The daemon parses and validates the whole response before opening the write transaction. One
`BEGIN IMMEDIATE` transaction then:

1. rechecks assessment state, active authority, scope, and complete immutable membership;
2. validates every decision again and rechecks exact inventory identity for links;
3. writes one durable decision intent per planner decision and exact intent/artifact memberships;
4. stores the canonical complete plan on every intent so only a byte-equivalent semantic replay is
   an idempotent success; and
5. commits without any network call.

Link, dismiss, and defer intents are immediately complete. Create intents enter a bounded outbox.
For each create intent the daemon, outside every DB transaction, searches GitHub for its stable
`quorum-review-followup:<assessment>:<ordinal>` body marker, ensures the `review-followup` label,
and creates the issue through `gh issue create` only when no marker match exists. It then records
the positive issue number and canonical URL in a second short transaction. Failures back off and
hold after three attempts. When all intents are complete, one transaction marks every contributing
batch `resolved` and the assessment `completed`. This preserves durable create/link/dismiss/defer
decisions without giving the planner external mutation authority.

## Scheduling and recovery

Follow-up work is background portfolio maintenance:

1. merge/review/rework/approval lifecycle;
2. decomposition needed to admit source work;
3. collector retries required to complete interpretation;
4. follow-up assessment and issue outbox.

It consumes planner capacity only during a provider turn and never reserves worker/reviewer slots.
The daemon runs at most one follow-up planner turn at a time. It may coexist with ordinary managed
delivery but not with an active decomposition planner turn in the same process.

On restart:

- `pending` resumes normally;
- `planning` with no live matching process is charged one provider failure and enters backoff;
- `provider-backoff` waits until eligible;
- `completed` is inert;
- `planning` with staged intents resumes only the issue outbox;
- marker lookup recovers an issue created before a crash that preceded the local completion write;
- inconsistent membership or missing source lineage fails loudly and creates nothing.

No network/model call occurs inside a database transaction. Reads open and close their connection
per reconciliation tick. Provider calls and repository archive creation are bounded and
cancellation-safe; shutdown kills/reaps the planner without applying partial output.

## Inspection

The SQLite source of record retains immutable batches/artifacts, sealed assessment membership,
bounded failure counters and hold reasons, every decision intent, exact artifact membership,
idempotency markers, and created/linked issue number/URL. Existing bounded core batch/artifact
reads remain available; a broader human-facing inspection command is not introduced by this
activation. No artifact or assessment mutation command is added. Full prompts and model
transcripts are never persisted.

## Design integration

The main design's review responsibility and severity contract changes from “serious concrete class
implies BLOCKING” to two-axis classification. Its collector section changes from analytics-only to
retrospective interpretation with two outputs: replaceable analytics and immutable follow-up
artifacts. Retrospective means it never changes the originating review, merge, task, or graph; it
does not mean its artifacts can never produce separate future work.

## Implementation seams

1. **Core artifacts:** schema migration, closed types, atomic successful interpretation write,
   artifact/batch reads, and inspection projections.
2. **Collector:** prompt/protocol extension, evidence validation, artifact limits, and current-
   generation zero-artifact markers.
3. **Assessment core:** eligibility, immutable membership, guarded claims, independent budgets,
   and recovery.
4. **Planner boundary:** restricted follow-up prompt/response turn, closed core parser, exact issue
   inventory validation, and no-session-continuation behavior.
5. **Daemon orchestration:** ordinary/graph reconciliation, scheduling priority, shutdown/restart,
   durable issue intents, marker-based GitHub recovery, and completion.
6. **Reviewer calibration:** all R1/R2/rereview prompt variants and tests for scope, threat-model,
   impact, disposition, and zero-blocker-with-follow-ups approval.

Activation is prospective: no historical PR, batch, artifact, assessment, or issue intent is
backfilled. Only immutable batches produced by the current collector generation are admitted.

## Required evidence

- Fresh-schema and populated previous-version migration tests.
- Collector parser tests for every enum, evidence mismatch, artifact bound, zero artifacts, and
  atomic preservation of a prior successful batch on failure/re-interpretation.
- Real SQLite repeated concurrent processes proving one assessment/membership per scope and no
  duplicate staged intents.
- Fault injection around intent staging, external issue creation, and local completion proving
  marker-idempotent recovery and no partial plan rows.
- Ordinary-task, completed-graph, cancelled-partial-graph, zero-artifact, waiting-interpretation,
  and collector-exhaustion lifecycle tests.
- Restart tests for pending, planning, backoff, held, completed, and inconsistent assessment state.
- Planner closed-protocol, prompt/output/timeout, tool-free isolation, and provider-binary argument
  tests, plus an end-to-end restricted-turn-to-GitHub-outbox test.
- Reviewer prompt contract tests proving technical severity does not itself force BLOCKING and
  follow-ups do not increase `--blocking`.
- Full `rtk proxy ./preflight.sh` before submission.
