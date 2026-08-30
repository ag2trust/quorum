---
name: quorum
description: Coordinate work through the local Quorum daemon and CLI. Use when creating or inspecting managed tasks, checking daemon health, messaging agents, or acting as a daemon-managed worker or reviewer.
---

# Quorum

Quorum owns managed implementation, review, and merge. Identify your role before acting.

## Managed worker or reviewer

Follow the spawn prompt: it defines the role, task, repository, and assigned worktree. Work
only there. Do not claim or select tasks, create successor tasks, impersonate another agent,
merge, or manually change lifecycle state.

Write only inside the assigned managed worktree and repository. You may read another repository
as context, but a sibling or outside-repository change is not a managed deliverable. Leave it
to the owner interactively, or create a separately scoped task for the repository that owns it.

The same boundary applies inside a decomposition graph: if the task cannot compile or pass
preflight without editing files its non-goals reserve for a sibling task, the plan is wrong.
Signal `blocked` with that contradiction instead of expanding scope. An out-of-boundary
delivery is a graph-blocker: it fails the task, ends the review, and freezes the entire graph.
Never submit with a failing preflight; report the failure instead.

If blocked, failed, or awaiting input, signal the daemon:

```sh
quorum react --agent <You> --task-id <N> --state blocked|failed|needs-info
```

Finish only with the completion command in the prompt. The daemon alone transitions lifecycle,
posts formal reviews, and merges.

## Target the correct repository

Each repository has its own database. Do not assume the current directory selects the intended
daemon when checkouts or daemons coexist. For commands that support it, pass `--repo owner/name`:

```sh
quorum task-create --repo owner/name --created-by <You> --title "<outcome>" --body-stdin
QUORUM_REPO=owner/name quorum task-get --task-id <N>
```

Otherwise set `QUORUM_REPO=owner/name` for that invocation. The daemon sets it for managed
agents. Confirm the exact flags with `quorum <command> --help` before acting.

## Interactive coordinator

Investigate and scope unclear work yourself. Create only execution-ready tasks with observed
and expected behavior, relevant paths, constraints, and verification. Interactive coordinators
create, inspect, cancel, and recover work; they do not claim tasks, impersonate managed agents,
call `submit`, grant lifecycle authority, or merge.

Interactive coordinators never start, restart, supervise, stop, signal, or otherwise control
`quorum serve`. They may inspect daemon health and durable task state with short-lived CLI
commands. If the daemon is unavailable or requires an operational change, report the evidence
and hand the action to a separately designated operator session or human owner. Never invoke
`quorum serve`, `scripts/serve-supervisor.sh`, or send process signals to the daemon from this
role.

### Choose the entry mode

New work starts from the configured base branch:

```sh
quorum task-create --created-by <You> --title "<outcome>" --body-stdin
```

Use an implementation continuation when a managed worker must continue an existing PR. The
daemon binds the task to the exact current PR head and runs its normal implementation/review/
merge lifecycle:

```sh
quorum task-create --created-by <You> --title "<outcome>" --continue-pr <PR> --body-stdin
```

Use a review-only task only when an existing delivery needs review and merge, not edits:

```sh
quorum task-create --created-by <You> --title "Review PR #<PR>" --review-pr <PR> --body-stdin
```

`--continue-pr` and `--review-pr` are mutually exclusive. A review-only task skips the initial
implementation worker. A changes verdict starts bounded daemon-managed remediation against the
existing PR, then returns to review; use a continuation when implementation work is needed from
the outset.

The classifier uses this shared complexity rubric:

- 1: Trivial — config tweak, typo fix, simple rename
- 2: Simple — single-file change, clear spec
- 3: Moderate — multi-file change, some design decisions
- 4: Complex — cross-cutting change, multiple components
- 5: Very complex — architectural change, new subsystem

Do not encode PR workflow or lifecycle authority in generic `refs`, titles, labels, or task
body. In particular, this shipped CLI has no public successor-task creation interface for
durable `source_task` provenance. Do not manufacture `refs.source_task`; `--continue-pr` alone
preserves a PR but does not create automatic provenance-backed recovery discovery. A named,
evidence-gated `decomposition-adopt-recovery` remains available for the exact pair.

### Recover without losing authority or PR work

First inspect the task and lifecycle log:

```sh
quorum task-get --task-id <N>
quorum log --refs task#<N>
```

Use `task-retry` only when the daemon has parked the task after a bounded failure or the task is
provider-blocked. It resumes the same task and preserves its PR, branch, dependencies, and
rework context; it is not a general retry button for terminal failures:

```sh
quorum task-retry --task-id <N> --by <You>
```

Retrying a failed generated graph child whose graph is blocked with `generated-child-failed`
also reactivates that graph in the same transaction; held siblings resume on the next tick.

A graph blocked by a reviewer graph-blocker hold (for example `boundary-violation`) is not
retryable and no CLI reactivates it directly. The supported recovery is to cancel the source
task, which atomically cancels the graph and its children, then create one replacement task
that owns the combined scope. Cancellation requires the source's creator or assignee identity
and, for a decomposed source, `--expected-revision`:

```sh
quorum task-update --agent <creator> --task-id <source> --status cancelled \
  --expected-revision <rev> --note-file <why>
quorum task-create --created-by <You> --continue-pr <PR> --title "<combined outcome>" --body-stdin
```

Cancelling deletes the cancelled children's remote branches, which closes their PRs. If the
replacement continues from a child's PR, restore that branch and reopen the PR before the
daemon attempts publication (or before creating the `--continue-pr` task).

For a failed generated graph child, do not infer equivalence from matching text or a shared PR.
Only after the exact managed continuation is completed and merged may an operator adopt that
named pair. The command rechecks final-child graph membership, repository, PR, head, managed
worker/reviewer, and merged-completion evidence. `source_task` provenance must agree when
present, but may be absent because the operator names the exact child and recovery task:

```sh
quorum decomposition-adopt-recovery \
  --original-child-id <failed-child> --recovery-task-id <merged-continuation> --by <You>
```

This is a one-pair, evidence-gated recovery operation, not a way to create a continuation or
skip review. The missing public successor interface does not block this explicit path; do not
bypass the guard by inventing refs.

Use `task-close` only for a documented manual resolution: work merged by hand, fixed elsewhere,
or obsolete (including a failed task whose PR later landed). Supply a durable reason. It records
a manual close, never substitutes for managed review and merge:

```sh
quorum task-close --agent <You> --task-id <N> --reason-file <path>
```

If implementation moves outside an existing Quorum task, cancel that task before external work
starts. When the external PR is ready, create a new `--review-pr` task.

## Operator

This is a separately designated daemon-operator role, not an interactive coordinator. Start the
manager with `quorum serve`, or `scripts/serve-supervisor.sh` when the managed repository needs
supervised self-update. Inspect with `quorum status`, `quorum task-list
--brief`, `quorum task-get --task-id <N>`, `quorum log --refs task#<N>`, and `quorum tail
<agent>`. Use `quorum kill` only for a stuck managed agent.

`quorum init` installs the embedded repository skill when it is missing and reports drift
without overwriting it. `quorum upgrade` publishes the embedded skill artifact; use
`quorum upgrade --check` to detect drift without writing. Put free text in stdin or files, not
shell arguments. Exit codes: 0 success, 1 expected negative, 2 bad input, 3 internal/DB failure.
