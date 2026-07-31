---
name: quorum
description: Coordinate work through the local Quorum daemon and CLI. Use when creating or inspecting managed tasks, checking daemon health, messaging agents, or acting as a daemon-managed worker or reviewer.
---

# Quorum

Quorum runs a local, daemon-managed implementation → review → merge pipeline. Identify your
role before using it.

## Managed worker or reviewer

Your spawn prompt is authoritative. It contains your task, role, worktree, and completion
command. Work only on that assignment; do not discover, claim, release, or select tasks.
Do not mark tasks done or merge PRs yourself.

If you are blocked, failed, or need input:

```sh
quorum react --agent <You> --task-id <N> --state blocked|failed|needs-info
```

## Interactive coordinator

Investigate and scope unclear work yourself. Send Quorum execution-ready tasks with an
observed outcome, expected outcome, relevant paths and constraints, and verification.

Choose one entry mode:

```sh
# New implementation from the configured base branch
quorum task-create --created-by <You> --title "<outcome>" --body-stdin

# Continue implementation from the exact current head of an existing PR
quorum task-create --created-by <You> --title "<outcome>" --continue-pr <N> --body-stdin

# Existing implementation only needs review and merge
quorum task-create --created-by <You> --title "Review PR #N" --review-pr <N> --body-stdin
```

`--continue-pr` and `--review-pr` are mutually exclusive. Generic `refs` are metadata and
do not select a PR workflow. The daemon chooses complexity, model, and effort; do not pass
`complexity:*`, `tier:*`, or `effort:*` labels.

A review-only task has no implementation worker. If it receives a changes verdict, the
task fails and the outside PR author remains responsible for updating the branch.

Interactive sessions create, inspect, and cancel work. They do not claim tasks, impersonate
managed agents, call `submit`, or manually set a task to `done`.

If implementation moves outside an existing Quorum task, cancel that task before external
work starts. When the external PR is ready, create a new `--review-pr` task. This prevents
the daemon from starting a second implementation for the same outcome.

## Operator

Start the manager with `quorum serve`, or `scripts/serve-supervisor.sh` when the managed
repository needs supervised self-update. The useful inspection commands are:

```sh
quorum status [--json]
quorum web
quorum task-list --brief
quorum task-get --task-id <N>
quorum log --refs task#N
quorum tail <agent>
```

Use `quorum kill` only to terminate a stuck managed agent. Use `quorum <command> --help`
for exact flags and `quorum help` for a short workflow reminder.

Feed messages and lifecycle events are separate: `post`/`read` are agent-authored messages;
`log` contains state changes. Put free text in stdin or files, not shell arguments.

Exit codes: `0` success · `1` expected negative · `2` bad input · `3` internal/DB failure.
