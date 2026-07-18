---
name: quorum
description: Coordinate work through the local Quorum daemon and CLI. Use when creating or inspecting managed tasks, checking daemon health, messaging agents, or acting as a daemon-managed worker or reviewer.
---

# Quorum — managed agent coordination

Quorum is a local coordination substrate: one `quorum` binary, one SQLite database per
repo, and `quorum serve` managing implementation, review, rework, and merge. There is no
network coordination service or auth layer. CLI commands are short-lived; the daemon is
the long-lived lifecycle manager.

## First: identify your role

**Daemon-managed worker or reviewer.** Your spawn prompt already contains the task, role,
agent name, worktree, and branch. Work only on that assignment. Do not discover, claim,
release, or select tasks yourself.

- Worker finished: `quorum submit --agent <You> --pr <N>`.
- Reviewer verdict: `quorum submit --agent <You> --pr <N> --verdict approved --blocking 0`
  or `--verdict changes --blocking <N> --feedback "..."`.
- Blocked, failed, or needing input: `quorum react --agent <You> --task-id <N> --state <state>`.
- Never review your own work or set a task to `done`; the daemon owns lifecycle transitions
  and merge.

**External or interactive caller.** You may create and inspect work, post/read messages,
and check health. Do not use the legacy passive-agent workflow (`sync`, `task-claim`, manual
release, or external `submit`). The daemon selects and provisions managed agents.

Own ambiguity before dispatch. Interactive callers handle production access, open-ended
troubleshooting, incident diagnosis, feature design, architectural planning, and task
scoping. Use narrowly bounded discovery tasks for support when useful, but do not ask a
managed worker to own those activities.

**Operator.** Start the manager with `scripts/serve-supervisor.sh` (recommended for
self-updating repos) or `quorum serve`. Use `quorum status` for health and `quorum kill`
only for emergency termination.

## Create the right kind of task

Dispatch only execution-ready work. Do not create a fix task from a reported symptom
alone. First confirm the issue or gather enough evidence to state the observed and
expected behavior, affected path, proposed remediation, constraints, and verification
criteria.

Implementation is still needed:

```sh
quorum task-create \
  --created-by <You> \
  --title "Implement <outcome>" \
  --labels '["complexity:2"]' \
  --body-stdin <<'EOF'
Describe the desired outcome, constraints, and verification.
EOF
```

The implementation already exists in PR #N and only review/merge is needed:

```sh
quorum task-create \
  --created-by <You> \
  --title "Review and merge PR #N" \
  --review-pr <N> \
  --labels '["complexity:1","type:review"]' \
  --body-stdin <<'EOF'
Review the existing implementation and drive it through merge.
EOF
```

`--review-pr` starts directly in `in-review` and does not provision an implementation
worker. The outside PR author remains responsible for pushes requested by reviewers. A
changes verdict fails the review-only task because Quorum has no managed worker to perform
rework; a merge conflict may leave it waiting for the outside author to update the PR.

For either path, include a clear body, `complexity:1-5`, and `--depends-on '[...]'` when
work must wait. Use the shared rubric:

- 1: Trivial — config tweak, typo fix, simple rename
- 2: Simple — single-file change, clear spec; < 15 min agent work
- 3: Moderate — multi-file change, some design decisions; 15-30 min
- 4: Complex — cross-cutting change, multiple components; 30-60 min
- 5: Very complex — architectural change, new subsystem; > 60 min

## Observe and communicate

- `quorum status [--json]` — daemon and queue health.
- `quorum task-list --brief` / `quorum task-get --task-id <N>` — queue and full task.
- `quorum log --refs task#N` — lifecycle events.
- `quorum post ...` / `quorum read ...` — durable agent-authored feed messages.
- `quorum pins` — standing context.
- `quorum help` — shipped command cheat-sheet; use `quorum <command> --help` for exact flags.

Feed messages and lifecycle events are separate streams: use `read` for what agents said
and `log` for what changed. Free-text bodies go through `--body-stdin` or `--body-file`,
never a shell flag.

Exit codes are stable: `0` success · `1` clean negative result · `2` usage/bad input ·
`3` internal/DB error.
