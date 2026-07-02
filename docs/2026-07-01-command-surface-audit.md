# Quorum Command Surface Audit — Minimal Set Proposal

> **Context:** Owner is evolving quorum from coordination hub into a full agent orchestration
> system (git branch/worktree creation, agent invocation + params, post-run cleanup). This
> audit enumerates the current command surface, classifies each by real usage, and proposes
> a minimal set as the foundation for orchestration.
>
> **Deliverable:** Proposal only — no command removal. Follow-up tasks gated on owner approval.

## Methodology

Every subcommand from `quorum --help` was grepped across both repos (a few — `pin`,
`unpin`, `pins` — are only surfaced by `quorum --help` and not by the `quorum help`
cheat-sheet, so `--help` is the authoritative inventory source):
- `~/dev/quorum/{.claude/skills/,docs/,CLAUDE.md}` (quorum repo)
- `~/dev/ag2trust/{.claude/skills/,docs/,CLAUDE.md}` (parent repo)

"Hits" = lines containing `quorum <command>` as an invocation or instructional reference.
Prose mentions ("quorum handles X") are excluded unless they document a specific invocation.

## Full Command Inventory

### 1. `sync` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 27 | hub-onboard, work-loop, cto, review-and-merge, git-workflow, code-maintenance, CLAUDE.md |
| quorum | 3 | docs (design, sync-capstone-plan) |

**Rationale:** The single most-used command. Every agent tick starts with `sync`. It's the
compass — returns current_task, next_task, messages, events in one call. Non-negotiable.

**Orchestration note:** `sync` is the natural injection point for orchestration metadata
(suggested model, effort, agent-type per task).

### 2. `task-create` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 1 | hub-onboard (self-created tasks) |
| quorum | 2 | docs (design, review-as-task-plan) |

**Rationale:** Low hit count but structurally required — the CTO skill and any coordinating
agent must create tasks. Also auto-called internally by `task-update --status done` (review
task spawning). Essential for the orchestration layer (it will create tasks with
orchestration params).

### 3. `task-claim` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 13 | hub-onboard, work-loop, cto, review-and-merge, git-workflow, feature-owner |
| quorum | 7 | docs (design, review-as-task-plan, implementation-plan) |

**Rationale:** Core work-queue primitive. Atomic claim is load-bearing invariant #1. Every
agent claims via this. Highest cross-skill usage after `sync`.

### 4. `task-update` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 8 | work-loop, hub-onboard, git-workflow, review-and-merge |
| quorum | 5 | docs (design, review-as-task-plan) |

**Rationale:** Lifecycle transitions (done, open/release, cancelled), note appending, refs
linking, review verdicts. Absorbed the old `task-release` and `task-cancel` — already
consolidated. Essential.

### 5. `task-list` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 6 | cto (primary consumer — fleet queue view) |
| quorum | 2 | docs (design, review-as-task-plan) |

**Rationale:** The CTO skill uses `task-list --brief` heavily for queue visibility. Also
useful for debugging. Essential for any orchestration dashboard/status view.

### 6. `task-get` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 9 | work-loop, hub-onboard, review-and-merge, git-workflow |
| quorum | 2 | docs (design, review-as-task-plan) |

**Rationale:** Fetch full task body + notes after claim or resume. Every task execution path
calls this. Essential.

### 7. `post` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 27 | work-loop, hub-onboard, cto, review-and-merge, code-maintenance, feature-owner |
| quorum | 3 | docs (design, parent-repo-integration) |

**Rationale:** Agent-to-agent messaging. Used for sign-off announcements, direct asks,
review handoff notes, CTO broadcasts. High usage across all skills. Essential.

### 8. `read` — **CONSOLIDATE into `sync`**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 2 | hub-onboard (mentioned as fallback for strict at-least-once) |
| quorum | 3 | docs (design, parent-repo-integration) |

**Rationale:** `sync` already returns unread messages and auto-acks the cursor. The only
documented use of standalone `read` is hub-onboard mentioning it as an escape hatch for
"strict at-least-once" delivery — a theoretical need, never observed in practice. The
cursor-less `read` (without `--agent`) is an inspection/debug tool.

**Proposal:** Keep for now (low maintenance cost), but mark as internal/debug. Not part of
the agent-facing minimal set. Agents should use `sync` exclusively.

### 9. `log` — **CUT (unused)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Zero usage in any skill or doc across both repos. The event log is consumed
*through* `sync` (which includes scoped events). No skill or CLAUDE.md section references
`quorum log` as a command to run. The data is valuable (state-change audit trail) but the
standalone command has no callers.

**Proposal:** Cut from agent-facing surface. If orchestration needs event queries, add a
filtered view to `sync` or `status` instead of a separate command.

### 10. `stop` — **ESSENTIAL (owner/CTO only)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 4 | work-loop (halt condition), hub-onboard (sign-off trigger), CLAUDE.md |
| quorum | 0 | — |

**Rationale:** Emergency halt. Referenced in work-loop Phase 3a and hub-onboard sign-off.
The owner/CTO issues it; agents detect it via `sync.stop`. Low usage but critical safety
control. Essential.

### 11. `resume` — **ESSENTIAL (pairs with `stop`)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — (but implicitly required — clearing a `stop` needs `resume`) |
| quorum | 0 | — |

**Rationale:** Zero explicit references but structurally required as the counterpart to
`stop`. Without it, a `stop` is permanent. The work-loop's Phase 3a says "next tick
re-syncs and notices when the matching `resume` fires" — agents don't call it, but the
owner must be able to. Essential.

### 12. `stops` — **CONSOLIDATE into `status`**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Zero usage. `status --json` could include active stops. Dedicated command
adds surface area for no demonstrated need.

**Proposal:** Fold into `status` output (add a `stops` field to the JSON). Cut standalone
command.

### 13. `pin` — **CUT (unused)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Zero usage across both repos. Implemented (issue #78) but never adopted by
any skill. Standing notices could be useful in theory (e.g. "deploy freeze until Thursday")
but no skill reads `sync.pinned` or references pins.

### 14. `unpin` — **CUT (unused)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Counterpart to `pin`. Same zero usage. Cut together.

### 15. `pins` — **CUT (unused)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Read-only companion to `pin`/`unpin`. Same zero usage. Cut together.

### 16. `status` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 12 | cto (fleet monitoring), hub-onboard (collision check), CLAUDE.md |
| quorum | 5 | docs (design), CLAUDE.md |

**Rationale:** Read-only health snapshot. CTO uses it for fleet monitoring (`--agents` for
presence). Hub-onboard uses it to check for agent name collisions. `--watch` mode for
continuous monitoring. Essential.

### 17. `sweep` — **ESSENTIAL (ops)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 6 | docs (design — WAL checkpoint), CLAUDE.md |

**Rationale:** Housekeeping — reclaims expired rows + WAL checkpoint. Not called by agent
skills (it's an ops command), but required for DB health. Without it, WAL grows unbounded
(invariant #7). Essential for ops, not for agent-facing surface.

**Proposal:** Keep but classify as ops-only (not agent-facing).

### 18. `init` — **ESSENTIAL (ops)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 1 | CLAUDE.md (quorum PR merge procedure) |
| quorum | 8 | docs (design, implementation-plan), CLAUDE.md |

**Rationale:** Creates DB + runs migrations. Idempotent. Required after every schema change.
CLAUDE.md mandates `quorum init` after merging quorum PRs. Essential for ops.

### 19. `reset` — **CUT (dangerous, unused)**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 0 | — |

**Rationale:** Wipes ALL state. Zero usage in any skill. Dangerous — requires `--yes` but
still destroys the entire coordination state. Only conceivable use is local dev debugging.

**Proposal:** Cut from agent-facing surface. If needed for dev, keep as a hidden/undocumented
command.

### 20. `help` — **ESSENTIAL**

| Repo | Hits | Callers |
|------|------|---------|
| ag2trust | 0 | — |
| quorum | 5 | docs (design), CLAUDE.md |

**Rationale:** Agent self-orientation. Referenced in CLAUDE.md as the way agents discover
commands. Essential for discoverability, especially post-trim.

## Proposed Minimal Set

### Agent-facing (8 commands)

| Command | Role |
|---------|------|
| `sync` | Tick compass — tasks, messages, events, presence |
| `task-create` | Create work items |
| `task-claim` | Atomic claim (or auto-pick highest priority) |
| `task-update` | Lifecycle transitions, notes, refs, verdicts |
| `task-list` | Queue visibility (CTO, debugging) |
| `task-get` | Full task body + notes |
| `post` | Agent-to-agent messaging |
| `help` | Self-orientation cheat-sheet |

### Owner/CTO-only (3 commands)

| Command | Role |
|---------|------|
| `stop` | Emergency halt (global or per-agent) |
| `resume` | Clear a halt |
| `status` | Fleet health + agent presence |

### Ops-only (2 commands)

| Command | Role |
|---------|------|
| `init` | DB creation + migration |
| `sweep` | Expired-row reclamation + WAL checkpoint |

### Cut (5 commands)

| Command | Verdict | Evidence |
|---------|---------|----------|
| `log` | Cut | 0 hits across both repos. Events consumed via `sync`. |
| `pin` | Cut | 0 hits across both repos. Never adopted. |
| `unpin` | Cut | 0 hits. Counterpart to unused `pin`. |
| `pins` | Cut | 0 hits. Read-only companion to unused `pin`/`unpin`. |
| `reset` | Cut | 0 hits. Dangerous wipe-all; dev-only if kept. |

### Consolidate (2 commands)

| Command | Into | Rationale |
|---------|------|-----------|
| `read` | `sync` | `sync` already delivers messages. `read` is escape-hatch only; mark internal/debug. |
| `stops` | `status` | Active stops should be a field in `status --json` output. |

**Resulting surface: 13 commands (8 agent + 3 control + 2 ops), down from 20.**

## Must-Keep Skill-Callers

Commands that, if removed or renamed, would break a shipping skill:

| Command | Skills that invoke it |
|---------|-----------------------|
| `sync` | hub-onboard, work-loop, cto, review-and-merge, git-workflow, code-maintenance |
| `task-claim` | hub-onboard, work-loop, cto, review-and-merge, git-workflow, feature-owner |
| `task-update` | work-loop, hub-onboard, git-workflow, review-and-merge |
| `task-get` | work-loop, hub-onboard, review-and-merge, git-workflow |
| `post` | work-loop, hub-onboard, cto, review-and-merge, code-maintenance, feature-owner |
| `task-create` | hub-onboard |
| `task-list` | cto |
| `status` | cto, hub-onboard |
| `stop` | work-loop (detection via sync), hub-onboard (sign-off) |

## Orchestration Gaps

Capabilities the future orchestrator needs that no current command covers:

### 1. Agent invocation + lifecycle management
No command spawns, monitors, or terminates an agent process. Today this is the harness's
job (Claude Code's `Agent` tool). The orchestrator needs:
- `quorum agent-spawn --task-id <n> [--model <m>] [--effort <e>] [--type <t>]` — launch an
  agent with specific params, bound to a task
- `quorum agent-stop --agent <id>` — graceful termination signal (vs `stop` which is a
  coordination halt, not a process kill)

### 2. Git branch/worktree lifecycle
`task-claim` returns `suggested_branch` and `suggested_worktree` but doesn't create them.
Skills contain the `git worktree add` / `git worktree remove` boilerplate. The orchestrator
needs:
- `quorum worktree-create --task-id <n>` — create branch + worktree from the task's
  suggested names, handling the fresh-vs-rework fork
- `quorum worktree-cleanup --task-id <n>` — remove worktree after task completion

### 3. Post-run cleanup
No command handles the "task finished, clean up resources" flow. Today agents manually:
remove worktrees, delete branches (GitHub does this on merge), clear busy semaphores. The
orchestrator needs a `task-complete` or hook system that runs cleanup on `done`.

### 4. Task params for orchestration
`task-create` accepts `--labels` and `--refs` but has no structured fields for:
- `model` (which model to use)
- `effort` (reasoning effort level)
- `agent_type` (e.g. "code-reviewer", "Explore")
- `timeout` (max wall-clock for this task)

These could go in `--refs` JSON (flexible) or as first-class fields (typed, queryable).

### 5. Batch/pipeline operations
No command creates multiple linked tasks atomically. The CTO today creates tasks one by one
with `--depends-on` wiring. An orchestrator needs:
- `quorum pipeline-create` — create N tasks with dependency chain in one transaction
- Or: extend `task-create` to accept a batch JSON on stdin

### 6. Result capture
When an agent finishes a task, the result (PR number, summary, error) goes into
`task-update --note-stdin`. There's no structured result field. The orchestrator needs a
typed result (success/failure + output) for conditional downstream dispatch.
