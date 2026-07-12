# Quorum agent skill — design

**Date:** 2026-07-12 · **Status:** approved by owner (conversation) · **Scope:** one new skill + install wiring

## Problem

Agents on a quorum-enabled machine don't know quorum exists unless the repo they sit in
happens to document it. Coordination knowledge today lives in per-repo instruction files,
which (a) only covers that repo, and (b) drifts from the CLI (see the stale
`parent-repo-integration.md`, which still referenced removed commands). The binary already
ships an authoritative, version-locked command surface (`quorum help`); what's missing is
the *when/why* layer that tells an agent quorum is there and how to behave on it.

## Decision

Ship a **`quorum` skill inside this repo** at `.claude/skills/quorum/SKILL.md`, installed
**globally** to `~/.claude/skills/quorum/` by `install.sh` and `dev-install.sh` alongside
the binary. Re-running the installer upgrades binary + skill in lockstep.

Division of labor (anti-drift):

| Layer | Owns |
|---|---|
| `quorum help` / `sync` | command surface — flags, exact syntax; versioned with binary |
| `quorum` skill | when/why — orientation, modes, lifecycle etiquette, tick loop |
| consuming repo's instruction file | policy — naming, labels, merge rules; overrides skill defaults |

The skill contains **no flag syntax** beyond a handful of load-bearing calls; everything
else defers to `quorum help`.

**Purity rule:** the skill is quorum-only. No consuming-project terms, policies, or
conventions may appear in it — only concepts that are quorum-native or agnostic
agent-workflow patterns (e.g. subagent delegation, periodic ticks — with no fixed
intervals or model/label policy, which belong to the consuming repo).

## Skill content (~1.5–2K tokens)

Frontmatter description triggers on: session start / onboarding, "what should I work on",
claiming/filing tasks, coordinating or messaging agents, checking the queue, any quorum
mention. Invocable as `/quorum`, so a periodic tick can be armed as a loop over the skill.

1. **What it is** — local agent-coordination substrate; one binary + one SQLite DB per
   repo (cwd-resolved); JSON output; branch on exit codes 0/1/2/3.
2. **Mode detection** — daemon-managed worker (spawned by `quorum serve`; identity,
   branch, worktree given in the spawn prompt; speak via `done`, `react`, `message`) vs
   passive/interactive (self-generate a unique name, check `status --agents` for
   collisions, drive yourself via `sync`).
3. **Orient** — one `quorum sync --agent <Name>` is the compass. Act on the payload in
   order: stop → halt; retire → sign off; `current_task` → resume; `next_task` → claim;
   directs/pins → handle; empty → idle.
4. **Work loop** — tick = sync + act. Drain: after finishing a task, re-sync immediately
   and claim the next; go idle only when nothing is claimable. Where the harness supports
   it, delegate task execution to a subagent so the coordinating context stays thin.
5. **Lifecycle etiquette** — `open → working → in-review → merging → done` with rework
   loop (cap 3). Claims are renewable leases that auto-renew on any `--agent` command;
   silence lapses the lease and the task returns to `open`. `done --pr N` submits;
   `task-update --status open` gives up cleanly; never self-review; set
   `--refs '{"pr":N}'` on submit (load-bearing for review traceability).
6. **Feed vs event log + free text** — `read`/`post` are agent-authored messages;
   `log` is system-emitted state changes; two streams, two cursors. Bodies never travel
   as flags — quoted heredoc on stdin or `--body-file`.
7. **Defer to `quorum help`** — the authoritative one-screen surface. The consuming
   repo's instruction file adds policy and wins on conflict.

## Install wiring

- `install.sh`: after installing the binary, fetch/copy `.claude/skills/quorum/` to
  `~/.claude/skills/quorum/` (release-mode: download from the same tag; overwrite).
- `dev-install.sh`: copy from the working tree.
- Uninstall/upgrade is idempotent overwrite; no per-repo vendoring.

## Also in this change

- Delete `docs/parent-repo-integration.md` (stale command surface, superseded by the skill).
- `pr-review` skill stays repo-local — it encodes this repo's review policy, not platform usage.

## Out of scope

- Consuming repos trimming their instruction files down to policy + a skill pointer
  (separate change per repo).
- Plugin/marketplace packaging (revisit if quorum goes public).

## Testing

- `install.sh`/`dev-install.sh` run on a clean HOME installs skill dir; re-run overwrites.
- Skill file lints: valid frontmatter, description present, no consuming-project terms
  (grep guard in preflight optional).
