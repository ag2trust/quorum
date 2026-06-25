# Integrating Quorum into the agent workflow (draft snippet)

This is a **proposed** replacement for the GitHub-issue hub (#1455) coordination block in the
parent project's `CLAUDE.md`. It is staged here for review; adopting it is a separate,
owner-approved change to the parent repo (not done by building Quorum).

> Migration posture: run Quorum **alongside** the gh hub first (dual-write claims/status),
> confirm agents adopt it, then retire the #1455 conventions. PRs/code-review stay on GitHub.

---

## Agent coordination via Quorum (replaces the #1455 hub)

Coordination runs through the local `quorum` CLI (`quorum help` for the full cheat-sheet;
`help-agent` is a back-compat alias).
It is a single binary + `~/.quorum/quorum.db`; no daemon. Output is JSON; branch on exit codes
(`0` ok · `1` didn't-get-it/not-holder · `2` usage · `3` internal).

**Presence** is implicit — any write bumps your `last_seen`; `quorum roster` shows who's active.

**Claim before hub-visible work** (replaces the post→wait→rescan→tiebreak semaphore):

```bash
quorum claim --agent <Name> --target pr#2459 --ttl 45m   # exit 0 = yours; exit 1 = {holder} has it
# ... do the work, renewing on long tasks ...
quorum renew  --agent <Name> --claim-id <n> --ttl 45m    # ~every 15 min
quorum release --agent <Name> --target pr#2459           # when done
```

The claim is atomic — no 10-second wait, no comment-ID tiebreak. Exactly one winner.

**Work queue** (replaces `cto:agent-ready` issues):

```bash
quorum task-claim --agent <Name>                         # highest-priority open task, atomically
quorum task-update --agent <Name> --task-id <n> --status in_progress
quorum task-update --agent <Name> --task-id <n> --status done
```

**The feed** (replaces hub comments) — cheap delta polling, auto-expiring:

```bash
quorum read --agent <Name>                               # what's new since you last looked
quorum read --agent <Name> --ack-through <seq>           # mark handled (advances your cursor)
quorum post --agent <Name> --kind request --body-stdin <<'EOF'
need a reviewer on pr#2459
EOF
```

Messages auto-expire (default 48h) — **no manual pruning**. `quorum status` is the at-a-glance
health view.

**Why this replaces the old pain:** writes are local (no `gh` round-trip), TTL means nothing
accumulates, reads are deltas (not "last N comments" re-reads), and claims are a single atomic
call instead of a racy multi-step dance.
