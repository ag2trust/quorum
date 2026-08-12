# Quorum self-update: drain-on-self-merge + supervisor

**Status:** Draft — pending owner approval
**Date:** 2026-07-03
**Author:** Sable-k42 (Claude Fable 5)
**Depends on:** daemon M0–M7 (all merged), #153 strike counter, #155 branch cleanup, #156 merge-state verification

## Motivation

The daemon can now fix its own bugs end-to-end (observed live 2026-07-03: PRs #153/#154/#155 authored, reviewed, and merged autonomously) — but every merged improvement only takes effect after a **manual** rebuild + restart of the daemon binary (today a CTO standing duty). This closes the loop: when quorum's own main advances, the daemon drains, exits, and an outer supervisor rebuilds and relaunches it. Merged improvements go live in minutes with no human in the path, and the CTO manual-rebuild duty is retired.

A daemon cannot safely rebuild and replace itself in-place; the design therefore splits into a **smart inner drain** (daemon) and a **dumb outer supervisor** (shell wrapper). The supervisor stays trivial on purpose: it is the component that must never break.

## Component 1 — daemon drain mode (`--self-update-drain`)

### Trigger

Two detection paths, either sets `draining = true`:

- **A (primary, free):** immediately after the merge executor reports success for a task whose `refs.repo` equals the daemon's own repo (config `--self-repo <owner/name>`; default: derived from `--repo-dir`'s `origin` remote).
- **B (catch-all):** per-tick poll of `origin/main`'s sha via `git ls-remote` (NOT `gh api` — no rate-limit exposure), throttled to once per 60s. Catches human merges and merges by other daemons.

### Drain behavior

Once draining:

1. Stop pulling new tasks from the queue (claimable tasks remain queued for the next daemon generation).
2. **Shallow drain** of in-flight agents: let each agent finish its *current turn*, then tear it down through the existing teardown paths (worktree removed, branch deleted, name returned). Do NOT wait for full work→review→merge lifecycles — M7 journal recovery is the load-bearing mechanism here: awaiting-review PRs live on GitHub, the journal records lifecycle state, and the next daemon generation resumes them. This bounds drain time to one turn (~minutes), not one lifecycle (~tens of minutes).
3. **Drain timeout** (config `--drain-timeout-secs`, default 900): if the roster has not emptied, SIGTERM remaining children and proceed — M7 recovery makes this safe by construction.
4. When the roster is empty: log `DRAIN: exiting for self-update -> <new-sha>` and **exit with code 75** (chosen to avoid collision with existing exit-code contract; verify against the documented exit-code invariants before implementation and adjust if taken).

### Debounce

`draining` is idempotent — N merges during one drain window produce one restart. The supervisor re-checks the sha before relaunch, so improvements that landed mid-drain are included in the same rebuild.

### Build-SHA drain shutdown matrix

The SHA poll is a drain *request*, never an in-tick lifecycle transition. The
tick loop owns the transition before its next dispatch, and the source controls
the eventual exit code.

| SHA-poll outcome / live state | Daemon action | Process exit | Regression coverage |
|---|---|---:|---|
| Build SHA A equals origin/main A; idle | Keep serving | none | `build_sha_matching_origin_does_not_drain` |
| Build SHA A, origin unavailable; idle | Warn and keep serving | none | `unreachable_origin_logs_warning_and_daemon_keeps_running` |
| Build SHA A, origin/main advances to B; no live slots | Complete the empty self-update drain | 75 | `build_sha_advance_drains_and_exits_75` |
| Build SHA A, origin/main advances to B; active worker and queued task | Stop new claims, tear down the drained/expired worker, leave queued work recoverable | 75 | `drain_timeout_force_kills_and_exits_75` |
| Signal drain while merge checks are waiting | Interrupt the wait and drain without requesting a rebuild | 0 | `drain_timeout_honored_during_merge_checks` |

Exit **75** is the explicit daemon-to-supervisor handoff: it means
rebuild-and-relaunch. A signal drain exits 0, and every other exit code is
propagated by the supervisor without a rebuild.

## Component 2 — supervisor (`scripts/serve-supervisor.sh`)

A ~40-line shell loop, deliberately dumb:

```
while true; do
  quorum serve "$@"; code=$?
  case $code in
    75)
      git -C "$REPO_DIR" fetch origin main
      if ./dev-install.sh; then
        continue          # relaunch new binary
      else
        alert "self-update build FAILED — relaunching OLD binary"
        continue          # old binary keeps serving; no rebuild retry until sha changes again
      fi ;;
    *) exit $code ;;      # normal stop / crash: propagate, do not loop
  esac
done
```

Requirements:

- **Build-failure fallback:** `dev-install.sh` already verifies the built binary (version, commands, schema). On any failure the supervisor relaunches the *existing* installed binary and alerts loudly. Broken main must never take the fleet manager down.
- **Thrash guard:** max 6 restarts per hour; beyond that, alert and hold with the current binary until manually cleared.
- **Schema safety:** if the new binary's expected schema is ahead of the live DB, `dev-install.sh` verification fails → fallback path above. Schema-bumping merges therefore park loudly instead of half-migrating; the alert is the signal for a supervised upgrade.
- **Crash exits (non-75) do not rebuild.** A crash is not an upgrade signal; systemd/launchd-style auto-restart of crashes is out of scope here (M7 + manual relaunch covers it today).

## Failure modes

| Failure | Behavior |
|---|---|
| Broken main (build/verify fails) | Old binary relaunched, loud alert, no retry until sha changes |
| Schema mismatch | Same as above (verify step catches it) |
| Drain never empties | Timeout → SIGTERM children → M7 recovery next generation |
| Merge storm | Debounced to one drain; sha re-checked at relaunch |
| Supervisor itself dies | Daemon dies with terminal/session as today — no regression vs. status quo |

## Non-goals

- In-place binary swap (`exec`-style, nginx-like upgrades) — complexity without benefit at cap≤4.
- Coordinating multiple concurrent daemons.
- Updating the `claude` CLI or other host tooling.
- Auto-restart on crash (distinct concern; unchanged).

## Rollout

1. Daemon drain mode behind `--self-update-drain` (default off). Integration-tested with `fake-agent`: seed self-repo merge event → roster drains → exit 75; negative path: non-self-repo merge does NOT trigger drain.
2. Supervisor script + shell test for the build-failure fallback (stub `dev-install.sh` that fails).
3. First live run supervised (owner watching), then becomes the default way `quorum serve` is launched. CTO standing duty "rebuild binary on quorum merges" is retired in the same change (update CLAUDE.md / operating rules).

## Testing (per repo test-quality bar)

- Unit: trigger detection (A: self-repo merge sets draining; negative: other-repo merge does not), debounce, sha-poll throttle.
- Integration: drain empties roster then exits 75; drain-timeout path force-terminates and still exits 75; queued tasks remain claimable across a restart (journal recovery).
- Supervisor: exit-75 → rebuild → relaunch; build-failure → old-binary relaunch + alert; non-75 exit → propagate.
