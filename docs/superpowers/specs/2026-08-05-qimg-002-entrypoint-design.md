# QIMG-002 — Container entrypoint (serve + web supervision)

Date: 2026-08-05
Status: approved design, pre-implementation
Target: public Quorum image; PR against `develop`.

## Problem

The merged QIMG-001 image (`Dockerfile`, PR #504) ships a diagnostic default only
(`CMD ["quorum", "--help"]`). It does not run the daemon or dashboard. QIMG-002 replaces
that default with one minimal PID-1 entrypoint that runs `quorum serve` and the read-only
`quorum web` for one configured repository, behaves correctly as PID 1, and fails loudly
when a required child fails — without hiding crashes or ever running two live daemons.

## Constraints (from HANDOFF.md and repo invariants)

- Quorum must not know it runs inside Hosted. No Rust changes to the binary.
- One Quorum monolith per container: no scaling, no inter-Quorum communication, no
  clustering, no service discovery.
- No auto-restart inside the container. Any child exit ends the container.
- Web stays loopback-only.
- Use documented `/data` paths.
- Preserve daemon exit 75 exactly (self-update drain AND SchemaTooNew both surface 75).
- No credentials, no hosted auth/MCP/gateway/public-port publication in this image.

## Verified facts (traced 2026-08-05 against `develop` @ 3790880e)

- `quorum serve` exit codes: normal SIGTERM/SIGINT drain → 0; **self-update drain → 75**
  (`EXIT_SELF_UPDATE`, `quorum/src/serve/mod.rs:1898`); a schema bump seen by the
  **already-running** tick loop (real self-update) → 75 (`mod.rs:2626`, `:3685-3709`);
  another live daemon holds the lock → 2 (Usage). `main.rs:1905` does
  `process::exit(code)` — propagates unchanged.
- **Schema-too-new at STARTUP is exit 3, NOT 75.** `run_serve` opens the DB to take the
  daemon lock (`mod.rs:1917` `db::open`) before the tick loop; a too-new DB raises
  `SchemaTooNew` there, and `QuorumError::SchemaTooNew.exit_code() == 3` (`quorum-core/src/error.rs:38`
  `_ => 3`, asserted `error.rs:55`). The `→75` mapping is tick-loop-only. A container
  booting against a too-new DB therefore exits 3. 75 requires a daemon that started
  cleanly and later detects a bump — not reproducible by a plain cold-start smoke test.
- Consequence for this design: the supervisor's guarantee is **propagate serve's exact
  code verbatim (0 / 1 / 2 / 3 / 75)**, never remap. The orchestrator distinguishes causes
  by code + log, not by assuming 75.
- serve installs SIGINT + SIGTERM handlers only (`mod.rs:3298-3301`); first signal drains,
  second forces. No SIGHUP.
- `--self-update-drain` is opt-in; base-sha self-update drain only triggers with the flag.
  SchemaTooNew → 75 happens regardless of the flag.
- Single-daemon authority is the SQLite `daemon_lock` row, not a file
  (`quorum-core/src/daemon_lock.rs`). Stale/dead locks self-heal in a 30s window.
- `quorum status` **always exits 0** (`quorum/src/main.rs:709`); it does not signal
  liveness via exit code. Liveness is a JSON field: `"daemon":"None"` vs
  `"daemon":{"Alive":{...}}` (`quorum-core/src/stats.rs:305-316`, `daemon_lock.rs:107`).
  The probe is `quorum status --json | grep -q '"Alive"'`. It opens/closes a connection —
  no held read.
- `quorum web` and `quorum status` take no repo flag; they resolve serve's per-repo DB via
  the `QUORUM_REPO` **env** (`paths::resolve_repo`, `paths.rs:26-27`). serve uses `--repo`.
  The three agree only because all inherit the same exported `QUORUM_REPO`.
- `quorum web` installs NO signal handlers (`web.rs:74`, plain `axum::serve`, killed by
  default disposition). Its "listening" line prints BEFORE `TcpListener::bind` (`web.rs:67`
  before `:71`) — not a true readiness signal. Real web readiness = the port accepting.

## Design

### PID-1 model

`tini` is PID 1, with the supervisor as the default *command* so `docker run image <cmd>`
still overrides it (keeps QIMG-001 `smoke.sh` green): `ENTRYPOINT ["/usr/bin/tini", "--"]`
+ `CMD ["/usr/local/bin/entrypoint.sh"]`.
tini reaps zombies (serve spawns agent/git/codex trees that reparent to PID 1 if serve
dies) and forwards the container's SIGTERM/SIGINT to the script. tini does NOT restart —
it exits with its child's code. `entrypoint.sh` owns the two children.

Rationale over bare `sh` as PID 1: dash reaps its direct children but is poor at reparented
orphans. Over a native `quorum supervise` command: that is a real future feature but
speculative until MCP's stay-alive shape exists — YAGNI for two processes. Revisit native
when MCP lands with a known process count and readiness/exit contract.

### Preconditions and initialization

- One active container owns each globally unique `owner/name` repository identity. Different
  identities resolve distinct coordination-state paths. Starting overlapping containers for
  the same identity is unsupported; the external runtime manager stops the prior container
  before starting its replacement.
- `/data/repos/project` must already be a git checkout of the managed repo. serve does NOT
  clone — `--repo-dir` is used directly for worktree provisioning and sha-polling. The
  orchestrator / mounted volume provisions it; the entrypoint asserts its presence and
  fails loud before initialization if absent.
- The entrypoint runs idempotent `quorum init` before either child. Although `db::open`
  can create/migrate the database, serve also requires the per-repository routing config
  that init writes. A first-run container therefore needs no external init step. An init
  failure starts no children and its exact exit code reaches the container boundary.

### Configuration (env, defaults on `/data`)

- `QUORUM_REPO` — required, `owner/name`. Unset → fail loud, nonzero, before starting
  children. **Exported** so web and status inherit it and resolve serve's DB.
- serve: `--repo-dir /data/repos/project`, `--worktree-base /data/worktrees`,
  `--log-dir /data/quorum/logs`, `--repo "$QUORUM_REPO"`.
- web: `--port ${QUORUM_WEB_PORT:-8080} --bind 127.0.0.1 --log-dir /data/quorum/logs`.
- `--self-update-drain` appended only when `QUORUM_SELF_UPDATE_DRAIN=1` (off by default).

### Startup + readiness

1. Run `quorum init` synchronously. On failure, exit with its exact code. On success,
   start `quorum serve …` → `$SERVE` and `quorum web …` → `$WEB`.
2. Readiness gate (bounded, ~30 × 1s). Each tick, in order:
   - if `kill -0 "$SERVE"` fails → serve exited during startup (schema-too-new exit 3,
     lock-held exit 2, DB error, etc.); `wait "$SERVE"`, SIGTERM web, `exit` with serve's
     **exact code** — do NOT synthesize a generic nonzero.
   - if `kill -0 "$WEB"` fails → web exited (e.g. bind failure); log, SIGTERM serve with
     bounded drain, `exit 1`.
   - if `quorum status --json | grep -q '"Alive"'` → daemon ready; break the gate.
   - else `sleep 1`, continue.
   - budget exhausted with serve still not Alive → SIGTERM both, `exit 1` (startup timeout).
3. Container tests prove web readiness with a real TCP connection, rather than its
   pre-bind log line. A bind failure exits web nonzero and is caught above.

### Steady state + shutdown

- `trap` on SIGTERM/SIGINT → forward SIGTERM to both children, then return to the wait
  loop.
- No `wait -n` (absent in dash / `/bin/sh`). POSIX wait loop:
  `while kill -0 "$SERVE" 2>/dev/null && kill -0 "$WEB" 2>/dev/null; do sleep 1; done` —
  exits when either child dies (or when a trapped signal interrupts `sleep`, re-checking
  liveness). Then identify the dead child via `kill -0` and reap its exact code with
  `wait "$PID"`.
  - serve died → SIGTERM web, bounded reap, `exit` with serve's exact code (0/1/2/3/75,
    verbatim — never remapped).
  - web died → log web-failure, SIGTERM serve (allow its drain, bounded), `exit 1`.
- No restarts. Any child death ends the container, so serve's exit code (including a
  steady-state self-update 75) reaches the container boundary; the external orchestrator
  (private runtime manager, or a self-hoster's own orchestrator / "upgrade Quorum" action)
  owns rebuild and relaunch.

### Exit-code handling

Never remapped in the shell. serve's code is captured with `wait "$SERVE"; exit $?` on
every path where serve is the dead child — startup gate and steady state alike. Codes:
0 clean drain · 2 lock-held · 3 schema-too-new-at-startup / DB error · 75 running-daemon
self-update. The one synthesized code is `1` when **web** is the failing child (web's own
code is meaningless — it dies by signal or bind error).

## Files

- `docker/entrypoint.sh` — new supervisor, `#!/bin/sh`, strict POSIX (dash-safe: no
  `wait -n`, no bashisms). ~50 lines.
- `Dockerfile` — add pinned `tini` apt package (pin version like existing `git`/`gh` pins),
  set `ENTRYPOINT` to tini + entrypoint.sh, remove the `quorum --help` CMD.
- `docker/supervise.sh` — new real-container tests (or extend `verify.sh`).

## Testing / evidence

- `rtk proxy ./preflight.sh`.
- Clean Docker build from an archive/bounded context.
- Real-container first-run start / readiness / stop tests (without external init): serve Alive via
  `quorum status --json | grep '"Alive"'`; web port accepts on loopback; clean SIGTERM →
  container exit 0; no orphaned children after stop.
- Negative: web-start failure (occupy the port first) → container exit 1, observable, serve
  torn down. Missing `/data/repos/project` checkout → fail loud before/at startup.
- Signal test: SIGTERM to the container drains serve, leaves no orphaned agent/git child,
  causes no incorrect task transition.
- Schema-too-new at startup → **exit 3** (NOT 75): open the container against a DB whose
  `user_version` exceeds the binary; assert the container exits 3 and the code propagates
  verbatim. (The 75 self-update path needs a running daemon detecting a later bump — out of
  scope for a cold-start smoke test; covered by existing serve-level tests.)
- Second-daemon guard: a second `quorum serve` launched inside the active container against
  its DB exits 2 (lock held). A second same-identity container is outside the supported
  one-runtime-per-repository topology and is not used as lock evidence.
- Independent review before merge.

## Out of scope / revisit later

- Native `quorum supervise` command — reconsider when MCP's stay-alive process lands.
- Any restart/rebuild policy — owned outside the container.
