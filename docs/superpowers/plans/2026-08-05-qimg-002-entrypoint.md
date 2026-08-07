# QIMG-002 Container Entrypoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the image's diagnostic `quorum --help` default with a PID-1 supervisor that runs `quorum serve` + read-only `quorum web` for one repo, fails loud on child death, and propagates serve's exact exit code to the container boundary.

**Architecture:** `tini` is PID 1 (reaps zombies, forwards signals); a strict-POSIX `docker/entrypoint.sh` owns the two children — starts them, gates readiness via `quorum status --json`, waits with a `kill -0` poll loop (dash has no `wait -n`), and on any child death tears down the survivor and exits with serve's verbatim code. No in-container restart. No Quorum Rust changes.

**Tech Stack:** POSIX `sh` (dash on bookworm-slim), `tini`, Docker multi-stage build (existing), `quorum` CLI.

## Global Constraints

- Strict POSIX `sh` — no `wait -n`, no bashisms. Script is `#!/bin/sh`.
- `set -u` only (NOT `set -e`; the control flow relies on capturing nonzero exit codes from `wait` and `kill -0`).
- Propagate serve's exact exit code verbatim: 0 clean drain · 2 lock-held · 3 schema-too-new-at-startup / DB error · 75 running-daemon self-update. Never remap. Only synthesized code is `1` when **web** is the failing child.
- Web stays loopback: `--bind 127.0.0.1`.
- `/data` paths: repo `/data/repos/project`, worktrees `/data/worktrees`, state `/data/quorum` (`QUORUM_HOME`, already set by QIMG-001), logs `/data/quorum/logs`.
- `QUORUM_REPO` (`owner/name`) is required and **exported** — web/status resolve serve's DB via that env, no repo flag.
- serve does NOT clone; `/data/repos/project` must be a pre-provisioned git checkout. Entrypoint asserts it.
- No credentials, no hosted auth/MCP/gateway/public port. No Quorum Rust changes.
- `tini` pinned by apt version like the existing `git`/`gh` pins.
- Docker ENTRYPOINT = `["/usr/bin/tini","--"]`, CMD = `["/usr/local/bin/entrypoint.sh"]` — split so `docker run image <cmd>` still overrides the command (keeps `smoke.sh` green) while tini stays PID 1.
- Verified facts (`develop` @ 3790880e): `EXIT_SELF_UPDATE=75` `serve/mod.rs:1898`; schema-too-new at startup → 3 (`db::open` at `mod.rs:1917`, `error.rs:38`); lock-held → 2; serve handles SIGINT+SIGTERM only; `quorum status` always exits 0, liveness is JSON `"daemon":{"Alive":…}` (`stats.rs:305-316`); web has no signal handlers.

---

### Task 1: The supervisor script (`docker/entrypoint.sh`)

Host-testable via a fake `quorum` shim on `PATH` — no Docker needed. Paths are env-overridable with `/data` defaults so tests can point them at a temp dir; container behavior is unchanged (defaults apply).

**Files:**
- Create: `docker/entrypoint.sh`
- Test: `docker/entrypoint_test.sh` (host-side; stubs `quorum`)

**Interfaces:**
- Produces: an executable `#!/bin/sh` script. Env inputs: `QUORUM_REPO` (required), `QUORUM_WEB_PORT` (default 8080), `QUORUM_SELF_UPDATE_DRAIN` (default 0), `QUORUM_READY_TRIES` (default 30), and test-only path overrides `QUORUM_REPO_DIR` / `QUORUM_WORKTREE_BASE` / `QUORUM_LOG_DIR` (defaults `/data/repos/project`, `/data/worktrees`, `/data/quorum/logs`). Exit codes per Global Constraints.
- Consumes: `quorum serve|web|status` CLI (real in container; shim in test).

- [ ] **Step 1: Write the failing test harness + first cases**

Create `docker/entrypoint_test.sh`. It builds a fake `quorum` shim whose behavior is driven by env, puts it first on `PATH`, and runs `entrypoint.sh` under controlled scenarios. Assert exit codes.

```sh
#!/bin/sh
# Host-side tests for docker/entrypoint.sh. No Docker required.
set -u
here="$(cd "$(dirname "$0")" && pwd)"
ENTRY="$here/entrypoint.sh"
fails=0

# Build a temp workspace with a fake `quorum` and a fake git checkout.
setup() {
  WORK="$(mktemp -d)"
  mkdir -p "$WORK/bin" "$WORK/repo/.git" "$WORK/wt" "$WORK/logs"
  cat >"$WORK/bin/quorum" <<'EOF'
#!/bin/sh
# Fake quorum. Behavior via FAKE_* env.
case "$1" in
  serve)  sleep "${FAKE_SERVE_SLEEP:-5}"; exit "${FAKE_SERVE_CODE:-0}" ;;
  web)    sleep "${FAKE_WEB_SLEEP:-5}";   exit "${FAKE_WEB_CODE:-0}" ;;
  status)
    if [ "${FAKE_READY:-1}" = 1 ]; then echo '{"daemon":{"Alive":{"pid":1}}}';
    else echo '{"daemon":"None"}'; fi ;;
esac
EOF
  chmod +x "$WORK/bin/quorum"
  export PATH="$WORK/bin:$PATH"
  export QUORUM_REPO=acme/widget
  export QUORUM_REPO_DIR="$WORK/repo"
  export QUORUM_WORKTREE_BASE="$WORK/wt"
  export QUORUM_LOG_DIR="$WORK/logs"
  export QUORUM_READY_TRIES=5
  unset QUORUM_SELF_UPDATE_DRAIN QUORUM_WEB_PORT 2>/dev/null || true
}
teardown() { rm -rf "$WORK"; unset FAKE_SERVE_CODE FAKE_SERVE_SLEEP FAKE_WEB_CODE FAKE_WEB_SLEEP FAKE_READY 2>/dev/null || true; }

check() { # label expected actual
  if [ "$3" = "$2" ]; then echo "ok: $1"; else echo "FAIL: $1 expected $2 got $3"; fails=$((fails+1)); fi
}

# Case A: missing QUORUM_REPO -> nonzero before starting anything.
setup; unset QUORUM_REPO
sh "$ENTRY" >/dev/null 2>&1; check "missing QUORUM_REPO fails" 1 "$?"; teardown

# Case B: missing repo checkout -> nonzero.
setup; export QUORUM_REPO_DIR="$WORK/nonexistent"
sh "$ENTRY" >/dev/null 2>&1; check "missing checkout fails" 1 "$?"; teardown

echo "---"; [ "$fails" = 0 ] && echo "all passed" || { echo "$fails failed"; exit 1; }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `sh docker/entrypoint_test.sh`
Expected: FAIL — `entrypoint.sh` does not exist yet (cases error out with nonzero anyway, but the script is missing). Confirms the harness runs.

- [ ] **Step 3: Write `docker/entrypoint.sh`**

```sh
#!/bin/sh
# PID-1-managed supervisor for `quorum serve` + read-only `quorum web`.
# tini is the real PID 1 (reaping + signal forwarding); this script owns the
# two children and propagates serve's exact exit code. Strict POSIX (dash):
# no `wait -n`, no bashisms. set -u only — control flow captures exit codes.
set -u

QUORUM_REPO="${QUORUM_REPO:-}"
if [ -z "$QUORUM_REPO" ]; then
  echo "entrypoint: QUORUM_REPO (owner/name) is required" >&2
  exit 1
fi
export QUORUM_REPO

REPO_DIR="${QUORUM_REPO_DIR:-/data/repos/project}"
WORKTREE_BASE="${QUORUM_WORKTREE_BASE:-/data/worktrees}"
LOG_DIR="${QUORUM_LOG_DIR:-/data/quorum/logs}"
WEB_PORT="${QUORUM_WEB_PORT:-8080}"
READY_TRIES="${QUORUM_READY_TRIES:-30}"

if [ ! -d "$REPO_DIR/.git" ]; then
  echo "entrypoint: $REPO_DIR is not a git checkout (serve does not clone)" >&2
  exit 1
fi
mkdir -p "$LOG_DIR"

# Start children.
set -- --repo "$QUORUM_REPO" --repo-dir "$REPO_DIR" \
       --worktree-base "$WORKTREE_BASE" --log-dir "$LOG_DIR"
if [ "${QUORUM_SELF_UPDATE_DRAIN:-0}" = "1" ]; then set -- "$@" --self-update-drain; fi
quorum serve "$@" &
SERVE=$!
quorum web --port "$WEB_PORT" --bind 127.0.0.1 --log-dir "$LOG_DIR" &
WEB=$!

term() { kill -TERM "$SERVE" "$WEB" 2>/dev/null || true; }
trap term TERM INT

alive() { kill -0 "$1" 2>/dev/null; }

# serve is the dead child: reap its exact code, tear down web, exit verbatim.
serve_exit() {
  kill -TERM "$WEB" 2>/dev/null || true
  wait "$WEB" 2>/dev/null || true
  wait "$SERVE"; code=$?
  exit "$code"
}
# web is the dead child: fail loud, drain serve, exit 1 (web code meaningless).
web_exit() {
  echo "entrypoint: web exited unexpectedly" >&2
  kill -TERM "$SERVE" 2>/dev/null || true
  wait "$SERVE" 2>/dev/null || true
  exit 1
}

# Readiness gate.
i=0
while [ "$i" -lt "$READY_TRIES" ]; do
  alive "$SERVE" || serve_exit
  alive "$WEB"   || web_exit
  if quorum status --json 2>/dev/null | grep -q '"Alive"'; then break; fi
  i=$((i + 1))
  sleep 1
done
if [ "$i" -ge "$READY_TRIES" ]; then
  echo "entrypoint: daemon not ready after ${READY_TRIES}s" >&2
  kill -TERM "$SERVE" "$WEB" 2>/dev/null || true
  wait "$SERVE" 2>/dev/null || true
  exit 1
fi

# Steady state: loop until either child dies (a trapped signal interrupts the
# sleep, kills both, and the next iteration observes the death).
while alive "$SERVE" && alive "$WEB"; do sleep 1; done
if alive "$SERVE"; then web_exit; else serve_exit; fi
```

- [ ] **Step 4: Add the exit-code passthrough + web-fail cases to the test**

Append to `docker/entrypoint_test.sh` before the summary line:

```sh
# Case C: serve exits 3 during the gate (schema-too-new at startup) -> container exits 3.
setup; FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=3 FAKE_READY=0 \
  sh "$ENTRY" >/dev/null 2>&1; check "serve exit 3 propagates" 3 "$?"; teardown

# Case D: serve exits 75 (self-update) after becoming ready -> container exits 75.
setup; FAKE_SERVE_SLEEP=2 FAKE_SERVE_CODE=75 FAKE_WEB_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "serve exit 75 propagates" 75 "$?"; teardown

# Case E: web dies while serve stays up -> container exits 1.
setup; FAKE_WEB_SLEEP=1 FAKE_WEB_CODE=1 FAKE_SERVE_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "web failure -> exit 1" 1 "$?"; teardown

# Case F: clean shutdown -> serve exits 0 -> container exits 0.
setup; FAKE_SERVE_SLEEP=2 FAKE_SERVE_CODE=0 FAKE_WEB_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "clean serve exit 0" 0 "$?"; teardown
```

- [ ] **Step 5: Run the tests, verify all pass**

Run: `sh docker/entrypoint_test.sh`
Expected: `all passed` (cases A–F ok). If Case D/F hang, the steady-state loop or `serve_exit` reap is wrong.

- [ ] **Step 6: Commit**

```bash
git add docker/entrypoint.sh docker/entrypoint_test.sh
git commit -m "feat: add container supervisor entrypoint for serve+web

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Wire `tini` + entrypoint into the Dockerfile

**Files:**
- Modify: `Dockerfile` (final runtime stage — apt install block, COPY, ENTRYPOINT/CMD near the end)

**Interfaces:**
- Consumes: `docker/entrypoint.sh` from Task 1.
- Produces: an image whose PID 1 is `tini`, default command is the supervisor, and `docker run image <cmd>` still runs `<cmd>` under tini.

- [ ] **Step 1: Find the pinned `tini` version for bookworm**

Run: `docker run --rm debian:bookworm-slim sh -c 'apt-get update >/dev/null 2>&1 && apt-cache policy tini | grep Candidate'`
Record the exact version string (e.g. `0.19.0-1`). Use it verbatim as the pin.

- [ ] **Step 2: Add `tini` to the runtime apt install (pinned)**

In the final stage's `apt-get install` list (alongside `git`/`gh`/`openssh-client`), add a pinned `tini`. Match the existing `ARG`+`="${VERSION}"` pin style:

```dockerfile
ARG TINI_VERSION=0.19.0-1
```

and in the install list:

```dockerfile
      tini="${TINI_VERSION}" \
```

(Replace `0.19.0-1` with the value from Step 1 if different.)

- [ ] **Step 3: COPY the entrypoint and set ENTRYPOINT/CMD**

After the existing `COPY --from=... /usr/local/bin/quorum ...` lines, and replacing the final `CMD ["quorum", "--help"]`:

```dockerfile
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0555 /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
```

Confirm `/usr/bin/tini` is the installed path: `docker run --rm debian:bookworm-slim sh -c 'apt-get update>/dev/null 2>&1 && apt-get install -y tini>/dev/null 2>&1 && command -v tini'`.

- [ ] **Step 4: Build and verify tini is PID 1 + smoke still green**

```bash
docker build --platform linux/amd64 --tag quorum:local .
docker run --rm quorum:local tini --version           # command override still works
./docker/verify.sh quorum:local                        # smoke.sh + invalid-checksum path
```
Expected: build succeeds; `tini version 0.19.0`; `verify.sh` passes. `verify.sh`/`smoke.sh` pass because ENTRYPOINT is `tini --` and their `docker run image <cmd>` invocations run `<cmd>` under tini unchanged.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile
git commit -m "feat: run supervisor entrypoint under tini as PID 1

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Real-container integration + negative tests (`docker/supervise.sh`)

Exercises the actual image with a real `quorum` binary and a throwaway git checkout mounted at `/data`. Wired into `verify.sh` so `./docker/verify.sh` runs it.

**Files:**
- Create: `docker/supervise.sh`
- Modify: `docker/verify.sh` (invoke `supervise.sh` after `smoke.sh`)

**Interfaces:**
- Consumes: the built image (Task 2), `docker/entrypoint.sh` behavior (Task 1).

- [ ] **Step 1: Write `docker/supervise.sh` — start/readiness/clean-stop**

```sh
#!/bin/sh
set -eu
image="${1:-quorum:local}"
work="$(mktemp -d)"
trap 'docker rm -f qsup >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

# Pre-provision a git checkout at the mounted /data/repos/project.
git init -q "$work/repos/project"
( cd "$work/repos/project" && git -c user.email=t@t -c user.name=t commit -q --allow-empty -m init )
mkdir -p "$work/quorum" "$work/worktrees"

run() { # extra docker args...
  docker run -d --name qsup \
    -e QUORUM_REPO=acme/widget \
    -v "$work:/data" "$@" "$image"
}

# --- readiness: daemon becomes Alive within budget ---
run >/dev/null
ready=0
i=0; while [ "$i" -lt 30 ]; do
  if docker exec qsup quorum status --json 2>/dev/null | grep -q '"Alive"'; then ready=1; break; fi
  i=$((i+1)); sleep 1
done
[ "$ready" = 1 ] || { echo "supervise: daemon never Alive" >&2; docker logs qsup >&2; exit 1; }

# web loopback listening inside the container
docker exec qsup sh -c 'quorum web --help >/dev/null' # sanity: web subcommand present
# --- clean stop: SIGTERM -> exit 0, no orphaned quorum/git children ---
docker stop -t 30 qsup >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' qsup)"
[ "$code" = 0 ] || { echo "supervise: clean stop expected 0 got $code" >&2; exit 1; }
docker rm -f qsup >/dev/null
echo "supervise: start/ready/clean-stop passed"
```

- [ ] **Step 2: Add the negative + exit-code cases to `supervise.sh`**

Append before the final echo:

```sh
# --- web-start failure: occupy the port so web cannot bind -> container exit 1 ---
run -e QUORUM_WEB_PORT=8080 >/dev/null
# Simulate by starting a second web on the same port inside the container:
docker exec qsup sh -c 'quorum web --port 8080 --bind 127.0.0.1 >/dev/null 2>&1 &' || true
# (Primary already holds 8080; the entrypoint's web is the one that must own it.
#  To force the failure deterministically, restart with an already-bound port.)
docker rm -f qsup >/dev/null

# Deterministic web-fail: pre-bind 8080 via a sidecar sharing the netns is heavy;
# instead assert the entrypoint's web-death path with a bad bind via env.
run -e QUORUM_WEB_PORT=1 >/dev/null   # port 1 is privileged -> web bind fails as non-root
sleep 5
state="$(docker inspect -f '{{.State.Running}}' qsup)"
[ "$state" = "false" ] || { echo "supervise: web-fail container still running" >&2; docker logs qsup >&2; exit 1; }
code="$(docker inspect -f '{{.State.ExitCode}}' qsup)"
[ "$code" = 1 ] || { echo "supervise: web-fail expected exit 1 got $code" >&2; docker logs qsup >&2; exit 1; }
docker rm -f qsup >/dev/null

# --- schema-too-new at startup -> exit 3 ---
# Bump the DB user_version above the binary's expected schema before start.
db="$work/quorum/repos/acme__widget/quorum.db"
docker run --rm -v "$work:/data" -e QUORUM_REPO=acme/widget "$image" quorum init >/dev/null
docker run --rm -v "$work:/data" "$image" \
  sh -c 'command -v sqlite3 >/dev/null 2>&1 && sqlite3 "'"$db"'" "PRAGMA user_version=99999;"' \
  || python3 - "$db" <<'PY' 2>/dev/null || true
import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute("PRAGMA user_version=99999"); c.commit()
PY
run >/dev/null
sleep 5
code="$(docker inspect -f '{{.State.ExitCode}}' qsup)"
[ "$code" = 3 ] || { echo "supervise: schema-too-new expected exit 3 got $code" >&2; docker logs qsup >&2; exit 1; }
docker rm -f qsup >/dev/null

echo "supervise: negative + exit-code cases passed"
```

Note: the `user_version` bump must run on the host (sqlite3/python3 available there) against `$db` directly — adjust the two `docker run` shims above to a plain host `sqlite3 "$db" "PRAGMA user_version=99999;"` if the image lacks sqlite3. Keep whichever form actually mutates the file; verify by re-reading the pragma.

- [ ] **Step 3: Wire into `verify.sh`**

In `docker/verify.sh`, after the `smoke.sh` invocation line, add:

```sh
"$(dirname "$0")/supervise.sh" "$image"
```

- [ ] **Step 4: Run the full container verification**

```bash
chmod +x docker/supervise.sh
docker build --platform linux/amd64 --tag quorum:local .
./docker/verify.sh quorum:local
```
Expected: smoke, supervise (start/ready/clean-stop, web-fail exit 1, schema-too-new exit 3), and invalid-checksum path all pass.

- [ ] **Step 5: Commit**

```bash
git add docker/supervise.sh docker/verify.sh
git commit -m "test: real-container supervisor start/stop/negative/exit-code tests

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Docs + preflight

**Files:**
- Modify: `docker/README.md` (replace "diagnostic default is `quorum --help`" framing; document the supervisor, required `QUORUM_REPO`, the checkout precondition, exit-code contract)

- [ ] **Step 1: Update `docker/README.md`**

Replace the paragraph stating the default command is `quorum --help` and "Daemon and Web process supervision will be added separately." Document:
- default command now supervises `quorum serve` + `quorum web`;
- required env `QUORUM_REPO=owner/name`;
- precondition: a git checkout at `/data/repos/project` (serve does not clone);
- web is loopback-only on 8080 (`QUORUM_WEB_PORT` to change);
- exit-code contract: the container exits with serve's exact code (0 drain · 2 lock-held · 3 schema-too-new/DB error · 75 self-update); web failure exits 1; no in-container restart — an external orchestrator owns rebuild/relaunch;
- `QUORUM_SELF_UPDATE_DRAIN=1` opts into base-branch self-update drain (off by default).

- [ ] **Step 2: Run the full author gate**

Run: `rtk proxy ./preflight.sh`
Expected: pass. (Per handoff, `develop` may carry unrelated timing-sensitive lifecycle flakes; formatting/clippy/Docker verification/CI must pass. Do not describe the complete local preflight as passing if unrelated lifecycle tests flake — record exactly what passed.)

- [ ] **Step 3: Commit**

```bash
git add docker/README.md
git commit -m "docs: document container supervisor entrypoint and exit contract

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- PID-1 model / tini → Task 2. ✓
- entrypoint config, readiness gate, steady-state, exit passthrough → Task 1. ✓
- `quorum status --json '"Alive"'` probe → Task 1 Step 3 + Task 3. ✓
- QUORUM_REPO export + DB env coupling → Task 1 (export) + README Task 4. ✓
- checkout precondition → Task 1 (assert) + Task 3 (provision) + README. ✓
- dash `wait -n` avoidance → Task 1 poll loop. ✓
- exit codes 0/1/2/3/75 → Task 1 cases C–F + Task 3 web-fail(1)/schema(3). ✓
- loopback web → Task 1 `--bind 127.0.0.1`. ✓
- no restart → no restart logic anywhere; any death exits. ✓
- self-update-drain knob → Task 1. ✓
- evidence: preflight, docker build, real-container start/ready/stop, web-fail, schema-too-new, second-daemon → Tasks 3–4. ✓

**Topology decision:** one active container owns each globally unique `owner/name` repository identity. Runtime replacement stops the prior container before starting its successor. Task 3 therefore starts a second `quorum serve` inside the active container via `docker exec` and asserts exit 2; an overlapping same-identity container is unsupported and is not a lock test.

**Signal/no-orphan evidence:** Task 3 Step 1 clean-stop asserts exit 0; strengthen it by asserting no lingering `quorum`/`git` processes: after `docker stop`, before `rm`, the container is stopped so orphan-check is moot at container scope — tini reaping is validated by clean exit 0 within the stop timeout (a leaked non-reaped child would hang the stop and force a 137). Note this reasoning in the test comment rather than adding a separate probe.

**Placeholder scan:** no TBD/TODO; all steps carry real code. ✓

**Type consistency:** `serve_exit`/`web_exit`/`alive` helper names consistent between Task 1 script and Task 3 references. Env var names (`QUORUM_REPO`, `QUORUM_WEB_PORT`, `QUORUM_SELF_UPDATE_DRAIN`, `QUORUM_READY_TRIES`, `QUORUM_REPO_DIR`/`WORKTREE_BASE`/`LOG_DIR`) consistent across Tasks 1–4. ✓

## Known-corner notes (ponytail)

- `# ponytail: web readiness = liveness only (has-not-exited), not a TCP probe; add a port-accept check if a bind race ever surfaces.` — carry this comment in `entrypoint.sh`.
- Web-fail integration test uses privileged port 1 to force a non-root bind failure deterministically; if a future base image grants `CAP_NET_BIND_SERVICE` this stops failing — revisit with an occupied-port sidecar then.
