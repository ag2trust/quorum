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
# Child shutdown is deliberately bounded. After this many one-second checks,
# an uncooperative child is killed and reaped so container shutdown cannot hang.
SHUTDOWN_TRIES=5

if [ ! -e "$REPO_DIR/.git" ]; then
  echo "entrypoint: $REPO_DIR is not a git checkout (serve does not clone)" >&2
  exit 1
fi
mkdir -p "$LOG_DIR"

# `serve` requires the per-repository routing config written by `init`. Init is
# idempotent and also creates/migrates the selected repository database. Keep it
# in the foreground so no child starts after a failed initialization, and
# preserve its exact exit code at the container boundary.
if quorum init; then
  :
else
  code=$?
  exit "$code"
fi

# Start children.
set -- --repo "$QUORUM_REPO" --repo-dir "$REPO_DIR" \
       --worktree-base "$WORKTREE_BASE" --log-dir "$LOG_DIR"
if [ "${QUORUM_SELF_UPDATE_DRAIN:-0}" = "1" ]; then set -- "$@" --self-update-drain; fi
quorum serve "$@" &
SERVE=$!
quorum web --port "$WEB_PORT" --bind 127.0.0.1 --log-dir "$LOG_DIR" &
WEB=$!

SHUTTING_DOWN=0
term() { SHUTTING_DOWN=1; kill -TERM "$SERVE" "$WEB" 2>/dev/null || true; }
trap term TERM INT

alive() { kill -0 "$1" 2>/dev/null; }

# Stop one child, escalate if it ignores TERM, and always reap it. The return
# value is the child's exact wait status, including a serve status captured
# before its sibling is torn down.
stop_and_wait() {
  child_pid=$1
  kill -TERM "$child_pid" 2>/dev/null || true
  stop_i=0
  while alive "$child_pid" && [ "$stop_i" -lt "$SHUTDOWN_TRIES" ]; do
    stop_i=$((stop_i + 1))
    sleep 1
  done
  if alive "$child_pid"; then
    echo "entrypoint: child $child_pid ignored TERM; sending KILL" >&2
    kill -KILL "$child_pid" 2>/dev/null || true
  fi
  wait "$child_pid"
}

# serve is the dead child: reap its exact code, tear down web, exit verbatim.
serve_exit() {
  if stop_and_wait "$SERVE"; then code=0; else code=$?; fi
  stop_and_wait "$WEB" >/dev/null 2>&1 || true
  exit "$code"
}
# web is the dead child: fail loud, drain serve, exit 1 (web code meaningless).
web_exit() {
  echo "entrypoint: web exited unexpectedly" >&2
  stop_and_wait "$SERVE" >/dev/null 2>&1 || true
  stop_and_wait "$WEB" >/dev/null 2>&1 || true
  exit 1
}

# Readiness gate.
i=0
while [ "$i" -lt "$READY_TRIES" ]; do
  # Check SHUTTING_DOWN first: a signal during readiness kills both children,
  # so attribute that externally requested shutdown to serve even when web
  # happens to disappear first.
  [ "$SHUTTING_DOWN" = 1 ] && serve_exit
  alive "$SERVE" || serve_exit
  alive "$WEB"   || web_exit
  if quorum status --json 2>/dev/null | grep -q '"Alive"'; then break; fi
  i=$((i + 1))
  sleep 1
done
if [ "$i" -ge "$READY_TRIES" ]; then
  echo "entrypoint: daemon not ready after ${READY_TRIES}s" >&2
  stop_and_wait "$SERVE" >/dev/null 2>&1 || true
  stop_and_wait "$WEB" >/dev/null 2>&1 || true
  exit 1
fi

# Steady state: loop until either child dies. dash defers a trapped signal
# until the current `sleep` returns (no mid-sleep interrupt), then runs
# `term` to kill both children; the loop's next ~1s tick observes the death.
#
# On an externally requested shutdown (SHUTTING_DOWN=1, set by `term`), both
# children were told to die together — which one the poll happens to observe
# dead first is not meaningful, only serve's own exit code is. Real `quorum
# serve` reliably takes longer to unwind (tokio runtime teardown) than real
# `quorum web`, so without this check the web-died-first race would routinely
# misclassify a clean supervised stop as "web crashed" and synthesize exit 1
# instead of propagating serve's real (0) code. Only fall back to the
# which-died-first heuristic for a genuine, unprompted child death (no signal
# received) during steady state.
while alive "$SERVE" && alive "$WEB"; do sleep 1; done
if [ "$SHUTTING_DOWN" = 1 ]; then
  serve_exit
elif alive "$SERVE"; then
  web_exit
else
  serve_exit
fi
