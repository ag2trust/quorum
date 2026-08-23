#!/bin/sh
# serve-supervisor.sh — rebuild-verify-relaunch wrapper for quorum serve.
#
# Runs `quorum serve "$@"` in a loop. On the self-update handoff code 75:
#   1. git fetch origin $SELF_UPDATE_BRANCH (default: QUORUM_BASE_BRANCH, then main)
#   2. git merge --ff-only to advance the working tree
#   3. ./dev-install.sh (build + verify), with a 300s timeout
#   4. On success: relaunch new binary
#   5. On failure: alert loudly, relaunch OLD binary (no retry until sha changes)
#
# Signals (INT/TERM) are forwarded to the child daemon and the supervisor
# waits for it to exit before exiting itself — no orphaned daemons.
#
# Contract: the daemon uses 75 only after a self-update drain (or when its
# schema is too new to read). Any other exit code is propagated and stops the
# supervisor; a crash is never mistaken for an upgrade signal.
# Thrash guard: max 6 restarts per hour.
#
# Usage:
#   scripts/serve-supervisor.sh [quorum serve flags...]
#
# Env overrides:
#   QUORUM_REPO_DIR          repo to fetch from (default: script's own repo root)
#   QUORUM_SERVE_BIN         binary to run (default: quorum, found via PATH)
#   QUORUM_SELF_UPDATE_BRANCH branch to fetch/rebuild and pass to daemon staleness checks
#                              (default: QUORUM_BASE_BRANCH, then main)
#   QUORUM_BASE_BRANCH       legacy fallback for QUORUM_SELF_UPDATE_BRANCH
#   --self-update-branch     caller-supplied daemon branch takes precedence over
#                            both environment controls and is forwarded unchanged
#   QUORUM_THRASH_MAX        max restarts per hour (default: 6)
#   QUORUM_THRASH_WINDOW     thrash window in seconds (default: 3600)
#   QUORUM_BUILD_TIMEOUT     timeout for dev-install.sh in seconds (default: 300)

set -u

REPO_DIR="${QUORUM_REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
SERVE_BIN="${QUORUM_SERVE_BIN:-quorum}"
SELF_UPDATE_BRANCH="${QUORUM_SELF_UPDATE_BRANCH:-${QUORUM_BASE_BRANCH:-main}}"
CALLER_SELF_UPDATE_BRANCH=""
EXPECT_SELF_UPDATE_BRANCH_VALUE=0
for arg in "$@"; do
  if [ "$EXPECT_SELF_UPDATE_BRANCH_VALUE" -eq 1 ]; then
    if [ -z "$arg" ] || [ -n "$CALLER_SELF_UPDATE_BRANCH" ]; then
      printf 'serve-supervisor.sh: --self-update-branch requires one non-empty value\n' >&2
      exit 2
    fi
    CALLER_SELF_UPDATE_BRANCH="$arg"
    EXPECT_SELF_UPDATE_BRANCH_VALUE=0
    continue
  fi
  case "$arg" in
    --self-update-branch)
      EXPECT_SELF_UPDATE_BRANCH_VALUE=1
      ;;
    --self-update-branch=*)
      branch=${arg#*=}
      if [ -z "$branch" ] || [ -n "$CALLER_SELF_UPDATE_BRANCH" ]; then
        printf 'serve-supervisor.sh: --self-update-branch requires one non-empty value\n' >&2
        exit 2
      fi
      CALLER_SELF_UPDATE_BRANCH="$branch"
      ;;
  esac
done
if [ "$EXPECT_SELF_UPDATE_BRANCH_VALUE" -eq 1 ]; then
  printf 'serve-supervisor.sh: --self-update-branch requires one non-empty value\n' >&2
  exit 2
fi
if [ -n "$CALLER_SELF_UPDATE_BRANCH" ]; then
  SELF_UPDATE_BRANCH="$CALLER_SELF_UPDATE_BRANCH"
fi
THRASH_MAX="${QUORUM_THRASH_MAX:-6}"
THRASH_WINDOW="${QUORUM_THRASH_WINDOW:-3600}"
BUILD_TIMEOUT="${QUORUM_BUILD_TIMEOUT:-300}"
# Must remain aligned with quorum::serve::EXIT_SELF_UPDATE. Keep this literal
# here: this POSIX shell wrapper intentionally has no dependency on the binary
# it is supervising.
SELF_UPDATE_EXIT_CODE=75

RESTART_TIMES=""
RESTART_COUNT=0
child=""

now_epoch() {
  date +%s
}

alert() {
  printf 'SUPERVISOR ALERT: %s\n' "$1" >&2
  printf '[supervisor] %s' "$1" | quorum post --agent supervisor --kind alert --body-stdin 2>/dev/null || true
}

if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="gtimeout"
else
  TIMEOUT_CMD=""
fi

forward_signal() {
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
  exit 143
}
trap forward_signal INT TERM

thrash_check() {
  now=$(now_epoch)
  cutoff=$((now - THRASH_WINDOW))
  new_times=""
  new_count=0
  for t in $RESTART_TIMES; do
    if [ "$t" -gt "$cutoff" ]; then
      new_times="$new_times $t"
      new_count=$((new_count + 1))
    fi
  done
  RESTART_TIMES="$new_times"
  RESTART_COUNT=$new_count
  if [ "$RESTART_COUNT" -ge "$THRASH_MAX" ]; then
    return 1
  fi
  RESTART_TIMES="$RESTART_TIMES $now"
  RESTART_COUNT=$((RESTART_COUNT + 1))
  return 0
}

while true; do
  # The daemon polls its configured self-update branch for build staleness.
  # Supplying this resolved branch keeps that poll aligned with this wrapper's
  # rebuild branch. A caller-provided flag already supplies that exact branch,
  # so do not append a duplicate rejected by the daemon CLI.
  if [ -n "$CALLER_SELF_UPDATE_BRANCH" ]; then
    "$SERVE_BIN" serve "$@" &
  else
    "$SERVE_BIN" serve "$@" --self-update-branch "$SELF_UPDATE_BRANCH" &
  fi
  child=$!
  code=0
  wait "$child" || code=$?
  child=""

  case $code in
    "$SELF_UPDATE_EXIT_CODE")
      if ! thrash_check; then
        alert "thrash guard: ${RESTART_COUNT} restarts in the last ${THRASH_WINDOW}s (max ${THRASH_MAX}) — holding on current binary"
        exit "$SELF_UPDATE_EXIT_CODE"
      fi

      printf 'SUPERVISOR: exit %s — self-update drain handoff; fetching and rebuilding\n' "$SELF_UPDATE_EXIT_CODE" >&2
      git -C "$REPO_DIR" fetch origin "$SELF_UPDATE_BRANCH" 2>&1

      if ! git -C "$REPO_DIR" merge --ff-only origin/"$SELF_UPDATE_BRANCH" 2>&1; then
        alert "fast-forward merge failed (dirty tree?) — relaunching OLD binary"
        continue
      fi

      if [ -n "$TIMEOUT_CMD" ]; then
        ( cd "$REPO_DIR" && "$TIMEOUT_CMD" "$BUILD_TIMEOUT" ./dev-install.sh )
      else
        ( cd "$REPO_DIR" && ./dev-install.sh )
      fi
      build_exit=$?

      if [ "$build_exit" -eq 0 ]; then
        printf 'SUPERVISOR: rebuild OK — relaunching\n' >&2
      elif [ "$build_exit" -eq 124 ]; then
        alert "self-update build TIMED OUT after ${BUILD_TIMEOUT}s — relaunching OLD binary"
      else
        alert "self-update build FAILED — relaunching OLD binary"
      fi
      ;;
    *)
      exit $code
      ;;
  esac
done
