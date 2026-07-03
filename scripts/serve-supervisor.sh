#!/bin/sh
# serve-supervisor.sh — rebuild-verify-relaunch wrapper for quorum serve.
#
# Runs `quorum serve "$@"` in a loop. On exit 75 (self-update drain):
#   1. git fetch origin $QUORUM_BASE_BRANCH (default: main)
#   2. ./dev-install.sh (build + verify)
#   3. On success: relaunch new binary
#   4. On failure: alert loudly, relaunch OLD binary (no retry until sha changes)
#
# Any other exit code: propagate and stop (crash ≠ upgrade signal).
# Thrash guard: max 6 restarts per hour.
#
# Usage:
#   scripts/serve-supervisor.sh [quorum serve flags...]
#
# Env overrides:
#   QUORUM_REPO_DIR          repo to fetch from (default: script's own repo root)
#   QUORUM_SERVE_BIN         binary to run (default: quorum, found via PATH)
#   QUORUM_BASE_BRANCH       branch to fetch on rebuild (default: main)
#   QUORUM_THRASH_MAX        max restarts per hour (default: 6)
#   QUORUM_THRASH_WINDOW     thrash window in seconds (default: 3600)

set -u

REPO_DIR="${QUORUM_REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
SERVE_BIN="${QUORUM_SERVE_BIN:-quorum}"
BASE_BRANCH="${QUORUM_BASE_BRANCH:-main}"
THRASH_MAX="${QUORUM_THRASH_MAX:-6}"
THRASH_WINDOW="${QUORUM_THRASH_WINDOW:-3600}"

RESTART_TIMES=""
RESTART_COUNT=0

now_epoch() {
  date +%s
}

alert() {
  printf 'SUPERVISOR ALERT: %s\n' "$1" >&2
}

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
  "$SERVE_BIN" serve "$@"
  code=$?

  case $code in
    75)
      if ! thrash_check; then
        alert "thrash guard: ${RESTART_COUNT} restarts in the last ${THRASH_WINDOW}s (max ${THRASH_MAX}) — holding on current binary"
        exit 75
      fi

      printf 'SUPERVISOR: exit 75 — self-update drain; fetching and rebuilding\n' >&2
      git -C "$REPO_DIR" fetch origin "$BASE_BRANCH" 2>&1

      if ( cd "$REPO_DIR" && ./dev-install.sh ); then
        printf 'SUPERVISOR: rebuild OK — relaunching\n' >&2
      else
        alert "self-update build FAILED — relaunching OLD binary"
      fi
      ;;
    *)
      exit $code
      ;;
  esac
done
