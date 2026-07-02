#!/bin/sh
# preflight.sh — hard mechanical gate before `task-update --status done`.
#
# Models follow mechanical gates far more reliably than judgment prose: PRs #112
# and #114 shipped CI-red (fmt/clippy) and cost ~5 reviewer sessions in one week.
# Run this, paste its FULL output in the PR body under `## Verification`, and only
# then mark the quorum task done. On agent machines run it as
# `rtk proxy ./preflight.sh` so RTK doesn't compress the evidence (see CLAUDE.md).
#
# Gates (in order, fail-fast):
#   1. branch base   — HEAD is branched from origin/main, not another feature branch
#   2. cargo fmt     — --all -- --check
#   3. cargo clippy  — --all-targets -- -D warnings
#   4. cargo test    — full suite incl. the claim-race canary
#
# Usage:
#   ./preflight.sh          # all four gates
#   ./preflight.sh --quick  # gates 1+2 only (what the pre-push hook runs)

set -u

QUICK=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
    *) printf 'preflight.sh: unknown arg: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

fail() { printf '\nPREFLIGHT: FAIL (%s)\n' "$1"; exit 1; }

# --- Gate 1: branch base ------------------------------------------------------
printf '=== preflight 1/4: branch base ===\n'
git fetch origin --quiet || fail "git fetch origin"
# Fail loud if origin/main is absent — otherwise the git log pipelines below
# error mid-pipe, N_SESSIONS silently becomes 0, and the gate passes vacuously.
git rev-parse --verify --quiet origin/main >/dev/null \
  || fail "origin/main not found — missing remote-tracking ref; gate cannot run"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" = "main" ]; then
  printf 'on main — nothing to compare, skipping\n'
else
  # Every commit ahead of origin/main must be this PR's own work. Multiple
  # distinct Co-Authored-By sessions ahead of main means the branch was cut from
  # another feature branch (#114) — rebase onto origin/main before pushing.
  printf 'commits ahead of origin/main:\n'
  git log --oneline origin/main..HEAD
  # Dedupe on the session-name token only ("Flint-r3 (Claude Opus 4.6) <...>" →
  # "Flint-r3") — trailer model-string formats vary within one session.
  N_SESSIONS=$(git log origin/main..HEAD --format='%(trailers:key=Co-Authored-By,valueonly)' \
    | sed '/^$/d' | awk '{print $1}' | sort -u | wc -l | tr -d ' ')
  if [ "$N_SESSIONS" -gt 1 ]; then
    printf 'distinct co-author sessions ahead of origin/main:\n'
    git log origin/main..HEAD --format='%(trailers:key=Co-Authored-By,valueonly)' \
      | sed '/^$/d' | awk '{print $1}' | sort -u
    fail "branch base: ${N_SESSIONS} sessions in origin/main..HEAD — branched from a feature branch? Rebase onto origin/main"
  fi
  # Heuristic limit: session attribution rides on Co-Authored-By trailers
  # (convention-mandatory on every commit, CLAUDE.md §Engineering practices 1).
  # Trailer-less commits can't be attributed — surface that instead of implying
  # a clean pass.
  N_AHEAD=$(git rev-list --count origin/main..HEAD)
  if [ "$N_SESSIONS" -eq 0 ] && [ "$N_AHEAD" -gt 0 ]; then
    printf 'note: %s commit(s) ahead carry no Co-Authored-By trailer — sessions unattributable; eyeball the commit list above\n' "$N_AHEAD"
  fi
  printf 'branch base OK (%s session(s) ahead of origin/main)\n' "$N_SESSIONS"
fi

# --- Gate 2: cargo fmt --------------------------------------------------------
printf '=== preflight 2/4: cargo fmt --all -- --check ===\n'
cargo fmt --all -- --check || fail "cargo fmt"
printf 'fmt OK\n'

if [ "$QUICK" -eq 1 ]; then
  printf '\nPREFLIGHT: PASS (quick — gates 1-2; run without --quick before task-update --status done)\n'
  exit 0
fi

# --- Gate 3: cargo clippy -----------------------------------------------------
printf '=== preflight 3/4: cargo clippy --all-targets -- -D warnings ===\n'
cargo clippy --all-targets -- -D warnings || fail "cargo clippy"
printf 'clippy OK\n'

# --- Gate 4: cargo test -------------------------------------------------------
printf '=== preflight 4/4: cargo test ===\n'
cargo test || fail "cargo test"

printf '\nPREFLIGHT: PASS (all 4 gates green)\n'
