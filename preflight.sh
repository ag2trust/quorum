#!/bin/sh
# preflight.sh — hard mechanical author gate before `quorum submit`.
#
# Models follow mechanical gates far more reliably than judgment prose: PRs #112
# and #114 shipped CI-red (fmt/clippy) and cost ~5 reviewer sessions in one week.
# Run this before submitting the quorum task. The daemon owns CI gating; reviewers
# do not enforce PR-body evidence formatting. On agent machines use
# `rtk proxy ./preflight.sh` when complete local output is needed.
#
# Gates (in order, fail-fast):
#   1. branch base   — HEAD is branched from origin/main or origin/develop, not another feature branch
#   2. cargo fmt     — --all -- --check
#   3. cargo clippy  — --all-targets -- -D warnings
#   4. cargo test    — full suite incl. the claim-race canary
#   5. entrypoint    — host-side supervisor contract tests
#   6. Docker        — build the runtime image and run real-container verification
#
# Usage:
#   ./preflight.sh          # all six gates
#   ./preflight.sh --quick  # gates 1+2 only (what the pre-push hook runs)

set -u

QUICK=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    *) printf 'preflight.sh: unknown arg: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

fail() { printf '\nPREFLIGHT: FAIL (%s)\n' "$1"; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 \
    || fail "required command not found: $1"
}

PREFLIGHT_IMAGE_TAG=
cleanup() {
  status=$?
  trap - 0
  if [ -n "$PREFLIGHT_IMAGE_TAG" ]; then
    docker image rm "$PREFLIGHT_IMAGE_TAG" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup 0

# --- Gate 1: branch base ------------------------------------------------------
printf '=== preflight 1/6: branch base ===\n'
git fetch origin --quiet || fail "git fetch origin"
# Fail loud if origin/main is absent — otherwise the git log pipelines below
# error mid-pipe, N_SESSIONS silently becomes 0, and the gate passes vacuously.
git rev-parse --verify --quiet origin/main >/dev/null \
  || fail "origin/main not found — missing remote-tracking ref; gate cannot run"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
BASE_REF=origin/main
BASE_NAME=origin/main
INTEGRATION=0
case "$BRANCH" in
  sync/develop-*)
    git rev-parse --verify --quiet origin/develop >/dev/null \
      || fail "origin/develop not found — integration gate cannot run"
    git merge-base --is-ancestor origin/main HEAD \
      || fail "integration branch does not contain current origin/main"
    git merge-base --is-ancestor origin/develop HEAD \
      || fail "integration branch does not contain current origin/develop"
    BASE_REF=origin/develop
    INTEGRATION=1
    ;;
esac
if [ "$INTEGRATION" -eq 0 ] && [ "$BRANCH" != "main" ]; then
  # Ordinary feature work may target main or develop and need not move merely
  # because the remote base advanced. Select the remote whose merge-base leaves
  # the fewest branch-owned commits; the session scan below still rejects
  # inherited feature work.
  MAIN_BASE=$(git merge-base origin/main HEAD) \
    || fail "branch base: no merge-base with origin/main"
  MAIN_AHEAD=$(git rev-list --count "$MAIN_BASE"..HEAD)
  BASE_REF=$MAIN_BASE
  if git rev-parse --verify --quiet origin/develop >/dev/null; then
    DEVELOP_BASE=$(git merge-base origin/develop HEAD) \
      || fail "branch base: no merge-base with origin/develop"
    DEVELOP_AHEAD=$(git rev-list --count "$DEVELOP_BASE"..HEAD)
    if [ "$DEVELOP_AHEAD" -lt "$MAIN_AHEAD" ]; then
      BASE_REF=$DEVELOP_BASE
      BASE_NAME=origin/develop
    fi
  fi
fi
if [ "$BRANCH" = "main" ]; then
  printf 'on main — nothing to compare, skipping\n'
else
  # Every commit ahead of the selected base must be this PR's own work. Multiple
  # distinct Co-Authored-By sessions ahead of main means the branch was cut from
  # another feature branch (#114) — rebase onto origin/main before pushing.
  if [ "$INTEGRATION" -eq 1 ]; then
    # Main's commits are inherited integration input, not work authored by the
    # sync session. Inspect only commits belonging to neither remote history.
    OWN_COMMITS=$(git rev-list HEAD --not origin/main origin/develop)
    printf 'integration-only commits:\n'
    if [ -n "$OWN_COMMITS" ]; then
      git show -s --oneline $OWN_COMMITS
    fi
  else
    OWN_COMMITS=$(git rev-list "$BASE_REF"..HEAD)
    printf 'commits ahead of %s:\n' "$BASE_NAME"
    git log --oneline "$BASE_REF"..HEAD
  fi
  # Dedupe on the session-name token only ("Flint-r3 (Claude Opus 4.6) <...>" →
  # "Flint-r3") — trailer model-string formats vary within one session.
  N_SESSIONS=$(if [ -n "$OWN_COMMITS" ]; then
      git show -s --format='%(trailers:key=Co-Authored-By,valueonly)' $OWN_COMMITS
    fi | sed '/^$/d' | awk '{print $1}' | sort -u | wc -l | tr -d ' ')
  if [ "$N_SESSIONS" -gt 1 ]; then
    printf 'distinct co-author sessions in branch-owned commits:\n'
    git show -s --format='%(trailers:key=Co-Authored-By,valueonly)' $OWN_COMMITS \
      | sed '/^$/d' | awk '{print $1}' | sort -u
    fail "branch base: ${N_SESSIONS} sessions in branch-owned commits — branched from a feature branch?"
  fi
  # Heuristic limit: session attribution rides on Co-Authored-By trailers
  # (convention-mandatory on every commit, CLAUDE.md §Engineering practices 1).
  # Trailer-less commits can't be attributed — surface that instead of implying
  # a clean pass.
  N_AHEAD=$(printf '%s\n' "$OWN_COMMITS" | sed '/^$/d' | wc -l | tr -d ' ')
  if [ "$N_SESSIONS" -eq 0 ] && [ "$N_AHEAD" -gt 0 ]; then
    printf 'note: %s commit(s) ahead carry no Co-Authored-By trailer — sessions unattributable; eyeball the commit list above\n' "$N_AHEAD"
  fi
  printf 'branch base OK (%s session(s) ahead of %s)\n' "$N_SESSIONS" "$BASE_NAME"
fi

# --- Gate 2: cargo fmt --------------------------------------------------------
printf '=== preflight 2/6: cargo fmt --all -- --check ===\n'
cargo fmt --all -- --check || fail "cargo fmt"
printf 'fmt OK\n'

if [ "$QUICK" -eq 1 ]; then
  printf '\nPREFLIGHT: PASS (quick — gates 1-2; run without --quick before quorum submit)\n'
  exit 0
fi

# The mandatory full gate exercises a real linux/amd64 image. Check its host
# dependencies before spending time on clippy and the Rust test suite.
require_command docker
require_command sqlite3
docker buildx version >/dev/null 2>&1 \
  || fail "Docker buildx is unavailable"
docker info >/dev/null 2>&1 \
  || fail "Docker daemon is unavailable"

# --- Gate 3: cargo clippy -----------------------------------------------------
printf '=== preflight 3/6: cargo clippy --all-targets -- -D warnings ===\n'
cargo clippy --all-targets -- -D warnings || fail "cargo clippy"
printf 'clippy OK\n'

# --- Gate 4: cargo test -------------------------------------------------------
printf '=== preflight 4/6: cargo test ===\n'
cargo test || fail "cargo test"

# --- Gate 5: entrypoint contract ---------------------------------------------
printf '=== preflight 5/6: docker/entrypoint_test.sh ===\n'
./docker/entrypoint_test.sh || fail "entrypoint contract"
printf 'entrypoint contract OK\n'

# --- Gate 6: real container --------------------------------------------------
printf '=== preflight 6/6: Docker image verification ===\n'
PREFLIGHT_IMAGE_TAG="quorum-preflight:$(date +%s)-$$"
docker buildx build --load --platform linux/amd64 --tag "$PREFLIGHT_IMAGE_TAG" . \
  || fail "Docker image build"
./docker/verify.sh "$PREFLIGHT_IMAGE_TAG" || fail "Docker image verification"
printf 'Docker verification OK\n'

printf '\nPREFLIGHT: PASS (all 6 gates green)\n'
