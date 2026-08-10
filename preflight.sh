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
#   1. branch base   — HEAD is branched from origin/main, not another feature branch
#   2. cargo fmt     — --all -- --check
#   3. cargo clippy  — all targets and quorum-core/test-support
#   4. cargo test    — full suite incl. real-process contention canaries (4 threads)
#
# Usage:
#   ./preflight.sh          # all four gates
#   ./preflight.sh --quick  # gates 1+2 only (what the pre-push hook runs)

set -u

QUICK=0
CONTINUATION_FROM=
CONTINUATION_SET=0
CONTINUATION_BASE=
CONTINUATION_BASE_SET=0
PROPOSED_SHA=
PROPOSED_SET=0
HOOK_FORMAT_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --continuation-from=*)
      [ "$CONTINUATION_SET" -eq 0 ] \
        || { printf 'preflight.sh: duplicate --continuation-from\n' >&2; exit 2; }
      CONTINUATION_FROM=${arg#*=}
      CONTINUATION_SET=1
      ;;
    --continuation-base=*)
      [ "$CONTINUATION_BASE_SET" -eq 0 ] \
        || { printf 'preflight.sh: duplicate --continuation-base\n' >&2; exit 2; }
      CONTINUATION_BASE=${arg#*=}
      CONTINUATION_BASE_SET=1
      ;;
    --proposed-sha=*)
      [ "$PROPOSED_SET" -eq 0 ] \
        || { printf 'preflight.sh: duplicate --proposed-sha\n' >&2; exit 2; }
      PROPOSED_SHA=${arg#*=}
      PROPOSED_SET=1
      ;;
    --hook-format-only) HOOK_FORMAT_ONLY=1 ;;
    -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
    *) printf 'preflight.sh: unknown arg: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

if [ "$CONTINUATION_SET" -eq 1 ] && [ -z "$CONTINUATION_FROM" ]; then
  printf 'preflight.sh: --continuation-from requires a commit\n' >&2
  exit 2
fi
if [ "$CONTINUATION_BASE_SET" -eq 1 ] && [ -z "$CONTINUATION_BASE" ]; then
  printf 'preflight.sh: --continuation-base requires a commit\n' >&2
  exit 2
fi
if [ "$PROPOSED_SET" -eq 1 ] && [ -z "$PROPOSED_SHA" ]; then
  printf 'preflight.sh: --proposed-sha requires a commit\n' >&2
  exit 2
fi
if { [ "$CONTINUATION_SET" -eq 1 ] || [ "$CONTINUATION_BASE_SET" -eq 1 ] \
  || [ "$PROPOSED_SET" -eq 1 ] \
  || [ "$HOOK_FORMAT_ONLY" -eq 1 ]; } && [ "$QUICK" -ne 1 ]; then
  printf 'preflight.sh: hook-only options require --quick\n' >&2
  exit 2
fi
if [ "$CONTINUATION_SET" -eq 1 ] && [ "$PROPOSED_SET" -ne 1 ]; then
  printf 'preflight.sh: --continuation-from requires --proposed-sha\n' >&2
  exit 2
fi
if [ "$CONTINUATION_SET" -eq 1 ] && [ "$CONTINUATION_BASE_SET" -ne 1 ]; then
  printf 'preflight.sh: --continuation-from requires --continuation-base\n' >&2
  exit 2
fi
if [ "$CONTINUATION_BASE_SET" -eq 1 ] && [ "$CONTINUATION_SET" -ne 1 ]; then
  printf 'preflight.sh: --continuation-base requires --continuation-from\n' >&2
  exit 2
fi
if [ "$HOOK_FORMAT_ONLY" -eq 1 ] \
  && { [ "$CONTINUATION_SET" -eq 1 ] || [ "$CONTINUATION_BASE_SET" -eq 1 ] \
    || [ "$PROPOSED_SET" -eq 1 ]; }; then
  printf 'preflight.sh: --hook-format-only conflicts with publication options\n' >&2
  exit 2
fi

fail() { printf '\nPREFLIGHT: FAIL (%s)\n' "$1"; exit 1; }

# --- Gate 1: branch base ------------------------------------------------------
printf '=== preflight 1/4: branch base ===\n'
if [ "$HOOK_FORMAT_ONLY" -eq 1 ]; then
  printf 'non-publication push — branch base not applicable\n'
else
git fetch origin --quiet || fail "git fetch origin"
# Fail loud if origin/main is absent — otherwise the git log pipelines below
# error mid-pipe, N_SESSIONS silently becomes 0, and the gate passes vacuously.
git rev-parse --verify --quiet origin/main >/dev/null \
  || fail "origin/main not found — missing remote-tracking ref; gate cannot run"
BASE_REF=origin/main
INTEGRATION=0
BRANCH=$(git rev-parse --abbrev-ref HEAD)
TIP=HEAD
if [ "$PROPOSED_SET" -eq 1 ]; then
  git cat-file -e "$PROPOSED_SHA^{commit}" 2>/dev/null \
    || fail "proposed SHA is not a local commit"
  TIP=$PROPOSED_SHA
fi
# Detect an intentional develop -> main integration from immutable ancestry, not
# from a branch-name convention. Daemon-owned branches are always named
# `daemon/<agent>-t<task>` and therefore cannot preserve a caller's
# `sync/develop-*` prefix. Require both *current* remote tips, and require
# develop to contain commits not already in main, so ordinary feature branches
# keep the stricter single-session check.
if git rev-parse --verify --quiet origin/develop >/dev/null \
  && ! git merge-base --is-ancestor origin/develop origin/main \
  && git merge-base --is-ancestor origin/main "$TIP" \
  && git merge-base --is-ancestor origin/develop "$TIP"; then
  BASE_REF=origin/develop
  INTEGRATION=1
fi
if [ "$BRANCH" = "main" ] && [ "$PROPOSED_SET" -eq 0 ]; then
  printf 'on main — nothing to compare, skipping\n'
else
  # Every branch-owned commit must be this push's own work. Multiple distinct
  # Co-Authored-By sessions in that range mean the branch was cut from another
  # feature branch (#114) — rebase onto the applicable base before pushing.
  if [ -n "$CONTINUATION_FROM" ]; then
    git cat-file -e "$CONTINUATION_FROM^{commit}" 2>/dev/null \
      || fail "continuation base is not a local commit"
    git merge-base --is-ancestor "$CONTINUATION_FROM" "$TIP" \
      || fail "continuation base is not an ancestor of proposed SHA"
    git cat-file -e "$CONTINUATION_BASE^{commit}" 2>/dev/null \
      || fail "configured continuation base is not a local commit"
    # A continuation may merge the freshly fetched base to preserve the
    # published PR head as an ancestor. Commits already reachable from the
    # base are inherited integration input, not work authored by this worker.
    # Exclude both histories while retaining the merge and worker commits.
    BASE_REF="$CONTINUATION_FROM + $CONTINUATION_BASE"
    OWN_COMMITS=$(git rev-list "$TIP" --not "$CONTINUATION_FROM" "$CONTINUATION_BASE")
    printf 'continuation-owned commits excluding published head and %s:\n' \
      "$CONTINUATION_BASE"
    if [ -n "$OWN_COMMITS" ]; then
      git show -s --oneline $OWN_COMMITS
    fi
  elif [ "$INTEGRATION" -eq 1 ]; then
    # Main's commits are inherited integration input, not work authored by the
    # sync session. Inspect only commits belonging to neither remote history.
    OWN_COMMITS=$(git rev-list "$TIP" --not origin/main origin/develop)
    printf 'integration-only commits:\n'
    if [ -n "$OWN_COMMITS" ]; then
      git show -s --oneline $OWN_COMMITS
    fi
  else
    OWN_COMMITS=$(git rev-list "$BASE_REF".."$TIP")
    printf 'commits ahead of %s:\n' "$BASE_REF"
    git log --oneline "$BASE_REF".."$TIP"
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
  printf 'branch base OK (%s session(s) ahead of %s)\n' "$N_SESSIONS" "$BASE_REF"
fi
fi

# --- Gate 2: cargo fmt --------------------------------------------------------
printf '=== preflight 2/4: cargo fmt --all -- --check ===\n'
cargo fmt --all -- --check || fail "cargo fmt"
printf 'fmt OK\n'

if [ "$QUICK" -eq 1 ]; then
  printf '\nPREFLIGHT: PASS (quick — gates 1-2; run without --quick before quorum submit)\n'
  exit 0
fi

# --- Gate 3: cargo clippy -----------------------------------------------------
printf '=== preflight 3/4: cargo clippy --all-targets --all-features --features quorum-core/test-support -- -D warnings ===\n'
cargo clippy --all-targets --all-features --features quorum-core/test-support -- -D warnings || fail "cargo clippy"
printf 'clippy OK\n'

# --- Gate 4: cargo test -------------------------------------------------------
TEST_THREADS="${RUST_TEST_THREADS:-4}"
printf '=== preflight 4/4: RUST_TEST_THREADS=%s cargo test --workspace --all-features --features quorum-core/test-support ===\n' "$TEST_THREADS"
RUST_TEST_THREADS="$TEST_THREADS" cargo test --workspace --all-features --features quorum-core/test-support || fail "cargo test"

printf '\nPREFLIGHT: PASS (all 4 gates green)\n'
