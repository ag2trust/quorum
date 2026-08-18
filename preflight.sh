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
#   4. cargo test    — compile/no-run then full execution, timed by the collector
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

# Full preflight writes its timing evidence to this deterministic path. The
# collector owns the Cargo gates and the per-test-binary top-N list; preflight
# contributes the branch-base gate, whose work necessarily happens before the
# collector can run.
TIMING_DIR=target/preflight-timing
TIMING_ARTIFACT="$TIMING_DIR/timing.json"
TIMING_SUMMARY="$TIMING_DIR/summary.txt"
GREEN_CACHE="$TIMING_DIR/last-green.json"

# Gates 2-4 are expensive but purely a function of the working tree.  Keep
# their cache deliberately local to target/: the branch-base gate still runs
# on every full invocation because origin/main can advance independently.
#
# The fingerprint includes tracked state plus both ordinary and ignored
# untracked inputs.  target/ is the one intentional exception: it contains
# Cargo's generated outputs and this cache itself, neither of which is an
# author input to the gates.  If anything about collection is uncertain, the
# caller treats it as a cache miss and runs the gates.
working_tree_fingerprint() {
  python3 - <<'PY'
import hashlib
import os
import stat
import subprocess
import sys

def git(*args):
    return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL)

def add(hasher, label, value):
    hasher.update(label)
    hasher.update(len(value).to_bytes(8, "big"))
    hasher.update(value)

try:
    head = git("rev-parse", "--verify", "HEAD^{commit}")
    status = git("status", "--porcelain=v1", "-z", "--untracked-files=all")
    diff = git("-c", "core.quotepath=false", "diff", "--binary", "--no-ext-diff", "HEAD")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z")
    ignored = git("ls-files", "--others", "--ignored", "--exclude-standard", "-z")

    paths = set(untracked.split(b"\0")[:-1]) | set(ignored.split(b"\0")[:-1])
    hasher = hashlib.sha256()
    add(hasher, b"HEAD\0", head)
    add(hasher, b"STATUS\0", status)
    add(hasher, b"DIFF\0", diff)
    for path in sorted(path for path in paths if path != b"target" and not path.startswith(b"target/")):
        metadata = os.lstat(path)
        add(hasher, b"PATH\0", path)
        add(hasher, b"MODE\0", str(stat.S_IMODE(metadata.st_mode)).encode())
        if stat.S_ISREG(metadata.st_mode):
            with open(path, "rb") as source:
                add(hasher, b"FILE\0", source.read())
        elif stat.S_ISLNK(metadata.st_mode):
            add(hasher, b"LINK\0", os.readlink(path))
        else:
            raise RuntimeError(f"unsupported untracked input: {path!r}")
except BaseException as error:
    print(f"preflight.sh: cannot fingerprint working tree: {error}", file=sys.stderr)
    sys.exit(1)

print(hasher.hexdigest())
PY
}

has_green_cache() {
  python3 - "$GREEN_CACHE" "$1" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as source:
        cached = json.load(source)
    expected = {"exit": 0, "fingerprint": sys.argv[2]}
    if cached != expected:
        raise ValueError("cache entry is not an exact green fingerprint")
except BaseException:
    sys.exit(1)
PY
}

write_green_cache() {
  python3 - "$GREEN_CACHE" "$1" <<'PY'
import json
import os
import sys
import tempfile

cache = sys.argv[1]
directory = os.path.dirname(cache)
os.makedirs(directory, exist_ok=True)
fd, temporary = tempfile.mkstemp(prefix=".last-green.", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump({"exit": 0, "fingerprint": sys.argv[2]}, output, sort_keys=True)
        output.write("\n")
    os.replace(temporary, cache)
except BaseException:
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
    raise
PY
}

monotonic_now() {
  python3 -c 'import time; print(time.monotonic())'
}

record_branch_base_timing() {
  python3 - "$TIMING_ARTIFACT" "$TIMING_SUMMARY" "$1" <<'PY'
import json
import os
import sys
import tempfile
from pathlib import Path

artifact = Path(sys.argv[1])
summary = Path(sys.argv[2])
duration = round(float(sys.argv[3]), 3)

data = json.loads(artifact.read_text())
gates = [gate for gate in data.get("gates", [])
         if gate.get("name") != "branch_base"]
gates.insert(0, {
    "name": "branch_base",
    "duration_secs": duration,
    "exit_code": 0,
})
data["gates"] = gates

def replace_atomically(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise

replace_atomically(artifact, json.dumps(data, indent=2, sort_keys=True) + "\n")

branch_line = f"  {'branch_base':<24} {duration:>10.2f}s  ok\n"
summary_text = summary.read_text()
marker = "gates:\n"
if marker not in summary_text:
    raise RuntimeError(f"timing summary missing {marker!r}")
replace_atomically(summary, summary_text.replace(marker, marker + branch_line, 1))
PY
}

timing_failure_gate() {
  python3 - "$TIMING_ARTIFACT" <<'PY'
import json
import sys

for gate in json.load(open(sys.argv[1])).get("gates", []):
    if gate.get("exit_code"):
        print(gate.get("name", ""))
        break
PY
}

# --- Gate 1: branch base ------------------------------------------------------
printf '=== preflight 1/4: branch base ===\n'
if [ "$QUICK" -eq 0 ]; then
  BRANCH_BASE_STARTED=$(monotonic_now) || fail "start branch-base timer"
fi
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
# A full author gate has no pre-push tuple, but a daemon-prepared PR
# continuation still has authoritative local evidence: its configured remote
# upstream is the published PR head. Recognize that history only when the
# upstream is on TIP's first-parent chain and a later first-parent merge
# contains the freshly fetched origin/main. This keeps an ordinary branch that
# merely tracks another feature branch on the strict origin/main..TIP path.
CONFIGURED_CONTINUATION_FROM=
if [ "$QUICK" -eq 0 ] && [ "$PROPOSED_SET" -eq 0 ]; then
  CONFIGURED_UPSTREAM_REF=$(git rev-parse --symbolic-full-name '@{upstream}' 2>/dev/null)
  case "$CONFIGURED_UPSTREAM_REF" in
    refs/remotes/origin/main|refs/remotes/origin/develop|'') ;;
    refs/remotes/origin/*)
      CONFIGURED_UPSTREAM_SHA=$(git rev-parse --verify "$CONFIGURED_UPSTREAM_REF^{commit}" 2>/dev/null)
      if [ -n "$CONFIGURED_UPSTREAM_SHA" ] \
        && git rev-list --first-parent "$TIP" | grep -Fqx "$CONFIGURED_UPSTREAM_SHA"; then
        for MERGE_SHA in $(git rev-list --first-parent --merges \
          "$CONFIGURED_UPSTREAM_SHA..$TIP"); do
          if git merge-base --is-ancestor origin/main "$MERGE_SHA"; then
            CONFIGURED_CONTINUATION_FROM=$CONFIGURED_UPSTREAM_SHA
            break
          fi
        done
      fi
      ;;
  esac
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
  elif [ -n "$CONFIGURED_CONTINUATION_FROM" ]; then
    BASE_REF="$CONFIGURED_UPSTREAM_REF + origin/main"
    OWN_COMMITS=$(git rev-list "$TIP" --not \
      "$CONFIGURED_CONTINUATION_FROM" origin/main)
    printf 'configured-upstream continuation-owned commits excluding published head and origin/main:\n'
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
if [ "$QUICK" -eq 1 ]; then
  printf '=== preflight 2/4: cargo fmt --all -- --check ===\n'
  cargo fmt --all -- --check || fail "cargo fmt"
  printf 'fmt OK\n'
  printf '\nPREFLIGHT: PASS (quick — gates 1-2; run without --quick before quorum submit)\n'
  exit 0
fi

# --- Gates 2-4: timing collector ---------------------------------------------
TEST_THREADS="${RUST_TEST_THREADS:-4}"
TEST_JOBS="${PREFLIGHT_TEST_JOBS:-2}"
BRANCH_BASE_DURATION=$(python3 - "$BRANCH_BASE_STARTED" <<'PY'
import sys
import time
print(time.monotonic() - float(sys.argv[1]))
PY
) || fail "finish branch-base timer"

FULL_GATE_FINGERPRINT=
if FULL_GATE_FINGERPRINT=$(working_tree_fingerprint); then
  if has_green_cache "$FULL_GATE_FINGERPRINT"; then
    printf '\nPREFLIGHT: PASS (cached — tree unchanged since last green run)\n'
    exit 0
  fi
else
  printf 'preflight.sh: fingerprint unavailable; running full gates\n' >&2
fi

printf '=== preflight 2-4: timing collector (fmt, clippy, compile/no-run, test execution) ===\n'
if scripts/preflight/timing.sh \
  --test-jobs "$TEST_JOBS" --test-threads "$TEST_THREADS"; then
  record_branch_base_timing "$BRANCH_BASE_DURATION" \
    || fail "publish branch-base timing"
else
  TIMING_STATUS=$?
  record_branch_base_timing "$BRANCH_BASE_DURATION" \
    || fail "publish branch-base timing"
  TIMING_FAILED_GATE=$(timing_failure_gate) || fail "read timing failure"
  case "$TIMING_FAILED_GATE" in
    cargo_fmt) fail "cargo fmt" ;;
    cargo_clippy) fail "cargo clippy" ;;
    cargo_test_no_run)
      # The collector keeps structured Cargo stdout separately, but Cargo's
      # actionable compiler diagnostics are stderr. Replay it verbatim before
      # failing as the original `cargo test` gate would have done.
      cat "$TIMING_DIR/cargo-test-no-run.stderr" >&2
      fail "cargo test"
      ;;
    test_execute) fail "cargo test" ;;
    *)
      printf 'preflight.sh: timing collector failed (exit %s)\n' \
        "$TIMING_STATUS" >&2
      exit "$TIMING_STATUS"
      ;;
  esac
fi

if [ -n "$FULL_GATE_FINGERPRINT" ]; then
  if FINAL_FINGERPRINT=$(working_tree_fingerprint); then
    if [ "$FINAL_FINGERPRINT" = "$FULL_GATE_FINGERPRINT" ]; then
      write_green_cache "$FINAL_FINGERPRINT" \
        || printf 'preflight.sh: could not write green cache; next run will be full\n' >&2
    else
      printf 'preflight.sh: tree changed while gates ran; green cache not written\n' >&2
    fi
  else
    printf 'preflight.sh: could not fingerprint green tree; cache not written\n' >&2
  fi
fi

printf '\nPREFLIGHT: PASS (all 4 gates green)\n'
