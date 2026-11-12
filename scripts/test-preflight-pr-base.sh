#!/bin/sh
# Regression tests for preflight's pull-request branch-base mode (`--pr-base`),
# which the CI `branch-base` job runs so the pre-push hook's branch-base gate
# still applies to pushes that bypass hooks.

set -eu

ROOT=$(git rev-parse --show-toplevel)
PREFLIGHT="$ROOT/preflight.sh"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

REMOTE="$TMP/origin.git"
REPO="$TMP/repo"
BIN="$TMP/bin"
CARGO_LOG="$TMP/cargo.log"
git init --bare -q "$REMOTE"
git init -q -b main "$REPO"
mkdir -p "$BIN"
: >"$CARGO_LOG"

# The CI job installs no Rust toolchain. Any Cargo invocation is a failure.
cat >"$BIN/cargo" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >> "$CARGO_LOG"
exit 1
EOF
chmod +x "$BIN/cargo"

cd "$REPO"
git config user.name CI
git config user.email ci@example.invalid
git remote add origin "$REMOTE"

# commit_as SESSION FILE — one commit carrying SESSION's Co-Authored-By trailer.
commit_as() {
  printf '%s\n' "$1" >> "$2"
  git add "$2"
  git commit -qm "work by $1" -m "Co-Authored-By: $1 <$1@example.invalid>"
}

# pr_check NAME BASE HEAD [extra preflight args...] — the CI job's invocation.
pr_check() {
  name=$1
  base=$2
  head=$3
  shift 3
  PATH="$BIN:$PATH" "$PREFLIGHT" --quick --pr-base="$base" \
    --proposed-sha="$head" "$@" >"$TMP/$name.out" 2>&1
}

expect_pass() {
  name=$1
  if ! pr_check "$@"; then
    cat "$TMP/$name.out" >&2
    echo "expected $name to pass" >&2
    exit 1
  fi
  grep -q 'PREFLIGHT: PASS (branch base for a pull request into' "$TMP/$name.out"
}

# expect_fail NAME PATTERN BASE HEAD [extra preflight args...]
expect_fail() {
  name=$1
  pattern=$2
  shift 2
  if pr_check "$name" "$@"; then
    cat "$TMP/$name.out" >&2
    echo "expected $name to fail" >&2
    exit 1
  fi
  grep -q "$pattern" "$TMP/$name.out" || {
    cat "$TMP/$name.out" >&2
    echo "expected $name output to match: $pattern" >&2
    exit 1
  }
}

printf 'base\n' > state
git add state
git commit -qm 'base'
git push -q origin main

# develop carries two sessions not yet synced to main.
git switch -qc develop
commit_as Dev-A develop-work
DEVELOP_OLD=$(git rev-parse HEAD)
commit_as Dev-B develop-work
git push -q origin develop

# main advances independently (an earlier hotfix not yet back-merged).
git switch -q main
commit_as Hot-0 hotfix-0
git push -q origin main
git fetch -q origin

# --- Pull requests into develop ----------------------------------------------

git switch -q --detach origin/develop
commit_as Feat-A feature
FEATURE=$(git rev-parse HEAD)
expect_pass feature-current-develop develop "$FEATURE"

git switch -q --detach "$DEVELOP_OLD"
commit_as Feat-Old feature-old
FEATURE_OLD=$(git rev-parse HEAD)
expect_pass feature-older-develop develop "$FEATURE_OLD"

# main -> develop back-merges: main itself, or a merge of main into develop.
expect_pass back-merge-main-head develop "$(git rev-parse origin/main)"
git switch -q --detach origin/develop
git merge -q --no-ff origin/main -m 'Merge branch main into develop'
expect_pass back-merge-commit develop "$(git rev-parse HEAD)"

# A branch cut from another unmerged feature branch carries two sessions.
git switch -q --detach "$FEATURE"
commit_as Stack-B feature
STACKED=$(git rev-parse HEAD)
expect_fail stacked-feature 'sessions in branch-owned commits' develop "$STACKED"

# --- Pull requests into main -------------------------------------------------

git switch -q --detach origin/main
commit_as Hot-1 hotfix-1
HOTFIX=$(git rev-parse HEAD)
expect_pass hotfix-from-main main "$HOTFIX"

# A hotfix continuation that merges the advanced main is still a hotfix.
git switch -q main
commit_as Hot-2 hotfix-2
git push -q origin main
git fetch -q origin
git switch -q --detach "$HOTFIX"
git merge -q --no-ff origin/main -m 'merge advanced main into hotfix'
commit_as Hot-1 hotfix-1
expect_pass hotfix-continuation main "$(git rev-parse HEAD)" \
  --continuation-from="$HOTFIX"

# Feature branches from develop must not target main, current or older.
expect_fail feature-into-main 'PR into main carries 2 origin/develop commit(s)' \
  main "$FEATURE"
expect_fail older-feature-into-main 'PR into main carries 1 origin/develop commit(s)' \
  main "$FEATURE_OLD"

# develop -> main syncs: develop itself, or a promotion merge on top of main.
expect_pass sync-develop-head main "$(git rev-parse origin/develop)"
grep -q 'develop -> main sync: 2 origin/develop commit(s)' \
  "$TMP/sync-develop-head.out"
git switch -q --detach origin/main
git merge -q --no-ff origin/develop -m 'chore(release): promote develop to main' \
  -m 'Co-Authored-By: Release <release@example.invalid>'
PROMOTION=$(git rev-parse HEAD)
expect_pass sync-promotion-merge main "$PROMOTION"

# A sync that adds authored work of its own (even from the same session) is
# feature work aimed at main.
commit_as Release sync-extra
expect_fail sync-with-authored-work 'plus its own work' main "$(git rev-parse HEAD)"

# --- synchronize: one push at a time, like the hook ---------------------------

# develop advances with two more sessions after the feature PR opened.
git switch -q develop
commit_as Dev-C develop-work
commit_as Dev-D develop-work
DEVELOP_SYNC_BEFORE=$(git rev-parse HEAD~2)
git push -q origin develop
git fetch -q origin

# A remediation round by a different session merges the fresh base and adds its
# own commit. As a continuation push it passes; judged as a whole branch it
# would mix the original author and the remediation session.
git switch -q --detach "$FEATURE"
git merge -q --no-ff origin/develop -m 'merge origin/develop into remediation'
commit_as Remediate-B feature
REMEDIATION=$(git rev-parse HEAD)
expect_pass remediation-push develop "$REMEDIATION" --continuation-from="$FEATURE"
grep -q 'continuation-owned commits excluding published head' \
  "$TMP/remediation-push.out"
expect_fail remediation-whole-branch 'sessions in branch-owned commits' \
  develop "$REMEDIATION"

# Two new sessions in one continuation push remain stacking.
git switch -q --detach "$FEATURE"
commit_as Push-X feature
commit_as Push-Y feature
expect_fail stacked-push 'sessions in branch-owned commits' \
  develop "$(git rev-parse HEAD)" --continuation-from="$FEATURE"

# A sync PR whose head is develop advances with every merge into develop.
expect_pass sync-pr-advances main "$(git rev-parse origin/develop)" \
  --continuation-from="$DEVELOP_SYNC_BEFORE"

# A rewritten PR head (force push / rebase update) is checked as a new branch.
git switch -q --detach origin/develop
commit_as Feat-A feature
REWRITTEN=$(git rev-parse HEAD)
expect_pass rewritten-head develop "$REWRITTEN" --continuation-from="$FEATURE"
grep -q 'is not an ancestor' "$TMP/rewritten-head.out"
git switch -q --detach origin/develop
commit_as Feat-A feature
commit_as Stack-C feature
expect_fail rewritten-stacked 'sessions in branch-owned commits' \
  develop "$(git rev-parse HEAD)" --continuation-from="$FEATURE"
expect_fail unknown-previous-head 'sessions in branch-owned commits' \
  develop "$(git rev-parse HEAD)" \
  --continuation-from=0123456789abcdef0123456789abcdef01234567

# A PR into any other branch continues from that branch, as the daemon does
# for a task whose base branch is neither main nor develop.
git switch -q --detach origin/main
git switch -qc release
commit_as Rel-A release-work
git push -q origin release
git switch -q --detach origin/release
commit_as Release-PR release-pr
RELEASE_PR=$(git rev-parse HEAD)
git switch -q release
commit_as Rel-B release-work
commit_as Rel-C release-work
git push -q origin release
git fetch -q origin
git switch -q --detach "$RELEASE_PR"
git merge -q --no-ff origin/release -m 'merge origin/release into PR'
commit_as Release-PR release-pr
expect_pass other-base-continuation release "$(git rev-parse HEAD)" \
  --continuation-from="$RELEASE_PR"

# --- Usage and environment errors --------------------------------------------

expect_usage() {
  name=$1
  shift
  set +e
  PATH="$BIN:$PATH" "$PREFLIGHT" "$@" >"$TMP/$name.out" 2>&1
  status=$?
  set -e
  [ "$status" -eq 2 ] || {
    cat "$TMP/$name.out" >&2
    echo "expected $name to exit 2, got $status" >&2
    exit 1
  }
}
expect_usage usage-no-quick --pr-base=develop --proposed-sha="$FEATURE"
expect_usage usage-no-sha --quick --pr-base=develop
expect_usage usage-empty-base --quick --pr-base= --proposed-sha="$FEATURE"
expect_usage usage-bad-base --quick '--pr-base=bad..name' --proposed-sha="$FEATURE"
expect_usage usage-dup-base --quick --pr-base=develop --pr-base=main \
  --proposed-sha="$FEATURE"
expect_usage usage-explicit-continuation-base --quick --pr-base=develop \
  --proposed-sha="$REMEDIATION" --continuation-from="$FEATURE" \
  --continuation-base=origin/develop
expect_usage usage-format-only --quick --pr-base=develop --hook-format-only \
  --proposed-sha="$FEATURE"

expect_fail missing-base 'PR base origin/no-such-base not found' no-such-base "$FEATURE"

# Pull-request mode never reaches the Cargo gates.
if [ -s "$CARGO_LOG" ]; then
  cat "$CARGO_LOG" >&2
  echo 'expected --pr-base mode to run no cargo command' >&2
  exit 1
fi

# CI wires the job ahead of the expensive test job.
grep -Fqx '  branch-base:' "$ROOT/.github/workflows/ci.yml"
grep -Fq -- '--pr-base="$PR_BASE" --proposed-sha="$PR_HEAD_SHA"' \
  "$ROOT/.github/workflows/ci.yml"
grep -Fq -- "github.event.action == 'synchronize' && github.event.before || ''" \
  "$ROOT/.github/workflows/ci.yml"

echo 'test-preflight-pr-base: PASS'
