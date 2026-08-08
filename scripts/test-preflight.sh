#!/bin/sh
# Regression tests for preflight branch-base policy.

set -eu

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

REMOTE="$TMP/origin.git"
REPO="$TMP/repo"
BIN="$TMP/bin"
git init --bare -q "$REMOTE"
git init -q "$REPO"
mkdir -p "$BIN"

cat >"$BIN/cargo" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$BIN/cargo"

cd "$REPO"
git config user.name CI
git config user.email ci@example.invalid
git remote add origin "$REMOTE"

printf 'base\n' > state
git add state
git commit -qm 'base'
git branch -M main
git push -q origin main

git switch -qc develop
printf 'develop\n' >> state
git commit -qam 'develop work' -m 'Co-Authored-By: Develop-agent <develop@example.invalid>'
git push -q origin develop

git switch -q main
printf 'main\n' > main-only
git add main-only
git commit -qm 'main work' -m 'Co-Authored-By: Main-agent <main@example.invalid>'
git push -q origin main

# A daemon branch containing both current tips is a valid integration even
# though inherited commits carry different session trailers.
git switch -qc daemon/tester-t1
git merge -q --no-ff origin/develop -m 'integrate develop' \
  -m 'Co-Authored-By: Merge-agent <merge@example.invalid>'
cp "$ROOT/preflight.sh" ./preflight.sh
chmod +x ./preflight.sh
PATH="$BIN:$PATH" ./preflight.sh --quick >"$TMP/integration.out"
grep -q 'PREFLIGHT: PASS (quick' "$TMP/integration.out"

# Merely branching from develop is not integration: the branch omits current
# main and must remain subject to the normal branch-base rejection.
git switch -q --detach origin/develop
git switch -qc daemon/tester-t2
printf 'feature\n' > feature
git add feature
git commit -qm 'feature work' -m 'Co-Authored-By: Feature-agent <feature@example.invalid>'
cp "$ROOT/preflight.sh" ./preflight.sh
chmod +x ./preflight.sh
if PATH="$BIN:$PATH" ./preflight.sh --quick >"$TMP/feature.out" 2>&1; then
  echo 'expected develop-based feature branch to fail preflight' >&2
  exit 1
fi

# Install the real hook and prove its standard stdin tuple against a bare
# remote. The daemon publishes an exact SHA rather than a same-named local ref,
# so use that production-shaped refspec here too.
mkdir -p .githooks
cp "$ROOT/.githooks/pre-push" .githooks/pre-push
chmod +x .githooks/pre-push
git config core.hooksPath .githooks

git switch -q --detach origin/main
git switch -qc daemon/continuation-t3
printf 'worker A\n' > remediation
git add remediation
git commit -qm 'initial publication' \
  -m 'Co-Authored-By: Worker-A <worker-a@example.invalid>'
WORKER_A_SHA=$(git rev-parse HEAD)
PATH="$BIN:$PATH" git push -q origin \
  "$WORKER_A_SHA:refs/heads/daemon/continuation-t3"

printf 'worker B\n' >> remediation
git commit -qam 'sequential remediation' \
  -m 'Co-Authored-By: Worker-B <worker-b@example.invalid>'
WORKER_B_SHA=$(git rev-parse HEAD)
PATH="$BIN:$PATH" git push -q origin \
  "$WORKER_B_SHA:refs/heads/daemon/continuation-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/continuation-t3)
[ "$REMOTE_SHA" = "$WORKER_B_SHA" ]

# A genuinely stacked new branch still exposes both sessions because a zero
# remote SHA keeps the original origin/main..HEAD quick-gate range.
git switch -q --detach origin/main
git switch -qc daemon/stacked-t4
printf 'stack A\n' > stacked
git add stacked
git commit -qm 'stack base' \
  -m 'Co-Authored-By: Stack-A <stack-a@example.invalid>'
printf 'stack B\n' >> stacked
git commit -qam 'stacked feature' \
  -m 'Co-Authored-By: Stack-B <stack-b@example.invalid>'
STACKED_SHA=$(git rev-parse HEAD)
if PATH="$BIN:$PATH" git push -q origin \
  "$STACKED_SHA:refs/heads/daemon/stacked-t4" >"$TMP/stacked.out" 2>&1; then
  echo 'expected a newly stacked feature branch to fail pre-push' >&2
  exit 1
fi
grep -q 'sessions in branch-owned commits' "$TMP/stacked.out"

# The existing remote head must be an ancestor of the proposed update. Even a
# forced Git push cannot turn a stale/non-fast-forward tuple into a continuation.
git switch -q --detach origin/main
git switch -qc daemon/stale-t5
printf 'stale\n' > stale
git add stale
git commit -qm 'stale replacement' \
  -m 'Co-Authored-By: Stale-agent <stale@example.invalid>'
STALE_SHA=$(git rev-parse HEAD)
if PATH="$BIN:$PATH" git push --force -q origin \
  "$STALE_SHA:refs/heads/daemon/continuation-t3" >"$TMP/stale.out" 2>&1; then
  echo 'expected stale/non-fast-forward update to fail pre-push' >&2
  exit 1
fi
grep -q 'stale or non-fast-forward' "$TMP/stale.out"

ZERO_SHA=0000000000000000000000000000000000000000

# Reject malformed, deletion, wrong-mapping, and multi-ref tuples before the
# quick gate can inspect an unrelated HEAD.
if printf 'malformed\n' \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/malformed.out" 2>&1; then
  echo 'expected malformed pre-push input to fail' >&2
  exit 1
fi
grep -q 'malformed local SHA' "$TMP/malformed.out"

if printf '%s %s %s %s\n' \
  '(delete)' "$ZERO_SHA" refs/heads/daemon/continuation-t3 "$WORKER_B_SHA" \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/delete.out" 2>&1; then
  echo 'expected deleted ref to fail pre-push' >&2
  exit 1
fi
grep -q 'deleted refs are not permitted' "$TMP/delete.out"

if printf '%s %s %s %s\n' \
  refs/heads/main "$STALE_SHA" refs/heads/daemon/wrong-t6 "$ZERO_SHA" \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/wrong-map.out" 2>&1; then
  echo 'expected wrong local ref mapping to fail pre-push' >&2
  exit 1
fi
grep -q 'local ref does not map to local SHA' "$TMP/wrong-map.out"

if {
    printf '%s %s %s %s\n' \
      "$STALE_SHA" "$STALE_SHA" refs/heads/daemon/one-t7 "$ZERO_SHA"
    printf '%s %s %s %s\n' \
      "$STALE_SHA" "$STALE_SHA" refs/heads/daemon/two-t8 "$ZERO_SHA"
  } | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/multi-ref.out" 2>&1; then
  echo 'expected multiple ref mappings to fail pre-push' >&2
  exit 1
fi
grep -q 'expected exactly one ref update' "$TMP/multi-ref.out"

# Continuation ranges are hook-only quick-gate input and cannot weaken the
# mandatory full preflight invocation.
if PATH="$BIN:$PATH" ./preflight.sh \
  --continuation-from="$WORKER_A_SHA" >"$TMP/full-continuation.out" 2>&1; then
  echo 'expected full preflight to reject continuation range input' >&2
  exit 1
fi
grep -q 'requires --quick' "$TMP/full-continuation.out"

echo 'test-preflight: PASS'
