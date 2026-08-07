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

echo 'test-preflight: PASS'
