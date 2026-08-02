#!/bin/sh
# test-dogfood-install.sh — isolation tests for dogfood-install.sh.

set -eu

REPO_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
INSTALLER="$REPO_DIR/dogfood-install.sh"
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

install_dir="$TEST_ROOT/bin"
mkdir -p "$install_dir"
cat > "$install_dir/quorum-dev" <<'STUB'
#!/bin/sh
case "${1:-}" in
  --version) printf 'quorum 0.0.0-dogfood\n' ;;
  serve) [ "${2:-}" = "--help" ] || exit 2 ;;
  *) exit 2 ;;
esac
STUB
chmod +x "$install_dir/quorum-dev"

stable_home="$TEST_ROOT/stable-home"
mkdir -p "$stable_home"
printf 'stable-state\n' > "$stable_home/sentinel"

HOME="$stable_home" \
QUORUM_DOGFOOD_INSTALL_DIR="$install_dir" \
"$INSTALLER" --verify-only > "$TEST_ROOT/output"

grep -Fq 'quorum 0.0.0-dogfood' "$TEST_ROOT/output"
grep -Fqx 'stable-state' "$stable_home/sentinel"
[ ! -e "$install_dir/quorum" ]
[ ! -e "$stable_home/.quorum" ]
[ ! -e "$stable_home/.claude" ]

printf 'test-dogfood-install: PASS\n'

