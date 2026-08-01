#!/bin/sh
# test-dev-install.sh — shell tests for dev-install.sh command verification.
#
# Usage:
#   scripts/test-dev-install.sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DEV_INSTALL="$SCRIPT_DIR/dev-install.sh"

PASS=0
FAIL=0
TESTS=0

pass() { PASS=$((PASS + 1)); TESTS=$((TESTS + 1)); printf '  PASS: %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); TESTS=$((TESTS + 1)); printf '  FAIL: %s\n' "$1"; }

TMPDIR_TEST=$(mktemp -d)
cleanup() { rm -rf "$TMPDIR_TEST"; }
trap cleanup EXIT

make_stub_quorum() {
  stub_path="$1"
  sync_exit="$2"
  guide_text="$3"
  cat > "$stub_path" <<STUB
#!/bin/sh
case "\$1" in
  --version) printf 'quorum 0.0.0-test\\n' ;;
  help) printf '%s\\n' '$guide_text' ;;
  sync) [ "\${2:-}" = "--help" ] && exit $sync_exit; exit 2 ;;
  init) [ "\${2:-}" = "--help" ] && exit 0; printf '%s\\n' '{"schema_version":1,"migrated_from":1}' ;;
  status) [ "\${2:-}" = "--help" ] && exit 0; exit 2 ;;
  *) exit 2 ;;
esac
STUB
  chmod +x "$stub_path"
}

run_verify() {
  install_dir="$1"
  output_path="$2"
  QUORUM_INSTALL_DIR="$install_dir" \
  QUORUM_SKILL_DIR="$TMPDIR_TEST/skills" \
  "$DEV_INSTALL" --verify-only >"$output_path" 2>&1
}

printf 'test-dev-install: running tests\n\n'

# `sync` is intentionally absent from the curated guide, but direct --help works.
printf 'test 1: hidden sync command passes direct verification\n'
pass_dir="$TMPDIR_TEST/passes"
mkdir -p "$pass_dir"
make_stub_quorum "$pass_dir/quorum" 0 'Agent guide: manage daemon work.'
pass_output="$TMPDIR_TEST/pass-output"
if run_verify "$pass_dir" "$pass_output"; then
  pass "hidden sync passes when its direct --help probe succeeds"
else
  fail "hidden sync passes when its direct --help probe succeeds"
fi

# Incidental prose must not make a missing command look valid.
printf '\ntest 2: missing sync command fails despite guide prose\n'
fail_dir="$TMPDIR_TEST/fails"
mkdir -p "$fail_dir"
make_stub_quorum "$fail_dir/quorum" 2 'Agent guide: synchronize work.'
fail_output="$TMPDIR_TEST/fail-output"
verify_exit=0
run_verify "$fail_dir" "$fail_output" || verify_exit=$?
if [ "$verify_exit" -eq 1 ]; then
  pass "missing sync exits 1"
else
  fail "missing sync exits 1 (got $verify_exit)"
fi
if grep -Fq "installed binary lacks 'sync' subcommand — stale build?" "$fail_output"; then
  pass "missing sync reports useful error"
else
  fail "missing sync reports useful error"
fi

printf '\n%d tests: %d passed, %d failed\n' "$TESTS" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
