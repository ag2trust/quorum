#!/bin/sh
# test-dogfood-install.sh — isolation tests for dogfood-install.sh.

set -eu

REPO_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

fake_bin="$TEST_ROOT/fake-bin"
mkdir -p "$fake_bin"
cat > "$fake_bin/cargo" <<'FAKE_CARGO'
#!/bin/sh
set -eu
[ "$1" = "build" ]
[ "$2" = "--release" ]
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
case "$target_dir" in
  /*) ;;
  *) target_dir="$PWD/$target_dir" ;;
esac
mkdir -p "$target_dir/release"
cat > "$target_dir/release/quorum" <<'STUB'
#!/bin/sh
case "${1:-}" in
  --version) printf 'quorum 0.0.0-dogfood\n' ;;
  serve) [ "${2:-}" = "--help" ] || exit 2 ;;
  *) exit 2 ;;
esac
STUB
chmod +x "$target_dir/release/quorum"
FAKE_CARGO
chmod +x "$fake_bin/cargo"

run_install_case() {
  case_name="$1"
  target_dir="$2"
  case_root="$TEST_ROOT/$case_name"
  source_dir="$case_root/source"
  install_dir="$case_root/bin"
  stable_home="$case_root/home"
  mkdir -p "$source_dir" "$install_dir" \
    "$stable_home/.quorum" "$stable_home/.claude/skills/quorum"
  cp "$REPO_DIR/dogfood-install.sh" "$source_dir/dogfood-install.sh"
  chmod +x "$source_dir/dogfood-install.sh"

  git -C "$source_dir" init -q
  git -C "$source_dir" config core.hooksPath stable-hooks
  printf 'stable-binary\n' > "$install_dir/quorum"
  printf 'stable-config\n' > "$stable_home/.quorum/config.toml"
  printf 'stable-db\n' > "$stable_home/.quorum/quorum.db"
  printf 'stable-skill\n' > "$stable_home/.claude/skills/quorum/SKILL.md"
  cp "$install_dir/quorum" "$case_root/quorum.before"
  cp "$stable_home/.quorum/config.toml" "$case_root/config.before"
  cp "$stable_home/.quorum/quorum.db" "$case_root/db.before"
  cp "$stable_home/.claude/skills/quorum/SKILL.md" "$case_root/skill.before"
  hooks_before=$(git -C "$source_dir" config --local core.hooksPath)

  HOME="$stable_home" \
  PATH="$fake_bin:$PATH" \
  QUORUM_DOGFOOD_INSTALL_DIR="$install_dir" \
  CARGO_TARGET_DIR="$target_dir" \
    "$source_dir/dogfood-install.sh" > "$case_root/output"

  grep -Fq 'quorum 0.0.0-dogfood' "$case_root/output"
  "$install_dir/quorum-dev" --version | grep -Fq 'quorum 0.0.0-dogfood'
  cmp "$case_root/quorum.before" "$install_dir/quorum"
  cmp "$case_root/config.before" "$stable_home/.quorum/config.toml"
  cmp "$case_root/db.before" "$stable_home/.quorum/quorum.db"
  cmp "$case_root/skill.before" "$stable_home/.claude/skills/quorum/SKILL.md"
  [ "$(git -C "$source_dir" config --local core.hooksPath)" = "$hooks_before" ]
  [ -z "$(find "$install_dir" -name '.quorum-dev.tmp.*' -print)" ]
}

run_install_case relative relative-target
run_install_case absolute "$TEST_ROOT/absolute-target"

printf 'test-dogfood-install: PASS\n'
