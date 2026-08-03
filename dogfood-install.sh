#!/bin/sh
# dogfood-install.sh — build the current checkout as an isolated `quorum-dev` binary.
#
# This intentionally does not run `dev-install.sh`: it must not replace the stable
# `quorum` binary, open or migrate ~/.quorum, install agent skills, or change Git hooks.
# The resulting binary uses Quorum's normal home/config when invoked unless the caller
# explicitly sets QUORUM_HOME.
#
# Usage:
#   ./dogfood-install.sh
#   ./dogfood-install.sh --verify-only
#
# Env overrides:
#   QUORUM_DOGFOOD_INSTALL_DIR  destination directory (default: ~/.local/bin)
#   CARGO_TARGET_DIR            Cargo build directory (standard Cargo override)

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_DIR="${QUORUM_DOGFOOD_INSTALL_DIR:-$HOME/.local/bin}"
BINARY="$INSTALL_DIR/quorum-dev"
VERIFY_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --verify-only) VERIFY_ONLY=1 ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) printf 'dogfood-install.sh: unknown arg: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

err() { printf 'dogfood-install.sh: %s\n' "$1" >&2; exit 1; }

if [ "$VERIFY_ONLY" -eq 0 ]; then
  printf '=== Building dogfood quorum from %s ===\n' "$SCRIPT_DIR"
  (cd "$SCRIPT_DIR" && cargo build --release) \
    || err "cargo build --release failed"

  TARGET_DIR="${CARGO_TARGET_DIR:-$SCRIPT_DIR/target}"
  case "$TARGET_DIR" in
    /*) ;;
    *) TARGET_DIR="$SCRIPT_DIR/$TARGET_DIR" ;;
  esac
  BUILT="$TARGET_DIR/release/quorum"
  [ -f "$BUILT" ] || err "expected binary at $BUILT after build"

  printf '=== Installing dogfood binary to %s ===\n' "$BINARY"
  mkdir -p "$INSTALL_DIR"
  TEMP_BINARY=$(mktemp "$INSTALL_DIR/.quorum-dev.tmp.XXXXXX") \
    || err "could not create temporary binary in $INSTALL_DIR"
  trap 'rm -f "$TEMP_BINARY"' EXIT HUP INT TERM
  cp "$BUILT" "$TEMP_BINARY"
  chmod 0755 "$TEMP_BINARY"
  mv "$TEMP_BINARY" "$BINARY"
  trap - EXIT HUP INT TERM
fi

printf '=== Verifying dogfood binary ===\n'
[ -f "$BINARY" ] || err "binary not found at $BINARY"
[ -x "$BINARY" ] || err "binary at $BINARY is not executable"

VERSION=$("$BINARY" --version 2>&1) || err "'quorum-dev --version' failed"
"$BINARY" serve --help >/dev/null 2>&1 || err "dogfood binary lacks 'serve' subcommand"
printf '  version: %s\n' "$VERSION"
printf '=== OK: dogfood quorum installed at %s ===\n' "$BINARY"
