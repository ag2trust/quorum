#!/bin/sh
# dev-install.sh — build from source, replace the installed binary, verify.
#
# This is the development counterpart of install.sh (which downloads from GitHub Releases).
# Use it after pulling new source, or when the installed binary is stale. Re-run to upgrade.
#
# The verification step is load-bearing: the 2026-06-26 cutover stalled because a stale
# binary at ~/.local/bin/quorum lacked the `sync` subcommand, and there was no script that
# built + replaced + verified in one shot (#74).
#
# Usage:
#   ./dev-install.sh               # build + install + verify
#   ./dev-install.sh --verify-only  # skip build, just verify the installed binary
#
# Env overrides:
#   QUORUM_INSTALL_DIR   install destination (default: ~/.local/bin)

set -eu

INSTALL_DIR="${QUORUM_INSTALL_DIR:-$HOME/.local/bin}"
BINARY="$INSTALL_DIR/quorum"
VERIFY_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --verify-only) VERIFY_ONLY=1 ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) printf 'dev-install.sh: unknown arg: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

err() { printf 'dev-install.sh: %s\n' "$1" >&2; exit 1; }

if [ "$VERIFY_ONLY" -eq 0 ]; then
  # --- Build ---------------------------------------------------------------
  printf '=== Building quorum (release) ===\n'
  cargo build --release || err "cargo build --release failed"

  BUILT="target/release/quorum"
  [ -f "$BUILT" ] || err "expected binary at $BUILT after build"

  # --- Install -------------------------------------------------------------
  printf '=== Installing to %s ===\n' "$BINARY"
  mkdir -p "$INSTALL_DIR"
  cp "$BUILT" "$BINARY"
  chmod 0755 "$BINARY"
fi

# --- Verify ----------------------------------------------------------------
printf '=== Verifying installed binary ===\n'

[ -f "$BINARY" ] || err "binary not found at $BINARY"
[ -x "$BINARY" ] || err "binary at $BINARY is not executable"

# 1. Version
VERSION="$("$BINARY" --version 2>&1)" || err "'quorum --version' failed"
printf '  version : %s\n' "$VERSION"

# 2. Required subcommands — sync is the one that was missing in the cutover incident.
HELP="$("$BINARY" help 2>&1)" || err "'quorum help' failed"
for cmd in sync init status; do
  if ! printf '%s' "$HELP" | grep -q "$cmd"; then
    err "installed binary lacks '$cmd' subcommand — stale build?"
  fi
done
printf '  commands: sync, init, status — present\n'

# 3. Schema migration on existing DB (non-destructive: init is idempotent).
INIT_OUT="$("$BINARY" init 2>&1)" || err "'quorum init' failed"
SCHEMA_V="$(printf '%s' "$INIT_OUT" | grep -o '"schema_version":[0-9]*' | head -1 | cut -d: -f2)"
MIGRATED="$(printf '%s' "$INIT_OUT" | grep -o '"migrated_from":[0-9]*' | head -1 | cut -d: -f2)"
if [ -n "$SCHEMA_V" ]; then
  if [ -n "$MIGRATED" ] && [ "$MIGRATED" != "$SCHEMA_V" ]; then
    printf '  schema  : migrated %s → %s\n' "$MIGRATED" "$SCHEMA_V"
  else
    printf '  schema  : v%s (up to date)\n' "$SCHEMA_V"
  fi
else
  printf '  schema  : init OK (version not reported — older binary format)\n'
fi

printf '=== OK: quorum verified at %s ===\n' "$BINARY"
