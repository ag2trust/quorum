#!/bin/sh
# install.sh — toolchain-free installer for the `quorum` binary (#60).
#
# Downloads the prebuilt release binary for this OS/arch from GitHub Releases, verifies its
# SHA-256, and installs it to ~/.local/bin/ (no Rust/cargo required). Re-run to upgrade.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/ag2trust/quorum/main/install.sh | sh
#   ./install.sh [VERSION]            # VERSION like v0.2.0; default = latest release
#
# Env overrides:
#   QUORUM_VERSION       pin a release tag (same as the positional arg)
#   QUORUM_INSTALL_DIR   install destination (default: ~/.local/bin)
#   QUORUM_DRY_RUN=1     resolve target + URL and print the plan, then exit (no download)
#
# Fails loud (non-zero exit) on any unsupported platform, missing tool, download error, or
# checksum mismatch — never installs a partial or unverified binary.

set -eu

REPO="ag2trust/quorum"
INSTALL_DIR="${QUORUM_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${1:-${QUORUM_VERSION:-latest}}"

err() { printf 'install.sh: %s\n' "$1" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

# --- Resolve the release target triple for this host ------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)
    case "$arch" in
      x86_64 | amd64) target="x86_64-unknown-linux-gnu" ;;
      *) err "no prebuilt binary for Linux/$arch — build from source: cargo build --release" ;;
    esac
    ;;
  Darwin)
    case "$arch" in
      arm64 | aarch64) target="aarch64-apple-darwin" ;;
      x86_64) target="x86_64-apple-darwin" ;;
      *) err "no prebuilt binary for macOS/$arch — build from source: cargo build --release" ;;
    esac
    ;;
  *)
    err "unsupported OS '$os' — build from source: cargo build --release"
    ;;
esac

# --- Pick a downloader (curl or wget) ---------------------------------------------------
if have curl; then
  dl() { curl -fsSL "$1" -o "$2"; }
  fetch() { curl -fsSL "$1"; }
elif have wget; then
  dl() { wget -qO "$2" "$1"; }
  fetch() { wget -qO- "$1"; }
else
  err "need curl or wget to download the release"
fi

# --- Resolve the version tag ------------------------------------------------------------
if [ "$VERSION" = "latest" ]; then
  # Read the latest release's tag_name from the public API (no auth needed — public repo).
  VERSION="$(fetch "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  [ -n "$VERSION" ] || err "could not resolve the latest release tag (no releases yet, or network/API error)"
fi

asset="quorum-${target}.tar.gz"
base_url="https://github.com/$REPO/releases/download/$VERSION"

printf 'quorum installer\n  os/arch : %s/%s -> %s\n  version : %s\n  asset   : %s\n  dest    : %s\n' \
  "$os" "$arch" "$target" "$VERSION" "$asset" "$INSTALL_DIR/quorum"

if [ "${QUORUM_DRY_RUN:-0}" = "1" ]; then
  printf '  url     : %s/%s\n(dry run — nothing downloaded)\n' "$base_url" "$asset"
  exit 0
fi

# --- Download + verify in a temp dir ----------------------------------------------------
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

dl "$base_url/$asset" "$tmp/$asset" || err "download failed: $base_url/$asset"
dl "$base_url/$asset.sha256" "$tmp/$asset.sha256" || err "checksum download failed: $base_url/$asset.sha256"

# Verify SHA-256 (sha256sum on Linux, shasum on macOS). The .sha256 file holds
# "<hash>  <filename>"; compare against a locally computed hash of the downloaded asset.
expected="$(awk '{print $1}' "$tmp/$asset.sha256")"
if have sha256sum; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
elif have shasum; then
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
else
  err "need sha256sum or shasum to verify the download"
fi
[ -n "$expected" ] || err "checksum file was empty"
[ "$expected" = "$actual" ] || err "checksum mismatch (expected $expected, got $actual) — refusing to install"

# --- Extract + install ------------------------------------------------------------------
tar -C "$tmp" -xzf "$tmp/$asset" quorum || err "failed to extract quorum from $asset"
mkdir -p "$INSTALL_DIR"
install -m 0755 "$tmp/quorum" "$INSTALL_DIR/quorum" 2>/dev/null \
  || { cp "$tmp/quorum" "$INSTALL_DIR/quorum" && chmod 0755 "$INSTALL_DIR/quorum"; }

printf 'installed quorum %s -> %s\n' "$VERSION" "$INSTALL_DIR/quorum"
# shellcheck disable=SC2016 # the literal $PATH below is intentional — a copy-paste hint for the user
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) printf 'note: %s is not on your PATH — add it, e.g.\n  export PATH="%s:$PATH"\n' "$INSTALL_DIR" "$INSTALL_DIR" ;;
esac
printf 'next: quorum init && quorum help\n'
