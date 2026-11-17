#!/bin/sh
# Contract tests for docker/pin-check.sh: the repository Dockerfile passes,
# and each mutable-input fixture exits 1 with its specific diagnostic.
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
check="$here/pin-check.sh"
base="$here/../Dockerfile"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0

# expect STATUS NEEDLE LABEL -- ARGS...: run pin-check, assert exit STATUS and
# (when NEEDLE is nonempty) that stderr names the violation.
expect() {
  want="$1" needle="$2" label="$3"
  shift 4
  set +e
  "$check" "$@" >"$tmp/out" 2>"$tmp/err"
  got=$?
  set -e
  if [ "$got" -ne "$want" ]; then
    printf 'FAIL %s: expected exit %s, got %s\n' "$label" "$want" "$got" >&2
    sed 's/^/  /' "$tmp/err" >&2
    failures=$((failures + 1))
  elif [ -n "$needle" ] && ! grep -F -- "$needle" "$tmp/err" >/dev/null; then
    printf 'FAIL %s: stderr lacks %s\n' "$label" "$needle" >&2
    sed 's/^/  /' "$tmp/err" >&2
    failures=$((failures + 1))
  else
    printf 'ok   %s\n' "$label"
  fi
}

# mutate NAME SED_EXPR: derive a fixture from the real Dockerfile and require
# the edit to change it, so a stale expression cannot pass vacuously.
mutate() {
  sed "$2" "$base" >"$tmp/$1"
  if cmp -s "$base" "$tmp/$1"; then
    printf 'FAIL fixture %s: sed expression did not change the Dockerfile\n' "$1" >&2
    exit 1
  fi
}

expect 0 '' 'current Dockerfile is pinned' -- --dockerfile "$base"

mutate from-tag 's|^FROM debian:bookworm-slim@sha256:[0-9a-f]*|FROM debian:bookworm-slim|'
expect 1 'FROM is not pinned by @sha256 digest: debian:bookworm-slim' \
  'FROM without digest' -- --dockerfile "$tmp/from-tag"

mutate from-short 's|^\(FROM rust:[^@]*@sha256:\)[0-9a-f]*|\1abc123|'
expect 1 'FROM is not pinned by @sha256 digest: rust:' \
  'FROM with truncated digest' -- --dockerfile "$tmp/from-short"

mutate from-var 's|^FROM debian:bookworm-slim@sha256:[0-9a-f]*$|ARG BASE=debian\nFROM ${BASE}|'
expect 1 'FROM uses a variable image reference' \
  'FROM via build arg' -- --dockerfile "$tmp/from-var"

mutate syntax-tag 's|^# syntax=docker/dockerfile:1.7@sha256:[0-9a-f]*|# syntax=docker/dockerfile:1.7|'
expect 1 'syntax frontend is not pinned' \
  'syntax directive without digest' -- --dockerfile "$tmp/syntax-tag"

mutate sha-zero 's|^ARG CODEX_SHA256=.*|ARG CODEX_SHA256=0000000000000000000000000000000000000000000000000000000000000000|'
expect 1 'CODEX_SHA256 is an all-zero placeholder' \
  'all-zero CODEX_SHA256' -- --dockerfile "$tmp/sha-zero"

mutate sha-missing '/^ARG CODEX_SHA256=/d'
expect 1 'CODEX_SHA256 must default to a 64-hex digest' \
  'missing CODEX_SHA256' -- --dockerfile "$tmp/sha-missing"

mutate sha-placeholder 's|^ARG CODEX_SHA256=.*|ARG CODEX_SHA256=REPLACE_ME|'
expect 1 'CODEX_SHA256 must default to a 64-hex digest' \
  'placeholder CODEX_SHA256' -- --dockerfile "$tmp/sha-placeholder"

mutate codex-latest 's|^ARG CODEX_VERSION=.*|ARG CODEX_VERSION=latest|'
expect 1 'CODEX_VERSION must default to an exact x.y.z release' \
  'mutable CODEX_VERSION' -- --dockerfile "$tmp/codex-latest"

mutate apt-unpinned 's|tini="${TINI_VERSION}"|tini|'
expect 1 'apt package tini is not version-pinned' \
  'unpinned apt package' -- --dockerfile "$tmp/apt-unpinned"

mutate apt-noarg 's|^ARG GH_VERSION=.*|ARG GH_VERSION|'
expect 1 'apt package gh pin ${GH_VERSION} has no nonempty ARG default' \
  'apt pin without ARG default' -- --dockerfile "$tmp/apt-noarg"

mutate apt-ca 's|ca-certificates=20230311+deb12u1 curl|ca-certificates curl|'
expect 1 'apt package ca-certificates is not version-pinned' \
  'unpinned ca-certificates' -- --dockerfile "$tmp/apt-ca"

sha=0123456789abcdef0123456789abcdef01234567
cat >"$tmp/pinned.yml" <<EOF
jobs:
  build:
    steps:
      - uses: actions/checkout@$sha # v4.2.2
      - name: nested
        uses: "owner/repo/path@$sha"
      - uses: ./.github/actions/local
      - uses: docker://alpine@sha256:$(printf '%064d' 0)
EOF
expect 0 '' 'SHA-pinned workflow passes' -- --dockerfile "$base" "$tmp/pinned.yml"

printf 'jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v4\n' >"$tmp/tag.yml"
expect 1 'tag.yml:4: action is not pinned by a full 40-hex commit SHA: actions/checkout@v4' \
  'tag-pinned action' -- --dockerfile "$base" "$tmp/tag.yml"

printf 'jobs:\n  a:\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n' >"$tmp/branch.yml"
expect 1 'action is not pinned by a full 40-hex commit SHA: dtolnay/rust-toolchain@stable' \
  'branch-pinned action' -- --dockerfile "$base" "$tmp/branch.yml"

printf 'jobs:\n  a:\n    steps:\n      - uses: actions/checkout@0123456\n' >"$tmp/short.yml"
expect 1 'action is not pinned by a full 40-hex commit SHA: actions/checkout@0123456' \
  'short-SHA action' -- --dockerfile "$base" "$tmp/short.yml"

printf 'jobs:\n  a:\n    uses: owner/repo/.github/workflows/x.yml@main\n' >"$tmp/reusable.yml"
expect 1 'owner/repo/.github/workflows/x.yml@main' \
  'tag-pinned reusable workflow' -- --dockerfile "$base" "$tmp/reusable.yml"

printf 'jobs:\n  a:\n    steps:\n      - uses: docker://alpine:3.20\n' >"$tmp/docker.yml"
expect 1 'docker action is not pinned by @sha256 digest' \
  'tag-pinned docker action' -- --dockerfile "$base" "$tmp/docker.yml"

expect 1 'tag.yml:4' 'one bad workflow among good ones fails' \
  -- --dockerfile "$base" "$tmp/pinned.yml" "$tmp/tag.yml"

expect 2 'cannot read' 'missing Dockerfile is a usage error' -- --dockerfile "$tmp/absent"
expect 2 'cannot read' 'missing workflow is a usage error' -- --dockerfile "$base" "$tmp/absent.yml"
expect 2 'usage:' 'unknown flag is a usage error' -- --bogus

if [ "$failures" -ne 0 ]; then
  printf 'pin-check_test: %s failure(s)\n' "$failures" >&2
  exit 1
fi
printf 'pin-check_test: all cases passed\n'
