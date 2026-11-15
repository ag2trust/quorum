#!/bin/sh
# ci-branch-base.sh — the CI `branch-base` check for one pull-request event.
#
# Reproduces the ref update the pre-push hook would have judged for the push
# that produced this PR head, then runs preflight's branch-base gate on it:
#   PR_BASE        PR base branch (github.base_ref)
#   PR_HEAD_SHA    PR head commit (github.event.pull_request.head.sha)
#   PR_BEFORE_SHA  previous PR head for a `synchronize` event, else empty
# An empty PR_BEFORE_SHA is a new branch (whole branch-owned range). Run from
# the checkout under test; the checked-out commit (e.g. refs/pull/N/merge) is
# irrelevant because the gate judges PR_HEAD_SHA.

set -eu

[ -n "${PR_BASE:-}" ] || { printf 'ci-branch-base.sh: PR_BASE is required\n' >&2; exit 2; }
[ -n "${PR_HEAD_SHA:-}" ] \
  || { printf 'ci-branch-base.sh: PR_HEAD_SHA is required\n' >&2; exit 2; }

PREFLIGHT="$(cd "$(dirname "$0")/.." && pwd -P)/preflight.sh"

set -- --quick --pr-base="$PR_BASE" --proposed-sha="$PR_HEAD_SHA"
if [ -n "${PR_BEFORE_SHA:-}" ]; then
  set -- "$@" --continuation-from="$PR_BEFORE_SHA"
fi
exec "$PREFLIGHT" "$@"
