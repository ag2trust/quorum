#!/usr/bin/env bash
set -euo pipefail

CARGO_TOML="Cargo.toml"
BUMP="${1:-patch}"
BASE_BRANCH="main"

if [[ ! -f "$CARGO_TOML" ]]; then
  echo "error: $CARGO_TOML not found — run from repo root" >&2
  exit 1
fi

current=$(grep -m1 'version = ' "$CARGO_TOML" | sed 's/.*"\(.*\)"/\1/')
IFS='.' read -r major minor patch <<< "$current"

case "$BUMP" in
  major) major=$((major + 1)); minor=0; patch=0 ;;
  minor) minor=$((minor + 1)); patch=0 ;;
  patch) patch=$((patch + 1)) ;;
  tag)
    # Post-merge: tag the current HEAD and push the tag.
    tag="v${current}"
    if git tag -l "$tag" | grep -q .; then
      echo "error: tag $tag already exists" >&2
      exit 1
    fi
    git tag "$tag"
    git push origin "$tag"
    echo "tagged + pushed $tag — release CI triggered"
    exit 0
    ;;
  *)
    echo "usage: bump-version.sh [major|minor|patch|tag]  (default: patch)" >&2
    echo ""
    echo "  patch  0.2.0 -> 0.2.1  (default)"
    echo "  minor  0.2.0 -> 0.3.0"
    echo "  major  0.2.0 -> 1.0.0"
    echo "  tag    tag current version + push (run after PR merges)"
    exit 2
    ;;
esac

next="${major}.${minor}.${patch}"
tag="v${next}"
branch="release/${tag}"

echo "$current -> $next"

if git tag -l "$tag" | grep -q .; then
  echo "error: tag $tag already exists" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is dirty — commit or stash first" >&2
  exit 1
fi

git checkout "$BASE_BRANCH"
git pull --ff-only
git checkout -b "$branch"

sed -i '' "s/version = \"$current\"/version = \"$next\"/" "$CARGO_TOML"

cargo check --quiet 2>/dev/null

git add "$CARGO_TOML"
git commit -m "chore: bump version to $next"
git push -u origin "$branch"

pr_url=$(gh pr create \
  --title "chore: bump version to $next" \
  --body "Bump workspace version \`$current\` → \`$next\`." \
  --base "$BASE_BRANCH")

echo ""
echo "PR created: $pr_url"
echo "after merge, run:  ./bump-version.sh tag"
