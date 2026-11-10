#!/bin/sh
# Real linux/amd64 fail-closed replacement cases over one host /data tree.
#
# Accepted state is seeded through a real container, cleanly stopped, and
# recorded (artifact manifest plus a byte-level listing of the whole tree).
# Each negative case then breaks one precondition and starts a replacement
# that must exit with the expected non-zero status, leave no container
# serving the tree, and leave every byte, owner, and mode of the tree exactly
# as it was before that replacement started (so no daemon_lock row was taken
# and no fresh state was created over accepted data). The precondition is
# then restored and the tree must equal the accepted listing again:
#
#   (1) tree unwritable by 10001:10001: a read-only /data, a read-only
#       /data/quorum under a writable /data, and a root-owned tree;
#   (2) missing /data/repos/project/.git, corrupt repository HEAD, and a
#       corrupt quorum.db header;
#   (3) quorum.db with PRAGMA user_version above the supported value, through
#       the entrypoint and through `quorum serve` directly;
#   (4) a different QUORUM_REPO, which must serve from its own
#       /data/quorum/repos/<owner>__<repo>/quorum.db while the original
#       identity's DB and config stay byte-identical.
#
# Finally the original identity must start again as the sole authority with
# every recorded artifact, DB row, and the exact config checksum intact.
set -eu

IMAGE=${1:-quorum:local}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=docker/replace-lib.sh
. "$SCRIPT_DIR/replace-lib.sh"
replace_init "$IMAGE" replace-negative

OTHER_REPO=acme/other-widget
OTHER_SLUG=$(printf '%s' "$OTHER_REPO" | sed 's#/#__#')
ORIGINAL_DB_PATH=./quorum/repos/$REPLACE_SLUG/quorum.db
ASIDE=$REPLACE_WORK/aside
mkdir "$ASIDE"

# Start container $1 against the tree (mount mode $3: rw, ro for a read-only
# /data, or ro-state for a writable /data whose /data/quorum is read-only;
# remaining arguments replace the image CMD) and require that it exits with
# status $2 within a bounded wait and leaves no container serving the tree.
# The container's log must contain fixed string $NEGATIVE_REASON, so a case
# cannot pass by failing for an unrelated reason.
negative_expect() {
  name=$1
  expect=$2
  mount=$3
  shift 3
  case "$mount" in
    rw) set -- -v "$REPLACE_TREE:/data" "$REPLACE_IMAGE" "$@" ;;
    ro) set -- -v "$REPLACE_TREE:/data:ro" "$REPLACE_IMAGE" "$@" ;;
    ro-state) set -- -v "$REPLACE_TREE:/data" \
      -v "$REPLACE_TREE/quorum:/data/quorum:ro" "$REPLACE_IMAGE" "$@" ;;
    *) replace_fail "unknown mount mode: $mount" ;;
  esac
  docker run -d --platform "$REPLACE_PLATFORM" --name "$name" \
    -e QUORUM_REPO="$REPLACE_REPO" "$@" >/dev/null \
    || replace_fail "cannot start $name"
  replace_wait_stopped "$name" 90 \
    || replace_fail "$name is still running; it must fail closed"
  replace_assert_eq "$expect" "$(docker inspect -f '{{.State.ExitCode}}' "$name")" \
    "$name fail-closed exit status"
  holders=$(replace_tree_containers)
  [ -z "$holders" ] || replace_fail "containers still serve the tree: $holders"
  docker logs "$name" 2>&1 | grep -qF "$NEGATIVE_REASON" \
    || replace_fail "$name did not fail for the expected reason: $NEGATIVE_REASON"
  docker rm "$name" >/dev/null
}

# One negative case: the precondition has already been broken. Record the
# tree, run the replacement, require the tree to be unchanged by it, then run
# restore script $5 and require the accepted tree listing again.
negative_case() {
  label=$1
  expect=$2
  mount=$3
  NEGATIVE_REASON=$4
  restore=$5
  shift 5
  replace_tree_state "$REPLACE_WORK/$label.pre"
  replace_name "$label"
  negative_expect "$REPLACE_NAME" "$expect" "$mount" "$@"
  replace_tree_state "$REPLACE_WORK/$label.post"
  replace_assert_tree_equal "$REPLACE_WORK/$label.pre" "$REPLACE_WORK/$label.post" \
    "$label: failed replacement altered the host tree"
  replace_root_sh "$restore" "$ASIDE" >/dev/null \
    || replace_fail "$label: restoring the precondition failed"
  replace_tree_state "$REPLACE_WORK/$label.restored"
  replace_assert_tree_equal "$ACCEPTED_TREE" "$REPLACE_WORK/$label.restored" \
    "$label: accepted data differs after restore"
  printf 'replace-negative: %s failed closed with exit %s; tree unchanged\n' \
    "$label" "$expect"
}

# Lines of tree listing $1 for the original identity's DB family and config.
original_namespace() {
  grep -E " (\\./quorum/repos/$REPLACE_SLUG(/.*)?|\\./quorum/serve/$REPLACE_SLUG\\.toml)\$" "$1"
}

replace_assert_platform
replace_prepare_tree
replace_seed_offline

# Accepted state: seeded through a real container, then a clean stop.
replace_name accept
ACCEPT=$REPLACE_NAME
replace_run_default "$ACCEPT"
replace_wait_ready "$ACCEPT" || replace_fail 'accepting container never became ready'
replace_seed_live "$ACCEPT"
ACCEPTED_VIEW=$(replace_cli_view "$ACCEPT")
BASE=$REPLACE_WORK/base
replace_status_snapshot "$ACCEPT" "$BASE"
replace_stop_remove "$ACCEPT"
replace_snapshot "$BASE"
replace_assert_recorded "$BASE"
SUPPORTED=$(sqlite3 "$BASE/db/quorum.db" 'PRAGMA user_version')
replace_assert_eq 0 \
  "$(sqlite3 "$BASE/db/quorum.db" 'SELECT count(*) FROM daemon_lock')" \
  'accepted daemon_lock rows after clean stop'
[ ! -s "$BASE/db-files" ] || replace_fail 'accepted DB left WAL/SHM files after clean stop'
ACCEPTED_TREE=$REPLACE_WORK/accepted.tree
replace_tree_state "$ACCEPTED_TREE"
grep -q " $ORIGINAL_DB_PATH\$" "$ACCEPTED_TREE" \
  || replace_fail "accepted tree has no $ORIGINAL_DB_PATH"

# The tree comparator must notice a stray file.
replace_root_sh ': >/data/quorum/selftest-stray' >/dev/null
replace_tree_state "$REPLACE_WORK/selftest.tree"
if diff "$ACCEPTED_TREE" "$REPLACE_WORK/selftest.tree" >/dev/null; then
  replace_fail 'tree comparator accepted a stray file'
fi
replace_root_sh 'rm /data/quorum/selftest-stray' >/dev/null
replace_comparator_selftest "$BASE"
printf 'replace-negative: accepted state recorded (user_version %s); comparators self-checked\n' \
  "$SUPPORTED"

# (1) Tree unwritable by 10001:10001. Write denial is kernel-enforced with
# read-only bind mounts, because Docker Desktop host bind mounts do not
# enforce owner/mode bits. A root-owned tree is refused as foreign-owned.
# Ownership is saved per path and restored exactly, since accepted state may
# already hold paths not owned by 10001.
replace_root_sh '
  find /data -printf "%U:%G %p\n" >/out/owners
  chown -R 0:0 /data
' "$ASIDE" >/dev/null
negative_case root-owned-tree 1 rw 'is not a valid git checkout' \
  'while read -r owner path; do chown -h "$owner" "$path"; done </out/owners'

negative_case readonly-tree 3 ro 'unable to open database file' ':'
negative_case readonly-state 3 ro-state 'unable to open database file' ':'

# (2) Missing and corrupt required paths.
replace_root_sh 'mv /data/repos/project/.git /data/repos/negative-git-aside' >/dev/null
negative_case missing-git 1 rw 'is not a git checkout (.git is missing)' \
  'mv /data/repos/negative-git-aside /data/repos/project/.git'

replace_root_sh '
  cp /data/repos/project/.git/HEAD /out/HEAD
  printf "corrupt\n" >/data/repos/project/.git/HEAD
' "$ASIDE" >/dev/null
negative_case corrupt-repo 1 rw 'is not a valid git checkout' \
  'cat /out/HEAD >/data/repos/project/.git/HEAD'

cp "$BASE/db/quorum.db" "$ASIDE/accepted.db"
replace_root_sh '
  printf "corrupt-header!!" | dd of="/data/quorum/repos/$REPLACE_SLUG/quorum.db" \
    bs=16 count=1 conv=notrunc 2>/dev/null
' >/dev/null
negative_case corrupt-db 3 rw 'file is not a database' \
  'cat /out/accepted.db >"/data/quorum/repos/$REPLACE_SLUG/quorum.db"'

# (3) Newer database: user_version above the supported value. Both the
# entrypoint (through `quorum init`) and `quorum serve` itself must refuse it
# before taking daemon_lock or applying any persistent PRAGMA. A refusal at
# serve startup is exit 3; exit 75 is the running-tick self-update contract.
NEWER=$((SUPPORTED + 1))
cp "$BASE/db/quorum.db" "$ASIDE/newer.db"
sqlite3 "$ASIDE/newer.db" "PRAGMA user_version = $NEWER"
replace_assert_eq "$NEWER" "$(sqlite3 "$ASIDE/newer.db" 'PRAGMA user_version')" \
  'newer DB fixture user_version'
replace_assert_eq 0 "$(sqlite3 "$ASIDE/newer.db" 'SELECT count(*) FROM daemon_lock')" \
  'newer DB fixture daemon_lock rows'
NEWER_REASON="db schema version $NEWER is newer than this binary ($SUPPORTED)"
NEWER_SHA=$(sha256sum <"$ASIDE/newer.db" | cut -d' ' -f1)
NEWER_INSTALL='cat /out/newer.db >"/data/quorum/repos/$REPLACE_SLUG/quorum.db"'
RESTORE_DB='cat /out/accepted.db >"/data/quorum/repos/$REPLACE_SLUG/quorum.db"'

replace_root_sh "$NEWER_INSTALL" "$ASIDE" >/dev/null
negative_case newer-db-entrypoint 3 rw "$NEWER_REASON" "$RESTORE_DB"

replace_root_sh "$NEWER_INSTALL" "$ASIDE" >/dev/null
negative_case newer-db-serve 3 rw "$NEWER_REASON" "$RESTORE_DB" \
  quorum serve --config "$REPLACE_CONFIG" --repo "$REPLACE_REPO" \
  --repo-dir /data/repos/project --worktree-base /data/worktrees \
  --log-dir /data/quorum/logs
for label in newer-db-entrypoint newer-db-serve; do
  grep -qF " $NEWER_SHA $ORIGINAL_DB_PATH" "$REPLACE_WORK/$label.post" \
    || replace_fail "$label: refused newer DB is not byte-identical"
done
printf 'replace-negative: user_version %s refused (supported %s); newer DB sha256 %s unchanged\n' \
  "$NEWER" "$SUPPORTED" "$NEWER_SHA"

# (4) A different QUORUM_REPO on the same tree is a separate identity: it
# serves from its own namespace and never touches the original DB or config.
OTHER_DB=/data/quorum/repos/$OTHER_SLUG/quorum.db
OTHER_CONFIG=/data/quorum/serve/$OTHER_SLUG.toml
original_namespace "$ACCEPTED_TREE" >"$REPLACE_WORK/original.ns"
grep -q " $ORIGINAL_DB_PATH\$" "$REPLACE_WORK/original.ns" \
  || replace_fail 'original namespace listing is vacuous'
! grep -q "$OTHER_SLUG" "$ACCEPTED_TREE" \
  || replace_fail "accepted tree already contains $OTHER_SLUG state"
replace_name other
OTHER=$REPLACE_NAME
docker run -d --platform "$REPLACE_PLATFORM" --name "$OTHER" \
  -e QUORUM_REPO="$OTHER_REPO" -v "$REPLACE_TREE:/data" "$REPLACE_IMAGE" \
  >/dev/null || replace_fail "cannot start $OTHER"
# Its DB does not exist yet: wait from outside the container until `quorum
# init` has created it and serve runs, so no in-container status probe opens
# the brand-new DB concurrently with init.
replace_wait_serve_started "$OTHER" || replace_fail 'other identity never started serve'
replace_wait_ready "$OTHER" || replace_fail 'other identity never became ready'
replace_assert_single_authority "$OTHER"
for pid in $(replace_serve_pids "$OTHER"); do
  command=$(docker exec "$OTHER" sh -c "tr '\\000' ' ' </proc/$pid/cmdline")
  case " $command " in
    *" --config $OTHER_CONFIG --repo $OTHER_REPO "*) ;;
    *) replace_fail "other identity serve has unexpected arguments: $command" ;;
  esac
done
replace_tree_state "$REPLACE_WORK/other.live"
grep -q " \\.${OTHER_DB#/data}\$" "$REPLACE_WORK/other.live" \
  || replace_fail "other identity has no DB at $OTHER_DB"
original_namespace "$REPLACE_WORK/other.live" >"$REPLACE_WORK/other.live.ns"
replace_assert_tree_equal "$REPLACE_WORK/original.ns" "$REPLACE_WORK/other.live.ns" \
  'other identity touched the original DB or config while serving'
replace_stop_remove "$OTHER"
replace_tree_state "$REPLACE_WORK/other.post"
original_namespace "$REPLACE_WORK/other.post" >"$REPLACE_WORK/other.post.ns"
replace_assert_tree_equal "$REPLACE_WORK/original.ns" "$REPLACE_WORK/other.post.ns" \
  'other identity altered the original DB or config'
OTHER_COPY=$REPLACE_WORK/other-db
mkdir "$OTHER_COPY"
replace_root_sh "cp '$OTHER_DB' /out/ && chmod -R a+rwX /out" "$OTHER_COPY" \
  >/dev/null || replace_fail 'cannot copy the other identity DB'
replace_assert_eq 0 "$(sqlite3 "$OTHER_COPY/quorum.db" 'SELECT count(*) FROM tasks')" \
  'tasks visible in the other identity DB'
printf 'replace-negative: %s served from %s; original DB sha256 %s unchanged\n' \
  "$OTHER_REPO" "$OTHER_DB" \
  "$(sed -n "s|.* \\([0-9a-f]\\{64\\}\\) $ORIGINAL_DB_PATH\$|\\1|p" "$REPLACE_WORK/other.post")"

# The original identity still starts with every recorded artifact intact.
replace_name final
FINAL_C=$REPLACE_NAME
replace_run_default "$FINAL_C"
replace_wait_ready "$FINAL_C" || replace_fail 'original identity never restarted'
replace_assert_single_authority "$FINAL_C"
replace_assert_effective_config "$FINAL_C"
[ "$ACCEPTED_VIEW" = "$(replace_cli_view "$FINAL_C")" ] \
  || replace_fail 'CLI view of accepted rows changed after negative cases'
FINAL=$REPLACE_WORK/final
replace_status_snapshot "$FINAL_C" "$FINAL"
replace_stop_remove "$FINAL_C"
replace_snapshot "$FINAL" "$BASE"
replace_assert_unchanged "$BASE" "$FINAL"
printf 'replace-negative: original identity restarted; artifacts and config (sha256 %s) unchanged\n' \
  "$(sed -n 's/^config sha256 //p' "$FINAL/manifest")"
printf 'replace-negative: fail-closed replacement cases passed\n'
