#!/bin/sh
# Real linux/amd64 clean container replacement over one host /data tree.
#
# Container one seeds state, is cleanly stopped and removed; container two
# starts from the same image, QUORUM_REPO, and host tree. Every recorded
# repository, DB, config, log byte, and worktree-root artifact must survive;
# logs may append replacement startup records. The operator-edited serve
# config must not be regenerated, Web in container two must read the
# replacement daemon's database, and exactly one live daemon authority must
# exist. This proves on-disk state survival only; it does not prove that an
# in-flight provider turn resumes.
#
# REPLACE_TAMPER=<repo|config|db|log|worktree> deliberately alters one
# recorded artifact between containers; the suite must then fail.
set -eu

IMAGE=${1:-quorum:local}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=docker/replace-lib.sh
. "$SCRIPT_DIR/replace-lib.sh"
replace_init "$IMAGE" replace

replace_assert_platform
replace_prepare_tree
replace_seed_offline

replace_name first
FIRST=$REPLACE_NAME
replace_name second
SECOND=$REPLACE_NAME

replace_run_default "$FIRST"
replace_wait_ready "$FIRST" || replace_fail 'container one never became ready'
replace_assert_eq 10001:10001 \
  "$(docker exec "$FIRST" sh -c 'printf "%s:%s" "$(id -u)" "$(id -g)"')" \
  'host-tree runtime identity'
replace_seed_live "$FIRST"
before_view=$(replace_cli_view "$FIRST")
BEFORE=$REPLACE_WORK/before
replace_status_snapshot "$FIRST" "$BEFORE"
first_image=$(docker inspect -f '{{.Image}}' "$FIRST")
first_shape=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}{{range .Mounts}}{{.Source}}:{{.Destination}}{{end}}' "$FIRST" \
  | grep -E '^QUORUM_REPO=|:/data$')
replace_stop_remove "$FIRST"
if docker inspect "$FIRST" >/dev/null 2>&1; then
  replace_fail 'container one still exists after removal'
fi

# A deliberate operator edit to the effective serve config before replacement.
EDIT="# operator edit before replacement $$"
replace_root_sh "printf '%s\n' '$EDIT' >>/data/quorum/serve/\$REPLACE_SLUG.toml" \
  || replace_fail 'operator config edit failed'

replace_snapshot "$BEFORE"
replace_assert_recorded "$BEFORE"
grep -qxF "$EDIT" "$BEFORE/config.bytes" \
  || replace_fail 'operator edit missing from recorded config'
replace_assert_eq "config path $REPLACE_CONFIG" \
  "$(grep '^config path ' "$BEFORE/manifest")" 'effective Codex serve-config path'
grep -q '^runner = "codex"$' "$BEFORE/config.bytes" \
  || replace_fail 'recorded serve config is not Codex routing'
replace_comparator_selftest "$BEFORE"
printf 'replace: recorded %s artifacts; comparator self-check passed\n' \
  "$(wc -l <"$BEFORE/manifest" | tr -d ' ')"

[ -z "${REPLACE_TAMPER:-}" ] || replace_tamper "$REPLACE_TAMPER"

replace_run_default "$SECOND"
replace_wait_ready "$SECOND" || replace_fail 'container two never became ready'
replace_assert_eq "$first_image" "$(docker inspect -f '{{.Image}}' "$SECOND")" \
  'replacement image'
replace_assert_eq "$first_shape" \
  "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}{{range .Mounts}}{{.Source}}:{{.Destination}}{{end}}' "$SECOND" \
    | grep -E '^QUORUM_REPO=|:/data$')" 'replacement QUORUM_REPO and host mount'

replace_assert_single_authority "$SECOND"
replace_assert_effective_config "$SECOND"
printf 'replace: exactly one live daemon authority after replacement\n'

[ "$before_view" = "$(replace_cli_view "$SECOND")" ] \
  || replace_fail 'CLI view of seeded rows changed across replacement'
AFTER=$REPLACE_WORK/after
replace_status_snapshot "$SECOND" "$AFTER"
replace_assert_status_unchanged "$BEFORE" "$AFTER"

# Web in container two shows the persisted row and note exactly as the
# replacement daemon's database records them.
task_title=$(sqlite3 "$BEFORE/db/quorum.db" 'SELECT title FROM tasks WHERE id=1')
task_status=$(sqlite3 "$BEFORE/db/quorum.db" 'SELECT status FROM tasks WHERE id=1')
task_note=$(sqlite3 "$BEFORE/db/quorum.db" \
  "SELECT body FROM task_notes WHERE task_id=1 AND agent='$REPLACE_SEED_AGENT'")
replace_assert_eq "$REPLACE_TASK_TITLE" "$task_title" 'seeded task title'
replace_assert_eq cancelled "$task_status" 'seeded task status'
replace_assert_eq 'persisted note' "$task_note" 'seeded task note'
web=$(replace_http_get "$SECOND" /api/tasks/1)
for fragment in 'HTTP/1.1 200' "\"title\":\"$task_title\"" \
  "\"status\":\"$task_status\"" "$task_note"; do
  printf '%s\n' "$web" | grep -qF "$fragment" \
    || replace_fail "Web task view lacks $fragment"
done
printf 'replace: Web in container two reads the replacement database\n'

replace_stop_remove "$SECOND"
replace_snapshot "$AFTER" "$BEFORE"
replace_assert_unchanged "$BEFORE" "$AFTER"
printf 'replace: repository, DB rows, config (sha256 %s), recorded log bytes, and worktree root unchanged\n' \
  "$(sed -n 's/^config sha256 //p' "$AFTER/manifest")"
printf 'replace: clean container replacement passed\n'
