#!/bin/sh
# Session-recording fake external CLI for daemon session tests. It is not a
# real product. It speaks the stream protocol of the spec example declaration
# (`external_runner_spec.rs`, `tests::VALID` with `tests::with_session()`), so
# it expects `--session <file>` from `args_session`.
#
# SESSION_CLI_OBSERVE  directory that receives, for run N (1-based, in launch
#                      order): `run.N.session` ("absent" or the session file's
#                      byte size when the run started), `run.N.prompt` (stdin,
#                      verbatim), `run.N.dir` (QUORUM_SESSION_DIR), and one line
#                      per run appended to `runs`.
# SESSION_CLI_SCENARIO success (default): emit a completed turn and append one
#                        line to the session file, as a CLI saving its session.
#                      refuse_resume: when the session file existed, exit 2
#                        before any protocol line (a CLI refusing a corrupt or
#                        locked session); otherwise behave as success.
#                      silent_fail: always exit 2 before any protocol line.
set -u

obs="${SESSION_CLI_OBSERVE:?SESSION_CLI_OBSERVE is required}"
session=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--session" ]; then
    session="$arg"
  fi
  prev="$arg"
done

n=1
if [ -f "$obs/runs" ]; then
  n=$(( $(wc -l < "$obs/runs") + 1 ))
fi
if [ -n "$session" ] && [ -e "$session" ]; then
  existed=$(wc -c < "$session" | tr -d ' ')
else
  existed=absent
fi
printf '%s' "$existed" > "$obs/run.$n.session"
printf '%s' "${QUORUM_SESSION_DIR:-}" > "$obs/run.$n.dir"
cat > "$obs/run.$n.prompt"
printf 'run %s session=%s\n' "$n" "$existed" >> "$obs/runs"

scenario="${SESSION_CLI_SCENARIO:-success}"
if [ "$scenario" = "silent_fail" ] || { [ "$scenario" = "refuse_resume" ] && [ "$existed" != absent ]; }; then
  echo 'usage: session refused' >&2
  exit 2
fi

if [ -n "$session" ]; then
  printf 'turn %s\n' "$n" >> "$session"
fi
echo '{"type":"run.started","format":2}'
echo '{"type":"item.completed","item":{"type":"message","text":"working","id":"m1"}}'
echo '{"type":"run.completed","status":"completed","final_text":"done","usage":{"input":3,"output":1}}'
exit 0
