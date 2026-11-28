#!/bin/sh
# Fake external CLI for the external-runner adapter tests
# (quorum/src/serve/external_agent.rs). It is not a real product.
#
# FAKE_CLI_OBSERVE  directory that receives argv (one element per line), env
#                   (`env` output), and stdin (the prompt, verbatim).
# FAKE_CLI_SCENARIO selects the JSONL protocol to emit; default "success".
set -u

obs="${FAKE_CLI_OBSERVE:-}"
if [ -n "$obs" ]; then
  : > "$obs/argv"
  for arg in "$@"; do
    printf '%s\n' "$arg" >> "$obs/argv"
  done
  env > "$obs/env"
  cat > "$obs/stdin"
else
  cat > /dev/null
fi

start='{"type":"run.started","format":2}'
completed='{"type":"run.completed","status":"completed","final_text":"final answer","usage":{"input":10,"cache_read":2,"cache_write":1,"output":5}}'

case "${FAKE_CLI_SCENARIO:-success}" in
  success)
    echo "$start"
    echo '{"type":"item.started","item":{"type":"tool_call","name":"Bash"}}'
    echo '{"type":"item.completed","item":{"type":"message","text":"hello","id":"m1"}}'
    echo 'stderr-secret-text' >&2
    echo "$completed"
    exit 0
    ;;
  overlong)
    echo "$start"
    # max_line_bytes is 256 in the test declaration; this line is 300 bytes.
    printf '%0300d\n' 0
    sleep 30
    ;;
  non_json)
    echo "$start"
    echo 'plain words, not json'
    echo '[1,2,3]'
    echo "$completed"
    exit 0
    ;;
  format_mismatch)
    echo '{"type":"run.started","format":1}'
    sleep 30
    ;;
  second_terminal)
    echo "$start"
    echo "$completed"
    echo "$completed"
    sleep 30
    ;;
  line_after_terminal)
    echo "$start"
    echo "$completed"
    echo '{"type":"item.started","item":{"type":"tool_call","name":"Bash"}}'
    sleep 30
    ;;
  # The *_then_exit scenarios write everything in one write and exit at
  # once, as a block-buffered CLI does: the exit is observable before the
  # trailing line is read.
  second_terminal_then_exit)
    printf '%s\n%s\n%s\n' "$start" "$completed" "$completed"
    exit 0
    ;;
  line_after_terminal_then_exit)
    printf '%s\n%s\n%s\n' "$start" "$completed" \
      '{"type":"item.started","item":{"type":"tool_call","name":"Bash"}}'
    exit 0
    ;;
  non_json_after_terminal_then_exit)
    printf '%s\n%s\n%s\n' "$start" "$completed" 'plain words, not json'
    exit 0
    ;;
  undeclared_status)
    echo "$start"
    echo '{"type":"run.completed","status":"mystery-status"}'
    exit 0
    ;;
  undeclared_error_kind)
    echo "$start"
    echo '{"type":"run.completed","status":"provider_error","error":{"kind":"mystery","message":"error-secret-text"}}'
    exit 4
    ;;
  declared_error_kind)
    echo "$start"
    echo '{"type":"run.completed","status":"provider_error","error":{"kind":"auth","message":"error-secret-text"}}'
    exit 4
    ;;
  contradicting_exit)
    echo "$start"
    echo "$completed"
    exit 3
    ;;
  signal_death)
    echo "$start"
    echo '{"type":"run.completed","status":"cancelled"}'
    kill -TERM $$
    sleep 30
    ;;
  no_exit_after_terminal)
    echo "$start"
    echo "$completed"
    sleep 30
    ;;
  stdout_held_after_exit)
    echo "$start"
    echo "$completed"
    # A descendant keeps stdout open after the CLI itself exits.
    sleep 30 &
    exit 0
    ;;
  eof_without_terminal)
    echo "$start"
    exit 0
    ;;
  exit_without_terminal)
    echo 'usage: stderr-secret-text' >&2
    exit 2
    ;;
  hang)
    echo "$start"
    sleep 30
    ;;
  *)
    exit 64
    ;;
esac
