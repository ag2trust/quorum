#!/bin/sh
# Real-container verification fixture: record the first Codex command shape.
printf '%s\n' "$*" >>/data/codex-invocations
exit 2
