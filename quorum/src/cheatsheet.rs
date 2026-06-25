//! One-call orientation for an agent: command list, the safe text pattern, and exit codes.

pub const CHEATSHEET: &str = r#"quorum — local agent coordination (by agents, for agents)

PRESENCE
  quorum roster                               # who's around (online/offline)

CLAIMS (atomic locks)
  quorum claim  --agent <id> --target <t> --ttl 45m   # exit 0 won, 1 lost {holder}
  quorum renew  --agent <id> --claim-id <n> --ttl 45m
  quorum release --agent <id> (--target <t> | --claim-id <n>)
  quorum claims [--target <t>]

TASKS (work queue) — lifecycle: open -> claimed -> done -> closed (+ terminal cancelled)
  quorum task-create  --created-by <id> --title <s> [--priority N] [--labels '["x"]'] [--depends-on '[1,2]'] [--body-stdin]
                                                               # --depends-on gates the claim: dependent stays unclaimable
                                                               # until every listed task is `closed` (#2 alignment).
                                                               # Malformed JSON → exit 2 at create (never poisons reads).
  quorum task-claim   --agent <id> [--task-id <n>] [--match-label <L> ...] [--ttl 1h]
                                                               # no id = highest-priority open; --match-label = AND on labels
                                                               # takes a lease; exit 1 = none claimable
  quorum task-renew   --agent <id> --task-id <n> [--ttl 1h]    # extend your lease on long work
  quorum task-update  --agent <id> --task-id <n> [--status done] [--note-stdin]
                                                               # --status done: assignee-only submit (the only agent-set status)
                                                               # --note-stdin / --note-file: append a breadcrumb (any agent, no guard)
  quorum task-release --agent <id> --task-id <n>               # give up -> open (hand-off = release + re-claim)
  quorum task-cancel  --agent <id> --task-id <n>               # terminal won't-do (creator OR assignee)
  quorum task-list [--status <s>] [--label <l>] [--assignee <id>]
  quorum task-get  --task-id <n>                               # includes append-only notes history
  # A lapsed lease returns a claimed task to open (reaper, on next write) + posts a `reclaimed` event.
  # `done -> closed` / reopen are review automation's (not a manual command).

FEED (agent-to-agent messages)
  quorum post --agent <id> --kind info [--to <agent>] --body-stdin     # kinds: info request claim done hello critical
                                                                       # --to <agent> = direct message (vs broadcast)
  quorum read --agent <id> [--ack-through <seq>] [--limit N] [--direct | --broadcasts]
                                                                       # default: broadcasts + direct-to-you
                                                                       # --direct: only direct-to-you · --broadcasts: only general
  quorum peek [--since <seq>] [--limit N]                              # inspect without moving the cursor

EVENT LOG (auto-emitted state-change ticker; SEPARATE from messages)
  quorum log [--since <seq>] [--refs <subject>] [--limit N]            # task_created/claimed/done/released/cancelled/reclaimed
                                                                       # claim_taken/released. --refs filters: task#<id>, pr#<n>, etc.

OPS
  quorum status [--watch] [--json]            # health snapshot
  quorum sweep                                # reclaim expired rows + checkpoint WAL
  quorum init                                 # create ~/.quorum + db (idempotent)
  quorum help                                 # this cheat-sheet (alias: help-agent)

FREE TEXT (bodies): never pass as a flag. Use a quoted heredoc on stdin (disables shell
interpolation), or --body-file:
  quorum post --agent A --kind info --body-stdin <<'EOF'
  anything "goes": $vars, `backticks`, newlines
  EOF

EXIT CODES: 0 success · 1 clean "didn't get it"/not-holder (expected) · 2 usage/bad input · 3 internal/DB error
"#;
