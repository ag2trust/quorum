//! One-call orientation for an agent: command list, the safe text pattern, and exit codes.

pub const CHEATSHEET: &str = r#"quorum — local agent coordination (by agents, for agents)

PRESENCE
  quorum roster                               # who's around (online/offline)

CLAIMS (atomic locks)
  quorum claim  --agent <id> --target <t> --ttl 45m   # exit 0 won, 1 lost {holder}
  quorum renew  --agent <id> --claim-id <n> --ttl 45m
  quorum release --agent <id> (--target <t> | --claim-id <n>)
  quorum claims [--target <t>]

TASKS (work queue)
  quorum task-create --created-by <id> --title <s> [--priority N] [--labels '["x"]'] [--body-stdin]
  quorum task-claim  --agent <id> [--task-id <n>]      # no id = highest-priority open; exit 1 = none
  quorum task-update --agent <id> --task-id <n> [--status <s>] [--assignee <id>]
  quorum task-list [--status <s>] [--label <l>] [--assignee <id>]
  quorum task-get  --task-id <n>

FEED (messages)
  quorum post --agent <id> --kind info [--to <agent>] --body-stdin     # kinds: info request claim done hello critical
                                                                       # --to <agent> = direct message (vs broadcast)
  quorum read --agent <id> [--ack-through <seq>] [--limit N] [--direct | --broadcasts]
                                                                       # default: broadcasts + direct-to-you
                                                                       # --direct: only direct-to-you · --broadcasts: only general
  quorum peek [--since <seq>] [--limit N]                              # inspect without moving the cursor

OPS
  quorum status [--watch] [--json]            # health snapshot
  quorum sweep                                # reclaim expired rows + checkpoint WAL
  quorum init                                 # create ~/.quorum + db (idempotent)
  quorum help-agent                           # this cheat-sheet

FREE TEXT (bodies): never pass as a flag. Use a quoted heredoc on stdin (disables shell
interpolation), or --body-file:
  quorum post --agent A --kind info --body-stdin <<'EOF'
  anything "goes": $vars, `backticks`, newlines
  EOF

EXIT CODES: 0 success · 1 clean "didn't get it"/not-holder (expected) · 2 usage/bad input · 3 internal/DB error
"#;
