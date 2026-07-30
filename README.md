# Quorum

**A local manager for teams of AI coding agents.**

Quorum turns a repo-local task queue into a managed pipeline:

```text
task → implementation agent → review agent → merge
                         ↖ rework ↙
```

One `quorum serve` daemon chooses work, provisions isolated worktrees, keeps workers and
reviewers attached across rework, and merges approved PRs. One SQLite file per repo is the
source of truth. The CLI creates work, reports state, and lets managed agents signal the
daemon.

There is no web UI, network coordination service, or auth layer. Quorum is local and
agent-first; `quorum status` is the small human-readable window into it.

## Why Quorum

- **Atomic:** SQLite transactions prevent double assignment under concurrent processes.
- **Fail-safe:** stable exit codes and JSON errors make failures loud and branchable.
- **Managed:** the daemon owns claiming, leases, review, rework, and merge.
- **Recoverable:** expired leases and supervised restarts keep work from stranding.
- **Cheap:** agents receive focused task context instead of repeatedly scanning a shared log.

## Install

Prebuilt binary (macOS and Linux, no Rust toolchain):

```sh
curl -fsSL https://raw.githubusercontent.com/ag2trust/quorum/main/install.sh | sh
quorum init
```

Or build from source:

```sh
cargo build --release
cp target/release/quorum ~/.local/bin/
quorum init
```

The binary statically links SQLite. State lives at
`~/.quorum/repos/<owner>__<name>/quorum.db`.

## Run the manager

For this repository, the supervised launcher is recommended because it rebuilds and
restarts after Quorum merges an update to itself:

```sh
scripts/serve-supervisor.sh \
  --repo owner/name \
  --repo-dir /path/to/repo \
  --worktree-base /path/to/worktrees \
  --cap 4 \
  --self-update-drain
```

For a basic launch, use `quorum serve --help` to see configuration flags. Only one daemon
may hold a repo database; a second live daemon fails loudly.

## Create work

If implementation is needed, create a normal task. The daemon will select it and spawn a
managed worker:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Add retry telemetry" \
  --labels '["type:observability","area:status"]' \
  --body-stdin <<'EOF'
Record retry counts in the status JSON and cover the failure path.
EOF
```

If a PR already exists and only needs review and merge, use `--review-pr`:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Review and merge PR #412" \
  --review-pr 412 \
  --labels '["type:review"]' \
  --body-stdin <<'EOF'
Review the existing PR and drive it through merge.
EOF
```

A review-only task has no implementation worker. If review requests changes, the outside
PR author must update the branch and create a new review request as needed; the task may
fail because Quorum cannot assign rework. Merge conflicts likewise require the outside
author to update the PR before review/merge can continue.

Task creators describe scope and acceptance criteria but do not select complexity, model,
or effort. The daemon classifies each task before dispatch and applies its configured
provider's routing table. Labels beginning with `complexity:`, `tier:`, or `effort:` are
rejected. Complexity-5 tasks are classified and then durably parked without an agent run;
split or rescope them into smaller replacement tasks. Retrying the unchanged parked task
is a clean negative.

## Watch progress

```sh
quorum status                 # compact human view
quorum status --json          # machine-readable health
quorum task-list --brief      # token-cheap queue summary
quorum task-get --task-id 42  # full task and notes
quorum log --refs task#42     # lifecycle history
quorum tail --agent Agent-42  # managed session output
```

Managed agents do not poll or claim work. Their prompt contains the assignment, branch,
and worktree. Workers hand off a PR with `quorum submit`; reviewers submit an attested
verdict. The daemon performs the state transitions and merge.

## Messages and safe text

The feed contains agent-authored messages; the event log contains state changes emitted by
Quorum. Use `read` for “what did agents say?” and `log` for “what changed?”

Free text always travels through stdin or a file, not a shell flag:

```sh
quorum post --agent coordinator --kind info --body-stdin <<'EOF'
anything "goes": $vars, `backticks`, and multiple lines
EOF
```

Input must be valid UTF-8 without NUL bytes. Quorum binds text as SQLite parameters and
emits JSON.

## Command discovery and exit codes

Run `quorum help` for the workflow cheat-sheet and `quorum <command> --help` for exact
flags. `help-agent` remains a compatibility alias.

| Code | Meaning |
|---:|---|
| `0` | Success |
| `1` | Clean negative result (nothing available, not holder) |
| `2` | Usage or invalid input |
| `3` | Internal, database, or migration error |

## How it works

Every CLI invocation opens the repo database, migrates if needed, performs one atomic
operation, prints JSON, and exits. The long-lived daemon drives the task state machine:

```text
open → working → in-review → merging → done
           ↑          ↕
           └────── rework
```

SQLite WAL mode, `BEGIN IMMEDIATE`, guarded updates, and partial unique indexes provide
cross-process atomicity. Expiring rows are filtered by time before physical cleanup, and a
single-daemon lease prevents competing managers.

The full invariants and transition table live in
[`docs/2026-06-23-quorum-design.md`](docs/2026-06-23-quorum-design.md).

## Development

```sh
./preflight.sh
```

That gate checks the branch base, formatting, clippy, and the full test suite, including
the multi-process claim-race canary. Contributor rules are in [`AGENTS.md`](AGENTS.md).

## License

MIT — see [`LICENSE`](LICENSE).
