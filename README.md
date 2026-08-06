# Quorum

Quorum is a local tool I use to run coding agents through implementation, review, rework,
and merge:

```text
task → worker → review → merge
             ↖ rework ↙
```

It is built for agents first. The human interface is mostly `quorum status`, a small local
dashboard, and the occasional intervention when an agent or provider gets stuck.

This is not a polished or stable product. It follows my own workflow, assumes Git, GitHub,
local credentials, and capable coding agents, and has sharp edges. Commands, prompts,
configuration, database schemas, and existing behavior may change without notice. If it is
useful to you, great—but there are no compatibility or support guarantees.

## What it does

One `quorum serve` process manages a repository-local task queue. It chooses work,
provisions isolated worktrees, starts Claude or Codex agents, sends completed changes
through independent review, and merges approved PRs. SQLite is the local source of truth.

Agents are expected to do most of the work. Quorum provides atomic assignment and a
bounded lifecycle, but it cannot make an agent correct, recover unavailable credentials,
or turn an unclear task into a good implementation.

## Install

Prebuilt binary on macOS or Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/ag2trust/quorum/main/install.sh | sh
quorum init
```

Or build it:

```sh
cargo build --release
cp target/release/quorum ~/.local/bin/
quorum init
```

For the initial self-hostable container image, including its persistence and provider
packaging contract, see [`docker/README.md`](docker/README.md).

State lives under `~/.quorum/repos/<owner>__<name>/`.

## Start it

For a normal repository:

```sh
quorum serve \
  --repo owner/name \
  --repo-dir /path/to/repo \
  --worktree-base /path/to/worktrees
```

Use `quorum serve --help` for provider, model, concurrency, and budget settings. This repo
uses `scripts/serve-supervisor.sh` so Quorum can rebuild and restart after updating itself.
Only one daemon can manage a repository database at a time.

## Give it work

There are three task entry modes.

Start a new implementation from the configured base branch:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Add retry telemetry" \
  --body-stdin <<'EOF'
Record retry counts in the status JSON and cover the failure path.
EOF
```

Continue work on the exact current head of an existing PR:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Finish PR #412" \
  --continue-pr 412 \
  --body-stdin <<'EOF'
Finish the implementation and verify the complete result.
EOF
```

Review and merge a PR whose implementation is already complete:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Review PR #412" \
  --review-pr 412 \
  --body-stdin <<'EOF'
Review the existing implementation and merge it if it is sound.
EOF
```

`--continue-pr` creates a managed worker from the recorded PR head. `--review-pr` skips the
worker and starts with review. They are mutually exclusive. If a review-only PR needs code
changes, its outside author must update it; Quorum has no managed worker for that task.

Give implementation tasks a concrete outcome, relevant constraints, and a way to verify
the result. The daemon chooses complexity, model, and effort. Managed agents receive their
assignment directly; they do not poll or claim tasks.

## See what is happening

```sh
quorum status                 # terminal overview
quorum web                    # loopback-only dashboard
quorum task-list --brief      # queue summary
quorum task-get --task-id 42  # full task and notes
quorum log --refs task#42     # lifecycle events
quorum tail Agent-42          # one managed session
```

Use `quorum <command> --help` for exact flags and `quorum help` for the current workflow.
Most commands emit JSON. Exit codes are `0` for success, `1` for an expected negative
result, `2` for bad input, and `3` for an internal or database failure.

## Working on Quorum

Contributor and agent instructions live in [`AGENTS.md`](AGENTS.md). The design record is
[`docs/2026-06-23-quorum-design.md`](docs/2026-06-23-quorum-design.md). Run the full gate
before submitting any change:

```sh
./preflight.sh
```

## License

MIT — see [`LICENSE`](LICENSE).
