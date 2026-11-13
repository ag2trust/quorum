# Quorum

[![boost my agents](https://boostmyagents.com/badge/brevitize.svg)](https://boostmyagents.com/d/brevitize)

> [!WARNING]
> **Experimental — not ready for production.** Quorum is under active development,
> has sharp edges, and may change its commands, configuration, schemas, and behavior
> without notice. Do not rely on it for production delivery workflows or unattended
> critical repositories.

Quorum is a local Git/GitHub agent pipeline. A repository-local `quorum serve`
daemon owns task lifecycle and uses SQLite as its source of truth; agents provide
implementation, review, and analysis turns, but their messages and signals do
not themselves transition or merge tasks.

The short version is not just `task → worker → review → merge`. A submitted
task can cause several provider calls:

```mermaid
flowchart LR
    C[Classifier] --> W[Worker]
    C --> P[Planner]
    P --> CC[Classifier]
    CC --> WS[Workers]
    WS --> R1[R1]
    W --> R1
    R1 --> R2[R2]
    R1 --> W
    R2 --> W
    R2 --> M[Daemon merge]
    M --> CO[Collector]
```

The direct path sends classified work to a worker. Qualifying larger work first
passes through the planner, whose proposed children are classified before they
reach dependency-ordered workers. Blocking findings from either review stage
return the existing Proposed Change to its worker for rework; the updated head
then returns to the responsible review stage.

Some stages run only when their lifecycle conditions or configured policy call
for them: planning/decomposition depends on task classification, rework depends
on review or verification findings, and R2 participation follows review policy.
"Conditional" does not mean every stage has an independent on/off switch in the
current configuration. Collection is part of the current post-merge path;
follow-up activation and doctor work have their own eligibility rules. This
README describes provider consumption, not a guarantee that every task visits
every branch.

Quorum follows my own workflow, assumes
Git, GitHub, local credentials, and capable coding agents, and has sharp edges.
If it is useful to you, great—but there are no compatibility or support
guarantees.

## What it does

One `quorum serve` process manages a repository-local queue. It assesses task
readiness and complexity, schedules dependencies, provisions isolated
worktrees, publishes GitHub PRs, runs independent review stages, and performs
the final merge only after the required gates pass. The daemon is the sole
lifecycle and merge authority: agents can report work or verdicts, but cannot
advance lifecycle by messaging or merge a PR themselves.

The managed capabilities include:

- Atomic lifecycle, claims, and role assignment backed by SQLite; recovery and
  provider retry are durable rather than dependent on a still-running process.
- Classifier-led routing and, for qualifying larger work, dependency-aware
  decomposition into smaller children.
- Isolated worktrees and PR publication for implementation tasks, plus exact-PR
  continuation for `--continue-pr` tasks.
- Sticky implementation/rework continuity: rework is a new turn on the existing
  Proposed Change, rather than a new, unrelated implementation assignment.
- Two independent review stages (R1 and R2), renewed review for a new rework
  head, daemon-owned merge, and post-merge review analytics.
- Local inspection through terminal status and a loopback-only, read-only web
  dashboard; supervised self-update for Quorum's own repository.

Quorum provides bounded coordination, not correctness. It cannot make an agent
correct, recover unavailable credentials, or turn an unclear task into a good
implementation. It is deliberately local Git/GitHub infrastructure, not a
hosted service or a general-purpose workflow system.

## Managed pipeline

1. **Intake and classification.** The daemon sends submitted work through a
   classifier assessment before worker dispatch. The classifier supplies
   complexity (1–5), execution size (S/M/L/XL), readiness, and duplicate hints.
   It can operate on a bounded batch of tasks.
2. **Conditional decomposition.** A ready, non-review-only, non-continuation
   L or XL task classified at complexity 4 or 5 may enter the planner path.
   The planner proposes a closed, dependency-ordered DAG of independently
   deliverable S/M children. Quorum classifies the proposed children before
   materializing the graph; planning can instead hold for a blocker or retry
   within its bounded policy. Smaller or otherwise direct-routed tasks skip
   this stage.
3. **Implementation and publication.** A worker receives an execution-ready
   task in an isolated worktree, implements it, verifies it, and publishes or
   updates the PR. A `--continue-pr` task starts from the recorded PR head.
4. **Independent review.** R1 reviews the PR head. Required R2 then has its
   own allocation and review responsibility; the safe default makes R2
   mandatory, while deterministic R2 sampling can be configured for later
   steady-state coverage. Neither reviewer is the author or merge authority.
5. **Rework and renewed review.** Blocking review feedback returns the same
   Proposed Change to a rework turn. The reviewer responsible for the changes
   verdict re-reviews the updated head in that same stage: R1 resumes R1, while
   R2-originated rework resumes R2 directly rather than restarting at R1. For a
   review-only task with no managed worker, the daemon provisions a remediation
   worker. Prior approval is not reused for changed code.
6. **Merge and collection.** Once the daemon has the required approvals and
   merge gates, it performs the merge. A detached collector then records
   post-merge review analytics. Collection failures are visible and have a
   bounded retry path. Qualifying, evidence-backed follow-up material is
   prepared for the separately bounded follow-up-planning path when that path
   is activated; it never reopens the merged task.
7. **Optional troubleshooting.** When enabled, a one-shot doctor turn can
   investigate a stalled task with no active worker or reviewer. It reports
   evidence; it does not take lifecycle authority.

Review-only tasks (`--review-pr`) intentionally skip initial managed
implementation. If blocking feedback requires code changes, the daemon enters
rework and provisions managed remediation for the existing PR. Reviewer-only
tasks (`--review-only`) also skip implementation but never enter rework: an
approval merges normally, while requested changes, failed CI, or a merge
conflict close the task as `done` with a durable `refs.review_outcome`, a
findings note, a daemon-owned GitHub REQUEST_CHANGES review, and an owner alert.
Such an unmerged close does not satisfy dependents: they stay blocked until their
`depends_on` is edited.

## Managed roles

These roles are responsibilities selected and supervised by the daemon, not
independent authorities:

- **Classifier** assesses task complexity, execution size, readiness, and
  duplicate hints; it also assesses proposed decomposition children.
- **Planner** turns a qualifying large outcome into a bounded dependency DAG of
  S/M children or records why it cannot safely split it.
- **Worker** implements and publishes the Proposed Change; its rework turns
  continue that same change.
- **R1** performs the first independent review of a PR head.
- **R2** performs a separately assigned second review when the review policy
  requires it; it is not an extension of the worker or R1 session.
- **Collector** performs post-merge, analytics-only extraction of review
  findings and possible follow-up material.
- **Doctor** is optional, off by default, and only troubleshoots an eligible
  stalled task.

The daemon alone applies lifecycle transitions, formal review disposition, and
merge. Provider output, agent messages, and task notes are inputs or evidence,
not lifecycle commands.

## Models and weighted routing

`quorum serve` reads reusable `[model_profiles]` that bind a provider runner,
model, and effort, then assigns those profiles through role-specific routing
pools. Claude and Codex are enabled for their configured managed roles; Grok
Build is enabled only for managed worker pools. Planner, reviewer, classifier,
and collector routing remain non-Grok.

The complete vocabulary and a production-shaped policy are in
[`docs/serve-config.example.toml`](docs/serve-config.example.toml). This is a
representative excerpt, not a complete serve configuration:

```toml
[model_profiles.terra]
runner = "codex"
model = "gpt-5.6-terra"
effort = "high"

[model_profiles.opus]
runner = "claude"
model = "claude-opus-5-5"
effort = "high"

[routing.classifier]
terra = 95
opus = 5
[routing.planner]
opus = 100
[routing.collector]
terra = 95
opus = 5
[routing.worker.3]
terra = 80
opus = 20
[routing.reviewer.3]
terra = 80
opus = 20
```

Configuration requires pools for classifier, planner, collector, workers at
complexities 1–5, and reviewers at complexities 1–5. Every referenced profile
must be valid for its runner, every weight must be positive, and each pool must
total exactly 100. Classifier and collector turns fail over to another pool
profile when a provider is unavailable, so name two providers in those pools. A
single-provider pool alerts the owner and retries only within each role's
budget. Task classification stays pending, but planning classification holds its
graph and fails the source task after two provider failures, and a collection
dead-letters after five attempts. The planner pool currently accepts Claude
profiles; the complete example reflects that constraint.

Weights are not independent per-call randomness. For each pool Quorum builds a
shuffled, durable 100-slot assignment epoch containing the configured number of
each profile, then atomically advances a cursor as it creates assignments. That
makes the allocation restart-safe and preserves the intended distribution per
epoch. R1 and R2 share the reviewer policy but use separate persisted assignment
bags, so one stage does not consume the other's allocation sequence.

## Where tokens are spent

Treat a submitted task as a pipeline of provider turns, not as one worker
session. Even with no rework, it can spend tokens on the intake classifier, an
optional planner and classifiers for proposed children, implementation, R1, R2,
the post-merge collector, and qualifying follow-up planning when activated.
Provider retries and an optional doctor turn can add further calls. A decomposed
parent can also produce several child implementation and review paths. At most
`max_graph_children_working` children of one graph implement at once; the key
defaults to `cap`, must be at least 1, and is effectively bounded by `cap`.

Set token, USD, and wall-clock ceilings in the
[serve configuration](docs/serve-config.example.toml); the exact flags are also
listed by `quorum serve --help`. Do not assume every auxiliary role is covered
by a single worker-task budget: classifier, planner, collector, follow-up, and
doctor work are distinct managed responsibilities. Inspect live work with
`quorum status` or `quorum web`, individual task/run details with
`quorum task-get --task-id <N>`, and aggregate terminal-task reporting with
`quorum perf [--by complexity|reviewer|risk-flag]`. Aggregate rows use the same
included-intent evidence as `quorum perf --facts --json`; `EFF_TOKENS` is a
provisional effective-token sum, and `-` means token coverage is incomplete.

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

For the initial self-hostable container image, including its persistence and
provider packaging contract, see [`docker/README.md`](docker/README.md). It is
an image/runtime foundation, not a completed hosted service.

State lives under `~/.quorum/repos/<owner>__<name>/`.

## Start it

For a normal repository:

```sh
quorum serve \
  --repo owner/name \
  --repo-dir /path/to/repo \
  --worktree-base /path/to/worktrees
```

Use `quorum serve --help` for provider, model, concurrency, troubleshooting,
and budget settings. The configured `log_dir` also receives daemon decisions in
`serve.log`; it rotates at 1 MiB and retains the active file plus two prior
files. This repo uses `scripts/serve-supervisor.sh` so Quorum can rebuild and
restart after updating itself. Only one daemon can manage a repository database
at a time.

### Branch responsibilities for operators

`base_branch` and `self_update_branch` are independent settings. Set
`base_branch` for managed task and PR work: it supplies the worktree base and
the PR validation and merge target. Set `self_update_branch` for Quorum's own
build-staleness poll and supervised rebuild source. For example, a repository
can send task PRs to `develop` while rebuilding its daemon from `main`:

```toml
base_branch = "develop"
self_update_branch = "main"
```

For compatibility, if neither `--self-update-branch` nor
`self_update_branch` in the serve config is set, the self-update branch is the
fully resolved `base_branch` (including any `--base-branch` override). Thus a
config that only sets `base_branch = "develop"` also polls `develop` for
self-updates; set `self_update_branch` explicitly to decouple the two.

When using `scripts/serve-supervisor.sh`, `QUORUM_SELF_UPDATE_BRANCH` selects
both the branch it fetches and the branch passed to daemon staleness checks. If
it is unset, the supervisor accepts `QUORUM_BASE_BRANCH` as its legacy
fallback; if both are unset, it uses `main`. A caller-supplied
`--self-update-branch` takes precedence over both environment variables.

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

Review a PR without ever letting Quorum change it:

```sh
quorum task-create \
  --created-by coordinator \
  --title "Review PR #412" \
  --review-only 412 \
  --body-stdin <<'EOF'
Review the external contribution; merge it only if it is sound.
EOF
```

`--continue-pr` creates a managed worker from the recorded PR head. `--review-pr` skips the
initial worker and starts with review. If a `--review-pr` PR receives blocking feedback,
the daemon enters rework and provisions a managed remediation worker for that existing PR.
`--review-only` also starts with review, but Quorum never reworks the PR: approval merges,
and requested changes, failed CI, or a merge conflict end the task as `done` with
`refs.review_outcome` (`changes_requested`, `ci_failed`, or `merge_conflict`) and the
findings. `--continue-pr`, `--review-pr`, and `--review-only` are mutually exclusive.

Give implementation tasks a concrete outcome, relevant constraints, and a way to verify
the result. The daemon chooses complexity, model, and effort. Managed agents receive their
assignment directly; they do not poll or claim tasks.

## See what is happening

```sh
quorum status                 # terminal overview (includes active branch syncs)
quorum web                    # loopback-only, read-only dashboard
quorum task-list --brief      # queue summary
quorum task-get --task-id 42  # full task and notes
quorum log --refs task#42     # lifecycle events
quorum tail Agent-42          # one managed session
quorum perf --by complexity   # facts-backed included-intent aggregates
quorum perf --by risk-flag    # outcomes and late blockers by classifier risk flag
quorum branch-sync --list     # active + last 10 terminal branch-sync rows
```

Configured branch synchronization runs internally between owner-selected branch pairs.
A clean sync creates no task; a conflict or CI failure creates one judgment task with
R1-only scoped review. Enqueue with `quorum branch-sync --by <id> --from <src> --to <dst>`
and cancel a clean-path row with `quorum branch-sync --cancel <id> --by <id>`.

The web dashboard's Live events view follows newly arriving provider events by default.
Use **Load history** to replay the selected run from its recorded beginning. Unless
`--log-dir` is supplied, `quorum web` reads the repository's configured daemon log root.

`quorum status` samples host-available memory, swap use, and free capacity on the database
and configured worktree filesystems even when the daemon is not running. Low-resource values
raise the read-only health display to attention; they do not throttle or terminate agents.

Use `quorum <command> --help` for exact flags and `quorum help` for the current workflow.
Most commands emit JSON. Exit codes are `0` for success, `1` for an expected negative
result, `2` for bad input, and `3` for an internal or database failure.

## Active work / roadmap

This is a non-binding view of ongoing directions, not a release schedule:

- Grok Build worker routing is active for the exact `grok-4.5`, `grok-4.6`, and `grok-4.7`
  models; attended real-CLI worker and lifecycle canaries remain
  required before production use.
- Continuity for turn-oriented runners is being matured, including sticky and
  dormant execution states.
- Provider failover across eligible routing alternatives and durable per-task
  base branches are active design directions.
- Qualifying, evidence-backed review follow-ups have bounded planning and
  storage foundations; their lifecycle activation remains separate work.
- The self-hostable container/runtime foundation is being matured; it is not a
  hosted Quorum service.
- The existing loopback-only, read-only web dashboard may expand as local
  inspection needs are proven, while retaining its local, read-only posture.
- `quorum orchestrator-mcp --listen <SOCKETADDR> --token-file <PATH> --identity <NAME>` serves
  an HTTP MCP server at `/mcp` for an outside orchestrating agent: create, list, inspect,
  cancel, and retry tasks, and message a managed agent, each through the same operation as its
  CLI command and all as the one bound identity. It requires an operator bearer token in an
  owner-only file, speaks plain HTTP, needs `--allowed-host` for a non-loopback bind, and is
  started by the operator, not automatically. The design record's "Orchestrator MCP" section
  states its trust model and limits.
- `quorum agent-mcp --listen <SOCKETADDR>` serves the managed-run MCP adapter over
  Streamable HTTP at `/mcp`, so a managed worker or reviewer can signal the daemon without
  the `quorum` binary or its socket. It is separate from the server above: it binds
  loopback only, speaks plain HTTP, authorizes each request by its run capability sent as
  a bearer token, and sends no tool-list-changed notification. Nothing starts it
  automatically, agent prompts still instruct the shell commands, and its GitHub tools
  remain unimplemented.
- `quorum inference-forwarder --listen <SOCKETADDR>` gives a sandboxed managed run model
  access without the upstream credential. It mirrors `POST /v1/messages` only (streaming
  or not) on loopback plain HTTP; a run points `ANTHROPIC_BASE_URL` at it and presents its
  run capability as the API key. Each request is authorized by the daemon endpoint while
  the run is live, then re-sent with the operator's credential; a stream is re-checked
  every `QUORUM_INFERENCE_AUTH_RECHECK_SECS` (default 60) and cut once its run ends, or
  once two intervals pass without the daemon confirming the run is live.
  Nothing starts the forwarder automatically, and its logs carry metadata only.
  - **Request allowlist.** Anything not listed is refused with 400, so a request cannot
    make the upstream act for the run. Top-level fields: `model`, `max_tokens`, `messages`,
    `system`, `tools`, `tool_choice`, `stream`, `temperature`, `top_p`, `top_k`,
    `stop_sequences`, `metadata`, `thinking`, `output_config`, `cache_control`. Content
    blocks: `text`, `tool_use`, `tool_result` (a string or text blocks), `thinking`,
    `redacted_thinking`. Tools: only tools the run executes itself (no `type`, or
    `custom`). Images and documents are refused even inline: their token cost is not
    bounded by their size, so it could not be held against a cap.
  - **Upstream credential.** Set exactly one of `QUORUM_INFERENCE_UPSTREAM_API_KEY_FILE`
    (recommended) or `QUORUM_INFERENCE_UPSTREAM_API_KEY`; both or neither is a startup
    error, and an empty variable does not count as set. The file form names a file read
    once at startup, so the credential never appears in the forwarder's environment, in
    container configuration, or in `docker inspect`; the path itself is not secret.
    Symbolic links are followed. The file must be a regular file of at most 4 KiB that
    its group and others cannot write; its owner and directory are trusted. Group and
    other read access is allowed, so a read-only mounted secret owned by another user
    (for example `0440` or `0444`) works; restrict who can read it through the mount.
    The file holds the credential on one line of printable ASCII with no spaces; one
    trailing newline is ignored. Errors name the variable and the reason, never the
    credential.
  - **Configuration** lives in the forwarder's own environment, which a run must not
    inherit: the upstream credential (above), `QUORUM_INFERENCE_UPSTREAM_URL`
    (default `https://api.anthropic.com`; `http://` only for a loopback host; TLS is
    verified against the bundled public web roots only, so an upstream behind a private
    certificate authority is not reachable), `QUORUM_INFERENCE_ALLOWED_MODELS` and
    `QUORUM_INFERENCE_ALLOWED_BETAS` (comma-separated; unset allows any model and no
    `anthropic-beta` value), `QUORUM_INFERENCE_RUN_TOKEN_CAP`,
    `QUORUM_INFERENCE_RUN_MAX_IN_FLIGHT` (default 2),
    `QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS` (upstream silence, default 600), and
    `QUORUM_INFERENCE_CLIENT_STALL_TIMEOUT_SECS` (a caller that stops reading, default 60).
  - **Token cap.** The cap counts input plus output tokens, not cost: models are priced
    very differently, so set `QUORUM_INFERENCE_ALLOWED_MODELS` whenever the cap is meant
    to bound spend. Every request must state `max_tokens`. A request is admitted only if
    the run's remaining allowance covers `max_tokens` plus an upper bound on its input
    (one token per body byte, plus 1024 for upstream framing); that amount is held while
    the request is in flight, so concurrent requests cannot pass the cap together, and
    is replaced by the upstream's reported usage when the response completes. A response
    that ends without final usage (the caller hung up, the stream broke, the upstream
    went silent after the request was sent) is charged the whole hold. Counts are kept
    in the forwarder's memory: they outlive a daemon restart and reset when the forwarder
    restarts.
  - **Compaction** (the upstream summarizing a long conversation) is off by default.
    `QUORUM_INFERENCE_ALLOW_COMPACTION=1`, together with a `compact-*` value in
    `QUORUM_INFERENCE_ALLOWED_BETAS`, admits `context_management` with compaction edits
    only and `compaction` blocks in replayed assistant content. The upstream bills a
    compaction step outside a response's top-level usage and itemizes it under
    `usage.iterations`; the forwarder charges the itemized total, and doubles the input
    part of the hold for such requests. A response to such a request that does not
    itemize its steps, streamed or not, is charged its whole hold.
  - **Limits.** 16 connections; 8 MiB request bodies (requests are text only; about twice
    a million-token conversation); a 64 KiB request head; a connection takes no new
    request after one hour. Worst case, each connection buffers one body at about four
    times its size while it is checked, roughly 512 MiB in all, and one run can occupy
    only its in-flight slots' share. A loopback peer with no capability can still occupy
    connection slots with unfinished request heads until each times out (10 seconds):
    the listener trusts loopback not to be hostile at that level.

## Working on Quorum

Contributor and agent instructions live in [`AGENTS.md`](AGENTS.md). The design record is
[`docs/2026-06-23-quorum-design.md`](docs/2026-06-23-quorum-design.md). Run the full local gate
before submitting any change. It serializes across linked worktrees so concurrent workers do
not saturate one machine. The linux/amd64 container build runs locally for changes under
`docker/`, `Dockerfile`, or `.dockerignore`; use `--docker` to force it. Required CI runs the
container build and verification for every PR regardless:

```sh
rtk proxy ./preflight.sh
```

The required suite includes one real-process SQLite smoke race per contention
path. To run the full repeated contention depth on demand, use:

```sh
scripts/stress-process-canaries.sh
```

## License

MIT — see [`LICENSE`](LICENSE).
