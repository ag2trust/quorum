# Sandboxed Workspaces — Implementation Plan

**Date:** 2026-11-12
**Status:** Proposed
**Spec:** `docs/2026-11-12-sandboxed-workspace-technical-spec.md`
**Implementation base:** `origin/develop` at `b090a1a`

Fourteen slices. Each is one PR against `develop` with its own tests and leaves `develop`
green. Slices 1–4 harden today's behavior in both modes. Slices 5–12 build the clone model
beside the linked-worktree model, which stays the default and is unchanged. Slice 13 migrates
and flips the default. Slice 14 deletes the legacy path one release later.

Until slice 13, `workspace_mode = "clone"` starts only with the hidden test-only flag
`--incomplete-clone-mode`, following the existing hidden-seam precedent (`--merge-cmd`).
Operators cannot enable a partial clone mode.

## Shared adversarial fixture

Slice 1 adds `quorum/tests/support/hostile_git.rs`. Later slices reuse it.

- **Tripwire.** Every planted command appends its name to a file under a per-test
  `TRIPWIRE` directory. Every hostile test ends by asserting the directory is empty for
  daemon-side operations, and lists what fired for run-side operations, which are expected to
  fire.
- **`plant_workspace(dir)`.**
  - Hooks: `post-checkout`, `pre-push`, `reference-transaction`, `post-merge`,
    `pre-merge-commit`, `pre-commit`, `post-rewrite`, `pre-auto-gc`.
  - `.git/config` keys:
    - `core.hooksPath`, `core.fsmonitor`, `core.sshCommand`, `core.pager`, `core.editor`,
      `core.askPass`, `core.alternateRefsCommand`
    - `filter.t.clean`, `filter.t.smudge`, `filter.t.process`
    - `diff.t.textconv`, `diff.t.command`, `diff.external`
    - `merge.t.driver`
    - `credential.helper`
    - `remote.origin.url`, `remote.origin.pushurl`, `remote.origin.uploadpack`,
      `remote.origin.receivepack`
    - `include.path` pointing at a second planted file that sets `core.hooksPath`
    - `includeIf.gitdir:…`
  - Tree content:
    - `.gitattributes` with `* filter=t diff=t merge=t`
    - `.githooks/pre-push` with `core.hooksPath=.githooks`
    - an executable `preflight.sh` tripwire
    - a planted executable named like each provider binary at the workspace root
- **`plant_operator(home)`.** The same keys in a global `.gitconfig` under a test `HOME`.
- **`plant_repo_dir(dir)`.** The same keys in the operator checkout's config and `hooks/`.

## 1. Hardened git constructor

Add `quorum_core::git::{LocalGit, TransportGit}`, the two profiles in the spec's "Hardened git
invocation" section:

- cleared environment and allowlist;
- the full `-c` list;
- `--git-dir` only, run from `<state dir>/empty`;
- `--end-of-options`;
- SHA and ref validators;
- startup checks for the empty directories and the git version.

This slice is additive and nothing calls the new constructors yet.

Tests:

- Unit tests: exact argv and env for both profiles. `inherit` keeps `HOME` and `SSH_AUTH_SOCK`
  and strips every `GIT_*` variable. `hermetic` clears both.
- `plant_operator` plus a planted `GIT_CONFIG_PARAMETERS` in the parent env. With the local
  profile, `rev-parse`, `update-ref`, `fetch` from a `file://` repo, `diff`, and `archive` all
  succeed and the tripwire is empty. A `reference-transaction` hook in the global
  `core.hooksPath` does not fire.
- Protocol: the local profile rejects `ext::sh -c …`, `https://`, and `ssh://`. The transport
  profile for an `https` remote rejects `file://` and `ext::`.
- `--end-of-options`: a ref argument `--upload-pack=…` is passed as data, not as an option.
- Version gate: a fake `git` reporting `2.37` fails startup with exit 2.

## 2. Untrusted-file reader and symlink-safe removal

Add `quorum/src/untrusted_fs.rs`:

- `open_regular_beneath(root_fd, rel, cap)`, component-wise `openat` with `O_NOFOLLOW`, or
  `openat2` on Linux;
- `copy_capped_to_private(…)`, which writes the copy `O_EXCL`, mode `0600`;
- `remove_tree(root)`, which refuses a symlink root and wraps `remove_dir_all`.

Adopt it for HARDEN rows H1 and H2 (provider project-config discovery and the auth-state
copy-back).

Tests:

- final symlink, intermediate symlinked directory, `..` component;
- FIFO, which must return promptly without blocking;
- hardlink to a file outside the root (`st_nlink == 2`);
- sparse file over the cap;
- file growing past the cap during the copy, written by a concurrent writer;
- directory where a file is expected.
- `remove_tree`: a tree containing a symlink to a sentinel directory outside the root leaves the
  sentinel intact, and a symlink root is refused.
- H1: a project `.mcp.json` symlinked to an outside file is refused, and an oversized one is
  refused with a clear launch failure. Existing H1 behavior tests stay green.
- H2: an invocation-home auth file replaced by a symlink to a sentinel secret does not copy the
  sentinel into the durable home.

## 3. `gh` and repository-less `git` from an empty directory

Add one `GhCommand` constructor. It runs from `<state dir>/empty` with mandatory `--repo`, and
sets `GH_REPO`, `GIT_DIR=<empty>/.none`, `GIT_CEILING_DIRECTORIES`, and the prompt and update
switches. Port EMPTY-DIR rows E1–E3 and E5–E10. Port E4 (self-update `ls-remote`) and E11
(`check-ref-format`) to the local or transport git profile with an explicit URL. This applies
in both modes.

`quorum serve` stops calling `gh repo view` in `repo_dir`. It uses `repo`, then
`parse_repo_from_url` on the configured or derived URL.

Tests:

- A fake `gh` on `PATH` records `cwd`, argv, and env. Every daemon `gh` call site, driven by the
  existing integration tests (publication, merge, collector, cleanup, branch sync, drift), has
  `cwd` equal to the empty directory, carries `--repo` or `GH_REPO`, and has `GIT_DIR` pointing
  at the nonexistent path.
- With the daemon's cwd and `repo_dir` set to a repository whose planted `remote.origin.url`
  names a different `owner/name`, every recorded call still targets the configured repository.
- `gh api repos/{owner}/{repo}/…` calls carry `GH_REPO`, so the placeholders cannot come from a
  remote.
- E11: `validate_target_branch` gives the same accept and reject results as today, and spawns
  `git` from the empty directory.

## 4. Provider executable resolution

REMOVE row R18. `routed_provider_executable_in_path` resolves a bare name only through absolute
`PATH` entries and accepts a configured path only when it is absolute. Relative `PATH` entries
and relative configured paths are rejected with the existing fallback launch failure. This
applies in both modes.

Tests:

- `PATH=.:<bin>` with a planted executable at the workspace root resolves to `<bin>`, never the
  planted file.
- `PATH=::<bin>` behaves the same.
- A relative `agent_bin` fails the fallback launch with `NonFailover`, and the tripwire is
  empty.

## 5. Configuration and the private repository

Add these keys to the parser:

- `workspace_mode`, `private_repo`, `remote_url`, `git_transport_config`;
- `[workspace_limits]`.

Add `PrivateRepo`, which owns its manager mutex, and its operations:

- create with an empty template and Quorum-written config;
- verify at startup (unexpected config keys, `hooks/`, `info/attributes`, alternates, overlap
  with `worktree_base`, `log_dir`, or `repo_dir`, and the staging device ID);
- `remote_url` validation and the one-time derivation via `git config --file`;
- `fetch_branch`, `fetch_pr_head` (with fsck), `resolve`, and `is_ancestor`.

Linked-worktree startup is unchanged. `clone` mode starts only with `--incomplete-clone-mode`.

Tests:

- URL validation. Accepted: `https`, `ssh`, scp-like, and `file`. Rejected with exit 2:
  `ext::`, `fd::`, `-oProxyCommand=…`, an embedded `user:pass@`, CR/LF, and an empty URL.
- Derivation reads only `<repo_dir>/.git/config`. A planted `include.path` in that file is not
  followed. A `repo_dir` whose `.git` is a file requires an explicit `remote_url`.
- Startup refuses each tampered private-repository state with exit 3 and a message naming it:
  extra config key, hook file, `info/attributes`, alternates, overlap, cross-device staging.
- `fetch_pr_head` of a fork-like source containing an fsck-invalid tree (an entry named `.git`,
  or a `..` path, built with `git mktree --missing` in a fixture) fails with `fsck-failed`, and
  no ref is created.
- With `plant_repo_dir` and `plant_operator` active, `fetch_branch` honors neither, and the
  tripwire is empty, under both `inherit` and `hermetic`. `inherit` still overrides the planted
  global `core.hooksPath` and `core.fsmonitor`.

## 6. Bundle header parser and verified import

Add `bundle::parse_header` (pure Rust). Add
`PrivateRepo::import_bundle(staged_copy, Expectation { task_id, run_id, branch, start, exclude, limits })`,
implementing verification rules 4–15 from the spec, quarantine-ref promotion, and prune on
rejection.

Tests (each rejection asserts the exact rule code, that no `refs/quorum/tasks/*` ref moved, that
`refs/quorum/import/*` is empty, and that `git count-objects` shows no retained loose objects
after prune):

- **Extra refs:** `bundle create --all`, which carries `HEAD`, another branch, and the task
  branch. Rule: `unexpected-refs`.
- **Other branch only:** a bundle of `refs/heads/other`. Rule: `unexpected-refs`.
- **Task branch plus a tag.** Rule: `unexpected-refs`.
- **Unrelated history:** a bundle from an unrelated repository with no prerequisites. Rule:
  `no-prerequisite`. The same bundle exported with a forged prerequisite that is present but
  not seeded: rule `foreign-prerequisite`.
- **Unrelated root through a merge:** the task branch merges an unrelated root
  (`--allow-unrelated-histories`) and descends from the base. Rule: `new-root`.
- **Prerequisite not in the private repository:** exported against a commit the private
  repository never had. Rule: `missing-prerequisite`.
- **Rewritten history on rework:** the tip does not descend from the last imported head. Rule:
  `unrelated-history`.
- **No new commits:** `bundle create` refuses an empty bundle. A hand-built header whose tip
  equals the start gets rule `no-new-commits`.
- **Oversized:**
  - a staged copy over `max_bundle_bytes` (rule `oversized-bundle`);
  - a highly compressible 200 MiB blob inside a small bundle (rule `oversized-blob`, with bounded
    memory checked by the test);
  - `max_commits + 1` commits (rule `too-many-commits`).
- **Malformed header:** a v1 or `# v4` header, a 65 KiB header line, a NUL in a ref name,
  invalid hex, and a v3 `@filter=blob:none` capability. Rules: `malformed-header` and
  `unsupported-bundle`. An `@object-format` mismatch: `unsupported-bundle`.
- **fsck:** a bundle carrying a tree with a `.gitmodules` URL starting with `-`, or a `.git`
  entry. Rule: `fsck-failed`.
- **Hostile planting:** `plant_operator` plus a workspace planted with every hook and key
  produces a valid bundle. The import succeeds and the tripwire shows no daemon-side firing.
  Only the run-side export, performed by the test, may fire planted workspace hooks.
- **Idempotent replay:** importing the same staged copy twice for the same run ID gives one
  promotion and a clean second outcome.

## 7. Hardened publication from the private repository

Port M1–M4 and M15–M21 to `PrivateRepo`:

- pin, retire, and reconcile publication sources;
- `push_new_branch` and `push_to_pr_head`, with the same lease, fast-forward, and `ls-remote`
  checks, using `push --no-verify --porcelain --end-of-options <remote_url>`;
- cleanup tombstones;
- task-ref deletion.

Clone mode uses these. Linked-worktree mode is unchanged. Remove
`QUORUM_CONTINUATION_BASE_BRANCH` and the `hook-stdout` section from clone-mode push
diagnostics.

Tests:

- **Planted `pre-push` (the core regression):**
  - The pushed tree contains `.githooks/pre-push` and a `preflight.sh` tripwire.
  - The operator config sets `core.hooksPath=.githooks`, and `plant_operator` sets a global
    `core.hooksPath` to a tripwire hooks directory.
  - The push succeeds, and the remote ref equals the imported head.
  - The tripwire is empty, so neither `pre-push`, `reference-transaction`, nor `preflight.sh`
    ran.
- **Push target:** the push goes to `remote_url` even when `repo_dir` and the workspace both name
  a different `origin` URL. The decoy remote is unchanged.
- **Credentials:** under `hermetic`, a planted global `credential.helper` tripwire does not fire.
- **Remote rejection:** a bare `file://` remote with a `pre-receive` that rejects. The remote's
  own hook runs, as it must, and the task parks with the existing push-rejection reason, which
  contains the remote text and no local hook section.
- **Lease and fast-forward:** the existing `push_to_pr_head` tests are ported with the private
  repository as the source.

## 8. Workspace seeding, retirement, and cleanup

Add `Workspace::seed(variant)` in staging, followed by `rename` into `worktree_base`. Add
`Workspace::retire` and the retention sweep, using `untrusted_fs::remove_tree`. Add the DB fields
`seeded_base_sha` and `imported_head_sha` (additive migration).

Tests:

- The seeded workspace has no remote, no `hooks/`, and only the identity in its config. No file
  under `.git/objects` has `st_nlink > 1`. The rename is the only step that touches
  `worktree_base`.
- **Hostile repository content:** the base commit contains `.gitattributes` with filter, diff,
  and merge drivers, symlinks, `.githooks/post-checkout`, and an executable `preflight.sh`.
  Seeding checks them out verbatim, and the tripwire is empty, even with `plant_operator`
  setting `core.hooksPath` and filter drivers globally.
- A planted global `post-checkout` hook does not run during the seeding checkout.
- **Retirement:** a workspace containing symlinks to a sentinel directory outside, and a FIFO,
  is retired and swept without touching the sentinel, without blocking, and without running
  git.
- **Staging collision:** a pre-existing staging directory with the same name is retired first.
  Seeding never runs inside it.

## 9. Run outbox, export step, and worker flow in clone mode

The local launcher creates the outbox and runs the fixed export command after the agent exits,
including on timeout and kill. It runs in a separate process group with a scrubbed environment.
`fake_agent` gains a commit-and-exit mode. Clone-mode workers follow the lifecycle steps in the
spec: seed, run, export, collect, import, publish. Add the failure mapping from the spec's
"Failure modes" table for `no-delivery` and `bundle-rejected:<rule>` (`AgentFailed`,
`NonFailover`, and the park after repeated rejection).

Tests (real `quorum serve` with `--incomplete-clone-mode`, a `file://` remote, a fake `gh`, and
`fake_agent`):

- **Happy path:** the worker commits in its workspace. A PR is created from the imported head,
  the remote branch equals that head, and the task enters review.
- **Hostile workspace end to end:** `fake_agent` runs `plant_workspace` before committing. The
  task publishes. The tripwire shows no daemon-side entries. The test also checks entries from
  the export step, which are allowed but bounded by the export's hardening, so none is expected.
- **Symlinked bundle:** `fake_agent` replaces `delivery.bundle` with a symlink to the daemon's DB
  file. Outcome: `AgentFailed` with `bundle-rejected:unsafe-file`, the DB bytes never reach
  `imports/`, and nothing is pushed.
- **Symlinked outbox subpath:** the outbox is replaced by a symlink to a directory containing a
  valid bundle from another task. Rejected (`unsafe-file`).
- **FIFO bundle:** the daemon tick does not block, and the outcome is `unsafe-file`.
- **Oversized bundle:** the outcome is `oversized-bundle`, and `imports/` stays below the cap.
- **No commits:** the worker signals done without committing, so the outcome is `no-delivery`,
  with the retry budget consumed as for an agent failure.
- **Other branch:** the worker commits only on another branch, so the outcome is `no-delivery`.
  The export names only the task branch.
- **Repeated rejection:** the same rule on consecutive runs parks the task at the configured cap,
  and the park reason names the rule.
- **Rework:** a second worker run reuses the workspace. Its bundle's prerequisite is the last
  imported head, and the push is fast-forward.
- **Crash between collect and import:** restart replays the staged copy idempotently.

## 10. Read-only roles and planning in clone mode

Implement:

- reviewer, review-only, and continuation-reviewer seeding at `refs/quorum/pr/<n>`, with the
  read-only mode passed to the launcher;
- a durable seeded head SHA in the reviewer journal, replacing R17;
- the planner frozen view and frozen-base capture from the private repository, replacing M25
  and M26;
- `PrivateRepo::review_materials`, which writes the spec's file set into the run's inputs
  directory.

Prompts are unchanged; that is separate work.

Tests:

- **Reviewer verdict recovery:** recovery reads the journal's seeded SHA. A reviewer workspace
  whose `HEAD` was moved by `fake_agent`, or whose `.git` was replaced by a symlink, does not
  change the recovered SHA, and no git runs in it.
- **Planner view:** the view is built from the frozen SHA even when `repo_dir`'s `HEAD` moves.
  A planted `repo_dir` config has no effect.
- **Review materials:**
  - outputs match `git diff` and `git log` on a fixture;
  - a `diff=t` attribute plus a global `diff.t.textconv` tripwire does not fire;
  - an oversized diff is truncated at the bound, with a `TRUNCATED` trailer listing the
    omitted paths;
  - `remerge.patch` is present for a daemon-made merge.

## 11. Continuation and branch synchronization via `merge-tree`

Replace R5–R8 in clone mode:

- `merge-tree --write-tree` in the private repository;
- `commit-tree` for clean merges, pushed through the hardened push for branch sync, and used as
  the continuation seed;
- for conflicts, the conflicted paths come from `--name-only` output, the workspace is seeded at
  `<ours>` with `<theirs>` present, and run instructions tell the agent to merge;
- prompt text at `quorum/src/serve/mod.rs:3127-3130` adjusted for clone mode.

Tests:

- **Clean merge:** the continuation seed equals the expected two-parent merge commit. Branch sync
  pushes it, and the existing completion ancestry checks pass.
- **Conflict:** the judgment task body lists exactly the conflicted files. The seeded workspace
  has both commits. The agent's merge in the workspace, which is run-side, then exports and
  imports as a normal delivery.
- **Hostile merge configuration:** a tree attribute `merge=t` plus a global `merge.t.driver`
  tripwire. The daemon-side merge does not fire it. Without the driver, a `merge=union`
  repository produces a conflict, as the spec accepts.
- **`--ff` semantics:** when the base is an ancestor of the head, no merge commit is made.

## 12. Recovery, reconciliation, and cleanup in clone mode

Port M19–M24 (tombstones, recover refs) to the private repository. Implement:

- startup directory reconciliation under `worktree_base`: keep allocated directories, retire
  unallocated ones, unlink top-level symlinks;
- staged-bundle replay at startup;
- publication-pin reconciliation against the private repository;
- resume-from-recover through seeding (R13–R16 replaced in clone mode).

Tests:

- **Startup GC:** an unallocated workspace containing planted `.gitattributes`, filters, and
  hooks is retired, and no git runs. The tripwire is empty, which is the regression for R13's
  `git add -A`.
- **Top-level symlink:** a symlink under `worktree_base` pointing at `$HOME` is unlinked, and the
  target is intact.
- **Resume from recover:** a recover ref's snapshot tree is reproduced as uncommitted changes in
  the new workspace, through staging, and the old workspace is retired, not deleted.
- **Tombstones and pins:** the existing CAS tests are ported, plus a crash after the tombstone is
  written and before DB settlement.
- **Process crash during import:** the quarantine ref and staged copy are found at startup and
  resolved, either replayed or pruned.

## 13. Migration and default flip

Prerequisite: this repository's pre-push checks (base-branch and `fmt`) run as required CI jobs.
That is a `main` hotfix-class change, synced to `develop`, landed before this slice.

Implement:

- the one-time legacy import from `repo_dir` (task branch heads, publication pins, recover
  refs, cleanup tombstones) with fsck;
- the durable migration marker (additive);
- refusal to switch back once the marker exists;
- legacy worktrees left in place, with one logged event per task;
- in-progress branch syncs restarted from pinned tips;
- the `workspace_mode` default flipped to `clone`, and `--incomplete-clone-mode` removed.

Update `README.md`, `docs/serve-config.example.toml`, and the workspace and lifecycle sections of
`docs/2026-06-23-quorum-design.md`, since product behavior changes.

Tests:

- **Migration:** a DB plus a `repo_dir` with legacy linked worktrees, in-flight tasks, recover
  refs, and publication pins migrate. Every non-terminal task's head is imported, and every
  legacy worktree directory is byte-identical afterwards.
- **Replay:** a second start does not re-import. The marker exists, and the import does not run
  again.
- **Fallback refused:** `workspace_mode = "linked-worktree"` after migration exits 2 with a clear
  message.
- **Hostile `repo_dir`:** `plant_repo_dir` with every key and hook. The migration fetch succeeds,
  and the tripwire is empty (the `upload-pack` path).
- **No configuration:** a local operator with no new keys starts in clone mode, with
  `remote_url` derived and logged, and completes the slice 9 happy path.

## 14. Remove the linked-worktree mode

One release after slice 13, delete:

- the linked-worktree code paths in `worktree.rs` and `recover.rs` (REMOVE rows R1–R16);
- `disable_push`;
- the `workspace_mode = "linked-worktree"` value. It remains a recognized value that exits 2
  with a migration message.

Tighten the source-scan test (`quorum-core/src/git.rs:48`). Outside `PrivateRepo`, the
seeding module, and the local launcher's export step, production code must not construct a
`git` or `gh` process, pass `-C`, or set `current_dir` on one.

Tests:

- The source-scan test fails on a deliberately added `git -C` in a fixture copy.
- The full clone-mode suite from slices 5–13 still passes.

## Verification per slice

Each slice runs `rtk proxy ./preflight.sh` before its PR, as the repository guide requires. The
`hostile_git` tripwire suites run in the normal test target, not behind a feature flag, so
every later change keeps the property.
