//! One-call orientation for an agent: command list, the safe text pattern, and exit codes.

pub fn cheatsheet() -> String {
    let rubric = quorum_core::complexity::rubric_inline();
    let rubric_full = quorum_core::complexity::rubric_lines();
    let recommendations = quorum_core::complexity::all_recommendation_lines();

    format!(
        r#"quorum — local agent coordination (by agents, for agents)

TASKS (daemon-managed queue) — lifecycle: open -> working -> in-review -> merging -> done (+ rework loop, terminal cancelled/failed)
  quorum task-create  --created-by <id> --title <s> [--priority N] [--labels '["x"]'] [--depends-on '[1,2]'] [--refs '{{"pr":N}}'] [--body-stdin]
                                                               # --labels complexity:N (1-5): {rubric}
                                                               # --depends-on gates the claim: dependent stays unclaimable
                                                               # until every listed task is `closed` (#2 alignment).
                                                               # --refs: structured external-ref JSON (e.g. {{"pr":N}}) — load-bearing
                                                               # for review-loop traceability (#10 + creator monitor #62).
                                                               # Malformed JSON → exit 2 at create (never poisons reads).
  quorum task-create  --created-by <id> --title <s> --review-pr <N> [--labels '["complexity:1","type:review"]'] [--body-stdin]
                                                               # Existing PR: skip implementation and start in-review.
                                                               # No managed worker exists for requested changes or conflicts;
                                                               # the outside PR author remains responsible for updating the PR.
  quorum task-update  --agent <id> --task-id <n> [--status open|cancelled] [--refs '{{"pr":N}}'] [--note-stdin]
                                                               # --status open: compatibility release path; daemon owns normal assignment
                                                               # --status cancelled: creator OR assignee terminal won't-do
                                                               # --refs: link PR ref, e.g. `--refs '{{"pr":2459}}'`
                                                               #   surfaced through `log --refs pr#N` + task inspection.
                                                               # NOTE: `done` is lifecycle-only — use `quorum submit` or `quorum task-close`.
                                                               #   Verdict transitions are daemon-managed only (#130).
                                                               # --note-stdin / --note-file: append a breadcrumb (any agent, no guard)
  quorum task-close --agent <id> --task-id <n> --reason-stdin   # manual/external terminal close (merged by hand, fixed elsewhere,
                                                               # obsolete). Also closes a `failed` task whose PR landed by hand.
                                                               # Emits `task_closed_manual` event — NEVER `task_done`.
                                                               # Owner/manual use; managed agents finishing work use `quorum submit`.
  quorum task-retry --task-id <n> --by <operator>               # retry a task durably parked by a provider failure
  quorum task-list [--status <s>] [--label <l>] [--assignee <id>] [--brief]
                                                               # --brief: summary rows (no body) for a token-cheap queue scan
  quorum task-get  --task-id <n>                               # includes append-only notes history
  # COMPACT WRITE RESPONSES (#64): task-update returns only {{id, status, assignee, refs}}
  # (+ note_id on --note-*). Body and descriptive fields are omitted —
  # you just wrote them, no need to re-pay tokens. Use `task-get <id>` for the full record.
  # LEASE RENEWAL (#130): the daemon renews the exact task lease for active workers each tick.
  # External writes no longer auto-extend leases. Silence past the lease lapses it.
  # A lapsed lease returns a claimed task to open (reaper, on next write) + posts a `reclaimed` event.
  # Lifecycle: open -> working -> in-review -> merging -> done (with rework loop).

COMPLEXITY RUBRIC (used by classifier and for task-creation guidance)
{rubric_full}

  Built-in model/effort routing policy per complexity level:
{recommendations}
  The active daemon provider selects its ladder; this is routing policy, not a cross-vendor benchmark.
  Daemon `suggested_models` config overrides the active provider's defaults when set.
  Explicit tier:/effort: labels on a task take precedence over defaults.
  Mismatch alerts are advisory — the daemon posts a note when actual < suggested.

SUBMIT (canonical hand-off — aliased as `done`, which is deprecated)
  quorum submit --agent <id> --pr <N>                                  # worker: signal task completion (PR posted)
  quorum submit --agent <id> --pr <N> --verdict approved --blocking 0  # reviewer: approve
  quorum submit --agent <id> --pr <N> --verdict changes --blocking <N> --feedback "..."
                                                                       # reviewer: request changes
  # Requires daemon run identity: reads QUORUM_RUN_ID env var (set by daemon at spawn)
  # or explicit --run-id flag. Validates capability — agent and role must match.
  # Verdict contract (#206): approved requires --blocking 0; changes requires --feedback.
  # `quorum done` is accepted but deprecated.

ROLES
  External callers create/inspect tasks and messages; they do not sync, claim, release, or submit work.
  Managed workers/reviewers receive task context from `quorum serve` and use submit/react for that run.
  Operators start `quorum serve` (or scripts/serve-supervisor.sh), inspect health, and use kill for emergencies.

REVIEW (lifecycle-driven)
  Lifecycle:  T: open -> working(claim) -> in-review(SignaledDone --pr N)
              Daemon attaches a reviewer (self-review blocked: author != reviewer)
              approve -> merging -> done(MergeSucceeded)
              changes -> rework(author resumes) -> in-review(ReworkPushed) [rework_round increments]

FEED (agent-to-agent messages)
  quorum post --agent <id> --kind info [--to <agent>] --body-stdin     # kinds: info request claim done hello critical
                                                                       # --to <agent> = direct message (vs broadcast)
  quorum read --agent <id> [--ack-through <seq>] [--limit N] [--direct | --broadcasts]
                                                                       # default: broadcasts + direct-to-you
                                                                       # --direct: only direct-to-you · --broadcasts: only general
  quorum read [--since <seq>] [--limit N]                              # without --agent: inspect without cursor (no ack)

EVENT LOG (auto-emitted state-change ticker; SEPARATE from messages)
  quorum log [--since <seq>] [--refs <subject>] [--limit N]            # task_created/claimed/done/released/cancelled/closed_manual/reclaimed/renewed
                                                                       # claim_taken/released/renewed. --refs filters: task#<id>, pr#<n>, etc.
  # read/post (FEED) = agent-to-agent MESSAGES you author. log (EVENT LOG) = state-changes the
  # SYSTEM auto-emits. Two streams, two cursors — `read` never surfaces `log` events and vice
  # versa. Want "what did agents say?" -> read; "what changed in the queue/claims?" -> log.

CONTROL (emergency halt; non-expiring — only `resume` clears)
  quorum stop   [--agent <id>] --by <id> --reason-stdin     # set; omit --agent = global halt
  quorum resume [--agent <id>] --by <id>                    # clear; emits stop_cleared event (exit 1 = nothing set)
  quorum stops                                              # list active stops

DAEMON IPC (M5 — agent-to-agent messaging + state reactions)
  quorum message --from <id> --to <id> --body-stdin  # send a message to another daemon-managed agent;
                                                      # daemon delivers as a turn when the target is idle
  quorum react   --agent <id> --task-id <n> --state <s>
                                                      # signal non-terminal state: blocked, failed, needs-info, note
                                                      # requires daemon run identity (QUORUM_RUN_ID or --run-id)
                                                      # daemon tracks per-slot and surfaces via `quorum status`
  quorum kill    --agent <id> --by <id> [--reason-stdin]
                                                      # hard-terminate a daemon-managed agent (SIGTERM->SIGKILL);
                                                      # task returns to open. daemon-down = row waits until consumed.

INSPECT (read-only diagnostics — includes expired rows, retention caveats)
  quorum inspect-message --seq <n> [--raw]       # single message by seq (live or expired)
  quorum inspect-messages [--author <a>] [--recipient <r>] [--kind <k>] [--topic <t>]
                          [--refs-contains <s>] [--state live|expired]
                          [--since-seq N] [--before-seq N] [--since-ts N] [--before-ts N]
                          [--limit N] [--raw]    # bounded message listing with filters
  quorum inspect-events [--kind <k>] [--subject <s>] [--state live|expired]
                        [--since-seq N] [--before-seq N] [--since-ts N] [--before-ts N]
                        [--limit N] [--raw]      # bounded event listing with filters
  quorum inspect-errors [--source <s>] [--state live|expired]
                        [--since-id N] [--before-id N] [--since-ts N] [--before-ts N]
                        [--limit N] [--raw]      # bounded error listing with filters
  # All inspect outputs include as_of timestamp and retention notes. Absence of a row
  # does not prove the event/message/error never existed — rows expire per TTL and may
  # be swept. --raw omits the wrapper (as_of, retention, bounded).

OPS
  quorum status [--watch] [--json] [--agents]    # health snapshot; --agents = agent presence (online/offline)
  quorum perf [--json] [--all] [--by complexity|reviewer]
                                              # model × effort performance aggregates over terminal tasks.
                                              # Default: prospective-only (tasks completed after analytics rollout).
                                              # --all: include historical tasks from before the rollout boundary.
                                              # --by complexity: adds complexity:* label as a cut dimension.
                                              # --by reviewer: splits by reviewer identity.
  quorum sweep                                # reclaim expired rows + checkpoint WAL (control state is NOT swept)
  quorum init                                 # create ~/.quorum + db (idempotent)
  quorum reset --yes                          # wipe ALL state -> clean db (needs --yes; refuses without)
  quorum help                                 # this cheat-sheet (alias: help-agent)

FREE TEXT (bodies): never pass as a flag. Use a quoted heredoc on stdin (disables shell
interpolation), or --body-file:
  quorum post --agent A --kind info --body-stdin <<'EOF'
  anything "goes": $vars, `backticks`, newlines
  EOF

EXIT CODES: 0 success · 1 clean "didn't get it"/not-holder (expected) · 2 usage/bad input · 3 internal/DB error
"#
    )
}

#[cfg(test)]
mod tests {
    use super::cheatsheet;

    #[test]
    fn help_shows_review_only_intake_and_ownership() {
        let help = cheatsheet();
        assert!(help.contains("--review-pr <N>"));
        assert!(help.contains("outside PR author remains responsible"));
    }

    #[test]
    fn help_teaches_daemon_managed_roles() {
        let help = cheatsheet();
        assert!(help.contains("TASKS (daemon-managed queue)"));
        assert!(help.contains("External callers create/inspect tasks"));
        assert!(help.contains("Daemon attaches a reviewer"));
    }
}
