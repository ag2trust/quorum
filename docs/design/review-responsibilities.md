# Review responsibilities and lifecycle context

## Authority boundary

The GitHub PR discussion is authoritative for the review conversation. It records
BLOCKING and FOLLOW-UP findings, author remedies and pushback, reviewer responses,
and the evidence needed to determine whether a finding was fixed, accepted,
overridden, downgraded, reaffirmed, or remains unresolved. A reviewer must read the
relevant PR threads and independently determine the current disposition; a pushed
commit or an author's claim alone never resolves a finding.

The daemon may provide a bounded review ledger only as navigation context. It does
not replace the relevant PR threads, change a finding's disposition, or authorize a
lifecycle transition. Reviewers post findings and dispositions to the PR. Their
`quorum submit --verdict ... --blocking ...` payload is a lifecycle signal whose
blocker count attests to the complete current BLOCKING set; it is not a second
findings ledger. The daemon alone owns formal GitHub APPROVE/REQUEST_CHANGES reviews
and merge. It alone changes task lifecycle state.

## Bounded cross-round ledger

The current implementation has no reliable structured extraction of GitHub review
threads and intentionally introduces no durable daemon finding state. On every
re-review it therefore requires the reviewer to publish a standardized cumulative
disposition section in that SHA's PR review summary. This is the bounded-ledger
fallback; the PR remains the record of authority and recovery can reconstruct the
needed context by reading it.

The section has two independently bounded parts:

- `Prior BLOCKING findings`, in first-appearance PR chronology (oldest first), has
  at most 16 entries. Every entry gives a stable PR thread/comment reference and a
  bounded description, the author's claimed remedy or response (or `none found`),
  one reviewer-owned disposition (`fixed`, `reaffirmed`, `downgraded/follow-up`,
  `overridden/accepted`, or `unresolved`), and the reviewer's independent basis.
  A prior blocker cannot be silently dropped; `fixed` requires reviewer
  verification.
- `Newly discovered findings`, in discovery order, also has at most 16 entries.
  Each gives a stable PR reference and bounded description, its merge disposition
  (`BLOCKING` or `FOLLOW-UP`), the reviewer's evidence and rationale, and, for a
  BLOCKING finding in unchanged behavior, why it was not reasonably discoverable
  during the prior complete audit. New findings are never folded into prior entries.

Each reviewer-authored field is limited to 500 Unicode scalar values, including the
suffix. A truncated field ends with
`... [field truncated; read PR #<n> thread]`. If a section exceeds 16 entries, it
retains the earliest entries in the stated ordering and appends exactly
`TRUNCATED: additional ledger history omitted; read PR #<n> discussion for authoritative history.`
The marker is not a ledger entry. Omitted history remains on the PR, and omitted
current BLOCKING findings still count in `--blocking`.

This bounded section is required navigation context, not a quota for findings or a
substitute for the full current-diff audit. A complete review can still have zero
blockers; reviewers neither relax valid blockers to avoid rework nor manufacture
findings. FOLLOW-UP findings stay on the PR and never force a changes verdict.

## Final-opportunity calibration

`rework_round` is lifecycle-owned persisted state: it counts completed
`InReview -> Rework` changes transitions, not review ordinals. The initial review
has no rework-round context. The review-cycle context derives its cap directly from
the lifecycle's `REWORK_CAP` and marks a re-review as the final opportunity when
`rework_round >= REWORK_CAP`; it preserves the raw stored count rather than
normalizing unexpected values at the prompt boundary.

At the final opportunity, a changes verdict with any valid remaining BLOCKING
finding makes the daemon fail the task because no remediation transition remains.
The prompt must state this consequence neutrally: reviewers must not approve,
downgrade, omit, or defer a valid blocker merely to avoid failure, and must not
manufacture a blocker. Zero blockers is valid only after a complete independent
review. The same cycle context and bounded ledger are supplied to sticky re-reviews
and replacement/recovery reviewer prompts, including R1/R2 where they review a
rework head; initial-review prompts do not receive it.

## Future durable structured navigation state

If a future implementation adds structured daemon state, it remains a cache of
GitHub evidence, never a competing finding ledger. Before such state is used, the
daemon must fetch and reconcile the authoritative PR discussion. Missing, changed,
or unresolvable GitHub records invalidate the cache and require either a refreshed
snapshot or the standardized reviewer-published fallback above; stale cached state
must never resolve a finding or authorize approval, rework, or merge.

Each refresh must be atomic and idempotent. In one write transaction it must replace
or upsert the complete bounded snapshot for one PR, its GitHub record identifiers,
and its reconciliation/checkpoint metadata; a repeated refresh of the same GitHub
snapshot must produce the same stored state without duplicate findings. The snapshot
must keep stable GitHub record identifiers so reconciliation can detect deletion,
edits, and new discussion. The daemon may use that state only to point a reviewer to
the PR; the reviewer's current PR-published disposition and the daemon's independent
lifecycle guards remain authoritative.
