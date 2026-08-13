# Historical Carving Contracts

Historical carving is the first stage that may promote generator candidates
into accepted synthetic events. It consumes process-local candidate
rows, applies versioned historical constraints in a fixed order, and returns
accepted rows plus bounded decision evidence. It does not perform cross-series
reconciliation, broker conditioning, or final persistence.

`ReconstructionCandidateBatchV1` is the generator-neutral structural surface
used by `carve_reconstruction_candidates()`. The original
`carve_empirical_motif_candidates()` entry point remains a strict compatible
wrapper. `EventClockCandidateBatchV1`, `MarkedHawkesCandidateBatchV1`, and
`RegimeHawkesCandidateBatchV1` therefore reach the same hard carving and
output contracts without weakening the empirical API; see
[`classical-event-clock-challengers.md`](classical-event-clock-challengers.md),
[`marked-hawkes-challenger.md`](marked-hawkes-challenger.md), and
[`regime-switching-hawkes-challenger.md`](regime-switching-hawkes-challenger.md).

## Bound inputs

One carving call binds:

- the semantic `ReconstructionRunV1` and owned `ReconstructionWindowV1`;
- one primary structural candidate batch and optional same-scope substitution
  batches;
- the caller's immutable observed events, including both anchor IDs;
- one window-covering `MarketContextQueryV1` in the same information mode;
- one `HistoricalCarvingConstraintSetV1` listed in the run configuration; and
- when required, a `CarvingFingerprintEvidenceV1` wrapping the existing
  synthetic-fingerprint validator result.

Fingerprint evidence must report `match` and list every candidate batch it
validates. Reference and candidate quality-report IDs are part of the evidence
identity. A successful validation result therefore cannot be silently reused
for unrelated candidate rows.

The market-context query may report `no_matching_event`; an ordinary interval
with a complete calendar profile is still supported evidence. Timeline gaps,
point-in-time unavailability, missing calendar state, or an incomplete required
profile refuse the batch. Context and candidate information modes must agree.

## Fixed precedence and fail-closed behavior

Version one evaluates these rule stages in order:

```text
1. candidate integrity
2. immutable-anchor lineage
3. resource and pathological-gap envelope
4. fingerprint validation
5. market-context support
6. source-quality quarantine
7. session closure
8. conditioned motif eligibility
9. conditioned intensity
10. conditioned spread projection
11. final local validation
```

The exact rule IDs and order are embedded in the constraint-set identity.
Hard rules always precede conditioned rules. A weekend/session closure or
quarantine can therefore never be undone by a news, rollover, crisis, or other
state policy. Version one has no advisory carving rule: a configured
conditioned policy affects output semantics and is included in the constraint
set ID.

Hard failures are not repaired by projection. Every candidate must already be
a finite, positive, non-negative-spread candidate-only event strictly inside
its immutable anchor interval and the window's half-open ownership range.
Spread projection runs only after those checks and after the bound fingerprint
validator passes.

## Conditioned policies

`HistoricalCarvingConditionPolicyV1` matches explicit lower-case tags from:

- the motif condition's session, special, and event tags;
- the calendar state's sessions, overlaps, special, holiday, event, and
  combined tags; and
- market-context event kinds and tags whose conditioned windows overlap the
  candidate timestamp.

This supplies explicit policies for states such as `news_window`,
`daily_rollover`, and `crisis`. All matching policy IDs are retained. Their
acceptance rates and spread multipliers combine multiplicatively. The final
spread multiplier must remain inside the constraint set's hard bound.

Intensity thinning uses a stable score derived from run/member identity,
anchor interval, timestamp, event sequence, policy IDs, and constraint-set ID.
It excludes worker, retry, window ID, storage path, and execution order.
Repeating a batch or repartitioning the same anchor interval therefore cannot
change an owned event decision.

Spread projection keeps the candidate midpoint fixed and changes only the
spread. Price precision is explicit. A projected accepted lineage stores the
original bid and ask, candidate content hash, output content hash, exact policy
IDs, and multiplier. Projection cannot conceal the pre-projection quote.

## Motif refusal and substitution

A condition policy may restrict eligible motif IDs. If the primary event is
incompatible, the engine searches optional substitution batches at the exact
same `(event_time_ns, event_sequence)` position. Alternatives must share run,
window, member, symbol, anchors, and anchor interval. Selection is a stable
event-ID ordering.

An eligible replacement retains its own candidate batch, candidate event, and
motif transformation IDs in accepted lineage. If no same-position replacement
is eligible, the candidate is rejected as `motif_incompatible`. Version one
does not invent an interpolation or mutate an incompatible motif into apparent
support.

## Identity and accepted-event lineage

Candidate events use the candidate-only constraint namespace. Every accepted
event is rebuilt with the final carving constraint-set ID, producing a final
carving-stage event identity while preserving generator, reference, motif,
anchor, feed-epoch, and confidence fields.

`HistoricalCarvingEventLineageV1` connects the two identities and records:

- candidate and output event IDs and full-row content hashes;
- candidate batch and motif transformation IDs;
- accepted, projected, substituted, or substituted-and-projected action;
- applied rule, context-event, and policy IDs;
- original and final constraint-set IDs;
- deterministic acceptance score and spread multiplier; and
- original quotes whenever projection changed them.

This extra content binding is intentional. `SyntheticEventV1` version one does
not put bid, ask, or confidence in `event_id`; carving batch and lineage hashes
prevent a value change from hiding behind an unchanged semantic event key.

## Bounded rejection evidence and refusal

Rejected candidate rows are never retained by
`HistoricalCarvedCandidateBatchV1`. `RejectionSummaryV1` stores exact candidate,
accepted, and rejected counts plus deterministic reason counts. The counts must
reconcile. A configurable bounded sample stores only candidate ID, content
hash, source batch ID, position, reason, and rule ID. Batch metadata explicitly
reports `rejected_rows_retained = false`.

An empty upstream interval remains `empty`. Insufficient fingerprint/context
support, an upstream refusal, session closure, pathological anchor gap, resource
limit, or a batch with no admissible candidates becomes `refused` with a stable
reason. Some candidates accepted and some rejected becomes `partial`. Refusal
is a valid, measurable output rather than an exception or fabricated fill.

API misuse—such as cross-run batches, a context query for another window, or a
constraint set absent from the run—is rejected as a contract error before
carving begins.

## Final local validation and streaming boundary

Before accepted rows leave the stage, the local validator rechecks:

- final constraint-set and immutable-anchor IDs;
- half-open window ownership;
- unique timestamp/sequence positions;
- valid narrow event contracts and merged-stream ordering; and
- exact preservation of every caller-supplied observed event ID.

The batch records both the bound synthetic-fingerprint validator schema and the
local event-validator ID. `merged_stream()` delegates to
`SyntheticEventStreamV1` and passes observations through unchanged.

Only accepted rows and accepted lineage remain process-local. `metadata()`
uses content hashes instead of embedding them. Rejected rows are discarded;
final Parquet publication is implemented as the later #446 stage. This preserves the streaming
design: candidate generation, carving, and later reconciliation can operate one
bounded window at a time without materializing a permanent augmented-history
intermediate.

## Downstream stages

- #441's implemented synchronized cross-currency reconciliation consumes these
  accepted streams; see
  [`cross-currency-reconciliation-contracts.md`](cross-currency-reconciliation-contracts.md).
- later broker-transfer work owns broker-style conditioning.
- #446 implements final incremental Parquet layout and atomic publication.
- #447's implemented control plane owns production Temporal activities,
  backpressure, cancellation, and recovery; see
  [`reconstruction-temporal-orchestration.md`](reconstruction-temporal-orchestration.md).
  resume behavior.

Changing rule meaning or order, constraint identity, decision meaning,
projection semantics, or accepted-lineage interpretation requires a new schema
and engine version.
