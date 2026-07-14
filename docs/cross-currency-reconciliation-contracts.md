# Cross-Currency Reconciliation Contracts

Cross-currency reconciliation turns individually carved event streams into one
synchronized reconstruction unit. It is a generation-time invariant, not a
repair performed after independent symbol products have already been accepted.

The first certified relationship is:

```text
EURUSD / GBPUSD ~= EURGBP
```

The contracts are generic enough to represent inverse pairs as well, but this
issue does not add every available currency relationship.

## Common-coverage planning

`plan_cross_currency_windows()` consumes explicit half-open coverage for every
symbol in a `ReconstructionRunV1`. It intersects that coverage with the
requested interval and delegates only the common span to the existing
worker-count-independent window planner.

The returned `CrossCurrencyWindowPlanV1` records:

- available or explicitly missing status for every symbol;
- source periods used to establish each coverage range;
- per-symbol leading and trailing exclusions;
- group-level spans without common support;
- the limiting common start and end; and
- the complete all-symbol `ReconstructionWindowV1` sequence.

A missing leg or empty intersection produces a deterministic refused plan with
no windows. Unequal raw histories are therefore visible evidence and cannot be
silently shortened or filled.

## Relationship and projection policy

`CrossCurrencyRelationshipV1` supports two algebraic forms:

```text
triangle: numerator / denominator ~= direct
inverse:  left * right ~= 1
```

Each relationship declares a deterministic projection priority. The EURUSD
triangle projects EURGBP first, then EURUSD, then GBPUSD. Only synthetic events
are eligible. If every event at a supported point is observed, the quotes stay
unchanged even when the relationship is infeasible.

Projection changes the complete executable bid/ask envelope, not just the
midpoint. Triangle bid uses `numerator.bid / denominator.ask`; triangle ask uses
`numerator.ask / denominator.bid`. Inverse bid/ask use the opposite side of the
reciprocal quote. All generator, motif, anchor, feed-epoch, constraint, and
confidence lineage remains intact. `CrossCurrencyProjectionLineageV1`
content-binds the input and output quotes because `SyntheticEventV1.event_id`
intentionally does not include bid, ask, or confidence. A projection may
therefore retain the semantic event ID while still providing cryptographic
proof of the value change and its spread change.

The hard projection limit, residual tolerance, combined-spread multiplier, and
rounding precision are versioned in
`CrossCurrencyReconciliationConfigV1`. A required midpoint move beyond the hard
limit refuses the group. It is never clipped to a more convenient value.

## Exact event-time support

Reconciliation does not forward-fill, backward-fill, or interpolate another
instrument merely to manufacture simultaneous support.

Events are compared only when every leg has the exact same nanosecond event
time. Multiple events at one timestamp are paired by `(event_sequence,
event_id)` ordinal. Excess duplicates and asynchronous timestamps remain in
their original symbol streams and are counted in topology evidence.

The validation report includes union/common/asynchronous timestamp counts,
duplicate-event counts, and stale-forward-fill risk runs. The risk metric
describes what an unsafe downstream join could do; the reconciliation engine
does not perform that join.

## Anchor preservation and refusal

Every observed event in the input streams is hashed before reconciliation and
rechecked afterward. Observed quotes, timestamps, sequences, identity, and
source lineage must match byte-for-byte at the contract level.

The group refuses when:

- a required symbol stream is missing;
- a relationship has no exact event-time support;
- an observed-only residual exceeds its spread-aware tolerance;
- the required synthetic projection exceeds its configured hard limit;
- a projected quote would be invalid; or
- post-projection validation or anchor preservation fails.

Refused results retain the available unchanged streams and bounded failure
reasons. They cannot be presented as partially successful triangle products.

## Conditioned residual evidence

`CrossCurrencyConditionV1` maps half-open event-time intervals to session,
event, and feed-epoch keys. Every supported relationship point contributes to
all three dimensions. If no external condition exists, the event's own
feed-epoch IDs are used and other dimensions are marked `unclassified`.

`CrossCurrencyResidualSliceV1` reports support, projections, infeasibility, and
pre/post maximum residuals for each relationship/dimension/key. This keeps
coherence evidence stratified instead of hiding weak regimes behind one global
average.

## Generation and final validation gates

`reconcile_cross_currency_window()` always emits a generation-stage
`CrossCurrencyValidationReportV1`. A passing
`CrossCurrencyReconciledGroupV1` is eligible to proceed to ensemble and broker
conditioning, but it is not sufficient for publication.

`validate_cross_currency_output()` uses the same exact-time, spread-aware,
anchor-preserving rules at either of two explicit stages:

```text
generation
post_broker
```

The post-broker stage is mandatory even if the broker transformation claims not
to affect prices. It binds the complete final stream content hash and detects a
quote change that would otherwise retain the same semantic event and stream
IDs.

`validate_cross_currency_atomic_manifest()` requires:

- a passing `post_broker` validation;
- identical run, window, synchronization-unit, member, and symbol scope;
- one stream and manifest count for every symbol; and
- an exact match between final stream content and validation content.

The existing `PartitionManifestV1` remains the smallest publication unit. A
single leg or stale validation report cannot authorize a commit. Physical
Parquet staging and atomic promotion are implemented downstream by #446.

## Existing diagnostic compatibility

`cross_currency_quality_report()` adapts the reconciled group directly to the
existing #331 `HistDataCrossInstrumentConsistencyRule`; no permanent cache
roundtrip is required. The compatibility adapter buckets nanosecond times to
the existing HistData millisecond diagnostic grain, retains collisions as
duplicates, and never forward-fills.

The native reconciliation report remains authoritative at nanosecond event
time. The #331 report supplies the established triangle, inverse, grid, and
stale-join audit surface over the same reconstructed group.

## Streaming and issue boundaries

The group object is process-local. `metadata()` hashes output and projection
content and does not inline event or lineage rows into workflow history.
Window workers may write larger streams behind artifact references, then pass
only the bounded validation and group metadata forward.

- #442's implemented calibration layer consumes passing synchronized groups,
  retains bounded representative members, and hash-gates regeneration; see
  [`reconstruction-ensemble-calibration-contracts.md`](reconstruction-ensemble-calibration-contracts.md).
- #445 applies broker delivery conditioning and must rerun `post_broker`
  validation.
- #446 writes final Parquet and commits only through the atomic manifest gate.
- #447 maps these functions onto retryable Temporal activities.

Changing relationship meaning, projection priority semantics, event-time join
policy, hard-limit interpretation, validation stages, or atomic gate behavior
requires a new schema version.
