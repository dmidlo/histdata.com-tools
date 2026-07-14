# Empirical Motif Candidate-Generation Contracts

The empirical motif generator is the first production variable-cardinality
candidate for historical reconstruction. It proposes zero, one, or many
`SyntheticEventV1` rows between two immutable observations. It does not carve,
accept, broker-condition, or persist a final synthetic tick.

## Boundary and data flow

One call to `generate_empirical_motif_candidates()` consumes:

- a semantic `ReconstructionRunV1` and one owned
  `ReconstructionWindowV1`;
- two observed `SyntheticEventV1` anchors from the same source, symbol, run,
  and ensemble member;
- one bounded `ReferenceMotifQueryResultV1`, including its conditioning cell,
  support/backoff trace, and selected empirical fragments; and
- one `EmpiricalMotifGeneratorConfigV1` bound into the run configuration.

It returns an `EmpiricalMotifCandidateBatchV1`. Candidate rows and per-event
lineage remain process-local. The batch's `metadata()` projection is bounded
control-plane evidence and explicitly reports:

```text
candidate_only = true
hard_carving_status = not_evaluated
broker_conditioning_status = not_applied
final_storage_status = not_persisted
```

The metadata projection carries counts and SHA-256 digests for candidate,
transform, and event-lineage content rather than embedding those rows or large
ID lists. The process-local batch retains the actual rows for immediate
streaming or later artifact externalization.

The motif query's `used_at_ns` must equal the right-anchor timestamp. This
binds point-in-time retrieval evidence to the exact interval that consumes it
instead of allowing a query result from a different target boundary to be
silently reused.

This prevents a plausible empirical proposal from being mislabeled as a valid
historical, cross-series-consistent, broker-styled, or durable result.

## Variable cardinality and delivery regime

Version one derives target cadence from the conditioned `tick_intensity`
metric, with `interarrival_ns` as the fallback when intensity is absent. The
strictly interior cardinality is:

```text
floor((right_anchor_ns - left_anchor_ns - 1) / cadence_ns)
```

Timestamp precision is then applied relative to the left anchor. Multiple
events may quantize to the same timestamp, but their stable global ordinal is
used as `event_sequence`, so `(event_time_ns, event_sequence)` remains unique
and ordered.

Closed session states and zero target activity produce an explicit empty
decision. A zero-width or reversed anchor interval is an explicit refusal.
The generator never invents an ordinary weekend stream merely because a
global reference motif exists.

## Fragment selection and transforms

The complete anchor interval is planned before window ownership is applied.
For each segment, the generator derives a seed from:

- run ID and base seed;
- ensemble-member ID;
- generator/config ID;
- anchor-interval ID; and
- global segment ordinal.

Worker, retry, window, scratch path, and batch placement are absent. The seed
rotates through the deterministically ranked motif matches. A fragment may be
interpolated to more or fewer output events, but the resulting segment duration
must remain inside that fragment's declared time-scale range. Requested
volatility scaling is clamped to the fragment's declared price-scale range.

Each `EmpiricalMotifTransformationV1` records the source index, query/result,
fragment, window, series, period, source artifact SHA-256, backoff level,
support, distance, source-event count, output ordinal range, time and price
scales, clamp decision, spread-shape decision, seed, and confidence.

Each `EmpiricalMotifEventLineageV1` maps an emitted event ID to its transform,
global and segment ordinals, source/anchor progress, and requested timestamp.
The query result retained by the process-local batch supplies the full
conditioning cell, fallback trace, and source fragment. Config, transform, and
event-lineage contracts have deterministic JSON round trips.

## Seam and quote safety

Source mid-price and optional spread shapes are detrended to zero at both ends
of each transformed segment. They are applied around a linear bridge between
the immutable anchor mids and spreads. The virtual transform therefore equals
the historical anchors at progress zero and one, and adjacent fragments meet
on the same anchor bridge instead of accumulating translation jumps.

Target price precision is applied after transformation. Every candidate is
then checked for finite, positive bid/ask values and non-negative spread. One
invalid transformed quote refuses the complete anchor interval; version one
does not silently clip, swap, or partially retain an unsafe path.

`EmpiricalMotifCandidateBatchV1.merged_stream()` delegates to the frozen
synthetic-event stream contract. The caller's observed event objects are
passed through unchanged, generated timestamps remain strictly between the
anchors, and duplicate positions are rejected by the existing stream
validator.

## Window ownership, carry, and determinism

Every legal window recomputes the same complete semantic anchor plan and emits
only timestamps inside its half-open core interval. A partition edge can
therefore split one anchor interval without changing any retained event ID,
timestamp, bid, ask, motif selection, seed, or transform ID. Unioning adjacent
owned outputs equals a single-window result.

The returned `CarryStateV1` records the per-symbol watermark and last emitted
or observed event reference for downstream streaming orchestration. Large
state remains outside workflow history. Carry and window IDs do not enter
candidate identity.

## Sparse evidence and resource refusal

The generator consumes the reference index's exact-to-global backoff trace. A
`no_supported_cell` or `not_available_as_of` query result becomes a named
generation refusal, and every sparse/unavailable attempt remains visible in
batch metadata.

Before motif paths are materialized, the complete interval target count is
converted into a `ReconstructionResourceEstimateV1`. The run's storage policy
checks candidate amplification, peak batch events, retained members, inflight
batches, memory, scratch, and output estimates. Violations return the rejected
estimate and all reason strings in a `resource_limit` decision.

Generator safety caps and byte-size estimates are serialized with the config
but excluded from its semantic `config_id`. Consequently, changing legal
execution estimates changes resource evidence, not successful candidate IDs
or values. Semantic settings such as precision, closed-state handling, and the
pre-carving constraint namespace remain identity inputs.

## Reverse-degradation comparison

`EmpiricalMotifBenchmarkGeneratorV1` implements the shared
`BenchmarkGeneratorV1` interface. It turns adjacent degraded observations into
immutable anchors, queries the motif index under the configured information
mode, emits the anchors plus owned proposals, and passes that stream to
`generate_benchmark_candidate_window()`.

The existing reverse-degradation engine can therefore compare this generator
with no-fill, linear interpolation, resample-last, and empirical-overlay
controls across the same feed-epoch, severity, session, event, and sparsity
slices. The scorecard remains report-only: it does not automatically select a
winner or bypass hard historical constraints.

## Downstream stages

- [Historical carving](historical-carving-contracts.md) consumes these
  candidate-only batches and owns accepted/rejected constraint decisions.
- #441's implemented synchronized cross-currency reconciliation consumes these
  candidate streams after historical carving; see
  [`cross-currency-reconciliation-contracts.md`](cross-currency-reconciliation-contracts.md).
- later broker-transfer work owns broker-style conditioning.
- #446 owns final incremental Parquet layout and atomic publication.
- #447 owns production Temporal activities, backpressure, cancellation, and
  resume behavior.

Changing event-generation semantics, identity fields, decision meanings, or
lineage interpretation requires a new schema/generator version.
