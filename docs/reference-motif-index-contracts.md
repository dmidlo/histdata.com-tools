# Empirical Reference-Motif Index Contracts

The reference-motif domain turns bounded, modern augmented tick windows into a
versioned empirical library for downstream variable-cardinality reconstruction.
It indexes evidence; it does not generate, carve, or accept synthetic events.

## Evidence boundary

`reference_motif_source_window_from_training_frame()` reads only the augmented
row fields needed for identity, observed bid/ask values, event ordering, and
training eligibility. The hundreds of analytical and classical-model columns
remain available while selecting windows, but are not copied into motif
records.

Each `ReferenceMotifFragmentV1` retains:

- the source artifact reference and SHA-256;
- series, period, window, source-row, and source-event identities;
- inclusive source timestamp boundaries and duration;
- event-time offsets from the first observation;
- bid and ask deltas from the first quote;
- start, bid-only, ask-only, both-mark, and unchanged transitions;
- the complete conditioning cell and data-quality eligibility;
- the versioned admissible price/time scale and warp envelope.

The compact sequence is therefore replayable against its exact evidence
without serializing the 521-column augmented row for every event.

## Conditioning and fallback

`ReferenceMotifConditionV1` covers the following coordinates:

- symbol and currency exposure;
- technological feed epoch;
- session state, active sessions, overlap, rollover/special, holiday, and event
  tags;
- return, range, volatility, spread, activity, and inter-arrival regimes;
- timestamp precision, price precision, and source-quality state;
- bounded numeric return, range, volatility, spread, intensity, inter-arrival,
  precision, and source-quality metrics.

Retrieval starts with the exact cell and follows the configured hierarchy
through symbol/epoch/session/event, state, currency, symbol, epoch, and global
cells. Every attempted level records its pattern, candidate count, available
count, minimum support, and outcome. A sparse or point-in-time-unavailable cell
cannot silently become a match.

Within the first supported cell, weighted normalized metric distance and
categorical penalties are explicit. Results are ordered by `(distance,
fragment_id)`, making tie-breaking stable for fixed artifacts and config.

## Split and leakage policy

Version one requires chronological `train`, `calibration`, `validation`, and
`final_holdout` splits. Only eligible training windows may enter the index.
Other splits are counted but never serialized into the reference library.

Before retention, the builder audits all declared source windows for:

1. overlapping or guard-adjacent intervals from the same source artifact and
   symbol across train and a withheld split; and
2. equal normalized quote-shape signatures across train and a withheld split.

Either condition raises `ReferenceMotifLeakageError`; it is not downgraded to a
warning. Normalized signatures retain relative event timing, bid/ask shape, and
transition marks, so price- or time-scaled copies cannot cross the boundary.

## Bounds and artifact identity

`ReferenceMotifIndexConfigV1` bounds source windows, events per fragment,
retained fragments, matches, exclusions, artifact bytes, and fallback levels.
When eligible training windows exceed the retention budget, stable hash
priority selects a worker- and input-order-independent subset. Counts for
withheld, ineligible, and budget-omitted windows reconcile with the original
source-window count.

`write_reference_motif_index()` uses an atomic local replacement and returns an
`ArtifactRef` containing the schema, index/config identities, counts, size, and
SHA-256. `read_reference_motif_index()` verifies the optional reference before
restoring every nested deterministic ID.

Period-scale operation remains bounded by `max_source_windows` during build,
`max_fragments` after retention, and `max_fragments * fallback_levels` during a
query. Dense event panels are not retained beside the artifact.

## Point-in-time use

An ex-ante `ReferenceMotifQueryV1` requires `as_of_ns`. Retrieval hides a motif
unless both its artifact availability and its last source observation are no
later than that time. If all supported evidence is hidden, the result is
`not_available_as_of`, not an empty successful match.

`reference_motif_information_inputs()` maps every returned motif to the #433
information graph with:

- `MOTIF_SELECTION` stage;
- `EMPIRICAL_MOTIF` scope;
- training split identity;
- exact observation interval and availability;
- actual ex-post lookahead, or zero ex-ante lookahead.

This lets the existing reconstruction information audit enforce the same
point-in-time rules before #439 generation begins.

The implemented candidate consumer is documented in
[`empirical-motif-generation-contracts.md`](empirical-motif-generation-contracts.md).
It retains the query result and compact source lineage while keeping candidate
rows process-local and separate from later carving.

## Trust gates

- Withheld or ineligible windows never enter the index.
- Cross-split overlap or normalized near-duplicate evidence fails closed.
- Returned fragments include source artifact, row, event, window, and quality
  lineage.
- Fallback level, support, distance, and tie-breaking are observable.
- Artifact and nested contract identities are content-derived and verified on
  read.
- The index does not create candidate or final synthetic events.
