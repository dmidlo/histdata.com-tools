# Reconstruction Ensemble Calibration Contracts

Reconstruction ensembles represent uncertainty about missing market events
without promoting one generated path to historical truth. The version-one
layer plans reproducible members, evaluates their interval coverage through
reverse degradation, diagnoses non-substantive diversity, retains only a
bounded subset, and hash-gates on-demand regeneration of omitted members.

Dense event rows remain process-local or behind streaming artifact references.
The durable plan, calibration samples, storage estimate, report, and
regeneration request contain bounded metadata and aggregate evidence only.

## Deterministic plan and member identity

`plan_reconstruction_ensemble()` binds the complete plan to:

- normalized symbols;
- exact source artifact IDs and SHA-256 digests;
- exact generator, carving, reconciliation, and ensemble configuration IDs and
  SHA-256 digests;
- the versioned `EnsembleCalibrationConfigV1`; and
- a semantic base seed.

Member IDs are derived from those inputs plus a stable one-based ordinal.
Member seeds are then derived through `ReconstructionRunV1.seed_for()`. Worker
count, retry number, partition layout, row order, scratch path, and retention
rank are absent from member identity. Replaying the same source and config
hashes produces the same plan, member IDs, and seeds; a hash mismatch fails
before regeneration.

The configuration freezes member/retention counts, forecast horizons, nominal
and minimum achieved coverage, minimum fit support, collapse and false-
diversity tolerances, failure penalty, byte estimates, rounding, sample/slice
limits, and final payload bytes.

## Reverse-degradation calibration samples

`benchmark_ensemble_calibration_sample()` adapts one complete
`BenchmarkCandidateWindowV1` set from the #436 reverse-degradation harness. A
sample belongs to one exact stratum:

```text
feed epoch, session, event state, symbol, horizon, sparsity
```

It records compact reference and member metrics for event count, observed
duration, mean inter-arrival, mean spread, midpoint path range, endpoint
midpoint, and downstream sensitivity. It also records a logical-content hash
for each completed member. No event rows are serialized into the sample.

Unattempted work, failed execution, hard-constraint violations, empty streams,
and missing downstream sensitivity become explicit refused or failed member
results with bounded reason codes. Reports preserve attempts, completions,
refusals, failures, and their rates by split and exact stratum.

## Calibration and holdout semantics

Only `validation` samples fit interval adjustments. For each metric and
stratum, the engine forms the empirical member interval at the configured
nominal coverage, measures how far the withheld reference falls outside it,
and selects a deterministic finite-sample adjustment from those validation
nonconformity scores.

Only `final_holdout` samples measure the resulting raw and adjusted coverage.
Each `EnsembleMetricCalibrationV1` reports fit/evaluation support, adjustment,
covered counts and rates, raw/calibrated widths, median error, and a calibrated,
miscalibrated, or insufficient-support status. An absent final-holdout cell or
insufficient validation support cannot claim calibration.

The named confidence quantity is:

```text
finite-sample-interval-coverage-v1
```

Its scope is an exact stratum, metric, and horizon summary. It is not a
per-event probability, probability that a generated tick is historically
correct, or guarantee beyond the measured benchmark surface.

## Substantive diversity

`benchmark_logical_content_sha256()` and
`ensemble_logical_content_sha256()` hash ordered market content after removing
member, run, seed, source-row, and lineage identity. Input row order does not
change the hash.

For every split/stratum cell, completed member pairs are checked for:

- **collapse**: identical logical market content despite different members;
- **false diversity**: different content hashes with metric distance at or
  below the configured tolerance; and
- substantive diversity: distinct content with measurable metric separation.

Collapse or false-diversity rates above the versioned limits block calibrated
status. Merely changing IDs, seeds, or row ordering never establishes
diversity.

## Representative member, not winner

Validation samples alone rank members by normalized distance from the
validation ensemble median plus an explicit refusal/failure penalty. The first
eligible member is a compact representative primary and must be retained. This
selection is labeled:

```text
representative_member_not_historical_truth
```

The report always emits `automatic_winner=false`, `winner_member_id=null`, and
`default_generator_id=null`. Final-holdout results do not choose the primary,
and the report does not select a production generator.

## Storage and regeneration

`estimate_reconstruction_ensemble_resources()` accounts for candidate events
across every member, peak member memory, all-member scratch, and the largest
configured retained-member outputs. It delegates refusal to the existing
`ReconstructionStoragePolicyV1` preflight, so amplification, memory, scratch,
output, batch, and retained-member quotas remain centralized.

Only the configured retained members need durable final artifacts. The report
lists the other planned members as regenerable. A regeneration request is
accepted only when:

- the report is calibrated;
- the plan, run, config, and report identities match;
- every source and configuration hash exactly matches the frozen plan; and
- every requested member is omitted/regenerable, not retained or unknown.

Verification requires freshly computed hashes for the source and configuration
artifacts available to the regeneration worker. Repeating the plan's own
declared hashes without checking the available artifacts is insufficient.

The request returns stable member IDs and seeds; it does not bypass the normal
streaming, carving, synchronization, broker, validation, or publication gates.

## Confidence boundary with motif generation

The empirical motif generator retains `1 / (1 + match_distance)` only as
`uncalibrated-motif-match-similarity-v1` on its transformation evidence. As of
generator version 1.1.0, candidate `SyntheticEventV1.confidence` is null. Raw
retrieval similarity is not copied into a row field that could be mistaken for
calibrated confidence.

Any future populated event-level confidence requires its own versioned,
validated quantity and scope. The ensemble report's interval-coverage evidence
does not populate pointwise tick confidence.

## Streaming and downstream boundaries

The ensemble layer consumes passing carved and synchronized member windows and
the reverse-degradation evidence for the same semantic configurations. It does
not persist the wide augmented analytical cache or all candidate paths.

- #445 may apply broker delivery conditioning only after an ensemble has
  trustworthy evidence and must preserve member lineage.
- #446 implements retained-member/final Parquet layout and atomic publication.
- #447's implemented control plane owns production Temporal activities,
  retries, cancellation, and backpressure; see
  [`reconstruction-temporal-orchestration.md`](reconstruction-temporal-orchestration.md).
  backpressure, and regeneration execution.

Changing member identity, calibration split use, metric meaning, diversity
meaning, confidence scope, retention semantics, or regeneration authorization
requires a new schema or engine version.
