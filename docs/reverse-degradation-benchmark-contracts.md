# Reverse-degradation reconstruction benchmark

The version-one reverse-degradation benchmark is the falsifiability boundary
for reconstruction generators. It asks whether a candidate can restore dense
modern delivery behavior after a controlled historical feed degradation while
preserving market, timing, anchor, and lineage constraints.

The benchmark does not select a production generator. It emits reproducible,
stratified evidence that later comparison semantics may use.

## Streaming boundary

The process is:

```text
dense modern reference events
  -> ObservationOperatorV1.degrade()
  -> degraded BenchmarkEventV1 stream
  -> transparent control or BenchmarkGeneratorV1
  -> candidate BenchmarkEventV1 stream
  -> ReverseDegradationBenchmarkV1.consume_window()
  -> bounded online aggregates
  -> ReverseDegradationScorecardV1
```

`BenchmarkEventV1` is the shared versioned interface on both sides of the
degradation/generation boundary. Adapters accept:

- dense `ObservationInputEventV1` values;
- sparse `ObservationOutputEventV1` values;
- observed or generated `SyntheticEventV1` values; and
- the row-aligned `synth_bid`/`synth_ask` surface produced by the existing
  empirical-overlay control.

Generator configuration and degradation configuration use separate
deterministic namespaces and IDs. A benchmark manifest rejects any identity
collision. `degrade_benchmark_window()` also verifies that the operator object
is the exact operator bound by the scenario before processing events.

Candidate-window event tuples are data-plane values. Their `metadata()` method
records only event count and bounded evidence; it never places events in
workflow history. `consume_window()` immediately reduces a complete window to
online accumulators and retains no reference, degraded, candidate, rejected,
or wide analytical rows.

## Immutable research periods

The existing reconstruction-information v1 schema remains immutable and keeps
its `train -> calibration -> validation` contract. The benchmark adds its own
three-period manifest:

```text
calibration -> validation -> final_holdout
```

`validate_benchmark_information_boundary()` requires:

- identical run and information-manifest IDs;
- exact reuse of the information manifest's calibration interval; and
- ordered benchmark validation/final-holdout intervals wholly contained in
  the information manifest's validation interval.

The benchmark therefore subdivides the already-withheld information boundary
without relabeling or mutating the upstream v1 contract. Scenarios may evaluate
validation and final holdout only. Every scenario must consume contiguous
windows spanning its complete split before finalization.

## Scenario matrix

`BenchmarkScenarioV1` binds:

- an immutable validation or final-holdout split;
- one feed epoch;
- one degradation severity;
- one exact observation-operator ID;
- bounded degradation parameters; and
- the shared benchmark-event schema version.

A valid manifest requires at least two feed epochs, at least two degradation
severities, and scenarios in both validation and final holdout. Epoch and
severity are separate dimensions: a historical technology epoch is not
silently treated as a sparsity level or market state.

## Transparent controls

Every manifest contains exactly one of each control:

| Control | Semantics |
| --- | --- |
| `no_fill` | Pass the degraded observation stream through unchanged. |
| `linear_interpolation` | Interpolate bid and ask on an explicit regular interval without reading withheld reference prices. |
| `resample_last` | Select the last degraded observation per explicit context-aware time bucket. |
| `empirical_overlay` | Accept the existing row-aligned `synth_*` output and require exact degraded-row cardinality. |

The interpolation and resampling intervals are part of the deterministic
generator configuration. The empirical adapter requires
`timestamp_utc_ms`, `synth_bid`, and `synth_ask`; missing or null generated
values fail closed. Controls appear in the same scorecard as candidates, so
complexity is visible relative to no fill instead of being presumed useful.

## Online metric surface

Fixed histograms and running aggregates cover:

- event counts and per-second intensity;
- inter-arrival distributions;
- burst and quiet-run rates;
- spread means, distributions, and transitions;
- midpoint endpoint and range behavior;
- historical-anchor preservation;
- uncertainty-interval coverage;
- common-timestamp ensemble diversity;
- cross-series metric hooks;
- strategy-sensitivity metric hooks; and
- attempts, convergence, failures, wall time, peak memory, scratch bytes, and
  durable bytes.

Scores are stratified by the exact tuple:

```text
symbol, feed epoch, session, event state, sparsity
```

Here `sparsity` is shared scenario context such as a degradation severity or
measured sparsity bucket. It is not a label for whether an event came from the
reference, degraded, control, or candidate stream.

Every slice retains reference, degraded, and mean candidate-member counts,
metric support, and its own soft loss. Candidate summaries include both mean
and worst-slice loss, restoration gain relative to the degraded surface,
uncertainty support, anchor support, and ensemble support. Aggregate means are
explicitly advisory; they cannot replace the stratified evidence.

## Hard gates and interpretation

Candidate windows accept bounded hard-constraint violation counts from the
synthetic-constraint and later carving layers. Missing protected reference
anchors are also converted into an automatic hard violation.

A reconstruction candidate is promotion-eligible only when:

- it was attempted;
- every attempt converged;
- it has no failure metadata; and
- it has zero hard-constraint violations.

A low soft loss can never override a hard violation. Controls are never marked
promotion-eligible.

`ReverseDegradationScorecardV1` always serializes:

```json
{
  "automatic_winner": false,
  "winner_candidate_id": null
}
```

Relative-to-no-fill deltas are evidence, not a ranking. No default generator or
automatic winner exists until separate comparison semantics are established.

## Bounds and replay

`BenchmarkProfileV1` freezes:

- scenario, candidate, slice, event, hook, and reason-code limits;
- fixed inter-arrival and spread histogram buckets;
- burst and quiet thresholds;
- score rounding; and
- final JSON payload bytes.

Exceeding a bound fails before an unbounded scorecard is produced. Stable IDs
cover the profile, splits, scenarios, generator/degradation configurations,
manifest, events, execution evidence, slices, candidate scores, and final
scorecard. Replaying the same versioned inputs produces the same JSON and
scorecard ID.

The engine accepts only time-owned events, complete configured ensemble-member
sets, complete mandatory controls, ordered contiguous windows, and complete
scenario split coverage. Window retries and durable artifact orchestration
remain the responsibility of the reconstruction streaming/checkpoint layer;
the benchmark consumes each deduplicated logical window once.

## Issue boundaries

- #431 supplies the narrow variable-cardinality event contracts.
- #432 supplies bounded windows, carry, resource, and artifact-reference
  contracts.
- #433 supplies the immutable information-mode and leakage boundary.
- #434 supplies feed epochs and transition evidence.
- #435 supplies the fitted observation operator and controlled degradation.
- #436 supplies the benchmark contracts, controls, online engine, scorecards,
  hard gate, and hooks documented here.
- #439 and later generator issues implement candidate generation behind
  `BenchmarkGeneratorV1`.
- #440 supplies detailed carving violations.
- #441 supplies synchronized cross-series metrics.
- #448 supplies downstream strategy-sensitivity metrics.

Changing split meaning, control semantics, metric meaning, promotion gating,
event identity, or required scorecard fields requires a new schema version.
