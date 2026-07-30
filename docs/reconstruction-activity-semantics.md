# Reconstruction activity and liquidity-proxy semantics

## Purpose

The accepted reconstruction product contains quote events. It does not contain
centralized spot-FX transactions, and the historical HistData tick source does
not provide a defensible traded-volume field. This contract turns event
cardinality, timing, quote transitions, and spreads into explicit activity
evidence without relabeling any of those observations as traded volume.

The implementation lives in `histdatacom.synthetic.activity`. It consumes the
same final `SyntheticEventV1` rows that are stored by reconstruction
persistence. The 26-column event schema remains unchanged. Activity results are
compact derived metadata, while the 521-column analytical/training frame
remains an ephemeral input surface.

## Contracts

| Contract | Responsibility |
| --- | --- |
| `ReconstructionActivityPolicyV1` | Selected origin scopes, honest volume state, rounding, symbol/slice/provenance limits, and payload limit. |
| `ReconstructionActivityMetricV1` | One value with a fixed name, unit, aggregation rule, scientific semantics, support, optional confidence, and limitations. |
| `ReconstructionActivitySliceV1` | One symbol plus observed, synthetic, or merged population with metric coverage and bounded lineage. |
| `ReconstructionActivityBenchmarkEvidenceV1` | Activity-specific extraction from the shared reverse-degradation scorecard. |
| `ReconstructionActivityManifestV1` | Information-mode-bound collection of slices, policy, source content identity, optional product/calibration identity, benchmark evidence, and derived-bar rules. |

All version-one identities are SHA-256 hashes of canonical JSON. Readers
recompute policy, slice, benchmark-evidence, and manifest identities. Derived
claims such as the event schema version, metric definitions, bar semantics,
absence of a centralized-volume claim, and absence of an automatic winner are
also verified on read.

## Event and volume boundary

One durable `SyntheticEventV1` row is one delivered quote update. Exact
duplicate or stale quote deliveries still count as activity because the feed
delivered them. A separate transition metric distinguishes rows whose bid or
ask changed from rows whose quote remained stale.

The volume state is always explicit:

| State | Meaning |
| --- | --- |
| `unavailable` | No supported size or volume value exists. This is the default for the current final product. |
| `omitted` | A supported upstream value was intentionally excluded from this output. |
| `observed_source_size` | A future source supplies a documented size with stable units and provenance. The current event schema cannot claim this state. |
| `broker_supplied_size` | A broker supplies quoted or broker-specific size with documented semantics. The current event schema cannot claim this state. |
| `synthetic_activity_proxy` | A model-derived activity proxy is explicitly labeled as a proxy and is never called traded volume. |

`observed_source_size` and `broker_supplied_size` fail closed when aggregating
the current final event schema because it contains no size fields. Broker
capture retains any honest quoted/broker-specific size evidence separately; it
is not silently copied into reconstructed history. A future per-event size
requires a new event-schema version and its own support, units, provenance, and
validation contract.

## Metrics

Every slice carries the complete fixed metric set:

| Metric | Unit | Definition | Projection rule |
| --- | --- | --- | --- |
| `event_count` | event | Number of durable quote-event rows. | Sum. |
| `quote_update_count` | quote update | Number of delivered quote updates; equal to event count in v1. | Sum. |
| `exposure_duration_ns` | nanosecond | Last event time minus first event time in the slice. | Recompute from target interval event bounds. |
| `tick_intensity_per_second` | event/second | Event count divided by exposure duration. Unavailable for zero-duration slices. | Recompute count divided by duration. |
| `mean_interarrival_ns` | nanosecond | Mean adjacent event-time difference within the slice scope. | Transition-support-weighted mean. |
| `min_interarrival_ns` | nanosecond | Minimum adjacent event-time difference. | Minimum. |
| `max_interarrival_ns` | nanosecond | Maximum adjacent event-time difference. | Maximum. |
| `price_change_count` | transition | Adjacent quote transitions whose bid or ask changed. | Sum with prior-quote boundary carry. |
| `stale_quote_count` | transition | Adjacent quote transitions whose bid and ask were unchanged. | Sum with prior-quote boundary carry. |
| `stale_quote_rate` | ratio | Stale transitions divided by all quote transitions. | Recompute from transition counts. |
| `mean_spread` | price | Mean ask-minus-bid spread. This is a liquidity proxy, not volume. | Event-support-weighted mean. |
| `min_spread` | price | Minimum ask-minus-bid spread. | Minimum. |
| `max_spread` | price | Maximum ask-minus-bid spread. | Maximum. |
| `mean_event_confidence` | probability | Mean populated per-event confidence. Missing confidence is not filled. | Confidence-support-weighted mean. |

Counts and durations remain integers. Floating-point values are rounded only
when the immutable metric is finalized. Unavailable values carry zero support
and an explicit limitation rather than a zero masquerading as a measurement.

## Origin scopes and reconciliation

The default policy emits:

- `observed`: immutable historical anchors only;
- `synthetic`: accepted generated events only;
- `merged`: the final delivered observed-plus-synthetic product.

For every symbol, the merged event count must equal observed plus synthetic
counts when those origin slices are present. Events are never mutated or
copied into the manifest. Each slice retains only bounded sets of source
versions, generator IDs and versions, generator configuration IDs, reference
and motif IDs, feed-epoch IDs, broker profile IDs, constraint-set IDs, and
stream IDs, plus a hash of canonical event content. Exceeding a provenance
bound refuses the summary instead of silently truncating lineage.

## Information modes

Every activity manifest binds to `information_manifest_id` and
`InformationMode`:

- `ex_post_reconstruction` rejects an `as_of_ns` value;
- `ex_ante_simulation` requires `as_of_ns`.

The as-of value describes what information was available to the reconstruction
or simulation. It is not an upper bound on simulated event timestamps. The
existing information audit remains authoritative for motif, context,
calibration, and future-anchor use. Activity aggregation does not weaken that
audit and does not infer an information mode from event times.

## Bounded streaming

`summarize_reconstruction_activity()` consumes ordered event iterables using
constant numeric state per symbol/scope. Events may be interleaved across
symbols, but positions must increase strictly within each symbol.

`summarize_reconstruction_activity_streams()` consumes compatible
`SyntheticEventStreamV1` objects and retains their stream identities.

`summarize_committed_reconstruction_activity()` first verifies the atomic
publication and then requests only these 19 Parquet columns:

```text
event_id, origin, symbol, event_time_ns, event_sequence, bid, ask,
run_id, ensemble_member_id, source_version_id, generator_id,
generator_version, generator_config_id, reference_id, motif_id, feed_epoch_id,
broker_profile_id, constraint_set_id, confidence
```

Arrow batches default to 8,192 rows and are configurable. The implementation
does not load the 521-column frame, retain event rows, or produce a tick-sized
side table. Symbol, slice, provenance-value, and serialized-payload limits fail
closed.

Example:

```python
from histdatacom.synthetic import (
    InformationMode,
    summarize_committed_reconstruction_activity,
)

activity = summarize_committed_reconstruction_activity(
    "archive/reconstruction-products/.../commits/.../manifest.json",
    information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    information_manifest_id="information-manifest:sha256:...",
)
```

## Reverse-degradation and calibration evidence

`reconstruction_activity_benchmark_evidence()` consumes the existing
`ReverseDegradationScorecardV1`. It requires activity-relevant evidence in
every score slice:

- event-count relative error;
- intensity relative error;
- interarrival histogram L1 distance;
- burst-rate absolute error;
- quiet-rate absolute error;
- spread-mean relative error;
- spread-histogram L1 distance.

The derived evidence reports per-metric support and mean errors, restoration
gain versus the degraded input, promotion-eligible count, calibration-support
count, and execution failures. It carries scorecard and candidate-score IDs.
It cannot name a winner. Existing hard historical constraints and benchmark
promotion gates remain authoritative.

Event confidence and ensemble calibration are distinct. Missing per-event
confidence remains missing. A manifest may bind a separate immutable
calibration report ID; benchmark evidence records whether candidate uncertainty
support was present.

## Derived-bar handoff

The manifest embeds exact #18 projection semantics:

- event and quote-update counts sum;
- duration is recomputed from events in the target bar rather than blindly
  summing overlapping durations;
- tick intensity is recomputed from the target count and duration;
- transition counts sum only with the preceding-quote boundary carry needed to
  prevent window double counting;
- stale rate is recomputed from transition counts;
- mean spread is weighted by event support;
- volume remains unavailable unless separately sourced.

Derived bars must choose observed-only, synthetic-only diagnostic, or merged
product scope explicitly. Empty intervals, market closures, duplicate
timestamps, partial bars, and resumption identity remain #18 responsibilities.
No raw vendor M1 input is introduced by this activity layer.

## Failure conditions

Aggregation or deserialization fails closed for:

- unsupported or relabeled schema versions;
- fabricated centralized-volume or automatic-winner claims;
- source/broker size states without size-bearing event fields;
- run/member mismatches;
- non-increasing positions within a symbol;
- unsupported metric names, units, semantics, or aggregation rules;
- missing metric coverage;
- origin/merged count mismatch;
- reversed time bounds;
- non-finite or negative metrics;
- confidence outside `[0, 1]`;
- unbounded symbol, slice, provenance, batch, or payload sizes;
- benchmark slices missing required activity metrics.
