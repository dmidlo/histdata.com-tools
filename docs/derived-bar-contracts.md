# Derived Reconstruction Bar Contracts

Derived candlesticks are optional views over a verified final reconstruction
product. They are not raw evidence and are not an alternative persistence path
for the enriched analytical frame.

## Product boundary

The sole version-one input is a committed
`ReconstructionProductManifestV1`. Before aggregation begins,
`verify_reconstruction_publication()` checks the input manifest, exact
26-column `SyntheticEventV1` Parquet schemas, byte hashes, logical hashes, row
counts, ordering, origin support, and artifact confinement.

The bar reader requests only the 19 event fields required for price, origin,
confidence, and bounded lineage calculations. Projection, symbol pruning, and
event-time predicates are pushed into Arrow dataset scans. Raw `ascii/T`
caches, raw HistData M1 files, and the 521-column analytical frame are never
opened by this module.

The durable derived product contains:

- a compact `DerivedBarProductManifestV1`;
- exact `DerivedBarV1` rows in Zstandard Parquet;
- monthly partitions by interval, scope, and symbol;
- physical byte evidence and logical content evidence.

The original reconstruction manifest ID, publication ID, logical event hash,
run ID, and ensemble member ID bind every derived product to its source.

## Versioned contracts

| Contract | Purpose |
| --- | --- |
| `DerivedBarIntervalV1` | Supported duration, UTC alignment, and half-open bin semantics. |
| `DerivedBarPolicyV1` | Intervals, scopes, empty/partial/transition policies, rounding, and resource limits. |
| `DerivedBarV1` | One non-empty, provenance-bearing price and activity bar. |
| `DerivedBarPartitionV1` | Monthly Parquet axis, row/time bounds, logical hash, byte hash, and row-group evidence. |
| `DerivedBarProductManifestV1` | Source binding, policy, partitions, counts, writer evidence, and publication identity. |

Unknown schema versions, relabeled derived fields, ID drift, unsupported
intervals, or inconsistent physical/logical evidence fail closed.

## Intervals and ordering

Version one supports exactly:

| Code | Duration |
| --- | ---: |
| `1m` | 60 seconds |
| `5m` | 300 seconds |
| `15m` | 900 seconds |
| `30m` | 1,800 seconds |
| `1h` | 3,600 seconds |
| `4h` | 14,400 seconds |
| `1d` | 86,400 seconds |

All intervals are aligned to the Unix epoch in UTC. Bins are half-open
`[bar_start_ns, bar_end_ns)`. Alternative time zones, exchange-local daily
boundaries, and arbitrary durations require a future schema version.

Events are consumed per symbol in canonical
`(event_time_ns, event_sequence, event_id)` order. Multiple rows at one
timestamp remain distinct and their `event_sequence` determines open and close
position. A repeated or reversed position is refused.

## Price fields

Every non-empty bar contains independent OHLC fields for:

- bid;
- ask;
- midpoint, calculated per event as `(bid + ask) / 2`;
- spread, calculated per event as `ask - bid`.

Mean spread is event-support weighted. Prices must be finite and positive,
spread must be non-negative, and OHLC extrema must enclose open and close.
Rounding is deterministic and part of the policy identity.
Each row carries that policy ID and rounding precision so its arithmetic can be
validated even when it is read apart from the compact manifest.

No interpolation, forward fill, price repair, or synthetic bar-only value is
allowed.

## Scopes

The policy can request any explicit combination of:

- `observed`: immutable observed events only;
- `synthetic`: accepted generated events only, for diagnostics;
- `merged`: the final practical observed-plus-synthetic product.

Each row records total, observed, and synthetic support. Counts must reconcile,
and origin-only scopes refuse support from the other origin. Downstream users
must select a scope explicitly; the default policy publishes merged bars only.

## Activity projection

The bar layer implements the handoff declared by #80:

| Field | Bar operation |
| --- | --- |
| `event_count` / `quote_update_count` | Count delivered quote events. |
| `activity_duration_ns` | Recompute from first and last in-bar event times. |
| `tick_intensity_per_second` | Recompute count divided by positive activity duration. |
| `price_change_count` | Count changed quote transitions with boundary carry. |
| `stale_quote_count` | Count unchanged quote transitions with boundary carry. |
| `stale_quote_rate` | Recompute over supported transitions. |
| `mean_spread` | Event-support-weighted mean. |
| `mean_event_confidence` | Mean over rows where confidence exists, with support count. |
| `volume` | Always null in version one. |

One event is one delivered quote update. Neither event count nor activity is
labeled centralized traded volume. Every row uses volume state `unavailable`
and asserts `centralized_traded_volume_claim=false`.

The previous quote in the same symbol/scope/interval sequence is carried into
the next non-empty bar solely for transition classification. This includes a
transition across omitted empty bins or a market closure. The previous event is
not added to the next bar's price extrema, event count, activity duration, or
content hash.

## Empty, closure, and partial bins

Empty bins emit no rows. This applies to ordinary quiet gaps and expected
market closures. The bar product therefore does not invent liquidity or use
forward-filled rows to make a visually continuous grid.

Partial flags describe explicit query cuts:

- `is_partial_start=true` when `start_ns` falls inside the emitted bin;
- `is_partial_end=true` when `end_ns` falls inside the emitted bin.

The requested nullable `query_start_ns` and `query_end_ns` are also durable
manifest fields and participate in publication identity, including when a
bound falls exactly on a bar edge.

Without explicit query bounds, the committed source product defines the
requested coverage and edge rows are not labeled partial merely because the
first quote occurs after the clock boundary. The event bounds remain available
for downstream support decisions.

## Provenance

Each bar retains:

- source product manifest, run, member, symbol, scope, and interval identity;
- first and last event IDs and event timestamps;
- observed and synthetic support counts;
- bounded source-version IDs;
- bounded generator, generator-version, generator-config, reference, motif,
  feed-epoch, broker-profile, and constraint-set IDs;
- a SHA-256 digest over the canonical in-bar event projection;
- a deterministic `bar_id` over the full logical row.

Lineage cardinality is bounded by policy. Exceeding the limit refuses the bar
instead of silently truncating provenance.

## Streaming and resource behavior

Aggregation retains one numeric/provenance state per active
symbol/scope/interval plus the previous quote needed for transition carry. It
does not retain input events or the analytical frame. The number of intervals
is fixed at seven, scopes at three, symbols are policy-bounded, provenance is
policy-bounded, and the total emitted bar count has an explicit ceiling.

Publication buffers a bounded number of bar rows per active monthly writer and
writes bounded Parquet row groups. Input `batch_size`, output buffer size, and
row-group size are physical execution choices; changing them cannot change bar
IDs or the logical product hash.

## Atomic persistence

`stage_derived_bar_publication()` writes below:

```text
derived-bar-products/
  schema=<bar-product-schema>/
  source=<reconstruction-manifest-id>/
  policy=<bar-policy-id>/
  .scratch/publication.tmp-*/
```

The staging directory is not discoverable. After every Parquet file and the
manifest pass validation, `commit_derived_bar_publication()` promotes the
complete directory into `commits/<publication-id>` with one same-filesystem
rename. Repeating an identical commit is idempotent. A matching logical
publication with different physical evidence is refused rather than silently
overwritten.

Monthly relative paths are:

```text
interval=<code>/scope=<scope>/symbol=<symbol>/
  bar_month=YYYY-MM/part-00000.parquet
```

The logical publication ID is independent of input/output chunk sizes. The
manifest separately records PyArrow version, Python runtime, compression, row
group size, partition byte hashes, and row-group counts.

## Verification and reading

`verify_derived_bar_publication()` checks:

- committed-directory identity;
- exact manifest-declared artifact set;
- absence of symlinked files/directories;
- exact 64-column Arrow schema;
- byte size and SHA-256;
- row-group bounds;
- partition symbol/scope/interval/month axes;
- row ordering and contract identities;
- row/time counts and partition logical hashes;
- product logical hash and compact-manifest identity.

`iter_derived_bar_batches()` supports column, symbol, scope, interval, and time
projection/pruning. `scan_derived_bars_polars()` provides the equivalent lazy
Polars surface. Time predicates use interval-overlap semantics (`bar_end_ns >
start_ns` and `bar_start_ns < end_ns`), so a leading partial bar is not silently
discarded. Both readers verify the publication before exposing rows.

## Downstream use

#448 must compare strategy/execution sensitivity using an explicit scope,
interval, product manifest ID, and time-aligned window. It must not compare a
merged bar series with an observed-only series without labeling the different
support.

#449 must reconcile:

- source reconstruction and derived-product identities;
- bar event/origin counts against #80 activity semantics;
- deterministic replay across batch and partition choices;
- raw-anchor preservation in the source event product;
- absence of raw M1 inputs, analytical-frame persistence, fabricated volume,
  and undeclared artifacts.

Derived bars are useful views, not historical truth and not evidence that a
synthetic reconstruction is valid by themselves.

## Qualified hierarchical aggregation

`histdatacom.synthetic.bar_hierarchy` adds an explicit, fail-closed hierarchy
over already qualified `DerivedBarV1` rows. It does not change the existing v1
row, policy, event digest, or bar identity. Callers must first verify the source
publication using the existing verification surface; structural validation and
hierarchical replay do not establish independent authenticity of input bars.

There are two deliberately different operations:

- `aggregate_qualified_bar_projection()` computes the exact bar-only subset
  without loading events. Its result is a partial field mapping, not a
  `DerivedBarV1`; the eight event-dependent fields listed below are absent.
- `reconstruct_bar_from_qualified_children()` accepts the exact ordered
  in-parent events, verifies that replay reproduces every child field and ID,
  and produces a complete, identity-equivalent `DerivedBarV1`. It requires
  event support and is **not** a tick-free storage optimization.

Both require one symbol, scope, source product, run, member, and policy. The
supplied policy must include both intervals. Children must be chronological,
non-partial, equal-duration, UTC epoch-aligned rows that exactly partition the
explicit parent bounds. Missing, duplicate, reordered, mixed-identity or
query-partial children fail closed. A finer interval cannot be synthesized
from a coarser one. A same-interval singleton is an identity projection; this
is the base case for `1m`, since no smaller supported interval exists.

An absent child bin is not filled, even when its absence reflects a known
market closure. Thus a sparse interval may have a valid directly aggregated
event bar but cannot make this hierarchy's complete-partition claim. Empty
children produce no synthetic row: requesting such a parent raises an explicit
empty-bin refusal. Weekly, session, and non-UTC alignments are unsupported and
require a successor contract, not a reinterpretation of `1d`.

### Exhaustive field rules

`bar_hierarchy_field_rules()` declares a rule for every serialized v1 field and
refuses a schema whose field set is no longer covered. The regression suite
checks the exact field inventory.

| Fields | Qualified hierarchy rule |
| --- | --- |
| `schema_version`, `event_schema_version`, `source_product_manifest_id`, `policy_id`, `rounding_digits`, `run_id`, `ensemble_member_id`, `symbol`, `scope` | Require exact equality across children. |
| `volume_state`, `volume`, `event_schema_augmented`, `raw_m1_input`, `centralized_traded_volume_claim` | Preserve equal frozen v1 constants; volume remains unavailable/null. |
| `interval_code`, `interval_ns`, `bar_start_ns`, `bar_end_ns` | Use the explicitly validated parent interval and UTC-aligned half-open bounds. |
| `first_event_id`, `first_event_time_ns` | First child endpoint. |
| `last_event_id`, `last_event_time_ns` | Last child endpoint. |
| `bid_open`, `ask_open`, `mid_open`, `spread_open` | First child open. |
| `bid_high`, `ask_high`, `mid_high`, `spread_high` | Maximum child high, independently for each price series. |
| `bid_low`, `ask_low`, `mid_low`, `spread_low` | Minimum child low, independently for each price series. |
| `bid_close`, `ask_close`, `mid_close`, `spread_close` | Last child close. |
| `event_count`, `observed_event_count`, `synthetic_event_count`, `quote_update_count`, `confidence_support_count` | Exact integer sum. |
| `activity_duration_ns` | Last event time minus first event time; never sum child durations. |
| `tick_intensity_per_second` | Recompute total event count over positive parent activity duration with the unchanged policy rounding; null for zero duration. |
| `is_partial_start`, `is_partial_end` | False only after rejecting all partial children and verifying exact partition coverage. |
| `source_version_ids`, `generator_ids`, `generator_versions`, `generator_config_ids`, `reference_ids`, `motif_ids`, `feed_epoch_ids`, `broker_profile_ids`, `constraint_set_ids` | Sorted set union, bounded by the existing policy's provenance cap. |
| `mean_spread`, `mean_event_confidence` | `must_recompute_from_events`: preserve the existing ordered floating accumulation and round only at the parent. |
| `transition_count`, `price_change_count`, `stale_quote_count`, `stale_quote_rate` | `must_recompute_from_events`: verify raw-quote incoming carry, then require exact additive count reconciliation and recompute the parent rate. |
| `event_content_sha256` | `must_recompute_from_events`: hash the ordered canonical in-parent event projection; child digests are not concatenable hash state. |
| `bar_id` | `must_recompute_from_events`: derive the unchanged identity from the complete parent payload, including its correct event digest and reductions. |

An event-count-weighted mean is valid only when sufficient unrounded support
is retained. Rounded child means are not such support: with precision one,
five equal-count child spreads `0.14, 0.14, 0.04, 0.04, 0.04` store means
`0.1, 0.1, 0.0, 0.0, 0.0`. Combining those produces `0.0` after rounding,
whereas the direct parent mean is `0.1`. The same problem applies to confidence
means, with their distinct non-null support count. Neither rounded-mean
composition nor an invented floating tolerance is allowed.

Likewise, different raw quotes can round to identical child endpoints.
Comparing those endpoints cannot safely distinguish a changed transition from
a stale one. If the first child has incoming carry, the full reconstruction
requires `previous_event`, the last same-symbol/scope source event before the
parent. This event participates solely in transition classification; it is
excluded from parent OHLC, counts, duration, confidence, and event digest.
Replay must reproduce every child's incoming transition result exactly.
Independently reset child bars therefore cannot silently become a continuous
parent. The caller remains responsible for the preceding event's source
provenance: v1 records transition results, not the incoming quote's identity.

### Frozen equivalence matrix and storage choice

The focused regression matrix compares direct aggregation with finest-child
aggregation for all seven intervals (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`)
and all three scopes (`observed`, `synthetic`, `merged`). Every complete row
field, endpoint, count, categorical value, content hash and bar ID must be
exactly equal; this implementation introduces no numerical tolerance. The
`1m` row is the explicitly labeled identity base case; every larger interval
uses qualified `1m` children. Duplicate timestamps retain canonical event
sequence ordering. Separate cases cover incoming carry across a closure,
rounded-endpoint collisions, rounded-mean counterexamples, the three-child
OHLC/count/spread reference, malformed support, and partition failures.

A storage-efficient pyramid may retain qualified fine bars and compute the
declared bar-only subset on demand. Applications requiring complete v1 rows,
scientific transition metrics, means, or matching v1 identities must retain or
reread verified event support. Removing events and claiming those omitted
fields would require a successor sufficient-statistics and identity contract;
this module makes no such claim.
