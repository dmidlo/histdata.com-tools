# Source-replayed training feature joins

Issue #610 adds join contract **1.0.0** to the v2.10 research substrate. It
does not change #606, #608, #649 or #651 bytes or meanings. The public consumer
executes native source/value replay before appending columns to a retained
#606 batch; a digest supplied by a caller is not a verification capability.

## Public execution and persistence

Import declarations from
`histdatacom.data_quality.training_join_contracts`, execution from
`training_join_views`, and persistence from `training_join_artifacts`:

```python
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
    replay_training_joins,
    training_join_records,
)
from histdatacom.data_quality.training_join_artifacts import (
    read_training_join_artifact,
    write_training_join_artifact,
)

# plan is a TrainingJoinPlanV1 over an already materialized TrainingBatchV1.
joined = materialize_training_joins(plan)
assert replay_training_joins(joined) == joined
rows = training_join_records(joined)
path = write_training_join_artifact(joined, output_directory)
restored = read_training_join_artifact(
    path, information_mode=plan.information_mode
)
assert restored == joined
```

The immutable plan contains the complete source catalog, ownership map,
original batch and request, named source bindings, ordered column declarations,
and appended-column information mode. Each output retains exactly the original
row ID, historical evidence-unit ID, decision clock and column order. No panel
or timeframe expansion occurs. The record consumer returns an unchanged
`spine` payload beside the appended scalar names, per-column `join_states`,
`spine_consumer_mode`, `join_information_mode` and
`historical_availability_verified=False`. Raw quote `vol` is never reinterpreted
as traded volume.

Every invocation replays the complete spine and ownership map, followed by
every declared source, even for zero selected rows, zero columns, an unused
binding, or rows belonging to another column entity. The fixed #606 verification
passes are per invocation, not per row/column. Quote indexes and bounded vintage
cutoff caches are process-local implementation details, not persisted admission
receipts. Already verified feature objects are consumed directly; a second
mutable-path interpretation cannot change values under an earlier ownership ID.

## Information modes are separate dimensions

`normalized_as_of` applies to appended non-null values. Selection requires
source time and normalized availability at or before the row decision, and
the complete retained dependency end at or before decision plus one nanosecond
(dependency intervals are half-open). Native bar/triangle replay still enforces
its own information-mode and generation policies. Explicit future-known null
metadata is also ineligible: a later cancellation cannot erase an earlier
available observation.

`ex_post` permits retrospectively known values, retains unknown availability
as null, and still uses the declared direction, age and dependency semantics.
Neither mode upgrades the spine. Its original consumer eligibility and unknown
historical availability remain unchanged; the legacy #606 causal consumer is
still refused. Normalized clock consistency, native source/value replay, and
historical availability authenticity are different claims. No output claims
authentic historical availability, empirical predictive skill, independent
synthetic history, qualified #607 weights, or protected-holdout approval.

## Native adapter contracts

| Family / namespace | Retained inputs and executable proof | Selection and limitations |
| --- | --- | --- |
| `market.tick.*` | Completely verified observed IPC or committed native product/member, with physical ordinal or native event sequence | Bid, ask, uncrossed midpoint/spread. Exact or prior with frozen age. Duplicate clocks select the last actual native sequence; different partition ownership at one coordinate is ambiguous. Legacy availability stays unknown. |
| `market.bar.<symbol>.<scope>.<timeframe>.*` | Owned reconstruction manifest, derived-bar manifest and canonical `CausalBarSnapshotV1`; native source replays publication and bars | Native last fully closed interval, all seven standard widths, exact retained snapshot cutoff. A partial cell cannot become a closed candle. Observed and generated support/origins remain distinct. |
| `market.indicator.<symbol>.<timeframe>.*` | Same native bar replay and registry as bar values | Exact native policy feature names and full indicator warmup support; no flat-price imputation or forward fill. |
| `triangle.<timeframe>.*` | Three-leg native bar snapshot, or projection snapshot with complete proposal/reconciliation/delivery evidence | Entity must belong to the actual three legs. Exact synchronized/bounded native math and full support; `projection.*` exposes native scalar summaries. Observed-only projection burden is not applicable, not zero. |
| `calendar.*` | Retained normalized calendar corpus, complete native replay and release/schedule identities; or an owned `FeatureMatrixSnapshotV1` rebuilt separately at each cutoff | Exact native currency/economy/series attribution for releases. Generic macro matrices are explicitly declared covariates: exact feature key, no fabricated official FX attribution. Unknown empty keys have no invented macro origin. Occurrence is exact only; actual/schedule state can persist with bounded age. |
| `forecast.*` | Owned full `ForecastFeatureSnapshotV1`, actual feature-baseline execution or full `ForecastEngineSnapshotV1` execution receipt | Point forecast retains the complete distribution, model and training/inference inputs. No actual/revision substitution. Full fitting history is a dependency; future scheduled dates and corpus coverage metadata are not future observed conditioning. Other model producers require an executable adapter, not a caller-supplied output hash. |
| `positioning.*` | Content-addressed CFTC corpus and complete retained raw source directory, bounded no-follow reads and native raw rebuild | Exact report family/scope/contract and dated native pair mapping. Current/restated or unproven original releases cannot supply normalized-as-of values. Integer UTC-day conversion preserves nanosecond midnight boundaries. CFTC futures positioning is not spot traded volume. |
| `activity.*` | Owned committed reconstruction product and exact recomputed native activity manifest | Full-window statistics are snapshot values at the full support end, never backdated event features. Quote activity is not traded volume. Historical availability remains unknown. |
| `broker_style.*` | Native fingerprint, retained capture sessions and actual complete refit, with optional exact native calendar profile / market-context timeline | Exact metric/condition/source symbol and whole capture support. Effective start is not proof of fitting availability: without a fitted-at evidence contract these are ex-post only. Successor profiles without their full predecessor proof are refused in this adapter version. |
| `synthetic_flow.*`, `uncertainty.*`, `lineage.*` | Reserved unsupported-emitter declarations | Explicit unsupported nulls; no synthetic order flow, confidence, provenance or health observation is invented. |

Source configuration schemas are closed. Tick bindings declare
`product_manifest_id` and `ensemble_member_id` (both null for observed quotes).
Bar/indicator/triangle paths are `(reconstruction_manifest, bar_manifest)` and
retain exact native snapshot JSON. Calendar has one corpus path; vintage/forecast
paths must already belong to the complete #606 derived inventory. CFTC paths
are `(corpus, raw_source_directory)`. Activity has one product path and one
manifest. Broker paths are `(capture_root, session_manifest, ...)`, one native
fingerprint, and optionally `calendar_profile_json` /
`market_context_timeline_json`. All other configuration keys are rejected.

## Direction, ownership and missingness

Each `TrainingJoinColumnV1` freezes name, native field and coordinate, entity,
definition version, direction, meaning, maximum age and native parent namespaces.
`training_join_parent_namespaces(family, column)` supplies the exact required
dependency-role tuple; omission, invention or incompatible family reuse refuses.
The roles are paired with exact machine-readable per-cell source/parent IDs,
native schema/origin and dependency clocks, not arbitrary declared derivations.

Exact selects only a matching coordinate. Prior and bounded-prior select the
latest eligible source coordinate at or before the decision, subject to the
declared inclusive age bound. Interval membership is `[start,end)` and is
supported only where a native source defines interval state. Nearest-future is
not an available direction. Native persistent-state joins may fall back to an
older actually available vintage when a newer release is delayed; equal-time
native records without an authoritative ordering are refused, never sorted by
opaque identity. Event, snapshot and closed-bar meanings forbid forward fill.

Null states distinguish unavailable, unsupported, not applicable, empty market
interval, insufficient warmup, expected closure, source outage, stale support and
unknown availability. `confirmed_absence` is a separate reserved meaning: no
current adapter infers it merely from an incomplete release inventory. A genuine
numeric zero remains an available value, not missingness. Native absence
declarations and warmup states survive as nulls without fabricating a numeric
availability clock; any explicit declaration clock must still be known by the
decision. Null does not authorize a causal claim.

Stale and delayed/unknown selected candidates retain typed
`TrainingJoinRefusalEvidenceV1`: exact source and parent IDs, native schema/origin,
selection/source/availability clocks and dependency interval. It contains no
future values. A wholly absent coordinate retains its verified source root and
explicit reason. Full artifacts permit exact replay of either case.

## Bounded durable contract and verification

The closed v1 envelopes are source, entity, column, plan, refusal evidence, value,
row and batch contracts. They use the inherited #606 strict canonical reader,
schema/identity checks, 8 MiB encoded bound, depth 16 and 200,000 structural-node
bound. Additional preflight limits are 256 spine rows, 128 columns, 4,096 cells,
32 source bindings, 32 paths / native evidence items per binding, 2 MiB
aggregate embedded evidence per binding, 64 KiB configuration, 4,096 candidate
records and 32 retained
equal-coordinate refusal candidates. Native readers retain their stricter
source-specific budgets; CFTC raw reads enforce both per-source and total bounds
before parsing.

Files are content-addressed canonical JSON. Public reads require the requested
information mode, reject oversized/nonregular/symlink input, and replay sources
and values. Writers verify before publication, fsync a private temporary file,
publish atomically without clobbering a conflicting target, fsync the directory
where supported, and verify existing bytes for idempotence. A correctly re-sealed
but false value, clock, null, origin or parent is not accepted by source replay.
Changing native semantics requires a new versioned contract/adapter definition,
not reinterpretation of retained v1 bytes.

## Synthetic acceptance evidence

The dedicated core/context/market suites execute generated canonical IPC,
committed three-leg publications, seven bar widths, native projection delivery,
calendar vintages, both executable forecast paths, CFTC raw fixtures and actual
broker capture refits. Canaries cover later unavailable values/nulls, strict
empty-grid origin, postverification feature replacement, exact inventory cutoff,
fit-history support, unknown clocks, warmup/absence states and native envelope
tampering. Separate contract and artifact suites exercise namespace/entity/parent
rules, boundaries, complete-source replay with empty projections, correctly
re-sealed mutations, resource limits and atomic/nonregular I/O. These are
controlled source-backed software tests, not an empirical study or authorization
to read historical/protected inputs.
