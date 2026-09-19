# Real-time vintage feature store

`histdatacom.forecasting` provides a bounded, provider-neutral, offline
information-set consumer for economic forecasting (#561). It assembles actual
macro releases, event consensus, qualified market observations and optional
alternative/classification inputs at an explicit cutoff. It does not fetch a
current database, fill unavailable cells, or infer historical availability
from retrieval time.

The existing calendar-only `ForecastInputsV1`, `ForecastModelIdentityV1`,
`ForecastSnapshotV1`, scoring classes and artifact dispatch from #560 retain
their original schemas and identities. Feature-aware forecasting uses the
separate `ForecastFeature*V1` family. A calendar projection alone is not a
complete feature-aware forecast and is rejected by its consumer/persistence
APIs.

## Information-set semantics

`FeatureObservationV1` retains an immutable reference interval, numerical value
(or explicit source missingness), vintage sequence, exact predecessor identity,
publication time, availability time, point-in-time semantic definition and
normalized source evidence. A valid chain is contiguous, starts at zero, and
does not move backward in availability time. The store selects its last vintage
whose availability is at or before the cutoff, retaining all visible predecessors.
Equal availability times are ordered only by the explicit validated revision
chain, never by an arbitrary filename or input-list order.

`FeatureDefinitionV1` commits unit, multiplier, base, seasonal adjustment,
methodology, frequency, classification and the time these metadata were known.
Metadata cannot become known after the vintage that uses it. A later full-sample
classification must therefore be a new, later-available vintage, not a mutation
to old rows. The feature layer does not estimate retrospective regimes. Different
semantic eras cannot be combined by rolling transforms or the reference model;
there is no implicit rebasing or comparability bridge.

`FeatureScheduleV1` has an independent `known_at_ns`. The selected schedule is
the latest assertion known at cutoff, regardless of the scheduled event clock.
For level cells, `availability_lag_ns` is vintage availability minus that selected
schedule, so it may be negative and is not a claim about publisher latency.
`value_age_ns` is cutoff minus selected vintage availability; `reference_age_ns`
is cutoff minus reference-period end. These are separate quantities: a revision
can be newly available for an old reference period. The optional maximum age
policy applies to value age. A stale cell keeps its original evidence but exposes
no executable value.

Raw revision news is current minus immediately preceding visible value, only
when both have the same semantic identity. It is unavailable before the new
vintage arrives and is absent across semantic breaks. Its two exact
`revision_observation_ids` are separate from transformed-value support IDs.
For example, a lagged value can use last period while raw revision news refers
to the current period. Transformed cells do not copy a misleading level-value
availability lag.

## Ragged-edge grids and transforms

`FeatureRequestV1` fixes cutoff, ordered non-overlapping reference intervals,
named columns, transform specifications and optional staleness thresholds.
Columns and periods are never discovered from the eventual full dataset.
`VintageFeatureStoreV1.snapshot(request)` returns a row-major
`FeatureMatrixSnapshotV1` with explicit available, unavailable, source-missing,
stale, warmup, semantic-break or zero-scale cells. It never backfills from future
vintages or converts source missingness to zero.

The executable transforms are level, positional lag, positional difference,
rolling mean, expanding mean and rolling sample z-score. Windows end at the
current requested reference row; no later row enters a calculation. Missing
positions are not skipped or imputed. Minimum support controls warmup, and
sample z-score uses `ddof=1` with at least two values; zero variance refuses a
numerical result. Expanding means start at the first explicitly requested row.
Lag and difference refer to requested grid positions, not guessed calendar
frequencies; callers must supply the intended complete reference grid.

Every transformed cell binds source observation IDs, transform support count
and the information-set fit cutoff. A matrix is one information set at one
cutoff, **not** a set of historical training-example cutoffs: historical
evaluation must request separate snapshots for separate forecast cutoffs.
Visible revisions of earlier reference periods are legitimate inputs at the
current cutoff, never automatically copied into earlier forecast snapshots.

## Source adapters and qualification boundary

`calendar_feature_records` executes against an `EconomicCalendarCorpusV1`, with
an explicit `(series_key, reference_period)` interval map because arbitrary
provider labels do not establish period starts. It preserves complete release
or survey records. Actual feature identities separate series versions. Only
observed event consensus and official professional event-target surveys enter
consensus features; central-bank projections, machine estimates and unrelated
period forecasts are not relabeled consensus.

Survey metadata must come from an exact release/schedule vintage already
visible by survey availability. An eventual release cannot supply earlier
seasonality, frequency or classification. That metadata release is retained
beside the survey. Multiple logical events/stages sharing one feature reference
period fail closed instead of being stitched into a fictional revision chain.
Optional `EconomicReleaseScheduleEvidenceV1` uses its independent knowledge
clock; it cannot accelerate the release record's own availability.

Generic macro and alternative providers supply the same typed observation and
evidence contracts. Source qualification remains the adapter's responsibility:
the library verifies declared clocks and exact retained content, not the truth
of an external provider's historical clock. Current-revised, ex-post and
synthetic-reconstruction source modes are expressly refused by the ex-ante
store. INSEE current BDM cross-check series are therefore not historical
vintage substitutes.

`observed_bar_feature` is an executable bridge for a `DerivedBarV1` close/spread
field. It requires a complete observed-only bar, exact normalized bar bytes and
identity in a typed receipt, and separately supplied availability no earlier
than the bar end. A final reconstructed bar, synthetic support, partial bar,
or bar-end timestamp alone is insufficient. **This adapter does not replay or
verify a storage publication.** The proof-carrying publication boundary from
#649 is separate; its verified result and independently qualified availability
may feed this adapter. A normalized receipt is not a substitute for that check.

## Executed feature-aware forecasting

The public sequence is:

1. Assemble a `FeatureMatrixSnapshotV1` from the vintage store.
2. Call `capture_forecast_feature_inputs` with fixed calendar event keys and
   coverage bounds. These bounds are explicit request parameters, not copied
   from a mutable global catalog. Per-record provenance remains retained;
   future catalog coverage/limitations cannot change a historical hash.
3. Fit with `train_feature_baseline`, or construct an explicitly identified
   `ForecastFeatureModelV1` for an independently implemented consumer.
4. Execute `forecast_feature_baseline` and receive a
   `ForecastFeatureSnapshotV1` with exact input/model evidence.
5. Construct `ForecastFeatureScoreV1` from that snapshot and normalized outcome
   evidence, then use `write_feature_artifact` / `read_feature_artifact`.

For example, with a qualified `store`, normalized `calendar`, explicit request,
and an existing #560 cutoff/target:

```python
from histdatacom.forecasting import (
    ForecastFeatureScoreV1,
    capture_forecast_feature_inputs,
    forecast_feature_baseline,
    read_feature_artifact,
    train_feature_baseline,
    write_feature_artifact,
)

inputs = capture_forecast_feature_inputs(
    calendar,
    store.snapshot(request),
    calendar_event_keys=(target.logical_event_key,),
    calendar_coverage_start_ns=coverage_start,
    calendar_coverage_end_ns=coverage_end,
)
model = train_feature_baseline(
    inputs, column="inflation", trained_at_ns=cutoff.cutoff_at_ns
)
forecast = forecast_feature_baseline(
    model, inputs, cutoff=cutoff, target=target,
    generated_at_ns=cutoff.cutoff_at_ns,
)
score = ForecastFeatureScoreV1(forecast, outcomes.to_json(), scored_at_ns)
restored = read_feature_artifact(write_feature_artifact(score, artifact_dir))
forecast.verify_against(calendar, store)
```

The reference consumer is intentionally simple: fit the mean of the selected
column's visible training cells, then predict half that training mean plus half
the last inference cell. It refuses missing/stale/warmup cells, mixed semantics,
changed transform specifications and incompatible target unit/scale/base.
Its state is recomputed before prediction, its algorithm-spec digest is distinct
from the SHA-256 of the actual installed implementation module bytes, and changed
executable identity fails fit replay. It demonstrates execution and evidence
binding; it is not a qualified model-bank engine, trained release model,
profitability claim or holdout-validation result.

Training time cannot precede input cutoff; inference evidence must be available
by actual generation time, not merely by a later nominal cutoff. Overlapping
training/inference feature-period grids must have exactly equal historical
prefixes through the training cutoff, including explicit missingness and
schedule knowledge. Later genuine vintages are allowed. Training periods may
be absent from the inference grid; their full evidence remains embedded in the
model and therefore in the sealed forecast information set.

All #560 horizon, latest-known-schedule, first-actual, consensus, revision,
surprise and normalization rules are reused through a private calendar semantics
adapter. It neither inserts macro/market values into calendar events nor hides
them in arbitrary model state. Full feature snapshots and score envelopes bind
that projection identity and recompute it on restore; equal calendar projections
do not imply equal feature forecasts. For predicted-consensus surprise, the
reference identity exposed by feature scores is the full feature forecast ID.

## Replay, bounds and persistence

Snapshot identity contains the fixed request, selected normalized evidence and
recomputed cells, never a global store ID. Adding later releases, surveys,
revisions, market observations or classifications leaves earlier snapshots
byte-identical. Replay against a store recomputes the historical selection;
missing, changed, or newly backdated records fail rather than silently updating
old evaluation evidence. Replay requires the relevant exact vintage history,
not merely an event's final revised value.

The source inventory is immutable after construction; append by constructing
a new store. Its local LRU is synchronized, capped by both entries and UTF-8
bytes, and never shared across changed inventories. Limits are 50,000 retained
source records, 32 MiB aggregate normalized source bytes, 256 columns, 10,000
requested periods and 20,000 cells per request. Cache bounds are at most 128
entries and 32 MiB (defaults: eight entries / eight MiB; zero entries disables
caching). These are local safety limits, not advertised throughput targets.
Expanded transform/matrix lineage is capped at 200,000 source references to
bound rolling/expanding support amplification before JSON construction.

All accepted sealed artifacts share #560's final-envelope limit of 32 MiB and
64 levels of expanded JSON nesting, including nested model/training/feature
evidence. A legal inner object can be refused when a larger composed envelope
exceeds those limits. Opaque normalized source JSON is decoded into the expanded
evidence tree before sealing, so encoding it as a string cannot bypass bounds.
Restoration rejects duplicate keys, changed derived cells/metrics, wrong nested
identities and noncanonical artifact bytes.

Persistence writes a same-directory temporary file, flushes and fsyncs it,
then atomically publishes by a no-clobber hard link. Existing conflicting bytes
are never overwritten. Reads are bounded and require a regular file with
nonblocking/no-follow final-component checks and an inode check; directories,
symlinks and FIFOs are rejected. Temporary files are cleaned after success or
failure. There is no promise of cross-filesystem atomic publication or a remote
object-store protocol.
