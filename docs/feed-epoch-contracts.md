# Feed-epoch contracts

Feed epochs describe changes in the technology through which historical tick
data was observed. They are not calendar eras, market regimes, or claims about
missing historical trades. A feed epoch is fitted from canonical time-series
fingerprints and becomes usable by reconstruction only after deterministic
stability checks pass.

This implementation replaces the earlier `sparse`, `transitional`, and `dense`
labels derived from an independent raw-tick scan. The command name
`histdatacom analytics feed-regimes` remains available for compatibility, but
its discovery and evidence now flow through the data-quality target and
fingerprint engine.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `FeedEpochEvidenceV1` | One bounded symbol-period projection of a canonical tick fingerprint, including feature provenance and source/config hashes. |
| `FeedEpochFitConfigV1` | Feature selection, coverage requirements, boundary thresholds, sensitivity policy, and resource limits. |
| `FeedEpochBoundaryV1` | Central boundary, uncertainty interval, change score, perturbation support, and contributing features. |
| `FeedEpochIntervalV1` | One named epoch and its inclusive period bounds. |
| `FeedEpochStabilityV1` | Sampling, missing-period, and feature-removal run counts plus pass/limited/fail status. |
| `FeedEpochDefinitionV1` | Versioned epochs, uncertain transitions, stability result, and complete evidence/config lineage. |
| `FeedEpochAssignmentV1` | Deterministic assignment of a period to an epoch or uncertain transition. |

All readers validate their schema version and deterministic identity. Modified
feature values, source hashes, config values, boundary order, interval order,
or lineage counts fail closed rather than silently producing a different
interpretation under the same ID.

## Canonical evidence only

`FeedEpochEvidenceV1.from_fingerprint()` accepts ASCII tick fingerprints from
the canonical `fingerprint.series` surface. It projects only bounded
observation-regime signals:

- tick rate and inter-arrival cadence;
- observed timestamp interval and price precision;
- spread level and conditioned spread level;
- spread changes, stale quotes, bursts, duplicates, and suspicious gaps;
- source-quality penalties;
- session, holiday, event, and special-window conditioning metadata.

The evidence records both the fingerprint hash and its source hash. Inline
canonical findings use the fingerprint identity as that source hash; a persisted
fingerprint artifact may instead supply its byte-level SHA-256. The explicit
`source_hash_basis` field prevents those two meanings from being conflated. Its
feature-provenance map states the exact fingerprint path used for every value.
Calendar/event coverage and quality limitations are retained even when they are
not themselves fitted as numeric features.

The epoch fitter does not discover paths or scan raw ticks. Public CLI and API
compatibility functions delegate discovery to `discover_quality_targets()`, run
the canonical fingerprint rules, then pass the resulting evidence into the
standalone fitter. This keeps one interpretation of source freshness, sibling
caches, timestamp parsing, session closures, and known quality limitations.

## Panel normalization and boundary fitting

Features are robustly normalized within each symbol before the symbol-period
panel is aggregated by period. This prevents a symbol with a naturally
different quote cadence or spread from creating a false boundary merely by
entering or leaving panel coverage. Eligible features must satisfy configured
period coverage, and the period panel is normalized again before adjacent
multivariate change scores are computed.

Candidate boundaries must:

1. exceed the configured robust change score;
2. leave the configured minimum number of periods on both sides; and
3. remain separated from stronger neighboring candidates.

A boundary is an uncertainty interval, not an exact instant. Its central period
comes from the full fit. Its lower and upper periods include the matched
boundaries recovered by deterministic perturbation runs.

No calendar year is treated as a regime a priori. A stable no-change history is
a valid one-epoch result.

Canonical histories may contain annual artifacts in older years and monthly
artifacts later. The compatibility surface never fabricates monthly evidence
from an annual fingerprint. Instead it deterministically coarsens monthly
fingerprints to annual evidence whenever annual input is present or an annual
bucket is requested. An exact annual fingerprint takes precedence over
overlapping monthly fingerprints for the same symbol-year. Each aggregate
retains the complete component fingerprint and source-hash list; definition
lineage exposes both fitted-evidence and original canonical-source counts.

## Stability and downstream trust

Every full fit reruns the detector under three perturbation families:

- deterministic even/odd period sampling;
- removal of each eligible internal period;
- removal of each eligible feature.

Each boundary records support by family and overall support. The definition
passes only when all required families are available and every boundary meets
the configured support threshold. It is `limited` when the evidence is too
short to exercise all families, and `fail` when an asserted boundary is not
stable.

`FeedEpochDefinitionV1.valid_for_observation_models` is the downstream trust
gate. `assign()` refuses a limited or failed artifact by default. Callers must
make an explicit, visible override to inspect an unstable assignment; such an
override is not suitable for reconstruction or broker-conditioning claims.

A period inside a boundary uncertainty interval is assigned to
`kind="transition"`, not forced into either adjacent epoch. Periods outside
those intervals receive a stable epoch assignment.

## Resource limits and determinism

Evidence count, selected feature count, and total sensitivity runs are bounded
by the fit config. Inputs are sorted and de-duplicated before fitting, every
sensitivity variant is enumerated deterministically, and all IDs are hashes of
canonical JSON payloads. Input order, worker count, and filesystem path order
do not affect the artifact.

The default full-fit contract requires six periods and at least two periods on
each side of a boundary. The compatibility analytics report can describe a
two-period fixture with relaxed segment limits, but its stability is necessarily
`limited` and therefore fails the downstream trust gate.

## CLI and artifacts

Write the compatibility report and the compact epoch definition separately:

```sh
histdatacom analytics feed-regimes \
  --target data/ASCII/T/ \
  --bucket month \
  --report reports/feed-regimes.json \
  --epoch-artifact reports/feed-epochs.v1.json
```

Fit policy can be pinned explicitly for reproducible research:

```sh
histdatacom analytics feed-regimes \
  --target data/ASCII/T/ \
  --features log_tick_rate log_median_interarrival_ms price_precision spread_median \
  --min-evidence-periods 12 \
  --min-segment-periods 3 \
  --min-change-score 0.8 \
  --min-boundary-support 0.75 \
  --max-evidence 4096 \
  --max-sensitivity-runs 256 \
  --epoch-artifact reports/feed-epochs.v1.json
```

The definition artifact contains the complete source/fingerprint/config lineage
needed to replay or reject a result. The larger compatibility report embeds the
same definition alongside period profiles and summary metrics.

## Streaming reconstruction integration

Feed-epoch fitting is a bounded control-plane step, not a per-row augmentation
job. Reconstruction should load and validate one `FeedEpochDefinitionV1` at run
admission, bind its `definition_id` into the semantic reconstruction config,
and assign each owned window or final synthetic event to an epoch or transition
by period.

The streaming data plane therefore carries compact fields such as:

```text
feed_epoch_definition_id
feed_epoch_id
feed_epoch_assignment_kind
feed_epoch_boundary_id       # only for uncertain transitions
```

It does not carry the fingerprint payload, fitting panel, sensitivity matrices,
or the 521-column analytical frame. Those are reproducible intermediates. The
final synthetic tick remains the durable row, while the epoch definition is a
small immutable sidecar referenced by the run manifest and output lineage.

The reconstruction source manifest must record the epoch definition as a
semantic source, not as mutable workflow metadata. Changing the definition or
accepting a new version consequently changes the reconstruction run identity.
Windowing, retries, or storage tuning do not.

## Issue boundaries

- #321–#333 own canonical fingerprints, their persistence/constraints, and the
  evidence source consumed by epoch fitting.
- #433 governs ex-post versus ex-ante information access.
- #435 consumes only stability-passing definitions and implements historical
  feed-observation operators.
- #436 owns reverse-degradation scorecards over the #435 interface.
- #439 consumes only stable epoch artifacts while generating candidates.
- #443 owns live broker delivery capture; #444–#445 own broker fingerprints and
  style transfer rather than historical technological epochs.
- #446 publishes final Parquet and manifest artifacts atomically.
- #447 carries compact artifact references through production streaming
  workflows.

Changing a required field, identity derivation, stability family, transition
meaning, or assignment trust rule requires a new schema version and contract
class.
