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

## Active-time multivariate v2 fit

`histdatacom analytics feed-epochs-v2` is the production research path for
issue #460. It does not reinterpret the v1 schema. It reads the real monthly
ASCII tick Arrow caches directly and writes three bounded artifacts: source
evidence, the compact epoch definition, and campaign/runtime metadata.

Each monthly observation states three different denominators:

- full UTC calendar-month duration;
- duration in the shared fixed-EST FX market-open policy, including the
  labelled Friday-close and Sunday-open windows but excluding weekend closure;
- observed active time, defined as the sum of positive market-open
  inter-arrivals at or below the configured active-gap cap.

Rates never silently substitute one denominator or numerator for another. The
calendar rate uses all rows, the market-open rate uses only market-open rows,
and the active-window rate uses qualifying intervals divided by their summed
duration. Both filtered numerators are retained as evidence counts. The v2
evidence also records bid-only, ask-only, joint, and unchanged transitions;
hourly-count Fano dispersion; inter-arrival dispersion and lag; timestamp
quantization plus bounded last-digit counts; price precision; duplicate, burst,
and stale rates; exact stale-run p95 and maximum lengths; spread tails and
jumps; normalized activity over the shared overlapping session windows and
source-calendar weekdays; and cross-symbol hourly activity correlation and
overlap. Hourly synchronization is explicitly a bounded activity proxy, not a
claim that quote identities or missing ticks are shared.

The shared detector robustly scales features within each symbol, takes the
cross-symbol median for each common month, and applies multivariate PELT with a
versioned winsorized squared-error cost, penalty, and minimum segment length.
Separate per-symbol fits are retained as deviations rather than folded into the
global epochs. Boundary support and uncertainty come from penalty variants,
leave-one-symbol and leave-one-feature runs, alternate denominator-feature
exclusions, and duplicate-feature exclusion.

PELT candidates do not automatically become epochs. A candidate must meet the
support threshold overall and within every available sensitivity family.
Candidates that fail a family are retained in `rejected_candidates` with their
family support table, but they are excluded from the intervals exposed to
downstream code.

The definition is admitted to observation-operator fitting only when the
configured symbol, common-period, feature-coverage, and boundary-sensitivity
requirements pass. Failed or unsupported definitions remain inspectable but
are not valid reconstruction inputs.

```sh
histdatacom analytics feed-epochs-v2 \
  --target data/ASCII/T/eurusd data/ASCII/T/gbpusd data/ASCII/T/eurgbp \
  --artifact-dir data/.histdatacom/feed-epochs-v2 \
  --json
```

This analysis describes changes in the *observation technology represented by
these source caches*. It does not identify market regimes, recover unobserved
historical quotes, prove a causal vendor change, or supply broker adaptation.

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

The v1 epoch fitter does not discover paths or scan raw ticks. Public v1 CLI and API
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
- #436 implements reverse-degradation scorecards over the #435 interface; see
  [`reverse-degradation-benchmark-contracts.md`](reverse-degradation-benchmark-contracts.md).
- #439 consumes only stable epoch artifacts while generating candidates.
- #443's implemented
  [live broker delivery capture](broker-capture-contracts.md) supplies qualified
  wall/monotonic delivery evidence; #444–#445 own broker fingerprints and style
  transfer rather than historical technological epochs.
- #446 implements atomic final Parquet and manifest publication.
- #447's implemented Temporal control plane carries compact artifact
  references through production streaming; see
  [`reconstruction-temporal-orchestration.md`](reconstruction-temporal-orchestration.md).
  workflows.

Changing a required field, identity derivation, stability family, transition
meaning, or assignment trust rule requires a new schema version and contract
class.
