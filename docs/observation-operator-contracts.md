# Historical feed-observation operators

Historical feed-observation operators describe how an underlying market-event
surface becomes the sparse, quantized, filtered feed preserved by a
technological epoch. They model delivery technology, not market-price
dynamics, candidate generation, or broker-specific style.

The operator layer consumes stability-passing v1 or active-time v2
[`feed-epoch definitions`](feed-epoch-contracts.md) and bounded,
provenance-bearing fit evidence. It never persists the 521-column analytical
frame.

## Identity boundary

`SyntheticEventV1` remains the immutable market-event identity. Quantizing its
timestamp or price in place would silently create a different event while
retaining misleading generator/source lineage. Version one therefore uses a
separate observation boundary:

```text
SyntheticEventV1 or another market event
  -> ObservationInputEventV1
  -> ObservationOperatorV1.apply/degrade
  -> ObservationOutputEventV1
```

An output observation references its `source_event_id`, operator, and resolved
stratum. Its deterministic identity includes the delivered timestamp,
sequence, bid/ask, duplicate ordinal, and transformation labels. Market-event
generation and delivery observation remain independently auditable.

## Contract surface

| Contract | Responsibility |
| --- | --- |
| `ObservationContextV1` | Symbol, epoch/transition, state, session, and event conditioning coordinates. |
| `ObservationFitEvidenceV1` | Bounded parameter values, uncertainty, support, basis, provenance, period, and source hash. |
| `ObservationParameterEstimateV1` | One fitted value with combined support, uncertainty, estimation bases, and evidence IDs. |
| `ObservationOperatorFitConfigV1` | Support gates, fallback order, row/stratum limits, halo requirements, and diagnostic bounds. |
| `ObservationStratumV1` | One conditioning stratum, fitted parameters, support status, and explicit fallback keys. |
| `ObservationFitDiagnosticsV1` | Bounded support/residual summaries and sampled stratum diagnostics. |
| `ObservationOperatorV1` | Versioned, replayable operator artifact with complete epoch/config/source lineage. |
| `ObservationInputEventV1` | Market-event values plus observation-only context and optional protected-anchor status. |
| `ObservationOutputEventV1` | Operator-lineaged delivery observation without mutating the market event. |
| `ObservationCarryStateV1` | Prior delivered quote, rate bucket, outage state, and watermarks needed across windows. |
| `ObservationApplicationResultV1` | Bounded in-memory outputs, reason/fallback counts, samples, and next carry state. |

All readers validate their schema version and recompute deterministic IDs.
Changed parameters, uncertainty, evidence, fallback order, lineage, output
values, or carry state cannot retain an earlier ID.

## Base parameter family

The version-one family exposes these fixed parameter names:

| Parameter | Delivery behavior |
| --- | --- |
| `retention_probability` | General deterministic thinning. |
| `unchanged_retention_probability` | Filtering of unchanged delivered quotes. |
| `timestamp_quantum_ns` | Timestamp precision/quantization. |
| `price_precision_digits` | Bid/ask rounding precision. |
| `quote_transition_threshold` | Suppression of sub-threshold quote transitions. |
| `batch_window_ns` | Delivery batching into aligned time buckets. |
| `duplicate_probability` | Duplicate delivery of retained events. |
| `rate_cap_per_second` | Per-burst-window output cap. |
| `burst_window_ns` | Rate-cap/burst-compression bucket. |
| `quiet_gap_probability` | Deterministic outage-bucket selection. |
| `outage_window_ns` | Quiet-gap/outage bucket duration. |
| `reconnect_duplicate_probability` | Duplicate behavior on the first retained event after an outage. |

Parameter support is independent. An unsupported parameter uses a recorded
neutral identity value: retain, do not quantize, do not duplicate, and do not
invent outages. This conservative behavior is visible in diagnostics and does
not turn an unavailable thinning estimate into a claimed observation.

## Canonical versus paired evidence

`ObservationFitEvidenceV1.from_feed_epoch_evidence()` projects the bounded
canonical evidence created by the feed-epoch layer. That surface supports
descriptive proxies for cadence, timestamp/price precision, stale quotes,
duplicates, burst windows, and quiet gaps.

Historical sparse data does not contain the unobserved dense-event
denominator needed to identify a true thinning probability. Canonical
projection therefore records `retention_probability=1` with zero support,
the basis `identity_without_dense_denominator`, and uncertainty spanning the
admissible probability range. It cannot masquerade as a directly measured
retention rate.

Paired calibration or controlled-degradation evidence may supply supported
input/output parameters. Every parameter still carries its own:

- support count;
- lower and upper uncertainty bounds;
- estimation basis;
- exact evidence IDs; and
- bounded feature/source provenance.

For marked-Hawkes reconstruction, the supported point estimate and interval
are consumed as three distinct operator scenarios rather than collapsed into
seed-only dispersion. See [Observation-process uncertainty
propagation](observation-process-uncertainty.md) for endpoint identity,
worst-case admission, holdout calibration, and legacy replay rules.

The implemented
[`reverse-degradation-benchmark-contracts.md`](reverse-degradation-benchmark-contracts.md)
layer owns the full experiment, splits, controls, and scorecards. This module
supplies the deterministic operator interface and controlled fixtures exercised
by that benchmark.

## Real-evidence calibration v2

`ObservationCalibrationCampaignV2` makes a stricter claim than a bare v1
operator. It uses the last stable technology epoch as the dense reference,
estimates earlier symbol/epoch targets relative to its active-time denominator,
and evaluates the fitted operator on chronologically blocked calibration,
validation, and final-holdout windows. Split periods must be distinct and span
three years. Dense and degraded rows stay process-local; only bounded aggregate
window evidence is persisted.

The calibration surface conditions retention by update type and session. It
records timestamp precision and quote-step support directly, while spread,
activity, and volatility conditioning remain explicitly unsupported until
conditional epoch denominators exist. Batching versus timestamp quantization,
and rate cap versus retention, are recorded as bounded confounded mechanisms.
Archive gaps, outage duration, and reconnect identity remain unsupported rather
than being inferred from silence.

Every target includes all twelve parameter names with a value, interval,
support status, estimation/refusal reason, source evidence IDs, hashes, and
distinct mechanism diagnostics. The nested `ObservationOperatorV1` contains
only supported parameters; its conservative neutral behavior does not promote
an unsupported mechanism into a calibration claim.

Application readiness is enforced by
`ObservationCalibrationCampaignV2.require_application_ready()`. The default
required mechanisms include retention, unchanged filtering, timestamp and
price precision, duplicates, and burst cadence. A caller requesting a bounded
or unsupported mechanism is rejected. A retention estimate whose only basis is
`identity_without_dense_denominator` is always rejected, even if an older bare
v1 operator can still replay it.

Use the artifacts emitted by `feed-epochs-v2` directly:

```sh
histdatacom analytics observation-calibrate-v2 \
  --definition data/.histdatacom/feed-epochs-v2/feed-epochs-v2-definition.json \
  --evidence data/.histdatacom/feed-epochs-v2/feed-epochs-v2-evidence.json \
  --artifact-dir data/.histdatacom/observation-calibration-v2
```

The command verifies every selected dense cache's byte size and SHA-256 against
the epoch evidence, then writes separate campaign, operator, and fit-evidence
JSON files. Per-window event count and per-source byte limits are explicit in
the profile and campaign resource evidence. Campaign wall-clock and peak-memory
ceilings are also hard readiness gates; the measured runtime and process peak
are persisted beside those limits.
It exits with status 2 when a final holdout or readiness gate fails, while
retaining the failed evidence for audit. Existing v1 operator artifacts remain
readable; v2 feed-definition identities are accepted without rewriting their
schema or IDs.

## Explicit conditioning and fallback

Fit evidence is aggregated through this fixed hierarchy:

```text
symbol + epoch + state + session + event
  -> symbol + epoch + state + session
  -> symbol + epoch + state
  -> symbol + epoch
  -> epoch
  -> global
```

Each stratum is `ready`, `limited`, or `unsupported` according to the versioned
support gates. Unsupported specific strata are retained as evidence and name
their fallback keys. Application walks the same hierarchy and fails when no
usable parent exists. Sparse strata are never silently treated as if their
specific parameters were well supported.

Technological epochs and market states remain separate coordinates. A feed
epoch cannot be relabeled as a volatility regime, and an uncertain epoch
boundary remains an explicit transition label supplied by the epoch artifact.
The [feed-epoch transition policy](feed-epoch-transition-uncertainty.md) keeps
that boundary-shape uncertainty separate from the operator's fitted retention
interval and requires the full crossed scenario identity in new marked-Hawkes
products.

## Forward application and controlled degradation

`ObservationOperatorV1.apply()` is the forward reconstruction interface.
Protected historical anchors are retained with their exact source timestamp,
bid, and ask. Ex-ante mode rejects protected anchors because they would expose
future historical evidence.

`ObservationOperatorV1.degrade()` is the generator-neutral benchmark
interface. It ignores input anchor flags and protects only explicitly named
event IDs, allowing modern holdout events to be degraded without pretending
they are immutable historical anchors.

Both methods:

- sort input independently of caller order;
- select thinning/outage/duplicate decisions by stable hashes, not mutable RNG
  state;
- retain one source event ID for every output observation;
- validate positive bid/ask domains and `ask >= bid`;
- assign deterministic within-timestamp delivery sequences;
- emit bounded reason, fallback, and sample diagnostics; and
- enforce input/output amplification limits before returning work.

The operator never changes a market price path to improve fit. It may retain,
filter, quantize, batch, duplicate, or suppress delivery observations only
through its declared parameters.

## Streaming windows, alignment, and carry

Application accepts an existing `ReconstructionWindowV1`. Only source events
owned by `[core_start_ns, core_end_ns)` can emit output for that call. Halo rows
may seed state but do not acquire output ownership.

Timestamp and batch quantization use Unix-epoch-aligned floor buckets. Window
core boundaries must be divisible by every active timestamp/batch quantum so
an event cannot be shifted into another window. Misaligned work fails before
application.

The artifact declares `required_left_halo_ns`, whether carry is required after
the first window, and the exact carry fields. The bounded carry state records:

```text
last_source_time_ns last_observed_time_ns last_bid last_ask
rate_bucket_start_ns rate_bucket_count
outage_bucket_start_ns outage_active reconnect_pending
```

The first source window may start without carry. Version one requires every
later window to provide the matching operator/symbol carry artifact; a finite
time halo cannot prove that it contains the last delivered quote across an
arbitrarily long historical gap. The declared halo remains explicit contract
metadata for operator inputs, but it is not allowed to masquerade as complete
state. Partition count, caller input order, and retry count do not participate
in observation decisions.

## Resource bounds

Version one admits at most 4,096 fit-evidence records, 512 strata, and 250,000
input events per application. A source event can produce at most three output
observations, and diagnostic samples stop at 128 records. Operator artifacts
are rejected above 64 MiB before JSON parsing. Timestamp and batch quanta are
bounded to one day; burst and outage durations are bounded to 31 days. Fit
configuration may lower these ceilings but cannot raise them.

`ObservationApplicationResultV1` contains events and is an in-memory data-plane
object. It must not be placed into Temporal workflow history. Period-scale
workers write event batches and larger carry payloads outside history, then
exchange the existing compact `ArtifactRef` contracts.

## Artifacts and replay

`write_observation_operator()` writes canonical JSON and returns an
`ArtifactRef` with byte size, SHA-256, schema version, operator ID, and epoch
definition ID. `read_observation_operator_artifact()` verifies size, bytes,
metadata, every nested deterministic ID, and the internal lineage hash before
returning an operator.

Operator lineage includes:

- the exact feed-epoch definition ID;
- fit-config ID;
- every fit-evidence ID;
- every source artifact hash and its basis; and
- the complete bounded source list used by the fit.

The operator can therefore be replayed from its artifact and manifest hashes
without the original in-memory fingerprint panel or enriched frame.

The durable reconstruction manifest binds the operator ID as a semantic
configuration input. Observation outputs and fitting panels remain
reconstructable intermediates; the final accepted synthetic tick and compact
operator sidecar are the intended durable products.

## Issue boundaries

- #434 supplies stability-passing technological epochs and canonical evidence.
- #435 owns the contracts and deterministic fit/apply/degrade implementation
  documented here.
- #436 implements reverse-degradation splits, controls, metrics, and promotion
  scorecards over this operator.
- #439 owns variable-cardinality market-event proposal, not delivery filtering.
- #443–#445 own live broker capture, broker fingerprints, and broker-style
  transfer.
- #446 implements final atomic Parquet/manifests; #447's implemented control
  plane owns production Temporal orchestration; see
  [`reconstruction-temporal-orchestration.md`](reconstruction-temporal-orchestration.md).
  orchestration.

Changing a required field, parameter meaning, fallback level, anchor policy,
output identity, carry field, or application decision requires a new schema
version and contract class.
