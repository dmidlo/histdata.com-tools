# Broker delivery fingerprint contracts

`histdatacom.broker_capture` fits qualified live-capture evidence into a compact,
immutable description of one broker observation/delivery system. The artifact
does not copy augmented tick rows, assert market-wide truth, reconstruct history,
select a model winner, or apply broker style to a synthetic stream. Application
belongs to #445.

## Streaming and storage boundary

Statistical aggregation uses two bounded streaming passes over capture JSONL:

1. `assess_broker_capture_eligibility()` verifies manifest state, partial/orphan
   inspection, partition sidecars, bytes, hashes, row contracts, sequence,
   clock health, minimum event support, adapter policy, and collector version.
2. `fit_broker_delivery_fingerprint()` replays the same immutable content through
   bounded aggregators and requires the second logical-content SHA-256 to equal
   the health-pass hash.

These statistical aggregators do not materialize tick rows or write augmented
capture rows. They retain distribution support, sums, squared sums, extrema, and
bounded deterministic bottom-hash samples in memory.

Fitting additionally performs host-health, provenance, and current-rights
replays. Provenance verification retains the bounded journal evidence, including
native payload entries, in memory. The two statistical passes therefore do not
bound the total number of source reads or imply constant-memory provenance
verification. Fresh scientific material-use checks can replay sources again.

Input event, capture, journal, cell, sample, context, and comparison limits fail
closed instead of truncating the evidence. The downstream reconstruction
pipeline receives a compact profile and exact capture-root inventory rather
than another augmented tick dataset.

Cadence uses session-local monotonic receive time. Separate capture sessions are
never bridged, because monotonic clock origins are process-local. Calendar and
market-event conditioning use UTC receive time. Exact source price lexemes—not
binary-float rendering—supply decimal-place and trailing-zero behavior.

## Eligibility contract

`BrokerCaptureEligibilityV1` records a deterministic decision for every capture:

- `eligible`: clean, complete, verified, supported, and without limitations;
- `limited`: fit is allowed but nonfatal clock or manifest limitations remain;
- `ineligible`: fitting is refused with bounded reason codes.

Hard failures include incomplete capture, unsupported adapter/collector policy,
fatal manifest limitations, partial/orphan evidence, integrity failure,
insufficient events or quotes, excessive clock corrections, excessive correction
magnitude, and unexplained UTC regression. A verified decision binds the capture
manifest ID, fit-config ID, event/quote counts, clock findings, UTC support, and
logical event-content SHA-256.

Unreleased v3 additionally requires independently replayed, measured
[host-health evidence](broker-host-health.md) meeting its declared SLOs. Missing,
degraded or insufficient evidence refuses a new fit; historical V1 inspection is
still available. Successful fitting writes a separate immutable health
qualification under the explicit capture root, so that root must be writable.
The proof links the fingerprint bytes to the complete native capture, audit and
health-policy inventory. New fitting returns `BrokerDeliveryFingerprintV2`,
which composes unchanged V1 statistics with every exact native capture manifest,
output contract, provenance header and final seal. Its outer identity binds the
entire root inventory. Pure numerical comparison is not health certification,
and deserializing a proof does not reauthorize material use.

New broker-conditioned consumers require V2 and independently replay the actual
retained captures against those expected roots at material boundaries. Supply
explicit source locations with `broker_fingerprint_sources(*roots)` and current
provider rights separately. Missing, ambiguous, partial or tampered sources
refuse. Historical V1 readers remain available but cannot admit new science.
Retained downstream products carry the complete fingerprint-parent lineage.
See [host-owned provenance](broker-plugin-provenance.md) for anchoring, resource
bounds and the distinction between integrity and provider truth. SDK-native
captures are verifiable but are not silently coerced into this legacy fitter.

Every fitted profile also retains `BrokerDeliveryCaptureEvidenceV1` per input
session: manifest and eligibility IDs, logical content hash, a digest over the
ordered partition IDs and artifact hashes, partition/event counts, and wall-time
support. The top-level identity binds adapter ID/version/config hash, protocol,
environment, server, hashed account, collector ID/version, and the complete fit
policy.

## Condition cells and explicit support

`BrokerDeliveryFingerprintV1.cells` always includes a global cell and may include
these deterministic dimensions:

| Dimension | Evidence source |
| --- | --- |
| `symbol` | capture quote |
| `session` | canonical calendar classifier |
| `overlap` | canonical session overlaps |
| `special` | rollover/fix/open/close calendar tags |
| `holiday` | versioned calendar holiday tags |
| `event` | calendar event tags and optional versioned market-context timeline |
| `lifecycle` | bounded post-reconnect, post-outage, and post-restart quote windows |

The fitter emits both context-only and symbol-plus-context cells. Every cell has
an observed quote count and one of three states:

- `supported`: its own support meets `min_cell_support`;
- `backed_off`: its own evidence remains visible, but `effective_condition_id`
  selects the first qualified parent in the declared ordered chain;
- `unsupported`: neither the cell nor a declared parent has enough support.

A symbol-plus-context cell backs off to symbol, then context, then global. A
single-dimension cell backs off to global. Backoff order is identity-bearing and
must not be sorted or inferred by a downstream consumer.

## Fitted behavior

Each metric carries its own observation support, retained sample count, estimate,
uncertainty interval, extrema, configured quantiles, units, and limitations.
Version one fits:

- event and quote inter-arrival cadence plus per-session intensity;
- bid/ask spread and signed/absolute spread changes;
- burst, quiet, unchanged/stale, transition, and exact-duplicate rates;
- source timestamp precision and exact-lexeme price precision/trailing zeros;
- contiguous source-batch quote counts;
- known gap/outage duration and absolute clock-correction magnitude;
- event rates for quote, reconnect, gap, outage, process restart, and clock
  correction;
- selected quote/lifecycle event-transition rates;
- the same quote behavior under the supported condition cells above.

Means use all observed finite values. Quantiles use the deterministic bounded
sample and declare that limitation when sampled. Rate bounds use Wilson
intervals; distribution means use a normal mean interval. These intervals are
bounded diagnostics, not a claim of independent identically distributed ticks.

## Drift comparison

`compare_broker_delivery_fingerprints()` compares the union of explicit
condition/metric rows. It reports `stable`, `sampling_noise`, `material_drift`,
or `unsupported` using minimum support, combined uncertainty, and configured
relative plus metric-specific absolute effect thresholds.

Material rows are retained first when `max_comparisons` bounds the output. The
artifact records the full candidate count and whether rows were truncated.
Status counts reconcile only the retained rows. There is deliberately no global
similarity score, automatic winner, or collapse of session/event strata into one
number. Cadence, spread, timestamp precision, price precision, stale/burst
behavior, reconnect/outage behavior, and their conditional cells remain
inspectable separately.

## Versioning, supersession, and persistence

A profile ID is derived from the complete canonical payload. A successor must
match the predecessor's broker/adapter/collector identity, begin later, and
record `supersedes_fingerprint_id`. It creates a new artifact; the predecessor
is never edited. Existing `SyntheticEventV1.broker_profile_id` lineage therefore
continues to resolve to the original profile even after drift causes a successor
to become effective.

`write_broker_delivery_fingerprint()` atomically publishes canonical JSON and
returns an `ArtifactRef` with byte size and SHA-256. Rewriting identical bytes is
idempotent. Different content at the same path is refused. Loading reconstructs
all contracts and rechecks every derived identity.

## Broker-transfer handoff

`histdatacom.synthetic.broker_transfer` selects only a profile whose effective
interval, capture lineage, eligibility, support state, and broker identity
satisfy its reconstruction run. For a supported cell it uses that cell; for
`backed_off` it follows the recorded `effective_condition_id`; for `unsupported`
it refuses that conditioned claim.

The selected fingerprint ID must remain on generated-event lineage and the
reconstruction manifest. Proposal conditioning and final delivery rendering may
read the profile, but they may not reinterpret capture clock semantics, invent
support for sparse cells, mutate the profile, overwrite prior manifests, or
treat this delivery fingerprint as a historical price-path generator. See
[`broker-delivery-transfer-contracts.md`](broker-delivery-transfer-contracts.md)
for the implemented selection, rendering, validation, benchmark, and streaming
boundaries.
