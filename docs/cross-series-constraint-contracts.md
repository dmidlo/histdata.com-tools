# Synchronized cross-series constraint contracts

`histdatacom.cross_series_constraints` turns the established #331
cross-series fingerprint into bounded, point-in-time evidence that the
first-party reconstruction runtime can actually consume. The contracts are
provider-neutral; the only executable v1 compiler reads immutable
HistData.com ASCII tick (`ASCII/T`) events for the complete
EURGBP/EURUSD/GBPUSD triangle.

> **Current milestone boundary:** HistData.com is the only admitted dataset
> provider. OANDA, other historical providers, live broker feeds, and
> broker-specific adaptations require separately implemented and qualified
> adapters in a later milestone. The generic contracts are the future seam;
> they do not make those providers executable now.

## Evidence and execution are different roles

The constraint layer describes what synchronized evidence exists before a
candidate is generated. It does not project or repair quotes. The existing
cross-currency reconciliation layer remains the only stage allowed to alter an
eligible synthetic quote, and it still operates only at exact nanosecond event
times. Observed anchors remain immutable in both layers.

`CrossSeriesSourceBindingV1` strongly binds every member to provider, dataset
version, symbol, month, series ID, source partition ID, source artifact ID, and
source SHA-256. `CrossSeriesMemberEvidenceV1` then binds event and quote content,
coverage, row count, unique timestamps, and duplicate-timestamp count. A
timestamp is never treated as durable row identity; event ID, sequence, and
source-row identity remain part of every alignment digest.

## Window contract

One `CrossSeriesConstraintWindowV1` records:

- synchronization-unit, reconstruction-window, relationship, member, and
  period IDs;
- the actual half-open alignment-support interval;
- availability, as-of time, and information mode;
- explicit alignment policy, support/probe counts, age bounds, unmatched
  counts, duplicate-safe alignment digest, and one supported recommended event
  time;
- #331 fingerprint schema and content hashes;
- bounded relative-residual severity and distribution evidence where algebraic
  comparisons exist; and
- readiness, eligible uses, excluded uses, and limitations.

The compiler emits five relationship views per observed month:

| Kind | Meaning |
| --- | --- |
| `triangle` | EURUSD / GBPUSD versus EURGBP, using exact event-sequence support when sufficient and otherwise a bounded nearest-prior diagnostic. |
| `inverse` | Available inverse-pair consistency, or explicit unavailability for the current triangle. |
| `timestamp_grid` | Exact common timestamp support and sparse-grid status. |
| `range_overlap` | Common member coverage and unequal-range status. |
| `stale_alignment` | Diagnostic exposure to a configured nearest-prior age; never a forward-fill instruction. |

Exact duplicate timestamps are paired by deterministic ordinal after complete
event ordering. Excess duplicates remain unmatched evidence. The bounded
nearest-prior view hashes the exact member event IDs and ages for each accepted
probe. It is diagnostic only: `forward_fill=false`, timestamp-only joins are
forbidden, and no aligned tick rows are embedded in the artifact.

## Point-in-time and readiness rules

`ex_post_reconstruction` may use the completed core window. In
`ex_ante_simulation`, events later than `as_of_ns` are withheld before any
fingerprint, residual, count, or alignment is computed. A window is visible to
a stage only after both its availability and as-of boundary. Exact maximum-age
comparisons use an inclusive configured boundary; an event one nanosecond past
that boundary is unsupported.

Readiness is explicit:

- `ready` has sufficient synchronized support with no policy violation;
- `limited` remains usable with recorded sparse, stale, unequal-range, or
  warning evidence;
- `contradictory` retains anomaly evidence but cannot condition production;
- `insufficient` and `unavailable` expose missing support rather than
  inventing it; and
- `excluded` is reserved for a declared non-use.

Contradictory or incomplete critical relationships are eligible for anomaly
labeling and excluded from normal training. Proposal, carving,
cross-series reconciliation, and validation fail closed. Negative-control
tests verify that contradictory observed quotes remain byte-for-byte unchanged.

## First-party runtime flow

The public `ReconstructionPlanSpecV1` carries a
`CrossSeriesConstraintPolicyV1`. Planning writes the content-addressed policy,
adds its ID to run configuration identity, and declares it in the execution
and information manifests. The current compatibility boundary accepts exactly
`supported_provider_ids=["histdata.com"]` and the complete three-symbol
triangle.

At runtime:

1. source enrichment compiles one bounded bundle from immutable core-window
   HistData events and writes it behind a strong artifact reference;
2. proposal selects the bundle's explicit supported synchronization instant,
   records the exact constraint-window ID in its manifest and every candidate
   ledger row, and refuses unsupported or contradictory evidence;
3. carving records the constraint-use decision in every carved ledger row;
4. reconciliation and delivery propagate bundle, window, and decision IDs;
5. validation re-evaluates use at its declared time and stores the validation
   decision in the staging descriptor; and
6. the committed delivery-quality manifest content-binds all bundle, window,
   and decision IDs.

The sidecar is capped by policy, stores no full tick rows, and uses stable
content-derived IDs throughout. The real closure gate runs the complete
HistData source-to-commit path, verifies durable lineage, repeats it under a
different concurrency setting, and requires identical logical and physical
publication identity.

## Relationship to #331 and reconciliation

`HistDataCrossSeriesFingerprintRule.evaluate_series()` is the single
in-memory adapter to #331's authoritative descriptive statistics. It adds a
fixed, bounded streaming histogram for triangle and inverse relative
differences so constraint artifacts can report support and severity without
retaining unbounded residual samples.

The constraint bundle decides whether synchronized evidence is usable and
which supported instant proposal consumes. It never replaces the native
nanosecond reconciliation report. Reconciliation still forbids interpolation
and forward filling, preserves every observed anchor, projects only eligible
synthetic events, and reruns final validation before publication.

Changing provider admission, identity semantics, alignment policy, support
boundary meaning, readiness/use rules, or observed-anchor mutability requires a
new schema version and a separately qualified adapter.
