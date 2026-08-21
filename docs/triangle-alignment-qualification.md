# Triangle alignment qualification

`histdatacom.synthetic.alignment_qualification` is the release gate for the
planner's exact-event-sequence and bounded nearest-prior triangle support. It
does not replace planning or reconciliation. It independently expands their
compact evidence into a complete, candidate-bound scientific audit before the
full reconstruction campaign may run.

## Alignment semantics

For probe time `t` and triangle symbol `j`, the bounded treatment selects the
latest source event at or before the probe:

\[
t_j^-(t)=\max\{s\in\mathcal T_j:s\le t\},\qquad
a_j(t)=t-t_j^-(t).
\]

A tuple is supported only when every leg exists and
`0 <= a_j(t) <= maximum_age_ns`. The bound is inclusive. No future event,
retimestamping, interpolation, or silent widening is representable by the
tuple contract. Duplicate timestamps remain distinct through event sequence
and content-derived event identity.

The qualifier enumerates every possible probe leg. Selection still uses the
planner's deterministic rule—maximum supported probes, then fewer source
probes, then symbol—but every alternative count is retained. Maximizing
coverage is evidence, never the sole qualification criterion.

## Complete-range coverage census

Every contiguous half-open candidate window receives exactly one source state
and one support class:

| Source/support class | Meaning |
| --- | --- |
| `exact` | Minimum exact event-sequence support is present. |
| `bounded_prior_only` | Exact support is insufficient, but the bounded policy passes. |
| `unsupported_complete` | All three source legs exist, but neither treatment has enough support. |
| `incomplete_source` | A one- or two-leg outage; terminally unavailable and never infilled through. |
| `empty` | Verified source-empty interval. |
| `expected_closure` | Declared closure with no fabricated liquidity. |

`TriangleSupportCensusV1` reports each category by window and duration, exact
support, total and genuinely stale bounded support, selected probe-leg counts,
all alternative-probe counts, and the fraction of complete-window coverage
created only by bounded alignment.

Every selected tuple embeds source event IDs, immutable row-content SHA-256
values, original event times, per-leg ages, the selected probe event, and the
configured ceiling. The window evidence publishes separate tuple and selected
event digests, so a one-nanosecond or one-row change changes identity.

## Quote-age evidence

Nearest-rank p0, p50, p90, p95, p99, and p100 ages are emitted for exact and
bounded-only support by:

- symbol and probe leg;
- year and feed epoch;
- session/overlap;
- event state; and
- activity/volatility stratum.

The slices bind the exact contributing tuple IDs. Exact slices retain zero
ages; bounded slices expose both zero-age probe observations and stale legs.

## Sensitivity and residual relation

`TriangleAlignmentOutcomeV1` records the six required treatment metrics:

- synthetic count;
- path variation;
- mark-transition distance;
- triangle residual;
- projection burden; and
- downstream sensitivity.

On exact-support windows, otherwise-identical semantic member, scenario, and
experiment identities must contain both exact and bounded outcomes. On
bounded-only windows, validation-only outcomes must cover at least three
predeclared ceilings, including the release maximum. Derived comparisons apply
hard or advisory absolute-and-relative tolerances; final residual quality
cannot hide excessive projection burden.

`TriangleAlignmentResidualBinV1` separately publishes observed-only residual,
synthetic post-projection residual, and projection burden as functions of
maximum tuple age. Observed-only residual remains immutable source-quality
evidence and does not authorize source mutation. Synthetic-involved residual
and burden are blocking. Contiguous `TriangleAlignmentAgeRuleV1` ranges either
admit an age region under predeclared limits or refuse it outright.

## Runtime and publication binding

Every executable window needs a
`TriangleAlignmentConsumptionReceiptV1`. Planner, runtime, final validation,
and atomic publication must agree exactly on:

- alignment evidence ID and policy ID;
- exact versus bounded treatment;
- maximum age;
- probe leg and recommended event time; and
- selected tuple-content digest.

Any mismatch, missing receipt, non-atomic publication, future source row,
outage infill, missing ceiling comparison, unqualified age bin, or
synthetic-residual failure makes the qualification fail closed. The final
content-addressed artifact includes the release-candidate byte reference,
policy and maximum age, coverage census, age distributions, comparisons,
residual relation, receipts, and recomputed status.
