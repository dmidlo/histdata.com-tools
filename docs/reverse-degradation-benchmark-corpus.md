# Real reverse-degradation benchmark corpus and promotion gates

This document defines the real-data acceptance boundary owned by issue #463.
It supplements the generator-neutral v1 engine documented in
[`reverse-degradation-benchmark-contracts.md`](reverse-degradation-benchmark-contracts.md).

The packaged promotion policy is intentionally committed before any promotable
real candidate report. At this policy stage there is no candidate winner,
promotion result, or claim that reconstruction recovers missing historical
ticks.

## Frozen policy artifact

`histdatacom.synthetic` packages
`assets/reverse_degradation_promotion_gates_v1.json`. The installed loader
`load_default_benchmark_promotion_gate_policy()` verifies its schema, complete
hard/advisory surface, and content-derived `policy_id`.

The policy records:

- issue authority `#463` and policy version `v1`;
- `frozen_before_candidate_results=true`;
- separate campaign and candidate scopes;
- explicit comparators and thresholds;
- hard gates that fail closed when evidence is missing; and
- advisory gates that remain visible but cannot silently become hard gates.

`BenchmarkPromotionDecisionV1` always records
`automatic_winner=false`. Passing gates makes one measured subject eligible
under this policy; it does not select a default generator or compare unrelated
campaigns.

## Campaign hard gates

The real campaign must demonstrate all of the following before its candidate
decisions are meaningful:

| Evidence | Predeclared hard threshold |
| --- | ---: |
| source hash mismatches | `0` |
| split/near-neighbor holdout leakage | `0` |
| information-audit violations | `0` |
| missing required strata | `0` |
| dense identity-control failures | `0` |
| negative controls that unexpectedly pass | `0` |
| maximum bounded hook metrics per window | `64` |
| campaign runtime | `<= 900 seconds` |
| peak memory | `<= 2 GiB` |
| compact persisted artifact bytes | `<= 64 MiB` |

Campaign support of at least eighteen real synchronized windows and at least
two deterministic ensemble members is advisory. The final report must expose
these observations even when they miss the advisory threshold.

## Candidate hard gates

A measured candidate fails promotion eligibility when any hard observation is
missing or violates its threshold:

| Evidence | Predeclared hard threshold |
| --- | ---: |
| immutable anchor violations | `0` |
| failed or non-converged measured windows | `0` |
| unsupported-context emissions | `0` |
| worst event-count relative error | `<= 0.50` |
| worst inter-arrival histogram L1 | `<= 0.45` |
| worst update-transition matrix L1 | `<= 0.55` |
| worst spread-tail relative error | `<= 0.75` |
| worst realized-variation relative error | `<= 0.75` |

Refusal-rate reporting, a p99 synchronized-triangle residual no greater than
five pips, and at least six supported uncertainty intervals are advisory. They
remain part of every decision so a nominal hard-gate pass cannot hide weak
support.

## Evidence ordering

The scientific order is mandatory:

1. Commit the policy schemas, evaluator, packaged thresholds, and tests.
2. Record the policy commit and content ID in the later campaign manifest.
3. Construct immutable calibration, validation, and final-holdout partitions.
4. Run the untouched dense identity and declared negative controls.
5. Run transparent controls and any provisional empirical candidate.
6. Evaluate only the already-frozen requirements.
7. Publish compact source/split hashes, leakage audit, decisions, scorecards,
   resource evidence, and deterministic replay evidence.

Changing a threshold, comparator, scope, severity, or metric meaning requires a
new policy version and a new content ID. A candidate result produced before
that new policy commit is ineligible under the new version.

## Candidate boundary before #464

Issue #464 owns the qualified production reference-motif library. The #463
campaign may exercise the existing `EmpiricalMotifBenchmarkGeneratorV1` only
with a provisional calibration-only motif input whose source windows cannot
touch validation or final holdout. Such a result is labeled provisional and
cannot claim that #464 is complete.

## Nonclaims

- A policy pass does not identify the ticks that historically went missing.
- A policy pass does not select an automatic winner or trading strategy.
- Negative-control failure is required evidence, not an implementation error.
- Bars and M1 data are not benchmark source inputs.
- Broker capture, fingerprinting, and transfer are outside this policy.
