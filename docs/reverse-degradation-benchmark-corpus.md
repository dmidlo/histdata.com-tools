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

## Installed corpus and campaign API

`build_reverse_degradation_benchmark_corpus()` accepts only explicit prior
artifact paths. It does not refresh epochs, fit a new observation operator, or
download context while a benchmark is running. The resulting
`ReverseDegradationBenchmarkCorpusV1` binds:

- three monthly Arrow tick caches per blocked role for EURUSD, GBPUSD, and
  EURGBP;
- six synchronized UTC windows per role, with Asia, London, and New York
  session coverage;
- the v2 feed-epoch definition and fitted observation-operator IDs;
- the point-in-time market-context corpus and the CFTC reconstruction-view
  positioning corpus;
- nine degradation configurations and the complete metric registry; and
- the packaged policy ID and the full commit SHA that predates candidate
  output.

Each monthly source records its relative cache path, byte size, row count, and
SHA-256. Each window records the half-open UTC interval, source partition IDs,
event counts by symbol and update state, and a SHA-256 of the first bounded
event selection for every symbol.
`replay_reverse_degradation_benchmark_corpus()` re-hashes every source,
recounts Arrow rows, reselects every bounded window, and compares all window
hashes before candidate execution. Neither dense nor holdout event rows appear
in the corpus or scorecard.

`run_reverse_degradation_benchmark_campaign()` builds a train-only provisional
motif index, executes every declared degradation, runs dense identity,
degraded/no-fill identity, linear interpolation, the existing
`EmpiricalMotifBenchmarkGeneratorV1`, and an anchor-drop negative control, then
evaluates the frozen policy. A required degradation failure aborts the
campaign rather than becoming a passing aggregate. The empirical motif report
is always marked provisional until #464 supplies the qualified library.

The CFTC archive used by #468 represents the corrected reconstruction view,
not a fully preserved original publication vintage. The campaign therefore
queries it with `EX_POST_RECONSTRUCTION` and retains that limitation; it does
not relabel corrected state as ex-ante knowledge. Market-context event queries
remain ex-ante at each window start.

## Installed command

Run the complete bounded build, replay, campaign, and artifact write through
the installed CLI:

```console
histdatacom analytics reverse-degradation-benchmark-corpus \
  --source-root data/ASCII/T \
  --definition data/.histdatacom/analytics/feed-epochs-v2-issue-460/feed-epochs-v2-definition.json \
  --observation-campaign data/.histdatacom/analytics/observation-calibration-v2-issue-462/observation-calibration-v2-campaign.json \
  --market-context-corpus .histdatacom/market-context-461-final-v4/market-context-corpus-9255f8c39f999b7a54e41a59a6f1d96f02e897af8383795e464a2f8738b08e00.json \
  --cftc-positioning-corpus data/.histdatacom/analytics/cftc-positioning-issue-468-final/cftc-positioning-corpus-887a47840090cdab1982fe910a4bdf8c1fcc9af256ab687bceae1b8dd1cbd3e0.json \
  --artifact-dir data/.histdatacom/analytics/reverse-degradation-benchmark-issue-463-final-v5 \
  --json
```

The defaults select `201001`, `202401`, and `202510` as calibration/training,
validation, and final holdout. All periods and resource limits are explicit CLI
options, but changing them creates a different content ID.

The artifact directory contains five immutable JSON files:

```text
reverse-degradation-manifest-<sha256>.json
reverse-degradation-motif-index-<sha256>.json
reverse-degradation-leakage-audit-<sha256>.json
reverse-degradation-resource-audit-<sha256>.json
reverse-degradation-scorecard-<sha256>.json
```

Readers verify the filename digest before restoring strict contracts. Writes
are atomic and reuse identical content, while an existing content-addressed
path with different bytes is refused.

## Executed degradation and metric surface

Every real synchronized window executes uniform thinning, the fitted
state-dependent operator, unchanged filtering, timestamp quantization,
batching, rate caps, a missing-window stress, duplicate injection, and
symbol-specific thinning. Protected first/last anchors remain immutable in all
non-negative-control paths.

The compact report covers multiscale counts and dispersion; inter-arrival
histograms, quantiles, duration dependence, and burst/quiet rates; quote-update
proportions and transition matrices; spread tails/jumps, stale runs, timestamp
precision, and tick-grid adherence; increments, realized variation, jump
proxies, excursions, and anchors; triangle synchronization and residuals;
context-conditioned slices; and refusal/unsupported rates. Point-process fit
is recorded as not applicable when a candidate exposes no conditional
intensity instead of fabricating a diagnostic. Six frequentist 95% uncertainty
intervals summarize window/member variation for every candidate report.

## Candidate boundary supplied to #464

The #463 campaign exercised `EmpiricalMotifBenchmarkGeneratorV1` with a
provisional calibration-only motif input whose source windows could not touch
validation or final holdout. That failed result remains immutable baseline
evidence. The qualified production library, independent split manifest, and
non-provisional rerun are now owned by #464 and documented in
[`modern-reference-motif-library.md`](modern-reference-motif-library.md).

## Issue #463 reference campaign

The installed command produced the closure campaign on 2026-07-15. Its
immutable identities are:

- corpus ID
  `reverse-degradation-corpus:sha256:a760a010d44de2d6258b7c3d71651b00bc24eaef53092f37bd75b3ae2395c5dc`;
- provisional motif index ID
  `reference-motif-index:sha256:7c56a5d1caf219df72762499743c3ca1fd3eba162c605612ffe46fa0495b6835`;
- campaign ID
  `reverse-degradation-campaign:sha256:9158e185c5fdeec12a2450e1e8f9d0b3d42d735f63d1d0e9ec530340c373792d`;
  and
- frozen policy ID
  `benchmark-promotion-gates:sha256:f59039526fca4a70b40f836525ca08efc0bb336668a1d6676bc1c6a3ba7a186f`
  from commit `0caec1480a957528ebefdff062e13012ea11e84d`.

The corpus binds nine real monthly sources containing 11,225,291 Arrow rows
and 314,343,815 bytes. Replay verified all nine source hashes and all 54
symbol-window hashes. Eighteen synchronized windows cover all three blocked
roles and sessions. Six windows carry point-in-time macro-release context, and
twelve explicitly record no matching event. Across the corpus, all four update
states are represented: 3,153 ask-only, 3,137 bid-only, 5,843 joint, and 162
unchanged observations. The provisional index retained 221 train fragments
from 770 projected source windows and performed 5,310 cross-split leakage
comparisons; neighbor leakage and information-audit violations were both zero.

All nine degradation families executed on all eighteen windows with zero
execution failures and zero protected-anchor violations. The campaign also
refuses a vacuous family: every family had to change the observable stream in
at least one real window. The affected-window counts were 12 for batching, 18
for duplicate injection, 18 for the fitted state-dependent operator, one for
the missing-window stress, eight for the eight-events/second rate cap, 18 for
symbol-specific thinning, 12 for timestamp quantization, 12 for unchanged
filtering, and 18 for uniform thinning. The campaign passed its frozen
campaign gates in 18.089916 seconds at 555,483,136 bytes peak RSS. The five
persisted artifacts total exactly 1,093,214 bytes, matching both the scorecard
observation and the post-write filesystem measurement.

The controls behaved falsifiably:

- untouched dense identity passed with zero hard-metric or anchor error;
- the anchor-drop negative control failed with one immutable-anchor violation;
- degraded identity and linear interpolation failed their applicable hard
  gates; and
- the provisional empirical motif emitted real proposals but was not eligible:
  its candidate-window refusal rate was `0.4166666666666667`, worst event-count
  relative error was `0.6653645833333334` against the hard `0.50` threshold,
  and its p99 triangle residual was `6.219778927497112` pips against the
  advisory five-pip target. Its inter-arrival, path-variation, and
  update-transition gates also failed.

That motif result is intentionally published as a failure, not tuned away.
It gives #464 a concrete event-count, refusal, and cross-series acceptance
surface while preserving the rule that #463 does not select a winning model.

The reference artifact SHA-256 values are:

| Artifact | SHA-256 |
| --- | --- |
| manifest | `d1ddf45d68ade8c1ba4abc3df5a60a26483bb3eab950d4c29f53709e9214ed24` |
| motif index | `b32f355f2d2348682684723213a617035c98943ac67ac97eeb5518b81281f7d4` |
| leakage audit | `3f532feaebabf3f3503dd719b17a4c04398cd110c283f29fcd9c60bfee6269c5` |
| resource audit | `3c12b6d3749e557d25b18106bae424b2711b8c9be6861dec879a1fc140be9d6b` |
| scorecard | `841ffd9f2cfbef2578bf9cf339b6f045650423eff7a95b4caaadb4f233858b03` |

## Nonclaims

- A policy pass does not identify the ticks that historically went missing.
- A policy pass does not select an automatic winner or trading strategy.
- Negative-control failure is required evidence, not an implementation error.
- Bars and M1 data are not benchmark source inputs.
- Broker capture, fingerprinting, and transfer are outside this policy.
