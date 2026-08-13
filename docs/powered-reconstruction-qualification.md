# Powered reconstruction qualification

The v2.4 qualification layer turns one exact HistData reverse-degradation
campaign into a bounded, content-addressed scientific decision dossier. It is
a stricter permission layer over the proposal-engine campaign: it may reduce
benchmark, reconstruction, or ensemble eligibility, but it can never promote
an engine that failed its retained campaign or lacks runtime evidence.

The objective is regime-conditioned, constrained synthetic infill. Historical
ticks remain immutable observations; candidates are plausible samples under a
declared modern observation process, not recovered historical truth. A good
diagnostic is evidence about a named failure mode, not a model-ranking rule.

## Current data boundary

Real qualification accepts only catalog-identified `histdata.com` ASCII/T
ticks for the synchronized EURGBP/EURUSD/GBPUSD triangle. The provider,
dataset, origin, delivery, and split fields are provider-neutral domain
contracts so later data sources do not require an identity redesign. They do
not enable another provider.

OANDA, broker feeds, broker fingerprints, broker-conditioned delivery, and
alternate historical sources are later milestones. The registered
Schrödinger-bridge configuration is retained as a machine-readable deferred
refusal because its target requires that later broker capability.

## Evidence flow

The installed path is deliberately end to end:

1. The #486 experiment freezes the HistData dataset version, whole-period
   roles, leakage policy, operator identities, and strong artifact bindings.
2. The #489 evaluator runs every HistData benchmark-eligible engine and emits
   a row-free, per-window/per-member metric trace beside the campaign
   scorecard.
3. `qualify` verifies both inputs, applies the frozen policy and power study,
   fits portfolio weights on validation windows, evaluates those frozen
   weights once on the final holdout, and writes one dossier.
4. Portfolio construction and plan validation verify the exact dossier and
   intersect its permissions with legacy promotion evidence. A stale hash,
   registry, implementation, corpus, campaign, experiment, decision, or
   weight fails before submission.

The plan may retain an older benchmark artifact as an immutable dependency of
the already-built motif library while binding a newer qualification corpus as
proposal evidence. Those artifacts have different roles and are not asserted
to be identical. The planner instead requires the retained proposal campaign
to match the dossier's exact corpus and campaign, while the motif artifact
continues to match its own declared benchmark dependency.

Tick rows and residual samples remain process-local. Persisted contracts carry
bounded summaries and strong references only.

## Residual contracts

`PointProcessResidualInputV1` has two explicit routes:

- `analytic_time_rescaling` accepts integrated hazards from a model that
  exposes a cumulative conditional intensity. It deterministically maps each
  increment to a uniform PIT and reports uniformity, lag-one dependence, and
  conditioned mark-PIT diagnostics.
- `simulation_predictive` accepts predictive PITs for engines without a
  usable analytic compensator, or for the common cross-engine evaluator path.

`PointProcessResidualReportV1` persists sample support, KS-style statistics,
combined probabilities, serial dependence, quantiles, method, applicability,
and reason codes without persisting residual rows. The current common real
campaign uses the simulation-predictive route for all engines because the
#489 row-free trace exposes comparable realized/predictive streams rather than
model-specific compensator increments. The analytic route is a first-party
contract and deterministic implementation, but its use must be declared by a
future evaluator adapter that actually supplies an exact fitted compensator;
the qualification layer does not infer one from a status label.

Time-rescaling can reveal intensity misspecification. It cannot by itself
establish realistic marks, paths, cross-currency dependence, tail behavior,
strategy utility, or historical truth.

## Scores and hard gates

Each engine and the dense, linear-interpolation, and negative controls receive
validation and final-holdout reports with:

- energy and variogram scores over the synchronized predictive feature
  vector;
- marginal CRPS, nominal coverage, empirical coverage, calibration error,
  and sharpness;
- tail, path, and cross-series errors; and
- event-count, interarrival, mark/update, stale/burst, spread, timestamp,
  transition, triangle, and synchronized-path observables inherited from the
  metric trace.

Energy, variogram, path, tail, and cross-series gates compare against the
predeclared linear-interpolation control. Dense identity must behave as the
reference, and the negative anchor-drop control must fail for anchor loss.
Benjamini-Hochberg adjustment is applied within each engine's final-holdout
residual family.

Every hard gate has a named test and misspecification simulator. The power
artifact reports deterministic false-positive rate and power over the frozen
sample-size grid and at the campaign's observed support. A gate is reliable
only when observed support meets policy, false-positive rate is within policy,
and power reaches policy. Otherwise the engine decision records
`insufficient_evidence`; missing or underpowered evidence never becomes pass.

The five statuses are `passed`, `failed`, `insufficient_evidence`,
`not_applicable`, and `refused`.

## Portfolio calibration and no-decision semantics

Cross-engine nonnegative weights are fitted only on validation-window energy
score. The artifact freezes the exact validation window IDs and weights before
reading the distinct final-holdout window set, then records one final-holdout
evaluation. Portfolio order, issue order, and final-holdout performance never
select a winner.

Qualification is monotonic: a powered decision is intersected with retained
promotion and runtime eligibility. It cannot revive an engine that failed the
campaign. If every engine is failed or underpowered, the correct product
result is no decision. Planning then refuses the requested product selection
instead of producing an executable plan or silently falling back to motif.

## Installed CLI and typed API

Run the evaluator first, then bind its exact result to an exact frozen
experiment:

```sh
histdatacom reconstruction --json qualify \
  --evaluation work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json \
  --experiment work/experiment/reconstruction-experiment-<sha256>.json \
  --output-directory work/qualification
```

The typed API uses the same implementation and produces the same dossier:

```python
from histdatacom.reconstruction import ReconstructionClient

client = ReconstructionClient()
dossier = client.qualify_proposal_portfolio(
    "work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json",
    "work/experiment/reconstruction-experiment-<sha256>.json",
    output_directory="work/qualification",
)
```

New v2 plan specifications may set `qualification_dossier_path`. V1
compatibility inputs reject that field. A v2 planner verifies the dossier,
requires its corpus/campaign to match retained evaluation evidence, embeds the
policy/power/calibration/decision identities and weights in the proposal
portfolio, and re-verifies them during plan validation.

## Retained HistData campaigns

The first attempted final holdout, October 2025, contained a synchronized
one-hour timestamp regression in all three source files. HistData documents
its timestamps as fixed EST without daylight-saving adjustment, so the source
adapter correctly refused the non-monotone files; the campaign did not rewrite
or sanitize them. An untouched November 2025 month was selected under the same
predeclared profile instead.

The replacement corpus contains 18 synchronized windows over January 2010,
January 2024, and November 2025, with zero recorded neighbor leakage. Its
identities are:

- corpus:
  `reverse-degradation-corpus:sha256:78d1e49540f8b0903f42f9ed060b7941062d87b180ee5bd8bf999890cc56c876`;
- campaign:
  `reverse-degradation-campaign:sha256:021a375bbd312e9643d974f55ca179215516028d00d18ba808ec4a5ab8b16300`;
- experiment:
  `reconstruction-experiment:sha256:856b8cbe428736bad7e50919a3dbbada4c8f4d4c1b8dab43cbf77631bd2400e1`;
- dossier:
  `powered-qualification-dossier:sha256:1ce8513a9f548748feb52edce60421cd4f5bccd5a12924dad62ed156d51685e8`.

All 12 HistData engines executed; the broker-target bridge was refused. At six
validation windows, no hard-gate power study met the complete reliability
policy. The result was therefore 12 `insufficient_evidence` decisions, one
deferred `refused` decision, no reconstruction-eligible engine, no
ensemble-eligible engine, and no automatic winner. The installed CLI and typed
API reproduced the exact dossier. A product planning attempt consumed it and
failed closed because the requested motif engine was no longer reconstruction
eligible under powered qualification.

This result does not say all engines are scientifically equivalent or bad. It
says that exact retained experiment cannot support a product promotion at the
predeclared reliability target.

For #491, a second predeclared campaign expanded support to 96 synchronized
ten-minute windows across 18 HistData months: 32 calibration windows in the
first half of 2024, 32 validation windows in the second half of 2024, and 32
protected final-holdout windows in the second half of 2025. The catalog-bound
experiment covers 54 source partitions and freezes all assignments before
candidate results. The final holdout is evaluated once after validation-only
portfolio fitting.

The powered comparison retains three marked-Hawkes ablations. Diagonal
self-excitation passes all ten hard gate families and is the sole
reconstruction- and ensemble-eligible variant. Full self/cross excitation
fails time-uniformity; zero excitation fails time-uniformity and path/tail
behavior. Nonpassing variants remain in the evidence graph but cannot enter
the selected product. This is an eligibility decision, not an automatic
winner, historical-truth claim, broker claim, or investment recommendation.
The content-addressed v2.4 identities are published with the final #491
certification so implementation or release-version changes cannot reuse a
stale dossier.

## Scientific interpretation

The implementation follows the role of time rescaling described by
[Brown et al. (2002)](https://pubmed.ncbi.nlm.nih.gov/11802915/), joint marked
process checks described by
[Yousefi et al. (2020)](https://pubmed.ncbi.nlm.nih.gov/32946712/), calibration
and sharpness from
[Gneiting, Balabdaoui, and Raftery (2007)](https://academic.oup.com/jrsssb/article/69/2/243/7109375),
finite-sample score reliability from
[Marcotte et al. (2023)](https://proceedings.mlr.press/v202/marcotte23a.html),
and the complementary sensitivity of energy and variogram scores from
[Scheuerer and Hamill (2015)](https://journals.ametsoc.org/view/journals/mwre/143/4/mwr-d-14-00269.1.xml).
Validation-only portfolio weighting is consistent with the held-out predictive
principle in
[Yao et al. (2018)](https://doi.org/10.1214/17-BA1091).

These references justify diagnostic tools, not an automatic winner, trading
claim, centralized-volume claim, or historical-truth claim.
