# Proposal-engine registry and qualified portfolios

The v2.4 proposal layer is a first-party model bank, not an empirical-motif
pipeline with optional post-certification side projects. Engine identity,
benchmark role, and product eligibility are separate:

- a descriptor identifies one concrete implementation/configuration family;
- a campaign assigns a baseline, control, candidate, ablation, or reference
  role for that experiment; and
- an eligibility audit grants at most research-only, benchmark,
  reconstruction, or ensemble use from exact retained evidence.

“Challenger” remains useful as a campaign role in older module and document
names. It is not a permanent product classification.

## Current executable boundary

Only HistData.com ASCII/T data for the synchronized EURGBP/EURUSD/GBPUSD
triangle is executable in this milestone. The contracts retain provider,
dataset, origin, and delivery identities so later providers do not require a
schema rewrite. They do not enable OANDA, a broker feed, broker adaptation,
broker-conditioned delivery, or another historical provider. The constrained
Schrödinger bridge is registered but is research-only because its implemented
target requires the deferred broker milestone.

The installed registry discovers 13 concrete variants: empirical motif, four
event clocks, three marked Hawkes variants, two regime-Hawkes variants, RMTPP,
Add-Thin, and the constrained bridge. Registry descriptors bind implementation
version and source hash, supported information modes/marks/symbols, config and
fit schemas, deterministic seed policy, and bounded resources.

## Plan and evidence rules

`ReconstructionPlanSpecV2` requires all three declarations:

- `proposal_engine_ids`: unique engine IDs in operator-declared order;
- `selected_proposal_engine_ids`: the explicit product selection; and
- `proposal_evaluation_paths`: strong retained HistData scorecards.

It may also bind one `qualification_dossier_path`. When present, the planner
verifies the exact #486 experiment, #489 evaluation, corpus, campaign, metric
trace, installed registry, implementation hash, policy, power study, engine
decisions, and validation-fitted portfolio weights. Powered qualification is
an additional restriction: it can revoke a legacy permission but cannot
promote an engine that failed its retained campaign or lacks runtime evidence.
The powered proposal corpus is allowed to be newer than the benchmark artifact
immutably bound inside an existing motif library; each chain is verified
against its own role, and the retained proposal campaign must match the
dossier exactly.

The plan binds config, dataset resolution, point-in-time context, benchmark,
qualification, leakage, and scorecard artifacts by SHA-256. Evidence is
eligible only when its benchmark corpus and engine config identity match the
plan. A changed artifact, installed implementation hash, dataset version,
schema, or eligibility decision fails preflight. Portfolio order is not
scientific rank, and the fixed fallback policy is
`refuse-no-silent-fallback-v1`.

The v2.3 `ReconstructionPlanSpecV1` remains a deprecated compatibility input.
It translates deterministically to an explicit motif-only portfolio; it does
not silently add the other engines or import their evidence.

Retained #454 evidence historically qualified empirical motif under the legacy
promotion policy. The #490 powered qualification campaign supersedes that
permission for an exact plan: all 12 executable HistData engines are currently
`insufficient_evidence` at the observed powered support, while the deferred
broker-target bridge is `refused`. Consequently the honest powered result is
no product selection. A plan bound to that dossier fails closed instead of
constructing an executable portfolio or silently falling back to motif.

Plans without a powered dossier retain the legacy v2.4 compatibility behavior
and describe motif as a `single-qualified-engine`; they do not claim that
ensemble-member variation is cross-model diversity. New qualification-aware
work should bind the dossier and honor its stricter no-decision result.

The ensemble plan may describe more seed members than the retention policy
permits the product to publish. Only the predeclared retained-member set is
scheduled through the product workflow. This keeps workflow counts, resource
estimates, validation, and atomic persistence aligned while preserving the
larger ensemble design as planning evidence.

## Public discovery and evaluation

```sh
histdatacom reconstruction --json engines
histdatacom reconstruction --json portfolio \
  --plan work/synthetic-infill-plan-<sha256>.json

# Evaluate the current HistData model bank. The bridge is retained as a
# machine-readable refusal because broker targets are out of scope.
histdatacom reconstruction --json engine-evaluate \
  --benchmark-manifest artifacts/reverse-degradation-manifest-<sha256>.json \
  --source-root data/ASCII/T \
  --output-directory work/proposal-evaluation

# Evaluate one engine against the always-present empirical reference.
histdatacom reconstruction --json engine-evaluate \
  --benchmark-manifest artifacts/reverse-degradation-manifest-<sha256>.json \
  --source-root data/ASCII/T \
  --output-directory work/nhpp-evaluation \
  --engine histdatacom.event-clock.nhpp

# Power-qualify that exact evaluation against a frozen HistData experiment.
histdatacom reconstruction --json qualify \
  --evaluation work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json \
  --experiment work/experiment/reconstruction-experiment-<sha256>.json \
  --output-directory work/qualification
```

The same operations are available as
`ReconstructionClient.proposal_engines()`, `proposal_portfolio()`,
`evaluate_proposal_portfolio()`, and `qualify_proposal_portfolio()`.
Evaluation and qualification never promote an automatic winner. Product
execution dispatches only a selected reconstruction-eligible engine through
an installed first-party adapter, then uses generic carving, synchronized
triangle reconciliation, ensemble-member handling, validation, atomic
persistence, preview, and replay. Missing adapters and failed/research-only
eligibility are refusals, never motif fallback.

See
[`powered-reconstruction-qualification.md`](powered-reconstruction-qualification.md)
for residual methods, proper scores, power semantics, the retained real
campaign, and the exact no-decision boundary.

## Lineage and failure semantics

Proposal ledgers and committed validation evidence carry registry, portfolio,
engine, binding, audit, and retained evidence IDs in addition to the existing
event-level generator/config/anchor lineage. Default stage loading verifies
every strong artifact. Preflight also reconstructs the installed registry, so
a code change invalidates an old plan even when its JSON was not edited.

Cancellation, retry, checkpoint resume, deterministic semantic seeding,
immutable observed anchors, and atomic commit remain properties of the common
reconstruction runtime. Adding another reconstruction-eligible engine must
provide the same adapter and lineage guarantees before it may appear in
`selected_proposal_engine_ids`.
