# Forecast taxonomy and executable engine registry

`histdatacom.forecasting` supplies a versioned scientific taxonomy and a real,
offline registry-backed fit/generate/score/replay path (#562). Registration is
not evidence of empirical skill, independent ensemble votes or production
qualification.

## Source traceability and remaining acceptance

The packaged `assets/forecast_taxonomy_v1.json` retains the 18 minimum technique
roots from #562 and fine-grained descendants from #563–#565, plus the explicitly
implemented reference comparators. Each node has requirement references. It
contains 99 nodes, **not 99 engines**. There is no target engine count such as 50.

The original 23-turn Forex Factory conversation archive referenced by #556 and
#573 was not available for direct reconciliation. The shipped taxonomy therefore
states `archive_coverage: unverified`; neither the family minimum nor the finer
issue-derived nodes prove full archived-taxonomy expressiveness. #562 remains
open for that requirement. `archive_source` permits a later versioned successor
to retain a source pointer but still does not certify exhaustive mapping. The
archive is planning provenance, never runtime economic data. #691 owns the
cross-program requirement ledger and orphan reconciliation.

## Independent classification axes

`ForecastTechniqueV1` and `ForecastTaxonomyV1` represent bounded hierarchical
techniques. Stable root families cover benchmarks; univariate/state-space;
multivariate VAR/BVAR/VECM/GVAR; latent factors; mixed frequency; regularized
linear; nonlinear; trees; neural temporal; regimes; structural accounting;
semi-structural econometrics; analogues; market-implied; text; alternative
high-frequency; panels; and density models. Engines can reference multiple
techniques without duplicating the engine.

`ForecastEngineDescriptorV1` independently declares:

- Information channels: macro vintages, event consensus, observed markets,
  point-in-time documents, qualified alternatives, panels, components and known
  release calendars.
- Forecast horizons and target kinds, independently of source frequencies.
- Function: primary, component, revision, regime, reliability, residual,
  calibration, ensemble weighting, anomaly or fallback.
- Ensemble role: permanent benchmark, candidate, challenger, member subject to
  diversity qualification, ablation control, fallback or controller.
- Compatible output schema and point-only versus finite-support probabilistic
  capability. A degenerate reference distribution is not a calibrated density.
- Minimum/maximum sample and columns, fit/generate replay claims, training-cost
  declaration, refusal modes, leakage constraints, scientific role, a distinct
  ablation engine and minimum benchmark engine keys.

Auxiliary function schemas are declarations, not invented implementations. The
runner refuses unknown bindings, including declarations whose engine names or
metadata resemble installed implementations. Wire metadata never imports code.
`family_groups()` groups variants by their declared roots; it makes no effective
rank, error-correlation, weight or independent-model-count claim. Those require
#563 evidence.

## Implemented reference consumer

`ForecastEngineRunnerV1` has explicit installed bindings for four actual
univariate reference estimators:

| Engine | Executed estimate | Fit versus inference |
| --- | --- | --- |
| `historical-mean` | Arithmetic mean | Fixed training sample |
| `historical-median` | Sample median | Fixed training sample |
| `last-value` | Last known requested observation | Latest inference row |
| `reference-blend` | Half training mean plus half latest inference value | Existing #561 consumer |

These all belong to the benchmark family. The mean and median remain distinct
permanent comparator keys even when a particular sample gives equal estimates.
They are historical-value comparators, not the forecast-combination mean/median
methods owned by #563/#564. The blend's historical-mean ablation removes its
latest-value contribution; comparison reports retain both exact executed fits.

References currently accept a single macro **level** column, monthly or quarterly
source and target frequency, at least three complete training observations and
at most 10,000 requested periods. First-release actual is the only implemented
target kind, at any of the five existing #560 horizons. Other targets and
information channels can be classified but are not silently admitted to these
estimators. There is no fill, implicit rebase, mixed-era fit or frequency bridge.
Numerical overflow fails with a finite-value refusal, including an even-sample
median whose intermediate addition overflows despite finite input values.

Fit binds the horizon, target kind, column and exact vintage training inputs.
Generation rejects a different horizon/task, incompatible source/target
unit/scale/base/frequency, unavailable/stale/warmup cells, resource overflow,
changed executable, wrong registry or changed fitted state. Before prediction
it applies the existing full feature/calendar rules for clocks, schedules,
first-release status and historical-prefix consistency. A later observation
may update the last-known estimator; a later unavailable vintage cannot change
a historical forecast identity.

The existing `feature_forecasts.py`, its model version `"1.0"`, and all #560/#561
schemas remain unchanged. A separate registry binding version `"1.0.0"` maps
that legacy baseline explicitly. New mean/median/last fits have their own model
identities. Both actual implementation-module bytes and the registry runner's
module bytes are hashed. A prose specification is not executable provenance.
Exact replay is a same-installed-code/runtime claim, not cross-platform bitwise
certification (#637–#641).

```python
from histdatacom.forecasting import (
    ForecastEngineRunnerV1, REFERENCE_BLEND, default_forecast_registry,
    read_engine_artifact, write_engine_artifact,
)

runner = ForecastEngineRunnerV1(default_forecast_registry())
model = runner.fit(
    REFERENCE_BLEND, training_inputs, column="inflation",
    horizon=cutoff.horizon, target_kind=target.kind,
    trained_at_ns=training_time,
)
snapshot = runner.generate(
    model, inference_inputs, cutoff=cutoff, target=target,
    generated_at_ns=generation_time,
)
score = runner.score(snapshot, outcome_calendar, scored_at_ns=scoring_time)
restored = read_engine_artifact(write_engine_artifact(score, artifact_dir))
snapshot.verify_against(calendar_history, vintage_store)
```

All variables in this example are explicit #560/#561 contracts and clocks;
there is no ambient latest-data lookup. `runner.compare(...)` executes the
candidate, each required benchmark and its ablation on identical training and
inference information sets, target, horizon, clocks and normalized outcome
evidence. `ForecastEngineComparisonV1` recomputes absolute-error reductions and
retains every full score. Three competing predictions for one event remain one
evidence unit. A reference case is not a chronological validation campaign.

`require_production()` always refuses in this version. There is no caller pass
flag, digest-only receipt or registry status that can promote a model. Actual
production qualification requires chronological benchmark/ablation evidence,
protected evaluation, incremental value or documented fallback qualification
and diversity governance in #563–#566/#580. This release deliberately does not
claim those downstream issues are complete, nor automatically close #557.

## Version changes and durable replay

`ForecastEngineRegistryV1.successor()` returns a `ForecastRegistryChangeV1`
receipt retaining exact predecessor and successor snapshots. It classifies
additive nodes/engines as at least a minor change; removals or changed existing
meanings require major versions. Changed taxonomy content must also advance
its own version. Changed existing engine descriptors conservatively require
their own major version. A version-only unchanged-content successor may be a
patch. Same-version mutations and patch-only additions fail. An initial
registry constructor describes a snapshot, not authority to republish a known
version: evolution of a retained registry must use this predecessor-bound gate.

`ForecastEngineModelV1`, `ForecastEngineSnapshotV1`, `ForecastEngineScoreV1` and
`ForecastEngineComparisonV1` retain the complete registry and feature envelopes.
Construction/restoration reexecutes the installed fit/prediction and scoring
semantics. Bare feature or calendar projections cannot enter the engine score
or persistence boundary. The existing lower-level feature API remains usable
for its original purpose; it does not imply registry admission.

All final sealed envelopes share the 32 MiB UTF-8 and expanded depth-64 bounds
from #560. A valid inner artifact may be refused in a larger envelope. Collection
limits are checked before expensive traversal: 4,096 techniques/engines, 256
entries in a descriptor axis, 16,384 registry technique edges, hierarchy depth
32 and 64 scores per comparison. These are resource bounds, not scientific
quotas or throughput promises. Declared complexity is not a wall-time scheduler.

Persistence replays before writing, uses same-directory temporary files,
flush/fsync and atomic no-clobber hard-link publication, and refuses conflicting
existing bytes. Reads require bounded regular, non-symlink files and canonical
content-addressed names; FIFOs and directories fail without blocking. It is a
local offline persistence path, not a distributed registry or remote object store.
