# Frozen forecasting benchmark suite

Issue #557 adds a public, installed executable **architectural benchmark**. The
parent roadmap #556 permits point-in-time fixtures before national archives are
complete. This suite freezes task semantics and permanent reference comparisons;
it is not a historical forecasting campaign, model promotion, calibration result,
holdout decision, or empirical accuracy claim. The original #562 taxonomy archive
remains unverified. #558/#559 and the #563–#568 research requirements remain open.

```python
from histdatacom.forecasting import (
    default_forecast_benchmark_suite, ForecastBenchmarkRunV1,
    write_benchmark_artifact, read_benchmark_artifact,
)

suite = default_forecast_benchmark_suite()
run = ForecastBenchmarkRunV1(suite)  # Actually fit, generate, score and check.
path = write_benchmark_artifact(run, "benchmark-evidence")  # Re-executes.
restored = read_benchmark_artifact(path)  # Re-executes; never trusts a pass flag.
assert restored.to_json() == run.to_json()
```

There is no dependency on the repository's `tests` package, a local corpus,
network access, ambient current database, or optional forecast-library backend.
The package contains `assets/forecast_benchmark_suite_v1.json`. Exact bytes are
bound to the maintained root digest as well as execution provenance. Changing
that asset cannot silently republish a different version-one root.

## What is frozen

`ForecastBenchmarkCaseV1` retains a task/horizon, a closed scenario, the exact
required comparator inventory, hand-specified expected points and realized
target, or one specific expected refusal. `ForecastBenchmarkSuiteV1` retains
the complete sorted case inventory, synthetic source parameters, SemVer and
exact predecessor when present. Numerical expectations are checked against
execution: changing an expected number does not make an incorrect computation
pass. Fixture constants deliberately differ across actual, revision and survey
vintages.

The default suite has 20 cases and 60 exact case/comparator coordinates:

- Five horizons × two objectives, with five comparisons per case.
- Explicit future-consensus evolution, revision level, revision-minus-initial,
  observed-consensus surprise, and delayed actual availability.
- Missing consensus, explicit missing feature, insufficient history,
  incompatible target semantics, and unavailable schedule.

The ten ordinary cases form the full T-30d/T-7d/T-1d/T-1h/final-pre-release ×
first-release-actual/consensus rectangle. Final-pre-release uses an explicit
one-second dispatch lead, not hindsight knowledge of the eventual last forecast.
The default execution retains 55 scored results and five refusals, not a
denominator silently reduced to successful models.

## Precisely different comparators

| Comparator | Inputs and executed operation |
| --- | --- |
| `historical-mean` | Arithmetic mean of the three complete retained training-history observations |
| `historical-median` | Sample median of the same historical observations |
| `last-value` | Latest requested historically available inference observation; actual task |
| `consensus-persistence` | Named source's latest visible survey; consensus task only |
| `equal-forecast-mean` | Arithmetic mean of **three complete component forecast points** |
| `forecast-median` | Sample median of those component points; midpoint for an even count |

Actual-history and consensus-history channels have different values, source
records and feature kinds. Target kind is part of the model state and model
name; both objectives retain their own full forecast and score. A benchmark
algorithm may have similar operations across objectives without collapsing
their targets or metrics.

Combination components are the executed historical mean, historical median,
and actual last-value or consensus-persistence forecast. They must match exact
target, unit/scale/base, cutoff/horizon, complete input/source identity and
generation time. Full component artifacts are retained, not only names or
points. The historical mean is **not** called an ensemble mean. These are simple
reference members in one family; three members do not imply three independent
opinions. No covariance weighting, fitted stacking, regime selection or model
bank qualification is claimed here.

At-cutoff consensus persistence is an **observable-reference copy** and may have
zero error by construction. That is an integrity/control benchmark, not evidence
that a model predicted an unknown professional consensus. Only the separate
explicit future-consensus task scores that persisted earlier value against the
later named survey. The future survey is present in eventual outcome evidence
but absent from forecast and training information sets.

## Exact clocks, evidence and metrics

The installed fixture factory constructs complete normalized schedule, initial
release, later revision and survey vintages with synthetic source records and
availability evidence. No invented record is presented as an authenticated
official publication. Source retrieval occurs later and never determines which
vintage was available to a forecast.

Each statistical historical fit uses the information set one second before
forecast generation. Inference and generation occur at the declared cutoff.
Parameter-free copying and combination retain their actual generation-time
information as model state; current surveys or component forecasts are not
backdated into an earlier fit. Publisher time, availability, generation and
scoring remain separate fields. The delayed-publication case deliberately has
a publisher timestamp before cutoff and source availability after cutoff.

All feature matrices and calendar projections use the existing #560/#561
contracts. Historical metadata and vintage chains stay intact; inserting an
unavailable future value, survey, classification or market observation cannot
rewrite an earlier prediction. Missingness is not filled. An earlier vintage
that actually changes the visible input must change the prediction identity.
Current or revised convenience tables are never requested.

Scores retain the first actual even though a different later revision is in the
outcome corpus. Revision and surprise cases use explicit separate target kinds.
At-cutoff consensus replication error and actual forecast error/value-added are
distinct. Every scored result contains a complete `ForecastMathCheckedReportV1`,
including the original feature score, full inputs/model/distribution/outcomes,
formula version, executed #580 receipt and exact code bindings. These checked
reports are not extended or rewritten by the benchmark layer.

Combination arithmetic has an additional versioned reference check: production
uses Python's statistics operations; a separate 50-digit decimal calculation
reconstructs each component's declared mean or lower median directly from its
probability support and computes the combination. Actual member evidence,
formula version, independent result and executed point are retained. Frozen
case values provide another independent oracle. Existing #580 checks alone are
not treated as proof that the new combination ran correctly.

## Refusal and version policy

The five expected refusal codes correspond to explicit checks on retained
source support. The result contains the exact visible input matrix/calendar,
target/cutoff and eventual corpus needed to replay the condition. Unexpected
exceptions propagate; they are not converted into successful negative tests.
A missing expected refusal, a wrong reason, or an incorrect expected numerical
output fails the run.

Use `suite.successor(version=..., cases=..., source_json=...)` for changes.
An additive case requires a minor-or-major increase. Changed source values or
existing case meaning require a major increase. Unchanged definitions may get
a patch successor. Exact predecessor content is embedded. Permanent cases and
their comparator coverage cannot be removed, even by a major successor; a
different scientific task should receive a new case. The root cannot be replaced
by a caller-created case list or relabeled version. This is not a model-tuning
API and never permits selecting expectations after inspecting a protected set.

## Replay, bounds and compatibility

`ForecastBenchmarkRunV1` construction executes the suite. Its immutable JSON
caches the completed evidence so merely inspecting a run does not continually
fit models. `from_json`, `from_dict`, persisted reads and writes re-execute the
whole suite and compare exact results. Missing, extra, duplicated or reordered
coverage and changed executable bindings refuse before expensive execution.
Changed metrics, member evidence, inputs or refusal reasons fail exact replay,
even if an outer content hash is recomputed. Runtime code substitution is tested
with mutation canaries; a caller-supplied status cannot authorize a result.

Bounds are 32 cases, 128 result coordinates, six comparators per case and eight
members for the independent combination check. New envelopes use the #580
8 MiB canonical-byte, 64-level and bounded-collection guards, including expanded
ancestry and final identity overhead. Execution also stops before accumulating
more than 7.5 MB of retained result evidence. This is a bounded architectural
suite, not a many-thousands-event campaign runner. The initial full run is about
5.3 MB; execution/replay intentionally costs more than decoding metadata.

Publication uses closed temporary files, fsync and atomic no-clobber linking.
Readers use bounded regular-file/no-follow checks and validate the content
filename before replay. Interrupted publication cannot replace a completed
artifact. Directory fsync is guarded on Windows; POSIX FIFO and privileged
symlink tests are isolated without skipping portable integrity checks.

Existing contracts, reference engine implementations, taxonomy and math report
bytes are unchanged. New public types use the existing forecasting-scoped typing
marker. Qualification covers Python 3.10/3.13, installed imports without source
path overrides, packaged-asset/source-byte equality, real write/read/re-execution
and positive/negative public typing. Local qualification does not claim that a
Windows runner or remote CI job was executed.
