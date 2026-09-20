# Forecast mathematics and checked reports

Issue #580 adds independent, deterministic mathematical references and an
executable report boundary. It does not fit an ensemble, certify independence,
qualify a forecast model, establish empirical calibration, or approve use of a
holdout. It does not complete the benchmark-bank freeze in #557, archived
taxonomy reconciliation in #562, or research in #563/#567/#568.

## Installed use and compatibility

```python
from histdatacom.forecasting import (
    ForecastMathCheckedReportV1,
    ForecastMathVerificationV1,
    read_math_artifact,
    write_math_artifact,
)

verification = ForecastMathVerificationV1.current()
checked = ForecastMathCheckedReportV1(
    existing_score_or_report.to_json(),
    verification,
    thresholds=(0.0, 2.0),  # explicitly in this target's units
)
path = write_math_artifact(checked, artifact_directory)
replayed = read_math_artifact(path)
assert replayed == checked
```

Supported complete subjects are `ForecastScoreV1`, `ForecastScoreReportV1`,
`ForecastFeatureScoreV1`, `ForecastEngineScoreV1`, and
`ForecastEngineComparisonV1`. Feature and engine evidence remains embedded;
an internal calendar adapter does not replace that envelope. Unscored snapshots
and arbitrary metric dictionaries are refused.

All prior schemas, serialized identities, baseline implementation bytes and
engine versions remain unchanged. Old reports remain valid old reports; they
are not retroactively labeled math-checked. The additive
`forecast-math-checked-report` envelope binds their exact content and IDs.

The current verification receipt includes every required formula/version,
deterministic case and actual installed SHA-256 of the four new modules and
the existing contracts, scoring, feature-forecast and engine-runner modules.
Its constructor and reader execute the frozen cases again, rather than trust
stored expected/actual values, a path string, digest or `passed` flag. Checked
report serialization and persistence revalidate that current receipt and
recompute subject metrics. Missing/extra formulas, changed checks, stale code,
altered targets and forged metrics fail closed.
`subject_formula_ids` separates the formulas actually used by that subject
from `harness_only_formula_ids`; a point-forecast report cannot imply that it
executed or empirically qualified covariance weights or reliability calibration.

This complete envelope is the forecasting certification attachment: consumers
can bind its report ID, receipt ID, formula version and file digest. It does
not alter the unrelated frozen reconstruction certification policy or claim
an operational forecasting certification service exists.

## Formula coverage and independent constructions

The twelve named formulas in `FORMULAS` are versioned `1.0.0`. The current
receipt executes 31 fixed checks. Reference calculations do not import the
production scoring helper under test. Tests additionally use NumPy's solver,
array pairwise sums and statsmodels' intercept-only HAC covariance as independent
numerical implementations; these libraries are not new runtime dependencies.

| Formula | Reference and boundary |
| --- | --- |
| Covariance combination | Solve `Sigma*x=1`, `w=x/sum(x)`, variance=`1/sum(x)`; independently compare `w' Sigma w`, diagonal/analytic/dense cases and sum-to-one. |
| Equicorrelation | Variance ratio=`(1+(n-1)*rho)/n`; effective count is its reciprocal under equal variances/weights only. |
| Empirical CRPS | Transparent pairwise absolute-distance expression, including duplicated/constant draws. |
| Weighted CRPS | Alternative exact finite-support CDF-interval integral, including outcome tails. |
| Pinball and quantiles | Check loss, lower inverse CDF, and all minimizing quantiles when a probability lies on a mass boundary. |
| Binary Brier | `(p-y)^2` for Boolean outcomes, bounded by zero and one. |
| Binary log score | Stable `-log(p)` or `-log1p(-p)`; impossible realized events retain explicit positive-infinite loss status. |
| Fixed-bin reliability | Counts, mean probabilities and outcome frequencies in fixed bins; empty bins remain unavailable. |
| Surprise/value added | `D=Ahat-Chat`, `S=A-F`, `VA=abs(A-F)-abs(A-Ahat)`, raw numerical direction and declared polarity. |
| Point errors/aggregation | Signed, absolute, squared and normalized errors; independent MAE/RMSE/median and value-added aggregation. |
| Distribution point | Probability-weighted mean or lower weighted median recomputed from retained masses, not the reported point. |
| Paired Bartlett HAC | Ordered paired loss differences, explicit lag weights, finite-sample variance correction and conditional normal interval. |

Covariance must be exactly symmetric and positive definite. Scaled Cholesky
pivots at or below `1e-12` refuse near-singularity; zero/negative variances and
underflow refuse. No unannounced ridge, pseudoinverse, negative-weight clipping,
weight fitting or shrinkage selection is performed. Negative unconstrained
weights are legitimate mathematical outputs, not recommended trading weights.
For equicorrelation, admissible negative correlation can produce effective
count greater than model count. The singular perfect-cancellation boundary
records zero relative variance and `effective_count=None`, meaning positive
infinity under those exact assumptions. It is not empirical independence.

Missing cells, invalid probabilities, nonfinite values and arithmetic overflow
are refused. A mathematically finite CRPS may be refused by the pairwise
reference when an intermediate difference is unrepresentable; the alternative
CDF construction may still succeed. No report silently substitutes one method
after a failed method or calls that failure a zero score. Tiny rounding error
in a nonnegative empirical CRPS is bounded before conversion to zero.

Probability masses follow the existing distribution contract's `1e-12` sum
tolerance; masses are not silently renormalized. Exact zero/one binary
probabilities are not clipped: a wrong deterministic event has `log_score=null`
and `log_score_status=positive_infinity_impossible_event`, distinct from missing
evidence. Quantile and exceedance requests are explicit and version-bound.
Exceedance means strictly greater than the threshold, including at ties.

## Actual outcome, units and direction

The checker independently selects the first non-revision actual, requested
revision sequence, or latest admissible named consensus from retained records.
Future consensus is selected only for an explicit evolution target. Predicted
consensus points are independently recomputed from their embedded support.
Availability and publication clocks are checked as exact integers, not float
approximations. Later revised values never substitute for the first release.

Normalized scores require the existing exact, positive, named ex-ante scale
receipt committed in the forecast. No pooled cross-unit denominator is fitted.
Paired comparisons require identical target, unit, scale, base, cutoff and
outcome evidence per event. A bare sign is numerical, not economic-good/bad.
`direction_polarity` can explicitly be `1` or `-1`, with a retained
`direction_label`; reports preserve both raw error sign and mapped sign. Zero
stays zero. The label is a caller declaration, not proof of economic meaning,
currency impact, execution feasibility or profitability.

## Paired comparison and limits of inference

`ForecastPairedLossReportV1` accepts two bounded collections of complete scores
and an explicit `ForecastHacPolicyV1`. Version one supports homogeneous
first-release-actual tasks only. Each column must share model implementation,
horizon, series/indicator, reference semantics and complete stable source-series
semantics, including economy, event family, frequency and publisher/adapter.
Duplicate events, missing counterpart predictions, heterogeneous families and
ambiguous simultaneous outcome clocks are refused rather than dropped.

Rows are ordered by first-release **availability**, with event-index lags, not
calendar-day lags. The difference is baseline loss minus candidate loss; a
positive mean means smaller candidate loss. Either raw absolute or squared
loss is declared. Actual horizon overlap in event-index units must be declared
as `horizon_steps`; bandwidth must be at least `horizon_steps-1`. The API does
not infer this geometry from a calendar-day horizon label.

The fixed estimator is
`gamma(k)=sum((d[t]-mean(d))*(d[t-k]-mean(d)))/n`,
`LRV=n/(n-1)*(gamma(0)+2*sum((1-k/(L+1))*gamma(k)))`,
and `SE=sqrt(LRV/n)`. It reports a normal-approximation 95% interval and two-sided
normal p-value. It is not a universally valid finite-sample Diebold–Mariano
test; no Harvey–Leybourne–Newbold correction is implied. Zero long-run variance
has explicit no-inference status, not an infinite statistic or manufactured
significance. Effect size is retained even in that case.

Weak stationarity, appropriate lag length, overlap geometry, missing-period
coverage and adequacy of asymptotics are **conditional assumptions**, not facts
verified by a policy string. The default minimum count of 20 is a conservative
software guard, not proof that 20 observations are enough. An explicit minimum
can be 8–128; passing it proves no power/calibration/validity claim. This contract
does not prove parameter preregistration or control multiple-comparison/model
selection. Those governance and empirical checks remain #563/#568 work. No
holdout can select parameters merely because a report has a small p-value.

## Bounds, persistence and validation

Numerical limits are 16 covariance dimensions, 512 probability support points
or draws, 4,096 reference loss observations and 128 HAC lags. Checked reports
retain at most 128 complete scores, 32 quantiles and 32 thresholds. New wire
envelopes have an 8 MiB final-byte limit, depth 64, bounded collections and
finite JSON numbers; limits apply to expanded content and final identity
overhead. Composition can refuse an individually valid legacy artifact that
does not fit the new envelope. No silent truncation or sampling occurs.

Files are content-addressed, canonical and replayed before publication.
Temporary-file flush/fsync followed by atomic no-clobber hardlink publication
preserves existing evidence on interruption/conflict. Reads are bounded,
nonblocking, no-follow regular-file reads with inode checks; symlink, FIFO and
directory inputs are refused. This is same-installed-code/runtime replay, not
cross-platform bitwise certification or a security sandbox.

Tests cover all named edge cases, independent numerical implementations,
production-metric/distribution-point mutations, complete engine/feature/legacy
subjects, exact formula completeness, stale code, output tampering, target
selection, source heterogeneity, resource bounds and durable artifact replay.
