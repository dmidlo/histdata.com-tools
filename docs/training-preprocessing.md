# Leakage-safe preprocessing and dense model views

Issues #655 and #697 add an explicit fit/apply boundary over the native wide
training view. This is an additive MINOR capability with versioned v1 contracts,
not a package release or a claim to finish the broader training substrate.
The evidence view preserves original nullable values, states,
clocks, dependency bounds and lineage. A dense model value is a derived
representation, never a recovered observation or proof of historical availability.

The public module is `histdatacom.data_quality.training_preprocessing`.
Callers supply a retained `TrainingWidePlanV1` and a complete
`TrainingTemporalSplitV1`, not an arbitrary feature array or asserted weights.
Every public fit, application, projection and retained-fit reader freshly
replays its admitted inputs. Unselected columns, source bindings, products and
current provider rights remain subject to admission.

## Fit modes and ownership

| Mode | Admitted fitting information |
| --- | --- |
| `train_fit` | An explicit, nonempty set of whole TRAIN evidence units in one frozen fold. Validation and test units never fit parameters. |
| `rolling_pti` | TRAIN-only, predeclared GRID/BAR decision coordinates in the frozen rolling window through one explicit cutoff. Used added-feature cells must pass native normalized availability and dependency-clock checks. |
| `fixed_external` | An independently retained and freshly refitted native calibration plan, with a known cutoff and original observed anchors disjoint from application sources. No caller coefficient arrays. |
| `none` | No learned parameters; only explicitly versioned, semantically admitted stateless dense operations. |

All assignments cover the complete native ownership map in chronological
TRAIN/VALIDATION/TEST order, including empty intervals. Selecting a fit subset
does not redefine units or move synthetic siblings across partitions. A fold
identity is part of fit lineage; an object fitted for a different fold is not
silently reused. Hyperparameter selection and private model fitting are outside
this public reference interface.

The enforceable boundary is the declared policy and its exact TRAIN-derived
fit state. Native replay rejects a resealed selector result altered using
protected label outcomes when that result disagrees with the unchanged
declared variance/selection policy. The API cannot infer whether an external
person chose a new declared threshold after viewing protected outcomes; it does
not certify hidden policy-selection intent or preregistration. Experiment
governance must freeze policies and keep any policy search within training-only
inner folds. Merely constructing a new plan is not evidence that this happened.

`train_fit` is experiment fitting, not a claim that parameters were available at
earlier TRAIN event times. Rolling fitting does not repair the legacy quote
spine's unknown historical availability: only normalized added feature clocks
are admitted. Current admitted families for that scope are native vintage,
calendar, forecast and positioning features on GRID/BAR coordinates. Raw quote,
broker and synthetic-spine fields do not become causal features through a
normalized label. A later revision cannot update an earlier cutoff's fit.

One rolling fit applies to its exact decision cutoff. To materialize several
cutoffs, retain separate fit identities. All retained sources are still audited,
but numerical application executes only the prefix through that cutoff,
including eligible prior-state donor history, then emits the exact cutoff.
A valid future extreme value cannot overflow an earlier cutoff's transform.
Future-extension comparisons hold the
declared ownership/fold and semantics fixed; changing that experiment, numerical
runtime or schema is not an identity-preserving operation.

## Evidence mass and missingness

The new descriptive policy is
`equal-complete-unit-retained-member-native-row.v1`: each native scientific
stratum has one mass unit per evidence unit, divided equally among its complete
retained-member roster and then among complete native rows. A member binds both
its actual run and member identity. Same-label members in distinct runs are not
merged; repeated windows within a run/member do not create independent history.
Repeated logical/grid copies of one native row share that row's mass. Column
tiles do not multiply it.

Observed and reconstruction strata, and different exact generator/configuration,
constraint and delivery/broker identities, are not pooled into one fit.
The denominator is the complete declared source and native retained-member
inventory, not all hypothetical or refused generation attempts in an absent
campaign. Declared but unavailable members and selected-away rows retain their
withheld mass. This policy is not the failed #607 study's empirical weighting or
calibrated scenario probabilities.

Normalized rolling fitting uses
`equal-training-unit-available-prefix-member-row.v1`, implemented over the
declared unit/grid-coordinate prefix and declared symbols, not future quote or
member counts. Missing anchors keep their mass withheld and have no fabricated
row ownership. A grid cutoff crossing the retained anchor's evidence-unit
boundary refuses normalized fitting, even when both units belong to TRAIN.
An older TRAIN anchor cannot move a later protected cutoff into fitting.
Null feature support remains visible. Weighted statistics
condition on their reported usable support; they do not claim missing mass was
observed or turn a partial roster into complete success.

The `weight` and `weight_policy` on an exported row describe the complete native
source inventory; they are not an online-prefix fitting weight. The fitted
artifact's exact membership masses and `fit_support.mass_policy` separately
record the policy actually used for parameter estimation. A future source
extension can change descriptive full-inventory mass while leaving the
normalized prefix fit and numerical output unchanged.

Original native JoinState, nullable value, source identifiers, information mode,
availability/dependency clocks, age, meaning and scalar dtype are retained.
Every dense or transformed-nullable record includes `input_schema`, the same
complete selected-input descriptors as the evidence view's `schema`. It keeps
native units, dtype, meaning and state-category ontology even after a transform
replaces or combines the input columns. Each output's `native_dependencies`
resolves to that mapping; native input units are not assertions about the units
of a scaled or projected output.
Unknown/outage/refusal is distinct from a confirmed zero. Native state labels
can be selected explicitly as `state.<native-column>` categorical features;
timestamps, row IDs, symbol IDs and arbitrary labels are not automatically
numeric predictors.

## Executable reference transforms

The closed fitted families are `zscale`, `robust_scale`, `minmax`, `winsorize`,
`quantile_rank`, `categorical`, `median_imputer`, `variance_selector`,
`correlation_pruner`, `pca`, `svd`, `whitening`, `population_reduction` and
`linear_embedding`. The last two are explicitly neutral linear PCA reference
transforms, not a claim to support arbitrary proprietary or nonlinear learners.
PCA, SVD, whitening, population reduction and linear embedding require NumPy
in the numerical environment, for example through `histdatacom[models]`.
Projection kernels import it lazily; without it they fail with an import error,
not a silently substituted transform. Scalar reference transforms use the
standard library and do not require that numerical extra.

Each step retains its version, options, input/output schema, exact fitted
parameters, support, environment and parent lineage. Numeric scalar reductions
use exact rational weights. Quantiles use the weighted inverse CDF; population
variance divides by the admitted weight sum. PCA-family numerical execution
binds its runtime and refuses unsupported rank or tied selected eigenspaces.
An exported output's `lineage_id` additionally binds the full application plan,
retained fit, mode and cutoff; `pipeline_lineage_id` retains the transform-chain
identity. Unchanged numerical prefix values therefore do not imply identical
full-source or application-view identities.
The public categorical fit derives its ontology identity from the native input
semantics and freezes a TRAIN vocabulary; unknown and missing categories have
separate outputs. An explicitly supplied ontology must match that derived
identity. Protected categories do not expand the vocabulary.

`options_json` is a canonical JSON object; unknown options refuse. Rational
options use canonical integer or fraction strings, not approximate JSON floats.
The closed options and defaults are:

- `zscale`, `minmax`, `median_imputer`, `quantile_rank`: `{}`.
- `robust_scale`: `{"mad_scale":"1"}`; no Gaussian calibration is implied.
- `winsorize`: `{"lower":"1/20","upper":"19/20"}`.
- `variance_selector`: `{"threshold":"0"}`; variance must be strictly greater.
- `correlation_pruner`: `{"threshold":"19/20","variance_threshold":"0"}`;
  preserve input-column priority and prune absolute weighted correlation at or
  above the threshold. Unsupported pairs do not establish correlation.
- `categorical`: `{}` at the public native boundary, which binds the ontology.
- `pca`, `svd`, `whitening`, `population_reduction`, `linear_embedding`:
  `{"eigen_tolerance":"1/1000000000000","n_components":1}`. SVD uses
  uncentered second moments; the other four use centered covariance. Whitening
  divides by the square root of the retained eigenvalue.
- `confirmed_zero`: `{}`. `prior_state` requires `{"max_age_ns":N}` with a
  nonnegative integer `N`; there is no implicit carry-age default.

`prior_state` may carry only a prior original, actually clocked persistent state
within the same realization and its declared age bound. Event occurrences,
cutoff-specific snapshots and closed bars are not persistent-state donors.

For `train_fit` and `rolling_pti`, fitting uses only the selected fit-membership
rows, including any inner-fold subset or rolling window. Application freezes
that exact TRAIN donor universe. Every requested TRAIN target remains in the
view, but it can carry only from fit-member rows: other TRAIN rows excluded by
the fold or window cannot become donors. VAL/TEST targets may additionally use
causal, original VAL/TEST history; those protected rows never donate into TRAIN
targets or update learned coefficients. All carries must still satisfy the
native realization, source/availability clocks, dependency cutoff and age
checks.

`none` and independent `fixed_external` application instead use their own
supplied causal original history. The retained prior-state kernel JSON and
each carry proof record the versioned `donor_policy` identity:
`frozen-fit-train-donors-plus-causal-nontrain.v1` for `train_fit`/`rolling_pti`,
or `supplied-application-history-causal-originals.v1` for the other modes.

`confirmed_zero` applies only to native confirmed-absence semantics. The native
calendar path can establish absence for a retained `CANCELLED` occurrence with
no release timestamp at that exact scheduled time. Its evidence value stays
null: this is not a zero economic release value or an ordinary quiet period.
Dense zero additionally requires known source/availability clocks no later than
the row decision and a dependency interval ending by that inclusive cutoff
(exclusive end no later than decision plus one nanosecond), even for ex-post
inputs. A cancellation learned later cannot fill the earlier row;
that cell remains unresolved and a required-dense export refuses it. A median
imputer uses retained TRAIN-fitted parameters and explicit support. An imputed
value is never silently relabeled observed or reused as fresh donor evidence.

Dense outputs retain masks, reasons, original nullable values, ages, policies,
support and step lineage. `require_dense=True` refuses unresolved cells; it does
not coerce unsupported data into zero. Callers can request evidence or an
explicitly partial projection instead.

The `masks` mapping is true for an originally missing selected input, not true
for an observed value. `originals` retains that input's native state, reason,
age and provenance after transformation. Each output's schema records its
native dependencies, including when a projection mixes multiple input columns.
Reported support is descriptive usable evidence, not calibrated confidence.

## Typed use

```python
from histdatacom.data_quality.training_preprocessing import (
    PreprocessingFitMode,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
    apply_training_preprocessing,
    evidence_records,
    fit_training_preprocessing,
    materialize_training_evidence_view,
    preprocessing_records,
    read_training_preprocessing_fit,
    write_training_preprocessing_fit,
)

plan = TrainingPreprocessingPlanV1(
    wide_plan=wide_plan,
    split=complete_split,
    fold_id="fold-1",
    fit_mode=PreprocessingFitMode.TRAIN_FIT,
    fit_unit_ids=tuple(sorted(train_unit_ids)),
    columns=("market.tick.EURUSD.bid",),
    steps=(TrainingPreprocessingStepV1(
        "scale", "zscale", ("market.tick.EURUSD.bid",),
    ),),
)
evidence = materialize_training_evidence_view(plan)
original_records = evidence_records(evidence)
fit = fit_training_preprocessing(plan)
path = write_training_preprocessing_fit(fit, output_directory)
retained = read_training_preprocessing_fit(path, expected_fit_id=fit.artifact_id)
view = apply_training_preprocessing(retained, expected_fit_id=fit.artifact_id)
records = preprocessing_records(view)
```

The example assumes independently retained native plan/split inputs. It does not
create acquisition authority, qualify a corpus or establish empirical model
utility. Expected fit IDs select the intended artifact; hashes and constructors
alone are not evidence that fitting excluded protected data.

For an unscaled view, use `fit_mode=PreprocessingFitMode.NONE`, empty
`fit_unit_ids` and empty `steps`, then call
`materialize_training_preprocessing_view(plan, require_dense=False)`. The same
entry point admits only the named stateless operations in `none` mode; it cannot
fit a learned transform implicitly. A `TrainingPreprocessingViewV1.records`
property is a detached snapshot, not a fresh verification. Use
`preprocessing_records(view)` before relying on a previously returned view.

## Bounds and evidence scope

Native wide admission retains its existing 512-row/32,768-cell bounds and at
most 4,096 columns. A preprocessing pipeline has at most 16 steps, 64 selected
input columns and 256 output columns, still within 32,768 cells. Numerical
projection kernels additionally cap input width at 16 and input cells at 8,192.
The native adapter retains at most 32,768 distinct original observed anchors
for actual external-overlap comparison and 4,096 denominator records. Larger
inputs refuse rather than truncate. These are payload/work bounds, not OS memory
reservations or guarantees against concurrent resource exhaustion.

Fit artifacts use immutable content-addressed canonical JSON, exclusive
publication and fresh native refitting on read. Required no-follow filesystem
protections are explicit platform prerequisites; unsupported platforms refuse.
There is no generic pickle loader, fitted-parameter injection or lifetime cached
authority.

Synthetic native fixtures and neutral fixed downstream learners can test mask
handling, transformations and leakage canaries. They do not establish profitable
trading, empirical superiority, private-model validity, historical provider
availability, the complete training substrate or a published-corpus certificate.
The scoped requirement inventory keeps implementation, execution and evidence
separate for #525/#691 and distinguishes evidence, raw, transformed and imputed
views for #681; it does not mark those parent issues globally complete.
