# Public decision attribution

`histdatacom.attribution` implements the public contract and synthetic
conformance scope of [#719](https://github.com/dmidlo/histdata.com-tools/issues/719),
under the [owner's public/private classification](https://github.com/dmidlo/histdata.com-tools/issues/719#issuecomment-5363602471).
It does not load private weights, fit a model, access historical/protected
observations, run a strategy bank, monitor a broker, or deploy an action.
Concrete private explanations and empirical global reports remain private.

Every explanation/report retains the exact nonclaim:

`model attribution != causal effect in the historical market`

Hash equality establishes identity, not authenticity, availability, source
rights, independence, model accuracy, or an undisclosed causal design. A supplied
scalar is not an observed market value merely because the reference model was
successfully evaluated on it. `declared_external_unverified` values cannot acquire
executed-reference status without the actual admitted model/explainer replay.

## Object boundaries and identity

`DecisionAttributionV1` retains the exact policy, reference/model identity,
feature snapshot, preprocessing/projection/lineage references, background,
explained dimension/unit/class/horizon, raw output, baseline, feature/group/pair
contributions, numerical residuals, ambiguity diagnostics and generation clock.
Class and horizon may be explicitly inapplicable (`None`), not silently guessed.

`ExplanationPolicyV1` and `ExplanationPolicyRegistryV1` freeze method/version,
background-handling convention, group structure, reporting cut, absolute and
relative tolerances, approximation/work budget, instability threshold and
correlated-feature rule. A reused method/version with changed contents is refused
within one registry. A policy/background/group/budget/semantic change creates a
new content identity. There is no mutable current-explainer setting.

An explanation can be made later than its target decision. The target feature
availability is checked against the target cutoff, while policy/background
declaration and explanation generation have distinct clocks. Later explanation
creation never changes the original `ReferenceDecisionV1` action identity.
An externally declared clock is still a declaration, not proof of historical
knowledge.

Raw score, calibrated score and final action are not one value. The immutable
action artifact retains all eight execution stages. An exact registry sidecar
resolves the attribution to its original action/deployment/account/policy stack;
the raw attribution object alone does not claim a deployment occurred. Missing
action/calibration linkage is explicit, not an invented identity calibration.

## Executed reference mathematics

The admitted model is a bounded multilinear polynomial with at most eight
selected coordinates and 128 distinct terms. `explain_reference` actually
evaluates every required coalition over every retained background sample.
No callback, arbitrary output map, model hash or report name is accepted as proof
of model execution. The exact reference is not a production SHAP implementation.

For coalition `S`, nonmembers take values from the retained background sample.
Uniform averaging over its explicitly retained samples defines the background
distribution. Feature Shapley values use the factorial coalition weights. Exact
`Fraction` arithmetic verifies conservation before finite-float serialization.
The issue's model `3*x1 + 2*x2 + 4*x1*x2`, zero baseline and `(1,1)` point gives
`(5,4)` and output `9`; the tests independently compute coalition and permutation
oracles rather than comparing one invocation against itself.

Feature and group float conservation are checked separately using the policy's
frozen `absolute_tolerance + relative_tolerance * abs(f(x)-f0)` bound. The public
reference refuses tolerances above `1e-6`; fixtures use `1e-12`. Unrepresentable
or nonzero-underflowed outputs refuse rather than becoming infinity or false zero.
This is explicit reference arithmetic, not empirical approximation certification.

The retained pair quantity is the **pair dividend at the empty coalition**:
`v({i,j}) - v({i}) - v({j}) + v(empty)`. It is not a full SHAP interaction matrix
and must not be added again to the Shapley values. Structured feature pairs avoid
ambiguous concatenated names. The two-feature fixture's pair dividend is `4`.

Evaluation budgets preflight the full background × coalition × sibling-ablation
work before the first model evaluation. The raw output reuses a retained full
coalition value. At most 32 backgrounds and 65,536 model evaluations are admitted.
Exact reader replay has the same bounded work; there is no stale verified cache.

## Grouping, redundancy and logical width

The taxonomy covers tick/activity, bar/indicator, triangle, calendar, forecast,
positioning, strategy, synthetic flow, broker style, regime, uncertainty and
missingness planes. All seven bar timeframes are explicit. Event-backed triangle
features need not pretend to be OHLC bars. Strategy metadata retains the exact
FXES-001…FXES-1000 namespace, declared family and input-dependency class, plus the
population grouping level. This describes metadata, not proof that 1,000 compiled
strategies exist or have produced historical responses.

Groups may form a hierarchy, but each reporting cut must partition the complete
selected feature set exactly once. Missing/cyclic ancestors, overlapping selected
groups and selecting both an ancestor and its child refuse. Direct group-coalition
Shapley and sums of singleton Shapley values have different method identities.
They can yield different group values; the software never silently equates them.

`AttributionFeatureShardV1` retains up to 256 feature definitions. A taxonomy binds
up to 64 exact shard references and 16,384 logical features, with a 128 MiB
aggregate shard budget. Resolution checks complete inventory, catalog/content
identity, exact count, duplicates and named selection. The synthetic suite
actually resolves 10,001 declarations across 40 shards. This is **not** a
historical 10k-column physical projection benchmark or completion of #681/#689.
The selected exact explainer remains independently limited to eight coordinates;
an oversized explanation must refuse or use a separately identified admitted
method, not silently truncate the taxonomy.

Background-wise attribution range and exact sibling ablations measure credit
stability. Exceeding the frozen threshold yields `attribution_nonidentifiable`;
reports present declared groups and do not invent a precise singleton ranking.
Background Pearson participation ratio is computed using the existing exact
`member_correlation_dimension` arithmetic. Constant or insufficient background
support is explicitly unavailable. This metric measures dependence in the
retained background, not historical market independence.

## Transformed, latent and imputed inputs

Snapshots explicitly name raw, transformed, latent-component or grouped-source
space. Imputed numerical values require a retained matching missingness mask.
They are not relabelled observed merely because the matrix is numeric.

`AttributionMappingV1` admits only a one-to-one invertible diagonal affine
coordinate mapping with exact units and identity. `MappedAttributionV1` replays
every raw/transformed target and background pair, including unchanged cutoff,
session, universe and imputation/availability state. The transformed snapshot
must bind this exact preprocessing identity. Raw and transformed backgrounds
must retain the same role and declaration time; coordinate mapping cannot
reclassify protected samples or backdate their declaration. Under this
coordinate-preserving coalition mapping the contribution transfers by the
explicit name mapping, not by multiplying its value by a scaler coefficient.
A PCA/latent-space attribution cannot enter this raw mapping; there is no
general inversion claim.

## Decision decomposition

`ReferenceDecisionPolicyV1` executes a small transparent fixture policy, not a
private `ModelDecisionPolicyV1`, account simulator or live deployment registry.
`ReferenceDecisionV1` retains these exact stages:

1. Raw model score, explicitly not an action.
2. Affine calibration.
3. Long/short/dead-band thresholds and a contemporaneous expected-cost gate.
4. Fixed sizing, turnover hysteresis and position clipping.
5. Symmetric portfolio cap projection.
6. Symmetric margin cap projection.
7. Validity pass/refusal.
8. Synthetic execution accept/reject.

Every stage retains input/output, distinct reason and active constraints; readers
re-execute all equations. The registry also reconciles raw score against an
actually replayed model/snapshot. A positive score can therefore end at zero due
to cost, risk, validity or execution without being described as a neutral model
prediction. This is a reference constrained policy, not a claim to implement the
full private portfolio, currency, jurisdiction, financing or fill engines.

## Native-backed counterfactual conformance

The closed `synthetic-native-triangle-regeneration.v1` intervention changes only
two primitive synthetic FX prices. It regenerates EURGBP from EURUSD/GBPUSD,
constructs real `SyntheticEventV1` fixtures, invokes existing exact native
`synchronized_triangle_tuples`, and uses the existing triangle residual
implementation. All dependent midpoints/residuals and exact event/tuple identities
are retained. These are synchronous zero-spread fixtures, not executable market
opportunities, liquidity estimates or empirical bid/ask qualification.

`NativeTriangleInterventionV1` replays full before/after native evidence and counts
its parsed contents in the aggregate envelope budget. Unit, universe, time,
availability and fixture-session constraints are explicit. All nonempty external
context is refused: a caller-supplied value cannot establish independence from
the perturbed prices. This closed adapter admits only its four regenerated
triangle fields; macro/context interventions need a separate native generator.
Session labels are declared fixture context, not a historical exchange calendar.
`CounterfactualAttributionV1` requires both sides to have supported, executed
reference models and checks output difference from their replayed results.
Declared external values cannot pass this positive proof boundary.

Other interventions remain unsupported unless separately versioned and admitted.
There is no arbitrary independent-column editing path or callback-as-certificate.

## Experiment and action linkage

`ExplanationPlanV1` is noncircular: exact model, explainer, background,
preprocessing and projection references plus declaration time, never an
experiment/result/action ID. `explanation_plan_fixture` creates an exact method
configuration specification for the existing #718 `MODEL_CONFIGURATION` inventory.
This additional reference must be frozen before cited scientific results. The
bridge compares its complete native payload, not a fixture label alone.

`AttributionRegistryV1` consumes the actual canonical `ExperimentRegistryV1` and
reconciles exact model/preprocessing/projection references, plan configuration,
result production clocks and any original action. Existing #718 external records
remain declared-unverified there; the new sidecar independently verifies only its
admitted reference model. No existing #718 v1 meaning, field or reader changes.

Sidecars have two roles. `scientific_result` requires attribution generation
before production of each cited result. `posthoc_explanation` may cite an already
retained result, action, or experiment alone; its later generation/link clock does
not rewrite original evidence. A method/background/plan must still precede any
cited scientific result. Action-only and experiment-only posthoc links do not
manufacture result IDs. Exact stack mismatch, missing ancestors and impossible
ordering refuse. Reverse dependency queries return the retained attribution,
experiment/results and action links, not undisclosed external work.

The inline existing-registry bridge is deliberately limited to **64 KiB** native
JSON within the aggregate envelope. It does not claim to embed every otherwise
valid 8 MiB #718 registry. Larger private registries remain explicit external
references until a separately admitted reference-driven bridge exists.

## Stratified reports

`build_attribution_report` derives means/medians of absolute group contributions,
signed distributions, HHI and effective contributing groups from retained
attributions. Pair, regime, era, domain, support, session and broker-state labels
remain explicit. Exact model/policy/preprocessing/projection and feature/output
semantics must match across comparisons. Within a stratum the background must be
identical. Different strata may use different retained backgrounds; every contrast
explicitly records `background_changed`, so that difference is not hidden as
feature drift. All retained stratum contrasts are emitted or the bounded report
refuses; results are not selectively filtered by sign.

For nonnegative mean absolute contributions, normalized masses `a_g` define
`HHI=sum(a_g^2)` and `N_eff=1/HHI`. All-zero mass yields unavailable concentration,
not infinity. Exact intermediate arithmetic avoids overflow/cancellation.
Unavailable, unverified and nonidentified explanation counts are separate.
Repeated attribution IDs refuse; declared distinct unit counts are not proof of
independent historical samples. Domain labels, including observed/synthetic/live,
are declarations in these fixtures, never a claim of real domain qualification or
an active live drift monitor. Reports enforce both causal and independence
nonclaims and recompute summaries during deserialization.

## Public API, storage and qualification

Public classes/functions are exported from `histdatacom.attribution`, with scoped
`py.typed`. The wire/math/registry layer is stdlib-friendly; optional native
intervention imports reuse existing package owners. No new scientific dependency
or private schema is introduced.

Artifacts use canonical finite ASCII JSON, exact scalar types, strict duplicate/
unknown-field rejection and content-addressed versioned envelopes. General
limits are 8 MiB, 131,072 expanded nodes, depth 20, 4,096 items and 65,536
characters per string. Embedded native evidence is charged after parsing as well
as within its outer object. Resource limits describe this public conformance
surface, not full production model/background capacity.

`read_attribution_artifact(path, Type)` checks regular/no-follow/nonblocking
bounded reads, canonical filename/content and fresh computational replay.
`write_attribution_artifact(artifact, existing_directory)` replays before fsynced
no-clobber publication. Exact repeats are idempotent; conflicting historical
bytes refuse. POSIX directory durability and atomic publication do not constitute
a hostile same-user filesystem sandbox. A raced FIFO cannot block the reader.

`assets/requirements_v1.json` maps every public requirement to its API and
synthetic conformance. It keeps implementation/execution separate from private
scientific qualification. This is scoped #691/#525 evidence, **not completion of
those global programs**, #605 wide-corpus closure, or private #714/#716 operations.
