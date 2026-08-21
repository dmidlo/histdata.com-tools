# Observation-process uncertainty propagation

The historical observation operator estimates how many latent market events
may be absent from HistData input. A seed-only reconstruction ensemble varies
the generated path conditional on one fitted retention value; it does not
measure uncertainty in the observation operator itself. The v2.5 contracts
therefore add a separate, evidence-bound operator-scenario axis without
changing retained v2.4 ensemble member IDs or path seeds.

Feed-boundary shape is a separate semantic axis governed by the
[feed-epoch transition policy](feed-epoch-transition-uncertainty.md); new
marked-Hawkes transition products retain the complete 3×3 crossed design.

No scenario claims recovered historical truth. Every output remains a
plausible counterfactual conditioned on the declared observation operator.

## Frozen scenarios

`derive_observation_uncertainty_scenarios()` reads the qualified joint
retention estimate and its interval from the synchronized feed-epoch
conditioning artifact. It creates exactly three scenarios:

| Scenario | Retention value | Interpretation |
| --- | --- | --- |
| `high_retention_low_infill` | qualified upper endpoint | least latent infill |
| `central_fitted_retention` | qualified point estimate | fitted central case |
| `low_retention_high_infill` | qualified lower endpoint | greatest latent infill |

The implementation never invents a multiplier around the fitted value. A
scenario identity binds the operator, conditioning record, feed epoch,
stratum ID/key/level, central/lower/upper estimates, selected endpoint,
support, evidence IDs, estimation bases, and provenance. Missing two-sided
support is machine-readable as `lower_only`, `upper_only`, or `unavailable`;
scenario construction refuses anything other than a supported, ordered,
two-sided interval.

`ObservationUncertaintyMemberV1` maps an existing ensemble member ID and path
seed to one scenario using a frozen balanced round robin. `scenario_id` is the
operator axis; `path_seed` is the conditional stochastic axis. Neither is a
substitute for the other.

## Cardinality and admission

For `r` retained observations and retention probability `p`, missing-event
count uses the verified negative-binomial failures parameterization:

```text
E[M]   = r(1-p)/p
Var[M] = r(1-p)/p²
```

`ObservationCardinalityEvidenceV1` publishes those moments, configured
missing/total-count quantiles, a one-sided Cantelli admission bound,
candidate-amplification bound, limit-exceedance probability bound, and an
explicit refusal risk for every symbol and scenario. The policy's admission
decision always uses the low-retention/high-infill endpoint. The planner uses
that same endpoint and admission quantile for adaptive windows; runtime builds
the same ensemble and refuses it before generation if any scenario exceeds
the generator or storage limits. An admitted job cannot switch to the central
case to avoid a worst-case refusal.

The candidate-amplification bound is `M/r`, matching the generator and storage
contracts. Both planning and runtime use every immutable input anchor,
including left-halo anchors, and assert that the runtime input count and
worst-case admission bound exactly match the resource preflight.

The current v1 release policy requires at least one path for each scenario and
fully retains all three scenario products. Aggregate-only scenario products
are not accepted by this release contract.

## Diagnostics and calibration

Every completed scenario/path cell reports aggregate metrics for:

- event count and mean interarrival;
- price path and spread;
- cross-currency triangle behavior; and
- downstream strategy sensitivity.

Refused or failed cells contain a bounded reason and no fabricated metrics.
`calibrate_observation_uncertainty()` requires complete scenario coverage on
both `validation` and an untouched `final_holdout`. The holdout has no
selection role. For every split and metric,
`ObservationUncertaintyDecompositionV1` reports between-scenario operator
variance, within-scenario path variance, their total, and the operator share.
The report explicitly declares that seed-only dispersion is not total
uncertainty.

`ObservationUncertaintyReportV1` is content addressed and is required by the
v2.5 conditioned-scorecard certification gate alongside powered qualification
and the validation-only Hawkes product-selection dossier.

## Planning, runtime, and publication

Create the frozen default policy through either public surface:

```sh
histdatacom reconstruction --json observation-uncertainty-policy \
  --output-directory work/observation-uncertainty
```

```python
from histdatacom.reconstruction import ReconstructionClient

policy = ReconstructionClient().create_observation_uncertainty_policy(
    output_directory="work/observation-uncertainty"
)
```

A new v2 plan selecting diagonal or full marked Hawkes must set
`observation_uncertainty_policy_path`. Planning binds the policy into the
experiment, configuration hashes, and execution graph; it also requires at
least three planned and retained ensemble members. Runtime proposal evidence,
candidate ledgers, final product benchmark evidence, and campaign product
entries expose the uncertainty ensemble ID, scenario ID/kind, and path seed as
separate fields.

Retained v2.4 point-estimate artifacts keep their original member identities
and replay semantics. If read through the legacy path, they carry an explicit
`v2.4-point-estimate-replay-not-v2.5-scenario-v1` marker and no v2.5 scenario
ID. They are never silently relabeled as three-scenario evidence or accepted
as a v2.5 uncertainty report.

All policy, ensemble, and report readers verify content-addressed names,
strong hashes, schema versions, derived identities, bounded payloads, and
cross-contract lineage.
