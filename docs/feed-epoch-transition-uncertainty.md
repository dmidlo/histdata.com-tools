# Feed-epoch transition uncertainty

`histdatacom.feed-epoch-transition-policy.v1` prevents an uncertain feed
boundary from being treated as a stable epoch or as one silently assumed
linear observation process. A transition is a boundary-time uncertainty, not
a fitted third epoch and not a volatility regime.

## Frozen scenarios

Let the boundary interval be half-open, `[T0, T1)`, let `t` be the product
decision time, and let `pL` and `pR` be retention estimates from the qualified
adjacent epoch strata. The historical bridge weight is

```text
w = (t - T0) / (T1 - T0)
```

The policy evaluates exactly three alternatives:

| Scenario | Left weight | Right weight | Meaning |
|---|---:|---:|---|
| `left_persistence` | 1 | 0 | the prior feed behavior persists through the uncertain interval |
| `linear_bridge` | `1-w` | `w` | the previous elapsed-time interpolation |
| `early_right_adoption` | 0 | 1 | the later feed behavior applies throughout the interval |

No protected transition product is fitted. Every scenario uses only the two
already-qualified adjacent operator strata. The scenario identity binds the
policy, feed definition, observation operator, transition label and boundary,
the exact interval, adjacent epoch and stratum IDs, operator evidence IDs,
symbol scope, information mode, and weights.

## Independent uncertainty axes

Feed-transition shape and observation-operator endpoint uncertainty are
different questions. The retained marked-Hawkes ensemble therefore covers the
complete `3 transition × 3 observation` cross-product. Observation scenarios
cycle inside blocks of three members; transition scenarios select the block.
The default retained ensemble for this product has nine members, with at least
one path seed in every crossed cell.

Planning computes missing-cardinality and resource bounds from the minimum
qualified lower retention across all three transition scenarios. The runtime
must reproduce that preflight bound, then selects the scenario from the
semantic member ordinal. Candidate ledgers, generation scenarios, proposal
manifests, and validation evidence expose:

```text
feed_epoch_transition_policy_id
transition_scenario_id
transition_scenario_kind
transition_boundary_id
```

Stable-epoch products expose these fields as not applicable. Older v2.4
point-estimate products retain their original identity and are not relabeled as
transition-scenario products.

## Evaluation and certification

`evaluate_feed_epoch_transition()` requires completed validation and final
holdout cells for every transition-by-observation scenario pair and the
predeclared minimum number of distinct path seeds per crossed cell. The
row-free diagnostics cover:

- missing counts and their uncertainty;
- adaptive boundary count, refusals, and resource work;
- interarrival timing and mark transitions;
- path variation and spread;
- synchronization age;
- triangle residuals and projection burden; and
- strategy dispersion.

Each endpoint scenario is compared with the linear scenario under predeclared
absolute and relative materiality tolerances. The report can produce only one
of three certification states:

- `qualified_linear_sensitivity_negligible`: retain linear and publish the
  supporting evidence;
- `qualified_multiple_transition_scenarios_required`: retain the complete
  scenario set because sensitivity is material; or
- `transition_support_limited_or_refused`: publish the limitation or refuse
  the affected support window.

The final holdout has no selection role. Missing scenario cells, absent
adjacent strata, invalid decision times, or mismatched feed-definition/operator
identities cannot be converted into a stable-epoch claim.

## Information boundary

The installed policy is ex-post by default because adjacent-epoch evidence can
include information unavailable at the historical decision time. Ex-ante use
is rejected unless the policy binds a separate point-in-time-valid prior
artifact. Merely knowing the later fitted epoch is not such a prior.

Create the content-addressed default policy with:

```sh
histdatacom reconstruction feed-epoch-transition-policy \
  --output-directory artifacts
```

Supply the resulting path as `feed_epoch_transition_policy_path` beside the
observation-uncertainty policy in a marked-Hawkes v2 plan. Planning verifies
the content hash, policy ID, schema, complete crossed-member capacity, and
artifact-graph binding before execution.
