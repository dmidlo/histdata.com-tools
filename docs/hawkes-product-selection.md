# Marked-Hawkes product-selection dossier

Powered qualification and product selection are separate decisions. A powered
qualification dossier may declare both
`histdatacom.marked-hawkes.diagonal_self_excitation` and
`histdatacom.marked-hawkes.full_self_cross_excitation` reconstruction-eligible.
The v2.5 product may bind either engine only through a replay-verified
`histdatacom.hawkes-product-selection-dossier.v1`.

The dossier first replays the powered hard-gate decisions. If exactly one
candidate is reconstruction-eligible, that candidate is selected immediately
and the rejected candidate's failed gates are retained. The pairwise decision
surface is explicitly recorded as not reached; an ineligible model is never
treated as a valid comparator. This gate-only path is derived from two
content-addressed inputs:

1. a predeclared `histdatacom.hawkes-product-selection-policy.v1`; and
2. the powered qualification dossier.

If both candidates remain eligible, the dossier is instead derived from three
content-addressed inputs:

1. a predeclared `histdatacom.hawkes-product-selection-policy.v1`;
2. an exact paired `histdatacom.hawkes-validation-comparison.v1`; and
3. the powered qualification dossier that admits both candidates.

For each candidate, the selection dossier also freezes the qualification
decision's residual-report identities. The set must contain exact
`raw_proposal` Hawkes diagnostics and the common simulation-predictive
`benchmark_candidate` diagnostics. The dossier separately derives one
`final_constrained_product` report per candidate from the paired validation
observations after carving and reconciliation. Each final report binds the
coordinate IDs, final constraint-set IDs, and mean/q95 event, time, mark,
cross-series, path, tail, failure/refusal, and diversity metrics. Selection
refuses a candidate whose analytic or final-product report is missing,
underpowered, stale, or substituted across diagnostic stages.

No final-holdout metric is accepted. Every comparison coordinate binds the
same validation window, degradation scenario, seed, anchor set, adaptive
partition, final constraint set, and early/transition/modern era. Each
coordinate contains exactly one observation for each candidate.

## Frozen decision surface

Every candidate retains raw and post-reconciliation triangle residuals,
projection count and burden, event-count, interarrival, time-rescaling, mark,
spread, path, tail, stability, fit-sensitivity, era-transport,
adaptive-partition, failure/refusal, runtime, peak-memory, Poisson-work,
output-byte, amplification, and ensemble-diversity metrics.

Projection burden is frozen as

\[
B_{\mathrm{proj}} =
\frac{\sum_e \lVert q'_e-q_e\rVert_1}
     {\sum_e \max(\operatorname{spread}(q_e),\epsilon)}.
\]

The event set is every proposal quote vector before projection. Clipping is
forbidden. Only a zero denominator term is replaced by the policy epsilon.
Each row-free observation retains the aggregate numerator, denominator, and
projection event count; construction refuses a supplied burden or count that
does not reproduce those aggregates.

When both candidates pass the hard gates, the implementation publishes paired
means, a Student-t confidence interval for the oriented relative effect,
achieved power at the predeclared materiality margin, and one of
`favors_diagonal`, `favors_full`, `practically_equivalent`, or `inconclusive`.
Selection refuses underpowered or inconclusive comparisons and Pareto conflicts.
Both candidates must remain reconstruction-eligible and every observed
branching matrix must satisfy the strict predeclared spectral-radius bound.

The replay rule evaluates raw cross-series behavior and projection burden
first, then the complete scientific surface. Resource comparisons are used
only when scientific metrics are practically equivalent; exact resource ties
use the frozen lower-complexity order. Repository order, issue order, manual
preference, and final-holdout results are not inputs.

## Publication and planning

Applications can construct the typed policy and validation contracts, publish
them with `write_hawkes_product_selection_policy()` and
`write_hawkes_validation_comparison()`, and then freeze the result through the
public CLI:

```sh
histdatacom reconstruction --json hawkes-select \
  --policy work/selection/hawkes-product-selection-policy-<sha256>.json \
  --comparison work/selection/hawkes-validation-comparison-<sha256>.json \
  --qualification work/qualification/powered-qualification-dossier-<sha256>.json \
  --output-directory work/selection
```

When powered qualification leaves exactly one eligible candidate, omit the
comparison. The hard-gate decision is replayed before any pairwise result could
influence selection:

```sh
histdatacom reconstruction --json hawkes-select \
  --policy work/selection/hawkes-product-selection-policy-<sha256>.json \
  --qualification work/qualification/powered-qualification-dossier-<sha256>.json \
  --output-directory work/selection
```

A v2 plan that selects either marked-Hawkes candidate must set
`hawkes_product_selection_dossier_path` and
`observation_uncertainty_policy_path`, and
`feed_epoch_transition_policy_path`. Planning verifies the complete replay,
requires the explicit selected engine to equal the derived engine, binds all
artifacts into the plan graph and frozen experiment, and rejects stale,
missing, or unbound evidence. The v2.5 certification policy requires the
selection dossier and the holdout-calibrated uncertainty reports alongside
powered qualification. See [Observation-process uncertainty
propagation](observation-process-uncertainty.md) and [feed-epoch transition
uncertainty](feed-epoch-transition-uncertainty.md).

The unselected candidate remains named with machine-readable exclusion reasons,
whether it failed a hard gate or lost the powered paired comparison. The dossier
makes no historical-truth, broker, or investment claim.
