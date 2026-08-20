# Marked-Hawkes product-selection dossier

Powered qualification and product selection are separate decisions. A powered
qualification dossier may declare both
`histdatacom.marked-hawkes.diagonal_self_excitation` and
`histdatacom.marked-hawkes.full_self_cross_excitation` reconstruction-eligible.
The v2.5 product may bind either engine only through a replay-verified
`histdatacom.hawkes-product-selection-dossier.v1`.

The dossier is derived from three content-addressed inputs:

1. a predeclared `histdatacom.hawkes-product-selection-policy.v1`;
2. an exact paired `histdatacom.hawkes-validation-comparison.v1`; and
3. the powered qualification dossier that admits both candidates.

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

For every metric, the implementation publishes paired means, a Student-t
confidence interval for the oriented relative effect, achieved power at the
predeclared materiality margin, and one of `favors_diagonal`, `favors_full`,
`practically_equivalent`, or `inconclusive`. Selection refuses underpowered or
inconclusive comparisons and Pareto conflicts. Both candidates must remain
reconstruction-eligible and every observed branching matrix must satisfy the
strict predeclared spectral-radius bound.

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

A v2 plan that selects either marked-Hawkes candidate must set
`hawkes_product_selection_dossier_path`. Planning verifies the complete replay,
requires the explicit selected engine to equal the derived engine, binds the
dossier into the plan artifact graph and frozen experiment, and rejects stale,
missing, or unbound selection evidence. The v2.5 certification policy requires
the same dossier alongside powered qualification.

The eligible-but-unselected candidate remains named with machine-readable
exclusion reasons. The dossier makes no historical-truth, broker, or investment
claim.
