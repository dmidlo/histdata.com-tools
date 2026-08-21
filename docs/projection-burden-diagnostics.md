# Reconciliation projection-burden diagnostics

A passing post-reconciliation triangle residual does not prove that a proposal
model generated coherent joint quotes. Reconciliation can force an incoherent
proposal onto the permitted triangle region. The v2.5 release therefore treats
proposal movement as an independent, release-critical diagnostic.

## Frozen event metric

For every synthetic proposal event (e), the diagnostic accepts the complete
three-leg bid/ask vector before projection (q_e) and after projection
(q'_e). Its primary scale is frozen before results:

\[
s_e = \sum_{j \in \{\mathrm{EURGBP},\mathrm{EURUSD},\mathrm{GBPUSD}\}}
      \max(\operatorname{ask}_{e,j}-\operatorname{bid}_{e,j},\epsilon),
\]

\[
b_e = \frac{\lVert q'_e-q_e\rVert_1}{s_e}.
\]

The versioned scale is
`combined-triangle-proposal-spread-epsilon.v1`. It is strictly positive. Only
an individual zero-spread denominator term is replaced by the predeclared
epsilon. The movement numerator and resulting burden are never clipped.

Each report publishes arithmetic `mean`, `total`, `maximum`, p50, p90, and p99
burden. It also publishes the scale-weighted mean

\[
\frac{\sum_e \lVert q'_e-q_e\rVert_1}{\sum_e s_e},
\]

which is the exact aggregate consumed by the diagonal-versus-full Hawkes
selection dossier.

Bid/ask L1 movement is decomposed exactly with
`shapley-midpoint-spread-l1.v1`. For each leg, the shared movement on the
midpoint and spread axes is divided equally and each axis receives its exclusive
component. The two reported totals therefore sum exactly to the quote-vector L1
numerator; they are not overlapping proxy measures.

## Required evidence surface

`ProjectionBurdenReportV1` contains no event rows or rejected proposals. It
binds the sorted exact event IDs, event content hashes, proposal-lineage
identity, reconciliation configuration, alignment policy, and scenario
definitions into aggregate digests. It publishes:

- a baseline slice for every window, ensemble member, and model;
- an exact validation-coordinate slice for #508 replay;
- model-conditioned era, session, event-state, alignment, and quote-age slices;
- baseline and intentionally incoherent scenario slices;
- proposal count, projected-event count/rate, and hard-refusal count/rate;
- pre- and post-projection synthetic triangle-residual distributions;
- immutable observed-only residual distributions excluded from burden;
- burden distributions and scale-weighted aggregates;
- exact midpoint/spread movement totals and projection-priority leg counts;
- quote-age distributions and burden/age Pearson association;
- signed and absolute path/spread metric changes caused by projection; and
- counts where a passing final residual would otherwise hide excessive burden.

Observed-only residual exceedances remain source-quality evidence. They cannot
be projected and never enter synthetic burden. A residual involving synthetic
data that remains above tolerance after permitted projection is blocking.

## Qualification and negative controls

`ProjectionBurdenPolicyV1` freezes validation-only advisory and hard limits for
mean, p90, p99, maximum, and projected-event rate. Hard violations fail a model;
advisory violations explicitly limit it. Final residuals are evaluated
independently, so a passing post-projection residual cannot override excessive
movement.

Every policy names one or more predeclared misspecification scenarios whose raw
proposals are intentionally cross-series-incoherent. Each model must either
produce burden above the frozen detection floor or refuse the proposal at its
hard projection limit. A negative control that escapes both mechanisms fails
qualification. Pairwise model comparisons use exactly matched baseline
window/member cells and reject a model whose mean burden exceeds the
predeclared comparator ratio.

## Hawkes selection and release consumption

`bind_projection_burden_to_hawkes_selection()` reconciles every #508 validation
coordinate. The event-derived L1 numerator, scale denominator, and projected
event count must equal the exact `HawkesValidationObservationV1` aggregates.
The selected engine fails the binding if its burden decision fails or if the
comparator-relative gate identifies it as excessive.

Every release surface obtains a content-addressed
`ProjectionBurdenConsumptionReceiptV1`. Complete release coverage requires
receipts for a predeclared, exact set of consumer IDs in every category:

1. Hawkes model selection;
2. each reconstruction product manifest;
3. campaign shard summaries;
4. era audits; and
5. certification.

Qualified report and receipt IDs flow into
`ReconstructionDeliveryQualityManifestV1`; campaign product shards aggregate
the exact product receipt IDs. A product can set a cross-currency coherence
claim only when its projection status is `qualified`. A `limited` receipt must
publish its limitation codes, and a failed receipt cannot enter release
coverage.

`ProjectionBurdenReleaseCoverageV1` binds the complete receipt set and exposes
the required consumer IDs alongside the four scalar certification observations:
coverage validity, excessive product count, surviving synthetic-residual
failure count, and final-residual-only pass count. Coverage is invalid when
even one predeclared product, shard, era audit, certification run, or Hawkes
selection consumer lacks its exact receipt; category-only presence is not
sufficient. It is published as the exact
`projection-burden-consumption-receipts` artifact kind.

The v2.5 certification policy requires both `projection-burden-report` and
`projection-burden-consumption-receipts` artifacts. Certification checks that
release-consumer coverage is complete, no product has excessive burden, no
synthetic post-projection residual remains outside tolerance, and no model
passes from final residual alone.

Reports are published with `write_projection_burden_report()` and restored with
`read_projection_burden_report()`. Filenames contain the SHA-256 of the exact
canonical bytes; readback rejects filename/content substitution and reconstructs
all typed identities before returning the report. The parallel
`write_projection_burden_release_coverage()` and
`read_projection_burden_release_coverage()` functions apply the same guarantees
to the certification handoff.
