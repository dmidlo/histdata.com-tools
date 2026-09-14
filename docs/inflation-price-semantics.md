# Inflation and price-index semantics

Version 2.6 defines a provider-neutral contract for CPI, HICP, PPI, PCE and
GDP deflators, retail-price indexes, import/export prices, official component
tables, and related price releases. The contract complements the generic
economic-calendar and release-vintage layers; it does not replace their
schedule, availability, or raw-snapshot evidence.

## Exact concept identity

`InflationConceptV1` identifies all dimensions that can change the meaning of
a displayed value:

- economy and legal producer series;
- index family and headline, core, robust, sector, or component variant;
- index level, MoM, QoQ, YoY, annualized QoQ, index change, contribution, or
  weight transformation;
- monthly, quarterly, or annual reference frequency;
- seasonal/calendar adjustment;
- unit, scale, coverage, price basis, component path, exclusions, base period,
  and methodology era.

These dimensions participate in the deterministic `concept_id`. A headline
YoY rate cannot therefore be substituted for core YoY, MoM, a seasonally
adjusted rate, a different population, or a different methodology era. Index
levels must retain their published base period. Core concepts must name their
excluded components, and component values must retain their classification
path.

## Index levels and derived rates

Whenever official index levels are available, retain them as immutable
`InflationObservationV1` values. `InflationRateDerivationV1` records both exact
index observations and checks the applicable formula:

\[
\Delta_m=100(I_t/I_{t-1}-1), \qquad
\Delta_q=100(I_t/I_{t-1q}-1), \qquad
\Delta_y=100(I_t/I_{t-1y}-1).
\]

Annualized quarter-over-quarter change is
`100 * ((I_t / I_(t-1q))^4 - 1)`. Absolute index change is `I_t - I_lag`.
The required lag is checked against the concept frequency, and both inputs
must have the same index-series identity, base, methodology era, variant,
coverage, and seasonal basis.

The derivation declares one of three vintage policies: initial release,
as-known-at-release, or latest current. A derived value may be labeled an
historical initial-release actual only when both retained index inputs are
revision sequence zero and the declared policy is `initial-release`. A
recomputed current-vintage YoY rate can never masquerade as the historical
initial actual.

## Initial values and revisions

Every `InflationObservationV1` records publication and availability time,
source request/release identifiers, content hash, raw lexical value, and
revision lineage. `InflationVintageChainV1` starts at sequence zero and keeps
every later value. Revisions must be gap-free, point to their immediate
predecessor, preserve the exact concept/event/reference period/stage, and move
availability forward. `as_known_at(t)` returns only the most recent public
vintage at the cutoff.

Simultaneous revisions of the previous month belong to that previous period's
chain. They may populate `previous_as_known` only when their availability is
at or before the frozen pre-release cutoff; later database revisions never
flow backward.

## Actual, previous, and forecast

`InflationReleaseTripletV1` binds:

- `actual_initial`: sequence-zero value for the displayed concept;
- `previous_as_known`: the prior reference-period vintage visible before the
  current release; and
- `expectation`: an exact-event pre-release forecast or explicit unavailable
  declaration.

All three use the same `concept_id`. Event consensus, official professional
survey event targets, and machine event targets are eligible when their
availability precedes publication. Official survey period targets, central
bank projections, and government projections remain useful evidence but
cannot silently populate a calendar-style forecast field.

## Release stages

`InflationReleaseV1` binds every aggregate and component in one source snapshot
to a single economy, reference period, stage, timestamp, request, and content
hash. `InflationReleaseSequenceV1` retains flash, advance, preliminary, second,
final, and revision stages as separate logical events in publication order.
For example, euro-area flash HICP is not overwritten by final HICP even when
the displayed value happens to be unchanged.

## Components, weights, and contributions

`InflationComponentContributionV1` retains the component observation, weight
vintage, contribution value, and whether the contribution was source-published
or computed from the weight. A computed contribution must equal
`weight_percent * component_rate / 100` within the declared tolerance.

`InflationComponentAggregationV1` distinguishes:

- `complete`: weights sum to 100 and component contributions plus the explicit
  residual reconcile to the aggregate;
- `partial`: retained components and the missing classification keys are both
  explicit, with no reconciliation claim; and
- `unavailable`: no component values are invented and the missing keys remain
  recorded.

This keeps missing historical component vintages from being filled with a
current component table or weight regime.

## Rebases and methodology changes

`InflationMethodologyEventV1` records rebases, reweights, seasonal reanalysis,
classification/coverage changes, tax treatment, and owner-occupied-housing
changes. Rebases name both bases; a comparability claim requires a positive
bridge factor. Reweights name both weight vintages. Publication, availability,
effective reference period, source request, and content hash are retained, so
a methodology change is never inferred solely from the latest database.

## Economy profiles and source ownership

`built_in_inflation_profiles()` derives one reviewed profile for each of the
21 economies in the official-source registry. Every supported index family has
an explicit legal producer. This matters where ownership is split: U.S. CPI
and PPI are owned by BLS while PCE/GDP deflators are owned by BEA; Japan's CPI
is owned by the Statistics Bureau while its producer-price family is owned by
the Bank of Japan.

The profiles also require universal handling for initial/revision chains,
component weight vintages, seasonal revisions, rebases, energy-tax changes,
and simultaneous previous-period revisions. Special cases identify euro-area
flash/final HICP, national CPI versus HICP in Germany and France, Tokyo versus
national CPI, and U.S. shelter/owner-equivalent-rent concepts.

Profiles are semantic qualification envelopes, not claims that every series
has already been backfilled. `audit_inflation_profiles()` checks registry
scope, legal-producer identity, required families, stages, variants, and edge
cases. `require_inflation_profile_coverage()` fails closed on any gap.

## Minimal example

```python
from histdatacom.market_context import (
    InflationConceptV1,
    InflationFrequency,
    InflationIndexFamily,
    InflationSeasonalBasis,
    InflationTransformation,
    InflationVariant,
)

headline_yoy = InflationConceptV1(
    economy_code="US",
    indicator_key="us-cpi",
    source_series_id="cuur0000sa0",
    index_family=InflationIndexFamily.CPI,
    variant=InflationVariant.HEADLINE,
    transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
    frequency=InflationFrequency.MONTHLY,
    seasonal_basis=InflationSeasonalBasis.NOT_SEASONALLY_ADJUSTED,
    unit="percent",
    scale=1.0,
    component_path=(),
    excluded_components=(),
    coverage="United States urban consumers",
    price_basis="consumer transaction prices",
    index_base_period=None,
    methodology_era_id="cpi-era-2024",
)
```

Treat `concept_id`, observation/vintage identity, source evidence, and cutoff
time as required model inputs. Human-readable labels are display metadata, not
a safe join key.
