# Cross-family activity-indicator semantics

Version 2.6 defines provider-neutral semantics for retail and consumption,
production and construction, capital-goods orders, external trade, housing,
and official confidence or diffusion surveys. These contracts refine the
generic calendar, source, schedule, and archive layers; they do not replace
them or claim that every historical observation has already been acquired.

## Exact concepts, units, and transformations

`ActivityConceptV1` identifies the official program and source series together
with the economic family, statistic, headline/component variant,
transformation, frequency, seasonal basis, unit and scale, currency, component
path, exact window, and methodology era. Every field participates in
`concept_id`.

This keeps economically different values separate:

- an import, export, trade-balance, and current-account value are distinct;
- a balance level is not its MoM, QoQ, or YoY growth rate;
- a count, currency value, volume index, price index, and percentage are not
  interchangeable;
- a producer-defined diffusion index is measured in diffusion-index points,
  not percent; and
- a net-balance percentage is accepted only when the producer explicitly
  defines that statistic as a percentage.

Headline and component concepts retain separate identities. A component path
cannot be attached to a headline or omitted from a component. Seasonal and
calendar adjustments and methodology eras are also part of identity, so an
adapter cannot silently substitute a differently adjusted or rebased series.

## Exact rolling and cumulative windows

`ActivityWindowV1` distinguishes a single period, overlapping trailing
window, calendar-year or fiscal-year cumulative window, and three-month over
three-month window. It retains the period count, anchor, and overlap rule.
`ROLLING_SUM`, `CUMULATIVE_SUM`, and
`THREE_MONTH_OVER_THREE_MONTH_PERCENT` transformations require the
corresponding window; ordinary level and growth transformations require a
single period.

This prevents a rolling 12-month total from becoming a monthly level, a
year-to-date value from losing its calendar/fiscal anchor, or a 3m/3m rate from
being treated as MoM growth.

## Observations, revisions, and calendar triplets

`ActivityObservationV1` retains the exact concept, event and reference period,
first-published value and raw lexical form, publication/availability times,
source request and release IDs, content hash, and revision evidence. A
`ActivityVintageChainV1` is gap-free and never overwrites an earlier vintage.
Routine, benchmark, methodology, seasonal, and simultaneous-previous
revisions remain distinguishable.

`ActivityReleaseTripletV1` binds the initial actual for the current event, the
immediately preceding period as known at the release cutoff, and a pre-release
expectation for the same exact concept and period. A simultaneous revision to
the previous period is eligible at the current release timestamp. Later
revisions are not. Weekly, monthly, quarterly, and annual adjacency is checked
explicitly.

Only event-targeted consensus, event-targeted official surveys, and explicit
machine event targets can populate a calendar forecast. Broad period surveys
and government projections remain projections, not synthetic event
consensus. An unavailable expectation remains explicit.

## Publications containing multiple indicators

`ActivityReleaseV1` can retain a headline and its components for one logical
event without merging their concept identities. `ActivityReleasePackageV1`
groups distinct events published in one official package while requiring the
same economy, timestamp, request, release, and content evidence.

For example, imports, exports, and the trade balance may share one statistical
bulletin but remain three independently addressable releases. The same rule
applies to starts, permits, sales, and price indicators when a producer
packages them together. A package may not repeat or collapse logical events.

## Proprietary survey boundary

`UnsupportedActivityGapV1` represents requested concepts whose source is
unqualified, unavailable, or has no official equivalent. It deliberately
cannot be used as an `ActivityObservationV1` concept.

Private PMI and proprietary confidence series therefore stay unsupported
until a future source is legally and operationally qualified. An official
business-tendency or central-bank survey can be supported under its own exact
identity, but it cannot be relabeled as the missing private series. No
official-looking substitute is fabricated.

## Economy profiles

`built_in_activity_profiles()` derives one semantic/source profile for each of
the 21 economies in the official registry. Every profile binds the five broad
calendar families to their legal primary producers and requires all detailed
activity-family semantics, exact windows, release packages, simultaneous
previous revisions, and an explicit proprietary-PMI gap. These are adapter
requirements, not claims of empirical historical coverage.

Reviewed special cases include US retail control groups, advance durable and
core capital-goods orders, and housing packages; extra- versus intra-EU trade;
official European Commission tendency surveys; UK ONS output and trade; and
the distinction between Japan's official Tankan and private PMI products.
`audit_activity_profiles()` compares every owner to the source registry and
checks semantic completeness. `require_activity_profile_coverage()` fails
closed on any missing economy, ownership drift, or semantic gap.

## Minimal concept

```python
from histdatacom.market_context import (
    ActivityConceptV1,
    ActivityFrequency,
    ActivityIndicatorFamily,
    ActivitySeasonalBasis,
    ActivityStatisticKind,
    ActivityTransformation,
    ActivityUnitKind,
    ActivityVariant,
    ActivityWindowV1,
)

retail_mom = ActivityConceptV1(
    economy_code="US",
    indicator_key="us-retail-sales-mom",
    source_series_id="census-retail-sales",
    official_program="Monthly Retail Trade Survey",
    family=ActivityIndicatorFamily.RETAIL_SALES,
    statistic_kind=ActivityStatisticKind.SALES,
    variant=ActivityVariant.HEADLINE,
    transformation=ActivityTransformation.MONTH_OVER_MONTH_PERCENT,
    frequency=ActivityFrequency.MONTHLY,
    seasonal_basis=ActivitySeasonalBasis.SEASONALLY_ADJUSTED,
    unit_kind=ActivityUnitKind.PERCENT,
    unit="percent",
    scale=1.0,
    currency_code=None,
    component_path=(),
    window=ActivityWindowV1.single_period(),
    producer_defines_percentage=False,
    methodology_era_id="census-retail-current",
)
```

Use concept, window, event, period, vintage, source, and cutoff identities as
joins and model inputs. A display label such as “Retail Sales” or “PMI” is
never a safe substitute for those contracts.
