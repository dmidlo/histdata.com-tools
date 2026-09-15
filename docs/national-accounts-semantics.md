# GDP and national-accounts release semantics

Version 2.6 defines provider-neutral semantics for monthly, quarterly, and
annual GDP and national accounts. The contracts distinguish the statistic,
price/volume basis, transformation, seasonal treatment, valuation, component
framework, publication stage, and exact vintage. They refine the generic
calendar, source, schedule, and archive contracts rather than replacing them.

## Exact output concepts

`NationalAccountsConceptV1` gives each displayed value an immutable identity
across:

- GDP, gross national income, gross value added, or an expenditure, industry,
  or income component;
- real chain-volume, real fixed-price, nominal current-price, or volume-index
  output;
- level, annualized level, index, absolute change, MoM, simple QoQ,
  annualized QoQ, YoY, contribution, or share transformation;
- monthly, quarterly, or annual frequency;
- seasonal/calendar treatment, including a distinct seasonally adjusted
  annual-rate basis;
- market/basic/other publisher-defined valuation, unit, scale, price base,
  chain-linking method, source series, component path, and methodology era.

All fields participate in `concept_id`. Nominal output cannot substitute for
real output, market-price output cannot substitute for basic-price output, and
simple quarterly growth cannot substitute for annualized quarterly growth.
Component measures require both an accounting framework and an exact component
path. Real/index concepts require a price base; chain-volume concepts also
require their linking method.

## Stage-specific first actuals

`NationalAccountsObservationV1` records a source-published or level-derived
value together with its exact reference period, publication stage, raw lexical
value, publication/availability times, source request/release IDs, content
hash, revision sequence/kind, and derivation links.

The first value published at each stage is immutable. An advance estimate and
a second, preliminary, or final estimate are separate logical events even when
they describe the same quarter. `NationalAccountsReleaseSequenceV1` requires
distinct event IDs, strictly increasing publication times, and valid stage
order. It never relabels today's latest GDP value as the historical advance
actual.

Within-stage changes form a gap-free `NationalAccountsVintageChainV1`. Routine,
simultaneous-previous, benchmark, methodology, and seasonal revisions retain
their immediate predecessor and become visible only from their availability
time.

## U.S. Gross Domestic Product archive

The GDP specialization reconstructs 318 official BEA release occurrences from
the fourth-quarter 1999 advance estimate through the second estimate for
second-quarter 2026. Its one headline concept is real GDP, seasonally adjusted,
annualized quarter-over-quarter percent change. The 107 advance, 105
second/preliminary, and 106 third/final estimates remain separate stage
identities.

Every second or final estimate compares with the immediately preceding stage
for the same quarter. Every advance estimate compares the prior quarter's last
published value with the value printed in the new artifact, exposing 17
concurrent annual/comprehensive-update revisions. Across all stages, 192 of
318 comparison lineages change numerically. The final third-quarter 1999 PDF
anchors the first prior-quarter comparison, and the official March 2001 PDF
supplies the exact embargo header omitted from its imported HTML page.

The source's irregular evidence remains explicit. Fourth-quarter 2018 and
third-quarter 2025 each omit the standard second-stage publication after a
federal shutdown. The archive lists two advance releases one day before their
artifact headers, and five releases print EST/EDT abbreviations inconsistent
with their dates. The parser preserves those lexical values while the artifact
header and `America/New_York` control occurrence time. It also retains the
corrected July 2000 advance artifact and 15 title-declared historical updates.
The 18-page archive inventory, one bounded current-release supplement, all 318
release hashes, and both supporting PDF hashes replay deterministically.

## U.S. Personal Income and Outlays archive

The Personal Income and Outlays specialization reconstructs 319 official BEA
publications and keeps four monthly concepts separate: current-dollar personal
income, current-dollar PCE, headline PCE-price change, and core PCE-price
change. Income and spending cover December 1999 through July 2026; both price
series cover June 2000 through July 2026. Price changes through January 2002
are derived only from the index levels printed in those same publications.

Each measure retains its preceding period as previously known and any value
restated in the new publication. The 2019 shutdown's split and catch-up
releases, the 2025 revision-only workbook, and the combined October-November
2025 release remain their actual source-authored occurrence shapes. PDF, text,
and workbook support artifacts are hash-bound where HTML omits the required
table or time evidence. One date-only release and all source-authored zone/date
discrepancies remain explicit rather than being inferred away.

## Published and derived growth

Source-published growth takes precedence. When it is genuinely unavailable,
`NationalAccountsGrowthDerivationV1` binds two exact same-series level vintages
and the vintage policy used. For quarterly levels, simple growth is:

\[
g_{q/q}=100\left(\frac{L_t}{L_{t-1}}-1\right),
\]

while the annualized quarterly rate is:

\[
g_{q/q,ann}=100\left[\left(\frac{L_t}{L_{t-1}}\right)^4-1\right].
\]

They are different concepts and formulas. Monthly growth requires adjacent
months; quarterly growth requires adjacent quarters; YoY growth requires a
12-month lag. Frequency, real/nominal basis, seasonal basis, valuation, price
base, linking method, component identity, and methodology era must all match.

A derived value may claim to be an historical first actual at a stage only
when the published transformation is unavailable and both level inputs are the
first vintages at their stages. Latest-current or benchmark-revised levels
cannot fabricate an earlier advance estimate.

## Previous and forecast

`NationalAccountsReleaseTripletV1` binds:

- `actual_stage_initial`: the first value published for this exact stage;
- `previous_as_known`: the immediately preceding reference-period value visible
  at the release cutoff, with its own stage and revision identity retained;
- `expectation`: a pre-release forecast for the exact concept, stage, event,
  and reference period.

A simultaneous revision to the prior period may become available at the
current release timestamp. Later database revisions cannot flow backward into
that historical triplet. A forecast for advance GDP cannot populate final GDP,
and a broad quarterly/annual official projection cannot masquerade as an exact
stage forecast. Unsupported forecasts remain explicitly unavailable.

## Components and non-additivity

`NationalAccountsComponentReconciliationV1` records the expenditure, industry,
or income framework, signed component entries, official table identity,
coverage, residual, tolerance, and reconciliation result.

The permitted states are deliberately distinct:

- additive current/fixed-price levels may reconcile to their aggregate;
- source-published contribution points may reconcile to an aggregate growth
  rate;
- chain-volume levels are explicitly non-additive and cannot be forced to
  reconcile merely because a small example happens to sum;
- partial tables list missing component keys and cannot claim reconciliation;
- unavailable tables remain explicit rather than inventing components.

Imports or another subtractive accounting entry retain their negative role.
Every retained component must share the aggregate's event, period, stage,
publication/source evidence, output basis, frequency, seasonal treatment,
valuation, base/link method, and methodology era. A
`NationalAccountsReleaseV1` keeps the full stage table and validates that every
reconciliation references observations present in that release.

## Benchmark and methodology revisions

`NationalAccountsBenchmarkRevisionV1` groups the multiple historical
stage-period cells changed by one official benchmark publication. Every member
is a benchmark revision that supersedes an already retained observation and
shares exact publication and source evidence. Earlier event snapshots remain
addressable through their vintage chains.

`NationalAccountsMethodologyEventV1` records benchmark revisions, rebases,
chain-linking changes, classification/coverage changes, seasonal reanalysis,
and valuation changes. It binds affected concept IDs, old/new methodology
eras, effective and benchmark periods, publication evidence, and any explicit
bridge. A comparability claim requires a bridge ID; otherwise the series break
remains visible.

## Economy profiles

`built_in_national_accounts_profiles()` derives one legal-producer semantic
profile for each of the 21 economies in the official registry. Every profile
requires quarterly/annual, real/nominal, simple/annualized growth,
expenditure/industry/income component, stage, previous-as-known, non-additivity,
and benchmark-vintage semantics. Profiles describe the contract an adapter
must preserve; they do not claim empirical historical coverage.

Reviewed special cases include:

- Canada and the United Kingdom: monthly plus quarterly GDP;
- Euro Area: preliminary-flash, preliminary, and final chain-volume releases;
- Japan: first and second preliminary estimates;
- United States: advance, second, and final (third-estimate) stages,
  seasonally adjusted annual-rate levels, annualized quarterly growth, and NIPA
  comprehensive updates.

`audit_national_accounts_profiles()` checks exact registry ownership, one
profile per economy, expected frequencies/stages, all component frameworks,
benchmark-set support, and difficult-case coverage.
`require_national_accounts_profile_coverage()` fails closed on any mismatch.

## Minimal concept

```python
from histdatacom.market_context import (
    NationalAccountsConceptV1,
    NationalAccountsFrequency,
    NationalAccountsMeasure,
    NationalAccountsOutputBasis,
    NationalAccountsSeasonalBasis,
    NationalAccountsTransformation,
)

advance_real_gdp = NationalAccountsConceptV1(
    economy_code="US",
    indicator_key="us-real-gdp",
    source_series_id="bea-gdp-real",
    measure=NationalAccountsMeasure.GDP,
    component_framework=None,
    component_path=(),
    output_basis=NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
    transformation=(
        NationalAccountsTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
    ),
    frequency=NationalAccountsFrequency.QUARTERLY,
    seasonal_basis=NationalAccountsSeasonalBasis.SEASONALLY_ADJUSTED,
    valuation_basis="market prices",
    unit="percent",
    scale=1.0,
    price_base_period="2017",
    chain_linking_method="fisher-chain",
    methodology_era_id="nipa-2023-benchmark",
)
```

Use concept, stage-event, observation/vintage, component-table, source evidence,
and cutoff identities as joins and model inputs. A display label such as
"GDP" is never a safe substitute for those contracts.
