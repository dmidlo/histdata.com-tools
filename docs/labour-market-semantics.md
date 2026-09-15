# Labour-market release semantics

Version 2.6 defines provider-neutral semantics for employment and payroll
levels/changes, unemployment, participation, employment/population ratios,
earnings, hours, vacancies, hires, separations, claimant counts, and benefit
claims. These contracts refine the generic calendar, source, and release-
vintage layers; they do not replace their schedule or raw-snapshot evidence.

## Measurement and survey identity

`LabourConceptV1` gives every displayed statistic an immutable identity across:

- household, establishment, vacancy, business, administrative-register,
  benefit-claims, or explicitly mixed source basis;
- employment/payroll level or change, unemployment level/rate,
  participation, employment/population, earnings, hours, vacancies, flows,
  claimant counts, or benefit claims;
- level, period change, rate, MoM/YoY change, index, or average-hours
  transformation;
- weekly, monthly, quarterly, or annual frequency;
- seasonal/calendar basis, population, industry, worker, duration, unit,
  scale, legal-producer series, and methodology era.

All fields participate in `concept_id`. Household employment therefore cannot
substitute for establishment payrolls; a level cannot substitute for a change;
and a seasonally adjusted statistic cannot substitute for an unadjusted one.
Payroll concepts require establishment-survey identity. Benefit claims require
administrative-claims identity, while vacancies/hires/separations require an
eligible vacancy, business, or administrative source.

## Published changes and level vintages

`LabourObservationV1` records the raw lexical value, publication/availability
time, source request/release IDs, content hash, revision sequence/kind, and any
derivation links. Source-published and derived values are separate bases.

When an official producer publishes employment change, retain that published
change even when level vintages are also available. `LabourChangeDerivationV1`
may reconstruct a period change as:

\[
\Delta L_t = L_t - L_{t-1}
\]

only from two exact, adjacent, same-survey level observations. It records the
initial-release, as-known-at-release, or latest-current vintage policy. A
derived value may claim to be an historical initial actual only when the source
change is explicitly unavailable and both level inputs are initial vintages.
Today's benchmark-revised levels can never fabricate yesterday's initial
payroll change.

## Revisions and previous-as-known

`LabourVintageChainV1` retains the initial value and every routine,
simultaneous-previous, benchmark, methodology, or seasonal revision. Sequences
are gap-free, preserve exact concept/event/reference/stage identity, point to
their immediate predecessor, and move availability forward.

`LabourReleaseTripletV1` binds the current initial actual, immediately
preceding reference-period value as known at the cutoff, and an exact-concept
pre-release expectation. A simultaneous revision to the previous period is a
new vintage whose availability equals the current release timestamp. It may be
the displayed `previous_as_known`; a later database revision may not flow
backward.

Event consensus, exact-event official surveys, and machine event targets can
populate `forecast` only when available before publication. Survey-period and
government projections remain separate evidence and cannot populate a
calendar-style forecast. Measure, survey, transformation, reference period,
seasonal basis, population, and methodology era must match exactly.

## Multi-indicator release packages

`LabourReleaseV1` represents one independently meaningful calendar event.
Every observation in it shares the event, reference period, timestamp, legal
producer request/release, and content hash.

`LabourReleasePackageV1` groups releases that came from one publication at one
timestamp without merging their identities. A U.S. Employment Situation
package can therefore link payroll change, household unemployment, earnings,
and hours while retaining distinct event and concept IDs. The values and their
surprises may move in opposite directions. Duplicate logical events or mixed
package timestamps/source evidence fail closed.

## Benchmark and methodology changes

`LabourMethodologyEventV1` records benchmark revisions, new population
controls, seasonal reanalysis, classification/coverage changes, survey
redesigns, and administrative-system changes. It binds affected concept IDs,
old/new methodology eras, effective reference period, publication and
availability times, source request, and content hash.

Benchmark changes require their benchmark reference period. Population-control
events require old and new control IDs. A claim of comparability across any
change requires an explicit bridge ID; otherwise the break remains visible to
downstream models.

## U.S. Employment Situation archive

The U.S. specialization applies these distinctions to every indexed January
2000 through August 2026 Employment Situation publication. Each release
package carries three independent raw-backed triplets: CPS all-workers
unemployment rate, CES total-nonfarm monthly change, and CES total-private
average hourly earnings. A package identity proves common publication
evidence; it does not merge their concepts or surprises.

Fixed-width Summary Table A payroll revisions are derived only from the two
adjacent levels printed in the current occurrence. Semantic Summary Table B
provides the published change directly. Earlier initial changes remain bound
to their predecessor artifacts, so annual CES benchmarks cannot rewrite
history. The January 2010 earnings headline changes from production and
nonsupervisory workers to all employees and is explicitly noncomparable.
`replay_bls_employment_situation_archive()` verifies all 957 measure triplets
against the 320-artifact chain.

## U.S. JOLTS archive

The JOLTS specialization retains 269 official BLS artifacts and reconstructs
268 publication packages from March 2004 through July 2026. Each package keeps
three independent total-nonfarm, seasonally adjusted level measures in
thousands of persons: job openings on the last business day, hires over the
month, and total separations over the month. A shared publication does not
collapse those concepts into one event identity.

Occurrence-specific current and predecessor documents establish initial,
previous-as-known, and revised-prior values. The parser qualifies 45
fixed-width text, 77 preformatted HTML, and 146 semantic Table A current
releases. Twenty-two January-reference publications are marked as annual
benchmark/revision editions, so their restated history never overwrites an
earlier vintage. The September 2025 release was not published; October retains
August as its last published predecessor and marks the three unlike-period
revision fields noncomparable. `replay_bls_jolts_archive()` recomputes all 804
measure triplets and their raw and normalized hashes.

## U.S. Productivity and Costs archive

The Productivity and Costs specialization retains 215 official BLS artifacts
and reconstructs 214 preliminary/revised release stages from fourth-quarter
1999 through second-quarter 2026. Every release keeps nonfarm-business labor
productivity separate from nonfarm-business unit labor costs; both use
seasonally adjusted annualized quarter-over-quarter percent changes.

The parser follows 64 text and 150 preformatted HTML current releases across
155 sector-row and 59 measure-row Table A layouts. It also parses the current
release's comparison table, so preliminary stages preserve third estimates for
the preceding quarter and revised stages preserve second estimates for the
current quarter. The resulting 428 normalized measure records contain 426
numeric revision observations and remain independently replayable from the
current/predecessor artifact chain.

The February 2019 preliminary fourth-quarter 2018 release explicitly reports
the selected measures as `N.A.`. That source-authored unavailability remains a
counted nonblocking gap; the following revised stage does not acquire an
invented preliminary value. The March 2024 reissue notice and May 2024
corrected prior-quarter comparison are likewise both retained, making the
productivity predecessor mismatch explicit. Annual benchmark and historical
revisions never rewrite earlier release-stage evidence.

## Economy profiles

`built_in_labour_profiles()` derives one semantic/source profile for all 21
economies in the official registry. Every profile identifies the legal
producer and requires survey distinction, published-change plus level-vintage
retention, release-package identity, benchmark revisions, seasonal reanalysis,
and simultaneous prior revisions. Profiles describe required semantics, not a
claim that every series or historical vintage is already available.

Special cases include:

- euro-area harmonized versus national unemployment;
- U.K. Labour Force Survey, claimant count, and payrolled employees;
- Japan household measures and jobs-to-applicants;
- U.S. establishment versus household employment, nonfarm-payroll benchmarks,
  JOLTS vacancies/hires/separations, and weekly benefit claims.

`audit_labour_profiles()` checks exact registry ownership, one profile per
economy, required surveys/measures, package/revision support, and special-case
coverage. `require_labour_profile_coverage()` fails closed on any mismatch.

## Minimal concept

```python
from histdatacom.market_context import (
    LabourConceptV1,
    LabourFrequency,
    LabourMeasure,
    LabourSeasonalBasis,
    LabourSurveyBasis,
    LabourTransformation,
)

payroll_change = LabourConceptV1(
    economy_code="US",
    indicator_key="us-nonfarm-payrolls",
    source_series_id="ces0000000001",
    measure=LabourMeasure.PAYROLL_CHANGE,
    survey_basis=LabourSurveyBasis.ESTABLISHMENT_SURVEY,
    transformation=LabourTransformation.PERIOD_CHANGE,
    frequency=LabourFrequency.MONTHLY,
    seasonal_basis=LabourSeasonalBasis.SEASONALLY_ADJUSTED,
    population_scope="civilian noninstitutional population age 16+",
    industry_scope="total nonfarm",
    worker_scope="employees on nonfarm payrolls",
    duration_scope=None,
    unit="thousand-persons",
    scale=1_000.0,
    methodology_era_id="ces-era-2024",
)
```

Use `concept_id`, observation/revision identity, package identity, source
evidence, and cutoff time as joins and model inputs. A display title is never a
safe substitute for those contracts.
