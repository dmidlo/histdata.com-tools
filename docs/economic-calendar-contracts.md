# Provider-neutral economic-calendar contracts

`histdatacom.market_context.economic_calendar` preserves economic release
vintages without making any commercial calendar the source of truth. Canonical
identity comes from the legal statistical producer or institution, its series,
the reference period, and retained source evidence. Acquisition and source
selection remain outside this module.

This is a v2.6 foundation. It does not claim that the installed official-source
corpus already supplies every indicator, original schedule vintage, or
professional consensus required by the later calendar roadmap.

## Contract surface

| Contract | Role |
| --- | --- |
| `EconomicCalendarReleaseV1` | One immutable schedule, status change, publication, or value revision. It retains institutional and versioned series identity, reference period, frequency, seasonality, unit/scale/base, source IDs, lexical timezone evidence, precision, and lineage. |
| `EconomicCalendarForecastV1` | One expectation vintage with explicit provenance kind, target scope, statistic, collection window, support, and unavailable state. |
| `EconomicCalendarCorpusV1` | Bounded, content-identifiable release and forecast history with declared coverage and completeness. |
| `EconomicCalendarEventStateV1` | The latest state of one logical release at a decision time, retaining actual revisions, previous-initial, previous-as-known, and the separately labelled latest prior-period diagnostic. |
| `EconomicCalendarQueryV1` | Deterministic bounded result for one half-open event interval. |
| `EconomicCalendarSourceAdapterV1` | Normalization seam used by official-source and independently governed forecast adapters. |
| `EconomicCalendarAsKnownReaderV1` | The sole event/trader strategy seam. Consumers request point-in-time state and do not inspect adapter or transport classes. |
| `EconomicUnitConversionV1` | Versioned affine normalization with source/target unit, scale and base plus the raw lexical value. |
| `EconomicSurprisePolicyV1` | Versioned direction map, robust-scale window, support floor, and epsilon. |
| `EconomicCalendarSurpriseV1` | Raw, directional, and prior-only robust standardized surprise evidence. |

`StaticOfficialEconomicCalendarAdapterV1` supports deterministic official
fixtures and already-normalized imports. `build_economic_calendar_corpus()`
checks that every emitted source names the adapter and version that produced
it. This makes two official producers interchangeable at the query boundary
without pretending their upstream schemas are identical.

## Identity and source precedence

A logical release names an economy code and display name, currency set,
institution, `EconomicEventFamily`, indicator, official series/table/release
and request IDs, reference period, frequency, seasonality, unit/scale/base,
affected instruments, and the established `MarketContextKind`. `series_key`
identifies the continuing concept while `series_version` and the derived
`series_id` identify one comparable semantic era. A rename, rebase, seasonal
treatment, source-series migration, or methodology change must either advance
`series_version` or fail corpus construction. An optional
`comparability_bridge_id` names a separately governed bridge; its presence
does not silently make values comparable.

Source precedence follows the governing roadmap in #533: the legal statistical
producer controls CPI, employment, GDP, trade, and similar series; central
banks control their own institutional decisions and communications. A
latest-only API is evidence of current state, not proof of historical
vintages. Archived releases and snapshots remain necessary under #581.

Each release and forecast embeds `MarketContextSourceV1`, preserving exact
source version, retrieval time, content digest, adapter identity, license,
redistribution constraints, URI, and limitations. Deterministic release,
forecast, state, query, and corpus IDs cover their complete scientific
payloads.

## Release, revision, schedule, and timing semantics

Publication stage and event status are deliberately separate. Stage is one of
`initial`, `flash`, `advance`, `preliminary`, `second`, `final`, or
`revision`. Status is independently `tentative`, `scheduled`, `rescheduled`,
`delayed`, `cancelled`, `released`, `unscheduled`, `discontinued`,
`source-calendar-unavailable`, or `unresolved`. Thus a scheduled advance GDP
event remains the same stage when released, while a later final GDP
publication is a distinct logical event. A value revision is a new vintage;
it does not turn the original first release into a final value.

Every observed status, schedule, or value change is immutable. Revision
sequences are contiguous and each later vintage names the exact predecessor.
A changed scheduled time requires an explicit rescheduled/delayed status, and
a reschedule/cancellation retains a reason. The first actual is never
overwritten by a later revised value.

Every record separates:

- `scheduled_for_ns`: the retained target time;
- `scheduled_lexical`: source timestamp with its explicit offset;
- `released_at_ns`: the observed publication time, when an actual exists;
- `released_lexical`: source publication timestamp with its explicit offset;
- `first_observed_at_ns`: when this evidence was first observed;
- `available_at_ns`: the earliest defensible strategy admission time; and
- source `retrieved_at_ns`: when the retained source object was acquired.

`source_timezone` retains the IANA zone and `timezone_evidence` records the
source/rule basis. Lexical timestamps must normalize exactly to their integer
nanosecond values and their offsets must agree with that zone. The separate
`EconomicTimePrecision` records exact-second, exact-minute, scheduled-only,
date-only, or inferred-bounded evidence. DST folds therefore remain distinct
identities instead of ambiguous wall-clock strings.

An as-known query admits a release or forecast only when

`available_at_ns <= decision_at_ns`.

It selects the latest admissible release vintage but retains the complete
visible actual chain in the returned state. Later schedule changes, actual
revisions, and forecast snapshots cannot leak backward.

## Actual, previous, and projected values

The three-field professional-calendar surface stays provenance-explicit:

- `actual_initial` is `A_e`, the first-release actual;
- `previous_value` is `P_e`, the latest prior-period actual whose evidence was
  available strictly before the current release;
- `observed_consensus` is `F_obs[e,t]`; and
- `machine_projection` is `F_mach[e,t]`.

Observed consensus and machine projections have separate kinds, identities,
sources, limitations, and surprise calculations. A post-release forecast is
never selected into release state. `project_economic_calendar_state()` chooses
observed consensus before a machine projection only when adapting to the older
single-`expected_value` `MarketContextEventV1` seam, and records that choice in
an explicit `projection:*` tag. The neutral query never collapses them.

Forecast scope is explicit: `event_consensus`,
`official_professional_survey_event_target`,
`official_professional_survey_period_target`, `central_bank_projection`,
`government_projection`, `market_implied`, `machine_event_target`, or
`unavailable`. Only the first two are calendar-style consensus without a
proxy label. Forecast statistic, collection window, respondent count,
dispersion, quantile and raw unit-conversion evidence are separately typed;
unsupported values are explicit null/unavailable records rather than
inferences from actuals.

Previous-as-known is computed, not trusted from a provider's mutable
`Previous` field. The query finds the immediately preceding reference period
for the same canonical series and chooses its latest actual revision with
`available_at_ns < current_release_time`. A correction published at or after
the current release is excluded forever from `P_e` for that release. It may
later appear only as the distinct `previous_latest` diagnostic at a later
query cutoff; `previous_initial` retains the first value.

## Surprise policy

`compute_economic_calendar_surprises()` emits raw surprise
`S_e = A_e - F_e` and directional surprise `d_e S_e`, where every direction
is a predeclared `-1` or `+1` in `EconomicSurprisePolicyV1`. Robust scale is
computed only from earlier event times within the declared bounded window:

`scale_e = max(1.4826 * MAD(S_i for i < e), epsilon)`.

`robust_z = S_e / scale_e` is absent until the policy's minimum prior support
is met. Events sharing an event timestamp are evaluated as one batch, so they
cannot supply one another's scale history. Policy version, direction map,
window, support floor, epsilon, release/forecast identity, and observed scale
are all content-addressed; price response never infers direction.

## Strategy query and compatibility

Strategies type against `EconomicCalendarAsKnownReaderV1` and call
`as_known_at()` with event bounds, decision time, optional currencies, symbols,
families, and an explicit result bound. `EconomicCalendarCorpusV1` implements
the protocol directly. No strategy needs a provider class, credential,
endpoint, or raw field name.

`economic_release_from_market_context()` bridges already-approved official
`MarketContextEventV1` output into the richer release contract. This lets the
existing ONS, ECB, Bank of England, and Federal Reserve source work evolve
without changing the consumer protocol. It does not invent reference-period
or release-stage, currency, and unit metadata; callers must bind those fields
from the official series and source evidence.

## Artifacts and replay

`write_economic_calendar_corpus()` writes one immutable artifact named
`economic-calendar-corpus-<sha256>.json`. Existing content-addressed files are
reused only when the bytes match. `read_economic_calendar_corpus()` verifies
the filename digest, byte bound, JSON, schema, all nested identities, and every
revision chain. `replay_economic_calendar_corpus()` reconstructs all canonical
identities and requires the corpus ID to remain exact.

`economic_calendar_corpus_to_arrow()` produces one bounded table with a fixed
schema, corpus/coverage/count metadata, indexed release/forecast identity
columns, and canonical lossless JSON payloads. The inverse validates exact
field and metadata schemas, row bounds/counts, every projected identity/time,
canonical payload bytes, nested contracts, and the final corpus ID. This
keeps Arrow useful for bounded corpus exchange without creating a second
semantic source of truth.

Raw acquisition replay is intentionally not part of the neutral module. Each
official adapter remains responsible for bounded acquisition and exact raw
snapshot replay under the shared source registry planned in #535. Coverage
diagnostics and archived-vintage completeness continue under #537, #581, and
related source-specific issues.

The audited disposition of every symbol on the superseded branch is recorded
in [`economic-calendar-branch-migration.md`](economic-calendar-branch-migration.md).
