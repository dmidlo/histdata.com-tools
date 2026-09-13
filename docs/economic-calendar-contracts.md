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
| `EconomicCalendarReleaseV1` | One immutable schedule, cancellation, first release, or revision. It retains institutional and series identity, reference period, frequency, seasonality, unit/scale/base, release timing, precision, source evidence, and revision lineage. |
| `EconomicCalendarForecastV1` | One expectation vintage, explicitly either independently observed consensus or a machine projection. |
| `EconomicCalendarCorpusV1` | Bounded, content-identifiable release and forecast history with declared coverage and completeness. |
| `EconomicCalendarEventStateV1` | The latest state of one logical release at a decision time, including visible actual vintages and previous-as-known. |
| `EconomicCalendarQueryV1` | Deterministic bounded result for one half-open event interval. |
| `EconomicCalendarSourceAdapterV1` | Normalization seam used by official-source and independently governed forecast adapters. |
| `EconomicCalendarAsKnownReaderV1` | The sole event/trader strategy seam. Consumers request point-in-time state and do not inspect adapter or transport classes. |

`StaticOfficialEconomicCalendarAdapterV1` supports deterministic official
fixtures and already-normalized imports. `build_economic_calendar_corpus()`
checks that every emitted source names the adapter and version that produced
it. This makes two official producers interchangeable at the query boundary
without pretending their upstream schemas are identical.

## Identity and source precedence

A logical release names an economy/currency, institution, event family,
indicator, official series, reference period, frequency, seasonality,
unit/scale/base, affected instruments, and the established
`MarketContextKind`. These semantic fields cannot drift across its revision
chain. A source-specific row ID may remain in source metadata, but it is not
canonical identity.

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

## Release, revision, and timing semantics

The release stages are `scheduled`, `initial`, `revision`, and `cancelled`.
Each observed change is a new immutable vintage. Revision sequences must be
contiguous and each later vintage must name the exact predecessor. The first
actual is never overwritten by a later revised value.

Every record separates:

- `scheduled_for_ns`: the retained target time;
- `released_at_ns`: the observed publication time, when an actual exists;
- `first_observed_at_ns`: when this evidence was first observed;
- `available_at_ns`: the earliest defensible strategy admission time; and
- source `retrieved_at_ns`: when the retained source object was acquired.

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

Previous-as-known is computed, not trusted from a provider's mutable
`Previous` field. The query finds the immediately preceding reference period
for the same canonical series and chooses its latest actual revision with
`available_at_ns < current_release_time`. A correction published at or after
the current release is excluded forever from `P_e` for that release.

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

Raw acquisition replay is intentionally not part of the neutral module. Each
official adapter remains responsible for bounded acquisition and exact raw
snapshot replay under the shared source registry planned in #535. Coverage
diagnostics and archived-vintage completeness continue under #537, #581, and
related source-specific issues.

The audited disposition of every symbol on the superseded branch is recorded
in [`economic-calendar-branch-migration.md`](economic-calendar-branch-migration.md).
