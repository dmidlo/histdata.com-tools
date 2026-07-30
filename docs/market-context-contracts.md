# Point-in-time market-context contracts

The market-context domain supplies immutable macro, central-bank, news, and
calendar evidence to reconstruction without adding repeated context columns to
every tick. A versioned timeline is durable; each bounded reconstruction window
receives a compact query sidecar.

The contracts do not authorize or scrape a paid corpus. Production corpus
support selects only the official sources documented below. Source adapters
normalize approved evidence into the common interface while retaining the
original provenance and redistribution policy. Additional sources still
require an operator license review.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `MarketContextSourceV1` | Source/version, retrieval time, content hash, adapter identity, license, redistribution constraints, limitations, and bounded metadata. |
| `MarketContextEventV1` | One immutable schedule, release, revision, decision, communication, shock, or news vintage. |
| `MarketContextTimelineV1` | Ordered vintages, revision chains, declared coverage, completeness, limitations, and deterministic artifact identity. |
| `MarketContextCalendarStateV1` | Compact reuse of the existing HistData session, closure, rollover, holiday, and period-end classifier. |
| `MarketContextQueryV1` | Bounded ex-post or ex-ante sidecar for one half-open interval or reconstruction window. |
| `MarketContextSourceAdapterV1` | Shared normalization seam for operator-approved sources. |

Every external event embeds its complete source contract. The record therefore
retains retrieval/version metadata, a content SHA-256 digest, license and
redistribution policy, source URI where appropriate, adapter version, affected
currencies/symbols, confidence, limitations, and source-time normalization.
Raw source text need not be redistributed. A licensed adapter may store only a
content hash and operator-approved normalized values.

## Event identity and revisions

`canonical_key` identifies the economic or news event across vintages. It is
normalized to a stable lowercase identifier. `event_id` hashes the complete
event vintage, including source provenance and values.

An initial record has `revision_sequence=0`. Each later vintage must:

- name the immediately preceding `event_id`;
- retain the canonical key, semantic market-event time, and first-known time;
- increment the sequence by exactly one; and
- have a strictly later availability timestamp.

The timeline retains both the first release and every revision. It rejects
duplicate IDs, duplicate logical vintages, orphan revisions, sequence gaps,
and revisions that rewrite the original event identity. A revision never
updates an earlier object in place.

Scheduled macro records may carry expected, actual, previous, revised-previous,
and surprise values with explicit units. When expected and actual are both
present, surprise is deterministically `actual - expected`; contradictory
values fail validation.

## Knowledge time versus market-event time

Three times have distinct meanings:

- `event_time_ns` is when the target market event occurs;
- `first_known_at_ns` is when the existence or schedule was first known; and
- `available_at_ns` is when this exact vintage became usable.

An ex-ante query requires `as_of_ns` and excludes any vintage whose first-known
or availability time is later. Ex-post queries expose all matching vintages.
This lets a schedule be visible before a release without exposing the actual or
a later revision.

When query events are added to `ReconstructionInformationInputV1`, the
information graph uses the vintage's availability time as its semantic input
event. The target release time remains in `MarketContextEventV1`. This prevents
a known schedule from being mislabeled as realized future information while
the existing #433 audit still rejects an actual or revision used before it was
available.

## Timezone and event-window semantics

Source event timestamps are ISO-8601 strings paired with an IANA timezone.
Normalization verifies that an explicit offset agrees with the named zone.
Naive local times are localized with `zoneinfo`:

- ambiguous daylight-saving folds require an explicit `source_time_fold`;
- nonexistent wall-clock times fail closed; and
- the normalized value must equal the stored UTC nanosecond timestamp.

Each record declares `pre_event_ns` and `post_event_ns`. Its conditioned window
is exactly:

```text
[event_time_ns - pre_event_ns, event_time_ns + post_event_ns)
```

This half-open rule makes boundary behavior deterministic across streaming
windows.

Unscheduled shocks and news windows cannot claim exact precision. They require
`approximate` or `window_only` precision, a confidence value, an explicit
ambiguity reason, and limitations.

Policy-rate transitions use `policy_rate_change`. They are not interchangeable
with `central_bank_decision`: a rate history can establish that no level
changed during a supported interval, but it cannot establish that no meeting
or unchanged-rate decision occurred.

## Bounded streaming joins

`query_market_context_window()` consumes `ReconstructionWindowV1` and returns a
bounded `MarketContextQueryV1`. It filters by the synchronized window's symbols,
optional currencies and event kinds, point-in-time availability, and
conditioned-window overlap. The result contains event contracts, a compact
calendar classification at the window boundary, the exact timeline/window IDs,
and limitations.

It never emits a dataframe or repeats context over market rows. The default
limit is 512 events per query and 4,096 events per timeline. Exceeding a query
limit raises `MarketContextQueryLimitError` rather than silently truncating
evidence. Large source bodies and analytical enrichment remain outside the
query payload.

## Calendar context

`market_context_calendar_state()` reuses
`classify_histdata_timestamp()` and `calendar_policy_metadata()` from the data
quality domain. It carries session state, clock and active sessions, overlaps,
rollover/open/close/fix tags, holiday/event tags, month/quarter/year-end tags,
calendar profile source/version, completeness, and limitations.

The default holiday profile remains explicitly incomplete and advisory. A
calendar state is a deterministic classification, not evidence that a news or
macro event occurred.

## Explicit missing context

An empty query is never converted into a neutral or invented event label. Its
status is `missing` with one stable reason:

- `no_matching_event` inside complete declared coverage;
- `not_available_as_of` when matching evidence exists but was not yet known;
- `outside_timeline_coverage`; or
- `timeline_incomplete` when an approved source is absent or incomplete.

Calendar state may still be available alongside missing external context. The
two facts remain separate.

## Adapter and trust gates

`MarketContextSourceAdapterV1` exposes only an adapter name/version and
`load_events()`. Collection verifies that every emitted event names that exact
adapter identity. `StaticMarketContextSourceAdapterV1` supplies a deterministic
fixture and normalized-import implementation without implying a production
news source.

Context may condition activity, spread, timing, motif selection, and carving
only when its timeline and query IDs are preserved in the information graph.
It may not be added retrospectively as narrative decoration, treated as
point-in-time evidence before availability, or used to replace missing context
with an invented classification.

Changing required fields, identity rules, revision semantics, query visibility,
or timezone behavior requires a new schema version and contract class.

## Production event corpus

`MarketContextCorpusV1` composes the v1 timeline with the acquisition policy,
content-addressed source evidence, source diagnostics, coverage/missingness
slices, year/currency/type counts, runtime, peak memory, and a deterministic
logical corpus ID. Runtime is reported but excluded from logical identity, so
a replay can prove identical evidence without pretending wall-clock timing is
deterministic.

The initial production adapters deliberately use public official sources:

| Adapter | Selection and mapping | Reuse basis | Point-in-time limitation |
| --- | --- | --- | --- |
| ONS release calendar | Published, allowlisted UK CPI, GDP, labour-market, retail-sales, and public-finance records; maps to GBP, EURGBP, and GBPUSD. | [ONS release-search API](https://developer.ons.gov.uk/search/search-releases/) under [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). | The current search record does not preserve when the schedule was first published. A release becomes eligible only at its retained release time. Date changes without change-known times remain limitations, not invented revisions. |
| ECB key interest rate | `FM.D.U2.EUR.4F.KR.MRR_RT.LEV`, the combined minimum-bid/fixed-rate daily MRO level from 1999; maps to EUR, EURUSD, and EURGBP. The raw daily response is preserved byte-for-byte and the derived corpus suppresses unchanged consecutive levels. | [ECB Data API](https://data.ecb.europa.eu/help/api/data) under the [ECB statistics reuse policy](https://www.ecb.europa.eu/stats/ecb_statistics/governance_and_quality_framework/html/usage_policy.en.html). Derived events retain `Source: ECB statistics`; raw statistics and metadata are not modified. | The series covers both the 2000–2008 variable-rate minimum-bid regime and the fixed-rate regimes. Observations are effective state dates, not announcement timestamps. They are `window_only`, become ex-ante eligible on the following UTC day, and contain no inferred consensus. |
| Bank of England Bank Rate | Official Bank Rate history (`IUDBEDR`); maps to GBP, GBPUSD, and EURGBP. | [Bank Rate history](https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp?hl=en-GB) under the Bank's [OGL terms](https://www.bankofengland.co.uk/legal). Third-party database series are excluded. | The table supplies change dates and levels, not original decision times or schedule vintages. Records are full-day and `window_only`; ex-ante eligibility is delayed until the following UTC day. |
| Federal Reserve FOMC | Official historical meeting pages for 2000–2020 plus the current 2021+ calendar; maps to USD, EURUSD, and GBPUSD. | Federal Reserve [historical materials](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm) and [current calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm), normally [public domain](https://www.federalreserve.gov/disclaimer.htm) with attribution. | Historical date-only meetings become eligible after the local meeting day, preventing intraday look-ahead. The current supported calendar uses the Fed's documented [2 p.m. Eastern decision time](https://www.federalreserve.gov/economy-at-a-glance-policy-rate.htm), but never invents when a schedule was first published. |
| Operator public-shock catalog | Small cited public-record windows for major shocks affecting the common triangle. | MIT-licensed normalized factual metadata; every record retains its upstream public-record citation and limitations. | It is explicitly selective. Absence is never a `no_shock` label, and date windows make no causal or exact-timing claim. Date-only windows become ex-ante eligible on the following UTC day; the SNB window retains its sourced announcement time. |

FRED/ALFRED is not used. Its current API terms leave third-party series rights
with each data owner, bind downstream application users to the API terms, and
make the API license terminable. Those conditions do not provide the uniform
reuse basis required for an immutable redistributable corpus. The adapters use
the selected primary official sources above instead. No fixture-only or
unspecified-provider adapter qualifies as production coverage.

## Acquisition, immutable artifacts, and replay

Build the corpus from installed code:

```console
histdatacom analytics market-context-corpus \
  --artifact-dir .histdatacom/market-context \
  --start-date 2002-03-01 \
  --end-date 2026-06-30
```

Acquisition has explicit response, total-byte, page, event, timeout, and
runtime bounds. It sends a named user agent, fails on an empty or oversized
response, and records every exact request URI, query/offset, retrieval time,
content type, response SHA-256, provider, license URI, adapter version, and
source limitation.

`write_market_context_corpus()` writes:

```text
artifact directory/
├── sources/<source-key>-<content-sha256>.<type>
├── market-context-timeline-<artifact-sha256>.json
└── market-context-corpus-<artifact-sha256>.json
```

Files are content addressed. An existing path with different bytes is refused;
an identical path is reused. Prior refreshes are never silently replaced.
`replay_market_context_corpus()` restores every raw body from its source
evidence, verifies sizes and hashes, re-runs the production adapters, and
requires the rebuilt logical corpus ID to match.

## Coverage and preflight

Coverage is not inferred from event density. ECB support is bounded by the
first and last parsed daily observations and becomes incomplete if the daily
state has a gap. Bank Rate support starts with the first parsed historical
change and ends at the snapshot retrieval day; invalid source rows make the
slice incomplete. Those sources can therefore support an interval containing
no transition without treating a sparse or broken response as complete. ONS
and Federal Reserve live archives are bounded by their first and last retained
official records. The operator shock catalog is always incomplete by design.

`preflight_market_context_corpus()` evaluates required currency/kind pairs
against these declared source intervals. `require_market_context_corpus()`
raises `MarketContextCorpusPreflightError` for an unsupported period, currency,
event kind, incomplete source, or outside-timeline request. By contrast, a
supported ordinary window with no matching event returns
`no_matching_event`; that is not a preflight failure and is not converted into
a neutral invented event. `query_market_context_corpus()` performs this
preflight automatically whenever the caller declares both an event kind and a
currency (directly or through a six-letter FX symbol). Exploratory inspection
must opt out explicitly with `require_supported=False`.

## Carving and benchmark consumption

`read_market_context_corpus()` strictly loads the self-contained artifact.
`query_market_context_corpus()` returns the exact `MarketContextQueryV1`
already consumed by `carve_empirical_motif_candidates()`, so no notebook or
row materialization is needed. `market_context_benchmark_event_state()`
projects the same bounded query onto the benchmark's existing compact
`event_state` string. Timeline, query, event-vintage, and information-input IDs
remain available for leakage and output-lineage audits.

An ex-ante query hides actuals, late publications, future shocks, and later
revisions until their `available_at_ns`. Missing revision history remains an
explicit source limitation instead of being reconstructed from the latest
page.

## CFTC Commitments of Traders boundary

Commitments of Traders is intentionally not coerced into this event corpus.
COT is a weekly persistent positioning state measured on one date, published
later, and valid until superseded. It needs latest-known-state queries,
high-dimensional position vectors, publication-confidence evidence, and
restatement handling rather than pre/post event overlap. The sibling domain is
documented in
[`cftc-positioning-contracts.md`](cftc-positioning-contracts.md).

COT reuses source-policy, content-addressed artifact, replay, coverage, and
preflight patterns from this corpus. It does not become permanent
augmented columns on every tick; reconstruction will consume one bounded
positioning sidecar per window.

## Real campaign evidence

The #461 closure campaign covered 2002-03-01 through 2026-06-30 and retained
29 official/operator source snapshots. The final audited run read 9,475,207
source bytes in 14.843019 seconds with a 159,039,488-byte peak resident-memory
measurement. It produced 1,028 unique in-range events after detecting 15
duplicate ONS logical records: 726 scheduled UK macro releases, 48 ECB and 47
Bank of England policy-rate changes, 202 Federal Reserve decisions, and five
in-range curated shocks. The event timeline spans 2002-03-19 through
2026-06-30.

Coverage keeps those policy semantics separate. ECB and Bank of England
support `policy_rate_change` over the full requested interval; this does not
claim that an unchanged-rate meeting did not occur. Federal Reserve historical
and current meeting pages support `central_bank_decision` over the interval.
ONS macro coverage begins at its first retained archive record on 2016-02-19
and preflight refuses earlier GBP macro requirements. The only source
diagnostic was the explicitly cancelled 2020-03-18 FOMC meeting; cancellation
is retained as a diagnostic and is not emitted as a decision.

The final logical corpus ID is
`market-context-corpus:sha256:4dcd5319d8e0e2e5a7ed7f4d2950a1bb129fb12323d09bdd2c283524fb5f952d`;
the timeline ID is
`market-context-timeline:sha256:ee2761b90c65967b925745a4e9de23b040c3b82c5ac18ac0aec393db2dad1fd1`;
and the self-contained corpus artifact SHA-256 is
`9255f8c39f999b7a54e41a59a6f1d96f02e897af8383795e464a2f8738b08e00`.
An artifact reload and raw-source replay produced the same logical corpus ID. A
2025 policy preflight passed separately for EUR/GBP rate changes and USD FOMC
decisions, while a query just before the SNB shock returned
`not_available_as_of`. A 2010 GBP macro preflight was refused.

The retained sources emitted zero revision vintages (0 of 1,028 events,
0.00%). This is reported as source missingness, not evidence that no historical
revision occurred: the current ONS, policy-rate, and meeting archives do not
retain complete change-known vintage history. The operator revision and late
publication paths are exercised with deterministic fixtures.

| Year | EUR policy-rate changes | GBP policy-rate changes | GBP macro releases | USD FOMC decisions | Curated shocks |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2002 | 1 | 0 | 0 | 7 | 0 |
| 2003 | 2 | 3 | 0 | 9 | 0 |
| 2004 | 0 | 4 | 0 | 8 | 0 |
| 2005 | 1 | 1 | 0 | 8 | 0 |
| 2006 | 5 | 2 | 0 | 8 | 0 |
| 2007 | 2 | 4 | 0 | 8 | 0 |
| 2008 | 4 | 5 | 0 | 8 | 1 |
| 2009 | 4 | 3 | 0 | 8 | 0 |
| 2010 | 0 | 0 | 0 | 8 | 0 |
| 2011 | 4 | 0 | 0 | 8 | 0 |
| 2012 | 1 | 0 | 0 | 8 | 0 |
| 2013 | 2 | 0 | 0 | 8 | 0 |
| 2014 | 2 | 0 | 0 | 8 | 0 |
| 2015 | 0 | 0 | 0 | 8 | 1 |
| 2016 | 1 | 1 | 11 | 8 | 1 |
| 2017 | 0 | 1 | 15 | 8 | 0 |
| 2018 | 0 | 1 | 45 | 8 | 0 |
| 2019 | 0 | 0 | 56 | 8 | 0 |
| 2020 | 0 | 2 | 56 | 13 | 1 |
| 2021 | 0 | 1 | 91 | 8 | 0 |
| 2022 | 4 | 8 | 97 | 8 | 1 |
| 2023 | 6 | 5 | 106 | 8 | 0 |
| 2024 | 4 | 2 | 98 | 8 | 0 |
| 2025 | 4 | 4 | 102 | 9 | 0 |
| 2026 | 1 | 0 | 49 | 4 | 0 |
