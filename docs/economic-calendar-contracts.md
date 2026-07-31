# Comprehensive economic-calendar contracts

The comprehensive calendar is an operator-licensed companion to the small
redistributable official-source corpus described in
[`market-context-contracts.md`](market-context-contracts.md). It is designed
for Trading Economics-style country calendars covering every HistData.com
instrument from 2000 onward, with forecast, provider forecast, previous,
actual, revised-previous, schedule, and refresh-vintage evidence.

The code does not bundle provider data, credentials, or a subscription. A live
backfill requires the operator's own Trading Economics API entitlement and an
explicit acknowledgement that local acquisition and retention are allowed.
The resulting raw and normalized artifacts remain restricted-source data and
are not suitable for package or repository distribution.

## Provider decision

Trading Economics is the implemented provider seam because its documented
calendar schema supplies stable calendar IDs, release and reference times,
country, category, event, source, actual, previous, consensus forecast,
provider forecast, importance, last update, revised value, currency, unit,
ticker, and symbol. Requests use the documented country/date API and request
numeric companions with `values=true`.

Forex Factory is deliberately not scraped. Its notices prohibit copying,
republication, and redistribution of the calendar schedule and specifications.
The weekly RSS/ICS surfaces are also not a 2000-to-present vintage archive.

This provider choice has two important nonclaims:

- The current implementation has no bundled license and has not run the full
  commercial backfill in project CI. `--plan-only` is the credential-free
  verification surface.
- A historical response can preserve the provider's original calendar row and
  its `Revised` field, but it cannot prove every intermediate correction time.
  Exact row changes observed in later refreshes are accumulated as immutable
  vintages. A complete pre-2000-to-present correction history is therefore
  limited by what the licensed provider account returns, not reconstructed or
  invented by this package.

## Coverage matrix

`histdata_pair_economies()` maps all 66 public `Pairs` enum members to calendar
economies. The default acquisition profile includes 21 economies:

| Instrument component | Calendar economy |
| --- | --- |
| AUD, CAD, CHF, CZK, DKK, GBP, HKD, HUF, JPY, MXN, NOK, NZD, PLN, SEK, SGD, TRY, USD, ZAR | The corresponding national economy |
| EUR | Euro Area |
| FRX | France, plus its EUR quote economy |
| GRX | Germany, plus its EUR quote economy |
| Other national indices | The index economy plus any distinct quote-currency economy |
| XAU, XAG, BCO, WTI | The quote-currency economy; WTI/USD is United States |

Every FX symbol receives the union of its base- and quote-currency economies.
Metals and global commodities do not receive a fabricated national issuer.
`histdata_economy_symbols()` provides the reverse mapping used to bind a
country event to all affected HistData symbols.

The default 2000-01-01 through 2026-07-30 profile produces 567 initial
country/date requests using inclusive 366-day windows. A response at the
provider's documented 1,000-row ceiling is split recursively until each leaf
is below the ceiling or a single-day leaf proves that the provider cannot
return the requested coverage safely. The fetcher also enforces request,
response-byte, total-byte, event, runtime, and rate bounds.

## Field and vintage semantics

`EconomicCalendarEventV1` retains the raw lexical values and parsed numeric
values. Scaled strings such as `198K`, `$-70.5B`, percentages, and accounting
parentheses remain recoverable while numeric companions support calculation.

| Provider evidence | Contract field |
| --- | --- |
| `Actual` / `ActualValue` | `actual_raw` / `actual_value` |
| `Previous` / `PreviousValue` | `previous_raw` / `previous_value` |
| `Forecast` / `ForecastValue` | `forecast_raw` / `forecast_value` |
| `TEForecast` / `TEForecastValue` | `provider_forecast_raw` / `provider_forecast_value` |
| `Revised` / `RevisedValue` | `revised_raw` / `revised_value` |
| `Date`, `ReferenceDate`, `LastUpdate` | release, reference, and provider-update times |
| `CalendarId` | stable logical event key within the provider |

Trading Economics uses `Revised` for the old value that preceded its corrected
`Previous`. The common `MarketContextEventV1` projection therefore exposes
provider `Previous` as `revised_previous_value` and provider `Revised` as
`previous_value`; this preserves the before/after ordering used by the shared
market-context contract.

An initial observed provider row has `revision_sequence=0`. On refresh, a
changed row with the same calendar ID becomes a new immutable revision naming
the prior event ID. This includes value corrections and reschedules. Identical
rows are deduplicated. Provider update time and local observation time are both
retained so an ex-ante query cannot use a revision before it was observed.

Historical schedule publication times are generally unavailable. Backfilled
records therefore become no earlier than their release/provider-update
evidence; the implementation never pretends that an old schedule was known at
an unsourced earlier time. Future recurring snapshots can establish earlier
first-observed schedule evidence.

## Acquisition and credentials

Inspect the complete request and pair/economy plan without credentials:

```console
histdatacom analytics economic-calendar-corpus \
  --start-date 2000-01-01 \
  --end-date 2026-07-30 \
  --plan-only
```

For licensed acquisition, put the key in a dedicated environment variable;
never pass it on the command line:

```console
export TRADING_ECONOMICS_API_KEY='client:secret'
histdatacom analytics economic-calendar-corpus \
  --artifact-dir .histdatacom/economic-calendar \
  --start-date 2000-01-01 \
  --end-date 2026-07-30 \
  --provider-license-acknowledged
```

The key is sent only in the `Authorization` header. Fetch plans, request URIs,
exceptions, snapshots, corpus metadata, and artifact names remain
credential-free. Raw source files are created with mode `0600` under a
`restricted-sources` directory.

To preserve newly observed changes, bind the prior corpus explicitly:

```console
histdatacom analytics economic-calendar-corpus \
  --artifact-dir .histdatacom/economic-calendar-refresh \
  --start-date 2000-01-01 \
  --end-date 2026-07-30 \
  --previous-corpus .histdatacom/economic-calendar/economic-calendar-corpus-<sha256>.json \
  --provider-license-acknowledged
```

The writer stores content-addressed raw snapshots and one self-contained
`economic-calendar-corpus-<sha256>.json`. `read_economic_calendar_corpus()`
verifies the filename digest, schema, logical corpus ID, source hashes, and
event revision chains. `replay_economic_calendar_corpus()` rereads the exact
restricted snapshots and requires a deterministic corpus rebuild.

## Consumer integration

`read_context_corpus()`, `preflight_context_corpus()`, and
`query_context_corpus()` accept either the original official-source corpus or
the comprehensive licensed corpus. The reverse-degradation benchmark and
modern reference-motif builder use this common seam and preserve the specific
artifact kind in their dependency lineage.

Queries project only the bounded matching rows into `MarketContextQueryV1`;
the full multi-decade corpus is never materialized as a 4,096-event v1
timeline or repeated onto market ticks. Ex-ante visibility uses the requested
as-of time. Ex-post queries select the latest observed revision. Unsupported
currency, symbol, date, or incomplete acquisition coverage fails preflight
instead of becoming an empty neutral event label.

## Licensing boundary

Trading Economics pricing and terms distinguish ordinary API analysis from
enterprise calendar and redistribution rights. Operators must verify that
their subscription covers the requested history, request volume, retention,
use, and any downstream access. This package records
`redistribution_allowed=false` and does not publish normalized provider rows.

Changing providers requires a new adapter and explicit source/license
evidence. It must not silently alter calendar IDs, value semantics,
availability times, or coverage claims under the v1 schema.
