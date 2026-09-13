# Economic-calendar branch migration map

This map reconciles the sole unique commit on
`codex/492-economic-calendar-vintages` with #533, #552, and #682. The audited
source is commit `63da3b6`. The branch contains one calendar module plus CLI,
reconstruction, tests, and documentation integrations built around an
operator-licensed commercial calendar. It must not be merged wholesale.

The classifications mean:

- **retain**: semantics remain materially intact;
- **refactor**: useful semantics survive behind a provider-neutral contract;
- **supersede**: an installed neutral or official-source surface replaces it;
- **drop**: commercial-provider behavior is intentionally not canonical; and
- **defer**: still useful, but belongs to a later source-registry or coverage
  issue and is not implied by #682.

## Module and class disposition

| Branch symbol | Class | Disposition and installed replacement |
| --- | --- | --- |
| `market_context.economic_calendar` | module | **refactor** — replaced by the provider-neutral module of the same path; acquisition is excluded. |
| `EconomicCalendarFetchProfileV1` | class | **drop** — its country/date request, credential, rate, response, and subscription limits describe one provider. Official adapters own bounded fetch profiles under #535. |
| `EconomicCalendarSourceEvidenceV1` | class | **supersede** — `MarketContextSourceV1` supplies neutral source/version/hash/license evidence; source snapshots remain adapter-owned. |
| `EconomicCalendarEventV1` | class | **refactor** — split into `EconomicCalendarReleaseV1`, `EconomicCalendarForecastV1`, and `EconomicCalendarEventStateV1`, so provider fields cannot define canonical truth. |
| `TradingEconomicsCalendarAdapterV1` | class | **drop** — the commercial adapter and raw schema are not a canonical dependency. `EconomicCalendarSourceAdapterV1` is the neutral seam. |
| `EconomicCalendarCoverageV1` | class | **defer** — corpus interval/completeness is explicit now; source/series coverage matrices belong to #535/#537. |
| `EconomicCalendarCorpusV1` | class | **retain/refactor** — retains immutable releases, forecasts, coverage, limitations, deterministic identity, and `as_known_at()`, with no provider profile or pair-coverage claims. |
| `EconomicCalendarCorpusBuildV1` | class | **supersede** — `build_economic_calendar_corpus()` collects neutral adapters; raw snapshot bundles stay with each official acquisition layer. |
| `_FetchBudget` | class | **drop** — commercial API request/rate/byte accounting does not belong in the canonical contract. Official fetchers must remain bounded independently. |

## Function disposition

| Branch function | Disposition and installed replacement |
| --- | --- |
| `_required_text` | **supersede** — bounded neutral validation is local to the new module. |
| `_optional_text` | **supersede** — local neutral validator. |
| `_strict_int` | **supersede** — `_bounded_ns()` and explicit integer checks validate canonical fields. |
| `_bounded_int` | **supersede** — `_bounded_ns()` and contract-specific bounds. |
| `_finite_float` | **supersede** — `_finite()`. |
| `_optional_number` | **supersede** — `_optional_finite()`; parsing upstream lexical fields is adapter-owned. |
| `_parse_date` | **drop** — provider request-window parsing is acquisition-layer work. |
| `_parse_utc_ns` | **supersede** — official adapters normalize timestamps before emitting neutral contracts. |
| `_optional_utc_ns` | **supersede** — official adapters normalize optional timestamps. |
| `_utc_text` | **refactor** — retained only for exact `MarketContextEventV1` compatibility projection. |
| `_canonical_json` | **supersede** — shared `canonical_contract_json()`. |
| `_stable_id` | **retain/refactor** — deterministic SHA-256 identity remains, over neutral payloads. |
| `_mapping` | **supersede** — local strict deserialization helper. |
| `_sequence` | **supersede** — local strict deserialization helper. |
| `_string_tuple` | **supersede** — field-specific tuple normalizers. |
| `_source_key` | **supersede** — source identity is governed by `MarketContextSourceV1`; canonical event identity uses official series keys. |
| `_sha256` | **retain/refactor** — strict lowercase digest validation remains local. |
| `validate_api_key_env_name` | **drop** — the canonical module has no credential path. |
| `histdata_pair_economies` | **defer** — source registry and indicator relevance policy must bind instruments under #535/#552; a commercial country map is not scientific truth. |
| `histdata_economy_symbols` | **defer** — same registry-owned replacement as the forward map. |
| `parse_calendar_number` | **refactor** — neutral contracts preserve lexical and finite numeric values; source-specific suffix/currency parsing belongs in official adapters. |
| `_provider_number` | **drop** — provider raw-field fallback rules are not canonical. |
| `_event_kind` | **drop** — commercial category/title regexes cannot define `MarketContextKind`; official adapters bind it explicitly. |
| `_date_windows` | **drop** — commercial API request planning is not retained. |
| `trading_economics_request_uri` | **drop** — commercial endpoint construction is removed. |
| `economic_calendar_fetch_plan` | **drop** — its plan is inseparable from the superseded country/date API. |
| `_event_from_row` | **drop/refactor** — raw provider decoding is removed; official adapters emit `EconomicCalendarReleaseV1` and `EconomicCalendarForecastV1`. |
| `_pair_coverage_payload` | **defer** — neutral source/series coverage belongs to #535/#537. |
| `_counts_by_year_currency_category` | **defer** — diagnostics may return with official-source taxonomy; commercial categories are not retained. |
| `_snapshot` | **drop** — commercial response construction is removed. |
| `_fetch_response` | **drop** — commercial HTTP/auth/rate semantics are removed. |
| `_fetch_range` | **drop** — provider pagination/splitting is removed. |
| `fetch_trading_economics_calendar_snapshots` | **drop** — canonical operation does not require the licensed path. |
| `_coverage_complete` | **defer** — neutral completeness is declared explicitly; source-specific proof belongs to #537. |
| `_merge_event` | **retain/refactor** — immutable deduplication becomes strict deterministic ID uniqueness and contiguous predecessor validation in `EconomicCalendarCorpusV1`. |
| `build_economic_calendar_corpus_from_snapshots` | **supersede** — `build_economic_calendar_corpus()` consumes neutral adapters rather than a provider snapshot schema. |
| `build_live_economic_calendar_corpus` | **drop/defer** — live acquisition moves to bounded official adapters under #535; no commercial default remains. |
| `_artifact_ref` | **supersede** — the neutral writer returns the shared `ArtifactRef` directly. |
| `_write_immutable` | **retain/refactor** — `write_economic_calendar_corpus()` uses content addressing and refuses differing existing bytes; restricted commercial raw storage is removed. |
| `write_economic_calendar_corpus` | **retain/refactor** — writes a self-contained neutral corpus, not provider payloads. |
| `read_economic_calendar_corpus` | **retain/refactor** — verifies content-addressed name, digest, size, schema, identities, and chains. |
| `replay_economic_calendar_corpus` | **retain/refactor** — reconstructs exact canonical identities; raw official replay remains adapter-owned. |
| `_latest_visible_events` | **retain/refactor** — implemented by `query_economic_calendar_as_known()` with `available_at_ns <= decision_at_ns`. |
| `_project_market_event` | **retain/refactor** — `project_economic_calendar_state()` preserves the compatibility seam and tags projection provenance. |
| `query_economic_calendar_corpus` | **retain/refactor** — replaced by `query_economic_calendar_as_known()` and the `EconomicCalendarAsKnownReaderV1` protocol. |
| `context_corpus_artifact_kind` | **supersede** — consumers depend on the reader protocol and retain the concrete input artifact ID separately, not a branch-private union. |
| `context_corpus_event_times` | **supersede** — bounded reader queries replace corpus-type inspection. |
| `read_context_corpus` | **supersede** — explicit typed readers avoid filename-based union dispatch. |
| `preflight_context_corpus` | **defer** — installed market-context preflight remains; comprehensive official series coverage follows #535/#537. |
| `query_context_corpus` | **supersede** — strategy code consumes `EconomicCalendarAsKnownReaderV1`, never provider or corpus union classes. |

## Constants and assumptions

The six branch schema constants are superseded by separate release, forecast,
state, query, and corpus schemas. Generic event/query/adapter/corpus byte bounds
remain, but request, response, rate, row, and provider-source bounds are
dropped with acquisition. All `TRADING_ECONOMICS_*` constants, the credential
name regex, country/instrument provider maps, provider event classifiers, and
provider value multipliers are dropped. Official adapters may define their own
documented mappings without exporting them as universal calendar truth.

## Integration disposition

The branch's CLI and reconstruction edits are **superseded**. They dispatch on
a union containing the licensed corpus and expose provider credential and
license-acknowledgement flags. The canonical consumer seam is now
`EconomicCalendarAsKnownReaderV1`; future trader/event strategies receive that
protocol by dependency injection. Existing official market-context adapters
can bridge with `economic_release_from_market_context()` while richer adapters
emit the release and forecast contracts directly.

The branch tests are **refactored** into official-source-neutral tests covering
availability cutoffs, immutable actual/revision chains, previous-as-known,
observed-versus-machine forecast provenance, adapter interchangeability,
content-addressed replay, and absence of the commercial dependency. Commercial
request planning, credential, rate-limit, and licensed-payload tests are
**dropped**.

The branch documentation is **superseded** by
[`economic-calendar-contracts.md`](economic-calendar-contracts.md). Deferred
capabilities are exhaustive source/series coverage, live official acquisition,
raw-snapshot replay, importance/shock calibration, and complete professional
consensus history. Those remain governed by #534, #535, #537, #581, #582, and
source-specific descendants; #682 does not claim them complete.
