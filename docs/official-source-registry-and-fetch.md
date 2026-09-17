# Authoritative-source registry and bounded official-data fetches

Economic-calendar acquisition starts from a reviewed legal producer, not from
an aggregator, a commercial calendar, or whichever mirror is easiest to
download. The installed `OfficialSourceRegistryV1` freezes that choice before
an adapter can plan a request. Every request then retains the exact official
bytes and enough identity to replay parsing without network access.

This is the acquisition foundation implemented by #535. The reusable
[official-source adapters](official-source-adapters.md) implement the typed
boundary established here. A format declared by an endpoint is still only a
capability until its first-party fixture qualification passes.

## Reviewed scope and authority rule

The packaged registry is
`histdatacom/market_context/assets/official_sources_v1.json`. Its 71 entries
cover every cell in the 21-economy by 12-event-family matrix. Exactly one entry
with role `primary-producer` must exist in every cell. Registry construction
fails on a missing or ambiguous cell.

The source choice follows these rules:

1. the institution legally producing a statistic is primary;
2. a central bank is primary for its policy decisions and the monetary or
   financial series it produces;
3. a central-bank copy of CPI, employment, GDP, trade, or another statistic is
   at most an independent official cross-check when the statistics office or
   ministry is the legal producer;
4. Eurostat is the statistical producer for euro-area aggregates, while
   national offices remain primary for Germany and France;
5. ECB decisions are the monetary-policy source for the euro area, Germany,
   and France; Bundesbank and Banque de France remain primary only for their
   own national money/credit series; and
6. no fetch operation follows `fallback_source_keys` automatically. A fallback
   must be selected explicitly, producing a different `source_id`, request
   manifest, and corpus lineage.

The current high-level matrix is:

| Economy | Non-monetary statistical producer(s) | Monetary/policy producer(s) |
| --- | --- | --- |
| Australia | Australian Bureau of Statistics | Reserve Bank of Australia |
| Canada | Statistics Canada | Bank of Canada |
| Switzerland | Swiss Federal Statistical Office | Swiss National Bank |
| Czech Republic | Czech Statistical Office | Czech National Bank |
| Germany | Destatis | ECB (policy); Deutsche Bundesbank (money/credit) |
| Denmark | Statistics Denmark | Danmarks Nationalbank |
| Euro area | Eurostat | European Central Bank |
| France | INSEE | ECB (policy); Banque de France (money/credit) |
| United Kingdom | Office for National Statistics | Bank of England |
| Hong Kong | Census and Statistics Department | Hong Kong Monetary Authority |
| Hungary | Hungarian Central Statistical Office | Magyar Nemzeti Bank |
| Japan | Statistics Bureau, Cabinet Office/ESRI, METI, MOF, and MLIT by legal remit | Bank of Japan |
| Mexico | INEGI | Banco de México |
| Norway | Statistics Norway | Norges Bank |
| New Zealand | Stats NZ | Reserve Bank of New Zealand |
| Poland | Statistics Poland | Narodowy Bank Polski |
| Sweden | Statistics Sweden | Sveriges Riksbank |
| Singapore | Singapore Department of Statistics | Monetary Authority of Singapore |
| Türkiye | Turkish Statistical Institute | Central Bank of the Republic of Türkiye |
| United States | BLS, BEA, Census, Federal Reserve Board, and Treasury by legal remit | Federal Reserve Board/FOMC |
| South Africa | Statistics South Africa | South African Reserve Bank |

Representative reviewed machine interfaces include the [ECB SDMX
service](https://data.ecb.europa.eu/help/getting-data-web-services-sdmx-0),
[Eurostat dissemination APIs](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started),
[ABS SDMX API](https://www.abs.gov.au/statistics/application-programming-interfaces-apis/data-api-user-guide),
[Statistics Canada WDS](https://www.statcan.gc.ca/en/developers/wds),
[Statistics Norway PxWeb API](https://www.ssb.no/en/api/pxwebapi),
[SingStat TableBuilder](https://tablebuilder.singstat.gov.sg/view-api/for-developers),
and the [Bank of Japan time-series
API](https://www.stat-search.boj.or.jp/info/api_manual_en.pdf). Each registry
entry retains its own official verification URI; these examples are not
substitutes for the complete matrix.

`reviewed-entrypoint` is intentionally weaker than
`empirically-verified`. It proves that the entrypoint and institutional remit
were reviewed. It does not prove complete indicator coverage, exact historical
depth, historical publication times, or original vintages. The registry marks
those series-dependent obligations explicitly so an economy backfill cannot
close on an institution name alone.

The dedicated ECB monetary-policy archive entry is `empirically-verified` for
the three key interest rates. Its versioned FOEDB database, 81 data chunks,
15 December 1999 predecessor, and all 298 decision pages from 2000 through
10 September 2026 are content-addressed and replayable. The qualification
keeps 226 legacy records date-only, retains 72 exact publication minutes, and
parses both the historical main-refinancing-first and current deposit-first
rate order. See the [euro-area, Germany, and France backfill](euro-area-germany-france-backfill.md).

The Federal Reserve G.17 industrial-production entry is the first route raised
to `empirically-verified`: its official release index and all 319 enumerated
plain-text publications through August 2026 have content-addressed manifest
evidence and parser replay. That status is deliberately limited to the total
industrial-production headline; it does not qualify capacity utilization,
component series, current revised databases, or another source entry.

The separate Federal Reserve H.6 entry is also `empirically-verified`. Its
official JSON ledger, 1,170 value-bearing release artifacts including one
predecessor, and 1,075 occurrence-authored timing PDFs qualify seasonally
adjusted M2 publications from 2000 through August 2026. The binding uses the
dedicated `official.federal-reserve-h6.v1` parser across HTML, JSON, PDF, and
one official text fallback; it does not inherit evidence from the FOMC route
or treat stale and special-notice PDF links as publication clocks.

The DOL/ETA Initial Claims entry is `empirically-verified` only for the
regular-state seasonally adjusted headline. Its dedicated
`official.dol-eta-initial-claims.v1` parser and annual POST-index contract bind
1,240 unique HTML/ASP/PDF releases from October 2002 through September 2026,
1,238 comparable triplets, 26 exact index receipts, and three hash-bound index
exclusions. The entry names the pre-archive boundary, omitted October 2019
artifact, seven unpublished 2025 shutdown occurrences, and noncomparable
restart instead of substituting ETA's current revised time series.

The registry's complete authority matrix feeds the
[canonical indicator catalog](economic-indicator-catalog.md). The catalog
adds exact concept, methodology-era, recurrence-rule, and expected-occurrence
identity while retaining this registry's immutable legal-producer choice.

## Versioned contracts

| Contract | Responsibility |
| --- | --- |
| `OfficialSourceEntryV1` | Institution, jurisdiction, legal-producer role, event families and canonical indicator families, endpoint/host/format/MIME capabilities, source IDs, release calendar/archive, timezone and precision, history/break/revision notes, rate/auth/terms, fallback identities, parser identity, replay policy, owner, and review evidence. |
| `OfficialSourceRegistryV1` | Ordered source entries, exact economy scope, complete unique primary-source matrix, fallbacks, and deterministic registry identity. |
| `OfficialFetchPolicyV1` | Attempts/backoff, redirects, connect/read timeouts, response/total bytes, requests/pages/events, runtime, chunk size, and user agent. |
| `OfficialSourceRequestV1` | Credential-free method, URI, public query/body/headers, chosen source format, parser identity, date window, page, and conditional validators. |
| `OfficialRequestManifestV1` | Ordered request plan plus exact registry and policy identity. |
| `OfficialRawSnapshotV1` | Exact bytes, response/status/final-URI evidence, safe headers, source clocks, validators, hash/size, attempt count, and explicit 304 predecessor reuse. |
| `OfficialFetchBundleV1` | One request manifest and exactly one ordered snapshot per request. |
| `OfficialSourceParserV1` | Parser ID/version, supported formats, and bounded normalized-record output. |
| `OfficialSourceTimestampV1` | Source lexical timestamp, IANA timezone, precision, DST fold, and normalized UTC nanoseconds. |

All identities use canonical JSON and SHA-256. Changing an endpoint, allowed
host, selected format, source role, parser ID/version, request parameter,
conditional validator, response byte, or response clock changes the relevant
identity. Retrieval credentials are the deliberate exception: secrets affect
authorization, never scientific identity, and are never serialized.

## Request planning and source binding

Date windows are inclusive and gap-free. Pagination and window expansion occur
before network access and are capped by the policy:

```python
from histdatacom.market_context import (
    OfficialFetchPolicyV1,
    build_official_request_manifest,
    load_packaged_official_source_registry,
    plan_official_source_requests,
)

registry = load_packaged_official_source_registry()
source = registry.source("ca.boc.valet")
policy = OfficialFetchPolicyV1(max_requests=32, max_pages=16)
requests = plan_official_source_requests(
    source,
    policy,
    uri_template=(
        "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json"
    ),
    start_date="2025-01-01",
    end_date="2025-12-31",
    max_window_days=93,
    start_parameter="start_date",
    end_parameter="end_date",
)
manifest = build_official_request_manifest(
    registry,
    requests,
    policy=policy,
)
```

The registry endpoint is the reviewed service or archive root. A protocol
adapter supplies `uri_template` when a concrete dataset, series, table, or
release path is required; only registered hosts and the bounded
`{start}`/`{end}`/`{page}`/`{page_size}` placeholders are accepted. The
manifest builder verifies the exact `source_key`/`source_id`, registered host,
selected source format, and parser ID/version. A request cannot carry an
undeclared host or a credential parameter. Serialized request headers reject
authorization, cookies, keys, tokens, and secrets.

Both GET and POST are available because official services differ: BLS and
PxWeb-style interfaces may require POST bodies, while many SDMX and JSON APIs
use GET. Bodies and their media types are retained exactly. Protocol adapters
are responsible for canonical request-body construction and source-specific
pagination semantics.

## Bounded acquisition and corruption checks

`fetch_official_request_manifest()` applies one shared policy to the entire
manifest. Production uses `requests.request`; tests and specialized runtimes
can inject `OfficialHttpTransportV1`. The fetcher:

- sets explicit user-agent, accepted media types, identity encoding, and
  connect/read timeouts;
- retries only transient network failures and explicit transient HTTP statuses
  within the finite attempts/backoff schedule; malformed content, integrity
  failures, and permanent HTTP statuses fail immediately;
- caps declared and streamed bytes per response and across the bundle;
- caps requests, pagination, serialized manifests, normalized events, and total runtime;
- rejects empty, incomplete, non-byte, unregistered-MIME, and
  format-signature-mismatched responses, and rejects content encoding because
  an automatically decoded transport body would not be the wire representation;
- records only bounded provenance headers, dropping cookies and other secret
  material;
- manually follows a finite redirect chain only for credential-free requests,
  refuses unregistered hosts and POST redirects that change method semantics,
  and records the final registered URI; and
- never tries another endpoint or producer after failure.

The generic signature check catches gross corruption: JSON/JSON-stat must begin
as JSON, SDMX/feed documents must have a structured opening, PDF and Office
files must have their binary signatures, ICS must begin as a calendar, and
text tables cannot contain NUL bytes. Protocol parsers add schema checks,
workbook/table versions, codelists, and drift checks.

## Conditional requests

`condition_official_request()` copies ETag and Last-Modified evidence only from
the exact prior request target (all fields except the conditional validators).
On HTTP 304, the fetcher requires that
conditional request and its explicitly supplied predecessor snapshot. The new
snapshot retains the previous bytes and hash plus
`reused_from_snapshot_id`; status 304 never becomes an empty source artifact.

This keeps each refresh self-contained while preserving the fact that the
server did not send a new body.

## Credentials

The registry records only an authentication kind, environment-variable name,
and exactly one header name, query-parameter name, or path placeholder. It
contains no secret.
`environment_official_credentials()` resolves a value at execution time.
Custom credential providers may use a keychain or workload identity.

Header credentials are added after the serializable headers are frozen. Query
credentials are added to a temporary transport parameter map and stripped from
the retained resolved URL. For official APIs such as INEGI that require a key
inside the path, the serialized URI contains the literal `{credential}` and
the fetcher substitutes an escaped runtime value, then restores the placeholder
before retaining redirect evidence. A required missing credential fails before
network success can be claimed; an optional key may be absent when the official
API offers a lower anonymous quota.

## Immutable artifacts and replay

```python
from histdatacom.market_context import (
    fetch_official_request_manifest,
    replay_official_fetch_bundle,
    write_official_fetch_bundle,
)

bundle = fetch_official_request_manifest(registry, manifest)
artifacts = write_official_fetch_bundle(bundle, "data/official-fetch")
replayed = replay_official_fetch_bundle(
    artifacts["bundle"].path,
    registry=registry,
)
assert replayed.bundle_id == bundle.bundle_id
```

Raw bodies are stored once at `sources/<content-sha256>.bin`. The bundle JSON is
itself content addressed. Writes are atomic and refuse different bytes at an
existing immutable path. Replay verifies the bundle filename hash, request
manifest ID, every request/snapshot/source hash and size, response order,
shared policy limits, bundle ID, and—when supplied—the current registry ID.

Parsers consume only `OfficialRawSnapshotV1`. `parse_official_snapshot()`
requires the request's exact parser ID/version and selected format, caps the
record count, requires mapping-shaped records, and caps each canonical record.
Thus a real adapter can be rerun from retained official bytes without a live
endpoint, while a parser upgrade creates new lineage rather than silently
changing old normalized records.

## Time and vintage discipline

`normalize_official_source_timestamp()` uses the existing fail-closed IANA
timezone normalizer but retains the original lexical string, named zone,
precision, and DST fold. Ambiguous local times require an explicit fold;
nonexistent local times and conflicting numeric offsets are rejected.

A response `Date`, ETag, Last-Modified, or database update timestamp is source
clock evidence, not automatically an economic release time. Likewise,
historical observations from a current revised database are not historical
vintages. `HISTORICAL_VINTAGES` may be declared only where the official
interface exposes prior versions; economy adapters still require empirical
tests or retained contemporaneous release artifacts governed by the
[archive-first vintage layer](archive-vintage-reconstruction.md).

## Closure boundary

#535 establishes the complete reviewed institutional matrix and reusable
transport/replay contracts; #584 adds reusable protocol parsing and fixture
qualification. Together they deliberately leave these claims open for economy
backfills and dependent issues:

- concrete source-specific series/table/release IDs and empirical coverage in
  economy backfills;
- concrete archive parsing and backfills using the implemented release-chain
  and [archive-vintage](archive-vintage-reconstruction.md) contracts;
- observed professional or official consensus (#536/#583); and
- professional calendar materialization and display resolution (#582/#585).

That boundary prevents either a successful HTTP response or a parseable table
from being mistaken for a qualified point-in-time economic-calendar corpus.
