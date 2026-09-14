# United States official macro-calendar backfill

`histdatacom.market_context.us_backfill` defines the legal-producer inventory,
raw-artifact replay boundary, point-in-time triplets, and closure audit for the
United States 2000-present macro calendar. It specializes the shared economic
calendar, release-vintage, schedule, archive, and family-semantic contracts; it
does not weaken them or treat a current revised time series as an original
release.

## Required program inventory

`built_in_united_states_backfill_profile()` resolves 22 programs through the
reviewed source registry and covers every provider-neutral event family:

| Authority | Programs |
| --- | --- |
| Federal Reserve Board / FOMC | decisions and statements, minutes, SEP, G.17 industrial production, H.6 money stock |
| BLS | CPI, PPI, Employment Situation, JOLTS, Productivity and Costs |
| BEA | GDP stages and Personal Income and Outlays, including PCE |
| Census, including joint HUD/BEA releases | retail sales, durable goods, international trade, housing starts/permits, new-home sales, manufacturing/trade inventories |
| DOL/ETA | weekly initial and continued unemployment-insurance claims |
| Philadelphia Fed | Manufacturing Business Outlook Survey and Survey of Professional Forecasters |
| Treasury | Monthly Treasury Statement |

Each program fixes its legal producer, registry source, archive and schedule
entry points, earliest published date inside the requested window, frequency,
release-time precision, publication stages, archive strategy, previous-value
and revision requirements, permitted official forecast source, and explicit
limitations. An archive start date is a route declaration, not a historical
coverage claim.

The source registry includes separate supplemental entries for DOL/ETA and the
Philadelphia Fed. This prevents the broad family-level BLS and Census entries
from being incorrectly named as the producers of weekly claims or regional Fed
surveys. The Federal Reserve G.17 entry also declares plain text as a real
archive format. The reusable HTML-release adapter preserves such text as one
bounded UTF-8 record, including the exact line count and raw-snapshot lineage.

## Retained real release

The package contains a base64 transport envelope for the exact 150,109-byte
[December 17, 2002 G.17 release](https://www.federalreserve.gov/releases/g17/20021217/g17.txt).
`load_packaged_federal_reserve_g17_2002()` decodes it and refuses a SHA-256
other than:

`5f4a2be1e9dfd9de4656062c86f5693680ab4b8e779677558488874296d9c5c9`

The source-specific parser independently recovers from the release header and
summary table:

- publication: December 17, 2002, 9:15 a.m. EST;
- reference period: November 2002;
- total industrial production MoM actual: `0.1` percent;
- October value as known before this release: `-0.8` percent;
- October value revised in this release: `-0.6` percent.

All three lexical values and their line/column locators are retained. The
normalized result has its own pinned SHA-256, independent of the later time at
which an operator captured the raw file. `federal_reserve_g17_2002_artifact_lock()`
verifies the raw hash, normalized hash, URI, publication time, period, actual,
previous-as-known, and revised-previous values. A changed header, missing table,
changed row shape, absent previous estimate, equal old/revised value, or hash
drift fails closed.

The triplet projects directly to `EconomicCalendarReleaseV1`. Retrospective
archive acquisition is recorded as `first_observed_at_ns`, while
`available_at_ns` remains the evidenced 2002 publication instant. The generic
release retains the raw snapshot/request IDs and both previous values in source
metadata; it does not pretend that the archive was downloaded in 2002.

```python
from histdatacom.market_context import (
    OfficialRawSnapshotV1,
    build_federal_reserve_g17_archive_request,
    economic_calendar_release_from_g17_triplet,
    federal_reserve_g17_2002_artifact_lock,
    load_packaged_federal_reserve_g17_2002,
    load_packaged_official_source_registry,
    parse_federal_reserve_g17_release,
)

registry = load_packaged_official_source_registry()
request = build_federal_reserve_g17_archive_request(registry, "2002-12-17")
content = load_packaged_federal_reserve_g17_2002()
snapshot = OfficialRawSnapshotV1(
    request=request,
    retrieved_at_ns=1_789_400_000_000_000_000,
    completed_at_ns=1_789_400_000_000_000_001,
    status_code=200,
    resolved_uri=request.uri,
    response_headers={"Content-Type": "text/plain"},
    content=content,
    content_type="text/plain",
)
triplet = parse_federal_reserve_g17_release(snapshot)
federal_reserve_g17_2002_artifact_lock().verify(triplet)
release = economic_calendar_release_from_g17_triplet(triplet, snapshot)
```

## Complete G.17 archive qualification

`histdatacom.market_context.us_g17_archive` extends the single retained
qualification fixture to every publication enumerated by the official G.17
release-date index. The packaged `us_g17_archive_v1.json` manifest is observed
as of September 14, 2026 and records:

- 319 releases from January 14, 2000 through August 18, 2026;
- 49,791,597 exact source bytes and 319 distinct raw SHA-256 digests;
- 319 distinct normalized triplet digests;
- 261 releases that changed the previously published prior-month estimate and
  58 that legitimately left that estimate unchanged;
- two official `NA` schedule rows in October and November 2025, followed by
  two December 2025 releases; and
- one preserved source discrepancy: archive directory `20000215` contains a
  release whose own header says February 16, 2000.

The exact 117,098-byte release-date HTML used to derive that denominator is
retained in `us_g17_release_dates_v1.html.b64`. Its content hash, the
parsed historical/future/NA dates, every release URI, byte count, raw hash,
reference period, lexical and numeric triplet values, release time, normalized
hash, and all nested identities contribute to the manifest identity.

Historical parser behavior is evidence-driven rather than inferred from the
current format. It recognizes the four- and six-month summary layouts,
case changes in the summary heading, revision markers in month labels,
UTF-8 and Windows-1252 source text, equal prior/revised estimates, and the
official 2026 `AM`/`PM` parenthetical header defects. A real `EST` or `EDT`
label that conflicts with the release date still fails closed.

The refresh command requires an explicit observation boundary. Supplying a raw
directory retains the full content-addressed corpus outside the wheel:

```console
uv run python scripts/refresh_us_g17_archive.py \
  --as-of 2026-09-14 \
  --raw-directory /absolute/operator/path/g17-raw
```

The live index contains volatile CDN worker and challenge tokens. When its
published, future, and unavailable inventories are unchanged, refresh keeps
the already validated packaged HTML while retaining the newly downloaded raw
page externally. This makes repeated generation stable without treating
presentation-layer churn as a new release schedule.

`replay_federal_reserve_g17_archive()` accepts those retained snapshots and
recomputes every manifest entry. Any missing, duplicate, changed, or newly
unparseable occurrence rejects the replay.
`federal_reserve_g17_coverage_from_manifest()` derives a complete 319-event
coverage slice directly from the verified manifest; route declarations or
current revised databases cannot influence the counts.

## Complete CPI archive qualification

`histdatacom.market_context.us_cpi_archive` qualifies the CPI-U all-items,
seasonally adjusted headline from the official BLS archive. The dedicated
`us.bls.cpi` supplemental source leaves `us.bls.public-data` as the registry's
single primary inflation source while giving occurrence-specific release
documents their own formats, parser route, exact-minute precision, and
empirical verification status.

The packaged `us_cpi_archive_v1.json` manifest is observed as of September 14,
2026 and records:

- 318 published reference periods from January 2000 through July 2026;
- 319 distinct source artifacts and 214,928,540 bytes, including the December
  1999 release needed to recover January 2000's previous-as-known value;
- 96 plain-text releases, 221 HTML releases, and one official PDF;
- 318 distinct normalized triplet hashes and 15 observed prior-period
  revisions; and
- one explicitly noncomparable revised-previous value in December 2025.

The exact 128,354-byte BLS release-index HTML is retained in
`us_cpi_release_index_v1.html.b64`. It supplies the reference-period
denominator, publication links, and the explicit statement that October 2025
was not published because of the federal appropriations lapse. The next
release reports November as a September-to-November two-month change because
October data were not collected. December returns to a monthly change, but
its release cannot restate a comparable November two-month measure; replay
therefore retains the November value as known and marks the comparison
unavailable instead of inventing a revision.

BLS's indexed May 2016 HTML artifact currently returns a zero-byte HTTP 200
response. The index contract preserves that indexed URI and records the
bounded substitution of BLS's official `cpi_06162016.pdf`. The PDF parser uses
Table 1's all-items seasonally adjusted columns and fails if extractable text,
the release header, title, or table shape changes.

Historical parsing is driven by the observed source eras. It handles
Windows-1252 and UTF-8 text, plain-text Table A rows with three-month annualized
columns and `r` revision markers, preformatted HTML, recent semantic
`table#cpi_pressa` rows, and source headers labeled EST, EDT, or ET. A concrete
EST/EDT label that conflicts with the publication date fails closed. Every
normalized triplet binds both the current artifact and its immediately prior
published predecessor; a current revised BLS series cannot satisfy replay.

The refresh command requires an explicit observation boundary. Supplying a
raw directory retains the full content-addressed corpus outside the wheel:

```console
uv run python scripts/refresh_us_cpi_archive.py \
  --as-of 2026-09-14 \
  --raw-directory /absolute/operator/path/cpi-raw
```

`replay_bls_cpi_archive()` recomputes every current/predecessor pair and rejects
missing, duplicate, changed, or newly unparseable bytes.
`bls_cpi_coverage_from_manifest()` derives the complete 318-event inflation
slice directly from that evidence, with unsupported event forecasts kept as an
explicit nonblocking gap.

## Quantified closure audit

`UnitedStatesProgramCoverageV1` records one exact 2000-present denominator and
the corresponding schedule, initial-actual, previous-as-known, revision,
release-time, forecast, and distinct artifact counts. Contemporaneous-release
strategies require at least one distinct artifact hash per expected occurrence;
an official vintage-table strategy may qualify multiple occurrences from one
retained table. Counts cannot exceed the expected denominator.

`audit_united_states_backfill()` passes only when:

1. every required program has exactly one coverage slice;
2. all 12 economic families remain represented;
3. all slices share the same window end and begin on `2000-01-01`;
4. every expected schedule and initial actual is present;
5. required previous-as-known values and revision history are present;
6. programs with exact recurring times qualify every occurrence at minute or
   better precision;
7. the archive strategy has enough distinct raw artifact hashes; and
8. no blocking schedule, artifact, initial, previous, revision, or time gap
   remains.

Programs that did not yet exist in 2000 use an explicit
`program-not-yet-published` gap. A missing official event-level forecast uses
`no-event-forecast`; both are nonblocking only because neither invents a
historical value. FOMC projections, SPF, and MBOS future-diffusion results keep
their native horizons and scopes. They are never projected into proprietary
monthly event consensus.

Coverage objects and audits are content addressed and round-trip through
canonical JSON. A passing audit is the closure receipt; the program inventory
alone is not.

## Primary evidence routes

- [BLS archived releases](https://www.bls.gov/bls/news-release/) retain
  occurrence-specific CPI, PPI, employment, JOLTS, and productivity documents.
- [BEA release archive](https://www.bea.gov/news/archive) and its GDP vintage
  tables retain superseded estimates and publication stages.
- [Census economic-indicator schedule](https://www.census.gov/economic-indicators/calendar-listview.html)
  retains release/reference identifiers and scheduled times; program archives
  retain the value-bearing releases.
- [Federal Reserve G.17 archive](https://www.federalreserve.gov/releases/g17/)
  exposes releases and release dates back before 2000.
- [FOMC historical materials](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm)
  distinguish statements, minutes, projections, and other meeting materials.
- [DOL/ETA claims](https://oui.doleta.gov/unemploy/claims.asp) is the legal
  administrative source for weekly unemployment-insurance claims.
- [Philadelphia Fed MBOS archives](https://www.philadelphiafed.org/surveys-and-data/mbos-archives)
  retain monthly release PDFs; the revised download is not substituted for
  those vintages.

The package preserves the real G.17 and CPI indexes, one representative G.17
raw release, and both complete raw/normalized manifests. The remaining
production raw corpora stay external, content addressed, and subject to the
coverage audit so wheel size does not grow with thousands of federal release
files.
