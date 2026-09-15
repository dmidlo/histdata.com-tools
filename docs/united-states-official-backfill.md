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

The source registry includes separate supplemental entries for DOL/ETA, the
Philadelphia Fed, CPI, PPI, the Employment Situation, JOLTS, and Productivity
and Costs. This prevents broad family-level entries from being incorrectly
named as the occurrence-specific source for weekly claims, regional Fed
surveys, or BLS release documents. The Federal Reserve G.17 entry also
declares plain text as a real archive format.
The reusable HTML-release adapter preserves such text as one bounded UTF-8
record, including the exact line count and raw-snapshot lineage.

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

## Complete PPI archive qualification

`histdatacom.market_context.us_ppi_archive` qualifies the seasonally adjusted
headline change from every official BLS PPI publication in the requested
window. The dedicated `us.bls.ppi` supplemental source distinguishes those
occurrence-specific documents from the registry's family-level public-data
entry and records their exact formats, parser route, minute precision, and
empirical verification status.

The packaged `us_ppi_archive_v1.json` manifest is observed as of September 14,
2026 and records:

- 319 published reference periods from January 2000 through August 2026;
- 320 distinct source artifacts and 237,890,117 bytes, including the December
  1999 release needed for January 2000's previous-as-known value;
- 96 plain-text and 223 HTML current releases;
- 319 distinct normalized triplet hashes and 59 observed prior-period
  revisions; and
- one explicitly noncomparable revised-previous value at the January 2014
  transition from finished-goods to final-demand PPI.

The exact 129,766-byte BLS release-index HTML is retained in
`us_ppi_release_index_v1.html.b64`. It fixes the published-period denominator,
publication links, and the explicit October 2025 federal-appropriations-lapse
gap. The November 2025 release reports a normal monthly change using a
subsequently calculated October index, while the prior published release and
previous-as-known value still refer to September. Replay preserves that
distinction.

Historical parsing follows the observed source eras: Windows-1252 and UTF-8
text releases, four preformatted-text HTML releases from 2008, and 219
semantic Table A HTML releases. It accepts bounded prefix and suffix `r`
revision markers and the official January 2017 header's extra comma. Concrete
EST and EDT labels must still agree with the publication date. Before January
2014 the headline is finished-goods PPI; from that release onward it is final
demand, so replay does not mislabel the transition as a revision of a
comparable measure. BLS's monthly interim-revision policy beginning with the
November 2021 data is retained as an explicit historical limitation.

The refresh command can fetch live official bytes or replay an operator's
retained basename-addressed corpus. Either path can retain the full corpus
content-addressed outside the wheel:

```console
uv run python scripts/refresh_us_ppi_archive.py \
  --as-of 2026-09-14 \
  --raw-directory /absolute/operator/path/ppi-raw

uv run python scripts/refresh_us_ppi_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/ppi-source \
  --raw-directory /absolute/operator/path/ppi-raw
```

`replay_bls_ppi_archive()` recomputes every current/predecessor pair and
rejects missing, duplicate, changed, or newly unparseable bytes.
`bls_ppi_coverage_from_manifest()` derives the complete 319-event inflation
slice directly from the verified manifest, while the absence of an official
event-level forecast remains an explicit nonblocking gap.

## Complete Employment Situation archive qualification

`histdatacom.market_context.us_employment_situation_archive` qualifies three
separate headline measures from every official BLS Employment Situation
publication: the CPS all-workers unemployment rate, the CES total-nonfarm
monthly payroll change, and CES total-private average hourly earnings. The
dedicated `us.bls.employment-situation` supplemental source gives the
occurrence-specific release documents their own formats, parser route,
exact-minute precision, and empirical verification status without displacing
the family-level BLS public-data source.

The packaged `us_employment_situation_archive_v1.json` manifest is observed as
of September 14, 2026 and records:

- 319 published reference periods from January 2000 through August 2026;
- 320 distinct source artifacts and 183,693,130 bytes, including the December
  1999 publication needed for January 2000's previous-as-known values;
- 96 fixed-width text, 24 preformatted HTML, and 199 semantic HTML current
  releases;
- 957 distinct normalized measure triplets;
- prior-period revisions in 6 CPS unemployment, 314 CES payroll, and 239 CES
  earnings occurrences, affecting 316 publication packages overall; and
- one explicitly noncomparable earnings comparison at the January 2010
  transition from production/nonsupervisory workers to all employees.

The exact 132,524-byte BLS archive-index HTML is retained in
`us_employment_situation_release_index_v1.html.b64`. It fixes the denominator,
selected publication links, and the explicit October 2025 federal-
appropriations-lapse gap. The November publication keeps September as the
previous published occurrence while using the subsequently calculated October
values where the current measure requires them.

Historical parsing keeps CPS and CES evidence separate. Modern summary-table
row identities select the unemployment rate, published payroll change, and
all-employee earnings level. The earlier fixed-width table supplies the same
headlines; its revised prior payroll change is derived only from the two
occurrence-specific adjacent level columns in that release, never from a
latest-state database. The December 7, 2012 artifact's erroneous `EDT` label
is retained as source evidence while timestamp normalization correctly uses
`America/New_York`. The December 1999 predecessor's URL is dated January 19,
2000, but its release header establishes the actual January 7 publication.

The refresh command can fetch live official bytes or replay a retained
basename-addressed corpus:

```console
uv run python scripts/refresh_us_employment_situation_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/empsit-source \
  --raw-directory /absolute/operator/path/empsit-raw
```

`replay_bls_employment_situation_archive()` recomputes all three triplets from
every current/predecessor pair and rejects missing, duplicated, changed, or
newly unparseable evidence. `bls_employment_situation_coverage_from_manifest()`
derives the complete 319-event labour slice while leaving absent official
event-level consensus as an explicit nonblocking gap.

## Complete JOLTS archive qualification

`histdatacom.market_context.us_jolts_archive` qualifies the total-nonfarm,
seasonally adjusted levels for job openings, hires, and total separations from
the official Job Openings and Labor Turnover Survey archive. The dedicated
`us.bls.jolts` source declares the occurrence-specific text and HTML formats,
archive enumeration, exact 10:00 Eastern release time, revision behavior, and
empirically verified parser route.

The packaged `us_jolts_archive_v1.json` manifest is observed as of September
14, 2026 and records:

- 268 qualified reference periods from March 2004 through July 2026;
- 269 distinct source artifacts and 99,093,148 bytes, including February 2004
  predecessor evidence;
- 45 fixed-width text, 77 preformatted HTML, and 146 semantic HTML Table A
  current releases;
- 804 distinct normalized measure triplets and 267 publication packages with
  at least one comparable prior-period revision;
- 266 job-openings, 267 hires, and 266 total-separations revision occurrences;
  and
- 22 annual benchmark/revision releases plus three explicitly noncomparable
  October 2025 measures.

The retained 89,112-byte BLS index fixes all 269 selected artifact URIs and the
explicit unpublished September 2025 period. It also resolves the index's 2012
heading typo: `jolts_02122013.htm` and the document header establish December
2012, not December 2013. The October publication contains September estimates,
but those are not comparable to August's last published occurrence; replay
therefore retains August as previous-as-known without inventing a September
release.

Historical parsing preserves 46 text artifacts including the predecessor, 77
preformatted HTML editions, and 146 semantic Table A editions. Annual
benchmark releases remain preformatted after semantic tables first appear.
Concrete EST and EDT labels must agree with the publication date, while the
later source-neutral `ET` label is retained lexically and normalized through
`America/New_York`.

The refresh command can fetch live official bytes or replay a retained
basename-addressed corpus:

```console
uv run python scripts/refresh_us_jolts_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/jolts-source \
  --raw-directory /absolute/operator/path/jolts-raw
```

`replay_bls_jolts_archive()` recomputes all three measures from every
current/predecessor pair and rejects missing, duplicated, changed, or newly
unparseable evidence. `bls_jolts_coverage_from_manifest()` derives the complete
268-event labour slice while retaining pre-program years and absent official
event-level consensus as explicit nonblocking gaps.

## Complete Productivity and Costs archive qualification

`histdatacom.market_context.us_productivity_costs_archive` qualifies two
nonfarm-business headline measures from every indexed BLS Productivity and
Costs publication in the requested window: labor productivity and unit labor
costs, both seasonally adjusted quarter-over-quarter percent changes at annual
rates. The dedicated `us.bls.productivity-costs` source records the stage-
specific archive, exact 08:30 Eastern release time, formats, revision behavior,
and empirical parser evidence separately from the broad BLS public-data API.

The packaged `us_productivity_costs_archive_v1.json` manifest is observed as
of September 14, 2026 and records:

- 214 stage-specific releases: 107 preliminary and 107 revised publications
  from fourth-quarter 1999 through second-quarter 2026;
- 215 distinct artifacts and 21,086,013 exact source bytes, including the
  revised third-quarter 1999 predecessor;
- 64 text and 150 preformatted HTML current artifacts;
- 155 sector-row and 59 measure-row Table A layouts;
- 426 numeric comparison-table revisions across the two measures, with 191
  nonzero productivity revisions and 202 nonzero unit-labor-cost revisions;
  and
- 76 releases containing an annual-benchmark, comprehensive, or historical-
  revision notice.

The retained 117,331-byte archive index fixes all stage labels, reference
quarters, release dates, formats, and artifact URIs. Every quarter has a
preliminary/revised pair. A preliminary release carries both the new quarter's
first estimate and a comparison-table revision of the preceding quarter; a
revised release carries the current quarter's second estimate. Replay retains
those identities instead of collapsing two publications into one quarterly
row.

Two official discontinuities fail closed rather than receiving synthetic
values. The February 6, 2019 preliminary fourth-quarter 2018 publication
reports both selected headline measures as `N.A.` because underlying BEA data
were unavailable during the federal shutdown. Its comparison table still
revises third-quarter 2018, while the following revised publication has no
numeric preliminary value to compare. Separately, BLS reissued the March 7,
2024 artifact with notice that its estimates would not be corrected in that
document. The May 2 comparison table carries corrected prior-quarter
productivity (`3.3`) that differs from the prior artifact (`3.2`); both values
and the noncomparable predecessor lineage remain explicit.

The refresh command can fetch live official bytes or replay a retained
basename-addressed corpus:

```console
uv run python scripts/refresh_us_productivity_costs_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/prod-source \
  --raw-directory /absolute/operator/path/prod-raw
```

`replay_bls_productivity_costs_archive()` recomputes all 428 normalized
measure records from the full 215-artifact chain. It rejects missing, changed,
duplicated, or newly unparseable evidence and any unexplained difference
between an artifact and the next comparison table.
`bls_productivity_costs_coverage_from_manifest()` derives the complete
qualified 214-stage labour slice while counting the one official unavailable
stage explicitly.

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
4. every expected schedule and either its initial actual or a counted official
   unavailable value is present;
5. required previous-as-known values, or the same counted official
   unavailability, and revision history are present;
6. programs with exact recurring times qualify every occurrence at minute or
   better precision;
7. the archive strategy has enough distinct raw artifact hashes; and
8. no blocking schedule, artifact, initial, previous, revision, or time gap
   remains.

Programs that did not yet exist in 2000 use an explicit
`program-not-yet-published` gap. A missing official event-level forecast uses
`no-event-forecast`. A source document that explicitly publishes a required
measure as unavailable uses `official-measure-unavailable` and a bounded count;
all three are nonblocking only because none invents a historical value. FOMC
projections, SPF, and MBOS future-diffusion results keep their native horizons
and scopes. They are never projected into proprietary monthly event consensus.

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

The package preserves the real G.17, CPI, PPI, Employment Situation, JOLTS, and
Productivity and Costs indexes, one representative G.17 raw release, and all
six complete raw/normalized manifests. The remaining production raw corpora
stay external, content addressed, and subject to the coverage audit so wheel
size does not grow with thousands of federal release files.
