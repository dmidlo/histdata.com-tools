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
Philadelphia Fed's MBOS and SPF programs, CPI, PPI, the Employment Situation,
JOLTS, Productivity and Costs, GDP, Personal Income and Outlays, Advance
Monthly Retail Sales, Advance Durable Goods, New Residential Construction, and
New Residential Sales. FOMC meeting publications and H.6 Money Stock Measures
also use separate registry entries: qualifying the meeting archive therefore
cannot imply that the distinct statistical release has been verified. This
prevents broad family-level entries from being incorrectly named as the
occurrence-specific source for weekly claims, regional Fed surveys, or
statistical release documents. The Federal Reserve G.17 entry also declares
plain text as a real archive format.
The Advance Durable Goods entry binds the Census M3 historical archive and
release schedule as separate evidence roles and accepts both HTML and PDF.
The New Residential Construction entry separately binds the Census/HUD
housing-release archive, including its legacy text and unified PDF eras.
The New Residential Sales entry binds the successor Census release archive
explicitly because the older historical-data route is retired.
The joint Census/BEA International Trade entry binds the FT-900 monthly archive
and keeps its retired route, incorrect links, and annual-revision behavior
explicit.
The Manufacturing and Trade Inventories and Sales entry binds its successor
historical page, official directory, current briefing, table formats, and
release-document evidence separately from the broad Census EITS API entry.
The SPF entry binds its release archive, true-deadline/release-date ledger,
median workbooks, documentation, and date-only publication precision without
turning quarterly professional forecasts into monthly event consensus.
The MBOS entry binds its complete legacy ZIP, live archive receipt,
occurrence-specific PDFs and pages, reviewed OCR, changing publication-time
eras, and contemporaneous current/prior/future diffusion indexes without
substituting the revised history download.
The reusable HTML-release adapter preserves such text as one bounded UTF-8
record, including the exact line count and raw-snapshot lineage.

## Complete FOMC archive qualification

`histdatacom.market_context.us_fomc_archive` qualifies the three distinct FOMC
programs without flattening them into one calendar row. The package retains
the exact 21 annual historical meeting pages for 2000--2020 plus the current
calendar page. Those 22 pages bind documents to official meeting containers;
a URL resemblance alone can never classify a statement, minutes publication,
or SEP vintage.

The packaged `us_fomc_archive_v1.json` manifest is observed through September
15, 2026, the last fully observable day before the September meeting. It
records:

- 249 meeting, conference-call, and notation-vote containers;
- 225 policy statements from February 2000 through July 2026;
- 213 unique minutes publications, including documents shared by separately
  indexed conference calls;
- 75 contemporaneous SEP vintages from October 2007 through June 2026; and
- 521 unique official artifacts containing 49,272,968 bytes.

Every decision preserves the native federal-funds setting shape and its
previous-as-known lineage. The first statement derives the preceding 5.5
percent target from the source-authored 25-basis-point increase to 5.75
percent. The December 2008 change from a 1 percent scalar target to a
0--0.25 percent range remains an explicit shape transition. The archive yields
148 holds, 40 tightenings, 34 easings, one guidance-only statement, and two
operational liquidity statements that state no target change and therefore
carry the preceding known setting. Only those three pinned source URIs may use
that carry rule; an unrecognized parsing failure stops the build. The
August 2025 longer-run strategy notation vote is not misclassified as a rate
decision. Ninety statements supply an exact publication clock; the others
remain honestly bounded. Thirteen decisions are explicitly unscheduled.

SEP publication regimes remain occurrence-specific. Fourteen October
2007--January 2011 vintages use the public minutes addendum released after the
meeting. Thirty-nine April 2011--December 2020 vintages use the contemporaneous
advance projection-table PDFs, and 22 March 2021--June 2026 vintages use the
current HTML projection tables. These central-bank projection vintages remain
distinct from a proprietary event consensus. Likewise, minutes are documentary
events and are not given fabricated numeric triplets.

Only the compact manifest and the exact 2,172,157-byte index-page corpus ship
in the package. The larger statement, minutes, and SEP corpus stays in a
caller-retained content-addressed directory. It can be fetched once or replayed
without network access:

```console
uv run python scripts/refresh_us_fomc_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/fomc-source \
  --fetch-missing
```

`replay_federal_reserve_fomc_archive()` reparses every retained index and
document and compares the rebuilt manifest exactly. Missing, changed,
duplicated, newly ambiguous, incorrectly classified, or unused evidence fails
closed. `federal_reserve_fomc_coverages_from_manifest()` derives three complete
program slices while leaving the unavailable event-level decision consensus
explicit.

## Complete H.6 money-stock archive qualification

`histdatacom.market_context.us_h6_archive` qualifies the distinct Federal
Reserve H.6 Money Stock Measures program from the official release landing
page, JSON release-date ledger, occurrence pages, one occurrence-specific text
fallback, and matching timing PDFs. The packaged manifest is observed through
September 15, 2026 and contains 1,169 in-scope publications from January 6,
2000 through August 25, 2026, plus the December 30, 1999 predecessor needed to
recover the first previous-as-known value.

The archive preserves the native publication redesign instead of projecting
all observations onto one frequency. It contains 1,102 weekly publications
through February 11, 2021 and 67 monthly publications beginning February 23,
2021. Weekly releases use the last two seasonally adjusted M2 week-average
rows; monthly releases use the last two seasonally adjusted monthly M2 rows.
For each occurrence, the previous-as-known value comes from the preceding
release and is kept distinct from the current release's revised previous row.
The first monthly occurrence deliberately compares December 2020 with the
preceding weekly release's monthly table. The Federal Reserve's associated M1
definition change stays explicit without falsely declaring an M2 break.

Five source-layout eras are replayed: 324 pre-discontinuation M3 weekly
publications, 396 preformatted M2 weekly publications, 178 legacy-table weekly
publications, 204 modern-table weekly publications, and 67 modern-table monthly
publications. The manifest records 973 occurrences where the previous value
changed. Of 1,169 occurrences, 1,091 have an occurrence-authored minute and 78
retain only the official scheduled convention. A scheduled convention never
creates an artificial publication instant.

The qualification also makes the surviving archive's defects reviewable:

- four JSON ledger dates differ from dates authored by both the occurrence
  page and PDF; every correction is pinned to that one occurrence and retains
  both the indexed date and authored date;
- the June 13, 2002 directory's default and screen-reader HTML are stale May
  16 copies, so its official text release is retained and cross-checked against
  the correctly authored June 13 PDF; and
- 19 weekly `h6.pdf` routes are special notices without an occurrence clock or
  are stale, while the 22 apparent monthly PDF routes through November 2022
  resolve to the final weekly notice. None is accepted as timing evidence.

The replay inventory contains 2,247 unique official artifacts and 174,852,559
bytes: two index receipts, 1,170 value-bearing release artifacts, and 1,075
matching timing PDFs. Only the compact 2.4 MiB manifest and the exact packaged
index receipts ship in the wheel; the larger corpus remains in a
caller-retained directory:

```console
uv run python scripts/refresh_us_h6_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/h6-source \
  --fetch-missing
```

`replay_federal_reserve_h6_archive()` reparses and hash-compares the complete
retained corpus. Missing, extra, duplicated, changed, misdated, stale, or
unparseable evidence fails closed. `federal_reserve_h6_coverage_from_manifest()`
derives complete schedule, initial-actual, previous-as-known, revision, and
timing coverage while leaving unavailable event-level consensus explicit.
Together with the previously qualified archives, 20 of the 22 U.S. programs
are now complete; initial claims and the Monthly Treasury Statement remain
open.

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

## Complete Gross Domestic Product archive qualification

`histdatacom.market_context.us_gdp_archive` qualifies the headline real-GDP
annualized quarter-over-quarter percent change from every BEA national GDP
publication in the requested window. The dedicated `us.bea.gdp` source keeps
the occurrence-specific HTML/PDF archive, exact 08:30 Eastern evidence, and
release-stage behavior separate from the current-series BEA API.

The packaged `us_gdp_archive_v1.json` manifest is observed as of September 14,
2026 and records:

- 318 stage-specific releases from the fourth-quarter 1999 advance estimate
  through the second estimate for second-quarter 2026;
- 107 advance, 105 second/preliminary, and 106 third/final stages;
- 320 distinct release/support artifacts and 27,626,013 exact source bytes,
  including the final third-quarter 1999 predecessor and one release-time
  support PDF;
- 213 fixed-width preformatted, 86 narrative, and 19 comparison-table HTML
  layouts;
- 192 nonzero revision lineages: 17 at advance, 92 at second, and 83 at final
  stages; and
- 15 title-declared annual/comprehensive/historical updates plus one corrected
  release.

For a second/preliminary or third/final publication, the preceding stage for
the same reference quarter is `previous_as_known` and the new headline is the
revised value. For an advance publication, the prior quarter's last artifact
is `previous_as_known`, while the prior-quarter value printed in the advance
release is the revised value. This exposes annual or comprehensive updates
that occur alongside 17 advance estimates instead of silently replacing the
older quarter.

The source has several bounded irregularities. Fourth-quarter 2018 and
third-quarter 2025 each omit a standard second-stage publication after a
federal shutdown; their initial and following final/updated releases remain
the actual source inventory. The August 26, 2026 second estimate is retained
as an explicit direct-release supplement because the 18-page product-filtered
archive had not yet indexed it. The March 29, 2001 imported HTML omits its
embargo line, so its exact time comes only from the hash-bound official PDF.

Five source headers print a seasonally incorrect EST or EDT abbreviation, and
the archive lists two advance releases one day before their embargo-header
dates. Both lexical and archive metadata remain explicit; the artifact header
and `America/New_York` control normalized occurrence time. The corrected July
2000 artifact remains a correction rather than being treated as the original
uncorrected release.

The refresh command can fetch live official bytes or replay a retained
basename-addressed corpus:

```console
uv run python scripts/refresh_us_gdp_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/gdp-source \
  --raw-directory /absolute/operator/path/gdp-raw
```

`replay_bea_gdp_archive()` reparses all 318 occurrences from the complete
320-artifact release chain and all 18 retained archive pages. It rejects
missing, changed, duplicated, newly unparseable, or differently normalized
evidence. `bea_gdp_coverage_from_manifest()` derives the complete qualified
318-occurrence national-accounts slice.

## Complete Personal Income and Outlays archive qualification

`histdatacom.market_context.us_personal_income_outlays_archive` qualifies four
separate monthly BEA measures: current-dollar personal income, current-dollar
personal consumption expenditures, the headline PCE price index, and the PCE
price index excluding food and energy. The dedicated
`us.bea.personal-income-outlays` source is separate from the latest-state BEA
API and declares the HTML, PDF, text, and XLSX formats present in the retained
archive.

The packaged `us_pio_archive_v1.json` manifest is observed as of September 14,
2026. It binds 319 publications and 1,280 measure occurrences to 334 distinct
raw artifacts containing 21,699,009 source bytes. Income and spending have 320
gap-free initial monthly values from December 1999 through July 2026. Headline
and core prices have 314 each from June 2000 through July 2026; the first 42
price occurrences are derived from contemporaneous index levels, and the
source-published monthly rate becomes authoritative in February 2002.

The archive retains its irregular publication history. Three 2019 shutdown
publications split and then caught up income and outlays. The 2025 shutdown
produced ten workbook-backed releases, a revision-only July-through-September
data update, and a combined October-November publication. Fourteen releases
therefore bind official PDF, text, or XLSX support artifacts. The parser also
preserves nine title-declared historical updates, four incorrect source DST
abbreviations, the archive/header date conflict for the December 2018 release,
and the date-only February 2000 artifact without inventing an exact time.

The refresh command can fetch official bytes or replay the retained corpus:

```console
uv run python scripts/refresh_us_pio_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/pio-source \
  --raw-directory /absolute/operator/path/pio-raw
```

`replay_bea_pio_archive()` recomputes the inventory, all four measure histories,
and 626 comparable nonzero revision lineages. It rejects missing, changed,
duplicated, newly unparseable, or differently normalized evidence.
`bea_pio_coverage_from_manifest()` derives the complete 319-publication
national-accounts slice.

## Complete Advance Monthly Retail Sales archive qualification

`histdatacom.market_context.us_retail_sales_archive` qualifies four distinct
Census measures for every monthly reference period from December 1999 through
July 2026: retail and food services total, total excluding autos, total
excluding gasoline, and the retail control group. The dedicated
`us.census.retail-sales` source remains separate from the broader EITS source
and binds both the monthly MARTS archive and the annual revision archive.

The packaged `us_retail_sales_archive_v1.json` manifest is observed as of
September 14, 2026. It records 320 publications and 1,280 measure occurrences
from exactly 512 raw artifacts containing 174,591,370 source bytes: two archive
indexes, the November 1999 predecessor, 320 monthly PDFs, 163 spreadsheet
companions, and 26 annual benchmark artifacts. All 320 releases retain their
exact 08:30 Eastern publication minute. Thirty-seven target PDFs and the
predecessor are image-only, so reviewed page-aware Tesseract text is bound to
each original PDF hash; 120 later publications use native PDF tables, 108 use
XLS, and 55 use XLSX.

Previous-as-known lineage uses the last official state available before each
publication. Normally that is the preceding monthly release. For 24 releases
from 2002 through 2025, however, an intervening annual revision publishes a
new adjusted terminal month; those benchmark rates become the next release's
prior state. The 2000 and 2001 annual reports do not overlap the required
month, so they remain retained evidence without becoming an override. The 2023
workbook's terminal header says `Mar. 2022(a)` on its 2023 sheet; the parser
accepts only that exact source-authored typo while preserving the raw bytes.

The corpus also preserves the May 2001 SIC-to-NAICS boundary as noncomparable,
six broken July–December 2020 spreadsheet links as bounded `adv`-to-`rs`
corrections, two malformed Strict OOXML external-link records normalized only
in memory, and nine shutdown-delayed publications. Ex-gasoline levels before
May 2018 and all control-group levels are derived strictly from adjusted
components in the same occurrence. The 163 support files provide 850 exact
checks between reported Table 2 percentages and rates recomputed from Table 1
levels.

The refresh command can fetch official bytes or replay the two retained
corpora:

```console
uv run python scripts/refresh_us_retail_sales_archive.py \
  --as-of 2026-09-14 \
  --source-directory /absolute/operator/path/retail-monthly \
  --benchmark-source-directory /absolute/operator/path/retail-annual \
  --ocr-page-directory /absolute/operator/path/retail-reviewed-ocr \
  --raw-directory /absolute/operator/path/retail-raw
```

`replay_census_retail_archive()` recomputes both indexes, all annual terminal
states, all monthly publications, and every measure lineage. It rejects a
missing, changed, duplicated, newly unparseable, differently normalized, or
unused artifact. `census_retail_coverage_from_manifest()` derives the complete
320-publication retail-consumption slice without manufacturing historical
event-level consensus.

## Complete Advance Durable Goods archive qualification

`histdatacom.market_context.us_durable_goods_archive` qualifies Census advance
durable-goods new orders and nondefense capital-goods new orders excluding
aircraft for every scheduled monthly reference period from January 2000
through June 2026. The dedicated `us.census.durable-goods` source binds the M3
historical release index and Census release schedule without treating the
current revised time series as an original release.

The packaged `us_durable_goods_archive_v1.json` manifest is observed as of
September 15, 2026. It records 318 scheduled publications, 621 value-bearing
measure occurrences, and 580 comparable revisions from 320 exact raw
artifacts containing 95,545,357 source bytes: the historical index, the
December 1999 predecessor, and 318 target PDFs. Every artifact has a distinct
SHA-256 digest. Thirty-six target releases and the predecessor are image-only,
so reviewed page-aware Tesseract text is separately content-addressed and
bound to the original PDFs; the remaining releases use native PDF tables.

The manifest preserves source limitations rather than filling them by
inference. The official November 2009 report says its results are unavailable
because of a processing error, so the occurrence has no fabricated measures.
The core capital-goods row is absent from the official Table 1 for 13 periods
from April 2001 through April 2002 and resumes with explicitly noncomparable
lineage. December 2009 also starts from the intervening corrected historical
state. Ninety-seven publications carry explicit historical-revision notices,
nine target releases were shutdown-delayed, nine target PDFs contain source
timezone labels inconsistent with their release dates, and three releases
were published at 10:00 rather than the normal 08:30 Eastern time. The broken
official July 2005 index link is corrected only to the surviving Census-hosted
legacy PDF. Across the two measures, 1,242 source-reported rates are checked
against same-release levels.

The refresh command can fetch missing official bytes or replay a retained
corpus and reviewed OCR pages:

```console
uv run python scripts/refresh_us_durable_goods_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/durable-source \
  --ocr-page-directory /absolute/operator/path/durable-reviewed-ocr \
  --fetch-missing
```

`replay_census_durable_archive()` reparses the retained index, OCR corpus, and
all publications. It rejects missing, changed, duplicated, newly unparseable,
differently normalized, or unused evidence.
`census_durable_coverage_from_manifest()` derives the complete 318-occurrence
manufacturing slice, counting the official unavailable report explicitly and
never manufacturing historical event-level consensus.

## Complete International Trade archive qualification

`histdatacom.market_context.us_international_trade_archive` qualifies the
joint Census/BEA FT-900 U.S. International Trade in Goods and Services archive
from January 2000 through July 2026. The dedicated
`us.bea-census.international-trade` source binds the live monthly archive after
the retired archive route began returning 404; it does not substitute a
current revised series for contemporaneous release values.

The packaged `us_international_trade_archive_v1.json` manifest is observed as
of September 15, 2026. It records 319 monthly publications, 319
previous-as-known comparisons, and 296 changed prior estimates from 321
distinct raw artifacts containing 349,754,556 source bytes. The inventory is
the retained archive index, the December 1999 predecessor, and all 319 target
PDFs. Every publication time is recovered to the exact minute at 08:30
Eastern.

Standard PDF extraction qualifies 315 publications. Four difficult PDFs are
accepted only under reviewed SHA-256-bound rows: May and August 2000, April
2002, and July 2015. Six index links that point to a neighboring month or year
are corrected only to the live predictable Census-hosted PDF. The source's
`Ferbruary 2019` index label remains retained evidence.

Sixty-one legacy reports omit a zone token and therefore use the declared
producer-local `America/New_York` convention. All 258 explicit EST/EDT tokens
are preserved, including five source-authored abbreviations inconsistent with
the release date. Twenty-seven April reports carry concurrent annual
revisions, and eight reports retain source-authored federal-disruption
notices. The normalized headline is the positive deficit magnitude as
published; a consumer that models a signed trade balance must negate it
explicitly.

The refresh command can fetch missing official bytes or deterministically
replay a retained corpus:

```console
uv run python scripts/refresh_us_international_trade_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/international-trade-source \
  --fetch-missing
```

`replay_census_international_trade_archive()` reparses the exact archive index,
predecessor, and every monthly publication. It rejects missing, changed,
duplicated, newly unparseable, differently normalized, implausibly delayed, or
unused evidence. `census_international_trade_coverage_from_manifest()` derives
the complete 319-publication trade slice without manufacturing historical
event-level consensus.

## Complete New Residential Construction archive qualification

`histdatacom.market_context.us_housing_construction_archive` qualifies the
Census/HUD New Residential Construction archive for total building permits,
housing starts, and housing completions from January 2000 through July 2026.
The dedicated `us.census.new-residential-construction` source binds the release
archive and Census economic-indicator schedule without substituting the current
revised time series for contemporaneous releases.

The packaged `us_housing_construction_archive_v1.json` manifest is observed as
of September 15, 2026. It records 330 logical publications, 957 complete
period-measure observations, 943 comparable previous-as-known checks, and 916
changed prior estimates from 334 distinct raw artifacts containing 47,485,575
source bytes. The raw inventory comprises the retained archive index, two
December 1999 predecessor releases, and all 331 target artifacts. All 330
publication times are recovered to the exact minute.

The qualification preserves the archive's real historical shape. January 2000
through March 2001 has separate starts/permits and completions text releases,
producing 30 publications rather than collapsing two release times into one.
Later releases use PDF layout extraction, with a bounded standard-extraction
fallback for 31 difficult layouts. Three broken index links are corrected only
to verified Census-hosted targets. Two byte-distinct September/October 2013
index artifacts contain the same November 26 publication and therefore remain
two artifact aliases for one logical event.

Five hash-bound catch-up publications preserve multi-month tables created by
the 2013 and 2025-2026 federal disruptions. They recover the missed September
and October 2013 components and the September 2025, November 2025, and February
2026 observations at the actual later publication time. Fourteen comparisons
whose prior month first appeared in that same document are explicitly
noncomparable; they are not mislabeled as ex-ante previous-as-known values.
Twenty-five releases retain explicit historical-revision or methodology
notices, 15 were published at 10:00 Eastern, and no source timezone label is
inconsistent with its publication date.

The refresh command can fetch missing official bytes or deterministically
replay a retained corpus:

```console
uv run python scripts/refresh_us_housing_construction_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/housing-construction-source \
  --fetch-missing
```

`replay_census_housing_archive()` reparses the exact archive index and every
source publication. It rejects missing, changed, duplicated, newly unparseable,
differently normalized, or unused evidence.
`census_housing_coverage_from_manifest()` derives the complete 330-publication
housing slice without manufacturing historical event-level consensus.

## Complete New Residential Sales archive qualification

`histdatacom.market_context.us_new_home_sales_archive` qualifies the Census/HUD
New Residential Sales archive from January 2000 through July 2026. The
dedicated `us.census.new-residential-sales` source records the retired
historical-data route and binds its live Census release-archive successor; it
does not silently treat the route change as a gap or substitute revised series
data for contemporaneous releases.

The packaged `us_new_home_sales_archive_v1.json` manifest is observed as of
September 15, 2026. It records 315 logical publications, 319 reference-period
measures, 315 comparable previous-as-known checks, and 306 changed prior
estimates from 318 distinct raw artifacts containing 41,950,804 source bytes.
The inventory comprises the retained archive index, a December 1999
predecessor, and 316 target artifacts. All 315 publication times are recovered
to the exact minute at 10:00 Eastern.

Fifteen target publications use the legacy text format, 294 use layout-aware
PDF extraction, and two difficult PDFs use a bounded standard-extraction
fallback. The qualification corrects the archive's December 2018 link only to
the verified Census-hosted release, retains the February 2001 report's
mistyped prior-period year, and records source timezone-label mismatches in the
March 2004 and January 2019 reports.

Four hash-bound catch-up publications preserve multi-month tables created by
the 2013 and 2025-2026 federal disruptions. The byte-distinct September and
October 2013 artifacts remain two aliases for one December 4 publication with
two reference-period measures. Later catch-ups recover September 2025,
November 2025, and February 2026 at their actual publication times. Four prior
comparisons that first appeared in the same catch-up document are explicitly
noncomparable, and 14 releases retain historical-revision, sample, or
methodology notices.

The refresh command can fetch missing official bytes or deterministically
replay a retained corpus:

```console
uv run python scripts/refresh_us_new_home_sales_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/new-home-sales-source \
  --fetch-missing
```

`replay_census_new_home_sales_archive()` reparses the exact archive index and
every source publication. It rejects missing, changed, duplicated, newly
unparseable, differently normalized, or unused evidence.
`census_new_home_sales_coverage_from_manifest()` derives the complete
315-publication slice without manufacturing historical event-level consensus.

## Complete Manufacturing and Trade Inventories and Sales qualification

`histdatacom.market_context.us_mtis_archive` qualifies the Census Manufacturing
and Trade Inventories and Sales archive from January 2000 through June 2026.
The dedicated `us.census.mtis` source binds the live historical page after the
retired `/mtis/www/historical/index.html` route began returning 404. It keeps
total-business sales, total-business inventories, and the inventories-to-sales
ratio as three separate concepts instead of conflating their units or revision
chains.

The packaged `us_mtis_archive_v1.json` manifest is observed as of September 15,
2026. It records 318 monthly publications, 954 initial values, 954
previous-as-known comparisons, and 723 changed prior estimates from 635
distinct raw artifacts containing 91,178,773 source bytes. The inventory
comprises three current official pages, seven archived copies of original
Census pages used only as 2000 timing witnesses, the December 1999 predecessor
table, 318 target data tables, and 306 target release PDFs. Every publication
time is recovered to the exact minute.

The data-table eras are explicit: 157 releases use text, including the bounded
May 2014 fallback for an indexed spreadsheet that returns 404; 109 use legacy
XLS; and 52 use XLSX. The historical page omits the live July 2001 PDF, while
the official directory still enumerates it. The June 2026 stable historical
objects were live and the current Census briefing identified the same report
before the historical page listed it. Those three defects are closed only by
their exact predictable Census-hosted targets.

Three PDFs cannot supply their own timestamp: April 2001 and April 2003 are
image-only, and November 2009 omits its release-date header. Replay accepts
only the immediately preceding report's explicit next-release schedule for
those three occurrences. Seven archived Census-page captures establish the 12
publication times in 2000; the Internet Archive is retained as transport and
timing evidence, never reclassified as the legal producer. Nine publications
retain three-month lags caused by the 2018-2019 and 2025-2026 federal
disruptions.

The refresh command can fetch missing source bytes or deterministically replay
a retained corpus:

```console
uv run python scripts/refresh_us_mtis_archive.py \
  --as-of 2026-09-15 \
  --source-directory /absolute/operator/path/mtis-source \
  --fetch-missing
```

`replay_census_mtis_archive()` reparses the official enumeration evidence,
legacy timing witnesses, every table, and every PDF timestamp. It rejects
missing, changed, duplicated, newly unparseable, differently normalized, or
unused evidence. `census_mtis_coverage_from_manifest()` derives the complete
318-publication slice without manufacturing historical event-level consensus.

## Complete Manufacturing Business Outlook Survey qualification

`histdatacom.market_context.us_mbos_archive` qualifies every Philadelphia Fed
Manufacturing Business Outlook Survey publication from January 2000 through
August 2026. The official 1968--2007 ZIP supplies the December 1999
previous-as-known predecessor and all 2000--2007 reports; the live archive
supplies an unbroken January 2008--August 2026 sequence. MBOS remains a Third
District manufacturing diffusion survey and is never relabeled as national
output.

The packaged `us_mbos_archive_v1.json` manifest is observed as of September 16,
2026. It records 320 monthly publications, 320 initial current indexes, 320
previous-as-known comparisons, 320 native six-month-future indexes, and 54
changed prior values across 27 publications. The 321 occurrence PDFs contain
94,818,605 bytes. The full caller-retained corpus has 234 distinct fetched
artifacts containing 114,235,398 bytes: the archive receipt, legacy ZIP, 224
direct PDFs, and eight companion HTML reports. The package retains only the
compact manifest, exact archive receipt, and reviewed OCR.

December 1999 through December 2001 are image-only two-page reports. Their 25
OCR records are bound to exact PDF hashes, fixed 300-DPI rendering, Tesseract
version, and page-segmentation modes; the 24 target publications remain
distinguishable from the predecessor. Seven damaged OCR/text-layer rows use
hash-bound corrections checked against the report narrative and the diffusion
identity. The Summary of Returns uses eight columns through February 2003 and
ten columns thereafter, where the added columns preserve the prior diffusion
indexes printed in that same release.

Release precision is occurrence-specific. The first 65 target publications
state no release time and retain only the producer's third-Thursday convention.
Reports state noon ET from June 2005 through December 2007, 10:00 ET from
January 2008 through October 2015, and 08:30 ET thereafter. January 2010 omits
its release line and remains inferred-bounded even though its expected local
time is recorded. Consequently 254 occurrences have exact-minute evidence and
66 remain inferred-bounded; no synthetic instant is created.

Annual January seasonal revisions retain both the preceding initial index and
the revised previous index. The April 2021 pandemic-era off-cycle seasonal
revision is handled the same way, while January 2022's revision notice is kept
even though neither selected index changed. The future general-activity index
keeps its native six-month horizon and is not converted into event consensus.

The refresh command can fetch missing source bytes, produce OCR candidates for
review, or deterministically replay retained evidence:

```console
uv run python scripts/refresh_us_mbos_archive.py \
  --as-of 2026-09-16 \
  --source-directory /absolute/operator/path/mbos-source \
  --ocr-page-directory /absolute/operator/path/mbos-reviewed-ocr \
  --fetch-missing
```

`replay_philadelphia_fed_mbos_archive()` reparses the live archive and legacy
ZIP, verifies every member/direct PDF and report page, rereads every table and
reviewed OCR row, and compares the resulting manifest exactly. Missing,
duplicated, changed, newly ambiguous, differently normalized, or unused
evidence fails closed. `philadelphia_fed_mbos_coverage_from_manifest()` derives
complete monthly actual, previous, revision, schedule, timing, and native
outlook coverage.

## Complete Survey of Professional Forecasters qualification

`histdatacom.market_context.us_spf_archive` qualifies every Philadelphia Fed
Survey of Professional Forecasters publication from 2000 Q1 through 2026 Q3.
The dedicated `us.frb.philadelphia-spf` source is separate from MBOS because
the two programs have different archives, frequencies, evidence, and release
precision.

The packaged `us_spf_archive_v1.json` manifest is observed as of September 15,
2026. It records 107 quarterly publications and 428 forecast occurrences from
113 distinct official artifacts containing 93,449,761 bytes. The corpus binds
the archive page, median-forecast landing page, exact release-date ledger,
median-level and median-growth workbooks, methodology documentation, and 107
distinct release PDFs. The small release-date ledger is packaged byte-for-byte;
the workbooks and release PDFs remain in the external content-addressed corpus.

Each publication keeps four independent current-quarter medians: annualized
real-GDP growth, the unemployment rate, annualized headline-CPI inflation, and
the three-month Treasury-bill rate. The official documentation defines column
`2` as the current-quarter nowcast and column `1` as the prior-quarter
historical value. All 428 selected cells are numeric. Workbook coordinates and
source lexical precision are retained instead of rounding values to the one
decimal place commonly shown in release summaries.

The true deadline and news-release date remain separate. The ledger identifies
the 2013 Q4, 2019 Q1, 2025 Q4, and 2026 Q1 shutdown-delayed surveys and the
missing historical jump-off values in 2025 Q4. SPF publishes no stable release
time, so all 107 occurrences remain honestly date-only. Each quarterly survey
is a forecast vintage, not a revision stage; later panels do not rewrite prior
survey observations.

The bounded refresh command can also retain all raw artifacts outside the
wheel:

```console
uv run python scripts/refresh_us_spf_archive.py \
  --as-of 2026-09-15 \
  --raw-directory /absolute/operator/path/spf-raw
```

`replay_philadelphia_fed_spf_archive()` reparses the archive and ledger,
validates every PDF, rereads all selected workbook cells, and compares the
resulting manifest exactly. Missing, duplicated, changed, newly nonnumeric, or
unreadable evidence fails closed.
`philadelphia_fed_spf_coverage_from_manifest()` derives complete date-only
survey-vintage coverage while leaving event-level consensus unsupported.

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
- [BEA release archive](https://www.bea.gov/news/archive), GDP vintage tables,
  and Personal Income and Outlays tables retain superseded estimates and
  publication stages.
- [Census economic-indicator schedule](https://www.census.gov/economic-indicators/calendar-listview.html)
  retains release/reference identifiers and scheduled times; program archives
  retain the value-bearing releases.
- [Federal Reserve G.17 archive](https://www.federalreserve.gov/releases/g17/)
  exposes releases and release dates back before 2000.
- [Federal Reserve H.6 archive](https://www.federalreserve.gov/releases/h6/)
  retains the weekly and monthly Money Stock Measures occurrence history.
- [FOMC historical materials](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm)
  distinguish statements, minutes, projections, and other meeting materials.
- [DOL/ETA claims](https://oui.doleta.gov/unemploy/claims.asp) is the legal
  administrative source for weekly unemployment-insurance claims.
- [Philadelphia Fed MBOS archives](https://www.philadelphiafed.org/surveys-and-data/mbos-archives)
  retain monthly release PDFs; the revised download is not substituted for
  those vintages.
- [Philadelphia Fed SPF](https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/survey-of-professional-forecasters)
  retains quarterly release PDFs, release dates, methodology, and median
  forecast workbooks as a distinct professional-survey archive.

The package preserves the real G.17, CPI, PPI, Employment Situation, JOLTS,
Productivity and Costs, GDP, Personal Income and Outlays, and Advance Monthly
Retail Sales indexes, the Advance Durable Goods index and reviewed OCR corpus,
the International Trade, New Residential Construction, New Residential Sales,
and Manufacturing and Trade Inventories and Sales indexes, the MBOS archive
receipt and reviewed OCR corpus, the SPF release ledger, one representative
G.17 raw release, and all twenty complete raw/normalized manifests. The
remaining production raw corpora stay external, content addressed, and subject
to the coverage audit so wheel size does not grow with thousands of federal
release files.
