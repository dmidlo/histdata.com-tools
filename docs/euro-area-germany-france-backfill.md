# Euro area, Germany, and France official macro-calendar backfill

Issue #539 owns the first-party reconstruction of euro-area, German, and
French macroeconomic calendar history from 2000 onward. The work is staged by
legal producer and program. A reviewed registry entry is only an acquisition
route; each program remains incomplete until retained artifacts, parser
replay, timing evidence, and previous-as-known lineage have been qualified.

## Complete ECB monetary-policy decision archive

`histdatacom.market_context.ecb_monetary_policy_archive` qualifies the ECB's
complete `Monetary policy decisions` publication series from 5 January 2000
through 10 September 2026. The 15 December 1999 decision is retained as the
predecessor for the first previous-as-known comparison.

The source inventory is the ECB's versioned public FOEDB database, not a URL
guess or a third-party calendar. Replay retains and verifies:

- `versions.json` and its one active database version;
- the versioned `metadata.json` record schema;
- all 81 data chunks containing 20,073 publication records;
- the exact selection predicate: publication type `92` and either observed
  source title, `Monetary policy decisions` or `Monetary Policy Decisions`;
- the 1999 predecessor and all 299 in-window HTML decision pages; and
- byte length and SHA-256 for every one of the 383 official artifacts.

The retained corpus contains 42,121,110 bytes and 383 distinct SHA-256
digests. Its compact packaged receipt is
`ecb-archive-manifest:sha256:7cc0ce75602e8e9d425e0fae46458b3d81025ddaff4f125ea2ad0fcc2f38cbbe`.

Each decision preserves a native `RATE_SET` with separate components for the
main refinancing operations, marginal lending facility, and deposit facility.
The parser does not assign values by a fixed position: it reads the
source-authored component order. This is essential after 12 September 2024,
when the ECB began presenting the deposit rate first. Ordered comparison with
the preceding official page produces 238 holds, 32 easing decisions, and 29
tightening decisions with no discontinuity in the rate lineage.

FOEDB's publication timestamp has two empirically distinct meanings:

- 227 in-window publications through 7 September 2017 carry date-only
  placeholder epochs and therefore do not receive invented intraday clocks;
- 72 publications from 26 October 2017 onward carry exact minutes, with the
  local decision clock moving from 13:45 to 14:15 Europe/Berlin on 21 July
  2022.

The 17 September 2001 and 8 October 2008 releases are retained as emergency
actions. Other publications belong to the ordinary Governing Council decision
sequence, but that classification does not replace contemporaneous calendar
evidence for a historical scheduled clock. The official archive also does not
publish a historical market-consensus rate set, so the qualification does not
manufacture numeric consensus or surprise values.

An operator with the retained corpus can reproduce the packaged receipt with:

```console
uv run python scripts/refresh_ecb_monetary_policy_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-ecb-corpus
```

The refresh discovers the active database version before constructing bounded
metadata, chunk, and report requests. `--fetch-missing` is explicit; without
it, a missing retained artifact fails closed.

## Complete ECB monetary-policy account archive

`histdatacom.market_context.ecb_monetary_policy_accounts_archive` independently
qualifies all 96 FOEDB type-20 accounts published from 19 February 2015 through
27 August 2026. The selection retains 83 versioned FOEDB database artifacts
and all 96 official HTML pages: 179 artifacts, 26,253,142 bytes, and 179 unique
SHA-256 digests. Its packaged receipt is
`ecb-account-archive-manifest:sha256:4fac63ef83fd6bebf1c810f91f9b0a75b6aec20d321d3b52baa0737021b6b814`.

Replay parses the source-authored meeting dates rather than inferring them from
publication dates. It preserves three observed title eras (35 generic titles,
six long-form Governing Council titles, and 55 meeting-date titles), one
source-omitted meeting-year inference, and release lags of 21 through 64 days.
Nineteen early FOEDB timestamps remain date-only; 77 later publications retain
their exact local minute, including the distinct 13:25 and 13:30 publications
on 28 August 2025.

Ninety-three ordinary accounts link to the exact qualified decision occurrence
by meeting end date. The 18 March 2020 PEPP emergency meeting and strategy
reviews of 7 July 2021 and 25 June 2025 remain explicit, unlinked exceptions;
they are not forced onto unrelated three-rate decisions. Accounts are
documentary releases and do not supply a numeric event consensus or surprise.

An operator with the retained corpus can reproduce the account receipt with:

```console
uv run python scripts/refresh_ecb_monetary_policy_accounts_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-ecb-corpus
```

## Complete ECB monetary-policy statement archive

`histdatacom.market_context.ecb_monetary_policy_statements_archive` qualifies
all 270 decision-linked monetary-policy statements from 5 January 2000 through
10 September 2026. The exact selection combines FOEDB type `54`, six observed
source-title spellings, the monetary-policy-statement URL family, and an
independently qualified decision date. This excludes unrelated press events
that FOEDB stores under the same broad type and historical path family.

Replay retains the 83 versioned FOEDB artifacts and all 270 official statement
pages: 353 artifacts, 48,734,109 bytes, and 353 unique SHA-256 digests. Its
packaged receipt is
`ecb-statement-archive-manifest:sha256:65437f191d30425c37a735049fce9640b8fdea11eacf6095d9e8e3a72da900b4`.
Every page contains exactly one of nine observed historical heading forms and
every retained statement links to exactly one qualified decision occurrence.

The first 196 statement records retain date-only placeholder epochs. The 74
later records preserve an exact FOEDB minute: 40 at 14:45 and 34 at 15:00
Europe/Berlin, with the clock change beginning on 21 July 2022. Twenty-nine of
the 299 qualified decision dates have no statement. These are preserved as
explicit absences, chiefly early twice-monthly decisions without a press
conference plus the 17 September 2001 and 8 October 2008 emergency actions.
No replacement statement or clock is fabricated.

An operator with the retained corpus can reproduce the statement receipt with:

```console
uv run python scripts/refresh_ecb_monetary_policy_statements_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-ecb-corpus
```

## Complete ECB staff-projection release archive

`histdatacom.market_context.ecb_staff_projections_archive` qualifies all 90
main ECB and Eurosystem staff macroeconomic projection rounds from 3 June 2004
through 10 September 2026. FOEDB type `95` and the independent official
all-releases page must reconcile one-to-one by round, publication date, title,
producer, and canonical main-document URI. The official standalone archive
does not begin until June 2004; the implementation does not manufacture
projection rounds for 2000 through May 2004 from unrelated publications.

Replay retains the 83 versioned FOEDB artifacts, the all-releases index, 90
official PDFs, and the 31 available HTML counterparts: 205 artifacts,
43,393,009 bytes, and 205 distinct SHA-256 digests. Its packaged receipt is
`ecb-projection-archive-manifest:sha256:271a76a2336ca13aea7d42fe3d9fa2282655093e41f679a5ccf315ae95e777f3`.
The inventory contains 45 ECB staff rounds in March/September and 45
Eurosystem staff rounds in June/December. It also preserves the September 2006
round's early 31 August publication date instead of forcing the release date
to equal the round month.

Each round is explicitly scoped as an annual euro-area central-bank
projection with core real-GDP-growth and HICP-inflation targets. The retained
source text supplies a horizon one, two, or three years beyond the release
year: 29, 51, and 10 rounds respectively. These are period projections, not
monthly or quarterly event-consensus estimates, and they cannot populate a
calendar-style consensus field. The compact receipt intentionally qualifies
release identity, scope, and horizon without transcribing every projected
value from the retained reports; value-level forecast mapping remains part of
the separately governed forecast work.

The first 54 projection records preserve date-only placeholder epochs. The 36
later records retain their exact FOEDB minute: 19 at 15:30 and 17 at 15:45
Europe/Berlin, with the observed clock change in September 2022. The first 59
rounds are PDF-only; the 31 rounds from March 2019 onward retain both HTML and
PDF evidence.

An operator with the retained corpus can reproduce the projection receipt
with:

```console
uv run python scripts/refresh_ecb_staff_projections_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-ecb-corpus
```

## Complete ECB monthly monetary-developments archive

`histdatacom.market_context.ecb_monetary_developments_archive` accounts for all
333 FOEDB type-10 publications from 30 December 1998 through 27 August 2026.
The qualified issue window contains 320 monthly releases from 28 January 2000
through 27 August 2026, covering reference months December 1999 through July
2026. The 28 December 1999 release for November 1999 is retained as the exact
predecessor, while the 12 still earlier type-10 records remain explicitly
counted as outside the issue window.

Replay retains the 83 versioned FOEDB artifacts and every document named by
the 321 selected records: 248 PDFs and 96 HTML pages. The resulting 427
artifacts contain 62,669,957 bytes and 427 distinct SHA-256 digests. The
packaged receipt is
`ecb-monetary-developments-archive-manifest:sha256:512ae529aa95c07c0d6cfddcd899d747130b4b14af77a4faf826e2eb62112a3f`.
The in-window source-format eras comprise 224 PDF-only releases, 73 HTML-only
releases, and 23 releases with both English HTML and a localized PDF. Localized
PDFs remain hashed evidence; values come from the corresponding English HTML.

Each release parses the annual growth rate of broad monetary aggregate M3 and
the preceding month's value as known in that document. The latter is compared
with the immediately preceding release's first-published value, yielding 126
explicit revision occurrences. For example, the 25 February 2000 release
reports January M3 growth of 5.0% and revises December to 6.2% from its
first-published 6.4%. This lineage uses the retained release text rather than a
latest-state data endpoint.

Reference periods are monthly and gap-free. One historical URI,
`m3release.pdf`, lacks a period token; January 2010 is sequence-derived and
independently required to match the retained heading. Two FOEDB placeholder
epochs are one month early: the headings in `md0112.pdf` and `md0210.pdf`
establish release dates of 28 January 2002 and 28 November 2002. Both
corrections remain explicit provenance flags instead of silently replacing the
source metadata.

The first 211 in-window records remain date-only. The 109 records beginning on
28 August 2017 preserve an exact 10:00 Europe/Berlin publication minute. The
release text explicitly discloses seasonal and end-of-month calendar
adjustment from 26 July 2001 onward; the earlier absence of that exact wording
is not reclassified as an unadjusted series. The compact receipt qualifies M3
annual growth and its prior-month comparison, not every money, credit,
counterpart, or annex series.

An operator with the retained corpus can reproduce the monthly M3 receipt
with:

```console
uv run python scripts/refresh_ecb_monetary_developments_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-ecb-corpus
```

## Eurostat HICP first-release and publication archive

`histdatacom.market_context.eurostat_hicp_archive` keeps Eurostat's two
first-published HICP tables and its release publications as independent
official evidence surfaces. It does not use the current HICP time series as a
surrogate historical vintage.

The retained JSON-stat tables are filtered to monthly all-items annual rates
for the euro area, Germany, and France:

- retired `prc_hicp_fp` / ECOICOP `CP00` contains 1,238 observations: 312
  final values for each economy from January 2000 through December 2025, plus
  101 euro-area, 100 German, and 101 French flash values from 2017 onward;
- current `prc_hicp_fpd` / ECOICOP v2 `TOTAL` contains 981 observations: 319
  final values for each economy through July 2026 and eight flash values for
  each economy from January through August 2026; and
- the 936 common cells produce 365 explicit differences. There are 173
  numeric differences (34 euro-area, 124 German, and 15 French) and 204 French
  status differences; 12 cells differ in both respects.

The qualification therefore prefers the retired table through December 2025
and uses the successor table only for 2026 onward. Both complete source
responses and every difference remain hash-bound, so that policy is auditable
rather than destructive.

Release evidence is independently retained from all 180 pages of the
dedicated HICP publication index. Its 540 cards contain 539 in-scope HICP
publications from the source-dated 5 January 2004 release through 1 September
2026. One G20 inflation card on index page 103 is retained as an explicit
non-euro-area exclusion. Forty additional official pre-migration releases
were recovered from Eurostat's date-addressed Euro-indicator products: 22
final publications and 18 flash estimates from 5 November 2001 through 17
December 2003. The first is Eurostat's October 2001 flash estimate. The legacy
discovery surface is incomplete, so the manifest enumerates every missing
final reference month from January 2000 and every missing flash month from
October 2001 instead of manufacturing release dates. The combined lineage has
579 releases: 289 final and 290 flash. It records 30 unavailable final
reference months and nine unavailable flash reference months.

Each selected occurrence retains the official primary artifact named by the
index, whether that artifact is a landing page or a direct HTML/PDF document.
Older product stubs must also retain the linked English release document; the
pre-migration set contains 38 HTML documents and two PDFs. Newer `/w/` pages
contain the release directly. Twenty-eight early `/w/` pages also expose a
same-release PDF, which is retained; the other 62 are valid landing-only
evidence. The 22 February 2024 final page embeds only the prior 1 February
flash PDF, so that cross-release link is deliberately excluded rather than
misattributed. Every occurrence keeps flash versus final stage, reference
month, headline lexical value, and the date encoded by its official URI.
Source pages and some migrated cards display one day earlier than that URI;
those dates and offsets remain explicit. The migrated primary set comprises
90 landing pages, 16 HTML documents, and 433 PDFs. A single malformed 2011
document token omits a year digit, so its 28 February 2011 index date is
retained as an explicit source-date correction. No intraday clock is inferred.

The compact receipt binds 829 raw artifacts, 189,367,122 bytes, and 829
distinct SHA-256 digests. Its identity is
`eurostat-hicp-archive-manifest:sha256:4d26c01714ac5fb9deaca70d2ee82f1a1e7e9cdd48858d25c03e35c2b5f3a8f6`.
The current registry-bound packaged JSON is 2,988,977 bytes with SHA-256
`5e99dd15e408e928703bdce1f47e085ec6a651d92eeceec97f4fa1eebf89ff4b`.
The release-page/date audit records 40 page offsets and 15 migrated-card
offsets of one day.

Release headlines and first-published-table values are compared but never
collapsed. Historical differences can reflect the source-era euro-area
composition or later table treatment. Germany and France remain separate
economy scopes, and unavailable country flash cells remain absent. Eurostat
does not provide a historical market-consensus series, so this archive does
not manufacture consensus or surprise values. Of 579 release headlines, 398
have a same-stage, same-reference-month euro-area table value and 38 differ
numerically; 181 releases carry no additional German or French table value
because those country cells are unavailable for the occurrence.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_hicp_archive.py \
  --as-of 2026-09-15 \
  --page-count 180 \
  --legacy-uri-file /path/to/legacy-release-uris.txt \
  --source-directory /path/to/retained-eurostat-hicp-corpus
```

`--fetch-missing` is explicit. The refresh is sequential, honors a minimum
13-second interval between Eurostat requests, and handles bounded `429`
retry-after responses.

## Eurostat quarterly-GDP publication archive

`histdatacom.market_context.eurostat_gdp_archive` qualifies 279 quarterly
euro-area GDP publications from 10 January 2002 through 7 September 2026. The
inventory is Eurostat's official `CAT_PREREL` Atom search, not a third-party
calendar or a guessed product sequence. Ten exact result pages contain 932
unique entries. A deterministic GDP, euro-area, and movement predicate matches
280 candidates; the 17 October 2014 ESA 2010 level-shift methodology item is
retained as the one explicit non-release exclusion.

The selected publications cover reference quarters 2001-Q3 through 2026-Q2.
Their source-dated landing pages preserve 42 preliminary-flash, 94 flash, 35
first-estimate, 37 second-estimate, eight third-estimate, and 63
regular-estimate occurrences. Stage classification is evidence-graded from
the source-stated reference quarter, each publication lag, and the observed
within-quarter sequence because many legacy headlines omit an explicit stage
label. All four headlines that do state a stage explicitly agree with that
classification. It never collapses successive estimates into one latest
value. Legacy product suffixes such as `AP2`, `AP_0`, and `BP1` remain exact
identifiers.

Each occurrence binds the Atom title and product-code date to the exact
landing bytes and source-authored page date. The archive also retains all 248
discovered English source documents (216 PDFs and 32 HTML/HTM artifacts); the
remaining 31 modern publications embed the complete release in their landing
page. The receipt retains 46 exact publication timestamps from modern
first-party metadata and keeps legacy products date-only. It also records 34
source-authored page-date offsets: 33 pages dated one day before the
product-code date and one page dated one day after it.

The release-value layer preserves the current-reference-quarter
quarter-over-quarter and year-over-year table values for source-supported
`EA`, Germany, and France rows. Every occurrence independently reconciles its
`EA` quarter-over-quarter table value with the Atom/landing headline, and
successive estimate values remain separate so unchanged and changed revision
comparisons can be derived and validated. The receipt contains 1,582 published
values and 982 same-series comparisons against the current revised table, of
which 247 differ. Historical aggregate values retain composition-neutral `EA`
scope because euro-area membership changed during the archive; they are not
retrospectively relabeled `EA20`. German and French rows are Eurostat's values
as published in the aggregate release, not a substitute for the separate
national-producer release lineage.

The searchable publication surface begins with the release for 2001-Q3, so
2000-Q1 through 2001-Q2 remain six explicit release-artifact gaps rather than
inferred dates. That boundary quarter is itself only partially represented by
its 10 January 2002 second estimate and 7 February 2002 third estimate; its
earlier flash and first-estimate artifacts remain unavailable. The compact
receipt binds 538 raw artifacts and 538 distinct SHA-256 digests totaling
118,956,446 bytes: ten search pages, one revised-series response, 279
source-dated landings, and 248 English release documents. Its identity is
`eurostat-gdp-archive-manifest:sha256:1d7182518601561dfd6b04cd10f1209f9cb64aa3bbdd6632c005af66f15ef1a2`.
The current registry-bound packaged JSON is 1,973,058 bytes with SHA-256
`63b9656c764e90a95cecbb50da6e51e4e39e822e2da7565a582b5cd66c625a68`.

The independently retained `namq_10_gdp` JSON-stat response contributes 636
current revised observations: quarter-over-quarter and year-over-year real-GDP
growth for EA20, Germany, and France across 106 quarters from 2000-Q1 through
2026-Q2. Those values are a scope and revised-series cross-check only. They do
not replace a historical release headline, establish the composition of a
source-era euro area, or reconstruct national German and French publication
vintages. Eurostat supplies no historical market-consensus series, so numeric
consensus and surprise values remain unavailable.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_gdp_archive.py \
  --as-of 2026-09-15 \
  --source-directory /path/to/retained-eurostat-gdp-corpus
```

`--fetch-missing` is explicit. Network acquisition is sequential, enforces at
least 13 seconds between Eurostat requests, and handles bounded `429`
retry-after responses.

## Eurostat construction-output publication archive

`histdatacom.market_context.eurostat_construction_output_archive` qualifies
257 euro-area construction-output publications from 13 March 2002 through 18
September 2026. Ten populated pages of Eurostat's official `CAT_PREREL` Atom
search contain 916 raw results and 915 unique alternate URIs. The sole repeated
URI is an identical excluded job-vacancy result. A deterministic predicate for
the four source title eras selects 257 releases; all 658 unique nonselected
results remain explicit exclusions.

The source changes frequency during the retained lineage. Twenty-one releases
cover 20 unique reference quarters from 2001-Q4 through 2006-Q3, including two
March 2003 publications for 2002-Q4. The monthly era contains 236 releases for
236 unique reference months from November 2006 through July 2026; April 2015
is the sole publication gap. Quarter and month identities are never coerced
into one cadence or compared as revisions across frequencies.

The May 2016 Atom entry carries an unrelated inflation summary that mentions
April, while the source landing and attached construction release consistently
identify March 2016. Reference-period evidence therefore follows the retained
release document, then its landing, with Atom summary text only as a fallback.
The February 2018 release also opens with a rebasing notice for reference month
January before its December construction headline, so a qualified
month-compared-with clause precedes an incidental methodology month.

The earliest Atom summaries and landing pages omit the reference quarter, so
the archive reads that evidence from their first-party release documents. Each
occurrence retains its product-code date, source-authored page date and
available timestamp, publication lag, signed headline movement, and historical
euro-area composition.

All 257 landing pages and all 225 English documents they expose are retained:
11 HTML and 214 PDF. Thirty-two self-contained modern landings expose no
separate document. Eleven early landings are source-dated one day before their
product codes, while 46 modern landings preserve an exact source-authored
publication timestamp. Six landing-only releases from October 2024 through
March 2025 expose a qualified headline but no parseable EA/Germany/France
source table; the archive does not backfill those absent cells from the current
cube.

The September 2011 PDF repeats June 2011 where May belongs in an otherwise
contiguous February-through-July table header. The archive binds the exact
observed sequence and applies one product-code-scoped correction; it does not
generalize that repair to other publications. Footnote digits extracted next
to a valid historical EA composition label remain label metadata rather than a
different composition.

The February 2020 publication omits France's February monthly and annual
cells, and the March 2020 publication omits the France row from both supported
tables. Those source-authored pandemic-era gaps remain absent rather than
being filled from the current cube.

The independently retained `sts_copr_m` JSON-stat response declares January
2000 through July 2026 and contains 1,914 non-null revised monthly
observations: month-over-month and year-over-year percentage changes for EA21,
Germany, and France across 319 months. It is a scope and revised-series
cross-check only, not a substitute for quarterly evidence, historical release
values, or independent national-producer publications.

The frozen receipt contains 8,884 published values: 228 releases expose 36
values, one exposes 34, one exposes 24, 20 expose 30, one exposes 12, and six
expose only the qualified headline value. It records 7,213 comparable
release-to-release cells, of which 4,358 changed, plus 5,590 comparisons with
the current dataset, of which 5,384 changed. The 493 retained artifacts have
493 unique digests and total 149,658,751 bytes. Two independent offline
rebuilds produced the same 6,690,664-byte canonical JSON file, manifest
identity `f9a5e5a163b7461d4986f247bd42f300b2588a66b358d2475a2329f617524f7a`,
and file SHA-256
`a93831561b4ff506815ff0b93ba3605d5c8c7f402ddac27c33c0f09ca9662027`.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_construction_output_archive.py \
  --as-of 2026-09-18 \
  --source-directory /path/to/retained-eurostat-construction-output-corpus
```

`--fetch-missing` is explicit. Network acquisition is sequential, enforces at
least 13 seconds between Eurostat requests, and handles bounded `429`
retry-after responses.

## Eurostat monthly-unemployment publication archive

`histdatacom.market_context.eurostat_unemployment_archive` qualifies 296
monthly euro-area unemployment publications from 4 January 2002 through 1
September 2026. Four populated pages of Eurostat's official `CAT_PREREL` Atom
search contain 383 unique results; 317 titles mention unemployment. The
deterministic monthly-headline predicate selects 296 releases. All 87
nonselected search results remain explicit exclusions: 63 summary-only
matches, 21 regional articles, and three thematic labour articles.

The selected releases cover reference months November 2001 through July 2026.
February 2003 is the one internal searchable-publication gap; January 2000
through October 2001 are explicit pre-search gaps supplied by the current
dataset boundary, not inferred release artifacts. Each occurrence retains its
product-code date, source-authored page date, available first-party timestamp,
reference month, publication lag, exact headline rate and movement wording.
Historical aggregate scope follows the source-era euro-area composition from
EA12 through EA21 rather than relabeling old values with today's membership.

The retained corpus contains all 296 landings and all 264 English documents
they expose: 234 PDF and 30 HTML. Thirty-two self-contained landings have no
separate English document; one is the source-authored 4 November 2004 release
prose and the other 31 are modern full landing pages. The landings establish 46
exact first-party timestamps. Their page dates equal the product-code date in
265 releases, precede it by one day in 30 legacy releases, and follow it by
three days only for `3-30032007-bp` (30 March versus 2 April 2007); both dates
remain explicit.

Where the English publication exposes the seasonally adjusted totals table,
the archive preserves the rate rows for the source-era euro area, Germany, and
France across every reported month. Repeated cells remain separate so
unchanged and changed revisions can be counted; euro-area revision chains are
partitioned by membership composition. Germany and France are Eurostat's
harmonised values within the aggregate publication; they do not replace the
independent Destatis and INSEE release lineages. Eurostat provides no
historical market-consensus series, so the archive does not manufacture
consensus or surprise values.

Modern full landing tables changed from split year/month headers to `Mon-YY`
headers, and the January 2026 composition transition exposes parallel EA20 and
EA21 rows. Both layouts are parsed against the release's independently
qualified reference month, and the source-era composition selects the
appropriate aggregate row.

Legacy English documents likewise change from a 20-economy monthly matrix to
an eight-month country-row table and later to a five-period rates-and-persons
table. Replay accepts only the observed header and row-label eras, binds
concatenated source footnotes without treating them as membership labels, and
repairs only the bounded PDF extraction artifacts observed in year tokens,
decimal rates, and two split month tokens. Intervening chart-only PDFs do not
expose a totals table; those releases retain their independently qualified
headline instead of manufacturing country rows from chart geometry.

The packaged receipt preserves 4,036 published values across 232 table-bearing
releases, while the other 64 releases retain one qualified headline value. It
records 3,146 within-scope consecutive revision comparisons, of which 1,144
change, plus 2,683 comparisons with the retained current dataset, of which
2,248 differ. The receipt binds 565 raw artifacts and 565 distinct SHA-256
digests totaling 134,483,606 bytes. Its identity is
`eurostat-unemployment-archive-manifest:sha256:9f25332871763036e1b4350efc582cbc50c66f508196eaadd8887daa4e34af64`.
Two fresh offline rebuilds produced byte-identical 3,577,040-byte packaged JSON
with SHA-256
`3b0389571b63382bb444f9c11d6a04fe9073bdccbc8a4feedbff1771541f8e79`.

The independently retained `une_rt_m` JSON-stat response declares the monthly
cube from January 2000 through August 2026 and contains 957 non-null revised
observations for EA21, Germany, and France through July 2026. Its three August
coordinates are still null at the qualification boundary. These values are a
scope and revised-series cross-check only. Historical EA comparisons are made
only when the release itself uses EA21; earlier membership compositions are
not treated as like-for-like.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_unemployment_archive.py \
  --as-of 2026-09-17 \
  --source-directory /path/to/retained-eurostat-unemployment-corpus
```

`--fetch-missing` is explicit. Network acquisition is sequential, enforces at
least 13 seconds between Eurostat requests, and handles bounded `429`
retry-after responses.

## Eurostat monthly-industrial-production publication archive

`histdatacom.market_context.eurostat_industrial_production_archive` qualifies
297 monthly euro-area industrial-production publications from 21 January 2002
through 16 September 2026. Thirteen populated pages of Eurostat's official
`CAT_PREREL` Atom search contain 1,213 raw results and 1,212 unique alternate
URIs. The sole repeated URI is an identical job-vacancy result. The exact
`Industrial production ...` title-prefix predicate selects 297 releases; all
915 unique nonselected results remain explicit exclusions.

The releases form an uninterrupted reference-month lineage from November 2001
through July 2026. Each occurrence retains its product-code date,
source-authored page date, available first-party timestamp, reference month,
publication lag, signed headline movement, and historical euro-area
composition. Headline wording is normalized semantically: for example, “down
by 0.8%” retains the source lexical magnitude while its comparison value is
`-0.8`.

One September 2020 PDF repeats June in the otherwise contiguous February–July
monthly table header where May belongs. That exact observed header and its
single correction remain a code-scoped exception; the parser does not apply a
general silent month repair.

All 297 landing pages and 262 discovered English documents are retained: 31
HTML and 231 PDF. Thirty-five source-complete or anomalous landings expose no
separate English document. Thirty landing dates precede their product-code
dates by one day. The malformed `4-1703204-bp` code resolves to 17 March 2004
while its Atom and landing metadata say 21 March; that migrated landing has no
body or document, so its January 2004 reference month is the archive's one
explicit cadence inference rather than source-stated body evidence.

Legacy Word-exported HTML places monthly/annual table meaning in headings
outside the table, while later PDFs expose text-bearing country rows. The
archive binds both layouts and retains supported month-over-month and
year-over-year values for the source-era euro area, Germany, and France.
Production-index tables are deliberately excluded from percentage-change
parsing. Repeated source-era cells remain separate evidence so revision chains
can be counted without crossing euro-area membership compositions.

The packaged receipt preserves 10,278 published values: 284 releases expose
the complete 36-cell supported table, two preserve smaller source layouts,
and 11 retain one independently qualified headline value. It records 8,393
within-scope consecutive revision comparisons, of which 4,660 change, plus
6,936 comparisons with the retained current dataset, of which 6,473 differ.
The receipt binds 573 raw artifacts and 573 distinct SHA-256 digests totaling
151,722,249 bytes. Its identity is
`eurostat-industrial-production-archive-manifest:sha256:9d8dabc5fc410b83b8ce6fe7ebbc21d48d23fe72622e36ce17a8219219be5b46`.
Two fresh offline rebuilds produced byte-identical 7,767,781-byte packaged JSON
with SHA-256
`859268e41f9dc7bf017969447034e3f8090c591a08a606277378e458d38841af`.

The independently retained `sts_inpr_m` JSON-stat response declares January
2000 through July 2026 and contains 1,914 non-null revised observations: both
qualified percentage-change measures for EA21, Germany, and France across 319
months. Its valid coordinate pairs are seasonally adjusted/previous-period for
month-over-month change and calendar adjusted/same-month for year-over-year
change. It is a scope and revised-series cross-check only, not a replacement
for historical release values or independent national-producer publications.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_industrial_production_archive.py \
  --as-of 2026-09-18 \
  --source-directory /path/to/retained-eurostat-industrial-production-corpus
```

`--fetch-missing` is explicit. Network acquisition is sequential, enforces at
least 13 seconds between Eurostat requests, and handles bounded `429`
retry-after responses.

## Eurostat retail-trade publication archive

`histdatacom.market_context.eurostat_retail_trade_archive` qualifies 298
source-dated euro-area retail-trade publications from 12 April 2000 through 4
September 2026. Five populated pages of Eurostat's official `CAT_PREREL` Atom
search contain 417 raw results and 417 unique alternate URIs. A deterministic
`Volume of retail trade ...` title predicate selects the 298 releases; all 119
nonselected results remain explicit exclusions comprising 77 labour-market, 28
broad-search, and 14 production releases.

The first searchable publication reports January 2000. The source lineage then
resumes with October 2001, runs through November 2003, has no searchable
December 2003 release, and resumes continuously from January 2004 through July
2026. Both the February 2000 through September 2001 gap and the December 2003
gap remain explicit rather than being reconstructed from the current
dissemination table.

The source headline changes measure during the lineage. Headlines report the
year-over-year rate through reference month June 2007 and the month-over-month
rate from July 2007 onward. Each release preserves that authored measure,
signed direction, reference month, publication lag, historical euro-area
composition, and any supported euro-area, German, and French table values.
The April 2000 release therefore retains its 2.3% annual headline alongside its
0.3% monthly table value instead of treating them as a conflict.

All 298 landing pages expose 267 unique English documents: 32 HTML and 235
PDF. Thirty-one self-contained modern landings expose no separate document.
The 5 May 2023 landing links the same content-addressed PDF for preview and
download; replay collapses only the `download=true` variant while retaining
the identical content path and other query parameters. Forty-six modern
landings expose a source-authored exact timestamp. Thirty-two legacy landings
are dated one day before their product codes, while `4-04052007-ap` is
source-dated three days later and `4-19032009-bp` six days earlier; the two
exceptional dates agree across Atom and landing metadata and remain explicit.

Across the corpus, supported release tables contribute 9,393 source-authored
values and 7,528 comparable repeated-month observations; 4,933 comparisons
change between publications. Of 6,195 table values comparable with the current
EA21/Germany/France cube, 5,842 differ, which is retained as revision evidence
rather than normalized away.

The earliest legacy HTML document supplies January 2000 directly and establishes
an observed 72-day publication lag. Its two-digit table headers also include
1999 predecessor months, which replay resolves with a bounded 1990s/2000s
century pivot. The August 2004 product code `4-040582004-ap` contains a source
typo; one exact override binds its Atom entry, landing, and suffixed English
document path without generalizing the malformed shape.

The independently retained `sts_trtu_m` JSON-stat response reports update time
16 September 2026 and contains 1,901 non-null revised observations from January
2000 through July 2026. It covers month-over-month and year-over-year retail-
trade-volume changes for EA21, Germany, and France. The 13 absent cells are
explicit: EA21's January 2000 monthly rate and its January through December
2000 annual rates. This current cube is comparison evidence only and never
fills a missing historical publication or substitutes EA21 for a source-era
euro-area composition.

The receipt binds 571 raw artifacts and 571 distinct SHA-256 digests totaling
156,617,385 bytes. Its identity is
`eurostat-retail-trade-archive-manifest:sha256:4b8a55383656205e2449b1036aa8d214d46dfde19248361d9dcc9986f341caeb`.
Two fresh offline rebuilds produced byte-identical 6,566,066-byte canonical
JSON with SHA-256
`0756139dccb2ac53cf12fb2f5d1d4f91db1697c06c31d13c146cb0c2c883a0c3`.

An operator with the exact retained corpus can rebuild or replay the receipt
with:

```console
uv run python scripts/refresh_eurostat_retail_trade_archive.py \
  --as-of 2026-09-18 \
  --source-directory /path/to/retained-eurostat-retail-trade-corpus
```

`--fetch-missing` is explicit. Network acquisition is sequential, enforces at
least 13 seconds between Eurostat requests, and handles bounded `429`
retry-after responses.

## Remaining issue scope

These ECB decision, account, statement, staff-projection, monthly
monetary-developments, Eurostat HICP, GDP, monthly-unemployment, and
monthly-industrial-production, construction-output, and retail-trade archives
complete four ECB monetary-policy artifact families, the selected euro-area
money/credit lineage, the harmonized inflation and aggregate-GDP anchors, and
the first labour-market and production slices, not issue #539 as a whole.
Euro-area employment and earnings, trade/current-account, and survey releases,
the remaining ECB statistical families, Destatis and Bundesbank programs for
Germany, and INSEE and Banque de France programs for France still require
their own empirical archive
qualifications before the regional calendar can produce a closure receipt.

Official entrypoints:

- [ECB monetary-policy decisions](https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html)
- [ECB public FOEDB version pointer](https://www.ecb.europa.eu/foedb/dbs/foedb/publications.en/versions.json)
- [Eurostat release calendar](https://ec.europa.eu/eurostat/news/release-calendar)
- [Eurostat HICP publications](https://ec.europa.eu/eurostat/en/web/hicp/publications)
- [Eurostat GDP release search](https://ec.europa.eu/eurostat/search?text=GDP)
- [Eurostat quarterly GDP dataset](https://ec.europa.eu/eurostat/databrowser/view/namq_10_gdp/default/table)
- [Eurostat unemployment release search](https://ec.europa.eu/eurostat/search?text=unemployment)
- [Eurostat monthly unemployment dataset](https://ec.europa.eu/eurostat/databrowser/view/une_rt_m/default/table)
- [Eurostat industrial-production release search](https://ec.europa.eu/eurostat/search?text=industrial%20production)
- [Eurostat monthly industrial-production dataset](https://ec.europa.eu/eurostat/databrowser/view/sts_inpr_m/default/table)
- [Eurostat construction-output release search](https://ec.europa.eu/eurostat/search?text=production%20in%20construction)
- [Eurostat monthly construction-output dataset](https://ec.europa.eu/eurostat/databrowser/view/sts_copr_m/default/table)
- [Eurostat retail-trade release search](https://ec.europa.eu/eurostat/search?text=retail%20trade)
- [Eurostat monthly retail-trade dataset](https://ec.europa.eu/eurostat/databrowser/view/sts_trtu_m/default/table)
- [ECB monetary-policy accounts](https://www.ecb.europa.eu/press/accounts/html/index.en.html)
- [ECB monetary-policy statements](https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/html/index.en.html)
- [ECB staff projections](https://www.ecb.europa.eu/press/projections/html/all-releases.en.html)
- [ECB monetary developments](https://www.ecb.europa.eu/press/stats/md/html/ecb.md2607~e7127e7d02.en.html)
