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
`ecb-archive-manifest:sha256:fbaf4a82a577a08a32ce7e362d81013d316d1e9327b68cbdaabc171699712ce7`.

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
`ecb-account-archive-manifest:sha256:5fad33b623094208e4fa661647b1477f1522649455c2b9faa260af4dbb788697`.

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
`ecb-statement-archive-manifest:sha256:3dabf9f1af4c7ae74e2d4745abba001cd52f3ca52743c82e668159c0ac972a92`.
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
`ecb-projection-archive-manifest:sha256:4017121492436032b16dd709cdeec01213bfc8ff7e5d03d4954c7dbd4e40e0a4`.
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
`ecb-monetary-developments-archive-manifest:sha256:085c88744ab7c98e2d3683a4c66a551e99cc60db38f30c15e3a906ba73530007`.
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
`eurostat-hicp-archive-manifest:sha256:dc6748a381c10fa8daf5bfb89dd4287e1f54065f5b7c1342835fd1d207877066`.
Two consecutive offline rebuilds produced byte-identical packaged JSON with
SHA-256 `3648b4433379f7d810c1398a9fb76d997c735ca5d8f7da5c6a6de8b4f789766d`.
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

## Remaining issue scope

These ECB decision, account, statement, staff-projection, monthly
monetary-developments, and Eurostat HICP archives complete the four ECB
monetary-policy artifact families, the selected euro-area money/credit
lineage, and the harmonized inflation anchor, not issue #539 as a whole.
Euro-area GDP, labour, production, trade/current-account, and survey releases,
the remaining ECB statistical families, Destatis and Bundesbank programs for
Germany, and INSEE and Banque de France programs for France still require
their own empirical archive qualifications before the regional calendar can
produce a closure receipt.

Official entrypoints:

- [ECB monetary-policy decisions](https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html)
- [ECB public FOEDB version pointer](https://www.ecb.europa.eu/foedb/dbs/foedb/publications.en/versions.json)
- [Eurostat release calendar](https://ec.europa.eu/eurostat/news/release-calendar)
- [Eurostat HICP publications](https://ec.europa.eu/eurostat/en/web/hicp/publications)
- [ECB monetary-policy accounts](https://www.ecb.europa.eu/press/accounts/html/index.en.html)
- [ECB monetary-policy statements](https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/html/index.en.html)
- [ECB staff projections](https://www.ecb.europa.eu/press/projections/html/all-releases.en.html)
- [ECB monetary developments](https://www.ecb.europa.eu/press/stats/md/html/ecb.md2607~e7127e7d02.en.html)
