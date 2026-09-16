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
`ecb-archive-manifest:sha256:eda20db2d19520a5181bef3a3fded63b74353f52de131fea325938920affeb18`.

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
`ecb-account-archive-manifest:sha256:e36d73ada818582c1ada1eedc9bcdc98e39b76f649164c77eb5cc4cf6db7fc40`.

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
`ecb-statement-archive-manifest:sha256:6653f89ab5e780d9913b4f9e858297499af64b56ab2199373b1b405ea40a0961`.
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
`ecb-projection-archive-manifest:sha256:6ec72398f96749bdde29c4e28ffd9afa87e1287b29f8bc8b1672f618bbe0e19f`.
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
`ecb-monetary-developments-archive-manifest:sha256:1b39fa9ce46fae4d645a22f5728e911407c31b5a22c94f3771971ded10a69859`.
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

## Remaining issue scope

These ECB decision, account, statement, staff-projection, and monthly
monetary-developments archives complete the four ECB monetary-policy artifact
families and the selected euro-area money/credit lineage, not issue #539 as a
whole. Eurostat aggregate releases and the remaining ECB statistical families,
Destatis and Bundesbank programs for Germany, and INSEE and Banque de France
programs for France still require their own empirical archive qualifications
before the regional calendar can produce a closure receipt.

Official entrypoints:

- [ECB monetary-policy decisions](https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html)
- [ECB public FOEDB version pointer](https://www.ecb.europa.eu/foedb/dbs/foedb/publications.en/versions.json)
- [Eurostat release calendar](https://ec.europa.eu/eurostat/news/release-calendar)
- [ECB monetary-policy accounts](https://www.ecb.europa.eu/press/accounts/html/index.en.html)
- [ECB monetary-policy statements](https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/html/index.en.html)
- [ECB staff projections](https://www.ecb.europa.eu/press/projections/html/all-releases.en.html)
- [ECB monetary developments](https://www.ecb.europa.eu/press/stats/md/html/ecb.md2607~e7127e7d02.en.html)
