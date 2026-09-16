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
`ecb-archive-manifest:sha256:4f24e5843b0f705fd1a717a52c11f0a1829d837d3771077d4430e4a1c162e87f`.

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
`ecb-account-archive-manifest:sha256:c43b486b6d38f25a36033e8c936819b3770959163d0a96c66ea87124fcb6c6e8`.

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
`ecb-statement-archive-manifest:sha256:b9eb22f74ce5fc54a5ca2be45261e3e06a07a8db87adf75593fd48a978bdd372`.
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

## Remaining issue scope

These ECB decision, account, and statement archives complete three
monetary-policy artifact families, not issue #539 as a whole. Eurostat
aggregate releases and the ECB projection/statistical families, Destatis and
Bundesbank programs for Germany, and INSEE and Banque de France programs for
France still require their own empirical archive qualifications before the
regional calendar can produce a closure receipt.

Official entrypoints:

- [ECB monetary-policy decisions](https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html)
- [ECB public FOEDB version pointer](https://www.ecb.europa.eu/foedb/dbs/foedb/publications.en/versions.json)
- [Eurostat release calendar](https://ec.europa.eu/eurostat/news/release-calendar)
- [ECB monetary-policy accounts](https://www.ecb.europa.eu/press/accounts/html/index.en.html)
- [ECB monetary-policy statements](https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/html/index.en.html)
- [ECB staff projections](https://www.ecb.europa.eu/press/projections/html/all-releases.en.html)
