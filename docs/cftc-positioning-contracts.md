# CFTC positioning-state contracts

The CFTC positioning domain supplies immutable weekly Commitments of Traders
(COT) state to reconstruction. It is deliberately separate from
`MarketContextEventV1`: a Tuesday measurement is persistent futures positioning
published later, not an instantaneous spot-FX event.

The durable source of truth is a bounded latest-known-state sidecar. COT values
are never repeated onto every tick row, interpolated into invented observations,
or treated as spot volume, sentiment truth, or a causal shock label.

## Versioned contract boundary

| Contract | Responsibility |
| --- | --- |
| `CftcPositioningFetchProfileV1` | Date/code selection plus page, source-byte, row, runtime, memory, timeout, and staleness limits. |
| `CftcPositioningRawSourceV1` | Query parameters, deterministic ordering, retrieval time, dataset/family/scope identity, adapter version, response hash, size, URI, redistribution policy, and limitations. |
| `CftcPositioningSymbolMappingV1` | Versioned contract codes, direct/two-leg status, quote direction, support start, official CFTC metadata, and CME citation URIs. |
| `CftcReleaseEvidenceV1` | Date-only report measurement, publication time, knowledge time, confidence, restatement detection, notes, and evidence source. |
| `CftcPositioningSnapshotV1` | One immutable family/scope/contract/report-date position vector and source-row hash. |
| `CftcPositioningCorpusV1` | Sources, mappings, snapshots, coverage, compressed-history consistency, limits, and deterministic identity. |
| `CftcPositioningDiffV1` | Bounded added, removed, and content-changed logical keys between immutable refreshes. |
| `CftcPositioningQueryV1` | Latest eligible state, mapping kind, snapshot IDs, age, refusal status, and point-in-time-derived values for one window. |
| `CftcPositioningConsumerBindingV1` | Companion lineage receipt for benchmark, motif selection, planning, or carving without changing their immutable v1 schemas. |

All contracts use `histdatacom.cftc-positioning-*.v1` schema identifiers. The
feature is a v2.1.0 minor addition; existing market-context v1 contracts remain
readable and are not extended with incompatible fields.

## Official sources and reuse

Production acquisition selects these official CFTC resources:

- PRE Legacy raw dataset [`srt6-5q2f`](https://publicreporting.cftc.gov/resource/srt6-5q2f.json);
- PRE Traders in Financial Futures (TFF) raw dataset [`udgc-27he`](https://publicreporting.cftc.gov/resource/udgc-27he.json);
- the CFTC [release schedule](https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm);
- [historical special announcements](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm);
- the [historical compressed-file index](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm);
- CFTC release [9147-25](https://www.cftc.gov/PressRoom/PressReleases/9147-25), which records the 2025 shutdown backlog's actual publication dates; and
- the CFTC [web/reuse policy](https://www.cftc.gov/WebPolicy/index.htm).

The retained consolidated consistency archives are
`deacot1986_2016.zip`, `deahistfo_1995_2016.zip`,
`fin_fut_txt_2006_2016.zip`, and `fin_com_txt_2006_2016.zip`. They are
current corrected history, not original-vintage proof.

CFTC states that United States government information on its site is public
domain, while asking users to acknowledge the CFTC as the source. Corpus users
should use an acknowledgement such as “Source: U.S. Commodity Futures Trading
Commission.” Third-party marks and linked content are not covered by that
statement.

Quote direction is recorded in immutable mapping contracts using the official
CFTC dataset metadata together with CME's
[FX quote-convention guide](https://www.cmegroup.com/education/courses/introduction-to-fx/understanding-fx-quote-conventions)
and [EUR/GBP Rule 301](https://www.cmegroup.com/rulebook/CME/III/300/301/301.pdf).
The mapping artifact retains those citation URIs, direction, codes, and notes.
The live corpus does not mirror or redistribute CME pages, and reconstruction
does not depend on CME web availability after the versioned mapping is loaded.

## Report families, scopes, and fields

Legacy and TFF are separate classification schemas. A TFF participant category
must not be interpreted as a Legacy category, back-cast before TFF support, or
pooled with Legacy. Futures-only and futures-plus-options-combined reports are
also separate scopes; combining both would double-count overlapping state.

The logical duplicate key is exactly:

```text
(report_family, report_scope, cftc_contract_market_code, report_date)
```

PRE `id` is retained as evidence but is not trusted as a cross-dataset primary
key. Identical duplicate logical rows are counted; contradictory rows fail the
build.

Common source fields include `report_date_as_yyyy_mm_dd`,
`cftc_contract_market_code`, `futonly_or_combined`,
`contract_market_name`, `market_and_exchange_names`, and
`open_interest_all`. Numeric position, percentage-of-open-interest, change,
trader-count, and concentration fields are retained under their normalized PRE
names. Legacy examples include `noncomm_positions_long_all` and
`noncomm_positions_short_all`; TFF examples include
`lev_money_positions_long_all` and `lev_money_positions_short_all`.

## Symbol and direction mapping

| Window state | CFTC code(s) | Mapping | Direction / interpretation |
| --- | --- | --- | --- |
| EURUSD | `099741` | direct | USD per EUR; EUR FX futures positioning. |
| GBPUSD | `096742` | direct | USD per GBP; British Pound futures positioning. |
| EURGBP before 2014-06-10 | `099741` + `096742` | two-leg | Retain both EURUSD and GBPUSD leg snapshot IDs; do not label or pool them as direct EURGBP COT. |
| EURGBP from 2014-06-10 | `299741` | direct | GBP per EUR; direct EUR/GBP contract state. |

The pre-2014 two-leg record is identity and conditioning evidence only. It does
not subtract heterogeneous participant totals into a fictitious direct
contract.

## Measurement, publication, knowledge, and restatement time

`report_date` is the CFTC measurement date and remains date-only.
`measurement_start_ns` is a deterministic midnight-UTC boundary used only for
ordering and age calculations; it is not claimed as an observed market
timestamp. `publication_at_ns` records a publication time when evidence
supports one, `knowledge_at_ns` records when that publication could be used,
and `valid_from_ns` is derived from that knowledge time. A separate
`restatement_detected_at_ns` preserves correction discovery.

Availability confidence is one of `verified`, `nominal`, `unknown`,
`correction_qualified`, or `restatement_qualified`. The ordinary Friday 15:30
America/New_York rule is only nominal, including DST conversion, and is never
strict-ex-ante eligible. The 2025 backlog dates are verified from CFTC release
9147-25. Special-announcement fixtures cover July 2015 holiday/premature
publication cases and the documented 2010 and 2018 corrections.

PRE and compressed history expose current corrected state. They do not retain
every original published value. A row therefore remains `current_state_only`
or `restated_current_state` unless an original vintage is separately verified.
A known release time cannot make a current corrected value strict-ex-ante
eligible; the query returns `restatement_incomplete`.

## Acquisition, immutable artifacts, and crash recovery

```bash
histdatacom analytics cftc-positioning-corpus \
  --artifact-dir data/.histdatacom/analytics/cftc-positioning \
  --start-date 2002-03-01 \
  --end-date 2026-06-30
```

PRE requests record the exact `$where`, `$order`, `$limit`, and `$offset`
parameters. Ordering is report date, contract code, report scope, and PRE ID.
Pagination is contiguous and bounded. Every response records retrieval time,
resolved URI, dataset/family identity, adapter version
`cftc-pre-positioning-adapter-v1`, content type, bytes, SHA-256, reuse policy,
and limitations.

The writer creates content-addressed raw sources plus corpus, coverage, and
archive-consistency JSON. Writes use a flushed temporary file followed by an
atomic rename, so a crash cannot leave a partial final-named artifact.
`read_cftc_positioning_corpus()` verifies filename and content hashes.
`replay_cftc_positioning_corpus()` rebuilds from retained responses and refuses
an identity change.

A refresh may pass `--previous-corpus`. It never overwrites the prior corpus;
it writes a `CftcPositioningDiffV1` with bounded added, removed, and changed
logical keys plus both snapshot IDs. Retrieval-time changes alone do not become
false row restatements because row comparison uses canonical source-row hashes.

Default limits are 100,000 selected rows, 128 PRE pages per dataset, 128 MiB
per response, 256 MiB total configured source bytes, 600 seconds, and 2 GiB
peak resident memory. ZIP consistency checks validate expansion size and stream
CSV rows rather than decoding whole archive members.

## Coverage and consistency evidence

Coverage is partitioned by year, family, scope, and contract. Each slice
records rows, first/last report date, missing weekly intervals, duplicate keys,
contract and market names, availability-confidence counts, restatement counts,
source hashes, source bytes, and processing time. It does not infer that a
missing week means “neutral positioning.”

Compressed-history evidence reports selected rows, PRE matches, PRE
missingness, open-interest mismatches, and contract/name changes for every
family/scope archive. Equality proves current-state consistency only; it does
not reconstruct the original publication vintage.

## Query, derived values, and refusal semantics

`query_cftc_positioning_corpus()` selects the latest eligible report at or
before the reconstruction window. It returns only bounded snapshots and keeps
family/scope/code identities. It reports per-snapshot age and one of `ready`,
`missing`, `stale`, `not_available_as_of`, `unsupported`, or
`restatement_incomplete`.

Derived values include participant net, net/open-interest, change from the
prior eligible family/scope/contract snapshot, and a trailing 52-report
standard score. History is cut off at the selected report, and no value pools
families, scopes, or the two EURGBP legs. Nominal or unknown publication
history is excluded from strict ex-ante selection.

`preflight_cftc_positioning_corpus()` returns structured refusal evidence.
`require_cftc_positioning_corpus()` raises
`CftcPositioningPreflightError`; it never substitutes a neutral state.

## Information audit and consumers

`cftc_positioning_information_inputs()` emits one
`ReconstructionInformationInputV1` per selected snapshot. Verified original
vintages use point-in-time scope. Current corrected state is revision-scoped;
when it is learned after the reconstruction time in an ex-post run, it is
explicitly labeled `full_period_summary` with bounded allowed lookahead. The
existing #433 leakage audit therefore sees the same temporal nonclaim as the
query.

`CftcPositioningConsumerBindingV1` retains corpus, query, snapshot, information
input, run, window, and consumer artifact IDs. Installed helpers:

- project a compact state label into benchmark `event_state`;
- add the state label to motif event tags while keeping positioning metrics in
  the companion receipt because motif v1 has a strict metric allowlist;
- validate run/window/artifact continuity for planning and carving; and
- write binding and held-out benchmark-smoke artifacts.

No custom notebook or permanent tick-row augmentation is required.

## Real closure campaign

The #468 campaign covered 2002-03-01 through 2026-06-30. It retained 24
official responses totaling 92,920,837 bytes and produced 11,324 distinct
snapshots in 232 year/family/scope/contract coverage slices with zero duplicate
logical keys. The final refresh ran in 87.581906 seconds and recorded a
414,351,360-byte peak resident-memory measurement after the archive parser was
changed from whole-member decoding to streaming.

Coverage includes 1,270 Legacy reports per scope for EUR `099741` and GBP
`096742` from 2002-03-05, 1,047 TFF reports per scope from its supported
2006-06-13 start, and 514 direct EURGBP `299741` reports per family/scope from
2014-06-10. The four consolidated archives supplied 4,060 overlapping rows;
all 4,060 matched PRE and none had an open-interest mismatch.

The final logical corpus ID is
`cftc-positioning-corpus-277495b704e33d13a1340b19e3b0f7f80dbeef7970281c77b626a75945a96414`;
its self-contained artifact SHA-256 is
`887a47840090cdab1982fe910a4bdf8c1fcc9af256ab687bceae1b8dd1cbd3e0`.
An independent raw-source replay of the preceding same-schema refresh
reproduced its exact 11,324-snapshot corpus ID. The next immutable live refresh
reported zero added, zero removed, and zero changed logical rows while
retaining the distinct retrieval-evidence corpus IDs in diff
`cftc-positioning-diff-ee030e5f2b73bd2e56ab5267a12eefc3c5098bcbaadcc3d46b3b0d8bd89fbccb`.

The real held-out smoke consumed the first 4,096 EURUSD events from the local
December 2024 HistData Arrow cache (source SHA-256
`68f9938c2e302ffada2add14393f8ba072da07c5addceda0f1e600d076d7a954`).
Its window selected the four 2024-11-26 Legacy/TFF and
futures-only/combined snapshots. Artifact reload produced the identical output
SHA-256
`82fd59e316354e33468ef1a66a929601084d74b2eec569777af62b589f080f5b`.
The smoke ID is
`cftc-positioning-smoke-7ee89ee3b63ac9883f064dbf4de4c8d38ec4183022ee5ec32eaa256f1b49d8c0`.

The real corpus separately returns `restatement_incomplete` for strict ex-ante
use of current PRE state even when a delayed publication date is verified, and
preflight returns an explicit unsupported refusal for a non-triangle symbol.

## Explicit limitations and nonclaims

- COT is futures positioning/open interest, not decentralized spot-FX volume.
- Participant categories are not individual-trader identities or sentiment
  truth.
- Position changes are not causal shock labels.
- Weekly values are not interpolated into invented observations.
- Absence, stale state, or unsupported history is not a neutral position.
- No automatic positioning winner, strategy, or trading recommendation is
  produced.
