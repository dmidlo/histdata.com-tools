# Economic release-time and vintage reconstruction

`histdatacom.market_context.release_vintages` turns retained official archive
observations into immutable `EconomicCalendarReleaseV1` chains. It is the
temporal reconstruction layer between source-specific parsing and the
provider-neutral `as_known_at(t)` query surface.

The layer does not fetch sources, scrape archives, or claim missing history.
Source discovery and bounded fetch/replay are governed by the
[official-source registry](official-source-registry-and-fetch.md). Historical
schedule state is reconstructed by the companion
[schedule-reconstruction layer](release-schedule-reconstruction.md), while
the [archive-first reconstruction layer](archive-vintage-reconstruction.md)
binds source-specific artifacts and parsed records to these chains. This
module supplies the shared release-time and immutable-mutation rules those
adapters must use.
Expected concepts and occurrences are governed separately by the
[canonical indicator catalog](economic-indicator-catalog.md), so releases
found in an archive cannot silently define their own coverage denominator.

## Contracts

| Contract | Purpose |
| --- | --- |
| `EconomicReleaseTimeEvidenceV1` | Retains schedule, official publication, first observation, source update, admission time, bounds, timezone/DST evidence, and precision separately. |
| `EconomicReleaseVintageMutationV1` | Describes one immutable status, schedule, publication, or value transition and binds it to its exact predecessor and source. |
| `EconomicReleaseVintageChainV1` | Replays ordered mutations into an exact release chain and rejects any retained result that differs from reconstruction. |
| `EconomicReleaseVintageAuditV1` | Reports coverage and verifies chain replay, point-in-time visibility, previous-as-known, and simultaneous previous revisions. |

All four contracts are versioned and content-addressed. JSON restoration
recomputes nested identities and fails on modified evidence. A mutation must
name the exact predecessor `release_id`, logical event, and derived
`series_id`; its admission time must move strictly forward.

`build_economic_calendar_corpus_from_vintage_chains()` admits only replayed
chains, rejects duplicate logical-event chains, and delegates cross-event
semantic and lineage validation to `EconomicCalendarCorpusV1`.

## Release-time hierarchy

The selected `available_at_ns` is the earliest defensible time at which a
strategy could have used that exact vintage. It is never silently copied from
the archival fetch time.

| Basis | Required evidence | Admission time |
| --- | --- | --- |
| `official-publication` | Official publication timestamp with exact-second or exact-minute precision | Official publication timestamp |
| `source-update-upper-bound` | Retained source page/file update timestamp | Source update timestamp |
| `first-observed` | First successful observation | First-observed timestamp |
| `inferred-bounded` | Explicit lower and upper publication bounds with date-only or inferred-bounded precision | Conservative upper bound |

The schedule, actual publication, first-observed fetch, source update, and
source retrieval remain distinct fields. An archive fetched today may prove
an official publication time years ago: the release becomes historically
available at that proved publication time while its source still records
today's retrieval and first-observed times. Conversely, latest-state data
without archive evidence is never backdated.

All lexical schedule, publication, and update timestamps retain an explicit
UTC offset, normalize exactly to their nanosecond values, and must agree with
the retained IANA timezone. `date-only` and `inferred-bounded` evidence cannot
masquerade as an exact official publication time.

`select_release_time_evidence()` resolves duplicate mirrors deterministically:
official publication evidence outranks a source-update upper bound, which
outranks first observation, which outranks bounded inference. Precision,
earliest defensible availability, observation time, and evidence identity
break ties. Candidates that disagree on the scheduled occurrence are not
silently merged.

## Reconstruction algorithm

For one logical event:

1. Normalize the earliest retained observation as revision sequence zero and
   bind it to its `EconomicReleaseTimeEvidenceV1`.
2. Order later observations by their defensible availability time.
3. Convert each observed change into an
   `EconomicReleaseVintageMutationV1`, naming the exact predecessor.
4. Apply each mutation with
   `apply_economic_release_vintage_mutation()`. Semantic series/event fields
   are inherited, the sequence advances by one, and the old release remains
   immutable.
5. Build the chain and corpus, then run
   `audit_economic_release_vintages()` before use.

The chain can contain tentative schedules, reschedules, delays,
cancellations, publication, and later value revisions. Schedule changes and
cancellations require a reason. Released and unscheduled records require both
numeric and raw lexical values plus publication-time evidence. A revision
requires an explicit kind: `routine`, `correction`, `seasonal-adjustment`,
`simultaneous-previous`, or `benchmark`.

## Initial actual, revision deltas, and previous as known

The first published non-revision value is the initial actual `A_e`. Later
values remain separate vintages. Query state exposes each delta from that
first value:

`R_(e,k) = A_(e,k)^rev - A_e^initial`.

The immediately preceding reference period is selected within the same
versioned semantic series. For release `e`, the query chooses the last prior
vintage whose availability is strictly before the first actual publication
time:

`P_e = max_k { V_(r-1,k) : available_(r-1,k) < published_e }`.

The cutoff is fixed to the event's first actual publication. A later revision
of `e` cannot move it. A revised previous value published simultaneously with
the new actual is therefore excluded from `P_e` forever, though it can appear
later as the explicitly ex-post `previous_latest` diagnostic.

Before every mutation's availability, chain replay returns its predecessor;
at availability, it returns the new immutable vintage. This property prevents
a current revised database value from leaking into an earlier decision time.

## Difficult-case policy

- **Reschedules, delays, and cancellations:** later vintages of the same
  logical event, with a required reason; old schedule evidence remains.
- **Unscheduled decisions:** status is `unscheduled`, while actual publication
  and observation evidence remain mandatory. A placeholder schedule time may
  equal the actual occurrence but does not imply advance notice.
- **Embargoes and press conferences:** admission follows the proven public
  boundary, not an embargoed producer timestamp. A press conference or later
  publication phase is a distinct logical event rather than a value revision.
- **Simultaneous previous revisions:** the prior-period mutation names the
  triggering newer event and affected period. The audit requires equal
  publication times and a later trigger reference period.
- **Benchmark revisions:** every affected period is retained; at least two are
  required. Each changed logical event receives its own mutation, with a
  shared source/batch identity supplied by the adapter.
- **Seasonal-adjustment revisions:** explicit revision kind; they remain in the
  same chain only while the versioned series semantics stay comparable.
- **Rebases, methodology changes, and source table/series migrations:** start a
  new `series_version` and chain. The mutation function rejects a changed
  `series_id`. An optional separately governed comparability bridge may link
  eras without declaring them identical.
- **Renames and split/merged products:** a presentation-only title change can
  be mapped by an adapter, but changed statistical meaning or product topology
  requires a new canonical series/event identity.
- **Duplicate official mirrors:** retain every raw source, select time evidence
  by the explicit hierarchy, and record conflicting occurrence times instead
  of merging them.

## Audit interpretation

The audit counts every availability basis and revision kind, verifies exact
chain/corpus agreement, checks visibility immediately before and at each
admission boundary, and checks previous-as-known results for bounded corpora.
It also verifies the trigger relationship for simultaneous previous
revisions.

`missing_initial_actual_event_keys` is diagnostic, not synthetic repair. A
schedule-only or cancelled event can pass structural integrity while still
reporting that no retained initial actual exists. Downstream coverage policy
decides whether that explicit gap is admissible.

The audit does not prove that an upstream archive is complete or historically
truthful. That claim requires source-specific acquisition evidence, retained
raw bytes, archive coverage declarations and diagnostics, and replay through
the [archive-first reconstruction layer](archive-vintage-reconstruction.md).
