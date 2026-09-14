# Historical economic-release schedule reconstruction

`histdatacom.market_context.release_schedules` reconstructs the calendar that
was publicly knowable at a historical decision time. It keeps that schedule
surface separate from the actual-publication surface: a later release may add
an actual timestamp and value, but it does not replace the time that market
participants had been told to expect.

The layer joins the [canonical indicator catalog](economic-indicator-catalog.md)
to immutable [release-vintage chains](release-vintage-reconstruction.md). It
does not fetch calendars or infer missing events. Source-specific adapters in
#581 must retain official calendar, archive, RSS/ICS, or publication-metadata
evidence before constructing these contracts.

## Contract topology

| Contract | Responsibility |
| --- | --- |
| `EconomicReleaseScheduleEvidenceV1` | Binds one immutable release vintage to its official source, public knowledge time, evidence hash, timezone-database version, holiday-calendar version, DST fold, and exception reason. |
| `EconomicReleaseScheduleChainV1` | Joins every vintage in one release chain to exactly one ordered schedule-evidence record and, except for an emergency, to one qualified expected occurrence. |
| `EconomicReleaseScheduleCorpusV1` | Provides a bounded, content-addressed collection tied to one exact indicator-catalog identity. |
| `EconomicReleaseScheduleStateV1` | Returns the latest schedule vintage admissible at a decision cutoff, with its ordered visible-vintage lineage. |
| `EconomicReleaseScheduleQueryV1` | Answers a bounded “what was believed scheduled in this interval?” query on `scheduled_for_ns`. |
| `EconomicReleaseScheduleAuditV1` | Counts difficult cases, compares terminal release and expected-occurrence states, and checks every before/at knowledge boundary for future leakage. |

Every identity is computed from canonical JSON. Restoration recomputes all
nested identities, so modifying a schedule, status, source, knowledge time,
timezone rule, or evidence hash fails closed. Corpus persistence is capped at
64 MiB, chain and query sizes are bounded, and duplicate logical events or
expected occurrences are rejected.

## Point-in-time query rule

For an event occurrence `e`, each retained schedule vintage is:

\[
Q_{e,k}=(t^{sched}_{e,k},\tau^{known}_{e,k},status_k,source_k).
\]

At decision time `t`, `query_economic_release_schedule_as_known()` admits only
records with `known_at_ns <= t` and selects the latest admitted record in the
chain. The interval filter is then applied to that record's
`scheduled_for_ns`. Consequently:

- an initial schedule is absent before its first public evidence;
- a reschedule, delay, or cancellation changes the answer only when its own
  evidence becomes public;
- an actual publication remains available as `released_at_ns` while the query
  continues to sort and filter on the retained scheduled time; and
- an unscheduled emergency first appears at its defensible publication or
  observation boundary and never acquires fictional advance notice.

`advance_notice_ns` is `scheduled_for_ns - known_at_ns`. A negative value after
publication is valid and makes lateness observable without mutating the
schedule surface. Cancelled events are retained by default and may be excluded
explicitly with `include_cancelled=False`.

## States and evidence policy

The underlying release contract preserves `tentative`, `scheduled`,
`rescheduled`, `delayed`, `cancelled`, `released`, `unscheduled`,
`discontinued`, `source-calendar-unavailable`, and `unresolved` states.
Tentative or date-only records still carry a normalized nanosecond anchor, but
their `time_precision` prevents that anchor from being interpreted as an exact
publication time.

Schedule transitions require matching evidence:

| Release state | Required schedule exception |
| --- | --- |
| `rescheduled` | `reschedule`, `holiday-shift`, or `dst-transition` |
| `delayed` | `producer-delay` |
| `cancelled` | `cancellation` |
| `source-calendar-unavailable` | `source-calendar-outage` |
| first `unscheduled` vintage | `unscheduled-emergency` |

A DST-transition record must retain an explicit fold (`0` or `1`). Every
record retains the IANA source timezone and offset-bearing lexical timestamp
from `EconomicReleaseTimeEvidenceV1`, plus the exact timezone-database and
holiday-calendar versions used during schedule reconstruction. A changed
scheduled time is accepted only with a rescheduled or delayed release state.

Later revisions of an unscheduled event use routine evidence: they may add or
revise the published value, but cannot invent a historical schedule notice.
Routine states cannot carry a schedule-exception claim, and exception states
require an explanation.

## Catalog and source boundaries

A scheduled chain must bind to an `EconomicExpectedOccurrenceV1` emitted by an
evidence-qualified recurrence rule in the exact catalog. The chain's economy,
family, indicator, legal-producer source, initial expected time, rule identity,
and occurrence identity must agree. Conservative bootstrap rules from #572 are
intentionally unqualified and cannot support a reconstructed schedule corpus.

Unscheduled emergencies omit the expected occurrence by design. They remain
visible in the audit as unmatched events rather than being retroactively added
to the expected-event denominator.

The catalog source key resolves through the reviewed
[official-source registry](official-source-registry-and-fetch.md). A current
calendar is not historical proof. Adapters must retain source bytes and the
earliest defensible public-knowledge evidence for each schedule vintage;
retrieval today cannot silently backdate a schedule.

## Audit and operator use

Run `audit_economic_release_schedules()` after corpus construction and before
downstream use. A passing audit means:

- every release vintage has one aligned schedule-evidence record;
- every retained exception is internally consistent with its release state;
- each event exposes exactly the expected predecessor immediately before a
  knowledge boundary and the new vintage exactly at that boundary; and
- the terminal expected-occurrence state agrees with the reconstructed release
  state.

The audit also lists rescheduled, cancelled, and unscheduled logical-event keys
and counts all exception kinds. It verifies internal replay integrity, not
archive completeness. Corpus `complete`, limitations, adapter coverage, raw
source manifests, and the structural coverage audit remain separate evidence
and must be reviewed together.
