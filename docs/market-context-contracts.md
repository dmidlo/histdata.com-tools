# Point-in-time market-context contracts

The market-context domain supplies immutable macro, central-bank, news, and
calendar evidence to reconstruction without adding repeated context columns to
every tick. A versioned timeline is durable; each bounded reconstruction window
receives a compact query sidecar.

The contracts do not authorize or scrape a paid corpus. An operator must select
and license a source separately. Source adapters normalize approved evidence
into the common interface while retaining the original provenance and
redistribution policy.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `MarketContextSourceV1` | Source/version, retrieval time, content hash, adapter identity, license, redistribution constraints, limitations, and bounded metadata. |
| `MarketContextEventV1` | One immutable schedule, release, revision, decision, communication, shock, or news vintage. |
| `MarketContextTimelineV1` | Ordered vintages, revision chains, declared coverage, completeness, limitations, and deterministic artifact identity. |
| `MarketContextCalendarStateV1` | Compact reuse of the existing HistData session, closure, rollover, holiday, and period-end classifier. |
| `MarketContextQueryV1` | Bounded ex-post or ex-ante sidecar for one half-open interval or reconstruction window. |
| `MarketContextSourceAdapterV1` | Shared normalization seam for operator-approved sources. |

Every external event embeds its complete source contract. The record therefore
retains retrieval/version metadata, a content SHA-256 digest, license and
redistribution policy, source URI where appropriate, adapter version, affected
currencies/symbols, confidence, limitations, and source-time normalization.
Raw source text need not be redistributed. A licensed adapter may store only a
content hash and operator-approved normalized values.

## Event identity and revisions

`canonical_key` identifies the economic or news event across vintages. It is
normalized to a stable lowercase identifier. `event_id` hashes the complete
event vintage, including source provenance and values.

An initial record has `revision_sequence=0`. Each later vintage must:

- name the immediately preceding `event_id`;
- retain the canonical key, semantic market-event time, and first-known time;
- increment the sequence by exactly one; and
- have a strictly later availability timestamp.

The timeline retains both the first release and every revision. It rejects
duplicate IDs, duplicate logical vintages, orphan revisions, sequence gaps,
and revisions that rewrite the original event identity. A revision never
updates an earlier object in place.

Scheduled macro records may carry expected, actual, previous, revised-previous,
and surprise values with explicit units. When expected and actual are both
present, surprise is deterministically `actual - expected`; contradictory
values fail validation.

## Knowledge time versus market-event time

Three times have distinct meanings:

- `event_time_ns` is when the target market event occurs;
- `first_known_at_ns` is when the existence or schedule was first known; and
- `available_at_ns` is when this exact vintage became usable.

An ex-ante query requires `as_of_ns` and excludes any vintage whose first-known
or availability time is later. Ex-post queries expose all matching vintages.
This lets a schedule be visible before a release without exposing the actual or
a later revision.

When query events are added to `ReconstructionInformationInputV1`, the
information graph uses the vintage's availability time as its semantic input
event. The target release time remains in `MarketContextEventV1`. This prevents
a known schedule from being mislabeled as realized future information while
the existing #433 audit still rejects an actual or revision used before it was
available.

## Timezone and event-window semantics

Source event timestamps are ISO-8601 strings paired with an IANA timezone.
Normalization verifies that an explicit offset agrees with the named zone.
Naive local times are localized with `zoneinfo`:

- ambiguous daylight-saving folds require an explicit `source_time_fold`;
- nonexistent wall-clock times fail closed; and
- the normalized value must equal the stored UTC nanosecond timestamp.

Each record declares `pre_event_ns` and `post_event_ns`. Its conditioned window
is exactly:

```text
[event_time_ns - pre_event_ns, event_time_ns + post_event_ns)
```

This half-open rule makes boundary behavior deterministic across streaming
windows.

Unscheduled shocks and news windows cannot claim exact precision. They require
`approximate` or `window_only` precision, a confidence value, an explicit
ambiguity reason, and limitations.

## Bounded streaming joins

`query_market_context_window()` consumes `ReconstructionWindowV1` and returns a
bounded `MarketContextQueryV1`. It filters by the synchronized window's symbols,
optional currencies and event kinds, point-in-time availability, and
conditioned-window overlap. The result contains event contracts, a compact
calendar classification at the window boundary, the exact timeline/window IDs,
and limitations.

It never emits a dataframe or repeats context over market rows. The default
limit is 512 events per query and 4,096 events per timeline. Exceeding a query
limit raises `MarketContextQueryLimitError` rather than silently truncating
evidence. Large source bodies and analytical enrichment remain outside the
query payload.

## Calendar context

`market_context_calendar_state()` reuses
`classify_histdata_timestamp()` and `calendar_policy_metadata()` from the data
quality domain. It carries session state, clock and active sessions, overlaps,
rollover/open/close/fix tags, holiday/event tags, month/quarter/year-end tags,
calendar profile source/version, completeness, and limitations.

The default holiday profile remains explicitly incomplete and advisory. A
calendar state is a deterministic classification, not evidence that a news or
macro event occurred.

## Explicit missing context

An empty query is never converted into a neutral or invented event label. Its
status is `missing` with one stable reason:

- `no_matching_event` inside complete declared coverage;
- `not_available_as_of` when matching evidence exists but was not yet known;
- `outside_timeline_coverage`; or
- `timeline_incomplete` when an approved source is absent or incomplete.

Calendar state may still be available alongside missing external context. The
two facts remain separate.

## Adapter and trust gates

`MarketContextSourceAdapterV1` exposes only an adapter name/version and
`load_events()`. Collection verifies that every emitted event names that exact
adapter identity. `StaticMarketContextSourceAdapterV1` supplies a deterministic
fixture and normalized-import implementation without implying a production
news source.

Context may condition activity, spread, timing, motif selection, and carving
only when its timeline and query IDs are preserved in the information graph.
It may not be added retrospectively as narrative decoration, treated as
point-in-time evidence before availability, or used to replace missing context
with an invented classification.

Changing required fields, identity rules, revision semantics, query visibility,
or timezone behavior requires a new schema version and contract class.
