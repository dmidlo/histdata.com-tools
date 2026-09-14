# Canonical economic-indicator and expected-occurrence catalog

`histdatacom.market_context.indicator_catalog` defines what official economic
events should exist independently from the releases that happen to have been
found. This separation prevents a latest-state API, a vendor checklist, or an
empty archive search from deciding the historical event universe.

The catalog is the #572 bridge between the provider-neutral calendar contracts
in #534, the legal-producer registry in #535, and the release-vintage engine in
#537. Event-family specifications #574–#578 replace conservative family cells
with exact indicator definitions. Schedule reconstruction #579 emits their
expected occurrences; archive recovery #581 supplies reconstructed releases.

## Contract topology

| Contract | Responsibility |
| --- | --- |
| `EconomicIndicatorMethodologyEraV1` | Effective dates, exact source series/release/table identifiers, unit/scale, seasonal basis, reference-period convention, official evidence, and break notes. |
| `EconomicExpectedReleaseRuleV1` | Frequency, effective dates, official calendar owner, timezone, time precision, recurrence semantics, qualification evidence, and explicit limitations. |
| `EconomicIndicatorCatalogEntryV1` | Economy/currency, family, canonical identity, aliases, legal producer, frequency, reference convention, normal schedule, stages, measurement basis, methodology eras, schedule rules, and versioned structural importance prior. |
| `EconomicIndicatorLineageV1` | Evidence-backed rename, successor, rebase, methodology-break, split, or merge edge without rewriting either endpoint. |
| `EconomicExpectedOccurrenceV1` | Exact indicator/rule binding, reference period, expected time, as-known status time, official source occurrence ID, source hash, and absence reason. |
| `EconomicIndicatorCatalogV1` | Complete economy/family matrix, deterministic identities, lineage validation, and frozen source-registry identity. |
| `EconomicIndicatorCoverageSliceV1` | Qualified denominator, reconstructed intersection, explicit gaps, status counts, and structural coverage for one indicator. |
| `EconomicIndicatorCoverageAuditV1` | Bounded collection of coverage slices, unmatched official releases, and queryable unexplained gaps. |

Every contract is immutable. Nested JSON restoration recomputes content
identities and rejects modified fields. Source series, table, release, and
occurrence identifiers retain their official spelling; canonical internal keys
are normalized separately.

## Complete bootstrap matrix

`build_official_indicator_catalog()` deterministically projects the reviewed
official-source registry into 252 conservative cells: 21 scoped economies by
12 event families. Each cell records the reviewed legal producer, affected
currency, provider-neutral family identity, initial structural importance, and
an explicit family-level methodology/rule placeholder.

The bootstrap proves only matrix coverage and ownership. It deliberately does
not claim that a broad family such as “inflation prices” is one exact series or
that it recurs monthly throughout history. Its recurrence rule is unqualified,
so its structural coverage is unavailable. Regional and family adapters must
replace these placeholders with exact concepts and official evidence.

The catalog validator requires at least one indicator in every declared
economy/family cell. `validate_indicator_catalog_sources()` then checks the
frozen `registry_id`, legal-producer status, economy/family remit, and every
rule’s source binding. A registry change therefore produces a new catalog
identity rather than silently changing ownership.

## Exact indicator identity

An exact catalog entry keeps these dimensions together:

- economy and affected currency;
- canonical family and indicator key;
- common display name and aliases;
- legal producer plus exact series, table, and release identifiers;
- frequency and reference-period convention;
- normal schedule rule and publication stages;
- unit, scale, and seasonal basis;
- validity interval and non-overlapping methodology eras;
- non-overlapping, versioned expected-release rules; and
- structural importance prior, policy version, rationale, and limitations.

Rebases and methodology breaks do not mutate an old entry. A successor entry
and directed lineage edge retain the transition evidence. Catalog construction
rejects unknown endpoints, cross-economy or cross-family edges, unrelated
producers, duplicate edges, and cycles. Split and merge relationships use
multiple directed edges rather than comma-separated identifiers.

The structural importance integer is a versioned family prior from zero to
three. It is not copied from a professional calendar and is not evidence of
realized market impact. Retrospective response estimates belong to #549; an
ex-ante consumer may use only a catalog version frozen before its cutoff.

## Expected occurrence states

An exact recurrence rule may emit occurrences only after its schedule model is
qualified from an official calendar or archive. Each occurrence has one state:

- `scheduled`;
- `released`;
- `rescheduled`;
- `cancelled`;
- `discontinued`;
- `source-unavailable`; or
- `unresolved`.

Cancellation, discontinuation, source unavailability, and unresolved states
require an explicit reason. The occurrence is bound to the exact rule identity
and must fall inside that rule’s effective interval. A content hash and source
occurrence identifier preserve the evidence. Unscheduled or emergency releases
are not retroactively invented as expected events; when real, they appear as
unmatched releases for investigation and can later be incorporated under the
schedule semantics in #579.

## Structural coverage

For a qualified indicator `j` and bounded era, let `E_j` contain releasable
expected occurrences and let `R_j` contain reconstructed official releases
whose logical event and catalog identity match. The audit reports

\[
C_j=\frac{|R_j\cap E_j|}{|E_j|}.
\]

Cancelled and discontinued occurrences remain in state counts but are excluded
from the releasable denominator: a complete reconstruction should not be
penalized for an event the producer explicitly did not publish. Scheduled,
rescheduled, released, source-unavailable, and unresolved occurrences remain in
the denominator. Missing logical-event keys are directly queryable.

If the expected model is unqualified, or if the bounded interval contains no
releasable expected occurrences, `structural_coverage` is `None`. The engine
never substitutes zero or one because either number would overstate knowledge
of `E_j`. A release inside the interval without a matching expected occurrence
is retained in `unmatched_release_ids`, not discarded.

## Persistence and downstream rules

`write_economic_indicator_catalog()` writes canonical JSON under a 64 MiB
bound; `read_economic_indicator_catalog()` restores and verifies every nested
identity. Coverage accepts only `EconomicCalendarReleaseV1` records whose
series key, economy, family, and indicator identity match the expected catalog
entry. Duplicate occurrence keys, rules outside their effective interval, and
identity drift fail closed.

Professional calendars may be used only as non-authoritative coverage
checklists. They cannot provide legal producer identity, exact official series
semantics, historical existence, schedule knowledge time, or a historical
initial value.
