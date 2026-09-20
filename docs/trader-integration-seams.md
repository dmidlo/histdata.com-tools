# Provider-neutral trader integration seams

Issue #666 freezes the five input boundaries below on `dev`. It does not build
the production trader engine, the 1,000-strategy catalog, or the shared durable
`TraderFeatureSnapshotV1` planned in #660. These seams are independently usable
with synthetic fixtures; their existence does not qualify historical inputs,
provide a broker connection, or authorize an empirical campaign.

## One canonical owner per input

Public imports live under `histdatacom.synthetic.traders`. This small package
has its own `py.typed`; no new typing claim is made for an entire legacy
namespace. It introduces no persistent artifact format or second source
registry. New requests, results, protocols and locator adapters are
process-local. Native returned contracts keep their existing identity and
serialization owners.

| Input | Trader-facing boundary | Canonical implementation and wire owner |
| --- | --- | --- |
| Cross-currency events | `TraderTriangleReaderV1.query()` and `replay()` | `CommittedTraderTriangleReaderV1` replays `synthetic.persistence`; synchronization and native tuple artifacts remain in `synthetic.triangle_bar_features`, with the join enum from `synthetic.cross_currency`. |
| Positioning | `TraderPositioningReaderV1.as_known_at()` | `CftcTraderPositioningReaderV1` calls `market_context.positioning.query_cftc_positioning_corpus`; native corpus/query/mapping contracts are unchanged. |
| Calendar and events | `EconomicCalendarAsKnownReaderV1.as_known_at()` | The **same protocol object**, not a wrapper or duplicate, from `market_context.economic_calendar`; `EconomicCalendarCorpusV1` directly implements it. |
| Dataset registration and lineage | `TraderDatasetReaderV1.register/resolve/replay/verify/lineage` | `CatalogTraderDatasetReaderV1` composes `datasets.catalog.DatasetCatalog`; descriptors, versions, parents, receipts and qualification evidence remain `datasets` contracts. |
| Bars and activity | `TraderBarActivityReaderV1.snapshot()` | Existing `synthetic.bar_features.BarFeatureSourceV1` directly implements it; bars/activity, feature policy, availability and missingness remain canonical native contracts. |

Acquisition and raw-source replay remain the relevant adapter's responsibility.
In particular, injecting an already normalized calendar/CFTC corpus does not
newly prove its upstream bytes, completeness or original publication time.
Use the owner's readers and replay functions when restoring those inputs.

## Explicit event queries, not OHLC proxies

```python
from histdatacom.synthetic.traders import (
    CommittedTraderTriangleReaderV1,
    TraderTriangleReaderV1,
    TraderTriangleRequestV1,
)

reader: TraderTriangleReaderV1 = CommittedTraderTriangleReaderV1(manifest_path)
request = TraderTriangleRequestV1(
    start_ns=start_ns,
    end_ns=end_ns,
    decision_at_ns=decision_at_ns,
    policy_known_at_ns=policy_known_at_ns,
    ensemble_member_id=member_id,
    information_mode=information_mode,
)
result = reader.query(request, availability=event_availability)
reader.replay(result)  # reopens the current publication, not a cached receipt
```

The query is an arbitrary half-open interval `[start_ns, end_ns)`, not a bar
width or inferred OHLC endpoint. It must be closed at the decision cutoff.
All clocks are explicit nonnegative integer nanoseconds in the signed int64
domain, and the fixed join policy must be known by that cutoff. The exact
ensemble member and the three-symbol universe EURGBP/EURUSD/GBPUSD are bound.
The adapter verifies the complete committed publication before selecting its
interval; no caller-supplied source ID substitutes for that replay.

Synchronization calls the existing native helper on admitted events:

- Exact event-time alignment sorts each admitted leg by native time, sequence
  and event ID, pairing same-time ordinal positions up to minimum support.
- Bounded-prior alignment uses the explicitly selected fixed probe symbol
  (EURUSD by default), chooses the latest event at or before each probe, and
  admits age **equal to** the positive fixed maximum. It never selects a future
  quote or carries a quote from before the requested interval.
- Both preserve native event, tuple, run, member, origin and source-version
  identities. The bounded-prior fixed-probe policy is the existing #651 policy,
  not a claim of equivalence to every other event-probe policy or of #515
  empirical alignment qualification.

Each returned tuple retains its exact three native quotes and ages. Its
`endpoint_ns` records the query end; it does not assert that a quote was
observed at the endpoint. Independent OHLC opens/closes can be nonsimultaneous
and are not replaced by these tuples. Bid/ask sides remain separate; this
interface computes neither executable profit nor transaction-cost/liquidity
qualification. The canonical direct cross is EURGBP = EURUSD / GBPUSD.

Every admitted quote needs `TraderQuoteAvailabilityV1`: exact event ID,
availability time, declaration-known time, basis and explanation. Generated
events additionally require a generation time no later than availability.
Unknown or future declarations do not supply values. Declarations wholly
after the cutoff are ignored before duplicate checks and are not retained in
the result, so adding them cannot alter that historical result. Source counts
and source roots describe the same immutable publication, not future knowledge.
An admitted declaration whose availability precedes the event refuses.

Observed, synthetic and merged scopes remain distinct. Ex-post reconstruction
supports all three. Legacy generated publications lack a persisted ex-ante
information audit, so synthetic/merged ex-ante requests refuse **before I/O**.
Observed ex-ante requests require declared source clocks and refuse replay-clock
assumptions. Even their successful result explicitly reports
`historical_availability_verified == False`: verification of values is not
verification of historical knowledge, and an assertion or digest cannot supply
that proof.

`TraderTriangleState` has four states:

| State | Meaning |
| --- | --- |
| `READY` | Every in-scope source event has admitted availability and at least one synchronized tuple exists. This does not mean every event has a match or that the market/source window is complete. |
| `EMPTY_WINDOW` | The verified source contains no event in this interval/scope; not proof of market closure or a zero opportunity. |
| `UNAVAILABLE` | Some in-scope events lack admitted availability. Any retained tuples describe only available support; they cannot stand in for the complete window. |
| `NO_SUPPORT` | All in-scope availability is admitted, but no tuple satisfies the fixed synchronization policy. |

The request caps complete-publication verification at 100,000 events and
retained window events at 4,096 (callers can lower either). At most 4,096
availability declarations are accepted. Native retained-event and tuple
payloads each pass the existing bar-feature canonical byte/depth/collection
bounds. Budget excess, malformed input and changed bytes raise rather than
becoming a fabricated no-support result. These are bounded inputs, not a hard
wall-clock or process-sandbox guarantee. Results have no new wire ID: use
`replay(result)` for fresh verification, including any in-memory alteration or
re-sealed native tuple; a dataclass constructor alone certifies nothing.

## Calendar, positioning, dataset and bar semantics

Calendar queries preserve coverage, source roots, event identity, actual first
release versus latest revision, previous value as known, observed consensus
versus machine projection, and missingness. A/P/observed-F/machine-F are not
interchangeable. They use the canonical official-source architecture, whose
input adapters—not a licensed commercial calendar—own acquisition.

Positioning calls are deliberately strict ex-ante queries. Decision time may
not follow the requested window start. The adapter cannot fall back to ex-post
corrected CFTC history, nominal release schedules or an unverified vintage.
Native `READY`, missing, stale, unavailable, unsupported and
restatement-incomplete statuses pass through unchanged, with report family,
scope, currency mapping, report date and age. Futures positioning is not spot
volume or an interpolated tick feature; synthetic tests that construct an
original-vintage fixture do not establish a real original vintage.

Dataset registration returns a new validated catalog-backed reader, preserves
the old reader, deduplicates identical entries and refuses conflicting IDs or
aliases. An addition is itself a valid self-contained canonical catalog.
Resolve a mutable alias once with an explicit `DatasetQueryScopeV1`; retain and
replay that receipt rather than resolving today's alias again. `lineage()`
returns the root and complete distinct ancestor manifests sorted by exact
version ID, preserving each native parent role/ordinal. Its traversal is
bounded and detects cycles rather than truncating. `verify()` delegates to
the native catalog's selected-version artifact verification: ancestor closure
is metadata, **not** a claim that all ancestor bytes were recursively verified.
If needed, explicitly resolve/verify each retained ancestor under its own
appropriate scope. Registration/byte verification cannot manufacture licensing,
source qualification, historical availability or scientific eligibility.

Bar/activity requests return the exact existing `CausalBarSnapshotV1`, including
immutable source-product and bar-manifest roots even when all cells are missing.
Closed-boundary ownership, seven supported widths, origin scopes, warmup,
partial support and explicit availability/absence policies are unchanged.
Observed-only ex-ante limitations and generated-mode refusals remain enforced
by that source. Counts, tick intensity and quote-update rates are activity,
not traded volume; HistData's nominal volume placeholder is not exposed as a
new meaningful volume feature.

## Integration base and merge sequence

The implementation selection was based on the complete 640-issue/27-advertised-
branch audit, not the older `dev` SHA in the original issue body. Frozen input
refs for this additive integration are:

| Ref | Audited value and disposition |
| --- | --- |
| `dev` / `origin/dev` | `23a2f28b6a47fede6f76989d58d9f22fc7023dbf`; implementation base. |
| `origin/main` | `045fdd3c630454d0149b9b17d5e875116e019c5d`; remote production ref, unchanged by this work. |
| Local `main` | `7002bb8d6f5c26fa2579cc319ac1c3268dee869e`; differs from remote, so never silently use it as the production comparison. |
| Common base of audited `dev` and `origin/main` | `ec80924041b1193207ab3a89a4f80aebf4b38ee0`; 117 commits unique to `dev`, 9 unique to remote `main` at selection. These counts are historical, not a future merge recipe. |
| Retired calendar source | `63da3b6f23131ec2713fe3b032163acda3a05105`; retain as audit provenance, never wholesale merge. |
| Neutral calendar replacement | `5a60a9b6fdbabb936f1576a61c5908947b203f60`; canonical implementation on `dev`. |

The reviewed [calendar migration map](economic-calendar-branch-migration.md)
closes #682 with 58/58 dispositions. Provider-neutral query/vintage/replay
semantics survive; provider credential/fetch/schema assumptions do not. The
retired branch and 17 dirty old worktrees are not merge inputs, and none are
deleted or modified by #666. TTS supplies no scientific trader delta;
packaging/CI branches are environment changes requiring their own gates, not
alternate input-contract owners.

The integration sequence is:

1. Start from the audited canonical `dev` owners and preserve their native
   schemas; do not cherry-pick obsolete `main` or calendar implementations.
2. Integrate this isolated additive package, tests and documentation on `dev`.
   Coordinator-owned #608 temporal training and #633 compatibility work is
   non-overlapping; shared packaging/docs and final validation are reconciled
   after their files are frozen, not by overwriting any dirty worktree.
3. Run complete repository/source, typing, documentation and clean installed-
   wheel checks on the final combined bytes before committing/closing.
4. Subsequent #660 snapshot and production trader work consumes these public
   seams and retains its own missing prerequisites (including catalog provenance
   and empirical gates). This foundation does not close those issues.
5. Production release remains a separate reviewed `dev`-to-`main` workflow,
   including the repository's TestPyPI/local-registry preflight. No blind
   branch merge, version release or package publication is part of this change.

## Executable reconciliation guard

`tests/unit/test_trader_integration.py` uses only generated local fixtures. It
exercises all five actual adapters, native query equivalence, strict CFTC
refusals/positive vintage state, calendar revisions/forecasts, immutable
registration/diamond lineage, exact bars/activity, committed event publications,
all join/scope combinations, cutoff and age boundaries, empty/no-support states,
fresh-byte tampering and re-sealed result replay refusal.

Its AST guard allowlists canonical dependencies, refuses private imports and
retired calendar symbols, and forbids importing alternate package modules via
plain module imports. Any future dependency expansion requires an explicit
reviewed test change. It is a maintenance architecture guard, not a sandbox
against dynamic hostile Python. `reconstruction.py` and the broad
`synthetic/__init__.py` are unchanged; no production trader needs branch-private
or licensed-calendar APIs to consume any of the five seams.
