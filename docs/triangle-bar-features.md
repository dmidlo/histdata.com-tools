# Causal triangle bar features v1

Issue #651 adds an opt-in feature plane for EURGBP (`X`), EURUSD (`U`) and
GBPUSD (`G`). It does not change the narrow event schema, old bar identities,
reconstruction engines, frozen campaigns or scientific eligibility gates.
It is one executable input to #605, not completion or certification of that
wide-corpus program. No empirical alignment/model qualification is asserted.

## Public APIs and evidence

Import `TriangleBarPolicyV1`, `TriangleBarStratumV1`,
`TriangleBarSourceV1`, `TriangleBarSnapshotV1` and
`triangle_feature_definitions` from
`histdatacom.synthetic.triangle_bar_features`.
Construct the source with the existing `BarFeatureSourceV1` pointing to a
committed reconstruction manifest and its committed derived-bar manifest.
`source.snapshot(...)` verifies both publications, replays bars from events,
and retains the exact full event content supporting the selected bars.
`source.verify_snapshot(..., information_mode=...)` repeats this at consumption.

All three legs must share the exact source product, derived-bar manifest,
information policy and decision cutoff. Four closed-bar intervals are retained
per leg/scope/width for bounded history. The seven standard widths are 1m, 5m,
15m, 30m, 1h, 4h and 1d. A cutoff `t` owns the last complete UTC interval
`[floor(t/w)*w-w, floor(t/w)*w)`. No event at that interval's end enters it.
Missing or partial legs produce explicit missing values; there is no fill,
cross-outage carry, invented session schedule or partial-bar promotion.

The #649 information-mode rules still apply: observed assertions may support
ex-ante state; legacy synthetic/merged products cannot be relabeled ex-ante
because they lack a bound source information audit. Synthetic/merged state
requires explicit prior-generated admission and generation clocks in ex-post
mode. A content hash verifies values, not when anyone knew them. All snapshots
retain `historical_availability_verified=false`; replay-clock assumptions
remain separately flagged and require opt-in.

Policy knowledge, bar availability and knowledge, and stratum knowledge are
distinct clocks. Future declarations are removed before feature selection and
cannot alter a past snapshot's missing reasons or identity. Policy definitions
themselves must be known by the cutoff. Session/epoch labels are assertions
bound to an exact bar, not free-standing qualifying IDs. Frozen observed events
forbid `feed_epoch_id`, so their declared epoch is explicitly **not** verified
event metadata. Any non-null generated epoch must match the assertion. The
robust stratum key additionally includes actual source-version IDs for each
leg; mixed or absent labels never silently pool reference support.

## Frozen mathematical constructions

`triangle_feature_definitions()` declares names, units, formula, exact required
closed-bar span, output lag and availability rule. Outputs own the current
closed bar (lag zero); the robust baseline alone has a one-bar lag.
No additional decimal rounding is applied to these diagnostics: they consume
the existing frozen rounded bar values or exact event quotes as declared.
Nonfinite derived values are unavailable, never clipped or zero-imputed.

| Family | Definition / support |
| --- | --- |
| OHLC residual open/close | `log(U)-log(X)-log(G)` on each leg's own midpoint endpoint; endpoints can be asynchronous |
| Conservative bounds | lower=`log(U.low)-log(X.high)-log(G.high)`; upper=`log(U.high)-log(X.low)-log(G.low)`; range=upper-lower |
| Residual change | current close residual minus previous close residual; two complete bars per leg |
| Robust score | `(current residual - prior median)/max(1.4826*MAD, policy epsilon)` over exactly three preceding residuals in the same per-leg source/epoch/session stratum; four complete bars per leg |
| Robust scale flag | one when declared epsilon exceeds the prior-only scaled MAD; epsilon is fixed in policy, not fit to the sample |
| Returns | each pair's `log(current mid close)-log(previous mid close)`; three pairwise return differences; common return is the equal-weight mean of the three correlated returns |
| Spreads | each close spread divided by its midpoint; asymmetry=`log(X.ask/X.bid)-log(U.ask/U.bid)-log(G.ask/G.bid)` at asynchronous close |
| Activity | three pairwise event-count ratios; common activity=sum counts/(3*full interval seconds) |
| Origin support | observed and synthetic counts divided by total counts across all legs |

The conservative high/low bounds are **not sampled residual extrema**: each
price extremum can occur at a different time. The common return is an explicitly
fixed descriptive factor, not a fitted latent/PCA factor or three independent
pieces of historical evidence. Quote counts/intensities are not traded volume,
and origin proportions are not sample weights.

## Event-backed synchronization and executable sides

Exact matching pairs events at a common timestamp by per-leg sequence/ID
ordinal, matching #515 semantics. Bounded-prior matching uses the declared
probe symbol and latest nonfuture quote from each other leg, with inclusive
`max_quote_age_ns`. There is no age optimization using later support and no
previous-interval carry. This explicit diagnostic policy does not claim its
bound passed the #515 empirical qualification campaign.

Each first/last supported tuple retains full quote content, event IDs, probe
time, logical endpoint and per-leg ages. `first_*` is the first supported probe,
**not** the asynchronous OHLC open. `last_*` is the last supported probe, not
a claim that all legs' final bar events were simultaneous. OHLC and event-backed
residuals remain separate columns. Last-tuple values additionally require
every selected quote to be no older than `max_endpoint_age_ns` at the logical
closed boundary. Three equally old quotes have zero mutual age but may fail
this endpoint-age check. Exact integer nanosecond ages remain integer columns.

In the existing engine orientation, **direct means EURGBP (`X`)**, while the
midpoint residual is `log(U)-log(X)-log(G)`:

- `sell_direct_gap = log(X.bid)+log(G.bid)-log(U.ask)`.
- `buy_direct_gap = log(U.bid)-log(X.ask)-log(G.ask)`.

These signed log executable-side envelope gaps differ from the engine's
nonnegative maximum absolute side discrepancy. Neither a midpoint residual nor
a positive gap establishes realizable arbitrage: fees, available depth, latency,
credit and execution certainty are unqualified.

## Positive retained projection bridge

Import `TriangleProjectionEvidenceV1`, `TriangleProjectionSnapshotV1` and
`derive_triangle_projection_features` from
`histdatacom.synthetic.triangle_projection_features`.
`TriangleProjectionEvidenceV1.retain(...)` preserves complete canonical run,
window, reconciliation config, original proposal streams and conditions.
It replays the public reconciliation engine followed by modern-reference
identity delivery. Consumption matches that actual delivery identity and full
output content to the committed generic V2/V3 product. Neither process-local
group metadata nor an arbitrary report/receipt ID substitutes for these inputs.

Version one supports one fixed oriented triangle relationship and exact-event
ordinal mapping. Every tuple uses the full pre/post six-quote vector:
`s = sum(max(ask_pre[j]-bid_pre[j], epsilon), j=1..3)`,
`b = sum(abs(quote_post-quote_pre))/s`, matching the #516 primary scale.
The declared epsilon defaults to the #516 policy default, `1e-9` quote units;
its value is part of the retained evidence identity, never a sample-fitted floor.
Bars receive only tuples whose actual probe times lie in their half-open bins.
Unchanged synthetic-support tuples contribute zero burden **and remain in the
denominator**. Observed-only tuples do not enter synthetic burden means;
observed scope is explicitly not applicable, not a fabricated zero.
Both mean normalized burden and spread-weighted burden are exposed, alongside
support/projected counts, rate, total movement/scale and pre/post signed-log
and absolute engine residuals. These are distinct residual definitions.

The whole retained window is available only at the maximum of declared
knowledge/generation clocks, input-window end and latest retained event time.
Future evidence is absent before that clock. Unsupported broker rendering,
ambiguous bounded-prior re-projection, incomplete window-to-bar ownership or
absent proposal evidence is never guessed. Omit unsupported retained evidence
to obtain explicit unavailable projection fields. Whole-window row-free #516
report slices cannot be prorated or reversed to invent event proposals; those
reports and their scientific gates remain unchanged.

## Actual row-preserving training consumers

Import from `histdatacom.data_quality.triangle_training_features`:

- `enrich_tick_cache_with_triangle_features`: runs existing observed tick
  enrichment, then appends observed-only triangle features. Explicit row event
  IDs must match verified observed symbol, exact millisecond timestamp and
  bid/ask. Decision time equals the original row timestamp; delayed evidence is
  not admitted through this path. Other legacy enrichment columns can remain
  diagnostic/noncausal; namespace information mode qualifies only new columns.
- `append_triangle_features_to_reconstruction_rows`: accepts every original
  native event column unchanged, plus explicit `decision_time_ns`. It verifies
  full row provenance against committed events and exact snapshot ownership.
  Ex-post decisions may be later than event time, but never earlier. Delayed
  evidence is flagged without changing or backdating `event_time_ns`. Optional
  projection snapshots are independently replay-verified against publication.

Both preserve row order, count, IDs, values and original schema, including null
provenance columns. Existing input duplicates remain duplicates; neither API
creates duplicates or assigns independent evidence mass. Columns use the
`triangle_bar_v1__` namespace, refuse overwrite and freeze nullable dtypes.
Source product, source event, run/member, cutoff, policy/snapshot and retained
projection window/evidence IDs accompany values and exact bar/event lineage.
These APIs do not assign train/test splits, fit preprocessing, alter holdouts,
invent historical weights or certify ML/backtest/live-trading eligibility.

## Bounded wire and work

Artifacts use the existing strict causal-bar envelope machinery with distinct
`triangle-*` kinds: exact types, unknown-field rejection, whole-envelope hashes,
4 MiB wire limit, 4,096 collection entries and depth 16. Legacy nested payloads
must match their full canonical roundtrip, including null and otherwise ignored
fields; coercive old readers cannot silently discard new evidence. Constructors
recompute feature cells from retained evidence. Source event/bar budgets are
checked on typed whole-product manifests before expensive verification, not
only after selecting one symbol. At most 4,096 source events are retained for a
triangle snapshot or proposal bridge; oversized spans refuse rather than sample.
Exact timestamp indexing is grouped in one pass before ordinal matching.
Training consumers additionally preflight at most 250,000 added output cells
and a conservative 32 MiB wire-equivalent scalar/provenance-string estimate
before source replay. This is not an allocator-specific resident-memory claim.
Use smaller row chunks for wide seven-timeframe/three-scope projections; the
4,096-row cardinality cap alone is not permission to allocate an unbounded wide
matrix. No truncation or feature dropping is performed to make a chunk fit.
After source verification, a second guard includes actual retained run/member
text lengths before allocating extension rows; old event contracts permit
large metadata strings, so a fixed per-cell allowance alone is insufficient.
