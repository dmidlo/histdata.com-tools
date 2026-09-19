# Causal closed-bar features and explicit consumer paths

Issue #649 adds opt-in version-one APIs. Existing event/bar identities,
`DerivedBarV1`, strategy helpers, motif ranking, broker delivery models,
training defaults, and frozen campaign selection are unchanged. This is a
SemVer-minor capability, not empirical qualification of a model or strategy.

## Source verification and decision availability

Import the core from `histdatacom.synthetic.bar_features`. Construct a
`BarFeatureSourceV1(reconstruction_manifest_path, bar_manifest_path)` with real
committed publication manifests. It loads typed manifest identities first and
checks **total product** declared event/bar work against the feature policy,
before verifying entire publications. It then verifies their source binding,
replays every selected-symbol bar from committed events using the original
bar policy/query bounds (including incoming transition carry), and requires
exact row equality with stored bars. Caller-provided IDs do not establish
source verification. Every snapshot, including entirely unavailable state,
binds the exact verified source product manifest and derived-bar manifest IDs.
Its available bars must bind that same source product. Consumers repeat source
verification; an identity field alone is never a verification certificate.

Source budgets default to 100,000 events and 10,000 bars, capped at 1,000,000
events and 100,000 bars. Declarations and serialized arrays are capped at 4,096
items; canonical artifacts are capped at 4 MiB, nesting at 16, integer fields
at int64, and floating-point fields must be finite. Limits refuse rather than
truncate. Artifacts are frozen, content-addressed, strict about scalar types,
and reject unknown fields, duplicate JSON keys, identity mismatch, and derived
feature disagreement. Snapshot symbols are uppercase canonical FX pairs;
verified underlying event/bar products retain their original lowercase symbols.

For width Δ and decision cutoff t, b = floor(t/Δ)Δ. The current standard bar
is exactly **[b−Δ,b)**. At b−1 it is the preceding interval; at b and b+1 it
is [b−Δ,b). A standard bar cannot be available before its end, regardless of
its last tick. An event at b belongs to the next bar. Partial start/end bars
are retained as explicit `partial_support` cells with null feature values;
there is no partial-bar feature namespace or silent completion in this version.

Each `BarAvailabilityDeclarationV1` binds one exact bar ID, `available_at_ns`,
`known_at_ns`, basis, and explanatory text. Both clocks must be no later than
the decision cutoff. `known_at_ns` dates the declaration itself. Source clocks
are explicit operator assertions, **not certificates of historical knowledge**.
`REPLAY_CLOCK_ASSUMPTION` requires `allow_replay_clock_assumptions=True` and is
explicitly visible in the snapshot. A generated/merged bar additionally needs
`allow_prior_generated_state=True` and `generated_at_ns <= available_at_ns`.

| Information mode | Observed scope | Synthetic / merged scope |
| --- | --- | --- |
| Ex-ante simulation | Admitted only with cutoff-qualified availability | Refused: current persisted products have no bound source information audit |
| Ex-post reconstruction | Admitted with cutoff-qualified availability | Requires prior-generated permission and cutoff-qualified generation/availability |

The observed exception uses only immutable observed anchors; it does not
certify when those anchors were historically available. Merely supplying a
matching run/audit ID or a generation clock cannot relabel future-conditioned
synthetic values ex-ante. All snapshots and consumer result artifacts have
`historical_availability_verified=False`; consumer artifacts also forbid
`empirical_qualification_claim=True`.

Unknown support is `unavailable`, never an invented closure or zero.
`BarAbsenceDeclarationV1` explicitly distinguishes `missing_bar`,
`expected_closure`, `source_outage`, and `unsupported`. Its own `known_at_ns`
must qualify at cutoff. Conflicting available-bar/absence declarations and
duplicate ownership refuse. Only the four-bar dependency span is retained.
Future/late availability, absences, and explanations are filtered before
selection; they do not change historical snapshot hashes or missing reasons.
Changing the underlying committed product legitimately changes existing bar
IDs: source-version identity is distinct from adding unavailable declarations.

## Frozen feature registry

`bar_feature_definitions()` provides typed versioned names, units, required
closed-bar span, lag, and formula. The 46-feature registry includes bid, ask,
mid, and spread OHLC; event/origin/quote-update/price-change/stale/transition/
confidence-support counts; verified mean spread, tick intensity, stale rate,
and mean event confidence; plus the following fixed indicators. No bar means
are composed to reconstruct tick-level statistics. Raw primitive values retain
the source bar's rounding; indicators are rounded once at the policy's
`rounding_digits` (default 12). There is no approximate-equality tolerance.

Here C is the current close, C−k a prior close, and H/L/O current OHLC.

| Feature | Exact unrounded arithmetic | Consecutive closed-bar span |
| --- | --- | --- |
| `mid_log_open_close_return` | log(C/O) | 1 |
| `mid_log_close_return` | log(C/C−1) | 2 |
| `mid_log_range` | log(H/L) | 1 |
| `mid_body`, upper/lower wick | C−O; H−max(O,C); min(O,C)−L | 1 |
| `mid_body_range_ratio` | (C−O)/(H−L) | 1 |
| `mid_true_range` | max(H−L, abs(H−C−1), abs(L−C−1)) | 2 |
| `mid_atr_3` | Arithmetic mean of latest three true ranges | 4 |
| `mid_realized_variation_3` | Sum of squared latest three close log returns; no square root/annualization | 4 |
| `mid_sma_3` | Mean(C−2,C−1,C) | 3 |
| `mid_momentum_3`, `mid_slope_3` | C−C−3; (C−C−2)/2 (three-point OLS, units per bar) | 4; 3 |
| `mid_prior_mean_3`, `mid_prior_std_3` | Mean/population standard deviation of C−3,C−2,C−1 only | 4 |
| `mid_prior_zscore_3` | (C−prior mean)/prior population standard deviation | 4 |
| Observed/synthetic proportions | Respective origin count / event count | 1 |

The prior mean/std use only three **prior** closes in arithmetic (`lag_bars=1`),
but require the current complete bar as their ownership gate. Thus their
four-bar span, evidence IDs, and availability include that gate. Z-score also
uses current close and has lag 0. Other indicators have lag 0, even when they
use prior closes. Availability is the maximum of the admitted availability and
declaration clocks across all evidence/gating bars. Each value retains exact
ordered source bar IDs; scope/timeframe are explicit on its containing cell.

Missing or partial bars anywhere in a required span produce
`insufficient_warmup`; no gap bridging or forward fill occurs. Retained absence
declarations explain known prior gaps separately. Zero range/standard deviation
produce `zero_scale`, not zero-valued ratios. Missing transition, confidence,
or positive event-duration support have distinct null states. Quote activity
is not centralized traded volume. Session boundaries do not implicitly reset
UTC bars or indicators; declared closures/gaps prevent rolling composition.

## Construct and consume a snapshot

```python
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAvailabilityBasis, BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1, BarFeatureSourceV1,
)
from histdatacom.synthetic.information import InformationMode

source = BarFeatureSourceV1("/archive/product/manifest.json",
                            "/archive/bars/manifest.json")
policy = BarFeaturePolicyV1(
    InformationMode.EX_ANTE_SIMULATION,
    intervals=("1m", "5m", "15m", "30m", "1h", "4h", "1d"),
    scopes=(ActivitySliceScope.OBSERVED,),
)
# Supply actual operator availability declarations; never infer them merely
# from a bar's final tick. Omitted declarations leave the bar unavailable.
snapshot = source.snapshot(symbol="EURUSD", decision_time_ns=decision_ns,
                           policy=policy, availability=declarations)
```

The same snapshot contract drives four actual, explicitly invoked consumers:

- `histdatacom.synthetic.bar_conditioning.query_reference_motifs_with_bar_state`
  runs existing motif retrieval using log **open/close** return and tick
  intensity, whose meanings match existing motif metrics. It does not substitute
  mean spread for median spread, close/previous-close return for open/close
  return, or bar variation for tick variation. Unknown categories remain
  unknown; ambiguous feed epochs and unavailable required features refuse.
  Ex-ante queries use the snapshot cutoff as their `as_of_ns`. The result binds
  conversion version, scope, interval, query/retrieval, snapshot and policy.
- `histdatacom.synthetic.bar_strategy.evaluate_causal_bar_strategy` accepts
  `CausalBarStrategyInputV1` source/snapshot inputs, not arbitrary normalized
  quotes. It verifies each case's exact bar product, run/member, scope/width,
  mode and decision window, creates quotes at **decision_time_ns**, and runs
  the existing strategy engine/evaluator. A plan-level explicit diagnostic
  invalid-for-backtest reason is mandatory. Existing information audits remain
  required but cannot override source admission. Legacy
  `StrategyQuoteV1.from_derived_bar` is unchanged and diagnostic-only: its
  last-event timestamp is **not** the #649 causal consumer path.
- `histdatacom.broker_capture.bar_fingerprints.fit_broker_delivery_fingerprint_with_bar_state`
  really fits verified capture events with the existing model, requires the
  fitted profile ID to equal the reconstruction product's rendering profile,
  and adds a separate bar-state fingerprint. Broker-specific V1 products
  are supported; generic-delivery V2/V3 products
  cannot impersonate a broker profile and are refused by this consumer.
  Repeated observations of the
  same closed feature bar cannot increase support; conflicting observations
  refuse. Widths/scopes remain separate, not independent pseudo-observations.
  The association is a verified **rendering profile**, not simultaneous capture
  evidence. `compare_broker_delivery_fingerprints_with_bar_state` runs existing
  delivery comparison plus per-cell mean deltas under exactly equal feature
  policies; it validates result schemas and replays support/mean/population
  variance from retained evidence. Comparing supplied fitted artifacts does not
  independently authenticate raw captures or upgrade availability assertions.
- `histdatacom.data_quality.bar_training_features.enrich_tick_cache_with_causal_bar_features`
  first runs existing tick enrichment, then adds `causal_bar_v1__...` columns
  without changing any old column or row. Observed-only snapshots must match
  each exact UTC-millisecond row cutoff, symbol and common policy; multiple
  ticks at one time reuse one snapshot. There are no nearest/future joins or
  orphan snapshots. Feature value/state/source-ID/availability columns have
  fixed nullable types even when all values are missing. Generated-state tick
  training is refused because this existing row grain is observed. Reapplying
  the namespace refuses rather than overwrites. The namespace's information
  mode qualifies **only the added bar features**, not other diagnostic training
  columns, the entire frame, training eligibility, or backtest validity.

Consumers never modify their frozen predecessor APIs. Source budgets and
artifact caps apply; large datasets should be explicitly partitioned into
bounded source products and consumer calls. The APIs are available by the
module imports above; they do not require shared package-root re-exports.

## Verification and remaining qualification

Focused tests independently calculate all indicator families across 7 widths
× 3 scopes, test cutoff−1/cutoff/cutoff+1 with boundary events and contradictory
future prices, exercise real committed source/bar replay, future stored-bar
and late-outage invisibility, mode/scope refusal, budgets before deep reads,
strict identities and immutable wire round trips. Real consumer integration
tests execute retrieval, momentum evaluation, capture fitting/comparison and
tick enrichment, including refusal/missingness paths and unchanged old columns.

This satisfies a common causal-state interface and real opt-in adoption, not
the later held-out conditioning qualification in #650, complete training
substrate integration in #652, or completion of #606/#608/#610. Empirical gain,
future-safe generated ex-ante lineage, and verified historical availability
remain separate evidence requirements.
