# Strategy-sensitivity contracts

The version-one strategy-sensitivity layer measures whether the behavior of one
deterministic strategy/execution specification changes when the same historical
window is presented through observed, degraded, reconstructed,
broker-conditioned, unconditioned, or derived-bar data. It is a downstream
scientific diagnostic. It does not validate a reconstruction by itself and it
does not issue a trading, investment, or model-promotion recommendation.

## Contract surface

| Contract | Responsibility |
| --- | --- |
| `StrategySpecificationV1` | Method, implementation version, bounded parameters, and a content-derived strategy identity. |
| `StrategyExecutionSpecificationV1` | Entry latency, maximum quote wait, per-side slippage, per-side fixed cost, quote-crossing semantics, and normalized exposure. |
| `StrategyEvaluationPolicyV1` | Multiple horizons and hard case, quote, signal, pending-signal, slice, payload, and rounding bounds. |
| `StrategyEvaluationCaseV1` | One source artifact and exact aligned half-open symbol/time window, including information mode, audit identity, member, broker, scope, and bar interval. |
| `StrategyEvaluationPlanV1` | One strategy/execution/policy applied identically to every case and alignment group. |
| `StrategyQuoteV1` | Minimal bid/ask quote plus feed epoch, session, event state, sparsity, member, and broker context. |
| `StrategySignalV1` | A current-quote-bound, content-addressed long or short decision. |
| `StrategyWindowResultV1` | Bounded window status, cadence/spread support, counts, and online slice summaries without retained quotes or outcomes. |
| `StrategySliceResultV1` | Source/epoch/session/event/sparsity/broker/member/horizon execution response. |
| `StrategyUncertaintySummaryV1` | Cross-member and cross-window dispersion for one source/regime/horizon cell. |
| `StrategyRestorationResultV1` | Reverse-degradation distance of a reconstructed execution response from the dense reference relative to the degraded input. |
| `StrategySensitivityReportV1` | Deterministic plan, window evidence, uncertainty, restoration evidence, terminal rates, and trust labels. |

Every input and result has a versioned schema plus a content-derived identifier.
Changing field meaning, accounting, time alignment, validity semantics, or
identity derivation requires a new schema version.

## Source surfaces and exact alignment

`StrategySourceKind` distinguishes:

- untouched observed history;
- intentionally degraded modern holdouts;
- reconstructed ensemble streams;
- unconditioned reconstructions;
- broker-conditioned reconstructions; and
- bars derived from committed final events.

Cases sharing an `alignment_window_id` must have the same normalized symbol and
the same `[start_ns,end_ns)` bounds. A plan rejects a source/member/broker role
that appears twice in the same aligned window. The strategy, execution
assumptions, and evaluation horizons live once on the plan, so source cases
cannot quietly use different logic.

Derived-bar cases must name both `source_scope` and `bar_interval_code`. Their
`source_artifact_id` is expected to be the verified derived-bar manifest. This
prevents merged bars from being compared with observed-only bars without making
the support difference explicit.

`StrategyQuoteV1` adapters bridge the existing product contracts:

- `from_benchmark_event()` handles observed, degraded, control, and candidate
  reverse-degradation events;
- `from_synthetic_event()` handles final observed/generated reconstruction
  events after the caller supplies the point-in-time session/event context; and
- `from_derived_bar()` uses the verified bar close and never invents volume.

Quote streams must be strictly ordered by
`(event_time_ns,event_sequence,quote_id)`, stay inside the case window, and
match its symbol, ensemble member, and broker profile where declared.

## Information modes and the invalid-for-backtest boundary

Every case binds an `InformationAuditReportV1`. The evaluator verifies the run,
manifest, audit identity, and information mode and requires an accepted audit.
An ex-ante case that claims prospective usefulness additionally requires the
existing `valid_for_strategy_usefulness_claim` gate.

Ex-post reconstruction is useful for historical counterfactual diagnosis but
is not point-in-time-valid strategy evidence. Every ex-post case therefore
requires an `invalid_for_backtest_reason`. Mixing ex-ante and ex-post cases in
one plan additionally requires an explicit plan-level reason. The case, plan,
window result, and final report then serialize:

```json
{
  "valid_for_backtest": false,
  "backtest_label": "invalid-for-backtest"
}
```

An explicit label permits a descriptive comparison; it does not make the
comparison prospective-valid.

## Pluggable strategy boundary

`StrategySignalEngineV1` exposes a versioned `StrategySpecificationV1` and
creates fresh `StrategySignalStateV1` state for every case. Window-local state
consumes one current quote at a time and may emit only signals bound to that
quote and decision time. The evaluator rejects implementation/specification
drift and signals that refer to another quote or time.

`ReferenceMomentumStrategyV1` is the transparent fixture. It compares the
current midpoint with the last bounded quote at or before a configured
time-based lookback, rate-limits decisions, applies a threshold in basis points,
and emits a long or short signal. Its bounded deque is reset for each case. It
exists to validate alignment and accounting semantics, not as a recommended
trading strategy.

## Execution response accounting

Version one uses normalized unit exposure and always crosses the quoted market:

- long entry at ask and exit at bid;
- short entry at bid and exit at ask;
- configured per-side slippage worsens both prices;
- configured per-side fixed costs are subtracted twice;
- entry uses the first quote at or after `decision_time + entry_latency_ns`;
- entry and horizon exits must arrive within `max_execution_wait_ns`.

Each completed signal/horizon records only online aggregates for:

- gross signed midpoint response in basis points;
- net executable response after spread, slippage, and fixed costs;
- cost drag;
- actual entry delay; and
- favorable-response rate.

These are normalized response/sensitivity quantities, not currency P&L. The
report fixes `profit_claim`, `investment_recommendation`, and
`automatic_winner` to false.

## Stratification, uncertainty, and reverse degradation

Signal outcomes are accumulated under the decision quote's complete key:

```text
source kind, symbol, feed epoch, session, event state, sparsity,
broker profile, ensemble member, horizon
```

Uncertainty summaries retain member IDs and report cross-member/cross-window
mean, range, and population standard deviation of the mean net executable
response. Different horizons remain separate cells.

Where one aligned cell contains dense observed, degraded holdout, and
reconstructed results, the restoration result computes:

```text
degraded_error  = abs(degraded_response - dense_response)
candidate_error = abs(candidate_response - dense_response)
restoration_gain = degraded_error - candidate_error
approaches_dense_reference = candidate_error <= degraded_error
```

Sparsity remains visible on each result but is not part of the restoration join
because dense, degraded, and reconstructed surfaces necessarily have different
support labels. Missing dense or degraded comparators increment a bounded
`restoration_unavailable_count`; they never imply success.

`strategy_sensitivity_benchmark_hooks()` projects a completed window into the
existing #436 `strategy_hooks` surface, including the canonical
`downstream_sensitivity` consumed by #442 ensemble calibration plus gross
response, cost drag, entry delay, and missing-support rate. Non-completed
windows refuse hook projection instead of receiving a plausible zero; their
missing hook preserves the existing ensemble-member refusal behavior.

## Terminal states and rates

Every planned case produces exactly one terminal window result:

| Status | Meaning |
| --- | --- |
| `completed` | At least one signal/horizon had valid entry and exit support. |
| `no_trade` | Quotes were present but the identical strategy emitted no signal. |
| `missing_support` | The stream was absent/empty or no signal horizon could be completed. |
| `refused` | A configured quote, signal, pending-signal, slice, or payload resource ceiling stopped evaluation. |
| `failed` | A strategy plugin reported an explicit scientific/evaluation failure. |

The final summary includes counts and rates for failure, no-trade,
missing-support, and refusal plus the missing-support outcome rate. Contract or
ordering violations raise and fail closed instead of becoming a plausible
window result.

## Streaming and storage

Cases are evaluated sequentially. The evaluator retains only:

- the strategy's bounded current state;
- bounded pending signals until their largest horizon resolves; and
- constant-size numeric accumulators per bounded slice.

It does not retain the 521-column analytical surface, whole quote streams, or
individual signal outcomes. Window metadata fixes `quotes_retained` and
`outcomes_retained` to false. The report uses
`bounded-derived-metadata`, and `event_schema_augmented` is false. Strategy
sensitivity is therefore a compact replayable side artifact, not another set of
per-tick augmented columns.

## Downstream obligations

- #449 must run identical strategy/execution plans across the certified
  EURUSD/GBPUSD/EURGBP source surfaces and include terminal/support rates,
  member uncertainty, and restoration evidence in its acceptance dossier.
- A strategy result remains one downstream criterion alongside structural,
  cross-currency, carving, broker, activity, and reverse-degradation evidence.
- No caller may promote a reconstruction, strategy, or ensemble member solely
  because its strategy response looks favorable.
