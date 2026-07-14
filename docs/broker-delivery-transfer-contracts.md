# Broker delivery transfer contracts

`histdatacom.synthetic.broker_transfer` applies an effective-dated broker
delivery fingerprint as a bounded observation-style transform. It does not
generate historical price paths, mutate observed anchors, or persist augmented
tick-sized intermediates. The transfer has two explicit stages:

1. `condition_broker_proposal()` changes the delivery-related coordinates of a
   `ReferenceMotifQueryV1` before motif retrieval and candidate generation.
2. `render_broker_delivery()` applies delivery precision, rounding, timestamp
   batching, stale-quote, exact-duplicate, and spread behavior to synthetic rows
   only after historical carving and cross-currency reconciliation.

This order keeps the historical episode as content and the broker profile as
delivery style. A renderer never uses the broker profile to invent directional
price movement.

## Profile selection and refusal

`select_broker_profile()` requires the requested condition cell to exist. A
supported cell is used directly. A backed-off cell follows only the fingerprint's
recorded `effective_condition_id`. An absent or unsupported cell refuses; there
is no implicit nearest-neighbor or global fallback for the requested condition.
Global-only delivery measurements can supplement an otherwise supported cell,
and every resolved metric records the condition ID that supplied it.

Selection also verifies that `selected_at_utc_ns` falls inside the fingerprint's
effective interval. If drift evidence is supplied, the comparison must include
the selected fingerprint. The selection embeds the fingerprint ID, predecessor
ID, effective period, comparison ID, material-drift count, resolved metrics, and
per-metric source cells. That evidence is carried into every render manifest.

## Proposal conditioning

Proposal conditioning operates on the motif query rather than rewriting a
generated candidate. The versioned `BrokerTransferConfigV1.strength` is a convex
blend: zero preserves the historical query and one requests the measured broker
target. The cadence target combines active/ordinary quote inter-arrival time
with measured burst, quiet, and outage behavior. Timestamp precision, price
precision, and spread are separate coordinates. Cadence and spread are changed
only when the historical query supplies a compatible coordinate; timestamp and
price precision have explicit one-nanosecond and configured input-decimal
baselines. Session, event, symbol, and reconnect eligibility remains explicit in
the selected fingerprint cell.

An unsupported proposal returns a `BrokerConditionedProposalV1` with no
conditioned query. The before and after metrics are identical so callers can
audit that no retrieval or generation request was emitted.

## Delivery rendering

Rendering accepts a passing `CrossCurrencyReconciledGroupV1` and its exact
`ReconstructionRunV1`, `ReconstructionWindowV1`, and
`HistoricalCarvingConstraintSetV1`. Programmer-level scope mismatches are
rejected at the API boundary. Profile-support, resource, rendering, and
validation failures return an explicit refused result before output is exposed.
The configured `max_events_per_group` is checked before event materialization.

Observed events are reused byte-for-byte. Synthetic rows may receive:

- bounded timestamp quantization or batching inside their historical anchor
  interval;
- source-like price precision and deterministic rounding;
- bounded spread projection without negative spread;
- deterministic stale-quote or exact-duplicate presentation; and
- the selected broker fingerprint ID on event lineage.

All stochastic-looking decisions are content-addressed. Repeating the same
inputs, fingerprint, condition selection, and config produces the same output
and identifiers. Transfer strength, maximum timestamp movement, spread
multiplier, batch size, decimal limits, feature switches, and rounding are
versioned in `BrokerTransferConfigV1`.

The renderer refuses the entire group if any row would escape its anchor
interval, reorder timestamps, alter an observation, violate a historical hard
constraint, acquire a negative spread, lose required quarantine state, or use
untraceable profile lineage. It never publishes partial rows.

## Final validation and manifests

An applied group must pass three gates after rendering:

1. local anchor, ordering, spread, constraint, quarantine, and lineage checks;
2. `validate_cross_currency_output(..., stage=post_broker)`; and
3. the cross-instrument quality path from #331 via
   `cross_currency_quality_report()`.

`BrokerTransferManifestV1` records input/output content hashes, the complete
transfer config, selections and effective periods, action counts, event and
lineage counts, a lineage hash, post-broker validation identity/status, #331
quality status/hash, and optional paired benchmark comparison IDs. Durable event
rows remain out of the manifest; persistence belongs to the later atomic
partition-publishing stage.

`BrokerRenderedGroupV1` is therefore a process-local handoff containing streams,
per-event lineage, validation evidence, and compact manifest metadata. A refused
result contains reasons but no streams, lineage, validation report, or quality
payload.

## Benchmark comparison

`compare_broker_benchmark_results()` pairs broker-conditioned and unconditioned
candidates on the same reverse-degradation scenarios. It reports bounded metric
deltas and scenario coverage through `BrokerBenchmarkComparisonV1`. The contract
sets `automatic_winner` to false and never emits a winner candidate. Promotion
semantics remain a separate policy decision after the comparison meaning is
proven.

## Streaming and persistence boundary

The normal pipeline carries one synchronized group at a time:

```text
historical anchors
  -> broker-conditioned motif query
  -> candidate generation
  -> historical carving
  -> cross-currency reconciliation
  -> broker delivery rendering
  -> local + post-broker + #331 validation
  -> downstream atomic partition writer
```

Only the final validated synthetic tick partitions and compact lineage/manifests
need durable storage. Proposal queries, candidate surfaces, reconciled groups,
and rendered groups can remain bounded in-flight artifacts unless a diagnostic
retention policy explicitly preserves them.
