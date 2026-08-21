# Final adaptive support verification

The final v2.5 support artifact is an independently replayed, candidate-bound
proof of every planning decision. It is not another planner summary. The
verifier reads immutable Arrow partitions and lower-level scientific artifacts,
reconstructs the evidence for each half-open interval, and compares the result
with the plan set and claimed support map. It never calls the planner's terminal
decision helper as its oracle.

Planning has exactly three terminal states:

- `executable`: scientific support and generator admission are complete;
- `empty`: raw replay proves a source-empty or expected-closure interval; and
- `refused`: source evidence exists but a named scientific gate is unsupported.

Operational failure is a later runtime state. It is explicitly excluded from
the planning schema and cannot be relabeled as a scientific refusal.

## Artifact graph

| Contract | Grain and proof |
| --- | --- |
| `FinalSupportPartitionReplayV1` | One reread Arrow partition: exact artifact hash, row count, coverage, first/last timestamp, requested-domain counts, and a digest over partition ID, source-row ordinal, and timestamp. |
| `FinalSupportWindowVerificationV1` | One exact interval: core/input counts, row and anchor digests, alignment-event digest, feed assignment, transition scenarios, session/event state, CFTC mode, modeled deficit, amplification, split depth, members, tasks, and terminal decision. |
| `FinalSupportCensusV1` | Content-addressed complete census: durations, terminal/alignment counts, age/deficit/amplification quantiles, epoch/transition/session/event/CFTC counts, split sizes, and refusal reasons by era. |
| `FinalSupportVerificationShardV1` | Bounded, gap-free verification output for one plan shard, bound to the plan set, plan, source inventory, claimed support shard, and release candidate. |
| `FinalAdaptiveSupportMapIndexV1` | Bounded root over all verification shards, exact requested range, frozen engine/scenario IDs, source cutoff, scientific nonclaim, and full census. |

All IDs are SHA-256 identities over canonical contract JSON. Writers use an
atomic replace. Readers verify strong references, shard metadata against shard
content, exact plan-shard coverage, global contiguity, aggregate terminal
counts, and the census rebuilt from every verified window.

## Independent replay

For each plan shard, the verifier performs these checks from lower-level facts:

1. Verify the plan graph and the exact alignment, feed-epoch, observation,
   market-context, CFTC, catalog, experiment, scientific-ledger, engine-config,
   and engine-fit artifacts frozen by the release candidate.
2. Hash-verify every source partition, reread its Arrow `datetime` column, and
   reconcile row count and timestamp coverage with the inventory and candidate
   source-hash set.
3. Assign every source row to exactly one core interval using strict
   `[start_ns,end_ns)` ownership. The sum of per-window core rows must equal the
   requested-domain partition census.
4. Reconstruct input halos and immutable row identities from partition ID plus
   zero-based Arrow row ordinal. A timestamp alone is never identity.
5. Recompute exact-event and bounded-nearest-prior support, including the
   selected probe, quote ages, recommendation, and selected source-row digest.
   Future rows, retimestamping, interpolation, and silent age widening remain
   forbidden.
6. Re-evaluate feed-epoch/transition conditioning, context availability, CFTC
   availability or explicit qualified unconditioned mode, source-empty state,
   and scientific refusal classification.
7. Independently recompute the worst-case modeled missing cardinality and the
   complete per-task memory, scratch, output, batch, member, and campaign
   resource rectangle.
8. Compare every planner claim with the replay. Any mismatch fails before a
   final artifact can be written.

The final index refuses a valid-common-data implementation refusal. This
means a complete source triangle with qualified alignment cannot be discarded
because an implemented feed, context, CFTC, or information-policy path failed
to handle it. Genuine scientific source/alignment limitations remain explicit
refusals.

## Census semantics

The census includes every interval, including empty and refused intervals.
Durations are exact `end_ns - start_ns` values, not rounded calendar buckets.
Nearest-prior alignment age uses the maximum selected tuple age per window.
Modeled deficit and amplification are independently reconstructed admission
bounds, not realized runtime output counts. Refusal keys combine feed era and
stable refusal code so early, transition, and modern limitations remain
separable.

The census identity changes for any terminal state, duration, alignment,
context/CFTC classification, cardinality, resource, or selected source-row
change. A one-nanosecond boundary or alignment-recommendation shift therefore
changes identity and fails comparison with the claimed plan.

## Installed operator surface

Build the claimed support map first, then independently finalize it against the
frozen release candidate:

```sh
histdatacom reconstruction --json support-map \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --output-directory work/support-map

histdatacom reconstruction --json support-verify \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --release-candidate work/release/reconstruction-release-candidate-<sha256>.json \
  --output-directory work/final-support
```

The equivalent Python call is:

```python
final_support_ref = ReconstructionClient().construct_final_adaptive_support_map(
    "work/artifacts/reconstruction-plan-set-<sha256>.json",
    "work/support-map/reconstruction-plan-support-map-index-<sha256>.json",
    "work/release/reconstruction-release-candidate-<sha256>.json",
    output_directory="work/final-support",
)
```

Pass that final index—not the preliminary claimed map—to `request-set`. Every
child `ReconstructionExecutionRequestV1` receives the exact same strong final
support reference. Reading or running the request set rejects a missing,
different, changed, or campaign-incompatible child reference.

Retained pre-verifier request artifacts remain readable with their historical
identity. They do not gain final-support qualification and cannot satisfy the
new final-campaign binding rule.

## Fail-closed mutations

The verifier rejects, at minimum:

- a one-nanosecond gap, overlap, bound, or alignment recommendation change;
- a missing, added, or changed release-candidate source hash;
- changed partition bytes, row count, coverage, or immutable row ordinal;
- changed alignment policy, selected event, maximum age, or event digest;
- changed feed assignment, transition policy, context corpus/state, or CFTC
  decision;
- a modeled cardinality, amplification, task estimate, member rectangle, or
  aggregate-resource mismatch;
- a shard whose metadata differs from its content or whose census does not
  rebuild exactly; and
- an engine, observation scenario, transition scenario, source cutoff, or
  scientific nonclaim that differs from the frozen campaign.

Runtime pending, running, operational failure, cancellation, and verified
product states are recorded later by execution receipts and product indexes.
They never mutate this planning proof.
