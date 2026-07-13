# Reconstruction streaming contracts

The version-one reconstruction streaming contracts define how bounded event
batches move between synthetic reconstruction stages without persisting the
521-column analytical frame as a second permanent dataset. Enrichment,
candidate surfaces, and rejected rows remain process-local or quota-managed
scratch data. Workflow history contains compact metadata and `ArtifactRef`
values only.

This is a contract layer. It does not generate events, implement production
Temporal workflows, or publish the final Parquet product.

The implemented [feed-epoch contracts](feed-epoch-contracts.md) are one of its
bounded semantic inputs. A reconstruction run binds the compact epoch
definition ID, and streamed windows or final events carry only their epoch or
uncertain-transition assignment. Fingerprint payloads, fitting panels, and
sensitivity intermediates never become permanent per-row columns.

The implemented
[historical feed-observation operator](observation-operator-contracts.md) is a
second bounded semantic input. Runs bind its operator ID in
`configuration_ids`; window workers use the existing ownership/halo rules and
persist only larger output/carry batches behind `ArtifactRef` values. Fitted
panels and `ObservationApplicationResultV1` objects never enter workflow
history.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `ReconstructionRunV1` | Semantic sources, configuration, members, symbols, and partition-independent seed namespace. |
| `ReconstructionWindowV1` | One synchronized symbol group, half-open generation interval, left halo, and right look-ahead. |
| `EventBatchV1` | Counts, time bounds, logical content hash, and a strong artifact reference; never event rows. |
| `CarryStateV1` | Per-symbol watermarks, last-event IDs, and references to larger carry artifacts. |
| `RejectionSummaryV1` | Reconciled aggregate rejection counts without rejected candidate rows. |
| `PartitionManifestV1` | One all-symbol synchronization unit whose batch counts reconcile by symbol. |
| `ReconstructionCheckpointV1` | Chained recovery state, completed batch IDs, watermarks, and publication references. |
| `ReconstructionStoragePolicyV1` | Candidate, batch, memory, scratch, output, ensemble, heartbeat, and checkpoint limits. |
| `ReconstructionResourceEstimateV1` | The complete estimate retained when resource preflight accepts or refuses work. |
| `ReconstructionHeartbeatV1` | Bounded progress, storage use, cancellation intent, and last-valid-checkpoint identity. |

All version-one readers verify schema versions and deterministic IDs. Unknown
JSON envelope keys may be ignored by individual readers, but derived IDs,
counts, phases, and advertised references fail closed when they do not
reconcile.

## Semantic identity and deterministic seeds

`ReconstructionRunV1.run_id` includes only inputs that may affect scientific
output: source versions, configuration IDs, ensemble members, symbols, and the
base seed. It deliberately excludes the storage policy, worker count, retry
count, batch size, checkpoint cadence, and window plan.

`seed_for(member, semantic_key)` derives a 64-bit seed from stable semantic
lineage such as an anchor interval or motif decision. A window ID, worker ID,
attempt number, or scratch path is not a valid semantic key. Consequently,
changing legal execution parallelism or storage tuning does not change event
identity or generated values.

An `EventBatchV1` ID uses logical batch scope and content evidence. The
worker-local artifact path and optional artifact metadata are excluded from
logical identity, so a retry can place identical bytes at a new scratch path
without creating a different batch. Checkpoints retain the exact artifact
paths because recovery must know which physical artifacts to validate or
clean.

## Window ownership, halo, and look-ahead

Each window owns exactly:

```text
[core_start_ns, core_end_ns)
```

Only the owning window may generate an event at a timestamp. Its readable
input interval is:

```text
[core_start_ns - left_halo_ns,
 core_end_ns + right_lookahead_ns)
```

Halo and look-ahead observations provide seam, anchor, and synchronized-pair
context but never grant generation ownership. The planner produces contiguous
windows over the complete symbol group, and plan validation rejects gaps,
overlaps, member drift, run drift, or symbol-group drift. This gives later
generators an explicit edge contract while keeping the scientific need for
future anchors visible.

Right look-ahead is also an information-access channel. The implemented
[`reconstruction-information-modes.md`](reconstruction-information-modes.md)
contract binds the exact window-plan identity into the information manifest.
Its pre-generation audit requires zero right look-ahead for ex-ante simulation
and checks ex-post windows against their declared policy limit.

Every event batch repeats its half-open ownership bounds and rejects a first or
last event time outside them. A halo observation can therefore inform a batch
but cannot be mislabeled as output owned by that batch.

The planner has no worker-count argument. Different legal window sizes produce
different transport windows but preserve single ownership of every event time
and use the same semantic seed namespace.

## Resource preflight and backpressure inputs

Before a window is admitted, its estimate records:

- input and proposed candidate event counts;
- peak events per batch and total estimated batch count;
- retained ensemble-member count;
- maximum simultaneously in-flight batches;
- estimated peak memory, scratch, and durable output bytes.

The storage policy checks every estimate together and raises
`ReconstructionResourceLimitError` with the rejected estimate and every
violated limit. This is an early refusal, not an out-of-memory recovery path.
The policy also declares maximum events per batch, checkpoint and heartbeat
cadence, checkpoint payload bytes, removal of uncommitted scratch on
cancellation, and the committed-only publication rule.

Production backpressure belongs to #447, but it must enforce these frozen
limits rather than inventing an unrelated queue policy.

## Checkpoints, retries, and duplicate delivery

Checkpoints form an optimistic-concurrency chain. Every transition names the
exact current checkpoint ID and produces the next revision with the current ID
as its parent. A transition from an older checkpoint therefore fails as stale
instead of overwriting newer progress.

Completed batch IDs are a bounded set of logical identities. On recovery,
`pending_batches()` deduplicates repeated delivery and returns only unfinished
batches. Watermarks may advance or remain fixed but cannot move backward.
This supports process-loss recovery without advertising or appending the same
logical events twice.

Cancellation and failure checkpoints retain enough bounded evidence to resume
but expose no committed product. Resuming transitions back to `running`, keeps
completed batch IDs and watermarks, and clears the stale staged reference after
the caller performs the policy-mandated scratch cleanup.

## Two-phase publication protocol

The checkpoint state machine is:

```text
planned -> running -> staged -> validated -> committed
   |          |          |          |
   +----------+----------+----------+-> cancelled / failed
                                      cancelled / failed -> running
```

The phase meanings are strict:

1. `staged` references a temporary manifest that is not discoverable.
2. `validated` retains that same temporary reference after schema, count,
   checksum, and synchronization-unit validation.
3. `committed` requires a promoted manifest whose SHA-256 and byte size match
   the validated staging artifact.
4. `advertised_manifest_ref` is `null` in every phase except `committed`.
5. Repeating the same final commit is a no-op; attempting to replace the
   committed manifest fails.

The state machine specifies the required behavior but does not perform the
filesystem operation. #446 owns temporary paths, Parquet validation, checksums,
atomic rename/promotion, final layout, discovery, and cleanup. A #446 writer
must not record the `committed` checkpoint until its atomic promotion has
succeeded.

## Cross-symbol synchronization

A reconstruction window contains the complete sorted symbol group and has one
`synchronization_unit_id`. Every event batch and the partition manifest must
match that run, member, window, and synchronization unit. The manifest carries
an explicit count for every symbol, including zero-event symbols, and validates
those counts against all referenced batches.

The manifest is therefore the smallest commit unit for later triangular and
inverse consistency work. A subset of the symbols cannot be published as
though the synchronized unit succeeded.

## Heartbeat and Temporal payload rules

`ReconstructionHeartbeatV1` carries phase, bounded progress counts, event
counts, scratch/output bytes, cancellation intent, and the last checkpoint ID.
It states that cancellation stops future work and resume begins from the last
valid checkpoint. It contains no dataframe, event list, candidate rows, or
statistics surface and has a hard serialized-size ceiling.

`ArtifactRef` values used by reconstruction contracts require a non-empty
kind/path, non-negative byte size, and lowercase SHA-256 digest. Artifact
metadata is bounded and rejects inline `rows`, `records`, `events`, `table`, or
`dataframe` keys. Large carry state, batches, manifests, and rejection detail
must be written to artifact storage.

## Issue boundaries

- #433's implemented information-mode contracts and leakage audit govern what
  these windows may read and what downstream claims are valid.
- #434's implemented feed-epoch contract supplies only stability-passing,
  uncertainty-aware observation-regime assignments and complete lineage.
- #435's implemented observation operator consumes those assignments and uses
  these windows, alignment rules, limits, and carry seams for deterministic
  forward observation and controlled degradation.
- #436 implements the streaming reverse-degradation benchmark and scorecards
  over that operator interface; see
  [`reverse-degradation-benchmark-contracts.md`](reverse-degradation-benchmark-contracts.md).
- #439 generates variable-cardinality candidate events using these windows,
  seeds, budgets, and carry contracts.
- #440 owns hard historical carving and detailed rejection decisions.
- #441 owns cross-currency generation and reconciliation semantics.
- #446 implements final atomic Parquet and manifest publication.
- #447 maps the contracts onto production Temporal workflows and activities.

Changing a required field, identity rule, ownership interval, phase meaning,
or retry guarantee requires a new schema version and contract class.
