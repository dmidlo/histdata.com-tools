# Temporal reconstruction orchestration

The production reconstruction control plane runs period-scale work as one
`ReconstructionRunWorkflow` with bounded `ReconstructionWindowWorkflow`
children. Temporal orders and retries the work. Market events, augmented
frames, candidate surfaces, fitted statistics, and Parquet data never enter a
workflow argument, result, query, or heartbeat.

The workflow-visible chain is:

```text
source/enrichment
  -> proposal
  -> carving
  -> cross-series reconciliation
  -> delivery projection
  -> validation/staging
  -> atomic partition commit
```

Each arrow is an artifact boundary. A stage handler reads declared input and
configuration `ArtifactRef` values, operates on data outside Temporal, writes
its outputs, and returns one bounded `ReconstructionStageOutcomeV1`. The
orchestrator verifies the output bytes and atomically writes that outcome as a
stage receipt before advancing the checkpoint.

## Contracts

| Contract | Responsibility |
| --- | --- |
| `ReconstructionWorkflowRequestV1` | Run identity, bounded window tasks, queue names, status/report roots, and concurrency limits. |
| `ReconstructionWindowTaskV1` | One all-symbol window, resource estimate, scratch scope, and the complete ordered command list. |
| `ReconstructionStageCommandV1` | Handler name, strong input/configuration references, and a receipt path below that window's scratch directory. |
| `ReconstructionStageInvocationV1` | Activity-side handler context, exact input fingerprint, heartbeat hook, and cancellation check. |
| `ReconstructionStageOutcomeV1` | Status, output references, event counts, scratch/output bytes, and deterministic outcome identity. |
| `ReconstructionWindowStateV1` | Existing reconstruction checkpoint plus a strict prefix of completed stage outcomes. |
| `ReconstructionRunReportV1` | Reconciled workflow/storage status and committed manifest references. |

The request has a hard one-megabyte serialized ceiling and a 512-window
ceiling. Artifact references must include byte size and lowercase SHA-256.
Inline keys such as `rows`, `events`, `records`, `table`, or `dataframe` are
rejected even when hidden inside artifact metadata.

## First-party data-plane adapters

Scientific code runs only in activity workers. Default worker construction
idempotently registers the versioned handlers emitted by the first-party plan:

| Stage | First-party action |
| --- | --- |
| source/enrichment | Hash-verifies ASCII/T Arrow partitions, preserves complete source-row identity, compiles point-in-time quality and synchronized cross-series constraint sidecars from the HistData core window, and materializes input/core streams plus context. |
| proposal | Selects an explicitly supported cross-series synchronization instant, queries the qualified modern motif index, and writes candidate rows plus constraint-use evidence to a content-addressed ledger. |
| carving | Applies the declared hard/advisory and point-in-time constraints, writes quality and cross-series use decisions to a content-addressed ledger, and materializes accepted core streams. |
| cross-series reconciliation | Rechecks constraint readiness and reconciles the complete EURUSD/GBPUSD/EURGBP group using exact nanosecond event-time support without forward filling. |
| delivery projection | Applies explicit modern-reference identity delivery; it never invents a broker fingerprint. |
| validation | Rechecks cross-instrument output, synchronized constraint use, benchmark qualification, motif leakage, information safety, immutable anchors, retention, and storage before staging and durable lineage binding. |
| atomic commit | Promotes or recovers the complete v2 Parquet transaction. |

Applications do not register these handlers themselves. A deliberately custom
worker may still register a different, separately versioned adapter before
constructing a worker with an explicit activity list:

```python
from histdatacom.orchestration import register_reconstruction_stage_handler


def proposal_handler(invocation):
    # Read invocation.command.input_manifest_refs from artifact storage.
    # Stream bounded batches through the existing pure proposal code.
    # Write the result before returning its strong ArtifactRef.
    output_ref = build_proposal_artifact(invocation)
    return invocation.completed(
        output_refs=(output_ref,),
        observed_event_count=observed_count,
        candidate_event_count=candidate_count,
        scratch_bytes=scratch_bytes,
        output_bytes=output_ref.size_bytes or 0,
    )


register_reconstruction_stage_handler("proposal-v3", proposal_handler)
```

Handler registration remains process-local by design. `handler_name` is
deterministic command metadata; the callable is outside workflow history.
Configuration artifact hashes and the command ID bind the selected behavior.

Long-running handlers call `invocation.heartbeat(...)` at bounded batch
intervals and check `invocation.cancellation_requested` before producing more
work. Those hooks expose phase, window, counts, scratch/output bytes, and the
inputs needed to calculate rate/ETA without carrying rows.

## Recovery and idempotency

Every window state is stored in the existing manifest/status SQLite database.
`compare_and_swap_job_snapshot()` acquires an immediate SQLite transaction and
replaces a state only when its expected snapshot ID is still current. The same
write succeeds idempotently. A stale worker with different evidence fails
closed.

This closes the important worker-loss interval:

```text
stage output written
  -> stage receipt atomically written
  -> worker dies before checkpoint
  -> Temporal retries activity
  -> receipt and every output hash are verified
  -> outcome is reused
  -> checkpoint advances once
```

Atomic publication also closes the later loss interval:

```text
validated manifest mirror and transaction descriptor written
  -> staging directory atomically renamed into commits
  -> worker dies before the commit receipt
  -> retry verifies the byte-identical manifest mirror
  -> descriptor locates the expected committed identity
  -> committed Parquet and manifest are fully reverified
  -> the commit receipt and checkpoint advance once
```

The transaction descriptor is not the staged phase artifact. The staged phase
reference is a durable byte-identical mirror of the product manifest, so its
SHA-256 must equal the committed manifest even after the rename removes the
original staging path.

If the checkpoint was already advanced, the strict outcome prefix resolves a
duplicate completion to the newer state. A different receipt, input
fingerprint, output hash, or checkpoint branch is rejected.

The window checkpoint follows the existing protocol:

```text
planned -> running -> staged -> validated -> committed
```

Only the validation outcome may supply exactly one `commit_phase=staged`
manifest. Only the atomic-commit outcome may supply exactly one
`commit_phase=committed` manifest with identical manifest bytes. Since the
window always contains the full sorted symbol group, a subset cannot reach the
committed phase independently.

## Backpressure and resource refusal

The parent creates deterministic waves constrained by both:

- `max_parallel_windows`; and
- the sum of admitted `estimated_memory_bytes`.

Stages within a window are strictly sequential, so a producer cannot outrun
its next consumer. `max_parallel_windows` may not exceed the storage policy's
in-flight-batch limit. A task above the run or lane limit is placed alone in a
preflight wave; its activity records a durable failed/refused checkpoint
without invoking a scientific handler.

After admission, each outcome is checked against candidate, scratch, and
output limits. Underestimated actual use fails the window before another stage
starts.

The existing lanes are sufficient:

- parent and child workflow decisions use `orchestration`;
- window activities, stage handlers, storage verification, and report
  reconciliation use `cpu_file`.

No new unbounded queue or implicit worker pool is introduced.

## Cancellation and scratch

Cancellation is checked between stages and is visible inside handlers. The
last valid state becomes `cancelled`, then only that task's explicitly scoped
scratch directory is removed. The cleanup helper rejects symlinks, files, the
home directory, and `/`. Request validation also rejects nested/shared window
scratch trees, overlap with manifest or report storage, and durable stage inputs
placed below disposable scratch.

Because cancellation cleanup removes every uncommitted receipt and artifact in
the window scratch tree, resume discards the entire disposable stage prefix and
rebuilds the window from its immutable inputs. It never retains references to
deleted scratch files. Committed manifests are terminal and are never removed
by cancellation cleanup.

## Final reconciliation

`reconstruction_report` receives the bounded request only; it reloads each
window checkpoint from the manifest store instead of fanning every child state
back into one large Temporal activity payload. For every committed window it:

1. verifies the committed manifest `ArtifactRef` bytes;
2. runs `verify_reconstruction_publication()` over the manifest and Parquet;
3. matches run, window, synchronization unit, and complete symbol set;
4. sums observed and synthetic event counts from storage evidence; and
5. sums stage runtimes once and retains maxima for RSS, scratch, and candidate
   amplification instead of mistaking final commit telemetry for the whole
   window; and
6. atomically writes one compact run report and records it in the manifest
   store.

The report activity heartbeats between windows. A committed checkpoint without
a valid matching publication fails report reconciliation.

## Submission and worker registration

`submit_reconstruction_request()` adds the workspace task-queue map, starts
`ReconstructionRunWorkflow` with a deterministic workflow ID, and records the
submission in the manifest store. Default workers register:

- `ReconstructionRunWorkflow`;
- `ReconstructionWindowWorkflow`;
- `reconstruction_window`; and
- `reconstruction_report`.

They also install all seven first-party stage handlers before accepting
activities; no application-level registry setup is required.

Both workflows validate in Temporal's sandbox. The activity-side module is
passed through the sandbox import boundary and is never called from workflow
decision code.

## Fault contract

The orchestration tests inject and verify:

- output/receipt completion followed by worker loss before checkpoint;
- stage timeout followed by restart at the last completed stage;
- duplicate checkpoint completion;
- stale checkpoint branches;
- corrupt receipts and changed output bytes;
- cancellation with scoped scratch cleanup;
- resource and lane refusal;
- memory-weighted producer/consumer backpressure;
- manifest-store re-open after a process/server-style restart; and
- final workflow/storage scope and count reconciliation.

The real-artifact integration gate additionally runs a qualified synchronized
triangle through the first-party handlers, compares logical and physical
hashes across concurrency settings, injects termination after the atomic
rename, injects cancellation, and proves a failing qualification prevents
commit. Set `HISTDATACOM_REAL_RECONSTRUCTION_PLAN` to a #465 plan artifact to
run that gate; no scientific handler is mocked.
