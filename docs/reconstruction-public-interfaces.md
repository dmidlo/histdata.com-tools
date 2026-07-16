# Public reconstruction CLI and Python API

The supported reconstruction boundary is the installed
`histdatacom reconstruction` command family and
`histdatacom.reconstruction.ReconstructionClient`. Both surfaces consume the
same content-addressed `SyntheticInfillPlanV1`, explicit operator request, and
operation receipt contracts. Neither surface accepts tick rows, passwords, or
large analytical frames in flags or workflow control metadata.

## Supported input and scientific acknowledgement

Version 2.1 accepts only:

- HistData ASCII tick caches below an `ASCII/T` source root;
- the complete `EURGBP`, `EURUSD`, `GBPUSD` synchronized triangle;
- `ex_post_reconstruction` or `ex_ante_simulation`, selected explicitly; and
- modern-reference delivery by default, or broker-conditioned delivery only
  when a strong `broker_delivery_artifact_v1` reference is supplied.

M1, OHLC, bar, partial-triangle, and broker-only requests are unsupported and
exit as invalid plans. Every execution request must also carry the exact
machine-readable scientific nonclaim and a true acknowledgement:

> Output is a plausible counterfactual ensemble conditioned on declared
> artifacts and constraints; it is not recovered historical truth.

The acknowledgement records operator intent. It does not weaken information
audits, validation, immutable-anchor checks, or certification gates.

## Construct a plan and request

Planning starts from a JSON `ReconstructionPlanSpecV1`. Paths point to strong,
qualified artifacts; data rows remain in their source artifacts.

```json
{
  "schema_version": "histdatacom.reconstruction-plan-spec.v1",
  "source_root": "data/ASCII/T",
  "feed_epoch_definition_path": "artifacts/feed-epochs-v2-definition.json",
  "observation_operator_path": "artifacts/observation-operator.json",
  "market_context_corpus_path": "artifacts/market-context-corpus.json",
  "cftc_positioning_corpus_path": "artifacts/cftc-positioning-corpus.json",
  "benchmark_manifest_path": "artifacts/reverse-degradation-manifest.json",
  "motif_manifest_path": "artifacts/modern-reference-motif-manifest.json",
  "motif_index_path": "artifacts/modern-reference-motif-index.json",
  "motif_qualification_path": "artifacts/modern-reference-motif-qualification.json",
  "motif_leakage_audit_path": "artifacts/modern-reference-motif-leakage-audit.json",
  "artifact_root": "work/plan-artifacts",
  "output_root": "work/output",
  "checkpoint_root": "work/checkpoints",
  "scratch_root": "work/scratch",
  "information_mode": "ex_post_reconstruction",
  "delivery_mode": "modern_reference",
  "start_period": "201101",
  "end_period": "201101",
  "requested_start_ns": null,
  "requested_end_ns": null,
  "window_size_ns": 86400000000000
}
```

`window_size_ns` is a positive execution bound. Use smaller synchronized
windows when dense monthly inputs would exceed the declared memory, scratch,
or output policy; it changes plan and run identity and is never a hidden
runtime override.

`requested_start_ns` and `requested_end_ns` are an optional paired half-open
UTC interval for bounded representative-window campaigns. When present, they
must agree with any supplied `start_period` and `end_period`; the planner still
inventories and hashes every complete monthly source partition touched by the
interval. Omitting them preserves whole-month planning.

```sh
histdatacom reconstruction --json plan --spec plan-spec.json

histdatacom reconstruction --json request \
  --plan work/plan-artifacts/synthetic-infill-plan-<sha256>.json \
  --information-mode ex_post_reconstruction \
  --acknowledge-scientific-nonclaim \
  --output work/execution-request.json

histdatacom reconstruction --json preflight \
  --request work/execution-request.json
```

Ranges whose safe execution window would make one plan exceed the 64 MiB plan
artifact bound use the public plan-set surface. The source specification keeps
the exact full range and resource-safe `window_size_ns`; the command starts
with bounded month groups and deterministically bisects them on execution-plan,
retention, or artifact-size preflight failures:

```sh
histdatacom reconstruction --json plan-set \
  --spec full-range-plan-spec.json \
  --periods-per-shard 12

histdatacom reconstruction --json preflight-set \
  --plan-set work/plan-artifacts/reconstruction-plan-set-<sha256>.json
```

Every resulting shard is an ordinary `SyntheticInfillPlanV1` with its own
hash-verified inventory, request graph, refusals, and resource limits. A fully
unsupported span is represented by a refusal-only plan with no workflow
requests and zero work/output estimates; it preflights only when refusals are
explicitly allowed and cannot publish a product. Dense months may therefore
contain multiple exact-bound shards. The parent
`ReconstructionPlanSetV1` requires contiguous nanosecond bounds, stores strong
plan references, and aggregates sums only for total work and output while
retaining maxima for peak memory and scratch. Raw rows, bytes, and partitions
are de-duplicated by immutable partition identity when dense months split into
multiple shards. Fresh plan-set preflight hashes each unique stat-identified
artifact once, execution-validates every shard, and rejects changed, missing,
overlapping, or gapped content. Construction and preflight retain only compact
resource summaries and partition identities after each full shard is handled.
Large qualified context corpora are resolved once per unchanged device, inode,
size, modification-time, and change-time identity set during the operation.

Preflight hash-verifies the plan and its declared artifacts, validates that the
operator information mode matches the immutable plan, and emits the bounded
dry-run graph, resources, refusal reasons, and validation/qualification audit
references. A plan containing refused windows is not executable unless the
request was explicitly created with `--allow-refusals`; refusal evidence is
never discarded.

## Execute, inspect, cancel, and resume

`run` waits for Temporal completion by default. `--submit-only` returns after
submission. `--local` is an explicit in-process smoke/recovery path through the
same seven registered first-party handlers; it never becomes a silent fallback
for a failed Temporal submission.

```sh
# Start the local runtime, submit all plan batches, and wait.
histdatacom reconstruction --start-runtime --json run \
  --request work/execution-request.json \
  --receipt work/run-receipt.json

# Submit and return immediately.
histdatacom reconstruction --start-runtime --json run \
  --request work/execution-request.json \
  --submit-only \
  --receipt work/submission-receipt.json

# Explicit bounded local parity/recovery execution.
histdatacom reconstruction --json run \
  --request work/execution-request.json \
  --local --window-id <window-id> \
  --receipt work/local-receipt.json
```

Receipts bind every Temporal handle to the exact manifest/status-store root
used at submission. This prevents reconstruction status from accidentally
reading the generic runtime store. Control commands consume receipts:

```sh
histdatacom reconstruction --json status \
  --receipt work/submission-receipt.json --offline

histdatacom reconstruction --start-runtime --json cancel \
  --receipt work/submission-receipt.json \
  --reason "operator request" \
  --output work/cancel-receipt.json

histdatacom reconstruction --start-runtime --json resume \
  --receipt work/submission-receipt.json \
  --output work/resume-receipt.json
```

Resume does not reinterpret a reconstruction workflow as the legacy ETL
`RunRequest`. It keeps the immutable scientific request and checkpoint keys,
while assigning fresh deterministic parent and child Temporal identities to
the recovery attempt. Committed windows therefore replay idempotently and a
partially completed earlier wave cannot collide with completed child workflow
IDs.

## List, preview, and replay output

```sh
histdatacom reconstruction --json outputs \
  --request work/execution-request.json

histdatacom reconstruction --json preview \
  --manifest work/output/reconstruction-products/.../manifest.json \
  --limit 20

histdatacom reconstruction --json replay \
  --manifest work/output/reconstruction-products/.../manifest.json
```

`preview` is capped at 100 events. Each row includes observed/synthetic origin,
immutable-anchor lineage, generator/motif/reference identity, confidence, the
constraint-set identity, and an explicit accepted or immutable-anchor decision.
The preview also carries the product validation and constraint manifests plus
the replay logical-content hash. `replay` verifies committed files and rebuilds
the exact streams before reporting the reconciled event count and logical hash.

## Certify modern-reference evidence

The certification campaign is part of the installed command family:

```sh
histdatacom reconstruction --json certify \
  --spec evidence/campaign.json \
  --output-directory evidence/dossier
```

The campaign fixes the broker-neutral `modern_reference` /
`unconditioned_reference` release claim. It verifies every JSON evidence file's
SHA-256, schema version, and subject identity before extracting a scalar through
the JSON pointer declared by the campaign. Scalar values are not allowed inline
in the campaign specification. The command publishes the frozen campaign,
methodology report, canonical dossier JSON, human Markdown, and a bounded result
receipt.

An incomplete dossier exits as a scientific refusal (`3`), a measured gate
failure exits as validation failure (`5`), and `ready-for-promotion` or
`certified` exits successfully. Ordinary `dev` campaigns cannot include the
promotion-only coverage observation.

## Typed Python surface

```python
from histdatacom import ReconstructionClient
from histdatacom.reconstruction import (
    InformationMode,
    read_execution_request,
    read_plan_spec,
    write_operation_receipt,
)

client = ReconstructionClient()
plan_set_ref = client.construct_plan_set(
    read_plan_spec("full-range-plan-spec.json"), periods_per_shard=12
)
plan_set_preflight = client.preflight_plan_set(plan_set_ref.path)
request = client.create_request(
    "work/plan-artifacts/synthetic-infill-plan-<sha256>.json",
    information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    acknowledge_scientific_nonclaim=True,
)
preflight = client.preflight(request)
receipt = client.submit(request, wait=False)
write_operation_receipt(receipt, "work/submission-receipt.json")

restored = read_execution_request("work/execution-request.json")
outputs = client.outputs(restored)
preview = client.preview(outputs["outputs"][0]["manifest_path"], limit=20)
replay = client.replay(outputs["outputs"][0]["manifest_path"])
certification, certification_result = client.certify(
    "evidence/campaign.json",
    output_directory="evidence/dossier",
)
```

The facade also provides asynchronous `submit_async`, `inspect_async`, and
`cancel_async` methods for callers that already own an event loop.

## Exit codes

| Code | Name | Meaning |
| ---: | --- | --- |
| `0` | success | Plan/request created, preflight executable, submission/control accepted, status inspected, output verified, or certification is ready/certified. |
| `2` | invalid plan | Malformed/changed plan, unsupported schema, M1/bar/partial triangle, broker-only request, or invalid arguments. |
| `3` | refused | Scientific/resource refusal, missing nonclaim acknowledgement, or incomplete certification evidence. |
| `4` | runtime failure | Temporal/runtime connectivity or unexpected operational failure. |
| `5` | validation failure | Execution did not reach validated committed state or a measured certification gate failed. |

Unsupported requests and scientific refusals retain distinct reason codes in
JSON error/preflight output even when both stop execution before data-plane
work.
