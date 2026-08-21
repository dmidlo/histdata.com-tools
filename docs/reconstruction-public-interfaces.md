# Public reconstruction CLI and Python API

The supported reconstruction boundary is the installed
`histdatacom reconstruction` command family and
`histdatacom.reconstruction.ReconstructionClient`. Both surfaces consume the
same content-addressed `SyntheticInfillPlanV1`, explicit operator request, and
operation receipt contracts. Neither surface accepts tick rows, passwords, or
large analytical frames in flags or workflow control metadata.

## Supported input and scientific acknowledgement

The current v2.5 compatibility boundary accepts only:

- HistData ASCII tick caches below an `ASCII/T` source root;
- the complete `EURGBP`, `EURUSD`, `GBPUSD` synchronized triangle;
- `ex_post_reconstruction` or `ex_ante_simulation`, selected explicitly; and
- modern-reference delivery.

Provider-neutral domain contracts remain the architectural foundation, but
alternate providers, OANDA, live broker inputs, and broker-conditioned
delivery are later-milestone work and are non-executable here. Existing broker
research contracts are not current dataset qualification. M1, OHLC, bar, and
partial-triangle requests are unsupported and exit as invalid plans. Every
execution request must also carry the exact
machine-readable scientific nonclaim and a true acknowledgement:

> Output is a plausible counterfactual ensemble conditioned on declared
> artifacts and constraints; it is not recovered historical truth.

The acknowledgement records operator intent. It does not weaken information
audits, validation, immutable-anchor checks, or certification gates.

Inspect the installed scientific target or verify a retained ledger before
planning:

```sh
histdatacom reconstruction --json science
histdatacom reconstruction science --ledger work/scientific-ledger.json
```

The equivalent typed call is `ReconstructionClient.scientific_ledger()`. The
result exposes the ledger/estimand IDs, exact assumptions, context-missingness
definitions, generated-row constraints, strategy-validity label, current
HistData-only scope, and v2.4 legacy-unbound migration rule. See
[`reconstruction-scientific-ledger.md`](reconstruction-scientific-ledger.md).

## Construct a plan and request

Discover the installed substrate and audit a JSON plan before constructing it:

```sh
histdatacom reconstruction schemas --json
histdatacom reconstruction engines --json
histdatacom reconstruction compatibility --plan plan-spec.json --json
```

Both commands use the same registry and compatibility engine consumed by
`ReconstructionClient.construct_plan()`. See
[`reconstruction-schema-compatibility.md`](reconstruction-schema-compatibility.md)
for field metadata, cache translations, and compatibility states.

New planning starts from a JSON `ReconstructionPlanSpecV2`. It explicitly
orders the proposal engines, selects the reconstruction-eligible product
engine, and binds retained evaluation evidence. The v1 input remains a
deprecated deterministic translation to a motif-only portfolio. A catalog
selector is preferred; the legacy `source_root` field invokes the documented
v2.3-to-v2.4 catalog translation. Paths point to strong, qualified artifacts;
data rows remain in their source artifacts.

```json
{
  "schema_version": "histdatacom.reconstruction-plan-spec.v2",
  "source_root": null,
  "dataset_catalog_path": "artifacts/histdata-dataset-catalog.json",
  "dataset_reference": "reconstruction-selected",
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
  "window_size_ns": 86400000000000,
  "proposal_engine_ids": [
    "histdatacom.marked-hawkes.diagonal_self_excitation"
  ],
  "selected_proposal_engine_ids": [
    "histdatacom.marked-hawkes.diagonal_self_excitation"
  ],
  "proposal_evaluation_paths": [
    "artifacts/reverse-degradation-scorecard-<sha256>.json"
  ],
  "qualification_dossier_path": "artifacts/powered-qualification-dossier-<sha256>.json",
  "hawkes_product_selection_dossier_path": "artifacts/hawkes-product-selection-dossier-<sha256>.json",
  "observation_uncertainty_policy_path": "artifacts/observation-uncertainty-policy-<sha256>.json",
  "feed_epoch_transition_policy_path": "artifacts/feed-epoch-transition-policy-<sha256>.json"
}
```

Portfolio order is not scientific rank. A powered dossier can only reduce the
eligibility granted by retained campaign evidence. The current powered dossier
makes diagonal and full self/cross excitation eligible; the frozen HistData
product portfolio deliberately selects only diagonal self-excitation. Failed,
underpowered, refused, and eligible-but-unselected engines remain inspectable,
but they cannot enter a committed product and never become a silent fallback.
Inspect resolved audits and evidence with:

```sh
histdatacom reconstruction --json portfolio \
  --plan work/plan-artifacts/synthetic-infill-plan-<sha256>.json
```

See
[`proposal-engine-portfolios.md`](proposal-engine-portfolios.md) for discovery,
single-engine evaluation, refusal, lineage, and no-fallback semantics.

Produce the exact powered dossier through either installed surface:

```sh
histdatacom reconstruction --json qualify \
  --evaluation work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json \
  --experiment work/experiment/reconstruction-experiment-<sha256>.json \
  --output-directory work/qualification
```

```python
dossier = ReconstructionClient().qualify_proposal_portfolio(
    "work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json",
    "work/experiment/reconstruction-experiment-<sha256>.json",
    output_directory="work/qualification",
)
```

The CLI and API call the same verifier and writer and therefore reproduce the
same content identity. See
[`powered-reconstruction-qualification.md`](powered-reconstruction-qualification.md)
for residual, score, power, holdout, and no-decision semantics, and
[`hawkes-product-selection.md`](hawkes-product-selection.md) for the separate
validation-only product choice. Freeze that choice through either installed
surface:

```sh
histdatacom reconstruction --json hawkes-select \
  --policy work/selection/hawkes-product-selection-policy-<sha256>.json \
  --comparison work/selection/hawkes-validation-comparison-<sha256>.json \
  --qualification work/qualification/powered-qualification-dossier-<sha256>.json \
  --output-directory work/selection
```

```python
selection = ReconstructionClient().select_hawkes_product(
    "work/selection/hawkes-product-selection-policy-<sha256>.json",
    "work/selection/hawkes-validation-comparison-<sha256>.json",
    "work/qualification/powered-qualification-dossier-<sha256>.json",
    output_directory="work/selection",
)
```

Freeze the required three-scenario observation policy separately:

```sh
histdatacom reconstruction --json observation-uncertainty-policy \
  --output-directory work/observation-uncertainty
```

```python
uncertainty_policy = (
    ReconstructionClient().create_observation_uncertainty_policy(
        output_directory="work/observation-uncertainty"
    )
)
```

The plan binds both the validation-only engine choice and this policy. Runtime
and campaign evidence keep the observation scenario ID and path seed as
separate machine-readable axes. See
[`observation-process-uncertainty.md`](observation-process-uncertainty.md).

Freeze the independent feed-boundary scenario policy too:

```sh
histdatacom reconstruction --json feed-epoch-transition-policy \
  --output-directory work/feed-epoch-transition
```

```python
transition_policy = (
    ReconstructionClient().create_feed_epoch_transition_policy(
        output_directory="work/feed-epoch-transition"
    )
)
```

The retained marked-Hawkes ensemble must cover the complete transition by
observation scenario cross-product. See
[`feed-epoch-transition-uncertainty.md`](feed-epoch-transition-uncertainty.md).

Retained evidence diagnostics use the same public boundary:

```sh
histdatacom reconstruction --json diagnostic-build \
  --spec work/diagnostic-spec.json \
  --output-directory work/diagnostics
histdatacom reconstruction --json diagnostic-list \
  --manifest work/diagnostics/reconstruction-diagnostic-publication-<sha256>.json
```

```python
publication = ReconstructionClient().publish_diagnostics(
    "work/diagnostic-spec.json",
    output_directory="work/diagnostics",
)
listing = ReconstructionClient().diagnostics(
    "work/diagnostics/reconstruction-diagnostic-publication-<sha256>.json"
)
```

JSON chart data works in the base install. SVG and PNG require the optional
`histdatacom[viz]` extra. Both surfaces verify the same strong evidence graph,
preserve explicit unavailable and underpowered states, and never reinterpret a
figure as an eligibility decision. Family and view counts remain distinct so
dense evidence is not forced onto one unreadable mixed axis. See
[`reconstruction-diagnostics.md`](reconstruction-diagnostics.md).

The omitted `evidence_policy` and `cross_series_constraint_policy` fields use
their versioned HistData-only defaults. Supplying either field serializes the
complete v1 policy object, changes plan/run identity, and is compatibility
checked before source scanning. Merely adding `oanda` or another provider to a
policy is rejected; a future provider requires a separately implemented and
qualified adapter milestone.

The selected catalog reference is resolved to an exact dataset version,
revision, query scope, and partition set before plan identity is computed. The
plan graph retains the catalog, resolution receipt, and experiment manifest;
the committed product repeats the experiment ID in `source.experiment_id`.
See
[`reconstruction-experiment-contracts.md`](reconstruction-experiment-contracts.md).

Frozen experiments are discoverable and independently verifiable without
publishing local paths:

```sh
histdatacom reconstruction --json experiment-list --root work/plan-artifacts
histdatacom reconstruction --json experiment-inspect \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
histdatacom reconstruction --json experiment-verify \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
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
Each shard preserves the four operator-supplied storage bases: plan artifacts
remain below `artifact_root`, while output, checkpoints, and scratch use stable
shard children below their respective roots. Output and scratch must be on the
same filesystem for atomic publication; see the
[complete campaign runbook](reconstruction-campaign-runbook.md) for mount,
storage, execution, crash/restart, product-index, and dataset-publication gates.

Full campaigns add a second, independent support pass after the claimed support
map is built:

```sh
histdatacom reconstruction --json support-verify \
  --plan-set work/plan-artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --release-candidate work/release/reconstruction-release-candidate-<sha256>.json \
  --output-directory work/final-support
```

`ReconstructionClient.construct_final_adaptive_support_map()` is the equivalent
typed API. It rereads raw partitions and independently reconstructs ownership,
alignment, feed/context/CFTC decisions, modeled cardinality, and resources. The
returned `FinalAdaptiveSupportMapIndexV1` is the support reference supplied to
`request-set`; every child execution request binds that exact reference. See
[`final-adaptive-support-verification.md`](final-adaptive-support-verification.md).

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
science = client.scientific_ledger()
assert science["ledger_id"].startswith("reconstruction-scientific-ledger:")
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
