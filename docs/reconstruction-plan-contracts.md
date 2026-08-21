# First-party reconstruction plan

`SyntheticInfillPlanV1` is the public planning boundary for the v2.5 tick
reconstruction product. It resolves installed scientific artifacts, inventories
immutable ASCII tick partitions, performs compatibility and information-safety
preflight, estimates resources, and emits bounded
`ReconstructionWorkflowRequestV1` batches. It does not generate reconstructed
ticks; the resulting requests are consumed by the installed first-party
Temporal runtime and stage handlers.

The current executable source is only HistData.com ASCII/T. Provider-neutral
dataset, evidence, and cross-series contracts preserve future adapter seams,
but OANDA, other providers, and live broker feeds are later-milestone work and
are not admitted by this plan.

The scientific claim is deliberately narrow:

> Output is a plausible counterfactual ensemble conditioned on declared
> artifacts and constraints; it is not recovered historical truth.

Observed ASCII ticks remain immutable anchors. Their bid, ask, timestamp,
monthly partition, and zero-based Arrow row ordinal are never replaced or
renumbered. Synthetic ticks may only be added around those anchors. M1, bar,
OHLC, and other aggregated products are not accepted as planning inputs; bars
are downstream projections from committed final ticks.

## Contracts and execution handoff

| Public API | Responsibility |
| --- | --- |
| `ReconstructionSourcePartitionV1` | Strong hash, monthly ownership, Arrow row count, timestamp bounds, feed-epoch evidence, and immutable row-identity policy for one ASCII/T cache. |
| `ReconstructionSourceInventoryV1` | Complete Cartesian inventory of the synchronized EURUSD/GBPUSD/EURGBP triangle over the selected common periods. |
| `ReconstructionScientificLedgerV1` | Content-addressed estimand, assumptions, context-missingness taxonomy, generated-row constraints, validity label, and legacy replay policy. |
| `ReconstructionExperimentManifestV1` | Catalog revision, immutable partitions, roles, split/leakage policy, authoritative domain bindings, implementation identity, and limitations shared by the plan and product. |
| `ReconstructionPlanConfigurationV1` | Information, generation, carving, cross-currency, ensemble, storage, delivery, window, and handler policies. |
| `CrossSeriesConstraintPolicyV1` | Hash-bound alignment, tolerance, readiness, refusal, and artifact-size policy for synchronized source evidence; current admission is the complete HistData triangle only. |
| `ReconstructionPlanExecutionManifestV1` | Content-addressed artifact graph plus durable output/checkpoint roots and disposable scratch root. |
| `ReconstructionPlanRefusalV1` | Stable, bounded reason that a planned window cannot execute. Refused windows never become workflow tasks. |
| `ReconstructionPlanResourceSummaryV1` | Source size, candidate amplification, peak memory/scratch, retained output, partition, window, member, and request estimates. |
| `SyntheticInfillPlanV1` | Deterministic top-level identity, requests, resources, refusals, delivery mode, information mode, nonclaim, and anchor policy. |
| `FinalAdaptiveSupportMapIndexV1` | Frozen-candidate root over independently replayed partition/window evidence, exact terminal census, selected engine/scenario IDs, and bounded verification shards. |
| `load_reconstruction_stage_plan()` | Public loader used by a stage handler to resolve its exact execution manifest, configuration, source inventory, and input references. |

Every stage command carries one strong reference to the execution manifest.
The stage loader verifies that reference, reads the public configuration and
source inventory, verifies the catalog/resolution/experiment graph, checks the
handler name and stage-specific artifact kinds,
and rejects any input not present in the execution graph. No dataframe, tick
row, or process-private configuration object is placed in Temporal history.
Artifact, output, checkpoint, and scratch roots must be non-overlapping and
must remain outside the immutable ASCII/T source tree. Execution manifests also
reject durable input artifacts placed beneath scratch, so cleanup cannot cross
an ownership boundary. The manifest contains strong references to the
scientific ledger, point-in-time evidence policy, and cross-series constraint
policy, and all three IDs participate in run configuration identity. Source
enrichment classifies every bounded market-context and CFTC query against the
ledger taxonomy. Validation re-derives those states before product publication
rather than trusting a carried label.
Requests are split by ensemble member and bounded window chunks because
different members share window boundaries and therefore cannot coexist in one
request whose task cores must not overlap.

Modern-reference delivery is the v2.5 public mode. The orchestration stage
named `broker_transfer` becomes a deterministic identity delivery projection
with no broker input. Broker-conditioned/OANDA selection is rejected by public
compatibility and remains a later feed-qualified milestone.

## Ex-post construction from installed artifacts

This example uses the qualified artifacts produced by #460-#464 and #468. If
`start_period` and `end_period` are omitted, the builder selects the full
continuous period range common to all three symbols.

```python
from pathlib import Path

from histdatacom.synthetic import (
    InformationMode,
    build_synthetic_infill_plan,
    load_reconstruction_stage_plan,
    validate_synthetic_infill_plan_for_execution,
    write_synthetic_infill_plan,
)

repo = Path.cwd()
analytics = repo / "data/.histdatacom/analytics"
motifs = analytics / "modern-reference-motif-issue-464-final-v4"
work = repo / ".histdatacom/reconstruction-plan-v2.1"

plan = build_synthetic_infill_plan(
    repo / "data/ASCII/T",
    feed_epoch_definition_path=(
        analytics
        / "feed-epochs-v2-issue-460/feed-epochs-v2-definition.json"
    ),
    observation_operator_path=(
        analytics
        / "observation-calibration-v2-issue-462/"
        "observation-calibration-v2-operator.json"
    ),
    market_context_corpus_path=next(
        (repo / ".histdatacom/market-context-461-final-v4").glob(
            "market-context-corpus-*.json"
        )
    ),
    cftc_positioning_corpus_path=(
        analytics
        / "cftc-positioning-issue-468-final/"
        "cftc-positioning-corpus-"
        "887a47840090cdab1982fe910a4bdf8c1fcc9af256ab687bceae1b8dd1cbd3e0.json"
    ),
    benchmark_manifest_path=next(
        (analytics / "reverse-degradation-benchmark-issue-463-final-v5").glob(
            "reverse-degradation-manifest-*.json"
        )
    ),
    motif_manifest_path=next(motifs.glob("modern-reference-motif-manifest-*.json")),
    motif_index_path=next(motifs.glob("modern-reference-motif-index-*.json")),
    motif_qualification_path=next(
        motifs.glob("modern-reference-motif-qualification-*.json")
    ),
    motif_leakage_audit_path=next(
        motifs.glob("modern-reference-motif-leakage-audit-*.json")
    ),
    artifact_root=work / "artifacts",
    output_root=work / "output",
    checkpoint_root=work / "checkpoints",
    scratch_root=work / "scratch",
    information_mode=InformationMode.EX_POST_RECONSTRUCTION,
)

validate_synthetic_infill_plan_for_execution(plan)
plan_ref = write_synthetic_infill_plan(plan, work / "artifacts")

# The exact public object that a #466 stage handler consumes.
first_command = plan.workflow_requests[0].tasks[0].commands[0]
stage_plan = load_reconstruction_stage_plan(first_command)
assert stage_plan.configuration.configuration_id == plan.configuration_id
assert "experiment_manifest" in stage_plan.execution_manifest.artifacts
assert "scientific_ledger" in stage_plan.execution_manifest.artifacts
```

Supplying `source_root` uses the documented v2.3 translation: feed-epoch hashes
are verified, a local HistData catalog is compiled, and the same immutable
resolution/experiment path continues. New callers pass `source_root=None`,
`dataset_catalog_path=<catalog>`, and `dataset_reference=<alias-or-version>`.
The reference is frozen before plan identity is computed.

The persisted plan and its companion artifacts are content addressed. Repeated
construction with the same files, policies, roots, and period selection has
the same IDs. Source file paths are retained for execution, while scientific
source identity is based on catalog/version/revision, digest, size, row count,
role, split, evidence, and period semantics. See
[`reconstruction-experiment-contracts.md`](reconstruction-experiment-contracts.md).
The authoritative estimand and migration rules are in
[`reconstruction-scientific-ledger.md`](reconstruction-scientific-ledger.md).

New v2.5 plans selecting marked Hawkes also bind a content-addressed
`ObservationUncertaintyPolicyV1`. The planner uses its low-retention endpoint
and admission quantile for adaptive cardinality and requires three retained
members so high, central, and low scenarios each have a path. Runtime may not
replace the admitted scenario with a less demanding one. See
[`observation-process-uncertainty.md`](observation-process-uncertainty.md).

Retained v2.4 plans remain readable and identity-verifiable, but are
`legacy-unbound`. Current execution refuses them until they are replanned from
their original inputs, producing new experiment, run, plan, and scientific
ledger bindings without rewriting the retained identity.

## Ex-ante construction and fail-closed behavior

Ex-ante mode always uses zero right look-ahead and requires artifacts that were
trained and available before the requested period. Merely changing the mode on
the current full-history fitted artifacts is intentionally rejected:

```python
from histdatacom.synthetic import (
    InformationMode,
    ReconstructionPlanCompatibilityError,
    build_synthetic_infill_plan,
)

try:
    ex_ante_plan = build_synthetic_infill_plan(
        repo / "data/ASCII/T",
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        start_period="202001",
        end_period="202001",
        **the_same_artifact_and_root_arguments,
    )
except ReconstructionPlanCompatibilityError as error:
    assert "observe the requested future" in str(error)
```

A successful ex-ante plan therefore requires a point-in-time feed-epoch
definition, observation operator, motif index, context vintages, and CFTC
states whose training/availability boundaries precede the requested window.
The information audit is run over every ensemble member's complete synchronized
window plan and rejects unavailable vintages, future observations, future
motif selection, or any undeclared look-ahead.

## Refusals and resource preflight

Planning distinguishes an incompatible product from a scientifically
unsupported window. Hash mismatches, partial triangles, discontinuous periods,
wrong schemas, unstable/unqualified dependencies, leakage, broker-mode errors,
and quota overflow fail the whole build. Qualified source periods that lack a
supported feed assignment, market context, or sufficiently fresh CFTC state
remain visible as deterministic window refusals. At least one executable
window is required.

`plan.to_dry_run_json()` returns the bounded operational view: request chunks,
artifact digests and sizes, resources, and refusals without expanding source
rows. `plan.resources` records both per-concurrent-window peaks and retained
ensemble output estimates. Storage-policy preflight occurs before a workflow
request can be submitted.

The preliminary planner support map is not the final execution proof. Before a
full campaign, `build_final_adaptive_support_map()` independently rereads the
Arrow partitions, reconstructs terminal decisions and resource estimates, and
binds the result to the frozen release candidate. See
[`final-adaptive-support-verification.md`](final-adaptive-support-verification.md).
