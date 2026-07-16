"""Real-artifact closure gates for first-party reconstruction handlers.

Set ``HISTDATACOM_REAL_RECONSTRUCTION_PLAN`` to a #465 plan artifact.  These
tests never replace scientific handlers; they re-home only execution roots so
the committed products and injected failures remain isolated under pytest's
temporary directory.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from histdatacom.orchestration.reconstruction import (
    RegisteredReconstructionStageExecutor,
    ReconstructionCheckpointStore,
    ReconstructionStage,
    ReconstructionStageCommandV1,
    ReconstructionStageInvocationV1,
    ReconstructionStageOutcomeV1,
    ReconstructionStageStatus,
    ReconstructionWindowTaskV1,
    ReconstructionWorkflowRequestV1,
    run_reconstruction_window,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV2,
    ReconstructionRetentionPlanV1,
    discover_reconstruction_manifests,
    load_reconstruction_manifest,
    read_reconstruction_streams,
)
from histdatacom.synthetic.reconstruction_handlers import (
    atomic_commit_handler,
    carving_handler,
    cross_series_reconciliation_handler,
    delivery_projection_handler,
    proposal_handler,
    register_first_party_reconstruction_handlers,
    source_enrichment_handler,
    validation_handler,
)
from histdatacom.synthetic.reconstruction_plan import (
    ReconstructionPlanExecutionManifestV1,
    SyntheticInfillPlanV1,
    read_reconstruction_plan_execution_manifest,
    read_synthetic_infill_plan,
)
from histdatacom.synthetic.streaming import (
    ReconstructionCommitPhase,
    ReconstructionResourceEstimateV1,
    ReconstructionWindowV1,
)

_PLAN_ENV = "HISTDATACOM_REAL_RECONSTRUCTION_PLAN"
_START_ENV = "HISTDATACOM_REAL_RECONSTRUCTION_START"


@dataclass(frozen=True)
class _Case:
    plan: SyntheticInfillPlanV1
    execution: ReconstructionPlanExecutionManifestV1
    request: ReconstructionWorkflowRequestV1
    task: ReconstructionWindowTaskV1


class _InjectedWorkerTermination(RuntimeError):
    """Fault injection after validation and before the commit handler."""


class _StopBeforeCommitExecutor:
    def __init__(self) -> None:
        self.delegate = RegisteredReconstructionStageExecutor()

    async def execute(
        self, invocation: ReconstructionStageInvocationV1
    ) -> ReconstructionStageOutcomeV1:
        if (
            invocation.command.stage
            is ReconstructionStage.ATOMIC_PARTITION_COMMIT
        ):
            raise _InjectedWorkerTermination("worker terminated before receipt")
        return await self.delegate.execute(invocation)


def test_real_triangle_is_deterministic_and_recovers_post_rename_crash(
    tmp_path: Path,
) -> None:
    """All seven real handlers commit deterministic queryable Parquet."""
    first = _case(tmp_path / "concurrency-1", max_parallel_windows=1)
    register_first_party_reconstruction_handlers()
    first_store = ReconstructionCheckpointStore(
        first.request.manifest_store_root
    )
    with pytest.raises(_InjectedWorkerTermination):
        asyncio.run(
            run_reconstruction_window(
                first.request,
                first.task,
                checkpoint_store=first_store,
                stage_executor=_StopBeforeCommitExecutor(),
            )
        )
    validated = first_store.load(first.task.window)
    assert validated is not None
    assert validated.checkpoint.phase is ReconstructionCommitPhase.VALIDATED
    assert len(validated.outcomes) == 6

    commit_invocation = ReconstructionStageInvocationV1(
        run=first.request.run,
        task=first.task,
        command=first.task.commands[-1],
        prior_outcomes=validated.outcomes,
    )
    promoted_without_receipt = atomic_commit_handler(commit_invocation)
    assert (
        promoted_without_receipt.status is ReconstructionStageStatus.COMPLETED
    )
    assert not Path(first.task.commands[-1].receipt_path).exists()

    recovered = asyncio.run(
        run_reconstruction_window(
            first.request,
            first.task,
            checkpoint_store=first_store,
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    assert recovered.checkpoint.phase is ReconstructionCommitPhase.COMMITTED
    assert recovered.outcomes[-1].output_refs[0].metadata["idempotent_retry"]
    first_manifest = _committed_manifest(recovered)
    first_streams = read_reconstruction_streams(
        recovered.committed_manifest_ref.path  # type: ignore[union-attr]
    )
    assert first_manifest.observed_event_count > 0
    assert first_manifest.synthetic_event_count > 0
    assert sum(len(stream.events) for stream in first_streams) == (
        first_manifest.event_count
    )
    for outcome in recovered.outcomes:
        telemetry = outcome.output_refs[0].metadata
        assert telemetry["runtime_seconds"] >= 0.0
        assert telemetry["peak_rss_bytes"] > 0
        assert telemetry["scratch_bytes"] >= 0
        assert telemetry["output_bytes"] > 0
        assert "candidate_amplification" in telemetry

    second = _case(tmp_path / "concurrency-2", max_parallel_windows=2)
    second_state = asyncio.run(
        run_reconstruction_window(
            second.request,
            second.task,
            checkpoint_store=ReconstructionCheckpointStore(
                second.request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    second_manifest = _committed_manifest(second_state)
    assert second_manifest.replay.logical_content_sha256 == (
        first_manifest.replay.logical_content_sha256
    )
    assert second_manifest.publication_id == first_manifest.publication_id
    assert second_manifest.replay.partition_byte_sha256 == (
        first_manifest.replay.partition_byte_sha256
    )


def test_real_validation_refusal_prevents_atomic_commit(tmp_path: Path) -> None:
    """A schema-valid but failing qualification cannot become discoverable."""
    case = _case(
        tmp_path / "negative-validation",
        max_parallel_windows=1,
        failing_qualification=True,
    )
    register_first_party_reconstruction_handlers()
    state = asyncio.run(
        run_reconstruction_window(
            case.request,
            case.task,
            checkpoint_store=ReconstructionCheckpointStore(
                case.request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    assert state.checkpoint.phase is ReconstructionCommitPhase.FAILED
    assert "final_validation_failed" in state.checkpoint.interruption_reason
    assert len(state.outcomes) == 5
    assert discover_reconstruction_manifests(case.execution.output_root) == ()


def test_real_cancellation_removes_partial_window_scratch_for_every_handler(
    tmp_path: Path,
) -> None:
    """Every handler cancels before work and removes disposable artifacts."""
    case = _case(tmp_path / "cancellation", max_parallel_windows=1)
    scratch = Path(case.task.scratch_directory)
    invocation = ReconstructionStageInvocationV1(
        run=case.request.run,
        task=case.task,
        command=case.task.commands[0],
        prior_outcomes=(),
        cancellation_check=lambda: True,
    )
    handlers = (
        source_enrichment_handler,
        proposal_handler,
        carving_handler,
        cross_series_reconciliation_handler,
        delivery_projection_handler,
        validation_handler,
        atomic_commit_handler,
    )
    for handler in handlers:
        scratch.mkdir(parents=True)
        (scratch / "partial.parquet").write_bytes(b"not publishable")
        with pytest.raises(asyncio.CancelledError):
            handler(invocation)
        assert not scratch.exists()
        assert (
            discover_reconstruction_manifests(case.execution.output_root) == ()
        )


def _case(
    root: Path,
    *,
    max_parallel_windows: int,
    failing_qualification: bool = False,
) -> _Case:
    plan = _real_plan()
    start = _real_start_ns()
    retention = ReconstructionRetentionPlanV1.from_dict(
        json.loads(
            Path(plan.artifact_graph["retention_plan"].path).read_text(
                encoding="utf-8"
            )
        )
    )
    primary = retention.primary_member_id
    original = next(
        task
        for request in plan.workflow_requests
        for task in request.tasks
        if task.window.ensemble_member_id == primary
        and task.window.reads_event_time(start)
    )
    old_execution = read_reconstruction_plan_execution_manifest(
        original.commands[0].configuration_refs[0].path
    )
    artifacts = dict(old_execution.artifacts)
    replacement_qualification: ArtifactRef | None = None
    if failing_qualification:
        replacement_qualification = _failing_qualification(
            root / "artifacts", artifacts["motif_qualification"]
        )
        artifacts["motif_qualification"] = replacement_qualification
    execution = replace(
        old_execution,
        artifacts=artifacts,
        output_root=str(root / "output"),
        checkpoint_root=str(root / "checkpoints"),
        scratch_root=str(root / "scratch"),
        manifest_id="",
    )
    execution_ref = _write_execution_manifest(root / "artifacts", execution)
    window = ReconstructionWindowV1(
        run_id=plan.run.run_id,
        ensemble_member_id=primary,
        symbols=plan.run.symbols,
        core_start_ns=start,
        core_end_ns=start + 5 * 60 * 1_000_000_000,
        left_halo_ns=original.window.left_halo_ns,
        right_lookahead_ns=original.window.right_lookahead_ns,
    )
    scratch = root / "scratch" / window.window_id
    commands = []
    for ordinal, command in enumerate(original.commands):
        input_refs = command.input_manifest_refs
        if replacement_qualification is not None:
            input_refs = tuple(
                (
                    replacement_qualification
                    if ref.kind == replacement_qualification.kind
                    else ref
                )
                for ref in input_refs
            )
        commands.append(
            ReconstructionStageCommandV1(
                stage=command.stage,
                handler_name=command.handler_name,
                receipt_path=str(
                    scratch
                    / "receipts"
                    / f"{ordinal:02d}-{command.stage.value}.json"
                ),
                input_manifest_refs=input_refs,
                configuration_refs=(execution_ref,),
            )
        )
    task = ReconstructionWindowTaskV1(
        window=window,
        resource_estimate=ReconstructionResourceEstimateV1(
            input_event_count=5_000,
            candidate_event_count=1_000,
            retained_ensemble_members=1,
            inflight_batches=1,
            peak_events_per_batch=1_000,
            estimated_memory_bytes=16 * 1024**2,
            estimated_scratch_bytes=32 * 1024**2,
            estimated_output_bytes=32 * 1024**2,
            estimated_batch_count=10,
        ),
        commands=tuple(commands),
        scratch_directory=str(scratch),
    )
    request = ReconstructionWorkflowRequestV1(
        request_id=f"real-handler-{root.name}",
        run=plan.run,
        tasks=(task,),
        manifest_store_root=execution.checkpoint_root,
        report_root=str(root / "reports"),
        task_queues={"reconstruction": "local"},
        max_parallel_windows=max_parallel_windows,
        max_inflight_memory_bytes=64 * 1024**2,
    )
    return _Case(plan=plan, execution=execution, request=request, task=task)


def _real_plan() -> SyntheticInfillPlanV1:
    value = os.environ.get(_PLAN_ENV, "").strip()
    if not value:
        pytest.skip(f"set {_PLAN_ENV} to run qualified real-artifact gates")
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        pytest.skip(f"qualified reconstruction plan is missing: {path}")
    return read_synthetic_infill_plan(path)


def _real_start_ns() -> int:
    value = os.environ.get(_START_ENV, "2011-01-13T00:00:00+00:00")
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return int(timestamp.timestamp() * 1_000_000_000)


def _write_execution_manifest(
    root: Path, manifest: ReconstructionPlanExecutionManifestV1
) -> ArtifactRef:
    encoded = manifest.to_json().encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"reconstruction-plan-execution-{digest}.json"
    path.write_bytes(encoded)
    return ArtifactRef(
        kind="reconstruction_plan_execution_manifest_v1",
        path=str(path.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={"manifest_id": manifest.manifest_id},
    )


def _failing_qualification(root: Path, original: ArtifactRef) -> ArtifactRef:
    payload: dict[str, Any] = json.loads(
        Path(original.path).read_text(encoding="utf-8")
    )
    payload["candidate_promotion_eligible"] = False
    encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"modern-reference-motif-qualification-{digest}.json"
    path.write_bytes(encoded)
    return ArtifactRef(
        kind=original.kind,
        path=str(path.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(original.metadata),
    )


def _committed_manifest(state: Any) -> ReconstructionProductManifestV2:
    assert state.committed_manifest_ref is not None
    manifest = load_reconstruction_manifest(state.committed_manifest_ref.path)
    assert isinstance(manifest, ReconstructionProductManifestV2)
    return manifest
