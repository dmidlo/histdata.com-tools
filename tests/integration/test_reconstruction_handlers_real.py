"""Real-artifact closure gates for first-party reconstruction handlers.

Set ``HISTDATACOM_REAL_RECONSTRUCTION_PLAN`` to a #486 plan artifact.  These
tests never replace scientific handlers; they re-home only execution roots so
the committed products and injected failures remain isolated under pytest's
temporary directory.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from histdatacom import reconstruction_cli
from histdatacom.cross_series_constraints import (
    CROSS_SERIES_CONSTRAINT_BUNDLE_ARTIFACT_KIND,
    read_cross_series_constraint_bundle,
)
from histdatacom.orchestration.reconstruction import (
    ReconstructionCheckpointStore,
    ReconstructionStage,
    ReconstructionStageCommandV1,
    ReconstructionStageInvocationV1,
    ReconstructionStageOutcomeV1,
    ReconstructionStageStatus,
    ReconstructionWindowTaskV1,
    ReconstructionWorkflowRequestV1,
    RegisteredReconstructionStageExecutor,
    run_reconstruction_window,
)
from histdatacom.reconstruction import (
    ReconstructionClient,
    read_operation_receipt,
    write_execution_request,
)
from histdatacom.reconstruction_evidence import (
    RECONSTRUCTION_EVIDENCE_PROJECTION_ARTIFACT_KIND,
    read_point_in_time_evidence_projection,
)
from histdatacom.reconstruction_experiment import read_reconstruction_experiment
from histdatacom.reconstruction_science import (
    RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL,
    read_reconstruction_scientific_ledger,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.ensembles import ReconstructionEnsemblePlanV1
from histdatacom.synthetic.observation_uncertainty import (
    ObservationUncertaintyPolicyV1,
    ObservationUncertaintyScenarioKind,
)
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV3,
    ReconstructionRetentionPlanV1,
    discover_reconstruction_manifests,
    load_reconstruction_manifest,
    read_reconstruction_streams,
)
from histdatacom.synthetic.reconstruction_handlers import (
    STAGING_DESCRIPTOR_ARTIFACT_KIND,
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
from histdatacom.synthetic.streaming import ReconstructionCommitPhase

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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All seven real handlers commit deterministic queryable Parquet."""
    first = _case(
        tmp_path / "concurrency-1",
        max_parallel_windows=1,
        observation_scenario_kind=(
            ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL
        ),
    )
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
    monkeypatch.setattr(
        "histdatacom.synthetic.reconstruction_handlers."
        "_cleanup_committed_window_scratch",
        lambda *_, **__: 0,
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
    experiment = read_reconstruction_experiment(
        first.plan.artifact_graph["experiment_manifest"].path
    )
    assert first_manifest.source.experiment_id == experiment.experiment_id
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
    source_outcome = next(
        outcome
        for outcome in recovered.outcomes
        if outcome.stage is ReconstructionStage.SOURCE_ENRICHMENT
    )
    source_ref = next(
        ref
        for ref in source_outcome.output_refs
        if ref.kind == "reconstruction_source_stage_v2"
    )
    source_manifest = json.loads(
        Path(source_ref.path).read_text(encoding="utf-8")
    )
    scientific_ledger_ref = first.plan.artifact_graph["scientific_ledger"]
    scientific_ledger = read_reconstruction_scientific_ledger(
        scientific_ledger_ref.path
    )
    conditioning = source_manifest["scientific_conditioning"]
    assert conditioning["scientific_ledger_id"] == scientific_ledger.ledger_id
    assert conditioning["estimand_id"] == scientific_ledger.estimand.estimand_id
    assert set(conditioning["conditioning_states"]) == {
        "market_context",
        "cftc_positioning",
    }
    assert set(conditioning["conditioning_state_ids"]) == {
        "market_context",
        "cftc_positioning",
    }
    assert all(
        state["scientific_ledger_id"] == scientific_ledger.ledger_id
        and state["information_mode"] == "ex_post_reconstruction"
        and state["invalid_for_backtest_reason"]
        == RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
        for state in conditioning["conditioning_states"].values()
    )
    assert (
        first_manifest.quality.benchmark_evidence["scientific_ledger_id"]
        == scientific_ledger.ledger_id
    )
    assert (
        first_manifest.quality.benchmark_evidence["conditioning_state_ids"]
        == conditioning["conditioning_state_ids"]
    )
    assert (
        first_manifest.quality.benchmark_evidence["invalid_for_backtest_reason"]
        == RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
    )
    assert (
        scientific_ledger_ref.sha256
        in first_manifest.quality.benchmark_artifact_ids
    )
    assert source_manifest["point_in_time_evidence_projection_ids"]
    assert set(source_manifest["point_in_time_evidence_use"]) == {
        "eurgbp",
        "eurusd",
        "gbpusd",
    }
    projection_refs = tuple(
        ArtifactRef.from_dict(value)
        for value in source_manifest["point_in_time_evidence_refs"].values()
    )
    assert all(
        ref.kind == RECONSTRUCTION_EVIDENCE_PROJECTION_ARTIFACT_KIND
        for ref in projection_refs
    )
    projections = tuple(
        read_point_in_time_evidence_projection(ref.path)
        for ref in projection_refs
    )
    assert all(projection.records for projection in projections)
    assert all(
        len(projection.to_json()) < 1_000_000 for projection in projections
    )
    assert all(
        '"full_tick_rows_embedded":false' in item.to_json()
        for item in projections
    )
    constraint_refs = tuple(
        ArtifactRef.from_dict(value)
        for value in source_manifest["cross_series_constraint_refs"].values()
    )
    assert all(
        ref.kind == CROSS_SERIES_CONSTRAINT_BUNDLE_ARTIFACT_KIND
        for ref in constraint_refs
    )
    constraint_bundles = tuple(
        read_cross_series_constraint_bundle(ref.path) for ref in constraint_refs
    )
    assert [item.bundle_id for item in constraint_bundles] == (
        source_manifest["cross_series_constraint_bundle_ids"]
    )
    assert all(item.windows for item in constraint_bundles)
    assert all(len(item.to_json()) < 1_000_000 for item in constraint_bundles)
    assert all(
        '"full_tick_rows_embedded":false' in item.to_json()
        for item in constraint_bundles
    )
    assert source_manifest["cross_series_constraint_use"]["status"] in {
        "applied",
        "not_applicable",
    }
    proposal_outcome = next(
        outcome
        for outcome in recovered.outcomes
        if outcome.stage is ReconstructionStage.PROPOSAL
    )
    proposal_manifest = json.loads(
        Path(proposal_outcome.output_refs[0].path).read_text(encoding="utf-8")
    )
    assert {
        value["session_state"]
        for value in proposal_manifest["query_conditions"].values()
    } == {"asia"}
    if proposal_manifest["proposal_engine_id"].startswith(
        "histdatacom.marked-hawkes."
    ):
        assert proposal_manifest["proposal_generation_evidence"]["status"] == (
            "generated"
        )
        degradation = proposal_manifest["proposal_generation_scenario"][
            "degradation_parameters"
        ]
        if proposal_manifest["observation_uncertainty_ensemble_ref"] is None:
            assert proposal_manifest["observation_scenario_id"] is None
            assert degradation["legacy_observation_uncertainty_policy"] == (
                "v2.4-point-estimate-replay-not-v2.5-scenario-v1"
            )
        else:
            assert proposal_manifest["observation_uncertainty_ensemble_id"]
            assert proposal_manifest["observation_scenario_id"]
            assert proposal_manifest["observation_scenario_kind"] in {
                "high_retention_low_infill",
                "central_fitted_retention",
                "low_retention_high_infill",
            }
            assert isinstance(proposal_manifest["observation_path_seed"], int)
            assert degradation["observation_scenario_id"] == (
                proposal_manifest["observation_scenario_id"]
            )
            assert degradation["observation_path_seed"] == (
                proposal_manifest["observation_path_seed"]
            )
            runtime_evidence = first_manifest.quality.benchmark_evidence[
                "runtime_proposal_evidence"
            ]
            assert runtime_evidence["observation_scenario_id"] == (
                proposal_manifest["observation_scenario_id"]
            )
            assert runtime_evidence["observation_path_seed"] == (
                proposal_manifest["observation_path_seed"]
            )
    assert proposal_manifest["synchronization_constraint_window_id"] in (
        source_manifest["cross_series_constraint_window_ids"]
    )
    assert proposal_manifest["cross_series_constraint_use"]["status"] == (
        "applied"
    )
    proposal_ledger_ref = ArtifactRef.from_dict(
        proposal_manifest["batch_ledger_ref"]
    )
    with gzip.open(proposal_ledger_ref.path, "rb") as stream:
        proposal_rows = tuple(json.loads(line) for line in stream)
    assert all("cross_series_constraint_use" in row for row in proposal_rows)
    if proposal_manifest["observation_scenario_id"] is not None:
        assert all(
            row["observation_scenario_id"]
            == proposal_manifest["observation_scenario_id"]
            and row["observation_path_seed"]
            == proposal_manifest["observation_path_seed"]
            for row in proposal_rows
        )
    assert all(
        row["cross_series_synchronization_constraint_window_id"]
        == proposal_manifest["synchronization_constraint_window_id"]
        for row in proposal_rows
    )
    carving_outcome = next(
        outcome
        for outcome in recovered.outcomes
        if outcome.stage is ReconstructionStage.CARVING
    )
    carving_ref = carving_outcome.output_refs[0]
    carving_manifest = json.loads(
        Path(carving_ref.path).read_text(encoding="utf-8")
    )
    assert carving_ref.size_bytes is not None
    assert carving_ref.size_bytes < 1_000_000
    assert carving_manifest["carved_batches_inline"] is False
    assert carving_manifest["point_in_time_evidence_projection_ids"]
    assert carving_manifest["point_in_time_evidence_decision_ids"]
    ledger_ref = ArtifactRef.from_dict(
        carving_manifest["carved_batch_ledger_ref"]
    )
    assert ledger_ref.kind == "reconstruction_carved_batch_ledger_v3"
    assert ledger_ref.metadata["format"] == "canonical-json-lines-gzip-v1"
    assert ledger_ref.path.endswith(".ndjson.gz")
    assert (
        ledger_ref.metadata["batch_count"]
        == carving_manifest["carved_batch_count"]
    )
    with gzip.open(ledger_ref.path, "rb") as stream:
        rows = tuple(json.loads(line) for line in stream)
    assert len(rows) == carving_manifest["carved_batch_count"]
    assert all("point_in_time_evidence_use" in row for row in rows)
    assert all("cross_series_constraint_use" in row for row in rows)
    for stage in (
        ReconstructionStage.CROSS_SERIES_RECONCILIATION,
        ReconstructionStage.BROKER_TRANSFER,
    ):
        outcome = next(
            item for item in recovered.outcomes if item.stage is stage
        )
        downstream = json.loads(
            Path(outcome.output_refs[0].path).read_text(encoding="utf-8")
        )
        assert downstream["point_in_time_evidence_projection_ids"] == (
            carving_manifest["point_in_time_evidence_projection_ids"]
        )
        assert downstream["point_in_time_evidence_decision_ids"] == (
            carving_manifest["point_in_time_evidence_decision_ids"]
        )
        assert downstream["cross_series_constraint_bundle_ids"] == (
            carving_manifest["cross_series_constraint_bundle_ids"]
        )
        assert downstream["cross_series_constraint_window_ids"] == (
            carving_manifest["cross_series_constraint_window_ids"]
        )
        assert downstream["cross_series_constraint_decision_ids"]
    validation_outcome = next(
        item
        for item in recovered.outcomes
        if item.stage is ReconstructionStage.VALIDATION
    )
    descriptor_ref = next(
        ref
        for ref in validation_outcome.output_refs
        if ref.kind == STAGING_DESCRIPTOR_ARTIFACT_KIND
    )
    descriptor = json.loads(
        Path(descriptor_ref.path).read_text(encoding="utf-8")
    )
    assert descriptor["point_in_time_evidence_projection_ids"] == (
        carving_manifest["point_in_time_evidence_projection_ids"]
    )
    assert (
        list(first_manifest.quality.point_in_time_evidence_projection_ids)
        == descriptor["point_in_time_evidence_projection_ids"]
    )
    assert list(first_manifest.quality.point_in_time_evidence_decision_ids) == (
        descriptor["point_in_time_evidence_decision_ids"]
    )
    assert descriptor["point_in_time_evidence_validation_use"]["stage"] == (
        "validation"
    )
    assert descriptor["point_in_time_evidence_validation_use"]["status"] in {
        "applied",
        "not_applicable",
    }
    assert descriptor["cross_series_constraint_bundle_ids"] == (
        carving_manifest["cross_series_constraint_bundle_ids"]
    )
    assert descriptor["cross_series_constraint_window_ids"] == (
        carving_manifest["cross_series_constraint_window_ids"]
    )
    assert descriptor["cross_series_constraint_validation_use"]["status"] == (
        "applied"
    )
    assert list(first_manifest.quality.cross_series_constraint_bundle_ids) == (
        descriptor["cross_series_constraint_bundle_ids"]
    )
    assert list(first_manifest.quality.cross_series_constraint_window_ids) == (
        descriptor["cross_series_constraint_window_ids"]
    )
    assert list(
        first_manifest.quality.cross_series_constraint_decision_ids
    ) == (descriptor["cross_series_constraint_decision_ids"])

    monkeypatch.undo()
    second = _case(
        tmp_path / "concurrency-2",
        max_parallel_windows=2,
        observation_scenario_kind=(
            ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL
        ),
    )
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
    scratch_entries = {
        item.name for item in Path(second.task.scratch_directory).iterdir()
    }
    assert scratch_entries == {"receipts", "validation"}
    # The commit handler removes every disposable stage artifact before it
    # returns.  The orchestration boundary then durably writes the one final
    # atomic-commit receipt, which is required to recover a crash between the
    # publication rename and checkpoint persistence.  No earlier stage
    # receipt or intermediate may survive the successful commit.
    atomic_receipt = Path(second.task.commands[-1].receipt_path)
    assert {item.resolve() for item in atomic_receipt.parent.iterdir()} == {
        atomic_receipt.resolve()
    }
    receipt_outcome = ReconstructionStageOutcomeV1.from_json(
        atomic_receipt.read_text(encoding="utf-8")
    )
    assert receipt_outcome.outcome_id == second_state.outcomes[-1].outcome_id
    assert (
        second_state.outcomes[-1]
        .output_refs[0]
        .metadata["removed_scratch_bytes"]
        > 0
    )


def test_real_zero_deficit_triangle_commits_observed_only(
    tmp_path: Path,
) -> None:
    """A qualified zero-cardinality deficit remains a verified product."""
    case = _case(
        tmp_path / "zero-deficit",
        max_parallel_windows=1,
    )
    if case.task.resource_estimate.candidate_event_count != 0:
        pytest.skip("selected real plan window does not have zero deficit")
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
    manifest = _committed_manifest(state)

    assert state.checkpoint.phase is ReconstructionCommitPhase.COMMITTED
    assert len(state.outcomes) == 7
    assert manifest.observed_event_count > 0
    assert manifest.synthetic_event_count == 0
    assert manifest.physical_event_count == 0
    assert manifest.partitions == ()
    assert (
        sum(item.row_count for item in manifest.observed_anchor_segments)
        == manifest.observed_event_count
    )
    assert manifest.source.observed_values_verified_exactly
    assert manifest.quality.final_validation_status == "passed"
    assert manifest.quality.cross_instrument_quality_status == "passed"
    replay = ReconstructionClient().replay(
        state.committed_manifest_ref.path  # type: ignore[union-attr]
    )
    assert replay["event_count"] == manifest.observed_event_count
    assert replay["replay_verified"]


def test_real_scientific_evidence_refusal_prevents_atomic_commit(
    tmp_path: Path,
) -> None:
    """Schema-valid cross-split leakage evidence cannot become discoverable."""
    case = _case(
        tmp_path / "negative-validation",
        max_parallel_windows=1,
        failing_scientific_evidence=True,
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
    assert state.checkpoint.interruption_reason in {
        "proposal_validation_failed",
        "final_validation_failed",
    }
    assert 1 <= len(state.outcomes) <= 5
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


def test_real_public_cli_and_api_execute_same_one_window_product(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Installed CLI and typed API meet at one real committed logical output."""
    case = _case(tmp_path / "public-cli-api", max_parallel_windows=1)
    plan_path = str(Path(os.environ[_PLAN_ENV]).expanduser().resolve())
    client = ReconstructionClient()
    execution_request = client.create_request(
        plan_path,
        information_mode=case.plan.information_mode,
        acknowledge_scientific_nonclaim=True,
        allow_refusals=bool(case.plan.refusals),
    )
    request_path = write_execution_request(
        execution_request, tmp_path / "execution-request.json"
    )
    cli_receipt_path = tmp_path / "cli-receipt.json"

    assert (
        reconstruction_cli.main(
            [
                "--json",
                "run",
                "--request",
                str(request_path),
                "--local",
                "--window-id",
                case.task.window.window_id,
                "--receipt",
                str(cli_receipt_path),
            ]
        )
        == 0
    )
    cli_run = json.loads(capsys.readouterr().out)
    cli_receipt = read_operation_receipt(cli_receipt_path)
    api_receipt = client.execute_local(
        execution_request, window_id=case.task.window.window_id
    )
    manifest_path = cli_receipt.reports[0].committed_manifest_refs[0].path
    manifest = load_reconstruction_manifest(manifest_path)
    assert isinstance(manifest, ReconstructionProductManifestV3)

    assert cli_run["status"] == "committed"
    assert cli_receipt.reports == api_receipt.reports
    assert (
        cli_receipt.reports[0].observed_event_count
        == manifest.observed_event_count
    )
    assert (
        cli_receipt.reports[0].synthetic_event_count
        == manifest.synthetic_event_count
    )

    api_preview = client.preview(manifest_path, limit=100)
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "preview",
                "--manifest",
                manifest_path,
                "--limit",
                "100",
            ]
        )
        == 0
    )
    cli_preview = json.loads(capsys.readouterr().out)
    api_replay = client.replay(manifest_path)
    assert (
        reconstruction_cli.main(
            ["--json", "replay", "--manifest", manifest_path]
        )
        == 0
    )
    cli_replay = json.loads(capsys.readouterr().out)

    assert cli_preview == api_preview
    assert cli_replay == api_replay
    preview_origins = {row["origin"] for row in api_preview["rows"]}
    assert "observed" in preview_origins
    assert preview_origins <= {"observed", "synthetic"}
    synthetic_rows = tuple(
        row for row in api_preview["rows"] if row["origin"] == "synthetic"
    )
    for synthetic in synthetic_rows:
        assert synthetic["generation"]["generator_id"]
        assert synthetic["generation"]["confidence"] is None
        assert synthetic["constraint_decision"]["decision"] == "accepted"
        assert synthetic["constraint_decision"]["constraint_set_id"]
        assert synthetic["lineage"]["left_anchor_event_id"]
        assert synthetic["lineage"]["right_anchor_event_id"]
    assert api_replay["event_count"] == manifest.event_count
    assert api_replay["replay_verified"]


def _case(
    root: Path,
    *,
    max_parallel_windows: int,
    failing_scientific_evidence: bool = False,
    observation_scenario_kind: ObservationUncertaintyScenarioKind | None = None,
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
    member_id = retention.primary_member_id
    if observation_scenario_kind is not None:
        ensemble = ReconstructionEnsemblePlanV1.from_dict(
            json.loads(
                Path(plan.artifact_graph["ensemble_plan"].path).read_text(
                    encoding="utf-8"
                )
            )
        )
        policy = ObservationUncertaintyPolicyV1.from_dict(
            json.loads(
                Path(
                    plan.artifact_graph["observation_uncertainty_policy"].path
                ).read_text(encoding="utf-8")
            )
        )
        scenario_index = policy.scenario_order.index(observation_scenario_kind)
        member_id = next(
            member.member_id
            for member in ensemble.members
            if (member.ordinal - 1) % len(policy.scenario_order)
            == scenario_index
        )
        assert member_id in retention.retained_member_ids
    original_request, original = next(
        (request, task)
        for request in plan.workflow_requests
        for task in request.tasks
        if task.window.ensemble_member_id == member_id
        and task.window.reads_event_time(start)
    )
    old_execution = read_reconstruction_plan_execution_manifest(
        original.commands[0].configuration_refs[0].path
    )
    artifacts = dict(old_execution.artifacts)
    replacement_leakage: ArtifactRef | None = None
    if failing_scientific_evidence:
        replacement_leakage = _failing_scientific_evidence(
            root / "artifacts", artifacts["motif_leakage_audit"]
        )
        artifacts["motif_leakage_audit"] = replacement_leakage
    execution = replace(
        old_execution,
        artifacts=artifacts,
        output_root=str(root / "output"),
        checkpoint_root=str(root / "checkpoints"),
        scratch_root=str(root / "scratch"),
        manifest_id="",
    )
    execution_ref = _write_execution_manifest(root / "artifacts", execution)
    # The source-support contract is keyed to the exact adaptive
    # synchronization unit selected by the planner.  Reusing only its start
    # and inventing a five-minute end produces a window that the plan never
    # qualified, so the runtime must (correctly) refuse it as source
    # corruption.  Exercise the real planned window end to end instead.
    window = original.window
    scratch = root / "scratch" / window.window_id
    commands = []
    for ordinal, command in enumerate(original.commands):
        input_refs = command.input_manifest_refs
        if replacement_leakage is not None:
            input_refs = tuple(
                (
                    replacement_leakage
                    if ref.kind == replacement_leakage.kind
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
        resource_estimate=original.resource_estimate,
        commands=tuple(commands),
        scratch_directory=str(scratch),
    )
    request = ReconstructionWorkflowRequestV1(
        request_id=f"real-handler-{root.name}",
        run=plan.run,
        tasks=(task,),
        manifest_store_root=execution.checkpoint_root,
        report_root=str(root / "reports"),
        task_queues=original_request.task_queues,
        max_parallel_windows=max_parallel_windows,
        max_inflight_memory_bytes=(original_request.max_inflight_memory_bytes),
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


def _failing_scientific_evidence(
    root: Path, original: ArtifactRef
) -> ArtifactRef:
    payload: dict[str, Any] = json.loads(
        Path(original.path).read_text(encoding="utf-8")
    )
    payload["post_exclusion_cross_split_finding_count"] = 1
    encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"modern-reference-motif-leakage-audit-{digest}.json"
    path.write_bytes(encoded)
    return ArtifactRef(
        kind=original.kind,
        path=str(path.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(original.metadata),
    )


def _committed_manifest(state: Any) -> ReconstructionProductManifestV3:
    assert state.committed_manifest_ref is not None
    manifest = load_reconstruction_manifest(state.committed_manifest_ref.path)
    assert isinstance(manifest, ReconstructionProductManifestV3)
    return manifest
