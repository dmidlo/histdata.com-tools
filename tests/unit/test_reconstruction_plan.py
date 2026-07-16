"""Tests for the first-party reconstruction plan and artifact graph."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from histdatacom.orchestration.reconstruction import (
    RECONSTRUCTION_STAGE_ORDER,
    ReconstructionStage,
    artifact_ref_for_file,
)
from histdatacom.synthetic import (
    ASCII_TICK_SOURCE_KIND,
    FIRST_PARTY_RECONSTRUCTION_HANDLERS,
    IMMUTABLE_ANCHOR_POLICY,
    SCIENTIFIC_NONCLAIM,
    TICK_ONLY_INPUT_POLICY,
    InformationMode,
    ModernReferenceMotifProfileV1,
    ReconstructionDeliveryMode,
    ReconstructionPlanCompatibilityError,
    ReconstructionSourceInventoryV1,
    ReconstructionStoragePolicyV1,
    SyntheticInfillPlanV1,
    build_synthetic_infill_plan,
    load_reconstruction_stage_plan,
    read_reconstruction_plan_execution_manifest,
    read_reconstruction_source_inventory,
    read_synthetic_infill_plan,
    validate_synthetic_infill_plan_for_execution,
    write_synthetic_infill_plan,
)
from histdatacom.synthetic import reconstruction_plan as plan_module

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
_PERIOD = "202001"
_START_MS = 1_578_268_800_000


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_tick_partition(path: Path, offset: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "datetime": [_START_MS + offset, _START_MS + 60_000 + offset],
            "bid": [1.0 + offset / 1_000_000, 1.0001 + offset / 1_000_000],
            "ask": [1.0002 + offset / 1_000_000, 1.0003 + offset / 1_000_000],
            "vol": [0, 0],
        }
    )
    with pa.OSFile(str(path), "wb") as sink:
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


def _artifact(tmp_path: Path, role: str, kind: str) -> Any:
    path = tmp_path / "inputs" / f"{role}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"role": role}, sort_keys=True), encoding="utf-8"
    )
    return artifact_ref_for_file(path, kind=kind)


def _resolved_inputs(tmp_path: Path, source_root: Path) -> Any:
    lineage: list[dict[str, str]] = []
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = source_root / symbol / "2020" / "1" / ".data"
        _write_tick_partition(path, ordinal)
        lineage.append(
            {
                "period": _PERIOD,
                "symbol": symbol.upper(),
                "source_artifact_sha256": f"sha256:{_sha256(path)}",
                "evidence_id": f"feed-evidence:{symbol}",
            }
        )
    definition = SimpleNamespace(
        definition_id="feed-epochs-v2:test",
        symbols=tuple(symbol.upper() for symbol in _SYMBOLS),
        lineage={"sources": lineage},
        coverage_end_utc_ms=1_580_515_200_000,
        assign=lambda **_: SimpleNamespace(assignment_kind="assigned"),
    )
    artifacts = {
        "feed_epochs": _artifact(
            tmp_path, "feed-epochs", "feed_epoch_definition_v2"
        ),
        "observation_operator": _artifact(
            tmp_path, "observation", "observation-operator"
        ),
        "market_context": _artifact(
            tmp_path, "market-context", "market_context_corpus_v1"
        ),
        "cftc_positioning": _artifact(
            tmp_path, "cftc-positioning", "cftc_positioning_corpus_v1"
        ),
        "benchmark_manifest": _artifact(
            tmp_path, "benchmark", "reverse_degradation_manifest_v1"
        ),
        "motif_manifest": _artifact(
            tmp_path, "motif-manifest", "modern_reference_motif_manifest_v1"
        ),
        "motif_index": _artifact(
            tmp_path, "motif-index", "modern_reference_motif_index_v1"
        ),
        "motif_qualification": _artifact(
            tmp_path,
            "motif-qualification",
            "modern_reference_motif_qualification_v1",
        ),
        "motif_leakage_audit": _artifact(
            tmp_path,
            "motif-leakage",
            "modern_reference_motif_leakage_audit_v1",
        ),
    }
    return plan_module._ResolvedPlanInputs(
        feed_epoch_definition=definition,
        observation_operator=SimpleNamespace(
            operator_id="observation-operator:test",
            required_left_halo_ns=0,
        ),
        market_context=SimpleNamespace(corpus_id="market-context:test"),
        cftc_positioning=SimpleNamespace(corpus_id="cftc-positioning:test"),
        benchmark_corpus=SimpleNamespace(corpus_id="benchmark:test"),
        motif_profile=ModernReferenceMotifProfileV1(),
        motif_index=SimpleNamespace(index_id="motif-index:test"),
        artifacts=artifacts,
        motif_manifest={"library_id": "modern-reference-motif:test"},
        motif_qualification={"qualified": True},
        motif_leakage_audit={"accepted": True},
    )


def _builder_kwargs(tmp_path: Path) -> dict[str, Any]:
    unused = tmp_path / "unused.json"
    return {
        "feed_epoch_definition_path": unused,
        "observation_operator_path": unused,
        "market_context_corpus_path": unused,
        "cftc_positioning_corpus_path": unused,
        "benchmark_manifest_path": unused,
        "motif_manifest_path": unused,
        "motif_index_path": unused,
        "motif_qualification_path": unused,
        "motif_leakage_audit_path": unused,
        "artifact_root": tmp_path / "artifacts",
        "output_root": tmp_path / "output",
        "checkpoint_root": tmp_path / "checkpoints",
        "scratch_root": tmp_path / "scratch",
        "start_period": _PERIOD,
        "end_period": _PERIOD,
    }


@pytest.fixture  # type: ignore[untyped-decorator]
def planned_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, dict[str, Any]]:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(tmp_path, source_root)
    monkeypatch.setattr(
        plan_module, "_resolve_plan_inputs", lambda **_: resolved
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=()),
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_cftc_positioning_corpus",
        lambda *_, **__: SimpleNamespace(ready=True, reasons=()),
    )
    return source_root, _builder_kwargs(tmp_path)


def test_public_builder_is_deterministic_bounded_and_stage_consumable(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment

    first = build_synthetic_infill_plan(source_root, **kwargs)
    second = build_synthetic_infill_plan(source_root, **kwargs)
    plan_ref = write_synthetic_infill_plan(first, kwargs["artifact_root"])
    restored = read_synthetic_infill_plan(plan_ref.path)

    assert first == second == restored
    assert SyntheticInfillPlanV1.from_json(first.to_json()) == first
    assert first.delivery_mode is ReconstructionDeliveryMode.MODERN_REFERENCE
    assert first.status == "ready"
    assert first.resources.planned_window_count == 2
    assert first.resources.executable_window_count == 2
    assert first.resources.ensemble_member_count == 4
    assert len(first.workflow_requests) == 4
    assert (
        max(
            len(json.dumps(request.to_dict()).encode("utf-8"))
            for request in first.workflow_requests
        )
        < 1_048_576
    )
    assert '"rows"' not in first.to_json()
    assert '"events"' not in first.to_json()
    assert first.to_dict()["scientific_nonclaim"] == SCIENTIFIC_NONCLAIM
    assert first.to_dict()["immutable_anchor_policy"] == IMMUTABLE_ANCHOR_POLICY
    assert first.to_dict()["input_policy"] == TICK_ONLY_INPUT_POLICY

    validate_synthetic_infill_plan_for_execution(restored)
    task = restored.workflow_requests[0].tasks[0]
    assert tuple(command.stage for command in task.commands) == (
        RECONSTRUCTION_STAGE_ORDER
    )
    for command in task.commands:
        loaded = load_reconstruction_stage_plan(command)
        assert loaded.command == command
        assert (
            loaded.configuration.configuration_id == restored.configuration_id
        )
        assert (
            command.handler_name
            == FIRST_PARTY_RECONSTRUCTION_HANDLERS[command.stage]
        )
        assert command.configuration_refs == (
            restored.artifact_graph["execution_manifest"],
        )
    broker_command = next(
        command
        for command in task.commands
        if command.stage is ReconstructionStage.BROKER_TRANSFER
    )
    assert broker_command.input_manifest_refs == ()


def test_source_inventory_declares_tick_only_ordinal_identity(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    plan = build_synthetic_infill_plan(source_root, **kwargs)
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )

    assert len(inventory.partitions) == 3
    assert all(
        item.artifact.kind == ASCII_TICK_SOURCE_KIND
        for item in inventory.partitions
    )
    assert all(
        item.to_dict()["row_identity_basis"]
        == "zero-based-arrow-row-ordinal-v1"
        for item in inventory.partitions
    )
    assert inventory.to_dict()["input_contract"] == "ascii/T-tick-bid-ask-only"

    with pytest.raises(ValueError, match="complete synchronized triangle"):
        ReconstructionSourceInventoryV1(
            source_root=inventory.source_root,
            symbols=inventory.symbols,
            periods=inventory.periods,
            partitions=inventory.partitions[:-1],
            requested_start_ns=inventory.requested_start_ns,
            requested_end_ns=inventory.requested_end_ns,
            total_row_count=sum(
                item.row_count for item in inventory.partitions[:-1]
            ),
            total_size_bytes=sum(
                item.artifact.size_bytes or 0
                for item in inventory.partitions[:-1]
            ),
        )


def test_builder_rejects_stale_source_hash(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    path = source_root / "eurusd" / "2020" / "1" / ".data"
    path.write_bytes(path.read_bytes() + b"stale")

    with pytest.raises(
        ReconstructionPlanCompatibilityError, match="source hash differs"
    ):
        build_synthetic_infill_plan(source_root, **kwargs)


def test_builder_rejects_partial_triangle_before_artifact_resolution(
    tmp_path: Path,
) -> None:
    kwargs = _builder_kwargs(tmp_path)

    with pytest.raises(
        ReconstructionPlanCompatibilityError, match="complete.*triangle"
    ):
        build_synthetic_infill_plan(
            tmp_path / "ASCII" / "T",
            symbols=("EURUSD", "GBPUSD"),
            **kwargs,
        )


def test_builder_rejects_plan_root_inside_immutable_source(
    tmp_path: Path,
) -> None:
    kwargs = _builder_kwargs(tmp_path)
    source_root = tmp_path / "ASCII" / "T"
    kwargs["output_root"] = source_root / "generated"

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="output root overlaps the immutable source tree",
    ):
        build_synthetic_infill_plan(source_root, **kwargs)


def test_builder_rejects_overlapping_durable_roots(tmp_path: Path) -> None:
    kwargs = _builder_kwargs(tmp_path)
    kwargs["checkpoint_root"] = Path(kwargs["output_root"]) / "checkpoints"

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="output and checkpoint roots overlap",
    ):
        build_synthetic_infill_plan(tmp_path / "ASCII" / "T", **kwargs)


def test_execution_manifest_rejects_artifact_inside_scratch(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    plan = build_synthetic_infill_plan(source_root, **kwargs)
    execution = read_reconstruction_plan_execution_manifest(
        plan.artifact_graph["execution_manifest"].path
    )

    with pytest.raises(ValueError, match="artifact .* inside the scratch root"):
        replace(
            execution,
            scratch_root=Path(execution.artifacts["configuration"].path).parent,
            manifest_id="",
        )


def test_builder_rejects_unsupported_period(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    kwargs = {**kwargs, "start_period": "201912", "end_period": "201912"}

    with pytest.raises(
        ReconstructionPlanCompatibilityError, match="periods.*common"
    ):
        build_synthetic_infill_plan(source_root, **kwargs)


def test_builder_rejects_ex_ante_artifact_leakage(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="ex-ante plan refused.*observe the requested future",
    ):
        build_synthetic_infill_plan(
            source_root,
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            **kwargs,
        )


def test_builder_rejects_quota_overflow(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    policy = ReconstructionStoragePolicyV1(
        max_memory_bytes=1,
        max_scratch_bytes=1,
        max_output_bytes=1,
    )

    with pytest.raises(ValueError, match="resource preflight failed"):
        build_synthetic_infill_plan(
            source_root, storage_policy=policy, **kwargs
        )


def test_broker_only_delivery_requires_the_broker_artifact(
    tmp_path: Path,
) -> None:
    kwargs = _builder_kwargs(tmp_path)

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="requires a strong broker artifact",
    ):
        build_synthetic_infill_plan(
            tmp_path / "ASCII" / "T",
            delivery_mode=ReconstructionDeliveryMode.BROKER_CONDITIONED,
            **kwargs,
        )


def test_stage_loader_rejects_graph_reference_substitution(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    plan = build_synthetic_infill_plan(source_root, **kwargs)
    command = plan.workflow_requests[0].tasks[0].commands[0]
    foreign = kwargs["artifact_root"] / "foreign.json"
    foreign.write_text("{}", encoding="utf-8")
    substituted = replace(
        command,
        input_manifest_refs=(
            artifact_ref_for_file(
                foreign,
                kind="unexpected",
            ),
        ),
        command_id="",
    )

    with pytest.raises(ReconstructionPlanCompatibilityError):
        load_reconstruction_stage_plan(substituted, verify_artifacts=False)
