"""Tests for the first-party reconstruction plan and artifact graph."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from itertools import pairwise
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from pyarrow import ipc

import histdatacom.reconstruction as reconstruction_module
from histdatacom.cross_series_constraints import (
    CrossSeriesConstraintPolicyV1,
    read_cross_series_constraint_policy,
)
from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.datasets import DatasetVersionManifestV1
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.orchestration.queues import build_orchestration_worker_config
from histdatacom.orchestration.reconstruction import (
    RECONSTRUCTION_STAGE_ORDER,
    ReconstructionStage,
    ReconstructionStageInvocationV1,
    artifact_ref_for_file,
)
from histdatacom.reconstruction import (
    ReconstructionCampaignDatasetPublicationV1,
    ReconstructionCampaignProductIndexV1,
    ReconstructionCampaignProductShardV1,
    ReconstructionClient,
    ReconstructionExecutionRequestV1,
    ReconstructionOperationReceiptV1,
    ReconstructionPlanError,
    ReconstructionPlanSetExecutionRequestV1,
    ReconstructionPlanSetReceiptIndexV1,
    ReconstructionPlanSetV1,
    ReconstructionPlanSpecV1,
    ReconstructionPlanSpecV2,
    ReconstructionPlanSupportMapIndexV2,
    ReconstructionPlanSupportMapV1,
    ReconstructionRefusedError,
    ReconstructionUnsupportedError,
    iter_reconstruction_plan_support_maps,
    read_execution_request,
    read_operation_receipt,
    read_plan_spec,
    read_reconstruction_campaign_dataset_publication,
    read_reconstruction_campaign_product_index,
    read_reconstruction_campaign_product_shard,
    read_reconstruction_plan_set,
    read_reconstruction_plan_set_execution_request,
    read_reconstruction_plan_set_receipt_index,
    read_reconstruction_plan_support_map,
    read_reconstruction_plan_support_map_index,
    write_execution_request,
    write_operation_receipt,
    write_reconstruction_plan_set_execution_request,
)
from histdatacom.reconstruction_evidence import (
    ReconstructionEvidencePolicyV1,
    read_reconstruction_evidence_policy,
)
from histdatacom.reconstruction_experiment import (
    ReconstructionExperimentRole,
    read_reconstruction_experiment,
)
from histdatacom.reconstruction_science import (
    RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND,
    read_reconstruction_scientific_ledger,
)
from histdatacom.synthetic import (
    ASCII_TICK_SOURCE_KIND,
    FIRST_PARTY_RECONSTRUCTION_HANDLERS,
    IMMUTABLE_ANCHOR_POLICY,
    SCIENTIFIC_NONCLAIM,
    TICK_ONLY_INPUT_POLICY,
    FeedEpochTransitionPolicyV1,
    InformationMode,
    ModernReferenceMotifProfileV1,
    ObservationUncertaintyPolicyV1,
    ReconstructionDeliveryMode,
    ReconstructionPlanCompatibilityError,
    ReconstructionSourceInventoryV1,
    ReconstructionStoragePolicyV1,
    SyntheticInfillPlanV1,
    build_synthetic_infill_plan,
    load_reconstruction_stage_plan,
    read_reconstruction_plan_execution_manifest,
    read_reconstruction_plan_source_support_map,
    read_reconstruction_source_inventory,
    read_synthetic_infill_plan,
    validate_synthetic_infill_plan_for_execution,
    write_synthetic_infill_plan,
)
from histdatacom.synthetic import reconstruction_handlers as handlers_module
from histdatacom.synthetic import reconstruction_plan as plan_module
from histdatacom.synthetic import support_verification as support_module
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.generation import EMPIRICAL_MOTIF_GENERATOR_ID
from histdatacom.synthetic.proposal_engines import (
    ProposalEngineEvidenceV1,
    proposal_engine_default_configs,
)
from tests.fixtures.reconstruction_transition import (
    reconstruction_transition_fixture,
)

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
_PERIOD = "202001"
_START_MS = 1_577_836_800_000


def _ready_cftc_query(*_: Any, **__: Any) -> SimpleNamespace:
    return SimpleNamespace(
        status=plan_module.CftcPositioningQueryStatus.READY,
        reason="ready",
        query_id="cftc-positioning-query:test",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_tick_partition(
    path: Path,
    offset: int,
    *,
    timestamp_offset: int = 0,
    enriched: bool = False,
    invert_quote_order: bool = False,
    source_order_regression: bool = False,
    days: tuple[int, ...] = tuple(range(31)),
    max_chunksize: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamps = [
        _START_MS + day * 86_400_000 + minute * 60_000 + timestamp_offset
        for day in days
        for minute in range(3)
    ]
    if source_order_regression:
        timestamps[1], timestamps[2] = timestamps[2], timestamps[1]
    bids = [
        1.0 + offset / 1_000_000 + index / 10_000_000
        for index in range(len(timestamps))
    ]
    asks = [
        bid - 0.0002 if invert_quote_order else bid + 0.0002 for bid in bids
    ]
    table = pa.table(
        {
            "datetime": timestamps,
            "bid": bids,
            "ask": asks,
            "vol": [0] * len(timestamps),
        }
    )
    if enriched:
        frame = enrich_tick_cache_with_training_features(
            pl.from_arrow(table),
            symbol=path.parents[2].name.upper(),
            data_format="ascii",
            timeframe="T",
            period=_PERIOD,
        )
        precision_warning = [False] * len(timestamps)
        precision_warning[1] = True
        frame = frame.with_columns(
            pl.Series("dq_issue_precision_warning", precision_warning)
        )
        table = frame.to_arrow()
    with (
        pa.OSFile(str(path), "wb") as sink,
        ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table, max_chunksize=max_chunksize)


def _artifact(
    tmp_path: Path,
    role: str,
    kind: str,
    **identity: Any,
) -> Any:
    path = tmp_path / "inputs" / f"{role}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"role": role, **identity}, sort_keys=True),
        encoding="utf-8",
    )
    return artifact_ref_for_file(path, kind=kind)


def _resolved_inputs(
    tmp_path: Path,
    source_root: Path,
    *,
    enriched: bool = False,
    source_days_by_symbol: dict[str, tuple[int, ...]] | None = None,
    timestamp_offsets_by_symbol: dict[str, int] | None = None,
    inverted_symbols: frozenset[str] = frozenset(),
    source_order_regression_symbols: frozenset[str] = frozenset(),
    max_chunksize: int | None = None,
) -> Any:
    lineage: list[dict[str, str]] = []
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = source_root / symbol / "2020" / "1" / ".data"
        _write_tick_partition(
            path,
            ordinal,
            timestamp_offset=(
                timestamp_offsets_by_symbol[symbol]
                if timestamp_offsets_by_symbol is not None
                else 0
            ),
            enriched=enriched,
            invert_quote_order=symbol in inverted_symbols,
            source_order_regression=(symbol in source_order_regression_symbols),
            days=(
                source_days_by_symbol[symbol]
                if source_days_by_symbol is not None
                else tuple(range(31))
            ),
            max_chunksize=max_chunksize,
        )
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
            tmp_path,
            "feed-epochs",
            "feed_epoch_definition_v2",
            definition_id="feed-epochs-v2:test",
        ),
        "observation_operator": _artifact(
            tmp_path,
            "observation",
            "observation-operator",
            operator_id="observation-operator:test",
        ),
        "market_context": _artifact(
            tmp_path,
            "market-context",
            "market_context_corpus_v1",
            corpus_id="market-context:test",
        ),
        "cftc_positioning": _artifact(
            tmp_path,
            "cftc-positioning",
            "cftc_positioning_corpus_v1",
            corpus_id="cftc-positioning:test",
        ),
        "benchmark_manifest": _artifact(
            tmp_path,
            "benchmark",
            "reverse_degradation_manifest_v1",
            schema_version="histdatacom.reverse-degradation-manifest.v1",
            corpus={"corpus_id": "benchmark:test"},
        ),
        "motif_manifest": _artifact(
            tmp_path,
            "motif-manifest",
            "modern_reference_motif_manifest_v1",
            library_id="modern-reference-motif:test",
        ),
        "motif_index": _artifact(
            tmp_path,
            "motif-index",
            "modern_reference_motif_index_v1",
            index_id="motif-index:test",
        ),
        "motif_qualification": _artifact(
            tmp_path,
            "motif-qualification",
            "modern_reference_motif_qualification_v1",
            library_id="modern-reference-motif:test",
        ),
        "motif_leakage_audit": _artifact(
            tmp_path,
            "motif-leakage",
            "modern_reference_motif_leakage_audit_v1",
            library_id="modern-reference-motif:test",
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
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
    assert first.resources.retained_member_count == 2
    assert len(first.workflow_requests) == 2
    retention = json.loads(
        Path(first.artifact_graph["retention_plan"].path).read_text(
            encoding="utf-8"
        )
    )
    assert {
        task.window.ensemble_member_id
        for request in first.workflow_requests
        for task in request.tasks
    } == set(retention["retained_member_ids"])
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
    assert first.artifact_graph["evidence_policy"].kind == (
        "reconstruction_evidence_policy_v1"
    )
    assert first.artifact_graph["cross_series_constraint_policy"].kind == (
        "cross_series_constraint_policy_v1"
    )
    scientific_ledger = read_reconstruction_scientific_ledger(
        first.artifact_graph["scientific_ledger"].path
    )
    assert first.artifact_graph["scientific_ledger"].kind == (
        RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND
    )
    assert ReconstructionClient().scientific_ledger() == scientific_ledger
    source_support_map = read_reconstruction_plan_source_support_map(
        first.artifact_graph["source_support_map"].path
    )
    execution_manifest = read_reconstruction_plan_execution_manifest(
        first.artifact_graph["execution_manifest"].path
    )
    experiment = read_reconstruction_experiment(
        first.artifact_graph["experiment_manifest"].path
    )
    product_input = experiment.selection_for_role(
        ReconstructionExperimentRole.PRODUCT_INPUT
    )
    assert source_support_map.source_inventory_id == (
        execution_manifest.source_inventory_id
    )
    assert first.run.source_version_ids == (product_input.dataset_version_id,)
    scientific_binding = next(
        item
        for item in experiment.artifact_bindings
        if item.name == "scientific-ledger"
    )
    assert (
        scientific_binding.artifact == first.artifact_graph["scientific_ledger"]
    )
    assert scientific_binding.artifact_id == scientific_ledger.ledger_id
    assert source_support_map.windows == first.source_support
    assert all(
        item.selected_cross_series_alignment == "exact_event_sequence"
        for item in source_support_map.windows
    )
    translated_portfolio = ReconstructionClient().proposal_portfolio(
        plan_ref.path
    )
    assert tuple(item.engine_id for item in translated_portfolio.entries) == (
        "histdatacom.empirical-motif-resampling",
    )
    assert translated_portfolio.selected_engine_ids == (
        "histdatacom.empirical-motif-resampling",
    )
    assert len(ReconstructionClient().proposal_engines().descriptors) == 13

    validate_synthetic_infill_plan_for_execution(restored)
    legacy_unbound = replace(
        restored,
        artifact_graph={
            name: ref
            for name, ref in restored.artifact_graph.items()
            if name != "scientific_ledger"
        },
        plan_id="",
    )
    assert SyntheticInfillPlanV1.from_json(legacy_unbound.to_json()) == (
        legacy_unbound
    )
    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="scientific-ledger-unbound.*regenerate",
    ):
        validate_synthetic_infill_plan_for_execution(
            legacy_unbound, verify_artifacts=False
        )
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
        assert "evidence_policy" in loaded.execution_manifest.artifacts
        assert (
            "cross_series_constraint_policy"
            in loaded.execution_manifest.artifacts
        )
        assert "source_support_map" in loaded.execution_manifest.artifacts
        assert "scientific_ledger" in loaded.execution_manifest.artifacts

    source_command = task.commands[0]
    source_invocation = ReconstructionStageInvocationV1(
        run=restored.workflow_requests[0].run,
        task=task,
        command=source_command,
        prior_outcomes=(),
    )
    source_plan = load_reconstruction_stage_plan(source_command)
    source_events, _ = handlers_module._read_source_events(
        source_invocation,
        source_plan,
    )
    core_events = {
        symbol: tuple(
            event
            for event in events
            if task.window.owns_event_time(event.event_time_ns)
        )
        for symbol, events in source_events.items()
    }
    runtime_bundle, _, _ = handlers_module._compile_cross_series_constraints(
        source_invocation,
        source_plan,
        core_events,
    )
    handlers_module._require_planned_cross_series_support(
        source_invocation,
        source_plan,
        runtime_bundle,
    )

    triangle = next(
        item
        for item in runtime_bundle.windows
        if item.relation_kind.value == "triangle"
        and item.alignment.policy.value == "exact_event_sequence"
    )
    assert triangle.alignment.recommended_event_time_ns is not None
    mismatched_alignment = replace(
        triangle.alignment,
        recommended_event_time_ns=(
            triangle.alignment.recommended_event_time_ns + 1
        ),
    )
    mismatched_triangle = replace(
        triangle,
        alignment=mismatched_alignment,
        constraint_window_id="",
    )
    mismatched_bundle = replace(
        runtime_bundle,
        windows=tuple(
            mismatched_triangle if item is triangle else item
            for item in runtime_bundle.windows
        ),
        bundle_id="",
    )
    with pytest.raises(
        ValueError,
        match="runtime cross-series alignment differs from planned support",
    ):
        handlers_module._require_planned_cross_series_support(
            source_invocation,
            source_plan,
            mismatched_bundle,
        )
    broker_command = next(
        command
        for command in task.commands
        if command.stage is ReconstructionStage.BROKER_TRANSFER
    )
    assert broker_command.input_manifest_refs == ()


def test_execution_manifest_schedules_executable_scenario_policies(
    planned_environment: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    """The proposal stage receives both frozen executable policy artifacts."""
    source_root, kwargs = planned_environment
    plan = build_synthetic_infill_plan(source_root, **kwargs)
    execution = read_reconstruction_plan_execution_manifest(
        plan.artifact_graph["execution_manifest"].path
    )
    uncertainty_ref = _artifact(
        tmp_path,
        "observation-uncertainty-policy",
        "observation_uncertainty_policy_v1",
    )
    transition_ref = _artifact(
        tmp_path,
        "feed-epoch-transition-policy",
        "feed_epoch_transition_policy_v1",
    )
    rebound = replace(
        execution,
        artifacts={
            **execution.artifacts,
            "observation_uncertainty_policy": uncertainty_ref,
            "feed_epoch_transition_policy": transition_ref,
        },
        manifest_id="",
    )
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )

    commands = plan_module._stage_commands(
        plan.workflow_requests[0].tasks[0].window,
        scratch=Path(rebound.scratch_root),
        inventory=inventory,
        execution_manifest=rebound,
        execution_ref=plan.artifact_graph["execution_manifest"],
    )
    proposal = next(
        item for item in commands if item.stage is ReconstructionStage.PROPOSAL
    )

    assert uncertainty_ref in proposal.input_manifest_refs
    assert transition_ref in proposal.input_manifest_refs


def test_plan_records_source_empty_windows_without_fabricating_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        source_days_by_symbol={symbol: (0,) for symbol in _SYMBOLS},
    )
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )

    plan = build_synthetic_infill_plan(source_root, **_builder_kwargs(tmp_path))
    restored = SyntheticInfillPlanV1.from_json(plan.to_json())

    assert restored == plan
    assert plan.status == "ready_with_empty_windows"
    assert plan.resources.planned_window_count == 2
    assert plan.resources.executable_window_count == 1
    assert plan.resources.empty_window_count == 1
    assert plan.resources.refused_window_count == 0
    assert [item.status.value for item in plan.source_support] == [
        "complete",
        "empty",
    ]
    assert not any(plan.source_support[-1].core_event_counts.values())
    execution = read_reconstruction_plan_execution_manifest(
        plan.artifact_graph["execution_manifest"].path
    )
    assert execution.empty_window_ids == (plan.source_support[-1].support_id,)


def test_plan_refuses_a_partial_source_triangle_before_context_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        source_days_by_symbol={
            "eurgbp": (1,),
            "eurusd": (0,),
            "gbpusd": (0,),
        },
    )
    monkeypatch.setattr(
        plan_module, "_resolve_plan_inputs", lambda **_: resolved
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: pytest.fail(
            "context preflight must not run for incomplete source"
        ),
    )
    monkeypatch.setattr(
        plan_module,
        "query_cftc_positioning_corpus",
        lambda *_, **__: pytest.fail(
            "CFTC query must not run for incomplete source"
        ),
    )
    start_ns = int(
        datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )

    plan = build_synthetic_infill_plan(
        source_root,
        **_builder_kwargs(tmp_path),
        requested_start_ns=start_ns,
        requested_end_ns=start_ns + 86_400_000_000_000,
        window_size_ns=86_400_000_000_000,
    )

    assert plan.status == "ready_with_refusals"
    assert plan.resources.executable_window_count == 0
    assert plan.resources.empty_window_count == 0
    assert plan.resources.refused_window_count == 1
    assert not plan.workflow_requests
    assert plan.source_support[0].status.value == "incomplete"
    assert plan.source_support[0].core_event_counts["eurgbp"] == 0
    assert plan.refusals[0].code.value == "source_triangle_incomplete"


def test_plan_executes_bounded_nearest_triangle_without_exact_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        timestamp_offsets_by_symbol={
            "eurgbp": 0,
            "eurusd": 1,
            "gbpusd": 2,
        },
    )
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )

    plan = build_synthetic_infill_plan(source_root, **_builder_kwargs(tmp_path))

    assert plan.resources.executable_window_count == 2
    assert plan.resources.refused_window_count == 0
    assert all(item.status.value == "complete" for item in plan.source_support)
    assert all(
        item.common_exact_core_timestamp_count == 0
        for item in plan.source_support
    )
    assert all(
        item.selected_cross_series_alignment == "nearest_prior_bounded"
        for item in plan.source_support
    )
    assert all(
        item.bounded_nearest_core_timestamp_count > 0
        for item in plan.source_support
    )
    assert all(
        item.conditioning_mode
        is plan_module.ReconstructionCftcConditioningMode.CONDITIONED
        for item in plan.cftc_support
    )


def test_plan_projects_bounded_histdata_source_order_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        source_order_regression_symbols=frozenset({"eurgbp"}),
    )
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )

    plan = build_synthetic_infill_plan(source_root, **_builder_kwargs(tmp_path))
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    partition = next(
        item for item in inventory.partitions if item.symbol == "eurgbp"
    )

    assert plan.resources.executable_window_count == 2
    assert partition.artifact.metadata["timestamp_regression_count"] == 1
    assert (
        partition.artifact.metadata["maximum_timestamp_regression_ms"] == 60_000
    )
    assert all(
        item.common_exact_core_timestamp_count > 0
        for item in plan.source_support
        if item.status.value == "complete"
    )


@pytest.mark.parametrize(
    "query_status",
    [
        plan_module.CftcPositioningQueryStatus.PRE_COVERAGE,
        plan_module.CftcPositioningQueryStatus.MISSING,
        plan_module.CftcPositioningQueryStatus.STALE,
        plan_module.CftcPositioningQueryStatus.NOT_AVAILABLE,
        plan_module.CftcPositioningQueryStatus.RESTATEMENT_INCOMPLETE,
    ],
)
@pytest.mark.parametrize(
    "engine_id",
    sorted(plan_module.QUALIFIED_CFTC_UNAVAILABLE_ENGINE_IDS),
)
def test_unavailable_cftc_is_explicitly_qualified_or_refused(
    monkeypatch: pytest.MonkeyPatch,
    query_status: plan_module.CftcPositioningQueryStatus,
    engine_id: str,
) -> None:
    start_ns = int(
        datetime(2002, 3, 4, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )
    end_ns = start_ns + 86_400_000_000_000
    window = plan_module.ReconstructionWindowV1(
        run_id="reconstruction-run:test",
        ensemble_member_id="ensemble-member:test",
        symbols=_SYMBOLS,
        core_start_ns=start_ns,
        core_end_ns=end_ns,
    )
    source_support = plan_module.ReconstructionPlanSourceSupportV1(
        start_ns=start_ns,
        end_ns=end_ns,
        symbols=_SYMBOLS,
        core_event_counts={symbol: 3 for symbol in _SYMBOLS},
        input_event_counts={symbol: 3 for symbol in _SYMBOLS},
        common_exact_core_timestamp_count=3,
        status=plan_module.ReconstructionPlanSourceSupportStatus.COMPLETE,
        reason="complete source triangle",
    )
    qualification = (
        plan_module.ReconstructionContextAvailabilityQualificationV1(
            proposal_portfolio_id="proposal-portfolio:test",
            powered_qualification_dossier_id="powered-dossier:test",
            powered_engine_decision_id="engine-decision:test",
            carving_constraint_set_id="carving-constraints:test",
            selected_engine_ids=(engine_id,),
        )
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=()),
    )
    monkeypatch.setattr(
        plan_module,
        "query_cftc_positioning_corpus",
        lambda *_, **__: SimpleNamespace(
            status=query_status,
            reason="positioning state is unavailable",
            query_id="cftc-positioning-query:unavailable-test",
        ),
    )
    parameters = {
        "windows": (window,),
        "source_support": {
            (start_ns, end_ns): source_support,
        },
        "definition": SimpleNamespace(
            assign=lambda **_: SimpleNamespace(assignment_kind="assigned")
        ),
        "context": SimpleNamespace(),
        "positioning": SimpleNamespace(),
        "mode": InformationMode.EX_POST_RECONSTRUCTION,
    }

    refusals, executable, cftc_support = plan_module._preflight_window_support(
        **parameters,
        context_availability_qualification=qualification,
    )

    assert not refusals
    assert executable == (window,)
    assert cftc_support[0].query_status == query_status.value
    assert cftc_support[0].conditioning_mode.value == (
        plan_module.CFTC_UNAVAILABLE_CONDITIONING_MODE
    )
    assert cftc_support[0].qualification_id == qualification.qualification_id

    refused, executable, cftc_support = plan_module._preflight_window_support(
        **parameters,
        context_availability_qualification=None,
    )

    assert not executable
    assert refused[0].code.value == "cftc_positioning_unsupported"
    assert cftc_support[0].conditioning_mode is (
        plan_module.ReconstructionCftcConditioningMode.REFUSED
    )
    assert "lacks qualification" in cftc_support[0].reason


def test_unavailable_cftc_qualification_rejects_unisolated_engine() -> None:
    with pytest.raises(ValueError, match="structurally isolated"):
        plan_module.ReconstructionContextAvailabilityQualificationV1(
            proposal_portfolio_id="proposal-portfolio:test",
            powered_qualification_dossier_id="powered-dossier:test",
            powered_engine_decision_id="engine-decision:test",
            carving_constraint_set_id="carving-constraints:test",
            selected_engine_ids=(EMPIRICAL_MOTIF_GENERATOR_ID,),
        )


@pytest.mark.parametrize(
    "engine_id",
    sorted(plan_module.QUALIFIED_CFTC_UNAVAILABLE_ENGINE_IDS),
)
def test_planner_issues_unavailable_cftc_qualification_for_eligible_hawkes(
    engine_id: str,
) -> None:
    decision = SimpleNamespace(
        reconstruction_eligible=True,
        decision_id="engine-decision:test",
    )
    qualification = plan_module._build_context_availability_qualification(
        proposal_portfolio=SimpleNamespace(
            portfolio_id="proposal-portfolio:test",
            selected_engine_ids=(engine_id,),
        ),
        qualification_dossier=SimpleNamespace(
            dossier_id="powered-dossier:test",
            decision=lambda selected: (
                decision
                if selected == engine_id
                else pytest.fail("planner requested the wrong engine decision")
            ),
        ),
        carving_constraints=SimpleNamespace(
            constraint_set_id="carving-constraints:test",
            condition_policies=(),
        ),
    )

    assert qualification is not None
    assert qualification.selected_engine_ids == (engine_id,)
    assert qualification.powered_engine_decision_id == decision.decision_id


def test_planner_refuses_unavailable_cftc_with_matching_carving_policy() -> (
    None
):
    engine_id = "histdatacom.marked-hawkes.full_self_cross_excitation"

    with pytest.raises(ReconstructionPlanCompatibilityError, match="carving"):
        plan_module._build_context_availability_qualification(
            proposal_portfolio=SimpleNamespace(
                portfolio_id="proposal-portfolio:test",
                selected_engine_ids=(engine_id,),
            ),
            qualification_dossier=SimpleNamespace(
                dossier_id="powered-dossier:test",
                decision=lambda _: SimpleNamespace(
                    reconstruction_eligible=True,
                    decision_id="engine-decision:test",
                ),
            ),
            carving_constraints=SimpleNamespace(
                constraint_set_id="carving-constraints:test",
                condition_policies=(
                    SimpleNamespace(
                        match_tags=("cftc_positioning:legacy:futures_only",)
                    ),
                ),
            ),
        )


def test_transition_cardinality_support_matches_runtime_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition, operator = reconstruction_transition_fixture()
    operator = replace(
        operator,
        strata=tuple(
            replace(
                stratum,
                parameters=tuple(
                    (
                        replace(
                            parameter,
                            value=0.25,
                            lower=0.2,
                            upper=0.3,
                        )
                        if parameter.name == "retention_probability"
                        else parameter
                    )
                    for parameter in stratum.parameters
                ),
                stratum_id="",
            )
            for stratum in operator.strata
        ),
        operator_id="",
    )
    start_ns = int(
        datetime(2009, 5, 15, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )
    end_ns = start_ns + 86_400_000_000_000
    window = plan_module.ReconstructionWindowV1(
        run_id="reconstruction-run:test",
        ensemble_member_id="ensemble-member:test",
        symbols=_SYMBOLS,
        core_start_ns=start_ns,
        core_end_ns=end_ns,
    )
    support = plan_module.ReconstructionPlanSourceSupportV1(
        start_ns=start_ns,
        end_ns=end_ns,
        symbols=_SYMBOLS,
        core_event_counts={symbol: 300 for symbol in _SYMBOLS},
        input_event_counts={symbol: 300 for symbol in _SYMBOLS},
        common_exact_core_timestamp_count=300,
        status=plan_module.ReconstructionPlanSourceSupportStatus.COMPLETE,
        reason="complete source triangle",
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=()),
    )
    monkeypatch.setattr(
        plan_module, "query_cftc_positioning_corpus", _ready_cftc_query
    )

    parameters = {
        "windows": (window,),
        "source_support": {(start_ns, end_ns): support},
        "definition": definition,
        "observation_operator": operator,
        "context": SimpleNamespace(),
        "positioning": SimpleNamespace(),
        "context_availability_qualification": None,
        "transition_policy": FeedEpochTransitionPolicyV1(),
    }
    refusals, executable, _ = plan_module._preflight_window_support(
        **parameters,
        mode=InformationMode.EX_POST_RECONSTRUCTION,
    )

    assert not refusals
    assert executable == (window,)

    transition_policy = parameters["transition_policy"]
    uncertainty_policy = ObservationUncertaintyPolicyV1()
    proposal_config = proposal_engine_default_configs()[
        plan_module.QUALIFIED_CFTC_UNAVAILABLE_ENGINE_ID
    ]
    storage_policy = ReconstructionStoragePolicyV1()
    estimate = plan_module._window_resource_estimate(
        window,
        source_support=support,
        configuration=SimpleNamespace(
            generator_config=plan_module.EmpiricalMotifGeneratorConfigV1(),
            storage_policy=storage_policy,
        ),
        proposal_config=proposal_config,
        definition=definition,
        observation_operator=operator,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        observation_uncertainty_policy=uncertainty_policy,
        transition_policy=transition_policy,
    )
    lower_probabilities = tuple(
        plan_module.historical_product_retention_probability(
            operator,
            feed_epoch_label=next(
                iter(
                    {
                        definition.assign(
                            symbol=symbol,
                            timestamp_utc_ms=(start_ns + end_ns) // 2_000_000,
                        ).label
                        for symbol in _SYMBOLS
                    }
                )
            ),
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=(start_ns + end_ns) // 2,
            feed_epoch_definition=definition,
            retention_endpoint="lower",
            symbols=_SYMBOLS,
            transition_policy=transition_policy,
            transition_scenario_kind=kind,
        )
        for kind in transition_policy.scenario_order
    )
    assert estimate.candidate_event_count == (
        plan_module.observation_admission_missing_count_bound(
            sum(support.input_event_counts.values()),
            min(lower_probabilities),
            uncertainty_policy.admission_quantile,
        )
    )

    refusals, executable, _ = plan_module._preflight_window_support(
        **parameters,
        mode=InformationMode.EX_ANTE_SIMULATION,
    )

    assert not executable
    assert len(refusals) == 1
    assert refusals[0].code is (
        plan_module.ReconstructionPlanRefusalCode.FEED_EPOCH_UNSUPPORTED
    )
    assert "ex-ante forbidden" in refusals[0].reason


def test_adaptive_hawkes_windows_preserve_common_anchors_and_runtime_headroom(
    tmp_path: Path,
) -> None:
    definition, operator = reconstruction_transition_fixture()
    # Keep the adaptive-window behavior under test while using a controlled
    # qualified lower endpoint that leaves at least one millisecond admissible.
    operator = replace(
        operator,
        strata=tuple(
            (
                replace(
                    stratum,
                    parameters=tuple(
                        (
                            replace(parameter, lower=0.24)
                            if parameter.name == "retention_probability"
                            else parameter
                        )
                        for parameter in stratum.parameters
                    ),
                    stratum_id="",
                )
                if stratum.key == "epoch|epoch_id=technology_epoch_01"
                else stratum
            )
            for stratum in operator.strata
        ),
        operator_id="",
    )
    start_ms = 1_015_200_000_000
    end_ms = start_ms + 86_400_000
    row_count = 2_402
    timestamps = [
        start_ms - 60_000 + index * 30_000 for index in range(row_count)
    ]
    partitions = []
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = tmp_path / "ASCII" / "T" / symbol / "2002" / "3" / ".data"
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "datetime": timestamps,
                "bid": [
                    1.0 + ordinal / 100 + index / 1_000_000
                    for index in range(row_count)
                ],
                "ask": [
                    1.0002 + ordinal / 100 + index / 1_000_000
                    for index in range(row_count)
                ],
                "vol": [0] * row_count,
            }
        )
        with (
            pa.OSFile(str(path), "wb") as sink,
            ipc.new_file(sink, table.schema) as writer,
        ):
            writer.write_table(table, max_chunksize=300)
        ref = artifact_ref_for_file(
            path,
            kind=ASCII_TICK_SOURCE_KIND,
            metadata={
                "symbol": symbol,
                "period": "200203",
                "timestamp_regression_count": 0,
                "maximum_timestamp_regression_ms": 0,
            },
        )
        partitions.append(
            plan_module.ReconstructionSourcePartitionV1(
                symbol=symbol,
                period="200203",
                artifact=ref,
                row_count=row_count,
                coverage_start_ns=1_014_940_800_000_000_000,
                coverage_end_ns=1_017_619_200_000_000_000,
                first_timestamp_ms=timestamps[0],
                last_timestamp_ms=timestamps[-1],
                feed_epoch_evidence_id=f"feed-evidence:{symbol}:200203",
            )
        )
    inventory = plan_module.ReconstructionSourceInventoryV1(
        source_root=str(tmp_path / "ASCII" / "T"),
        symbols=_SYMBOLS,
        periods=("200203",),
        partitions=tuple(partitions),
        requested_start_ns=start_ms * 1_000_000,
        requested_end_ns=end_ms * 1_000_000,
        total_row_count=row_count * len(_SYMBOLS),
        total_size_bytes=sum(
            int(item.artifact.size_bytes or 0) for item in partitions
        ),
    )
    run = plan_module.ReconstructionRunV1(
        symbols=_SYMBOLS,
        source_version_ids=("source-version:test",),
        configuration_ids=("configuration:test",),
        ensemble_member_ids=("ensemble-member:a", "ensemble-member:b"),
        base_seed=1,
        storage_policy=ReconstructionStoragePolicyV1(),
    )
    coverages = plan_module._source_coverages(inventory)
    plans = tuple(
        plan_module.plan_cross_currency_windows(
            run,
            ensemble_member_id=member_id,
            requested_start_ns=start_ms * 1_000_000,
            requested_end_ns=end_ms * 1_000_000,
            window_size_ns=86_400_000_000_000,
            coverages=coverages,
            left_halo_ns=60_000_000_000,
        )
        for member_id in run.ensemble_member_ids
    )
    support = (
        plan_module.ReconstructionPlanSourceSupportV1(
            start_ns=start_ms * 1_000_000,
            end_ns=end_ms * 1_000_000,
            symbols=_SYMBOLS,
            core_event_counts={symbol: 2_400 for symbol in _SYMBOLS},
            input_event_counts={symbol: row_count for symbol in _SYMBOLS},
            common_exact_core_timestamp_count=2_400,
            status=plan_module.ReconstructionPlanSourceSupportStatus.COMPLETE,
            reason="complete source triangle",
        ),
    )
    engine_id = plan_module.QUALIFIED_CFTC_UNAVAILABLE_ENGINE_ID
    config = proposal_engine_default_configs()[engine_id]

    adapted, audit, cardinality_refusals = (
        plan_module._adapt_cross_currency_window_plans_for_cardinality(
            plans,
            initial_source_support=support,
            inventory=inventory,
            definition=definition,
            observation_operator=operator,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            proposal_engine_id=engine_id,
            proposal_config=config,
            storage_policy=run.storage_policy,
            observation_uncertainty_policy=ObservationUncertaintyPolicyV1(),
            requested_max_window_size_ns=86_400_000_000_000,
        )
    )
    adapted_support = plan_module._build_exact_source_support(
        adapted[0].windows, inventory=inventory
    )

    assert audit.initial_window_count == 1
    assert audit.final_window_count > 1
    assert audit.subdivided_window_count == 1
    assert not cardinality_refusals
    assert (
        audit.maximum_modeled_missing_events
        <= audit.modeled_missing_event_limit
    )
    assert adapted[0].common_start_ns == start_ms * 1_000_000
    assert adapted[0].common_end_ns == end_ms * 1_000_000
    assert tuple(
        (item.core_start_ns, item.core_end_ns) for item in adapted[0].windows
    ) == tuple(
        (item.core_start_ns, item.core_end_ns) for item in adapted[1].windows
    )
    assert all(
        item.common_exact_core_timestamp_count > 0 for item in adapted_support
    )
    uncertainty_policy = ObservationUncertaintyPolicyV1()
    planning_configuration = SimpleNamespace(
        generator_config=plan_module.EmpiricalMotifGeneratorConfigV1(),
        storage_policy=run.storage_policy,
    )
    for window, exact_support in zip(
        adapted[0].windows, adapted_support, strict=True
    ):
        estimate = plan_module._window_resource_estimate(
            window,
            source_support=exact_support,
            configuration=planning_configuration,
            proposal_config=config,
            definition=definition,
            observation_operator=operator,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            observation_uncertainty_policy=uncertainty_policy,
        )
        observed = sum(exact_support.input_event_counts.values())
        retention = plan_module.historical_product_retention_probability(
            operator,
            feed_epoch_label="technology_epoch_01",
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=(window.core_start_ns + window.core_end_ns) // 2,
            feed_epoch_definition=definition,
            retention_endpoint="lower",
        )
        assert estimate.input_event_count == observed
        assert estimate.candidate_event_count == (
            plan_module.observation_admission_missing_count_bound(
                observed,
                retention,
                uncertainty_policy.admission_quantile,
            )
        )


@pytest.mark.parametrize("duration_ms", [1, 60_000])
def test_cardinality_amplification_overflow_is_a_finite_bounded_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    duration_ms: int,
) -> None:
    definition, operator = reconstruction_transition_fixture()
    operator = replace(
        operator,
        strata=tuple(
            (
                replace(
                    stratum,
                    parameters=tuple(
                        (
                            replace(parameter, lower=0.001)
                            if parameter.name == "retention_probability"
                            else parameter
                        )
                        for parameter in stratum.parameters
                    ),
                    stratum_id="",
                )
                if stratum.key == "epoch|epoch_id=technology_epoch_01"
                else stratum
            )
            for stratum in operator.strata
        ),
        operator_id="",
    )
    start_ms = 1_015_200_000_000
    end_ms = start_ms + duration_ms
    timestamps = [start_ms - 60_000, start_ms - 1, start_ms]
    partitions = []
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = tmp_path / "ASCII" / "T" / symbol / "2002" / "3" / ".data"
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "datetime": timestamps,
                "bid": [
                    1.0 + ordinal / 100 + index / 1_000_000
                    for index in range(3)
                ],
                "ask": [
                    1.0002 + ordinal / 100 + index / 1_000_000
                    for index in range(3)
                ],
                "vol": [0, 0, 0],
            }
        )
        with (
            pa.OSFile(str(path), "wb") as sink,
            ipc.new_file(sink, table.schema) as writer,
        ):
            writer.write_table(table)
        ref = artifact_ref_for_file(
            path,
            kind=ASCII_TICK_SOURCE_KIND,
            metadata={
                "symbol": symbol,
                "period": "200203",
                "timestamp_regression_count": 0,
                "maximum_timestamp_regression_ms": 0,
            },
        )
        partitions.append(
            plan_module.ReconstructionSourcePartitionV1(
                symbol=symbol,
                period="200203",
                artifact=ref,
                row_count=3,
                coverage_start_ns=1_014_940_800_000_000_000,
                coverage_end_ns=1_017_619_200_000_000_000,
                first_timestamp_ms=timestamps[0],
                last_timestamp_ms=timestamps[-1],
                feed_epoch_evidence_id=f"feed-evidence:{symbol}:200203",
            )
        )
    inventory = plan_module.ReconstructionSourceInventoryV1(
        source_root=str(tmp_path / "ASCII" / "T"),
        symbols=_SYMBOLS,
        periods=("200203",),
        partitions=tuple(partitions),
        requested_start_ns=start_ms * 1_000_000,
        requested_end_ns=end_ms * 1_000_000,
        total_row_count=9,
        total_size_bytes=sum(
            int(item.artifact.size_bytes or 0) for item in partitions
        ),
    )
    run = plan_module.ReconstructionRunV1(
        symbols=_SYMBOLS,
        source_version_ids=("source-version:test",),
        configuration_ids=("configuration:test",),
        ensemble_member_ids=("ensemble-member:a",),
        base_seed=1,
        storage_policy=ReconstructionStoragePolicyV1(),
    )
    plans = (
        plan_module.plan_cross_currency_windows(
            run,
            ensemble_member_id=run.ensemble_member_ids[0],
            requested_start_ns=start_ms * 1_000_000,
            requested_end_ns=end_ms * 1_000_000,
            window_size_ns=duration_ms * 1_000_000,
            coverages=plan_module._source_coverages(inventory),
            left_halo_ns=60_000_000_000,
        ),
    )
    initial_support = (
        plan_module.ReconstructionPlanSourceSupportV1(
            start_ns=start_ms * 1_000_000,
            end_ns=end_ms * 1_000_000,
            symbols=_SYMBOLS,
            core_event_counts={symbol: 1 for symbol in _SYMBOLS},
            input_event_counts={symbol: 3 for symbol in _SYMBOLS},
            common_exact_core_timestamp_count=1,
            status=plan_module.ReconstructionPlanSourceSupportStatus.COMPLETE,
            reason="complete source triangle",
        ),
    )
    engine_id = plan_module.QUALIFIED_CFTC_UNAVAILABLE_ENGINE_ID
    config = proposal_engine_default_configs()[engine_id]

    adapted, audit, cardinality_refusals = (
        plan_module._adapt_cross_currency_window_plans_for_cardinality(
            plans,
            initial_source_support=initial_support,
            inventory=inventory,
            definition=definition,
            observation_operator=operator,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            proposal_engine_id=engine_id,
            proposal_config=config,
            storage_policy=run.storage_policy,
            observation_uncertainty_policy=ObservationUncertaintyPolicyV1(),
            requested_max_window_size_ns=duration_ms * 1_000_000,
        )
    )

    assert len(adapted[0].windows) == 1
    assert audit.minimum_window_size_ns == duration_ms * 1_000_000
    assert audit.final_window_count == 1
    assert audit.subdivided_window_count == 0
    assert audit.maximum_modeled_missing_events == 0
    assert len(cardinality_refusals) == 1
    assert audit.cardinality_refusal_count == 1
    assert audit.maximum_refused_modeled_missing_events > (
        audit.modeled_missing_event_limit
    )
    assert (
        plan_module.ReconstructionWindowSizingAuditV1.from_dict(audit.to_dict())
        == audit
    )
    cardinality_refusal = cardinality_refusals[0]
    assert cardinality_refusal.code is (
        plan_module.ReconstructionPlanRefusalCode.OBSERVATION_CARDINALITY_UNSUPPORTED
    )
    assert "requires " in cardinality_refusal.reason
    assert "amplification headroom" in cardinality_refusal.reason
    assert "subdivision cannot repair" in cardinality_refusal.reason
    assert "inf" not in cardinality_refusal.reason.lower()

    exact_support = plan_module._build_exact_source_support(
        adapted[0].windows, inventory=inventory
    )
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=()),
    )
    monkeypatch.setattr(
        plan_module, "query_cftc_positioning_corpus", _ready_cftc_query
    )
    refusals, executable, cftc_support = plan_module._preflight_window_support(
        adapted[0].windows,
        source_support={
            (item.start_ns, item.end_ns): item for item in exact_support
        },
        definition=definition,
        observation_operator=operator,
        context=SimpleNamespace(),
        positioning=SimpleNamespace(),
        mode=InformationMode.EX_POST_RECONSTRUCTION,
        context_availability_qualification=None,
        cardinality_refusals=cardinality_refusals,
    )

    assert refusals == cardinality_refusals
    assert not executable
    assert cftc_support[0].conditioning_mode is (
        plan_module.ReconstructionCftcConditioningMode.CONDITIONED
    )


def test_unsupported_cftc_query_cannot_use_unconditioned_availability_mode() -> (
    None
):
    with pytest.raises(ValueError, match="unavailable CFTC support"):
        plan_module.ReconstructionPlanCftcSupportV1(
            start_ns=1,
            end_ns=2,
            source_support_id="source-support:test",
            query_status=(
                plan_module.CftcPositioningQueryStatus.UNSUPPORTED.value
            ),
            conditioning_mode=(
                plan_module.ReconstructionCftcConditioningMode.UNCONDITIONED_UNAVAILABLE
            ),
            reason="unsupported query shape",
            query_id="cftc-positioning-query:unsupported-test",
            qualification_id="context-availability-qualification:test",
        )


def test_non_motif_portfolio_does_not_require_motif_generator_promotion(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root, kwargs = planned_environment
    resolved = plan_module._resolve_plan_inputs(
        feed_epoch_definition_path=kwargs["feed_epoch_definition_path"],
        observation_operator_path=kwargs["observation_operator_path"],
        market_context_corpus_path=kwargs["market_context_corpus_path"],
        cftc_positioning_corpus_path=kwargs["cftc_positioning_corpus_path"],
        benchmark_manifest_path=kwargs["benchmark_manifest_path"],
        motif_manifest_path=kwargs["motif_manifest_path"],
        motif_index_path=kwargs["motif_index_path"],
        motif_qualification_path=kwargs["motif_qualification_path"],
        motif_leakage_audit_path=kwargs["motif_leakage_audit_path"],
        symbols=_SYMBOLS,
        require_motif_promotion=False,
    )
    observed: dict[str, bool] = {}

    def capture_resolution(**arguments: Any) -> Any:
        observed["require_motif_promotion"] = arguments[
            "require_motif_promotion"
        ]
        return resolved

    monkeypatch.setattr(plan_module, "_resolve_plan_inputs", capture_resolution)
    engine_id = "histdatacom.marked-hawkes.diagonal_self_excitation"
    monkeypatch.setattr(
        plan_module,
        "read_proposal_evidence_campaigns",
        lambda _: (),
    )
    monkeypatch.setattr(
        plan_module,
        "proposal_evidence_from_campaigns",
        lambda _: (),
    )

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="marked-Hawkes product selection requires",
    ):
        build_synthetic_infill_plan(
            source_root,
            **kwargs,
            proposal_engine_ids=(engine_id,),
            selected_proposal_engine_ids=(engine_id,),
        )

    assert observed == {"require_motif_promotion": False}


def test_v2_spec_round_trips_explicit_portfolio_without_hidden_default(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_root, kwargs = planned_environment
    scorecard = tmp_path / "retained-scorecard.json"
    scorecard.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        plan_module, "read_proposal_evidence_campaigns", lambda _: ()
    )
    config_id = proposal_engine_default_configs()[
        EMPIRICAL_MOTIF_GENERATOR_ID
    ].config_id
    monkeypatch.setattr(
        plan_module,
        "proposal_evidence_from_campaigns",
        lambda _: (
            ProposalEngineEvidenceV1(
                engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
                campaign_id="campaign:sha256:" + "1" * 64,
                corpus_id="benchmark:test",
                report_id="report:sha256:" + "2" * 64,
                candidate_id="candidate:sha256:" + "3" * 64,
                method_name="empirical_motif",
                promotion_eligible=True,
                provisional=False,
                failure_count=0,
                refusal_count=0,
                failed_gate_ids=(),
                config_ids=(config_id,),
                fit_ids=(),
                checkpoint_ids=(),
                training_dataset_ids=(),
            ),
        ),
    )
    base = _public_spec(source_root, kwargs)
    payload = base.to_dict()
    payload.update(
        {
            "schema_version": "histdatacom.reconstruction-plan-spec.v2",
            "proposal_engine_ids": ["histdatacom.empirical-motif-resampling"],
            "selected_proposal_engine_ids": [
                "histdatacom.empirical-motif-resampling"
            ],
            "proposal_evaluation_paths": [str(scorecard)],
        }
    )
    spec = ReconstructionPlanSpecV2.from_dict(payload)
    spec_path = tmp_path / "plan-spec-v2.json"
    spec_path.write_text(json.dumps(spec.to_dict()), encoding="utf-8")

    restored = read_plan_spec(spec_path)
    client = ReconstructionClient()
    plan_set_ref = client.construct_plan_set(restored, periods_per_shard=1)
    plan_set = read_reconstruction_plan_set(plan_set_ref.path)
    portfolio = client.proposal_portfolio(plan_set.shards[0].plan_ref.path)

    assert isinstance(restored, ReconstructionPlanSpecV2)
    assert restored == spec
    assert isinstance(plan_set.source_spec, ReconstructionPlanSpecV2)
    assert plan_set.source_spec == spec
    assert portfolio.selected_engine_ids == spec.selected_proposal_engine_ids


def test_execution_rejects_registry_from_changed_installed_code(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root, kwargs = planned_environment
    plan = build_synthetic_infill_plan(source_root, **kwargs)
    monkeypatch.setattr(plan_module, "proposal_engine_registry", object)

    with pytest.raises(
        ReconstructionPlanCompatibilityError,
        match="differs from installed code",
    ):
        validate_synthetic_infill_plan_for_execution(
            plan, verify_artifacts=False
        )


def test_catalog_selector_reproduces_legacy_translation_and_binds_experiment(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    legacy = build_synthetic_infill_plan(source_root, **kwargs)
    catalog_path = legacy.artifact_graph["dataset_catalog"].path
    catalog_spec = replace(
        _public_spec(source_root, kwargs),
        source_root=None,
        dataset_catalog_path=catalog_path,
        dataset_reference="reconstruction-selected",
    )
    client = ReconstructionClient()

    compatibility = client.compatibility(
        catalog_spec,
        inspect_source=True,
        inspect_artifacts=False,
    )
    selected_ref = client.construct_plan(catalog_spec)
    selected = read_synthetic_infill_plan(selected_ref.path)
    experiment = read_reconstruction_experiment(
        selected.artifact_graph["experiment_manifest"].path
    )

    assert compatibility.executable
    assert selected == legacy
    assert experiment.experiment_id in selected.run.configuration_ids
    assert experiment.dataset_version_ids == (
        experiment.selections[0].dataset_version_id,
    )
    assert (
        selected.artifact_graph["dataset_catalog"].kind == "dataset_catalog_v1"
    )
    assert selected.artifact_graph["dataset_resolution"].kind == (
        "dataset_resolution_receipt_v1"
    )


def test_source_reader_preserves_complete_enriched_cache_row_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(tmp_path, source_root, enriched=True)
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )
    plan = build_synthetic_infill_plan(source_root, **_builder_kwargs(tmp_path))
    task = plan.workflow_requests[0].tasks[0]
    command = task.commands[0]
    invocation = ReconstructionStageInvocationV1(
        run=plan.run,
        task=task,
        command=command,
        prior_outcomes=(),
    )

    source_events, cached = handlers_module._read_source_events(
        invocation, load_reconstruction_stage_plan(command)
    )

    assert all(len(events) == 90 for events in source_events.values())
    assert len(cached) == 3
    for schema_version, rows, complete in cached.values():
        assert schema_version == "histdatacom.ascii-tick-training-features.v1"
        assert complete
        assert rows[2]["precision_warning"] is True


def test_source_partition_selection_includes_recorded_month_spill(
    tmp_path: Path,
) -> None:
    february_start_ms = 1_580_515_200_000
    partitions = []
    for period, first_ms, last_ms in (
        ("202001", 1_577_836_800_000, february_start_ms + 3_600_000),
        ("202002", february_start_ms, 1_583_020_799_999),
    ):
        for symbol in _SYMBOLS:
            ref = _artifact(
                tmp_path,
                f"{symbol}-{period}",
                ASCII_TICK_SOURCE_KIND,
            )
            ref = plan_module.ArtifactRef(
                kind=ref.kind,
                path=ref.path,
                size_bytes=ref.size_bytes,
                sha256=ref.sha256,
                metadata={"symbol": symbol, "period": period},
            )
            partitions.append(
                plan_module.ReconstructionSourcePartitionV1(
                    symbol=symbol,
                    period=period,
                    artifact=ref,
                    row_count=2,
                    coverage_start_ns=(
                        1_577_836_800_000_000_000
                        if period == "202001"
                        else 1_580_515_200_000_000_000
                    ),
                    coverage_end_ns=(
                        1_580_515_200_000_000_000
                        if period == "202001"
                        else 1_583_020_800_000_000_000
                    ),
                    first_timestamp_ms=first_ms,
                    last_timestamp_ms=last_ms,
                    feed_epoch_evidence_id=f"feed-evidence:{symbol}:{period}",
                )
            )
    inventory = plan_module.ReconstructionSourceInventoryV1(
        source_root=str(tmp_path),
        symbols=_SYMBOLS,
        periods=("202001", "202002"),
        partitions=tuple(partitions),
        requested_start_ns=1_577_836_800_000_000_000,
        requested_end_ns=1_583_020_800_000_000_000,
        total_row_count=12,
        total_size_bytes=sum(
            int(item.artifact.size_bytes or 0) for item in partitions
        ),
    )
    window = plan_module.ReconstructionWindowV1(
        run_id="reconstruction-run:test",
        ensemble_member_id="ensemble-member:test",
        symbols=_SYMBOLS,
        core_start_ns=february_start_ms * 1_000_000,
        core_end_ns=(february_start_ms + 1_800_000) * 1_000_000,
    )

    selected = inventory.partitions_for_window(window)

    assert len(selected) == 6
    assert {item.period for item in selected} == {"202001", "202002"}


def test_source_reader_prunes_batches_without_losing_raw_row_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        inverted_symbols=frozenset({"eurgbp"}),
        max_chunksize=3,
    )
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )
    start_ns = (_START_MS + 14 * 86_400_000 + 60_000) * 1_000_000
    end_ns = start_ns + 10 * 60_000_000_000
    plan = build_synthetic_infill_plan(
        source_root,
        **_builder_kwargs(tmp_path),
        requested_start_ns=start_ns,
        requested_end_ns=end_ns,
        window_size_ns=end_ns - start_ns,
    )
    task = plan.workflow_requests[0].tasks[0]
    command = task.commands[0]
    invocation = ReconstructionStageInvocationV1(
        run=plan.run,
        task=task,
        command=command,
        prior_outcomes=(),
    )

    source_events, cached = handlers_module._read_source_events(
        invocation, load_reconstruction_stage_plan(command)
    )

    assert {
        symbol: tuple(event.source_row_id for event in events)
        for symbol, events in source_events.items()
    } == {symbol: (44, 45) for symbol in _SYMBOLS}
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    eurgbp = next(
        item for item in inventory.partitions if item.symbol == "eurgbp"
    )
    _, rows, complete = cached[eurgbp.partition_id]
    assert complete is False
    assert rows == {
        44: {"quote_order_projected": True},
        45: {"quote_order_projected": True},
    }


def test_atomic_cleanup_retains_only_retry_evidence(tmp_path: Path) -> None:
    scratch = tmp_path / "scratch"
    proposal = scratch / "proposal-batches" / "ledger.ndjson.gz"
    proposal.parent.mkdir(parents=True)
    proposal.write_bytes(b"transient")
    recovery = scratch / "validation" / "descriptor.json"
    recovery.parent.mkdir(parents=True)
    recovery.write_text('{"recovery":true}\n', encoding="utf-8")
    mirror = recovery.parent / "product-manifest.json"
    mirror.write_text('{"manifest":true}\n', encoding="utf-8")
    recovery_ref = artifact_ref_for_file(
        recovery, kind="reconstruction_staging_descriptor_v2"
    )
    invocation = SimpleNamespace(
        task=SimpleNamespace(scratch_directory=str(scratch))
    )

    removed = handlers_module._cleanup_committed_window_scratch(
        invocation,
        recovery_ref=recovery_ref,
    )

    assert removed >= len(b"transient")
    assert not proposal.parent.exists()
    assert recovery.is_file()
    assert mirror.is_file()


def test_source_inventory_carries_quote_projection_into_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    resolved = _resolved_inputs(
        tmp_path,
        source_root,
        inverted_symbols=frozenset({"eurgbp"}),
    )
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
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )

    plan = build_synthetic_infill_plan(source_root, **_builder_kwargs(tmp_path))
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    eurgbp = next(
        item for item in inventory.partitions if item.symbol == "eurgbp"
    )
    assert eurgbp.artifact.metadata["raw_negative_spread_count"] == 93
    assert eurgbp.artifact.metadata["quote_order_projection_policy"] == (
        "rowwise-min-bid-max-ask-preserve-raw-v1"
    )
    task = plan.workflow_requests[0].tasks[0]
    command = task.commands[0]
    invocation = ReconstructionStageInvocationV1(
        run=plan.run,
        task=task,
        command=command,
        prior_outcomes=(),
    )

    source_events, cached = handlers_module._read_source_events(
        invocation, load_reconstruction_stage_plan(command)
    )

    assert all(event.ask >= event.bid for event in source_events["eurgbp"])
    _, rows, _ = cached[eurgbp.partition_id]
    assert rows
    assert all(
        metrics.get("quote_order_projected") is True
        for metrics in rows.values()
    )


def test_typed_public_facade_constructs_requests_and_preflights(
    planned_environment: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Public callers need no private imports from construction to dry-run."""
    source_root, kwargs = planned_environment
    spec = _public_spec(source_root, kwargs)
    client = ReconstructionClient()

    plan_ref = client.construct_plan(spec)
    request = client.create_request(
        plan_ref.path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        acknowledge_scientific_nonclaim=True,
    )
    request_path = write_execution_request(request, tmp_path / "request.json")
    restored = read_execution_request(request_path)
    preflight = client.preflight(restored)

    assert restored == request
    assert preflight.executable
    assert preflight.status == "ready"
    assert preflight.dry_run["information_mode"] == "ex_post_reconstruction"
    assert preflight.dry_run["resources"]["workflow_request_count"] == 2
    assert "benchmark_manifest" in preflight.evidence_refs
    assert "information_audit" in preflight.evidence_refs
    assert "evidence_policy" in preflight.evidence_refs
    assert "cross_series_constraint_policy" in preflight.evidence_refs


def test_public_plan_spec_round_trips_and_applies_histdata_evidence_policy(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    source_root, kwargs = planned_environment
    policy = ReconstructionEvidencePolicyV1(
        suspicious_gap_fallback_ms=86_400_000,
        wide_spread_multiplier=4.0,
        max_records=128,
        max_row_records=32,
    )
    cross_policy = CrossSeriesConstraintPolicyV1(
        nearest_prior_max_age_ns=4_000_000_000,
        max_staleness_ns=20_000_000_000,
        max_alignment_samples=32,
    )
    spec = replace(
        _public_spec(source_root, kwargs),
        evidence_policy=policy,
        cross_series_constraint_policy=cross_policy,
    )

    restored = ReconstructionPlanSpecV1.from_dict(spec.to_dict())
    plan_ref = ReconstructionClient().construct_plan(restored)
    plan = read_synthetic_infill_plan(plan_ref.path)
    installed = read_reconstruction_evidence_policy(
        plan.artifact_graph["evidence_policy"].path
    )
    installed_cross_policy = read_cross_series_constraint_policy(
        plan.artifact_graph["cross_series_constraint_policy"].path
    )
    assert restored.evidence_policy == policy
    assert restored.cross_series_constraint_policy == cross_policy
    assert installed == policy
    assert installed_cross_policy == cross_policy
    assert policy.policy_id in plan.run.configuration_ids
    assert cross_policy.policy_id in plan.run.configuration_ids
    assert plan.artifact_graph["evidence_policy"].sha256
    assert plan.artifact_graph["cross_series_constraint_policy"].sha256


def test_public_plan_spec_rejects_alternate_evidence_provider() -> None:
    with pytest.raises(
        ReconstructionUnsupportedError,
        match="supports only HistData.com",
    ):
        ReconstructionPlanSpecV1(
            source_root="/tmp/ASCII/T",
            feed_epoch_definition_path="/tmp/feed.json",
            observation_operator_path="/tmp/observation.json",
            market_context_corpus_path="/tmp/context.json",
            cftc_positioning_corpus_path="/tmp/cftc.json",
            benchmark_manifest_path="/tmp/benchmark.json",
            motif_manifest_path="/tmp/motif.json",
            motif_index_path="/tmp/index.json",
            motif_qualification_path="/tmp/qualification.json",
            motif_leakage_audit_path="/tmp/leakage.json",
            artifact_root="/tmp/artifacts",
            output_root="/tmp/output",
            checkpoint_root="/tmp/checkpoints",
            scratch_root="/tmp/scratch",
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            start_period="202001",
            end_period="202001",
            evidence_policy=ReconstructionEvidencePolicyV1(
                supported_provider_ids=("histdata.com", "oanda")
            ),
        )


def test_public_plan_spec_defers_cross_series_broker_adapters() -> None:
    with pytest.raises(
        ReconstructionUnsupportedError,
        match="cross-series policy supports only HistData.com",
    ):
        ReconstructionPlanSpecV1(
            source_root="/tmp/ASCII/T",
            feed_epoch_definition_path="/tmp/feed.json",
            observation_operator_path="/tmp/observation.json",
            market_context_corpus_path="/tmp/context.json",
            cftc_positioning_corpus_path="/tmp/cftc.json",
            benchmark_manifest_path="/tmp/benchmark.json",
            motif_manifest_path="/tmp/motif.json",
            motif_index_path="/tmp/index.json",
            motif_qualification_path="/tmp/qualification.json",
            motif_leakage_audit_path="/tmp/leakage.json",
            artifact_root="/tmp/artifacts",
            output_root="/tmp/output",
            checkpoint_root="/tmp/checkpoints",
            scratch_root="/tmp/scratch",
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            start_period="202001",
            end_period="202001",
            cross_series_constraint_policy=CrossSeriesConstraintPolicyV1(
                supported_provider_ids=("histdata.com", "oanda")
            ),
        )


def test_public_plan_set_shards_and_revalidates_bounded_full_range(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A range plan remains public and strong without one unbounded payload."""
    source_root, kwargs = planned_environment
    spec = replace(
        _public_spec(source_root, kwargs),
        window_size_ns=24 * 60 * 60 * 1_000_000_000,
    )
    client = ReconstructionClient()
    ordinary_construct = client._construct_plan_model

    def resource_bounded_construct(
        shard_spec: ReconstructionPlanSpecV1,
    ) -> SyntheticInfillPlanV1:
        assert shard_spec.requested_start_ns is not None
        assert shard_spec.requested_end_ns is not None
        if (
            shard_spec.requested_end_ns - shard_spec.requested_start_ns
            > 8 * 24 * 60 * 60 * 1_000_000_000
        ):
            raise ReconstructionPlanError(
                "reconstruction persistence preflight failed: fixture bound"
            )
        return ordinary_construct(shard_spec)

    monkeypatch.setattr(
        client, "_construct_plan_model", resource_bounded_construct
    )

    ref = client.construct_plan_set(spec, periods_per_shard=1)
    plan_set = read_reconstruction_plan_set(ref.path)
    preflight = client.preflight_plan_set(ref.path)
    support_ref = client.construct_plan_support_map(
        ref.path, output_directory=source_root / "support-map"
    )
    support_map = read_reconstruction_plan_support_map(support_ref.path)

    assert isinstance(plan_set, ReconstructionPlanSetV1)
    assert plan_set.status == "ready"
    assert plan_set.executable
    assert len(plan_set.shards) == 4
    assert all(item.start_period == _PERIOD for item in plan_set.shards)
    assert all(item.end_period == _PERIOD for item in plan_set.shards)
    assert plan_set.resource_summary["plan_shard_count"] == 4
    assert plan_set.resource_summary["planned_window_count"] == 31
    artifact_root = Path(spec.artifact_root).resolve()
    output_root = Path(spec.output_root).resolve()
    checkpoint_root = Path(spec.checkpoint_root).resolve()
    scratch_root = Path(spec.scratch_root).resolve()
    for shard in plan_set.shards:
        shard_plan_path = Path(shard.plan_ref.path).resolve()
        assert shard_plan_path.is_relative_to(artifact_root)
        shard_plan = read_synthetic_infill_plan(shard_plan_path)
        execution = read_reconstruction_plan_execution_manifest(
            shard_plan.artifact_graph["execution_manifest"].path
        )
        assert Path(execution.output_root).is_relative_to(output_root)
        assert Path(execution.checkpoint_root).is_relative_to(checkpoint_root)
        assert Path(execution.scratch_root).is_relative_to(scratch_root)
        assert not Path(execution.output_root).is_relative_to(artifact_root)
        assert not Path(execution.checkpoint_root).is_relative_to(artifact_root)
        assert not Path(execution.scratch_root).is_relative_to(artifact_root)
    first_plan = read_synthetic_infill_plan(plan_set.shards[0].plan_ref.path)
    first_inventory = read_reconstruction_source_inventory(
        first_plan.artifact_graph["source_inventory"].path
    )
    assert plan_set.resource_summary["source_partition_count"] == 3
    assert plan_set.resource_summary["source_event_count"] == (
        first_inventory.total_row_count
    )
    assert plan_set.resource_summary["source_size_bytes"] == (
        first_inventory.total_size_bytes
    )
    assert preflight.executable
    assert preflight.verified_shard_count == 4
    assert preflight.resource_summary == plan_set.resource_summary
    create_request_calls: list[Path] = []
    ordinary_create_request = client.create_request

    def observed_create_request(
        plan_path: str | Path,
        **request_kwargs: Any,
    ) -> ReconstructionExecutionRequestV1:
        create_request_calls.append(Path(plan_path))
        return ordinary_create_request(plan_path, **request_kwargs)

    monkeypatch.setattr(client, "create_request", observed_create_request)
    plan_set_requests = client.create_plan_set_requests(
        ref.path,
        information_mode=spec.information_mode,
        acknowledge_scientific_nonclaim=True,
    )
    assert len(create_request_calls) == len(plan_set.shards)
    assert len(plan_set_requests) == len(plan_set.shards)
    assert tuple(item.plan_id for item in plan_set_requests) == tuple(
        item.plan_id for item in plan_set.shards
    )
    assert all(item.allow_refusals for item in plan_set_requests)
    assert isinstance(support_map, ReconstructionPlanSupportMapV1)
    assert support_map.plan_set_id == plan_set.plan_set_id
    assert support_map.status == "ready"
    assert support_map.executable_window_count == 31
    assert support_map.refused_window_count == 0
    assert support_map.windows[0].start_ns == plan_set.requested_start_ns
    assert support_map.windows[-1].end_ns == plan_set.requested_end_ns
    assert all(item.status == "executable" for item in support_map.windows)
    assert read_reconstruction_plan_support_map(support_ref.path) == support_map

    monkeypatch.setattr(
        reconstruction_module,
        "MAX_MONOLITHIC_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS",
        1,
    )
    index_ref = client.construct_plan_support_map(
        ref.path,
        output_directory=source_root / "support-map-index",
    )
    support_index = read_reconstruction_plan_support_map_index(index_ref.path)
    indexed_maps = tuple(iter_reconstruction_plan_support_maps(support_index))

    assert isinstance(support_index, ReconstructionPlanSupportMapIndexV2)
    assert index_ref.kind == "reconstruction_plan_support_map_index_v2"
    assert support_index.plan_set_id == plan_set.plan_set_id
    assert len(support_index.shard_refs) == len(plan_set.shards) == 4
    assert sum(len(item.windows) for item in indexed_maps) == 31
    assert indexed_maps[0].requested_start_ns == plan_set.requested_start_ns
    assert indexed_maps[-1].requested_end_ns == plan_set.requested_end_ns
    assert all(
        left.requested_end_ns == right.requested_start_ns
        for left, right in pairwise(indexed_maps)
    )
    inspection = client.inspect_plan_support_map(
        index_ref.path,
        start_ns=plan_set.requested_start_ns,
        end_ns=indexed_maps[0].requested_end_ns,
        limit=3,
    )
    assert inspection["support_artifact_kind"] == (
        "reconstruction_plan_support_map_index_v2"
    )
    assert inspection["selected_window_count"] == len(indexed_maps[0].windows)
    assert inspection["returned_window_count"] == 3
    assert inspection["truncated"]
    campaign_request = client.create_plan_set_execution_request(
        ref.path,
        index_ref.path,
        information_mode=spec.information_mode,
        acknowledge_scientific_nonclaim=True,
    )
    request_set_ref = write_reconstruction_plan_set_execution_request(
        campaign_request, source_root / "campaign-request"
    )
    restored_request = read_reconstruction_plan_set_execution_request(
        request_set_ref.path
    )

    def fake_submit(
        request: ReconstructionExecutionRequestV1,
        *,
        wait: bool,
        execution_attempt_id: str,
    ) -> ReconstructionOperationReceiptV1:
        assert not wait
        return ReconstructionOperationReceiptV1(
            operation="submit",
            request=request,
            status="submitted",
            execution_attempt_id=execution_attempt_id,
        )

    monkeypatch.setattr(client, "submit", fake_submit)
    receipt_index_ref = client.run_plan_set_execution_request(
        restored_request,
        output_directory=source_root / "campaign-submit",
        execution_attempt_id="campaign-001",
    )
    receipt_index = read_reconstruction_plan_set_receipt_index(
        receipt_index_ref.path
    )

    def fake_inspect(
        receipt: ReconstructionOperationReceiptV1, *, offline: bool
    ) -> ReconstructionOperationReceiptV1:
        assert offline
        return ReconstructionOperationReceiptV1(
            operation="status",
            request=receipt.request,
            status="completed",
            execution_attempt_id=receipt.execution_attempt_id,
        )

    monkeypatch.setattr(client, "inspect", fake_inspect)
    completed_ref = client.operate_plan_set_receipt_index(
        receipt_index_ref.path,
        operation="status",
        output_directory=source_root / "campaign-status",
        offline=True,
    )
    completed_index = read_reconstruction_plan_set_receipt_index(
        completed_ref.path
    )

    assert isinstance(campaign_request, ReconstructionPlanSetExecutionRequestV1)
    assert restored_request == campaign_request
    assert len(campaign_request.requests) == len(plan_set.shards)
    assert isinstance(receipt_index, ReconstructionPlanSetReceiptIndexV1)
    assert receipt_index.status == "submitted"
    assert receipt_index.status_counts == {"submitted": len(plan_set.shards)}
    assert completed_index.status == "completed"
    assert completed_index.status_counts == {"completed": len(plan_set.shards)}
    assert tuple(
        item.metadata["request_id"] for item in completed_index.receipt_refs
    ) == tuple(item.request_id for item in campaign_request.requests)
    assert ReconstructionPlanSetV1.from_dict(plan_set.to_dict()) == plan_set

    legacy_payload = plan_set.to_dict()
    legacy_source = legacy_payload["source_spec"]
    assert isinstance(legacy_source, dict)
    for field_name in (
        "dataset_catalog_path",
        "dataset_reference",
        "evidence_policy",
        "cross_series_constraint_policy",
    ):
        legacy_source.pop(field_name)
    legacy_payload.pop("plan_set_id")
    legacy_id = (
        "reconstruction-plan-set:sha256:"
        + hashlib.sha256(
            canonical_contract_json(legacy_payload).encode("utf-8")
        ).hexdigest()
    )
    legacy_payload["plan_set_id"] = legacy_id
    legacy_path = source_root / "legacy-plan-set.json"
    legacy_path.write_text(
        canonical_contract_json(legacy_payload) + "\n",
        encoding="utf-8",
    )

    legacy = read_reconstruction_plan_set(legacy_path)
    assert legacy.plan_set_id == legacy_id
    assert legacy.to_dict() == legacy_payload
    assert ReconstructionPlanSetV1.from_dict(legacy.to_dict()) == legacy

    legacy_payload["status"] = "ready_with_refusals"
    legacy_path.write_text(
        canonical_contract_json(legacy_payload) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        ReconstructionPlanError,
        match="status differs|identity differs",
    ):
        read_reconstruction_plan_set(legacy_path)

    Path(plan_set.shards[0].plan_ref.path).write_bytes(
        Path(plan_set.shards[0].plan_ref.path).read_bytes() + b"\n"
    )
    with pytest.raises(ReconstructionPlanError, match="shard artifact differs"):
        client.preflight_plan_set(ref.path)


def test_legacy_all_member_task_layout_requires_complete_v1_rectangle() -> None:
    """Pre-portfolio v1 tasks remain readable only as an exact member grid."""
    member_ids = tuple(f"member-{index}" for index in range(4))
    counts = {member_id: 59 for member_id in member_ids}
    kwargs = {
        "task_counts_by_member": counts,
        "executable_window_count": 59,
        "retained_member_count": 2,
        "ensemble_member_count": 4,
        "run_ensemble_member_ids": member_ids,
        "artifact_names": ("configuration", "execution_manifest"),
    }

    assert plan_module._legacy_all_member_task_layout_is_valid(**kwargs)
    assert not plan_module._legacy_all_member_task_layout_is_valid(
        **{**kwargs, "task_counts_by_member": {**counts, "member-0": 58}}
    )
    assert not plan_module._legacy_all_member_task_layout_is_valid(
        **{
            **kwargs,
            "artifact_names": (
                "configuration",
                "execution_manifest",
                "proposal_engine_registry",
            ),
        }
    )


def test_histdata_source_quote_projection_is_explicit_and_fail_closed() -> None:
    policy = "rowwise-min-bid-max-ask-preserve-raw-v1"

    assert handlers_module._canonical_histdata_source_quote(
        1.0004, 1.0, quote_order_projection_policy=policy
    ) == (1.0, 1.0004, True)
    assert handlers_module._canonical_histdata_source_quote(
        1.0, 1.0004, quote_order_projection_policy=policy
    ) == (1.0, 1.0004, False)
    with pytest.raises(ValueError, match="lacks an explicit"):
        handlers_module._canonical_histdata_source_quote(
            1.0004, 1.0, quote_order_projection_policy=None
        )


def test_public_plan_set_preserves_a_contiguous_refusal_only_shard(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full-range planning accounts for unsupported spans without fake work."""
    source_root, kwargs = planned_environment
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=("context unsupported",)),
    )
    client = ReconstructionClient()
    ref = client.construct_plan_set(
        replace(
            _public_spec(source_root, kwargs),
            window_size_ns=24 * 60 * 60 * 1_000_000_000,
        ),
        periods_per_shard=1,
    )

    plan_set = read_reconstruction_plan_set(ref.path)
    preflight = client.preflight_plan_set(ref.path)

    assert plan_set.status == "ready_with_refusals"
    assert plan_set.executable
    assert len(plan_set.shards) == 1
    assert plan_set.shards[0].preflight_status == "ready_with_refusals"
    assert plan_set.resource_summary["executable_window_count"] == 0
    assert plan_set.resource_summary["refused_window_count"] == 31
    assert plan_set.resource_summary["estimated_output_bytes"] == 0
    assert preflight.executable
    assert preflight.status == "ready_with_refusals"
    assert preflight.refusal_count == 31

    support_ref = client.construct_plan_support_map(
        ref.path, output_directory=source_root / "support-map"
    )
    support_map = read_reconstruction_plan_support_map(support_ref.path)
    assert support_map.status == "ready_with_refusals"
    assert support_map.executable_window_count == 0
    assert support_map.refused_window_count == 31
    assert all(item.refusal_code for item in support_map.windows)
    assert all(
        item.refusal_reason == "context unsupported"
        for item in support_map.windows
    )


def test_independent_support_replay_reconstructs_raw_source_decisions(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The final verifier derives support from Arrow rows, not planner helpers."""
    source_root, kwargs = planned_environment
    resolved = plan_module._resolve_plan_inputs()
    monkeypatch.setattr(
        support_module,
        "read_active_time_feed_epoch_definition",
        lambda *_: resolved.feed_epoch_definition,
    )
    monkeypatch.setattr(
        support_module,
        "read_observation_operator_artifact",
        lambda *_: resolved.observation_operator,
    )
    monkeypatch.setattr(
        support_module,
        "read_market_context_corpus",
        lambda *_: resolved.market_context,
    )
    monkeypatch.setattr(
        support_module,
        "read_cftc_positioning_corpus",
        lambda *_: resolved.cftc_positioning,
    )
    monkeypatch.setattr(
        support_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=()),
    )
    monkeypatch.setattr(
        support_module,
        "query_cftc_positioning_corpus",
        _ready_cftc_query,
    )
    monkeypatch.setattr(
        support_module,
        "_context_labels",
        lambda *_, **__: ("fx-open", "market_context:none:test"),
    )
    client = ReconstructionClient()
    plan_set_ref = client.construct_plan_set(
        _public_spec(source_root, kwargs), periods_per_shard=1
    )
    support_ref = client.construct_plan_support_map(
        plan_set_ref.path, output_directory=source_root / "independent-support"
    )
    plan_set = read_reconstruction_plan_set(plan_set_ref.path)
    support_maps = (
        (read_reconstruction_plan_support_map(support_ref.path),)
        if support_ref.kind == "reconstruction_plan_support_map_v1"
        else tuple(
            iter_reconstruction_plan_support_maps(
                read_reconstruction_plan_support_map_index(support_ref.path)
            )
        )
    )
    first_shard = plan_set.shards[0]
    plan = read_synthetic_infill_plan(first_shard.plan_ref.path)
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    claims = tuple(
        window
        for support_map in support_maps
        for window in support_map.windows
        if window.shard_id == first_shard.shard_id
    )
    claimed_map_id = next(
        support_map.support_map_id
        for support_map in support_maps
        if any(
            window.shard_id == first_shard.shard_id
            for window in support_map.windows
        )
    )
    candidate = SimpleNamespace(
        candidate_id="reconstruction-release-candidate:sha256:" + "a" * 64,
        source_partition_hashes={
            f"{item.symbol}:{item.period}": item.artifact.sha256
            for item in inventory.partitions
        },
    )

    verified = support_module._verify_plan_shard(
        plan_set=plan_set,
        plan_shard=first_shard,
        plan=plan,
        claimed_windows=claims,
        claimed_support_map_id=claimed_map_id,
        candidate=candidate,
    )

    assert verified.plan_set_id == plan_set.plan_set_id
    assert verified.census.window_count == len(claims)
    assert verified.census.terminal_counts == {"executable": len(claims)}
    assert verified.census.valid_common_data_implementation_refusal_count == 0
    assert sum(
        item.in_requested_domain_row_count
        for item in verified.partition_replays
    ) == sum(sum(item.core_event_counts.values()) for item in verified.windows)
    assert all(item.core_row_identity_digest for item in verified.windows)
    assert all(item.alignment_source_event_digest for item in verified.windows)

    shifted = (
        replace(
            claims[0],
            start_ns=claims[0].start_ns + 1,
            support_id="",
        ),
        *claims[1:],
    )
    with pytest.raises(
        support_module.FinalSupportVerificationError,
        match="bounds differ|not contiguous",
    ):
        support_module._verify_plan_shard(
            plan_set=plan_set,
            plan_shard=first_shard,
            plan=plan,
            claimed_windows=shifted,
            claimed_support_map_id=claimed_map_id,
            candidate=candidate,
        )

    alignment_shifted = (
        replace(
            claims[0],
            recommended_cross_series_event_time_ns=(
                claims[0].recommended_cross_series_event_time_ns + 1
            ),
            support_id="",
        ),
        *claims[1:],
    )
    with pytest.raises(
        support_module.FinalSupportVerificationError,
        match="recommended_cross_series_event_time_ns",
    ):
        support_module._verify_plan_shard(
            plan_set=plan_set,
            plan_shard=first_shard,
            plan=plan,
            claimed_windows=alignment_shifted,
            claimed_support_map_id=claimed_map_id,
            candidate=candidate,
        )

    resource = dict(claims[0].resource_estimate or {})
    resource["candidate_event_count"] = (
        int(resource["candidate_event_count"]) + 1
    )
    cardinality_shifted = (
        replace(claims[0], resource_estimate=resource, support_id=""),
        *claims[1:],
    )
    with pytest.raises(
        support_module.FinalSupportVerificationError,
        match="resource_estimate",
    ):
        support_module._verify_plan_shard(
            plan_set=plan_set,
            plan_shard=first_shard,
            plan=plan,
            claimed_windows=cardinality_shifted,
            claimed_support_map_id=claimed_map_id,
            candidate=candidate,
        )

    bad_candidate = SimpleNamespace(
        candidate_id=candidate.candidate_id,
        source_partition_hashes={
            **candidate.source_partition_hashes,
            next(iter(candidate.source_partition_hashes)): "0" * 64,
        },
    )
    with pytest.raises(
        support_module.FinalSupportVerificationError,
        match="source hash differs",
    ):
        support_module._verify_plan_shard(
            plan_set=plan_set,
            plan_shard=first_shard,
            plan=plan,
            claimed_windows=claims,
            claimed_support_map_id=claimed_map_id,
            candidate=bad_candidate,
        )


def test_public_plan_spec_threads_bounded_window_size_into_resources(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    """Operators can split large monthly inputs before resource preflight."""
    source_root, kwargs = planned_environment
    window_size_ns = 6 * 60 * 60 * 1_000_000_000
    spec = replace(
        _public_spec(source_root, kwargs),
        window_size_ns=window_size_ns,
    )

    restored_spec = ReconstructionPlanSpecV1.from_dict(spec.to_dict())
    plan_ref = ReconstructionClient().construct_plan(restored_spec)
    plan = read_synthetic_infill_plan(plan_ref.path)
    configuration = plan_module.read_reconstruction_plan_configuration(
        plan.artifact_graph["configuration"].path
    )

    assert restored_spec.window_size_ns == window_size_ns
    assert configuration.window_size_ns == window_size_ns
    estimates = tuple(
        task.resource_estimate
        for request in plan.workflow_requests
        for task in request.tasks
    )
    nonempty = tuple(
        estimate for estimate in estimates if estimate.input_event_count
    )
    assert nonempty
    assert all(estimate.estimated_batch_count >= 3 for estimate in nonempty)
    assert all(
        estimate.estimated_memory_bytes >= 512 * 1024 * 1024
        for estimate in estimates
    )


def test_public_request_requires_ack_and_exact_information_mode(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    """The operator cannot omit the nonclaim or relabel plan information."""
    source_root, kwargs = planned_environment
    client = ReconstructionClient()
    plan_ref = client.construct_plan(_public_spec(source_root, kwargs))

    with pytest.raises(ReconstructionRefusedError, match="acknowledgement"):
        client.create_request(
            plan_ref.path,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            acknowledge_scientific_nonclaim=False,
        )

    mismatched = ReconstructionExecutionRequestV1(
        plan_path=plan_ref.path,
        plan_id=read_synthetic_infill_plan(plan_ref.path).plan_id,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        scientific_nonclaim_acknowledged=True,
    )
    with pytest.raises(ReconstructionRefusedError, match="information mode"):
        client.preflight(mismatched)


def test_typed_public_facade_accepts_a_point_in_time_ex_ante_plan(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ex-ante request is executable when every fitted artifact predates it."""
    source_root, kwargs = planned_environment
    resolved = plan_module._resolve_plan_inputs()
    definition = SimpleNamespace(
        **{
            **vars(resolved.feed_epoch_definition),
            "coverage_end_utc_ms": 1_514_764_800_000,
        }
    )
    profile = ModernReferenceMotifProfileV1(
        split_periods={
            "train": ("201501",),
            "calibration": ("201601",),
            "validation": ("201701",),
            "final_holdout": ("201801",),
        }
    )
    point_in_time = replace(
        resolved,
        feed_epoch_definition=definition,
        motif_profile=profile,
    )
    monkeypatch.setattr(
        plan_module, "_resolve_plan_inputs", lambda **_: point_in_time
    )
    client = ReconstructionClient()
    plan_ref = client.construct_plan(
        replace(
            _public_spec(source_root, kwargs),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
        )
    )
    request = client.create_request(
        plan_ref.path,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        acknowledge_scientific_nonclaim=True,
    )

    preflight = client.preflight(request)

    assert preflight.executable
    assert preflight.dry_run["information_mode"] == "ex_ante_simulation"


def test_public_spec_rejects_m1_partial_triangle_and_broker_only(
    planned_environment: tuple[Path, dict[str, Any]],
) -> None:
    """Unsupported formats and delivery requests fail before plan building."""
    source_root, kwargs = planned_environment
    spec = _public_spec(source_root, kwargs)

    with pytest.raises(ReconstructionUnsupportedError, match="timeframe"):
        replace(spec, timeframe="M1")
    with pytest.raises(ReconstructionUnsupportedError, match="symbols"):
        replace(spec, symbols=("eurusd", "gbpusd"))
    with pytest.raises(
        ReconstructionUnsupportedError, match="broker_delivery_artifact"
    ):
        replace(
            spec,
            delivery_mode=ReconstructionDeliveryMode.BROKER_CONDITIONED,
        )


def test_public_submit_status_resume_and_receipt_round_trip(
    planned_environment: tuple[Path, dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public controls retain exact request stores and fresh resume attempts."""
    source_root, kwargs = planned_environment
    plan_ref = ReconstructionClient().construct_plan(
        _public_spec(source_root, kwargs)
    )
    request = ReconstructionClient().create_request(
        plan_ref.path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        acknowledge_scientific_nonclaim=True,
    )
    config = build_orchestration_worker_config(
        workspace=tmp_path,
        runtime_home=tmp_path / "runtime",
    )

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, dict[str, Any], dict[str, Any]]] = []

        async def start_workflow(
            self, workflow: Any, payload: Any, **options: Any
        ) -> Any:
            self.calls.append((workflow, payload, options))
            return SimpleNamespace(id=options["id"], run_id="fake-run")

    temporal = FakeClient()
    client = ReconstructionClient(config=config, temporal_client=temporal)
    submitted = client.submit(request)
    receipt_path = write_operation_receipt(
        submitted, tmp_path / "submission.json"
    )
    restored = read_operation_receipt(receipt_path)
    status = client.inspect(restored, offline=True)

    async def fake_cancel_job(workflow_id: str, **options: Any) -> Any:
        assert workflow_id
        assert options["reason"] == "operator request"
        assert isinstance(options["status_store"], ManifestStatusStore)
        return SimpleNamespace(
            to_dict=lambda: {
                "workflow_id": workflow_id,
                "status": "CANCELLED",
                "lifecycle": "cancel_requested",
            }
        )

    monkeypatch.setattr(
        "histdatacom.reconstruction.cancel_job", fake_cancel_job
    )
    cancelled = client.cancel(restored, reason="operator request")
    resumed = client.resume(restored, wait=False)

    assert restored == submitted
    assert len(submitted.handles) == 2
    assert status.status == "running"
    assert cancelled.status == "cancellation_requested"
    assert len(cancelled.job_snapshots) == 2
    assert resumed.operation == "resume"
    assert resumed.execution_attempt_id == "resume-001"
    resume_calls = [
        payload
        for _, payload, _ in temporal.calls
        if payload.get("execution_attempt_id") == "resume-001"
    ]
    assert len(resume_calls) == 2
    assert all(
        call["request"]["request_id"] == workflow_request.request_id
        for call, workflow_request in zip(
            resume_calls,
            read_synthetic_infill_plan(plan_ref.path).workflow_requests,
            strict=True,
        )
    )


def _public_spec(
    source_root: Path, kwargs: dict[str, Any]
) -> ReconstructionPlanSpecV1:
    return ReconstructionPlanSpecV1(
        source_root=str(source_root),
        feed_epoch_definition_path=str(kwargs["feed_epoch_definition_path"]),
        observation_operator_path=str(kwargs["observation_operator_path"]),
        market_context_corpus_path=str(kwargs["market_context_corpus_path"]),
        cftc_positioning_corpus_path=str(
            kwargs["cftc_positioning_corpus_path"]
        ),
        benchmark_manifest_path=str(kwargs["benchmark_manifest_path"]),
        motif_manifest_path=str(kwargs["motif_manifest_path"]),
        motif_index_path=str(kwargs["motif_index_path"]),
        motif_qualification_path=str(kwargs["motif_qualification_path"]),
        motif_leakage_audit_path=str(kwargs["motif_leakage_audit_path"]),
        artifact_root=str(kwargs["artifact_root"]),
        output_root=str(kwargs["output_root"]),
        checkpoint_root=str(kwargs["checkpoint_root"]),
        scratch_root=str(kwargs["scratch_root"]),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        start_period=kwargs["start_period"],
        end_period=kwargs["end_period"],
    )


def test_public_plan_spec_supports_exact_paired_window_bounds(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root, kwargs = planned_environment
    month_start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    start_ns = int(month_start.timestamp() * 1_000_000_000) + 60_000_000_000
    end_ns = start_ns + 600_000_000_000
    spec = replace(
        _public_spec(source_root, kwargs),
        requested_start_ns=start_ns,
        requested_end_ns=end_ns,
        window_size_ns=600_000_000_000,
    )

    restored = ReconstructionPlanSpecV1.from_dict(spec.to_dict())
    plan_ref = ReconstructionClient().construct_plan(restored)
    plan = read_synthetic_infill_plan(plan_ref.path)

    assert plan.requested_start_ns == start_ns
    assert plan.requested_end_ns == end_ns
    assert plan.resources.planned_window_count == 1
    assert (
        plan.resources.candidate_amplification
        <= plan.run.storage_policy.max_candidate_amplification
    )
    assert {
        task.window.core_start_ns
        for request in plan.workflow_requests
        for task in request.tasks
    } == {start_ns}

    plan_set_ref = ReconstructionClient().construct_plan_set(
        restored, periods_per_shard=1
    )
    plan_set = read_reconstruction_plan_set(plan_set_ref.path)
    support_ref = ReconstructionClient().construct_plan_support_map(
        plan_set_ref.path,
        output_directory=source_root / "exact-support-map",
    )
    support_map = read_reconstruction_plan_support_map(support_ref.path)

    assert plan_set.requested_start_ns == start_ns
    assert plan_set.requested_end_ns == end_ns
    assert len(plan_set.shards) == 1
    assert len(support_map.windows) == 1
    assert support_map.windows[0].status == "executable"
    assert support_map.windows[0].common_exact_core_timestamp_count == 2

    selected_plan = read_synthetic_infill_plan(plan_set.shards[0].plan_ref.path)
    task_windows = tuple(
        task.window
        for request in selected_plan.workflow_requests
        for task in request.tasks
    )

    class FakeProduct:
        pass

    fake_products: dict[Path, Any] = {}
    scientific_ledger_ref = selected_plan.artifact_graph["scientific_ledger"]
    scientific_ledger = read_reconstruction_scientific_ledger(
        scientific_ledger_ref.path
    )
    experiment = read_reconstruction_experiment(
        selected_plan.artifact_graph["experiment_manifest"].path
    )
    for ordinal, window in enumerate(task_windows, start=1):
        path = source_root / f"fake-product-{ordinal}.json"
        path.write_text("{}", encoding="utf-8")
        product = FakeProduct()
        product.run_id = selected_plan.run.run_id
        product.window_id = window.window_id
        product.ensemble_member_id = window.ensemble_member_id
        product.delivery_profile_id = (
            "modern-reference:" + selected_plan.configuration_id
        )
        product.source = SimpleNamespace(
            source_version_ids=selected_plan.run.source_version_ids,
            experiment_id=experiment.experiment_id,
        )
        product.symbols = _SYMBOLS
        product.quality = SimpleNamespace(
            final_validation_status="passed",
            cross_instrument_quality_status="passed",
            benchmark_evidence={
                "scientific_ledger_id": scientific_ledger.ledger_id,
                "runtime_proposal_evidence": {
                    "observation_uncertainty_ensemble_id": (
                        "observation-uncertainty-ensemble:test"
                    ),
                    "observation_scenario_id": f"observation-scenario:{ordinal}",
                    "observation_scenario_kind": "central_fitted_retention",
                    "observation_path_seed": ordinal,
                },
            },
            benchmark_artifact_ids=(scientific_ledger_ref.sha256,),
        )
        product.manifest_id = f"reconstruction-manifest-v3:fake-{ordinal}"
        product.publication_id = f"reconstruction-publication-v3:fake-{ordinal}"
        product.observed_event_count = 6
        product.synthetic_event_count = 7
        product.replay = SimpleNamespace(
            logical_content_sha256=str(ordinal) * 64
        )
        fake_products[path.resolve()] = product

    monkeypatch.setattr(
        reconstruction_module, "ReconstructionProductManifestV3", FakeProduct
    )
    monkeypatch.setattr(
        reconstruction_module,
        "discover_reconstruction_manifests",
        lambda _: tuple(fake_products),
    )
    monkeypatch.setattr(
        reconstruction_module,
        "verify_reconstruction_publication",
        lambda path: fake_products[Path(path).resolve()],
    )
    product_index_ref = ReconstructionClient().construct_campaign_product_index(
        plan_set_ref.path,
        support_ref.path,
        output_directory=source_root / "product-index",
    )
    product_index = read_reconstruction_campaign_product_index(
        product_index_ref.path
    )
    product_shard = read_reconstruction_campaign_product_shard(
        product_index.shard_refs[0].path
    )

    assert isinstance(product_index, ReconstructionCampaignProductIndexV1)
    assert isinstance(product_shard, ReconstructionCampaignProductShardV1)
    assert product_index.status == "complete"
    assert product_index.support_window_count == 1
    assert product_index.verified_product_count == len(task_windows) == 2
    assert product_index.missing_product_count == 0
    assert product_index.observed_dataset_version_id == (
        selected_plan.run.source_version_ids[0]
    )
    assert product_index.synthetic_event_count == 14
    assert {
        (
            entry.observation_uncertainty_ensemble_id,
            entry.observation_scenario_id,
            entry.observation_scenario_kind,
            entry.observation_path_seed,
        )
        for entry in product_shard.entries
    } == {
        (
            "observation-uncertainty-ensemble:test",
            "observation-scenario:1",
            "central_fitted_retention",
            1,
        ),
        (
            "observation-uncertainty-ensemble:test",
            "observation-scenario:2",
            "central_fitted_retention",
            2,
        ),
    }
    product_inspection = ReconstructionClient().inspect_campaign_products(
        product_index_ref.path,
        start_ns=start_ns,
        end_ns=end_ns,
        limit=1,
    )
    assert product_inspection["selected_entry_count"] == 2
    assert product_inspection["returned_entry_count"] == 1
    assert product_inspection["truncated"]
    publication_ref = ReconstructionClient().publish_campaign_dataset(
        product_index_ref.path,
        output_directory=source_root / "dataset-publication",
    )
    publication = read_reconstruction_campaign_dataset_publication(
        publication_ref.path
    )
    assert isinstance(publication, ReconstructionCampaignDatasetPublicationV1)
    assert publication.observed_parent_dataset_version_id == (
        product_index.observed_dataset_version_id
    )
    assert publication.synthetic_dataset_version_id.startswith(
        "dataset-version:sha256:"
    )
    dataset_version = DatasetVersionManifestV1.from_dict(
        json.loads(
            Path(publication.dataset_version_ref.path).read_text("utf-8")
        )
    )
    assert scientific_ledger_ref in dataset_version.qualification_evidence
    assert publication.dataset_version_ref.metadata["scientific_ledger_id"] == (
        scientific_ledger.ledger_id
    )

    missing_path = next(iter(fake_products))
    monkeypatch.setattr(
        reconstruction_module,
        "discover_reconstruction_manifests",
        lambda _: (missing_path,),
    )
    incomplete_ref = ReconstructionClient().construct_campaign_product_index(
        plan_set_ref.path,
        support_ref.path,
        output_directory=source_root / "product-index-incomplete",
    )
    incomplete = read_reconstruction_campaign_product_index(incomplete_ref.path)
    assert incomplete.status == "incomplete"
    assert incomplete.verified_product_count == 1
    assert incomplete.missing_product_count == 1
    with pytest.raises(
        ReconstructionRefusedError, match="every retained product"
    ):
        ReconstructionClient().publish_campaign_dataset(
            incomplete_ref.path,
            output_directory=source_root / "incomplete-dataset-publication",
        )

    payload = spec.to_dict()
    payload["requested_end_ns"] = None
    with pytest.raises(
        ReconstructionUnsupportedError,
        match="must be supplied together",
    ):
        ReconstructionPlanSpecV1.from_dict(payload)


def test_exact_period_resolution_does_not_round_month_boundary_nanoseconds() -> (
    None
):
    """The last nanosecond in a month stays in that source partition."""
    february_start_ns = int(
        datetime(2020, 2, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )

    assert plan_module._period_for_ns(february_start_ns - 1) == "202001"
    assert plan_module._period_for_ns(february_start_ns) == "202002"


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


def test_builder_records_a_fully_refused_interval_without_executable_work(
    planned_environment: tuple[Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported spans remain contiguous, explicit, and safe to skip."""
    source_root, kwargs = planned_environment
    monkeypatch.setattr(
        plan_module,
        "preflight_market_context_corpus",
        lambda *_, **__: SimpleNamespace(reasons=("context unsupported",)),
    )

    plan = build_synthetic_infill_plan(source_root, **kwargs)
    plan_ref = write_synthetic_infill_plan(plan, kwargs["artifact_root"])
    restored = read_synthetic_infill_plan(plan_ref.path)
    execution = read_reconstruction_plan_execution_manifest(
        restored.artifact_graph["execution_manifest"].path
    )

    assert restored.status == "ready_with_refusals"
    assert restored.workflow_requests == ()
    assert restored.resources.executable_window_count == 0
    assert restored.resources.refused_window_count == 2
    assert restored.resources.workflow_request_count == 0
    assert restored.resources.estimated_output_bytes == 0
    assert execution.executable_window_count == 0
    assert len(execution.refusal_ids) == 2
    validate_synthetic_infill_plan_for_execution(restored)
    request = ReconstructionClient().create_request(
        plan_ref.path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        acknowledge_scientific_nonclaim=True,
    )
    preflight = ReconstructionClient().preflight(request)
    assert preflight.status == "refused"
    assert not preflight.executable


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
