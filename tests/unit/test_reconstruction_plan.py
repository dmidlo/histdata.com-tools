"""Tests for the first-party reconstruction plan and artifact graph."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from pyarrow import ipc

from histdatacom.cross_series_constraints import (
    CrossSeriesConstraintPolicyV1,
    read_cross_series_constraint_policy,
)
from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.orchestration.queues import build_orchestration_worker_config
from histdatacom.orchestration.reconstruction import (
    RECONSTRUCTION_STAGE_ORDER,
    ReconstructionStage,
    ReconstructionStageInvocationV1,
    artifact_ref_for_file,
)
from histdatacom.reconstruction import (
    ReconstructionClient,
    ReconstructionExecutionRequestV1,
    ReconstructionPlanError,
    ReconstructionPlanSetV1,
    ReconstructionPlanSpecV1,
    ReconstructionPlanSpecV2,
    ReconstructionRefusedError,
    ReconstructionUnsupportedError,
    read_execution_request,
    read_operation_receipt,
    read_plan_spec,
    read_reconstruction_plan_set,
    write_execution_request,
    write_operation_receipt,
)
from histdatacom.reconstruction_evidence import (
    ReconstructionEvidencePolicyV1,
    read_reconstruction_evidence_policy,
)
from histdatacom.reconstruction_experiment import read_reconstruction_experiment
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
from histdatacom.synthetic import reconstruction_handlers as handlers_module
from histdatacom.synthetic import reconstruction_plan as plan_module
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.generation import EMPIRICAL_MOTIF_GENERATOR_ID
from histdatacom.synthetic.proposal_engines import (
    ProposalEngineEvidenceV1,
    proposal_engine_default_configs,
)

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
_PERIOD = "202001"
_START_MS = 1_578_268_800_000


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_tick_partition(
    path: Path, offset: int, *, enriched: bool = False
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "datetime": [_START_MS + offset, _START_MS + 60_000 + offset],
            "bid": [1.0 + offset / 1_000_000, 1.0001 + offset / 1_000_000],
            "ask": [1.0002 + offset / 1_000_000, 1.0003 + offset / 1_000_000],
            "vol": [0, 0],
        }
    )
    if enriched:
        frame = enrich_tick_cache_with_training_features(
            pl.from_arrow(table),
            symbol=path.parents[2].name.upper(),
            data_format="ascii",
            timeframe="T",
            period=_PERIOD,
        ).with_columns(pl.Series("dq_issue_precision_warning", [False, True]))
        table = frame.to_arrow()
    with (
        pa.OSFile(str(path), "wb") as sink,
        ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table)


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
    tmp_path: Path, source_root: Path, *, enriched: bool = False
) -> Any:
    lineage: list[dict[str, str]] = []
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = source_root / symbol / "2020" / "1" / ".data"
        _write_tick_partition(path, ordinal, enriched=enriched)
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
    broker_command = next(
        command
        for command in task.commands
        if command.stage is ReconstructionStage.BROKER_TRANSFER
    )
    assert broker_command.input_manifest_refs == ()


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
        ValueError,
        match="selected proposal engines are not reconstruction eligible",
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
    plan_ref = ReconstructionClient().construct_plan(restored)
    portfolio = ReconstructionClient().proposal_portfolio(plan_ref.path)

    assert isinstance(restored, ReconstructionPlanSpecV2)
    assert restored == spec
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
        "preflight_cftc_positioning_corpus",
        lambda *_, **__: SimpleNamespace(ready=True, reasons=()),
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

    assert all(len(events) == 2 for events in source_events.values())
    assert len(cached) == 3
    for schema_version, rows, complete in cached.values():
        assert schema_version == "histdatacom.ascii-tick-training-features.v1"
        assert complete
        assert rows[2]["precision_warning"] is True


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

    assert isinstance(plan_set, ReconstructionPlanSetV1)
    assert plan_set.status == "ready"
    assert plan_set.executable
    assert len(plan_set.shards) == 4
    assert all(item.start_period == _PERIOD for item in plan_set.shards)
    assert all(item.end_period == _PERIOD for item in plan_set.shards)
    assert plan_set.resource_summary["plan_shard_count"] == 4
    assert plan_set.resource_summary["planned_window_count"] == 31
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
