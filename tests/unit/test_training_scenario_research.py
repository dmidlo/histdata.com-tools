"""Whole-payload commitments and real research replay, not promotion proof."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from histdatacom.data_quality import training_scenario_research as research
from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_scenario_contracts import (
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioResearchBindingV1,
)

pytest_plugins = ("tests.fixtures.training_scenario_v1",)

_SCIENTIFIC_FIELDS = (
    "conditioning",
    "observation",
    "assignment",
    "fit",
    "generation",
    "generation_scenario",
    "calendar",
    "constraints",
    "cross_currency_config",
    "final_validation",
    "cross_reconciliation",
)


def _wire(payload):
    # Independent JSON framing and hashlib, not a production digest helper.
    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    )


@pytest.mark.parametrize("field", _SCIENTIFIC_FIELDS)
def test_compact_commitment_covers_scientific_fields_not_only_ids(field):
    # Metadata-only vectors: constructing this private invocation-local record
    # is NOT native scientific execution or a qualified retained product.
    payload = {
        "recipe_id": "unchanged-recipe-id",
        "scope": research.RESEARCH_SCOPE,
        "evidence_ids": ["unchanged-evidence-id"],
        **{
            name: {
                "native_id": f"unchanged-{name}-id",
                "law": {"values": [1, 2, 3]},
            }
            for name in _SCIENTIFIC_FIELDS
        },
    }
    original = research._ResearchCell(
        run=None,
        window=None,
        observation_kind="metadata-vector",
        transition_kind="metadata-vector",
        observation_id="unchanged-observation-id",
        transition_id="unchanged-transition-id",
        evidence_ids=("unchanged-evidence-id",),
        status="refused",
        reasons=("metadata_vector_not_native_evidence",),
        delivered=None,
        validation=None,
        anchors=(),
        scientific_json=_wire(payload),
    )
    changed = json.loads(original.scientific_json)
    changed[field]["law"]["values"][1] = 19
    altered = replace(original, scientific_json=_wire(changed))
    assert changed["recipe_id"] == payload["recipe_id"]
    assert changed["evidence_ids"] == payload["evidence_ids"]
    assert all(
        changed[name]["native_id"] == payload[name]["native_id"]
        for name in _SCIENTIFIC_FIELDS
    )
    before = original.benchmark_evidence["training_scenario_research"]
    after = altered.benchmark_evidence["training_scenario_research"]
    assert (
        before["scientific_payload_sha256"]
        == hashlib.sha256(original.scientific_json.encode("ascii")).hexdigest()
    )
    assert (
        after["scientific_payload_sha256"]
        == hashlib.sha256(altered.scientific_json.encode("ascii")).hexdigest()
    )
    assert (
        after["scientific_payload_sha256"]
        != before["scientific_payload_sha256"]
    )
    assert {
        key: value
        for key, value in before.items()
        if key != "scientific_payload_sha256"
    } == {
        key: value
        for key, value in after.items()
        if key != "scientific_payload_sha256"
    }
    assert (
        original.benchmark_evidence["scientific_promotion_evaluated"] is False
    )
    assert altered.benchmark_evidence["candidate_promotion_eligible"] is False


def test_all_actual_cell_commitments_include_complete_retained_payload(
    scenario_research,
):
    cells = scenario_research["cells"]
    assert len(cells) == 18
    assert {cell.status for cell in cells} == {"eligible", "refused"}
    for cell in cells:
        wire = _wire(json.loads(cell.scientific_json))
        assert wire == cell.scientific_json
        commitment = cell.benchmark_evidence["training_scenario_research"]
        assert (
            commitment["scientific_payload_sha256"]
            == hashlib.sha256(wire.encode("ascii")).hexdigest()
        )
        assert (
            commitment["recipe_id"]
            == scenario_research["binding"].expected_recipe_id
        )
        assert commitment["evidence_ids"] == list(cell.evidence_ids)
        assert commitment["scientific_payload_commitment"] == (
            "complete-canonical-native-replay-payload.v1"
        )


@pytest.mark.parametrize(
    "defect,expected_error",
    (
        ("scientific_digest", "retained scientific recipe lineage differs"),
        ("pit_projection", "product quality differs from fresh replay"),
        ("cross_series", "product quality differs from fresh replay"),
    ),
)
def test_native_valid_republication_cannot_replace_fresh_research_evidence(
    scenario_research, tmp_path, defect, expected_error
):
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV2,
        commit_delivery_reconstruction_publication,
        read_reconstruction_streams,
        stage_delivery_reconstruction_publication,
        verify_reconstruction_publication,
    )
    from histdatacom.synthetic.streaming import ReconstructionStoragePolicyV1

    cell = next(
        cell for cell in scenario_research["cells"] if cell.status == "eligible"
    )
    original = next(
        product
        for product in scenario_research["products"]
        if product.manifest.ensemble_member_id == cell.window.ensemble_member_id
        and product.manifest.window_id == cell.window.window_id
    )
    original_bytes = original.manifest_path.read_bytes()
    benchmark = json.loads(_wire(cell.benchmark_evidence))
    extra = {}
    if defect == "scientific_digest":
        claims = benchmark["training_scenario_research"]
        old_digest = claims["scientific_payload_sha256"]
        claims["scientific_payload_sha256"] = (
            "0" if old_digest[0] != "0" else "1"
        ) + old_digest[1:]
    elif defect == "pit_projection":
        extra["point_in_time_evidence_projection_ids"] = (
            "adversarial-pit-projection:not-produced-by-research",
        )
    else:
        extra["cross_series_constraint_bundle_ids"] = (
            "adversarial-cross-bundle:not-produced-by-research",
        )
    storage = ReconstructionStoragePolicyV1.from_dict(
        json.loads(scenario_research["recipe_json"])["storage_policy"]
    )
    # Real stage/commit/Parquet readback, with actual unchanged delivered rows
    # and final native validation. Only the retained scientific claim changes.
    # Native byte/row validity is not proof that that claim was ever executed.
    staged = stage_delivery_reconstruction_publication(
        tmp_path / "products",
        cell.delivered,
        final_validation=cell.validation,
        benchmark_artifact_ids=cell.evidence_ids,
        benchmark_evidence=benchmark,
        immutable_source_anchors=cell.anchors,
        symbol_group_id=cell.window.synchronization_unit_id,
        retention_plan=original.manifest.retention,
        storage_policy=storage,
        staging_root=tmp_path / "scratch",
        **extra,
    )
    published = commit_delivery_reconstruction_publication(staged)
    native = verify_reconstruction_publication(published.manifest_path)
    assert type(native) is ReconstructionProductManifestV2
    assert native.manifest_id == published.manifest.manifest_id
    assert native.quality.quality_manifest_id != (
        original.manifest.quality.quality_manifest_id
    )
    assert native.quality.cross_instrument_quality_sha256 != (
        original.manifest.quality.cross_instrument_quality_sha256
    )
    assert native.quality.final_validation_id == cell.validation.validation_id
    assert tuple(
        event.to_dict()
        for stream in read_reconstruction_streams(published.manifest_path)
        for event in stream.events
    ) == tuple(
        event.to_dict()
        for stream in cell.delivered.streams
        for event in stream.events
    )
    paths = set(scenario_research["source"].product_manifest_paths)
    paths.remove(str(original.manifest_path))
    paths.add(str(published.manifest_path))
    source = replace(
        scenario_research["source"], product_manifest_paths=tuple(sorted(paths))
    )
    plan = TrainingScenarioPlanV1(
        source,
        build_training_ownership(source),
        TrainingScenarioPolicyV1(),
        research_binding=scenario_research["binding"],
    )
    with pytest.raises(ValueError, match=expected_error):
        research.replay_training_scenario_research(plan)
    assert original.manifest_path.read_bytes() == original_bytes
    assert verify_reconstruction_publication(original.manifest_path) == (
        original.manifest
    )


def _binding(payload):
    wire = training_json(payload)
    return TrainingScenarioResearchBindingV1(
        wire, research.research_recipe_id(wire)
    )


def _unexpected_native_work(*args, **kwargs):
    pytest.fail("prospective refusal reached native scan or scientific fit")


@pytest.mark.parametrize("period", ("201599", "189912", "220101"))
def test_invalid_calendar_period_refuses_before_source_reads(
    scenario_research, monkeypatch, period
):
    data = json.loads(scenario_research["recipe_json"])
    data["calibration_periods"] = sorted((*data["calibration_periods"], period))
    monkeypatch.setattr(research, "_inputs", _unexpected_native_work)
    with pytest.raises(ValueError, match="calibration periods"):
        research.execute_training_scenario_research_recipe(
            scenario_research["observed_source"], _binding(data)
        )


def test_creation_refuses_existing_products_before_source_reads(
    scenario_research, monkeypatch
):
    monkeypatch.setattr(research, "_inputs", _unexpected_native_work)
    with pytest.raises(ValueError, match="only the observed source"):
        research.execute_training_scenario_research_recipe(
            scenario_research["source"], scenario_research["binding"]
        )


@pytest.mark.parametrize("include_observed", (False, True))
def test_complete_output_reservation_precedes_scientific_work(
    scenario_research, monkeypatch, include_observed
):
    import histdatacom.data_analytics as analytics
    from histdatacom.data_quality.training_lineage import verify_training_source
    from histdatacom.synthetic.marked_hawkes import MarkedHawkesConfigV1

    data = json.loads(scenario_research["recipe_json"])
    source = scenario_research["observed_source"]
    verified = verify_training_source(source)
    selected = tuple(
        row
        for row in verified.observed
        if data["start_ns"] <= row.event_time_ns < data["end_ns"]
    )
    assert len(selected) == 6
    roster = 3 * 3 * data["paths_per_cell"]
    maximum = research.MAX_OUTPUT_ROWS // roster + 1
    if include_observed:
        maximum -= len(selected)
    native_config = MarkedHawkesConfigV1.from_dict(data["marked_config"])
    limits = replace(
        native_config.limits,
        max_generated_events_per_window=maximum,
        limits_id="",
    )
    data["marked_config"] = replace(
        native_config, limits=limits, config_id=""
    ).to_dict()
    binding = _binding(data)
    if include_observed:
        # Generated-only budget fits. Multiplying actual observed anchors into
        # every member is independently enough to exceed the fixed ceiling.
        assert roster * maximum <= research.MAX_OUTPUT_ROWS
        assert roster * (maximum + len(selected)) > research.MAX_OUTPUT_ROWS
        admitted = research._admit(binding, source)
        assert admitted["member_count"] == roster
        error = "observed plus generated output reservation"
    else:
        assert roster * maximum > research.MAX_OUTPUT_ROWS
        monkeypatch.setattr(research, "_inputs", _unexpected_native_work)
        error = "complete crossed output reservation"
    # Negative canaries only. No successful scientific/native reader is doubled.
    monkeypatch.setattr(
        analytics, "scan_active_time_evidence", _unexpected_native_work
    )
    monkeypatch.setattr(
        analytics, "fit_active_time_feed_epochs", _unexpected_native_work
    )
    with pytest.raises(ValueError, match=error):
        research.execute_training_scenario_research_recipe(source, binding)


def test_absent_actual_calibration_period_refuses_before_scientific_work(
    scenario_research, monkeypatch
):
    import histdatacom.data_analytics as analytics

    data = json.loads(scenario_research["recipe_json"])
    data["calibration_periods"].append("201508")
    binding = _binding(data)
    research._admit(binding, scenario_research["observed_source"])
    monkeypatch.setattr(
        analytics, "scan_active_time_evidence", _unexpected_native_work
    )
    monkeypatch.setattr(
        analytics, "fit_active_time_feed_epochs", _unexpected_native_work
    )
    with pytest.raises(ValueError, match="lacks complete actual symbols"):
        research.execute_training_scenario_research_recipe(
            scenario_research["observed_source"], binding
        )
