"""Publication-safe reconstruction diagnostic contracts and rendering."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import histdatacom.synthetic.diagnostics as diagnostics_module
from histdatacom.reconstruction import ReconstructionClient
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.diagnostic_rendering import (
    _ordered_x_labels,
    render_diagnostic_bundle,
)
from histdatacom.synthetic.diagnostics import (
    REQUIRED_DIAGNOSTIC_FAMILIES,
    DiagnosticFamily,
    DiagnosticPublicationSpecV1,
    DiagnosticRendererConfigV1,
    DiagnosticRenderFormat,
    DiagnosticSourceV1,
    DiagnosticStatus,
    build_reconstruction_diagnostic_bundle,
    publish_reconstruction_diagnostics,
    read_diagnostic_publication_spec,
    verify_reconstruction_diagnostic_publication,
    write_diagnostic_publication_spec,
)
from histdatacom.synthetic.qualification import QualificationStatus


def _source(key: str) -> DiagnosticSourceV1:
    digest = hashlib.sha256(key.encode()).hexdigest()
    return DiagnosticSourceV1(
        kind=f"{key}_v1",
        subject_schema_version=f"histdatacom.{key}.v1",
        subject_id=f"{key}:sha256:{digest}",
        relative_locator=f"fixtures/{key}-{digest}.json",
        size_bytes=100,
        sha256=digest,
    )


def _spec(
    tmp_path: Path, *, render: bool = False
) -> DiagnosticPublicationSpecV1:
    payload = b"{}\n"
    path = tmp_path / "dossier.json"
    path.write_bytes(payload)
    return DiagnosticPublicationSpecV1(
        qualification_dossier=ArtifactRef(
            kind="powered_qualification_dossier_v1",
            path=str(path),
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
            metadata={"dossier_id": "powered-dossier:test"},
        ),
        max_points_per_chart=128,
        renderer=DiagnosticRendererConfigV1(
            formats=(
                (
                    DiagnosticRenderFormat.SVG,
                    DiagnosticRenderFormat.PNG,
                )
                if render
                else ()
            ),
            width_px=480,
            height_px=320,
            dpi=96,
        ),
    )


def _context() -> tuple[SimpleNamespace, tuple[DiagnosticSourceV1, ...]]:
    source_by_key = {
        key: _source(key)
        for key in (
            "qualification",
            "evaluation",
            "experiment",
            "trace",
            "corpus",
            "scorecard",
            "feed_epochs",
            "observation_campaign",
        )
    }
    residual = SimpleNamespace(
        engine_id="histdatacom.event-clock.nhpp",
        split_kind="final_holdout",
        time_uniform_ks=0.08,
        time_lag1_autocorrelation=-0.03,
        mark_uniform_ks=0.06,
        mark_uniform_p_value=0.42,
        status=QualificationStatus.PASSED,
    )
    power_result = SimpleNamespace(
        gate_id="time_uniformity",
        misspecification_family="wrong_intensity",
        status=QualificationStatus.INSUFFICIENT_EVIDENCE,
        power_by_sample_size={"3": 0.1, "6": 0.2, "12": 0.4},
        false_positive_by_sample_size={"3": 0.04, "6": 0.02, "12": 0.01},
    )
    calibration = SimpleNamespace(
        weights={"histdatacom.event-clock.nhpp": 1.0},
        status=QualificationStatus.PASSED,
        reason_codes=("weights_frozen_before_holdout",),
    )
    score = SimpleNamespace(
        engine_id="histdatacom.event-clock.nhpp",
        split_kind="final_holdout",
        energy_score=0.8,
        variogram_score_p05=0.2,
        variogram_score_p1=0.3,
        marginal_crps=0.1,
        nominal_coverage=0.9,
        empirical_coverage=0.8,
        calibration_error=0.1,
        sharpness=0.25,
        tail_error=0.15,
        path_error=0.12,
        cross_series_error=0.12,
        status=QualificationStatus.PASSED,
    )
    dossier = SimpleNamespace(
        dossier_id="powered-dossier:test",
        experiment_id="experiment:test",
        campaign_id="campaign:test",
        residual_reports=(residual,),
        power_study=SimpleNamespace(results=(power_result,), reliable=False),
        portfolio_calibration=calibration,
        score_reports=(score,),
    )
    target = {
        "epoch_label": "technology_epoch_01",
        "symbol": "EURUSD",
        "parameter_values": {
            "retention_probability": 0.5,
            "duplicate_probability": 0.1,
            "unchanged_retention_probability": 0.9,
        },
        "parameter_lower_bounds": {
            "retention_probability": 0.4,
            "duplicate_probability": 0.05,
            "unchanged_retention_probability": 0.8,
        },
        "parameter_upper_bounds": {
            "retention_probability": 0.6,
            "duplicate_probability": 0.15,
            "unchanged_retention_probability": 1.0,
        },
        "parameter_status": {
            "retention_probability": "supported",
            "duplicate_probability": "supported",
            "unchanged_retention_probability": "bounded",
            "outage_window_ns": "unsupported",
        },
    }
    windows = tuple(
        SimpleNamespace(
            split_kind=split,
            window_id=f"window-{split}",
            period=f"20100{index}",
            session="london",
            epoch_label="technology_epoch_01",
            context_state="market_context:none",
            context_supported=True,
        )
        for index, split in enumerate(
            ("calibration", "validation", "final_holdout"), start=1
        )
    )
    observations = tuple(
        SimpleNamespace(
            observation_id=f"observation-{window.window_id}",
            window_id=window.window_id,
            method_name="non_homogeneous_poisson",
            split_kind=window.split_kind,
            reference_metrics={
                "event_rate_hz": 1.2,
                "interarrival_mean_seconds": 0.4,
                "spread_q95_pips": 1.1,
                "stale_run_fraction": 0.01,
                "timestamp_quantum_ms": 1.0,
            },
            candidate_metrics={"triangle_residual_p99_pips": 0.5},
            comparison_metrics={
                "event_count_relative_error": 0.1,
                "interarrival_quantile_relative_error": 0.2,
                "spread_tail_relative_error": 0.1,
                "stale_run_relative_error": 0.3,
                "burst_quiet_rate_error": 0.2,
                "unsupported_context_emission_count": 0.0,
                "path_realized_variation_relative_error": 0.2,
                "update_type_proportion_l1": 0.1,
                "update_transition_l1": 0.15,
                "simulation_mark_pit_ks": 0.08,
                "triangle_synchronization_error": 0.05,
            },
        )
        for window in windows
    )
    context = SimpleNamespace(
        dossier=dossier,
        evaluation=SimpleNamespace(
            engine_evidence=(
                SimpleNamespace(
                    engine_id="histdatacom.event-clock.nhpp",
                    method_name="non_homogeneous_poisson",
                    refusal_count=0,
                ),
            )
        ),
        trace=SimpleNamespace(observations=observations),
        experiment=SimpleNamespace(
            leakage_audit=SimpleNamespace(
                accepted=True,
                overlap_count=0,
                neighbor_guard_violation_count=0,
                shared_partition_count=0,
                shared_cohesion_group_count=0,
            )
        ),
        corpus=SimpleNamespace(windows=windows),
        feed_epochs={
            "boundaries": [
                {
                    "right_period": "201001",
                    "support": 1.0,
                    "transition_label": "technology transition",
                }
            ]
        },
        observation_campaign={"targets": [target]},
        optional_payloads=(),
        source_by_key=source_by_key,
        evidence_limitations=(),
    )
    return context, tuple(source_by_key.values())


def test_bundle_covers_every_family_without_inventing_missing_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, sources = _context()
    monkeypatch.setattr(
        diagnostics_module,
        "_load_evidence_context",
        lambda _: (context, sources),
    )

    bundle = build_reconstruction_diagnostic_bundle(_spec(tmp_path))

    assert {chart.family for chart in bundle.charts} == set(
        REQUIRED_DIAGNOSTIC_FAMILIES
    )
    assert len(bundle.charts) == 19
    by_family = {
        family: tuple(
            chart for chart in bundle.charts if chart.family is family
        )
        for family in REQUIRED_DIAGNOSTIC_FAMILIES
    }
    assert any(
        chart.status is DiagnosticStatus.UNDERPOWERED
        for chart in by_family[DiagnosticFamily.PROPER_SCORE_POWER]
    )
    assert {
        point.series
        for chart in by_family[DiagnosticFamily.FEED_EPOCH_OBSERVATION]
        for point in chart.points
    } >= {
        "reference event density",
        "reference event cadence",
        "reference spread q95",
        "reference stale-run fraction",
        "reference timestamp quantum",
    }
    assert {
        point.series
        for chart in by_family[DiagnosticFamily.PROPER_SCORE_POWER]
        for point in chart.points
    } >= {
        "final_holdout energy score",
        "final_holdout marginal CRPS",
        "time_uniformity",
    }
    assert all(
        chart.status is DiagnosticStatus.LIMITED
        for chart in by_family[DiagnosticFamily.CROSS_SERIES_RECONCILIATION]
    )
    for family in (
        DiagnosticFamily.CARVING_DECISION_FLOW,
        DiagnosticFamily.PRODUCT_ORIGIN_LINEAGE,
        DiagnosticFamily.BAR_STRATEGY_SENSITIVITY,
    ):
        assert len(by_family[family]) == 1
        chart = by_family[family][0]
        assert chart.status is DiagnosticStatus.UNAVAILABLE
        assert not chart.points
        assert chart.reason_codes
    encoded = bundle.to_json()
    assert '"raw_rows_embedded":false' in encoded
    assert '"automatic_winner":false' in encoded
    assert "oanda" not in encoded.lower()


def test_publication_round_trip_is_deterministic_and_tamper_evident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, sources = _context()
    monkeypatch.setattr(
        diagnostics_module,
        "_load_evidence_context",
        lambda _: (context, sources),
    )
    spec = _spec(tmp_path, render=True)
    spec_path = write_diagnostic_publication_spec(
        spec, tmp_path / "diagnostic-spec.json"
    )
    assert read_diagnostic_publication_spec(spec_path) == spec

    first = ReconstructionClient().publish_diagnostics(
        spec_path, output_directory=tmp_path / "publication"
    )
    second = publish_reconstruction_diagnostics(
        spec_path, output_directory=tmp_path / "publication"
    )
    assert first == second
    assert first.status_counts == {
        "available": 5,
        "limited": 3,
        "unavailable": 3,
        "underpowered": 1,
    }
    assert first.view_status_counts == {
        "available": 10,
        "limited": 5,
        "unavailable": 3,
        "underpowered": 1,
    }
    assert len(first.rendered_artifacts) == 2 * first.chart_count
    assert {item.format for item in first.rendered_artifacts} == {
        DiagnosticRenderFormat.SVG,
        DiagnosticRenderFormat.PNG,
    }
    manifest_path = next(
        (tmp_path / "publication").glob(
            "reconstruction-diagnostic-publication-*.json"
        )
    )
    verified, bundle = verify_reconstruction_diagnostic_publication(
        manifest_path
    )
    assert verified == first
    assert bundle.bundle_id == first.bundle_id
    listing = ReconstructionClient().diagnostics(manifest_path)
    assert listing["family_count"] == len(REQUIRED_DIAGNOSTIC_FAMILIES)
    assert listing["chart_count"] == 19
    assert listing["status_counts"] == {
        "available": 5,
        "limited": 3,
        "unavailable": 3,
        "underpowered": 1,
    }
    assert listing["view_status_counts"] == {
        "available": 10,
        "limited": 5,
        "unavailable": 3,
        "underpowered": 1,
    }
    assert str(tmp_path) not in bundle.to_json()

    rendered_path = (
        tmp_path / "publication" / first.rendered_artifacts[0].relative_path
    )
    rendered_path.write_bytes(rendered_path.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="size differs"):
        verify_reconstruction_diagnostic_publication(manifest_path)


def test_contracts_reject_local_publication_paths_and_broker_artifacts(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="local path"):
        DiagnosticSourceV1(
            kind="source",
            subject_schema_version="schema",
            subject_id="subject",
            relative_locator="/Users/example/private.json",
            size_bytes=1,
            sha256="1" * 64,
        )
    spec = _spec(tmp_path)
    broker = ArtifactRef(
        kind="broker_profile_v1",
        path=spec.qualification_dossier.path,
        size_bytes=spec.qualification_dossier.size_bytes,
        sha256=spec.qualification_dossier.sha256,
    )
    with pytest.raises(ValueError, match="later milestone"):
        DiagnosticPublicationSpecV1(
            qualification_dossier=spec.qualification_dossier,
            additional_artifacts=(broker,),
        )


def test_renderer_orders_numeric_axes_numerically() -> None:
    assert _ordered_x_labels({"12", "24", "3", "48", "6"}) == [
        "3",
        "6",
        "12",
        "24",
        "48",
    ]
    assert _ordered_x_labels(
        {
            "power 12",
            "power 3",
            "power 6",
            "false positive 12",
            "false positive 3",
            "false positive 6",
        }
    ) == [
        "false positive 3",
        "false positive 6",
        "false positive 12",
        "power 3",
        "power 6",
        "power 12",
    ]


def test_base_chart_data_remains_available_when_viz_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, sources = _context()
    monkeypatch.setattr(
        diagnostics_module,
        "_load_evidence_context",
        lambda _: (context, sources),
    )
    bundle = build_reconstruction_diagnostic_bundle(_spec(tmp_path))
    monkeypatch.setitem(sys.modules, "matplotlib", None)

    with pytest.raises(
        ModuleNotFoundError,
        match=r"require histdatacom\[viz\]",
    ):
        render_diagnostic_bundle(
            bundle,
            DiagnosticRendererConfigV1(formats=(DiagnosticRenderFormat.SVG,)),
            output_directory=tmp_path / "publication",
        )
