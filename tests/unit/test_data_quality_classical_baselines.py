"""Tests for optional classical baseline diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, cast

import polars as pl

from histdatacom.data_quality.classical_baselines import (
    CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY,
    CLASSICAL_BASELINE_SCHEMA_VERSION,
    CLASSICAL_BASELINE_SUMMARY_METADATA_KEY,
    CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION,
    CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION,
    ClassicalBaselineProfile,
    classical_baseline_diagnostics_from_training_frame,
    classical_baseline_summary,
    project_classical_baseline_onto_training_frame,
)
from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.fingerprints import (
    FINGERPRINT_AUDIT_SECTIONS,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.reporting import (
    QualityExitPolicy,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
)
from histdatacom.data_quality.training_features import (
    ensure_tick_training_features,
)
from histdatacom.histdata_ascii import TICK, format_influx_line
from histdatacom.runtime_contracts import JSONValue


def test_classical_baselines_are_deterministic_and_use_explicit_split() -> None:
    """Low-dependency models should be repeatable and leakage-resistant."""
    frame = _enriched_frame(30, session_states=(1, 2, 3))
    profile = _profile()
    fingerprint = _fingerprint()

    first = classical_baseline_diagnostics_from_training_frame(
        frame, fingerprint, profile=profile
    )
    second = classical_baseline_diagnostics_from_training_frame(
        frame, fingerprint, profile=profile
    )

    assert first == second
    assert first["schema_version"] == CLASSICAL_BASELINE_SCHEMA_VERSION
    assert first["status"] == "ready"
    split = _mapping(first["split_policy"])
    assert split == {
        "kind": "chronological_holdout",
        "order_by": ["series_id", "period", "row_id"],
        "timestamp_required": False,
        "shuffle": False,
        "walk_forward": True,
        "future_values_visible": False,
        "evaluation_fraction": 0.2,
        "row_count": 30,
        "training_row_count": 24,
        "evaluation_row_count": 6,
        "split_index": 24,
        "split_row_id": 25,
        "metrics_emitted": True,
    }
    evaluation = _mapping(first["evaluation"])
    assert evaluation["status"] == "evaluated"
    assert evaluation["transforms_applied"] == []
    assert evaluation["guard_codes"] == []
    models = _mapping_rows(evaluation["models"])
    assert [model["model"] for model in models] == [
        "naive_random_walk",
        "rolling_mean",
        "rolling_median",
        "session_seasonal_naive",
    ]
    assert all(model["status"] == "evaluated" for model in models)
    assert _mapping(evaluation["best_model"])["model"] == ("naive_random_walk")


def test_classical_baselines_do_not_require_timestamp_for_identity() -> None:
    """Timestamp masking must not break deterministic baseline identity."""
    frame = (
        _enriched_frame(31)
        .with_columns(
            pl.when(pl.col("row_id") == 2)
            .then(False)
            .otherwise(pl.col("training_usable"))
            .alias("training_usable")
        )
        .drop("datetime", "timestamp_utc_ms")
    )

    diagnostics = classical_baseline_diagnostics_from_training_frame(
        frame, _fingerprint(), profile=_profile()
    )
    projected = project_classical_baseline_onto_training_frame(
        frame, diagnostics
    )

    assert diagnostics["status"] == "ready"
    assert projected.get_column("row_id").to_list() == list(range(1, 32))
    assert projected.get_column("series_id").n_unique() == 1
    assert projected.get_column("baseline_training_ready").all()
    assert projected.get_column("baseline_split_row_id").unique().item() == 26


def test_classical_baseline_projection_preserves_observed_rows_and_influx() -> (
    None
):
    """Projection should preserve quotes and share the enriched Influx point."""
    frame = _enriched_frame(30, duplicate_timestamp=True)
    observed = frame.select("row_id", "datetime", "bid", "ask").to_dicts()
    diagnostics = classical_baseline_diagnostics_from_training_frame(
        frame, _fingerprint(), profile=_profile()
    )

    projected = project_classical_baseline_onto_training_frame(
        frame, diagnostics
    )

    assert projected.select("row_id", "datetime", "bid", "ask").to_dicts() == (
        observed
    )
    assert projected.get_column("row_id").n_unique() == 30
    lines = [
        format_influx_line(
            "eurusd",
            "ascii",
            TICK,
            row,
            columns=projected.columns,
        )
        for row in projected.head(2).iter_rows()
    ]
    assert lines[0].endswith(" 1000")
    assert lines[1].endswith(" 1000")
    assert "row_id=1" in lines[0].split(" ", maxsplit=1)[0]
    assert "row_id=2" in lines[1].split(" ", maxsplit=1)[0]
    assert "baseline_status_code=3i" in lines[0]
    assert "baseline_training_ready=true" in lines[0]
    assert "bidquote=1.0" in lines[0]


def test_classical_baselines_enrich_legacy_raw_tick_frame() -> None:
    """Legacy raw caches should be enriched before baseline evaluation."""
    raw = _raw_frame(30)
    target = _target(".data", QualityTargetKind.CACHE)

    diagnostics = classical_baseline_diagnostics_from_training_frame(
        raw,
        _fingerprint(),
        profile=_profile(),
        target=target,
    )

    substrate = _mapping(diagnostics["training_substrate"])
    assert diagnostics["status"] == "ready"
    assert substrate["legacy_cache_enriched_on_read"] is True
    assert substrate["timestamp_required"] is False


def test_classical_baselines_report_insufficient_data_without_metrics() -> None:
    """Accuracy metrics require a valid chronological split."""
    diagnostics = classical_baseline_diagnostics_from_training_frame(
        _enriched_frame(8), _fingerprint(), profile=_profile()
    )

    assert diagnostics["status"] == "unavailable"
    assert diagnostics["reason"] == "insufficient_training_rows"
    evaluation = _mapping(diagnostics["evaluation"])
    assert evaluation["status"] == "not_evaluated"
    assert evaluation["models"] == []
    projection = _mapping(
        _mapping(diagnostics["training_projection"])["values"]
    )
    assert _mapping(diagnostics["training_projection"])["schema_version"] == (
        CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION
    )
    assert projection["baseline_training_ready"] is False
    assert projection["baseline_exclusion_reason_code"] == 3


def test_stationarity_guards_are_advisory_and_do_not_hide_metrics() -> None:
    """Limited/unavailable stationarity should change readiness, not severity."""
    for stationarity_status, guard in (
        ("limited", "stationarity_limited"),
        ("unavailable", "stationarity_unavailable"),
    ):
        fingerprint = _fingerprint(stationarity_status=stationarity_status)
        diagnostics = classical_baseline_diagnostics_from_training_frame(
            _enriched_frame(30), fingerprint, profile=_profile()
        )

        assert diagnostics["status"] == "limited"
        prerequisite = _mapping(diagnostics["prerequisite_readiness"])
        assert prerequisite["stationarity_status"] == stationarity_status
        evaluation = _mapping(diagnostics["evaluation"])
        assert guard in _strings(evaluation["guard_codes"])
        assert evaluation["evaluated_model_count"] > 0
        assert _mapping(evaluation["best_model"])["mae"] is not None


def test_transform_recommendations_are_reported_but_not_applied() -> None:
    """Fingerprint transform advice should remain explicit and non-mutating."""
    fingerprint = _fingerprint(
        recommended_transforms=(
            "log_return",
            "differencing",
            "session_conditioning",
        )
    )
    diagnostics = classical_baseline_diagnostics_from_training_frame(
        _enriched_frame(30), fingerprint, profile=_profile()
    )

    evaluation = _mapping(diagnostics["evaluation"])
    assert diagnostics["status"] == "limited"
    assert evaluation["transforms_applied"] == []
    assert evaluation["recommended_transforms"] == [
        "log_return",
        "differencing",
        "session_conditioning",
    ]
    projection = _mapping(
        _mapping(diagnostics["training_projection"])["values"]
    )
    assert projection["baseline_transform_advisory_code"] == 7


def test_fingerprint_profile_opt_in_emits_baselines_and_audit_status(
    tmp_path: Path,
) -> None:
    """The ordinary fingerprint path should emit baselines only when enabled."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    source.write_text("\n".join(_tick_lines(30)) + "\n", encoding="utf-8")
    target = _target(str(source), QualityTargetKind.CSV)
    baseline_profile = _profile()
    fingerprint_profile = HistDataFingerprintProfile(
        rolling_windows=(2, 3),
        classical_baselines=baseline_profile,
    )

    finding = HistDataSeriesFingerprintRule(
        profile=fingerprint_profile
    ).evaluate(target)[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    default_fingerprint = _mapping(
        HistDataSeriesFingerprintRule()
        .evaluate(target)[0]
        .metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )

    assert "classical_baselines" in fingerprint
    assert "classical_baselines" not in default_fingerprint
    audit = _mapping(fingerprint["fingerprint_audit"])
    assert "classical_baselines" in audit["sections_expected"]
    assert "classical_baselines" in audit["sections_emitted"]
    assert _mapping(audit["section_statuses"])["classical_baselines"] in {
        "valid",
        "limited",
    }
    assert "classical_baselines" in FINGERPRINT_AUDIT_SECTIONS
    assert finding.severity.value == "info"


def test_classical_baseline_report_bounded_and_console_surfaces(
    tmp_path: Path,
) -> None:
    """Opt-in diagnostics should not be trapped in nested findings."""
    finding, target = _baseline_finding(tmp_path)
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=finding.rule_id,
                target=target,
                findings=(finding,),
            ),
        ),
        metadata={"check_groups": ["fingerprint"]},
    )

    report_payload = quality_report_payload(report)
    summary = _mapping(
        _mapping(report_payload["metadata"])[
            CLASSICAL_BASELINE_SUMMARY_METADATA_KEY
        ]
    )
    bounded = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"targets": []},
        report=report,
        decision=QualityExitPolicy().evaluate(report.summary()),
        artifact=None,
    )
    console = format_quality_console_summary(
        report, check_groups=("fingerprint",)
    )

    assert (
        summary["schema_version"] == CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 1
    assert CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY in bounded
    assert "Classical fingerprint baselines" in console
    assert "best=naive_random_walk" in console


def test_classical_baseline_summary_is_bounded() -> None:
    """Run summaries should retain full counts while bounding target details."""
    findings = []
    for symbol in ("AUDUSD", "EURUSD", "GBPUSD"):
        target = _target(f"DAT_ASCII_{symbol}_T_201202.csv")
        diagnostics = classical_baseline_diagnostics_from_training_frame(
            _enriched_frame(30, symbol=symbol),
            _fingerprint(symbol=symbol),
            profile=_profile(),
        )
        findings.append(_finding(target, diagnostics))

    summary = cast(
        Mapping[str, JSONValue],
        classical_baseline_summary(findings, target_limit=1),
    )

    assert summary["target_count"] == 3
    assert summary["included_target_count"] == 1
    assert summary["omitted_target_count"] == 2
    assert summary["truncated"] is True
    assert len(cast(list[JSONValue], summary["target_summaries"])) == 1


def test_classical_baseline_golden_fixture() -> None:
    """The public diagnostic contract should remain golden-testable."""
    payload = classical_baseline_diagnostics_from_training_frame(
        _enriched_frame(30, session_states=(1, 2, 3)),
        _fingerprint(),
        profile=_profile(),
    )
    expected = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures"
            / "data_quality_reports"
            / "classical_baseline.json"
        ).read_text(encoding="utf-8")
    )

    assert payload == expected


def _profile() -> ClassicalBaselineProfile:
    return ClassicalBaselineProfile(
        enabled=True,
        evaluation_fraction=0.2,
        minimum_training_rows=10,
        minimum_evaluation_rows=5,
        rolling_windows=(3,),
        session_seasonal_enabled=True,
        rounding_digits=6,
    )


def _raw_frame(
    count: int,
    *,
    duplicate_timestamp: bool = False,
) -> pl.DataFrame:
    timestamps = [1000 + index * 100 for index in range(count)]
    if duplicate_timestamp and count > 1:
        timestamps[1] = timestamps[0]
    bids = [1.0 + index * 0.01 for index in range(count)]
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": bids,
            "ask": [value + 0.0002 for value in bids],
            "vol": [0] * count,
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )


def _enriched_frame(
    count: int,
    *,
    symbol: str = "EURUSD",
    session_states: tuple[int, ...] = (0,),
    duplicate_timestamp: bool = False,
) -> pl.DataFrame:
    frame = ensure_tick_training_features(
        _raw_frame(count, duplicate_timestamp=duplicate_timestamp),
        symbol=symbol,
        data_format="ascii",
        timeframe=TICK,
        period="201202",
    )
    states = [
        session_states[index % len(session_states)] for index in range(count)
    ]
    return frame.with_columns(
        pl.Series("class_session_state_code", states, dtype=pl.Int32)
    )


def _fingerprint(
    *,
    symbol: str = "EURUSD",
    stationarity_status: str = "valid",
    recommended_transforms: tuple[str, ...] = (),
) -> dict[str, JSONValue]:
    raw_status = {
        "valid": "ok",
        "limited": "limited",
        "unavailable": "unavailable",
    }[stationarity_status]
    section_statuses = {
        "coverage": "valid",
        "temporal_topology": "valid",
        "calendar_regimes": "valid",
        "tick_distribution": "valid",
        "microstructure_dynamics": "valid",
        "dependence": "valid",
        "stationarity_diagnostics": stationarity_status,
        "decomposition": "valid",
    }
    return {
        "fingerprint_id": f"fingerprint-{symbol.lower()}",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": symbol,
            "period": "201202",
            "kind": "cache",
        },
        "calendar_regimes": {"status": "ok"},
        "stationarity_diagnostics": {
            "stationarity_status": raw_status,
            "reason": (
                None
                if stationarity_status == "valid"
                else f"stationarity_{stationarity_status}"
            ),
            "rolling_windows": {"3": {"status": "computed"}},
            "computed_window_count": 1,
            "skipped_window_count": 0,
            "zero_variance_metrics": [],
            "first_middle_last_distribution_shift": {"status": "computed"},
            "recommended_transforms": list(recommended_transforms),
        },
        "fingerprint_audit": {"section_statuses": section_statuses},
    }


def _target(
    path: str,
    kind: QualityTargetKind = QualityTargetKind.CSV,
) -> QualityTarget:
    name = Path(path).name.upper()
    symbol = "EURUSD"
    for candidate in ("AUDUSD", "EURUSD", "GBPUSD"):
        if candidate in name:
            symbol = candidate
            break
    return QualityTarget(
        path=path,
        kind=kind,
        data_format="ascii",
        timeframe=TICK,
        symbol=symbol,
        period="201202",
    )


def _finding(
    target: QualityTarget, diagnostics: Mapping[str, JSONValue]
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id="fingerprint.series",
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "classical_baselines": dict(diagnostics)
            }
        },
    )


def _baseline_finding(tmp_path: Path) -> tuple[QualityFinding, QualityTarget]:
    target = _target(str(tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"))
    diagnostics = classical_baseline_diagnostics_from_training_frame(
        _enriched_frame(30), _fingerprint(), profile=_profile()
    )
    return _finding(target, diagnostics), target


def _tick_lines(count: int) -> tuple[str, ...]:
    return tuple(
        (
            f"20120201 00{index // 60:02d}{index % 60:02d}000,"
            f"{1.0 + index * 0.01:.6f},"
            f"{1.0002 + index * 0.01:.6f},0"
        )
        for index in range(count)
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    assert isinstance(value, Mapping)
    return value


def _mapping_rows(value: Any) -> list[Mapping[str, Any]]:
    assert isinstance(value, list)
    assert all(isinstance(item, Mapping) for item in value)
    return cast(list[Mapping[str, Any]], value)


def _strings(value: Any) -> list[str]:
    assert isinstance(value, list)
    return [str(item) for item in value]
