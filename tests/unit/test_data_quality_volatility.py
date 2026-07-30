"""Tests for explicit ARCH/GARCH volatility contracts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

import histdatacom.data_quality.volatility as volatility_module
from histdatacom.data_quality.classical_model_contracts import (
    ClassicalModelInputProfile,
    ClassicalModelResourcePolicy,
)
from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.fingerprint_contracts import (
    implemented_fingerprint_target_section_names,
)
from histdatacom.data_quality.fingerprints import (
    FINGERPRINT_AUDIT_SECTIONS,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.profiles import (
    QUALITY_PROFILE_SCHEMA_VERSION,
    quality_profile_from_mapping,
)
from histdatacom.data_quality.training_features import (
    VOLATILITY_COLUMNS,
    required_training_feature_columns,
    training_feature_definitions,
)
from histdatacom.data_quality.volatility import (
    ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY,
    VOLATILITY_SCHEMA_VERSION,
    VOLATILITY_SUMMARY_SCHEMA_VERSION,
    VolatilityProfile,
    VolatilitySpecification,
    project_volatility_onto_training_frame,
    volatility_from_training_frame,
    volatility_summary,
)
from histdatacom.histdata_ascii import (
    TICK,
    format_influx_line,
    read_polars_cache,
    write_polars_cache,
)


def test_arch_and_garch_are_deterministic_and_keep_metrics_separate() -> None:
    result = _run(_process_frame(1_000))
    repeated = _run(_process_frame(1_000))

    assert result.diagnostics == repeated.diagnostics
    assert result.diagnostics["schema_version"] == VOLATILITY_SCHEMA_VERSION
    evaluation = _mapping(result.diagnostics["evaluation"])
    models = _mapping_rows(evaluation["models"])
    assert {model["family"] for model in models} == {"arch", "garch"}
    assert evaluation["automatic_winner"] is False
    assert evaluation["variance_scale"] == "unscaled_return_squared"
    assert evaluation["mean_scale"] == "unscaled_return"
    assert evaluation["realized_variance_proxy"] == "squared_return"
    assert evaluation["reference_variance_baselines"]
    assert evaluation["baseline_relative_skill"]
    for model in models:
        for metrics in _mapping(model["horizon_metrics"]).values():
            summary = _mapping(metrics)
            assert "mean_metrics" in summary
            assert "variance_metrics" in summary
            assert summary["metrics_are_not_interchangeable"] is True
        fit_samples = _mapping_rows(model["fit_samples"])
        assert all(
            sample["covariance_condition_number"] is not None
            for sample in fit_samples
        )


def test_projection_is_same_row_point_in_time_and_survives_cache_and_influx(
    tmp_path: Path,
) -> None:
    frame = _process_frame(1_000)
    result = _run(frame)
    projected = project_volatility_onto_training_frame(
        frame, result, target=_target()
    )

    assert set(VOLATILITY_COLUMNS).issubset(projected.columns)
    for family in ("arch", "garch"):
        forecasts = projected.filter(pl.col(f"cm_{family}_forecast_available"))
        diagnostics = projected.filter(
            pl.col(f"cm_{family}_diagnostic_available")
        )
        assert forecasts.height > 0
        assert diagnostics.height > 0
        assert forecasts.get_column(f"cm_{family}_training_eligible").all()
        assert diagnostics.get_column(f"cm_{family}_diagnostic_only").all()

    cache_path = tmp_path / "volatility.data"
    write_polars_cache(projected, cache_path)
    restored = read_polars_cache(cache_path)
    assert (
        restored.select(VOLATILITY_COLUMNS).to_dicts()
        == projected.select(VOLATILITY_COLUMNS).to_dicts()
    )
    available = restored.filter(pl.col("cm_garch_forecast_available")).row(0)
    line = format_influx_line(
        "EURUSD", "ascii", TICK, available, columns=restored.columns
    )
    assert "cm_garch_variance_forecast=" in line
    assert "cm_garch_training_eligible=true" in line


def test_legacy_masked_and_dropped_frames_receive_nullable_columns() -> None:
    frame = _process_frame(800)
    result = _run(frame)
    enriched = project_volatility_onto_training_frame(
        frame, result, target=_target()
    )
    masked = enriched.with_columns(
        pl.lit(None).cast(pl.Float64).alias("cm_garch_variance_forecast")
    )
    dropped = enriched.drop(VOLATILITY_COLUMNS)

    masked_projection = project_volatility_onto_training_frame(masked, result)
    dropped_projection = project_volatility_onto_training_frame(dropped, result)
    assert masked_projection.select(VOLATILITY_COLUMNS).to_dicts() == (
        dropped_projection.select(VOLATILITY_COLUMNS).to_dicts()
    )


def test_configuration_guards_and_stable_failures_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="ARCH requires"):
        VolatilitySpecification("bad", "arch", variance_order=1)
    with pytest.raises(ValueError, match="reference ID"):
        VolatilitySpecification(
            "bad", "garch", input_definition="mean_model_residual"
        )
    with pytest.raises(ValueError, match="zero mean"):
        VolatilitySpecification(
            "bad",
            "garch",
            input_definition="demeaned_return",
            mean_model="constant",
        )

    level_input = _input_profile(transform="level")
    invalid = _run(_process_frame(800), input_profile=level_input)
    reasons = _mapping(
        _mapping(invalid.diagnostics["fit_summary"])["reason_counts"]
    )
    assert reasons == {"invalid_transform": 12}

    singular = _run(
        _process_frame(800),
        profile=VolatilityProfile(
            enabled=True, maximum_covariance_condition_number=10.0
        ),
    )
    singular_reasons = _mapping(
        _mapping(singular.diagnostics["fit_summary"])["reason_counts"]
    )
    assert singular_reasons == {"singular_covariance": 12}

    monkeypatch.setattr(volatility_module, "_load_backend", lambda: None)
    unavailable = _run(_process_frame(800))
    assert unavailable.diagnostics["reason"] == "dependency_unavailable"
    assert "traceback" not in str(unavailable.diagnostics).lower()


def test_profile_discovery_columns_and_asymmetric_registry_are_public() -> None:
    parsed = quality_profile_from_mapping(
        {
            "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
            "name": "volatility",
            "rules": {
                "fingerprint.series": {
                    "classical_model_input": {
                        "enabled": True,
                        "transform": "return",
                    },
                    "volatility": {
                        "enabled": True,
                        "annualization_periods": 252,
                        "specifications": [
                            {
                                "specification_id": "arch-t",
                                "family": "arch",
                                "distribution": "students_t",
                                "innovation_order": 3,
                            },
                            {
                                "specification_id": "garch-normal",
                                "family": "garch",
                                "mean_model": "constant",
                                "innovation_order": 1,
                                "variance_order": 1,
                            },
                        ],
                    },
                }
            },
        }
    ).fingerprint_profile()
    assert parsed.volatility.enabled is True
    assert parsed.volatility.annualization_periods == 252
    assert parsed.volatility.projection_specification_ids == (
        "arch-t",
        "garch-normal",
    )
    assert "volatility" in FINGERPRINT_AUDIT_SECTIONS
    assert "volatility" in implemented_fingerprint_target_section_names()
    assert set(VOLATILITY_COLUMNS).issubset(required_training_feature_columns())
    definitions = {item.name: item for item in training_feature_definitions()}
    assert all(
        definitions[name].source == "volatility" for name in VOLATILITY_COLUMNS
    )
    assert {
        item["family"] for item in ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY
    } == {
        "gjr_garch",
        "egarch",
    }
    assert all(
        item["status"] == "registered_not_enabled"
        for item in ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY
    )


def test_summary_is_bounded_and_schema_versioned() -> None:
    findings = [
        _finding(
            _target(symbol="EURUSD"), _run(_process_frame(800)).diagnostics
        ),
        _finding(
            _target(symbol="GBPUSD"), _run(_process_frame(800)).diagnostics
        ),
    ]
    summary = volatility_summary(findings, target_limit=1)
    assert summary is not None
    assert summary["schema_version"] == VOLATILITY_SUMMARY_SCHEMA_VERSION
    assert summary["target_count"] == 2
    assert summary["included_target_count"] == 1
    assert summary["truncated"] is True


def test_fingerprint_rule_runs_opt_in_volatility_on_supported_csv(
    tmp_path: Path,
) -> None:
    source = tmp_path / "DAT_ASCII_EURUSD_T_201201.csv"
    source.write_text("\n".join(_tick_lines(1_000)) + "\n", encoding="ascii")
    finding = HistDataSeriesFingerprintRule(
        profile=HistDataFingerprintProfile(
            classical_model_input=_input_profile(),
            volatility=VolatilityProfile(enabled=True),
        )
    ).evaluate(_target(path=str(source)))[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    audit = _mapping(fingerprint["fingerprint_audit"])
    assert _mapping(fingerprint["volatility"])["status"] in {
        "ready",
        "limited",
    }
    assert "volatility" in audit["sections_expected"]
    assert "volatility" in audit["sections_emitted"]


def _run(
    frame: pl.DataFrame,
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: VolatilityProfile | None = None,
) -> Any:
    return volatility_from_training_frame(
        frame,
        _fingerprint(),
        input_profile=input_profile or _input_profile(),
        profile=profile or VolatilityProfile(enabled=True),
        target=_target(),
    )


def _input_profile(**overrides: Any) -> ClassicalModelInputProfile:
    values: dict[str, Any] = {
        "enabled": True,
        "frequency_ms": 60_000,
        "transform": "return",
        "minimum_training_observations": 32,
        "minimum_evaluation_observations": 1,
        "step_size": 20,
        "horizons": (1, 2),
        "resources": ClassicalModelResourcePolicy(
            max_source_rows=10_000,
            max_regularized_observations=10_000,
            max_folds=12,
            max_horizons=4,
            max_candidate_orders=16,
            max_fit_attempts=12,
            max_wall_time_seconds=30,
            max_retained_diagnostics=12,
        ),
    }
    values.update(overrides)
    return ClassicalModelInputProfile(**values)


def _process_frame(count: int) -> pl.DataFrame:
    base = 1_325_376_000_000
    prices = [
        1.1
        + index * 0.000001
        + ((index * 17) % 23 - 11) * 0.000003
        + ((index // 7) % 5 - 2) * 0.00001
        for index in range(count)
    ]
    return pl.DataFrame(
        {
            "datetime": [base + index * 10_000 for index in range(count)],
            "bid": prices,
            "ask": [value + 0.0002 for value in prices],
            "vol": [0] * count,
        }
    )


def _tick_lines(count: int) -> tuple[str, ...]:
    start = datetime(2012, 1, 1)
    return tuple(
        (
            f"{(start + timedelta(seconds=index * 10)).strftime('%Y%m%d %H%M%S')}000,"
            f"{1.1 + index * 0.000001 + ((index * 17) % 23 - 11) * 0.000003:.6f},"
            f"{1.1002 + index * 0.000001 + ((index * 17) % 23 - 11) * 0.000003:.6f},0"
        )
        for index in range(count)
    )


def _fingerprint() -> dict[str, Any]:
    return {
        "fingerprint_id": "fingerprint-eurusd",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": "EURUSD",
            "period": "201201",
            "kind": "cache",
        },
    }


def _target(
    *, symbol: str = "EURUSD", path: str | None = None
) -> QualityTarget:
    return QualityTarget(
        path=path or f"DAT_ASCII_{symbol}_T_201201.csv",
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol=symbol,
        period="201201",
    )


def _finding(
    target: QualityTarget, diagnostics: Mapping[str, Any]
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id="fingerprint.series",
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "volatility": dict(diagnostics)
            }
        },
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [_mapping(row) for row in cast(list[Any], value)]
