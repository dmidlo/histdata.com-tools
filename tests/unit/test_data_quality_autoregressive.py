"""Tests for explicit-order AR, ARMA, and ARIMA diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, cast

import polars as pl
import pytest

import histdatacom.data_quality.autoregressive as ar_module
from histdatacom.data_quality.autoregressive import (
    AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY,
    AUTOREGRESSIVE_COLUMNS,
    AUTOREGRESSIVE_SCHEMA_VERSION,
    AUTOREGRESSIVE_SUMMARY_METADATA_KEY,
    AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION,
    AutoregressiveProfile,
    AutoregressiveSpecification,
    autoregressive_from_training_frame,
    autoregressive_summary,
    project_autoregressive_onto_training_frame,
)
from histdatacom.data_quality.classical_model_contracts import (
    ClassicalModelInputProfile,
    ClassicalModelResourcePolicy,
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
from histdatacom.data_quality.profiles import (
    QUALITY_PROFILE_SCHEMA_VERSION,
    quality_profile_from_mapping,
)
from histdatacom.data_quality.reporting import (
    QualityExitPolicy,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
)
from histdatacom.data_quality.training_features import (
    ensure_tick_training_features,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    format_influx_line,
    read_polars_cache,
    write_polars_cache,
)


def test_full_family_is_explicit_deterministic_and_leakage_safe() -> None:
    """All three named families should refit independently on shared folds."""
    first = _run(_process_frame(260))
    second = _run(_process_frame(260))

    assert first.diagnostics == second.diagnostics
    assert first.diagnostics["schema_version"] == AUTOREGRESSIVE_SCHEMA_VERSION
    evaluation = _mapping(first.diagnostics["evaluation"])
    models = _mapping_rows(evaluation["models"])
    assert {model["family"] for model in models} == {"ar", "arma", "arima"}
    assert {
        tuple(_mapping(model["configuration"])["order"]) for model in models
    } == {
        (2, 0, 0),
        (1, 0, 1),
        (1, 1, 1),
    }
    rows = [
        row
        for model in models
        for row in _mapping_rows(_mapping(model)["fold_results"])
    ]
    assert rows
    assert all(row["future_values_visible"] is False for row in rows)
    assert all(
        row["residual_state_reused_across_origins"] is False for row in rows
    )
    assert all(row["original_scale"] is True for row in rows)
    assert evaluation["automatic_winner"] is False
    assert evaluation["comparison_semantics"] == "descriptive_shared_folds_only"
    assert _mapping(first.diagnostics["backend"])["provider"] == "statsmodels"


def test_orders_trends_estimators_and_fixed_parameters_validate_before_fit() -> (
    None
):
    """Invalid or ambiguous configurations must not reach the backend."""
    with pytest.raises(ValueError, match="AR requires"):
        AutoregressiveSpecification("bad-ar", "ar", 0)
    with pytest.raises(ValueError, match="ARMA requires"):
        AutoregressiveSpecification("bad-arma", "arma", 1)
    with pytest.raises(ValueError, match="ARIMA requires"):
        AutoregressiveSpecification("bad-arima", "arima", 1)
    with pytest.raises(ValueError, match="constant trend"):
        AutoregressiveSpecification("bad-trend", "arima", 1, d=1, trend="c")
    with pytest.raises(ValueError, match="AR-only"):
        AutoregressiveSpecification(
            "bad-estimator", "arma", 1, q=1, estimation_method="burg"
        )
    with pytest.raises(ValueError, match="fixed parameters require"):
        AutoregressiveSpecification(
            "bad-fixed",
            "ar",
            1,
            estimation_method="burg",
            fixed_parameters=(("ar.L1", 0.5),),
        )


def test_profile_parser_preserves_first_class_family_configuration() -> None:
    """Profile JSON should retain explicit families, orders, and constraints."""
    profile = (
        quality_profile_from_mapping(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "ar-family",
                "rules": {
                    "fingerprint.series": {
                        "autoregressive": {
                            "enabled": True,
                            "projection_specification_ids": ["ar-fixed"],
                            "specifications": [
                                {
                                    "specification_id": "ar-fixed",
                                    "family": "ar",
                                    "p": 2,
                                    "trend": "c",
                                    "initialization_method": "stationary",
                                    "estimation_method": "statespace",
                                    "fixed_parameters": {"ar.L1": 0.4},
                                    "max_iterations": 17,
                                }
                            ],
                        }
                    }
                },
            }
        )
        .fingerprint_profile()
        .autoregressive
    )

    assert profile.enabled is True
    assert profile.projection_specification_ids == ("ar-fixed",)
    assert profile.specifications[0].family == "ar"
    assert profile.specifications[0].fixed_parameters == (("ar.L1", 0.4),)
    assert profile.specifications[0].max_iterations == 17


def test_short_zero_variance_missing_and_transformed_inputs_fail_safely() -> (
    None
):
    """Structural limitations should remain bounded and never trigger filling."""
    short = _run(_process_frame(25))
    assert short.diagnostics["status"] in {"limited", "unavailable"}
    constant = _run(
        _process_frame(180).with_columns(
            pl.lit(1.0).alias("bid"), pl.lit(1.0002).alias("ask")
        )
    )
    reasons = _mapping(
        _mapping(constant.diagnostics["fit_summary"])["reason_counts"]
    )
    assert int(reasons["zero_variance"]) > 0

    timestamps = [1_000 + index * 100 for index in range(220)]
    timestamps = timestamps[:100] + [
        value + 2_000 for value in timestamps[100:]
    ]
    missing = _run(_process_frame(220, timestamps=timestamps))
    assert missing.diagnostics["forward_fill_policy"] == "never"
    assert all(
        sample["training_segment_policy"] == "trailing_contiguous_after_missing"
        for sample in _mapping_rows(
            _mapping(missing.diagnostics["fit_summary"])["fit_samples"]
        )
    )

    transformed = _run(
        _process_frame(260),
        input_profile=_input_profile(
            transform="log_level", differencing_order=1
        ),
    )
    assert transformed.diagnostics["original_scale_forecasts"] is True
    assert (
        transformed.diagnostics[
            "model_differencing_is_separate_from_input_differencing"
        ]
        is True
    )
    assert (
        _mapping(transformed.diagnostics["input_transform_policy"])["transform"]
        == "log_level"
    )


def test_dependency_absence_and_one_model_failure_are_isolated(
    monkeypatch: Any,
) -> None:
    """Optional-backend absence and fold failure should be advisory."""
    monkeypatch.setattr(ar_module, "_load_backend", lambda: None)
    unavailable = _run(_process_frame(180))
    assert unavailable.diagnostics["status"] == "unavailable"
    assert unavailable.diagnostics["reason"] == "dependency_unavailable"

    monkeypatch.undo()
    original = ar_module._fit_specification

    def fail_ar(*args: Any, **kwargs: Any) -> Any:
        specification = cast(AutoregressiveSpecification, args[0])
        if specification.family == "ar":
            return ar_module._empty_fit("failed", "singularity", 20)
        return original(*args, **kwargs)

    monkeypatch.setattr(ar_module, "_fit_specification", fail_ar)
    isolated = _run(_process_frame(220))
    models = _mapping_rows(
        _mapping(isolated.diagnostics["evaluation"])["models"]
    )
    statuses = {model["family"]: model["status"] for model in models}
    assert statuses["ar"] == "limited"
    assert statuses["arma"] == "ready"
    assert statuses["arima"] == "ready"
    assert isolated.diagnostics["hard_fail_quality_gate"] is False


def test_candidate_fit_memory_and_diagnostic_limits_are_enforced() -> None:
    """The inherited #421 policy must bound model work and retained artifacts."""
    resources = ClassicalModelResourcePolicy(
        max_source_rows=10_000,
        max_regularized_observations=10_000,
        max_folds=64,
        max_horizons=4,
        max_candidate_orders=1,
        max_fit_attempts=1,
        max_wall_time_seconds=30,
        max_memory_bytes=10_000_000,
        max_retained_diagnostics=1,
    )
    result = _run(
        _process_frame(260),
        input_profile=_input_profile(resources=resources),
    )
    usage = _mapping(result.diagnostics["resource_usage"])
    evaluation = _mapping(result.diagnostics["evaluation"])
    assert usage["fit_attempt_count"] == 1
    assert evaluation["model_count"] == 1
    assert result.diagnostics["status"] == "limited"
    assert result.diagnostics["reason"] == "resource_limit"
    assert (
        len(
            _mapping_rows(
                _mapping(result.diagnostics["fit_summary"])["fit_samples"]
            )
        )
        == 1
    )


def test_backend_options_fixed_parameters_and_ets_references_are_recorded() -> (
    None
):
    """Backend controls and #422 comparison data should remain explicit."""
    specification = AutoregressiveSpecification(
        "ar-fixed",
        "ar",
        1,
        trend="n",
        initialization_method="stationary",
        estimation_method="statespace",
        fixed_parameters=(("ar.L1", 0.5),),
        max_iterations=25,
    )
    profile = AutoregressiveProfile(
        enabled=True,
        specifications=(specification,),
        projection_specification_ids=("ar-fixed",),
    )
    ets = {
        "evaluation": {
            "models": [
                {
                    "status": "ready",
                    "family": "ses",
                    "specification_id": "ses",
                    "model_id": "sha256:ets",
                    "horizon_metrics": [{"horizon": 1, "mae": 0.1}],
                }
            ]
        }
    }
    result = autoregressive_from_training_frame(
        _process_frame(220),
        _fingerprint("EURUSD"),
        input_profile=_input_profile(),
        profile=profile,
        exponential_smoothing=ets,
        target=_target("DAT_ASCII_EURUSD_T_201202.csv"),
    )
    fit_sample = _mapping_rows(
        _mapping(result.diagnostics["fit_summary"])["fit_samples"]
    )[0]
    references = _mapping_rows(
        _mapping(result.diagnostics["evaluation"])[
            "reference_exponential_smoothing"
        ]
    )

    assert _mapping(fit_sample["parameters"])["ar.L1"] == pytest.approx(0.5)
    assert references[0]["model_id"] == "sha256:ets"
    assert references[0]["calculation_basis"] == "shared_regular_grid_folds"
    assert (
        _mapping(result.diagnostics["prerequisite_readiness"])[
            "recommendations_applied_automatically"
        ]
        is False
    )


def test_projection_is_flat_identity_safe_available_and_serializable(
    tmp_path: Path,
) -> None:
    """All family projections should survive duplicate timestamps and IPC."""
    raw = _process_frame(260, duplicate_timestamp=True)
    observed = raw.select("datetime", "bid", "ask").to_dicts()
    target = _target("DAT_ASCII_EURUSD_T_201202.csv")
    result = _run(raw, target=target)
    projected = project_autoregressive_onto_training_frame(
        raw, result, target=target
    )

    assert projected.select("datetime", "bid", "ask").to_dicts() == observed
    assert projected.get_column("row_id").n_unique() == raw.height
    assert set(AUTOREGRESSIVE_COLUMNS) <= set(projected.columns)
    for family in ("ar", "arma", "arima"):
        available = projected.filter(pl.col(f"cm_{family}_forecast_available"))
        assert available.height > 0
        assert available.get_column(f"cm_{family}_training_eligible").all()
        assert (
            available.get_column(f"cm_{family}_actual").null_count()
            == available.height
        )
    diagnostic = projected.filter(pl.col("cm_ar_diagnostic_available"))
    assert diagnostic.height > 0
    assert diagnostic.get_column("cm_ar_diagnostic_only").all()

    line = format_influx_line(
        "eurusd",
        "ascii",
        TICK,
        projected.filter(pl.col("cm_ar_forecast_available")).row(0),
        columns=projected.columns,
    )
    assert "cm_ar_forecast_available=true" in line
    cache = tmp_path / CACHE_FILENAME
    write_polars_cache(projected, cache)
    restored = read_polars_cache(cache)
    assert (
        restored.select(AUTOREGRESSIVE_COLUMNS).to_dicts()
        == projected.select(AUTOREGRESSIVE_COLUMNS).to_dicts()
    )


def test_projection_survives_masked_timestamps_and_legacy_enrichment() -> None:
    """Projection joins by durable identity and enriches legacy raw rows in memory."""
    raw = _process_frame(220)
    target = _target("legacy.data", QualityTargetKind.CACHE)
    enriched = ensure_tick_training_features(raw, target=target)
    result = _run(enriched, target=target)
    masked = enriched.with_columns(
        pl.when(pl.col("row_id") == 7)
        .then(None)
        .otherwise(pl.col("timestamp_utc_ms"))
        .alias("timestamp_utc_ms")
    )
    projected = project_autoregressive_onto_training_frame(masked, result)
    dropped = project_autoregressive_onto_training_frame(
        enriched.drop("timestamp_utc_ms", "datetime"), result
    )

    assert projected.get_column("row_id").to_list() == list(range(1, 221))
    assert dropped.get_column("cm_ar_forecast_available").any()
    legacy = _run(raw, target=target)
    assert (
        _mapping(legacy.input_result.contract["source"])[
            "legacy_cache_enriched_on_read"
        ]
        is True
    )


def test_columns_discovery_fingerprint_and_report_surfaces_are_complete() -> (
    None
):
    """Schema, opt-in rule, full, bounded, and CLI surfaces must agree."""
    definitions = {row.name: row for row in training_feature_definitions()}
    assert set(AUTOREGRESSIVE_COLUMNS) <= set(definitions)
    assert all(definitions[name].nullable for name in AUTOREGRESSIVE_COLUMNS)
    assert all(
        definitions[name].dtype in {"Utf8", "Int64", "Float64", "Boolean"}
        for name in AUTOREGRESSIVE_COLUMNS
    )
    assert "autoregressive" in FINGERPRINT_AUDIT_SECTIONS

    diagnostics = _run(_process_frame(220)).diagnostics
    finding = _finding(_target("DAT_ASCII_EURUSD_T_201202.csv"), diagnostics)
    report = QualityReport(
        targets=(finding.target,),
        rule_results=(
            QualityRuleResult(
                rule_id=finding.rule_id,
                target=finding.target,
                findings=(finding,),
            ),
        ),
    )
    full = quality_report_payload(report)
    bounded = bounded_quality_payload(
        report=report,
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"targets": []},
        artifact=None,
        decision=QualityExitPolicy().evaluate(report.summary()),
    )
    assert AUTOREGRESSIVE_SUMMARY_METADATA_KEY in _mapping(full["metadata"])
    assert AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY in bounded
    assert "Autoregressive models" in format_quality_console_summary(report)


def test_fingerprint_rule_is_opt_in_and_summary_matches_golden() -> None:
    """The rule should omit defaults and bounded run summaries should be stable."""
    assert not AutoregressiveProfile().enabled
    assert "autoregressive" not in _mapping(
        HistDataSeriesFingerprintRule()
        .evaluate(_target("missing.csv"))[0]
        .metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )

    findings = []
    for symbol in ("AUDUSD", "EURUSD", "GBPUSD"):
        diagnostics = _run(_process_frame(220), symbol=symbol).diagnostics
        findings.append(
            _finding(_target(f"DAT_ASCII_{symbol}_T_201202.csv"), diagnostics)
        )
    summary = autoregressive_summary(findings, target_limit=1)
    expected = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures"
            / "data_quality_reports"
            / "autoregressive_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert summary == expected
    assert (
        _mapping(summary)["schema_version"]
        == AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION
    )


def test_fingerprint_rule_emits_family_from_the_supported_csv_path(
    tmp_path: Path,
) -> None:
    """The ordinary fingerprint lifecycle should run the opt-in family."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    source.write_text("\n".join(_tick_lines(220)) + "\n", encoding="ascii")
    profile = HistDataFingerprintProfile(
        classical_model_input=_input_profile(frequency_ms=1_000),
        autoregressive=_profile(),
    )
    finding = HistDataSeriesFingerprintRule(profile=profile).evaluate(
        _target(str(source))
    )[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    audit = _mapping(fingerprint["fingerprint_audit"])

    assert _mapping(fingerprint["autoregressive"])["status"] in {
        "ready",
        "limited",
    }
    assert "autoregressive" in audit["sections_expected"]
    assert "autoregressive" in audit["sections_emitted"]
    assert _mapping(audit["section_statuses"])["autoregressive"] in {
        "valid",
        "limited",
    }


def _run(
    frame: pl.DataFrame,
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    target: QualityTarget | None = None,
    symbol: str = "EURUSD",
) -> Any:
    return autoregressive_from_training_frame(
        frame,
        _fingerprint(symbol),
        input_profile=input_profile or _input_profile(),
        profile=_profile(),
        target=target or _target(f"DAT_ASCII_{symbol}_T_201202.csv"),
    )


def _profile() -> AutoregressiveProfile:
    return AutoregressiveProfile(
        enabled=True,
        specifications=(
            AutoregressiveSpecification("ar-2", "ar", 2, trend="c"),
            AutoregressiveSpecification("arma-1-1", "arma", 1, q=1, trend="c"),
            AutoregressiveSpecification("arima-1-1-1", "arima", 1, d=1, q=1),
        ),
        projection_specification_ids=("ar-2", "arma-1-1", "arima-1-1-1"),
    )


def _input_profile(**overrides: Any) -> ClassicalModelInputProfile:
    values: dict[str, Any] = {
        "enabled": True,
        "frequency_ms": 500,
        "minimum_training_observations": 20,
        "minimum_evaluation_observations": 1,
        "step_size": 100,
        "horizons": (1, 2),
        "resources": ClassicalModelResourcePolicy(
            max_source_rows=10_000,
            max_regularized_observations=10_000,
            max_folds=64,
            max_horizons=4,
            max_candidate_orders=16,
            max_fit_attempts=32,
            max_wall_time_seconds=30,
            max_retained_diagnostics=64,
        ),
    }
    values.update(overrides)
    return ClassicalModelInputProfile(**values)


def _process_frame(
    count: int,
    *,
    timestamps: list[int] | None = None,
    duplicate_timestamp: bool = False,
) -> pl.DataFrame:
    times = timestamps or [1_000 + index * 100 for index in range(count)]
    if duplicate_timestamp and count > 1:
        times[1] = times[0]
    innovations = [((index * 17) % 13 - 6) * 0.00003 for index in range(count)]
    prices = [1.0, 1.0001]
    for index in range(2, count):
        delta = 0.65 * (prices[-1] - prices[-2]) + innovations[index]
        prices.append(prices[-1] + 0.00005 + delta)
    return pl.DataFrame(
        {
            "datetime": times,
            "bid": prices,
            "ask": [value + 0.0002 for value in prices],
            "vol": [0] * count,
        }
    )


def _fingerprint(symbol: str) -> dict[str, Any]:
    return {
        "fingerprint_id": f"fingerprint-{symbol.lower()}",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": symbol,
            "period": "201202",
            "kind": "cache",
        },
        "fingerprint_audit": {
            "section_statuses": {
                "dependence": "valid",
                "stationarity_diagnostics": "limited",
            }
        },
        "stationarity_diagnostics": {"recommended_transforms": ["log_return"]},
    }


def _tick_lines(count: int) -> tuple[str, ...]:
    return tuple(
        (
            f"20120201 00{index // 60:02d}{index % 60:02d}000,"
            f"{1.0 + index * 0.001:.6f},"
            f"{1.0002 + index * 0.001:.6f},0"
        )
        for index in range(count)
    )


def _target(
    path: str, kind: QualityTargetKind = QualityTargetKind.CSV
) -> QualityTarget:
    symbol = next(
        (
            item
            for item in ("AUDUSD", "EURUSD", "GBPUSD")
            if item in path.upper()
        ),
        "EURUSD",
    )
    return QualityTarget(
        path=path,
        kind=kind,
        data_format="ascii",
        timeframe=TICK,
        symbol=symbol,
        period="201202",
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
                "autoregressive": dict(diagnostics)
            }
        },
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [_mapping(row) for row in cast(list[Any], value)]
