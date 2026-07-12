"""Tests for optional exponential-smoothing model diagnostics."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, cast
import warnings

import polars as pl
import pytest
from statsmodels.tools.sm_exceptions import ConvergenceWarning

import histdatacom.data_quality.exponential_smoothing as ets_module
from histdatacom.data_quality.classical_model_contracts import (
    ClassicalModelInputProfile,
    ClassicalModelResourcePolicy,
)
from histdatacom.data_quality.exponential_smoothing import (
    EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY,
    EXPONENTIAL_SMOOTHING_COLUMNS,
    EXPONENTIAL_SMOOTHING_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY,
    EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION,
    ExponentialSmoothingProfile,
    ExponentialSmoothingSpecification,
    exponential_smoothing_from_training_frame,
    exponential_smoothing_summary,
    project_exponential_smoothing_onto_training_frame,
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
    training_feature_definitions,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    format_influx_line,
    parse_ascii_lines,
    read_polars_cache,
    to_polars_frame,
    write_polars_cache,
)


def test_full_exponential_smoothing_family_is_explicit_and_deterministic() -> (
    None
):
    """SES, Holt, damped Holt, Holt-Winters, and ETS should share folds."""
    profile = _family_profile()
    first = exponential_smoothing_from_training_frame(
        _raw_frame(240),
        _fingerprint(),
        input_profile=_input_profile(),
        profile=profile,
    )
    second = exponential_smoothing_from_training_frame(
        _raw_frame(240),
        _fingerprint(),
        input_profile=_input_profile(),
        profile=profile,
    )

    assert first.diagnostics == second.diagnostics
    assert (
        first.diagnostics["schema_version"]
        == EXPONENTIAL_SMOOTHING_SCHEMA_VERSION
    )
    assert first.diagnostics["status"] in {"ready", "limited"}
    evaluation = _mapping(first.diagnostics["evaluation"])
    models = _mapping_rows(evaluation["models"])
    assert {model["family"] for model in models} == {
        "ses",
        "holt",
        "holt_winters",
        "ets",
    }
    assert {model["specification_id"] for model in models} == {
        "ses",
        "holt-add",
        "holt-damped",
        "hw-add",
        "hw-mul",
        "ets-aaa",
    }
    assert all(_mapping(model)["automatic_winner"] is False for model in models)
    fold_results = [
        row
        for model in models
        for row in _mapping_rows(_mapping(model)["fold_results"])
    ]
    assert fold_results
    assert all(
        any(
            row["status"] == "evaluated"
            for row in _mapping_rows(model["fold_results"])
        )
        for model in models
    )
    assert all(row["future_values_visible"] is False for row in fold_results)
    assert all(
        row["target_bin_end_utc_ms"] > row["origin_bin_end_utc_ms"]
        for row in fold_results
    )
    assert all(
        row["target_bin_end_utc_ms"] - row["origin_bin_end_utc_ms"]
        == row["horizon"] * _input_profile().frequency_ms
        for row in fold_results
    )
    assert all(row["original_scale"] is True for row in fold_results)
    assert int(evaluation["evaluated_fold_count"]) > 0
    assert evaluation["automatic_winner"] is False
    assert {
        row["model"] for row in _mapping_rows(evaluation["reference_baselines"])
    } == {
        "naive_random_walk",
        "rolling_mean",
        "rolling_median",
        "session_seasonal_naive",
    }
    assert first.diagnostics["fit_duration_included"] is False


def test_ses_constant_series_forecasts_on_original_scale() -> None:
    """The simplest family member should preserve a constant level."""
    raw = _raw_frame(160).with_columns(
        pl.lit(1.25).alias("bid"), pl.lit(1.2502).alias("ask")
    )
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )
    model = _mapping_rows(_mapping(result.diagnostics["evaluation"])["models"])[
        0
    ]
    evaluated = [
        row
        for row in _mapping_rows(_mapping(model)["fold_results"])
        if row["status"] == "evaluated"
    ]

    assert evaluated
    assert all(row["forecast"] == pytest.approx(1.2501) for row in evaluated)


def test_multiplicative_models_require_positive_training_values() -> None:
    """Multiplicative components must fail safely on non-positive values."""
    raw = _raw_frame(160).with_columns(
        (1.0 + (pl.int_range(0, pl.len()) % 10).cast(pl.Float64) * 0.01).alias(
            "bid"
        ),
        (
            1.0002 + (pl.int_range(0, pl.len()) % 10).cast(pl.Float64) * 0.01
        ).alias("ask"),
    )
    profile = ExponentialSmoothingProfile(
        enabled=True,
        specifications=(
            ExponentialSmoothingSpecification(
                specification_id="mul",
                family="holt_winters",
                trend="mul",
                seasonal="mul",
                seasonal_periods=4,
            ),
        ),
        projection_specification_id="mul",
    )
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(
            transform="return", minimum_training_observations=10
        ),
        profile=profile,
    )

    fit = _mapping(result.diagnostics["fit_summary"])
    assert _mapping(fit["reason_counts"])["invalid_multiplicative_domain"] > 0
    assert result.diagnostics["status"] == "limited"


def test_missing_bins_reset_training_segment_without_forward_fill() -> None:
    """A grid gap should bound the fit segment instead of being filled."""
    raw = _raw_frame(200)
    raw = pl.concat([raw.head(100), raw.tail(80)], how="vertical")
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(step_size=1),
        profile=_ses_profile(),
    )

    assert result.diagnostics["forward_fill_policy"] == "never"
    assert result.diagnostics["missing_observation_policy"] == (
        "reset_to_trailing_contiguous_segment"
    )
    samples = _mapping_rows(
        _mapping(result.diagnostics["fit_summary"])["fit_samples"]
    )
    assert samples
    assert all(
        sample["training_segment_policy"] == "trailing_contiguous_after_missing"
        for sample in samples
    )


def test_expected_closures_remain_distinct_from_unexpected_missing_bins() -> (
    None
):
    """The fitted-family payload must preserve #421 closure semantics."""
    raw = _raw_at(
        (
            _utc_ms(2022, 1, 7, 21),
            _utc_ms(2022, 1, 10, 12),
        )
    )
    profile = _input_profile(
        frequency_ms=6 * 60 * 60 * 1000,
        minimum_training_observations=1,
        minimum_evaluation_observations=1,
        step_size=1,
        horizons=(1,),
    )
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=profile,
        profile=_ses_profile(),
    )
    policy = _mapping(result.diagnostics["input_missingness_policy"])

    assert int(policy["expected_closure_count"]) > 0
    assert int(policy["unexpected_missing_count"]) > 0
    assert policy["expected_closure_grid_rows_retained"] is True
    assert result.diagnostics["forward_fill_policy"] == "never"


def test_explicit_transform_and_differencing_are_inverted_for_metrics() -> None:
    """Configured transformations should remain explicit and invertible."""
    result = exponential_smoothing_from_training_frame(
        _raw_frame(240),
        _fingerprint(),
        input_profile=_input_profile(
            transform="log_return",
            differencing_order=1,
            minimum_training_observations=20,
        ),
        profile=_ses_profile(),
    )
    policy = _mapping(result.diagnostics["input_transform_policy"])
    evaluation = _mapping(result.diagnostics["evaluation"])

    assert policy["transform"] == "log_return"
    assert policy["differencing_order"] == 1
    assert int(policy["warmup_loss"]) > 0
    assert policy["original_scale_metrics_required"] is True
    assert int(evaluation["evaluated_fold_count"]) > 0
    assert evaluation["original_scale"] is True


def test_insufficient_seasonal_cycles_are_bounded_advisory_failures() -> None:
    """Seasonal initialization should not fit without two complete cycles."""
    profile = ExponentialSmoothingProfile(
        enabled=True,
        specifications=(
            ExponentialSmoothingSpecification(
                specification_id="seasonal",
                family="holt_winters",
                trend="add",
                seasonal="add",
                seasonal_periods=20,
            ),
        ),
        projection_specification_id="seasonal",
    )
    result = exponential_smoothing_from_training_frame(
        _raw_frame(120),
        _fingerprint(),
        input_profile=_input_profile(minimum_training_observations=10),
        profile=profile,
    )

    reasons = _mapping(
        _mapping(result.diagnostics["fit_summary"])["reason_counts"]
    )
    assert reasons["insufficient_seasonal_cycles"] > 0
    assert result.diagnostics["hard_fail_quality_gate"] is False


def test_dependency_absence_keeps_contract_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Core users should receive a stable unavailable payload without Statsmodels."""
    monkeypatch.setattr(ets_module, "_load_backend", lambda: None)
    result = exponential_smoothing_from_training_frame(
        _raw_frame(160),
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )

    assert result.diagnostics["status"] == "unavailable"
    assert result.diagnostics["reason"] == "dependency_unavailable"
    assert _mapping(result.diagnostics["backend"])["available"] is False


def test_memory_and_fit_attempt_limits_are_enforced() -> None:
    """Large-period work must stop predictably at shared #421 limits."""
    input_profile = _input_profile(
        resources=ClassicalModelResourcePolicy(
            max_source_rows=10_000,
            max_regularized_observations=10_000,
            max_folds=64,
            max_horizons=4,
            max_candidate_orders=16,
            max_fit_attempts=1,
            max_wall_time_seconds=30,
            max_memory_bytes=1,
            max_retained_diagnostics=64,
        )
    )
    result = exponential_smoothing_from_training_frame(
        _raw_frame(240),
        _fingerprint(),
        input_profile=input_profile,
        profile=_family_profile(),
    )
    resources = _mapping(result.diagnostics["resource_usage"])

    assert result.diagnostics["status"] == "limited"
    assert result.diagnostics["reason"] == "resource_limit"
    assert resources["memory_limit_exceeded"] is True
    assert resources["fit_attempt_count"] == 0


def test_convergence_warning_is_limited_but_forecast_remains_advisory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A usable non-converged fit should remain visible with a stable warning."""

    class FakeFit:
        params = {"smoothing_level": 0.5}
        mle_retvals = {"success": False}

        @staticmethod
        def forecast(horizon: int) -> list[float]:
            return [1.5] * horizon

    def fake_fit(*args: object, **kwargs: object) -> FakeFit:
        del args, kwargs
        warnings.warn("did not converge", ConvergenceWarning, stacklevel=2)
        return FakeFit()

    monkeypatch.setattr(ets_module, "_statsmodels_fit", fake_fit)
    result = exponential_smoothing_from_training_frame(
        _raw_frame(160),
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )
    fit = _mapping(result.diagnostics["fit_summary"])

    assert _mapping(fit["status_counts"])["limited"] > 0
    assert _mapping(fit["warning_counts"])["convergence_warning"] > 0


def test_projection_uses_row_identity_and_separates_forecast_from_diagnostic() -> (
    None
):
    """Forecast and realized error must appear only at their availability rows."""
    raw = _raw_frame(200, duplicate_timestamp=True)
    observed = raw.select("datetime", "bid", "ask").to_dicts()
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )
    projected = project_exponential_smoothing_onto_training_frame(raw, result)

    assert projected.select("datetime", "bid", "ask").to_dicts() == observed
    assert projected.get_column("row_id").n_unique() == raw.height
    forecast_rows = projected.filter(pl.col("cm_ets_forecast_available"))
    diagnostic_rows = projected.filter(pl.col("cm_ets_diagnostic_available"))
    assert forecast_rows.height > 0
    assert diagnostic_rows.height > 0
    assert (
        forecast_rows.get_column("cm_ets_actual").null_count()
        == forecast_rows.height
    )
    assert forecast_rows.get_column("cm_ets_training_eligible").all()
    assert diagnostic_rows.get_column("cm_ets_diagnostic_only").all()
    assert (
        forecast_rows.get_column("timestamp_utc_ms")
        >= forecast_rows.get_column("cm_ets_forecast_available_at_utc_ms")
    ).all()
    line = format_influx_line(
        "eurusd",
        "ascii",
        TICK,
        forecast_rows.row(0),
        columns=forecast_rows.columns,
    )
    assert "cm_ets_forecast_available=true" in line
    assert "bidquote=" in line


def test_projection_survives_masked_or_dropped_timestamps() -> None:
    """Build-time availability should project later through durable row IDs."""
    raw = _raw_frame(200)
    enriched = ensure_tick_training_features(
        raw,
        symbol="EURUSD",
        data_format="ascii",
        timeframe=TICK,
        period="201202",
    )
    result = exponential_smoothing_from_training_frame(
        enriched,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )
    masked = enriched.with_columns(
        pl.when(pl.col("row_id") == 7)
        .then(None)
        .otherwise(pl.col("timestamp_utc_ms"))
        .alias("timestamp_utc_ms")
    )
    projected = project_exponential_smoothing_onto_training_frame(
        masked, result
    )
    dropped = project_exponential_smoothing_onto_training_frame(
        enriched.drop("timestamp_utc_ms", "datetime"), result
    )

    assert projected.get_column("row_id").to_list() == list(range(1, 201))
    assert dropped.get_column("row_id").to_list() == list(range(1, 201))
    assert dropped.get_column("cm_ets_forecast_available").any()


def test_projection_round_trips_through_the_polars_cache(
    tmp_path: Path,
) -> None:
    """All registered ETS scalars should remain Arrow IPC serializable."""
    raw = _raw_frame(200)
    result = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )
    projected = project_exponential_smoothing_onto_training_frame(raw, result)
    cache = tmp_path / CACHE_FILENAME

    write_polars_cache(projected, cache)
    restored = read_polars_cache(cache)

    assert restored.select(EXPONENTIAL_SMOOTHING_COLUMNS).to_dicts() == (
        projected.select(EXPONENTIAL_SMOOTHING_COLUMNS).to_dicts()
    )


def test_ets_columns_are_registered_flat_nullable_scalars() -> None:
    """Every augmented ETS column should be discoverable and cache-safe."""
    definitions = {
        definition.name: definition
        for definition in training_feature_definitions()
    }
    assert set(EXPONENTIAL_SMOOTHING_COLUMNS) <= set(definitions)
    assert all(
        definitions[name].nullable for name in EXPONENTIAL_SMOOTHING_COLUMNS
    )
    assert all(
        definitions[name].grain == "row"
        for name in EXPONENTIAL_SMOOTHING_COLUMNS
    )
    assert all(
        definitions[name].dtype in {"Utf8", "Int64", "Float64", "Boolean"}
        for name in EXPONENTIAL_SMOOTHING_COLUMNS
    )


def test_exponential_smoothing_profile_rejects_ambiguous_specifications() -> (
    None
):
    """Invalid family/component combinations should fail before fitting."""
    with pytest.raises(ValueError, match="SES cannot"):
        ExponentialSmoothingSpecification(family="ses", trend="add")
    with pytest.raises(ValueError, match="Holt-Winters requires"):
        ExponentialSmoothingSpecification(family="holt_winters")
    with pytest.raises(ValueError, match="add or mul"):
        ExponentialSmoothingSpecification(error="none")


def test_legacy_raw_and_enriched_frames_share_the_annotation_engine() -> None:
    """Legacy caches should enrich in memory without changing ETS semantics."""
    raw = _raw_frame(200)
    enriched = ensure_tick_training_features(
        raw,
        symbol="EURUSD",
        data_format="ascii",
        timeframe=TICK,
        period="201202",
    )
    legacy = exponential_smoothing_from_training_frame(
        raw,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
        target=_target("legacy.data", QualityTargetKind.CACHE),
    )
    current = exponential_smoothing_from_training_frame(
        enriched,
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    )

    legacy_model = _mapping_rows(
        _mapping(legacy.diagnostics["evaluation"])["models"]
    )[0]
    current_model = _mapping_rows(
        _mapping(current.diagnostics["evaluation"])["models"]
    )[0]
    assert (
        _mapping(legacy_model)["horizon_metrics"]
        == _mapping(current_model)["horizon_metrics"]
    )
    assert (
        _mapping(legacy.input_result.contract["source"])[
            "legacy_cache_enriched_on_read"
        ]
        is True
    )


def test_fingerprint_opt_in_works_for_csv_direct_and_fresh_sibling_cache(
    tmp_path: Path,
) -> None:
    """The ordinary rule must expose the family across supported cache paths."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    rows = _tick_lines(240)
    source.write_text("\n".join(rows) + "\n", encoding="ascii")
    cache = tmp_path / CACHE_FILENAME
    write_polars_cache(to_polars_frame(parse_ascii_lines(TICK, rows)), cache)
    csv_mtime_ns = source.stat().st_mtime_ns
    os.utime(cache, ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000))
    profile = HistDataFingerprintProfile(
        classical_model_input=_input_profile(),
        exponential_smoothing=_ses_profile(),
    )
    direct = _ets_from_target(
        _target(str(cache), QualityTargetKind.CACHE), profile
    )
    sibling = _ets_from_target(
        _target(str(source), QualityTargetKind.CSV), profile
    )
    default = _mapping(
        HistDataSeriesFingerprintRule()
        .evaluate(_target(str(source), QualityTargetKind.CSV))[0]
        .metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )

    assert direct["status"] == sibling["status"]
    assert direct["input_transform_policy"] == sibling["input_transform_policy"]
    direct_model = _mapping_rows(_mapping(direct["evaluation"])["models"])[0]
    sibling_model = _mapping_rows(_mapping(sibling["evaluation"])["models"])[0]
    assert (
        _mapping(direct_model)["horizon_metrics"]
        == _mapping(sibling_model)["horizon_metrics"]
    )
    assert "exponential_smoothing" not in default
    assert "exponential_smoothing" in FINGERPRINT_AUDIT_SECTIONS


def test_exponential_smoothing_has_full_bounded_and_console_surfaces() -> None:
    """Users should not need to parse a nested finding for model status."""
    diagnostics = exponential_smoothing_from_training_frame(
        _raw_frame(200),
        _fingerprint(),
        input_profile=_input_profile(),
        profile=_ses_profile(),
    ).diagnostics
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
    console = format_quality_console_summary(report)

    summary = _mapping(
        _mapping(full["metadata"])[EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY]
    )
    assert (
        summary["schema_version"]
        == EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION
    )
    assert EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY in bounded
    assert "Exponential-smoothing models" in console


def test_exponential_smoothing_summary_is_bounded_and_golden() -> None:
    """Run-level fitted-family summaries should be bounded and stable."""
    findings = []
    for symbol in ("AUDUSD", "EURUSD", "GBPUSD"):
        fingerprint = _fingerprint(symbol=symbol)
        diagnostics = exponential_smoothing_from_training_frame(
            _raw_frame(200),
            fingerprint,
            input_profile=_input_profile(),
            profile=_ses_profile(),
        ).diagnostics
        findings.append(
            _finding(_target(f"DAT_ASCII_{symbol}_T_201202.csv"), diagnostics)
        )
    summary = exponential_smoothing_summary(findings, target_limit=1)
    expected = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures"
            / "data_quality_reports"
            / "exponential_smoothing_summary.json"
        ).read_text(encoding="utf-8")
    )

    assert summary == expected


def _family_profile() -> ExponentialSmoothingProfile:
    specifications = (
        ExponentialSmoothingSpecification(
            parameter_bounds=(("smoothing_level", 0.01, 0.99),)
        ),
        ExponentialSmoothingSpecification(
            specification_id="holt-add", family="holt", trend="add"
        ),
        ExponentialSmoothingSpecification(
            specification_id="holt-damped",
            family="holt",
            trend="add",
            damped_trend=True,
        ),
        ExponentialSmoothingSpecification(
            specification_id="hw-add",
            family="holt_winters",
            trend="add",
            seasonal="add",
            seasonal_periods=4,
        ),
        ExponentialSmoothingSpecification(
            specification_id="hw-mul",
            family="holt_winters",
            trend="add",
            seasonal="mul",
            seasonal_periods=4,
        ),
        ExponentialSmoothingSpecification(
            specification_id="ets-aaa",
            family="ets",
            error="add",
            trend="add",
            seasonal="add",
            seasonal_periods=4,
        ),
    )
    return ExponentialSmoothingProfile(
        enabled=True,
        specifications=specifications,
        projection_specification_id="ses",
        projection_horizon=1,
    )


def _ses_profile() -> ExponentialSmoothingProfile:
    return ExponentialSmoothingProfile(enabled=True)


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


def _raw_frame(
    count: int, *, duplicate_timestamp: bool = False
) -> pl.DataFrame:
    timestamps = [1_000 + index * 100 for index in range(count)]
    if duplicate_timestamp and count > 1:
        timestamps[1] = timestamps[0]
    prices = [
        1.0 + index * 0.001 + (index % 20) * 0.0002 for index in range(count)
    ]
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": prices,
            "ask": [value + 0.0002 for value in prices],
            "vol": [0] * count,
        }
    )


def _raw_at(timestamps: tuple[int, ...]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "datetime": list(timestamps),
            "bid": [1.0 + index * 0.01 for index in range(len(timestamps))],
            "ask": [1.0002 + index * 0.01 for index in range(len(timestamps))],
            "vol": [0] * len(timestamps),
        }
    )


def _utc_ms(year: int, month: int, day: int, hour: int) -> int:
    return int(
        datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000
    )


def _fingerprint(*, symbol: str = "EURUSD") -> dict[str, Any]:
    return {
        "fingerprint_id": f"fingerprint-{symbol.lower()}",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": symbol,
            "period": "201202",
            "kind": "cache",
        },
    }


def _target(
    path: str,
    kind: QualityTargetKind = QualityTargetKind.CSV,
) -> QualityTarget:
    name = Path(path).name.upper()
    symbol = next(
        (
            candidate
            for candidate in ("AUDUSD", "EURUSD", "GBPUSD")
            if candidate in name
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
    target: QualityTarget,
    diagnostics: Mapping[str, Any],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id="fingerprint.series",
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "exponential_smoothing": dict(diagnostics)
            }
        },
    )


def _ets_from_target(
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> Mapping[str, Any]:
    finding = HistDataSeriesFingerprintRule(profile=profile).evaluate(target)[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    audit = _mapping(fingerprint["fingerprint_audit"])
    assert "exponential_smoothing" in audit["sections_expected"]
    assert "exponential_smoothing" in audit["sections_emitted"]
    assert _mapping(audit["section_statuses"])["exponential_smoothing"] in {
        "valid",
        "limited",
    }
    return _mapping(fingerprint["exponential_smoothing"])


def _tick_lines(count: int) -> tuple[str, ...]:
    return tuple(
        (
            f"20120201 00{index // 60:02d}{index % 60:02d}000,"
            f"{1.0 + index * 0.001:.6f},"
            f"{1.0002 + index * 0.001:.6f},0"
        )
        for index in range(count)
    )


def _mapping(value: object) -> Mapping[str, Any]:
    assert isinstance(value, Mapping)
    return value


def _mapping_rows(value: object) -> list[Mapping[str, Any]]:
    assert isinstance(value, list)
    assert all(isinstance(row, Mapping) for row in value)
    return cast(list[Mapping[str, Any]], value)
