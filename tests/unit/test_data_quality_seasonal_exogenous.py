"""Tests for explicit SARIMA, ARIMAX, and SARIMAX diagnostics."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, cast

import polars as pl
import pytest

import histdatacom.data_quality.seasonal_exogenous as seasonal_module
from histdatacom.data_quality.calendar_profiles import default_calendar_profile
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
from histdatacom.data_quality.seasonal_exogenous import (
    SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY,
    SEASONAL_EXOGENOUS_COLUMNS,
    SEASONAL_EXOGENOUS_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY,
    SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION,
    CalendarRegressorProfile,
    SeasonalExogenousProfile,
    SeasonalExogenousSpecification,
    project_seasonal_exogenous_onto_training_frame,
    seasonal_exogenous_from_training_frame,
    seasonal_exogenous_summary,
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
    """All named families should use shared folds and known calendar values."""
    first = _run(_process_frame(420))
    second = _run(_process_frame(420))

    assert first.diagnostics == second.diagnostics
    assert (
        first.diagnostics["schema_version"] == SEASONAL_EXOGENOUS_SCHEMA_VERSION
    )
    evaluation = _mapping(first.diagnostics["evaluation"])
    models = _mapping_rows(evaluation["models"])
    assert {model["family"] for model in models} == {
        "sarima",
        "arimax",
        "sarimax",
    }
    assert {
        tuple(_mapping(model["configuration"])["seasonal_order"])
        for model in models
    } == {(1, 0, 0, 4), (0, 0, 0, 0)}
    fold_rows = [
        row
        for model in models
        for row in _mapping_rows(_mapping(model)["fold_results"])
    ]
    assert fold_rows
    assert all(row["future_values_visible"] is False for row in fold_rows)
    assert all(row["original_scale"] is True for row in fold_rows)
    regressors = _mapping(first.diagnostics["regressors"])
    assert regressors["future_observed_market_values_allowed"] is False
    assert (
        regressors["future_values_derived_without_market_observations"] is True
    )
    assert evaluation["automatic_winner"] is False
    assert evaluation["comparison_semantics"] == "descriptive_shared_folds_only"


def test_configuration_and_profile_parser_keep_each_family_first_class() -> (
    None
):
    """Orders, seasonality, exogenous columns, and controls must stay explicit."""
    with pytest.raises(ValueError, match="SARIMA requires"):
        SeasonalExogenousSpecification("bad", "sarima", 1)
    with pytest.raises(ValueError, match="ARIMAX requires"):
        SeasonalExogenousSpecification("bad", "arimax", 1)
    with pytest.raises(ValueError, match="SARIMAX requires"):
        SeasonalExogenousSpecification(
            "bad", "sarimax", 1, regressor_names=("source_hour_sin",)
        )
    with pytest.raises(ValueError, match="constant"):
        SeasonalExogenousSpecification(
            "bad",
            "sarima",
            1,
            d=1,
            seasonal_p=1,
            seasonal_period=4,
            seasonal_cycle_ms=240_000,
            trend="c",
        )
    with pytest.raises(ValueError, match="unknown calendar regressor"):
        SeasonalExogenousSpecification(
            "bad", "arimax", 1, regressor_names=("future_price",)
        )

    parsed = (
        quality_profile_from_mapping(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "seasonal-family",
                "rules": {
                    "fingerprint.series": {
                        "seasonal_exogenous": {
                            "enabled": True,
                            "projection_specification_ids": [
                                "sarimax-explicit"
                            ],
                            "regressor_profile": {
                                "allow_partial_calendar": False,
                                "max_regressors": 3,
                            },
                            "specifications": [
                                {
                                    "specification_id": "sarimax-explicit",
                                    "family": "sarimax",
                                    "p": 1,
                                    "seasonal_p": 1,
                                    "seasonal_period": 4,
                                    "seasonal_cycle_ms": 240000,
                                    "regressor_names": ["source_hour_sin"],
                                    "optimizer": "powell",
                                    "max_iterations": 17,
                                }
                            ],
                        }
                    }
                },
            }
        )
        .fingerprint_profile()
        .seasonal_exogenous
    )
    assert parsed.enabled is True
    assert parsed.specifications[0].family == "sarimax"
    assert parsed.specifications[0].seasonal_period == 4
    assert parsed.specifications[0].optimizer == "powell"
    assert parsed.regressor_profile.allow_partial_calendar is False


def test_invalid_cycle_rank_guards_and_dependency_absence_are_advisory(
    monkeypatch: Any,
) -> None:
    """Runtime mismatches, collinearity, and missing extras isolate failures."""
    mismatch = _run(
        _process_frame(360),
        profile=_profile(seasonal_cycle_ms=120_000),
    )
    reasons = _mapping(
        _mapping(mismatch.diagnostics["fit_summary"])["reason_counts"]
    )
    assert int(reasons["invalid_seasonality"]) > 0

    collinear = SeasonalExogenousProfile(
        enabled=True,
        specifications=(
            SeasonalExogenousSpecification(
                "arimax-collinear",
                "arimax",
                1,
                regressor_names=("market_open", "session_london"),
            ),
        ),
        projection_specification_ids=("arimax-collinear",),
    )
    limited = _run(_process_frame(360), profile=collinear)
    rank_reasons = _mapping(
        _mapping(limited.diagnostics["fit_summary"])["reason_counts"]
    )
    assert set(rank_reasons) & {"rank_deficient_regressors", "collinearity"}

    monkeypatch.setattr(seasonal_module, "_load_backend", lambda: None)
    unavailable = _run(_process_frame(360))
    assert unavailable.diagnostics["status"] == "unavailable"
    assert unavailable.diagnostics["reason"] == "dependency_unavailable"


def test_partial_calendar_and_resource_limits_are_bounded() -> None:
    """Incomplete event calendars and inherited resource limits are explicit."""
    event_profile = SeasonalExogenousProfile(
        enabled=True,
        specifications=(
            SeasonalExogenousSpecification(
                "arimax-event", "arimax", 1, regressor_names=("event_any",)
            ),
        ),
        projection_specification_ids=("arimax-event",),
        regressor_profile=CalendarRegressorProfile(
            allow_partial_calendar=False,
            require_complete_calendar_for=("event_any",),
        ),
    )
    event = _run(_process_frame(360), profile=event_profile)
    event_reasons = _mapping(
        _mapping(event.diagnostics["fit_summary"])["reason_counts"]
    )
    assert set(event_reasons) & {
        "partial_calendar_unavailable",
        "future_regressor_unavailable",
    }

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
    bounded = _run(
        _process_frame(420), input_profile=_input_profile(resources=resources)
    )
    usage = _mapping(bounded.diagnostics["resource_usage"])
    assert usage["fit_attempt_count"] == 1
    assert _mapping(bounded.diagnostics["evaluation"])["model_count"] == 1
    assert bounded.diagnostics["status"] == "limited"


def test_calendar_regressors_preserve_fixed_est_boundaries_and_tag_vocabulary() -> (
    None
):
    """DST dates must not shift the documented fixed-EST source-clock basis."""
    profile = SeasonalExogenousProfile(
        enabled=True,
        specifications=(
            SeasonalExogenousSpecification(
                "arimax-calendar",
                "arimax",
                1,
                regressor_names=(
                    "source_hour_sin",
                    "tag:major_holiday:new_years_day",
                ),
            ),
        ),
        projection_specification_ids=("arimax-calendar",),
    )
    timestamps = (
        _utc_ms(2012, 1, 1, 6, 30),
        _utc_ms(2022, 3, 13, 6, 30),
        _utc_ms(2022, 3, 13, 7, 30),
    )
    rows = [{"cm_input_bin_start_utc_ms": value} for value in timestamps]
    first, contract = seasonal_module._calendar_regressors(
        rows,
        profile,
        default_calendar_profile(),
        target=_target("calendar.csv"),
    )
    second, _ = seasonal_module._calendar_regressors(
        rows,
        profile,
        default_calendar_profile(),
        target=_target("calendar.csv"),
    )

    assert [row.values for row in first] == [row.values for row in second]
    assert contract["column_order"] == [
        "source_hour_sin",
        "tag:major_holiday:new_years_day",
    ]
    assert first[0].values["tag:major_holiday:new_years_day"] == 1.0
    assert first[1].values["source_hour_sin"] == pytest.approx(
        math.sin(2.0 * math.pi * 1.5 / 24.0)
    )
    assert first[2].values["source_hour_sin"] == pytest.approx(
        math.sin(2.0 * math.pi * 2.5 / 24.0)
    )


def test_expected_closures_and_true_missing_bins_remain_distinct() -> None:
    """The family must inherit #421 closure/missing semantics without filling."""
    frame = pl.DataFrame(
        {
            "datetime": [
                _utc_ms(2022, 1, 7, 21),
                _utc_ms(2022, 1, 10, 12),
            ],
            "bid": [1.1, 1.2],
            "ask": [1.1002, 1.2002],
            "vol": [0, 0],
        }
    )
    profile = SeasonalExogenousProfile(
        enabled=True,
        specifications=(
            SeasonalExogenousSpecification(
                "sarima-closure",
                "sarima",
                0,
                seasonal_p=1,
                seasonal_period=2,
                seasonal_cycle_ms=12 * 60 * 60 * 1000,
            ),
        ),
        projection_specification_ids=("sarima-closure",),
    )
    result = _run(
        frame,
        profile=profile,
        input_profile=_input_profile(
            frequency_ms=6 * 60 * 60 * 1000,
            minimum_training_observations=1,
            step_size=1,
            horizons=(1,),
        ),
    )
    missingness = _mapping(result.diagnostics["input_missingness_policy"])
    assert int(missingness["expected_closure_count"]) > 0
    assert int(missingness["unexpected_missing_count"]) > 0
    assert missingness["forward_fill_policy"] == "never"


def test_projection_is_flat_identity_safe_serializable_and_no_fill(
    tmp_path: Path,
) -> None:
    """Augmented columns survive duplicate/masked timestamps, IPC, and Influx."""
    raw = _process_frame(420, duplicate_timestamp=True)
    target = _target("DAT_ASCII_EURUSD_T_201202.csv")
    result = _run(raw, target=target)
    projected = project_seasonal_exogenous_onto_training_frame(
        raw, result, target=target
    )

    assert (
        projected.select("datetime", "bid", "ask").to_dicts()
        == raw.select("datetime", "bid", "ask").to_dicts()
    )
    assert projected.get_column("row_id").n_unique() == raw.height
    assert set(SEASONAL_EXOGENOUS_COLUMNS) <= set(projected.columns)
    for family in ("sarima", "arimax", "sarimax"):
        available = projected.filter(pl.col(f"cm_{family}_forecast_available"))
        assert available.height > 0
        assert available.get_column(f"cm_{family}_training_eligible").all()
        assert (
            available.get_column(f"cm_{family}_actual").null_count()
            == available.height
        )

    enriched = ensure_tick_training_features(raw, target=target)
    masked = enriched.with_columns(
        pl.when(pl.col("row_id") == 7)
        .then(None)
        .otherwise(pl.col("timestamp_utc_ms"))
        .alias("timestamp_utc_ms")
    )
    masked_projection = project_seasonal_exogenous_onto_training_frame(
        masked, result
    )
    assert masked_projection.get_column("row_id").to_list() == list(
        range(1, raw.height + 1)
    )
    line = format_influx_line(
        "eurusd",
        "ascii",
        TICK,
        projected.filter(pl.col("cm_sarima_forecast_available")).row(0),
        columns=projected.columns,
    )
    assert "cm_sarima_forecast_available=true" in line
    cache = tmp_path / CACHE_FILENAME
    write_polars_cache(projected, cache)
    restored = read_polars_cache(cache)
    assert (
        restored.select(SEASONAL_EXOGENOUS_COLUMNS).to_dicts()
        == projected.select(SEASONAL_EXOGENOUS_COLUMNS).to_dicts()
    )
    assert result.diagnostics["forward_fill_policy"] == "never"


def test_columns_and_report_surfaces_are_complete() -> None:
    """Registry, full JSON, bounded JSON, and CLI expose the same contract."""
    definitions = {row.name: row for row in training_feature_definitions()}
    assert len(SEASONAL_EXOGENOUS_COLUMNS) == 123
    assert set(SEASONAL_EXOGENOUS_COLUMNS) <= set(definitions)
    assert all(
        definitions[name].nullable for name in SEASONAL_EXOGENOUS_COLUMNS
    )
    assert "seasonal_exogenous" in FINGERPRINT_AUDIT_SECTIONS

    diagnostics = _run(_process_frame(360)).diagnostics
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
    assert SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY in _mapping(full["metadata"])
    assert SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY in bounded
    assert "Seasonal and exogenous models" in format_quality_console_summary(
        report
    )


def test_comparison_references_are_descriptive_and_never_select_a_winner() -> (
    None
):
    """Available #422/#423 results should be carried as shared-fold references."""
    reference = {
        "evaluation": {
            "models": [
                {
                    "status": "ready",
                    "family": "reference-family",
                    "specification_id": "reference-spec",
                    "model_id": "sha256:reference",
                    "horizon_metrics": [{"horizon": 1, "mae": 0.1}],
                }
            ]
        }
    }
    result = _run(
        _process_frame(360),
        exponential_smoothing=reference,
        autoregressive=reference,
    )
    evaluation = _mapping(result.diagnostics["evaluation"])
    references = _mapping(evaluation["reference_models"])
    for family in ("exponential_smoothing", "autoregressive"):
        rows = _mapping_rows(references[family])
        assert rows[0]["model_id"] == "sha256:reference"
        assert rows[0]["calculation_basis"] == "shared_regular_grid_folds"
        assert rows[0]["automatic_winner"] is False
    assert evaluation["automatic_winner"] is False


def test_bounded_summary_matches_golden() -> None:
    """Target sorting, status counts, and truncation should remain stable."""
    findings = []
    for symbol, status, failed in (
        ("GBPUSD", "limited", 1),
        ("AUDUSD", "ready", 0),
        ("EURUSD", "ready", 0),
    ):
        target = QualityTarget(
            path=f"DAT_ASCII_{symbol}_T_201201.csv",
            kind=QualityTargetKind.CSV,
            data_format="ascii",
            timeframe=TICK,
            symbol=symbol,
            period="201201",
        )
        findings.append(
            _finding(
                target,
                {
                    "status": status,
                    "reason": "optimizer_failure" if failed else None,
                    "target_axis": {
                        "data_format": "ascii",
                        "timeframe": TICK,
                        "symbol": symbol,
                        "period": "201201",
                        "kind": "csv",
                    },
                    "fit_summary": {
                        "fit_attempt_count": 6,
                        "failed_fit_count": failed,
                    },
                    "evaluation": {
                        "model_count": 3,
                        "evaluated_fold_count": 5 if failed else 6,
                    },
                },
            )
        )
    summary = seasonal_exogenous_summary(findings, target_limit=1)
    expected = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures"
            / "data_quality_reports"
            / "seasonal_exogenous_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert summary == expected
    assert _mapping(summary)["schema_version"] == (
        SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION
    )


def test_fingerprint_rule_runs_the_opt_in_family_on_supported_csv(
    tmp_path: Path,
) -> None:
    """The ordinary fingerprint lifecycle should emit and audit the family."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201201.csv"
    source.write_text("\n".join(_tick_lines(360)) + "\n", encoding="ascii")
    profile = HistDataFingerprintProfile(
        classical_model_input=_input_profile(),
        seasonal_exogenous=_profile(),
    )
    finding = HistDataSeriesFingerprintRule(profile=profile).evaluate(
        _target(str(source))
    )[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    audit = _mapping(fingerprint["fingerprint_audit"])

    assert _mapping(fingerprint["seasonal_exogenous"])["status"] in {
        "ready",
        "limited",
    }
    assert "seasonal_exogenous" in audit["sections_expected"]
    assert "seasonal_exogenous" in audit["sections_emitted"]


def _run(
    frame: pl.DataFrame,
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: SeasonalExogenousProfile | None = None,
    target: QualityTarget | None = None,
    exponential_smoothing: Mapping[str, Any] | None = None,
    autoregressive: Mapping[str, Any] | None = None,
) -> Any:
    return seasonal_exogenous_from_training_frame(
        frame,
        _fingerprint(),
        input_profile=input_profile or _input_profile(),
        profile=profile or _profile(),
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        target=target or _target("DAT_ASCII_EURUSD_T_201202.csv"),
    )


def _profile(*, seasonal_cycle_ms: int = 240_000) -> SeasonalExogenousProfile:
    return SeasonalExogenousProfile(
        enabled=True,
        specifications=(
            SeasonalExogenousSpecification(
                "sarima-1x1",
                "sarima",
                1,
                seasonal_p=1,
                seasonal_period=4,
                seasonal_cycle_ms=seasonal_cycle_ms,
            ),
            SeasonalExogenousSpecification(
                "arimax-hour",
                "arimax",
                1,
                regressor_names=("source_hour_sin",),
            ),
            SeasonalExogenousSpecification(
                "sarimax-1x1-hour",
                "sarimax",
                1,
                seasonal_p=1,
                seasonal_period=4,
                seasonal_cycle_ms=seasonal_cycle_ms,
                regressor_names=("source_hour_sin",),
            ),
        ),
        projection_specification_ids=(
            "sarima-1x1",
            "arimax-hour",
            "sarimax-1x1-hour",
        ),
    )


def _input_profile(**overrides: Any) -> ClassicalModelInputProfile:
    values: dict[str, Any] = {
        "enabled": True,
        "frequency_ms": 60_000,
        "minimum_training_observations": 24,
        "minimum_evaluation_observations": 1,
        "step_size": 16,
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
    count: int, *, duplicate_timestamp: bool = False
) -> pl.DataFrame:
    base = 1_325_376_000_000  # 2012-01-01 UTC
    times = [base + index * 10_000 for index in range(count)]
    if duplicate_timestamp and count > 1:
        times[1] = times[0]
    prices = [
        1.1
        + index * 0.000003
        + ((index // 6) % 4 - 1.5) * 0.00004
        + ((index * 17) % 11 - 5) * 0.000002
        for index in range(count)
    ]
    return pl.DataFrame(
        {
            "datetime": times,
            "bid": prices,
            "ask": [value + 0.0002 for value in prices],
            "vol": [0] * count,
        }
    )


def _utc_ms(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(
        datetime(
            year, month, day, hour, minute, tzinfo=timezone.utc
        ).timestamp()
        * 1000
    )


def _tick_lines(count: int) -> tuple[str, ...]:
    start = datetime(2012, 1, 1)
    return tuple(
        (
            f"{(start + timedelta(seconds=index * 10)).strftime('%Y%m%d %H%M%S')}000,"
            f"{1.1 + index * 0.000003:.6f},"
            f"{1.1002 + index * 0.000003:.6f},0"
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
        "fingerprint_audit": {
            "section_statuses": {
                "dependence": "valid",
                "stationarity_diagnostics": "limited",
            }
        },
    }


def _target(path: str) -> QualityTarget:
    return QualityTarget(
        path=path,
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
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
                "seasonal_exogenous": dict(diagnostics)
            }
        },
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [_mapping(row) for row in cast(list[Any], value)]
