"""Tests for family-neutral classical-model comparisons and projections."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from histdatacom.data_quality import (
    QualityExitPolicy,
    bounded_quality_payload,
    quality_report_payload,
)
from histdatacom.data_quality.classical_model_comparison import (
    CLASSICAL_MODEL_COMPARISON_BOUNDED_PAYLOAD_KEY,
    CLASSICAL_MODEL_COMPARISON_SCHEMA_VERSION,
    CLASSICAL_MODEL_COMPARISON_SUMMARY_METADATA_KEY,
    ClassicalModelComparisonProfile,
    classical_model_comparison_from_saved_results,
    classical_model_comparison_summary,
    format_classical_model_comparison_summary_lines,
    project_classical_model_comparison_onto_training_frame,
)
from histdatacom.data_quality.contracts import (
    QualityTarget,
    QualityTargetKind,
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
)
from histdatacom.data_quality.profiles import (
    QUALITY_PROFILE_SCHEMA_VERSION,
    quality_profile_from_mapping,
)
from histdatacom.data_quality.reporting import format_quality_console_summary
from histdatacom.data_quality.fingerprints import (
    FINGERPRINT_AUDIT_SECTIONS,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.training_features import (
    CLASSICAL_MODEL_COMPARISON_COLUMNS,
    required_training_feature_columns,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import TICK, format_influx_line


def test_comparison_is_deterministic_multihorizon_and_has_no_winner() -> None:
    payload = _mean_family_payload(
        model_metrics={1: (2.0, 3.0), 2: (6.0, 7.0)},
        baseline_metrics={1: (4.0, 5.0), 2: (4.0, 5.0)},
    )

    result = _compare(exponential_smoothing=payload)
    repeated = _compare(exponential_smoothing=payload)

    assert result.diagnostics == repeated.diagnostics
    assert result.diagnostics["schema_version"] == (
        CLASSICAL_MODEL_COMPARISON_SCHEMA_VERSION
    )
    assert result.diagnostics["horizons"] == [1, 2]
    records = _records(result.diagnostics, model_id="ets:ses")
    skills = {
        (record["horizon"], record["metric"]): _mapping(record["skill"])
        for record in records
    }
    assert skills[(1, "mae")]["value"] == 0.5
    assert skills[(2, "mae")]["value"] == -0.5
    assert skills[(2, "mae")]["negative"] is True
    assert all(record["metric_value"] is not None for record in records)
    assert result.diagnostics["selection_policy"] == "none"
    _assert_no_key(result.diagnostics, {"winner", "best_model"})


def test_saved_artifact_identity_mismatch_is_ineligible() -> None:
    baseline_family = _mean_family_payload(models=())
    mismatched_family = _mean_family_payload(
        input_derivation_id="different-input",
        include_baselines=False,
    )

    result = _compare(
        exponential_smoothing=baseline_family,
        autoregressive=mismatched_family,
    )

    [record] = [
        row
        for row in _records(result.diagnostics, model_id="ets:ses")
        if row["metric"] == "mae"
    ]
    assert record["eligible"] is False
    assert "regularization_contract_mismatch" in record["eligibility_reasons"]
    assert _mapping(record["skill"])["status"] == "unavailable"


def test_missing_near_zero_and_truncated_baselines_are_explicit() -> None:
    near_zero = _mean_family_payload(
        baseline_metrics={1: (0.0, 0.0)},
    )
    missing = _mean_family_payload(include_baselines=False)
    truncated = _mean_family_payload(fold_results_truncated=True)

    zero_record = _record(_compare(exponential_smoothing=near_zero), "mae")
    missing_record = _record(_compare(exponential_smoothing=missing), "mae")
    truncated_record = _record(_compare(exponential_smoothing=truncated), "mae")

    assert _mapping(zero_record["skill"])["reason"] == "baseline_near_zero"
    assert (
        "reference_baseline_unavailable"
        in missing_record["eligibility_reasons"]
    )
    assert truncated_record["eligible"] is False
    assert "fold_evidence_truncated" in truncated_record["eligibility_reasons"]


def test_mean_variance_and_volatility_metrics_are_never_interchanged() -> None:
    result = _compare(
        exponential_smoothing=_mean_family_payload(),
        volatility=_volatility_payload(),
    )
    records = _records(result.diagnostics)
    groups = {(row["target_metric"], row["scale"]) for row in records}

    assert ("mid_level", "original_mid") in groups
    assert ("return_mean", "unscaled_return") in groups
    assert ("conditional_variance", "unscaled_return_squared") in groups
    assert (
        "absolute_return_volatility",
        "absolute_unscaled_return",
    ) in groups
    assert all(
        not (
            row["target_metric"] == "conditional_variance"
            and row["reference_baseline"] == "naive_random_walk"
        )
        for row in records
    )


def test_fit_accounting_preserves_failures_and_resource_reasons() -> None:
    payload = _volatility_payload(
        fit_samples=(
            {"status": "converged", "reason": None},
            {"status": "failed", "reason": "singular_covariance"},
            {"status": "timed_out", "reason": "wall_time_timeout"},
            {"status": "unavailable", "reason": "dependency_unavailable"},
        )
    )
    result = _compare(volatility=payload)
    accounting = _mapping(result.diagnostics["fit_accounting"])
    totals = _mapping(accounting["totals"])

    assert totals["attempted"] == 4
    assert totals["converged"] == 1
    assert totals["failed"] == 2
    assert totals["timed_out"] >= 1
    assert totals["resource_limited"] >= 1
    assert totals["numerically_invalid"] >= 1
    assert totals["dependency_unavailable"] >= 1
    assert accounting["failed_models_preserved_in_denominator"] is True
    specifications = cast(
        list[dict[str, Any]],
        accounting["by_specification_horizon_period"],
    )
    garch = next(row for row in specifications if row["family"] == "garch")
    assert _mapping(garch["counts"])["attempted"] == 4


@pytest.mark.parametrize(
    ("errors", "parameter_stability", "expected"),
    (
        ((1.0, 1.0, 1.0), {}, "stable"),
        ((1.0, 1.2, 3.0), {}, "persistent_degradation"),
        (
            (1.0, 1.0, 1.0),
            {"parameters": {"level": {"min": 1.0, "median": 1.0, "max": 2.0}}},
            "structural_shift",
        ),
    ),
)
def test_stability_states_are_distinct(
    errors: tuple[float, ...],
    parameter_stability: Mapping[str, Any],
    expected: str,
) -> None:
    payload = _mean_family_payload(
        fold_errors=errors,
        parameter_stability=parameter_stability,
    )
    record = _record(_compare(exponential_smoothing=payload), "mae")

    assert _mapping(record["stability"])["status"] == expected


def test_projection_preserves_duplicate_rows_cache_scalars_and_influx() -> None:
    frame = _raw_frame(duplicate_timestamp=True)
    observed = frame.select("datetime", "bid", "ask").to_dicts()
    result = _compare(frame=frame, exponential_smoothing=_mean_family_payload())
    projected = project_classical_model_comparison_onto_training_frame(
        frame, result, target=_target()
    )

    assert projected.select("datetime", "bid", "ask").to_dicts() == observed
    assert projected.height == frame.height
    assert projected.get_column("row_id").n_unique() == frame.height
    assert set(CLASSICAL_MODEL_COMPARISON_COLUMNS).issubset(projected.columns)
    assert projected.get_column(
        "cm_comparison_training_eligible"
    ).drop_nulls().to_list() == [False]
    assert not any(name.startswith("winner") for name in projected.columns)
    annotated = projected.filter(pl.col("cm_comparison_diagnostic_available"))
    assert annotated.height == 1
    line = format_influx_line(
        "EURUSD",
        "ascii",
        TICK,
        annotated.row(0),
        columns=projected.columns,
    )
    assert "cm_comparison_eligible=" in line
    assert "cm_skill_value=" in line
    assert "cm_stability_status_code=" in line


def test_profile_registry_bounds_and_publication_safety() -> None:
    parsed = quality_profile_from_mapping(
        {
            "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
            "name": "comparison",
            "rules": {
                "fingerprint.series": {
                    "classical_model_comparison": {
                        "enabled": True,
                        "mean_reference_baseline": "rolling_mean",
                        "max_models": 2,
                        "max_horizons": 1,
                        "max_comparisons": 3,
                    }
                }
            },
        }
    ).fingerprint_profile()
    assert parsed.classical_model_comparison.mean_reference_baseline == (
        "rolling_mean"
    )
    assert parsed.classical_model_comparison.max_comparisons == 3
    with pytest.raises(ValueError, match="max_models"):
        ClassicalModelComparisonProfile(max_models=0)

    result = _compare(
        exponential_smoothing=_mean_family_payload(
            model_metrics={1: (2.0, 3.0), 2: (2.0, 3.0)},
            baseline_metrics={1: (4.0, 5.0), 2: (4.0, 5.0)},
        ),
        profile=ClassicalModelComparisonProfile(
            enabled=True,
            max_models=2,
            max_horizons=1,
            max_comparisons=3,
        ),
    )
    assert (
        _mapping(_mapping(result.diagnostics["bounds"])["horizons"])[
            "included_count"
        ]
        == 1
    )
    text = str(result.diagnostics).lower()
    assert "/users/" not in text
    assert "traceback" not in text
    _assert_no_key(
        result.diagnostics, {"exception", "residuals", "fitted_object"}
    )

    definitions = training_feature_definitions()
    registered = {row.name for row in definitions}
    assert set(CLASSICAL_MODEL_COMPARISON_COLUMNS).issubset(registered)
    assert set(CLASSICAL_MODEL_COMPARISON_COLUMNS).issubset(
        required_training_feature_columns()
    )
    assert len(CLASSICAL_MODEL_COMPARISON_COLUMNS) == 43


def test_fingerprint_rule_emits_opt_in_saved_artifact_comparison(
    tmp_path: Path,
) -> None:
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    source.write_text(
        "\n".join(
            f"20120201 0000{index:02d}000,1.{index:06d},1.{index + 200:06d},0"
            for index in range(20)
        )
        + "\n",
        encoding="ascii",
    )
    target = QualityTarget(
        path=source,
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    finding = HistDataSeriesFingerprintRule(
        profile=HistDataFingerprintProfile(
            classical_model_comparison=ClassicalModelComparisonProfile(
                enabled=True
            )
        )
    ).evaluate(target)[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    comparison = _mapping(fingerprint["classical_model_comparison"])
    audit = _mapping(fingerprint["fingerprint_audit"])

    assert comparison["status"] == "unavailable"
    assert comparison["reason"] == "no_model_results"
    assert (
        _mapping(comparison["source_contracts"])["model_fits_triggered"]
        is False
    )
    assert "classical_model_comparison" in FINGERPRINT_AUDIT_SECTIONS
    assert "classical_model_comparison" in audit["sections_expected"]
    assert "classical_model_comparison" in audit["sections_emitted"]


def test_bounded_summary_and_cli_output_match_goldens() -> None:
    diagnostics = _compare(
        exponential_smoothing=_mean_family_payload()
    ).diagnostics
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id="fingerprint.series",
        target=_target(),
        metadata={
            "time_series_fingerprint": {
                "classical_model_comparison": diagnostics
            }
        },
    )
    summary = classical_model_comparison_summary([finding], target_limit=1)
    fixture_root = (
        Path(__file__).parents[1] / "fixtures" / "data_quality_reports"
    )
    expected = json.loads(
        (fixture_root / "classical_model_comparison_summary.json").read_text(
            encoding="utf-8"
        )
    )
    cli_expected = (
        fixture_root / "classical_model_comparison_cli.golden"
    ).read_text(encoding="utf-8")

    assert summary == expected
    assert (
        "\n".join(format_classical_model_comparison_summary_lines(summary))
        + "\n"
        == cli_expected
    )

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
    surfaces = {
        "full_report_metadata": _mapping(full["metadata"])[
            CLASSICAL_MODEL_COMPARISON_SUMMARY_METADATA_KEY
        ],
        "bounded_payload": bounded[
            CLASSICAL_MODEL_COMPARISON_BOUNDED_PAYLOAD_KEY
        ],
        "cli_contains_summary": "Classical model comparison"
        in format_quality_console_summary(report),
    }
    expected_surfaces = json.loads(
        (fixture_root / "classical_model_comparison_surfaces.json").read_text(
            encoding="utf-8"
        )
    )
    assert surfaces == expected_surfaces


def _compare(
    *,
    frame: pl.DataFrame | None = None,
    exponential_smoothing: Mapping[str, Any] | None = None,
    autoregressive: Mapping[str, Any] | None = None,
    volatility: Mapping[str, Any] | None = None,
    profile: ClassicalModelComparisonProfile | None = None,
) -> Any:
    return classical_model_comparison_from_saved_results(
        frame,
        _fingerprint(),
        model_input=_model_input(),
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        volatility=volatility,
        profile=profile or ClassicalModelComparisonProfile(enabled=True),
        target=_target(),
    )


def _mean_family_payload(
    *,
    input_derivation_id: str = "input-1",
    model_metrics: Mapping[int, tuple[float, float]] | None = None,
    baseline_metrics: Mapping[int, tuple[float, float]] | None = None,
    models: tuple[str, ...] = ("ets:ses",),
    include_baselines: bool = True,
    fold_errors: tuple[float, ...] = (1.0, 1.0, 1.0),
    fold_results_truncated: bool = False,
    parameter_stability: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    model_values = model_metrics or {1: (2.0, 3.0)}
    baseline_values = baseline_metrics or {1: (4.0, 5.0)}
    model_rows = []
    for model_id in models:
        model_rows.append(
            {
                "model_id": model_id,
                "specification_id": "ses",
                "specification_code": 1,
                "family": "ses",
                "status": "ready",
                "fit_status_counts": {"converged": len(fold_errors)},
                "evaluation_status_counts": {"evaluated": len(fold_errors)},
                "reason_counts": {},
                "horizon_metrics": [
                    {
                        "horizon": horizon,
                        "evaluation_count": len(fold_errors),
                        "mae": values[0],
                        "rmse": values[1],
                        "bias": values[0] / 2,
                    }
                    for horizon, values in sorted(model_values.items())
                ],
                "fold_results": [
                    {
                        "fold_id": index + 1,
                        "horizon": 1,
                        "target_row_id": index + 10,
                        "status": "evaluated",
                        "error": error,
                    }
                    for index, error in enumerate(fold_errors)
                ],
                "fold_results_truncated": fold_results_truncated,
                "parameter_stability": dict(parameter_stability or {}),
            }
        )
    baselines = (
        [
            {
                "model": "naive_random_walk",
                "horizon_metrics": [
                    {
                        "horizon": horizon,
                        "evaluation_count": len(fold_errors),
                        "mae": values[0],
                        "rmse": values[1],
                        "bias": values[0] / 2,
                    }
                    for horizon, values in sorted(baseline_values.items())
                ],
            }
        ]
        if include_baselines
        else []
    )
    return {
        "schema_version": "histdatacom.exponential-smoothing.v1",
        "input_derivation_id": input_derivation_id,
        "input_transform_policy": {"transform": "level"},
        "target_axis": _fingerprint()["target_axis"],
        "fit_summary": {
            "fit_attempt_count": len(fold_errors),
            "status_counts": {"converged": len(fold_errors)},
            "reason_counts": {},
            "warning_counts": {},
            "failed_fit_count": 0,
        },
        "evaluation": {
            "models": model_rows,
            "reference_baselines": baselines,
        },
    }


def _volatility_payload(
    *, fit_samples: tuple[Mapping[str, Any], ...] | None = None
) -> dict[str, Any]:
    samples = fit_samples or ({"status": "converged", "reason": None},)
    model = {
        "model_id": "garch:garch-1-1",
        "specification_id": "garch-1-1",
        "specification_code": 1,
        "family": "garch",
        "status": "evaluated",
        "fit_samples": list(samples),
        "horizon_metrics": {
            "1": {
                "mean_metrics": {
                    "count": 3,
                    "mae": 0.2,
                    "rmse": 0.3,
                    "bias": 0.1,
                },
                "variance_metrics": {
                    "count": 3,
                    "mae": 0.4,
                    "rmse": 0.5,
                    "mean_qlike": 0.6,
                },
                "volatility_metrics": {
                    "count": 3,
                    "mae": 0.3,
                    "rmse": 0.4,
                    "bias": 0.1,
                },
            }
        },
    }
    statuses: dict[str, int] = {}
    reasons: dict[str, int] = {}
    for sample in samples:
        status = str(sample.get("status") or "")
        reason = str(sample.get("reason") or "")
        if status:
            statuses[status] = statuses.get(status, 0) + 1
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "schema_version": "histdatacom.volatility.v1",
        "input_derivation_id": "input-1",
        "input_transform_policy": {"transform": "level"},
        "target_axis": _fingerprint()["target_axis"],
        "fit_summary": {
            "fit_attempt_count": len(samples),
            "status_counts": statuses,
            "reason_counts": reasons,
            "warning_counts": {},
            "failed_fit_count": sum(
                value
                for key, value in statuses.items()
                if key in {"failed", "unavailable"}
            ),
        },
        "evaluation": {
            "models": [model],
            "reference_variance_baselines": [
                {
                    "name": "ewma_variance_0.94",
                    "horizon_metrics": {
                        "1": {
                            "count": 3,
                            "mae": 0.8,
                            "rmse": 0.9,
                            "mean_qlike": 1.0,
                        }
                    },
                }
            ],
        },
    }


def _model_input() -> dict[str, Any]:
    return {
        "schema_version": "histdatacom.classical-model-input.v1",
        "derivation_id": "input-1",
        "regularization": {
            "frequency_ms": 100,
            "expected_closure_policy": "retain_missing_bin",
            "unexpected_missing_policy": "retain_missing_bin",
            "empty_bin_value_policy": "null",
            "forward_fill_policy": "never",
        },
        "transform_policy": {"transform": "level"},
        "fold_policy": {
            "kind": "rolling_origin_expanding",
            "fold_count": 1,
            "horizons": [1],
            "minimum_training_observations": 2,
            "rolling_window_observations": None,
            "embargo_observations": 0,
            "fold_samples": [
                {
                    "series_id": "ascii:T:EURUSD:histdata.com",
                    "period": "201202",
                    "fold_id": 1,
                    "horizon": 1,
                    "origin_row_id": 2,
                    "target_row_id": 3,
                    "target_bin_end_utc_ms": 1200,
                }
            ],
        },
    }


def _fingerprint() -> dict[str, Any]:
    return {
        "fingerprint_id": "fingerprint-eurusd",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": "EURUSD",
            "period": "201202",
            "kind": "cache",
        },
        "stationarity_diagnostics": {"status": "valid"},
        "decomposition": {"status": "valid"},
        "calendar_regimes": {"status": "valid"},
    }


def _target() -> QualityTarget:
    return QualityTarget(
        path=Path("DAT_ASCII_EURUSD_T_201202.data"),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )


def _raw_frame(*, duplicate_timestamp: bool = False) -> pl.DataFrame:
    timestamps = [1000, 1100, 1200, 1300]
    if duplicate_timestamp:
        timestamps[1] = timestamps[0]
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [1.0, 1.1, 1.2, 1.3],
            "ask": [1.01, 1.11, 1.21, 1.31],
            "vol": [0, 0, 0, 0],
        }
    )


def _records(
    diagnostics: Mapping[str, Any], *, model_id: str | None = None
) -> list[dict[str, Any]]:
    rows = [
        _mapping(row)
        for row in cast(list[Any], diagnostics["comparison_records"])
    ]
    return [
        row for row in rows if model_id is None or row["model_id"] == model_id
    ]


def _record(result: Any, metric: str) -> dict[str, Any]:
    return next(
        row
        for row in _records(result.diagnostics, model_id="ets:ses")
        if row["metric"] == metric
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _assert_no_key(value: Any, forbidden: set[str]) -> None:
    if isinstance(value, Mapping):
        assert not (set(value) & forbidden)
        for child in value.values():
            _assert_no_key(child, forbidden)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _assert_no_key(child, forbidden)
