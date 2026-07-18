"""Tests for structural state-space and leakage-safe Kalman diagnostics."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

import histdatacom.data_quality.state_space as state_module
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
from histdatacom.data_quality.reporting import (
    QualityExitPolicy,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
)
from histdatacom.data_quality.state_space import (
    STATE_SPACE_BOUNDED_PAYLOAD_KEY,
    STATE_SPACE_SCHEMA_VERSION,
    STATE_SPACE_SUMMARY_METADATA_KEY,
    STATE_SPACE_SUMMARY_SCHEMA_VERSION,
    StateSpaceProfile,
    StateSpaceSpecification,
    project_state_space_onto_training_frame,
    state_space_from_training_frame,
    state_space_summary,
)
from histdatacom.data_quality.training_features import (
    KALMAN_COLUMNS,
    STATE_SPACE_COLUMNS,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    format_influx_line,
    read_polars_cache,
    write_polars_cache,
)


def test_full_family_is_explicit_and_filtered_smoothed_boundaries_are_safe() -> (
    None
):
    """Local, trend, and structural models keep state availability explicit."""
    result = _run(_process_frame(420))
    repeated = _run(_process_frame(420))
    diagnostics = _mapping(result.diagnostics)

    assert result.diagnostics == repeated.diagnostics
    assert diagnostics["schema_version"] == STATE_SPACE_SCHEMA_VERSION
    models = _mapping_rows(_mapping(diagnostics["evaluation"])["models"])
    assert {model["family"] for model in models} == {
        "local_level",
        "local_linear_trend",
        "structural",
    }
    assert _mapping(diagnostics["evaluation"])["automatic_winner"] is False
    assert diagnostics["smoothed_state_used_for_forecast"] is False
    assert diagnostics["full_series_smoothing_used"] is False
    folds = [
        row
        for model in models
        for row in _mapping_rows(_mapping(model)["fold_results"])
        if row["status"] == "evaluated"
    ]
    assert folds
    assert all(row["future_values_visible"] is False for row in folds)
    assert all(
        row["smoothed_state_used_for_forecast"] is False for row in folds
    )
    assert all(row["prediction_only_transition_count"] >= 0 for row in folds)

    projected = project_state_space_onto_training_frame(
        _process_frame(420), result, target=_target("projection.csv")
    )
    filtered = projected.filter(pl.col("cm_kalman_filtered_available"))
    smoothed = projected.filter(pl.col("cm_kalman_smoothed_available"))
    assert filtered.height > 0
    assert filtered.get_column("cm_kalman_filtered_training_eligible").all()
    assert smoothed.height > 0
    assert smoothed.get_column("cm_kalman_smoothed_retrospective").all()
    assert smoothed.get_column("cm_kalman_smoothed_diagnostic_only").all()
    assert not smoothed.get_column("cm_kalman_smoothed_training_eligible").any()


def test_configuration_and_profile_parser_keep_components_first_class() -> None:
    """Structural choices, initialization, and limits remain explicit."""
    with pytest.raises(ValueError, match="local-level"):
        StateSpaceSpecification("bad", "local_level", stochastic_trend=True)
    with pytest.raises(ValueError, match="requires stochastic_trend"):
        StateSpaceSpecification("bad", "local_linear_trend")
    with pytest.raises(ValueError, match="explicit component"):
        StateSpaceSpecification(
            "bad",
            "structural",
            stochastic_level=False,
            irregular=False,
        )

    parsed = (
        quality_profile_from_mapping(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "state-space-family",
                "rules": {
                    "fingerprint.series": {
                        "state_space": {
                            "enabled": True,
                            "projection_specification_id": "structural-explicit",
                            "max_state_dimension": 24,
                            "max_prediction_only_gap": 12,
                            "specifications": [
                                {
                                    "specification_id": "structural-explicit",
                                    "family": "structural",
                                    "seasonal_period": 4,
                                    "seasonal_cycle_ms": 240000,
                                    "initialization_method": "exact_diffuse",
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
        .state_space
    )
    assert parsed.enabled is True
    assert parsed.max_state_dimension == 24
    assert parsed.max_prediction_only_gap == 12
    assert parsed.specifications[0].seasonal_period == 4
    assert parsed.specifications[0].initialization_method == "exact_diffuse"


def test_missing_observations_use_prediction_only_transitions_without_fill() -> (
    None
):
    """Expected closures and true missing bins stay regular-grid omissions."""
    timestamps = [1_641_571_200_000] + [
        1_641_816_000_000 + index * 6 * 60 * 60 * 1000 for index in range(10)
    ]
    frame = pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [1.1 + index * 0.001 for index in range(len(timestamps))],
            "ask": [1.1002 + index * 0.001 for index in range(len(timestamps))],
            "vol": [0] * len(timestamps),
        }
    )
    result = _run(
        frame,
        input_profile=_input_profile(
            frequency_ms=6 * 60 * 60 * 1000,
            minimum_training_observations=1,
            step_size=1,
            horizons=(1,),
        ),
        profile=StateSpaceProfile(
            enabled=True,
            specifications=(StateSpaceSpecification("local", "local_level"),),
            projection_specification_id="local",
            max_prediction_only_gap=20,
        ),
    )
    policy = _mapping(result.diagnostics["missing_observation_policy"])
    assert policy["fill_policy"] == "none"
    assert policy["transition_policy"] == "prediction_only"
    assert policy["regular_time_basis"] is True
    input_policy = _mapping(result.diagnostics["input_missingness_policy"])
    assert int(input_policy["expected_closure_count"]) > 0
    assert int(input_policy["unexpected_missing_count"]) > 0
    samples = _mapping_rows(
        _mapping(result.diagnostics["fit_summary"])["fit_samples"]
    )
    assert any(
        int(row["prediction_only_transition_count"]) > 0 for row in samples
    )


def test_dependency_configuration_resource_and_gap_failures_are_bounded(
    monkeypatch: Any,
) -> None:
    """Expected runtime failures are normalized without exception text."""
    mismatch = _run(
        _process_frame(360),
        profile=_profile(seasonal_cycle_ms=120_000),
    )
    reasons = _mapping(
        _mapping(mismatch.diagnostics["fit_summary"])["reason_counts"]
    )
    assert int(reasons["invalid_time_basis"]) > 0

    long_gap_frame = _process_frame(180).vstack(
        _process_frame(180).with_columns(
            (pl.col("datetime") + 60 * 60 * 1000).alias("datetime")
        )
    )
    long_gap = _run(
        long_gap_frame,
        profile=StateSpaceProfile(
            enabled=True,
            specifications=(StateSpaceSpecification("local", "local_level"),),
            projection_specification_id="local",
            max_prediction_only_gap=1,
        ),
    )
    gap_reasons = _mapping(
        _mapping(long_gap.diagnostics["fit_summary"])["reason_counts"]
    )
    assert int(gap_reasons["long_missing_gap"]) > 0

    assert (
        state_module._backend_failure_reason(
            RuntimeError("singular covariance")
        )
        == "singular_covariance"
    )
    assert (
        state_module._backend_failure_reason(RuntimeError("NaN state"))
        == "numerical_instability"
    )

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
        _process_frame(420),
        input_profile=_input_profile(resources=resources),
    )
    assert (
        _mapping(bounded.diagnostics["resource_usage"])["fit_attempt_count"]
        == 1
    )
    assert bounded.diagnostics["status"] == "limited"

    monkeypatch.setattr(state_module, "_load_backend", lambda: None)
    unavailable = _run(_process_frame(360))
    assert unavailable.diagnostics["status"] == "unavailable"
    assert unavailable.diagnostics["reason"] == "dependency_unavailable"
    assert unavailable.diagnostics["backend_exception_text_included"] is False


def test_failed_fit_with_state_metadata_has_no_state_value_payload() -> None:
    """Backend failures retain dimensions without indexing absent state values."""
    specification = StateSpaceSpecification(
        "structural-seasonal",
        "structural",
        seasonal_period=4,
        seasonal_cycle_ms=240_000,
    )
    profile = StateSpaceProfile(
        enabled=True,
        specifications=(specification,),
        projection_specification_id=specification.specification_id,
    )
    outcome = state_module._empty_fit(
        "failed",
        "non_positive_covariance",
        observed_count=40,
        missing_count=0,
        max_gap=0,
        state_dimension=5,
        state_names=("level", "seasonal", "seasonal.L1", "seasonal.L2"),
    )
    fold = {
        "series_id": "ascii:T:EURUSD:histdata.com",
        "period": "201201",
        "fold_id": 1,
        "status": "valid",
        "origin_row_id": 1,
        "target_row_id": 2,
        "origin_bin_end_utc_ms": 60_000,
        "target_bin_end_utc_ms": 120_000,
        "target_index": 0,
        "horizon": 1,
    }

    sample = state_module._fit_sample(
        outcome,
        specification,
        "sha256:failed-fit",
        fold,
        tuple(range(40)),
        40,
        profile,
    )
    evaluation = state_module._fold_evaluation(
        ({"cm_input_observed_value": 1.0},),
        fold,
        specification,
        3,
        "sha256:failed-fit",
        outcome,
        (),
        _input_profile(),
        profile.rounding_digits,
        profile.max_retained_states,
    )

    assert sample["status"] == "failed"
    assert sample["reason"] == "non_positive_covariance"
    assert sample["state_dimension"] == 5
    assert sample["state_names"] == []
    assert sample["states"] == []
    assert sample["states_truncated"] is False
    assert evaluation["status"] == "not_evaluated"
    assert evaluation["state_names"] == []
    assert evaluation["filtered_state"] == []
    assert evaluation["smoothed_state"] == []
    assert evaluation["filtered_state_available_at_origin"] is False
    assert evaluation["smoothed_state_retrospective"] is False
    assert evaluation["states_truncated"] is False


def test_projection_preserves_identity_and_serializes_cache_and_influx(
    tmp_path: Path,
) -> None:
    """The augmented row contract survives duplicates, IPC, and Influx."""
    raw = _process_frame(420, duplicate_timestamp=True)
    target = _target("DAT_ASCII_EURUSD_T_201202.csv")
    result = _run(raw, target=target)
    projected = project_state_space_onto_training_frame(
        raw, result, target=target
    )

    assert (
        projected.select("datetime", "bid", "ask").to_dicts()
        == raw.select("datetime", "bid", "ask").to_dicts()
    )
    assert projected.get_column("row_id").n_unique() == raw.height
    assert set((*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)) <= set(
        projected.columns
    )
    available = projected.filter(pl.col("cm_state_space_forecast_available"))
    assert available.height > 0
    assert available.get_column("cm_state_space_training_eligible").all()
    line = format_influx_line(
        "eurusd", "ascii", TICK, available.row(0), columns=available.columns
    )
    assert "cm_state_space_forecast_available=true" in line

    masked = projected.with_columns(
        pl.when(pl.col("row_id") == 7)
        .then(None)
        .otherwise(pl.col("timestamp_utc_ms"))
        .alias("timestamp_utc_ms")
    )
    masked_projection = project_state_space_onto_training_frame(masked, result)
    dropped_projection = project_state_space_onto_training_frame(
        projected.drop("timestamp_utc_ms"), result
    )
    expected_ids = list(range(1, raw.height + 1))
    assert masked_projection.get_column("row_id").to_list() == expected_ids
    assert dropped_projection.get_column("row_id").to_list() == expected_ids

    cache = tmp_path / CACHE_FILENAME
    write_polars_cache(projected, cache)
    restored = read_polars_cache(cache)
    columns = (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)
    assert (
        restored.select(columns).to_dicts()
        == projected.select(columns).to_dicts()
    )


def test_columns_discovery_reports_and_comparisons_cover_the_full_contract() -> (
    None
):
    """Registry and every report surface expose the same implemented family."""
    definitions = {row.name: row for row in training_feature_definitions()}
    assert len(STATE_SPACE_COLUMNS) == 32
    assert len(KALMAN_COLUMNS) == 20
    assert set((*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)) <= set(definitions)
    assert all(
        definitions[name].nullable
        for name in (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)
    )
    assert "state_space" in FINGERPRINT_AUDIT_SECTIONS
    assert "state_space" in implemented_fingerprint_target_section_names()

    reference = {
        "evaluation": {
            "models": [
                {
                    "status": "ready",
                    "family": "reference",
                    "specification_id": "reference",
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
        seasonal_exogenous=reference,
    )
    references = _mapping(
        _mapping(result.diagnostics["evaluation"])["reference_models"]
    )
    for family in (
        "exponential_smoothing",
        "autoregressive",
        "seasonal_exogenous",
    ):
        row = _mapping_rows(references[family])[0]
        assert row["model_id"] == "sha256:reference"
        assert row["automatic_winner"] is False

    finding = _finding(
        _target("DAT_ASCII_EURUSD_T_201202.csv"), result.diagnostics
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
    assert STATE_SPACE_SUMMARY_METADATA_KEY in _mapping(full["metadata"])
    assert STATE_SPACE_BOUNDED_PAYLOAD_KEY in bounded
    assert "State-space and Kalman models" in format_quality_console_summary(
        report
    )


def test_bounded_summary_matches_golden() -> None:
    """Target sorting, statuses, and truncation remain deterministic."""
    findings = []
    for symbol, status, failed in (
        ("GBPUSD", "limited", 1),
        ("AUDUSD", "ready", 0),
        ("EURUSD", "ready", 0),
    ):
        target = _target(f"DAT_ASCII_{symbol}_T_201201.csv", symbol=symbol)
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
    summary = state_space_summary(findings, target_limit=1)
    expected = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures"
            / "data_quality_reports"
            / "state_space_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert summary == expected
    assert _mapping(cast(Mapping[str, Any], summary))["schema_version"] == (
        STATE_SPACE_SUMMARY_SCHEMA_VERSION
    )


def test_fingerprint_rule_runs_opt_in_state_space_on_supported_csv(
    tmp_path: Path,
) -> None:
    """The ordinary fingerprint lifecycle emits and audits the family."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201201.csv"
    source.write_text("\n".join(_tick_lines(360)) + "\n", encoding="ascii")
    profile = HistDataFingerprintProfile(
        classical_model_input=_input_profile(),
        state_space=_profile(),
    )
    finding = HistDataSeriesFingerprintRule(profile=profile).evaluate(
        _target(str(source))
    )[0]
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    audit = _mapping(fingerprint["fingerprint_audit"])
    assert _mapping(fingerprint["state_space"])["status"] in {
        "ready",
        "limited",
    }
    assert "state_space" in audit["sections_expected"]
    assert "state_space" in audit["sections_emitted"]


def _run(
    frame: pl.DataFrame,
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: StateSpaceProfile | None = None,
    target: QualityTarget | None = None,
    exponential_smoothing: Mapping[str, Any] | None = None,
    autoregressive: Mapping[str, Any] | None = None,
    seasonal_exogenous: Mapping[str, Any] | None = None,
) -> Any:
    return state_space_from_training_frame(
        frame,
        _fingerprint(),
        input_profile=input_profile or _input_profile(),
        profile=profile or _profile(),
        target=target or _target("DAT_ASCII_EURUSD_T_201202.csv"),
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        seasonal_exogenous=seasonal_exogenous,
    )


def _profile(*, seasonal_cycle_ms: int = 240_000) -> StateSpaceProfile:
    return StateSpaceProfile(
        enabled=True,
        specifications=(
            StateSpaceSpecification("local-level", "local_level"),
            StateSpaceSpecification(
                "local-linear-trend",
                "local_linear_trend",
                stochastic_trend=True,
            ),
            StateSpaceSpecification(
                "structural-seasonal",
                "structural",
                seasonal_period=4,
                seasonal_cycle_ms=seasonal_cycle_ms,
            ),
        ),
        projection_specification_id="local-level",
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
    base = 1_325_376_000_000
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


def _target(path: str, *, symbol: str = "EURUSD") -> QualityTarget:
    return QualityTarget(
        path=path,
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
                "state_space": dict(diagnostics)
            }
        },
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [_mapping(row) for row in cast(list[Any], value)]
