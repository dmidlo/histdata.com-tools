"""Tests for classical model input and evaluation contracts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping, cast

import polars as pl
import pytest

from histdatacom.data_quality.classical_model_contracts import (
    CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION,
    CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION,
    CLASSICAL_MODEL_FOLD_SCHEMA_VERSION,
    CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
    CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY,
    CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY,
    ClassicalModelInputProfile,
    ClassicalModelResourcePolicy,
    build_classical_model_input,
    classical_model_dependency_status,
    classical_model_evaluation_result,
    classical_model_fit_result,
    classical_model_input_summary,
    format_classical_model_input_summary_lines,
    project_classical_model_input_onto_training_frame,
)
from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.reporting import (
    QualityExitPolicy,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
)
from histdatacom.data_quality.fingerprints import (
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.training_features import (
    CLASSICAL_MODEL_CONTRACT_COLUMNS,
    ensure_tick_training_features,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    format_influx_line,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)


def test_model_input_regularizes_ticks_deterministically() -> None:
    """UTC bins, aggregation, OHLC, and IDs should be deterministic."""
    profile = _profile()
    first = build_classical_model_input(
        _raw_frame(50), _fingerprint(), profile=profile
    )
    second = build_classical_model_input(
        _raw_frame(50), _fingerprint(), profile=profile
    )

    assert first.contract == second.contract
    assert (
        first.regularized_frame.to_dicts()
        == second.regularized_frame.to_dicts()
    )
    assert (
        first.contract["schema_version"] == CLASSICAL_MODEL_INPUT_SCHEMA_VERSION
    )
    assert first.contract["status"] == "ready"
    assert str(first.contract["derivation_id"]).startswith("sha256:")
    regularization = _mapping(first.contract["regularization"])
    assert regularization["frequency_ms"] == 500
    assert regularization["bin_interval"] == "[start,end)"
    assert regularization["forward_fill_policy"] == "never"
    assert regularization["row_mapping_policy"] == (
        "availability_safe_repetition_after_bin_close"
    )
    assert regularization["regularized_observation_count"] == 10
    row = first.regularized_frame.row(0, named=True)
    assert row["cm_input_observation_count"] == 5
    assert row["cm_input_mid_open"] == pytest.approx(1.0001)
    assert row["cm_input_mid_close"] == pytest.approx(1.0401)
    assert row["cm_input_mid_high"] == pytest.approx(1.0401)
    assert row["cm_input_mid_low"] == pytest.approx(1.0001)


def test_model_input_marks_closures_and_missing_without_fill() -> None:
    """Weekend closures and unexpected gaps must remain different null bins."""
    timestamps = [
        _utc_ms(2022, 1, 7, 21),
        _utc_ms(2022, 1, 10, 12),
    ]
    result = build_classical_model_input(
        _raw_at(timestamps),
        _fingerprint(),
        profile=_profile(
            frequency_ms=6 * 60 * 60 * 1000,
            minimum_training_observations=1,
            minimum_evaluation_observations=1,
            step_size=1,
            horizons=(1,),
        ),
    )

    regularization = _mapping(result.contract["regularization"])
    assert int(regularization["expected_closure_count"]) > 0
    assert int(regularization["unexpected_missing_count"]) > 0
    missing = result.regularized_frame.filter(
        pl.col("cm_input_observation_count") == 0
    )
    assert missing.get_column("cm_input_value").null_count() == missing.height
    assert missing.get_column("cm_input_spread").null_count() == missing.height


def test_closure_omit_policy_preserves_grid_horizons_but_omits_fold_targets() -> (
    None
):
    """Omitting closures must not collapse elapsed regular-grid horizons."""
    timestamps = [
        _utc_ms(2022, 1, 7, 21),
        _utc_ms(2022, 1, 10, 12),
    ]
    common = {
        "frequency_ms": 6 * 60 * 60 * 1000,
        "minimum_training_observations": 1,
        "minimum_evaluation_observations": 1,
        "step_size": 1,
        "horizons": (1, 11),
    }
    marked = build_classical_model_input(
        _raw_at(timestamps),
        _fingerprint(),
        profile=_profile(expected_closure_policy="mark", **common),
    )
    omitted = build_classical_model_input(
        _raw_at(timestamps),
        _fingerprint(),
        profile=_profile(expected_closure_policy="omit", **common),
    )

    assert omitted.regularized_frame.height == marked.regularized_frame.height
    omitted_regularization = _mapping(omitted.contract["regularization"])
    assert omitted_regularization["expected_closure_grid_rows_retained"] is True
    assert (
        omitted_regularization["expected_closure_model_observations_omitted"]
        is True
    )
    assert any(
        fold["status"] == "skipped" and fold["reason"] == "target_unavailable"
        for fold in marked.folds
    )
    assert all(fold["status"] == "valid" for fold in omitted.folds)


def test_model_transforms_and_differences_are_explicit() -> None:
    """Configured transforms should report warm-up and inverse behavior."""
    profile = _profile(
        transform="log_return",
        differencing_order=1,
        seasonal_differencing_order=1,
        seasonal_period=2,
    )
    result = build_classical_model_input(
        _raw_frame(80), _fingerprint(), profile=profile
    )

    policy = _mapping(result.contract["transform_policy"])
    assert policy["transform"] == "log_return"
    assert policy["differencing_order"] == 1
    assert policy["seasonal_differencing_order"] == 1
    assert policy["seasonal_period"] == 2
    assert policy["inverse_transform"] == "exp_and_compound_from_last_level"
    assert policy["applied_explicitly"] is True
    assert int(policy["warmup_loss"]) > 0


def test_model_folds_are_chronological_and_leakage_safe() -> None:
    """Every fold should keep target rows after its training origin."""
    result = build_classical_model_input(
        _raw_frame(80), _fingerprint(), profile=_profile(horizons=(1, 2, 3))
    )

    assert result.folds
    assert all(
        fold["schema_version"] == CLASSICAL_MODEL_FOLD_SCHEMA_VERSION
        for fold in result.folds
    )
    assert all(fold["shuffle"] is False for fold in result.folds)
    assert all(fold["future_values_visible"] is False for fold in result.folds)
    assert all(
        int(fold["target_index"]) > int(fold["training_end_index"])
        for fold in result.folds
    )
    assert all(
        int(fold["target_bin_end_utc_ms"]) > int(fold["origin_bin_end_utc_ms"])
        for fold in result.folds
    )


def test_projection_augments_same_rows_only_after_bin_close() -> None:
    """Completed-bin values should appear no earlier than their close."""
    raw = _raw_frame(50, duplicate_timestamp=True)
    observed = raw.select("datetime", "bid", "ask").to_dicts()
    result = build_classical_model_input(
        raw, _fingerprint(), profile=_profile()
    )
    projected = project_classical_model_input_onto_training_frame(raw, result)

    assert projected.select("datetime", "bid", "ask").to_dicts() == observed
    assert projected.get_column("row_id").n_unique() == 50
    assert projected.row(0, named=True)["cm_input_value"] is None
    available = projected.filter(pl.col("cm_input_available"))
    assert available.height > 0
    assert (
        available.get_column("timestamp_utc_ms")
        >= available.get_column("cm_input_available_at_utc_ms")
    ).all()
    lines = [
        format_influx_line(
            "eurusd", "ascii", TICK, row, columns=projected.columns
        )
        for row in projected.filter(pl.col("cm_input_available"))
        .head(2)
        .iter_rows()
    ]
    assert all("cm_input_status_code=3i" in line for line in lines)
    assert all("cm_input_available=true" in line for line in lines)
    assert all("bidquote=" in line for line in lines)


def test_projection_preserves_identity_when_timestamp_is_masked_or_dropped() -> (
    None
):
    """Timestamp is a projection feature, never the durable row key."""
    enriched = ensure_tick_training_features(
        _raw_frame(50),
        symbol="EURUSD",
        data_format="ascii",
        timeframe=TICK,
        period="201202",
    )
    result = build_classical_model_input(
        enriched, _fingerprint(), profile=_profile()
    )
    masked = enriched.with_columns(
        pl.when(pl.col("row_id") == 7)
        .then(None)
        .otherwise(pl.col("timestamp_utc_ms"))
        .alias("timestamp_utc_ms")
    )
    projected = project_classical_model_input_onto_training_frame(
        masked, result
    )
    dropped = project_classical_model_input_onto_training_frame(
        enriched.drop("timestamp_utc_ms", "datetime"), result
    )

    assert projected.get_column("row_id").to_list() == list(range(1, 51))
    assert (
        projected.filter(pl.col("row_id") == 7).item(0, "cm_input_value")
        is None
    )
    assert dropped.get_column("row_id").to_list() == list(range(1, 51))
    assert dropped.get_column("cm_input_value").null_count() == 50


def test_resource_limits_are_explicit_and_deterministic() -> None:
    """Source and grid bounds should limit rather than silently consume all rows."""
    profile = _profile(
        resources=ClassicalModelResourcePolicy(
            max_source_rows=30,
            max_regularized_observations=5,
            max_folds=2,
            max_horizons=2,
            max_candidate_orders=2,
            max_fit_attempts=2,
            max_wall_time_seconds=1,
            max_memory_bytes=1024,
            max_retained_diagnostics=2,
        )
    )
    result = build_classical_model_input(
        _raw_frame(100), _fingerprint(), profile=profile
    )

    assert result.contract["status"] == "limited"
    assert result.contract["reason"] == "source_row_limit"
    assert _mapping(result.contract["source"])["source_rows_truncated"] is True
    assert _mapping(result.contract["regularization"])["truncated"] is True
    assert result.regularized_frame.height == 5
    assert len(result.folds) <= 2


def test_legacy_raw_input_is_enriched_and_insufficient_data_is_advisory() -> (
    None
):
    """Raw caches should enrich on read and short data should not hard fail."""
    result = build_classical_model_input(
        _raw_frame(8), _fingerprint(), profile=_profile()
    )

    assert result.contract["status"] == "limited"
    assert result.contract["reason"] == "insufficient_regularized_observations"
    assert (
        _mapping(result.contract["source"])["legacy_cache_enriched_on_read"]
        is True
    )
    assert result.contract["hard_fail_quality_gate"] is False
    projected = project_classical_model_input_onto_training_frame(
        _raw_frame(8), result
    )
    assert not projected.get_column("cm_input_ready").any()
    assert _mapping(result.contract["training_projection"])["ready"] is False


def test_fit_and_evaluation_result_contracts_are_bounded() -> None:
    """Future families should share stable fit/evaluation status contracts."""
    fit = classical_model_fit_result(
        model_id="ets-1",
        family="ets",
        status="dependency_unavailable",
        reason="dependency_unavailable",
        warning_codes=("z", "a", "a"),
    )
    evaluation = classical_model_evaluation_result(
        model_id="ets-1",
        status="contract_ready",
        fold_count=4,
        horizon_count=2,
        metric_scale="original_mid",
    )

    assert fit["schema_version"] == CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION
    assert fit["warning_codes"] == ["a", "z"]
    assert fit["backend_exception_text_included"] is False
    assert (
        evaluation["schema_version"]
        == CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION
    )
    assert evaluation["automatic_winner"] is False
    assert evaluation["full_forecasts_included"] is False
    with pytest.raises(ValueError, match="unsupported fit status"):
        classical_model_fit_result(model_id="x", family="x", status="bad")


def test_optional_model_dependencies_are_not_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The contract should work when rich numerical providers are absent."""
    monkeypatch.setattr(
        "histdatacom.data_quality.classical_model_contracts.importlib.util.find_spec",
        lambda _name: None,
    )

    status = classical_model_dependency_status(probe=True)

    assert status["core_dependency_added"] is False
    assert status["future_install_extra"] == "models"
    assert status["contract_available_without_optional_dependencies"] is True
    assert status["rich_model_fitting_available"] is False
    assert status["availability_basis"] == "runtime_probe"
    assert all(
        dependency["available"] is False
        for dependency in cast(list[dict[str, Any]], status["dependencies"])
    )


def test_model_contract_columns_are_registered_flat_scalars() -> None:
    """All Phase II foundation columns should belong to the row registry."""
    definitions = {
        definition.name: definition
        for definition in training_feature_definitions()
    }

    assert set(CLASSICAL_MODEL_CONTRACT_COLUMNS).issubset(definitions)
    assert all(
        definitions[name].grain == "row"
        for name in CLASSICAL_MODEL_CONTRACT_COLUMNS
    )
    assert all(
        definitions[name].nullable for name in CLASSICAL_MODEL_CONTRACT_COLUMNS
    )
    assert all(
        definitions[name].source == "classical_model_contracts"
        for name in CLASSICAL_MODEL_CONTRACT_COLUMNS
    )


def test_model_input_summary_is_bounded_and_human_readable() -> None:
    """Contract status should have ordinary bounded report surfaces."""
    findings = []
    for symbol in ("AUDUSD", "EURUSD"):
        result = build_classical_model_input(
            _raw_frame(50), _fingerprint(symbol), profile=_profile()
        )
        findings.append(_finding(symbol, result.contract))

    summary = classical_model_input_summary(findings, target_limit=1)
    lines = format_classical_model_input_summary_lines(summary)

    assert summary is not None
    assert summary["target_count"] == 2
    assert summary["included_target_count"] == 1
    assert summary["truncated"] is True
    assert lines[1] == "Classical model input contracts"
    assert "targets: 2" in lines[2]


def test_model_input_has_full_bounded_and_console_report_surfaces() -> None:
    """The ordinary quality report path should expose the model input contract."""
    result = build_classical_model_input(
        _raw_frame(50), _fingerprint(), profile=_profile()
    )
    finding = _finding("EURUSD", result.contract)
    report = QualityReport(
        targets=(finding.target,),
        rule_results=(
            QualityRuleResult(
                rule_id="fingerprint.series",
                target=finding.target,
                findings=(finding,),
            ),
        ),
    )

    full = quality_report_payload(report)
    bounded = bounded_quality_payload(
        report=report,
        operation="quality",
        check_groups=("fingerprint",),
        discovery={"targets": []},
        artifact=None,
        decision=QualityExitPolicy().evaluate(report.summary()),
    )
    console = format_quality_console_summary(report)

    assert CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY in full["metadata"]
    assert CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY in bounded
    assert "Classical model input contracts" in console


def test_fingerprint_profile_opt_in_emits_model_input_and_audit(
    tmp_path: Path,
) -> None:
    """The ordinary fingerprint rule should own opt-in contract emission."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    source.write_text("\n".join(_tick_lines(50)) + "\n", encoding="ascii")
    target = QualityTarget(
        path=str(source),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    finding = HistDataSeriesFingerprintRule(
        profile=HistDataFingerprintProfile(classical_model_input=_profile())
    ).evaluate(target)[0]
    fingerprint = _mapping(finding.metadata["time_series_fingerprint"])
    model_input = _mapping(fingerprint["classical_model_input"])
    audit = _mapping(fingerprint["fingerprint_audit"])
    statuses = _mapping(audit["section_statuses"])

    assert model_input["schema_version"] == CLASSICAL_MODEL_INPUT_SCHEMA_VERSION
    assert model_input["status"] == "ready"
    assert str(model_input["reference_fingerprint_id"]).startswith("sha256:")
    assert model_input["reference_fingerprint_basis"] == (
        "canonical_pre_contract_snapshot"
    )
    assert str(model_input["derivation_id"]).startswith("sha256:")
    assert statuses["classical_model_input"] == "valid"
    assert "classical_model_input" in cast(list[str], audit["sections_emitted"])


def test_model_input_is_available_from_direct_and_fresh_sibling_caches(
    tmp_path: Path,
) -> None:
    """Canonical cache paths must feed the same model-input annotation engine."""
    source = tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"
    rows = _tick_lines(50)
    source.write_text("\n".join(rows) + "\n", encoding="ascii")
    cache = tmp_path / CACHE_FILENAME
    write_polars_cache(to_polars_frame(parse_ascii_lines(TICK, rows)), cache)
    csv_mtime_ns = source.stat().st_mtime_ns
    os.utime(
        cache,
        ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000),
    )
    profile = HistDataFingerprintProfile(classical_model_input=_profile())
    direct_target = QualityTarget(
        path=str(cache),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    sibling_target = QualityTarget(
        path=str(source),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )

    direct = _model_input_from_target(direct_target, profile)
    sibling = _model_input_from_target(sibling_target, profile)

    assert direct["status"] == sibling["status"] == "ready"
    assert direct["regularization"] == sibling["regularization"]
    assert direct["transform_policy"] == sibling["transform_policy"]
    assert _mapping(direct["source"])["usable_row_count"] == 50
    assert _mapping(sibling["source"])["usable_row_count"] == 50


def test_classical_model_input_golden_fixture() -> None:
    """The representative model-input contract should not drift silently."""
    contract = build_classical_model_input(
        _raw_frame(50), _fingerprint(), profile=_profile()
    ).contract
    expected = (
        json.dumps(
            contract,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
        )
        + "\n"
    )
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "data_quality_reports"
        / "classical_model_input.json"
    )
    if os.environ.get("UPDATE_GOLDEN_FIXTURES") == "1":
        fixture.write_text(expected, encoding="utf-8")
    assert fixture.read_text(encoding="utf-8") == expected


def _profile(**overrides: Any) -> ClassicalModelInputProfile:
    values: dict[str, Any] = {
        "enabled": True,
        "frequency_ms": 500,
        "minimum_training_observations": 4,
        "minimum_evaluation_observations": 2,
        "step_size": 1,
        "horizons": (1, 2),
        "resources": ClassicalModelResourcePolicy(
            max_source_rows=1_000,
            max_regularized_observations=1_000,
            max_folds=64,
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
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [1.0 + index * 0.01 for index in range(count)],
            "ask": [1.0002 + index * 0.01 for index in range(count)],
            "vol": [0] * count,
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )


def _raw_at(timestamps: list[int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [1.0 + index * 0.01 for index in range(len(timestamps))],
            "ask": [1.0002 + index * 0.01 for index in range(len(timestamps))],
            "vol": [0] * len(timestamps),
        }
    )


def _utc_ms(year: int, month: int, day: int, hour: int) -> int:
    return int(
        datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000
    )


def _tick_lines(count: int) -> tuple[str, ...]:
    rows = []
    for index in range(count):
        total_ms = index * 100
        second, millisecond = divmod(total_ms, 1_000)
        rows.append(
            f"20120201 0000{second:02d}{millisecond:03d},"
            f"{1.0 + index * 0.01:.6f},"
            f"{1.0002 + index * 0.01:.6f},0"
        )
    return tuple(rows)


def _fingerprint(symbol: str = "EURUSD") -> dict[str, Any]:
    return {
        "fingerprint_id": f"fingerprint-{symbol.lower()}",
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": symbol,
            "period": "201202",
            "kind": "cache",
        },
        "stationarity_diagnostics": {
            "stationarity_status": "ok",
            "recommended_transforms": [],
        },
        "decomposition": {"status": "ok"},
    }


def _finding(symbol: str, contract: Mapping[str, Any]) -> QualityFinding:
    target = QualityTarget(
        path=f"/tmp/DAT_ASCII_{symbol}_T_201202.csv",
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol=symbol,
        period="201202",
    )
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id="fingerprint.series",
        target=target,
        metadata={
            "time_series_fingerprint": {"classical_model_input": dict(contract)}
        },
    )


def _model_input_from_target(
    target: QualityTarget, profile: HistDataFingerprintProfile
) -> Mapping[str, Any]:
    finding = HistDataSeriesFingerprintRule(profile=profile).evaluate(target)[0]
    fingerprint = _mapping(finding.metadata["time_series_fingerprint"])
    return _mapping(fingerprint["classical_model_input"])


def _mapping(value: object) -> Mapping[str, Any]:
    assert isinstance(value, Mapping)
    return value
