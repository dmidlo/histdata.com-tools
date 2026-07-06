"""Golden compatibility tests for public data-quality report payloads."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from histdatacom.data_quality import (
    QUALITY_NEXT_ACTIONS_METADATA_KEY,
    QUALITY_NEXT_ACTIONS_SCHEMA_VERSION,
    QUALITY_REMEDIATION_COVERAGE_METADATA_KEY,
    QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION,
    QUALITY_REPORT_SCHEMA_VERSION,
    QualityExitPolicy,
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
    bounded_quality_payload,
    quality_report_payload,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

UPDATE_ENV_VAR = "HISTDATACOM_UPDATE_QUALITY_GOLDENS"
GOLDEN_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "data_quality_reports"
)
SEVERITY_VALUES = {item.value for item in QualitySeverity}
STATUS_VALUES = {item.value for item in QualityStatus}
TARGET_KIND_VALUES = {item.value for item in QualityTargetKind}

GOLDEN_CASES: tuple[tuple[str, str, str], ...] = (
    ("clean_csv_report", "report", "_clean_csv_report_payload"),
    ("dirty_csv_report", "report", "_dirty_csv_report_payload"),
    ("corrupt_zip_report", "report", "_corrupt_zip_report_payload"),
    (
        "coverage_manifest_failure_report",
        "report",
        "_coverage_manifest_failure_report_payload",
    ),
    ("cache_target_report", "report", "_cache_target_report_payload"),
    ("fingerprint_report", "report", "_fingerprint_report_payload"),
    (
        "fingerprint_bounded_payload",
        "bounded",
        "_fingerprint_bounded_payload",
    ),
    ("run_scoped_report", "report", "_run_scoped_report_payload"),
    (
        "orchestration_bounded_payload",
        "bounded",
        "_orchestration_bounded_payload",
    ),
)


@pytest.mark.parametrize(
    ("fixture_name", "payload_kind", "payload_factory_name"),
    GOLDEN_CASES,
)
def test_quality_payload_golden_fixture_compatibility(
    fixture_name: str,
    payload_kind: str,
    payload_factory_name: str,
) -> None:
    """Representative report payloads should not drift silently."""
    payload_factory = globals()[payload_factory_name]
    assert callable(payload_factory)
    payload = payload_factory()
    if payload_kind == "bounded":
        _assert_bounded_payload_contract(payload)
    else:
        _assert_report_contract(payload)

    expected_text = _canonical_json(payload)
    fixture_path = GOLDEN_ROOT / f"{fixture_name}.json"
    if _updating_goldens():
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(expected_text, encoding="utf-8")

    if not fixture_path.exists():
        pytest.fail(
            f"missing golden fixture: {fixture_path}. "
            f"Regenerate with {UPDATE_ENV_VAR}=1."
        )
    assert fixture_path.read_text(encoding="utf-8") == expected_text


def test_quality_report_golden_update_workflow_is_documented() -> None:
    """Schema updates should have a documented, intentional fixture path."""
    docs = Path("docs/data-quality/report-compatibility.md").read_text(
        encoding="utf-8"
    )

    assert UPDATE_ENV_VAR in docs
    assert "histdatacom.quality-report.v1" in docs
    assert "schema version" in docs.lower()


def _clean_csv_report_payload() -> dict[str, JSONValue]:
    target = _target(
        path="/quality-fixtures/DAT_ASCII_EURUSD_M1_201202.csv",
        kind=QualityTargetKind.CSV,
        metadata={"filename": "DAT_ASCII_EURUSD_M1_201202.csv"},
    )
    finding = _finding(
        target,
        severity=QualitySeverity.INFO,
        code="ASCII_SCHEMA_SUMMARY",
        message="ASCII M1 schema profile.",
        rule_id="ingestion.ascii.schema",
        metadata={
            "row_count": 3,
            "columns": ["datetime", "open", "high", "low", "close", "vol"],
        },
    )
    return quality_report_payload(
        QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="ingestion.ascii.schema",
                    target=target,
                    findings=(finding,),
                ),
            ),
            metadata={
                "operation": "data-quality",
                "check_groups": ["ingestion"],
            },
        )
    )


def _dirty_csv_report_payload() -> dict[str, JSONValue]:
    target = _target(
        path="/quality-fixtures/DAT_ASCII_EURUSD_M1_201202_DIRTY.csv",
        kind=QualityTargetKind.CSV,
        metadata={"filename": "DAT_ASCII_EURUSD_M1_201202_DIRTY.csv"},
    )
    duplicate = _finding(
        target,
        severity=QualitySeverity.WARNING,
        code="ASCII_TIMESTAMP_DUPLICATE",
        message="Duplicate timestamp found in M1 rows.",
        rule_id="time.ascii.timestamp",
        location=QualityLocation(
            path=target.path,
            row_number=2,
            timestamp_source="20120201 000000",
            timestamp_utc_ms=1328072400000,
            column="datetime",
            metadata={"duplicate_of_row": 1},
        ),
    )
    invalid_ohlc = _finding(
        target,
        severity=QualitySeverity.ERROR,
        code="ASCII_M1_OHLC_INVALID",
        message="M1 OHLC values violate high/low ordering.",
        rule_id="bars.ascii.m1.ohlc",
        location=QualityLocation(
            path=target.path,
            row_number=3,
            timestamp_source="20120201 000100",
            timestamp_utc_ms=1328072460000,
            column="high",
        ),
        metadata={
            "open": 1.30657,
            "high": 1.30647,
            "low": 1.30656,
            "close": 1.30656,
        },
    )
    return quality_report_payload(
        QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="time.ascii.timestamp",
                    target=target,
                    findings=(duplicate,),
                ),
                QualityRuleResult(
                    rule_id="bars.ascii.m1.ohlc",
                    target=target,
                    findings=(invalid_ohlc,),
                ),
            ),
            metadata={
                "operation": "data-quality",
                "check_groups": ["time", "bars"],
            },
        )
    )


def _corrupt_zip_report_payload() -> dict[str, JSONValue]:
    target = _target(
        path="/quality-fixtures/DAT_ASCII_EURUSD_M1_201202.zip",
        kind=QualityTargetKind.ZIP,
        metadata={"filename": "DAT_ASCII_EURUSD_M1_201202.zip"},
    )
    finding = _finding(
        target,
        severity=QualitySeverity.ERROR,
        code="ZIP_CORRUPT",
        message="ZIP archive could not be opened.",
        rule_id="inventory.zip.integrity",
        metadata={
            "error_type": "BadZipFile",
            "error": "File is not a zip file",
        },
    )
    return quality_report_payload(
        QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="inventory.zip.integrity",
                    target=target,
                    findings=(finding,),
                ),
            ),
            metadata={
                "operation": "data-quality",
                "check_groups": ["inventory"],
            },
        )
    )


def _coverage_manifest_failure_report_payload() -> dict[str, JSONValue]:
    target = _target(
        path="/quality-fixtures/data",
        kind=QualityTargetKind.DIRECTORY,
        symbol="",
        period="",
        metadata={"root": "/quality-fixtures/data"},
    )
    missing_dimension = {
        "data_format": "ascii",
        "timeframe": "M1",
        "symbol": "EURUSD",
        "period": "201203",
    }
    finding = _finding(
        target,
        severity=QualitySeverity.ERROR,
        code="COVERAGE_PERIOD_MISSING",
        message="Expected dataset period is missing from local targets.",
        rule_id="inventory.coverage_manifest",
        location=QualityLocation(
            path=target.path,
            metadata={"dimension": missing_dimension},
        ),
        metadata={"dimension": missing_dimension},
    )
    return quality_report_payload(
        QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="inventory.coverage_manifest",
                    target=target,
                    findings=(finding,),
                ),
            ),
            metadata={
                "operation": "data-quality",
                "check_groups": ["inventory"],
                "coverage_manifest": {
                    "schema_version": "histdatacom.coverage-manifest.v1",
                    "expected_source": "metadata",
                    "expected_count": 2,
                    "present_count": 1,
                    "missing_count": 1,
                    "missing": [missing_dimension],
                    "duplicates": [],
                    "unexpected": [],
                },
            },
        )
    )


def _cache_target_report_payload() -> dict[str, JSONValue]:
    target = _target(
        path="/quality-fixtures/data/ASCII/M1/eurusd/2012/02/.data",
        kind=QualityTargetKind.CACHE,
        metadata={
            "filename": ".data",
            "cache_schema": "polars-ipc",
        },
    )
    finding = _finding(
        target,
        severity=QualitySeverity.INFO,
        code="ASCII_CACHE_SCHEMA_SUMMARY",
        message="Canonical Polars cache schema profile.",
        rule_id="ingestion.ascii.cache_schema",
        metadata={
            "row_count": 3,
            "schema": {
                "datetime": "Int64",
                "open": "Float64",
                "high": "Float64",
                "low": "Float64",
                "close": "Float64",
                "vol": "Int64",
            },
        },
    )
    return quality_report_payload(
        QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="ingestion.ascii.cache_schema",
                    target=target,
                    findings=(finding,),
                ),
            ),
            metadata={
                "operation": "data-quality",
                "check_groups": ["ingestion"],
            },
        )
    )


def _fingerprint_report_payload() -> dict[str, JSONValue]:
    return quality_report_payload(_fingerprint_report())


def _fingerprint_bounded_payload() -> dict[str, JSONValue]:
    report = _fingerprint_report()
    artifact = ArtifactRef(
        kind="quality-report",
        path="/quality-fixtures/reports/fingerprint-report.json",
        size_bytes=4096,
        sha256="1" * 64,
        metadata={
            "schema_version": QUALITY_REPORT_SCHEMA_VERSION,
            "status": report.status.value,
            "max_severity": report.max_severity.value,
            "target_count": report.summary().target_count,
            "finding_count": report.summary().finding_count,
            "warning_count": report.summary().warning_count,
            "error_count": report.summary().error_count,
        },
    )
    return bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={
            "roots": ["/quality-fixtures/data/ASCII/M1"],
            "target_count": 1,
            "metadata": {"supported_kinds": ["zip", "csv", "cache"]},
        },
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=artifact,
    )


def _fingerprint_report() -> QualityReport:
    target = _target(
        path="/quality-fixtures/DAT_ASCII_EURUSD_M1_201202.csv",
        kind=QualityTargetKind.CSV,
        metadata={"filename": "DAT_ASCII_EURUSD_M1_201202.csv"},
    )
    fingerprint = _fingerprint_payload(
        {
            "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
            "target_axis": {
                "data_format": "ascii",
                "timeframe": "M1",
                "symbol": "EURUSD",
                "period": "201202",
                "kind": "csv",
            },
            "coverage": {
                "row_count": 3,
                "parsed_row_count": 3,
                "start_timestamp_utc_ms": 1328072400000,
                "end_timestamp_utc_ms": 1328072520000,
                "duration_ms": 120000,
            },
            "m1_bar_distribution": {
                "row_count": 3,
                "sampled_row_count": 3,
                "usable_row_count": 3,
                "invalid_row_count": 0,
                "truncated": False,
                "precision": {
                    "precision_source": "text",
                    "decimal_place_counts": {
                        "6": 12,
                    },
                    "column_decimal_place_counts": {
                        "open": {"6": 3},
                        "high": {"6": 3},
                        "low": {"6": 3},
                        "close": {"6": 3},
                    },
                },
            },
            "temporal_topology": {
                "row_count": 3,
                "parsed_row_count": 3,
                "invalid_timestamp_count": 0,
                "non_monotonic_count": 0,
                "duplicate_timestamp_count": 0,
                "duplicate_timestamp_source_counts": {
                    "m1_duplicate_timestamp": 0,
                    "tick_duplicate_row": 0,
                },
                "m1_duplicate_timestamp_count": 0,
                "tick_duplicate_row_count": 0,
                "min_interval_ms": 60000,
                "median_interval_ms": 60000,
                "interval_count": 2,
                "max_gap_ms": 60000,
                "gap_bucket_counts": {
                    "gt_1d": 0,
                    "gt_1h": 0,
                    "gt_1m": 0,
                    "gt_30m": 0,
                    "gt_5m": 0,
                },
                "suspicious_gap_count": 0,
                "expected_session_closure_count": 0,
                "weekend_activity_count": 0,
                "sampling_basis": "observed_sequence",
                "computed_from": "text_scan",
                "timestamp_projection": "text_scan",
                "cache_source": None,
                "gap_tolerance": {
                    "expected_interval_ms": 60000,
                    "suspicious_gap_ms": 300000,
                    "bucket_thresholds_ms": [
                        60000,
                        300000,
                        1800000,
                        3600000,
                        86400000,
                    ],
                    "session_boundary_grace_ms": 3600000,
                    "dynamic_window_initial_ms": 300000,
                    "dynamic_window_max_ms": 3600000,
                    "dynamic_window_growth_factor": 2.0,
                    "dynamic_window_shrink_factor": 0.5,
                },
            },
            "fingerprint_audit": {
                "schema_version": TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
                "sections_expected": [
                    "coverage",
                    "temporal_topology",
                    "calendar_regimes",
                    "m1_bar_distribution",
                    "return_dynamics",
                ],
                "sections_emitted": [
                    "coverage",
                    "temporal_topology",
                    "m1_bar_distribution",
                ],
                "sections_skipped": {
                    "calendar_regimes": {"reason": "not_emitted"},
                    "return_dynamics": {
                        "reason": "not_emitted",
                        "details": {"timeframe": "M1"},
                    },
                },
                "section_statuses": {
                    "coverage": "valid",
                    "temporal_topology": "valid",
                    "calendar_regimes": "skipped",
                    "m1_bar_distribution": "valid",
                    "return_dynamics": "skipped",
                },
                "target_capability": {
                    "supported": True,
                    "unsupported_reason": None,
                },
                "source_status": {
                    "kind": "csv_text",
                    "readable": True,
                    "reason": None,
                },
                "conditional_distribution_eligibility": {
                    "tick_spread": {
                        "eligible": False,
                        "status": "ineligible",
                        "reason": "unsupported_timeframe",
                    }
                },
                "profile_completeness": {
                    "source": "quality_profile",
                    "calendar_profile_complete": False,
                    "missing_optional_calendar_data": True,
                    "calendar_profile_name": "static-major-holidays",
                    "calendar_profile_source": (
                        "static_month_day_major_holidays"
                    ),
                    "calendar_profile_version": "1",
                    "calendar_profile_static_advisory": True,
                },
                "dynamics_readiness": {
                    "return_dynamics": {
                        "status": "skipped",
                        "reason": "not_emitted",
                    },
                    "microstructure_dynamics": {
                        "status": "skipped",
                        "reason": "unsupported_timeframe",
                    },
                },
            },
            "source": {
                "kind": "csv_text",
                "path": "/quality-fixtures/DAT_ASCII_EURUSD_M1_201202.csv",
            },
        }
    )
    finding = _finding(
        target,
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        location=QualityLocation(
            path=target.path,
            column=TIME_SERIES_FINGERPRINT_METADATA_KEY,
        ),
        metadata={TIME_SERIES_FINGERPRINT_METADATA_KEY: fingerprint},
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=target,
                findings=(finding,),
            ),
        ),
        metadata={
            "operation": "data-quality",
            "check_groups": ["fingerprint"],
        },
    )


def _run_scoped_report_payload() -> dict[str, JSONValue]:
    return quality_report_payload(_run_scoped_report())


def _orchestration_bounded_payload() -> dict[str, JSONValue]:
    report = _run_scoped_report()
    artifact = ArtifactRef(
        kind="quality-report",
        path="/quality-fixtures/reports/run-scoped-report.json",
        size_bytes=4096,
        sha256="0" * 64,
        metadata={
            "schema_version": QUALITY_REPORT_SCHEMA_VERSION,
            "status": report.status.value,
            "max_severity": report.max_severity.value,
            "target_count": report.summary().target_count,
            "finding_count": report.summary().finding_count,
            "warning_count": report.summary().warning_count,
            "error_count": report.summary().error_count,
        },
    )
    return bounded_quality_payload(
        operation="data-quality",
        check_groups=("domain",),
        discovery={
            "roots": ["/quality-fixtures/data/ASCII/M1"],
            "target_count": 3,
            "metadata": {"supported_kinds": ["zip", "csv", "cache"]},
        },
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=artifact,
    )


def _run_scoped_report() -> QualityReport:
    target = _target(
        path="/quality-fixtures/data/ASCII/M1",
        kind=QualityTargetKind.DIRECTORY,
        symbol="",
        period="",
        metadata={"root": "/quality-fixtures/data/ASCII/M1"},
    )
    finding = _finding(
        target,
        severity=QualitySeverity.ERROR,
        code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR",
        message="Triangular FX relationship differs from the direct pair.",
        rule_id="domain.cross_instrument_consistency",
        location=QualityLocation(
            path=target.path,
            metadata={
                "direct_symbol": "AUDCAD",
                "period": "2008",
                "timeframe": "M1",
            },
        ),
        metadata={
            "row_count": 11191,
            "samples": [
                {
                    "denominator_symbol": "CADCHF",
                    "direct_price": 1.0417,
                    "direct_symbol": "AUDCAD",
                    "implied_price": 0.9498519438341771,
                    "numerator_symbol": "AUDCHF",
                    "period": "2008",
                    "relationship": "AUDCHF / CADCHF ~= AUDCAD",
                    "relative_difference": 0.088171312437192,
                    "timeframe": "M1",
                    "timestamp_utc_ms": 1212357720000,
                }
            ],
        },
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="domain.cross_instrument_consistency",
                target=target,
                findings=(finding,),
            ),
        ),
        metadata={
            "operation": "data-quality",
            "check_groups": ["domain"],
        },
    )


def _target(
    *,
    path: str,
    kind: QualityTargetKind,
    data_format: str = "ascii",
    timeframe: str = "M1",
    symbol: str = "EURUSD",
    period: str = "201202",
    metadata: dict[str, JSONValue] | None = None,
) -> QualityTarget:
    return QualityTarget(
        path=path,
        kind=kind,
        data_format=data_format,
        timeframe=timeframe,
        symbol=symbol,
        period=period,
        metadata=metadata or {},
    )


def _finding(
    target: QualityTarget,
    *,
    severity: QualitySeverity,
    code: str,
    message: str,
    rule_id: str,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=metadata or {},
    )


def _assert_report_contract(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "metadata",
        "rule_results",
        "schema_version",
        "summary",
        "target_summaries",
        "targets",
    }
    assert payload["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    metadata = _mapping(payload["metadata"])
    if TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY in metadata:
        _assert_fingerprint_coverage(
            _mapping(metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY])
        )
    if TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY in metadata:
        _assert_fingerprint_distribution(
            _mapping(
                metadata[
                    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
                ]
            )
        )
    if TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY in metadata:
        _assert_fingerprint_distribution_attention(
            _mapping(
                metadata[
                    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
                ]
            )
        )
    if TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY in metadata:
        _assert_fingerprint_regime(
            _mapping(
                metadata[TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY]
            )
        )
    if TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY in metadata:
        _assert_fingerprint_topology(
            _mapping(
                metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY]
            )
        )
    if TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY in metadata:
        _assert_fingerprint_topology_attention(
            _mapping(
                metadata[
                    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY
                ]
            )
        )
    if TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY in metadata:
        _assert_fingerprint_readiness(
            _mapping(
                metadata[TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY]
            )
        )
    if QUALITY_NEXT_ACTIONS_METADATA_KEY in metadata:
        _assert_quality_next_actions(
            _mapping(metadata[QUALITY_NEXT_ACTIONS_METADATA_KEY])
        )
    if QUALITY_REMEDIATION_COVERAGE_METADATA_KEY in metadata:
        _assert_quality_remediation_coverage(
            _mapping(metadata[QUALITY_REMEDIATION_COVERAGE_METADATA_KEY])
        )
    summary = _mapping(payload["summary"])
    _assert_summary(summary)

    targets = _list(payload["targets"])
    target_summaries = _list(payload["target_summaries"])
    rule_results = _list(payload["rule_results"])
    assert len(targets) == summary["target_count"]
    assert len(target_summaries) == summary["target_count"]
    assert rule_results

    for target in targets:
        _assert_target(_mapping(target))
    for target_summary in target_summaries:
        _assert_target_summary(_mapping(target_summary))
    for rule_result in rule_results:
        _assert_rule_result(_mapping(rule_result))


def _assert_bounded_payload_contract(payload: dict[str, JSONValue]) -> None:
    expected_keys = {
        "check_groups",
        "cross_target_summaries",
        "discovery",
        "exit_decision",
        "operation",
        "payload_limits",
        "quality_profile",
        "report_artifact",
        "report_schema_version",
        "summary",
        "target_status_counts",
        "target_summaries",
    }
    optional_keys = {
        "fingerprint_coverage",
        "fingerprint_distribution",
        "fingerprint_distribution_attention",
        "fingerprint_readiness",
        "fingerprint_regime",
        "fingerprint_topology",
        "fingerprint_topology_attention",
        "next_actions",
        "remediation_coverage",
    }
    assert expected_keys <= set(payload)
    assert set(payload) <= expected_keys | optional_keys
    assert payload["operation"] == "data-quality"
    assert payload["report_schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert "rule_results" not in payload
    assert "findings" not in payload
    assert isinstance(payload["quality_profile"], dict)
    _assert_summary(_mapping(payload["summary"]))
    if "fingerprint_coverage" in payload:
        _assert_fingerprint_coverage(_mapping(payload["fingerprint_coverage"]))
    if "fingerprint_distribution" in payload:
        _assert_fingerprint_distribution(
            _mapping(payload["fingerprint_distribution"])
        )
    if "fingerprint_distribution_attention" in payload:
        _assert_fingerprint_distribution_attention(
            _mapping(payload["fingerprint_distribution_attention"])
        )
    if "fingerprint_regime" in payload:
        _assert_fingerprint_regime(_mapping(payload["fingerprint_regime"]))
    if "fingerprint_topology" in payload:
        _assert_fingerprint_topology(_mapping(payload["fingerprint_topology"]))
    if "fingerprint_topology_attention" in payload:
        _assert_fingerprint_topology_attention(
            _mapping(payload["fingerprint_topology_attention"])
        )
    if "fingerprint_readiness" in payload:
        _assert_fingerprint_readiness(
            _mapping(payload["fingerprint_readiness"])
        )
    if "next_actions" in payload:
        _assert_quality_next_actions(_mapping(payload["next_actions"]))
    if "remediation_coverage" in payload:
        _assert_quality_remediation_coverage(
            _mapping(payload["remediation_coverage"])
        )

    for target_summary in _list(payload["target_summaries"]):
        _assert_target_summary(_mapping(target_summary))
    for cross_summary in _list(payload["cross_target_summaries"]):
        _assert_target_summary(
            _mapping(cross_summary),
            allow_cross_target=True,
        )

    artifact = _mapping(payload["report_artifact"])
    assert set(artifact) == {
        "kind",
        "metadata",
        "path",
        "sha256",
        "size_bytes",
    }
    assert artifact["kind"] == "quality-report"
    assert artifact["path"] in {
        "quality-fixtures/reports/fingerprint-report.json",
        "quality-fixtures/reports/run-scoped-report.json",
    }
    assert len(str(artifact["sha256"])) == 64
    artifact_metadata = _mapping(artifact["metadata"])
    assert artifact_metadata["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert not str(artifact["path"]).startswith("/")

    decision = _mapping(payload["exit_decision"])
    assert set(decision) == {"exit_code", "policy", "reason"}
    policy = _mapping(decision["policy"])
    assert set(policy) == {"fail_on", "max_errors", "max_warnings"}


def _assert_fingerprint_coverage(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "cache_source_counts",
        "discovered_target_count",
        "evaluated_fingerprint_target_count",
        "fingerprint_target_count",
        "parsed_non_empty_coverage_count",
        "rule_id",
        "schema_version",
        "skipped_fingerprint_target_count",
        "skipped_reason_counts",
        "source_kind_counts",
        "supported_readable_count",
        "target_kind_counts",
        "timeframe_counts",
        "unavailable_count",
        "unavailable_reason_counts",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "fingerprint_target_count",
        "discovered_target_count",
        "evaluated_fingerprint_target_count",
        "parsed_non_empty_coverage_count",
        "skipped_fingerprint_target_count",
        "supported_readable_count",
        "unavailable_count",
    ):
        assert isinstance(payload[key], int)
    for key in (
        "cache_source_counts",
        "skipped_reason_counts",
        "source_kind_counts",
        "target_kind_counts",
        "timeframe_counts",
        "unavailable_reason_counts",
    ):
        assert isinstance(payload[key], dict)


def _assert_fingerprint_distribution(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "cache_backed_distribution_target_count",
        "cache_source_counts",
        "distribution_kind_counts",
        "distribution_source_counts",
        "distribution_target_count",
        "empty_distribution_target_count",
        "included_target_count",
        "invalid_row_target_count",
        "m1_bar_distribution_target_count",
        "missing_distribution_target_count",
        "omitted_target_count",
        "partial_row_target_count",
        "precision_source_counts",
        "rule_id",
        "schema_version",
        "source_kind_counts",
        "status_counts",
        "target_count",
        "target_summaries",
        "text_backed_distribution_target_count",
        "tick_distribution_target_count",
        "total_invalid_row_count",
        "total_partial_row_count",
        "truncated",
        "truncated_distribution_target_count",
        "unavailable_distribution_target_count",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "cache_backed_distribution_target_count",
        "distribution_target_count",
        "empty_distribution_target_count",
        "included_target_count",
        "invalid_row_target_count",
        "m1_bar_distribution_target_count",
        "missing_distribution_target_count",
        "omitted_target_count",
        "partial_row_target_count",
        "target_count",
        "text_backed_distribution_target_count",
        "tick_distribution_target_count",
        "total_invalid_row_count",
        "total_partial_row_count",
        "truncated_distribution_target_count",
        "unavailable_distribution_target_count",
    ):
        assert isinstance(payload[key], int)
    assert isinstance(payload["truncated"], bool)
    for key in (
        "cache_source_counts",
        "distribution_kind_counts",
        "distribution_source_counts",
        "precision_source_counts",
        "source_kind_counts",
        "status_counts",
    ):
        assert isinstance(payload[key], dict)
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_distribution_target(_mapping(target_summary))


def _assert_fingerprint_distribution_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "cache_source",
        "distribution_kind",
        "distribution_source",
        "invalid_row_count",
        "invalid_row_rate",
        "negative_spread_count",
        "negative_spread_rate",
        "partial_row_count",
        "precision_decimal_place_count",
        "precision_source",
        "row_count",
        "sampled_row_count",
        "source_kind",
        "status",
        "target_axis",
        "truncated",
        "usable_row_count",
        "zero_spread_count",
        "zero_spread_rate",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    assert payload["distribution_kind"] in {"m1_bar", "missing", "tick"}
    assert payload["status"] in {"available", "missing", "unavailable"}
    assert isinstance(payload["truncated"], bool)
    for key in (
        "invalid_row_count",
        "negative_spread_count",
        "partial_row_count",
        "precision_decimal_place_count",
        "row_count",
        "sampled_row_count",
        "usable_row_count",
        "zero_spread_count",
    ):
        assert isinstance(payload[key], int)


def _assert_fingerprint_distribution_attention(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "attention_flag_counts",
        "attention_level_counts",
        "attention_target_count",
        "attention_thresholds",
        "distribution_target_count",
        "included_attention_target_count",
        "omitted_attention_target_count",
        "rule_id",
        "schema_version",
        "target_summaries",
        "truncated",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "distribution_target_count",
        "attention_target_count",
        "included_attention_target_count",
        "omitted_attention_target_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["truncated"], bool)
    assert isinstance(payload["attention_flag_counts"], dict)
    assert isinstance(payload["attention_level_counts"], dict)
    _assert_fingerprint_distribution_attention_thresholds(
        _mapping(payload["attention_thresholds"])
    )
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_distribution_attention_target(
            _mapping(target_summary)
        )


def _assert_fingerprint_distribution_attention_thresholds(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "flag_cache_float_precision",
        "flag_truncated_distribution",
        "invalid_row_min_count",
        "invalid_row_min_rate",
        "negative_spread_min_count",
        "negative_spread_min_rate",
        "zero_spread_min_count",
        "zero_spread_min_rate",
    }
    for key in (
        "invalid_row_min_count",
        "negative_spread_min_count",
        "zero_spread_min_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 1
    for key in (
        "invalid_row_min_rate",
        "negative_spread_min_rate",
        "zero_spread_min_rate",
    ):
        assert isinstance(payload[key], float)
        assert 0.0 <= payload[key] <= 1.0
    assert isinstance(payload["flag_cache_float_precision"], bool)
    assert isinstance(payload["flag_truncated_distribution"], bool)


def _assert_fingerprint_distribution_attention_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "attention_flags",
        "attention_level",
        "cache_source",
        "distribution_kind",
        "distribution_source",
        "invalid_row_count",
        "invalid_row_rate",
        "negative_spread_count",
        "negative_spread_rate",
        "partial_row_count",
        "precision_decimal_place_count",
        "precision_source",
        "row_count",
        "sampled_row_count",
        "source_kind",
        "status",
        "target_axis",
        "truncated",
        "usable_row_count",
        "zero_spread_count",
        "zero_spread_rate",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    assert payload["attention_level"] in {
        "defect",
        "microstructure",
        "missing",
        "precision",
        "sample",
    }
    assert isinstance(payload["attention_flags"], list)
    assert payload["distribution_kind"] in {"m1_bar", "missing", "tick"}
    for key in (
        "invalid_row_count",
        "negative_spread_count",
        "partial_row_count",
        "precision_decimal_place_count",
        "row_count",
        "sampled_row_count",
        "usable_row_count",
        "zero_spread_count",
    ):
        assert isinstance(payload[key], int)


def _assert_fingerprint_regime(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "cache_source_counts",
        "calendar_profile",
        "calendar_regime_target_count",
        "calendar_status_counts",
        "computed_from_counts",
        "conditional_distribution_target_count",
        "conditional_status_counts",
        "count_limit",
        "included_target_count",
        "omitted_target_count",
        "rule_id",
        "schema_version",
        "target_count",
        "target_summaries",
        "top_active_session_counts",
        "top_day_of_week_counts",
        "top_event_tag_counts",
        "top_holiday_tag_counts",
        "top_hour_of_day_counts",
        "top_session_state_counts",
        "top_special_tag_counts",
        "truncated",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "calendar_regime_target_count",
        "conditional_distribution_target_count",
        "count_limit",
        "included_target_count",
        "omitted_target_count",
        "target_count",
    ):
        assert isinstance(payload[key], int)
    assert isinstance(payload["truncated"], bool)
    for key in (
        "cache_source_counts",
        "calendar_status_counts",
        "computed_from_counts",
        "conditional_status_counts",
    ):
        assert isinstance(payload[key], dict)
    profile = _mapping(payload["calendar_profile"])
    assert set(profile) == {
        "complete_count",
        "incomplete_count",
        "source_counts",
        "static_advisory_count",
        "version_counts",
    }
    for key in (
        "complete_count",
        "incomplete_count",
        "static_advisory_count",
    ):
        assert isinstance(profile[key], int)
    assert isinstance(profile["source_counts"], dict)
    assert isinstance(profile["version_counts"], dict)
    for key in (
        "top_active_session_counts",
        "top_day_of_week_counts",
        "top_event_tag_counts",
        "top_holiday_tag_counts",
        "top_hour_of_day_counts",
        "top_session_state_counts",
        "top_special_tag_counts",
    ):
        for row in _list(payload[key]):
            assert set(_mapping(row)) == {"count", "value"}
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_regime_target(_mapping(target_summary))


def _assert_fingerprint_regime_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "calendar_regimes",
        "conditional_distributions",
        "source_kind",
        "target_axis",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    calendar = _mapping(payload["calendar_regimes"])
    assert set(calendar) == {
        "active_session_counts",
        "calendar_profile",
        "cache_source",
        "computed_from",
        "day_of_week_counts",
        "event_tag_counts",
        "holiday_tag_counts",
        "hour_of_day_counts",
        "invalid_timestamp_count",
        "parsed_row_count",
        "raw_status",
        "row_count",
        "session_state_counts",
        "special_tag_counts",
        "status",
    }
    assert calendar["status"] in {"available", "missing", "unavailable"}
    for key in ("invalid_timestamp_count", "parsed_row_count", "row_count"):
        assert isinstance(calendar[key], int)
    profile = _mapping(calendar["calendar_profile"])
    assert set(profile) == {
        "complete",
        "missing_optional_calendar_data",
        "name",
        "source",
        "static_advisory",
        "version",
    }
    conditional = _mapping(payload["conditional_distributions"])
    assert conditional["status"] in {
        "absent",
        "available",
        "not_applicable",
    }
    if conditional["status"] == "available":
        assert "by_active_session" in conditional
        assert "by_special_tag" in conditional


def _assert_fingerprint_topology(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "cache_source_counts",
        "computed_from_counts",
        "flag_counts",
        "included_target_count",
        "omitted_target_count",
        "rule_id",
        "sampling_basis_counts",
        "schema_version",
        "status_counts",
        "target_count",
        "target_summaries",
        "truncated",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "target_count",
        "included_target_count",
        "omitted_target_count",
    ):
        assert isinstance(payload[key], int)
    assert isinstance(payload["truncated"], bool)
    for key in (
        "cache_source_counts",
        "computed_from_counts",
        "flag_counts",
        "sampling_basis_counts",
        "status_counts",
    ):
        assert isinstance(payload[key], dict)
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_topology_target(_mapping(target_summary))


def _assert_fingerprint_topology_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "cache_source",
        "computed_from",
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "flags",
        "invalid_timestamp_count",
        "max_gap_ms",
        "median_interval_ms",
        "non_monotonic_count",
        "parsed_row_count",
        "row_count",
        "sampling_basis",
        "status",
        "suspicious_gap_count",
        "target_axis",
        "weekend_activity_count",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    assert payload["status"] in {"regular", "irregular", "unavailable"}
    assert isinstance(payload["flags"], list)
    for key in (
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "invalid_timestamp_count",
        "non_monotonic_count",
        "row_count",
        "suspicious_gap_count",
        "weekend_activity_count",
    ):
        assert isinstance(payload[key], int)


def _assert_fingerprint_topology_attention(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "attention_flag_counts",
        "attention_level_counts",
        "attention_target_count",
        "included_attention_target_count",
        "omitted_attention_target_count",
        "rule_id",
        "schema_version",
        "target_summaries",
        "topology_target_count",
        "truncated",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "topology_target_count",
        "attention_target_count",
        "included_attention_target_count",
        "omitted_attention_target_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["truncated"], bool)
    assert isinstance(payload["attention_flag_counts"], dict)
    assert isinstance(payload["attention_level_counts"], dict)
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_topology_attention_target(_mapping(target_summary))


def _assert_fingerprint_topology_attention_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "attention_flags",
        "attention_level",
        "cache_source",
        "computed_from",
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "flags",
        "invalid_timestamp_count",
        "max_gap_ms",
        "non_monotonic_count",
        "remediation_hints",
        "status",
        "suspicious_gap_count",
        "target_axis",
        "weekend_activity_count",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    assert payload["attention_level"] in {
        "unavailable",
        "structural",
        "sequence",
        "session",
    }
    assert isinstance(payload["attention_flags"], list)
    assert isinstance(payload["flags"], list)
    for hint in _list(payload["remediation_hints"]):
        _assert_fingerprint_remediation_hint(_mapping(hint))
    assert payload["status"] in {"regular", "irregular", "unavailable"}
    for key in (
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "invalid_timestamp_count",
        "non_monotonic_count",
        "suspicious_gap_count",
        "weekend_activity_count",
    ):
        assert isinstance(payload[key], int)
    assert payload["max_gap_ms"] is None or isinstance(
        payload["max_gap_ms"],
        int,
    )


def _assert_fingerprint_readiness(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "applicable_dynamics_status_counts",
        "cache_source_counts",
        "computed_from_counts",
        "dependence_acf_basis_counts",
        "dependence_computed_lag_count",
        "dependence_limitation_counts",
        "dependence_reason_counts",
        "dependence_skipped_lag_count",
        "dependence_skipped_lag_reason_counts",
        "dependence_status_counts",
        "dynamics_limitation_counts",
        "dynamics_reason_counts",
        "dynamics_status_counts",
        "included_target_count",
        "omitted_target_count",
        "profile_completeness",
        "row_order_counts",
        "rule_id",
        "schema_version",
        "section_skip_reason_counts",
        "section_status_counts",
        "target_count",
        "target_summaries",
        "tick_spread_conditioning_status_counts",
        "topology_limitation_counts",
        "truncated",
    }
    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
    )
    assert payload["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    for key in (
        "target_count",
        "included_target_count",
        "omitted_target_count",
        "dependence_computed_lag_count",
        "dependence_skipped_lag_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["truncated"], bool)
    for key in (
        "applicable_dynamics_status_counts",
        "cache_source_counts",
        "computed_from_counts",
        "dependence_acf_basis_counts",
        "dependence_limitation_counts",
        "dependence_reason_counts",
        "dependence_skipped_lag_reason_counts",
        "dependence_status_counts",
        "dynamics_limitation_counts",
        "dynamics_reason_counts",
        "dynamics_status_counts",
        "row_order_counts",
        "section_skip_reason_counts",
        "section_status_counts",
        "tick_spread_conditioning_status_counts",
        "topology_limitation_counts",
    ):
        assert isinstance(payload[key], dict)
    profile = _mapping(payload["profile_completeness"])
    assert set(profile) == {
        "calendar_profile_complete_count",
        "calendar_profile_incomplete_count",
        "calendar_profile_static_advisory_count",
    }
    for target_summary in _list(payload["target_summaries"]):
        _assert_fingerprint_readiness_target(_mapping(target_summary))


def _assert_fingerprint_readiness_target(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "applicable_dynamics_reason",
        "applicable_dynamics_section",
        "applicable_dynamics_status",
        "dependence",
        "microstructure_dynamics",
        "profile_completeness",
        "return_dynamics",
        "section_skip_reasons",
        "section_statuses",
        "sections_emitted_count",
        "sections_expected_count",
        "sections_skipped_count",
        "source_kind",
        "source_reason",
        "target_axis",
        "tick_spread_conditioning",
        "topology",
        "topology_limitations",
    }
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    assert payload["applicable_dynamics_section"] in {
        "microstructure_dynamics",
        "none",
        "return_dynamics",
    }
    assert payload["applicable_dynamics_status"] in {
        "limited",
        "skipped",
        "unavailable",
        "valid",
    }
    assert isinstance(payload["section_skip_reasons"], list)
    assert isinstance(payload["section_statuses"], dict)
    assert isinstance(payload["topology_limitations"], list)
    for key in (
        "sections_emitted_count",
        "sections_expected_count",
        "sections_skipped_count",
    ):
        assert isinstance(payload[key], int)
    _assert_fingerprint_readiness_topology(_mapping(payload["topology"]))
    _assert_fingerprint_readiness_profile(
        _mapping(payload["profile_completeness"])
    )
    _assert_fingerprint_readiness_tick_spread(
        _mapping(payload["tick_spread_conditioning"])
    )
    _assert_fingerprint_readiness_dynamics(_mapping(payload["return_dynamics"]))
    _assert_fingerprint_readiness_dynamics(
        _mapping(payload["microstructure_dynamics"])
    )
    _assert_fingerprint_readiness_dependence(_mapping(payload["dependence"]))


def _assert_fingerprint_readiness_topology(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "cache_source",
        "computed_from",
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "invalid_timestamp_count",
        "non_monotonic_count",
        "parsed_row_count",
        "row_count",
        "sampling_basis",
        "suspicious_gap_count",
        "weekend_activity_count",
    }
    for key in (
        "duplicate_timestamp_count",
        "expected_session_closure_count",
        "invalid_timestamp_count",
        "non_monotonic_count",
        "row_count",
        "suspicious_gap_count",
        "weekend_activity_count",
    ):
        assert isinstance(payload[key], int)


def _assert_fingerprint_readiness_profile(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "calendar_profile_complete",
        "calendar_profile_name",
        "calendar_profile_source",
        "calendar_profile_static_advisory",
        "calendar_profile_version",
        "missing_optional_calendar_data",
        "source",
    }
    assert isinstance(payload["calendar_profile_complete"], bool)
    assert isinstance(payload["calendar_profile_static_advisory"], bool)
    assert isinstance(payload["missing_optional_calendar_data"], bool)


def _assert_fingerprint_readiness_tick_spread(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {"eligible", "emitted", "reason", "status"}
    assert isinstance(payload["eligible"], bool)
    assert isinstance(payload["emitted"], bool)


def _assert_fingerprint_readiness_dynamics(
    payload: dict[str, JSONValue],
) -> None:
    required = {
        "basis",
        "cache_source",
        "computed_from",
        "invalid_row_count",
        "limitations",
        "partial_row_count",
        "reason",
        "regular_grid",
        "row_count",
        "row_order",
        "sampled_row_count",
        "status",
        "truncated",
        "usable_row_count",
    }
    assert required <= set(payload)
    assert payload["status"] in {"limited", "skipped", "unavailable", "valid"}
    assert isinstance(payload["limitations"], list)
    assert isinstance(payload["regular_grid"], bool)
    assert isinstance(payload["truncated"], bool)
    for key in (
        "invalid_row_count",
        "partial_row_count",
        "row_count",
        "sampled_row_count",
        "usable_row_count",
    ):
        assert isinstance(payload[key], int)
    for key in (
        "absolute_return",
        "absolute_spread_change",
        "close_log_return",
        "interarrival_ms",
        "open_jump",
        "spread",
        "spread_change",
        "squared_return",
    ):
        if key in payload:
            _assert_compact_numeric_summary(_mapping(payload[key]))
    for key in (
        "burst",
        "flatline",
        "one_sided_movement",
        "spread_jump",
        "stale_quote",
    ):
        if key in payload:
            assert isinstance(payload[key], dict)


def _assert_fingerprint_readiness_dependence(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "acf_basis",
        "basis",
        "cache_source",
        "computed_from",
        "computed_lag_count",
        "included_lag_count",
        "invalid_row_count",
        "lag_count",
        "lags",
        "lags_truncated",
        "limitations",
        "omitted_lag_count",
        "partial_row_count",
        "reason",
        "regular_grid",
        "row_count",
        "row_order",
        "sampled_row_count",
        "series",
        "series_count",
        "skipped_lag_count",
        "skipped_lag_reason_counts",
        "status",
        "truncated",
        "usable_row_count",
    }
    assert payload["status"] in {"limited", "skipped", "unavailable", "valid"}
    assert isinstance(payload["limitations"], list)
    assert isinstance(payload["lags"], list)
    assert isinstance(payload["lags_truncated"], bool)
    assert isinstance(payload["regular_grid"], bool)
    assert isinstance(payload["truncated"], bool)
    assert isinstance(payload["skipped_lag_reason_counts"], dict)
    for key in (
        "computed_lag_count",
        "included_lag_count",
        "invalid_row_count",
        "lag_count",
        "omitted_lag_count",
        "partial_row_count",
        "row_count",
        "sampled_row_count",
        "series_count",
        "skipped_lag_count",
        "usable_row_count",
    ):
        assert isinstance(payload[key], int)
    series = _mapping(payload["series"])
    for summary in series.values():
        _assert_fingerprint_readiness_acf_series(_mapping(summary))


def _assert_fingerprint_readiness_acf_series(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "computed_lag_count",
        "sample_count",
        "skipped_lag_count",
        "skipped_lag_reason_counts",
    }
    assert isinstance(payload["skipped_lag_reason_counts"], dict)
    for key in ("computed_lag_count", "sample_count", "skipped_lag_count"):
        assert isinstance(payload[key], int)


def _assert_compact_numeric_summary(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "count",
        "mad",
        "max",
        "mean",
        "median",
        "min",
        "p95",
        "p99",
    }
    assert isinstance(payload["count"], int)


def _assert_fingerprint_remediation_hint(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {"action_kind", "code", "flag", "message", "rule_id"}
    assert isinstance(payload["action_kind"], str)
    assert payload["action_kind"] in {
        "configure",
        "inspect",
        "rebuild",
        "repair",
        "verify",
    }
    assert isinstance(payload["code"], str)
    assert payload["code"]
    assert isinstance(payload["flag"], str)
    assert payload["flag"]
    assert isinstance(payload["message"], str)
    assert payload["message"]
    assert isinstance(payload["rule_id"], str)
    assert payload["rule_id"]


def _assert_quality_next_actions(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "action_count",
        "actions",
        "included_action_count",
        "omitted_action_count",
        "schema_version",
        "source_counts",
        "truncated",
    }
    assert payload["schema_version"] == QUALITY_NEXT_ACTIONS_SCHEMA_VERSION
    assert isinstance(payload["action_count"], int)
    assert isinstance(payload["included_action_count"], int)
    assert isinstance(payload["omitted_action_count"], int)
    assert isinstance(payload["truncated"], bool)
    assert isinstance(payload["source_counts"], dict)
    for action in _list(payload["actions"]):
        _assert_quality_next_action(_mapping(action))


def _assert_quality_next_action(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "action_kind",
        "affected_target_count",
        "attention_level_counts",
        "code",
        "finding_code_counts",
        "flag_counts",
        "included_target_axis_count",
        "max_attention_level",
        "max_severity",
        "message",
        "occurrence_count",
        "omitted_target_axis_count",
        "rule_id",
        "severity_counts",
        "source_counts",
        "target_axis_count",
        "target_axis_counts",
        "target_axis_truncated",
        "urgency",
    }
    assert payload["urgency"] in {"high", "medium", "low"}
    assert payload["action_kind"] in {
        "configure",
        "inspect",
        "rebuild",
        "repair",
        "verify",
    }
    assert isinstance(payload["code"], str)
    assert payload["code"]
    assert isinstance(payload["message"], str)
    assert payload["message"]
    assert isinstance(payload["rule_id"], str)
    assert payload["rule_id"]
    for key in (
        "affected_target_count",
        "included_target_axis_count",
        "occurrence_count",
        "omitted_target_axis_count",
        "target_axis_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["target_axis_truncated"], bool)
    for key in (
        "attention_level_counts",
        "finding_code_counts",
        "flag_counts",
        "severity_counts",
        "source_counts",
    ):
        assert isinstance(payload[key], dict)
    for item in _list(payload["target_axis_counts"]):
        _assert_quality_next_action_axis_count(_mapping(item))


def _assert_quality_next_action_axis_count(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {"count", "target_axis"}
    assert isinstance(payload["count"], int)
    assert payload["count"] > 0
    axis = _mapping(payload["target_axis"])
    assert set(axis) == {
        "data_format",
        "kind",
        "period",
        "symbol",
        "timeframe",
    }
    for value in axis.values():
        assert isinstance(value, str)
        assert value


def _assert_quality_remediation_coverage(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "count_limits",
        "finding_code_counts",
        "finding_count",
        "included_unmapped_group_count",
        "included_unmapped_warning_error_group_count",
        "mapped_finding_code_counts",
        "mapped_finding_count",
        "mapped_rule_id_counts",
        "mapped_severity_counts",
        "omitted_unmapped_group_count",
        "omitted_unmapped_warning_error_group_count",
        "rule_id_counts",
        "schema_version",
        "severity_counts",
        "unmapped_finding_code_counts",
        "unmapped_finding_count",
        "unmapped_group_count",
        "unmapped_groups",
        "unmapped_rule_id_counts",
        "unmapped_severity_counts",
        "unmapped_truncated",
        "unmapped_warning_error_finding_count",
        "unmapped_warning_error_group_count",
    }
    assert payload["schema_version"] == (
        QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION
    )
    for key in (
        "finding_count",
        "included_unmapped_group_count",
        "included_unmapped_warning_error_group_count",
        "mapped_finding_count",
        "omitted_unmapped_group_count",
        "omitted_unmapped_warning_error_group_count",
        "unmapped_finding_count",
        "unmapped_group_count",
        "unmapped_warning_error_finding_count",
        "unmapped_warning_error_group_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["unmapped_truncated"], bool)
    for key in (
        "mapped_severity_counts",
        "severity_counts",
        "unmapped_severity_counts",
    ):
        assert isinstance(payload[key], dict)
    for key in (
        "finding_code_counts",
        "mapped_finding_code_counts",
        "mapped_rule_id_counts",
        "rule_id_counts",
        "unmapped_finding_code_counts",
        "unmapped_rule_id_counts",
    ):
        assert isinstance(payload[key], list)
    count_limits = _mapping(payload["count_limits"])
    assert set(count_limits) == {
        "finding_code_counts",
        "mapped_finding_code_counts",
        "mapped_rule_id_counts",
        "rule_id_counts",
        "unmapped_finding_code_counts",
        "unmapped_rule_id_counts",
    }
    for limit in count_limits.values():
        _assert_payload_limit(_mapping(limit))
    for item in _list(payload["rule_id_counts"]):
        _assert_named_count(_mapping(item), "rule_id")
    for item in _list(payload["finding_code_counts"]):
        _assert_named_count(_mapping(item), "finding_code")
    for group in _list(payload["unmapped_groups"]):
        _assert_quality_remediation_coverage_group(_mapping(group))


def _assert_quality_remediation_coverage_group(
    payload: dict[str, JSONValue],
) -> None:
    assert set(payload) == {
        "finding_code",
        "included_target_axis_count",
        "mapped",
        "max_severity",
        "occurrence_count",
        "omitted_target_axis_count",
        "rule_id",
        "severity_counts",
        "target_axis_count",
        "target_axis_counts",
        "target_axis_truncated",
    }
    assert payload["mapped"] is False
    assert payload["max_severity"] in SEVERITY_VALUES
    assert isinstance(payload["finding_code"], str)
    assert payload["finding_code"]
    assert isinstance(payload["rule_id"], str)
    assert payload["rule_id"]
    for key in (
        "included_target_axis_count",
        "occurrence_count",
        "omitted_target_axis_count",
        "target_axis_count",
    ):
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
    assert isinstance(payload["severity_counts"], dict)
    assert isinstance(payload["target_axis_truncated"], bool)
    for item in _list(payload["target_axis_counts"]):
        _assert_quality_next_action_axis_count(_mapping(item))


def _assert_named_count(
    payload: dict[str, JSONValue],
    key_name: str,
) -> None:
    assert set(payload) == {key_name, "count"}
    assert isinstance(payload[key_name], str)
    assert payload[key_name]
    assert isinstance(payload["count"], int)
    assert payload["count"] > 0


def _assert_payload_limit(payload: dict[str, JSONValue]) -> None:
    assert set(payload) == {
        "included_count",
        "limit",
        "omitted_count",
        "total_count",
        "truncated",
    }
    for key in ("included_count", "limit", "omitted_count", "total_count"):
        assert isinstance(payload[key], int)
    assert isinstance(payload["truncated"], bool)


def _assert_summary(summary: dict[str, Any]) -> None:
    assert set(summary) == {
        "error_count",
        "finding_count",
        "info_count",
        "max_severity",
        "rule_count",
        "status",
        "target_count",
        "warning_count",
    }
    assert summary["status"] in STATUS_VALUES
    assert summary["max_severity"] in SEVERITY_VALUES
    for key in (
        "target_count",
        "rule_count",
        "finding_count",
        "info_count",
        "warning_count",
        "error_count",
    ):
        assert isinstance(summary[key], int)
        assert summary[key] >= 0


def _assert_target_summary(
    summary: dict[str, Any],
    *,
    allow_cross_target: bool = False,
) -> None:
    assert set(summary) == {
        "error_count",
        "finding_count",
        "info_count",
        "max_severity",
        "rule_count",
        "status",
        "target",
        "warning_count",
    }
    assert summary["status"] in STATUS_VALUES
    assert summary["max_severity"] in SEVERITY_VALUES
    _assert_target(
        _mapping(summary["target"]),
        allow_cross_target=allow_cross_target,
    )


def _assert_rule_result(rule_result: dict[str, Any]) -> None:
    assert set(rule_result) == {
        "findings",
        "max_severity",
        "rule_id",
        "status",
        "target",
    }
    assert rule_result["status"] in STATUS_VALUES
    assert rule_result["max_severity"] in SEVERITY_VALUES
    assert isinstance(rule_result["rule_id"], str)
    assert rule_result["rule_id"]
    _assert_target(_mapping(rule_result["target"]))
    for finding in _list(rule_result["findings"]):
        _assert_finding(_mapping(finding))


def _assert_finding(finding: dict[str, Any]) -> None:
    assert set(finding) == {
        "code",
        "location",
        "message",
        "metadata",
        "rule_id",
        "severity",
        "target",
    }
    assert finding["severity"] in SEVERITY_VALUES
    assert isinstance(finding["code"], str)
    assert finding["code"]
    assert isinstance(finding["message"], str)
    assert finding["message"]
    assert isinstance(finding["rule_id"], str)
    assert finding["rule_id"]
    _assert_target(_mapping(finding["target"]))
    _assert_location(_mapping(finding["location"]))
    assert isinstance(finding["metadata"], dict)


def _assert_location(location: dict[str, Any]) -> None:
    assert set(location) == {
        "column",
        "metadata",
        "path",
        "row_number",
        "timestamp_source",
        "timestamp_utc_ms",
    }
    assert isinstance(location["path"], str)
    assert location["row_number"] is None or isinstance(
        location["row_number"],
        int,
    )
    assert location["timestamp_utc_ms"] is None or isinstance(
        location["timestamp_utc_ms"],
        int,
    )
    assert isinstance(location["metadata"], dict)


def _assert_target(
    target: dict[str, Any],
    *,
    allow_cross_target: bool = False,
) -> None:
    assert set(target) == {
        "data_format",
        "kind",
        "metadata",
        "path",
        "period",
        "symbol",
        "timeframe",
    }
    target_kind_values = set(TARGET_KIND_VALUES)
    if allow_cross_target:
        target_kind_values.add("cross-target-finding")
    assert target["kind"] in target_kind_values
    assert isinstance(target["path"], str)
    assert not target["path"].startswith("/")
    assert "/Users/" not in target["path"]
    assert "/home/" not in target["path"]
    assert isinstance(target["metadata"], dict)


def _mapping(value: JSONValue) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def _list(value: JSONValue) -> list[JSONValue]:
    assert isinstance(value, list)
    return value


def _canonical_json(payload: dict[str, JSONValue]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _fingerprint_payload(
    payload: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    payload["fingerprint_id"] = _fingerprint_id(payload)
    return payload


def _fingerprint_id(payload: dict[str, JSONValue]) -> str:
    material = dict(payload)
    material.pop("fingerprint_id", None)
    source = dict(_mapping(material.get("source") or {}))
    source.pop("path", None)
    material["source"] = source
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _updating_goldens() -> bool:
    return os.environ.get(UPDATE_ENV_VAR) == "1"
