"""Golden compatibility tests for public data-quality report payloads."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from histdatacom.data_quality import (
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
    assert set(payload) == {
        "check_groups",
        "cross_target_summaries",
        "discovery",
        "exit_decision",
        "operation",
        "quality_profile",
        "report_artifact",
        "report_schema_version",
        "summary",
        "target_summaries",
    }
    assert payload["operation"] == "data-quality"
    assert payload["report_schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert "rule_results" not in payload
    assert "findings" not in payload
    assert isinstance(payload["quality_profile"], dict)
    _assert_summary(_mapping(payload["summary"]))

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
    assert artifact["path"] == "quality-fixtures/reports/run-scoped-report.json"
    assert len(str(artifact["sha256"])) == 64
    artifact_metadata = _mapping(artifact["metadata"])
    assert artifact_metadata["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert not str(artifact["path"]).startswith("/")

    decision = _mapping(payload["exit_decision"])
    assert set(decision) == {"exit_code", "policy", "reason"}
    policy = _mapping(decision["policy"])
    assert set(policy) == {"fail_on", "max_errors", "max_warnings"}


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


def _updating_goldens() -> bool:
    return os.environ.get(UPDATE_ENV_VAR) == "1"
