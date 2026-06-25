"""Tests for data-quality report output and exit policy helpers."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_REPORT_SCHEMA_VERSION,
    QualityExitPolicy,
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    bounded_quality_payload,
    format_quality_console_summary,
    publish_safe_json_value,
    publish_safe_path,
    quality_report_payload,
    quality_report_to_json,
    write_quality_report,
)
from histdatacom.runtime_contracts import ArtifactRef


def test_quality_json_report_is_deterministic_and_investigable(
    tmp_path: Path,
) -> None:
    """JSON reports should be stable and include finding context."""
    report = _mixed_report(tmp_path)
    first = quality_report_to_json(report)
    second = quality_report_to_json(report)

    assert first == second

    payload = json.loads(first)
    finding = payload["rule_results"][1]["findings"][0]

    assert payload["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert payload["summary"] == {
        "error_count": 1,
        "finding_count": 2,
        "info_count": 0,
        "max_severity": "error",
        "rule_count": 2,
        "status": "failed",
        "target_count": 3,
        "warning_count": 1,
    }
    assert finding["location"]["path"].endswith("warning.csv")
    assert finding["location"]["row_number"] == 7
    assert finding["location"]["timestamp_source"] == "20120201 000600"
    assert finding["location"]["timestamp_utc_ms"] == 1328072760000
    assert finding["target"]["symbol"] == "EURUSD"
    assert str(tmp_path) not in first


def test_quality_report_payload_is_publish_safe_by_default(
    tmp_path: Path,
) -> None:
    """Public report JSON should not expose local filesystem roots."""
    report = _mixed_report(tmp_path)
    payload = quality_report_payload(report)
    encoded = json.dumps(payload, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert "/Users/" not in encoded
    assert "/home/" not in encoded
    assert payload["targets"][0]["path"] == "clean.csv"
    assert (
        payload["rule_results"][1]["findings"][0]["location"]["path"]
        == "warning.csv"
    )


def test_quality_report_payload_can_preserve_raw_local_paths(
    tmp_path: Path,
) -> None:
    """Local debugging callers can still opt into exact report paths."""
    report = _mixed_report(tmp_path)
    payload = quality_report_payload(report, publish_safe=False)

    assert payload["targets"][0]["path"] == str(tmp_path / "clean.csv")


def test_publish_safe_json_value_sanitizes_nested_path_metadata() -> None:
    """Metadata path fields and embedded local paths should be publishable."""
    payload = {
        "m1_path": (
            "/Users/alice/projects/histdata.com-tools/data/ASCII/M1/"
            "eurusd/2012/DAT_ASCII_EURUSD_M1_2012.csv"
        ),
        "message": (
            "read /Users/alice/projects/histdata.com-tools/"
            "data/ASCII/M1/eurusd/2012/input.csv"
        ),
        "store_root": (
            "/Users/alice/Library/Application Support/histdatacom/"
            "sidecar/workspaces/project/manifests"
        ),
    }

    safe = publish_safe_json_value(payload)

    assert safe["m1_path"] == (
        "data/ASCII/M1/eurusd/2012/DAT_ASCII_EURUSD_M1_2012.csv"
    )
    assert safe["message"] == "read data/ASCII/M1/eurusd/2012/input.csv"
    assert safe["store_root"] == "manifests"
    assert publish_safe_path("/tmp/quality.json") == "quality.json"


def test_quality_report_writer_returns_orchestration_artifact_ref(
    tmp_path: Path,
) -> None:
    """Written reports should have a stable quality-report artifact surface."""
    report = _mixed_report(tmp_path)
    output = tmp_path / "reports" / "quality.json"

    artifact = write_quality_report(report, output)

    assert artifact.kind == "quality-report"
    assert artifact.path == str(output.resolve())
    assert artifact.size_bytes == output.stat().st_size
    assert len(artifact.sha256) == 64
    assert artifact.metadata["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert artifact.metadata["status"] == "failed"
    assert artifact.metadata["target_count"] == 3
    assert (
        json.loads(output.read_text(encoding="utf-8"))["summary"]["error_count"]
        == 1
    )


def test_bounded_payload_keeps_cross_target_finding_summaries(
    tmp_path: Path,
) -> None:
    """Run-level findings should stay visible without full rule history."""
    report = _cross_target_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("domain",),
        discovery={},
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=None,
    )

    summaries = payload["cross_target_summaries"]

    assert "rule_results" not in payload
    assert isinstance(summaries, list)
    assert {summary["target"]["symbol"] for summary in summaries} == {
        "AUDCAD",
        "AUDCHF",
        "CADCHF",
    }
    assert {summary["target"]["period"] for summary in summaries} == {"2008"}
    assert {summary["status"] for summary in summaries} == {"failed"}
    assert {summary["error_count"] for summary in summaries} == {1}


def test_bounded_payload_sanitizes_discovery_and_artifact_paths(
    tmp_path: Path,
) -> None:
    """Bounded orchestration metadata should be safe to persist in reports."""
    report = _mixed_report(tmp_path)
    artifact = ArtifactRef(
        kind="quality-report",
        path=str(tmp_path / "reports" / "quality.json"),
    )
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("inventory",),
        discovery={
            "roots": [str(tmp_path / "data" / "ASCII")],
            "metadata": {
                "store_path": (
                    "/Users/alice/Library/Application Support/histdatacom/"
                    "sidecar/workspaces/project/manifests/.histdatacom/"
                    "manifest-status.sqlite3"
                )
            },
        },
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=artifact,
    )
    encoded = json.dumps(payload, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert "/Users/" not in encoded
    assert payload["discovery"]["roots"] == ["data/ASCII"]
    assert payload["discovery"]["metadata"]["store_path"] == (
        ".histdatacom/manifest-status.sqlite3"
    )
    assert payload["report_artifact"]["path"] == "reports/quality.json"


def test_quality_console_summary_separates_target_statuses(
    tmp_path: Path,
) -> None:
    """Human output should group clean, warning, and failed files."""
    output = format_quality_console_summary(
        _mixed_report(tmp_path),
        check_groups=("inventory", "time"),
    )

    assert "Data quality assessment" in output
    assert "checks: inventory, time" in output
    assert "status: failed" in output
    assert "targets: 3 clean: 1 warning: 1 failed: 1" in output
    assert "Clean files\n- csv:" in output
    assert "Warning files\n- csv:" in output
    assert "Failed files\n- csv:" in output


def test_quality_exit_policy_applies_error_warning_and_never_modes(
    tmp_path: Path,
) -> None:
    """Exit decisions should be derived from configured thresholds."""
    summary = _mixed_report(tmp_path).summary()

    assert QualityExitPolicy.from_values().evaluate(summary).exit_code == 1
    assert (
        QualityExitPolicy.from_values(max_errors=1).evaluate(summary).exit_code
        == 0
    )
    assert (
        QualityExitPolicy.from_values(
            fail_on="warning",
            max_errors=1,
            max_warnings=0,
        )
        .evaluate(summary)
        .reason
        == "quality warning threshold exceeded: 1 > 0"
    )
    assert (
        QualityExitPolicy.from_values(fail_on="never")
        .evaluate(summary)
        .exit_code
        == 0
    )


def _mixed_report(tmp_path: Path) -> QualityReport:
    clean = _target(tmp_path / "clean.csv")
    warning = _target(tmp_path / "warning.csv")
    failed = _target(tmp_path / "failed.csv")
    warning_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="m1.timestamp.unique",
        target=warning,
        location=QualityLocation(
            path=warning.path,
            row_number=7,
            timestamp_source="20120201 000600",
            timestamp_utc_ms=1328072760000,
            column="datetime",
        ),
    )
    error_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=failed,
        location=QualityLocation(path=failed.path),
    )
    return QualityReport(
        targets=(clean, warning, failed),
        rule_results=(
            QualityRuleResult(rule_id="file.exists", target=clean),
            QualityRuleResult(
                rule_id="m1.timestamp.unique",
                target=warning,
                findings=(warning_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=failed,
                findings=(error_finding,),
            ),
        ),
    )


def _cross_target_report(tmp_path: Path) -> QualityReport:
    directory = QualityTarget(
        path=str(tmp_path / "data" / "ASCII" / "M1"),
        kind=QualityTargetKind.DIRECTORY,
        data_format="ascii",
    )
    finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR",
        message="triangular relationship differs from the direct pair",
        rule_id="domain.cross_instrument_consistency",
        target=directory,
        location=QualityLocation(
            path=directory.path,
            metadata={
                "direct_symbol": "AUDCAD",
                "period": "2008",
                "timeframe": "M1",
            },
        ),
        metadata={
            "samples": [
                {
                    "denominator_symbol": "CADCHF",
                    "direct_symbol": "AUDCAD",
                    "numerator_symbol": "AUDCHF",
                    "period": "2008",
                    "relationship": "AUDCHF / CADCHF ~= AUDCAD",
                    "timeframe": "M1",
                }
            ]
        },
    )
    return QualityReport(
        targets=(directory,),
        rule_results=(
            QualityRuleResult(
                rule_id="domain.cross_instrument_consistency",
                target=directory,
                findings=(finding,),
            ),
        ),
    )


def _target(path: Path) -> QualityTarget:
    return QualityTarget(
        path=str(path),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
