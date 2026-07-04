"""Tests for data-quality report output and exit policy helpers."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_REPORT_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
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


def test_bounded_payload_caps_cache_scale_target_lists(
    tmp_path: Path,
) -> None:
    """Large quality runs should return counts plus bounded target samples."""
    report = _many_target_report(tmp_path, clean_count=5)
    discovery_targets = [
        _target(tmp_path / f"discovered-{index}.csv").to_dict()
        for index in range(6)
    ]
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("inventory",),
        discovery={
            "roots": [str(tmp_path / "data")],
            "target_count": len(discovery_targets),
            "targets": discovery_targets,
        },
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=None,
        discovery_target_limit=2,
        target_summary_limit=3,
        cross_target_summary_limit=1,
    )

    target_summaries = payload["target_summaries"]
    limits = payload["payload_limits"]

    assert payload["summary"]["target_count"] == 7
    assert payload["target_status_counts"] == {
        "clean": 5,
        "warning": 1,
        "failed": 1,
    }
    assert [summary["status"] for summary in target_summaries] == [
        "failed",
        "warning",
        "clean",
    ]
    assert payload["discovery"]["target_count"] == 6
    assert len(payload["discovery"]["targets"]) == 2
    assert payload["discovery"]["target_omitted_count"] == 4
    assert limits["target_summaries"]["omitted_count"] == 4
    assert limits["target_summaries"]["truncated"] is True


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


def test_quality_report_payload_adds_fingerprint_coverage_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize run coverage metadata."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]

    assert summary["discovered_target_count"] == 2
    assert summary["evaluated_fingerprint_target_count"] == 2
    assert summary["fingerprint_target_count"] == 2
    assert summary["skipped_fingerprint_target_count"] == 0
    assert summary["supported_readable_count"] == 1
    assert summary["unavailable_count"] == 1
    assert summary["parsed_non_empty_coverage_count"] == 1
    assert summary["skipped_reason_counts"] == {}
    assert summary["source_kind_counts"] == {"cache": 1, "unavailable": 1}
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["unavailable_reason_counts"] == {
        "unsupported_target_kind": 1,
    }


def test_quality_report_payload_adds_fingerprint_topology_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize human-readable topology metadata."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY
    ]
    target_summaries = summary["target_summaries"]

    assert summary["target_count"] == 2
    assert summary["included_target_count"] == 2
    assert summary["omitted_target_count"] == 0
    assert summary["status_counts"] == {
        "regular": 1,
        "unavailable": 1,
    }
    assert summary["computed_from_counts"] == {
        "direct_cache": 1,
        "unavailable": 1,
    }
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["flag_counts"] == {
        "cache_backed": 1,
        "unavailable_topology": 1,
    }
    assert target_summaries[0]["target_axis"] == {
        "data_format": "ascii",
        "timeframe": "M1",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "cache",
    }
    assert target_summaries[0]["row_count"] == 3
    assert target_summaries[0]["parsed_row_count"] == 3
    assert target_summaries[0]["duplicate_timestamp_count"] == 0
    assert target_summaries[0]["non_monotonic_count"] == 0
    assert target_summaries[0]["median_interval_ms"] == 60_000
    assert target_summaries[0]["max_gap_ms"] == 60_000
    assert target_summaries[0]["suspicious_gap_count"] == 0
    assert target_summaries[0]["expected_session_closure_count"] == 0
    assert target_summaries[0]["weekend_activity_count"] == 0
    assert target_summaries[0]["sampling_basis"] == "observed_sequence"
    assert target_summaries[0]["computed_from"] == "direct_cache"
    assert target_summaries[0]["cache_source"] == "direct"


def test_fingerprint_console_summary_reports_coverage_counts(
    tmp_path: Path,
) -> None:
    """Human output should summarize fingerprint coverage for operators."""
    output = format_quality_console_summary(
        _fingerprint_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint coverage" in output
    assert (
        "- targets: 2 supported/readable: 1 unavailable: 1 "
        "parsed/non-empty: 1"
    ) in output
    assert "- source kinds: cache=1, unavailable=1" in output
    assert "- cache sources: direct=1" in output
    assert "- unavailable reasons: unsupported_target_kind=1" in output
    assert "- target kinds: cache=1, spreadsheet=1" in output
    assert "- timeframes: M1=2" in output


def test_fingerprint_console_summary_reports_topology_lines(
    tmp_path: Path,
) -> None:
    """Human output should summarize per-target topology for operators."""
    output = format_quality_console_summary(
        _fingerprint_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint topology" in output
    assert (
        "- targets: 2 included: 2 regular: 1 irregular: 0 unavailable: 1"
    ) in output
    assert (
        "- ascii EURUSD M1 201202 cache: regular, observed_sequence, "
        "3 rows, 3 parsed, no duplicates, non-monotonic=0, "
        "median interval 60s, max gap 60s, 0 expected closures, "
        "0 suspicious gaps, weekend activity=0, "
        "computed_from=direct_cache, cache=direct"
    ) in output
    assert (
        "- ascii EURUSD M1 201202 spreadsheet: unavailable, unavailable, "
        "0 rows, unknown parsed, no duplicates, non-monotonic=0, "
        "median interval unavailable, max gap unavailable, "
        "0 expected closures, 0 suspicious gaps, weekend activity=0, "
        "computed_from=unavailable"
    ) in output
    assert "cache=unknown" not in output


def test_fingerprint_console_summary_reports_skipped_targets(
    tmp_path: Path,
) -> None:
    """Human output should expose skipped fingerprint targets when present."""
    base_report = _fingerprint_report(tmp_path)
    payload = quality_report_payload(base_report)
    metadata = payload["metadata"]
    assert isinstance(metadata, dict)
    coverage = metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]
    assert isinstance(coverage, dict)
    coverage = dict(coverage)
    coverage["discovered_target_count"] = 3
    coverage["skipped_fingerprint_target_count"] = 1
    coverage["skipped_reason_counts"] = {
        "duplicate_archive_preferred_csv": 1,
    }
    report = QualityReport(
        targets=base_report.targets,
        rule_results=base_report.rule_results,
        metadata={TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY: coverage},
    )

    output = format_quality_console_summary(
        report,
        check_groups=("fingerprint",),
    )

    assert "- skipped: 1" in output
    assert ("- skipped reasons: duplicate_archive_preferred_csv=1") in output


def test_bounded_quality_payload_includes_fingerprint_coverage(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose fingerprint coverage."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_coverage"]["source_kind_counts"] == {
        "cache": 1,
        "unavailable": 1,
    }


def test_bounded_quality_payload_includes_fingerprint_topology(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose topology summaries."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_topology"]["status_counts"] == {
        "regular": 1,
        "unavailable": 1,
    }
    assert (
        payload["fingerprint_topology"]["target_summaries"][0]["computed_from"]
        == "direct_cache"
    )


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


def _fingerprint_report(tmp_path: Path) -> QualityReport:
    cache = QualityTarget(
        path=str(tmp_path / ".data"),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    unavailable = QualityTarget(
        path=str(tmp_path / "unsupported.xlsx"),
        kind=QualityTargetKind.SPREADSHEET,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    cache_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=cache,
        metadata={
            "time_series_fingerprint": {
                "target_axis": {
                    "kind": "cache",
                    "timeframe": "M1",
                },
                "coverage": {
                    "row_count": 3,
                    "parsed_row_count": 3,
                },
                "temporal_topology": {
                    "row_count": 3,
                    "parsed_row_count": 3,
                    "invalid_timestamp_count": 0,
                    "duplicate_timestamp_count": 0,
                    "non_monotonic_count": 0,
                    "median_interval_ms": 60_000,
                    "max_gap_ms": 60_000,
                    "suspicious_gap_count": 0,
                    "expected_session_closure_count": 0,
                    "weekend_activity_count": 0,
                    "sampling_basis": "observed_sequence",
                    "computed_from": "direct_cache",
                    "cache_source": "direct",
                },
                "source": {
                    "kind": "cache",
                    "cache_source": "direct",
                },
            }
        },
    )
    unavailable_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SOURCE_UNAVAILABLE",
        message="Target source is unavailable for canonical fingerprinting.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=unavailable,
        metadata={
            "time_series_fingerprint": {
                "target_axis": {
                    "kind": "spreadsheet",
                    "timeframe": "M1",
                },
                "coverage": {
                    "row_count": 0,
                    "parsed_row_count": None,
                },
                "temporal_topology": {
                    "row_count": 0,
                    "parsed_row_count": None,
                    "invalid_timestamp_count": 0,
                    "duplicate_timestamp_count": 0,
                    "non_monotonic_count": 0,
                    "median_interval_ms": None,
                    "max_gap_ms": None,
                    "suspicious_gap_count": 0,
                    "expected_session_closure_count": 0,
                    "weekend_activity_count": 0,
                    "sampling_basis": "unavailable",
                    "computed_from": "unavailable",
                    "cache_source": None,
                },
                "source": {
                    "kind": "unavailable",
                    "reason": "unsupported_target_kind",
                },
            }
        },
    )
    return QualityReport(
        targets=(cache, unavailable),
        rule_results=(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=cache,
                findings=(cache_finding,),
            ),
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=unavailable,
                findings=(unavailable_finding,),
            ),
        ),
    )


def _many_target_report(tmp_path: Path, *, clean_count: int) -> QualityReport:
    clean_targets = tuple(
        _target(tmp_path / f"clean-{index}.csv") for index in range(clean_count)
    )
    warning = _target(tmp_path / "warning.csv")
    failed = _target(tmp_path / "failed.csv")
    warning_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="m1.timestamp.unique",
        target=warning,
        location=QualityLocation(path=warning.path),
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
        targets=(*clean_targets, warning, failed),
        rule_results=(
            *(
                QualityRuleResult(rule_id="file.exists", target=target)
                for target in clean_targets
            ),
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
