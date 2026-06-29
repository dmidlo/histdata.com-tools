"""Tests for the data-quality assessment engine contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from histdatacom.data_quality import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualityRunSummary,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    QualityTargetSummary,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import M1
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
)


@dataclass(frozen=True, slots=True)
class _StaticRule:
    rule_id: str
    description: str
    severity_by_case: dict[str, QualitySeverity | None]

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        severity = self.severity_by_case.get(str(target.metadata["case"]))
        if severity is None:
            return ()
        return (
            QualityFinding(
                severity=severity,
                code=f"TEST_{severity.value.upper()}",
                message=f"{self.rule_id} found {severity.value}",
                rule_id=self.rule_id,
                target=target,
                location=QualityLocation(
                    path=target.path,
                    row_number=1,
                    timestamp_source="20120201 000000",
                    timestamp_utc_ms=1328072400000,
                    column="datetime",
                    metadata={"case": target.metadata["case"]},
                ),
                metadata={"rule": self.rule_id},
            ),
        )


def test_quality_target_and_finding_round_trip_preserves_context(
    tmp_path: Path,
) -> None:
    """Findings should carry file, row, timestamp, symbol, and check context."""
    target = _target_for_case(tmp_path, CLEAN_M1_CASE)
    finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="m1.timestamp.unique",
        target=target,
        location=QualityLocation(
            path=target.path,
            row_number=2,
            timestamp_source="20120201 000100",
            timestamp_utc_ms=1328072460000,
            column="datetime",
            metadata={"source_timezone": "EST-no-DST"},
        ),
        metadata={"duplicate_of_row": 1},
    )

    restored_target = QualityTarget.from_dict(target.to_dict())
    restored_finding = QualityFinding.from_dict(finding.to_dict())

    assert restored_target == target
    assert restored_finding == finding
    assert restored_finding.target.symbol == "EURUSD"
    assert restored_finding.target.timeframe == M1
    assert restored_finding.location.row_number == 2
    assert restored_finding.location.metadata["source_timezone"] == "EST-no-DST"


def test_quality_engine_runs_multiple_rules_and_aggregates_status(
    tmp_path: Path,
) -> None:
    """One engine path should aggregate clean, warning, and error findings."""
    clean = _target_for_case(tmp_path, CLEAN_M1_CASE)
    duplicate = _target_for_case(
        tmp_path,
        case_by_name("m1_duplicate_timestamp"),
    )
    missing = _target_for_case(tmp_path, case_by_name("m1_missing_file"))
    rules = (
        _StaticRule(
            rule_id="manifest.case-observed",
            description="records that each case was checked",
            severity_by_case={
                "clean_m1": QualitySeverity.INFO,
                "m1_duplicate_timestamp": QualitySeverity.INFO,
                "m1_missing_file": QualitySeverity.INFO,
            },
        ),
        _StaticRule(
            rule_id="m1.timestamp.unique",
            description="flags duplicate timestamps",
            severity_by_case={
                "m1_duplicate_timestamp": QualitySeverity.WARNING,
            },
        ),
        _StaticRule(
            rule_id="file.exists",
            description="flags missing files",
            severity_by_case={"m1_missing_file": QualitySeverity.ERROR},
        ),
    )

    report = run_quality_assessment(
        targets=(clean, duplicate, missing),
        rules=rules,
        metadata={"request_id": "run-test"},
    )

    assert len(report.rule_results) == 9
    assert [result.status for result in report.rule_results].count(
        QualityStatus.CLEAN
    ) == 7
    assert report.summary() == QualityRunSummary(
        target_count=3,
        rule_count=3,
        finding_count=5,
        info_count=3,
        warning_count=1,
        error_count=1,
        status=QualityStatus.FAILED,
        max_severity=QualitySeverity.ERROR,
    )
    assert report.status == QualityStatus.FAILED
    assert report.max_severity == QualitySeverity.ERROR
    assert report.metadata == {"request_id": "run-test"}
    assert report.target_summaries == (
        QualityTargetSummary(
            target=clean,
            rule_count=3,
            finding_count=1,
            info_count=1,
            warning_count=0,
            error_count=0,
            status=QualityStatus.CLEAN,
            max_severity=QualitySeverity.INFO,
        ),
        QualityTargetSummary(
            target=duplicate,
            rule_count=3,
            finding_count=2,
            info_count=1,
            warning_count=1,
            error_count=0,
            status=QualityStatus.WARNING,
            max_severity=QualitySeverity.WARNING,
        ),
        QualityTargetSummary(
            target=missing,
            rule_count=3,
            finding_count=2,
            info_count=1,
            warning_count=0,
            error_count=1,
            status=QualityStatus.FAILED,
            max_severity=QualitySeverity.ERROR,
        ),
    )


def test_quality_engine_skips_duplicate_archive_semantic_scans() -> None:
    """Matching CSV targets should own non-inventory checks for same data."""
    archive = QualityTarget(
        path="/tmp/DAT_ASCII_EURUSD_M1_201202.zip",
        kind=QualityTargetKind.ZIP,
        data_format="ascii",
        timeframe=M1,
        symbol="EURUSD",
        period="201202",
        metadata={"case": "archive"},
    )
    csv = QualityTarget(
        path="/tmp/DAT_ASCII_EURUSD_M1_201202.csv",
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=M1,
        symbol="EURUSD",
        period="201202",
        metadata={"case": "csv"},
    )

    report = run_quality_assessment(
        targets=(archive, csv),
        rules=(
            _StaticRule(
                rule_id="inventory.zip.integrity",
                description="inventory still owns archive checks",
                severity_by_case={
                    "archive": QualitySeverity.INFO,
                    "csv": QualitySeverity.INFO,
                },
            ),
            _StaticRule(
                rule_id="time.ascii.gaps",
                description="semantic scans prefer extracted CSVs",
                severity_by_case={
                    "archive": QualitySeverity.ERROR,
                    "csv": QualitySeverity.INFO,
                },
            ),
        ),
    )

    assert [
        (result.rule_id, result.target.kind) for result in report.rule_results
    ] == [
        ("inventory.zip.integrity", QualityTargetKind.ZIP),
        ("inventory.zip.integrity", QualityTargetKind.CSV),
        ("time.ascii.gaps", QualityTargetKind.CSV),
    ]
    assert report.status is QualityStatus.CLEAN
    assert report.metadata["quality_engine"] == {
        "target_count": 2,
        "rule_count": 2,
        "target_rule_evaluation_count": 3,
        "skipped_duplicate_archive_rule_evaluation_count": 1,
        "duplicate_archive_scan_policy": (
            "prefer_extracted_csv_for_non_inventory_rules"
        ),
    }


def test_quality_engine_reports_bounded_progress(tmp_path: Path) -> None:
    """Long quality runs should expose progress without local file paths."""
    clean = _target_for_case(tmp_path, CLEAN_M1_CASE)
    duplicate = _target_for_case(
        tmp_path,
        case_by_name("m1_duplicate_timestamp"),
    )
    events: list[dict] = []

    report = run_quality_assessment(
        targets=(clean, duplicate),
        rules=(
            _StaticRule(
                rule_id="manifest.case-observed",
                description="records that each case was checked",
                severity_by_case={
                    "clean_m1": QualitySeverity.INFO,
                    "m1_duplicate_timestamp": QualitySeverity.INFO,
                },
            ),
            _StaticRule(
                rule_id="m1.timestamp.unique",
                description="flags duplicate timestamps",
                severity_by_case={
                    "m1_duplicate_timestamp": QualitySeverity.WARNING,
                },
            ),
        ),
        progress_callback=events.append,
    )

    assert report.summary().target_count == 2
    assert [event["phase"] for event in events].count("rule_start") == 4
    assert [event["phase"] for event in events].count("rule_complete") == 4
    assert events[0]["phase"] == "start"
    assert events[-1]["phase"] == "complete"
    assert events[-1]["completed"] == 4
    assert events[-1]["total"] == 4
    assert events[1]["target_symbol"] == "EURUSD"
    assert str(tmp_path) not in str(events)
    assert "path" not in str(events)


def test_quality_report_round_trip_recomputes_summary_state(
    tmp_path: Path,
) -> None:
    """Serialized reports should restore findings and aggregate status."""
    target = _target_for_case(tmp_path, case_by_name("m1_ohlc_violation"))
    report = run_quality_assessment(
        targets=(target,),
        rules=(
            _StaticRule(
                rule_id="m1.ohlc.valid",
                description="flags invalid OHLC ordering",
                severity_by_case={"m1_ohlc_violation": QualitySeverity.ERROR},
            ),
        ),
    )

    restored = QualityReport.from_dict(report.to_dict())

    assert restored == report
    assert restored.summary().to_dict() == report.to_dict()["summary"]
    assert restored.target_summaries[0].status == QualityStatus.FAILED
    assert restored.rule_results[0] == QualityRuleResult.from_dict(
        report.rule_results[0].to_dict()
    )


def test_empty_quality_run_is_clean() -> None:
    """A run with no evaluated targets or findings should aggregate as clean."""
    report = run_quality_assessment(targets=(), rules=())

    assert report.status == QualityStatus.CLEAN
    assert report.max_severity == QualitySeverity.INFO
    assert report.summary() == QualityRunSummary(
        target_count=0,
        rule_count=0,
        finding_count=0,
        info_count=0,
        warning_count=0,
        error_count=0,
        status=QualityStatus.CLEAN,
        max_severity=QualitySeverity.INFO,
    )
    assert report.to_dict()["target_summaries"] == []


def test_quality_enums_normalize_aliases_and_reject_bad_values() -> None:
    """Public enum constructors should be stable at API boundaries."""
    assert QualitySeverity.from_value(None) == QualitySeverity.INFO
    assert QualitySeverity.from_value("warn") == QualitySeverity.WARNING
    assert QualitySeverity.from_value("fatal") == QualitySeverity.ERROR
    assert (
        QualitySeverity.max((QualitySeverity.INFO, "warning", "error"))
        == QualitySeverity.ERROR
    )
    assert QualityStatus.from_value("passed") == QualityStatus.CLEAN
    assert QualityStatus.from_value("failure") == QualityStatus.FAILED
    assert QualityTargetKind.from_value("zip-file") == QualityTargetKind.ZIP
    assert QualityTargetKind.from_value("parquet") == QualityTargetKind.CACHE
    assert QualityTargetKind.from_value("xlsx") == QualityTargetKind.SPREADSHEET
    assert QualityTargetKind.from_value("not-a-kind") == (
        QualityTargetKind.UNKNOWN
    )

    with pytest.raises(ValueError, match="unknown quality severity"):
        QualitySeverity.from_value("critical")
    with pytest.raises(ValueError, match="unknown quality status"):
        QualityStatus.from_value("partial")


def _target_for_case(
    tmp_path: Path,
    case: HistDataAsciiCase,
) -> QualityTarget:
    path = write_ascii_case(tmp_path, case)
    return QualityTarget(
        path=str(path),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=case.timeframe,
        symbol="EURUSD",
        period="201202",
        metadata={
            "case": case.name,
            "anomalies": list(case.anomalies),
            "missing": case.missing,
        },
    )
