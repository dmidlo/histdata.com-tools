"""Tests for timestamp and calendar data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_EST_NO_DST_TIME_RULE_ID,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import (
    M1,
    parse_histdata_datetime_to_utc_ms,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    EST_NO_DST_CALENDAR_CASES,
    write_ascii_case,
    write_zip_case,
)


def test_time_group_registers_est_no_dst_rule() -> None:
    """The advertised time group should execute concrete timestamp checks."""
    assert [rule.rule_id for rule in quality_rules_for_groups(("time",))] == [
        ASCII_EST_NO_DST_TIME_RULE_ID
    ]


def test_clean_ascii_file_reports_est_no_dst_conversion_summary(
    tmp_path: Path,
) -> None:
    """Clean source timestamps should be normalized as fixed UTC-05:00."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    summary = _finding(report.findings, "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.severity is QualitySeverity.INFO
    assert summary.rule_id == ASCII_EST_NO_DST_TIME_RULE_ID
    assert summary.metadata["source_timezone"] == "EST-no-DST"
    assert summary.metadata["source_utc_offset"] == "-05:00"
    assert summary.metadata["utc_normalization_offset_ms"] == 18_000_000
    assert summary.metadata["parsed_row_count"] == 3
    assert summary.metadata["source_period_mismatch_count"] == 0
    assert summary.metadata["utc_month_boundary_count"] == 0
    assert summary.metadata["samples"][0]["timestamp_source"] == (
        "20120201 000000"
    )
    assert summary.metadata["samples"][0]["timestamp_utc_ms"] == 1328072400000


def test_dst_boundary_rows_keep_fixed_est_no_dst_offset(
    tmp_path: Path,
) -> None:
    """DST transition rows must not be localized as America/New_York."""
    cases = [
        case
        for case in EST_NO_DST_CALENDAR_CASES
        if "DST transition" in case.description
    ]
    paths = []
    for case in cases:
        period = case.raw[:6]
        path = tmp_path / f"DAT_ASCII_EURUSD_M1_{period}_DST.csv"
        path.write_text(
            f"{case.raw};1.306600;1.306600;1.306560;1.306560;0\n",
            encoding="utf-8",
        )
        paths.append(path)

    for path, case in zip(paths, cases, strict=True):
        report = _report_for_path(path)
        summary = _finding(
            report.findings,
            "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY",
        )
        sample = summary.metadata["samples"][0]
        assert report.status is QualityStatus.CLEAN
        assert sample["timestamp_source"] == case.raw
        assert sample["timestamp_utc_ms"] == case.expected_utc_ms


def test_source_month_membership_wins_over_utc_month_boundary(
    tmp_path: Path,
) -> None:
    """Rows near month end should belong to files by source EST-no-DST month."""
    case = next(
        case
        for case in EST_NO_DST_CALENDAR_CASES
        if case.raw == "20120229 235900"
    )
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202_BOUNDARY.csv"
    path.write_text(
        f"{case.raw};1.306600;1.306600;1.306560;1.306560;0\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    boundary = _finding(report.findings, "ASCII_TIMESTAMP_UTC_MONTH_BOUNDARY")
    assert report.status is QualityStatus.CLEAN
    assert boundary.severity is QualitySeverity.INFO
    assert boundary.location.row_number == 1
    assert boundary.location.timestamp_source == case.raw
    assert boundary.location.timestamp_utc_ms == case.expected_utc_ms
    assert boundary.metadata["target_period"] == "201202"
    assert boundary.metadata["samples"][0]["source_period"] == "201202"
    assert boundary.metadata["samples"][0]["utc_period"] == "201203"
    assert boundary.metadata["samples"][0]["utc_timestamp"] == (
        "2012-03-01T04:59:00Z"
    )


def test_tick_month_boundary_preserves_millisecond_precision(
    tmp_path: Path,
) -> None:
    """Tick rows should report UTC month boundaries with millisecond precision."""
    case = next(
        case
        for case in EST_NO_DST_CALENDAR_CASES
        if case.raw == "20120229 235959999"
    )
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_BOUNDARY.csv"
    path.write_text(f"{case.raw},1.306600,1.306770,0\n", encoding="utf-8")

    report = _report_for_path(path)

    boundary = _finding(report.findings, "ASCII_TIMESTAMP_UTC_MONTH_BOUNDARY")
    assert report.status is QualityStatus.CLEAN
    assert boundary.location.timestamp_utc_ms == case.expected_utc_ms
    assert boundary.metadata["samples"][0]["utc_timestamp"] == (
        "2012-03-01T04:59:59.999Z"
    )


def test_source_period_mismatch_is_an_error_with_source_and_utc_context(
    tmp_path: Path,
) -> None:
    """Wrong source-month rows should fail even when UTC context is available."""
    raw_timestamp = "20120301 000000"
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202_WRONG_MONTH.csv"
    path.write_text(
        f"{raw_timestamp};1.306600;1.306600;1.306560;1.306560;0\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    mismatch = _finding(
        report.findings,
        "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
    )
    assert report.status is QualityStatus.FAILED
    assert mismatch.severity is QualitySeverity.ERROR
    assert mismatch.location.row_number == 1
    assert mismatch.location.timestamp_source == raw_timestamp
    assert mismatch.location.timestamp_utc_ms == (
        parse_histdata_datetime_to_utc_ms(raw_timestamp, M1)
    )
    assert mismatch.metadata["target_period"] == "201202"
    assert mismatch.metadata["samples"][0]["source_period"] == "201203"
    assert mismatch.metadata["samples"][0]["utc_period"] == "201203"


def test_zip_member_timestamp_findings_include_source_member(
    tmp_path: Path,
) -> None:
    """ZIP timestamp findings should retain member context for investigation."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )

    report = _report_for_path(archive)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["source_member"] == CLEAN_M1_CASE.filename
    assert summary.metadata["samples"][0]["source_member"] == (
        CLEAN_M1_CASE.filename
    )


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("time",)),
    )


def _finding(findings, code: str):
    matches = tuple(finding for finding in findings if finding.code == code)
    assert len(matches) == 1
    return matches[0]
