"""Tests for timestamp and calendar data-quality rules."""

from __future__ import annotations

from pathlib import Path

import histdatacom.data_quality.time as time_quality
from histdatacom.data_quality import (
    ASCII_EST_NO_DST_TIME_RULE_ID,
    ASCII_TIMESTAMP_CONTINUITY_RULE_ID,
    ASCII_TIMESTAMP_GAP_RULE_ID,
    ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
    HistDataAsciiTimestampContinuityRule,
    HistDataAsciiTimestampGapRule,
    HistDataGapTolerance,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    discover_quality_targets,
    quality_run_rules_for_groups,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import (
    TICK,
    parse_histdata_datetime_to_utc_ms,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_TICK_CASE,
    CLEAN_TICK_ROWS,
    EST_NO_DST_CALENDAR_CASES,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_time_group_registers_est_no_dst_rule() -> None:
    """The advertised time group should execute concrete timestamp checks."""
    assert [rule.rule_id for rule in quality_rules_for_groups(("time",))] == [
        ASCII_EST_NO_DST_TIME_RULE_ID,
        ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
        ASCII_TIMESTAMP_GAP_RULE_ID,
    ]


def test_time_group_registers_continuity_run_rule() -> None:
    """Cross-file timestamp checks should run for the advertised time group."""
    assert [
        rule.rule_id for rule in quality_run_rules_for_groups(("time",))
    ] == [ASCII_TIMESTAMP_CONTINUITY_RULE_ID]


def test_time_rules_noop_unsupported_raw_dimensions() -> None:
    """Timestamp rules must remain limited to ASCII tick targets."""
    rules = quality_rules_for_groups(("time",))
    for data_format, timeframe in (
        ("ascii", "M1"),
        ("metatrader", "T"),
    ):
        target = QualityTarget(
            path="/missing/retired.csv",
            kind=QualityTargetKind.CSV,
            data_format=data_format,
            timeframe=timeframe,
        )
        assert all(rule.evaluate(target) == () for rule in rules)


def test_clean_ascii_file_reports_est_no_dst_conversion_summary(
    tmp_path: Path,
) -> None:
    """Clean source timestamps should be normalized as fixed UTC-05:00."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

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
        "20120201 000003660"
    )
    assert summary.metadata["samples"][0]["timestamp_utc_ms"] == 1328072403660

    sequence = _finding(report.findings, "ASCII_TIMESTAMP_SEQUENCE_SUMMARY")
    assert sequence.rule_id == ASCII_TIMESTAMP_SEQUENCE_RULE_ID
    assert sequence.metadata["non_monotonic_count"] == 0
    assert sequence.metadata["tick_duplicate_row_count"] == 0
    assert sequence.metadata["tick_precision_mismatch_count"] == 0
    assert sequence.metadata["duplicate_policy"] == "detect-only"

    gaps = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    assert gaps.rule_id == ASCII_TIMESTAMP_GAP_RULE_ID
    assert gaps.metadata["tracked_gap_count"] == 0
    assert gaps.metadata["max_gap_ms"] == 11_017
    assert gaps.metadata["gap_bucket_counts"] == {
        "gt_1m": 0,
        "gt_5m": 0,
        "gt_30m": 0,
        "gt_1h": 0,
        "gt_1d": 0,
    }
    assert gaps.metadata["dynamic_window_policy"] == "inverted-tcp-backoff"


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
        path = tmp_path / f"DAT_ASCII_EURUSD_T_{period}_DST.csv"
        path.write_text(_tick_row(case.raw) + "\n", encoding="utf-8")
        paths.append(path)

    for path, case in zip(paths, cases, strict=True):
        report = _report_for_path(path)
        summary = _finding(
            report.findings,
            "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY",
        )
        sample = summary.metadata["samples"][0]
        assert report.summary().error_count == 0
        assert sample["timestamp_source"] == case.raw
        assert sample["timestamp_utc_ms"] == case.expected_utc_ms


def test_source_month_membership_wins_over_utc_month_boundary(
    tmp_path: Path,
) -> None:
    """Rows near month end should belong to files by source EST-no-DST month."""
    case = next(
        case
        for case in EST_NO_DST_CALENDAR_CASES
        if case.raw == "20120229 235959999"
    )
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_BOUNDARY.csv"
    path.write_text(_tick_row(case.raw) + "\n", encoding="utf-8")

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
        "2012-03-01T04:59:59.999Z"
    )


def test_annual_tick_source_membership_allows_year_periods(
    tmp_path: Path,
) -> None:
    """Annual source-year files still validate by source year if present."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_2012.csv"
    path.write_text(_tick_row("20121231 235959999") + "\n", encoding="utf-8")

    report = _report_for_path(path)

    boundary = _finding(report.findings, "ASCII_TIMESTAMP_UTC_MONTH_BOUNDARY")
    assert report.status is QualityStatus.CLEAN
    assert (
        _findings(report.findings, "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH")
        == ()
    )
    assert boundary.metadata["target_period"] == "2012"
    assert boundary.metadata["samples"][0]["source_period"] == "2012"
    assert boundary.metadata["samples"][0]["utc_period"] == "2013"


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
    raw_timestamp = "20120301 000000000"
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_WRONG_MONTH.csv"
    path.write_text(_tick_row(raw_timestamp) + "\n", encoding="utf-8")

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
        parse_histdata_datetime_to_utc_ms(raw_timestamp, TICK)
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
        CLEAN_TICK_CASE,
        zip_filename="DAT_ASCII_EURUSD_T_201202.zip",
    )

    report = _report_for_path(archive)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["source_member"] == CLEAN_TICK_CASE.filename
    assert summary.metadata["samples"][0]["source_member"] == (
        CLEAN_TICK_CASE.filename
    )


def test_tick_duplicate_timestamp_reports_without_deduping(
    tmp_path: Path,
) -> None:
    """Duplicate tick rows should be reported, not mutated."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_duplicate_row"))
    )

    finding = _finding(report.findings, "ASCII_TICK_DUPLICATE_ROW")
    summary = _finding(report.findings, "ASCII_TIMESTAMP_SEQUENCE_SUMMARY")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.rule_id == ASCII_TIMESTAMP_SEQUENCE_RULE_ID
    assert finding.location.row_number == 2
    assert finding.location.timestamp_source == "20120201 000003660"
    assert finding.location.metadata["duplicate_of_row"] == 1
    assert finding.location.metadata["dedupe_policy"] == "report-only"
    assert finding.metadata["row_count"] == 1
    assert summary.metadata["tick_duplicate_row_count"] == 1


def test_non_monotonic_timestamp_reports_previous_row_context(
    tmp_path: Path,
) -> None:
    """Timestamp ordering findings should identify the preceding row."""
    report = _report_for_path(
        write_ascii_case(
            tmp_path,
            case_by_name("tick_non_monotonic_timestamp"),
        )
    )

    finding = _finding(report.findings, "ASCII_TIMESTAMP_NON_MONOTONIC")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.location.row_number == 2
    assert finding.location.timestamp_source == "20120201 000003660"
    assert finding.location.metadata["previous_row_number"] == 1
    assert finding.location.metadata["previous_timestamp_source"] == (
        "20120201 000003973"
    )
    assert finding.location.timestamp_utc_ms == 1328072403660
    assert finding.location.metadata["previous_timestamp_utc_ms"] == (
        1328072403973
    )


def test_tick_duplicate_row_reports_exact_row_policy(
    tmp_path: Path,
) -> None:
    """Tick duplicate detection should be exact-row and report-only."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_duplicate_row"))
    )

    finding = _finding(report.findings, "ASCII_TICK_DUPLICATE_ROW")
    summary = _finding(report.findings, "ASCII_TIMESTAMP_SEQUENCE_SUMMARY")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.location.row_number == 2
    assert finding.location.timestamp_source == "20120201 000003660"
    assert finding.location.metadata["duplicate_of_row"] == 1
    assert finding.location.metadata["dedupe_policy"] == "report-only"
    assert finding.location.metadata["duplicate_row_values"] == [
        "20120201 000003660",
        "1.306600",
        "1.306770",
        "0",
    ]
    assert summary.metadata["tick_duplicate_row_count"] == 1


def test_tick_same_timestamp_with_different_quote_is_not_duplicate(
    tmp_path: Path,
) -> None:
    """Tick duplicates are exact row matches, not timestamp-only matches."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_SAME_MS.csv"
    path.write_text(
        "\n".join(
            (
                CLEAN_TICK_ROWS[0],
                "20120201 000003660,1.306610,1.306780,25",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    assert report.status is QualityStatus.CLEAN
    assert _findings(report.findings, "ASCII_TICK_DUPLICATE_ROW") == ()


def test_tick_precision_mismatch_reports_millisecond_width(
    tmp_path: Path,
) -> None:
    """Tick timestamps should retain HistData's millisecond width."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_PRECISION.csv"
    path.write_text(
        "\n".join(
            (
                "20120201 0000039,1.306600,1.306770,0",
                "20120201 000004973000,1.306580,1.306750,25",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_TICK_PRECISION_MISMATCH")
    summary = _finding(report.findings, "ASCII_TIMESTAMP_SEQUENCE_SUMMARY")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.location.row_number == 1
    assert finding.location.metadata["expected_fractional_digits"] == 3
    assert finding.location.metadata["observed_fractional_digits"] == 1
    assert finding.metadata["row_count"] == 2
    assert (
        finding.metadata["samples"][1]["metadata"]["observed_fractional_digits"]
        == 6
    )
    assert summary.metadata["tick_precision_mismatch_count"] == 2


def test_suspicious_weekday_gap_reports_distribution_and_dynamic_score(
    tmp_path: Path,
) -> None:
    """Weekday gaps outside tolerance should warn with bucket context."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_WEEKDAY_GAP.csv"
    path.write_text(
        "\n".join(
            (
                _tick_row("20120201 000000000"),
                _tick_row("20120201 000200000", volume=17),
                _tick_row("20120201 001000000", volume=2),
                _tick_row("20120201 001100000", volume=3),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    gap = _finding(report.findings, "ASCII_TIMESTAMP_SUSPICIOUS_GAP")
    assert report.status is QualityStatus.WARNING
    assert gap.severity is QualitySeverity.WARNING
    assert gap.location.row_number == 3
    assert gap.location.metadata["gap_ms"] == 480_000
    assert gap.location.metadata["previous_row_number"] == 2
    assert gap.location.metadata["classification"] == "suspicious"
    assert summary.metadata["tracked_gap_count"] == 2
    assert summary.metadata["suspicious_gap_count"] == 1
    assert summary.metadata["max_gap_ms"] == 480_000
    assert summary.metadata["gap_bucket_counts"] == {
        "gt_1m": 2,
        "gt_5m": 1,
        "gt_30m": 0,
        "gt_1h": 0,
        "gt_1d": 0,
    }
    assert summary.metadata["dynamic_gap_score"] > 0


def test_weekend_closure_gap_is_reported_as_expected_session_gap(
    tmp_path: Path,
) -> None:
    """Friday close to Sunday open should be classified as normal closure."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_WEEKEND_CLOSE.csv"
    path.write_text(
        "\n".join(
            (
                _tick_row("20120203 165900000"),
                _tick_row("20120205 170100000", volume=17),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    closure = _finding(
        report.findings,
        "ASCII_TIMESTAMP_EXPECTED_SESSION_CLOSURE_GAP",
    )
    assert report.status is QualityStatus.CLEAN
    assert closure.severity is QualitySeverity.INFO
    assert closure.location.row_number == 2
    assert closure.location.metadata["classification"] == (
        "expected_session_closure"
    )
    assert summary.metadata["expected_session_closure_count"] == 1
    assert summary.metadata["suspicious_gap_count"] == 0
    assert summary.metadata["weekend_activity_count"] == 0
    assert summary.metadata["gap_bucket_counts"]["gt_1d"] == 1


def test_weekend_closure_classification_requires_boundary_windows(
    tmp_path: Path,
) -> None:
    """Large gaps spanning Friday close are not automatically normal closure."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_BROAD_WEEKEND_GAP.csv"
    path.write_text(
        "\n".join(
            (
                _tick_row("20120201 000000000"),
                _tick_row("20120205 170100000", volume=17),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    suspicious = _finding(report.findings, "ASCII_TIMESTAMP_SUSPICIOUS_GAP")
    assert report.status is QualityStatus.WARNING
    assert suspicious.location.metadata["classification"] == "suspicious"
    assert summary.metadata["expected_session_closure_count"] == 0
    assert summary.metadata["suspicious_gap_count"] == 1
    assert (
        _findings(
            report.findings,
            "ASCII_TIMESTAMP_EXPECTED_SESSION_CLOSURE_GAP",
        )
        == ()
    )


def test_unexpected_weekend_activity_warns_without_hard_failure(
    tmp_path: Path,
) -> None:
    """Weekend records should be warnings by default."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_WEEKEND_ACTIVITY.csv"
    path.write_text(_tick_row("20120204 120000000") + "\n", encoding="utf-8")

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    activity = _finding(report.findings, "ASCII_TIMESTAMP_WEEKEND_ACTIVITY")
    assert report.status is QualityStatus.WARNING
    assert activity.severity is QualitySeverity.WARNING
    assert activity.location.row_number == 1
    assert activity.location.timestamp_source == "20120204 120000000"
    assert activity.location.metadata["session_state"] == "weekend_closure"
    assert summary.metadata["weekend_activity_count"] == 1


def test_gap_tolerance_windows_are_adjustable(
    tmp_path: Path,
) -> None:
    """Operators can widen suspicious-gap tolerance without changing parser."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_ADJUSTABLE_GAP.csv"
    path.write_text(
        "\n".join(
            (
                _tick_row("20120201 000000000"),
                _tick_row("20120201 000800000", volume=17),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    discovery = discover_quality_targets((path,))

    default_report = run_quality_assessment(
        discovery.targets,
        (HistDataAsciiTimestampGapRule(),),
    )
    widened_report = run_quality_assessment(
        discovery.targets,
        (
            HistDataAsciiTimestampGapRule(
                tolerance=HistDataGapTolerance(
                    suspicious_gap_ms=10 * 60_000,
                )
            ),
        ),
    )

    assert _finding(
        default_report.findings,
        "ASCII_TIMESTAMP_SUSPICIOUS_GAP",
    )
    widened_summary = _finding(
        widened_report.findings,
        "ASCII_TIMESTAMP_GAP_SUMMARY",
    )
    assert widened_report.status is QualityStatus.CLEAN
    assert widened_summary.metadata["tracked_gap_count"] == 1
    assert widened_summary.metadata["suspicious_gap_count"] == 0
    assert (
        _findings(
            widened_report.findings,
            "ASCII_TIMESTAMP_SUSPICIOUS_GAP",
        )
        == ()
    )


def test_dynamic_gap_score_uses_inverted_backoff_windowing(
    tmp_path: Path,
) -> None:
    """Clustered suspicious gaps should score higher as tolerance narrows."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202_DYNAMIC_GAP.csv"
    path.write_text(
        "\n".join(
            (
                _tick_row("20120201 000000000"),
                _tick_row("20120201 000100000", volume=17),
                _tick_row("20120201 000200000", volume=2),
                _tick_row("20120201 000500000", volume=3),
                _tick_row("20120201 000800000", volume=4),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    discovery = discover_quality_targets((path,))
    tolerance = HistDataGapTolerance(
        suspicious_gap_ms=60_000,
        dynamic_window_initial_ms=60_000,
        dynamic_window_max_ms=4 * 60_000,
    )

    report = run_quality_assessment(
        discovery.targets,
        (HistDataAsciiTimestampGapRule(tolerance=tolerance),),
    )

    summary = _finding(report.findings, "ASCII_TIMESTAMP_GAP_SUMMARY")
    gap = _finding(report.findings, "ASCII_TIMESTAMP_SUSPICIOUS_GAP")
    samples = gap.metadata["samples"]
    assert summary.metadata["dynamic_window_policy"] == "inverted-tcp-backoff"
    assert summary.metadata["suspicious_gap_count"] == 2
    assert summary.metadata["dynamic_gap_score"] == 9.0
    assert samples[0]["dynamic_window_ms"] == 240_000
    assert samples[0]["dynamic_score_increment"] == 3.0
    assert samples[1]["dynamic_window_ms"] == 120_000
    assert samples[1]["dynamic_score_increment"] == 6.0


def test_cross_file_continuity_clean_adjacent_months(
    tmp_path: Path,
) -> None:
    """Adjacent monthly files should compare clean boundary timestamps."""
    _write_tick_file(tmp_path, "201202", ("20120229 235900000",))
    _write_tick_file(tmp_path, "201203", ("20120301 000000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.rule_id == ASCII_TIMESTAMP_CONTINUITY_RULE_ID
    assert summary.metadata["adjacent_pair_count"] == 1
    assert summary.metadata["clean_boundary_count"] == 1
    assert summary.metadata["missing_period_count"] == 0
    assert summary.metadata["suspicious_gap_count"] == 0


def test_cross_file_continuity_reports_missing_next_month(
    tmp_path: Path,
) -> None:
    """Observed month gaps should name the skipped period and both files."""
    previous = _write_tick_file(tmp_path, "201202", ("20120229 235900000",))
    current = _write_tick_file(tmp_path, "201204", ("20120401 000000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    missing = _finding(
        report.findings,
        "ASCII_TIMESTAMP_CONTINUITY_PERIOD_MISSING",
    )
    sample = missing.metadata["samples"][0]
    assert report.status is QualityStatus.WARNING
    assert missing.severity is QualitySeverity.WARNING
    assert missing.location.path == str(current.resolve())
    assert missing.location.metadata["previous_path"] == str(previous.resolve())
    assert missing.location.metadata["current_path"] == str(current.resolve())
    assert missing.location.metadata["missing_periods"] == ["201203"]
    assert sample["missing_periods"] == ["201203"]
    assert summary.metadata["missing_period_count"] == 1
    assert summary.metadata["period_gap_count"] == 1
    assert summary.metadata["adjacent_pair_count"] == 0


def test_cross_file_continuity_reports_duplicate_overlap(
    tmp_path: Path,
) -> None:
    """Repeated boundary timestamps should be reported across files."""
    previous = _write_tick_file(tmp_path, "201202", ("20120301 000000000",))
    current = _write_tick_file(tmp_path, "201203", ("20120301 000000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    overlap = _finding(
        report.findings,
        "ASCII_TIMESTAMP_CONTINUITY_DUPLICATE_OVERLAP",
    )
    assert report.status is QualityStatus.WARNING
    assert overlap.location.metadata["classification"] == "duplicate_overlap"
    assert overlap.location.metadata["gap_ms"] == 0
    assert overlap.metadata["samples"][0]["previous"]["path"] == (
        str(previous.resolve())
    )
    assert overlap.metadata["samples"][0]["current"]["path"] == (
        str(current.resolve())
    )
    assert summary.metadata["duplicate_overlap_count"] == 1


def test_cross_file_continuity_reports_reversed_file_ordering(
    tmp_path: Path,
) -> None:
    """A next file that starts before the previous file ends should warn."""
    _write_tick_file(tmp_path, "201202", ("20120301 001000000",))
    _write_tick_file(tmp_path, "201203", ("20120301 000000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    reversed_order = _finding(
        report.findings,
        "ASCII_TIMESTAMP_CONTINUITY_REVERSED_ORDER",
    )
    assert report.status is QualityStatus.WARNING
    assert reversed_order.location.metadata["classification"] == (
        "reversed_order"
    )
    assert reversed_order.location.metadata["gap_ms"] == -600_000
    assert summary.metadata["reversed_order_count"] == 1


def test_cross_file_continuity_reports_suspicious_boundary_gap(
    tmp_path: Path,
) -> None:
    """Adjacent files with large non-session boundary gaps should warn."""
    _write_tick_file(tmp_path, "201202", ("20120229 235000000",))
    _write_tick_file(tmp_path, "201203", ("20120301 001000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    gap = _finding(
        report.findings,
        "ASCII_TIMESTAMP_CONTINUITY_SUSPICIOUS_GAP",
    )
    assert report.status is QualityStatus.WARNING
    assert gap.location.metadata["classification"] == "suspicious_gap"
    assert gap.location.metadata["gap_ms"] == 20 * 60_000
    assert summary.metadata["suspicious_gap_count"] == 1


def test_cross_file_continuity_allows_expected_session_closure(
    tmp_path: Path,
) -> None:
    """Month boundaries crossing FX weekend closure should remain clean."""
    _write_tick_file(tmp_path, "201203", ("20120330 165900000",))
    _write_tick_file(tmp_path, "201204", ("20120401 170100000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    closure = _finding(
        report.findings,
        "ASCII_TIMESTAMP_CONTINUITY_EXPECTED_SESSION_CLOSURE",
    )
    assert report.status is QualityStatus.CLEAN
    assert closure.severity is QualitySeverity.INFO
    assert closure.location.metadata["classification"] == (
        "expected_session_closure"
    )
    assert summary.metadata["expected_session_closure_count"] == 1
    assert summary.metadata["suspicious_gap_count"] == 0


def test_cross_file_continuity_handles_adjacent_annual_tick_files(
    tmp_path: Path,
) -> None:
    """Annual tick files should participate in boundary continuity checks."""
    _write_tick_file(tmp_path, "2012", ("20121231 235900000",))
    _write_tick_file(tmp_path, "2013", ("20130101 000000000",))

    report = _continuity_report_for_path(tmp_path)

    summary = _finding(report.findings, "ASCII_TIMESTAMP_CONTINUITY_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["adjacent_pair_count"] == 1
    assert summary.metadata["clean_boundary_count"] == 1


def test_time_rules_reuse_timestamp_scan_per_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The three per-target time rules should share one parsed scan."""
    path = write_ascii_case(tmp_path, CLEAN_TICK_CASE)
    time_quality.clear_timestamp_scan_caches()
    calls = 0
    original = time_quality._scan_timestamp_rows

    def counting_scan(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(time_quality, "_scan_timestamp_rows", counting_scan)

    report = _report_for_path(path)

    assert report.status is QualityStatus.CLEAN
    assert calls == 1
    time_quality.clear_timestamp_scan_caches()


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("time",)),
    )


def _continuity_report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        (),
        run_rules=(HistDataAsciiTimestampContinuityRule(),),
    )


def _write_tick_file(
    directory: Path,
    period: str,
    timestamps: tuple[str, ...],
) -> Path:
    path = directory / f"DAT_ASCII_EURUSD_T_{period}.csv"
    path.write_text(
        "\n".join(_tick_row(timestamp) for timestamp in timestamps) + "\n",
        encoding="utf-8",
    )
    return path


def _tick_row(timestamp: str, *, volume: int = 0) -> str:
    return f"{timestamp},1.306600,1.306770,{volume}"


def _finding(findings, code: str):
    matches = tuple(finding for finding in findings if finding.code == code)
    assert len(matches) == 1
    return matches[0]


def _findings(findings, code: str):
    return tuple(finding for finding in findings if finding.code == code)
