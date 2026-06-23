"""Tests for raw text ingestion data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_ROW_COUNT_INGESTION_RULE_ID,
    ASCII_SCHEMA_INGESTION_RULE_ID,
    ASCII_TEXT_INGESTION_RULE_ID,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    M1,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_M1_ROWS,
    CLEAN_TICK_CASE,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_ingestion_group_registers_text_rule() -> None:
    """The advertised ingestion group should execute concrete checks."""
    assert [
        rule.rule_id for rule in quality_rules_for_groups(("ingestion",))
    ] == [
        ASCII_ROW_COUNT_INGESTION_RULE_ID,
        ASCII_TEXT_INGESTION_RULE_ID,
        ASCII_SCHEMA_INGESTION_RULE_ID,
    ]


def test_clean_ascii_file_passes_ingestion_text_checks(
    tmp_path: Path,
) -> None:
    """Clean headerless HistData ASCII should pass raw text checks."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "ASCII_ROW_COUNT_SUMMARY"
    ]
    summary = _finding(report.findings, "ASCII_ROW_COUNT_SUMMARY")
    assert summary.severity is QualitySeverity.INFO
    assert summary.rule_id == ASCII_ROW_COUNT_INGESTION_RULE_ID
    assert summary.metadata["row_count"] == 3
    assert summary.metadata["line_count"] == 3
    assert summary.metadata["payload_size_bytes"] > 80
    assert summary.metadata["container_size_bytes"] > 80
    assert summary.metadata["symbol"] == "EURUSD"
    assert summary.metadata["timeframe"] == "M1"
    assert summary.metadata["period"] == "201202"


def test_clean_m1_and_tick_files_pass_strict_schema_checks(
    tmp_path: Path,
) -> None:
    """Both supported ASCII layouts should parse through quality mode."""
    m1_report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))
    tick_report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    assert m1_report.status is QualityStatus.CLEAN
    assert tick_report.status is QualityStatus.CLEAN
    assert _non_info_codes(m1_report.findings) == []
    assert _non_info_codes(tick_report.findings) == []
    assert (
        _finding(m1_report.findings, "ASCII_ROW_COUNT_SUMMARY").metadata[
            "row_count"
        ]
        == 3
    )
    assert (
        _finding(tick_report.findings, "ASCII_ROW_COUNT_SUMMARY").metadata[
            "row_count"
        ]
        == 3
    )


def test_zip_csv_member_reports_row_count_and_member_size(
    tmp_path: Path,
) -> None:
    """ZIP reports should expose row count for the CSV member payload."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )

    report = _report_for_path(archive)

    finding = _finding(report.findings, "ASCII_ROW_COUNT_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert finding.metadata["row_count"] == 3
    assert finding.metadata["source_member"] == CLEAN_M1_CASE.filename
    assert finding.metadata["payload_size_bytes"] > 80
    assert (
        finding.metadata["container_size_bytes"]
        >= finding.metadata["payload_size_bytes"]
    )


def test_empty_ascii_file_is_a_hard_ingestion_failure(
    tmp_path: Path,
) -> None:
    """Zero-row HistData ASCII files should fail before later checks."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_empty_file"))
    )

    finding = _finding(report.findings, "ASCII_FILE_EMPTY")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.rule_id == ASCII_ROW_COUNT_INGESTION_RULE_ID
    assert finding.metadata["row_count"] == 0
    assert finding.metadata["payload_size_bytes"] == 0


def test_tiny_ascii_file_warns_with_configured_thresholds(
    tmp_path: Path,
) -> None:
    """Tiny valid files should be visible without requiring baselines."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_tiny_file",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_TINY.csv",
            rows=(CLEAN_M1_ROWS[0],),
        ),
    )

    report = _report_for_path(path)

    tiny_rows = _finding(report.findings, "ASCII_FILE_TINY")
    tiny_size = _finding(report.findings, "ASCII_FILE_SIZE_TINY")
    assert report.status is QualityStatus.WARNING
    assert tiny_rows.severity is QualitySeverity.WARNING
    assert tiny_rows.metadata["row_count"] == 1
    assert tiny_rows.metadata["minimum_row_count"] == 2
    assert tiny_size.metadata["payload_size_bytes"] < 60
    assert tiny_size.metadata["minimum_size_bytes"] == 60


def test_missing_final_line_ending_warns_as_possible_truncation(
    tmp_path: Path,
) -> None:
    """A non-empty text payload lacking a final terminator is suspicious."""
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202_TRUNCATED.csv"
    path.write_text("\n".join(CLEAN_M1_ROWS[:2]), encoding="utf-8")

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_FILE_TRUNCATED")
    assert report.status is QualityStatus.WARNING
    assert finding.metadata["row_count"] == 2
    assert finding.metadata["final_line_terminated"] is False


def test_cache_target_reports_row_count_size_schema_and_bounds(
    tmp_path: Path,
) -> None:
    """Readable Polars cache files should expose equivalent metadata."""
    cache_path = tmp_path / CACHE_FILENAME
    batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)

    report = _report_for_path(cache_path)

    finding = _finding(report.findings, "ASCII_ROW_COUNT_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert finding.metadata["kind"] == "cache"
    assert finding.metadata["row_count"] == 3
    assert finding.metadata["line_count"] == 3
    assert finding.metadata["payload_size_bytes"] == cache_path.stat().st_size
    assert finding.metadata["schema"]["datetime"] == "Int64"
    assert finding.metadata["start_timestamp_utc_ms"] == batch.summary.start
    assert finding.metadata["end_timestamp_utc_ms"] == batch.summary.end


def test_malformed_rows_are_counted_without_aborting_run(
    tmp_path: Path,
) -> None:
    """Wrong field counts should be reported with bounded row samples."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_malformed_row"))
    )

    finding = _finding(report.findings, "ASCII_ROW_FIELD_COUNT_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["expected_field_count"] == 6
    assert finding.metadata["row_count"] == 1
    assert finding.metadata["samples"] == [
        {
            "row_number": 2,
            "field_count": 3,
            "raw": "20120201 000100;1.306570;1.306570",
        }
    ]
    assert finding.location.row_number == 2


def test_bad_timestamp_rows_fail_schema_checks(
    tmp_path: Path,
) -> None:
    """Timestamps should use strict HistData source parsing."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_bad_timestamp"))
    )

    finding = _finding(report.findings, "ASCII_TIMESTAMP_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.location.row_number == 2
    assert finding.location.column == "datetime"
    assert finding.metadata["source_timezone"] == "EST-no-DST"
    assert finding.metadata["samples"][0]["raw_value"] == "20120230 000000"


def test_bad_numeric_rows_fail_schema_checks(
    tmp_path: Path,
) -> None:
    """Price columns should reject strings and non-finite values."""
    m1_report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_bad_numeric"))
    )
    tick_report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_bad_numeric"))
    )

    m1_finding = _finding(m1_report.findings, "ASCII_NUMERIC_INVALID")
    tick_finding = _finding(tick_report.findings, "ASCII_NUMERIC_INVALID")
    assert m1_finding.location.column == "open"
    assert m1_finding.metadata["price_columns"] == [
        "open",
        "high",
        "low",
        "close",
    ]
    assert "ask" not in m1_finding.metadata["price_columns"]
    assert m1_finding.metadata["samples"][0]["raw_value"] == "$1.306570"
    assert tick_finding.location.column == "bid"
    assert tick_finding.metadata["price_columns"] == ["bid", "ask"]
    assert tick_finding.metadata["samples"][0]["raw_value"] == "inf"


def test_shifted_column_rows_are_reported_once(
    tmp_path: Path,
) -> None:
    """Rows with exact field counts but shifted values need clear findings."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_shifted_column"))
    )

    finding = _finding(report.findings, "ASCII_ROW_SCHEMA_SHIFTED")
    assert report.status is QualityStatus.FAILED
    assert finding.location.row_number == 2
    assert finding.location.column == "datetime"
    assert finding.rule_id == ASCII_SCHEMA_INGESTION_RULE_ID
    assert finding.metadata["columns"] == ["datetime", "bid", "ask", "vol"]
    assert _non_info_codes(report.findings) == ["ASCII_ROW_SCHEMA_SHIFTED"]


def test_bad_volume_rows_fail_schema_checks_without_rejecting_zero_volume(
    tmp_path: Path,
) -> None:
    """Volume is uninformative for FX but still has strict dtype intent."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_bad_volume"))
    )

    finding = _finding(report.findings, "ASCII_VOLUME_INVALID")
    assert finding.location.column == "vol"
    assert finding.metadata["zero_volume_allowed"] is True
    assert finding.metadata["structurally_uninformative"] is True
    assert finding.metadata["max_value"] == 2147483647
    assert finding.metadata["samples"][0]["raw_value"] == "2147483648"


def test_delimiter_mismatch_is_actionable_and_still_counts_fields(
    tmp_path: Path,
) -> None:
    """Wrong dialect delimiters should be distinct from generic row shape."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_bad_delimiter"))
    )

    delimiter = _finding(report.findings, "ASCII_DELIMITER_MISMATCH")
    field_count = _finding(report.findings, "ASCII_ROW_FIELD_COUNT_INVALID")
    assert delimiter.metadata["expected_delimiter"] == ";"
    assert delimiter.metadata["suspect_delimiter"] == ","
    assert delimiter.metadata["row_count"] == 1
    assert field_count.metadata["expected_field_count"] == 6
    assert field_count.metadata["samples"][0]["field_count"] == 1


def test_header_rows_are_reported_explicitly(
    tmp_path: Path,
) -> None:
    """HistData ASCII files are headerless; accidental headers should fail."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_header_row"))
    )

    finding = _finding(report.findings, "ASCII_HEADER_ROW_PRESENT")
    assert finding.location.row_number == 1
    assert finding.metadata["expected_headerless"] is True
    assert finding.metadata["columns"] == ["datetime", "bid", "ask", "vol"]


def test_encoding_failures_are_reported_without_row_scanning(
    tmp_path: Path,
) -> None:
    """Invalid UTF-8 should produce an encoding finding and stop text parse."""
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202.csv"
    path.write_bytes(CLEAN_M1_ROWS[0].encode("utf-8") + b"\xff\n")

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_TEXT_ENCODING_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.metadata["encoding"] == "utf-8"
    assert "UnicodeDecodeError" not in finding.message
    assert "byte_start" in finding.metadata


def test_line_ending_findings_include_counts(
    tmp_path: Path,
) -> None:
    """Mixed line endings and bare CR terminators should be visible."""
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202.csv"
    path.write_bytes(
        b"\r\n".join((CLEAN_M1_ROWS[0].encode(), CLEAN_M1_ROWS[1].encode()))
        + b"\n"
        + CLEAN_M1_ROWS[2].encode()
        + b"\r"
    )

    report = _report_for_path(path)

    inconsistent = _finding(
        report.findings,
        "ASCII_LINE_ENDINGS_INCONSISTENT",
    )
    malformed = _finding(report.findings, "ASCII_LINE_ENDINGS_MALFORMED")
    assert report.status is QualityStatus.WARNING
    assert inconsistent.severity is QualitySeverity.WARNING
    assert inconsistent.metadata["line_endings"] == {
        "lf": 1,
        "crlf": 1,
        "cr": 1,
    }
    assert malformed.metadata["line_endings"]["cr"] == 1


def test_zip_csv_member_is_scanned_for_ingestion_findings(
    tmp_path: Path,
) -> None:
    """ZIP targets should scan their single CSV payload when available."""
    archive = write_zip_case(
        tmp_path,
        case_by_name("tick_bad_delimiter"),
        zip_filename="DAT_ASCII_EURUSD_T_201202.zip",
    )

    report = _report_for_path(archive)

    finding = _finding(report.findings, "ASCII_DELIMITER_MISMATCH")
    assert finding.metadata["expected_delimiter"] == ","
    assert finding.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_T_201202_BAD_DELIMITER.csv"
    )
    assert finding.location.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_T_201202_BAD_DELIMITER.csv"
    )


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("ingestion",)),
    )


def _finding(findings, code: str):
    matches = tuple(finding for finding in findings if finding.code == code)
    assert len(matches) == 1
    return matches[0]


def _non_info_codes(findings) -> list[str]:
    return [
        finding.code
        for finding in findings
        if finding.severity is not QualitySeverity.INFO
    ]
