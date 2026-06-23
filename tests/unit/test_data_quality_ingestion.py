"""Tests for raw text ingestion data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_TEXT_INGESTION_RULE_ID,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_M1_ROWS,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_ingestion_group_registers_text_rule() -> None:
    """The advertised ingestion group should execute concrete checks."""
    assert [
        rule.rule_id for rule in quality_rules_for_groups(("ingestion",))
    ] == [ASCII_TEXT_INGESTION_RULE_ID]


def test_clean_ascii_file_passes_ingestion_text_checks(
    tmp_path: Path,
) -> None:
    """Clean headerless HistData ASCII should pass raw text checks."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


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
