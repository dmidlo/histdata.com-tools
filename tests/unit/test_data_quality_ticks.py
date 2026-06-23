"""Tests for tick bid/ask spread data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_TICK_SPREAD_RULE_ID,
    HistDataAsciiTickSpreadRule,
    HistDataTickSpreadThresholds,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import TICK
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_TICK_CASE,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_ticks_group_registers_spread_rule() -> None:
    """The advertised ticks group should execute spread checks."""
    assert [rule.rule_id for rule in quality_rules_for_groups(("ticks",))] == [
        ASCII_TICK_SPREAD_RULE_ID,
    ]


def test_clean_tick_file_reports_spread_summary(
    tmp_path: Path,
) -> None:
    """Clean tick bid/ask rows should pass with spread profile metadata."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "ASCII_TICK_SPREAD_SUMMARY",
    ]
    assert summary.rule_id == ASCII_TICK_SPREAD_RULE_ID
    assert summary.severity is QualitySeverity.INFO
    assert summary.metadata["row_count"] == 3
    assert summary.metadata["parsed_row_count"] == 3
    assert summary.metadata["missing_bid_ask_count"] == 0
    assert summary.metadata["invalid_bid_ask_count"] == 0
    assert summary.metadata["negative_spread_count"] == 0
    assert summary.metadata["zero_spread_count"] == 0
    assert summary.metadata["zero_spread_run_count"] == 0
    assert summary.metadata["spread_definition"] == "ask - bid"
    assert summary.metadata["price_columns"] == ["bid", "ask"]
    assert summary.metadata["min_spread"] > 0.0
    assert summary.metadata["max_spread"] > 0.0


def test_m1_files_are_ignored_by_tick_spread_rule(
    tmp_path: Path,
) -> None:
    """M1 bars are owned by the bars quality group, not tick rules."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


def test_negative_tick_spread_is_a_hard_failure(
    tmp_path: Path,
) -> None:
    """Rows where ask < bid should fail with bid/ask row context."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_negative_spread"))
    )

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_NEGATIVE_SPREAD")
    assert report.status is QualityStatus.FAILED
    assert summary.metadata["negative_spread_count"] == 1
    assert summary.metadata["min_spread"] < 0.0
    assert finding.severity is QualitySeverity.ERROR
    assert finding.location.row_number == 2
    assert finding.location.column == "ask"
    assert finding.location.timestamp_source == "20120201 000003973"
    assert finding.location.timestamp_utc_ms == 1328072403973
    assert finding.location.metadata["values"]["bid"] == 1.3068
    assert finding.location.metadata["values"]["ask"] == 1.30675
    assert finding.metadata["samples"][0]["values"]["spread"] < 0.0


def test_zero_spread_runs_warn_by_default(
    tmp_path: Path,
) -> None:
    """Zero-spread tick runs should warn without hard-failing ingestion."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_zero_spread_run",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_ZERO_SPREAD.csv",
            rows=(
                "20120201 000003660,1.306600,1.306600,0",
                "20120201 000003973,1.306580,1.306580,25",
                "20120201 000014990,1.306570,1.306740,0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_ZERO_SPREAD_RUN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["zero_spread_count"] == 2
    assert summary.metadata["zero_spread_run_count"] == 1
    assert finding.severity is QualitySeverity.WARNING
    assert finding.location.row_number == 1
    assert finding.location.metadata["run_length"] == 2
    assert finding.metadata["row_count"] == 1
    assert finding.metadata["zero_spread_count"] == 2
    assert finding.metadata["samples"][0]["run_end_row_number"] == 2


def test_zero_spread_run_threshold_is_configurable(
    tmp_path: Path,
) -> None:
    """Operators should be able to require longer zero-spread runs."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_zero_spread_below_threshold",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_ZERO_SPREAD_OK.csv",
            rows=(
                "20120201 000003660,1.306600,1.306600,0",
                "20120201 000003973,1.306580,1.306580,25",
            ),
        ),
    )
    report = _report_for_path(
        path,
        rules=(
            HistDataAsciiTickSpreadRule(
                thresholds=HistDataTickSpreadThresholds(
                    zero_spread_run_length=3,
                )
            ),
        ),
    )

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["zero_spread_count"] == 2
    assert summary.metadata["zero_spread_run_count"] == 0


def test_missing_tick_bid_or_ask_field_is_schema_error(
    tmp_path: Path,
) -> None:
    """Tick rows need both bid and ask for spread checks."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_missing_ask",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_MISSING_ASK.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000003973,1.306580,,25",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_BID_ASK_MISSING")
    assert report.status is QualityStatus.FAILED
    assert summary.metadata["missing_bid_ask_count"] == 1
    assert finding.severity is QualitySeverity.ERROR
    assert finding.location.row_number == 2
    assert finding.location.column == "ask"
    assert finding.location.metadata["required_columns"] == ["bid", "ask"]
    assert finding.metadata["samples"][0]["raw_values"] == [
        "20120201 000003973",
        "1.306580",
        "",
        "25",
    ]


def test_invalid_tick_bid_or_ask_field_is_schema_error(
    tmp_path: Path,
) -> None:
    """Tick spread checks should validate bid/ask parseability directly."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_bad_numeric"))
    )

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_BID_ASK_INVALID")
    assert report.status is QualityStatus.FAILED
    assert summary.metadata["invalid_bid_ask_count"] == 1
    assert finding.location.row_number == 2
    assert finding.location.column == "bid"
    assert finding.location.metadata["raw_bid"] == "inf"
    assert finding.metadata["samples"][0]["metadata"]["raw_ask"] == "1.306750"


def test_zip_csv_member_is_scanned_for_tick_spread_findings(
    tmp_path: Path,
) -> None:
    """ZIP targets should scan their single CSV payload for tick spreads."""
    archive = write_zip_case(
        tmp_path,
        case_by_name("tick_negative_spread"),
        zip_filename="DAT_ASCII_EURUSD_T_201202.zip",
    )

    report = _report_for_path(archive)

    finding = _finding(report.findings, "ASCII_TICK_NEGATIVE_SPREAD")
    assert report.status is QualityStatus.FAILED
    assert finding.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_T_201202_NEGATIVE_SPREAD.csv"
    )
    assert finding.location.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_T_201202_NEGATIVE_SPREAD.csv"
    )


def _report_for_path(path: Path, *, rules=None):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("ticks",)) if rules is None else rules,
    )


def _finding(findings, code: str):
    matches = tuple(finding for finding in findings if finding.code == code)
    assert len(matches) == 1
    return matches[0]
