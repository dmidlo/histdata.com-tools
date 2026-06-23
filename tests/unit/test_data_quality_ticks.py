"""Tests for tick bid/ask spread data-quality rules."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.data_quality import (
    ASCII_TICK_MICROSTRUCTURE_RULE_ID,
    ASCII_TICK_SPREAD_REGIME_RULE_ID,
    ASCII_TICK_SPREAD_RULE_ID,
    HistDataAsciiTickMicrostructureRule,
    HistDataAsciiTickSpreadRegimeRule,
    HistDataAsciiTickSpreadRule,
    HistDataTickMicrostructureThresholds,
    HistDataTickSpreadRegimeThresholds,
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
        ASCII_TICK_MICROSTRUCTURE_RULE_ID,
        ASCII_TICK_SPREAD_REGIME_RULE_ID,
    ]


def test_clean_tick_file_reports_spread_summary(
    tmp_path: Path,
) -> None:
    """Clean tick bid/ask rows should pass with spread profile metadata."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_SUMMARY")
    microstructure = _finding(
        report.findings,
        "ASCII_TICK_MICROSTRUCTURE_SUMMARY",
    )
    regimes = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "ASCII_TICK_SPREAD_SUMMARY",
        "ASCII_TICK_MICROSTRUCTURE_SUMMARY",
        "ASCII_TICK_SPREAD_REGIME_SUMMARY",
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
    assert microstructure.rule_id == ASCII_TICK_MICROSTRUCTURE_RULE_ID
    assert microstructure.metadata["duplicate_row_count"] == 0
    assert microstructure.metadata["stale_quote_run_count"] == 0
    assert microstructure.metadata["burst_run_count"] == 0
    assert microstructure.metadata["one_sided_run_count"] == 0
    assert regimes.rule_id == ASCII_TICK_SPREAD_REGIME_RULE_ID
    assert regimes.metadata["profiled_row_count"] == 3
    assert regimes.metadata["baseline_profile"]["source"] == (
        "liquid_session_non_special"
    )
    assert regimes.metadata["symbol_spread_profiles"]["EURUSD"]["count"] == 3
    assert regimes.metadata["source_hour_spread_profiles"]["00"]["count"] == 3
    assert regimes.metadata["wide_spread_count"] == 0
    assert regimes.metadata["spread_jump_count"] == 0
    assert regimes.metadata["regime_shift_count"] == 0


def test_m1_files_are_ignored_by_tick_spread_rule(
    tmp_path: Path,
) -> None:
    """M1 bars are owned by the bars quality group, not tick rules."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


def test_tick_spread_regime_profiles_symbol_hour_session_and_rollover(
    tmp_path: Path,
) -> None:
    """Spread regime reports should bucket by symbol, hour, and session."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_spread_regime_buckets",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_REGIMES.csv",
            rows=(
                "20120201 000000000,1.000000,1.000100,0",
                "20120201 030000000,1.000000,1.000200,0",
                "20120201 080000000,1.000000,1.000300,0",
                "20120205 170000000,1.000000,1.000400,0",
                "20120203 165900000,1.000000,1.000500,0",
            ),
        ),
    )
    report = _report_for_path(
        path,
        rules=(
            HistDataAsciiTickSpreadRegimeRule(
                thresholds=HistDataTickSpreadRegimeThresholds(
                    wide_spread_multiplier=99.0,
                    jump_spread_multiplier=99.0,
                    regime_median_multiplier=99.0,
                )
            ),
        ),
    )

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["profiled_row_count"] == 5
    assert summary.metadata["symbol_spread_profiles"]["EURUSD"]["count"] == 5
    assert summary.metadata["source_hour_spread_profiles"]["00"][
        "median_spread"
    ] == pytest.approx(0.0001)
    assert summary.metadata["source_hour_spread_profiles"]["17"]["count"] == 1
    assert summary.metadata["session_spread_profiles"]["asia"]["count"] == 2
    assert summary.metadata["session_spread_profiles"]["london"]["count"] == 2
    assert summary.metadata["session_spread_profiles"]["new_york"]["count"] == 1
    assert (
        summary.metadata["session_spread_profiles"]["market_closed"]["count"]
        == 2
    )
    special = summary.metadata["special_regime_spread_profiles"]
    assert special["daily_rollover"]["count"] == 2
    assert special["sunday_open"]["count"] == 1
    assert special["friday_close"]["count"] == 1
    assert summary.metadata["baseline_profile"]["median_spread"] == (
        pytest.approx(0.0002)
    )


def test_tick_spread_regime_warns_for_outliers_and_spread_jumps(
    tmp_path: Path,
) -> None:
    """Wide spreads and abrupt spread jumps should warn by default."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_spread_regime_outlier",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_SPREAD_OUTLIER.csv",
            rows=(
                "20120201 000000000,1.000000,1.000100,0",
                "20120201 000100000,1.000000,1.000100,0",
                "20120201 000200000,1.000000,1.001000,0",
                "20120201 000300000,1.000000,1.000100,0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_SUMMARY")
    wide = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_WIDE_SPREAD")
    jump = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_JUMP")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["wide_spread_count"] == 1
    assert summary.metadata["spread_jump_count"] == 2
    assert summary.metadata["wide_spread_threshold"] == pytest.approx(0.0003)
    assert summary.metadata["spread_jump_threshold"] == pytest.approx(0.0002)
    assert wide.severity is QualitySeverity.WARNING
    assert wide.location.row_number == 3
    assert wide.location.metadata["values"]["spread"] == pytest.approx(0.001)
    assert jump.severity is QualitySeverity.WARNING
    assert jump.metadata["row_count"] == 2
    assert jump.metadata["samples"][0]["previous"]["row_number"] == 2


def test_tick_spread_regime_warns_when_special_regimes_widen(
    tmp_path: Path,
) -> None:
    """Sunday open, Friday close, and rollover regimes should compare separately."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_spread_regime_shift",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_REGIME_SHIFT.csv",
            rows=(
                "20120201 000000000,1.000000,1.000100,0",
                "20120201 010000000,1.000000,1.000100,0",
                "20120205 170000000,1.000000,1.000500,0",
                "20120203 165900000,1.000000,1.000500,0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_SUMMARY")
    shift = _finding(report.findings, "ASCII_TICK_SPREAD_REGIME_SHIFT")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["regime_shift_count"] == 3
    assert summary.metadata["regime_median_threshold"] == pytest.approx(0.0002)
    special = summary.metadata["special_regime_spread_profiles"]
    assert special["daily_rollover"]["median_spread"] == pytest.approx(0.0005)
    assert special["sunday_open"]["median_spread"] == pytest.approx(0.0005)
    assert special["friday_close"]["median_spread"] == pytest.approx(0.0005)
    assert shift.severity is QualitySeverity.WARNING
    assert shift.metadata["row_count"] == 3
    assert {sample["profile_key"] for sample in shift.metadata["samples"]} == {
        "daily_rollover",
        "friday_close",
        "sunday_open",
    }


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


def test_tick_microstructure_summarizes_duplicate_rows_without_warning(
    tmp_path: Path,
) -> None:
    """Exact duplicate tick findings stay owned by the timestamp rule."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("tick_duplicate_row")),
        rules=(HistDataAsciiTickMicrostructureRule(),),
    )

    summary = _finding(report.findings, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["duplicate_row_count"] == 1
    assert summary.metadata["duplicate_detection"] == {
        "summary_only": True,
        "owner_rule_id": "time.ascii.timestamp_sequence",
        "owner_finding_code": "ASCII_TICK_DUPLICATE_ROW",
    }
    duplicate = summary.metadata["duplicate_samples"][0]
    assert duplicate["row_number"] == 2
    assert duplicate["metadata"]["duplicate_of_row"] == 1
    assert duplicate["metadata"]["dedupe_policy"] == "summary-only"


def test_tick_microstructure_detects_stale_quote_runs(
    tmp_path: Path,
) -> None:
    """Repeated bid/ask quotes inside the active gap window should warn."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_stale_quote_run",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_STALE.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000004660,1.306600,1.306770,25",
                "20120201 000005660,1.306600,1.306770,25",
                "20120201 000006660,1.306610,1.306780,25",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_STALE_QUOTE_RUN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["stale_quote_repeat_count"] == 2
    assert summary.metadata["stale_quote_run_count"] == 1
    assert summary.metadata["stale_quote_run_row_count"] == 3
    assert finding.location.row_number == 1
    assert finding.location.timestamp_source == "20120201 000003660"
    assert finding.location.metadata["run_length"] == 3
    assert finding.metadata["samples"][0]["run_end_row_number"] == 3


def test_tick_microstructure_detects_tick_bursts(
    tmp_path: Path,
) -> None:
    """Dense timestamp clusters should warn as possible batched ticks."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_burst",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_BURST.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000003700,1.306610,1.306780,25",
                "20120201 000003750,1.306620,1.306790,25",
                "20120201 000014990,1.306630,1.306800,25",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_BURST_RUN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["burst_interval_count"] == 2
    assert summary.metadata["burst_run_count"] == 1
    assert summary.metadata["burst_tick_count"] == 3
    assert finding.location.row_number == 1
    assert finding.location.column == "datetime"
    assert finding.location.metadata["run_length"] == 3
    assert finding.metadata["samples"][0]["metadata"]["duration_ms"] == 90


def test_tick_microstructure_detects_one_sided_quote_runs(
    tmp_path: Path,
) -> None:
    """Bid-only or ask-only quote movement should be profiled."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_one_sided_bid",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_ONE_SIDED.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000004660,1.306610,1.306770,25",
                "20120201 000005660,1.306620,1.306770,25",
                "20120201 000006660,1.306630,1.306790,25",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    finding = _finding(report.findings, "ASCII_TICK_ONE_SIDED_QUOTE_RUN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["one_sided_movement_count"] == 2
    assert summary.metadata["one_sided_run_count"] == 1
    assert summary.metadata["bid_only_movement_count"] == 2
    assert summary.metadata["ask_only_movement_count"] == 0
    assert finding.location.row_number == 2
    assert finding.location.metadata["direction"] == "bid_only"
    assert finding.location.metadata["previous_row_number"] == 1
    assert finding.metadata["samples"][0]["run_end_row_number"] == 3


def test_tick_microstructure_thresholds_are_symbol_session_configurable(
    tmp_path: Path,
) -> None:
    """Symbol/session profiles should override default warning thresholds."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_one_sided_below_session_threshold",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_ONE_SIDED_OK.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000004660,1.306610,1.306770,25",
                "20120201 000005660,1.306620,1.306770,25",
            ),
        ),
    )
    report = _report_for_path(
        path,
        rules=(
            HistDataAsciiTickMicrostructureRule(
                session_name="rollover",
                thresholds_by_symbol_session={
                    "EURUSD:rollover": HistDataTickMicrostructureThresholds(
                        one_sided_run_length=3,
                    )
                },
            ),
        ),
    )

    summary = _finding(report.findings, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["one_sided_movement_count"] == 2
    assert summary.metadata["one_sided_run_count"] == 0
    assert summary.metadata["threshold_profile"]["source"] == "symbol-session"
    assert summary.metadata["threshold_profile"]["profile_key"] == (
        "EURUSD:rollover"
    )


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
