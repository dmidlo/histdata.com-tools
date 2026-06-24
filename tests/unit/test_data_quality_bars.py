"""Tests for M1 bar data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_M1_BAR_INTEGRITY_RULE_ID,
    ASCII_M1_OUTLIER_RULE_ID,
    ASCII_M1_PRECISION_RULE_ID,
    ASCII_M1_TICK_RECONSTRUCTION_RULE_ID,
    ASSET_CLASS_FX,
    ASSET_CLASS_INDEX,
    ASSET_CLASS_METAL,
    ASSET_CLASS_OIL,
    ASSET_CLASS_UNKNOWN,
    HistDataAsciiM1OutlierRule,
    HistDataAsciiM1TickReconstructionRule,
    HistDataM1OutlierThresholds,
    HistDataM1TickReconstructionTolerance,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
    symbol_metadata_for,
)
from histdatacom.histdata_ascii import M1, TICK
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_TICK_CASE,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_bars_group_registers_m1_bar_integrity_rule() -> None:
    """The advertised bars group should execute concrete M1 checks."""
    assert [rule.rule_id for rule in quality_rules_for_groups(("bars",))] == [
        ASCII_M1_BAR_INTEGRITY_RULE_ID,
        ASCII_M1_PRECISION_RULE_ID,
        ASCII_M1_OUTLIER_RULE_ID,
    ]
    assert [
        rule.rule_id for rule in quality_run_rules_for_groups(("bars",))
    ] == [ASCII_M1_TICK_RECONSTRUCTION_RULE_ID]


def test_clean_m1_file_passes_bar_integrity_checks(
    tmp_path: Path,
) -> None:
    """Clean M1 bid OHLC rows should pass with a summary profile."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    summary = _finding(report.findings, "ASCII_M1_OHLC_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "ASCII_M1_OHLC_SUMMARY",
        "ASCII_M1_PRECISION_SUMMARY",
        "ASCII_M1_OUTLIER_SUMMARY",
    ]
    assert summary.severity is QualitySeverity.INFO
    assert summary.rule_id == ASCII_M1_BAR_INTEGRITY_RULE_ID
    assert summary.metadata["parsed_row_count"] == 3
    assert summary.metadata["invalid_ohlc_count"] == 0
    assert summary.metadata["non_positive_price_count"] == 0
    assert summary.metadata["quote_side"] == "bid"
    assert summary.metadata["price_columns"] == [
        "open",
        "high",
        "low",
        "close",
    ]

    precision = _finding(report.findings, "ASCII_M1_PRECISION_SUMMARY")
    assert precision.rule_id == ASCII_M1_PRECISION_RULE_ID
    assert precision.metadata["precision_rule_available"] is True
    assert precision.metadata["symbol_metadata"]["asset_class"] == "fx"
    assert precision.metadata["symbol_metadata"]["precision_rule"] == {
        "expected_decimal_places": [6],
        "name": "fx_non_jpy_six_decimal_bid",
        "pip_size": "0.0001",
        "quote_side": "bid",
        "tick_size": "0.000001",
    }
    assert precision.metadata["observed_decimal_places"] == {"6": 12}
    assert precision.metadata["unexpected_precision_count"] == 0
    assert precision.metadata["regime_shift_count"] == 0

    outlier = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    assert outlier.rule_id == ASCII_M1_OUTLIER_RULE_ID
    assert outlier.metadata["symbol_metadata"]["asset_class"] == "fx"
    assert outlier.metadata["threshold_selection"]["source"] == "default"
    assert outlier.metadata["range_outlier_count"] == 0
    assert outlier.metadata["open_jump_count"] == 0
    assert outlier.metadata["flatline_run_count"] == 0
    assert outlier.metadata["return_outlier_count"] == 0


def test_tick_files_are_ignored_by_m1_bar_integrity_rule(
    tmp_path: Path,
) -> None:
    """Tick datasets are owned by the later tick/spread rule block."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


def test_m1_tick_reconstruction_exact_match_is_reported(
    tmp_path: Path,
) -> None:
    """Matching tick bid aggregates should be distinguished explicitly."""
    m1_path, tick_path = _write_reconstruction_pair(
        tmp_path,
        suffix="EXACT",
        m1_rows=("20120201 000000;1.000000;1.000500;0.999900;1.000200;0",),
        tick_rows=(
            "20120201 000000000,1.000000,1.000100,0",
            "20120201 000010000,1.000500,1.000600,0",
            "20120201 000020000,0.999900,1.000000,0",
            "20120201 000059000,1.000200,1.000300,0",
        ),
    )

    report = _run_report_for_paths(m1_path, tick_path)

    summary = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_SUMMARY",
    )
    exact = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_EXACT_MATCH",
    )
    assert report.status is QualityStatus.CLEAN
    assert summary.rule_id == ASCII_M1_TICK_RECONSTRUCTION_RULE_ID
    assert summary.metadata["candidate_pair_count"] == 1
    assert summary.metadata["compared_bar_count"] == 1
    assert summary.metadata["exact_match_count"] == 1
    assert summary.metadata["tolerance_match_count"] == 0
    assert summary.metadata["mismatch_count"] == 0
    assert summary.metadata["aggregation"]["open"] == (
        "first tick bid in minute"
    )
    assert exact.severity is QualitySeverity.INFO
    assert exact.location.timestamp_utc_ms == 1328072400000
    assert exact.location.metadata["classification"] == "exact_match"
    assert exact.metadata["samples"][0]["reconstructed_values"] == {
        "open": 1.0,
        "high": 1.0005,
        "low": 0.9999,
        "close": 1.0002,
    }


def test_m1_tick_reconstruction_tolerance_match_is_reported(
    tmp_path: Path,
) -> None:
    """Small configured deviations should be classified separately."""
    m1_path, tick_path = _write_reconstruction_pair(
        tmp_path,
        suffix="TOLERANCE",
        m1_rows=("20120201 000000;1.000001;1.000500;0.999900;1.000200;0",),
        tick_rows=(
            "20120201 000000000,1.000000,1.000100,0",
            "20120201 000010000,1.000500,1.000600,0",
            "20120201 000020000,0.999900,1.000000,0",
            "20120201 000059000,1.000200,1.000300,0",
        ),
    )
    run_rule = HistDataAsciiM1TickReconstructionRule(
        tolerance=HistDataM1TickReconstructionTolerance(
            price_tolerance=0.000002,
        )
    )

    report = _run_report_for_paths(m1_path, tick_path, run_rules=(run_rule,))

    summary = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_SUMMARY",
    )
    tolerance = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_TOLERANCE_MATCH",
    )
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["exact_match_count"] == 0
    assert summary.metadata["tolerance_match_count"] == 1
    assert summary.metadata["mismatch_count"] == 0
    assert tolerance.location.metadata["classification"] == "tolerance_match"
    max_difference = tolerance.location.metadata["max_abs_difference"]
    assert 0.0 < max_difference <= 0.000002
    assert tolerance.metadata["tolerance"]["price_tolerance"] == 0.000002


def test_m1_tick_reconstruction_mismatch_warns(
    tmp_path: Path,
) -> None:
    """Downloaded bars outside tolerance should warn with row context."""
    m1_path, tick_path = _write_reconstruction_pair(
        tmp_path,
        suffix="MISMATCH",
        m1_rows=("20120201 000000;1.000000;1.000500;0.999900;1.000300;0",),
        tick_rows=(
            "20120201 000000000,1.000000,1.000100,0",
            "20120201 000010000,1.000500,1.000600,0",
            "20120201 000020000,0.999900,1.000000,0",
            "20120201 000059000,1.000200,1.000300,0",
        ),
    )

    report = _run_report_for_paths(m1_path, tick_path)

    summary = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_SUMMARY",
    )
    mismatch = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_MISMATCH",
    )
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["mismatch_count"] == 1
    assert mismatch.severity is QualitySeverity.WARNING
    assert mismatch.location.column == "close"
    assert mismatch.location.metadata["classification"] == "mismatch"
    close_difference = mismatch.metadata["samples"][0]["differences"]["close"]
    assert 0.00009 < close_difference < 0.00011


def test_m1_tick_reconstruction_unavailable_without_matching_tick(
    tmp_path: Path,
) -> None:
    """M1 targets without same-period tick input should be reported cleanly."""
    m1_path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_reconstruction_unavailable",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_UNAVAILABLE.csv",
            rows=("20120201 000000;1.000000;1.000500;0.999900;1.000200;0",),
        ),
    )

    report = _run_report_for_paths(m1_path)

    summary = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_SUMMARY",
    )
    unavailable = _finding(
        report.findings,
        "ASCII_M1_TICK_RECONSTRUCTION_UNAVAILABLE",
    )
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["candidate_pair_count"] == 0
    assert summary.metadata["unavailable_count"] == 1
    assert unavailable.severity is QualitySeverity.INFO
    assert unavailable.location.metadata["reason"] == (
        "matching_tick_target_unavailable"
    )
    assert unavailable.metadata["samples"][0]["tick_path"] == ""


def test_symbol_metadata_identifies_asset_class_precision_profiles() -> None:
    """Symbol metadata should expose asset-specific precision profiles."""
    eurusd = symbol_metadata_for("EURUSD")
    usdjpy = symbol_metadata_for("USDJPY")
    xauusd = symbol_metadata_for("XAUUSD")
    wtiusd = symbol_metadata_for("WTIUSD")
    spxusd = symbol_metadata_for("SPXUSD")
    unknown = symbol_metadata_for("FOOBAR")

    assert eurusd.asset_class == ASSET_CLASS_FX
    assert eurusd.base == "EUR"
    assert eurusd.quote == "USD"
    assert eurusd.precision_rule is not None
    assert eurusd.precision_rule.expected_decimal_places == (6,)
    assert eurusd.precision_rule.pip_size == "0.0001"
    assert eurusd.precision_rule.tick_size == "0.000001"

    assert usdjpy.asset_class == ASSET_CLASS_FX
    assert usdjpy.quote == "JPY"
    assert usdjpy.precision_rule is not None
    assert usdjpy.precision_rule.expected_decimal_places == (3,)
    assert usdjpy.precision_rule.pip_size == "0.01"
    assert usdjpy.precision_rule.tick_size == "0.001"

    assert xauusd.asset_class == ASSET_CLASS_METAL
    assert xauusd.precision_rule is not None
    assert xauusd.precision_rule.expected_decimal_places == (3,)
    assert xauusd.precision_rule.name != eurusd.precision_rule.name

    assert wtiusd.asset_class == ASSET_CLASS_OIL
    assert wtiusd.precision_rule is not None
    assert wtiusd.precision_rule.expected_decimal_places == (3,)
    assert wtiusd.precision_rule.name != eurusd.precision_rule.name

    assert spxusd.asset_class == ASSET_CLASS_INDEX
    assert spxusd.precision_rule is not None
    assert spxusd.precision_rule.expected_decimal_places == (3,)
    assert spxusd.precision_rule.name != eurusd.precision_rule.name

    assert unknown.asset_class == ASSET_CLASS_UNKNOWN
    assert unknown.precision_rule is None


def test_jpy_m1_prices_pass_three_decimal_precision(
    tmp_path: Path,
) -> None:
    """JPY-quoted FX pairs should use JPY-specific tick precision."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_usdjpy_precision",
            timeframe=M1,
            filename="DAT_ASCII_USDJPY_M1_201202.csv",
            rows=(
                "20120201 000000;76.123;76.124;76.120;76.121;0",
                "20120201 000100;76.121;76.125;76.119;76.122;0",
            ),
        ),
    )

    report = _report_for_path(path)

    precision = _finding(report.findings, "ASCII_M1_PRECISION_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert precision.metadata["symbol_metadata"]["quote"] == "JPY"
    assert precision.metadata["symbol_metadata"]["precision_rule"][
        "expected_decimal_places"
    ] == [3]
    assert precision.metadata["observed_decimal_places"] == {"3": 8}


def test_unexpected_m1_precision_warns_with_expected_rule_context(
    tmp_path: Path,
) -> None:
    """Unexpected decimal precision should warn without failing ingestion."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_eurusd_precision_shift",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_PRECISION.csv",
            rows=(
                "20120201 000000;1.306600;1.306610;1.306590;1.306600;0",
                "20120201 000100;1.3066;1.3067;1.3065;1.3066;0",
            ),
        ),
    )

    report = _report_for_path(path)

    unexpected = _finding(report.findings, "ASCII_M1_PRECISION_UNEXPECTED")
    regime = _finding(report.findings, "ASCII_M1_PRECISION_REGIME_SHIFT")
    summary = _finding(report.findings, "ASCII_M1_PRECISION_SUMMARY")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["unexpected_precision_count"] == 4
    assert summary.metadata["regime_shift_count"] == 4
    assert unexpected.severity is QualitySeverity.WARNING
    assert unexpected.location.row_number == 2
    assert unexpected.location.column == "open"
    assert unexpected.location.metadata["decimal_places"] == 4
    assert unexpected.location.metadata["expected_rule"][
        "expected_decimal_places"
    ] == [6]
    assert unexpected.metadata["symbol_metadata"]["normalized_symbol"] == (
        "EURUSD"
    )
    assert unexpected.metadata["samples"][0]["raw_value"] == "1.3066"
    assert regime.metadata["samples"][0]["observed_decimal_places"] == [4, 6]


def test_known_non_fx_symbols_use_calibrated_precision_rules(
    tmp_path: Path,
) -> None:
    """Known non-FX symbols should not fall back to unavailable precision."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_xauusd_precision_calibrated",
            timeframe=M1,
            filename="DAT_ASCII_XAUUSD_M1_201202.csv",
            rows=("20120201 000000;1730.120;1730.125;1730.100;1730.110;0",),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_PRECISION_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["precision_rule_available"] is True
    assert summary.metadata["symbol_metadata"]["asset_class"] == "metal"
    assert summary.metadata["symbol_metadata"]["precision_rule"]["name"] == (
        "metal_three_decimal_bid"
    )
    assert summary.metadata["observed_decimal_places"] == {"3": 4}
    assert not any(
        finding.code == "ASCII_M1_PRECISION_RULE_UNAVAILABLE"
        for finding in report.findings
    )


def test_unknown_symbols_warn_without_precision_thresholds(
    tmp_path: Path,
) -> None:
    """Unknown symbols should not be forced through FX precision rules."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_unknown_symbol_precision",
            timeframe=M1,
            filename="DAT_ASCII_FOOBAR_M1_201202.csv",
            rows=("20120201 000000;1.306600;1.306610;1.306590;1.306600;0",),
        ),
    )

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_M1_SYMBOL_METADATA_UNKNOWN")
    assert report.status is QualityStatus.WARNING
    assert finding.metadata["symbol_metadata"]["asset_class"] == "unknown"
    assert finding.metadata["symbol_metadata"]["known"] is False
    assert finding.metadata["symbol_metadata"]["precision_rule"] is None
    assert finding.metadata["observed_decimal_places"] == {"6": 4}


def test_m1_high_low_range_outlier_warns_with_symbol_context(
    tmp_path: Path,
) -> None:
    """Implausibly large M1 high-low ranges should warn with row context."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_range_outlier",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_RANGE_OUTLIER.csv",
            rows=("20120201 000000;1.050000;1.200000;1.000000;1.050000;0",),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_RANGE_OUTLIER")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["range_outlier_count"] == 1
    assert finding.severity is QualitySeverity.WARNING
    assert finding.rule_id == ASCII_M1_OUTLIER_RULE_ID
    assert finding.location.row_number == 1
    assert finding.location.column == "high"
    assert finding.location.metadata["metric"] == "high_low_range_ratio"
    assert finding.location.metadata["metric_value"] > 0.005
    assert finding.metadata["symbol_metadata"]["normalized_symbol"] == "EURUSD"
    assert (
        finding.metadata["threshold_selection"]["thresholds"]["max_range_ratio"]
        == 0.005
    )


def test_m1_open_close_jump_warns_with_previous_close_context(
    tmp_path: Path,
) -> None:
    """Large current-open versus previous-close jumps should warn."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_open_jump",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_OPEN_JUMP.csv",
            rows=(
                "20120201 000000;1.000000;1.000010;0.999990;1.000000;0",
                "20120201 000100;1.050000;1.050010;1.049990;1.050000;0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_OPEN_CLOSE_JUMP")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["open_jump_count"] == 1
    assert finding.location.row_number == 2
    assert finding.location.column == "open"
    assert finding.location.metadata["metric"] == (
        "previous_close_to_open_ratio"
    )
    assert finding.location.metadata["previous_close_price"] == 1.0
    assert finding.metadata["samples"][0]["previous_row_number"] == 1


def test_m1_flatline_runs_warn_with_bounded_run_sample(
    tmp_path: Path,
) -> None:
    """Long runs of open == high == low == close should warn."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_flatline_run",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_FLATLINE.csv",
            rows=(
                "20120201 000000;1.000000;1.000000;1.000000;1.000000;0",
                "20120201 000100;1.000000;1.000000;1.000000;1.000000;0",
                "20120201 000200;1.000000;1.000000;1.000000;1.000000;0",
                "20120201 000300;1.000000;1.000000;1.000000;1.000000;0",
                "20120201 000400;1.000000;1.000000;1.000000;1.000000;0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_FLATLINE_RUN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["flatline_run_count"] == 1
    assert summary.metadata["flatline_affected_row_count"] == 5
    assert finding.location.row_number == 1
    assert finding.location.metadata["run_end_row_number"] == 5
    assert finding.location.metadata["run_length"] == 5
    assert finding.metadata["affected_row_count"] == 5


def test_m1_return_outlier_warns_with_mad_threshold_metadata(
    tmp_path: Path,
) -> None:
    """Close-to-close returns should be profiled with robust thresholds."""
    rows = []
    previous_close = 1.000000
    closes = (
        1.000010,
        1.000020,
        1.000030,
        1.000040,
        1.050000,
        1.050010,
        1.050020,
        1.050030,
        1.050040,
    )
    for minute, close in enumerate(closes):
        high = max(previous_close, close) + 0.000010
        low = min(previous_close, close) - 0.000010
        rows.append(
            "20120201 "
            f"00{minute:02d}00;{previous_close:.6f};{high:.6f};"
            f"{low:.6f};{close:.6f};0"
        )
        previous_close = close
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_return_outlier",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_RETURN_OUTLIER.csv",
            rows=tuple(rows),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_RETURN_OUTLIER")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["return_sample_count"] == 8
    assert summary.metadata["return_outlier_count"] == 1
    assert finding.location.column == "close"
    assert finding.location.metadata["metric"] == (
        "absolute_log_return_deviation"
    )
    assert finding.metadata["return_mad"] is not None
    assert finding.metadata["return_effective_threshold"] >= 0.005


def test_m1_outlier_thresholds_can_be_overridden_by_asset_class(
    tmp_path: Path,
) -> None:
    """Outlier thresholds should be configurable by asset class."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_open_jump_with_asset_threshold_override",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_OPEN_JUMP_OK.csv",
            rows=(
                "20120201 000000;1.000000;1.000010;0.999990;1.000000;0",
                "20120201 000100;1.050000;1.050010;1.049990;1.050000;0",
            ),
        ),
    )
    discovery = discover_quality_targets((path,))
    report = run_quality_assessment(
        discovery.targets,
        (
            HistDataAsciiM1OutlierRule(
                thresholds_by_asset_class={
                    ASSET_CLASS_FX: HistDataM1OutlierThresholds(
                        max_open_jump_ratio=0.10,
                    )
                },
            ),
        ),
    )

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["open_jump_count"] == 0
    assert summary.metadata["threshold_selection"]["source"] == "asset_class"
    assert summary.metadata["threshold_selection"]["key"] == ASSET_CLASS_FX


def test_m1_outliers_select_default_non_fx_asset_thresholds(
    tmp_path: Path,
) -> None:
    """Known non-FX symbols should select calibrated asset thresholds."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_xauusd_asset_outlier_thresholds",
            timeframe=M1,
            filename="DAT_ASCII_XAUUSD_M1_201202.csv",
            rows=(
                "20120201 000000;1730.120;1730.125;1730.100;1730.110;0",
                "20120201 000100;1730.110;1730.125;1730.100;1730.120;0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OUTLIER_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert (
        summary.metadata["symbol_metadata"]["asset_class"] == ASSET_CLASS_METAL
    )
    assert summary.metadata["threshold_selection"]["source"] == "asset_class"
    assert summary.metadata["threshold_selection"]["key"] == ASSET_CLASS_METAL
    assert (
        summary.metadata["threshold_selection"]["thresholds"][
            "max_open_jump_ratio"
        ]
        == 0.03
    )


def test_invalid_m1_ohlc_rows_fail_with_row_context(
    tmp_path: Path,
) -> None:
    """Rows whose high does not contain open/close should hard fail."""
    report = _report_for_path(
        write_ascii_case(tmp_path, case_by_name("m1_ohlc_violation"))
    )

    summary = _finding(report.findings, "ASCII_M1_OHLC_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_OHLC_INVALID")
    assert report.status is QualityStatus.FAILED
    assert summary.metadata["invalid_ohlc_count"] == 1
    assert finding.severity is QualitySeverity.ERROR
    assert finding.location.row_number == 2
    assert finding.location.column == "high"
    assert finding.location.timestamp_source == "20120201 000100"
    assert finding.location.timestamp_utc_ms == 1328072460000
    assert finding.location.metadata["source_timezone"] == "EST-no-DST"
    assert finding.location.metadata["violations"] == [
        "high_below_open_or_close"
    ]
    assert finding.metadata["samples"][0]["values"]["high"] == 1.3065
    assert finding.metadata["samples"][0]["values"]["close"] == 1.30656


def test_low_above_open_close_and_high_low_order_are_reported(
    tmp_path: Path,
) -> None:
    """Every OHLC ordering constraint should appear in violation metadata."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_bad_low_and_range",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_BAD_RANGE.csv",
            rows=("20120201 000000;1.100000;1.150000;1.200000;1.100000;0",),
        ),
    )

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_M1_OHLC_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.location.row_number == 1
    assert finding.location.column == "low"
    assert finding.metadata["samples"][0]["violations"] == [
        "low_above_open_or_close",
        "high_below_low",
    ]


def test_non_positive_m1_ohlc_prices_fail_with_row_context(
    tmp_path: Path,
) -> None:
    """M1 bid OHLC prices must be strictly positive."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_non_positive_prices",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_NON_POSITIVE.csv",
            rows=("20120201 000000;0.000000;1.000000;0.000000;1.000000;0",),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report.findings, "ASCII_M1_OHLC_SUMMARY")
    finding = _finding(report.findings, "ASCII_M1_PRICE_NON_POSITIVE")
    assert report.status is QualityStatus.FAILED
    assert summary.metadata["non_positive_price_count"] == 1
    assert finding.location.row_number == 1
    assert finding.location.column == "open"
    assert finding.location.metadata["non_positive_columns"] == [
        "open",
        "low",
    ]
    assert finding.metadata["samples"][0]["non_positive_columns"] == [
        "open",
        "low",
    ]
    assert finding.metadata["samples"][0]["values"]["open"] == 0.0


def test_zip_csv_member_is_scanned_for_m1_bar_findings(
    tmp_path: Path,
) -> None:
    """ZIP targets should scan their single CSV payload for bar integrity."""
    archive = write_zip_case(
        tmp_path,
        case_by_name("m1_ohlc_violation"),
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )

    report = _report_for_path(archive)

    finding = _finding(report.findings, "ASCII_M1_OHLC_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_M1_201202_BAD_OHLC.csv"
    )
    assert finding.location.metadata["source_member"] == (
        "DAT_ASCII_EURUSD_M1_201202_BAD_OHLC.csv"
    )


def _write_reconstruction_pair(
    tmp_path: Path,
    *,
    suffix: str,
    m1_rows: tuple[str, ...],
    tick_rows: tuple[str, ...],
) -> tuple[Path, Path]:
    m1_path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name=f"m1_reconstruction_{suffix.lower()}",
            timeframe=M1,
            filename=f"DAT_ASCII_EURUSD_M1_201202_{suffix}.csv",
            rows=m1_rows,
        ),
    )
    tick_path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name=f"tick_reconstruction_{suffix.lower()}",
            timeframe=TICK,
            filename=f"DAT_ASCII_EURUSD_T_201202_{suffix}.csv",
            rows=tick_rows,
        ),
    )
    return (m1_path, tick_path)


def _run_report_for_paths(
    *paths: Path,
    run_rules=None,
):
    discovery = discover_quality_targets(paths)
    return run_quality_assessment(
        discovery.targets,
        (),
        run_rules=(
            quality_run_rules_for_groups(("bars",))
            if run_rules is None
            else run_rules
        ),
    )


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("bars",)),
    )


def _finding(findings, code: str):
    matches = tuple(finding for finding in findings if finding.code == code)
    assert len(matches) == 1
    return matches[0]
