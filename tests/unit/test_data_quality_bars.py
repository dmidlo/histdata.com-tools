"""Tests for M1 bar data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    ASCII_M1_BAR_INTEGRITY_RULE_ID,
    ASCII_M1_PRECISION_RULE_ID,
    ASSET_CLASS_FX,
    ASSET_CLASS_INDEX,
    ASSET_CLASS_METAL,
    ASSET_CLASS_OIL,
    ASSET_CLASS_UNKNOWN,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
    symbol_metadata_for,
)
from histdatacom.histdata_ascii import M1
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
    ]


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


def test_tick_files_are_ignored_by_m1_bar_integrity_rule(
    tmp_path: Path,
) -> None:
    """Tick datasets are owned by the later tick/spread rule block."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


def test_symbol_metadata_identifies_fx_precision_classes() -> None:
    """FX metadata should distinguish JPY and non-JPY quote precision."""
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
    assert xauusd.precision_rule is None

    assert wtiusd.asset_class == ASSET_CLASS_OIL
    assert wtiusd.precision_rule is None

    assert spxusd.asset_class == ASSET_CLASS_INDEX
    assert spxusd.precision_rule is None

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


def test_known_non_fx_symbols_warn_until_precision_rules_exist(
    tmp_path: Path,
) -> None:
    """Known non-FX symbols should be classified but threshold-light."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_xauusd_precision_unavailable",
            timeframe=M1,
            filename="DAT_ASCII_XAUUSD_M1_201202.csv",
            rows=("20120201 000000;1730.120;1730.125;1730.100;1730.110;0",),
        ),
    )

    report = _report_for_path(path)

    finding = _finding(report.findings, "ASCII_M1_PRECISION_RULE_UNAVAILABLE")
    assert report.status is QualityStatus.WARNING
    assert finding.metadata["symbol_metadata"]["asset_class"] == "metal"
    assert finding.metadata["symbol_metadata"]["precision_rule"] is None
    assert finding.metadata["observed_decimal_places"] == {"3": 4}


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
