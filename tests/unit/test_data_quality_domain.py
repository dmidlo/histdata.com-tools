"""Tests for domain symbol metadata data-quality rules."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.data_quality import (
    CROSS_INSTRUMENT_METADATA_KEY,
    DOMAIN_CALENDAR_SESSION_RULE_ID,
    DOMAIN_CROSS_INSTRUMENT_RULE_ID,
    DOMAIN_SYMBOL_METADATA_RULE_ID,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    discover_quality_targets,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import M1, TICK
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_TICK_CASE,
    HistDataAsciiCase,
    write_ascii_case,
)


def test_domain_group_registers_symbol_metadata_rule() -> None:
    """The advertised domain group should execute symbol metadata checks."""
    assert [rule.rule_id for rule in quality_rules_for_groups(("domain",))] == [
        DOMAIN_SYMBOL_METADATA_RULE_ID,
        DOMAIN_CALENDAR_SESSION_RULE_ID,
    ]
    assert [
        rule.rule_id for rule in quality_run_rules_for_groups(("domain",))
    ] == [DOMAIN_CROSS_INSTRUMENT_RULE_ID]
    assert DOMAIN_SYMBOL_METADATA_RULE_ID in {
        rule.rule_id for rule in quality_rules_for_groups(("all",))
    }
    assert DOMAIN_CROSS_INSTRUMENT_RULE_ID in {
        rule.rule_id for rule in quality_run_rules_for_groups(("all",))
    }


def test_domain_symbol_metadata_reports_eurusd_quote_convention(
    tmp_path: Path,
) -> None:
    """EURUSD should report FX base/quote and bid-only M1 assumptions."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    summary = _finding(report, "DOMAIN_SYMBOL_METADATA_SUMMARY")
    symbol = summary.metadata["symbol_metadata"]
    quote = summary.metadata["quote_convention"]
    assumptions = summary.metadata["format_assumptions"]
    assert report.status is QualityStatus.CLEAN
    assert symbol["normalized_symbol"] == "EURUSD"
    assert symbol["asset_class"] == "fx"
    assert symbol["base"] == "EUR"
    assert symbol["quote"] == "USD"
    assert symbol["pip_size"] == "0.0001"
    assert symbol["tick_size"] == "0.000001"
    assert symbol["quote_side"] == "bid"
    assert symbol["m1_bid_only"] is True
    assert "EUR_USD" in symbol["aliases"]
    assert quote["pair_direction"] == "base_quote"
    assert quote["price_unit"] == "USD per EUR"
    assert quote["m1_bid_only"] is True
    assert assumptions["m1_bid_ohlc"] is True
    assert assumptions["active_quote_side"] == "bid"


def test_domain_symbol_metadata_reports_jpy_pip_size(
    tmp_path: Path,
) -> None:
    """JPY-quoted FX pairs should carry JPY pip and tick-size defaults."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_usdjpy_domain",
            timeframe=M1,
            filename="DAT_ASCII_USDJPY_M1_201202.csv",
            rows=("20120201 000000;76.123;76.124;76.120;76.121;0",),
        ),
    )

    report = _report_for_path(path)

    symbol = _finding(report, "DOMAIN_SYMBOL_METADATA_SUMMARY").metadata[
        "symbol_metadata"
    ]
    assert report.status is QualityStatus.CLEAN
    assert symbol["asset_class"] == "fx"
    assert symbol["base"] == "USD"
    assert symbol["quote"] == "JPY"
    assert symbol["pip_size"] == "0.01"
    assert symbol["tick_size"] == "0.001"


def test_domain_symbol_metadata_reports_tick_bid_ask_assumptions(
    tmp_path: Path,
) -> None:
    """Tick files should report bid/ask quote-side assumptions."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    assumptions = _finding(report, "DOMAIN_SYMBOL_METADATA_SUMMARY").metadata[
        "format_assumptions"
    ]
    assert report.status is QualityStatus.CLEAN
    assert assumptions["timeframe"] == TICK
    assert assumptions["tick_bid_ask"] is True
    assert assumptions["tick_price_columns"] == ["bid", "ask"]
    assert assumptions["active_quote_side"] == "bid/ask"


def test_domain_symbol_metadata_reports_known_metals_and_indexes(
    tmp_path: Path,
) -> None:
    """Known non-FX assets should stay assessable without FX pip defaults."""
    metal_path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_xauusd_domain",
            timeframe=M1,
            filename="DAT_ASCII_XAUUSD_M1_201202.csv",
            rows=("20120201 000000;1730.120;1730.125;1730.100;1730.110;0",),
        ),
    )
    index_path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_spxusd_domain",
            timeframe=M1,
            filename="DAT_ASCII_SPXUSD_M1_201202.csv",
            rows=("20120201 000000;4500.100;4500.200;4499.900;4500.000;0",),
        ),
    )

    metal = _finding(
        _report_for_path(metal_path), "DOMAIN_SYMBOL_METADATA_SUMMARY"
    )
    index = _finding(
        _report_for_path(index_path), "DOMAIN_SYMBOL_METADATA_SUMMARY"
    )

    assert metal.metadata["symbol_metadata"]["asset_class"] == "metal"
    assert metal.metadata["symbol_metadata"]["known"] is True
    assert metal.metadata["symbol_metadata"]["precision_rule"]["name"] == (
        "metal_three_decimal_bid"
    )
    assert index.metadata["symbol_metadata"]["asset_class"] == "index"
    assert index.metadata["symbol_metadata"]["known"] is True
    assert index.metadata["symbol_metadata"]["precision_rule"]["name"] == (
        "index_three_decimal_bid"
    )


def test_domain_symbol_metadata_supports_enum_value_aliases(
    tmp_path: Path,
) -> None:
    """Pairs enum values such as EUR_USD should normalize to EURUSD."""
    path = tmp_path / "DAT_ASCII_EUR_USD_M1_201202.csv"
    path.write_text(
        "20120201 000000;1.306600;1.306610;1.306590;1.306600;0\n",
        encoding="utf-8",
    )
    target = QualityTarget(
        path=str(path.resolve()),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=M1,
        symbol="EUR_USD",
        period="201202",
        metadata={"symbol": "EUR_USD"},
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("domain",)),
    )

    symbol = _finding(report, "DOMAIN_SYMBOL_METADATA_SUMMARY").metadata[
        "symbol_metadata"
    ]
    assert report.status is QualityStatus.CLEAN
    assert symbol["normalized_symbol"] == "EURUSD"
    assert symbol["pair_key"] == "eurusd"
    assert symbol["source"] == "fx_enums.Pairs"
    assert "EUR_USD" in symbol["aliases"]


def test_domain_symbol_metadata_warns_for_unknown_symbols(
    tmp_path: Path,
) -> None:
    """Unknown symbols should warn without blocking assessment."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_unknown_domain",
            timeframe=M1,
            filename="DAT_ASCII_FOOBAR_M1_201202.csv",
            rows=("20120201 000000;1.306600;1.306610;1.306590;1.306600;0",),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report, "DOMAIN_SYMBOL_METADATA_SUMMARY")
    warning = _finding(report, "DOMAIN_SYMBOL_METADATA_UNKNOWN")
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["symbol_metadata"]["asset_class"] == "unknown"
    assert summary.metadata["symbol_metadata"]["known"] is False
    assert warning.location.column == "symbol"
    assert warning.metadata["symbol_metadata"]["normalized_symbol"] == "FOOBAR"


def test_cross_instrument_run_rule_compares_triangular_fx_sets(
    tmp_path: Path,
) -> None:
    """EURUSD / GBPUSD should be compared against direct EURGBP."""
    _write_m1_prices(
        tmp_path,
        "EURUSD",
        ("1.200000", "1.210000", "1.220000"),
    )
    _write_m1_prices(
        tmp_path,
        "GBPUSD",
        ("1.500000", "1.512500", "1.525000"),
    )
    _write_m1_prices(
        tmp_path,
        "EURGBP",
        ("0.810000", "0.900000", "0.800000"),
    )

    report = _domain_run_report_for_paths((tmp_path,))

    summary = _finding(report, "DOMAIN_CROSS_INSTRUMENT_SUMMARY")
    warning = _finding(report, "DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_WARNING")
    error = _finding(report, "DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR")
    payload = report.metadata[CROSS_INSTRUMENT_METADATA_KEY]
    assert report.status is QualityStatus.FAILED
    assert summary.rule_id == DOMAIN_CROSS_INSTRUMENT_RULE_ID
    assert payload["triangular_candidate_count"] == 1
    assert payload["triangular_compared_timestamp_count"] == 3
    assert payload["triangular_warning_count"] == 1
    assert payload["triangular_error_count"] == 1
    assert warning.severity is QualitySeverity.WARNING
    assert error.severity is QualitySeverity.ERROR
    assert warning.metadata["samples"][0]["relationship"] == (
        "EURUSD / GBPUSD ~= EURGBP"
    )
    assert error.metadata["samples"][0]["direct_symbol"] == "EURGBP"


def test_cross_instrument_run_rule_preserves_issue_242_data_defect(
    tmp_path: Path,
) -> None:
    """The reviewed AUDCAD 2008 triangle defect should remain a hard error."""
    _write_single_m1_close(
        tmp_path,
        symbol="AUDCHF",
        period="2008",
        timestamp="20080601 170200",
        close="0.994400",
    )
    _write_single_m1_close(
        tmp_path,
        symbol="CADCHF",
        period="2008",
        timestamp="20080601 170200",
        close="1.046900",
    )
    _write_single_m1_close(
        tmp_path,
        symbol="AUDCAD",
        period="2008",
        timestamp="20080601 170200",
        close="1.041700",
    )

    report = _domain_run_report_for_paths((tmp_path,))

    error = _finding(report, "DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR")
    sample = error.metadata["samples"][0]
    assert report.status is QualityStatus.FAILED
    assert sample["relationship"] == "AUDCHF / CADCHF ~= AUDCAD"
    assert sample["direct_symbol"] == "AUDCAD"
    assert sample["numerator_symbol"] == "AUDCHF"
    assert sample["denominator_symbol"] == "CADCHF"
    assert sample["timeframe"] == M1
    assert sample["period"] == "2008"
    assert sample["timestamp_utc_ms"] == 1212357720000
    assert sample["direct_price"] == 1.0417
    assert sample["implied_price"] == pytest.approx(0.9498519438341771)
    assert sample["relative_difference"] == pytest.approx(0.088171312437192)


def test_cross_instrument_run_rule_warns_for_stale_join_risk(
    tmp_path: Path,
) -> None:
    """Sparse joins should flag forward-filled stale instrument values."""
    _write_m1_prices(
        tmp_path,
        "EURUSD",
        ("1.200000", "1.201000", "1.202000", "1.203000"),
    )
    _write_m1_prices(tmp_path, "GBPUSD", ("1.500000",))

    report = _domain_run_report_for_paths((tmp_path,))

    summary = _finding(report, "DOMAIN_CROSS_INSTRUMENT_SUMMARY")
    sparse = _finding(
        report,
        "DOMAIN_CROSS_INSTRUMENT_TIMESTAMP_GRID_SPARSE",
    )
    stale = _finding(report, "DOMAIN_CROSS_INSTRUMENT_STALE_JOIN_RISK")
    sample = stale.metadata["samples"][0]
    assert report.status is QualityStatus.WARNING
    assert summary.metadata["timestamp_grid_group_count"] == 1
    assert sparse.metadata["samples"][0]["common_timestamp_ratio"] == 0.25
    assert sample["stale_symbol"] == "GBPUSD"
    assert sample["active_symbol"] == "EURUSD"
    assert sample["affected_timestamp_count"] == 3


def test_cross_instrument_run_rule_reports_unavailable_symbol_sets(
    tmp_path: Path,
) -> None:
    """Single-instrument runs should report unavailable cross checks as info."""
    _write_m1_prices(tmp_path, "EURUSD", ("1.200000", "1.201000"))

    report = _domain_run_report_for_paths((tmp_path,))

    unavailable = _finding(report, "DOMAIN_CROSS_INSTRUMENT_UNAVAILABLE")
    reasons = {sample["reason"] for sample in unavailable.metadata["samples"]}
    assert report.status is QualityStatus.CLEAN
    assert "no_multi_instrument_group" in reasons
    assert "no_triangular_symbol_sets" in reasons
    assert "no_inverse_symbol_sets" in reasons


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("domain",)),
    )


def _domain_run_report_for_paths(paths: tuple[Path, ...]):
    discovery = discover_quality_targets(paths)
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("domain",)),
        run_rules=quality_run_rules_for_groups(("domain",)),
        metadata={"roots": [str(path) for path in paths]},
    )


def _write_m1_prices(
    directory: Path,
    symbol: str,
    closes: tuple[str, ...],
) -> Path:
    rows = tuple(
        f"20120201 000{index}00;{close};{close};{close};{close};0"
        for index, close in enumerate(closes)
    )
    return write_ascii_case(
        directory,
        HistDataAsciiCase(
            name=f"m1_{symbol.lower()}",
            timeframe=M1,
            filename=f"DAT_ASCII_{symbol}_M1_201202.csv",
            rows=rows,
        ),
    )


def _write_single_m1_close(
    directory: Path,
    *,
    symbol: str,
    period: str,
    timestamp: str,
    close: str,
) -> Path:
    return write_ascii_case(
        directory,
        HistDataAsciiCase(
            name=f"m1_{symbol.lower()}_{period}",
            timeframe=M1,
            filename=f"DAT_ASCII_{symbol}_M1_{period}.csv",
            rows=(f"{timestamp};{close};{close};{close};{close};0",),
        ),
    )


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]
