"""Tests for domain symbol metadata data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    DOMAIN_CALENDAR_SESSION_RULE_ID,
    DOMAIN_SYMBOL_METADATA_RULE_ID,
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
    assert quality_run_rules_for_groups(("domain",)) == ()
    assert DOMAIN_SYMBOL_METADATA_RULE_ID in {
        rule.rule_id for rule in quality_rules_for_groups(("all",))
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
    assert metal.metadata["symbol_metadata"]["precision_rule"] is None
    assert index.metadata["symbol_metadata"]["asset_class"] == "index"
    assert index.metadata["symbol_metadata"]["known"] is True
    assert index.metadata["symbol_metadata"]["precision_rule"] is None


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


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("domain",)),
    )


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]
