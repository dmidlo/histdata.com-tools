"""Deep data-quality checks for canonical Polars cache targets."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl

from histdatacom.data_quality import (
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_M1_ROWS,
    case_by_name,
)


def test_clean_m1_cache_runs_deep_ingestion_time_and_bar_checks(
    tmp_path: Path,
) -> None:
    """A direct .data target should receive semantic validation."""
    cache_path = _write_cache_case(tmp_path, CLEAN_M1_CASE)

    report = _report_for_cache(cache_path, groups=("ingestion", "time", "bars"))

    assert report.status is QualityStatus.CLEAN
    assert _non_info_codes(report.findings) == []
    row_count = _finding(report.findings, "ASCII_ROW_COUNT_SUMMARY")
    assert row_count.metadata["kind"] == "cache"
    assert row_count.metadata["data_format"] == "ascii"
    assert row_count.metadata["timeframe"] == "M1"
    assert row_count.metadata["symbol"] == "EURUSD"
    assert row_count.metadata["period"] == "201202"
    assert row_count.metadata["row_count"] == 3
    assert (
        _finding(
            report.findings,
            "ASCII_TIMESTAMP_EST_NO_DST_SUMMARY",
        ).metadata["parsed_row_count"]
        == 3
    )
    assert (
        _finding(
            report.findings,
            "ASCII_M1_OHLC_SUMMARY",
        ).metadata["parsed_row_count"]
        == 3
    )
    precision = _finding(report.findings, "ASCII_M1_PRECISION_SUMMARY")
    assert precision.metadata["raw_decimal_precision_preserved"] is False


def test_cache_schema_validation_flags_missing_columns(
    tmp_path: Path,
) -> None:
    """Required canonical cache columns should be enforced."""
    cache_path = _write_cache_case(
        tmp_path,
        CLEAN_M1_CASE,
        mutate=lambda frame: frame.drop("close"),
    )

    report = _report_for_cache(cache_path, groups=("ingestion",))

    finding = _finding(report.findings, "ASCII_CACHE_SCHEMA_MISSING_COLUMNS")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["missing_columns"] == ["close"]


def test_cache_schema_validation_flags_dtypes_nulls_and_non_finite_values(
    tmp_path: Path,
) -> None:
    """Cache payloads should retain canonical dtypes and finite values."""
    cache_path = _write_cache_case(
        tmp_path,
        CLEAN_M1_CASE,
        mutate=lambda frame: frame.with_columns(
            [
                pl.col("datetime").cast(pl.Float64),
                pl.lit(None).cast(pl.Float64).alias("open"),
                pl.lit(float("inf")).alias("high"),
                pl.lit(-1).alias("vol"),
            ]
        ),
    )

    report = _report_for_cache(cache_path, groups=("ingestion",))

    assert report.status is QualityStatus.FAILED
    assert (
        _finding(report.findings, "ASCII_CACHE_DTYPE_INVALID").metadata[
            "row_count"
        ]
        >= 1
    )
    assert (
        _finding(report.findings, "ASCII_NUMERIC_INVALID").metadata["row_count"]
        >= 2
    )
    assert (
        _finding(report.findings, "ASCII_VOLUME_INVALID").metadata["row_count"]
        == 3
    )


def test_m1_cache_time_checks_detect_non_monotonic_rows(
    tmp_path: Path,
) -> None:
    """Timestamp rules should inspect direct M1 cache ordering."""
    cache_path = _write_cache_case(
        tmp_path,
        case_by_name("m1_non_monotonic_timestamp"),
    )

    report = _report_for_cache(cache_path, groups=("time",))

    finding = _finding(report.findings, "ASCII_TIMESTAMP_NON_MONOTONIC")
    assert report.status is QualityStatus.WARNING
    assert finding.location.row_number == 2


def test_m1_cache_bar_checks_detect_ohlc_violations(
    tmp_path: Path,
) -> None:
    """Bar integrity rules should inspect direct M1 cache prices."""
    cache_path = _write_cache_case(tmp_path, case_by_name("m1_ohlc_violation"))

    report = _report_for_cache(cache_path, groups=("bars",))

    finding = _finding(report.findings, "ASCII_M1_OHLC_INVALID")
    assert report.status is QualityStatus.FAILED
    assert finding.location.row_number == 2


def test_tick_cache_spread_checks_detect_negative_spreads(
    tmp_path: Path,
) -> None:
    """Tick spread rules should inspect direct tick cache bid/ask values."""
    cache_path = _write_cache_case(
        tmp_path,
        case_by_name("tick_negative_spread"),
    )

    report = _report_for_cache(cache_path, groups=("ticks",))

    finding = _finding(report.findings, "ASCII_TICK_NEGATIVE_SPREAD")
    assert report.status is QualityStatus.FAILED
    assert finding.location.row_number == 2
    assert finding.metadata["samples"][0]["timestamp_source"] == (
        "20120201 000003973"
    )


def _write_cache_case(
    directory: Path,
    case,
    *,
    mutate: Callable[[Any], Any] | None = None,
) -> Path:
    batch = parse_ascii_lines(case.timeframe, case.rows or CLEAN_M1_ROWS)
    frame = to_polars_frame(batch)
    if mutate is not None:
        frame = mutate(frame)
    source_row = (case.rows or CLEAN_M1_ROWS)[0]
    period = source_row[:6]
    year = period[:4]
    month = period[4:6]
    cache_path = (
        directory
        / "ASCII"
        / case.timeframe
        / "eurusd"
        / year
        / month
        / CACHE_FILENAME
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    write_polars_cache(frame, cache_path)
    return cache_path


def _report_for_cache(path: Path, *, groups: tuple[str, ...]):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(groups),
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
