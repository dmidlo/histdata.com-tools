"""Tests for cache-scale data-quality preflight benchmarks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from histdatacom import Options
from histdatacom.cli import ArgParser
from histdatacom.data_quality.preflight import (
    QUALITY_PREFLIGHT_SCHEMA_VERSION,
    format_quality_preflight_console_summary,
    run_cache_quality_preflight,
    write_quality_preflight_report,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)
from tests.fixtures.histdata_ascii.quality_cases import CLEAN_TICK_CASE


def test_quality_preflight_samples_cache_quantiles_and_estimates_runtime(
    tmp_path: Path,
) -> None:
    """Preflight should benchmark a bounded representative cache sample."""
    data_dir = tmp_path / "data"
    for index, symbol in enumerate(
        ("eurusd", "gbpusd", "usdjpy", "audusd", "nzdusd"),
        start=1,
    ):
        _write_tick_cache(data_dir, symbol=symbol, row_multiplier=index)
    ticks = iter((10.0, 12.0))

    payload = run_cache_quality_preflight(
        data_dir,
        pair_groups=("majors",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=3,
        activity_budget_seconds=1000,
        clock=lambda: next(ticks),
    )

    assert payload["schema_version"] == QUALITY_PREFLIGHT_SCHEMA_VERSION
    assert payload["operation"] == "data-quality-cache-preflight"
    assert payload["status"] == "pass"
    assert payload["target_count"] == 5
    assert payload["sample"]["strategy"] == "size-quantiles"
    assert payload["sample"]["requested_count"] == 3
    assert payload["sample"]["selected_count"] == 3
    assert payload["benchmark"]["elapsed_seconds"] == 2.0
    assert payload["benchmark"]["sample_row_count"] > 0
    assert payload["benchmark"]["rows_per_second"] > 0
    assert payload["benchmark"]["bytes_per_second"] > 0
    assert payload["temporal_budget"]["activity"] == "data_quality"
    assert payload["temporal_budget"]["status"] == "pass"
    assert payload["sample_quality"]["summary"]["target_count"] == 3
    encoded = json.dumps(payload, sort_keys=True)
    assert str(tmp_path) not in encoded
    assert "/Users/" not in encoded
    assert "/private/" not in encoded
    assert "/var/folders/" not in encoded


def test_quality_preflight_flags_budget_failures(tmp_path: Path) -> None:
    """Budget comparison should fail when extrapolated runtime is too large."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    ticks = iter((0.0, 10.0))

    payload = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=1,
        clock=lambda: next(ticks),
    )

    assert payload["status"] == "fail"
    assert payload["temporal_budget"]["status"] == "fail"
    assert "exceeds" in str(payload["temporal_budget"]["reason"])


def test_quality_preflight_report_and_summary_are_publish_safe(
    tmp_path: Path,
) -> None:
    """JSON reports and console summaries should be safe for GitHub evidence."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    payload = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
    )
    payload["report_path"] = "reports/preflight.json"

    report_path = write_quality_preflight_report(
        payload,
        tmp_path / "reports" / "preflight.json",
    )
    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    summary = format_quality_preflight_console_summary(payload)

    assert loaded["schema_version"] == QUALITY_PREFLIGHT_SCHEMA_VERSION
    assert "Data quality cache preflight" in summary
    assert "report: reports/preflight.json" in summary
    assert str(tmp_path) not in json.dumps(loaded, sort_keys=True)


def test_cli_accepts_quality_preflight_without_temporal_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """CLI parsing should treat preflight as a standalone quality mode."""
    import sys

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality-preflight",
            "--quality-target",
            str(tmp_path),
            "--quality-checks",
            "ticks",
            "--quality-preflight-sample-size",
            "2",
            "--pair-groups",
            "majors",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
        ],
    )

    options = ArgParser(Options())()

    assert options.quality_preflight
    assert options.quality_paths == [str(tmp_path)]
    assert options.quality_check_groups == ["ticks"]
    assert options.quality_preflight_sample_size == 2
    assert options.pair_groups == ["majors"]


def test_api_quality_preflight_returns_payload_without_temporal_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API callers should get preflight evidence without job submission."""
    import histdatacom.histdata_com as histdata_com

    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)

    def fail_submit(*args: object, **kwargs: object) -> None:
        raise AssertionError("preflight should not submit to Temporal")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = Options()
    options.quality_preflight = True
    options.quality_paths = (str(data_dir),)
    options.quality_check_groups = {"inventory"}
    options.quality_preflight_sample_size = 1
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"T"}

    payload = histdata_com.main(options)

    assert payload["operation"] == "data-quality-cache-preflight"
    assert payload["target_count"] == 1
    assert payload["sample"]["selected_count"] == 1


def _write_tick_cache(
    root: Path,
    *,
    symbol: str,
    row_multiplier: int,
) -> Path:
    cache_path = (
        root
        / "ASCII"
        / TICK
        / symbol
        / "2012"
        / f"{row_multiplier:02d}"
        / CACHE_FILENAME
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    rows = CLEAN_TICK_CASE.rows * row_multiplier
    write_polars_cache(
        to_polars_frame(parse_ascii_lines(TICK, rows)),
        cache_path,
    )
    return cache_path
