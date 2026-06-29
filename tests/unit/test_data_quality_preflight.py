"""Tests for cache-scale data-quality preflight benchmarks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import histdatacom
import pytest

from histdatacom import Options
from histdatacom.cli import ArgParser
from histdatacom.data_quality.preflight import (
    QUALITY_PREFLIGHT_SCHEMA_VERSION,
    format_quality_preflight_console_summary,
    format_quality_run_preflight_warning,
    quality_run_preflight_warning,
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
    assert str(payload["generated_at_utc"]).endswith("Z")
    assert payload["package"]["version"] == histdatacom.__version__
    assert payload["status"] == "pass"
    assert payload["target_count"] == 5
    assert payload["cache_inventory"]["fingerprint_algorithm"] == "sha256"
    assert len(str(payload["cache_inventory"]["fingerprint"])) == 64
    assert payload["sample"]["strategy"] == "size-quantiles"
    assert payload["sample"]["requested_count"] == 3
    assert payload["sample"]["selected_count"] == 3
    assert payload["benchmark"]["elapsed_seconds"] == 2.0
    assert payload["benchmark"]["sample_row_count"] > 0
    assert payload["benchmark"]["rows_per_second"] > 0
    assert payload["benchmark"]["bytes_per_second"] > 0
    assert payload["temporal_budget"]["activity"] == "data_quality"
    assert payload["temporal_budget"]["status"] == "pass"
    assert payload["preflight_policy"]["sample_size"] == 3
    assert (
        payload["preflight_policy"]["temporal_budget"][
            "activity_budget_seconds"
        ]
        == 1000
    )
    assert payload["decision"]["state"] == "safe"
    assert payload["decision"]["next_command"].startswith(
        "histdatacom --quality"
    )
    assert "--pair-groups majors" in payload["decision"]["next_command"]
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
    assert payload["decision"]["state"] == "fail"
    assert payload["decision"]["next_command"] == ""
    assert "exceeds" in str(payload["temporal_budget"]["reason"])


def test_quality_preflight_decision_warns_near_temporal_budget(
    tmp_path: Path,
) -> None:
    """Near-budget estimates should recommend review before a full run."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    ticks = iter((0.0, 9.0))

    payload = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=10,
        clock=lambda: next(ticks),
    )

    assert payload["status"] == "warn"
    assert payload["decision"]["state"] == "warn"
    assert payload["decision"]["next_command"].startswith(
        "histdatacom --quality"
    )
    assert "larger sample" in payload["decision"]["action"]


def test_quality_preflight_no_target_diagnostics_report_cache_dimensions(
    tmp_path: Path,
) -> None:
    """No-target decisions should show requested and discovered dimensions."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)

    payload = run_cache_quality_preflight(
        data_dir,
        pairs=("gbpusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
    )
    diagnostics = payload["diagnostics"]
    dimensions = diagnostics["discovered_cache_dimensions"]
    summary = format_quality_preflight_console_summary(payload)

    assert payload["status"] == "fail"
    assert payload["decision"]["state"] == "no-targets"
    assert diagnostics["requested_filters"]["pairs"] == ["gbpusd"]
    assert dimensions["canonical_data_cache_count"] == 1
    assert dimensions["matching_cache_count"] == 0
    assert dimensions["pairs"] == ["eurusd"]
    assert "requested filters: groups=all; pairs=gbpusd" in summary
    assert "discovered caches: 1 canonical .data" in summary


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


def test_quality_run_warning_requires_matching_preflight_evidence(
    tmp_path: Path,
) -> None:
    """Large cache quality runs should warn unless evidence matches scope."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        large_target_count=1,
    )

    assert warning is not None
    assert warning["status"] == "warn"
    assert warning["target_count"] == 1
    assert "histdatacom --quality-preflight" in str(
        warning["suggested_preflight_command"]
    )
    assert "continuing without prompting" in (
        format_quality_run_preflight_warning(warning)
    )

    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
    )
    report_path = write_quality_preflight_report(
        evidence,
        tmp_path / "preflight.json",
    )

    assert (
        quality_run_preflight_warning(
            (data_dir,),
            pairs=("eurusd",),
            formats=("ascii",),
            timeframes=("T",),
            quality_check_groups=("inventory",),
            evidence_path=report_path,
            activity_budget_seconds=100,
            large_target_count=1,
        )
        is None
    )


def test_quality_run_warning_rejects_stale_preflight_evidence(
    tmp_path: Path,
) -> None:
    """Matching evidence should still warn when generated outside max age."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    generated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
        utc_now=lambda: generated_at,
    )
    report_path = write_quality_preflight_report(
        evidence,
        tmp_path / "preflight.json",
    )

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        evidence_path=report_path,
        evidence_max_age_seconds=60,
        activity_budget_seconds=100,
        utc_now=lambda: generated_at + timedelta(minutes=2),
        large_target_count=1,
    )

    assert warning is not None
    assert warning["evidence"]["status"] == "stale"
    assert "older than 60 seconds" in str(warning["evidence"]["reason"])


def test_quality_run_warning_rejects_version_mismatched_evidence(
    tmp_path: Path,
) -> None:
    """Evidence from another package version should not suppress warnings."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
        utc_now=lambda: now,
    )
    evidence["package"]["version"] = "0.0.0"
    report_path = tmp_path / "preflight.json"
    report_path.write_text(json.dumps(evidence), encoding="utf-8")

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        evidence_path=report_path,
        activity_budget_seconds=100,
        utc_now=lambda: now,
        large_target_count=1,
    )

    assert warning is not None
    assert warning["evidence"]["status"] == "version-mismatch"
    assert warning["evidence"]["expected_version"] == histdatacom.__version__


def test_quality_run_warning_rejects_policy_mismatched_evidence(
    tmp_path: Path,
) -> None:
    """Evidence from a different Temporal budget should not suppress warnings."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
        utc_now=lambda: now,
    )
    report_path = write_quality_preflight_report(
        evidence,
        tmp_path / "preflight.json",
    )

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        evidence_path=report_path,
        activity_budget_seconds=200,
        utc_now=lambda: now,
        large_target_count=1,
    )

    assert warning is not None
    assert warning["evidence"]["status"] == "policy-mismatch"
    assert "budget differs" in str(warning["evidence"]["reason"])


def test_quality_run_warning_rejects_cache_inventory_mismatch(
    tmp_path: Path,
) -> None:
    """Evidence should not match after the local cache inventory changes."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
        utc_now=lambda: now,
    )
    evidence["cache_inventory"]["fingerprint"] = "stale"
    report_path = tmp_path / "preflight.json"
    report_path.write_text(json.dumps(evidence), encoding="utf-8")

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        evidence_path=report_path,
        activity_budget_seconds=100,
        utc_now=lambda: now,
        large_target_count=1,
    )

    assert warning is not None
    assert warning["evidence"]["status"] == "mismatch"
    assert "fingerprint differs" in str(warning["evidence"]["reason"])


def test_quality_run_warning_allows_explicit_stale_bypass(
    tmp_path: Path,
) -> None:
    """Operators can explicitly bypass age checks without bypassing scope."""
    data_dir = tmp_path / "data"
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    generated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    evidence = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        activity_budget_seconds=100,
        utc_now=lambda: generated_at,
    )
    report_path = write_quality_preflight_report(
        evidence,
        tmp_path / "preflight.json",
    )

    warning = quality_run_preflight_warning(
        (data_dir,),
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        evidence_path=report_path,
        evidence_max_age_seconds=60,
        allow_stale_evidence=True,
        activity_budget_seconds=100,
        utc_now=lambda: generated_at + timedelta(days=30),
        large_target_count=1,
    )

    assert warning is None


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
