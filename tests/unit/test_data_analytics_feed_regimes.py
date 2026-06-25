"""Tests for feed-regime analytics."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from histdatacom.data_analytics import (
    ANALYTICS_REPORT_SCHEMA_VERSION,
    analyze_feed_regimes,
    discover_analytics_targets,
    format_feed_regime_console_summary,
)
from histdatacom.data_analytics.cli import main as analytics_main


def _write_tick_csv(
    root: Path,
    period: str,
    rows: tuple[tuple[str, float, float, int], ...],
) -> Path:
    path = root / f"DAT_ASCII_EURUSD_T_{period}.csv"
    path.write_text(
        "\n".join(
            f"{timestamp},{bid:.5f},{ask:.5f},{volume}"
            for timestamp, bid, ask, volume in rows
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _sampled_long_history_dataset(root: Path) -> tuple[Path, Path]:
    sparse = _write_tick_csv(
        root,
        "200101",
        (
            ("20010102 000000000", 1.00000, 1.00020, 0),
            ("20010102 001000000", 1.00000, 1.00020, 0),
            ("20010102 002000000", 1.00010, 1.00030, 0),
        ),
    )
    dense = _write_tick_csv(
        root,
        "202201",
        tuple(
            (
                f"20220103 00000{second}000",
                1.10000 + second * 0.00001,
                1.10020 + second * 0.00001,
                second,
            )
            for second in range(10)
        ),
    )
    return sparse, dense


def test_discovery_is_separate_from_data_quality_targets(
    tmp_path: Path,
) -> None:
    """Analytics discovery should expose its own target contract."""
    sparse, dense = _sampled_long_history_dataset(tmp_path)

    discovery = discover_analytics_targets((tmp_path,))

    assert {target.path for target in discovery.targets} == {
        str(sparse.resolve()),
        str(dense.resolve()),
    }
    assert all(target.is_supported_tick_target for target in discovery.targets)
    assert discovery.metadata["quality_semantics"] == (
        "analytics-only; no pass/fail status"
    )


def test_feed_regime_analysis_segments_sparse_and_dense_periods(
    tmp_path: Path,
) -> None:
    """A sampled long-history dataset should produce regime boundaries."""
    _sampled_long_history_dataset(tmp_path)

    report = analyze_feed_regimes((tmp_path,), quiet_gap_ms=60_000)
    payload = report.to_dict()
    labels = {regime.label for regime in report.regimes}
    profile_by_period = {
        profile.period: profile for profile in report.period_profiles
    }

    assert payload["schema_version"] == ANALYTICS_REPORT_SCHEMA_VERSION
    assert payload["operation"] == "feed-regime-detection"
    assert payload["summary"]["symbols"] == ["EURUSD"]
    assert {"sparse", "dense"} <= labels
    assert profile_by_period["200101"].quiet_gap_count == 2
    assert profile_by_period["200101"].zero_change_run_count == 1
    assert profile_by_period["202201"].tick_rate_per_hour > (
        profile_by_period["200101"].tick_rate_per_hour
    )
    assert "pass/fail" in payload["metadata"]["quality_semantics"]


def test_feed_regime_report_console_summary(tmp_path: Path) -> None:
    """Console output should stay compact and descriptive."""
    _sampled_long_history_dataset(tmp_path)
    report = analyze_feed_regimes((tmp_path,))

    summary = format_feed_regime_console_summary(report)

    assert "Feed regime analytics" in summary
    assert "regimes: 2" in summary
    assert "EURUSD 200101-200101 sparse" in summary
    assert "EURUSD 202201-202201 dense" in summary


def test_feed_regime_cli_writes_machine_readable_report(
    tmp_path: Path,
    capsys,
) -> None:
    """The analytics subcommand should write structured report JSON."""
    _sampled_long_history_dataset(tmp_path)
    report_path = tmp_path / "reports" / "feed-regimes.json"

    exit_code = analytics_main(
        [
            "feed-regimes",
            "--target",
            str(tmp_path),
            "--report",
            str(report_path),
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Feed regime analytics" in output
    assert f"report: {report_path.resolve()}" in output
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["operation"] == "feed-regime-detection"
    assert payload["summary"]["regime_count"] == 2


def test_top_level_main_dispatches_analytics_subcommand(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """histdatacom analytics should bypass the orchestration request parser."""
    import histdatacom.histdata_com as histdata_com

    _sampled_long_history_dataset(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "analytics",
            "feed-regimes",
            "--target",
            str(tmp_path),
            "--json",
        ],
    )

    assert histdata_com.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == ANALYTICS_REPORT_SCHEMA_VERSION
