"""Tests for quality utility CLI commands."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pytest

import histdatacom.histdata_com as histdata_com
import histdatacom.quality_cli as quality_cli
from histdatacom.data_quality.preflight import (
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
from histdatacom.quality_cli import main
from tests.fixtures.histdata_ascii.quality_cases import CLEAN_TICK_CASE


def test_main_routes_quality_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level entry point should route quality utility commands."""
    captured: list[str] = []

    def fake_quality_main(argv: list[str]) -> int:
        captured.extend(argv)
        return 7

    monkeypatch.setattr(sys, "argv", ["histdatacom", "quality", "evidence"])
    monkeypatch.setattr(quality_cli, "main", fake_quality_main)

    assert histdata_com.main() == 7
    assert captured == ["evidence"]


def test_quality_evidence_cli_reports_human_accepted_status(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Human output should explain accepted evidence without local paths."""
    data_dir = tmp_path / "data"
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    report_path = _write_preflight_evidence(data_dir, now=now)

    exit_code = main(
        [
            "evidence",
            "--evidence",
            str(report_path),
            "--target",
            str(data_dir),
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "--quality-checks",
            "inventory",
            "--quality-preflight-evidence-stale-ok",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Quality preflight evidence inspection" in captured.out
    assert "status: accepted" in captured.out
    assert "accepted: yes" in captured.out
    assert str(tmp_path) not in captured.out


def test_quality_evidence_cli_reports_json_rejection(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """JSON output should be machine-readable and fail when unusable."""
    data_dir = tmp_path / "data"
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    report_path = _write_preflight_evidence(
        data_dir,
        now=now,
        version="0.0.0",
    )

    exit_code = main(
        [
            "evidence",
            "--evidence",
            str(report_path),
            "--target",
            str(data_dir),
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "--quality-checks",
            "inventory",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 1
    assert payload["status"] == "version-mismatch"
    assert payload["accepted"] is False
    assert payload["evidence"]["expected_version"]
    assert str(tmp_path) not in captured.out


def test_quality_evidence_cli_applies_yaml_defaults(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Quality utility commands should support recurrent YAML defaults."""
    data_dir = tmp_path / "data"
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _write_tick_cache(data_dir, symbol="eurusd", row_multiplier=1)
    report_path = _write_preflight_evidence(data_dir, now=now)
    config_path = tmp_path / "quality.yaml"
    config_path.write_text(
        f"""
histdatacom:
  quality:
    command: evidence
    evidence: {report_path}
    target: {data_dir}
    pairs: [eurusd]
    formats: [ascii]
    timeframes: [tick-data-quotes]
    quality_checks: [inventory]
    quality_preflight_evidence_stale_ok: true
    json: true
""",
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path)])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["status"] == "accepted"


def _write_preflight_evidence(
    data_dir: Path,
    *,
    now: datetime,
    version: str = "",
) -> Path:
    payload = run_cache_quality_preflight(
        data_dir,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        quality_check_groups=("inventory",),
        sample_size=1,
        utc_now=lambda: now,
    )
    if version:
        payload["package"]["version"] = version
    return write_quality_preflight_report(
        payload,
        data_dir.parent / "preflight.json",
    )


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
