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


def test_quality_remediation_catalog_cli_reports_json(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The remediation-catalog command should expose JSON audit output."""
    captured: dict[str, object] = {}

    def fake_audit(
        report_paths: list[str], **kwargs: object
    ) -> dict[str, object]:
        captured["report_paths"] = report_paths
        captured.update(kwargs)
        return _catalog_payload(gap_count=1)

    monkeypatch.setattr(
        quality_cli,
        "audit_remediation_catalog_report_paths",
        fake_audit,
    )

    exit_code = main(
        [
            "remediation-catalog",
            "--report",
            "reports/quality.json",
            "--code-limit",
            "2",
            "--rule-limit",
            "3",
            "--source-limit",
            "1",
            "--target-axis-limit",
            "4",
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["status"] == "needs-remediation-guidance"
    assert captured["report_paths"] == ["reports/quality.json"]
    assert captured["code_limit"] == 2
    assert captured["rule_limit"] == 3
    assert captured["source_limit"] == 1
    assert captured["target_axis_limit"] == 4


def test_quality_remediation_catalog_cli_reports_ranked_human_output(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The text command should expose the ranked remediation backlog."""

    def fake_audit(
        report_paths: list[str], **kwargs: object
    ) -> dict[str, object]:
        return _catalog_payload(gap_count=1, ranked_gap=True)

    monkeypatch.setattr(
        quality_cli,
        "audit_remediation_catalog_report_paths",
        fake_audit,
    )

    exit_code = main(["remediation-catalog"])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Ranked remediation gaps" in output
    assert "#1 warning CLI_GAP family=time" in output
    assert "report_occurrences=3" in output


def test_quality_remediation_catalog_cli_applies_yaml_defaults(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML defaults should support remediation-catalog audit options."""
    captured: dict[str, object] = {}

    def fake_audit(
        report_paths: list[str], **kwargs: object
    ) -> dict[str, object]:
        captured["report_paths"] = report_paths
        captured.update(kwargs)
        return _catalog_payload(gap_count=0)

    monkeypatch.setattr(
        quality_cli,
        "audit_remediation_catalog_report_paths",
        fake_audit,
    )
    config_path = tmp_path / "quality.yaml"
    config_path.write_text(
        """
histdatacom:
  quality:
    command: remediation-catalog
    reports:
      - reports/one.json
      - reports/two.json
    code_limit: 5
    rule_limit: 6
    source_limit: 7
    target_axis_limit: 8
    json: true
""",
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path)])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == "covered"
    assert captured["report_paths"] == [
        "reports/one.json",
        "reports/two.json",
    ]
    assert captured["code_limit"] == 5
    assert captured["rule_limit"] == 6
    assert captured["source_limit"] == 7
    assert captured["target_axis_limit"] == 8


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


def _catalog_payload(
    *,
    gap_count: int,
    ranked_gap: bool = False,
) -> dict[str, object]:
    ranked_gaps: list[dict[str, object]] = []
    if ranked_gap:
        ranked_gaps.append(
            {
                "finding_code": "CLI_GAP",
                "known_source_occurrence_count": 0,
                "mapped": False,
                "max_severity": "warning",
                "rank": 1,
                "rank_reasons": [
                    "severity=warning",
                    "source_family=time",
                    "report_occurrences=3",
                    "known_sources=0",
                ],
                "report_occurrence_count": 3,
                "rule_id": "time.ascii.sequence",
                "source_family": "time",
            }
        )
    return {
        "schema_version": "histdatacom.quality-remediation-catalog-audit.v1",
        "status": ("needs-remediation-guidance" if gap_count else "covered"),
        "summary": {
            "known_code_count": 1,
            "known_finding_occurrence_count": 1,
            "known_warning_error_code_count": 1,
            "mapped_known_code_count": 0 if gap_count else 1,
            "report_count": 0,
            "report_finding_count": 0,
            "report_mapped_finding_count": 0,
            "report_unmapped_finding_count": 0,
            "report_unmapped_warning_error_group_count": 0,
            "unmapped_info_only_code_count": 0,
            "unmapped_known_code_count": 1 if gap_count else 0,
            "unmapped_warning_error_code_count": gap_count,
            "unmapped_warning_error_gap_count": gap_count,
        },
        "known_code_counts": {},
        "known_unmapped_codes": [],
        "ranked_gaps": ranked_gaps,
        "report_coverage": [],
        "payload_limits": {},
    }
