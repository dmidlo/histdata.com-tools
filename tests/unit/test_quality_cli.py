"""Tests for quality utility CLI commands."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pytest

import histdatacom.histdata_com as histdata_com
import histdatacom.quality_cli as quality_cli
from histdatacom.data_quality import (
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    write_quality_report,
)
from histdatacom.data_quality.preflight import (
    run_cache_quality_preflight,
    write_quality_preflight_report,
)
from histdatacom.data_quality.fingerprint_discovery import (
    TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION,
)
from histdatacom.data_quality.fingerprints import (
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
)
from histdatacom.data_quality.profiles import QUALITY_PROFILE_SCHEMA_VERSION
from histdatacom.data_quality.synthetic_constraints import (
    SYNTHETIC_VALIDATION_SCHEMA_VERSION,
    synthetic_constraints_from_fingerprint,
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


def test_quality_repair_plan_cli_emits_bounded_non_mutating_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """The repair-plan command should translate a report without changing data."""
    archive = tmp_path / "DAT_ASCII_EURUSD_T_201202.zip"
    archive.write_bytes(b"not a zip")
    target = QualityTarget(
        path=str(archive),
        kind=QualityTargetKind.ZIP,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="ZIP_CORRUPT",
        message="ZIP archive could not be opened.",
        rule_id="inventory.zip.integrity",
        target=target,
        metadata={"error_type": "BadZipFile"},
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="inventory.zip.integrity",
                target=target,
                findings=(finding,),
            ),
        ),
    )
    report_path = tmp_path / "quality.json"
    write_quality_report(report, report_path)
    archive_before = archive.read_bytes()
    report_before = report_path.read_bytes()

    exit_code = main(
        [
            "repair-plan",
            "--report",
            str(report_path),
            "--item-limit",
            "1",
            "--evidence-limit",
            "1",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["schema_version"] == "histdatacom.quality-repair-plan.v1"
    assert payload["mode"] == "non_mutating"
    assert payload["apply_supported"] is False
    assert payload["items"][0]["operation"]["category"] == (
        "redownload_archive"
    )
    assert str(tmp_path) not in captured.out
    assert archive.read_bytes() == archive_before
    assert report_path.read_bytes() == report_before


def test_quality_repair_plan_cli_human_output_is_concise(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The repair-plan command should explain missing plans without failing."""
    report_path = Path(
        "tests/fixtures/data_quality_reports/corrupt_zip_report.json"
    )

    exit_code = main(["repair-plan", "--report", str(report_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Quality repair plan" in captured.out
    assert "mode: non_mutating" in captured.out
    assert "ZIP_CORRUPT" in captured.out
    assert "redownload_archive" in captured.out
    assert "error_type" not in captured.out


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
    assert "Remediation plan" in output
    assert "#1 high/90 CLI_GAP selector=exact_rule_and_finding" in output
    assert "#1 warning CLI_GAP family=time" in output
    assert "attribution=inferred(unique_helper_rule)" in output
    assert (
        "actionability=remediable_defect(unmapped_warning_or_error)" in output
    )
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


def test_quality_fingerprint_schema_cli_reports_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """The fingerprint schema command should expose machine-readable JSON."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "cli-fingerprint-profile",
                "rules": {
                    SERIES_FINGERPRINT_RULE_ID: {
                        "quantiles": [0.2, 0.5, 0.8],
                        "lags": [1, 2],
                        "max_rows": 25,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "fingerprint-schema",
            "--quality-profile",
            str(profile_path),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert (
        payload["schema_version"]
        == TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION
    )
    assert payload["profile"]["name"] == "cli-fingerprint-profile"
    assert payload["profile"]["source_path"] == "quality-profile.json"
    assert payload["profile"]["effective_fingerprint_profile"]["lags"] == [
        1,
        2,
    ]
    assert str(tmp_path) not in output


def test_quality_fingerprint_schema_cli_reports_human_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Default fingerprint schema output should be concise text."""
    exit_code = main(["fingerprint-schema"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Fingerprint Schema Discovery" in output
    assert "Implemented Sections" in output
    assert "- microstructure_dynamics: implemented; timeframes=[T]" in output


def test_quality_fingerprint_schema_cli_verifies_contract_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fingerprint schema verify mode should emit machine-readable audit."""
    exit_code = main(["fingerprint-schema", "--verify", "--json"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["schema_version"] == (
        "histdatacom.time-series-fingerprint-contract-audit.v1"
    )
    assert payload["status"] == "pass"
    assert payload["error_count"] == 0
    assert payload["findings"] == []


def test_quality_fingerprint_schema_cli_verifies_contract_text(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fingerprint schema verify mode should render human audit text."""
    exit_code = main(["fingerprint-schema", "--verify"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Fingerprint Contract Audit" in output
    assert "status: pass" in output
    assert "No contract drift detected." in output


def test_quality_bounded_payload_contract_cli_reports_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bounded payload contract command should emit machine-readable audit."""
    exit_code = main(["bounded-payload-contract", "--json"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["schema_version"] == (
        "histdatacom.bounded-payload-contract-audit.v1"
    )
    assert payload["status"] == "pass"
    assert payload["finding_count"] == 0
    assert payload["payload_source"] == "representative"


def test_quality_bounded_payload_contract_cli_reports_human_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bounded payload contract command should render concise text."""
    exit_code = main(["bounded-payload-contract"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Bounded Payload Contract Audit" in output
    assert "status: pass" in output
    assert "No bounded payload contract drift detected." in output


def test_quality_synthetic_validate_cli_compares_saved_reports(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Synthetic validation should use the saved quality-report CLI path."""
    report_path = _write_fingerprint_quality_report(tmp_path)

    exit_code = main(
        [
            "synthetic-validate",
            "--reference-report",
            str(report_path),
            "--candidate-report",
            str(report_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["schema_version"] == SYNTHETIC_VALIDATION_SCHEMA_VERSION
    assert payload["status"] == "mismatch"
    assert payload["mismatched_target_count"] == 1
    assert "synthetic_candidate_avoid_duplicate_timestamps_present" in (
        payload["mismatch_code_counts"]
    )


def test_quality_fingerprint_schema_cli_applies_yaml_defaults(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """YAML defaults should support fingerprint-schema discovery."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "yaml-fingerprint-profile",
                "rules": {SERIES_FINGERPRINT_RULE_ID: {"rounding_digits": 4}},
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "quality.yaml"
    config_path.write_text(
        f"""
histdatacom:
  quality:
    command: fingerprint_schema
    quality_profile: {profile_path}
    json: true
""",
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["profile"]["name"] == "yaml-fingerprint-profile"
    assert (
        payload["profile"]["effective_fingerprint_profile"]["rounding_digits"]
        == 4
    )


def test_quality_fingerprint_schema_cli_accepts_yaml_verify(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """YAML defaults should support fingerprint-schema contract verification."""
    config_path = tmp_path / "quality.yaml"
    config_path.write_text(
        """
histdatacom:
  quality:
    command: fingerprint_schema
    verify: true
    json: true
""",
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["schema_version"] == (
        "histdatacom.time-series-fingerprint-contract-audit.v1"
    )
    assert payload["status"] == "pass"


def test_quality_fingerprint_readiness_cli_reports_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """The readiness command should rank risks from a saved report."""
    report_path = _write_fingerprint_quality_report(tmp_path)

    exit_code = main(
        [
            "fingerprint-readiness",
            "--report",
            str(report_path),
            "--target-limit",
            "1",
            "--section-limit",
            "2",
            "--reason-limit",
            "3",
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    summary = payload["reports"][0]["summary"]

    assert exit_code == 0
    assert payload["schema_version"] == (
        quality_cli.FINGERPRINT_READINESS_RISK_COMMAND_SCHEMA_VERSION
    )
    assert payload["report_count"] == 1
    assert payload["risk_report_count"] == 1
    assert payload["reports"][0]["report_path"] == "fingerprint-quality.json"
    assert (
        summary["schema_version"]
        == TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION
    )
    assert summary["included_target_count"] == 1
    assert summary["target_risks"][0]["target_axis"]["symbol"] == "GBPUSD"
    assert (
        "invalid_timestamps_skipped"
        in summary["target_risks"][0]["reason_codes"]
    )
    assert str(tmp_path) not in output


def test_quality_fingerprint_readiness_cli_reports_human_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """The readiness command should render a concise text ranking."""
    report_path = _write_fingerprint_quality_report(tmp_path)

    exit_code = main(["fingerprint-readiness", "--report", str(report_path)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Fingerprint readiness risk command" in output
    assert "Report: fingerprint-quality.json" in output
    assert "Fingerprint readiness risk" in output
    assert "#1 ascii GBPUSD T 201202 csv: high" in output
    assert "invalid_timestamps_skipped" in output
    assert str(tmp_path) not in output


def test_quality_fingerprint_readiness_cli_recommends_next_work(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The readiness command should optionally render bounded next work."""
    report_path = Path(
        "tests/fixtures/data_quality_reports/fingerprint_report.json"
    )

    exit_code = main(
        [
            "fingerprint-readiness",
            "--report",
            str(report_path),
            "--next-work",
            "--alternate-limit",
            "1",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["next_work"]["schema_version"] == (
        "histdatacom.fingerprint-next-work.v1"
    )
    assert payload["next_work"]["recommendation"]["rank"] == 1
    assert len(payload["next_work"]["alternates"]) == 1
    assert payload["next_work"]["basis"]["market_data_rescanned"] is False

    exit_code = main(
        [
            "fingerprint-readiness",
            "--report",
            str(report_path),
            "--next-work",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Next fingerprint work" in output
    assert "suggested acceptance criteria:" in output
    assert "does not" not in output


def test_quality_help_advertises_fingerprint_schema_command() -> None:
    """The quality utility help should expose fingerprint schema discovery."""
    help_text = quality_cli.build_parser().format_help()

    assert "fingerprint-schema" in help_text
    assert "discover fingerprint schemas" in help_text
    assert "fingerprint-readiness" in help_text
    assert "rank fingerprint readiness risks" in help_text


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


def _write_fingerprint_quality_report(tmp_path: Path) -> Path:
    target = QualityTarget(
        path=str(tmp_path / "DAT_ASCII_GBPUSD_T_201202.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
        symbol="GBPUSD",
        period="201202",
    )
    payload = {
        "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        "target_axis": {
            "data_format": "ascii",
            "timeframe": "T",
            "symbol": "GBPUSD",
            "period": "201202",
            "kind": "csv",
        },
        "source": {"kind": "text"},
        "temporal_topology": {
            "row_count": 4,
            "parsed_row_count": 3,
            "invalid_timestamp_count": 1,
            "duplicate_timestamp_count": 1,
            "non_monotonic_count": 1,
            "suspicious_gap_count": 1,
            "expected_session_closure_count": 0,
            "weekend_activity_count": 0,
            "sequence_status": "limited",
            "limitations": [
                "invalid_timestamps_skipped",
                "duplicate_timestamps",
                "suspicious_gaps",
            ],
        },
        "microstructure_dynamics": {
            "basis": "text",
            "row_order": "source_text_order",
            "computed_from": "text",
            "regular_grid": False,
            "row_count": 4,
            "sampled_row_count": 4,
            "usable_row_count": 3,
            "invalid_row_count": 1,
            "partial_row_count": 0,
            "limitations": ["invalid_timestamps_skipped"],
            "spread_change": {"count": 2},
        },
        "dependence": {
            "basis": "text",
            "acf_basis": "observed_sequence",
            "row_order": "source_text_order",
            "computed_from": "text",
            "regular_grid": False,
            "reason": "invalid_timestamps_skipped",
            "limitations": ["invalid_timestamps_skipped"],
            "row_count": 4,
            "sampled_row_count": 4,
            "usable_row_count": 3,
            "lags": [1, 3],
            "computed_lag_count": 1,
            "skipped_lag_count": 1,
            "spread_acf": {
                "sample_count": 2,
                "computed_lag_count": 1,
                "skipped_lag_count": 1,
                "skipped_lag_reason_counts": {
                    "insufficient_sample_count": 1,
                },
            },
        },
        "fingerprint_audit": {
            "schema_version": TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
            "sections_expected": [
                "coverage",
                "temporal_topology",
                "microstructure_dynamics",
                "dependence",
            ],
            "sections_emitted": [
                "temporal_topology",
                "microstructure_dynamics",
                "dependence",
            ],
            "sections_skipped": {},
            "section_statuses": {
                "coverage": "valid",
                "temporal_topology": "limited",
                "microstructure_dynamics": "limited",
                "dependence": "limited",
            },
            "dynamics_readiness": {
                "microstructure_dynamics": {
                    "status": "limited",
                    "reason": "invalid_timestamps_skipped",
                    "basis": "text",
                    "row_order": "source_text_order",
                    "computed_from": "text",
                    "regular_grid": False,
                    "limitations": ["invalid_timestamps_skipped"],
                    "row_count": 4,
                    "sampled_row_count": 4,
                    "usable_row_count": 3,
                    "invalid_row_count": 1,
                    "partial_row_count": 0,
                }
            },
        },
    }
    payload["synthetic_constraints"] = synthetic_constraints_from_fingerprint(
        payload
    )
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={TIME_SERIES_FINGERPRINT_METADATA_KEY: payload},
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=target,
                findings=(finding,),
            ),
        ),
        metadata={"operation": "data-quality", "check_groups": ["fingerprint"]},
    )
    report_path = tmp_path / "fingerprint-quality.json"
    write_quality_report(report, report_path)
    return report_path


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
                "attribution_status": "inferred",
                "attribution_reason": "unique_helper_rule",
                "actionability": "remediable_defect",
                "actionability_reason": "unmapped_warning_or_error",
            }
        )
    remediation_plan: dict[str, object] = {
        "schema_version": "histdatacom.quality-remediation-plan.v1",
        "plan_item_count": 0,
        "included_plan_item_count": 0,
        "omitted_plan_item_count": 0,
        "truncated": False,
        "actionability_counts": {},
        "fixability_counts": {},
        "items": [],
    }
    if ranked_gap:
        remediation_plan.update(
            {
                "plan_item_count": 1,
                "included_plan_item_count": 1,
                "actionability_counts": {"remediable_defect": 1},
                "fixability_counts": {"high": 1},
                "items": [
                    {
                        "rank": 1,
                        "finding_code": "CLI_GAP",
                        "rule_id": "time.ascii.sequence",
                        "suggested_selector": {
                            "shape": "exact_rule_and_finding"
                        },
                        "suggested_action": {"action_kind": "inspect"},
                        "fixability": {
                            "level": "high",
                            "score": 90,
                            "confidence": "high",
                        },
                        "missing_fields": ["message"],
                    }
                ],
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
            "exact_attribution_occurrence_count": 0,
            "inferred_attribution_occurrence_count": 1,
            "unresolved_attribution_occurrence_count": 0,
        },
        "known_code_counts": {},
        "known_unmapped_codes": [],
        "ranked_gaps": ranked_gaps,
        "remediation_plan": remediation_plan,
        "report_coverage": [],
        "payload_limits": {},
    }
