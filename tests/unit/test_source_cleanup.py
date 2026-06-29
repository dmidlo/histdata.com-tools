"""Tests for transient source-artifact cleanup."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom import histdata_com
from histdatacom.cache_status import collect_cache_run_status
from histdatacom.fx_enums import MAJOR_TRIANGLE_SYMBOLS, PAIR_GROUPS
from histdatacom.cleanup_cli import main as cleanup_main
from histdatacom.source_cleanup import cleanup_transient_source_artifacts


def test_source_cleanup_dry_run_preserves_sources_and_caches(
    tmp_path: Path,
) -> None:
    """Dry-run reports removable source files without deleting anything."""
    data_dir = _write_cleanup_case(tmp_path)

    result = cleanup_transient_source_artifacts(data_dir)
    artifact_dir = _case_artifact_dir(data_dir)

    assert result.dry_run is True
    assert result.matched_count == 4
    assert result.deleted_count == 0
    assert (artifact_dir / "HISTDATA_COM_ASCII_EURUSD_T202001.zip").exists()
    assert (artifact_dir / "DAT_ASCII_EURUSD_T_202001.csv").exists()
    assert (artifact_dir / "quality.xlsx").exists()
    assert (artifact_dir / "notes.xls").exists()
    assert (artifact_dir / ".data").exists()
    assert (artifact_dir / "README.txt").exists()


def test_source_cleanup_apply_deletes_only_transient_sources(
    tmp_path: Path,
) -> None:
    """Apply mode removes source artifacts and leaves internal caches alone."""
    data_dir = _write_cleanup_case(tmp_path)

    result = cleanup_transient_source_artifacts(data_dir, apply=True)
    artifact_dir = _case_artifact_dir(data_dir)

    assert result.dry_run is False
    assert result.matched_count == 4
    assert result.deleted_count == 4
    assert not (artifact_dir / "HISTDATA_COM_ASCII_EURUSD_T202001.zip").exists()
    assert not (artifact_dir / "DAT_ASCII_EURUSD_T_202001.csv").exists()
    assert not (artifact_dir / "quality.xlsx").exists()
    assert not (artifact_dir / "notes.xls").exists()
    assert (artifact_dir / ".data").exists()
    assert (artifact_dir / "README.txt").exists()


def test_cleanup_cli_dry_run_json_reports_matches(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The cleanup CLI exposes a machine-readable dry-run path."""
    data_dir = _write_cleanup_case(tmp_path)

    assert (
        cleanup_main(
            [
                "sources",
                "--data-directory",
                str(data_dir),
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["matched_count"] == 4
    assert payload["deleted_count"] == 0
    assert (
        _case_artifact_dir(data_dir) / "HISTDATA_COM_ASCII_EURUSD_T202001.zip"
    ).exists()


def test_cache_status_reports_group_cleanup_and_missing_symbols(
    tmp_path: Path,
) -> None:
    """Cache status should summarize group caches and cleanup state."""
    data_dir = _write_cleanup_case(tmp_path)

    result = collect_cache_run_status(
        data_dir,
        pair_groups=("majors",),
        timeframes=("tick-data-quotes",),
        formats=("ascii",),
        runtime={"state": "stopped", "message": "runtime is stopped"},
        job_snapshots=(),
    )
    payload = result.to_dict()

    assert payload["status"] == "pending-cleanup"
    assert payload["summary"]["cache_count"] == 1
    assert payload["summary"]["source_artifact_count"] == 4
    assert payload["cleanup"]["state"] == "pending"
    assert payload["groups"][0]["group"] == "majors"
    assert payload["groups"][0]["expected_symbol_count"] == 7
    assert "usdjpy" in payload["groups"][0]["missing_symbols"]
    assert any(
        "histdatacom cleanup sources" in step for step in payload["next_steps"]
    )


def test_cache_status_reports_major_triangle_group_scope(
    tmp_path: Path,
) -> None:
    """Cache status should summarize the full major-triangle basket."""
    data_dir = _write_cleanup_case(tmp_path)

    result = collect_cache_run_status(
        data_dir,
        pair_groups=("major-triangles",),
        timeframes=("tick-data-quotes",),
        formats=("ascii",),
        runtime={"state": "stopped", "message": "runtime is stopped"},
        job_snapshots=(),
    )
    payload = result.to_dict()

    assert payload["groups"][0]["group"] == "major-triangles"
    assert payload["groups"][0]["expected_symbol_count"] == len(
        MAJOR_TRIANGLE_SYMBOLS
    )
    assert "eurusd" not in payload["groups"][0]["missing_symbols"]
    assert "eurjpy" in payload["groups"][0]["missing_symbols"]


def test_cache_status_reports_drained_stuck_workflow(
    tmp_path: Path,
) -> None:
    """File-complete cache scopes should flag active workflows as stuck."""
    data_dir = tmp_path / "data"
    for pair in PAIR_GROUPS["majors"]:
        cache_dir = data_dir / "ASCII" / "T" / pair / "2024" / "1"
        cache_dir.mkdir(parents=True)
        (cache_dir / ".data").write_bytes(b"cache")

    result = collect_cache_run_status(
        data_dir,
        pair_groups=("majors",),
        timeframes=("T",),
        formats=("ASCII",),
        runtime={"state": "running", "message": "runtime is running"},
        job_snapshots=(
            {
                "job_id": "histdatacom-cache",
                "workflow_id": "histdatacom-cache",
                "lifecycle": "running",
                "status": "CACHE_READY",
                "progress": {
                    "current_stage": "build_cache",
                    "completed_children": 7,
                    "total_children": 7,
                },
            },
        ),
    )
    payload = result.to_dict()

    assert payload["status"] == "drained-stuck"
    assert payload["summary"]["missing_symbol_count"] == 0
    assert payload["workflows"]["active_count"] == 1
    assert any(
        "histdatacom jobs inspect histdatacom-cache" in step
        for step in payload["next_steps"]
    )


def test_cleanup_status_cli_json_reports_runtime_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The cleanup status command should remain scriptable via JSON."""
    data_dir = _write_cleanup_case(tmp_path)
    monkeypatch.setattr(
        "histdatacom.cleanup_cli._runtime_status",
        lambda _policy: {"state": "stopped", "message": "runtime is stopped"},
    )
    monkeypatch.setattr(
        "histdatacom.cleanup_cli._job_snapshots",
        lambda _policy: ((), "job-store.sqlite3"),
    )

    assert (
        cleanup_main(
            [
                "status",
                "--data-directory",
                str(data_dir),
                "--pair-groups",
                "majors",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["runtime"]["state"] == "stopped"
    assert payload["workflows"]["state"] == "no-jobs"
    assert payload["summary"]["source_artifact_count"] == 4
    assert payload["cleanup"]["preserves"] == [".data"]
    assert payload["groups"][0]["group"] == "majors"


def test_histdatacom_main_dispatches_cleanup_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The top-level histdatacom command should route cleanup work."""
    data_dir = _write_cleanup_case(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "cleanup",
            "sources",
            "--data-directory",
            str(data_dir),
            "--apply",
            "--json",
        ],
    )

    assert histdata_com.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is False
    assert payload["deleted_count"] == 4
    artifact_dir = _case_artifact_dir(data_dir)
    assert (artifact_dir / ".data").exists()
    assert not (artifact_dir / "DAT_ASCII_EURUSD_T_202001.csv").exists()


def test_cleanup_cli_reads_scoped_yaml_config(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cleanup commands should accept recurrent YAML defaults."""
    data_dir = _write_cleanup_case(tmp_path)
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        f"""
histdatacom:
  cleanup:
    command: sources
    data_directory: {data_dir}
    apply: true
    json: true
""",
        encoding="utf-8",
    )

    assert cleanup_main(["--config", str(config_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is False
    assert payload["deleted_count"] == 4
    artifact_dir = _case_artifact_dir(data_dir)
    assert (artifact_dir / ".data").exists()
    assert not (artifact_dir / "HISTDATA_COM_ASCII_EURUSD_T202001.zip").exists()


def test_cleanup_status_cli_reads_scoped_yaml_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cleanup status should accept recurrent YAML defaults."""
    data_dir = _write_cleanup_case(tmp_path)
    monkeypatch.setattr(
        "histdatacom.cleanup_cli._runtime_status",
        lambda _policy: {"state": "stopped", "message": "runtime is stopped"},
    )
    monkeypatch.setattr(
        "histdatacom.cleanup_cli._job_snapshots",
        lambda _policy: ((), "job-store.sqlite3"),
    )
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        f"""
histdatacom:
  cleanup:
    command: status
    data_directory: {data_dir}
    pair_groups:
      - majors
    timeframes:
      - tick-data-quotes
    formats:
      - ascii
    json: true
""",
        encoding="utf-8",
    )

    assert cleanup_main(["--config", str(config_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["pair_groups"] == ["majors"]
    assert payload["filters"]["timeframes"] == ["T"]
    assert payload["filters"]["formats"] == ["ASCII"]
    assert payload["summary"]["source_artifact_count"] == 4


def _write_cleanup_case(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data" / "ASCII" / "T" / "eurusd" / "2020" / "1"
    data_dir.mkdir(parents=True)
    (data_dir / "HISTDATA_COM_ASCII_EURUSD_T202001.zip").write_bytes(b"zip")
    (data_dir / "DAT_ASCII_EURUSD_T_202001.csv").write_text(
        "timestamp,bid,ask\n",
        encoding="utf-8",
    )
    (data_dir / "quality.xlsx").write_bytes(b"xlsx")
    (data_dir / "notes.xls").write_bytes(b"xls")
    (data_dir / ".data").write_bytes(b"cache")
    (data_dir / "README.txt").write_text("keep\n", encoding="utf-8")
    return tmp_path / "data"


def _case_artifact_dir(data_dir: Path) -> Path:
    return data_dir / "ASCII" / "T" / "eurusd" / "2020" / "1"
