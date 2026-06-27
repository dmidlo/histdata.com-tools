"""Tests for transient source-artifact cleanup."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom import histdata_com
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
