"""Tests for publishable data-quality report generation."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


def test_data_quality_publication_report_sanitizes_existing_json(
    tmp_path: Path,
) -> None:
    """The publication script should clean local paths without rerunning QA."""
    source = tmp_path / "data" / ".quality" / "issue-999"
    source.mkdir(parents=True)
    report = source / "quality.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "histdatacom.quality-report.v1",
                "summary": {
                    "status": "failed",
                    "target_count": 1,
                    "rule_count": 1,
                    "finding_count": 1,
                    "info_count": 0,
                    "warning_count": 0,
                    "error_count": 1,
                },
                "rule_results": [
                    {
                        "rule_id": "inventory.file_exists",
                        "findings": [
                            {
                                "code": "FILE_MISSING",
                                "path": (
                                    "/Users/alice/projects/"
                                    "histdata.com-tools/data/ASCII/M1/"
                                    "eurusd/2012/missing.csv"
                                ),
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "docs" / "report.md"
    index = tmp_path / "docs" / "index.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/data_quality_publication_report.py",
            "--source",
            str(tmp_path / "data" / ".quality"),
            "--output",
            str(output),
            "--index",
            str(index),
            "--sanitize-in-place",
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
    )

    cleaned = report.read_text(encoding="utf-8")
    assert completed.returncode == 0
    assert "/Users/" not in cleaned
    assert "data/ASCII/M1/eurusd/2012/missing.csv" in cleaned
    assert "/Users/" not in index.read_text(encoding="utf-8")
    assert "Data Quality Executive Report" in output.read_text(encoding="utf-8")
