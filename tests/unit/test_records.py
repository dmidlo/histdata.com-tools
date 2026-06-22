"""Pytest unit tests for histdatacom.records.py."""

import json
import os
from pathlib import Path

from histdatacom.records import Record
from histdatacom.runtime_contracts import WorkStatus

ASCII_M1_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/1-minute-bar-quotes/eurusd/2022"
)


def _expected_ascii_m1_dir(base_dir: Path) -> str:
    """Return the canonical ASCII M1 test data directory.

    Args:
        base_dir (Path): base data directory.

    Returns:
        str: canonical ASCII M1 test data directory.
    """
    data_path = Path("ASCII", "M1", "eurusd", "2022")
    record_path = base_dir / data_path
    return f"{record_path}{os.sep}"


def test_records() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_record_data_dir_accepts_base_dir_without_trailing_separator(
    tmp_path: Path,
) -> None:
    """Record paths should not depend on caller-provided trailing separators.

    Args:
        tmp_path (Path): temporary test directory.
    """
    record = Record(url=ASCII_M1_URL)

    record.write_memento_file(base_dir=str(tmp_path))

    assert record.data_dir == _expected_ascii_m1_dir(tmp_path)


def test_record_status_is_normalized_to_work_status() -> None:
    """Record status should no longer be stored as an untyped string."""
    record = Record(url=ASCII_M1_URL, status="CSV_FILE")

    assert record.status is WorkStatus.CSV_FILE
    assert record == record(status="url_valid")
    assert record.status is WorkStatus.URL_VALID
    assert record.legacy_status == "URL_VALID"


def test_record_preserves_unknown_legacy_status_text() -> None:
    """Unknown legacy statuses should stay serializable for migration."""
    record = Record(url=ASCII_M1_URL, status="CUSTOM_STATUS")

    assert record.status is WorkStatus.UNKNOWN
    assert record.status_text == "CUSTOM_STATUS"
    assert record.legacy_status == "CUSTOM_STATUS"
    assert record._to_dict()["status"] == "CUSTOM_STATUS"


def test_memento_writes_do_not_create_legacy_meta_file(
    tmp_path: Path,
) -> None:
    """New metadata writes should use the manifest, not legacy `.meta`.

    Args:
        tmp_path (Path): temporary test directory.
    """
    record = Record(url=ASCII_M1_URL, status="CSV_FILE")

    record.write_memento_file(base_dir=f"{tmp_path}{os.sep}")

    meta_path = tmp_path / "ASCII" / "M1" / "eurusd" / "2022" / ".meta"

    assert not meta_path.exists()


def test_restore_momento_ignores_stale_persisted_data_dir(
    tmp_path: Path,
) -> None:
    """Moved data directories should restore records under the current base.

    Args:
        tmp_path (Path): temporary test directory.
    """
    current_base = tmp_path / "current"
    stale_base = tmp_path / "stale"
    restored = Record(url=ASCII_M1_URL)
    current_data_dir = _expected_ascii_m1_dir(current_base)
    stale_data_dir = _expected_ascii_m1_dir(stale_base)
    meta_path = Path(current_data_dir) / ".meta"
    meta_path.parent.mkdir(parents=True)
    meta_path.write_text(
        json.dumps(
            {
                "url": ASCII_M1_URL,
                "status": "CSV_FILE",
                "data_dir": stale_data_dir,
                "zip_filename": "stale.zip",
            },
        ),
        encoding="UTF-8",
    )

    assert restored.restore_momento(f"{current_base}{os.sep}")
    assert restored.status is WorkStatus.CSV_FILE
    assert restored.zip_filename == "stale.zip"
    assert restored.data_dir == current_data_dir
