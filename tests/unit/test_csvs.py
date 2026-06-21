"""Pytest unit tests for histdatacom.csvs.py."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path


def test_csvs() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_extract_csv_accepts_xlsx_members(tmp_path: Path) -> None:
    """Excel archives should extract their XLSX payload and update metadata."""
    from histdatacom.csvs import Csv
    from histdatacom.records import Record

    archive_path = tmp_path / "excel.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("DAT_XLSX_EURUSD_M1_2022.xlsx", b"spreadsheet")

    record = Record(
        data_dir=str(tmp_path) + os.sep,
        zip_filename=archive_path.name,
        status="CSV_ZIP",
    )
    result = Csv()._extract_csv(
        record,
        {"default_download_dir": str(tmp_path) + os.sep},
    )

    assert result is record
    assert record.status == "CSV_FILE"
    assert record.csv_filename == "DAT_XLSX_EURUSD_M1_2022.xlsx"
    assert (tmp_path / record.csv_filename).read_bytes() == b"spreadsheet"
    assert not archive_path.exists()


def test_extract_csv_rejects_archives_without_data_members(
    tmp_path: Path,
) -> None:
    """Malformed archives should fail instead of silently leaving ZIPs behind."""
    from histdatacom.csvs import Csv
    from histdatacom.records import Record

    archive_path = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("README.txt", "not market data")

    record = Record(
        data_dir=str(tmp_path) + os.sep,
        zip_filename=archive_path.name,
        status="CSV_ZIP",
    )
    result = Csv()._extract_csv(
        record,
        {"default_download_dir": str(tmp_path) + os.sep},
    )

    assert result is None
    assert record.status == "FAILED"
