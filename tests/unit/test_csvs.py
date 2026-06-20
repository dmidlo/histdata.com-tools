"""Pytest unit tests for histdatacom.csvs.py."""

from __future__ import annotations

import os
import zipfile
from concurrent.futures import Future
from pathlib import Path

import pytest


def test_csvs() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


class _RecordsStub:
    """Minimal queue-like object for direct Csv worker tests."""

    def __init__(self) -> None:
        """Initialize captured queue operations."""
        self.items: list[object] = []
        self.task_done_called = False

    def put(self, item: object) -> None:
        """Record queued item."""
        self.items.append(item)

    def task_done(self) -> None:
        """Record task completion."""
        self.task_done_called = True


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
    current = _RecordsStub()
    next_records = _RecordsStub()

    Csv()._extract_csv(
        record,
        {"default_download_dir": str(tmp_path) + os.sep},
        current,
        next_records,
    )

    assert record.status == "CSV_FILE"
    assert record.csv_filename == "DAT_XLSX_EURUSD_M1_2022.xlsx"
    assert (tmp_path / record.csv_filename).read_bytes() == b"spreadsheet"
    assert not archive_path.exists()
    assert current.task_done_called
    assert next_records.items == [record]


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
    current = _RecordsStub()

    with pytest.raises(SystemExit):
        Csv()._extract_csv(
            record,
            {"default_download_dir": str(tmp_path) + os.sep},
            current,
            _RecordsStub(),
        )

    assert current.task_done_called


def test_complete_future_propagates_worker_exceptions() -> None:
    """Thread and process pool helpers should not hide worker failures."""
    from histdatacom.concurrency import _complete_future

    class ProgressStub:
        """Minimal progress object for the helper under test."""

        def __init__(self) -> None:
            """Initialize captured progress calls."""
            self.advanced_by: list[float] = []

        def advance(self, task_id: int, amount: float) -> None:
            """Record progress advancement."""
            self.advanced_by.append(amount)

    progress = ProgressStub()
    future: Future = Future()
    future.set_exception(RuntimeError("worker failed"))
    futures = [future]

    with pytest.raises(RuntimeError, match="worker failed"):
        _complete_future(progress, 1, futures, future)

    assert progress.advanced_by == [0.75]
    assert futures == []
