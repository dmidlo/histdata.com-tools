"""Pytest unit tests for histdatacom.api.py."""
from __future__ import annotations

import importlib
import os
import shutil
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.histdata_ascii import CACHE_FILENAME


FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"

EXPECTED_M1_COLUMNS = ["datetime", "open", "high", "low", "close", "vol"]
EXPECTED_TICK_COLUMNS = ["datetime", "bid", "ask", "vol"]
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]
EXPECTED_TICK_DATETIMES = [1328072403660, 1328072403973, 1328072414990]


def test_api() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_api_module_import_does_not_import_datatable() -> None:
    """Raw Polars ingest should not require datatable at API import time."""
    sys.modules.pop("histdatacom.api", None)
    sys.modules.pop("datatable", None)

    module = importlib.import_module("histdatacom.api")

    assert module.Api.__name__ == "Api"
    assert "datatable" not in sys.modules


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_columns"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv", EXPECTED_M1_COLUMNS),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv", EXPECTED_TICK_COLUMNS),
    ),
)
def test_import_frame_with_headers_returns_polars_raw_ingest_frame(
    timeframe: str,
    filename: str,
    expected_columns: list[str],
) -> None:
    """The API ingest seam should now return Polars dataframes."""
    import polars as pl

    from histdatacom.api import Api

    frame = Api._import_frame_with_headers(timeframe, FIXTURES / filename)

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == expected_columns
    assert frame.schema["datetime"] == pl.String
    assert frame.schema["vol"] == pl.Int32
    assert all(
        frame.schema[column] == pl.Float64
        for column in expected_columns[1:-1]
    )


def test_import_frame_with_headers_rejects_unsupported_timeframes() -> None:
    """Unsupported HistData layouts should keep failing clearly."""
    from histdatacom.api import Api

    with pytest.raises(ValueError, match="unsupported ASCII timeframe"):
        Api._import_frame_with_headers(
            "T_LAST",
            FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
        )


def test_import_file_to_polars_wraps_raw_ingest_for_records() -> None:
    """Record-based ingest should normalize raw CSV timestamps in Polars."""
    import polars as pl

    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="M1")
    frame = Api._import_file_to_polars(
        record,
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == EXPECTED_M1_COLUMNS
    assert frame.schema["datetime"] == pl.Int64
    assert frame.select("datetime").to_series().to_list() == EXPECTED_M1_DATETIMES


def test_import_file_to_polars_reads_zip_archives(tmp_path: Path) -> None:
    """Record-based ingest should normalize downloaded ZIP archives."""
    import polars as pl

    from histdatacom.api import Api

    filename = "DAT_ASCII_EURUSD_T_201202.csv"
    archive_path = tmp_path / f"{filename}.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(FIXTURES / filename, arcname=filename)

    record = SimpleNamespace(data_timeframe="T")
    frame = Api._import_file_to_polars(record, archive_path)

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == EXPECTED_TICK_COLUMNS
    assert frame.schema["datetime"] == pl.Int64
    assert frame.select("datetime").to_series().to_list() == EXPECTED_TICK_DATETIMES
    assert frame.height == 3


def test_import_file_to_datatable_is_polars_compatibility_wrapper() -> None:
    """The legacy method name should no longer create datatable frames."""
    import polars as pl

    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="T")
    frame = Api._import_file_to_datatable(
        record,
        FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == EXPECTED_TICK_COLUMNS
    assert frame.schema["datetime"] == pl.Int64
    assert frame.select("datetime").to_series().to_list() == EXPECTED_TICK_DATETIMES


def test_import_file_to_polars_preserves_system_exit_on_bad_timeframe() -> None:
    """The record wrapper should preserve existing SystemExit behavior."""
    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="T_LAST")

    with pytest.raises(SystemExit):
        Api._import_file_to_polars(
            record,
            FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
        )


def test_create_jay_writes_polars_cache_and_record_metadata(
    tmp_path: Path,
) -> None:
    """Cache creation should preserve the transitional Record metadata fields."""
    import polars as pl

    from histdatacom.api import Api
    from histdatacom.records import Record

    filename = "DAT_ASCII_EURUSD_M1_201202.csv"
    shutil.copyfile(FIXTURES / filename, tmp_path / filename)
    record = Record(
        data_dir=str(tmp_path) + os.sep,
        csv_filename=filename,
        zip_filename="missing.zip",
        data_timeframe="M1",
        data_format="ascii",
        data_fxpair="eurusd",
        status="CSV",
    )

    Api._create_jay(
        record,
        {"default_download_dir": str(tmp_path) + os.sep},
    )
    cache_frame = Api.import_jay_data(str(tmp_path / CACHE_FILENAME))

    assert record.jay_filename == CACHE_FILENAME
    assert record.jay_line_count == 3
    assert record.jay_start == str(EXPECTED_M1_DATETIMES[0])
    assert record.jay_end == str(EXPECTED_M1_DATETIMES[-1])
    assert (tmp_path / CACHE_FILENAME).exists()
    assert (tmp_path / ".meta").exists()
    assert isinstance(cache_frame, pl.DataFrame)
    assert cache_frame.schema["datetime"] == pl.Int64
    assert cache_frame.select("datetime").to_series().to_list() == (
        EXPECTED_M1_DATETIMES
    )


def test_import_jay_data_reads_polars_cache_without_datatable(
    tmp_path: Path,
) -> None:
    """The cache reader should not import datatable at runtime."""
    from histdatacom.api import Api

    sys.modules.pop("datatable", None)
    record = SimpleNamespace(data_timeframe="T")
    frame = Api._import_file_to_polars(
        record,
        FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
    )

    Api._export_datatable_to_jay(frame, str(tmp_path / CACHE_FILENAME))
    cache_frame = Api.import_jay_data(str(tmp_path / CACHE_FILENAME))

    assert cache_frame.to_dicts() == frame.to_dicts()
    assert "datatable" not in sys.modules


def test_merge_records_reads_polars_cache_files(
    tmp_path: Path,
) -> None:
    """Merge should read the replacement cache format from disk."""
    import polars as pl

    from histdatacom import config
    from histdatacom.api import Api
    from histdatacom.histdata_ascii import write_polars_cache

    api = Api()
    source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="M1"),
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    write_polars_cache(source.slice(0, 1), first_dir / CACHE_FILENAME)
    write_polars_cache(source.slice(1, 2), second_dir / CACHE_FILENAME)
    first = SimpleNamespace(
        data_dir=str(first_dir) + os.sep,
        jay_filename=CACHE_FILENAME,
        jay_start=str(EXPECTED_M1_DATETIMES[0]),
    )
    second = SimpleNamespace(
        data_dir=str(second_dir) + os.sep,
        jay_filename=CACHE_FILENAME,
        jay_start=str(EXPECTED_M1_DATETIMES[1]),
    )
    original_args = config.ARGS.copy()

    try:
        config.ARGS["api_return_type"] = "polars"
        tp_set = {
            "timeframe": "M1",
            "pair": "eurusd",
            "records": [second, first],
            "data": None,
        }
        api._merge_records(tp_set)
    finally:
        config.ARGS = original_args

    assert isinstance(tp_set["data"], pl.DataFrame)
    assert tp_set["data"].select("datetime").to_series().to_list() == (
        EXPECTED_M1_DATETIMES
    )
