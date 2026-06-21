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

from histdatacom.histdata_ascii import CACHE_FILENAME, write_polars_cache

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"

EXPECTED_M1_COLUMNS = ["datetime", "open", "high", "low", "close", "vol"]
EXPECTED_TICK_COLUMNS = ["datetime", "bid", "ask", "vol"]
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]
EXPECTED_TICK_DATETIMES = [1328072403660, 1328072403973, 1328072414990]


def _write_cache_record(
    tmp_path: Path,
    dirname: str,
    frame: object,
    *,
    pair: str,
    timeframe: str,
    start: int,
) -> SimpleNamespace:
    """Write a cache frame and return the minimal merge record shape."""
    data_dir = tmp_path / dirname
    data_dir.mkdir()
    write_polars_cache(frame, data_dir / CACHE_FILENAME)
    return SimpleNamespace(
        data_dir=str(data_dir) + os.sep,
        cache_filename=CACHE_FILENAME,
        cache_start=str(start),
        data_fxpair=pair,
        data_timeframe=timeframe,
    )


def test_api() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_api_module_imports_cleanly() -> None:
    """Raw Polars ingest should keep API imports lightweight."""
    sys.modules.pop("histdatacom.api", None)

    module = importlib.import_module("histdatacom.api")

    assert module.Api.__name__ == "Api"


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
        frame.schema[column] == pl.Float64 for column in expected_columns[1:-1]
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
    assert (
        frame.select("datetime").to_series().to_list() == EXPECTED_M1_DATETIMES
    )


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
    assert (
        frame.select("datetime").to_series().to_list()
        == EXPECTED_TICK_DATETIMES
    )
    assert frame.height == 3


def test_import_file_to_polars_preserves_system_exit_on_bad_timeframe() -> None:
    """The record wrapper should preserve existing SystemExit behavior."""
    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="T_LAST")

    with pytest.raises(SystemExit):
        Api._import_file_to_polars(
            record,
            FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
        )


def test_create_cache_writes_polars_cache_and_record_metadata(
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

    Api._create_cache(
        record,
        {"default_download_dir": str(tmp_path) + os.sep},
    )
    cache_frame = Api.import_cache_data(str(tmp_path / CACHE_FILENAME))

    assert record.cache_filename == CACHE_FILENAME
    assert record.cache_line_count == 3
    assert record.cache_start == str(EXPECTED_M1_DATETIMES[0])
    assert record.cache_end == str(EXPECTED_M1_DATETIMES[-1])
    assert (tmp_path / CACHE_FILENAME).exists()
    assert (tmp_path / ".meta").exists()
    assert isinstance(cache_frame, pl.DataFrame)
    assert cache_frame.schema["datetime"] == pl.Int64
    assert cache_frame.select("datetime").to_series().to_list() == (
        EXPECTED_M1_DATETIMES
    )


def test_import_cache_data_reads_polars_cache_without_legacy_backend(
    tmp_path: Path,
) -> None:
    """The cache reader should not import the retired backend at runtime."""
    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="T")
    frame = Api._import_file_to_polars(
        record,
        FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
    )

    Api._write_cache_data(frame, str(tmp_path / CACHE_FILENAME))
    cache_frame = Api.import_cache_data(str(tmp_path / CACHE_FILENAME))

    assert cache_frame.to_dicts() == frame.to_dicts()


def test_merge_records_reads_polars_cache_files(
    tmp_path: Path,
) -> None:
    """Merge should read the replacement cache format from disk."""
    import polars as pl

    from histdatacom import config
    from histdatacom.api import Api

    api = Api()
    source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="M1"),
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    first = _write_cache_record(
        tmp_path,
        "first",
        source.slice(0, 1),
        pair="eurusd",
        timeframe="M1",
        start=EXPECTED_M1_DATETIMES[0],
    )
    second = _write_cache_record(
        tmp_path,
        "second",
        source.slice(1, 2),
        pair="eurusd",
        timeframe="M1",
        start=EXPECTED_M1_DATETIMES[1],
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


def test_merge_records_empty_set_returns_empty_polars_dataframe() -> None:
    """Empty merge sets should produce an intentional empty Polars frame."""
    import polars as pl

    from histdatacom import config
    from histdatacom.api import Api

    original_args = config.ARGS.copy()

    try:
        config.ARGS["api_return_type"] = "polars"
        tp_set = {
            "timeframe": "M1",
            "pair": "eurusd",
            "records": [],
            "data": None,
        }
        Api()._merge_records(tp_set)
    finally:
        config.ARGS = original_args

    assert isinstance(tp_set["data"], pl.DataFrame)
    assert tp_set["data"].is_empty()


@pytest.mark.parametrize(
    "api_return_type",
    ("polars", "pandas", "arrow"),
)
def test_merge_caches_converts_single_pair_timeframe_return_types(
    tmp_path: Path, api_return_type: str
) -> None:
    """A single observed pair/timeframe should return the requested type."""
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from histdatacom import config
    from histdatacom.api import Api
    from histdatacom.records import Records

    source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="M1"),
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    first = _write_cache_record(
        tmp_path,
        "single-first",
        source.slice(0, 1),
        pair="eurusd",
        timeframe="M1",
        start=EXPECTED_M1_DATETIMES[0],
    )
    second = _write_cache_record(
        tmp_path,
        "single-second",
        source.slice(1, 2),
        pair="eurusd",
        timeframe="M1",
        start=EXPECTED_M1_DATETIMES[1],
    )
    records = Records()
    records.put(second)
    records.put(first)
    original_args = config.ARGS.copy()
    original_current_queue = config.CURRENT_QUEUE

    try:
        config.ARGS["api_return_type"] = api_return_type
        config.CURRENT_QUEUE = records
        result = Api().merge_caches()
    finally:
        config.ARGS = original_args
        config.CURRENT_QUEUE = original_current_queue

    if api_return_type == "polars":
        assert isinstance(result, pl.DataFrame)
        values = result.select("datetime").to_series().to_list()
    elif api_return_type == "pandas":
        assert isinstance(result, pd.DataFrame)
        values = result["datetime"].tolist()
    else:
        assert isinstance(result, pa.Table)
        values = result.column("datetime").to_pylist()

    assert values == EXPECTED_M1_DATETIMES


def test_merge_caches_returns_only_observed_pair_timeframe_sets(
    tmp_path: Path,
) -> None:
    """Merge collation should not emit empty cross-product result sets."""
    import polars as pl

    from histdatacom import config
    from histdatacom.api import Api
    from histdatacom.records import Records

    m1_source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="M1"),
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    tick_source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="T"),
        FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
    )
    tick_record = _write_cache_record(
        tmp_path,
        "gbpusd-tick",
        tick_source,
        pair="gbpusd",
        timeframe="T",
        start=EXPECTED_TICK_DATETIMES[0],
    )
    m1_record = _write_cache_record(
        tmp_path,
        "eurusd-m1",
        m1_source,
        pair="eurusd",
        timeframe="M1",
        start=EXPECTED_M1_DATETIMES[0],
    )
    records = Records()
    records.put(tick_record)
    records.put(m1_record)
    original_args = config.ARGS.copy()
    original_current_queue = config.CURRENT_QUEUE

    try:
        config.ARGS["api_return_type"] = "polars"
        config.CURRENT_QUEUE = records
        result = Api().merge_caches()
    finally:
        config.ARGS = original_args
        config.CURRENT_QUEUE = original_current_queue

    assert isinstance(result, list)
    assert [(item["timeframe"], item["pair"]) for item in result] == [
        ("T", "gbpusd"),
        ("M1", "eurusd"),
    ]
    assert all(
        set(item) == {"timeframe", "pair", "records", "data"} for item in result
    )
    assert [len(item["records"]) for item in result] == [1, 1]
    assert all(isinstance(item["data"], pl.DataFrame) for item in result)


def test_merge_caches_returns_empty_list_when_no_cache_records(
    tmp_path: Path,
) -> None:
    """Records without cache files should be ignored by merge_caches."""
    from histdatacom import config
    from histdatacom.api import Api
    from histdatacom.records import Records

    missing_cache = SimpleNamespace(
        data_dir=str(tmp_path) + os.sep,
        cache_filename=CACHE_FILENAME,
        data_fxpair="eurusd",
        data_timeframe="M1",
    )
    records = Records()
    records.put(missing_cache)
    original_args = config.ARGS.copy()
    original_current_queue = config.CURRENT_QUEUE

    try:
        config.ARGS["api_return_type"] = "polars"
        config.CURRENT_QUEUE = records
        result = Api().merge_caches()
    finally:
        config.ARGS = original_args
        config.CURRENT_QUEUE = original_current_queue

    assert result == []
