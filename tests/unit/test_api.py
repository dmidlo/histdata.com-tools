"""Pytest unit tests for histdatacom.api.py."""
from __future__ import annotations

import importlib
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest


FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"

EXPECTED_M1_COLUMNS = ["datetime", "open", "high", "low", "close", "vol"]
EXPECTED_TICK_COLUMNS = ["datetime", "bid", "ask", "vol"]


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
    """Record-based ingest should use the same Polars raw CSV loader."""
    import polars as pl

    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="M1")
    frame = Api._import_file_to_polars(
        record,
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == EXPECTED_M1_COLUMNS


def test_import_file_to_polars_reads_zip_archives(tmp_path: Path) -> None:
    """Record-based ingest should support downloaded ZIP archives."""
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


def test_import_file_to_polars_preserves_system_exit_on_bad_timeframe() -> None:
    """The record wrapper should preserve existing SystemExit behavior."""
    from histdatacom.api import Api

    record = SimpleNamespace(data_timeframe="T_LAST")

    with pytest.raises(SystemExit):
        Api._import_file_to_polars(
            record,
            FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv",
        )
