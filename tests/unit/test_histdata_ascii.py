"""Domain coverage for HistData ASCII behavior required by Polars migration."""
from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from histdatacom.histdata_ascii import (
    EST_NO_DST_OFFSET_MS,
    M1_COLUMNS,
    TICK_COLUMNS,
    convert_batch_for_api,
    delimiter_for_timeframe,
    format_influx_line,
    merge_batches,
    normalize_ascii_row,
    parse_histdata_datetime_to_utc_ms,
    read_ascii_file,
    read_ascii_file_to_polars,
    raw_polars_schema_for_timeframe,
    rows_as_records,
    summarize_rows,
)


FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"

EXPECTED_M1_ROWS = (
    (1328072400000, 1.3066, 1.3066, 1.30656, 1.30656, 0),
    (1328072460000, 1.30657, 1.30657, 1.30647, 1.30656, 17),
    (1328072520000, 1.30652, 1.30656, 1.30652, 1.30656, 2147483647),
)

EXPECTED_TICK_ROWS = (
    (1328072403660, 1.3066, 1.30677, 0),
    (1328072403973, 1.30658, 1.30675, 25),
    (1328072414990, 1.30657, 1.30674, 2147483647),
)

EXPECTED_RAW_M1_RECORDS = (
    {
        "datetime": "20120201 000000",
        "open": 1.3066,
        "high": 1.3066,
        "low": 1.30656,
        "close": 1.30656,
        "vol": 0,
    },
    {
        "datetime": "20120201 000100",
        "open": 1.30657,
        "high": 1.30657,
        "low": 1.30647,
        "close": 1.30656,
        "vol": 17,
    },
    {
        "datetime": "20120201 000200",
        "open": 1.30652,
        "high": 1.30656,
        "low": 1.30652,
        "close": 1.30656,
        "vol": 2147483647,
    },
)

EXPECTED_RAW_TICK_RECORDS = (
    {
        "datetime": "20120201 000003660",
        "bid": 1.3066,
        "ask": 1.30677,
        "vol": 0,
    },
    {
        "datetime": "20120201 000003973",
        "bid": 1.30658,
        "ask": 1.30675,
        "vol": 25,
    },
    {
        "datetime": "20120201 000014990",
        "bid": 1.30657,
        "ask": 1.30674,
        "vol": 2147483647,
    },
)


def test_columns_and_delimiters_are_locked_for_supported_ascii_timeframes() -> None:
    """Document the two HistData ASCII layouts the API pipeline supports."""
    assert M1_COLUMNS == ("datetime", "open", "high", "low", "close", "vol")
    assert TICK_COLUMNS == ("datetime", "bid", "ask", "vol")
    assert delimiter_for_timeframe("M1") == ";"
    assert delimiter_for_timeframe("T") == ","


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_columns", "expected_rows"),
    (
        (
            "M1",
            "DAT_ASCII_EURUSD_M1_201202.csv",
            M1_COLUMNS,
            EXPECTED_M1_ROWS,
        ),
        (
            "T",
            "DAT_ASCII_EURUSD_T_201202.csv",
            TICK_COLUMNS,
            EXPECTED_TICK_ROWS,
        ),
    ),
)
def test_ascii_csv_fixtures_parse_to_current_domain_values(
    timeframe: str,
    filename: str,
    expected_columns: tuple[str, ...],
    expected_rows: tuple[tuple[object, ...], ...],
) -> None:
    """Lock schema, row count, int32 volume intent, and cache summary values."""
    batch = read_ascii_file(FIXTURES / filename, timeframe)

    assert batch.timeframe == timeframe
    assert batch.columns == expected_columns
    assert batch.rows == expected_rows
    assert batch.summary.line_count == len(expected_rows)
    assert batch.summary.start == expected_rows[0][0]
    assert batch.summary.end == expected_rows[-1][0]
    assert all(isinstance(row[-1], int) for row in batch.rows)
    assert max(row[-1] for row in batch.rows) == 2147483647


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_rows"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv", EXPECTED_M1_ROWS),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv", EXPECTED_TICK_ROWS),
    ),
)
def test_ascii_zip_fixtures_parse_like_direct_csv_files(
    tmp_path: Path,
    timeframe: str,
    filename: str,
    expected_rows: tuple[tuple[object, ...], ...],
) -> None:
    """Downloaded ZIP archives must preserve the same parsed rows as CSV files."""
    source = FIXTURES / filename
    archive_path = tmp_path / f"{filename}.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(source, arcname=filename)

    assert read_ascii_file(archive_path, timeframe).rows == expected_rows


@pytest.mark.parametrize(
    (
        "timeframe",
        "filename",
        "expected_columns",
        "expected_records",
    ),
    (
        (
            "M1",
            "DAT_ASCII_EURUSD_M1_201202.csv",
            M1_COLUMNS,
            EXPECTED_RAW_M1_RECORDS,
        ),
        (
            "T",
            "DAT_ASCII_EURUSD_T_201202.csv",
            TICK_COLUMNS,
            EXPECTED_RAW_TICK_RECORDS,
        ),
    ),
)
def test_polars_ingest_reads_csv_fixtures_with_stable_raw_schema(
    timeframe: str,
    filename: str,
    expected_columns: tuple[str, ...],
    expected_records: tuple[dict[str, object], ...],
) -> None:
    """Polars raw CSV ingest should preserve HistData layout and dtypes."""
    import polars as pl

    frame = read_ascii_file_to_polars(FIXTURES / filename, timeframe)

    assert frame.columns == list(expected_columns)
    assert frame.to_dicts() == list(expected_records)
    assert frame.schema["datetime"] == pl.String
    assert frame.schema["vol"] == pl.Int32
    assert all(
        frame.schema[column] == pl.Float64
        for column in expected_columns[1:-1]
    )


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_records"),
    (
        (
            "M1",
            "DAT_ASCII_EURUSD_M1_201202.csv",
            EXPECTED_RAW_M1_RECORDS,
        ),
        (
            "T",
            "DAT_ASCII_EURUSD_T_201202.csv",
            EXPECTED_RAW_TICK_RECORDS,
        ),
    ),
)
def test_polars_ingest_reads_zip_fixtures_like_csv_files(
    tmp_path: Path,
    timeframe: str,
    filename: str,
    expected_records: tuple[dict[str, object], ...],
) -> None:
    """Downloaded ZIP archives should produce equivalent Polars frames."""
    source = FIXTURES / filename
    archive_path = tmp_path / f"{filename}.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(source, arcname=filename)

    assert read_ascii_file_to_polars(
        archive_path,
        timeframe,
    ).to_dicts() == list(expected_records)


def test_polars_raw_schema_rejects_unsupported_timeframes() -> None:
    """Unsupported layouts should fail before Polars scans input data."""
    with pytest.raises(ValueError, match="unsupported ASCII timeframe"):
        raw_polars_schema_for_timeframe("T_LAST")


def test_polars_zip_ingest_requires_exactly_one_csv_member(
    tmp_path: Path,
) -> None:
    """Ambiguous downloaded archives should fail instead of guessing."""
    archive_path = tmp_path / "ambiguous.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("first.csv", "")
        archive.writestr("second.csv", "")

    with pytest.raises(ValueError, match="one CSV file"):
        read_ascii_file_to_polars(archive_path, "M1")


@pytest.mark.parametrize(
    ("timeframe", "raw_value", "expected_ms"),
    (
        ("M1", "20120201 000000", 1328072400000),
        ("M1", "20120201 000100", 1328072460000),
        ("T", "20120201 000003660", 1328072403660),
        ("T", "20120201 000014990", 1328072414990),
        ("M1", "20220313 023000", 1647156600000),
        ("M1", "20221106 013000", 1667716200000),
    ),
)
def test_timestamp_conversion_uses_fixed_est_no_dst_offset(
    timeframe: str, raw_value: str, expected_ms: int
) -> None:
    """DST boundary examples prove the current conversion is fixed EST, not local."""
    assert EST_NO_DST_OFFSET_MS == 18_000_000
    assert parse_histdata_datetime_to_utc_ms(raw_value, timeframe) == expected_ms


def test_summarize_rows_rejects_empty_inputs() -> None:
    """Current cache metadata requires start and end values."""
    with pytest.raises(ValueError, match="empty data file"):
        summarize_rows(())


@pytest.mark.parametrize(
    ("timeframe", "raw_row", "error"),
    (
        ("M1", ("20120201 000000", "1.0"), "6 fields"),
        ("T", ("20120201 000003660", "1.0", "1.1"), "4 fields"),
        ("T_LAST", ("20120201 000003660", "1.0", "0"), "unsupported"),
    ),
)
def test_invalid_or_unsupported_ascii_rows_fail_fast(
    timeframe: str, raw_row: tuple[str, ...], error: str
) -> None:
    """Unsupported layouts should not silently enter the migration pipeline."""
    with pytest.raises(ValueError, match=error):
        normalize_ascii_row(timeframe, raw_row)


def test_merge_batches_orders_by_cache_start_and_preserves_rows() -> None:
    """Current merge behavior sorts record batches by the cached start value."""
    first = read_ascii_file(FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv", "M1")
    second = read_ascii_file(
        FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv", "T"
    )

    assert merge_batches((second, first)) == (*first.rows, *second.rows)


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_rows"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv", EXPECTED_M1_ROWS),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv", EXPECTED_TICK_ROWS),
    ),
)
def test_records_api_return_adapter_preserves_schema_order_and_values(
    timeframe: str,
    filename: str,
    expected_rows: tuple[tuple[object, ...], ...],
) -> None:
    """The adapter seam exposes row records for dataframe-independent checks."""
    batch = read_ascii_file(FIXTURES / filename, timeframe)
    records = convert_batch_for_api(batch, "records")

    assert records == rows_as_records(batch)
    assert tuple(records[0]) == batch.columns
    assert tuple(records[0].values()) == expected_rows[0]
    assert tuple(records[-1].values()) == expected_rows[-1]


@pytest.mark.parametrize(
    ("timeframe", "filename"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv"),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv"),
    ),
)
def test_pandas_api_return_adapter_preserves_values_and_dtype_intent(
    timeframe: str, filename: str
) -> None:
    """Pandas API returns should preserve columns, rows, and integer dtypes."""
    batch = read_ascii_file(FIXTURES / filename, timeframe)
    frame = convert_batch_for_api(batch, "pandas")

    assert frame.columns.to_list() == list(batch.columns)
    assert frame.to_dict("records") == list(rows_as_records(batch))
    assert str(frame.dtypes["datetime"]) == "int64"
    assert str(frame.dtypes["vol"]) == "int32"
    assert all(
        str(frame.dtypes[column]) == "float64"
        for column in batch.columns[1:-1]
    )


@pytest.mark.parametrize(
    ("timeframe", "filename"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv"),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv"),
    ),
)
def test_polars_api_return_adapter_preserves_values_and_dtype_intent(
    timeframe: str, filename: str
) -> None:
    """Polars API returns should preserve columns, rows, and integer dtypes."""
    import polars as pl

    batch = read_ascii_file(FIXTURES / filename, timeframe)
    frame = convert_batch_for_api(batch, "polars")

    assert frame.columns == list(batch.columns)
    assert frame.to_dicts() == list(rows_as_records(batch))
    assert frame.schema["datetime"] == pl.Int64
    assert frame.schema["vol"] == pl.Int32
    assert all(frame.schema[column] == pl.Float64 for column in batch.columns[1:-1])


@pytest.mark.parametrize(
    ("timeframe", "filename"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv"),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv"),
    ),
)
def test_arrow_api_return_adapter_preserves_values_and_dtype_intent(
    timeframe: str, filename: str
) -> None:
    """Arrow API returns should preserve columns, rows, and integer dtypes."""
    import pyarrow as pa

    batch = read_ascii_file(FIXTURES / filename, timeframe)
    table = convert_batch_for_api(batch, "arrow")

    assert table.column_names == list(batch.columns)
    assert table.num_rows == len(batch.rows)
    assert table.to_pylist() == list(rows_as_records(batch))
    assert table.schema.field("datetime").type == pa.int64()
    assert table.schema.field("vol").type == pa.int32()
    assert all(
        table.schema.field(column).type == pa.float64()
        for column in batch.columns[1:-1]
    )


def test_api_return_adapter_rejects_unsupported_return_types() -> None:
    """The datatable-free seam should fail clearly for unported return types."""
    batch = read_ascii_file(FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv", "M1")

    with pytest.raises(ValueError, match="unsupported API return type: datatable"):
        convert_batch_for_api(batch, "datatable")


def test_influx_line_protocol_for_m1_matches_current_fields() -> None:
    """M1 rows become bid OHLC fields and keep millisecond timestamps."""
    line = format_influx_line("eurusd", "ascii", "M1", EXPECTED_M1_ROWS[1])

    assert line == (
        "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
        "openbid=1.30657,highbid=1.30657,lowbid=1.30647,closebid=1.30656 "
        "1328072460000"
    )


def test_influx_line_protocol_for_ticks_matches_current_fields() -> None:
    """Tick rows become bid/ask quote fields and keep millisecond timestamps."""
    line = format_influx_line("eurusd", "ascii", "T", EXPECTED_TICK_ROWS[1])

    assert line == (
        "eurusd,source=histdata.com,format=ascii,timeframe=T "
        "bidquote=1.30658,askquote=1.30675 1328072403973"
    )
