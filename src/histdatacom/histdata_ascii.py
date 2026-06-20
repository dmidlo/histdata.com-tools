"""Domain helpers for HistData ASCII market data.

This module is intentionally dataframe-independent. It captures the data
semantics that must survive the backend migration to Polars.
"""
from __future__ import annotations

import csv
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Sequence


EST_NO_DST_OFFSET_MS = 18_000_000
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

M1 = "M1"
TICK = "T"

M1_COLUMNS = ("datetime", "open", "high", "low", "close", "vol")
TICK_COLUMNS = ("datetime", "bid", "ask", "vol")
CACHE_FILENAME = ".data"
CACHE_FORMAT = "Polars Arrow IPC"
LEGACY_CACHE_ERROR = (
    f"cannot read cache file as {CACHE_FORMAT}. Existing legacy caches must "
    f"be regenerated: delete the {CACHE_FILENAME} file and "
    "rerun validation or import so histdatacom can rebuild it."
)


@dataclass(frozen=True)
class CacheSummary:
    """Data-file summary currently stored in Record jay metadata fields."""

    line_count: int
    start: int
    end: int


@dataclass(frozen=True)
class ParsedAsciiBatch:
    """Parsed HistData ASCII rows plus schema and cache summary."""

    timeframe: str
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    summary: CacheSummary


def _column_values(
    rows: Sequence[Sequence[Any]], index: int
) -> tuple[Any, ...]:
    """Return one column's values from row-oriented parsed data."""
    return tuple(row[index] for row in rows)


def columns_for_timeframe(timeframe: str) -> tuple[str, ...]:
    """Return the canonical ASCII columns for a supported timeframe."""
    match timeframe:
        case "M1":
            return M1_COLUMNS
        case "T":
            return TICK_COLUMNS
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def delimiter_for_timeframe(timeframe: str) -> str:
    """Return HistData's delimiter for a supported ASCII timeframe."""
    match timeframe:
        case "M1":
            return ";"
        case "T":
            return ","
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def parse_histdata_datetime_to_utc_ms(value: str, timeframe: str) -> int:
    """Convert HistData EST-no-DST datetime text to UTC epoch milliseconds."""
    value = value.strip()
    match timeframe:
        case "M1":
            parsed = datetime.strptime(value, "%Y%m%d %H%M%S")
        case "T":
            parsed = datetime.strptime(value, "%Y%m%d %H%M%S%f")
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")

    delta = parsed.replace(tzinfo=timezone.utc) - UNIX_EPOCH
    epoch_ms = (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )
    return epoch_ms + EST_NO_DST_OFFSET_MS


def normalize_ascii_row(
    timeframe: str, row: Sequence[str]
) -> tuple[int, float, float, float, float, int] | tuple[int, float, float, int]:
    """Normalize a raw HistData ASCII row into typed values."""
    values = tuple(cell.strip() for cell in row)
    match timeframe:
        case "M1":
            if len(values) != 6:
                raise ValueError(f"M1 rows must have 6 fields, got {len(values)}")
            return (
                parse_histdata_datetime_to_utc_ms(values[0], timeframe),
                float(values[1]),
                float(values[2]),
                float(values[3]),
                float(values[4]),
                int(values[5]),
            )
        case "T":
            if len(values) != 4:
                raise ValueError(f"T rows must have 4 fields, got {len(values)}")
            return (
                parse_histdata_datetime_to_utc_ms(values[0], timeframe),
                float(values[1]),
                float(values[2]),
                int(values[3]),
            )
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def parse_ascii_lines(timeframe: str, lines: Iterable[str]) -> ParsedAsciiBatch:
    """Parse HistData ASCII rows from text lines."""
    reader = csv.reader(lines, delimiter=delimiter_for_timeframe(timeframe))
    rows = tuple(
        normalize_ascii_row(timeframe, row)
        for row in reader
        if row and any(cell.strip() for cell in row)
    )
    return ParsedAsciiBatch(
        timeframe=timeframe,
        columns=columns_for_timeframe(timeframe),
        rows=rows,
        summary=summarize_rows(rows),
    )


def read_ascii_file(path: Path, timeframe: str) -> ParsedAsciiBatch:
    """Parse a plain CSV file or a ZIP containing one HistData CSV file."""
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = tuple(name for name in archive.namelist() if not name.endswith("/"))
            if len(names) != 1:
                raise ValueError("expected ZIP archive to contain one CSV file")
            with archive.open(names[0]) as source:
                text = source.read().decode("utf-8").splitlines()
        return parse_ascii_lines(timeframe, text)

    with path.open("r", encoding="utf-8") as source:
        return parse_ascii_lines(timeframe, source)


def summarize_rows(rows: Sequence[Sequence[Any]]) -> CacheSummary:
    """Return line count and first/last datetime values for parsed rows."""
    if not rows:
        raise ValueError("cannot summarize an empty data file")
    return CacheSummary(
        line_count=len(rows),
        start=int(rows[0][0]),
        end=int(rows[-1][0]),
    )


def rows_as_records(batch: ParsedAsciiBatch) -> tuple[dict[str, Any], ...]:
    """Return row dictionaries with the same field names as API dataframes."""
    return tuple(dict(zip(batch.columns, row, strict=True)) for row in batch.rows)


def _arrow_type_for_column(column: str) -> Any:
    """Return the Arrow type that preserves current dataframe dtype intent."""
    import pyarrow as pa

    match column:
        case "datetime":
            return pa.int64()
        case "vol":
            return pa.int32()
        case _:
            return pa.float64()


def _polars_type_for_column(column: str) -> Any:
    """Return the Polars type that preserves current dataframe dtype intent."""
    import polars as pl

    match column:
        case "datetime":
            return pl.Int64
        case "vol":
            return pl.Int32
        case _:
            return pl.Float64


def raw_polars_schema_for_timeframe(timeframe: str) -> dict[str, Any]:
    """Return the raw ingest Polars schema for a supported timeframe."""
    import polars as pl

    return {
        column: pl.Utf8 if column == "datetime" else _polars_type_for_column(column)
        for column in columns_for_timeframe(timeframe)
    }


def polars_datetime_to_utc_ms_expr(
    timeframe: str, column: str = "datetime"
) -> Any:
    """Return a Polars expression for HistData UTC millisecond timestamps."""
    import polars as pl

    raw = pl.col(column)
    args = [
        raw.str.slice(0, 4).cast(pl.Int32),
        raw.str.slice(4, 2).cast(pl.Int32),
        raw.str.slice(6, 2).cast(pl.Int32),
        raw.str.slice(9, 2).cast(pl.Int32),
        raw.str.slice(11, 2).cast(pl.Int32),
        raw.str.slice(13, 2).cast(pl.Int32),
    ]
    match timeframe:
        case "M1":
            parsed = pl.datetime(*args, time_unit="ms")
        case "T":
            parsed = pl.datetime(
                *args,
                raw.str.slice(15, 3).cast(pl.Int32) * 1_000,
                time_unit="ms",
            )
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")

    return (
        parsed.dt.epoch("ms")
        + EST_NO_DST_OFFSET_MS
    ).cast(pl.Int64).alias(column)


def convert_polars_datetime_to_utc_ms(
    frame: Any, timeframe: str, column: str = "datetime"
) -> Any:
    """Convert a raw Polars HistData datetime column to UTC epoch millis."""
    return frame.with_columns(
        polars_datetime_to_utc_ms_expr(timeframe, column)
    )


def _read_csv_to_polars(source: Any, timeframe: str) -> Any:
    """Read a HistData ASCII CSV source into a raw Polars dataframe."""
    import polars as pl

    return pl.read_csv(
        source,
        has_header=False,
        separator=delimiter_for_timeframe(timeframe),
        new_columns=list(columns_for_timeframe(timeframe)),
        schema_overrides=raw_polars_schema_for_timeframe(timeframe),
    )


def _single_csv_member_from_zip(path: Path) -> bytes:
    """Return the single CSV member payload from a HistData ZIP archive."""
    with zipfile.ZipFile(path) as archive:
        names = tuple(
            name
            for name in archive.namelist()
            if not name.endswith("/") and Path(name).suffix.lower() == ".csv"
        )
        if len(names) != 1:
            raise ValueError("expected ZIP archive to contain one CSV file")
        return archive.read(names[0])


def read_ascii_file_to_polars(path: Path, timeframe: str) -> Any:
    """Read a plain CSV file or ZIP archive into a raw Polars dataframe."""
    if path.suffix.lower() == ".zip":
        return _read_csv_to_polars(
            BytesIO(_single_csv_member_from_zip(path)),
            timeframe,
        )

    return _read_csv_to_polars(path, timeframe)


def write_polars_cache(frame: Any, path: Path) -> None:
    """Write a Polars dataframe cache using Arrow IPC payloads."""
    frame.write_ipc(path)


def read_polars_cache(path: Path) -> Any:
    """Read a Polars Arrow IPC cache, or fail with migration guidance."""
    import polars as pl

    try:
        return pl.read_ipc(path)
    except Exception as err:
        raise ValueError(LEGACY_CACHE_ERROR) from err


def to_arrow_table(batch: ParsedAsciiBatch) -> Any:
    """Convert parsed rows to the Arrow table shape returned by the API."""
    import pyarrow as pa

    arrays = [
        pa.array(
            _column_values(batch.rows, index),
            type=_arrow_type_for_column(column),
        )
        for index, column in enumerate(batch.columns)
    ]
    return pa.Table.from_arrays(arrays, names=list(batch.columns))


def to_pandas_frame(batch: ParsedAsciiBatch) -> Any:
    """Convert parsed rows to the pandas dataframe shape returned by the API."""
    import pandas as pd

    data = {
        column: _column_values(batch.rows, index)
        for index, column in enumerate(batch.columns)
    }
    frame = pd.DataFrame(data, columns=batch.columns)
    return frame.astype({"datetime": "int64", "vol": "int32"})


def to_polars_frame(batch: ParsedAsciiBatch) -> Any:
    """Convert parsed rows to the Polars dataframe shape returned by the API."""
    import polars as pl

    data = {
        column: _column_values(batch.rows, index)
        for index, column in enumerate(batch.columns)
    }
    schema = {
        column: _polars_type_for_column(column)
        for column in batch.columns
    }
    return pl.DataFrame(data, schema=schema)


def convert_batch_for_api(batch: ParsedAsciiBatch, return_type: str) -> Any:
    """Convert parsed rows to a supported API return type."""
    match return_type:
        case "records":
            return rows_as_records(batch)
        case "arrow":
            return to_arrow_table(batch)
        case "pandas":
            return to_pandas_frame(batch)
        case "polars":
            return to_polars_frame(batch)
        case _:
            raise ValueError(f"unsupported API return type: {return_type}")


def merge_batches(batches: Iterable[ParsedAsciiBatch]) -> tuple[tuple[Any, ...], ...]:
    """Merge batches in current record-start order."""
    ordered = sorted(batches, key=lambda batch: str(batch.summary.start))
    return tuple(row for batch in ordered for row in batch.rows)


def format_influx_line(
    pair: str, data_format: str, timeframe: str, row: Sequence[Any]
) -> str:
    """Return the line protocol string currently emitted for a parsed row."""
    tags = (
        f"source=histdata.com,format={data_format},timeframe={timeframe}"
    ).replace(" ", "")

    match timeframe:
        case "M1":
            fields = (
                f"openbid={row[1]},"
                f"highbid={row[2]},"
                f"lowbid={row[3]},"
                f"closebid={row[4]}"
            ).replace(" ", "")
        case "T":
            fields = f"bidquote={row[1]},askquote={row[2]}".replace(" ", "")
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")

    return f"{pair},{tags} {fields} {row[0]}"
