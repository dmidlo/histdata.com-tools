"""Domain helpers for HistData ASCII market data.

This module is intentionally dataframe-independent. It captures the data
semantics that must survive the backend migration to Polars.
"""

from __future__ import annotations

import csv
import math
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

EST_NO_DST_OFFSET_MS = 18_000_000
# Real HistData tick archives can preserve one one-hour source-order fallback
# around the late-October clock transition.  The UTC conversion remains fixed
# EST; this bound governs ordering diagnostics, not timezone conversion.
MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS = 3_600_000
MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION = 1
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

TICK = "T"

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
    """Data-file summary currently stored in Record cache metadata fields."""

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
        case "T":
            return TICK_COLUMNS
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def delimiter_for_timeframe(timeframe: str) -> str:
    """Return HistData's delimiter for a supported ASCII timeframe."""
    match timeframe:
        case "T":
            return ","
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def parse_histdata_datetime_to_utc_ms(value: str, timeframe: str) -> int:
    """Convert HistData EST-no-DST datetime text to UTC epoch milliseconds."""
    value = value.strip()
    match timeframe:
        case "T":
            parsed = _parse_tick_datetime(value)
        case _:
            raise ValueError(f"unsupported ASCII timeframe: {timeframe}")

    delta = parsed - UNIX_EPOCH
    epoch_ms = (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )
    return epoch_ms + EST_NO_DST_OFFSET_MS


def _parse_tick_datetime(value: str) -> datetime:
    if len(value) != 18 or value[8] != " ":
        raise ValueError(f"time data {value!r} does not match T format")
    compact = value[:8] + value[9:]
    if not compact.isdigit():
        raise ValueError(f"time data {value!r} does not match T format")
    return datetime(
        int(value[:4]),
        int(value[4:6]),
        int(value[6:8]),
        int(value[9:11]),
        int(value[11:13]),
        int(value[13:15]),
        int(value[15:18]) * 1_000,
        tzinfo=timezone.utc,
    )


def normalize_ascii_row(
    timeframe: str, row: Sequence[str]
) -> tuple[int, float, float, int]:
    """Normalize a raw HistData ASCII row into typed values."""
    values = tuple(cell.strip() for cell in row)
    match timeframe:
        case "T":
            if len(values) != 4:
                raise ValueError(
                    f"T rows must have 4 fields, got {len(values)}"
                )
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
    if filename_has_unsupported_raw_dimensions(path):
        raise ValueError("raw import supports ASCII tick inputs only")
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = tuple(
                name for name in archive.namelist() if not name.endswith("/")
            )
            if len(names) != 1:
                raise ValueError("expected ZIP archive to contain one CSV file")
            if filename_has_unsupported_raw_dimensions(names[0]):
                raise ValueError("raw import supports ASCII tick inputs only")
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
    return tuple(
        dict(zip(batch.columns, row, strict=True)) for row in batch.rows
    )


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
        column: (
            pl.Utf8 if column == "datetime" else _polars_type_for_column(column)
        )
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
    if timeframe != TICK:
        raise ValueError(f"unsupported ASCII timeframe: {timeframe}")
    parsed = pl.datetime(
        *args,
        raw.str.slice(15, 3).cast(pl.Int32) * 1_000,
        time_unit="ms",
    )

    return (
        (parsed.cast(pl.Int64) + EST_NO_DST_OFFSET_MS)
        .cast(pl.Int64)
        .alias(column)
    )


def convert_polars_datetime_to_utc_ms(
    frame: Any, timeframe: str, column: str = "datetime"
) -> Any:
    """Convert a raw Polars HistData datetime column to UTC epoch millis."""
    return frame.with_columns(polars_datetime_to_utc_ms_expr(timeframe, column))


def _read_csv_to_polars(source: Any, timeframe: str) -> Any:
    """Read a HistData ASCII CSV source into a raw Polars dataframe."""
    import polars as pl

    columns = list(columns_for_timeframe(timeframe))
    frame = pl.read_csv(
        source,
        has_header=False,
        separator=delimiter_for_timeframe(timeframe),
        new_columns=columns,
        schema_overrides={column: pl.Utf8 for column in columns},
    )
    return frame.with_columns(
        [_raw_polars_cast_expr(column) for column in columns]
    )


def _raw_polars_cast_expr(column: str) -> Any:
    """Return a trimmed raw-ingest expression for one Polars column."""
    import polars as pl

    stripped = pl.col(column).str.strip_chars()
    match column:
        case "datetime":
            return stripped.alias(column)
        case "vol":
            return stripped.cast(pl.Int32).alias(column)
        case _:
            return stripped.cast(pl.Float64).alias(column)


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
        if filename_has_unsupported_raw_dimensions(names[0]):
            raise ValueError("raw import supports ASCII tick inputs only")
        return archive.read(names[0])


def filename_has_unsupported_raw_dimensions(filename: str | Path) -> bool:
    """Return whether a HistData filename declares a retired raw axis."""
    name = Path(str(filename)).name.upper()
    stem = name
    # Live HistData ASCII archives include a same-stem ``.txt`` status report
    # beside the CSV data member.  It declares the same supported raw axes and
    # must not be mistaken for a retired platform or timeframe.
    for suffix in (".ZIP", ".CSV", ".TXT"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    parts = stem.split("_")
    if stem.startswith("DAT_"):
        if len(parts) < 5:
            return True
        period = parts[4]
        return (
            parts[1] != "ASCII"
            or parts[3] != TICK
            or len(period) not in {4, 6}
            or not period.isdigit()
        )
    if stem.startswith("HISTDATA_COM_"):
        if len(parts) != 5:
            return True
        timeframe_period = parts[4]
        period = timeframe_period[1:]
        is_tick = (
            len(period) in {4, 6}
            and timeframe_period.startswith(TICK)
            and period.isdigit()
        )
        return parts[2] != "ASCII" or not is_tick
    return False


def read_ascii_file_to_polars(path: Path, timeframe: str) -> Any:
    """Read a plain CSV file or ZIP archive into a raw Polars dataframe."""
    if filename_has_unsupported_raw_dimensions(path):
        raise ValueError("raw import supports ASCII tick inputs only")
    if path.suffix.lower() == ".zip":
        return _read_csv_to_polars(
            BytesIO(_single_csv_member_from_zip(path)),
            timeframe,
        )

    return _read_csv_to_polars(path, timeframe)


def write_polars_cache(frame: Any, path: Path) -> None:
    """Write a Polars dataframe cache using Arrow IPC payloads."""
    _validate_cache_dimensions(frame)
    frame.write_ipc(path)


def read_polars_cache(path: Path) -> Any:
    """Read a Polars Arrow IPC cache, or fail with migration guidance."""
    import polars as pl

    try:
        frame = pl.read_ipc(path)
    except Exception as err:
        raise ValueError(LEGACY_CACHE_ERROR) from err
    _validate_cache_dimensions(frame)
    return frame


def _validate_cache_dimensions(frame: Any) -> None:
    """Reject enriched caches that declare retired raw dimensions."""
    columns = set(getattr(frame, "columns", ()))
    if "format" in columns:
        formats = {
            str(value).lower()
            for value in frame.get_column("format").drop_nulls().unique()
        }
        if formats != {"ascii"}:
            raise ValueError("cache supports ASCII tick inputs only")
    if "timeframe" in columns:
        timeframes = {
            str(value)
            for value in frame.get_column("timeframe").drop_nulls().unique()
        }
        if timeframes != {TICK}:
            raise ValueError("cache supports ASCII tick inputs only")


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
        column: _polars_type_for_column(column) for column in batch.columns
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


def merge_batches(
    batches: Iterable[ParsedAsciiBatch],
) -> tuple[tuple[Any, ...], ...]:
    """Merge batches in current record-start order."""
    ordered = sorted(batches, key=lambda batch: str(batch.summary.start))
    return tuple(row for batch in ordered for row in batch.rows)


def format_influx_line(
    pair: str,
    data_format: str,
    timeframe: str,
    row: Sequence[Any],
    *,
    columns: Sequence[str] | None = None,
) -> str:
    """Return line protocol for a raw or enriched ASCII tick cache row."""
    _validate_influx_dimensions(data_format, timeframe)

    if columns is None:
        tags = (
            f"source=histdata.com,format={data_format},timeframe={timeframe}"
        ).replace(" ", "")
        fields = f"bidquote={row[1]},askquote={row[2]}".replace(" ", "")
        return f"{pair},{tags} {fields} {row[0]}"

    values = _row_values(row, columns)
    _validate_influx_dimensions(
        str(values.get("format") or data_format),
        str(values.get("timeframe") or timeframe),
    )
    tags = _influx_tags(values, data_format, timeframe)
    fields = _influx_fields(values)
    timestamp = values.get("datetime", row[0])
    return f"{_escape_influx_key(pair)},{tags} {fields} {timestamp}"


def _validate_influx_dimensions(data_format: str, timeframe: str) -> None:
    if str(data_format).lower() != "ascii":
        raise ValueError("Influx projection supports ASCII tick inputs only")
    if timeframe != TICK:
        raise ValueError(f"unsupported ASCII timeframe: {timeframe}")


def _row_values(
    row: Sequence[Any],
    columns: Sequence[str],
) -> dict[str, Any]:
    return dict(zip(columns, row, strict=False))


def _influx_tags(
    values: Mapping[str, Any],
    data_format: str,
    timeframe: str,
) -> str:
    source = str(values.get("source") or "histdata.com")
    tags = {
        "source": source,
        "format": str(values.get("format") or data_format),
        "timeframe": str(values.get("timeframe") or timeframe),
    }
    period = values.get("period")
    if period not in (None, ""):
        tags["period"] = str(period)
    row_id = values.get("row_id")
    if row_id not in (None, ""):
        tags["row_id"] = str(row_id)
    return ",".join(
        f"{_escape_influx_key(key)}={_escape_influx_key(value)}"
        for key, value in tags.items()
    )


def _influx_fields(values: Mapping[str, Any]) -> str:
    fields: list[str] = []
    _append_influx_field(fields, "bidquote", values.get("bid"))
    _append_influx_field(fields, "askquote", values.get("ask"))
    excluded = {
        "datetime",
        "bid",
        "ask",
        "vol",
        "training_schema_version",
        "series_id",
        "row_id",
        "symbol",
        "format",
        "timeframe",
        "source",
        "period",
    }
    for name, value in values.items():
        if name in excluded:
            continue
        _append_influx_field(fields, name, value)
    return ",".join(fields)


def _append_influx_field(
    fields: list[str],
    name: str,
    value: Any,
) -> None:
    formatted = _format_influx_field_value(value)
    if formatted is None:
        return
    fields.append(f"{_escape_influx_key(name)}={formatted}")


def _format_influx_field_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return str(value)
    return _format_influx_string_field(str(value))


def _format_influx_string_field(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _escape_influx_key(value: Any) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )
