"""Download (if needed), format, and import data to influxdb."""

# pylint: disable=redefined-outer-name
from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Iterable, Tuple

from rich import print  # pylint: disable=redefined-builtin
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    coerce_batch_size,
    emit_influx_cache_batches,
    import_to_influx_work_item,
    iter_polars_row_batches,
)
from histdatacom.exceptions import (
    InfluxConfigurationError,
    InfluxDependencyError,
)
from histdatacom.histdata_ascii import format_influx_line
from histdatacom.observability import ProgressState, progress_increment
from histdatacom.runtime_contracts import WorkItem, WorkStatus
from histdatacom.utils import check_installed_module, load_influx_yaml

if TYPE_CHECKING:
    from histdatacom.records import Record

LineSink = Callable[[list[str]], None]
INFLUX_CONFIG_FIELDS = (
    "INFLUX_ORG",
    "INFLUX_BUCKET",
    "INFLUX_URL",
    "INFLUX_TOKEN",
)


def _coerce_batch_size(batch_size: Any) -> int:
    """Return a positive integer batch size."""
    return int(coerce_batch_size(batch_size))


def _iter_polars_row_batches(
    frame: Any, batch_size: int
) -> Iterable[list[tuple[Any, ...]]]:
    """Yield bounded row batches from a Polars dataframe."""
    yield from iter_polars_row_batches(frame, batch_size)


class Influx:  # noqa:H601
    """Download (if needed), format, and import data to influxdb."""

    def import_data(self, records: Iterable[Record]) -> list[Record]:
        """Upload explicit cache records through direct activity-style batches."""
        records_to_import = list(records)
        records_count = len(records_to_import)
        progress_state = ProgressState(
            stage="import_to_influx",
            total=float(records_count),
            unit="records",
            status=WorkStatus.INFLUX_UPLOAD,
        )
        with InfluxBatchWriter(config.ARGS) as writer:
            with Progress(
                TextColumn(text_format="[cyan]Uploading to InfluxDB"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
            ) as progress:
                task_id = progress.add_task("uploading", total=records_count)

                imported = []
                for record in records_to_import:
                    imported_record = self._import_file(
                        record,
                        config.ARGS,
                        writer.write_lines,
                    )
                    imported.append(imported_record)
                    event = progress_state.advance(
                        message="Uploaded cache record to InfluxDB."
                    )
                    progress.advance(task_id, progress_increment(event))

        print("[cyan] done.")  # noqa:T201
        return imported

    def _import_file(
        self,
        record: Record,
        args: dict,
        emit_lines: LineSink | Any,
    ) -> Record:
        """Import ASCII data to InfluxDB through an explicit line sink.

        Args:
            record (Record): a record to import
            args (dict): config.ARGS
            emit_lines (LineSink | Any): line sink

        Raises:
        """
        output = import_to_influx_work_item(
            WorkItem.from_record(record),
            args=args,
            emit_lines=_line_sink(emit_lines),
        )
        apply_stage_output_to_record(output, record)
        return record

    def _import_cache(
        self,
        record: Record,
        args: dict,
        emit_lines: LineSink | Any,
    ) -> None:
        """Import a cache file with bounded line-protocol batches.

        Args:
            record (Record): a record to import
            args (dict): config.ARGS
            emit_lines (LineSink | Any): line sink
        """
        emit_influx_cache_batches(
            WorkItem.from_record(record),
            args=args,
            emit_lines=_line_sink(emit_lines),
        )

    def _parse_cache_rows(
        self, iterable: Iterable, record: Record
    ) -> list[str]:
        """Create line-protocol lines from cached dataframe rows.

        Args:
            iterable (Iterable): cached rows
            record (Record): a record being imported.
        """
        map_func = partial(self._parse_cache_row, record=record)
        return list(map(map_func, iterable))

    def _parse_cache_row(self, row: Tuple[Any, ...], record: Record) -> str:
        """Return influxdb line-protocol line (str) for each from a map function.

            Applies different fields for line in line-protocol on Timeframe
                M1 or T.

        Args:
            row (Tuple[Any]): row from cached dataframe
            record (Record): record being imported

        Returns:
            str: line-protocol (influxdb)
        """
        return str(
            format_influx_line(
                record.data_fxpair,
                record.data_format,
                record.data_timeframe,
                row,
            )
        )


class InfluxBatchWriter:
    """Write bounded line-protocol batches directly to InfluxDB."""

    def __init__(self, args: dict):
        """Initialize an InfluxDB client for direct batch writes."""
        self.args = _args_with_influx_config(args)
        client_class, write_precision, write_options = _load_influx_client_api()
        self.write_precision = write_precision.MS
        self.client = client_class(
            url=self.args["INFLUX_URL"],
            token=self.args["INFLUX_TOKEN"],
            org=self.args["INFLUX_ORG"],
            debug=False,
        )
        self.write_api = self.client.write_api(write_options=write_options)

    def __enter__(self) -> "InfluxBatchWriter":
        """Return this writer for context-managed imports."""
        return self

    def __exit__(self, *args: object) -> None:
        """Close the underlying client resources."""
        self.close()

    def write_lines(self, lines: list[str]) -> None:
        """Write one bounded line-protocol batch."""
        if not lines:
            return

        self.write_api.write(
            org=self.args["INFLUX_ORG"],
            bucket=self.args["INFLUX_BUCKET"],
            record=lines,
            write_precision=self.write_precision,
        )

    def close(self) -> None:
        """Close InfluxDB write API and client resources."""
        self.write_api.close()
        self.client.close()

    def terminate(self) -> None:
        """Compatibility alias for interrupt handlers."""
        self.close()


def _line_sink(emit_lines: LineSink | Any) -> LineSink:
    if callable(emit_lines):

        def call_sink(lines: list[str]) -> None:
            emit_lines(lines)

        return call_sink

    def put_sink(lines: list[str]) -> None:
        emit_lines.put(lines)

    return put_sink


def _args_with_influx_config(args: dict) -> dict:
    values = dict(args)
    missing = [key for key in INFLUX_CONFIG_FIELDS if not values.get(key)]
    if not missing:
        return values

    influx_yaml = load_influx_yaml()
    try:
        influx_config = influx_yaml["influxdb"]
        values["INFLUX_ORG"] = influx_config["org"]
        values["INFLUX_BUCKET"] = influx_config["bucket"]
        values["INFLUX_URL"] = influx_config["url"]
        values["INFLUX_TOKEN"] = influx_config["token"]
    except (KeyError, TypeError) as err:
        raise InfluxConfigurationError(
            "influxdb.yaml is missing required influxdb keys: "
            "org, bucket, url, token."
        ) from err

    return values


def _load_influx_client_api() -> tuple[Any, Any, Any]:
    try:
        check_installed_module("influxdb_client")
    except SystemExit as err:
        raise InfluxDependencyError(str(err)) from err
    from influxdb_client import InfluxDBClient, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    return InfluxDBClient, WritePrecision, SYNCHRONOUS
