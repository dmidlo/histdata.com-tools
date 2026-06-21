"""Download (if needed), format, and import data to influxdb."""

# pylint: disable=redefined-outer-name
from __future__ import annotations

import sys
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
from histdatacom.histdata_ascii import format_influx_line
from histdatacom.runtime_contracts import WorkItem
from histdatacom.utils import check_installed_module, load_influx_yaml

if TYPE_CHECKING:
    from histdatacom.records import Record, Records

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

    def import_data(self) -> None:
        """Upload queued cache records through direct activity-style batches."""
        if config.CURRENT_QUEUE is None or config.NEXT_QUEUE is None:
            raise ValueError("Influx import requires configured record queues.")

        records_count = config.CURRENT_QUEUE.qsize()  # type: ignore
        with InfluxBatchWriter(config.ARGS) as writer:
            with Progress(
                TextColumn(text_format="[cyan]Uploading to InfluxDB"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
            ) as progress:
                task_id = progress.add_task("uploading", total=records_count)

                while not config.CURRENT_QUEUE.empty():  # type: ignore
                    record = config.CURRENT_QUEUE.get()  # type: ignore
                    if record is None:
                        break

                    self._import_file(
                        record,
                        config.ARGS,
                        config.CURRENT_QUEUE,  # type: ignore[arg-type]
                        config.NEXT_QUEUE,  # type: ignore[arg-type]
                        writer.write_lines,
                    )
                    progress.advance(task_id)

        print("[cyan] done.")  # noqa:T201
        config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    def _import_file(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
        emit_lines: LineSink | Any,
    ) -> None:
        """Import ASCII data to InfluxDB through an explicit line sink.

        Args:
            record (Record): a record from the work queue
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            emit_lines (LineSink | Any): line sink or legacy queue

        Raises:
            Exception: on unknown exception.
        """
        try:
            output = import_to_influx_work_item(
                WorkItem.from_record(record),
                args=args,
                emit_lines=_line_sink(emit_lines),
            )
            apply_stage_output_to_record(output, record)
            records_next.put(record)
        except Exception as err:
            print(  # noqa:T201
                "Unexpected error from here:", sys.exc_info(), err
            )  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    def _import_cache(
        self,
        record: Record,
        args: dict,
        records_current: Records,  # noqa:W0613 # pylint: disable=W0613
        records_next: Records,  # noqa:W0613 # pylint: disable=W0613
        emit_lines: LineSink | Any,
    ) -> None:
        """Import a cache file with bounded line-protocol batches.

        Args:
            record (Record): a record from the work queue config.CURRENT_QUEUE
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            emit_lines (LineSink | Any): line sink or legacy queue
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
            record (Record): a record from the work queue.
        """
        map_func = partial(self._parse_cache_row, record=record)
        return list(map(map_func, iterable))

    def _parse_cache_row(self, row: Tuple[Any, ...], record: Record) -> str:
        """Return influxdb line-protocol line (str) for each from a map function.

            Applies different fields for line in line-protocol on Timeframe
                M1 or T.

        Args:
            row (Tuple[Any]): row from cached dataframe
            record (Record): record from the work queue

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
        raise ValueError(
            "influxdb.yaml is missing required influxdb keys: "
            "org, bucket, url, token."
        ) from err

    return values


def _load_influx_client_api() -> tuple[Any, Any, Any]:
    check_installed_module("influxdb_client")
    from influxdb_client import InfluxDBClient, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    return InfluxDBClient, WritePrecision, SYNCHRONOUS
