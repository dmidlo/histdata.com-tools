"""Download (if needed), format, and import data to influxdb."""

# pylint: disable=redefined-outer-name
from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Process, Queue
from typing import TYPE_CHECKING, Any, Iterable, Optional, Tuple

from influxdb_client import InfluxDBClient, WriteOptions, WritePrecision
from influxdb_client.client.write_api import WriteType
from reactivex.scheduler import ThreadPoolScheduler  # noqa:I900
from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    coerce_batch_size,
    import_to_influx_work_item,
    iter_polars_row_batches,
)
from histdatacom.api import Api
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count
from histdatacom.histdata_ascii import format_influx_line
from histdatacom.runtime_contracts import WorkItem

if TYPE_CHECKING:
    from histdatacom.records import Record, Records


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
        """Initialize a pool of influxdb writers and...

        processes pool with a progress bar.
        """
        writer = InfluxDBWriter(config.ARGS, config.INFLUX_CHUNKS_QUEUE)
        writer.start()

        pool = ProcessPool(
            self._import_file,
            config.ARGS,
            "Adding",
            "CSVs to influx queue...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
            join=False,
            dump=False,
        )

        pool(
            config.CURRENT_QUEUE,
            config.NEXT_QUEUE,
            config.INFLUX_CHUNKS_QUEUE,
        )

        with Progress(
            TextColumn(text_format="[cyan]...finishing upload to influxdb"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("waiting", total=0)

            config.CURRENT_QUEUE.join()  # type: ignore
            config.INFLUX_CHUNKS_QUEUE.put(None)  # type: ignore
            config.INFLUX_CHUNKS_QUEUE.join()  # type: ignore
            progress.advance(task_id, 0.75)

        print("[cyan] done.")  # noqa:T201
        config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    def _init_counters(
        self,
        influx_chunks_queue_: Queue,
        args_: dict,
    ) -> None:
        """Initialize pool with access to these global variables.

        Args:
            influx_chunks_queue_ (Queue): ReactiveX queue
                * serialized through SyncManager
            args_ (dict): config.ARGS
        """
        # pylint: disable=global-variable-undefined
        global INFLUX_CHUNKS_QUEUE  # noqa:WPS100
        INFLUX_CHUNKS_QUEUE = influx_chunks_queue_  # type: ignore
        global ARGS  # noqa:WPS100
        ARGS = args_  # type: ignore

    def _import_file(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
        influx_chunks_queue: Queue,
    ) -> None:
        """Import ASCII data to influxdb, both for csv and cache.

        Args:
            record (Record): a record from the work queue
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            influx_chunks_queue (Queue): config.INFLUX_CHUNKS_QUEUE

        Raises:
            Exception: on unknown exception.
        """
        try:
            output = import_to_influx_work_item(
                WorkItem.from_record(record),
                args=args,
                emit_lines=influx_chunks_queue.put,
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
        influx_chunks_queue: Queue,
    ) -> None:
        """Import a cache file with a ReactiveX pub/sub queue.

        Args:
            record (Record): a record from the work queue config.CURRENT_QUEUE
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            influx_chunks_queue (Queue): config.INFLUX_CHUNKS_QUEUE
        """
        cache = Api.import_cache_data(record.data_dir + record.cache_filename)
        batch_size = _coerce_batch_size(args["batch_size"])

        with ProcessPoolExecutor(
            max_workers=1,
            initializer=self._init_counters,
            initargs=(influx_chunks_queue, args),
        ) as executor:
            for rows in _iter_polars_row_batches(cache, batch_size):
                executor.submit(self._parse_cache_rows, rows, record).result()

    def _parse_cache_rows(self, iterable: Iterable, record: Record) -> None:
        """Create a list by mapping row-by-row from a cached dataframe.

        Args:
            iterable (Iterable): cached rows
            record (Record): a record from the work queue.
        """
        map_func = partial(self._parse_cache_row, record=record)
        parsed_rows = list(map(map_func, iterable))

        INFLUX_CHUNKS_QUEUE.put(parsed_rows)  # type: ignore

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


class InfluxDBWriter(Process):
    """Write data from the chunks queue as line-protocol to influxdb write api.

    Args:
        Process (Process): A Python Process.
    """

    def __init__(self, args: dict, influx_chunks_queue: Optional[Queue]):
        """Initialize a process for the influxdbClient write api.

        Args:
            args (dict): config.ARGS
            influx_chunks_queue (Optional[Queue]): config.INFLUX_CHUNKS_QUEUE
        """
        Process.__init__(self)
        self.args = args
        self.influx_chunks_queue = influx_chunks_queue
        batch_size = _coerce_batch_size(args["batch_size"])
        self.client = InfluxDBClient(
            url=self.args["INFLUX_URL"],
            token=self.args["INFLUX_TOKEN"],
            org=self.args["INFLUX_ORG"],
            debug=False,
        )

        self.write_api = self.client.write_api(
            write_options=WriteOptions(
                write_type=WriteType.batching,
                batch_size=batch_size,
                flush_interval=batch_size,
            ),
            write_scheduler=ThreadPoolScheduler(
                max_workers=get_pool_cpu_count(config.ARGS["cpu_utilization"])
            ),
        )

    def run(self) -> None:
        """Process chunks from config.INFLUX_CHUNKS_QUEUE."""
        try:
            while True:
                try:
                    chunk = self.influx_chunks_queue.get()  # type: ignore
                except EOFError:
                    break

                if chunk is None:
                    self.terminate()
                    self.influx_chunks_queue.task_done()  # type: ignore
                    break

                self.write_api.write(
                    org=self.args["INFLUX_ORG"],
                    bucket=self.args["INFLUX_BUCKET"],
                    record=chunk,
                    write_precision=WritePrecision.MS,
                )
                self.influx_chunks_queue.task_done()  # type: ignore
        except KeyboardInterrupt:
            self.terminate()

    def terminate(self) -> None:
        """Terminate the influxdb subprocess."""
        self.write_api.close()
        self.client.close()
        self.close()
