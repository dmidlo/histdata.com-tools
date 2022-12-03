"""Download (if needed), format, and import data to influxdb."""
# pylint: disable=redefined-outer-name
from __future__ import annotations

import sys
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Process, Queue
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional, Tuple

import rx
from influxdb_client import InfluxDBClient, WriteOptions, WritePrecision
from influxdb_client.client.write_api import WriteType
from reactivex.scheduler import ThreadPoolScheduler  # noqa:I900
from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rx import operators as ops

from histdatacom import config
from histdatacom.api import Api
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count

if TYPE_CHECKING:
    from histdatacom.records import Record, Records


class Influx:  # noqa:H601
    """Download (if needed), format, and import data to influxdb."""

    def import_data(self) -> None:
        """Initialize a pool of influxdb writers and...

        processes pool with a progress bar.
        """
        for _ in range(  # noqa:BLK100
            1, get_pool_cpu_count(config.ARGS["cpu_utilization"]) + 1
        ):
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
        """Import ASCII data to influxdb, both for csv and jay.

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
            if (
                record.status != "INFLUX_UPLOAD"
                and str.lower(record.data_format) == "ascii"
            ):
                jay_path = Path(record.data_dir, ".data")
                if jay_path.exists():
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        influx_chunks_queue,
                    )
                elif "CSV" in record.status:
                    Api.test_for_jay_or_create(record, args)
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        influx_chunks_queue,
                    )

            record.status = "INFLUX_UPLOAD"
            record.write_memento_file(base_dir=args["default_download_dir"])

            if args["delete_after_influx"]:
                Path(record.data_dir, record.zip_filename).unlink()
                Path(record.data_dir, record.jay_filename).unlink()
            records_next.put(record)
        except Exception as err:
            print(  # noqa:T201
                "Unexpected error from here:", sys.exc_info(), err
            )  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    def _import_jay(
        self,
        record: Record,
        args: dict,
        records_current: Records,  # noqa:W0613 # pylint: disable=W0613
        records_next: Records,  # noqa:W0613 # pylint: disable=W0613
        influx_chunks_queue: Queue,
    ) -> None:
        """Import a jay file with a ReactiveX pub/sub queue.

        Args:
            record (Record): a record from the work queue config.CURRENT_QUEUE
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            influx_chunks_queue (Queue): config.INFLUX_CHUNKS_QUEUE
        """
        jay = Api.import_jay_data(record.data_dir + record.jay_filename)

        with ProcessPoolExecutor(
            max_workers=1,
            initializer=self._init_counters,
            initargs=(influx_chunks_queue, config.ARGS),
        ) as executor:

            rx_data_queue = rx.from_iterable(jay.to_tuples()).pipe(
                ops.buffer_with_count(args["batch_size"]),
                ops.flat_map(
                    lambda rows: executor.submit(  # noqa:BLK100
                        self._parse_jay_rows, rows, record
                    )
                ),
            )

            rx_data_queue.subscribe(
                on_next=lambda x: None,
                on_error=lambda er: print(  # noqa:T201
                    f"Unexpected error: {er}"
                ),  # noqa:T201
            )

    def _parse_jay_rows(self, iterable: Iterable, record: Record) -> None:
        """Create a list by mapping row-by-row from datatable Frame (from jay).

        Args:
            iterable (Iterable): datatable.Frame
            record (Record): a record from the work queue.
        """
        map_func = partial(self._parse_jay_row, record=record)
        parsed_rows = list(map(map_func, iterable))

        INFLUX_CHUNKS_QUEUE.put(parsed_rows)  # type: ignore

    def _parse_jay_row(self, row: Tuple[Any], record: Record) -> str:
        """Return influxdb line-protocol line (str) for each from a map function.

            Applies different fields for line in line-protocol on Timeframe
                M1 or T.

        Args:
            row (Tuple[Any]): row from datatable.Frame
            record (Record): record from the work queue

        Returns:
            str: line-protocol (influxdb)
        """
        # pylint: disable=line-too-long
        measurement = f"{record.data_fxpair}"
        tags = (
            f"source=histdata.com,format={record.data_format}"
            f",timeframe={record.data_timeframe}"
        ).replace(" ", "")

        match record.data_timeframe:
            case "M1":
                _row = namedtuple(
                    "_row",
                    ["datetime", "open", "high", "low", "close", "vol"],
                )
                named_row = _row(
                    row[0],
                    row[1],  # type: ignore
                    row[2],  # type: ignore
                    row[3],  # type: ignore
                    row[4],  # type: ignore
                    row[5],  # type: ignore
                )

                fields = (
                    f"openbid={named_row.open},"
                    f"highbid={named_row.high},"
                    f"lowbid={named_row.low},"
                    f"closebid={named_row.close}"
                ).replace(" ", "")
                time = str(named_row.datetime)
            case "T":
                _row = namedtuple(  # type: ignore
                    "_row", ["datetime", "bid", "ask", "vol"]
                )
                named_row = _row(row[0], row[1], row[2], row[3])  # type: ignore

                fields = (
                    f"bidquote={named_row.bid},"  # type: ignore
                    f"askquote={named_row.ask}"
                ).replace(" ", "")
                time = str(named_row.datetime)

        # return in line-protocol format.
        return f"{measurement},{tags} {fields} {time}"


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
        self.client = InfluxDBClient(
            url=self.args["INFLUX_URL"],
            token=self.args["INFLUX_TOKEN"],
            org=self.args["INFLUX_ORG"],
            debug=False,
        )

        self.write_api = self.client.write_api(
            write_options=WriteOptions(
                write_type=WriteType.batching,
                batch_size=args["batch_size"],
                flush_interval=args["batch_size"],
            ),
            write_scheduler=ThreadPoolScheduler(
                max_workers=get_pool_cpu_count(config.ARGS["cpu_utilization"])
            ),
        )

    def run(self) -> None:
        """Process chunks from config.INFLUX_CHUNKS_QUEUE."""
        while True:
            try:
                chunk = self.influx_chunks_queue.get()  # type: ignore
            except EOFError:
                break

            if chunk is None:
                self._terminate()
                self.influx_chunks_queue.task_done()  # type: ignore
                break

            self.write_api.write(
                org=self.args["INFLUX_ORG"],
                bucket=self.args["INFLUX_BUCKET"],
                record=chunk,
                write_precision=WritePrecision.MS,
            )
            self.influx_chunks_queue.task_done()  # type: ignore

    def _terminate(self) -> None:
        """Terminate the influxdb subprocess."""
        del self.write_api  # noqa:WPS100
        del self.client  # noqa:WPS100
