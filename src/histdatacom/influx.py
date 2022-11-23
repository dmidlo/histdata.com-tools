# pylint: disable=redefined-outer-name
from typing import Any
from typing import Optional
from typing import Iterable
from typing import Tuple

import os
import sys
from functools import partial
from collections import namedtuple

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process
from multiprocessing import Queue
import rx
from rx import operators as ops
from reactivex.scheduler import ThreadPoolScheduler

from influxdb_client import InfluxDBClient
from influxdb_client import WriteOptions
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import WriteType

from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress
from rich.progress import TextColumn, TimeElapsedColumn, SpinnerColumn

from histdatacom import config
from histdatacom import Api

from histdatacom.records import Record
from histdatacom.records import Records

from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.concurrency import ProcessPool


class Influx:
    def init_counters(
        self,
        influx_chunks_queue_: Queue,
        records_current_: Records,
        records_next_: Records,
        args_: dict,
    ) -> None:
        global INFLUX_CHUNKS_QUEUE  # pylint: disable=global-variable-undefined
        INFLUX_CHUNKS_QUEUE = influx_chunks_queue_  # type: ignore
        global RECORDS_CURRENT  # pylint: disable=global-variable-undefined
        RECORDS_CURRENT = records_current_  # type: ignore
        global RECORDS_NEXT  # pylint: disable=global-variable-undefined
        RECORDS_NEXT = records_next_  # type: ignore
        global ARGS  # pylint: disable=global-variable-undefined
        ARGS = args_  # type: ignore

    def import_data(self) -> None:

        for _ in range(1, get_pool_cpu_count(config.ARGS["cpu_utilization"]) + 1):
            writer = InfluxDBWriter(config.ARGS, config.INFLUX_CHUNKS_QUEUE)
            writer.start()

        pool = ProcessPool(
            self.import_file,
            config.ARGS,
            "Adding",
            "CSVs to influx queue...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
            join=False,
            dump=False,
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE, config.INFLUX_CHUNKS_QUEUE)

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

        print("[cyan] done.")
        config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    def import_file(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
        influx_chunks_queue: Queue,
    ) -> None:
        try:
            if (
                record.status != "INFLUX_UPLOAD"
                and str.lower(record.data_format) == "ascii"
            ):
                jay_path = f"{record.data_dir}.data"
                if os.path.exists(jay_path):
                    self.import_jay(
                        record, args, records_current, records_next, influx_chunks_queue
                    )
                elif "CSV" in record.status:
                    Api.test_for_jay_or_create(record, args)
                    self.import_jay(
                        record, args, records_current, records_next, influx_chunks_queue
                    )

            record.status = "INFLUX_UPLOAD"
            record.write_info_file(base_dir=args["default_download_dir"])

            if args["delete_after_influx"]:
                os.remove(f"{record.data_dir}{record.zip_filename}")
                os.remove(f"{record.data_dir}{record.jay_filename}")
            records_next.put(record)
        except Exception as err:
            print("Unexpected error from here:", sys.exc_info(), err)
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def import_jay(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
        influx_chunks_queue: Queue,
    ) -> None:

        jay = Api.import_jay_data(record.data_dir + record.jay_filename)

        with ProcessPoolExecutor(
            max_workers=1,
            initializer=self.init_counters,
            initargs=(influx_chunks_queue, records_current, records_next, config.ARGS),
        ) as executor:

            data = rx.from_iterable(jay.to_tuples()).pipe(
                ops.buffer_with_count(args["batch_size"]),
                ops.flat_map(
                    lambda rows: executor.submit(self.parse_jay_rows, rows, record)
                ),
            )

            data.subscribe(
                on_next=lambda x: None,
                on_error=lambda er: print(f"Unexpected error: {er}"),
            )

    def parse_jay_rows(self, iterable: Iterable, record: Record) -> None:
        map_func = partial(self.parse_jay_row, record=record)
        _parsed_rows = list(map(map_func, iterable))

        INFLUX_CHUNKS_QUEUE.put(_parsed_rows)  # type: ignore

    def parse_jay_row(self, row: Tuple[Any], record: Record) -> str:
        # pylint: disable=line-too-long
        measurement = f"{record.data_fxpair}"
        tags = f"source=histdata.com,format={record.data_format},timeframe={record.data_timeframe}".replace(
            " ", ""
        )

        match record.data_timeframe:
            case "M1":
                _row = namedtuple(
                    "_row", ["datetime", "open", "high", "low", "close", "vol"]
                )
                named_row = _row(row[0], row[1], row[2], row[3], row[4], row[5])  # type: ignore

                fields = f"openbid={named_row.open},highbid={named_row.high},lowbid={named_row.low},closebid={named_row.close}".replace(
                    " ", ""
                )
                time = str(named_row.datetime)
            case "T":
                _row = namedtuple("_row", ["datetime", "bid", "ask", "vol"])  # type: ignore
                named_row = _row(row[0], row[1], row[2], row[3])  # type: ignore

                fields = f"bidquote={named_row.bid},askquote={named_row.ask}".replace(" ", "")  # type: ignore
                time = str(named_row.datetime)

        line_protocol = f"{measurement},{tags} {fields} {time}"

        return line_protocol


class InfluxDBWriter(Process):
    def __init__(self, args: dict, influx_chunks_queue: Optional[Queue]):
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

    def terminate(self) -> None:
        del self.write_api
        del self.client
