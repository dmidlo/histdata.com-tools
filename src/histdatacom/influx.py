import os
import sys
import yaml

import rx
from rx import operators as ops

from concurrent.futures import ProcessPoolExecutor
from reactivex.scheduler import ThreadPoolScheduler
from multiprocessing import Process

from influxdb_client import InfluxDBClient
from influxdb_client import WriteOptions
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import WriteType

from functools import partial
from collections import namedtuple

from rich import print
from rich.progress import Progress
from rich.progress import TextColumn, TimeElapsedColumn, SpinnerColumn

from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.concurrency import ProcessPool
from histdatacom.api import _API
from histdatacom import config

class _Influx():
    def init_counters(self, influx_chunks_queue_, records_current_, records_next_, args_):
        global influx_chunks_queue
        influx_chunks_queue = influx_chunks_queue_
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def import_data(self):

        for cpu in range(1, get_pool_cpu_count(config.args['cpu_utilization']) + 1):
            writer = InfluxDBWriter(config.args, config.influx_chunks_queue)
            writer.start()
            


        pool = ProcessPool(self.import_file,
                           config.args,
                           "Adding", "CSVs to influx queue...",
                           get_pool_cpu_count(config.args['cpu_utilization']),
                           join=False,
                           dump=False)

        pool(config.current_queue, config.next_queue, config.influx_chunks_queue)

        with Progress(TextColumn(text_format="[cyan]...finishing upload to influxdb"),
                      SpinnerColumn(), SpinnerColumn(), SpinnerColumn(),
                      TimeElapsedColumn()) as progress:
            task_id = progress.add_task("waiting", total=0)

            config.current_queue.join()
            config.influx_chunks_queue.put(None)
            config.influx_chunks_queue.join()
            progress.advance(task_id, 0.75)

        print("[cyan] done.")
        config.next_queue.dump_to_queue(config.current_queue)

    def import_file(self, record, args, records_current, records_next, influx_chunks_queue):
        try:
            if record.status != "INFLUX_UPLOAD" and str.lower(record.data_format) == "ascii":
                jay_path = f"{record.data_dir}.data"
                if os.path.exists(jay_path):
                    self.import_jay(record, args, records_current, records_next, influx_chunks_queue)
                elif "CSV" in record.status:
                    _API.test_for_jay_or_create(record, args)
                    self.import_jay(record, args, records_current, records_next, influx_chunks_queue)

            record.status = "INFLUX_UPLOAD"
            record.write_info_file(base_dir=args['default_download_dir'])

            if args['delete_after_influx']:
                os.remove(f"{record.data_dir}{record.zip_filename}")
                os.remove(f"{record.data_dir}{record.jay_filename}")
            records_next.put(record)
        except Exception as e:
            print("Unexpected error from here:", sys.exc_info(e))
            record.delete_into_file()
            raise
        finally:
            records_current.task_done()

    def import_jay(self, record, args, records_current, records_next, influx_chunks_queue):

        jay = _API.import_jay_data(record.data_dir + record.jay_filename)

        with ProcessPoolExecutor(max_workers=1,
                                 initializer=self.init_counters,
                                 initargs=(influx_chunks_queue,
                                           records_current,
                                           records_next,
                                           config.args)) as executor:

            data = rx.from_iterable(jay.to_tuples()) \
                .pipe(ops.buffer_with_count(args['batch_size']),
                      ops.flat_map(
                        lambda rows: executor.submit(self.parse_jay_rows, rows, record)))

            data.subscribe(
                on_next=lambda x: None,
                on_error=lambda er: print(f"Unexpected error: {er}"))

    def parse_jay_rows(self, iterable, record):
        mapfunc = partial(self.parse_jay_row, record=record)
        _parsed_rows = list(map(mapfunc, iterable))

        influx_chunks_queue.put(_parsed_rows)

    def parse_jay_row(self, row, record):
        measurement = f"{record.data_fxpair}"
        tags = f"source=histdata.com,format={record.data_format},timeframe={record.data_timeframe}".replace(" ", "")

        match record.data_timeframe:
            case "M1":
                _row = namedtuple('_row', ['datetime', 'open', 'high', 'low', 'close', 'vol'])
                named_row = _row(row[0], row[1], row[2], row[3], row[4], row[5])

                fields = f"openbid={named_row.open},highbid={named_row.high},lowbid={named_row.low},closebid={named_row.close}".replace(" ", "")
                time = str(named_row.datetime)
            case "T":
                _row = namedtuple('_row', ['datetime','bid','ask','vol'])
                named_row = _row(row[0], row[1], row[2], row[3])

                fields = f"bidquote={named_row.bid},askquote={named_row.ask}".replace(" ", "")
                time = str(named_row.datetime)

        line_protocol = f"{measurement},{tags} {fields} {time}"

        return line_protocol

    @classmethod
    def load_influx_yaml(cls):

        if os.path.exists('influxdb.yaml'):
            with open('influxdb.yaml', 'r') as file:
                try:
                    yamlfile = yaml.safe_load(file)
                except yaml.YAMLError as exc:
                    print(exc)
                    sys.exit()

            return yamlfile

        print(""" ERROR: -I flag is used to import data to a influxdb instance...
                          there is no influxdb.yaml file in working directory.
                          did you forget to set it up?
              """)
        sys.exit()


class InfluxDBWriter(Process):
    def __init__(self, args, influx_chunks_queue):
        Process.__init__(self)
        self.args = args
        self.influx_chunks_queue = influx_chunks_queue
        self.client = InfluxDBClient(url=self.args['INFLUX_URL'],
                                     token=self.args['INFLUX_TOKEN'],
                                     org=self.args['INFLUX_ORG'],
                                     debug=False)

        self.write_api = self.client.write_api(write_options=WriteOptions(
                                               write_type=WriteType.batching,
                                               batch_size=args['batch_size'],
                                               flush_interval=args['batch_size']),
                                               write_scheduler=ThreadPoolScheduler(\
                                                  max_workers=get_pool_cpu_count(\
                                                    config.args['cpu_utilization'])))


    def run(self):

        while True:
            try:
                chunk = self.influx_chunks_queue.get()
            except EOFError:
                break

            if chunk is None:
                self.terminate()
                self.influx_chunks_queue.task_done()
                break

            self.write_api.write(org=self.args['INFLUX_ORG'],
                                 bucket=self.args['INFLUX_BUCKET'],
                                 record=chunk,
                                 write_precision=WritePrecision.MS)
            self.influx_chunks_queue.task_done()

    def terminate(self):
        self.write_api.__del__()
        self.client.__del__()
