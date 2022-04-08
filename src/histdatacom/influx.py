import multiprocessing, io
import rx
from rx import operators as ops
from concurrent.futures import ProcessPoolExecutor, as_completed
from influxdb_client import Point, InfluxDBClient, WriteOptions, WritePrecision
from influxdb_client.client.write_api import WriteType
from urllib.request import urlopen
from rich.progress import Progress
from csv import DictReader
from histdatacom.fx_enums import TimePrecision
import os, sys

class _Influx():
    def __init__(self, args, records_current_, records_next_, csv_chunks_queue_, csv_counter_, csv_progress_):
        self.args = args

        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

        global csv_chunks_queue
        csv_chunks_queue = csv_chunks_queue_

        global csv_counter
        csv_counter = csv_counter_

        global csv_progress
        csv_progress = csv_progress_

    def init_counters(self, csv_chunks_queue_, csv_counter_, csv_progress_, records_current_, records_next_, args_):
        global csv_chunks_queue
        csv_chunks_queue = csv_chunks_queue_
        global csv_counter
        csv_counter = csv_counter_
        global csv_progress
        csv_progress = csv_progress_
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def parse_row(self, row):

        ### This results in a "signed integer is greater than maximum" error
        # return Point("forex") \
        #     .tag("source", row["Source"]) \
        #     .tag("platform", row["Platform"]) \
        #     .tag("timeframe", row["Timeframe"]) \
        #     .tag("instrument", row["Instrument"]) \
        #     .field("bidquote", row["bidQuote"]) \
        #     .field("askquote", row["askQuote"]) \
        #     .time(row["msSinceEpochUTC"], write_precision=TimePrecision.ASCII_T.value)

        ### Manually generating line-protocol works and does not generate an error.
        ### This schema is not quite what I want
        # return "forex,source=" + row["Source"] + \
        #         ",platform=" + row["Platform"] + \
        #         ",timeframe=" + row["Timeframe"] + \
        #         ",instrument=" + row["Instrument"] + " " + \
        #         "bidquote=" + str(row["bidQuote"]) + \
        #         ",askquote=" + str(row["askQuote"]) + \
        #         ",volume=" + str(row["Volume"]) + " " + \
        #         str(row["msSinceEpochUTC"])

        return row["Instrument"] + \
            ",source=" + row["Source"].replace(" ", "") + \
            ",platform=" + row["Platform"].replace(" ", "") + \
            ",timeframe=" + row["Timeframe"].replace(" ", "") + " " + \
            "bidquote=" + str(row["bidQuote"]).replace(" ", "") + \
            ",askquote=" + str(row["askQuote"]).replace(" ", "") + \
            ",volume=" + str(row["Volume"]).replace(" ", "") + " " + \
            str(row["msSinceEpochUTC"]).replace(" ", "")    

    def parse_rows(self, rows, record, total_size):
        _parsed_rows = list(map(self.parse_row, rows))

        csv_counter.value += len(_parsed_rows)

        csv_chunks_queue.put(_parsed_rows)

    def importCSV(self, record):
        if "CLEAN" in record.status:

            status_elements = record.status.split("_")
            csv_path = record.data_dir + record.csv_filename
            file_endpoint = "file://" + record.data_dir + record.csv_filename

            try:
                res = urlopen(file_endpoint)

                if res.headers:
                    content_length = res.headers['content-length']

                io_wrapper = _ProgressTextIOWrapper(res)
                io_wrapper.progress = csv_progress

                with ProcessPoolExecutor(max_workers=(multiprocessing.cpu_count() - 2),
                                                        initializer=self.init_counters, 
                                                        initargs=(csv_chunks_queue,
                                                                    csv_counter,
                                                                    csv_progress,
                                                                    records_current,
                                                                    records_next,
                                                                    self.args.copy())) as executor:
                        data = rx.from_iterable(DictReader(io_wrapper)
                                        ).pipe(
                                            ops.buffer_with_count(25_000),
                                            ops.flat_map(
                                                lambda rows: executor.submit(self.parse_rows, rows, record, content_length)))
                        data.subscribe(
                            on_next=lambda x: None,
                            on_error=lambda er: print(f"Unexpected error: {er}")
                        )
            except:
                print("Unexpected error from here:", sys.exc_info()[0],"\n", record)
                record.status = f"INFLUX_{status_elements[1]}_UPLOAD_FAIL"
                record.delete_info_file()
                records_next.put(record)
                raise

            finally:
                #os.remove(csv_path)
                record.status = f"INFLUX_{status_elements[1]}_UPLOAD"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
                records_current.task_done()
                return
        else:
            records_next.put(record)

        records_current.task_done()

    def ImportCSVs(self, records_current, records_next, csv_chunks_queue, csv_counter, csv_record):
        # cpu count -1: Manager, -1: DBWriter,

        writer = _InfluxDBWriter(self.args, csv_chunks_queue)
        writer.start()

        with Progress() as progress:
            records_count = records_current.qsize()
            task_id = progress.add_task(f"[cyan]Posting CSV lines to InfluxDB...", total=records_count)
            with ProcessPoolExecutor(max_workers=(multiprocessing.cpu_count() - 2),
                                                initializer=self.init_counters, 
                                                initargs=(csv_chunks_queue,
                                                            csv_counter,
                                                            csv_progress,
                                                            records_current,
                                                            records_next,
                                                            self.args.copy())) as executor:
                futures = []

                while not records_current.empty():
                    record = records_current.get()

                    if record is None:
                        break

                    future = executor.submit(self.importCSV, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)

                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    records_current.task_done()
                    futures.remove(future)
                    del future

        records_current.join()
        
        csv_chunks_queue.put(None)
        csv_chunks_queue.join()

        records_next.dump_to_queue(records_current)
        records_current.write_pickle(f"{self.args['data_directory']}/{self.args['queue_filename']}")

class _InfluxDBWriter(multiprocessing.Process):
    def __init__(self, args, csv_chunks_queue):
        multiprocessing.Process.__init__(self)
        self.args = args
        self.csv_chunks_queue = csv_chunks_queue
        self.client = InfluxDBClient(url=self.args['INFLUX_URL'], token=self.args['INFLUX_TOKEN'], org=self.args['INFLUX_ORG'], debug=False)
        self.write_api = self.client.write_api(write_options=WriteOptions(
                                                                write_type=WriteType.batching,
                                                                batch_size=25_000,
                                                                flush_interval=12_000))
    def run(self):
        while True:
            chunk = self.csv_chunks_queue.get()

            if chunk is None:
                self.terminate()
                self.csv_chunks_queue.task_done()
                break

            self.write_api.write(org=self.args['INFLUX_ORG'], bucket=self.args['INFLUX_BUCKET'], record=chunk, write_precision=WritePrecision.MS)
            self.csv_chunks_queue.task_done()

    def terminate(self):
        self.write_api.__del__()
        self.client.__del__()

class _ProgressTextIOWrapper(io.TextIOWrapper):
    def __init__(self, *args, **kwargs):
        io.TextIOWrapper.__init__(self, *args, **kwargs)
        self.progress = None

    def readline(self, *args, **kwargs):
        readline = super().readline(*args, **kwargs)
        self.progress.value += len(readline)
        return readline
