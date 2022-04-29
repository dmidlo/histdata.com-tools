import itertools
import sys
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import datatable as dt
from datatable import f
dt.options.progress.enabled = False
from rich.progress import Progress
from rich.progress import BarColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from histdatacom.utils import replace_date_punct


class _API():
    def __init__(self, args_, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args_

        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

    def init_counters(self, records_current_, records_next_, args_):
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def create_jay(self, record):
        zip_path = record.data_dir + record.zip_filename
        zip_data = self.import_file_to_datatable(record, zip_path)

        record.jay_filename = ".data"
        jay_path = record.data_dir + record.jay_filename
        self.export_datatable_to_jay(zip_data, jay_path)

        record.jay_linecount = zip_data.nrows
        record.jay_start = self.extract_single_value_from_frame(zip_data, 0, "datetime")
        record.jay_end = self.extract_single_value_from_frame(zip_data, zip_data.nrows - 1, "datetime")
        record.write_info_file(base_dir=self.args['default_download_dir'])

    def validate_jay(self, record):
        try:
            if str.lower(record.data_format) == "ascii" \
            and (record.data_timeframe == "T"
                 or record.data_timeframe == "M1"):
                if "CSV" in record.status:
                    self.create_jay(record)

            records_next.put(record, self.args)
        except Exception:
                print("Unexpected error:", sys.exc_info())
                record.delete_info_file()
                raise
        finally:
                records_current.task_done()

    def validate_jays(self, records_current, records_next):
            
            records_count = records_current.qsize()
            with Progress(TextColumn(text_format=f"[cyan]Staging {records_count} datafiles..."),
                          BarColumn(),
                          "[progress.percentage]{task.percentage:>3.0f}%",
                          TimeElapsedColumn()) as progress:
                task_id = progress.add_task("extract", total=records_count)
                
                with ProcessPoolExecutor(max_workers=(multiprocessing.cpu_count() - 1),
                                         initializer=self.init_counters, 
                                         initargs=(records_current,
                                                   records_next,
                                                   self.args.copy())) as executor:
                    futures = []

                    while not records_current.empty():
                        record = records_current.get()

                        if record is None:
                            return

                        future = executor.submit(self.validate_jay, record)
                        progress.advance(task_id, 0.25)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        progress.advance(task_id, 0.75)
                        futures.remove(future)
                        del future

            records_current.join()
            records_next.dump_to_queue(records_current)

    def merge_jays(self, records_current, records_next):

        records_to_merge = []
        pairs = []
        timeframes = []
        while not records_current.empty():
            record = records_current.get()

            if record is None:
                return

            if (record.jay_filename == ".data" 
            and os.path.exists(record.data_dir + record.jay_filename)):
                pairs.append(record.data_fxpair)
                timeframes.append(record.data_timeframe)
                records_to_merge.append(record)

        sets_to_merge = []
        for timeframe, pair in itertools.product(set(timeframes), set(pairs)):
            tp_set_dict = {
                'timeframe': timeframe,
                'pair': pair,
                'records': [],
                'data': None
            }
            for m_record in records_to_merge:
                if m_record.data_timeframe == timeframe \
                and m_record.data_fxpair == pair:
                    tp_set_dict['records'].append(m_record)
            sets_to_merge.append(tp_set_dict)
        

        for tp_set in sets_to_merge:
            self.merge_records(tp_set)

        if len(sets_to_merge) == 1:
            return sets_to_merge[0]["data"]
        return sets_to_merge

    def merge_records(self, tp_set_dict):
        match tp_set_dict['timeframe']:
            case "T":
                merged = dt.Frame(names=["datetime", "bid", "ask", "vol"])
            case "M1":
                merged = dt.Frame(names=["datetime", "open", "high", "low", "close", "vol"])

        tp_set_dict['records'].sort(key=lambda record: record.jay_start)

        records_count = len(tp_set_dict)
        with Progress(TextColumn(text_format=f"[cyan]Merging {records_count} records..."),
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.0f}%",
                        TimeElapsedColumn()) as progress:
            task_id = progress.add_task("extract", total=records_count)

            for m_record in tp_set_dict['records']:
                jay_path = m_record.data_dir + m_record.jay_filename
                jay_data = self.import_jay_data(jay_path)
                merged.rbind(jay_data)

            if self.args['api_return_type'] == "datatable":
                tp_set_dict['data'] = merged
            if self.args['api_return_type'] == "arrow":
                tp_set_dict['data'] = merged.to_arrow()
            if self.args['api_return_type'] == "pandas":
                tp_set_dict['data'] = merged.to_pandas()
        
    @classmethod
    def extract_single_value_from_frame(cls, frame, row, column):
        return int(replace_date_punct(frame[row, column]))

    @classmethod
    def import_file_to_datatable(cls, record, zip_path):
        try:
            match record.data_timeframe:
                case "M1":
                    data = dt.fread(zip_path, 
                                    header=False,
                                    columns=["datetime", "open", "high", "low", "close", "vol"],
                                    multiple_sources="ignore")
                case "T":
                    data = dt.fread(zip_path,
                                    header=False,
                                    columns=["datetime", "bid", "ask", "vol"],
                                    multiple_sources="ignore")
                case _:
                    raise ValueError("Error creating jay")

            data['vol'] = dt.int32
            return data
        except ValueError as err:
            print(err)
            sys.exit(err)

    @classmethod
    def export_datatable_to_jay(cls, data_frame, file_path):
        data_path = file_path
        data_frame.to_jay(data_path)
        return 0

    @classmethod
    def import_jay_data(cls, jay_path):
        return dt.fread(jay_path)
