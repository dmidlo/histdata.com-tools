"""histdatacom.api
"""
import itertools
import sys
import os
from rich.progress import Progress
from rich.progress import BarColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
import datatable as dt
from datatable import f
from datatable import update
from datatable import Frame
from pandas.core.frame import DataFrame
from pyarrow import Table
from histdatacom.records import Record
from histdatacom.records import Records
from histdatacom.urls import _URLs
from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.concurrency import ProcessPool
from histdatacom import config
dt.options.progress.enabled = False

class _API():
    @classmethod
    def create_jay(cls, record: Record, args: dict) -> None:
        """creates a datatable file, saves it in dt's native jay format
           using and updating relevant information in a Record of work.

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """

        zip_path = record.data_dir + record.zip_filename
        csv_path = record.data_dir + record.csv_filename
        if os.path.exists(zip_path):
            file_data = cls.import_file_to_datatable(record, zip_path)
        elif os.path.exists(csv_path):
            file_data = cls.import_file_to_datatable(record, csv_path)

        record.jay_filename = ".data"
        jay_path = record.data_dir + record.jay_filename
        cls.export_datatable_to_jay(file_data, jay_path)

        record.jay_line_count = file_data.nrows
        record.jay_start = cls.extract_single_value_from_frame(file_data, 0, "datetime")
        record.jay_end = cls.extract_single_value_from_frame(file_data,
                                                             file_data.nrows - 1,
                                                             "datetime")
        record.write_info_file(base_dir=args['default_download_dir'])

    @classmethod
    def test_for_jay_or_create(cls, record: Record, args: dict) -> None:
        """a helper method to ensure the existence of a Record's jay file
           prior to further processing from the API or Influx classes

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """

        if str.lower(record.data_format) == "ascii" and record.data_timeframe in ["T", "M1"]:
            jay_path = f"{record.data_dir}.data"
            if not os.path.exists(jay_path):
                if "CSV" not in record.status:
                    _URLs.get_zip_file(record, args)
                cls.create_jay(record, args)

    @classmethod
    def validate_jay(cls, record: Record,
                          args: dict,
                          records_current: Records,
                          records_next: Records) -> None:
        """A Wrapper to be passed to an individual process within the process pool
           to test for or create a datatable jay file based on a Record of Work's
           information.  Receives a unit of work from the pool, performs validation,
           readies the Record for further processing, and marks the current work as
           complete.

        Args:
            record (Record): a Histdatacom.records.Record
            args (dict): arguments received from argparse
            records_current (multiprocessing.managers.AutoProxy[Record]):
                Current Work Records Queue
            records_next (multiprocessing.managers.AutoProxy[Record]):
                Records Queue for Further Work
        """
        try:
            cls.test_for_jay_or_create(record, args)
            records_next.put(record)
        except Exception:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def validate_jays(self) -> None:
        """Initializes a process pool and calls self.validate_jay against
           a Queue of records.

        Args:
            records_current (multiprocessing.managers.AutoProxy[Record]):
                Current Work Records Queue
            records_next (multiprocessing.managers.AutoProxy[Record]):
                Records Queue for Further Work
        """
        pool = ProcessPool(self.validate_jay,
                            config.args,
                            "Staging", "data files...",
                            get_pool_cpu_count(config.args['cpu_utilization']))
        pool(config.current_queue, config.next_queue)

    def merge_jays(self) -> list | Frame | DataFrame | Table:

        records_to_merge = []
        pairs = []
        timeframes = []
        while not config.current_queue.empty():
            record = config.current_queue.get()

            if record is None:
                break

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

        return sets_to_merge[0]["data"] if len(sets_to_merge) == 1 else sets_to_merge

    def merge_records(self, tp_set_dict: dict) -> None:
        match tp_set_dict['timeframe']:
            case "T":
                merged = dt.Frame(names=["datetime", "bid", "ask", "vol"])
            case "M1":
                merged = dt.Frame(names=["datetime", "open", "high", "low", "close", "vol"])

        tp_set_dict['records'].sort(key=lambda record: record.jay_start)

        records_count = len(tp_set_dict)
        with Progress(TextColumn(text_format="[cyan]Merging records..."),
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.0f}%",
                        TimeElapsedColumn()) as progress:
            progress.add_task("merge", total=records_count)

            for m_record in tp_set_dict['records']:
                jay_path = m_record.data_dir + m_record.jay_filename
                jay_data = self.import_jay_data(jay_path)
                merged.rbind(jay_data)

            match config.args['api_return_type']:
                case "datatable":
                    tp_set_dict['data'] = merged
                case "arrow":
                    tp_set_dict['data'] = merged.to_arrow()
                case "pandas":
                    tp_set_dict['data'] = merged.to_pandas()

    @classmethod
    def extract_single_value_from_frame(cls, frame: DataFrame, row: int, column: str) -> int:
        return int(frame[row, column])

    @classmethod
    def import_file_to_datatable(cls, record: Record, zip_path: str) -> Frame:
        try:
            match record.data_timeframe:
                case "M1":
                    data = dt.fread(zip_path,
                                    header=False,
                                    columns=["datetime", "open", "high", "low", "close", "vol"],
                                    multiple_sources="ignore")

                    ascii_m1_str_splitter = (dt.time.ymdt(f.datetime[0:4].as_type(int), \
                                             f.datetime[4:6].as_type(int), \
                                             f.datetime[6:8].as_type(int), \
                                             f.datetime[9:11].as_type(int), \
                                             f.datetime[11:13].as_type(int), \
                                             f.datetime[13:15].as_type(int)))
                    ascii_m1_etc_ms_timestamp = (ascii_m1_str_splitter.as_type(int)//10**6)
                    ascii_m1_utc_ms_timestamp = (ascii_m1_etc_ms_timestamp + 18000000)
                    data[:, update(datetime = ascii_m1_utc_ms_timestamp)]
                case "T":
                    data = dt.fread(zip_path,
                                    header=False,
                                    columns=["datetime", "bid", "ask", "vol"],
                                    multiple_sources="ignore")

                    ascii_t_str_splitter = (dt.time.ymdt(f.datetime[0:4].as_type(int), \
                                            f.datetime[4:6].as_type(int), \
                                            f.datetime[6:8].as_type(int), \
                                            f.datetime[9:11].as_type(int), \
                                            f.datetime[11:13].as_type(int), \
                                            f.datetime[13:15].as_type(int), \
                                            10**6 * f.datetime[15:18].as_type(int)))
                    ascii_t_etc_ms_timestamp = (ascii_t_str_splitter.as_type(int)//10**6)
                    ascii_t_utc_ms_timestamp = (ascii_t_etc_ms_timestamp + 18000000)
                    data[:, update(datetime = ascii_t_utc_ms_timestamp)]
                case _:
                    raise ValueError("Error creating jay")

            data['vol'] = dt.int32
            return data
        except ValueError as err:
            print(err)
            sys.exit(err)

    @classmethod
    def export_datatable_to_jay(cls, data_frame: DataFrame, file_path: str) -> None:
        data_path = file_path
        data_frame.to_jay(data_path)

    @classmethod
    def import_jay_data(cls, jay_path: str) -> Frame:
        return dt.fread(jay_path)
