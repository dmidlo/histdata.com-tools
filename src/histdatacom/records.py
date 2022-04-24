import contextlib
import queue
import pickle
import os
import sys
from rich import print
from histdatacom.fx_enums import Timeframe, Format
from histdatacom.utils import get_month_from_datemonth
from histdatacom.utils import get_year_from_datemonth
from histdatacom.utils import get_query_string
from histdatacom.utils import create_full_path


class Record:
    def __init__(self, **kwargs):
        self.url = kwargs.get("url", "")
        self.status = kwargs.get("status", "")
        self.encoding = kwargs.get("encoding", "")
        self.bytes_length = kwargs.get("bytes_length", "")
        self.data_date = kwargs.get("data_date", "")
        self.data_year = kwargs.get("data_year", "")
        self.data_month = kwargs.get("data_month", "")
        self.data_datemonth = kwargs.get("data_datemonth", "")
        self.data_format = kwargs.get("data_format", "")
        self.data_timeframe = kwargs.get("data_timeframe", "")
        self.data_fxpair = kwargs.get("data_fxpair", "")
        self.data_dir = kwargs.get("data_dir", "")
        self.data_tk = kwargs.get("data_tk", "")
        self.zip_filename = kwargs.get("zip_filename", "")
        self.csv_filename = kwargs.get("csv_filename", "")
        self.jay_filename = kwargs.get("jay_filename", "")
        self.csv_linecount = kwargs.get("csv_linecount", "")
        self.jay_linecount = kwargs.get("jay_linecount", "")
        self.jay_start = kwargs.get("jay_start", "")
        self.jay_end = kwargs.get("jay_end", "")
        self.zip_persist = kwargs.get("zip_persist", "")

    def __call__(self, string="updated", **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])
        return self

    def set_datetime_attrs(self):
        self.data_year = get_year_from_datemonth(self.data_datemonth)
        self.data_month = get_month_from_datemonth(self.data_datemonth)

    def set_record_data_dir(self, base_dir):
        query_string_args = get_query_string(self.url)
        length = len(query_string_args)

        csv_format = Format(query_string_args[1]).name
        timeframe = Timeframe(query_string_args[2]).name

        record_data_dir = base_dir + csv_format + os.sep + timeframe + os.sep

        if length == 3:
            self.data_dir = record_data_dir
            return self.data_dir

        pair = query_string_args[3]
        record_data_dir = record_data_dir + pair.lower() + os.sep

        if length == 4:
            self.data_dir = record_data_dir
            return self.data_dir

        year = query_string_args[4]
        record_data_dir = record_data_dir + year + os.sep

        if length == 5:
            self.data_dir = record_data_dir
            return self.data_dir

        month = query_string_args[5]
        record_data_dir = record_data_dir + month + os.sep

        if length == 6:
            self.data_dir = record_data_dir
            return self.data_dir

    def set_csv_linecount(self):
        with open(f"{self.data_dir}{self.csv_filename}") as csv:
            self.csv_linecount = sum(1 for _ in csv)

    def create_record_data_dir(self, base_dir=""):
        try:
            if self.data_dir != "":
                create_full_path(self.data_dir)
            elif base_dir != "":
                create_full_path(self.set_record_data_dir(base_dir))
            else:
                raise ValueError("Error: create_record_data_dir not provided base_dir=")
        except ValueError as err:
            print(err)
            sys.exit()

    def write_info_file(self, base_dir=""):
        try:
            if self.data_dir == "":
                if base_dir != "":
                    self.create_record_data_dir(base_dir=base_dir)
                else:
                    raise ValueError("Error: create_record_data_dir not provided base_dir=")

            if not os.path.exists(self.data_dir):
                create_full_path(self.data_dir)

            path = f'{self.data_dir}.info'

            with open(path, 'wb') as filepath:
                pickle.dump(self.to_dict(), filepath)
        except ValueError as err:
            print(err)
            sys.exit()

    def delete_info_file(self):
        if os.path.exists(f"{self.data_dir}.info"):
            os.remove(f"{self.data_dir}.info")

    def restore_momento(self, base_dir):
        self.set_record_data_dir(base_dir)
        if not os.path.exists(f"{self.data_dir}.info"):
            return False
        record_dict = {}
        with open(f"{self.data_dir}.info", 'rb') as fileread:
            with contextlib.suppress(Exception):
                while True:
                    record_dict.update(pickle.load(fileread))
        self(**record_dict)
        return True

    def to_dict(self):
        return {'url': self.url,
                'status': self.status,
                'encoding': self.encoding,
                'bytes_length': self.bytes_length,
                'data_date': self.data_date,
                'data_year': self.data_year,
                'data_month': self.data_month,
                'data_datemonth': self.data_datemonth,
                'data_format': self.data_format,
                'data_timeframe': self.data_timeframe,
                'data_fxpair': self.data_fxpair,
                'data_dir': self.data_dir,
                'data_tk': self.data_tk,
                'zip_filename': self.zip_filename,
                'csv_filename': self.csv_filename,
                'csv_linecount': self.csv_linecount,
                'jay_linecount': self.jay_linecount,
                'jay_start': self.jay_start,
                'jay_end': self.jay_end,
                'jay_filename': self.jay_filename,
                'zip_persist': self.zip_persist}

    def print_record(self, string="Updated"):
        print(f"{string}:",
              self.status,
              self.data_format,
              self.data_timeframe,
              self.data_fxpair,
              self.data_year,
              self.data_month,
              "-",
              self.data_dir)


class Records(queue.Queue):
    def __init__(self, *args, **kwargs):
        queue.Queue.__init__(self, *args, **kwargs)

    def print(self):
        printable = []

        while not self.empty():
            record = self.get()
            if record is None:
                break

            printable.append(record.to_dict())

        for p_record in printable:
            new_record = Record(**p_record)
            print(new_record.to_dict())
            self.put(new_record)

    def write_pickle(self, path):
        picklable = []

        while not self.empty():
            record = self.get()
            if record is None:
                break

            picklable.append(record.to_dict())

        with open(path, 'wb') as filepath:
            pickle.dump(picklable, filepath)

        for p_record in picklable:
            new_record = Record(**p_record)
            self.put(new_record)

    def __contains__(self, item):
        with self.mutex:
            return item in self.queue

    def __len__(self):
        return len(self.queue)

    def dump_to_queue(self, dst_queue, count=0):
        if count == 0:
            _count = None
        else:
            _count = count
            _counter = 0

        while not self.empty():
            record = self.get()

            if record is None:
                break

            if _count is None:
                dst_queue.put(record)
            elif _counter < _count:
                dst_queue.put(record)
                _counter += 1
            else:
                self.put(record)
                break
