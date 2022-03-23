import queue, pickle, os
from rich import print
from multiprocessing import current_process
from fx_enums import Timeframe, Platform
from utils import get_month_from_datemonth, get_year_from_datemonth, get_query_string, create_full_path

class Record:
        def __init__(self,
                    url="",
                    status="",
                    encoding="",
                    bytes_length=0,
                    data_date="",
                    data_year="",
                    data_month="",
                    data_datemonth="",
                    data_platform="",
                    data_timeframe="",
                    data_fxpair="",
                    data_dir="",
                    data_tk="",
                    zip_filename="",
                    csv_filename="",
                    csv_linecount="",
                    links=list()):
            self.url = url
            self.status = status
            self.encoding = encoding
            self.bytes_length = bytes_length
            self.data_date = data_date
            self.data_year = data_year
            self.data_month = data_month
            self.data_datemonth = data_datemonth
            self.data_platform = data_platform
            self.data_timeframe = data_timeframe
            self.data_fxpair = data_fxpair
            self.data_dir = data_dir
            self.data_tk = data_tk
            self.zip_filename = zip_filename
            self.csv_filename = csv_filename
            self.csv_linecount = csv_linecount
            self.links = links

        def __call__(self, str="updated", **kwargs):

            for arg in kwargs:
                setattr(self, arg, kwargs[arg])

            #self.print_record(str)
            return self

        def set_datetime_attrs(self):
            self.data_year = get_year_from_datemonth(self.data_datemonth)
            self.data_month = get_month_from_datemonth(self.data_datemonth)
            return

        def set_record_data_dir(self, base_dir):
            query_string_args = get_query_string(self.url)
            length = len(query_string_args)
            
            platform = Platform(query_string_args[1]).name
            timeframe = Timeframe(query_string_args[2]).name

            record_data_dir = base_dir + platform + os.sep + timeframe + os.sep

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

        def create_record_data_dir(self, base_dir=""):
            if not self.data_dir == "":
                create_full_path(self.data_dir)
            else:
                if not base_dir == "":
                    create_full_path(self.set_record_data_dir(base_dir))
                else:
                    print("Error: create_record_data_dir not provided base_dir=")
                    raise

        def to_dict(self):
            return {'url': self.url,
                    'status': self.status,
                    'encoding': self.encoding,
                    'bytes_length': self.bytes_length,
                    'data_date': self.data_date,
                    'data_year': self.data_year,
                    'data_month': self.data_month,
                    'data_datemonth': self.data_datemonth,
                    'data_platform': self.data_platform,
                    'data_timeframe': self.data_timeframe,
                    'data_fxpair': self.data_fxpair,
                    'data_dir': self.data_dir,
                    'data_tk': self.data_tk,
                    'zip_filename': self.zip_filename,
                    'csv_filename': self.csv_filename,
                    'csv_linecount': self.csv_linecount,
                    'links': self.links}

        def write_info_file(self, str="Momento", base_dir=""):
            if self.data_dir == "":
                if not base_dir == "":
                    self.create_record_data_dir(base_dir=base_dir)
                else:
                    print("Error: create_record_data_dir not provided base_dir=")
                    raise

            if not os.path.exists(self.data_dir):
                create_full_path(self.data_dir)

            path = self.data_dir + ".info"
            
            with open(path, 'wb') as filepath:
                pickle.dump(self.to_dict(), filepath)

            #self.print_record(str=f"{current_process().name} {str}")

        def delete_info_file(self):
            if os.path.exists(f"{self.data_dir}.info"):
                os.remove(f"{self.data_dir}.info")

        def set_csv_linecount(self):
            with open(f"{self.data_dir}{self.csv_filename}") as csv:
                self.csv_linecount = sum(1 for line in csv)

        def print_record(self, str="Updated"):
            print(f"{str}:",
                    self.status,
                    self.data_platform,
                    self.data_timeframe,
                    self.data_fxpair,
                    self.data_year,
                    self.data_month,
                    "-",
                    self.data_dir)

class Records(queue.Queue):
        def __init__(self, *args, **kwargs):
            queue.Queue.__init__(self, *args, **kwargs)

        def write_pickle(self, path):
            picklable = []
            
            while not self.empty():
                record = self.get()
                if record is None:
                    break
                
                picklable.append(record.to_dict())

            with open(path, 'wb') as filepath:
                pickle.dump(picklable, filepath)

            for r in picklable:
                new_record = Record(**r)
                self.put(new_record)

            return

        def __contains__(self, item):
            with self.mutex:
                return item in self.queue

        def __len__(self):
            return len(self.queue)

        def dump_to_queue(self, queue, count=0):
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
                    queue.put(record)
                else:
                    if _counter < _count:
                        queue.put(record)
                        _counter += 1
                    else:
                        self.put(record)
                        break
