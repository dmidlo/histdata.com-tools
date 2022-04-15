from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing, sys, os, zipfile
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn
import csv

from datetime import datetime
import pytz
from histdatacom.fx_enums import TimeFormat

from rich import print

class _CSVs:

    def __init__(self, args, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args
        
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

    def extract_csv(self, record):
        try:
            if ("CSV_ZIP" in record.status):
                zip_path = record.data_dir + record.zip_filename

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    [record.csv_filename] = [x for x in zip_ref.namelist() if (".csv" or ".xlsx") in x]
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                os.remove(zip_path)
                record.status = f"CSV_FILE"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
            else:
                records_next.put(record)
        except:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def extractCSVs(self, records_current, records_next):

        records_count = records_current.qsize()
        with Progress(
                TextColumn(text_format=f"[cyan]Extracting {records_count} CSVs..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn()) as progress:

            task_id = progress.add_task(f"[cyan]Extracting CSVs", total=records_count)
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

                    future = executor.submit(self.extract_csv, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)
                
                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    futures.remove(future)
                    del future

        records_current.join()

        records_next.dump_to_queue(records_current)
    
    def clean_csv(self, record):
        try:
            if ("CSV_FILE" in record.status):
                csv_path = record.data_dir + record.csv_filename
                temp_csv_path = record.data_dir + "temp.csv"

                header = self.header_match(record.data_platform, record.data_timeframe)

                with open(temp_csv_path, 'w', newline="") as destcsv:
                    dest_csv_writer = csv.writer(destcsv)
                    dest_csv_writer.writerow(header)

                    with open(csv_path, "r") as srccsv:
                        dialect = csv.Sniffer().sniff(srccsv.read(), delimiters=",; ")
                        srccsv.seek(0)
                        src_csv_reader = csv.reader(srccsv, dialect)

                        for row in src_csv_reader:
                            timestamp_utc = self.convert_datetime_to_UTC_timestamp(record.data_platform, record.data_timeframe, row)

                            row_prefix = ["histdata.com", record.data_platform, record.data_timeframe, record.data_fxpair]
                            row_data =  row_prefix + timestamp_utc + self.trim_row_data(record.data_platform, record.data_timeframe, row)

                            dest_csv_writer.writerow(row_data)

                os.remove(csv_path)
                os.rename(temp_csv_path, csv_path)

                record.status = f"CSV_CLEAN"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
            else:
                records_next.put(record)
        except:
            print("Unexpected error:", sys.exc_info())
            record.status = f"CSV_CLEAN_FAIL"
            record.delete_info_file()
            records_next.put(record)
            raise
        finally:
            records_current.task_done()

    def cleanCSVs(self, records_current, records_next):
        
        records_count = records_current.qsize()
        with Progress(
                TextColumn(text_format=f"[cyan]Cleaning {records_count} CSVs..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn()) as progress:

            task_id = progress.add_task(f"[cyan]Cleaning CSVs", total=records_count)
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

                    future = executor.submit(self.clean_csv, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)
                
                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    futures.remove(future)
                    del future

        records_current.join()
        records_next.dump_to_queue(records_current)

    @classmethod
    def header_match(cls, platform, timeframe):

        pre_header = ["Source", "Platform", "Timeframe", "Instrument"]

        match platform:
            case "MT" if timeframe == "M1":
                header = pre_header + ["secsSinceEpochUTC", "MTopenBid", "MThighBid", "MTlowBid", "MTcloseBid", "MTVolume"]
            case "ASCII" if timeframe == "M1":
                header = pre_header + ["secsSinceEpochUTC", "openBid", "highBid", "lowBid", "closeBid", "Volume"]
            case "ASCII" if timeframe == "T":
                header = pre_header + ["msSinceEpochUTC","bidQuote","askQuote","Volume"]
            case "NT" if timeframe == "M1":
                header = pre_header + ["secsSinceEpochUTC", "NTopenBid", "NThighBid", "NTlowBid", "NTcloseBid", "NTVolume"]
            case "NT" if timeframe == "T_LAST":
                header = pre_header + ["secsSinceEpochUTC", "sLastQuote", "ntlVolume"]
            case "NT" if timeframe == "T_BID":
                header = pre_header + ["secsSinceEpochUTC", "sBidQuote", "ntbVolume"]
            case "NT" if timeframe == "T_ASK":
                header = pre_header + ["secsSinceEpochUTC", "sAskQuote", "ntaVolume"]
            case "MS" if timeframe == "M1":
                header = pre_header + ["secsSinceEpochUTC", "MSopenBid", "MShighBid", "MSlowBid", "MScloseBid", "MSVolume"]

        return header

    @classmethod
    def get_timeformat(cls, platform, timeframe):

        format_enum_key = str(platform) + "_" + str(timeframe)

        return TimeFormat[format_enum_key].value

    @classmethod 
    def parse_datetime_columns(cls, platform, timeframe, row):

        match platform:
            case "MT" if timeframe == "M1":
                return str(row[0]) + " " + str(row[1])
            case "MS" if timeframe == "M1":
                return str(row[1])
            case _:
                return str(row[0])

    @classmethod
    def convert_datetime_to_UTC_timestamp(cls, platform, timeframe, row):
        
        est_timestamp = cls.parse_datetime_columns(platform, timeframe, row)
        date_object = datetime.strptime(est_timestamp, cls.get_timeformat(platform, timeframe))
        tz_date_object = date_object.replace(tzinfo=pytz.timezone("Etc/GMT+5"))

        if platform == "ASCII" and timeframe == "T":
            timestamp = int(tz_date_object.timestamp() * 1000)
        else:
            timestamp = int(tz_date_object.timestamp())

        return [str(timestamp)]

    @classmethod
    def trim_row_data(cls, platform, timeframe, row):

        match platform:
            case "MT" if timeframe == "M1":
                return row[2:]
            case "MS" if timeframe == "M1":
                return row[2:]
            case _:
                return row[1:]