from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing, requests, sys, os, pickle
import zipfile
from urllib.parse import urlparse
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn
import bs4
from bs4 import BeautifulSoup

from rich import print

from histdatacom.utils import get_query_string, create_full_path
from histdatacom.utils import get_year_from_datemonth, get_month_from_datemonth, get_current_datemonth_GMTplus5, replace_date_punct
from histdatacom.fx_enums import Timeframe, Platform, get_valid_platform_timeframes
from histdatacom.records import Record

class _URLs:
    def __init__(self, args, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args
        
        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

        self.args["base_url"] = 'http://www.histdata.com/download-free-forex-data/'
        self.args["post_headers"] = {
                "Host": "www.histdata.com",
                "Connection": "keep-alive",
                "Content-Length": "101",
                "Cache-Control": "max-age=0",
                "Origin": "http://www.histdata.com",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Referer": "",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9"}

    def init_counters(self, records_current_, records_next_, args_):
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def populateInitialQueue(self, records_current, records_next):
        for url in self.generate_form_urls(self.args["start_yearmonth"],
                                      self.args["end_yearmonth"],
                                      self.args['platforms'],
                                      self.args["pairs"],
                                      self.args['timeframes'],
                                      self.args["base_url"]):
            record = Record()
            record( url = url, status = "URL_NEW")
            record.restore_momento(base_dir=self.args['default_download_dir'])
            if record.status != "URL_NO_REPO_DATA":
                record.write_info_file(base_dir=self.args['default_download_dir'])
                records_next.put(record)

        records_next.dump_to_queue(records_current)

    def validate_url(self, record):    
        try:
            if record.status == "URL_NEW":
                page_data = self.get_page_data(record.url)
                self.fetch_form_values(page_data, record)
                
                if record.data_tk == "":
                    raise ValueError
                else:
                    record.status = f"URL_VALID"
                    record.write_info_file(base_dir=args['default_download_dir'])
                    records_next.put(record)
            else:
                records_next.put(record)
        except ValueError:
            print(f"No data in web repository for: {record.url}")
            record.status = f"URL_NO_REPO_DATA"
            record.write_info_file(base_dir=args['default_download_dir'])
        except:
            print(f"Unknown Error for URL: {record.url}", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def validateURLs(self, records_current, records_next):

        records_count = records_current.qsize()
        with Progress(
                TextColumn(text_format=f"[cyan]Validating {records_count} URLs..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn()) as progress:

            task_id = progress.add_task(f"Validating URLs", total=records_count)
            with ThreadPoolExecutor(max_workers=(multiprocessing.cpu_count() - 1) * 3,
                                initializer=self.init_counters, 
                                initargs=(records_current,
                                    records_next,
                                    self.args.copy())) as executor:
                futures = []

                while not records_current.empty():
                    record = records_current.get()

                    if record is None:
                        return

                    future = executor.submit(self.validate_url, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)
                
                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    futures.remove(future)
                    del future

        records_current.join()
        records_next.dump_to_queue(records_current)

    def download_zip(self, record):
        try:
            if ("URL_VALID" in record.status):
                post_headers = args['post_headers'].copy()
                post_headers["Referer"] = record.url
                res = requests.post("http://www.histdata.com/get.php",
                    data = {
                        "tk": record.data_tk,
                        "date": record.data_date,
                        "datemonth": record.data_datemonth,
                        "platform": record.data_platform,
                        "timeframe": record.data_timeframe,
                        "fxpair": record.data_fxpair},
                    headers=post_headers)
                record.zip_filename = res.headers["Content-Disposition"].split(";")[1].split("=")[1]

                zip_path = record.data_dir + record.zip_filename
                with open(zip_path, "wb") as zip_file:
                    zip_file.write(res.content)

                record.status = f"CSV_ZIP"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
            else:
                records_next.put(record)  
        except KeyError:
            print(f"Invalid Zip on Repository: {record.url}", sys.exc_info())
            record.delete_info_file()
        except:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()
        
    def downloadZIPs(self, records_current, records_next):

        records_count = records_current.qsize()
        with Progress(
                TextColumn(text_format=f"[cyan]Downloading {records_count} ZIPs..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn()) as progress:

            task_id = progress.add_task(f"[cyan]Downloading ZIPs", total=records_count)
            with ThreadPoolExecutor(max_workers=(multiprocessing.cpu_count() - 1) * 3,
                                initializer=self.init_counters, 
                                initargs=(records_current,
                                    records_next,
                                    self.args.copy())) as executor:
                futures = []

                while not records_current.empty():
                    record = records_current.get()

                    if record is None:
                        return

                    future = executor.submit(self.download_zip, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)

                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    futures.remove(future)
                    del future

        records_current.join()

        records_next.dump_to_queue(records_current)

    @classmethod
    def get_page_data(cls, url):
        request = requests.get(url)

        page_content = BeautifulSoup(request.content, "html.parser")
        encoding = dict(request.headers)['Content-Encoding']
        bytes_length = dict(request.headers)['Content-Length']
        
        return {"page_content": page_content, "encoding": encoding, "bytes_length": bytes_length}

    @classmethod
    def get_page_links(cls, page_data, class_id, base_url):

        html_data = page_data['page_content'].findAll('div', attrs={'class': class_id})

        links = list()
        for element in html_data:
            for a_tag in element.findAll('a')[:-1]: # :-1 to trim the last for histdata.com
                links.append(base_url + a_tag['href']) # hrefs from histdata.com are relative

        page_data['links'] = links
        return page_data

    @classmethod
    def get_base_url(cls, url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @classmethod
    def fetch_form_values(cls, page_data, record):
        form_page_content = page_data['page_content']
        form = form_page_content.find_all('form', id='file_down')

        for element in form:
            for e in element:
                if type(e) is bs4.element.Tag:
                    if e.get("id") == "tk":
                        record.data_tk = e.get("value")

                    if e.get("id") == "date":
                        record.data_date = e.get("value")

                    if e.get("id") == "datemonth":
                        record.data_datemonth = e.get("value")

                    if e.get("id") == "platform":
                        record.data_platform = e.get("value")

                    if e.get("id") == "timeframe":
                        record.data_timeframe = e.get("value")

                    if e.get("id") == "fxpair":
                        record.data_fxpair = e.get("value")
        return record

    @classmethod
    def generate_form_urls(cls, sYearMonth, eYearMonth, platforms, pairs, timeframes, base_url):
        
        try:
            if int(replace_date_punct(sYearMonth)) < 200000:
                raise ValueError
        except ValueError:
            print(f"start date of {sYearMonth} is before 2000-00.")
            print("setting start date to 2000-00")
            sYearMonth = "200000"

        current_yearmonth = get_current_datemonth_GMTplus5()

        try:
            if int(replace_date_punct(eYearMonth)) > int(current_yearmonth):
                raise ValueError
        except ValueError:
            print(f"start date of {eYearMonth} is in the future.")
            print(f"setting end date to {current_yearmonth}")
            eYearMonth = current_yearmonth

        start_year = int(get_year_from_datemonth(sYearMonth))
        start_month = int(get_month_from_datemonth(sYearMonth))
        end_year = int(get_year_from_datemonth(eYearMonth))
        end_month = int(get_month_from_datemonth(eYearMonth))

        for platform in platforms:
            for timeframe in timeframes:
                if timeframe in get_valid_platform_timeframes(platform):
                    for pair in pairs:
                        form_url = f"{base_url}?/{platform}/{Timeframe[timeframe].value}/{pair}"
                        for year in range(start_year,
                                        end_year + 1):
                            if timeframe in ("M1"):
                                current_year = int(get_year_from_datemonth(str(current_yearmonth)))

                                if year == current_year:
                                    for month in range(1, end_month + 1):
                                        yield f"{base_url}?/{platform}/{Timeframe[timeframe].value}/{pair}/{year}/{month}"
                                else:
                                    yield f"{base_url}?/{platform}/{Timeframe[timeframe].value}/{pair}/{year}/"   
                            else:
                                if year == start_year:
                                    if start_month == 0:
                                        start_month = 1

                                    if year == end_year:
                                        for month in range(start_month, end_month + 1):
                                            yield f"{form_url}/{year}/{month}"
                                    
                                    else:
                                        for month in range(start_month, 13):
                                            yield f"{form_url}/{year}/{month}"

                                elif year == end_year:
                                    for month in range(1, end_month + 1):
                                        yield f"{form_url}/{year}/{month}"
                            
                                else:
                                    for month in range(1,13):
                                        yield f"{form_url}/{year}/{month}"
                else:
                    # pass on else for "if timeframe in get_valid_platform_timeframes(platform)"
                    pass
