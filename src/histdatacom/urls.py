from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing, requests, sys, os, pickle
from urllib.parse import urlparse
from rich.progress import Progress
import bs4
from bs4 import BeautifulSoup

from rich import print

from histdatacom.utils import get_query_string, create_full_path
from histdatacom.fx_enums import Timeframe, Platform
from histdatacom.records import Record

class _URLs:
    def __init__(self, args, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args
        
        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

        self.fetch_dl_index_links()

    def fetch_dl_index_links(self):
        print(f"Generating records for urls in main download page: \n {self.args['index_url']}")
        for url in self.get_page_links(
                        self.get_page_data(self.args['index_url']),
                                                self.args['html_container_classname'],
                                                self.args['base_url'])['links']:
            
            query_string = get_query_string(url)
            if (query_string[1] in self.args['platforms']) and (Timeframe(query_string[2]).name in self.args['timeframes']):
                record = Record()

                record( url = url, 
                        status = "URL_INDEX_NEW")

                records_current.put(record)
                
        create_full_path(f"{self.args['data_directory']}")

    def init_counters(self, records_current_, records_next_, args_):
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def restore_momento(self, record, base_dir):
        record.set_record_data_dir(base_dir)
        if os.path.exists(f"{record.data_dir}.info"):
            record_dict = dict()
            with open(f"{record.data_dir}.info", 'rb') as fileread:
                try:
                    while True:
                        record_dict.update(pickle.load(fileread))
                except:
                    pass

            record(**record_dict)
            return True
        else:
            return False

    def walk_index_url(self, record):

        if record is None:
            return

        base_url = args['base_url']
        class_name = args['html_container_classname']

        query_string = get_query_string(record.url)
        if (query_string[1] in args['platforms']) and (Timeframe(query_string[2]).name in args['timeframes']):
            
            if record.status == "URL_INDEX_NEW":
                platform_data = self.get_page_data(record.url)
                
                record(
                    links = self.get_page_links(platform_data, class_name, base_url)['links'],
                    encoding = platform_data['encoding'],
                    bytes_length = platform_data['bytes_length'],
                    data_platform = Platform(query_string[1]).name,
                    data_timeframe = Timeframe(query_string[2]).name,
                    status = "URL_PLATFORM_PARENT")
                record.write_info_file(base_dir=args['default_download_dir'])
                records_current.put(record)
            
            elif record.status == "URL_PLATFORM_PARENT":

                for pair_url in record.links:

                    query_string = get_query_string(pair_url)
                    
                    if query_string[3].lower() in args['pairs']:
                        p_record = Record()
                        pair_data = self.get_page_data(pair_url)
                        p_record(
                            links = self.get_page_links(pair_data, class_name, base_url)['links'],
                            url = pair_url,
                            encoding = pair_data['encoding'],
                            bytes_length = pair_data['bytes_length'],
                            data_platform = Platform(query_string[1]).name,
                            data_timeframe = Timeframe(query_string[2]).name,
                            data_fxpair = query_string[3].lower(),
                            status = "URL_PAIR_PARENT")
                        p_record.write_info_file(base_dir=args['default_download_dir'])
                        records_current.put(p_record)
                
                record.status = "URL_PLATFORM_PARENT_DONE"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
            
            elif record.status == "URL_PAIR_PARENT":

                for year_url in record.links:
                    
                    query_string = get_query_string(year_url)
                    
                    y_record = Record()
                    year_data = self.get_page_data(year_url)

                    y_record.url = year_url
                    y_record.encoding = year_data['encoding']
                    y_record.bytes_length = year_data['bytes_length']
                    y_record.data_platform = Platform(query_string[1]).name
                    y_record.data_timeframe = Timeframe(query_string[2]).name
                    y_record.data_fxpair = query_string[3].lower()
                    y_record.data_year = query_string[4]

                    y_record(status = "URL_YEAR_FORM")
                    try:
                        self.fetch_form_values(year_data, y_record)
                        if y_record.data_tk != "":
                            y_record.set_datetime_attrs()

                            if not self.restore_momento(y_record, args['default_download_dir']):
                                y_record.write_info_file(base_dir=args['default_download_dir'])

                            records_next.put(y_record)
                        else:
                            y_record(
                                links = self.get_page_links(year_data, class_name, base_url)['links'],
                                status = "URL_YEAR_PARENT")

                            if 'http://www.histdata.comjavascript:return true;' in y_record.links:
                                y_record(status = "URL_YEAR_INVALID")
                                records_next.put(y_record)
                                continue
                            
                            y_record.write_info_file(base_dir=args['default_download_dir'])
                            records_current.put(y_record)
                    except:
                        raise
                
                record.status = "URL_PAIR_PARENT_DONE"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
            
            elif record.status == "URL_YEAR_PARENT":

                for month_url in record.links:
                    m_record = Record()
                    month_data = self.get_page_data(month_url)

                    m_record.url = month_url
                    m_record.encoding = month_data['encoding']
                    m_record.bytes_length = month_data['bytes_length']

                    self.fetch_form_values(month_data, m_record)
                    if m_record.data_tk != "":
                        m_record.set_datetime_attrs()
                        m_record(status = "URL_MONTH_FORM")

                        if not self.restore_momento(m_record, args['default_download_dir']):
                            m_record.write_info_file(base_dir=args['default_download_dir'])

                        records_next.put(m_record)
                    else:
                        m_record(status = "URL_MONTH_EXCEPTION")
                        records_next.put(m_record)

                record.status = "URL_YEAR_PARENT_DONE"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
        else:
            records_next.put(record)

        records_current.task_done()

    def walkIndexURLs(self, records_current, records_next):
        with Progress() as progress:
            task_id = progress.add_task("[cyan]Walking Histdata.com Repository...", start=False)
            while not records_current.empty():
                with ThreadPoolExecutor(max_workers=(multiprocessing.cpu_count() - 1) * 3,
                                        initializer=self.init_counters, 
                                        initargs=(records_current,
                                            records_next,
                                            self.args.copy())) as executor:
                    futures = []
                    while not records_current.empty():
                        record = records_current.get()
                        future = executor.submit(self.walk_index_url, record)
                        futures.append(future)
                        
                    for future in as_completed(futures):
                        futures.remove(future)
                        del future
            
        records_current.join()
        records_next.dump_to_queue(records_current)
        records_current.write_pickle(f"{self.args['data_directory']}/{self.args['queue_filename']}")

    def download_zip(self, record):
        
        if ("FORM" in record.status) and (("DONE" not in record.status) 
                                        or ("KEYERROR" not in record.status)
                                        or ("FAIL" not in record.status)):
            status_elements = record.status.split("_")

            try:
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
            except KeyError:
                record.status = f"URL_{status_elements[1]}_FORM_KEYERROR"
                record.delete_info_file()
                records_next.put(record)
                records_current.task_done()
                return
            except:
                print("Unexpected error:", sys.exc_info()[0])
                record.status = f"URL_{status_elements[1]}_FORM_FAIL"
                record.delete_info_file()
                records_next.put(record)
                raise
            else:
                zip_path = record.data_dir + record.zip_filename
                with open(zip_path, "wb") as zip_file:
                    zip_file.write(res.content)

                record.status = f"CSV_{status_elements[1]}_ZIP"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
                records_current.task_done()
                return
        else:
            records_next.put(record)

        records_current.task_done()
        
    def download_zips(self, records_current, records_next):
        with Progress() as progress:
            records_count = records_current.qsize()
            task_id = progress.add_task(f"[cyan]Downloading {records_count} CSVs...", total=records_count)
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
                    records_current.task_done()
                    futures.remove(future)
                    del future

        records_current.join()

        records_next.dump_to_queue(records_current)
        records_current.write_pickle(f"{self.args['data_directory']}/{self.args['queue_filename']}")

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
