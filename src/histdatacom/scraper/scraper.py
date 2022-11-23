from typing import Sized

import os
import sys
import traceback

from urllib.parse import urlparse
from urllib.parse import ParseResult

import requests
import bs4
from bs4 import BeautifulSoup


from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress
from rich.progress import TextColumn
from rich.progress import SpinnerColumn
from rich.progress import TimeElapsedColumn

from histdatacom import config
from histdatacom.records import Record
from histdatacom.scraper.urls import Urls
from histdatacom.concurrency import ThreadPool
from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.utils import Utils


class Scraper:
    @staticmethod
    def set_base_url() -> None:
        config.ARGS["base_url"] = "http://www.histdata.com/download-free-forex-data/"

    @staticmethod
    def set_post_headers() -> None:
        # pylint: disable=line-too-long
        config.ARGS["post_headers"] = {
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
            "Accept-Language": "en-US,en;q=0.9",
        }

    @staticmethod
    def populate_initial_queue() -> None:
        with Progress(
            TextColumn(text_format="[cyan] Generating API Requests"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            progress.add_task("waiting", total=0)

            if (
                not (
                    config.ARGS["update_remote_data"]
                    and config.ARGS["available_remote_data"]
                )
                and config.FILTER_PAIRS is None
            ):
                config.FILTER_PAIRS = config.ARGS["pairs"]

            for url in Urls.generate_form_urls(
                config.ARGS["start_yearmonth"],
                config.ARGS["end_yearmonth"],
                config.ARGS["formats"],
                config.FILTER_PAIRS,
                config.ARGS["timeframes"],
                config.ARGS["base_url"],
            ):
                record = Record()
                record(url=url, status="URL_NEW")
                record.restore_momento(base_dir=config.ARGS["default_download_dir"])

                if record.status != "URL_NO_REPO_DATA":
                    record.write_info_file(base_dir=config.ARGS["default_download_dir"])
                    if (
                        config.ARGS["update_remote_data"]
                        or config.FILTER_PAIRS
                        or (
                            config.ARGS["available_remote_data"]
                            and not config.REPO_DATA_FILE_EXISTS
                        )
                    ) and record.status != "URL_NEW":
                        Scraper.set_available_data(record)
                    config.NEXT_QUEUE.put(record)  # type: ignore

            config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    @staticmethod
    def validate_urls() -> None:
        pool = ThreadPool(
            Scraper.validate_url,
            config.ARGS,
            "Validating",
            "URLs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]) * 3,
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    @staticmethod
    def validate_url(record: Record, args: dict) -> None:
        try:
            if record.status == "URL_NEW":
                page_data: dict = Scraper.get_page_data(record.url)
                Scraper.fetch_form_values(page_data, record)

                if record.data_tk == "":
                    raise ValueError

                assert isinstance(config.FILTER_PAIRS, Sized)

                if (
                    config.ARGS["update_remote_data"]
                    or len(config.FILTER_PAIRS) > 0
                    or (
                        config.ARGS["available_remote_data"]
                        and not config.REPO_DATA_FILE_EXISTS
                    )
                ):
                    Scraper.set_available_data(record)

                record.status = "URL_VALID"
                record.write_info_file(base_dir=args["default_download_dir"])

            config.NEXT_QUEUE.put(record)  # type: ignore
        except ValueError:
            if (not config.ARGS["available_remote_data"]) and (
                not config.ARGS["update_remote_data"]
            ):
                print(f"Info: Histdata.com does not have: {record.url}")

            record.status = "URL_NO_REPO_DATA"
            record.write_info_file(base_dir=args["default_download_dir"])
        except Exception as err:
            print(f"Unknown Error for URL: {record.url}", err, traceback.format_exc())
            record.delete_info_file()
            raise SystemExit from err
        finally:
            config.CURRENT_QUEUE.task_done()  # type: ignore

    @staticmethod
    def download_zips() -> None:
        pool = ThreadPool(
            Scraper.download_zip,
            config.ARGS,
            "Downloading",
            "ZIPs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]) * 3,
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    @staticmethod
    def download_zip(record: Record, args: dict) -> None:
        try:
            if "URL_VALID" in record.status or (
                args["from_api"]
                and not (
                    os.path.exists(record.data_dir + record.zip_filename)
                    or os.path.exists(record.data_dir + record.csv_filename)
                    or os.path.exists(record.data_dir + record.jay_filename)
                )
            ):

                Scraper.get_zip_file(record, args)
                record.status = (
                    "CSV_ZIP" if record.status == "URL_VALID" else record.status
                )
                record.write_info_file(base_dir=args["default_download_dir"])

            config.NEXT_QUEUE.put(record)  # type: ignore
        except KeyError:
            print(f"Invalid Zip on histdata.com: {record.url}", sys.exc_info())
            record.delete_info_file()
        except Exception:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            config.CURRENT_QUEUE.task_done()  # type: ignore

    @staticmethod
    def request_file(record: Record, args: dict) -> requests.Response:
        post_headers = args["post_headers"].copy()
        post_headers["Referer"] = record.url
        return requests.post(
            "http://www.histdata.com/get.php",
            data={
                "tk": record.data_tk,
                "date": record.data_date,
                "datemonth": record.data_datemonth,
                "platform": record.data_format,
                "timeframe": record.data_timeframe,
                "fxpair": record.data_fxpair,
            },
            headers=post_headers,
            timeout=config.REQUESTS_TIMEOUT,
        )

    @staticmethod
    def write_file(record: Record, content: bytes) -> None:
        zip_path = record.data_dir + record.zip_filename
        with open(zip_path, "wb") as zip_file:
            zip_file.write(content)

    @staticmethod
    def get_page_data(url: str) -> dict:
        request = requests.get(url, timeout=config.REQUESTS_TIMEOUT)

        page_content = BeautifulSoup(request.content, "html.parser")
        encoding = dict(request.headers)["Content-Encoding"]
        bytes_length = dict(request.headers)["Content-Length"]

        return {
            "page_content": page_content,
            "encoding": encoding,
            "bytes_length": bytes_length,
        }

    @staticmethod
    def get_base_url(url: str) -> str:
        parsed_url: ParseResult = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @staticmethod
    def fetch_form_values(page_data: dict, record: Record) -> Record:
        form_page_content = page_data["page_content"]
        form = form_page_content.find_all("form", id="file_down")

        for element in form:
            for value in element:
                if isinstance(value, bs4.element.Tag):
                    match value.get("id"):
                        case "tk":
                            record.data_tk = value.get("value")
                        case "date":
                            record.data_date = value.get("value")
                        case "datemonth":
                            record.data_datemonth = value.get("value")
                        case "platform":
                            record.data_format = value.get("value")
                        case "timeframe":
                            record.data_timeframe = value.get("value")
                        case "fxpair":
                            record.data_fxpair = value.get("value")
        return record

    @staticmethod
    def get_zip_file_name(response: requests.Response) -> str:
        zip_file_name: str = (
            response.headers["Content-Disposition"].split(";")[1].split("=")[1]
        )
        return zip_file_name

    @staticmethod
    def get_zip_file(record: Record, args: dict) -> None:
        res = Scraper.request_file(record, args)
        record.zip_filename = Scraper.get_zip_file_name(res)
        Scraper.write_file(record, res.content)

    @staticmethod
    def set_available_data(record: Record) -> None:
        datemonth = Utils.force_datemonth_if_only_year(record.data_datemonth)
        pair = record.data_fxpair.lower()

        if pair not in config.REPO_DATA:
            config.REPO_DATA[pair] = {"start": datemonth, "end": datemonth}
        else:
            if int(datemonth) < int(config.REPO_DATA[pair]["start"]):
                config.REPO_DATA[pair]["start"] = datemonth
            if int(datemonth) > int(config.REPO_DATA[pair]["end"]):
                config.REPO_DATA[pair]["end"] = datemonth
