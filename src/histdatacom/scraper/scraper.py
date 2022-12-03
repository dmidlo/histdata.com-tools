"""Scrape histdata.com website for pairs data.

Raises:
    ValueError: On stale download url
    SystemExit: On any undefined error from scraping
"""

import os
import sys
import traceback
from pathlib import Path
from typing import Callable

import bs4
import requests
from bs4 import BeautifulSoup
from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.concurrency import ThreadPool, get_pool_cpu_count
from histdatacom.records import Record
from histdatacom.scraper.urls import Urls


class Scraper:  # noqa:H601
    """Scrape histdata.com website for pairs data.

    Attributes:
        set_repo_datum: static method from scraper.repo.Repo
        check_if_queue_is_needed: static method from scraper.repo.Repo
        check_for_repo_action: static method from scraper.repo.Repo

    Raises:
        ValueError: On stale download url
        SystemExit: On any undefined error from scraping
    """

    def __init__(self) -> None:
        """Initialize parameters for requests."""
        # pylint: disable-next=import-outside-toplevel
        from histdatacom.scraper.repo import Repo  # noqa:WPS131

        self.set_repo_datum: Callable = Repo.set_repo_datum
        self.check_if_queue_is_needed: Callable = Repo.check_if_queue_is_needed
        self.check_for_repo_action: Callable = Repo.check_for_repo_action

        # Setup
        self.urls = Urls()
        self._ensure_pairs()

    @classmethod
    def get_zip_file(cls, record: Record) -> None:
        """Download and write zip file to disk.

        Args:
            record (Record): a record from the work queue
        """
        res = cls._request_file(record, config.REQUESTS_TIMEOUT)
        record.zip_filename = cls._get_zip_file_name(res)
        cls._write_file(record, res.content)

    @classmethod
    def _get_zip_file_name(cls, response: requests.Response) -> str:
        """Parse the content-disposition header and return the zip file name.

        Args:
            response (requests.Response): Response

        Returns:
            str: *.zip file name
        """
        content_disposition_header = response.headers["Content-Disposition"]
        return str(content_disposition_header.split(";")[1].split("=")[1])

    @classmethod
    def _write_file(cls, record: Record, zip_content: bytes) -> None:
        """Write binary zip data to disk.

        Args:
            record (Record): a record from the work queue.
            zip_content (bytes): binary zip data
        """
        zip_path = Path(record.data_dir, record.zip_filename)
        zip_path.write_bytes(zip_content)

    @classmethod
    def _request_file(cls, record: Record, timeout: int) -> requests.Response:
        """Place a POST request for zip file to http://www.histdata.com/get.php.

        Args:
            record (Record): a record from the work queue
            timeout (int): retry timeout for POST

        Returns:
            requests.Response: zip file response
        """
        post_headers = config.POST_HEADERS
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
            timeout=timeout,
        )

    def populate_initial_queue(self) -> None:
        """Fill Current Queue with records to be acted on."""
        with Progress(
            TextColumn(text_format="[cyan] Generating API Requests"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            progress.add_task("waiting", total=0)

            for url in self.urls.generate_form_urls(
                config.ARGS["start_yearmonth"],
                config.ARGS["end_yearmonth"],
                config.ARGS["formats"],
                config.FILTER_PAIRS,
                config.ARGS["timeframes"],
            ):
                record = self._init_record(url)

                if record.status != "URL_NO_REPO_DATA":
                    record.write_memento_file(  # noqa:BLK100
                        base_dir=config.ARGS["default_download_dir"]
                    )
                    if (  # noqa:BLK100
                        self.check_if_queue_is_needed()  # noqa:BLK100
                        and record.status != "URL_NEW"
                    ):
                        self.set_repo_datum(record)
                    config.NEXT_QUEUE.put(record)  # type: ignore

            config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    def validate_urls(self) -> None:
        """Initialize and Execute a thread pool to validate generated URLs."""
        pool = ThreadPool(
            self._validate_url,
            config.ARGS,
            "Validating",
            "URLs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]) * 3,
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def download_zips(self) -> None:
        """Initialize and Execute a thread pool to download zip archives."""
        pool = ThreadPool(
            self._download_zip,
            config.ARGS,
            "Downloading",
            "ZIPs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]) * 3,
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def _init_record(self, url: str) -> Record:
        """Create a new record for processing.

        Create a placeholder record, and if the record is already on disk,
        restore it.

        Args:
            url (str): url as primary ID of record.

        Returns:
            record (Record): a record for the work queue.
        """
        record = Record()
        record(url=url, status="URL_NEW")
        record.restore_momento(base_dir=config.ARGS["default_download_dir"])
        return record

    def _ensure_pairs(self) -> None:
        """Normalize pairs input for initial queue."""
        if (
            not (
                config.ARGS["update_remote_data"]
                and config.ARGS["available_remote_data"]
            )
            and config.FILTER_PAIRS is None
        ):
            config.FILTER_PAIRS = config.ARGS["pairs"]

    def _validate_url(self, record: Record, args: dict) -> None:
        """Scrape url for the presence of downloadable zips and related metadata.

        executed by the validate_urls thread pool.

        Args:
            record (Record): a record from the work queue.
            args (dict): a global config.ARGS dict.

        Raises:
            SystemExit: On any undefined error from scraping
        """
        try:
            if record.status == "URL_NEW":
                record = self._scrape_record_info(record)
                self._check_for_valid_download(record)

                if self.check_if_queue_is_needed():
                    self.set_repo_datum(record)

                record.status = "URL_VALID"
                record.write_memento_file(base_dir=args["default_download_dir"])

            config.NEXT_QUEUE.put(record)  # type: ignore
        except ValueError:
            if not self.check_for_repo_action():
                print(  # noqa:T201,BLK100
                    f"Info: Histdata.com does not have: {record.url}"
                )

            record.status = "URL_NO_REPO_DATA"
            record.write_memento_file(base_dir=args["default_download_dir"])
        except Exception as err:
            print(  # noqa:T201
                f"Unknown Error for URL: {record.url}",
                err,
                traceback.format_exc(),
            )
            record.delete_momento_file()
            raise SystemExit from err
        finally:
            config.CURRENT_QUEUE.task_done()  # type: ignore

    def _scrape_record_info(self, record: Record) -> Record:
        """Scrape page for archive meta data and populate record with info.

        Args:
            record (Record): a Record with a url string in Record.url

        Returns:
            Record: a record for the work queue.
        """
        page_data: dict = self._get_page_data(  # noqa:BLK001
            record.url, config.REQUESTS_TIMEOUT
        )
        self._fetch_form_values(page_data, record)
        return record

    def _check_for_valid_download(self, record: Record) -> None:
        """Check for the existence of the tk (token) key.

        Args:
            record (Record): a record with page metadata

        Raises:
            ValueError: If the token (tk) has no data.
        """
        if record.data_tk == "":
            raise ValueError

    def _download_zip(self, record: Record, args: dict) -> None:
        """Download zip from record.url.

        Executed by the download_zips thread pool.

        Args:
            record (Record): a record from the queue.
            args (dict): a global config.ARGS dict.

        Raises:
            KeyError: Invalid Zip from remote. # noqa:DAR402
            Exception: Unknown error.
        """
        try:
            if "URL_VALID" in record.status or (
                args["from_api"]
                and not self._check_for_existing_archives_on_disk(record)
            ):

                self.get_zip_file(record)
                record.status = (
                    "CSV_ZIP" if record.status == "URL_VALID" else record.status
                )
                record.write_memento_file(base_dir=args["default_download_dir"])

            config.NEXT_QUEUE.put(record)  # type: ignore
        except KeyError:
            print(  # noqa:T201
                f"Invalid Zip on histdata.com: {record.url}",
                sys.exc_info(),
            )
            record.delete_momento_file()
        except Exception:
            print("Unexpected error:", sys.exc_info())  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            config.CURRENT_QUEUE.task_done()  # type: ignore

    def _check_for_existing_archives_on_disk(self, record: Record) -> bool:
        """Check for zip, csv, or jay file.

        Args:
            record (Record): a record from the work queue.

        Returns:
            bool: file exists.
        """
        return bool(
            os.path.exists(record.data_dir + record.zip_filename)
            or os.path.exists(record.data_dir + record.csv_filename)
            or os.path.exists(record.data_dir + record.jay_filename)
        )

    def _get_page_data(self, url: str, timeout: int) -> dict:
        """Get the whole page's html.

        Args:
            url (str): url of the archive record.
            timeout (int): requests' timeout

        Returns:
            dict: of the form:
                {"page_content": html_content,
                 "encoding": encoding,
                 "bytes_length: bytes_length}
        """
        request = requests.get(url, timeout=timeout)

        page_content = BeautifulSoup(request.content, "html.parser")
        encoding = dict(request.headers)["Content-Encoding"]
        bytes_length = dict(request.headers)["Content-Length"]

        return {
            "page_content": page_content,
            "encoding": encoding,
            "bytes_length": bytes_length,
        }

    def _fetch_form_values(self, page_data: dict, record: Record) -> Record:
        """Get values from page's file download form.

        Args:
            page_data (dict): dict from _get_page_data()
            record (Record): record from the work queue.

        Returns:
            Record: record for the work queue.
        """
        form_page_content = page_data["page_content"]
        form = form_page_content.find_all("form", id="file_down")

        for element in form:
            for form_value in element:
                if isinstance(form_value, bs4.element.Tag):
                    match form_value.get("id"):
                        case "tk":
                            record.data_tk = form_value.get("value")
                        case "date":
                            record.data_date = form_value.get("value")
                        case "datemonth":
                            record.data_datemonth = form_value.get("value")
                        case "platform":
                            record.data_format = form_value.get("value")
                        case "timeframe":
                            record.data_timeframe = form_value.get("value")
                        case "fxpair":
                            record.data_fxpair = form_value.get("value")
        return record
