"""Scrape histdata.com website for pairs data.

Raises:
    ValueError: On stale download url
    SystemExit: On any undefined error from scraping
"""

import os
from pathlib import Path
from typing import Any, Callable, Iterable, cast

import requests
from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    apply_form_metadata_to_work_item,
    archive_filename_from_response,
    atomic_write_zip_archive,
    download_archive_work_item,
    download_histdata_archive_to_record,
    fetch_histdata_page_data,
    plan_dataset_work_items,
    parse_histdata_form_metadata,
    validate_url_work_item,
)
from histdatacom.observability import ProgressState
from histdatacom.records import Record
from histdatacom.runtime_contracts import WorkItem, WorkStatus
from histdatacom.scraper.urls import Urls


class Scraper:  # noqa:H601
    """Scrape histdata.com website for pairs data.

    Attributes:
        set_repo_datum: static method from scraper.repo.Repo
        check_if_repo_validation_is_needed: static method from scraper.repo.Repo
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
        self.check_if_repo_validation_is_needed: Callable = (
            Repo.check_if_repo_validation_is_needed
        )
        self.check_for_repo_action: Callable = Repo.check_for_repo_action

        # Setup
        self.urls = Urls()
        self._ensure_pairs()

    @classmethod
    def get_zip_file(cls, record: Record) -> None:
        """Download and write zip file to disk.

        Args:
            record (Record): a record to download
        """
        download_histdata_archive_to_record(
            record,
            timeout=config.REQUESTS_TIMEOUT,
            post_headers=config.POST_HEADERS,
        )

    @classmethod
    def _get_zip_file_name(cls, response: requests.Response) -> str:
        """Parse the content-disposition header and return the zip file name.

        Args:
            response (requests.Response): Response

        Returns:
            str: *.zip file name
        """
        return str(archive_filename_from_response(response))

    @classmethod
    def _write_file(cls, record: Record, zip_content: bytes) -> None:
        """Write binary zip data to disk.

        Args:
            record (Record): a record to write
            zip_content (bytes): binary zip data
        """
        atomic_write_zip_archive(
            Path(record.data_dir),
            record.zip_filename,
            zip_content,
            work_id=record.url,
        )

    @classmethod
    def _request_file(cls, record: Record, timeout: int) -> requests.Response:
        """Place a POST request for zip file to http://www.histdata.com/get.php.

        Args:
            record (Record): a record to request
            timeout (int): retry timeout for POST

        Returns:
            requests.Response: zip file response
        """
        post_headers = dict(config.POST_HEADERS)
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

    def plan_initial_records(self) -> list[Record]:
        """Return planned records to be acted on."""
        progress_state = ProgressState(
            stage="dataset_plan",
            total=0.0,
            unit="requests",
            status=WorkStatus.COMPLETED,
        )
        with Progress(
            TextColumn(text_format="[cyan] Generating API Requests"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            progress.add_task("waiting", total=0)

            planned_count = 0
            records: list[Record] = []
            for work_item in plan_dataset_work_items(
                start_yearmonth=config.ARGS["start_yearmonth"],
                end_yearmonth=config.ARGS["end_yearmonth"],
                formats=config.ARGS["formats"],
                pairs=config.FILTER_PAIRS,
                timeframes=config.ARGS["timeframes"],
                default_download_dir=config.ARGS["default_download_dir"],
                base_url=self.urls.base_url,
                zip_persist=bool(config.ARGS["zip_persist"]),
            ):
                planned_count += 1
                progress_state.advance(
                    0.0,
                    message="Planned dataset request.",
                    current=work_item.url,
                    metadata={"planned_count": planned_count},
                )
                record = self._init_record_from_work_item(work_item)

                if record.status is not WorkStatus.URL_NO_REPO_DATA:
                    record.write_memento_file(  # noqa:BLK100
                        base_dir=config.ARGS["default_download_dir"]
                    )
                    if (  # noqa:BLK100
                        self.check_if_repo_validation_is_needed()  # noqa:BLK100
                        and record.status is not WorkStatus.URL_NEW
                    ):
                        self.set_repo_datum(record)
                    records.append(record)

            return records

    def validate_urls(self, records: Iterable[Record]) -> list[Record]:
        """Validate generated URLs and return forwarded records."""
        return [
            output
            for record in records
            if (output := self._validate_url(record, config.ARGS)) is not None
        ]

    def download_zips(self, records: Iterable[Record]) -> list[Record]:
        """Download zip archives and return forwarded records."""
        return [
            output
            for record in records
            if (output := self._download_zip(record, config.ARGS)) is not None
        ]

    def _init_record(self, url: str) -> Record:
        """Create a new record for processing.

        Create a placeholder record, and if the record is already on disk,
        restore it.

        Args:
            url (str): url as primary ID of record.

        Returns:
            record (Record): a record for processing.
        """
        record = Record()
        record(url=url, status=WorkStatus.URL_NEW)
        record.restore_momento(base_dir=config.ARGS["default_download_dir"])
        return record

    def _init_record_from_work_item(self, work_item: WorkItem) -> Record:
        """Create a legacy record from a planned work item."""
        record = Record(**work_item.to_record_kwargs())
        record.restore_momento(base_dir=config.ARGS["default_download_dir"])
        return record

    def _ensure_pairs(self) -> None:
        """Normalize pairs input for planning."""
        if (
            not (
                config.ARGS["update_remote_data"]
                and config.ARGS["available_remote_data"]
            )
            and config.FILTER_PAIRS is None
        ):
            config.FILTER_PAIRS = config.ARGS["pairs"]

    def _validate_url(
        self,
        record: Record,
        args: dict,
    ) -> Record | None:  # noqa:CCR001
        """Scrape url for presence of downloadable zips and related metadata.

        Args:
            record (Record): a record to validate.
            args (dict): a global config.ARGS dict.

        Raises:
            KeyboardInterrupt: User Exit.
        """
        try:
            output = validate_url_work_item(
                WorkItem.from_record(record),
                args=args,
                fetch_page_data=self._get_page_data,
                repo_validation_needed=self.check_if_repo_validation_is_needed,
                set_repo_datum=self.set_repo_datum,
            )
            apply_stage_output_to_record(output, record)
            if (
                output.result.status == WorkStatus.URL_NO_REPO_DATA
                and not self.check_for_repo_action()
            ):
                print(  # noqa:T201,BLK100
                    f"Info: Histdata.com does not have: {record.url}"
                )
            if output.forward:
                return record
            return None
        except KeyboardInterrupt as exc_info:
            print("keyboard from _validate_url.")  # noqa:T201
            raise KeyboardInterrupt from exc_info

    def _scrape_record_info(self, record: Record) -> Record:
        """Scrape page for archive meta data and populate record with info.

        Args:
            record (Record): a Record with a url string in Record.url

        Returns:
            Record: a populated record.
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

    def _download_zip(
        self,
        record: Record,
        args: dict,
    ) -> Record | None:  # noqa:CCR001
        """Download zip from record.url.

        Args:
            record (Record): a record to download.
            args (dict): a global config.ARGS dict.

        Raises:
            KeyboardInterrupt: User Exit.
        """
        try:
            output = download_archive_work_item(
                WorkItem.from_record(record),
                args=args,
            )
            apply_stage_output_to_record(output, record)
            if output.result.failure is not None:
                print(  # noqa:T201
                    f"Invalid Zip on histdata.com: {record.url}",
                    output.result.failure.message,
                )
            if output.forward:
                return record
            return None
        except KeyboardInterrupt as exc_info:
            print("keyboard from _download_zip.")  # noqa:T201
            raise KeyboardInterrupt from exc_info

    def _check_for_existing_archives_on_disk(self, record: Record) -> bool:
        """Check for zip, csv, or cache file.

        Args:
            record (Record): a record to inspect.

        Returns:
            bool: file exists.
        """
        return bool(
            os.path.exists(record.data_dir + record.zip_filename)
            or os.path.exists(record.data_dir + record.csv_filename)
            or os.path.exists(record.data_dir + record.cache_filename)
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
        return cast(
            dict[Any, Any], fetch_histdata_page_data(url, timeout).to_dict()
        )

    def _fetch_form_values(self, page_data: dict, record: Record) -> Record:
        """Get values from page's file download form.

        Args:
            page_data (dict): dict from _get_page_data()
            record (Record): record to update.

        Returns:
            Record: updated record.
        """
        metadata = parse_histdata_form_metadata(page_data)
        work_item = apply_form_metadata_to_work_item(
            WorkItem.from_record(record),
            metadata,
        )
        record(**work_item.to_record_kwargs())
        return record
