"""Check for or generate a list date ranges for instruments available.

Raises:
    SystemExit: Raised when called from CLI

Returns:
    dict: _description_
"""

import contextlib
import json
import ssl
from pathlib import Path
from ssl import SSLCertVerificationError
from typing import TYPE_CHECKING
from urllib.error import URLError
from urllib.request import urlopen

import certifi
from rich import print  # pylint: disable=redefined-builtin
from rich import box
from rich.table import Table

from histdatacom import config
from histdatacom.scraper.scraper import Scraper
from histdatacom.utils import (
    force_datemonth_if_only_year,
    create_full_path,
    hash_dict,
    get_now_utc_timestamp,
    get_year_from_datemonth,
    get_month_from_datemonth,
)

if TYPE_CHECKING:
    from histdatacom.records import Record


class Repo:  # noqa: H601
    """Creates, updates, or retrieves date ranges for histdata.com pairs.

    Attributes:
        repo_url: remote url of pre-generated repository information.
        repo_local_path: local file path for repo data.

    """

    def __init__(self) -> None:
        """Initialize repo class with remote url."""
        self.repo_url = (
            "https://raw.githubusercontent.com/dmidlo/"
            "histdata.com-tools/main/data/.repo"
        )
        self.repo_local_path = Path(  # noqa:BLK100
            config.ARGS["default_download_dir"], ".repo"
        )

    @staticmethod
    def check_if_queue_is_needed() -> bool:
        # pylint: disable=line-too-long
        """Conditions to populate queue for renewing repo data.

        Returns:
            bool: conditonal filter
        """
        return bool(
            (  # noqa:BLK100
                not config.REPO_DATA_FILE_EXISTS
                and config.ARGS["available_remote_data"]
            )
            or config.ARGS["update_remote_data"]
            or config.FILTER_PAIRS
        )

    @staticmethod
    def check_for_repo_action() -> bool:
        """Check to see if -A or -U has been used.

        Returns:
            bool: general truth case for repo actions
        """
        return bool(
            config.ARGS["available_remote_data"]  # noqa:BLK100
            or config.ARGS["update_remote_data"]
        )

    @staticmethod
    def set_repo_datum(record: "Record") -> None:
        """Create and sort individual date ranges for repo.

        Args:
            record (Record): a single downloadable record
                             of pair, year, and month
        """
        datemonth: str = force_datemonth_if_only_year(  # noqa:BLK100
            record.data_datemonth
        )
        pair = record.data_fxpair.lower()

        if pair not in config.REPO_DATA:
            config.REPO_DATA[pair] = {"start": datemonth, "end": datemonth}
        else:
            if int(datemonth) < int(config.REPO_DATA[pair]["start"]):
                config.REPO_DATA[pair]["start"] = datemonth
            if int(datemonth) > int(config.REPO_DATA[pair]["end"]):
                config.REPO_DATA[pair]["end"] = datemonth

    def test_for_repo_data_file(self) -> bool:
        """Test for repo data file and update global boolean.

        Returns:
            bool: existence of repo data file.
        """
        if self.repo_local_path.exists():
            config.REPO_DATA_FILE_EXISTS = True
            return True
        config.REPO_DATA_FILE_EXISTS = False
        return False

    def read_repo_data_file(self) -> None:
        """Read local file repo data. Append/update global working repo data."""
        with self.repo_local_path.open(
            "r", encoding="UTF-8"
        ) as json_read, contextlib.suppress(Exception):
            while True:
                config.REPO_DATA.update(json.load(json_read))

    def update_repo_from_github(self) -> None:
        """Fetch remote repo data.

        Diffs hash of current repo data with remote. If hash is different and
        remote timestamp is more recent than local file timestamp, overwrite
        local data with remote data.
        """
        try:
            with urlopen(  # noqa: S310
                self.repo_url,
                context=ssl.create_default_context(cafile=certifi.where()),
            ) as repo_data:
                remote_repo = json.load(repo_data)

            if config.REPO_DATA_FILE_EXISTS:
                old_hash = config.REPO_DATA["hash"]
                old_time = config.REPO_DATA["hash_utc"]

                remote_hash = remote_repo["hash"]
                remote_time = remote_repo["hash_utc"]

                if old_hash != remote_hash and old_time < remote_time:
                    config.REPO_DATA = remote_repo
            else:
                config.REPO_DATA = remote_repo
                config.REPO_DATA_FILE_EXISTS = True
                self._write_repo_data_file()
        except SSLCertVerificationError:
            print(  # noqa: T201
                """[red]Unable to fetch repo list from github.
                        - Please install certifi package with:
                            pip install certifi`"""  # noqa: W605
            )
        except URLError:
            # pylint: disable=anomalous-backslash-in-string
            print(  # noqa:T201
                """[red]Unable to fetch repo list from github.
                - You can manually update using `-U \[pair(s)]`"""  # noqa:W605
            )

    def get_available_repo_data(self) -> dict | None:
        """Fetch available data based on -p Pairs filter.

        Raises:
            SystemExit: Raises only if called from the cli.

        Returns:
            dict: If called from API, returns dict of the form:
              {'pair': {'start': 'datemonth', 'end': 'datemonth'}
              ...}
        """
        filter_pairs = config.ARGS["pairs"] - set(config.REPO_DATA)
        config.FILTER_PAIRS = None if len(filter_pairs) == 0 else filter_pairs

        if self.check_if_queue_is_needed():
            scraper = Scraper()
            scraper.populate_initial_queue()

        if self._check_for_create_or_update():
            scraper.validate_urls()
            self._write_repo_data_file()

        if config.ARGS["from_api"]:
            return self._sort_repo_dict_by(
                config.REPO_DATA.copy(), config.ARGS["pairs"]
            )

        self._print_repo_data_table()
        raise SystemExit(0)

    def _check_for_create_or_update(self) -> bool:
        """Conditions for creating or updating repo data.

        Returns:
            bool: conditonal filter
        """
        return bool(
            config.ARGS["update_remote_data"]
            or not config.REPO_DATA_FILE_EXISTS
            or config.FILTER_PAIRS
        )

    def _write_repo_data_file(self) -> None:
        """Write repository data file with hash. Create directories if needed."""
        self._hash_repo()

        create_full_path(self.repo_local_path.parent)

        with self.repo_local_path.open("w", encoding="UTF-8") as target:
            json.dump(config.REPO_DATA, target)

    def _hash_repo(self) -> None:
        """Sanitize global data repo and update hash and timestamp."""
        if "hash" in config.REPO_DATA:
            config.REPO_DATA.pop("hash", None)
        if "hash_utc" in config.REPO_DATA:
            config.REPO_DATA.pop("hash_utc", None)
        config.REPO_DATA["hash"] = hash_dict(config.REPO_DATA)
        config.REPO_DATA["hash_utc"] = get_now_utc_timestamp()

    def _sort_repo_dict_by(
        self, repo_dict_copy: dict, filter_pairs: set
    ) -> dict:  # noqa: LN001
        # pylint: disable=line-too-long
        """Sorts the output/return according to argument "--by".

            Pairs (alpha) ascending    - pair_asc
            Pairs (alpha) descending   - pair_dsc
            data start date ascending  - start_asc
            data start date descending - start_dsc

        Args:
            repo_dict_copy (dict): a copy of the global date range repository
            filter_pairs (set): set derived from CLI arg '-p' or API option
                                'pairs'

        Returns:
            dict: returns sorted dict of the form:
              {'pair': {'start': 'datemonth', 'end': 'datemonth'}
              ...}
        """
        filtered_pairs: dict = self._filter_repo_dict_by_pairs(
            repo_dict_copy, filter_pairs
        )

        match config.ARGS["by"]:
            case "pair_asc":
                filtered_pairs = dict(sorted(filtered_pairs.items()))
            case "pair_dsc":
                filtered_pairs = dict(  # noqa:BLK100
                    sorted(filtered_pairs.items(), reverse=True)
                )
            case "start_asc":
                filtered_pairs = dict(
                    sorted(
                        filtered_pairs.items(), key=lambda pair: pair[1]["start"]  # type: ignore # noqa: E501,LN002
                    )
                )
            case "start_dsc":
                filtered_pairs = dict(
                    sorted(
                        filtered_pairs.items(),
                        key=lambda pair: pair[1]["start"],  # type: ignore
                        reverse=True,
                    )
                )
        return filtered_pairs

    def _filter_repo_dict_by_pairs(
        self, repo_dict_copy: dict, filter_pairs: set
    ) -> dict:
        """Filter repo dict data according to single pair values xxxyyy.

        Args:
            repo_dict_copy (dict): a copy of the global date range repository
            filter_pairs (set): set derived from CLI arg '-p' or API option
                                'pairs'

        Returns:
            dict: returns dict created from '-p pairs' set it the form:
              {'pair': {'start': 'datemonth', 'end': 'datemonth'}
              ...}
        """
        filtered: dict = {
            pair: {
                "start": repo_dict_copy[pair]["start"],
                "end": repo_dict_copy[pair]["end"],
            }
            for pair in set(repo_dict_copy) & filter_pairs
        }

        return filtered if filter_pairs else repo_dict_copy

    def _print_repo_data_table(self) -> None:
        """Print filtered repo info to terminal."""
        table = Table(
            title="Data and date ranges available from HistData.com",
            box=box.MARKDOWN,
        )
        table.add_column("Pair -p")
        table.add_column("Start -s")
        table.add_column("End -e")

        for row in self._sort_repo_dict_by(  # pylint: disable=not-an-iterable
            config.REPO_DATA.copy(),
            config.ARGS["pairs"],
        ):
            start = config.REPO_DATA[row]["start"]
            start_year = get_year_from_datemonth(start)
            start_month = get_month_from_datemonth(start)
            end = config.REPO_DATA[row]["end"]
            end_year = get_year_from_datemonth(end)
            end_month = get_month_from_datemonth(end)
            table.add_row(
                row.lower(),
                f"{start_year}-{start_month}",
                f"{end_year}-{end_month}",
            )
        print(table)  # noqa: T201
