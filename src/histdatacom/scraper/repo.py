"""Check for or generate a list date ranges for instruments available.

Raises:
    SystemExit: Raised when called from CLI

Returns:
    dict: _description_
"""

from pathlib import Path
from typing import TYPE_CHECKING
from rich import print  # pylint: disable=redefined-builtin
from rich import box
from rich.table import Table

from histdatacom import config
from histdatacom.activity_stages import (
    DEFAULT_REPOSITORY_URL,
    fetch_repository_data_from_url,
    filter_repository_data_by_pairs,
    hash_repository_data,
    read_repository_data_file,
    repository_data_with_record,
    repository_missing_pairs,
    repository_queue_needed,
    repository_refresh_stage,
    repository_should_create_or_update,
    sort_repository_data,
    write_repository_data_file,
)
from histdatacom.scraper.scraper import Scraper
from histdatacom.utils import (
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
        self.repo_url = DEFAULT_REPOSITORY_URL
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
        return repository_queue_needed(
            config.ARGS,
            repo_file_exists=config.REPO_DATA_FILE_EXISTS,
            filter_pairs=config.FILTER_PAIRS,
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
        config.REPO_DATA = repository_data_with_record(
            config.REPO_DATA,
            record,
        )

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
        config.REPO_DATA.update(read_repository_data_file(self.repo_local_path))

    def update_repo_from_github(self) -> None:
        """Fetch remote repo data.

        Diffs hash of current repo data with remote. If hash is different and
        remote timestamp is more recent than local file timestamp, overwrite
        local data with remote data.
        """
        output = repository_refresh_stage(
            repo_data=config.REPO_DATA,
            repo_file_exists=config.REPO_DATA_FILE_EXISTS,
            repo_local_path=self.repo_local_path,
            repo_url=self.repo_url,
            pairs=config.ARGS["pairs"],
            by=config.ARGS.get("by"),
            available_remote_data=config.ARGS["available_remote_data"],
            update_remote_data=config.ARGS["update_remote_data"],
            fetch_remote_repository=fetch_repository_data_from_url,
        )
        if output.result.failure is not None:
            self._print_refresh_failure(output.result.failure.code)
            return

        config.REPO_DATA = output.repo_data
        config.REPO_DATA_FILE_EXISTS = output.repo_file_exists

    def get_available_repo_data(self) -> dict | None:
        """Fetch available data based on -p Pairs filter.

        Raises:
            SystemExit: Raises only if called from the cli.

        Returns:
            dict: If called from API, returns dict of the form:
              {'pair': {'start': 'datemonth', 'end': 'datemonth'}
              ...}
        """
        filter_pairs = repository_missing_pairs(
            config.REPO_DATA,
            config.ARGS["pairs"],
        )
        config.FILTER_PAIRS = (
            None if len(filter_pairs) == 0 else set(filter_pairs)
        )

        if self.check_if_queue_is_needed():
            scraper = Scraper()
            scraper.populate_initial_queue()

        if self._check_for_create_or_update():
            scraper.validate_urls()
            self._write_repo_data_file()

        if config.ARGS["from_api"]:
            return self._sort_repo_dict_by(
                config.REPO_DATA.copy(),
                config.ARGS["pairs"],
            )

        self._print_repo_data_table()
        raise SystemExit(0)

    def _check_for_create_or_update(self) -> bool:
        """Conditions for creating or updating repo data.

        Returns:
            bool: conditonal filter
        """
        return repository_should_create_or_update(
            config.ARGS,
            repo_file_exists=config.REPO_DATA_FILE_EXISTS,
            filter_pairs=config.FILTER_PAIRS,
        )

    def _write_repo_data_file(self) -> None:
        """Write repository data file with hash. Create directories if needed."""
        write_repository_data_file(config.REPO_DATA, self.repo_local_path)
        config.REPO_DATA = read_repository_data_file(self.repo_local_path)
        config.REPO_DATA_FILE_EXISTS = True

    def _hash_repo(self) -> None:
        """Sanitize global data repo and update hash and timestamp."""
        config.REPO_DATA = hash_repository_data(config.REPO_DATA)

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
        return sort_repository_data(
            repo_dict_copy,
            filter_pairs,
            config.ARGS.get("by"),
        )

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
        return filter_repository_data_by_pairs(repo_dict_copy, filter_pairs)

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

    def _print_refresh_failure(self, code: str) -> None:
        if code == "REPOSITORY_NETWORK_ERROR":
            print(r"""[red]Unable to fetch repo list from github.
                - You can manually update using `-U \[pair(s)]`""")  # noqa:T201
            return
        print("""[red]Unable to fetch repo list from github.
                        - Please install certifi package with:
                            pip install certifi`""")  # noqa:T201,W605
