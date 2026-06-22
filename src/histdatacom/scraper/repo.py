"""Check for or generate a list date ranges for instruments available.

Raises:
    SystemExit: Raised when called from CLI

Returns:
    dict: _description_
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from rich import print  # pylint: disable=redefined-builtin
from rich import box
from rich.table import Table

from histdatacom import config
from histdatacom.activity_stages import (
    DEFAULT_REPOSITORY_URL,
    fetch_repository_data_from_url,
    filter_repository_data_by_pairs,
    hash_repository_data,
    plan_dataset_work_items,
    read_repository_data_file,
    repository_data_with_record,
    repository_missing_pairs,
    repository_refresh_stage,
    repository_should_create_or_update,
    repository_validation_needed,
    sort_repository_data,
    validate_url_work_item,
    write_repository_data_file,
)
from histdatacom.helper_args import helper_runtime_args
from histdatacom.records import Record
from histdatacom.utils import (
    get_year_from_datemonth,
    get_month_from_datemonth,
)


class Repo:  # noqa: H601
    """Creates, updates, or retrieves date ranges for histdata.com pairs.

    Attributes:
        repo_url: remote url of pre-generated repository information.
        repo_local_path: local file path for repo data.

    """

    def __init__(self, args: Mapping[str, Any] | None = None) -> None:
        """Initialize repo class with remote url."""
        self.args: dict[str, Any] = helper_runtime_args(args)
        self.repo_url = DEFAULT_REPOSITORY_URL
        self.repo_local_path = self._repo_local_path(self.args)

    @staticmethod
    def check_if_repo_validation_is_needed(
        args: Mapping[str, Any],
        *,
        repo_file_exists: bool | None = None,
        filter_pairs: set | None = None,
    ) -> bool:
        # pylint: disable=line-too-long
        """Conditions to validate coverage for renewing repo data.

        Returns:
            bool: conditonal filter
        """
        runtime_args = helper_runtime_args(args)
        return bool(
            repository_validation_needed(
                runtime_args,
                repo_file_exists=(
                    config.REPO_DATA_FILE_EXISTS
                    if repo_file_exists is None
                    else repo_file_exists
                ),
                filter_pairs=(
                    config.FILTER_PAIRS
                    if filter_pairs is None
                    else filter_pairs
                ),
            ),
        )

    @staticmethod
    def check_for_repo_action(args: Mapping[str, Any]) -> bool:
        """Check to see if -A or -U has been used.

        Returns:
            bool: general truth case for repo actions
        """
        runtime_args = helper_runtime_args(args)
        return bool(
            runtime_args["available_remote_data"]
            or runtime_args["update_remote_data"]
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

    def update_repo_from_github(
        self,
        args: Mapping[str, Any] | None = None,
    ) -> None:
        """Fetch remote repo data.

        Diffs hash of current repo data with remote. If hash is different and
        remote timestamp is more recent than local file timestamp, overwrite
        local data with remote data.
        """
        runtime_args = self._runtime_args(args)
        output = repository_refresh_stage(
            repo_data=config.REPO_DATA,
            repo_file_exists=config.REPO_DATA_FILE_EXISTS,
            repo_local_path=self.repo_local_path,
            repo_url=self.repo_url,
            pairs=runtime_args["pairs"],
            by=runtime_args.get("by"),
            available_remote_data=runtime_args["available_remote_data"],
            update_remote_data=runtime_args["update_remote_data"],
            fetch_remote_repository=fetch_repository_data_from_url,
        )
        if output.result.failure is not None:
            self._print_refresh_failure(output.result.failure.code)
            return

        config.REPO_DATA = output.repo_data
        config.REPO_DATA_FILE_EXISTS = output.repo_file_exists

    def get_available_repo_data(
        self,
        args: Mapping[str, Any] | None = None,
    ) -> dict | None:
        """Fetch available data based on -p Pairs filter.

        Raises:
            SystemExit: Raises only if called from the cli.

        Returns:
            dict: If called from API, returns dict of the form:
              {'pair': {'start': 'datemonth', 'end': 'datemonth'}
              ...}
        """
        runtime_args = self._runtime_args(args)
        filter_pairs = repository_missing_pairs(
            config.REPO_DATA,
            runtime_args["pairs"],
        )
        config.FILTER_PAIRS = (
            None if len(filter_pairs) == 0 else set(filter_pairs)
        )

        if self._check_for_create_or_update(runtime_args):
            self._validate_repository_coverage(runtime_args)
            self._write_repo_data_file()

        if runtime_args["from_api"]:
            return self._sort_repo_dict_by(
                config.REPO_DATA.copy(),
                runtime_args["pairs"],
                by=runtime_args.get("by"),
            )

        self._print_repo_data_table(runtime_args)
        raise SystemExit(0)

    def _check_for_create_or_update(
        self,
        args: Mapping[str, Any],
    ) -> bool:
        """Conditions for creating or updating repo data.

        Returns:
            bool: conditonal filter
        """
        runtime_args = helper_runtime_args(args)
        return bool(
            repository_should_create_or_update(
                runtime_args,
                repo_file_exists=config.REPO_DATA_FILE_EXISTS,
                filter_pairs=config.FILTER_PAIRS,
            )
        )

    def _write_repo_data_file(self) -> None:
        """Write repository data file with hash. Create directories if needed."""
        write_repository_data_file(config.REPO_DATA, self.repo_local_path)
        config.REPO_DATA = read_repository_data_file(self.repo_local_path)
        config.REPO_DATA_FILE_EXISTS = True

    def _validate_repository_coverage(
        self,
        args: Mapping[str, Any],
    ) -> None:
        """Validate planned URLs and update repository ranges."""
        runtime_args = helper_runtime_args(args)
        for work_item in plan_dataset_work_items(
            start_yearmonth=runtime_args["start_yearmonth"],
            end_yearmonth=runtime_args["end_yearmonth"],
            formats=runtime_args["formats"],
            pairs=config.FILTER_PAIRS,
            timeframes=runtime_args["timeframes"],
            default_download_dir=runtime_args["default_download_dir"],
            zip_persist=bool(runtime_args["zip_persist"]),
        ):
            output = validate_url_work_item(work_item, args=runtime_args)
            if output.forward:
                self.set_repo_datum(
                    Record(**output.work_item.to_record_kwargs())
                )

    def _hash_repo(self) -> None:
        """Sanitize global data repo and update hash and timestamp."""
        config.REPO_DATA = hash_repository_data(config.REPO_DATA)

    def _sort_repo_dict_by(
        self,
        repo_dict_copy: dict,
        filter_pairs: set,
        *,
        by: str | None = None,
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
        return cast(
            dict[Any, Any],
            sort_repository_data(
                repo_dict_copy,
                filter_pairs,
                by or self.args.get("by"),
            ),
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
        return cast(
            dict[Any, Any],
            filter_repository_data_by_pairs(repo_dict_copy, filter_pairs),
        )

    def _print_repo_data_table(self, args: Mapping[str, Any]) -> None:
        """Print filtered repo info to terminal."""
        runtime_args = helper_runtime_args(args)
        table = Table(
            title="Data and date ranges available from HistData.com",
            box=box.MARKDOWN,
        )
        table.add_column("Pair -p")
        table.add_column("Start -s")
        table.add_column("End -e")

        for row in self._sort_repo_dict_by(  # pylint: disable=not-an-iterable
            config.REPO_DATA.copy(),
            runtime_args["pairs"],
            by=runtime_args.get("by"),
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

    def _runtime_args(
        self,
        args: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime_args: dict[str, Any] = helper_runtime_args(self.args, args)
        self.args = runtime_args
        self.repo_local_path = self._repo_local_path(runtime_args)
        return runtime_args

    @staticmethod
    def _repo_local_path(args: Mapping[str, Any]) -> Path:
        runtime_args = helper_runtime_args(args)
        return Path(str(runtime_args["default_download_dir"]), ".repo")

    def _print_refresh_failure(self, code: str) -> None:
        if code == "REPOSITORY_NETWORK_ERROR":
            print(r"""[red]Unable to fetch repo list from github.
                - You can manually update using `-U \[pair(s)]`""")  # noqa:T201
            return
        print("""[red]Unable to fetch repo list from github.
                        - Please install certifi package with:
                            pip install certifi`""")  # noqa:T201,W605
