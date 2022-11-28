"""Check for or generate a list date ranges for instruments available.

Raises:
    SystemExit: _description_

Returns:
    _type_: _description_
"""
import os
import contextlib
import json
from urllib.request import urlopen
from pathlib import Path
import ssl
import certifi

from urllib.error import URLError
from ssl import SSLCertVerificationError

from rich import box
from rich.table import Table
from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config
from histdatacom.utils import Utils
from histdatacom.scraper.scraper import Scraper


class Repo:
    def __init__(self) -> None:
        self.repo_url = (
            "https://raw.githubusercontent.com/dmidlo/histdata.com-tools/9-implement-testing/data/.repo"
        )

    def test_for_repo_data_file(self) -> bool:
        if os.path.exists(f"{config.ARGS['default_download_dir']}{os.sep}.repo"):
            config.REPO_DATA_FILE_EXISTS = True
            return True
        config.REPO_DATA_FILE_EXISTS = False
        return False

    def read_repo_data_file(self) -> None:
        with open(
            f"{config.ARGS['default_download_dir']}{os.sep}.repo", "rb"
        ) as json_read:
            with contextlib.suppress(Exception):
                while True:
                    config.REPO_DATA.update(json.load(json_read))

    def update_repo_from_github(self) -> None:
        try:
            data = urlopen(
                self.repo_url,
                context=ssl.create_default_context(cafile=certifi.where()),
            )
            remote_repo = json.load(data)

            print(remote_repo)

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
            print(
                """[red]Unable to fetch repo list from github.
                        - Please install certifi package with:
                            pip install certifi`"""  # noqa: W605
            )
        except URLError:
            # pylint: disable=anomalous-backslash-in-string
            print(
                """[red]Unable to fetch repo list from github.
                        - You can manually update using `-U \[pair(s)]`"""  # noqa: W605
            )

    def _write_repo_data_file(self) -> None:
        self._hash_repo()

        path = Path(config.ARGS["default_download_dir"])
        Utils.create_full_path(path)

        file_path = path / ".repo"

        with file_path.open("w", encoding="UTF-8") as target:
            json.dump(config.REPO_DATA, target)

    def _hash_repo(self) -> None:
        if "hash" in config.REPO_DATA:
            del config.REPO_DATA["hash"]
        if "hash_utc" in config.REPO_DATA:
            del config.REPO_DATA["hash_utc"]
        config.REPO_DATA["hash"] = Utils.hash_dict(config.REPO_DATA)
        config.REPO_DATA["hash_utc"] = Utils.get_now_utc_timestamp()

    def get_available_repo_data(self) -> dict | None:
        filter_pairs = config.ARGS["pairs"] - set(config.REPO_DATA)
        config.FILTER_PAIRS = None if len(filter_pairs) == 0 else filter_pairs

        if (
            (not config.REPO_DATA_FILE_EXISTS and config.ARGS["available_remote_data"])
            or config.ARGS["update_remote_data"]
            or config.FILTER_PAIRS
        ):
            Scraper.populate_initial_queue()

        if config.ARGS["available_remote_data"] or config.ARGS["update_remote_data"]:
            if (
                config.ARGS["update_remote_data"]
                or not config.REPO_DATA_FILE_EXISTS
                or config.FILTER_PAIRS
            ):
                Scraper.validate_urls()
                self._write_repo_data_file()

            if config.ARGS["from_api"]:
                return self._sort_repo_dict_by(
                    config.REPO_DATA.copy(), config.ARGS["pairs"]
                )

            self._print_repo_data_table()
            raise SystemExit(0)

        return None

    def _sort_repo_dict_by(self, repo_dict_copy: dict, filter_pairs: set) -> dict:
        filtered_pairs: dict = self._filter_repo_dict_by_pairs(
            repo_dict_copy, filter_pairs
        )

        match config.ARGS["by"]:
            case "pair_asc":
                return dict(sorted(filtered_pairs.items()))
            case "pair_dsc":
                return dict(sorted(filtered_pairs.items(), reverse=True))
            case "start_asc":
                return dict(
                    sorted(
                        filtered_pairs.items(), key=lambda pair: pair[1]["start"]  # type: ignore # noqa: E501
                    )
                )
            case "start_dsc":
                return dict(
                    sorted(
                        filtered_pairs.items(),
                        key=lambda pair: pair[1]["start"],  # type: ignore
                        reverse=True,
                    )
                )
            case _:
                return filtered_pairs

    def _print_repo_data_table(self) -> None:
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
            start_year = Utils.get_year_from_datemonth(start)
            start_month = Utils.get_month_from_datemonth(start)
            end = config.REPO_DATA[row]["end"]
            end_year = Utils.get_year_from_datemonth(end)
            end_month = Utils.get_month_from_datemonth(end)
            table.add_row(
                row.lower(),
                f"{start_year}-{start_month}",
                f"{end_year}-{end_month}",
            )
        print(table)

    def _filter_repo_dict_by_pairs(
        self, repo_dict_copy: dict, filter_pairs: set
    ) -> dict:
        filtered: dict = {
            x: {
                "start": repo_dict_copy[x]["start"],
                "end": repo_dict_copy[x]["end"],
            }
            for x in set(repo_dict_copy) & filter_pairs
        }

        return filtered if filter_pairs else repo_dict_copy
