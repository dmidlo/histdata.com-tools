import os
import sys
import contextlib
import pickle
from urllib.request import urlopen

from urllib.error import URLError

from rich import box
from rich.table import Table
from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config
from histdatacom.utils import Utils
from histdatacom.scraper.scraper import Scraper


class Repo:
    @staticmethod
    def set_repo_url() -> None:
        config.ARGS[
            "repo_url"
        ] = "https://github.com/dmidlo/histdata.com-tools/blob/main/data/.repo?raw=true"

    @staticmethod
    def test_for_repo_data_file() -> bool:
        if os.path.exists(f"{config.ARGS['default_download_dir']}{os.sep}.repo"):
            config.REPO_DATA_FILE_EXISTS = True
            return True
        config.REPO_DATA_FILE_EXISTS = False
        return False

    @staticmethod
    def read_repo_data_file() -> None:
        with open(
            f"{config.ARGS['default_download_dir']}{os.sep}.repo", "rb"
        ) as pickle_read:
            with contextlib.suppress(Exception):
                while True:
                    config.REPO_DATA.update(pickle.load(pickle_read))

    @staticmethod
    def update_repo_from_github() -> None:
        try:
            data = urlopen(config.ARGS["repo_url"])
            remote_repo = pickle.load(data)
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
                Repo.write_repo_data_file()
        except URLError:
            # pylint: disable=anomalous-backslash-in-string
            print(
                """[red]Unable to fetch repo list from github.
                            - You can manually update using `-U \[pair(s)]`"""
            )

    @staticmethod
    def write_repo_data_file() -> None:
        try:
            Repo.hash_repo()

            path = config.ARGS["default_download_dir"]
            Utils.create_full_path(path)

            with open(f"{path}.repo", "wb") as filepath:
                pickle.dump(config.REPO_DATA, filepath)
        except ValueError as err:
            print(err)
            sys.exit()

    @staticmethod
    def hash_repo() -> None:
        if "hash" in config.REPO_DATA:
            del config.REPO_DATA["hash"]
        if "hash_utc" in config.REPO_DATA:
            del config.REPO_DATA["hash_utc"]
        config.REPO_DATA["hash"] = Utils.hash_dict(config.REPO_DATA)
        config.REPO_DATA["hash_utc"] = Utils.get_now_utc_timestamp()

    @staticmethod
    def get_available_repo_data() -> dict | None:
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
                Repo.write_repo_data_file()

            if config.ARGS["from_api"]:
                return Repo.sort_repo_dict_by(
                    config.REPO_DATA.copy(), config.ARGS["pairs"]
                )

            Repo.print_repo_data_table()
            raise SystemExit(0)

        return None

    @staticmethod
    def sort_repo_dict_by(repo_dict_copy: dict, filter_pairs: set) -> dict:  # type: ignore
        filtered_pairs: dict = Repo.filter_repo_dict_by_pairs(
            repo_dict_copy, filter_pairs
        )

        match config.ARGS["by"]:
            case "pair_asc":
                return dict(sorted(filtered_pairs.items()))
            case "pair_dsc":
                return dict(sorted(filtered_pairs.items(), reverse=True))
            case "start_asc":
                return dict(
                    sorted(filtered_pairs.items(), key=lambda pair: pair[1]["start"])  # type: ignore
                )
            case "start_dsc":
                return dict(
                    sorted(
                        filtered_pairs.items(),
                        key=lambda pair: pair[1]["start"],  # type: ignore
                        reverse=True,
                    )
                )

    @staticmethod
    def print_repo_data_table() -> None:
        table = Table(
            title="Data and date ranges available from HistData.com", box=box.MARKDOWN
        )
        table.add_column("Pair -p")
        table.add_column("Start -s")
        table.add_column("End -e")

        for row in Repo.sort_repo_dict_by(  # pylint: disable=not-an-iterable
            config.REPO_DATA.copy(),
            config.ARGS["pairs"],
        ):
            start = config.REPO_DATA[row]["start"]
            end = config.REPO_DATA[row]["end"]
            table.add_row(
                row.lower(),
                f"{Utils.get_year_from_datemonth(start)}-{Utils.get_month_from_datemonth(start)}",
                f"{Utils.get_year_from_datemonth(end)}-{Utils.get_month_from_datemonth(end)}",
            )
        print(table)

    @staticmethod
    def filter_repo_dict_by_pairs(repo_dict_copy: dict, filter_pairs: set) -> dict:
        filtered: dict = {
            x: {"start": repo_dict_copy[x]["start"], "end": repo_dict_copy[x]["end"]}
            for x in set(repo_dict_copy) & filter_pairs
        }

        return filtered if filter_pairs else repo_dict_copy
