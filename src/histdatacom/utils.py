import os
import sys
import csv
import re
import hashlib
import json
from datetime import datetime

from typing import Optional
from typing import Union
from typing import Tuple
from typing import Type
from typing import Any
from os import PathLike
from csv import Dialect

import pytz
import yaml

from rich.progress import TextColumn
from rich.progress import BarColumn
from rich.progress import TimeElapsedColumn


class Utils:
    @staticmethod
    def get_month_from_datemonth(datemonth: Optional[str] | Optional[int]) -> str:
        return datemonth[-2:] if datemonth is not None and len(datemonth) > 4 else ""  # type: ignore

    @staticmethod
    def get_year_from_datemonth(datemonth: Optional[str] | Optional[int]) -> str:
        return datemonth[:4] if datemonth is not None else ""  # type: ignore

    @staticmethod
    def force_datemonth_if_only_year(datemonth: str) -> str:
        return f"{datemonth}01" if len(datemonth) == 4 else datemonth

    @staticmethod
    def get_query_string(url: str) -> list[str]:
        return url.split("?")[1].split("/")

    @staticmethod
    def create_full_path(
        path_str: Union[str, bytes, PathLike[str], PathLike[bytes]]
    ) -> None:
        if not os.path.exists(path_str):
            os.makedirs(path_str)

    @staticmethod
    def set_working_data_dir(data_dirname: str) -> str:
        return f"{os.getcwd()}{os.sep}{data_dirname}{os.sep}"

    @classmethod
    def load_influx_yaml(cls) -> dict | Any:

        if os.path.exists("influxdb.yaml"):
            with open("influxdb.yaml", "r", encoding="utf-8") as file:
                try:
                    yaml_file = yaml.safe_load(file)
                except yaml.YAMLError as exc:
                    raise SystemExit from exc

            return yaml_file

        print(
            """ ERROR: -I flag is used to import data to a influxdb instance...
                          there is no influxdb.yaml file in working directory.
                          did you forget to set it up?
              """
        )
        sys.exit()

    @staticmethod
    def get_current_datemonth_gmt_minus5() -> str:
        now: datetime = datetime.now().astimezone()
        gmt_minus5: datetime = now.astimezone(pytz.timezone("Etc/GMT-5"))
        return f"{gmt_minus5.year}{gmt_minus5.strftime('%m')}"

    @staticmethod
    def get_progress_bar(
        progress_string: str,
    ) -> Tuple[TextColumn, BarColumn, str, TimeElapsedColumn]:

        return (
            TextColumn(text_format=progress_string),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        )

    @staticmethod
    def get_csv_dialect(csv_path: str) -> Type[Dialect]:
        with open(csv_path, "r", encoding="utf-8") as srccsv:
            dialect = csv.Sniffer().sniff(srccsv.read(), delimiters=",; ")
        return dialect

    @staticmethod
    def replace_date_punct(datemonth_str: Optional[str]) -> str:
        """removes year-month punctuation and returns str("000000")"""
        return re.sub("[-_.: ]", "", datemonth_str) if datemonth_str is not None else ""

    @staticmethod
    def hash_dict(data_dict: dict) -> str:
        dict_hash = hashlib.md5()
        encoded = json.dumps(data_dict, sort_keys=True).encode()
        dict_hash.update(encoded)
        return dict_hash.hexdigest()

    @staticmethod
    def get_now_utc_timestamp() -> float:
        return datetime.utcnow().timestamp()
