"""Utilities for histdatacom."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
from datetime import datetime
from typing import Any, Optional

import pytz
import yaml


def get_month_from_datemonth(
    datemonth: str | int,
) -> str:
    """Extract month from datemonth string.

    Args:
        datemonth (Optional[str] | Optional[int]): YYYYMM

    Returns:
        str: MM
    """
    return (
        datemonth[-2:]  # type: ignore
        if datemonth is not None and len(datemonth) > 4  # type: ignore
        else ""
    )  # noqa: E501


def get_year_from_datemonth(
    datemonth: Optional[str] | Optional[int],
) -> str:
    """Extract year from datemonth string.

    Args:
        datemonth (Optional[str] | Optional[int]): YYYYMM

    Returns:
        str: YYYY
    """
    return datemonth[:4] if datemonth is not None else ""  # type: ignore


def force_datemonth_if_only_year(datemonth: str) -> str:
    """Rewrite datemonth if no month provided.

    Args:
        datemonth (str): YYYY

    Returns:
        str: YYYYMM
    """
    return f"{datemonth}01" if len(datemonth) == 4 else datemonth


def get_query_string(url: str) -> list[str]:
    """Extract query string from histdata.com archive urls.

    Args:
        url (str): archive url.

    Returns:
        list[str]: query string, url after '?'
    """
    return url.split("?")[1].split("/")


def create_full_path(path_str: str | Path) -> None:
    """Check if path exists. If not, create it.

    Args:
        path_str (str): path
    """
    path = Path(path_str)
    if not path.exists():
        path.mkdir(parents=True)


def set_working_data_dir(data_dir_name: str) -> str:
    """Create current working directory str.

    Args:
        data_dir_name (str): dir string

    Returns:
        str: working dir string
    """
    return f"{Path.cwd()}{os.sep}{data_dir_name}{os.sep}"


def load_influx_yaml() -> dict | Any:
    """Load settings from influx.yaml.

    # noqa: DAR402

    Raises:
        yaml.YAMLError: malformed yaml file.
        SystemExit: Exit on error

    Returns:
        yaml_file (dict): influx.yaml as dict
    """
    yaml_path = Path(".", "influxdb.yaml")

    if yaml_path.exists():
        with yaml_path.open("r", encoding="UTF-8") as file:
            try:
                yaml_file = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                raise SystemExit from exc

        return yaml_file

    print(  # noqa:T201
        """ ERROR: -I flag is used to import data to a influxdb instance...
                        there is no influxdb.yaml file in working directory.
                        did you forget to set it up?
            """
    )
    raise SystemExit


def get_current_datemonth_gmt_minus5() -> str:
    """Adjust the current date month to ESTnoDST.

    Returns:
        str: YYYYMM
    """
    now: datetime = datetime.now().astimezone()
    gmt_minus5: datetime = now.astimezone(pytz.timezone("Etc/GMT-5"))
    return f"{gmt_minus5.year}{gmt_minus5.strftime('%m')}"


def replace_date_punct(datemonth_str: Optional[str]) -> str:
    """Remove year-month punctuation and returns str("000000").

    Args:
        datemonth_str (Optional[str]): YYYYMM

    Returns:
        str: YYYYMM
    """
    return (  # noqa:BLK100
        re.sub("[-_.: ]", "", datemonth_str)  # noqa:BLK100
        if datemonth_str is not None
        else ""
    )


def hash_dict(data_dict: dict) -> str:
    """Generate a MD5 identity hash for a dict.

    Args:
        data_dict (dict): dict to hash

    Returns:
        str: MD5 identity hash
    """
    dict_hash = hashlib.md5(usedforsecurity=False)
    encoded = json.dumps(data_dict, sort_keys=True).encode()
    dict_hash.update(encoded)
    return dict_hash.hexdigest()


def get_now_utc_timestamp() -> float:
    """Get the current timestamp in UTC.

    Returns:
        float: UTC timestamp
    """
    return datetime.utcnow().timestamp()  # sourcery skip
