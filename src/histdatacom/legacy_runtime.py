"""Explicit runtime arguments for legacy helper surfaces."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

DEFAULT_HELPER_ARGS: dict[str, Any] = {
    "available_remote_data": False,
    "update_remote_data": False,
    "by": "pair_asc",
    "validate_urls": False,
    "download_data_archives": False,
    "extract_csvs": False,
    "import_to_influxdb": False,
    "pairs": set(),
    "formats": set(),
    "timeframes": set(),
    "start_yearmonth": "",
    "end_yearmonth": "",
    "default_download_dir": f"data{os.sep}",
    "from_api": False,
    "api_return_type": "polars",
    "batch_size": "5000",
    "delete_after_influx": False,
    "zip_persist": False,
}


def helper_runtime_args(
    *sources: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return explicit helper args merged over conservative defaults."""
    values = dict(DEFAULT_HELPER_ARGS)
    for source in sources:
        if source is not None:
            values.update(dict(source))
    return values
