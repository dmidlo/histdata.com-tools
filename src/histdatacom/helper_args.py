"""Explicit argument defaults for API and helper surfaces."""

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
    "build_cache": False,
    "import_to_influxdb": False,
    "pair_groups": set(),
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
    "data_quality": False,
    "quality_paths": (),
    "quality_check_groups": {"all"},
    "quality_report_path": "",
    "quality_fail_on": "error",
    "quality_max_errors": 0,
    "quality_max_warnings": 0,
    "repo_quality_refresh": False,
    "repo_quality_columns": False,
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
