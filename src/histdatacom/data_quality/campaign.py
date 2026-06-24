"""Data-quality full-dataset campaign planning helpers."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from histdatacom.activity_stages import (
    DEFAULT_REPOSITORY_URL,
    iter_dataset_periods,
    valid_dataset_dimensions,
)
from histdatacom.data_quality.format_support import DEEP_QUALITY_DIMENSIONS
from histdatacom.fx_enums import Format, Timeframe
from histdatacom.runtime_contracts import JSONValue
from histdatacom.utils import get_current_datemonth_gmt_minus5

CAMPAIGN_REPORT_SCHEMA_VERSION = "histdatacom.data-quality-campaign.v1"
DEFAULT_FOLLOW_UP_ISSUES = {
    "cache_deep_validation": 223,
    "installed_quality_smoke": 224,
    "non_ascii_quality_boundary": 225,
    "sidecar_provenance": 229,
}
GIB = 1024**3


def build_full_dataset_campaign_report(
    *,
    issue_number: int,
    repo_data: Mapping[str, Any],
    symbols: Iterable[str],
    data_directory: str,
    disk_total_bytes: int | None = None,
    disk_used_bytes: int | None = None,
    disk_available_bytes: int | None = None,
    minimum_free_bytes: int | None = None,
    current_yearmonth: str | None = None,
    repo_url: str = DEFAULT_REPOSITORY_URL,
    command_lines: Iterable[str] = (),
    observations: Iterable[Mapping[str, Any]] = (),
    follow_up_issues: Mapping[str, int] | None = None,
) -> dict[str, JSONValue]:
    """Return a bounded campaign report for a full-dataset quality batch."""
    normalized_symbols = tuple(str(symbol).lower() for symbol in symbols)
    resolved_current = current_yearmonth or get_current_datemonth_gmt_minus5()
    dimensions = valid_dataset_dimensions(
        Format.list_values(),
        Timeframe.list_keys(),
    )
    rows = [
        _symbol_campaign_row(
            repo_data,
            symbol,
            dimensions=dimensions,
            current_yearmonth=resolved_current,
        )
        for symbol in normalized_symbols
    ]
    missing_symbols = tuple(
        str(row["symbol"]) for row in rows if row.get("repo_status") != "found"
    )
    totals = _campaign_totals(rows)
    disk = _disk_preflight(
        total_bytes=disk_total_bytes,
        used_bytes=disk_used_bytes,
        available_bytes=disk_available_bytes,
        minimum_free_bytes=minimum_free_bytes,
    )
    deferred = _deferred_scope(
        totals, follow_up_issues or DEFAULT_FOLLOW_UP_ISSUES
    )
    status = _campaign_status(
        missing_symbols=missing_symbols,
        disk_status=str(disk["status"]),
        deferred_work_items=_int(totals["deferred_work_item_count"]),
    )
    return {
        "schema_version": CAMPAIGN_REPORT_SCHEMA_VERSION,
        "issue_number": issue_number,
        "status": status,
        "repo": {
            "url": repo_url,
            "pair_count": _repo_pair_count(repo_data),
            "hash": _optional_string(repo_data.get("hash")),
            "hash_utc": _optional_string(repo_data.get("hash_utc")),
        },
        "data_directory": data_directory,
        "current_yearmonth": resolved_current,
        "symbols": _json_value(rows),
        "dimensions": _json_value(
            [
                _dimension_report(csv_format, timeframe)
                for csv_format, timeframe in dimensions
            ]
        ),
        "totals": totals,
        "disk_preflight": disk,
        "missing_symbols": list(missing_symbols),
        "deferred_scope": _json_value(deferred),
        "command_lines": [str(command) for command in command_lines],
        "observations": _json_value(
            [_json_mapping(observation) for observation in observations]
        ),
    }


def _symbol_campaign_row(
    repo_data: Mapping[str, Any],
    symbol: str,
    *,
    dimensions: tuple[tuple[str, str], ...],
    current_yearmonth: str,
) -> dict[str, JSONValue]:
    entry = repo_data.get(symbol)
    if not isinstance(entry, Mapping):
        return {
            "symbol": symbol,
            "repo_status": "missing",
            "work_item_count": 0,
            "deep_quality_work_item_count": 0,
            "deferred_work_item_count": 0,
            "dimensions": [],
        }

    start = str(entry.get("start", "") or "")
    end = str(entry.get("end", "") or "")
    dimension_rows: list[dict[str, JSONValue]] = []
    work_item_count = 0
    deep_quality_count = 0
    deferred_count = 0
    for csv_format, timeframe in dimensions:
        periods = iter_dataset_periods(
            start,
            end,
            timeframe=timeframe,
            current_yearmonth=current_yearmonth,
        )
        period_count = len(periods)
        deep_supported = (csv_format, timeframe) in DEEP_QUALITY_DIMENSIONS
        work_item_count += period_count
        if deep_supported:
            deep_quality_count += period_count
        else:
            deferred_count += period_count
        dimension_rows.append(
            {
                "format": csv_format,
                "timeframe": timeframe,
                "period_count": period_count,
                "deep_quality_supported": deep_supported,
            }
        )

    return {
        "symbol": symbol,
        "repo_status": "found",
        "repo_start": start,
        "repo_end": end,
        "work_item_count": work_item_count,
        "deep_quality_work_item_count": deep_quality_count,
        "deferred_work_item_count": deferred_count,
        "dimensions": _json_value(dimension_rows),
    }


def _campaign_totals(rows: Iterable[Mapping[str, Any]]) -> dict[str, JSONValue]:
    totals: Counter[str] = Counter()
    by_dimension: Counter[tuple[str, str]] = Counter()
    for row in rows:
        totals["symbol_count"] += 1
        if row.get("repo_status") == "found":
            totals["repo_symbol_count"] += 1
        totals["work_item_count"] += _int(row.get("work_item_count"))
        totals["deep_quality_work_item_count"] += _int(
            row.get("deep_quality_work_item_count")
        )
        totals["deferred_work_item_count"] += _int(
            row.get("deferred_work_item_count")
        )
        for dimension in row.get("dimensions", []):
            if not isinstance(dimension, Mapping):
                continue
            by_dimension[
                (
                    str(dimension.get("format", "")),
                    str(dimension.get("timeframe", "")),
                )
            ] += _int(dimension.get("period_count"))

    return {
        "symbol_count": totals["symbol_count"],
        "repo_symbol_count": totals["repo_symbol_count"],
        "work_item_count": totals["work_item_count"],
        "deep_quality_work_item_count": totals["deep_quality_work_item_count"],
        "deferred_work_item_count": totals["deferred_work_item_count"],
        "work_items_by_dimension": _json_value(
            [
                {
                    "format": csv_format,
                    "timeframe": timeframe,
                    "work_item_count": count,
                }
                for (csv_format, timeframe), count in sorted(
                    by_dimension.items()
                )
            ]
        ),
    }


def _dimension_report(csv_format: str, timeframe: str) -> dict[str, JSONValue]:
    deep_supported = (csv_format, timeframe) in DEEP_QUALITY_DIMENSIONS
    return {
        "format": csv_format,
        "timeframe": timeframe,
        "deep_quality_supported": deep_supported,
        "status": "deep-supported" if deep_supported else "deferred",
    }


def _disk_preflight(
    *,
    total_bytes: int | None,
    used_bytes: int | None,
    available_bytes: int | None,
    minimum_free_bytes: int | None,
) -> dict[str, JSONValue]:
    status = "unknown"
    if available_bytes is not None and minimum_free_bytes is not None:
        status = (
            "pass"
            if int(available_bytes) >= int(minimum_free_bytes)
            else "blocked"
        )
    elif available_bytes is not None:
        status = "observed"

    return {
        "status": status,
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "available_bytes": available_bytes,
        "minimum_free_bytes": minimum_free_bytes,
        "available_gib": _bytes_to_gib(available_bytes),
        "minimum_free_gib": _bytes_to_gib(minimum_free_bytes),
    }


def _deferred_scope(
    totals: Mapping[str, JSONValue],
    follow_up_issues: Mapping[str, int],
) -> list[dict[str, JSONValue]]:
    deferred: list[dict[str, JSONValue]] = []
    if _int(totals.get("deferred_work_item_count")):
        deferred.append(
            {
                "scope": "non-ascii and inventory-only format coverage",
                "reason": (
                    "Deep parser-level quality is currently limited to "
                    "ASCII M1 and ASCII tick artifacts."
                ),
                "issue": follow_up_issues.get("non_ascii_quality_boundary"),
            }
        )
    deferred.extend(
        [
            {
                "scope": "canonical cache deep validation",
                "reason": "Cache quality rules are tracked separately.",
                "issue": follow_up_issues.get("cache_deep_validation"),
            },
            {
                "scope": "sidecar provenance validation",
                "reason": "Lineage checks are tracked separately.",
                "issue": follow_up_issues.get("sidecar_provenance"),
            },
        ]
    )
    return deferred


def _campaign_status(
    *,
    missing_symbols: tuple[str, ...],
    disk_status: str,
    deferred_work_items: int,
) -> str:
    if missing_symbols:
        return "failed"
    if disk_status == "blocked":
        return "blocked"
    if deferred_work_items:
        return "deferred"
    if disk_status in {"unknown", "observed"}:
        return "preflighted"
    return "ready"


def _repo_pair_count(repo_data: Mapping[str, Any]) -> int:
    return sum(
        1
        for value in repo_data.values()
        if isinstance(value, Mapping) and "start" in value and "end" in value
    )


def _json_mapping(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return _json_mapping(value)
    return str(value)


def _optional_string(value: Any) -> str:
    return "" if value is None else str(value)


def _bytes_to_gib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(int(value) / GIB, 3)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
