"""Data-quality full-dataset campaign planning helpers."""

from __future__ import annotations

import shlex
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

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
CAMPAIGN_PLAN_SCHEMA_VERSION = "histdatacom.data-quality-campaign-plan.v1"
DEFAULT_FOLLOW_UP_ISSUES = {
    "cache_deep_validation": 223,
    "installed_quality_smoke": 224,
    "non_ascii_quality_boundary": 225,
}
DEFAULT_CAMPAIGN_SLICE_SYMBOL_COUNT = 1
DEFAULT_CAMPAIGN_REPORTS_DIRECTORY = ".quality/campaign"
DEFAULT_CAMPAIGN_QUALITY_CHECKS = ("all",)
DEFAULT_CAMPAIGN_CLEANUP_MODE = "none"
CAMPAIGN_CLEANUP_MODES = ("none", "cache", "working-artifacts")
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


def build_storage_backed_campaign_plan(
    *,
    issue_number: int,
    repo_data: Mapping[str, Any],
    symbols: Iterable[str],
    data_directory: str,
    reports_directory: str = DEFAULT_CAMPAIGN_REPORTS_DIRECTORY,
    disk_total_bytes: int | None = None,
    disk_used_bytes: int | None = None,
    disk_available_bytes: int | None = None,
    minimum_free_bytes: int | None = None,
    current_yearmonth: str | None = None,
    repo_url: str = DEFAULT_REPOSITORY_URL,
    formats: Iterable[str] | None = None,
    timeframes: Iterable[str] | None = None,
    slice_symbol_count: int = DEFAULT_CAMPAIGN_SLICE_SYMBOL_COUNT,
    cleanup_mode: str = DEFAULT_CAMPAIGN_CLEANUP_MODE,
    quality_checks: Iterable[str] = DEFAULT_CAMPAIGN_QUALITY_CHECKS,
    platform_executable_bundled: bool | None = None,
) -> dict[str, JSONValue]:
    """Return an executable, bounded full-dataset campaign plan.

    The plan intentionally keeps ordinary repo refresh separate from
    ``--repo-quality``. Each slice downloads/extracts only a bounded
    symbol/format/timeframe surface, writes detailed quality reports, updates
    ``.repo`` with bounded findings, and then performs any explicitly selected
    disk-pressure cleanup.
    """
    normalized_symbols = tuple(str(symbol).lower() for symbol in symbols)
    normalized_cleanup_mode = _cleanup_mode(cleanup_mode)
    normalized_quality_checks = tuple(str(check) for check in quality_checks)
    normalized_slice_size = _positive_int(
        slice_symbol_count,
        default=DEFAULT_CAMPAIGN_SLICE_SYMBOL_COUNT,
    )
    selected_dimensions = _selected_dimensions(
        formats=formats,
        timeframes=timeframes,
    )
    report = build_full_dataset_campaign_report(
        issue_number=issue_number,
        repo_data=repo_data,
        symbols=normalized_symbols,
        data_directory=data_directory,
        disk_total_bytes=disk_total_bytes,
        disk_used_bytes=disk_used_bytes,
        disk_available_bytes=disk_available_bytes,
        minimum_free_bytes=minimum_free_bytes,
        current_yearmonth=current_yearmonth,
        repo_url=repo_url,
    )
    slices = _campaign_execution_slices(
        issue_number=issue_number,
        repo_data=repo_data,
        symbols=normalized_symbols,
        dimensions=selected_dimensions,
        data_directory=data_directory,
        reports_directory=reports_directory,
        current_yearmonth=str(report["current_yearmonth"]),
        slice_symbol_count=normalized_slice_size,
        cleanup_mode=normalized_cleanup_mode,
        quality_checks=normalized_quality_checks,
    )
    return {
        "schema_version": CAMPAIGN_PLAN_SCHEMA_VERSION,
        "issue_number": issue_number,
        "status": _campaign_plan_status(
            report,
            platform_executable_bundled=platform_executable_bundled,
            cleanup_mode=normalized_cleanup_mode,
        ),
        "campaign_report": report,
        "execution_environment": {
            "data_directory": data_directory,
            "reports_directory": reports_directory,
            "requires_storage_backed_data_root": True,
            "requires_bundled_platform_wheel": True,
            "platform_executable_bundled": platform_executable_bundled,
            "source_checkout_sdist_fallback_expected": (
                platform_executable_bundled is False
            ),
            "source_checkout_sdist_fallback_action": (
                "Install a bundled platform wheel or pass "
                "histdatacom-sidecar start --executable /path/to/temporal."
            ),
        },
        "preflight_commands": [
            _shell_command(
                "histdatacom-sidecar",
                "doctor",
                "--json",
            ),
            _shell_command(
                "histdatacom",
                "-U",
                "-p",
                *normalized_symbols,
                "--repo-quality-columns",
                "--data-directory",
                data_directory,
            ),
        ],
        "repo_quality_contract": {
            "required_after_each_slice": True,
            "repo_path": str(Path(data_directory) / ".repo"),
            "preserve_repo_file": True,
            "preserve_quality_reports": True,
            "quality_checks": list(normalized_quality_checks),
            "summary_storage": (
                "Each slice runs --repo-quality so bounded findings are stored "
                "in .repo while detailed findings remain in the JSON report."
            ),
        },
        "cleanup_policy": _cleanup_policy(normalized_cleanup_mode),
        "slice_count": len(slices),
        "slices": _json_value(slices),
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


def _selected_dimensions(
    *,
    formats: Iterable[str] | None,
    timeframes: Iterable[str] | None,
) -> tuple[tuple[str, str], ...]:
    return cast(
        tuple[tuple[str, str], ...],
        valid_dataset_dimensions(
            tuple(formats or sorted(Format.list_values())),
            tuple(timeframes or sorted(Timeframe.list_keys())),
        ),
    )


def _campaign_execution_slices(
    *,
    issue_number: int,
    repo_data: Mapping[str, Any],
    symbols: tuple[str, ...],
    dimensions: tuple[tuple[str, str], ...],
    data_directory: str,
    reports_directory: str,
    current_yearmonth: str,
    slice_symbol_count: int,
    cleanup_mode: str,
    quality_checks: tuple[str, ...],
) -> list[dict[str, JSONValue]]:
    slices: list[dict[str, JSONValue]] = []
    for csv_format, timeframe in dimensions:
        deep_supported = (csv_format, timeframe) in DEEP_QUALITY_DIMENSIONS
        for symbol_group in _symbol_groups(symbols, slice_symbol_count):
            slice_index = len(slices) + 1
            target_paths = [
                _slice_target_path(
                    data_directory,
                    csv_format=csv_format,
                    timeframe=timeframe,
                    symbol=symbol,
                )
                for symbol in symbol_group
            ]
            report_path = _slice_report_path(
                reports_directory,
                issue_number=issue_number,
                slice_index=slice_index,
                csv_format=csv_format,
                timeframe=timeframe,
                symbols=symbol_group,
            )
            work_item_count = sum(
                _dimension_work_item_count(
                    repo_data,
                    symbol=symbol,
                    timeframe=timeframe,
                    current_yearmonth=current_yearmonth,
                )
                for symbol in symbol_group
            )
            slices.append(
                {
                    "slice_id": _slice_id(
                        issue_number=issue_number,
                        slice_index=slice_index,
                        csv_format=csv_format,
                        timeframe=timeframe,
                        symbols=symbol_group,
                    ),
                    "slice_index": slice_index,
                    "symbols": list(symbol_group),
                    "format": csv_format,
                    "timeframe": timeframe,
                    "deep_quality_supported": deep_supported,
                    "work_item_count": work_item_count,
                    "target_paths": target_paths,
                    "quality_report": report_path,
                    "commands": _slice_commands(
                        data_directory=data_directory,
                        csv_format=csv_format,
                        timeframe=timeframe,
                        symbols=symbol_group,
                        target_paths=target_paths,
                        report_path=report_path,
                        cleanup_mode=cleanup_mode,
                        quality_checks=quality_checks,
                    ),
                }
            )
    return slices


def _slice_commands(
    *,
    data_directory: str,
    csv_format: str,
    timeframe: str,
    symbols: tuple[str, ...],
    target_paths: list[str],
    report_path: str,
    cleanup_mode: str,
    quality_checks: tuple[str, ...],
) -> list[dict[str, JSONValue]]:
    commands: list[dict[str, JSONValue]] = [
        {
            "step": "download_extract_slice",
            "command": _shell_command(
                "histdatacom",
                "-D",
                "-X",
                "-p",
                *symbols,
                "-f",
                csv_format,
                "-t",
                _cli_timeframe_arg(timeframe),
                "--data-directory",
                data_directory,
            ),
        },
        {
            "step": "refresh_repo_quality",
            "command": _shell_command(
                "histdatacom",
                "--repo-quality",
                "--quality-target",
                *target_paths,
                "--quality-checks",
                *quality_checks,
                "--quality-report",
                report_path,
                "--data-directory",
                data_directory,
            ),
            "updates_repo": True,
            "repo_path": str(Path(data_directory) / ".repo"),
        },
    ]
    cleanup_command = _cleanup_command(cleanup_mode, target_paths)
    if cleanup_command:
        commands.append(
            {
                "step": "cleanup_after_repo_quality",
                "command": cleanup_command,
                "preserves_repo": True,
                "preserves_quality_reports": True,
            }
        )
    return commands


def _campaign_plan_status(
    report: Mapping[str, JSONValue],
    *,
    platform_executable_bundled: bool | None,
    cleanup_mode: str,
) -> str:
    if report.get("status") == "failed":
        return "failed"
    if platform_executable_bundled is False:
        return "needs-platform-wheel"
    if report.get("status") == "blocked" and cleanup_mode != "none":
        return "slice-cleanup-required"
    if report.get("status") == "blocked":
        return "blocked"
    if platform_executable_bundled is None:
        return "preflight-required"
    return "ready"


def _cleanup_policy(cleanup_mode: str) -> dict[str, JSONValue]:
    match cleanup_mode:
        case "none":
            removes = "nothing"
            command_shape = ""
        case "cache":
            removes = "canonical .data cache files under each slice target"
            command_shape = "find <slice-target> -name .data -type f -delete"
        case "working-artifacts":
            removes = (
                "slice target directories after --repo-quality has written "
                "the detailed report and .repo summary"
            )
            command_shape = "rm -rf <slice-target>"
        case _:
            raise ValueError(f"unknown campaign cleanup mode: {cleanup_mode}")

    return {
        "mode": cleanup_mode,
        "removes": removes,
        "command_shape": command_shape,
        "runs_after_repo_quality": True,
        "preserves_repo_file": True,
        "preserves_quality_reports": True,
    }


def _cleanup_command(cleanup_mode: str, target_paths: list[str]) -> str:
    if cleanup_mode == "none":
        return ""
    if cleanup_mode == "cache":
        return " && ".join(
            _shell_command(
                "find",
                target_path,
                "-name",
                ".data",
                "-type",
                "f",
                "-delete",
            )
            for target_path in target_paths
        )
    if cleanup_mode == "working-artifacts":
        return _shell_command("rm", "-rf", *target_paths)
    raise ValueError(f"unknown campaign cleanup mode: {cleanup_mode}")


def _dimension_work_item_count(
    repo_data: Mapping[str, Any],
    *,
    symbol: str,
    timeframe: str,
    current_yearmonth: str,
) -> int:
    entry = repo_data.get(symbol)
    if not isinstance(entry, Mapping):
        return 0
    return len(
        iter_dataset_periods(
            str(entry.get("start", "") or ""),
            str(entry.get("end", "") or ""),
            timeframe=timeframe,
            current_yearmonth=current_yearmonth,
        )
    )


def _symbol_groups(
    symbols: tuple[str, ...],
    slice_symbol_count: int,
) -> Iterable[tuple[str, ...]]:
    for offset in range(0, len(symbols), slice_symbol_count):
        yield symbols[offset : offset + slice_symbol_count]


def _slice_target_path(
    data_directory: str,
    *,
    csv_format: str,
    timeframe: str,
    symbol: str,
) -> str:
    return str(
        Path(data_directory) / Format(csv_format).name / timeframe / symbol
    )


def _cli_timeframe_arg(timeframe: str) -> str:
    if timeframe in Timeframe.list_keys():
        return str(Timeframe[timeframe].value)
    return str(Timeframe(timeframe).value)


def _slice_report_path(
    reports_directory: str,
    *,
    issue_number: int,
    slice_index: int,
    csv_format: str,
    timeframe: str,
    symbols: tuple[str, ...],
) -> str:
    identifier = _slice_id(
        issue_number=issue_number,
        slice_index=slice_index,
        csv_format=csv_format,
        timeframe=timeframe,
        symbols=symbols,
    )
    filename = f"{identifier}-quality.json"
    return str(Path(reports_directory) / filename)


def _slice_id(
    *,
    issue_number: int,
    slice_index: int,
    csv_format: str,
    timeframe: str,
    symbols: tuple[str, ...],
) -> str:
    return (
        f"issue-{issue_number}-{slice_index:03d}-"
        f"{csv_format}-{timeframe.lower()}-{'-'.join(symbols)}"
    )


def _shell_command(*parts: str) -> str:
    return " ".join(shlex.quote(str(part)) for part in parts)


def _cleanup_mode(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in CAMPAIGN_CLEANUP_MODES:
        msg = (
            "cleanup_mode must be one of "
            f"{', '.join(CAMPAIGN_CLEANUP_MODES)}"
        )
        raise ValueError(msg)
    return normalized


def _positive_int(value: int, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


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
