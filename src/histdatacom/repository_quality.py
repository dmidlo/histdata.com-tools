"""Repository quality metadata helpers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from histdatacom.observability import utc_now_iso
from histdatacom.runtime_contracts import JSONValue

REPOSITORY_QUALITY_KEY = "quality"
REPOSITORY_QUALITY_SCHEMA_VERSION = "histdatacom.repo-quality.v1"
_REPOSITORY_METADATA_KEYS = {"hash", "hash_utc"}
_STATUS_ORDER = {"clean": 0, "warning": 1, "failed": 2}
_SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2}


def repository_data_with_quality_payload(
    repo_data: Mapping[str, Any],
    quality_payload: Mapping[str, Any],
    *,
    request_id: str,
    checked_at_utc: str | None = None,
) -> dict[str, Any]:
    """Return repository data updated with bounded quality summaries."""
    updated = _copy_repository_pairs(repo_data)
    checked_at = checked_at_utc or utc_now_iso()
    groups = _target_summaries_by_symbol(quality_payload)

    for symbol, summaries in groups.items():
        entry = dict(updated.get(symbol, {}))
        periods = sorted(
            {
                period
                for summary in summaries
                if (period := _target_value(summary, "period"))
            }
        )
        if "start" not in entry and periods:
            entry["start"] = periods[0]
        if "end" not in entry and periods:
            entry["end"] = periods[-1]
        if "start" not in entry or "end" not in entry:
            continue
        entry[REPOSITORY_QUALITY_KEY] = _quality_summary(
            quality_payload,
            summaries,
            request_id=request_id,
            checked_at_utc=checked_at,
        )
        updated[symbol] = entry

    return updated


def repository_quality_columns(
    pair_entry: Mapping[str, Any],
) -> dict[str, str]:
    """Return display-safe quality columns for a repository row."""
    quality = pair_entry.get(REPOSITORY_QUALITY_KEY)
    if not isinstance(quality, Mapping):
        return {
            "status": "",
            "targets": "",
            "findings": "",
            "report": "",
        }
    artifact = quality.get("report_artifact")
    report = ""
    if isinstance(artifact, Mapping):
        report = str(artifact.get("path", "") or "")
    return {
        "status": str(quality.get("status", "") or ""),
        "targets": str(quality.get("target_count", "") or ""),
        "findings": str(quality.get("finding_count", "") or ""),
        "report": report,
    }


def _copy_repository_pairs(repo_data: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(pair): dict(value)
        for pair, value in repo_data.items()
        if pair not in _REPOSITORY_METADATA_KEYS
        and isinstance(value, Mapping)
        and "start" in value
        and "end" in value
    }


def _target_summaries_by_symbol(
    quality_payload: Mapping[str, Any],
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    summaries = quality_payload.get("target_summaries") or []
    if not isinstance(summaries, list):
        return {}
    for summary in summaries:
        if not isinstance(summary, Mapping):
            continue
        symbol = _target_value(summary, "symbol")
        if symbol:
            grouped[symbol.lower()].append(summary)
    return dict(grouped)


def _quality_summary(
    quality_payload: Mapping[str, Any],
    target_summaries: Iterable[Mapping[str, Any]],
    *,
    request_id: str,
    checked_at_utc: str,
) -> dict[str, Any]:
    summaries = tuple(target_summaries)
    formats = sorted(
        {
            value
            for summary in summaries
            if (value := _target_value(summary, "data_format"))
        }
    )
    timeframes = sorted(
        {
            value
            for summary in summaries
            if (value := _target_value(summary, "timeframe"))
        }
    )
    periods = sorted(
        {
            value
            for summary in summaries
            if (value := _target_value(summary, "period"))
        }
    )
    status_counts = _counts_by_key(summaries, "status")
    finding_count = sum(
        _int(summary.get("finding_count")) for summary in summaries
    )
    info_count = sum(_int(summary.get("info_count")) for summary in summaries)
    warning_count = sum(
        _int(summary.get("warning_count")) for summary in summaries
    )
    error_count = sum(_int(summary.get("error_count")) for summary in summaries)

    return {
        "schema_version": REPOSITORY_QUALITY_SCHEMA_VERSION,
        "checked_at_utc": checked_at_utc,
        "operation": "repo-quality-refresh",
        "request_id": request_id,
        "report_schema_version": str(
            quality_payload.get("report_schema_version", "") or ""
        ),
        "status": _worst_value(status_counts, _STATUS_ORDER, "clean"),
        "max_severity": _worst_max_severity(summaries),
        "check_groups": _string_list(quality_payload.get("check_groups")),
        "target_count": len(summaries),
        "clean_target_count": status_counts.get("clean", 0),
        "warning_target_count": status_counts.get("warning", 0),
        "failed_target_count": status_counts.get("failed", 0),
        "finding_count": finding_count,
        "info_count": info_count,
        "warning_finding_count": warning_count,
        "error_count": error_count,
        "formats": formats,
        "timeframes": timeframes,
        "periods": periods,
        "report_artifact": _mapping_or_none(
            quality_payload.get("report_artifact")
        ),
        "exit_decision": _mapping_or_none(quality_payload.get("exit_decision")),
    }


def _target_value(summary: Mapping[str, Any], key: str) -> str:
    target = summary.get("target")
    if not isinstance(target, Mapping):
        return ""
    return str(target.get(key, "") or "")


def _counts_by_key(
    summaries: Iterable[Mapping[str, Any]],
    key: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for summary in summaries:
        value = str(summary.get(key, "") or "")
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _worst_max_severity(summaries: Iterable[Mapping[str, Any]]) -> str:
    counts = _counts_by_key(summaries, "max_severity")
    return _worst_value(counts, _SEVERITY_ORDER, "info")


def _worst_value(
    counts: Mapping[str, int],
    order: Mapping[str, int],
    default: str,
) -> str:
    if not counts:
        return default
    return max(counts, key=lambda value: order.get(value, -1))


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _mapping_or_none(value: Any) -> dict[str, JSONValue] | None:
    if not isinstance(value, Mapping):
        return None
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)
