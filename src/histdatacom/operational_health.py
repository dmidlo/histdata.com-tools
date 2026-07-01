"""Bounded operational health payloads for human-facing status displays."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from histdatacom.cache_status import collect_cache_run_status
from histdatacom.runtime_contracts import JSONValue, RunRequest
from histdatacom.orchestration.control import OrchestrationJobSnapshot

OPERATIONAL_HEALTH_METADATA_KEY = "operational_health"
RUNTIME_HEALTH_METADATA_KEY = "runtime_health"
DISK_WARNING_PERCENT_USED = 90.0
DISK_WARNING_FREE_BYTES = 5 * 1024**3
GROUP_LIMIT = 8

HealthProvider = Callable[
    [OrchestrationJobSnapshot],
    Mapping[str, Any] | None,
]


def operational_health_provider_for_request(
    request: RunRequest,
) -> HealthProvider:
    """Return a snapshot-aware health provider for live job rendering."""

    def provider(
        snapshot: OrchestrationJobSnapshot,
    ) -> Mapping[str, Any] | None:
        return operational_health_for_request(request, snapshot=snapshot)

    return provider


def attach_operational_health_from_snapshot(
    snapshot: OrchestrationJobSnapshot,
) -> OrchestrationJobSnapshot:
    """Return a snapshot enriched with request-scoped health when available."""
    request_payload = snapshot.metadata.get("run_request")
    if not isinstance(request_payload, Mapping):
        return snapshot
    request = RunRequest.from_dict(request_payload)
    health = operational_health_for_request(request, snapshot=snapshot)
    return replace(
        snapshot,
        metadata={
            **snapshot.metadata,
            OPERATIONAL_HEALTH_METADATA_KEY: health,
        },
    )


def operational_health_for_request(
    request: RunRequest,
    *,
    snapshot: OrchestrationJobSnapshot | None = None,
) -> dict[str, JSONValue]:
    """Return bounded, path-free cache/runtime/disk health for one request."""
    runtime = (
        _mapping(snapshot.metadata.get(RUNTIME_HEALTH_METADATA_KEY))
        if snapshot is not None
        else {}
    )
    job_snapshots = (snapshot.to_dict(),) if snapshot is not None else ()
    status = collect_cache_run_status(
        _health_root(request),
        pairs=request.pairs,
        pair_groups=_pair_groups(request),
        formats=request.formats,
        timeframes=request.timeframes,
        runtime=runtime or None,
        job_snapshots=job_snapshots,
    )
    return _bounded_cache_status_payload(status.to_dict(), runtime=runtime)


def _bounded_cache_status_payload(
    payload: Mapping[str, Any],
    *,
    runtime: Mapping[str, Any],
) -> dict[str, JSONValue]:
    summary = _mapping(payload.get("summary"))
    cleanup = _mapping(payload.get("cleanup"))
    workflows = _mapping(payload.get("workflows"))
    return {
        "status": str(payload.get("status", "") or "unknown"),
        "summary": _summary_payload(summary),
        "cleanup": _cleanup_payload(cleanup),
        "runtime": dict(runtime or _mapping(payload.get("runtime"))),
        "disk": _disk_payload(_mapping(payload.get("disk"))),
        "workflows": _workflow_payload(workflows),
        "groups": [
            _group_payload(group)
            for group in _list_of_mappings(payload.get("groups"))[:GROUP_LIMIT]
        ],
    }


def _summary_payload(summary: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {
        "cache_count": _int_value(summary.get("cache_count")),
        "cache_size_bytes": _int_value(summary.get("cache_size_bytes")),
        "source_artifact_count": _int_value(
            summary.get("source_artifact_count")
        ),
        "source_artifact_size_bytes": _int_value(
            summary.get("source_artifact_size_bytes")
        ),
        "symbol_count": _int_value(summary.get("symbol_count")),
        "symbols_with_cache": _int_value(summary.get("symbols_with_cache")),
        "symbols_with_sources": _int_value(summary.get("symbols_with_sources")),
        "missing_symbol_count": _int_value(summary.get("missing_symbol_count")),
    }


def _cleanup_payload(cleanup: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {
        "state": str(cleanup.get("state", "") or ""),
        "source_artifact_count": _int_value(
            cleanup.get("source_artifact_count")
        ),
        "source_artifact_size_bytes": _int_value(
            cleanup.get("source_artifact_size_bytes")
        ),
        "by_suffix": {
            str(key): _int_value(value)
            for key, value in _mapping(cleanup.get("by_suffix")).items()
        },
    }


def _disk_payload(disk: Mapping[str, Any]) -> dict[str, JSONValue]:
    total = _int_value(disk.get("total_bytes"))
    used = _int_value(disk.get("used_bytes"))
    free = _int_value(disk.get("free_bytes"))
    percent_used = (
        round((used / total) * 100, 1)
        if total > 0
        else _float_value(disk.get("percent_used"))
    )
    raw_state = str(disk.get("state", "") or "unknown")
    state = raw_state
    if total > 0 and (
        percent_used >= DISK_WARNING_PERCENT_USED
        or free < DISK_WARNING_FREE_BYTES
    ):
        state = "warning"
    return {
        "state": state,
        "semantics": "posix_write_available",
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "percent_used": percent_used,
    }


def _workflow_payload(workflows: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {
        "state": str(workflows.get("state", "") or "unknown"),
        "active_count": _int_value(workflows.get("active_count")),
        "job_count": _int_value(workflows.get("job_count")),
    }


def _group_payload(group: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {
        "group": str(group.get("group", "") or ""),
        "status": str(group.get("status", "") or "unknown"),
        "cache_count": _int_value(group.get("cache_count")),
        "source_artifact_count": _int_value(group.get("source_artifact_count")),
        "expected_symbol_count": _int_value(group.get("expected_symbol_count")),
        "symbols_with_cache": _int_value(group.get("symbols_with_cache")),
    }


def _health_root(request: RunRequest) -> str:
    if request.data_quality and request.quality_paths:
        return str(request.quality_paths[0])
    return str(request.data_directory)


def _pair_groups(request: RunRequest) -> tuple[str, ...]:
    value = request.metadata.get("pair_groups")
    if not isinstance(value, (list, tuple, set, frozenset)):
        return ()
    return tuple(str(item) for item in value)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list | tuple):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


__all__ = [
    "OPERATIONAL_HEALTH_METADATA_KEY",
    "RUNTIME_HEALTH_METADATA_KEY",
    "attach_operational_health_from_snapshot",
    "operational_health_for_request",
    "operational_health_provider_for_request",
]
