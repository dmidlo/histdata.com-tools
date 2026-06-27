"""Cache-run status helpers for local HistData artifacts."""

from __future__ import annotations

import shutil
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from histdatacom.fx_enums import (
    Format,
    PAIR_GROUPS,
    Timeframe,
    expand_pair_selection,
    normalize_pair_group,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.source_cleanup import (
    TRANSIENT_SOURCE_SUFFIXES,
    find_transient_source_artifacts,
)

CACHE_STATUS_SCHEMA_VERSION = 1
_ACTIVE_LIFECYCLES = {
    "submitted",
    "running",
    "cancel_requested",
    "retry_requested",
    "retrying",
    "resume_requested",
    "resuming",
    "unknown",
}
_TERMINAL_LIFECYCLES = {"succeeded", "failed", "cancelled"}


@dataclass(frozen=True, slots=True)
class CacheRunStatusResult:
    """Serializable cache-run status for CLI, API, and operators."""

    root: str
    status: str
    filters: dict[str, Any]
    summary: dict[str, Any]
    disk: dict[str, Any]
    cleanup: dict[str, Any]
    runtime: dict[str, Any]
    workflows: dict[str, Any]
    groups: tuple[dict[str, Any], ...] = ()
    symbols: tuple[dict[str, Any], ...] = ()
    next_steps: tuple[str, ...] = ()
    errors: tuple[dict[str, str], ...] = ()
    schema_version: int = CACHE_STATUS_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable payload."""
        return {
            "schema_version": self.schema_version,
            "root": self.root,
            "status": self.status,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "disk": dict(self.disk),
            "cleanup": dict(self.cleanup),
            "runtime": dict(self.runtime),
            "workflows": dict(self.workflows),
            "groups": [dict(group) for group in self.groups],
            "symbols": [dict(symbol) for symbol in self.symbols],
            "next_steps": list(self.next_steps),
            "errors": [dict(error) for error in self.errors],
        }


@dataclass(frozen=True, slots=True)
class _Artifact:
    path: Path
    size_bytes: int
    file_format: str
    timeframe: str
    symbol: str


@dataclass(slots=True)
class _SymbolAccumulator:
    symbol: str
    cache_count: int = 0
    cache_size_bytes: int = 0
    source_artifact_count: int = 0
    source_artifact_size_bytes: int = 0
    formats: set[str] = field(default_factory=set)
    timeframes: set[str] = field(default_factory=set)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible symbol status."""
        status = _file_status(
            cache_count=self.cache_count,
            source_artifact_count=self.source_artifact_count,
            missing_symbols=(),
            active_workflows=0,
            runtime_state="",
        )
        return {
            "symbol": self.symbol,
            "status": status,
            "cache_count": self.cache_count,
            "cache_size_bytes": self.cache_size_bytes,
            "source_artifact_count": self.source_artifact_count,
            "source_artifact_size_bytes": self.source_artifact_size_bytes,
            "formats": sorted(self.formats),
            "timeframes": sorted(self.timeframes),
        }


def collect_cache_run_status(
    root: str | Path,
    *,
    pairs: Iterable[object] | None = None,
    pair_groups: Iterable[object] | None = None,
    timeframes: Iterable[object] | None = None,
    formats: Iterable[object] | None = None,
    runtime: Mapping[str, Any] | None = None,
    job_snapshots: Iterable[Mapping[str, Any]] | None = None,
    workflow_store_path: str = "",
    max_jobs: int = 5,
) -> CacheRunStatusResult:
    """Collect one cache-run status payload without live Temporal queries."""
    root_path = Path(root).expanduser()
    resolved_root = root_path.resolve(strict=False)
    normalized_groups = _normalize_groups(pair_groups)
    selected_pairs = _selected_pairs(pairs, normalized_groups)
    selected_timeframes = _normalize_timeframes(timeframes)
    selected_formats = _normalize_formats(formats)
    cache_artifacts = _selected_artifacts(
        _cache_artifacts(root_path),
        pairs=selected_pairs,
        timeframes=selected_timeframes,
        formats=selected_formats,
    )
    source_artifacts = _selected_artifacts(
        _source_artifacts(root_path),
        pairs=selected_pairs,
        timeframes=selected_timeframes,
        formats=selected_formats,
    )
    symbols = _symbol_payloads(
        cache_artifacts=cache_artifacts,
        source_artifacts=source_artifacts,
        selected_pairs=selected_pairs,
    )
    group_payloads = _group_payloads(
        groups=normalized_groups,
        symbols=symbols,
    )
    workflows = _workflow_payload(
        job_snapshots,
        workflow_store_path=workflow_store_path,
        max_jobs=max_jobs,
    )
    runtime_payload = _runtime_payload(runtime)
    missing_symbols = _missing_symbols(symbols)
    status = _overall_status(
        cache_count=len(cache_artifacts),
        source_artifact_count=len(source_artifacts),
        missing_symbols=missing_symbols,
        runtime_state=str(runtime_payload.get("state", "")),
        active_workflows=int(workflows.get("active_count", 0) or 0),
    )
    cleanup = _cleanup_payload(source_artifacts)
    summary = {
        "cache_count": len(cache_artifacts),
        "cache_size_bytes": sum(item.size_bytes for item in cache_artifacts),
        "source_artifact_count": len(source_artifacts),
        "source_artifact_size_bytes": sum(
            item.size_bytes for item in source_artifacts
        ),
        "symbol_count": len(symbols),
        "symbols_with_cache": sum(
            1 for symbol in symbols if int(symbol["cache_count"]) > 0
        ),
        "symbols_with_sources": sum(
            1 for symbol in symbols if int(symbol["source_artifact_count"]) > 0
        ),
        "missing_symbol_count": len(missing_symbols),
    }
    filters: dict[str, Any] = {
        "pairs": list(selected_pairs),
        "pair_groups": list(normalized_groups),
        "timeframes": list(selected_timeframes),
        "formats": list(selected_formats),
    }

    return CacheRunStatusResult(
        root=str(resolved_root),
        status=status,
        filters=filters,
        summary=summary,
        disk=_disk_payload(root_path),
        cleanup=cleanup,
        runtime=runtime_payload,
        workflows=workflows,
        groups=tuple(group_payloads),
        symbols=tuple(symbols),
        next_steps=tuple(
            _next_steps(
                root=str(resolved_root),
                status=status,
                cleanup=cleanup,
                runtime=runtime_payload,
                workflows=workflows,
            )
        ),
    )


def _normalize_groups(groups: Iterable[object] | None) -> tuple[str, ...]:
    normalized = {normalize_pair_group(group) for group in groups or ()}
    return tuple(sorted(normalized))


def _selected_pairs(
    pairs: Iterable[object] | None,
    groups: Iterable[object],
) -> tuple[str, ...]:
    if not pairs and not groups:
        return ()
    return tuple(expand_pair_selection(pairs, groups))


def _normalize_timeframes(values: Iterable[object] | None) -> tuple[str, ...]:
    return tuple(
        sorted({_normalize_timeframe(value) for value in values or ()})
    )


def _normalize_timeframe(value: object) -> str:
    text = str(value).strip()
    upper = text.upper()
    if upper in Timeframe.__members__:
        return upper
    lower = text.lower()
    for member in Timeframe:
        if member.value.lower() == lower:
            return str(member.name)
    return upper


def _normalize_formats(values: Iterable[object] | None) -> tuple[str, ...]:
    return tuple(sorted({_normalize_format(value) for value in values or ()}))


def _normalize_format(value: object) -> str:
    text = str(value).strip()
    upper = text.upper()
    if upper in Format.__members__:
        return upper
    lower = text.lower()
    for member in Format:
        if member.value.lower() == lower:
            return str(member.name)
    return upper


def _cache_artifacts(root: Path) -> tuple[_Artifact, ...]:
    if not root.exists():
        return ()
    artifacts: list[_Artifact] = []
    for path in sorted(root.rglob(CACHE_FILENAME), key=lambda item: str(item)):
        if not path.is_file():
            continue
        layout = _layout_for_path(root, path)
        if layout is None:
            continue
        artifacts.append(
            _Artifact(
                path=path,
                size_bytes=_file_size(path),
                file_format=layout["format"],
                timeframe=layout["timeframe"],
                symbol=layout["symbol"],
            )
        )
    return tuple(artifacts)


def _source_artifacts(root: Path) -> tuple[_Artifact, ...]:
    artifacts: list[_Artifact] = []
    for path in find_transient_source_artifacts(root):
        layout = _layout_for_path(root, path)
        if layout is None:
            continue
        artifacts.append(
            _Artifact(
                path=path,
                size_bytes=_file_size(path),
                file_format=layout["format"],
                timeframe=layout["timeframe"],
                symbol=layout["symbol"],
            )
        )
    return tuple(artifacts)


def _layout_for_path(root: Path, path: Path) -> dict[str, str] | None:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return None
    parts = relative.parts
    if len(parts) < 3:
        return None
    return {
        "format": _normalize_format(parts[0]),
        "timeframe": _normalize_timeframe(parts[1]),
        "symbol": parts[2].lower(),
    }


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _selected_artifacts(
    artifacts: Iterable[_Artifact],
    *,
    pairs: tuple[str, ...],
    timeframes: tuple[str, ...],
    formats: tuple[str, ...],
) -> tuple[_Artifact, ...]:
    pair_filter = set(pairs)
    timeframe_filter = set(timeframes)
    format_filter = set(formats)
    return tuple(
        artifact
        for artifact in artifacts
        if (not pair_filter or artifact.symbol in pair_filter)
        and (not timeframe_filter or artifact.timeframe in timeframe_filter)
        and (not format_filter or artifact.file_format in format_filter)
    )


def _symbol_payloads(
    *,
    cache_artifacts: Iterable[_Artifact],
    source_artifacts: Iterable[_Artifact],
    selected_pairs: tuple[str, ...],
) -> list[dict[str, Any]]:
    accumulators: dict[str, _SymbolAccumulator] = {
        pair: _SymbolAccumulator(pair) for pair in selected_pairs
    }
    for artifact in cache_artifacts:
        accumulator = accumulators.setdefault(
            artifact.symbol,
            _SymbolAccumulator(artifact.symbol),
        )
        accumulator.cache_count += 1
        accumulator.cache_size_bytes += artifact.size_bytes
        accumulator.formats.add(artifact.file_format)
        accumulator.timeframes.add(artifact.timeframe)
    for artifact in source_artifacts:
        accumulator = accumulators.setdefault(
            artifact.symbol,
            _SymbolAccumulator(artifact.symbol),
        )
        accumulator.source_artifact_count += 1
        accumulator.source_artifact_size_bytes += artifact.size_bytes
        accumulator.formats.add(artifact.file_format)
        accumulator.timeframes.add(artifact.timeframe)
    return [accumulators[symbol].to_dict() for symbol in sorted(accumulators)]


def _group_payloads(
    *,
    groups: tuple[str, ...],
    symbols: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_symbol = {str(symbol["symbol"]): dict(symbol) for symbol in symbols}
    payloads: list[dict[str, Any]] = []
    for group in groups:
        group_symbols = tuple(PAIR_GROUPS[group])
        summaries = [
            by_symbol.get(symbol, _SymbolAccumulator(symbol).to_dict())
            for symbol in group_symbols
        ]
        missing = [
            str(summary["symbol"])
            for summary in summaries
            if int(summary["cache_count"]) == 0
        ]
        source_artifact_count = sum(
            int(summary["source_artifact_count"]) for summary in summaries
        )
        cache_count = sum(int(summary["cache_count"]) for summary in summaries)
        payloads.append(
            {
                "group": group,
                "status": _file_status(
                    cache_count=cache_count,
                    source_artifact_count=source_artifact_count,
                    missing_symbols=missing,
                    active_workflows=0,
                    runtime_state="",
                ),
                "expected_symbol_count": len(group_symbols),
                "symbols_with_cache": sum(
                    1
                    for summary in summaries
                    if int(summary["cache_count"]) > 0
                ),
                "missing_symbols": missing,
                "cache_count": cache_count,
                "source_artifact_count": source_artifact_count,
            }
        )
    return payloads


def _missing_symbols(
    symbols: Iterable[Mapping[str, Any]],
) -> tuple[str, ...]:
    return tuple(
        str(symbol["symbol"])
        for symbol in symbols
        if int(symbol["cache_count"]) == 0
    )


def _runtime_payload(runtime: Mapping[str, Any] | None) -> dict[str, Any]:
    if runtime is None:
        return {
            "state": "unknown",
            "message": "runtime status was not collected",
        }
    payload = dict(runtime)
    payload.setdefault("state", "unknown")
    payload.setdefault("message", "")
    return payload


def _workflow_payload(
    job_snapshots: Iterable[Mapping[str, Any]] | None,
    *,
    workflow_store_path: str,
    max_jobs: int,
) -> dict[str, Any]:
    if job_snapshots is None:
        return {
            "state": "unknown",
            "store_path": workflow_store_path,
            "job_count": 0,
            "active_count": 0,
            "terminal_count": 0,
            "by_lifecycle": {},
            "by_status": {},
            "latest": [],
        }
    snapshots = [dict(snapshot) for snapshot in job_snapshots]
    by_lifecycle = Counter(
        str(snapshot.get("lifecycle", "unknown") or "unknown")
        for snapshot in snapshots
    )
    by_status = Counter(
        str(snapshot.get("status", "UNKNOWN") or "UNKNOWN")
        for snapshot in snapshots
    )
    active = [
        snapshot
        for snapshot in snapshots
        if _snapshot_lifecycle(snapshot) in _ACTIVE_LIFECYCLES
    ]
    terminal = [
        snapshot
        for snapshot in snapshots
        if _snapshot_lifecycle(snapshot) in _TERMINAL_LIFECYCLES
    ]
    latest = [_compact_snapshot(snapshot) for snapshot in snapshots[:max_jobs]]
    state = "no-jobs"
    if active:
        state = "active"
    elif terminal:
        state = "terminal-only"
    return {
        "state": state,
        "store_path": workflow_store_path,
        "job_count": len(snapshots),
        "active_count": len(active),
        "terminal_count": len(terminal),
        "by_lifecycle": dict(sorted(by_lifecycle.items())),
        "by_status": dict(sorted(by_status.items())),
        "latest": latest,
    }


def _snapshot_lifecycle(snapshot: Mapping[str, Any]) -> str:
    return str(snapshot.get("lifecycle", "unknown") or "unknown").lower()


def _compact_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    progress = snapshot.get("progress")
    progress_payload = progress if isinstance(progress, Mapping) else {}
    return {
        "job_id": str(snapshot.get("job_id", "") or ""),
        "workflow_id": str(snapshot.get("workflow_id", "") or ""),
        "request_id": str(snapshot.get("request_id", "") or ""),
        "lifecycle": str(snapshot.get("lifecycle", "unknown") or "unknown"),
        "status": str(snapshot.get("status", "UNKNOWN") or "UNKNOWN"),
        "current_stage": str(progress_payload.get("current_stage", "") or ""),
        "completed_children": int(
            progress_payload.get("completed_children", 0) or 0
        ),
        "total_children": int(progress_payload.get("total_children", 0) or 0),
        "last_error": str(progress_payload.get("last_error", "") or ""),
        "updated_at_utc": str(snapshot.get("updated_at_utc", "") or ""),
    }


def _disk_payload(root: Path) -> dict[str, Any]:
    anchor = _nearest_existing_path(root)
    try:
        usage = shutil.disk_usage(anchor)
    except OSError as exc:
        return {
            "path": str(anchor.resolve(strict=False)),
            "state": "unknown",
            "message": str(exc),
        }
    percent_used = (usage.used / usage.total) * 100 if usage.total else 0.0
    return {
        "path": str(anchor.resolve(strict=False)),
        "state": "ok",
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "percent_used": round(percent_used, 1),
    }


def _nearest_existing_path(path: Path) -> Path:
    current = path.expanduser()
    while not current.exists() and current != current.parent:
        current = current.parent
    return current if current.exists() else Path.cwd()


def _cleanup_payload(source_artifacts: Iterable[_Artifact]) -> dict[str, Any]:
    artifacts = tuple(source_artifacts)
    by_suffix: Counter[str] = Counter(
        artifact.path.suffix.lower() for artifact in artifacts
    )
    return {
        "state": "pending" if artifacts else "not-needed",
        "preserves": [CACHE_FILENAME],
        "transient_suffixes": list(TRANSIENT_SOURCE_SUFFIXES),
        "source_artifact_count": len(artifacts),
        "source_artifact_size_bytes": sum(
            artifact.size_bytes for artifact in artifacts
        ),
        "by_suffix": dict(sorted(by_suffix.items())),
    }


def _overall_status(
    *,
    cache_count: int,
    source_artifact_count: int,
    missing_symbols: tuple[str, ...],
    runtime_state: str,
    active_workflows: int,
) -> str:
    if runtime_state == "stopped" and active_workflows:
        return "runtime-stopped"
    if (
        active_workflows
        and cache_count
        and not source_artifact_count
        and not missing_symbols
    ):
        return "drained-stuck"
    return _file_status(
        cache_count=cache_count,
        source_artifact_count=source_artifact_count,
        missing_symbols=missing_symbols,
        active_workflows=active_workflows,
        runtime_state=runtime_state,
    )


def _file_status(
    *,
    cache_count: int,
    source_artifact_count: int,
    missing_symbols: Iterable[str],
    active_workflows: int,
    runtime_state: str,
) -> str:
    missing = tuple(missing_symbols)
    if active_workflows and source_artifact_count:
        return "active"
    if source_artifact_count:
        return "pending-cleanup"
    if missing and cache_count:
        return "partial-cache"
    if missing:
        return "not-started"
    if cache_count:
        return "cache-ready"
    if runtime_state == "running" or active_workflows:
        return "active"
    return "not-started"


def _next_steps(
    *,
    root: str,
    status: str,
    cleanup: Mapping[str, Any],
    runtime: Mapping[str, Any],
    workflows: Mapping[str, Any],
) -> list[str]:
    steps: list[str] = []
    if cleanup.get("state") == "pending":
        steps.append(
            "Run "
            f"`histdatacom cleanup sources --data-directory {root} --apply` "
            "after confirming source artifacts are no longer needed."
        )
    has_active_workflows = bool(workflows.get("active_count"))
    if runtime.get("state") == "stopped" and has_active_workflows:
        steps.append(
            "Start or restart the local runtime with "
            "`histdatacom runtime start` before waiting on active jobs."
        )
    if status in {"drained-stuck", "runtime-stopped"} and has_active_workflows:
        workflow_id = _latest_active_workflow_id(workflows)
        inspect_target = workflow_id or "WORKFLOW_ID"
        steps.append(
            "File cache work appears drained while a workflow remains active; "
            f"inspect it with `histdatacom jobs inspect {inspect_target} "
            "--offline --json`."
        )
        steps.append(
            "Use `histdatacom jobs cancel`, `histdatacom jobs retry`, or "
            "`histdatacom jobs resume` for the stuck workflow after inspection."
        )
    if not steps and cleanup.get("state") == "not-needed":
        steps.append("No transient source cleanup is needed for this scope.")
    return steps


def _latest_active_workflow_id(workflows: Mapping[str, Any]) -> str:
    latest = workflows.get("latest")
    if not isinstance(latest, list):
        return ""
    for item in latest:
        if not isinstance(item, Mapping):
            continue
        if _snapshot_lifecycle(item) not in _ACTIVE_LIFECYCLES:
            continue
        return str(item.get("workflow_id") or item.get("job_id") or "")
    return ""
