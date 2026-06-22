"""Worker readiness markers for the local Temporal sidecar."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from histdatacom.sidecar.queues import SidecarWorkerConfig, TaskQueueLane

WORKER_READINESS_SCHEMA_VERSION = 1
WORKER_READINESS_DIRNAME = "worker-readiness"


def worker_readiness_dir(state_dir: Path | str) -> Path:
    """Return the sidecar worker readiness marker directory."""
    return Path(state_dir).expanduser() / WORKER_READINESS_DIRNAME


def worker_readiness_path(
    state_dir: Path | str,
    lane: str | TaskQueueLane,
) -> Path:
    """Return the readiness marker path for a worker lane."""
    resolved_lane = TaskQueueLane.from_value(lane)
    return worker_readiness_dir(state_dir) / f"{resolved_lane.value}.json"


def write_worker_readiness_payload(
    state_dir: Path | str,
    lane: str | TaskQueueLane,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Write a JSON readiness payload for a worker lane."""
    resolved_lane = TaskQueueLane.from_value(lane)
    output = dict(payload)
    output.setdefault("schema_version", WORKER_READINESS_SCHEMA_VERSION)
    output.setdefault("lane", resolved_lane.value)
    output.setdefault("updated_at_utc", _utc_now())
    path = worker_readiness_path(state_dir, resolved_lane)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(output, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output


def write_worker_readiness(
    config: SidecarWorkerConfig,
    *,
    pid: int,
    state: str = "ready",
    message: str = "Worker connected and entering run loop.",
) -> dict[str, Any]:
    """Write a worker readiness marker from runtime configuration."""
    payload = {
        "schema_version": WORKER_READINESS_SCHEMA_VERSION,
        "lane": config.lane.value,
        "component": f"worker:{config.lane.value}",
        "pid": int(pid),
        "state": state,
        "message": message,
        "namespace": config.namespace,
        "task_queue": config.task_queue,
        "target_host": config.target_host,
        "updated_at_utc": _utc_now(),
    }
    return write_worker_readiness_payload(
        config.runtime_policy.paths.state_dir,
        config.lane,
        payload,
    )


def read_worker_readiness(
    state_dir: Path | str,
    lane: str | TaskQueueLane,
) -> dict[str, Any] | None:
    """Read a worker readiness marker if it exists and is well-formed."""
    path = worker_readiness_path(state_dir, lane)
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    return loaded


def remove_worker_readiness(
    state_dir: Path | str,
    lane: str | TaskQueueLane | None = None,
) -> None:
    """Remove worker readiness markers."""
    if lane is not None:
        worker_readiness_path(state_dir, lane).unlink(missing_ok=True)
        return
    readiness_dir = worker_readiness_dir(state_dir)
    for path in readiness_dir.glob("*.json"):
        path.unlink(missing_ok=True)


def _utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
