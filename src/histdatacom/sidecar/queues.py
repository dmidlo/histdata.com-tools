"""Central Temporal task queue naming and worker configuration."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from histdatacom.sidecar.runtime import (
    SidecarRuntimePolicy,
    build_sidecar_runtime_policy,
)

DEFAULT_TEMPORAL_NAMESPACE = "default"
DEFAULT_TASK_QUEUE_PREFIX = "histdatacom"


class TaskQueueLane(str, Enum):
    """Known Temporal task queue lanes for the sidecar migration."""

    ORCHESTRATION = "orchestration"
    NETWORK = "network"
    CPU_FILE = "cpu-file"
    INFLUX = "influx"

    @classmethod
    def from_value(cls, value: str | "TaskQueueLane") -> "TaskQueueLane":
        """Normalize a lane name from API, CLI, or tests."""
        if isinstance(value, cls):
            return value
        normalized = value.strip().lower().replace("_", "-")
        for lane in cls:
            if lane.value == normalized:
                return lane
        allowed = ", ".join(lane.value for lane in cls)
        raise ValueError(
            f"unknown Temporal task queue lane {value!r}; use {allowed}"
        )


@dataclass(frozen=True, slots=True)
class SidecarTaskQueues:
    """Workspace-scoped Temporal task queue names."""

    prefix: str
    workspace_id: str
    orchestration: str
    network: str
    cpu_file: str
    influx: str

    def for_lane(self, lane: str | TaskQueueLane) -> str:
        """Return the configured task queue name for a lane."""
        normalized = TaskQueueLane.from_value(lane)
        if normalized == TaskQueueLane.ORCHESTRATION:
            return self.orchestration
        if normalized == TaskQueueLane.NETWORK:
            return self.network
        if normalized == TaskQueueLane.CPU_FILE:
            return self.cpu_file
        if normalized == TaskQueueLane.INFLUX:
            return self.influx
        raise ValueError(f"unhandled Temporal task queue lane {lane!r}")

    def to_dict(self) -> dict[str, str]:
        """Return JSON-compatible task queue metadata."""
        return {
            "prefix": self.prefix,
            "workspace_id": self.workspace_id,
            "orchestration": self.orchestration,
            "network": self.network,
            "cpu_file": self.cpu_file,
            "influx": self.influx,
        }


@dataclass(frozen=True, slots=True)
class SidecarWorkerConfig:
    """Runtime configuration shared by Temporal clients and workers."""

    runtime_policy: SidecarRuntimePolicy
    namespace: str
    task_queues: SidecarTaskQueues
    lane: TaskQueueLane = TaskQueueLane.ORCHESTRATION

    @property
    def target_host(self) -> str:
        """Return the Temporal frontend host:port for this workspace."""
        ports = self.runtime_policy.ports
        return f"{ports.bind_ip}:{ports.grpc}"

    @property
    def task_queue(self) -> str:
        """Return the active task queue for the configured worker lane."""
        return self.task_queues.for_lane(self.lane)

    def for_lane(self, lane: str | TaskQueueLane) -> "SidecarWorkerConfig":
        """Return this config pointed at a different worker lane."""
        return SidecarWorkerConfig(
            runtime_policy=self.runtime_policy,
            namespace=self.namespace,
            task_queues=self.task_queues,
            lane=TaskQueueLane.from_value(lane),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible worker configuration document."""
        return {
            "namespace": self.namespace,
            "target_host": self.target_host,
            "lane": self.lane.value,
            "task_queue": self.task_queue,
            "task_queues": self.task_queues.to_dict(),
            "runtime_policy": self.runtime_policy.to_dict(),
        }


def build_sidecar_task_queues(
    *,
    runtime_policy: SidecarRuntimePolicy | None = None,
    prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
) -> SidecarTaskQueues:
    """Build workspace-scoped Temporal task queue names."""
    policy = runtime_policy or build_sidecar_runtime_policy()
    normalized_prefix = _normalize_task_queue_prefix(prefix)
    queue_prefix = f"{normalized_prefix}.{policy.workspace_id}"
    return SidecarTaskQueues(
        prefix=normalized_prefix,
        workspace_id=policy.workspace_id,
        orchestration=f"{queue_prefix}.{TaskQueueLane.ORCHESTRATION.value}",
        network=f"{queue_prefix}.{TaskQueueLane.NETWORK.value}",
        cpu_file=f"{queue_prefix}.{TaskQueueLane.CPU_FILE.value}",
        influx=f"{queue_prefix}.{TaskQueueLane.INFLUX.value}",
    )


def build_sidecar_worker_config(
    *,
    runtime_policy: SidecarRuntimePolicy | None = None,
    workspace: Path | str | None = None,
    runtime_home: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    lane: str | TaskQueueLane = TaskQueueLane.ORCHESTRATION,
) -> SidecarWorkerConfig:
    """Build Temporal worker/client config from sidecar runtime policy."""
    policy = runtime_policy or build_sidecar_runtime_policy(
        workspace=workspace,
        runtime_home=runtime_home,
        environ=environ,
    )
    return SidecarWorkerConfig(
        runtime_policy=policy,
        namespace=namespace.strip() or DEFAULT_TEMPORAL_NAMESPACE,
        task_queues=build_sidecar_task_queues(
            runtime_policy=policy,
            prefix=task_queue_prefix,
        ),
        lane=TaskQueueLane.from_value(lane),
    )


def _normalize_task_queue_prefix(prefix: str) -> str:
    normalized = prefix.strip().strip(".")
    if not normalized:
        raise ValueError("Temporal task queue prefix cannot be empty")
    return normalized
