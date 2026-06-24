"""Central Temporal task queue naming and worker configuration."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from histdatacom.orchestration.performance import (
    DEFAULT_INFLUX_WORKERS,
    DEFAULT_NETWORK_MULTIPLIER,
    DEFAULT_ORCHESTRATION_WORKERS,
    OrchestrationConcurrencyProfile,
    build_orchestration_concurrency_profile,
)
from histdatacom.orchestration.runtime import (
    OrchestrationRuntimePolicy,
    build_orchestration_runtime_policy,
)

DEFAULT_TEMPORAL_NAMESPACE = "default"
DEFAULT_TASK_QUEUE_PREFIX = "histdatacom"


class TaskQueueLane(str, Enum):
    """Known Temporal task queue lanes for the orchestration migration."""

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
class OrchestrationTaskQueues:
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
class OrchestrationWorkerConfig:
    """Runtime configuration shared by Temporal clients and workers."""

    runtime_policy: OrchestrationRuntimePolicy
    namespace: str
    task_queues: OrchestrationTaskQueues
    lane: TaskQueueLane = TaskQueueLane.ORCHESTRATION
    concurrency: OrchestrationConcurrencyProfile | None = None

    @property
    def target_host(self) -> str:
        """Return the Temporal frontend host:port for this workspace."""
        ports = self.runtime_policy.ports
        return f"{ports.bind_ip}:{ports.grpc}"

    @property
    def task_queue(self) -> str:
        """Return the active task queue for the configured worker lane."""
        return self.task_queues.for_lane(self.lane)

    @property
    def concurrency_profile(self) -> OrchestrationConcurrencyProfile:
        """Return configured or default orchestration concurrency policy."""
        return self.concurrency or build_orchestration_concurrency_profile()

    @property
    def worker_options(self) -> dict[str, int]:
        """Return Temporal worker options for this lane."""
        return {
            "max_concurrent_activities": (
                self.concurrency_profile.workers_for_lane(self.lane)
            )
        }

    def for_lane(
        self, lane: str | TaskQueueLane
    ) -> "OrchestrationWorkerConfig":
        """Return this config pointed at a different worker lane."""
        return OrchestrationWorkerConfig(
            runtime_policy=self.runtime_policy,
            namespace=self.namespace,
            task_queues=self.task_queues,
            lane=TaskQueueLane.from_value(lane),
            concurrency=self.concurrency_profile,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible worker configuration document."""
        return {
            "namespace": self.namespace,
            "target_host": self.target_host,
            "lane": self.lane.value,
            "task_queue": self.task_queue,
            "task_queues": self.task_queues.to_dict(),
            "concurrency": self.concurrency_profile.to_dict(),
            "worker_options": dict(self.worker_options),
            "runtime_policy": self.runtime_policy.to_dict(),
        }


def build_orchestration_task_queues(
    *,
    runtime_policy: OrchestrationRuntimePolicy | None = None,
    prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
) -> OrchestrationTaskQueues:
    """Build workspace-scoped Temporal task queue names."""
    policy = runtime_policy or build_orchestration_runtime_policy()
    normalized_prefix = _normalize_task_queue_prefix(prefix)
    queue_prefix = f"{normalized_prefix}.{policy.workspace_id}"
    return OrchestrationTaskQueues(
        prefix=normalized_prefix,
        workspace_id=policy.workspace_id,
        orchestration=f"{queue_prefix}.{TaskQueueLane.ORCHESTRATION.value}",
        network=f"{queue_prefix}.{TaskQueueLane.NETWORK.value}",
        cpu_file=f"{queue_prefix}.{TaskQueueLane.CPU_FILE.value}",
        influx=f"{queue_prefix}.{TaskQueueLane.INFLUX.value}",
    )


def build_orchestration_worker_config(
    *,
    runtime_policy: OrchestrationRuntimePolicy | None = None,
    workspace: Path | str | None = None,
    runtime_home: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    lane: str | TaskQueueLane = TaskQueueLane.ORCHESTRATION,
    cpu_utilization: str | int | None = "medium",
    network_multiplier: int = DEFAULT_NETWORK_MULTIPLIER,
    orchestration_workers: int = DEFAULT_ORCHESTRATION_WORKERS,
    influx_workers: int = DEFAULT_INFLUX_WORKERS,
    concurrency_overrides: Mapping[str | TaskQueueLane, int] | None = None,
) -> OrchestrationWorkerConfig:
    """Build Temporal worker/client config from orchestration runtime policy."""
    policy = runtime_policy or build_orchestration_runtime_policy(
        workspace=workspace,
        runtime_home=runtime_home,
        environ=environ,
    )
    return OrchestrationWorkerConfig(
        runtime_policy=policy,
        namespace=namespace.strip() or DEFAULT_TEMPORAL_NAMESPACE,
        task_queues=build_orchestration_task_queues(
            runtime_policy=policy,
            prefix=task_queue_prefix,
        ),
        lane=TaskQueueLane.from_value(lane),
        concurrency=build_orchestration_concurrency_profile(
            cpu_utilization=cpu_utilization,
            network_multiplier=network_multiplier,
            orchestration_workers=orchestration_workers,
            influx_workers=influx_workers,
            lane_overrides=concurrency_overrides,
        ),
    )


RuntimeTaskQueues = OrchestrationTaskQueues
WorkerConfig = OrchestrationWorkerConfig
build_task_queues = build_orchestration_task_queues
build_worker_config = build_orchestration_worker_config


def _normalize_task_queue_prefix(prefix: str) -> str:
    normalized = prefix.strip().strip(".")
    if not normalized:
        raise ValueError("Temporal task queue prefix cannot be empty")
    return normalized
