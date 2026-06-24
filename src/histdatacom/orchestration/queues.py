"""Public orchestration task-queue configuration helpers."""

from __future__ import annotations

from histdatacom.sidecar.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    SidecarTaskQueues as RuntimeTaskQueues,
    SidecarWorkerConfig as WorkerConfig,
    TaskQueueLane,
    build_sidecar_task_queues,
    build_sidecar_worker_config,
)

build_task_queues = build_sidecar_task_queues
build_worker_config = build_sidecar_worker_config

__all__ = [
    "DEFAULT_TASK_QUEUE_PREFIX",
    "DEFAULT_TEMPORAL_NAMESPACE",
    "RuntimeTaskQueues",
    "TaskQueueLane",
    "WorkerConfig",
    "build_task_queues",
    "build_worker_config",
]
