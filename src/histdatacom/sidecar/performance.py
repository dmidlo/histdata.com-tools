"""Performance policy and lightweight benchmark helpers for the sidecar."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field, replace
from importlib import import_module
from enum import Enum
from typing import Any, Callable, Mapping

from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.runtime_contracts import JSONValue, RunRequest, WorkItem

DEFAULT_NETWORK_MULTIPLIER = 3
DEFAULT_ORCHESTRATION_WORKERS = 1
DEFAULT_INFLUX_WORKERS = 1
LANE_ORCHESTRATION = "orchestration"
LANE_NETWORK = "network"
LANE_CPU_FILE = "cpu-file"
LANE_INFLUX = "influx"


@dataclass(frozen=True, slots=True)
class SidecarConcurrencyProfile:
    """Worker concurrency policy derived from the legacy CPU setting."""

    cpu_utilization: str
    base_workers: int
    orchestration_workers: int
    network_workers: int
    cpu_file_workers: int
    influx_workers: int
    network_multiplier: int = DEFAULT_NETWORK_MULTIPLIER
    source: str = "legacy_cpu_policy"

    def workers_for_lane(self, lane: object) -> int:
        """Return max concurrent activities for a task queue lane."""
        normalized = _normalize_lane(lane)
        if normalized == LANE_ORCHESTRATION:
            return self.orchestration_workers
        if normalized == LANE_NETWORK:
            return self.network_workers
        if normalized == LANE_CPU_FILE:
            return self.cpu_file_workers
        if normalized == LANE_INFLUX:
            return self.influx_workers
        raise ValueError(f"unhandled Temporal task queue lane {lane!r}")

    def with_lane_override(
        self,
        lane: object,
        workers: int,
    ) -> "SidecarConcurrencyProfile":
        """Return a profile with one lane explicitly overridden."""
        normalized_workers = _positive_int(
            workers,
            field_name="max_concurrent_activities",
        )
        normalized = _normalize_lane(lane)
        if normalized == LANE_ORCHESTRATION:
            return replace(
                self,
                orchestration_workers=normalized_workers,
                source="explicit_override",
            )
        if normalized == LANE_NETWORK:
            return replace(
                self,
                network_workers=normalized_workers,
                source="explicit_override",
            )
        if normalized == LANE_CPU_FILE:
            return replace(
                self,
                cpu_file_workers=normalized_workers,
                source="explicit_override",
            )
        if normalized == LANE_INFLUX:
            return replace(
                self,
                influx_workers=normalized_workers,
                source="explicit_override",
            )
        raise ValueError(f"unhandled Temporal task queue lane {lane!r}")

    def worker_options_for_lane(
        self,
        lane: object,
    ) -> dict[str, int]:
        """Return Temporal worker options for the configured lane."""
        return {"max_concurrent_activities": self.workers_for_lane(lane)}

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible concurrency policy metadata."""
        return {
            "cpu_utilization": self.cpu_utilization,
            "base_workers": self.base_workers,
            "orchestration_workers": self.orchestration_workers,
            "network_workers": self.network_workers,
            "cpu_file_workers": self.cpu_file_workers,
            "influx_workers": self.influx_workers,
            "network_multiplier": self.network_multiplier,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkMeasurement:
    """Compact performance measurement for local fixture benchmarks."""

    name: str
    work_item_count: int
    elapsed_seconds: float
    cpu_seconds: float
    peak_rss_bytes: int
    retry_count: int = 0
    startup_seconds: float = 0.0
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def throughput_per_second(self) -> float:
        """Return processed work items per elapsed wall-clock second."""
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.work_item_count / self.elapsed_seconds

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible benchmark metadata."""
        return {
            "name": self.name,
            "work_item_count": self.work_item_count,
            "elapsed_seconds": self.elapsed_seconds,
            "cpu_seconds": self.cpu_seconds,
            "peak_rss_bytes": self.peak_rss_bytes,
            "retry_count": self.retry_count,
            "startup_seconds": self.startup_seconds,
            "throughput_per_second": self.throughput_per_second,
            "metadata": dict(self.metadata),
        }


def build_sidecar_concurrency_profile(
    *,
    cpu_utilization: str | int | None = "medium",
    network_multiplier: int = DEFAULT_NETWORK_MULTIPLIER,
    orchestration_workers: int = DEFAULT_ORCHESTRATION_WORKERS,
    influx_workers: int = DEFAULT_INFLUX_WORKERS,
    lane_overrides: Mapping[Any, int] | None = None,
) -> SidecarConcurrencyProfile:
    """Build sidecar worker concurrency from the legacy CPU policy."""
    base_workers = get_pool_cpu_count(cpu_utilization)
    multiplier = _positive_int(
        network_multiplier,
        field_name="network_multiplier",
    )
    profile = SidecarConcurrencyProfile(
        cpu_utilization=str(cpu_utilization or "medium"),
        base_workers=base_workers,
        orchestration_workers=_positive_int(
            orchestration_workers,
            field_name="orchestration_workers",
        ),
        network_workers=max(1, base_workers * multiplier),
        cpu_file_workers=max(1, base_workers),
        influx_workers=_positive_int(
            influx_workers,
            field_name="influx_workers",
        ),
        network_multiplier=multiplier,
    )
    for lane, workers in dict(lane_overrides or {}).items():
        profile = profile.with_lane_override(lane, workers)
    return profile


def benchmark_operation(
    name: str,
    operation: Callable[[], Any],
    *,
    work_item_count: int,
    retry_count: int = 0,
    startup_seconds: float = 0.0,
    metadata: Mapping[str, JSONValue] | None = None,
) -> BenchmarkMeasurement:
    """Run one local operation and capture throughput/resource metadata."""
    normalized_count = _nonnegative_int(
        work_item_count,
        field_name="work_item_count",
    )
    normalized_retries = _nonnegative_int(
        retry_count,
        field_name="retry_count",
    )
    start_wall = time.perf_counter()
    start_cpu = time.process_time()
    operation()
    elapsed_seconds = max(0.0, time.perf_counter() - start_wall)
    cpu_seconds = max(0.0, time.process_time() - start_cpu)
    return BenchmarkMeasurement(
        name=name,
        work_item_count=normalized_count,
        elapsed_seconds=elapsed_seconds,
        cpu_seconds=cpu_seconds,
        peak_rss_bytes=_peak_rss_bytes(),
        retry_count=normalized_retries,
        startup_seconds=max(0.0, float(startup_seconds)),
        metadata=dict(metadata or {}),
    )


def compare_partition_batching(
    request: RunRequest,
    work_items: tuple[WorkItem, ...],
    *,
    max_work_items_per_batch: int | None = None,
) -> dict[str, JSONValue]:
    """Compare coarse pair/timeframe fanout with period-batch fanout."""
    from histdatacom.sidecar.workflows import (
        max_work_items_per_batch as configured_batch_size,
        period_batch_partitions,
        request_partitions,
    )

    coarse_partitions = request_partitions(request)
    period_batches = period_batch_partitions(
        request,
        work_items,
        max_work_items_per_batch=max_work_items_per_batch,
    )
    batch_size = (
        max_work_items_per_batch
        if max_work_items_per_batch is not None
        else configured_batch_size(request)
    )
    coarse_counts = [
        _coarse_partition_work_item_count(work_items, partition)
        for partition in coarse_partitions
    ]
    batch_counts = [
        int(str(partition.get("work_item_count", "0") or "0"))
        for partition in period_batches
    ]
    coarse_max = max(coarse_counts, default=0)
    batch_max = max(batch_counts, default=0)
    return {
        "coarse_partition_count": len(coarse_partitions),
        "period_batch_count": len(period_batches),
        "work_item_count": len(work_items),
        "max_work_items_per_batch": int(batch_size),
        "coarse_max_work_items_per_child": coarse_max,
        "period_batch_max_work_items_per_child": batch_max,
        "max_child_work_item_reduction": max(0, coarse_max - batch_max),
    }


def benchmark_partition_batching(
    request: RunRequest,
    work_items: tuple[WorkItem, ...],
    *,
    max_work_items_per_batch: int | None = None,
) -> BenchmarkMeasurement:
    """Measure deterministic period-batch planning against coarse fanout."""
    comparison: dict[str, JSONValue] = {}

    def plan_batches() -> None:
        comparison.update(
            compare_partition_batching(
                request,
                work_items,
                max_work_items_per_batch=max_work_items_per_batch,
            )
        )

    return benchmark_operation(
        "period-batch-partitioning",
        plan_batches,
        work_item_count=len(work_items),
        metadata=comparison,
    )


def measure_startup(
    factory: Callable[[], Any],
) -> tuple[Any, float]:
    """Return a constructed object plus startup wall-clock seconds."""
    start_wall = time.perf_counter()
    value = factory()
    return value, max(0.0, time.perf_counter() - start_wall)


def _peak_rss_bytes() -> int:
    try:
        resource = import_module("resource")
    except ModuleNotFoundError:
        return 0
    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak = int(getattr(usage, "ru_maxrss", 0) or 0)
    if sys.platform.startswith("linux"):
        return peak * 1024
    return peak


def _normalize_lane(lane: object) -> str:
    value = lane.value if isinstance(lane, Enum) else lane
    normalized = str(value).strip().lower().replace("_", "-")
    allowed = {
        LANE_ORCHESTRATION,
        LANE_NETWORK,
        LANE_CPU_FILE,
        LANE_INFLUX,
    }
    if normalized not in allowed:
        raise ValueError(f"unknown Temporal task queue lane {lane!r}")
    return normalized


def _positive_int(value: int, *, field_name: str) -> int:
    normalized = int(value)
    if normalized < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return normalized


def _nonnegative_int(value: int, *, field_name: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{field_name} must be nonnegative")
    return normalized


def _coarse_partition_work_item_count(
    work_items: tuple[WorkItem, ...],
    partition: Mapping[str, str],
) -> int:
    pair = str(partition.get("pair", "") or "").lower()
    timeframe = str(partition.get("timeframe", "") or "").lower()
    return sum(
        1
        for item in work_items
        if (not pair or item.data_fxpair.lower() == pair)
        and (not timeframe or item.data_timeframe.lower() == timeframe)
    )
