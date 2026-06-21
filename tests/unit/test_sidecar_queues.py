"""Tests for Temporal sidecar task queue configuration."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.sidecar.queues import (
    TaskQueueLane,
    build_sidecar_task_queues,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy


def test_task_queues_are_workspace_scoped(tmp_path: Path) -> None:
    """Workspaces should get deterministic, isolated task queue names."""
    left_policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "left" / "project",
        runtime_home=tmp_path / "runtime",
    )
    right_policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "right" / "project",
        runtime_home=tmp_path / "runtime",
    )

    left = build_sidecar_task_queues(runtime_policy=left_policy)
    right = build_sidecar_task_queues(runtime_policy=right_policy)

    assert left.orchestration != right.orchestration
    assert left.network == f"histdatacom.{left_policy.workspace_id}.network"
    assert left.cpu_file == f"histdatacom.{left_policy.workspace_id}.cpu-file"
    assert left.influx == f"histdatacom.{left_policy.workspace_id}.influx"


def test_task_queue_lanes_round_trip_from_cli_values(tmp_path: Path) -> None:
    """Lane names should normalize across CLI and Python spelling."""
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    config = build_sidecar_worker_config(
        runtime_policy=policy,
        lane="cpu_file",
    )

    assert config.lane == TaskQueueLane.CPU_FILE
    assert config.task_queue == config.task_queues.cpu_file
    assert config.for_lane("network").task_queue == config.task_queues.network
    assert config.worker_options["max_concurrent_activities"] >= 1
    assert (
        config.for_lane("network").worker_options["max_concurrent_activities"]
        >= config.worker_options["max_concurrent_activities"]
    )


def test_worker_config_accepts_concurrency_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Sidecar workers should expose lane-level concurrency tuning."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    config = build_sidecar_worker_config(
        runtime_policy=policy,
        lane="network",
        cpu_utilization="medium",
        concurrency_overrides={"network": 12},
    )

    assert config.concurrency_profile.base_workers == 5
    assert config.worker_options == {"max_concurrent_activities": 12}
    assert config.to_dict()["concurrency"]["network_workers"] == 12


def test_empty_task_queue_prefix_fails_clearly(tmp_path: Path) -> None:
    """Configuration should reject unusable task queue prefixes."""
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )

    with pytest.raises(ValueError, match="prefix cannot be empty"):
        build_sidecar_worker_config(
            runtime_policy=policy,
            task_queue_prefix=".",
        )
