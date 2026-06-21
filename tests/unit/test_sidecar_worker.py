"""Tests for Temporal sidecar worker skeleton wiring."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from histdatacom.sidecar import worker
from histdatacom.sidecar.queues import (
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy


class _FakeWorker:
    """Test double for temporalio.worker.Worker."""

    instances: list["_FakeWorker"] = []

    def __init__(
        self,
        client: object,
        *,
        task_queue: str,
        workflows: list[object],
        activities: list[object],
        **worker_options: object,
    ) -> None:
        self.client = client
        self.task_queue = task_queue
        self.workflows = workflows
        self.activities = activities
        self.worker_options = worker_options
        self.ran = False
        self.instances.append(self)

    async def run(self) -> None:
        """Record that the worker run loop was invoked."""
        self.ran = True


class _FakeClient:
    """Test double for temporalio.client.Client."""

    connected: list[dict[str, str]] = []

    @classmethod
    async def connect(cls, target_host: str, *, namespace: str):
        """Record connection arguments and return a fake client."""
        cls.connected.append(
            {"target_host": target_host, "namespace": namespace}
        )
        return cls()


def _config(tmp_path: Path):
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    return build_sidecar_worker_config(
        runtime_policy=policy,
        lane=TaskQueueLane.CPU_FILE,
    )


def test_build_temporal_worker_uses_central_worker_config(
    tmp_path: Path,
) -> None:
    """Worker construction should receive the configured task queue lane."""
    _FakeWorker.instances.clear()
    config = _config(tmp_path)

    built = worker.build_temporal_worker(
        object(),
        config=config,
        worker_class=_FakeWorker,
        workflows=("workflow",),
        activities=("activity",),
        max_concurrent_activities=2,
    )

    assert built is _FakeWorker.instances[0]
    assert built.task_queue == config.task_queue
    assert built.workflows == ["workflow"]
    assert built.activities == ["activity"]
    assert built.worker_options == {"max_concurrent_activities": 2}


def test_run_temporal_worker_accepts_fake_temporal_classes(
    tmp_path: Path,
) -> None:
    """Worker startup should be directly invokable in unit tests."""
    _FakeClient.connected.clear()
    _FakeWorker.instances.clear()
    config = _config(tmp_path)

    ran = asyncio.run(
        worker.run_temporal_worker(
            config=config,
            client_class=_FakeClient,
            worker_class=_FakeWorker,
        )
    )

    assert ran.ran is True
    assert _FakeClient.connected == [
        {"target_host": config.target_host, "namespace": "default"}
    ]
    assert _FakeWorker.instances[0].task_queue == config.task_queue
    assert {
        workflow.__name__ for workflow in _FakeWorker.instances[0].workflows
    } >= {
        "HistDataRunWorkflow",
        "SymbolTimeframeWorkflow",
        "ValidateUrlsWorkflow",
    }
    assert {
        activity.__name__ for activity in _FakeWorker.instances[0].activities
    } == {
        "repository_refresh_activity",
        "dataset_plan_activity",
        "validate_urls_activity",
        "download_archives_activity",
    }


def test_default_workflows_include_topology_classes() -> None:
    """Default worker registration should include the workflow hierarchy."""
    assert [workflow.__name__ for workflow in worker.default_workflows()] == [
        "HistDataRunWorkflow",
        "RepositoryRefreshWorkflow",
        "DatasetPlanWorkflow",
        "SymbolTimeframeWorkflow",
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    ]


def test_default_activities_include_repository_refresh() -> None:
    """Default worker registration should include migrated activities."""
    assert [activity.__name__ for activity in worker.default_activities()] == [
        "repository_refresh_activity",
        "dataset_plan_activity",
        "validate_urls_activity",
        "download_archives_activity",
    ]


def test_worker_config_cli_emits_queue_metadata(
    tmp_path: Path,
    capsys,
) -> None:
    """The worker CLI should expose config without importing temporalio."""
    exit_code = worker.main(
        [
            "config",
            "--workspace",
            str(tmp_path / "workspace"),
            "--runtime-home",
            str(tmp_path / "runtime"),
            "--lane",
            "network",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["lane"] == "network"
    assert payload["task_queue"] == payload["task_queues"]["network"]
    assert payload["task_queues"]["cpu_file"].endswith(".cpu-file")


def test_worker_run_cli_reports_missing_temporal_extra(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    """Running the real worker without temporalio should fail with the extra."""

    async def fake_connect_temporal_client(**kwargs: object) -> object:
        return object()

    def missing_temporal_worker_class() -> object:
        raise worker.TemporalDependencyError(worker.TEMPORAL_EXTRA_HINT)

    monkeypatch.setattr(
        worker,
        "connect_temporal_client",
        fake_connect_temporal_client,
    )
    monkeypatch.setattr(
        worker,
        "_load_temporal_worker_class",
        missing_temporal_worker_class,
    )

    exit_code = worker.main(
        [
            "run",
            "--workspace",
            str(tmp_path / "workspace"),
            "--runtime-home",
            str(tmp_path / "runtime"),
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 1
    assert "histdatacom[temporal]" in output.err
