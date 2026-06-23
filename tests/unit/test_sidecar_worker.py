"""Tests for Temporal sidecar worker skeleton wiring."""

from __future__ import annotations

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path

from histdatacom.sidecar import worker
from histdatacom.sidecar.queues import (
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.readiness import read_worker_readiness
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
        await asyncio.sleep(0)


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
    assert built.worker_options["max_concurrent_activities"] == 2
    assert isinstance(
        built.worker_options["activity_executor"],
        ThreadPoolExecutor,
    )


def test_build_temporal_worker_applies_configured_concurrency(
    tmp_path: Path,
) -> None:
    """Worker construction should apply lane concurrency by default."""
    _FakeWorker.instances.clear()
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    config = build_sidecar_worker_config(
        runtime_policy=policy,
        lane=TaskQueueLane.NETWORK,
        concurrency_overrides={TaskQueueLane.NETWORK: 11},
    )

    built = worker.build_temporal_worker(
        object(),
        config=config,
        worker_class=_FakeWorker,
        workflows=("workflow",),
        activities=("activity",),
    )

    assert built.worker_options["max_concurrent_activities"] == 11
    assert isinstance(
        built.worker_options["activity_executor"],
        ThreadPoolExecutor,
    )


def test_build_temporal_worker_preserves_explicit_activity_executor(
    tmp_path: Path,
) -> None:
    """Worker construction should not replace a caller-provided executor."""
    _FakeWorker.instances.clear()
    config = _config(tmp_path)
    activity_executor = object()

    built = worker.build_temporal_worker(
        object(),
        config=config,
        worker_class=_FakeWorker,
        workflows=("workflow",),
        activities=("activity",),
        activity_executor=activity_executor,
    )

    assert built.worker_options["activity_executor"] is activity_executor


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
    readiness = read_worker_readiness(
        config.runtime_policy.paths.state_dir,
        config.lane,
    )
    assert readiness is not None
    assert readiness["state"] == "ready"
    assert readiness["pid"] == os.getpid()
    assert readiness["task_queue"] == config.task_queue
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
        "data_quality_activity",
        "validate_urls_activity",
        "download_archives_activity",
        "extract_csv_activity",
        "build_cache_activity",
        "merge_cache_activity",
        "import_to_influx_activity",
    }


def test_default_workflows_include_topology_classes() -> None:
    """Default worker registration should include the workflow hierarchy."""
    assert [workflow.__name__ for workflow in worker.default_workflows()] == [
        "HistDataRunWorkflow",
        "RepositoryRefreshWorkflow",
        "DataQualityWorkflow",
        "DatasetPlanWorkflow",
        "SymbolTimeframeWorkflow",
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    ]


def test_default_workflows_validate_in_temporal_sandbox() -> None:
    """Workflow imports should not drag activity-only dependencies into sandbox."""
    temporal_workflow = import_module("temporalio.workflow")
    sandbox = import_module("temporalio.worker.workflow_sandbox")

    async def validate() -> None:
        for workflow_class in worker.default_workflows():
            definition = temporal_workflow._Definition.must_from_class(
                workflow_class
            )
            sandbox.SandboxedWorkflowRunner().prepare_workflow(definition)

    asyncio.run(validate())


def test_default_activities_include_repository_refresh() -> None:
    """Default worker registration should include migrated activities."""
    assert [activity.__name__ for activity in worker.default_activities()] == [
        "repository_refresh_activity",
        "dataset_plan_activity",
        "data_quality_activity",
        "validate_urls_activity",
        "download_archives_activity",
        "extract_csv_activity",
        "build_cache_activity",
        "merge_cache_activity",
        "import_to_influx_activity",
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
            "--cpu-utilization",
            "medium",
            "--max-concurrent-activities",
            "12",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["lane"] == "network"
    assert payload["task_queue"] == payload["task_queues"]["network"]
    assert payload["task_queues"]["cpu_file"].endswith(".cpu-file")
    assert payload["worker_options"] == {"max_concurrent_activities": 12}
    assert payload["concurrency"]["network_workers"] == 12


def test_worker_run_cli_reports_missing_temporal_dependency(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    """Running the real worker without temporalio should fail clearly."""

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
    assert "temporalio" in output.err
