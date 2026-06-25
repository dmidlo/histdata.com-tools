"""Tests for Temporal orchestration client helpers."""

from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace
from pathlib import Path

import pytest

from histdatacom.manifest_store import STATUS_STORE_REF_KEY
from histdatacom.runtime_contracts import (
    ArtifactRef,
    RunRequest,
    StatusEvent,
    WorkStatus,
)
from histdatacom.orchestration.control import JobLifecycle
from histdatacom.orchestration.control import OrchestrationJobSnapshot
from histdatacom.orchestration import client
from histdatacom.orchestration.queues import (
    OrchestrationWorkerConfig,
    TaskQueueLane,
    build_orchestration_worker_config,
)
from histdatacom.orchestration.readiness import write_worker_readiness_payload
from histdatacom.orchestration.resources import (
    OrchestrationExecutableUnavailable,
)
from histdatacom.orchestration.runtime import build_orchestration_runtime_policy
from histdatacom.orchestration.supervisor import (
    OrchestrationStatus,
    OrchestrationSupervisor,
)


class _FakeTemporalClient:
    """Test double for temporalio.client.Client."""

    connect_calls: list[dict[str, str]] = []

    def __init__(
        self,
        status_payload: dict[str, object] | None = None,
        result_payload: dict[str, object] | None = None,
    ) -> None:
        self.started: list[dict[str, object]] = []
        self.list_query = ""
        self.handles: dict[str, _FakeWorkflowHandle] = {}
        self.status_payload = status_payload
        self.result_payload = result_payload

    @classmethod
    async def connect(cls, target_host: str, *, namespace: str):
        """Record connect arguments and return a fake client."""
        cls.connect_calls.append(
            {"target_host": target_host, "namespace": namespace}
        )
        return cls()

    async def start_workflow(
        self,
        workflow: object,
        payload: dict,
        *,
        id: str,
        task_queue: str,
    ) -> object:
        """Record workflow submission arguments."""
        self.started.append(
            {
                "workflow": workflow,
                "payload": payload,
                "id": id,
                "task_queue": task_queue,
            }
        )
        handle = _FakeWorkflowHandle(
            id=id,
            run_id="run-fake",
            status_payload=self.status_payload,
            result_payload=self.result_payload,
        )
        self.handles[id] = handle
        return handle

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str = "",
    ) -> "_FakeWorkflowHandle":
        """Return a fake handle for status/control calls."""
        return self.handles.setdefault(
            workflow_id,
            _FakeWorkflowHandle(
                id=workflow_id,
                run_id=run_id or "run-fake",
                status_payload=self.status_payload,
                result_payload=self.result_payload,
            ),
        )

    def list_workflows(self, *, query: str):
        """Return fake workflow descriptions for list calls."""
        self.list_query = query
        return [
            SimpleNamespace(
                execution=SimpleNamespace(
                    workflow_id="histdatacom-run-listed",
                    run_id="run-listed",
                ),
                status="WORKFLOW_EXECUTION_STATUS_RUNNING",
            )
        ]


class _FakeWorkflowHandle:
    """Minimal fake Temporal workflow handle."""

    def __init__(
        self,
        *,
        id: str,
        run_id: str,
        status_payload: dict[str, object] | None = None,
        result_payload: dict[str, object] | None = None,
    ) -> None:
        self.id = id
        self.run_id = run_id
        self.cancel_calls = 0
        self.status_payload = status_payload
        self.result_payload = result_payload

    async def result(self) -> dict[str, object]:
        """Return a fake workflow result payload."""
        if self.result_payload is not None:
            return self.result_payload
        return _workflow_result_payload()

    async def query(self, query_name: str) -> dict[str, object]:
        """Return a fake workflow status query payload."""
        assert query_name == "status"
        if self.status_payload is not None:
            return self.status_payload
        return {
            "workflow_name": "HistDataRunWorkflow",
            "request_id": "run-test",
            "status": WorkStatus.UNKNOWN.value,
            "current_stage": "DownloadArchivesWorkflow",
            "total_children": 3,
            "completed_children": 1,
            "planned_children": ["ValidateUrlsWorkflow"],
            "completed_stages": ["ValidateUrlsWorkflow"],
            "events": [
                StatusEvent(
                    status=WorkStatus.URL_VALID,
                    stage="validate_urls",
                    message="URLs validated",
                ).to_dict()
            ],
            "artifacts": [
                ArtifactRef(
                    kind="manifest", path="/tmp/manifest.json"
                ).to_dict()
            ],
        }

    async def cancel(self) -> None:
        """Record a fake cancellation request."""
        self.cancel_calls += 1


def _workflow_result_payload(
    status: WorkStatus = WorkStatus.COMPLETED,
) -> dict[str, object]:
    """Return a fake workflow result payload with a chosen terminal status."""
    message = ""
    failure = None
    if status == WorkStatus.FAILED:
        message = "validation failed"
        failure = {
            "code": "VALIDATION_FAILED",
            "message": message,
            "retryable": True,
            "detail": {},
        }
    elif status == WorkStatus.CANCELLED:
        message = "cancelled by caller"
        failure = {
            "code": "CANCELLED",
            "message": message,
            "retryable": True,
            "detail": {},
        }
    stage_result: dict[str, object] = {
        "work_id": "work-1",
        "stage": "validate_urls",
        "status": status.value,
    }
    if failure is not None:
        stage_result["failure"] = failure
    return {
        "workflow_name": "HistDataRunWorkflow",
        "request_id": "run-test",
        "status": status.value,
        "progress": {
            "workflow_name": "HistDataRunWorkflow",
            "request_id": "run-test",
            "status": status.value,
            "current_stage": "finished",
            "total_children": 1,
            "completed_children": 1,
            "planned_children": ["ValidateUrlsWorkflow"],
            "completed_stages": ["ValidateUrlsWorkflow"],
            "last_error": message,
            "events": [
                StatusEvent(
                    status=status,
                    stage="finished",
                    message=message,
                ).to_dict()
            ],
            "artifacts": [
                ArtifactRef(
                    kind="manifest",
                    path="/tmp/manifest.json",
                ).to_dict()
            ],
        },
        "stage_results": [stage_result],
        "work_items": [{"work_id": "work-1"}],
        "artifacts": [
            ArtifactRef(kind="manifest", path="/tmp/manifest.json").to_dict()
        ],
    }


class _FakeSupervisor:
    """Test double for orchestration availability checks."""

    def __init__(
        self,
        *,
        current_state: str = "running",
        started_state: str = "running",
    ) -> None:
        self.current_state = current_state
        self.started_state = started_state
        self.status_calls = 0
        self.start_calls = 0

    def status(self, *, repair: bool = False) -> OrchestrationStatus:
        """Return the configured current status."""
        self.status_calls += 1
        return _status(self.current_state)

    def start(self) -> OrchestrationStatus:
        """Record a start attempt and return the configured status."""
        self.start_calls += 1
        return _status(self.started_state)


class _ResourceFailingSupervisor(_FakeSupervisor):
    """Supervisor that simulates metadata-only or unsupported artifacts."""

    def start(self) -> OrchestrationStatus:
        """Fail as packaged executable lookup would fail."""
        self.start_calls += 1
        raise OrchestrationExecutableUnavailable(
            "Temporal orchestration executable for platform 'linux-x86_64' "
            "is not bundled in this distribution."
        )


class _DependencyFailingSupervisor(_FakeSupervisor):
    """Supervisor that simulates a broken install missing temporalio."""

    def start(self) -> OrchestrationStatus:
        """Fail as worker dependency lookup would fail."""
        self.start_calls += 1
        raise RuntimeError(
            "Temporal worker support requires temporalio. Base histdatacom "
            "installs include this dependency."
        )


class _RuntimeFailingSupervisor(_FakeSupervisor):
    """Supervisor that simulates an unrelated startup failure."""

    def start(self) -> OrchestrationStatus:
        """Fail with an unrelated runtime error."""
        self.start_calls += 1
        raise RuntimeError("worker lane crashed during startup")


def _config(tmp_path: Path):
    policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    return build_orchestration_worker_config(
        runtime_policy=policy,
        namespace="histdatacom-test",
    )


def _write_ready_marker(state_dir: Path, lane: str, pid: int) -> None:
    """Write a fake worker readiness marker."""
    write_worker_readiness_payload(
        state_dir,
        lane,
        {
            "component": f"worker:{lane}",
            "pid": pid,
            "state": "ready",
            "message": "ready",
            "namespace": "histdatacom-custom",
            "task_queue": f"histdatacom-custom.test.{lane}",
            "target_host": "127.0.0.1:20333",
        },
    )


def _running_supervisor(
    tmp_path: Path,
    *,
    namespace: str = "histdatacom-custom",
    task_queue_prefix: str = "histdatacom-custom",
    grpc: int = 20333,
    include_worker_fleet: bool = True,
) -> tuple[OrchestrationSupervisor, OrchestrationWorkerConfig]:
    """Create a real supervisor with fake healthy persisted state."""
    policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    config = build_orchestration_worker_config(
        runtime_policy=policy,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
    )
    pids = {
        "server": 100,
        "worker:orchestration": 200,
        "worker:network": 300,
        "worker:cpu-file": 400,
        "worker:influx": 500,
    }
    for lane, pid in {
        "orchestration": 200,
        "network": 300,
        "cpu-file": 400,
        "influx": 500,
    }.items():
        _write_ready_marker(policy.paths.state_dir, lane, pid)
    ports = {
        "bind_ip": "127.0.0.1",
        "grpc": grpc,
        "ui": grpc + 1000,
        "source": "workspace",
        "collisions": [grpc - 1],
    }
    runtime_policy = policy.to_dict()
    runtime_policy["ports"] = ports
    state: dict[str, object] = {
        "pids": pids,
        "command": ["/tmp/temporal", "server", "start-dev"],
        "ports": ports,
        "runtime_policy": runtime_policy,
    }
    if include_worker_fleet:
        state["worker_fleet"] = {
            "namespace": config.namespace,
            "task_queue_prefix": config.task_queues.prefix,
            "task_queues": config.task_queues.to_dict(),
            "lanes": [lane.value for lane in TaskQueueLane],
            "concurrency": config.concurrency_profile.to_dict(),
        }
    policy.paths.pid_file.write_text(json.dumps(state), encoding="utf-8")
    supervisor = OrchestrationSupervisor(
        runtime_policy=policy,
        process_exists=lambda pid: True,
        frontend_ready=lambda runtime_policy: True,
        worker_dependency_available=lambda: True,
        sleep=lambda seconds: None,
    )
    return supervisor, config


def _status(state: str) -> OrchestrationStatus:
    """Create a minimal orchestration status object."""
    return OrchestrationStatus(
        state=state,
        message=f"{state} message",
        state_dir="/tmp/orchestration",
        pid_file="/tmp/orchestration/orchestration.pid.json",
        lock_file="/tmp/orchestration/orchestration.lock",
        logs={},
        pids={"temporal": 123} if state == "running" else {},
    )


def _status_payload(
    *,
    stage: str,
    status: WorkStatus,
    artifact_path: str = "/tmp/manifest.json",
    artifact_kind: str = "manifest",
) -> dict[str, object]:
    return {
        "workflow_name": "HistDataRunWorkflow",
        "request_id": "run-test",
        "status": status.value,
        "current_stage": stage,
        "total_children": 3,
        "completed_children": 1,
        "planned_children": ["ValidateUrlsWorkflow"],
        "completed_stages": ["ValidateUrlsWorkflow"],
        "events": [
            StatusEvent(
                status=status,
                stage=stage,
                message=f"{stage} status",
            ).to_dict()
        ],
        "artifacts": [
            ArtifactRef(
                kind=artifact_kind,
                path=artifact_path,
            ).to_dict()
        ],
    }


def _run_request(
    *,
    request_id: str = "run-test",
    data_directory: str = "data",
) -> RunRequest:
    return RunRequest(
        request_id=request_id,
        pairs=("EURUSD",),
        formats=("ascii",),
        timeframes=("m1",),
        data_directory=data_directory,
        validate_urls=True,
        download_data_archives=True,
        extract_csvs=True,
        api_return_type="polars",
    )


def _submit_seed_run(
    *,
    config,
    temporal_client: _FakeTemporalClient,
    request: RunRequest,
    status_payload: dict[str, object],
) -> None:
    asyncio.run(
        client.submit_run_request(
            request,
            config=config,
            client=temporal_client,
        )
    )
    temporal_client.handles[
        client.workflow_id_for_request(request)
    ].status_payload = status_payload


def test_connect_temporal_client_uses_runtime_policy_target(
    tmp_path: Path,
) -> None:
    """Client connection should use the orchestration runtime host and namespace."""
    _FakeTemporalClient.connect_calls.clear()
    config = _config(tmp_path)

    temporal_client = asyncio.run(
        client.connect_temporal_client(
            config=config,
            client_class=_FakeTemporalClient,
        )
    )

    assert isinstance(temporal_client, _FakeTemporalClient)
    assert _FakeTemporalClient.connect_calls == [
        {
            "target_host": config.target_host,
            "namespace": "histdatacom-test",
        }
    ]


def test_submit_run_request_uses_orchestration_queue(
    tmp_path: Path,
) -> None:
    """Submitting a job should not require activity implementation imports."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    request = RunRequest(
        request_id="run-test",
        pairs=("EURUSD",),
        formats=("ascii",),
        timeframes=("m1",),
    )

    handle = asyncio.run(
        client.submit_run_request(
            request,
            config=config,
            client=temporal_client,
        )
    )

    assert handle.to_dict() == {
        "request_id": "run-test",
        "workflow_id": "histdatacom-run-test",
        "run_id": "run-fake",
        "task_queue": config.task_queues.orchestration,
        "namespace": "histdatacom-test",
    }
    assert temporal_client.started == [
        {
            "workflow": "HistDataRunWorkflow",
            "payload": client.run_request_payload(request, config),
            "id": "histdatacom-run-test",
            "task_queue": config.task_queues.orchestration,
        }
    ]
    payload = temporal_client.started[0]["payload"]
    assert payload["metadata"]["orchestration_task_queues"] == (
        config.task_queues.to_dict()
    )
    assert payload["metadata"]["workflow_topology_version"] == 1
    status_store_ref = payload["metadata"][STATUS_STORE_REF_KEY]
    assert status_store_ref["kind"] == "manifest_status_store"
    assert Path(status_store_ref["store_path"]).parts[-2:] == (
        ".histdatacom",
        "manifest-status.sqlite3",
    )
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    assert stored is not None
    assert stored["lifecycle"] == JobLifecycle.SUBMITTED.value
    assert (
        client.orchestration_job_store_path(config).parent.name
        == ".histdatacom"
    )


def test_submit_run_request_without_config_fails_on_malformed_running_state(
    tmp_path: Path,
) -> None:
    """Direct submission should not silently fall back from bad live state."""
    supervisor, _ = _running_supervisor(tmp_path, include_worker_fleet=False)

    with pytest.raises(
        client.OrchestrationUnavailableError,
        match="missing worker_fleet",
    ):
        asyncio.run(
            client.submit_run_request(
                RunRequest(request_id="run-bad-direct"),
                supervisor=supervisor,
                client=_FakeTemporalClient(),
            )
        )


def test_submit_and_observe_reuses_running_orchestration(
    tmp_path: Path,
) -> None:
    """A healthy orchestration should be reused instead of started again."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    supervisor = _FakeSupervisor(current_state="running")
    request = RunRequest(request_id="run-test")

    result = asyncio.run(
        client.submit_run_request_and_observe(
            request,
            config=config,
            client=temporal_client,
            supervisor=supervisor,  # type: ignore[arg-type]
        )
    )

    assert result.status == "completed"
    assert result.result["workflow_name"] == "HistDataRunWorkflow"
    assert result.result["status"] == "COMPLETED"
    assert "stage_results" in result.result
    assert result.snapshot is not None
    assert result.snapshot.lifecycle == JobLifecycle.SUCCEEDED
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    assert stored is not None
    assert stored["lifecycle"] == JobLifecycle.SUCCEEDED.value
    assert stored["result"]["status"] == WorkStatus.COMPLETED.value
    assert stored["result"]["stage_result_count"] == 1
    assert "stage_results" not in stored["result"]
    assert supervisor.status_calls == 1
    assert supervisor.start_calls == 0
    assert temporal_client.started[0]["id"] == "histdatacom-run-test"


def test_submit_and_observe_notifies_progress_observer(
    tmp_path: Path,
) -> None:
    """Waited jobs should notify optional foreground progress observers."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    supervisor = _FakeSupervisor(current_state="running")
    snapshots: list[OrchestrationJobSnapshot] = []

    result = asyncio.run(
        client.submit_run_request_and_observe(
            RunRequest(request_id="run-test"),
            config=config,
            client=temporal_client,
            supervisor=supervisor,  # type: ignore[arg-type]
            progress_observer=snapshots.append,
            progress_interval_seconds=0.1,
        )
    )

    assert result.status == "completed"
    assert len(snapshots) == 2
    assert snapshots[0].lifecycle == JobLifecycle.SUBMITTED
    assert snapshots[-1].lifecycle == JobLifecycle.SUCCEEDED


def test_submit_and_observe_logs_bounded_lifecycle_metadata(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Lifecycle logs should identify the job without dumping request payloads."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    supervisor = _FakeSupervisor(current_state="running")
    request = RunRequest(
        request_id="run-logs",
        pairs=("EURUSD",),
        formats=("ascii",),
        timeframes=("m1",),
        validate_urls=True,
        metadata={"influx_config": {"INFLUX_TOKEN": "super-secret-token"}},
    )
    caplog.set_level(logging.INFO, logger="histdatacom.orchestration.client")

    result = asyncio.run(
        client.submit_run_request_and_observe(
            request,
            config=config,
            client=temporal_client,
            supervisor=supervisor,  # type: ignore[arg-type]
        )
    )

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert result.status == "completed"
    assert (
        "Preparing HistData orchestration job request_id=run-logs" in messages
    )
    assert (
        "Submitting HistData orchestration job request_id=run-logs" in messages
    )
    assert "Observed HistData orchestration job request_id=run-logs" in messages
    assert "super-secret-token" not in messages
    submit_record = next(
        record
        for record in caplog.records
        if record.getMessage().startswith(
            "Submitting HistData orchestration job"
        )
    )
    assert submit_record.request_id == "run-logs"
    assert submit_record.workflow_id == "histdatacom-run-logs"
    assert submit_record.operations == ["validate_urls"]


@pytest.mark.parametrize(
    ("workflow_status", "expected_status", "expected_lifecycle"),
    (
        (WorkStatus.FAILED, "failed", JobLifecycle.FAILED),
        (WorkStatus.CANCELLED, "cancelled", JobLifecycle.CANCELLED),
    ),
)
def test_submit_and_observe_derives_terminal_status_from_workflow_result(
    tmp_path: Path,
    workflow_status: WorkStatus,
    expected_status: str,
    expected_lifecycle: JobLifecycle,
) -> None:
    """Waited jobs should expose failed/cancelled workflow status."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient(
        result_payload=_workflow_result_payload(workflow_status)
    )
    supervisor = _FakeSupervisor(current_state="running")
    request = RunRequest(request_id="run-test")

    result = asyncio.run(
        client.submit_run_request_and_observe(
            request,
            config=config,
            client=temporal_client,
            supervisor=supervisor,  # type: ignore[arg-type]
        )
    )

    assert result.status == expected_status
    assert result.result["status"] == workflow_status.value
    assert result.snapshot is not None
    assert result.snapshot.status == workflow_status
    assert result.snapshot.lifecycle == expected_lifecycle
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    assert stored is not None
    assert stored["status"] == workflow_status.value
    assert stored["lifecycle"] == expected_lifecycle.value
    assert stored["result"]["status"] == workflow_status.value


def test_submit_and_observe_can_start_unavailable_orchestration(
    tmp_path: Path,
) -> None:
    """The client can start the orchestration only when explicitly requested."""
    config = _config(tmp_path)
    supervisor = _FakeSupervisor(current_state="stopped")
    request = RunRequest(request_id="run-start")

    result = asyncio.run(
        client.submit_run_request_and_observe(
            request,
            config=config,
            client=_FakeTemporalClient(),
            supervisor=supervisor,  # type: ignore[arg-type]
            start_if_needed=True,
            wait_for_result=False,
        )
    )

    assert result.status == "submitted"
    assert result.snapshot is not None
    assert result.snapshot.lifecycle == JobLifecycle.SUBMITTED
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-start"
    )
    assert stored is not None
    assert stored["lifecycle"] == JobLifecycle.SUBMITTED.value
    assert supervisor.start_calls == 1


def test_submit_and_observe_without_config_uses_running_orchestration_state(
    tmp_path: Path,
) -> None:
    """Default submission should inherit persisted running orchestration routing."""
    _FakeTemporalClient.connect_calls.clear()
    supervisor, expected_config = _running_supervisor(tmp_path)

    result = asyncio.run(
        client.submit_run_request_and_observe(
            RunRequest(request_id="run-state"),
            supervisor=supervisor,
            client_class=_FakeTemporalClient,
            wait_for_result=False,
        )
    )

    assert _FakeTemporalClient.connect_calls == [
        {
            "target_host": "127.0.0.1:20333",
            "namespace": "histdatacom-custom",
        }
    ]
    assert result.handle.task_queue == (
        expected_config.task_queues.orchestration
    )
    assert result.handle.namespace == "histdatacom-custom"
    assert result.snapshot is not None
    assert (
        result.snapshot.task_queue == expected_config.task_queues.orchestration
    )


def test_submit_and_observe_without_config_fails_on_malformed_running_state(
    tmp_path: Path,
) -> None:
    """Workflow submission should not fall back from malformed live state."""
    supervisor, _ = _running_supervisor(tmp_path, include_worker_fleet=False)

    with pytest.raises(
        client.OrchestrationUnavailableError,
        match="missing worker_fleet",
    ):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-bad-state"),
                supervisor=supervisor,
                client=_FakeTemporalClient(),
                wait_for_result=False,
            )
        )


def test_submit_and_observe_fails_when_orchestration_is_unavailable(
    tmp_path: Path,
) -> None:
    """Orchestration-backed runs should fail clearly when no orchestration is running."""
    config = _config(tmp_path)
    supervisor = _FakeSupervisor(current_state="stopped")

    with pytest.raises(
        client.OrchestrationUnavailableError, match="not running"
    ):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-missing"),
                config=config,
                client=_FakeTemporalClient(),
                supervisor=supervisor,  # type: ignore[arg-type]
            )
        )

    assert supervisor.start_calls == 0


def test_submit_and_observe_wraps_orchestration_resource_failures(
    tmp_path: Path,
) -> None:
    """Metadata-only and unsupported artifacts should fail as unavailable."""
    config = _config(tmp_path)
    supervisor = _ResourceFailingSupervisor(current_state="stopped")

    with pytest.raises(
        client.OrchestrationUnavailableError,
        match="not bundled in this distribution",
    ):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-metadata-only"),
                config=config,
                client=_FakeTemporalClient(),
                supervisor=supervisor,  # type: ignore[arg-type]
                start_if_needed=True,
            )
        )

    assert supervisor.start_calls == 1


def test_submit_and_observe_wraps_missing_temporal_dependency_failures(
    tmp_path: Path,
) -> None:
    """Broken installs missing temporalio should use the unavailable contract."""
    config = _config(tmp_path)
    supervisor = _DependencyFailingSupervisor(current_state="stopped")

    with pytest.raises(
        client.OrchestrationUnavailableError,
        match="temporalio",
    ):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-missing-temporalio"),
                config=config,
                client=_FakeTemporalClient(),
                supervisor=supervisor,  # type: ignore[arg-type]
                start_if_needed=True,
            )
        )

    assert supervisor.start_calls == 1


def test_submit_and_observe_does_not_hide_unrelated_runtime_failures(
    tmp_path: Path,
) -> None:
    """Only known missing-dependency startup errors should be normalized."""
    config = _config(tmp_path)
    supervisor = _RuntimeFailingSupervisor(current_state="stopped")

    with pytest.raises(RuntimeError, match="worker lane crashed"):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-runtime-failure"),
                config=config,
                client=_FakeTemporalClient(),
                supervisor=supervisor,  # type: ignore[arg-type]
                start_if_needed=True,
            )
        )

    assert supervisor.start_calls == 1


def test_inspect_job_status_queries_workflow_status(tmp_path: Path) -> None:
    """The client should expose workflow status as a control snapshot."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()

    snapshot = asyncio.run(
        client.inspect_job_status(
            "histdatacom-run-test",
            config=config,
            client=temporal_client,
        )
    )

    assert snapshot.workflow_id == "histdatacom-run-test"
    assert snapshot.lifecycle == JobLifecycle.RUNNING
    assert snapshot.progress is not None
    assert snapshot.progress.completed_children == 1
    assert snapshot.logs[0].message == "URLs validated"
    assert snapshot.artifacts[0].kind == "manifest"
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    store = client.orchestration_job_store(config)
    history = store.status_history("histdatacom-run-test", owner_kind="job")
    [artifact] = store.list_artifacts(
        "histdatacom-run-test",
        owner_kind="job",
    )
    assert stored is not None
    assert stored["progress"]["current_stage"] == "DownloadArchivesWorkflow"
    assert history[-1]["stage"] == "validate_urls"
    assert artifact["kind"] == "manifest"

    offline = asyncio.run(
        client.inspect_job_status(
            "histdatacom-run-test",
            config=config,
            offline=True,
        )
    )

    assert offline.workflow_id == "histdatacom-run-test"
    assert offline.logs[0].message == "URLs validated"
    assert offline.artifacts[0].kind == "manifest"


def test_cancel_job_requests_temporal_cancel_and_reports_state(
    tmp_path: Path,
) -> None:
    """Cancellation should call Temporal and return explicit intent state."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    handle = temporal_client.get_workflow_handle("histdatacom-run-test")

    snapshot = asyncio.run(
        client.cancel_job(
            "histdatacom-run-test",
            reason="operator",
            config=config,
            client=temporal_client,
        )
    )

    assert handle.cancel_calls == 1
    assert snapshot.lifecycle == JobLifecycle.CANCEL_REQUESTED
    assert snapshot.controls.cancel.reason == "operator"
    assert (
        snapshot.controls.cancel.metadata["cancellation"]["stops_future_work"]
        is True
    )
    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    assert stored is not None
    assert stored["lifecycle"] == JobLifecycle.CANCEL_REQUESTED.value


def test_retry_and_resume_include_current_stage_resume_policy(
    tmp_path: Path,
) -> None:
    """Retry/resume should start replacement workflows with resume policy."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    request = _run_request()
    _submit_seed_run(
        config=config,
        temporal_client=temporal_client,
        request=request,
        status_payload=_status_payload(
            stage="DownloadArchivesWorkflow",
            status=WorkStatus.FAILED,
        ),
    )

    retry = asyncio.run(
        client.retry_job(
            "histdatacom-run-test",
            reason="network",
            config=config,
            client=temporal_client,
        )
    )
    resume = asyncio.run(
        client.resume_job(
            "histdatacom-run-test",
            reason="continue",
            config=config,
            client=temporal_client,
        )
    )

    assert retry.workflow_id == (
        "histdatacom-run-test-retry-download-archives-001"
    )
    assert retry.lifecycle == JobLifecycle.RETRYING
    assert retry.controls.retry.metadata["resume_policy"]["stage"] == (
        "download_archives"
    )
    assert retry.controls.retry.metadata["parent_workflow_id"] == (
        "histdatacom-run-test"
    )
    assert (
        retry.controls.retry.metadata["replacement_handle"]["workflow_id"]
        == retry.workflow_id
    )
    assert temporal_client.started[-2]["id"] == retry.workflow_id

    assert resume.workflow_id == (
        "histdatacom-run-test-resume-download-archives-001"
    )
    assert resume.lifecycle == JobLifecycle.RESUMING
    assert resume.controls.resume.metadata["resume_policy"]["stage"] == (
        "download_archives"
    )
    assert resume.controls.resume.metadata["parent_workflow_id"] == (
        "histdatacom-run-test"
    )
    assert temporal_client.started[-1]["id"] == resume.workflow_id

    retry_payload = temporal_client.started[-2]["payload"]
    assert retry_payload["request_id"] == (
        "run-test-retry-download-archives-001"
    )
    assert retry_payload["metadata"]["control_execution"]["action"] == "retry"
    assert (
        retry_payload["metadata"]["control_execution"][
            "reuse_completed_artifacts"
        ]
        is True
    )

    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    stored_retry = client.orchestration_job_store(config).get_job_snapshot(
        retry.workflow_id
    )
    stored_resume = client.orchestration_job_store(config).get_job_snapshot(
        resume.workflow_id
    )
    assert stored is not None
    assert stored["lifecycle"] == JobLifecycle.RESUME_REQUESTED.value
    assert stored["metadata"]["control_attempts"]["retry"] == 1
    assert stored["metadata"]["control_attempts"]["resume"] == 1
    assert stored_retry is not None
    assert (
        stored_retry["metadata"]["control_execution"]["parent_workflow_id"]
        == "histdatacom-run-test"
    )
    assert stored_resume is not None
    assert stored_resume["lifecycle"] == JobLifecycle.RESUMING.value


@pytest.mark.parametrize(
    ("stage", "expected_stage", "artifact_kind", "filename", "partial_name"),
    (
        (
            "ExtractCsvWorkflow",
            "extract_csv",
            "csv",
            "EURUSD.csv",
            ".EURUSD.csv.abc.tmp",
        ),
        (
            "BuildCacheWorkflow",
            "build_cache",
            "cache",
            ".data",
            ".data.abc.tmp",
        ),
    ),
)
def test_retry_replacement_handles_failed_file_and_cache_states(
    tmp_path: Path,
    stage: str,
    expected_stage: str,
    artifact_kind: str,
    filename: str,
    partial_name: str,
) -> None:
    """Retry should replace failed file/cache jobs and clean known partials."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    data_dir = tmp_path / "data" / expected_stage
    data_dir.mkdir(parents=True)
    artifact_path = data_dir / filename
    artifact_path.write_text("complete")
    partial_path = data_dir / partial_name
    partial_path.write_text("partial")
    request = _run_request(data_directory=str(tmp_path / "data"))
    _submit_seed_run(
        config=config,
        temporal_client=temporal_client,
        request=request,
        status_payload=_status_payload(
            stage=stage,
            status=WorkStatus.FAILED,
            artifact_path=str(artifact_path),
            artifact_kind=artifact_kind,
        ),
    )

    retry = asyncio.run(
        client.retry_job(
            "histdatacom-run-test",
            reason="repair",
            config=config,
            client=temporal_client,
        )
    )

    assert retry.workflow_id == (
        f"histdatacom-run-test-retry-{expected_stage.replace('_', '-')}-001"
    )
    assert retry.lifecycle == JobLifecycle.RETRYING
    assert retry.controls.retry.metadata["resume_policy"]["stage"] == (
        expected_stage
    )
    assert retry.controls.retry.metadata["cleanup"][0]["removed"] is True
    assert not partial_path.exists()


def test_resume_replacement_handles_cancelled_state(
    tmp_path: Path,
) -> None:
    """Resume should replace cancelled jobs with explicit lineage metadata."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()
    data_dir = tmp_path / "data" / "download"
    data_dir.mkdir(parents=True)
    artifact_path = data_dir / "EURUSD.zip"
    artifact_path.write_text("complete")
    partial_path = data_dir / ".EURUSD.zip.abc.tmp"
    partial_path.write_text("partial")
    request = _run_request(data_directory=str(tmp_path / "data"))
    _submit_seed_run(
        config=config,
        temporal_client=temporal_client,
        request=request,
        status_payload=_status_payload(
            stage="DownloadArchivesWorkflow",
            status=WorkStatus.CANCELLED,
            artifact_path=str(artifact_path),
            artifact_kind="zip",
        ),
    )

    resume = asyncio.run(
        client.resume_job(
            "histdatacom-run-test",
            reason="operator continue",
            config=config,
            client=temporal_client,
        )
    )

    assert resume.workflow_id == (
        "histdatacom-run-test-resume-download-archives-001"
    )
    assert resume.lifecycle == JobLifecycle.RESUMING
    assert resume.controls.resume.metadata["previous_run_id"] == "run-fake"
    assert resume.controls.resume.metadata["cleanup"][0]["removed"] is True
    assert not partial_path.exists()
    stored_original = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )
    assert stored_original is not None
    assert stored_original["lifecycle"] == JobLifecycle.RESUME_REQUESTED.value


def test_get_job_result_persists_result_snapshot(tmp_path: Path) -> None:
    """Result lookup should persist the bounded workflow result payload."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()

    snapshot = asyncio.run(
        client.get_job_result(
            "histdatacom-run-test",
            config=config,
            client=temporal_client,
        )
    )

    stored = client.orchestration_job_store(config).get_job_snapshot(
        "histdatacom-run-test"
    )

    assert snapshot.lifecycle == JobLifecycle.SUCCEEDED
    assert stored is not None
    assert stored["result"]["workflow_name"] == "HistDataRunWorkflow"
    assert stored["result"]["status"] == WorkStatus.COMPLETED.value
    assert stored["result"]["stage_result_count"] == 1
    assert stored["result"]["work_item_count"] == 1
    assert stored["result"]["artifact_count"] == 1
    assert stored["result"]["progress"]["event_count"] == 1
    assert "stage_results" not in stored["result"]
    assert "work_items" not in stored["result"]
    assert "events" not in stored["result"]["progress"]


def test_list_job_statuses_offline_reads_local_store(
    tmp_path: Path,
) -> None:
    """Offline listing should not require a live Temporal client."""
    config = _config(tmp_path)
    store = client.orchestration_job_store(config)
    store.write_job_snapshot(
        OrchestrationJobSnapshot.from_handle(
            SimpleNamespace(
                request_id="run-offline",
                workflow_id="histdatacom-run-offline",
                run_id="run-stored",
                task_queue=config.task_queues.orchestration,
                namespace=config.namespace,
            )
        )
    )

    jobs = asyncio.run(client.list_job_statuses(config=config, offline=True))

    assert len(jobs.jobs) == 1
    assert jobs.jobs[0].workflow_id == "histdatacom-run-offline"
    assert jobs.jobs[0].lifecycle == JobLifecycle.SUBMITTED


@pytest.mark.parametrize(
    ("raw_status", "expected"),
    (
        ("WORKFLOW_EXECUTION_STATUS_RUNNING", WorkStatus.UNKNOWN),
        ("WORKFLOW_EXECUTION_STATUS_COMPLETED", WorkStatus.COMPLETED),
        ("WORKFLOW_EXECUTION_STATUS_FAILED", WorkStatus.FAILED),
        ("WORKFLOW_EXECUTION_STATUS_CANCELED", WorkStatus.CANCELLED),
        ("WORKFLOW_EXECUTION_STATUS_TERMINATED", WorkStatus.FAILED),
        ("WORKFLOW_EXECUTION_STATUS_TIMED_OUT", WorkStatus.FAILED),
        ("WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW", WorkStatus.UNKNOWN),
    ),
)
def test_status_from_temporal_description_normalizes_terminal_statuses(
    raw_status: str,
    expected: WorkStatus,
) -> None:
    """Temporal visibility statuses should map into package statuses."""
    assert client._status_from_temporal_description(raw_status) == expected


@pytest.mark.parametrize(
    ("raw_status", "expected_status", "expected_lifecycle"),
    (
        (
            "WORKFLOW_EXECUTION_STATUS_CANCELED",
            WorkStatus.CANCELLED,
            JobLifecycle.CANCELLED,
        ),
        (
            "WORKFLOW_EXECUTION_STATUS_TERMINATED",
            WorkStatus.FAILED,
            JobLifecycle.FAILED,
        ),
        (
            "WORKFLOW_EXECUTION_STATUS_TIMED_OUT",
            WorkStatus.FAILED,
            JobLifecycle.FAILED,
        ),
    ),
)
def test_temporal_visibility_snapshot_preserves_raw_status_metadata(
    tmp_path: Path,
    raw_status: str,
    expected_status: WorkStatus,
    expected_lifecycle: JobLifecycle,
) -> None:
    """Listed jobs should preserve raw Temporal status for diagnostics."""
    config = _config(tmp_path)
    description = SimpleNamespace(
        execution=SimpleNamespace(
            workflow_id="histdatacom-run-terminal",
            run_id="run-terminal",
        ),
        status=raw_status,
    )

    snapshot = client._snapshot_from_workflow_description(
        description,
        config=config,
    )

    assert snapshot.status == expected_status
    assert snapshot.lifecycle == expected_lifecycle
    assert snapshot.controls.cancel.available is False
    assert snapshot.controls.retry.available is True
    assert snapshot.controls.resume.available is True
    assert snapshot.metadata["temporal_execution_status"] == {
        "raw": raw_status,
        "normalized": raw_status.removeprefix("WORKFLOW_EXECUTION_STATUS_"),
    }


def test_list_job_statuses_uses_temporal_visibility_list(
    tmp_path: Path,
) -> None:
    """Listing should expose job handles without querying workflow history."""
    config = _config(tmp_path)
    temporal_client = _FakeTemporalClient()

    jobs = asyncio.run(
        client.list_job_statuses(config=config, client=temporal_client)
    )

    assert temporal_client.list_query == "WorkflowType='HistDataRunWorkflow'"
    assert jobs.jobs[0].workflow_id == "histdatacom-run-listed"
    assert jobs.jobs[0].lifecycle == JobLifecycle.RUNNING


def test_missing_temporal_dependency_has_core_dependency_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Using the real client loader without temporalio should fail clearly."""

    def missing_temporalio(module_name: str) -> object:
        raise ModuleNotFoundError(
            "No module named 'temporalio'",
            name="temporalio",
        )

    monkeypatch.setattr(client, "import_module", missing_temporalio)

    with pytest.raises(client.TemporalDependencyError) as err:
        client._load_temporal_client_class()

    assert "temporalio" in str(err.value)
