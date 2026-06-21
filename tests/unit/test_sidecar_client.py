"""Tests for Temporal sidecar client helpers."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from pathlib import Path

import pytest

from histdatacom.runtime_contracts import (
    ArtifactRef,
    RunRequest,
    StatusEvent,
    WorkStatus,
)
from histdatacom.sidecar.control import JobLifecycle
from histdatacom.sidecar import client
from histdatacom.sidecar.queues import build_sidecar_worker_config
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy
from histdatacom.sidecar.supervisor import SidecarStatus


class _FakeTemporalClient:
    """Test double for temporalio.client.Client."""

    connect_calls: list[dict[str, str]] = []

    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []
        self.list_query = ""
        self.handles: dict[str, _FakeWorkflowHandle] = {}

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
        handle = _FakeWorkflowHandle(id=id, run_id="run-fake")
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

    def __init__(self, *, id: str, run_id: str) -> None:
        self.id = id
        self.run_id = run_id
        self.cancel_calls = 0

    async def result(self) -> dict[str, str]:
        """Return a fake workflow result payload."""
        return {"workflow_name": "HistDataRunWorkflow", "status": "COMPLETED"}

    async def query(self, query_name: str) -> dict[str, object]:
        """Return a fake workflow status query payload."""
        assert query_name == "status"
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
                    kind="manifest",
                    path="/tmp/manifest.json",
                ).to_dict()
            ],
        }

    async def cancel(self) -> None:
        """Record a fake cancellation request."""
        self.cancel_calls += 1


class _FakeSupervisor:
    """Test double for sidecar availability checks."""

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

    def status(self, *, repair: bool = False) -> SidecarStatus:
        """Return the configured current status."""
        self.status_calls += 1
        return _status(self.current_state)

    def start(self) -> SidecarStatus:
        """Record a start attempt and return the configured status."""
        self.start_calls += 1
        return _status(self.started_state)


def _config(tmp_path: Path):
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    return build_sidecar_worker_config(
        runtime_policy=policy,
        namespace="histdatacom-test",
    )


def _status(state: str) -> SidecarStatus:
    """Create a minimal sidecar status object."""
    return SidecarStatus(
        state=state,
        message=f"{state} message",
        state_dir="/tmp/sidecar",
        pid_file="/tmp/sidecar/sidecar.pid.json",
        lock_file="/tmp/sidecar/sidecar.lock",
        logs={},
        pids={"temporal": 123} if state == "running" else {},
    )


def test_connect_temporal_client_uses_runtime_policy_target(
    tmp_path: Path,
) -> None:
    """Client connection should use the sidecar runtime host and namespace."""
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
    assert payload["metadata"]["sidecar_task_queues"] == (
        config.task_queues.to_dict()
    )
    assert payload["metadata"]["workflow_topology_version"] == 1


def test_submit_and_observe_reuses_running_sidecar(
    tmp_path: Path,
) -> None:
    """A healthy sidecar should be reused instead of started again."""
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
    assert result.result == {
        "workflow_name": "HistDataRunWorkflow",
        "status": "COMPLETED",
    }
    assert result.snapshot is not None
    assert result.snapshot.lifecycle == JobLifecycle.SUCCEEDED
    assert supervisor.status_calls == 1
    assert supervisor.start_calls == 0
    assert temporal_client.started[0]["id"] == "histdatacom-run-test"


def test_submit_and_observe_can_start_unavailable_sidecar(
    tmp_path: Path,
) -> None:
    """The client can start the sidecar only when explicitly requested."""
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
    assert supervisor.start_calls == 1


def test_submit_and_observe_fails_when_sidecar_is_unavailable(
    tmp_path: Path,
) -> None:
    """Sidecar-backed runs should fail clearly when no sidecar is running."""
    config = _config(tmp_path)
    supervisor = _FakeSupervisor(current_state="stopped")

    with pytest.raises(client.SidecarUnavailableError, match="not running"):
        asyncio.run(
            client.submit_run_request_and_observe(
                RunRequest(request_id="run-missing"),
                config=config,
                client=_FakeTemporalClient(),
                supervisor=supervisor,  # type: ignore[arg-type]
            )
        )

    assert supervisor.start_calls == 0


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


def test_missing_temporal_dependency_has_optional_extra_hint(
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

    assert "histdatacom[temporal]" in str(err.value)
