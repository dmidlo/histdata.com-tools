"""Tests for Temporal sidecar client helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from histdatacom.runtime_contracts import RunRequest
from histdatacom.sidecar import client
from histdatacom.sidecar.queues import build_sidecar_worker_config
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy


class _FakeTemporalClient:
    """Test double for temporalio.client.Client."""

    connect_calls: list[dict[str, str]] = []

    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []

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
        return _FakeWorkflowHandle(id=id, run_id="run-fake")


class _FakeWorkflowHandle:
    """Minimal fake Temporal workflow handle."""

    def __init__(self, *, id: str, run_id: str) -> None:
        self.id = id
        self.run_id = run_id


def _config(tmp_path: Path):
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    return build_sidecar_worker_config(
        runtime_policy=policy,
        namespace="histdatacom-test",
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
            "payload": request.to_dict(),
            "id": "histdatacom-run-test",
            "task_queue": config.task_queues.orchestration,
        }
    ]


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
