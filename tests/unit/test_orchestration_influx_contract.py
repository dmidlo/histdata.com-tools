"""Contract-backed Influx coverage for the Temporal orchestration."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Mapping

import pytest
from temporalio.exceptions import ApplicationError

from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.runtime_contracts import JSONValue, RunRequest, WorkStatus
from histdatacom.orchestration import activities, workflows
from histdatacom.orchestration.workflow_metadata import TASK_QUEUE_METADATA_KEY

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"
EXPECTED_M1_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
    "1328072400000"
)


class _FakeInfluxWriter:
    """Context-managed Influx writer test double."""

    instances: list["_FakeInfluxWriter"] = []
    fail_with: Exception | None = None

    def __init__(self, args: Mapping[str, Any]) -> None:
        self.args = dict(args)
        self.batches: list[list[str]] = []
        self.closed = False
        self.instances.append(self)

    def __enter__(self) -> "_FakeInfluxWriter":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def write_lines(self, lines: list[str]) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.batches.append(list(lines))


class _LocalImportActivityExecutor:
    """Activity executor that calls the real Influx activity locally."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_activity(
        self,
        activity_name: str,
        payload: Mapping[str, JSONValue],
        *,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Record the workflow handoff and execute the real activity."""
        self.calls.append(
            {
                "activity_name": activity_name,
                "task_queue": task_queue,
                "payload": dict(payload),
            }
        )
        if activity_name != "import_to_influx":
            raise AssertionError(f"unexpected activity: {activity_name}")
        return activities.import_to_influx_activity(dict(payload))


def test_import_workflow_contract_writes_batches_without_live_influx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """ImportWorkflow should batch line protocol through the Influx lane."""
    _FakeInfluxWriter.instances.clear()
    _FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        activities,
        "_influx_batch_writer",
        _FakeInfluxWriter,
    )
    payload = _influx_workflow_payload(
        tmp_path,
        batch_size="2",
        delete_after_influx=True,
    )
    executor = _LocalImportActivityExecutor()
    workflow = workflows.ImportWorkflow(activity_executor=executor)

    summary = asyncio.run(workflow.run(payload))

    [writer] = _FakeInfluxWriter.instances
    [call] = executor.calls
    result = summary["stage_results"][0]
    assert call["activity_name"] == "import_to_influx"
    assert call["task_queue"] == "queue-influx"
    assert writer.args["batch_size"] == "2"
    assert writer.args["delete_after_influx"] is True
    assert [len(batch) for batch in writer.batches] == [2, 1]
    assert writer.batches[0][0] == EXPECTED_M1_LINE
    assert writer.closed
    assert summary["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert summary["progress"]["completed_children"] == 1
    assert result["stage"] == "import_to_influx"
    assert result["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert result["metrics"]["batch_count"] == 2
    assert result["metrics"]["line_count"] == 3
    assert result["metrics"]["heartbeat_count"] == 2
    assert result["metrics"]["idempotency_key"].endswith(":2")
    assert not (tmp_path / CACHE_FILENAME).exists()
    assert not (tmp_path / "DAT_ASCII_EURUSD_M1_201202.zip").exists()


def test_import_workflow_contract_classifies_retryable_influx_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Workflow-level Influx failures should preserve retry metadata."""
    _FakeInfluxWriter.instances.clear()
    _FakeInfluxWriter.fail_with = OSError("temporary influx failure")
    monkeypatch.setattr(
        activities,
        "_influx_batch_writer",
        _FakeInfluxWriter,
    )
    payload = _influx_workflow_payload(tmp_path, batch_size="2")
    executor = _LocalImportActivityExecutor()
    workflow = workflows.ImportWorkflow(activity_executor=executor)

    with pytest.raises(ApplicationError) as raised:
        asyncio.run(workflow.run(payload))

    [writer] = _FakeInfluxWriter.instances
    [detail] = raised.value.details
    result = detail["stage_result"]
    failure = result["failure"]
    assert writer.closed
    assert writer.batches == []
    assert raised.value.type == "INFLUX_IMPORT_RETRYABLE"
    assert raised.value.non_retryable is False
    assert result["status"] == WorkStatus.RETRIED.value
    assert failure["code"] == "INFLUX_IMPORT_RETRYABLE"
    assert failure["retryable"] is True
    assert failure["detail"]["idempotent_retry"] is True
    assert detail["retry_policy"]["name"] == "idempotent_write"
    assert (tmp_path / CACHE_FILENAME).exists()
    _FakeInfluxWriter.fail_with = None


def _influx_workflow_payload(
    tmp_path: Path,
    *,
    batch_size: str,
    delete_after_influx: bool = False,
) -> dict[str, Any]:
    _write_fixture_cache(tmp_path)
    zip_filename = "DAT_ASCII_EURUSD_M1_201202.zip"
    if delete_after_influx:
        (tmp_path / zip_filename).write_bytes(b"zip placeholder")
    request = RunRequest(
        request_id="run-influx-contract",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        data_directory=str(tmp_path),
        batch_size=batch_size,
        import_to_influxdb=True,
        delete_after_influx=delete_after_influx,
        metadata={
            TASK_QUEUE_METADATA_KEY: {
                "orchestration": "queue-orchestration",
                "network": "queue-network",
                "cpu_file": "queue-cpu-file",
                "influx": "queue-influx",
            },
            "influx_contract": True,
        },
    )
    return {
        "request": request.to_dict(),
        "workflow_name": "ImportWorkflow",
        "workflow_id": "run-influx-contract-import",
        "partition": {
            "pair": "EURUSD",
            "timeframe": "M1",
            "format": "ascii",
        },
        "work_items": [
            {
                "work_id": "work-influx-contract",
                "status": WorkStatus.CACHE_READY.value,
                "data_dir": f"{tmp_path}/",
                "cache_filename": CACHE_FILENAME,
                "cache_line_count": "3",
                "cache_start": "1328072400000",
                "cache_end": "1328072520000",
                "zip_filename": zip_filename,
                "data_format": "ascii",
                "data_timeframe": "M1",
                "data_fxpair": "eurusd",
            }
        ],
    }


def _write_fixture_cache(tmp_path: Path) -> None:
    source = convert_polars_datetime_to_utc_ms(
        read_ascii_file_to_polars(
            FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
            "M1",
        ),
        "M1",
    )
    write_polars_cache(source, tmp_path / CACHE_FILENAME)
