"""Tests for Temporal sidecar activity wrappers."""

from __future__ import annotations

import asyncio
from importlib import import_module
import io
import shutil
import zipfile
from pathlib import Path
from typing import Any, get_type_hints

import pytest
from temporalio.exceptions import ApplicationError

from histdatacom.activity_stages import UrlValidationError
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.manifest_store import (
    DATASET_PLAN_BATCHES_KEY,
    DATASET_PLAN_REF_KEY,
    ManifestStatusStore,
    STATUS_STORE_REF_KEY,
)
from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.control import SidecarJobSnapshot
from histdatacom.sidecar.activities import (
    build_cache_activity,
    dataset_plan_activity,
    default_activities,
    download_archives_activity,
    extract_csv_activity,
    import_to_influx_activity,
    merge_cache_activity,
    repository_refresh_activity,
    validate_urls_activity,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]
EXPECTED_M1_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
    "1328072400000"
)
EXPECTED_TICK_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=T "
    "bidquote=1.3066,askquote=1.30677 1328072403660"
)


class FakeInfluxWriter:
    """Context-managed Influx writer test double."""

    instances: list["FakeInfluxWriter"] = []
    fail_with: Exception | None = None

    def __init__(self, args: dict) -> None:
        self.args = dict(args)
        self.batches: list[list[str]] = []
        self.closed = False
        self.instances.append(self)

    def __enter__(self) -> "FakeInfluxWriter":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def write_lines(self, lines: list[str]) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.batches.append(list(lines))


def _form_html(*, token: str = "token") -> str:
    """Return a minimal HistData download form."""
    return f"""
    <html>
      <form id="file_down">
        <input id="tk" value="{token}">
        <input id="date" value="2022">
        <input id="datemonth" value="2022">
        <input id="platform" value="ASCII">
        <input id="timeframe" value="M1">
        <input id="fxpair" value="eurusd">
      </form>
    </html>
    """


def _validation_payload(tmp_path) -> dict:
    """Return a minimal validation activity payload."""
    request = RunRequest(
        request_id="run-validate",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-validation",
            "status": WorkStatus.URL_NEW.value,
            "url": (
                "http://www.histdata.com/download-free-forex-data/"
                "?/ascii/1-minute-bar-quotes/eurusd/2022"
            ),
        },
    }


def _zip_bytes() -> bytes:
    """Return a minimal valid ZIP payload."""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_M1_2022.csv", "rows")
    return stream.getvalue()


def _download_payload(tmp_path) -> dict:
    """Return a minimal download activity payload with existing ZIP."""
    zip_path = tmp_path / "DAT_ASCII_EURUSD_M1_2022.zip"
    zip_path.write_bytes(_zip_bytes())
    request = RunRequest(
        request_id="run-download",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-download",
            "status": WorkStatus.URL_VALID.value,
            "url": (
                "http://www.histdata.com/download-free-forex-data/"
                "?/ascii/1-minute-bar-quotes/eurusd/2022"
            ),
            "data_dir": f"{tmp_path}/",
            "zip_filename": zip_path.name,
        },
    }


def _extraction_payload(tmp_path) -> dict:
    """Return a minimal extraction activity payload with existing ZIP."""
    zip_path = tmp_path / "DAT_ASCII_EURUSD_M1_2022.zip"
    zip_path.write_bytes(_zip_bytes())
    request = RunRequest(
        request_id="run-extract",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-extract",
            "status": WorkStatus.CSV_ZIP.value,
            "data_dir": f"{tmp_path}/",
            "zip_filename": zip_path.name,
        },
    }


def _cache_payload(tmp_path) -> dict:
    """Return a minimal cache activity payload with an existing CSV."""
    filename = "DAT_ASCII_EURUSD_M1_201202.csv"
    shutil.copyfile(FIXTURES / filename, tmp_path / filename)
    request = RunRequest(
        request_id="run-cache",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-cache",
            "status": WorkStatus.CSV_FILE.value,
            "data_dir": f"{tmp_path}/",
            "csv_filename": filename,
            "zip_filename": "missing.zip",
            "data_format": "ascii",
            "data_timeframe": "M1",
            "data_fxpair": "eurusd",
        },
    }


def _merge_payload(tmp_path) -> dict:
    """Return a merge activity payload with two cache artifacts."""
    source = convert_polars_datetime_to_utc_ms(
        read_ascii_file_to_polars(
            FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
            "M1",
        ),
        "M1",
    )
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    write_polars_cache(source.slice(0, 1), first_dir / CACHE_FILENAME)
    write_polars_cache(source.slice(1, 2), second_dir / CACHE_FILENAME)
    request = RunRequest(
        request_id="run-merge",
        data_directory=str(tmp_path),
        api_return_type="polars",
    )
    return {
        "request": request.to_dict(),
        "work_items": [
            {
                "work_id": "work-second",
                "status": WorkStatus.CACHE_READY.value,
                "data_dir": f"{second_dir}/",
                "cache_filename": CACHE_FILENAME,
                "cache_line_count": "2",
                "cache_start": str(EXPECTED_M1_DATETIMES[1]),
                "cache_end": str(EXPECTED_M1_DATETIMES[2]),
                "data_format": "ascii",
                "data_timeframe": "M1",
                "data_fxpair": "eurusd",
            },
            {
                "work_id": "work-first",
                "status": WorkStatus.CACHE_READY.value,
                "data_dir": f"{first_dir}/",
                "cache_filename": CACHE_FILENAME,
                "cache_line_count": "1",
                "cache_start": str(EXPECTED_M1_DATETIMES[0]),
                "cache_end": str(EXPECTED_M1_DATETIMES[0]),
                "data_format": "ascii",
                "data_timeframe": "M1",
                "data_fxpair": "eurusd",
            },
        ],
    }


def _influx_payload(
    tmp_path,
    *,
    timeframe: str = "M1",
    batch_size: str = "2",
    delete_after_influx: bool = False,
) -> dict:
    """Return an Influx activity payload with a cache artifact."""
    filename = f"DAT_ASCII_EURUSD_{timeframe}_201202.csv"
    source = convert_polars_datetime_to_utc_ms(
        read_ascii_file_to_polars(FIXTURES / filename, timeframe),
        timeframe,
    )
    write_polars_cache(source, tmp_path / CACHE_FILENAME)
    zip_filename = f"DAT_ASCII_EURUSD_{timeframe}_201202.zip"
    if delete_after_influx:
        (tmp_path / zip_filename).write_bytes(_zip_bytes())
    request = RunRequest(
        request_id=f"run-influx-{timeframe}",
        data_directory=str(tmp_path),
        batch_size=batch_size,
        import_to_influxdb=True,
        delete_after_influx=delete_after_influx,
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": f"work-influx-{timeframe}",
            "status": WorkStatus.CACHE_READY.value,
            "data_dir": f"{tmp_path}/",
            "cache_filename": CACHE_FILENAME,
            "cache_line_count": "3",
            "cache_start": str(EXPECTED_M1_DATETIMES[0]),
            "cache_end": str(EXPECTED_M1_DATETIMES[-1]),
            "zip_filename": zip_filename,
            "data_format": "ascii",
            "data_timeframe": timeframe,
            "data_fxpair": "eurusd",
        },
    }


def _decode_with_temporal_converter(
    converter: Any,
    payload: dict[str, Any],
    *,
    type_hint: type,
) -> dict[str, Any]:
    async def round_trip() -> dict[str, Any]:
        encoded = await converter.encode([payload])
        [decoded] = await converter.decode(encoded, type_hints=[type_hint])
        return decoded

    decoded_payload = asyncio.run(round_trip())
    assert isinstance(decoded_payload, dict)
    return decoded_payload


def test_repository_refresh_activity_returns_stage_result(
    monkeypatch,
    tmp_path,
) -> None:
    """Repository refresh should be callable as a registered activity."""
    remote_repo = {
        "eurusd": {"start": "200005", "end": "202212"},
        "hash": "remote",
        "hash_utc": 10.0,
    }
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_repository_data_from_url",
        lambda url: remote_repo,
    )
    request = RunRequest(
        request_id="run-repo",
        pairs=("eurusd",),
        data_directory=str(tmp_path),
        available_remote_data=True,
    )

    result = repository_refresh_activity({"request": request.to_dict()})

    assert result["stage"] == "repository_refresh"
    assert result["status"] == WorkStatus.COMPLETED.value
    assert result["metrics"]["available_data"] == {
        "eurusd": {"start": "200005", "end": "202212"}
    }
    assert result["artifacts"][0]["kind"] == "repository"


def test_dataset_plan_activity_returns_explicit_work_items(tmp_path) -> None:
    """Dataset planning should be callable as a registered activity."""
    request = RunRequest(
        request_id="run-plan",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202203",
        data_directory=str(tmp_path),
    )

    result = dataset_plan_activity({"request": request.to_dict()})

    assert result["result"]["stage"] == "dataset_plan"
    assert result["result"]["status"] == WorkStatus.COMPLETED.value
    assert result["result"]["metrics"]["work_item_count"] == 1
    assert len(result["work_items"]) == 1
    assert result["work_items"][0]["url"] == (
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/eurusd/2022"
    )
    assert result["work_items"][0]["data_datemonth"] == "2022"


def test_dataset_plan_activity_spills_large_plan_to_manifest(
    tmp_path: Path,
) -> None:
    """Large dataset plans should return references instead of full items."""
    request = RunRequest(
        request_id="run-plan-spill",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        start_yearmonth="202201",
        end_yearmonth="202203",
        data_directory=str(tmp_path),
        metadata={
            "temporal_plan_spill": {"inline_work_item_limit": 2},
            "temporal_batching": {"max_work_items_per_batch": 1},
        },
    )

    result = dataset_plan_activity({"request": request.to_dict()})

    assert "work_items" not in result
    assert result[DATASET_PLAN_REF_KEY]["work_item_count"] == 3
    assert len(result[DATASET_PLAN_BATCHES_KEY]) == 3
    assert result["result"]["metrics"]["work_items_spilled"] is True
    store = ManifestStatusStore(str(tmp_path))
    loaded = store.get_dataset_plan_work_items(
        str(result[DATASET_PLAN_REF_KEY]["plan_id"])
    )
    assert [item.data_datemonth for item in loaded] == [
        "202201",
        "202202",
        "202203",
    ]


def test_validate_urls_activity_loads_work_items_from_plan_ref(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Leaf activities should hydrate only their referenced batch."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )
    request = RunRequest(
        request_id="run-plan-load",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("T",),
        start_yearmonth="202201",
        end_yearmonth="202203",
        data_directory=str(tmp_path),
        validate_urls=True,
        metadata={
            "temporal_plan_spill": {"inline_work_item_limit": 2},
            "temporal_batching": {"max_work_items_per_batch": 1},
        },
    )
    plan_payload = dataset_plan_activity({"request": request.to_dict()})
    partition = plan_payload[DATASET_PLAN_BATCHES_KEY][1]

    result = validate_urls_activity(
        {
            "request": request.to_dict(),
            "partition": partition,
            DATASET_PLAN_REF_KEY: plan_payload[DATASET_PLAN_REF_KEY],
            "workflow_id": "validate-from-plan-ref",
        }
    )

    assert str(result["work_item"]["url"]).endswith("/2022/2")
    assert result["work_item"]["status"] == WorkStatus.URL_VALID.value
    assert result["result"]["metrics"]["progress"]["total"] == 1.0


def test_dataset_plan_activity_payload_survives_temporal_converter(
    tmp_path,
) -> None:
    """Temporal's data converter must preserve nested request payloads."""
    converter_module = import_module("temporalio.converter")
    request = RunRequest(
        request_id="run-plan-converter",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        data_directory=str(tmp_path),
        metadata={
            "requests_timeout": "30",
            "temporal_batching": {"max_work_items_per_batch": 1},
        },
    )
    payload = {"request": request.to_dict(), "stage": "dataset_plan"}
    parameter_hint = get_type_hints(dataset_plan_activity)["payload"]

    decoded = _decode_with_temporal_converter(
        converter_module.default(),
        payload,
        type_hint=parameter_hint,
    )
    result = dataset_plan_activity(decoded)

    assert result["result"]["metrics"]["work_item_count"] == 1
    assert len(result["work_items"]) == 1
    assert result["work_items"][0]["data_fxpair"] == "eurusd"


def test_validate_urls_activity_returns_form_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """URL validation should be callable as a sidecar activity."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )
    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["stage"] == "validate_url"
    assert result["result"]["status"] == WorkStatus.URL_VALID.value
    assert result["work_item"]["data_tk"] == "token"
    assert result["work_item"]["encoding"] == "gzip"


def test_validate_urls_activity_emits_progress_heartbeat(
    monkeypatch,
    tmp_path,
) -> None:
    """Activities should publish GUI-ready progress heartbeat metadata."""
    import histdatacom.sidecar.activities as activities

    heartbeats: list[dict] = []
    monkeypatch.setattr(
        activities.activity,
        "heartbeat",
        heartbeats.append,
        raising=False,
    )
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert heartbeats[-1]["event_type"] == "progress"
    assert heartbeats[-1]["stage"] == "validate_url"
    assert heartbeats[-1]["status"] == WorkStatus.URL_VALID.value
    assert heartbeats[-1]["completed"] == 1.0
    assert heartbeats[-1]["total"] == 1.0
    assert result["result"]["events"][-1]["metadata"]["event_type"] == (
        "progress"
    )
    assert result["result"]["metrics"]["progress"]["stage"] == "validate_url"


def test_validate_urls_activity_persists_live_status_without_inspect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Activities should persist progress before any client inspect query."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )
    store = ManifestStatusStore(tmp_path / "sidecar-status")
    request = RunRequest(
        request_id="run-live-status",
        data_directory=str(tmp_path / "data"),
        metadata={
            STATUS_STORE_REF_KEY: store.status_store_ref(),
            "sidecar_task_queues": {"orchestration": "test-queue"},
        },
    )
    store.write_job_snapshot(
        {
            "job_id": "histdatacom-run-live-status",
            "request_id": request.request_id,
            "workflow_id": "histdatacom-run-live-status",
            "lifecycle": "submitted",
            "status": WorkStatus.PLANNED.value,
            "metadata": {"run_request": request.to_dict()},
        }
    )
    payload = {
        "request": request.to_dict(),
        "workflow_id": "validate-child-workflow",
        "work_item": {
            "work_id": "work-live-status",
            "status": WorkStatus.URL_NEW.value,
            "url": (
                "http://www.histdata.com/download-free-forex-data/"
                "?/ascii/1-minute-bar-quotes/eurusd/2022"
            ),
        },
    }

    result = validate_urls_activity(payload)

    assert result["result"]["status"] == WorkStatus.URL_VALID.value
    stored_item = store.get_work_item("work-live-status")
    assert stored_item is not None
    assert stored_item.status == WorkStatus.URL_VALID
    stage_results = store.list_stage_results("work-live-status")
    assert stage_results[-1]["stage"] == "validate_url"
    stored_snapshot = store.get_job_snapshot("histdatacom-run-live-status")
    assert stored_snapshot is not None
    snapshot = SidecarJobSnapshot.from_dict(stored_snapshot)
    assert snapshot.progress is not None
    assert snapshot.lifecycle.value == "running"
    assert snapshot.progress.current_stage == "validate_url"
    assert snapshot.progress.completed_children == 1
    assert snapshot.logs[-1].source == "validate_url"
    assert snapshot.metadata["activity_workflow_id"] == (
        "validate-child-workflow"
    )
    assert snapshot.metadata["run_request"]["request_id"] == "run-live-status"
    assert snapshot.result["stage_result_count"] == 1


def test_activity_context_helpers_ignore_absent_temporal_context(
    monkeypatch,
) -> None:
    """Temporal API probes should be local-call safe outside a worker."""
    import histdatacom.sidecar.activities as activities

    def heartbeat(metadata: dict) -> None:
        raise RuntimeError("Not in activity context")

    def is_cancelled() -> bool:
        raise RuntimeError("Not in activity context")

    monkeypatch.setattr(
        activities.activity,
        "heartbeat",
        heartbeat,
        raising=False,
    )
    monkeypatch.setattr(
        activities.activity,
        "is_cancelled",
        is_cancelled,
        raising=False,
    )

    activities._activity_heartbeat({"stage": "validate_url"})

    assert activities._activity_cancelled() is False


def test_validate_urls_activity_returns_no_data(monkeypatch, tmp_path) -> None:
    """Missing tokens should flow through the registered activity."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(token=""),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["status"] == WorkStatus.URL_NO_REPO_DATA.value
    assert result["result"]["failure"] is None
    assert not result["forward"]


def test_validate_urls_activity_returns_failed(monkeypatch, tmp_path) -> None:
    """Malformed validation failures should flow through the activity."""

    def fetch(url: str, timeout: int):
        raise UrlValidationError("MALFORMED_HEADERS", "missing length")

    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        fetch,
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["status"] == WorkStatus.FAILED.value
    assert result["result"]["failure"]["code"] == "MALFORMED_HEADERS"
    assert not result["result"]["failure"]["retryable"]


def test_validate_urls_activity_raises_retryable_temporal_error(
    monkeypatch,
    tmp_path,
) -> None:
    """Retryable validation failures should be retried by Temporal."""

    def fetch(url: str, timeout: int):
        raise UrlValidationError(
            "URL_FETCH_RETRYABLE",
            "timeout",
            retryable=True,
        )

    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        fetch,
    )

    with pytest.raises(ApplicationError) as raised:
        validate_urls_activity(_validation_payload(tmp_path))

    assert raised.value.type == "URL_FETCH_RETRYABLE"
    assert raised.value.non_retryable is False
    [detail] = raised.value.details
    assert detail["stage_result"]["status"] == WorkStatus.RETRIED.value
    assert detail["stage_result"]["failure"]["retryable"] is True
    assert detail["retry_policy"]["name"] == "network"


def test_validate_urls_activity_returns_cancelled_when_requested(
    monkeypatch,
    tmp_path,
) -> None:
    """Activity cancellation should skip future work and emit metadata."""
    import histdatacom.sidecar.activities as activities

    monkeypatch.setattr(
        activities.activity,
        "is_cancelled",
        lambda: True,
        raising=False,
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["work_item"]["status"] == WorkStatus.CANCELLED.value
    assert result["result"]["status"] == WorkStatus.CANCELLED.value
    assert result["result"]["failure"]["code"] == "OPERATION_CANCELLED"
    assert result["result"]["metrics"]["cancelled"] is True
    assert result["result"]["metrics"]["resume_policy"]["stage"] == (
        "validate_url"
    )
    cancellation = result["result"]["events"][-1]["metadata"]["cancellation"]
    assert cancellation["stops_future_work"] is True


def test_download_archives_activity_reuses_existing_zip(tmp_path) -> None:
    """Archive downloads should be callable as a registered activity."""
    result = download_archives_activity(_download_payload(tmp_path))

    assert result["result"]["stage"] == "download_archive"
    assert result["result"]["status"] == WorkStatus.CSV_ZIP.value
    assert result["result"]["metrics"]["decision"] == "reuse_existing"
    assert result["work_item"]["status"] == WorkStatus.CSV_ZIP.value
    assert result["result"]["artifacts"][0]["kind"] == "zip"
    assert result["result"]["artifacts"][0]["sha256"]


def test_download_archives_activity_removes_partial_temp_on_cancel(
    monkeypatch,
    tmp_path,
) -> None:
    """Cancellation should clean hidden temp files and preserve complete ZIPs."""
    import histdatacom.sidecar.activities as activities

    payload = _download_payload(tmp_path)
    zip_path = tmp_path / payload["work_item"]["zip_filename"]
    partial = tmp_path / ".DAT_ASCII_EURUSD_M1_2022.zip.partial.tmp"
    partial.write_text("partial", encoding="utf-8")
    monkeypatch.setattr(
        activities.activity,
        "is_cancelled",
        lambda: True,
        raising=False,
    )

    result = download_archives_activity(payload)

    assert result["result"]["status"] == WorkStatus.CANCELLED.value
    assert result["result"]["metrics"]["resume_policy"]["stage"] == (
        "download_archive"
    )
    assert zip_path.exists()
    assert not partial.exists()
    assert any(
        item["path"] == str(partial) and item["removed"]
        for item in result["result"]["metrics"]["cleanup"]
    )


def test_extract_csv_activity_extracts_existing_zip(tmp_path) -> None:
    """Archive extraction should be callable as a registered activity."""
    result = extract_csv_activity(_extraction_payload(tmp_path))

    assert result["result"]["stage"] == "extract_csv"
    assert result["result"]["status"] == WorkStatus.CSV_FILE.value
    assert result["result"]["metrics"]["decision"] == "extracted"
    assert result["result"]["metrics"]["zip_deleted"] is True
    assert result["work_item"]["status"] == WorkStatus.CSV_FILE.value
    assert result["work_item"]["csv_filename"] == (
        "DAT_ASCII_EURUSD_M1_2022.csv"
    )
    assert result["result"]["artifacts"][0]["kind"] == "csv"
    assert result["result"]["artifacts"][0]["sha256"]


def test_build_cache_activity_builds_polars_cache(tmp_path) -> None:
    """Polars cache builds should be callable as a registered activity."""
    result = build_cache_activity(_cache_payload(tmp_path))

    assert result["result"]["stage"] == "build_cache"
    assert result["result"]["status"] == WorkStatus.CACHE_READY.value
    assert result["result"]["metrics"]["decision"] == "built"
    assert result["result"]["metrics"]["cache_line_count"] == 3
    assert result["result"]["metrics"]["cache_start"] == str(
        EXPECTED_M1_DATETIMES[0]
    )
    assert result["result"]["metrics"]["cache_end"] == str(
        EXPECTED_M1_DATETIMES[-1]
    )
    assert result["result"]["metrics"]["timeframe"] == "M1"
    assert result["result"]["artifacts"][0]["kind"] == "cache"
    assert result["work_item"]["status"] == WorkStatus.CACHE_READY.value
    assert result["work_item"]["cache_filename"] == CACHE_FILENAME
    assert result["work_item"]["cache_line_count"] == "3"
    assert (tmp_path / CACHE_FILENAME).exists()


def test_merge_cache_activity_returns_bounded_merge_metadata(tmp_path) -> None:
    """Cache merge should return summaries and refs, not frame payloads."""
    result = merge_cache_activity(_merge_payload(tmp_path))

    assert result["result"]["stage"] == "merge_cache"
    assert result["result"]["status"] == WorkStatus.COMPLETED.value
    assert result["result"]["metrics"]["record_count"] == 2
    assert result["result"]["metrics"]["set_count"] == 1
    assert result["result"]["metrics"]["materialized"] is False
    assert "data" not in result
    merge_set = result["merge_sets"][0]
    assert merge_set["timeframe"] == "M1"
    assert merge_set["pair"] == "eurusd"
    assert merge_set["line_count"] == 3
    assert merge_set["start"] == str(EXPECTED_M1_DATETIMES[0])
    assert merge_set["end"] == str(EXPECTED_M1_DATETIMES[2])
    assert [artifact["kind"] for artifact in merge_set["artifacts"]] == [
        "cache",
        "cache",
    ]


def test_import_to_influx_activity_writes_m1_batches(
    monkeypatch,
    tmp_path,
) -> None:
    """Influx activity should write bounded M1 line-protocol batches."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    result = import_to_influx_activity(_influx_payload(tmp_path))

    [writer] = FakeInfluxWriter.instances
    assert writer.args["batch_size"] == "2"
    assert [len(batch) for batch in writer.batches] == [2, 1]
    assert writer.batches[0][0] == EXPECTED_M1_LINE
    assert writer.closed
    assert result["work_item"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert result["result"]["stage"] == "import_to_influx"
    assert result["result"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert result["result"]["metrics"]["batch_count"] == 2
    assert result["result"]["metrics"]["line_count"] == 3
    assert result["result"]["metrics"]["heartbeat_count"] == 2
    assert "data" not in result


def test_import_to_influx_activity_writes_tick_batches(
    monkeypatch,
    tmp_path,
) -> None:
    """Influx activity should preserve tick bid/ask line formatting."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    result = import_to_influx_activity(
        _influx_payload(tmp_path, timeframe="T", batch_size="2")
    )

    [writer] = FakeInfluxWriter.instances
    assert [len(batch) for batch in writer.batches] == [2, 1]
    assert writer.batches[0][0] == EXPECTED_TICK_LINE
    assert result["result"]["metrics"]["line_count"] == 3


def test_import_to_influx_activity_raises_retryable_write_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """Writer failures should be explicit and retried by Temporal."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = OSError("temporary influx failure")
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    with pytest.raises(ApplicationError) as raised:
        import_to_influx_activity(_influx_payload(tmp_path))

    assert raised.value.type == "INFLUX_IMPORT_RETRYABLE"
    assert raised.value.non_retryable is False
    [detail] = raised.value.details
    assert detail["stage_result"]["status"] == WorkStatus.RETRIED.value
    assert detail["stage_result"]["failure"]["retryable"] is True
    assert (
        detail["stage_result"]["failure"]["detail"]["idempotent_retry"] is True
    )
    assert detail["retry_policy"]["name"] == "idempotent_write"
    FakeInfluxWriter.fail_with = None


def test_import_to_influx_activity_reports_optional_dependency_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """Missing Influx extra should remain explicit under the activity path."""

    def missing_influx_writer(args: dict) -> FakeInfluxWriter:  # noqa: ARG001
        raise SystemExit(
            "InfluxDB import not installed. please run:\n\n  "
            "pip install histdatacom[influx]"
        )

    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        missing_influx_writer,
    )

    result = import_to_influx_activity(_influx_payload(tmp_path))

    assert result["work_item"]["status"] == WorkStatus.FAILED.value
    assert result["result"]["failure"]["code"] == (
        "INFLUX_OPTIONAL_DEPENDENCY_MISSING"
    )
    assert "histdatacom[influx]" in result["result"]["failure"]["message"]


def test_import_to_influx_activity_honors_delete_after_influx(
    monkeypatch,
    tmp_path,
) -> None:
    """Successful imports should preserve existing cleanup behavior."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )
    payload = _influx_payload(tmp_path, delete_after_influx=True)
    zip_path = tmp_path / payload["work_item"]["zip_filename"]
    cache_path = tmp_path / CACHE_FILENAME

    result = import_to_influx_activity(payload)

    assert result["result"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert not zip_path.exists()
    assert not cache_path.exists()


def test_default_activities_register_operation_activities() -> None:
    """The worker default activity set should include migrated activities."""
    assert default_activities() == (
        repository_refresh_activity,
        dataset_plan_activity,
        validate_urls_activity,
        download_archives_activity,
        extract_csv_activity,
        build_cache_activity,
        merge_cache_activity,
        import_to_influx_activity,
    )
