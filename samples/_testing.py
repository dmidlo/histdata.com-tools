"""Hermetic support for executing user-facing samples in tests."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl

import histdatacom.histdata_com as histdata_app
from histdatacom.histdata_ascii import CACHE_FILENAME, write_polars_cache
from histdatacom.orchestration.client import JobHandle, JobResult

_TEMP_WORKSPACES: list[TemporaryDirectory[str]] = []


def install_fake_orchestration() -> dict[str, Any]:
    """Patch the API boundary so samples execute without network or Temporal."""
    workspace = TemporaryDirectory()
    _TEMP_WORKSPACES.append(workspace)
    cache_path = Path(workspace.name) / CACHE_FILENAME
    source = sample_polars_frame()
    write_polars_cache(source, cache_path)
    captured: dict[str, Any] = {}

    def fake_submit(request: object, **kwargs: Any) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        artifact = {
            "kind": "cache",
            "path": str(cache_path),
            "metadata": {
                "filename": CACHE_FILENAME,
                "timeframe": "M1",
                "pair": "eurusd",
                "line_count": str(source.height),
                "start": str(source.item(0, "datetime")),
                "end": str(source.item(source.height - 1, "datetime")),
                "work_id": "work-sample-cache",
            },
        }
        return JobResult(
            handle=JobHandle(
                request_id="run-sample",
                workflow_id="histdatacom-run-sample",
                run_id="run-fake",
                task_queue="histdatacom.test.sample",
                namespace="default",
            ),
            status="completed",
            result={
                "workflow_name": "HistDataRunWorkflow",
                "status": "COMPLETED",
                "stage_results": [
                    {
                        "stage": "merge_cache",
                        "status": "COMPLETED",
                        "artifacts": [artifact],
                    }
                ],
            },
        )

    histdata_app.submit_run_request_and_observe_sync = fake_submit
    return captured


def sample_polars_frame() -> pl.DataFrame:
    """Return a tiny frame shaped like an M1 HistData API result."""
    return pl.DataFrame(
        {
            "datetime": [1328072400000, 1328072460000],
            "open": [1.3066, 1.3067],
            "high": [1.3068, 1.3069],
            "low": [1.3065, 1.3066],
            "close": [1.3067, 1.3068],
            "vol": [0, 0],
        }
    )
