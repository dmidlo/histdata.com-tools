"""Tests for the queue-free foreground runtime."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from histdatacom import config
from histdatacom.activity_stages import (
    ActivityStageOutput,
    DatasetPlanOutput,
)
from histdatacom.foreground import run_foreground
from histdatacom.runtime_contracts import (
    RunRequest,
    StageResult,
    WorkItem,
    WorkStatus,
)


def _args(tmp_path: Path) -> dict[str, Any]:
    """Return minimal foreground args."""
    return {
        "default_download_dir": f"{tmp_path}/",
        "from_api": True,
        "by": "pair_asc",
        "zip_persist": False,
    }


def _work_item(status: WorkStatus = WorkStatus.URL_NEW) -> WorkItem:
    """Return a representative dataset work item."""
    return WorkItem(
        work_id="work-eurusd-m1",
        status=status,
        url="http://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes/eurusd/2022/1",
        data_format="ASCII",
        data_timeframe="M1",
        data_fxpair="eurusd",
        data_datemonth="202201",
    )


def _stage_output(
    item: WorkItem,
    *,
    stage: str,
    status: WorkStatus,
    forward: bool = True,
) -> ActivityStageOutput:
    """Return a minimal activity stage output."""
    updated = item.with_status(status)
    return ActivityStageOutput(
        work_item=updated,
        result=StageResult(
            work_id=updated.work_id,
            stage=stage,
            status=status,
        ),
        forward=forward,
    )


def _plan_output(item: WorkItem) -> DatasetPlanOutput:
    """Return a minimal dataset plan output."""
    return DatasetPlanOutput(
        work_items=(item,),
        result=StageResult(
            work_id="run-test",
            stage="dataset_plan",
            status=WorkStatus.COMPLETED,
        ),
    )


def test_foreground_api_return_uses_explicit_stage_sequence(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """API cache returns should not depend on repository refresh or queues."""
    import histdatacom.foreground as foreground

    calls: list[str] = []
    planned = _work_item()

    def plan_stage(**kwargs: object) -> DatasetPlanOutput:
        calls.append(f"plan:{tuple(kwargs['pairs'])}")
        return _plan_output(planned)

    def validate(
        item: WorkItem, *, args: dict[str, Any]
    ) -> ActivityStageOutput:
        calls.append("validate")
        return _stage_output(
            item,
            stage="validate_url",
            status=WorkStatus.URL_VALID,
        )

    def download(
        item: WorkItem, *, args: dict[str, Any]
    ) -> ActivityStageOutput:
        calls.append("download")
        return _stage_output(
            item,
            stage="download_archive",
            status=WorkStatus.CSV_ZIP,
        )

    def build_cache(
        item: WorkItem,
        *,
        args: dict[str, Any],
    ) -> ActivityStageOutput:
        calls.append("cache")
        return _stage_output(
            item,
            stage="build_cache",
            status=WorkStatus.CACHE_READY,
        )

    def merge(
        work_items: Sequence[WorkItem],
        *,
        return_type: str,
        materialize: bool,
    ) -> Any:
        calls.append(f"merge:{return_type}:{materialize}:{len(work_items)}")
        return type(
            "MergeOutput",
            (),
            {
                "data": "merged",
                "result": StageResult(
                    work_id="run-test",
                    stage="merge_cache",
                    status=WorkStatus.COMPLETED,
                ),
            },
        )()

    monkeypatch.setattr(config, "FILTER_PAIRS", None)
    monkeypatch.setattr(
        foreground,
        "repository_refresh_stage",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("repo refresh should not run")
        ),
    )
    monkeypatch.setattr(foreground, "dataset_plan_stage", plan_stage)
    monkeypatch.setattr(foreground, "validate_url_work_item", validate)
    monkeypatch.setattr(foreground, "download_archive_work_item", download)
    monkeypatch.setattr(foreground, "build_cache_work_item", build_cache)
    monkeypatch.setattr(
        foreground,
        "extract_csv_work_item",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("API cache return should not extract CSVs")
        ),
    )
    monkeypatch.setattr(foreground, "merge_cache_work_items", merge)

    request = RunRequest(
        request_id="run-test",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        api_return_type="polars",
        validate_urls=True,
        download_data_archives=True,
        extract_csvs=True,
    )

    result = run_foreground(request, _args(tmp_path))

    assert result == "merged"
    assert calls == [
        "plan:('eurusd',)",
        "validate",
        "download",
        "cache",
        "merge:polars:True:1",
    ]


def test_foreground_import_uses_direct_influx_writer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Influx imports should use direct line writes from explicit work items."""
    import histdatacom.foreground as foreground

    calls: list[str] = []
    planned = _work_item()

    class FakeWriter:
        """Capture foreground Influx writes."""

        def __init__(self, args: dict[str, Any]) -> None:
            self.args = dict(args)

        def __enter__(self) -> "FakeWriter":
            calls.append("writer_open")
            return self

        def __exit__(self, *exc_info: object) -> None:
            calls.append("writer_close")

        def write_lines(self, lines: list[str]) -> None:
            calls.append(f"write:{lines[0]}")

    def plan_stage(**kwargs: object) -> DatasetPlanOutput:
        calls.append("plan")
        return _plan_output(planned)

    def validate(
        item: WorkItem, *, args: dict[str, Any]
    ) -> ActivityStageOutput:
        calls.append("validate")
        return _stage_output(
            item,
            stage="validate_url",
            status=WorkStatus.URL_VALID,
        )

    def download(
        item: WorkItem, *, args: dict[str, Any]
    ) -> ActivityStageOutput:
        calls.append("download")
        return _stage_output(
            item,
            stage="download_archive",
            status=WorkStatus.CSV_ZIP,
        )

    def import_to_influx(
        item: WorkItem,
        *,
        args: dict[str, Any],
        emit_lines,
    ) -> ActivityStageOutput:
        calls.append("import")
        emit_lines(["eurusd value=1 1328072400000"])
        return _stage_output(
            item,
            stage="import_to_influx",
            status=WorkStatus.INFLUX_UPLOAD,
        )

    monkeypatch.setattr(config, "FILTER_PAIRS", None)
    monkeypatch.setattr(foreground, "dataset_plan_stage", plan_stage)
    monkeypatch.setattr(foreground, "validate_url_work_item", validate)
    monkeypatch.setattr(foreground, "download_archive_work_item", download)
    monkeypatch.setattr(foreground, "InfluxBatchWriter", FakeWriter)
    monkeypatch.setattr(
        foreground,
        "import_to_influx_work_item",
        import_to_influx,
    )

    request = RunRequest(
        request_id="run-test",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        validate_urls=True,
        download_data_archives=True,
        import_to_influxdb=True,
    )

    result = run_foreground(request, _args(tmp_path))

    assert result is None
    assert calls == [
        "plan",
        "validate",
        "download",
        "writer_open",
        "import",
        "write:eurusd value=1 1328072400000",
        "writer_close",
    ]
