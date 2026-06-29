"""Pytest unit tests for histdatacom.histdata_com.py."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import histdatacom
from histdatacom.exceptions import InfluxConfigurationError
from histdatacom.fx_enums import MAJOR_TRIANGLE_SYMBOLS
from histdatacom.options import Options
from histdatacom.runtime_contracts import WorkStatus
from histdatacom.orchestration.client import (
    JobHandle,
    JobResult,
    OrchestrationUnavailableError,
    RuntimeDependencyError,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    write_ascii_case,
    write_corrupt_zip,
    write_zip_case,
)


def test_histdata_com() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _orchestration_options(api_return_type: str = "polars") -> Options:
    """Return a small API request configured for orchestration execution."""
    options = Options()
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"
    options.api_return_type = api_return_type
    return options


def _orchestration_repository_options() -> Options:
    """Return an API repository request configured for orchestration execution."""
    options = Options()
    options.available_remote_data = True
    options.pairs = {"eurusd", "gbpusd"}
    options.by = "start_dsc"
    return options


def _job_result(*, status: str = "completed") -> JobResult:
    """Return a fake orchestration job result."""
    return JobResult(
        handle=JobHandle(
            request_id="run-test",
            workflow_id="histdatacom-run-test",
            run_id="run-fake",
            task_queue="histdatacom.test.orchestration",
            namespace="default",
        ),
        status=status,
        result={"workflow_name": "HistDataRunWorkflow"},
    )


def _orchestration_repository_result(
    *,
    status: str = "completed",
    failure_code: str = "",
    include_quality: bool = False,
) -> JobResult:
    """Return a completed orchestration result with repository metrics."""
    available_data = {
        "gbpusd": {"start": "200005", "end": "202212"},
        "eurusd": {"start": "200005", "end": "202212"},
    }
    if include_quality:
        available_data["eurusd"]["quality"] = {
            "status": "clean",
            "target_count": 2,
            "finding_count": 0,
            "report_artifact": {"path": "/tmp/quality.json"},
        }
    stage_result = {
        "stage": "RepositoryRefreshWorkflow",
        "status": status,
        "metrics": {
            "available_data": available_data,
            "filter_pairs": [],
            "repo_file_exists": True,
        },
        "failure": None,
    }
    if failure_code:
        stage_result["failure"] = {
            "code": failure_code,
            "message": "offline",
            "retryable": True,
            "detail": {},
        }
    return JobResult(
        handle=_job_result().handle,
        status="completed",
        result={
            "workflow_name": "HistDataRunWorkflow",
            "status": status,
            "stage_results": [stage_result],
            "artifacts": [],
        },
    )


def _orchestration_quality_result(
    *,
    status: str = WorkStatus.COMPLETED.value,
    target_count: int = 1,
    finding_count: int = 0,
    warning_count: int = 0,
    error_count: int = 0,
    exit_code: int = 0,
    report_path: str = "/tmp/quality.json",
    error: str = "",
) -> JobResult:
    """Return an orchestration result containing bounded quality metadata."""
    quality_status = (
        "failed" if error_count else "warning" if warning_count else "clean"
    )
    max_severity = (
        "error" if error_count else "warning" if warning_count else "info"
    )
    quality = {
        "operation": "data-quality",
        "check_groups": ["inventory"],
        "summary": {
            "target_count": target_count,
            "rule_count": 0,
            "finding_count": finding_count,
            "info_count": 0,
            "warning_count": warning_count,
            "error_count": error_count,
            "status": quality_status,
            "max_severity": max_severity,
        },
        "target_summaries": [
            {
                "target": {
                    "path": "/tmp/DAT_ASCII_EURUSD_M1_201202.csv",
                    "kind": "csv",
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "symbol": "EURUSD",
                    "period": "201202",
                    "metadata": {},
                },
                "rule_count": 1,
                "finding_count": finding_count,
                "info_count": 0,
                "warning_count": warning_count,
                "error_count": error_count,
                "status": quality_status,
                "max_severity": max_severity,
            }
            for _ in range(target_count)
        ],
        "report_schema_version": "histdatacom.quality-report.v1",
        "report_artifact": {
            "kind": "quality-report",
            "path": report_path,
            "size_bytes": 128,
            "sha256": "quality-sha",
            "metadata": {
                "target_count": target_count,
                "finding_count": finding_count,
                "warning_count": warning_count,
                "error_count": error_count,
            },
        },
        "exit_decision": {
            "exit_code": exit_code,
            "reason": (
                "quality error threshold exceeded"
                if exit_code
                else "quality report is within configured exit policy"
            ),
            "policy": {
                "fail_on": "error",
                "max_errors": 0,
                "max_warnings": 0,
            },
        },
    }
    if error:
        quality = {
            "operation": "data-quality",
            "report_schema_version": "histdatacom.quality-report.v1",
            "error": error,
        }
    stage_result = {
        "stage": "data_quality",
        "status": status,
        "metrics": {"quality": quality},
        "artifacts": (
            [quality["report_artifact"]] if "report_artifact" in quality else []
        ),
        "failure": None,
    }
    if status == WorkStatus.FAILED.value:
        stage_result["failure"] = {
            "code": "DATA_QUALITY_FAILED",
            "message": error or "quality error threshold exceeded",
            "retryable": False,
            "detail": {},
        }
    return JobResult(
        handle=_job_result().handle,
        status="completed",
        result={
            "workflow_name": "HistDataRunWorkflow",
            "status": status,
            "stage_results": [stage_result],
            "artifacts": stage_result["artifacts"],
        },
    )


def _orchestration_cache_result(tmp_path: Path) -> JobResult:
    """Return a completed orchestration result with a cache artifact."""
    from histdatacom.api import Api
    from histdatacom.histdata_ascii import CACHE_FILENAME, write_polars_cache

    source = Api._import_file_to_polars(
        SimpleNamespace(data_timeframe="M1"),
        Path("tests/fixtures/histdata_ascii/DAT_ASCII_EURUSD_M1_201202.csv"),
    )
    cache_path = tmp_path / CACHE_FILENAME
    write_polars_cache(source, cache_path)
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
            "work_id": "work-cache",
        },
    }
    return JobResult(
        handle=_job_result().handle,
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


def _orchestration_terminal_result(
    status: WorkStatus,
    tmp_path: Path | None = None,
) -> JobResult:
    """Return a failed or cancelled orchestration result payload."""
    message = (
        "validation failed"
        if status == WorkStatus.FAILED
        else "cancelled by caller"
    )
    failure = {
        "code": status.value,
        "message": message,
        "retryable": True,
        "detail": {},
    }
    stage_result: dict[str, object] = {
        "stage": "validate_urls",
        "status": status.value,
        "failure": failure,
    }
    if tmp_path is not None:
        cache_result = _orchestration_cache_result(tmp_path)
        stage_result["stage"] = "merge_cache"
        stage_result["artifacts"] = cache_result.result["stage_results"][0][
            "artifacts"
        ]
    return JobResult(
        handle=_job_result().handle,
        status=status.value.lower(),
        result={
            "workflow_name": "HistDataRunWorkflow",
            "status": status.value,
            "progress": {
                "workflow_name": "HistDataRunWorkflow",
                "status": status.value,
                "last_error": message,
            },
            "stage_results": [stage_result],
            "artifacts": [],
        },
    )


def test_api_options_can_submit_orchestration_job_and_return_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should submit orchestration-backed jobs by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_orchestration_options())

    assert result["status"] == "completed"
    assert result["result"] == {"workflow_name": "HistDataRunWorkflow"}
    assert captured["request"].pairs == ("eurusd",)
    assert captured["request"].api_return_type == "polars"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_api_options_can_keep_owned_orchestration_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers can request that an auto-started runtime stays running."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _orchestration_options()
    options.orchestration_keep_runtime = True

    result = histdata_com.main(options)

    assert result["status"] == "completed"
    assert captured["request"].pairs == ("eurusd",)
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
        "keep_runtime": True,
    }


def test_api_build_cache_submits_cache_only_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Options.build_cache should submit a cache-only orchestration request."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = Options()
    options.build_cache = True
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"tick-data-quotes"}
    options.start_yearmonth = "2022-12"
    options.data_directory = str(tmp_path)

    result = histdata_com.main(options)

    request = captured["request"]
    assert result["status"] == "completed"
    assert request.build_cache
    assert request.validate_urls
    assert request.download_data_archives
    assert not request.extract_csvs
    assert request.api_return_type == ""
    assert request.formats == ("ascii",)
    assert request.timeframes == ("T",)
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


@pytest.mark.parametrize(
    ("target_kind", "expected_count"),
    (
        ("directory", 2),
        ("csv", 1),
        ("zip", 1),
        ("empty-directory", 0),
    ),
)
def test_data_quality_cli_submits_quality_request_to_orchestration(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    target_kind: str,
    expected_count: int,
) -> None:
    """Quality CLI should submit an offline quality request to orchestration."""
    import histdatacom.histdata_com as histdata_com

    root = tmp_path / target_kind
    root.mkdir()
    csv_path = write_ascii_case(root, CLEAN_M1_CASE)
    zip_path = write_zip_case(root, CLEAN_M1_CASE)
    target_path = {
        "directory": root,
        "csv": csv_path,
        "zip": zip_path,
        "empty-directory": tmp_path / "empty",
    }[target_kind]
    if target_kind == "empty-directory":
        target_path.mkdir()

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(target_count=expected_count)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(target_path),
        ],
    )

    assert histdata_com.main() is None

    output = capsys.readouterr().out
    request = captured["request"]
    assert request.data_quality
    assert request.quality_paths == (str(target_path),)
    assert request.validate_urls is False
    assert request.download_data_archives is False
    assert request.extract_csvs is False
    assert request.import_to_influxdb is False
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }
    assert "Data quality assessment" in output
    assert f"targets: {expected_count}" in output


def test_data_quality_cli_missing_path_reports_orchestration_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Missing quality targets should surface orchestration activity failure."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            status=WorkStatus.FAILED.value,
            target_count=0,
            error="quality target path does not exist",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(tmp_path / "missing"),
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 1
    assert captured["request"].data_quality
    output = capsys.readouterr()
    assert "quality target path does not exist" in output.out
    assert "orchestration job failed" in output.err


def test_data_quality_api_returns_orchestration_quality_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API quality runs should return the bounded orchestration quality payload."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    options = Options()
    options.data_quality = True
    options.quality_paths = (str(csv_path),)
    options.quality_check_groups = {"inventory"}

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            target_count=1,
            report_path=str(tmp_path / "quality.json"),
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(options)

    assert captured["request"].data_quality
    assert result["operation"] == "data-quality"
    assert result["check_groups"] == ["inventory"]
    assert result["summary"]["target_count"] == 1
    assert result["report_artifact"]["kind"] == "quality-report"
    assert result["exit_decision"]["exit_code"] == 0


def test_data_quality_api_runs_inventory_rules_for_corrupt_zip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API quality runs should submit inventory checks to the orchestration."""
    import histdatacom.histdata_com as histdata_com

    archive = write_corrupt_zip(
        tmp_path,
        filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )
    options = Options()
    options.data_quality = True
    options.quality_paths = (str(archive),)
    options.quality_check_groups = {"inventory"}
    options.quality_report_path = str(tmp_path / "quality.json")

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            status=WorkStatus.FAILED.value,
            target_count=1,
            finding_count=1,
            error_count=1,
            exit_code=1,
            report_path=options.quality_report_path,
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(options)

    request = captured["request"]
    assert request.data_quality
    assert request.quality_paths == (str(archive),)
    assert request.quality_check_groups == ("inventory",)
    assert request.quality_report_path == options.quality_report_path
    assert result["summary"]["status"] == "failed"
    assert result["summary"]["error_count"] == 1
    assert result["target_summaries"][0]["status"] == "failed"
    assert result["report_artifact"]["path"] == options.quality_report_path


def test_data_quality_api_writes_coverage_manifest_from_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API quality runs should carry expected coverage into report output."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    options = Options()
    options.data_quality = True
    options.quality_paths = (str(csv_path),)
    options.quality_check_groups = {"inventory"}
    options.quality_report_path = str(tmp_path / "quality.json")
    options.metadata = {
        "coverage_manifest": {
            "expected_dimensions": [
                {
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "symbol": "EURUSD",
                    "period": "201202",
                },
                {
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "symbol": "EURUSD",
                    "period": "201203",
                },
            ]
        }
    }

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            status=WorkStatus.FAILED.value,
            target_count=1,
            finding_count=1,
            error_count=1,
            exit_code=1,
            report_path=options.quality_report_path,
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(options)

    request = captured["request"]
    assert request.data_quality
    assert request.quality_paths == (str(csv_path),)
    assert request.quality_check_groups == ("inventory",)
    assert (
        request.metadata["coverage_manifest"]
        == options.metadata["coverage_manifest"]
    )
    assert result["summary"]["status"] == "failed"
    assert result["report_artifact"]["path"] == options.quality_report_path


def test_data_quality_cli_writes_json_report_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Quality CLI should write a full JSON report when requested."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    report_path = tmp_path / "reports" / "quality.json"

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            report_path=str(report_path.resolve())
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(csv_path),
            "--quality-report",
            str(report_path),
        ],
    )

    assert histdata_com.main() is None

    output = capsys.readouterr().out
    assert captured["request"].quality_report_path == str(report_path)
    assert "Clean files" in output
    assert "Warning files\n- none" in output
    assert "Failed files\n- none" in output
    assert f"report: {report_path.resolve()}" in output


def test_data_quality_console_summary_reports_scratch_and_sources() -> None:
    """Quality console output should surface scratch cleanup and sources."""
    import histdatacom.histdata_com as histdata_com

    output = histdata_com._format_orchestration_quality_console_summary(
        {
            "operation": "data-quality",
            "check_groups": ["inventory"],
            "summary": {
                "target_count": 1,
                "finding_count": 0,
                "info_count": 0,
                "warning_count": 0,
                "error_count": 0,
                "status": "clean",
            },
            "target_status_counts": {
                "clean": 1,
                "warning": 0,
                "failed": 0,
            },
            "target_summaries": [],
            "quality_report": {
                "mode": "scratch",
                "deleted": True,
                "kept": False,
            },
            "source_cleanliness": {
                "state": "dirty",
                "source_artifact_count": 2,
            },
            "exit_decision": {"exit_code": 0, "reason": "ok"},
        }
    )

    assert "quality report: scratch report deleted after validation" in output
    assert "source artifacts: dirty (2 transient ZIP/CSV/XLS/XLSX)" in output


def test_data_quality_cli_exit_policy_fails_on_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Quality CLI should exit non-zero when findings exceed policy."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_quality_result(
            status=WorkStatus.FAILED.value,
            target_count=1,
            finding_count=1,
            error_count=1,
            exit_code=1,
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(csv_path),
            "--quality-fail-on",
            "error",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 1
    assert captured["request"].quality_fail_on == "error"
    output = capsys.readouterr().out
    assert "status: failed" in output
    assert "findings: 1 info: 0 warning: 0 error: 1" in output
    assert "decision: quality error threshold exceeded" in output


def test_back_to_back_orchestration_api_calls_do_not_leak_global_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Orchestration submissions should use per-call context, not global args."""
    import histdatacom.histdata_com as histdata_com

    captured: list[tuple[object, dict[str, object]]] = []

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured.append((request, kwargs))
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    first_options = _orchestration_options("polars")
    first_options.orchestration_start = False
    first_options.orchestration_wait_result = False
    second_options = _orchestration_options("arrow")
    second_options.orchestration_start = True
    second_options.orchestration_wait_result = True

    first_result = histdata_com.main(first_options)
    second_result = histdata_com.main(second_options)

    assert first_result["status"] == "completed"
    assert second_result["status"] == "completed"
    assert len(captured) == 2
    assert captured[0][0].api_return_type == "polars"
    assert captured[0][1] == {
        "start_if_needed": False,
        "wait_for_result": False,
    }
    assert captured[1][0].api_return_type == "arrow"
    assert captured[1][1] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_api_default_runtime_uses_orchestration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API calls should submit to orchestration by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = Options()
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"

    result = histdata_com.main(options)

    assert result["status"] == "completed"
    assert captured["request"].pairs == ("eurusd",)
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_api_pair_groups_submit_expanded_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should be able to submit named instrument groups."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = Options()
    options.pair_groups = {"majors"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"

    result = histdata_com.main(options)

    assert result["status"] == "completed"
    assert captured["request"].pairs == (
        "audusd",
        "eurusd",
        "gbpusd",
        "nzdusd",
        "usdcad",
        "usdchf",
        "usdjpy",
    )


def test_api_major_triangle_group_submits_expanded_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should be able to submit major triangle groups."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = Options()
    options.pair_groups = {"major triangles"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"

    result = histdata_com.main(options)

    assert result["status"] == "completed"
    assert captured["request"].pairs == MAJOR_TRIANGLE_SYMBOLS


def test_cli_default_runtime_uses_orchestration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI calls should submit to orchestration by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    assert histdata_com.main() is None
    assert captured["request"].pairs == ("eurusd",)
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_interactive_cli_waited_job_uses_rich_progress_observer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interactive waited CLI calls should render foreground job progress."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    class FakeProgressRenderer:
        def __enter__(self):
            captured["entered"] = True
            return self

        def __exit__(self, *exc_info: object) -> None:
            captured["exited"] = True

        def update(self, snapshot: object) -> None:
            captured["progress_snapshot"] = snapshot

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        observer = kwargs.get("progress_observer")
        if callable(observer):
            observer("progress")
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        histdata_com,
        "LiveJobProgressRenderer",
        FakeProgressRenderer,
    )
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    assert histdata_com.main() is None
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert captured["entered"] is True
    assert captured["exited"] is True
    assert callable(kwargs["progress_observer"])
    assert captured["progress_snapshot"] == "progress"


def test_back_to_back_cli_orchestration_requests_use_fresh_parser_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI orchestration requests should not reuse parser state in one process."""
    import histdatacom.histdata_com as histdata_com

    captured: list[tuple[object, dict[str, object]]] = []

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured.append((request, kwargs))
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    (tmp_path / "influxdb.yaml").write_text(
        "\n".join(
            [
                "influxdb:",
                "  org: org",
                "  bucket: bucket",
                "  url: http://127.0.0.1:8086",
                "  token: token",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--no-orchestration-start",
            "--submit-only",
            "-I",
            "-d",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "1-minute-bar-quotes",
            "-s",
            "2022-12",
            "--data-directory",
            str(tmp_path / "first"),
        ],
    )

    first_result = histdata_com.main()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "gbpusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2021-11",
            "--data-directory",
            str(tmp_path / "second"),
        ],
    )

    second_result = histdata_com.main()

    assert first_result is None
    assert second_result is None
    assert len(captured) == 2
    first_request, first_kwargs = captured[0]
    second_request, second_kwargs = captured[1]
    assert first_request.pairs == ("eurusd",)
    assert first_request.timeframes == ("M1",)
    assert first_request.data_directory == str(tmp_path / "first")
    assert first_request.validate_urls
    assert first_request.download_data_archives
    assert first_request.extract_csvs
    assert first_request.import_to_influxdb
    assert first_request.delete_after_influx
    assert first_request.metadata["influx_config"]["INFLUX_BUCKET"] == (
        "bucket"
    )
    assert first_kwargs == {
        "start_if_needed": False,
        "wait_for_result": False,
    }
    assert second_request.pairs == ("gbpusd",)
    assert second_request.timeframes == ("T",)
    assert second_request.data_directory == str(tmp_path / "second")
    assert second_request.validate_urls
    assert not second_request.download_data_archives
    assert not second_request.import_to_influxdb
    assert not second_request.delete_after_influx
    assert second_kwargs == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_cli_foreground_flag_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retired CLI foreground flag should fail before runtime dispatch."""
    import histdatacom.histdata_com as histdata_com

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "orchestration should not be submitted"
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--foreground",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 2


def test_api_foreground_opt_out_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should get a clear error for the removed runtime."""
    import histdatacom.histdata_com as histdata_com

    def fail_submit(*args: object, **kwargs: object) -> object:
        raise AssertionError("removed foreground opt-out should not submit")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = Options()
    options.use_orchestration = False
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"

    with pytest.raises(ValueError, match="foreground compatibility runtime"):
        histdata_com.main(options)


@pytest.mark.parametrize(
    ("api_return_type", "expected_module", "expected_name"),
    (
        ("polars", "polars", "DataFrame"),
        ("pandas", "pandas", "DataFrame"),
        ("arrow", "pyarrow", "Table"),
    ),
)
def test_api_orchestration_dataframe_return_is_materialized_from_cache_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    api_return_type: str,
    expected_module: str,
    expected_name: str,
) -> None:
    """API orchestration runs should preserve the legacy dataframe return contract."""
    import importlib

    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_cache_result(tmp_path)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_orchestration_options(api_return_type))

    expected_type = getattr(
        importlib.import_module(expected_module), expected_name
    )
    assert isinstance(result, expected_type)
    assert len(result) == 3
    assert captured["request"].api_return_type == api_return_type
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


@pytest.mark.parametrize(
    ("status", "expected"),
    (
        (WorkStatus.FAILED, "failed"),
        (WorkStatus.CANCELLED, "cancelled"),
    ),
)
def test_api_waited_orchestration_terminal_failure_returns_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: WorkStatus,
    expected: str,
) -> None:
    """API waited terminal failures should not materialize cache artifacts."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_terminal_result(status, tmp_path)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_orchestration_options("polars"))

    assert isinstance(result, dict)
    assert result["status"] == expected
    assert result["result"]["status"] == status.value
    assert result["result"]["stage_results"][0]["artifacts"]
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_api_orchestration_repository_request_returns_available_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Waited orchestration repository API calls should return the legacy dict."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _orchestration_repository_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_orchestration_repository_options())

    assert list(result) == ["gbpusd", "eurusd"]
    assert result["eurusd"] == {"start": "200005", "end": "202212"}
    assert captured["request"].available_remote_data is True
    assert captured["request"].metadata["repo_sort"] == "start_dsc"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_influx_cli_config_is_captured_before_orchestration_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Influx imports should use the caller cwd, not the worker cwd."""
    import histdatacom.histdata_com as histdata_com

    (tmp_path / "influxdb.yaml").write_text(
        "\n".join(
            [
                "influxdb:",
                "  org: org",
                "  bucket: bucket",
                "  url: http://127.0.0.1:8086",
                "  token: token",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _orchestration_options()
    options.import_to_influxdb = True

    histdata_com.main(options)

    request = captured["request"]
    assert request.validate_urls is True
    assert request.download_data_archives is True
    assert request.extract_csvs is True
    assert request.import_to_influxdb is True
    assert request.metadata["influx_config"] == {
        "INFLUX_ORG": "org",
        "INFLUX_BUCKET": "bucket",
        "INFLUX_URL": "http://127.0.0.1:8086",
        "INFLUX_TOKEN": "token",
    }
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_influx_cli_config_validation_happens_before_orchestration_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed Influx config should fail in the caller process."""
    import histdatacom.histdata_com as histdata_com

    (tmp_path / "influxdb.yaml").write_text(
        "\n".join(
            [
                "influxdb:",
                "  org: org",
                "  bucket: bucket",
                "  url: http://127.0.0.1:8086",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    def fail_submit(*args: object, **kwargs: object) -> None:
        raise AssertionError(
            "malformed config should not submit to orchestration"
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = _orchestration_options()
    options.import_to_influxdb = True

    with pytest.raises(InfluxConfigurationError, match="token"):
        histdata_com.main(options)


def test_api_orchestration_repository_failure_returns_available_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repository API failure behavior should preserve output parity."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> JobResult:
        return _orchestration_repository_result(
            status="failed",
            failure_code="REPOSITORY_NETWORK_ERROR",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_orchestration_repository_options())

    assert result["eurusd"] == {"start": "200005", "end": "202212"}


def test_cli_orchestration_repository_request_prints_legacy_table(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Waited orchestration repository CLI calls should keep table output."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> JobResult:
        return _orchestration_repository_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "-A", "-p", "eurusd"],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 0
    output = capsys.readouterr().out
    assert "Data and date ranges available" in output
    assert "from HistData.com" in output
    assert "eurusd" in output
    assert '"status"' not in output


def test_cli_orchestration_repository_request_can_print_quality_columns(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Repository table quality columns should be opt-in."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> JobResult:
        return _orchestration_repository_result(include_quality=True)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-A",
            "-p",
            "eurusd",
            "--repo-quality-columns",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 0
    output = capsys.readouterr().out
    assert "Quality" in output
    assert "Q Targets" in output
    assert "clean" in output
    assert "2" in output


@pytest.mark.parametrize(
    ("status", "expected"),
    (
        (WorkStatus.FAILED, "failed"),
        (WorkStatus.CANCELLED, "cancelled"),
    ),
)
def test_cli_waited_orchestration_terminal_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: WorkStatus,
    expected: str,
) -> None:
    """CLI waited terminal failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> JobResult:
        return _orchestration_terminal_result(status)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    captured = capsys.readouterr()
    assert err.value.code == 1
    assert f'"status": "{expected}"' in captured.out
    assert f'"status": "{status.value}"' in captured.out
    assert f"HistData orchestration job {expected}" in captured.err
    assert f"code: {status.value}" in captured.err
    message = (
        "validation failed" if expected == "failed" else "cancelled by caller"
    )
    assert f"message: {message}" in captured.err
    assert f"orchestration_status: {status.value}" in captured.err
    assert "Traceback" not in captured.err


def test_cli_orchestration_repository_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Repository CLI failure behavior should preserve output parity."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> JobResult:
        return _orchestration_repository_result(
            status="failed",
            failure_code="REPOSITORY_NETWORK_ERROR",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "-A", "-p", "eurusd"],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 1
    assert "Unable to fetch repo list" in capsys.readouterr().out


def test_api_orchestration_repository_submit_only_keeps_job_payload_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submit-only orchestration repository calls should not materialize data."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return JobResult(
            handle=_job_result().handle,
            status="submitted",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _orchestration_repository_options()
    options.orchestration_wait_result = False

    result = histdata_com.main(options)

    assert result["status"] == "submitted"
    assert result["result"] is None
    assert captured["request"].available_remote_data is True
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": False,
    }


def test_api_orchestration_submit_only_keeps_job_payload_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submit-only orchestration API calls should not try to read cache artifacts."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> JobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return JobResult(
            handle=_job_result().handle,
            status="submitted",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _orchestration_options()
    options.orchestration_wait_result = False

    result = histdata_com.main(options)

    assert result["status"] == "submitted"
    assert result["result"] is None
    assert captured["request"].api_return_type == "polars"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": False,
    }


def test_api_orchestration_unavailable_error_is_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should get a catchable orchestration-unavailable exception."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise OrchestrationUnavailableError("not running")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    with pytest.raises(OrchestrationUnavailableError, match="not running"):
        histdata_com.main(_orchestration_options())


def test_cli_orchestration_unavailable_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI orchestration failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise OrchestrationUnavailableError("not running")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    captured = capsys.readouterr()
    assert err.value.code == 1
    assert "HistData orchestration unavailable" in captured.err
    assert "code: ORCHESTRATION_UNAVAILABLE" in captured.err
    assert "category: dependency" in captured.err
    assert "message: not running" in captured.err
    assert "Traceback" not in captured.err


def test_api_temporal_dependency_error_is_orchestration_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should catch missing Temporal SDK as orchestration unavailable."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise RuntimeDependencyError("Temporal support requires temporalio.")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    with pytest.raises(OrchestrationUnavailableError, match="temporalio"):
        histdata_com.main(_orchestration_options())


def test_cli_temporal_dependency_error_exits_nonzero_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI missing-SDK failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise RuntimeDependencyError("Temporal support requires temporalio.")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    captured = capsys.readouterr()
    assert err.value.code == 1
    assert "HistData orchestration unavailable" in captured.err
    assert "code: TEMPORAL_DEPENDENCY_UNAVAILABLE" in captured.err
    assert "message: Temporal support requires temporalio." in captured.err
    assert "Traceback" not in captured.err


def test_version_does_not_submit_orchestration_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Version remains a local fast path even when orchestration is requested."""
    import histdatacom.histdata_com as histdata_com

    def fail_submit(*args: object, **kwargs: object) -> object:
        raise AssertionError("orchestration should not be used for version")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = Options()
    options.version = True
    options.use_orchestration = True

    assert histdata_com.main(options) == histdatacom.__version__


def test_cli_orchestration_version_exits_without_job_submission(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI version remains a zero-exit fast path in orchestration mode."""
    import histdatacom.histdata_com as histdata_com

    def fail_submit(*args: object, **kwargs: object) -> object:
        raise AssertionError("orchestration should not be used for version")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--version"],
    )

    assert histdata_com.main() is None
    assert histdatacom.__version__ in capsys.readouterr().out
