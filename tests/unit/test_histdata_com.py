"""Pytest unit tests for histdatacom.histdata_com.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import histdatacom
from histdatacom.data_quality import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
)
from histdatacom.exceptions import InfluxConfigurationError
from histdatacom.options import Options
from histdatacom.runtime_contracts import WorkStatus
from histdatacom.sidecar.client import (
    SidecarJobHandle,
    SidecarJobResult,
    SidecarUnavailableError,
    TemporalDependencyError,
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


def _sidecar_options(api_return_type: str = "polars") -> Options:
    """Return a small API request configured for sidecar execution."""
    options = Options()
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"
    options.api_return_type = api_return_type
    return options


def _sidecar_repository_options() -> Options:
    """Return an API repository request configured for sidecar execution."""
    options = Options()
    options.available_remote_data = True
    options.pairs = {"eurusd", "gbpusd"}
    options.by = "start_dsc"
    return options


def _job_result(*, status: str = "completed") -> SidecarJobResult:
    """Return a fake sidecar job result."""
    return SidecarJobResult(
        handle=SidecarJobHandle(
            request_id="run-test",
            workflow_id="histdatacom-run-test",
            run_id="run-fake",
            task_queue="histdatacom.test.orchestration",
            namespace="default",
        ),
        status=status,
        result={"workflow_name": "HistDataRunWorkflow"},
    )


def _sidecar_repository_result(
    *,
    status: str = "completed",
    failure_code: str = "",
) -> SidecarJobResult:
    """Return a completed sidecar result with repository metrics."""
    available_data = {
        "gbpusd": {"start": "200005", "end": "202212"},
        "eurusd": {"start": "200005", "end": "202212"},
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
    return SidecarJobResult(
        handle=_job_result().handle,
        status="completed",
        result={
            "workflow_name": "HistDataRunWorkflow",
            "status": status,
            "stage_results": [stage_result],
            "artifacts": [],
        },
    )


def _sidecar_cache_result(tmp_path: Path) -> SidecarJobResult:
    """Return a completed sidecar result with a cache artifact."""
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
    return SidecarJobResult(
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


def _sidecar_terminal_result(
    status: WorkStatus,
    tmp_path: Path | None = None,
) -> SidecarJobResult:
    """Return a failed or cancelled sidecar result payload."""
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
        cache_result = _sidecar_cache_result(tmp_path)
        stage_result["stage"] = "merge_cache"
        stage_result["artifacts"] = cache_result.result["stage_results"][0][
            "artifacts"
        ]
    return SidecarJobResult(
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


def test_api_options_can_submit_sidecar_job_and_return_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should submit sidecar-backed jobs by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_sidecar_options())

    assert result["status"] == "completed"
    assert result["result"] == {"workflow_name": "HistDataRunWorkflow"}
    assert captured["request"].pairs == ("eurusd",)
    assert captured["request"].api_return_type == "polars"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


@pytest.mark.parametrize(
    ("target_kind", "expected_count", "expected_text"),
    (
        ("directory", 2, "targets: 2"),
        ("csv", 1, "csv:"),
        ("zip", 1, "zip:"),
        ("empty-directory", 0, "No data quality targets discovered."),
    ),
)
def test_data_quality_cli_discovers_local_targets_without_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    target_kind: str,
    expected_count: int,
    expected_text: str,
) -> None:
    """Quality CLI should discover local targets without starting sidecar."""
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

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
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
    assert "Data quality assessment" in output
    assert expected_text in output
    assert f"targets: {expected_count}" in output


def test_data_quality_cli_missing_path_exits_without_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Missing quality targets should fail locally before any sidecar call."""
    import histdatacom.histdata_com as histdata_com

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
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
    assert "quality target path does not exist" in capsys.readouterr().err


def test_data_quality_api_returns_discovery_payload_without_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API quality runs should return the same bounded discovery payload."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    options = Options()
    options.data_quality = True
    options.quality_paths = (str(csv_path),)
    options.quality_check_groups = {"inventory"}

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
    )

    result = histdata_com.main(options)

    assert result["operation"] == "data-quality"
    assert result["check_groups"] == ["inventory"]
    assert result["discovery"]["target_count"] == 1
    assert result["summary"]["target_count"] == 1
    assert result["report_artifact"] is None
    assert result["exit_decision"]["exit_code"] == 0


def test_data_quality_api_runs_inventory_rules_for_corrupt_zip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API quality runs should execute concrete inventory checks locally."""
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

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
    )

    result = histdata_com.main(options)

    assert result["summary"]["status"] == "failed"
    assert result["summary"]["error_count"] == 1
    assert result["target_summaries"][0]["status"] == "failed"
    assert result["target_summaries"][0]["target"]["kind"] == "zip"
    assert result["target_summaries"][0]["target"]["symbol"] == "EURUSD"
    report = json.loads(
        Path(result["report_artifact"]["path"]).read_text(encoding="utf-8")
    )
    assert report["rule_results"][0]["rule_id"] == "inventory.zip.integrity"
    assert report["rule_results"][0]["findings"][0]["code"] == "ZIP_CORRUPT"


def test_data_quality_cli_writes_json_report_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Quality CLI should write a full JSON report when requested."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    report_path = tmp_path / "reports" / "quality.json"
    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
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
    assert "Clean files" in output
    assert "Warning files\n- none" in output
    assert "Failed files\n- none" in output
    assert f"report: {report_path.resolve()}" in output
    assert '"schema_version": "histdatacom.quality-report.v1"' in (
        report_path.read_text(encoding="utf-8")
    )


def test_data_quality_cli_exit_policy_fails_on_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Quality CLI should exit non-zero when findings exceed policy."""
    import histdatacom.histdata_com as histdata_com

    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)

    def fake_assessment(targets, rules, *, metadata=None):
        target = tuple(targets)[0]
        finding = QualityFinding(
            severity=QualitySeverity.ERROR,
            code="FILE_MISSING",
            message="expected local file is missing",
            rule_id="file.exists",
            target=target,
            location=QualityLocation(path=target.path),
        )
        return QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id="file.exists",
                    target=target,
                    findings=(finding,),
                ),
            ),
            metadata=dict(metadata or {}),
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        lambda *args, **kwargs: pytest.fail(
            "quality mode should not submit to sidecar"
        ),
    )
    monkeypatch.setattr(histdata_com, "run_quality_assessment", fake_assessment)
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
    output = capsys.readouterr().out
    assert "status: failed" in output
    assert "Failed files\n- csv:" in output


def test_back_to_back_sidecar_api_calls_do_not_leak_global_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sidecar submissions should use per-call context, not global args."""
    import histdatacom.histdata_com as histdata_com

    captured: list[tuple[object, dict[str, object]]] = []

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured.append((request, kwargs))
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    first_options = _sidecar_options("polars")
    first_options.sidecar_start = False
    first_options.sidecar_wait_result = False
    second_options = _sidecar_options("arrow")
    second_options.sidecar_start = True
    second_options.sidecar_wait_result = True

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


def test_api_default_runtime_uses_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API calls should submit to sidecar by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
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


def test_cli_default_runtime_uses_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI calls should submit to sidecar by default."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
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


def test_back_to_back_cli_sidecar_requests_use_fresh_parser_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI sidecar requests should not reuse parser state in one process."""
    import histdatacom.histdata_com as histdata_com

    captured: list[tuple[object, dict[str, object]]] = []

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
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
            "--no-sidecar-start",
            "--sidecar-submit-only",
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
        lambda *args, **kwargs: pytest.fail("sidecar should not be submitted"),
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
    options.use_sidecar = False
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
def test_api_sidecar_dataframe_return_is_materialized_from_cache_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    api_return_type: str,
    expected_module: str,
    expected_name: str,
) -> None:
    """API sidecar runs should preserve the legacy dataframe return contract."""
    import importlib

    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _sidecar_cache_result(tmp_path)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_sidecar_options(api_return_type))

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
def test_api_waited_sidecar_terminal_failure_returns_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: WorkStatus,
    expected: str,
) -> None:
    """API waited terminal failures should not materialize cache artifacts."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _sidecar_terminal_result(status, tmp_path)

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_sidecar_options("polars"))

    assert isinstance(result, dict)
    assert result["status"] == expected
    assert result["result"]["status"] == status.value
    assert result["result"]["stage_results"][0]["artifacts"]
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_api_sidecar_repository_request_returns_available_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Waited sidecar repository API calls should return the legacy dict."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _sidecar_repository_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_sidecar_repository_options())

    assert list(result) == ["gbpusd", "eurusd"]
    assert result["eurusd"] == {"start": "200005", "end": "202212"}
    assert captured["request"].available_remote_data is True
    assert captured["request"].metadata["repo_sort"] == "start_dsc"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": True,
    }


def test_influx_cli_config_is_captured_before_sidecar_handoff(
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

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _job_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _sidecar_options()
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


def test_influx_cli_config_validation_happens_before_sidecar_handoff(
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
        raise AssertionError("malformed config should not submit to sidecar")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = _sidecar_options()
    options.import_to_influxdb = True

    with pytest.raises(InfluxConfigurationError, match="token"):
        histdata_com.main(options)


def test_api_sidecar_repository_failure_returns_available_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repository API failure behavior should preserve output parity."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> SidecarJobResult:
        return _sidecar_repository_result(
            status="failed",
            failure_code="REPOSITORY_NETWORK_ERROR",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    result = histdata_com.main(_sidecar_repository_options())

    assert result["eurusd"] == {"start": "200005", "end": "202212"}


def test_cli_sidecar_repository_request_prints_legacy_table(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Waited sidecar repository CLI calls should keep table output."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> SidecarJobResult:
        return _sidecar_repository_result()

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--sidecar", "-A", "-p", "eurusd"],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 0
    output = capsys.readouterr().out
    assert "Data and date ranges available" in output
    assert "from HistData.com" in output
    assert "eurusd" in output
    assert '"status"' not in output


@pytest.mark.parametrize(
    ("status", "expected"),
    (
        (WorkStatus.FAILED, "failed"),
        (WorkStatus.CANCELLED, "cancelled"),
    ),
)
def test_cli_waited_sidecar_terminal_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: WorkStatus,
    expected: str,
) -> None:
    """CLI waited terminal failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> SidecarJobResult:
        return _sidecar_terminal_result(status)

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
            "--sidecar",
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
    assert f"error: sidecar job {expected}" in captured.err
    assert "Traceback" not in captured.err


def test_cli_sidecar_repository_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Repository CLI failure behavior should preserve output parity."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> SidecarJobResult:
        return _sidecar_repository_result(
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
        ["histdatacom", "--sidecar", "-A", "-p", "eurusd"],
    )

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 1
    assert "Unable to fetch repo list" in capsys.readouterr().out


def test_api_sidecar_repository_submit_only_keeps_job_payload_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submit-only sidecar repository calls should not materialize data."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return SidecarJobResult(
            handle=_job_result().handle,
            status="submitted",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _sidecar_repository_options()
    options.sidecar_wait_result = False

    result = histdata_com.main(options)

    assert result["status"] == "submitted"
    assert result["result"] is None
    assert captured["request"].available_remote_data is True
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": False,
    }


def test_api_sidecar_submit_only_keeps_job_payload_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submit-only sidecar API calls should not try to read cache artifacts."""
    import histdatacom.histdata_com as histdata_com

    captured: dict[str, object] = {}

    def fake_submit(request, **kwargs: object) -> SidecarJobResult:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return SidecarJobResult(
            handle=_job_result().handle,
            status="submitted",
        )

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )
    options = _sidecar_options()
    options.sidecar_wait_result = False

    result = histdata_com.main(options)

    assert result["status"] == "submitted"
    assert result["result"] is None
    assert captured["request"].api_return_type == "polars"
    assert captured["kwargs"] == {
        "start_if_needed": True,
        "wait_for_result": False,
    }


def test_api_sidecar_unavailable_error_is_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should get a catchable sidecar-unavailable exception."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise SidecarUnavailableError("not running")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    with pytest.raises(SidecarUnavailableError, match="not running"):
        histdata_com.main(_sidecar_options())


def test_cli_sidecar_unavailable_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI sidecar failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise SidecarUnavailableError("not running")

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
            "--sidecar",
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

    assert err.value.code == 1
    assert "not running" in capsys.readouterr().err


def test_api_temporal_dependency_error_is_sidecar_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should catch missing Temporal SDK as sidecar unavailable."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise TemporalDependencyError("Temporal support requires temporalio.")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fake_submit,
    )

    with pytest.raises(SidecarUnavailableError, match="temporalio"):
        histdata_com.main(_sidecar_options())


def test_cli_temporal_dependency_error_exits_nonzero_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI missing-SDK failures should be shell-friendly."""
    import histdatacom.histdata_com as histdata_com

    def fake_submit(*args: object, **kwargs: object) -> object:
        raise TemporalDependencyError("Temporal support requires temporalio.")

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
    assert "error: Temporal support requires temporalio." in captured.err
    assert "Traceback" not in captured.err


def test_version_does_not_submit_sidecar_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Version remains a local fast path even when sidecar is requested."""
    import histdatacom.histdata_com as histdata_com

    def fail_submit(*args: object, **kwargs: object) -> object:
        raise AssertionError("sidecar should not be used for version")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    options = Options()
    options.version = True
    options.use_sidecar = True

    assert histdata_com.main(options) == histdatacom.__version__


def test_cli_sidecar_version_exits_without_job_submission(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI version remains a zero-exit fast path in sidecar mode."""
    import histdatacom.histdata_com as histdata_com

    def fail_submit(*args: object, **kwargs: object) -> object:
        raise AssertionError("sidecar should not be used for version")

    monkeypatch.setattr(
        histdata_com,
        "submit_run_request_and_observe_sync",
        fail_submit,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--sidecar", "--version"],
    )

    assert histdata_com.main() is None
    assert histdatacom.__version__ in capsys.readouterr().out
