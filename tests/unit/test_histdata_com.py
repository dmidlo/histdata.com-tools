"""Pytest unit tests for histdatacom.histdata_com.py."""

from __future__ import annotations

import sys

import pytest

import histdatacom
from histdatacom.options import Options
from histdatacom.sidecar.client import (
    SidecarJobHandle,
    SidecarJobResult,
    SidecarUnavailableError,
)


def test_histdata_com() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _sidecar_options() -> Options:
    """Return a small API request configured for sidecar execution."""
    options = Options()
    options.use_sidecar = True
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1"}
    options.start_yearmonth = "2022-12"
    options.api_return_type = "polars"
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


def test_api_options_can_submit_sidecar_job_and_return_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API callers should be able to opt into sidecar-backed execution."""
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
        "start_if_needed": False,
        "wait_for_result": True,
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
