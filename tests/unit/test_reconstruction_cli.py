"""Tests for the installed reconstruction CLI and stable exit contract."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

import pytest

from histdatacom import histdata_com
from histdatacom import reconstruction_cli
from histdatacom.cli_config import configured_reconstruction_argv
from histdatacom.reconstruction import (
    ReconstructionExecutionRequestV1,
    ReconstructionExitCode,
    ReconstructionOperationReceiptV1,
    ReconstructionPreflightV1,
    ReconstructionRefusedError,
    ReconstructionUnsupportedError,
    read_operation_receipt,
)
from histdatacom.synthetic.information import InformationMode


def _request(tmp_path: Path) -> ReconstructionExecutionRequestV1:
    return ReconstructionExecutionRequestV1(
        plan_path=str(tmp_path / "plan.json"),
        plan_id="synthetic-infill-plan:sha256:" + "1" * 64,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        scientific_nonclaim_acknowledged=True,
    )


def _receipt(
    tmp_path: Path, *, status: str = "submitted"
) -> ReconstructionOperationReceiptV1:
    return ReconstructionOperationReceiptV1(
        operation="submit_only",
        request=_request(tmp_path),
        status=status,
    )


def test_installed_help_lists_complete_reconstruction_family(
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = reconstruction_cli.build_parser()

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    for command in (
        "plan",
        "request",
        "preflight",
        "run",
        "status",
        "cancel",
        "resume",
        "outputs",
        "preview",
        "replay",
    ):
        assert command in output
    assert "not recovered historical truth" in output
    assert "M1/bar inputs" in output


def test_histdatacom_main_dispatches_reconstruction_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, tuple[str, ...]] = {}

    def fake_main(argv: list[str]) -> int:
        captured["argv"] = tuple(argv)
        return 0

    monkeypatch.setattr(reconstruction_cli, "main", fake_main)
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "reconstruction", "preflight", "--request", "r.json"],
    )

    assert histdata_com.main() == 0
    assert captured["argv"] == ("preflight", "--request", "r.json")


def test_reconstruction_config_injects_command_and_explicit_flags_win(
    tmp_path: Path,
) -> None:
    config = tmp_path / "histdatacom.yaml"
    config.write_text(
        """
histdatacom:
  reconstruction:
    command: preview
    manifest: configured.json
    limit: 7
    json: true
""".lstrip(),
        encoding="utf-8",
    )

    configured = configured_reconstruction_argv(["--config", str(config)])
    explicit = configured_reconstruction_argv(
        [
            "--config",
            str(config),
            "preview",
            "--manifest",
            "explicit.json",
            "--limit",
            "3",
        ]
    )

    assert configured == [
        "--json",
        "preview",
        "--limit",
        "7",
        "--manifest",
        "configured.json",
    ]
    assert explicit == [
        "--json",
        "preview",
        "--limit",
        "7",
        "--manifest",
        "configured.json",
        "--manifest",
        "explicit.json",
        "--limit",
        "3",
    ]


def test_cli_submit_only_writes_identity_checked_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    request = _request(tmp_path)
    receipt = _receipt(tmp_path)
    receipt_path = tmp_path / "receipt.json"

    class FakeClient:
        def submit(self, supplied, *, wait):
            assert supplied == request
            assert not wait
            return receipt

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())
    monkeypatch.setattr(
        reconstruction_cli, "read_execution_request", lambda _: request
    )

    code = reconstruction_cli.main(
        [
            "--json",
            "run",
            "--request",
            "request.json",
            "--submit-only",
            "--receipt",
            str(receipt_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == ReconstructionExitCode.SUCCESS
    assert payload["receipt_path"] == str(receipt_path)
    assert read_operation_receipt(receipt_path) == receipt


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (
            ReconstructionUnsupportedError("M1 is unsupported"),
            ReconstructionExitCode.INVALID_PLAN,
        ),
        (
            ReconstructionRefusedError("scientific refusal"),
            ReconstructionExitCode.REFUSED,
        ),
        (
            RuntimeError("Temporal crashed"),
            ReconstructionExitCode.RUNTIME_FAILURE,
        ),
    ),
)
def test_cli_maps_invalid_refused_and_runtime_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: Exception,
    expected: ReconstructionExitCode,
) -> None:
    class FakeClient:
        def submit(self, *_: Any, **__: Any) -> Any:
            raise failure

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())
    monkeypatch.setattr(
        reconstruction_cli,
        "read_execution_request",
        lambda _: _request(tmp_path),
    )

    code = reconstruction_cli.main(
        [
            "--json",
            "run",
            "--request",
            "request.json",
            "--submit-only",
            "--receipt",
            str(tmp_path / "receipt.json"),
        ]
    )

    payload = json.loads(capsys.readouterr().err)
    assert code == expected
    assert payload["exit_code"] == expected


def test_cli_maps_failed_local_report_to_validation_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    request = _request(tmp_path)

    class FakeClient:
        def execute_local(self, supplied, *, window_id):
            assert supplied == request
            assert window_id == "window-1"
            return _receipt(tmp_path, status="failed")

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())
    monkeypatch.setattr(
        reconstruction_cli, "read_execution_request", lambda _: request
    )

    code = reconstruction_cli.main(
        [
            "--json",
            "run",
            "--request",
            "request.json",
            "--local",
            "--window-id",
            "window-1",
            "--receipt",
            str(tmp_path / "failed.json"),
        ]
    )

    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    assert code == ReconstructionExitCode.VALIDATION_FAILURE


def test_cli_preflight_refusal_is_distinct_from_unsupported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    request = _request(tmp_path)
    preflight = ReconstructionPreflightV1(
        request_id=request.request_id,
        plan_id=request.plan_id,
        status="refused",
        executable=False,
        plan_status="ready_with_refusals",
        dry_run={},
        evidence_refs={},
        refusal_reasons=(
            {
                "code": "market_context_unsupported",
                "reason": "calendar evidence absent",
            },
        ),
    )

    class FakeClient:
        def preflight(self, supplied):
            assert supplied == request
            return preflight

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())
    monkeypatch.setattr(
        reconstruction_cli, "read_execution_request", lambda _: request
    )

    code = reconstruction_cli.main(
        ["--json", "preflight", "--request", "request.json"]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == ReconstructionExitCode.REFUSED
    assert payload["refusal_reasons"][0]["code"] == (
        "market_context_unsupported"
    )


def test_cli_status_cancel_and_resume_use_receipt_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _receipt(tmp_path)
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(source.to_dict()), encoding="utf-8")
    calls: list[str] = []

    class FakeClient:
        def inspect(self, receipt, *, offline):
            assert receipt == source
            assert offline
            calls.append("status")
            return _receipt(tmp_path, status="running")

        def cancel(self, receipt, *, reason):
            assert receipt == source
            assert reason == "operator request"
            calls.append("cancel")
            return _receipt(tmp_path, status="cancellation_requested")

        def resume(self, receipt, *, wait, local):
            assert receipt == source
            assert not wait
            assert not local
            calls.append("resume")
            return _receipt(tmp_path, status="submitted")

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())

    assert (
        reconstruction_cli.main(
            ["status", "--receipt", str(source_path), "--offline"]
        )
        == ReconstructionExitCode.SUCCESS
    )
    assert (
        reconstruction_cli.main(
            [
                "cancel",
                "--receipt",
                str(source_path),
                "--reason",
                "operator request",
                "--output",
                str(tmp_path / "cancel.json"),
            ]
        )
        == ReconstructionExitCode.SUCCESS
    )
    assert (
        reconstruction_cli.main(
            [
                "resume",
                "--receipt",
                str(source_path),
                "--submit-only",
                "--output",
                str(tmp_path / "resume.json"),
            ]
        )
        == ReconstructionExitCode.SUCCESS
    )
    assert calls == ["status", "cancel", "resume"]
