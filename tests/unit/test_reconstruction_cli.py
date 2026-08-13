"""Tests for the installed reconstruction CLI and stable exit contract."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, ClassVar

import pytest

from histdatacom import histdata_com, reconstruction_cli
from histdatacom.cli_config import configured_reconstruction_argv
from histdatacom.reconstruction import (
    ReconstructionExecutionRequestV1,
    ReconstructionExitCode,
    ReconstructionOperationReceiptV1,
    ReconstructionPlanSetPreflightV1,
    ReconstructionPreflightV1,
    ReconstructionRefusedError,
    ReconstructionUnsupportedError,
    read_operation_receipt,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.certification import CertificationState
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
        "schemas",
        "engines",
        "portfolio",
        "engine-evaluate",
        "qualify",
        "diagnostic-build",
        "diagnostic-list",
        "compatibility",
        "plan",
        "plan-set",
        "preflight-set",
        "request",
        "preflight",
        "run",
        "status",
        "cancel",
        "resume",
        "outputs",
        "preview",
        "replay",
        "certify",
    ):
        assert command in output
    assert "not recovered historical truth" in output
    assert "M1/bar inputs" in output


def test_cli_exposes_engine_discovery_portfolio_and_selected_evaluation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[str, Any]] = []

    class Result:
        def __init__(self, schema_version: str) -> None:
            self.schema_version = schema_version

        def to_dict(self) -> dict[str, Any]:
            return {"schema_version": self.schema_version}

    class FakeClient:
        def proposal_engines(self):
            calls.append(("engines", None))
            return Result("histdatacom.proposal-engine-registry.v1")

        def proposal_portfolio(self, plan):
            calls.append(("portfolio", plan))
            return Result("histdatacom.proposal-engine-portfolio.v1")

        def evaluate_proposal_portfolio(
            self,
            benchmark,
            source,
            *,
            output_directory,
            engine_ids,
        ):
            calls.append(
                (
                    "evaluate",
                    (benchmark, source, output_directory, tuple(engine_ids)),
                )
            )
            return Result("histdatacom.proposal-portfolio-evaluation.v1")

        def qualify_proposal_portfolio(
            self,
            evaluation,
            experiment,
            *,
            output_directory,
        ):
            calls.append(
                (
                    "qualify",
                    (evaluation, experiment, output_directory),
                )
            )
            return Result("histdatacom.powered-qualification-dossier.v1")

        def publish_diagnostics(self, spec, *, output_directory):
            calls.append(("diagnostic-build", (spec, output_directory)))
            return Result(
                "histdatacom.reconstruction-diagnostic-publication.v1"
            )

        def diagnostics(self, manifest):
            calls.append(("diagnostic-list", manifest))
            return {
                "schema_version": (
                    "histdatacom.reconstruction-diagnostic-publication.v1"
                ),
                "family_count": 12,
                "status_counts": {"available": 12},
            }

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())

    assert reconstruction_cli.main(["--json", "engines"]) == 0
    assert json.loads(capsys.readouterr().out)["schema_version"].endswith(
        "registry.v1"
    )
    assert (
        reconstruction_cli.main(["--json", "portfolio", "--plan", "plan.json"])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema_version"].endswith(
        "portfolio.v1"
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "engine-evaluate",
                "--benchmark-manifest",
                "benchmark.json",
                "--source-root",
                "ASCII/T",
                "--output-directory",
                "evaluation",
                "--engine",
                "histdatacom.event-clock.nhpp",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema_version"].endswith(
        "evaluation.v1"
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "qualify",
                "--evaluation",
                "evaluation.json",
                "--experiment",
                "experiment.json",
                "--output-directory",
                "qualification",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema_version"].endswith(
        "dossier.v1"
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "diagnostic-build",
                "--spec",
                "diagnostics.json",
                "--output-directory",
                "publication",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema_version"].endswith(
        "publication.v1"
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "diagnostic-list",
                "--manifest",
                "publication.json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["family_count"] == 12
    assert calls == [
        ("engines", None),
        ("portfolio", "plan.json"),
        (
            "evaluate",
            (
                "benchmark.json",
                "ASCII/T",
                "evaluation",
                ("histdatacom.event-clock.nhpp",),
            ),
        ),
        (
            "qualify",
            ("evaluation.json", "experiment.json", "qualification"),
        ),
        (
            "diagnostic-build",
            ("diagnostics.json", "publication"),
        ),
        ("diagnostic-list", "publication.json"),
    ]


def test_cli_constructs_and_preflights_public_plan_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The full-range bounded planning surface is available to operators."""
    spec = object()
    ref = ArtifactRef(
        kind="reconstruction_plan_set_v1",
        path=str(tmp_path / "plan-set.json"),
        size_bytes=10,
        sha256="1" * 64,
        metadata={"plan_set_id": "reconstruction-plan-set:test"},
    )
    preflight = ReconstructionPlanSetPreflightV1(
        plan_set_id="reconstruction-plan-set:test",
        status="ready_with_refusals",
        executable=True,
        shard_count=25,
        verified_shard_count=25,
        refusal_count=3,
        resource_summary={"planned_window_count": 8888},
        shard_preflights=(),
    )
    calls: list[tuple[str, int | str]] = []

    class FakeClient:
        def construct_plan_set(self, supplied, *, periods_per_shard):
            assert supplied is spec
            calls.append(("plan-set", periods_per_shard))
            return ref

        def preflight_plan_set(self, supplied):
            calls.append(("preflight-set", supplied))
            return preflight

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())
    monkeypatch.setattr(reconstruction_cli, "read_plan_spec", lambda _: spec)

    assert (
        reconstruction_cli.main(
            [
                "--json",
                "plan-set",
                "--spec",
                "full.json",
                "--periods-per-shard",
                "6",
            ]
        )
        == ReconstructionExitCode.SUCCESS
    )
    assert json.loads(capsys.readouterr().out)["kind"] == (
        "reconstruction_plan_set_v1"
    )
    assert (
        reconstruction_cli.main(
            ["--json", "preflight-set", "--plan-set", "plan-set.json"]
        )
        == ReconstructionExitCode.SUCCESS
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["verified_shard_count"] == 25
    assert payload["refusal_count"] == 3
    assert calls == [("plan-set", 6), ("preflight-set", "plan-set.json")]


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


@pytest.mark.parametrize(
    ("state", "expected"),
    (
        (
            CertificationState.READY_FOR_PROMOTION,
            ReconstructionExitCode.SUCCESS,
        ),
        (CertificationState.INCOMPLETE, ReconstructionExitCode.REFUSED),
        (CertificationState.FAILED, ReconstructionExitCode.VALIDATION_FAILURE),
    ),
)
def test_cli_certify_runs_public_campaign_and_maps_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    state: CertificationState,
    expected: ReconstructionExitCode,
) -> None:
    """The installed command publishes its receipt and stable exit category."""
    captured: dict[str, str] = {}

    class FakeDossier:
        summary: ClassVar[dict[str, int]] = {
            "passed_gate_count": 14,
            "missing_gate_count": 1,
        }

        def __init__(self, selected_state: CertificationState) -> None:
            self.state = selected_state

    class FakeResult:
        def to_dict(self) -> dict[str, Any]:
            return {
                "schema_version": "campaign-result.v1",
                "state": state.value,
                "dossier_id": "dossier:test",
            }

    class FakeClient:
        def certify(self, spec: str, *, output_directory: str):
            captured["spec"] = spec
            captured["output"] = output_directory
            return FakeDossier(state), FakeResult()

    monkeypatch.setattr(reconstruction_cli, "_client", lambda _: FakeClient())

    code = reconstruction_cli.main(
        [
            "--json",
            "certify",
            "--spec",
            "campaign.json",
            "--output-directory",
            str(tmp_path / "dossier"),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == expected
    assert payload["state"] == state.value
    assert payload["summary"]["passed_gate_count"] == 14
    assert captured == {
        "spec": "campaign.json",
        "output": str(tmp_path / "dossier"),
    }
