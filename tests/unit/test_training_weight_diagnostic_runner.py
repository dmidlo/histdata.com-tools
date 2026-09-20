"""Diagnostic orchestration is qualified with newly generated fixtures only."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import training_weight_diagnostic_runner as runner
from tests.fixtures.training_weight_diagnostics import fixture_diagnostic_plan


def test_complete_source_refused_fixture(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path)
    directory = tmp_path / "attempt"
    terminal = runner.run_training_weight_diagnostics(plan, directory)
    assert terminal.status == "completed", terminal.reason
    assert len(terminal.inventory) == 234
    assert (
        len([r for r in terminal.components if r.kind == "source-shard"]) == 9
    )
    assert not any(r.kind == "day" for r in terminal.components)
    assert {c.status for c in terminal.inventory if c.stage == "generator"} == {
        "not_evaluable"
    }
    assert set(json.loads(terminal.day_selection_json).values()) == {
        "source_refused"
    }
    # Artifact inspection must never follow any declared source/model locator.
    for ref in plan.inputs:
        Path(ref.file.path).unlink()
    assert (
        runner.inspect_training_weight_diagnostic_artifacts(directory)
        == terminal
    )
    with pytest.raises(ValueError, match="new attempt"):
        runner.run_training_weight_diagnostics(plan, directory)


def test_selected_fixture_cells_and_tamper(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path, admitted=True)
    directory = tmp_path / "attempt"
    terminal = runner.run_training_weight_diagnostics(plan, directory)
    assert terminal.status == "completed", terminal.reason
    assert len([r for r in terminal.components if r.kind == "day"]) == 4
    assert {c.status for c in terminal.inventory if c.stage == "generator"} <= {
        "evaluated",
        "deterministic_refused",
    }
    assert (
        runner.inspect_training_weight_diagnostic_artifacts(directory)
        == terminal
    )
    ref = next(r for r in terminal.components if r.kind == "source-shard")
    path = directory / ref.file.path
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="digest"):
        runner.inspect_training_weight_diagnostic_artifacts(directory)


@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("campaign_deadline", "attempt_deadline"),
        ("campaign_cancelled", "attempt_cancelled"),
        ("worker_failed", "worker_failed"),
        ("execution_completed_not_qualification", "worker_terminal_missing"),
    ],
)
def test_supervisor_failures_leave_complete_inventory(
    tmp_path, monkeypatch, outcome, expected
):
    plan = fixture_diagnostic_plan(tmp_path)
    monkeypatch.setattr(
        runner, "_supervise_campaign", lambda *args: (42, outcome)
    )
    terminal = runner.run_training_weight_diagnostics(
        plan, tmp_path / "attempt"
    )
    assert (terminal.status, terminal.reason) == ("failed", expected)
    assert len(terminal.inventory) == 234
    assert {c.status for c in terminal.inventory} == {"not_attempted"}
    assert (
        runner.inspect_training_weight_diagnostic_artifacts(
            tmp_path / "attempt"
        )
        == terminal
    )


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {
            "stage": "completed",
            "reason": "diagnostics_completed_not_qualification",
        },
        {"stage": "alien", "reason": "worker_failed"},
        {"stage": "preflight", "reason": "invented"},
        {"stage": "preflight", "reason": "worker_failed", "extra": True},
    ],
)
def test_missing_or_malformed_worker_never_claims_completion(
    tmp_path, monkeypatch, payload
):
    plan = fixture_diagnostic_plan(tmp_path)
    root = tmp_path / "attempt"

    def supervisor(*args):
        if payload is not None:
            runner._publish(
                root, "worker-status.json", runner.diagnostic_json(payload)
            )
        runner._checkpoint(root, "source", "2010-01-04", "", "EURGBP")
        return 42, "execution_completed_not_qualification"

    monkeypatch.setattr(runner, "_supervise_campaign", supervisor)
    result = runner.run_training_weight_diagnostics(plan, root)
    assert result.status == "failed"
    assert (
        next(c for c in result.inventory if c.utc_date == "2010-01-04").status
        == "attempted_result_unavailable"
    )
    assert sum(c.status == "not_attempted" for c in result.inventory) == 233


def test_fixture_approval_gate_precedes_output_or_input_reads(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path)
    for ref in plan.inputs:
        Path(ref.file.path).unlink()
    with pytest.raises((ValueError, TypeError)):
        runner.run_training_weight_diagnostics(
            plan, tmp_path / "attempt", approval=object()
        )
    assert not (tmp_path / "attempt").exists()


def test_completed_contract_rejects_unattempted_cells(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path)
    with pytest.raises(ValueError, match="incomplete"):
        runner.TrainingWeightDiagnosticAttemptV1(
            plan.artifact_id,
            plan.evidence_kind,
            "completed",
            "diagnostics_completed_not_qualification",
            0,
            None,
            "completed",
            runner._empty_inventory(),
            (),
            runner.diagnostic_json(
                dict.fromkeys(runner._dates(), "source_refused")
            ),
            "training-weight-diagnostic-runtime:sha256:" + "a" * 64,
        )


def test_real_cancel_is_durable_and_nonresumable(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path)
    root = tmp_path / "attempt"
    result = runner.run_training_weight_diagnostics(
        plan, root, cancelled=lambda: True
    )
    assert (result.status, result.reason) == ("failed", "attempt_cancelled")
    assert (root / "started.json").is_file()
    assert (
        runner.TrainingWeightDiagnosticAttemptV1.from_json(
            (root / "terminal.json").read_text()
        )
        == result
    )


def test_missing_input_is_operational_failure_not_source_refusal(tmp_path):
    plan = fixture_diagnostic_plan(tmp_path)
    Path(
        next(
            i.file.path
            for i in plan.inputs
            if i.role.value == "historical-source"
        )
    ).unlink()
    result = runner.run_training_weight_diagnostics(plan, tmp_path / "attempt")
    assert (result.status, result.reason) == ("failed", "worker_failed")
    assert not any(c.status == "evaluated" for c in result.inventory)


def test_fixture_ancestor_alias_refused_before_reads(tmp_path, monkeypatch):
    plan = fixture_diagnostic_plan(tmp_path)
    source = tmp_path / "training-weight-diagnostic-fixture"
    real = tmp_path / "outside-fixture"
    source.rename(real)
    source.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink alias"):
        runner._input_paths(plan)


def test_atomic_publish_never_exposes_partial_component(tmp_path, monkeypatch):
    original = runner.os.link

    def failed_link(*args, **kwargs):
        raise OSError("synthetic publication interruption")

    monkeypatch.setattr(runner.os, "link", failed_link)
    with pytest.raises(OSError):
        runner._publish(tmp_path, "diagnostic-day-" + "a" * 64 + ".json", "{}")
    assert not tuple(tmp_path.iterdir())
    monkeypatch.setattr(runner.os, "link", original)
    runner._publish(tmp_path, "retained.json", "{}")
    with pytest.raises(FileExistsError):
        runner._publish(tmp_path, "retained.json", "{}")


@pytest.mark.parametrize(
    "name", ["plan.json", "started.json", "runtime.json", "terminal.json"]
)
def test_metadata_encoding_not_silently_canonicalized(
    tmp_path, monkeypatch, name
):
    plan = fixture_diagnostic_plan(tmp_path)
    monkeypatch.setattr(
        runner, "_supervise_campaign", lambda *args: (42, "worker_failed")
    )
    root = tmp_path / "attempt"
    runner.run_training_weight_diagnostics(plan, root)
    path = root / name
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="canonical"):
        runner.inspect_training_weight_diagnostic_artifacts(root)


def test_resealed_runtime_cannot_relabel_attempt(tmp_path, monkeypatch):
    plan = fixture_diagnostic_plan(tmp_path)
    monkeypatch.setattr(
        runner, "_supervise_campaign", lambda *args: (42, "worker_failed")
    )
    root = tmp_path / "attempt"
    runner.run_training_weight_diagnostics(plan, root)
    path = root / "runtime.json"
    runtime = runner.TrainingWeightDiagnosticRuntimeV1.from_json(
        path.read_text()
    )
    path.write_text(replace(runtime, source_fingerprint="b" * 64).to_json())
    with pytest.raises(ValueError, match="started inventory"):
        runner.inspect_training_weight_diagnostic_artifacts(root)


def test_failed_attempt_retains_completed_source_month(tmp_path, monkeypatch):
    plan = fixture_diagnostic_plan(tmp_path)
    root = tmp_path / "attempt"

    def interrupted(*args):
        runner._month_worker(plan.to_json(), None, str(root), "201001")
        # An abandoned atomic temporary is not a committed component.
        (root / ".weight-interrupted").write_bytes(b"{")
        runner._checkpoint(root, "source", "2011-01-03", "", "EURGBP")
        return 42, "worker_failed"

    monkeypatch.setattr(runner, "_supervise_campaign", interrupted)
    result = runner.run_training_weight_diagnostics(plan, root)
    assert result.status == "failed"
    assert len(result.components) == 4
    assert (
        sum(
            c.component_id is not None
            for c in result.inventory
            if c.stage == "source"
        )
        == 63
    )
    assert (
        next(c for c in result.inventory if c.utc_date == "2011-01-03").status
        == "attempted_result_unavailable"
    )
    assert runner.inspect_training_weight_diagnostic_artifacts(root) == result
    admission = root / "source-admission-201001.json"
    admission.write_bytes(admission.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="source admission metadata"):
        runner.inspect_training_weight_diagnostic_artifacts(root)
