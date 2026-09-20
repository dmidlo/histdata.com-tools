"""Synthetic operational attempts only; no retained market source is decoded."""

import errno
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import histdatacom.data_quality.training_weight_campaign as campaign
from histdatacom.data_quality.training_contracts import training_json
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)


@pytest.fixture
def plan(tmp_path):
    sources = tuple(
        fixture_weight_source(
            tmp_path / period, dates=(day,), period=period, step_seconds=8
        )[0]
        for period, day in (
            ("201001", "2010-01-04"),
            ("201101", "2011-01-04"),
            ("201102", "2011-02-01"),
        )
    )
    return campaign.TrainingWeightCampaignPlanV1(
        sources, fixture_weight_model(tmp_path / "model"), "synthetic-attempt"
    )


def test_closed_plan_never_omits_months_or_mislabels_replay(plan):
    assert (
        campaign.TrainingWeightCampaignPlanV1.from_json(plan.to_json()) == plan
    )
    with pytest.raises(ValueError, match="three source"):
        replace(plan, sources=plan.sources[:2])
    with pytest.raises(ValueError, match="prior attempt"):
        replace(plan, purpose="qualification_replay")
    with pytest.raises(ValueError, match="prior attempt"):
        replace(plan, prior_attempt_id="caller-proof")


def test_nonposix_refuses_before_validation_source_or_filesystem_work(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(campaign.os, "name", "nt")
    with pytest.raises(NotImplementedError, match="POSIX"):
        campaign.run_training_weight_campaign(object(), tmp_path / "absent")
    assert not (tmp_path / "absent").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
@pytest.mark.parametrize(
    "reason", ["campaign_deadline", "worker_failed", "campaign_cancelled"]
)
def test_failed_attempt_is_durable_never_complete_and_cannot_silently_retry(
    tmp_path, monkeypatch, plan, reason
):
    target = tmp_path / "attempt"
    calls = []

    def fail(command, seconds, cancelled):
        calls.append(command)
        assert (target / "started.json").is_file()
        assert (target / "plan.json").is_file()
        assert 0 <= seconds <= 10800.0
        return 12345, reason

    monkeypatch.setattr(campaign, "_supervise_campaign", fail)
    result = campaign.run_training_weight_campaign(plan, target)
    assert result.status == "failed" and result.reason == reason
    assert result.evaluation_id is None
    assert (
        campaign.TrainingWeightCampaignAttemptV1.from_json(
            (target / "terminal.json").read_text(encoding="ascii")
        )
        == result
    )
    with pytest.raises(ValueError, match="new attempt"):
        campaign.run_training_weight_campaign(plan, target)
    assert len(calls) == 1
    with pytest.raises(ValueError, match="failed attempt"):
        replace(result, reason="pending")


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_success_requires_retained_terminal_evaluation_not_only_exit_zero(
    tmp_path, monkeypatch, plan
):
    monkeypatch.setattr(
        campaign,
        "_supervise_campaign",
        lambda *_: (12345, "execution_completed_not_qualification"),
    )
    result = campaign.run_training_weight_campaign(
        plan, tmp_path / "no-terminal"
    )
    assert (
        result.status == "failed" and result.reason == "worker_terminal_missing"
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
@pytest.mark.parametrize("cancel", [False, True])
def test_global_supervisor_cleans_active_nested_day_worker(tmp_path, cancel):
    worker_pid = tmp_path / "nested.pid"
    code = """
import pathlib, signal, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding='ascii')
def interrupted(signum, frame):
    raise SystemExit(0)
signal.signal(signal.SIGTERM, interrupted)
try:
    time.sleep(30)
finally:
    child.terminate()
    child.wait(timeout=2)
"""
    start = time.monotonic()
    pid, reason = campaign._supervise_campaign(
        (sys.executable, "-c", code, str(worker_pid)),
        2.0,
        (lambda: worker_pid.exists()) if cancel else None,
    )
    assert reason == ("campaign_cancelled" if cancel else "campaign_deadline")
    assert time.monotonic() - start < 10
    assert worker_pid.exists()
    nested = int(worker_pid.read_text(encoding="ascii"))
    for target in (pid, nested):
        with pytest.raises(ProcessLookupError):
            os.kill(target, 0)


def test_completed_attempt_is_not_an_empirical_success_claim(plan):
    completed = campaign.TrainingWeightCampaignAttemptV1(
        plan.artifact_id,
        plan.model.evidence_kind,
        "completed",
        "execution_completed_not_qualification",
        1,
        12345,
        "completed",
        "training-weight-campaign-evaluation:sha256:" + "a" * 64,
    )
    assert "not_qualification" in completed.to_json()
    with pytest.raises(ValueError, match="completion"):
        replace(completed, reason="pending")
    with pytest.raises(ValueError, match="evaluation"):
        replace(completed, evaluation_id="caller-proof")


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
@pytest.mark.parametrize("exit_code", [0, 3])
def test_exited_leader_cannot_abandon_live_native_work(tmp_path, exit_code):
    heartbeat = tmp_path / "heartbeat"
    child = """
import pathlib, sys, time
path = pathlib.Path(sys.argv[1])
while True:
    path.write_text(str(time.monotonic()), encoding='ascii')
    time.sleep(0.01)
"""
    leader = """
import os, pathlib, subprocess, sys, time
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])
while not pathlib.Path(sys.argv[2]).exists():
    time.sleep(0.01)
os._exit(int(sys.argv[3]))
"""
    _, reason = campaign._supervise_campaign(
        (sys.executable, "-c", leader, child, str(heartbeat), str(exit_code)),
        3.0,
    )
    assert reason == "worker_failed"
    time.sleep(0.1)
    stopped = heartbeat.read_text(encoding="ascii")
    time.sleep(0.1)
    assert heartbeat.read_text(encoding="ascii") == stopped


@pytest.mark.parametrize(
    "results, expected, polls",
    [
        ((errno.EPERM, 0, errno.ESRCH), True, 1),
        ((errno.EPERM, errno.ESRCH, errno.ESRCH), False, 1),
        ((0, errno.EPERM, 0), True, 1),
        ((0, errno.EPERM, errno.ESRCH), True, 1),
    ],
)
def test_darwin_transient_group_denial_requires_success_or_absence(
    monkeypatch, results, expected, polls
):
    clock = [0.0]
    calls = []
    reaped = []
    outcomes = iter(results)

    def killpg(pid, signum):
        calls.append((pid, signum))
        result = next(outcomes)
        if result:
            raise OSError(result, "synthetic scoped signal result")

    process = SimpleNamespace(
        pid=12345,
        poll=lambda: reaped.append(True),
        wait=lambda timeout: 0,
    )
    monkeypatch.setattr(campaign.sys, "platform", "darwin")
    monkeypatch.setattr(campaign.os, "killpg", killpg, raising=False)
    monkeypatch.setattr(campaign.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(
        campaign,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock[0],
            sleep=lambda delay: clock.__setitem__(0, clock[0] + delay),
        ),
    )
    assert campaign._stop_group(process) is expected
    assert len(reaped) == polls
    assert all(pid == process.pid for pid, _ in calls)
    assert calls[0][1] == signal.SIGTERM
    assert calls[-1][1] == signal.SIGKILL
    assert 0 < clock[0] <= 0.25


@pytest.mark.parametrize("platform", ["darwin", "linux", "win32"])
@pytest.mark.parametrize("failure_errno", [errno.EPERM, errno.EACCES])
def test_persistent_group_denial_is_bounded_and_never_claims_cleanup(
    monkeypatch, platform, failure_errno
):
    clock = [0.0]
    calls = []
    polls = []

    def denied(pid, signum):
        calls.append((pid, signum))
        raise PermissionError(failure_errno, "synthetic real denial")

    monkeypatch.setattr(campaign.sys, "platform", platform)
    monkeypatch.setattr(campaign.os, "killpg", denied, raising=False)
    monkeypatch.setattr(campaign.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(
        campaign,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock[0],
            sleep=lambda delay: clock.__setitem__(0, clock[0] + delay),
        ),
    )
    process = SimpleNamespace(pid=12345, poll=lambda: polls.append(True))
    with pytest.raises(PermissionError) as caught:
        campaign._signal_group(process, signal.SIGKILL)
    assert caught.value.errno == failure_errno
    assert set(calls) == {(12345, signal.SIGKILL)}
    if platform == "darwin" and failure_errno == errno.EPERM:
        assert clock[0] == 0.25
        assert 2 <= len(calls) <= 30 and len(polls) == len(calls)
    else:
        assert clock[0] == 0 and len(calls) == 1 and not polls


@pytest.mark.skipif(
    sys.platform != "darwin" or not hasattr(os, "waitid"),
    reason="Darwin waitid(WNOWAIT) zombie observation requires os.waitid",
)
def test_darwin_direct_zombie_is_reaped_before_group_retry():
    process = subprocess.Popen(
        (sys.executable, "-c", "pass"), start_new_session=True
    )
    try:
        deadline = time.monotonic() + 10
        while (
            os.waitid(
                os.P_PID, process.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG
            )
            is None
        ):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        # waitid(WNOWAIT) observes exit but deliberately retains our zombie.
        # The first group signal can be EPERM even with our own same-UID child.
        assert campaign._stop_group(process) is False
        assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)


def test_expired_global_budget_never_spawns(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("expired attempt may not start any new work")

    monkeypatch.setattr(campaign.subprocess, "Popen", forbidden)
    assert campaign._supervise_campaign(("unused",), 0.0) == (
        None,
        "campaign_deadline",
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
@pytest.mark.parametrize(
    "worker_reason", ["candidate_day_deadline", "invented"]
)
def test_failure_day_retained_and_malformed_status_fails_closed(
    tmp_path, monkeypatch, plan, worker_reason
):
    root = tmp_path / "attempt"

    def finished(*args):
        campaign._publish(
            root,
            "worker-status.json",
            training_json(
                {
                    "stage": "candidates_201001_1",
                    "reason": worker_reason,
                    "failing_utc_date": "2010-01-04",
                    "evaluation_id": None,
                }
            ),
        )
        return 12345, "execution_completed_not_qualification"

    monkeypatch.setattr(campaign, "_supervise_campaign", finished)
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "failed"
    assert result.reason == (
        "candidate_day_deadline"
        if worker_reason != "invented"
        else "worker_failed"
    )
    assert result.failing_utc_date == (
        "2010-01-04" if worker_reason != "invented" else None
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_abrupt_global_timeout_recovers_durable_stage_and_active_day(
    tmp_path, monkeypatch, plan
):
    root = tmp_path / "attempt"

    def stopped(*args):
        campaign._publish(
            root,
            "stage-01.json",
            training_json(
                {
                    "stage": "candidates_201001_1",
                    "artifact_id": "",
                }
            ),
        )
        campaign._publish(
            root,
            "active-day.json",
            training_json(
                {
                    "utc_date": "2010-01-04",
                }
            ),
        )
        return 12345, "campaign_deadline"

    monkeypatch.setattr(campaign, "_supervise_campaign", stopped)
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "failed" and result.reason == "campaign_deadline"
    assert result.last_stage == "candidates_201001_1"
    assert result.failing_utc_date == "2010-01-04"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
@pytest.mark.parametrize(
    ("stage", "day"),
    [
        ("bad stage", "2010-01-04"),
        ("candidates_201001_1", "not-a-date"),
        ("candidates_201001_1", "2099-01-01"),
    ],
)
def test_malformed_checkpoint_cannot_poison_failed_terminal(
    tmp_path, monkeypatch, plan, stage, day
):
    root = tmp_path / "attempt"

    def stopped(*args):
        campaign._publish(
            root,
            "stage-01.json",
            training_json({"stage": stage, "artifact_id": ""}),
        )
        campaign._publish(
            root, "active-day.json", training_json({"utc_date": day})
        )
        return 12345, "campaign_deadline"

    monkeypatch.setattr(campaign, "_supervise_campaign", stopped)
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "failed" and result.reason == "worker_failed"
    assert result.last_stage == "preflight" and result.failing_utc_date is None
    assert (root / "terminal.json").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_success_status_cannot_substitute_for_retained_evaluation(
    tmp_path, monkeypatch, plan
):
    root = tmp_path / "attempt"

    def status_only(*args):
        campaign._publish(
            root,
            "worker-status.json",
            training_json(
                {
                    "stage": "completed",
                    "reason": "execution_completed_not_qualification",
                    "evaluation_id": "training-weight-campaign-evaluation:sha256:"
                    + "a" * 64,
                    "failing_utc_date": None,
                }
            ),
        )
        return 12345, "execution_completed_not_qualification"

    monkeypatch.setattr(campaign, "_supervise_campaign", status_only)
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "failed" and result.reason == "worker_failed"
    assert result.evaluation_id is None


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_interrupt_during_final_report_check_retains_failed_terminal(
    tmp_path, monkeypatch, plan
):
    root = tmp_path / "attempt"

    def status_only(*args):
        campaign._publish(
            root,
            "worker-status.json",
            training_json(
                {
                    "stage": "completed",
                    "reason": "execution_completed_not_qualification",
                    "evaluation_id": "training-weight-campaign-evaluation:sha256:"
                    + "a" * 64,
                    "failing_utc_date": None,
                }
            ),
        )
        return 12345, "execution_completed_not_qualification"

    def interrupted(*args):
        raise KeyboardInterrupt

    monkeypatch.setattr(campaign, "_supervise_campaign", status_only)
    monkeypatch.setattr(campaign, "_verify_completed_report", interrupted)
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "failed" and result.reason == "campaign_cancelled"
    assert result.evaluation_id is None
    assert (root / "terminal.json").is_file()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_complete_tiny_source_campaign_runs_actual_consumers_and_persists_terminal(
    tmp_path, plan
):
    root = tmp_path / "complete"
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "completed", (
        result.to_json(),
        (root / "worker-status.json").read_text(),
    )
    from histdatacom.data_quality.training_weight_workflow import (
        TrainingWeightEvaluationV1,
    )

    evaluation = TrainingWeightEvaluationV1.from_json(
        (root / "evaluation.json").read_text(encoding="ascii")
    )
    assert result.evaluation_id == evaluation.artifact_id
    assert (
        evaluation.allocation.fit.calibration.status.value
        == "insufficient_calibration_units"
    )
    assert (root / "training-export.json").is_file()
    assert (
        campaign.TrainingWeightCampaignAttemptV1.from_json(
            (root / "terminal.json").read_text(encoding="ascii")
        )
        == result
    )


def test_public_empty_export_still_refuses_before_source_reads(
    tmp_path, monkeypatch
):
    import histdatacom.data_quality.training_weight_workflow as workflow

    def forbidden(*args, **kwargs):
        pytest.fail("empty export must refuse before allocation/source reads")

    monkeypatch.setattr(workflow, "read_training_weight_allocation", forbidden)
    with pytest.raises(ValueError, match="row export request count"):
        list(
            workflow.iter_weighted_training_weight_rows(
                tmp_path / "must-not-exist", (), (), ()
            )
        )
    assert not (tmp_path / "must-not-exist").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group runner")
def test_complete_all_refused_source_campaign_retains_unavailable_evaluation(
    tmp_path,
):
    from histdatacom.data_quality.training_weight_contracts import (
        read_training_weight_preregistration,
    )
    from histdatacom.data_quality.training_weight_lineage import (
        WeightEvidenceKind,
    )
    from histdatacom.data_quality.training_weight_workflow import (
        TrainingWeightEvaluationV1,
        TrainingWeightFitV1,
    )

    # Actual small IPC sources contain only Saturdays. None supplies a frozen
    # weekday's required boundary anchors; all refusals follow real replay.
    sources = tuple(
        fixture_weight_source(
            tmp_path / period, dates=(day,), period=period, step_seconds=8
        )[0]
        for period, day in (
            ("201001", "2010-01-02"),
            ("201101", "2011-01-01"),
            ("201102", "2011-02-05"),
        )
    )
    plan = campaign.TrainingWeightCampaignPlanV1(
        sources,
        fixture_weight_model(tmp_path / "model"),
        "synthetic-all-refused",
    )
    root = tmp_path / "complete-all-refused"
    result = campaign.run_training_weight_campaign(plan, root)
    assert result.status == "completed", (
        result.to_json(),
        (root / "worker-status.json").read_text(encoding="ascii"),
    )
    assert result.reason == "execution_completed_not_qualification"
    assert result.last_stage == "completed"
    evaluation = TrainingWeightEvaluationV1.from_json(
        (root / "evaluation.json").read_text(encoding="ascii")
    )
    assert result.evaluation_id == evaluation.artifact_id
    allocation = evaluation.allocation
    fit = allocation.fit
    assert fit.evidence_kind is WeightEvidenceKind.FIXTURE
    assert fit.calibration is None
    assert allocation.days == evaluation.outcomes == ()
    assert evaluation.member_feature_dimensions == ()
    policy = read_training_weight_preregistration()
    for role, shards, inventory, count in (
        ("calibration", fit.shards, fit.inventory, 42),
        ("application", allocation.shards, allocation.inventory, 20),
    ):
        assert len(shards) == (6 if role == "calibration" else 3)
        assert len(inventory) == count
        assert tuple(d.utc_date for d in inventory) == policy.scheduled_dates(
            role
        )
        assert all(
            d.status == "source:missing_boundary_anchor_within_fixed_support"
            and d.historical_unit_id is None
            and d.candidate_day_id is None
            for d in inventory
        )
        assert all(not s.candidate_day_ids for s in shards)
        assert sum(len(s.refusal_records) for s in shards) == count
    for coverage in (
        evaluation.pre_abstention_coverage,
        evaluation.admitted_coverage,
    ):
        assert coverage.numerator == coverage.denominator == 0
        assert coverage.evaluated_unit_ids == ()
        assert coverage.covered_unit_ids == ()
        assert coverage.unavailable_unit_ids == ()
        assert coverage.refused_or_missing_dates == policy.scheduled_dates(
            "application"
        )
    assert len(evaluation.policy_reports) == 9
    for report in evaluation.policy_reports:
        assert report.historical_unit_count == 0
        assert report.unavailable_unit_ids == ()
        assert report.complete_native_support_rows == 0
        assert report.complete_core_rows == 0
        assert report.positive_weight_core_rows == 0
        assert report.member_unit_count == 0
        assert report.positive_weight_member_unit_count == 0
        assert report.total_mass_numerator == "0"
        assert report.total_mass_denominator == "1"
        assert report.aggregate_row_kish_ess is None
    assert len(evaluation.comparisons) == 7
    assert evaluation.holm_pvalues == (None,) * 7
    for comparison in evaluation.comparisons:
        assert comparison.status == "unavailable_no_common_units"
        assert comparison.units == comparison.interval == ()
        assert comparison.mean_difference is comparison.sign_pvalue is None
        assert comparison.nonzero_unit_count == 0
    assert json.loads(
        (root / "training-export.json").read_text(encoding="ascii")
    ) == {"batches": []}
    assert (
        TrainingWeightFitV1.from_json(
            (root / "fit.json").read_text(encoding="ascii")
        )
        == fit
    )
    assert (
        campaign.TrainingWeightCampaignAttemptV1.from_json(
            (root / "terminal.json").read_text(encoding="ascii")
        )
        == result
    )
