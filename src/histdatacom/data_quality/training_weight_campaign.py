"""POSIX-contained complete weighting attempts, never silent retry selection.

The worker is trusted package computation, not an untrusted-plugin sandbox.
Portable stage APIs remain available; only this whole-attempt containment
guarantee requires POSIX process groups. A completed attempt is still not an
empirical success or a release/corpus qualification.
"""

from __future__ import annotations

import errno
import hashlib
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, ClassVar, Sequence

from .training_contracts import TrainingContract, training_json, training_load
from .training_lineage import read_training_regular
from .training_weight_approval import (
    TRAINING_WEIGHT_ALLOWED_OPERATIONS,
    TrainingWeightExecutionApprovalV1,
    read_training_weight_execution_approval,
    require_training_weight_approval,
)
from .training_weight_artifacts import (
    _sync,
    write_training_weight_candidate_shard,
)
from .training_weight_candidates import (
    TrainingWeightCandidateShard,
    TrainingWeightModelV1,
    TrainingWeightOperationalFailure,
    generate_training_weight_candidate_shard,
    _DAY_OBSERVER,
)
from .training_weight_contracts import (
    _array,
    _object,
    _text,
    read_training_weight_preregistration,
)
from .training_weight_lineage import (
    TrainingWeightSourcePlanV1,
    WeightEvidenceKind,
    create_training_weight_degradation,
    _day_ns,
)

MAX_CAMPAIGN_SECONDS = 10800.0
MAX_ATTEMPT_BYTES = 64 * 1024
_STAGES = frozenset(
    {
        "preflight",
        "calibration",
        "allocation",
        "training_export",
        "evaluation",
        "completed",
        *("degradation_" + period for period in ("201001", "201101", "201102")),
        *(
            f"candidates_{period}_{part}"
            for period in ("201001", "201101", "201102")
            for part in (1, 2, 3)
        ),
    }
)


@dataclass(frozen=True, slots=True)
class TrainingWeightCampaignPlanV1(TrainingContract):
    KIND: ClassVar[str] = "weight-campaign-plan"
    sources: tuple[TrainingWeightSourcePlanV1, ...]
    model: TrainingWeightModelV1
    attempt_label: str
    purpose: str = "preregistered_comparison"
    prior_attempt_id: str | None = None

    def _validate(self) -> None:
        if tuple((s.period, s.role) for s in self.sources) != (
            ("201001", "calibration"),
            ("201101", "calibration"),
            ("201102", "application"),
        ):
            raise ValueError("campaign requires the exact three source months")
        if any(
            s.evidence_kind is not self.model.evidence_kind
            for s in self.sources
        ):
            raise ValueError(
                "campaign cannot mix fixture and empirical sources"
            )
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}", self.attempt_label)
            is None
        ):
            raise ValueError("campaign attempt label must be bounded")
        if self.purpose not in (
            "preregistered_comparison",
            "qualification_replay",
        ):
            raise ValueError("unknown campaign purpose")
        if (self.purpose == "qualification_replay") != (
            self.prior_attempt_id is not None
        ):
            raise ValueError("qualification replay requires a prior attempt")
        if (
            self.prior_attempt_id is not None
            and re.fullmatch(
                r"training-weight-campaign-attempt:sha256:[0-9a-f]{64}",
                self.prior_attempt_id,
            )
            is None
        ):
            raise ValueError("invalid prior attempt identity")


@dataclass(frozen=True, slots=True)
class TrainingWeightCampaignAttemptV1(TrainingContract):
    """Operational evidence only; identifiers never authorize source reads."""

    KIND: ClassVar[str] = "weight-campaign-attempt"
    plan_id: str
    evidence_kind: WeightEvidenceKind
    status: str
    reason: str
    elapsed_nanoseconds: int
    worker_pid: int | None
    last_stage: str
    evaluation_id: str | None
    failing_utc_date: str | None = None

    def _validate(self) -> None:
        if (
            re.fullmatch(
                r"training-weight-campaign-plan:sha256:[0-9a-f]{64}",
                self.plan_id,
            )
            is None
        ):
            raise ValueError("attempt lacks retained plan identity")
        if self.status not in ("started", "completed", "failed"):
            raise ValueError("unknown attempt state")
        if self.reason not in (
            "pending",
            "execution_completed_not_qualification",
            "campaign_deadline",
            "campaign_cancelled",
            "candidate_day_deadline",
            "unexpected_candidate_failure",
            "candidate_worker_abnormal_exit",
            "candidate_worker_not_reaped",
            "worker_failed",
            "worker_start_failed",
            "worker_terminal_missing",
        ):
            raise ValueError("unknown attempt outcome reason")
        if self.elapsed_nanoseconds < 0 or self.elapsed_nanoseconds > 2**63 - 1:
            raise ValueError("invalid attempt elapsed clock")
        if self.worker_pid is not None and not 0 < self.worker_pid < 2**31:
            raise ValueError("invalid attempt worker PID")
        if self.failing_utc_date is not None:
            _day_ns(self.failing_utc_date)
            if self.status != "failed":
                raise ValueError("only failed attempts may retain failing day")
        if self.last_stage not in _STAGES:
            raise ValueError("invalid attempt stage")
        if (self.status == "completed") != (self.evaluation_id is not None):
            raise ValueError("only completed execution can retain evaluation")
        if self.status == "started" and self.reason != "pending":
            raise ValueError("started attempt is pending, not a result")
        if self.status == "failed" and self.reason in (
            "pending",
            "execution_completed_not_qualification",
        ):
            raise ValueError("failed attempt must retain operational failure")
        if (
            self.status == "completed"
            and self.reason != "execution_completed_not_qualification"
        ):
            raise ValueError("completion must not imply empirical success")
        if (
            self.evaluation_id is not None
            and re.fullmatch(
                r"training-weight-campaign-evaluation:sha256:[0-9a-f]{64}",
                self.evaluation_id,
            )
            is None
        ):
            raise ValueError("invalid retained evaluation identity")
        if len(self.to_json()) > MAX_ATTEMPT_BYTES:
            raise ValueError("attempt evidence exceeds bound")


def _publish(root: Path, name: str, payload: str) -> Path:
    """Exclusive fsynced records: never replace a prior attempt's evidence."""
    path = root / name
    with path.open("xb") as stream:
        stream.write(payload.encode("ascii"))
        stream.flush()
        os.fsync(stream.fileno())
    _sync(root)
    return path


def _signal_group(process: subprocess.Popen[bytes], signum: int) -> bool:
    """Signal this exact group; never treat persistent denial as cleanup.

    Darwin's killpg1 excludes SZOMB members and can return EPERM while an
    otherwise empty group awaits orphan reaping. Allow that transient state
    to settle, but require an actual successful signal or ESRCH. This does
    not claim to reap grandchildren, change credentials or rerun any work.
    """
    deadline = time.monotonic() + 0.25
    while True:
        try:
            os.killpg(process.pid, signum)
            return True
        except ProcessLookupError:
            return False
        except PermissionError as exc:
            if sys.platform != "darwin" or exc.errno != errno.EPERM:
                raise
            # Reap only our direct child if it is already dead; otherwise a
            # direct-child-only zombie group cannot disappear during retry.
            process.poll()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(0.01, remaining))


def _stop_group(process: subprocess.Popen[bytes]) -> bool:
    # The per-day spawn child deliberately inherits this group. Signal the
    # whole group even if its leader exited; a late native call cannot escape.
    signalled = _signal_group(process, signal.SIGTERM)
    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        pass
    _signal_group(process, signal.SIGKILL)
    process.wait(timeout=2.0)
    return signalled


def _supervise_campaign(
    command: Sequence[str],
    timeout_seconds: float,
    cancelled: Callable[[], bool] | None = None,
) -> tuple[int | None, str]:
    if timeout_seconds <= 0:
        return None, "campaign_deadline"
    start = time.monotonic()
    process = subprocess.Popen(
        command,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    reason = "execution_completed_not_qualification"
    try:
        while process.poll() is None:
            if cancelled is not None and cancelled():
                reason = "campaign_cancelled"
                break
            remaining = timeout_seconds - (time.monotonic() - start)
            if remaining <= 0:
                reason = "campaign_deadline"
                break
            try:
                process.wait(timeout=min(0.2, remaining))
            except subprocess.TimeoutExpired:
                continue
        if reason != "execution_completed_not_qualification":
            _stop_group(process)
        elif process.returncode != 0:
            reason = "worker_failed"
            _stop_group(process)
        elif _stop_group(process):
            # Clean leader exit is insufficient if it abandoned descendants.
            # We reap the direct leader; POSIX init owns orphan reaping. The
            # process-group signal guarantees cessation, not waitpid of a
            # process that is no longer our child.
            reason = "worker_failed"
    except BaseException:
        _stop_group(process)
        raise
    return process.pid, reason


def _verify_completed_report(
    root: Path, plan: TrainingWeightCampaignPlanV1, evaluation_id: str
) -> None:
    """Bounded structural/file reconciliation, not another empirical replay."""
    from .training_weight_workflow import TrainingWeightEvaluationV1

    payload = read_training_regular(root / "evaluation.json", 8 * 1024 * 1024)
    report = TrainingWeightEvaluationV1.from_json(payload.decode("ascii"))
    if (
        report.to_json().encode("ascii") != payload
        or report.artifact_id != evaluation_id
        or report.allocation.fit.model_id != plan.model.artifact_id
        or report.allocation.fit.evidence_kind is not plan.model.evidence_kind
        or {
            s.source_plan_id
            for s in (*report.allocation.fit.shards, *report.allocation.shards)
        }
        != {s.artifact_id for s in plan.sources}
    ):
        raise ValueError("retained evaluation differs from the attempt plan")
    stages = sorted(root.glob("stage-*.json"))
    if not 0 < len(stages) <= 32:
        raise ValueError("completion lacks bounded stage inventory")
    stage = training_load(
        read_training_regular(stages[-1], MAX_ATTEMPT_BYTES).decode("ascii")
    )
    if stage != {"stage": "completed", "artifact_id": evaluation_id}:
        raise ValueError("retained evaluation lacks completed stage binding")


def _last_checkpoint(root: Path) -> tuple[str, str | None]:
    stages = sorted(root.glob("stage-*.json"))
    if len(stages) > 32:
        raise ValueError("campaign checkpoint inventory exceeds bound")
    if tuple(path.name for path in stages) != tuple(
        f"stage-{index:02d}.json" for index in range(1, len(stages) + 1)
    ):
        raise ValueError("campaign checkpoint sequence differs")
    stage = "preflight"
    if stages:
        payload = read_training_regular(stages[-1], MAX_ATTEMPT_BYTES).decode(
            "ascii"
        )
        raw = training_load(payload)
        if (
            set(raw) != {"stage", "artifact_id"}
            or training_json(raw) != payload
        ):
            raise ValueError("campaign checkpoint is not canonical")
        stage = _text(raw["stage"])
        if stage not in _STAGES:
            raise ValueError(
                "campaign checkpoint stage is outside closed scope"
            )
    active = root / "active-day.json"
    day = None
    if active.exists():
        payload = read_training_regular(active, MAX_ATTEMPT_BYTES).decode(
            "ascii"
        )
        raw = training_load(payload)
        if set(raw) != {"utc_date"} or training_json(raw) != payload:
            raise ValueError("active day checkpoint is not canonical")
        if raw["utc_date"] is not None:
            day = _text(raw["utc_date"])
            _day_ns(day)
            policy = read_training_weight_preregistration()
            if day not in (
                *policy.scheduled_dates("calibration"),
                *policy.scheduled_dates("application"),
            ):
                raise ValueError("active checkpoint day is outside campaign")
    return stage, day


def run_training_weight_campaign(
    plan: TrainingWeightCampaignPlanV1,
    directory: str | Path,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> TrainingWeightCampaignAttemptV1:
    """One complete, nonresumable attempt with a single10800second deadline.

    No partial output qualifies after an operational failure. A fresh attempt
    needs a fresh directory and explicit label; no automatic retry or choice
    of a faster successful attempt occurs. Qualification replay is separately
    labelled and cannot supply additional historical evidence units.
    """
    if os.name != "posix":
        raise NotImplementedError("hard-supervised campaigns require POSIX")
    if type(plan) is not TrainingWeightCampaignPlanV1:
        raise TypeError("campaign requires a complete typed plan")
    start = time.monotonic()
    if plan.model.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        for operation in TRAINING_WEIGHT_ALLOWED_OPERATIONS:
            require_training_weight_approval(approval, operation)
    root = Path(directory)
    if root.exists() or root.is_symlink():
        raise ValueError("campaign requires a new attempt directory")
    root.mkdir(parents=True, exist_ok=False)
    _sync(root.parent)
    _publish(root, "plan.json", plan.to_json())
    if approval is not None:
        # An exact retained copy of external coordinator approval, not a new
        # authorisation or an approval provider/default generated by this API.
        _publish(root, "approval.json", approval.to_json())
    started = TrainingWeightCampaignAttemptV1(
        plan.artifact_id,
        plan.model.evidence_kind,
        "started",
        "pending",
        0,
        None,
        "preflight",
        None,
    )
    _publish(root, "started.json", started.to_json())
    pid = None
    stage = "preflight"
    evaluation_id = None
    failing_day = None
    checkpoint_stage = "preflight"
    checkpoint_day = None
    reason = "worker_start_failed"
    try:
        pid, reason = _supervise_campaign(
            (sys.executable, "-m", __name__, "--worker", str(root.absolute())),
            max(0.0, MAX_CAMPAIGN_SECONDS - (time.monotonic() - start)),
            cancelled,
        )
        stage, failing_day = _last_checkpoint(root)
        checkpoint_stage, checkpoint_day = stage, failing_day
        status_path = root / "worker-status.json"
        if status_path.exists():
            status_text = read_training_regular(
                status_path, MAX_ATTEMPT_BYTES
            ).decode("ascii")
            status = training_load(status_text)
            if (
                set(status)
                != {"stage", "reason", "evaluation_id", "failing_utc_date"}
                or training_json(status) != status_text
            ):
                raise ValueError(
                    "worker terminal has unknown/noncanonical fields"
                )
            stage = _text(status["stage"])
            if status.get("failing_utc_date") is not None:
                failing_day = _text(status["failing_utc_date"])
                if failing_day not in {
                    day for source in plan.sources for day in source.dates
                }:
                    raise ValueError("worker failing day is outside campaign")
            if reason == "execution_completed_not_qualification":
                reason = _text(status["reason"])
                if reason == "execution_completed_not_qualification":
                    evaluation_id = _text(status["evaluation_id"])
                    if stage != "completed" or failing_day is not None:
                        raise ValueError(
                            "worker completion contradicts stage/failure"
                        )
                    if time.monotonic() - start >= MAX_CAMPAIGN_SECONDS:
                        reason = "campaign_deadline"
                        evaluation_id = None
                    else:
                        _verify_completed_report(root, plan, evaluation_id)
                        if time.monotonic() - start >= MAX_CAMPAIGN_SECONDS:
                            reason = "campaign_deadline"
                            evaluation_id = None
        elif reason == "execution_completed_not_qualification":
            reason = "worker_terminal_missing"
        if failing_day is not None and failing_day not in {
            day for source in plan.sources for day in source.dates
        }:
            raise ValueError("worker failing day is outside campaign")
        # Validate worker fields inside the guarded block; malformed status
        # must still leave a bounded failed terminal record, not escape later.
        TrainingWeightCampaignAttemptV1(
            plan.artifact_id,
            plan.model.evidence_kind,
            "completed" if evaluation_id is not None else "failed",
            reason,
            0,
            pid,
            stage,
            evaluation_id,
            failing_day,
        )
    except KeyboardInterrupt:
        reason = "campaign_cancelled"
        stage = checkpoint_stage
        evaluation_id = None
        failing_day = checkpoint_day
    except Exception:
        reason = "worker_failed"
        stage = checkpoint_stage
        evaluation_id = None
        failing_day = checkpoint_day
    terminal = TrainingWeightCampaignAttemptV1(
        plan.artifact_id,
        plan.model.evidence_kind,
        "completed" if evaluation_id is not None else "failed",
        reason,
        int((time.monotonic() - start) * 1_000_000_000),
        pid,
        stage,
        evaluation_id,
        failing_day,
    )
    _publish(root, "terminal.json", terminal.to_json())
    return terminal


def _execute_campaign(
    root: Path,
    plan: TrainingWeightCampaignPlanV1,
    approval: TrainingWeightExecutionApprovalV1 | None,
) -> str:
    def observe(day: str | None) -> None:
        # A single atomically replaced *operational* checkpoint is owned by
        # this fresh attempt directory. It cannot qualify rows or carry work
        # into another attempt; interrupted writes retain the earlier state.
        pending = root / "active-day.pending"
        with pending.open("xb") as stream:
            stream.write(training_json({"utc_date": day}).encode("ascii"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, root / "active-day.json")
        _sync(root)

    token = _DAY_OBSERVER.set(observe)
    try:
        return _execute_campaign_body(root, plan, approval)
    finally:
        _DAY_OBSERVER.reset(token)


def _execute_campaign_body(
    root: Path,
    plan: TrainingWeightCampaignPlanV1,
    approval: TrainingWeightExecutionApprovalV1 | None,
) -> str:
    from .training_weight_workflow import (
        TrainingWeightRowRequestV1,
        allocate_training_weight_campaign,
        evaluate_training_weight_campaign,
        fit_training_weight_campaign,
        iter_weighted_training_weight_rows,
        write_training_weight_allocation,
    )

    number = 0

    def checkpoint(stage: str, identity: str = "") -> None:
        nonlocal number
        number += 1
        if number > 32:
            raise ValueError("campaign stage inventory exceeds bound")
        _publish(
            root,
            f"stage-{number:02d}.json",
            training_json({"stage": stage, "artifact_id": identity}),
        )

    shards: list[TrainingWeightCandidateShard] = []
    policy = read_training_weight_preregistration().to_dict()
    for source in plan.sources:
        checkpoint("degradation_" + source.period)
        degradation = create_training_weight_degradation(
            source, root / ("subset-" + source.period), approval=approval
        )
        schedule = next(
            _object(v)
            for v in _array(policy["schedule"])
            if _object(v)["period"] == source.period
        )
        for raw in _array(schedule["shards"]):
            shard_id = _text(_object(raw)["shard_id"])
            checkpoint("candidates_" + shard_id.replace("-", "_"))
            shard = generate_training_weight_candidate_shard(
                degradation, shard_id, plan.model, approval=approval
            )
            write_training_weight_candidate_shard(
                shard, root / ("shard-" + shard_id), approval=approval
            )
            shards.append(shard)
    fit_shards = tuple(
        s for s in shards if s.degradation.source_plan.role == "calibration"
    )
    app_shards = tuple(
        s for s in shards if s.degradation.source_plan.role == "application"
    )
    checkpoint("calibration")
    fit = fit_training_weight_campaign(fit_shards, approval=approval)
    _publish(root, "fit.json", fit.to_json())
    checkpoint("allocation", fit.artifact_id)
    allocation = allocate_training_weight_campaign(
        app_shards, fit, fit_shards, approval=approval
    )
    allocation_path = write_training_weight_allocation(
        allocation,
        root / "allocation",
        app_shards,
        fit_shards,
        approval=approval,
    )
    checkpoint("training_export", allocation.artifact_id)
    requests = tuple(
        TrainingWeightRowRequestV1(
            day.utc_date,
            choice.policy,
            choice.training_epoch,
            (member.member_id,),
        )
        for day in allocation.days
        for choice in day.policies
        if choice.status != "confidence_unavailable"
        for member in choice.members
        if member.mass > 0
    )
    exports: list[dict[str, object]] = []
    # Complete source replay can admit no days. Retain that empty export
    # without invoking the public iterator's nonempty-request contract.
    batches = (
        iter_weighted_training_weight_rows(
            allocation_path, app_shards, fit_shards, requests, approval=approval
        )
        if requests
        else ()
    )
    for batch in batches:
        # Execute every positive-mass policy/member consumer. Retain exact
        # batch identity/count, not another unbounded copy of all native rows.
        exports.append(
            {
                "batch_id": batch.artifact_id,
                "rows": len(batch.rows),
                "sha256": hashlib.sha256(
                    batch.to_json().encode("ascii")
                ).hexdigest(),
            }
        )
    _publish(root, "training-export.json", training_json({"batches": exports}))
    checkpoint("evaluation", allocation.artifact_id)
    evaluation = evaluate_training_weight_campaign(
        allocation_path, app_shards, fit_shards, approval=approval
    )
    _publish(root, "evaluation.json", evaluation.to_json())
    checkpoint("completed", evaluation.artifact_id)
    return evaluation.artifact_id


def _worker(root: Path) -> None:
    def interrupted(signum: int, frame: object) -> None:
        raise TrainingWeightOperationalFailure("campaign_cancelled")

    signal.signal(signal.SIGTERM, interrupted)
    reason = "worker_failed"
    evaluation_id = None
    failing_day = None
    try:
        plan = TrainingWeightCampaignPlanV1.from_json(
            read_training_regular(root / "plan.json", 8 * 1024 * 1024).decode(
                "ascii"
            )
        )
        approval_path = root / "approval.json"
        approval = (
            read_training_weight_execution_approval(approval_path)
            if approval_path.exists()
            else None
        )
        evaluation_id = _execute_campaign(root, plan, approval)
        reason = "execution_completed_not_qualification"
    except TrainingWeightOperationalFailure as exc:
        reason = exc.reason
        failing_day = exc.utc_date or None
    except Exception:
        pass
    stages = sorted(root.glob("stage-*.json"))
    stage = "preflight"
    if stages and len(stages) <= 32:
        stage = _text(
            training_load(
                read_training_regular(stages[-1], MAX_ATTEMPT_BYTES).decode(
                    "ascii"
                )
            )["stage"]
        )
    _publish(
        root,
        "worker-status.json",
        training_json(
            {
                "stage": stage,
                "reason": reason,
                "evaluation_id": evaluation_id,
                "failing_utc_date": failing_day,
            }
        ),
    )


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit(
            "private campaign worker requires its exact plan directory"
        )
    _worker(Path(sys.argv[2]))
