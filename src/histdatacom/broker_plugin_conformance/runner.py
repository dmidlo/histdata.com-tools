"""Bounded fresh-interpreter candidate runs, with no driver verdict hooks."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from .assessment import assess_broker_conformance
from .catalog import broker_conformance_catalog, verify_plan
from .contracts import (
    BrokerConformancePlanV1,
    BrokerConformanceReportV1,
    BrokerConformanceSubject,
)
from .contracts import (
    BrokerConformanceReason as Reason,
)
from .contracts import (
    BrokerConformanceStatus as Status,
)
from .evidence import BrokerConformanceEvidenceV1
from .storage import read_text, write_artifact, write_runner_outcome
from .verification import (
    verify_conformance_equivalence,
    verify_conformance_evidence,
)


def _execute(
    plan: BrokerConformancePlanV1,
    case_id: str,
    root: Path,
    directory: Path,
    cancelled: Callable[[], bool],
    *,
    equivalence: bool = False,
) -> tuple[Status, Reason] | None:
    command = [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "histdatacom.broker_plugin_conformance._worker",
        str(root / "plan.json"),
        case_id,
        str(directory),
    ]
    if equivalence:
        command.append("--trusted-equivalence")
    # No inherited PYTHONPATH, credentials, proxy variables, user site or cwd
    # imports. Candidate discovery is the chosen interpreter's installed set.
    environment = {"LANG": "C", "PATH": os.defpath}
    started = time.monotonic()
    with subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=directory,
        env=environment,
        start_new_session=True,
    ) as child:
        while child.poll() is None:
            stopped = cancelled()
            timeout = time.monotonic() - started >= plan.case_timeout_ms / 1000
            if stopped or timeout:
                # First unwind native cleanup: its owned kernel worker has a
                # distinct process group and must be reaped by its supervisor.
                child.terminate()
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    if os.name == "posix":
                        os.killpg(child.pid, signal.SIGKILL)
                    else:
                        child.kill()
                    child.wait(timeout=5)
                return (
                    (Status.NOT_RUN, Reason.CANCELLED)
                    if stopped
                    else (Status.ERROR, Reason.TIMEOUT)
                )
            try:
                child.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        if child.returncode:
            return Status.ERROR, Reason.EXECUTION
    return None


def run_broker_conformance(
    plan: BrokerConformancePlanV1,
    directory: Path,
    *,
    cancelled: Callable[[], bool] = lambda: False,
) -> BrokerConformanceReportV1:
    """Execute the entire frozen denominator; never overwrite a prior run."""
    verify_plan(plan)
    directory = Path(directory).absolute()
    directory.mkdir(mode=0o700)
    write_artifact(directory / "plan.json", plan)
    scenarios = {scenario.scenario_id for scenario in plan.driver.scenarios}
    catalog = {
        case.case_id: case for case in broker_conformance_catalog().cases
    }
    results = []
    for case_id in plan.case_ids:
        case_dir = directory / case_id
        case_dir.mkdir(mode=0o700)
        absent = None
        if cancelled():
            absent = Status.NOT_RUN, Reason.CANCELLED
        elif catalog[case_id].scenario_id not in scenarios:
            absent = Status.NOT_RUN, Reason.DRIVER
        elif (
            case_id in {"queue.overflow", "provenance.mutations"}
            and plan.subject is not BrokerConformanceSubject.REFERENCE
        ):
            absent = Status.UNSUPPORTED, Reason.CAPABILITY
        else:
            absent = _execute(plan, case_id, directory, case_dir, cancelled)
        if absent is not None:
            results.append(
                write_runner_outcome(plan, case_id, *absent, case_dir)
            )
            continue
        try:
            evidence = BrokerConformanceEvidenceV1.from_json(
                read_text(case_dir / "evidence.json")
            )
            if case_id in {
                "execution.equivalence",
                "replay.determinism",
                "session.freshness",
            }:
                second_dir = case_dir / (
                    "trusted"
                    if case_id == "execution.equivalence"
                    else "repeat"
                )
                second_dir.mkdir(mode=0o700)
                failed = _execute(
                    plan,
                    case_id,
                    directory,
                    second_dir,
                    cancelled,
                    equivalence=case_id == "execution.equivalence",
                )
                if failed is not None:
                    results.append(
                        write_runner_outcome(plan, case_id, *failed, case_dir)
                    )
                    continue
                second = BrokerConformanceEvidenceV1.from_json(
                    read_text(second_dir / "evidence.json")
                )
                result = verify_conformance_equivalence(
                    plan, evidence, second, case_dir
                )
            else:
                result = verify_conformance_evidence(plan, evidence, case_dir)
            results.append(result)
        except (ValueError, OSError):
            results.append(
                write_runner_outcome(
                    plan, case_id, Status.ERROR, Reason.EVIDENCE, case_dir
                )
            )
    report = assess_broker_conformance(plan, tuple(results))
    write_artifact(directory / "report.json", report)
    return report
