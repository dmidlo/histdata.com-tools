"""Strict bounded evidence files and deterministic independent report replay."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from ._wire import MAX_CONFORMANCE_BYTES, Artifact
from .assessment import assess_broker_conformance
from .contracts import (
    BrokerConformanceCaseResultV1,
    BrokerConformancePlanV1,
    BrokerConformanceReason,
    BrokerConformanceReportV1,
    BrokerConformanceStatus,
)
from .evidence import (
    BrokerConformanceEvidenceV1,
    BrokerConformanceRunnerOutcomeV1,
)
from .verification import (
    verify_conformance_equivalence,
    verify_conformance_evidence,
)


def read_text(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        details = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_size > MAX_CONFORMANCE_BYTES
        ):
            raise ValueError("invalid bounded conformance file")
        data = stream.read(MAX_CONFORMANCE_BYTES + 1)
    if len(data) != details.st_size or len(data) > MAX_CONFORMANCE_BYTES:
        raise ValueError("changed conformance file")
    return data.decode("ascii")


def write_artifact(path: Path, artifact: Artifact) -> None:
    data = artifact.to_json().encode("ascii")
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _evidence_ids(directory: Path) -> tuple[str, ...]:
    identities = []
    for relative in (
        "evidence.json",
        "trusted/evidence.json",
        "repeat/evidence.json",
    ):
        path = directory / relative
        if path.parent != directory and path.parent.is_symlink():
            raise ValueError("symlink paired evidence directory")
        if path.exists():
            try:
                evidence = BrokerConformanceEvidenceV1.from_json(
                    read_text(path)
                )
            except (ValueError, OSError):
                continue
            identities.append(evidence.artifact_id)
    if len(identities) > 2:
        raise ValueError("unexpected paired evidence inventory")
    return tuple(sorted(identities))


def write_runner_outcome(
    plan: BrokerConformancePlanV1,
    case_id: str,
    status: BrokerConformanceStatus,
    reason: BrokerConformanceReason,
    directory: Path,
) -> BrokerConformanceCaseResultV1:
    outcome = BrokerConformanceRunnerOutcomeV1(
        plan.artifact_id, case_id, status, reason, _evidence_ids(directory)
    )
    write_artifact(directory / "runner-outcome.json", outcome)
    return BrokerConformanceCaseResultV1(
        case_id, status, reason, plan.subject, outcome.evidence_ids
    )


def verify_broker_conformance(directory: Path) -> BrokerConformanceReportV1:
    """Exact same retained evidence yields exact same canonical report bytes."""
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("symlink conformance directory")
    plan = BrokerConformancePlanV1.from_json(read_text(directory / "plan.json"))
    report = BrokerConformanceReportV1.from_json(
        read_text(directory / "report.json")
    )
    if report.plan != plan:
        raise ValueError("report plan substituted")
    expected = {"plan.json", "report.json", *plan.case_ids}
    names: set[str] = set()
    with os.scandir(directory) as entries:
        for item in entries:
            names.add(item.name)
            if len(names) > len(expected):
                raise ValueError(
                    "conformance directory inventory exceeds bound"
                )
    if names != expected:
        raise ValueError("conformance directory inventory changed")
    results = []
    for supplied in report.results:
        case_dir = directory / supplied.case_id
        if not case_dir.is_dir() or case_dir.is_symlink():
            raise ValueError("invalid conformance case directory")
        path = case_dir / "evidence.json"
        if (case_dir / "runner-outcome.json").exists():
            outcome = BrokerConformanceRunnerOutcomeV1.from_json(
                read_text(case_dir / "runner-outcome.json")
            )
            if (
                outcome.plan_id != plan.artifact_id
                or outcome.case_id != supplied.case_id
                or outcome.evidence_ids != _evidence_ids(case_dir)
            ):
                raise ValueError("runner outcome evidence changed")
            result = BrokerConformanceCaseResultV1(
                outcome.case_id,
                outcome.status,
                outcome.reason,
                plan.subject,
                outcome.evidence_ids,
            )
            if result != supplied:
                raise ValueError("runner outcome differs from report")
            results.append(result)
        elif path.exists():
            evidence = BrokerConformanceEvidenceV1.from_json(read_text(path))
            if supplied.case_id in {
                "execution.equivalence",
                "replay.determinism",
                "session.freshness",
            }:
                second_dir = case_dir / (
                    "trusted"
                    if supplied.case_id == "execution.equivalence"
                    else "repeat"
                )
                if second_dir.is_symlink() or not second_dir.is_dir():
                    raise ValueError("invalid paired evidence directory")
                second = BrokerConformanceEvidenceV1.from_json(
                    read_text(second_dir / "evidence.json")
                )
                result = verify_conformance_equivalence(
                    plan, evidence, second, case_dir
                )
            else:
                result = verify_conformance_evidence(plan, evidence, case_dir)
            if result != supplied:
                raise ValueError(
                    "conformance verdict differs from native replay"
                )
            results.append(result)
        else:
            # Interrupted/unsupported/missing scenarios remain in the complete
            # denominator, but absent evidence can never be promoted to PASS.
            if supplied.evidence_ids or supplied.status.value == "pass":
                raise ValueError("conformance execution evidence missing")
            results.append(supplied)
    replay = assess_broker_conformance(plan, tuple(results))
    if replay.to_json() != report.to_json():
        raise ValueError("conformance report replay differs")
    return replay
