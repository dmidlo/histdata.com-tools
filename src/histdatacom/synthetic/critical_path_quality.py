"""Commit-bound quality evidence for the reconstruction critical path.

Repository-wide averages can conceal untested branches in fail-closed
scientific code.  This module defines a small, content-addressed report shared
by the branch-coverage, property, and mutation gates.  The executable runner
is kept in ``scripts/critical_path_quality.py`` so the runtime package does not
need to invoke pytest or mutate source code.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

CRITICAL_PATH_GATE_REPORT_SCHEMA_VERSION = (
    "histdatacom.critical-path-gate-report.v1"
)
CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS = {
    "critical_branch_coverage": "critical_branch_coverage_report_v1",
    "critical_property_invariants": "critical_property_invariants_report_v1",
    "critical_mutation_testing": "critical_mutation_testing_report_v1",
}
MAX_CRITICAL_PATH_CHECKS = 256
MAX_CRITICAL_PATH_EVIDENCE_ITEMS = 64
MAX_CRITICAL_PATH_REPORT_BYTES = 4 * 1024 * 1024

_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class CriticalPathGateCheckV1:
    """One named pass/fail observation within a critical-path gate."""

    check_id: str
    status: str
    evidence: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "check_id", _required_text(self.check_id))
        status = _required_text(self.status).lower()
        if status not in {"passed", "failed"}:
            raise ValueError("critical-path check status is invalid")
        object.__setattr__(self, "status", status)
        if not isinstance(self.evidence, Mapping):
            raise TypeError("critical-path check evidence must be a mapping")
        if len(self.evidence) > MAX_CRITICAL_PATH_EVIDENCE_ITEMS:
            raise ValueError("critical-path check evidence exceeds limit")
        evidence = {
            _required_text(str(key)): value
            for key, value in sorted(self.evidence.items())
        }
        canonical_contract_json(evidence)
        object.__setattr__(self, "evidence", evidence)

    @property
    def passed(self) -> bool:
        """Return whether the check passed."""
        return self.status == "passed"

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible check evidence."""
        return {
            "check_id": self.check_id,
            "status": self.status,
            "evidence": dict(self.evidence),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CriticalPathGateCheckV1:
        """Restore one check from JSON-compatible evidence."""
        return cls(
            check_id=str(data.get("check_id", "")),
            status=str(data.get("status", "")),
            evidence=_mapping(data.get("evidence")),
        )


@dataclass(frozen=True, slots=True)
class CriticalPathGateReportV1:
    """Exact report for one commit-bound reconstruction quality gate."""

    gate_name: str
    git_commit_sha: str
    command: str
    profile: str
    source_hashes: Mapping[str, str]
    checks: tuple[CriticalPathGateCheckV1, ...]
    created_at_utc: str
    passed: bool = True
    report_id: str = ""
    schema_version: str = CRITICAL_PATH_GATE_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CRITICAL_PATH_GATE_REPORT_SCHEMA_VERSION:
            raise ValueError("unsupported critical-path gate report schema")
        gate_name = _required_text(self.gate_name)
        if gate_name not in CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS:
            raise ValueError("unsupported critical-path gate name")
        object.__setattr__(self, "gate_name", gate_name)
        object.__setattr__(
            self, "git_commit_sha", _git_object_id(self.git_commit_sha)
        )
        object.__setattr__(self, "command", _required_text(self.command))
        object.__setattr__(self, "profile", _required_text(self.profile))
        hashes = {
            _required_text(str(name)): _sha256(value)
            for name, value in sorted(self.source_hashes.items())
        }
        if not hashes or len(hashes) > MAX_CRITICAL_PATH_EVIDENCE_ITEMS:
            raise ValueError("critical-path source hash set is invalid")
        object.__setattr__(self, "source_hashes", hashes)
        checks = tuple(sorted(self.checks, key=lambda item: item.check_id))
        if (
            not checks
            or len(checks) > MAX_CRITICAL_PATH_CHECKS
            or any(
                not isinstance(item, CriticalPathGateCheckV1) for item in checks
            )
        ):
            raise ValueError("critical-path checks are invalid")
        if len({item.check_id for item in checks}) != len(checks):
            raise ValueError("critical-path checks must be unique")
        object.__setattr__(self, "checks", checks)
        observed_pass = all(item.passed for item in checks)
        if self.passed is not observed_pass:
            raise ValueError(
                "critical-path report pass state differs from checks"
            )
        object.__setattr__(
            self, "created_at_utc", _timestamp(self.created_at_utc)
        )
        expected = _stable_id("critical-path-gate-report", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("critical-path gate report identity differs")
        object.__setattr__(self, "report_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_CRITICAL_PATH_REPORT_BYTES:
            raise ValueError("critical-path gate report exceeds size limit")

    def payload(self) -> dict[str, JSONValue]:
        """Return identity-bearing report content."""
        return {
            "schema_version": self.schema_version,
            "gate_name": self.gate_name,
            "git_commit_sha": self.git_commit_sha,
            "command": self.command,
            "profile": self.profile,
            "source_hashes": dict(self.source_hashes),
            "checks": [item.to_dict() for item in self.checks],
            "created_at_utc": self.created_at_utc,
            "passed": self.passed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible report content."""
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CriticalPathGateReportV1:
        """Restore and verify a report from JSON-compatible content."""
        return cls(
            gate_name=str(data.get("gate_name", "")),
            git_commit_sha=str(data.get("git_commit_sha", "")),
            command=str(data.get("command", "")),
            profile=str(data.get("profile", "")),
            source_hashes={
                str(key): str(value)
                for key, value in _mapping(data.get("source_hashes")).items()
            },
            checks=tuple(
                CriticalPathGateCheckV1.from_dict(_mapping(item))
                for item in _sequence(data.get("checks"))
            ),
            created_at_utc=str(data.get("created_at_utc", "")),
            passed=data.get("passed") is True,
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def write_critical_path_gate_report(
    report: CriticalPathGateReportV1,
    path: str | Path,
) -> ArtifactRef:
    """Atomically persist a report and return release-gate-ready evidence."""
    if not isinstance(report, CriticalPathGateReportV1):
        raise TypeError("critical-path report must be a v1 report")
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (report.to_json() + "\n").encode("utf-8")
    temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return artifact_ref_for_file(
        target,
        kind=CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS[report.gate_name],
        metadata={
            "gate_name": report.gate_name,
            "git_commit_sha": report.git_commit_sha,
            "passed": report.passed,
            "report_id": report.report_id,
            "check_count": len(report.checks),
            "profile": report.profile,
        },
    )


def read_critical_path_gate_report(
    path: str | Path,
) -> CriticalPathGateReportV1:
    """Read and revalidate exact report bytes."""
    target = Path(path).expanduser().resolve()
    if (
        not target.is_file()
        or target.stat().st_size > MAX_CRITICAL_PATH_REPORT_BYTES
    ):
        raise ValueError("critical-path report file is missing or oversized")
    return CriticalPathGateReportV1.from_dict(
        _mapping(json.loads(target.read_text(encoding="utf-8")))
    )


def utc_now() -> str:
    """Return a normalized UTC timestamp for evidence generation."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def file_sha256(path: str | Path) -> str:
    """Hash one evidence or source file without loading it all into memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("critical-path report value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("critical-path report value must be a sequence")
    return value


def _required_text(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("critical-path report text is required")
    return normalized


def _sha256(value: str) -> str:
    normalized = _required_text(value).lower()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError("critical-path SHA-256 is invalid")
    return normalized


def _git_object_id(value: str) -> str:
    normalized = _required_text(value).lower()
    if _GIT_OBJECT_ID.fullmatch(normalized) is None:
        raise ValueError("critical-path full Git object ID is invalid")
    return normalized


def _timestamp(value: str) -> str:
    normalized = _required_text(value)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError("critical-path timestamp is invalid") from err
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("critical-path timestamp requires a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS",
    "CRITICAL_PATH_GATE_REPORT_SCHEMA_VERSION",
    "CriticalPathGateCheckV1",
    "CriticalPathGateReportV1",
    "file_sha256",
    "read_critical_path_gate_report",
    "utc_now",
    "write_critical_path_gate_report",
]
