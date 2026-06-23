"""Report serialization and exit policy helpers for data-quality runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from pathlib import Path

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityRunSummary,
    QualityStatus,
    QualityTargetSummary,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

QUALITY_REPORT_SCHEMA_VERSION = "histdatacom.quality-report.v1"


class QualityExitTrigger(str, Enum):
    """Quality severities that can make a process exit non-zero."""

    ERROR = "error"
    WARNING = "warning"
    NEVER = "never"

    @classmethod
    def from_value(
        cls, value: str | "QualityExitTrigger" | None
    ) -> "QualityExitTrigger":
        """Normalize a public exit trigger value."""
        if isinstance(value, cls):
            return value
        normalized = str(value or cls.ERROR.value).strip().lower()
        aliases = {
            "errors": cls.ERROR,
            "warn": cls.WARNING,
            "warnings": cls.WARNING,
            "none": cls.NEVER,
            "off": cls.NEVER,
            "false": cls.NEVER,
        }
        if normalized in aliases:
            return aliases[normalized]
        try:
            return cls(normalized)
        except ValueError as exc:
            msg = f"unknown quality exit trigger: {value!r}"
            raise ValueError(msg) from exc


QUALITY_EXIT_TRIGGERS = tuple(trigger.value for trigger in QualityExitTrigger)


@dataclass(frozen=True, slots=True)
class QualityExitPolicy:
    """Configurable thresholds for quality-run process exit behavior."""

    fail_on: QualityExitTrigger = QualityExitTrigger.ERROR
    max_errors: int = 0
    max_warnings: int = 0

    @classmethod
    def from_values(
        cls,
        *,
        fail_on: str | QualityExitTrigger | None = None,
        max_errors: int = 0,
        max_warnings: int = 0,
    ) -> "QualityExitPolicy":
        """Create a validated exit policy from public values."""
        if max_errors < 0:
            msg = "quality max error threshold must be non-negative"
            raise ValueError(msg)
        if max_warnings < 0:
            msg = "quality max warning threshold must be non-negative"
            raise ValueError(msg)
        return cls(
            fail_on=QualityExitTrigger.from_value(fail_on),
            max_errors=max_errors,
            max_warnings=max_warnings,
        )

    def evaluate(self, summary: QualityRunSummary) -> "QualityExitDecision":
        """Return the process-exit decision for a quality summary."""
        if self.fail_on is QualityExitTrigger.NEVER:
            return QualityExitDecision(
                exit_code=0,
                reason="quality exit policy is disabled",
                policy=self,
            )
        if summary.error_count > self.max_errors:
            return QualityExitDecision(
                exit_code=1,
                reason=(
                    "quality error threshold exceeded: "
                    f"{summary.error_count} > {self.max_errors}"
                ),
                policy=self,
            )
        if (
            self.fail_on is QualityExitTrigger.WARNING
            and summary.warning_count > self.max_warnings
        ):
            return QualityExitDecision(
                exit_code=1,
                reason=(
                    "quality warning threshold exceeded: "
                    f"{summary.warning_count} > {self.max_warnings}"
                ),
                policy=self,
            )
        return QualityExitDecision(
            exit_code=0,
            reason="quality report is within configured exit policy",
            policy=self,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "fail_on": self.fail_on.value,
            "max_errors": self.max_errors,
            "max_warnings": self.max_warnings,
        }


@dataclass(frozen=True, slots=True)
class QualityExitDecision:
    """Result of applying a quality exit policy to a report summary."""

    exit_code: int
    reason: str
    policy: QualityExitPolicy

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "exit_code": self.exit_code,
            "reason": self.reason,
            "policy": self.policy.to_dict(),
        }


def quality_report_payload(report: QualityReport) -> dict[str, JSONValue]:
    """Return the stable JSON report payload for a quality report."""
    payload: dict[str, JSONValue] = dict(report.to_dict())
    payload["schema_version"] = QUALITY_REPORT_SCHEMA_VERSION
    return payload


def quality_report_to_json(report: QualityReport) -> str:
    """Return deterministic formatted JSON for a quality report."""
    return json.dumps(
        quality_report_payload(report),
        indent=2,
        sort_keys=True,
    )


def write_quality_report(
    report: QualityReport,
    path: str | Path,
) -> ArtifactRef:
    """Write a JSON quality report and return its artifact reference."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = f"{quality_report_to_json(report)}\n".encode("utf-8")
    output.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    summary = report.summary()
    return ArtifactRef(
        kind="quality-report",
        path=str(output.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "schema_version": QUALITY_REPORT_SCHEMA_VERSION,
            "status": summary.status.value,
            "max_severity": summary.max_severity.value,
            "target_count": summary.target_count,
            "finding_count": summary.finding_count,
            "warning_count": summary.warning_count,
            "error_count": summary.error_count,
        },
    )


def format_quality_console_summary(
    report: QualityReport,
    *,
    check_groups: tuple[str, ...] = (),
    artifact: ArtifactRef | None = None,
) -> str:
    """Return a human-readable quality summary derived from the report."""
    summary = report.summary()
    lines = [
        "Data quality assessment",
        "checks: " + ", ".join(check_groups or ("all",)),
        f"status: {summary.status.value}",
        (
            "targets: "
            f"{summary.target_count} "
            f"clean: {_target_count(report, QualityStatus.CLEAN)} "
            f"warning: {_target_count(report, QualityStatus.WARNING)} "
            f"failed: {_target_count(report, QualityStatus.FAILED)}"
        ),
        (
            "findings: "
            f"{summary.finding_count} "
            f"info: {summary.info_count} "
            f"warning: {summary.warning_count} "
            f"error: {summary.error_count}"
        ),
    ]
    if artifact is not None:
        lines.append(f"report: {artifact.path}")
    if summary.target_count == 0:
        lines.append("No data quality targets discovered.")

    sections = (
        (QualityStatus.CLEAN, "Clean files"),
        (QualityStatus.WARNING, "Warning files"),
        (QualityStatus.FAILED, "Failed files"),
    )
    for status, title in sections:
        lines.extend(("", title))
        target_lines = tuple(
            _format_target_summary(item)
            for item in report.target_summaries
            if item.status is status
        )
        if not target_lines:
            lines.append("- none")
        else:
            lines.extend(target_lines)
    return "\n".join(lines)


def bounded_quality_payload(
    *,
    operation: str,
    check_groups: tuple[str, ...],
    discovery: Mapping[str, JSONValue],
    report: QualityReport,
    decision: QualityExitDecision,
    artifact: ArtifactRef | None,
) -> dict[str, JSONValue]:
    """Return a bounded result payload without detailed findings."""
    return {
        "operation": operation,
        "check_groups": list(check_groups),
        "discovery": dict(discovery),
        "summary": report.summary().to_dict(),
        "target_summaries": [
            summary.to_dict() for summary in report.target_summaries
        ],
        "report_schema_version": QUALITY_REPORT_SCHEMA_VERSION,
        "report_artifact": None if artifact is None else artifact.to_dict(),
        "exit_decision": decision.to_dict(),
    }


def _target_count(report: QualityReport, status: QualityStatus) -> int:
    return sum(
        1 for summary in report.target_summaries if summary.status is status
    )


def _format_target_summary(summary: QualityTargetSummary) -> str:
    target = summary.target
    return (
        f"- {target.kind.value}: {target.path} "
        f"(findings={summary.finding_count}, "
        f"warnings={summary.warning_count}, errors={summary.error_count})"
    )
