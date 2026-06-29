"""Report serialization and exit policy helpers for data-quality runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from pathlib import Path

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRunSummary,
    QualitySeverity,
    QualityStatus,
    QualityTargetSummary,
)
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

QUALITY_REPORT_SCHEMA_VERSION = "histdatacom.quality-report.v1"
QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT = 128
QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT = 128
QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT = 128


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


def quality_report_payload(
    report: QualityReport,
    *,
    publish_safe: bool = True,
) -> dict[str, JSONValue]:
    """Return the stable JSON report payload for a quality report."""
    payload: dict[str, JSONValue] = dict(report.to_dict())
    payload["schema_version"] = QUALITY_REPORT_SCHEMA_VERSION
    if not publish_safe:
        return payload
    return _publish_safe_mapping(payload)


def quality_report_to_json(
    report: QualityReport,
    *,
    publish_safe: bool = True,
) -> str:
    """Return deterministic formatted JSON for a quality report."""
    return json.dumps(
        quality_report_payload(report, publish_safe=publish_safe),
        indent=2,
        sort_keys=True,
    )


def write_quality_report(
    report: QualityReport,
    path: str | Path,
    *,
    publish_safe: bool = True,
) -> ArtifactRef:
    """Write a JSON quality report and return its artifact reference."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        f"{quality_report_to_json(report, publish_safe=publish_safe)}\n"
    ).encode("utf-8")
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
    publish_safe: bool = True,
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
        report_path = (
            publish_safe_path(artifact.path) if publish_safe else artifact.path
        )
        lines.append(f"report: {report_path}")
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
            _format_target_summary(item, publish_safe=publish_safe)
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
    publish_safe: bool = True,
    discovery_target_limit: int = QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT,
    target_summary_limit: int = QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT,
    cross_target_summary_limit: int = QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT,
) -> dict[str, JSONValue]:
    """Return a bounded result payload without detailed findings."""
    target_summaries = report.target_summaries
    cross_target_summaries = _cross_target_summaries(report)
    payload: dict[str, JSONValue] = {
        "operation": operation,
        "check_groups": list(check_groups),
        "discovery": _bounded_discovery_payload(
            discovery,
            target_limit=discovery_target_limit,
        ),
        "summary": report.summary().to_dict(),
        "target_status_counts": _target_status_counts(target_summaries),
        "target_summaries": _bounded_target_summaries(
            target_summaries,
            limit=target_summary_limit,
        ),
        "cross_target_summaries": _bounded_json_list(
            cross_target_summaries,
            limit=cross_target_summary_limit,
        ),
        "quality_profile": _quality_profile_metadata(report),
        "report_schema_version": QUALITY_REPORT_SCHEMA_VERSION,
        "report_artifact": None if artifact is None else artifact.to_dict(),
        "exit_decision": decision.to_dict(),
        "payload_limits": {
            "discovery_targets": _payload_limit_metadata(
                _sequence_count(discovery.get("targets")),
                discovery_target_limit,
            ),
            "target_summaries": _payload_limit_metadata(
                len(target_summaries),
                target_summary_limit,
            ),
            "cross_target_summaries": _payload_limit_metadata(
                len(cross_target_summaries),
                cross_target_summary_limit,
            ),
        },
    }
    if not publish_safe:
        return payload
    return _publish_safe_mapping(payload)


def _bounded_discovery_payload(
    discovery: Mapping[str, JSONValue],
    *,
    target_limit: int,
) -> dict[str, JSONValue]:
    """Return discovery metadata with the target list capped."""
    payload = dict(discovery)
    targets = discovery.get("targets")
    if isinstance(targets, list):
        bounded_targets = _bounded_json_list(targets, limit=target_limit)
        target_count = discovery.get("target_count")
        payload["targets"] = bounded_targets
        payload["target_count"] = (
            int(target_count)
            if isinstance(target_count, (int, float, str)) and target_count
            else len(targets)
        )
        payload["target_included_count"] = len(bounded_targets)
        payload["target_omitted_count"] = max(
            0,
            len(targets) - len(bounded_targets),
        )
    return payload


def _bounded_target_summaries(
    summaries: tuple[QualityTargetSummary, ...],
    *,
    limit: int,
) -> list[JSONValue]:
    """Return capped target summaries with warning/error examples first."""
    ordered = sorted(
        summaries,
        key=lambda summary: (
            _target_summary_status_priority(summary),
            summary.target.path,
        ),
    )
    return _bounded_json_list(
        [summary.to_dict() for summary in ordered],
        limit=limit,
    )


def _target_summary_status_priority(summary: QualityTargetSummary) -> int:
    if summary.status is QualityStatus.FAILED:
        return 0
    if summary.status is QualityStatus.WARNING:
        return 1
    return 2


def _bounded_json_list(
    values: list[JSONValue],
    *,
    limit: int,
) -> list[JSONValue]:
    if limit < 0:
        return list(values)
    return list(values[:limit])


def _payload_limit_metadata(
    total_count: int, limit: int
) -> dict[str, JSONValue]:
    included_count = total_count if limit < 0 else min(total_count, limit)
    return {
        "limit": limit,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": max(0, total_count - included_count),
        "truncated": total_count > included_count,
    }


def _sequence_count(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


def _target_status_counts(
    summaries: tuple[QualityTargetSummary, ...],
) -> dict[str, JSONValue]:
    return {
        QualityStatus.CLEAN.value: sum(
            1 for summary in summaries if summary.status is QualityStatus.CLEAN
        ),
        QualityStatus.WARNING.value: sum(
            1
            for summary in summaries
            if summary.status is QualityStatus.WARNING
        ),
        QualityStatus.FAILED.value: sum(
            1 for summary in summaries if summary.status is QualityStatus.FAILED
        ),
    }


def _cross_target_summaries(
    report: QualityReport,
) -> list[JSONValue]:
    """Return compact symbol summaries for run-level warning/error findings."""
    summaries: list[JSONValue] = []
    for result in report.rule_results:
        if result.target.symbol:
            continue
        for finding in result.findings:
            if finding.severity is QualitySeverity.INFO:
                continue
            for target in _finding_symbol_targets(finding):
                summaries.append(
                    {
                        "target": target,
                        "rule_count": 1,
                        "finding_count": 1,
                        "info_count": 0,
                        "warning_count": int(
                            finding.severity is QualitySeverity.WARNING
                        ),
                        "error_count": int(
                            finding.severity is QualitySeverity.ERROR
                        ),
                        "status": (
                            QualityStatus.FAILED.value
                            if finding.severity is QualitySeverity.ERROR
                            else QualityStatus.WARNING.value
                        ),
                        "max_severity": finding.severity.value,
                    }
                )
    return summaries


def _quality_profile_metadata(report: QualityReport) -> dict[str, JSONValue]:
    """Return compact quality-profile metadata when present."""
    profile = report.metadata.get("quality_profile")
    if isinstance(profile, dict):
        return dict(profile)
    return {}


def _finding_symbol_targets(
    finding: QualityFinding,
) -> tuple[dict[str, JSONValue], ...]:
    fallback = {
        "data_format": finding.target.data_format,
        "timeframe": finding.target.timeframe,
        "period": finding.target.period,
    }
    records: set[tuple[str, str, str, str]] = set()
    location_metadata = finding.location.metadata
    contexts = _finding_contexts(finding)
    for context in contexts:
        symbols = _symbols_from_mapping(context) | _symbols_from_mapping(
            location_metadata
        )
        data_format = (
            _string_field(context, "data_format") or fallback["data_format"]
        )
        timeframe = (
            _string_field(context, "timeframe")
            or _string_field(location_metadata, "timeframe")
            or fallback["timeframe"]
        )
        period = (
            _string_field(context, "period")
            or _string_field(location_metadata, "period")
            or fallback["period"]
        )
        for symbol in symbols:
            records.add((symbol, data_format, timeframe, period))
    return tuple(
        {
            "kind": "cross-target-finding",
            "path": finding.target.path,
            "data_format": data_format,
            "timeframe": timeframe,
            "symbol": symbol,
            "period": period,
            "metadata": {
                "code": finding.code,
                "rule_id": finding.rule_id,
            },
        }
        for symbol, data_format, timeframe, period in sorted(records)
    )


def _finding_contexts(
    finding: QualityFinding,
) -> tuple[Mapping[str, JSONValue], ...]:
    metadata = finding.metadata
    samples = metadata.get("samples")
    contexts: list[Mapping[str, JSONValue]] = [metadata]
    if isinstance(samples, list):
        contexts.extend(
            sample for sample in samples if isinstance(sample, Mapping)
        )
    return tuple(contexts)


def _symbols_from_mapping(value: Mapping[str, JSONValue]) -> set[str]:
    symbols: set[str] = set()
    for key, item in value.items():
        if key == "symbols" and isinstance(item, list):
            symbols.update(_normalized_symbol(symbol) for symbol in item)
        elif key == "symbol" or key.endswith("_symbol"):
            symbols.add(_normalized_symbol(item))
    symbols.discard("")
    return symbols


def _normalized_symbol(value: object) -> str:
    text = str(value or "").strip().upper()
    return text if text.isalnum() else ""


def _string_field(value: Mapping[str, JSONValue], key: str) -> str:
    return str(value.get(key, "") or "")


def _target_count(report: QualityReport, status: QualityStatus) -> int:
    return sum(
        1 for summary in report.target_summaries if summary.status is status
    )


def _format_target_summary(
    summary: QualityTargetSummary,
    *,
    publish_safe: bool = True,
) -> str:
    target = summary.target
    target_path = (
        publish_safe_path(target.path) if publish_safe else target.path
    )
    return (
        f"- {target.kind.value}: {target_path} "
        f"(findings={summary.finding_count}, "
        f"warnings={summary.warning_count}, errors={summary.error_count})"
    )


def _publish_safe_mapping(
    payload: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload
