"""Report serialization and exit policy helpers for data-quality runs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
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
from histdatacom.data_quality.fingerprints import (
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    series_fingerprint_coverage_summary,
    series_fingerprint_topology_attention_summary,
    series_fingerprint_topology_summary,
)
from histdatacom.data_quality.remediation import (
    remediation_hint_payloads_for_finding,
)
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

QUALITY_REPORT_SCHEMA_VERSION = "histdatacom.quality-report.v1"
QUALITY_NEXT_ACTIONS_SCHEMA_VERSION = "histdatacom.quality-next-actions.v1"
QUALITY_NEXT_ACTIONS_METADATA_KEY = "quality_next_actions"
QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.quality-remediation-coverage.v1"
)
QUALITY_REMEDIATION_COVERAGE_METADATA_KEY = "remediation_coverage"
QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT = 128
QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT = 128
QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT = 128
QUALITY_PAYLOAD_NEXT_ACTION_LIMIT = 16
QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT = 8
QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT = 16
QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT = 8

_NEXT_ACTION_SEVERITY_RANK = {"info": 1, "warning": 2, "error": 3}
_REMEDIATION_COVERAGE_SEVERITY_RANK = {"info": 1, "warning": 2, "error": 3}
_NEXT_ACTION_ATTENTION_RANK = {
    "session": 1,
    "sequence": 2,
    "unavailable": 3,
    "structural": 3,
}
_NEXT_ACTION_KIND_RANK = {
    "verify": 1,
    "configure": 2,
    "inspect": 2,
    "rebuild": 3,
    "repair": 3,
}
_NEXT_ACTION_URGENCY_SORT = {"high": 0, "medium": 1, "low": 2}
_TARGET_AXIS_FIELDS = ("data_format", "timeframe", "symbol", "period", "kind")


@dataclass(slots=True)
class _NextActionAggregate:
    code: str
    message: str
    action_kind: str
    rule_id: str
    occurrence_count: int = 0
    severity_counts: Counter[str] = field(default_factory=Counter)
    attention_level_counts: Counter[str] = field(default_factory=Counter)
    source_counts: Counter[str] = field(default_factory=Counter)
    finding_code_counts: Counter[str] = field(default_factory=Counter)
    flag_counts: Counter[str] = field(default_factory=Counter)
    target_axis_counts: Counter[tuple[str, str, str, str, str]] = field(
        default_factory=Counter
    )


@dataclass(slots=True)
class _RemediationCoverageAggregate:
    rule_id: str
    finding_code: str
    mapped: bool
    occurrence_count: int = 0
    severity_counts: Counter[str] = field(default_factory=Counter)
    target_axis_counts: Counter[tuple[str, str, str, str, str]] = field(
        default_factory=Counter
    )


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
    fingerprint_coverage = _fingerprint_coverage_summary(report)
    if fingerprint_coverage is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY] = (
            fingerprint_coverage
        )
        payload["metadata"] = metadata
    fingerprint_topology = _fingerprint_topology_summary(report)
    if fingerprint_topology is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY] = (
            fingerprint_topology
        )
        payload["metadata"] = metadata
    fingerprint_topology_attention = _fingerprint_topology_attention_summary(
        report
    )
    if fingerprint_topology_attention is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY] = (
            fingerprint_topology_attention
        )
        payload["metadata"] = metadata
    next_actions = quality_next_actions_summary(report)
    if next_actions is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[QUALITY_NEXT_ACTIONS_METADATA_KEY] = next_actions
        payload["metadata"] = metadata
    remediation_coverage = quality_remediation_coverage_summary(report)
    if remediation_coverage is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[QUALITY_REMEDIATION_COVERAGE_METADATA_KEY] = (
            remediation_coverage
        )
        payload["metadata"] = metadata
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
    lines.extend(
        format_quality_next_action_lines(quality_next_actions_summary(report))
    )
    lines.extend(
        format_quality_remediation_coverage_lines(
            quality_remediation_coverage_summary(report)
        )
    )
    if _fingerprint_group_selected(check_groups):
        lines.extend(
            _format_fingerprint_coverage_lines(
                _fingerprint_coverage_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_topology_attention_lines(
                _fingerprint_topology_attention_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_topology_summary_lines(
                _fingerprint_topology_summary(report)
            )
        )

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
    fingerprint_coverage = _fingerprint_coverage_summary(report)
    fingerprint_topology = _fingerprint_topology_summary(report)
    fingerprint_topology_attention = _fingerprint_topology_attention_summary(
        report
    )
    next_actions = quality_next_actions_summary(report)
    remediation_coverage = quality_remediation_coverage_summary(report)
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
            "next_actions": _next_action_payload_limit_metadata(next_actions),
            "remediation_coverage": (
                _remediation_coverage_payload_limit_metadata(
                    remediation_coverage
                )
            ),
        },
    }
    if fingerprint_coverage is not None:
        payload["fingerprint_coverage"] = fingerprint_coverage
    if fingerprint_topology is not None:
        payload["fingerprint_topology"] = fingerprint_topology
    if fingerprint_topology_attention is not None:
        payload["fingerprint_topology_attention"] = (
            fingerprint_topology_attention
        )
    if next_actions is not None:
        payload["next_actions"] = next_actions
    if remediation_coverage is not None:
        payload["remediation_coverage"] = remediation_coverage
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


def _next_action_payload_limit_metadata(
    summary: Mapping[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    if summary is None:
        return _payload_limit_metadata(0, QUALITY_PAYLOAD_NEXT_ACTION_LIMIT)
    total_count = _int_metadata(summary, "action_count")
    included_count = _int_metadata(summary, "included_action_count")
    omitted_count = _int_metadata(summary, "omitted_action_count")
    truncated = summary.get("truncated")
    return {
        "limit": QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "truncated": (
            truncated if isinstance(truncated, bool) else omitted_count > 0
        ),
    }


def _remediation_coverage_payload_limit_metadata(
    summary: Mapping[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    if summary is None:
        return _payload_limit_metadata(
            0,
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
        )
    total_count = _int_metadata(summary, "unmapped_group_count")
    included_count = _int_metadata(summary, "included_unmapped_group_count")
    omitted_count = _int_metadata(summary, "omitted_unmapped_group_count")
    truncated = summary.get("unmapped_truncated")
    return {
        "limit": QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
        "target_axis_limit": (
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
        ),
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "truncated": (
            truncated if isinstance(truncated, bool) else omitted_count > 0
        ),
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


def quality_remediation_coverage_summary(
    report: QualityReport,
    *,
    group_limit: int = QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
    target_axis_limit: int = QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded remediation-catalog coverage for quality findings."""
    summary = report.metadata.get(QUALITY_REMEDIATION_COVERAGE_METADATA_KEY)
    if isinstance(summary, Mapping):
        return dict(summary)

    aggregates = _remediation_coverage_aggregates(report)
    if not aggregates:
        return None

    severity_counts: Counter[str] = Counter()
    mapped_severity_counts: Counter[str] = Counter()
    unmapped_severity_counts: Counter[str] = Counter()
    rule_id_counts: Counter[str] = Counter()
    mapped_rule_id_counts: Counter[str] = Counter()
    unmapped_rule_id_counts: Counter[str] = Counter()
    finding_code_counts: Counter[str] = Counter()
    mapped_finding_code_counts: Counter[str] = Counter()
    unmapped_finding_code_counts: Counter[str] = Counter()
    mapped_finding_count = 0
    unmapped_finding_count = 0

    for aggregate in aggregates.values():
        severity_counts.update(aggregate.severity_counts)
        rule_id_counts[aggregate.rule_id] += aggregate.occurrence_count
        finding_code_counts[
            aggregate.finding_code
        ] += aggregate.occurrence_count
        if aggregate.mapped:
            mapped_finding_count += aggregate.occurrence_count
            mapped_severity_counts.update(aggregate.severity_counts)
            mapped_rule_id_counts[
                aggregate.rule_id
            ] += aggregate.occurrence_count
            mapped_finding_code_counts[
                aggregate.finding_code
            ] += aggregate.occurrence_count
        else:
            unmapped_finding_count += aggregate.occurrence_count
            unmapped_severity_counts.update(aggregate.severity_counts)
            unmapped_rule_id_counts[
                aggregate.rule_id
            ] += aggregate.occurrence_count
            unmapped_finding_code_counts[
                aggregate.finding_code
            ] += aggregate.occurrence_count

    unmapped_groups = sorted(
        (
            aggregate
            for aggregate in aggregates.values()
            if not aggregate.mapped
        ),
        key=_remediation_coverage_group_sort_key,
    )
    included_unmapped_groups = (
        unmapped_groups if group_limit < 0 else unmapped_groups[:group_limit]
    )
    unmapped_warning_error_group_count = sum(
        1
        for aggregate in unmapped_groups
        if _remediation_coverage_group_is_warning_or_error(aggregate)
    )
    included_unmapped_warning_error_group_count = sum(
        1
        for aggregate in included_unmapped_groups
        if _remediation_coverage_group_is_warning_or_error(aggregate)
    )
    count_limits = _remediation_coverage_count_limits(
        rule_id_counts=rule_id_counts,
        mapped_rule_id_counts=mapped_rule_id_counts,
        unmapped_rule_id_counts=unmapped_rule_id_counts,
        finding_code_counts=finding_code_counts,
        mapped_finding_code_counts=mapped_finding_code_counts,
        unmapped_finding_code_counts=unmapped_finding_code_counts,
        limit=group_limit,
    )
    omitted_unmapped_group_count = max(
        0,
        len(unmapped_groups) - len(included_unmapped_groups),
    )
    return {
        "schema_version": QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION,
        "finding_count": mapped_finding_count + unmapped_finding_count,
        "mapped_finding_count": mapped_finding_count,
        "unmapped_finding_count": unmapped_finding_count,
        "unmapped_warning_error_finding_count": sum(
            count
            for severity, count in unmapped_severity_counts.items()
            if severity in {"error", "warning"}
        ),
        "severity_counts": _counter_payload(severity_counts),
        "mapped_severity_counts": _counter_payload(mapped_severity_counts),
        "unmapped_severity_counts": _counter_payload(unmapped_severity_counts),
        "rule_id_counts": _named_counter_payloads(
            rule_id_counts,
            key_name="rule_id",
            limit=group_limit,
        ),
        "mapped_rule_id_counts": _named_counter_payloads(
            mapped_rule_id_counts,
            key_name="rule_id",
            limit=group_limit,
        ),
        "unmapped_rule_id_counts": _named_counter_payloads(
            unmapped_rule_id_counts,
            key_name="rule_id",
            limit=group_limit,
        ),
        "finding_code_counts": _named_counter_payloads(
            finding_code_counts,
            key_name="finding_code",
            limit=group_limit,
        ),
        "mapped_finding_code_counts": _named_counter_payloads(
            mapped_finding_code_counts,
            key_name="finding_code",
            limit=group_limit,
        ),
        "unmapped_finding_code_counts": _named_counter_payloads(
            unmapped_finding_code_counts,
            key_name="finding_code",
            limit=group_limit,
        ),
        "count_limits": count_limits,
        "unmapped_group_count": len(unmapped_groups),
        "included_unmapped_group_count": len(included_unmapped_groups),
        "omitted_unmapped_group_count": omitted_unmapped_group_count,
        "unmapped_truncated": omitted_unmapped_group_count > 0,
        "unmapped_warning_error_group_count": (
            unmapped_warning_error_group_count
        ),
        "included_unmapped_warning_error_group_count": (
            included_unmapped_warning_error_group_count
        ),
        "omitted_unmapped_warning_error_group_count": max(
            0,
            unmapped_warning_error_group_count
            - included_unmapped_warning_error_group_count,
        ),
        "unmapped_groups": [
            _remediation_coverage_group_payload(
                aggregate,
                target_axis_limit=target_axis_limit,
            )
            for aggregate in included_unmapped_groups
        ],
    }


def format_quality_remediation_coverage_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for remediation coverage gaps."""
    if not summary:
        return []
    warning_error_group_count = _int_metadata(
        summary,
        "unmapped_warning_error_group_count",
    )
    if not warning_error_group_count:
        return []
    groups = [
        item
        for item in _list_metadata(summary.get("unmapped_groups"))
        if _optional_string_metadata(item, "max_severity")
        in {"error", "warning"}
    ]
    lines = [
        "",
        "Remediation coverage",
        (
            "- findings: "
            f"{_int_metadata(summary, 'finding_count')} "
            f"mapped: {_int_metadata(summary, 'mapped_finding_count')} "
            f"unmapped: {_int_metadata(summary, 'unmapped_finding_count')}"
        ),
        (
            "- unmapped warning/error groups: "
            f"{warning_error_group_count} "
            "included: "
            f"{_int_metadata(summary, 'included_unmapped_warning_error_group_count')} "
            "omitted: "
            f"{_int_metadata(summary, 'omitted_unmapped_warning_error_group_count')}"
        ),
    ]
    for group in groups:
        lines.append(f"- {_format_quality_remediation_coverage_group(group)}")
    return lines


def quality_next_actions_summary(
    report: QualityReport,
    *,
    action_limit: int = QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
    target_axis_limit: int = QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded run-level next actions from remediation hints."""
    summary = report.metadata.get(QUALITY_NEXT_ACTIONS_METADATA_KEY)
    if isinstance(summary, Mapping):
        return dict(summary)

    aggregates = _next_action_aggregates(report)
    if not aggregates:
        return None

    actions = sorted(
        (
            _next_action_payload(
                aggregate,
                target_axis_limit=target_axis_limit,
            )
            for aggregate in aggregates.values()
        ),
        key=_next_action_sort_key,
    )
    included = actions if action_limit < 0 else actions[:action_limit]
    included_actions: list[JSONValue] = [action for action in included]
    omitted_count = max(0, len(actions) - len(included))
    source_counts: Counter[str] = Counter()
    for aggregate in aggregates.values():
        source_counts.update(aggregate.source_counts)
    return {
        "schema_version": QUALITY_NEXT_ACTIONS_SCHEMA_VERSION,
        "action_count": len(actions),
        "included_action_count": len(included),
        "omitted_action_count": omitted_count,
        "truncated": omitted_count > 0,
        "source_counts": _counter_payload(source_counts),
        "actions": included_actions,
    }


def format_quality_next_action_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for quality next actions."""
    if not summary:
        return []
    actions = summary.get("actions")
    if not isinstance(actions, list) or not actions:
        return []
    lines = [
        "",
        "Next actions",
        (
            "- actions: "
            f"{_int_metadata(summary, 'action_count')} "
            f"included: {_int_metadata(summary, 'included_action_count')} "
            f"omitted: {_int_metadata(summary, 'omitted_action_count')}"
        ),
    ]
    for item in actions:
        if isinstance(item, Mapping):
            lines.append(f"- {_format_quality_next_action_line(item)}")
    return lines


def _next_action_aggregates(
    report: QualityReport,
) -> dict[tuple[str, str, str], _NextActionAggregate]:
    aggregates: dict[tuple[str, str, str], _NextActionAggregate] = {}
    _collect_fingerprint_topology_next_actions(report, aggregates)
    _collect_finding_next_actions(report, aggregates)
    return aggregates


def _collect_fingerprint_topology_next_actions(
    report: QualityReport,
    aggregates: dict[tuple[str, str, str], _NextActionAggregate],
) -> None:
    summary = _fingerprint_topology_attention_summary(report)
    if not summary:
        return
    target_summaries = summary.get("target_summaries")
    if not isinstance(target_summaries, list):
        return
    for target_summary in target_summaries:
        if not isinstance(target_summary, Mapping):
            continue
        target_axis = _mapping_payload(target_summary.get("target_axis"))
        attention_level = _optional_string_metadata(
            target_summary,
            "attention_level",
        )
        hints = target_summary.get("remediation_hints")
        if not isinstance(hints, list):
            continue
        for hint in hints:
            if isinstance(hint, Mapping):
                _add_next_action_hint(
                    aggregates,
                    hint,
                    source="fingerprint_topology_attention",
                    target_axis=target_axis,
                    attention_level=attention_level,
                )


def _collect_finding_next_actions(
    report: QualityReport,
    aggregates: dict[tuple[str, str, str], _NextActionAggregate],
) -> None:
    for finding in report.findings:
        for hint in remediation_hint_payloads_for_finding(finding):
            if not isinstance(hint, Mapping):
                continue
            _add_next_action_hint(
                aggregates,
                hint,
                source="quality_finding",
                target_axis=_target_axis_from_finding(finding),
                severity=finding.severity.value,
                fallback_rule_id=finding.rule_id,
                fallback_finding_code=finding.code,
            )


def _add_next_action_hint(
    aggregates: dict[tuple[str, str, str], _NextActionAggregate],
    hint: Mapping[str, JSONValue],
    *,
    source: str,
    target_axis: Mapping[str, JSONValue],
    severity: str = "",
    attention_level: str = "",
    fallback_rule_id: str = "",
    fallback_finding_code: str = "",
) -> None:
    code = _optional_string_metadata(hint, "code")
    message = _optional_string_metadata(hint, "message")
    if not code or not message:
        return
    action_kind = _optional_string_metadata(hint, "action_kind") or "inspect"
    rule_id = _optional_string_metadata(hint, "rule_id") or fallback_rule_id
    rule_id = rule_id or "unknown"
    aggregate = aggregates.setdefault(
        (code, action_kind, rule_id),
        _NextActionAggregate(
            code=code,
            message=message,
            action_kind=action_kind,
            rule_id=rule_id,
        ),
    )
    aggregate.occurrence_count += 1
    aggregate.source_counts[source] += 1
    aggregate.target_axis_counts[_target_axis_key(target_axis)] += 1
    if severity:
        aggregate.severity_counts[severity] += 1
    if attention_level:
        aggregate.attention_level_counts[attention_level] += 1
    finding_code = (
        _optional_string_metadata(hint, "finding_code") or fallback_finding_code
    )
    if finding_code:
        aggregate.finding_code_counts[finding_code] += 1
    flag = _optional_string_metadata(hint, "flag")
    if flag:
        aggregate.flag_counts[flag] += 1


def _next_action_payload(
    aggregate: _NextActionAggregate,
    *,
    target_axis_limit: int,
) -> dict[str, JSONValue]:
    target_axis_counts = _target_axis_count_payloads(
        aggregate.target_axis_counts,
        limit=target_axis_limit,
    )
    target_axis_count = len(aggregate.target_axis_counts)
    included_target_axis_count = len(target_axis_counts)
    omitted_target_axis_count = max(
        0,
        target_axis_count - included_target_axis_count,
    )
    max_severity = _ranked_counter_max(
        aggregate.severity_counts,
        _NEXT_ACTION_SEVERITY_RANK,
    )
    max_attention_level = _ranked_counter_max(
        aggregate.attention_level_counts,
        _NEXT_ACTION_ATTENTION_RANK,
    )
    return {
        "code": aggregate.code,
        "message": aggregate.message,
        "action_kind": aggregate.action_kind,
        "rule_id": aggregate.rule_id,
        "urgency": _next_action_urgency(
            aggregate.action_kind,
            max_severity=max_severity,
            max_attention_level=max_attention_level,
        ),
        "max_severity": max_severity,
        "max_attention_level": max_attention_level,
        "occurrence_count": aggregate.occurrence_count,
        "affected_target_count": target_axis_count,
        "target_axis_count": target_axis_count,
        "included_target_axis_count": included_target_axis_count,
        "omitted_target_axis_count": omitted_target_axis_count,
        "target_axis_truncated": omitted_target_axis_count > 0,
        "severity_counts": _counter_payload(aggregate.severity_counts),
        "attention_level_counts": _counter_payload(
            aggregate.attention_level_counts
        ),
        "source_counts": _counter_payload(aggregate.source_counts),
        "finding_code_counts": _counter_payload(aggregate.finding_code_counts),
        "flag_counts": _counter_payload(aggregate.flag_counts),
        "target_axis_counts": target_axis_counts,
    }


def _next_action_sort_key(
    action: Mapping[str, JSONValue],
) -> tuple[int, int, int, int, str, str, str]:
    urgency = _optional_string_metadata(action, "urgency")
    max_severity = _optional_string_metadata(action, "max_severity")
    max_attention_level = _optional_string_metadata(
        action,
        "max_attention_level",
    )
    return (
        _NEXT_ACTION_URGENCY_SORT.get(urgency, 99),
        -_NEXT_ACTION_SEVERITY_RANK.get(max_severity, 0),
        -_NEXT_ACTION_ATTENTION_RANK.get(max_attention_level, 0),
        -_int_metadata(action, "affected_target_count"),
        _optional_string_metadata(action, "code"),
        _optional_string_metadata(action, "action_kind"),
        _optional_string_metadata(action, "rule_id"),
    )


def _next_action_urgency(
    action_kind: str,
    *,
    max_severity: str | None,
    max_attention_level: str | None,
) -> str:
    rank = max(
        _NEXT_ACTION_KIND_RANK.get(action_kind, 0),
        _NEXT_ACTION_SEVERITY_RANK.get(max_severity or "", 0),
        _NEXT_ACTION_ATTENTION_RANK.get(max_attention_level or "", 0),
    )
    if rank >= 3:
        return "high"
    if rank >= 2:
        return "medium"
    return "low"


def _ranked_counter_max(
    counter: Counter[str],
    ranks: Mapping[str, int],
) -> str | None:
    if not counter:
        return None
    return sorted(
        counter,
        key=lambda key: (-ranks.get(key, 0), key),
    )[0]


def _target_axis_count_payloads(
    counter: Counter[tuple[str, str, str, str, str]],
    *,
    limit: int,
) -> list[JSONValue]:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-item[1], item[0]),
    )
    included = ordered if limit < 0 else ordered[:limit]
    return [
        {
            "target_axis": _target_axis_payload_from_key(axis),
            "count": count,
        }
        for axis, count in included
    ]


def _target_axis_from_finding(finding: QualityFinding) -> dict[str, JSONValue]:
    target = finding.target
    return {
        "data_format": target.data_format,
        "timeframe": target.timeframe,
        "symbol": target.symbol,
        "period": target.period,
        "kind": target.kind.value,
    }


def _target_axis_key(
    target_axis: Mapping[str, JSONValue],
) -> tuple[str, str, str, str, str]:
    return (
        _target_axis_value(target_axis.get("data_format")),
        _target_axis_value(target_axis.get("timeframe")),
        _target_axis_value(target_axis.get("symbol")),
        _target_axis_value(target_axis.get("period")),
        _target_axis_value(target_axis.get("kind")),
    )


def _target_axis_value(value: JSONValue) -> str:
    if isinstance(value, bool) or value is None:
        return "unknown"
    text = str(value).strip()
    return text or "unknown"


def _target_axis_payload_from_key(
    key: tuple[str, str, str, str, str],
) -> dict[str, JSONValue]:
    return dict(zip(_TARGET_AXIS_FIELDS, key, strict=True))


def _counter_payload(counter: Counter[str]) -> dict[str, JSONValue]:
    return {key: count for key, count in sorted(counter.items()) if count > 0}


def _format_quality_next_action_line(
    action: Mapping[str, JSONValue],
) -> str:
    severity = _optional_string_metadata(action, "max_severity")
    attention = _optional_string_metadata(action, "max_attention_level")
    qualifiers = []
    if severity:
        qualifiers.append(f"severity={severity}")
    if attention:
        qualifiers.append(f"attention={attention}")
    qualifier_text = f", {', '.join(qualifiers)}" if qualifiers else ""
    return (
        f"{_optional_string_metadata(action, 'urgency')} "
        f"{_optional_string_metadata(action, 'action_kind')}: "
        f"{_optional_string_metadata(action, 'message')} "
        f"({_optional_string_metadata(action, 'code')}, "
        f"rule={_optional_string_metadata(action, 'rule_id')}, "
        f"targets={_int_metadata(action, 'affected_target_count')}"
        f"{qualifier_text})"
    )


def _remediation_coverage_aggregates(
    report: QualityReport,
) -> dict[tuple[bool, str, str], _RemediationCoverageAggregate]:
    aggregates: dict[tuple[bool, str, str], _RemediationCoverageAggregate] = {}
    for finding in report.findings:
        mapped = bool(remediation_hint_payloads_for_finding(finding))
        rule_id = finding.rule_id or "unknown"
        finding_code = finding.code or "unknown"
        aggregate = aggregates.setdefault(
            (mapped, rule_id, finding_code),
            _RemediationCoverageAggregate(
                rule_id=rule_id,
                finding_code=finding_code,
                mapped=mapped,
            ),
        )
        aggregate.occurrence_count += 1
        aggregate.severity_counts[finding.severity.value] += 1
        aggregate.target_axis_counts[
            _target_axis_key(_target_axis_from_finding(finding))
        ] += 1
    return aggregates


def _remediation_coverage_group_payload(
    aggregate: _RemediationCoverageAggregate,
    *,
    target_axis_limit: int,
) -> dict[str, JSONValue]:
    target_axis_counts = _target_axis_count_payloads(
        aggregate.target_axis_counts,
        limit=target_axis_limit,
    )
    target_axis_count = len(aggregate.target_axis_counts)
    included_target_axis_count = len(target_axis_counts)
    omitted_target_axis_count = max(
        0,
        target_axis_count - included_target_axis_count,
    )
    return {
        "rule_id": aggregate.rule_id,
        "finding_code": aggregate.finding_code,
        "mapped": aggregate.mapped,
        "max_severity": _ranked_counter_max(
            aggregate.severity_counts,
            _REMEDIATION_COVERAGE_SEVERITY_RANK,
        ),
        "occurrence_count": aggregate.occurrence_count,
        "target_axis_count": target_axis_count,
        "included_target_axis_count": included_target_axis_count,
        "omitted_target_axis_count": omitted_target_axis_count,
        "target_axis_truncated": omitted_target_axis_count > 0,
        "severity_counts": _counter_payload(aggregate.severity_counts),
        "target_axis_counts": target_axis_counts,
    }


def _remediation_coverage_group_sort_key(
    aggregate: _RemediationCoverageAggregate,
) -> tuple[int, int, int, str, str]:
    max_severity = _ranked_counter_max(
        aggregate.severity_counts,
        _REMEDIATION_COVERAGE_SEVERITY_RANK,
    )
    return (
        -_REMEDIATION_COVERAGE_SEVERITY_RANK.get(max_severity or "", 0),
        -aggregate.occurrence_count,
        -len(aggregate.target_axis_counts),
        aggregate.rule_id,
        aggregate.finding_code,
    )


def _remediation_coverage_group_is_warning_or_error(
    aggregate: _RemediationCoverageAggregate,
) -> bool:
    max_severity = _ranked_counter_max(
        aggregate.severity_counts,
        _REMEDIATION_COVERAGE_SEVERITY_RANK,
    )
    return max_severity in {"error", "warning"}


def _remediation_coverage_count_limits(
    *,
    rule_id_counts: Counter[str],
    mapped_rule_id_counts: Counter[str],
    unmapped_rule_id_counts: Counter[str],
    finding_code_counts: Counter[str],
    mapped_finding_code_counts: Counter[str],
    unmapped_finding_code_counts: Counter[str],
    limit: int,
) -> dict[str, JSONValue]:
    return {
        "rule_id_counts": _payload_limit_metadata(len(rule_id_counts), limit),
        "mapped_rule_id_counts": _payload_limit_metadata(
            len(mapped_rule_id_counts),
            limit,
        ),
        "unmapped_rule_id_counts": _payload_limit_metadata(
            len(unmapped_rule_id_counts),
            limit,
        ),
        "finding_code_counts": _payload_limit_metadata(
            len(finding_code_counts),
            limit,
        ),
        "mapped_finding_code_counts": _payload_limit_metadata(
            len(mapped_finding_code_counts),
            limit,
        ),
        "unmapped_finding_code_counts": _payload_limit_metadata(
            len(unmapped_finding_code_counts),
            limit,
        ),
    }


def _named_counter_payloads(
    counter: Counter[str],
    *,
    key_name: str,
    limit: int,
) -> list[JSONValue]:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-item[1], item[0]),
    )
    included = ordered if limit < 0 else ordered[:limit]
    return [
        {
            key_name: key,
            "count": count,
        }
        for key, count in included
        if count > 0
    ]


def _format_quality_remediation_coverage_group(
    group: Mapping[str, JSONValue],
) -> str:
    return (
        f"{_optional_string_metadata(group, 'max_severity')} "
        f"{_optional_string_metadata(group, 'rule_id')}:"
        f"{_optional_string_metadata(group, 'finding_code')} "
        f"findings={_int_metadata(group, 'occurrence_count')} "
        f"targets={_int_metadata(group, 'target_axis_count')}"
    )


def _fingerprint_coverage_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint coverage metadata from the report or findings."""
    summary = report.metadata.get(TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY)
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_coverage_summary(report.findings)
    )


def _fingerprint_topology_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint topology metadata from the report or findings."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_topology_summary(report.findings)
    )


def _fingerprint_topology_attention_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint topology attention metadata."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_topology_attention_summary(report.findings)
    )


def _fingerprint_group_selected(check_groups: tuple[str, ...]) -> bool:
    normalized = {group.strip().lower() for group in check_groups if group}
    if not normalized:
        normalized = {"all"}
    return "all" in normalized or "fingerprint" in normalized


def _format_fingerprint_coverage_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    if summary is None:
        return []
    lines = [
        "",
        "Fingerprint coverage",
        (
            "- targets: "
            f"{_int_metadata(summary, 'fingerprint_target_count')} "
            "supported/readable: "
            f"{_int_metadata(summary, 'supported_readable_count')} "
            f"unavailable: {_int_metadata(summary, 'unavailable_count')} "
            "parsed/non-empty: "
            f"{_int_metadata(summary, 'parsed_non_empty_coverage_count')}"
        ),
    ]
    skipped_count = _int_metadata(summary, "skipped_fingerprint_target_count")
    if skipped_count:
        lines.append(f"- skipped: {skipped_count}")
    count_lines = (
        ("source kinds", "source_kind_counts"),
        ("cache sources", "cache_source_counts"),
        ("unavailable reasons", "unavailable_reason_counts"),
        ("skipped reasons", "skipped_reason_counts"),
        ("target kinds", "target_kind_counts"),
        ("timeframes", "timeframe_counts"),
    )
    for label, key in count_lines:
        counts = _format_count_metadata(summary.get(key))
        if counts:
            lines.append(f"- {label}: {counts}")
    return lines


def format_fingerprint_topology_attention_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for topology attention."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint topology attention",
        (
            "- targets needing attention: "
            f"{_int_metadata(summary, 'attention_target_count')} "
            "included: "
            f"{_int_metadata(summary, 'included_attention_target_count')} "
            "omitted: "
            f"{_int_metadata(summary, 'omitted_attention_target_count')}"
        ),
    ]
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_topology_attention_target_line(item)}"
                )
    return lines


def _format_fingerprint_topology_attention_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    data_format = _string_metadata(axis, "data_format")
    symbol = _string_metadata(axis, "symbol")
    timeframe = _string_metadata(axis, "timeframe")
    period = _string_metadata(axis, "period")
    kind = _string_metadata(axis, "kind")
    flags = _string_list_metadata(summary.get("attention_flags"))
    flag_text = ", ".join(flags) if flags else "no actionable flags"
    cache_source = _optional_string_metadata(summary, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    hints = _remediation_hint_messages(summary.get("remediation_hints"))
    hint_text = f", next={'; '.join(hints)}" if hints else ""
    return (
        f"{data_format} {symbol} {timeframe} {period} {kind}: "
        f"{_string_metadata(summary, 'attention_level')}, "
        f"{flag_text}, "
        f"invalid={_int_metadata(summary, 'invalid_timestamp_count')}, "
        f"duplicates={_int_metadata(summary, 'duplicate_timestamp_count')}, "
        f"non-monotonic={_int_metadata(summary, 'non_monotonic_count')}, "
        f"suspicious gaps={_int_metadata(summary, 'suspicious_gap_count')}, "
        f"weekend activity={_int_metadata(summary, 'weekend_activity_count')}, "
        "max gap "
        f"{_format_duration_ms(summary.get('max_gap_ms'))}, "
        f"computed_from={_string_metadata(summary, 'computed_from')}"
        f"{cache_text}"
        f"{hint_text}"
    )


def format_fingerprint_topology_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for fingerprint topology."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint topology",
        (
            "- targets: "
            f"{_int_metadata(summary, 'target_count')} "
            f"included: {_int_metadata(summary, 'included_target_count')} "
            "regular: "
            f"{_count_metadata(summary.get('status_counts'), 'regular')} "
            "irregular: "
            f"{_count_metadata(summary.get('status_counts'), 'irregular')} "
            "unavailable: "
            f"{_count_metadata(summary.get('status_counts'), 'unavailable')}"
        ),
    ]
    omitted_count = _int_metadata(summary, "omitted_target_count")
    if omitted_count:
        lines.append(f"- omitted: {omitted_count}")
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_topology_target_line(item)}"
                )
    return lines


def _format_fingerprint_topology_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    data_format = _string_metadata(axis, "data_format")
    symbol = _string_metadata(axis, "symbol")
    timeframe = _string_metadata(axis, "timeframe")
    period = _string_metadata(axis, "period")
    kind = _string_metadata(axis, "kind")
    duplicate_count = _int_metadata(summary, "duplicate_timestamp_count")
    duplicate_text = (
        "no duplicates"
        if duplicate_count == 0
        else f"duplicates={duplicate_count}"
    )
    cache_source = _optional_string_metadata(summary, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    return (
        f"{data_format} {symbol} {timeframe} {period} {kind}: "
        f"{_string_metadata(summary, 'status')}, "
        f"{_string_metadata(summary, 'sampling_basis')}, "
        f"{_int_metadata(summary, 'row_count')} rows, "
        f"{_optional_int_text(summary.get('parsed_row_count'))} parsed, "
        f"{duplicate_text}, "
        f"non-monotonic={_int_metadata(summary, 'non_monotonic_count')}, "
        "median interval "
        f"{_format_duration_ms(summary.get('median_interval_ms'))}, "
        f"max gap {_format_duration_ms(summary.get('max_gap_ms'))}, "
        f"{_int_metadata(summary, 'expected_session_closure_count')} "
        "expected closures, "
        f"{_int_metadata(summary, 'suspicious_gap_count')} suspicious gaps, "
        f"weekend activity={_int_metadata(summary, 'weekend_activity_count')}, "
        f"computed_from={_string_metadata(summary, 'computed_from')}"
        f"{cache_text}"
    )


def _count_metadata(value: JSONValue, key: str) -> int:
    if not isinstance(value, Mapping):
        return 0
    item = value.get(key)
    if isinstance(item, bool) or item is None:
        return 0
    if isinstance(item, (int, float)):
        return int(item)
    try:
        return int(str(item))
    except ValueError:
        return 0


def _int_metadata(value: Mapping[str, JSONValue], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or item is None:
        return 0
    if isinstance(item, (int, float)):
        return int(item)
    try:
        return int(str(item))
    except ValueError:
        return 0


def _string_metadata(value: Mapping[str, JSONValue], key: str) -> str:
    text = str(value.get(key, "") or "").strip()
    return text or "unknown"


def _optional_string_metadata(
    value: Mapping[str, JSONValue],
    key: str,
) -> str:
    item = value.get(key)
    if item is None or isinstance(item, bool):
        return ""
    return str(item).strip()


def _optional_int_text(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return "unknown"
    if isinstance(value, (int, float)):
        return str(int(value))
    text = str(value).strip()
    return text or "unknown"


def _string_list_metadata(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _list_metadata(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _remediation_hint_messages(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    messages: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        message = _optional_string_metadata(item, "message")
        if message:
            messages.append(message)
    return messages


def _format_duration_ms(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return "unavailable"
    if isinstance(value, (int, float)):
        milliseconds = int(value)
    else:
        try:
            milliseconds = int(str(value))
        except ValueError:
            return "unavailable"
    absolute = abs(milliseconds)
    if absolute >= 86_400_000 and milliseconds % 86_400_000 == 0:
        return f"{milliseconds // 86_400_000}d"
    if absolute >= 3_600_000 and milliseconds % 3_600_000 == 0:
        return f"{milliseconds // 3_600_000}h"
    if absolute >= 1_000 and milliseconds % 1_000 == 0:
        return f"{milliseconds // 1_000}s"
    return f"{milliseconds}ms"


def _format_count_metadata(value: JSONValue) -> str:
    if not isinstance(value, Mapping):
        return ""
    parts = []
    for key in sorted(value):
        count = value[key]
        if isinstance(count, bool) or not isinstance(count, (int, float)):
            continue
        parts.append(f"{key}={int(count)}")
    return ", ".join(parts)


def _mapping_payload(value: JSONValue) -> dict[str, JSONValue]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _optional_mapping_payload(value: JSONValue) -> dict[str, JSONValue] | None:
    if isinstance(value, Mapping):
        return dict(value)
    return None


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
