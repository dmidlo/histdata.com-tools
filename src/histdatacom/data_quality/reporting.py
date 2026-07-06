"""Report serialization and exit policy helpers for data-quality runs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRunSummary,
    QualitySeverity,
    QualityStatus,
    QualityTargetSummary,
)
from histdatacom.data_quality.fingerprint_contracts import (
    FINGERPRINT_COVERAGE_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_DISTRIBUTION_ATTENTION_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_DISTRIBUTION_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_READINESS_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_READINESS_RISK_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_REGIME_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_TOPOLOGY_ATTENTION_BOUNDED_PAYLOAD_KEY,
    FINGERPRINT_TOPOLOGY_BOUNDED_PAYLOAD_KEY,
)
from histdatacom.data_quality.fingerprints import (
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    series_fingerprint_coverage_summary,
    series_fingerprint_distribution_attention_summary,
    series_fingerprint_distribution_summary,
    series_fingerprint_readiness_summary,
    series_fingerprint_readiness_risk_summary,
    series_fingerprint_regime_summary,
    series_fingerprint_topology_attention_summary,
    series_fingerprint_topology_summary,
)
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_report_surface_evidence,
)
from histdatacom.data_quality.limits import (
    BoundedReportLimit,
    bounded_report_limit,
)
from histdatacom.data_quality.remediation import (
    remediation_hint_payloads_for_finding,
)
from histdatacom.data_quality.profiles import QUALITY_REPORTING_METADATA_KEY
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
QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY = "remediation_catalog_audit"
QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT = 128
QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT = 128
QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT = 128
QUALITY_PAYLOAD_NEXT_ACTION_LIMIT = 16
QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT = 8
QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT = 16
QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT = 8
QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT = 16
QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT = 8

_FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE = False
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
    fingerprint_distribution = _fingerprint_distribution_summary(report)
    if fingerprint_distribution is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY] = (
            fingerprint_distribution
        )
        payload["metadata"] = metadata
    fingerprint_distribution_attention = (
        _fingerprint_distribution_attention_summary(report)
    )
    if fingerprint_distribution_attention is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
        ] = fingerprint_distribution_attention
        payload["metadata"] = metadata
    fingerprint_regimes = _fingerprint_regime_summary(report)
    if fingerprint_regimes is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY] = (
            fingerprint_regimes
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
    fingerprint_readiness = _fingerprint_readiness_summary(report)
    if fingerprint_readiness is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY] = (
            fingerprint_readiness
        )
        payload["metadata"] = metadata
    fingerprint_readiness_risk = _fingerprint_readiness_risk_summary(report)
    if fingerprint_readiness_risk is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY] = (
            fingerprint_readiness_risk
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
    remediation_catalog_audit = quality_remediation_catalog_audit_summary(
        report
    )
    if remediation_catalog_audit is not None:
        metadata = _mapping_payload(payload.get("metadata"))
        metadata[QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY] = (
            remediation_catalog_audit
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
    lines.extend(
        format_quality_remediation_catalog_audit_lines(
            quality_remediation_catalog_audit_summary(report)
        )
    )
    if _fingerprint_group_selected(check_groups):
        lines.extend(
            _format_fingerprint_coverage_lines(
                _fingerprint_coverage_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_distribution_attention_lines(
                _fingerprint_distribution_attention_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_distribution_summary_lines(
                _fingerprint_distribution_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_regime_summary_lines(
                _fingerprint_regime_summary(report)
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
        lines.extend(
            format_fingerprint_readiness_summary_lines(
                _fingerprint_readiness_summary(report)
            )
        )
        lines.extend(
            format_fingerprint_readiness_risk_lines(
                _fingerprint_readiness_risk_summary(report)
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
    discovery_target_limit: int | None = QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT,
    target_summary_limit: int | None = QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT,
    cross_target_summary_limit: int | None = (
        QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT
    ),
) -> dict[str, JSONValue]:
    """Return a bounded result payload without detailed findings."""
    discovery_target_limit_state = bounded_report_limit(
        discovery_target_limit,
        default_limit=QUALITY_PAYLOAD_DISCOVERY_TARGET_LIMIT,
    )
    target_summary_limit_state = bounded_report_limit(
        target_summary_limit,
        default_limit=QUALITY_PAYLOAD_TARGET_SUMMARY_LIMIT,
    )
    cross_target_summary_limit_state = bounded_report_limit(
        cross_target_summary_limit,
        default_limit=QUALITY_PAYLOAD_CROSS_TARGET_SUMMARY_LIMIT,
    )
    target_summaries = report.target_summaries
    cross_target_summaries = _cross_target_summaries(report)
    fingerprint_coverage = _fingerprint_coverage_summary(report)
    fingerprint_distribution = _fingerprint_distribution_summary(report)
    fingerprint_distribution_attention = (
        _fingerprint_distribution_attention_summary(report)
    )
    fingerprint_regimes = _fingerprint_regime_summary(report)
    fingerprint_topology = _fingerprint_topology_summary(report)
    fingerprint_topology_attention = _fingerprint_topology_attention_summary(
        report
    )
    fingerprint_readiness = _fingerprint_readiness_summary(report)
    fingerprint_readiness_risk = _fingerprint_readiness_risk_summary(report)
    next_actions = quality_next_actions_summary(report)
    remediation_coverage = quality_remediation_coverage_summary(report)
    remediation_catalog_audit = quality_remediation_catalog_audit_summary(
        report
    )
    payload_limits: dict[str, JSONValue] = {
        "discovery_targets": _payload_limit_metadata(
            _sequence_count(discovery.get("targets")),
            discovery_target_limit_state,
        ),
        "target_summaries": _payload_limit_metadata(
            len(target_summaries),
            target_summary_limit_state,
        ),
        "cross_target_summaries": _payload_limit_metadata(
            len(cross_target_summaries),
            cross_target_summary_limit_state,
        ),
        "next_actions": _next_action_payload_limit_metadata(next_actions),
        "remediation_coverage": _remediation_coverage_payload_limit_metadata(
            remediation_coverage
        ),
    }
    payload: dict[str, JSONValue] = {
        "operation": operation,
        "check_groups": list(check_groups),
        "discovery": _bounded_discovery_payload(
            discovery,
            target_limit=discovery_target_limit_state,
        ),
        "summary": report.summary().to_dict(),
        "target_status_counts": _target_status_counts(target_summaries),
        "target_summaries": _bounded_target_summaries(
            target_summaries,
            limit=target_summary_limit_state,
        ),
        "cross_target_summaries": _bounded_json_list(
            cross_target_summaries,
            limit=cross_target_summary_limit_state,
        ),
        "quality_profile": _quality_profile_metadata(report),
        "report_schema_version": QUALITY_REPORT_SCHEMA_VERSION,
        "report_artifact": None if artifact is None else artifact.to_dict(),
        "exit_decision": decision.to_dict(),
        "payload_limits": payload_limits,
    }
    if fingerprint_coverage is not None:
        payload[FINGERPRINT_COVERAGE_BOUNDED_PAYLOAD_KEY] = fingerprint_coverage
    if fingerprint_distribution is not None:
        payload[FINGERPRINT_DISTRIBUTION_BOUNDED_PAYLOAD_KEY] = (
            fingerprint_distribution
        )
    if fingerprint_distribution_attention is not None:
        payload[FINGERPRINT_DISTRIBUTION_ATTENTION_BOUNDED_PAYLOAD_KEY] = (
            fingerprint_distribution_attention
        )
    if fingerprint_regimes is not None:
        payload[FINGERPRINT_REGIME_BOUNDED_PAYLOAD_KEY] = fingerprint_regimes
    if fingerprint_topology is not None:
        payload[FINGERPRINT_TOPOLOGY_BOUNDED_PAYLOAD_KEY] = fingerprint_topology
    if fingerprint_topology_attention is not None:
        payload[FINGERPRINT_TOPOLOGY_ATTENTION_BOUNDED_PAYLOAD_KEY] = (
            fingerprint_topology_attention
        )
    if fingerprint_readiness is not None:
        payload[FINGERPRINT_READINESS_BOUNDED_PAYLOAD_KEY] = (
            fingerprint_readiness
        )
    if fingerprint_readiness_risk is not None:
        payload[FINGERPRINT_READINESS_RISK_BOUNDED_PAYLOAD_KEY] = (
            fingerprint_readiness_risk
        )
    if next_actions is not None:
        payload["next_actions"] = next_actions
    if remediation_coverage is not None:
        payload["remediation_coverage"] = remediation_coverage
    if remediation_catalog_audit is not None:
        payload["remediation_catalog_audit"] = remediation_catalog_audit
        payload_limits["remediation_catalog_audit"] = (
            _remediation_catalog_audit_payload_limit_metadata(
                remediation_catalog_audit
            )
        )
    if not publish_safe:
        return payload
    return _publish_safe_mapping(payload)


def _bounded_discovery_payload(
    discovery: Mapping[str, JSONValue],
    *,
    target_limit: BoundedReportLimit,
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
    limit: BoundedReportLimit,
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
    limit: BoundedReportLimit,
) -> list[JSONValue]:
    return cast(list[JSONValue], limit.slice(values))


def _payload_limit_metadata(
    total_count: int,
    limit: int | BoundedReportLimit | None,
    *,
    default_limit: int | None = None,
    minimum_limit: int = 0,
    maximum_limit: int | None = None,
    allow_unbounded: bool = True,
) -> dict[str, JSONValue]:
    limit_state = _normalize_report_limit(
        limit,
        default_limit=(
            default_limit
            if default_limit is not None
            else (limit if isinstance(limit, int) else 0)
        ),
        minimum_limit=minimum_limit,
        maximum_limit=maximum_limit,
        allow_unbounded=allow_unbounded,
    )
    return cast(  # type: ignore[redundant-cast]
        dict[str, JSONValue],
        limit_state.count_payload(total_count),
    )


def _normalize_report_limit(
    limit: int | BoundedReportLimit | None,
    *,
    default_limit: int,
    minimum_limit: int = 0,
    maximum_limit: int | None = None,
    allow_unbounded: bool = True,
) -> BoundedReportLimit:
    if isinstance(limit, BoundedReportLimit):
        return limit
    return bounded_report_limit(
        limit,
        default_limit=default_limit,
        minimum_limit=minimum_limit,
        maximum_limit=maximum_limit,
        allow_unbounded=allow_unbounded,
    )


def _summary_limit_payload(
    summary: Mapping[str, JSONValue],
    key: str,
) -> dict[str, JSONValue]:
    metadata = _mapping_payload(summary.get("limit_metadata"))
    return _mapping_payload(metadata.get(key))


def _limit_payload_effective_limit(
    payload: Mapping[str, JSONValue],
    *,
    fallback: int,
) -> int:
    value = payload.get("effective_limit", payload.get("limit"))
    if isinstance(value, bool) or value is None:
        return fallback
    if not isinstance(value, (int, float, str)):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _next_action_payload_limit_metadata(
    summary: Mapping[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    if summary is None:
        payload = _payload_limit_metadata(0, QUALITY_PAYLOAD_NEXT_ACTION_LIMIT)
        target_axes = bounded_report_limit(
            QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
        ).limit_payload()
        payload["target_axis_limit"] = (
            QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT
        )
        payload["target_axes"] = target_axes
        return payload
    total_count = _int_metadata(summary, "action_count")
    included_count = _int_metadata(summary, "included_action_count")
    omitted_count = _int_metadata(summary, "omitted_action_count")
    truncated = summary.get("truncated")
    action_limit_payload = _summary_limit_payload(summary, "actions")
    target_axis_limit_payload = _summary_limit_payload(summary, "target_axes")
    if not action_limit_payload:
        action_limit_payload = bounded_report_limit(
            QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
            default_limit=QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
        ).limit_payload()
    if not target_axis_limit_payload:
        target_axis_limit_payload = bounded_report_limit(
            QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
        ).limit_payload()
    metadata_payload: dict[str, JSONValue] = {
        **action_limit_payload,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "truncated": (
            truncated if isinstance(truncated, bool) else omitted_count > 0
        ),
    }
    metadata_payload["target_axis_limit"] = _limit_payload_effective_limit(
        target_axis_limit_payload,
        fallback=QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
    )
    metadata_payload["target_axes"] = target_axis_limit_payload
    return metadata_payload


def _remediation_coverage_payload_limit_metadata(
    summary: Mapping[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    if summary is None:
        payload = _payload_limit_metadata(
            0,
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
        )
        target_axes = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
        ).limit_payload()
        payload["target_axis_limit"] = (
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
        )
        payload["target_axes"] = target_axes
        return payload
    total_count = _int_metadata(summary, "unmapped_group_count")
    included_count = _int_metadata(summary, "included_unmapped_group_count")
    omitted_count = _int_metadata(summary, "omitted_unmapped_group_count")
    truncated = summary.get("unmapped_truncated")
    group_limit_payload = _summary_limit_payload(summary, "groups")
    target_axis_limit_payload = _summary_limit_payload(summary, "target_axes")
    if not group_limit_payload:
        group_limit_payload = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
        ).limit_payload()
    if not target_axis_limit_payload:
        target_axis_limit_payload = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
        ).limit_payload()
    payload = {
        **group_limit_payload,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "truncated": (
            truncated if isinstance(truncated, bool) else omitted_count > 0
        ),
    }
    payload["target_axis_limit"] = _limit_payload_effective_limit(
        target_axis_limit_payload,
        fallback=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
    )
    payload["target_axes"] = target_axis_limit_payload
    return payload


def _remediation_catalog_audit_payload_limit_metadata(
    summary: Mapping[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    if summary is None:
        payload = _payload_limit_metadata(
            0,
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
        )
        rule_counts = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
        ).limit_payload()
        sources = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
        ).limit_payload()
        target_axes = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
        ).limit_payload()
        payload["rule_limit"] = (
            QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT
        )
        payload["rule_counts"] = rule_counts
        payload["source_limit"] = (
            QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT
        )
        payload["sources"] = sources
        payload["target_axis_limit"] = (
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
        )
        payload["target_axes"] = target_axes
        return payload
    limits = _mapping_payload(summary.get("payload_limits"))
    ranked_gaps = _mapping_payload(limits.get("ranked_gaps"))
    total_count = _int_metadata(ranked_gaps, "total_count")
    included_count = _int_metadata(ranked_gaps, "included_count")
    omitted_count = _int_metadata(ranked_gaps, "omitted_count")
    truncated = ranked_gaps.get("truncated")
    rule_limit_payload = _mapping_payload(limits.get("known_rule_id_counts"))
    source_limit_payload = _mapping_payload(limits.get("known_code_sources"))
    target_axis_limit_payload = _mapping_payload(
        _mapping_payload(limits.get("report_unmapped_groups")).get(
            "target_axes"
        )
    )
    if not target_axis_limit_payload:
        target_axis_limit_payload = bounded_report_limit(
            QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
            default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
        ).limit_payload()
    payload = {
        **ranked_gaps,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "truncated": (
            truncated if isinstance(truncated, bool) else omitted_count > 0
        ),
    }
    payload["rule_limit"] = _limit_payload_effective_limit(
        rule_limit_payload,
        fallback=QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    )
    payload["rule_counts"] = rule_limit_payload
    payload["source_limit"] = _limit_payload_effective_limit(
        source_limit_payload,
        fallback=QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    )
    payload["sources"] = source_limit_payload
    payload["target_axis_limit"] = _limit_payload_effective_limit(
        target_axis_limit_payload,
        fallback=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
    )
    payload["target_axes"] = target_axis_limit_payload
    return payload


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
    group_limit: int | None = QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
    target_axis_limit: int | None = (
        QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
    ),
) -> dict[str, JSONValue] | None:
    """Return bounded remediation-catalog coverage for quality findings."""
    summary = report.metadata.get(QUALITY_REMEDIATION_COVERAGE_METADATA_KEY)
    if isinstance(summary, Mapping):
        return dict(summary)
    group_limit_state = bounded_report_limit(
        group_limit,
        default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
    )
    target_axis_limit_state = bounded_report_limit(
        target_axis_limit,
        default_limit=QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
    )

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
    included_unmapped_groups = group_limit_state.slice(unmapped_groups)
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
        limit=group_limit_state,
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
            limit=group_limit_state,
        ),
        "mapped_rule_id_counts": _named_counter_payloads(
            mapped_rule_id_counts,
            key_name="rule_id",
            limit=group_limit_state,
        ),
        "unmapped_rule_id_counts": _named_counter_payloads(
            unmapped_rule_id_counts,
            key_name="rule_id",
            limit=group_limit_state,
        ),
        "finding_code_counts": _named_counter_payloads(
            finding_code_counts,
            key_name="finding_code",
            limit=group_limit_state,
        ),
        "mapped_finding_code_counts": _named_counter_payloads(
            mapped_finding_code_counts,
            key_name="finding_code",
            limit=group_limit_state,
        ),
        "unmapped_finding_code_counts": _named_counter_payloads(
            unmapped_finding_code_counts,
            key_name="finding_code",
            limit=group_limit_state,
        ),
        "count_limits": count_limits,
        "limit_metadata": {
            "groups": group_limit_state.limit_payload(),
            "target_axes": target_axis_limit_state.limit_payload(),
        },
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
                target_axis_limit=target_axis_limit_state,
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


def quality_remediation_catalog_audit_summary(
    report: QualityReport,
    *,
    enabled: bool | None = None,
    code_limit: int | None = QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
    rule_limit: (
        int | None
    ) = QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    source_limit: int | None = (
        QUALITY_PAYLOAD_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT
    ),
    target_axis_limit: int | None = (
        QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
    ),
) -> dict[str, JSONValue] | None:
    """Return an opt-in bounded remediation-catalog audit for a report."""
    summary = report.metadata.get(
        QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    if enabled is None:
        enabled = _remediation_catalog_audit_enabled(report)
    if not enabled:
        return None

    from histdatacom.data_quality.remediation_audit import (
        audit_remediation_catalog,
    )

    audit_summary: dict[str, JSONValue] = audit_remediation_catalog(
        reports=(("current-report", report),),
        code_limit=code_limit,
        rule_limit=rule_limit,
        source_limit=source_limit,
        target_axis_limit=target_axis_limit,
    )
    return audit_summary


def format_quality_remediation_catalog_audit_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise report lines for remediation-catalog audit gaps."""
    if not summary:
        return []
    audit_summary = _mapping_payload(summary.get("summary"))
    warning_error_gap_count = _int_metadata(
        audit_summary,
        "unmapped_warning_error_gap_count",
    )
    report_gap_count = _int_metadata(
        audit_summary,
        "report_unmapped_warning_error_group_count",
    )
    if not warning_error_gap_count and not report_gap_count:
        return []
    lines = [
        "",
        "Remediation catalog audit",
        (
            "- status: "
            f"{_optional_string_metadata(summary, 'status') or 'unknown'} "
            "known warning/error gaps: "
            f"{warning_error_gap_count}"
        ),
        (
            "- observed report: "
            f"reports={_int_metadata(audit_summary, 'report_count')} "
            f"findings={_int_metadata(audit_summary, 'report_finding_count')} "
            "unmapped warning/error groups="
            f"{report_gap_count}"
        ),
    ]
    for group in _remediation_catalog_observed_gap_groups(summary):
        lines.append(
            "- observed " + _format_quality_remediation_coverage_group(group)
        )
    ranked = [
        item
        for item in _list_metadata(summary.get("ranked_gaps"))
        if _optional_string_metadata(item, "max_severity")
        in {"error", "warning"}
    ]
    for gap in ranked:
        lines.append(f"- {_format_quality_remediation_catalog_gap(gap)}")
    return lines


def quality_next_actions_summary(
    report: QualityReport,
    *,
    action_limit: int | None = QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
    target_axis_limit: (
        int | None
    ) = QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded run-level next actions from remediation hints."""
    summary = report.metadata.get(QUALITY_NEXT_ACTIONS_METADATA_KEY)
    if isinstance(summary, Mapping):
        return dict(summary)
    action_limit_state = bounded_report_limit(
        action_limit,
        default_limit=QUALITY_PAYLOAD_NEXT_ACTION_LIMIT,
    )
    target_axis_limit_state = bounded_report_limit(
        target_axis_limit,
        default_limit=QUALITY_PAYLOAD_NEXT_ACTION_TARGET_AXIS_LIMIT,
    )

    aggregates = _next_action_aggregates(report)
    if not aggregates:
        return None

    actions = sorted(
        (
            _next_action_payload(
                aggregate,
                target_axis_limit=target_axis_limit_state,
            )
            for aggregate in aggregates.values()
        ),
        key=_next_action_sort_key,
    )
    included = action_limit_state.slice(actions)
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
        "limit_metadata": {
            "actions": action_limit_state.limit_payload(),
            "target_axes": target_axis_limit_state.limit_payload(),
        },
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
    target_axis_limit: BoundedReportLimit,
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
        "limit_metadata": {
            "target_axes": target_axis_limit.limit_payload(),
        },
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
    limit: BoundedReportLimit,
) -> list[JSONValue]:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-item[1], item[0]),
    )
    included = limit.slice(ordered)
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
    target_axis_limit: BoundedReportLimit,
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
        "limit_metadata": {
            "target_axes": target_axis_limit.limit_payload(),
        },
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
    limit: BoundedReportLimit,
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
    limit: BoundedReportLimit,
) -> list[JSONValue]:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-item[1], item[0]),
    )
    included = limit.slice(ordered)
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


def _format_quality_remediation_catalog_gap(
    gap: Mapping[str, JSONValue],
) -> str:
    return (
        f"{_optional_string_metadata(gap, 'max_severity')} "
        f"rank={_int_metadata(gap, 'rank')} "
        f"{_optional_string_metadata(gap, 'rule_id')}:"
        f"{_optional_string_metadata(gap, 'finding_code')} "
        "known="
        f"{_int_metadata(gap, 'known_source_occurrence_count')} "
        "observed="
        f"{_int_metadata(gap, 'report_occurrence_count')}"
    )


def _remediation_catalog_observed_gap_groups(
    summary: Mapping[str, JSONValue],
) -> list[Mapping[str, JSONValue]]:
    groups: list[Mapping[str, JSONValue]] = []
    for report_payload in _list_metadata(summary.get("report_coverage")):
        coverage = _mapping_payload(report_payload.get("remediation_coverage"))
        for group in _list_metadata(coverage.get("unmapped_groups")):
            if _optional_string_metadata(group, "max_severity") in {
                "error",
                "warning",
            }:
                groups.append(group)
    return groups


def _remediation_catalog_audit_enabled(report: QualityReport) -> bool:
    reporting = _mapping_payload(
        report.metadata.get(QUALITY_REPORTING_METADATA_KEY)
    )
    audit = _mapping_payload(
        reporting.get(QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY)
    )
    enabled = audit.get("enabled")
    if isinstance(enabled, bool):
        return enabled

    profile = _mapping_payload(report.metadata.get("quality_profile"))
    profile_reporting = _mapping_payload(profile.get("reporting"))
    profile_audit = _mapping_payload(
        profile_reporting.get(QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY)
    )
    profile_enabled = profile_audit.get("enabled")
    return profile_enabled if isinstance(profile_enabled, bool) else False


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


def _fingerprint_distribution_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint distribution metadata from report or findings."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_distribution_summary(report.findings)
    )


def _fingerprint_distribution_attention_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint distribution attention metadata."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_distribution_attention_summary(report.findings)
    )


def _fingerprint_regime_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint regime metadata from report or findings."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_regime_summary(report.findings)
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


def _fingerprint_readiness_summary(
    report: QualityReport,
) -> dict[str, JSONValue] | None:
    """Return fingerprint readiness metadata from the report or findings."""
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
    )
    if isinstance(summary, Mapping):
        return dict(summary)
    return _optional_mapping_payload(
        series_fingerprint_readiness_summary(report.findings)
    )


def fingerprint_readiness_risk_summary(
    report: QualityReport,
    *,
    target_limit: int | None = None,
    section_limit: int | None = None,
    reason_limit: int | None = None,
) -> dict[str, JSONValue] | None:
    """Return fingerprint readiness risk metadata for one report."""
    return _fingerprint_readiness_risk_summary(
        report,
        target_limit=target_limit,
        section_limit=section_limit,
        reason_limit=reason_limit,
    )


def _fingerprint_readiness_risk_summary(
    report: QualityReport,
    *,
    target_limit: int | None = None,
    section_limit: int | None = None,
    reason_limit: int | None = None,
) -> dict[str, JSONValue] | None:
    summary = report.metadata.get(
        TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY
    )
    if (
        isinstance(summary, Mapping)
        and target_limit is None
        and section_limit is None
        and reason_limit is None
    ):
        return dict(summary)
    findings = tuple(report.findings)
    if not _has_fingerprint_series_findings(findings):
        return None
    return _optional_mapping_payload(
        series_fingerprint_readiness_risk_summary(
            findings,
            target_limit=target_limit,
            section_limit=section_limit,
            reason_limit=reason_limit,
            report_surface_evidence=(
                _fingerprint_report_surface_evidence_for_readiness_risk()
            ),
        )
    )


def _has_fingerprint_series_findings(
    findings: tuple[QualityFinding, ...],
) -> bool:
    return any(
        isinstance(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY),
            Mapping,
        )
        for finding in findings
    )


def _fingerprint_report_surface_evidence_for_readiness_risk() -> (
    dict[str, JSONValue]
):
    global _FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE  # noqa: PLW0603
    if _FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE:
        return {}
    _FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE = True
    try:
        return cast(  # type: ignore[redundant-cast]
            dict[str, JSONValue],
            fingerprint_report_surface_evidence(),
        )
    finally:
        _FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE = False


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


def format_fingerprint_distribution_attention_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for distribution attention."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint distribution attention",
        (
            "- targets needing attention: "
            f"{_int_metadata(summary, 'attention_target_count')} "
            "included: "
            f"{_int_metadata(summary, 'included_attention_target_count')} "
            "omitted: "
            f"{_int_metadata(summary, 'omitted_attention_target_count')}"
        ),
    ]
    threshold_text = _format_fingerprint_distribution_attention_thresholds(
        summary.get("attention_thresholds")
    )
    if threshold_text:
        lines.append(f"- thresholds: {threshold_text}")
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_distribution_attention_target_line(item)}"
                )
    return lines


def _format_fingerprint_distribution_attention_thresholds(
    value: JSONValue,
) -> str:
    thresholds = _mapping_payload(value)
    if not thresholds:
        return ""
    return (
        "invalid rows >= "
        f"{_int_metadata(thresholds, 'invalid_row_min_count')} "
        "and rate >= "
        f"{_format_rate(thresholds.get('invalid_row_min_rate'))}; "
        "zero spreads >= "
        f"{_int_metadata(thresholds, 'zero_spread_min_count')} "
        "and rate >= "
        f"{_format_rate(thresholds.get('zero_spread_min_rate'))}; "
        "negative spreads >= "
        f"{_int_metadata(thresholds, 'negative_spread_min_count')} "
        "and rate >= "
        f"{_format_rate(thresholds.get('negative_spread_min_rate'))}; "
        "truncated="
        f"{_bool_text(thresholds.get('flag_truncated_distribution'))}; "
        "cache_float_precision="
        f"{_bool_text(thresholds.get('flag_cache_float_precision'))}"
    )


def _format_fingerprint_distribution_attention_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    data_format = _string_metadata(axis, "data_format")
    symbol = _string_metadata(axis, "symbol")
    timeframe = _string_metadata(axis, "timeframe")
    period = _string_metadata(axis, "period")
    kind = _string_metadata(axis, "kind")
    flags = _string_list_metadata(summary.get("attention_flags"))
    flag_text = ", ".join(flags) if flags else "no advisory flags"
    cache_source = _optional_string_metadata(summary, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    return (
        f"{data_format} {symbol} {timeframe} {period} {kind}: "
        f"{_string_metadata(summary, 'attention_level')}, "
        f"{_string_metadata(summary, 'distribution_kind')}, "
        f"{flag_text}, "
        f"rows={_int_metadata(summary, 'row_count')}, "
        f"usable={_int_metadata(summary, 'usable_row_count')}, "
        f"invalid={_int_metadata(summary, 'invalid_row_count')}, "
        f"sampled={_int_metadata(summary, 'sampled_row_count')}, "
        f"zero spread={_format_rate(summary.get('zero_spread_rate'))}, "
        f"negative spread={_format_rate(summary.get('negative_spread_rate'))}, "
        f"precision={_string_metadata(summary, 'precision_source')}, "
        f"source={_string_metadata(summary, 'distribution_source')}"
        f"{cache_text}"
    )


def format_fingerprint_distribution_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for fingerprint distributions."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint distributions",
        (
            "- targets: "
            f"{_int_metadata(summary, 'target_count')} "
            "with distributions: "
            f"{_int_metadata(summary, 'distribution_target_count')} "
            f"m1: {_int_metadata(summary, 'm1_bar_distribution_target_count')} "
            f"tick: {_int_metadata(summary, 'tick_distribution_target_count')} "
            "missing: "
            f"{_int_metadata(summary, 'missing_distribution_target_count')}"
        ),
        (
            "- data conditions: "
            f"empty={_int_metadata(summary, 'empty_distribution_target_count')} "
            f"invalid={_int_metadata(summary, 'invalid_row_target_count')} "
            f"partial={_int_metadata(summary, 'partial_row_target_count')} "
            "truncated="
            f"{_int_metadata(summary, 'truncated_distribution_target_count')}"
        ),
    ]
    count_lines = (
        ("distribution kinds", "distribution_kind_counts"),
        ("sources", "distribution_source_counts"),
        ("source kinds", "source_kind_counts"),
        ("cache sources", "cache_source_counts"),
        ("precision sources", "precision_source_counts"),
        ("statuses", "status_counts"),
    )
    for label, key in count_lines:
        counts = _format_count_metadata(summary.get(key))
        if counts:
            lines.append(f"- {label}: {counts}")
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_distribution_target_line(item)}"
                )
    return lines


def _format_fingerprint_distribution_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    cache_source = _optional_string_metadata(summary, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    return (
        f"{_string_metadata(axis, 'data_format')} "
        f"{_string_metadata(axis, 'symbol')} "
        f"{_string_metadata(axis, 'timeframe')} "
        f"{_string_metadata(axis, 'period')} "
        f"{_string_metadata(axis, 'kind')}: "
        f"{_string_metadata(summary, 'status')}, "
        f"{_string_metadata(summary, 'distribution_kind')}, "
        f"{_int_metadata(summary, 'row_count')} rows, "
        f"{_int_metadata(summary, 'usable_row_count')} usable, "
        f"{_int_metadata(summary, 'invalid_row_count')} invalid, "
        f"{_int_metadata(summary, 'sampled_row_count')} sampled, "
        f"truncated={_bool_text(summary.get('truncated'))}, "
        f"precision={_string_metadata(summary, 'precision_source')}, "
        f"source={_string_metadata(summary, 'distribution_source')}"
        f"{cache_text}"
    )


def format_fingerprint_regime_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for fingerprint regimes."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint regimes",
        (
            "- targets: "
            f"{_int_metadata(summary, 'target_count')} "
            f"included: {_int_metadata(summary, 'included_target_count')} "
            f"omitted: {_int_metadata(summary, 'omitted_target_count')} "
            "calendar: "
            f"{_int_metadata(summary, 'calendar_regime_target_count')} "
            "conditioned-spread: "
            f"{_int_metadata(summary, 'conditional_distribution_target_count')}"
        ),
    ]
    count_lines = (
        ("calendar statuses", "calendar_status_counts"),
        ("conditioned spread statuses", "conditional_status_counts"),
        ("computed from", "computed_from_counts"),
        ("cache sources", "cache_source_counts"),
    )
    for label, key in count_lines:
        counts = _format_count_metadata(summary.get(key))
        if counts:
            lines.append(f"- {label}: {counts}")
    profile = _mapping_payload(summary.get("calendar_profile"))
    if profile:
        source_counts = _format_count_metadata(profile.get("source_counts"))
        version_counts = _format_count_metadata(profile.get("version_counts"))
        suffix = []
        if source_counts:
            suffix.append(f"sources: {source_counts}")
        if version_counts:
            suffix.append(f"versions: {version_counts}")
        suffix_text = f" {'; '.join(suffix)}" if suffix else ""
        lines.append(
            "- calendar profile: "
            f"complete={_int_metadata(profile, 'complete_count')} "
            f"incomplete={_int_metadata(profile, 'incomplete_count')} "
            "static-advisory="
            f"{_int_metadata(profile, 'static_advisory_count')}"
            f"{suffix_text}"
        )
    for label, key in (
        ("session states", "top_session_state_counts"),
        ("active sessions", "top_active_session_counts"),
        ("special tags", "top_special_tag_counts"),
        ("holiday tags", "top_holiday_tag_counts"),
        ("event tags", "top_event_tag_counts"),
        ("source hours", "top_hour_of_day_counts"),
        ("source days", "top_day_of_week_counts"),
    ):
        counts = _format_count_rows(summary.get(key))
        if counts:
            lines.append(f"- {label}: {counts}")
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_regime_target_line(item)}"
                )
    return lines


def _format_fingerprint_regime_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    calendar = _mapping_payload(summary.get("calendar_regimes"))
    conditional = _mapping_payload(summary.get("conditional_distributions"))
    profile = _mapping_payload(calendar.get("calendar_profile"))
    cache_source = _optional_string_metadata(calendar, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    session_counts = _format_count_metadata(
        calendar.get("session_state_counts")
    )
    active_counts = _format_count_metadata(
        calendar.get("active_session_counts")
    )
    special_counts = _format_count_metadata(calendar.get("special_tag_counts"))
    holiday_counts = _format_count_metadata(calendar.get("holiday_tag_counts"))
    event_counts = _format_count_metadata(calendar.get("event_tag_counts"))
    hour_counts = _format_count_metadata(calendar.get("hour_of_day_counts"))
    day_counts = _format_count_metadata(calendar.get("day_of_week_counts"))
    counts = []
    for label, value in (
        ("sessions", session_counts),
        ("active", active_counts),
        ("special", special_counts),
        ("holiday", holiday_counts),
        ("event", event_counts),
        ("hours", hour_counts),
        ("days", day_counts),
    ):
        if value:
            counts.append(f"{label}={value}")
    counts_text = "; ".join(counts) if counts else "no calendar buckets"
    conditional_text = _format_regime_conditioned_spread(conditional)
    if conditional_text:
        conditional_text = f"; {conditional_text}"
    return (
        f"{_string_metadata(axis, 'data_format')} "
        f"{_string_metadata(axis, 'symbol')} "
        f"{_string_metadata(axis, 'timeframe')} "
        f"{_string_metadata(axis, 'period')} "
        f"{_string_metadata(axis, 'kind')}: "
        f"calendar={_string_metadata(calendar, 'status')} "
        f"raw={_string_metadata(calendar, 'raw_status')}, "
        f"rows={_int_metadata(calendar, 'row_count')} "
        f"parsed={_int_metadata(calendar, 'parsed_row_count')} "
        f"invalid={_int_metadata(calendar, 'invalid_timestamp_count')}, "
        f"computed_from={_string_metadata(calendar, 'computed_from')}"
        f"{cache_text}, "
        "profile="
        f"{_string_metadata(profile, 'source')}/"
        f"{_string_metadata(profile, 'version')} "
        f"complete={_bool_text(profile.get('complete'))} "
        f"static-advisory={_bool_text(profile.get('static_advisory'))}; "
        f"{counts_text}"
        f"{conditional_text}"
    )


def _format_regime_conditioned_spread(
    conditional: Mapping[str, JSONValue],
) -> str:
    status = _string_metadata(conditional, "status")
    if status != "available":
        if status in {"absent", "not_applicable"}:
            return f"conditioned_spread={status}"
        return ""
    by_session = _format_conditioned_spread_rows(
        conditional.get("by_active_session")
    )
    by_special = _format_conditioned_spread_rows(
        conditional.get("by_special_tag")
    )
    parts = [
        "conditioned_spread="
        f"{_string_metadata(conditional, 'basis')} "
        f"rows={_int_metadata(conditional, 'row_count')} "
        f"usable={_int_metadata(conditional, 'usable_row_count')}"
    ]
    if by_session:
        parts.append(f"by_session={by_session}")
    if by_special:
        parts.append(f"by_special={by_special}")
    return " ".join(parts)


def _format_conditioned_spread_rows(value: JSONValue) -> str:
    rows = _list_metadata(value)
    parts: list[str] = []
    for row in rows:
        spread = _mapping_payload(row.get("spread"))
        parts.append(
            f"{_string_metadata(row, 'bucket')}:"
            f"n={_int_metadata(row, 'count')}/"
            f"median={_format_rate(spread.get('median'))}/"
            f"p95={_format_rate(spread.get('p95'))}"
        )
    return ";".join(parts)


def _format_count_rows(value: JSONValue) -> str:
    rows = _list_metadata(value)
    parts: list[str] = []
    for row in rows:
        name = _string_metadata(row, "value")
        count = _int_metadata(row, "count")
        if name != "unknown" or count:
            parts.append(f"{name}={count}")
    return ", ".join(parts)


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


def format_fingerprint_readiness_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for fingerprint readiness."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint readiness",
        (
            "- targets: "
            f"{_int_metadata(summary, 'target_count')} "
            f"included: {_int_metadata(summary, 'included_target_count')} "
            f"omitted: {_int_metadata(summary, 'omitted_target_count')}"
        ),
    ]
    status_counts = _format_count_metadata(
        summary.get("applicable_dynamics_status_counts")
    )
    if status_counts:
        lines.append(f"- applicable dynamics statuses: {status_counts}")
    for label, section in (
        ("return dynamics", "return_dynamics"),
        ("microstructure dynamics", "microstructure_dynamics"),
    ):
        counts = _format_nested_count_metadata(
            summary.get("dynamics_status_counts"),
            section,
        )
        reasons = _format_nested_count_metadata(
            summary.get("dynamics_reason_counts"),
            section,
        )
        if counts:
            line = f"- {label}: {counts}"
            if reasons:
                line += f" reasons: {reasons}"
            lines.append(line)
    dependence_counts = _format_count_metadata(
        summary.get("dependence_status_counts")
    )
    if dependence_counts:
        dependence_line = f"- dependence: {dependence_counts}"
        dependence_reasons = _format_count_metadata(
            summary.get("dependence_reason_counts")
        )
        if dependence_reasons:
            dependence_line += f" reasons: {dependence_reasons}"
        skipped_reasons = _format_count_metadata(
            summary.get("dependence_skipped_lag_reason_counts")
        )
        if skipped_reasons:
            dependence_line += f" skipped-lag reasons: {skipped_reasons}"
        acf_basis = _format_count_metadata(
            summary.get("dependence_acf_basis_counts")
        )
        if acf_basis:
            dependence_line += f" acf_basis: {acf_basis}"
        dependence_line += (
            " computed_lags="
            f"{_int_metadata(summary, 'dependence_computed_lag_count')} "
            "skipped_lags="
            f"{_int_metadata(summary, 'dependence_skipped_lag_count')}"
        )
        lines.append(dependence_line)
    count_lines = (
        ("topology limitations", "topology_limitation_counts"),
        ("dynamics limitations", "dynamics_limitation_counts"),
        ("dependence limitations", "dependence_limitation_counts"),
        ("row order", "row_order_counts"),
        ("computed from", "computed_from_counts"),
        ("cache sources", "cache_source_counts"),
        ("section skips", "section_skip_reason_counts"),
        (
            "tick spread conditioning",
            "tick_spread_conditioning_status_counts",
        ),
    )
    for label, key in count_lines:
        counts = _format_count_metadata(summary.get(key))
        if counts:
            lines.append(f"- {label}: {counts}")
    profile = _mapping_payload(summary.get("profile_completeness"))
    if profile:
        lines.append(
            "- calendar profile: "
            f"complete={_int_metadata(profile, 'calendar_profile_complete_count')} "
            "incomplete="
            f"{_int_metadata(profile, 'calendar_profile_incomplete_count')} "
            "static-advisory="
            f"{_int_metadata(profile, 'calendar_profile_static_advisory_count')}"
        )
    target_summaries = summary.get("target_summaries")
    if isinstance(target_summaries, list):
        for item in target_summaries:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_readiness_target_line(item)}"
                )
    return lines


def format_fingerprint_readiness_risk_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable lines for readiness risk ranking."""
    if not summary:
        return []
    lines = [
        "",
        "Fingerprint readiness risk",
        (
            "- targets: "
            f"{_int_metadata(summary, 'target_count')} "
            f"risk: {_int_metadata(summary, 'risk_target_count')} "
            f"clean: {_int_metadata(summary, 'clean_target_count')} "
            f"included: {_int_metadata(summary, 'included_target_count')} "
            f"omitted: {_int_metadata(summary, 'omitted_target_count')}"
        ),
    ]
    risk_levels = _format_count_metadata(summary.get("risk_level_counts"))
    if risk_levels:
        lines.append(f"- risk levels: {risk_levels}")
    reasons = _format_count_metadata(summary.get("reason_counts"))
    if reasons:
        lines.append(f"- top reasons: {reasons}")
    sections = _format_count_metadata(summary.get("section_risk_counts"))
    if sections:
        lines.append(f"- sections: {sections}")
    surface = _mapping_payload(summary.get("report_surface_evidence"))
    surface_count = _int_metadata(surface, "surface_count")
    if surface_count:
        cli_states = _format_count_metadata(
            surface.get("cli_summary_state_counts")
        )
        lines.append(
            "- report surfaces: "
            f"{surface_count}"
            f"{' cli=' + cli_states if cli_states else ''}"
        )
    target_risks = summary.get("target_risks")
    if isinstance(target_risks, list):
        for item in target_risks:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {_format_fingerprint_readiness_risk_target(item)}"
                )
    return lines


def _format_fingerprint_readiness_risk_target(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    reasons = _string_list_metadata(summary.get("reason_codes"))
    reason_text = ", ".join(reasons) if reasons else "none"
    sections = [
        _string_metadata(section, "section")
        for section in _list_metadata(summary.get("section_risks"))
    ]
    section_text = ", ".join(sections) if sections else "none"
    return (
        f"#{_int_metadata(summary, 'rank')} "
        f"{_string_metadata(axis, 'data_format')} "
        f"{_string_metadata(axis, 'symbol')} "
        f"{_string_metadata(axis, 'timeframe')} "
        f"{_string_metadata(axis, 'period')} "
        f"{_string_metadata(axis, 'kind')}: "
        f"{_string_metadata(summary, 'risk_level')} "
        f"score={_int_metadata(summary, 'risk_score')} "
        f"sections={section_text} reasons={reason_text}"
    )


def _format_nested_count_metadata(value: JSONValue, key: str) -> str:
    return _format_count_metadata(_mapping_payload(value).get(key))


def _format_fingerprint_readiness_target_line(
    summary: Mapping[str, JSONValue],
) -> str:
    axis = _mapping_payload(summary.get("target_axis"))
    section = _string_metadata(summary, "applicable_dynamics_section")
    dynamics = _mapping_payload(summary.get(section))
    if not dynamics:
        dynamics = _mapping_payload(summary.get("return_dynamics"))
    status = _string_metadata(summary, "applicable_dynamics_status")
    reason = _optional_string_metadata(summary, "applicable_dynamics_reason")
    reason_text = f", reason={reason}" if reason else ""
    limitations = _string_list_metadata(dynamics.get("limitations"))
    limitation_text = ", ".join(limitations) if limitations else "none"
    cache_source = _optional_string_metadata(dynamics, "cache_source")
    cache_text = f", cache={cache_source}" if cache_source else ""
    details = _format_fingerprint_readiness_dynamics_details(section, dynamics)
    details_text = f", {details}" if details else ""
    dependence_details = _format_fingerprint_readiness_dependence_details(
        _mapping_payload(summary.get("dependence"))
    )
    dependence_text = f", {dependence_details}" if dependence_details else ""
    return (
        f"{_string_metadata(axis, 'data_format')} "
        f"{_string_metadata(axis, 'symbol')} "
        f"{_string_metadata(axis, 'timeframe')} "
        f"{_string_metadata(axis, 'period')} "
        f"{_string_metadata(axis, 'kind')}: "
        f"{section} {status}"
        f"{reason_text}, "
        f"sections emitted={_int_metadata(summary, 'sections_emitted_count')} "
        f"skipped={_int_metadata(summary, 'sections_skipped_count')}, "
        f"basis={_string_metadata(dynamics, 'basis')}, "
        f"row_order={_string_metadata(dynamics, 'row_order')}, "
        f"computed_from={_string_metadata(dynamics, 'computed_from')}"
        f"{cache_text}, "
        f"rows={_int_metadata(dynamics, 'row_count')} "
        f"usable={_int_metadata(dynamics, 'usable_row_count')} "
        f"invalid={_int_metadata(dynamics, 'invalid_row_count')} "
        f"sampled={_int_metadata(dynamics, 'sampled_row_count')} "
        f"truncated={_bool_text(dynamics.get('truncated'))}, "
        f"limitations={limitation_text}"
        f"{details_text}"
        f"{dependence_text}"
    )


def _format_fingerprint_readiness_dynamics_details(
    section: str,
    dynamics: Mapping[str, JSONValue],
) -> str:
    if section == "return_dynamics":
        close_return = _mapping_payload(dynamics.get("close_log_return"))
        absolute_return = _mapping_payload(dynamics.get("absolute_return"))
        open_jump = _mapping_payload(dynamics.get("open_jump"))
        flatline = _mapping_payload(dynamics.get("flatline"))
        if not any((close_return, absolute_return, open_jump, flatline)):
            return ""
        return (
            "close_returns="
            f"{_int_metadata(close_return, 'count')} "
            f"median={_format_rate(close_return.get('median'))}; "
            "abs_returns="
            f"{_int_metadata(absolute_return, 'count')} "
            f"p95={_format_rate(absolute_return.get('p95'))}; "
            "open_jumps="
            f"{_int_metadata(open_jump, 'count')} "
            f"p95={_format_rate(open_jump.get('p95'))}; "
            "zero_returns="
            f"{_int_metadata(flatline, 'zero_return_count')} "
            f"rate={_format_rate(flatline.get('zero_return_rate'))}; "
            "flatline_runs="
            f"{_int_metadata(flatline, 'ohlc_flatline_run_count')}"
        )
    if section == "microstructure_dynamics":
        interarrival = _mapping_payload(dynamics.get("interarrival_ms"))
        spread = _mapping_payload(dynamics.get("spread"))
        spread_change = _mapping_payload(dynamics.get("spread_change"))
        spread_jump = _mapping_payload(dynamics.get("spread_jump"))
        stale = _mapping_payload(dynamics.get("stale_quote"))
        burst = _mapping_payload(dynamics.get("burst"))
        one_sided = _mapping_payload(dynamics.get("one_sided_movement"))
        if not any(
            (
                interarrival,
                spread,
                spread_change,
                spread_jump,
                stale,
                burst,
                one_sided,
            )
        ):
            return ""
        return (
            "interarrival="
            f"{_int_metadata(interarrival, 'count')} "
            f"median={_format_duration_ms(interarrival.get('median'))}; "
            f"spread_median={_format_rate(spread.get('median'))}; "
            "spread_change="
            f"{_int_metadata(spread_change, 'count')} "
            f"p95={_format_rate(spread_change.get('p95'))}; "
            "spread_jumps="
            f"{_int_metadata(spread_jump, 'count')} "
            f"rate={_format_rate(spread_jump.get('rate'))}; "
            "stale_runs="
            f"{_int_metadata(stale, 'run_count')} "
            f"repeat_rate={_format_rate(stale.get('repeat_rate'))}; "
            f"burst_rate={_format_rate(burst.get('burst_rate'))}; "
            "one_sided="
            f"{_int_metadata(one_sided, 'count')} "
            f"bid_only={_int_metadata(one_sided, 'bid_only_count')} "
            f"ask_only={_int_metadata(one_sided, 'ask_only_count')}"
        )
    return ""


def _format_fingerprint_readiness_dependence_details(
    dependence: Mapping[str, JSONValue],
) -> str:
    if not dependence:
        return ""
    status = _string_metadata(dependence, "status")
    reason = _optional_string_metadata(dependence, "reason")
    reason_text = f" reason={reason}" if reason else ""
    skipped_reasons = _format_count_metadata(
        dependence.get("skipped_lag_reason_counts")
    )
    skipped_reason_text = (
        f" skipped_reasons={skipped_reasons}" if skipped_reasons else ""
    )
    series = _format_dependence_series_metadata(dependence.get("series"))
    series_text = f" series={series}" if series else ""
    return (
        f"dependence={status}{reason_text} "
        f"acf_basis={_string_metadata(dependence, 'acf_basis')} "
        f"lags={_format_lag_metadata(dependence)} "
        "computed_lags="
        f"{_int_metadata(dependence, 'computed_lag_count')} "
        "skipped_lags="
        f"{_int_metadata(dependence, 'skipped_lag_count')}"
        f"{skipped_reason_text}"
        f"{series_text}"
    )


def _format_lag_metadata(dependence: Mapping[str, JSONValue]) -> str:
    lags = _int_list_metadata(dependence.get("lags"))
    omitted = _int_metadata(dependence, "omitted_lag_count")
    lag_text = "[" + ",".join(str(lag) for lag in lags) + "]"
    if omitted:
        lag_text += f"+{omitted}"
    return lag_text


def _format_dependence_series_metadata(value: JSONValue) -> str:
    series = _mapping_payload(value)
    parts: list[str] = []
    for name in sorted(series):
        summary = _mapping_payload(series[name])
        if not summary:
            continue
        parts.append(
            f"{name}:samples={_int_metadata(summary, 'sample_count')}/"
            f"computed={_int_metadata(summary, 'computed_lag_count')}/"
            f"skipped={_int_metadata(summary, 'skipped_lag_count')}"
        )
    return ";".join(parts)


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


def _int_list_metadata(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    parsed: list[int] = []
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            parsed.append(int(item))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
    return parsed


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


def _format_rate(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return "unavailable"
    if isinstance(value, (int, float)):
        return f"{float(value):.6g}"
    text = str(value).strip()
    return text or "unavailable"


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


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
