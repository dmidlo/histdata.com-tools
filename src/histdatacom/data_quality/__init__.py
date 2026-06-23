"""Data-quality assessment contracts and orchestration helpers."""

from __future__ import annotations

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityRunSummary,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    QualityTargetSummary,
)
from histdatacom.data_quality.discovery import (
    QUALITY_CHECK_GROUPS,
    QualityDiscoveryError,
    QualityDiscoveryResult,
    discover_quality_targets,
    normalize_quality_check_groups,
    quality_target_from_path,
)
from histdatacom.data_quality.engine import (
    evaluate_quality_rule,
    run_quality_assessment,
)
from histdatacom.data_quality.reporting import (
    QUALITY_EXIT_TRIGGERS,
    QUALITY_REPORT_SCHEMA_VERSION,
    QualityExitDecision,
    QualityExitPolicy,
    QualityExitTrigger,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
    quality_report_to_json,
    write_quality_report,
)

__all__ = [
    "QUALITY_EXIT_TRIGGERS",
    "QUALITY_REPORT_SCHEMA_VERSION",
    "QualityFinding",
    "QualityDiscoveryError",
    "QualityDiscoveryResult",
    "QualityExitDecision",
    "QualityExitPolicy",
    "QualityExitTrigger",
    "QualityLocation",
    "QualityReport",
    "QualityRule",
    "QualityRuleResult",
    "QualityRunSummary",
    "QualitySeverity",
    "QualityStatus",
    "QualityTarget",
    "QualityTargetKind",
    "QualityTargetSummary",
    "QUALITY_CHECK_GROUPS",
    "bounded_quality_payload",
    "discover_quality_targets",
    "evaluate_quality_rule",
    "format_quality_console_summary",
    "normalize_quality_check_groups",
    "quality_report_payload",
    "quality_report_to_json",
    "quality_target_from_path",
    "run_quality_assessment",
    "write_quality_report",
]
