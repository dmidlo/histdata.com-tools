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

__all__ = [
    "QualityFinding",
    "QualityDiscoveryError",
    "QualityDiscoveryResult",
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
    "discover_quality_targets",
    "evaluate_quality_rule",
    "normalize_quality_check_groups",
    "quality_target_from_path",
    "run_quality_assessment",
]
