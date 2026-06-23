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
from histdatacom.data_quality.engine import (
    evaluate_quality_rule,
    run_quality_assessment,
)

__all__ = [
    "QualityFinding",
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
    "evaluate_quality_rule",
    "run_quality_assessment",
]
