"""Rule orchestration for HistData data-quality assessments."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityTarget,
)
from histdatacom.runtime_contracts import JSONValue


def evaluate_quality_rule(
    rule: QualityRule,
    target: QualityTarget,
) -> QualityRuleResult:
    """Evaluate one data-quality rule against one target."""
    return QualityRuleResult(
        rule_id=rule.rule_id,
        target=target,
        findings=tuple(rule.evaluate(target)),
    )


def run_quality_assessment(
    targets: Iterable[QualityTarget],
    rules: Iterable[QualityRule],
    *,
    metadata: Mapping[str, JSONValue] | None = None,
) -> QualityReport:
    """Run every rule against every target through one orchestration path."""
    target_tuple = tuple(targets)
    rule_tuple = tuple(rules)
    return QualityReport(
        targets=target_tuple,
        rule_results=tuple(
            evaluate_quality_rule(rule, target)
            for target in target_tuple
            for rule in rule_tuple
        ),
        metadata=dict(metadata or {}),
    )
