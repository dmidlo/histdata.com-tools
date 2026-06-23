"""Rule orchestration for HistData data-quality assessments."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityRunRule,
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
    run_rules: Iterable[QualityRunRule] = (),
    metadata: Mapping[str, JSONValue] | None = None,
) -> QualityReport:
    """Run every rule against every target through one orchestration path."""
    target_tuple = tuple(targets)
    rule_tuple = tuple(rules)
    base_metadata = dict(metadata or {})
    rule_results = tuple(
        evaluate_quality_rule(rule, target)
        for target in target_tuple
        for rule in rule_tuple
    )
    run_reports = tuple(
        rule.evaluate_run(target_tuple, metadata=base_metadata)
        for rule in tuple(run_rules)
    )
    merged_metadata = dict(base_metadata)
    for report in run_reports:
        merged_metadata.update(report.metadata)

    return QualityReport(
        targets=_merge_targets(
            target_tuple,
            *(report.targets for report in run_reports),
        ),
        rule_results=(
            rule_results
            + tuple(
                result
                for report in run_reports
                for result in report.rule_results
            )
        ),
        metadata=merged_metadata,
    )


def _merge_targets(
    targets: tuple[QualityTarget, ...],
    *target_groups: tuple[QualityTarget, ...],
) -> tuple[QualityTarget, ...]:
    merged = list(targets)
    for target in (target for group in target_groups for target in group):
        if target not in merged:
            merged.append(target)
    return tuple(merged)
