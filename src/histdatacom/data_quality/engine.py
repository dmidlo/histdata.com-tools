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
    csv_dimensions = _csv_target_dimensions(target_tuple)
    skipped_duplicate_archive_rule_count = 0
    rule_results_list: list[QualityRuleResult] = []
    for target in target_tuple:
        for rule in rule_tuple:
            if _should_skip_duplicate_archive_rule(
                rule,
                target,
                csv_dimensions,
            ):
                skipped_duplicate_archive_rule_count += 1
                continue
            rule_results_list.append(evaluate_quality_rule(rule, target))

    if skipped_duplicate_archive_rule_count:
        base_metadata["quality_engine"] = {
            "target_count": len(target_tuple),
            "rule_count": len(rule_tuple),
            "target_rule_evaluation_count": len(rule_results_list),
            "skipped_duplicate_archive_rule_evaluation_count": (
                skipped_duplicate_archive_rule_count
            ),
            "duplicate_archive_scan_policy": (
                "prefer_extracted_csv_for_non_inventory_rules"
            ),
        }
    rule_results = tuple(rule_results_list)
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


def _csv_target_dimensions(
    targets: tuple[QualityTarget, ...],
) -> set[tuple[str, str, str, str]]:
    return {
        _target_dimension(target)
        for target in targets
        if target.kind.value == "csv" and all(_target_dimension(target))
    }


def _should_skip_duplicate_archive_rule(
    rule: QualityRule,
    target: QualityTarget,
    csv_dimensions: set[tuple[str, str, str, str]],
) -> bool:
    if target.kind.value != "zip":
        return False
    if _target_dimension(target) not in csv_dimensions:
        return False
    return not str(rule.rule_id).startswith("inventory.")


def _target_dimension(target: QualityTarget) -> tuple[str, str, str, str]:
    return (
        str(target.data_format or "").strip().lower(),
        str(target.timeframe or "").strip().upper(),
        str(target.symbol or "").strip().upper(),
        str(target.period or "").strip(),
    )
