"""Rule orchestration for HistData data-quality assessments."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityRunRule,
    QualityTarget,
)
from histdatacom.data_quality.ticks import (
    can_evaluate_tick_quality_bundle,
    evaluate_tick_quality_bundle,
)
from histdatacom.runtime_contracts import JSONValue

QualityProgressCallback = Callable[[Mapping[str, JSONValue]], None]


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
    progress_callback: QualityProgressCallback | None = None,
) -> QualityReport:
    """Run every rule against every target through one orchestration path."""
    target_tuple = tuple(targets)
    rule_tuple = tuple(rules)
    run_rule_tuple = tuple(run_rules)
    base_metadata = dict(metadata or {})
    csv_dimensions = _csv_target_dimensions(target_tuple)
    skipped_duplicate_archive_rule_count = 0
    evaluation_plan: list[
        tuple[int, QualityTarget, tuple[tuple[int, QualityRule], ...]]
    ] = []
    for target_index, target in enumerate(target_tuple, start=1):
        rule_offset = 0
        while rule_offset < len(rule_tuple):
            rule_index = rule_offset + 1
            rule = rule_tuple[rule_offset]
            if _should_skip_duplicate_archive_rule(
                rule,
                target,
                csv_dimensions,
            ):
                skipped_duplicate_archive_rule_count += 1
                rule_offset += 1
                continue
            bundle_candidates = rule_tuple[rule_offset : rule_offset + 3]
            if can_evaluate_tick_quality_bundle(target, bundle_candidates):
                evaluation_plan.append(
                    (
                        target_index,
                        target,
                        tuple(
                            (rule_offset + offset + 1, bundle_rule)
                            for offset, bundle_rule in enumerate(
                                bundle_candidates
                            )
                        ),
                    )
                )
                rule_offset += len(bundle_candidates)
                continue
            evaluation_plan.append(
                (target_index, target, ((rule_index, rule),))
            )
            rule_offset += 1

    total_evaluation_count = sum(
        len(rule_group) for _, _, rule_group in evaluation_plan
    ) + len(run_rule_tuple)
    _emit_quality_progress(
        progress_callback,
        phase="start",
        completed=0,
        total=total_evaluation_count,
        target_count=len(target_tuple),
        rule_count=len(rule_tuple),
        run_rule_count=len(run_rule_tuple),
    )

    rule_results_list: list[QualityRuleResult] = []
    completed = 0
    for (
        target_index,
        target,
        rule_group,
    ) in evaluation_plan:
        group_rules = tuple(rule for _, rule in rule_group)
        if can_evaluate_tick_quality_bundle(target, group_rules):
            first_rule_index, first_rule = rule_group[0]
            _emit_quality_progress(
                progress_callback,
                phase="rule_start",
                completed=completed,
                total=total_evaluation_count,
                target_count=len(target_tuple),
                rule_count=len(rule_tuple),
                run_rule_count=len(run_rule_tuple),
                target_index=target_index,
                rule_index=first_rule_index,
                rule_id=first_rule.rule_id,
                target=target,
            )
            bundle_results = evaluate_tick_quality_bundle(target, group_rules)
            for result_index, (
                (rule_index, rule),
                result,
            ) in enumerate(zip(rule_group, bundle_results, strict=True)):
                if result_index:
                    _emit_quality_progress(
                        progress_callback,
                        phase="rule_start",
                        completed=completed,
                        total=total_evaluation_count,
                        target_count=len(target_tuple),
                        rule_count=len(rule_tuple),
                        run_rule_count=len(run_rule_tuple),
                        target_index=target_index,
                        rule_index=rule_index,
                        rule_id=rule.rule_id,
                        target=target,
                    )
                rule_results_list.append(result)
                completed += 1
                _emit_quality_progress(
                    progress_callback,
                    phase="rule_complete",
                    completed=completed,
                    total=total_evaluation_count,
                    target_count=len(target_tuple),
                    rule_count=len(rule_tuple),
                    run_rule_count=len(run_rule_tuple),
                    target_index=target_index,
                    rule_index=rule_index,
                    rule_id=rule.rule_id,
                    target=target,
                )
            continue

        rule_index, rule = rule_group[0]
        _emit_quality_progress(
            progress_callback,
            phase="rule_start",
            completed=completed,
            total=total_evaluation_count,
            target_count=len(target_tuple),
            rule_count=len(rule_tuple),
            run_rule_count=len(run_rule_tuple),
            target_index=target_index,
            rule_index=rule_index,
            rule_id=rule.rule_id,
            target=target,
        )
        rule_results_list.append(evaluate_quality_rule(rule, target))
        completed += 1
        _emit_quality_progress(
            progress_callback,
            phase="rule_complete",
            completed=completed,
            total=total_evaluation_count,
            target_count=len(target_tuple),
            rule_count=len(rule_tuple),
            run_rule_count=len(run_rule_tuple),
            target_index=target_index,
            rule_index=rule_index,
            rule_id=rule.rule_id,
            target=target,
        )

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
    run_reports_list: list[QualityReport] = []
    for run_rule_index, run_rule in enumerate(run_rule_tuple, start=1):
        _emit_quality_progress(
            progress_callback,
            phase="run_rule_start",
            completed=completed,
            total=total_evaluation_count,
            target_count=len(target_tuple),
            rule_count=len(rule_tuple),
            run_rule_count=len(run_rule_tuple),
            run_rule_index=run_rule_index,
            rule_id=run_rule.rule_id,
        )
        run_reports_list.append(
            run_rule.evaluate_run(target_tuple, metadata=base_metadata)
        )
        completed += 1
        _emit_quality_progress(
            progress_callback,
            phase="run_rule_complete",
            completed=completed,
            total=total_evaluation_count,
            target_count=len(target_tuple),
            rule_count=len(rule_tuple),
            run_rule_count=len(run_rule_tuple),
            run_rule_index=run_rule_index,
            rule_id=run_rule.rule_id,
        )
    run_reports = tuple(run_reports_list)
    merged_metadata = dict(base_metadata)
    for report in run_reports:
        merged_metadata.update(report.metadata)

    _emit_quality_progress(
        progress_callback,
        phase="complete",
        completed=total_evaluation_count,
        total=total_evaluation_count,
        target_count=len(target_tuple),
        rule_count=len(rule_tuple),
        run_rule_count=len(run_rule_tuple),
    )

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


def _emit_quality_progress(
    callback: QualityProgressCallback | None,
    *,
    phase: str,
    completed: int,
    total: int,
    target_count: int,
    rule_count: int,
    run_rule_count: int,
    target_index: int = 0,
    rule_index: int = 0,
    run_rule_index: int = 0,
    rule_id: str = "",
    target: QualityTarget | None = None,
) -> None:
    """Emit bounded, publish-safe progress metadata for long quality scans."""
    if callback is None:
        return

    payload: dict[str, JSONValue] = {
        "event_type": "progress",
        "stage": "data_quality",
        "phase": phase,
        "completed": completed,
        "total": total,
        "unit": "quality_rule_evaluations",
        "target_count": target_count,
        "target_index": target_index,
        "rule_count": rule_count,
        "rule_index": rule_index,
        "run_rule_count": run_rule_count,
        "run_rule_index": run_rule_index,
        "rule_id": str(rule_id or ""),
    }
    if target is not None:
        payload.update(
            {
                "target_kind": target.kind.value,
                "target_format": str(target.data_format or ""),
                "target_timeframe": str(target.timeframe or ""),
                "target_symbol": str(target.symbol or ""),
                "target_period": str(target.period or ""),
            }
        )
    callback(payload)
