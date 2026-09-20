"""Resolve report values from retained results, never manually supplied metrics."""

from __future__ import annotations

import math
from dataclasses import dataclass
from fractions import Fraction

from .contracts import EvidenceKind, MetricDirection, ResultState
from .lineage import ExperimentRegistryV1, RegistryView
from .validation import ExperimentReportV1, ExperimentStatus


@dataclass(frozen=True, slots=True)
class ReportRowV1:
    """Process-local projection; authoritative durable input is the registry."""

    experiment_id: str
    result_id: str
    metric_id: str
    metric_name: str
    units: str
    stratum_id: str
    state: ResultState
    value: float | None
    support_units: int
    uncertainty_low: float | None
    uncertainty_high: float | None
    unavailable_reason: str | None
    current_status: ExperimentStatus | None
    evidence_kinds: tuple[EvidenceKind, ...]
    search_plan_id: str


@dataclass(frozen=True, slots=True)
class ComparisonRowV1:
    comparison_id: str
    left_result_id: str
    right_result_id: str
    metric_id: str
    stratum_id: str
    direction: MetricDirection
    declared_differences: tuple[str, ...]
    right_minus_left: float | None


@dataclass(frozen=True, slots=True)
class ResolvedReportV1:
    """No durable independent report-value copy; regenerate from retained IDs."""

    report_id: str
    registry_id: str
    title: str
    rows: tuple[ReportRowV1, ...]
    comparisons: tuple[ComparisonRowV1, ...]


def resolve_report(
    registry: ExperimentRegistryV1, report: ExperimentReportV1
) -> ResolvedReportV1:
    view = RegistryView(registry)
    view.validate()
    rows: list[ReportRowV1] = []
    comparisons: list[ComparisonRowV1] = []
    try:
        for result_id in report.result_ids:
            result = view.results[result_id]
            metric = view.metrics[result.metric_id]
            inputs = view.experiments[result.experiment_id].scientific_inputs
            kinds = tuple(
                sorted(
                    {ref.evidence_kind for ref in inputs.references()},
                    key=lambda value: value.value,
                )
            )
            rows.append(
                ReportRowV1(
                    result.experiment_id,
                    result_id,
                    result.metric_id,
                    metric.name,
                    metric.units,
                    result.stratum_id,
                    result.payload.state,
                    result.payload.value,
                    result.payload.support_units,
                    result.payload.uncertainty_low,
                    result.payload.uncertainty_high,
                    result.payload.unavailable_reason,
                    view.status(result.experiment_id),
                    kinds,
                    inputs.search_plan_id,
                )
            )
        for comparison_id in report.comparison_ids:
            comparison = view.comparisons[comparison_id]
            if (
                comparison.left_result_id not in report.result_ids
                or comparison.right_result_id not in report.result_ids
            ):
                raise ValueError(
                    "report comparison results absent from citations"
                )
            assert (
                comparison.left_result_id is not None
                and comparison.right_result_id is not None
            )
            a, b = (
                view.results[comparison.left_result_id],
                view.results[comparison.right_result_id],
            )
            delta = None
            if a.payload.value is not None and b.payload.value is not None:
                try:
                    exact = Fraction(b.payload.value) - Fraction(
                        a.payload.value
                    )
                    delta = float(exact)
                except OverflowError as exc:
                    raise ValueError(
                        "unrepresentable report difference"
                    ) from exc
                if not math.isfinite(delta) or (delta == 0 and exact != 0):
                    raise ValueError("unrepresentable report difference")
            comparisons.append(
                ComparisonRowV1(
                    comparison_id,
                    a.result_id,
                    b.result_id,
                    a.metric_id,
                    a.stratum_id,
                    view.metrics[a.metric_id].direction,
                    comparison.declared_differences,
                    delta,
                )
            )
    except KeyError as exc:
        raise ValueError("unresolved report result") from exc
    return ResolvedReportV1(
        report.artifact_id,
        registry.artifact_id,
        report.title,
        tuple(rows),
        tuple(comparisons),
    )


def render_report_markdown(
    registry: ExperimentRegistryV1, report: ExperimentReportV1
) -> str:
    """Deterministic human table with exact result IDs and explicit nonclaims."""
    resolved = resolve_report(registry, report)

    def escape(value: object) -> str:
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("|", "\\|")
            .replace("\n", " ")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    lines = [
        "# " + escape(resolved.title),
        "",
        "Declared interoperability only; no empirical qualification or historical-availability proof.",
        "",
        "Report: `" + resolved.report_id + "`",
        "Registry: `" + resolved.registry_id + "`",
        "",
        "| Result ID | Metric | Stratum | Value | Units | Support | Current status |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in resolved.rows:
        value = (
            repr(row.value)
            if row.value is not None
            else "unavailable: " + str(row.unavailable_reason)
        )
        lines.append(
            "| "
            + " | ".join(
                escape(v)
                for v in (
                    row.result_id,
                    row.metric_name,
                    row.stratum_id,
                    value,
                    row.units,
                    row.support_units,
                    (
                        row.current_status.value
                        if row.current_status
                        else "unknown"
                    ),
                )
            )
            + " |"
        )
    if resolved.comparisons:
        lines.extend(["", "## Exact retained comparisons", ""])
        for comparison_row in resolved.comparisons:
            lines.append(
                f"- `{comparison_row.comparison_id}`: right minus left = {comparison_row.right_minus_left!r}; direction `{comparison_row.direction.value}`; differences: "
                + ", ".join(
                    escape(v) for v in comparison_row.declared_differences
                )
                + "."
            )
    return "\n".join(lines) + "\n"
