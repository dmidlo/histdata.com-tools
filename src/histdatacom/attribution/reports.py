"""Recomputed descriptive distributions; never empirical/global causal claims."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import ClassVar

from ._wire import Artifact, Record, canonical_json, ordered, text
from .contracts import (
    AttributionEvidenceKind,
    AttributionState,
    CAUSAL_NONCLAIM,
    INDEPENDENCE_NONCLAIM,
    DecisionAttributionV1,
)
from .reference import _float


@dataclass(frozen=True, slots=True)
class AttributionStratumV1(Record):
    pair: str
    regime: str
    era: str
    domain: str
    support: str
    session: str
    broker_state: str

    def _validate(self) -> None:
        for value in (
            self.pair,
            self.regime,
            self.era,
            self.domain,
            self.support,
            self.session,
            self.broker_state,
        ):
            text(value)


@dataclass(frozen=True, slots=True)
class StratifiedAttributionV1(Record):
    attribution: DecisionAttributionV1
    stratum: AttributionStratumV1
    declared_evidence_unit: str

    def _validate(self) -> None:
        text(self.declared_evidence_unit)
        if (
            self.stratum.pair not in self.attribution.snapshot.universe
            or self.stratum.session != self.attribution.snapshot.session
            or self.stratum.domain != self.attribution.snapshot.domain
        ):
            raise ValueError("report stratum differs from retained snapshot")


@dataclass(frozen=True, slots=True)
class GroupImportanceV1(Record):
    group: str
    mean_absolute: float
    median_absolute: float
    signed_distribution: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class AttributionStratumReportV1(Record):
    stratum: AttributionStratumV1
    available_explanations: int
    unavailable_explanations: int
    unverified_explanations: int
    nonidentified_explanations: int
    declared_distinct_units: int
    groups: tuple[GroupImportanceV1, ...]
    hhi: float | None
    effective_contributing_groups: float | None


@dataclass(frozen=True, slots=True)
class AttributionStratumDifferenceV1(Record):
    left_stratum: AttributionStratumV1
    right_stratum: AttributionStratumV1
    group: str
    mean_absolute_difference: float
    background_changed: bool


def _summary(
    inputs: tuple[StratifiedAttributionV1, ...],
) -> tuple[
    tuple[AttributionStratumReportV1, ...],
    tuple[AttributionStratumDifferenceV1, ...],
]:
    if not inputs or len(inputs) > 256:
        raise ValueError("report requires1..256 retained explanation inputs")
    if len({item.attribution.artifact_id for item in inputs}) != len(inputs):
        raise ValueError(
            "duplicate attribution cannot manufacture report support"
        )
    stack = {
        (
            item.attribution.policy.artifact_id,
            canonical_json(item.attribution.model_reference),
            item.attribution.snapshot.space,
            canonical_json(item.attribution.snapshot.preprocessing),
            canonical_json(item.attribution.snapshot.projection),
            canonical_json(
                tuple(v.feature for v in item.attribution.snapshot.values)
            ),
            canonical_json(item.attribution.explained_output),
        )
        for item in inputs
    }
    if len(stack) != 1:
        raise ValueError(
            "report must not pool different model/explainer/preprocessing semantics"
        )
    blocks: dict[str, list[StratifiedAttributionV1]] = {}
    for item in inputs:
        blocks.setdefault(canonical_json(item.stratum), []).append(item)
    summaries = []
    for key, items in sorted(blocks.items()):
        del key
        if len({i.attribution.background.artifact_id for i in items}) != 1:
            raise ValueError(
                "one stratum cannot pool different reference backgrounds"
            )
        selected = [
            item.attribution
            for item in items
            if item.attribution.evidence_kind is AttributionEvidenceKind.FIXTURE
            and item.attribution.state
            in (AttributionState.IDENTIFIED, AttributionState.NONIDENTIFIABLE)
        ]
        groups = []
        masses = []
        for name in (
            inputs[0].attribution.policy.reporting_cut if selected else ()
        ):
            values = tuple(
                sorted(
                    next(c.value for c in a.groups if c.name == name)
                    for a in selected
                )
            )
            absolute = sorted(Fraction(abs(v)) for v in values)
            mean = sum(absolute, Fraction()) / len(absolute)
            middle = len(absolute) // 2
            median = (
                absolute[middle]
                if len(absolute) % 2
                else (absolute[middle - 1] + absolute[middle]) / 2
            )
            groups.append(
                GroupImportanceV1(name, _float(mean), _float(median), values)
            )
            masses.append(mean)
        total = sum(masses, Fraction())
        hhi = (
            sum((m * m for m in masses), Fraction()) / (total * total)
            if total
            else None
        )
        summaries.append(
            AttributionStratumReportV1(
                items[0].stratum,
                len(selected),
                sum(
                    i.attribution.state is AttributionState.UNSUPPORTED
                    for i in items
                ),
                sum(
                    i.attribution.evidence_kind
                    is AttributionEvidenceKind.DECLARED
                    for i in items
                ),
                sum(
                    a.state is AttributionState.NONIDENTIFIABLE
                    for a in selected
                ),
                len({i.declared_evidence_unit for i in items}),
                tuple(groups),
                None if hhi is None else _float(hhi),
                None if hhi is None else _float(1 / hhi),
            )
        )
    differences: list[AttributionStratumDifferenceV1] = []
    # Retain ALL stratum contrasts, not a result-selected favorable pair. This
    # includes era/regime/domain drift and observed-vs-synthetic labelled cells.
    # Labels remain declarations; synthetic conformance is not a live monitor.
    for i, left in enumerate(summaries):
        for right in summaries[i + 1 :]:
            if len(differences) + len(left.groups) > 4096:
                raise ValueError("stratum comparison inventory exceeds bound")
            if tuple(g.group for g in left.groups) != tuple(
                g.group for g in right.groups
            ):
                continue
            for a, b in zip(left.groups, right.groups):
                differences.append(
                    AttributionStratumDifferenceV1(
                        left.stratum,
                        right.stratum,
                        a.group,
                        _float(
                            Fraction(b.mean_absolute)
                            - Fraction(a.mean_absolute)
                        ),
                        next(
                            i.attribution.background.artifact_id
                            for i in inputs
                            if i.stratum == left.stratum
                        )
                        != next(
                            i.attribution.background.artifact_id
                            for i in inputs
                            if i.stratum == right.stratum
                        ),
                    )
                )
    return tuple(summaries), tuple(differences)


@dataclass(frozen=True, slots=True)
class AttributionReportV1(Artifact):
    KIND: ClassVar[str] = "report"
    inputs: tuple[StratifiedAttributionV1, ...]
    summaries: tuple[AttributionStratumReportV1, ...]
    differences: tuple[AttributionStratumDifferenceV1, ...]
    causal_nonclaim: str = CAUSAL_NONCLAIM
    independence_nonclaim: str = INDEPENDENCE_NONCLAIM

    def _validate(self) -> None:
        if (
            self.causal_nonclaim != CAUSAL_NONCLAIM
            or self.independence_nonclaim != INDEPENDENCE_NONCLAIM
        ):
            raise ValueError("report cannot claim causal effects/independence")
        ordered(
            tuple(item.attribution.artifact_id for item in self.inputs),
            nonempty=True,
        )
        if (self.summaries, self.differences) != _summary(self.inputs):
            raise ValueError("report differs from retained contribution replay")


def build_attribution_report(
    inputs: tuple[StratifiedAttributionV1, ...],
) -> AttributionReportV1:
    inputs = tuple(
        sorted(inputs, key=lambda item: item.attribution.artifact_id)
    )
    summaries, differences = _summary(inputs)
    return AttributionReportV1(inputs, summaries, differences)


def render_attribution_report(report: AttributionReportV1) -> str:
    report = AttributionReportV1.from_json(report.to_json())
    lines = [
        "# Public attribution accounting",
        "",
        CAUSAL_NONCLAIM,
        INDEPENDENCE_NONCLAIM,
        "Declared strata and unit labels; synthetic fixtures/unverified references are not historical or live qualification.",
        "",
    ]
    for item in report.inputs:
        lines.append(
            f"{item.attribution.artifact_id}: nominal selected features={len(item.attribution.snapshot.values)}, background Pearson effective dimension={item.attribution.effective_feature_dimension}"
        )
    for summary in report.summaries:
        lines.append(canonical_json(summary.stratum))
        lines.append(
            f"Available={summary.available_explanations}; unavailable={summary.unavailable_explanations}; unverified={summary.unverified_explanations}; nonidentified={summary.nonidentified_explanations}"
        )
        lines.append(
            f"Group HHI={summary.hhi}; effective contributing groups={summary.effective_contributing_groups}"
        )
        # Names, never an unstable singleton ranking.
        for group in summary.groups:
            lines.append(
                f"{group.group}: mean absolute={group.mean_absolute}, median absolute={group.median_absolute}, signed={group.signed_distribution}"
            )
    return "\n".join(lines) + "\n"
