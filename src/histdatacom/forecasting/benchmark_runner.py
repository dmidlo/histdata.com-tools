"""Executed benchmark fitting, predictions, refusals and complete replay."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field, replace
from decimal import Decimal, localcontext
from typing import Any, Mapping, cast

from histdatacom.runtime_contracts import JSONValue

from .benchmark_cases import (
    ForecastBenchmarkEvidenceV1,
    build_forecast_benchmark_evidence,
)
from .benchmark_contracts import (
    BenchmarkComparator,
    ForecastBenchmarkSuiteV1,
    QUALIFICATION,
    benchmark_code_bindings,
)
from .contracts import (
    SECOND_NS,
    ConsensusTiming,
    ForecastDistributionV1,
    ForecastTargetKind,
    PointStatistic,
    _verify,
)
from .feature_contracts import FeatureCellStatus
from .feature_forecasts import (
    ForecastFeatureInputsV1,
    ForecastFeatureModelV1,
    ForecastFeatureScoreV1,
    ForecastFeatureSnapshotV1,
)
from .math_reference import number
from .math_reports import ForecastMathCheckedReportV1
from .math_verification import (
    ForecastMathVerificationV1,
    math_json,
    math_load,
    math_seal,
)

COMBINATION_FORMULA_VERSION = "1.0.0"


class BenchmarkRefusal(ValueError):
    """A specific source-supported refusal; never wraps unexpected errors."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _preflight(
    evidence: ForecastBenchmarkEvidenceV1,
    inputs: ForecastFeatureInputsV1,
    *,
    require_consensus: bool = True,
) -> tuple[float, ...]:
    releases = inputs.calendar_inputs.calendar.releases
    if not any(
        r.release_id == evidence.cutoff.schedule_release_id for r in releases
    ):
        raise BenchmarkRefusal("schedule-unavailable")
    schedule = next(
        r
        for r in releases
        if r.release_id == evidence.cutoff.schedule_release_id
    )
    if (schedule.unit, schedule.scale, schedule.base) != (
        evidence.target.unit,
        evidence.target.scale,
        evidence.target.base,
    ):
        raise BenchmarkRefusal("semantic-mismatch")
    cells = inputs.features.column("history")
    if (
        any(c.status is FeatureCellStatus.UNAVAILABLE for c in cells)
        or len(cells) < 3
    ):
        raise BenchmarkRefusal("insufficient-support")
    if any(
        c.status is not FeatureCellStatus.AVAILABLE or c.value is None
        for c in cells
    ):
        raise BenchmarkRefusal("missing-feature")
    definitions = {
        o.definition.semantic_id: o.definition
        for o in inputs.features.observations
    }
    if len(definitions) != 1:
        raise BenchmarkRefusal("semantic-mismatch")
    definition = next(iter(definitions.values()))
    if (
        definition.unit,
        definition.scale,
        definition.base,
        definition.frequency,
    ) != (
        evidence.target.unit,
        evidence.target.scale,
        evidence.target.base,
        schedule.frequency,
    ):
        raise BenchmarkRefusal("semantic-mismatch")
    if require_consensus and evidence.target.consensus_reference is not None:
        reference = replace(
            evidence.target.consensus_reference,
            target_at_ns=evidence.cutoff.cutoff_at_ns,
            timing=ConsensusTiming.AT_CUTOFF,
        )
        if not any(
            f.logical_event_key == evidence.target.logical_event_key
            and f.source.name == reference.source_name
            and f.source.adapter_name == reference.adapter_name
            and f.scope is reference.scope
            and f.statistic is reference.statistic
            for f in inputs.calendar_inputs.calendar.forecasts
        ):
            raise BenchmarkRefusal("consensus-unavailable")
        reference.select(
            inputs.calendar_inputs.calendar, evidence.target.logical_event_key
        )
    return tuple(number(c.value) for c in cells)


def _estimate(key: BenchmarkComparator, values: tuple[float, ...]) -> float:
    try:
        if key in (
            BenchmarkComparator.HISTORICAL_MEAN,
            BenchmarkComparator.FORECAST_MEAN,
        ):
            return number(statistics.fmean(values))
        if key in (
            BenchmarkComparator.HISTORICAL_MEDIAN,
            BenchmarkComparator.FORECAST_MEDIAN,
        ):
            return number(statistics.median(values))
        return number(values[-1])
    except OverflowError as exc:
        raise ValueError("benchmark arithmetic is not representable") from exc


def combination_reference(
    members: tuple[ForecastFeatureSnapshotV1, ...],
    comparator: BenchmarkComparator,
) -> float:
    """Independent decimal oracle over actual component distributions' points."""
    if (
        comparator
        not in (
            BenchmarkComparator.FORECAST_MEAN,
            BenchmarkComparator.FORECAST_MEDIAN,
        )
        or type(members) is not tuple
        or not 2 <= len(members) <= 8
    ):
        raise ValueError(
            "combination requires bounded full component forecasts"
        )
    base = members[0]
    if len({m.snapshot_id for m in members}) != len(members):
        raise ValueError("duplicate component forecast")
    for member in members:
        if (
            member.target,
            member.cutoff,
            member.inputs,
            member.generated_at_ns,
        ) != (base.target, base.cutoff, base.inputs, base.generated_at_ns):
            raise ValueError(
                "combination component task, source or cutoff differs"
            )
    # Recompute each point directly from the retained support, not .point.
    with localcontext() as context:
        context.prec = 50
        points = []
        for member in members:
            support = member.distribution.support
            if member.distribution.point_statistic is PointStatistic.MEAN:
                point = sum(
                    (Decimal(str(v)) * Decimal(str(p)) for v, p in support),
                    Decimal(0),
                )
            else:
                cumulative = Decimal(0)
                point = Decimal(str(support[-1][0]))
                for value, mass in support:
                    cumulative += Decimal(str(mass))
                    if cumulative >= Decimal("0.5"):
                        point = Decimal(str(value))
                        break
            points.append(point)
        points.sort()
        if comparator is BenchmarkComparator.FORECAST_MEAN:
            expected = sum(points, Decimal(0)) / Decimal(len(points))
        else:
            mid = len(points) // 2
            expected = (
                points[mid]
                if len(points) % 2
                else (points[mid - 1] + points[mid]) / 2
            )
    return number(float(expected))


def execute_benchmark_prediction(
    evidence: ForecastBenchmarkEvidenceV1, comparator: BenchmarkComparator
) -> tuple[ForecastFeatureSnapshotV1, tuple[ForecastFeatureSnapshotV1, ...]]:
    """Run a closed reference algorithm; arbitrary metadata cannot name code.

    This returns a prediction, not a suite certification. Complete suite replay
    re-executes it and independently checks the prediction and realized target.
    """
    if (
        type(evidence) is not ForecastBenchmarkEvidenceV1
        or type(comparator) is not BenchmarkComparator
    ):
        raise TypeError(
            "benchmark execution requires typed evidence/comparator"
        )
    cutoff = evidence.cutoff.cutoff_at_ns
    training = evidence.inputs(cutoff - SECOND_NS)
    inputs = evidence.inputs(cutoff)
    training_values = _preflight(evidence, training, require_consensus=False)
    current_values = _preflight(evidence, inputs)
    members: tuple[ForecastFeatureSnapshotV1, ...] = ()
    model_training = training
    trained_at = cutoff - SECOND_NS
    state: dict[str, Any] = {
        "recipe": comparator.value,
        "target_kind": evidence.target.kind.value,
        "horizon": evidence.cutoff.horizon.value,
        "training_values": list(training_values),
        "column": "history",
        "member_ids": [],
        "qualification": QUALIFICATION,
    }
    if comparator in (
        BenchmarkComparator.FORECAST_MEAN,
        BenchmarkComparator.FORECAST_MEDIAN,
    ):
        third = (
            BenchmarkComparator.CONSENSUS_PERSISTENCE
            if evidence.target.kind is ForecastTargetKind.CONSENSUS
            else BenchmarkComparator.LAST_VALUE
        )
        members = tuple(
            execute_benchmark_prediction(evidence, key)[0]
            for key in (
                BenchmarkComparator.HISTORICAL_MEAN,
                BenchmarkComparator.HISTORICAL_MEDIAN,
                third,
            )
        )
        point = _estimate(
            comparator, tuple(m.distribution.point for m in members)
        )
        reference = combination_reference(members, comparator)
        if not math.isclose(point, reference, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError(
                "combination differs from independent decimal oracle"
            )
        state["member_ids"] = [m.snapshot_id for m in members]
        # Combination members exist at generation, not at the earlier fit.
        model_training = inputs
        trained_at = cutoff
        state["training_values"] = list(current_values)
        state["combination_formula_version"] = COMBINATION_FORMULA_VERSION
        state["member_count_is_not_independence"] = True
    elif comparator is BenchmarkComparator.CONSENSUS_PERSISTENCE:
        if (
            evidence.target.kind is not ForecastTargetKind.CONSENSUS
            or evidence.target.consensus_reference is None
        ):
            raise ValueError(
                "consensus persistence requires its separate target"
            )
        reference_at_cutoff = replace(
            evidence.target.consensus_reference,
            target_at_ns=cutoff,
            timing=ConsensusTiming.AT_CUTOFF,
        )
        survey = reference_at_cutoff.select(
            inputs.calendar_inputs.calendar, evidence.target.logical_event_key
        )
        point = number(survey.value)
        # Observable copying has no earlier statistical fit; retain the actual
        # information set when the selected survey is available.
        model_training = inputs
        trained_at = cutoff
        state["training_values"] = list(current_values)
        state["selected_consensus_id"] = survey.forecast_id
        state["interpretation"] = (
            "observable-reference-copy-not-predictive-skill"
            if evidence.target.consensus_reference.timing
            is ConsensusTiming.AT_CUTOFF
            else "last-known-consensus-persistence-for-explicit-future-target"
        )
    else:
        point = _estimate(
            comparator,
            (
                current_values
                if comparator is BenchmarkComparator.LAST_VALUE
                else training_values
            ),
        )
    # Fitted parameters never include outcome values or a future survey.
    state["fit_estimate"] = (
        _estimate(comparator, training_values)
        if comparator
        in (
            BenchmarkComparator.HISTORICAL_MEAN,
            BenchmarkComparator.HISTORICAL_MEDIAN,
        )
        else None
    )
    model = ForecastFeatureModelV1(
        f"benchmark-{comparator.value}-{evidence.target.kind.value}",
        "1.0.0",
        benchmark_code_bindings()["benchmark_runner.py"],
        math_json(state),
        model_training,
        trained_at,
    )
    snapshot = ForecastFeatureSnapshotV1(
        evidence.cutoff,
        evidence.target,
        inputs,
        model,
        ForecastDistributionV1(((point, 1.0),)),
        cutoff,
    )
    return snapshot, members


def _execute(suite: ForecastBenchmarkSuiteV1) -> list[dict[str, Any]]:
    receipt = ForecastMathVerificationV1.current()
    results: list[dict[str, Any]] = []
    retained_bytes = len(suite.to_json())
    for case in suite.cases:
        evidence = build_forecast_benchmark_evidence(suite, case)
        for comparator in case.comparators:
            try:
                snapshot, members = execute_benchmark_prediction(
                    evidence, comparator
                )
            except BenchmarkRefusal as exc:
                if exc.code != case.expected_refusal:
                    raise ValueError("unexpected benchmark refusal") from exc
                row: dict[str, Any] = {
                    "case_id": case.case_id,
                    "comparator": comparator.value,
                    "status": "refused",
                    "reason": exc.code,
                    "inputs": evidence.inputs(
                        evidence.cutoff.cutoff_at_ns
                    ).to_dict(),
                    "target": evidence.target.to_dict(),
                    "cutoff": evidence.cutoff.to_dict(),
                    "outcomes": evidence.corpus.to_dict(),
                    "checked_report": None,
                    "members": [],
                    "combination_check": None,
                }
            else:
                if case.expected_refusal is not None:
                    raise ValueError("expected benchmark refusal did not occur")
                expected = dict(case.expected_points)[comparator]
                if not math.isclose(
                    snapshot.distribution.point,
                    expected,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                ):
                    raise ValueError(
                        "executed benchmark differs from frozen point oracle"
                    )
                score = ForecastFeatureScoreV1(
                    snapshot,
                    evidence.corpus.to_json(),
                    evidence.corpus.coverage_end_ns,
                )
                if score.metrics["observed_target"] != case.expected_target:
                    raise ValueError(
                        "benchmark realized target differs from frozen oracle"
                    )
                checked = ForecastMathCheckedReportV1(score.to_json(), receipt)
                row = {
                    "case_id": case.case_id,
                    "comparator": comparator.value,
                    "status": "scored",
                    "reason": None,
                    "checked_report": checked.to_dict(),
                    "members": [m.to_dict() for m in members],
                    "combination_check": (
                        None
                        if not members
                        else {
                            "formula_version": COMBINATION_FORMULA_VERSION,
                            "formula": (
                                "arithmetic mean of component point forecasts"
                                if comparator
                                is BenchmarkComparator.FORECAST_MEAN
                                else "sample median of component point forecasts; even midpoint"
                            ),
                            "independent_decimal_reference": combination_reference(
                                members, comparator
                            ),
                            "actual_point": snapshot.distribution.point,
                            "independent_model_count": None,
                        }
                    ),
                }
            retained_bytes += len(math_json(row)) + 1
            if retained_bytes > 7_500_000:
                raise ValueError(
                    "benchmark execution exceeds retained evidence bound"
                )
            results.append(row)
    return results


@dataclass(frozen=True, slots=True)
class ForecastBenchmarkRunV1:
    """Construction executes; restoration re-executes exact full coverage.

    Serialized bytes cache immutable completed evidence, not authorization to
    skip replay. Persisted readers and writers always construct a fresh run.
    """

    suite: ForecastBenchmarkSuiteV1
    _sealed_json: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.suite) is not ForecastBenchmarkSuiteV1:
            raise TypeError("benchmark run requires a complete suite")
        bindings = benchmark_code_bindings()
        results = _execute(self.suite)
        if bindings != benchmark_code_bindings():
            raise ValueError("benchmark executable changed during execution")
        coverage = [
            [c.case_id, k.value]
            for c in self.suite.cases
            for k in c.comparators
        ]
        if coverage != [[r["case_id"], r["comparator"]] for r in results]:
            raise ValueError("benchmark execution coverage differs")
        payload = math_seal(
            "forecast-benchmark-run",
            {
                "suite": self.suite.to_dict(),
                "code_bindings": bindings,
                "coverage": coverage,
                "results": results,
                "scored_count": sum(r["status"] == "scored" for r in results),
                "refused_count": sum(r["status"] == "refused" for r in results),
                "qualification": QUALIFICATION,
            },
        )
        object.__setattr__(self, "_sealed_json", math_json(payload))

    @property
    def run_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return cast(dict[str, JSONValue], dict(math_load(self._sealed_json)))

    def to_json(self) -> str:
        return self._sealed_json

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastBenchmarkRunV1:
        math_json(data)
        suite = ForecastBenchmarkSuiteV1.from_dict(data["suite"])
        coverage = [
            [c.case_id, k.value] for c in suite.cases for k in c.comparators
        ]
        if (
            data.get("coverage") != coverage
            or type(data.get("results")) is not list
            or len(data["results"]) != len(coverage)
            or [
                [r.get("case_id"), r.get("comparator")]
                for r in data["results"]
                if type(r) is dict
            ]
            != coverage
        ):
            raise ValueError(
                "missing, extra, duplicate or reordered benchmark coverage"
            )
        if data.get("code_bindings") != benchmark_code_bindings():
            raise ValueError("benchmark executable binding differs")
        result = cls(suite)
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastBenchmarkRunV1:
        return cls.from_dict(math_load(text))
