"""Additive checked evidence envelopes; legacy forecast identities stay intact.

Complete feature/engine subjects remain embedded. Calendar projection is only
an internal semantic adapter, never a substitute for the source envelope.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from histdatacom.market_context.economic_calendar import (
    EconomicForecastKind,
    EconomicReleaseStage,
)
from histdatacom.runtime_contracts import JSONValue

from . import math_reference as reference
from .contracts import (
    ConsensusTiming,
    ForecastTargetKind,
    RevisionMeasure,
    SurpriseReference,
    _verify,
)
from .engine_runner import ForecastEngineComparisonV1, ForecastEngineScoreV1
from .feature_forecasts import ForecastFeatureScoreV1
from .math_verification import (
    FORMULAS,
    ForecastMathVerificationV1,
    MAX_MATH_ARTIFACT_BYTES,
    math_json,
    math_load,
    math_seal,
)
from .scoring import ForecastScoreReportV1, ForecastScoreV1, _snapshot_task_key

ForecastMathSubject = (
    ForecastScoreV1
    | ForecastScoreReportV1
    | ForecastFeatureScoreV1
    | ForecastEngineScoreV1
    | ForecastEngineComparisonV1
)
ForecastMathScore = (
    ForecastScoreV1 | ForecastFeatureScoreV1 | ForecastEngineScoreV1
)
MAX_CHECKED_SCORES = 128


def _restore_subject(text: str) -> ForecastMathSubject:
    data = math_load(text)
    if "scores" in data and (
        type(data["scores"]) is not list
        or not 1 <= len(data["scores"]) <= MAX_CHECKED_SCORES
    ):
        raise ValueError("checked score collection exceeds bounds")
    kind = data.get("contract_type")
    if kind == "forecast-score":
        return ForecastScoreV1.from_json(text)
    if kind == "forecast-score-report":
        return ForecastScoreReportV1.from_json(text)
    if kind == "forecast-feature-score":
        return ForecastFeatureScoreV1.from_json(text)
    if kind == "forecast-engine-score":
        return ForecastEngineScoreV1.from_json(text)
    if kind == "forecast-engine-comparison":
        return ForecastEngineComparisonV1.from_json(text)
    raise ValueError(
        "math checking requires a complete supported scored subject"
    )


def _calendar(score: ForecastMathScore) -> ForecastScoreV1:
    if isinstance(score, ForecastEngineScoreV1):
        return score.feature_score._calendar_score()
    if isinstance(score, ForecastFeatureScoreV1):
        return score._calendar_score()
    return score


def _rows(subject: ForecastMathSubject) -> tuple[ForecastMathScore, ...]:
    if isinstance(subject, (ForecastScoreReportV1, ForecastEngineComparisonV1)):
        return subject.scores
    return (subject,)


def _homogeneous_key(score: ForecastScoreV1) -> tuple[object, ...]:
    snapshot = score.snapshot
    initial = tuple(
        release
        for release in score.outcome_calendar.releases
        if release.logical_event_key == snapshot.target.logical_event_key
        and release.actual_value is not None
        and release.stage is not EconomicReleaseStage.REVISION
    )
    if len(initial) != 1:
        raise ValueError("homogeneous comparison requires one first release")
    release = initial[0]
    return (
        _snapshot_task_key(snapshot),
        snapshot.target.indicator_id,
        math_json(release.series_identity_payload()),
        release.source.name,
        release.source.adapter_name,
        release.source.adapter_version,
    )


def _same_number(actual: object, expected: float | None, name: str) -> None:
    if expected is None:
        if actual is not None:
            raise ValueError(f"independent score differs: {name}")
    elif not math.isclose(
        reference.number(actual), expected, rel_tol=1e-12, abs_tol=1e-12
    ):
        raise ValueError(f"independent score differs: {name}")


def _oracle(score: ForecastScoreV1) -> dict[str, Any]:
    """Select outcome/consensus independently from immutable vintage records."""
    snapshot, corpus = score.snapshot, score.outcome_calendar
    target = snapshot.target
    point = reference.distribution_point(
        snapshot.distribution.support,
        snapshot.distribution.point_statistic.value,
    )
    actuals = tuple(
        release
        for release in corpus.releases
        if release.logical_event_key == target.logical_event_key
        and release.actual_value is not None
        and release.stage is not EconomicReleaseStage.REVISION
    )
    if len(actuals) > 1 or (
        not actuals and target.kind is not ForecastTargetKind.CONSENSUS
    ):
        raise ValueError("math oracle requires unique first-release evidence")
    initial = actuals[0] if actuals else None
    value = None if initial is None else initial.actual_value
    reference_value, reference_id = None, None
    named = target.consensus_reference
    if named is not None:
        history = (
            corpus
            if named.timing is ConsensusTiming.FUTURE_EVOLUTION
            else snapshot.inputs.calendar
        )
        choices = tuple(
            item
            for item in history.forecasts
            if item.logical_event_key == target.logical_event_key
            and item.kind is EconomicForecastKind.OBSERVED_CONSENSUS
            and item.scope is named.scope
            and item.statistic is named.statistic
            and item.source.name == named.source_name
            and item.source.adapter_name == named.adapter_name
            and item.available_at_ns <= named.target_at_ns
        )
        if not choices:
            raise ValueError("math oracle lacks exact observed consensus")
        selected = sorted(
            choices, key=lambda item: item.available_at_ns, reverse=True
        )
        if (
            len(selected) > 1
            and selected[0].available_at_ns == selected[1].available_at_ns
        ):
            raise ValueError("math oracle consensus clock is ambiguous")
        reference_value, reference_id = (
            selected[0].value,
            selected[0].forecast_id,
        )
    revision_id = None
    if target.kind is ForecastTargetKind.CONSENSUS:
        value = reference_value
    elif target.kind is ForecastTargetKind.REVISION:
        revisions = tuple(
            item
            for item in corpus.releases
            if item.logical_event_key == target.logical_event_key
            and item.stage is EconomicReleaseStage.REVISION
            and item.revision_sequence == target.revision_sequence
            and item.actual_value is not None
        )
        if len(revisions) != 1:
            raise ValueError("math oracle lacks exact requested revision")
        value, revision_id = revisions[0].actual_value, revisions[0].release_id
        if target.revision_measure is RevisionMeasure.CHANGE_FROM_FIRST:
            if initial is None or initial.actual_value is None or value is None:
                raise ValueError("math oracle revision lacks initial support")
            value = reference.number(value - initial.actual_value)
    elif target.kind is ForecastTargetKind.SURPRISE:
        if target.surprise_reference is SurpriseReference.PREDICTED_CONSENSUS:
            predicted = snapshot.predicted_consensus
            if predicted is None:
                raise ValueError("math oracle lacks predicted consensus")
            reference_value = reference.distribution_point(
                predicted.distribution.support,
                predicted.distribution.point_statistic.value,
            )
        if value is None or reference_value is None:
            raise ValueError("math oracle surprise lacks reference evidence")
        value = reference.number(value - reference_value)
    observed = reference.number(value)
    losses = reference.point_losses(
        point,
        observed,
        consensus=(
            reference_value
            if target.kind is ForecastTargetKind.FIRST_RELEASE_ACTUAL
            else None
        ),
        scale=(
            None if score.normalizer is None else score.normalizer.denominator
        ),
    )
    return {
        **losses,
        "forecast_point": point,
        "observed_target": observed,
        "reference_consensus_value": reference_value,
        "reference_consensus_id": reference_id,
        "first_release_id": None if initial is None else initial.release_id,
        "first_release_available_at_ns": (
            None if initial is None else initial.available_at_ns
        ),
        "first_release_published_at_ns": (
            None if initial is None else initial.released_at_ns
        ),
        "cutoff_at_ns": snapshot.cutoff.cutoff_at_ns,
        "horizon": snapshot.cutoff.horizon.value,
        "target_kind": target.kind.value,
        "revision_release_id": revision_id,
        "consensus_replication_error": (
            abs(point - observed)
            if target.kind is ForecastTargetKind.CONSENSUS
            else None
        ),
    }


def _score_check(
    score: ForecastMathScore,
    quantiles: tuple[float, ...],
    thresholds: tuple[float, ...],
    direction_polarity: int = 1,
) -> dict[str, JSONValue]:
    calendar = _calendar(score)
    expected = _oracle(calendar)
    actual = score.metrics
    for key, value in expected.items():
        if key.endswith(("_id", "_ns")) or key in ("horizon", "target_kind"):
            if actual.get(key) != value or type(actual.get(key)) is not type(
                value
            ):
                raise ValueError(f"independent outcome identity differs: {key}")
        else:
            _same_number(actual.get(key), value, key)
    snapshot = calendar.snapshot
    observed = reference.number(expected["observed_target"])
    support = snapshot.distribution.support
    distribution: dict[str, JSONValue] = {
        "weighted_crps": reference.weighted_crps(support, observed),
        "quantile_scores": [
            {
                "probability": probability,
                "quantile": reference.discrete_quantile(support, probability),
                "pinball_loss": reference.pinball_loss(
                    reference.discrete_quantile(support, probability),
                    observed,
                    probability,
                ),
            }
            for probability in quantiles
        ],
        "exceedance_scores": [],
    }
    exceedances: list[JSONValue] = []
    for threshold in thresholds:
        probability = reference.finite_sum(
            tuple(mass for value, mass in support if value > threshold)
        )
        outcome = observed > threshold
        log = reference.binary_log_score(probability, outcome)
        exceedances.append(
            {
                "threshold": threshold,
                "relation": "strict-greater-than",
                "probability": probability,
                "outcome": outcome,
                "brier": reference.brier_score(probability, outcome),
                "log_score": log.loss,
                "log_score_status": (
                    "positive_infinity_impossible_event"
                    if log.impossible_event
                    else "finite"
                ),
            }
        )
    distribution["exceedance_scores"] = exceedances
    raw_direction, mapped_direction = reference.directional_mapping(
        reference.number(expected["forecast_minus_observed"]),
        direction_polarity,
    )
    return {
        "score_id": score.score_id,
        "error_raw_sign": raw_direction,
        "error_mapped_sign": mapped_direction,
        "event_key": snapshot.target.logical_event_key,
        "unit": snapshot.target.unit,
        "scale": snapshot.target.scale,
        "base": snapshot.target.base,
        "horizon": snapshot.cutoff.horizon.value,
        "target_kind": snapshot.target.kind.value,
        "reference_point_metrics": expected,
        "distribution_metrics": distribution,
    }


def _aggregation_check(
    subject: ForecastMathSubject, rows: list[dict[str, JSONValue]]
) -> dict[str, JSONValue] | None:
    if isinstance(subject, ForecastScoreReportV1):
        expected = [_oracle(score) for score in subject.scores]

        def aggregate(name: str, operation: str = "mean") -> float:
            values = tuple(reference.number(row[name]) for row in expected)
            if operation == "median":
                return reference.median_value(values)
            mean = reference.average(values)
            return math.sqrt(mean) if operation == "rmse" else mean

        result: dict[str, JSONValue] = {
            "count": len(expected),
            "mae": aggregate("absolute_error"),
            "rmse": aggregate("squared_error", "rmse"),
            "median_absolute_error": aggregate("absolute_error", "median"),
        }
        for name, field, operation in (
            ("normalized_mae", "normalized_absolute_error", "mean"),
            ("normalized_rmse", "normalized_squared_error", "rmse"),
            (
                "normalized_median_absolute_error",
                "normalized_absolute_error",
                "median",
            ),
            (
                "mean_consensus_replication_error",
                "consensus_replication_error",
                "mean",
            ),
            ("mean_independent_value_added", "independent_value_added", "mean"),
        ):
            result[name] = (
                aggregate(field, operation)
                if all(row[field] is not None for row in expected)
                else None
            )
        for name, value in result.items():
            _same_number(
                subject.metrics.get(name),
                reference.number(value) if value is not None else None,
                name,
            )
        return result
    if isinstance(subject, ForecastEngineComparisonV1):
        errors = {
            score.snapshot.bound_model.engine_key: reference.number(
                _oracle(_calendar(score))["absolute_error"]
            )
            for score in subject.scores
        }
        actual = subject.metrics
        expected_reduction = {
            key: reference.number(value - errors[subject.candidate_key])
            for key, value in errors.items()
            if key != subject.candidate_key
        }
        if (
            actual.get("absolute_errors") != errors
            or actual.get("candidate_absolute_error_reduction")
            != expected_reduction
            or actual.get("evidence_unit_count") != 1
            or actual.get("independent_model_count") is not None
        ):
            raise ValueError("independent benchmark/ablation math differs")
        return {
            "absolute_errors": {key: value for key, value in errors.items()},
            "candidate_absolute_error_reduction": {
                key: value for key, value in expected_reduction.items()
            },
            "evidence_unit_count": 1,
            "independent_model_count": None,
        }
    return None


@dataclass(frozen=True, slots=True)
class ForecastMathCheckedReportV1:
    subject_json: str
    verification: ForecastMathVerificationV1
    quantiles: tuple[float, ...] = (0.1, 0.5, 0.9)
    thresholds: tuple[float, ...] = ()
    direction_polarity: int = 1
    direction_label: str = "numerical-error-orientation"

    def __post_init__(self) -> None:
        reference.directional_mapping(0.0, self.direction_polarity)
        if (
            type(self.direction_label) is not str
            or not 1 <= len(self.direction_label) <= 256
        ):
            raise ValueError("direction mapping requires a bounded declaration")
        if type(self.verification) is not ForecastMathVerificationV1:
            raise ValueError("checked report requires executed verification")
        current = ForecastMathVerificationV1.from_dict(
            self.verification.to_dict()
        )
        if not current.passed:
            raise ValueError("current mathematical verification failed")
        if (
            type(self.quantiles) is not tuple
            or not 1 <= len(self.quantiles) <= 32
            or type(self.thresholds) is not tuple
            or len(self.thresholds) > 32
        ):
            raise ValueError("checked distribution request exceeds bounds")
        probabilities = tuple(
            reference.number(value) for value in self.quantiles
        )
        thresholds = tuple(reference.number(value) for value in self.thresholds)
        if (
            any(not 0 < value < 1 for value in probabilities)
            or tuple(sorted(set(probabilities))) != probabilities
            or tuple(sorted(set(thresholds))) != thresholds
        ):
            raise ValueError(
                "checked distribution requests must be sorted and unique"
            )
        object.__setattr__(self, "quantiles", probabilities)
        object.__setattr__(self, "thresholds", thresholds)
        subject = _restore_subject(self.subject_json)
        object.__setattr__(self, "subject_json", subject.to_json())
        self.to_json()

    @property
    def report_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        if not ForecastMathVerificationV1.from_dict(
            self.verification.to_dict()
        ).passed:
            raise ValueError("current mathematical verification failed")
        subject = _restore_subject(self.subject_json)
        rows = [
            _score_check(
                score, self.quantiles, self.thresholds, self.direction_polarity
            )
            for score in _rows(subject)
        ]
        subject_formulas = {
            "distribution-point-v1",
            "point-scores-v1",
            "weighted-crps-v1",
            "pinball-quantile-v1",
            "surprise-value-added-v1",
        }
        if self.thresholds:
            subject_formulas.update(("binary-brier-v1", "binary-log-score-v1"))
        return math_seal(
            "forecast-math-checked-report",
            {
                "subject": math_load(self.subject_json),
                "verification": self.verification.to_dict(),
                "required_formula_ids": [key for key, _ in FORMULAS],
                "subject_formula_ids": sorted(subject_formulas),
                "harness_only_formula_ids": [
                    key for key, _ in FORMULAS if key not in subject_formulas
                ],
                "quantiles": list(self.quantiles),
                "thresholds": list(self.thresholds),
                "direction_polarity": self.direction_polarity,
                "direction_label": self.direction_label,
                "direction_interpretation": "declared-coordinate-not-economic-or-trading-qualification",
                "checks": rows,
                "aggregate_check": _aggregation_check(subject, rows),
                "qualification": "formula-checked-not-empirical-calibration-or-production-approval",
            },
        )

    def to_json(self) -> str:
        return math_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastMathCheckedReportV1:
        math_json(data)
        if (
            type(data.get("quantiles")) is not list
            or type(data.get("thresholds")) is not list
        ):
            raise ValueError("checked report distribution request is malformed")
        result = cls(
            math_json(data["subject"]),
            ForecastMathVerificationV1.from_dict(data["verification"]),
            tuple(data["quantiles"]),
            tuple(data["thresholds"]),
            data["direction_polarity"],
            data["direction_label"],
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastMathCheckedReportV1:
        return cls.from_dict(math_load(text))


@dataclass(frozen=True, slots=True)
class ForecastHacPolicyV1:
    """An explicit homogeneous task policy, not automatic bandwidth selection.

    This arithmetic contract does not prove preregistration of its parameters.
    Chronological selection/holdout governance belongs to downstream research.
    """

    lags: int
    horizon_steps: int
    minimum_count: int = 20
    loss: str = "absolute_error"
    ordering: str = "first-release-availability-event-index"
    assumption: str = "weakly-stationary-homogeneous-release-loss-differences"

    def __post_init__(self) -> None:
        if (
            type(self.lags) is not int
            or type(self.horizon_steps) is not int
            or not 1 <= self.horizon_steps <= reference.MAX_HAC_LAGS + 1
            or not self.horizon_steps - 1 <= self.lags <= reference.MAX_HAC_LAGS
        ):
            raise ValueError("HAC lag/horizon policy is invalid")
        if (
            type(self.minimum_count) is not int
            or not 8 <= self.minimum_count <= MAX_CHECKED_SCORES
            or self.loss not in ("absolute_error", "squared_error")
            or self.ordering != "first-release-availability-event-index"
            or self.assumption
            != "weakly-stationary-homogeneous-release-loss-differences"
        ):
            raise ValueError(
                "HAC inference policy differs from frozen semantics"
            )


@dataclass(frozen=True, slots=True)
class ForecastPairedLossReportV1:
    baseline_json: tuple[str, ...]
    candidate_json: tuple[str, ...]
    policy: ForecastHacPolicyV1
    verification: ForecastMathVerificationV1

    def __post_init__(self) -> None:
        for values in (self.baseline_json, self.candidate_json):
            if (
                type(values) is not tuple
                or not 1 <= len(values) <= MAX_CHECKED_SCORES
                or any(type(text) is not str for text in values)
            ):
                raise ValueError(
                    "paired report requires bounded immutable scores"
                )
        if (
            sum(len(text) for text in self.baseline_json + self.candidate_json)
            > MAX_MATH_ARTIFACT_BYTES
        ):
            raise ValueError(
                "paired evidence exceeds byte bounds before replay"
            )
        if (
            type(self.policy) is not ForecastHacPolicyV1
            or type(self.verification) is not ForecastMathVerificationV1
            or not ForecastMathVerificationV1.from_dict(
                self.verification.to_dict()
            ).passed
        ):
            raise ValueError(
                "paired comparison requires current policy/math verification"
            )
        for field in ("baseline_json", "candidate_json"):
            canonical = tuple(
                _restore_subject(text).to_json()
                for text in getattr(self, field)
            )
            object.__setattr__(self, field, canonical)
        self.to_json()

    def _evidence(self) -> tuple[list[dict[str, JSONValue]], dict[str, Any]]:
        columns = []
        for texts in (self.baseline_json, self.candidate_json):
            indexed = {}
            task_keys = set()
            for text in texts:
                subject = _restore_subject(text)
                if not isinstance(
                    subject,
                    (
                        ForecastScoreV1,
                        ForecastFeatureScoreV1,
                        ForecastEngineScoreV1,
                    ),
                ):
                    raise ValueError(
                        "paired report requires individual complete scores"
                    )
                calendar = _calendar(subject)
                snapshot = calendar.snapshot
                key = snapshot.target.logical_event_key
                if key in indexed:
                    raise ValueError("paired report duplicates an event")
                task_keys.add(_homogeneous_key(calendar))
                if (
                    snapshot.target.kind
                    is not ForecastTargetKind.FIRST_RELEASE_ACTUAL
                ):
                    raise ValueError(
                        "HAC v1 only supports first-release actual homogeneous tasks"
                    )
                # Target selection, errors, probability points and support are
                # checked against independent reference evidence first.
                _score_check(subject, (0.5,), ())
                indexed[key] = (subject, calendar, _oracle(calendar))
            if len(task_keys) != 1:
                raise ValueError(
                    "HAC cannot pool heterogeneous targets/horizons/models/units"
                )
            columns.append(indexed)
        baseline, candidate = columns
        if set(baseline) != set(candidate):
            raise ValueError("paired comparison rejects missing predictions")
        rows: list[dict[str, JSONValue]] = []
        for key in baseline:
            left, left_calendar, left_values = baseline[key]
            right, right_calendar, right_values = candidate[key]
            a, b = left_calendar.snapshot, right_calendar.snapshot
            if (
                a.target != b.target
                or a.cutoff != b.cutoff
                or left_calendar.outcome_calendar_json
                != right_calendar.outcome_calendar_json
            ):
                raise ValueError(
                    "paired comparison requires exact task/cutoff/outcome semantics"
                )
            clock = left_values["first_release_available_at_ns"]
            if type(clock) is not int:
                raise ValueError(
                    "paired comparison requires an actual availability clock"
                )
            rows.append(
                {
                    "event_key": key,
                    "available_at_ns": clock,
                    "baseline_score_id": left.score_id,
                    "candidate_score_id": right.score_id,
                    "baseline_loss": reference.number(
                        left_values[self.policy.loss]
                    ),
                    "candidate_loss": reference.number(
                        right_values[self.policy.loss]
                    ),
                    "difference": reference.number(
                        left_values[self.policy.loss]
                        - right_values[self.policy.loss]
                    ),
                }
            )

        def ordering(row: dict[str, JSONValue]) -> int:
            clock = row["available_at_ns"]
            if type(clock) is not int:
                raise ValueError("paired clock must be an exact integer")
            return clock

        rows.sort(key=ordering)
        if len({row["available_at_ns"] for row in rows}) != len(rows):
            raise ValueError(
                "paired event ordering has ambiguous simultaneous outcomes"
            )
        differences = tuple(reference.number(row["difference"]) for row in rows)
        result = reference.paired_hac(
            differences,
            lags=self.policy.lags,
            horizon_steps=self.policy.horizon_steps,
            minimum_count=self.policy.minimum_count,
        )
        return rows, asdict(result)

    @property
    def report_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        if not ForecastMathVerificationV1.from_dict(
            self.verification.to_dict()
        ).passed:
            raise ValueError("current mathematical verification failed")
        rows, result = self._evidence()
        subject_formulas = {
            "distribution-point-v1",
            "point-scores-v1",
            "paired-bartlett-hac-v1",
        }
        return math_seal(
            "forecast-paired-loss-report",
            {
                "baseline": [math_load(text) for text in self.baseline_json],
                "candidate": [math_load(text) for text in self.candidate_json],
                "policy": asdict(self.policy),
                "verification": self.verification.to_dict(),
                "required_formula_ids": [key for key, _ in FORMULAS],
                "subject_formula_ids": sorted(subject_formulas),
                "harness_only_formula_ids": [
                    key for key, _ in FORMULAS if key not in subject_formulas
                ],
                "paired_rows": rows,
                "result": result,
                "qualification": "conditional-normal-approximation-not-model-selection-or-holdout-approval",
            },
        )

    def to_json(self) -> str:
        return math_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastPairedLossReportV1:
        math_json(data)
        if (
            type(data.get("baseline")) is not list
            or type(data.get("candidate")) is not list
        ):
            raise ValueError("paired score arrays are malformed")
        result = cls(
            tuple(math_json(value) for value in data["baseline"]),
            tuple(math_json(value) for value in data["candidate"]),
            ForecastHacPolicyV1(**data["policy"]),
            ForecastMathVerificationV1.from_dict(data["verification"]),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastPairedLossReportV1:
        return cls.from_dict(math_load(text))
