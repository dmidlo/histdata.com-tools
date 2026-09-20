"""Explicit installed engine bindings and full-envelope deterministic replay.

This is an offline reference runner, not a sandbox, scheduler or qualification
authority. Unknown executable identities fail closed; metadata never imports code.
"""

from __future__ import annotations

import hashlib
import statistics
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
)
from histdatacom.runtime_contracts import JSONValue

from . import feature_forecasts
from .contracts import (
    ForecastCutoffV1,
    ForecastDistributionV1,
    ForecastHorizon,
    ForecastTargetKind,
    ForecastTargetV1,
    _finite,
    _json,
    _load,
    _mapping,
    _ns,
    _seal,
    _text,
    _verify,
)
from .engine_contracts import (
    _items,
    EngineEnsembleRole,
    EngineFunction,
    EngineInformation,
    EngineOutput,
    EngineProbability,
    EngineReadiness,
    ForecastEngineDescriptorV1,
    ForecastEngineRegistryV1,
    default_forecast_taxonomy,
)
from .feature_contracts import FeatureCellStatus, FeatureKind
from .feature_forecasts import (
    ForecastFeatureInputsV1,
    ForecastFeatureModelV1,
    ForecastFeatureScoreV1,
    ForecastFeatureSnapshotV1,
    forecast_feature_baseline,
    train_feature_baseline,
)
from .feature_store import VintageFeatureStoreV1

REFERENCE_BLEND = "reference-blend"
HISTORICAL_MEAN = "historical-mean"
HISTORICAL_MEDIAN = "historical-median"
LAST_VALUE = "last-value"
_KEYS = (REFERENCE_BLEND, HISTORICAL_MEAN, HISTORICAL_MEDIAN, LAST_VALUE)


def _implementation_digest() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _descriptor(key: str) -> ForecastEngineDescriptorV1:
    if key not in _KEYS:
        raise ValueError("no installed executable binding for engine")
    legacy = key == REFERENCE_BLEND
    module = feature_forecasts.__name__ if legacy else __name__
    path = Path(feature_forecasts.__file__) if legacy else Path(__file__)
    return ForecastEngineDescriptorV1(
        key,
        "1.0.0",
        "feature-reference-baseline" if legacy else key,
        "1.0" if legacy else "1.0.0",
        module,
        hashlib.sha256(path.read_bytes()).hexdigest(),
        (f"benchmark/{key}",),
        (EngineInformation.MACRO, EngineInformation.CALENDAR),
        tuple(ForecastHorizon),
        (ForecastTargetKind.FIRST_RELEASE_ACTUAL,),
        ("monthly", "quarterly"),
        EngineFunction.PRIMARY,
        (
            EngineEnsembleRole.CANDIDATE
            if legacy
            else EngineEnsembleRole.BENCHMARK
        ),
        EngineOutput.FORECAST,
        EngineProbability.POINT,
        EngineReadiness.REFERENCE,
        3,
        10_000,
        1,
        "exact-replay-same-installed-code-and-runtime; no-cross-platform-claim",
        "exact-replay-same-installed-code-and-runtime; no-RNG",
        "bounded univariate scan; median O(n log n), mean/last/blend O(n); no wall-time guarantee",
        (
            "unknown-executable",
            "unsupported-task",
            "insufficient-sample",
            "unavailable-feature",
            "semantic-mismatch",
            "resource-bound",
            "changed-fit-evidence",
        ),
        (
            "ex-ante-feature-envelope-only",
            "no-missing-value-fill",
            "known-at-generation",
            "same-horizon-target-kind-fit",
            "exact-vintage-prefix",
            "no-final-holdout-selection",
        ),
        (
            "Reference mean/latest blend; no empirical skill claim"
            if legacy
            else f"Permanent historical {key} comparator; not an ensemble independence claim"
        ),
        HISTORICAL_MEAN if key != HISTORICAL_MEAN else LAST_VALUE,
        (HISTORICAL_MEAN, HISTORICAL_MEDIAN),
    )


def default_forecast_registry() -> ForecastEngineRegistryV1:
    return ForecastEngineRegistryV1(
        "1.0.0",
        default_forecast_taxonomy(),
        tuple(_descriptor(key) for key in _KEYS),
    )


def _executable(
    registry: ForecastEngineRegistryV1, key: str
) -> ForecastEngineDescriptorV1:
    descriptor = registry.engine(key)
    if descriptor != _descriptor(key):
        raise ValueError(
            "registered descriptor differs from exact installed executable contract"
        )
    return descriptor


def _values(
    inputs: ForecastFeatureInputsV1,
    column: str,
    descriptor: ForecastEngineDescriptorV1,
    *,
    training: bool,
) -> tuple[tuple[float, ...], dict[str, JSONValue]]:
    if not isinstance(inputs, ForecastFeatureInputsV1):
        raise TypeError("engine requires exact feature-aware inputs")
    request = inputs.features.request
    if (
        len(request.columns) > descriptor.maximum_columns
        or len(request.periods) > descriptor.maximum_sample
    ):
        raise ValueError("engine input exceeds declared resource bound")
    if training and len(request.periods) < descriptor.minimum_sample:
        raise ValueError("engine has insufficient complete training sample")
    specification = next(
        (item for item in request.columns if item.name == column), None
    )
    if specification is None:
        raise ValueError("engine input column is absent")
    cells = inputs.features.column(column)
    if not cells or any(
        cell.status is not FeatureCellStatus.AVAILABLE or cell.value is None
        for cell in cells
    ):
        raise ValueError("engine refuses unavailable/stale/warmup cells")
    semantics = {cell.semantic_id for cell in cells}
    if len(semantics) != 1:
        raise ValueError("engine refuses mixed semantic eras")
    definition = next(
        item.definition
        for item in inputs.features.observations
        if item.definition.semantic_id == cells[-1].semantic_id
    )
    if (
        definition.kind is not FeatureKind.MACRO
        or definition.frequency not in descriptor.frequencies
    ):
        raise ValueError("engine information channel/frequency mismatch")
    # Reference level estimators do not silently forecast a transformed target.
    if specification.transform.kind.value != "level":
        raise ValueError("reference engine requires a level feature")
    values = tuple(
        _finite(cell.value, "engine feature")
        for cell in cells
        if cell.value is not None
    )
    return values, {
        "column": column,
        "semantic_id": definition.semantic_id,
        "unit": definition.unit,
        "scale": definition.scale,
        "base": definition.base,
        "frequency": definition.frequency,
        "transform": specification.transform.to_dict(),
    }


def _fit(
    registry: ForecastEngineRegistryV1,
    key: str,
    inputs: ForecastFeatureInputsV1,
    column: str,
    trained_at_ns: int,
) -> ForecastFeatureModelV1:
    descriptor = _executable(registry, key)
    _ns(trained_at_ns, "trained_at_ns")
    if trained_at_ns < inputs.cutoff_at_ns:
        raise ValueError("training precedes its information cutoff")
    values, semantic = _values(inputs, column, descriptor, training=True)
    if key == REFERENCE_BLEND:
        return train_feature_baseline(
            inputs, column=column, trained_at_ns=trained_at_ns
        )
    try:
        estimate = (
            statistics.mean(values)
            if key == HISTORICAL_MEAN
            else (
                statistics.median(values)
                if key == HISTORICAL_MEDIAN
                else values[-1]
            )
        )
    except OverflowError as exc:
        raise ValueError("reference fit exceeds finite numeric range") from exc
    return ForecastFeatureModelV1(
        descriptor.model_name,
        descriptor.model_version,
        descriptor.implementation_sha256,
        _json(
            {
                **semantic,
                "estimator": key,
                "training_count": len(values),
                "estimate": _finite(estimate, "reference estimate"),
            }
        ),
        inputs,
        trained_at_ns,
    )


@dataclass(frozen=True, slots=True)
class ForecastEngineModelV1:
    registry: ForecastEngineRegistryV1
    engine_key: str
    column: str
    horizon: ForecastHorizon
    target_kind: ForecastTargetKind
    model: ForecastFeatureModelV1
    binding_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(
            self.registry, ForecastEngineRegistryV1
        ) or not isinstance(self.model, ForecastFeatureModelV1):
            raise TypeError(
                "engine model requires full registry and feature model"
            )
        object.__setattr__(self, "column", _text(self.column, "column"))
        object.__setattr__(self, "horizon", ForecastHorizon(self.horizon))
        object.__setattr__(
            self, "target_kind", ForecastTargetKind(self.target_kind)
        )
        descriptor = _executable(self.registry, self.engine_key)
        if (
            self.horizon not in descriptor.horizons
            or self.target_kind not in descriptor.targets
        ):
            raise ValueError("unsupported fit horizon/target")
        if self.binding_sha256 != _implementation_digest():
            raise ValueError("engine runner executable identity changed")
        expected = _fit(
            self.registry,
            self.engine_key,
            self.model.training_inputs,
            self.column,
            self.model.trained_at_ns,
        )
        if expected != self.model:
            raise ValueError(
                "engine fitted model differs from executed exact replay"
            )
        self.to_dict()

    @property
    def model_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-model",
            {
                "registry": self.registry.to_dict(),
                "engine_key": self.engine_key,
                "column": self.column,
                "horizon": self.horizon.value,
                "target_kind": self.target_kind.value,
                "model": self.model.to_dict(),
                "binding_sha256": self.binding_sha256,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineModelV1:
        result = cls(
            ForecastEngineRegistryV1.from_dict(_mapping(data["registry"])),
            data["engine_key"],
            data["column"],
            ForecastHorizon(data["horizon"]),
            ForecastTargetKind(data["target_kind"]),
            ForecastFeatureModelV1.from_dict(_mapping(data["model"])),
            data["binding_sha256"],
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastEngineModelV1:
        return cls.from_dict(_load(text))


def _generate(
    bound: ForecastEngineModelV1,
    inputs: ForecastFeatureInputsV1,
    cutoff: ForecastCutoffV1,
    target: ForecastTargetV1,
    generated_at_ns: int,
) -> ForecastFeatureSnapshotV1:
    descriptor = _executable(bound.registry, bound.engine_key)
    if bound.binding_sha256 != _implementation_digest():
        raise ValueError("engine runner executable identity changed")
    if cutoff.horizon != bound.horizon or target.kind != bound.target_kind:
        raise ValueError("engine fit/generate horizon or target mismatch")
    current_values, semantic = _values(
        inputs, bound.column, descriptor, training=False
    )
    _, training_semantic = _values(
        bound.model.training_inputs, bound.column, descriptor, training=True
    )
    if semantic != training_semantic or (
        semantic["unit"],
        semantic["scale"],
        semantic["base"],
    ) != (target.unit, target.scale, target.base):
        raise ValueError("engine training/inference/target semantic mismatch")
    # Validate the complete scheduling/target/availability envelope before any
    # predictive implementation is called; the placeholder is never published.
    ForecastFeatureSnapshotV1(
        cutoff,
        target,
        inputs,
        bound.model,
        ForecastDistributionV1(((0.0, 1.0),)),
        generated_at_ns,
    )
    schedule = next(
        item
        for item in inputs.calendar_inputs.calendar.releases
        if item.release_id == cutoff.schedule_release_id
    )
    if (
        schedule.frequency not in descriptor.frequencies
        or schedule.frequency != semantic["frequency"]
    ):
        raise ValueError("engine source/target frequency mismatch")
    if bound.engine_key == REFERENCE_BLEND:
        result = forecast_feature_baseline(
            bound.model,
            inputs,
            cutoff=cutoff,
            target=target,
            generated_at_ns=generated_at_ns,
        )
    else:
        state = _load(bound.model.state_json)
        estimate = (
            current_values[-1]
            if bound.engine_key == LAST_VALUE
            else state["estimate"]
        )
        result = ForecastFeatureSnapshotV1(
            cutoff,
            target,
            inputs,
            bound.model,
            ForecastDistributionV1(((_finite(estimate, "estimate"), 1.0),)),
            generated_at_ns,
        )
    if (
        result.inputs != inputs
        or result.model != bound.model
        or result.cutoff != cutoff
        or result.target != target
        or result.generated_at_ns != generated_at_ns
        or len(result.distribution.support) != 1
    ):
        raise ValueError("engine returned incompatible output envelope")
    return result


@dataclass(frozen=True, slots=True)
class ForecastEngineSnapshotV1:
    bound_model: ForecastEngineModelV1
    forecast: ForecastFeatureSnapshotV1

    def __post_init__(self) -> None:
        if not isinstance(
            self.bound_model, ForecastEngineModelV1
        ) or not isinstance(self.forecast, ForecastFeatureSnapshotV1):
            raise TypeError(
                "engine snapshot requires its bound model and complete feature forecast"
            )
        if self.forecast.model != self.bound_model.model:
            raise ValueError("engine forecast/model binding mismatch")
        expected = _generate(
            self.bound_model,
            self.forecast.inputs,
            self.forecast.cutoff,
            self.forecast.target,
            self.forecast.generated_at_ns,
        )
        if expected != self.forecast:
            raise ValueError(
                "engine forecast differs from executed exact replay"
            )
        self.to_dict()

    @property
    def snapshot_id(self) -> str:
        return str(self.to_dict()["id"])

    def verify_against(
        self, corpus: EconomicCalendarCorpusV1, store: VintageFeatureStoreV1
    ) -> None:
        self.forecast.verify_against(corpus, store)
        ForecastEngineSnapshotV1(self.bound_model, self.forecast)

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-snapshot",
            {
                "bound_model": self.bound_model.to_dict(),
                "forecast": self.forecast.to_dict(),
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineSnapshotV1:
        result = cls(
            ForecastEngineModelV1.from_dict(_mapping(data["bound_model"])),
            ForecastFeatureSnapshotV1.from_dict(_mapping(data["forecast"])),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastEngineSnapshotV1:
        return cls.from_dict(_load(text))


@dataclass(frozen=True, slots=True)
class ForecastEngineScoreV1:
    snapshot: ForecastEngineSnapshotV1
    feature_score: ForecastFeatureScoreV1

    def __post_init__(self) -> None:
        if not isinstance(
            self.snapshot, ForecastEngineSnapshotV1
        ) or not isinstance(self.feature_score, ForecastFeatureScoreV1):
            raise TypeError(
                "engine scoring requires complete engine and feature envelopes"
            )
        ForecastEngineSnapshotV1(
            self.snapshot.bound_model, self.snapshot.forecast
        )
        if self.feature_score.snapshot != self.snapshot.forecast:
            raise ValueError("engine/feature score snapshot mismatch")
        self.to_dict()

    @property
    def score_id(self) -> str:
        return str(self.to_dict()["id"])

    @property
    def metrics(self) -> dict[str, JSONValue]:
        return self.feature_score.metrics

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-score",
            {
                "snapshot": self.snapshot.to_dict(),
                "feature_score": self.feature_score.to_dict(),
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineScoreV1:
        result = cls(
            ForecastEngineSnapshotV1.from_dict(_mapping(data["snapshot"])),
            ForecastFeatureScoreV1.from_dict(_mapping(data["feature_score"])),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastEngineScoreV1:
        return cls.from_dict(_load(text))


@dataclass(frozen=True, slots=True)
class ForecastEngineComparisonV1:
    """Executed same-task benchmarks/ablation; explicitly not qualification."""

    candidate_key: str
    scores: tuple[ForecastEngineScoreV1, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scores, tuple)
            or not 2 <= len(self.scores) <= 64
        ):
            raise ValueError(
                "comparison requires bounded immutable execution tuple"
            )
        if any(
            not isinstance(item, ForecastEngineScoreV1) for item in self.scores
        ):
            raise TypeError("comparison requires typed executed scores")
        scores = tuple(
            sorted(
                self.scores,
                key=lambda item: item.snapshot.bound_model.engine_key,
            )
        )
        if not 2 <= len(scores) <= 64:
            raise ValueError(
                "comparison requires bounded independent executions"
            )
        by_key = {item.snapshot.bound_model.engine_key: item for item in scores}
        if len(by_key) != len(scores) or self.candidate_key not in by_key:
            raise ValueError(
                "comparison keys must be unique and contain candidate"
            )
        candidate = by_key[self.candidate_key]
        model = candidate.snapshot.bound_model
        descriptor = model.registry.engine(self.candidate_key)
        required = {
            self.candidate_key,
            descriptor.ablation_key,
            *descriptor.minimum_benchmarks,
        }
        if set(by_key) != required:
            raise ValueError(
                "comparison lacks exact benchmark/ablation execution set"
            )
        base = candidate.snapshot.forecast
        for score in scores:
            ForecastEngineScoreV1(score.snapshot, score.feature_score)
            other = score.snapshot.forecast
            if (
                score.snapshot.bound_model.registry != model.registry
                or (
                    other.cutoff,
                    other.target,
                    other.inputs,
                    other.model.training_inputs,
                    other.model.trained_at_ns,
                    other.generated_at_ns,
                )
                != (
                    base.cutoff,
                    base.target,
                    base.inputs,
                    base.model.training_inputs,
                    base.model.trained_at_ns,
                    base.generated_at_ns,
                )
                or score.feature_score.outcome_calendar_json
                != candidate.feature_score.outcome_calendar_json
                or score.feature_score.scored_at_ns
                != candidate.feature_score.scored_at_ns
            ):
                raise ValueError(
                    "benchmark/ablation requires identical task, vintage inputs and outcome evidence"
                )
        object.__setattr__(self, "scores", scores)
        self.to_dict()

    @property
    def comparison_id(self) -> str:
        return str(self.to_dict()["id"])

    @property
    def metrics(self) -> dict[str, JSONValue]:
        errors: dict[str, float] = {}
        for score in self.scores:
            error = score.metrics["absolute_error"]
            if not isinstance(error, (int, float)):
                raise ValueError("absolute error must be numeric")
            errors[score.snapshot.bound_model.engine_key] = _finite(
                error, "absolute error"
            )
        candidate = errors[self.candidate_key]
        wire_errors: dict[str, JSONValue] = {
            key: value for key, value in errors.items()
        }
        return {
            "absolute_errors": wire_errors,
            "candidate_absolute_error_reduction": {
                key: value - candidate
                for key, value in errors.items()
                if key != self.candidate_key
            },
            "evidence_unit_count": 1,
            "qualification": "reference-case-only-not-chronological-validation",
            "independent_model_count": None,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-comparison",
            {
                "candidate_key": self.candidate_key,
                "scores": [item.to_dict() for item in self.scores],
                "metrics": self.metrics,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineComparisonV1:
        result = cls(
            data["candidate_key"],
            tuple(
                ForecastEngineScoreV1.from_dict(_mapping(item))
                for item in _items(data["scores"], 64)
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastEngineComparisonV1:
        return cls.from_dict(_load(text))


@dataclass(frozen=True, slots=True)
class ForecastEngineRunnerV1:
    """Standard bounded fit/generate/score interface for installed references."""

    registry: ForecastEngineRegistryV1

    def __post_init__(self) -> None:
        if not isinstance(self.registry, ForecastEngineRegistryV1):
            raise TypeError("runner requires an immutable engine registry")

    def fit(
        self,
        engine_key: str,
        inputs: ForecastFeatureInputsV1,
        *,
        column: str,
        horizon: ForecastHorizon,
        target_kind: ForecastTargetKind,
        trained_at_ns: int,
    ) -> ForecastEngineModelV1:
        descriptor = _executable(self.registry, engine_key)
        if (
            horizon not in descriptor.horizons
            or target_kind not in descriptor.targets
        ):
            raise ValueError("unsupported fit horizon/target")
        return ForecastEngineModelV1(
            self.registry,
            engine_key,
            column,
            horizon,
            target_kind,
            _fit(self.registry, engine_key, inputs, column, trained_at_ns),
            _implementation_digest(),
        )

    def generate(
        self,
        model: ForecastEngineModelV1,
        inputs: ForecastFeatureInputsV1,
        *,
        cutoff: ForecastCutoffV1,
        target: ForecastTargetV1,
        generated_at_ns: int,
    ) -> ForecastEngineSnapshotV1:
        if (
            not isinstance(model, ForecastEngineModelV1)
            or model.registry != self.registry
        ):
            raise ValueError("runner requires its exact registry-bound model")
        # Reexecute the fit as well as the prediction, not just hash a claim.
        ForecastEngineModelV1.from_dict(model.to_dict())
        return ForecastEngineSnapshotV1(
            model, _generate(model, inputs, cutoff, target, generated_at_ns)
        )

    def score(
        self,
        snapshot: ForecastEngineSnapshotV1,
        outcomes: EconomicCalendarCorpusV1,
        *,
        scored_at_ns: int,
    ) -> ForecastEngineScoreV1:
        if (
            not isinstance(snapshot, ForecastEngineSnapshotV1)
            or snapshot.bound_model.registry != self.registry
        ):
            raise ValueError(
                "runner refuses bare or differently registered forecasts"
            )
        return ForecastEngineScoreV1(
            snapshot,
            ForecastFeatureScoreV1(
                snapshot.forecast, outcomes.to_json(), scored_at_ns
            ),
        )

    def compare(
        self,
        candidate_key: str,
        training: ForecastFeatureInputsV1,
        inference: ForecastFeatureInputsV1,
        *,
        column: str,
        cutoff: ForecastCutoffV1,
        target: ForecastTargetV1,
        trained_at_ns: int,
        generated_at_ns: int,
        outcomes: EconomicCalendarCorpusV1,
        scored_at_ns: int,
    ) -> ForecastEngineComparisonV1:
        descriptor = _executable(self.registry, candidate_key)
        keys = {
            candidate_key,
            descriptor.ablation_key,
            *descriptor.minimum_benchmarks,
        }
        scores = []
        for key in sorted(keys):
            model = self.fit(
                key,
                training,
                column=column,
                horizon=cutoff.horizon,
                target_kind=target.kind,
                trained_at_ns=trained_at_ns,
            )
            snapshot = self.generate(
                model,
                inference,
                cutoff=cutoff,
                target=target,
                generated_at_ns=generated_at_ns,
            )
            scores.append(
                self.score(snapshot, outcomes, scored_at_ns=scored_at_ns)
            )
        return ForecastEngineComparisonV1(candidate_key, tuple(scores))
