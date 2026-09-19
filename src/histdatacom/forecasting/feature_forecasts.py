"""Feature-aware forecast execution/replay without changing calendar-only v1."""

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

from .contracts import (
    ForecastCutoffV1,
    ForecastDistributionV1,
    ForecastInputsV1,
    ForecastModelIdentityV1,
    ForecastSnapshotV1,
    ForecastTargetV1,
    _array,
    _finite,
    _json,
    _load,
    _mapping,
    _ns,
    _seal,
    _text,
    _verify,
)
from .feature_contracts import FeatureCellStatus
from .feature_store import FeatureMatrixSnapshotV1, VintageFeatureStoreV1
from .scoring import ForecastScaleV1, ForecastScoreV1

_CALENDAR_LIMITATIONS = (
    "Explicit event scope and cutoff-filtered normalized vintages; no completeness claim.",
    "Per-record source limitations are retained; global mutable catalog metadata is not an ex-ante feature.",
)


@dataclass(frozen=True, slots=True)
class ForecastFeatureInputsV1:
    """Both calendar target context and the exact feature information set."""

    calendar_inputs: ForecastInputsV1
    features: FeatureMatrixSnapshotV1
    calendar_event_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(
            self.calendar_inputs, ForecastInputsV1
        ) or not isinstance(self.features, FeatureMatrixSnapshotV1):
            raise TypeError(
                "feature-aware inputs require sealed calendar and feature contracts"
            )
        if (
            self.calendar_inputs.cutoff_at_ns
            != self.features.request.cutoff_at_ns
        ):
            raise ValueError("calendar/feature information-set cutoff mismatch")
        keys = tuple(
            sorted(
                _text(item, "calendar event key")
                for item in self.calendar_event_keys
            )
        )
        if not keys or len(keys) > 10_000 or len(set(keys)) != len(keys):
            raise ValueError("event scope must be nonempty, unique and bounded")
        if any(
            item.logical_event_key not in keys
            for item in self.calendar_inputs.calendar.releases
        ):
            raise ValueError(
                "calendar vintage lies outside explicit event scope"
            )
        object.__setattr__(self, "calendar_event_keys", keys)
        self.to_dict()

    @property
    def cutoff_at_ns(self) -> int:
        return self.features.request.cutoff_at_ns

    @property
    def information_set_id(self) -> str:
        return str(self.to_dict()["id"])

    def verify_against(
        self, corpus: EconomicCalendarCorpusV1, store: VintageFeatureStoreV1
    ) -> None:
        self.features.verify_against(store)
        expected = capture_forecast_feature_inputs(
            corpus,
            self.features,
            calendar_event_keys=self.calendar_event_keys,
            calendar_coverage_start_ns=self.calendar_inputs.calendar.coverage_start_ns,
            calendar_coverage_end_ns=self.calendar_inputs.calendar.coverage_end_ns,
        )
        if expected != self:
            raise ValueError(
                "exact historical calendar information set changed"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-feature-inputs",
            {
                "calendar_inputs": self.calendar_inputs.to_dict(),
                "features": self.features.to_dict(),
                "calendar_event_keys": list(self.calendar_event_keys),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastFeatureInputsV1:
        result = cls(
            ForecastInputsV1.from_dict(_mapping(data["calendar_inputs"])),
            FeatureMatrixSnapshotV1.from_dict(_mapping(data["features"])),
            tuple(_array(data["calendar_event_keys"])),
        )
        _verify(data, result.to_dict())
        return result


def capture_forecast_feature_inputs(
    corpus: EconomicCalendarCorpusV1,
    features: FeatureMatrixSnapshotV1,
    *,
    calendar_event_keys: tuple[str, ...],
    calendar_coverage_start_ns: int,
    calendar_coverage_end_ns: int,
) -> ForecastFeatureInputsV1:
    """Capture fixed calendar scope without importing future catalog metadata.

    Coverage is an explicit caller request, never copied from the evolving
    corpus. All known vintages of selected events are retained, not just their
    latest state; insufficient coverage bounds fail rather than truncate chains.
    """
    if not isinstance(corpus, EconomicCalendarCorpusV1) or not isinstance(
        features, FeatureMatrixSnapshotV1
    ):
        raise TypeError("capture requires calendar corpus and feature snapshot")
    cutoff = features.request.cutoff_at_ns
    requested = set(calendar_event_keys)
    releases = tuple(
        item
        for item in corpus.releases
        if item.available_at_ns <= cutoff
        and item.logical_event_key in requested
    )
    known = {item.logical_event_key for item in releases}
    captured = EconomicCalendarCorpusV1(
        calendar_coverage_start_ns,
        calendar_coverage_end_ns,
        False,
        releases,
        tuple(
            item
            for item in corpus.forecasts
            if item.available_at_ns <= cutoff
            and item.logical_event_key in known
        ),
        _CALENDAR_LIMITATIONS,
    )
    return ForecastFeatureInputsV1(
        ForecastInputsV1(cutoff, captured.to_json()),
        features,
        calendar_event_keys,
    )


@dataclass(frozen=True, slots=True)
class ForecastFeatureModelV1:
    """Trained state bound to full feature evidence, not a calendar-only claim."""

    name: str
    version: str
    implementation_sha256: str
    state_json: str
    training_inputs: ForecastFeatureInputsV1
    trained_at_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.training_inputs, ForecastFeatureInputsV1):
            raise TypeError(
                "feature model requires feature-aware training inputs"
            )
        projection = self._calendar_model()
        object.__setattr__(self, "name", projection.name)
        object.__setattr__(self, "version", projection.version)
        object.__setattr__(self, "state_json", projection.state_json)
        self.to_dict()

    def _calendar_model(self) -> ForecastModelIdentityV1:
        """Private validation adapter, never a complete model replay artifact."""
        return ForecastModelIdentityV1(
            self.name,
            self.version,
            self.implementation_sha256,
            self.state_json,
            self.training_inputs.calendar_inputs,
            self.trained_at_ns,
        )

    @property
    def model_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-feature-model",
            {
                "name": self.name,
                "version": self.version,
                "implementation_sha256": self.implementation_sha256,
                "state": _load(self.state_json),
                "training_inputs": self.training_inputs.to_dict(),
                "trained_at_ns": self.trained_at_ns,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastFeatureModelV1:
        result = cls(
            data["name"],
            data["version"],
            data["implementation_sha256"],
            _json(_mapping(data["state"])),
            ForecastFeatureInputsV1.from_dict(
                _mapping(data["training_inputs"])
            ),
            data["trained_at_ns"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastFeatureSnapshotV1:
    """Complete feature forecast; a bare calendar projection is not this type."""

    cutoff: ForecastCutoffV1
    target: ForecastTargetV1
    inputs: ForecastFeatureInputsV1
    model: ForecastFeatureModelV1
    distribution: ForecastDistributionV1
    generated_at_ns: int
    predicted_consensus: ForecastFeatureSnapshotV1 | None = None
    normalizer_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(
            self.inputs, ForecastFeatureInputsV1
        ) or not isinstance(self.model, ForecastFeatureModelV1):
            raise TypeError(
                "feature forecast requires full feature-aware inputs/model"
            )
        _ns(self.generated_at_ns, "generated_at_ns")
        if any(
            item.available_at_ns > self.generated_at_ns
            for item in self.inputs.features.observations
        ) or any(
            item.known_at_ns > self.generated_at_ns
            for item in self.inputs.features.schedules
        ):
            raise ValueError(
                "feature or schedule knowledge follows forecast generation"
            )
        _verify_training_overlap(
            self.model.training_inputs.features, self.inputs.features
        )
        if self.predicted_consensus is not None:
            if not isinstance(
                self.predicted_consensus, ForecastFeatureSnapshotV1
            ):
                raise TypeError(
                    "predicted consensus requires complete feature forecast"
                )
            if self.predicted_consensus.predicted_consensus is not None:
                raise ValueError(
                    "nested consensus exceeds feature forecast depth"
                )
            if self.predicted_consensus.inputs != self.inputs:
                raise ValueError(
                    "predicted consensus feature information-set mismatch"
                )
        self._calendar_projection()
        self.to_json()

    def _calendar_projection(self) -> ForecastSnapshotV1:
        """Reuse frozen target/clock rules without pretending features are events."""
        return ForecastSnapshotV1(
            self.cutoff,
            self.target,
            self.inputs.calendar_inputs,
            self.model._calendar_model(),
            self.distribution,
            self.generated_at_ns,
            (
                None
                if self.predicted_consensus is None
                else self.predicted_consensus._calendar_projection()
            ),
            self.normalizer_id,
        )

    @property
    def snapshot_id(self) -> str:
        return str(self.to_dict()["id"])

    def verify_against(
        self, corpus: EconomicCalendarCorpusV1, store: VintageFeatureStoreV1
    ) -> None:
        self.inputs.verify_against(corpus, store)
        self.model.training_inputs.verify_against(corpus, store)
        if self.predicted_consensus is not None:
            self.predicted_consensus.verify_against(corpus, store)

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-feature-snapshot",
            {
                "cutoff": self.cutoff.to_dict(),
                "target": self.target.to_dict(),
                "inputs": self.inputs.to_dict(),
                "model": self.model.to_dict(),
                "distribution": self.distribution.to_dict(),
                "generated_at_ns": self.generated_at_ns,
                "normalizer_id": self.normalizer_id,
                "predicted_consensus": (
                    None
                    if self.predicted_consensus is None
                    else self.predicted_consensus.to_dict()
                ),
                "calendar_semantics_projection_id": self._calendar_projection().snapshot_id,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastFeatureSnapshotV1:
        nested = data["predicted_consensus"]
        if (
            nested is not None
            and _mapping(nested).get("predicted_consensus") is not None
        ):
            raise ValueError("nested consensus exceeds feature forecast depth")
        result = cls(
            ForecastCutoffV1.from_dict(_mapping(data["cutoff"])),
            ForecastTargetV1.from_dict(_mapping(data["target"])),
            ForecastFeatureInputsV1.from_dict(_mapping(data["inputs"])),
            ForecastFeatureModelV1.from_dict(_mapping(data["model"])),
            ForecastDistributionV1.from_dict(_mapping(data["distribution"])),
            data["generated_at_ns"],
            None if nested is None else cls.from_dict(_mapping(nested)),
            data["normalizer_id"],
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastFeatureSnapshotV1:
        return cls.from_dict(_load(text))


@dataclass(frozen=True, slots=True)
class ForecastFeatureScoreV1:
    """Full feature forecast plus first-release-safe replayable scoring evidence."""

    snapshot: ForecastFeatureSnapshotV1
    outcome_calendar_json: str
    scored_at_ns: int
    normalizer: ForecastScaleV1 | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, ForecastFeatureSnapshotV1):
            raise TypeError(
                "feature scoring refuses bare calendar forecast projections"
            )
        projection = self._calendar_score()
        object.__setattr__(
            self, "outcome_calendar_json", projection.outcome_calendar_json
        )
        self.to_json()

    def _calendar_score(self) -> ForecastScoreV1:
        return ForecastScoreV1(
            self.snapshot._calendar_projection().to_json(),
            self.outcome_calendar_json,
            self.scored_at_ns,
            self.normalizer,
        )

    @property
    def metrics(self) -> dict[str, JSONValue]:
        result = self._calendar_score().metrics
        if self.snapshot.predicted_consensus is not None:
            result["predicted_consensus_snapshot_id"] = (
                self.snapshot.predicted_consensus.snapshot_id
            )
        return result

    @property
    def score_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-feature-score",
            {
                "snapshot": self.snapshot.to_dict(),
                "outcome_calendar": _load(self.outcome_calendar_json),
                "scored_at_ns": self.scored_at_ns,
                "normalizer": (
                    None
                    if self.normalizer is None
                    else self.normalizer.to_dict()
                ),
                "metrics": self.metrics,
                "calendar_semantics_projection_id": self._calendar_score().score_id,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastFeatureScoreV1:
        result = cls(
            ForecastFeatureSnapshotV1.from_dict(_mapping(data["snapshot"])),
            _json(_mapping(data["outcome_calendar"])),
            data["scored_at_ns"],
            (
                None
                if data["normalizer"] is None
                else ForecastScaleV1.from_dict(_mapping(data["normalizer"]))
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastFeatureScoreV1:
        return cls.from_dict(_load(text))


# A fixed, inspectable reference algorithm, not a qualified model-bank engine.
_BASELINE_ALGORITHM = b"feature-baseline-v1: mean(all available training cells); prediction=0.5*training_mean+0.5*latest_available_inference_cell; refuse missing requested cells and mixed semantics"


def _baseline_implementation_sha256() -> str:
    """The installed executable module bytes, not the prose algorithm spec."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _verify_training_overlap(
    training: FeatureMatrixSnapshotV1, inference: FeatureMatrixSnapshotV1
) -> None:
    """An overlapping requested series/period cannot contradict trained history."""
    requested = {
        (column.feature_key, period)
        for column in inference.request.columns
        for period in inference.request.periods
    }
    training_scope = {
        (column.feature_key, period)
        for column in training.request.columns
        for period in training.request.periods
    }
    overlap = requested & training_scope
    cutoff = training.request.cutoff_at_ns
    old = tuple(
        item
        for item in training.observations
        if (item.definition.feature_key, item.period) in overlap
    )
    actual = tuple(
        item
        for item in inference.observations
        if (item.definition.feature_key, item.period) in overlap
        and item.available_at_ns <= cutoff
    )
    if actual != old:
        raise ValueError(
            "inference contradicts overlapping training vintage evidence"
        )
    old_schedules = tuple(
        item
        for item in training.schedules
        if (item.feature_key, item.period) in overlap
    )
    actual_schedules = tuple(
        item
        for item in inference.schedules
        if (item.feature_key, item.period) in overlap
        and item.known_at_ns <= cutoff
    )
    if actual_schedules != old_schedules:
        raise ValueError(
            "inference contradicts overlapping training schedule evidence"
        )


def _baseline_state(
    inputs: ForecastFeatureInputsV1, column: str
) -> dict[str, JSONValue]:
    cells = inputs.features.column(column)
    if any(
        item.status is not FeatureCellStatus.AVAILABLE or item.value is None
        for item in cells
    ):
        raise ValueError(
            "reference consumer refuses unavailable/stale/warmup feature cells"
        )
    semantics = {item.semantic_id for item in cells}
    if len(semantics) != 1:
        raise ValueError("reference consumer refuses mixed semantic eras")
    values = [item.value for item in cells if item.value is not None]
    specification = next(
        item for item in inputs.features.request.columns if item.name == column
    )
    definition = next(
        item.definition
        for item in inputs.features.observations
        if item.definition.semantic_id == cells[-1].semantic_id
    )
    try:
        mean = statistics.mean(values)
    except OverflowError as exc:
        raise ValueError("training mean must be finite") from exc
    return {
        "algorithm": _BASELINE_ALGORITHM.decode("ascii"),
        "algorithm_spec_sha256": hashlib.sha256(
            _BASELINE_ALGORITHM
        ).hexdigest(),
        "column": column,
        "transform": specification.transform.to_dict(),
        "training_mean": _finite(mean, "training mean"),
        "training_count": len(values),
        "semantic_id": cells[-1].semantic_id,
        "unit": cells[-1].unit,
        "scale": definition.scale if cells[-1].unit == definition.unit else 1.0,
        "base": definition.base if cells[-1].unit == definition.unit else None,
    }


def train_feature_baseline(
    inputs: ForecastFeatureInputsV1, *, column: str, trained_at_ns: int
) -> ForecastFeatureModelV1:
    """Execute a bounded reference fit using only a sealed as-of matrix."""
    if not isinstance(inputs, ForecastFeatureInputsV1):
        raise TypeError(
            "training requires sealed ex-ante feature inputs, never a convenience table"
        )
    return ForecastFeatureModelV1(
        "feature-reference-baseline",
        "1.0",
        _baseline_implementation_sha256(),
        _json(_baseline_state(inputs, column)),
        inputs,
        trained_at_ns,
    )


def forecast_feature_baseline(
    model: ForecastFeatureModelV1,
    inputs: ForecastFeatureInputsV1,
    *,
    cutoff: ForecastCutoffV1,
    target: ForecastTargetV1,
    generated_at_ns: int,
) -> ForecastFeatureSnapshotV1:
    """Execute prediction, verify fitted content, then seal full forecast lineage."""
    if not isinstance(model, ForecastFeatureModelV1) or not isinstance(
        inputs, ForecastFeatureInputsV1
    ):
        raise TypeError("prediction requires feature-aware model and inputs")
    state = _load(model.state_json)
    column = state.get("column")
    if not isinstance(column, str):
        raise ValueError("reference model column is missing")
    expected = train_feature_baseline(
        model.training_inputs, column=column, trained_at_ns=model.trained_at_ns
    )
    if expected != model:
        raise ValueError(
            "reference model state differs from exact feature fit replay"
        )
    current = _baseline_state(inputs, column)
    if (
        current["semantic_id"] != state["semantic_id"]
        or current["transform"] != state["transform"]
        or current["unit"] != target.unit
        or current["scale"] != target.scale
        or current["base"] != target.base
    ):
        raise ValueError("training/inference/target feature semantics differ")
    latest = inputs.features.column(column)[-1].value
    mean = state["training_mean"]
    if not isinstance(mean, (int, float)) or latest is None:
        raise ValueError("reference prediction lacks finite feature values")
    point = _finite(mean / 2 + latest / 2, "reference prediction")
    return ForecastFeatureSnapshotV1(
        cutoff,
        target,
        inputs,
        model,
        ForecastDistributionV1(((point, 1.0),)),
        generated_at_ns,
    )
