"""Replayable first-release, consensus, revision and surprise score evidence."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
    EconomicReleaseStage,
)
from histdatacom.runtime_contracts import JSONValue

from .contracts import (
    ConsensusTiming,
    ForecastSnapshotV1,
    ForecastTargetKind,
    RevisionMeasure,
    SurpriseReference,
    _array,
    _finite,
    _has_unavailable_vintages,
    _json,
    _load,
    _mapping,
    _ns,
    _seal,
    _text,
    _verify,
    first_release_actual,
)


@dataclass(frozen=True, slots=True)
class ForecastScaleV1:
    """A named, ex-ante denominator; not a silently fitted test-set scale.

    The denominator is an explicit scoring parameter. Its estimation belongs
    to the feature/training layer; its method, establishment time and exact
    input release evidence are retained here for audit and score replay.
    """

    name: str
    denominator: float
    unit: str
    established_at_ns: int
    methodology: str
    evidence_release_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("name", "unit", "methodology"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        number = _finite(self.denominator, "normalization denominator")
        if number <= 0:
            raise ValueError("normalization denominator must be positive")
        object.__setattr__(self, "denominator", number)
        _ns(self.established_at_ns, "established_at_ns")
        identities = tuple(
            _text(item, "scale evidence identity")
            for item in self.evidence_release_ids
        )
        if len(identities) > 10_000 or len(set(identities)) != len(identities):
            raise ValueError(
                "scale evidence has duplicate or excess identities"
            )
        object.__setattr__(self, "evidence_release_ids", identities)

    def validate_snapshot(self, snapshot: ForecastSnapshotV1) -> None:
        if snapshot.normalizer_id != self.to_dict()["id"]:
            raise ValueError(
                "normalization was not committed in forecast snapshot"
            )
        if self.unit != snapshot.target.unit:
            raise ValueError("normalization unit differs from target")
        if self.established_at_ns > snapshot.generated_at_ns:
            raise ValueError(
                "normalization denominator follows forecast generation"
            )
        releases = {
            item.release_id: item for item in snapshot.inputs.calendar.releases
        }
        for identity in self.evidence_release_ids:
            item = releases.get(identity)
            if item is None or item.available_at_ns > self.established_at_ns:
                raise ValueError("scale evidence is not available ex ante")

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-scale",
            {
                "name": self.name,
                "denominator": self.denominator,
                "unit": self.unit,
                "established_at_ns": self.established_at_ns,
                "methodology": self.methodology,
                "evidence_release_ids": list(self.evidence_release_ids),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastScaleV1:
        result = cls(
            name=data["name"],
            denominator=data["denominator"],
            unit=data["unit"],
            established_at_ns=data["established_at_ns"],
            methodology=data["methodology"],
            evidence_release_ids=tuple(_array(data["evidence_release_ids"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastScoreV1:
    """A sealed snapshot plus independently sealed outcome-vintage evidence."""

    snapshot_json: str
    outcome_calendar_json: str
    scored_at_ns: int
    normalizer: ForecastScaleV1 | None = None

    def __post_init__(self) -> None:
        snapshot = ForecastSnapshotV1.from_json(self.snapshot_json)
        corpus = EconomicCalendarCorpusV1.from_dict(
            _load(self.outcome_calendar_json)
        )
        object.__setattr__(self, "snapshot_json", snapshot.to_json())
        object.__setattr__(
            self, "outcome_calendar_json", _json(corpus.to_dict())
        )
        _ns(self.scored_at_ns, "scored_at_ns")
        if self.scored_at_ns < snapshot.cutoff.cutoff_at_ns:
            raise ValueError("scoring precedes forecast cutoff")
        if _has_unavailable_vintages(corpus, self.scored_at_ns):
            raise ValueError("score evidence is unavailable at scoring time")
        snapshot.inputs.verify_against(corpus)
        if self.normalizer is not None:
            if not isinstance(self.normalizer, ForecastScaleV1):
                raise TypeError("normalizer requires its v1 contract")
            self.normalizer.validate_snapshot(snapshot)
        elif snapshot.normalizer_id is not None:
            raise ValueError("snapshot's committed normalization is missing")
        self.to_json()

    @property
    def snapshot(self) -> ForecastSnapshotV1:
        return ForecastSnapshotV1.from_json(self.snapshot_json)

    @property
    def outcome_calendar(self) -> EconomicCalendarCorpusV1:
        return EconomicCalendarCorpusV1.from_json(self.outcome_calendar_json)

    @property
    def score_id(self) -> str:
        return str(self.to_dict()["id"])

    @property
    def metrics(self) -> dict[str, JSONValue]:
        """Recompute every score from embedded evidence, never stored results."""
        snapshot = self.snapshot
        target = snapshot.target
        corpus = self.outcome_calendar
        cutoff = snapshot.cutoff.cutoff_at_ns
        point = snapshot.distribution.point
        initial = None
        target_actuals = [
            item
            for item in corpus.releases
            if item.logical_event_key == target.logical_event_key
            and item.actual_value is not None
        ]
        if target_actuals:
            initial = first_release_actual(corpus, target)
            if cutoff >= initial.available_at_ns:
                raise ValueError(
                    "forecast cutoff is not before first-release availability"
                )
        if target.kind is not ForecastTargetKind.CONSENSUS and initial is None:
            raise ValueError("score requires the first-release actual")
        reference = target.consensus_reference
        reference_value = None
        reference_id = None
        if reference is not None:
            if self.scored_at_ns < reference.target_at_ns:
                raise ValueError("consensus observation target has not arrived")
            selected = reference.select(
                (
                    corpus
                    if reference.timing is ConsensusTiming.FUTURE_EVOLUTION
                    else snapshot.inputs.calendar
                ),
                target.logical_event_key,
            )
            if initial is not None and (
                selected.available_at_ns >= initial.available_at_ns
                or reference.target_at_ns >= initial.available_at_ns
            ):
                raise ValueError("reference consensus is not pre-release")
            reference_value = selected.value
            reference_id = selected.forecast_id
        revision_id = None
        predicted_id = None
        if target.kind is ForecastTargetKind.CONSENSUS:
            if reference_value is None:
                raise ValueError("consensus score lacks reference value")
            observed = reference_value
        else:
            if initial is None or initial.actual_value is None:
                raise ValueError("first-release actual missing")
            observed = initial.actual_value
            if target.kind is ForecastTargetKind.REVISION:
                revisions = [
                    item
                    for item in corpus.releases
                    if item.logical_event_key == target.logical_event_key
                    and item.revision_sequence == target.revision_sequence
                    and item.stage is EconomicReleaseStage.REVISION
                    and item.actual_value is not None
                ]
                if len(revisions) != 1:
                    raise ValueError(
                        "requested revision vintage is unavailable"
                    )
                revision = revisions[0]
                target.verify_release(revision)
                if revision.actual_value is None:
                    raise ValueError("revision has no actual value")
                revision_id = revision.release_id
                observed = revision.actual_value
                if target.revision_measure is RevisionMeasure.CHANGE_FROM_FIRST:
                    observed -= initial.actual_value
            elif target.kind is ForecastTargetKind.SURPRISE:
                if (
                    target.surprise_reference
                    is SurpriseReference.PREDICTED_CONSENSUS
                ):
                    predicted = snapshot.predicted_consensus
                    if predicted is None:
                        raise ValueError("predicted consensus is missing")
                    reference_value = predicted.distribution.point
                    predicted_id = predicted.snapshot_id
                if reference_value is None:
                    raise ValueError("surprise score lacks reference value")
                observed -= reference_value
        observed = _finite(observed, "observed scoring target")
        error = _finite(point - observed, "forecast-minus-observed error")
        absolute = abs(error)
        squared = _finite(error * error, "squared error")
        normalized_absolute = None
        normalized_squared = None
        if self.normalizer is not None:
            normalized_absolute = _finite(
                absolute / self.normalizer.denominator,
                "normalized absolute error",
            )
            normalized_squared = _finite(
                normalized_absolute * normalized_absolute,
                "normalized squared error",
            )
        value_added = None
        if (
            target.kind is ForecastTargetKind.FIRST_RELEASE_ACTUAL
            and reference_value is not None
        ):
            value_added = _finite(
                abs(observed - reference_value) - absolute,
                "independent value added",
            )
        return {
            "target_kind": target.kind.value,
            "horizon": snapshot.cutoff.horizon.value,
            "cutoff_at_ns": cutoff,
            "forecast_point": point,
            "observed_target": observed,
            "forecast_minus_observed": error,
            "absolute_error": absolute,
            "squared_error": squared,
            "normalized_absolute_error": normalized_absolute,
            "normalized_squared_error": normalized_squared,
            "reference_consensus_value": reference_value,
            "reference_consensus_id": reference_id,
            "predicted_consensus_snapshot_id": predicted_id,
            "first_release_id": None if initial is None else initial.release_id,
            "first_release_available_at_ns": (
                None if initial is None else initial.available_at_ns
            ),
            "first_release_published_at_ns": (
                None if initial is None else initial.released_at_ns
            ),
            "revision_release_id": revision_id,
            "consensus_replication_error": (
                absolute
                if target.kind is ForecastTargetKind.CONSENSUS
                else None
            ),
            "independent_value_added": value_added,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-score",
            {
                "snapshot": _load(self.snapshot_json),
                "outcome_calendar": _load(self.outcome_calendar_json),
                "scored_at_ns": self.scored_at_ns,
                "normalizer": (
                    None
                    if self.normalizer is None
                    else self.normalizer.to_dict()
                ),
                "metrics": self.metrics,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastScoreV1:
        result = cls(
            snapshot_json=_json(_mapping(data["snapshot"])),
            outcome_calendar_json=_json(_mapping(data["outcome_calendar"])),
            scored_at_ns=data["scored_at_ns"],
            normalizer=(
                None
                if data["normalizer"] is None
                else ForecastScaleV1.from_dict(_mapping(data["normalizer"]))
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastScoreV1:
        return cls.from_dict(_load(text))


def _snapshot_task_key(snapshot: ForecastSnapshotV1) -> tuple[object, ...]:
    target = snapshot.target
    reference = target.consensus_reference
    predicted = snapshot.predicted_consensus
    return (
        target.kind,
        snapshot.cutoff.horizon,
        snapshot.cutoff.final_pre_release_lead_ns,
        target.series_id,
        target.unit,
        target.scale,
        target.base,
        target.revision_sequence,
        target.revision_measure,
        target.surprise_reference,
        snapshot.distribution.point_statistic,
        snapshot.model.name,
        snapshot.model.version,
        snapshot.model.implementation_sha256,
        (
            None
            if reference is None
            else (
                reference.source_name,
                reference.adapter_name,
                reference.scope,
                reference.statistic,
                reference.timing,
                reference.target_at_ns - snapshot.cutoff.cutoff_at_ns,
            )
        ),
        (None if predicted is None else _snapshot_task_key(predicted)),
    )


def _task_key(score: ForecastScoreV1) -> tuple[object, ...]:
    normalizer = score.normalizer
    return (
        _snapshot_task_key(score.snapshot),
        (
            None
            if normalizer is None
            else (
                normalizer.name,
                normalizer.unit,
                normalizer.methodology,
            )
        ),
    )


@dataclass(frozen=True, slots=True)
class ForecastScoreReportV1:
    """One comparable task's metrics with every constituent score embedded."""

    scores: tuple[ForecastScoreV1, ...]

    def __post_init__(self) -> None:
        scores = tuple(self.scores)
        if not 1 <= len(scores) <= 10_000 or any(
            not isinstance(item, ForecastScoreV1) for item in scores
        ):
            raise ValueError("report requires 1..10000 score contracts")
        snapshots = [item.snapshot.snapshot_id for item in scores]
        if len(set(snapshots)) != len(snapshots):
            raise ValueError("duplicate snapshot cannot be counted twice")
        events = [item.snapshot.target.logical_event_key for item in scores]
        if len(set(events)) != len(events):
            raise ValueError(
                "report contains multiple predictions for one event"
            )
        if len({_task_key(item) for item in scores}) != 1:
            raise ValueError(
                "cannot aggregate mismatched target/horizon/model/scale tasks"
            )
        object.__setattr__(
            self,
            "scores",
            tuple(sorted(scores, key=lambda item: item.score_id)),
        )
        self.to_json()

    @property
    def metrics(self) -> dict[str, JSONValue]:
        rows = [score.metrics for score in self.scores]

        def values(name: str) -> list[float]:
            numbers: list[float] = []
            for row in rows:
                value = row[name]
                if isinstance(value, (int, float)):
                    numbers.append(float(value))
            return numbers

        def mean(name: str) -> float:
            numbers = values(name)
            # Divide first: summing several large finite errors needn't overflow.
            return _finite(
                math.fsum(item / len(numbers) for item in numbers), name
            )

        normalized = all(
            row["normalized_absolute_error"] is not None for row in rows
        )
        consensus = all(
            row["consensus_replication_error"] is not None for row in rows
        )
        value_added = all(
            row["independent_value_added"] is not None for row in rows
        )
        return {
            "count": len(rows),
            "mae": mean("absolute_error"),
            "rmse": math.sqrt(mean("squared_error")),
            "median_absolute_error": statistics.median(
                values("absolute_error")
            ),
            "normalized_mae": (
                mean("normalized_absolute_error") if normalized else None
            ),
            "normalized_rmse": (
                math.sqrt(mean("normalized_squared_error"))
                if normalized
                else None
            ),
            "normalized_median_absolute_error": (
                statistics.median(values("normalized_absolute_error"))
                if normalized
                else None
            ),
            "mean_consensus_replication_error": (
                mean("consensus_replication_error") if consensus else None
            ),
            "mean_independent_value_added": (
                mean("independent_value_added") if value_added else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-score-report",
            {
                "scores": [item.to_dict() for item in self.scores],
                "metrics": self.metrics,
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_json(cls, text: str) -> ForecastScoreReportV1:
        data = _load(text)
        result = cls(
            tuple(
                ForecastScoreV1.from_dict(_mapping(item))
                for item in _array(data["scores"])
            )
        )
        _verify(data, result.to_dict())
        return result
