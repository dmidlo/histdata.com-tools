"""Immutable, point-in-time economic forecast contracts (no model runner)."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from itertools import pairwise
from typing import Any, cast

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
    EconomicCalendarForecastV1,
    EconomicCalendarReleaseV1,
    EconomicForecastKind,
    EconomicForecastScope,
    EconomicForecastStatistic,
    EconomicReleaseStage,
    EconomicReleaseStatus,
)
from histdatacom.runtime_contracts import JSONValue

SECOND_NS = 1_000_000_000
HOUR_NS = 3_600 * SECOND_NS
DAY_NS = 24 * HOUR_NS
MAX_FORECAST_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_FORECAST_JSON_DEPTH = 64
MAX_DISTRIBUTION_POINTS = 10_000
SCHEMA_VERSION = "1.0"


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 4096:
        raise ValueError(f"{name} requires bounded nonempty text")
    return value.strip()


def _ns(value: int, name: str) -> int:
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise ValueError(f"{name} must be an int64 timestamp")
    return value


def _finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in value
    ):
        raise ValueError("expected a JSON object")
    return cast(Mapping[str, Any], value)


def _array(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError("expected a JSON array")
    return value


def _validate_json_depth(value: object) -> None:
    """Bound the expanded envelope, including containers and scalar leaves.

    The root is depth zero. Iteration also refuses cycles/deep Python values
    before recursive JSON encoding, and tuples count like the arrays they
    become on the wire. Opaque strings are not recursively interpreted.
    """
    pending: list[tuple[object, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > MAX_FORECAST_JSON_DEPTH:
            raise ValueError("forecast JSON exceeds nesting bounds")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend((child, depth + 1) for child in item)


def _json(value: Mapping[str, Any]) -> str:
    body = dict(value)
    _validate_json_depth(body)
    try:
        result = str(canonical_contract_json(body))
    except (OverflowError, RecursionError) as exc:
        raise ValueError(
            "forecast JSON exceeds numerical or nesting bounds"
        ) from exc
    if len(result.encode("utf-8")) > MAX_FORECAST_ARTIFACT_BYTES:
        raise ValueError("forecast artifact exceeds byte bound")
    return result


def _load(text: str) -> Mapping[str, Any]:
    if not isinstance(text, str):
        raise TypeError("expected JSON text")
    if len(text.encode("utf-8")) > MAX_FORECAST_ARTIFACT_BYTES:
        raise ValueError("forecast artifact exceeds byte bound")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        result = json.loads(text, object_pairs_hook=pairs)
    except (OverflowError, RecursionError) as exc:
        raise ValueError(
            "forecast JSON exceeds numerical or nesting bounds"
        ) from exc
    _validate_json_depth(result)
    return _mapping(result)


def _digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _seal(kind: str, payload: Mapping[str, Any]) -> dict[str, JSONValue]:
    body = {"schema_version": SCHEMA_VERSION, "contract_type": kind, **payload}
    sealed = cast(
        dict[str, JSONValue], {**body, "id": f"{kind}:{_digest(body)}"}
    )
    # The digest body fitting is insufficient: its identity field also consumes
    # the final wire byte budget. Every public sealed object has the same bounds.
    _json(sealed)
    return sealed


def _verify(data: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    # Exact comparison rejects unknown fields, missing identities, and forged
    # derived values as well as altered nested evidence.
    if _json(data) != _json(expected):
        raise ValueError("forecast contract identity or payload mismatch")


def _has_unavailable_vintages(
    corpus: EconomicCalendarCorpusV1, as_of_ns: int
) -> bool:
    """Check both typed vintage streams without erasing their element types."""
    return any(
        release.available_at_ns > as_of_ns for release in corpus.releases
    ) or any(
        forecast.available_at_ns > as_of_ns for forecast in corpus.forecasts
    )


class ForecastHorizon(str, Enum):
    """Separate forecasting tasks, anchored to a schedule known at cutoff."""

    T30D = "T-30d"
    T7D = "T-7d"
    T1D = "T-1d"
    T1H = "T-1h"
    FINAL_PRE_RELEASE = "final-pre-release"


class ForecastTargetKind(str, Enum):
    CONSENSUS = "consensus"
    FIRST_RELEASE_ACTUAL = "first-release-actual"
    REVISION = "revision"
    SURPRISE = "surprise"


class ConsensusTiming(str, Enum):
    AT_CUTOFF = "at-cutoff"
    FUTURE_EVOLUTION = "future-consensus-evolution"


class SurpriseReference(str, Enum):
    OBSERVED_CONSENSUS = "observed-consensus"
    PREDICTED_CONSENSUS = "predicted-consensus"


class RevisionMeasure(str, Enum):
    LEVEL = "revised-level"
    CHANGE_FROM_FIRST = "revision-minus-first-release"


class PointStatistic(str, Enum):
    MEAN = "mean"
    MEDIAN = "lower-weighted-median"


@dataclass(frozen=True, slots=True)
class ForecastCutoffV1:
    """A cutoff bound to an immutable, previously observable schedule."""

    horizon: ForecastHorizon
    cutoff_at_ns: int
    scheduled_release_at_ns: int
    schedule_release_id: str
    final_pre_release_lead_ns: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "horizon", ForecastHorizon(self.horizon))
        _ns(self.cutoff_at_ns, "cutoff_at_ns")
        _ns(self.scheduled_release_at_ns, "scheduled_release_at_ns")
        object.__setattr__(
            self,
            "schedule_release_id",
            _text(self.schedule_release_id, "schedule_release_id"),
        )
        leads = {
            ForecastHorizon.T30D: 30 * DAY_NS,
            ForecastHorizon.T7D: 7 * DAY_NS,
            ForecastHorizon.T1D: DAY_NS,
            ForecastHorizon.T1H: HOUR_NS,
        }
        if self.horizon is ForecastHorizon.FINAL_PRE_RELEASE:
            lead = self.final_pre_release_lead_ns
            if type(lead) is not int or not 0 < lead <= HOUR_NS:
                raise ValueError(
                    "final-pre-release requires explicit lead <=1h"
                )
        else:
            if self.final_pre_release_lead_ns is not None:
                raise ValueError("fixed horizon cannot have a final lead")
            lead = leads[self.horizon]
        if self.scheduled_release_at_ns - self.cutoff_at_ns != lead:
            raise ValueError("horizon/cutoff mismatch")

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-cutoff",
            {
                "horizon": self.horizon.value,
                "cutoff_at_ns": self.cutoff_at_ns,
                "scheduled_release_at_ns": self.scheduled_release_at_ns,
                "schedule_release_id": self.schedule_release_id,
                "final_pre_release_lead_ns": self.final_pre_release_lead_ns,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastCutoffV1:
        result = cls(
            horizon=ForecastHorizon(data["horizon"]),
            cutoff_at_ns=data["cutoff_at_ns"],
            scheduled_release_at_ns=data["scheduled_release_at_ns"],
            schedule_release_id=data["schedule_release_id"],
            final_pre_release_lead_ns=data["final_pre_release_lead_ns"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastInputsV1:
    """Exact normalized vintage bytes, not a mutable 'latest data' locator.

    JSON text is intentional: calendar provenance contains dictionaries. A
    fresh decoded corpus is returned on every access, preserving deep
    immutability without changing the existing calendar contracts.
    """

    cutoff_at_ns: int
    calendar_json: str

    def __post_init__(self) -> None:
        _ns(self.cutoff_at_ns, "cutoff_at_ns")
        corpus = EconomicCalendarCorpusV1.from_dict(_load(self.calendar_json))
        if _has_unavailable_vintages(corpus, self.cutoff_at_ns):
            raise ValueError("input vintage is unavailable at cutoff")
        object.__setattr__(self, "calendar_json", _json(corpus.to_dict()))
        self.to_dict()

    @property
    def calendar(self) -> EconomicCalendarCorpusV1:
        return EconomicCalendarCorpusV1.from_json(self.calendar_json)

    @property
    def information_set_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-inputs",
            {
                "cutoff_at_ns": self.cutoff_at_ns,
                "calendar": _load(self.calendar_json),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastInputsV1:
        result = cls(data["cutoff_at_ns"], _json(_mapping(data["calendar"])))
        _verify(data, result.to_dict())
        return result

    def verify_against(self, corpus: EconomicCalendarCorpusV1) -> None:
        """Replay exact selected inputs; extra later vintages are harmless."""
        releases = {item.release_id: item for item in corpus.releases}
        forecasts = {item.forecast_id: item for item in corpus.forecasts}
        for item in self.calendar.releases:
            if releases.get(item.release_id) != item:
                raise ValueError(
                    "exact input release vintage missing or changed"
                )
        for forecast in self.calendar.forecasts:
            if forecasts.get(forecast.forecast_id) != forecast:
                raise ValueError(
                    "exact input forecast vintage missing or changed"
                )


def capture_forecast_inputs(
    corpus: EconomicCalendarCorpusV1, *, cutoff_at_ns: int
) -> ForecastInputsV1:
    """Freeze all admissible vintages, preserving complete revision chains."""
    _ns(cutoff_at_ns, "cutoff_at_ns")
    releases = tuple(
        item for item in corpus.releases if item.available_at_ns <= cutoff_at_ns
    )
    keys = {item.logical_event_key for item in releases}
    captured = EconomicCalendarCorpusV1(
        coverage_start_ns=corpus.coverage_start_ns,
        coverage_end_ns=corpus.coverage_end_ns,
        complete=False,
        releases=releases,
        forecasts=tuple(
            item
            for item in corpus.forecasts
            if item.available_at_ns <= cutoff_at_ns
            and item.logical_event_key in keys
        ),
        limitations=corpus.limitations
        + ("Cutoff-filtered vintages; not an assertion of complete coverage.",),
    )
    return ForecastInputsV1(cutoff_at_ns, captured.to_json())


@dataclass(frozen=True, slots=True)
class ConsensusReferenceV1:
    """Named source/statistic and observation time, never generic 'consensus'."""

    source_name: str
    adapter_name: str
    scope: EconomicForecastScope
    statistic: EconomicForecastStatistic
    target_at_ns: int
    timing: ConsensusTiming = ConsensusTiming.AT_CUTOFF

    def __post_init__(self) -> None:
        for name in ("source_name", "adapter_name"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        object.__setattr__(self, "scope", EconomicForecastScope(self.scope))
        object.__setattr__(
            self, "statistic", EconomicForecastStatistic(self.statistic)
        )
        object.__setattr__(self, "timing", ConsensusTiming(self.timing))
        _ns(self.target_at_ns, "consensus target_at_ns")
        if not self.scope.calendar_style_eligible:
            raise ValueError("reference does not support event consensus")
        if self.statistic not in {
            EconomicForecastStatistic.MEAN,
            EconomicForecastStatistic.MEDIAN,
        }:
            raise ValueError("consensus reference requires mean or median")

    def select(
        self, corpus: EconomicCalendarCorpusV1, logical_event_key: str
    ) -> EconomicCalendarForecastV1:
        values = [
            item
            for item in corpus.forecasts
            if item.logical_event_key == logical_event_key
            and item.kind is EconomicForecastKind.OBSERVED_CONSENSUS
            and item.scope is self.scope
            and item.statistic is self.statistic
            and item.source.name == self.source_name
            and item.source.adapter_name == self.adapter_name
            and item.available_at_ns <= self.target_at_ns
        ]
        if not values:
            raise ValueError("no admissible reference consensus vintage")
        latest = max(item.available_at_ns for item in values)
        selected = [item for item in values if item.available_at_ns == latest]
        if len(selected) != 1:
            raise ValueError("ambiguous reference consensus at target time")
        return selected[0]

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-consensus-reference",
            {
                "source_name": self.source_name,
                "adapter_name": self.adapter_name,
                "scope": self.scope.value,
                "statistic": self.statistic.value,
                "target_at_ns": self.target_at_ns,
                "timing": self.timing.value,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ConsensusReferenceV1:
        result = cls(
            source_name=data["source_name"],
            adapter_name=data["adapter_name"],
            scope=EconomicForecastScope(data["scope"]),
            statistic=EconomicForecastStatistic(data["statistic"]),
            target_at_ns=data["target_at_ns"],
            timing=ConsensusTiming(data["timing"]),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastTargetV1:
    """Scientific target identity; consensus, actual and revisions are disjoint."""

    kind: ForecastTargetKind
    logical_event_key: str
    series_id: str
    indicator_id: str
    reference_period: str
    unit: str
    scale: float
    base: str | None
    consensus_reference: ConsensusReferenceV1 | None = None
    revision_sequence: int | None = None
    revision_measure: RevisionMeasure | None = None
    surprise_reference: SurpriseReference | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ForecastTargetKind(self.kind))
        for name in (
            "logical_event_key",
            "series_id",
            "indicator_id",
            "reference_period",
            "unit",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("target scale must be positive")
        object.__setattr__(self, "scale", scale)
        if self.base is not None:
            object.__setattr__(self, "base", _text(self.base, "base"))
        if self.consensus_reference is not None and not isinstance(
            self.consensus_reference, ConsensusReferenceV1
        ):
            raise TypeError("consensus_reference requires its v1 contract")
        if self.kind is ForecastTargetKind.REVISION:
            if (
                type(self.revision_sequence) is not int
                or self.revision_sequence < 1
            ):
                raise ValueError(
                    "revision target requires a later vintage sequence"
                )
            object.__setattr__(
                self, "revision_measure", RevisionMeasure(self.revision_measure)
            )
            if self.consensus_reference is not None:
                raise ValueError("revision target cannot use actual consensus")
        elif (
            self.revision_sequence is not None
            or self.revision_measure is not None
        ):
            raise ValueError("non-revision target has revision fields")
        if self.kind is ForecastTargetKind.SURPRISE:
            object.__setattr__(
                self,
                "surprise_reference",
                SurpriseReference(self.surprise_reference),
            )
            observed = (
                self.surprise_reference is SurpriseReference.OBSERVED_CONSENSUS
            )
            if observed != (self.consensus_reference is not None):
                raise ValueError(
                    "surprise reference semantics are inconsistent"
                )
        elif self.surprise_reference is not None:
            raise ValueError("non-surprise target has a surprise reference")
        if (
            self.kind is ForecastTargetKind.CONSENSUS
            and self.consensus_reference is None
        ):
            raise ValueError(
                "consensus target requires explicit reference semantics"
            )

    def verify_release(self, release: EconomicCalendarReleaseV1) -> None:
        for name in (
            "logical_event_key",
            "series_id",
            "indicator_id",
            "reference_period",
            "unit",
            "scale",
            "base",
        ):
            if getattr(self, name) != getattr(release, name):
                raise ValueError(f"forecast target/release {name} mismatch")

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-target",
            {
                "kind": self.kind.value,
                "logical_event_key": self.logical_event_key,
                "series_id": self.series_id,
                "indicator_id": self.indicator_id,
                "reference_period": self.reference_period,
                "unit": self.unit,
                "scale": self.scale,
                "base": self.base,
                "consensus_reference": (
                    None
                    if self.consensus_reference is None
                    else self.consensus_reference.to_dict()
                ),
                "revision_sequence": self.revision_sequence,
                "revision_measure": (
                    None
                    if self.revision_measure is None
                    else self.revision_measure.value
                ),
                "surprise_reference": (
                    None
                    if self.surprise_reference is None
                    else self.surprise_reference.value
                ),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastTargetV1:
        result = cls(
            kind=ForecastTargetKind(data["kind"]),
            logical_event_key=data["logical_event_key"],
            series_id=data["series_id"],
            indicator_id=data["indicator_id"],
            reference_period=data["reference_period"],
            unit=data["unit"],
            scale=data["scale"],
            base=data["base"],
            consensus_reference=(
                None
                if data["consensus_reference"] is None
                else ConsensusReferenceV1.from_dict(
                    _mapping(data["consensus_reference"])
                )
            ),
            revision_sequence=data["revision_sequence"],
            revision_measure=data["revision_measure"],
            surprise_reference=data["surprise_reference"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastDistributionV1:
    """Finite probability distribution; point score statistic is explicit."""

    support: tuple[tuple[float, float], ...]
    point_statistic: PointStatistic = PointStatistic.MEAN

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "point_statistic", PointStatistic(self.point_statistic)
        )
        if not isinstance(self.support, (tuple, list)):
            raise TypeError("distribution support must be a sequence of pairs")
        if not 1 <= len(self.support) <= MAX_DISTRIBUTION_POINTS:
            raise ValueError("distribution point count outside bounds")
        if any(
            not isinstance(point, (tuple, list)) or len(point) != 2
            for point in self.support
        ):
            raise ValueError(
                "distribution support requires value/probability pairs"
            )
        points = tuple(
            (_finite(value, "support value"), _finite(mass, "probability"))
            for value, mass in self.support
        )
        if any(mass <= 0 or mass > 1 for _, mass in points):
            raise ValueError("distribution probabilities must be positive")
        if any(a[0] >= b[0] for a, b in pairwise(points)):
            raise ValueError("distribution support must be strictly increasing")
        if not math.isclose(
            math.fsum(mass for _, mass in points), 1.0, rel_tol=0, abs_tol=1e-12
        ):
            raise ValueError("distribution probabilities must sum to one")
        object.__setattr__(self, "support", points)
        _finite(self.point, "distribution point forecast")

    @property
    def point(self) -> float:
        if self.point_statistic is PointStatistic.MEAN:
            try:
                return math.fsum(value * mass for value, mass in self.support)
            except OverflowError as exc:
                raise ValueError("distribution mean must be finite") from exc
        cumulative = 0.0
        for value, mass in self.support:
            cumulative += mass
            if cumulative >= 0.5:
                return value
        raise ValueError("distribution has no median")

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-distribution",
            {
                "support": [[value, mass] for value, mass in self.support],
                "point_statistic": self.point_statistic.value,
                "point": self.point,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastDistributionV1:
        support = _array(data["support"])
        if not 1 <= len(support) <= MAX_DISTRIBUTION_POINTS:
            raise ValueError("distribution point count outside bounds")
        if any(
            not isinstance(point, list) or len(point) != 2 for point in support
        ):
            raise ValueError(
                "distribution support requires value/probability pairs"
            )
        result = cls(
            tuple((point[0], point[1]) for point in support),
            PointStatistic(data["point_statistic"]),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastModelIdentityV1:
    """Content-bound model/ensemble state and its exact training inputs.

    This is a replay contract, not an implementation of training or execution.
    Model state (including ensemble members/weights when applicable) is sealed
    canonical JSON; the implementation digest identifies external code bytes.
    """

    name: str
    version: str
    implementation_sha256: str
    state_json: str
    training_inputs: ForecastInputsV1
    trained_at_ns: int

    def __post_init__(self) -> None:
        for name in ("name", "version"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        digest = self.implementation_sha256
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
        ):
            raise ValueError("implementation_sha256 requires a SHA-256 digest")
        state = _load(self.state_json)
        if not state:
            raise ValueError("model state cannot be empty")
        object.__setattr__(self, "state_json", _json(state))
        if not isinstance(self.training_inputs, ForecastInputsV1):
            raise TypeError("model requires exact training inputs")
        _ns(self.trained_at_ns, "trained_at_ns")
        if self.training_inputs.cutoff_at_ns > self.trained_at_ns:
            raise ValueError("model training inputs follow training time")
        self.to_dict()

    @property
    def model_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-model",
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
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastModelIdentityV1:
        result = cls(
            name=data["name"],
            version=data["version"],
            implementation_sha256=data["implementation_sha256"],
            state_json=_json(_mapping(data["state"])),
            training_inputs=ForecastInputsV1.from_dict(
                _mapping(data["training_inputs"])
            ),
            trained_at_ns=data["trained_at_ns"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastSnapshotV1:
    """One immutable observation unit at one target and cutoff/horizon."""

    cutoff: ForecastCutoffV1
    target: ForecastTargetV1
    inputs: ForecastInputsV1
    model: ForecastModelIdentityV1
    distribution: ForecastDistributionV1
    generated_at_ns: int
    predicted_consensus: ForecastSnapshotV1 | None = None
    normalizer_id: str | None = None

    def __post_init__(self) -> None:
        for name, contract in (
            ("cutoff", ForecastCutoffV1),
            ("target", ForecastTargetV1),
            ("inputs", ForecastInputsV1),
            ("model", ForecastModelIdentityV1),
            ("distribution", ForecastDistributionV1),
        ):
            if not isinstance(getattr(self, name), contract):
                raise TypeError(f"{name} requires its v1 contract")
        _ns(self.generated_at_ns, "generated_at_ns")
        cutoff = self.cutoff.cutoff_at_ns
        if self.inputs.cutoff_at_ns != cutoff:
            raise ValueError("input information-set/cutoff mismatch")
        if not self.model.trained_at_ns <= self.generated_at_ns <= cutoff:
            raise ValueError("model/forecast generation follows cutoff")
        if _has_unavailable_vintages(
            self.inputs.calendar, self.generated_at_ns
        ):
            raise ValueError("input vintage follows forecast generation")
        if self.normalizer_id is not None:
            identity = _text(self.normalizer_id, "normalizer_id")
            prefix, separator, digest = identity.partition(":")
            if (
                prefix != "forecast-scale"
                or separator != ":"
                or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
            ):
                raise ValueError("normalizer_id must identify a sealed scale")
            object.__setattr__(self, "normalizer_id", identity)
        self.model.training_inputs.verify_against(self.inputs.calendar)
        vintages = [
            item
            for item in self.inputs.calendar.releases
            if item.logical_event_key == self.target.logical_event_key
        ]
        if not vintages:
            raise ValueError("target has no observable schedule")
        schedule = max(vintages, key=lambda item: item.revision_sequence)
        self.target.verify_release(schedule)
        if (
            schedule.release_id != self.cutoff.schedule_release_id
            or schedule.scheduled_for_ns != self.cutoff.scheduled_release_at_ns
        ):
            raise ValueError("cutoff is not anchored to latest known schedule")
        if schedule.actual_value is not None or schedule.status not in {
            EconomicReleaseStatus.SCHEDULED,
            EconomicReleaseStatus.RESCHEDULED,
            EconomicReleaseStatus.DELAYED,
            EconomicReleaseStatus.TENTATIVE,
        }:
            raise ValueError("target is not an unreleased scheduled event")
        if any(item.actual_value is not None for item in vintages):
            raise ValueError("first release is already observable at cutoff")
        reference = self.target.consensus_reference
        if reference is not None:
            if reference.timing is ConsensusTiming.AT_CUTOFF:
                if reference.target_at_ns != cutoff:
                    raise ValueError("reference consensus/cutoff mismatch")
                reference.select(
                    self.inputs.calendar, self.target.logical_event_key
                )
            elif (
                self.target.kind is not ForecastTargetKind.CONSENSUS
                or not cutoff
                < reference.target_at_ns
                < self.cutoff.scheduled_release_at_ns
            ):
                raise ValueError(
                    "future consensus requires an explicit evolution target"
                )
        predicted = self.predicted_consensus
        if (
            self.target.surprise_reference
            is SurpriseReference.PREDICTED_CONSENSUS
        ):
            if (
                not isinstance(predicted, ForecastSnapshotV1)
                or predicted.target.kind is not ForecastTargetKind.CONSENSUS
            ):
                raise ValueError(
                    "surprise requires a sealed consensus prediction"
                )
            if (
                predicted.cutoff != self.cutoff
                or predicted.inputs != self.inputs
            ):
                raise ValueError(
                    "predicted consensus horizon/cutoff/input mismatch"
                )
            predicted.target.verify_release(schedule)
            if predicted.generated_at_ns > self.generated_at_ns:
                raise ValueError(
                    "predicted consensus follows surprise forecast"
                )
        elif predicted is not None:
            raise ValueError("unexpected predicted consensus snapshot")
        # Also enforce a bounded artifact after nested validation.
        self.to_json()

    @property
    def snapshot_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-snapshot",
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
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastSnapshotV1:
        nested = data["predicted_consensus"]
        if (
            nested is not None
            and _mapping(nested).get("predicted_consensus") is not None
        ):
            raise ValueError("nested consensus prediction exceeds depth bound")
        result = cls(
            cutoff=ForecastCutoffV1.from_dict(_mapping(data["cutoff"])),
            target=ForecastTargetV1.from_dict(_mapping(data["target"])),
            inputs=ForecastInputsV1.from_dict(_mapping(data["inputs"])),
            model=ForecastModelIdentityV1.from_dict(_mapping(data["model"])),
            distribution=ForecastDistributionV1.from_dict(
                _mapping(data["distribution"])
            ),
            generated_at_ns=data["generated_at_ns"],
            normalizer_id=data["normalizer_id"],
            predicted_consensus=(
                None if nested is None else cls.from_dict(_mapping(nested))
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastSnapshotV1:
        return cls.from_dict(_load(text))


def first_release_actual(
    corpus: EconomicCalendarCorpusV1, target: ForecastTargetV1
) -> EconomicCalendarReleaseV1:
    """Find the first actual, never a revision substituted for missing history."""
    values = [
        item
        for item in corpus.releases
        if item.logical_event_key == target.logical_event_key
        and item.actual_value is not None
        and item.stage is not EconomicReleaseStage.REVISION
    ]
    if len(values) != 1:
        raise ValueError("exactly one first-release actual is required")
    target.verify_release(values[0])
    return values[0]
