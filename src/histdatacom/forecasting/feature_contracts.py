"""Immutable, availability-qualified building blocks for macro feature replay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from histdatacom.runtime_contracts import JSONValue

from .contracts import (
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

MAX_FEATURE_RECORDS = 50_000
MAX_FEATURE_CELLS = 20_000
MAX_FEATURE_COLUMNS = 256
MAX_FEATURE_PERIODS = 10_000


def _count(value: int, name: str, maximum: int, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside its integer bounds")
    return value


class FeatureKind(str, Enum):
    MACRO = "macro"
    CONSENSUS = "consensus"
    MARKET = "market"
    ALTERNATIVE = "alternative"
    CLASSIFICATION = "classification"


class FeatureSourceMode(str, Enum):
    HISTORICAL_VINTAGE = "historical-vintage"
    OBSERVED = "observed"
    LATEST_REVISED = "latest-revised"
    EX_POST = "ex-post"
    SYNTHETIC_RECONSTRUCTION = "synthetic-reconstruction"


class FeatureCellStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    SOURCE_MISSING = "source-missing"
    STALE = "stale"
    WARMUP = "warmup"
    SEMANTIC_BREAK = "semantic-break"
    ZERO_SCALE = "zero-scale"


class FeatureTransformKind(str, Enum):
    LEVEL = "level"
    LAG = "lag"
    DIFFERENCE = "difference"
    ROLLING_MEAN = "rolling-mean"
    EXPANDING_MEAN = "expanding-mean"
    ROLLING_ZSCORE = "rolling-zscore"


@dataclass(frozen=True, slots=True)
class FeaturePeriodV1:
    """An explicitly requested half-open reference interval, not row position."""

    label: str
    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "label", _text(self.label, "period label"))
        _ns(self.start_ns, "period start")
        _ns(self.end_ns, "period end")
        if self.start_ns >= self.end_ns:
            raise ValueError("reference period must have positive width")
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-period",
            {
                "label": self.label,
                "start_ns": self.start_ns,
                "end_ns": self.end_ns,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeaturePeriodV1:
        result = cls(data["label"], data["start_ns"], data["end_ns"])
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureEvidenceV1:
    """Retained normalized source record plus an explicit availability receipt.

    Retrieval is provenance, never a substitute for historical availability.
    Archive adapters must qualify their clocks; a latest-revised database is
    deliberately representable but cannot be admitted by the ex-ante store.
    """

    source_name: str
    source_uri: str
    source_sha256: str
    source_record_id: str
    adapter_name: str
    adapter_version: str
    availability_basis: str
    retrieved_at_ns: int
    mode: FeatureSourceMode
    record_json: str

    def __post_init__(self) -> None:
        for name in (
            "source_name",
            "source_uri",
            "source_record_id",
            "adapter_name",
            "adapter_version",
            "availability_basis",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        digest = self.source_sha256
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("source_sha256 requires a SHA-256 digest")
        _ns(self.retrieved_at_ns, "retrieved_at_ns")
        object.__setattr__(self, "mode", FeatureSourceMode(self.mode))
        record = _load(self.record_json)
        if not record:
            raise ValueError("normalized source record cannot be empty")
        object.__setattr__(self, "record_json", _json(record))
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-evidence",
            {
                "source_name": self.source_name,
                "source_uri": self.source_uri,
                "source_sha256": self.source_sha256,
                "source_record_id": self.source_record_id,
                "adapter_name": self.adapter_name,
                "adapter_version": self.adapter_version,
                "availability_basis": self.availability_basis,
                "retrieved_at_ns": self.retrieved_at_ns,
                "mode": self.mode.value,
                "record": _load(self.record_json),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureEvidenceV1:
        result = cls(
            data["source_name"],
            data["source_uri"],
            data["source_sha256"],
            data["source_record_id"],
            data["adapter_name"],
            data["adapter_version"],
            data["availability_basis"],
            data["retrieved_at_ns"],
            FeatureSourceMode(data["mode"]),
            _json(_mapping(data["record"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureDefinitionV1:
    """Semantic metadata as known at a named time, including classification."""

    feature_key: str
    kind: FeatureKind
    unit: str
    scale: float
    seasonality: str
    methodology: str
    frequency: str
    known_at_ns: int
    base: str | None = None
    classification: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "feature_key",
            "unit",
            "seasonality",
            "methodology",
            "frequency",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        for name in ("base", "classification"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        object.__setattr__(self, "kind", FeatureKind(self.kind))
        scale = _finite(self.scale, "feature scale")
        if scale <= 0:
            raise ValueError("feature scale must be positive")
        object.__setattr__(self, "scale", scale)
        _ns(self.known_at_ns, "metadata known_at_ns")
        self.to_dict()

    def _semantics(self) -> dict[str, JSONValue]:
        return {
            "feature_key": self.feature_key,
            "kind": self.kind.value,
            "unit": self.unit,
            "scale": self.scale,
            "seasonality": self.seasonality,
            "methodology": self.methodology,
            "frequency": self.frequency,
            "base": self.base,
            "classification": self.classification,
        }

    @property
    def semantic_id(self) -> str:
        return str(_seal("feature-semantics", self._semantics())["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-definition",
            {
                **self._semantics(),
                "known_at_ns": self.known_at_ns,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureDefinitionV1:
        result = cls(
            data["feature_key"],
            FeatureKind(data["kind"]),
            data["unit"],
            data["scale"],
            data["seasonality"],
            data["methodology"],
            data["frequency"],
            data["known_at_ns"],
            data["base"],
            data["classification"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureObservationV1:
    """One immutable vintage; missing source values remain missing."""

    definition: FeatureDefinitionV1
    period: FeaturePeriodV1
    value: float | None
    available_at_ns: int
    published_at_ns: int | None
    vintage_sequence: int
    supersedes_id: str | None
    evidence: FeatureEvidenceV1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.definition, FeatureDefinitionV1)
            or not isinstance(self.period, FeaturePeriodV1)
            or not isinstance(self.evidence, FeatureEvidenceV1)
        ):
            raise TypeError(
                "observation requires typed metadata/period/evidence"
            )
        if self.value is not None:
            object.__setattr__(
                self, "value", _finite(self.value, "feature value")
            )
        _ns(self.available_at_ns, "feature available_at_ns")
        if self.published_at_ns is not None:
            _ns(self.published_at_ns, "published_at_ns")
            if self.published_at_ns > self.available_at_ns:
                raise ValueError("availability precedes publication")
        if self.definition.known_at_ns > self.available_at_ns:
            raise ValueError("feature metadata is unavailable at vintage time")
        if self.definition.kind is FeatureKind.MARKET and (
            self.available_at_ns < self.period.end_ns
        ):
            raise ValueError("market period has not ended at availability")
        _count(self.vintage_sequence, "vintage sequence", MAX_FEATURE_RECORDS)
        if (self.vintage_sequence == 0) != (self.supersedes_id is None):
            raise ValueError("initial/revised vintage predecessor mismatch")
        if self.supersedes_id is not None:
            object.__setattr__(
                self,
                "supersedes_id",
                _text(self.supersedes_id, "supersedes_id"),
            )
        self.to_dict()

    @property
    def observation_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-observation",
            {
                "definition": self.definition.to_dict(),
                "period": self.period.to_dict(),
                "value": self.value,
                "available_at_ns": self.available_at_ns,
                "published_at_ns": self.published_at_ns,
                "vintage_sequence": self.vintage_sequence,
                "supersedes_id": self.supersedes_id,
                "evidence": self.evidence.to_dict(),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureObservationV1:
        result = cls(
            FeatureDefinitionV1.from_dict(_mapping(data["definition"])),
            FeaturePeriodV1.from_dict(_mapping(data["period"])),
            data["value"],
            data["available_at_ns"],
            data["published_at_ns"],
            data["vintage_sequence"],
            data["supersedes_id"],
            FeatureEvidenceV1.from_dict(_mapping(data["evidence"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureScheduleV1:
    """One schedule assertion, admitted by knowledge time rather than event time."""

    feature_key: str
    period: FeaturePeriodV1
    scheduled_at_ns: int
    known_at_ns: int
    evidence: FeatureEvidenceV1

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "feature_key", _text(self.feature_key, "feature_key")
        )
        if not isinstance(self.period, FeaturePeriodV1) or not isinstance(
            self.evidence, FeatureEvidenceV1
        ):
            raise TypeError("schedule requires typed period/evidence")
        _ns(self.scheduled_at_ns, "scheduled_at_ns")
        _ns(self.known_at_ns, "schedule known_at_ns")
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-schedule",
            {
                "feature_key": self.feature_key,
                "period": self.period.to_dict(),
                "scheduled_at_ns": self.scheduled_at_ns,
                "known_at_ns": self.known_at_ns,
                "evidence": self.evidence.to_dict(),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureScheduleV1:
        result = cls(
            data["feature_key"],
            FeaturePeriodV1.from_dict(_mapping(data["period"])),
            data["scheduled_at_ns"],
            data["known_at_ns"],
            FeatureEvidenceV1.from_dict(_mapping(data["evidence"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureTransformV1:
    """Reference-row causal transform; no implicit imputation or skip-missing."""

    kind: FeatureTransformKind = FeatureTransformKind.LEVEL
    window: int = 1
    min_support: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", FeatureTransformKind(self.kind))
        _count(self.window, "transform window", MAX_FEATURE_PERIODS, 1)
        _count(self.min_support, "minimum support", MAX_FEATURE_PERIODS, 1)
        if self.kind is FeatureTransformKind.LEVEL and (
            self.window != 1 or self.min_support != 1
        ):
            raise ValueError("level transform has no window parameters")
        if (
            self.kind
            in {FeatureTransformKind.LAG, FeatureTransformKind.DIFFERENCE}
            and self.min_support != 1
        ):
            raise ValueError("lag/difference minimum support must be one")
        if (
            self.kind
            in {
                FeatureTransformKind.ROLLING_MEAN,
                FeatureTransformKind.ROLLING_ZSCORE,
            }
            and self.min_support > self.window
        ):
            raise ValueError("minimum support exceeds rolling window")
        if (
            self.kind is FeatureTransformKind.ROLLING_ZSCORE
            and self.min_support < 2
        ):
            raise ValueError(
                "sample z-score requires at least two observations"
            )
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-transform",
            {
                "kind": self.kind.value,
                "window": self.window,
                "min_support": self.min_support,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureTransformV1:
        result = cls(
            FeatureTransformKind(data["kind"]),
            data["window"],
            data["min_support"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureColumnV1:
    name: str
    feature_key: str
    transform: FeatureTransformV1 = FeatureTransformV1()
    max_age_ns: int | None = None

    def __post_init__(self) -> None:
        for name in ("name", "feature_key"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if not isinstance(self.transform, FeatureTransformV1):
            raise TypeError("column requires a typed transform")
        if self.max_age_ns is not None:
            _count(self.max_age_ns, "maximum feature age", 2**63 - 1)
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-column",
            {
                "name": self.name,
                "feature_key": self.feature_key,
                "transform": self.transform.to_dict(),
                "max_age_ns": self.max_age_ns,
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureColumnV1:
        result = cls(
            data["name"],
            data["feature_key"],
            FeatureTransformV1.from_dict(_mapping(data["transform"])),
            data["max_age_ns"],
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class FeatureRequestV1:
    """Fixed grid and cutoff: neither discovered columns nor global store IDs."""

    cutoff_at_ns: int
    periods: tuple[FeaturePeriodV1, ...]
    columns: tuple[FeatureColumnV1, ...]

    def __post_init__(self) -> None:
        _ns(self.cutoff_at_ns, "feature cutoff")
        object.__setattr__(self, "periods", tuple(self.periods))
        object.__setattr__(self, "columns", tuple(self.columns))
        _count(len(self.periods), "period count", MAX_FEATURE_PERIODS, 1)
        _count(len(self.columns), "column count", MAX_FEATURE_COLUMNS, 1)
        _count(
            len(self.periods) * len(self.columns),
            "feature cells",
            MAX_FEATURE_CELLS,
            1,
        )
        if any(
            not isinstance(item, FeaturePeriodV1) for item in self.periods
        ) or any(
            not isinstance(item, FeatureColumnV1) for item in self.columns
        ):
            raise TypeError("request requires typed periods and columns")
        if any(
            left.end_ns > right.start_ns
            for left, right in zip(self.periods, self.periods[1:])
        ):
            raise ValueError("reference periods overlap or are unordered")
        if len({item.label for item in self.periods}) != len(
            self.periods
        ) or len({item.name for item in self.columns}) != len(self.columns):
            raise ValueError("duplicate period label or column name")
        self.to_dict()

    @property
    def request_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-request",
            {
                "cutoff_at_ns": self.cutoff_at_ns,
                "periods": [item.to_dict() for item in self.periods],
                "columns": [item.to_dict() for item in self.columns],
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureRequestV1:
        result = cls(
            data["cutoff_at_ns"],
            tuple(
                FeaturePeriodV1.from_dict(_mapping(item))
                for item in _array(data["periods"])
            ),
            tuple(
                FeatureColumnV1.from_dict(_mapping(item))
                for item in _array(data["columns"])
            ),
        )
        _verify(data, result.to_dict())
        return result
