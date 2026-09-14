"""Provider-neutral economic release vintages and as-known-at queries.

The contracts in this module describe evidence obtained from legal statistical
producers, central banks, or independently governed forecast processes.  They
do not fetch data and never make a provider's taxonomy, identifiers, schedule
history, or licensing model part of canonical event identity.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from statistics import median
from typing import Any, Protocol, cast, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from histdatacom.market_context.contracts import (
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    canonical_contract_json,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

ECONOMIC_CALENDAR_RELEASE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-release.v1"
)
ECONOMIC_CALENDAR_FORECAST_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-forecast.v1"
)
ECONOMIC_CALENDAR_STATE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-state.v1"
)
ECONOMIC_CALENDAR_QUERY_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-query.v1"
)
ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-corpus.v1"
)
ECONOMIC_CALENDAR_ARROW_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-arrow.v1"
)
ECONOMIC_CALENDAR_SURPRISE_POLICY_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-surprise-policy.v1"
)
ECONOMIC_CALENDAR_SURPRISE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-surprise.v1"
)
ECONOMIC_UNIT_CONVERSION_SCHEMA_VERSION = (
    "histdatacom.economic-unit-conversion.v1"
)

MAX_ECONOMIC_CALENDAR_RELEASES = 100_000
MAX_ECONOMIC_CALENDAR_FORECASTS = 250_000
MAX_ECONOMIC_CALENDAR_QUERY_EVENTS = 512
MAX_ECONOMIC_CALENDAR_ADAPTERS = 64
MAX_ECONOMIC_CALENDAR_CORPUS_BYTES = 64 * 1024 * 1024
MAX_ECONOMIC_CALENDAR_ARROW_BYTES = 3 * MAX_ECONOMIC_CALENDAR_CORPUS_BYTES

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_ECONOMY_CODE_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9._:-]{3,32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EconomicReleaseStage(str, Enum):
    """Publication stage for one logical release event."""

    INITIAL = "initial"
    FLASH = "flash"
    ADVANCE = "advance"
    PRELIMINARY = "preliminary"
    SECOND = "second"
    FINAL = "final"
    REVISION = "revision"

    @classmethod
    def from_value(
        cls, value: str | EconomicReleaseStage
    ) -> EconomicReleaseStage:
        """Return a strict release stage."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic release stage") from exc


class EconomicReleaseStatus(str, Enum):
    """Schedule/publication status of one immutable event vintage."""

    TENTATIVE = "tentative"
    SCHEDULED = "scheduled"
    RESCHEDULED = "rescheduled"
    DELAYED = "delayed"
    CANCELLED = "cancelled"
    RELEASED = "released"
    UNSCHEDULED = "unscheduled"
    DISCONTINUED = "discontinued"
    SOURCE_CALENDAR_UNAVAILABLE = "source-calendar-unavailable"
    UNRESOLVED = "unresolved"

    @classmethod
    def from_value(
        cls, value: str | EconomicReleaseStatus
    ) -> EconomicReleaseStatus:
        """Return a strict event status."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic release status") from exc


class EconomicEventFamily(str, Enum):
    """Provider-neutral top-level economic event families."""

    MONETARY_POLICY = "monetary-policy"
    INFLATION_PRICES = "inflation-prices"
    LABOUR_MARKET = "labour-market"
    GDP_NATIONAL_ACCOUNTS = "gdp-national-accounts"
    RETAIL_CONSUMPTION = "retail-consumption"
    INDUSTRIAL_PRODUCTION = "industrial-production"
    TRADE_EXTERNAL = "trade-external"
    HOUSING = "housing"
    CONFIDENCE_SURVEY = "confidence-survey"
    MONEY_CREDIT = "money-credit"
    FISCAL = "fiscal"
    OTHER_OFFICIAL = "other-official"

    @classmethod
    def from_value(
        cls, value: str | EconomicEventFamily
    ) -> EconomicEventFamily:
        """Return a strict canonical event family."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic event family") from exc


class EconomicTimePrecision(str, Enum):
    """Evidence precision for scheduled and actual publication times."""

    EXACT_SECOND = "exact-second"
    EXACT_MINUTE = "exact-minute"
    SCHEDULED_ONLY = "scheduled-only"
    DATE_ONLY = "date-only"
    INFERRED_BOUNDED = "inferred-bounded"

    @classmethod
    def from_value(
        cls, value: str | EconomicTimePrecision
    ) -> EconomicTimePrecision:
        """Return a strict release-time precision class."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic time precision") from exc


class EconomicForecastKind(str, Enum):
    """Provenance class for an expectation available before a release."""

    OBSERVED_CONSENSUS = "observed_consensus"
    MACHINE_PROJECTION = "machine_projection"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | EconomicForecastKind
    ) -> EconomicForecastKind:
        """Return a strict forecast kind."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic forecast kind") from exc


class EconomicForecastScope(str, Enum):
    """What one forecast estimates."""

    EVENT_CONSENSUS = "event_consensus"
    OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET = (
        "official_professional_survey_event_target"
    )
    OFFICIAL_PROFESSIONAL_SURVEY_PERIOD_TARGET = (
        "official_professional_survey_period_target"
    )
    CENTRAL_BANK_PROJECTION = "central_bank_projection"
    GOVERNMENT_PROJECTION = "government_projection"
    MARKET_IMPLIED = "market_implied"
    MACHINE_EVENT_TARGET = "machine_event_target"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | EconomicForecastScope
    ) -> EconomicForecastScope:
        """Return a strict forecast scope."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic forecast scope") from exc

    @property
    def calendar_style_eligible(self) -> bool:
        """Return whether the scope can be shown as consensus without a proxy."""
        return self in {
            EconomicForecastScope.EVENT_CONSENSUS,
            EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
        }


class EconomicForecastStatistic(str, Enum):
    """Statistic represented by one forecast vintage."""

    POINT = "point"
    MEAN = "mean"
    WEIGHTED_MEAN = "weighted_mean"
    MEDIAN = "median"
    MODE = "mode"
    QUANTILE = "quantile"
    DISTRIBUTION = "distribution"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | EconomicForecastStatistic
    ) -> EconomicForecastStatistic:
        """Return a strict forecast statistic."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic forecast statistic") from exc


_FIRST_PUBLICATION_STAGES = frozenset(
    {
        EconomicReleaseStage.INITIAL,
        EconomicReleaseStage.FLASH,
        EconomicReleaseStage.ADVANCE,
        EconomicReleaseStage.PRELIMINARY,
        EconomicReleaseStage.SECOND,
        EconomicReleaseStage.FINAL,
    }
)
_CALENDAR_FORECAST_SCOPES = frozenset(
    {
        EconomicForecastScope.EVENT_CONSENSUS,
        EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
    }
)


def _required_text(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _key(value: object, name: str) -> str:
    text = _required_text(value, name).lower()
    if _KEY_RE.fullmatch(text) is None:
        raise ValueError(f"{name} is not a canonical key")
    return text


def _bounded_ns(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _optional_ns(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _bounded_ns(value, name)


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_finite(value: object, name: str) -> float | None:
    if value is None:
        return None
    return _finite(value, name)


def _text_tuple(value: Iterable[object], name: str) -> tuple[str, ...]:
    result = tuple(
        sorted({_required_text(item, name) for item in tuple(value)})
    )
    if len(result) > 64:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _currencies(value: Iterable[object]) -> tuple[str, ...]:
    result = tuple(
        sorted({_required_text(item, "currency").upper() for item in value})
    )
    if any(_CURRENCY_RE.fullmatch(item) is None for item in result):
        raise ValueError("affected currency is invalid")
    return result


def _symbols(value: Iterable[object]) -> tuple[str, ...]:
    result = tuple(
        sorted({_required_text(item, "symbol").upper() for item in value})
    )
    if any(_SYMBOL_RE.fullmatch(item) is None for item in result):
        raise ValueError("affected symbol is invalid")
    return result


def _event_families(value: Iterable[object]) -> tuple[str, ...]:
    result = tuple(
        sorted(
            {
                EconomicEventFamily.from_value(
                    item if isinstance(item, EconomicEventFamily) else str(item)
                ).value
                for item in value
            }
        )
    )
    if len(result) > len(EconomicEventFamily):
        raise ValueError("event family filter exceeds the supported range")
    return result


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


def _economy_code(value: object) -> str:
    text = _required_text(value, "economy_code").upper()
    if _ECONOMY_CODE_RE.fullmatch(text) is None:
        raise ValueError("economy_code is invalid")
    return text


def _aware_datetime(value: object, name: str) -> datetime:
    text = _required_text(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must retain an explicit UTC offset")
    return parsed


def _datetime_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1_000


def _time_evidence(
    *, timestamp_ns: int, lexical: object, timezone_name: object, field: str
) -> tuple[str, str]:
    text = _required_text(lexical, f"{field}_lexical")
    zone_name = _required_text(timezone_name, "source_timezone")
    parsed = _aware_datetime(text, f"{field}_lexical")
    if _datetime_ns(parsed) != timestamp_ns:
        raise ValueError(
            f"{field} lexical evidence differs from normalized time"
        )
    try:
        zone = ZoneInfo(zone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("source_timezone is not an IANA timezone") from exc
    if parsed.astimezone(zone).utcoffset() != parsed.utcoffset():
        raise ValueError(f"{field} lexical offset differs from source timezone")
    return text, zone_name


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


@dataclass(frozen=True, slots=True)
class EconomicUnitConversionV1:
    """Auditable affine conversion from retained source evidence."""

    source_value: float
    source_unit: str
    source_scale: float
    source_base: str | None
    target_unit: str
    target_scale: float
    target_base: str | None
    multiplier: float
    offset: float
    raw_lexical: str
    policy_version: str
    conversion_id: str = ""
    schema_version: str = ECONOMIC_UNIT_CONVERSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_UNIT_CONVERSION_SCHEMA_VERSION:
            raise ValueError("unsupported economic unit conversion schema")
        object.__setattr__(
            self, "source_value", _finite(self.source_value, "source_value")
        )
        for name in ("source_unit", "target_unit", "raw_lexical"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        for name in ("source_scale", "target_scale"):
            result = _finite(getattr(self, name), name)
            if result <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, result)
        object.__setattr__(
            self, "source_base", _optional_text(self.source_base)
        )
        object.__setattr__(
            self, "target_base", _optional_text(self.target_base)
        )
        object.__setattr__(
            self, "multiplier", _finite(self.multiplier, "multiplier")
        )
        object.__setattr__(self, "offset", _finite(self.offset, "offset"))
        object.__setattr__(
            self,
            "policy_version",
            _required_text(self.policy_version, "policy_version"),
        )
        expected = _stable_id(
            "economic-unit-conversion", self.identity_payload()
        )
        supplied = _optional_text(self.conversion_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "conversion_id does not match deterministic identity"
            )
        object.__setattr__(self, "conversion_id", expected)

    @property
    def normalized_value(self) -> float:
        """Return the normalized numeric value proved by the policy."""
        return self.source_value * self.multiplier + self.offset

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic conversion identity."""
        return {
            "schema_version": self.schema_version,
            "source_value": self.source_value,
            "source_unit": self.source_unit,
            "source_scale": self.source_scale,
            "source_base": self.source_base,
            "target_unit": self.target_unit,
            "target_scale": self.target_scale,
            "target_base": self.target_base,
            "multiplier": self.multiplier,
            "offset": self.offset,
            "raw_lexical": self.raw_lexical,
            "policy_version": self.policy_version,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible conversion evidence."""
        return {**self.identity_payload(), "conversion_id": self.conversion_id}

    def to_json(self) -> str:
        """Return deterministic compact conversion JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicUnitConversionV1:
        """Restore and verify conversion evidence."""
        return cls(
            source_value=cast(float, data.get("source_value")),
            source_unit=str(data.get("source_unit", "")),
            source_scale=cast(float, data.get("source_scale")),
            source_base=_optional_text(data.get("source_base")),
            target_unit=str(data.get("target_unit", "")),
            target_scale=cast(float, data.get("target_scale")),
            target_base=_optional_text(data.get("target_base")),
            multiplier=cast(float, data.get("multiplier")),
            offset=cast(float, data.get("offset")),
            raw_lexical=str(data.get("raw_lexical", "")),
            policy_version=str(data.get("policy_version", "")),
            conversion_id=str(data.get("conversion_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicUnitConversionV1:
        """Restore conversion evidence from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic unit conversion is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicSurprisePolicyV1:
    """Versioned direction and prior-only robust surprise policy."""

    policy_version: str
    direction_by_series: tuple[tuple[str, int], ...]
    scale_window: int
    minimum_observations: int
    epsilon: float
    policy_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_SURPRISE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_CALENDAR_SURPRISE_POLICY_SCHEMA_VERSION
        ):
            raise ValueError("unsupported economic surprise policy schema")
        object.__setattr__(
            self,
            "policy_version",
            _required_text(self.policy_version, "policy_version"),
        )
        normalized: list[tuple[str, int]] = []
        for raw_series, raw_direction in self.direction_by_series:
            series = _key(raw_series, "direction series_key")
            if isinstance(raw_direction, bool) or raw_direction not in {-1, 1}:
                raise ValueError("surprise direction must be -1 or +1")
            normalized.append((series, raw_direction))
        normalized.sort()
        if len({item[0] for item in normalized}) != len(normalized):
            raise ValueError("surprise policy repeats a series direction")
        if not normalized:
            raise ValueError("surprise policy requires a direction map")
        if isinstance(self.scale_window, bool) or not isinstance(
            self.scale_window, int
        ):
            raise TypeError("scale_window must be an integer")
        if not 1 <= self.scale_window <= 10_000:
            raise ValueError("scale_window is outside the supported range")
        if isinstance(self.minimum_observations, bool) or not isinstance(
            self.minimum_observations, int
        ):
            raise TypeError("minimum_observations must be an integer")
        if not 1 <= self.minimum_observations <= self.scale_window:
            raise ValueError("minimum_observations exceeds the scale window")
        epsilon = _finite(self.epsilon, "epsilon")
        if epsilon <= 0:
            raise ValueError("epsilon must be positive")
        object.__setattr__(self, "direction_by_series", tuple(normalized))
        object.__setattr__(self, "epsilon", epsilon)
        expected = _stable_id(
            "economic-surprise-policy", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("policy_id does not match deterministic identity")
        object.__setattr__(self, "policy_id", expected)

    def direction_for(self, series_key: str) -> int:
        """Return the predeclared economic direction for a series."""
        selected = _key(series_key, "series_key")
        for candidate, direction in self.direction_by_series:
            if candidate == selected:
                return direction
        raise ValueError("surprise policy has no direction for series")

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic policy identity."""
        return {
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "direction_by_series": [
                {"series_key": series, "direction": direction}
                for series, direction in self.direction_by_series
            ],
            "scale_window": self.scale_window,
            "minimum_observations": self.minimum_observations,
            "epsilon": self.epsilon,
            "scale_estimator": "1.4826 * median_absolute_deviation",
            "history_rule": (
                "first_actual_publication_ns < "
                "current.first_actual_publication_ns"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy metadata."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return deterministic compact policy JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicSurprisePolicyV1:
        """Restore and verify a surprise policy."""
        directions = tuple(
            (
                str(_mapping(item, "direction").get("series_key", "")),
                cast(int, _mapping(item, "direction").get("direction")),
            )
            for item in _sequence(data.get("direction_by_series"))
        )
        return cls(
            policy_version=str(data.get("policy_version", "")),
            direction_by_series=directions,
            scale_window=cast(int, data.get("scale_window")),
            minimum_observations=cast(int, data.get("minimum_observations")),
            epsilon=cast(float, data.get("epsilon")),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicSurprisePolicyV1:
        """Restore a surprise policy from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic surprise policy is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicCalendarReleaseV1:
    """One immutable official release, schedule, cancellation, or revision."""

    logical_event_key: str
    series_key: str
    series_version: str
    comparability_bridge_id: str | None
    economy: str
    economy_code: str
    currency: str
    institution: str
    event_family: EconomicEventFamily
    indicator_id: str
    source_series_id: str
    source_table_id: str
    source_release_id: str
    source_request_id: str
    title: str
    reference_period: str
    reference_period_end_ns: int
    frequency: str
    seasonality: str
    unit: str
    scale: float
    base: str | None
    stage: EconomicReleaseStage
    status: EconomicReleaseStatus
    scheduled_for_ns: int
    scheduled_lexical: str
    released_at_ns: int | None
    released_lexical: str | None
    first_observed_at_ns: int
    available_at_ns: int
    source_timezone: str
    timezone_evidence: str
    time_precision: EconomicTimePrecision
    precision: MarketContextPrecision
    market_context_kind: MarketContextKind
    source: MarketContextSourceV1
    affected_currencies: tuple[str, ...]
    affected_symbols: tuple[str, ...]
    limitations: tuple[str, ...]
    schedule_change_reason: str | None = None
    value_conversion: EconomicUnitConversionV1 | None = None
    actual_value: float | None = None
    actual_lexical: str | None = None
    content_sha256: str | None = None
    revision_sequence: int = 0
    supersedes_release_id: str | None = None
    release_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar release schema")
        for name in (
            "logical_event_key",
            "series_key",
            "series_version",
            "indicator_id",
            "source_series_id",
            "source_table_id",
            "source_release_id",
            "source_request_id",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "comparability_bridge_id",
            _optional_text(self.comparability_bridge_id),
        )
        for name in (
            "economy",
            "institution",
            "title",
            "reference_period",
            "frequency",
            "seasonality",
            "unit",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "economy_code", _economy_code(self.economy_code)
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        currency = _required_text(self.currency, "currency").upper()
        if _CURRENCY_RE.fullmatch(currency) is None:
            raise ValueError("currency must be an ISO-style three-letter code")
        object.__setattr__(self, "currency", currency)
        object.__setattr__(
            self,
            "reference_period_end_ns",
            _bounded_ns(
                self.reference_period_end_ns, "reference_period_end_ns"
            ),
        )
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        object.__setattr__(self, "scale", scale)
        object.__setattr__(self, "base", _optional_text(self.base))
        stage = EconomicReleaseStage.from_value(self.stage)
        object.__setattr__(self, "stage", stage)
        status = EconomicReleaseStatus.from_value(self.status)
        object.__setattr__(self, "status", status)
        scheduled = _bounded_ns(self.scheduled_for_ns, "scheduled_for_ns")
        released = _optional_ns(self.released_at_ns, "released_at_ns")
        first_observed = _bounded_ns(
            self.first_observed_at_ns, "first_observed_at_ns"
        )
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if not isinstance(self.source, MarketContextSourceV1):
            raise TypeError("source must use MarketContextSourceV1")
        if self.source.retrieved_at_ns < max(first_observed, available):
            raise ValueError(
                "source retrieval precedes release availability or observation"
            )
        if status in {
            EconomicReleaseStatus.RELEASED,
            EconomicReleaseStatus.UNSCHEDULED,
        }:
            if released is None:
                raise ValueError("an actual release requires released_at_ns")
            if self.actual_value is None:
                raise ValueError("an actual release requires actual_value")
            if released > first_observed or released > available:
                raise ValueError(
                    "release publication follows observation/availability"
                )
            if _optional_text(self.actual_lexical) is None:
                raise ValueError(
                    "an actual release requires raw lexical evidence"
                )
        elif self.actual_value is not None or self.actual_lexical is not None:
            raise ValueError("a non-released status cannot contain an actual")
        scheduled_lexical, source_timezone = _time_evidence(
            timestamp_ns=scheduled,
            lexical=self.scheduled_lexical,
            timezone_name=self.source_timezone,
            field="scheduled",
        )
        released_lexical = _optional_text(self.released_lexical)
        if released is None:
            if released_lexical is not None:
                raise ValueError(
                    "released lexical evidence requires release time"
                )
        else:
            released_lexical, released_timezone = _time_evidence(
                timestamp_ns=released,
                lexical=released_lexical,
                timezone_name=source_timezone,
                field="released",
            )
            if released_timezone != source_timezone:
                raise ValueError("release timezones differ")
        object.__setattr__(self, "scheduled_for_ns", scheduled)
        object.__setattr__(self, "scheduled_lexical", scheduled_lexical)
        object.__setattr__(self, "released_at_ns", released)
        object.__setattr__(self, "released_lexical", released_lexical)
        object.__setattr__(self, "first_observed_at_ns", first_observed)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_timezone", source_timezone)
        object.__setattr__(
            self,
            "timezone_evidence",
            _required_text(self.timezone_evidence, "timezone_evidence"),
        )
        object.__setattr__(
            self,
            "time_precision",
            EconomicTimePrecision.from_value(self.time_precision),
        )
        object.__setattr__(
            self,
            "precision",
            MarketContextPrecision.from_value(self.precision),
        )
        object.__setattr__(
            self,
            "market_context_kind",
            MarketContextKind.from_value(self.market_context_kind),
        )
        currencies = _currencies(self.affected_currencies)
        symbols = _symbols(self.affected_symbols)
        if not currencies and not symbols:
            raise ValueError(
                "a release requires an affected currency or symbol"
            )
        if currency not in currencies:
            raise ValueError("release currency must be affected")
        object.__setattr__(self, "affected_currencies", currencies)
        object.__setattr__(self, "affected_symbols", symbols)
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("a release requires explicit limitations")
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(
            self,
            "actual_value",
            _optional_finite(self.actual_value, "actual_value"),
        )
        object.__setattr__(
            self, "actual_lexical", _optional_text(self.actual_lexical)
        )
        conversion = self.value_conversion
        if conversion is not None:
            if not isinstance(conversion, EconomicUnitConversionV1):
                raise TypeError("value_conversion must use the v1 contract")
            if (
                conversion.target_unit != self.unit
                or conversion.target_scale != self.scale
                or conversion.target_base != self.base
            ):
                raise ValueError("value conversion target differs from release")
            if self.actual_value is None or not math.isclose(
                conversion.normalized_value,
                self.actual_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError("value conversion does not reproduce actual")
            if conversion.raw_lexical != self.actual_lexical:
                raise ValueError("value conversion loses raw lexical evidence")
        object.__setattr__(self, "value_conversion", conversion)
        content_hash = _optional_text(self.content_sha256)
        if content_hash is not None:
            content_hash = _sha256(content_hash, "content_sha256")
        object.__setattr__(self, "content_sha256", content_hash)
        if isinstance(self.revision_sequence, bool) or not isinstance(
            self.revision_sequence, int
        ):
            raise TypeError("revision_sequence must be an integer")
        if not 0 <= self.revision_sequence <= 10_000:
            raise ValueError("revision_sequence is outside the supported range")
        supersedes = _optional_text(self.supersedes_release_id)
        if self.revision_sequence == 0 and supersedes is not None:
            raise ValueError(
                "an initial vintage cannot supersede another release"
            )
        if self.revision_sequence > 0 and supersedes is None:
            raise ValueError("a later vintage requires supersedes_release_id")
        if (
            stage is EconomicReleaseStage.REVISION
            and self.revision_sequence == 0
        ):
            raise ValueError("a revision stage requires a later vintage")
        schedule_reason = _optional_text(self.schedule_change_reason)
        if (
            status
            in {
                EconomicReleaseStatus.RESCHEDULED,
                EconomicReleaseStatus.DELAYED,
                EconomicReleaseStatus.CANCELLED,
            }
            and schedule_reason is None
        ):
            raise ValueError("schedule status change requires a reason")
        if (
            status is EconomicReleaseStatus.RESCHEDULED
            and not self.revision_sequence
        ):
            raise ValueError("a reschedule requires a later vintage")
        object.__setattr__(self, "schedule_change_reason", schedule_reason)
        object.__setattr__(self, "supersedes_release_id", supersedes)
        expected = _stable_id(
            "economic-calendar-release", self.identity_payload()
        )
        supplied = _optional_text(self.release_id)
        if supplied is not None and supplied != expected:
            raise ValueError("release_id does not match deterministic identity")
        object.__setattr__(self, "release_id", expected)

    @property
    def event_time_ns(self) -> int:
        """Return occurrence time without moving it to a later revision."""
        if self.stage is EconomicReleaseStage.REVISION:
            return self.scheduled_for_ns
        return self.released_at_ns or self.scheduled_for_ns

    def semantic_key(self) -> tuple[object, ...]:
        """Return fields that may not drift across vintages."""
        return (
            self.logical_event_key,
            self.series_key,
            self.series_version,
            self.comparability_bridge_id,
            self.economy,
            self.economy_code,
            self.currency,
            self.institution,
            self.event_family,
            self.indicator_id,
            self.source_series_id,
            self.source_table_id,
            self.reference_period,
            self.reference_period_end_ns,
            self.frequency,
            self.seasonality,
            self.unit,
            self.scale,
            self.base,
            self.market_context_kind,
            self.affected_currencies,
            self.affected_symbols,
        )

    def series_semantic_key(self) -> tuple[object, ...]:
        """Return fields that must remain comparable across periods."""
        return (
            self.series_key,
            self.series_version,
            self.comparability_bridge_id,
            self.economy,
            self.economy_code,
            self.currency,
            self.institution,
            self.event_family,
            self.indicator_id,
            self.source_series_id,
            self.source_table_id,
            self.frequency,
            self.seasonality,
            self.unit,
            self.scale,
            self.base,
            self.market_context_kind,
            self.affected_currencies,
            self.affected_symbols,
        )

    def series_identity_payload(self) -> dict[str, JSONValue]:
        """Return versioned semantic series identity, excluding one occurrence."""
        return {
            "series_key": self.series_key,
            "series_version": self.series_version,
            "comparability_bridge_id": self.comparability_bridge_id,
            "economy": self.economy,
            "economy_code": self.economy_code,
            "currency": self.currency,
            "institution": self.institution,
            "event_family": self.event_family.value,
            "indicator_id": self.indicator_id,
            "source_series_id": self.source_series_id,
            "source_table_id": self.source_table_id,
            "frequency": self.frequency,
            "seasonality": self.seasonality,
            "unit": self.unit,
            "scale": self.scale,
            "base": self.base,
            "market_context_kind": self.market_context_kind.value,
        }

    @property
    def series_id(self) -> str:
        """Return the deterministic versioned series identity."""
        return _stable_id(
            "economic-calendar-series", self.series_identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic release identity."""
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "series_key": self.series_key,
            "series_version": self.series_version,
            "series_id": self.series_id,
            "comparability_bridge_id": self.comparability_bridge_id,
            "economy": self.economy,
            "economy_code": self.economy_code,
            "currency": self.currency,
            "institution": self.institution,
            "event_family": self.event_family.value,
            "indicator_id": self.indicator_id,
            "source_series_id": self.source_series_id,
            "source_table_id": self.source_table_id,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "title": self.title,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "frequency": self.frequency,
            "seasonality": self.seasonality,
            "unit": self.unit,
            "scale": self.scale,
            "base": self.base,
            "stage": self.stage.value,
            "status": self.status.value,
            "scheduled_for_ns": self.scheduled_for_ns,
            "scheduled_lexical": self.scheduled_lexical,
            "released_at_ns": self.released_at_ns,
            "released_lexical": self.released_lexical,
            "first_observed_at_ns": self.first_observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_timezone": self.source_timezone,
            "timezone_evidence": self.timezone_evidence,
            "time_precision": self.time_precision.value,
            "precision": self.precision.value,
            "market_context_kind": self.market_context_kind.value,
            "source": self.source.to_dict(),
            "affected_currencies": list(self.affected_currencies),
            "affected_symbols": list(self.affected_symbols),
            "limitations": list(self.limitations),
            "schedule_change_reason": self.schedule_change_reason,
            "value_conversion": (
                None
                if self.value_conversion is None
                else self.value_conversion.to_dict()
            ),
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "content_sha256": self.content_sha256,
            "revision_sequence": self.revision_sequence,
            "supersedes_release_id": self.supersedes_release_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible release metadata."""
        return {**self.identity_payload(), "release_id": self.release_id}

    def to_json(self) -> str:
        """Return deterministic compact release JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarReleaseV1:
        """Restore and verify one release vintage."""
        restored = cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            series_key=str(data.get("series_key", "")),
            series_version=str(data.get("series_version", "")),
            comparability_bridge_id=_optional_text(
                data.get("comparability_bridge_id")
            ),
            economy=str(data.get("economy", "")),
            economy_code=str(data.get("economy_code", "")),
            currency=str(data.get("currency", "")),
            institution=str(data.get("institution", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            indicator_id=str(data.get("indicator_id", "")),
            source_series_id=str(data.get("source_series_id", "")),
            source_table_id=str(data.get("source_table_id", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            title=str(data.get("title", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            frequency=str(data.get("frequency", "")),
            seasonality=str(data.get("seasonality", "")),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            base=_optional_text(data.get("base")),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            status=EconomicReleaseStatus.from_value(
                str(data.get("status", ""))
            ),
            scheduled_for_ns=cast(int, data.get("scheduled_for_ns")),
            scheduled_lexical=str(data.get("scheduled_lexical", "")),
            released_at_ns=cast(int | None, data.get("released_at_ns")),
            released_lexical=_optional_text(data.get("released_lexical")),
            first_observed_at_ns=cast(int, data.get("first_observed_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_timezone=str(data.get("source_timezone", "")),
            timezone_evidence=str(data.get("timezone_evidence", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            precision=MarketContextPrecision.from_value(
                str(data.get("precision", ""))
            ),
            market_context_kind=MarketContextKind.from_value(
                str(data.get("market_context_kind", ""))
            ),
            source=MarketContextSourceV1.from_dict(
                _mapping(data.get("source"))
            ),
            affected_currencies=tuple(
                str(item) for item in _sequence(data.get("affected_currencies"))
            ),
            affected_symbols=tuple(
                str(item) for item in _sequence(data.get("affected_symbols"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            schedule_change_reason=_optional_text(
                data.get("schedule_change_reason")
            ),
            value_conversion=(
                None
                if data.get("value_conversion") is None
                else EconomicUnitConversionV1.from_dict(
                    _mapping(data.get("value_conversion"))
                )
            ),
            actual_value=cast(float | None, data.get("actual_value")),
            actual_lexical=_optional_text(data.get("actual_lexical")),
            content_sha256=_optional_text(data.get("content_sha256")),
            revision_sequence=cast(int, data.get("revision_sequence", 0)),
            supersedes_release_id=_optional_text(
                data.get("supersedes_release_id")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        supplied_series_id = _optional_text(data.get("series_id"))
        if (
            supplied_series_id is not None
            and supplied_series_id != restored.series_id
        ):
            raise ValueError("series_id does not match deterministic identity")
        return restored

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarReleaseV1:
        """Restore a release from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic calendar release is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicCalendarForecastV1:
    """One observed-consensus or machine projection vintage."""

    logical_event_key: str
    kind: EconomicForecastKind
    scope: EconomicForecastScope
    statistic: EconomicForecastStatistic
    collection_started_at_ns: int | None
    collection_ended_at_ns: int | None
    produced_at_ns: int
    available_at_ns: int
    value: float | None
    lexical_value: str | None
    unit: str
    scale: float
    base: str | None
    source: MarketContextSourceV1
    limitations: tuple[str, ...]
    respondent_count: int | None = None
    dispersion: float | None = None
    quantile: float | None = None
    value_conversion: EconomicUnitConversionV1 | None = None
    forecast_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_FORECAST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_FORECAST_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar forecast schema")
        object.__setattr__(
            self,
            "logical_event_key",
            _key(self.logical_event_key, "logical_event_key"),
        )
        object.__setattr__(
            self, "kind", EconomicForecastKind.from_value(self.kind)
        )
        object.__setattr__(
            self, "scope", EconomicForecastScope.from_value(self.scope)
        )
        statistic = EconomicForecastStatistic.from_value(self.statistic)
        object.__setattr__(self, "statistic", statistic)
        collection_started = _optional_ns(
            self.collection_started_at_ns, "collection_started_at_ns"
        )
        collection_ended = _optional_ns(
            self.collection_ended_at_ns, "collection_ended_at_ns"
        )
        if (collection_started is None) != (collection_ended is None):
            raise ValueError("forecast collection window is incomplete")
        if (
            collection_started is not None
            and collection_ended is not None
            and collection_ended < collection_started
        ):
            raise ValueError("forecast collection window moves backward")
        produced = _bounded_ns(self.produced_at_ns, "produced_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if produced > available:
            raise ValueError("forecast production follows availability")
        if collection_ended is not None and collection_ended > produced:
            raise ValueError("forecast collection ends after production")
        if not isinstance(self.source, MarketContextSourceV1):
            raise TypeError("source must use MarketContextSourceV1")
        if self.source.retrieved_at_ns < available:
            raise ValueError("forecast source retrieval precedes availability")
        object.__setattr__(self, "produced_at_ns", produced)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "collection_started_at_ns", collection_started)
        object.__setattr__(self, "collection_ended_at_ns", collection_ended)
        value = _optional_finite(self.value, "value")
        unavailable = self.scope is EconomicForecastScope.UNAVAILABLE
        if unavailable:
            if (
                self.kind is not EconomicForecastKind.UNAVAILABLE
                or statistic is not EconomicForecastStatistic.UNAVAILABLE
                or value is not None
            ):
                raise ValueError("unavailable forecast fields are inconsistent")
        elif value is None:
            raise ValueError("an available forecast requires a value")
        elif (
            self.kind is EconomicForecastKind.UNAVAILABLE
            or statistic is EconomicForecastStatistic.UNAVAILABLE
        ):
            raise ValueError("available forecast uses an unavailable enum")
        if (
            self.scope is EconomicForecastScope.MACHINE_EVENT_TARGET
            and self.kind is not EconomicForecastKind.MACHINE_PROJECTION
        ):
            raise ValueError("machine event scope requires machine projection")
        if (
            self.kind is EconomicForecastKind.MACHINE_PROJECTION
            and self.scope is not EconomicForecastScope.MACHINE_EVENT_TARGET
        ):
            raise ValueError("machine projection requires machine event scope")
        object.__setattr__(self, "value", value)
        object.__setattr__(
            self, "lexical_value", _optional_text(self.lexical_value)
        )
        object.__setattr__(self, "unit", _required_text(self.unit, "unit"))
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        object.__setattr__(self, "scale", scale)
        object.__setattr__(self, "base", _optional_text(self.base))
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("a forecast requires explicit limitations")
        object.__setattr__(self, "limitations", limitations)
        respondent_count = self.respondent_count
        if respondent_count is not None:
            if isinstance(respondent_count, bool) or not isinstance(
                respondent_count, int
            ):
                raise TypeError("respondent_count must be an integer")
            if not 1 <= respondent_count <= 10_000_000:
                raise ValueError(
                    "respondent_count is outside the supported range"
                )
        object.__setattr__(self, "respondent_count", respondent_count)
        dispersion = _optional_finite(self.dispersion, "dispersion")
        if dispersion is not None and dispersion < 0:
            raise ValueError("forecast dispersion cannot be negative")
        object.__setattr__(self, "dispersion", dispersion)
        quantile = _optional_finite(self.quantile, "quantile")
        if statistic is EconomicForecastStatistic.QUANTILE:
            if quantile is None or not 0 < quantile < 1:
                raise ValueError("quantile statistic requires 0 < quantile < 1")
        elif quantile is not None:
            raise ValueError("quantile is only valid for quantile statistic")
        object.__setattr__(self, "quantile", quantile)
        conversion = self.value_conversion
        if conversion is not None:
            if not isinstance(conversion, EconomicUnitConversionV1):
                raise TypeError("value_conversion must use the v1 contract")
            if (
                conversion.target_unit != self.unit
                or conversion.target_scale != self.scale
                or conversion.target_base != self.base
            ):
                raise ValueError(
                    "value conversion target differs from forecast"
                )
            if value is None or not math.isclose(
                conversion.normalized_value,
                value,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError("value conversion does not reproduce forecast")
            if conversion.raw_lexical != self.lexical_value:
                raise ValueError("value conversion loses raw lexical evidence")
        object.__setattr__(self, "value_conversion", conversion)
        expected = _stable_id(
            "economic-calendar-forecast", self.identity_payload()
        )
        supplied = _optional_text(self.forecast_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "forecast_id does not match deterministic identity"
            )
        object.__setattr__(self, "forecast_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic forecast identity."""
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "kind": self.kind.value,
            "scope": self.scope.value,
            "statistic": self.statistic.value,
            "collection_started_at_ns": self.collection_started_at_ns,
            "collection_ended_at_ns": self.collection_ended_at_ns,
            "produced_at_ns": self.produced_at_ns,
            "available_at_ns": self.available_at_ns,
            "value": self.value,
            "lexical_value": self.lexical_value,
            "unit": self.unit,
            "scale": self.scale,
            "base": self.base,
            "source": self.source.to_dict(),
            "limitations": list(self.limitations),
            "respondent_count": self.respondent_count,
            "dispersion": self.dispersion,
            "quantile": self.quantile,
            "value_conversion": (
                None
                if self.value_conversion is None
                else self.value_conversion.to_dict()
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible forecast metadata."""
        return {**self.identity_payload(), "forecast_id": self.forecast_id}

    def to_json(self) -> str:
        """Return deterministic compact forecast JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarForecastV1:
        """Restore and verify one forecast vintage."""
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            kind=EconomicForecastKind.from_value(str(data.get("kind", ""))),
            scope=EconomicForecastScope.from_value(str(data.get("scope", ""))),
            statistic=EconomicForecastStatistic.from_value(
                str(data.get("statistic", ""))
            ),
            collection_started_at_ns=cast(
                int | None, data.get("collection_started_at_ns")
            ),
            collection_ended_at_ns=cast(
                int | None, data.get("collection_ended_at_ns")
            ),
            produced_at_ns=cast(int, data.get("produced_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            value=cast(float, data.get("value")),
            lexical_value=_optional_text(data.get("lexical_value")),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            base=_optional_text(data.get("base")),
            source=MarketContextSourceV1.from_dict(
                _mapping(data.get("source"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            respondent_count=cast(int | None, data.get("respondent_count")),
            dispersion=cast(float | None, data.get("dispersion")),
            quantile=cast(float | None, data.get("quantile")),
            value_conversion=(
                None
                if data.get("value_conversion") is None
                else EconomicUnitConversionV1.from_dict(
                    _mapping(data.get("value_conversion"))
                )
            ),
            forecast_id=str(data.get("forecast_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarForecastV1:
        """Restore a forecast from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic calendar forecast is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicCalendarEventStateV1:
    """Point-in-time state for one logical economic release."""

    release: EconomicCalendarReleaseV1
    visible_actual_vintages: tuple[EconomicCalendarReleaseV1, ...]
    previous_visible_vintages: tuple[EconomicCalendarReleaseV1, ...]
    previous_as_known: EconomicCalendarReleaseV1 | None
    observed_consensus: EconomicCalendarForecastV1 | None
    machine_projection: EconomicCalendarForecastV1 | None
    decision_at_ns: int
    state_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar state schema")
        if not isinstance(self.release, EconomicCalendarReleaseV1):
            raise TypeError("state release must use the v1 contract")
        decision = _bounded_ns(self.decision_at_ns, "decision_at_ns")
        if self.release.available_at_ns > decision:
            raise ValueError("state exposes a release after decision time")
        actuals = tuple(self.visible_actual_vintages)
        if any(
            item.logical_event_key != self.release.logical_event_key
            or item.status
            not in {
                EconomicReleaseStatus.RELEASED,
                EconomicReleaseStatus.UNSCHEDULED,
            }
            or item.actual_value is None
            or item.available_at_ns > decision
            for item in actuals
        ):
            raise ValueError("visible actual vintages are inconsistent")
        actuals = tuple(
            sorted(actuals, key=lambda item: item.revision_sequence)
        )
        if actuals and actuals[0].stage not in _FIRST_PUBLICATION_STAGES:
            raise ValueError(
                "visible actual vintages require an initial release"
            )
        previous_vintages = tuple(
            sorted(
                self.previous_visible_vintages,
                key=lambda item: item.revision_sequence,
            )
        )
        if any(
            item.series_id != self.release.series_id
            or item.reference_period_end_ns
            >= self.release.reference_period_end_ns
            or item.status
            not in {
                EconomicReleaseStatus.RELEASED,
                EconomicReleaseStatus.UNSCHEDULED,
            }
            or item.actual_value is None
            or item.available_at_ns > decision
            for item in previous_vintages
        ):
            raise ValueError("previous visible vintages are inconsistent")
        if previous_vintages:
            selected_period = previous_vintages[0].reference_period_end_ns
            if any(
                item.reference_period_end_ns != selected_period
                for item in previous_vintages
            ):
                raise ValueError("previous vintages span multiple periods")
        previous = self.previous_as_known
        initial_release_time = (
            actuals[0].event_time_ns if actuals else self.release.event_time_ns
        )
        previous_as_known_vintages = tuple(
            item
            for item in previous_vintages
            if item.available_at_ns < initial_release_time
        )
        if previous is not None:
            if previous.series_id != self.release.series_id:
                raise ValueError("previous-as-known belongs to another series")
            if (
                previous.reference_period_end_ns
                >= self.release.reference_period_end_ns
            ):
                raise ValueError("previous-as-known is not an earlier period")
            if previous.available_at_ns >= initial_release_time:
                raise ValueError(
                    "previous-as-known was not known before release"
                )
            if (
                not previous_as_known_vintages
                or previous != previous_as_known_vintages[-1]
            ):
                raise ValueError(
                    "previous-as-known is not the latest prior vintage"
                )
        elif previous_as_known_vintages:
            raise ValueError("previous vintages require previous-as-known")
        for forecast in (self.observed_consensus, self.machine_projection):
            if forecast is None:
                continue
            if forecast.logical_event_key != self.release.logical_event_key:
                raise ValueError("forecast belongs to another logical event")
            if forecast.available_at_ns > decision:
                raise ValueError("state exposes a forecast after decision time")
            if forecast.available_at_ns >= initial_release_time:
                raise ValueError(
                    "forecast must be available strictly before release"
                )
            if forecast.unit != self.release.unit:
                raise ValueError("forecast unit differs from release unit")
            if (
                forecast.scale != self.release.scale
                or forecast.base != self.release.base
            ):
                raise ValueError("forecast scale/base differs from release")
        if (
            self.observed_consensus is not None
            and self.observed_consensus.kind
            is not EconomicForecastKind.OBSERVED_CONSENSUS
        ):
            raise ValueError("observed consensus has the wrong forecast kind")
        if (
            self.machine_projection is not None
            and self.machine_projection.kind
            is not EconomicForecastKind.MACHINE_PROJECTION
        ):
            raise ValueError("machine projection has the wrong forecast kind")
        object.__setattr__(self, "visible_actual_vintages", actuals)
        object.__setattr__(self, "previous_visible_vintages", previous_vintages)
        object.__setattr__(self, "decision_at_ns", decision)
        expected = _stable_id(
            "economic-calendar-state", self.identity_payload()
        )
        supplied = _optional_text(self.state_id)
        if supplied is not None and supplied != expected:
            raise ValueError("state_id does not match deterministic identity")
        object.__setattr__(self, "state_id", expected)

    @property
    def actual_initial(self) -> float | None:
        """Return ``A_e``, the first-release actual visible at decision time."""
        if not self.visible_actual_vintages:
            return None
        return self.visible_actual_vintages[0].actual_value

    @property
    def actual_latest(self) -> float | None:
        """Return the latest visible revision without replacing ``A_e``."""
        if not self.visible_actual_vintages:
            return None
        return self.visible_actual_vintages[-1].actual_value

    @property
    def initial_release_time_ns(self) -> int:
        """Return the first-publication time, never a later revision time."""
        if self.visible_actual_vintages:
            return self.visible_actual_vintages[0].event_time_ns
        return self.release.event_time_ns

    @property
    def event_time_ns(self) -> int:
        """Return the stable logical-event occurrence at this decision time."""
        return self.initial_release_time_ns

    @property
    def actual_revision_deltas(self) -> tuple[tuple[str, float], ...]:
        """Return each visible revision delta from the initial actual."""
        initial = self.actual_initial
        if initial is None:
            return ()
        return tuple(
            (item.release_id, cast(float, item.actual_value) - initial)
            for item in self.visible_actual_vintages[1:]
        )

    @property
    def previous_value(self) -> float | None:
        """Return ``P_e``, the prior period value known before release."""
        if self.previous_as_known is None:
            return None
        return self.previous_as_known.actual_value

    @property
    def previous_initial(self) -> float | None:
        """Return the first retained value for the preceding period."""
        if not self.previous_visible_vintages:
            return None
        return self.previous_visible_vintages[0].actual_value

    @property
    def previous_latest(self) -> float | None:
        """Return the ex-post latest prior-period revision visible at cutoff."""
        if not self.previous_visible_vintages:
            return None
        return self.previous_visible_vintages[-1].actual_value

    @property
    def observed_surprise(self) -> float | None:
        """Return first actual less independently observed consensus."""
        actual = self.actual_initial
        forecast = self.observed_consensus
        if actual is None or forecast is None or forecast.value is None:
            return None
        return actual - forecast.value

    @property
    def machine_surprise(self) -> float | None:
        """Return first actual less the point-in-time machine projection."""
        actual = self.actual_initial
        forecast = self.machine_projection
        if actual is None or forecast is None or forecast.value is None:
            return None
        return actual - forecast.value

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic state identity."""
        return {
            "schema_version": self.schema_version,
            "release": self.release.to_dict(),
            "visible_actual_vintages": [
                item.to_dict() for item in self.visible_actual_vintages
            ],
            "previous_visible_vintages": [
                item.to_dict() for item in self.previous_visible_vintages
            ],
            "previous_as_known": (
                None
                if self.previous_as_known is None
                else self.previous_as_known.to_dict()
            ),
            "observed_consensus": (
                None
                if self.observed_consensus is None
                else self.observed_consensus.to_dict()
            ),
            "machine_projection": (
                None
                if self.machine_projection is None
                else self.machine_projection.to_dict()
            ),
            "decision_at_ns": self.decision_at_ns,
            "initial_release_time_ns": self.initial_release_time_ns,
            "actual_initial": self.actual_initial,
            "actual_latest": self.actual_latest,
            "actual_revision_deltas": [
                {"release_id": release_id, "delta": delta}
                for release_id, delta in self.actual_revision_deltas
            ],
            "previous_value": self.previous_value,
            "previous_initial": self.previous_initial,
            "previous_latest": self.previous_latest,
            "observed_surprise": self.observed_surprise,
            "machine_surprise": self.machine_surprise,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible state metadata."""
        return {**self.identity_payload(), "state_id": self.state_id}

    def to_json(self) -> str:
        """Return deterministic compact state JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarEventStateV1:
        """Restore and verify one point-in-time state."""
        previous = data.get("previous_as_known")
        observed = data.get("observed_consensus")
        machine = data.get("machine_projection")
        return cls(
            release=EconomicCalendarReleaseV1.from_dict(
                _mapping(data.get("release"))
            ),
            visible_actual_vintages=tuple(
                EconomicCalendarReleaseV1.from_dict(_mapping(item))
                for item in _sequence(data.get("visible_actual_vintages"))
            ),
            previous_visible_vintages=tuple(
                EconomicCalendarReleaseV1.from_dict(_mapping(item))
                for item in _sequence(data.get("previous_visible_vintages"))
            ),
            previous_as_known=(
                None
                if previous is None
                else EconomicCalendarReleaseV1.from_dict(_mapping(previous))
            ),
            observed_consensus=(
                None
                if observed is None
                else EconomicCalendarForecastV1.from_dict(_mapping(observed))
            ),
            machine_projection=(
                None
                if machine is None
                else EconomicCalendarForecastV1.from_dict(_mapping(machine))
            ),
            decision_at_ns=cast(int, data.get("decision_at_ns")),
            state_id=str(data.get("state_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarEventStateV1:
        """Restore a point-in-time state from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("economic calendar state is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicCalendarSurpriseV1:
    """One raw, directional, and prior-only robust release surprise."""

    release_id: str
    series_id: str
    series_key: str
    event_time_ns: int
    forecast_id: str
    forecast_kind: EconomicForecastKind
    policy_id: str
    direction: int
    raw_surprise: float
    directional_surprise: float
    prior_support: int
    robust_scale: float | None
    robust_z: float | None
    surprise_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_SURPRISE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_SURPRISE_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar surprise schema")
        for name in ("release_id", "series_id", "forecast_id", "policy_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "series_key", _key(self.series_key, "series_key")
        )
        object.__setattr__(
            self,
            "event_time_ns",
            _bounded_ns(self.event_time_ns, "event_time_ns"),
        )
        kind = EconomicForecastKind.from_value(self.forecast_kind)
        if kind is EconomicForecastKind.UNAVAILABLE:
            raise ValueError("a surprise requires an available forecast")
        object.__setattr__(self, "forecast_kind", kind)
        if isinstance(self.direction, bool) or self.direction not in {-1, 1}:
            raise ValueError("surprise direction must be -1 or +1")
        raw = _finite(self.raw_surprise, "raw_surprise")
        directional = _finite(self.directional_surprise, "directional_surprise")
        if not math.isclose(
            directional,
            self.direction * raw,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "directional surprise differs from direction * raw"
            )
        if isinstance(self.prior_support, bool) or not isinstance(
            self.prior_support, int
        ):
            raise TypeError("prior_support must be an integer")
        if not 0 <= self.prior_support <= 10_000:
            raise ValueError("prior_support is outside the supported range")
        scale = _optional_finite(self.robust_scale, "robust_scale")
        robust_z = _optional_finite(self.robust_z, "robust_z")
        if (scale is None) != (robust_z is None):
            raise ValueError("robust scale and z must be present together")
        if scale is not None:
            if scale <= 0:
                raise ValueError("robust scale must be positive")
            assert robust_z is not None
            if not math.isclose(
                robust_z,
                raw / scale,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError("robust z differs from raw surprise / scale")
        object.__setattr__(self, "raw_surprise", raw)
        object.__setattr__(self, "directional_surprise", directional)
        object.__setattr__(self, "robust_scale", scale)
        object.__setattr__(self, "robust_z", robust_z)
        expected = _stable_id("economic-surprise", self.identity_payload())
        supplied = _optional_text(self.surprise_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "surprise_id does not match deterministic identity"
            )
        object.__setattr__(self, "surprise_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete deterministic surprise evidence."""
        return {
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "series_id": self.series_id,
            "series_key": self.series_key,
            "event_time_ns": self.event_time_ns,
            "forecast_id": self.forecast_id,
            "forecast_kind": self.forecast_kind.value,
            "policy_id": self.policy_id,
            "direction": self.direction,
            "raw_surprise": self.raw_surprise,
            "directional_surprise": self.directional_surprise,
            "prior_support": self.prior_support,
            "robust_scale": self.robust_scale,
            "robust_z": self.robust_z,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible surprise metadata."""
        return {**self.identity_payload(), "surprise_id": self.surprise_id}

    def to_json(self) -> str:
        """Return deterministic compact surprise JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarSurpriseV1:
        """Restore and verify one surprise."""
        return cls(
            release_id=str(data.get("release_id", "")),
            series_id=str(data.get("series_id", "")),
            series_key=str(data.get("series_key", "")),
            event_time_ns=cast(int, data.get("event_time_ns")),
            forecast_id=str(data.get("forecast_id", "")),
            forecast_kind=EconomicForecastKind.from_value(
                str(data.get("forecast_kind", ""))
            ),
            policy_id=str(data.get("policy_id", "")),
            direction=cast(int, data.get("direction")),
            raw_surprise=cast(float, data.get("raw_surprise")),
            directional_surprise=cast(float, data.get("directional_surprise")),
            prior_support=cast(int, data.get("prior_support")),
            robust_scale=cast(float | None, data.get("robust_scale")),
            robust_z=cast(float | None, data.get("robust_z")),
            surprise_id=str(data.get("surprise_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarSurpriseV1:
        """Restore a surprise from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic calendar surprise is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicCalendarQueryV1:
    """Bounded response from the provider-neutral as-known-at protocol."""

    corpus_id: str
    coverage_start_ns: int
    coverage_end_ns: int
    corpus_complete: bool
    start_ns: int
    end_ns: int
    decision_at_ns: int
    events: tuple[EconomicCalendarEventStateV1, ...]
    requested_currencies: tuple[str, ...]
    requested_symbols: tuple[str, ...]
    requested_event_families: tuple[str, ...]
    limitations: tuple[str, ...]
    query_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_QUERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_QUERY_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar query schema")
        object.__setattr__(
            self, "corpus_id", _required_text(self.corpus_id, "corpus_id")
        )
        coverage_start = _bounded_ns(
            self.coverage_start_ns, "coverage_start_ns"
        )
        coverage_end = _bounded_ns(self.coverage_end_ns, "coverage_end_ns")
        if coverage_end <= coverage_start:
            raise ValueError("query coverage end must follow coverage start")
        if not isinstance(self.corpus_complete, bool):
            raise TypeError("corpus_complete must be boolean")
        start = _bounded_ns(self.start_ns, "start_ns")
        end = _bounded_ns(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("query end must follow query start")
        if start < coverage_start or end > coverage_end:
            raise ValueError("query interval lies outside corpus coverage")
        decision = _bounded_ns(self.decision_at_ns, "decision_at_ns")
        events = tuple(self.events)
        if len(events) > MAX_ECONOMIC_CALENDAR_QUERY_EVENTS:
            raise ValueError("economic calendar query exceeds event bound")
        if any(item.decision_at_ns != decision for item in events):
            raise ValueError("query events use another decision time")
        if any(not start <= item.event_time_ns < end for item in events):
            raise ValueError("query event lies outside the requested interval")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(self, "coverage_start_ns", coverage_start)
        object.__setattr__(self, "coverage_end_ns", coverage_end)
        object.__setattr__(self, "decision_at_ns", decision)
        object.__setattr__(
            self,
            "events",
            tuple(
                sorted(
                    events,
                    key=lambda item: (
                        item.event_time_ns,
                        item.release.logical_event_key,
                    ),
                )
            ),
        )
        object.__setattr__(
            self, "requested_currencies", _currencies(self.requested_currencies)
        )
        object.__setattr__(
            self, "requested_symbols", _symbols(self.requested_symbols)
        )
        object.__setattr__(
            self,
            "requested_event_families",
            _event_families(self.requested_event_families),
        )
        object.__setattr__(
            self, "limitations", _text_tuple(self.limitations, "limitation")
        )
        expected = _stable_id(
            "economic-calendar-query", self.identity_payload()
        )
        supplied = _optional_text(self.query_id)
        if supplied is not None and supplied != expected:
            raise ValueError("query_id does not match deterministic identity")
        object.__setattr__(self, "query_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic query identity."""
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "corpus_complete": self.corpus_complete,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval_semantics": "[start_ns,end_ns)",
            "decision_at_ns": self.decision_at_ns,
            "admission_rule": "available_at_ns <= decision_at_ns",
            "events": [item.to_dict() for item in self.events],
            "requested_currencies": list(self.requested_currencies),
            "requested_symbols": list(self.requested_symbols),
            "requested_event_families": list(self.requested_event_families),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible query metadata."""
        return {**self.identity_payload(), "query_id": self.query_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarQueryV1:
        """Restore and verify one bounded query."""
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            coverage_start_ns=cast(int, data.get("coverage_start_ns")),
            coverage_end_ns=cast(int, data.get("coverage_end_ns")),
            corpus_complete=cast(bool, data.get("corpus_complete")),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            decision_at_ns=cast(int, data.get("decision_at_ns")),
            events=tuple(
                EconomicCalendarEventStateV1.from_dict(_mapping(item))
                for item in _sequence(data.get("events"))
            ),
            requested_currencies=tuple(
                str(item)
                for item in _sequence(data.get("requested_currencies"))
            ),
            requested_symbols=tuple(
                str(item) for item in _sequence(data.get("requested_symbols"))
            ),
            requested_event_families=tuple(
                str(item)
                for item in _sequence(data.get("requested_event_families"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            query_id=str(data.get("query_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarQueryV1:
        """Restore a bounded query from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("economic calendar query is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@runtime_checkable
class EconomicCalendarSourceAdapterV1(Protocol):
    """Provider-neutral normalization seam for legal source evidence."""

    @property
    def adapter_name(self) -> str:
        """Return the immutable adapter family name."""

    @property
    def adapter_version(self) -> str:
        """Return the immutable adapter implementation version."""

    def load_releases(self) -> Iterable[EconomicCalendarReleaseV1]:
        """Yield immutable normalized release vintages."""

    def load_forecasts(self) -> Iterable[EconomicCalendarForecastV1]:
        """Yield immutable normalized forecast vintages."""


@dataclass(frozen=True, slots=True)
class StaticOfficialEconomicCalendarAdapterV1:
    """Deterministic fixture/import adapter for normalized official evidence."""

    adapter_name: str
    adapter_version: str
    releases: tuple[EconomicCalendarReleaseV1, ...]
    forecasts: tuple[EconomicCalendarForecastV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "adapter_name",
            _required_text(self.adapter_name, "adapter_name"),
        )
        object.__setattr__(
            self,
            "adapter_version",
            _required_text(self.adapter_version, "adapter_version"),
        )
        releases = tuple(self.releases)
        forecasts = tuple(self.forecasts)
        if len(releases) > MAX_ECONOMIC_CALENDAR_RELEASES:
            raise ValueError("static adapter exceeds release bound")
        if len(forecasts) > MAX_ECONOMIC_CALENDAR_FORECASTS:
            raise ValueError("static adapter exceeds forecast bound")
        for release in releases:
            if not isinstance(release, EconomicCalendarReleaseV1):
                raise TypeError("adapter release must use the v1 contract")
            if release.source.adapter_name != self.adapter_name:
                raise ValueError("normalized evidence names another adapter")
            if release.source.adapter_version != self.adapter_version:
                raise ValueError(
                    "normalized evidence names another adapter version"
                )
        for forecast in forecasts:
            if not isinstance(forecast, EconomicCalendarForecastV1):
                raise TypeError("adapter forecast must use the v1 contract")
            if forecast.source.adapter_name != self.adapter_name:
                raise ValueError("normalized evidence names another adapter")
            if forecast.source.adapter_version != self.adapter_version:
                raise ValueError(
                    "normalized evidence names another adapter version"
                )
        object.__setattr__(self, "releases", releases)
        object.__setattr__(self, "forecasts", forecasts)

    def load_releases(self) -> Iterable[EconomicCalendarReleaseV1]:
        """Yield configured official release evidence."""
        return iter(self.releases)

    def load_forecasts(self) -> Iterable[EconomicCalendarForecastV1]:
        """Yield configured independently governed forecasts."""
        return iter(self.forecasts)


@runtime_checkable
class EconomicCalendarAsKnownReaderV1(Protocol):
    """Only calendar surface consumed by event/trader strategies."""

    def as_known_at(
        self,
        *,
        start_ns: int,
        end_ns: int,
        decision_at_ns: int,
        currencies: Sequence[str] = (),
        symbols: Sequence[str] = (),
        event_families: Sequence[str] = (),
        max_events: int = MAX_ECONOMIC_CALENDAR_QUERY_EVENTS,
    ) -> EconomicCalendarQueryV1:
        """Return only evidence available at the decision time."""


@dataclass(frozen=True, slots=True)
class EconomicCalendarCorpusV1:
    """Immutable provider-neutral release and forecast vintage corpus."""

    coverage_start_ns: int
    coverage_end_ns: int
    complete: bool
    releases: tuple[EconomicCalendarReleaseV1, ...]
    forecasts: tuple[EconomicCalendarForecastV1, ...]
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported economic calendar corpus schema")
        start = _bounded_ns(self.coverage_start_ns, "coverage_start_ns")
        end = _bounded_ns(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ValueError("corpus coverage end must follow start")
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be boolean")
        if any(
            not isinstance(item, EconomicCalendarReleaseV1)
            for item in self.releases
        ):
            raise TypeError("corpus releases must use the v1 contract")
        if any(
            not isinstance(item, EconomicCalendarForecastV1)
            for item in self.forecasts
        ):
            raise TypeError("corpus forecasts must use the v1 contract")
        releases = tuple(
            sorted(
                self.releases,
                key=lambda item: (
                    item.logical_event_key,
                    item.revision_sequence,
                    item.release_id,
                ),
            )
        )
        forecasts = tuple(
            sorted(
                self.forecasts,
                key=lambda item: (
                    item.logical_event_key,
                    item.available_at_ns,
                    item.forecast_id,
                ),
            )
        )
        if len(releases) > MAX_ECONOMIC_CALENDAR_RELEASES:
            raise ValueError("corpus exceeds release bound")
        if len(forecasts) > MAX_ECONOMIC_CALENDAR_FORECASTS:
            raise ValueError("corpus exceeds forecast bound")
        if len({item.release_id for item in releases}) != len(releases):
            raise ValueError("corpus contains duplicate release identities")
        if len({item.forecast_id for item in forecasts}) != len(forecasts):
            raise ValueError("corpus contains duplicate forecast identities")
        if any(not start <= item.event_time_ns < end for item in releases):
            raise ValueError("release lies outside corpus coverage")
        by_event: dict[str, list[EconomicCalendarReleaseV1]] = {}
        for release in releases:
            by_event.setdefault(release.logical_event_key, []).append(release)
        by_series: dict[tuple[str, str], tuple[object, ...]] = {}
        for values in by_event.values():
            first = values[0]
            initial_actuals = 0
            publication_stage: EconomicReleaseStage | None = None
            for index, item in enumerate(values):
                if item.revision_sequence != index:
                    raise ValueError(
                        "release revision sequence is not contiguous"
                    )
                if item.semantic_key() != first.semantic_key():
                    raise ValueError(
                        "release semantic identity drifts across vintages"
                    )
                if (
                    index
                    and item.supersedes_release_id
                    != values[index - 1].release_id
                ):
                    raise ValueError(
                        "release revision does not supersede its predecessor"
                    )
                if (
                    index
                    and item.available_at_ns < values[index - 1].available_at_ns
                ):
                    raise ValueError(
                        "release revision availability moves backward"
                    )
                if item.stage is not EconomicReleaseStage.REVISION:
                    if publication_stage is None:
                        publication_stage = item.stage
                    elif item.stage is not publication_stage:
                        raise ValueError(
                            "publication stage changes within a logical event"
                        )
                elif item.status not in {
                    EconomicReleaseStatus.RELEASED,
                    EconomicReleaseStatus.UNSCHEDULED,
                }:
                    raise ValueError(
                        "revision stage requires a released status"
                    )
                if (
                    index
                    and item.scheduled_for_ns
                    != values[index - 1].scheduled_for_ns
                ):
                    if item.status not in {
                        EconomicReleaseStatus.RESCHEDULED,
                        EconomicReleaseStatus.DELAYED,
                    }:
                        raise ValueError(
                            "schedule time changes without reschedule status"
                        )
                elif item.status is EconomicReleaseStatus.RESCHEDULED:
                    raise ValueError("reschedule does not change schedule time")
                if (
                    item.status
                    in {
                        EconomicReleaseStatus.RELEASED,
                        EconomicReleaseStatus.UNSCHEDULED,
                    }
                    and item.stage in _FIRST_PUBLICATION_STAGES
                ):
                    initial_actuals += 1
            if initial_actuals > 1:
                raise ValueError(
                    "logical event contains multiple initial actuals"
                )
            series_semantics = first.series_semantic_key()
            prior_semantics = by_series.setdefault(
                (first.series_key, first.series_version), series_semantics
            )
            if prior_semantics != series_semantics:
                raise ValueError(
                    "series semantic identity drifts across reference periods"
                )
        known_keys = set(by_event)
        for forecast in forecasts:
            if forecast.logical_event_key not in known_keys:
                raise ValueError("forecast has no matching logical release")
            if forecast.unit != by_event[forecast.logical_event_key][0].unit:
                raise ValueError("forecast unit differs from matching release")
            matching = by_event[forecast.logical_event_key][0]
            if (
                forecast.scale != matching.scale
                or forecast.base != matching.base
            ):
                raise ValueError(
                    "forecast scale/base differs from matching release"
                )
        forecast_slots: set[tuple[object, ...]] = set()
        for forecast in forecasts:
            slot = (
                forecast.logical_event_key,
                forecast.kind,
                forecast.scope,
                forecast.statistic,
                forecast.produced_at_ns,
                forecast.available_at_ns,
            )
            if slot in forecast_slots:
                raise ValueError("ambiguous forecast vintages occupy one slot")
            forecast_slots.add(slot)
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("corpus requires explicit limitations")
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(self, "forecasts", forecasts)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "economic-calendar-corpus", self.identity_payload()
        )
        supplied = _optional_text(self.corpus_id)
        if supplied is not None and supplied != expected:
            raise ValueError("corpus_id does not match deterministic identity")
        object.__setattr__(self, "corpus_id", expected)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_ECONOMIC_CALENDAR_CORPUS_BYTES
        ):
            raise ValueError("economic calendar corpus exceeds byte bound")

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic corpus identity."""
        return {
            "schema_version": self.schema_version,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "coverage_semantics": "[coverage_start_ns,coverage_end_ns)",
            "complete": self.complete,
            "releases": [item.to_dict() for item in self.releases],
            "forecasts": [item.to_dict() for item in self.forecasts],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible corpus metadata."""
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarCorpusV1:
        """Restore and verify one corpus."""
        return cls(
            coverage_start_ns=cast(int, data.get("coverage_start_ns")),
            coverage_end_ns=cast(int, data.get("coverage_end_ns")),
            complete=cast(bool, data.get("complete")),
            releases=tuple(
                EconomicCalendarReleaseV1.from_dict(_mapping(item))
                for item in _sequence(data.get("releases"))
            ),
            forecasts=tuple(
                EconomicCalendarForecastV1.from_dict(_mapping(item))
                for item in _sequence(data.get("forecasts"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicCalendarCorpusV1:
        """Restore a corpus from deterministic JSON."""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic calendar corpus is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))

    def as_known_at(
        self,
        *,
        start_ns: int,
        end_ns: int,
        decision_at_ns: int,
        currencies: Sequence[str] = (),
        symbols: Sequence[str] = (),
        event_families: Sequence[str] = (),
        max_events: int = MAX_ECONOMIC_CALENDAR_QUERY_EVENTS,
    ) -> EconomicCalendarQueryV1:
        """Implement the provider-neutral strategy query protocol."""
        return query_economic_calendar_as_known(
            self,
            start_ns=start_ns,
            end_ns=end_ns,
            decision_at_ns=decision_at_ns,
            currencies=currencies,
            symbols=symbols,
            event_families=event_families,
            max_events=max_events,
        )


def build_economic_calendar_corpus(
    adapters: Sequence[EconomicCalendarSourceAdapterV1],
    *,
    coverage_start_ns: int,
    coverage_end_ns: int,
    complete: bool,
    limitations: Sequence[str],
) -> EconomicCalendarCorpusV1:
    """Collect bounded official-source adapters into one neutral corpus."""
    values = tuple(adapters)
    if len(values) > MAX_ECONOMIC_CALENDAR_ADAPTERS:
        raise ValueError("economic calendar adapter count exceeds limit")
    releases: list[EconomicCalendarReleaseV1] = []
    forecasts: list[EconomicCalendarForecastV1] = []
    for adapter in values:
        if not isinstance(adapter, EconomicCalendarSourceAdapterV1):
            raise TypeError(
                "adapter does not implement the calendar source seam"
            )
        name = _required_text(adapter.adapter_name, "adapter_name")
        version = _required_text(adapter.adapter_version, "adapter_version")
        for release in adapter.load_releases():
            if not isinstance(release, EconomicCalendarReleaseV1):
                raise TypeError("adapter emitted a non-v1 release")
            if release.source.adapter_name != name:
                raise ValueError("release source differs from adapter identity")
            if release.source.adapter_version != version:
                raise ValueError("release source differs from adapter version")
            releases.append(release)
        for forecast in adapter.load_forecasts():
            if not isinstance(forecast, EconomicCalendarForecastV1):
                raise TypeError("adapter emitted a non-v1 forecast")
            if forecast.source.adapter_name != name:
                raise ValueError(
                    "forecast source differs from adapter identity"
                )
            if forecast.source.adapter_version != version:
                raise ValueError("forecast source differs from adapter version")
            forecasts.append(forecast)
    return EconomicCalendarCorpusV1(
        coverage_start_ns=coverage_start_ns,
        coverage_end_ns=coverage_end_ns,
        complete=complete,
        releases=tuple(releases),
        forecasts=tuple(forecasts),
        limitations=tuple(limitations),
    )


def _latest_forecast(
    forecasts: Iterable[EconomicCalendarForecastV1],
    *,
    kind: EconomicForecastKind,
    decision_at_ns: int,
    release_time_ns: int,
) -> EconomicCalendarForecastV1 | None:
    eligible = tuple(
        item
        for item in forecasts
        if item.kind is kind
        and (
            item.scope in _CALENDAR_FORECAST_SCOPES
            if kind is EconomicForecastKind.OBSERVED_CONSENSUS
            else item.scope is EconomicForecastScope.MACHINE_EVENT_TARGET
        )
        and item.available_at_ns <= decision_at_ns
        and item.available_at_ns < release_time_ns
    )
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda item: (
            item.available_at_ns,
            item.produced_at_ns,
            item.forecast_id,
        ),
    )


def query_economic_calendar_as_known(
    corpus: EconomicCalendarCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    decision_at_ns: int,
    currencies: Sequence[str] = (),
    symbols: Sequence[str] = (),
    event_families: Sequence[str] = (),
    max_events: int = MAX_ECONOMIC_CALENDAR_QUERY_EVENTS,
) -> EconomicCalendarQueryV1:
    """Return the latest event/value state admissible at decision time."""
    if not isinstance(corpus, EconomicCalendarCorpusV1):
        raise TypeError("query requires an economic calendar v1 corpus")
    start = _bounded_ns(start_ns, "start_ns")
    end = _bounded_ns(end_ns, "end_ns")
    if end <= start:
        raise ValueError("query end must follow query start")
    if start < corpus.coverage_start_ns or end > corpus.coverage_end_ns:
        raise ValueError("query interval lies outside corpus coverage")
    decision = _bounded_ns(decision_at_ns, "decision_at_ns")
    if isinstance(max_events, bool) or not isinstance(max_events, int):
        raise TypeError("max_events must be an integer")
    if not 1 <= max_events <= MAX_ECONOMIC_CALENDAR_QUERY_EVENTS:
        raise ValueError("max_events is outside the supported range")
    requested_currencies = _currencies(currencies)
    requested_symbols = _symbols(symbols)
    requested_families = _event_families(event_families)
    releases_by_event: dict[str, list[EconomicCalendarReleaseV1]] = {}
    actuals_by_series: dict[str, list[EconomicCalendarReleaseV1]] = {}
    for release in corpus.releases:
        releases_by_event.setdefault(release.logical_event_key, []).append(
            release
        )
        if (
            release.status
            in {
                EconomicReleaseStatus.RELEASED,
                EconomicReleaseStatus.UNSCHEDULED,
            }
            and release.actual_value is not None
        ):
            actuals_by_series.setdefault(release.series_id, []).append(release)
    forecasts_by_event: dict[str, list[EconomicCalendarForecastV1]] = {}
    for forecast in corpus.forecasts:
        forecasts_by_event.setdefault(forecast.logical_event_key, []).append(
            forecast
        )
    latest_visible: list[EconomicCalendarReleaseV1] = []
    candidate_times: dict[str, int] = {}
    for values in releases_by_event.values():
        visible = [item for item in values if item.available_at_ns <= decision]
        if visible:
            latest = max(visible, key=lambda item: item.revision_sequence)
            latest_visible.append(latest)
            visible_actuals = tuple(
                item
                for item in visible
                if item.status
                in {
                    EconomicReleaseStatus.RELEASED,
                    EconomicReleaseStatus.UNSCHEDULED,
                }
                and item.actual_value is not None
            )
            candidate_times[latest.release_id] = (
                visible_actuals[0].event_time_ns
                if visible_actuals
                else latest.event_time_ns
            )
    candidates = [
        item
        for item in latest_visible
        if start <= candidate_times[item.release_id] < end
        and (
            not requested_currencies
            or bool(set(requested_currencies) & set(item.affected_currencies))
        )
        and (
            not requested_symbols
            or bool(set(requested_symbols) & set(item.affected_symbols))
        )
        and (
            not requested_families
            or item.event_family.value in requested_families
        )
    ]
    candidates.sort(
        key=lambda item: (
            candidate_times[item.release_id],
            item.logical_event_key,
        )
    )
    if len(candidates) > max_events:
        raise ValueError(
            "economic calendar query exceeds requested event bound"
        )
    states: list[EconomicCalendarEventStateV1] = []
    for release in candidates:
        actuals = tuple(
            item
            for item in releases_by_event[release.logical_event_key]
            if item.status
            in {
                EconomicReleaseStatus.RELEASED,
                EconomicReleaseStatus.UNSCHEDULED,
            }
            and item.actual_value is not None
            and item.available_at_ns <= decision
        )
        initial_release_time = (
            actuals[0].event_time_ns if actuals else release.event_time_ns
        )
        prior_series_vintages = [
            item
            for item in actuals_by_series.get(release.series_id, ())
            if item.reference_period_end_ns < release.reference_period_end_ns
        ]
        previous: EconomicCalendarReleaseV1 | None = None
        previous_vintages: tuple[EconomicCalendarReleaseV1, ...] = ()
        if prior_series_vintages:
            previous_period = max(
                item.reference_period_end_ns for item in prior_series_vintages
            )
            previous_vintages = tuple(
                sorted(
                    (
                        item
                        for item in prior_series_vintages
                        if item.reference_period_end_ns == previous_period
                        and item.available_at_ns <= decision
                    ),
                    key=lambda item: item.revision_sequence,
                )
            )
            previous_as_known = tuple(
                item
                for item in previous_vintages
                if item.available_at_ns < initial_release_time
            )
            if previous_as_known:
                previous = previous_as_known[-1]
        forecasts = forecasts_by_event.get(release.logical_event_key, [])
        states.append(
            EconomicCalendarEventStateV1(
                release=release,
                visible_actual_vintages=actuals,
                previous_visible_vintages=previous_vintages,
                previous_as_known=previous,
                observed_consensus=_latest_forecast(
                    forecasts,
                    kind=EconomicForecastKind.OBSERVED_CONSENSUS,
                    decision_at_ns=decision,
                    release_time_ns=initial_release_time,
                ),
                machine_projection=_latest_forecast(
                    forecasts,
                    kind=EconomicForecastKind.MACHINE_PROJECTION,
                    decision_at_ns=decision,
                    release_time_ns=initial_release_time,
                ),
                decision_at_ns=decision,
            )
        )
    return EconomicCalendarQueryV1(
        corpus_id=corpus.corpus_id,
        coverage_start_ns=corpus.coverage_start_ns,
        coverage_end_ns=corpus.coverage_end_ns,
        corpus_complete=corpus.complete,
        start_ns=start,
        end_ns=end,
        decision_at_ns=decision,
        events=tuple(states),
        requested_currencies=requested_currencies,
        requested_symbols=requested_symbols,
        requested_event_families=requested_families,
        limitations=corpus.limitations,
    )


def compute_economic_calendar_surprises(
    states: Sequence[EconomicCalendarEventStateV1],
    *,
    policy: EconomicSurprisePolicyV1,
    forecast_kind: EconomicForecastKind = EconomicForecastKind.OBSERVED_CONSENSUS,
) -> tuple[EconomicCalendarSurpriseV1, ...]:
    """Compute raw/directional surprises with strictly prior robust scale."""
    if not isinstance(policy, EconomicSurprisePolicyV1):
        raise TypeError("surprise computation requires a v1 policy")
    kind = EconomicForecastKind.from_value(forecast_kind)
    if kind is EconomicForecastKind.UNAVAILABLE:
        raise ValueError("cannot compute surprise from unavailable forecasts")
    values = tuple(states)
    if len(values) > MAX_ECONOMIC_CALENDAR_QUERY_EVENTS:
        raise ValueError("surprise input exceeds the query event bound")
    if any(
        not isinstance(item, EconomicCalendarEventStateV1) for item in values
    ):
        raise TypeError("surprise input must contain v1 calendar states")
    if len({item.release.release_id for item in values}) != len(values):
        raise ValueError("surprise input repeats a release")
    ordered = tuple(
        sorted(
            values,
            key=lambda item: (
                item.event_time_ns,
                item.release.logical_event_key,
            ),
        )
    )
    history: dict[str, list[float]] = {}
    results: list[EconomicCalendarSurpriseV1] = []
    offset = 0
    while offset < len(ordered):
        event_time = ordered[offset].event_time_ns
        end = offset
        while end < len(ordered) and ordered[end].event_time_ns == event_time:
            end += 1
        pending: list[tuple[str, float]] = []
        for state in ordered[offset:end]:
            forecast = (
                state.observed_consensus
                if kind is EconomicForecastKind.OBSERVED_CONSENSUS
                else state.machine_projection
            )
            actual = state.actual_initial
            if forecast is None or forecast.value is None or actual is None:
                continue
            raw = actual - forecast.value
            prior = history.get(state.release.series_id, [])[
                -policy.scale_window :
            ]
            scale: float | None = None
            robust_z: float | None = None
            if len(prior) >= policy.minimum_observations:
                centre = median(prior)
                mad = median(abs(item - centre) for item in prior)
                scale = max(1.4826 * mad, policy.epsilon)
                robust_z = raw / scale
            direction = policy.direction_for(state.release.series_key)
            results.append(
                EconomicCalendarSurpriseV1(
                    release_id=state.release.release_id,
                    series_id=state.release.series_id,
                    series_key=state.release.series_key,
                    event_time_ns=event_time,
                    forecast_id=forecast.forecast_id,
                    forecast_kind=kind,
                    policy_id=policy.policy_id,
                    direction=direction,
                    raw_surprise=raw,
                    directional_surprise=direction * raw,
                    prior_support=len(prior),
                    robust_scale=scale,
                    robust_z=robust_z,
                )
            )
            pending.append((state.release.series_id, raw))
        for series_id, raw in pending:
            history.setdefault(series_id, []).append(raw)
        offset = end
    return tuple(results)


def economic_release_from_market_context(
    event: MarketContextEventV1,
    *,
    logical_event_key: str,
    series_key: str,
    series_version: str,
    comparability_bridge_id: str | None,
    economy: str,
    economy_code: str,
    currency: str,
    institution: str,
    event_family: EconomicEventFamily,
    indicator_id: str,
    source_series_id: str,
    source_table_id: str,
    source_release_id: str,
    source_request_id: str,
    reference_period: str,
    reference_period_end_ns: int,
    frequency: str,
    seasonality: str,
    unit: str,
    scale: float = 1.0,
    base: str | None = None,
    stage: EconomicReleaseStage,
    status: EconomicReleaseStatus,
    time_precision: EconomicTimePrecision,
    timezone_evidence: str,
    actual_lexical: str | None,
    value_conversion: EconomicUnitConversionV1 | None = None,
    schedule_change_reason: str | None = None,
    supersedes_release_id: str | None = None,
) -> EconomicCalendarReleaseV1:
    """Bridge an approved official market-context event into this contract."""
    if not isinstance(event, MarketContextEventV1):
        raise TypeError("bridge requires a MarketContextEventV1")
    selected_stage = EconomicReleaseStage.from_value(stage)
    selected_status = EconomicReleaseStatus.from_value(status)
    is_released = selected_status in {
        EconomicReleaseStatus.RELEASED,
        EconomicReleaseStatus.UNSCHEDULED,
    }
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key=series_key,
        series_version=series_version,
        comparability_bridge_id=comparability_bridge_id,
        economy=economy,
        economy_code=economy_code,
        currency=currency,
        institution=institution,
        event_family=event_family,
        indicator_id=indicator_id,
        source_series_id=source_series_id,
        source_table_id=source_table_id,
        source_release_id=source_release_id,
        source_request_id=source_request_id,
        title=event.title,
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency=frequency,
        seasonality=seasonality,
        unit=unit,
        scale=scale,
        base=base,
        stage=selected_stage,
        status=selected_status,
        scheduled_for_ns=event.event_time_ns,
        scheduled_lexical=event.source_event_time,
        released_at_ns=event.event_time_ns if is_released else None,
        released_lexical=event.source_event_time if is_released else None,
        first_observed_at_ns=event.first_known_at_ns,
        available_at_ns=event.available_at_ns,
        source_timezone=event.source_timezone,
        timezone_evidence=timezone_evidence,
        time_precision=time_precision,
        precision=event.precision,
        market_context_kind=event.kind,
        source=event.source,
        affected_currencies=event.affected_currencies,
        affected_symbols=event.affected_symbols,
        limitations=event.limitations,
        schedule_change_reason=schedule_change_reason,
        value_conversion=value_conversion,
        actual_value=event.actual_value,
        actual_lexical=actual_lexical,
        content_sha256=event.content_sha256,
        revision_sequence=event.revision_sequence,
        supersedes_release_id=supersedes_release_id,
    )


def project_economic_calendar_state(
    state: EconomicCalendarEventStateV1,
    *,
    pre_event_ns: int = 0,
    post_event_ns: int = 1,
) -> MarketContextEventV1:
    """Project one bounded state into the established market-context seam."""
    forecast = state.observed_consensus or state.machine_projection
    projection_tag = (
        "projection:none"
        if forecast is None
        else f"projection:{forecast.kind.value}"
    )
    release = state.release
    occurrence = (
        state.visible_actual_vintages[0]
        if state.visible_actual_vintages
        else release
    )
    return MarketContextEventV1(
        canonical_key=release.logical_event_key,
        kind=release.market_context_kind,
        title=release.title,
        source=release.source,
        source_event_time=(
            occurrence.released_lexical or occurrence.scheduled_lexical
        ),
        source_timezone=release.source_timezone,
        event_time_ns=state.event_time_ns,
        first_known_at_ns=release.first_observed_at_ns,
        available_at_ns=release.available_at_ns,
        pre_event_ns=pre_event_ns,
        post_event_ns=post_event_ns,
        affected_currencies=release.affected_currencies,
        affected_symbols=release.affected_symbols,
        confidence=1.0,
        precision=release.precision,
        limitations=release.limitations,
        vintage_id=state.state_id,
        expected_value=None if forecast is None else forecast.value,
        actual_value=state.actual_initial,
        previous_value=state.previous_value,
        value_unit=release.unit,
        content_sha256=release.content_sha256,
        tags=(
            "economic_calendar",
            f"release_stage:{release.stage.value}",
            f"release_status:{release.status.value}",
            projection_tag,
        ),
    )


def _economic_calendar_arrow_fields() -> Any:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
    except ImportError as exc:  # pragma: no cover - package dependency
        raise RuntimeError(
            "economic calendar Arrow export requires pyarrow"
        ) from exc
    return pa.schema(
        [
            pa.field("record_type", pa.string(), nullable=False),
            pa.field("record_id", pa.string(), nullable=False),
            pa.field("logical_event_key", pa.string(), nullable=False),
            pa.field("series_id", pa.string(), nullable=False),
            pa.field("event_time_ns", pa.int64(), nullable=False),
            pa.field("available_at_ns", pa.int64(), nullable=False),
            pa.field("revision_sequence", pa.int32(), nullable=True),
            pa.field("payload_json", pa.large_string(), nullable=False),
        ]
    )


def economic_calendar_corpus_to_arrow(corpus: EconomicCalendarCorpusV1) -> Any:
    """Return a bounded, lossless Arrow representation of one corpus."""
    if not isinstance(corpus, EconomicCalendarCorpusV1):
        raise TypeError("Arrow export requires an economic calendar v1 corpus")
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
    except ImportError as exc:  # pragma: no cover - package dependency
        raise RuntimeError(
            "economic calendar Arrow export requires pyarrow"
        ) from exc
    event_series = {
        release.logical_event_key: release.series_id
        for release in corpus.releases
    }
    event_times = {
        release.logical_event_key: release.event_time_ns
        for release in corpus.releases
    }
    rows: list[dict[str, Any]] = []
    for release in corpus.releases:
        rows.append(
            {
                "record_type": "release",
                "record_id": release.release_id,
                "logical_event_key": release.logical_event_key,
                "series_id": release.series_id,
                "event_time_ns": release.event_time_ns,
                "available_at_ns": release.available_at_ns,
                "revision_sequence": release.revision_sequence,
                "payload_json": release.to_json(),
            }
        )
    for forecast in corpus.forecasts:
        rows.append(
            {
                "record_type": "forecast",
                "record_id": forecast.forecast_id,
                "logical_event_key": forecast.logical_event_key,
                "series_id": event_series[forecast.logical_event_key],
                "event_time_ns": event_times[forecast.logical_event_key],
                "available_at_ns": forecast.available_at_ns,
                "revision_sequence": None,
                "payload_json": forecast.to_json(),
            }
        )
    metadata = {
        b"schema_version": ECONOMIC_CALENDAR_ARROW_SCHEMA_VERSION.encode(),
        b"corpus_id": corpus.corpus_id.encode(),
        b"coverage_start_ns": str(corpus.coverage_start_ns).encode(),
        b"coverage_end_ns": str(corpus.coverage_end_ns).encode(),
        b"complete": str(corpus.complete).lower().encode(),
        b"limitations": canonical_contract_json(
            list(corpus.limitations)
        ).encode(),
        b"release_count": str(len(corpus.releases)).encode(),
        b"forecast_count": str(len(corpus.forecasts)).encode(),
    }
    table = pa.Table.from_pylist(
        rows, schema=_economic_calendar_arrow_fields().with_metadata(metadata)
    )
    if table.nbytes > MAX_ECONOMIC_CALENDAR_ARROW_BYTES:
        raise ValueError("economic calendar Arrow table exceeds byte bounds")
    return table


def _arrow_metadata(table: Any) -> dict[bytes, bytes]:
    metadata = table.schema.metadata
    if metadata is None:
        raise ValueError("economic calendar Arrow metadata is missing")
    expected_keys = {
        b"schema_version",
        b"corpus_id",
        b"coverage_start_ns",
        b"coverage_end_ns",
        b"complete",
        b"limitations",
        b"release_count",
        b"forecast_count",
    }
    if set(metadata) != expected_keys:
        raise ValueError("economic calendar Arrow metadata schema differs")
    if (
        metadata[b"schema_version"].decode()
        != ECONOMIC_CALENDAR_ARROW_SCHEMA_VERSION
    ):
        raise ValueError("unsupported economic calendar Arrow schema")
    return dict(metadata)


def economic_calendar_corpus_from_arrow(table: Any) -> EconomicCalendarCorpusV1:
    """Restore and verify a corpus from its exact Arrow representation."""
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
    except ImportError as exc:  # pragma: no cover - package dependency
        raise RuntimeError(
            "economic calendar Arrow import requires pyarrow"
        ) from exc
    if not isinstance(table, pa.Table):
        raise TypeError("Arrow import requires a pyarrow Table")
    if table.schema.remove_metadata() != _economic_calendar_arrow_fields():
        raise ValueError("economic calendar Arrow field schema differs")
    if table.num_rows > (
        MAX_ECONOMIC_CALENDAR_RELEASES + MAX_ECONOMIC_CALENDAR_FORECASTS
    ):
        raise ValueError("economic calendar Arrow table exceeds row bounds")
    if table.nbytes > MAX_ECONOMIC_CALENDAR_ARROW_BYTES:
        raise ValueError("economic calendar Arrow table exceeds byte bounds")
    metadata = _arrow_metadata(table)
    try:
        coverage_start_ns = int(metadata[b"coverage_start_ns"])
        coverage_end_ns = int(metadata[b"coverage_end_ns"])
        release_count = int(metadata[b"release_count"])
        forecast_count = int(metadata[b"forecast_count"])
        limitations_value = json.loads(metadata[b"limitations"].decode())
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("economic calendar Arrow metadata is invalid") from exc
    complete_text = metadata[b"complete"].decode()
    if complete_text not in {"true", "false"}:
        raise ValueError("economic calendar Arrow completeness is invalid")
    if not isinstance(limitations_value, list) or any(
        not isinstance(item, str) for item in limitations_value
    ):
        raise ValueError("economic calendar Arrow limitations are invalid")
    releases: list[EconomicCalendarReleaseV1] = []
    forecasts: list[EconomicCalendarForecastV1] = []
    for row in table.to_pylist():
        record_type = row["record_type"]
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "economic calendar Arrow payload is invalid"
            ) from exc
        if record_type == "release":
            release = EconomicCalendarReleaseV1.from_dict(_mapping(payload))
            if (
                row["record_id"] != release.release_id
                or row["logical_event_key"] != release.logical_event_key
                or row["series_id"] != release.series_id
                or row["event_time_ns"] != release.event_time_ns
                or row["available_at_ns"] != release.available_at_ns
                or row["revision_sequence"] != release.revision_sequence
                or row["payload_json"] != release.to_json()
            ):
                raise ValueError(
                    "economic calendar Arrow release projection differs"
                )
            releases.append(release)
        elif record_type == "forecast":
            forecast = EconomicCalendarForecastV1.from_dict(_mapping(payload))
            if (
                row["record_id"] != forecast.forecast_id
                or row["logical_event_key"] != forecast.logical_event_key
                or row["available_at_ns"] != forecast.available_at_ns
                or row["revision_sequence"] is not None
                or row["payload_json"] != forecast.to_json()
            ):
                raise ValueError(
                    "economic calendar Arrow forecast projection differs"
                )
            forecasts.append(forecast)
        else:
            raise ValueError("economic calendar Arrow record type is invalid")
    if len(releases) != release_count or len(forecasts) != forecast_count:
        raise ValueError("economic calendar Arrow row counts differ")
    corpus = EconomicCalendarCorpusV1(
        coverage_start_ns=coverage_start_ns,
        coverage_end_ns=coverage_end_ns,
        complete=complete_text == "true",
        releases=tuple(releases),
        forecasts=tuple(forecasts),
        limitations=tuple(limitations_value),
        corpus_id=metadata[b"corpus_id"].decode(),
    )
    event_series = {
        release.logical_event_key: release.series_id
        for release in corpus.releases
    }
    event_times = {
        release.logical_event_key: release.event_time_ns
        for release in corpus.releases
    }
    for row in table.to_pylist():
        if row["record_type"] == "forecast" and (
            row["series_id"] != event_series[row["logical_event_key"]]
            or row["event_time_ns"] != event_times[row["logical_event_key"]]
        ):
            raise ValueError("economic calendar Arrow forecast linkage differs")
    return corpus


def write_economic_calendar_corpus(
    corpus: EconomicCalendarCorpusV1, directory: str | Path
) -> ArtifactRef:
    """Write one immutable content-addressed neutral corpus artifact."""
    if not isinstance(corpus, EconomicCalendarCorpusV1):
        raise TypeError("writer requires an economic calendar v1 corpus")
    content = corpus.to_json().encode("utf-8") + b"\n"
    digest = hashlib.sha256(content).hexdigest()
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"economic-calendar-corpus-{digest}.json"
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("existing content-addressed corpus differs")
    else:
        with path.open("xb") as stream:
            stream.write(content)
    return ArtifactRef(
        kind="economic_calendar_corpus_v1",
        path=str(path),
        size_bytes=len(content),
        sha256=digest,
        metadata={
            "corpus_id": corpus.corpus_id,
            "release_count": len(corpus.releases),
            "forecast_count": len(corpus.forecasts),
        },
    )


def read_economic_calendar_corpus(path: str | Path) -> EconomicCalendarCorpusV1:
    """Load and hash-verify one content-addressed neutral corpus."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_ECONOMIC_CALENDAR_CORPUS_BYTES:
        raise ValueError("economic calendar corpus artifact exceeds byte bound")
    match = re.fullmatch(
        r"economic-calendar-corpus-([0-9a-f]{64})\.json", source.name
    )
    if match is None:
        raise ValueError(
            "economic calendar corpus name is not content addressed"
        )
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError("economic calendar corpus hash differs from name")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("economic calendar corpus is not UTF-8") from exc
    return EconomicCalendarCorpusV1.from_json(text)


def replay_economic_calendar_corpus(
    path: str | Path,
) -> EconomicCalendarCorpusV1:
    """Rebuild canonical identities from an exact retained corpus artifact."""
    corpus = read_economic_calendar_corpus(path)
    replayed = EconomicCalendarCorpusV1.from_dict(corpus.to_dict())
    if replayed.corpus_id != corpus.corpus_id:
        raise ValueError("economic calendar replay changed corpus identity")
    return replayed


__all__ = [
    "ECONOMIC_CALENDAR_ARROW_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_FORECAST_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_QUERY_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_RELEASE_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_STATE_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_SURPRISE_POLICY_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_SURPRISE_SCHEMA_VERSION",
    "ECONOMIC_UNIT_CONVERSION_SCHEMA_VERSION",
    "MAX_ECONOMIC_CALENDAR_ADAPTERS",
    "MAX_ECONOMIC_CALENDAR_ARROW_BYTES",
    "MAX_ECONOMIC_CALENDAR_CORPUS_BYTES",
    "MAX_ECONOMIC_CALENDAR_FORECASTS",
    "MAX_ECONOMIC_CALENDAR_QUERY_EVENTS",
    "MAX_ECONOMIC_CALENDAR_RELEASES",
    "EconomicCalendarAsKnownReaderV1",
    "EconomicCalendarCorpusV1",
    "EconomicCalendarEventStateV1",
    "EconomicCalendarForecastV1",
    "EconomicCalendarQueryV1",
    "EconomicCalendarReleaseV1",
    "EconomicCalendarSourceAdapterV1",
    "EconomicCalendarSurpriseV1",
    "EconomicEventFamily",
    "EconomicForecastKind",
    "EconomicForecastScope",
    "EconomicForecastStatistic",
    "EconomicReleaseStage",
    "EconomicReleaseStatus",
    "EconomicSurprisePolicyV1",
    "EconomicTimePrecision",
    "EconomicUnitConversionV1",
    "StaticOfficialEconomicCalendarAdapterV1",
    "build_economic_calendar_corpus",
    "compute_economic_calendar_surprises",
    "economic_calendar_corpus_from_arrow",
    "economic_calendar_corpus_to_arrow",
    "economic_release_from_market_context",
    "project_economic_calendar_state",
    "query_economic_calendar_as_known",
    "read_economic_calendar_corpus",
    "replay_economic_calendar_corpus",
    "write_economic_calendar_corpus",
]
